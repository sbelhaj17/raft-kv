package raftkv.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.random.RandomGenerator;

import raftkv.raft.Entry;
import raftkv.raft.Message;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftNode;
import raftkv.raft.ReadState;
import raftkv.raft.Ready;
import raftkv.raft.Snapshot;
import raftkv.storage.Storage;
import raftkv.wire.Codec;

/**
 * A key-value server on top of one Raft node: it turns client requests into proposals and read
 * index requests, carries out each Ready in the order Raft requires, and answers clients once
 * their writes are applied or their reads are confirmed.
 *
 * <p>Both the simulator and the TCP server drive this same class; they differ only in the
 * {@link Storage} and {@link Outbox} they plug in. It is single threaded: the caller feeds it
 * inputs with {@link #tick}, {@link #receive} and {@link #request}, then calls {@link #flush}.
 */
public final class Replica {

    public interface Outbox {
        void send(Message m);

        void reply(Response r);
    }

    /** Hooks for the simulator's safety checks. */
    public interface Observer {
        Observer NONE = new Observer() {};

        /** Called after a Ready's entries and hard state are durable, before anything is sent. */
        default void persisted(Replica r, Ready rd) {}

        default void applied(Replica r, Entry e, long digest) {}

        default void restored(Replica r, Snapshot s, long digest) {}
    }

    /**
     * @param snapshotEvery take a snapshot and compact the log after this many applied entries (0: never)
     * @param staleReads    deliberately wrong: the leader answers reads from its own state without
     *                      confirming it is still the leader. Only for testing the checker.
     * @param dedup         false deliberately turns off duplicate detection. Only for testing the checker.
     */
    public record Options(int snapshotEvery, boolean staleReads, boolean dedup) {
        public static Options defaults() {
            return new Options(10_000, false, true);
        }

        public Options withSnapshotEvery(int n) {
            return new Options(n, staleReads, dedup);
        }
    }

    private record PendingWrite(Request request, long term) {}

    private record ConfirmedRead(long index, Request request) {}

    private final RaftNode node;
    private final Storage storage;
    private final Outbox out;
    private final Options opts;
    private final Observer observer;

    private KvStore kv;
    private long appliedIndex;
    private long digest;
    private long snapshotIndex;

    // Keyed by client id: a client has at most one request outstanding.
    private final Map<Long, PendingWrite> pendingWrites = new TreeMap<>();
    private final Map<Long, Request> pendingReads = new TreeMap<>();
    private final ArrayDeque<ConfirmedRead> confirmedReads = new ArrayDeque<>();
    private final List<byte[]> proposals = new ArrayList<>();
    private final List<Request> proposed = new ArrayList<>();
    private long nextReadCtx;

    public Replica(int id, int[] members, RaftConfig cfg, RandomGenerator rng, Storage storage, Outbox out,
                   Options opts, Observer observer) {
        this.storage = storage;
        this.out = out;
        this.opts = opts;
        this.observer = observer;
        Storage.State st = storage.load();
        this.node = new RaftNode(id, members, cfg, rng, st.hardState(), st.snapshot(), st.entries());
        this.kv = new KvStore(opts.dedup());
        if (st.snapshot().index() > 0) restore(st.snapshot());
    }

    public void tick() {
        node.tick();
    }

    public void receive(Message m) {
        node.step(m);
    }

    public void request(Request r) {
        switch (r.op()) {
            case Op.Status s -> out.reply(Response.ok(r, status(), true));
            case Op.Get g -> {
                if (opts.staleReads() && node.isLeader()) {
                    out.reply(Response.ok(r, kv.get(g.key()), true));
                    return;
                }
                long ctx = ++nextReadCtx;
                if (!node.readIndex(ctx)) {
                    out.reply(Response.notLeader(r, node.leader()));
                    return;
                }
                pendingReads.put(ctx, r);
            }
            default -> {
                // A copy of a request the client has since moved past, delayed in the network.
                if (kv.isStale(r.clientId(), r.seq())) return;
                Boolean done = kv.resultIfApplied(r.clientId(), r.seq());
                if (done != null) {
                    // A retry of a write that is already applied: answer it from the session.
                    out.reply(Response.ok(r, null, done));
                    return;
                }
                if (!node.isLeader()) {
                    out.reply(Response.notLeader(r, node.leader()));
                    return;
                }
                PendingWrite p = pendingWrites.get(r.clientId());
                // A leader never drops entries from its own log, so a retry that reaches the
                // same leader in the same term is already on its way.
                if (p != null && p.request().seq() == r.seq() && p.term() == node.term()) return;
                if (p != null && p.request().seq() > r.seq()) return;
                pendingWrites.put(r.clientId(), new PendingWrite(r, node.term()));
                proposals.add(Codec.encode(r));
                proposed.add(r);
            }
        }
    }

    /** Propose what has queued up, then carry out the node's Ready. */
    public void flush() {
        if (!proposals.isEmpty()) {
            if (node.propose(proposals) < 0) {
                for (Request r : proposed) {
                    pendingWrites.remove(r.clientId());
                    out.reply(Response.notLeader(r, node.leader()));
                }
            }
            proposals.clear();
            proposed.clear();
        }

        Ready rd = node.ready();
        storage.save(rd);
        observer.persisted(this, rd);
        for (Message m : rd.messages()) out.send(m);
        if (rd.snapshot() != null) restore(rd.snapshot());
        for (Entry e : rd.committed()) apply(e);
        for (ReadState rs : rd.reads()) {
            Request q = pendingReads.remove(rs.ctx());
            if (q != null) confirmedReads.add(new ConfirmedRead(rs.index(), q));
        }
        while (!confirmedReads.isEmpty() && confirmedReads.peek().index() <= appliedIndex) {
            Request q = confirmedReads.poll().request();
            out.reply(Response.ok(q, kv.get(q.op().key()), true));
        }
        if (opts.snapshotEvery() > 0 && appliedIndex - snapshotIndex >= opts.snapshotEvery()) takeSnapshot();
        if (!node.isLeader()) failPending();
    }

    /**
     * Tell waiting clients to go elsewhere. A write may still commit under the next leader; the
     * client retries it with the same seq and the session makes sure it applies once.
     */
    private void failPending() {
        if (pendingWrites.isEmpty() && pendingReads.isEmpty()) return;
        for (PendingWrite p : pendingWrites.values()) out.reply(Response.notLeader(p.request(), node.leader()));
        for (Request r : pendingReads.values()) out.reply(Response.notLeader(r, node.leader()));
        pendingWrites.clear();
        pendingReads.clear();
    }

    private void apply(Entry e) {
        appliedIndex = e.index();
        digest = mix(digest, e);
        if (e.data().length > 0) {
            Request r = (Request) Codec.decode(e.data());
            Boolean ok = kv.apply(r);
            PendingWrite p = pendingWrites.get(r.clientId());
            if (p != null && p.request().seq() == r.seq()) {
                pendingWrites.remove(r.clientId());
                if (ok != null) out.reply(Response.ok(r, null, ok));
            }
        }
        observer.applied(this, e, digest);
    }

    private void takeSnapshot() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (DataOutputStream d = new DataOutputStream(buf)) {
            d.writeLong(digest);
            d.write(kv.snapshot());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Snapshot s = node.compact(appliedIndex, buf.toByteArray());
        storage.saveSnapshot(s);
        snapshotIndex = appliedIndex;
    }

    private void restore(Snapshot s) {
        byte[] data = s.data();
        try (DataInputStream d = new DataInputStream(new ByteArrayInputStream(data))) {
            digest = d.readLong();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        kv = KvStore.restore(Arrays.copyOfRange(data, 8, data.length), opts.dedup());
        appliedIndex = s.index();
        snapshotIndex = s.index();
        observer.restored(this, s, digest);
    }

    /**
     * A running hash of every applied entry. Two replicas that applied the same entries in the
     * same order have the same digest, which makes divergence easy to spot from outside.
     */
    static long mix(long h, Entry e) {
        h = (h ^ e.index()) * 0x9E3779B97F4A7C15L;
        h = (h ^ e.term()) * 0xBF58476D1CE4E5B9L;
        h = (h ^ Arrays.hashCode(e.data())) * 0x94D049BB133111EBL;
        return h ^ (h >>> 31);
    }

    public String status() {
        return "id=" + node.id() + " role=" + node.role() + " term=" + node.term() + " leader=" + node.leader()
                + " commit=" + node.commitIndex() + " applied=" + appliedIndex + " last=" + node.lastIndex()
                + " snapshot=" + node.snapshotIndex() + " keys=" + kv.size() + " digest=" + Long.toHexString(digest);
    }

    public RaftNode node() { return node; }
    public long appliedIndex() { return appliedIndex; }
    public long digest() { return digest; }
    public KvStore kv() { return kv; }
}
