package raftkv.raft;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.random.RandomGenerator;

import raftkv.raft.Message.AppendEntries;
import raftkv.raft.Message.AppendResponse;
import raftkv.raft.Message.InstallSnapshot;
import raftkv.raft.Message.RequestVote;
import raftkv.raft.Message.VoteResponse;

/**
 * One Raft peer as a deterministic state machine. It does no I/O and owns no threads or clocks:
 * the caller feeds it {@link #tick()}s, incoming messages and proposals, then collects a
 * {@link Ready} describing what to persist, send and apply. Given the same inputs and the same
 * random generator it always produces the same outputs, which is what lets the simulator replay
 * a failing run from its seed.
 */
public final class RaftNode {
    /** Node ids start at 1, so 0 can mean "nobody". */
    public static final int NONE = 0;

    private static final byte[] NO_OP = new byte[0];

    private final int id;
    private final int[] peers;
    private final int quorum;
    private final RaftConfig cfg;
    private final RandomGenerator rng;

    private long term;
    private int votedFor;
    private Role role = Role.FOLLOWER;
    private int leader = NONE;
    private final RaftLog log;
    private long commit;
    /** Highest index already handed out in a Ready for applying. */
    private long applied;

    private int electionElapsed;
    private int electionTimeout;
    private int heartbeatElapsed;

    private final Set<Integer> votes = new HashSet<>();
    // TreeMap so that broadcasts go out in the same order every run.
    private final Map<Integer, Progress> progress = new TreeMap<>();

    // Read index. Every AppendEntries carries readSeq; a read is confirmed once a majority has
    // answered a message sent after the read arrived, i.e. one carrying the read's seq or later.
    private long readSeq;
    private long readSeqBroadcast;
    private final ArrayDeque<PendingRead> pendingReads = new ArrayDeque<>();
    private final List<Long> readsAwaitingCommit = new ArrayList<>();

    private final List<Message> outbox = new ArrayList<>();
    private final List<ReadState> readStates = new ArrayList<>();
    private Snapshot receivedSnapshot;
    private HardState savedHardState;

    private static final class Progress {
        long match;
        long next;
        long ackedSeq;

        Progress(long next) {
            this.next = next;
        }
    }

    private record PendingRead(long ctx, long index, long seq) {}

    /**
     * @param members every node in the cluster, including this one
     * @param hs      persisted term, vote and commit index ({@link HardState#EMPTY} for a new node)
     * @param snap    the latest persisted snapshot ({@link Snapshot#EMPTY} if none)
     * @param entries persisted entries after the snapshot
     */
    public RaftNode(int id, int[] members, RaftConfig cfg, RandomGenerator rng,
                    HardState hs, Snapshot snap, List<Entry> entries) {
        if (id == NONE) throw new IllegalArgumentException("node ids start at 1");
        if (Arrays.stream(members).noneMatch(m -> m == id)) throw new IllegalArgumentException("not a member");
        this.id = id;
        this.peers = Arrays.stream(members).filter(m -> m != id).sorted().toArray();
        this.quorum = members.length / 2 + 1;
        this.cfg = cfg;
        this.rng = rng;
        this.log = new RaftLog(snap, entries);
        this.term = hs.term();
        this.votedFor = hs.votedFor();
        this.commit = Math.max(hs.commit(), snap.index());
        this.applied = snap.index();
        this.savedHardState = hs;
        if (commit > log.lastIndex()) {
            throw new IllegalStateException("commit " + commit + " is past the last stored entry " + log.lastIndex());
        }
        resetElectionTimeout();
    }

    // ---- inputs ----------------------------------------------------------------------------

    /** Advance logical time by one tick. */
    public void tick() {
        if (role == Role.LEADER) {
            if (++heartbeatElapsed >= cfg.heartbeatTicks()) {
                heartbeatElapsed = 0;
                broadcastAppend();
            }
        } else if (++electionElapsed >= electionTimeout) {
            campaign();
        }
    }

    /** Append commands to the log. Returns the index of the first one, or -1 if this node is not the leader. */
    public long propose(List<byte[]> commands) {
        if (role != Role.LEADER) return -1;
        long first = log.lastIndex() + 1;
        for (byte[] c : commands) {
            if (c.length == 0) throw new IllegalArgumentException("empty commands are reserved for the leader's no-op");
            log.append(new Entry(term, log.lastIndex() + 1, c));
        }
        maybeCommit();
        broadcastAppend();
        return first;
    }

    public long propose(byte[] command) {
        return propose(List.of(command));
    }

    /**
     * Ask for a linearizable read. If this node is the leader, a {@link ReadState} with the same
     * {@code ctx} appears in a later Ready once a majority has confirmed it is still the leader;
     * the read may then be answered as soon as the state machine has applied
     * {@link ReadState#index()}. Returns false if this node is not the leader. If leadership is
     * lost first, the read is silently dropped and the caller should time out or retry.
     */
    public boolean readIndex(long ctx) {
        if (role != Role.LEADER) return false;
        // Until it has committed an entry of its own term, a new leader does not know how far
        // the previous leader got, so its commit index may be behind writes clients have seen.
        if (log.term(commit) != term) {
            readsAwaitingCommit.add(ctx);
        } else {
            startRead(ctx);
        }
        return true;
    }

    /**
     * The state machine has written a snapshot of everything up to {@code index}; drop those
     * entries from the log. The caller persists the returned snapshot itself.
     */
    public Snapshot compact(long index, byte[] data) {
        if (index > applied) throw new IllegalArgumentException("cannot snapshot unapplied index " + index);
        if (index <= log.snapshotIndex()) return log.snapshot();
        Snapshot s = new Snapshot(index, log.term(index), data);
        log.compact(s);
        return s;
    }

    public void step(Message m) {
        if (m.to() != id) throw new IllegalArgumentException("message for " + m.to() + " delivered to " + id);
        if (m.term() > term) {
            boolean fromLeader = m instanceof AppendEntries || m instanceof InstallSnapshot;
            becomeFollower(m.term(), fromLeader ? m.from() : NONE);
        } else if (m.term() < term) {
            // A stale leader or candidate. Answering with our term makes it step down.
            switch (m) {
                case AppendEntries a -> send(new AppendResponse(term, id, a.from(), false, a.prevIndex(), 0, a.seq()));
                case InstallSnapshot s ->
                        send(new AppendResponse(term, id, s.from(), false, s.snapshot().index(), 0, s.seq()));
                case RequestVote v -> send(new VoteResponse(term, id, v.from(), false));
                default -> { }
            }
            return;
        }
        switch (m) {
            case RequestVote v -> handleRequestVote(v);
            case VoteResponse v -> handleVoteResponse(v);
            case AppendEntries a -> handleAppendEntries(a);
            case AppendResponse r -> handleAppendResponse(r);
            case InstallSnapshot s -> handleInstallSnapshot(s);
        }
    }

    // ---- output ----------------------------------------------------------------------------

    /** Take everything that has to be done since the last call. See {@link Ready} for the required order. */
    public Ready ready() {
        if (role == Role.LEADER && readSeq > readSeqBroadcast) broadcastAppend();

        HardState hs = new HardState(term, votedFor, commit);
        HardState changed = hs.equals(savedHardState) ? null : hs;
        savedHardState = hs;

        List<Entry> committed = List.of();
        if (commit > applied) {
            committed = log.slice(applied + 1, commit);
            applied = commit;
        }
        Ready rd = new Ready(changed, log.takeUnstable(), receivedSnapshot, List.copyOf(outbox), committed,
                List.copyOf(readStates));
        receivedSnapshot = null;
        outbox.clear();
        readStates.clear();
        return rd;
    }

    // ---- introspection, for the application and the checkers --------------------------------

    public int id() { return id; }
    public long term() { return term; }
    public Role role() { return role; }
    public boolean isLeader() { return role == Role.LEADER; }
    /** The leader this node currently follows, or {@link #NONE}. */
    public int leader() { return leader; }
    public int votedFor() { return votedFor; }
    public long commitIndex() { return commit; }
    public long lastIndex() { return log.lastIndex(); }
    public long snapshotIndex() { return log.snapshotIndex(); }
    public long snapshotTerm() { return log.snapshot().term(); }

    /** Term of the entry at {@code i}, or -1 if it is not in the log (compacted or past the end). */
    public long termAt(long i) {
        if (i < log.snapshotIndex() || i > log.lastIndex()) return -1;
        return log.term(i);
    }

    /** The entry at {@code i}, or null if it is compacted or past the end. */
    public Entry entryAt(long i) {
        if (i < log.firstIndex() || i > log.lastIndex()) return null;
        return log.get(i);
    }

    // ---- elections -------------------------------------------------------------------------

    private void campaign() {
        role = Role.CANDIDATE;
        term++;
        votedFor = id;
        leader = NONE;
        clearLeaderState();
        votes.clear();
        votes.add(id);
        electionElapsed = 0;
        resetElectionTimeout();
        if (votes.size() >= quorum) {
            becomeLeader();
            return;
        }
        for (int p : peers) send(new RequestVote(term, id, p, log.lastIndex(), log.lastTerm()));
    }

    private void handleRequestVote(RequestVote v) {
        // One vote per term, and none once this node follows a leader of this term. This does not
        // stop a node that comes back with a higher term from forcing an election: step() forgets
        // the leader on any higher term, and there is no pre-vote.
        boolean canVote = votedFor == v.from() || (votedFor == NONE && leader == NONE);
        boolean upToDate = v.lastTerm() > log.lastTerm()
                || (v.lastTerm() == log.lastTerm() && v.lastIndex() >= log.lastIndex());
        if (canVote && upToDate) {
            votedFor = v.from();
            electionElapsed = 0;
            send(new VoteResponse(term, id, v.from(), true));
        } else {
            send(new VoteResponse(term, id, v.from(), false));
        }
    }

    private void handleVoteResponse(VoteResponse v) {
        if (role != Role.CANDIDATE || !v.granted()) return;
        votes.add(v.from());
        if (votes.size() >= quorum) becomeLeader();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leader = id;
        heartbeatElapsed = 0;
        clearLeaderState();
        for (int p : peers) progress.put(p, new Progress(log.lastIndex() + 1));
        // The no-op gives the new leader an entry of its own term to commit. Until one is
        // committed it can neither commit older entries nor serve reads.
        log.append(new Entry(term, log.lastIndex() + 1, NO_OP));
        maybeCommit();
        broadcastAppend();
    }

    private void becomeFollower(long newTerm, int newLeader) {
        if (newTerm != term) {
            term = newTerm;
            votedFor = NONE;
        }
        role = Role.FOLLOWER;
        leader = newLeader;
        electionElapsed = 0;
        resetElectionTimeout();
        votes.clear();
        clearLeaderState();
    }

    private void clearLeaderState() {
        progress.clear();
        pendingReads.clear();
        readsAwaitingCommit.clear();
        readSeq = 0;
        readSeqBroadcast = 0;
    }

    private void resetElectionTimeout() {
        electionTimeout = cfg.electionTicks() + rng.nextInt(cfg.electionTicks());
    }

    // ---- replication -----------------------------------------------------------------------

    private void broadcastAppend() {
        for (int p : peers) sendAppend(p);
        readSeqBroadcast = readSeq;
    }

    /**
     * Send the peer everything from its next index on. Next is advanced optimistically, so
     * entries are pipelined without waiting for acknowledgements; a lost message shows up as a
     * rejection of the following one, which moves next back.
     */
    private void sendAppend(int peer) {
        Progress pr = progress.get(peer);
        if (pr.next <= log.snapshotIndex()) {
            send(new InstallSnapshot(term, id, peer, log.snapshot(), readSeq));
            pr.next = log.snapshotIndex() + 1;
            return;
        }
        long prevIndex = pr.next - 1;
        long last = Math.min(log.lastIndex(), prevIndex + cfg.maxEntriesPerAppend());
        List<Entry> entries = log.slice(prevIndex + 1, last);
        send(new AppendEntries(term, id, peer, prevIndex, log.term(prevIndex), entries, commit, readSeq));
        pr.next = last + 1;
    }

    private void handleAppendEntries(AppendEntries a) {
        if (role == Role.LEADER) throw new IllegalStateException("two leaders in term " + term);
        if (role == Role.CANDIDATE) becomeFollower(term, a.from());
        leader = a.from();
        electionElapsed = 0;

        if (a.prevIndex() < commit) {
            // Everything up to our commit index is already known to match.
            send(new AppendResponse(term, id, a.from(), true, commit, 0, a.seq()));
            return;
        }
        if (!log.matches(a.prevIndex(), a.prevTerm())) {
            // Tell the leader where to try next: just past our log if it is short, otherwise the
            // start of the conflicting term, so one round trip skips a whole term of bad entries.
            long hint = a.prevIndex() > log.lastIndex()
                    ? log.lastIndex() + 1
                    : log.firstIndexOfTermAt(a.prevIndex(), commit + 1);
            send(new AppendResponse(term, id, a.from(), false, a.prevIndex(), hint, a.seq()));
            return;
        }
        long last = a.prevIndex();
        for (Entry e : a.entries()) {
            if (e.index() <= log.lastIndex()) {
                if (log.term(e.index()) == e.term()) {
                    last = e.index();
                    continue;
                }
                if (e.index() <= commit) {
                    throw new IllegalStateException("leader " + a.from() + " conflicts with committed entry " + e.index());
                }
                log.truncateFrom(e.index());
            }
            log.append(e);
            last = e.index();
        }
        // Only up to `last`: entries past it have not been checked against the leader's log.
        long newCommit = Math.min(a.commit(), last);
        if (newCommit > commit) commit = newCommit;
        send(new AppendResponse(term, id, a.from(), true, last, 0, a.seq()));
    }

    private void handleInstallSnapshot(InstallSnapshot m) {
        if (role == Role.LEADER) throw new IllegalStateException("two leaders in term " + term);
        if (role == Role.CANDIDATE) becomeFollower(term, m.from());
        leader = m.from();
        electionElapsed = 0;

        Snapshot s = m.snapshot();
        if (s.index() <= commit) {
            send(new AppendResponse(term, id, m.from(), true, commit, 0, m.seq()));
            return;
        }
        if (log.matches(s.index(), s.term())) {
            // We already hold the entry the snapshot ends with, so our log is a superset of it.
            commit = s.index();
        } else {
            log.reset(s);
            commit = s.index();
            applied = s.index();
            receivedSnapshot = s;
        }
        send(new AppendResponse(term, id, m.from(), true, s.index(), 0, m.seq()));
    }

    private void handleAppendResponse(AppendResponse r) {
        if (role != Role.LEADER) return;
        Progress pr = progress.get(r.from());
        if (r.seq() > pr.ackedSeq) pr.ackedSeq = r.seq();
        if (r.success()) {
            if (r.index() > pr.match) {
                pr.match = r.index();
                maybeCommit();
            }
            if (r.index() + 1 > pr.next) pr.next = r.index() + 1;
            if (pr.next <= log.lastIndex()) sendAppend(r.from());
        } else if (r.index() > pr.match) {
            // A rejection at or below match is a stale reply to an older message.
            pr.next = Math.max(pr.match + 1, Math.min(r.hint(), pr.next));
            sendAppend(r.from());
        }
        confirmReads();
    }

    private void maybeCommit() {
        long[] match = new long[peers.length + 1];
        match[0] = log.lastIndex();
        int i = 1;
        for (Progress pr : progress.values()) match[i++] = pr.match;
        Arrays.sort(match);
        long n = match[match.length - quorum];
        if (n <= commit) return;
        // Figure 8 of the paper: an entry from an earlier term can be on a majority and still be
        // overwritten later, so a leader only counts replicas for entries of its own term. Older
        // entries are committed indirectly when a later one is.
        if (log.term(n) != term && !cfg.unsafeCommitOldTerms()) return;
        commit = n;
        if (log.term(commit) == term && !readsAwaitingCommit.isEmpty()) {
            for (long ctx : readsAwaitingCommit) startRead(ctx);
            readsAwaitingCommit.clear();
        }
    }

    // ---- reads -----------------------------------------------------------------------------

    private void startRead(long ctx) {
        if (peers.length == 0) {
            readStates.add(new ReadState(ctx, commit));
            return;
        }
        // A fresh seq for each read. Reads that arrive together still share one broadcast,
        // because ready() sends a single heartbeat round carrying the latest seq.
        pendingReads.add(new PendingRead(ctx, commit, ++readSeq));
    }

    private void confirmReads() {
        while (!pendingReads.isEmpty()) {
            PendingRead r = pendingReads.peek();
            int acks = 1;
            for (Progress pr : progress.values()) if (pr.ackedSeq >= r.seq()) acks++;
            if (acks < quorum) return;
            pendingReads.poll();
            readStates.add(new ReadState(r.ctx(), r.index()));
        }
    }

    private void send(Message m) {
        outbox.add(m);
    }

    @Override
    public String toString() {
        return "node " + id + " " + role + " term " + term + " last " + log.lastIndex() + " commit " + commit;
    }
}
