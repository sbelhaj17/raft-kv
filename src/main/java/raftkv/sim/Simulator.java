package raftkv.sim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.stream.IntStream;

import raftkv.kv.Op;
import raftkv.kv.Replica;
import raftkv.kv.Request;
import raftkv.kv.Response;
import raftkv.raft.Entry;
import raftkv.raft.Message;
import raftkv.raft.RaftConfig;
import raftkv.raft.RaftNode;
import raftkv.raft.Ready;
import raftkv.raft.Snapshot;
import raftkv.sim.Linearizability.Operation;
import raftkv.storage.MemStorage;

/**
 * Runs a whole cluster, its network and a set of clients in one thread, driven by one seeded
 * random generator, so a run is a pure function of its seed and a failure can be replayed.
 *
 * <p>The network delays, drops, duplicates and reorders every packet, between nodes and between
 * clients and nodes. On top of that the run partitions the nodes and crashes and restarts them;
 * a crashed node loses everything except its {@link MemStorage}. After every tick it checks the
 * safety properties from the Raft paper, and at the end it heals everything, lets the clients
 * finish, and checks that the history the clients saw is linearizable.
 */
public final class Simulator {

    /**
     * @param ticks          length of the faulty part of the run
     * @param maxDelay       packets take 1..maxDelay ticks
     * @param dropRate       chance a packet is lost
     * @param dupRate        chance a packet is delivered twice
     * @param partitionRate  chance per tick of splitting the nodes, when they are not split already
     * @param crashRate      chance per tick that a random running node crashes
     * @param clientTimeout  ticks a client waits before sending the same request to another node
     * @param snapshotEvery  replicas compact their log after this many applied entries
     */
    public record Config(int nodes, int clients, int keys, int opsPerClient, int ticks, int maxDelay,
                         double dropRate, double dupRate, double partitionRate, double crashRate,
                         int clientTimeout, int snapshotEvery, RaftConfig raft, Replica.Options replica) {

        public static Config defaults() {
            return new Config(5, 4, 3, 60, 3000, 4, 0.05, 0.02, 0.004, 0.003, 40, 25,
                    RaftConfig.defaults(), Replica.Options.defaults());
        }

        public Config withNodes(int n) {
            return new Config(n, clients, keys, opsPerClient, ticks, maxDelay, dropRate, dupRate, partitionRate,
                    crashRate, clientTimeout, snapshotEvery, raft, replica);
        }

        public Config withCrashRate(double rate) {
            return new Config(nodes, clients, keys, opsPerClient, ticks, maxDelay, dropRate, dupRate, partitionRate,
                    rate, clientTimeout, snapshotEvery, raft, replica);
        }

        public Config withRaft(RaftConfig r) {
            return new Config(nodes, clients, keys, opsPerClient, ticks, maxDelay, dropRate, dupRate, partitionRate,
                    crashRate, clientTimeout, snapshotEvery, r, replica);
        }

        public Config withReplica(Replica.Options o) {
            return new Config(nodes, clients, keys, opsPerClient, ticks, maxDelay, dropRate, dupRate, partitionRate,
                    crashRate, clientTimeout, snapshotEvery, raft, o);
        }
    }

    /** What a finished run saw, for reporting. */
    public record Stats(long seed, int ticks, int completedOps, int pendingOps, int elections, int crashes,
                        int partitions, int snapshotsSent, long packets, long dropped, long committed) {}

    /** A safety or liveness violation, with the seed that reproduces it. */
    public static final class Failure extends RuntimeException {
        public final long seed;

        Failure(long seed, int tick, String what) {
            super("seed " + seed + ", tick " + tick + ": " + what);
            this.seed = seed;
        }
    }

    private static final int CLIENT_BASE = 1000;
    private static final int QUIESCE_TICKS = 4000;

    private final Config cfg;
    private final long seed;
    private final SplittableRandom rng;
    private final int[] members;

    private final Map<Integer, MemStorage> disks = new TreeMap<>();
    private final Map<Integer, Replica> running = new TreeMap<>();
    private final Map<Integer, Integer> downUntil = new TreeMap<>();
    private final List<Client> clients = new ArrayList<>();
    private final PriorityQueue<Packet> inFlight = new PriorityQueue<>();
    private final List<Operation> history = new ArrayList<>();
    private Set<Integer> sideA = Set.of();
    private int partitionEnds;

    private int tick;
    private long packetSeq;
    private long clock;  // orders client events for the linearizability check
    private boolean faults = true;

    // safety bookkeeping
    private final Map<Long, Integer> leaderOfTerm = new HashMap<>();
    private final Map<Integer, Long> leaderTermSeen = new HashMap<>();
    private final Map<Long, long[]> entryByIndexTerm = new HashMap<>();  // (index, term) -> {data hash, prev term}
    private final Map<Long, long[]> appliedAt = new HashMap<>();         // index -> {term, data hash}
    private final Map<Long, Long> digestAt = new HashMap<>();
    private long highestApplied;

    // stats
    private int elections, crashes, partitions, snapshotsSent;
    private long packets, dropped;

    private record Packet(int at, long seq, int from, int to, Object payload) implements Comparable<Packet> {
        @Override
        public int compareTo(Packet o) {
            return at != o.at ? Integer.compare(at, o.at) : Long.compare(seq, o.seq);
        }
    }

    private final class Client {
        final int id;
        int target;
        long seq;
        Op op;
        long call;
        int lastSent;
        boolean waiting;
        int done;

        Client(int id) {
            this.id = id;
            this.target = members[rng.nextInt(members.length)];
        }
    }

    public Simulator(Config cfg, long seed) {
        this.cfg = cfg;
        this.seed = seed;
        this.rng = new SplittableRandom(seed);
        this.members = IntStream.rangeClosed(1, cfg.nodes()).toArray();
        for (int id : members) {
            disks.put(id, new MemStorage());
            start(id);
        }
        for (int c = 0; c < cfg.clients(); c++) clients.add(new Client(CLIENT_BASE + c));
    }

    /** Run to completion. Throws {@link Failure} on any violation. */
    public Stats run() {
        for (tick = 0; tick < cfg.ticks(); tick++) {
            faults();
            step();
        }
        // Heal everything and give the clients time to finish what they started.
        faults = false;
        sideA = Set.of();
        for (int id : members) if (!running.containsKey(id)) start(id);
        int end = tick + QUIESCE_TICKS;
        for (; tick < end && !allClientsDone(); tick++) step();
        if (!allClientsDone()) fail("clients still waiting " + QUIESCE_TICKS + " ticks after the network healed");

        Linearizability.Result lin = Linearizability.check(history);
        if (!lin.ok()) fail("history is not linearizable on key " + lin.key() + " (" + lin.operations() + " operations)");

        int completed = (int) history.stream().filter(Operation::completed).count();
        return new Stats(seed, tick, completed, history.size() - completed, elections, crashes, partitions,
                snapshotsSent, packets, dropped, highestApplied);
    }

    public List<Operation> history() {
        return history;
    }

    // ---- one tick ----------------------------------------------------------------------------

    private void step() {
        for (Replica r : running.values()) r.tick();
        while (!inFlight.isEmpty() && inFlight.peek().at() <= tick) deliver(inFlight.poll());
        for (Client c : clients) drive(c);
        for (Replica r : running.values()) r.flush();
        checkLeaders();
    }

    private void faults() {
        if (!faults) return;
        if (!sideA.isEmpty() && tick >= partitionEnds) sideA = Set.of();
        if (sideA.isEmpty() && rng.nextDouble() < cfg.partitionRate()) {
            int[] shuffled = members.clone();
            for (int i = shuffled.length - 1; i > 0; i--) {
                int j = rng.nextInt(i + 1);
                int t = shuffled[i];
                shuffled[i] = shuffled[j];
                shuffled[j] = t;
            }
            int cut = 1 + rng.nextInt(shuffled.length - 1);
            sideA = new HashSet<>();
            for (int i = 0; i < cut; i++) sideA.add(shuffled[i]);
            partitionEnds = tick + 20 + rng.nextInt(300);
            partitions++;
        }
        if (rng.nextDouble() < cfg.crashRate() && !running.isEmpty()) {
            List<Integer> up = new ArrayList<>(running.keySet());
            int victim = up.get(rng.nextInt(up.size()));
            running.remove(victim);
            downUntil.put(victim, tick + 10 + rng.nextInt(200));
            leaderTermSeen.remove(victim);
            crashes++;
        }
        List<Integer> due = new ArrayList<>();
        for (var e : downUntil.entrySet()) if (e.getValue() <= tick) due.add(e.getKey());
        for (int id : due) start(id);
    }

    private void start(int id) {
        downUntil.remove(id);
        Replica.Outbox out = new Replica.Outbox() {
            @Override
            public void send(Message m) {
                transmit(id, m.to(), m);
            }

            @Override
            public void reply(Response r) {
                transmit(id, (int) r.clientId(), r);
            }
        };
        Replica.Options opts = new Replica.Options(cfg.snapshotEvery(), cfg.replica().staleReads(), cfg.replica().dedup());
        running.put(id, new Replica(id, members, cfg.raft(), rng.split(), disks.get(id), out, opts, observer()));
    }

    // ---- network -------------------------------------------------------------------------------

    private void transmit(int from, int to, Object payload) {
        packets++;
        if (payload instanceof Message.InstallSnapshot) snapshotsSent++;
        if (faults && rng.nextDouble() < cfg.dropRate()) {
            dropped++;
            return;
        }
        int copies = faults && rng.nextDouble() < cfg.dupRate() ? 2 : 1;
        for (int i = 0; i < copies; i++) {
            int delay = 1 + (faults ? rng.nextInt(cfg.maxDelay()) : 0);
            inFlight.add(new Packet(tick + delay, packetSeq++, from, to, payload));
        }
    }

    private boolean cut(int a, int b) {
        if (sideA.isEmpty() || a >= CLIENT_BASE || b >= CLIENT_BASE) return false;
        return sideA.contains(a) != sideA.contains(b);
    }

    private void deliver(Packet p) {
        if (cut(p.from(), p.to())) {
            dropped++;
            return;
        }
        if (p.to() >= CLIENT_BASE) {
            onResponse(clients.get(p.to() - CLIENT_BASE), (Response) p.payload());
            return;
        }
        Replica r = running.get(p.to());
        if (r == null) {
            dropped++;
            return;
        }
        switch (p.payload()) {
            case Message m -> r.receive(m);
            case Request q -> r.request(q);
            default -> throw new IllegalStateException("unexpected packet " + p.payload());
        }
    }

    // ---- clients -------------------------------------------------------------------------------

    private void drive(Client c) {
        if (!c.waiting) {
            if (c.done >= cfg.opsPerClient()) return;
            c.seq++;
            c.op = nextOp(c);
            c.call = clock++;
            c.waiting = true;
            send(c);
        } else if (tick - c.lastSent >= cfg.clientTimeout()) {
            c.target = members[rng.nextInt(members.length)];
            send(c);
        }
    }

    private Op nextOp(Client c) {
        String key = "k" + rng.nextInt(cfg.keys());
        // Values are unique per write, so a read pins down exactly which write it saw.
        String value = c.id + "." + c.seq;
        int roll = rng.nextInt(100);
        if (roll < 40) return new Op.Get(key);
        if (roll < 70) return new Op.Put(key, value);
        if (roll < 82) return new Op.Delete(key);
        // Expect a value somebody actually wrote, or absent, so some swaps succeed.
        String expected = rng.nextBoolean() ? null : (CLIENT_BASE + rng.nextInt(cfg.clients())) + "." + (1 + rng.nextInt((int) c.seq + 1));
        return new Op.Cas(key, expected, value);
    }

    private void send(Client c) {
        c.lastSent = tick;
        transmit(c.id, c.target, new Request(c.id, c.seq, c.op));
    }

    private void onResponse(Client c, Response r) {
        if (!c.waiting || r.seq() != c.seq) return;  // late or duplicated answer to an older request
        if (r.status() == Response.Status.NOT_LEADER) {
            c.target = r.leader() != RaftNode.NONE ? r.leader() : members[rng.nextInt(members.length)];
            send(c);
            return;
        }
        Boolean ok = c.op instanceof Op.Delete || c.op instanceof Op.Cas ? r.ok() : null;
        String value = c.op instanceof Op.Get ? r.value() : null;
        history.add(new Operation(c.id, c.op, c.call, clock++, value, ok));
        c.waiting = false;
        c.done++;
    }

    private boolean allClientsDone() {
        for (Client c : clients) if (c.waiting || c.done < cfg.opsPerClient()) return false;
        return true;
    }

    // ---- safety checks -------------------------------------------------------------------------

    private void checkLeaders() {
        for (Replica r : running.values()) {
            RaftNode n = r.node();
            if (!n.isLeader()) continue;
            Integer other = leaderOfTerm.putIfAbsent(n.term(), n.id());
            if (other != null && other != n.id()) {
                fail("election safety: nodes " + other + " and " + n.id() + " both led term " + n.term());
            }
            Long seen = leaderTermSeen.get(n.id());
            if (seen == null || seen != n.term()) {
                leaderTermSeen.put(n.id(), n.term());
                elections++;
                checkLeaderCompleteness(n);
            }
        }
    }

    /** A new leader must hold every entry that any replica has applied. */
    private void checkLeaderCompleteness(RaftNode n) {
        if (n.lastIndex() < highestApplied) {
            fail("leader completeness: node " + n.id() + " leads term " + n.term() + " with last index "
                    + n.lastIndex() + " but index " + highestApplied + " was applied somewhere");
        }
        for (long i = Math.max(1, n.snapshotIndex() + 1); i <= highestApplied; i++) {
            long[] a = appliedAt.get(i);
            if (a != null && n.termAt(i) != a[0]) {
                fail("leader completeness: node " + n.id() + " leads term " + n.term() + " with term "
                        + n.termAt(i) + " at index " + i + ", which was applied with term " + a[0]);
            }
        }
    }

    private Replica.Observer observer() {
        return new Replica.Observer() {
            @Override
            public void persisted(Replica r, Ready rd) {
                // Log matching: an (index, term) pair names one entry everywhere, forever, and
                // the entry before it is always the same too.
                RaftNode n = r.node();
                for (Entry e : rd.entries()) {
                    long key = e.index() * 1_000_003L + e.term();
                    long[] now = {Arrays.hashCode(e.data()), n.termAt(e.index() - 1)};
                    long[] before = entryByIndexTerm.putIfAbsent(key, now);
                    if (before != null && (before[0] != now[0] || (before[1] != now[1] && before[1] >= 0 && now[1] >= 0))) {
                        fail("log matching: node " + n.id() + " holds a different entry " + e.index() + "@" + e.term());
                    }
                }
            }

            @Override
            public void applied(Replica r, Entry e, long digest) {
                // State machine safety: every replica applies the same entry at each index.
                long[] now = {e.term(), Arrays.hashCode(e.data())};
                long[] before = appliedAt.putIfAbsent(e.index(), now);
                if (before != null && (before[0] != now[0] || before[1] != now[1])) {
                    fail("state machine safety: node " + r.node().id() + " applied " + e.index() + "@" + e.term()
                            + " where another node applied " + e.index() + "@" + before[0]);
                }
                Long d = digestAt.putIfAbsent(e.index(), digest);
                if (d != null && d != digest) {
                    fail("node " + r.node().id() + " reached a different state after applying index " + e.index());
                }
                highestApplied = Math.max(highestApplied, e.index());
            }

            @Override
            public void restored(Replica r, Snapshot s, long digest) {
                Long d = digestAt.putIfAbsent(s.index(), digest);
                if (d != null && d != digest) {
                    fail("node " + r.node().id() + " restored a snapshot at " + s.index() + " that disagrees with what others applied");
                }
            }
        };
    }

    private void fail(String what) {
        throw new Failure(seed, tick, what);
    }
}
