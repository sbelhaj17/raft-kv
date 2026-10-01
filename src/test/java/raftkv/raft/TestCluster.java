package raftkv.raft;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.stream.IntStream;

/**
 * A few RaftNodes wired together by a perfect, instant network, for unit tests. Persistence is
 * ignored; tests that care about it build nodes from explicit state instead.
 */
final class TestCluster {
    final Map<Integer, RaftNode> nodes = new TreeMap<>();
    final Map<Integer, List<Entry>> applied = new TreeMap<>();
    final Map<Integer, List<ReadState>> reads = new TreeMap<>();
    final Map<Integer, List<Snapshot>> snapshots = new TreeMap<>();
    final Set<Integer> isolated = new HashSet<>();
    final int[] members;

    TestCluster(int n, RaftConfig cfg, long seed) {
        members = IntStream.rangeClosed(1, n).toArray();
        SplittableRandom rng = new SplittableRandom(seed);
        for (int id : members) {
            nodes.put(id, new RaftNode(id, members, cfg, rng.split(), HardState.EMPTY, Snapshot.EMPTY, List.of()));
            applied.put(id, new ArrayList<>());
            reads.put(id, new ArrayList<>());
            snapshots.put(id, new ArrayList<>());
        }
    }

    RaftNode node(int id) {
        return nodes.get(id);
    }

    /** Collect every node's Ready and deliver messages until nothing more is sent. */
    void deliverAll() {
        ArrayDeque<Message> queue = new ArrayDeque<>();
        while (true) {
            for (RaftNode n : nodes.values()) {
                Ready rd = n.ready();
                if (rd.snapshot() != null) snapshots.get(n.id()).add(rd.snapshot());
                applied.get(n.id()).addAll(rd.committed());
                reads.get(n.id()).addAll(rd.reads());
                for (Message m : rd.messages()) {
                    if (!isolated.contains(m.from()) && !isolated.contains(m.to())) queue.add(m);
                }
            }
            if (queue.isEmpty()) return;
            while (!queue.isEmpty()) {
                Message m = queue.poll();
                nodes.get(m.to()).step(m);
            }
        }
    }

    void tickAll(int times) {
        for (int i = 0; i < times; i++) {
            for (RaftNode n : nodes.values()) n.tick();
            deliverAll();
        }
    }

    RaftNode leader() {
        RaftNode found = null;
        for (RaftNode n : nodes.values()) {
            if (n.isLeader() && !isolated.contains(n.id()) && (found == null || n.term() > found.term())) found = n;
        }
        return found;
    }

    RaftNode electLeader() {
        for (int i = 0; i < 1000; i++) {
            RaftNode l = leader();
            if (l != null) return l;
            tickAll(1);
        }
        throw new AssertionError("no leader after 1000 ticks");
    }

    static byte[] cmd(String s) {
        return s.getBytes();
    }
}
