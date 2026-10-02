package raftkv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import raftkv.kv.Replica;
import raftkv.net.KvClient;
import raftkv.net.LocalCluster;
import raftkv.net.Server;
import raftkv.raft.RaftConfig;

/**
 * <pre>
 *   raftkv node --id 1 --cluster 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 --data data/node1
 *   raftkv put|get|del --cluster ... key [value]
 *   raftkv bench [--nodes 3] [--clients 64] [--seconds 10] [--reads 0.0] [--sync false]
 *   raftkv failover [--nodes 3] [--rounds 10]
 * </pre>
 * {@code bench} and {@code failover} start their own cluster, one JVM per node, on localhost.
 */
public final class Main {
    private static final int TICK_MS = 10;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) usage();
        Map<String, String> opts = new HashMap<>();
        List<String> rest = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length) opts.put(args[i].substring(2), args[++i]);
            else rest.add(args[i]);
        }
        switch (args[0]) {
            case "node" -> node(opts);
            case "put", "get", "del" -> oneShot(args[0], opts, rest);
            case "bench" -> bench(opts);
            case "failover" -> failover(opts);
            default -> usage();
        }
    }

    private static void usage() {
        System.err.println("usage: node | put | get | del | bench | failover (see raftkv.Main for flags)");
        System.exit(2);
    }

    static Map<Integer, InetSocketAddress> parseCluster(String spec) {
        Map<Integer, InetSocketAddress> m = new TreeMap<>();
        for (String part : spec.split(",")) {
            String[] idAddr = part.split("=");
            String[] hostPort = idAddr[1].split(":");
            m.put(Integer.parseInt(idAddr[0]), new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])));
        }
        return m;
    }

    private static void node(Map<String, String> o) throws Exception {
        int id = Integer.parseInt(o.get("id"));
        boolean sync = Boolean.parseBoolean(o.getOrDefault("sync", "true"));
        Server s = new Server(id, parseCluster(o.get("cluster")), Path.of(o.get("data")), RaftConfig.defaults(),
                Replica.Options.defaults(), TICK_MS, sync);
        s.start();
        System.out.println("node " + id + " up");
        Thread.currentThread().join();
    }

    private static void oneShot(String cmd, Map<String, String> o, List<String> rest) throws IOException {
        try (KvClient c = new KvClient(ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE),
                parseCluster(o.get("cluster")), 1000)) {
            switch (cmd) {
                case "put" -> c.put(rest.get(0), rest.get(1));
                case "get" -> System.out.println(c.get(rest.get(0)));
                case "del" -> System.out.println(c.delete(rest.get(0)));
                default -> usage();
            }
        }
    }

    // ---- benchmarks ----------------------------------------------------------------------------

    private static LocalCluster cluster(int nodes, boolean sync) throws IOException {
        Path dir = Files.createTempDirectory("raftkv");
        int base = 20000 + ThreadLocalRandom.current().nextInt(20000);
        System.out.println("cluster of " + nodes + " node processes" + (sync ? "" : ", syncs OFF (unsafe)")
                + ", data and logs in " + dir);
        return new LocalCluster(nodes, base, dir, sync);
    }

    private static int waitForLeader(Map<Integer, InetSocketAddress> addrs) throws InterruptedException {
        try (KvClient c = new KvClient(-1, addrs, 300)) {
            for (int attempt = 0; attempt < 500; attempt++) {
                for (int id : addrs.keySet()) {
                    try {
                        if (c.status(id).contains("role=LEADER")) return id;
                    } catch (IOException e) {
                        // not up yet
                    }
                }
                Thread.sleep(20);
            }
        }
        throw new IllegalStateException("no leader after 10 s");
    }

    private static void bench(Map<String, String> o) throws Exception {
        int nodes = Integer.parseInt(o.getOrDefault("nodes", "3"));
        int clients = Integer.parseInt(o.getOrDefault("clients", "64"));
        int seconds = Integer.parseInt(o.getOrDefault("seconds", "10"));
        double reads = Double.parseDouble(o.getOrDefault("reads", "0"));
        boolean sync = Boolean.parseBoolean(o.getOrDefault("sync", "true"));
        try (LocalCluster lc = cluster(nodes, sync)) {
            int leader = waitForLeader(lc.addresses());
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            List<KvClient> cs = new ArrayList<>();
            try {
                // Connect one client at a time, straight to the leader. Thousands of connects at
                // once overflow the listen queue (macOS caps it at 128), and the retries and
                // redirects then run the machine out of local ports.
                for (int i = 0; i < clients; i++) {
                    KvClient c = new KvClient(rnd.nextLong(1, Long.MAX_VALUE), lc.addresses(), 2000);
                    cs.add(c);
                    c.connectTo(leader);
                }
                run(cs, 2, reads);  // warm up the JIT
                Result r = run(cs, seconds, reads);
                System.out.printf("%d clients, %d s, %.0f%% reads: %,d ops, %,.0f ops/s, latency p50 %.2f ms, p99 %.2f ms, max %.1f ms%n",
                        clients, seconds, reads * 100, r.ops, r.ops / (double) seconds, r.pct(0.50), r.pct(0.99),
                        r.pct(1.0));
            } finally {
                for (KvClient c : cs) c.close();
            }
        }
    }

    private record Result(long ops, long[] latencies) {
        double pct(double p) {
            if (latencies.length == 0) return 0;
            int i = (int) Math.min(latencies.length - 1, Math.floor(p * latencies.length));
            return latencies[i] / 1e6;
        }
    }

    private static Result run(List<KvClient> cs, int seconds, double reads) throws InterruptedException {
        int clients = cs.size();
        long end = System.nanoTime() + seconds * 1_000_000_000L;
        long[][] perClient = new long[clients][];
        int[] counts = new int[clients];
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < clients; i++) {
            int idx = i;
            KvClient c = cs.get(i);
            threads.add(Thread.ofVirtual().start(() -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                long[] lat = new long[1024];
                int n = 0;
                try {
                    while (System.nanoTime() < end) {
                        String key = "key" + rnd.nextInt(10_000);
                        long t0 = System.nanoTime();
                        if (rnd.nextDouble() < reads) c.get(key);
                        else c.put(key, "value-" + n);
                        if (n == lat.length) lat = Arrays.copyOf(lat, 2 * n);
                        lat[n++] = System.nanoTime() - t0;
                    }
                } catch (IOException e) {
                    System.err.println("client failed: " + e.getMessage());
                }
                perClient[idx] = lat;
                counts[idx] = n;
            }));
        }
        for (Thread t : threads) t.join();
        long total = Arrays.stream(counts).asLongStream().sum();
        long[] all = new long[(int) total];
        int k = 0;
        for (int i = 0; i < clients; i++) {
            System.arraycopy(perClient[i], 0, all, k, counts[i]);
            k += counts[i];
        }
        Arrays.sort(all);
        return new Result(total, all);
    }

    /**
     * Kill the leader with SIGKILL while a client keeps writing, and time how long the cluster
     * takes to accept a write again: from the moment the old leader's process has exited to the
     * first acknowledgement of a write sent after that. (Counting any acknowledgement after the
     * kill would let in writes the old leader answered just before it died.) Then restart the dead
     * node, let it catch up, and repeat.
     */
    private static void failover(Map<String, String> o) throws Exception {
        int nodes = Integer.parseInt(o.getOrDefault("nodes", "3"));
        int rounds = Integer.parseInt(o.getOrDefault("rounds", "10"));
        try (LocalCluster lc = cluster(nodes, true)) {
            Map<Integer, InetSocketAddress> addrs = lc.addresses();
            waitForLeader(addrs);
            ConcurrentLinkedQueue<long[]> acks = new ConcurrentLinkedQueue<>();  // {sent, acked}
            AtomicBoolean stop = new AtomicBoolean();
            Thread writer = Thread.ofVirtual().start(() -> {
                try (KvClient c = new KvClient(42, addrs, 200)) {
                    for (int i = 0; !stop.get(); i++) {
                        long sent = System.nanoTime();
                        c.put("k" + (i % 100), "v" + i);
                        acks.add(new long[] {sent, System.nanoTime()});
                    }
                } catch (IOException e) {
                    System.err.println("writer failed: " + e.getMessage());
                }
            });
            List<Double> gaps = new ArrayList<>();
            for (int round = 1; round <= rounds; round++) {
                Thread.sleep(1000);
                int leader = waitForLeader(addrs);
                lc.kill(leader);  // returns once the process has exited
                long killed = System.nanoTime();
                Long first = null;
                while (first == null) {
                    for (long[] a : acks) if (a[0] > killed) {
                        first = a[1];
                        break;
                    }
                    if (first == null) Thread.sleep(1);
                }
                double ms = (first - killed) / 1e6;
                gaps.add(ms);
                System.out.printf("round %d: killed leader %d, writes resumed after %.0f ms%n", round, leader, ms);
                acks.clear();
                lc.start(leader);
            }
            stop.set(true);
            writer.join(5000);
            List<Double> sorted = new ArrayList<>(gaps);
            sorted.sort(null);
            System.out.printf("over %d kills: min %.0f ms, median %.0f ms, max %.0f ms%n", rounds, sorted.get(0),
                    sorted.get(sorted.size() / 2), sorted.get(sorted.size() - 1));
        }
    }
}
