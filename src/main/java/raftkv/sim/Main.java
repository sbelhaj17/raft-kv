package raftkv.sim;

/**
 * Runs the simulator over a range of seeds.
 *
 * <pre>
 *   ./gradlew simulate --args="--seeds 10000"
 *   ./gradlew simulate --args="--from 4711 --seeds 1 --nodes 3"   # replay one seed
 * </pre>
 */
public final class Main {
    public static void main(String[] args) {
        long from = 1;
        long seeds = 1000;
        int nodes = 5;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--from" -> from = Long.parseLong(args[++i]);
                case "--seeds" -> seeds = Long.parseLong(args[++i]);
                case "--nodes" -> nodes = Integer.parseInt(args[++i]);
                default -> {
                    System.err.println("usage: [--from SEED] [--seeds N] [--nodes N]");
                    System.exit(2);
                }
            }
        }
        Simulator.Config cfg = Simulator.Config.defaults().withNodes(nodes);
        long start = System.nanoTime();
        long ops = 0, elections = 0, crashes = 0, partitions = 0, snapshots = 0, packets = 0, dropped = 0;
        for (long seed = from; seed < from + seeds; seed++) {
            Simulator.Stats s;
            try {
                s = new Simulator(cfg, seed).run();
            } catch (Simulator.Failure f) {
                System.out.println("FAILED " + f.getMessage());
                System.exit(1);
                return;
            }
            ops += s.completedOps();
            elections += s.elections();
            crashes += s.crashes();
            partitions += s.partitions();
            snapshots += s.snapshotsSent();
            packets += s.packets();
            dropped += s.dropped();
            long done = seed - from + 1;
            if (done % 1000 == 0 || done == seeds) {
                System.out.printf("%d seeds ok, %.0f s%n", done, (System.nanoTime() - start) / 1e9);
            }
        }
        System.out.printf("%d runs, %d nodes, no violations%n", seeds, nodes);
        System.out.printf("  client operations checked for linearizability: %,d%n", ops);
        System.out.printf("  leader elections: %,d   crashes: %,d   partitions: %,d   snapshots sent: %,d%n",
                elections, crashes, partitions, snapshots);
        System.out.printf("  packets: %,d, of which dropped or cut off: %,d%n", packets, dropped);
    }
}
