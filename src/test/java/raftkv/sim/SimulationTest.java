package raftkv.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import raftkv.kv.Replica;
import raftkv.raft.RaftConfig;

class SimulationTest {
    /** -Draftkv.seeds=N runs more; the default keeps ./gradlew test quick. */
    private static final int SEEDS = Integer.getInteger("raftkv.seeds", 100);

    @Test
    void safeUnderPartitionsCrashesAndLossyNetworks() {
        for (long seed = 1; seed <= SEEDS; seed++) {
            Simulator.Stats s = new Simulator(Simulator.Config.defaults(), seed).run();
            assertTrue(s.completedOps() > 0, "seed " + seed + " finished no operations");
        }
    }

    @Test
    void threeNodesToo() {
        for (long seed = 1; seed <= SEEDS / 2; seed++) {
            new Simulator(Simulator.Config.defaults().withNodes(3), seed).run();
        }
    }

    @Test
    void sameSeedSameRun() {
        Simulator.Stats a = new Simulator(Simulator.Config.defaults(), 42).run();
        Simulator.Stats b = new Simulator(Simulator.Config.defaults(), 42).run();
        assertEquals(a, b);
    }

    // The checks are only worth something if they fail when the system is wrong. Each of these
    // breaks one thing on purpose and expects the simulator to notice within a few hundred seeds.

    @Test
    void catchesReadsServedWithoutConfirmingLeadership() {
        Replica.Options broken = new Replica.Options(25, true, true);
        expectFailure(Simulator.Config.defaults().withReplica(broken), "stale reads");
    }

    @Test
    void catchesWritesAppliedTwice() {
        Replica.Options broken = new Replica.Options(25, false, false);
        expectFailure(Simulator.Config.defaults().withReplica(broken), "no duplicate detection");
    }

    /**
     * The Figure 8 bug is much harder to hit. A new leader sends the old entries and its own no-op
     * together, so the window where only the old entry is on a majority barely exists. With the
     * default settings 2,000 seeds never found it. With one entry per message and ten times the
     * crash rate on 3 nodes, a search from seed 1 stopped at its first failure, seed 1490, and that
     * run is replayed here.
     */
    @Test
    void catchesCommittingEntriesFromEarlierTerms() {
        RaftConfig broken = RaftConfig.defaults().withUnsafeCommitOldTerms(true).withMaxEntriesPerAppend(1);
        Simulator.Config cfg = Simulator.Config.defaults().withNodes(3).withRaft(broken).withCrashRate(0.03);
        Simulator.Failure f = assertThrows(Simulator.Failure.class, () -> new Simulator(cfg, 1490).run());
        assertTrue(f.getMessage().contains("leader completeness"), f.getMessage());

        // the same run with the commit rule intact is fine
        Simulator.Config fixed = cfg.withRaft(broken.withUnsafeCommitOldTerms(false));
        new Simulator(fixed, 1490).run();
    }

    private static void expectFailure(Simulator.Config cfg, String what) {
        for (long seed = 1; seed <= 500; seed++) {
            try {
                new Simulator(cfg, seed).run();
            } catch (Simulator.Failure f) {
                System.out.println(what + ": caught at " + f.getMessage());
                return;
            }
        }
        fail("the simulator did not catch the deliberate bug (" + what + ") in 500 seeds");
    }
}
