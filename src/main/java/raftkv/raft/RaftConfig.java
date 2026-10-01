package raftkv.raft;

/**
 * @param electionTicks       followers wait a random time in [electionTicks, 2 * electionTicks) before campaigning
 * @param heartbeatTicks      how often a leader sends AppendEntries when it has nothing new
 * @param maxEntriesPerAppend cap on entries in one AppendEntries
 * @param unsafeCommitOldTerms deliberately wrong: let a leader commit an entry from an earlier
 *                            term by counting replicas (Figure 8 of the paper). Only for showing
 *                            that the simulator catches it.
 */
public record RaftConfig(int electionTicks, int heartbeatTicks, int maxEntriesPerAppend,
                         boolean unsafeCommitOldTerms) {

    public RaftConfig {
        if (heartbeatTicks <= 0 || electionTicks <= heartbeatTicks) {
            throw new IllegalArgumentException("need 0 < heartbeatTicks < electionTicks");
        }
        if (maxEntriesPerAppend <= 0) throw new IllegalArgumentException("maxEntriesPerAppend");
    }

    public static RaftConfig defaults() {
        return new RaftConfig(15, 3, 1024, false);
    }

    public RaftConfig withMaxEntriesPerAppend(int n) {
        return new RaftConfig(electionTicks, heartbeatTicks, n, unsafeCommitOldTerms);
    }

    public RaftConfig withUnsafeCommitOldTerms(boolean b) {
        return new RaftConfig(electionTicks, heartbeatTicks, maxEntriesPerAppend, b);
    }
}
