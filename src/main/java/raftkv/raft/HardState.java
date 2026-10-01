package raftkv.raft;

/**
 * The part of a node's state that must survive a crash. Term and vote are required
 * by the protocol; commit is not, but saving it lets a restarted node replay its
 * committed entries without waiting to hear from a leader.
 */
public record HardState(long term, int votedFor, long commit) {
    public static final HardState EMPTY = new HardState(0, RaftNode.NONE, 0);
}
