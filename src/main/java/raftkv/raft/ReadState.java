package raftkv.raft;

/**
 * A read request the leader has confirmed. Once the state machine has applied
 * {@code index}, the read can be answered from it.
 */
public record ReadState(long ctx, long index) {}
