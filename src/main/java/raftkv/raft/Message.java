package raftkv.raft;

import java.util.List;

/** Messages between Raft nodes. There is no InstallSnapshot reply: followers answer with an AppendResponse. */
public sealed interface Message {
    long term();
    int from();
    int to();

    record RequestVote(long term, int from, int to, long lastIndex, long lastTerm) implements Message {}

    record VoteResponse(long term, int from, int to, boolean granted) implements Message {}

    /**
     * {@code seq} is the leader's read sequence number at send time. Followers echo it back,
     * which is how the leader learns that a majority still accepted it after a read arrived.
     */
    record AppendEntries(long term, int from, int to, long prevIndex, long prevTerm,
                         List<Entry> entries, long commit, long seq) implements Message {}

    /**
     * On success, {@code index} is the last index known to match the leader's log. On failure it
     * is the prevIndex that did not match, so the leader can ignore stale rejections, and
     * {@code hint} is where the leader should try next.
     */
    record AppendResponse(long term, int from, int to, boolean success, long index, long hint,
                          long seq) implements Message {}

    record InstallSnapshot(long term, int from, int to, Snapshot snapshot, long seq) implements Message {}
}
