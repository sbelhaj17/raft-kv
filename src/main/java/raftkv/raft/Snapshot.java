package raftkv.raft;

/** State machine contents after applying every entry up to and including {@code index}. */
public record Snapshot(long index, long term, byte[] data) {
    public static final Snapshot EMPTY = new Snapshot(0, 0, new byte[0]);

    @Override
    public String toString() {
        return "Snapshot[" + index + "@" + term + ", " + data.length + "b]";
    }
}
