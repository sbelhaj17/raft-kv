package raftkv.raft;

import java.util.Arrays;

/** One log entry. An empty {@code data} array is the no-op a new leader appends. */
public record Entry(long term, long index, byte[] data) {

    public boolean sameAs(Entry o) {
        return term == o.term && index == o.index && Arrays.equals(data, o.data);
    }

    @Override
    public String toString() {
        return "Entry[" + index + "@" + term + ", " + data.length + "b]";
    }
}
