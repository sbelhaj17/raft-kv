package raftkv.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * The in-memory log: a snapshot covering indexes 1..snapshot.index, then the entries after it.
 * It also remembers which entries have not been handed out for saving yet ({@link #takeUnstable}).
 */
final class RaftLog {
    private Snapshot snapshot;
    private final ArrayList<Entry> entries = new ArrayList<>();
    private long unstableFrom;

    RaftLog(Snapshot snapshot, List<Entry> stored) {
        this.snapshot = snapshot;
        for (Entry e : stored) {
            if (e.index() != snapshot.index() + 1 + entries.size()) {
                throw new IllegalArgumentException("stored entries are not contiguous after the snapshot: " + e);
            }
            entries.add(e);
        }
        unstableFrom = lastIndex() + 1;
    }

    Snapshot snapshot() {
        return snapshot;
    }

    long snapshotIndex() {
        return snapshot.index();
    }

    long firstIndex() {
        return snapshot.index() + 1;
    }

    long lastIndex() {
        return snapshot.index() + entries.size();
    }

    long lastTerm() {
        return term(lastIndex());
    }

    /** Term of the entry at {@code i}; valid from the snapshot index up to the last index. */
    long term(long i) {
        if (i == snapshot.index()) return snapshot.term();
        return get(i).term();
    }

    Entry get(long i) {
        if (i < firstIndex() || i > lastIndex()) {
            throw new IndexOutOfBoundsException(i + " not in [" + firstIndex() + ", " + lastIndex() + "]");
        }
        return entries.get((int) (i - firstIndex()));
    }

    boolean matches(long i, long term) {
        return i >= snapshot.index() && i <= lastIndex() && term(i) == term;
    }

    /** Copy of entries from..to inclusive. A copy, because later truncation would break a view. */
    List<Entry> slice(long from, long to) {
        if (from > to) return List.of();
        return List.copyOf(entries.subList((int) (from - firstIndex()), (int) (to - firstIndex() + 1)));
    }

    void append(Entry e) {
        if (e.index() != lastIndex() + 1) throw new IllegalStateException("gap before " + e);
        entries.add(e);
        unstableFrom = Math.min(unstableFrom, e.index());
    }

    /** Drop the entry at {@code i} and everything after it. */
    void truncateFrom(long i) {
        if (i <= snapshot.index()) throw new IllegalStateException("truncating into the snapshot at " + i);
        entries.subList((int) (i - firstIndex()), entries.size()).clear();
        unstableFrom = Math.min(unstableFrom, i);
    }

    /** First index of the run of entries that share the term at {@code i}, not going below {@code floor}. */
    long firstIndexOfTermAt(long i, long floor) {
        long t = term(i);
        while (i - 1 >= floor && i - 1 > snapshot.index() && term(i - 1) == t) i--;
        return i;
    }

    /** Replace everything up to {@code s.index} with the snapshot, keeping the entries after it. */
    void compact(Snapshot s) {
        if (s.index() <= snapshot.index()) return;
        if (s.index() > lastIndex()) throw new IllegalStateException("compacting past the end of the log");
        entries.subList(0, (int) (s.index() - firstIndex() + 1)).clear();
        snapshot = s;
        unstableFrom = Math.max(unstableFrom, s.index() + 1);
    }

    /** Throw the whole log away and start again from a snapshot sent by the leader. */
    void reset(Snapshot s) {
        entries.clear();
        snapshot = s;
        unstableFrom = s.index() + 1;
    }

    List<Entry> takeUnstable() {
        List<Entry> out = unstableFrom <= lastIndex() ? slice(unstableFrom, lastIndex()) : List.of();
        unstableFrom = lastIndex() + 1;
        return out;
    }
}
