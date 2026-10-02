package raftkv.storage;

import java.util.ArrayList;
import java.util.List;

import raftkv.raft.Entry;
import raftkv.raft.HardState;
import raftkv.raft.Ready;
import raftkv.raft.Snapshot;

/** Storage that lives in memory. The simulator keeps one per node and hands it back on restart. */
public final class MemStorage implements Storage {
    private HardState hardState = HardState.EMPTY;
    private Snapshot snapshot = Snapshot.EMPTY;
    private final List<Entry> entries = new ArrayList<>();

    @Override
    public State load() {
        return new State(hardState, snapshot, List.copyOf(entries));
    }

    @Override
    public void save(Ready rd) {
        if (rd.snapshot() != null) {
            snapshot = rd.snapshot();
            entries.clear();
        }
        for (Entry e : rd.entries()) {
            long first = snapshot.index() + 1;
            int pos = (int) (e.index() - first);
            if (pos < 0 || pos > entries.size()) throw new IllegalStateException("gap before " + e);
            entries.subList(pos, entries.size()).clear();
            entries.add(e);
        }
        if (rd.hardState() != null) hardState = rd.hardState();
    }

    @Override
    public void saveSnapshot(Snapshot s) {
        if (s.index() <= snapshot.index()) return;
        int drop = (int) Math.min(entries.size(), s.index() - snapshot.index());
        entries.subList(0, drop).clear();
        snapshot = s;
    }
}
