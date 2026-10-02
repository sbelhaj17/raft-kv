package raftkv.storage;

import java.util.List;

import raftkv.raft.Entry;
import raftkv.raft.HardState;
import raftkv.raft.Ready;
import raftkv.raft.Snapshot;

/** Where a node keeps the state Raft needs after a crash. */
public interface Storage {
    record State(HardState hardState, Snapshot snapshot, List<Entry> entries) {}

    State load();

    /**
     * Make the persistent parts of a Ready durable before returning: first the snapshot, if any
     * (it replaces the whole log), then the entries (each replaces any stored entry at the same
     * or a later index), then the hard state.
     */
    void save(Ready rd);

    /** A snapshot this node took itself. Entries up to its index are no longer needed. */
    void saveSnapshot(Snapshot s);
}
