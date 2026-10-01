package raftkv.raft;

import java.util.List;

/**
 * Everything a node wants done after a batch of inputs. The caller must handle it in this
 * order: save {@code snapshot}, then {@code hardState} and {@code entries}, durably; only then
 * send {@code messages}; then apply {@code committed} and answer {@code reads}. Sending before
 * saving would let a node acknowledge entries or votes it could forget in a crash.
 *
 * @param hardState null if unchanged since the last Ready
 * @param entries   entries to write; they replace any stored entries at the same or later indexes
 * @param snapshot  a snapshot received from the leader, replacing the whole log, or null
 */
public record Ready(HardState hardState, List<Entry> entries, Snapshot snapshot,
                    List<Message> messages, List<Entry> committed, List<ReadState> reads) {

    public boolean mustPersist() {
        return hardState != null || !entries.isEmpty() || snapshot != null;
    }

    public boolean isEmpty() {
        return !mustPersist() && messages.isEmpty() && committed.isEmpty() && reads.isEmpty();
    }
}
