package raftkv.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import raftkv.raft.Entry;
import raftkv.raft.HardState;
import raftkv.raft.Ready;
import raftkv.raft.Snapshot;

class FileStorageTest {
    @TempDir
    Path dir;

    private static Entry e(long index, long term, String data) {
        return new Entry(term, index, data.getBytes());
    }

    private static Ready persist(HardState hs, Snapshot snap, Entry... entries) {
        return new Ready(hs, List.of(entries), snap, List.of(), List.of(), List.of());
    }

    private static List<Long> indexes(Storage.State s) {
        return s.entries().stream().map(Entry::index).toList();
    }

    @Test
    void reopensWhatWasSaved() throws IOException {
        try (FileStorage fs = new FileStorage(dir)) {
            fs.save(persist(new HardState(2, 3, 1), null, e(1, 1, "a"), e(2, 2, "b")));
        }
        Storage.State s = new FileStorage(dir).load();
        assertEquals(new HardState(2, 3, 1), s.hardState());
        assertEquals(List.of(1L, 2L), indexes(s));
        assertArrayEquals("b".getBytes(), s.entries().get(1).data());
    }

    @Test
    void anEntryReplacesEverythingFromItsIndexOn() throws IOException {
        try (FileStorage fs = new FileStorage(dir)) {
            fs.save(persist(null, null, e(1, 1, "a"), e(2, 1, "b"), e(3, 1, "c")));
            // a new leader overwrote index 2
            fs.save(persist(new HardState(2, 2, 1), null, e(2, 2, "x")));
        }
        Storage.State s = new FileStorage(dir).load();
        assertEquals(List.of(1L, 2L), indexes(s));
        assertEquals(2, s.entries().get(1).term());
    }

    @Test
    void aTornLastRecordIsDroppedOnLoad() throws IOException {
        try (FileStorage fs = new FileStorage(dir)) {
            fs.save(persist(new HardState(1, 1, 0), null, e(1, 1, "a")));
            fs.save(persist(null, null, e(2, 1, "bbbbbbbb")));
        }
        Path wal = dir.resolve("wal");
        long full = Files.size(wal);
        try (FileChannel ch = FileChannel.open(wal, StandardOpenOption.WRITE)) {
            ch.truncate(full - 5);  // the crash hit halfway through writing entry 2
        }
        try (FileStorage fs = new FileStorage(dir)) {
            assertEquals(List.of(1L), indexes(fs.load()));
            // and the log is usable again from there
            fs.save(persist(null, null, e(2, 1, "c")));
        }
        assertEquals(List.of(1L, 2L), indexes(new FileStorage(dir).load()));
    }

    @Test
    void aCorruptedRecordEndsTheLog() throws IOException {
        try (FileStorage fs = new FileStorage(dir)) {
            fs.save(persist(null, null, e(1, 1, "a")));
            fs.save(persist(null, null, e(2, 1, "b")));
        }
        byte[] bytes = Files.readAllBytes(dir.resolve("wal"));
        bytes[bytes.length - 1] ^= 0x40;  // flip a bit in the last record's body
        Files.write(dir.resolve("wal"), bytes);
        assertEquals(List.of(1L), indexes(new FileStorage(dir).load()));
    }

    @Test
    void snapshotFromTheLeaderReplacesTheLog() throws IOException {
        try (FileStorage fs = new FileStorage(dir)) {
            fs.save(persist(null, null, e(1, 1, "a"), e(2, 1, "b")));
            Snapshot snap = new Snapshot(10, 3, "state".getBytes());
            fs.save(persist(new HardState(3, 0, 10), snap, e(11, 3, "k")));
        }
        Storage.State s = new FileStorage(dir).load();
        assertEquals(10, s.snapshot().index());
        assertArrayEquals("state".getBytes(), s.snapshot().data());
        assertEquals(List.of(11L), indexes(s));
        assertEquals(10, s.hardState().commit());
    }

    @Test
    void compactionKeepsOnlyEntriesAfterTheSnapshot() throws IOException {
        try (FileStorage fs = new FileStorage(dir)) {
            fs.save(persist(new HardState(1, 1, 3), null, e(1, 1, "a"), e(2, 1, "b"), e(3, 1, "c"), e(4, 1, "d")));
            long before = Files.size(dir.resolve("wal"));
            fs.saveSnapshot(new Snapshot(3, 1, "abc".getBytes()));
            assertEquals(true, Files.size(dir.resolve("wal")) < before);
        }
        Storage.State s = new FileStorage(dir).load();
        assertEquals(3, s.snapshot().index());
        assertEquals(List.of(4L), indexes(s));
        assertEquals(new HardState(1, 1, 3), s.hardState());
    }

    @Test
    void crashBetweenSnapshotAndLogRewriteIsHarmless() throws IOException {
        try (FileStorage fs = new FileStorage(dir)) {
            fs.save(persist(new HardState(1, 1, 3), null, e(1, 1, "a"), e(2, 1, "b"), e(3, 1, "c"), e(4, 1, "d")));
        }
        byte[] oldWal = Files.readAllBytes(dir.resolve("wal"));
        try (FileStorage fs = new FileStorage(dir)) {
            fs.saveSnapshot(new Snapshot(3, 1, "abc".getBytes()));
        }
        // put the old log back: as if the crash came after the snapshot rename, before the log one
        Files.write(dir.resolve("wal"), oldWal);
        Storage.State s = new FileStorage(dir).load();
        assertEquals(3, s.snapshot().index());
        assertEquals(List.of(4L), indexes(s));
    }
}
