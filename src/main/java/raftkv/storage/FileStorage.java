package raftkv.storage;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import raftkv.raft.Entry;
import raftkv.raft.HardState;
import raftkv.raft.Ready;
import raftkv.raft.Snapshot;
import raftkv.wire.Codec;

/**
 * Storage in a directory: a snapshot file and a write-ahead log.
 *
 * <p>The log is a sequence of records, each {@code [length][crc32][type][body]}. An entry record
 * replaces any entry already stored at its index or later, which is how a follower's log gets
 * truncated, so replaying the records in order rebuilds the log. Every {@link #save} appends its
 * records and then calls {@code force}, once, before returning; Raft sends nothing until then.
 *
 * <p>A crash can leave the last record half written. On load, replay stops at the first record
 * that is short or fails its checksum and the file is cut back to the end of the last good one.
 * Only the tail can be torn this way, because records are only ever appended.
 *
 * <p>When a snapshot is saved, the snapshot goes to a temporary file that is forced and then
 * renamed over the old one, and the log is rewritten the same way with just the entries after
 * it. A crash between the two renames leaves a new snapshot with an old log that still covers it,
 * which load handles by skipping entries the snapshot already contains.
 */
public final class FileStorage implements Storage, AutoCloseable {
    private static final byte ENTRY = 1, HARD_STATE = 2;
    private static final int MAX_RECORD = 64 << 20;

    private final Path dir;
    private final Path walPath;
    private final Path snapPath;
    private FileChannel wal;

    // what is on disk, kept in memory so the log can be rewritten after a snapshot
    private HardState hardState = HardState.EMPTY;
    private Snapshot snapshot = Snapshot.EMPTY;
    private final List<Entry> entries = new ArrayList<>();

    private long syncs;

    public FileStorage(Path dir) {
        this.dir = dir;
        this.walPath = dir.resolve("wal");
        this.snapPath = dir.resolve("snapshot");
        try {
            Files.createDirectories(dir);
            readSnapshot();
            replayWal();
            wal = FileChannel.open(walPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            wal.position(wal.size());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public State load() {
        return new State(hardState, snapshot, List.copyOf(entries));
    }

    @Override
    public void save(Ready rd) {
        try {
            if (rd.snapshot() != null) {
                // A snapshot from the leader replaces the whole log.
                entries.clear();
                writeSnapshotFile(rd.snapshot());
                rewriteWal();
            }
            if (rd.entries().isEmpty() && rd.hardState() == null) return;
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            for (Entry e : rd.entries()) {
                remember(e);
                record(buf, ENTRY, out -> Codec.writeEntry(out, e));
            }
            if (rd.hardState() != null) {
                hardState = rd.hardState();
                HardState hs = hardState;
                record(buf, HARD_STATE, out -> {
                    out.writeLong(hs.term());
                    out.writeInt(hs.votedFor());
                    out.writeLong(hs.commit());
                });
            }
            ByteBuffer b = ByteBuffer.wrap(buf.toByteArray());
            while (b.hasRemaining()) wal.write(b);
            wal.force(false);
            syncs++;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void saveSnapshot(Snapshot s) {
        if (s.index() <= snapshot.index()) return;
        try {
            int drop = (int) Math.min(entries.size(), s.index() - snapshot.index());
            entries.subList(0, drop).clear();
            writeSnapshotFile(s);
            rewriteWal();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** How many times {@link #save} has forced the log to disk. */
    public long syncs() {
        return syncs;
    }

    @Override
    public void close() throws IOException {
        wal.close();
    }

    // ---- writing -------------------------------------------------------------------------------

    private interface Body {
        void write(DataOutputStream out) throws IOException;
    }

    private static void record(ByteArrayOutputStream to, byte type, Body body) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeByte(type);
        body.write(d);
        byte[] bytes = b.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(bytes);
        DataOutputStream out = new DataOutputStream(to);
        out.writeInt(bytes.length);
        out.writeInt((int) crc.getValue());
        out.write(bytes);
    }

    private void remember(Entry e) {
        long first = snapshot.index() + 1;
        int pos = (int) (e.index() - first);
        if (pos < 0) return;  // already inside the snapshot
        if (pos > entries.size()) throw new IllegalStateException("gap before " + e);
        entries.subList(pos, entries.size()).clear();
        entries.add(e);
    }

    private void writeSnapshotFile(Snapshot s) throws IOException {
        Path tmp = dir.resolve("snapshot.tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf);
            out.writeLong(s.index());
            out.writeLong(s.term());
            Codec.writeBytes(out, s.data());
            byte[] body = buf.toByteArray();
            CRC32 crc = new CRC32();
            crc.update(body);
            ByteBuffer b = ByteBuffer.allocate(body.length + 4).putInt((int) crc.getValue()).put(body).flip();
            while (b.hasRemaining()) ch.write(b);
            ch.force(true);
        }
        Files.move(tmp, snapPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        snapshot = s;
    }

    /** Write a fresh log holding only the hard state and the entries after the snapshot. */
    private void rewriteWal() throws IOException {
        Path tmp = dir.resolve("wal.tmp");
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        for (Entry e : entries) record(buf, ENTRY, out -> Codec.writeEntry(out, e));
        HardState hs = hardState;
        record(buf, HARD_STATE, out -> {
            out.writeLong(hs.term());
            out.writeInt(hs.votedFor());
            out.writeLong(hs.commit());
        });
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer b = ByteBuffer.wrap(buf.toByteArray());
            while (b.hasRemaining()) ch.write(b);
            ch.force(true);
        }
        if (wal != null) wal.close();
        Files.move(tmp, walPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        wal = FileChannel.open(walPath, StandardOpenOption.WRITE);
        wal.position(wal.size());
    }

    // ---- reading -------------------------------------------------------------------------------

    private void readSnapshot() throws IOException {
        if (!Files.exists(snapPath)) return;
        byte[] all = Files.readAllBytes(snapPath);
        ByteBuffer b = ByteBuffer.wrap(all);
        int crc = b.getInt();
        CRC32 check = new CRC32();
        check.update(all, 4, all.length - 4);
        // The snapshot is only ever replaced by a rename, so a bad one is real corruption.
        if ((int) check.getValue() != crc) throw new IOException("snapshot file is corrupt: " + snapPath);
        DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(all, 4, all.length - 4));
        snapshot = new Snapshot(in.readLong(), in.readLong(), Codec.readBytes(in));
    }

    private void replayWal() throws IOException {
        if (!Files.exists(walPath)) return;
        byte[] all = Files.readAllBytes(walPath);
        ByteBuffer b = ByteBuffer.wrap(all);
        long good = 0;
        while (b.remaining() >= 8) {
            int len = b.getInt();
            int crc = b.getInt();
            if (len <= 0 || len > MAX_RECORD || len > b.remaining()) break;
            byte[] body = new byte[len];
            b.get(body);
            CRC32 check = new CRC32();
            check.update(body);
            if ((int) check.getValue() != crc) break;
            apply(body);
            good = b.position();
        }
        if (good < all.length) {
            // A torn write at the end, from a crash in the middle of an append: drop it.
            try (FileChannel ch = FileChannel.open(walPath, StandardOpenOption.WRITE)) {
                ch.truncate(good);
                ch.force(true);
            }
        }
    }

    private void apply(byte[] body) throws IOException {
        DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(body));
        byte type = in.readByte();
        switch (type) {
            case ENTRY -> remember(Codec.readEntry(in));
            case HARD_STATE -> hardState = new HardState(in.readLong(), in.readInt(), in.readLong());
            default -> throw new IOException("unknown record type " + type);
        }
    }
}
