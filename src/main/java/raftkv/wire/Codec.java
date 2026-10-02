package raftkv.wire;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import raftkv.kv.Op;
import raftkv.kv.Request;
import raftkv.kv.Response;
import raftkv.raft.Entry;
import raftkv.raft.Message;
import raftkv.raft.Snapshot;

/**
 * Binary encoding for everything that crosses a socket or goes into a log entry: Raft messages,
 * client requests and responses. Each value starts with a one-byte tag.
 */
public final class Codec {
    private static final byte REQUEST_VOTE = 1, VOTE_RESPONSE = 2, APPEND = 3, APPEND_RESPONSE = 4,
            INSTALL_SNAPSHOT = 5, REQUEST = 10, RESPONSE = 11, HELLO = 12;
    private static final byte GET = 1, PUT = 2, DELETE = 3, CAS = 4, STATUS = 5;

    /** First frame on a connection between peers, naming the sender. */
    public record Hello(int from) {}

    private Codec() {}

    public static byte[] encode(Object o) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(64);
        try {
            write(new DataOutputStream(buf), o);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return buf.toByteArray();
    }

    public static Object decode(byte[] b) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(b));
            Object o = read(in);
            if (in.available() != 0) throw new IOException("trailing bytes after " + o.getClass().getSimpleName());
            return o;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void write(DataOutput out, Object o) throws IOException {
        switch (o) {
            case Message.RequestVote m -> {
                header(out, REQUEST_VOTE, m);
                out.writeLong(m.lastIndex());
                out.writeLong(m.lastTerm());
            }
            case Message.VoteResponse m -> {
                header(out, VOTE_RESPONSE, m);
                out.writeBoolean(m.granted());
            }
            case Message.AppendEntries m -> {
                header(out, APPEND, m);
                out.writeLong(m.prevIndex());
                out.writeLong(m.prevTerm());
                out.writeLong(m.commit());
                out.writeLong(m.seq());
                out.writeInt(m.entries().size());
                for (Entry e : m.entries()) writeEntry(out, e);
            }
            case Message.AppendResponse m -> {
                header(out, APPEND_RESPONSE, m);
                out.writeBoolean(m.success());
                out.writeLong(m.index());
                out.writeLong(m.hint());
                out.writeLong(m.seq());
            }
            case Message.InstallSnapshot m -> {
                header(out, INSTALL_SNAPSHOT, m);
                out.writeLong(m.seq());
                out.writeLong(m.snapshot().index());
                out.writeLong(m.snapshot().term());
                writeBytes(out, m.snapshot().data());
            }
            case Request r -> {
                out.writeByte(REQUEST);
                out.writeLong(r.clientId());
                out.writeLong(r.seq());
                writeOp(out, r.op());
            }
            case Response r -> {
                out.writeByte(RESPONSE);
                out.writeLong(r.clientId());
                out.writeLong(r.seq());
                out.writeByte(r.status().ordinal());
                writeString(out, r.value());
                out.writeBoolean(r.ok());
                out.writeInt(r.leader());
            }
            case Hello h -> {
                out.writeByte(HELLO);
                out.writeInt(h.from());
            }
            default -> throw new IllegalArgumentException("cannot encode " + o.getClass());
        }
    }

    public static Object read(DataInput in) throws IOException {
        byte tag = in.readByte();
        switch (tag) {
            case REQUEST_VOTE: {
                long term = in.readLong();
                int from = in.readInt(), to = in.readInt();
                return new Message.RequestVote(term, from, to, in.readLong(), in.readLong());
            }
            case VOTE_RESPONSE: {
                long term = in.readLong();
                int from = in.readInt(), to = in.readInt();
                return new Message.VoteResponse(term, from, to, in.readBoolean());
            }
            case APPEND: {
                long term = in.readLong();
                int from = in.readInt(), to = in.readInt();
                long prevIndex = in.readLong(), prevTerm = in.readLong(), commit = in.readLong(), seq = in.readLong();
                int n = in.readInt();
                if (n < 0) throw new IOException("negative entry count");
                List<Entry> entries = new ArrayList<>(Math.min(n, 4096));
                for (int i = 0; i < n; i++) entries.add(readEntry(in));
                return new Message.AppendEntries(term, from, to, prevIndex, prevTerm, entries, commit, seq);
            }
            case APPEND_RESPONSE: {
                long term = in.readLong();
                int from = in.readInt(), to = in.readInt();
                return new Message.AppendResponse(term, from, to, in.readBoolean(), in.readLong(), in.readLong(),
                        in.readLong());
            }
            case INSTALL_SNAPSHOT: {
                long term = in.readLong();
                int from = in.readInt(), to = in.readInt();
                long seq = in.readLong();
                Snapshot s = new Snapshot(in.readLong(), in.readLong(), readBytes(in));
                return new Message.InstallSnapshot(term, from, to, s, seq);
            }
            case REQUEST:
                return new Request(in.readLong(), in.readLong(), readOp(in));
            case RESPONSE: {
                long clientId = in.readLong(), seq = in.readLong();
                int status = in.readByte();
                if (status < 0 || status >= Response.Status.values().length) throw new IOException("bad status " + status);
                return new Response(clientId, seq, Response.Status.values()[status], readString(in), in.readBoolean(),
                        in.readInt());
            }
            case HELLO:
                return new Hello(in.readInt());
            default:
                throw new IOException("unknown tag " + tag);
        }
    }

    public static void writeEntry(DataOutput out, Entry e) throws IOException {
        out.writeLong(e.term());
        out.writeLong(e.index());
        writeBytes(out, e.data());
    }

    public static Entry readEntry(DataInput in) throws IOException {
        return new Entry(in.readLong(), in.readLong(), readBytes(in));
    }

    public static void writeBytes(DataOutput out, byte[] b) throws IOException {
        out.writeInt(b.length);
        out.write(b);
    }

    public static byte[] readBytes(DataInput in) throws IOException {
        int n = in.readInt();
        if (n < 0) throw new IOException("negative length " + n);
        byte[] b = new byte[n];
        in.readFully(b);
        return b;
    }

    /** Strings are a length and UTF-8 bytes; length -1 is null. */
    public static void writeString(DataOutput out, String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
            return;
        }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    public static String readString(DataInput in) throws IOException {
        int n = in.readInt();
        if (n == -1) return null;
        if (n < 0) throw new IOException("negative length " + n);
        byte[] b = new byte[n];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static void header(DataOutput out, byte tag, Message m) throws IOException {
        out.writeByte(tag);
        out.writeLong(m.term());
        out.writeInt(m.from());
        out.writeInt(m.to());
    }

    private static void writeOp(DataOutput out, Op op) throws IOException {
        switch (op) {
            case Op.Get g -> {
                out.writeByte(GET);
                writeString(out, g.key());
            }
            case Op.Put p -> {
                out.writeByte(PUT);
                writeString(out, p.key());
                writeString(out, p.value());
            }
            case Op.Delete d -> {
                out.writeByte(DELETE);
                writeString(out, d.key());
            }
            case Op.Cas c -> {
                out.writeByte(CAS);
                writeString(out, c.key());
                writeString(out, c.expected());
                writeString(out, c.value());
            }
            case Op.Status s -> out.writeByte(STATUS);
        }
    }

    private static Op readOp(DataInput in) throws IOException {
        byte kind = in.readByte();
        return switch (kind) {
            case GET -> new Op.Get(readString(in));
            case PUT -> new Op.Put(readString(in), readString(in));
            case DELETE -> new Op.Delete(readString(in));
            case CAS -> new Op.Cas(readString(in), readString(in), readString(in));
            case STATUS -> new Op.Status();
            default -> throw new IOException("unknown op " + kind);
        };
    }
}
