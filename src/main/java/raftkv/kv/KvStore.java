package raftkv.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import raftkv.wire.Codec;

/**
 * The replicated state machine: a string map plus one session per client. The session holds
 * the client's last applied seq and its result, so a write that reaches the log twice (the
 * client retried after a timeout, or the network duplicated it) takes effect once and the
 * retry gets the original answer.
 */
public final class KvStore {
    private final Map<String, String> data = new HashMap<>();
    private final Map<Long, Session> sessions = new HashMap<>();
    private final boolean dedup;

    private record Session(long seq, boolean ok) {}

    public KvStore() {
        this(true);
    }

    /** @param dedup false turns off duplicate detection; only for showing the checker notices. */
    public KvStore(boolean dedup) {
        this.dedup = dedup;
    }

    /**
     * Apply a write. Returns its result, or null if the client has already moved past this seq,
     * in which case nobody is waiting for the answer.
     */
    public Boolean apply(Request r) {
        Session s = sessions.get(r.clientId());
        if (dedup && s != null && r.seq() <= s.seq()) {
            return r.seq() == s.seq() ? s.ok() : null;
        }
        boolean ok = switch (r.op()) {
            case Op.Put p -> {
                data.put(p.key(), p.value());
                yield true;
            }
            case Op.Delete d -> data.remove(d.key()) != null;
            case Op.Cas c -> {
                if (Objects.equals(data.get(c.key()), c.expected())) {
                    data.put(c.key(), c.value());
                    yield true;
                }
                yield false;
            }
            default -> throw new IllegalArgumentException("not a write: " + r.op());
        };
        sessions.put(r.clientId(), new Session(r.seq(), ok));
        return ok;
    }

    public String get(String key) {
        return data.get(key);
    }

    /** The answer to a write that has already been applied, or null if it has not been. */
    public Boolean resultIfApplied(long clientId, long seq) {
        Session s = sessions.get(clientId);
        return dedup && s != null && s.seq() == seq ? s.ok() : null;
    }

    /** True if the client has already moved on to a later request. */
    public boolean isStale(long clientId, long seq) {
        Session s = sessions.get(clientId);
        return dedup && s != null && seq < s.seq();
    }

    public int size() {
        return data.size();
    }

    /** Keys are written in sorted order so equal stores give equal bytes. */
    public byte[] snapshot() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buf)) {
            List<String> keys = new ArrayList<>(data.keySet());
            keys.sort(null);
            out.writeInt(keys.size());
            for (String k : keys) {
                Codec.writeString(out, k);
                Codec.writeString(out, data.get(k));
            }
            List<Long> ids = new ArrayList<>(sessions.keySet());
            ids.sort(null);
            out.writeInt(ids.size());
            for (long id : ids) {
                Session s = sessions.get(id);
                out.writeLong(id);
                out.writeLong(s.seq());
                out.writeBoolean(s.ok());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return buf.toByteArray();
    }

    public static KvStore restore(byte[] snapshot, boolean dedup) {
        KvStore kv = new KvStore(dedup);
        if (snapshot.length == 0) return kv;
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshot))) {
            int n = in.readInt();
            for (int i = 0; i < n; i++) kv.data.put(Codec.readString(in), Codec.readString(in));
            int m = in.readInt();
            for (int i = 0; i < m; i++) kv.sessions.put(in.readLong(), new Session(in.readLong(), in.readBoolean()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return kv;
    }
}
