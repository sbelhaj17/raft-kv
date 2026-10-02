package raftkv.wire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.UncheckedIOException;
import java.util.List;

import org.junit.jupiter.api.Test;

import raftkv.kv.Op;
import raftkv.kv.Request;
import raftkv.kv.Response;
import raftkv.raft.Entry;
import raftkv.raft.Message;
import raftkv.raft.Snapshot;

class CodecTest {
    private static Object roundTrip(Object o) {
        return Codec.decode(Codec.encode(o));
    }

    @Test
    void simpleMessagesRoundTrip() {
        List<Object> values = List.of(
                new Message.RequestVote(3, 1, 2, 10, 2),
                new Message.VoteResponse(3, 2, 1, true),
                new Message.AppendResponse(4, 2, 1, false, 9, 5, 77),
                new Request(5, 6, new Op.Get("k")),
                new Request(5, 7, new Op.Put("k", "välue")),
                new Request(5, 8, new Op.Delete("k")),
                new Request(5, 9, new Op.Cas("k", null, "v")),
                new Request(5, 10, new Op.Status()),
                new Response(5, 9, Response.Status.OK, null, true, 0),
                new Response(5, 9, Response.Status.NOT_LEADER, "x", false, 3),
                new Codec.Hello(2));
        for (Object v : values) assertEquals(v, roundTrip(v));
    }

    @Test
    void appendEntriesAndSnapshotsKeepTheirBytes() {
        Entry a = new Entry(2, 7, new byte[0]);
        Entry b = new Entry(2, 8, new byte[] {1, 2, 3});
        var m = (Message.AppendEntries) roundTrip(new Message.AppendEntries(2, 1, 3, 6, 1, List.of(a, b), 5, 9));
        assertEquals(6, m.prevIndex());
        assertEquals(5, m.commit());
        assertEquals(9, m.seq());
        assertEquals(2, m.entries().size());
        assertEquals(true, m.entries().get(1).sameAs(b));

        var s = (Message.InstallSnapshot) roundTrip(
                new Message.InstallSnapshot(4, 1, 2, new Snapshot(100, 3, new byte[] {9, 8}), 12));
        assertEquals(100, s.snapshot().index());
        assertEquals(3, s.snapshot().term());
        assertArrayEquals(new byte[] {9, 8}, s.snapshot().data());
        assertEquals(12, s.seq());
    }

    @Test
    void garbageIsRejected() {
        assertThrows(UncheckedIOException.class, () -> Codec.decode(new byte[] {99}));
        byte[] ok = Codec.encode(new Message.VoteResponse(1, 1, 2, true));
        byte[] longer = java.util.Arrays.copyOf(ok, ok.length + 1);
        assertThrows(UncheckedIOException.class, () -> Codec.decode(longer));
    }
}
