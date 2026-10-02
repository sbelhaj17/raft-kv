package raftkv.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class KvStoreTest {
    private static Request req(long client, long seq, Op op) {
        return new Request(client, seq, op);
    }

    @Test
    void putDeleteAndCompareAndSet() {
        KvStore kv = new KvStore();
        assertTrue(kv.apply(req(1, 1, new Op.Put("a", "1"))));
        assertEquals("1", kv.get("a"));
        assertFalse(kv.apply(req(1, 2, new Op.Cas("a", "0", "2"))));
        assertTrue(kv.apply(req(1, 3, new Op.Cas("a", "1", "2"))));
        assertEquals("2", kv.get("a"));
        assertTrue(kv.apply(req(1, 4, new Op.Cas("b", null, "new"))), "null expected means absent");
        assertTrue(kv.apply(req(1, 5, new Op.Delete("a"))));
        assertFalse(kv.apply(req(1, 6, new Op.Delete("a"))));
        assertNull(kv.get("a"));
    }

    @Test
    void aRetriedWriteAppliesOnceAndGetsTheFirstAnswer() {
        KvStore kv = new KvStore();
        kv.apply(req(1, 1, new Op.Put("k", "v1")));
        Request cas = req(2, 1, new Op.Cas("k", "v1", "v2"));
        assertTrue(kv.apply(cas));
        kv.apply(req(1, 2, new Op.Put("k", "v1")));

        // The CAS reaches the log a second time: it must not run again, and it must still say true.
        assertTrue(kv.apply(cas));
        assertEquals("v1", kv.get("k"));
        assertTrue(kv.resultIfApplied(2, 1));
        assertNull(kv.resultIfApplied(2, 2));
    }

    @Test
    void anOlderRetryIsIgnored() {
        KvStore kv = new KvStore();
        kv.apply(req(1, 1, new Op.Put("k", "old")));
        kv.apply(req(1, 2, new Op.Put("k", "new")));
        assertNull(kv.apply(req(1, 1, new Op.Put("k", "old"))));
        assertEquals("new", kv.get("k"));
    }

    @Test
    void withoutDedupARetryRunsTwice() {
        KvStore kv = new KvStore(false);
        kv.apply(req(1, 1, new Op.Put("k", "a")));
        kv.apply(req(2, 1, new Op.Put("k", "b")));
        kv.apply(req(1, 1, new Op.Put("k", "a")));
        assertEquals("a", kv.get("k"));
    }

    @Test
    void snapshotRoundTripKeepsDataAndSessions() {
        KvStore kv = new KvStore();
        kv.apply(req(1, 1, new Op.Put("x", "1")));
        kv.apply(req(2, 7, new Op.Cas("y", null, "2")));
        byte[] snap = kv.snapshot();
        KvStore back = KvStore.restore(snap, true);
        assertEquals("1", back.get("x"));
        assertEquals("2", back.get("y"));
        assertTrue(back.resultIfApplied(2, 7));
        assertArrayEquals(snap, back.snapshot(), "equal stores encode to equal bytes");
    }
}
