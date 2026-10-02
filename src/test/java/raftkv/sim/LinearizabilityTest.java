package raftkv.sim;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import raftkv.kv.Op;
import raftkv.sim.Linearizability.Operation;

class LinearizabilityTest {
    private static final long NEVER = Long.MAX_VALUE;

    private static Operation put(int client, String v, long call, long ret) {
        return new Operation(client, new Op.Put("k", v), call, ret, null, null);
    }

    private static Operation get(int client, String seen, long call, long ret) {
        return new Operation(client, new Op.Get("k"), call, ret, seen, null);
    }

    private static Operation cas(int client, String expected, String v, boolean ok, long call, long ret) {
        return new Operation(client, new Op.Cas("k", expected, v), call, ret, null, ok);
    }

    private static Operation del(int client, boolean existed, long call, long ret) {
        return new Operation(client, new Op.Delete("k"), call, ret, null, existed);
    }

    private static boolean ok(Operation... ops) {
        return Linearizability.check(List.of(ops)).ok();
    }

    @Test
    void sequentialHistories() {
        assertTrue(ok(put(1, "a", 0, 1), get(1, "a", 2, 3)));
        assertFalse(ok(put(1, "a", 0, 1), get(1, "b", 2, 3)));
        assertTrue(ok(get(1, null, 0, 1)));
        assertFalse(ok(put(1, "a", 0, 1), get(1, null, 2, 3)));
    }

    @Test
    void aReadAfterAWriteFinishedMustSeeIt() {
        // the put returned at 1, the get started at 2: the old value is not allowed
        assertFalse(ok(put(1, "a", 0, 1), put(2, "b", 2, 5), get(3, "a", 6, 7)));
        // but a get that overlaps the put may see either value
        assertTrue(ok(put(1, "a", 0, 1), put(2, "b", 2, 5), get(3, "a", 3, 4)));
        assertTrue(ok(put(1, "a", 0, 1), put(2, "b", 2, 5), get(3, "b", 3, 4)));
    }

    @Test
    void readsMustNotGoBackInTime() {
        // two writes overlap; once a reader has seen b, a later reader must not see a again
        assertTrue(ok(put(1, "a", 0, 10), put(2, "b", 0, 10), get(3, "b", 1, 2), get(4, "b", 3, 4)));
        assertFalse(ok(put(1, "a", 0, 1), put(2, "b", 2, 10), get(3, "b", 3, 4), get(4, "a", 5, 6)));
    }

    @Test
    void compareAndSetAndDelete() {
        assertTrue(ok(put(1, "a", 0, 1), cas(2, "a", "b", true, 2, 3), get(3, "b", 4, 5)));
        assertFalse(ok(put(1, "a", 0, 1), cas(2, "x", "b", true, 2, 3)));
        assertFalse(ok(put(1, "a", 0, 1), cas(2, "a", "b", false, 2, 3)));
        assertTrue(ok(put(1, "a", 0, 1), del(2, true, 2, 3), get(3, null, 4, 5)));
        assertFalse(ok(del(1, true, 0, 1)));
    }

    @Test
    void aWriteThatNeverReturnedMayOrMayNotHaveHappened() {
        assertTrue(ok(put(1, "a", 0, NEVER), get(2, "a", 5, 6)));
        assertTrue(ok(put(1, "a", 0, NEVER), get(2, null, 5, 6)));
        // but it cannot have happened before it was sent
        assertFalse(ok(get(2, "a", 0, 1), put(1, "a", 5, NEVER)));
    }

    @Test
    void keysAreCheckedSeparately() {
        Operation a = new Operation(1, new Op.Put("x", "1"), 0, 1, null, null);
        Operation b = new Operation(2, new Op.Get("y"), 2, 3, "1", null);
        assertFalse(Linearizability.check(List.of(a, b)).ok());
    }

    @Test
    void largeConcurrentHistoryFinishesQuickly() {
        // 8 clients each writing then reading their own value, all overlapping: linearizable,
        // and enough branching that the search would blow up without memoization
        List<Operation> ops = new ArrayList<>();
        for (int c = 0; c < 8; c++) {
            for (int i = 0; i < 25; i++) {
                long t = i * 100L + c;
                ops.add(put(c, c + "." + i, t, t + 50));
            }
        }
        assertTrue(Linearizability.check(ops).ok());
    }
}
