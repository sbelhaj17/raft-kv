package raftkv.sim;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import raftkv.kv.Op;

/**
 * Checks that a history of key-value operations is linearizable: that there is one order of all
 * operations, consistent with real time, in which every result is what a single map would have
 * returned.
 *
 * <p>Every operation touches one key, and linearizability is compositional, so each key is
 * checked on its own as a register. The search is the Wing and Gong algorithm with Lowe's
 * memoization (as in Porcupine): walk the history in time order, try to linearize any operation
 * that has been called and not yet linearized, and backtrack when an operation's return is
 * reached before it could be placed. A set of (linearized operations, register value) pairs
 * already explored keeps it from redoing work.
 */
public final class Linearizability {

    /**
     * One client operation. {@code ret} is {@link Long#MAX_VALUE} if the client never heard
     * back; such a write may have taken effect at any point after its call, or not at all.
     *
     * @param value for a Get, the value returned (null: absent)
     * @param ok    for a Delete or Cas, the result; null if unknown
     */
    public record Operation(int client, Op op, long call, long ret, String value, Boolean ok) {
        public boolean completed() {
            return ret != Long.MAX_VALUE;
        }
    }

    public record Result(boolean ok, String key, int operations) {
        static final Result OK = new Result(true, null, 0);
    }

    private static final Object INVALID = new Object();

    private Linearizability() {}

    public static Result check(List<Operation> history) {
        Map<String, List<Operation>> byKey = new LinkedHashMap<>();
        for (Operation o : history) {
            // A read that never returned tells us nothing.
            if (o.op() instanceof Op.Get && !o.completed()) continue;
            byKey.computeIfAbsent(o.op().key(), k -> new ArrayList<>()).add(o);
        }
        for (var e : byKey.entrySet()) {
            if (!checkRegister(e.getValue())) return new Result(false, e.getKey(), e.getValue().size());
        }
        return Result.OK;
    }

    /** The register's next value, or INVALID if {@code o}'s result is impossible from {@code state}. */
    static Object step(String state, Operation o) {
        return switch (o.op()) {
            case Op.Get g -> Objects.equals(o.value(), state) ? state : INVALID;
            case Op.Put p -> p.value();
            case Op.Delete d -> o.ok() != null && o.ok() != (state != null) ? INVALID : null;
            case Op.Cas c -> {
                boolean swaps = Objects.equals(state, c.expected());
                if (o.ok() != null && o.ok() != swaps) yield INVALID;
                yield swaps ? c.value() : state;
            }
            case Op.Status s -> throw new IllegalArgumentException("status is not part of the model");
        };
    }

    /** A call or return event in a doubly linked list that operations are lifted out of and put back into. */
    private static final class Node {
        final int op;
        final long time;
        final boolean call;
        Node match;
        Node prev;
        Node next;

        Node(int op, long time, boolean call) {
            this.op = op;
            this.time = time;
            this.call = call;
        }
    }

    private record Frame(Node call, String state) {}

    private record Seen(BitSet linearized, String state) {}

    static boolean checkRegister(List<Operation> ops) {
        List<Node> events = new ArrayList<>(2 * ops.size());
        for (int i = 0; i < ops.size(); i++) {
            Node c = new Node(i, ops.get(i).call(), true);
            Node r = new Node(i, ops.get(i).ret(), false);
            c.match = r;
            events.add(c);
            events.add(r);
        }
        // Times are unique except for the MAX_VALUE of unfinished operations; break ties with
        // calls first so an operation is never seen to return before it starts.
        events.sort((a, b) -> a.time != b.time ? Long.compare(a.time, b.time) : Boolean.compare(b.call, a.call));
        Node head = new Node(-1, Long.MIN_VALUE, false);
        Node prev = head;
        for (Node n : events) {
            prev.next = n;
            n.prev = prev;
            prev = n;
        }

        BitSet linearized = new BitSet(ops.size());
        Set<Seen> seen = new HashSet<>();
        ArrayDeque<Frame> stack = new ArrayDeque<>();
        String state = null;
        Node entry = head.next;
        while (head.next != null) {
            if (entry.call) {
                Object next = step(state, ops.get(entry.op));
                if (next != INVALID) {
                    BitSet after = (BitSet) linearized.clone();
                    after.set(entry.op);
                    if (seen.add(new Seen(after, (String) next))) {
                        stack.push(new Frame(entry, state));
                        state = (String) next;
                        linearized.set(entry.op);
                        lift(entry);
                        entry = head.next;
                        continue;
                    }
                }
                entry = entry.next;
            } else {
                // We reached the return of an operation we could not linearize before it: undo
                // the most recent choice and try the next candidate after it.
                if (stack.isEmpty()) return false;
                Frame f = stack.pop();
                state = f.state();
                linearized.clear(f.call().op);
                unlift(f.call());
                entry = f.call().next;
            }
        }
        return true;
    }

    private static void lift(Node call) {
        call.prev.next = call.next;
        if (call.next != null) call.next.prev = call.prev;
        Node ret = call.match;
        ret.prev.next = ret.next;
        if (ret.next != null) ret.next.prev = ret.prev;
    }

    private static void unlift(Node call) {
        Node ret = call.match;
        ret.prev.next = ret;
        if (ret.next != null) ret.next.prev = ret;
        call.prev.next = call;
        if (call.next != null) call.next.prev = call;
    }
}
