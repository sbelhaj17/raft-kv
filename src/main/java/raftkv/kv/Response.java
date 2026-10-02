package raftkv.kv;

/**
 * @param value  Get: the value, or null if absent. Status: a human-readable description.
 * @param ok     Delete: the key existed. Cas: the swap happened. Otherwise true.
 * @param leader for NOT_LEADER, the node this one believes is leader, or 0 if it does not know
 */
public record Response(long clientId, long seq, Status status, String value, boolean ok, int leader) {
    public enum Status { OK, NOT_LEADER }

    public static Response ok(Request r, String value, boolean ok) {
        return new Response(r.clientId(), r.seq(), Status.OK, value, ok, 0);
    }

    public static Response notLeader(Request r, int leader) {
        return new Response(r.clientId(), r.seq(), Status.NOT_LEADER, null, false, leader);
    }
}
