package raftkv.kv;

/**
 * A client request. {@code seq} numbers a client's requests 1, 2, 3, ...; a client has at most
 * one request outstanding and retries it with the same seq, which is what lets the state
 * machine recognise a retry of a write it has already applied.
 */
public record Request(long clientId, long seq, Op op) {}
