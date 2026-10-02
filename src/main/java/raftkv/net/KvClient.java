package raftkv.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import raftkv.kv.Op;
import raftkv.kv.Request;
import raftkv.kv.Response;
import raftkv.raft.RaftNode;
import raftkv.wire.Codec;

/**
 * A blocking client with one request in flight. It sends to the node it believes is leader,
 * follows NOT_LEADER hints, and on a timeout or a dead connection retries the same request (same
 * seq) on the next node; the server's client sessions make that retry safe for writes.
 */
public final class KvClient implements AutoCloseable {
    private final long id;
    private final Map<Integer, InetSocketAddress> nodes;
    private final List<Integer> ids;
    private final int timeoutMillis;
    private long seq;
    private int target;

    private Socket socket;
    private int connectedTo = RaftNode.NONE;
    private DataInputStream in;
    private DataOutputStream out;

    public KvClient(long id, Map<Integer, InetSocketAddress> nodes, int timeoutMillis) {
        this.id = id;
        this.nodes = new TreeMap<>(nodes);
        this.ids = new ArrayList<>(this.nodes.keySet());
        this.timeoutMillis = timeoutMillis;
        this.target = ids.get((int) (Math.abs(id) % ids.size()));
    }

    public Response put(String key, String value) throws IOException {
        return call(new Op.Put(key, value));
    }

    public String get(String key) throws IOException {
        return call(new Op.Get(key)).value();
    }

    public boolean delete(String key) throws IOException {
        return call(new Op.Delete(key)).ok();
    }

    public boolean cas(String key, String expected, String value) throws IOException {
        return call(new Op.Cas(key, expected, value)).ok();
    }

    /** Ask one particular node about itself. */
    public String status(int node) throws IOException {
        Request r = new Request(id, ++seq, new Op.Status());
        connect(node);
        Frames.write(out, Codec.encode(r));
        out.flush();
        return await(r).value();
    }

    /** Send until some node answers OK. Gives up after 30 seconds. */
    public Response call(Op op) throws IOException {
        Request r = new Request(id, ++seq, op);
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (System.nanoTime() < deadline) {
            try {
                connect(target);
                Frames.write(out, Codec.encode(r));
                out.flush();
                Response resp = await(r);
                if (resp.status() == Response.Status.OK) return resp;
                if (resp.leader() != RaftNode.NONE && resp.leader() != target) {
                    target = resp.leader();
                } else {
                    // nobody knows the leader yet: an election is going on
                    target = next(target);
                    pause(10);
                }
            } catch (IOException e) {
                disconnect();
                target = next(target);
                pause(5);
            }
        }
        throw new IOException("no node answered " + op + " within 30 s");
    }

    private Response await(Request r) throws IOException {
        socket.setSoTimeout(timeoutMillis);
        while (true) {
            Object o;
            try {
                o = Codec.decode(Frames.read(in));
            } catch (SocketTimeoutException e) {
                throw new IOException("timed out waiting for node " + connectedTo, e);
            }
            if (o instanceof Response resp && resp.seq() == r.seq()) return resp;
        }
    }

    private void connect(int node) throws IOException {
        if (connectedTo == node && socket != null) return;
        disconnect();
        Socket s = new Socket();
        s.setTcpNoDelay(true);
        s.connect(nodes.get(node), 500);
        socket = s;
        connectedTo = node;
        in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
    }

    private void disconnect() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
        socket = null;
        connectedTo = RaftNode.NONE;
    }

    private int next(int node) {
        return ids.get((ids.indexOf(node) + 1) % ids.size());
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        disconnect();
    }
}
