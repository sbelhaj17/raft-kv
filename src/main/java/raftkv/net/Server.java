package raftkv.net;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import raftkv.kv.Replica;
import raftkv.kv.Request;
import raftkv.kv.Response;
import raftkv.raft.Message;
import raftkv.raft.RaftConfig;
import raftkv.storage.FileStorage;
import raftkv.wire.Codec;

/**
 * One node of the cluster as a TCP server. Peers and clients connect to the same port; a peer
 * says who it is in its first frame.
 *
 * <p>All Raft and state machine work happens on one thread, the event loop. Reader threads put
 * what arrives on a queue; the loop takes everything that is waiting, ticks Raft if a tick is
 * due, then calls {@link Replica#flush}, which writes the log, syncs it once, and hands
 * messages and replies to writer threads. Requests that arrive together therefore share one
 * sync, which is where most of the throughput comes from.
 */
public final class Server implements AutoCloseable {
    private record Incoming(Request request, ClientLink link) {}

    private final int id;
    private final Map<Integer, InetSocketAddress> cluster;
    private final long tickNanos;
    private final FileStorage storage;
    private final Replica replica;
    private final LinkedBlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
    private final Map<Integer, PeerLink> peers = new TreeMap<>();
    private final Map<Long, ClientLink> clients = new ConcurrentHashMap<>();
    private volatile boolean running = true;
    private ServerSocket listener;
    private Thread loop;

    public Server(int id, Map<Integer, InetSocketAddress> cluster, Path dataDir, RaftConfig raft,
                  Replica.Options opts, int tickMillis) {
        this(id, cluster, dataDir, raft, opts, tickMillis, true);
    }

    /** @param sync false makes storage skip its syncs; see {@link FileStorage#FileStorage(Path, boolean)}. */
    public Server(int id, Map<Integer, InetSocketAddress> cluster, Path dataDir, RaftConfig raft,
                  Replica.Options opts, int tickMillis, boolean sync) {
        this.id = id;
        this.cluster = cluster;
        this.tickNanos = TimeUnit.MILLISECONDS.toNanos(tickMillis);
        this.storage = new FileStorage(dataDir, sync);
        int[] members = cluster.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
        for (int p : members) if (p != id) peers.put(p, new PeerLink(cluster.get(p)));
        Replica.Outbox out = new Replica.Outbox() {
            @Override
            public void send(Message m) {
                peers.get(m.to()).send(Codec.encode(m));
            }

            @Override
            public void reply(Response r) {
                ClientLink c = clients.get(r.clientId());
                if (c != null) c.send(Codec.encode(r));
            }
        };
        this.replica = new Replica(id, members, raft, new SplittableRandom(System.nanoTime() ^ id), storage, out, opts,
                Replica.Observer.NONE);
    }

    public void start() throws IOException {
        listener = new ServerSocket();
        listener.setReuseAddress(true);
        listener.bind(cluster.get(id));
        for (PeerLink p : peers.values()) Thread.ofVirtual().start(p::run);
        Thread.ofVirtual().name("accept-" + id).start(this::acceptLoop);
        loop = Thread.ofPlatform().name("raft-" + id).start(this::eventLoop);
    }

    @Override
    public void close() throws IOException {
        running = false;
        listener.close();
        for (PeerLink p : peers.values()) p.close();
        for (ClientLink c : clients.values()) c.close();
        try {
            loop.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        storage.close();
    }

    // ---- the event loop ------------------------------------------------------------------------

    private void eventLoop() {
        long nextTick = System.nanoTime() + tickNanos;
        try {
            while (running) {
                long wait = nextTick - System.nanoTime();
                Object first = wait > 0 ? inbox.poll(wait, TimeUnit.NANOSECONDS) : inbox.poll();
                if (first != null) {
                    handle(first);
                    for (Object o; (o = inbox.poll()) != null; ) handle(o);
                }
                long now = System.nanoTime();
                if (now >= nextTick) {
                    replica.tick();
                    // after a long pause, do not fire a burst of ticks to catch up
                    nextTick = Math.max(nextTick + tickNanos, now);
                }
                replica.flush();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void handle(Object o) {
        switch (o) {
            case Message m -> replica.receive(m);
            case Incoming in -> {
                clients.put(in.request().clientId(), in.link());
                replica.request(in.request());
            }
            default -> throw new IllegalStateException("unexpected " + o);
        }
    }

    // ---- connections ---------------------------------------------------------------------------

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = listener.accept();
                s.setTcpNoDelay(true);
                Thread.ofVirtual().start(() -> serve(s));
            } catch (IOException e) {
                if (running) System.err.println("node " + id + ": accept failed: " + e.getMessage());
            }
        }
    }

    private void serve(Socket s) {
        ClientLink link = null;
        try (s) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream(), Frames.BUFFER));
            while (running) {
                switch (Codec.decode(Frames.read(in))) {
                    case Codec.Hello h -> { }  // a peer; what follows are Raft messages
                    case Message m -> inbox.add(m);
                    case Request r -> {
                        if (link == null) {
                            link = new ClientLink(s.getOutputStream());
                            Thread.ofVirtual().start(link::run);
                        }
                        inbox.add(new Incoming(r, link));
                    }
                    default -> throw new IOException("unexpected frame");
                }
            }
        } catch (IOException e) {
            // the other side went away
        } finally {
            if (link != null) link.close();
        }
    }

    /** Outgoing connection to one peer, reopened after failures. */
    private final class PeerLink extends Writer {
        private final InetSocketAddress addr;
        private Socket socket;
        private long retryAt;

        PeerLink(InetSocketAddress addr) {
            this.addr = addr;
        }

        @Override
        OutputStream connect() throws IOException {
            if (System.nanoTime() < retryAt) return null;
            try {
                socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.connect(addr, 200);
                OutputStream out = socket.getOutputStream();
                java.io.DataOutputStream d = new java.io.DataOutputStream(out);
                Frames.write(d, Codec.encode(new Codec.Hello(id)));
                d.flush();
                return out;
            } catch (IOException e) {
                disconnected();
                throw e;
            }
        }

        @Override
        void disconnected() {
            retryAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
            try {
                if (socket != null) socket.close();
            } catch (IOException ignored) {
            }
        }

        @Override
        void close() {
            super.close();
            disconnected();
        }
    }

    /** Replies to one client connection. */
    private static final class ClientLink extends Writer {
        private final OutputStream out;

        ClientLink(OutputStream out) {
            this.out = out;
        }

        @Override
        OutputStream connect() {
            return isClosed() ? null : out;
        }

        @Override
        void disconnected() {
            close();
        }
    }
}
