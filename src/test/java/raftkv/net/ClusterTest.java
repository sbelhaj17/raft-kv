package raftkv.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import raftkv.kv.Replica;
import raftkv.raft.RaftConfig;

/** Three real servers on localhost sockets with real files, in one JVM. */
class ClusterTest {
    @TempDir
    Path dir;

    private final Map<Integer, InetSocketAddress> addrs = new TreeMap<>();
    private final Map<Integer, Server> servers = new TreeMap<>();

    private void startAll() throws IOException {
        for (int i = 1; i <= 3; i++) addrs.put(i, new InetSocketAddress("127.0.0.1", freePort()));
        for (int id : addrs.keySet()) start(id);
    }

    private void start(int id) throws IOException {
        Server s = new Server(id, addrs, dir.resolve("node" + id), RaftConfig.defaults(), Replica.Options.defaults(), 10);
        s.start();
        servers.put(id, s);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private int leader(KvClient c) throws Exception {
        for (int attempt = 0; attempt < 300; attempt++) {
            for (int id : servers.keySet()) {
                try {
                    if (c.status(id).contains("role=LEADER")) return id;
                } catch (IOException e) {
                    // not reachable yet
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("no leader");
    }

    @AfterEach
    void stop() throws IOException {
        for (Server s : servers.values()) s.close();
    }

    @Test
    void putGetDeleteCas() throws Exception {
        startAll();
        try (KvClient c = new KvClient(1, addrs, 1000)) {
            c.put("a", "1");
            assertEquals("1", c.get("a"));
            assertTrue(c.cas("a", "1", "2"));
            assertFalse(c.cas("a", "1", "3"));
            assertEquals("2", c.get("a"));
            assertTrue(c.delete("a"));
            assertNull(c.get("a"));
            assertFalse(c.delete("a"));
        }
    }

    @Test
    void writesSurviveTheLeaderGoingAwayAndComingBack() throws Exception {
        startAll();
        try (KvClient c = new KvClient(2, addrs, 500)) {
            for (int i = 0; i < 50; i++) c.put("k" + i, "v" + i);

            int old = leader(c);
            servers.remove(old).close();
            for (int i = 50; i < 100; i++) c.put("k" + i, "v" + i);
            int now = leader(c);
            assertTrue(now != old);

            // the old leader restarts from its files and catches up
            start(old);
            for (int i = 0; i < 100; i++) assertEquals("v" + i, c.get("k" + i));
            String status = null;
            for (int attempt = 0; attempt < 200; attempt++) {
                status = c.status(old);
                if (status.contains("applied=") && applied(status) >= applied(c.status(now))) break;
                Thread.sleep(20);
            }
            assertNotNull(status);
            assertTrue(applied(status) >= 100, status);
        }
    }

    private static long applied(String status) {
        for (String part : status.split(" ")) if (part.startsWith("applied=")) return Long.parseLong(part.substring(8));
        return -1;
    }
}
