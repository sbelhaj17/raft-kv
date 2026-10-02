package raftkv.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Starts a cluster on this machine with each node in its own JVM, so that killing a node is a
 * real process kill: no clean shutdown, no flushing, the socket just goes away.
 */
public final class LocalCluster implements AutoCloseable {
    private final Path dir;
    private final Map<Integer, InetSocketAddress> addrs = new TreeMap<>();
    private final Map<Integer, Process> procs = new TreeMap<>();
    private final String spec;
    private final boolean sync;

    public LocalCluster(int nodes, int basePort, Path dir, boolean sync) throws IOException {
        this.dir = dir;
        this.sync = sync;
        Files.createDirectories(dir);
        for (int i = 1; i <= nodes; i++) addrs.put(i, new InetSocketAddress("127.0.0.1", basePort + i));
        this.spec = addrs.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue().getHostString() + ":" + e.getValue().getPort())
                .collect(Collectors.joining(","));
        for (int id : addrs.keySet()) start(id);
    }

    public Map<Integer, InetSocketAddress> addresses() {
        return addrs;
    }

    public void start(int id) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder pb = new ProcessBuilder(java, "-Xmx512m", "-cp", System.getProperty("java.class.path"),
                "raftkv.Main", "node", "--id", String.valueOf(id), "--cluster", spec,
                "--data", dir.resolve("node" + id).toString(), "--sync", String.valueOf(sync));
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(dir.resolve("node" + id + ".log").toFile()));
        procs.put(id, pb.start());
    }

    /** SIGKILL, so the node gets no chance to clean up. */
    public void kill(int id) throws InterruptedException {
        Process p = procs.remove(id);
        if (p != null) {
            p.destroyForcibly();
            p.waitFor();
        }
    }

    @Override
    public void close() throws InterruptedException {
        for (int id : new TreeMap<>(procs).keySet()) kill(id);
    }
}
