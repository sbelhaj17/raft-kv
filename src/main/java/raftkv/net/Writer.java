package raftkv.net;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Sends frames on one connection from its own thread, so the event loop never blocks on a slow
 * or dead socket. Frames queued while the previous write was in progress go out in one flush.
 */
abstract class Writer {
    private final LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
    private volatile boolean closed;

    void send(byte[] frame) {
        if (!closed) queue.add(frame);
    }

    void close() {
        closed = true;
        queue.add(new byte[0]);  // wake the thread
    }

    boolean isClosed() {
        return closed;
    }

    /** Where to write; called again after a failure. Return null to drop what is queued for now. */
    abstract OutputStream connect() throws IOException;

    abstract void disconnected();

    void run() {
        DataOutputStream out = null;
        List<byte[]> batch = new ArrayList<>();
        while (!closed) {
            try {
                batch.clear();
                batch.add(queue.take());
                queue.drainTo(batch);
                if (closed) break;
                if (out == null) {
                    OutputStream raw = connect();
                    if (raw == null) continue;
                    out = new DataOutputStream(new BufferedOutputStream(raw, Frames.BUFFER));
                }
                for (byte[] f : batch) if (f.length > 0) Frames.write(out, f);
                out.flush();
            } catch (InterruptedException e) {
                return;
            } catch (IOException e) {
                // Raft copes with lost messages; drop this batch and reconnect on the next one.
                out = null;
                disconnected();
            }
        }
    }
}
