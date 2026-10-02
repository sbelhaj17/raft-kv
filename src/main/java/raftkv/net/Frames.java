package raftkv.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Every value on a socket is a frame: a four-byte length, then that many bytes of {@link raftkv.wire.Codec} output. */
final class Frames {
    private static final int MAX = 64 << 20;

    /**
     * Stream buffer size for one connection, each way. Frames bigger than this bypass the buffer,
     * so it only has to fit the small ones. Every client connection holds two of these, and with
     * 64 KB buffers 8,192 clients took a node past its 512 MB heap.
     */
    static final int BUFFER = 8 << 10;

    private Frames() {}

    static void write(DataOutputStream out, byte[] payload) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
    }

    static byte[] read(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > MAX) throw new IOException("bad frame length " + n);
        byte[] b = new byte[n];
        in.readFully(b);
        return b;
    }
}
