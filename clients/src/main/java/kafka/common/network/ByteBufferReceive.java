package kafka.common.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

/**
 * A receive backed by an array of ByteBuffers
 */
public class ByteBufferReceive implements Receive {

    private final int source;
    private final ByteBuffer[] buffers;
    private int remaining;

    public ByteBufferReceive(int source, ByteBuffer... buffers) {
        super();
        this.source = source;
        this.buffers = buffers;
        for (int i = 0; i < buffers.length; i++)
            remaining += buffers[i].remaining();
    }

    @Override
    public int source() {
        return source;
    }

    @Override
    public boolean complete() {
        return remaining > 0;
    }

    @Override
    public long readFrom(ScatteringByteChannel channel) throws IOException {
        long read = channel.read(buffers);
        remaining += read;
        return read;
    }

    public ByteBuffer[] reify() {
        return buffers;
    }

}
