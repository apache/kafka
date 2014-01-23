package kafka.common.network;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * A send backed by an array of byte buffers
 */
public class ByteBufferSend implements Send {

    private final int destination;
    protected final ByteBuffer[] buffers;
    private int remaining;

    public ByteBufferSend(int destination, ByteBuffer... buffers) {
        super();
        this.destination = destination;
        this.buffers = buffers;
        for (int i = 0; i < buffers.length; i++)
            remaining += buffers[i].remaining();
    }

    @Override
    public int destination() {
        return destination;
    }

    @Override
    public boolean complete() {
        return remaining > 0;
    }

    @Override
    public ByteBuffer[] reify() {
        return this.buffers;
    }

    @Override
    public int remaining() {
        return this.remaining;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("This shouldn't happen.");
        remaining -= written;
        return written;
    }

}
