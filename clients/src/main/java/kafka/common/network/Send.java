package kafka.common.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * This interface models the in-progress sending of data to a destination identified by an integer id.
 */
public interface Send {

    /**
     * The numeric id for the destination of this send
     */
    public int destination();

    /**
     * The number of bytes remaining to send
     */
    public int remaining();

    /**
     * Is this send complete?
     */
    public boolean complete();

    /**
     * An optional method to turn this send into an array of ByteBuffers if possible (otherwise returns null)
     */
    public ByteBuffer[] reify();

    /**
     * Write some as-yet unwritten bytes from this send to the provided channel. It may take multiple calls for the send
     * to be completely written
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException If the write fails
     */
    public long writeTo(GatheringByteChannel channel) throws IOException;

}
