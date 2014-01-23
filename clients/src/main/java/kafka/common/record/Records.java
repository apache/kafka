package kafka.common.record;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * A binary format which consists of a 4 byte size, an 8 byte offset, and the record bytes. See {@link MemoryRecords}
 * for the in-memory representation.
 */
public interface Records extends Iterable<LogEntry> {

    int SIZE_LENGTH = 4;
    int OFFSET_LENGTH = 8;
    int LOG_OVERHEAD = SIZE_LENGTH + OFFSET_LENGTH;

    /**
     * Write these records to the given channel
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException If the write fails.
     */
    public int writeTo(GatheringByteChannel channel) throws IOException;

    /**
     * The size of these records in bytes
     */
    public int sizeInBytes();

}
