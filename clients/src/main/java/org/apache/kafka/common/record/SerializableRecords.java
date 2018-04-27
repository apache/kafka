package org.apache.kafka.common.record;

import org.apache.kafka.common.TopicPartitionRecordsStats;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

public interface SerializableRecords {
    int OFFSET_OFFSET = 0;
    int OFFSET_LENGTH = 8;
    int SIZE_OFFSET = OFFSET_OFFSET + OFFSET_LENGTH;
    int SIZE_LENGTH = 4;
    int LOG_OVERHEAD = SIZE_OFFSET + SIZE_LENGTH;

    // the magic offset is at the same offset for all current message formats, but the 4 bytes
    // between the size and the magic is dependent on the version.
    int MAGIC_OFFSET = 16;
    int MAGIC_LENGTH = 1;
    int HEADER_SIZE_UP_TO_MAGIC = MAGIC_OFFSET + MAGIC_LENGTH;

    /**
     * The size of these records in bytes.
     * @return The size in bytes of the records
     */
    int sizeInBytes();

    /**
     * Attempts to write the contents of this buffer to a channel.
     * @param channel The channel to write to
     * @param position The position in the buffer to write from
     * @param length The number of bytes to write
     * @return The number of bytes actually written
     * @throws IOException For any IO errors
     */
    long writeTo(GatheringByteChannel channel, long position, int length) throws IOException;

    /**
     * Get records processing statistics, if there are any that were captured while processing records in this instance.
     * @return Records processing statistics for this instance.
     */
    TopicPartitionRecordsStats recordsProcessingStats();
}
