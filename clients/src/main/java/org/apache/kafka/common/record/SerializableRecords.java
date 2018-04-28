/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.record;

import org.apache.kafka.common.TopicPartitionRecordsStats;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * Interface defining serialization primitives for records. This low-level abstraction is exposed for cases when we
 * want to write out records over the network. For all other use cases, see the more generic {@link Records}
 * implementation.
 */
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
     * Get records processing statistics, if there are any. This is typically used for cases when records require
     * additional processing while serializing, for example for {@link LazyDownConversionRecords}.
     * @return Processing statistics for enclosed records
     */
    TopicPartitionRecordsStats recordsProcessingStats();
}
