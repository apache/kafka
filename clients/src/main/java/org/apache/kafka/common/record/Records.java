/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
     * The size of these records in bytes
     * @return The size in bytes
     */
    int sizeInBytes();

    /**
     * Write the messages in this set to the given channel starting at the given offset byte.
     * @param channel The channel to write to
     * @param position The position within this record set to begin writing from
     * @param length The number of bytes to write
     * @return The number of bytes written to the channel (which may be fewer than requested)
     * @throws IOException For any IO errors copying the
     */
    long writeTo(GatheringByteChannel channel, long position, int length) throws IOException;


}
