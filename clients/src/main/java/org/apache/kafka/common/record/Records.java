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
import java.util.Iterator;

/**
 * A log buffer is a sequence of log entries. Each log entry consists of a 4 byte size, an 8 byte offset,
 * and the record bytes. See {@link MemoryRecords} for the in-memory representation.
 */
public interface Records {

    int OFFSET_OFFSET = 0;
    int OFFSET_LENGTH = 8;
    int SIZE_OFFSET = OFFSET_OFFSET + OFFSET_LENGTH;
    int SIZE_LENGTH = 4;
    int LOG_OVERHEAD = SIZE_OFFSET + SIZE_LENGTH;

    /**
     * The size of these records in bytes.
     * @return The size in bytes of the entries
     */
    int sizeInBytes();

    /**
     * Write the contents of this buffer to a channel.
     * @param channel The channel to write to
     * @param position The position in the buffer to write from
     * @param length The number of bytes to write
     * @return The number of bytes written
     * @throws IOException For any IO errors
     */
    long writeTo(GatheringByteChannel channel, long position, int length) throws IOException;

    /**
     * Get the shallow log entries in this log buffer.
     * @return An iterator over the shallow entries of the log
     */
    Iterator<? extends LogEntry> shallowIterator();

    /**
     * Get the deep log entries (i.e. descend into compressed message sets)
     * @return An iterator over the deep entries of the log
     */
    Iterator<LogEntry> deepIterator();

    /**
     * Check whether all entries in this buffer have a certain magic value.
     * @param magic The magic value to check
     * @return true if all shallow entries have a matching magic value, false otherwise
     */
    boolean hasMatchingShallowMagic(byte magic);


    /**
     * Convert all entries in this buffer to a certain magic value.
     * @param toMagic The magic value to convert to
     * @return A Records (which may or may not be the same instance)
     */
    Records toMessageFormat(byte toMagic);

}
