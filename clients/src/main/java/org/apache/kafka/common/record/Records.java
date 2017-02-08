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
 * Interface for accessing the records contained in a log. The log itself is represented as a sequence of log entries.
 * Each log entry consists of an 8 byte offset, a 4 byte record size, and a "shallow" {@link Record record}.
 * If the entry is not compressed, then each entry will have only the shallow record contained inside it. If it is
 * compressed, the entry contains "deep" records, which are packed into the value field of the shallow record. To iterate
 * over the shallow records, use {@link #shallowEntries()}; for the deep records, use {@link #deepEntries()}. Note
 * that the deep iterator handles both compressed and non-compressed entries: if the entry is not compressed, the
 * shallow record is returned; otherwise, the shallow record is decompressed and the deep entries are returned.
 * See {@link MemoryRecords} for the in-memory representation and {@link FileRecords} for the on-disk representation.
 */
public interface Records {

    int OFFSET_OFFSET = 0;
    int OFFSET_LENGTH = 8;
    int SIZE_OFFSET = OFFSET_OFFSET + OFFSET_LENGTH;
    int SIZE_LENGTH = 4;
    int LOG_OVERHEAD = SIZE_OFFSET + SIZE_LENGTH;

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
     * Get the shallow log entries in this log buffer. Note that the signature allows subclasses
     * to return a more specific log entry type. This enables optimizations such as in-place offset
     * assignment (see {@link ByteBufferLogInputStream.ByteBufferLogEntry}), and partial reading of
     * record data (see {@link FileLogInputStream.FileChannelLogEntry#magic()}.
     * @return An iterator over the shallow entries of the log
     */
    Iterable<? extends LogEntry> shallowEntries();

    /**
     * Get the deep log entries (i.e. descend into compressed message sets). For the deep records,
     * there are fewer options for optimization since the data must be decompressed before it can be
     * returned. Hence there is little advantage in allowing subclasses to return a more specific type
     * as we do for {@link #shallowEntries()}.
     * @return An iterator over the deep entries of the log
     */
    Iterable<LogEntry> deepEntries();

    /**
     * Check whether all shallow entries in this buffer have a certain magic value.
     * @param magic The magic value to check
     * @return true if all shallow entries have a matching magic value, false otherwise
     */
    boolean hasMatchingShallowMagic(byte magic);


    /**
     * Convert all entries in this buffer to the format passed as a parameter. Note that this requires
     * deep iteration since all of the deep records must also be converted to the desired format.
     * @param toMagic The magic value to convert to
     * @param upconvertTimestampType The timestamp type to use if up-converting from magic 0
     * @return A Records (which may or may not be the same instance)
     */
    Records toMessageFormat(byte toMagic, TimestampType upconvertTimestampType);

}
