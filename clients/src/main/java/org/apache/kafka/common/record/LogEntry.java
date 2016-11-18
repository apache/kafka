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

import java.nio.ByteBuffer;

/**
 * An offset and record pair
 */
public interface LogEntry extends Iterable<LogRecord> {

    /**
     * Check whether the checksum of this entry is correct.
     *
     * @return true If so, false otherwise
     */
    boolean isValid();

    /**
     * Raise an exception if the checksum is not valid.
     */
    void ensureValid();

    /**
     * Get the checksum of this entry, which covers the entry header as well as all of the records.
     *
     * @return The 4-byte unsigned checksum represented as a long
     */
    long checksum();

    /**
     * Get the timestamp of this entry. This is the max timestamp among all records contained in this log entry.
     *
     * TODO: rename to maxTimestamp()?
     * @return The max timestamp
     */
    long timestamp();

    /**
     * Get the timestamp type of this entry. This will be {@link TimestampType#CREATE_TIME}
     * if the message has been up-converted from magic 0.
     * @return The timestamp type
     */
    TimestampType timestampType();

    /**
     * Get the first offset contained in this log entry. For magic version prior to 2, this generally
     * requires deep iteration and will return the offset of the first record in the message set. For
     * magic version 2 and above, this will return the first offset of the original message set (i.e.
     * prior to compaction). For non-compacted topics, the behavior is equivalent.
     *
     * Because this requires deep iteration for older magic versions, this method should be used with
     * caution. Generally {@link #lastOffset()} is safer since access is efficient for all magic versions.
     *
     * @return The base offset of this message set (which may or may not be the offset of the first record
     *         as described above).
     */
    long baseOffset();

    /**
     * Get the offset following this entry (i.e. the last offset contained in this entry plus one).
     * @return the next consecutive offset following this entry
     */
    long lastOffset();

    /**
     * Get the next consecutive offset following the records in this log entry.
     * @return
     */
    long nextOffset();

    /**
     * Get the message format version of this entry (i.e its magic value).
     * @return the magic byte
     */
    byte magic();

    /**
     * Get the PID (producer ID) for this log entry. For older magic versions, this will return 0.
     *
     * TODO: Maybe use -1 as sentinel instead of 0?
     * @return The PID or 0 if there is none
     */
    long pid();

    /**
     * Get the producer epoch for this log entry.
     *
     * @return The producer epoch, or 0 if there is none
     */
    short epoch();

    /**
     * Get the first sequence number of this message set.
     * @return The first sequence number
     */
    int firstSequence();

    /**
     * Get the last sequence number of this message set.
     *
     * @return The last sequence number
     */
    int lastSequence();

    /**
     * Get the compression type of this log entry
     *
     * @return The compression type
     */
    CompressionType compressionType();

    /**
     * Get the size in bytes of this entry, including the size of the record and the log overhead.
     * @return The size in bytes of this entry
     */
    int sizeInBytes();

    /**
     * Check whether this entry contains a compressed message set.
     * @return true if so, false otherwise
     */
    boolean isCompressed();

    /**
     * Write this entry into a buffer.
     * @param buffer The buffer to write the entry to
     */
    void writeTo(ByteBuffer buffer);


    /**
     * A mutable log entry is one that can be modified in place (without copying).
     */
    interface MutableLogEntry extends LogEntry {
        void setOffset(long offset);

        void setCreateTime(long timestamp);

        void setLogAppendTime(long timestamp);
    }

}
