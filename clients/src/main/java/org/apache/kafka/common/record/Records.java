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

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

import org.apache.kafka.common.utils.Time;

/**
 * Interface for accessing the records contained in a log. The log itself is represented as a sequence of record
 * batches (see {@link RecordBatch}).
 *
 * For magic versions 1 and below, each batch consists of an 8 byte offset, a 4 byte record size, and a "shallow"
 * {@link Record record}. If the batch is not compressed, then each batch will have only the shallow record contained
 * inside it. If it is compressed, the batch contains "deep" records, which are packed into the value field of the
 * shallow record. To iterate over the shallow batches, use {@link #batches()}; for the deep records, use
 * {@link #records()}. Note that the deep iterator handles both compressed and non-compressed batches: if the batch is
 * not compressed, the shallow record is returned; otherwise, the shallow batch is decompressed and the deep records
 * are returned.
 *
 * For magic version 2, every batch contains 1 or more log record, regardless of compression. You can iterate
 * over the batches directly using {@link #batches()}. Records can be iterated either directly from an individual
 * batch or through {@link #records()}. Just as in previous versions, iterating over the records typically involves
 * decompression and should therefore be used with caution.
 *
 * See {@link MemoryRecords} for the in-memory representation and {@link FileRecords} for the on-disk representation.
 */
public interface Records {

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
     * Get the record batches. Note that the signature allows subclasses
     * to return a more specific batch type. This enables optimizations such as in-place offset
     * assignment (see for example {@link DefaultRecordBatch}), and partial reading of
     * record data (see {@link FileLogInputStream.FileChannelRecordBatch#magic()}.
     * @return An iterator over the record batches of the log
     */
    Iterable<? extends RecordBatch> batches();

    /**
     * Check whether all batches in this buffer have a certain magic value.
     * @param magic The magic value to check
     * @return true if all record batches have a matching magic value, false otherwise
     */
    boolean hasMatchingMagic(byte magic);

    /**
     * Check whether this log buffer has a magic value compatible with a particular value
     * (i.e. whether all message sets contained in the buffer have a matching or lower magic).
     * @param magic The magic version to ensure compatibility with
     * @return true if all batches have compatible magic, false otherwise
     */
    boolean hasCompatibleMagic(byte magic);

    /**
     * Convert all batches in this buffer to the format passed as a parameter. Note that this requires
     * deep iteration since all of the deep records must also be converted to the desired format.
     * @param toMagic The magic value to convert to
     * @param firstOffset The starting offset for returned records. This only impacts some cases. See
     *                    {@link AbstractRecords#downConvert(Iterable, byte, long, Time) for an explanation.
     * @param time instance used for reporting stats
     * @return A ConvertedRecords instance which may or may not contain the same instance in its records field.
     */
    ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time);

    /**
     * Get an iterator over the records in this log. Note that this generally requires decompression,
     * and should therefore be used with care.
     * @return The record iterator
     */
    Iterable<Record> records();
}
