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

/**
 * Base interface for accessing the records contained in a log. The log itself is represented as a sequence of record
 * batches (see {@link RecordBatch}). There are two types of {@link Records} - {@link ReadableRecords} and
 * {@link WriteableRecords}. {@link ReadableRecords} provides methods for reading the underlying records, while
 * {@link WriteableRecords} provides methods to write records out to a channel.
 *
 * For magic versions 1 and below, each batch consists of an 8 byte offset, a 4 byte record size, and a "shallow"
 * {@link Record record}. If the batch is not compressed, then each batch will have only the shallow record contained
 * inside it. If it is compressed, the batch contains "deep" records, which are packed into the value field of the
 * shallow record. To iterate over the shallow batches, use {@link ReadableRecords#batches()}; for the deep records, use
 * {@link ReadableRecords#records()}. Note that the deep iterator handles both compressed and non-compressed batches:
 * if the batch is not compressed, the shallow record is returned; otherwise, the shallow batch is decompressed and the
 * deep records are returned.
 *
 * For magic version 2, every batch contains 1 or more log record, regardless of compression. You can iterate
 * over the batches directly using {@link ReadableRecords#batches()}. Records can be iterated either directly from an individual
 * batch or through {@link ReadableRecords#records()}. Just as in previous versions, iterating over the records typically involves
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
}
