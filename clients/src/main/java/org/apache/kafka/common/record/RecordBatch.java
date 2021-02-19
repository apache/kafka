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

import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * A record batch is a container for records. In old versions of the record format (versions 0 and 1),
 * a batch consisted always of a single record if no compression was enabled, but could contain
 * many records otherwise. Newer versions (magic versions 2 and above) will generally contain many records
 * regardless of compression.
 */
public interface RecordBatch extends Iterable<Record> {

    /**
     * The "magic" values
     */
    byte MAGIC_VALUE_V0 = 0;
    byte MAGIC_VALUE_V1 = 1;
    byte MAGIC_VALUE_V2 = 2;

    /**
     * The current "magic" value
     */
    byte CURRENT_MAGIC_VALUE = MAGIC_VALUE_V2;

    /**
     * Timestamp value for records without a timestamp
     */
    long NO_TIMESTAMP = -1L;

    /**
     * Values used in the v2 record format by non-idempotent/non-transactional producers or when
     * up-converting from an older format.
     */
    long NO_PRODUCER_ID = -1L;
    short NO_PRODUCER_EPOCH = -1;
    int NO_SEQUENCE = -1;

    /**
     * Used to indicate an unknown leader epoch, which will be the case when the record set is
     * first created by the producer.
     */
    int NO_PARTITION_LEADER_EPOCH = -1;

    /**
     * Check whether the checksum of this batch is correct.
     *
     * @return true If so, false otherwise
     */
    boolean isValid();

    /**
     * Raise an exception if the checksum is not valid.
     */
    void ensureValid();

    /**
     * Get the checksum of this record batch, which covers the batch header as well as all of the records.
     *
     * @return The 4-byte unsigned checksum represented as a long
     */
    long checksum();

    /**
     * Get the max timestamp or log append time of this record batch.
     *
     * If the timestamp type is create time, this is the max timestamp among all records contained in this batch and
     * the value is updated during compaction.
     *
     * @return The max timestamp
     */
    long maxTimestamp();

    /**
     * Get the timestamp type of this record batch. This will be {@link TimestampType#NO_TIMESTAMP_TYPE}
     * if the batch has magic 0.
     *
     * @return The timestamp type
     */
    TimestampType timestampType();

    /**
     * Get the base offset contained in this record batch. For magic version prior to 2, the base offset will
     * always be the offset of the first message in the batch. This generally requires deep iteration and will
     * return the offset of the first record in the record batch. For magic version 2 and above, this will return
     * the first offset of the original record batch (i.e. prior to compaction). For non-compacted topics, the
     * behavior is equivalent.
     *
     * Because this requires deep iteration for older magic versions, this method should be used with
     * caution. Generally {@link #lastOffset()} is safer since access is efficient for all magic versions.
     *
     * @return The base offset of this record batch (which may or may not be the offset of the first record
     *         as described above).
     */
    long baseOffset();

    /**
     * Get the last offset in this record batch (inclusive). Just like {@link #baseOffset()}, the last offset
     * always reflects the offset of the last record in the original batch, even if it is removed during log
     * compaction.
     *
     * @return The offset of the last record in this batch
     */
    long lastOffset();

    /**
     * Get the offset following this record batch (i.e. the last offset contained in this batch plus one).
     *
     * @return the next consecutive offset following this batch
     */
    long nextOffset();

    /**
     * Get the record format version of this record batch (i.e its magic value).
     *
     * @return the magic byte
     */
    byte magic();

    /**
     * Get the producer id for this log record batch. For older magic versions, this will return -1.
     *
     * @return The producer id or -1 if there is none
     */
    long producerId();

    /**
     * Get the producer epoch for this log record batch.
     *
     * @return The producer epoch, or -1 if there is none
     */
    short producerEpoch();

    /**
     * Does the batch have a valid producer id set.
     */
    boolean hasProducerId();

    /**
     * Get the base sequence number of this record batch. Like {@link #baseOffset()}, this value is not
     * affected by compaction: it always retains the base sequence number from the original batch.
     *
     * @return The first sequence number or -1 if there is none
     */
    int baseSequence();

    /**
     * Get the last sequence number of this record batch. Like {@link #lastOffset()}, the last sequence number
     * always reflects the sequence number of the last record in the original batch, even if it is removed during log
     * compaction.
     *
     * @return The last sequence number or -1 if there is none
     */
    int lastSequence();

    /**
     * Get the compression type of this record batch.
     *
     * @return The compression type
     */
    CompressionType compressionType();

    /**
     * Get the size in bytes of this batch, including the size of the record and the batch overhead.
     * @return The size in bytes of this batch
     */
    int sizeInBytes();

    /**
     * Get the count if it is efficiently supported by the record format (which is only the case
     * for magic 2 and higher).
     *
     * @return The number of records in the batch or null for magic versions 0 and 1.
     */
    Integer countOrNull();

    /**
     * Check whether this record batch is compressed.
     * @return true if so, false otherwise
     */
    boolean isCompressed();

    /**
     * Write this record batch into a buffer.
     * @param buffer The buffer to write the batch to
     */
    void writeTo(ByteBuffer buffer);

    /**
     * Whether or not this record batch is part of a transaction.
     * @return true if it is, false otherwise
     */
    boolean isTransactional();

    /**
     * Get the partition leader epoch of this record batch.
     * @return The leader epoch or -1 if it is unknown
     */
    int partitionLeaderEpoch();

    /**
     * Return a streaming iterator which basically delays decompression of the record stream until the records
     * are actually asked for using {@link Iterator#next()}. If the message format does not support streaming
     * iteration, then the normal iterator is returned. Either way, callers should ensure that the iterator is closed.
     *
     * @param decompressionBufferSupplier The supplier of ByteBuffer(s) used for decompression if supported.
     *                                    For small record batches, allocating a potentially large buffer (64 KB for LZ4)
     *                                    will dominate the cost of decompressing and iterating over the records in the
     *                                    batch. As such, a supplier that reuses buffers will have a significant
     *                                    performance impact.
     * @return The closeable iterator
     */
    CloseableIterator<Record> streamingIterator(BufferSupplier decompressionBufferSupplier);

    /**
     * Check whether this is a control batch (i.e. whether the control bit is set in the batch attributes).
     * For magic versions prior to 2, this is always false.
     *
     * @return Whether this is a batch containing control records
     */
    boolean isControlBatch();
}
