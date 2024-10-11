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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention;
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetentionResult;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A {@link Records} implementation backed by a ByteBuffer. This is used only for reading or
 * modifying in-place an existing buffer of record batches. To create a new buffer see {@link MemoryRecordsBuilder},
 * or one of the {@link #builder(ByteBuffer, byte, Compression, TimestampType, long)} variants.
 */
public class MemoryRecords extends AbstractRecords {
    private static final Logger log = LoggerFactory.getLogger(MemoryRecords.class);
    public static final MemoryRecords EMPTY = MemoryRecords.readableRecords(ByteBuffer.allocate(0));

    private final ByteBuffer buffer;

    private final Iterable<MutableRecordBatch> batches = this::batchIterator;

    private int validBytes = -1;

    // Construct a writable memory records
    private MemoryRecords(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer should not be null");
        this.buffer = buffer;
    }

    @Override
    public int sizeInBytes() {
        return buffer.limit();
    }

    @Override
    public MemoryRecords duplicate() {
        return new MemoryRecords(buffer.duplicate());
    }

    @Override
    public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
        if (((long) position) + length > buffer.limit())
            throw new IllegalArgumentException("position+length should not be greater than buffer.limit(), position: "
                    + position + ", length: " + length + ", buffer.limit(): " + buffer.limit());

        return Utils.tryWriteTo(channel, position, length, buffer);
    }

    /**
     * Write all records to the given channel (including partial records).
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException For any IO errors writing to the channel
     */
    public int writeFullyTo(GatheringByteChannel channel) throws IOException {
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes())
            written += channel.write(buffer);
        buffer.reset();
        return written;
    }

    /**
     * The total number of bytes in this message set not including any partial, trailing messages. This
     * may be smaller than what is returned by {@link #sizeInBytes()}.
     * @return The number of valid bytes
     */
    public int validBytes() {
        if (validBytes >= 0)
            return validBytes;

        int bytes = 0;
        for (RecordBatch batch : batches())
            bytes += batch.sizeInBytes();

        this.validBytes = bytes;
        return bytes;
    }

    @Override
    public ConvertedRecords<MemoryRecords> downConvert(byte toMagic, long firstOffset, Time time) {
        return RecordsUtil.downConvert(batches(), toMagic, firstOffset, time);
    }

    @Override
    public AbstractIterator<MutableRecordBatch> batchIterator() {
        return new RecordBatchIterator<>(new ByteBufferLogInputStream(buffer.duplicate(), Integer.MAX_VALUE));
    }

    /**
     * Validates the header of the first batch and returns batch size.
     * @return first batch size including LOG_OVERHEAD if buffer contains header up to
     *         magic byte, null otherwise
     * @throws CorruptRecordException if record size or magic is invalid
     */
    public Integer firstBatchSize() {
        if (buffer.remaining() < HEADER_SIZE_UP_TO_MAGIC)
            return null;
        return new ByteBufferLogInputStream(buffer, Integer.MAX_VALUE).nextBatchSize();
    }

    /**
     * Filter the records into the provided ByteBuffer.
     *
     * @param partition                   The partition that is filtered (used only for logging)
     * @param filter                      The filter function
     * @param destinationBuffer           The byte buffer to write the filtered records to
     * @param maxRecordBatchSize          The maximum record batch size. Note this is not a hard limit: if a batch
     *                                    exceeds this after filtering, we log a warning, but the batch will still be
     *                                    created.
     * @param decompressionBufferSupplier The supplier of ByteBuffer(s) used for decompression if supported. For small
     *                                    record batches, allocating a potentially large buffer (64 KB for LZ4) will
     *                                    dominate the cost of decompressing and iterating over the records in the
     *                                    batch. As such, a supplier that reuses buffers will have a significant
     *                                    performance impact.
     * @return A FilterResult with a summary of the output (for metrics) and potentially an overflow buffer
     */
    public FilterResult filterTo(TopicPartition partition, RecordFilter filter, ByteBuffer destinationBuffer,
                                 int maxRecordBatchSize, BufferSupplier decompressionBufferSupplier) {
        return filterTo(partition, batches(), filter, destinationBuffer, maxRecordBatchSize, decompressionBufferSupplier);
    }

    /**
     * Note: This method is also used to convert the first timestamp of the batch (which is usually the timestamp of the first record)
     * to the delete horizon of the tombstones or txn markers which are present in the batch.
     */
    private static FilterResult filterTo(TopicPartition partition, Iterable<MutableRecordBatch> batches,
                                         RecordFilter filter, ByteBuffer destinationBuffer, int maxRecordBatchSize,
                                         BufferSupplier decompressionBufferSupplier) {
        FilterResult filterResult = new FilterResult(destinationBuffer);
        ByteBufferOutputStream bufferOutputStream = new ByteBufferOutputStream(destinationBuffer);
        for (MutableRecordBatch batch : batches) {
            final BatchRetentionResult batchRetentionResult = filter.checkBatchRetention(batch);
            final boolean containsMarkerForEmptyTxn = batchRetentionResult.containsMarkerForEmptyTxn;
            final BatchRetention batchRetention = batchRetentionResult.batchRetention;

            filterResult.bytesRead += batch.sizeInBytes();

            if (batchRetention == BatchRetention.DELETE)
                continue;

            // We use the absolute offset to decide whether to retain the message or not. Due to KAFKA-4298, we have to
            // allow for the possibility that a previous version corrupted the log by writing a compressed record batch
            // with a magic value not matching the magic of the records (magic < 2). This will be fixed as we
            // recopy the messages to the destination buffer.
            byte batchMagic = batch.magic();
            List<Record> retainedRecords = new ArrayList<>();

            final BatchFilterResult iterationResult = filterBatch(batch, decompressionBufferSupplier, filterResult, filter,
                    batchMagic, true, retainedRecords);
            boolean containsTombstones = iterationResult.containsTombstones;
            boolean writeOriginalBatch = iterationResult.writeOriginalBatch;
            long maxOffset = iterationResult.maxOffset;

            if (!retainedRecords.isEmpty()) {
                // we check if the delete horizon should be set to a new value
                // in which case, we need to reset the base timestamp and overwrite the timestamp deltas
                // if the batch does not contain tombstones, then we don't need to overwrite batch
                boolean needToSetDeleteHorizon = batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && (containsTombstones || containsMarkerForEmptyTxn)
                    && !batch.deleteHorizonMs().isPresent();
                if (writeOriginalBatch && !needToSetDeleteHorizon) {
                    batch.writeTo(bufferOutputStream);
                    filterResult.updateRetainedBatchMetadata(batch, retainedRecords.size(), false);
                } else {
                    long deleteHorizonMs;
                    if (needToSetDeleteHorizon)
                        deleteHorizonMs = filter.currentTime + filter.deleteRetentionMs;
                    else
                        deleteHorizonMs = batch.deleteHorizonMs().orElse(RecordBatch.NO_TIMESTAMP);
                    try (final MemoryRecordsBuilder builder = buildRetainedRecordsInto(batch, retainedRecords, bufferOutputStream, deleteHorizonMs)) {
                        MemoryRecords records = builder.build();
                        int filteredBatchSize = records.sizeInBytes();
                        if (filteredBatchSize > batch.sizeInBytes() && filteredBatchSize > maxRecordBatchSize)
                            log.warn("Record batch from {} with last offset {} exceeded max record batch size {} after cleaning " +
                                    "(new size is {}). Consumers with version earlier than 0.10.1.0 may need to " +
                                    "increase their fetch sizes.",
                                partition, batch.lastOffset(), maxRecordBatchSize, filteredBatchSize);

                        MemoryRecordsBuilder.RecordsInfo info = builder.info();
                        filterResult.updateRetainedBatchMetadata(info.maxTimestamp, info.shallowOffsetOfMaxTimestamp,
                            maxOffset, retainedRecords.size(), filteredBatchSize);
                    }
                }
            } else if (batchRetention == BatchRetention.RETAIN_EMPTY) {
                if (batchMagic < RecordBatch.MAGIC_VALUE_V2)
                    throw new IllegalStateException("Empty batches are only supported for magic v2 and above");

                bufferOutputStream.ensureRemaining(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
                DefaultRecordBatch.writeEmptyHeader(bufferOutputStream.buffer(), batchMagic, batch.producerId(),
                        batch.producerEpoch(), batch.baseSequence(), batch.baseOffset(), batch.lastOffset(),
                        batch.partitionLeaderEpoch(), batch.timestampType(), batch.maxTimestamp(),
                        batch.isTransactional(), batch.isControlBatch());
                filterResult.updateRetainedBatchMetadata(batch, 0, true);
            }

            // If we had to allocate a new buffer to fit the filtered buffer (see KAFKA-5316), return early to
            // avoid the need for additional allocations.
            ByteBuffer outputBuffer = bufferOutputStream.buffer();
            if (outputBuffer != destinationBuffer) {
                filterResult.outputBuffer = outputBuffer;
                return filterResult;
            }
        }

        return filterResult;
    }

    private static BatchFilterResult filterBatch(RecordBatch batch,
                                                 BufferSupplier decompressionBufferSupplier,
                                                 FilterResult filterResult,
                                                 RecordFilter filter,
                                                 byte batchMagic,
                                                 boolean writeOriginalBatch,
                                                 List<Record> retainedRecords) {
        long maxOffset = -1;
        boolean containsTombstones = false;
        try (final CloseableIterator<Record> iterator = batch.streamingIterator(decompressionBufferSupplier)) {
            while (iterator.hasNext()) {
                Record record = iterator.next();
                filterResult.messagesRead += 1;

                if (filter.shouldRetainRecord(batch, record)) {
                    // Check for log corruption due to KAFKA-4298. If we find it, make sure that we overwrite
                    // the corrupted batch with correct data.
                    if (!record.hasMagic(batchMagic))
                        writeOriginalBatch = false;

                    if (record.offset() > maxOffset)
                        maxOffset = record.offset();

                    retainedRecords.add(record);

                    if (!record.hasValue()) {
                        containsTombstones = true;
                    }
                } else {
                    writeOriginalBatch = false;
                }
            }
            return new BatchFilterResult(writeOriginalBatch, containsTombstones, maxOffset);
        }
    }

    private static class BatchFilterResult {
        private final boolean writeOriginalBatch;
        private final boolean containsTombstones;
        private final long maxOffset;
        private BatchFilterResult(final boolean writeOriginalBatch,
                                 final boolean containsTombstones,
                                 final long maxOffset) {
            this.writeOriginalBatch = writeOriginalBatch;
            this.containsTombstones = containsTombstones;
            this.maxOffset = maxOffset;
        }
    }

    private static MemoryRecordsBuilder buildRetainedRecordsInto(RecordBatch originalBatch,
                                                                 List<Record> retainedRecords,
                                                                 ByteBufferOutputStream bufferOutputStream,
                                                                 final long deleteHorizonMs) {
        byte magic = originalBatch.magic();
        Compression compression = Compression.of(originalBatch.compressionType()).build();
        TimestampType timestampType = originalBatch.timestampType();
        long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ?
                originalBatch.maxTimestamp() : RecordBatch.NO_TIMESTAMP;
        long baseOffset = magic >= RecordBatch.MAGIC_VALUE_V2 ?
                originalBatch.baseOffset() : retainedRecords.get(0).offset();

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(bufferOutputStream, magic,
                compression, timestampType, baseOffset, logAppendTime, originalBatch.producerId(),
                originalBatch.producerEpoch(), originalBatch.baseSequence(), originalBatch.isTransactional(),
                originalBatch.isControlBatch(), originalBatch.partitionLeaderEpoch(), bufferOutputStream.limit(), deleteHorizonMs);

        for (Record record : retainedRecords)
            builder.append(record);

        if (magic >= RecordBatch.MAGIC_VALUE_V2)
            // we must preserve the last offset from the initial batch in order to ensure that the
            // last sequence number from the batch remains even after compaction. Otherwise, the producer
            // could incorrectly see an out of sequence error.
            builder.overrideLastOffset(originalBatch.lastOffset());

        return builder;
    }

    /**
     * Get the byte buffer that backs this instance for reading.
     */
    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    @Override
    public Iterable<MutableRecordBatch> batches() {
        return batches;
    }

    @Override
    public String toString() {
        return "MemoryRecords(size=" + sizeInBytes() +
                ", buffer=" + buffer +
                ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        MemoryRecords that = (MemoryRecords) o;

        return buffer.equals(that.buffer);
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    public abstract static class RecordFilter {
        public final long currentTime;
        public final long deleteRetentionMs;

        public RecordFilter(final long currentTime, final long deleteRetentionMs) {
            this.currentTime = currentTime;
            this.deleteRetentionMs = deleteRetentionMs;
        }

        public static class BatchRetentionResult {
            public final BatchRetention batchRetention;
            public final boolean containsMarkerForEmptyTxn;
            public BatchRetentionResult(final BatchRetention batchRetention,
                                        final boolean containsMarkerForEmptyTxn) {
                this.batchRetention = batchRetention;
                this.containsMarkerForEmptyTxn = containsMarkerForEmptyTxn;
            }
        }

        public enum BatchRetention {
            DELETE, // Delete the batch without inspecting records
            RETAIN_EMPTY, // Retain the batch even if it is empty
            DELETE_EMPTY  // Delete the batch if it is empty
        }

        /**
         * Check whether the full batch can be discarded (i.e. whether we even need to
         * check the records individually).
         */
        protected abstract BatchRetentionResult checkBatchRetention(RecordBatch batch);

        /**
         * Check whether a record should be retained in the log. Note that {@link #checkBatchRetention(RecordBatch)}
         * is used prior to checking individual record retention. Only records from batches which were not
         * explicitly discarded with {@link BatchRetention#DELETE} will be considered.
         */
        protected abstract boolean shouldRetainRecord(RecordBatch recordBatch, Record record);
    }

    public static class FilterResult {
        private ByteBuffer outputBuffer;
        private int messagesRead = 0;
        // Note that `bytesRead` should contain only bytes from batches that have been processed, i.e. bytes from
        // `messagesRead` and any discarded batches.
        private int bytesRead = 0;
        private int messagesRetained = 0;
        private int bytesRetained = 0;
        private long maxOffset = -1L;
        private long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        private long shallowOffsetOfMaxTimestamp = -1L;

        private FilterResult(ByteBuffer outputBuffer) {
            this.outputBuffer = outputBuffer;
        }

        private void updateRetainedBatchMetadata(MutableRecordBatch retainedBatch, int numMessagesInBatch, boolean headerOnly) {
            int bytesRetained = headerOnly ? DefaultRecordBatch.RECORD_BATCH_OVERHEAD : retainedBatch.sizeInBytes();
            updateRetainedBatchMetadata(retainedBatch.maxTimestamp(), retainedBatch.lastOffset(),
                    retainedBatch.lastOffset(), numMessagesInBatch, bytesRetained);
        }

        private void updateRetainedBatchMetadata(long maxTimestamp, long shallowOffsetOfMaxTimestamp, long maxOffset,
                                                 int messagesRetained, int bytesRetained) {
            validateBatchMetadata(maxTimestamp, shallowOffsetOfMaxTimestamp, maxOffset);
            if (maxTimestamp > this.maxTimestamp) {
                this.maxTimestamp = maxTimestamp;
                this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
            }
            this.maxOffset = Math.max(maxOffset, this.maxOffset);
            this.messagesRetained += messagesRetained;
            this.bytesRetained += bytesRetained;
        }

        private void validateBatchMetadata(long maxTimestamp, long shallowOffsetOfMaxTimestamp, long maxOffset) {
            if (maxTimestamp != RecordBatch.NO_TIMESTAMP && shallowOffsetOfMaxTimestamp < 0)
                throw new IllegalArgumentException("shallowOffset undefined for maximum timestamp " + maxTimestamp);
            if (maxOffset < 0)
                throw new IllegalArgumentException("maxOffset undefined");
        }

        public ByteBuffer outputBuffer() {
            return outputBuffer;
        }

        public int messagesRead() {
            return messagesRead;
        }

        public int bytesRead() {
            return bytesRead;
        }

        public int messagesRetained() {
            return messagesRetained;
        }

        public int bytesRetained() {
            return bytesRetained;
        }

        public long maxOffset() {
            return maxOffset;
        }

        public long maxTimestamp() {
            return maxTimestamp;
        }

        public long shallowOffsetOfMaxTimestamp() {
            return shallowOffsetOfMaxTimestamp;
        }
    }

    public static MemoryRecords readableRecords(ByteBuffer buffer) {
        return new MemoryRecords(buffer);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               Compression compression,
                                               TimestampType timestampType,
                                               long baseOffset) {
        return builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compression, timestampType, baseOffset);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               Compression compression,
                                               TimestampType timestampType,
                                               long baseOffset,
                                               int maxSize) {
        long logAppendTime = RecordBatch.NO_TIMESTAMP;
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            logAppendTime = System.currentTimeMillis();

        return new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compression, timestampType, baseOffset,
            logAppendTime, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
            false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, maxSize);
    }

    public static MemoryRecordsBuilder idempotentBuilder(ByteBuffer buffer,
                                                         Compression compression,
                                                         long baseOffset,
                                                         long producerId,
                                                         short producerEpoch,
                                                         int baseSequence) {
        return builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compression, TimestampType.CREATE_TIME,
                baseOffset, System.currentTimeMillis(), producerId, producerEpoch, baseSequence);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               Compression compression,
                                               TimestampType timestampType,
                                               long baseOffset,
                                               long logAppendTime) {
        return builder(buffer, magic, compression, timestampType, baseOffset, logAppendTime,
                RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               Compression compression,
                                               TimestampType timestampType,
                                               long baseOffset) {
        long logAppendTime = RecordBatch.NO_TIMESTAMP;
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            logAppendTime = System.currentTimeMillis();
        return builder(buffer, magic, compression, timestampType, baseOffset, logAppendTime,
                RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               Compression compression,
                                               TimestampType timestampType,
                                               long baseOffset,
                                               long logAppendTime,
                                               int partitionLeaderEpoch) {
        return builder(buffer, magic, compression, timestampType, baseOffset, logAppendTime,
                RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, partitionLeaderEpoch);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               Compression compression,
                                               long baseOffset,
                                               long producerId,
                                               short producerEpoch,
                                               int baseSequence,
                                               boolean isTransactional) {
        return builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compression, TimestampType.CREATE_TIME, baseOffset,
                RecordBatch.NO_TIMESTAMP, producerId, producerEpoch, baseSequence, isTransactional,
                RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               Compression compression,
                                               TimestampType timestampType,
                                               long baseOffset,
                                               long logAppendTime,
                                               long producerId,
                                               short producerEpoch,
                                               int baseSequence) {
        return builder(buffer, magic, compression, timestampType, baseOffset, logAppendTime,
                producerId, producerEpoch, baseSequence, false, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               Compression compression,
                                               TimestampType timestampType,
                                               long baseOffset,
                                               long logAppendTime,
                                               long producerId,
                                               short producerEpoch,
                                               int baseSequence,
                                               boolean isTransactional,
                                               int partitionLeaderEpoch) {
        return builder(buffer, magic, compression, timestampType, baseOffset,
                logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, false, partitionLeaderEpoch);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               Compression compression,
                                               TimestampType timestampType,
                                               long baseOffset,
                                               long logAppendTime,
                                               long producerId,
                                               short producerEpoch,
                                               int baseSequence,
                                               boolean isTransactional,
                                               boolean isControlBatch,
                                               int partitionLeaderEpoch) {
        return new MemoryRecordsBuilder(buffer, magic, compression, timestampType, baseOffset,
                logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch,
                buffer.remaining());
    }

    public static MemoryRecords withRecords(Compression compression, SimpleRecord... records) {
        return withRecords(RecordBatch.CURRENT_MAGIC_VALUE, compression, records);
    }

    public static MemoryRecords withRecords(Compression compression, int partitionLeaderEpoch, SimpleRecord... records) {
        return withRecords(RecordBatch.CURRENT_MAGIC_VALUE, 0L, compression, TimestampType.CREATE_TIME,
                RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE,
                partitionLeaderEpoch, false, records);
    }

    public static MemoryRecords withRecords(byte magic, Compression compression, SimpleRecord... records) {
        return withRecords(magic, 0L, compression, TimestampType.CREATE_TIME, records);
    }

    public static MemoryRecords withRecords(long initialOffset, Compression compression, SimpleRecord... records) {
        return withRecords(RecordBatch.CURRENT_MAGIC_VALUE, initialOffset, compression, TimestampType.CREATE_TIME,
                records);
    }

    public static MemoryRecords withRecords(byte magic, long initialOffset, Compression compression, SimpleRecord... records) {
        return withRecords(magic, initialOffset, compression, TimestampType.CREATE_TIME, records);
    }

    public static MemoryRecords withRecords(long initialOffset, Compression compression, Integer partitionLeaderEpoch, SimpleRecord... records) {
        return withRecords(RecordBatch.CURRENT_MAGIC_VALUE, initialOffset, compression, TimestampType.CREATE_TIME, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, partitionLeaderEpoch, false, records);
    }

    public static MemoryRecords withIdempotentRecords(Compression compression, long producerId,
                                                      short producerEpoch, int baseSequence, SimpleRecord... records) {
        return withRecords(RecordBatch.CURRENT_MAGIC_VALUE, 0L, compression, TimestampType.CREATE_TIME, producerId, producerEpoch,
                baseSequence, RecordBatch.NO_PARTITION_LEADER_EPOCH, false, records);
    }

    public static MemoryRecords withIdempotentRecords(byte magic, long initialOffset, Compression compression,
                                                      long producerId, short producerEpoch, int baseSequence,
                                                      int partitionLeaderEpoch, SimpleRecord... records) {
        return withRecords(magic, initialOffset, compression, TimestampType.CREATE_TIME, producerId, producerEpoch,
                baseSequence, partitionLeaderEpoch, false, records);
    }

    public static MemoryRecords withIdempotentRecords(long initialOffset, Compression compression, long producerId,
                                                      short producerEpoch, int baseSequence, int partitionLeaderEpoch,
                                                      SimpleRecord... records) {
        return withRecords(RecordBatch.CURRENT_MAGIC_VALUE, initialOffset, compression, TimestampType.CREATE_TIME,
                producerId, producerEpoch, baseSequence, partitionLeaderEpoch, false, records);
    }

    public static MemoryRecords withTransactionalRecords(Compression compression, long producerId,
                                                         short producerEpoch, int baseSequence, SimpleRecord... records) {
        return withRecords(RecordBatch.CURRENT_MAGIC_VALUE, 0L, compression, TimestampType.CREATE_TIME,
                producerId, producerEpoch, baseSequence, RecordBatch.NO_PARTITION_LEADER_EPOCH, true, records);
    }

    public static MemoryRecords withTransactionalRecords(byte magic, long initialOffset, Compression compression,
                                                         long producerId, short producerEpoch, int baseSequence,
                                                         int partitionLeaderEpoch, SimpleRecord... records) {
        return withRecords(magic, initialOffset, compression, TimestampType.CREATE_TIME, producerId, producerEpoch,
                baseSequence, partitionLeaderEpoch, true, records);
    }

    public static MemoryRecords withTransactionalRecords(long initialOffset, Compression compression, long producerId,
                                                         short producerEpoch, int baseSequence, int partitionLeaderEpoch,
                                                         SimpleRecord... records) {
        return withTransactionalRecords(RecordBatch.CURRENT_MAGIC_VALUE, initialOffset, compression,
                producerId, producerEpoch, baseSequence, partitionLeaderEpoch, records);
    }

    public static MemoryRecords withRecords(byte magic, long initialOffset, Compression compression,
                                            TimestampType timestampType, SimpleRecord... records) {
        return withRecords(magic, initialOffset, compression, timestampType, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, RecordBatch.NO_PARTITION_LEADER_EPOCH,
                false, records);
    }

    public static MemoryRecords withRecords(byte magic, long initialOffset, Compression compression,
                                            TimestampType timestampType, long producerId, short producerEpoch,
                                            int baseSequence, int partitionLeaderEpoch, boolean isTransactional,
                                            SimpleRecord... records) {
        if (records.length == 0)
            return MemoryRecords.EMPTY;
        int sizeEstimate = AbstractRecords.estimateSizeInBytes(magic, compression.type(), Arrays.asList(records));
        ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(sizeEstimate);
        long logAppendTime = RecordBatch.NO_TIMESTAMP;
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            logAppendTime = System.currentTimeMillis();
        try (final MemoryRecordsBuilder builder = new MemoryRecordsBuilder(bufferStream, magic, compression, timestampType,
                initialOffset, logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, false,
                partitionLeaderEpoch, sizeEstimate)) {
            for (SimpleRecord record : records)
                builder.append(record);
            return builder.build();
        }
    }

    public static MemoryRecords withEndTransactionMarker(long producerId, short producerEpoch, EndTransactionMarker marker) {
        return withEndTransactionMarker(0L, System.currentTimeMillis(), RecordBatch.NO_PARTITION_LEADER_EPOCH,
                producerId, producerEpoch, marker);
    }

    public static MemoryRecords withEndTransactionMarker(long timestamp, long producerId, short producerEpoch,
                                                         EndTransactionMarker marker) {
        return withEndTransactionMarker(0L, timestamp, RecordBatch.NO_PARTITION_LEADER_EPOCH, producerId,
                producerEpoch, marker);
    }

    public static MemoryRecords withEndTransactionMarker(long initialOffset, long timestamp, int partitionLeaderEpoch,
                                                         long producerId, short producerEpoch,
                                                         EndTransactionMarker marker) {
        int endTxnMarkerBatchSize = DefaultRecordBatch.RECORD_BATCH_OVERHEAD +
                EndTransactionMarker.CURRENT_END_TXN_SCHEMA_RECORD_SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(endTxnMarkerBatchSize);
        writeEndTransactionalMarker(buffer, initialOffset, timestamp, partitionLeaderEpoch, producerId,
                producerEpoch, marker);
        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    public static void writeEndTransactionalMarker(ByteBuffer buffer, long initialOffset, long timestamp,
                                                   int partitionLeaderEpoch, long producerId, short producerEpoch,
                                                   EndTransactionMarker marker) {
        boolean isTransactional = true;
        try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
                TimestampType.CREATE_TIME, initialOffset, timestamp, producerId, producerEpoch,
                RecordBatch.NO_SEQUENCE, isTransactional, true, partitionLeaderEpoch,
                buffer.capacity())
        ) {
            builder.appendEndTxnMarker(timestamp, marker);
        }
    }

    public static MemoryRecords withLeaderChangeMessage(
        long initialOffset,
        long timestamp,
        int leaderEpoch,
        ByteBuffer buffer,
        LeaderChangeMessage leaderChangeMessage
    ) {
        try (MemoryRecordsBuilder builder = createKraftControlRecordBuilder(
                initialOffset,
                timestamp,
                leaderEpoch,
                buffer
            )
        ) {
            builder.appendLeaderChangeMessage(timestamp, leaderChangeMessage);
            return builder.build();
        }
    }

    public static MemoryRecords withSnapshotHeaderRecord(
        long initialOffset,
        long timestamp,
        int leaderEpoch,
        ByteBuffer buffer,
        SnapshotHeaderRecord snapshotHeaderRecord
    ) {
        try (MemoryRecordsBuilder builder = createKraftControlRecordBuilder(
                initialOffset,
                timestamp,
                leaderEpoch,
                buffer
            )
        ) {
            builder.appendSnapshotHeaderMessage(timestamp, snapshotHeaderRecord);
            return builder.build();
        }
    }

    public static MemoryRecords withSnapshotFooterRecord(
        long initialOffset,
        long timestamp,
        int leaderEpoch,
        ByteBuffer buffer,
        SnapshotFooterRecord snapshotFooterRecord
    ) {
        try (MemoryRecordsBuilder builder = createKraftControlRecordBuilder(
                initialOffset,
                timestamp,
                leaderEpoch,
                buffer
            )
        ) {
            builder.appendSnapshotFooterMessage(timestamp, snapshotFooterRecord);
            return builder.build();
        }
    }

    public static MemoryRecords withKRaftVersionRecord(
        long initialOffset,
        long timestamp,
        int leaderEpoch,
        ByteBuffer buffer,
        KRaftVersionRecord kraftVersionRecord
    ) {
        try (MemoryRecordsBuilder builder = createKraftControlRecordBuilder(
                initialOffset,
                timestamp,
                leaderEpoch,
                buffer
            )
        ) {
            builder.appendKRaftVersionMessage(timestamp, kraftVersionRecord);
            return builder.build();
        }
    }

    public static MemoryRecords withVotersRecord(
        long initialOffset,
        long timestamp,
        int leaderEpoch,
        ByteBuffer buffer,
        VotersRecord votersRecord
    ) {
        try (MemoryRecordsBuilder builder = createKraftControlRecordBuilder(
                initialOffset,
                timestamp,
                leaderEpoch,
                buffer
            )
        ) {
            builder.appendVotersMessage(timestamp, votersRecord);
            return builder.build();
        }
    }

    private static MemoryRecordsBuilder createKraftControlRecordBuilder(
        long initialOffset,
        long timestamp,
        int leaderEpoch,
        ByteBuffer buffer
    ) {
        return new MemoryRecordsBuilder(
            buffer,
            RecordBatch.CURRENT_MAGIC_VALUE,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            initialOffset,
            timestamp,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE,
            false,
            true,
            leaderEpoch,
            buffer.capacity()
        );
    }
}
