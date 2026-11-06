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
package org.apache.kafka.common.record.internal;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.SingleByteBufferOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * This class is used to write new log data in memory, i.e. this is the write path for {@link MemoryRecords}.
 * It transparently handles compression and exposes methods for appending new records, possibly with message
 * format conversion.
 *
 * In cases where keeping memory retention low is important and there's a gap between the time that record appends stop
 * and the builder is closed (e.g. the Producer), it's important to call `closeForRecordAppends` when the former happens.
 * This will release resources like compression buffers that can be relatively large (64 KB for LZ4).
 */
public class MemoryRecordsBuilder extends AbstractRecordsBuilder {
    // Used to hold a reference to the underlying ByteBuffer so that we can write the record batch header and access
    // the written bytes. ByteBufferOutputStream allocates a new ByteBuffer if the existing one is not large enough,
    // so it's not safe to hold a direct reference to the underlying ByteBuffer.
    private final SingleByteBufferOutputStream bufferStream;

    public MemoryRecordsBuilder(SingleByteBufferOutputStream bufferStream,
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
                                int partitionLeaderEpoch,
                                int writeLimit,
                                long deleteHorizonMs) {
        super(bufferStream, magic, compression, timestampType, baseOffset, logAppendTime, producerId,
                producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch, writeLimit,
                deleteHorizonMs);
        this.bufferStream = bufferStream;
    }

    public MemoryRecordsBuilder(SingleByteBufferOutputStream bufferStream,
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
                                int partitionLeaderEpoch,
                                int writeLimit) {
        this(bufferStream, magic, compression, timestampType, baseOffset, logAppendTime, producerId,
             producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch, writeLimit,
             RecordBatch.NO_TIMESTAMP);
    }

    /**
     * Construct a new builder.
     *
     * @param buffer The underlying buffer to use (note that this class will allocate a new buffer if necessary
     *               to fit the records appended)
     * @param magic The magic value to use
     * @param compression The compression codec to use
     * @param timestampType The desired timestamp type. For magic > 0, this cannot be {@link TimestampType#NO_TIMESTAMP_TYPE}.
     * @param baseOffset The initial offset to use for
     * @param logAppendTime The log append time of this record set. Can be set to NO_TIMESTAMP if CREATE_TIME is used.
     * @param producerId The producer ID associated with the producer writing this record set
     * @param producerEpoch The epoch of the producer
     * @param baseSequence The sequence number of the first record in this set
     * @param isTransactional Whether or not the records are part of a transaction
     * @param isControlBatch Whether or not this is a control batch (e.g. for transaction markers)
     * @param partitionLeaderEpoch The epoch of the partition leader appending the record set to the log
     * @param writeLimit The desired limit on the total bytes for this record set (note that this can be exceeded
     *                   when compression is used since size estimates are rough, and in the case that the first
     *                   record added exceeds the size).
     */
    public MemoryRecordsBuilder(ByteBuffer buffer,
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
                                int partitionLeaderEpoch,
                                int writeLimit) {
        this(new SingleByteBufferOutputStream(buffer), magic, compression, timestampType, baseOffset, logAppendTime,
                producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch,
                writeLimit);
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    @Override
    public int initialCapacity() {
        return bufferStream.initialCapacity();
    }

    /**
     * Close this builder and return the resulting buffer.
     * @return The built log buffer
     */
    @Override
    public MemoryRecords build() {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }
        close();
        return (MemoryRecords) builtRecords;
    }


    /**
     * There are three cases of finding max timestamp to return:
     * 1) version 0: The max timestamp is NO_TIMESTAMP (-1)
     * 2) LogAppendTime: All records have same timestamp, and so the max timestamp is equal to logAppendTime
     * 3) CreateTime: The max timestamp of record
     * <p>
     * Let's talk about OffsetOfMaxTimestamp. There are some paths that we don't try to find the OffsetOfMaxTimestamp
     * to avoid expensive records iteration. Those paths include follower append and index recovery. In order to
     * avoid inconsistent time index, we let all paths find shallowOffsetOfMaxTimestamp instead of OffsetOfMaxTimestamp.
     * <p>
     * Let's define the shallowOffsetOfMaxTimestamp: It is last offset of the batch having max timestamp. If there are
     * many batches having same max timestamp, we pick up the earliest batch.
     * <p>
     * There are five cases of finding shallowOffsetOfMaxTimestamp to return:
     * 1) version 0: It is always the -1
     * 2) LogAppendTime with single batch: It is the offset of last record
     * 3) LogAppendTime with many single-record batches: Those single-record batches have same max timestamp, so we return
     *                                                   the base offset, which is equal to the last offset of earliest batch
     * 4) CreateTime with single batch: We return offset of last record to follow the spec we mentioned above. Of course,
     *                                  we do have the OffsetOfMaxTimestamp for this case, but we want to make all paths
     *                                  find the shallowOffsetOfMaxTimestamp rather than offsetOfMaxTimestamp
     * 5) CreateTime with many single-record batches: Each batch is composed of single record, and hence offsetOfMaxTimestamp
     *                                                is equal to the last offset of earliest batch with max timestamp
     */
    public RecordsInfo info() {
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            if (compression.type() != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                // maxTimestamp => case 2
                // shallowOffsetOfMaxTimestamp => case 2
                return new RecordsInfo(logAppendTime, lastOffset);
            else
                // maxTimestamp => case 2
                // shallowOffsetOfMaxTimestamp => case 3
                return new RecordsInfo(logAppendTime, baseOffset);
        } else if (maxTimestamp == RecordBatch.NO_TIMESTAMP) {
            // maxTimestamp => case 1
            // shallowOffsetOfMaxTimestamp => case 1
            return new RecordsInfo(RecordBatch.NO_TIMESTAMP, -1);
        } else {
            if (compression.type() != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                // maxTimestamp => case 3
                // shallowOffsetOfMaxTimestamp => case 4
                return new RecordsInfo(maxTimestamp, lastOffset);
            else
                // maxTimestamp => case 3
                // shallowOffsetOfMaxTimestamp => case 5
                return new RecordsInfo(maxTimestamp, offsetOfMaxTimestamp);
        }
    }

    /**
     * Return the sum of the size of the batch header (always uncompressed) and the records (before compression).
     */
    public int uncompressedBytesWritten() {
        return uncompressedRecordsSizeInBytes + batchHeaderSizeInBytes;
    }

    public void overrideLastOffset(long lastOffset) {
        if (builtRecords != null)
            throw new IllegalStateException("Cannot override the last offset after the records have been built");
        this.lastOffset = lastOffset;
    }

    @Override
    public void abort() {
        super.abort();
        rewindToInitialPosition();
    }

    private void rewindToInitialPosition() {
        bufferStream.buffer().position(initialPosition);
    }

    @Override
    public void close() {
        if (prepareClose()) {
            return;
        }

        if (magic > RecordBatch.MAGIC_VALUE_V1) {
            this.actualCompressionRatio = (float) writeDefaultBatchHeader() / this.uncompressedRecordsSizeInBytes;
        } else if (compression.type() != CompressionType.NONE) {
            this.actualCompressionRatio = (float) writeLegacyCompressedWrapperHeader() / this.uncompressedRecordsSizeInBytes;
        }

        ByteBuffer buffer = buffer().duplicate();
        buffer.flip();
        buffer.position(initialPosition);
        builtRecords = MemoryRecords.readableRecords(buffer.slice());
    }

    @Override
    protected void resetToEmpty() {
        rewindToInitialPosition();
        super.resetToEmpty();
    }

    /**
     * Write the header to the default batch.
     * @return the written compressed bytes.
     */
    private int writeDefaultBatchHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initialPosition);
        int size = pos - initialPosition;
        int writtenCompressed = size - DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        int offsetDelta = (int) (lastOffset - baseOffset);

        final long maxTimestamp;
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            maxTimestamp = logAppendTime;
        else
            maxTimestamp = this.maxTimestamp;

        DefaultRecordBatch.writeHeader(buffer, baseOffset, offsetDelta, size, magic, compression.type(), timestampType,
                baseTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch,
                hasDeleteHorizonMs(), partitionLeaderEpoch, numRecords);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * Write the header to the legacy batch.
     * @return the written compressed bytes.
     */
    private int writeLegacyCompressedWrapperHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initialPosition);

        int wrapperSize = pos - initialPosition - Records.LOG_OVERHEAD;
        int writtenCompressed = wrapperSize - LegacyRecord.recordOverhead(magic);
        AbstractLegacyRecordBatch.writeHeader(buffer, lastOffset, wrapperSize);

        long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
        LegacyRecord.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compression.type(), timestampType);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     */
    public void appendWithOffset(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        appendWithOffset(offset, false, timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     */
    public void appendWithOffset(long offset, long timestamp, byte[] key, byte[] value) {
        appendWithOffset(offset, timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     */
    public void appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value) {
        appendWithOffset(offset, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param record The record to append
     */
    public void appendWithOffset(long offset, SimpleRecord record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Append a control record at the given offset. The control record type must be known or
     * this method will raise an error.
     *
     * @param offset The absolute offset of the record in the log buffer
     * @param record The record to append
     */
    public void appendControlRecordWithOffset(long offset, SimpleRecord record) {
        short typeId = ControlRecordType.parseTypeId(record.key());
        ControlRecordType type = ControlRecordType.fromTypeId(typeId);
        if (type == ControlRecordType.UNKNOWN)
            throw new IllegalArgumentException("Cannot append record with unknown control record type " + typeId);

        appendWithOffset(offset, true, record.timestamp(),
            record.key(), record.value(), record.headers());
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     */
    public void append(long timestamp, ByteBuffer key, ByteBuffer value) {
        append(timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     */
    public void append(long timestamp, byte[] key, byte[] value) {
        append(timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param record The record to append
     */
    public void append(SimpleRecord record) {
        appendWithOffset(nextSequentialOffset(), record);
    }

    /**
     * Append a control record at the next sequential offset.
     *
     * @param timestamp The record timestamp
     * @param type The control record type (cannot be UNKNOWN)
     * @param value The control record value
     */
    public void appendControlRecord(long timestamp, ControlRecordType type, ByteBuffer value) {
        ByteBuffer key = type.recordKey();
        appendWithOffset(nextSequentialOffset(), true, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    public void appendEndTxnMarker(long timestamp, EndTransactionMarker marker) {
        if (producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("End transaction marker requires a valid producerId");
        if (!isTransactional)
            throw new IllegalArgumentException("End transaction marker depends on batch transactional flag being enabled");
        ByteBuffer value = marker.serializeValue();
        appendControlRecord(timestamp, marker.controlType(), value);
    }

    public void appendLeaderChangeMessage(long timestamp, LeaderChangeMessage leaderChangeMessage) {
        if (partitionLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) {
            throw new IllegalArgumentException("Partition leader epoch must be valid, but get " + partitionLeaderEpoch);
        }
        appendControlRecord(
            timestamp,
            ControlRecordType.LEADER_CHANGE,
            MessageUtil.toByteBufferAccessor(leaderChangeMessage, ControlRecordUtils.LEADER_CHANGE_CURRENT_VERSION).buffer()
        );
    }

    public void appendSnapshotHeaderMessage(long timestamp, SnapshotHeaderRecord snapshotHeaderRecord) {
        appendControlRecord(
            timestamp,
            ControlRecordType.SNAPSHOT_HEADER,
            MessageUtil.toByteBufferAccessor(snapshotHeaderRecord, ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION).buffer()
        );
    }

    public void appendSnapshotFooterMessage(long timestamp, SnapshotFooterRecord snapshotHeaderRecord) {
        appendControlRecord(
            timestamp,
            ControlRecordType.SNAPSHOT_FOOTER,
            MessageUtil.toByteBufferAccessor(snapshotHeaderRecord, ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION).buffer()
        );
    }

    public void appendKRaftVersionMessage(long timestamp, KRaftVersionRecord kraftVersionRecord) {
        appendControlRecord(
            timestamp,
            ControlRecordType.KRAFT_VERSION,
            MessageUtil.toByteBufferAccessor(kraftVersionRecord, ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION).buffer()
        );
    }

    public void appendVotersMessage(long timestamp, VotersRecord votersRecord) {
        appendControlRecord(
            timestamp,
            ControlRecordType.KRAFT_VOTERS,
            MessageUtil.toByteBufferAccessor(votersRecord, ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION).buffer()
        );
    }

    /**
     * Add a legacy record without doing offset/magic validation (this should only be used in testing).
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, LegacyRecord record) {
        ensureOpenForRecordAppend();
        try {
            int size = record.sizeInBytes();
            AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

            ByteBuffer buffer = record.buffer().duplicate();
            appendStream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            recordWritten(offset, record.timestamp(), size + Records.LOG_OVERHEAD);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Append a record without doing offset/magic validation (this should only be used in testing).
     *
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, SimpleRecord record) throws IOException {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            int offsetDelta = (int) (offset - baseOffset);
            long timestamp = record.timestamp();
            if (baseTimestamp == null)
                baseTimestamp = timestamp;

            int sizeInBytes = DefaultRecord.writeTo(appendStream,
                offsetDelta,
                timestamp - baseTimestamp,
                record.key(),
                record.value(),
                record.headers());
            recordWritten(offset, timestamp, sizeInBytes);
        } else {
            LegacyRecord legacyRecord = LegacyRecord.create(magic,
                record.timestamp(),
                Utils.toNullableArray(record.key()),
                Utils.toNullableArray(record.value()));
            appendUncheckedWithOffset(offset, legacyRecord);
        }
    }

    /**
     * Append a record at the next sequential offset.
     * @param record the record to add
     */
    public void append(Record record) {
        appendWithOffset(record.offset(), isControlBatch, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Append a log record using a different offset
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, Record record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Add a record with a given offset. The record must have a magic which matches the magic use to
     * construct this builder and the offset must be greater than the last appended record.
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, LegacyRecord record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value());
    }

    /**
     * Append the record at the next consecutive offset. If no records have been appended yet, use the base
     * offset of this builder.
     * @param record The record to add
     */
    public void append(LegacyRecord record) {
        appendWithOffset(nextSequentialOffset(), record);
    }

    private void ensureOpenForRecordBatchWrite() {
        if (isClosed())
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is closed");
        if (aborted)
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is aborted");
    }

    /**
     * Check if we have room for a given number of bytes.
     */
    public boolean hasRoomFor(int estimatedRecordsSize) {
        if (isFull()) return false;
        return this.writeLimit >= estimatedBytesWritten() + estimatedRecordsSize;
    }

    public int maxAllowedBytes() {
        return this.writeLimit - this.batchHeaderSizeInBytes;
    }

    public static class RecordsInfo {
        public final long maxTimestamp;
        public final long shallowOffsetOfMaxTimestamp;

        public RecordsInfo(long maxTimestamp,
                           long shallowOffsetOfMaxTimestamp) {
            this.maxTimestamp = maxTimestamp;
            this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
        }
    }
}
