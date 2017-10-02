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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.DataOutputStream;
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
public class MemoryRecordsBuilder {
    private static final float COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;

    private final TimestampType timestampType;
    private final CompressionType compressionType;
    // Used to append records, may compress data on the fly
    private final DataOutputStream appendStream;
    // Used to hold a reference to the underlying ByteBuffer so that we can write the record batch header and access
    // the written bytes. ByteBufferOutputStream allocates a new ByteBuffer if the existing one is not large enough,
    // so it's not safe to hold a direct reference to the underlying ByteBuffer.
    private final ByteBufferOutputStream bufferStream;
    private final byte magic;
    private final int initialPosition;
    private final long baseOffset;
    private final long logAppendTime;
    private final boolean isControlBatch;
    private final int partitionLeaderEpoch;
    private final int writeLimit;
    private final int batchHeaderSizeInBytes;

    // Use a conservative estimate of the compression ratio. The producer overrides this using statistics
    // from previous batches before appending any records.
    private float estimatedCompressionRatio = 1.0F;

    private boolean appendStreamIsClosed = false;
    private boolean isTransactional;
    private long producerId;
    private short producerEpoch;
    private int baseSequence;
    private int uncompressedRecordsSizeInBytes = 0; // Number of bytes (excluding the header) written before compression
    private int numRecords = 0;
    private float actualCompressionRatio = 1;
    private long maxTimestamp = RecordBatch.NO_TIMESTAMP;
    private long offsetOfMaxTimestamp = -1;
    private Long lastOffset = null;
    private Long firstTimestamp = null;

    private MemoryRecords builtRecords;
    private boolean aborted = false;

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream,
                                byte magic,
                                CompressionType compressionType,
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
        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("TimestampType must be set for magic >= 0");
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            if (isTransactional)
                throw new IllegalArgumentException("Transactional records are not supported for magic " + magic);
            if (isControlBatch)
                throw new IllegalArgumentException("Control records are not supported for magic " + magic);
        }

        this.magic = magic;
        this.timestampType = timestampType;
        this.compressionType = compressionType;
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.numRecords = 0;
        this.uncompressedRecordsSizeInBytes = 0;
        this.actualCompressionRatio = 1;
        this.maxTimestamp = RecordBatch.NO_TIMESTAMP;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
        this.isControlBatch = isControlBatch;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.writeLimit = writeLimit;
        this.initialPosition = bufferStream.position();
        this.batchHeaderSizeInBytes = AbstractRecords.recordBatchHeaderSizeInBytes(magic, compressionType);

        bufferStream.position(initialPosition + batchHeaderSizeInBytes);
        this.bufferStream = bufferStream;
        this.appendStream = new DataOutputStream(compressionType.wrapForOutput(this.bufferStream, magic));
    }

    /**
     * Construct a new builder.
     *
     * @param buffer The underlying buffer to use (note that this class will allocate a new buffer if necessary
     *               to fit the records appended)
     * @param magic The magic value to use
     * @param compressionType The compression codec to use
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
                                CompressionType compressionType,
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
        this(new ByteBufferOutputStream(buffer), magic, compressionType, timestampType, baseOffset, logAppendTime,
                producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch,
                writeLimit);
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public int initialCapacity() {
        return bufferStream.initialCapacity();
    }

    public double compressionRatio() {
        return actualCompressionRatio;
    }

    public CompressionType compressionType() {
        return compressionType;
    }

    public boolean isControlBatch() {
        return isControlBatch;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    /**
     * Close this builder and return the resulting buffer.
     * @return The built log buffer
     */
    public MemoryRecords build() {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }
        close();
        return builtRecords;
    }

    /**
     * Get the max timestamp and its offset. The details of the offset returned are a bit subtle.
     *
     * If the log append time is used, the offset will be the last offset unless no compression is used and
     * the message format version is 0 or 1, in which case, it will be the first offset.
     *
     * If create time is used, the offset will be the last offset unless no compression is used and the message
     * format version is 0 or 1, in which case, it will be the offset of the record with the max timestamp.
     *
     * @return The max timestamp and its offset
     */
    public RecordsInfo info() {
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            long shallowOffsetOfMaxTimestamp;
            // Use the last offset when dealing with record batches
            if (compressionType != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                shallowOffsetOfMaxTimestamp = lastOffset;
            else
                shallowOffsetOfMaxTimestamp = baseOffset;
            return new RecordsInfo(logAppendTime, shallowOffsetOfMaxTimestamp);
        } else if (maxTimestamp == RecordBatch.NO_TIMESTAMP) {
            return new RecordsInfo(RecordBatch.NO_TIMESTAMP, lastOffset);
        } else {
            long shallowOffsetOfMaxTimestamp;
            // Use the last offset when dealing with record batches
            if (compressionType != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                shallowOffsetOfMaxTimestamp = lastOffset;
            else
                shallowOffsetOfMaxTimestamp = offsetOfMaxTimestamp;
            return new RecordsInfo(maxTimestamp, shallowOffsetOfMaxTimestamp);
        }
    }

    public int numRecords() {
        return numRecords;
    }

    /**
     * Return the sum of the size of the batch header (always uncompressed) and the records (before compression).
     */
    public int uncompressedBytesWritten() {
        return uncompressedRecordsSizeInBytes + batchHeaderSizeInBytes;
    }

    public void setProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (isClosed()) {
            // Sequence numbers are assigned when the batch is closed while the accumulator is being drained.
            // If the resulting ProduceRequest to the partition leader failed for a retriable error, the batch will
            // be re queued. In this case, we should not attempt to set the state again, since changing the producerId and sequence
            // once a batch has been sent to the broker risks introducing duplicates.
            throw new IllegalStateException("Trying to set producer state of an already closed batch. This indicates a bug on the client.");
        }
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }

    public void overrideLastOffset(long lastOffset) {
        if (builtRecords != null)
            throw new IllegalStateException("Cannot override the last offset after the records have been built");
        this.lastOffset = lastOffset;
    }

    /**
     * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
     * possible to update the RecordBatch header.
     */
    public void closeForRecordAppends() {
        if (!appendStreamIsClosed) {
            try {
                appendStream.close();
                appendStreamIsClosed = true;
            } catch (IOException e) {
                throw new KafkaException(e);
            }
        }
    }

    public void abort() {
        closeForRecordAppends();
        buffer().position(initialPosition);
        aborted = true;
    }

    public void reopenAndRewriteProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (aborted)
            throw new IllegalStateException("Should not reopen a batch which is already aborted.");
        builtRecords = null;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }


    public void close() {
        if (aborted)
            throw new IllegalStateException("Cannot close MemoryRecordsBuilder as it has already been aborted");

        if (builtRecords != null)
            return;

        validateProducerState();

        closeForRecordAppends();

        if (numRecords == 0L) {
            buffer().position(initialPosition);
            builtRecords = MemoryRecords.EMPTY;
        } else {
            if (magic > RecordBatch.MAGIC_VALUE_V1)
                this.actualCompressionRatio = (float) writeDefaultBatchHeader() / this.uncompressedRecordsSizeInBytes;
            else if (compressionType != CompressionType.NONE)
                this.actualCompressionRatio = (float) writeLegacyCompressedWrapperHeader() / this.uncompressedRecordsSizeInBytes;

            ByteBuffer buffer = buffer().duplicate();
            buffer.flip();
            buffer.position(initialPosition);
            builtRecords = MemoryRecords.readableRecords(buffer.slice());
        }
    }

    private void validateProducerState() {
        if (isTransactional && producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("Cannot write transactional messages without a valid producer ID");

        if (producerId != RecordBatch.NO_PRODUCER_ID) {
            if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH)
                throw new IllegalArgumentException("Invalid negative producer epoch");

            if (baseSequence < 0 && !isControlBatch)
                throw new IllegalArgumentException("Invalid negative sequence number used");

            if (magic < RecordBatch.MAGIC_VALUE_V2)
                throw new IllegalArgumentException("Idempotent messages are not supported for magic " + magic);
        }
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

        DefaultRecordBatch.writeHeader(buffer, baseOffset, offsetDelta, size, magic, compressionType, timestampType,
                firstTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch,
                partitionLeaderEpoch, numRecords);

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
        LegacyRecord.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compressionType, timestampType);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * Append a record and return its checksum for message format v0 and v1, or null for for v2 and above.
     */
    private Long appendWithOffset(long offset, boolean isControlRecord, long timestamp, ByteBuffer key,
                                  ByteBuffer value, Header[] headers) {
        try {
            if (isControlRecord != isControlBatch)
                throw new IllegalArgumentException("Control records can only be appended to control batches");

            if (lastOffset != null && offset <= lastOffset)
                throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s " +
                        "(Offsets must increase monotonically).", offset, lastOffset));

            if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Invalid negative timestamp " + timestamp);

            if (magic < RecordBatch.MAGIC_VALUE_V2 && headers != null && headers.length > 0)
                throw new IllegalArgumentException("Magic v" + magic + " does not support record headers");

            if (firstTimestamp == null)
                firstTimestamp = timestamp;

            if (magic > RecordBatch.MAGIC_VALUE_V1) {
                appendDefaultRecord(offset, timestamp, key, value, headers);
                return null;
            } else {
                return appendLegacyRecord(offset, timestamp, key, value);
            }
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        return appendWithOffset(offset, false, timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        return appendWithOffset(offset, false, timestamp, key, value, headers);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, byte[] key, byte[] value) {
        return appendWithOffset(offset, timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value) {
        return appendWithOffset(offset, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param record The record to append
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, SimpleRecord record) {
        return appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, ByteBuffer key, ByteBuffer value) {
        return append(timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        return appendWithOffset(nextSequentialOffset(), timestamp, key, value, headers);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, byte[] key, byte[] value) {
        return append(timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, byte[] key, byte[] value, Header[] headers) {
        return append(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param record The record to append
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(SimpleRecord record) {
        return appendWithOffset(nextSequentialOffset(), record);
    }

    /**
     * Append a control record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param type The control record type (cannot be UNKNOWN)
     * @param value The control record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    private Long appendControlRecord(long timestamp, ControlRecordType type, ByteBuffer value) {
        Struct keyStruct = type.recordKey();
        ByteBuffer key = ByteBuffer.allocate(keyStruct.sizeOf());
        keyStruct.writeTo(key);
        key.flip();
        return appendWithOffset(nextSequentialOffset(), true, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendEndTxnMarker(long timestamp, EndTransactionMarker marker) {
        if (producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("End transaction marker requires a valid producerId");
        if (!isTransactional)
            throw new IllegalArgumentException("End transaction marker depends on batch transactional flag being enabled");
        ByteBuffer value = marker.serializeValue();
        return appendControlRecord(timestamp, marker.controlType(), value);
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

    private void appendDefaultRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value,
                                     Header[] headers) throws IOException {
        ensureOpenForRecordAppend();
        int offsetDelta = (int) (offset - baseOffset);
        long timestampDelta = timestamp - firstTimestamp;
        int sizeInBytes = DefaultRecord.writeTo(appendStream, offsetDelta, timestampDelta, key, value, headers);
        recordWritten(offset, timestamp, sizeInBytes);
    }

    private long appendLegacyRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value) throws IOException {
        ensureOpenForRecordAppend();
        if (compressionType == CompressionType.NONE && timestampType == TimestampType.LOG_APPEND_TIME)
            timestamp = logAppendTime;

        int size = LegacyRecord.recordSize(magic, key, value);
        AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

        if (timestampType == TimestampType.LOG_APPEND_TIME)
            timestamp = logAppendTime;
        long crc = LegacyRecord.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);
        recordWritten(offset, timestamp, size + Records.LOG_OVERHEAD);
        return crc;
    }

    private long toInnerOffset(long offset) {
        // use relative offsets for compressed messages with magic v1
        if (magic > 0 && compressionType != CompressionType.NONE)
            return offset - baseOffset;
        return offset;
    }

    private void recordWritten(long offset, long timestamp, int size) {
        if (numRecords == Integer.MAX_VALUE)
            throw new IllegalArgumentException("Maximum number of records per batch exceeded, max records: " + Integer.MAX_VALUE);
        if (offset - baseOffset > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Maximum offset delta exceeded, base offset: " + baseOffset +
                    ", last offset: " + offset);

        numRecords += 1;
        uncompressedRecordsSizeInBytes += size;
        lastOffset = offset;

        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
            offsetOfMaxTimestamp = offset;
        }
    }

    private void ensureOpenForRecordAppend() {
        if (appendStreamIsClosed)
            throw new IllegalStateException("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
    }

    private void ensureOpenForRecordBatchWrite() {
        if (isClosed())
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is closed");
        if (aborted)
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is aborted");
    }

    /**
     * Get an estimate of the number of bytes written (based on the estimation factor hard-coded in {@link CompressionType}.
     * @return The estimated number of bytes written
     */
    private int estimatedBytesWritten() {
        if (compressionType == CompressionType.NONE) {
            return batchHeaderSizeInBytes + uncompressedRecordsSizeInBytes;
        } else {
            // estimate the written bytes to the underlying byte buffer based on uncompressed written bytes
            return batchHeaderSizeInBytes + (int) (uncompressedRecordsSizeInBytes * estimatedCompressionRatio * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
    }

    /**
     * Set the estimated compression ratio for the memory records builder.
     */
    public void setEstimatedCompressionRatio(float estimatedCompressionRatio) {
        this.estimatedCompressionRatio = estimatedCompressionRatio;
    }

    /**
     * Check if we have room for a new record containing the given key/value pair. If no records have been
     * appended, then this returns true.
     */
    public boolean hasRoomFor(long timestamp, byte[] key, byte[] value, Header[] headers) {
        return hasRoomFor(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Check if we have room for a new record containing the given key/value pair. If no records have been
     * appended, then this returns true.
     *
     * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
     * accurate if compression is used. When this happens, the following append may cause dynamic buffer
     * re-allocation in the underlying byte buffer stream.
     */
    public boolean hasRoomFor(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        if (isFull())
            return false;

        // We always allow at least one record to be appended (the ByteBufferOutputStream will grow as needed)
        if (numRecords == 0)
            return true;

        final int recordSize;
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            recordSize = Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
        } else {
            int nextOffsetDelta = lastOffset == null ? 0 : (int) (lastOffset - baseOffset + 1);
            long timestampDelta = firstTimestamp == null ? 0 : timestamp - firstTimestamp;
            recordSize = DefaultRecord.sizeInBytes(nextOffsetDelta, timestampDelta, key, value, headers);
        }

        // Be conservative and not take compression of the new record into consideration.
        return this.writeLimit >= estimatedBytesWritten() + recordSize;
    }

    public boolean isClosed() {
        return builtRecords != null;
    }

    public boolean isFull() {
        // note that the write limit is respected only after the first record is added which ensures we can always
        // create non-empty batches (this is used to disable batching when the producer's batch size is set to 0).
        return appendStreamIsClosed || (this.numRecords > 0 && this.writeLimit <= estimatedBytesWritten());
    }

    /**
     * Get an estimate of the number of bytes written to the underlying buffer. The returned value
     * is exactly correct if the record set is not compressed or if the builder has been closed.
     */
    public int estimatedSizeInBytes() {
        return builtRecords != null ? builtRecords.sizeInBytes() : estimatedBytesWritten();
    }

    public byte magic() {
        return magic;
    }

    private long nextSequentialOffset() {
        return lastOffset == null ? baseOffset : lastOffset + 1;
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

    /**
     * Return the producer id of the RecordBatches created by this builder.
     */
    public long producerId() {
        return this.producerId;
    }

    public short producerEpoch() {
        return this.producerEpoch;
    }

    public int baseSequence() {
        return this.baseSequence;
    }
}
