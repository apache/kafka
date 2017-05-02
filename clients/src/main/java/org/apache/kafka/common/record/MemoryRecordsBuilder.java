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
    private static final float COMPRESSION_RATE_DAMPING_FACTOR = 0.9f;
    private static final float COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;
    private static final int COMPRESSION_DEFAULT_BUFFER_SIZE = 1024;

    private static final float[] TYPE_TO_RATE;

    static {
        int maxTypeId = -1;
        for (CompressionType type : CompressionType.values())
            maxTypeId = Math.max(maxTypeId, type.id);
        TYPE_TO_RATE = new float[maxTypeId + 1];
        for (CompressionType type : CompressionType.values()) {
            TYPE_TO_RATE[type.id] = type.rate;
        }
    }

    private final TimestampType timestampType;
    private final CompressionType compressionType;
    // Used to append records, may compress data on the fly
    private final DataOutputStream appendStream;
    // Used to hold a reference to the underlying ByteBuffer so that we can write the record batch header and access
    // the written bytes. ByteBufferOutputStream allocates a new ByteBuffer if the existing one is not large enough,
    // so it's not safe to hold a direct reference to the underlying ByteBuffer.
    private final ByteBufferOutputStream bufferStream;
    private final byte magic;
    private final int initPos;
    private final long baseOffset;
    private final long logAppendTime;
    private final boolean isTransactional;
    private final int partitionLeaderEpoch;
    private final int writeLimit;
    private final int initialCapacity;

    private boolean appendStreamIsClosed = false;
    private long producerId;
    private short producerEpoch;
    private int baseSequence;
    private long writtenUncompressed = 0;
    private int numRecords = 0;
    private float compressionRate = 1;
    private long maxTimestamp = RecordBatch.NO_TIMESTAMP;
    private long offsetOfMaxTimestamp = -1;
    private Long lastOffset = null;
    private Long baseTimestamp = null;

    private MemoryRecords builtRecords;

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
     * @param producerId The producer ID (PID) associated with the producer writing this record set
     * @param producerEpoch The epoch of the producer
     * @param baseSequence The sequence number of the first record in this set
     * @param isTransactional Whether or not the records are part of a transaction
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
                                int partitionLeaderEpoch,
                                int writeLimit) {
        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("TimestampType must be set for magic >= 0");

        if (isTransactional) {
            if (magic < RecordBatch.MAGIC_VALUE_V2)
                throw new IllegalArgumentException("Transactional messages are not supported for magic " + magic);
        }


        this.magic = magic;
        this.timestampType = timestampType;
        this.compressionType = compressionType;
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.initPos = buffer.position();
        this.numRecords = 0;
        this.writtenUncompressed = 0;
        this.compressionRate = 1;
        this.maxTimestamp = RecordBatch.NO_TIMESTAMP;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.writeLimit = writeLimit;
        this.initialCapacity = buffer.capacity();

        if (magic > RecordBatch.MAGIC_VALUE_V1) {
            buffer.position(initPos + DefaultRecordBatch.RECORDS_OFFSET);
        } else if (compressionType != CompressionType.NONE) {
            // for compressed records, leave space for the header and the shallow message metadata
            // and move the starting position to the value payload offset
            buffer.position(initPos + Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic));
        }

        // create the stream
        bufferStream = new ByteBufferOutputStream(buffer);
        appendStream = new DataOutputStream(compressionType.wrapForOutput(bufferStream, magic,
                COMPRESSION_DEFAULT_BUFFER_SIZE));
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public int initialCapacity() {
        return initialCapacity;
    }

    public double compressionRate() {
        return compressionRate;
    }

    /**
     * Close this builder and return the resulting buffer.
     * @return The built log buffer
     */
    public MemoryRecords build() {
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

    public void setProducerState(long pid, short epoch, int baseSequence) {
        if (isClosed()) {
            // Sequence numbers are assigned when the batch is closed while the accumulator is being drained.
            // If the resulting ProduceRequest to the partition leader failed for a retriable error, the batch will
            // be re queued. In this case, we should not attempt to set the state again, since changing the pid and sequence
            // once a batch has been sent to the broker risks introducing duplicates.
            throw new IllegalStateException("Trying to set producer state of an already closed batch. This indicates a bug on the client.");
        }
        this.producerId = pid;
        this.producerEpoch = epoch;
        this.baseSequence = baseSequence;
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

    public void close() {
        if (builtRecords != null)
            return;

        if (isTransactional && producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("Cannot write transactional messages without a valid producer ID");

        if (producerId != RecordBatch.NO_PRODUCER_ID) {
            if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH)
                throw new IllegalArgumentException("Invalid negative producer epoch");

            if (baseSequence == RecordBatch.NO_SEQUENCE)
                throw new IllegalArgumentException("Invalid negative sequence number used");

            if (magic < RecordBatch.MAGIC_VALUE_V2)
                throw new IllegalArgumentException("Idempotent messages are not supported for magic " + magic);
        }

        closeForRecordAppends();

        if (numRecords == 0L) {
            buffer().position(initPos);
            builtRecords = MemoryRecords.EMPTY;
        } else {
            if (magic > RecordBatch.MAGIC_VALUE_V1)
                writeDefaultBatchHeader();
            else if (compressionType != CompressionType.NONE)
                writeLegacyCompressedWrapperHeader();

            ByteBuffer buffer = buffer().duplicate();
            buffer.flip();
            buffer.position(initPos);
            builtRecords = MemoryRecords.readableRecords(buffer.slice());
        }
    }

    private void writeDefaultBatchHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initPos);
        int size = pos - initPos;
        int offsetDelta = (int) (lastOffset - baseOffset);

        final long baseTimestamp;
        final long maxTimestamp;
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            baseTimestamp = logAppendTime;
            maxTimestamp = logAppendTime;
        } else {
            baseTimestamp = this.baseTimestamp;
            maxTimestamp = this.maxTimestamp;
        }

        DefaultRecordBatch.writeHeader(buffer, baseOffset, offsetDelta, size, magic, compressionType, timestampType,
                baseTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, isTransactional,
                partitionLeaderEpoch, numRecords);

        buffer.position(pos);
    }

    private void writeLegacyCompressedWrapperHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initPos);

        int wrapperSize = pos - initPos - Records.LOG_OVERHEAD;
        int writtenCompressed = wrapperSize - LegacyRecord.recordOverhead(magic);
        AbstractLegacyRecordBatch.writeHeader(buffer, lastOffset, wrapperSize);

        long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
        LegacyRecord.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compressionType, timestampType);

        buffer.position(pos);

        // update the compression ratio
        this.compressionRate = (float) writtenCompressed / this.writtenUncompressed;
        TYPE_TO_RATE[compressionType.id] = TYPE_TO_RATE[compressionType.id] * COMPRESSION_RATE_DAMPING_FACTOR +
            compressionRate * (1 - COMPRESSION_RATE_DAMPING_FACTOR);
    }

    private long appendWithOffset(long offset, boolean isControlRecord, long timestamp, ByteBuffer key,
                                 ByteBuffer value, Header[] headers) {
        try {
            if (lastOffset != null && offset <= lastOffset)
                throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s (Offsets must increase monotonically).", offset, lastOffset));

            if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Invalid negative timestamp " + timestamp);

            if (magic < RecordBatch.MAGIC_VALUE_V2) {
                if (isControlRecord)
                    throw new IllegalArgumentException("Magic v" + magic + " does not support control records");
                if (headers != null && headers.length > 0)
                    throw new IllegalArgumentException("Magic v" + magic + " does not support record headers");
            }

            if (baseTimestamp == null)
                baseTimestamp = timestamp;

            if (magic > RecordBatch.MAGIC_VALUE_V1)
                return appendDefaultRecord(offset, isControlRecord, timestamp, key, value, headers);
            else
                return appendLegacyRecord(offset, timestamp, key, value);
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
     * @return crc of the record
     */
    public long appendWithOffset(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        return appendWithOffset(offset, false, timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return crc of the record
     */
    public long appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        return appendWithOffset(offset, false, timestamp, key, value, headers);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return crc of the record
     */
    public long appendWithOffset(long offset, long timestamp, byte[] key, byte[] value) {
        return appendWithOffset(offset, false, timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return crc of the record
     */
    public long appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value) {
        return appendWithOffset(offset, false, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param record The record to append
     * @return crc of the record
     */
    public long appendWithOffset(long offset, SimpleRecord record) {
        return appendWithOffset(offset, false, record.timestamp(), record.key(), record.value(), record.headers());
    }


    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return crc of the record
     */
    public long append(long timestamp, ByteBuffer key, ByteBuffer value) {
        return append(timestamp, key, value, Record.EMPTY_HEADERS);
    }
    
    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return crc of the record
     */
    public long append(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        return appendWithOffset(nextSequentialOffset(), false, timestamp, key, value, headers);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return crc of the record
     */
    public long append(long timestamp, byte[] key, byte[] value) {
        return append(timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return crc of the record
     */
    public long append(long timestamp, byte[] key, byte[] value, Header[] headers) {
        return append(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param record The record to append
     * @return crc of the record
     */
    public long append(SimpleRecord record) {
        return appendWithOffset(nextSequentialOffset(), record);
    }

    /**
     * Append a control record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param type The control record type (cannot be UNKNOWN)
     * @param value The control record value
     * @return crc of the record
     */
    public long appendControlRecord(long timestamp, ControlRecordType type, ByteBuffer value) {
        Struct keyStruct = type.recordKey();
        ByteBuffer key = ByteBuffer.allocate(keyStruct.sizeOf());
        keyStruct.writeTo(key);
        key.flip();
        return appendWithOffset(nextSequentialOffset(), true, timestamp, key, value, Record.EMPTY_HEADERS);
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
        appendWithOffset(record.offset(), record.isControlRecord(), record.timestamp(), record.key(), record.value(),
                record.headers());
    }

    /**
     * Append a log record using a different offset
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, Record record) {
        appendWithOffset(offset, record.isControlRecord(), record.timestamp(), record.key(), record.value(),
                record.headers());
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

    private long appendDefaultRecord(long offset, boolean isControlRecord, long timestamp,
                                     ByteBuffer key, ByteBuffer value, Header[] headers) throws IOException {
        ensureOpenForRecordAppend();
        int offsetDelta = (int) (offset - baseOffset);
        long timestampDelta = timestamp - baseTimestamp;
        long crc = DefaultRecord.writeTo(appendStream, isControlRecord, offsetDelta, timestampDelta, key, value, headers);
        // TODO: The crc is useless for the new message format. Maybe we should let writeTo return the written size?
        recordWritten(offset, timestamp, DefaultRecord.sizeInBytes(offsetDelta, timestampDelta, key, value, headers));
        return crc;
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
        writtenUncompressed += size;
        lastOffset = offset;

        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
            offsetOfMaxTimestamp = offset;
        }
    }

    private void ensureOpenForRecordAppend() {
        if (appendStreamIsClosed)
            throw new IllegalStateException("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
        if (isClosed())
            throw new IllegalStateException("Tried to append a record, but MemoryRecordsBuilder is closed");
    }

    private void ensureOpenForRecordBatchWrite() {
        if (isClosed())
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is closed");
    }

    /**
     * Get an estimate of the number of bytes written (based on the estimation factor hard-coded in {@link CompressionType}.
     * @return The estimated number of bytes written
     */
    private int estimatedBytesWritten() {
        if (compressionType == CompressionType.NONE) {
            return buffer().position();
        } else {
            // estimate the written bytes to the underlying byte buffer based on uncompressed written bytes
            return (int) (writtenUncompressed * TYPE_TO_RATE[compressionType.id] * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
    }

    /**
     * Check if we have room for a new record containing the given key/value pair
     *
     * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
     * accurate if compression is really used. When this happens, the following append may cause dynamic buffer
     * re-allocation in the underlying byte buffer stream.
     *
     * There is an exceptional case when appending a single message whose size is larger than the batch size, the
     * capacity will be the message size which is larger than the write limit, i.e. the batch size. In this case
     * the checking should be based on the capacity of the initialized buffer rather than the write limit in order
     * to accept this single record.
     */
    public boolean hasRoomFor(long timestamp, byte[] key, byte[] value) {
        if (isFull())
            return false;

        final int recordSize;
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            recordSize = Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
        } else {
            int nextOffsetDelta = lastOffset == null ? 0 : (int) (lastOffset - baseOffset + 1);
            long timestampDelta = baseTimestamp == null ? 0 : timestamp - baseTimestamp;
            recordSize = DefaultRecord.sizeInBytes(nextOffsetDelta, timestampDelta, key, value);
        }

        return numRecords == 0 ?
                this.initialCapacity >= recordSize :
                this.writeLimit >= estimatedBytesWritten() + recordSize;
    }

    public boolean isClosed() {
        return builtRecords != null;
    }

    public boolean isFull() {
        // note that the write limit is respected only after the first record is added which ensures we can always
        // create non-empty batches (this is used to disable batching when the producer's batch size is set to 0).
        return isClosed() || (this.numRecords > 0 && this.writeLimit <= estimatedBytesWritten());
    }

    public int sizeInBytes() {
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
     * Return the ProducerId (PID) of the RecordBatches created by this builder.
     */
    public long producerId() {
        return this.producerId;
    }

    public short producerEpoch() {
        return this.producerEpoch;
    }
}
