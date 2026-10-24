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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.internals.ByteBufferOutputStream;
import org.apache.kafka.common.utils.internals.Checksums;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * Abstract base for record-set builders that accumulate records into a {@link ByteBufferOutputStream}
 * and finalize them into a {@link BaseRecords}. It holds the shared state and the append /
 * compression / header-writing machinery; subclasses choose how the accumulated bytes are
 * materialized into a concrete records type by implementing {@link #build()} and {@link #close()}.
 *
 * <p>The single-buffer implementation is {@link MemoryRecordsBuilder}, which materializes into a
 * {@link MemoryRecords}. A subclass backed by a chunk list can materialize into a multi-buffer
 * records type by writing the batch header into its first chunk and computing the CRC across all
 * chunks (see {@link #writeDefaultBatchHeaderInto}), avoiding a flatten.
 *
 * <p>In cases where keeping memory retention low is important and there's a gap between the time
 * that record appends stop and the builder is closed (e.g. the Producer), it's important to call
 * {@link #closeForRecordAppends} when the former happens. This releases resources like compression
 * buffers that can be relatively large (64 KB for LZ4).
 */
public abstract class AbstractRecordsBuilder implements AutoCloseable {
    private static final float COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;
    protected static final DataOutputStream CLOSED_STREAM = new DataOutputStream(new OutputStream() {
        @Override
        public void write(int b) {
            throw new IllegalStateException("The records builder is closed for record appends");
        }
    });

    protected final TimestampType timestampType;
    protected final Compression compression;
    protected final byte magic;
    protected final int initialPosition;
    protected final long baseOffset;
    protected final long logAppendTime;
    protected final boolean isControlBatch;
    protected final int partitionLeaderEpoch;
    protected final int writeLimit;
    protected final int batchHeaderSizeInBytes;
    protected final long deleteHorizonMs;

    // Use a conservative estimate of the compression ratio. The producer overrides this using statistics
    // from previous batches before appending any records.
    private float estimatedCompressionRatio = 1.0F;

    // Used to append records, may compress data on the fly
    protected DataOutputStream appendStream;
    protected boolean isTransactional;
    protected long producerId;
    protected short producerEpoch;
    protected int baseSequence;
    protected int uncompressedRecordsSizeInBytes; // Number of bytes (excluding the header) written before compression
    protected int numRecords;
    protected float actualCompressionRatio;
    protected long maxTimestamp;
    protected long offsetOfMaxTimestamp = -1;
    protected Long lastOffset = null;
    protected Long baseTimestamp = null;

    protected BaseRecords builtRecords;
    protected boolean aborted = false;

    /**
     * {@code bufferStream} is used only to set up appends — capture {@link #initialPosition}, reserve
     * the batch header, and wrap it for output — and is not retained: the concrete stream is owned by
     * the subclass, which exposes what the base needs through {@link #initialCapacity()}.
     */
    protected AbstractRecordsBuilder(ByteBufferOutputStream bufferStream,
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
        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("TimestampType must be set for magic >= 0");
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            if (isTransactional)
                throw new IllegalArgumentException("Transactional records are not supported for magic " + magic);
            if (isControlBatch)
                throw new IllegalArgumentException("Control records are not supported for magic " + magic);
            if (compression.type() == CompressionType.ZSTD)
                throw new IllegalArgumentException("ZStandard compression is not supported for magic " + magic);
            if (deleteHorizonMs != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Delete horizon timestamp is not supported for magic " + magic);
        }

        this.magic = magic;
        this.timestampType = timestampType;
        this.compression = compression;
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
        this.deleteHorizonMs = deleteHorizonMs;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.writeLimit = writeLimit;
        this.batchHeaderSizeInBytes = AbstractRecords.recordBatchHeaderSizeInBytes(magic, compression.type());
        this.initialPosition = bufferStream.position();
        bufferStream.position(initialPosition + batchHeaderSizeInBytes);
        this.appendStream = new DataOutputStream(compression.wrapForOutput(bufferStream, magic));

        if (hasDeleteHorizonMs()) {
            this.baseTimestamp = deleteHorizonMs;
        }
    }

    /**
     * The capacity of the (first) backing buffer, for pool accounting. Provided by the subclass,
     * which owns the stream.
     */
    public abstract int initialCapacity();

    public double compressionRatio() {
        return actualCompressionRatio;
    }

    public Compression compression() {
        return compression;
    }

    public boolean isControlBatch() {
        return isControlBatch;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    public final boolean hasDeleteHorizonMs() {
        return magic >= RecordBatch.MAGIC_VALUE_V2 && deleteHorizonMs >= 0L;
    }

    /**
     * Close this builder and return the resulting records. Subclasses produce their specific
     * records type (e.g. a single {@link MemoryRecords} or a multi-buffer records type).
     */
    public abstract BaseRecords build();

    public int numRecords() {
        return numRecords;
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

    /**
     * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
     * possible to update the RecordBatch header.
     */
    public void closeForRecordAppends() {
        if (appendStream != CLOSED_STREAM) {
            try {
                appendStream.close();
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                appendStream = CLOSED_STREAM;
            }
        }
    }

    public void abort() {
        closeForRecordAppends();
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

    /**
     * Finalize the batch (write the header and set {@link #builtRecords}). Subclasses choose whether
     * to materialize the backing bytes into a single {@link MemoryRecords} or a multi-buffer type.
     */
    @Override
    public abstract void close();

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
     * Shared preamble for {@link #close()}: validate, release append resources, and (for an empty
     * batch) reset to an empty record set. Returns {@code true} if the caller should stop (already
     * built, or nothing to finalize), {@code false} if the caller must still write the header and
     * materialize the {@link #numRecords} records.
     */
    protected boolean prepareClose() {
        if (aborted)
            throw new IllegalStateException("Cannot close " + getClass().getSimpleName() + " as it has already been aborted");

        if (builtRecords != null)
            return true;

        validateProducerState();
        closeForRecordAppends();

        if (numRecords == 0L) {
            resetToEmpty();
            return true;
        }
        return false;
    }

    /**
     * Finalize an empty batch by setting {@link #builtRecords} to {@link MemoryRecords#EMPTY}.
     */
    protected void resetToEmpty() {
        builtRecords = MemoryRecords.EMPTY;
    }

    /**
     * Writes the v2 batch header into the first of {@code buffers} at {@link #initialPosition}, then the
     * CRC computed across all of them. {@code buffers} hold the batch's bytes in order, each from position 0
     * to its limit, and the header fits in the first one (its {@link #batchHeaderSizeInBytes} were reserved
     * at construction). The buffers' positions are left unchanged.
     *
     * @param buffers   the buffers backing the batch, in order
     * @param totalSize the total size of the batch including the header, across all buffers
     */
    protected void writeDefaultBatchHeaderInto(List<ByteBuffer> buffers, int totalSize) {
        ByteBuffer first = buffers.get(0);
        int offsetDelta = (int) (lastOffset - baseOffset);
        final long effectiveMaxTimestamp = timestampType == TimestampType.LOG_APPEND_TIME
                ? logAppendTime : this.maxTimestamp;

        int savedPos = first.position();
        first.position(initialPosition);
        DefaultRecordBatch.writeHeaderFieldsExceptCrc(first, baseOffset, offsetDelta, totalSize, magic,
                compression.type(), timestampType, baseTimestamp, effectiveMaxTimestamp, producerId,
                producerEpoch, baseSequence, isTransactional, isControlBatch, hasDeleteHorizonMs(),
                partitionLeaderEpoch, numRecords);
        first.position(savedPos);

        long crc = crc32c(buffers, initialPosition + DefaultRecordBatch.ATTRIBUTES_OFFSET,
                totalSize - DefaultRecordBatch.ATTRIBUTES_OFFSET);
        first.putInt(initialPosition + DefaultRecordBatch.CRC_OFFSET, (int) crc);
    }

    /**
     * The CRC32C over {@code [absoluteStart, absoluteStart + length)} of the buffers' concatenated bytes,
     * computed buffer by buffer so they are never copied into one.
     */
    private static long crc32c(List<ByteBuffer> buffers, int absoluteStart, int length) {
        Checksum crc = new CRC32C();
        int globalOffset = 0;
        int remaining = length;
        for (ByteBuffer buffer : buffers) {
            int bufferLength = buffer.limit();
            if (globalOffset + bufferLength <= absoluteStart) {
                globalOffset += bufferLength;
                continue;
            }
            int localStart = Math.max(absoluteStart - globalOffset, 0);
            int toUse = Math.min(bufferLength - localStart, remaining);
            Checksums.update(crc, buffer, localStart, toUse);
            remaining -= toUse;
            if (remaining <= 0) {
                break;
            }
            globalOffset += bufferLength;
        }
        return crc.getValue();
    }

    /**
     * Append a new record at the given offset.
     */
    protected void appendWithOffset(long offset, boolean isControlRecord, long timestamp, ByteBuffer key,
                                    ByteBuffer value, Header[] headers) {
        try {
            if (isControlRecord != isControlBatch)
                throw new IllegalArgumentException("Control records can only be appended to control batches");

            if (lastOffset != null && offset <= lastOffset)
                throw new IllegalArgumentException(String.format("Illegal offset %d following previous offset %d " +
                        "(Offsets must increase monotonically).", offset, lastOffset));

            if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Invalid negative timestamp " + timestamp);

            if (magic < RecordBatch.MAGIC_VALUE_V2 && headers != null && headers.length > 0)
                throw new IllegalArgumentException("Magic v" + magic + " does not support record headers");

            if (baseTimestamp == null)
                baseTimestamp = timestamp;

            if (magic > RecordBatch.MAGIC_VALUE_V1) {
                appendDefaultRecord(offset, timestamp, key, value, headers);
            } else {
                appendLegacyRecord(offset, timestamp, key, value, magic);
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
     */
    public void appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        appendWithOffset(offset, false, timestamp, key, value, headers);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     */
    public void append(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        appendWithOffset(nextSequentialOffset(), timestamp, key, value, headers);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     */
    public void append(long timestamp, byte[] key, byte[] value, Header[] headers) {
        append(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    private void appendDefaultRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value,
                                     Header[] headers) throws IOException {
        ensureOpenForRecordAppend();
        int offsetDelta = (int) (offset - baseOffset);
        long timestampDelta = timestamp - baseTimestamp;
        int sizeInBytes = DefaultRecord.writeTo(appendStream, offsetDelta, timestampDelta, key, value, headers);
        recordWritten(offset, timestamp, sizeInBytes);
    }

    private long appendLegacyRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value, byte magic) throws IOException {
        ensureOpenForRecordAppend();

        int size = LegacyRecord.recordSize(magic, key, value);
        AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

        if (timestampType == TimestampType.LOG_APPEND_TIME)
            timestamp = logAppendTime;
        long crc = LegacyRecord.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);
        recordWritten(offset, timestamp, size + Records.LOG_OVERHEAD);
        return crc;
    }

    protected long toInnerOffset(long offset) {
        // use relative offsets for compressed messages with magic v1
        if (magic > 0 && compression.type() != CompressionType.NONE)
            return offset - baseOffset;
        return offset;
    }

    protected void recordWritten(long offset, long timestamp, int size) {
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

    protected void ensureOpenForRecordAppend() {
        if (appendStream == CLOSED_STREAM)
            throw new IllegalStateException("Tried to append a record, but " + getClass().getSimpleName() + " is closed for record appends");
    }

    /**
     * Get an estimate of the number of bytes written (based on the estimation factor hard-coded in {@link CompressionType}).
     * @return The estimated number of bytes written
     */
    protected int estimatedBytesWritten() {
        return estimatedBytesWritten(uncompressedRecordsSizeInBytes);
    }

    /**
     * Returns the projected number of bytes the builder would write for the given uncompressed
     * record bytes: exact for uncompressed, a ratio-aware estimate for compressed.
     */
    private int estimatedBytesWritten(int uncompressedSize) {
        if (compression.type() == CompressionType.NONE) {
            return batchHeaderSizeInBytes + uncompressedSize;
        } else {
            return batchHeaderSizeInBytes + (int) (uncompressedSize * estimatedCompressionRatio * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
    }

    /**
     * Projected value of {@link #estimatedBytesWritten} after appending one more record with the
     * given fields, using the record's worst-case (upper-bound) per-record size. Used by the
     * incremental strategy to size mid-batch chunk extensions.
     */
    public int estimatedBytesWrittenAfter(byte[] key, byte[] value, Header[] headers) {
        ByteBuffer keyBuffer = wrapNullable(key);
        ByteBuffer valueBuffer = wrapNullable(value);
        final int recordSize;
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            recordSize = Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, keyBuffer, valueBuffer);
        } else {
            recordSize = DefaultRecord.recordSizeUpperBound(keyBuffer, valueBuffer, headers);
        }
        return estimatedBytesWritten(uncompressedRecordsSizeInBytes + recordSize);
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
            long timestampDelta = baseTimestamp == null ? 0 : timestamp - baseTimestamp;
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
        return appendStream == CLOSED_STREAM || (this.numRecords > 0 && this.writeLimit <= estimatedBytesWritten());
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

    protected long nextSequentialOffset() {
        return lastOffset == null ? baseOffset : lastOffset + 1;
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
