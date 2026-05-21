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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.nio.ByteBuffer;

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

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream,
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
        super(bufferStream, magic, compression, timestampType, baseOffset, logAppendTime, producerId, producerEpoch,
                baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch, writeLimit, deleteHorizonMs);
    }

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream,
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
        this(new ByteBufferOutputStream(buffer), magic, compression, timestampType, baseOffset, logAppendTime,
                producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch,
                writeLimit);
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

    @Override
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
            else if (compression.type() != CompressionType.NONE)
                this.actualCompressionRatio = (float) writeLegacyCompressedWrapperHeader() / this.uncompressedRecordsSizeInBytes;

            ByteBuffer buffer = buffer().duplicate();
            buffer.flip();
            buffer.position(initialPosition);
            builtRecords = MemoryRecords.readableRecords(buffer.slice());
        }
    }
}
