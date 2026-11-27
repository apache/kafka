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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.AbstractRecordsBuilder;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * An {@link AbstractRecordsBuilder} whose stream is a {@link ChunkedByteBufferOutputStream} and
 * which finalizes into a {@link CompositeMemoryRecords} — a multi-buffer records view over the
 * chunk list — instead of flattening the chunks into one contiguous {@link MemoryRecords}.
 *
 * <p>The record append machinery is inherited unchanged: records are written into the chunked
 * stream exactly as into a single buffer. Only finalization differs: the v2 batch header is written
 * into the first chunk and the batch CRC is computed across the chunks (see
 * {@link #writeDefaultBatchHeaderInto}), so no flatten is needed.
 */
public class CompositeMemoryRecordsBuilder extends AbstractRecordsBuilder {

    // This builder owns the chunk-backed stream; the base retains only the append stream.
    private final ChunkedByteBufferOutputStream stream;

    public CompositeMemoryRecordsBuilder(ChunkedByteBufferOutputStream bufferStream,
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
        super(validated(bufferStream, magic, compression), magic, compression, timestampType, baseOffset, logAppendTime,
                producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch, writeLimit,
                RecordBatch.NO_TIMESTAMP);
        this.stream = bufferStream;
    }

    /**
     * Checks the arguments before the base constructor runs, since it reserves the header in the stream and
     * wraps the stream for compression. The records are built from the start of the first chunk, so the
     * stream must not have been written to yet.
     */
    private static ChunkedByteBufferOutputStream validated(ChunkedByteBufferOutputStream bufferStream,
                                                           byte magic,
                                                           Compression compression) {
        if (magic < RecordBatch.MAGIC_VALUE_V2 || compression.type() != CompressionType.NONE) {
            throw new IllegalArgumentException("CompositeMemoryRecordsBuilder supports only uncompressed magic v2 "
                    + "batches, but got magic " + magic + " and compression " + compression.type());
        }
        if (bufferStream.position() != 0) {
            throw new IllegalArgumentException("CompositeMemoryRecordsBuilder requires a stream at position 0, but got "
                    + bufferStream.position());
        }
        return bufferStream;
    }

    @Override
    public int initialCapacity() {
        return stream.initialCapacity();
    }

    /** The chunk-backed stream, so {@link ChunkedProducerBatch} can extend and deallocate its chunks. */
    ChunkedByteBufferOutputStream bufferStream() {
        return stream;
    }

    @Override
    public CompositeMemoryRecords build() {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }
        close();
        return (CompositeMemoryRecords) builtRecords;
    }

    @Override
    public void close() {
        if (prepareClose()) {
            return;
        }

        int totalSize = stream.position() - initialPosition;

        // The flipped chunks share the stream's memory and their limits already cover the reserved
        // header space, so the header written into the first one is part of the records built from them.
        List<ByteBuffer> chunks = stream.flippedChunks();
        writeDefaultBatchHeaderInto(chunks, totalSize);

        // Uncompressed: the written size equals the uncompressed record bytes, so the ratio is ~1.
        if (uncompressedRecordsSizeInBytes > 0) {
            actualCompressionRatio = (float) (totalSize - batchHeaderSizeInBytes) / uncompressedRecordsSizeInBytes;
        }

        builtRecords = new CompositeMemoryRecords(chunks);
    }

    @Override
    protected void resetToEmpty() {
        builtRecords = CompositeMemoryRecords.EMPTY;
    }
}
