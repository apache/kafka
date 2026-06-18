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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A {@link ProducerBatch} for the incremental (chunked) buffer.memory allocation strategy, backed
 * by a {@link MemoryRecordsBuilder} whose stream is a {@link ChunkedByteBufferOutputStream}.
 * It adds mid-batch chunk extension support ({@link #extensionBytesNeeded} /
 * {@link #addBuffers}) and overrides the pool deallocation hooks so all chunks are returned to
 * the pool rather than a single buffer.
 * <p>
 * This class is not thread safe and external synchronization must be used when modifying it.
 */
public class ChunkedProducerBatch extends ProducerBatch {

    // The parent's builder reference is private; keep our own to access stream capacity state.
    private final MemoryRecordsBuilder recordsBuilder;

    public ChunkedProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs) {
        super(tp, recordsBuilder, createdMs);
        this.recordsBuilder = recordsBuilder;
    }

    /**
     * Bytes of physical buffer this batch needs before {@code tryAppend} could accept the given
     * record. Returns 0 when no extension is needed: the batch is empty (first record), it is
     * logically full, or the stream's combined chunk capacity already has room. Positive when
     * {@code hasRoomFor} allows the record but the
     * chunks lack physical capacity — the accumulator allocates exactly the missing bytes
     * (rounded up to whole chunks) and attaches them via {@link #addBuffers} before retrying.
     */
    int extensionBytesNeeded(long timestamp, byte[] key, byte[] value, Header[] headers) {
        if (recordCount == 0)
            return 0;
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers))
            return 0;
        // Size against the batch's projected total output after this record (header counted once,
        // ratio-adjusted when compressed), not per-record. Per-record sizing would over-count the
        // header and miss the compressor's flush-accumulation behavior.
        int target = recordsBuilder.estimatedBytesWrittenAfter(key, value, headers);
        ByteBufferOutputStream stream = recordsBuilder.bufferStream();
        int totalAttachedCapacity = stream.position() + stream.remaining();
        return Math.max(0, target - totalAttachedCapacity);
    }

    /**
     * Attach pre-allocated chunks to this batch's stream so the next {@code tryAppend} can
     * spill into them. Ownership of the chunks transfers to the stream.
     */
    void addBuffers(List<ByteBuffer> chunks) {
        stream().addBuffers(chunks);
    }

    @Override
    protected void deallocateBuffer(BufferPool pool) {
        stream().deallocate(pool);
    }

    /**
     * Unlike the single-buffer batch — which must donate a fresh buffer because the network
     * layer may still be reading the pooled one — a chunked batch's inflight bytes live in the
     * separate flattened buffer (see {@link ChunkedByteBufferOutputStream#buffer()}), so it is
     * safe to return the actual chunks to the pool here.
     */
    @Override
    protected void deallocateInflightBuffer(BufferPool pool) {
        stream().deallocate(pool);
    }

    private ChunkedByteBufferOutputStream stream() {
        return (ChunkedByteBufferOutputStream) recordsBuilder.bufferStream();
    }
}
