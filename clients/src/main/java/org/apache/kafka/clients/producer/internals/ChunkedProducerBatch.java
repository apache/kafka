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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A {@link ProducerBatch} for the incremental buffer.memory allocation strategy, backed
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
     * Bytes of chunk capacity this batch needs before {@code tryAppend} could accept the given
     * record. Returns 0 when no extension is needed: the batch is at its batch-size limit, or the
     * attached chunk capacity already has room (always the case for an empty batch, whose stream
     * is pre-sized for the first record). Positive when the record is within the batch-size limit
     * but the attached chunks lack capacity — the accumulator then allocates exactly the missing
     * bytes (rounded up to whole chunks) and attaches them via {@link #addBuffers} before retrying.
     */
    int extensionBytesNeeded(long timestamp, byte[] key, byte[] value, Header[] headers) {
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
     * Appends the record after asserting there's capacity for it.
     * At this point it's expected that the allocated chunks have
     * capacity for the record because the accumulator never routes an
     * append here without ensuring chunk capacity first: the first-record path pre-sizes the
     * stream for the record and the mid-batch path attaches extension chunks before retrying.
     */
    @Override
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        assert extensionBytesNeeded(timestamp, key, value, headers) == 0 :
                "Unexpected append to a chunked batch whose chunks lack capacity for the record; " +
                        "the accumulator should have attached extension chunks first";
        return super.tryAppend(timestamp, key, value, headers, callback, now);
    }

    /**
     * Attach pre-allocated chunks to this batch's stream so the next {@code tryAppend} can
     * spill into them. Ownership of the chunks transfers to the stream.
     * <p>
     * Concurrent appenders may over-attach (each sizes its own gap against the same remaining
     * capacity), attaching more than the batch-size limit can use; chunks left fully unused are
     * returned to the pool when the batch closes.
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
