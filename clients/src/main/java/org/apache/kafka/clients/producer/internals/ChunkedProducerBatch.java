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

    public ChunkedProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs) {
        super(tp, recordsBuilder, createdMs);
        if (!(recordsBuilder.bufferStream() instanceof ChunkedByteBufferOutputStream))
            throw new IllegalArgumentException("recordsBuilder must be an instance of "
                    + ChunkedByteBufferOutputStream.class.getSimpleName() + ", but found "
                    + recordsBuilder.bufferStream().getClass().getName());
    }

    /**
     * Bytes of chunk capacity this batch needs before {@code tryAppend} could accept the given
     * record. Returns 0 when no extension is needed: the batch is at its batch-size limit, or the
     * attached chunk capacity already has room (always the case for an empty batch, whose stream
     * is pre-sized for the first record). Positive when the record is within the batch-size limit
     * but the attached chunks lack capacity — the accumulator then allocates exactly the missing
     * bytes (rounded up to whole chunks) and attaches them via {@link #addBuffers} before retrying.
     * <p>
     * TODO (KAFKA-20859): improve by reusing size calculation.
     */
    int extensionBytesNeeded(long timestamp, byte[] key, byte[] value, Header[] headers) {
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers))
            return 0;
        // Size against the batch's projected total output after this record (header counted once,
        // ratio-adjusted when compressed), not per-record. Per-record sizing would over-count the
        // header and miss the compressor's flush-accumulation behavior.
        int target = recordsBuilder.estimatedBytesWrittenAfter(key, value, headers);
        return Math.max(0, target - stream().attachedCapacity());
    }

    /**
     * Appends the record. This batch never acquires memory itself: the capacity is arranged before
     * the append, by the accumulator attaching chunks (see {@link #extensionBytesNeeded} and
     * {@link #addBuffers}).
     * <p>
     * The capacity is verified here only for the batch's first record, which is the one append no
     * caller checks: {@code RecordAccumulator.appendNewBatch} creates the batch and appends straight
     * into it, relying on the stream having been pre-sized. Every later append arrives through
     * {@code ChunkedRecordAccumulator.tryAppend}, which evaluates {@link #extensionBytesNeeded}
     * itself and attaches chunks before retrying, so repeating the check for those would size the
     * record a second time on every append for no added safety.
     *
     * @return the record's future, or null if the batch is at its batch-size limit
     * @throws IllegalStateException if the stream was not pre-sized to hold the batch's first record
     */
    @Override
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        if (recordCount == 0 && extensionBytesNeeded(timestamp, key, value, headers) != 0)
            throw new IllegalStateException(
                    "Unexpected append to a chunked batch whose chunks lack capacity for the record; " +
                            "the stream should have been pre-sized for the batch's first record");
        return super.tryAppend(timestamp, key, value, headers, callback, now);
    }

    /**
     * Attach pre-allocated chunks to this batch's stream so the next {@code tryAppend} can
     * spill into them. Ownership of the chunks transfers to the stream.
     * <p>
     * The accumulator calls this only while {@link #extensionBytesNeeded} is positive, under the same
     * deque lock, so the chunks are still needed when attached. Any never written to are returned to
     * the pool when the batch closes for appends.
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
     * <p>
     * TODO (KAFKA-20580): review when removing the flatten.
     *  Once we send directly from the chunks, the chunks themselves hold the
     *  inflight bytes so would be unsafe to return them here.
     */
    @Override
    protected void deallocateInflightBuffer(BufferPool pool) {
        stream().deallocate(pool);
    }

    private ChunkedByteBufferOutputStream stream() {
        return (ChunkedByteBufferOutputStream) recordsBuilder.bufferStream();
    }
}
