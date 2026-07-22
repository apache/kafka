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

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.AbstractRecords;
import org.apache.kafka.common.record.internal.CompressionRatioEstimator;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * A {@link RecordAccumulator} variant that backs each batch with fixed-size chunks drawn from a
 * {@link BufferPool}, attaching more chunks on demand as records are appended instead of
 * reserving {@code batch.size} per batch up front. Buffered memory therefore scales with the data
 * actually written rather than with {@code active_partition_count × batch.size}.
 * <p>
 * See {@link #append} and {@link #tryAppend} for how batches are created and grown.
 * <p>
 * TODO: support compressed data (with mid-record growth); the constructor rejects compression for now.
 */
public class ChunkedRecordAccumulator extends RecordAccumulator {

    /**
     * Fixed size of every chunk, independent of {@code batch.size}. The incremental strategy is
     * only used when {@code batch.size >= CHUNK_SIZE} (see {@code KafkaProducer}); below it a batch
     * is smaller than a single chunk, so the producer uses the full strategy instead.
     */
    public static final int CHUNK_SIZE = 16 * 1024;

    private final BufferPool chunkedFree;

    public ChunkedRecordAccumulator(LogContext logContext,
                                    int batchSize,
                                    Compression compression,
                                    int lingerMs,
                                    long retryBackoffMs,
                                    long retryBackoffMaxMs,
                                    int deliveryTimeoutMs,
                                    PartitionerConfig partitionerConfig,
                                    Metrics metrics,
                                    String metricGrpName,
                                    Time time,
                                    TransactionManager transactionManager,
                                    BufferPool bufferPool) {
        super(logContext, batchSize, compression, lingerMs, retryBackoffMs, retryBackoffMaxMs,
                deliveryTimeoutMs, partitionerConfig, metrics, metricGrpName, time, transactionManager, bufferPool);
        // TODO: drop this once the incremental strategy supports compressed data (with the
        //   mid-record growth fallback for compressor overshoot).
        if (compression.type() != CompressionType.NONE)
            throw new UnsupportedOperationException(
                    "Compression is not yet supported with the incremental buffer.memory allocation strategy");
        this.chunkedFree = bufferPool;
    }

    public ChunkedRecordAccumulator(LogContext logContext,
                                    int batchSize,
                                    Compression compression,
                                    int lingerMs,
                                    long retryBackoffMs,
                                    long retryBackoffMaxMs,
                                    int deliveryTimeoutMs,
                                    Metrics metrics,
                                    String metricGrpName,
                                    Time time,
                                    TransactionManager transactionManager,
                                    BufferPool bufferPool) {
        this(logContext, batchSize, compression, lingerMs, retryBackoffMs, retryBackoffMaxMs,
                deliveryTimeoutMs, new PartitionerConfig(), metrics, metricGrpName, time, transactionManager,
                bufferPool);
    }

    @Override
    public RecordAppendResult append(String topic,
                                     int partition,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     AppendCallbacks callbacks,
                                     long maxTimeToBlock,
                                     long nowMs,
                                     Cluster cluster) throws InterruptedException {
        TopicInfo topicInfo = topicInfoFor(topic);

        appendsInProgress.incrementAndGet();
        // The buffer stream allocated to back a new batch (sized for its first record), paired
        // with that size. Set and cleared together across retries; null when none is held.
        NewBatchBuffer newBatch = null;
        List<ByteBuffer> extensionChunks = null;
        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            while (true) {
                final BuiltInPartitioner.StickyPartitionInfo partitionInfo;
                final int effectivePartition;
                if (partition == RecordMetadata.UNKNOWN_PARTITION) {
                    partitionInfo = topicInfo.builtInPartitioner.peekCurrentPartitionInfo(cluster);
                    effectivePartition = partitionInfo.partition();
                } else {
                    partitionInfo = null;
                    effectivePartition = partition;
                }
                setPartition(callbacks, effectivePartition);

                Deque<ProducerBatch> dq = topicInfo.batches.computeIfAbsent(effectivePartition, k -> new ArrayDeque<>());
                RecordAppendResult appendResult;
                synchronized (dq) {
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster))
                        continue;

                    // The tryAppend checks the open batch (dq.peekLast()) for chunk capacity:
                    // a needsBufferExtension result means it is within its batch-size limit
                    // but its chunks lack capacity for this record, so it will allocate the gap
                    // outside the deque lock. A needsNewBatch result means there is no open batch
                    // (full or absent), so it will fall through to the first-record (new batch) path.
                    appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
                    if (appendResult.appended())
                        return updatePartitionInfoOnAppend(appendResult, topicInfo, partitionInfo, dq, cluster);
                }

                if (appendResult.needsBufferExtension()) {
                    // Mid-batch extension: the open batch can still take this record so grow it in
                    // place. The acquire is non-blocking to fail fast when the pool is exhausted:
                    // close the batch and let the record block once on the new-batch path.
                    // A blocking call would lead to the same outcome, but would block once here
                    // and still need a second blocking call to start a new batch anyways
                    // (a first blocking call here would make all open batches drainable, including this one,
                    // so most probably our batch would be gone/drained by the time memory is returned to the pool,
                    // and we would need a new batch for our record anyways).
                    try {
                        extensionChunks = chunkedFree.allocateChunks(appendResult.extensionBytesNeeded, 0L);
                    } catch (BufferExhaustedException e) {
                        log.trace("Pool exhausted while extending batch for topic {} partition {}; closing existing batch",
                                topic, effectivePartition);
                        synchronized (dq) {
                            ProducerBatch last = dq.peekLast();
                            if (last != null && last.isWritable()) {
                                last.closeForRecordAppends();
                            }
                        }
                        // Continue to the next iteration that should block to start a new batch (needsNewBatch),
                        // given that this one has been closed for appends.
                        continue;
                    }
                    nowMs = time.milliseconds();
                } else if (appendResult.needsNewBatch() && newBatch == null) {
                    // The open batch is done (e.g., full, closed) so start a new one.
                    // Block on the pool for enough chunks to fit this record, sized with the
                    // same cumulative estimator used mid-batch (header + record bytes for NONE,
                    // ratio-adjusted when compressed) so the two stay consistent.
                    int recordUncompressed = AbstractRecords.recordSizeUpperBound(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, headers);
                    int newBatchSize = MemoryRecordsBuilder.estimatedBytesWritten(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(),
                            CompressionRatioEstimator.estimation(topic, compression.type()),
                            recordUncompressed);
                    log.trace("Allocating {} byte chunked buffer ({} byte chunks) for topic {} partition {} with remaining timeout {}ms",
                            newBatchSize, chunkedFree.poolableSize(), topic, effectivePartition, maxTimeToBlock);
                    List<ByteBuffer> initialChunks;
                    try {
                        initialChunks = chunkedFree.allocateChunks(newBatchSize, maxTimeToBlock);
                    } catch (BufferExhaustedException e) {
                        // The blocking new-batch acquire was not able to get memory within
                        // max.block.ms. Record it in the buffer-exhausted metrics.
                        chunkedFree.recordBufferExhausted();
                        throw e;
                    }
                    nowMs = time.milliseconds();
                    newBatch = new NewBatchBuffer(
                            new ChunkedByteBufferOutputStream(initialChunks, chunkedFree.poolableSize(), chunkedFree),
                            newBatchSize);
                }

                synchronized (dq) {
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster)) {
                        // The partition switched while we allocated extension chunks off-lock. They
                        // were sized against the previous partition's open batch, so they must not be
                        // attached to a different partition's open batch — refund them and let the next
                        // iteration re-check the new partition from scratch.
                        deallocateExtensionChunks(extensionChunks);
                        extensionChunks = null;
                        continue;
                    }

                    if (extensionChunks != null) {
                        ProducerBatch last = dq.peekLast();
                        // The off-lock allocateChunks window allows the open batch we checked to be
                        // drained and replaced — possibly by a split batch (a plain
                        // ProducerBatch), which can't take extension chunks. Only attach to a
                        // writable chunked batch; otherwise refund the chunks and re-evaluate.
                        if (last instanceof ChunkedProducerBatch && last.isWritable()) {
                            // Attach the chunks we sized off-lock without re-checking whether a
                            // concurrent appender already grew the batch enough. Optimizes for the
                            // uncontended case; under concurrency two appenders can each attach their
                            // own gap (temporary over-allocation, one could have been enough for both).
                            // The unused chunks returns to the pool when the batch closes for appends.
                            ((ChunkedProducerBatch) last).addBuffers(extensionChunks);
                            extensionChunks = null;
                            RecordAppendResult retryResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
                            if (retryResult.appended())
                                return updatePartitionInfoOnAppend(retryResult, topicInfo, partitionInfo, dq, cluster);
                            // Still not appended: concurrent appenders filled the batch,
                            // so the extension we attached is no longer enough.
                            // Loop so the next iteration routes the record
                            // right: needsBufferExtension with a fresh gap, or needsNewBatch
                            continue;
                        }
                        // The open batch is gone, closed, or non-chunked (e.g., a split batch). Return chunks to pool.
                        deallocateExtensionChunks(extensionChunks);
                        extensionChunks = null;
                        continue;
                    }

                    // needsNewBatch path: extensionChunks == null here implies needsNewBatch,
                    // so bufferStream was allocated (this iteration or carried from a prior one).
                    if (newBatch == null)
                        throw new IllegalStateException("needsNewBatch path reached without an allocated buffer stream");
                    // Reuse the new-batch size estimate as the write-limit basis (equals the record's
                    // uncompressed upper bound without compression). TODO: review once compression lands.
                    final NewBatchBuffer pending = newBatch;
                    appendResult = appendNewBatch(topic, effectivePartition, dq, timestamp, key, value, headers, callbacks,
                            () -> chunkedRecordsBuilder(pending.stream, pending.size), nowMs);
                    if (appendResult.needsNewBatch())
                        throw new IllegalStateException("appendNewBatch must not return a needsNewBatch result");
                    if (appendResult.needsBufferExtension()) {
                        // A concurrent appender created an open batch we should extend rather
                        // than start a new one (detected by appendNewBatch's in-lock tryAppend).
                        // Our bufferStream was sized for a fresh batch — release it and loop so
                        // the extension path allocates exactly the gap-sized chunks.
                        newBatch.stream.deallocate();
                        newBatch = null;
                        continue;
                    }
                    if (appendResult.newBatchCreated)
                        newBatch = null;
                    return updatePartitionInfoOnAppend(appendResult, topicInfo, partitionInfo, dq, cluster);
                }
            }
        } finally {
            if (newBatch != null)
                newBatch.stream.deallocate();
            deallocateExtensionChunks(extensionChunks);
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * Return any held extension chunks to the pool. No-op when none are held.
     */
    private void deallocateExtensionChunks(List<ByteBuffer> extensionChunks) {
        if (extensionChunks == null)
            return;
        for (ByteBuffer chunk : extensionChunks)
            chunkedFree.deallocate(chunk);
    }

    /**
     * Try to append to a ProducerBatch, with mid-batch chunk extension support.
     * <p>
     * If the open batch is within its batch-size limit but its chunked stream lacks chunk
     * capacity, returns {@link RecordAppendResult#needsExtension(int)} without
     * attempting the append; the caller allocates chunks outside the deque lock, attaches
     * them, and retries. Otherwise defers to the parent, which appends or returns
     * {@link RecordAppendResult#NEEDS_NEW_BATCH}.
     *
     * @return a {@link RecordAppendResult#needsExtension(int) needsBufferExtension} result when the open batch is
     * within its batch-size limit but its chunks lack capacity (the append is not attempted); otherwise the
     * parent implementation's outcome: an {@code appended} result ({@link RecordAppendResult#appended()}) or
     * {@link RecordAppendResult#NEEDS_NEW_BATCH}.
     */
    @Override
    protected RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                           Callback callback, Deque<ProducerBatch> deque, long nowMs) {
        if (closed)
            throw new KafkaException("Producer closed while send in progress");
        ProducerBatch last = deque.peekLast();
        // Split batches in an incremental deque are plain ProducerBatch (heap-backed, grow-on-demand)
        // and never need chunk extension, so the check only applies to chunked batches.
        if (last instanceof ChunkedProducerBatch) {
            int extensionBytes = ((ChunkedProducerBatch) last).extensionBytesNeeded(timestamp, key, value, headers);
            if (extensionBytes > 0)
                return RecordAppendResult.needsExtension(extensionBytes);
        }
        return super.tryAppend(timestamp, key, value, headers, callback, deque, nowMs);
    }

    @Override
    protected ProducerBatch createProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long nowMs) {
        return new ChunkedProducerBatch(tp, recordsBuilder, nowMs);
    }

    /**
     * Build a {@link MemoryRecordsBuilder} backed by the chunked stream.
     *
     * @param bufferStream    the chunked stream backing the batch
     * @param firstRecordSize the first record's uncompressed size upper bound. Used to set the
     *                        builder's write limit used by {@code hasRoomFor}/{@code isFull}
     */
    private MemoryRecordsBuilder chunkedRecordsBuilder(ChunkedByteBufferOutputStream bufferStream,
                                                       int firstRecordSize) {
        int writeLimit = Math.max(batchSize, firstRecordSize);
        return new MemoryRecordsBuilder(bufferStream, RecordBatch.CURRENT_MAGIC_VALUE, compression,
                TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, writeLimit);
    }

    /**
     * A buffer stream allocated to back a new batch, sized to fit the batch's first record,
     * paired with that size. That same size is also used as the batch's write-limit basis (see
     * {@link #chunkedRecordsBuilder}). The two are always set and cleared together in {@link #append},
     * including across retries.
     */
    private static final class NewBatchBuffer {
        final ChunkedByteBufferOutputStream stream;
        final int size;

        NewBatchBuffer(ChunkedByteBufferOutputStream stream, int size) {
            this.stream = stream;
            this.size = size;
        }
    }
}
