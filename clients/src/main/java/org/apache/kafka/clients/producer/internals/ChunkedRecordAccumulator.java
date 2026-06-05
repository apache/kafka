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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.AbstractRecords;
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
 * A {@link RecordAccumulator} variant that uses chunked buffer allocation: each new batch
 * pre-allocates {@code ceil(recordSize / chunkSize)} chunks from a {@link ChunkedBufferPool},
 * and grows mid-batch by attaching additional chunks via
 * {@link ProducerBatch#addBuffers(List)} when later records would overflow.
 * <p>
 * This is the "dynamic" buffer.memory allocation strategy: memory consumption scales with
 * actual data buffered, not with {@code active_partition_count × batch.size}.
 * <p>
 * <b>PR1 scope:</b> uncompressed only. Batch creation throws
 * {@link UnsupportedOperationException} if compression is requested. The mid-batch flow
 * matches the KIP: try a non-blocking pool acquire for the extension chunks; on
 * {@link BufferExhaustedException}, close the existing batch (so the sender can drain it)
 * and let the new record flow through the first-record path, which blocks on the pool for
 * a fresh batch's chunks up to {@code max.block.ms}.
 * <p>
 * TODO: add the new buffer.memory.allocation.strategy config and instantiate this class only when set to "dynamic"
 * TODO: add support for compressed data. Throws UnsupportedOperationException for now,
 *   and IllegalStateException if overflow detected. Follow-ups will support compressed data and growth.
 */
public class ChunkedRecordAccumulator extends RecordAccumulator {

    private final ChunkedBufferPool chunkedFree;

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
                                    ChunkedBufferPool bufferPool) {
        super(logContext, batchSize, compression, lingerMs, retryBackoffMs, retryBackoffMaxMs,
                deliveryTimeoutMs, partitionerConfig, metrics, metricGrpName, time, transactionManager, bufferPool);
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
                                    ChunkedBufferPool bufferPool) {
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
        TopicInfo topicInfo = topicInfoMap.computeIfAbsent(topic,
                k -> new TopicInfo(createBuiltInPartitioner(logContext, k, batchSize, partitionerRackAware, rack)));

        appendsInProgress.incrementAndGet();
        ChunkedByteBufferOutputStream bufferStream = null;
        List<ByteBuffer> extensionChunks = null;
        int extensionBytes = 0;
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
                synchronized (dq) {
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster))
                        continue;

                    // Probe the tail directly. If the existing batch has logical room but the
                    // underlying stream lacks physical capacity, capture the gap and fall through
                    // to allocate the extension outside the deque lock. Otherwise, attempt the
                    // append against the existing batch.
                    ProducerBatch tail = dq.peekLast();
                    int gap = tail == null ? 0 : tail.extensionBytesNeeded(timestamp, key, value, headers);
                    if (gap > 0) {
                        extensionBytes = gap;
                    } else {
                        extensionBytes = 0;  // clear any stale value carried from a prior iteration
                        RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
                        if (appendResult != null) {
                            boolean enableSwitch = allBatchesFull(dq);
                            topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, appendResult.appendedBytes, cluster, enableSwitch);
                            return appendResult;
                        }
                    }
                }

                if (extensionBytes > 0 && extensionChunks == null) {
                    // Mid-batch extension: non-blocking attempt only. The producer thread already
                    // holds chunks for the open batch; blocking here risks deadlock with the Sender
                    // thread (which frees pool memory by completing batches). If the pool is
                    // exhausted, we close the existing batch (so it becomes drainable) and fall
                    // through to the first-record blocking path on the next loop iteration.
                    try {
                        extensionChunks = chunkedFree.allocateChunks(extensionBytes, 0L);
                    } catch (BufferExhaustedException e) {
                        log.trace("Pool exhausted while extending batch for topic {} partition {}; closing existing batch",
                                topic, effectivePartition);
                        synchronized (dq) {
                            ProducerBatch last = dq.peekLast();
                            if (last != null && last.isWritable()) {
                                last.closeForRecordAppends();
                            }
                        }
                        extensionBytes = 0;
                        continue;
                    }
                    nowMs = time.milliseconds();
                } else if (extensionBytes == 0 && bufferStream == null) {
                    // First-record path: block on the pool for enough chunks to fit this record.
                    // Temporary while compressed data not supported.
                    // TODO: drop this throw and route through the
                    //   compressed path (which adds the mid-record growth fallback for compressor overshoot).
                    if (compression.type() != CompressionType.NONE) {
                        throw new UnsupportedOperationException(
                                "Compression is not implemented yet for the dynamic buffer.memory allocation strategy");
                    }
                    int size = AbstractRecords.estimateSizeInBytesUpperBound(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, headers);
                    log.trace("Allocating {} byte chunked buffer ({} byte chunks) for topic {} partition {} with remaining timeout {}ms",
                            size, chunkedFree.poolableSize(), topic, effectivePartition, maxTimeToBlock);
                    List<ByteBuffer> initialChunks = chunkedFree.allocateChunks(size, maxTimeToBlock);
                    nowMs = time.milliseconds();
                    bufferStream = new ChunkedByteBufferOutputStream(initialChunks, chunkedFree.poolableSize(), chunkedFree);
                }

                synchronized (dq) {
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster))
                        continue;

                    if (extensionChunks != null) {
                        ProducerBatch last = dq.peekLast();
                        // The off-lock allocateChunks window allows the original tail batch to be
                        // drained and a split batch (plain ByteBufferOutputStream) to be addFirst'd
                        // into an emptied deque — see splitAndReenqueue. The instanceof guard below
                        // is load-bearing: addBuffers is only defined on ChunkedByteBufferOutputStream;
                        // the else branch returns the chunks to the pool to avoid leaking them.
                        if (last != null && last.isWritable()
                                && last.bufferStream() instanceof ChunkedByteBufferOutputStream) {
                            ((ChunkedByteBufferOutputStream) last.bufferStream()).addBuffers(extensionChunks);
                            extensionChunks = null;
                            extensionBytes = 0;
                            // Re-probe after attach: between our first-lock probe and now we were off-lock,
                            // and another appender (or a tail-replacement) may have consumed capacity that
                            // our extension allocation was sized against. If a gap remains, loop to acquire
                            // more chunks against the now-current state. Without this re-probe, a
                            // tryAppend that passes hasRoomFor (writeLimit) but exceeds the stream's
                            // physical remaining() trips IllegalStateException in
                            // ChunkedByteBufferOutputStream.advanceToNextChunk. Termination: writeLimit is
                            // bounded and fixed; once hasRoomFor turns false, extensionBytesNeeded returns 0
                            // and we fall through to new-batch creation. Regression test:
                            // testConcurrentExtensionRaceDoesNotOverflowChunkedStream.
                            int recheck = last.extensionBytesNeeded(timestamp, key, value, headers);
                            if (recheck > 0) {
                                extensionBytes = recheck;
                                continue;
                            }
                            RecordAppendResult retryResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
                            if (retryResult != null) {
                                boolean enableSwitch = allBatchesFull(dq);
                                topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, retryResult.appendedBytes, cluster, enableSwitch);
                                return retryResult;
                            }
                            // Batch became full via writeLimit while we were off-lock — fall through
                            // to new-batch creation on the next iteration.
                            continue;
                        }
                        // Tail is gone, closed, or non-chunked (e.g., a split batch). Return chunks to pool.
                        for (ByteBuffer chunk : extensionChunks)
                            chunkedFree.deallocate(chunk);
                        extensionChunks = null;
                        extensionBytes = 0;
                        continue;
                    }

                    // bufferStream is non-null here (first-record path). Before creating a new
                    // batch with it, re-probe the tail: a concurrent appender may have created a
                    // chunked batch we should extend instead. If so, release the oversized
                    // bufferStream and let the next iteration take the extension path.
                    ProducerBatch tail = dq.peekLast();
                    if (tail != null && tail.isWritable()
                            && tail.bufferStream() instanceof ChunkedByteBufferOutputStream
                            && tail.extensionBytesNeeded(timestamp, key, value, headers) > 0) {
                        bufferStream.deallocate();
                        bufferStream = null;
                        continue;
                    }

                    int firstRecordSize = AbstractRecords.estimateSizeInBytesUpperBound(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, headers);
                    final ChunkedByteBufferOutputStream batchStream = bufferStream;
                    RecordAppendResult appendResult = appendNewBatch(topic, effectivePartition, dq, timestamp, key, value, headers, callbacks,
                            () -> chunkedRecordsBuilder(batchStream, firstRecordSize), nowMs);
                    if (appendResult.newBatchCreated)
                        bufferStream = null;
                    boolean enableSwitch = allBatchesFull(dq);
                    topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, appendResult.appendedBytes, cluster, enableSwitch);
                    return appendResult;
                }
            }
        } finally {
            if (bufferStream != null)
                bufferStream.deallocate();
            if (extensionChunks != null) {
                for (ByteBuffer chunk : extensionChunks)
                    chunkedFree.deallocate(chunk);
            }
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * Build a {@link MemoryRecordsBuilder} backed by the chunked stream.
     * {@code writeLimit} is set to {@code max(batchSize, firstRecordSize)} — the same logical
     * cap the legacy path uses. Physical chunk capacity grows on demand via
     * {@link ChunkedByteBufferOutputStream#addBuffers(List)}; {@code writeLimit} bounds when a
     * batch is considered logically full and a new one must be started.
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
     * Chunked-aware deallocation for {@link ProducerBatch}.
     * <p>
     * Intercepts the {@link #isInflight()} branch of the parent's
     * {@link RecordAccumulator#deallocate(ProducerBatch)} when the batch is backed by a
     * {@link ChunkedByteBufferOutputStream}. The parent's defensive path credits only
     * {@code initialCapacity} (one {@code chunkSize}) back to the pool and throws — that's the
     * KAFKA-19012-aware safety net for the legacy single-buffer path, where the in-flight
     * buffer <i>is</i> the pooled buffer. A chunked batch holds K chunks but the in-flight
     * network bytes live in a separate flattened heap buffer (see
     * {@link ChunkedByteBufferOutputStream#buffer()}), so it is safe to return all K chunks
     * to the pool here. The {@code IllegalStateException} is still propagated to preserve the
     * contract that deallocating an inflight batch is unexpected upstream.
     */
    @Override
    public void deallocate(ProducerBatch batch) {
        if (!batch.isSplitBatch()
                && !batch.isBufferDeallocated()
                && batch.isInflight()
                && batch.bufferStream() instanceof ChunkedByteBufferOutputStream) {
            batch.markBufferDeallocated();
            deallocateBatchBuffer(batch);
            throw new IllegalStateException("Attempting to deallocate a batch that is inflight. Batch is " + batch);
        }
        super.deallocate(batch);
    }

    /**
     * Route chunked batch deallocation through the stream rather than the legacy
     * single-buffer pool path. The flattened buffer returned by {@code batch.buffer()} is a
     * fresh heap allocation and must NOT be passed to the pool — instead, the stream owns
     * the chunk-sized buffers and returns them to the pool.
     */
    @Override
    protected void deallocateBatchBuffer(ProducerBatch batch) {
        if (batch.bufferStream() instanceof ChunkedByteBufferOutputStream) {
            ((ChunkedByteBufferOutputStream) batch.bufferStream()).deallocate(chunkedFree);
        } else {
            // Split batches and any non-chunked batches go through the default deallocation.
            super.deallocateBatchBuffer(batch);
        }
    }
}
