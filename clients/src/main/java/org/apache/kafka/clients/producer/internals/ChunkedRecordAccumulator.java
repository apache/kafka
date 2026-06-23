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
 * {@link ChunkedBufferPool}, attaching more chunks on demand as records are appended instead of
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
        int extensionBytes;
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

                    // The tryAppend override checks the open batch (dq.peekLast()) for chunk
                    // capacity: a needsBufferExtension result means it is within its batch-size limit
                    // but its chunks lack capacity for this record — fall through to allocate the gap
                    // outside the deque lock. A null result means there is no open batch (it is full
                    // or absent) — fall through to the first-record (new batch) path.
                    RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
                    if (appendResult != null && !appendResult.needsBufferExtension) {
                        boolean enableSwitch = allBatchesFull(dq);
                        topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, appendResult.appendedBytes, cluster, enableSwitch);
                        return appendResult;
                    }
                    extensionBytes = appendResult == null ? 0 : appendResult.extensionBytesNeeded;
                }

                if (extensionBytes > 0) {
                    // Mid-batch extension: non-blocking only. The thread already holds the open
                    // batch's chunks, so blocking here could deadlock with the Sender (which frees
                    // pool memory by completing batches). On exhaustion, close the batch (making it
                    // drainable) and fall through to the blocking first-record path next iteration.
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
                        continue;
                    }
                    nowMs = time.milliseconds();
                } else if (extensionBytes == 0 && bufferStream == null) {
                    // First-record path: block on the pool for enough chunks to fit this record,
                    // sized with the same cumulative estimator used mid-batch (header + record bytes
                    // for NONE, ratio-adjusted when compressed) so the two stay consistent.
                    int recordUncompressed = AbstractRecords.recordSizeUpperBound(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, headers);
                    int size = MemoryRecordsBuilder.estimatedBytesWritten(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(),
                            CompressionRatioEstimator.estimation(topic, compression.type()),
                            recordUncompressed);
                    log.trace("Allocating {} byte chunked buffer ({} byte chunks) for topic {} partition {} with remaining timeout {}ms",
                            size, chunkedFree.poolableSize(), topic, effectivePartition, maxTimeToBlock);
                    List<ByteBuffer> initialChunks = chunkedFree.allocateChunks(size, maxTimeToBlock);
                    nowMs = time.milliseconds();
                    bufferStream = new ChunkedByteBufferOutputStream(initialChunks, chunkedFree.poolableSize(), chunkedFree);
                }

                synchronized (dq) {
                    if (partitionChanged(topic, topicInfo, partitionInfo, dq, nowMs, cluster)) {
                        // The partition switched while we allocated extension chunks off-lock. They
                        // were sized against the previous partition's open batch, so they must not be
                        // attached to a different partition's open batch — refund them and let the next
                        // iteration re-check the new partition from scratch.
                        if (extensionChunks != null) {
                            for (ByteBuffer chunk : extensionChunks)
                                chunkedFree.deallocate(chunk);
                            extensionChunks = null;
                        }
                        continue;
                    }

                    if (extensionChunks != null) {
                        ProducerBatch last = dq.peekLast();
                        // The off-lock allocateChunks window allows the open batch we checked to be
                        // drained and replaced — possibly by a split batch (a plain
                        // ProducerBatch), which can't take extension chunks. Only attach to a
                        // writable chunked batch; otherwise refund the chunks and re-evaluate.
                        if (last instanceof ChunkedProducerBatch && last.isWritable()) {
                            ((ChunkedProducerBatch) last).addBuffers(extensionChunks);
                            extensionChunks = null;
                            RecordAppendResult retryResult = tryAppend(timestamp, key, value, headers, callbacks, dq, nowMs);
                            if (retryResult != null && !retryResult.needsBufferExtension) {
                                boolean enableSwitch = allBatchesFull(dq);
                                topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, retryResult.appendedBytes, cluster, enableSwitch);
                                return retryResult;
                            }
                            // needsBufferExtension: a concurrent appender consumed capacity our
                            // extension was sized against — loop to re-check. null: batch became
                            // full — loop into new-batch creation. Terminates because writeLimit is
                            // fixed: once full, the check stops requesting extension.
                            continue;
                        }
                        // The open batch is gone, closed, or non-chunked (e.g., a split batch). Return chunks to pool.
                        for (ByteBuffer chunk : extensionChunks)
                            chunkedFree.deallocate(chunk);
                        extensionChunks = null;
                        continue;
                    }

                    // First-record path: extensionChunks == null here implies extensionBytes == 0,
                    // so bufferStream was allocated (this iteration or carried from a prior one).
                    assert bufferStream != null;
                    int firstRecordSize = AbstractRecords.estimateSizeInBytesUpperBound(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, headers);
                    final ChunkedByteBufferOutputStream batchStream = bufferStream;
                    RecordAppendResult appendResult = appendNewBatch(topic, effectivePartition, dq, timestamp, key, value, headers, callbacks,
                            () -> chunkedRecordsBuilder(batchStream, firstRecordSize), nowMs);
                    if (appendResult.needsBufferExtension) {
                        // A concurrent appender created an open batch we should extend rather
                        // than start a new one (detected by appendNewBatch's in-lock tryAppend).
                        // Our bufferStream was sized for a fresh batch — release it and loop so
                        // the extension path allocates exactly the gap-sized chunks.
                        bufferStream.deallocate();
                        bufferStream = null;
                        continue;
                    }
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
     * Try to append to a ProducerBatch, with mid-batch chunk extension support.
     * <p>
     * If the open batch is within its batch-size limit but its chunked stream lacks chunk
     * capacity, returns {@link RecordAppendResult#needsExtension(int)} without
     * attempting the append; the caller allocates chunks outside the deque lock, attaches
     * them, and retries. Otherwise defers to the parent.
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
     * Build a {@link MemoryRecordsBuilder} backed by the chunked stream. Its {@code writeLimit}
     * bounds when the batch is full (the same batch-size limit as the full path), while chunk
     * capacity grows on demand via {@link ChunkedByteBufferOutputStream#addBuffers(List)}.
     */
    private MemoryRecordsBuilder chunkedRecordsBuilder(ChunkedByteBufferOutputStream bufferStream,
                                                       int firstRecordSize) {
        int writeLimit = Math.max(batchSize, firstRecordSize);
        return new MemoryRecordsBuilder(bufferStream, RecordBatch.CURRENT_MAGIC_VALUE, compression,
                TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, writeLimit);
    }
}
