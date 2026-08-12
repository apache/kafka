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
        if (bufferPool.allocationMode() != BufferPool.AllocationMode.INCREMENTAL)
            throw new IllegalArgumentException("bufferPool must serve "
                    + BufferPool.AllocationMode.INCREMENTAL + " allocation, but serves "
                    + bufferPool.allocationMode());
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

        // This append's share of max.block.ms, as an absolute deadline. Blocking allocations bound themselves against it.
        // Retries that never block are bounded by it through throwIfNoMoreRetriesAllowed: they are allowed while
        // time is left. All retries bounded consistently (retry failed extension, retry on partition change).
        long deadlineMs = appendDeadlineMs(nowMs, maxTimeToBlock);
        AppendAttemptState attemptState = AppendAttemptState.FIRST_ATTEMPT;
        // Whether the non-blocking extension was denied memory on the pass that just ended (only
        // memory exhaustion case a pass can survive because it's non-blocking, all others throw).
        // Cleared once the next pass has read it, so it can only ever describe the pass immediately before.
        boolean nonBlockingMemoryAllocationDenied = false;

        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            while (true) {
                attemptState = throwIfNoMoreRetriesAllowed(attemptState, deadlineMs, nonBlockingMemoryAllocationDenied, topic);
                nonBlockingMemoryAllocationDenied = false;
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
                // The batch the extension gap was sized against, non-null exactly when the result is
                // needsBufferExtension. The acquire below runs off the deque lock, so this is used
                // to check if that batch is still the open one once the acquire fails, so it can be closed.
                ProducerBatch batchToExtend = null;
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
                    if (appendResult.needsBufferExtension())
                        batchToExtend = dq.peekLast();
                }

                if (appendResult.needsBufferExtension()) {
                    extensionChunks = allocateExtensionChunks(appendResult.extensionBytesNeeded, batchToExtend, dq,
                            topic, effectivePartition);
                    if (extensionChunks == null) {
                        // Pool exhausted, so no chunks are held. allocateExtensionChunks has already
                        // decided whether to close the open batch; retry either way, bounded by
                        // throwIfNoMoreRetriesAllowed, which will report exhausted memory as the cause.
                        nonBlockingMemoryAllocationDenied = true;
                        continue;
                    }
                    nowMs = time.milliseconds();
                } else if (appendResult.needsNewBatch() && newBatch == null) {
                    // The open batch is done (e.g., full, closed) so start a new one. Size it for
                    // this first record with the same estimator the full strategy uses
                    // (RecordAccumulator.append), but reserve only enough for the record rather than
                    // a whole batch.size.
                    // TODO: review when compression is supported.
                    int newBatchSize = AbstractRecords.estimateSizeInBytesUpperBound(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, headers);
                    long remainingTimeToBlock = remainingTimeToBlockMs(deadlineMs);
                    log.trace("Allocating {} byte chunked buffer ({} byte chunks) for topic {} partition {} with remaining timeout {}ms",
                            newBatchSize, chunkedFree.poolableSize(), topic, effectivePartition, remainingTimeToBlock);
                    List<ByteBuffer> initialChunks;
                    try {
                        initialChunks = chunkedFree.allocateChunks(newBatchSize, remainingTimeToBlock);
                    } catch (BufferExhaustedException e) {
                        // The blocking new-batch acquire was not able to get memory within what is
                        // left of max.block.ms. Record it in the buffer-exhausted metrics.
                        chunkedFree.recordBufferExhausted();
                        throw e;
                    } finally {
                        nowMs = time.milliseconds();
                    }
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
                        // The batch may have changed while allocateChunks was off-lock: drained and
                        // replaced, closed for appends, filled to its limit, or already grown by a
                        // concurrent appender. extensionBytesNeeded is 0 whenever attaching would be
                        // wrong, so it serves as both tests at once: the batch still needs chunks for
                        // this record, and it is still open (attaching to a stream closed for appends
                        // would throw). The instanceof covers the one case it cannot: a replacement
                        // that is a plain ProducerBatch (a split batch), which takes no chunks at all.
                        if (last instanceof ChunkedProducerBatch
                                && ((ChunkedProducerBatch) last).extensionBytesNeeded(timestamp, key, value, headers) > 0) {
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
                        // The batch no longer needs these chunks, or cannot take them. Return them to the pool.
                        deallocateExtensionChunks(extensionChunks);
                        extensionChunks = null;
                        continue;
                    }

                    // needsNewBatch path: extensionChunks == null here implies needsNewBatch,
                    // so bufferStream was allocated (this iteration or carried from a prior one).
                    if (newBatch == null)
                        throw new IllegalStateException("needsNewBatch path reached without an allocated buffer stream");
                    // Reuse the new-batch size estimate as the write-limit basis.
                    // TODO: review when compression is supported.
                    final NewBatchBuffer pendingNewBatch = newBatch;
                    appendResult = appendNewBatch(topic, effectivePartition, dq, timestamp, key, value, headers, callbacks,
                            () -> chunkedRecordsBuilder(pendingNewBatch.stream, pendingNewBatch.firstAppendSize), nowMs);
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
     * Mid-batch extension: the open batch can still take this record so grow it in place. The
     * acquire is non-blocking and fails fast when the pool is exhausted, closing
     * {@code batchToExtend} for appends so the record retries on the new-batch path (blocks for memory)
     * <p>
     * The acquire runs off the deque lock, so the open batch may no longer be the one the gap was
     * sized against by the time this would close it: it could have been drained and replaced by a batch
     * a concurrent appender created. So close only if the open batch is still
     * {@code batchToExtend}; when it is not, nothing is closed and the caller's next iteration
     * re-evaluates against whatever is open then.
     *
     * @param batchToExtend the batch the gap was sized against; must not be null
     * @return the chunks, or null if the pool was exhausted
     */
    private List<ByteBuffer> allocateExtensionChunks(int extensionBytesNeeded, ProducerBatch batchToExtend,
                                                     Deque<ProducerBatch> dq, String topic, int partition)
            throws InterruptedException {
        try {
            return chunkedFree.allocateChunks(extensionBytesNeeded, 0L);
        } catch (BufferExhaustedException e) {
            synchronized (dq) {
                if (dq.peekLast() == batchToExtend) {
                    log.trace("Pool exhausted while extending batch for topic {} partition {}; closing existing batch",
                            topic, partition);
                    // No need to check whether it is still open: closeForRecordAppends is idempotent.
                    batchToExtend.closeForRecordAppends();
                } else {
                    log.trace("Pool exhausted while extending batch for topic {} partition {}; the batch it "
                            + "was sized against is no longer the open one, so closing nothing and retrying",
                            topic, partition);
                }
            }
            return null;
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
        final int firstAppendSize;

        NewBatchBuffer(ChunkedByteBufferOutputStream stream, int firstAppendSize) {
            this.stream = stream;
            this.firstAppendSize = firstAppendSize;
        }
    }
}
