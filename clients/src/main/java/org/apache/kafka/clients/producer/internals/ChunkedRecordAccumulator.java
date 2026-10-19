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

        // The first pass is always allowed; the deadline is enforced on every retry after it.
        boolean firstPass = true;

        // Whether the non-blocking extension was denied memory on the pass that just ended (only
        // memory exhaustion case a pass can survive because it's non-blocking, all others throw).
        // Cleared once the next pass has read it, so it can only ever describe the pass immediately before.
        boolean nonBlockingMemoryAllocationDenied = false;

        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            while (true) {
                throwIfNoMoreRetriesAllowed(firstPass, deadlineMs, nonBlockingMemoryAllocationDenied, topic);
                firstPass = false;
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
                TopicPartition tp = new TopicPartition(topic, effectivePartition);
                Deque<ProducerBatch> dq = topicInfo.batches.computeIfAbsent(tp, k -> new ArrayDeque<>());
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
                    if (extensionChunks.isEmpty()) {
                        extensionChunks = null;
                        // Pool exhausted or closed, or the extension can never fit; allocateExtensionChunks decided whether to close the
                        // open batch. Retry, bounded by the deadline, unless closed: the retry bound would report that as exhausted memory.
                        if (closed) throw new KafkaException("Producer closed while send in progress");
                        nonBlockingMemoryAllocationDenied = true;
                        continue;
                    }
                    nowMs = time.milliseconds();
                } else if (appendResult.needsNewBatch() && newBatch == null) {
                    // The open batch is done (e.g., full, closed) so start a new one, with chunks for this first
                    // record only (not a whole batch.size), sized by newBatchChunkBytes: by the compression ratio
                    // estimate when compressed, per KIP-1332. The record's uncompressed upper bound stays the
                    // builder's write-limit basis either way, as in the full strategy.
                    int firstRecordUpperBound = AbstractRecords.estimateSizeInBytesUpperBound(
                            RecordBatch.CURRENT_MAGIC_VALUE, compression.type(), key, value, headers);
                    int newBatchChunkBytes = newBatchChunkBytes(topic, firstRecordUpperBound);
                    long remainingTimeToBlock = remainingTimeToBlockMs(deadlineMs);
                    log.trace("Allocating {} byte chunked buffer ({} byte chunks, first record upper bound {} bytes) for topic {} partition {} with remaining timeout {}ms",
                            newBatchChunkBytes, chunkedFree.poolableSize(), firstRecordUpperBound, topic, effectivePartition, remainingTimeToBlock);
                    List<ByteBuffer> initialChunks;
                    try {
                        initialChunks = chunkedFree.allocateChunks(newBatchChunkBytes, remainingTimeToBlock);
                    } catch (BufferExhaustedException e) {
                        // The blocking new-batch acquire was not able to get memory within what is
                        // left of max.block.ms. Record it in the buffer-exhausted metrics.
                        chunkedFree.recordBufferExhausted();
                        throw e;
                    } finally {
                        nowMs = time.milliseconds();
                    }
                    newBatch = new NewBatchBuffer(new ChunkedByteBufferOutputStream(initialChunks,
                            chunkedFree.poolableSize(), chunkedFree, compression.type() != CompressionType.NONE),
                            firstRecordUpperBound);
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
                        // The batch may have changed while tryAllocateChunks was off-lock: drained and
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
                    // The first record's uncompressed upper bound is the write-limit basis; fullness checks
                    // in MemoryRecordsBuilder already fold in the estimated compression ratio.
                    final NewBatchBuffer pendingNewBatch = newBatch;
                    appendResult = appendNewBatch(tp, dq, timestamp, key, value, headers, callbacks,
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
     * Bytes of chunk capacity to allocate for a new batch's first record (see the new-batch path in
     * {@link #append}).
     * <p>
     * Uncompressed, this is the record's upper bound, which is exact, and the first-record capacity check
     * in {@link ChunkedProducerBatch#tryAppend} relies on it.
     * <p>
     * Compressed, per KIP-1332, it is the batch header plus the record's upper bound scaled by the topic's
     * compression ratio estimate and padded by the 1.05 estimation factor: the same formula
     * ({@link MemoryRecordsBuilder#estimatedCompressedSizeInBytes}) the batch's builder uses for its
     * fullness checks, and so {@link ChunkedProducerBatch#extensionBytesNeeded} for later records. Sizing
     * it to the uncompressed upper bound was a rejected alternative, because it over-reserves pool memory
     * for data that compresses. This is only an estimate, so a compressor overshoot is absorbed by the
     * growable stream growing mid-write. The estimate read here can differ from the one the batch reads in
     * its constructor moments later, which {@link ChunkedProducerBatch#tryAppend} accepts for the same reason.
     * <p>
     * The compressed size is capped at the pool's total memory. {@code KafkaProducer.ensureValidRecordSize}
     * bounds only the uncompressed upper bound by {@code buffer.memory}, so an estimate near or above 1.0
     * could otherwise size a record that passed that check past the whole pool, which
     * {@link BufferPool#allocateChunks} rejects. The stream is growable, so the cap is safe.
     *
     * @param firstRecordUpperBound the record's uncompressed upper bound, batch header included
     */
    private int newBatchChunkBytes(String topic, int firstRecordUpperBound) {
        if (compression.type() == CompressionType.NONE)
            return firstRecordUpperBound;
        int batchHeaderSize = AbstractRecords.recordBatchHeaderSizeInBytes(RecordBatch.CURRENT_MAGIC_VALUE, compression.type());
        int estimate = MemoryRecordsBuilder.estimatedCompressedSizeInBytes(batchHeaderSize,
                firstRecordUpperBound - batchHeaderSize, CompressionRatioEstimator.estimation(topic, compression.type()));
        return (int) Math.min(estimate, chunkedFree.totalMemory());
    }

    /**
     * Mid-batch extension: the open batch can still take this record so grow it in place. The
     * acquire ({@link BufferPool#tryAllocateChunks}) is non-blocking and fails fast when the pool is
     * exhausted, closing {@code batchToExtend} for appends so the record retries on the new-batch path
     * (blocks for memory).
     * <p>
     * {@code tryAllocateChunks} also refuses once the pool is closed, which happens only after the accumulator is
     * closed. The caller then throws {@link KafkaException} for the closed producer before the retry bound is
     * consulted, so the refusal is never reported as exhausted memory.
     * <p>
     * An extension that, rounded up to whole chunks, exceeds the pool's entire capacity can never be
     * satisfied, and {@code tryAllocateChunks} would throw {@link IllegalArgumentException} for it. The
     * open batch admits a record by its exact size ({@code MemoryRecordsBuilder.hasRoomFor}), while the
     * extension is sized from the record's upper bound and, for a compressed batch, the compression ratio
     * estimate times the 1.05 estimation factor. So with {@code batch.size} close to {@code buffer.memory}
     * the last admitted record can need more chunks than the pool holds. That is treated as a denied
     * acquire too: the batch is closed and the record retries as a new batch's first record, which
     * {@code KafkaProducer.ensureValidRecordSize} has already bounded by {@code buffer.memory}.
     * <p>
     * The acquire runs off the deque lock, so the open batch may no longer be the one the gap was
     * sized against by the time this would close it: it could have been drained and replaced by a batch
     * a concurrent appender created. So close only if the open batch is still
     * {@code batchToExtend}; when it is not, nothing is closed and the caller's next iteration
     * re-evaluates against whatever is open then.
     *
     * @param batchToExtend the batch the gap was sized against; must not be null
     * @return the chunks, or an empty list if the pool was exhausted or closed, or the extension can never fit in it
     */
    private List<ByteBuffer> allocateExtensionChunks(int extensionBytesNeeded, ProducerBatch batchToExtend,
                                                     Deque<ProducerBatch> dq, String topic, int partition) {
        // Codec-agnostic: even uncompressed, hasRoomFor uses the record's exact size while
        // extensionBytesNeeded uses DefaultRecord.recordSizeUpperBound, so with batch.size == buffer.memory
        // the last admitted record can round up one chunk past the pool. Same rounding as BufferPool.numChunksFor.
        int chunkSize = chunkedFree.poolableSize();
        long chunkAlignedSize = ((long) extensionBytesNeeded + chunkSize - 1L) / chunkSize * chunkSize;
        if (chunkAlignedSize > chunkedFree.totalMemory()) {
            closeBatchToExtendIfStillOpen(batchToExtend, dq, topic, partition,
                    "Extension of " + extensionBytesNeeded + " bytes (" + chunkAlignedSize
                            + " bytes in whole chunks) exceeds the pool's " + chunkedFree.totalMemory() + " bytes");
            return List.of();
        }
        List<ByteBuffer> chunks = chunkedFree.tryAllocateChunks(extensionBytesNeeded);
        if (!chunks.isEmpty())
            return chunks;
        closeBatchToExtendIfStillOpen(batchToExtend, dq, topic, partition, "Pool exhausted");
        return List.of();
    }

    /**
     * Close {@code batchToExtend} for appends if it is still the open batch, after its extension was denied.
     *
     * @param reason why the extension was denied, for the trace log
     */
    private void closeBatchToExtendIfStillOpen(ProducerBatch batchToExtend, Deque<ProducerBatch> dq,
                                               String topic, int partition, String reason) {
        synchronized (dq) {
            if (dq.peekLast() == batchToExtend) {
                log.trace("{} while extending batch for topic {} partition {}; closing existing batch",
                        reason, topic, partition);
                // No need to check whether it is still open: closeForRecordAppends is idempotent.
                batchToExtend.closeForRecordAppends();
            } else {
                log.trace("{} while extending batch for topic {} partition {}; the batch it "
                        + "was sized against is no longer the open one, so closing nothing and retrying",
                        reason, topic, partition);
            }
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
     * A buffer stream allocated to back a new batch, with chunks sized for the batch's first record (see
     * {@link #newBatchChunkBytes}: the record's upper bound when uncompressed, the compression ratio
     * estimate applied to it when compressed), paired with {@code firstAppendSize}, the record's
     * uncompressed upper bound. That upper bound, not the chunk size, is the batch's write-limit basis (see
     * {@link #chunkedRecordsBuilder}), as in the full strategy. The two are always set and cleared together
     * in {@link #append}, including across retries.
     * <p>
     * The stream is growable only for a compressed batch, whose chunk sizing is a ratio-based estimate the
     * compressor may overshoot. An uncompressed batch's sizing (the first record's upper bound, then
     * {@link ChunkedProducerBatch#extensionBytesNeeded}) is exact, so its stream is non-growable: running
     * past its chunks is a sizing bug and throws instead of silently allocating on the heap.
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
