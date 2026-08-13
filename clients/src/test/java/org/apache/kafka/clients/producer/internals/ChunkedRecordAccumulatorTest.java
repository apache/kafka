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

import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChunkedRecordAccumulatorTest {

    private final String topic = "test";
    private final int partition1 = 0;
    private final Node node1 = new Node(0, "localhost", 1111);
    private final TopicPartition tp1 = new TopicPartition(topic, partition1);

    private final PartitionMetadata partMetadata1 =
            new PartitionMetadata(Errors.NONE, tp1, Optional.of(node1.id()), Optional.empty(), null, null, null);
    private final List<PartitionMetadata> partMetadatas = new ArrayList<>(List.of(partMetadata1));
    private final Map<Integer, Node> nodes =
            Stream.of(node1).collect(Collectors.toMap(Node::id, java.util.function.Function.identity()));
    private final MetadataSnapshot metadataCache = new MetadataSnapshot(null, nodes, partMetadatas,
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());
    private final Cluster cluster = metadataCache.cluster();

    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    private final LogContext logContext = new LogContext();
    private final long maxBlockTimeMs = 1000;
    private final byte[] key = "k".getBytes();

    @AfterEach
    public void teardown() {
        metrics.close();
    }

    private ChunkedRecordAccumulator newAccumulator(int batchSize, int chunkSize, long totalMemory, Compression compression) {
        BufferPool pool = new BufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL);
        return newAccumulator(batchSize, compression, pool);
    }

    private ChunkedRecordAccumulator newAccumulator(int batchSize, Compression compression, BufferPool pool) {
        return new ChunkedRecordAccumulator(logContext, batchSize, compression,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool);
    }

    @Test
    public void testFirstRecordCreatesChunkedBatch() throws Exception {
        int chunkSize = 256;
        ChunkedRecordAccumulator accum = newAccumulator(1024, chunkSize, 16L * chunkSize, Compression.NONE);

        byte[] value = new byte[64];
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);

        Deque<ProducerBatch> dq = batchesFor(accum, tp1);
        assertEquals(1, dq.size());
        ProducerBatch batch = dq.peekFirst();
        assertNotNull(batch);
        assertEquals(1, batch.recordCount);
        accum.close();
    }

    @Test
    public void testSmallFollowupRecordFitsWithoutExtension() throws Exception {
        int chunkSize = 256;
        ChunkedRecordAccumulator accum = newAccumulator(1024, chunkSize, 16L * chunkSize, Compression.NONE);

        accum.append(topic, partition1, 0L, key, new byte[16], Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);
        accum.append(topic, partition1, 0L, key, new byte[16], Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);

        Deque<ProducerBatch> dq = batchesFor(accum, tp1);
        assertEquals(1, dq.size(), "Both records should land in the same batch");
        assertNotNull(dq.peekFirst());
        assertEquals(2, dq.peekFirst().recordCount);
        accum.close();
    }

    /**
     * A follow-up record that would overflow the existing chunks triggers mid-batch extension:
     * additional chunks are attached and the record lands in the same batch.
     */
    @Test
    public void testMidBatchExtensionGrowsExistingBatch() throws Exception {
        int chunkSize = 128;
        ChunkedRecordAccumulator accum = newAccumulator(8192, chunkSize, 16L * chunkSize, Compression.NONE);

        // First record fits in 1 chunk (~64+overhead).
        accum.append(topic, partition1, 0L, key, new byte[32], Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);

        Deque<ProducerBatch> dq = batchesFor(accum, tp1);
        assertEquals(1, dq.size());
        assertNotNull(dq.peekFirst());
        int initialRecordCount = dq.peekFirst().recordCount;
        assertEquals(1, initialRecordCount);

        // Second record big enough to require an extra chunk.
        accum.append(topic, partition1, 0L, key, new byte[200], Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);

        // Same batch, now with the second record.
        assertEquals(1, dq.size(), "Should still be the same batch — extension should not roll");
        assertNotNull(dq.peekFirst());
        assertEquals(2, dq.peekFirst().recordCount,
                "Second record should land in the extended batch");
        accum.close();
    }

    /**
     * Pool that adds one append right after a chunk allocation returns, mocking a concurrent
     * appender racing the same batch.
     */
    private BufferPool poolMockingConcurrentChunkAllocation(int chunkSize, long totalMemory,
                                                            AtomicReference<ChunkedRecordAccumulator> injectAppendOnce,
                                                            byte[] injectedValue) {
        return new BufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL) {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                List<ByteBuffer> chunks = super.allocateChunks(totalSize, maxTimeToBlockMs);
                ChunkedRecordAccumulator toInject = injectAppendOnce.getAndSet(null);
                if (toInject != null)
                    toInject.append(topic, partition1, 0L, key, injectedValue, Record.EMPTY_HEADERS, null,
                            maxBlockTimeMs, time.milliseconds(), cluster);
                return chunks;
            }
        };
    }

    /**
     * Two concurrent appenders race to extend the same open batch, each sizing its extension
     * against the same remaining capacity off-lock. Whichever attaches and appends first consumes
     * that capacity, leaving the other appender's extension too small for its record. Verifies
     * that the post-attach {@code tryAppend} — which attempts the write based on the batch's
     * actual capacity — makes that appender extend again and land its record in the same batch,
     * rather than write past the stream's chunk capacity (which would surface here as an
     * {@link IllegalStateException} thrown by the append).
     */
    @Test
    public void testConcurrentExtensionRaceLoserExtendsAgain() throws Exception {
        final int chunkSize = 256;
        final byte[] value = new byte[350];  // needs 2 chunks
        final AtomicReference<ChunkedRecordAccumulator> injectAppendOnce = new AtomicReference<>();

        BufferPool pool = poolMockingConcurrentChunkAllocation(chunkSize, 64L * chunkSize, injectAppendOnce, value);
        ChunkedRecordAccumulator accum = newAccumulator(8192, Compression.NONE, pool);
        try {
            // Tiny first record opens the batch, both racing records extend.
            accum.append(topic, partition1, 0L, key, new byte[1], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            // This append sizes its gap, then the injected append wins the race while the gap
            // chunks are held off-lock, shrinking the remaining capacity the gap was sized
            // against. Even with the over-allocation, this append shouldn't write past the stream's
            // chunk capacity.
            injectAppendOnce.set(accum);
            accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            Deque<ProducerBatch> dq = batchesFor(accum, tp1);
            assertEquals(1, dq.size(), "Expecting a single batch");
            assertNotNull(dq.peekFirst());
            assertEquals(3, dq.peekFirst().recordCount, "Expecting 3 records in the batch: the initial one plus the 2 racing ones.");
        } finally {
            accum.close();
        }
    }

    /**
     * Concurrent extensions can over-reserve: each appender sizes its own gap against the same
     * remaining capacity, so a batch can end up with more chunk capacity than its batch-size limit
     * admits. The limit must still be enforced on append: a record that no longer fits after the
     * race winner's append rolls to a new batch even though the over-reserved chunks could
     * physically hold it, and the unused surplus returns to the pool as soon as the batch is closed
     * for appends during that roll (before completion).
     */
    @Test
    public void testConcurrentExtensionRaceLoserStartsNewBatch() throws Exception {
        final int chunkSize = 256;
        final int batchSize = 1024;
        // Sized so each record fits the batch-size limit individually, but not both together.
        final byte[] value = new byte[500];
        final AtomicReference<ChunkedRecordAccumulator> injectAppendOnce = new AtomicReference<>();
        final AtomicInteger chunkDeallocations = new AtomicInteger();

        BufferPool pool = new BufferPool(64L * chunkSize, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL) {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                List<ByteBuffer> chunks = super.allocateChunks(totalSize, maxTimeToBlockMs);
                ChunkedRecordAccumulator toInject = injectAppendOnce.getAndSet(null);
                if (toInject != null)
                    toInject.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null,
                            maxBlockTimeMs, time.milliseconds(), cluster);
                return chunks;
            }

            @Override
            public void deallocate(ByteBuffer buffer) {
                chunkDeallocations.incrementAndGet();
                super.deallocate(buffer);
            }
        };
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, batchSize, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool);
        try {
            // Open the batch with a tiny record; both racing records will extend it.
            accum.append(topic, partition1, 0L, key, new byte[1], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);
            int deallocsAfterOpen = chunkDeallocations.get();

            // This append sizes its gap, then the injected append wins the race while the gap
            // chunks are held off-lock. The retry finds the batch over its batch-size limit, so
            // the record must roll to a new batch despite the attached (over-reserved) capacity.
            injectAppendOnce.set(accum);
            accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            Deque<ProducerBatch> dq = batchesFor(accum, tp1);
            assertEquals(2, dq.size(), "The losing record must roll to a new batch, not exceed batch.size");
            assertNotNull(dq.peekFirst());
            assertEquals(2, dq.peekFirst().recordCount, "First batch should have the initial record plus the race winner");
            assertNotNull(dq.peekLast());
            assertEquals(1, dq.peekLast().recordCount, "Second batch should have the loser record");

            // The loser's extension chunks were attached to the first batch but never used (the
            // over-reservation). They are freed as soon as the first batch is closed for appends
            // (during the roll to the new batch) — before completion, not held until deallocate.
            assertTrue(chunkDeallocations.get() > deallocsAfterOpen,
                    "the over-reserved unused chunks are freed when the first batch is closed for appends");
        } finally {
            accum.close();
        }
    }

    /**
     * When the sticky partition switches while extension chunks are held off-lock, those
     * chunks (sized against the previous partition's open batch) must be refunded to the pool on the
     * retry (not carried over and attached to a different partition's open batch)
     */
    @Test
    public void testPartitionSwitchRefundsHeldExtensionChunks() throws Exception {
        int chunkSize = 128;
        long totalMemory = 32L * chunkSize;

        AtomicBoolean extensionAllocated = new AtomicBoolean(false);
        AtomicInteger chunkDeallocations = new AtomicInteger(0);

        BufferPool pool = new BufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL) {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                List<ByteBuffer> chunks = super.allocateChunks(totalSize, maxTimeToBlockMs);
                // The mid-batch extension path is the only non-blocking caller.
                if (maxTimeToBlockMs == 0L)
                    extensionAllocated.set(true);
                return chunks;
            }

            @Override
            public void deallocate(ByteBuffer buffer) {
                chunkDeallocations.incrementAndGet();
                super.deallocate(buffer);
            }
        };

        AtomicBoolean switchFired = new AtomicBoolean(false);
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, 8192, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool) {
            @Override
            protected boolean partitionChanged(String topic, TopicInfo topicInfo,
                                               BuiltInPartitioner.StickyPartitionInfo partitionInfo,
                                               Deque<ProducerBatch> deque, long nowMs, Cluster cluster) {
                // Inject one spurious switch, but only once an extension allocation has happened —
                // i.e. on the second-sync-block check, while extension chunks are held.
                if (extensionAllocated.get() && switchFired.compareAndSet(false, true))
                    return true;
                return super.partitionChanged(topic, topicInfo, partitionInfo, deque, nowMs, cluster);
            }
        };
        try {
            // Warmup: tiny first record establishes an open batch (first-record path, blocking
            // allocate — does not flag extensionAllocated).
            accum.append(topic, partition1, 0L, key, new byte[1], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);
            int deallocBeforeExtend = chunkDeallocations.get();

            // Second record overflows the chunk → extension allocated off-lock → injected switch
            // fires in the second sync block with those chunks held.
            accum.append(topic, partition1, 0L, key, new byte[200], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            assertTrue(switchFired.get(), "the injected partition switch should have fired");
            assertTrue(chunkDeallocations.get() > deallocBeforeExtend,
                    "extension chunks held at the partition switch must be refunded to the pool; "
                            + "without the fix they are carried and attached instead");

            // The record still appends correctly after the switch-retry.
            Deque<ProducerBatch> dq = batchesFor(accum, tp1);
            assertEquals(1, dq.size());
            assertNotNull(dq.peekFirst());
            assertEquals(2, dq.peekFirst().recordCount,
                    "second record should still land in the batch after the switch-retry");
        } finally {
            accum.close();
        }
    }

    @Test
    public void testInflightExpirationReturnsAllChunksToPool() throws Exception {
        int chunkSize = 128;
        long totalMemory = 32L * chunkSize;
        BufferPool pool = new BufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL);
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, 8192, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool);

        // A record large enough that allocateChunks reserves multiple chunks (K > 1).
        byte[] value = new byte[400];
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);

        Deque<ProducerBatch> dq = batchesFor(accum, tp1);
        assertEquals(1, dq.size());
        ProducerBatch batch = dq.peekFirst();

        // Confirm the batch actually consumed multiple chunks. estimateSizeInBytesUpperBound for
        // a 400-byte value with v2 framing is well over chunkSize, so K should be >= 2.
        long heldBeforeDeallocate = totalMemory - pool.availableMemory();
        assertTrue(heldBeforeDeallocate >= 2L * chunkSize,
                "test setup expects K >= 2; pool held " + heldBeforeDeallocate + " bytes");

        // Mark the batch as if the Sender had drained it.
        batch.setInflight(true);

        // The inflight branch throws after deallocating. The chunked override must return
        // all K chunks (not just initialCapacity) before propagating the exception.
        assertThrows(IllegalStateException.class, () -> accum.deallocate(batch));

        assertEquals(totalMemory, pool.availableMemory(),
                "pool should be fully restored after inflight-expiration deallocate; "
                        + "any K-1 unsurrendered chunks indicate the chunked-leak regression");

        accum.close();
    }

    private Deque<ProducerBatch> batchesFor(RecordAccumulator accum, TopicPartition tp) {
        return accum.getDeque(tp);
    }

    private boolean hasOpenBatch(RecordAccumulator accum) {
        Deque<ProducerBatch> dq = batchesFor(accum, tp1);
        synchronized (dq) {
            return !dq.isEmpty();
        }
    }

    /**
     * When the pool is exhausted during a mid-batch extension, the append must not busy-loop
     * retrying the non-blocking acquire: after a single failed extension acquire
     * (maxTimeToBlockMs = 0), the open batch is closed and the very next pool call is the
     * blocking new-batch acquire (maxTimeToBlockMs > 0), where the record lands in a new batch.
     */
    @Test
    public void testExhaustedExtensionFallsBackToBlockingNewBatchPath() throws Exception {
        int chunkSize = 256;
        List<Long> allocTimeouts = new ArrayList<>();
        AtomicInteger closeForAppendsCalls = new AtomicInteger();
        List<Integer> closeCallsAtAlloc = new ArrayList<>();

        BufferPool pool = new BufferPool(16L * chunkSize, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL) {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                allocTimeouts.add(maxTimeToBlockMs);
                closeCallsAtAlloc.add(closeForAppendsCalls.get());
                // Simulate an exhausted pool for the non-blocking extension acquire only.
                if (maxTimeToBlockMs == 0L)
                    throw new BufferExhaustedException("injected: pool exhausted");
                return super.allocateChunks(totalSize, maxTimeToBlockMs);
            }
        };
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, 8192, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool) {
            @Override
            protected ProducerBatch createProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long nowMs) {
                // Count closeForRecordAppends calls on the batches this accumulator creates.
                return new ChunkedProducerBatch(tp, recordsBuilder, nowMs) {
                    @Override
                    public void closeForRecordAppends() {
                        closeForAppendsCalls.incrementAndGet();
                        super.closeForRecordAppends();
                    }
                };
            }
        };
        try {
            // First record establishes an open batch (blocking first-record acquire).
            accum.append(topic, partition1, 0L, key, new byte[100], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            // Second record overflows the batch's chunk so it needs extension; the injected
            // exhaustion fails the non-blocking acquire, the batch is closed, and the record
            // must go straight to the blocking new-batch path.
            RecordAccumulator.RecordAppendResult result = accum.append(topic, partition1, 0L, key,
                    new byte[100], Record.EMPTY_HEADERS, null, maxBlockTimeMs, time.milliseconds(), cluster);

            // Validate the expected call sequence: blocking (first batch), non-blocking (failed extension), blocking (new batch).
            assertEquals(List.of(maxBlockTimeMs, 0L, maxBlockTimeMs), allocTimeouts,
                    "expected a single failed extension acquire followed directly by the blocking new-batch acquire");
            assertEquals(0, closeCallsAtAlloc.get(0), "no close before the first-record acquire");
            assertEquals(0, closeCallsAtAlloc.get(1), "no close before the extension acquire");
            assertTrue(closeCallsAtAlloc.get(2) >= 1,
                    "the failed extension must close the batch before the blocking new-batch acquire");
            assertTrue(result.newBatchCreated, "record must land in a new batch");

            Deque<ProducerBatch> dq = batchesFor(accum, tp1);
            assertEquals(2, dq.size(), "closed batch + new batch expected");
            // The original batch is far below its writeLimit, so isFull() can only be true via
            // the closed append stream — i.e., it was closed for appends on the failed extension.
            assertNotNull(dq.peekFirst());
            assertTrue(dq.peekFirst().isFull(), "original batch must be closed for appends");
            assertEquals(1, dq.peekFirst().recordCount);
            assertNotNull(dq.peekLast());
            assertEquals(1, dq.peekLast().recordCount);
        } finally {
            accum.close();
        }
    }

    /**
     * The extension acquire runs off the deque lock, so the open batch can be replaced while it is in
     * flight: the sender drains the batch the gap was sized against and a concurrent appender creates a
     * new one with the memory that drain just freed. On exhaustion the append must then leave that new
     * batch open.
     */
    @Test
    public void testExhaustedExtensionLeavesAReplacementBatchOpen() throws Exception {
        int chunkSize = 256;
        AtomicBoolean injected = new AtomicBoolean();
        AtomicInteger closeForAppendsCalls = new AtomicInteger();
        AtomicReference<ChunkedRecordAccumulator> accumRef = new AtomicReference<>();
        AtomicReference<ProducerBatch> drainedRef = new AtomicReference<>();

        BufferPool pool = new BufferPool(16L * chunkSize, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL) {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                // Only the first non-blocking (extension) acquire is intercepted; the deque lock is not
                // held here, which is exactly what lets the open batch change under the appender.
                if (maxTimeToBlockMs == 0L && injected.compareAndSet(false, true)) {
                    // From here on dq.peekLast() is no longer the batch the gap was sized against.
                    drainedRef.set(simulateConcurrentDrainAndReplace(accumRef.get()));
                    throw new BufferExhaustedException("injected: pool exhausted");
                }
                return super.allocateChunks(totalSize, maxTimeToBlockMs);
            }
        };
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, 8192, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool) {
            @Override
            protected ProducerBatch createProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long nowMs) {
                return new ChunkedProducerBatch(tp, recordsBuilder, nowMs) {
                    @Override
                    public void closeForRecordAppends() {
                        closeForAppendsCalls.incrementAndGet();
                        super.closeForRecordAppends();
                    }
                };
            }
        };
        accumRef.set(accum);
        try {
            // First record establishes the open batch the extension gap will be sized against.
            accum.append(topic, partition1, 0L, key, new byte[100], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            // Second record overflows that batch's chunk, so it needs an extension. The injected
            // exhaustion fires after the batch has been replaced by the nested append's batch.
            accum.append(topic, partition1, 0L, key, new byte[100], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            assertTrue(injected.get(), "the extension acquire must have been intercepted");
            assertNotNull(drainedRef.get(), "the sized batch must have been drained by the injection");
            assertEquals(0, closeForAppendsCalls.get(),
                    "the failed extension must not close a batch it did not size the gap against");

            Deque<ProducerBatch> dq = batchesFor(accum, tp1);
            assertEquals(1, dq.size(), "only the replacement batch is expected");
            ProducerBatch replacement = dq.peekLast();
            assertNotNull(replacement);
            // Far below its writeLimit, so isFull() can only be true via a closed append stream.
            assertFalse(replacement.isFull(), "the replacement batch must stay open for appends");
            assertEquals(2, replacement.recordCount,
                    "the retried record must land in the replacement batch, extending it");
        } finally {
            accum.close();
        }
    }

    /**
     * Simulates the concurrent activity that can move the deque while an extension acquire runs off
     * the deque lock: the sender drains the open batch, returning its chunks to the pool, and another
     * appender claims that memory for a fresh batch in its place.
     *
     * @return the batch that was drained
     */
    private ProducerBatch simulateConcurrentDrainAndReplace(ChunkedRecordAccumulator accum) throws InterruptedException {
        Deque<ProducerBatch> dq = batchesFor(accum, tp1);
        ProducerBatch drained;
        synchronized (dq) {
            drained = dq.pollFirst();
        }
        assertNotNull(drained, "there must be an open batch to drain");
        accum.deallocate(drained);
        accum.append(topic, partition1, 0L, key, new byte[100], Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);
        return drained;
    }

    /**
     * Pool that refuses the non-blocking extension acquire after replacing the batch that acquire was sized
     * against. The refusal therefore closes nothing, since the batch it would close is no longer the open one.
     * <p>
     * The replacement batch is pre-sized for its own single record, so {@code chunkSize} decides whether
     * it has room to spare for the retried one.
     *
     * @param refusals counts the refusals: gates the safety limit below, and is what callers assert on
     * @param sleepOnRefusalMs mock-clock time the refusal spends, standing in for time gone earlier in the
     *                         append (a prior blocking acquire, or the metadata wait). Pass 0 to leave the
     *                         append time on the clock for another pass.
     */
    private BufferPool poolRefusingExtensionAfterBatchReplaced(int chunkSize,
                                                               AtomicReference<ChunkedRecordAccumulator> accumRef,
                                                               AtomicInteger refusals,
                                                               long sleepOnRefusalMs) {
        // Used to prevent the test from retrying forever if the logic fails.
        final int retrySafetyLimit = 5;
        return new BufferPool(16L * chunkSize, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL) {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                // The extension acquire always passes a zero timeout, and a new-batch acquire does too once no
                // time is left — but only with an empty deque here, since these tests always create the first
                // batch with a blocking acquire.
                boolean isExtensionPath = maxTimeToBlockMs == 0L && hasOpenBatch(accumRef.get());
                if (isExtensionPath && refusals.get() < retrySafetyLimit) {
                    refusals.incrementAndGet();
                    simulateConcurrentDrainAndReplace(accumRef.get());
                    if (sleepOnRefusalMs > 0)
                        time.sleep(sleepOnRefusalMs);
                    throw new BufferExhaustedException("injected: pool exhausted");
                }
                return super.allocateChunks(totalSize, maxTimeToBlockMs);
            }
        };
    }

    /**
     * Accumulator whose {@code partitionChanged} reports that the sticky partition moved, so the append
     * retries its pass having asked the pool for nothing. It stands in for a concurrent appender crossing the
     * switch threshold; only the append under test is affected, since {@code partitionInfo} is null for the
     * appends that name their partition (as the concurrent ones the pool overrides inject all do).
     *
     * @param activeWhile when to report the move, so the retry lands on the pass the test needs — it is
     *                    re-evaluated on every call, and reports nothing until it first holds
     * @param retries    counts the retries: gates the cap, and is what callers assert on
     * @param maxRetries caps the retries, so a bound that regressed fails an assertion rather than spinning
     */
    private ChunkedRecordAccumulator accumulatorWithPartitionChange(BufferPool pool,
                                                                    BooleanSupplier activeWhile,
                                                                    AtomicInteger retries, int maxRetries) {
        return new ChunkedRecordAccumulator(logContext, 8192, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool) {
            @Override
            protected boolean partitionChanged(String topic, TopicInfo topicInfo,
                                               BuiltInPartitioner.StickyPartitionInfo partitionInfo,
                                               Deque<ProducerBatch> deque, long nowMs, Cluster cluster) {
                if (partitionInfo != null && activeWhile.getAsBoolean() && retries.get() < maxRetries) {
                    retries.incrementAndGet();
                    return true;
                }
                return super.partitionChanged(topic, topicInfo, partitionInfo, deque, nowMs, cluster);
            }
        };
    }

    /**
     * The extension acquire is refused with the batch it was sized against already replaced, so nothing is
     * closed, the append's {@code max.block.ms} is spent, and the replacement needs memory too. The retry
     * finds the deadline gone and gives up, reporting the exhausted pool that refused the pass before it.
     */
    @Test
    public void testExtensionRetriesBoundedByMaxBlockTimeWhenAcquireFails() throws Exception {
        AtomicInteger refusals = new AtomicInteger();
        AtomicReference<ChunkedRecordAccumulator> accumRef = new AtomicReference<>();
        BufferPool pool = poolRefusingExtensionAfterBatchReplaced(256, accumRef, refusals,
                /* sleepOnRefusalMs */ maxBlockTimeMs + 1);
        ChunkedRecordAccumulator accum = newAccumulator(8192, Compression.NONE, pool);
        accumRef.set(accum);
        try {
            KafkaMetric exhausted = metrics.metric(metrics.metricName("buffer-exhausted-total", "producer-metrics"));

            accum.append(topic, partition1, 0L, key, new byte[100], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            // Needs an extension it never gets, on a batch that keeps being replaced: the first pass is
            // refused and spends the budget, so the retry gives up. The pass before it was denied memory, so
            // the failure carries the type, the metric and the diagnosis of an exhausted pool.
            BufferExhaustedException e = assertThrows(BufferExhaustedException.class,
                    () -> accum.append(topic, partition1, 0L, key, new byte[100], Record.EMPTY_HEADERS, null,
                            maxBlockTimeMs, time.milliseconds(), cluster));
            // BufferPool's own exhaustion message also reports "Available memory", so match the wording only
            // the retry bound uses: the failure must come from it, not from the blocking new-batch acquire.
            assertTrue(e.getMessage().contains("Failed to allocate memory for a record"), e.getMessage());
            assertEquals(1.0, (double) exhausted.metricValue(),
                    "a record dropped because the pool had no memory must be counted as one");
            assertEquals(1, refusals.get(),
                    "the extension acquire must be refused exactly once: the first pass must run, and the retry "
                            + "after it must give up on the spent deadline rather than acquire again");

            // Giving up must leave the open batch untouched.
            Deque<ProducerBatch> dq = batchesFor(accum, tp1);
            assertEquals(1, dq.size(), "only the replacement batch is expected");
            ProducerBatch replacement = dq.peekLast();
            assertNotNull(replacement);
            // Far below its writeLimit, so isFull() can only be true via a closed append stream.
            assertFalse(replacement.isFull(), "the replacement batch must stay open for appends");
            assertEquals(1, replacement.recordCount, "the dropped record must not have landed anywhere");
        } finally {
            accum.close();
        }
    }

    /**
     * A successful extension acquire whose chunks fall short, because a concurrent appender took the capacity
     * first, so the append comes back for more with the deadline already gone. Every acquire this append made
     * was granted, so it gives up with a plain timeout and charges the pool no buffer-exhausted drop.
     */
    @Test
    public void testExtensionRetryPastDeadlineFailsAfterInsufficientAttach() throws Exception {
        int chunkSize = 256;
        byte[] value = new byte[350];  // needs 2 chunks, so the open batch always needs an extension for it
        AtomicBoolean injecting = new AtomicBoolean();
        AtomicInteger extensionAcquires = new AtomicInteger();
        AtomicReference<ChunkedRecordAccumulator> accumRef = new AtomicReference<>();

        BufferPool pool = new BufferPool(64L * chunkSize, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL) {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                List<ByteBuffer> chunks = super.allocateChunks(totalSize, maxTimeToBlockMs);
                if (maxTimeToBlockMs == 0L && injecting.compareAndSet(false, true)) {
                    try {
                        extensionAcquires.incrementAndGet();
                        // Takes the capacity this acquire was sized against, so the attach that follows is
                        // too small and the append has to come back for more.
                        accumRef.get().append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null,
                                maxBlockTimeMs, time.milliseconds(), cluster);
                        // Leave the append with no max.block.ms left, so its retry is refused.
                        time.sleep(maxBlockTimeMs + 1);
                    } finally {
                        injecting.set(false);
                    }
                }
                return chunks;
            }
        };
        ChunkedRecordAccumulator accum = newAccumulator(8192, Compression.NONE, pool);
        accumRef.set(accum);
        try {
            // Tiny first record opens the batch.
            accum.append(topic, partition1, 0L, key, new byte[1], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            KafkaMetric exhausted = metrics.metric(metrics.metricName("buffer-exhausted-total", "producer-metrics"));

            TimeoutException e = assertThrows(TimeoutException.class,
                    () -> accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null,
                            maxBlockTimeMs, time.milliseconds(), cluster));
            // BufferExhaustedException extends TimeoutException, so assertThrows above would accept it too.
            assertEquals(TimeoutException.class, e.getClass(), e.getMessage());
            assertTrue(e.getMessage().contains("kept retrying"), e.getMessage());
            assertEquals(0.0, (double) exhausted.metricValue(),
                    "every acquire this append made was granted, so no drop may be charged to the pool");

            assertEquals(1, extensionAcquires.get(),
                    "the retry must be refused at the top of the loop, before it can acquire again");
            Deque<ProducerBatch> dq = batchesFor(accum, tp1);
            assertEquals(1, dq.size(), "only the one open batch is expected");
            // The opening record and the injected concurrent append; the record under test never landed.
            assertEquals(2, dq.peekLast().recordCount, "the refused record must not have landed");
        } finally {
            accum.close();
        }
    }

    /**
     * A failed extension acquire spends part of the append's {@code max.block.ms} before closing the batch
     * and falling through to the blocking new-batch acquire. That acquire is given what is left of it.
     */
    @Test
    public void testBlockingAcquireGetsOnlyWhatIsLeftOfMaxBlockTimeAfterFailedExtension() throws Exception {
        int chunkSize = 256;
        long spentInExtensionMs = 600;
        AtomicBoolean injected = new AtomicBoolean();
        AtomicLong blockingAcquireTimeout = new AtomicLong(-1);

        BufferPool pool = new BufferPool(16L * chunkSize, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL) {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                // The extension is the only acquire that does not block.
                boolean isExtensionPath = maxTimeToBlockMs == 0L;
                if (isExtensionPath && injected.compareAndSet(false, true)) {
                    // The open batch is left in place, so this failure closes it and the retry falls
                    // through to the blocking new-batch acquire below.
                    time.sleep(spentInExtensionMs);
                    throw new BufferExhaustedException("injected: pool exhausted");
                }
                // The first blocking acquire after that failure is the new-batch one under test.
                if (injected.get() && !isExtensionPath)
                    blockingAcquireTimeout.compareAndSet(-1, maxTimeToBlockMs);
                return super.allocateChunks(totalSize, maxTimeToBlockMs);
            }
        };
        ChunkedRecordAccumulator accum = newAccumulator(8192, Compression.NONE, pool);
        try {
            accum.append(topic, partition1, 0L, key, new byte[100], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            // Needs an extension; the failed acquire burns part of max.block.ms before the batch is
            // closed and the record retries on the blocking path.
            accum.append(topic, partition1, 0L, key, new byte[100], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            assertEquals(maxBlockTimeMs - spentInExtensionMs, blockingAcquireTimeout.get(),
                    "the blocking acquire must only get the remaining max.block.ms");
        } finally {
            accum.close();
        }
    }

    /**
     * Appends 100-byte records until one needs an extension, which the pool refuses after replacing the batch
     * it was sized against and spending the whole of the append's budget. The deadline is enforced strictly on
     * that append: the retry gives up even though the roomier replacement batch would take the record with no
     * allocation at all, so the deadline bounds the loop as well as the waiting inside it. The pool refused
     * this append, so the failure carries the type, metric and diagnosis of an exhausted pool.
     * <p>
     * Parameterized over the two ways an append arrives at its retry with no time left:
     * <ul>
     * <li>{@code zeroMaxBlockTime=false}: a normal {@code max.block.ms}, spent by an acquire that
     *     succeeded.</li>
     * <li>{@code zeroMaxBlockTime=true}: {@code max.block.ms} of 0, which is legal and whose deadline is
     *     behind the append before it starts, so such a producer gets a single pass and cannot survive a
     *     concurrent change to the batch it was sized against.</li>
     * </ul>
     */
    @ParameterizedTest(name = "{displayName} zeroMaxBlockTime={0}")
    @ValueSource(booleans = {false, true})
    public void testRetryPastDeadlineIsRefusedEvenWhenTheRecordNeedsNoMemory(boolean zeroMaxBlockTime) throws Exception {
        long maxTimeToBlock = zeroMaxBlockTime ? 0L : maxBlockTimeMs;
        AtomicInteger refusals = new AtomicInteger();
        AtomicReference<ChunkedRecordAccumulator> accumRef = new AtomicReference<>();
        // A chunk many times a record's size, so the replacement batch has room to spare and the
        // retried record needs no memory at all.
        BufferPool pool = poolRefusingExtensionAfterBatchReplaced(1024, accumRef, refusals,
                /* sleepOnRefusalMs */ maxBlockTimeMs + 1);
        ChunkedRecordAccumulator accum = newAccumulator(8192, Compression.NONE, pool);
        accumRef.set(accum);
        try {
            KafkaMetric exhausted = metrics.metric(metrics.metricName("buffer-exhausted-total", "producer-metrics"));

            BufferExhaustedException e = null;
            for (int i = 0; i < 50 && refusals.get() == 0; i++) {
                try {
                    accum.append(topic, partition1, 0L, key, new byte[100], Record.EMPTY_HEADERS, null,
                            maxTimeToBlock, time.milliseconds(), cluster);
                } catch (BufferExhaustedException thrown) {
                    e = thrown;
                    break;
                }
            }
            assertEquals(1, refusals.get(), "the extension acquire was never reached");
            assertNotNull(e, "the append past its deadline must be refused, not recovered");
            assertTrue(e.getMessage().contains("Failed to allocate memory for a record"), e.getMessage());

            Deque<ProducerBatch> dq = batchesFor(accum, tp1);
            assertEquals(1, dq.size(), "only the replacement batch is expected");
            assertEquals(1, dq.peekLast().recordCount,
                    "the refused record must not have landed, even though the batch had room for it");
            assertEquals(1.0, (double) exhausted.metricValue(),
                    "the pool refused this append, so the drop is counted against it");
        } finally {
            accum.close();
        }
    }

    /**
     * A single dropped record is counted exactly once even when it first fails the extension attempt
     * (recovered) and then fails the new-batch acquire.
     * Uses a real (non-overridden) pool so the actual allocateChunks path runs on both acquires.
     */
    @Test
    public void testBufferExhaustedNotDoubleCountedAcrossExtensionAndNewBatch() throws Exception {
        int chunkSize = 256;
        // Pool holds exactly one chunk: the first record consumes it, leaving the pool empty so both
        // the second record's extension attempt and its new-batch acquire fail.
        ChunkedRecordAccumulator accum = newAccumulator(8192, chunkSize, chunkSize, Compression.NONE);
        try {
            KafkaMetric exhausted = metrics.metric(metrics.metricName("buffer-exhausted-total", "producer-metrics"));

            // First record fills the one available chunk (pool now empty), opening the batch.
            accum.append(topic, partition1, 0L, key, new byte[150], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);
            assertEquals(0.0, (double) exhausted.metricValue());

            // Second record overflows the batch's chunk. The extension attempt (always non-blocking)
            // fails first — pool empty, recovered by closing the batch — then the new-batch acquire
            // blocks up to max.block.ms and also fails since the pool stays empty. A small block time
            // keeps the test fast (nothing frees memory during the wait). Both failed acquires must
            // count as a single dropped record.
            long newBatchBlockMs = 50L;
            assertThrows(BufferExhaustedException.class, () -> accum.append(topic, partition1, 0L, key,
                    new byte[150], Record.EMPTY_HEADERS, null, newBatchBlockMs, time.milliseconds(), cluster));
            assertEquals(1.0, (double) exhausted.metricValue(),
                    "the dropped record must be counted once, not once per failed acquire");
        } finally {
            accum.close();
        }
    }

    /**
     * Test that chunks are returned to the pool only at batch completion (deallocate), never at close.
     */
    @Test
    public void testBatchCloseDoesNotDeallocateChunksPrematurely() throws Exception {
        int chunkSize = 256;
        long totalMemory = 32L * chunkSize;
        BufferPool pool = new BufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL);
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, 8192, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool);

        // A small record fits in a single chunk, so exactly one (data-bearing) chunk is reserved.
        byte[] value = new byte[64];
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);

        Deque<ProducerBatch> dq = batchesFor(accum, tp1);
        ProducerBatch batch = dq.peekFirst();
        assertNotNull(batch);
        assertEquals(totalMemory - chunkSize, pool.availableMemory(), "append must reserve exactly one chunk");

        // Matches the call sites in RecordAccumulator.drain and Sender.
        batch.close();
        // close() must not return the chunk to the pool: available memory is unchanged, and the
        // built record set is still readable because its bytes still live in the batch's chunk.
        assertEquals(totalMemory - chunkSize, pool.availableMemory(), "close() must not return chunks to the pool");
        MemoryRecords records = batch.records();
        assertTrue(records.sizeInBytes() > 0,
                "batch must produce a non-empty record set after close; chunks were deallocated prematurely");

        int count = 0;
        for (RecordBatch rb : records.batches()) {
            for (Record r : rb) {
                count++;
                assertNotNull(r.value());
            }
        }
        assertEquals(1, count, "expected exactly 1 record after close");

        // Completion (deallocate) is what returns the chunk to the pool.
        accum.deallocate(batch);
        assertEquals(totalMemory, pool.availableMemory(), "deallocate must return the chunk to the pool");

        accum.close();
    }

    /**
     * As small records accumulate in a batch, the attached chunks grow with the batch's cumulative
     * projected output (not per-record).
     */
    @Test
    public void testExtensionTracksCumulativeBatchSize() throws Exception {
        int chunkSize = 64;
        int batchSize = 512;
        ChunkedRecordAccumulator accum = newAccumulator(batchSize, chunkSize, 64L * chunkSize, Compression.NONE);

        byte[] smallValue = new byte[24];
        for (int i = 0; i < 6; i++) {
            accum.append(topic, partition1, 0L, key, smallValue, Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);
        }

        Deque<ProducerBatch> dq = batchesFor(accum, tp1);
        assertEquals(1, dq.size());
        ProducerBatch batch = dq.peekFirst();
        assertNotNull(batch);
        assertEquals(6, batch.recordCount);

        // Finalize the batch: close() writes the record-batch header and builds the record set.
        // Chunk capacity for every record is ensured at append time (the accumulator attaches
        // extension chunks before appending, and an append without capacity would throw), so the
        // chunks already hold the whole batch by the time it is built.
        batch.close();
        MemoryRecords records = batch.records();
        int actualSize = records.sizeInBytes();
        // Under NONE compression, estimatedBytesWritten is exact: physical bytes = header + sum
        // of per-record bytes. The chunks attached must cover that, so actualSize must be >
        // chunkSize for a multi-record batch with non-trivial content.
        assertTrue(actualSize > chunkSize,
                "batch should have grown beyond a single chunk; got " + actualSize);

        accum.close();
    }

    @Test
    public void testCumulativeAccountsForBatchHeaderOnce() throws Exception {
        int chunkSize = 256;
        int batchSize = 8192;
        long totalMemory = 64L * chunkSize;
        BufferPool pool = new BufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL);
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, batchSize, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool);

        long beforeAlloc = pool.availableMemory();
        // First record establishes the batch. Each subsequent small record contributes its
        // uncompressed bytes to the cumulative target; the batch header is NOT re-counted.
        byte[] smallValue = new byte[8];
        for (int i = 0; i < 4; i++) {
            accum.append(topic, partition1, 0L, key, smallValue, Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);
        }

        // Each per-record append adds ~10-30 bytes (key + value + record overhead, V2). Cumulative
        // total for 4 records is well below chunkSize=256, so only 1 chunk should ever be attached.
        // The per-record formula (header counted once per record) would have allocated more.
        long held = beforeAlloc - pool.availableMemory();
        assertEquals(chunkSize, held,
                "cumulative formula should hold exactly one chunk for a small-record batch; "
                        + "header double-counting (per-record formula) would inflate this");

        accum.close();
    }

    @Test
    public void testChunkedBatchRejectsNonChunkedStream() {
        MemoryRecordsBuilder plainBuilder = MemoryRecords.builder(ByteBuffer.allocate(256),
                RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME, 0L);
        assertThrows(IllegalArgumentException.class,
                () -> new ChunkedProducerBatch(tp1, plainBuilder, time.milliseconds()));
    }

    /**
     * A batch closed for appends while the extension chunks were acquired off-lock must not be
     * attached to: the chunks go back to the pool and the record goes to a new batch.
     */
    @Test
    public void testBatchClosedForAppendsDuringAllocationIsNotExtended() throws Exception {
        final int chunkSize = 256;
        final AtomicBoolean closeOnce = new AtomicBoolean(true);
        final AtomicReference<Deque<ProducerBatch>> dqRef = new AtomicReference<>();
        // Chunks handed to the extension path, and every chunk handed back to the pool. Tracked by
        // identity so the assertions can name the exact buffers rather than infer from a total.
        final List<ByteBuffer> extensionChunks = new ArrayList<>();
        final Set<ByteBuffer> returnedToPool = Collections.newSetFromMap(new IdentityHashMap<>());

        BufferPool pool = new BufferPool(64L * chunkSize, chunkSize, metrics, time, "producer-metrics",
                BufferPool.AllocationMode.INCREMENTAL) {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                List<ByteBuffer> chunks = super.allocateChunks(totalSize, maxTimeToBlockMs);
                // The extension acquire is the only non-blocking one (see allocateExtensionChunks).
                if (maxTimeToBlockMs == 0L)
                    extensionChunks.addAll(chunks);
                Deque<ProducerBatch> dq = dqRef.get();
                // Mock a concurrent appender that found the batch full: RecordAccumulator.tryAppend
                // calls closeForRecordAppends() whenever last.tryAppend returns null.
                if (dq != null && closeOnce.getAndSet(false)) {
                    synchronized (dq) {
                        ProducerBatch last = dq.peekLast();
                        if (last != null)
                            last.closeForRecordAppends();
                    }
                }
                return chunks;
            }

            @Override
            public void deallocate(ByteBuffer buffer, int size) {
                returnedToPool.add(buffer);
                super.deallocate(buffer, size);
            }
        };
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, 8192, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool);
        try {
            // First record opens the batch with a single chunk.
            accum.append(topic, partition1, 0L, key, new byte[32], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);
            Deque<ProducerBatch> dq = batchesFor(accum, tp1);
            assertEquals(1, dq.size());
            dqRef.set(dq);

            // Second record needs an extension; the batch is closed for appends mid-window.
            RecordAccumulator.RecordAppendResult result = accum.append(topic, partition1, 0L, key,
                    new byte[300], Record.EMPTY_HEADERS, null, maxBlockTimeMs, time.milliseconds(), cluster);

            assertTrue(result.newBatchCreated, "record must land in a new batch, not the closed one");
            assertEquals(2, dq.size(), "closed batch + new batch expected");
            assertNotNull(dq.peekFirst());
            assertTrue(dq.peekFirst().isFull(), "the original batch must still be closed for appends");
            assertEquals(1, dq.peekFirst().recordCount, "no record may have been added to the closed batch");
            assertNotNull(dq.peekLast());
            assertEquals(1, dq.peekLast().recordCount);

            // Every chunk the extension path acquired for the now-closed batch must have gone back to
            // the pool, rather than being attached to it or dropped.
            assertFalse(extensionChunks.isEmpty(), "the extension path should have acquired chunks");
            assertTrue(returnedToPool.containsAll(extensionChunks),
                    "every chunk acquired to extend the closed batch must be returned to the pool");
        } finally {
            accum.close();
        }
    }

    /**
     * When the sticky partition keeps changing between the peek and the check under the deque lock,
     * the append abandons every pass without acquiring anything, so only this bound stops it.
     * The override stands in for the concurrent appender that moves the partition.
     */
    @Test
    public void testPartitionChangeRetriesBoundedByMaxBlockTime() {
        int chunkSize = 256;
        int batchSize = 1024;
        AtomicInteger forcedSwitches = new AtomicInteger();
        BufferPool pool = new BufferPool(16L * chunkSize, chunkSize, metrics, time, "producer-metrics",
                BufferPool.AllocationMode.INCREMENTAL);
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, batchSize, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool) {
            @Override
            protected boolean partitionChanged(String topic, TopicInfo topicInfo,
                                               BuiltInPartitioner.StickyPartitionInfo partitionInfo,
                                               Deque<ProducerBatch> deque, long nowMs, Cluster cluster) {
                // Capped so a regressed bound fails an assertion rather than spinning forever.
                if (partitionInfo != null && forcedSwitches.get() < 5) {
                    forcedSwitches.incrementAndGet();
                    // A concurrent appender to the sticky partition crossing the switch threshold, so the
                    // check below always sees a partition that moved and the caller always retries.
                    topicInfo.builtInPartitioner.updatePartitionInfo(partitionInfo, batchSize, cluster, true);
                    // Leave no time, so the append gets its first pass and the retry after it gives up.
                    time.sleep(maxBlockTimeMs + 1);
                }
                return super.partitionChanged(topic, topicInfo, partitionInfo, deque, nowMs, cluster);
            }
        };
        try {
            TimeoutException e = assertThrows(TimeoutException.class,
                    () -> accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, key, new byte[100],
                            Record.EMPTY_HEADERS, null, maxBlockTimeMs, time.milliseconds(), cluster));
            // BufferExhaustedException extends TimeoutException, so assertThrows above would accept it too.
            assertEquals(TimeoutException.class, e.getClass(), e.getMessage());
            assertTrue(e.getMessage().contains("kept retrying"), e.getMessage());
            assertEquals(1, forcedSwitches.get(),
                    "the partition must be moved exactly once: the first pass must run, and the retry after it "
                            + "must give up on the spent deadline rather than re-read the partition");
        } finally {
            accum.close();
        }
    }

    /**
     * An append that needs no waiting still lands its record, on the first pass,
     * which always runs whatever the time left.
     */
    @Test
    public void testZeroMaxBlockTimeStillAppendsWhenNothingHasToBeWaitedFor() throws Exception {
        int chunkSize = 256;
        ChunkedRecordAccumulator accum = newAccumulator(8192, chunkSize, 16L * chunkSize, Compression.NONE);
        try {
            // First record creates the batch; the acquire is non-blocking but the memory is there.
            accum.append(topic, partition1, 0L, key, new byte[100], Record.EMPTY_HEADERS, null,
                    /* maxTimeToBlock */ 0L, time.milliseconds(), cluster);
            // Second record overflows the batch's chunk, so it needs an extension — also non-blocking, also
            // satisfiable right away.
            accum.append(topic, partition1, 0L, key, new byte[100], Record.EMPTY_HEADERS, null,
                    /* maxTimeToBlock */ 0L, time.milliseconds(), cluster);

            Deque<ProducerBatch> dq = batchesFor(accum, tp1);
            assertEquals(1, dq.size(), "both records belong in the one extended batch");
            assertEquals(2, dq.peekLast().recordCount, "no record may be dropped for lack of time alone");
        } finally {
            accum.close();
        }
    }

    /**
     * An append that gives up still holding the stream it allocated for a new batch refunds it to the pool.
     * The stream is allocated on one pass and carried unattached across a partition switch, so the pass that
     * finds nothing left to spend leaves it for the {@code finally} to return.
     */
    @Test
    public void testGivingUpRefundsAnUnattachedNewBatchStream() {
        int chunkSize = 256;
        long totalMemory = 16L * chunkSize;
        AtomicBoolean streamAllocated = new AtomicBoolean();
        AtomicInteger retries = new AtomicInteger();

        BufferPool pool = new BufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics", BufferPool.AllocationMode.INCREMENTAL) {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                List<ByteBuffer> chunks = super.allocateChunks(totalSize, maxTimeToBlockMs);
                // Use up the time on the acquire that succeeded, standing in for a blocking acquire that
                // waited out the whole of max.block.ms before getting its memory.
                if (streamAllocated.compareAndSet(false, true))
                    time.sleep(maxBlockTimeMs + 1);
                return chunks;
            }
        };
        // The cap allows two retries so a regressed bound spins no further than an assertion failure; a
        // correct bound takes the single retry asserted below, holding the stream across it.
        ChunkedRecordAccumulator accum = accumulatorWithPartitionChange(pool, streamAllocated::get, retries,
                /* maxRetries */ 2);
        try {
            TimeoutException e = assertThrows(TimeoutException.class,
                    () -> accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, key, new byte[100],
                            Record.EMPTY_HEADERS, null, maxBlockTimeMs, time.milliseconds(), cluster));
            // BufferExhaustedException extends TimeoutException, so assertThrows above would accept it too.
            assertEquals(TimeoutException.class, e.getClass(), e.getMessage());
            assertEquals(1, retries.get(),
                    "the stream must have been held across the retry that gave up");
            assertEquals(totalMemory, pool.availableMemory(),
                    "the chunks reserved for a batch that was never created must go back to the pool");
        } finally {
            accum.close();
        }
    }

    /**
     * A refusal describes only the pass it happened on. The extension acquire is refused, then the pass after
     * it retries because the sticky partition moved, asking the pool for nothing at all, and it is that pass
     * which runs out of time. The append gives up with a plain timeout and charges the pool no
     * buffer-exhausted drop.
     */
    @Test
    public void testPartitionChangeTimeoutAfterExtensionFail() throws Exception {
        AtomicInteger refusals = new AtomicInteger();
        AtomicInteger retries = new AtomicInteger();
        AtomicReference<ChunkedRecordAccumulator> accumRef = new AtomicReference<>();

        // The refusal leaves time on the clock, so the pass after it runs rather than giving up.
        BufferPool pool = poolRefusingExtensionAfterBatchReplaced(256, accumRef, refusals,
                /* sleepOnRefusalMs */ 0);
        // Exactly one retry, on the pass right after the refusal — and it is that pass which spends the rest of
        // max.block.ms, so the pass after it gives up having asked the pool for nothing.
        ChunkedRecordAccumulator accum = accumulatorWithPartitionChange(pool, () -> {
            if (refusals.get() == 0)
                return false;
            time.sleep(maxBlockTimeMs + 1);
            return true;
        }, retries, /* maxRetries */ 1);
        accumRef.set(accum);
        try {
            KafkaMetric exhausted = metrics.metric(metrics.metricName("buffer-exhausted-total", "producer-metrics"));
            accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, key, new byte[100], Record.EMPTY_HEADERS,
                    null, maxBlockTimeMs, time.milliseconds(), cluster);

            TimeoutException e = assertThrows(TimeoutException.class,
                    () -> accum.append(topic, RecordMetadata.UNKNOWN_PARTITION, 0L, key, new byte[100],
                            Record.EMPTY_HEADERS, null, maxBlockTimeMs, time.milliseconds(), cluster));

            assertEquals(1, refusals.get(), "the extension acquire must have been refused once");
            assertEquals(1, retries.get(), "the interleaving under test was never reached");
            // BufferExhaustedException extends TimeoutException, so assertThrows above would accept it too.
            assertEquals(TimeoutException.class, e.getClass(), e.getMessage());
            assertTrue(e.getMessage().contains("kept retrying"), e.getMessage());
            assertEquals(0.0, (double) exhausted.metricValue(),
                    "the pass that gave up never asked the pool, so no drop may be attributed to it");
        } finally {
            accum.close();
        }
    }
}
