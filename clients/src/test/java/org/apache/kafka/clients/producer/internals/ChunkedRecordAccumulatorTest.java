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
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        ChunkedBufferPool pool = new ChunkedBufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics");
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
    private ChunkedBufferPool poolMockingConcurrentChunkAllocation(int chunkSize, long totalMemory,
                                                                   AtomicReference<ChunkedRecordAccumulator> injectAppendOnce,
                                                                   byte[] injectedValue) {
        return new ChunkedBufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics") {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                List<ByteBuffer> chunks = super.allocateChunks(totalSize, maxTimeToBlockMs);
                ChunkedRecordAccumulator toInject = injectAppendOnce.getAndSet(null);
                if (toInject != null) {
                    toInject.append(topic, partition1, 0L, key, injectedValue, Record.EMPTY_HEADERS, null,
                            maxBlockTimeMs, time.milliseconds(), cluster);
                }
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

        ChunkedBufferPool pool = poolMockingConcurrentChunkAllocation(chunkSize, 64L * chunkSize, injectAppendOnce, value);
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, 8192, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool);
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
     * physically hold it, and the unused surplus returns to the pool when the batch closes.
     */
    @Test
    public void testConcurrentExtensionRaceLoserStartsNewBatch() throws Exception {
        final int chunkSize = 256;
        final int batchSize = 1024;
        // Sized so each record fits the batch-size limit individually, but not both together.
        final byte[] value = new byte[500];
        final AtomicReference<ChunkedRecordAccumulator> injectAppendOnce = new AtomicReference<>();

        ChunkedBufferPool pool = poolMockingConcurrentChunkAllocation(chunkSize, 64L * chunkSize, injectAppendOnce, value);
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, batchSize, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool);
        try {
            // Open the batch with a tiny record; both racing records will extend it.
            accum.append(topic, partition1, 0L, key, new byte[1], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            // This append sizes its gap, then the injected append wins the race while the gap
            // chunks are held off-lock. The retry finds the batch over its batch-size limit, so
            // the record must roll to a new batch despite the attached (over-reserved) capacity.
            injectAppendOnce.set(accum);
            accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            Deque<ProducerBatch> dq = batchesFor(accum, tp1);
            assertEquals(2, dq.size(), "The losing record must roll to a new batch, not exceed batch.size");
            ProducerBatch first = dq.peekFirst();
            ProducerBatch second = dq.peekLast();
            assertNotNull(first);
            assertNotNull(second);
            assertEquals(2, first.recordCount, "First batch should have the initial record plus the race winner");
            assertEquals(1, second.recordCount, "Second batch should have the loser record");

            // The loser's extension chunks were attached but never used (the over-reservation);
            // closing the first batch returns that unused surplus to the pool.
            long beforeClose = pool.availableMemory();
            first.close();
            assertTrue(pool.availableMemory() > beforeClose,
                    "The over-reserved unused chunks should return to the pool at close");
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

        ChunkedBufferPool pool = new ChunkedBufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics") {
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
        ChunkedBufferPool pool = new ChunkedBufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics");
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

        ChunkedBufferPool pool = new ChunkedBufferPool(16L * chunkSize, chunkSize, metrics, time, "producer-metrics") {
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
            assertNotNull(dq.peekFirst());
            assertNotNull(dq.peekLast());
            // The original batch is far below its writeLimit, so isFull() can only be true via
            // the closed append stream — i.e., it was closed for appends on the failed extension.
            assertTrue(dq.peekFirst().isFull(), "original batch must be closed for appends");
            assertNotNull(dq.peekFirst());
            assertEquals(1, dq.peekFirst().recordCount);
            assertNotNull(dq.peekLast());
            assertEquals(1, dq.peekLast().recordCount);
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
        ChunkedRecordAccumulator accum = newAccumulator(8192, chunkSize, 32L * chunkSize, Compression.NONE);

        byte[] value = new byte[64];
        accum.append(topic, partition1, 0L, key, value, Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);

        Deque<ProducerBatch> dq = batchesFor(accum, tp1);
        ProducerBatch batch = dq.peekFirst();
        assertNotNull(batch);

        // Matches the call sites in RecordAccumulator.drain and Sender.
        batch.close();
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

        // batch.close() flattens chunks and writes the header. If chunks under-allocated, this
        // throws (insufficient capacity in the underlying buffer).
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
        ChunkedBufferPool pool = new ChunkedBufferPool(totalMemory, chunkSize, metrics, time, "producer-metrics");
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
}
