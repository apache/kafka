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
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChunkedRecordAccumulatorTest {

    private final String topic = "test";
    private final int partition1 = 0;
    private final Node node1 = new Node(0, "localhost", 1111);
    private final TopicPartition tp1 = new TopicPartition(topic, partition1);

    private final PartitionMetadata partMetadata1 =
            new PartitionMetadata(Errors.NONE, tp1, Optional.of(node1.id()), Optional.empty(), null, null, null);
    private final List<PartitionMetadata> partMetadatas = new ArrayList<>(Arrays.asList(partMetadata1));
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

    /** First record creates a batch backed by chunked allocation. */
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

    /**
     * A small follow-up record fits in the existing batch's pre-allocated chunks — no extension
     * needed.
     */
    @Test
    public void testFollowupRecordFitsWithoutExtension() throws Exception {
        int chunkSize = 256;
        ChunkedRecordAccumulator accum = newAccumulator(1024, chunkSize, 16L * chunkSize, Compression.NONE);

        accum.append(topic, partition1, 0L, key, new byte[16], Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);
        accum.append(topic, partition1, 0L, key, new byte[16], Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);

        Deque<ProducerBatch> dq = batchesFor(accum, tp1);
        assertEquals(1, dq.size(), "Both records should land in the same batch");
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
        int initialRecordCount = dq.peekFirst().recordCount;
        assertEquals(1, initialRecordCount);

        // Second record big enough to require an extra chunk.
        accum.append(topic, partition1, 0L, key, new byte[200], Record.EMPTY_HEADERS, null,
                maxBlockTimeMs, time.milliseconds(), cluster);

        // Same batch, now with the second record.
        assertEquals(1, dq.size(), "Should still be the same batch — extension should not roll");
        assertEquals(2, dq.peekFirst().recordCount,
                "Second record should land in the extended batch");
        accum.close();
    }

    /**
     * Race between two concurrent appenders on the same chunked tail. Both threads probe
     * {@code extensionBytesNeeded} under the deque lock against the same physical remaining
     * capacity, drop the lock, and allocate gap-sized chunks off-lock. When the first appender
     * re-takes the lock and consumes most of the original remaining, the second appender's
     * allocation — sized against the pre-race remaining — is short by the bytes the first
     * appender consumed. Without a re-probe in the second-lock block, the second appender
     * proceeds to write past the stream's physical capacity and {@code advanceToNextChunk}
     * throws {@link IllegalStateException}.
     * <p>
     * Coordination: a {@link ChunkedBufferPool} subclass blocks the FIRST {@code allocateChunks}
     * call after the test arms a latch (i.e. thread A's). Thread B's allocate is the second call
     * and proceeds normally — B re-takes the lock, attaches its chunks, appends its record, and
     * returns. Only then is thread A released, so A attaches its chunks and tries to append
     * against a tail whose remaining has already shrunk.
     */
    @Test
    public void testConcurrentExtensionRaceDoesNotOverflowChunkedStream() throws Exception {
        final int chunkSize = 256;
        final int recordValueSize = 350;  // sized so gap ≈ chunkSize → minimal rounding cushion

        final AtomicBoolean armed = new AtomicBoolean(false);
        final CountDownLatch aHoldsChunks = new CountDownLatch(1);
        final CountDownLatch bDoneAttaching = new CountDownLatch(1);

        ChunkedBufferPool pool = new ChunkedBufferPool(64L * chunkSize, chunkSize, metrics, time, "producer-metrics") {
            @Override
            public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
                boolean blockThisCall = armed.compareAndSet(true, false);
                List<ByteBuffer> chunks = super.allocateChunks(totalSize, maxTimeToBlockMs);
                if (blockThisCall) {
                    aHoldsChunks.countDown();
                    if (!bDoneAttaching.await(10, TimeUnit.SECONDS)) {
                        throw new InterruptedException("test timeout waiting for B");
                    }
                }
                return chunks;
            }
        };
        ChunkedRecordAccumulator accum = new ChunkedRecordAccumulator(logContext, 8192, Compression.NONE,
                /* lingerMs */ 0, /* retryBackoffMs */ 0L, /* retryBackoffMaxMs */ 0L,
                /* deliveryTimeoutMs */ 3200, metrics, "producer-metrics", time,
                /* transactionManager */ null, pool);
        try {
            // Warmup: tiny first record establishes a chunked tail with a known remainingInStream.
            accum.append(topic, partition1, 0L, key, new byte[1], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            // Arm the race: the next allocateChunks call (A's) will block until B has appended.
            armed.set(true);

            AtomicReference<Throwable> aError = new AtomicReference<>();
            Thread tA = new Thread(() -> {
                try {
                    accum.append(topic, partition1, 0L, key, new byte[recordValueSize],
                            Record.EMPTY_HEADERS, null, maxBlockTimeMs, time.milliseconds(), cluster);
                } catch (Throwable t) {
                    aError.set(t);
                }
            }, "test-appender-A");
            tA.start();

            // Wait for A to be parked inside allocateChunks holding its (pre-attach) chunks.
            assertTrue(aHoldsChunks.await(10, TimeUnit.SECONDS), "A did not reach allocateChunks");

            // B runs on this thread. Its allocateChunks is the second call — armed=false now,
            // so it proceeds without blocking. B probes the same R as A (A hasn't attached yet),
            // attaches its own chunks, appends its record. Now the tail's remaining has shrunk
            // by B's actual record size, but A's still-off-lock allocation didn't know that.
            accum.append(topic, partition1, 0L, key, new byte[recordValueSize], Record.EMPTY_HEADERS, null,
                    maxBlockTimeMs, time.milliseconds(), cluster);

            // Release A. Without the fix in the second-lock block, A's tryAppendToExisting
            // will overflow the chunked stream and throw IllegalStateException.
            bDoneAttaching.countDown();
            tA.join(10_000);
            assertFalse(tA.isAlive(), "Thread A did not complete in time");

            assertNull(aError.get(),
                    "Thread A failed: " + (aError.get() == null ? "" : aError.get().toString()));
        } finally {
            // Drain any state regardless of outcome.
            accum.close();
        }
    }

    /**
     * Regression guard: an inflight chunked batch returns all K chunks to the pool on the
     * expiration / broker-disconnect path, not just one {@code initialCapacity} chunkSize.
     * For an inflight batch the parent {@code RecordAccumulator.deallocate} calls
     * {@code batch.deallocateInflightBuffer(pool)} and then throws; {@link ChunkedProducerBatch}
     * overrides that hook to return all chunks (instead of donating one fresh buffer). Removing
     * that override would re-introduce a (K−1)×chunkSize leak and fail this test.
     */
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
     * Sender's drain (and any other caller of {@code batch.close()}) cascades through
     * {@code MemoryRecordsBuilder.closeForRecordAppends()} →
     * {@code appendStream.close()} → {@code bufferStream.close()}. The chunk data must remain
     * readable through that cascade so the builder can flatten the chunks into the heap buffer it
     * sends over the network — chunks are returned to the pool only at batch completion
     * (deallocate), never at close. If they were released at close, the flattened buffer would be
     * empty and the header write would fail (or produce an empty record set).
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
     * Chunks attached grow with the batch's cumulative projected output, not per-record. With a
     * chunkSize smaller than the batch's eventual content, a sequence of small records should
     * extend the chunks as the running total (header + uncompressed bytes for NONE) crosses
     * chunk boundaries — not allocate the first record's worth and then never extend until
     * physical capacity is exhausted (which the per-record formula would do).
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

    /**
     * The cumulative sizing formula counts the batch header once. The previous per-record
     * formula effectively included the batch header in each record's upper bound, which
     * over-allocated as records accumulated. With the cumulative formula, chunk count for N
     * small records of total uncompressed size {@code U} is {@code ceil((header + U) / chunkSize)},
     * not {@code N × ceil((header + recordSize) / chunkSize)}.
     */
    @Test
    public void testCumulativeAccountsForHeaderOnce() throws Exception {
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
