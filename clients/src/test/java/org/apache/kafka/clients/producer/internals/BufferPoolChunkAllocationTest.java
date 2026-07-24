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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BufferPoolChunkAllocationTest {

    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(time);

    // Safety ceiling for block-until-available requests that the test unblocks itself (by
    // deallocating or closing). The waiter is always signaled first, so a passing test returns in
    // milliseconds; this only bounds how long a broken test can block before failing.
    private static final long MAX_BLOCK_TIME_MS = 2_000;

    @AfterEach
    public void teardown() {
        metrics.close();
    }

    private BufferPool pool(long totalMemory, int chunkSize) {
        String metricGroup = "producer-metrics";
        return new BufferPool(totalMemory, chunkSize, metrics, time, metricGroup, BufferPool.AllocationMode.INCREMENTAL);
    }

    /** Single-chunk request returns a list of one buffer at the chunk size. */
    @Test
    public void testAllocateOneChunk() throws Exception {
        int chunkSize = 64;
        BufferPool p = pool(1024, chunkSize);
        List<ByteBuffer> chunks = p.allocateChunks(chunkSize, 100);
        assertEquals(1, chunks.size());
        assertEquals(chunkSize, chunks.get(0).capacity());
    }

    /** Total size that's not a multiple of chunk size rounds up */
    @Test
    public void testAllocateRoundsUpToChunkBoundary() throws Exception {
        int chunkSize = 64;
        BufferPool p = pool(1024, chunkSize);
        // 65 bytes requested → 2 chunks (128 bytes total).
        List<ByteBuffer> chunks = p.allocateChunks(65, 100);
        assertEquals(2, chunks.size());
        for (ByteBuffer chunk : chunks)
            assertEquals(chunkSize, chunk.capacity());
    }

    /** Multi-chunk request returns ceil(totalSize / chunkSize) chunks. */
    @Test
    public void testAllocateMultipleChunks() throws Exception {
        int chunkSize = 64;
        BufferPool p = pool(1024, chunkSize);
        List<ByteBuffer> chunks = p.allocateChunks(4 * chunkSize, 100);
        assertEquals(4, chunks.size());
    }

    /** Pool memory accounting: after allocation, the unallocated portion shrinks by the request. */
    @Test
    public void testAvailableMemoryAfterAllocation() throws Exception {
        int chunkSize = 64;
        long total = 256;
        BufferPool p = pool(total, chunkSize);
        p.allocateChunks(3 * chunkSize, 100);
        assertEquals(total - 3L * chunkSize, p.availableMemory());
    }

    /** Returning chunks via deallocate restores pool memory. */
    @Test
    public void testDeallocationRestoresMemory() throws Exception {
        int chunkSize = 64;
        long total = 256;
        BufferPool p = pool(total, chunkSize);
        List<ByteBuffer> chunks = p.allocateChunks(2 * chunkSize, 100);
        for (ByteBuffer chunk : chunks)
            p.deallocate(chunk);
        assertEquals(total, p.availableMemory());
    }

    @Test
    public void testRejectsRequestExceedingTotalMemory() {
        BufferPool p = pool(128, 64);
        assertThrows(IllegalArgumentException.class, () -> p.allocateChunks(129, 100));
    }

    @Test
    public void testRejectsNonPositiveRequest() {
        BufferPool p = pool(128, 64);
        assertThrows(IllegalArgumentException.class, () -> p.allocateChunks(0, 100));
        assertThrows(IllegalArgumentException.class, () -> p.allocateChunks(-1, 100));
    }

    /**
     * When a multi-chunk request can't be fully satisfied within the deadline, chunks already
     * acquired during the call must be returned to the pool.
     */
    @Test
    public void testRollbackOnPartialFailure() throws Exception {
        int chunkSize = 64;
        long total = 2 * chunkSize;  // only 2 chunks worth of memory
        BufferPool p = pool(total, chunkSize);
        // Reserve one chunk so the pool has only 1 left.
        ByteBuffer held = p.allocateChunks(chunkSize, 100).get(0);

        // Request 2 chunks with a zero deadline — first chunk fits, second can't, must throw.
        assertThrows(BufferExhaustedException.class, () -> p.allocateChunks(2 * chunkSize, 0));

        // Available memory must reflect only the chunk we deliberately hold; the request's
        // first-chunk acquisition was rolled back.
        assertEquals(total - chunkSize, p.availableMemory());

        p.deallocate(held);
        assertEquals(total, p.availableMemory());
    }

    /**
     * No partial chunk holds during the wait. While a multi-chunk request blocks, the pool's
     * {@code availableMemory()} must reflect bytes the waiter has not yet "earned".
     */
    @Test
    public void testNoPartialHoldsDuringWait() throws Exception {
        int chunkSize = 64;
        // Hold 2 of 3 chunks so exactly 1 is free — a 2-chunk request can't be satisfied immediately.
        long total = 3L * chunkSize;
        BufferPool p = pool(total, chunkSize);
        ByteBuffer h1 = p.allocateChunks(chunkSize, 100).get(0);
        ByteBuffer h2 = p.allocateChunks(chunkSize, 100).get(0);
        long available = p.availableMemory();
        assertEquals(chunkSize, available, "pool should have exactly 1 chunk free");

        // Background thread requests 2 chunks; will block on the 2nd.
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                p.allocateChunks(2 * chunkSize, MAX_BLOCK_TIME_MS);
            } catch (Throwable th) {
                err.set(th);
            }
        }, "chunked-waiter");
        t.start();
        // Wait until the thread has joined the waiters queue.
        TestUtils.waitForCondition(() -> p.queued() == 1, "waiter should be parked on the pool's queue");

        // While the waiter is parked, the pool's available memory must still report the
        // 1 chunk's worth — the waiter has NOT consumed any of it (no partial hold).
        assertEquals(chunkSize, p.availableMemory(),
                "pool memory must reflect no partial holds during the atomic wait");

        // The pool still has 1 chunk free (asserted above) and the waiter never consumed it, so
        // freeing just one more chunk reaches the 2 it needs and unblocks it.
        p.deallocate(h1);
        t.join();
        assertNull(err.get(), "waiter unexpectedly threw: " + err.get());
        p.deallocate(h2);
    }

    /**
     * FIFO fairness across the K-chunk request. A multi-chunk request that joins the wait queue
     * before a single-chunk request must complete first when memory becomes available — the
     * K-chunk request occupies a single waiter slot, not K of them.
     */
    @Test
    public void testFifoFairnessAcrossMultiChunkAndSingleChunkRequests() throws Exception {
        int chunkSize = 64;
        // Exactly the chunks the two waiters need: 2 for the multi request, 1 for the single.
        long total = 3L * chunkSize;
        BufferPool p = pool(total, chunkSize);
        // Drain the pool entirely so any new request must wait.
        ByteBuffer h1 = p.allocateChunks(chunkSize, 100).get(0);
        ByteBuffer h2 = p.allocateChunks(chunkSize, 100).get(0);
        ByteBuffer h3 = p.allocateChunks(chunkSize, 100).get(0);
        assertEquals(0, p.availableMemory());

        // T_multi enters first, requesting 2 chunks; T_single enters second, requesting 1 chunk.
        AtomicReference<Long> multiCompletionMs = new AtomicReference<>();
        AtomicReference<Long> singleCompletionMs = new AtomicReference<>();
        AtomicReference<List<ByteBuffer>> multiChunks = new AtomicReference<>();
        AtomicReference<ByteBuffer> singleChunk = new AtomicReference<>();
        CountDownLatch multiStarted = new CountDownLatch(1);
        Thread tMulti = new Thread(() -> {
            try {
                multiStarted.countDown();
                List<ByteBuffer> got = p.allocateChunks(2 * chunkSize, MAX_BLOCK_TIME_MS);
                multiChunks.set(got);
                multiCompletionMs.set(System.nanoTime());
                for (ByteBuffer b : got) p.deallocate(b);
            } catch (Throwable th) {
                multiCompletionMs.set(-1L);
            }
        }, "multi");
        Thread tSingle = new Thread(() -> {
            try {
                ByteBuffer got = p.allocateChunks(chunkSize, MAX_BLOCK_TIME_MS).get(0);
                singleChunk.set(got);
                singleCompletionMs.set(System.nanoTime());
                p.deallocate(got);
            } catch (Throwable th) {
                singleCompletionMs.set(-1L);
            }
        }, "single");

        tMulti.start();
        assertTrue(multiStarted.await(2, TimeUnit.SECONDS));
        // Wait for tMulti to be parked before starting tSingle, so the FIFO order is deterministic.
        TestUtils.waitForCondition(() -> p.queued() == 1, "multi-chunk waiter should be the only one queued");
        tSingle.start();
        // Wait for tSingle to also be parked.
        TestUtils.waitForCondition(() -> p.queued() == 2, "single-chunk waiter joined after the multi-chunk one");

        // Now free chunks one at a time, allowing the multi-chunk request to accumulate.
        p.deallocate(h1);
        Thread.sleep(50);
        p.deallocate(h2);
        // Both freed — the FIFO leader (multi) should claim both before the single-chunk waiter
        // gets any. The multi completes first.
        tMulti.join();
        // Now free another chunk for the single-chunk waiter.
        p.deallocate(h3);
        tSingle.join();

        assertNotNull(multiCompletionMs.get(), "multi did not complete");
        assertNotNull(singleCompletionMs.get(), "single did not complete");
        assertTrue(multiCompletionMs.get() != -1L && singleCompletionMs.get() != -1L,
                "both threads must complete without exception");
        // Each waiter must actually have received its chunks.
        assertNotNull(multiChunks.get(), "multi did not receive its chunks");
        assertEquals(2, multiChunks.get().size(), "multi must receive exactly 2 chunks");
        assertNotNull(singleChunk.get(), "single did not receive its chunk");
        assertTrue(multiCompletionMs.get() <= singleCompletionMs.get(),
                "FIFO violated: single (joined later) completed at "
                        + singleCompletionMs.get() + " before multi at " + multiCompletionMs.get());
    }

    /**
     * A chunk request that can't be satisfied immediately must wait for memory, taking chunks as
     * they are freed. If it then times out before acquiring all it needs, each chunk it already
     * took must be returned to the pool exactly once. A double refund would make the pool
     * over-report {@code availableMemory()}, letting later allocations exceed the configured
     * {@code buffer.memory} limit.
     */
    @Test
    public void testWaitingRequestDoesNotDoubleRefundChunksOnTimeout() throws Exception {
        int chunkSize = 64;
        long total = 3L * chunkSize;
        BufferPool p = pool(total, chunkSize);

        // Drain so the next allocateChunks must wait.
        ByteBuffer h1 = p.allocateChunks(chunkSize, 100).get(0);
        ByteBuffer h2 = p.allocateChunks(chunkSize, 100).get(0);
        ByteBuffer h3 = p.allocateChunks(chunkSize, 100).get(0);
        assertEquals(0, p.availableMemory());

        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                // Asks for 3 chunks with a finite deadline. We free 2 chunks below, so the waiter
                // takes those but is still 1 short and times out waiting for the 3rd.
                p.allocateChunks(3 * chunkSize, 500);
            } catch (Throwable th) {
                err.set(th);
            }
        }, "partial-holder");
        t.start();

        TestUtils.waitForCondition(() -> p.queued() == 1, "waiter should be parked on the pool's queue");

        // Free 2 chunks — the waiter takes both, then waits again for the 3rd it never gets.
        p.deallocate(h1);
        p.deallocate(h2);

        // Give the waiter a moment to wake, take the 2 chunks, and re-enter the wait before it times out.
        Thread.sleep(100);

        t.join();
        assertInstanceOf(BufferExhaustedException.class, err.get(), "expected BufferExhaustedException, got " + err.get());

        // After the timeout, exactly the 2 freed chunks (h1 + h2) must be back in the pool, each
        // returned once; h3 is still held outside the pool. A double refund would over-report
        // availableMemory() (e.g. 4*chunkSize instead of 2).
        assertEquals(2L * chunkSize, p.availableMemory(),
                "pool over-reports availableMemory if polled chunks are refunded twice on rollback");

        p.deallocate(h3);
    }

    /**
     * A chunk request that must wait for memory and then completes by taking chunks as they are
     * freed must account for those chunks exactly once — as memory now held by the caller — so the
     * pool neither loses nor over-reports capacity.
     */
    @Test
    public void testWaitingRequestDoesNotCorruptAccountingOnSuccess() throws Exception {
        int chunkSize = 64;
        long total = 3L * chunkSize;
        BufferPool p = pool(total, chunkSize);

        // Drain so a 2-chunk request must wait.
        ByteBuffer h1 = p.allocateChunks(chunkSize, 100).get(0);
        ByteBuffer h2 = p.allocateChunks(chunkSize, 100).get(0);
        ByteBuffer h3 = p.allocateChunks(chunkSize, 100).get(0);
        assertEquals(0, p.availableMemory());

        AtomicReference<Throwable> err = new AtomicReference<>();
        AtomicReference<List<ByteBuffer>> got = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                got.set(p.allocateChunks(2 * chunkSize, MAX_BLOCK_TIME_MS));
            } catch (Throwable th) {
                err.set(th);
            }
        }, "chunked-waiter");
        t.start();

        TestUtils.waitForCondition(() -> p.queued() == 1, "waiter should be parked on the pool's queue");

        // Free 2 chunks → the waiter wakes, takes both, and completes normally.
        p.deallocate(h1);
        p.deallocate(h2);
        t.join();
        assertNull(err.get(), "waiter unexpectedly threw: " + err.get());

        // After success the waiter owns the 2 chunks (returned via `got`) and the test still holds
        // h3, so the pool has lent out everything it had — availableMemory must be exactly 0.
        assertEquals(0, p.availableMemory(),
                "a completed waiting request must leave the pool with no available memory (accounting not corrupted)");

        // Returning all the held buffers must fully restore the pool.
        for (ByteBuffer chunk : got.get())
            p.deallocate(chunk);
        p.deallocate(h3);
        assertEquals(total, p.availableMemory(),
                "pool not fully restored after returning all chunks");
    }

    /**
     * Closing the pool while a multi-chunk request is parked must surface a
     * {@link KafkaException} and refund all reserved bytes.
     */
    @Test
    public void testCloseDuringAtomicWait() throws Exception {
        int chunkSize = 64;
        long total = 2L * chunkSize;
        BufferPool p = pool(total, chunkSize);
        // Drain so the next request must wait.
        ByteBuffer h1 = p.allocateChunks(chunkSize, 100).get(0);
        ByteBuffer h2 = p.allocateChunks(chunkSize, 100).get(0);
        assertEquals(0, p.availableMemory());

        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                p.allocateChunks(2 * chunkSize, MAX_BLOCK_TIME_MS);
            } catch (Throwable th) {
                err.set(th);
            }
        }, "close-during-wait");
        t.start();
        TestUtils.waitForCondition(() -> p.queued() == 1, "waiter should be parked on the pool's queue");

        // Close the pool: signals all waiters; the waiter must throw KafkaException.
        p.close();
        t.join();
        assertInstanceOf(KafkaException.class, err.get(), "expected KafkaException, got " + err.get());
        p.deallocate(h1);
        p.deallocate(h2);
    }
}
