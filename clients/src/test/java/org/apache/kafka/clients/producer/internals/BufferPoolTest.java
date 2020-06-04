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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class BufferPoolTest {
    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    private final long maxBlockTimeMs = 10;
    private final String metricGroup = "TestMetrics";

    @After
    public void teardown() {
        this.metrics.close();
    }

    /**
     * Test the simple non-blocking allocation paths
     */
    @Test
    public void testSimple() throws Exception {
        long totalMemory = 64 * 1024;
        int size = 1024;
        BufferPool pool = new BufferPool(totalMemory, size, metrics, time, metricGroup);
        ByteBuffer buffer = pool.allocate(size, maxBlockTimeMs);
        assertEquals("Buffer size should equal requested size.", size, buffer.limit());
        assertEquals("Unallocated memory should have shrunk", totalMemory - size, pool.unallocatedMemory());
        assertEquals("Available memory should have shrunk", totalMemory - size, pool.availableMemory());
        buffer.putInt(1);
        buffer.flip();
        pool.deallocate(buffer);
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("But now some is on the free list", totalMemory - size, pool.unallocatedMemory());
        buffer = pool.allocate(size, maxBlockTimeMs);
        assertEquals("Recycled buffer should be cleared.", 0, buffer.position());
        assertEquals("Recycled buffer should be cleared.", buffer.capacity(), buffer.limit());
        pool.deallocate(buffer);
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("Still a single buffer on the free list", totalMemory - size, pool.unallocatedMemory());
        buffer = pool.allocate(2 * size, maxBlockTimeMs);
        pool.deallocate(buffer);
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("Non-standard size didn't go to the free list.", totalMemory - size, pool.unallocatedMemory());
    }

    /**
     * Test that we cannot try to allocate more memory then we have in the whole pool
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCantAllocateMoreMemoryThanWeHave() throws Exception {
        BufferPool pool = new BufferPool(1024, 512, metrics, time, metricGroup);
        ByteBuffer buffer = pool.allocate(1024, maxBlockTimeMs);
        assertEquals(1024, buffer.limit());
        pool.deallocate(buffer);
        pool.allocate(1025, maxBlockTimeMs);
    }

    /**
     * Test that delayed allocation blocks
     */
    @Test
    public void testDelayedAllocation() throws Exception {
        BufferPool pool = new BufferPool(5 * 1024, 1024, metrics, time, metricGroup);
        ByteBuffer buffer = pool.allocate(1024, maxBlockTimeMs);
        CountDownLatch doDealloc = asyncDeallocate(pool, buffer);
        CountDownLatch allocation = asyncAllocate(pool, 5 * 1024);
        assertEquals("Allocation shouldn't have happened yet, waiting on memory.", 1L, allocation.getCount());
        doDealloc.countDown(); // return the memory
        assertTrue("Allocation should succeed soon after de-allocation", allocation.await(1, TimeUnit.SECONDS));
    }

    private CountDownLatch asyncDeallocate(final BufferPool pool, final ByteBuffer buffer) {
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            pool.deallocate(buffer);
        });
        thread.start();
        return latch;
    }

    private void delayedDeallocate(final BufferPool pool, final ByteBuffer buffer, final long delayMs) {
        Thread thread = new Thread(() -> {
            Time.SYSTEM.sleep(delayMs);
            pool.deallocate(buffer);
        });
        thread.start();
    }

    private CountDownLatch asyncAllocate(final BufferPool pool, final int size) {
        final CountDownLatch completed = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                pool.allocate(size, maxBlockTimeMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                completed.countDown();
            }
        });
        thread.start();
        return completed;
    }

    /**
     * Test if BufferExhausted exception is thrown when there is not enough memory to allocate and the elapsed
     * time is greater than the max specified block time.
     */
    @Test(expected = BufferExhaustedException.class)
    public void testBufferExhaustedExceptionIsThrown() throws Exception {
        BufferPool pool = new BufferPool(2, 1, metrics, time, metricGroup);
        pool.allocate(1, maxBlockTimeMs);
        pool.allocate(2, maxBlockTimeMs);
    }

    /**
     * Verify that a failed allocation attempt due to not enough memory finishes soon after the maxBlockTimeMs.
     */
    @Test
    public void testBlockTimeout() throws Exception {
        BufferPool pool = new BufferPool(10, 1, metrics, Time.SYSTEM, metricGroup);
        ByteBuffer buffer1 = pool.allocate(1, maxBlockTimeMs);
        ByteBuffer buffer2 = pool.allocate(1, maxBlockTimeMs);
        ByteBuffer buffer3 = pool.allocate(1, maxBlockTimeMs);
        // The first two buffers will be de-allocated within maxBlockTimeMs since the most recent allocation
        delayedDeallocate(pool, buffer1, maxBlockTimeMs / 2);
        delayedDeallocate(pool, buffer2, maxBlockTimeMs);
        // The third buffer will be de-allocated after maxBlockTimeMs since the most recent allocation
        delayedDeallocate(pool, buffer3, maxBlockTimeMs / 2 * 5);

        long beginTimeMs = Time.SYSTEM.milliseconds();
        try {
            pool.allocate(10, maxBlockTimeMs);
            fail("The buffer allocated more memory than its maximum value 10");
        } catch (BufferExhaustedException e) {
            // this is good
        }
        // Thread scheduling sometimes means that deallocation varies by this point
        assertTrue("available memory " + pool.availableMemory(), pool.availableMemory() >= 7 && pool.availableMemory() <= 10);
        long durationMs = Time.SYSTEM.milliseconds() - beginTimeMs;
        assertTrue("BufferExhaustedException should not throw before maxBlockTimeMs", durationMs >= maxBlockTimeMs);
        assertTrue("BufferExhaustedException should throw soon after maxBlockTimeMs", durationMs < maxBlockTimeMs + 1000);
    }

    /**
     * Test if the  waiter that is waiting on availability of more memory is cleaned up when a timeout occurs
     */
    @Test
    public void testCleanupMemoryAvailabilityWaiterOnBlockTimeout() throws Exception {
        BufferPool pool = new BufferPool(2, 1, metrics, time, metricGroup);
        pool.allocate(1, maxBlockTimeMs);
        try {
            pool.allocate(2, maxBlockTimeMs);
            fail("The buffer allocated more memory than its maximum value 2");
        } catch (BufferExhaustedException e) {
            // this is good
        }
        assertEquals(0, pool.queued());
        assertEquals(1, pool.availableMemory());
    }

    /**
     * Test if the  waiter that is waiting on availability of more memory is cleaned up when an interruption occurs
     */
    @Test
    public void testCleanupMemoryAvailabilityWaiterOnInterruption() throws Exception {
        BufferPool pool = new BufferPool(2, 1, metrics, time, metricGroup);
        long blockTime = 5000;
        pool.allocate(1, maxBlockTimeMs);
        Thread t1 = new Thread(new BufferPoolAllocator(pool, blockTime));
        Thread t2 = new Thread(new BufferPoolAllocator(pool, blockTime));
        // start thread t1 which will try to allocate more memory on to the Buffer pool
        t1.start();
        // sleep for 500ms. Condition variable c1 associated with pool.allocate() by thread t1 will be inserted in the waiters queue.
        Thread.sleep(500);
        Deque<Condition> waiters = pool.waiters();
        // get the condition object associated with pool.allocate() by thread t1
        Condition c1 = waiters.getFirst();
        // start thread t2 which will try to allocate more memory on to the Buffer pool
        t2.start();
        // sleep for 500ms. Condition variable c2 associated with pool.allocate() by thread t2 will be inserted in the waiters queue. The waiters queue will have 2 entries c1 and c2.
        Thread.sleep(500);
        t1.interrupt();
        // sleep for 500ms.
        Thread.sleep(500);
        // get the condition object associated with allocate() by thread t2
        Condition c2 = waiters.getLast();
        t2.interrupt();
        assertNotEquals(c1, c2);
        t1.join();
        t2.join();
        // both the allocate() called by threads t1 and t2 should have been interrupted and the waiters queue should be empty
        assertEquals(pool.queued(), 0);
    }

    @Test
    public void testCleanupMemoryAvailabilityOnMetricsException() throws Exception {
        BufferPool bufferPool = spy(new BufferPool(2, 1, new Metrics(), time, metricGroup));
        doThrow(new OutOfMemoryError()).when(bufferPool).recordWaitTime(anyLong());

        bufferPool.allocate(1, 0);
        try {
            bufferPool.allocate(2, 1000);
            fail("Expected oom.");
        } catch (OutOfMemoryError expected) {
        }
        assertEquals(1, bufferPool.availableMemory());
        assertEquals(0, bufferPool.queued());
        assertEquals(1, bufferPool.unallocatedMemory());
        //This shouldn't timeout
        bufferPool.allocate(1, 0);

        verify(bufferPool).recordWaitTime(anyLong());
    }

    private static class BufferPoolAllocator implements Runnable {
        BufferPool pool;
        long maxBlockTimeMs;

        BufferPoolAllocator(BufferPool pool, long maxBlockTimeMs) {
            this.pool = pool;
            this.maxBlockTimeMs = maxBlockTimeMs;
        }

        @Override
        public void run() {
            try {
                pool.allocate(2, maxBlockTimeMs);
                fail("The buffer allocated more memory than its maximum value 2");
            } catch (BufferExhaustedException e) {
                // this is good
            } catch (InterruptedException e) {
                // this can be neglected
            }
        }
    }

    /**
     * This test creates lots of threads that hammer on the pool
     */
    @Test
    public void testStressfulSituation() throws Exception {
        int numThreads = 10;
        final int iterations = 50000;
        final int poolableSize = 1024;
        final long totalMemory = numThreads / 2 * poolableSize;
        final BufferPool pool = new BufferPool(totalMemory, poolableSize, metrics, time, metricGroup);
        List<StressTestThread> threads = new ArrayList<StressTestThread>();
        for (int i = 0; i < numThreads; i++)
            threads.add(new StressTestThread(pool, iterations));
        for (StressTestThread thread : threads)
            thread.start();
        for (StressTestThread thread : threads)
            thread.join();
        for (StressTestThread thread : threads)
            assertTrue("Thread should have completed all iterations successfully.", thread.success.get());
        assertEquals(totalMemory, pool.availableMemory());
    }

    @Test
    public void testLargeAvailableMemory() throws Exception {
        long memory = 20_000_000_000L;
        int poolableSize = 2_000_000_000;
        final AtomicInteger freeSize = new AtomicInteger(0);
        BufferPool pool = new BufferPool(memory, poolableSize, metrics, time, metricGroup) {
            @Override
            protected ByteBuffer allocateByteBuffer(int size) {
                // Ignore size to avoid OOM due to large buffers
                return ByteBuffer.allocate(0);
            }

            @Override
            protected int freeSize() {
                return freeSize.get();
            }
        };
        pool.allocate(poolableSize, 0);
        assertEquals(18_000_000_000L, pool.availableMemory());
        pool.allocate(poolableSize, 0);
        assertEquals(16_000_000_000L, pool.availableMemory());

        // Emulate `deallocate` by increasing `freeSize`
        freeSize.incrementAndGet();
        assertEquals(18_000_000_000L, pool.availableMemory());
        freeSize.incrementAndGet();
        assertEquals(20_000_000_000L, pool.availableMemory());
    }

    @Test
    public void outOfMemoryOnAllocation() {
        BufferPool bufferPool = new BufferPool(1024, 1024, metrics, time, metricGroup) {
            @Override
            protected ByteBuffer allocateByteBuffer(int size) {
                throw new OutOfMemoryError();
            }
        };

        try {
            bufferPool.allocateByteBuffer(1024);
            // should not reach here
            fail("Should have thrown OutOfMemoryError");
        } catch (OutOfMemoryError ignored) {

        }

        assertEquals(bufferPool.availableMemory(), 1024);
    }

    public static class StressTestThread extends Thread {
        private final int iterations;
        private final BufferPool pool;
        private final long maxBlockTimeMs =  20_000;
        public final AtomicBoolean success = new AtomicBoolean(false);

        public StressTestThread(BufferPool pool, int iterations) {
            this.iterations = iterations;
            this.pool = pool;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < iterations; i++) {
                    int size;
                    if (TestUtils.RANDOM.nextBoolean())
                        // allocate poolable size
                        size = pool.poolableSize();
                    else
                        // allocate a random size
                        size = TestUtils.RANDOM.nextInt((int) pool.totalMemory());
                    ByteBuffer buffer = pool.allocate(size, maxBlockTimeMs);
                    pool.deallocate(buffer);
                }
                success.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testCloseAllocations() throws Exception {
        BufferPool pool = new BufferPool(10, 1, metrics, Time.SYSTEM, metricGroup);
        ByteBuffer buffer = pool.allocate(1, maxBlockTimeMs);

        // Close the buffer pool. This should prevent any further allocations.
        pool.close();

        assertThrows(KafkaException.class, () -> pool.allocate(1, maxBlockTimeMs));

        // Ensure deallocation still works.
        pool.deallocate(buffer);
    }

    @Test
    public void testCloseNotifyWaiters() throws Exception {
        final int numWorkers = 2;

        BufferPool pool = new BufferPool(1, 1, metrics, Time.SYSTEM, metricGroup);
        ByteBuffer buffer = pool.allocate(1, Long.MAX_VALUE);

        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        Callable<Void> work = new Callable<Void>() {
                public Void call() throws Exception {
                    assertThrows(KafkaException.class, () -> pool.allocate(1, Long.MAX_VALUE));
                    return null;
                }
            };
        for (int i = 0; i < numWorkers; ++i) {
            executor.submit(work);
        }

        TestUtils.waitForCondition(() -> pool.queued() == numWorkers, "Awaiting " + numWorkers + " workers to be blocked on allocation");

        // Close the buffer pool. This should notify all waiters.
        pool.close();

        TestUtils.waitForCondition(() -> pool.queued() == 0, "Awaiting " + numWorkers + " workers to be interrupted from allocation");

        pool.deallocate(buffer);
    }

}
