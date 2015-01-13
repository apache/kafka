/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class BufferPoolTest {
    private MockTime time = new MockTime();
    private Metrics metrics = new Metrics(time);
    String metricGroup = "TestMetrics";
    Map<String, String> metricTags = new LinkedHashMap<String, String>();

    /**
     * Test the simple non-blocking allocation paths
     */
    @Test
    public void testSimple() throws Exception {
        int totalMemory = 64 * 1024;
        int size = 1024;
        BufferPool pool = new BufferPool(totalMemory, size, false, metrics, time, metricGroup, metricTags);
        ByteBuffer buffer = pool.allocate(size);
        assertEquals("Buffer size should equal requested size.", size, buffer.limit());
        assertEquals("Unallocated memory should have shrunk", totalMemory - size, pool.unallocatedMemory());
        assertEquals("Available memory should have shrunk", totalMemory - size, pool.availableMemory());
        buffer.putInt(1);
        buffer.flip();
        pool.deallocate(buffer);
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("But now some is on the free list", totalMemory - size, pool.unallocatedMemory());
        buffer = pool.allocate(size);
        assertEquals("Recycled buffer should be cleared.", 0, buffer.position());
        assertEquals("Recycled buffer should be cleared.", buffer.capacity(), buffer.limit());
        pool.deallocate(buffer);
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("Still a single buffer on the free list", totalMemory - size, pool.unallocatedMemory());
        buffer = pool.allocate(2 * size);
        pool.deallocate(buffer);
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("Non-standard size didn't go to the free list.", totalMemory - size, pool.unallocatedMemory());
    }

    /**
     * Test that we cannot try to allocate more memory then we have in the whole pool
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCantAllocateMoreMemoryThanWeHave() throws Exception {
        BufferPool pool = new BufferPool(1024, 512, true, metrics, time, metricGroup, metricTags);
        ByteBuffer buffer = pool.allocate(1024);
        assertEquals(1024, buffer.limit());
        pool.deallocate(buffer);
        buffer = pool.allocate(1025);
    }

    @Test
    public void testNonblockingMode() throws Exception {
        BufferPool pool = new BufferPool(2, 1, false, metrics, time, metricGroup, metricTags);
        pool.allocate(1);
        try {
            pool.allocate(2);
            fail("The buffer allocated more than it has!");
        } catch (BufferExhaustedException e) {
            // this is good
        }
    }

    /**
     * Test that delayed allocation blocks
     */
    @Test
    public void testDelayedAllocation() throws Exception {
        BufferPool pool = new BufferPool(5 * 1024, 1024, true, metrics, time, metricGroup, metricTags);
        ByteBuffer buffer = pool.allocate(1024);
        CountDownLatch doDealloc = asyncDeallocate(pool, buffer);
        CountDownLatch allocation = asyncAllocate(pool, 5 * 1024);
        assertEquals("Allocation shouldn't have happened yet, waiting on memory.", 1, allocation.getCount());
        doDealloc.countDown(); // return the memory
        allocation.await();
    }

    private CountDownLatch asyncDeallocate(final BufferPool pool, final ByteBuffer buffer) {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                pool.deallocate(buffer);
            }
        }.start();
        return latch;
    }

    private CountDownLatch asyncAllocate(final BufferPool pool, final int size) {
        final CountDownLatch completed = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    pool.allocate(size);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    completed.countDown();
                }
            }
        }.start();
        return completed;
    }

    /**
     * This test creates lots of threads that hammer on the pool
     */
    @Test
    public void testStressfulSituation() throws Exception {
        int numThreads = 10;
        final int iterations = 50000;
        final int poolableSize = 1024;
        final int totalMemory = numThreads / 2 * poolableSize;
        final BufferPool pool = new BufferPool(totalMemory, poolableSize, true, metrics, time, metricGroup, metricTags);
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

    public static class StressTestThread extends Thread {
        private final int iterations;
        private final BufferPool pool;
        public final AtomicBoolean success = new AtomicBoolean(false);

        public StressTestThread(BufferPool pool, int iterations) {
            this.iterations = iterations;
            this.pool = pool;
        }

        public void run() {
            try {
                for (int i = 0; i < iterations; i++) {
                    int size;
                    if (TestUtils.random.nextBoolean())
                        // allocate poolable size
                        size = pool.poolableSize();
                    else
                        // allocate a random size
                        size = TestUtils.random.nextInt((int) pool.totalMemory());
                    ByteBuffer buffer = pool.allocate(size);
                    pool.deallocate(buffer);
                }
                success.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
