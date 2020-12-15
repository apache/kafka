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
package org.apache.kafka.common.memory;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.Utils;
import org.junit.Assert;
import org.junit.Test;


public class GarbageCollectedMemoryPoolTest {

    @Test(expected = IllegalArgumentException.class)
    public void testZeroSize() throws Exception {
        new GarbageCollectedMemoryPool(0, 7, true, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeSize() throws Exception {
        new GarbageCollectedMemoryPool(-1, 7, false, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroMaxAllocation() throws Exception {
        new GarbageCollectedMemoryPool(100, 0, true, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeMaxAllocation() throws Exception {
        new GarbageCollectedMemoryPool(100, -1, false, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxAllocationLargerThanSize() throws Exception {
        new GarbageCollectedMemoryPool(100, 101, true, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAllocationOverMaxAllocation() throws Exception {
        GarbageCollectedMemoryPool pool = new GarbageCollectedMemoryPool(1000, 10, false, null);
        pool.tryAllocate(11);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAllocationZero() throws Exception {
        GarbageCollectedMemoryPool pool = new GarbageCollectedMemoryPool(1000, 10, true, null);
        pool.tryAllocate(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAllocationNegative() throws Exception {
        GarbageCollectedMemoryPool pool = new GarbageCollectedMemoryPool(1000, 10, false, null);
        pool.tryAllocate(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReleaseNull() throws Exception {
        GarbageCollectedMemoryPool pool = new GarbageCollectedMemoryPool(1000, 10, true, null);
        pool.release(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReleaseForeignBuffer() throws Exception {
        GarbageCollectedMemoryPool pool = new GarbageCollectedMemoryPool(1000, 10, true, null);
        ByteBuffer fellOffATruck = ByteBuffer.allocate(1);
        pool.release(fellOffATruck);
        pool.close();
    }

    @Test
    public void testDoubleFree() throws Exception {
        GarbageCollectedMemoryPool pool = new GarbageCollectedMemoryPool(1000, 10, false, null);
        ByteBuffer buffer = pool.tryAllocate(5);
        Assert.assertNotNull(buffer);
        pool.release(buffer); //so far so good
        try {
            pool.release(buffer);
            Assert.fail("2nd release() should have failed");
        } catch (IllegalArgumentException e) {
            //as expected
        } catch (Throwable t) {
            Assert.fail("expected an IllegalArgumentException. instead got " + t);
        }
    }

    @Test
    public void testAllocationBound() throws Exception {
        GarbageCollectedMemoryPool pool = new GarbageCollectedMemoryPool(21, 10, false, null);
        ByteBuffer buf1 = pool.tryAllocate(10);
        Assert.assertNotNull(buf1);
        Assert.assertEquals(10, buf1.capacity());
        ByteBuffer buf2 = pool.tryAllocate(10);
        Assert.assertNotNull(buf2);
        Assert.assertEquals(10, buf2.capacity());
        ByteBuffer buf3 = pool.tryAllocate(10);
        Assert.assertNotNull(buf3);
        Assert.assertEquals(10, buf3.capacity());
        //no more allocations
        Assert.assertNull(pool.tryAllocate(1));
        //release a buffer
        pool.release(buf3);
        //now we can have more
        ByteBuffer buf4 = pool.tryAllocate(10);
        Assert.assertNotNull(buf4);
        Assert.assertEquals(10, buf4.capacity());
        //no more allocations
        Assert.assertNull(pool.tryAllocate(1));
    }

    @Test
    public void testBuffersGarbageCollected() throws Exception {
        Runtime runtime = Runtime.getRuntime();
        long maxHeap = runtime.maxMemory(); //in bytes
        long maxPool = maxHeap / 2;
        long maxSingleAllocation = maxPool / 10;
        Assert.assertTrue(maxSingleAllocation < Integer.MAX_VALUE / 2); //test JVM running with too much memory for this test logic (?)
        GarbageCollectedMemoryPool pool = new GarbageCollectedMemoryPool(maxPool, (int) maxSingleAllocation, false, null);

        //we will allocate 30 buffers from this pool, which is sized such that at-most
        //11 should coexist and 30 do not fit in the JVM memory, proving that:
        // 1. buffers were reclaimed and
        // 2. the pool registered the reclamation.

        int timeoutSeconds = 30;
        long giveUp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        boolean success = false;

        int buffersAllocated = 0;
        while (System.currentTimeMillis() < giveUp) {
            ByteBuffer buffer = pool.tryAllocate((int) maxSingleAllocation);
            if (buffer == null) {
                System.gc();
                Thread.sleep(10);
                continue;
            }
            buffersAllocated++;
            if (buffersAllocated >= 30) {
                success = true;
                break;
            }
        }

        Assert.assertTrue("failed to allocate 30 buffers in " + timeoutSeconds + " seconds."
                + " buffers allocated: " + buffersAllocated + " heap " + Utils.formatBytes(maxHeap)
                + " pool " + Utils.formatBytes(maxPool) + " single allocation "
                + Utils.formatBytes(maxSingleAllocation), success);
    }
}
