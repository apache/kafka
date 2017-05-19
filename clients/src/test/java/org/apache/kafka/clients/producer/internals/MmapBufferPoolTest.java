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

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
public class MmapBufferPoolTest {
    private final MockTime time = new MockTime();
    private final long maxBlockTimeMs = 2000;

    @Test
    public void testSimple() throws Exception {
        long totalMemory = 64 * 1024;
        int size = 1024;
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        BufferPool pool = new MmapBufferPool(totalMemory, size, time, storefile);
        ByteBuffer buffer = pool.allocate(size, maxBlockTimeMs);
        assertEquals("Buffer size should equal requested size.", size, buffer.limit());
        assertEquals("Unallocated memory should have shrunk", totalMemory - size, pool.unallocatedMemory());
        assertEquals("Available memory should have shrunk", totalMemory - size, pool.availableMemory());
//        buffer.putInt(1);
//        buffer.flip();
        pool.deallocate(buffer);
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("But now some is on the free list", totalMemory - size, pool.unallocatedMemory());
        buffer = pool.allocate(size, maxBlockTimeMs);
//        assertEquals("Recycled buffer should be cleared.", 0, buffer.position());
//        assertEquals("Recycled buffer should be cleared.", buffer.capacity(), buffer.limit());
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
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        BufferPool pool = new MmapBufferPool(1024, 512, time, storefile);
        ByteBuffer buffer = pool.allocate(1024, maxBlockTimeMs);
        assertEquals(1024, buffer.limit());
        pool.deallocate(buffer);
        pool.allocate(1025, maxBlockTimeMs);
    }

    private void delayedDeallocate(final BufferPool pool, final ByteBuffer buffer, final long delayMs) {
        Thread thread = new Thread() {
            public void run() {
                Time.SYSTEM.sleep(delayMs);
                pool.deallocate(buffer);
            }
        };
        thread.start();
    }

    /**
     * Test if Timeout exception is thrown when there is not enough memory to allocate and the elapsed time is greater than the max specified block time.
     * And verify that the allocation should finish soon after the maxBlockTimeMs.
     */
    @Test
    public void testBlockTimeout() throws Exception {
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        BufferPool pool = new MmapBufferPool(10, 1, time, storefile);
        ByteBuffer buffer1 = pool.allocate(1, maxBlockTimeMs);
        ByteBuffer buffer2 = pool.allocate(1, maxBlockTimeMs);
        ByteBuffer buffer3 = pool.allocate(1, maxBlockTimeMs);
        // First two buffers will be de-allocated within maxBlockTimeMs since the most recent de-allocation
        delayedDeallocate(pool, buffer1, maxBlockTimeMs / 2);
        delayedDeallocate(pool, buffer2, maxBlockTimeMs);
        // The third buffer will be de-allocated after maxBlockTimeMs since the most recent de-allocation
        delayedDeallocate(pool, buffer3, maxBlockTimeMs / 2 * 5);

        long beginTimeMs = Time.SYSTEM.milliseconds();
        try {
            pool.allocate(10, maxBlockTimeMs);
            fail("The buffer allocated more memory than its maximum value 10");
        } catch (TimeoutException e) {
            // this is good
        }
        assertTrue("available memory" + pool.availableMemory(), pool.availableMemory() >= 9 && pool.availableMemory() <= 10);
        long endTimeMs = Time.SYSTEM.milliseconds();
        assertTrue("Allocation should finish not much later than maxBlockTimeMs", endTimeMs - beginTimeMs < maxBlockTimeMs + 1000);
    }
}
