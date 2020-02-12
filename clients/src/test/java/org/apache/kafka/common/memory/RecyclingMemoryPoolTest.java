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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.junit.Assert;
import org.junit.Test;


public class RecyclingMemoryPoolTest {
    private static final int TWO_KILOBYTES = 2048;
    private static final int CACHEABLE_BUFFER_SIZE = 1024;
    private static final int BUFFER_CACHE_CAPACITY = 2;
    private static final Sensor ALLOCATE_SENSOR = new Metrics().sensor("allocate_sensor");

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeAllocation() {
        RecyclingMemoryPool memoryPool = new RecyclingMemoryPool(CACHEABLE_BUFFER_SIZE, BUFFER_CACHE_CAPACITY, ALLOCATE_SENSOR);
        memoryPool.tryAllocate(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroAllocation() {
        RecyclingMemoryPool memoryPool = new RecyclingMemoryPool(CACHEABLE_BUFFER_SIZE, BUFFER_CACHE_CAPACITY, ALLOCATE_SENSOR);
        memoryPool.tryAllocate(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRelease() {
        RecyclingMemoryPool memoryPool = new RecyclingMemoryPool(CACHEABLE_BUFFER_SIZE, BUFFER_CACHE_CAPACITY, ALLOCATE_SENSOR);
        memoryPool.release(null);
    }

    @Test
    public void testAllocation() {
        RecyclingMemoryPool memoryPool = new RecyclingMemoryPool(CACHEABLE_BUFFER_SIZE, BUFFER_CACHE_CAPACITY, ALLOCATE_SENSOR);
        ByteBuffer buffer1 = memoryPool.tryAllocate(TWO_KILOBYTES);
        ByteBuffer buffer2 = memoryPool.tryAllocate(CACHEABLE_BUFFER_SIZE);
        ByteBuffer buffer3 = memoryPool.tryAllocate(CACHEABLE_BUFFER_SIZE * 2 / 3);
        ByteBuffer buffer4 = memoryPool.tryAllocate(CACHEABLE_BUFFER_SIZE);

        memoryPool.release(buffer1);
        ByteBuffer reuse1 = memoryPool.tryAllocate(TWO_KILOBYTES);
        // Compare the references
        Assert.assertNotEquals(System.identityHashCode(reuse1), System.identityHashCode(buffer1));

        memoryPool.release(buffer2);
        memoryPool.release(buffer3);
        memoryPool.release(buffer4);
        ByteBuffer reuse2 = memoryPool.tryAllocate(CACHEABLE_BUFFER_SIZE);
        ByteBuffer reuse3 = memoryPool.tryAllocate(CACHEABLE_BUFFER_SIZE * 2 / 3);
        ByteBuffer reuse4 = memoryPool.tryAllocate(CACHEABLE_BUFFER_SIZE);

        Assert.assertEquals(System.identityHashCode(reuse2), System.identityHashCode(buffer2));
        Assert.assertEquals(System.identityHashCode(reuse3), System.identityHashCode(buffer3));
        Assert.assertNotEquals(System.identityHashCode(reuse4), System.identityHashCode(buffer4));
    }

    @Test
    public void testMultiThreadAllocation() {
        RecyclingMemoryPool memoryPool = new RecyclingMemoryPool(CACHEABLE_BUFFER_SIZE, BUFFER_CACHE_CAPACITY, ALLOCATE_SENSOR);
        AtomicReference<Throwable> error = new AtomicReference<>();
        List<Thread> processorThreads = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            processorThreads.add(new Thread(() -> {
                try {
                    ByteBuffer buffer = memoryPool.tryAllocate(CACHEABLE_BUFFER_SIZE);
                    Thread.sleep(1000);
                    memoryPool.release(buffer);
                } catch (InterruptedException e) {
                    error.compareAndSet(null, e);
                }
            }));
        }
        processorThreads.forEach(t -> {
            t.setDaemon(true);
            t.start();
        });
        processorThreads.forEach(t -> {
            try {
                t.join(30000);
            } catch (InterruptedException e) {
                error.compareAndSet(null, e);
            }
        });

        Assert.assertNull(error.get());
        Assert.assertEquals(memoryPool.bufferCache.size(), BUFFER_CACHE_CAPACITY);
    }
}
