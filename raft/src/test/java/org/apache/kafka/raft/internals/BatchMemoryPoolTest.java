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
package org.apache.kafka.raft.internals;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchMemoryPoolTest {

    @Test
    public void testAllocateAndRelease() {
        int batchSize = 1024;
        int maxRetainedBatches = 1;
        Set<ByteBuffer> released = Collections.newSetFromMap(new IdentityHashMap<>());

        BatchMemoryPool pool = new BatchMemoryPool(maxRetainedBatches, batchSize);
        assertEquals(Long.MAX_VALUE, pool.availableMemory());
        assertFalse(pool.isOutOfMemory());

        ByteBuffer buffer1 = pool.tryAllocate(batchSize);
        assertNotNull(buffer1);
        assertEquals(0, buffer1.position());
        assertEquals(batchSize, buffer1.limit());
        assertEquals(Long.MAX_VALUE, pool.availableMemory());
        assertFalse(pool.isOutOfMemory());

        // Test that allocation works even after maximum batches are allocated 
        ByteBuffer buffer2 = pool.tryAllocate(batchSize);
        assertNotNull(buffer2);
        // The size of the pool can exceed maxRetainedBatches * batchSize
        assertEquals(2 * batchSize, pool.size());
        release(update(buffer2), pool, released);

        release(update(buffer1), pool, released);
        assertEquals(maxRetainedBatches * batchSize, pool.size());

        ByteBuffer reallocated = pool.tryAllocate(batchSize);
        assertTrue(released.contains(reallocated));
        assertEquals(0, reallocated.position());
        assertEquals(batchSize, reallocated.limit());
    }

    @Test
    public void testMultipleAllocations() {
        int batchSize = 1024;
        int maxRetainedBatches = 3;
        Set<ByteBuffer> released = Collections.newSetFromMap(new IdentityHashMap<>());

        BatchMemoryPool pool = new BatchMemoryPool(maxRetainedBatches, batchSize);
        assertEquals(Long.MAX_VALUE, pool.availableMemory());

        ByteBuffer batch1 = pool.tryAllocate(batchSize);
        assertNotNull(batch1);

        ByteBuffer batch2 = pool.tryAllocate(batchSize);
        assertNotNull(batch2);

        ByteBuffer batch3 = pool.tryAllocate(batchSize);
        assertNotNull(batch3);

        // Test that allocation works even after maximum batches are allocated 
        ByteBuffer batch4 = pool.tryAllocate(batchSize);
        assertNotNull(batch4);
        // The size of the pool can exceed maxRetainedBatches * batchSize
        assertEquals(4 * batchSize, pool.size());
        release(batch4, pool, released);

        release(batch2, pool, released);
        ByteBuffer batch5 = pool.tryAllocate(batchSize);
        assertTrue(released.contains(batch5));
        released.remove(batch5);

        release(batch1, pool, released);
        release(batch3, pool, released);

        ByteBuffer batch6 = pool.tryAllocate(batchSize);
        assertTrue(released.contains(batch6));
        released.remove(batch6);

        // Release all previously allocated buffers
        release(batch5, pool, released);
        release(batch6, pool, released);
        assertEquals(maxRetainedBatches * batchSize, pool.size());
    }

    @Test
    public void testOversizeAllocation() {
        int batchSize = 1024;
        int maxRetainedBatches = 3;

        BatchMemoryPool pool = new BatchMemoryPool(maxRetainedBatches, batchSize);
        assertThrows(IllegalArgumentException.class, () -> pool.tryAllocate(batchSize + 1));
    }

    @Test
    public void testReleaseBufferNotMatchingBatchSize() {
        int batchSize = 1024;
        int maxRetainedBatches = 3;

        BatchMemoryPool pool = new BatchMemoryPool(maxRetainedBatches, batchSize);
        ByteBuffer buffer = ByteBuffer.allocate(1023);
        assertThrows(IllegalArgumentException.class, () -> pool.release(buffer));
    }

    private ByteBuffer update(ByteBuffer buffer) {
        buffer.position(512);
        buffer.limit(724);

        return buffer;
    }

    private void release(ByteBuffer buffer, BatchMemoryPool pool, Set<ByteBuffer> released) {
        pool.release(buffer);
        released.add(buffer);
    }
}
