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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchMemoryPoolTest {

    @Test
    public void testAllocateAndRelease() {
        int batchSize = 1024;
        int maxBatches = 1;

        BatchMemoryPool pool = new BatchMemoryPool(maxBatches, batchSize);
        assertEquals(batchSize, pool.availableMemory());
        assertFalse(pool.isOutOfMemory());

        ByteBuffer allocated = pool.tryAllocate(batchSize);
        assertNotNull(allocated);
        assertEquals(0, allocated.position());
        assertEquals(batchSize, allocated.limit());
        assertEquals(0, pool.availableMemory());
        assertTrue(pool.isOutOfMemory());
        assertNull(pool.tryAllocate(batchSize));

        allocated.position(512);
        allocated.limit(724);

        pool.release(allocated);
        ByteBuffer reallocated = pool.tryAllocate(batchSize);
        assertSame(allocated, reallocated);
        assertEquals(0, allocated.position());
        assertEquals(batchSize, allocated.limit());
    }

    @Test
    public void testMultipleAllocations() {
        int batchSize = 1024;
        int maxBatches = 3;

        BatchMemoryPool pool = new BatchMemoryPool(maxBatches, batchSize);
        assertEquals(batchSize * maxBatches, pool.availableMemory());

        ByteBuffer batch1 = pool.tryAllocate(batchSize);
        assertNotNull(batch1);

        ByteBuffer batch2 = pool.tryAllocate(batchSize);
        assertNotNull(batch2);

        ByteBuffer batch3 = pool.tryAllocate(batchSize);
        assertNotNull(batch3);

        assertNull(pool.tryAllocate(batchSize));

        pool.release(batch2);
        assertSame(batch2, pool.tryAllocate(batchSize));

        pool.release(batch1);
        pool.release(batch3);
        ByteBuffer buffer = pool.tryAllocate(batchSize);
        assertTrue(buffer == batch1 || buffer == batch3);
    }

    @Test
    public void testOversizeAllocation() {
        int batchSize = 1024;
        int maxBatches = 3;

        BatchMemoryPool pool = new BatchMemoryPool(maxBatches, batchSize);
        assertThrows(IllegalArgumentException.class, () -> pool.tryAllocate(batchSize + 1));
    }

    @Test
    public void testReleaseBufferNotMatchingBatchSize() {
        int batchSize = 1024;
        int maxBatches = 3;

        BatchMemoryPool pool = new BatchMemoryPool(maxBatches, batchSize);
        ByteBuffer buffer = ByteBuffer.allocate(1023);
        assertThrows(IllegalArgumentException.class, () -> pool.release(buffer));
    }

}
