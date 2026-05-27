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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ChunkedBufferPoolTest {
    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    private final long maxBlockTimeMs = 10;
    private final String metricGroup = "TestMetrics";

    @AfterEach
    public void teardown() {
        this.metrics.close();
    }

    @Test
    public void testAllocateExactMultipleOfChunkSize() throws Exception {
        int chunkSize = 1024;
        ChunkedBufferPool pool = new ChunkedBufferPool(64 * 1024, chunkSize, metrics, time, metricGroup);
        List<ByteBuffer> chunks = pool.allocateChunks(3 * chunkSize, maxBlockTimeMs);
        assertEquals(3, chunks.size());
        for (ByteBuffer chunk : chunks) {
            assertEquals(chunkSize, chunk.capacity());
        }
    }

    @Test
    public void testAllocateCeilsToWholeChunk() throws Exception {
        int chunkSize = 1024;
        ChunkedBufferPool pool = new ChunkedBufferPool(64 * 1024, chunkSize, metrics, time, metricGroup);
        List<ByteBuffer> chunks = pool.allocateChunks(2 * chunkSize + 1, maxBlockTimeMs);
        assertEquals(3, chunks.size());
        for (ByteBuffer chunk : chunks) {
            assertEquals(chunkSize, chunk.capacity());
        }
    }

    @Test
    public void testThrowsWhenTotalSizeExceedsTotalMemory() {
        int chunkSize = 1024;
        long totalMemory = 4L * chunkSize;
        ChunkedBufferPool pool = new ChunkedBufferPool(totalMemory, chunkSize, metrics, time, metricGroup);
        assertThrows(IllegalArgumentException.class,
                () -> pool.allocateChunks((int) totalMemory + 1, maxBlockTimeMs));
    }

    @Test
    public void testDeallocateReturnsMemoryToPool() throws Exception {
        int chunkSize = 1024;
        long totalMemory = 4L * chunkSize;
        ChunkedBufferPool pool = new ChunkedBufferPool(totalMemory, chunkSize, metrics, time, metricGroup);
        List<ByteBuffer> chunks = pool.allocateChunks(3 * chunkSize, maxBlockTimeMs);
        assertEquals(totalMemory - 3L * chunkSize, pool.availableMemory());
        for (ByteBuffer chunk : chunks) {
            pool.deallocate(chunk);
        }
        assertEquals(totalMemory, pool.availableMemory());
    }

    @Test
    public void testPartialFailureRollsBack() throws Exception {
        int chunkSize = 1024;
        long totalMemory = 2L * chunkSize;
        ChunkedBufferPool pool = new ChunkedBufferPool(totalMemory, chunkSize, metrics, time, metricGroup);

        ByteBuffer held = pool.allocate(chunkSize, maxBlockTimeMs);
        assertEquals(chunkSize, pool.availableMemory());

        assertThrows(BufferExhaustedException.class,
                () -> pool.allocateChunks(2 * chunkSize, maxBlockTimeMs));

        assertEquals(chunkSize, pool.availableMemory());
        assertEquals(0, pool.queued());

        pool.deallocate(held);
        assertEquals(totalMemory, pool.availableMemory());
    }
}
