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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ChunkedBufferPool extends BufferPool {

    /**
     * Create a new buffer pool
     *
     * @param memory        The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize  The buffer size to cache in the free list rather than deallocating
     * @param metrics       instance of Metrics
     * @param time          time instance
     * @param metricGrpName logical group name for metrics
     */
    public ChunkedBufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        super(memory, poolableSize, metrics, time, metricGrpName);
    }

    /**
     * Allocate {@code ceil(totalSize / chunkSize)} chunk-sized buffers. Blocks up to
     * {@code maxTimeToBlockMs} total across all chunk acquisitions, shrinking the budget after
     * each one. On any failure (timeout, close, interruption), chunks already acquired in this
     * call are returned to the pool before the exception propagates.
     *
     * @param totalSize        Minimum total capacity required across the returned chunks
     * @param maxTimeToBlockMs Maximum time in milliseconds to block waiting for memory
     * @return list of {@code ceil(totalSize / chunkSize)} ByteBuffers, each of capacity {@code chunkSize}
     * @throws InterruptedException     If interrupted while waiting
     * @throws IllegalArgumentException If {@code totalSize <= 0}
     */
    public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
        if (totalSize > totalMemory()) {
            throw new IllegalArgumentException("Attempt to allocate " + totalSize
                    + " bytes across chunks, but there is a hard limit of "
                    + totalMemory() + " on memory allocations.");
        }

        int chunkSize = poolableSize();
        int numChunks = (totalSize + chunkSize - 1) / chunkSize;  // ceil division
        List<ByteBuffer> chunks = new ArrayList<>(numChunks);
        long deadlineMs = time.milliseconds() + maxTimeToBlockMs;
        try {
            for (int i = 0; i < numChunks; i++) {
                long remainingMs = Math.max(0L, deadlineMs - time.milliseconds());
                chunks.add(allocate(chunkSize, remainingMs));
            }
            List<ByteBuffer> result = chunks;
            chunks = null;
            return result;
        } finally {
            if (chunks != null) {
                for (ByteBuffer chunk : chunks) {
                    deallocate(chunk);
                }
            }
        }
    }
}
