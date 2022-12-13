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

import org.apache.kafka.common.memory.MemoryPool;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple memory pool that tries to maintain a limited number of fixed-size buffers.
 *
 * This type implements an unbounded memory pool. When releasing byte buffers they will get pooled
 * up to the maximum retained number of batches.
 */
public class BatchMemoryPool implements MemoryPool {
    private final ReentrantLock lock;
    private final Deque<ByteBuffer> free;
    private final int maxRetainedBatches;
    private final int batchSize;

    private int numAllocatedBatches = 0;

    /**
     * Construct a memory pool.
     *
     * The byte buffers are always of batchSize size. The memory pool is unbounded but it will retain
     * up to maxRetainedBatches byte buffers for reuse.
     *
     * @param maxRetainedBatches maximum number of byte buffers to pool for reuse
     * @param batchSize the size of each byte buffer
     */
    public BatchMemoryPool(int maxRetainedBatches, int batchSize) {
        this.maxRetainedBatches = maxRetainedBatches;
        this.batchSize = batchSize;
        this.free = new ArrayDeque<>(maxRetainedBatches);
        this.lock = new ReentrantLock();
    }

    /**
     * Allocate a byte buffer with {@code batchSize} in this pool.
     *
     * This method should always succeed and never return null. The sizeBytes parameter must be less than
     * the batchSize used in the constructor.
     *
     * @param sizeBytes is used to determine if the requested size is exceeding the batchSize
     * @throws IllegalArgumentException if sizeBytes is greater than batchSize
     */
    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes > batchSize) {
            throw new IllegalArgumentException("Cannot allocate buffers larger than max " +
                "batch size of " + batchSize);
        }

        lock.lock();
        try {
            ByteBuffer buffer = free.poll();
            // Always allocation a new buffer if there are no free buffers
            if (buffer == null) {
                buffer = ByteBuffer.allocate(batchSize);
                numAllocatedBatches += 1;
            }

            return buffer;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Release a previously allocated byte buffer.
     *
     * The byte buffer is pooled if the number of pooled byte buffer is less than the maxRetainedBatches in
     * the constructor. Otherwise, the byte buffer is return to the JVM for garbage collection.
     */
    @Override
    public void release(ByteBuffer previouslyAllocated) {
        lock.lock();
        try {
            previouslyAllocated.clear();

            if (previouslyAllocated.capacity() != batchSize) {
                throw new IllegalArgumentException("Released buffer with unexpected size "
                    + previouslyAllocated.capacity());
            }

            // Free the buffer if the number of pooled buffers is already the maximum number of batches.
            // Otherwise return the buffer to the memory pool.
            if (free.size() >= maxRetainedBatches) {
                numAllocatedBatches--;
            } else {
                free.offer(previouslyAllocated);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Release the retained buffers in the free pool.
     */
    public void releaseRetained() {
        lock.lock();
        try {
            free.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long size() {
        lock.lock();
        try {
            return numAllocatedBatches * (long) batchSize;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long availableMemory() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean isOutOfMemory() {
        return false;
    }

}
