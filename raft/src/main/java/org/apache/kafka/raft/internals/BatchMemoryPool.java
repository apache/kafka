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
 * Simple memory pool which maintains a limited number of fixed-size buffers.
 */
public class BatchMemoryPool implements MemoryPool {
    private final ReentrantLock lock;
    private final Deque<ByteBuffer> free;
    private final int maxBatches;
    private final int batchSize;

    private int numAllocatedBatches = 0;

    public BatchMemoryPool(int maxBatches, int batchSize) {
        this.maxBatches = maxBatches;
        this.batchSize = batchSize;
        this.free = new ArrayDeque<>(maxBatches);
        this.lock = new ReentrantLock();
    }

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes > batchSize) {
            throw new IllegalArgumentException("Cannot allocate buffers larger than max " +
                "batch size of " + batchSize);
        }

        lock.lock();
        try {
            ByteBuffer buffer = free.poll();
            if (buffer == null && numAllocatedBatches < maxBatches) {
                buffer = ByteBuffer.allocate(batchSize);
                numAllocatedBatches += 1;
            }
            return buffer;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void release(ByteBuffer previouslyAllocated) {
        lock.lock();
        try {
            previouslyAllocated.clear();

            if (previouslyAllocated.limit() != batchSize) {
                throw new IllegalArgumentException("Released buffer with unexpected size "
                    + previouslyAllocated.limit());
            }

            free.offer(previouslyAllocated);
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
        lock.lock();
        try {
            int freeBatches = free.size() + (maxBatches - numAllocatedBatches);
            return freeBatches * (long) batchSize;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isOutOfMemory() {
        return availableMemory() == 0;
    }

}
