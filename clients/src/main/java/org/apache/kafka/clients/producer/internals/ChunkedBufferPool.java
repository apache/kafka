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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * A {@link BufferPool} dedicated to chunk-sized buffer reuse (chunk size = {@link #poolableSize()}).
 * <p>
 * Adds {@link #allocateChunks(int, long)} to acquire multiple chunks atomically.
 */
public class ChunkedBufferPool extends BufferPool {

    public ChunkedBufferPool(long memory, int chunkSize, Metrics metrics, Time time, String metricGrpName) {
        super(memory, chunkSize, metrics, time, metricGrpName);
    }

    /**
     * Allocate {@code ceil(totalSize / chunkSize)} chunk-sized buffers atomically, mirroring
     * {@link BufferPool#allocate}: satisfied immediately if memory is available, else blocks up to
     * {@code maxTimeToBlockMs} for the whole request (FIFO on {@link #waiters}).
     * The reservation is tracked as bytes against {@link #nonPooledAvailableMemory} plus chunks polled
     * from {@link #free}. Any failure refunds the whole reservation and signals
     * the next waiter before the exception propagates, so no partial holds are visible during the wait.
     *
     * @param totalSize        minimum total bytes of capacity required across the returned chunks
     * @param maxTimeToBlockMs maximum time in milliseconds to block waiting for memory
     * @return list of {@code ceil(totalSize / chunkSize)} {@code ByteBuffer}s, each of capacity
     *         {@code chunkSize}
     * @throws InterruptedException     if interrupted while waiting
     * @throws IllegalArgumentException if {@code totalSize <= 0}, or if the request rounded up to
     *         whole chunks exceeds {@code totalMemory()}
     * @throws BufferExhaustedException if the request can't be satisfied within {@code maxTimeToBlockMs}
     * @throws KafkaException           if the pool is closed during the wait
     */
    public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
        if (totalSize <= 0)
            throw new IllegalArgumentException("totalSize must be positive: " + totalSize);
        throwIfChunksNeededExceedsPool(totalSize);

        int chunkSize = poolableSize();
        int numChunks = (int) (((long) totalSize + chunkSize - 1L) / chunkSize);
        long memoryRequired = (long) numChunks * chunkSize;

        // Chunks taken from the free list. The remaining bytes are reserved against
        // nonPooledAvailableMemory and materialized as raw allocations after the lock is released.
        List<ByteBuffer> pooled = new ArrayList<>(numChunks);

        lock.lock();
        if (this.closed) {
            lock.unlock();
            throw new KafkaException("Producer closed while allocating memory");
        }
        try {
            long freeListBytes = (long) free.size() * chunkSize;
            if (this.nonPooledAvailableMemory + freeListBytes >= memoryRequired) {
                // Enough memory available to allocate the chunks needed
                while (pooled.size() < numChunks && !free.isEmpty())
                    pooled.add(free.pollFirst());
                long remainingBytes = memoryRequired - (long) pooled.size() * chunkSize;
                if (remainingBytes > 0) {
                    // remainingBytes <= memoryRequired <= totalMemory (validated above), so the int cast is safe.
                    freeUp((int) remainingBytes);
                    this.nonPooledAvailableMemory -= remainingBytes;
                }
            } else {
                // Not enough memory available to allocate the chunks needed, so we need to wait for memory.
                // Same as in BufferPool.allocate, but wait to acquire the memory needed for all the chunks.
                // A single Condition is added to the waiter's list to ensure FIFO fairness at the request level.
                //
                // `accumulated` tracks bytes drawn from nonPooledAvailableMemory only (pool chunks
                // already taken live in `pooled`). Matches BufferPool.allocate's semantics: on
                // failure, `accumulated` is exactly the amount to refund; on success it is reset to 0.
                long accumulated = 0;
                Condition moreMemory = lock.newCondition();
                try {
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    waiters.addLast(moreMemory);
                    while ((long) pooled.size() * chunkSize + accumulated < memoryRequired) {
                        long startWaitNs = time.nanoseconds();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            long endWaitNs = time.nanoseconds();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
                            recordWaitTime(timeNs);
                        }

                        if (this.closed)
                            throw new KafkaException("Producer closed while allocating memory");

                        if (waitingTimeElapsed) {
                            throw new BufferExhaustedException("Failed to allocate " + memoryRequired
                                + " bytes (" + numChunks + " chunks of " + chunkSize
                                + ") within the configured max blocking time " + maxTimeToBlockMs
                                + " ms. Total memory: " + totalMemory() + " bytes. Available memory: "
                                + availableMemory() + " bytes.");
                        }

                        remainingTimeToBlockNs -= timeNs;

                        // Take pool chunks first, then reserve non-pool bytes for the remainder.
                        while (pooled.size() < numChunks
                                && (long) (pooled.size() + 1) * chunkSize + accumulated <= memoryRequired
                                && !free.isEmpty()) {
                            pooled.add(free.pollFirst());
                        }
                        long stillNeeded = memoryRequired - (long) pooled.size() * chunkSize - accumulated;
                        if (stillNeeded > 0) {
                            freeUp((int) stillNeeded);
                            long got = Math.min(stillNeeded, this.nonPooledAvailableMemory);
                            this.nonPooledAvailableMemory -= got;
                            accumulated += got;
                        }
                    }
                    // Clear the rollback tracker.
                    accumulated = 0;
                } finally {
                    // On failure (timeout / close / interrupt), refund the non-pool bytes taken.
                    // Pool chunks already in `pooled` are returned to `free` separately by the
                    // outer catch.
                    this.nonPooledAvailableMemory += accumulated;
                    waiters.remove(moreMemory);
                }
            }
        } catch (RuntimeException | InterruptedException e) {
            // Any chunks taken from the pool must be returned.
            for (ByteBuffer chunk : pooled)
                free.addFirst(chunk);
            throw e;
        } finally {
            try {
                signalNextWaiterIfMemoryAvailable();
            } finally {
                lock.unlock();
            }
        }

        // Allocate raw chunks for the reserved non-pooled portion outside the lock. On error,
        // refund all reserved bytes (memoryRequired) and let the next waiter try.
        int chunksStillNeeded = numChunks - pooled.size();
        List<ByteBuffer> result = new ArrayList<>(numChunks);
        result.addAll(pooled);
        boolean error = true;
        try {
            for (int i = 0; i < chunksStillNeeded; i++)
                result.add(allocateByteBuffer(chunkSize));
            error = false;
            return result;
        } finally {
            if (error) {
                // The pooled buffers we already drained are also lost on this path; mirror
                // BufferPool.safeAllocateByteBuffer's behaviour (the bytes return, the ByteBuffer
                // instances become garbage).
                releaseReservedBytes(memoryRequired);
            }
        }
    }

    /**
     * Throw if rounding {@code totalSize} up to whole chunks would exceed the pool.
     */
    private void throwIfChunksNeededExceedsPool(int totalSize) {
        int chunkSize = poolableSize();
        int numChunks = (int) (((long) totalSize + chunkSize - 1L) / chunkSize);
        long memoryRequired = (long) numChunks * chunkSize;
        if (memoryRequired > totalMemory())
            throw new IllegalArgumentException("Attempt to allocate " + totalSize + " bytes ("
                + numChunks + " chunks of " + chunkSize + " = " + memoryRequired + " bytes), but the "
                + "hard limit on memory allocations is " + totalMemory() + ".");
    }
}
