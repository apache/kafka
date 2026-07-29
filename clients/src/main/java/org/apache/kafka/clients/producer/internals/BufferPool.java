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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public class BufferPool {

    static final String WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time";

    /**
     * Which allocation method a pool serves: {@link #FULL} accepts only {@link #allocate} (the full
     * strategy), {@link #INCREMENTAL} only {@link #allocateChunks} (the incremental strategy). Fixed at
     * construction so the two are never mixed on the same pool.
     */
    public enum AllocationMode { FULL, INCREMENTAL }

    private final long totalMemory;
    private final int poolableSize;
    /** Lock held for any read or write of {@link #free}, {@link #waiters}, {@link #nonPooledAvailableMemory}, or {@link #closed}. */
    private final ReentrantLock lock;
    /** Pooled buffers of capacity {@link #poolableSize}, available for reuse. Guarded by {@link #lock}. */
    private final Deque<ByteBuffer> free;
    /** FIFO queue of pending allocation requests; the longest-waiting thread is woken first. Guarded by {@link #lock}. */
    private final Deque<Condition> waiters;
    /** Total available memory is the sum of nonPooledAvailableMemory and the number of byte buffers in free * poolableSize. Guarded by {@link #lock}. */
    private long nonPooledAvailableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;
    private boolean closed;
    private final AllocationMode allocationMode;

    /**
     * Create a new buffer pool
     *
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this(memory, poolableSize, metrics, time, metricGrpName, AllocationMode.FULL);
    }

    /**
     * Create a new buffer pool that serves the given {@link AllocationMode}.
     *
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     * @param allocationMode which allocation method this pool serves ({@link #allocate} vs {@link #allocateChunks})
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName, AllocationMode allocationMode) {
        this.allocationMode = allocationMode;
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        this.totalMemory = memory;
        this.nonPooledAvailableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor(WAIT_TIME_SENSOR_NAME);
        MetricName rateMetricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        MetricName totalNsMetricName = metrics.metricName("bufferpool-wait-time-ns-total",
                                                    metricGrpName,
                                                    "The total time in nanoseconds an appender waits for space allocation.");

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        MetricName bufferExhaustedRateMetricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        MetricName bufferExhaustedTotalMetricName = metrics.metricName("buffer-exhausted-total", metricGrpName, "The total number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(new Meter(bufferExhaustedRateMetricName, bufferExhaustedTotalMetricName));

        this.waitTime.add(new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalNsMetricName));
        this.closed = false;
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     *
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (allocationMode != AllocationMode.FULL)
            throw new IllegalStateException("allocate() is not supported in " + allocationMode
                + " allocation mode; use allocateChunks()");
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        ByteBuffer buffer = null;
        this.lock.lock();

        if (this.closed) {
            this.lock.unlock();
            throw new KafkaException("Producer closed while allocating memory");
        }

        try {
            // check if we have a free buffer of the right size pooled
            if (size == poolableSize && !this.free.isEmpty())
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            int freeListSize = freeSize() * this.poolableSize;
            if (this.nonPooledAvailableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request, but need to allocate the buffer
                freeUp(size);
                this.nonPooledAvailableMemory -= size;
            } else {
                // we are out of memory and will have to block
                int accumulated = 0;
                Condition moreMemory = this.lock.newCondition();
                try {
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    this.waiters.addLast(moreMemory);
                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    while (accumulated < size) {
                        remainingTimeToBlockNs -= awaitMemory(moreMemory, remainingTimeToBlockNs, true,
                            () -> "Failed to allocate " + size + " bytes within the configured max blocking time "
                                + maxTimeToBlockMs + " ms. Total memory: " + totalMemory() + " bytes. Available memory: " + availableMemory()
                                + " bytes. Poolable size: " + poolableSize() + " bytes");

                        // check if we can satisfy this request from the free list,
                        // otherwise allocate memory
                        if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                            // just grab a buffer from the free list
                            buffer = this.free.pollFirst();
                            accumulated = size;
                        } else {
                            // we'll need to allocate memory, but we may only get
                            // part of what we need on this iteration
                            freeUp(size - accumulated);
                            int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
                            this.nonPooledAvailableMemory -= got;
                            accumulated += got;
                        }
                    }
                    // Don't reclaim memory on throwable since nothing was thrown
                    accumulated = 0;
                } finally {
                    // When this loop was not able to successfully terminate don't loose available memory
                    this.nonPooledAvailableMemory += accumulated;
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left
            // over for them
            try {
                signalNextWaiterIfMemoryAvailable();
            } finally {
                // Another finally... otherwise find bugs complains
                lock.unlock();
            }
        }

        if (buffer == null)
            return safeAllocateByteBuffer(size);
        else
            return buffer;
    }

    /**
     * Block once on {@code moreMemory} for up to {@code remainingTimeToBlockNs}, recording the wait
     * time. Shared by {@link #allocate} and {@link #allocateChunks}.
     *
     * @return the nanos actually waited, for the caller to deduct from its blocking budget
     * @throws KafkaException           if the pool was closed during the wait
     * @throws BufferExhaustedException if the wait timed out; the buffer-exhausted metric is recorded
     *         only when {@code recordExhaustedOnTimeout} is true (the incremental extension path
     *         recovers without dropping the record, so it records the drop itself if needed)
     */
    private long awaitMemory(Condition moreMemory, long remainingTimeToBlockNs,
                             boolean recordExhaustedOnTimeout, Supplier<String> exhaustedMessage) throws InterruptedException {
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
            if (recordExhaustedOnTimeout)
                recordBufferExhausted();
            throw new BufferExhaustedException(exhaustedMessage.get());
        }

        return timeNs;
    }

    /**
     * Allocate {@code ceil(totalSize / poolableSize)} poolable-sized buffers atomically, mirroring
     * {@link #allocate}: satisfied immediately if memory is available, else blocks up to
     * {@code maxTimeToBlockMs} for the whole request (FIFO on {@link #waiters}). The reservation is
     * tracked as bytes against {@link #nonPooledAvailableMemory} plus chunks polled from {@link #free}.
     * Any failure refunds the whole reservation and signals the next waiter before the exception
     * propagates, so a failed request leaves nothing reserved.
     * <p>
     * Used by the incremental buffer.memory allocation strategy; the poolable size is the chunk size.
     *
     * @param totalSize        minimum total bytes of capacity required across the returned chunks
     * @param maxTimeToBlockMs maximum time in milliseconds to block waiting for memory
     * @return list of {@code ceil(totalSize / poolableSize())} {@code ByteBuffer}s, each of capacity
     *         {@code poolableSize()}
     * @throws InterruptedException     if interrupted while waiting
     * @throws IllegalArgumentException if {@code totalSize <= 0}, or if the request rounded up to
     *         whole chunks exceeds {@code totalMemory()}
     * @throws BufferExhaustedException if the request can't be satisfied within {@code maxTimeToBlockMs}
     * @throws KafkaException           if the pool is closed during the wait
     */
    public List<ByteBuffer> allocateChunks(int totalSize, long maxTimeToBlockMs) throws InterruptedException {
        if (allocationMode != AllocationMode.INCREMENTAL)
            throw new IllegalStateException("allocateChunks() is not supported in " + allocationMode
                + " allocation mode; use allocate()");
        if (totalSize <= 0)
            throw new IllegalArgumentException("totalSize must be positive: " + totalSize);

        int chunkSize = poolableSize();
        int numChunks = (int) (((long) totalSize + chunkSize - 1L) / chunkSize);
        long memoryRequired = (long) numChunks * chunkSize;
        throwIfChunksNeededExceedsPool(totalSize, numChunks, chunkSize, memoryRequired);

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
                    // remainingBytes > 0 means the free list was fully drained into `pooled`, so the
                    // remainder comes entirely from non-pooled memory (sufficient per the check above).
                    this.nonPooledAvailableMemory -= remainingBytes;
                }
            } else {
                // Not enough memory available, so we wait to acquire the memory needed for all the chunks.
                // Same as allocate, but for the whole multi-chunk request. A single Condition is added to
                // the waiter's list to ensure FIFO fairness at the request level.
                //
                // `nonPoolAccumulated` tracks bytes drawn from nonPooledAvailableMemory only (pool chunks
                // already taken live in `pooled`), and is always a whole-chunk multiple. If the wait
                // does not complete (timeout / close / interrupt), the finally refunds the whole
                // reservation: `nonPoolAccumulated` back to non-pooled memory, `pooled` back to the free chunks list.
                long nonPoolAccumulated = 0;
                boolean allocationCompleted = false;
                Condition moreMemory = lock.newCondition();
                try {
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    waiters.addLast(moreMemory);
                    while ((long) pooled.size() * chunkSize + nonPoolAccumulated < memoryRequired) {
                        // Not recording the buffer-exhausted metric on timeout (recordExhaustedOnTimeout=false):
                        // this may be the extension path, which recovers without dropping the record, so the
                        // caller records the drop if needed.
                        remainingTimeToBlockNs -= awaitMemory(moreMemory, remainingTimeToBlockNs, false,
                            () -> "Failed to allocate " + memoryRequired + " bytes (" + numChunks + " chunks of "
                                + chunkSize + ") within the configured max blocking time " + maxTimeToBlockMs
                                + " ms. Total memory: " + totalMemory() + " bytes. Available memory: "
                                + availableMemory() + " bytes.");

                        // Reuse free-list chunks first, preferring them over raw reservations: if a
                        // taken chunk covers a slot already reserved as raw bytes in an earlier
                        // iteration, hand that raw reservation back to the pool.
                        while (pooled.size() < numChunks && !free.isEmpty()) {
                            pooled.add(free.pollFirst());
                            if (nonPoolAccumulated >= chunkSize) { // nonPoolAccumulated is always chunk-aligned
                                nonPoolAccumulated -= chunkSize;
                                this.nonPooledAvailableMemory += chunkSize;
                            }
                        }
                        // Reserve non-pooled memory for the still-uncovered chunks, in whole chunks
                        // (the buffers themselves are allocated after the lock is released).
                        while (pooled.size() + (int) (nonPoolAccumulated / chunkSize) < numChunks
                                && this.nonPooledAvailableMemory >= chunkSize) {
                            this.nonPooledAvailableMemory -= chunkSize;
                            nonPoolAccumulated += chunkSize;
                        }
                    }
                    allocationCompleted = true;
                } finally {
                    if (!allocationCompleted) {
                        // Refund all that was reserved (pooled chunks and non-pooled bytes)
                        this.nonPooledAvailableMemory += nonPoolAccumulated;
                        for (ByteBuffer chunk : pooled)
                            free.addFirst(chunk);
                        pooled.clear();
                    }
                    waiters.remove(moreMemory);
                }
            }
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
                // safeAllocateByteBuffer's behaviour (the bytes return, the ByteBuffer
                // instances become garbage).
                releaseReservedBytes(memoryRequired);
            }
        }
    }

    /**
     * Throw if the request memory rounded up to whole chunks would exceed the pool.
     */
    private void throwIfChunksNeededExceedsPool(int totalSize, int numChunks, int chunkSize, long memoryRequired) {
        if (memoryRequired > totalMemory())
            throw new IllegalArgumentException("Attempt to allocate " + totalSize + " bytes ("
                + numChunks + " chunks of " + chunkSize + " = " + memoryRequired + " bytes), but the "
                + "hard limit on memory allocations is " + totalMemory() + ".");
    }

    // Protected for testing
    protected void recordWaitTime(long timeNs) {
        this.waitTime.record(timeNs, time.milliseconds());
    }

    /**
     * Record that a record send was dropped because the buffer pool was exhausted. Shared by the
     * full strategy (allocate) and the incremental strategy ({@link ChunkedRecordAccumulator}),
     * so both update the same buffer-exhausted metrics.
     */
    void recordBufferExhausted() {
        this.metrics.sensor("buffer-exhausted-records").record();
    }

    /**
     * Wake the longest-waiting thread if any memory (pooled or non-pooled) is available.
     * Must be called with {@link #lock} held. No-op if no waiters or no memory is free.
     */
    private void signalNextWaiterIfMemoryAvailable() {
        if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty())
            this.waiters.peekFirst().signal();
    }

    /**
     * Allocate a buffer.  If buffer allocation fails (e.g. because of OOM) then return the size count back to
     * available memory and signal the next waiter if it exists.
     */
    private ByteBuffer safeAllocateByteBuffer(int size) {
        boolean error = true;
        try {
            ByteBuffer buffer = allocateByteBuffer(size);
            error = false;
            return buffer;
        } finally {
            if (error)
                releaseReservedBytes(size);
        }
    }

    /**
     * Return previously-reserved non-pooled bytes to the pool and signal the next
     * waiter. Acquires {@link #lock} internally. Used by callers
     * that reserve memory and then need to roll back the reservation (e.g., upon errors).
     */
    private void releaseReservedBytes(long bytes) {
        this.lock.lock();
        try {
            this.nonPooledAvailableMemory += bytes;
            if (!this.waiters.isEmpty())
                this.waiters.peekFirst().signal();
        } finally {
            this.lock.unlock();
        }
    }

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed). Must be called with {@link #lock} held.
     */
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size)
            this.nonPooledAvailableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     *
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this may be smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            if (size == this.poolableSize && size == buffer.capacity()) {
                buffer.clear();
                this.free.add(buffer);
            } else {
                this.nonPooledAvailableMemory += size;
            }
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        if (buffer != null)
            deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory + freeSize() * (long) this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }

    /**
     * Closes the buffer pool. Memory will be prevented from being allocated, but may be deallocated. All allocations
     * awaiting available memory will be notified to abort.
     */
    public void close() {
        this.lock.lock();
        this.closed = true;
        try {
            for (Condition waiter : this.waiters)
                waiter.signal();
        } finally {
            this.lock.unlock();
        }
    }
}
