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
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.RecordSerde;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class BatchAccumulator<T> implements Closeable {
    private final int epoch;
    private final Time time;
    private final SimpleTimer lingerTimer;
    private final int lingerMs;
    private final int maxBatchSize;
    private final CompressionType compressionType;
    private final MemoryPool memoryPool;
    private final ReentrantLock appendLock;
    private final RecordSerde<T> serde;

    private final ConcurrentLinkedQueue<CompletedBatch<T>> completed;
    private volatile DrainStatus drainStatus;

    // These fields are protected by the append lock
    private long nextOffset;
    private BatchBuilder<T> currentBatch;

    private enum DrainStatus {
        STARTED, FINISHED, NONE
    }

    public BatchAccumulator(
        int epoch,
        long baseOffset,
        int lingerMs,
        int maxBatchSize,
        MemoryPool memoryPool,
        Time time,
        CompressionType compressionType,
        RecordSerde<T> serde
    ) {
        this.epoch = epoch;
        this.lingerMs = lingerMs;
        this.maxBatchSize = maxBatchSize;
        this.memoryPool = memoryPool;
        this.time = time;
        this.lingerTimer = new SimpleTimer();
        this.compressionType = compressionType;
        this.serde = serde;
        this.nextOffset = baseOffset;
        this.drainStatus = DrainStatus.NONE;
        this.completed = new ConcurrentLinkedQueue<>();
        this.appendLock = new ReentrantLock();
    }

    /**
     * Append a list of records into an atomic batch. We guarantee all records
     * are included in the same underlying record batch so that either all of
     * the records become committed or none of them do.
     *
     * @param epoch the expected leader epoch. If this does not match, then
     *              {@link Long#MAX_VALUE} will be returned as an offset which
     *              cannot become committed.
     * @param records the list of records to include in a batch
     * @return the expected offset of the last record (which will be
     *         {@link Long#MAX_VALUE} if the epoch does not match), or null if
     *         no memory could be allocated for the batch at this time
     */
    public Long append(int epoch, List<T> records) {
        if (epoch != this.epoch) {
            // If the epoch does not match, then the state machine probably
            // has not gotten the notification about the latest epoch change.
            // In this case, ignore the append and return a large offset value
            // which will never be committed.
            return Long.MAX_VALUE;
        }

        Object serdeContext = serde.newWriteContext();
        int batchSize = 0;
        for (T record : records) {
            batchSize += serde.recordSize(record, serdeContext);
        }

        if (batchSize > maxBatchSize) {
            throw new IllegalArgumentException("The total size of " + records + " is " + batchSize +
                ", which exceeds the maximum allowed batch size of " + maxBatchSize);
        }

        appendLock.lock();
        try {
            maybeCompleteDrain();

            BatchBuilder<T> batch = maybeAllocateBatch(batchSize);
            if (batch == null) {
                return null;
            }

            // Restart the linger timer if necessary
            if (!lingerTimer.isRunning()) {
                lingerTimer.reset(time.milliseconds() + lingerMs);
            }

            for (T record : records) {
                batch.appendRecord(record, serdeContext);
                nextOffset += 1;
            }

            return nextOffset - 1;
        } finally {
            appendLock.unlock();
        }
    }

    private BatchBuilder<T> maybeAllocateBatch(int batchSize) {
        if (currentBatch == null) {
            startNewBatch();
        } else if (!currentBatch.hasRoomFor(batchSize)) {
            completeCurrentBatch();
            startNewBatch();
        }
        return currentBatch;
    }

    private void completeCurrentBatch() {
        MemoryRecords data = currentBatch.build();
        completed.add(new CompletedBatch<>(
            currentBatch.baseOffset(),
            currentBatch.records(),
            data,
            memoryPool,
            currentBatch.initialBuffer()
        ));
        currentBatch = null;
    }

    private void maybeCompleteDrain() {
        if (drainStatus == DrainStatus.STARTED) {
            if (currentBatch != null && currentBatch.nonEmpty()) {
                completeCurrentBatch();
            }
            // Reset the timer to a large value. The linger clock will begin
            // ticking after the next append.
            lingerTimer.reset(Long.MAX_VALUE);
            drainStatus = DrainStatus.FINISHED;
        }
    }

    private void startNewBatch() {
        ByteBuffer buffer = memoryPool.tryAllocate(maxBatchSize);
        if (buffer != null) {
            currentBatch = new BatchBuilder<>(
                buffer,
                serde,
                compressionType,
                nextOffset,
                time.milliseconds(),
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                maxBatchSize
            );
        }
    }

    /**
     * Check whether there are any batches which need to be drained now.
     *
     * @param currentTimeMs current time in milliseconds
     * @return true if there are batches ready to drain, false otherwise
     */
    public boolean needsDrain(long currentTimeMs) {
        return timeUntilDrain(currentTimeMs) <= 0;
    }

    /**
     * Check the time remaining until the next needed drain. If the accumulator
     * is empty, then {@link Long#MAX_VALUE} will be returned.
     *
     * @param currentTimeMs current time in milliseconds
     * @return the delay in milliseconds before the next expected drain
     */
    public long timeUntilDrain(long currentTimeMs) {
        if (drainStatus == DrainStatus.FINISHED) {
            return 0;
        } else {
            return lingerTimer.remainingMs(currentTimeMs);
        }
    }

    /**
     * Get the leader epoch, which is constant for each instance.
     *
     * @return the leader epoch
     */
    public int epoch() {
        return epoch;
    }

    /**
     * Drain completed batches. The caller is expected to first check whether
     * {@link #needsDrain(long)} returns true in order to avoid unnecessary draining.
     *
     * Note on thread-safety: this method is safe in the presence of concurrent
     * appends, but it assumes a single thread is responsible for draining.
     *
     * This call will not block, but the drain may require multiple attempts before
     * it can be completed if the thread responsible for appending is holding the
     * append lock. In the worst case, the append will be completed on the next
     * call to {@link #append(int, List)} following the initial call to this method.
     * The caller should respect the time to the next flush as indicated by
     * {@link #timeUntilDrain(long)}.
     *
     * @return the list of completed batches
     */
    public List<CompletedBatch<T>> drain() {
        // Start the drain if it has not been started already
        if (drainStatus == DrainStatus.NONE) {
            drainStatus = DrainStatus.STARTED;
        }

        // Complete the drain ourselves if we can acquire the lock
        if (appendLock.tryLock()) {
            try {
                maybeCompleteDrain();
            } finally {
                appendLock.unlock();
            }
        }

        // If the drain has finished, then all of the batches will be completed
        if (drainStatus == DrainStatus.FINISHED) {
            drainStatus = DrainStatus.NONE;
            return drainCompleted();
        } else {
            return Collections.emptyList();
        }
    }

    private List<CompletedBatch<T>> drainCompleted() {
        List<CompletedBatch<T>> res = new ArrayList<>(completed.size());
        while (true) {
            CompletedBatch<T> batch = completed.poll();
            if (batch == null) {
                return res;
            } else {
                res.add(batch);
            }
        }
    }

    public boolean isEmpty() {
        // The linger timer begins running when we have pending batches.
        // We use this to infer when the accumulator is empty to avoid the
        // need to acquire the append lock.
        return !lingerTimer.isRunning();
    }

    /**
     * Get the number of completed batches which are ready to be drained.
     * This does not include the batch that is currently being filled.
     */
    public int numCompletedBatches() {
        return completed.size();
    }

    @Override
    public void close() {
        List<CompletedBatch<T>> unwritten = drain();
        unwritten.forEach(CompletedBatch::release);
    }

    public static class CompletedBatch<T> {
        public final long baseOffset;
        public final List<T> records;
        public final MemoryRecords data;
        private final MemoryPool pool;
        private final ByteBuffer buffer;

        private CompletedBatch(
            long baseOffset,
            List<T> records,
            MemoryRecords data,
            MemoryPool pool,
            ByteBuffer buffer
        ) {
            this.baseOffset = baseOffset;
            this.records = records;
            this.data = data;
            this.pool = pool;
            this.buffer = buffer;
        }

        public int sizeInBytes() {
            return data.sizeInBytes();
        }

        public void release() {
            pool.release(buffer);
        }
    }

    private static class SimpleTimer {
        // We use an atomic long so that the Raft IO thread can query the linger
        // time without any locking
        private final AtomicLong deadlineMs = new AtomicLong(Long.MAX_VALUE);

        boolean isRunning() {
            return deadlineMs.get() != Long.MAX_VALUE;
        }

        void reset(long deadlineMs) {
            this.deadlineMs.set(deadlineMs);
        }

        long remainingMs(long currentTimeMs) {
            return Math.max(0, deadlineMs.get() - currentTimeMs);
        }
    }

}
