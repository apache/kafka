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
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.RecordSerde;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO: Also flush after minimum size limit is reached?
 */
public class BatchAccumulator<T> implements Closeable {
    private final int epoch;
    private final Time time;
    private final Timer lingerTimer;
    private final int lingerMs;
    private final int maxBatchSize;
    private final CompressionType compressionType;
    private final MemoryPool memoryPool;
    private final ReentrantLock lock;
    private final RecordSerde<T> serde;

    private long nextOffset;
    private BatchBuilder<T> currentBatch;
    private List<CompletedBatch<T>> completed;

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
        this.lingerTimer = time.timer(lingerMs);
        this.compressionType = compressionType;
        this.serde = serde;
        this.nextOffset = baseOffset;
        this.completed = new ArrayList<>();
        this.lock = new ReentrantLock();
    }

    /**
     * Append a list of records into an atomic batch. We guarantee all records
     * are included in the same underlying record batch so that either all of
     * the records become committed or none of them do.
     *
     * @param epoch the expected leader epoch
     * @param records the list of records to include in a batch
     * @return the offset of the last message or {@link Long#MAX_VALUE} if the epoch
     *         does not match
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

        lock.lock();
        try {
            BatchBuilder<T> batch = maybeAllocateBatch(batchSize);
            if (batch == null) {
                return null;
            }

            if (isEmpty()) {
                lingerTimer.update();
                lingerTimer.reset(lingerMs);
            }

            for (T record : records) {
                batch.appendRecord(record, serdeContext);
                nextOffset += 1;
            }

            return nextOffset - 1;
        } finally {
            lock.unlock();
        }
    }

    private BatchBuilder<T> maybeAllocateBatch(int batchSize) {
        if (currentBatch == null) {
            startNewBatch();
        } else if (!currentBatch.hasRoomFor(batchSize)) {
            completeCurrentBatch();
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
        startNewBatch();
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
     * Check whether there are any batches which need flushing now.
     *
     * @param currentTimeMs current time in milliseconds
     * @return true if there are batches ready to flush, false otherwise
     */
    public boolean needsFlush(long currentTimeMs) {
        return timeUntilFlush(currentTimeMs) <= 0;
    }

    /**
     * Check the time remaining until the next needed flush. If the accumulator
     * is empty, then {@link Long#MAX_VALUE} will be returned.
     *
     * @param currentTimeMs current time in milliseconds
     * @return the delay in milliseconds before the next expected flush
     */
    public long timeUntilFlush(long currentTimeMs) {
        lock.lock();
        try {
            lingerTimer.update(currentTimeMs);
            if (isEmpty()) {
                return Long.MAX_VALUE;
            } else {
                return lingerTimer.remainingMs();
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean isEmpty() {
        lock.lock();
        try {
            if (currentBatch != null && currentBatch.nonEmpty()) {
                return false;
            } else {
                return completed.isEmpty();
            }
        } finally {
            lock.unlock();
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
     * Flush completed batches. The caller is expected to first check whether
     * a flush is expected using {@link #needsFlush(long)} in order to avoid
     * unnecessary flushing.
     *
     * @return the list of completed batches
     */
    public List<CompletedBatch<T>> flush() {
        lock.lock();
        try {
            if (currentBatch != null && currentBatch.nonEmpty()) {
                completeCurrentBatch();
            }

            List<CompletedBatch<T>> res = completed;
            this.completed = new ArrayList<>();
            return res;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the number of batches including the one that is currently being
     * written to (if it exists).
     *
     * @return
     */
    public int count() {
        lock.lock();
        try {
            int count = completed.size();
            if (currentBatch != null) {
                return count + 1;
            } else {
                return count;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        List<CompletedBatch<T>> unwritten = flush();
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

}
