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

import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.errors.BufferAllocationException;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.raft.errors.UnexpectedBaseOffsetException;
import org.apache.kafka.server.common.serialization.RecordSerde;

import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Function;
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
     * Append to the accumulator.
     *
     * @param epoch                             The leader epoch to append at.
     * @param records                           The records to append.
     * @param requiredBaseOffset                If this is non-empty, the base offset which we must use.
     * @param isAtomic                          True if we should append the records as a single batch.
     * @return                                  The end offset.
     *
     * @throws NotLeaderException               Indicates that an append operation cannot be completed
     *                                          because the provided leader epoch was too old.
     * @throws IllegalArgumentException         Indicates that an append operation cannot be completed
     *                                          because the provided leader epoch was too new.
     * @throws UnexpectedBaseOffsetException    Indicates that an append operation cannot
     *                                          be completed because it would have resulted
     *                                          in an unexpected base offset.
     */
    public long append(
        int epoch,
        List<T> records,
        OptionalLong requiredBaseOffset,
        boolean isAtomic
    ) {
        if (epoch < this.epoch) {
            throw new NotLeaderException("Append failed because the given epoch " + epoch + " is stale. " +
                    "Current leader epoch = " + this.epoch());
        } else if (epoch > this.epoch) {
            throw new IllegalArgumentException("Attempt to append from epoch " + epoch +
                " which is larger than the current epoch " + this.epoch);
        }

        ObjectSerializationCache serializationCache = new ObjectSerializationCache();

        appendLock.lock();
        try {
            long lastOffset = nextOffset + records.size() - 1;
            requiredBaseOffset.ifPresent(r -> {
                if (r != nextOffset) {
                    throw new UnexpectedBaseOffsetException("Wanted base offset " + r +
                            ", but the next offset was " + nextOffset);
                }
            });
            maybeCompleteDrain();

            BatchBuilder<T> batch = null;
            if (isAtomic) {
                batch = maybeAllocateBatch(records, serializationCache);
            }

            for (T record : records) {
                if (!isAtomic) {
                    batch = maybeAllocateBatch(Collections.singleton(record), serializationCache);
                }

                if (batch == null) {
                    throw new BufferAllocationException("Append failed because we failed to allocate memory to write the batch");
                }

                batch.appendRecord(record, serializationCache);
            }

            maybeResetLinger();

            nextOffset = lastOffset + 1;
            return lastOffset;
        } finally {
            appendLock.unlock();
        }
    }

    private void maybeResetLinger() {
        if (!lingerTimer.isRunning()) {
            lingerTimer.reset(time.milliseconds() + lingerMs);
        }
    }

    private BatchBuilder<T> maybeAllocateBatch(
        Collection<T> records,
        ObjectSerializationCache serializationCache
    ) {
        if (currentBatch == null) {
            startNewBatch();
        }

        if (currentBatch != null) {
            OptionalInt bytesNeeded = currentBatch.bytesNeeded(records, serializationCache);
            if (bytesNeeded.isPresent() && bytesNeeded.getAsInt() > maxBatchSize) {
                throw new RecordBatchTooLargeException(
                    String.format(
                        "The total record(s) size of %d exceeds the maximum allowed batch size of %d",
                        bytesNeeded.getAsInt(),
                        maxBatchSize
                    )
                );
            } else if (bytesNeeded.isPresent()) {
                completeCurrentBatch();
                startNewBatch();
            }
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

    /**
     * Append a control batch from a supplied memory record.
     *
     * See the {@code valueCreator} parameter description for requirements on this function.
     *
     * @param valueCreator a function that uses the passed buffer to create the control
     *        batch that will be appended. The memory records returned must contain one
     *        control batch and that control batch have one record.
     */
    private void appendControlMessage(Function<ByteBuffer, MemoryRecords> valueCreator) {
        appendLock.lock();
        try {
            ByteBuffer buffer = memoryPool.tryAllocate(256);
            if (buffer != null) {
                try {
                    forceDrain();
                    completed.add(
                        new CompletedBatch<>(
                            nextOffset,
                            1,
                            valueCreator.apply(buffer),
                            memoryPool,
                            buffer
                        )
                    );
                    nextOffset += 1;
                } catch (Exception e) {
                    // Release the buffer now since the buffer was not stored in completed for a delayed release
                    memoryPool.release(buffer);
                    throw e;
                }
            } else {
                throw new IllegalStateException("Could not allocate buffer for the control record");
            }
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Append a {@link LeaderChangeMessage} record to the batch
     *
     * @param leaderChangeMessage The message to append
     * @param currentTimestamp The current time in milliseconds
     * @throws IllegalStateException on failure to allocate a buffer for the record
     */
    public void appendLeaderChangeMessage(
        LeaderChangeMessage leaderChangeMessage,
        long currentTimestamp
    ) {
        appendControlMessage(buffer -> MemoryRecords.withLeaderChangeMessage(
            this.nextOffset,
            currentTimestamp,
            this.epoch,
            buffer,
            leaderChangeMessage
        ));
    }


    /**
     * Append a {@link SnapshotHeaderRecord} record to the batch
     *
     * @param snapshotHeaderRecord The record to append
     * @param currentTimestamp The current time in milliseconds
     * @throws IllegalStateException on failure to allocate a buffer for the record
     */
    public void appendSnapshotHeaderRecord(
        SnapshotHeaderRecord snapshotHeaderRecord,
        long currentTimestamp
    ) {
        appendControlMessage(buffer -> MemoryRecords.withSnapshotHeaderRecord(
            this.nextOffset,
            currentTimestamp,
            this.epoch,
            buffer,
            snapshotHeaderRecord
        ));
    }

    /**
     * Append a {@link SnapshotFooterRecord} record to the batch
     *
     * @param snapshotFooterRecord The record to append
     * @param currentTimestamp The current time in milliseconds
     * @throws IllegalStateException on failure to allocate a buffer for the record
     */
    public void appendSnapshotFooterRecord(
        SnapshotFooterRecord snapshotFooterRecord,
        long currentTimestamp
    ) {
        appendControlMessage(buffer -> MemoryRecords.withSnapshotFooterRecord(
            this.nextOffset,
            currentTimestamp,
            this.epoch,
            buffer,
            snapshotFooterRecord
        ));
    }

    public void forceDrain() {
        appendLock.lock();
        try {
            drainStatus = DrainStatus.STARTED;
            maybeCompleteDrain();
        } finally {
            appendLock.unlock();
        }
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
                epoch,
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
     * call to {@link #append(int, List, OptionalLong, boolean)} following the
     * initial call to this method.
     *
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
        public final int numRecords;
        public final Optional<List<T>> records;
        public final MemoryRecords data;
        private final MemoryPool pool;
        // Buffer that was allocated by the MemoryPool (pool). This may not be the buffer used in
        // the MemoryRecords (data) object.
        private final ByteBuffer initialBuffer;

        private CompletedBatch(
            long baseOffset,
            List<T> records,
            MemoryRecords data,
            MemoryPool pool,
            ByteBuffer initialBuffer
        ) {
            Objects.requireNonNull(data.firstBatch(), "Expected memory records to contain one batch");

            this.baseOffset = baseOffset;
            this.records = Optional.of(records);
            this.numRecords = records.size();
            this.data = data;
            this.pool = pool;
            this.initialBuffer = initialBuffer;
        }

        private CompletedBatch(
            long baseOffset,
            int numRecords,
            MemoryRecords data,
            MemoryPool pool,
            ByteBuffer initialBuffer
        ) {
            Objects.requireNonNull(data.firstBatch(), "Expected memory records to contain one batch");

            this.baseOffset = baseOffset;
            this.records = Optional.empty();
            this.numRecords = numRecords;
            this.data = data;
            this.pool = pool;
            this.initialBuffer = initialBuffer;
        }

        public int sizeInBytes() {
            return data.sizeInBytes();
        }

        public void release() {
            pool.release(initialBuffer);
        }

        public long appendTimestamp() {
            // 1. firstBatch is not null because data has one and only one batch
            // 2. maxTimestamp is the append time of the batch. This needs to be changed
            //    to return the LastContainedLogTimestamp of the SnapshotHeaderRecord
            return data.firstBatch().maxTimestamp();
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
