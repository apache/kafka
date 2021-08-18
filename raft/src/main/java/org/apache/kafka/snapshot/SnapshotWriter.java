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

package org.apache.kafka.snapshot;

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.BatchAccumulator.CompletedBatch;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.record.ControlRecordUtils;

import java.util.Optional;
import java.util.List;
import java.util.function.Supplier;

/**
 * A type for writing a snapshot for a given end offset and epoch.
 *
 * A snapshot writer can be used to append objects until freeze is called. When freeze is
 * called the snapshot is validated and marked as immutable. After freeze is called any
 * append will fail with an exception.
 *
 * It is assumed that the content of the snapshot represents all of the records for the
 * topic partition from offset 0 up to but not including the end offset in the snapshot
 * id.
 *
 * @see org.apache.kafka.raft.KafkaRaftClient#createSnapshot(long, int, long)
 */
final public class SnapshotWriter<T> implements AutoCloseable {
    final private RawSnapshotWriter snapshot;
    final private BatchAccumulator<T> accumulator;
    final private Time time;
    final private long lastContainedLogTimestamp;

    private SnapshotWriter(
        RawSnapshotWriter snapshot,
        int maxBatchSize,
        MemoryPool memoryPool,
        Time time,
        long lastContainedLogTimestamp,
        CompressionType compressionType,
        RecordSerde<T> serde
    ) {
        this.snapshot = snapshot;
        this.time = time;
        this.lastContainedLogTimestamp = lastContainedLogTimestamp;

        this.accumulator = new BatchAccumulator<>(
            snapshot.snapshotId().epoch,
            0,
            Integer.MAX_VALUE,
            maxBatchSize,
            memoryPool,
            time,
            compressionType,
            serde
        );
    }

    /**
     * Adds a {@link SnapshotHeaderRecord} to snapshot
     *
     * @throws IllegalStateException if the snapshot is not empty
     */
    private void initializeSnapshotWithHeader() {
        if (snapshot.sizeInBytes() != 0) {
            String message = String.format(
                "Initializing writer with a non-empty snapshot: id = '%s'.",
                snapshot.snapshotId()
            );
            throw new IllegalStateException(message);
        }

        SnapshotHeaderRecord headerRecord = new SnapshotHeaderRecord()
            .setVersion(ControlRecordUtils.SNAPSHOT_HEADER_HIGHEST_VERSION)
            .setLastContainedLogTimestamp(lastContainedLogTimestamp);
        accumulator.appendSnapshotHeaderMessage(headerRecord, time.milliseconds());
        accumulator.forceDrain();
    }

    /**
     * Adds a {@link SnapshotFooterRecord} to the snapshot
     *
     * No more records should be appended to the snapshot after calling this method
     */
    private void finalizeSnapshotWithFooter() {
        SnapshotFooterRecord footerRecord = new SnapshotFooterRecord()
            .setVersion(ControlRecordUtils.SNAPSHOT_FOOTER_HIGHEST_VERSION);
        accumulator.appendSnapshotFooterMessage(footerRecord, time.milliseconds());
        accumulator.forceDrain();
    }

    /**
     * Create an instance of this class and initialize
     * the underlying snapshot with {@link SnapshotHeaderRecord}
     *
     * @param snapshot a lambda to create the low level snapshot writer
     * @param maxBatchSize the maximum size in byte for a batch
     * @param memoryPool the memory pool for buffer allocation
     * @param time the clock implementation
     * @param lastContainedLogTimestamp The append time of the highest record contained in this snapshot
     * @param compressionType the compression algorithm to use
     * @param serde the record serialization and deserialization implementation
     * @return {@link Optional}{@link SnapshotWriter}
     */
    public static <T> Optional<SnapshotWriter<T>> createWithHeader(
        Supplier<Optional<RawSnapshotWriter>> supplier,
        int maxBatchSize,
        MemoryPool memoryPool,
        Time snapshotTime,
        long lastContainedLogTimestamp,
        CompressionType compressionType,
        RecordSerde<T> serde
    ) {
        Optional<SnapshotWriter<T>> writer = supplier.get().map(snapshot -> {
            return new SnapshotWriter<T>(
                    snapshot,
                    maxBatchSize,
                    memoryPool,
                    snapshotTime,
                    lastContainedLogTimestamp,
                    CompressionType.NONE,
                    serde);
        });
        writer.ifPresent(SnapshotWriter::initializeSnapshotWithHeader);
        return writer;
    }

    /**
     * Returns the end offset and epoch for the snapshot.
     */
    public OffsetAndEpoch snapshotId() {
        return snapshot.snapshotId();
    }

    /**
     * Returns the last log offset which is represented in the snapshot.
     */
    public long lastContainedLogOffset() {
        return snapshot.snapshotId().offset - 1;
    }

    /**
     * Returns the epoch of the last log offset which is represented in the snapshot.
     */
    public int lastContainedLogEpoch() {
        return snapshot.snapshotId().epoch;
    }

    /**
     * Returns true if the snapshot has been frozen, otherwise false is returned.
     *
     * Modification to the snapshot are not allowed once it is frozen.
     */
    public boolean isFrozen() {
        return snapshot.isFrozen();
    }

    /**
     * Appends a list of values to the snapshot.
     *
     * The list of record passed are guaranteed to get written together.
     *
     * @param records the list of records to append to the snapshot
     * @throws IllegalStateException if append is called when isFrozen is true
     */
    public void append(List<T> records) {
        if (snapshot.isFrozen()) {
            String message = String.format(
                "Append not supported. Snapshot is already frozen: id = '%s'.",
                snapshot.snapshotId()
            );

            throw new IllegalStateException(message);
        }

        accumulator.append(snapshot.snapshotId().epoch, records);

        if (accumulator.needsDrain(time.milliseconds())) {
            appendBatches(accumulator.drain());
        }
    }

    /**
     * Freezes the snapshot by flushing all pending writes and marking it as immutable.
     *
     * Also adds a {@link SnapshotFooterRecord} to the end of the snapshot
     */
    public void freeze() {
        finalizeSnapshotWithFooter();
        appendBatches(accumulator.drain());
        snapshot.freeze();
        accumulator.close();
    }

    /**
     * Closes the snapshot writer.
     *
     * If close is called without first calling freeze the snapshot is aborted.
     */
    public void close() {
        snapshot.close();
        accumulator.close();
    }

    private void appendBatches(List<CompletedBatch<T>> batches) {
        try {
            for (CompletedBatch<T> batch : batches) {
                snapshot.append(batch.data);
            }
        } finally {
            batches.forEach(CompletedBatch::release);
        }
    }
}
