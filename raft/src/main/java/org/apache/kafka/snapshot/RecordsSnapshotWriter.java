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
import java.util.OptionalLong;
import java.util.function.Supplier;

final public class RecordsSnapshotWriter<T> implements SnapshotWriter<T> {
    final private RawSnapshotWriter snapshot;
    final private BatchAccumulator<T> accumulator;
    final private Time time;
    final private long lastContainedLogTimestamp;

    private RecordsSnapshotWriter(
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
            snapshot.snapshotId().epoch(),
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
            .setVersion(ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION)
            .setLastContainedLogTimestamp(lastContainedLogTimestamp);
        accumulator.appendSnapshotHeaderRecord(headerRecord, time.milliseconds());
        accumulator.forceDrain();
    }

    /**
     * Adds a {@link SnapshotFooterRecord} to the snapshot
     *
     * No more records should be appended to the snapshot after calling this method
     */
    private void finalizeSnapshotWithFooter() {
        SnapshotFooterRecord footerRecord = new SnapshotFooterRecord()
            .setVersion(ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION);
        accumulator.appendSnapshotFooterRecord(footerRecord, time.milliseconds());
        accumulator.forceDrain();
    }

    /**
     * Create an instance of this class and initialize
     * the underlying snapshot with {@link SnapshotHeaderRecord}
     *
     * @param supplier a lambda to create the low level snapshot writer
     * @param maxBatchSize the maximum size in byte for a batch
     * @param memoryPool the memory pool for buffer allocation
     * @param snapshotTime the clock implementation
     * @param lastContainedLogTimestamp The append time of the highest record contained in this snapshot
     * @param compressionType the compression algorithm to use
     * @param serde the record serialization and deserialization implementation
     * @return {@link Optional}{@link RecordsSnapshotWriter}
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
        return supplier.get().map(writer ->
            createWithHeader(
                writer,
                maxBatchSize,
                memoryPool,
                snapshotTime,
                lastContainedLogTimestamp,
                compressionType,
                serde
            )
        );
    }

    public static <T> RecordsSnapshotWriter<T> createWithHeader(
        RawSnapshotWriter rawSnapshotWriter,
        int maxBatchSize,
        MemoryPool memoryPool,
        Time snapshotTime,
        long lastContainedLogTimestamp,
        CompressionType compressionType,
        RecordSerde<T> serde
    ) {
        RecordsSnapshotWriter<T> writer = new RecordsSnapshotWriter<>(
            rawSnapshotWriter,
            maxBatchSize,
            memoryPool,
            snapshotTime,
            lastContainedLogTimestamp,
            compressionType,
            serde
        );
        writer.initializeSnapshotWithHeader();
        return writer;
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshot.snapshotId();
    }

    @Override
    public long lastContainedLogOffset() {
        return snapshot.snapshotId().offset() - 1;
    }

    @Override
    public int lastContainedLogEpoch() {
        return snapshot.snapshotId().epoch();
    }

    @Override
    public boolean isFrozen() {
        return snapshot.isFrozen();
    }

    @Override
    public void append(List<T> records) {
        if (snapshot.isFrozen()) {
            String message = String.format(
                "Append not supported. Snapshot is already frozen: id = '%s'.",
                snapshot.snapshotId()
            );

            throw new IllegalStateException(message);
        }

        accumulator.append(snapshot.snapshotId().epoch(), records, OptionalLong.empty(), false);

        if (accumulator.needsDrain(time.milliseconds())) {
            appendBatches(accumulator.drain());
        }
    }

    @Override
    public long freeze() {
        finalizeSnapshotWithFooter();
        appendBatches(accumulator.drain());
        snapshot.freeze();
        accumulator.close();
        return snapshot.sizeInBytes();
    }

    @Override
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
