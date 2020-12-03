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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RecordSerde;
import org.apache.kafka.raft.internals.BatchAccumulator.CompletedBatch;
import org.apache.kafka.raft.internals.BatchAccumulator;

/**
 * A type for writing a snapshot fora given end offset and epoch.
 *
 * A snapshot writer can be used to append objects until freeze is called. When freeze is
 * called the snapshot is validated and marked as immutable. After freeze is called any
 * append will fail with an exception.
 *
 * It is assumed that the content of the snapshot represents all of the records for the
 * topic partition from offset 0 up to but not including the end offset in the snapshot
 * id.
 *
 * @see org.apache.kafka.raft.RaftClient#createSnapshot(OffsetAndEpoch)
 */
final public class SnapshotWriter<T> implements Closeable {
    final private RawSnapshotWriter snapshot;
    final private BatchAccumulator<T> accumulator;
    final private Time time;

    /**
     * Initializes a new instance of the class.
     *
     * @param snapshot the low level snapshot writer
     * @param maxBatchSize the maximum size in byte for a batch
     * @param memoryPool the memory pool for buffer allocation
     * @param time the clock implementation
     * @param compressionType the compression algorithm to use
     * @param serde the record serialization and deserialization implementation
     */
    public SnapshotWriter(
        RawSnapshotWriter snapshot,
        int maxBatchSize,
        MemoryPool memoryPool,
        Time time,
        CompressionType compressionType,
        RecordSerde<T> serde
    ) {
        this.snapshot = snapshot;
        this.time = time;

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
     * Returns the end offset and epoch for the snapshot.
     */
    public OffsetAndEpoch snapshotId() {
        return snapshot.snapshotId();
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
     * @throws IOException for any IO error while appending
     * @throws IllegalStateException if append is called when isFrozen is true
     */
    public void append(List<T> records) throws IOException {
        if (snapshot.isFrozen()) {
            String message = String.format(
                "Append not supported. Snapshot is already frozen: id = {}.",
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
     * @throws IOException for any IO error during freezing
     */
    public void freeze() throws IOException {
        appendBatches(accumulator.drain());
        snapshot.freeze();
        accumulator.close();
    }

    /**
     * Closes the snapshot writer.
     *
     * If close is called without first calling freeze the the snapshot is aborted.
     *
     * @throws IOException for any IO error during close
     */
    public void close() throws IOException {
        snapshot.close();
        accumulator.close();
    }

    private void appendBatches(List<CompletedBatch<T>> batches) throws IOException {
        try {
            for (CompletedBatch batch : batches) {
                snapshot.append(batch.data.buffer());
            }
        } finally {
            batches.forEach(CompletedBatch::release);
        }
    }
}
