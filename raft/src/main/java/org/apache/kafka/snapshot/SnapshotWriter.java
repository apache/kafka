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

// TODO: Write documentation for this type and all of the methods
final public class SnapshotWriter<T> implements Closeable {
    final private RawSnapshotWriter snapshot;
    final private BatchAccumulator<T> accumulator;
    final private Time time;

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

    public OffsetAndEpoch snapshotId() {
        return snapshot.snapshotId();
    }

    public boolean isFrozen() {
        return snapshot.isFrozen();
    }

    public void append(List<T> records) throws IOException {
        if (snapshot.isFrozen()) {
            String message = String.format(
                "Append not supported. Snapshot is already frozen: id = {}.",
                snapshot.snapshotId()
            );

            throw new RuntimeException(message);
        }

        accumulator.append(snapshot.snapshotId().epoch, records);

        if (!accumulator.needsDrain(time.milliseconds())) {
            return;
        }

        appendBatches(accumulator.drain());
    }

    public void freeze() throws IOException {
        appendBatches(accumulator.drain());
        snapshot.freeze();
        accumulator.close();
    }

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
