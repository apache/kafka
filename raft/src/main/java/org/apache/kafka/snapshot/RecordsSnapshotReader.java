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

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.internals.RecordsIterator;
import org.apache.kafka.server.common.serialization.RecordSerde;

public final class RecordsSnapshotReader<T> implements SnapshotReader<T> {
    private final OffsetAndEpoch snapshotId;
    private final RecordsIterator<T> iterator;

    private Optional<Batch<T>> nextBatch = Optional.empty();
    private OptionalLong lastContainedLogTimestamp = OptionalLong.empty();

    private RecordsSnapshotReader(
        OffsetAndEpoch snapshotId,
        RecordsIterator<T> iterator
    ) {
        this.snapshotId = snapshotId;
        this.iterator = iterator;
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshotId;
    }

    @Override
    public long lastContainedLogOffset() {
        return snapshotId.offset() - 1;
    }

    @Override
    public int lastContainedLogEpoch() {
        return snapshotId.epoch();
    }

    @Override
    public long lastContainedLogTimestamp() {
        if (!lastContainedLogTimestamp.isPresent()) {
            nextBatch.ifPresent(batch -> {
                throw new IllegalStateException(
                    String.format(
                        "nextBatch was present when last contained log timestamp was not present: Batch(baseOffset=%d" +
                        ", epoch=%d, appendTimestamp=%d, sizeInBytes=%d, lastOffset=%d)",
                        batch.baseOffset(),
                        batch.epoch(),
                        batch.appendTimestamp(),
                        batch.sizeInBytes(),
                        batch.lastOffset()
                    )
                );
            });
            nextBatch = nextBatch();
        }

        return lastContainedLogTimestamp.getAsLong();
    }

    @Override
    public boolean hasNext() {
        if (!nextBatch.isPresent()) {
            nextBatch = nextBatch();
        }

        return nextBatch.isPresent();
    }

    @Override
    public Batch<T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Snapshot reader doesn't have any more elements");
        }

        Batch<T> batch = nextBatch.get();
        nextBatch = Optional.empty();

        return batch;
    }

    @Override
    public void close() {
        iterator.close();
    }

    public static <T> RecordsSnapshotReader<T> of(
        RawSnapshotReader snapshot,
        RecordSerde<T> serde,
        BufferSupplier bufferSupplier,
        int maxBatchSize,
        boolean doCrcValidation
    ) {
        return new RecordsSnapshotReader<>(
            snapshot.snapshotId(),
            new RecordsIterator<>(snapshot.records(), serde, bufferSupplier, maxBatchSize, doCrcValidation)
        );
    }

    /**
     * Returns the next non-control Batch
     */
    private Optional<Batch<T>> nextBatch() {
        while (iterator.hasNext()) {
            Batch<T> batch = iterator.next();

            if (!lastContainedLogTimestamp.isPresent()) {
                // This must be the first batch which is expected to be a control batch with one record for
                // the snapshot header.
                if (batch.controlRecords().isEmpty()) {
                    throw new IllegalStateException("First batch is not a control batch with at least one record");
                } else if (!ControlRecordType.SNAPSHOT_HEADER.equals(batch.controlRecords().get(0).type())) {
                    throw new IllegalStateException(
                        String.format(
                            "First control record is not a snapshot header (%s)",
                            batch.controlRecords().get(0).type()
                        )
                    );
                }

                lastContainedLogTimestamp = OptionalLong.of(
                    ((SnapshotHeaderRecord) batch.controlRecords().get(0).message()).lastContainedLogTimestamp()
                );
            }

            if (!batch.records().isEmpty()) {
                return Optional.of(batch);
            }
        }

        return Optional.empty();
    }
}
