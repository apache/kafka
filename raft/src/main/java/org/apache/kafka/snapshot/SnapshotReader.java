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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.raft.internals.RecordsIterator;

/**
 * A type for reading an immutable snapshot.
 *
 * A snapshot reader can be used to scan through all of the objects T in a snapshot. It
 * is assumed that the content of the snapshot represents all of the objects T for the topic
 * partition from offset 0 up to but not including the end offset in the snapshot id.
 *
 * The offsets ({@code baseOffset()} and {@code lastOffset()} stored in {@code Batch<T>}
 * objects returned by this iterator are independent of the offset of the records in the
 * log used to generate this batch.
 *
 * Use {@code lastContainedLogOffset()} and {@code lastContainedLogEpoch()} to query which
 * offsets and epoch from the log are included in this snapshot. Both of these values are
 * inclusive.
 */
public final class SnapshotReader<T> implements AutoCloseable, Iterator<Batch<T>> {
    private final OffsetAndEpoch snapshotId;
    private final RecordsIterator<T> iterator;

    private Optional<Batch<T>> nextBatch = Optional.empty();
    private OptionalLong lastContainedLogTimestamp = OptionalLong.empty();

    private SnapshotReader(
        OffsetAndEpoch snapshotId,
        RecordsIterator<T> iterator
    ) {
        this.snapshotId = snapshotId;
        this.iterator = iterator;
    }

    /**
     * Returns the end offset and epoch for the snapshot.
     */
    public OffsetAndEpoch snapshotId() {
        return snapshotId;
    }

    /**
     * Returns the last log offset which is represented in the snapshot.
     */
    public long lastContainedLogOffset() {
        return snapshotId.offset - 1;
    }

    /**
     * Returns the epoch of the last log offset which is represented in the snapshot.
     */
    public int lastContainedLogEpoch() {
        return snapshotId.epoch;
    }

    /**
     * Returns the timestamp of the last log offset which is represented in the snapshot.
     */
    public long lastContainedLogTimestamp() {
        if (!lastContainedLogTimestamp.isPresent()) {
            // nextBatch is expected to be empty
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

    /**
     * Closes the snapshot reader.
     */
    public void close() {
        iterator.close();
    }

    public static <T> SnapshotReader<T> of(
        RawSnapshotReader snapshot,
        RecordSerde<T> serde,
        BufferSupplier bufferSupplier,
        int maxBatchSize
    ) {
        return new SnapshotReader<>(
            snapshot.snapshotId(),
            new RecordsIterator<>(snapshot.records(), serde, bufferSupplier, maxBatchSize)
        );
    }

    /**
     * Returns the next non-control Batch
     */
    private Optional<Batch<T>> nextBatch() {
        while (iterator.hasNext()) {
            Batch<T> batch = iterator.next();

            if (!lastContainedLogTimestamp.isPresent()) {
                // The Batch type doesn't support returning control batches. For now lets just use
                // the append time of the first batch
                lastContainedLogTimestamp = OptionalLong.of(batch.appendTimestamp());
            }

            if (!batch.records().isEmpty()) {
                return Optional.of(batch);
            }
        }

        return Optional.empty();
    }
}
