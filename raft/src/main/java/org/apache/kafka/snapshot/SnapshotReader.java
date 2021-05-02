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
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RecordSerde;
import org.apache.kafka.raft.internals.RecordsIterator;

/**
 * A type for reading an immutable snapshot.
 *
 * A snapshot reader can be used to scan through all of the objects T in a snapshot. It
 * is assumed that the content of the snapshot represents all of the objects T for the topic
 * partition from offset 0 up to but not including the end offset in the snapshot id.
 */
public final class SnapshotReader<T> implements AutoCloseable, Iterator<Batch<T>> {
    private final OffsetAndEpoch snapshotId;
    private final RecordsIterator<T> iterator;

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

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Batch<T> next() {
        return iterator.next();
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
}
