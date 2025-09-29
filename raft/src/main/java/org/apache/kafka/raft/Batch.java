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
package org.apache.kafka.raft;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A batch of records.
 *
 * This type contains a list of records `T` along with the information associated with those records.
 */
public final class Batch<T> implements Iterable<T> {
    private final long baseOffset;
    private final int epoch;
    private final long appendTimestamp;
    private final int sizeInBytes;
    private final long lastOffset;
    private final List<T> records;
    private final List<ControlRecord> controlRecords;

    private Batch(
        long baseOffset,
        int epoch,
        long appendTimestamp,
        int sizeInBytes,
        long lastOffset,
        List<T> records,
        List<ControlRecord> controlRecords
    ) {
        this.baseOffset = baseOffset;
        this.epoch = epoch;
        this.appendTimestamp = appendTimestamp;
        this.sizeInBytes = sizeInBytes;
        this.lastOffset = lastOffset;
        this.records = records;
        this.controlRecords = controlRecords;
    }

    /**
     * The offset of the last record in the batch.
     */
    public long lastOffset() {
        return lastOffset;
    }

    /**
     * The offset of the first record in the batch.
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * The append timestamp in milliseconds of the batch.
     */
    public long appendTimestamp() {
        return appendTimestamp;
    }

    /**
     * The list of records in the batch.
     */
    public List<T> records() {
        return records;
    }

    /**
     * The list of control records in the batch.
     */
    public List<ControlRecord> controlRecords() {
        return controlRecords;
    }

    /**
     * The epoch of the leader that appended the record batch.
     */
    public int epoch() {
        return epoch;
    }

    /**
     * The number of bytes used by this batch.
     */
    public int sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public Iterator<T> iterator() {
        return records.iterator();
    }

    @Override
    public String toString() {
        return "Batch(" +
            "baseOffset=" + baseOffset +
            ", epoch=" + epoch +
            ", appendTimestamp=" + appendTimestamp +
            ", sizeInBytes=" + sizeInBytes +
            ", lastOffset=" + lastOffset +
            ", records=" + records +
            ", controlRecords=" + controlRecords +
            ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Batch<?> batch = (Batch<?>) o;
        return baseOffset == batch.baseOffset &&
            epoch == batch.epoch &&
            appendTimestamp == batch.appendTimestamp &&
            sizeInBytes == batch.sizeInBytes &&
            lastOffset == batch.lastOffset &&
            Objects.equals(records, batch.records) &&
            Objects.equals(controlRecords, batch.controlRecords);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            baseOffset,
            epoch,
            appendTimestamp,
            sizeInBytes,
            lastOffset,
            records,
            controlRecords
        );
    }

    /**
     * Create a control batch without any data records.
     *
     * Internally this is used to propagate offset information for control batches which do not decode to the type T.
     *
     * @param baseOffset offset of the batch
     * @param epoch epoch of the leader that created this batch
     * @param appendTimestamp timestamp in milliseconds of when the batch was appended
     * @param sizeInBytes number of bytes used by this batch
     * @param records the list of records in this batch
     */
    public static <T> Batch<T> control(
        long baseOffset,
        int epoch,
        long appendTimestamp,
        int sizeInBytes,
        List<ControlRecord> records
    ) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException(
                String.format(
                    "Control batch must contain at least one record; baseOffset = %d; epoch = %d",
                    baseOffset,
                    epoch
                )
            );
        }

        return new Batch<>(
            baseOffset,
            epoch,
            appendTimestamp,
            sizeInBytes,
            baseOffset + records.size() - 1,
            Collections.emptyList(),
            records
        );
    }

    /**
     * Create a data batch with the given base offset, epoch and records.
     *
     * @param baseOffset offset of the first record in the batch
     * @param epoch epoch of the leader that created this batch
     * @param appendTimestamp timestamp in milliseconds of when the batch was appended
     * @param sizeInBytes number of bytes used by this batch
     * @param records the list of records in this batch
     */
    public static <T> Batch<T> data(
        long baseOffset,
        int epoch,
        long appendTimestamp,
        int sizeInBytes,
        List<T> records
    ) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException(
                String.format(
                    "Batch must contain at least one record; baseOffset = %d; epoch = %d",
                    baseOffset,
                    epoch
                )
            );
        }

        return new Batch<>(
            baseOffset,
            epoch,
            appendTimestamp,
            sizeInBytes,
            baseOffset + records.size() - 1,
            records,
            Collections.emptyList()
        );
    }
}
