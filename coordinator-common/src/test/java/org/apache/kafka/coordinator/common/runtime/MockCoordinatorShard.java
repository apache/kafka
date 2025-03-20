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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A simple Coordinator implementation that stores the records into a set.
 */
public class MockCoordinatorShard implements CoordinatorShard<String> {
    static class RecordAndMetadata {
        public final long offset;
        public final long producerId;
        public final short producerEpoch;
        public final String record;

        public RecordAndMetadata(
            long offset,
            String record
        ) {
            this(
                offset,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                record
            );
        }

        public RecordAndMetadata(
            long offset,
            long producerId,
            short producerEpoch,
            String record
        ) {
            this.offset = offset;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.record = record;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RecordAndMetadata that = (RecordAndMetadata) o;

            if (offset != that.offset) return false;
            if (producerId != that.producerId) return false;
            if (producerEpoch != that.producerEpoch) return false;
            return Objects.equals(record, that.record);
        }

        @Override
        public int hashCode() {
            int result = (int) (offset ^ (offset >>> 32));
            result = 31 * result + (int) (producerId ^ (producerId >>> 32));
            result = 31 * result + (int) producerEpoch;
            result = 31 * result + (record != null ? record.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "RecordAndMetadata(" +
                "offset=" + offset +
                ", producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", record='" + record.substring(0, Math.min(10, record.length())) + '\'' +
                ')';
        }
    }

    private final SnapshotRegistry snapshotRegistry;
    private final TimelineHashSet<RecordAndMetadata> records;
    private final TimelineHashMap<Long, TimelineHashSet<RecordAndMetadata>> pendingRecords;
    private final CoordinatorTimer<Void, String> timer;
    private final CoordinatorExecutor<String> executor;

    MockCoordinatorShard(
        SnapshotRegistry snapshotRegistry,
        CoordinatorTimer<Void, String> timer
    ) {
        this(snapshotRegistry, timer, null);
    }

    MockCoordinatorShard(
        SnapshotRegistry snapshotRegistry,
        CoordinatorTimer<Void, String> timer,
        CoordinatorExecutor<String> executor
    ) {
        this.snapshotRegistry = snapshotRegistry;
        this.records = new TimelineHashSet<>(snapshotRegistry, 0);
        this.pendingRecords = new TimelineHashMap<>(snapshotRegistry, 0);
        this.timer = timer;
        this.executor = executor;
    }

    @Override
    public void replay(
        long offset,
        long producerId,
        short producerEpoch,
        String record
    ) throws RuntimeException {
        RecordAndMetadata recordAndMetadata = new RecordAndMetadata(
            offset,
            producerId,
            producerEpoch,
            record
        );

        if (producerId == RecordBatch.NO_PRODUCER_ID) {
            records.add(recordAndMetadata);
        } else {
            pendingRecords
                .computeIfAbsent(producerId, __ -> new TimelineHashSet<>(snapshotRegistry, 0))
                .add(recordAndMetadata);
        }
    }

    @Override
    public void replayEndTransactionMarker(
        long producerId,
        short producerEpoch,
        TransactionResult result
    ) throws RuntimeException {
        if (result == TransactionResult.COMMIT) {
            TimelineHashSet<RecordAndMetadata> pending = pendingRecords.remove(producerId);
            if (pending == null) return;
            records.addAll(pending);
        } else {
            pendingRecords.remove(producerId);
        }
    }

    Set<String> pendingRecords(long producerId) {
        TimelineHashSet<RecordAndMetadata> pending = pendingRecords.get(producerId);
        if (pending == null) return Set.of();
        return pending.stream().map(record -> record.record).collect(Collectors.toUnmodifiableSet());
    }

    Set<String> records() {
        return records.stream().map(record -> record.record).collect(Collectors.toUnmodifiableSet());
    }

    List<RecordAndMetadata> fullRecords() {
        return records
            .stream()
            .sorted(Comparator.comparingLong(record -> record.offset))
            .collect(Collectors.toList());
    }

    CoordinatorTimer<Void, String> timer() {
        return timer;
    }

    CoordinatorExecutor<String> executor() {
        return executor;
    }
}
