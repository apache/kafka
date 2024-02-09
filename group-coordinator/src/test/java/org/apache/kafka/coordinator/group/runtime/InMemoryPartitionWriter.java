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
package org.apache.kafka.coordinator.group.runtime;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.storage.internals.log.VerificationGuard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * An in-memory partition writer.
 *
 * @param <T> The record type.
 */
public class InMemoryPartitionWriter<T> implements PartitionWriter<T> {

    public static class LogEntry {
        public static <T> LogEntry value(T value) {
            return new LogValue<>(value);
        }

        public static <T> LogEntry value(
            long producerId,
            short producerEpoch,
            T value
        ) {
            return new LogValue<>(
                producerId,
                producerEpoch,
                value
            );
        }

        public static LogEntry control(
            long producerId,
            short producerEpoch,
            int coordinatorEpoch,
            TransactionResult result
        ) {
            return new LogControl(
                producerId,
                producerEpoch,
                coordinatorEpoch,
                result
            );
        }
    }

    public static class LogValue<T> extends LogEntry {
        public final long producerId;
        public final short producerEpoch;
        public final T value;

        private LogValue(
            long producerId,
            short producerEpoch,
            T value
        ) {
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.value = value;
        }

        private LogValue(T value) {
            this(
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                value
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LogValue<?> that = (LogValue<?>) o;

            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "BasicRecord(" +
                "producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", value=" + value +
                ')';
        }
    }

    public static class LogControl extends LogEntry {
        public final long producerId;
        public final short producerEpoch;
        public final int coordinatorEpoch;
        public final TransactionResult result;

        private LogControl(
            long producerId,
            short producerEpoch,
            int coordinatorEpoch,
            TransactionResult result
        ) {
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.coordinatorEpoch = coordinatorEpoch;
            this.result = result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LogControl that = (LogControl) o;

            if (producerId != that.producerId) return false;
            if (producerEpoch != that.producerEpoch) return false;
            if (coordinatorEpoch != that.coordinatorEpoch) return false;
            return result == that.result;
        }

        @Override
        public int hashCode() {
            int result1 = (int) (producerId ^ (producerId >>> 32));
            result1 = 31 * result1 + (int) producerEpoch;
            result1 = 31 * result1 + coordinatorEpoch;
            result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
            return result1;
        }

        @Override
        public String toString() {
            return "ControlRecord(" +
                "producerId=" + producerId +
                ", producerEpoch=" + producerEpoch +
                ", coordinatorEpoch=" + coordinatorEpoch +
                ", result=" + result +
                ')';
        }
    }

    private class PartitionState {
        private ReentrantLock lock = new ReentrantLock();
        private List<Listener> listeners = new ArrayList<>();
        private List<LogEntry> entries = new ArrayList<>();
        private long endOffset = 0L;
        private long committedOffset = 0L;
    }

    private final boolean autoCommit;
    private final Map<TopicPartition, PartitionState> partitions;

    public InMemoryPartitionWriter(boolean autoCommit) {
        this.autoCommit = autoCommit;
        this.partitions = new ConcurrentHashMap<>();
    }

    private PartitionState partitionState(
        TopicPartition tp
    ) {
        return partitions.computeIfAbsent(tp, __ -> new PartitionState());
    }

    @Override
    public void registerListener(
        TopicPartition tp,
        Listener listener
    ) {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            state.listeners.add(listener);
        } finally {
            state.lock.unlock();
        }
    }

    @Override
    public void deregisterListener(
        TopicPartition tp,
        Listener listener
    ) {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            state.listeners.remove(listener);
        } finally {
            state.lock.unlock();
        }
    }

    @Override
    public long append(
        TopicPartition tp,
        long producerId,
        short producerEpoch,
        VerificationGuard verificationGuard,
        List<T> records
    ) throws KafkaException {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            state.entries.addAll(records.stream().map(record -> new LogValue<T>(
                producerId,
                producerEpoch,
                record
            )).collect(Collectors.toList()));
            state.endOffset += records.size();
            if (autoCommit) commit(tp, state.endOffset);
            return state.endOffset;
        } finally {
            state.lock.unlock();
        }
    }

    @Override
    public long appendEndTransactionMarker(
        TopicPartition tp,
        long producerId,
        short producerEpoch,
        int coordinatorEpoch,
        TransactionResult result
    ) throws KafkaException {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            state.entries.add(new LogControl(
                producerId,
                producerEpoch,
                coordinatorEpoch,
                result
            ));
            state.endOffset += 1;
            if (autoCommit) commit(tp, state.endOffset);
            return state.endOffset;
        } finally {
            state.lock.unlock();
        }
    }

    @Override
    public CompletableFuture<VerificationGuard> maybeStartTransactionVerification(
        TopicPartition tp,
        String transactionalId,
        long producerId,
        short producerEpoch
    ) throws KafkaException {
        return CompletableFuture.completedFuture(new VerificationGuard());
    }

    public void commit(
        TopicPartition tp,
        long offset
    ) {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            state.committedOffset = offset;
            state.listeners.forEach(listener -> {
                listener.onHighWatermarkUpdated(tp, state.committedOffset);
            });
        } finally {
            state.lock.unlock();
        }
    }

    public void commit(
        TopicPartition tp
    ) {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            state.committedOffset = state.endOffset;
            state.listeners.forEach(listener -> {
                listener.onHighWatermarkUpdated(tp, state.committedOffset);
            });
        } finally {
            state.lock.unlock();
        }
    }

    public List<LogEntry> entries(
        TopicPartition tp
    ) {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            return Collections.unmodifiableList(state.entries);
        } finally {
            state.lock.unlock();
        }
    }
}
