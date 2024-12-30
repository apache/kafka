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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.VerificationGuard;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.StreamSupport;

/**
 * An in-memory partition writer.
 */
public class InMemoryPartitionWriter implements PartitionWriter {

    private static class PartitionState {
        private final ReentrantLock lock = new ReentrantLock();
        private final List<Listener> listeners = new ArrayList<>();
        private final List<MemoryRecords> entries = new ArrayList<>();
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
    public LogConfig config(TopicPartition tp) {
        return new LogConfig(Collections.emptyMap());
    }

    @Override
    public long append(
        TopicPartition tp,
        VerificationGuard verificationGuard,
        MemoryRecords batch
    ) {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            // Copy the memory records because the coordinator runtime reuse the buffer.
            ByteBuffer buffer = ByteBuffer.allocate(batch.sizeInBytes());
            batch.firstBatch().writeTo(buffer);
            buffer.flip();
            state.entries.add(MemoryRecords.readableRecords(buffer));
            // Increment the end offset.
            state.endOffset += StreamSupport.stream(batch.records().spliterator(), false).count();
            if (autoCommit) commit(tp, state.endOffset);
            return state.endOffset;
        } finally {
            state.lock.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> deleteRecords(
        TopicPartition tp,
        long deleteBeforeOffset
    ) throws KafkaException {
        throw new RuntimeException("method not implemented");
    }

    @Override
    public CompletableFuture<VerificationGuard> maybeStartTransactionVerification(
        TopicPartition tp,
        String transactionalId,
        long producerId,
        short producerEpoch,
        short apiVersion
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
            state.listeners.forEach(listener ->
                listener.onHighWatermarkUpdated(tp, state.committedOffset));
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
            state.listeners.forEach(listener ->
                listener.onHighWatermarkUpdated(tp, state.committedOffset));
        } finally {
            state.lock.unlock();
        }
    }

    public List<MemoryRecords> entries(
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
