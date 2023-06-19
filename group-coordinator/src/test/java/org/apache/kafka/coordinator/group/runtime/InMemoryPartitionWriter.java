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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An in-memory partition writer.
 *
 * @param <T> The record type.
 */
public class InMemoryPartitionWriter<T> implements PartitionWriter<T> {

    private class PartitionState {
        private ReentrantLock lock = new ReentrantLock();
        private List<Listener> listeners = new ArrayList<>();
        private List<T> records = new ArrayList<>();
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
        List<T> records
    ) throws KafkaException {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            state.records.addAll(records);
            state.endOffset += records.size();
            if (autoCommit) commit(tp, state.endOffset);
            return state.endOffset;
        } finally {
            state.lock.unlock();
        }
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

    public List<T> records(
        TopicPartition tp
    ) {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            return Collections.unmodifiableList(state.records);
        } finally {
            state.lock.unlock();
        }
    }
}
