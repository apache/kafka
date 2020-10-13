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
package org.apache.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.AbstractTask;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.Task;

import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StateMachineTask extends AbstractTask implements Task {
    private final boolean active;
    public boolean commitNeeded = false;
    public boolean commitRequested = false;
    public boolean commitPrepared = false;
    public Map<TopicPartition, Long> purgeableOffsets;
    public Map<TopicPartition, Long> changelogOffsets = Collections.emptyMap();
    public Map<TopicPartition, OffsetAndMetadata> committableOffsets = Collections.emptyMap();

    private final Map<TopicPartition, LinkedList<ConsumerRecord<byte[], byte[]>>> queue = new HashMap<>();

    public StateMachineTask(final TaskId id,
                            final Set<TopicPartition> partitions,
                            final boolean active) {
        this(id, partitions, active, null);
    }

    public StateMachineTask(final TaskId id,
                            final Set<TopicPartition> partitions,
                            final boolean active,
                            final ProcessorStateManager processorStateManager) {
        super(id, null, null, processorStateManager, partitions, 0L);
        this.active = active;
    }

    @Override
    public void clearTaskTimeout() {}

    @Override
    public void maybeInitTaskTimeoutOrThrow(final long currentWallClockMs,
                                            final TimeoutException timeoutException) throws StreamsException {}

    @Override
    public boolean initializeIfNeeded() {
        if (state() == State.CREATED) {
            transitionTo(State.RESTORING);
            if (!active) {
                transitionTo(State.RUNNING);
            }

            return true;
        }

        return false;
    }

    @Override
    public boolean completeRestorationIfPossible() {
        if (state() == State.RESTORING) {
            transitionTo(State.RUNNING);
            return true;
        } else {
            return state() == State.RUNNING;
        }
    }

    public void setCommitNeeded() {
        commitNeeded = true;
    }

    @Override
    public boolean commitNeeded() {
        return commitNeeded;
    }

    public void setCommitRequested() {
        commitRequested = true;
    }

    @Override
    public boolean commitRequested() {
        return commitRequested;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
        if (commitNeeded) {
            commitPrepared = true;
            return committableOffsets;
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public void postCommit(final boolean enforceCheckpoint) {
        commitNeeded = false;
    }

    @Override
    public void suspend() {
        if (state() == State.CLOSED) {
            throw new IllegalStateException("Illegal state " + state() + " while suspending active task " + id);
        } else if (state() != State.SUSPENDED) {
            transitionTo(State.SUSPENDED);
        }
    }

    @Override
    public void resume() {
        if (state() == State.SUSPENDED) {
            transitionTo(State.RUNNING);
        }
    }

    @Override
    public void closeClean() {
        transitionTo(State.CLOSED);
    }

    @Override
    public void closeDirty() {
        transitionTo(State.CLOSED);
    }

    @Override
    public void closeCleanAndRecycleState() {
        transitionTo(State.CLOSED);
    }

    @Override
    public void update(final Set<TopicPartition> topicPartitions, final Map<String, List<String>> nodeToSourceTopics) {
        inputPartitions = topicPartitions;
    }

    public void setCommittableOffsetsAndMetadata(final Map<TopicPartition, OffsetAndMetadata> committableOffsets) {
        if (!active) {
            throw new IllegalStateException("Cannot set CommittableOffsetsAndMetadate for StandbyTasks");
        }
        this.committableOffsets = committableOffsets;
    }

    @Override
    public StateStore getStore(final String name) {
        return null;
    }

    @Override
    public Collection<TopicPartition> changelogPartitions() {
        return changelogOffsets.keySet();
    }

    public boolean isActive() {
        return active;
    }

    public void setPurgeableOffsets(final Map<TopicPartition, Long> purgeableOffsets) {
        this.purgeableOffsets = purgeableOffsets;
    }

    @Override
    public Map<TopicPartition, Long> purgeableOffsets() {
        return purgeableOffsets;
    }

    public void setChangelogOffsets(final Map<TopicPartition, Long> changelogOffsets) {
        this.changelogOffsets = changelogOffsets;
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        return changelogOffsets;
    }

    @Override
    public void addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        if (isActive()) {
            final Deque<ConsumerRecord<byte[], byte[]>> partitionQueue =
                    queue.computeIfAbsent(partition, k -> new LinkedList<>());

            for (final ConsumerRecord<byte[], byte[]> record : records) {
                partitionQueue.add(record);
            }
        } else {
            throw new IllegalStateException("Can't add records to an inactive task.");
        }
    }

    @Override
    public boolean process(final long wallClockTime) {
        if (isActive() && state() == State.RUNNING) {
            for (final LinkedList<ConsumerRecord<byte[], byte[]>> records : queue.values()) {
                final ConsumerRecord<byte[], byte[]> record = records.poll();
                if (record != null) {
                    return true;
                }
            }
            return false;
        } else {
            throw new IllegalStateException("Can't process an inactive or non-running task.");
        }
    }
}
