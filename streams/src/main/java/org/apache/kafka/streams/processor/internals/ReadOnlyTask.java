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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public class ReadOnlyTask implements Task {

    private final Task task;

    public ReadOnlyTask(final Task task) {
        this.task = task;
    }

    @Override
    public TaskId id() {
        return task.id();
    }

    @Override
    public boolean isActive() {
        return task.isActive();
    }

    @Override
    public Set<TopicPartition> inputPartitions() {
        return task.inputPartitions();
    }

    @Override
    public Set<TopicPartition> changelogPartitions() {
        return task.changelogPartitions();
    }

    @Override
    public State state() {
        return task.state();
    }

    @Override
    public boolean commitRequested() {
        return task.commitRequested();
    }

    @Override
    public boolean needsInitializationOrRestoration() {
        return task.needsInitializationOrRestoration();
    }

    @Override
    public void initializeIfNeeded() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void addPartitionsForOffsetReset(final Set<TopicPartition> partitionsForOffsetReset) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void completeRestoration(final Consumer<Set<TopicPartition>> offsetResetter) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void suspend() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void resume() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void closeDirty() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void closeClean() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void updateInputPartitions(final Set<TopicPartition> topicPartitions,
                                      final Map<String, List<String>> allTopologyNodesToSourceTopics) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void maybeCheckpoint(final boolean enforceCheckpoint) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void markChangelogAsCorrupted(final Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void revive() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void prepareRecycle() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void resumePollingForPartitionsWithAvailableSpace() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void updateLags() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void updateNextOffsets(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public boolean process(final long wallClockTime) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void recordProcessBatchTime(final long processBatchTime) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void recordProcessTimeRatioAndBufferSize(final long allTaskProcessMs, final long now) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public boolean maybePunctuateStreamTime() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public boolean maybePunctuateSystemTime() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void postCommit(final boolean enforceCheckpoint) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public Map<TopicPartition, Long> purgeableOffsets() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void maybeInitTaskTimeoutOrThrow(final long currentWallClockMs, final Exception cause) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void clearTaskTimeout() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public void recordRestoration(final Time time, final long numRecords, final boolean initRemaining) {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public boolean commitNeeded() {
        if (task.isActive()) {
            throw new UnsupportedOperationException("This task is read-only");
        }
        return task.commitNeeded();
    }

    @Override
    public StateStore store(final String name) {
        return task.store(name);
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        return task.changelogOffsets();
    }

    @Override
    public Map<TopicPartition, Long> committedOffsets() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public Map<TopicPartition, Long> highWaterMark() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public Optional<Long> timeCurrentIdlingStarted() {
        throw new UnsupportedOperationException("This task is read-only");
    }

    @Override
    public ProcessorStateManager stateManager() {
        throw new UnsupportedOperationException("This task is read-only");
    }
}
