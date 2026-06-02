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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RebalanceConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * A restricted view of a {@link Consumer} that delegates permitted operations and enforces
 * lifecycle constraints. Instances are created by {@link ConsumerAwareRebalanceListener} for
 * each callback invocation and closed automatically when the callback returns.
 */
class DelegatingRebalanceConsumer implements RebalanceConsumer, AutoCloseable {
    private final Consumer<?, ?> delegate;
    private boolean isClosed = false;

    DelegatingRebalanceConsumer(Consumer<?, ?> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void commitSync() {
        ensureOpen();
        delegate.commitSync();
    }

    @Override
    public void commitSync(Duration timeout) {
        ensureOpen();
        delegate.commitSync(timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        ensureOpen();
        delegate.commitSync(offsets);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        ensureOpen();
        delegate.commitSync(offsets, timeout);
    }

    @Override
    public void commitAsync() {
        ensureOpen();
        delegate.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        ensureOpen();
        delegate.commitAsync(callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        ensureOpen();
        delegate.commitAsync(offsets, callback);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        ensureOpen();
        return delegate.committed(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        ensureOpen();
        return delegate.committed(partitions, timeout);
    }

    @Override
    public long position(TopicPartition partition) {
        ensureOpen();
        return delegate.position(partition);
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        ensureOpen();
        return delegate.position(partition, timeout);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        ensureOpen();
        delegate.seek(partition, offset);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        ensureOpen();
        delegate.seek(partition, offsetAndMetadata);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        ensureOpen();
        delegate.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        ensureOpen();
        delegate.seekToEnd(partitions);
    }

    @Override
    public Set<TopicPartition> assignment() {
        ensureOpen();
        return delegate.assignment();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        ensureOpen();
        delegate.pause(partitions);
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        ensureOpen();
        delegate.resume(partitions);
    }

    @Override
    public Set<TopicPartition> paused() {
        ensureOpen();
        return delegate.paused();
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        ensureOpen();
        return delegate.clientInstanceId(timeout);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        ensureOpen();
        return delegate.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        ensureOpen();
        return delegate.beginningOffsets(partitions, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        ensureOpen();
        return delegate.endOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        ensureOpen();
        return delegate.endOffsets(partitions, timeout);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        ensureOpen();
        return delegate.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        ensureOpen();
        return delegate.offsetsForTimes(timestampsToSearch, timeout);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        ensureOpen();
        return delegate.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        ensureOpen();
        return delegate.partitionsFor(topic, timeout);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        ensureOpen();
        return delegate.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        ensureOpen();
        return delegate.listTopics(timeout);
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        ensureOpen();
        return delegate.currentLag(topicPartition);
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        ensureOpen();
        return delegate.groupMetadata();
    }

    @Override
    public void close() {
        isClosed = true;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException(
                    "This RebalanceConsumer is already closed. Re-use of this object is not permitted");
        }
    }
}
