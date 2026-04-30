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
package org.apache.kafka.streams.internals;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData;
import org.apache.kafka.clients.consumer.internals.StreamsRebalanceListener;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

public abstract class ConsumerWrapper implements Consumer<byte[], byte[]> {
    protected AsyncKafkaConsumer<byte[], byte[]> delegate;

    public void wrapConsumer(
        final AsyncKafkaConsumer<byte[], byte[]> delegate,
        final Map<String, Object> config,
        final Optional<StreamsRebalanceData> streamsRebalanceData
    ) {
        this.delegate = delegate;
    }

    @Override
    public Set<TopicPartition> assignment() {
        return delegate.assignment();
    }

    @Override
    public Set<String> subscription() {
        return delegate.subscription();
    }

    @Override
    public void subscribe(final Collection<String> topics) {
        delegate.subscribe(topics);
    }

    @Override
    public void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {
        delegate.subscribe(topics, callback);
    }

    public void subscribe(final Collection<String> topics, final StreamsRebalanceListener streamsRebalanceListener) {
        delegate.subscribe(topics, streamsRebalanceListener);
    }

    @Override
    public void assign(final Collection<TopicPartition> partitions) {
        delegate.assign(partitions);
    }

    @Override
    public void subscribe(final Pattern pattern, final ConsumerRebalanceListener callback) {
        delegate.subscribe(pattern, callback);
    }

    @Override
    public void subscribe(final Pattern pattern) {
        delegate.subscribe(pattern);
    }

    @Override
    public void subscribe(final SubscriptionPattern pattern, final ConsumerRebalanceListener callback) {
        delegate.subscribe(pattern, callback);
    }

    @Override
    public void subscribe(final SubscriptionPattern pattern) {
        delegate.subscribe(pattern);
    }

    @Override
    public void unsubscribe() {
        delegate.unsubscribe();
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
        return delegate.poll(timeout);
    }

    @Override
    public void commitSync() {
        delegate.commitSync();
    }

    @Override
    public void commitSync(final Duration timeout) {
        delegate.commitSync(timeout);
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        delegate.commitSync(offsets);
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        delegate.commitSync(offsets, timeout);
    }

    @Override
    public void commitAsync() {
        delegate.commitAsync();
    }

    @Override
    public void commitAsync(final OffsetCommitCallback callback) {
        delegate.commitAsync(callback);
    }

    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        delegate.commitAsync(offsets, callback);
    }

    @Override
    public void registerMetricForSubscription(final KafkaMetric metric) {
        delegate.registerMetricForSubscription(metric);
    }

    @Override
    public void unregisterMetricFromSubscription(final KafkaMetric metric) {
        delegate.unregisterMetricFromSubscription(metric);
    }

    @Override
    public void seek(final TopicPartition partition, final long offset) {
        delegate.seek(partition, offset);
    }

    @Override
    public void seek(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {
        delegate.seek(partition, offsetAndMetadata);
    }

    @Override
    public void seekToBeginning(final Collection<TopicPartition> partitions) {
        delegate.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(final Collection<TopicPartition> partitions) {
        delegate.seekToEnd(partitions);
    }

    @Override
    public long position(final TopicPartition partition) {
        return delegate.position(partition);
    }

    @Override
    public long position(final TopicPartition partition, final Duration timeout) {
        return delegate.position(partition, timeout);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return delegate.committed(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
        return delegate.committed(partitions, timeout);
    }

    @Override
    public Uuid clientInstanceId(final Duration timeout) {
        return delegate.clientInstanceId(timeout);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return delegate.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic) {
        return delegate.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic, final Duration timeout) {
        return delegate.partitionsFor(topic, timeout);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return delegate.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(final Duration timeout) {
        return delegate.listTopics(timeout);
    }

    @Override
    public Set<TopicPartition> paused() {
        return delegate.paused();
    }

    @Override
    public void pause(final Collection<TopicPartition> partitions) {
        delegate.pause(partitions);
    }

    @Override
    public void resume(final Collection<TopicPartition> partitions) {
        delegate.resume(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch) {
        return delegate.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch, final Duration timeout) {
        return delegate.offsetsForTimes(timestampsToSearch, timeout);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions) {
        return delegate.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions, final Duration timeout) {
        return delegate.beginningOffsets(partitions, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
        return delegate.endOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions, final Duration timeout) {
        return delegate.endOffsets(partitions, timeout);
    }

    @Override
    public OptionalLong currentLag(final TopicPartition topicPartition) {
        return delegate.currentLag(topicPartition);
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return delegate.groupMetadata();
    }

    @Override
    public void enforceRebalance() {
        delegate.enforceRebalance();
    }

    @Override
    public void enforceRebalance(final String reason) {
        delegate.enforceRebalance(reason);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Deprecated
    @Override
    public void close(final Duration timeout) {
        delegate.close(timeout);
    }

    @Override
    public void close(final CloseOptions option) {
        delegate.close(option);
    }

    @Override
    public void wakeup() {
        delegate.wakeup();
    }
}