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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.KafkaAsyncConsumerBackend;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class KafkaAsyncConsumerFrontend<K, V> implements Consumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaAsyncConsumerFrontend.class);

    private final Consumer<K, V> consumer;

    private final KafkaAsyncConsumerBackend backend;

    public KafkaAsyncConsumerFrontend(Consumer<K, V> consumer) {
        this.consumer = consumer;
        this.backend = new KafkaAsyncConsumerBackend();
        this.backend.start();
    }

    @Override
    public Set<TopicPartition> assignment() {
        return submit(consumer::assignment);
    }

    @Override
    public Set<String> subscription() {
        return submit(consumer::subscription);
    }

    @Override
    public void subscribe(Collection<String> topics) {
        submit(() -> consumer.subscribe(topics));
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        submit(() -> consumer.subscribe(topics, callback));
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        submit(() -> consumer.assign(partitions));
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        submit(() -> consumer.subscribe(pattern, callback));
    }

    @Override
    public void subscribe(Pattern pattern) {
        submit(() -> consumer.subscribe(pattern));
    }

    @Override
    public void unsubscribe() {
        submit(() -> consumer.unsubscribe());
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        return submit(() -> consumer.poll(timeout));
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return submit(() -> consumer.poll(timeout));
    }

    @Override
    public void commitSync() {
        submit(() -> consumer.commitSync());
    }

    @Override
    public void commitSync(Duration timeout) {
        submit(() -> consumer.commitSync(timeout));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        submit(() -> consumer.commitSync(offsets));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        submit(() -> consumer.commitSync(offsets, timeout));
    }

    @Override
    public void commitAsync() {
        submit(() -> consumer.commitAsync());
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        submit(() -> consumer.commitAsync(callback));
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        submit(() -> consumer.commitAsync(offsets, callback));
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        submit(() -> consumer.seek(partition, offset));
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        submit(() -> consumer.seek(partition, offsetAndMetadata));
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        submit(() -> consumer.seekToBeginning(partitions));
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        submit(() -> consumer.seekToEnd(partitions));
    }

    @Override
    public long position(TopicPartition partition) {
        return submit(() -> consumer.position(partition));
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return submit(() -> consumer.position(partition));
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return submit(() -> consumer.committed(partition));
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return submit(() -> consumer.committed(partition, timeout));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return submit(() -> consumer.committed(partitions));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        return submit(() -> consumer.committed(partitions, timeout));
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return submit(consumer::metrics);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return submit(() -> consumer.partitionsFor(topic));
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return submit(() -> consumer.partitionsFor(topic, timeout));
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return submit(() -> consumer.listTopics());
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return submit(() -> consumer.listTopics(timeout));
    }

    @Override
    public Set<TopicPartition> paused() {
        return submit(consumer::paused);
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        submit(() -> consumer.pause(partitions));
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        submit(() -> consumer.resume(partitions));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return submit(() -> consumer.offsetsForTimes(timestampsToSearch));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return submit(() -> consumer.offsetsForTimes(timestampsToSearch, timeout));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return submit(() -> consumer.beginningOffsets(partitions));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return submit(() -> consumer.beginningOffsets(partitions, timeout));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return submit(() -> consumer.endOffsets(partitions));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return submit(() -> consumer.endOffsets(partitions, timeout));
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        return null;
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return submit(consumer::groupMetadata);
    }

    @Override
    public void enforceRebalance() {
        submit(() -> consumer.enforceRebalance());
    }

    @Override
    public void enforceRebalance(String reason) {
        submit(() -> consumer.enforceRebalance(reason));
    }

    @Override
    public void close() {
        submit(() -> consumer.close());
        backend.close();
    }

    @Override
    public void close(Duration timeout) {
        submit(() -> consumer.close(timeout));
        backend.close();
    }

    @Override
    public void wakeup() {
        submit(consumer::wakeup);
    }

    private <T> T submit(Supplier<T> supplier) {
        Future<T> future = backend.submit(supplier);

        try {
            return future.get();
        } catch (ExecutionException e) {
            throw new RuntimeException("Handle this better");
        } catch (InterruptedException e) {
            throw new RuntimeException("Handle this better, too");
        }
    }

    private void submit(Runnable runnable) {
        backend.submit(runnable);
    }

}
