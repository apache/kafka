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

import org.apache.kafka.clients.consumer.internals.AsyncConsumerForegroundState;
import org.apache.kafka.clients.consumer.internals.Fetch;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.SerializedRecordWrapper;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.consumer.internals.events.PollFetchEvent;
import org.apache.kafka.clients.consumer.internals.events.SubscribePatternEvent;
import org.apache.kafka.clients.consumer.internals.events.SubscribeTopicsEvent;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class StubbedAsyncKafkaConsumer<K, V> implements Consumer<K, V> {

    private Time time;

    private EventHandler eventHandler;

    private AsyncConsumerForegroundState foregroundState;

    private Deserializer<K> keyDeserializer;

    private Deserializer<V> valueDeserializer;

    @Override
    public Set<TopicPartition> assignment() {
        return null;
    }

    @Override
    public Set<String> subscription() {
        return Collections.unmodifiableSet(foregroundState.subscription());
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        foregroundState.subscribe(topics);
        eventHandler.add(new SubscribeTopicsEvent(topics, callback));
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        foregroundState.subscribe(pattern);
        eventHandler.add(new SubscribePatternEvent(pattern, callback));
    }

    @Override
    public void unsubscribe() {

    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {

    }

    @Deprecated
    @Override
    public ConsumerRecords<K, V> poll(final long timeoutMs) {
        return poll(time.timer(timeoutMs), false);
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return poll(time.timer(timeout), true);
    }

    private ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
        PollFetchEvent<K, V> pollFetchEvent = new PollFetchEvent<>();
        eventHandler.add(pollFetchEvent);

        try {
            List<SerializedRecordWrapper> wrappers = pollFetchEvent.future().get(timer.remainingMs(), TimeUnit.MILLISECONDS);

            for (SerializedRecordWrapper recordWrapper : wrappers) {
                ConsumerRecord<K, V> record = parseRecord(recordWrapper);

            }

            ConsumerRecords consumerRecords = new ConsumerRecords();


        } catch (Exception e) {

        }

    }

    /**
     * Mostly stolen from Fetcher.CompletedFetch's parseRecord...
     */
    private ConsumerRecord<K, V> parseRecord(SerializedRecordWrapper recordWrapper) {
        Record record = recordWrapper.record();
        TopicPartition partition = recordWrapper.topicPartition();

        try {
            long offset = record.offset();
            long timestamp = record.timestamp();
            Headers headers = new RecordHeaders(record.headers());
            ByteBuffer keyBytes = record.key();
            byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
            K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), headers, keyByteArray);
            ByteBuffer valueBytes = record.value();
            byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
            V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), headers, valueByteArray);
            return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                    timestamp, recordWrapper.timestampType(),
                    keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                    valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                    key, value, headers, recordWrapper.leaderEpoch());
        } catch (RuntimeException e) {
            throw new RecordDeserializationException(partition, record.offset(),
                    "Error deserializing key/value for partition " + partition +
                            " at offset " + record.offset() + ". If needed, please seek past the record to continue consumption.", e);
        }
    }

    @Override
    public void commitSync() {

    }

    @Override
    public void commitSync(Duration timeout) {

    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {

    }

    @Override
    public void commitAsync() {

    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {

    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {

    }

    @Override
    public void seek(TopicPartition partition, long offset) {

    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {

    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {

    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {

    }

    @Override
    public long position(TopicPartition partition) {
        return 0;
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return 0;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return null;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return null;
    }

    @Override
    public Set<TopicPartition> paused() {
        return null;
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {

    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {

    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        return null;
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return null;
    }

    @Override
    public void enforceRebalance() {

    }

    @Override
    public void enforceRebalance(String reason) {

    }

    @Override
    public void close() {

    }

    @Override
    public void close(Duration timeout) {

    }

    @Override
    public void wakeup() {

    }
}
