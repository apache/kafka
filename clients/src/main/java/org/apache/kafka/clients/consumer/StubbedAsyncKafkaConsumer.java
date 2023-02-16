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

import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.SerializedRecordWrapper;
import org.apache.kafka.clients.consumer.internals.events.AssignPartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitAsyncEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitSyncEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.consumer.internals.events.FetchCommittedOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchRecordsEvent;
import org.apache.kafka.clients.consumer.internals.events.RequestRebalanceEvent;
import org.apache.kafka.clients.consumer.internals.events.SubscribePatternEvent;
import org.apache.kafka.clients.consumer.internals.events.SubscribeTopicsEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeEvent;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

public class StubbedAsyncKafkaConsumer<K, V> implements Consumer<K, V> {

    private Time time;

    private EventHandler eventHandler;

    private SubscriptionState subscriptions;

    private Deserializer<K> keyDeserializer;

    private Deserializer<V> valueDeserializer;

    private long defaultApiTimeoutMs;

    private List<ConsumerPartitionAssignor> assignors;

    private Optional<String> groupId;

    @Override
    public Set<TopicPartition> assignment() {
        return Collections.unmodifiableSet(subscriptions.assignedPartitions());
    }

    @Override
    public Set<String> subscription() {
        return Collections.unmodifiableSet(subscriptions.subscription());
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        maybeThrowInvalidGroupIdException();
        if (topics == null)
            throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
        if (topics.isEmpty()) {
            // treat subscribing to empty topic list as the same as unsubscribing
            this.unsubscribe();
        } else {
            for (String topic : topics) {
                if (Utils.isBlank(topic))
                    throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
            }

            throwIfNoAssignorsConfigured();
            eventHandler.add(new SubscribeTopicsEvent(topics, callback));
        }
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        maybeThrowInvalidGroupIdException();
        if (pattern == null || pattern.toString().equals(""))
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));

        throwIfNoAssignorsConfigured();
        eventHandler.add(new SubscribePatternEvent(pattern, callback));
    }

    @Override
    public void unsubscribe() {
        eventHandler.add(new UnsubscribeEvent());
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        if (partitions == null) {
            throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
        } else if (partitions.isEmpty()) {
            this.unsubscribe();
        } else {
            for (TopicPartition tp : partitions) {
                String topic = (tp != null) ? tp.topic() : null;
                if (Utils.isBlank(topic))
                    throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
            }

            eventHandler.add(new AssignPartitionsEvent(partitions));
        }
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
        Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new HashMap<>();

        // The background thread will do the following when it receives this event:
        //
        // 1. Execute the 'update assignment metadata' logic (which I don't really understand at all, yet...)
        //    Update the assignments in the consumer coordinator.
        //    (hand waving) something about position validation
        //    Fetch committed offsets, if needed
        //    Reset positions & offsets
        // 2. Collect and return any previously loaded fetches
        // 3. Submit fetch requests for any ready partitions, including any we might have collected
        List<SerializedRecordWrapper> wrappers = eventHandler.addAndGet(new FetchRecordsEvent(), timer);

        // We return the serialized records from the background thread to the foreground thread so that the
        // potentially expensive task of deserializing the record won't stall out our background thread.
        for (SerializedRecordWrapper recordWrapper : wrappers) {
            TopicPartition tp = recordWrapper.topicPartition();

            // Make sure that this topic partition is still on our set of subscribed topics/assigned partitions,
            // as this might have changed since the fetcher submitted the fetch request.
            if (isRelevant(tp)) {
                ConsumerRecord<K, V> record = parseRecord(recordWrapper);
                List<ConsumerRecord<K, V>> list = records.computeIfAbsent(tp, __ -> new ArrayList<>());
                list.add(record);
            }
        }

        return !records.isEmpty() ? new ConsumerRecords<>(records) : ConsumerRecords.empty();
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
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(Duration timeout) {
        commitSync(subscriptions.allConsumed(), timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        // The background thread will do the following when it receives this event:
        //
        // 1. Metadata will update its 'last seen' epoch, if newer
        // 2. Invoke the coordinator commitOffsetsSync logic
        eventHandler.addAndGet(new CommitSyncEvent(offsets), time.timer(timeout));
    }

    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(subscriptions.allConsumed(), callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        // The background thread will do the following when it receives this event:
        //
        // 1. Metadata will update its 'last seen' epoch, if newer
        // 2. Invoke the coordinator commitOffsetsAsync logic
        eventHandler.add(new CommitAsyncEvent(offsets, callback));
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        // TODO: This needs to be propagated to the background thread, right?
        subscriptions.seek(partition, offset);
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
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition) {
        return committed(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return committed(Collections.singleton(partition), timeout).get(partition);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        // The background thread will do the following when it receives this event:
        //
        // 1. Invoke the coordinator's fetchCommittedOffsets logic
        // 2. Metadata will update its 'last seen' epoch, if newer
        return eventHandler.addAndGet(new FetchCommittedOffsetsEvent(partitions), time.timer(timeout));
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
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return offsets(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return offsets(partitions, ListOffsetsRequest.LATEST_TIMESTAMP, timeout);
    }

    private Map<TopicPartition, Long> offsets(Collection<TopicPartition> partitions, long timestamp, Duration timeout) {
        FetchOffsetsEvent event = new FetchOffsetsEvent(partitions, timestamp);
        Timer timer = time.timer(timeout);
        return eventHandler.addAndGet(event, timer);
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
        enforceRebalance(null);
    }

    @Override
    public void enforceRebalance(String reason) {
        eventHandler.add(new RequestRebalanceEvent(reason));
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

    private void throwIfNoAssignorsConfigured() {
        if (assignors.isEmpty())
            throw new IllegalStateException("Must configure at least one partition assigner class name to " +
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
    }

    private void maybeThrowInvalidGroupIdException() {
        if (!groupId.isPresent())
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
    }

    private boolean isRelevant(TopicPartition tp) {
        return true;
    }

}
