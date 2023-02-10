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

import org.apache.kafka.clients.consumer.internals.asyncstate.ForegroundState;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.SerializedRecordWrapper;
import org.apache.kafka.clients.consumer.internals.events.AssignPartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitAsyncEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitSyncEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.consumer.internals.events.PollFetchEvent;
import org.apache.kafka.clients.consumer.internals.events.SubscribePatternEvent;
import org.apache.kafka.clients.consumer.internals.events.SubscribeTopicsEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeEvent;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.Record;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

public class StubbedAsyncKafkaConsumer<K, V> implements Consumer<K, V> {

    private Time time;

    private EventHandler eventHandler;

    private ForegroundState foregroundState;

    private Deserializer<K> keyDeserializer;

    private Deserializer<V> valueDeserializer;

    private long defaultApiTimeoutMs;

    @Override
    public Set<TopicPartition> assignment() {
        return Collections.unmodifiableSet(foregroundState.manualAssignment());
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
        // TODO:    Question 1: who "owns" the callback? The rebalance happens on the background thread,
        //          but on which thread do we want to invoke the callback--background or foreground?
        //          -
        //          Question 2: Do we want to send the list of topics to the background thread and have it reconcile
        //          the set of topics, or should we let the foreground state resolve the subscriptions and then just
        //          pass that to the background thread to let it update its state?
        foregroundState.subscribe(new HashSet<>(topics));

        // The background thread will do the following when it receives this event:
        //
        // 1. Determine the topics that were removed and clear any previously-fetched data
        // 2. Request any metadata for the topics that were added
        eventHandler.add(new SubscribeTopicsEvent(topics, callback));
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        // TODO: Same questions about callbacks and state as on other subscribe method.
        foregroundState.subscribe(pattern);

        // The background thread will do the following when it receives this event:
        //
        // 1. Determine the topics that were removed and clear any previously-fetched data
        //    The existing code doesn't seem to do this?!?
        // 2. Request any metadata for the topics that were added
        eventHandler.add(new SubscribePatternEvent(pattern, callback));
    }

    @Override
    public void unsubscribe() {
        // The background thread will do the following when it receives this event:
        //
        // 1. Remove all topics' previously-fetched data
        // 2. Run the 'leave group' coordinator logic
        // 3. Clear out its set of topics/assignments
        eventHandler.add(new UnsubscribeEvent());

        // TODO: I assume the background state is easier to resolve here as it's just cleared out.
        foregroundState.unsubscribe();
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        // TODO: Same questions about state as on subscribe methods.
        foregroundState.assign(new HashSet<>(partitions));

        // The background thread will do the following when it receives this event:
        //
        // 1. Determine the topics that were removed and clear any previously-fetched data
        // 2. Run the 'maybe auto commit offsets' logic
        // 3. Request any metadata for the topics that were added
        eventHandler.add(new AssignPartitionsEvent(partitions));
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
        List<SerializedRecordWrapper> wrappers = eventHandler.addAndGet(new PollFetchEvent(), timer);

        // We return the serialized records from the background thread to the foreground thread so that the
        // potentially expensive task of deserializing the record won't stall out our background thread.
        for (SerializedRecordWrapper recordWrapper : wrappers) {
            TopicPartition tp = recordWrapper.topicPartition();

            // Make sure that this topic partition is still on our set of subscribed topics/assigned partitions,
            // as this might have changed since the fetcher submitted the fetch request.
            if (foregroundState.isRelevant(tp)) {
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
        commitSync(foregroundState.allConsumed(), timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        // The background thread will do the following when it receives this event:
        //
        // 1. Commit offsets sync.
        eventHandler.addAndGet(new CommitSyncEvent(offsets), time.timer(timeout));
    }

    @Override
    public void commitAsync() {
        eventHandler.add(new CommitAsyncEvent());
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {

    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {

    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        // TODO: This needs to be propagated to the background thread, right?
        foregroundState.seek(partition, offset);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        // TODO: This needs to be propagated to the background thread, right?
        foregroundState.seek(partition, offsetAndMetadata);
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
        return null;
    }

    @Override
    @Deprecated
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
        enforceRebalance(null);
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
