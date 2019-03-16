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

import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.LogContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;


/**
 * A mock of the {@link Consumer} interface you can use for testing code that uses Kafka. This class is <i> not
 * threadsafe </i>. However, you can use the {@link #schedulePollTask(Runnable)} method to write multithreaded tests
 * where a driver thread waits for {@link #poll(Duration)} to be called by a background thread and then can safely perform
 * operations during a callback.
 */
public class MockConsumer<K, V> implements Consumer<K, V> {

    private final Map<String, List<PartitionInfo>> partitions;
    private final SubscriptionState subscriptions;
    private final Map<TopicPartition, Long> beginningOffsets;
    private final Map<TopicPartition, List<Long>> endOffsets;
    private final Map<TopicPartition, OffsetAndMetadata> committed;
    private final Queue<Runnable> pollTasks;
    private final Set<TopicPartition> paused;

    private Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    private KafkaException exception;
    private AtomicBoolean wakeup;
    private boolean closed;

    public MockConsumer(OffsetResetStrategy offsetResetStrategy) {
        this.subscriptions = new SubscriptionState(new LogContext(), offsetResetStrategy);
        this.partitions = new HashMap<>();
        this.records = new HashMap<>();
        this.paused = new HashSet<>();
        this.closed = false;
        this.beginningOffsets = new HashMap<>();
        this.endOffsets = new HashMap<>();
        this.pollTasks = new LinkedList<>();
        this.exception = null;
        this.wakeup = new AtomicBoolean(false);
        this.committed = new HashMap<>();
    }

    @Override
    public synchronized Set<TopicPartition> assignment() {
        return this.subscriptions.assignedPartitions();
    }

    /** Simulate a rebalance event. */
    public synchronized void rebalance(Collection<TopicPartition> newAssignment) {
        // TODO: Rebalance callbacks
        this.records.clear();
        this.subscriptions.assignFromSubscribed(newAssignment);
    }

    @Override
    public synchronized Set<String> subscription() {
        return this.subscriptions.subscription();
    }

    @Override
    public synchronized void subscribe(Collection<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    @Override
    public synchronized void subscribe(Pattern pattern, final ConsumerRebalanceListener listener) {
        ensureNotClosed();
        committed.clear();
        this.subscriptions.subscribe(pattern, listener);
        Set<String> topicsToSubscribe = new HashSet<>();
        for (String topic: partitions.keySet()) {
            if (pattern.matcher(topic).matches() &&
                !subscriptions.subscription().contains(topic))
                topicsToSubscribe.add(topic);
        }
        ensureNotClosed();
        this.subscriptions.subscribeFromPattern(topicsToSubscribe);
        final Set<TopicPartition> assignedPartitions = new HashSet<>();
        for (final String topic : topicsToSubscribe) {
            for (final PartitionInfo info : this.partitions.get(topic)) {
                assignedPartitions.add(new TopicPartition(topic, info.partition()));
            }

        }
        subscriptions.assignFromSubscribed(assignedPartitions);
    }

    @Override
    public synchronized void subscribe(Pattern pattern) {
        subscribe(pattern, new NoOpConsumerRebalanceListener());
    }

    @Override
    public synchronized void subscribe(Collection<String> topics, final ConsumerRebalanceListener listener) {
        ensureNotClosed();
        committed.clear();
        this.subscriptions.subscribe(new HashSet<>(topics), listener);
    }

    @Override
    public synchronized void assign(Collection<TopicPartition> partitions) {
        ensureNotClosed();
        committed.clear();
        this.subscriptions.assignFromUser(new HashSet<>(partitions));
    }

    @Override
    public synchronized void unsubscribe() {
        ensureNotClosed();
        committed.clear();
        subscriptions.unsubscribe();
    }

    @Deprecated
    @Override
    public synchronized ConsumerRecords<K, V> poll(long timeout) {
        return poll(Duration.ZERO);
    }

    @Override
    public synchronized ConsumerRecords<K, V> poll(final Duration timeout) {
        ensureNotClosed();

        // Synchronize around the entire execution so new tasks to be triggered on subsequent poll calls can be added in
        // the callback
        synchronized (pollTasks) {
            Runnable task = pollTasks.poll();
            if (task != null)
                task.run();
        }

        if (wakeup.get()) {
            wakeup.set(false);
            throw new WakeupException();
        }

        if (exception != null) {
            RuntimeException exception = this.exception;
            this.exception = null;
            throw exception;
        }

        // Handle seeks that need to wait for a poll() call to be processed
        for (TopicPartition tp : subscriptions.assignedPartitions())
            if (!subscriptions.hasValidPosition(tp))
                updateFetchPosition(tp);

        // update the consumed offset
        final Map<TopicPartition, List<ConsumerRecord<K, V>>> results = new HashMap<>();

        for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : this.records.entrySet()) {
            if (!subscriptions.isPaused(entry.getKey())) {
                final List<ConsumerRecord<K, V>> recs = entry.getValue();
                for (final ConsumerRecord<K, V> rec : recs) {
                    if (beginningOffsets.get(entry.getKey()) != null && beginningOffsets.get(entry.getKey()) > subscriptions.position(entry.getKey())) {
                        throw new OffsetOutOfRangeException(Collections.singletonMap(entry.getKey(), subscriptions.position(entry.getKey())));
                    }

                    if (assignment().contains(entry.getKey()) && rec.offset() >= subscriptions.position(entry.getKey())) {
                        results.computeIfAbsent(entry.getKey(), partition -> new ArrayList<>()).add(rec);
                        subscriptions.position(entry.getKey(), rec.offset() + 1);
                    }
                }
            }
        }
        this.records.clear();
        return new ConsumerRecords<>(results);
    }

    public synchronized void addRecord(ConsumerRecord<K, V> record) {
        ensureNotClosed();
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        Set<TopicPartition> currentAssigned = new HashSet<>(this.subscriptions.assignedPartitions());
        if (!currentAssigned.contains(tp))
            throw new IllegalStateException("Cannot add records for a partition that is not assigned to the consumer");
        List<ConsumerRecord<K, V>> recs = this.records.computeIfAbsent(tp, k -> new ArrayList<>());
        recs.add(record);
    }

    public synchronized void setException(KafkaException exception) {
        this.exception = exception;
    }

    @Override
    public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        ensureNotClosed();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet())
            committed.put(entry.getKey(), entry.getValue());
        if (callback != null) {
            callback.onComplete(offsets, null);
        }
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitAsync(offsets, null);
    }

    @Override
    public synchronized void commitAsync() {
        commitAsync(null);
    }

    @Override
    public synchronized void commitAsync(OffsetCommitCallback callback) {
        ensureNotClosed();
        commitAsync(this.subscriptions.allConsumed(), callback);
    }

    @Override
    public synchronized void commitSync() {
        commitSync(this.subscriptions.allConsumed());
    }

    @Override
    public synchronized void commitSync(Duration timeout) {
        commitSync(this.subscriptions.allConsumed());
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        commitSync(offsets);
    }

    @Override
    public synchronized void seek(TopicPartition partition, long offset) {
        ensureNotClosed();
        subscriptions.seek(partition, offset);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        ensureNotClosed();
        subscriptions.seek(partition, offsetAndMetadata.offset());
    }

    @Override
    public synchronized OffsetAndMetadata committed(TopicPartition partition) {
        ensureNotClosed();
        if (subscriptions.isAssigned(partition)) {
            return committed.get(partition);
        }
        return new OffsetAndMetadata(0);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, final Duration timeout) {
        return committed(partition);
    }

    @Override
    public synchronized long position(TopicPartition partition) {
        ensureNotClosed();
        if (!this.subscriptions.isAssigned(partition))
            throw new IllegalArgumentException("You can only check the position for partitions assigned to this consumer.");
        Long offset = this.subscriptions.position(partition);
        if (offset == null) {
            updateFetchPosition(partition);
            offset = this.subscriptions.position(partition);
        }
        return offset;
    }

    @Override
    public synchronized long position(TopicPartition partition, final Duration timeout) {
        return position(partition);
    }

    @Override
    public synchronized void seekToBeginning(Collection<TopicPartition> partitions) {
        ensureNotClosed();
        for (TopicPartition tp : partitions)
            subscriptions.requestOffsetReset(tp, OffsetResetStrategy.EARLIEST);
    }

    public synchronized void updateBeginningOffsets(Map<TopicPartition, Long> newOffsets) {
        beginningOffsets.putAll(newOffsets);
    }

    @Override
    public synchronized void seekToEnd(Collection<TopicPartition> partitions) {
        ensureNotClosed();
        for (TopicPartition tp : partitions)
            subscriptions.requestOffsetReset(tp, OffsetResetStrategy.LATEST);
    }

    // needed for cases where you make a second call to endOffsets
    public synchronized void addEndOffsets(final Map<TopicPartition, Long> newOffsets) {
        innerUpdateEndOffsets(newOffsets, false);
    }

    public synchronized void updateEndOffsets(final Map<TopicPartition, Long> newOffsets) {
        innerUpdateEndOffsets(newOffsets, true);
    }

    private void innerUpdateEndOffsets(final Map<TopicPartition, Long> newOffsets,
                                       final boolean replace) {

        for (final Map.Entry<TopicPartition, Long> entry : newOffsets.entrySet()) {
            List<Long> offsets = endOffsets.get(entry.getKey());
            if (replace || offsets == null) {
                offsets = new ArrayList<>();
            }
            offsets.add(entry.getValue());
            endOffsets.put(entry.getKey(), offsets);
        }
    }

    @Override
    public synchronized Map<MetricName, ? extends Metric> metrics() {
        ensureNotClosed();
        return Collections.emptyMap();
    }

    @Override
    public synchronized List<PartitionInfo> partitionsFor(String topic) {
        ensureNotClosed();
        return this.partitions.get(topic);
    }

    @Override
    public synchronized Map<String, List<PartitionInfo>> listTopics() {
        ensureNotClosed();
        return partitions;
    }

    public synchronized void updatePartitions(String topic, List<PartitionInfo> partitions) {
        ensureNotClosed();
        this.partitions.put(topic, partitions);
    }

    @Override
    public synchronized void pause(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            subscriptions.pause(partition);
            paused.add(partition);
        }
    }

    @Override
    public synchronized void resume(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            subscriptions.resume(partition);
            paused.remove(partition);
        }
    }

    @Override
    public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public synchronized Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition tp : partitions) {
            Long beginningOffset = beginningOffsets.get(tp);
            if (beginningOffset == null)
                throw new IllegalStateException("The partition " + tp + " does not have a beginning offset.");
            result.put(tp, beginningOffset);
        }
        return result;
    }

    @Override
    public synchronized Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition tp : partitions) {
            Long endOffset = getEndOffset(endOffsets.get(tp));
            if (endOffset == null)
                throw new IllegalStateException("The partition " + tp + " does not have an end offset.");
            result.put(tp, endOffset);
        }
        return result;
    }

    @Override
    public synchronized void close() {
        close(Duration.ofMillis(KafkaConsumer.DEFAULT_CLOSE_TIMEOUT_MS));
    }

    @SuppressWarnings("deprecation")
    @Override
    public synchronized void close(long timeout, TimeUnit unit) {
        ensureNotClosed();
        this.closed = true;
    }

    public synchronized boolean closed() {
        return this.closed;
    }

    @Override
    public synchronized void wakeup() {
        wakeup.set(true);
    }

    /**
     * Schedule a task to be executed during a poll(). One enqueued task will be executed per {@link #poll(Duration)}
     * invocation. You can use this repeatedly to mock out multiple responses to poll invocations.
     * @param task the task to be executed
     */
    public synchronized void schedulePollTask(Runnable task) {
        synchronized (pollTasks) {
            pollTasks.add(task);
        }
    }

    public synchronized void scheduleNopPollTask() {
        schedulePollTask(() -> { });
    }

    public synchronized Set<TopicPartition> paused() {
        return Collections.unmodifiableSet(new HashSet<>(paused));
    }

    private void ensureNotClosed() {
        if (this.closed)
            throw new IllegalStateException("This consumer has already been closed.");
    }

    private void updateFetchPosition(TopicPartition tp) {
        if (subscriptions.isOffsetResetNeeded(tp)) {
            resetOffsetPosition(tp);
        } else if (!committed.containsKey(tp)) {
            subscriptions.requestOffsetReset(tp);
            resetOffsetPosition(tp);
        } else {
            subscriptions.seek(tp, committed.get(tp).offset());
        }
    }

    private void resetOffsetPosition(TopicPartition tp) {
        OffsetResetStrategy strategy = subscriptions.resetStrategy(tp);
        Long offset;
        if (strategy == OffsetResetStrategy.EARLIEST) {
            offset = beginningOffsets.get(tp);
            if (offset == null)
                throw new IllegalStateException("MockConsumer didn't have beginning offset specified, but tried to seek to beginning");
        } else if (strategy == OffsetResetStrategy.LATEST) {
            offset = getEndOffset(endOffsets.get(tp));
            if (offset == null)
                throw new IllegalStateException("MockConsumer didn't have end offset specified, but tried to seek to end");
        } else {
            throw new NoOffsetForPartitionException(tp);
        }
        seek(tp, offset);
    }

    private Long getEndOffset(List<Long> offsets) {
        if (offsets == null || offsets.isEmpty()) {
            return null;
        }
        return offsets.size() > 1 ? offsets.remove(0) : offsets.get(0);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return partitionsFor(topic);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return listTopics();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
            Duration timeout) {
        return offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return endOffsets(partitions);
    }

    @Override
    public void close(Duration timeout) {
        close();
    }
}
