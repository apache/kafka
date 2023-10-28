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

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.swallow;

public class LegacyKafkaConsumerDelegate<K, V> extends AbstractConsumerDelegate<K, V> {

    private static final long NO_CURRENT_THREAD = -1L;
    static final String DEFAULT_REASON = "rebalance enforced by user";

    private final Logger log;
    private final ConsumerCoordinator coordinator;
    private final Fetcher<K, V> fetcher;
    private final OffsetFetcher offsetFetcher;
    private final TopicMetadataFetcher topicMetadataFetcher;
    private final ConsumerNetworkClient client;

    // currentThread holds the threadId of the current thread accessing KafkaConsumer
    // and is used to prevent multithreaded access.
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);

    public LegacyKafkaConsumerDelegate(LogContext logContext,
                                       String clientId,
                                       Optional<String> groupId,
                                       SubscriptionState subscriptions,
                                       ConsumerMetadata metadata,
                                       Time time,
                                       Metrics metrics,
                                       KafkaConsumerMetrics kafkaConsumerMetrics,
                                       long requestTimeoutMs,
                                       int defaultApiTimeoutMs,
                                       long retryBackoffMs,
                                       ConsumerInterceptors<K, V> interceptors,
                                       IsolationLevel isolationLevel,
                                       List<ConsumerPartitionAssignor> assignors,
                                       ConsumerCoordinator coordinator,
                                       Deserializers<K, V> deserializers,
                                       Fetcher<K, V> fetcher,
                                       OffsetFetcher offsetFetcher,
                                       TopicMetadataFetcher topicMetadataFetcher,
                                       ConsumerNetworkClient client) {
        super(
                logContext,
                clientId,
                groupId,
                subscriptions,
                metadata,
                time,
                metrics,
                kafkaConsumerMetrics,
                deserializers,
                requestTimeoutMs,
                defaultApiTimeoutMs,
                retryBackoffMs,
                interceptors,
                isolationLevel,
                assignors
        );
        this.log = logContext.logger(LegacyKafkaConsumerDelegate.class);
        this.coordinator = coordinator;
        this.fetcher = fetcher;
        this.offsetFetcher = offsetFetcher;
        this.topicMetadataFetcher = topicMetadataFetcher;
        this.client = client;
    }

    public Set<TopicPartition> assignment() {
        acquireAndEnsureOpen();
        try {
            return super.assignment();
        } finally {
            release();
        }
    }

    public Set<String> subscription() {
        acquireAndEnsureOpen();
        try {
            return super.subscription();
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        acquireAndEnsureOpen();
        try {
            super.subscribe(topics, listener);
        } finally {
            release();
        }
    }

    public void unsubscribe() {
        acquireAndEnsureOpen();
        try {
            super.unsubscribe();
        } finally {
            release();
        }
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            super.assign(partitions);
        } finally {
            release();
        }
    }

    @Override
    protected int sendFetches() {
        offsetFetcher.validatePositionsOnMetadataChange();
        return fetcher.sendFetches();
    }

    @Override
    protected void blockForFetches(Timer pollTimer) {
        client.poll(pollTimer, () -> {
            // since a fetch might be completed by the background thread, we need this poll condition
            // to ensure that we do not block unnecessarily in poll()
            return !fetcher.hasAvailableFetches();
        });
    }

    @Override
    protected Fetch<K, V> collectFetch() {
        return fetcher.collectFetch();
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            super.commitSync(offsets, timeout);
        } finally {
            release();
        }
    }

    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            super.commitAsync(offsets, callback);
        } finally {
            release();
        }
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        acquireAndEnsureOpen();
        try {
            super.seek(partition, offset);
        } finally {
            release();
        }
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        acquireAndEnsureOpen();
        try {
            super.seek(partition, offsetAndMetadata);
        } finally {
            release();
        }
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            super.seekToBeginning(partitions);
        } finally {
            release();
        }
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            super.seekToEnd(partitions);
        } finally {
            release();
        }
    }

    @Override
    public long position(TopicPartition partition, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return super.position(partition, timeout);
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
                                                            final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return super.committed(partitions, timeout);
        } finally {
            release();
        }
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return super.partitionsFor(topic, timeout);
        } finally {
            release();
        }
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return super.listTopics(timeout);
        } finally {
            release();
        }
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            super.pause(partitions);
        } finally {
            release();
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            super.resume(partitions);
        } finally {
            release();
        }
    }

    @Override
    public Set<TopicPartition> paused() {
        acquireAndEnsureOpen();
        try {
            return super.paused();
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return super.offsetsForTimes(timestampsToSearch, timeout);
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return super.beginningOffsets(partitions, timeout);
        } finally {
            release();
        }
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        acquireAndEnsureOpen();
        try {
            return super.groupMetadata();
        } finally {
            release();
        }
    }

    @Override
    public void enforceRebalance(final String reason) {
        acquireAndEnsureOpen();
        try {
            if (coordinator == null) {
                throw new IllegalStateException("Tried to force a rebalance but consumer does not have a group.");
            }
            coordinator.requestRejoin(reason == null || reason.isEmpty() ? DEFAULT_REASON : reason);
        } finally {
            release();
        }
    }

    /**
     * @see #enforceRebalance(String)
     */
    @Override
    public void enforceRebalance() {
        enforceRebalance(null);
    }

    @Override
    public void close(Duration timeout) {
        acquire();
        try {
            super.close(timeout);
        } finally {
            release();
        }
    }

    @Override
    public void wakeup() {
        this.client.wakeup();
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     * @throws IllegalStateException If the consumer has been closed
     */
    private void acquireAndEnsureOpen() {
        acquire();
        if (this.closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this consumer from multi-threaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multi-threaded usage is not
     * supported).
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        final Thread thread = Thread.currentThread();
        final long threadId = thread.getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access. " +
                    "currentThread(name: " + thread.getName() + ", id: " + threadId + ")" +
                    " otherThread(id: " + currentThread.get() + ")"
            );
        refcount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multi-threaded access.
     */
    private void release() {
        if (refcount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    @Override
    protected void clearBufferedDataForUnassignedPartitions(Collection<TopicPartition> currentTopicPartitions) {
        fetcher.clearBufferedDataForUnassignedPartitions(currentTopicPartitions);
    }

    @Override
    protected void updatePatternSubscription(Cluster cluster) {
        coordinator.updatePatternSubscription(cluster);
    }

    @Override
    protected void unsubscribeInternal() {
        if (this.coordinator != null) {
            this.coordinator.onLeavePrepare();
            this.coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics");
        }
    }

    @Override
    protected void maybeAutoCommitOffsetsAsync(long currentTimeMs) {
        this.coordinator.maybeAutoCommitOffsetsAsync(currentTimeMs);
    }

    @Override
    protected boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, Timer timer) {
        return coordinator.commitOffsetsSync(new HashMap<>(offsets), timer);
    }

    @Override
    protected void commitOffsetsAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        coordinator.commitOffsetsAsync(new HashMap<>(offsets), callback);
    }

    @Override
    protected void blockForPositions(Timer timer) {
        client.poll(timer);
    }

    @Override
    protected Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(Set<TopicPartition> partitions, Timer timer) {
        return coordinator.fetchCommittedOffsets(partitions, timer);
    }

    @Override
    protected List<PartitionInfo> getTopicMetadata(String topic, boolean allowAutoTopicCreation, Timer timer) {
        return topicMetadataFetcher.getTopicMetadata(topic, metadata.allowAutoTopicCreation(), timer);
    }

    @Override
    protected Map<String, List<PartitionInfo>> getAllTopicMetadata(Timer timer) {
        return topicMetadataFetcher.getAllTopicMetadata(timer);
    }

    @Override
    protected Map<TopicPartition, OffsetAndTimestamp> fetchOffsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch,
                                                                           final Timer timer) {
        return offsetFetcher.offsetsForTimes(timestampsToSearch, timer);
    }

    @Override
    protected Map<TopicPartition, Long> fetchBeginningOffsets(Collection<TopicPartition> partitions, Timer timer) {
        return offsetFetcher.beginningOffsets(partitions, timer);
    }

    @Override
    protected Map<TopicPartition, Long> fetchEndOffsets(Collection<TopicPartition> partitions, Timer timer) {
        return offsetFetcher.endOffsets(partitions, timer);
    }

    @Override
    protected ConsumerGroupMetadata groupMetadataInternal() {
        return coordinator.groupMetadata();
    }

    @Override
    protected void maybeTriggerWakeup() {
        client.maybeTriggerWakeup();
    }

    @Override
    protected void maybeSendFetches() {

    }

    @Override
    protected void validatePositionsIfNeeded() {
        offsetFetcher.validatePositionsIfNeeded();
    }

    @Override
    protected void resetPositionsIfNeeded() {
        offsetFetcher.resetPositionsIfNeeded();
    }

    @Override
    protected boolean isCommittedOffsetsManagementEnabled() {
        return coordinator != null;
    }

    @Override
    protected boolean initWithCommittedOffsetsIfNeeded(Timer timer) {
        return coordinator.initWithCommittedOffsetsIfNeeded(timer);
    }

    @Override
    protected boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
        if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
            return false;
        }

        return updateFetchPositions(timer);
    }

    @Override
    protected long pollTimeout(final Timer timer) {
        return coordinator == null ? timer.remainingMs() : Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());
    }

    @Override
    protected void closeTimedResources(final Duration timeout, final AtomicReference<Throwable> firstException) {
        Timer closeTimer = time.timer(timeout);

        if (coordinator != null) {
            // This is a blocking call bound by the time remaining in closeTimer
            swallow(log, Level.ERROR, "Failed to close coordinator with a timeout(ms)=" + closeTimer.timeoutMs(), () -> coordinator.close(closeTimer), firstException);
        }

        if (fetcher != null) {
            // the timeout for the session close is at-most the requestTimeoutMs
            long remainingDurationInTimeout = Math.max(0, timeout.toMillis() - closeTimer.elapsedMs());
            if (remainingDurationInTimeout > 0) {
                remainingDurationInTimeout = Math.min(requestTimeoutMs, remainingDurationInTimeout);
            }

            closeTimer.reset(remainingDurationInTimeout);

            // This is a blocking call bound by the time remaining in closeTimer
            swallow(log, Level.ERROR, "Failed to close fetcher with a timeout(ms)=" + closeTimer.timeoutMs(), () -> fetcher.close(closeTimer), firstException);
        }
    }

    @Override
    protected void closeUntimedResources(AtomicReference<Throwable> firstException) {
        closeQuietly(interceptors, "consumer interceptors", firstException);
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(client, "consumer network client", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);
    }
}