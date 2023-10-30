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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.join;

public abstract class AbstractConsumerDelegate<K, V> implements ConsumerDelegate<K, V> {

    private final Logger log;
    protected final String clientId;
    protected final Optional<String> groupId;
    protected final SubscriptionState subscriptions;
    protected final ConsumerMetadata metadata;
    protected final Time time;
    protected final Metrics metrics;
    final KafkaConsumerMetrics kafkaConsumerMetrics;
    protected final Deserializers<K, V> deserializers;
    protected final long requestTimeoutMs;
    protected final int defaultApiTimeoutMs;
    protected final long retryBackoffMs;
    protected final ConsumerInterceptors<K, V> interceptors;
    protected final IsolationLevel isolationLevel;
    protected final List<ConsumerPartitionAssignor> assignors;
    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
    protected boolean cachedSubscriptionHasAllFetchPositions;
    protected volatile boolean closed = false;

    public AbstractConsumerDelegate(LogContext logContext,
                                    String clientId,
                                    Optional<String> groupId,
                                    SubscriptionState subscriptions,
                                    ConsumerMetadata metadata,
                                    Time time,
                                    Metrics metrics,
                                    KafkaConsumerMetrics kafkaConsumerMetrics,
                                    Deserializers<K, V> deserializers,
                                    long requestTimeoutMs,
                                    int defaultApiTimeoutMs,
                                    long retryBackoffMs,
                                    ConsumerInterceptors<K, V> interceptors,
                                    IsolationLevel isolationLevel,
                                    List<ConsumerPartitionAssignor> assignors) {
        this.log = logContext.logger(AbstractConsumerDelegate.class);
        this.clientId = clientId;
        this.groupId = groupId;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.time = time;
        this.metrics = metrics;
        this.kafkaConsumerMetrics = kafkaConsumerMetrics;
        this.deserializers = deserializers;
        this.requestTimeoutMs = requestTimeoutMs;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.interceptors = interceptors;
        this.isolationLevel = isolationLevel;
        this.assignors = assignors;
    }

    @Override
    public Metrics metricsInternal() {
        return metrics;
    }

    @Override
    public KafkaConsumerMetrics kafkaConsumerMetrics() {
        return kafkaConsumerMetrics;
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public Set<TopicPartition> assignment() {
        return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
    }

    @Override
    public Set<String> subscription() {
        return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.subscription()));
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        maybeThrowInvalidGroupIdException();
        if (topics == null)
            throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
        if (topics.isEmpty()) {
            // treat subscribing to empty topic list as the same as unsubscribing
            this.unsubscribe();
        } else {
            for (String topic : topics) {
                if (isBlank(topic))
                    throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
            }

            throwIfNoAssignorsConfigured();

            // Clear the buffered data which are not a part of newly assigned topics
            final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

            for (TopicPartition tp : subscriptions.assignedPartitions()) {
                if (topics.contains(tp.topic()))
                    currentTopicPartitions.add(tp);
            }

            clearBufferedDataForUnassignedPartitions(currentTopicPartitions);

            log.info("Subscribed to topic(s): {}", join(topics, ", "));
            if (this.subscriptions.subscribe(new HashSet<>(topics), listener))
                metadata.requestUpdateForNewTopics();
        }
    }

    protected abstract void clearBufferedDataForUnassignedPartitions(final Collection<TopicPartition> currentTopicPartitions);

    protected abstract void updatePatternSubscription(final Cluster cluster);

    protected abstract void unsubscribeInternal();

    protected abstract void maybeAutoCommitOffsetsAsync(final long currentTimeMs);

    protected abstract boolean commitOffsetsSync(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                 final Timer timer);

    protected abstract void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                               final OffsetCommitCallback callback);

    protected abstract void blockForPositions(final Timer timer);

    protected abstract Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(final Set<TopicPartition> partitions,
                                                                                    final Timer timer);

    protected abstract List<PartitionInfo> getTopicMetadata(final String topic,
                                                            final boolean allowAutoTopicCreation,
                                                            final Timer timer);

    protected abstract Map<String, List<PartitionInfo>> getAllTopicMetadata(final Timer timer);

    protected abstract Map<TopicPartition, OffsetAndTimestamp> fetchOffsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch,
                                                                                    final Timer timer);

    protected abstract Map<TopicPartition, Long> fetchBeginningOffsets(final Collection<TopicPartition> partitions,
                                                                       final Timer timer);

    protected abstract Map<TopicPartition, Long> fetchEndOffsets(final Collection<TopicPartition> timestampsToSearch,
                                                                 final Timer timer);

    protected abstract ConsumerGroupMetadata groupMetadataInternal();

    protected abstract void validatePositionsIfNeeded();

    protected abstract void resetPositionsIfNeeded();

    /**
     *
     * Indicates if the consumer is using the Kafka-based offset management strategy,
     * according to config {@link CommonClientConfigs#GROUP_ID_CONFIG}
     */
    protected abstract boolean isCommittedOffsetsManagementEnabled();

    /**
     * Refresh the committed offsets for partitions that require initialization.
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    protected abstract boolean initWithCommittedOffsetsIfNeeded(final Timer timer);

    protected abstract boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup);

    protected abstract void maybeTriggerWakeup();

    protected abstract void maybeSendFetches();

    protected abstract long pollTimeout(final Timer timer);

    protected abstract int sendFetches();

    protected abstract void blockForFetches(Timer pollTimer);

    protected abstract Fetch<K, V> collectFetch();

    protected abstract void closeTimedResources(final Duration duration,
                                                final AtomicReference<Throwable> firstException);

    protected abstract void closeUntimedResources(final AtomicReference<Throwable> firstException);

    @Override
    public boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
        return updateAssignmentMetadataIfNeeded(timer, true);
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        maybeThrowInvalidGroupIdException();

        if (pattern == null || pattern.pattern().isEmpty())
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));

        throwIfNoAssignorsConfigured();
        log.info("Subscribed to pattern: '{}'", pattern);
        this.subscriptions.subscribe(pattern, listener);
        updatePatternSubscription(metadata.fetch());
        this.metadata.requestUpdateForNewTopics();
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, new NoOpConsumerRebalanceListener());
    }

    public void unsubscribe() {
        clearBufferedDataForUnassignedPartitions(Collections.emptySet());
        unsubscribeInternal();
        this.subscriptions.unsubscribe();
        log.info("Unsubscribed all topics or patterns and assigned partitions");
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
                if (isBlank(topic))
                    throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
            }
            clearBufferedDataForUnassignedPartitions(partitions);

            // make sure the offsets of topic partitions the consumer is unsubscribing from
            // are committed since there will be no following rebalance
            maybeAutoCommitOffsetsAsync(time.milliseconds());

            log.info("Assigned to partition(s): {}", join(partitions, ", "));
            if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
                metadata.requestUpdateForNewTopics();
        }
    }

    @Deprecated
    @Override
    public ConsumerRecords<K, V> poll(final long timeoutMs) {
        return poll(time.timer(timeoutMs), false);
    }

    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        return poll(time.timer(timeout), true);
    }

    protected ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
        try {
            this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            do {
                maybeTriggerWakeup();

                if (includeMetadataInTimeout) {
                    // try to update assignment metadata BUT do not need to block on the timer for join group
                    updateAssignmentMetadataIfNeeded(timer, false);
                } else {
                    while (!updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE), true)) {
                        log.warn("Still waiting for metadata");
                    }
                }

                final Fetch<K, V> fetch = pollForFetches(timer);
                if (!fetch.isEmpty()) {
                    // before returning the fetched records, we can send off the next round of fetches
                    // and avoid block waiting for their responses to enable pipelining while the user
                    // is handling the fetched records.
                    //
                    // NOTE: since the consumed position has already been updated, we must not allow
                    // wakeups or any other errors to be triggered prior to returning the fetched records.
                    maybeSendFetches();

                    if (fetch.records().isEmpty()) {
                        log.trace("Returning empty records from `poll()` "
                                + "since the consumer's position has advanced for at least one topic partition");
                    }

                    return this.interceptors.onConsume(new ConsumerRecords<>(fetch.records()));
                }
            } while (timer.notExpired());

            return ConsumerRecords.empty();
        } finally {
            this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
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
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        long commitStart = time.nanoseconds();
        try {
            maybeThrowInvalidGroupIdException();
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            if (!commitOffsetsSync(new HashMap<>(offsets), time.timer(timeout))) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before successfully " +
                        "committing offsets " + offsets);
            }
        } finally {
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart);
        }
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
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,
                            final OffsetCommitCallback callback) {
        maybeThrowInvalidGroupIdException();
        log.debug("Committing offsets: {}", offsets);
        offsets.forEach(this::updateLastSeenEpochIfNewer);
        commitOffsetsAsync(new HashMap<>(offsets), callback);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        if (offset < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");

        log.info("Seeking to offset {} for partition {}", offset, partition);
        SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                offset,
                Optional.empty(), // This will ensure we skip validation
                this.metadata.currentLeader(partition));
        this.subscriptions.seekUnvalidated(partition, newPosition);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        long offset = offsetAndMetadata.offset();
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }

        if (offsetAndMetadata.leaderEpoch().isPresent()) {
            log.info("Seeking to offset {} for partition {} with epoch {}",
                    offset, partition, offsetAndMetadata.leaderEpoch().get());
        } else {
            log.info("Seeking to offset {} for partition {}", offset, partition);
        }
        Metadata.LeaderAndEpoch currentLeaderAndEpoch = this.metadata.currentLeader(partition);
        SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                offsetAndMetadata.offset(),
                offsetAndMetadata.leaderEpoch(),
                currentLeaderAndEpoch);
        this.updateLastSeenEpochIfNewer(partition, offsetAndMetadata);
        this.subscriptions.seekUnvalidated(partition, newPosition);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        Collection<TopicPartition> parts = partitions.isEmpty() ? this.subscriptions.assignedPartitions() : partitions;
        subscriptions.requestOffsetReset(parts, OffsetResetStrategy.EARLIEST);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        Collection<TopicPartition> parts = partitions.isEmpty() ? this.subscriptions.assignedPartitions() : partitions;
        subscriptions.requestOffsetReset(parts, OffsetResetStrategy.LATEST);
    }

    @Override
    public long position(TopicPartition partition) {
        return position(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public long position(TopicPartition partition, final Duration timeout) {
        if (!this.subscriptions.isAssigned(partition))
            throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");

        Timer timer = time.timer(timeout);
        do {
            SubscriptionState.FetchPosition position = this.subscriptions.validPosition(partition);
            if (position != null)
                return position.offset;

            updateFetchPositions(timer);
            blockForPositions(timer);
        } while (timer.notExpired());

        throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                "for partition " + partition + " could be determined");
    }

    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return committed(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, final Duration timeout) {
        return committed(Collections.singleton(partition), timeout).get(partition);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
        long start = time.nanoseconds();
        try {
            maybeThrowInvalidGroupIdException();
            final Map<TopicPartition, OffsetAndMetadata> offsets;
            offsets = fetchCommittedOffsets(partitions, time.timer(timeout));
            if (offsets == null) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last " +
                        "committed offset for partitions " + partitions + " could be determined. Try tuning default.api.timeout.ms " +
                        "larger to relax the threshold.");
            } else {
                offsets.forEach(this::updateLastSeenEpochIfNewer);
                return offsets;
            }
        } finally {
            kafkaConsumerMetrics.recordCommitted(time.nanoseconds() - start);
        }
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsFor(topic, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        Cluster cluster = this.metadata.fetch();
        List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
        if (!parts.isEmpty())
            return parts;

        Timer timer = time.timer(timeout);
        List<PartitionInfo> topicMetadata = getTopicMetadata(topic, metadata.allowAutoTopicCreation(), timer);
        return topicMetadata != null ? topicMetadata : Collections.emptyList();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return getAllTopicMetadata(time.timer(timeout));
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        log.debug("Pausing partitions {}", partitions);
        for (TopicPartition partition: partitions) {
            subscriptions.pause(partition);
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        log.debug("Resuming partitions {}", partitions);
        for (TopicPartition partition: partitions) {
            subscriptions.resume(partition);
        }
    }

    @Override
    public Set<TopicPartition> paused() {
        return Collections.unmodifiableSet(subscriptions.pausedPartitions());
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch,
                                                                   final Duration timeout) {
        requireNonNull(timestampsToSearch, "Timestamps to search cannot be null");

        if (timestampsToSearch.isEmpty()) {
            return Collections.emptyMap();
        }

        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            // we explicitly exclude the earliest and latest offset here so the timestamp in the returned
            // OffsetAndTimestamp is always positive.
            if (entry.getValue() < 0)
                throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                        entry.getValue() + ". The target time cannot be negative.");
        }

        // If timeout is set to zero return empty immediately; otherwise try to get the results
        // and throw timeout exception if it cannot complete in time.
        if (timeout.toMillis() == 0L) {
            Map<TopicPartition, OffsetAndTimestamp> offsetsByTimes = new HashMap<>(timestampsToSearch.size());

            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet())
                offsetsByTimes.put(entry.getKey(), null);

            return offsetsByTimes;
        }

        return fetchOffsetsForTimes(timestampsToSearch, time.timer(timeout));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return fetchBeginningOffsets(partitions, time.timer(timeout));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return fetchEndOffsets(partitions, time.timer(timeout));
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        final Long lag = subscriptions.partitionLag(topicPartition, isolationLevel);

        // if the log end offset is not known and hence cannot return lag and there is
        // no in-flight list offset requested yet,
        // issue a list offset request for that partition so that next time
        // we may get the answer; we do not need to wait for the return value
        // since we would not try to poll the network client synchronously
        if (lag == null) {
            if (subscriptions.partitionEndOffset(topicPartition, isolationLevel) == null &&
                    !subscriptions.partitionEndOffsetRequested(topicPartition)) {
                log.info("Requesting the log end offset for {} in order to compute lag", topicPartition);
                subscriptions.requestPartitionEndOffset(topicPartition);
                fetchEndOffsets(Collections.singleton(topicPartition), time.timer(0L));
            }

            return OptionalLong.empty();
        }

        return OptionalLong.of(lag);
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        maybeThrowInvalidGroupIdException();
        return groupMetadataInternal();
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    protected Fetch<K, V> pollForFetches(Timer timer) {
        // if data is available already, return it immediately
        final Fetch<K, V> fetch = collectFetch();
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // send any new fetches (won't resend pending fetches)
        sendFetches();

        long pollTimeout = pollTimeout(timer);

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        Timer pollTimer = time.timer(pollTimeout);
        blockForFetches(pollTimer);
        timer.update(pollTimer.currentTimeMs());

        return collectFetch();
    }

    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    @Override
    public void close(Duration timeout) {
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        try {
            if (!closed) {
                // need to close before setting the flag since the close function
                // itself may trigger rebalance callback that needs the consumer to be open still
                close(timeout, false);
            }
        } finally {
            closed = true;
        }
    }

    protected void close(Duration timeout, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        // Close objects with a timeout. The timeout is required because some resources send requests to
        // the server in the process of closing which may not respect the overall timeout defined for closing the
        // consumer.
        closeTimedResources(timeout, firstException);

        // TODO: add comments here...
        closeUntimedResources(firstException);

        AppInfoParser.unregisterAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    /**
     * Set the fetch position to the committed position (if there is one)
     * or reset it using the offset reset policy the user has configured.
     *
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no offset reset policy is
     *             defined
     * @return true iff the operation completed without timing out
     */
    protected boolean updateFetchPositions(final Timer timer) {
        // If any partitions have been truncated due to a leader change, we need to validate the offsets
        validatePositionsIfNeeded();

        cachedSubscriptionHasAllFetchPositions = subscriptions.hasAllFetchPositions();
        if (cachedSubscriptionHasAllFetchPositions)
            return true;

        // If there are any partitions which do not have a valid position and are not
        // awaiting reset, then we need to fetch committed offsets. We will only do a
        // coordinator lookup if there are partitions which have missing positions, so
        // a consumer with manually assigned partitions can avoid a coordinator dependence
        // by always ensuring that assigned partitions have an initial position.
        if (isCommittedOffsetsManagementEnabled() && !initWithCommittedOffsetsIfNeeded(timer))
            return false;

        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise an exception.
        subscriptions.resetInitializingPositions();

        // Finally send an asynchronous request to look up and update the positions of any
        // partitions which are awaiting reset.
        resetPositionsIfNeeded();

        return true;
    }

    protected void throwIfNoAssignorsConfigured() {
        if (assignors.isEmpty())
            throw new IllegalStateException("Must configure at least one partition assigner class name to " +
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
    }

    protected void maybeThrowInvalidGroupIdException() {
        if (!groupId.isPresent())
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
    }

    protected void updateLastSeenEpochIfNewer(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        if (offsetAndMetadata != null)
            offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
    }

}