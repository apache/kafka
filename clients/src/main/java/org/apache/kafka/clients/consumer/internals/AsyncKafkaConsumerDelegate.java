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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsApplicationEvent;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.refreshCommittedOffsets;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * This prototype consumer uses an {@link ApplicationEventHandler event handler} to process
 * {@link ApplicationEvent application events} so that the network IO can be processed in a dedicated
 * {@link ConsumerNetworkThread network thread}. Visit
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/Proposal%3A+Consumer+Threading+Model+Refactor">this document</a>
 * for detail implementation.
 * <p/>
 * Uses a thread-safe {@link FetchBuffer fetch buffer} for the results that are populated in the
 * {@link ConsumerNetworkThread network thread} when the results are available. Because of the interaction
 * of the fetch buffer in the application thread and the network I/O thread, this is shared between the
 * two threads and is thus designed to be thread-safe.
 */
public class AsyncKafkaConsumerDelegate<K, V> extends AbstractConsumerDelegate<K, V> {

    private final Logger log;
    private final ApplicationEventHandler applicationEventHandler;
    private final BackgroundEventProcessor backgroundEventProcessor;
    private final FetchBuffer fetchBuffer;
    private final FetchCollector<K, V> fetchCollector;
    private final WakeupTrigger wakeupTrigger = new WakeupTrigger();

    public AsyncKafkaConsumerDelegate(LogContext logContext,
                                      String clientId,
                                      Optional<String> groupId,
                                      SubscriptionState subscriptions,
                                      ConsumerMetadata metadata,
                                      Time time,
                                      Metrics metrics,
                                      KafkaConsumerMetrics kafkaConsumerMetrics,
                                      FetchMetricsManager fetchMetricsManager,
                                      ApiVersions apiVersions,
                                      int requestTimeoutMs,
                                      int defaultApiTimeoutMs,
                                      long retryBackoffMs,
                                      long retryBackoffMaxMs,
                                      int autoCommitIntervalMs,
                                      int rebalanceTimeoutMs,
                                      int heartbeatIntervalMs,
                                      boolean enableAutoCommit,
                                      boolean throwOnFetchStableOffsetUnsupported,
                                      ConsumerInterceptors<K, V> interceptors,
                                      FetchConfig fetchConfig,
                                      List<ConsumerPartitionAssignor> assignors,
                                      Deserializers<K, V> deserializers,
                                      GroupRebalanceConfig groupRebalanceConfig,
                                      Supplier<KafkaClient> kafkaClientSupplier) {
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
                fetchConfig.isolationLevel,
                assignors);

        this.log = logContext.logger(AsyncKafkaConsumerDelegate.class);

        final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
        final BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();

        // This FetchBuffer is shared between the application and network threads.
        this.fetchBuffer = new FetchBuffer(logContext);
        final Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(
                logContext,
                time,
                kafkaClientSupplier,
                requestTimeoutMs,
                retryBackoffMs
        );

        final Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(
                time,
                logContext,
                backgroundEventQueue,
                metadata,
                subscriptions,
                fetchConfig,
                fetchBuffer,
                groupRebalanceConfig,
                apiVersions,
                fetchMetricsManager,
                enableAutoCommit,
                autoCommitIntervalMs,
                rebalanceTimeoutMs,
                heartbeatIntervalMs,
                retryBackoffMs,
                retryBackoffMaxMs,
                requestTimeoutMs,
                throwOnFetchStableOffsetUnsupported,
                networkClientDelegateSupplier
        );
        final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(
                logContext,
                metadata,
                applicationEventQueue,
                requestManagersSupplier
        );
        this.applicationEventHandler = new ApplicationEventHandler(
                logContext,
                time,
                applicationEventQueue,
                applicationEventProcessorSupplier,
                networkClientDelegateSupplier,
                requestManagersSupplier
        );
        this.backgroundEventProcessor = new BackgroundEventProcessor(logContext, backgroundEventQueue);

        // The FetchCollector is only used on the application thread.
        this.fetchCollector = new FetchCollector<>(
                logContext,
                metadata,
                subscriptions,
                fetchConfig,
                deserializers,
                fetchMetricsManager,
                time
        );
    }

    // Visible for testing
    CompletableFuture<Void> commit(Map<TopicPartition, OffsetAndMetadata> offsets, final boolean isWakeupable) {
        maybeThrowInvalidGroupIdException();
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent(offsets);
        if (isWakeupable) {
            // the task can only be woken up if the top level API call is commitSync
            wakeupTrigger.setActiveTask(commitEvent.future());
        }
        applicationEventHandler.add(commitEvent);
        return commitEvent.future();
    }

    @Override
    public void enforceRebalance() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void enforceRebalance(String reason) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void wakeup() {
        wakeupTrigger.wakeup();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        long commitStart = time.nanoseconds();
        try {
            CompletableFuture<Void> commitFuture = commit(offsets, true);
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            ConsumerUtils.getResult(commitFuture, time.timer(timeout));
        } finally {
            wakeupTrigger.clearActiveTask();
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart);
        }
    }

    @Override
    protected void updatePatternSubscription(Cluster cluster) {
        throw new UnsupportedOperationException("");
    }

    @Override
    protected int sendFetches() {
        return 0;
    }

    @Override
    protected void blockForFetches(Timer pollTimer) {
        try {
            fetchBuffer.awaitNotEmpty(pollTimer);
        } catch (InterruptException e) {
            log.trace("Timeout during fetch", e);
        }
    }

    /**
     * Perform the "{@link FetchCollector#collectFetch(FetchBuffer) fetch collection}" step by reading raw data out
     * of the {@link #fetchBuffer}, converting it to a well-formed {@link CompletedFetch}, validating that it and
     * the internal {@link SubscriptionState state} are correct, and then converting it all into a {@link Fetch}
     * for returning.
     *
     * <p/>
     *
     * This method will {@link ApplicationEventHandler#wakeupNetworkThread() wake up} the {@link ConsumerNetworkThread} before
     * retuning. This is done as an optimization so that the <em>next round of data can be pre-fetched</em>.
     */
    @Override
    protected Fetch<K, V> collectFetch() {
        final Fetch<K, V> fetch = fetchCollector.collectFetch(fetchBuffer);

        // Notify the network thread to wake up and start the next round of fetching.
        applicationEventHandler.wakeupNetworkThread();

        return fetch;
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
    @Override
    protected boolean updateFetchPositions(final Timer timer) {
        // TODO: implement at higher level
        return true;
    }

    /**
     *
     * Indicates if the consumer is using the Kafka-based offset management strategy,
     * according to config {@link CommonClientConfigs#GROUP_ID_CONFIG}
     */
    @Override
    protected boolean isCommittedOffsetsManagementEnabled() {
        return groupId.isPresent();
    }

    @Override
    protected boolean initWithCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();

        log.debug("Refreshing committed offsets for partitions {}", initializingPartitions);
        try {
            final OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(initializingPartitions);
            final Map<TopicPartition, OffsetAndMetadata> offsets = applicationEventHandler.addAndGet(event, timer);
            refreshCommittedOffsets(offsets, metadata, subscriptions);
            return true;
        } catch (TimeoutException e) {
            log.error("Couldn't refresh committed offsets before timeout expired");
            return false;
        }
    }
    @Override
    protected boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
        backgroundEventProcessor.process();

        // Keeping this updateAssignmentMetadataIfNeeded wrapping up the updateFetchPositions as
        // in the previous implementation, because it will eventually involve group coordination
        // logic
        return updateFetchPositions(timer);
    }

    @Override
    protected void clearBufferedDataForUnassignedPartitions(Collection<TopicPartition> currentTopicPartitions) {
        fetchBuffer.retainAll(new HashSet<>(currentTopicPartitions));
    }

    @Override
    protected void unsubscribeInternal() {
        // TODO: send event to revoke partitions
    }

    @Override
    protected void maybeAutoCommitOffsetsAsync(long currentTimeMs) {
        // assignment change event will trigger autocommit if it is configured and the group id is specified. This is
        // to make sure offsets of topic partitions the consumer is unsubscribing from are committed since there will
        // be no following rebalance.
        //
        // See the ApplicationEventProcessor.process() method that handles this event for more detail.
        applicationEventHandler.add(new AssignmentChangeApplicationEvent(subscriptions.allConsumed(), time.milliseconds()));
    }

    @Override
    protected boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, Timer timer) {
        try {
            CompletableFuture<Void> commitFuture = commit(offsets, true);
            ConsumerUtils.getResult(commitFuture, timer);
            return true;
        } catch (TimeoutException e) {
            return false;
        } finally {
            wakeupTrigger.clearActiveTask();
        }
    }

    @Override
    protected void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                      final OffsetCommitCallback callback) {
        final CompletableFuture<Void> future = commit(offsets, false);
        final OffsetCommitCallback commitCallback = callback == null ? new DefaultOffsetCommitCallback() : callback;
        future.whenComplete((r, t) -> {
            if (t != null) {
                commitCallback.onComplete(offsets, new KafkaException(t));
            } else {
                commitCallback.onComplete(offsets, null);
            }
        }).exceptionally(e -> {
            throw new KafkaException(e);
        });
    }

    @Override
    protected void blockForPositions(Timer timer) {
        updateFetchPositions(timer);
    }

    @Override
    protected Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(Set<TopicPartition> partitions, Timer timer) {
        final OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(partitions);
        wakeupTrigger.setActiveTask(event.future());
        try {
            return applicationEventHandler.addAndGet(event, timer);
        } finally {
            wakeupTrigger.clearActiveTask();
        }
    }

    @Override
    protected List<PartitionInfo> getTopicMetadata(String topic, boolean allowAutoTopicCreation, Timer timer) {
        throw new KafkaException("method not implemented");
    }

    @Override
    protected Map<String, List<PartitionInfo>> getAllTopicMetadata(Timer timer) {
        throw new KafkaException("method not implemented");
    }

    @Override
    protected Map<TopicPartition, OffsetAndTimestamp> fetchOffsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Timer timer) {
        final ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampsToSearch,
                true
        );

        return applicationEventHandler.addAndGet(listOffsetsEvent, timer);
    }

    @Override
    protected Map<TopicPartition, Long> fetchBeginningOffsets(Collection<TopicPartition> partitions, Timer timer) {
        final Map<TopicPartition, Long> timestampToSearch = partitions
                .stream()
                .collect(Collectors.toMap(Function.identity(), tp -> ListOffsetsRequest.EARLIEST_TIMESTAMP));
        final ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampToSearch,
                false
        );
        final Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = applicationEventHandler.addAndGet(
                listOffsetsEvent,
                timer
        );
        return offsetAndTimestampMap
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    @Override
    protected Map<TopicPartition, Long> fetchEndOffsets(Collection<TopicPartition> partitions, Timer timer) {
        final Map<TopicPartition, Long> timestampToSearch = partitions
                .stream()
                .collect(Collectors.toMap(Function.identity(), tp -> ListOffsetsRequest.LATEST_TIMESTAMP));
        final ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampToSearch,
                false
        );
        final Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = applicationEventHandler.addAndGet(
                listOffsetsEvent,
                timer
        );
        return offsetAndTimestampMap
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    @Override
    protected ConsumerGroupMetadata groupMetadataInternal() {
        throw new KafkaException("method not implemented");
    }


    @Override
    protected void maybeTriggerWakeup() {

    }

    @Override
    protected void maybeSendFetches() {

    }

    @Override
    protected void validatePositionsIfNeeded() {
        // Validate positions using the partition leader end offsets, to detect if any partition
        // has been truncated due to a leader change. This will trigger an OffsetForLeaderEpoch
        // request, retrieve the partition end offsets, and validate the current position against it.
        applicationEventHandler.add(new ValidatePositionsApplicationEvent());
    }

    @Override
    protected void resetPositionsIfNeeded() {
        // Reset positions using partition offsets retrieved from the leader, for any partitions
        // which are awaiting reset. This will trigger a ListOffset request, retrieve the
        // partition offsets according to the strategy (ex. earliest, latest), and update the
        // positions.
        applicationEventHandler.add(new ResetPositionsApplicationEvent());
    }

    @Override
    protected long pollTimeout(Timer timer) {
        return timer.remainingMs();
    }

    @Override
    protected void closeTimedResources(Duration timeout, AtomicReference<Throwable> firstException) {
        if (applicationEventHandler != null)
            closeQuietly(() -> applicationEventHandler.close(timeout), "Failed to close application event handler with a timeout(ms)=" + timeout, firstException);
    }

    @Override
    protected void closeUntimedResources(AtomicReference<Throwable> firstException) {
        closeQuietly(fetchBuffer, "Failed to close the fetch buffer", firstException);
        closeQuietly(interceptors, "consumer interceptors", firstException);
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }
}
