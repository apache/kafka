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
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsApplicationEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredConsumerInterceptors;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.join;
import static org.apache.kafka.common.utils.Utils.propsToMap;

/**
 * This prototype consumer uses the EventHandler to process application
 * events so that the network IO can be processed in a background thread. Visit
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/Proposal%3A+Consumer+Threading+Model+Refactor" >this document</a>
 * for detail implementation.
 */
public class PrototypeAsyncConsumer<K, V> implements Consumer<K, V> {
    static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

    private final LogContext logContext;
    private final EventHandler eventHandler;
    private final Time time;
    private final Optional<String> groupId;
    private final Logger log;
    private final Deserializers<K, V> deserializers;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final Metrics metrics;
    private final long defaultApiTimeoutMs;

    private WakeupTrigger wakeupTrigger = new WakeupTrigger();
    public PrototypeAsyncConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(propsToMap(properties), keyDeserializer, valueDeserializer);
    }

    public PrototypeAsyncConsumer(final Map<String, Object> configs,
                                  final Deserializer<K> keyDeser,
                                  final Deserializer<V> valDeser) {
        this(new ConsumerConfig(appendDeserializerToConfig(configs, keyDeser, valDeser)), keyDeser, valDeser);
    }

    public PrototypeAsyncConsumer(final ConsumerConfig config,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer) {
        this.time = Time.SYSTEM;
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                GroupRebalanceConfig.ProtocolType.CONSUMER);
        this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
        this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        this.logContext = createLogContext(config, groupRebalanceConfig);
        this.log = logContext.logger(getClass());
        this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
        this.subscriptions = createSubscriptionState(config, logContext);
        this.metrics = createMetrics(config, time);
        List<ConsumerInterceptor<K, V>> interceptorList = configuredConsumerInterceptors(config);
        ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                metrics.reporters(),
                interceptorList,
                Arrays.asList(deserializers.keyDeserializer, deserializers.valueDeserializer));
        this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
        final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
        metadata.bootstrap(addresses);
        this.eventHandler = new DefaultEventHandler(
                config,
                groupRebalanceConfig,
                logContext,
                subscriptions,
                new ApiVersions(),
                this.metrics,
                clusterResourceListeners,
                null // this is coming from the fetcher, but we don't have one
        );
    }

    // Visible for testing
    PrototypeAsyncConsumer(Time time,
                           LogContext logContext,
                           ConsumerConfig config,
                           SubscriptionState subscriptions,
                           ConsumerMetadata metadata,
                           EventHandler eventHandler,
                           Metrics metrics,
                           Optional<String> groupId,
                           int defaultApiTimeoutMs) {
        this.time = time;
        this.logContext = logContext;
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.metrics = metrics;
        this.groupId = groupId;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.deserializers = new Deserializers<>(config);
        this.eventHandler = eventHandler;
    }

    /**
     * poll implementation using {@link EventHandler}.
     *  1. Poll for background events. If there's a fetch response event, process the record and return it. If it is
     *  another type of event, process it.
     *  2. Send fetches if needed.
     *  If the timeout expires, return an empty ConsumerRecord.
     *
     * @param timeout timeout of the poll loop
     * @return ConsumerRecord.  It can be empty if time timeout expires.
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        Timer timer = time.timer(timeout);
        try {
            do {
                if (!eventHandler.isEmpty()) {
                    final Optional<BackgroundEvent> backgroundEvent = eventHandler.poll();
                    // processEvent() may process 3 types of event:
                    // 1. Errors
                    // 2. Callback Invocation
                    // 3. Fetch responses
                    // Errors will be handled or rethrown.
                    // Callback invocation will trigger callback function execution, which is blocking until completion.
                    // Successful fetch responses will be added to the completedFetches in the fetcher, which will then
                    // be processed in the collectFetches().
                    backgroundEvent.ifPresent(event -> processEvent(event, timeout));
                }

                updateFetchPositionsIfNeeded(timer);

                // The idea here is to have the background thread sending fetches autonomously, and the fetcher
                // uses the poll loop to retrieve successful fetchResponse and process them on the polling thread.
                final Fetch<K, V> fetch = collectFetches();
                if (!fetch.isEmpty()) {
                    return processFetchResults(fetch);
                }
                // We will wait for retryBackoffMs
            } while (time.timer(timeout).notExpired());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        // TODO: Once we implement poll(), clear wakeupTrigger in a finally block: wakeupTrigger.clearActiveTask();

        return ConsumerRecords.empty();
    }

    /**
     * Set the fetch position to the committed position (if there is one) or reset it using the
     * offset reset policy the user has configured (if partitions require reset)
     *
     * @return true if the operation completed without timing out
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws NoOffsetForPartitionException                          If no offset is stored for a given partition and no offset reset policy is
     *                                                                defined
     */
    private boolean updateFetchPositionsIfNeeded(final Timer timer) {
        // Validate positions using the partition leader end offsets, to detect if any partition
        // has been truncated due to a leader change. This will trigger an OffsetForLeaderEpoch
        // request, retrieve the partition end offsets, and validate the current position against it.
        ValidatePositionsApplicationEvent validatePositionsEvent = new ValidatePositionsApplicationEvent();
        eventHandler.add(validatePositionsEvent);

        // Reset positions using committed offsets retrieved from the group coordinator, for any
        // partitions which do not have a valid position and are not awaiting reset. This will
        // trigger an OffsetFetch request and update positions with the offsets retrieved. This
        // will only do a coordinator lookup if there are partitions which have missing
        // positions, so a consumer with manually assigned partitions can avoid a coordinator
        // dependence by always ensuring that assigned partitions have an initial position.
        if (isCommittedOffsetsManagementEnabled() && !refreshCommittedOffsetsIfNeeded(timer))
            return false;

        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise a NoOffsetForPartitionException exception.
        subscriptions.resetInitializingPositions();

        // Reset positions using partition offsets retrieved from the leader, for any partitions
        // which are awaiting reset. This will trigger a ListOffset request, retrieve the
        // partition offsets according to the strategy (ex. earliest, latest), and update the
        // positions.
        ResetPositionsApplicationEvent resetPositionsEvent = new ResetPositionsApplicationEvent();
        eventHandler.add(resetPositionsEvent);
        return true;
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     */
    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    private void processEvent(final BackgroundEvent backgroundEvent, final Duration timeout) {
        // stubbed class
    }

    private ConsumerRecords<K, V> processFetchResults(final Fetch<K, V> fetch) {
        // stubbed class
        return ConsumerRecords.empty();
    }

    private Fetch<K, V> collectFetches() {
        // stubbed class
        return Fetch.empty();
    }

    /**
     * This method sends a commit event to the EventHandler and return.
     */
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
        CompletableFuture<Void> future = commit(offsets, false);
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

    // Visible for testing
    CompletableFuture<Void> commit(Map<TopicPartition, OffsetAndMetadata> offsets, final boolean isWakeupable) {
        maybeThrowInvalidGroupIdException();
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent(offsets);
        if (isWakeupable) {
            // the task can only be woken up if the top level API call is commitSync
            wakeupTrigger.setActiveTask(commitEvent.future());
        }
        eventHandler.add(commitEvent);
        return commitEvent.future();
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public long position(TopicPartition partition) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition) {
        throw new KafkaException("method not implemented");
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
                                                            final Duration timeout) {
        maybeThrowInvalidGroupIdException();
        if (partitions.isEmpty()) {
            return new HashMap<>();
        }

        final OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(partitions);
        wakeupTrigger.setActiveTask(event.future());
        try {
            return eventHandler.addAndGet(event, time.timer(timeout));
        } finally {
            wakeupTrigger.clearActiveTask();
        }
    }

    private void maybeThrowInvalidGroupIdException() {
        if (!groupId.isPresent() || groupId.get().isEmpty()) {
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Set<TopicPartition> paused() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        // Keeping same argument validation error thrown by the current consumer implementation
        // to avoid API level changes.
        requireNonNull(timestampsToSearch, "Timestamps to search cannot be null");
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            // Exclude the earliest and latest offset here so the timestamp in the returned
            // OffsetAndTimestamp is always positive.
            if (entry.getValue() < 0)
                throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                        entry.getValue() + ". The target time cannot be negative.");
        }

        if (timestampsToSearch.isEmpty()) {
            return Collections.emptyMap();
        }
        final ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampsToSearch,
                true);

        // If timeout is set to zero return empty immediately; otherwise try to get the results
        // and throw timeout exception if it cannot complete in time.
        if (timeout.toMillis() == 0L)
            return listOffsetsEvent.emptyResult();

        return eventHandler.addAndGet(listOffsetsEvent, time.timer(timeout));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.LATEST_TIMESTAMP, timeout);
    }

    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp,
                                                           Duration timeout) {
        // Keeping same argument validation error thrown by the current consumer implementation
        // to avoid API level changes.
        requireNonNull(partitions, "Partitions cannot be null");

        if (partitions.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<TopicPartition, Long> timestampToSearch =
                partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> timestamp));
        final ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampToSearch,
                false);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                eventHandler.addAndGet(listOffsetsEvent, time.timer(timeout));
        return offsetAndTimestampMap.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(),
                e -> e.getValue().offset()));
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        throw new KafkaException("method not implemented");
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
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    @Override
    public void close(Duration timeout) {
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        closeQuietly(this.eventHandler, "event handler", firstException);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    @Override
    public void wakeup() {
        wakeupTrigger.wakeup();
    }

    /**
     * This method sends a commit event to the EventHandler and waits for
     * the event to finish.
     *
     * @param timeout max wait time for the blocking operation.
     */
    @Override
    public void commitSync(final Duration timeout) {
        commitSync(subscriptions.allConsumed(), timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        CompletableFuture<Void> commitFuture = commit(offsets, true);
        try {
            commitFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            throw new org.apache.kafka.common.errors.TimeoutException(e);
        } catch (final InterruptedException e) {
            throw new InterruptException(e);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof WakeupException)
                throw new WakeupException();
            throw new KafkaException(e);
        } finally {
            wakeupTrigger.clearActiveTask();
        }
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Set<TopicPartition> assignment() {
        return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
    }

    /**
     * Get the current subscription.  or an empty set if no such call has
     * been made.
     * @return The set of topics currently subscribed to
     */
    @Override
    public Set<String> subscription() {
        return Collections.unmodifiableSet(this.subscriptions.subscription());
    }

    @Override
    public void subscribe(Collection<String> topics) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        if (partitions == null) {
            throw new IllegalArgumentException("Topic partitions collection to assign to cannot be null");
        }

        if (partitions.isEmpty()) {
            // TODO: implementation of unsubscribe() will be included in forthcoming commits.
            // this.unsubscribe();
            return;
        }

        for (TopicPartition tp : partitions) {
            String topic = (tp != null) ? tp.topic() : null;
            if (isBlank(topic))
                throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
        }

        // TODO: implementation of refactored Fetcher will be included in forthcoming commits.
        // fetcher.clearBufferedDataForUnassignedPartitions(partitions);

        // assignment change event will trigger autocommit if it is configured and the group id is specified. This is
        // to make sure offsets of topic partitions the consumer is unsubscribing from are committed since there will
        // be no following rebalance
        eventHandler.add(new AssignmentChangeApplicationEvent(this.subscriptions.allConsumed(), time.milliseconds()));

        log.info("Assigned to partition(s): {}", join(partitions, ", "));
        if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
            eventHandler.add(new NewTopicsMetadataUpdateRequestEvent());
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void subscribe(Pattern pattern) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void unsubscribe() {
        throw new KafkaException("method not implemented");
    }

    @Override
    @Deprecated
    public ConsumerRecords<K, V> poll(long timeout) {
        throw new KafkaException("method not implemented");
    }

    // Visible for testing
    WakeupTrigger wakeupTrigger() {
        return wakeupTrigger;
    }

    private static <K, V> ClusterResourceListeners configureClusterResourceListeners(
            final Deserializer<K> keyDeserializer,
            final Deserializer<V> valueDeserializer,
            final List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keyDeserializer);
        clusterResourceListeners.maybeAdd(valueDeserializer);
        return clusterResourceListeners;
    }

    /**
     *
     * Indicates if the consumer is using the Kafka-based offset management strategy,
     * according to config {@link CommonClientConfigs#GROUP_ID_CONFIG}
     */
    private boolean isCommittedOffsetsManagementEnabled() {
        return groupId.isPresent();
    }

    /**
     * Refresh the committed offsets for partitions that require initialization.
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    private boolean refreshCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();

        log.debug("Refreshing committed offsets for partitions {}", initializingPartitions);
        try {
            final Map<TopicPartition, OffsetAndMetadata> offsets = eventHandler.addAndGet(new OffsetFetchApplicationEvent(initializingPartitions), timer);
            return ConsumerUtils.refreshCommittedOffsets(offsets, this.metadata, this.subscriptions);
        } catch (org.apache.kafka.common.errors.TimeoutException e) {
            log.error("Couldn't refresh committed offsets before timeout expired");
            return false;
        }
    }


    // This is here temporary as we don't have public access to the ConsumerConfig in this module.
    public static Map<String, Object> appendDeserializerToConfig(Map<String, Object> configs,
                                                                 Deserializer<?> keyDeserializer,
                                                                 Deserializer<?> valueDeserializer) {
        // validate deserializer configuration, if the passed deserializer instance is null, the user must explicitly set a valid deserializer configuration value
        Map<String, Object> newConfigs = new HashMap<>(configs);
        if (keyDeserializer != null)
            newConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        else if (newConfigs.get(KEY_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(KEY_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        if (valueDeserializer != null)
            newConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        else if (newConfigs.get(VALUE_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(VALUE_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        return newConfigs;
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }
}
