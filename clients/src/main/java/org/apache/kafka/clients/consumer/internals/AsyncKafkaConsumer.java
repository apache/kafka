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

import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsApplicationEvent;
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
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
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
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredConsumerInterceptors;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.refreshCommittedOffsets;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.join;

/**
 * This {@link Consumer} implementation uses an {@link ApplicationEventHandler event handler} to process
 * {@link ApplicationEvent application events} so that the network I/O can be processed in a dedicated
 * {@link ConsumerNetworkThread network thread}. Visit
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/Consumer+threading+refactor+design">this document</a>
 * for implementation detail.
 *
 * <p/>
 *
 * <em>Note:</em> this {@link Consumer} implementation is part of the revised consumer group protocol from KIP-848.
 * This class should not be invoked directly; users should instead create a {@link KafkaConsumer} as before.
 * This consumer implements the new consumer group protocol and is intended to be the default in coming releases.
 */
public class AsyncKafkaConsumer<K, V> implements ConsumerDelegate<K, V> {

    private static final long NO_CURRENT_THREAD = -1L;

    private final ApplicationEventHandler applicationEventHandler;
    private final Time time;
    private final Optional<String> groupId;
    private final KafkaConsumerMetrics kafkaConsumerMetrics;
    private Logger log;
    private final String clientId;
    private final BackgroundEventProcessor backgroundEventProcessor;
    private final Deserializers<K, V> deserializers;

    /**
     * A thread-safe {@link FetchBuffer fetch buffer} for the results that are populated in the
     * {@link ConsumerNetworkThread network thread} when the results are available. Because of the interaction
     * of the fetch buffer in the application thread and the network I/O thread, this is shared between the
     * two threads and is thus designed to be thread-safe.
     */
    private final FetchBuffer fetchBuffer;
    private final FetchCollector<K, V> fetchCollector;
    private final ConsumerInterceptors<K, V> interceptors;
    private final IsolationLevel isolationLevel;

    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final Metrics metrics;
    private final long retryBackoffMs;
    private final int defaultApiTimeoutMs;
    private volatile boolean closed = false;
    private final List<ConsumerPartitionAssignor> assignors;

    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
    private boolean cachedSubscriptionHasAllFetchPositions;
    private final WakeupTrigger wakeupTrigger = new WakeupTrigger();

    // currentThread holds the threadId of the current thread accessing the AsyncKafkaConsumer
    // and is used to prevent multithreaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    private final AtomicInteger refCount = new AtomicInteger(0);

    AsyncKafkaConsumer(final ConsumerConfig config,
                       final Deserializer<K> keyDeserializer,
                       final Deserializer<V> valueDeserializer) {
        try {
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                    GroupRebalanceConfig.ProtocolType.CONSUMER);

            this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
            LogContext logContext = createLogContext(config, groupRebalanceConfig);
            this.log = logContext.logger(getClass());
            groupId.ifPresent(groupIdStr -> {
                if (groupIdStr.isEmpty()) {
                    log.warn("Support for using the empty group id by consumers is deprecated and will be removed in the next major release.");
                }
            });

            log.debug("Initializing the Kafka consumer");
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            this.time = Time.SYSTEM;
            this.metrics = createMetrics(config, time);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

            List<ConsumerInterceptor<K, V>> interceptorList = configuredConsumerInterceptors(config);
            this.interceptors = new ConsumerInterceptors<>(interceptorList);
            this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
            this.subscriptions = createSubscriptionState(config, logContext);
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(metrics.reporters(),
                    interceptorList,
                    Arrays.asList(deserializers.keyDeserializer, deserializers.valueDeserializer));
            this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
            final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            metadata.bootstrap(addresses);

            FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);
            FetchConfig fetchConfig = new FetchConfig(config);
            this.isolationLevel = fetchConfig.isolationLevel;

            ApiVersions apiVersions = new ApiVersions();
            final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
            final BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();

            // This FetchBuffer is shared between the application and network threads.
            this.fetchBuffer = new FetchBuffer(logContext);
            final Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(time,
                    logContext,
                    metadata,
                    config,
                    apiVersions,
                    metrics,
                    fetchMetricsManager);
            final Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(time,
                    logContext,
                    backgroundEventQueue,
                    metadata,
                    subscriptions,
                    fetchBuffer,
                    config,
                    groupRebalanceConfig,
                    apiVersions,
                    fetchMetricsManager,
                    networkClientDelegateSupplier);
            final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(logContext,
                    metadata,
                    applicationEventQueue,
                    requestManagersSupplier);
            this.applicationEventHandler = new ApplicationEventHandler(logContext,
                    time,
                    applicationEventQueue,
                    applicationEventProcessorSupplier,
                    networkClientDelegateSupplier,
                    requestManagersSupplier);
            this.backgroundEventProcessor = new BackgroundEventProcessor(logContext, backgroundEventQueue);
            this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
                    config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                    config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
            );

            // no coordinator will be constructed for the default (null) group id
            if (!groupId.isPresent()) {
                config.ignore(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
                config.ignore(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
            }

            // The FetchCollector is only used on the application thread.
            this.fetchCollector = new FetchCollector<>(logContext,
                    metadata,
                    subscriptions,
                    fetchConfig,
                    deserializers,
                    fetchMetricsManager,
                    time);

            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_METRIC_GROUP_PREFIX);

            config.logUnused();
            AppInfoParser.registerAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
            // we do not need to call `close` at all when `log` is null, which means no internal objects were initialized.
            if (this.log != null) {
                close(Duration.ZERO, true);
            }
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    // Visible for testing
    AsyncKafkaConsumer(LogContext logContext,
                       String clientId,
                       Deserializers<K, V> deserializers,
                       FetchBuffer fetchBuffer,
                       FetchCollector<K, V> fetchCollector,
                       ConsumerInterceptors<K, V> interceptors,
                       Time time,
                       ApplicationEventHandler applicationEventHandler,
                       BlockingQueue<BackgroundEvent> backgroundEventQueue,
                       Metrics metrics,
                       SubscriptionState subscriptions,
                       ConsumerMetadata metadata,
                       long retryBackoffMs,
                       int defaultApiTimeoutMs,
                       List<ConsumerPartitionAssignor> assignors,
                       String groupId) {
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.clientId = clientId;
        this.fetchBuffer = fetchBuffer;
        this.fetchCollector = fetchCollector;
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        this.interceptors = Objects.requireNonNull(interceptors);
        this.time = time;
        this.backgroundEventProcessor = new BackgroundEventProcessor(logContext, backgroundEventQueue);
        this.metrics = metrics;
        this.groupId = Optional.ofNullable(groupId);
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.deserializers = deserializers;
        this.applicationEventHandler = applicationEventHandler;
        this.assignors = assignors;
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, "consumer");
    }

    // Visible for testing
    AsyncKafkaConsumer(LogContext logContext,
                       Time time,
                       ConsumerConfig config,
                       Deserializer<K> keyDeserializer,
                       Deserializer<V> valueDeserializer,
                       KafkaClient client,
                       SubscriptionState subscriptions,
                       ConsumerMetadata metadata,
                       List<ConsumerPartitionAssignor> assignors) {
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
        this.fetchBuffer = new FetchBuffer(logContext);
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        this.interceptors = new ConsumerInterceptors<>(Collections.emptyList());
        this.time = time;
        this.metrics = new Metrics(time);
        this.groupId = Optional.ofNullable(config.getString(ConsumerConfig.GROUP_ID_CONFIG));
        this.metadata = metadata;
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        this.deserializers = new Deserializers<>(keyDeserializer, valueDeserializer);
        this.assignors = assignors;

        ConsumerMetrics metricsRegistry = new ConsumerMetrics(CONSUMER_METRIC_GROUP_PREFIX);
        FetchMetricsManager fetchMetricsManager = new FetchMetricsManager(metrics, metricsRegistry.fetcherMetrics);
        this.fetchCollector = new FetchCollector<>(logContext,
                metadata,
                subscriptions,
                new FetchConfig(config),
                deserializers,
                fetchMetricsManager,
                time);
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, "consumer");

        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
            config,
            GroupRebalanceConfig.ProtocolType.CONSUMER
        );

        BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
        BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventProcessor = new BackgroundEventProcessor(logContext, backgroundEventQueue);
        ApiVersions apiVersions = new ApiVersions();
        Supplier<NetworkClientDelegate> networkClientDelegateSupplier = () -> new NetworkClientDelegate(
            time,
            config,
            logContext,
            client
        );
        Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(
            time,
            logContext,
            backgroundEventQueue,
            metadata,
            subscriptions,
            fetchBuffer,
            config,
            groupRebalanceConfig,
            apiVersions,
            fetchMetricsManager,
            networkClientDelegateSupplier
        );
        Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(
                logContext,
                metadata,
                applicationEventQueue,
                requestManagersSupplier
        );
        this.applicationEventHandler = new ApplicationEventHandler(logContext,
                time,
                applicationEventQueue,
                applicationEventProcessorSupplier,
                networkClientDelegateSupplier,
                requestManagersSupplier);
    }

    /**
     * poll implementation using {@link ApplicationEventHandler}.
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

        acquireAndEnsureOpen();
        try {
            kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            if (subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            do {
                updateAssignmentMetadataIfNeeded(timer);
                final Fetch<K, V> fetch = pollForFetches(timer);

                if (!fetch.isEmpty()) {
                    if (fetch.records().isEmpty()) {
                        log.trace("Returning empty records from `poll()` "
                                + "since the consumer's position has advanced for at least one topic partition");
                    }

                    return interceptors.onConsume(new ConsumerRecords<>(fetch.records()));
                }
                // We will wait for retryBackoffMs
            } while (timer.notExpired());

            return ConsumerRecords.empty();
        } finally {
            kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
            release();
        }
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     */
    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
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
        acquireAndEnsureOpen();
        try {
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
        } finally {
            release();
        }
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
    public void seek(TopicPartition partition, long offset) {
        if (offset < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");

        acquireAndEnsureOpen();
        try {
            log.info("Seeking to offset {} for partition {}", offset, partition);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                offset,
                Optional.empty(), // This will ensure we skip validation
                metadata.currentLeader(partition));
            subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            release();
        }
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        long offset = offsetAndMetadata.offset();
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }

        acquireAndEnsureOpen();
        try {
            if (offsetAndMetadata.leaderEpoch().isPresent()) {
                log.info("Seeking to offset {} for partition {} with epoch {}",
                    offset, partition, offsetAndMetadata.leaderEpoch().get());
            } else {
                log.info("Seeking to offset {} for partition {}", offset, partition);
            }
            Metadata.LeaderAndEpoch currentLeaderAndEpoch = metadata.currentLeader(partition);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                offsetAndMetadata.offset(),
                offsetAndMetadata.leaderEpoch(),
                currentLeaderAndEpoch);
            updateLastSeenEpochIfNewer(partition, offsetAndMetadata);
            subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            release();
        }
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        acquireAndEnsureOpen();
        try {
            Collection<TopicPartition> parts = partitions.isEmpty() ? subscriptions.assignedPartitions() : partitions;
            subscriptions.requestOffsetReset(parts, OffsetResetStrategy.EARLIEST);
        } finally {
            release();
        }
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        acquireAndEnsureOpen();
        try {
            Collection<TopicPartition> parts = partitions.isEmpty() ? subscriptions.assignedPartitions() : partitions;
            subscriptions.requestOffsetReset(parts, OffsetResetStrategy.LATEST);
        } finally {
            release();
        }
    }

    @Override
    public long position(TopicPartition partition) {
        return position(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            if (!subscriptions.isAssigned(partition))
                throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");

            Timer timer = time.timer(timeout);
            do {
                SubscriptionState.FetchPosition position = subscriptions.validPosition(partition);
                if (position != null)
                    return position.offset;

                updateFetchPositions(timer);
            } while (timer.notExpired());

            throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                "for partition " + partition + " could be determined");
        } finally {
            release();
        }
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
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
                                                            final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            if (partitions.isEmpty()) {
                return new HashMap<>();
            }

            final OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(partitions);
            wakeupTrigger.setActiveTask(event.future());
            try {
                return applicationEventHandler.addAndGet(event, time.timer(timeout));
            } finally {
                wakeupTrigger.clearActiveTask();
            }
        } finally {
            release();
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
        return Collections.unmodifiableMap(metrics.metrics());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsFor(topic, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Set<TopicPartition> paused() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(subscriptions.pausedPartitions());
        } finally {
            release();
        }
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            log.debug("Pausing partitions {}", partitions);
            for (TopicPartition partition : partitions) {
                subscriptions.pause(partition);
            }
        } finally {
            release();
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            log.debug("Resuming partitions {}", partitions);
            for (TopicPartition partition : partitions) {
                subscriptions.resume(partition);
            }
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        acquireAndEnsureOpen();
        try {
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

            return applicationEventHandler.addAndGet(listOffsetsEvent, time.timer(timeout));
        } finally {
            release();
        }
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
        acquireAndEnsureOpen();
        try {
            // Keeping same argument validation error thrown by the current consumer implementation
            // to avoid API level changes.
            requireNonNull(partitions, "Partitions cannot be null");

            if (partitions.isEmpty()) {
                return Collections.emptyMap();
            }
            Map<TopicPartition, Long> timestampToSearch = partitions
                .stream()
                .collect(Collectors.toMap(Function.identity(), tp -> timestamp));
            ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampToSearch,
                false);
            Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = applicationEventHandler.addAndGet(
                listOffsetsEvent,
                time.timer(timeout));
            return offsetAndTimestampMap
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
        } finally {
            release();
        }
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        acquireAndEnsureOpen();
        try {
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
                    endOffsets(Collections.singleton(topicPartition), Duration.ofMillis(0));
                }

                return OptionalLong.empty();
            }

            return OptionalLong.of(lag);
        } finally {
            release();
        }
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
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        acquire();
        try {
            if (!closed) {
                // need to close before setting the flag since the close function
                // itself may trigger rebalance callback that needs the consumer to be open still
                close(timeout, false);
            }
        } finally {
            closed = true;
            release();
        }
    }

    private void close(Duration timeout, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        if (applicationEventHandler != null)
            closeQuietly(() -> applicationEventHandler.close(timeout), "Failed to close application event handler with a timeout(ms)=" + timeout, firstException);

        closeQuietly(fetchBuffer, "Failed to close the fetch buffer", firstException);
        closeQuietly(interceptors, "consumer interceptors", firstException);
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);

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
        acquireAndEnsureOpen();
        long commitStart = time.nanoseconds();
        try {
            CompletableFuture<Void> commitFuture = commit(offsets, true);
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            ConsumerUtils.getResult(commitFuture, time.timer(timeout));
        } finally {
            wakeupTrigger.clearActiveTask();
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart);
            release();
        }
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Set<TopicPartition> assignment() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(subscriptions.assignedPartitions());
        } finally {
            release();
        }
    }

    /**
     * Get the current subscription.  or an empty set if no such call has
     * been made.
     * @return The set of topics currently subscribed to
     */
    @Override
    public Set<String> subscription() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(subscriptions.subscription());
        } finally {
            release();
        }
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partitions collection to assign to cannot be null");
            }

            if (partitions.isEmpty()) {
                unsubscribe();
                return;
            }

            for (TopicPartition tp : partitions) {
                String topic = (tp != null) ? tp.topic() : null;
                if (isBlank(topic))
                    throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
            }

            // Clear the buffered data which are not a part of newly assigned topics
            final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

            for (TopicPartition tp : subscriptions.assignedPartitions()) {
                if (partitions.contains(tp))
                    currentTopicPartitions.add(tp);
            }

            fetchBuffer.retainAll(currentTopicPartitions);

            // assignment change event will trigger autocommit if it is configured and the group id is specified. This is
            // to make sure offsets of topic partitions the consumer is unsubscribing from are committed since there will
            // be no following rebalance.
            //
            // See the ApplicationEventProcessor.process() method that handles this event for more detail.
            applicationEventHandler.add(new AssignmentChangeApplicationEvent(subscriptions.allConsumed(), time.milliseconds()));

            log.info("Assigned to partition(s): {}", join(partitions, ", "));
            if (subscriptions.assignFromUser(new HashSet<>(partitions)))
                applicationEventHandler.add(new NewTopicsMetadataUpdateRequestEvent());
        } finally {
            release();
        }
    }

    /**
     * TODO: remove this when we implement the KIP-848 protocol.
     *
     * <p>
     * The contents of this method are shamelessly stolen from
     * {@link ConsumerCoordinator#updatePatternSubscription(Cluster)} and are used here because we won't have access
     * to a {@link ConsumerCoordinator} in this code. Perhaps it could be moved to a ConsumerUtils class?
     *
     * @param cluster Cluster from which we get the topics
     */
    private void updatePatternSubscription(Cluster cluster) {
        final Set<String> topicsToSubscribe = cluster.topics().stream()
                .filter(subscriptions::matchesSubscribedPattern)
                .collect(Collectors.toSet());
        if (subscriptions.subscribeFromPattern(topicsToSubscribe))
            metadata.requestUpdateForNewTopics();
    }

    @Override
    public void unsubscribe() {
        acquireAndEnsureOpen();
        try {
            fetchBuffer.retainAll(Collections.emptySet());
            subscriptions.unsubscribe();
        } finally {
            release();
        }
    }

    @Override
    @Deprecated
    public ConsumerRecords<K, V> poll(final long timeoutMs) {
        return poll(Duration.ofMillis(timeoutMs));
    }

    // Visible for testing
    WakeupTrigger wakeupTrigger() {
        return wakeupTrigger;
    }

    private Fetch<K, V> pollForFetches(Timer timer) {
        long pollTimeout = timer.remainingMs();

        // if data is available already, return it immediately
        final Fetch<K, V> fetch = collectFetch();
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        Timer pollTimer = time.timer(pollTimeout);

        // Wait a bit for some fetched data to arrive, as there may not be anything immediately available. Note the
        // use of a shorter, dedicated "pollTimer" here which updates "timer" so that calling method (poll) will
        // correctly handle the overall timeout.
        try {
            fetchBuffer.awaitNotEmpty(pollTimer);
        } catch (InterruptException e) {
            log.trace("Timeout during fetch", e);
        } finally {
            timer.update(pollTimer.currentTimeMs());
        }

        return collectFetch();
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
    private Fetch<K, V> collectFetch() {
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
    private boolean updateFetchPositions(final Timer timer) {
        // Validate positions using the partition leader end offsets, to detect if any partition
        // has been truncated due to a leader change. This will trigger an OffsetForLeaderEpoch
        // request, retrieve the partition end offsets, and validate the current position against it.
        applicationEventHandler.add(new ValidatePositionsApplicationEvent());

        cachedSubscriptionHasAllFetchPositions = subscriptions.hasAllFetchPositions();
        if (cachedSubscriptionHasAllFetchPositions) return true;

        // Reset positions using committed offsets retrieved from the group coordinator, for any
        // partitions which do not have a valid position and are not awaiting reset. This will
        // trigger an OffsetFetch request and update positions with the offsets retrieved. This
        // will only do a coordinator lookup if there are partitions which have missing
        // positions, so a consumer with manually assigned partitions can avoid a coordinator
        // dependence by always ensuring that assigned partitions have an initial position.
        if (isCommittedOffsetsManagementEnabled() && !initWithCommittedOffsetsIfNeeded(timer))
            return false;

        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise a NoOffsetForPartitionException exception.
        subscriptions.resetInitializingPositions();

        // Reset positions using partition offsets retrieved from the leader, for any partitions
        // which are awaiting reset. This will trigger a ListOffset request, retrieve the
        // partition offsets according to the strategy (ex. earliest, latest), and update the
        // positions.
        applicationEventHandler.add(new ResetPositionsApplicationEvent());
        return true;
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
    private boolean initWithCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();

        if (initializingPartitions.isEmpty())
            return true;

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

    private void throwIfNoAssignorsConfigured() {
        if (assignors.isEmpty())
            throw new IllegalStateException("Must configure at least one partition assigner class name to " +
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
    }

    private void updateLastSeenEpochIfNewer(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        if (offsetAndMetadata != null)
            offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }

    @Override
    public boolean updateAssignmentMetadataIfNeeded(Timer timer) {
        backgroundEventProcessor.process();

        // Keeping this updateAssignmentMetadataIfNeeded wrapping up the updateFetchPositions as
        // in the previous implementation, because it will eventually involve group coordination
        // logic
        return updateFetchPositions(timer);
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribeInternal(topics, Optional.empty());
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        subscribeInternal(topics, Optional.of(listener));
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribeInternal(pattern, Optional.empty());
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        subscribeInternal(pattern, Optional.of(listener));
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     *
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
     * Acquire the light lock protecting this consumer from multithreaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multithreaded usage is not
     * supported).
     *
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
        refCount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multithreaded access.
     */
    private void release() {
        if (refCount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    private void subscribeInternal(Pattern pattern, Optional<ConsumerRebalanceListener> listener) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            if (pattern == null || pattern.toString().isEmpty())
                throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));
            throwIfNoAssignorsConfigured();
            log.info("Subscribed to pattern: '{}'", pattern);
            subscriptions.subscribe(pattern, listener);
            updatePatternSubscription(metadata.fetch());
            metadata.requestUpdateForNewTopics();
        } finally {
            release();
        }
    }

    private void subscribeInternal(Collection<String> topics, Optional<ConsumerRebalanceListener> listener) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            if (topics == null)
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            if (topics.isEmpty()) {
                // treat subscribing to empty topic list as the same as unsubscribing
                unsubscribe();
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

                fetchBuffer.retainAll(currentTopicPartitions);
                log.info("Subscribed to topic(s): {}", join(topics, ", "));
                if (subscriptions.subscribe(new HashSet<>(topics), listener))
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            release();
        }
    }

    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public Metrics metricsRegistry() {
        return metrics;
    }

    @Override
    public KafkaConsumerMetrics kafkaConsumerMetrics() {
        return kafkaConsumerMetrics;
    }
}