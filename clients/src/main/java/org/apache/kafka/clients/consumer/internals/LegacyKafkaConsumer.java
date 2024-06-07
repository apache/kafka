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
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
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
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_RACK_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createConsumerNetworkClient;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredConsumerInterceptors;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.swallow;

/**
 * A client that consumes records from a Kafka cluster using the {@link GroupProtocol#CLASSIC classic group protocol}.
 * In this implementation, all network I/O happens in the thread of the application making the call.
 *
 * <p/>
 *
 * <em>Note:</em> per its name, this implementation is left for backward compatibility purposes. The updated consumer
 * group protocol (from KIP-848) introduces allows users continue using the legacy "classic" group protocol.
 * This class should not be invoked directly; users should instead create a {@link KafkaConsumer} as before.
 */
public class LegacyKafkaConsumer<K, V> implements ConsumerDelegate<K, V> {

    private static final long NO_CURRENT_THREAD = -1L;
    public static final String DEFAULT_REASON = "rebalance enforced by user";

    private final Metrics metrics;
    private final KafkaConsumerMetrics kafkaConsumerMetrics;
    private Logger log;
    private final String clientId;
    private final Optional<String> groupId;
    private final ConsumerCoordinator coordinator;
    private final Deserializers<K, V> deserializers;
    private final Fetcher<K, V> fetcher;
    private final OffsetFetcher offsetFetcher;
    private final TopicMetadataFetcher topicMetadataFetcher;
    private final ConsumerInterceptors<K, V> interceptors;
    private final IsolationLevel isolationLevel;

    private final Time time;
    private final ConsumerNetworkClient client;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final long retryBackoffMs;
    private final long retryBackoffMaxMs;
    private final int requestTimeoutMs;
    private final int defaultApiTimeoutMs;
    private volatile boolean closed = false;
    private final List<ConsumerPartitionAssignor> assignors;
    private final Optional<ClientTelemetryReporter> clientTelemetryReporter;

    // currentThread holds the threadId of the current thread accessing LegacyKafkaConsumer
    // and is used to prevent multi-threaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);

    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
    private boolean cachedSubscriptionHasAllFetchPositions;

    LegacyKafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        try {
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                    GroupRebalanceConfig.ProtocolType.CONSUMER);

            this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
            LogContext logContext = createLogContext(config, groupRebalanceConfig);
            this.log = logContext.logger(getClass());
            boolean enableAutoCommit = config.getBoolean(ENABLE_AUTO_COMMIT_CONFIG);
            groupId.ifPresent(groupIdStr -> {
                if (groupIdStr.isEmpty()) {
                    log.warn("Support for using the empty group id by consumers is deprecated and will be removed in the next major release.");
                }
            });

            log.debug("Initializing the Kafka consumer");
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            this.time = Time.SYSTEM;
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            this.clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            this.clientTelemetryReporter.ifPresent(reporters::add);
            this.metrics = createMetrics(config, time, reporters);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
            this.retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);

            List<ConsumerInterceptor<K, V>> interceptorList = configuredConsumerInterceptors(config);
            this.interceptors = new ConsumerInterceptors<>(interceptorList);
            this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
            this.subscriptions = createSubscriptionState(config, logContext);
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                    metrics.reporters(),
                    interceptorList,
                    Arrays.asList(this.deserializers.keyDeserializer, this.deserializers.valueDeserializer));
            this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            this.metadata.bootstrap(addresses);

            FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);
            FetchConfig fetchConfig = new FetchConfig(config);
            this.isolationLevel = fetchConfig.isolationLevel;

            ApiVersions apiVersions = new ApiVersions();
            this.client = createConsumerNetworkClient(config,
                    metrics,
                    logContext,
                    apiVersions,
                    time,
                    metadata,
                    fetchMetricsManager.throttleTimeSensor(),
                    retryBackoffMs,
                    clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null));

            this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
                    config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                    config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
            );

            // no coordinator will be constructed for the default (null) group id
            if (!groupId.isPresent()) {
                config.ignore(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
                config.ignore(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
                this.coordinator = null;
            } else {
                this.coordinator = new ConsumerCoordinator(groupRebalanceConfig,
                        logContext,
                        this.client,
                        assignors,
                        this.metadata,
                        this.subscriptions,
                        metrics,
                        CONSUMER_METRIC_GROUP_PREFIX,
                        this.time,
                        enableAutoCommit,
                        config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
                        this.interceptors,
                        config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED),
                        config.getString(ConsumerConfig.CLIENT_RACK_CONFIG),
                        clientTelemetryReporter);
            }
            this.fetcher = new Fetcher<>(
                    logContext,
                    this.client,
                    this.metadata,
                    this.subscriptions,
                    fetchConfig,
                    this.deserializers,
                    fetchMetricsManager,
                    this.time,
                    apiVersions);
            this.offsetFetcher = new OffsetFetcher(logContext,
                    client,
                    metadata,
                    subscriptions,
                    time,
                    retryBackoffMs,
                    requestTimeoutMs,
                    isolationLevel,
                    apiVersions);
            this.topicMetadataFetcher = new TopicMetadataFetcher(logContext,
                    client,
                    retryBackoffMs,
                    retryBackoffMaxMs);

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

    // visible for testing
    LegacyKafkaConsumer(LogContext logContext,
                        Time time,
                        ConsumerConfig config,
                        Deserializer<K> keyDeserializer,
                        Deserializer<V> valueDeserializer,
                        KafkaClient client,
                        SubscriptionState subscriptions,
                        ConsumerMetadata metadata,
                        List<ConsumerPartitionAssignor> assignors) {
        this.log = logContext.logger(getClass());
        this.time = time;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.metrics = new Metrics(time);
        this.clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
        this.groupId = Optional.ofNullable(config.getString(ConsumerConfig.GROUP_ID_CONFIG));
        this.deserializers = new Deserializers<>(keyDeserializer, valueDeserializer);
        this.isolationLevel = ConsumerUtils.configuredIsolationLevel(config);
        this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        this.assignors = assignors;
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_METRIC_GROUP_PREFIX);
        this.interceptors = new ConsumerInterceptors<>(Collections.emptyList());
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.clientTelemetryReporter = Optional.empty();

        int sessionTimeoutMs = config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        int rebalanceTimeoutMs = config.getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        boolean enableAutoCommit = config.getBoolean(ENABLE_AUTO_COMMIT_CONFIG);
        boolean throwOnStableOffsetNotSupported = config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
        int autoCommitIntervalMs = config.getInt(AUTO_COMMIT_INTERVAL_MS_CONFIG);
        String rackId = config.getString(CLIENT_RACK_CONFIG);
        Optional<String> groupInstanceId = Optional.ofNullable(config.getString(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));

        this.client = new ConsumerNetworkClient(
            logContext,
            client,
            metadata,
            time,
            retryBackoffMs,
            requestTimeoutMs,
            heartbeatIntervalMs
        );

        if (groupId.isPresent()) {
            GroupRebalanceConfig rebalanceConfig = new GroupRebalanceConfig(
                sessionTimeoutMs,
                rebalanceTimeoutMs,
                heartbeatIntervalMs,
                groupId.get(),
                groupInstanceId,
                retryBackoffMs,
                retryBackoffMaxMs,
                true
            );
            this.coordinator = new ConsumerCoordinator(
                rebalanceConfig,
                logContext,
                this.client,
                assignors,
                metadata,
                subscriptions,
                metrics,
                CONSUMER_METRIC_GROUP_PREFIX,
                time,
                enableAutoCommit,
                autoCommitIntervalMs,
                interceptors,
                throwOnStableOffsetNotSupported,
                rackId,
                clientTelemetryReporter
            );
        } else {
            this.coordinator = null;
        }

        int maxBytes = config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
        int maxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        int minBytes = config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        int fetchSize = config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        int maxPollRecords = config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        boolean checkCrcs = config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG);

        ConsumerMetrics metricsRegistry = new ConsumerMetrics(CONSUMER_METRIC_GROUP_PREFIX);
        FetchMetricsManager metricsManager = new FetchMetricsManager(metrics, metricsRegistry.fetcherMetrics);
        ApiVersions apiVersions = new ApiVersions();
        FetchConfig fetchConfig = new FetchConfig(
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                maxPollRecords,
                checkCrcs,
                rackId,
                isolationLevel
        );
        this.fetcher = new Fetcher<>(
            logContext,
            this.client,
            metadata,
            subscriptions,
            fetchConfig,
            deserializers,
            metricsManager,
            time,
            apiVersions
        );
        this.offsetFetcher = new OffsetFetcher(
            logContext,
            this.client,
            metadata,
            subscriptions,
            time,
            retryBackoffMs,
            requestTimeoutMs,
            isolationLevel,
            apiVersions
        );
        this.topicMetadataFetcher = new TopicMetadataFetcher(
            logContext,
            this.client,
            retryBackoffMs,
            retryBackoffMaxMs
        );
    }

    public Set<TopicPartition> assignment() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
        } finally {
            release();
        }
    }

    public Set<String> subscription() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.subscription()));
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        subscribeInternal(topics, Optional.of(listener));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribeInternal(topics, Optional.empty());
    }

    /**
     * Internal helper method for {@link #subscribe(Collection)} and
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}
     * <p>
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> It is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * @param topics The list of topics to subscribe to
     * @param listener {@link Optional} listener instance to get notifications on partition assignment/revocation
     *                 for the subscribed topics
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     * @throws IllegalStateException If {@code subscribe()} is called previously with pattern, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    private void subscribeInternal(Collection<String> topics, Optional<ConsumerRebalanceListener> listener) {
        acquireAndEnsureOpen();
        try {
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

                fetcher.clearBufferedDataForUnassignedPartitions(currentTopicPartitions);

                log.info("Subscribed to topic(s): {}", String.join(", ", topics));
                if (this.subscriptions.subscribe(new HashSet<>(topics), listener))
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        subscribeInternal(pattern, Optional.of(listener));
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribeInternal(pattern, Optional.empty());
    }

    /**
     * Internal helper method for {@link #subscribe(Pattern)} and
     * {@link #subscribe(Pattern, ConsumerRebalanceListener)}
     * <p>
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against all topics existing at the time of check.
     * This can be controlled through the {@code metadata.max.age.ms} configuration: by lowering
     * the max metadata age, the consumer will refresh metadata more often and check for matching topics.
     * <p>
     * See {@link #subscribe(Collection, ConsumerRebalanceListener)} for details on the
     * use of the {@link ConsumerRebalanceListener}. Generally rebalances are triggered when there
     * is a change to the topics matching the provided pattern and when consumer group membership changes.
     * Group rebalances only take place during an active call to {@link #poll(Duration)}.
     *
     * @param pattern Pattern to subscribe to
     * @param listener {@link Optional} listener instance to get notifications on partition assignment/revocation
     *                 for the subscribed topics
     * @throws IllegalArgumentException If pattern or listener is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with topics, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    private void subscribeInternal(Pattern pattern, Optional<ConsumerRebalanceListener> listener) {
        maybeThrowInvalidGroupIdException();
        if (pattern == null || pattern.toString().isEmpty())
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));

        acquireAndEnsureOpen();
        try {
            throwIfNoAssignorsConfigured();
            log.info("Subscribed to pattern: '{}'", pattern);
            this.subscriptions.subscribe(pattern, listener);
            this.coordinator.updatePatternSubscription(metadata.fetch());
            this.metadata.requestUpdateForNewTopics();
        } finally {
            release();
        }
    }

    public void unsubscribe() {
        acquireAndEnsureOpen();
        try {
            fetcher.clearBufferedDataForUnassignedPartitions(Collections.emptySet());
            if (this.coordinator != null) {
                this.coordinator.onLeavePrepare();
                this.coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics");
            }
            this.subscriptions.unsubscribe();
            log.info("Unsubscribed all topics or patterns and assigned partitions");
        } finally {
            release();
        }
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
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
                fetcher.clearBufferedDataForUnassignedPartitions(partitions);

                // make sure the offsets of topic partitions the consumer is unsubscribing from
                // are committed since there will be no following rebalance
                if (coordinator != null)
                    this.coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());

                log.info("Assigned to partition(s): {}", partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));
                if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
                    metadata.requestUpdateForNewTopics();
            }
        } finally {
            release();
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

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
        acquireAndEnsureOpen();
        try {
            this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            do {
                client.maybeTriggerWakeup();

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
                    if (sendFetches() > 0 || client.hasPendingRequests()) {
                        client.transmitSends();
                    }

                    if (fetch.records().isEmpty()) {
                        log.trace("Returning empty records from `poll()` "
                                + "since the consumer's position has advanced for at least one topic partition");
                    }

                    return this.interceptors.onConsume(new ConsumerRecords<>(fetch.records()));
                }
            } while (timer.notExpired());

            return ConsumerRecords.empty();
        } finally {
            release();
            this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
        }
    }

    private int sendFetches() {
        offsetFetcher.validatePositionsOnMetadataChange();
        return fetcher.sendFetches();
    }

    boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
        if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
            return false;
        }

        return updateFetchPositions(timer);
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private Fetch<K, V> pollForFetches(Timer timer) {
        long pollTimeout = coordinator == null ? timer.remainingMs() :
                Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());

        // if data is available already, return it immediately
        final Fetch<K, V> fetch = fetcher.collectFetch();
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // send any new fetches (won't resend pending fetches)
        sendFetches();

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        Timer pollTimer = time.timer(pollTimeout);
        client.poll(pollTimer, () -> {
            // since a fetch might be completed by the background thread, we need this poll condition
            // to ensure that we do not block unnecessarily in poll()
            return !fetcher.hasAvailableFetches();
        });
        timer.update(pollTimer.currentTimeMs());

        return fetcher.collectFetch();
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
        acquireAndEnsureOpen();
        long commitStart = time.nanoseconds();
        try {
            maybeThrowInvalidGroupIdException();
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            if (!coordinator.commitOffsetsSync(new HashMap<>(offsets), time.timer(timeout))) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before successfully " +
                        "committing offsets " + offsets);
            }
        } finally {
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart);
            release();
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
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            log.debug("Committing offsets: {}", offsets);
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            coordinator.commitOffsetsAsync(new HashMap<>(offsets), callback);
        } finally {
            release();
        }
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
                    this.metadata.currentLeader(partition));
            this.subscriptions.seekUnvalidated(partition, newPosition);
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
            Metadata.LeaderAndEpoch currentLeaderAndEpoch = this.metadata.currentLeader(partition);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    offsetAndMetadata.offset(),
                    offsetAndMetadata.leaderEpoch(),
                    currentLeaderAndEpoch);
            this.updateLastSeenEpochIfNewer(partition, offsetAndMetadata);
            this.subscriptions.seekUnvalidated(partition, newPosition);
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
            Collection<TopicPartition> parts = partitions.isEmpty() ? this.subscriptions.assignedPartitions() : partitions;
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
            Collection<TopicPartition> parts = partitions.isEmpty() ? this.subscriptions.assignedPartitions() : partitions;
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
    public long position(TopicPartition partition, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            if (!this.subscriptions.isAssigned(partition))
                throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");

            Timer timer = time.timer(timeout);
            do {
                SubscriptionState.FetchPosition position = this.subscriptions.validPosition(partition);
                if (position != null)
                    return position.offset;

                updateFetchPositions(timer);
                client.poll(timer);
            } while (timer.notExpired());

            throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                    "for partition " + partition + " could be determined");
        } finally {
            release();
        }
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
        acquireAndEnsureOpen();
        long start = time.nanoseconds();
        try {
            maybeThrowInvalidGroupIdException();
            final Map<TopicPartition, OffsetAndMetadata> offsets;
            offsets = coordinator.fetchCommittedOffsets(partitions, time.timer(timeout));
            if (offsets == null) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last " +
                        "committed offset for partitions " + partitions + " could be determined. Try tuning " +
                        ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG + " larger to relax the threshold.");
            } else {
                offsets.forEach(this::updateLastSeenEpochIfNewer);
                return offsets;
            }
        } finally {
            kafkaConsumerMetrics.recordCommitted(time.nanoseconds() - start);
            release();
        }
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        if (!clientTelemetryReporter.isPresent()) {
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");

        }

        return ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
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
        acquireAndEnsureOpen();
        try {
            Cluster cluster = this.metadata.fetch();
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            if (!parts.isEmpty())
                return parts;

            Timer timer = time.timer(timeout);
            List<PartitionInfo> topicMetadata = topicMetadataFetcher.getTopicMetadata(topic, metadata.allowAutoTopicCreation(), timer);
            return topicMetadata != null ? topicMetadata : Collections.emptyList();
        } finally {
            release();
        }
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return topicMetadataFetcher.getAllTopicMetadata(time.timer(timeout));
        } finally {
            release();
        }
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            log.debug("Pausing partitions {}", partitions);
            for (TopicPartition partition: partitions) {
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
            for (TopicPartition partition: partitions) {
                subscriptions.resume(partition);
            }
        } finally {
            release();
        }
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
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
                // we explicitly exclude the earliest and latest offset here so the timestamp in the returned
                // OffsetAndTimestamp is always positive.
                if (entry.getValue() < 0)
                    throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                            entry.getValue() + ". The target time cannot be negative.");
            }
            return offsetFetcher.offsetsForTimes(timestampsToSearch, time.timer(timeout));
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
        acquireAndEnsureOpen();
        try {
            return offsetFetcher.beginningOffsets(partitions, time.timer(timeout));
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return offsetFetcher.endOffsets(partitions, time.timer(timeout));
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
                    offsetFetcher.endOffsets(Collections.singleton(topicPartition), time.timer(0L));
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
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            return coordinator.groupMetadata();
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

    @Override
    public void enforceRebalance() {
        enforceRebalance(null);
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

    @Override
    public void wakeup() {
        this.client.wakeup();
    }

    private Timer createTimerForRequest(final Duration timeout) {
        // this.time could be null if an exception occurs in constructor prior to setting the this.time field
        final Time localTime = (time == null) ? Time.SYSTEM : time;
        return localTime.timer(Math.min(timeout.toMillis(), requestTimeoutMs));
    }

    private void close(Duration timeout, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        final Timer closeTimer = createTimerForRequest(timeout);
        clientTelemetryReporter.ifPresent(reporter -> reporter.initiateClose(timeout.toMillis()));
        closeTimer.update();
        // Close objects with a timeout. The timeout is required because the coordinator & the fetcher send requests to
        // the server in the process of closing which may not respect the overall timeout defined for closing the
        // consumer.
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

        closeQuietly(interceptors, "consumer interceptors", firstException);
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(client, "consumer network client", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);
        clientTelemetryReporter.ifPresent(reporter -> closeQuietly(reporter, "consumer telemetry reporter", firstException));
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
    private boolean updateFetchPositions(final Timer timer) {
        // If any partitions have been truncated due to a leader change, we need to validate the offsets
        offsetFetcher.validatePositionsIfNeeded();

        cachedSubscriptionHasAllFetchPositions = subscriptions.hasAllFetchPositions();
        if (cachedSubscriptionHasAllFetchPositions) return true;

        // If there are any partitions which do not have a valid position and are not
        // awaiting reset, then we need to fetch committed offsets. We will only do a
        // coordinator lookup if there are partitions which have missing positions, so
        // a consumer with manually assigned partitions can avoid a coordinator dependence
        // by always ensuring that assigned partitions have an initial position.
        if (coordinator != null && !coordinator.initWithCommittedOffsetsIfNeeded(timer)) return false;

        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise an exception.
        subscriptions.resetInitializingPositions();

        // Finally send an asynchronous request to look up and update the positions of any
        // partitions which are awaiting reset.
        offsetFetcher.resetPositionsIfNeeded();

        return true;
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

    private void updateLastSeenEpochIfNewer(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        if (offsetAndMetadata != null)
            offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
    }

    // Functions below are for testing only
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

    @Override
    public boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
        return updateAssignmentMetadataIfNeeded(timer, true);
    }
}
