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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.events.AllTopicsMetadataEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.AsyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CheckAndUpdatePositionsEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.CreateFetchRequestsEvent;
import org.apache.kafka.clients.consumer.internals.events.CurrentLagEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.EventProcessor;
import org.apache.kafka.clients.consumer.internals.events.FetchCommittedOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.LeaveGroupOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.PausePartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetOffsetEvent;
import org.apache.kafka.clients.consumer.internals.events.ResumePartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.SeekUnvalidatedEvent;
import org.apache.kafka.clients.consumer.internals.events.SyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicMetadataEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicPatternSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicRe2JPatternSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeEvent;
import org.apache.kafka.clients.consumer.internals.events.UpdatePatternSubscriptionEvent;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.internals.AbstractMembershipManager.TOPIC_PARTITION_COMPARATOR;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredConsumerInterceptors;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.swallow;

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

    /**
     * An {@link org.apache.kafka.clients.consumer.internals.events.EventProcessor} that is created and executes in the
     * application thread for the purpose of processing {@link BackgroundEvent background events} generated by the
     * {@link ConsumerNetworkThread network thread}.
     * Those events are generally of two types:
     *
     * <ul>
     *     <li>Errors that occur in the network thread that need to be propagated to the application thread</li>
     *     <li>{@link ConsumerRebalanceListener} callbacks that are to be executed on the application thread</li>
     * </ul>
     */
    private class BackgroundEventProcessor implements EventProcessor<BackgroundEvent> {

        @Override
        public void process(final BackgroundEvent event) {
            switch (event.type()) {
                case ERROR:
                    process((ErrorEvent) event);
                    break;

                case CONSUMER_REBALANCE_LISTENER_CALLBACK_NEEDED:
                    process((ConsumerRebalanceListenerCallbackNeededEvent) event);
                    break;

                default:
                    throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");

            }
        }

        private void process(final ErrorEvent event) {
            throw event.error();
        }

        private void process(final ConsumerRebalanceListenerCallbackNeededEvent event) {
            ConsumerRebalanceListenerCallbackCompletedEvent invokedEvent = invokeRebalanceCallbacks(
                rebalanceListenerInvoker,
                event.methodName(),
                event.partitions(),
                event.future()
            );
            applicationEventHandler.add(invokedEvent);
            if (invokedEvent.error().isPresent()) {
                throw invokedEvent.error().get();
            }
        }
    }

    private final ApplicationEventHandler applicationEventHandler;
    private final Time time;
    private final AtomicReference<Optional<ConsumerGroupMetadata>> groupMetadata = new AtomicReference<>(Optional.empty());
    private final KafkaConsumerMetrics kafkaConsumerMetrics;
    private Logger log;
    private final String clientId;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final BackgroundEventProcessor backgroundEventProcessor;
    private final CompletableEventReaper backgroundEventReaper;
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

    /**
     * This is a snapshot of the partitions assigned to this consumer. HOWEVER, this is only populated and used in
     * the case where this consumer is in a consumer group. Self-assigned partitions do not appear here.
     */
    private final AtomicReference<Set<TopicPartition>> groupAssignmentSnapshot = new AtomicReference<>(Collections.emptySet());
    private final ConsumerMetadata metadata;
    private final Metrics metrics;
    private final long retryBackoffMs;
    private final Duration defaultApiTimeoutMs;
    private final boolean autoCommitEnabled;
    private volatile boolean closed = false;
    // Init value is needed to avoid NPE in case of exception raised in the constructor
    private Optional<ClientTelemetryReporter> clientTelemetryReporter = Optional.empty();

    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
    private boolean cachedSubscriptionHasAllFetchPositions;
    private final WakeupTrigger wakeupTrigger = new WakeupTrigger();
    private final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker;
    private final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker;
    // Last triggered async commit future. Used to wait until all previous async commits are completed.
    // We only need to keep track of the last one, since they are guaranteed to complete in order.
    private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> lastPendingAsyncCommit = null;

    // currentThread holds the threadId of the current thread accessing the AsyncKafkaConsumer
    // and is used to prevent multithreaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    private final AtomicInteger refCount = new AtomicInteger(0);

    private final MemberStateListener memberStateListener = new MemberStateListener() {
        @Override
        public void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId) {
            updateGroupMetadata(memberEpoch, memberId);
        }

        @Override
        public void onGroupAssignmentUpdated(Set<TopicPartition> partitions) {
            setGroupAssignmentSnapshot(partitions);
        }
    };

    AsyncKafkaConsumer(final ConsumerConfig config,
                       final Deserializer<K> keyDeserializer,
                       final Deserializer<V> valueDeserializer) {
        this(
            config,
            keyDeserializer,
            valueDeserializer,
            Time.SYSTEM,
            ApplicationEventHandler::new,
            CompletableEventReaper::new,
            FetchCollector::new,
            ConsumerMetadata::new,
            new LinkedBlockingQueue<>()
        );
    }

    // Visible for testing
    AsyncKafkaConsumer(final ConsumerConfig config,
                       final Deserializer<K> keyDeserializer,
                       final Deserializer<V> valueDeserializer,
                       final Time time,
                       final ApplicationEventHandlerFactory applicationEventHandlerFactory,
                       final CompletableEventReaperFactory backgroundEventReaperFactory,
                       final FetchCollectorFactory<K, V> fetchCollectorFactory,
                       final ConsumerMetadataFactory metadataFactory,
                       final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue) {
        try {
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
                config,
                GroupRebalanceConfig.ProtocolType.CONSUMER
            );
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
            this.autoCommitEnabled = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
            LogContext logContext = createLogContext(config, groupRebalanceConfig);
            this.backgroundEventQueue = backgroundEventQueue;
            this.log = logContext.logger(getClass());

            log.debug("Initializing the Kafka consumer");
            this.defaultApiTimeoutMs = Duration.ofMillis(config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
            this.time = time;
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            this.clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            this.clientTelemetryReporter.ifPresent(reporters::add);
            this.metrics = createMetrics(config, time, reporters);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

            List<ConsumerInterceptor<K, V>> interceptorList = configuredConsumerInterceptors(config);
            this.interceptors = new ConsumerInterceptors<>(interceptorList);
            this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
            this.subscriptions = createSubscriptionState(config, logContext);
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(metrics.reporters(),
                    interceptorList,
                    Arrays.asList(deserializers.keyDeserializer, deserializers.valueDeserializer));
            this.metadata = metadataFactory.build(config, subscriptions, logContext, clusterResourceListeners);
            final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            metadata.bootstrap(addresses);

            FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);
            FetchConfig fetchConfig = new FetchConfig(config);
            this.isolationLevel = fetchConfig.isolationLevel;

            ApiVersions apiVersions = new ApiVersions();
            final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
            final BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(backgroundEventQueue);

            // This FetchBuffer is shared between the application and network threads.
            this.fetchBuffer = new FetchBuffer(logContext);
            final Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(time,
                    logContext,
                    metadata,
                    config,
                    apiVersions,
                    metrics,
                    fetchMetricsManager.throttleTimeSensor(),
                    clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null),
                    backgroundEventHandler);
            this.offsetCommitCallbackInvoker = new OffsetCommitCallbackInvoker(interceptors);
            this.groupMetadata.set(initializeGroupMetadata(config, groupRebalanceConfig));
            final Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(time,
                    logContext,
                    backgroundEventHandler,
                    metadata,
                    subscriptions,
                    fetchBuffer,
                    config,
                    groupRebalanceConfig,
                    apiVersions,
                    fetchMetricsManager,
                    networkClientDelegateSupplier,
                    clientTelemetryReporter,
                    metrics,
                    offsetCommitCallbackInvoker,
                    memberStateListener
            );
            final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(logContext,
                    metadata,
                    subscriptions,
                    requestManagersSupplier);
            this.applicationEventHandler = applicationEventHandlerFactory.build(
                    logContext,
                    time,
                    applicationEventQueue,
                    new CompletableEventReaper(logContext),
                    applicationEventProcessorSupplier,
                    networkClientDelegateSupplier,
                    requestManagersSupplier);

            this.rebalanceListenerInvoker = new ConsumerRebalanceListenerInvoker(
                    logContext,
                    subscriptions,
                    time,
                    new RebalanceCallbackMetricsManager(metrics)
            );
            this.backgroundEventProcessor = new BackgroundEventProcessor();
            this.backgroundEventReaper = backgroundEventReaperFactory.build(logContext);

            // The FetchCollector is only used on the application thread.
            this.fetchCollector = fetchCollectorFactory.build(logContext,
                    metadata,
                    subscriptions,
                    fetchConfig,
                    deserializers,
                    fetchMetricsManager,
                    time);

            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_METRIC_GROUP_PREFIX);

            if (groupMetadata.get().isPresent() &&
                GroupProtocol.of(config.getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG)) == GroupProtocol.CONSUMER) {
                config.ignore(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG); // Used by background thread
            }
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
                       CompletableEventReaper backgroundEventReaper,
                       ConsumerRebalanceListenerInvoker rebalanceListenerInvoker,
                       Metrics metrics,
                       SubscriptionState subscriptions,
                       ConsumerMetadata metadata,
                       long retryBackoffMs,
                       int defaultApiTimeoutMs,
                       String groupId,
                       boolean autoCommitEnabled) {
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.clientId = clientId;
        this.fetchBuffer = fetchBuffer;
        this.fetchCollector = fetchCollector;
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        this.interceptors = Objects.requireNonNull(interceptors);
        this.time = time;
        this.backgroundEventQueue = backgroundEventQueue;
        this.rebalanceListenerInvoker = rebalanceListenerInvoker;
        this.backgroundEventProcessor = new BackgroundEventProcessor();
        this.backgroundEventReaper = backgroundEventReaper;
        this.metrics = metrics;
        this.groupMetadata.set(initializeGroupMetadata(groupId, Optional.empty()));
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.defaultApiTimeoutMs = Duration.ofMillis(defaultApiTimeoutMs);
        this.deserializers = deserializers;
        this.applicationEventHandler = applicationEventHandler;
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, "consumer");
        this.clientTelemetryReporter = Optional.empty();
        this.autoCommitEnabled = autoCommitEnabled;
        this.offsetCommitCallbackInvoker = new OffsetCommitCallbackInvoker(interceptors);
    }

    AsyncKafkaConsumer(LogContext logContext,
                       Time time,
                       ConsumerConfig config,
                       Deserializer<K> keyDeserializer,
                       Deserializer<V> valueDeserializer,
                       KafkaClient client,
                       SubscriptionState subscriptions,
                       ConsumerMetadata metadata) {
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
        this.autoCommitEnabled = config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        this.fetchBuffer = new FetchBuffer(logContext);
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        this.interceptors = new ConsumerInterceptors<>(Collections.emptyList());
        this.time = time;
        this.metrics = new Metrics(time);
        this.metadata = metadata;
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.defaultApiTimeoutMs = Duration.ofMillis(config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
        this.deserializers = new Deserializers<>(keyDeserializer, valueDeserializer);
        this.clientTelemetryReporter = Optional.empty();

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

        this.groupMetadata.set(initializeGroupMetadata(config, groupRebalanceConfig));

        BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventQueue = new LinkedBlockingQueue<>();
        BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(backgroundEventQueue);
        this.rebalanceListenerInvoker = new ConsumerRebalanceListenerInvoker(
            logContext,
            subscriptions,
            time,
            new RebalanceCallbackMetricsManager(metrics)
        );
        ApiVersions apiVersions = new ApiVersions();
        Supplier<NetworkClientDelegate> networkClientDelegateSupplier = () -> new NetworkClientDelegate(
            time,
            config,
            logContext,
            client,
            metadata,
            backgroundEventHandler
        );
        this.offsetCommitCallbackInvoker = new OffsetCommitCallbackInvoker(interceptors);
        Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(
            time,
            logContext,
            backgroundEventHandler,
            metadata,
            subscriptions,
            fetchBuffer,
            config,
            groupRebalanceConfig,
            apiVersions,
            fetchMetricsManager,
            networkClientDelegateSupplier,
            clientTelemetryReporter,
            metrics,
            offsetCommitCallbackInvoker,
            memberStateListener
        );
        Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(
                logContext,
                metadata,
                subscriptions,
                requestManagersSupplier
        );
        this.applicationEventHandler = new ApplicationEventHandler(logContext,
                time,
                applicationEventQueue,
                new CompletableEventReaper(logContext),
                applicationEventProcessorSupplier,
                networkClientDelegateSupplier,
                requestManagersSupplier);
        this.backgroundEventProcessor = new BackgroundEventProcessor();
        this.backgroundEventReaper = new CompletableEventReaper(logContext);
    }

    // auxiliary interface for testing
    interface ApplicationEventHandlerFactory {

        ApplicationEventHandler build(
            final LogContext logContext,
            final Time time,
            final BlockingQueue<ApplicationEvent> applicationEventQueue,
            final CompletableEventReaper applicationEventReaper,
            final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
            final Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
            final Supplier<RequestManagers> requestManagersSupplier
        );

    }

    // auxiliary interface for testing
    interface CompletableEventReaperFactory {

        CompletableEventReaper build(final LogContext logContext);

    }

    // auxiliary interface for testing
    interface FetchCollectorFactory<K, V> {

        FetchCollector<K, V> build(
            final LogContext logContext,
            final ConsumerMetadata metadata,
            final SubscriptionState subscriptions,
            final FetchConfig fetchConfig,
            final Deserializers<K, V> deserializers,
            final FetchMetricsManager metricsManager,
            final Time time
        );

    }

    // auxiliary interface for testing
    interface ConsumerMetadataFactory {

        ConsumerMetadata build(
            final ConsumerConfig config,
            final SubscriptionState subscriptions,
            final LogContext logContext,
            final ClusterResourceListeners clusterResourceListeners
        );

    }

    private Optional<ConsumerGroupMetadata> initializeGroupMetadata(final ConsumerConfig config,
                                                                    final GroupRebalanceConfig groupRebalanceConfig) {
        final Optional<ConsumerGroupMetadata> groupMetadata = initializeGroupMetadata(
            groupRebalanceConfig.groupId,
            groupRebalanceConfig.groupInstanceId
        );
        if (groupMetadata.isEmpty()) {
            config.ignore(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
            config.ignore(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
        }
        return groupMetadata;
    }

    private Optional<ConsumerGroupMetadata> initializeGroupMetadata(final String groupId,
                                                                    final Optional<String> groupInstanceId) {
        if (groupId != null) {
            if (groupId.isEmpty()) {
                throw new InvalidGroupIdException("The configured " + ConsumerConfig.GROUP_ID_CONFIG
                    + " should not be an empty string or whitespace.");
            } else {
                return Optional.of(initializeConsumerGroupMetadata(groupId, groupInstanceId));
            }
        }
        return Optional.empty();
    }

    private ConsumerGroupMetadata initializeConsumerGroupMetadata(final String groupId,
                                                                  final Optional<String> groupInstanceId) {
        return new ConsumerGroupMetadata(
            groupId,
            JoinGroupRequest.UNKNOWN_GENERATION_ID,
            JoinGroupRequest.UNKNOWN_MEMBER_ID,
            groupInstanceId
        );
    }

    private void updateGroupMetadata(final Optional<Integer> memberEpoch, final String memberId) {
        memberEpoch.ifPresent(epoch -> groupMetadata.updateAndGet(
                oldGroupMetadataOptional -> oldGroupMetadataOptional.map(
                    oldGroupMetadata -> new ConsumerGroupMetadata(
                        oldGroupMetadata.groupId(),
                        memberEpoch.orElse(oldGroupMetadata.generationId()),
                        memberId,
                        oldGroupMetadata.groupInstanceId()
                    )
                )
            )
        );
    }

    void setGroupAssignmentSnapshot(final Set<TopicPartition> partitions) {
        groupAssignmentSnapshot.set(Collections.unmodifiableSet(partitions));
    }

    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        if (!metrics().containsKey(metric.metricName())) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricChange(metric));
        } else {
            log.debug("Skipping registration for metric {}. Existing consumer metrics cannot be overwritten.", metric.metricName());
        }
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        if (!metrics().containsKey(metric.metricName())) {
            clientTelemetryReporter.ifPresent(reporter -> reporter.metricRemoval(metric));
        } else {
            log.debug("Skipping unregistration for metric {}. Existing consumer metrics cannot be removed.", metric.metricName());
        }
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
     *
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.RecordTooLargeException if the fetched record is larger than the maximum
     *             allowable size
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws java.lang.IllegalStateException if the consumer is not subscribed to any topics or manually assigned any
     *             partitions to consume from or an unexpected error occurred
     * @throws org.apache.kafka.clients.consumer.OffsetOutOfRangeException if the fetch position of the consumer is
     *             out of range and no offset reset policy is configured.
     * @throws org.apache.kafka.common.errors.TopicAuthorizationException if the consumer is not authorized to read
     *             from a partition
     * @throws org.apache.kafka.common.errors.SerializationException if the fetched records cannot be deserialized
     * @throws org.apache.kafka.common.errors.UnsupportedAssignorException if the `group.remote.assignor` configuration
     *             is set to an assignor that is not available on the broker.
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

                // Make sure to let the background thread know that we are still polling.
                applicationEventHandler.add(new PollEvent(timer.currentTimeMs()));

                // We must not allow wake-ups between polling for fetches and returning the records.
                // If the polled fetches are not empty the consumed position has already been updated in the polling
                // of the fetches. A wakeup between returned fetches and returning records would lead to never
                // returning the records in the fetches. Thus, we trigger a possible wake-up before we poll fetches.
                wakeupTrigger.maybeTriggerWakeup();

                updateAssignmentMetadataIfNeeded(timer);
                final Fetch<K, V> fetch = pollForFetches(timer);
                if (!fetch.isEmpty()) {
                    // before returning the fetched records, we can send off the next round of fetches
                    // and avoid block waiting for their responses to enable pipelining while the user
                    // is handling the fetched records.
                    //
                    // NOTE: since the consumed position has already been updated, we must not allow
                    // wakeups or any other errors to be triggered prior to returning the fetched records.
                    sendPrefetches(timer);

                    if (fetch.records().isEmpty()) {
                        log.trace("Returning empty records from `poll()` "
                            + "since the consumer's position has advanced for at least one topic partition");
                    }

                    return interceptors.onConsume(new ConsumerRecords<>(fetch.records(), fetch.nextOffsets()));
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
        commitSync(defaultApiTimeoutMs);
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
        commitAsync(Optional.empty(), callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        commitAsync(Optional.of(offsets), callback);
    }

    private void commitAsync(Optional<Map<TopicPartition, OffsetAndMetadata>> offsets, OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            AsyncCommitEvent asyncCommitEvent = new AsyncCommitEvent(offsets);
            lastPendingAsyncCommit = commit(asyncCommitEvent).whenComplete((committedOffsets, throwable) -> {

                if (throwable == null) {
                    offsetCommitCallbackInvoker.enqueueInterceptorInvocation(committedOffsets);
                }

                if (callback == null) {
                    if (throwable != null) {
                        log.error("Offset commit with offsets {} failed", committedOffsets, throwable);
                    }
                    return;
                }

                offsetCommitCallbackInvoker.enqueueUserCallbackInvocation(callback, committedOffsets, (Exception) throwable);
            });
        } finally {
            release();
        }
    }

    private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commit(final CommitEvent commitEvent) {
        maybeThrowInvalidGroupIdException();
        offsetCommitCallbackInvoker.executeCallbacks();

        if (commitEvent.offsets().isPresent() && commitEvent.offsets().get().isEmpty()) {
            return CompletableFuture.completedFuture(null);
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
            SeekUnvalidatedEvent seekUnvalidatedEventEvent = new SeekUnvalidatedEvent(
                defaultApiTimeoutDeadlineMs(),
                partition,
                offset,
                Optional.empty()
            );
            applicationEventHandler.addAndGet(seekUnvalidatedEventEvent);
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

            applicationEventHandler.addAndGet(new SeekUnvalidatedEvent(
                defaultApiTimeoutDeadlineMs(),
                partition,
                offsetAndMetadata.offset(),
                offsetAndMetadata.leaderEpoch()
            ));
        } finally {
            release();
        }
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        seek(partitions, AutoOffsetResetStrategy.EARLIEST);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        seek(partitions, AutoOffsetResetStrategy.LATEST);
    }

    private void seek(Collection<TopicPartition> partitions, AutoOffsetResetStrategy offsetResetStrategy) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        acquireAndEnsureOpen();
        try {
            applicationEventHandler.addAndGet(new ResetOffsetEvent(
                partitions,
                offsetResetStrategy,
                defaultApiTimeoutDeadlineMs())
            );
        } finally {
            release();
        }
    }

    @Override
    public long position(TopicPartition partition) {
        return position(partition, defaultApiTimeoutMs);
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
                timer.update();
                wakeupTrigger.maybeTriggerWakeup();
            } while (timer.notExpired());

            throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                "for partition " + partition + " could be determined");
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, defaultApiTimeoutMs);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
                                                            final Duration timeout) {
        acquireAndEnsureOpen();
        long start = time.nanoseconds();
        try {
            maybeThrowInvalidGroupIdException();
            if (partitions.isEmpty()) {
                return Collections.emptyMap();
            }

            final FetchCommittedOffsetsEvent event = new FetchCommittedOffsetsEvent(
                partitions,
                calculateDeadlineMs(time, timeout));
            wakeupTrigger.setActiveTask(event.future());
            try {
                return applicationEventHandler.addAndGet(event);
            } catch (TimeoutException e) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last " +
                    "committed offset for partitions " + partitions + " could be determined. Try tuning " +
                    ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG + " larger to relax the threshold.");
            } finally {
                wakeupTrigger.clearTask();
            }
        } finally {
            kafkaConsumerMetrics.recordCommitted(time.nanoseconds() - start);
            release();
        }
    }

    private void maybeThrowInvalidGroupIdException() {
        if (groupMetadata.get().isEmpty()) {
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
        return partitionsFor(topic, defaultApiTimeoutMs);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            Cluster cluster = this.metadata.fetch();
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            if (!parts.isEmpty())
                return parts;

            if (timeout.toMillis() == 0L) {
                throw new TimeoutException();
            }

            final TopicMetadataEvent topicMetadataEvent = new TopicMetadataEvent(topic, calculateDeadlineMs(time, timeout));
            wakeupTrigger.setActiveTask(topicMetadataEvent.future());
            try {
                Map<String, List<PartitionInfo>> topicMetadata =
                        applicationEventHandler.addAndGet(topicMetadataEvent);

                return topicMetadata.getOrDefault(topic, Collections.emptyList());
            } finally {
                wakeupTrigger.clearTask();
            }
        } finally {
            release();
        }
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(defaultApiTimeoutMs);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            if (timeout.toMillis() == 0L) {
                throw new TimeoutException();
            }

            final AllTopicsMetadataEvent topicMetadataEvent = new AllTopicsMetadataEvent(calculateDeadlineMs(time, timeout));
            wakeupTrigger.setActiveTask(topicMetadataEvent.future());
            try {
                return applicationEventHandler.addAndGet(topicMetadataEvent);
            } finally {
                wakeupTrigger.clearTask();
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
    public void pause(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            Objects.requireNonNull(partitions, "The partitions to pause must be nonnull");

            if (!partitions.isEmpty())
                applicationEventHandler.addAndGet(new PausePartitionsEvent(partitions, defaultApiTimeoutDeadlineMs()));
        } finally {
            release();
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            Objects.requireNonNull(partitions, "The partitions to resume must be nonnull");

            if (!partitions.isEmpty())
                applicationEventHandler.addAndGet(new ResumePartitionsEvent(partitions, defaultApiTimeoutDeadlineMs()));
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, defaultApiTimeoutMs);
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
            ListOffsetsEvent listOffsetsEvent = new ListOffsetsEvent(
                    timestampsToSearch,
                    calculateDeadlineMs(time, timeout),
                    true);

            // If timeout is set to zero return empty immediately; otherwise try to get the results
            // and throw timeout exception if it cannot complete in time.
            if (timeout.toMillis() == 0L) {
                applicationEventHandler.add(listOffsetsEvent);
                return listOffsetsEvent.emptyResults();
            }

            try {
                Map<TopicPartition, OffsetAndTimestampInternal> offsets = applicationEventHandler.addAndGet(listOffsetsEvent);
                Map<TopicPartition, OffsetAndTimestamp> results = new HashMap<>(offsets.size());
                offsets.forEach((k, v) -> results.put(k, v != null ? v.buildOffsetAndTimestamp() : null));
                return results;
            } catch (TimeoutException e) {
                throw new TimeoutException("Failed to get offsets by times in " + timeout.toMillis() + "ms");
            }
        } finally {
            release();
        }
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, defaultApiTimeoutMs);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, defaultApiTimeoutMs);
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
            ListOffsetsEvent listOffsetsEvent = new ListOffsetsEvent(
                    timestampToSearch,
                    calculateDeadlineMs(time, timeout),
                    false);

            // If timeout is set to zero return empty immediately; otherwise try to get the results
            // and throw timeout exception if it cannot complete in time.
            if (timeout.isZero()) {
                applicationEventHandler.add(listOffsetsEvent);
                return listOffsetsEvent.emptyResults();
            }

            Map<TopicPartition, OffsetAndTimestampInternal> offsetAndTimestampMap;
            try {
                offsetAndTimestampMap = applicationEventHandler.addAndGet(listOffsetsEvent);
                return offsetAndTimestampMap.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().offset()));
            } catch (TimeoutException e) {
                throw new TimeoutException("Failed to get offsets by times in " + timeout.toMillis() + "ms");
            }
        } finally {
            release();
        }
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        acquireAndEnsureOpen();
        try {
            return applicationEventHandler.addAndGet(new CurrentLagEvent(
                topicPartition,
                isolationLevel,
                defaultApiTimeoutDeadlineMs()
            ));
        } finally {
            release();
        }
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            return groupMetadata.get().get();
        } finally {
            release();
        }
    }

    @Override
    public void enforceRebalance() {
        log.warn("Operation not supported in new consumer group protocol");
    }

    @Override
    public void enforceRebalance(String reason) {
        log.warn("Operation not supported in new consumer group protocol");
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

    /**
     * Please keep these tenets in mind for the implementation of the {@link AsyncKafkaConsumer}’s
     * {@link #close(Duration)} method. In the future, these tenets may be made officially part of the top-level
     * {@link KafkaConsumer#close(Duration)} API, but for now they remain here.
     *
     * <ol>
     *     <li>
     *         The execution of the {@link ConsumerRebalanceListener} callback (if applicable) must be performed on
     *         the application thread to ensure it does not interfere with the network I/O on the background thread.
     *     </li>
     *     <li>
     *         The {@link ConsumerRebalanceListener} callback execution must complete before an attempt to leave
     *         the consumer group is performed. In this context, “complete” does not necessarily imply
     *         <em>success</em>; execution is “complete” even if the execution <em>fails</em> with an error.
     *     </li>
     *     <li>
     *         Any error thrown during the {@link ConsumerRebalanceListener} callback execution will be caught to
     *         ensure it does not prevent execution of the remaining {@link #close()} logic.
     *     </li>
     *     <li>
     *         The application thread will be blocked during the entire duration of the execution of the
     *         {@link ConsumerRebalanceListener}. The consumer does not employ a mechanism to short-circuit the
     *         callback execution, so execution is not bound by the timeout in {@link #close(Duration)}.
     *     </li>
     *     <li>
     *         A given {@link ConsumerRebalanceListener} implementation may be affected by the application thread's
     *         interrupt state. If the callback implementation performs any blocking operations, it may result in
     *         an error. An implementation may choose to preemptively check the thread's interrupt flag via
     *         {@link Thread#isInterrupted()} or {@link Thread#isInterrupted()} and alter its behavior.
     *     </li>
     *     <li>
     *         If the application thread was interrupted <em>prior</em> to the execution of the
     *         {@link ConsumerRebalanceListener} callback, the thread's interrupt state will be preserved for the
     *         {@link ConsumerRebalanceListener} execution.
     *     </li>
     *     <li>
     *         If the application thread was interrupted <em>prior</em> to the execution of the
     *         {@link ConsumerRebalanceListener} callback <em>but</em> the callback cleared out the interrupt state,
     *         the {@link #close()} method will not make any effort to restore the application thread's interrupt
     *         state for the remainder of the execution of {@link #close()}.
     *     </li>
     *     <li>
     *         Leaving the consumer group is achieved by issuing a ‘leave group‘ network request. The consumer will
     *         attempt to leave the group on a “best-case” basis. There is no stated guarantee that the consumer will
     *         have successfully left the group before the {@link #close()} method completes processing.
     *     </li>
     *     <li>
     *         The consumer will attempt to leave the group regardless of the timeout elapsing or the application
     *         thread receiving an {@link InterruptException} or {@link InterruptedException}.
     *     </li>
     *     <li>
     *         The application thread will wait for confirmation that the consumer left the group until one of the
     *         following occurs:
     *
     *         <ol>
     *             <li>Confirmation that the ’leave group‘ response was received from the group coordinator</li>
     *             <li>The timeout provided by the user elapses</li>
     *             <li>An {@link InterruptException} or {@link InterruptedException} is thrown</li>
     *         </ol>
     *     </li>
     * </ol>
     */
    private void close(Duration timeout, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        // We are already closing with a timeout, don't allow wake-ups from here on.
        wakeupTrigger.disableWakeups();

        final Timer closeTimer = time.timer(timeout);
        clientTelemetryReporter.ifPresent(ClientTelemetryReporter::initiateClose);
        closeTimer.update();
        // Prepare shutting down the network thread
        // Prior to closing the network thread, we need to make sure the following operations happen in the right
        // sequence...
        swallow(log, Level.ERROR, "Failed to auto-commit offsets",
            () -> autoCommitOnClose(closeTimer), firstException);
        swallow(log, Level.ERROR, "Failed to release group assignment",
            () -> runRebalanceCallbacksOnClose(closeTimer), firstException);
        swallow(log, Level.ERROR, "Failed to leave group while closing consumer",
            () -> leaveGroupOnClose(closeTimer), firstException);
        swallow(log, Level.ERROR, "Failed invoking asynchronous commit callbacks while closing consumer",
            () -> awaitPendingAsyncCommitsAndExecuteCommitCallbacks(closeTimer, false), firstException);
        if (applicationEventHandler != null)
            closeQuietly(() -> applicationEventHandler.close(Duration.ofMillis(closeTimer.remainingMs())), "Failed shutting down network thread", firstException);
        closeTimer.update();

        // close() can be called from inside one of the constructors. In that case, it's possible that neither
        // the reaper nor the background event queue were constructed, so check them first to avoid NPE.
        if (backgroundEventReaper != null && backgroundEventQueue != null)
            backgroundEventReaper.reap(backgroundEventQueue);

        closeQuietly(interceptors, "consumer interceptors", firstException);
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);
        clientTelemetryReporter.ifPresent(reporter -> closeQuietly(reporter, "async consumer telemetry reporter", firstException));

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

    private void autoCommitOnClose(final Timer timer) {
        if (groupMetadata.get().isEmpty())
            return;

        if (autoCommitEnabled)
            commitSyncAllConsumed(timer);

        applicationEventHandler.add(new CommitOnCloseEvent());
    }

    private void runRebalanceCallbacksOnClose(final Timer timer) {
        if (groupMetadata.get().isEmpty())
            return;

        int memberEpoch = groupMetadata.get().get().generationId();

        Set<TopicPartition> assignedPartitions = groupAssignmentSnapshot.get();

        if (assignedPartitions.isEmpty())
            // Nothing to revoke.
            return;

        SortedSet<TopicPartition> droppedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        droppedPartitions.addAll(assignedPartitions);

        try {
            final Exception error;

            if (memberEpoch > 0)
                error = rebalanceListenerInvoker.invokePartitionsRevoked(droppedPartitions);
            else
                error = rebalanceListenerInvoker.invokePartitionsLost(droppedPartitions);

            if (error != null)
                throw ConsumerUtils.maybeWrapAsKafkaException(error);
        } finally {
            timer.update();
        }
    }

    private void leaveGroupOnClose(final Timer timer) {
        if (groupMetadata.get().isEmpty())
            return;

        log.debug("Leaving the consumer group during consumer close");
        try {
            applicationEventHandler.addAndGet(new LeaveGroupOnCloseEvent(calculateDeadlineMs(timer)));
            log.info("Completed leaving the group");
        } catch (TimeoutException e) {
            log.warn("Consumer attempted to leave the group but couldn't " +
                "complete it within {} ms. It will proceed to close.", timer.timeoutMs());
        } finally {
            timer.update();
        }
    }

    // Visible for testing
    void commitSyncAllConsumed(final Timer timer) {
        log.debug("Sending synchronous auto-commit on closing");
        try {
            commitSync(Duration.ofMillis(timer.remainingMs()));
        } catch (Exception e) {
            // consistent with async auto-commit failures, we do not propagate the exception
            log.warn("Synchronous auto-commit failed", e);
        }
        timer.update();
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
        commitSync(Optional.empty(), timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(Optional.of(offsets), defaultApiTimeoutMs);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        commitSync(Optional.of(offsets), timeout);
    }

    private void commitSync(Optional<Map<TopicPartition, OffsetAndMetadata>> offsets, Duration timeout) {
        acquireAndEnsureOpen();
        long commitStart = time.nanoseconds();
        try {
            SyncCommitEvent syncCommitEvent = new SyncCommitEvent(offsets, calculateDeadlineMs(time, timeout));
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitFuture = commit(syncCommitEvent);

            Timer requestTimer = time.timer(timeout.toMillis());
            awaitPendingAsyncCommitsAndExecuteCommitCallbacks(requestTimer, true);

            wakeupTrigger.setActiveTask(commitFuture);
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = ConsumerUtils.getResult(commitFuture, requestTimer);
            interceptors.onCommit(committedOffsets);
        } finally {
            wakeupTrigger.clearTask();
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart);
            release();
        }
    }

    private void awaitPendingAsyncCommitsAndExecuteCommitCallbacks(Timer timer, boolean enableWakeup) {
        if (lastPendingAsyncCommit == null) {
            return;
        }

        try {
            final CompletableFuture<Void> futureToAwait = new CompletableFuture<>();
            // We don't want the wake-up trigger to complete our pending async commit future,
            // so create new future here. Any errors in the pending async commit will be handled
            // by the async commit future / the commit callback - here, we just want to wait for it to complete.
            lastPendingAsyncCommit.whenComplete((v, t) -> futureToAwait.complete(null));
            if (enableWakeup) {
                wakeupTrigger.setActiveTask(futureToAwait);
            }
            ConsumerUtils.getResult(futureToAwait, timer);
            lastPendingAsyncCommit = null;
        } finally {
            if (enableWakeup) {
                wakeupTrigger.clearTask();
            }
            timer.update();
        }
        offsetCommitCallbackInvoker.executeCallbacks();
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        if (clientTelemetryReporter.isEmpty()) {
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");
        }

        return ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
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
            applicationEventHandler.addAndGet(new AssignmentChangeEvent(
                time.milliseconds(),
                defaultApiTimeoutDeadlineMs(),
                partitions
            ));
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe() {
        acquireAndEnsureOpen();
        try {
            fetchBuffer.retainAll(Collections.emptySet());
            Timer timer = time.timer(defaultApiTimeoutMs);
            UnsubscribeEvent unsubscribeEvent = new UnsubscribeEvent(calculateDeadlineMs(timer));
            applicationEventHandler.add(unsubscribeEvent);
            log.info("Unsubscribing all topics or patterns and assigned partitions {}",
                    subscriptions.assignedPartitions());

            try {
                // If users subscribe to a topic with invalid name or without permission, they will get some exceptions.
                // Because network thread keeps trying to send MetadataRequest or ConsumerGroupHeartbeatRequest in the background,
                // there will be some error events in the background queue.
                // When running unsubscribe, these exceptions should be ignored, or users can't unsubscribe successfully.
                processBackgroundEvents(unsubscribeEvent.future(), timer,
                    e -> e instanceof InvalidTopicException || e instanceof TopicAuthorizationException || e instanceof GroupAuthorizationException);
                log.info("Unsubscribed all topics or patterns and assigned partitions");
            } catch (TimeoutException e) {
                log.error("Failed while waiting for the unsubscribe event to complete");
            }
            resetGroupMetadata();
        } catch (Exception e) {
            log.error("Unsubscribe failed", e);
            throw e;
        } finally {
            release();
        }
    }

    private void resetGroupMetadata() {
        groupMetadata.updateAndGet(
            oldGroupMetadataOptional -> oldGroupMetadataOptional
                .map(oldGroupMetadata -> initializeConsumerGroupMetadata(
                    oldGroupMetadata.groupId(),
                    oldGroupMetadata.groupInstanceId()
                ))
        );
    }

    // Visible for testing
    WakeupTrigger wakeupTrigger() {
        return wakeupTrigger;
    }

    private Fetch<K, V> pollForFetches(Timer timer) {
        long pollTimeout = isCommittedOffsetsManagementEnabled()
                ? Math.min(applicationEventHandler.maximumTimeToWait(), timer.remainingMs())
                : timer.remainingMs();

        // if data is available already, return it immediately
        final Fetch<K, V> fetch = collectFetch();
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // send any new fetches (won't resend pending fetches)
        sendFetches(timer);

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        Timer pollTimer = time.timer(pollTimeout);
        wakeupTrigger.setFetchAction(fetchBuffer);

        // Wait a bit for some fetched data to arrive, as there may not be anything immediately available. Note the
        // use of a shorter, dedicated "pollTimer" here which updates "timer" so that calling method (poll) will
        // correctly handle the overall timeout.
        try {
            fetchBuffer.awaitNotEmpty(pollTimer);
        } catch (InterruptException e) {
            log.trace("Interrupt during fetch", e);
            throw e;
        } finally {
            timer.update(pollTimer.currentTimeMs());
            wakeupTrigger.clearTask();
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
     * This method will {@link ConsumerNetworkThread#wakeup() wake up the network thread} before returning. This is
     * done as an optimization so that the <em>next round of data can be pre-fetched</em>.
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
     * @return true iff the operation completed without timing out
     * @throws AuthenticationException       If authentication fails. See the exception for more details
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no offset reset policy is
     *                                       defined
     */
    private boolean updateFetchPositions(final Timer timer) {
        cachedSubscriptionHasAllFetchPositions = false;
        try {
            CheckAndUpdatePositionsEvent checkAndUpdatePositionsEvent = new CheckAndUpdatePositionsEvent(calculateDeadlineMs(timer));
            wakeupTrigger.setActiveTask(checkAndUpdatePositionsEvent.future());
            cachedSubscriptionHasAllFetchPositions = applicationEventHandler.addAndGet(checkAndUpdatePositionsEvent);
        } catch (TimeoutException e) {
            return false;
        } finally {
            wakeupTrigger.clearTask();
        }
        return true;
    }

    /**
     *
     * Indicates if the consumer is using the Kafka-based offset management strategy,
     * according to config {@link CommonClientConfigs#GROUP_ID_CONFIG}
     */
    private boolean isCommittedOffsetsManagementEnabled() {
        return groupMetadata.get().isPresent();
    }

    /**
     * This method signals the background thread to {@link CreateFetchRequestsEvent create fetch requests}.
     *
     * <p/>
     *
     * This method takes the following steps to maintain compatibility with the {@link ClassicKafkaConsumer} method
     * of the same name:
     *
     * <ul>
     *     <li>
     *         The method will wait for confirmation of the request creation before continuing.
     *     </li>
     *     <li>
     *         The method will throw exceptions encountered during request creation to the user <b>immediately</b>.
     *     </li>
     *     <li>
     *         The method will suppress {@link TimeoutException}s that occur while waiting for the confirmation.
     *         Timeouts during request creation are a byproduct of this consumer's thread communication mechanisms.
     *         That exception type isn't thrown in the request creation step of the {@link ClassicKafkaConsumer}.
     *         Additionally, timeouts will not impact the logic of {@link #pollForFetches(Timer) blocking requests}
     *         as it can handle requests that are created after the timeout.
     *     </li>
     * </ul>
     *
     * @param timer Timer used to bound how long the consumer waits for the requests to be created, which in practice
     *              is used to avoid using {@link Long#MAX_VALUE} to wait "forever"
     */
    private void sendFetches(Timer timer) {
        try {
            applicationEventHandler.addAndGet(new CreateFetchRequestsEvent(calculateDeadlineMs(timer)));
        } catch (TimeoutException e) {
            // Can be ignored, per above comments.
        }
    }

    /**
     * This method signals the background thread to {@link CreateFetchRequestsEvent create fetch requests} for the
     * pre-fetch case, i.e. right before {@link #poll(Duration)} exits. In the pre-fetch case, the application thread
     * will not wait for confirmation of the request creation before continuing.
     *
     * <p/>
     *
     * At the point this method is called, {@link KafkaConsumer#poll(Duration)} has data ready to return to the user,
     * which means the consumed position was already updated. In order to prevent potential gaps in records, this
     * method is designed to suppress all exceptions.
     *
     * @param timer Provides an upper bound for the event and its {@link CompletableFuture future}
     */
    private void sendPrefetches(Timer timer) {
        try {
            applicationEventHandler.add(new CreateFetchRequestsEvent(calculateDeadlineMs(timer)));
        } catch (Throwable t) {
            // Any unexpected errors will be logged for troubleshooting, but not thrown.
            log.warn("An unexpected error occurred while pre-fetching data in Consumer.poll(), but was suppressed", t);
        }
    }

    @Override
    public boolean updateAssignmentMetadataIfNeeded(Timer timer) {
        offsetCommitCallbackInvoker.executeCallbacks();
        if (subscriptions.hasPatternSubscription()) {
            try {
                applicationEventHandler.addAndGet(new UpdatePatternSubscriptionEvent(calculateDeadlineMs(timer)));
            } catch (TimeoutException e) {
                return false;
            } finally {
                timer.update();
            }
        }
        processBackgroundEvents();

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
    public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        subscribeToRegex(pattern, Optional.of(listener));
    }

    @Override
    public void subscribe(SubscriptionPattern pattern) {
        subscribeToRegex(pattern, Optional.empty());
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
            log.info("Subscribed to pattern: '{}'", pattern);
            applicationEventHandler.addAndGet(new TopicPatternSubscriptionChangeEvent(
                pattern,
                listener,
                defaultApiTimeoutDeadlineMs()
            ));
        } finally {
            release();
        }
    }

    /**
     * Subscribe to the RE2/J pattern. This will generate an event to update the pattern in the
     * subscription state, so it's included in the next heartbeat request sent to the broker.
     * No validation of the pattern is performed by the client (other than null/empty checks).
     */
    private void subscribeToRegex(SubscriptionPattern pattern,
                                  Optional<ConsumerRebalanceListener> listener) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            throwIfSubscriptionPatternIsInvalid(pattern);
            log.info("Subscribing to regular expression {}", pattern);
            applicationEventHandler.addAndGet(new TopicRe2JPatternSubscriptionChangeEvent(
                pattern,
                listener,
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs))));
        } finally {
            release();
        }
    }

    private void throwIfSubscriptionPatternIsInvalid(SubscriptionPattern subscriptionPattern) {
        if (subscriptionPattern == null) {
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be null");
        }
        if (subscriptionPattern.pattern().isEmpty()) {
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be empty");
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

                // Clear the buffered data which are not a part of newly assigned topics
                final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

                for (TopicPartition tp : subscriptions.assignedPartitions()) {
                    if (topics.contains(tp.topic()))
                        currentTopicPartitions.add(tp);
                }

                fetchBuffer.retainAll(currentTopicPartitions);
                log.info("Subscribed to topic(s): {}", String.join(", ", topics));
                applicationEventHandler.addAndGet(new TopicSubscriptionChangeEvent(
                    new HashSet<>(topics),
                    listener,
                    defaultApiTimeoutDeadlineMs()
                ));
            }
        } finally {
            release();
        }
    }

    /**
     * Process the events—if any—that were produced by the {@link ConsumerNetworkThread network thread}.
     * It is possible that {@link ErrorEvent an error}
     * could occur when processing the events. In such cases, the processor will take a reference to the first
     * error, continue to process the remaining events, and then throw the first error that occurred.
     */
    private boolean processBackgroundEvents() {
        AtomicReference<KafkaException> firstError = new AtomicReference<>();

        LinkedList<BackgroundEvent> events = new LinkedList<>();
        backgroundEventQueue.drainTo(events);

        for (BackgroundEvent event : events) {
            try {
                if (event instanceof CompletableEvent)
                    backgroundEventReaper.add((CompletableEvent<?>) event);

                backgroundEventProcessor.process(event);
            } catch (Throwable t) {
                KafkaException e = ConsumerUtils.maybeWrapAsKafkaException(t);

                if (!firstError.compareAndSet(null, e))
                    log.warn("An error occurred when processing the background event: {}", e.getMessage(), e);
            }
        }

        backgroundEventReaper.reap(time.milliseconds());

        if (firstError.get() != null)
            throw firstError.get();

        return !events.isEmpty();
    }

    /**
     * This method can be used by cases where the caller has an event that needs to both block for completion but
     * also process background events. For some events, in order to fully process the associated logic, the
     * {@link ConsumerNetworkThread background thread} needs assistance from the application thread to complete.
     * If the application thread simply blocked on the event after submitting it, the processing would deadlock.
     * The logic herein is basically a loop that performs two tasks in each iteration:
     *
     * <ol>
     *     <li>Process background events, if any</li>
     *     <li><em>Briefly</em> wait for {@link CompletableApplicationEvent an event} to complete</li>
     * </ol>
     *
     * <p/>
     *
     * Each iteration gives the application thread an opportunity to process background events, which may be
     * necessary to complete the overall processing.
     *
     * <p/>
     *
     * As an example, take {@link #unsubscribe()}. To start unsubscribing, the application thread enqueues an
     * {@link UnsubscribeEvent} on the application event queue. That event will eventually trigger the
     * rebalancing logic in the background thread. Critically, as part of this rebalancing work, the
     * {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} callback needs to be invoked for any
     * partitions the consumer owns. However,
     * this callback must be executed on the application thread. To achieve this, the background thread enqueues a
     * {@link ConsumerRebalanceListenerCallbackNeededEvent} on its background event queue. That event queue is
     * periodically queried by the application thread to see if there's work to be done. When the application thread
     * sees {@link ConsumerRebalanceListenerCallbackNeededEvent}, it is processed, and then a
     * {@link ConsumerRebalanceListenerCallbackCompletedEvent} is then enqueued by the application thread on the
     * application event queue. Moments later, the background thread will see that event, process it, and continue
     * execution of the rebalancing logic. The rebalancing logic cannot complete until the
     * {@link ConsumerRebalanceListener} callback is performed.
     *
     * @param future                    Event that contains a {@link CompletableFuture}; it is on this future that the
     *                                  application thread will wait for completion
     * @param timer                     Overall timer that bounds how long to wait for the event to complete
     * @param ignoreErrorEventException Predicate to ignore background errors.
     *                                  Any exceptions found while processing background events that match the predicate won't be propagated.
     * @return {@code true} if the event completed within the timeout, {@code false} otherwise
     */
    // Visible for testing
    <T> T processBackgroundEvents(Future<T> future, Timer timer, Predicate<Exception> ignoreErrorEventException) {
        do {
            boolean hadEvents = false;
            try {
                hadEvents = processBackgroundEvents();
            } catch (Exception e) {
                if (!ignoreErrorEventException.test(e))
                    throw e;
            }

            try {
                if (future.isDone()) {
                    // If the event is done (either successfully or otherwise), go ahead and attempt to return
                    // without waiting. We use the ConsumerUtils.getResult() method here to handle the conversion
                    // of the exception types.
                    return ConsumerUtils.getResult(future);
                } else if (!hadEvents) {
                    // If the above processing yielded no events, then let's sit tight for a bit to allow the
                    // background thread to either finish the task, or populate the background event
                    // queue with things to process in our next loop.
                    Timer pollInterval = time.timer(100L);
                    return ConsumerUtils.getResult(future, pollInterval);
                }
            } catch (TimeoutException e) {
                // Ignore this as we will retry the event until the timeout expires.
            } finally {
                timer.update();
            }
        } while (timer.notExpired());

        throw new TimeoutException("Operation timed out before completion");
    }

    static ConsumerRebalanceListenerCallbackCompletedEvent invokeRebalanceCallbacks(ConsumerRebalanceListenerInvoker rebalanceListenerInvoker,
                                                                                    ConsumerRebalanceListenerMethodName methodName,
                                                                                    SortedSet<TopicPartition> partitions,
                                                                                    CompletableFuture<Void> future) {
        final Exception e;

        switch (methodName) {
            case ON_PARTITIONS_REVOKED:
                e = rebalanceListenerInvoker.invokePartitionsRevoked(partitions);
                break;

            case ON_PARTITIONS_ASSIGNED:
                e = rebalanceListenerInvoker.invokePartitionsAssigned(partitions);
                break;

            case ON_PARTITIONS_LOST:
                e = rebalanceListenerInvoker.invokePartitionsLost(partitions);
                break;

            default:
                throw new IllegalArgumentException("The method " + methodName.fullyQualifiedMethodName() + " to invoke was not expected");
        }

        final Optional<KafkaException> error;

        if (e != null)
            error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(e, "User rebalance callback throws an error"));
        else
            error = Optional.empty();

        return new ConsumerRebalanceListenerCallbackCompletedEvent(methodName, future, error);
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

    // Visible for testing
    SubscriptionState subscriptions() {
        return subscriptions;
    }

    private long defaultApiTimeoutDeadlineMs() {
        return calculateDeadlineMs(time, defaultApiTimeoutMs);
    }
}
