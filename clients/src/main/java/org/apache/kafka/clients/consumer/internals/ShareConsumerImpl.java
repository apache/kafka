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
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.EventProcessor;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeAsyncEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgeSyncEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementCommitCallbackEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementCommitCallbackRegistrationEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareFetchEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareUnsubscribeEvent;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaShareConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_SHARE_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createShareFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.swallow;

/**
 * This {@link ShareConsumer} implementation uses an {@link ApplicationEventHandler event handler} to process
 * {@link ApplicationEvent application events} so that the network I/O can be processed in a dedicated
 * {@link ConsumerNetworkThread network thread}.
 */
@SuppressWarnings({"ClassFanOutComplexity", "ClassDataAbstractionCoupling"})
public class ShareConsumerImpl<K, V> implements ShareConsumerDelegate<K, V> {

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

        public BackgroundEventProcessor() {}

        @Override
        public void process(final BackgroundEvent event) {
            switch (event.type()) {
                case ERROR:
                    process((ErrorEvent) event);
                    break;

                case SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK:
                    process((ShareAcknowledgementCommitCallbackEvent) event);
                    break;

                default:
                    throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");
            }
        }

        private void process(final ErrorEvent event) {
            throw event.error();
        }

        private void process(final ShareAcknowledgementCommitCallbackEvent event) {
            if (acknowledgementCommitCallbackHandler != null) {
                completedAcknowledgements.add(event.acknowledgementsMap());
            }
        }
    }

    private final ApplicationEventHandler applicationEventHandler;
    private final Time time;
    private final KafkaShareConsumerMetrics kafkaShareConsumerMetrics;
    private Logger log;
    private final String clientId;
    private final String groupId;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final BackgroundEventProcessor backgroundEventProcessor;
    private final CompletableEventReaper backgroundEventReaper;
    private final Deserializers<K, V> deserializers;
    private ShareFetch<K, V> currentFetch;
    private AcknowledgementCommitCallbackHandler acknowledgementCommitCallbackHandler;
    private final List<Map<TopicIdPartition, Acknowledgements>> completedAcknowledgements;

    private enum AcknowledgementMode {
        /** Acknowledgement mode is not yet known */
        UNKNOWN,
        /** Acknowledgement mode is pending, meaning that {@link #poll(Duration)} has been called once and
         * {@link #acknowledge(ConsumerRecord, AcknowledgeType)} has not been called */
        PENDING,
        /** Acknowledgements are explicit, using {@link #acknowledge(ConsumerRecord, AcknowledgeType)} */
        EXPLICIT,
        /** Acknowledgements are implicit, not using {@link #acknowledge(ConsumerRecord, AcknowledgeType)} */
        IMPLICIT
    }

    private AcknowledgementMode acknowledgementMode = AcknowledgementMode.UNKNOWN;

    /**
     * A thread-safe {@link ShareFetchBuffer fetch buffer} for the results that are populated in the
     * {@link ConsumerNetworkThread network thread} when the results are available. Because of the interaction
     * of the fetch buffer in the application thread and the network I/O thread, this is shared between the
     * two threads and is thus designed to be thread-safe.
     */
    private final ShareFetchBuffer fetchBuffer;
    private final ShareFetchCollector<K, V> fetchCollector;

    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final Metrics metrics;
    private final int defaultApiTimeoutMs;
    private volatile boolean closed = false;
    // Init value is needed to avoid NPE in case of exception raised in the constructor
    private Optional<ClientTelemetryReporter> clientTelemetryReporter = Optional.empty();

    private final WakeupTrigger wakeupTrigger = new WakeupTrigger();

    // currentThread holds the threadId of the current thread accessing the KafkaShareConsumer
    // and is used to prevent multithreaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    private final AtomicInteger refCount = new AtomicInteger(0);

    ShareConsumerImpl(final ConsumerConfig config,
                      final Deserializer<K> keyDeserializer,
                      final Deserializer<V> valueDeserializer) {
        this(
                config,
                keyDeserializer,
                valueDeserializer,
                Time.SYSTEM,
                ApplicationEventHandler::new,
                CompletableEventReaper::new,
                ShareFetchCollector::new,
                new LinkedBlockingQueue<>()
        );
    }

    // Visible for testing
    ShareConsumerImpl(final ConsumerConfig config,
                      final Deserializer<K> keyDeserializer,
                      final Deserializer<V> valueDeserializer,
                      final Time time,
                      final ApplicationEventHandlerFactory applicationEventHandlerFactory,
                      final AsyncKafkaConsumer.CompletableEventReaperFactory backgroundEventReaperFactory,
                      final ShareFetchCollectorFactory<K, V> fetchCollectorFactory,
                      final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue) {
        try {
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
                    config,
                    GroupRebalanceConfig.ProtocolType.SHARE
            );
            this.clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
            this.groupId = config.getString(ConsumerConfig.GROUP_ID_CONFIG);
            maybeThrowInvalidGroupIdException();
            LogContext logContext = createLogContext(clientId, groupId);
            this.backgroundEventQueue = backgroundEventQueue;
            this.log = logContext.logger(getClass());

            log.debug("Initializing the Kafka share consumer");
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            this.time = time;
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            this.clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            this.clientTelemetryReporter.ifPresent(reporters::add);
            this.metrics = createMetrics(config, time, reporters);

            this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
            this.currentFetch = ShareFetch.empty();
            this.subscriptions = createSubscriptionState(config, logContext);
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                    metrics.reporters(),
                    Arrays.asList(deserializers.keyDeserializer, deserializers.valueDeserializer));
            this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
            final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            metadata.bootstrap(addresses);

            ShareFetchMetricsManager shareFetchMetricsManager = createShareFetchMetricsManager(metrics);
            ApiVersions apiVersions = new ApiVersions();
            final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
            final BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(backgroundEventQueue);

            // This FetchBuffer is shared between the application and network threads.
            this.fetchBuffer = new ShareFetchBuffer(logContext);
            final Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(
                    time,
                    logContext,
                    metadata,
                    config,
                    apiVersions,
                    metrics,
                    shareFetchMetricsManager.throttleTimeSensor(),
                    clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null),
                    backgroundEventHandler
            );
            this.completedAcknowledgements = new LinkedList<>();

            final Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(
                    time,
                    logContext,
                    backgroundEventHandler,
                    metadata,
                    subscriptions,
                    fetchBuffer,
                    config,
                    groupRebalanceConfig,
                    shareFetchMetricsManager,
                    clientTelemetryReporter,
                    metrics
            );
            final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(
                    logContext,
                    metadata,
                    subscriptions,
                    requestManagersSupplier
            );

            this.applicationEventHandler = applicationEventHandlerFactory.build(
                    logContext,
                    time,
                    applicationEventQueue,
                    new CompletableEventReaper(logContext),
                    applicationEventProcessorSupplier,
                    networkClientDelegateSupplier,
                    requestManagersSupplier);

            this.backgroundEventProcessor = new BackgroundEventProcessor();
            this.backgroundEventReaper = backgroundEventReaperFactory.build(logContext);

            this.fetchCollector = fetchCollectorFactory.build(
                    logContext,
                    metadata,
                    subscriptions,
                    new FetchConfig(config),
                    deserializers);

            this.kafkaShareConsumerMetrics = new KafkaShareConsumerMetrics(metrics, CONSUMER_SHARE_METRIC_GROUP_PREFIX);

            config.logUnused();
            AppInfoParser.registerAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka share consumer initialized");
        } catch (Throwable t) {
            // Call close methods if internal objects are already constructed; this is to prevent resource leak.
            // We do not need to call `close` at all when `log` is null, which means no internal objects were initialized.
            if (this.log != null) {
                close(Duration.ZERO, true);
            }
            // Now propagate the exception
            throw new KafkaException("Failed to construct Kafka share consumer", t);
        }
    }

    // Visible for testing
    ShareConsumerImpl(final LogContext logContext,
                      final String clientId,
                      final String groupId,
                      final ConsumerConfig config,
                      final Deserializer<K> keyDeserializer,
                      final Deserializer<V> valueDeserializer,
                      final Time time,
                      final KafkaClient client,
                      final SubscriptionState subscriptions,
                      final ConsumerMetadata metadata) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.log = logContext.logger(getClass());
        this.time = time;
        this.metrics = new Metrics(time);
        this.clientTelemetryReporter = Optional.empty();
        this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
        this.currentFetch = ShareFetch.empty();
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        this.fetchBuffer = new ShareFetchBuffer(logContext);
        this.completedAcknowledgements = new LinkedList<>();

        ShareConsumerMetrics metricsRegistry = new ShareConsumerMetrics(CONSUMER_SHARE_METRIC_GROUP_PREFIX);
        ShareFetchMetricsManager shareFetchMetricsManager = new ShareFetchMetricsManager(metrics, metricsRegistry.shareFetchMetrics);
        this.fetchCollector = new ShareFetchCollector<>(
                logContext,
                metadata,
                subscriptions,
                new FetchConfig(config),
                deserializers);
        this.kafkaShareConsumerMetrics = new KafkaShareConsumerMetrics(metrics, CONSUMER_SHARE_METRIC_GROUP_PREFIX);

        final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
        final BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
        final BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(backgroundEventQueue);

        final Supplier<NetworkClientDelegate> networkClientDelegateSupplier =
                () -> new NetworkClientDelegate(time, config, logContext, client, metadata, backgroundEventHandler);

        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
                config,
                GroupRebalanceConfig.ProtocolType.SHARE);
        final Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(
                time,
                logContext,
                backgroundEventHandler,
                metadata,
                subscriptions,
                fetchBuffer,
                config,
                groupRebalanceConfig,
                shareFetchMetricsManager,
                clientTelemetryReporter,
                metrics
        );

        final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(
                logContext,
                metadata,
                subscriptions,
                requestManagersSupplier
        );

        this.applicationEventHandler = new ApplicationEventHandler(
                logContext,
                time,
                applicationEventQueue,
                new CompletableEventReaper(logContext),
                applicationEventProcessorSupplier,
                networkClientDelegateSupplier,
                requestManagersSupplier);

        this.backgroundEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventProcessor = new BackgroundEventProcessor();
        this.backgroundEventReaper = new CompletableEventReaper(logContext);

        config.logUnused();
        AppInfoParser.registerAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics, time.milliseconds());
    }

    // Visible for testing
    @SuppressWarnings("ParameterNumber")
    ShareConsumerImpl(final LogContext logContext,
                      final String clientId,
                      final Deserializer<K> keyDeserializer,
                      final Deserializer<V> valueDeserializer,
                      final ShareFetchBuffer fetchBuffer,
                      final ShareFetchCollector<K, V> fetchCollector,
                      final Time time,
                      final ApplicationEventHandler applicationEventHandler,
                      final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                      final CompletableEventReaper backgroundEventReaper,
                      final Metrics metrics,
                      final SubscriptionState subscriptions,
                      final ConsumerMetadata metadata,
                      final int defaultApiTimeoutMs,
                      final String groupId) {
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.clientId = clientId;
        this.groupId = groupId;
        this.fetchBuffer = fetchBuffer;
        this.fetchCollector = fetchCollector;
        this.time = time;
        this.backgroundEventQueue = backgroundEventQueue;
        this.backgroundEventProcessor = new BackgroundEventProcessor();
        this.backgroundEventReaper = backgroundEventReaper;
        this.metrics = metrics;
        this.metadata = metadata;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.deserializers = new Deserializers<>(keyDeserializer, valueDeserializer);
        this.currentFetch = ShareFetch.empty();
        this.applicationEventHandler = applicationEventHandler;
        this.kafkaShareConsumerMetrics = new KafkaShareConsumerMetrics(metrics, CONSUMER_SHARE_METRIC_GROUP_PREFIX);
        this.clientTelemetryReporter = Optional.empty();
        this.completedAcknowledgements = Collections.emptyList();
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
    interface ShareFetchCollectorFactory<K, V> {

        ShareFetchCollector<K, V> build(
                final LogContext logContext,
                final ConsumerMetadata metadata,
                final SubscriptionState subscriptions,
                final FetchConfig fetchConfig,
                final Deserializers<K, V> deserializers
        );
    }

    /**
     * {@inheritDoc}
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(final Collection<String> topics) {
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

                // Trigger subscribe event to effectively join the group if not already part of it,
                // or just send the new subscription to the broker.
                applicationEventHandler.addAndGet(new ShareSubscriptionChangeEvent(topics));

                log.info("Subscribed to topics: {}", String.join(", ", topics));
            }
        } finally {
            release();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unsubscribe() {
        acquireAndEnsureOpen();
        try {
            Timer timer = time.timer(defaultApiTimeoutMs);
            ShareUnsubscribeEvent unsubscribeApplicationEvent = new ShareUnsubscribeEvent(calculateDeadlineMs(timer));
            applicationEventHandler.addAndGet(unsubscribeApplicationEvent);

            log.info("Unsubscribed all topics");
        } finally {
            release();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized ConsumerRecords<K, V> poll(final Duration timeout) {
        Timer timer = time.timer(timeout);

        acquireAndEnsureOpen();
        try {
            // Handle any completed acknowledgements for which we already have the responses
            handleCompletedAcknowledgements();

            // If using implicit acknowledgement, acknowledge the previously fetched records
            acknowledgeBatchIfImplicitAcknowledgement(true);

            kafkaShareConsumerMetrics.recordPollStart(timer.currentTimeMs());

            if (subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics.");
            }

            do {
                // Make sure the network thread can tell the application is actively polling
                applicationEventHandler.add(new PollEvent(timer.currentTimeMs()));

                processBackgroundEvents();

                // We must not allow wake-ups between polling for fetches and returning the records.
                // A wake-up between returned fetches and returning records would lead to never
                // returning the records in the fetches. Thus, we trigger a possible wake-up before we poll fetches.
                wakeupTrigger.maybeTriggerWakeup();

                final ShareFetch<K, V> fetch = pollForFetches(timer);
                if (!fetch.isEmpty()) {
                    currentFetch = fetch;
                    return new ConsumerRecords<>(fetch.records(), Map.of());
                }

                metadata.maybeThrowAnyException();

                // We will wait for retryBackoffMs
            } while (timer.notExpired());

            return ConsumerRecords.empty();
        } finally {
            kafkaShareConsumerMetrics.recordPollEnd(timer.currentTimeMs());
            release();
        }
    }

    private ShareFetch<K, V> pollForFetches(final Timer timer) {
        long pollTimeout = Math.min(applicationEventHandler.maximumTimeToWait(), timer.remainingMs());

        Map<TopicIdPartition, Acknowledgements> acknowledgementsMap = currentFetch.takeAcknowledgedRecords();

        // If data is available already, return it immediately
        final ShareFetch<K, V> fetch = collect(acknowledgementsMap);
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // Wait a bit - this is where we will fetch records
        Timer pollTimer = time.timer(pollTimeout);
        wakeupTrigger.setShareFetchAction(fetchBuffer);

        try {
            fetchBuffer.awaitNotEmpty(pollTimer);
        } catch (InterruptException e) {
            log.trace("Timeout during fetch", e);
            throw e;
        } finally {
            timer.update(pollTimer.currentTimeMs());
            wakeupTrigger.clearTask();
        }

        return collect(Collections.emptyMap());
    }

    private ShareFetch<K, V> collect(Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        if (currentFetch.isEmpty()) {
            final ShareFetch<K, V> fetch = fetchCollector.collect(fetchBuffer);
            if (fetch.isEmpty()) {
                // Fetch more records and send any waiting acknowledgements
                applicationEventHandler.add(new ShareFetchEvent(acknowledgementsMap));

                // Notify the network thread to wake up and start the next round of fetching
                applicationEventHandler.wakeupNetworkThread();
            } else if (!acknowledgementsMap.isEmpty()) {
                // Asynchronously commit any waiting acknowledgements
                applicationEventHandler.add(new ShareAcknowledgeAsyncEvent(acknowledgementsMap));

                // Notify the network thread to wake up and start the next round of fetching
                applicationEventHandler.wakeupNetworkThread();
            }
            return fetch;
        } else {
            if (!acknowledgementsMap.isEmpty()) {
                // Asynchronously commit any waiting acknowledgements
                applicationEventHandler.add(new ShareAcknowledgeAsyncEvent(acknowledgementsMap));

                // Notify the network thread to wake up and start the next round of fetching
                applicationEventHandler.wakeupNetworkThread();
            }
            return currentFetch;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void acknowledge(final ConsumerRecord<K, V> record) {
        acknowledge(record, AcknowledgeType.ACCEPT);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void acknowledge(final ConsumerRecord<K, V> record, final AcknowledgeType type) {
        acquireAndEnsureOpen();
        try {
            ensureExplicitAcknowledgement();
            currentFetch.acknowledge(record, type);
        } finally {
            release();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync() {
        return this.commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync(final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            // Handle any completed acknowledgements for which we already have the responses
            handleCompletedAcknowledgements();

            // If using implicit acknowledgement, acknowledge the previously fetched records
            acknowledgeBatchIfImplicitAcknowledgement(false);

            Timer requestTimer = time.timer(timeout.toMillis());
            Map<TopicIdPartition, Acknowledgements> acknowledgementsMap = acknowledgementsToSend();
            if (acknowledgementsMap.isEmpty()) {
                return Collections.emptyMap();
            } else {
                ShareAcknowledgeSyncEvent event = new ShareAcknowledgeSyncEvent(acknowledgementsMap, calculateDeadlineMs(requestTimer));
                applicationEventHandler.add(event);
                CompletableFuture<Map<TopicIdPartition, Acknowledgements>> commitFuture = event.future();
                wakeupTrigger.setActiveTask(commitFuture);
                try {
                    Map<TopicIdPartition, Optional<KafkaException>> result = new HashMap<>();
                    Map<TopicIdPartition, Acknowledgements> completedAcknowledgements = ConsumerUtils.getResult(commitFuture);
                    completedAcknowledgements.forEach((tip, acks) -> {
                        Errors ackErrorCode = acks.getAcknowledgeErrorCode();
                        if (ackErrorCode == null) {
                            result.put(tip, Optional.empty());
                        } else {
                            ApiException exception = ackErrorCode.exception();
                            if (exception == null) {
                                result.put(tip, Optional.empty());
                            } else {
                                result.put(tip, Optional.of(ackErrorCode.exception()));
                            }
                        }
                    });
                    return result;

                } finally {
                    wakeupTrigger.clearTask();
                }
            }
        } finally {
            release();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commitAsync() {
        acquireAndEnsureOpen();
        try {
            // Handle any completed acknowledgements for which we already have the responses
            handleCompletedAcknowledgements();

            // If using implicit acknowledgement, acknowledge the previously fetched records
            acknowledgeBatchIfImplicitAcknowledgement(false);

            Map<TopicIdPartition, Acknowledgements> acknowledgementsMap = acknowledgementsToSend();
            if (!acknowledgementsMap.isEmpty()) {
                ShareAcknowledgeAsyncEvent event = new ShareAcknowledgeAsyncEvent(acknowledgementsMap);
                applicationEventHandler.add(event);
            }
        } finally {
            release();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAcknowledgementCommitCallback(final AcknowledgementCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            if (callback != null) {
                if (acknowledgementCommitCallbackHandler == null) {
                    ShareAcknowledgementCommitCallbackRegistrationEvent event = new ShareAcknowledgementCommitCallbackRegistrationEvent(true);
                    applicationEventHandler.add(event);
                }
                acknowledgementCommitCallbackHandler = new AcknowledgementCommitCallbackHandler(callback);
            } else {
                if (acknowledgementCommitCallbackHandler != null) {
                    ShareAcknowledgementCommitCallbackRegistrationEvent event = new ShareAcknowledgementCommitCallbackRegistrationEvent(false);
                    applicationEventHandler.add(event);
                }
                completedAcknowledgements.clear();
                acknowledgementCommitCallbackHandler = null;
            }
        } finally {
            release();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Uuid clientInstanceId(final Duration timeout) {
        if (clientTelemetryReporter.isEmpty()) {
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");
        }

        return ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(metrics.metrics());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close(final Duration timeout) {
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        acquire();
        try {
            if (!closed) {
                // need to close before setting the flag since the close function
                // itself may trigger code that needs the consumer to be open still
                close(timeout, false);
            }
        } finally {
            closed = true;
            release();
        }
    }

    private void close(final Duration timeout, final boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        // We are already closing with a timeout, don't allow wake-ups from here on.
        wakeupTrigger.disableWakeups();

        final Timer closeTimer = time.timer(timeout);
        clientTelemetryReporter.ifPresent(ClientTelemetryReporter::initiateClose);
        closeTimer.update();

        // Prepare shutting down the network thread
        swallow(log, Level.ERROR, "Failed to release assignment before closing consumer",
                () -> sendAcknowledgementsAndLeaveGroup(closeTimer, firstException), firstException);
        swallow(log, Level.ERROR, "Failed invoking acknowledgement commit callback",
                this::handleCompletedAcknowledgements, firstException);
        if (applicationEventHandler != null)
            closeQuietly(() -> applicationEventHandler.close(Duration.ofMillis(closeTimer.remainingMs())), "Failed shutting down network thread", firstException);
        closeTimer.update();

        // close() can be called from inside one of the constructors. In that case, it's possible that neither
        // the reaper nor the background event queue were constructed, so check them first to avoid NPE.
        if (backgroundEventReaper != null && backgroundEventQueue != null)
            backgroundEventReaper.reap(backgroundEventQueue);

        closeQuietly(kafkaShareConsumerMetrics, "kafka share consumer metrics", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);
        clientTelemetryReporter.ifPresent(reporter -> closeQuietly(reporter, "consumer telemetry reporter", firstException));

        AppInfoParser.unregisterAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics);
        log.debug("Kafka share consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close Kafka share consumer", exception);
        }
    }

    /**
     * Prior to closing the network thread, we need to make sure the following operations happen in the right sequence:
     * 1. commit pending acknowledgements and close any share sessions
     * 2. leave the group
     */
    private void sendAcknowledgementsAndLeaveGroup(final Timer timer, final AtomicReference<Throwable> firstException) {
        completeQuietly(
                () -> applicationEventHandler.addAndGet(new ShareAcknowledgeOnCloseEvent(acknowledgementsToSend(), calculateDeadlineMs(timer))),
                "Failed to send pending acknowledgements with a timeout(ms)=" + timer.timeoutMs(), firstException);
        timer.update();

        ShareUnsubscribeEvent unsubscribeEvent = new ShareUnsubscribeEvent(calculateDeadlineMs(timer));
        applicationEventHandler.add(unsubscribeEvent);
        try {
            processBackgroundEvents(unsubscribeEvent.future(), timer);
            log.info("Completed releasing assignment and leaving group to close consumer.");
        } catch (TimeoutException e) {
            log.warn("Consumer triggered an unsubscribe event to leave the group but couldn't " +
                    "complete it within {} ms. It will proceed to close.", timer.timeoutMs());
        } finally {
            timer.update();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void wakeup() {
        wakeupTrigger.wakeup();
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
            throw new ConcurrentModificationException("KafkaShareConsumer is not safe for multi-threaded access. " +
                    "currentThread(name: " + thread.getName() + ", id: " + threadId + ")" +
                    " otherThread(id: " + currentThread.get() + ")"
            );
        if (acknowledgementCommitCallbackHandler != null && acknowledgementCommitCallbackHandler.hasEnteredCallback()) {
            throw new IllegalStateException("KafkaShareConsumer methods are not accessible from user-defined " +
                    "acknowledgement commit callback.");
        }
        refCount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multithreaded access.
     */
    private void release() {
        if (refCount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    public static LogContext createLogContext(final String clientId, final String groupId) {
        return new LogContext("[ShareConsumer clientId=" + clientId + ", groupId=" + groupId + "] ");
    }

    private void maybeThrowInvalidGroupIdException() {
        if (groupId == null || groupId.isEmpty()) {
            throw new InvalidGroupIdException(
                    "You must provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
        }
    }

    /**
     * Handles any completed acknowledgements. If there is an acknowledgement commit callback registered,
     * call it. Otherwise, discard the information about completed acknowledgements because the application
     * is not interested.
     * <p>
     * If the acknowledgement commit callback throws an exception, this method will throw an exception.
     */
    private void handleCompletedAcknowledgements() {
        processBackgroundEvents();

        if (!completedAcknowledgements.isEmpty()) {
            try {
                if (acknowledgementCommitCallbackHandler != null) {
                    acknowledgementCommitCallbackHandler.onComplete(completedAcknowledgements);
                }
            } finally {
                completedAcknowledgements.clear();
            }
        }
    }

    /**
     * Called to progressively move the acknowledgement mode into IMPLICIT if it is not known to be EXPLICIT.
     * If the acknowledgement mode is IMPLICIT, acknowledges all records in the current batch.
     *
     * @param calledOnPoll If true, called on poll. Otherwise, called on commit.
     */
    private void acknowledgeBatchIfImplicitAcknowledgement(boolean calledOnPoll) {
        if (calledOnPoll) {
            if (acknowledgementMode == AcknowledgementMode.UNKNOWN) {
                // The first call to poll(Duration) moves into PENDING
                acknowledgementMode = AcknowledgementMode.PENDING;
            } else if (acknowledgementMode == AcknowledgementMode.PENDING) {
                // The second call to poll(Duration) if PENDING moves into IMPLICIT
                acknowledgementMode = AcknowledgementMode.IMPLICIT;
            }
        } else {
            // If there are records to acknowledge and PENDING, moves into IMPLICIT
            if (acknowledgementMode == AcknowledgementMode.PENDING && !currentFetch.isEmpty()) {
                acknowledgementMode = AcknowledgementMode.IMPLICIT;
            }
        }

        // If IMPLICIT, acknowledge all records
        if (acknowledgementMode == AcknowledgementMode.IMPLICIT) {
            currentFetch.acknowledgeAll(AcknowledgeType.ACCEPT);
        }
    }

    /**
     * Returns any ready acknowledgements to be sent to the cluster.
     */
    private Map<TopicIdPartition, Acknowledgements> acknowledgementsToSend() {
        return currentFetch.takeAcknowledgedRecords();
    }

    /**
     * Called to move the acknowledgement mode into EXPLICIT, if it is not known to be IMPLICIT.
     */
    private void ensureExplicitAcknowledgement() {
        if (acknowledgementMode == AcknowledgementMode.PENDING) {
            // If poll(Duration) has been called once, moves into EXPLICIT
            acknowledgementMode = AcknowledgementMode.EXPLICIT;
        } else if (acknowledgementMode == AcknowledgementMode.IMPLICIT) {
            throw new IllegalStateException("Implicit acknowledgement of delivery is being used.");
        } else if (acknowledgementMode == AcknowledgementMode.UNKNOWN) {
            throw new IllegalStateException("Acknowledge called before poll.");
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
     * @param future         Event that contains a {@link CompletableFuture}; it is on this future that the
     *                       application thread will wait for completion
     * @param timer          Overall timer that bounds how long to wait for the event to complete
     * @return {@code true} if the event completed within the timeout, {@code false} otherwise
     */
    // Visible for testing
    <T> T processBackgroundEvents(final Future<T> future,
                                  final Timer timer) {
        log.trace("Will wait up to {} ms for future {} to complete", timer.remainingMs(), future);

        do {
            boolean hadEvents = processBackgroundEvents();

            try {
                if (future.isDone()) {
                    // If the event is done (either successfully or otherwise), go ahead and attempt to return
                    // without waiting. We use the ConsumerUtils.getResult() method here to handle the conversion
                    // of the exception types.
                    T result = ConsumerUtils.getResult(future);
                    log.trace("Future {} completed successfully", future);
                    return result;
                } else if (!hadEvents) {
                    // If the above processing yielded no events, then let's sit tight for a bit to allow the
                    // background thread to either finish the task, or populate the background event
                    // queue with things to process in our next loop.
                    Timer pollInterval = time.timer(100L);
                    log.trace("Waiting {} ms for future {} to complete", pollInterval.remainingMs(), future);
                    T result = ConsumerUtils.getResult(future, pollInterval);
                    log.trace("Future {} completed successfully", future);
                    return result;
                }
            } catch (TimeoutException e) {
                // Ignore this as we will retry the event until the timeout expires.
            } finally {
                timer.update();
            }
        } while (timer.notExpired());

        log.trace("Future {} did not complete within timeout", future);
        throw new TimeoutException("Operation timed out before completion");
    }

    // Visible for testing
    void completeQuietly(final Utils.ThrowingRunnable function,
                         final String msg,
                         final AtomicReference<Throwable> firstException) {
        try {
            function.run();
        } catch (TimeoutException e) {
            log.debug("Timeout expired before the {} operation could complete.", msg);
        } catch (Exception e) {
            firstException.compareAndSet(null, e);
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
    public KafkaShareConsumerMetrics kafkaShareConsumerMetrics() {
        return kafkaShareConsumerMetrics;
    }
}
