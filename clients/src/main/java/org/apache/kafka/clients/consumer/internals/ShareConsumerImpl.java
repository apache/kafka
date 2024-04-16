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
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.EventProcessor;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareLeaveOnCloseApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareSubscriptionChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareUnsubscribeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
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
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
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
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.join;

/**
 * This {@link ShareConsumer} implementation uses an {@link ApplicationEventHandler event handler} to process
 * {@link ApplicationEvent application events} so that the network I/O can be processed in a dedicated
 * {@link ConsumerNetworkThread network thread}.
 */
@SuppressWarnings("ClassFanOutComplexity")
public class ShareConsumerImpl<K, V> implements ShareConsumer<K, V> {

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
    private class BackgroundEventProcessor extends EventProcessor<BackgroundEvent> {

        public BackgroundEventProcessor(final LogContext logContext,
                                        final BlockingQueue<BackgroundEvent> backgroundEventQueue) {
            super(logContext, backgroundEventQueue);
        }

        /**
         * Process the events, if any, that were produced by the {@link ConsumerNetworkThread network thread}.
         * It is possible that {@link org.apache.kafka.clients.consumer.internals.events.ErrorEvent an error}
         * could occur when processing the events. In such cases, the processor will take a reference to the first
         * error, continue to process the remaining events, and then throw the first error that occurred.
         */
        @Override
        public boolean process() {
            AtomicReference<KafkaException> firstError = new AtomicReference<>();

            ProcessHandler<BackgroundEvent> processHandler = (event, error) -> {
                if (error.isPresent()) {
                    KafkaException e = error.get();

                    if (!firstError.compareAndSet(null, e)) {
                        log.warn("An error occurred when processing the event: {}", e.getMessage(), e);
                    }
                }
            };

            boolean hadEvents = process(processHandler);

            if (firstError.get() != null)
                throw firstError.get();

            return hadEvents;
        }

        @Override
        public void process(final BackgroundEvent event) {
            if (event.type() == BackgroundEvent.Type.ERROR) {
                process((ErrorEvent) event);
            } else {
                throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");
            }
        }

        private void process(final ErrorEvent event) {
            throw event.error();
        }
    }

    private final ApplicationEventHandler applicationEventHandler;
    private final Time time;
    private final KafkaConsumerMetrics kafkaConsumerMetrics;
    private Logger log;
    private final String clientId;
    private final String groupId;
    private final BackgroundEventProcessor backgroundEventProcessor;
    private final Deserializers<K, V> deserializers;
    private ShareFetch<K, V> currentFetch;
    private AcknowledgementCommitCallbackHandler acknowledgementCommitCallbackHandler;

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

    private AcknowledgementMode acknowledgementMode;

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
    private volatile boolean closed = false;
    private final Optional<ClientTelemetryReporter> clientTelemetryReporter;

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
            this.log = logContext.logger(getClass());
            this.acknowledgementMode = AcknowledgementMode.UNKNOWN;

            log.debug("Initializing the Kafka share consumer");
            this.time = time;
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            this.clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            this.clientTelemetryReporter.ifPresent(reporters::add);
            this.metrics = createMetrics(config, time, reporters);

            this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
            this.subscriptions = createSubscriptionState(config, logContext);
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                    metrics.reporters(),
                    Arrays.asList(deserializers.keyDeserializer, deserializers.valueDeserializer));
            this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
            final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            metadata.bootstrap(addresses);

            FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);
            ApiVersions apiVersions = new ApiVersions();
            final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
            final BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(
                    logContext,
                    backgroundEventQueue
            );

            // This FetchBuffer is shared between the application and network threads.
            this.fetchBuffer = new ShareFetchBuffer(logContext);
            final Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(
                    time,
                    logContext,
                    metadata,
                    config,
                    apiVersions,
                    metrics,
                    fetchMetricsManager,
                    clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null)
            );
            final Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(
                    time,
                    logContext,
                    backgroundEventHandler,
                    metadata,
                    subscriptions,
                    fetchBuffer,
                    config,
                    groupRebalanceConfig,
                    networkClientDelegateSupplier,
                    fetchMetricsManager,
                    clientTelemetryReporter,
                    metrics
            );
            final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(
                    logContext,
                    metadata,
                    applicationEventQueue,
                    requestManagersSupplier
            );

            this.applicationEventHandler = applicationEventHandlerFactory.build(
                    logContext,
                    time,
                    applicationEventQueue,
                    applicationEventProcessorSupplier,
                    networkClientDelegateSupplier,
                    requestManagersSupplier);

            this.backgroundEventProcessor = new BackgroundEventProcessor(
                    logContext,
                    backgroundEventQueue);

            this.fetchCollector = fetchCollectorFactory.build(
                    logContext,
                    metadata,
                    subscriptions,
                    new FetchConfig(config),
                    deserializers);

            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_METRIC_GROUP_PREFIX);

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

    // auxiliary interface for testing
    interface ApplicationEventHandlerFactory {

        ApplicationEventHandler build(
                final LogContext logContext,
                final Time time,
                final BlockingQueue<ApplicationEvent> applicationEventQueue,
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
            return subscriptions.subscription();
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

                log.info("Subscribed to topic(s): {}", join(topics, ", "));
                if (subscriptions.subscribeToShareGroup(new HashSet<>(topics)))
                    metadata.requestUpdateForNewTopics();

                // Trigger subscribe event to effectively join the group if not already part of it,
                // or just send the new subscription to the broker.
                applicationEventHandler.add(new ShareSubscriptionChangeApplicationEvent());
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
            ShareUnsubscribeApplicationEvent unsubscribeApplicationEvent = new ShareUnsubscribeApplicationEvent();
            applicationEventHandler.add(unsubscribeApplicationEvent);
            log.info("Unsubscribing all topics");

            subscriptions.unsubscribe();
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
            maybeSendAcknowledgements();

            kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            if (subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics.");
            }

            do {
                // We must not allow wake-ups between polling for fetches and returning the records.
                // If the polled fetches are not empty the consumed position has already been updated in the polling
                // of the fetches. A wakeup between returned fetches and returning records would lead to never
                // returning the records in the fetches. Thus, we trigger a possible wake-up before we poll fetches.
                wakeupTrigger.maybeTriggerWakeup();

                backgroundEventProcessor.process();

                // Make sure the network thread can tell the application is actively polling
                applicationEventHandler.add(new PollEvent(timer.currentTimeMs()));

                final ShareFetch<K, V> fetch = pollForFetches(timer);
                if (!fetch.isEmpty()) {
                    currentFetch = fetch;
                    return new ConsumerRecords<>(fetch.records());
                }

                // We will wait for retryBackoffMs
            } while (timer.notExpired());

            return ConsumerRecords.empty();
        } finally {
            kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
            wakeupTrigger.clearTask();
            release();
        }
    }

    private ShareFetch<K, V> pollForFetches(final Timer timer) {
        long pollTimeout = Math.min(applicationEventHandler.maximumTimeToWait(), timer.remainingMs());

        // If data is available already, return it immediately
        final ShareFetch<K, V> fetch = collect();
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // Wait a bit - this is where we will fetch records
        Timer pollTimer = time.timer(pollTimeout);
        try {
            fetchBuffer.awaitNotEmpty(pollTimer);
        } catch (InterruptException e) {
            log.trace("Timeout during fetch", e);
        } finally {
            timer.update(pollTimer.currentTimeMs());
        }

        return collect();
    }

    private ShareFetch<K, V> collect() {
        // Notify the network thread to wake up and start the next round of fetching
        applicationEventHandler.wakeupNetworkThread();

        return fetchCollector.collect(fetchBuffer);
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
            if (currentFetch != null) {
                currentFetch.acknowledge(record, type);
            } else {
                throw new IllegalStateException("The record cannot be acknowledged.");
            }
        } finally {
            release();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync(final Duration timeout) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commitAsync() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAcknowledgementCommitCallback(final AcknowledgementCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            if (callback != null) {
                acknowledgementCommitCallbackHandler = new AcknowledgementCommitCallbackHandler(callback);
            } else {
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
        if (!clientTelemetryReporter.isPresent()) {
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

        final Timer closeTimer = time.timer(timeout);
        clientTelemetryReporter.ifPresent(reporter -> reporter.initiateClose(timeout.toMillis()));
        closeTimer.update();

        // Send any outstanding acknowledgements and release any acquired records
        maybeSendAcknowledgementsOnClose();

        // Prepare shutting down the network thread
        prepareShutdown(closeTimer, firstException);
        closeTimer.update();
        if (applicationEventHandler != null)
            closeQuietly(() -> applicationEventHandler.close(Duration.ofMillis(closeTimer.remainingMs())), "Failed shutting down network thread", firstException);
        closeTimer.update();
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
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
     * 1. send leave group
     */
    void prepareShutdown(final Timer timer, final AtomicReference<Throwable> firstException) {
        completeQuietly(
            () -> applicationEventHandler.addAndGet(new ShareLeaveOnCloseApplicationEvent(timer), timer),
            "Failed to send leaveGroup heartbeat with a timeout(ms)=" + timer.timeoutMs(), firstException);
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
     * Handles any completed acknowledgements. This will be the integration point for the acknowledge
     * commit callback when that is implemented.
     *
     * @return The completed Acknowledgements which will contain the acknowledgement error code for each
     * topic-partition.
     */
    private Map<TopicIdPartition, Acknowledgements> handleCompletedAcknowledgements() {
        Map<TopicIdPartition, Acknowledgements> completedAcks = fetchBuffer.getCompletedAcknowledgements();
        if (acknowledgementCommitCallbackHandler != null) {
            acknowledgementCommitCallbackHandler.onComplete(completedAcks);
        }
        return completedAcks;
    }

    /**
     * Called to progressively move the acknowledgement mode into IMPLICIT if it is not known to be EXPLICIT.
     * If the acknowledgement mode is IMPLICIT, acknowledges the current batch and puts them into the fetch
     * buffer for the background thread to pick up.
     * If the acknowledgement mode is EXPLICIT, puts any ready acknowledgements into the fetch buffer for the
     * background thread to pick up.
     */
    private void maybeSendAcknowledgements() {
        if (acknowledgementMode == AcknowledgementMode.UNKNOWN) {
            // The first call to poll(Duration) moves into PENDING
            acknowledgementMode = AcknowledgementMode.PENDING;
        } else if (acknowledgementMode == AcknowledgementMode.PENDING) {
            // The second call to poll(Duration) if PENDING moves into IMPLICIT
            acknowledgementMode = AcknowledgementMode.IMPLICIT;
        }

        if (currentFetch != null) {
            // If IMPLICIT, acknowledge all records and send
            if (acknowledgementMode == AcknowledgementMode.IMPLICIT) {
                currentFetch.acknowledgeAll(AcknowledgeType.ACCEPT);
                fetchBuffer.acknowledgementsReadyToSend(currentFetch.acknowledgementsByPartition());
            } else if (acknowledgementMode == AcknowledgementMode.EXPLICIT) {
                // If EXPLICIT, send any acknowledgements which are ready
                fetchBuffer.acknowledgementsReadyToSend(currentFetch.acknowledgementsByPartition());
            }

            currentFetch = null;
        }
    }

    /**
     * Called to send any outstanding acknowledgements during close.
     */
    private void maybeSendAcknowledgementsOnClose() {
        if (currentFetch != null) {
            fetchBuffer.acknowledgementsReadyToSend(currentFetch.acknowledgementsByPartition());
        }
    }

    /**
     * Called to move the acknowledgement mode into EXPLICIT, if it is not known to be IMPLICIT.
     */
    private void ensureExplicitAcknowledgement() {
        if ((acknowledgementMode == AcknowledgementMode.UNKNOWN) || (acknowledgementMode == AcknowledgementMode.PENDING)) {
            // If poll(Duration) has been called at most once, moves into EXPLICIT
            acknowledgementMode = AcknowledgementMode.EXPLICIT;
        } else if (acknowledgementMode == AcknowledgementMode.IMPLICIT) {
            throw new IllegalStateException("Implicit acknowledgement of delivery is being used.");
        }
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
     * @param eventProcessor Event processor that contains the queue of events to process
     * @param future         Event that contains a {@link CompletableFuture}; it is on this future that the
     *                       application thread will wait for completion
     * @param timer          Overall timer that bounds how long to wait for the event to complete
     * @return {@code true} if the event completed within the timeout, {@code false} otherwise
     */
    // Visible for testing
    <T> T processBackgroundEvents(final EventProcessor<?> eventProcessor,
                                  final Future<T> future,
                                  final Timer timer) {
        log.trace("Will wait up to {} ms for future {} to complete", timer.remainingMs(), future);

        do {
            boolean hadEvents = eventProcessor.process();

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
}
