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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollContext;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import static org.apache.kafka.clients.consumer.internals.PrototypeAsyncConsumer.CLIENT_ID_METRIC_TAG;

/**
 * Background thread runnable that consumes {@code ApplicationEvent} and
 * produces {@code BackgroundEvent}. It uses an event loop to consume and
 * produce events, and poll the network client to handle network IO.
 * <p>
 * It holds a reference to the {@link SubscriptionState}, which is
 * initialized by the polling thread.
 */
public class DefaultBackgroundThread extends KafkaThread {
    private static final int MAX_POLL_TIMEOUT_MS = 5000;
    private static final String BACKGROUND_THREAD_NAME =
            "consumer_background_thread";
    private final Time time;
    private final Logger log;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final Metrics metrics;
    private final ConsumerConfig config;
    // empty if groupId is null
    private final Optional<CoordinatorRequestManager> coordinatorManager;
    private final FetchRequestManager<?, ?> fetchRequestManager;
    private final ApplicationEventProcessor applicationEventProcessor;
    private final NetworkClientDelegate networkClientDelegate;
    private final ErrorEventHandler errorEventHandler;

    private boolean running;

    // Visible for testing
    DefaultBackgroundThread(final Time time,
                            final ConsumerConfig config,
                            final LogContext logContext,
                            final BlockingQueue<ApplicationEvent> applicationEventQueue,
                            final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                            final ErrorEventHandler errorEventHandler,
                            final ApplicationEventProcessor processor,
                            final ConsumerMetadata metadata,
                            final SubscriptionState subscriptions,
                            final Metrics metrics,
                            final NetworkClientDelegate networkClient,
                            final CoordinatorRequestManager coordinatorManager,
                            final FetchRequestManager<?, ?> fetchRequestManager) {
        super(BACKGROUND_THREAD_NAME, true);
        this.time = time;
        this.running = true;
        this.log = logContext.logger(getClass());
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
        this.applicationEventProcessor = processor;
        this.config = config;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.metrics = metrics;
        this.networkClientDelegate = networkClient;
        this.coordinatorManager = Optional.ofNullable(coordinatorManager);
        this.fetchRequestManager = fetchRequestManager;
        this.errorEventHandler = errorEventHandler;
    }
    public DefaultBackgroundThread(final Time time,
                                   final ConsumerConfig config,
                                   final GroupRebalanceConfig rebalanceConfig,
                                   final LogContext logContext,
                                   final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                   final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                   final ConsumerMetadata metadata,
                                   final SubscriptionState subscriptions,
                                   final Metrics metrics,
                                   final KafkaClient networkClient) {
        super(BACKGROUND_THREAD_NAME, true);
        try {
            this.time = time;
            this.log = logContext.logger(getClass());
            this.applicationEventQueue = applicationEventQueue;
            this.backgroundEventQueue = backgroundEventQueue;
            this.config = config;
            this.metadata = metadata;
            this.subscriptions = subscriptions;
            this.metrics = metrics;
            this.networkClientDelegate = new NetworkClientDelegate(
                    this.time,
                    this.config,
                    logContext,
                    networkClient);
            this.running = true;
            this.errorEventHandler = new ErrorEventHandler(this.backgroundEventQueue);
            String groupId = rebalanceConfig.groupId;
            int requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
            this.coordinatorManager = groupId == null ?
                    Optional.empty() :
                    Optional.of(new CoordinatorRequestManager(
                        time,
                        logContext,
                        retryBackoffMs,
                        errorEventHandler,
                        groupId
                    ));

            String clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
            Deserializer<?> keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            keyDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), true);
            Deserializer<?> valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            valueDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), false);
            IsolationLevel isolationLevel = IsolationLevel.valueOf(config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));

            String metricGrpPrefix = "consumer";

            FetcherMetricsRegistry metricsRegistry = new FetcherMetricsRegistry(Collections.singleton(CLIENT_ID_METRIC_TAG), metricGrpPrefix);

            // TODO: KIRK - Need to make sure this configuration is legit.
            final FetchContext<?, ?> fetchContext = new FetchContext<>(logContext,
                                                                       time,
                                                                       config,
                                                                       keyDeserializer,
                                                                       valueDeserializer,
                                                                       retryBackoffMs,
                                                                       requestTimeoutMs,
                                                                       isolationLevel,
                                                                       metrics,
                                                                       metricsRegistry);

            // TODO: KIRK - Do we need a shared ApiVersions or should there be multiple?
            this.fetchRequestManager = new FetchRequestManager<>(fetchContext, new ApiVersions(), errorEventHandler, new RequestState(retryBackoffMs));
            this.applicationEventProcessor = new ApplicationEventProcessor(backgroundEventQueue, metadata, subscriptions, fetchRequestManager);
        } catch (final Exception e) {
            close();
            throw new KafkaException("Failed to construct background processor", e.getCause());
        }
    }

    @Override
    public void run() {
        try {
            log.debug("Background thread started");
            while (running) {
                try {
                    runOnce();
                } catch (final WakeupException e) {
                    log.debug("WakeupException caught, background thread won't be interrupted");
                    // swallow the wakeup exception to prevent killing the background thread.
                }
            }
        } catch (final Throwable t) {
            log.error("The background thread failed due to unexpected error", t);
            throw new RuntimeException(t);
        } finally {
            close();
            log.debug("{} closed", getClass());
        }
    }

    /**
     * Poll and process an {@link ApplicationEvent}. It performs the following tasks:
     * 1. Drains and try to process all of the requests in the queue.
     * 2. Poll request managers to queue up the necessary requests.
     * 3. Poll the networkClient to send and retrieve the response.
     */
    void runOnce() {
        drainAndProcess();

        final PollContext pollContext = new PollContext(metadata, subscriptions, time.milliseconds());

        List<RequestManager> requestManagers = new ArrayList<>();
        coordinatorManager.ifPresent(requestManagers::add);
        requestManagers.add(fetchRequestManager);

        long pollWaitTimeMs = MAX_POLL_TIMEOUT_MS;

        for (final RequestManager requestManager : requestManagers) {
            final PollResult pollResult = requestManager.poll(pollContext);
            final long timeUntilNextPollMs = handlePollResult(pollResult);
            pollWaitTimeMs = Math.min(pollWaitTimeMs, timeUntilNextPollMs);
        }

        networkClientDelegate.poll(pollWaitTimeMs, pollContext.currentTimeMs);

        // Handle any fetch responses that may be available.
        // TODO: KIRK - How do we make this available for the polling thread?
        Fetch<?, ?> fetch = fetchRequestManager.fetch(metadata, subscriptions);
    }

    private void drainAndProcess() {
        Queue<ApplicationEvent> events = pollApplicationEvent();
        Iterator<ApplicationEvent> iter = events.iterator();
        while (iter.hasNext()) {
            ApplicationEvent event = iter.next();
            log.debug("processing application event: {}", event);
            consumeApplicationEvent(event);
        }
    }

    long handlePollResult(PollResult res) {
        if (!res.unsentRequests.isEmpty()) {
            networkClientDelegate.addAll(res.unsentRequests);
        }
        return res.timeUntilNextPollMs;
    }

    private Queue<ApplicationEvent> pollApplicationEvent() {
        if (this.applicationEventQueue.isEmpty()) {
            return new LinkedList<>();
        }

        LinkedList<ApplicationEvent> res = new LinkedList<>();
        this.applicationEventQueue.drainTo(res);
        return res;
    }

    private void consumeApplicationEvent(final ApplicationEvent event) {
        log.debug("try consuming event: {}", Optional.ofNullable(event));
        Objects.requireNonNull(event);
        applicationEventProcessor.process(event);
    }

    public boolean isRunning() {
        return this.running;
    }

    public void wakeup() {
        networkClientDelegate.wakeup();
    }

    public void close() {
        this.running = false;
        this.wakeup();
        Utils.closeQuietly(networkClientDelegate, "network client utils");
        Utils.closeQuietly(metadata, "consumer metadata client");
    }
}
