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
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

import static org.apache.kafka.clients.consumer.internals.Utils.CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.consumer.internals.Utils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchMetricsManager;

/**
 * Background thread runnable that consumes {@code ApplicationEvent} and
 * produces {@code BackgroundEvent}. It uses an event loop to consume and
 * produce events, and poll the network client to handle network IO.
 * <p>
 * It holds a reference to the {@link SubscriptionState}, which is
 * initialized by the polling thread.
 */
public class DefaultBackgroundThread<K, V> extends KafkaThread implements Closeable {

    private static final long MAX_POLL_TIMEOUT_MS = 5000;
    private static final String BACKGROUND_THREAD_NAME = "consumer_background_thread";
    private final Time time;
    private final Logger log;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final ConsumerMetadata metadata;
    private final ConsumerConfig config;
    // empty if groupId is null
    private final ApplicationEventProcessor<K, V> applicationEventProcessor;
    private final NetworkClientDelegate networkClientDelegate;
//    private final GroupState groupState;
    private volatile boolean running;
    private volatile boolean closed;

    private final RequestManagers<K, V> requestManagers;

    // Visible for testing
    DefaultBackgroundThread(final LogContext logContext,
                            final Time time,
                            final ConsumerConfig config,
                            final BlockingQueue<ApplicationEvent> applicationEventQueue,
                            final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                            final ConsumerMetadata metadata,
                            final NetworkClientDelegate networkClient,
                            final ApplicationEventProcessor<K, V> processor,
                            final RequestManagers<K, V> requestManagers) {
        super(BACKGROUND_THREAD_NAME, true);

        this.log = logContext.logger(getClass());
        this.time = time;
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
        this.applicationEventProcessor = processor;
        this.config = config;
        this.metadata = metadata;
        this.networkClientDelegate = networkClient;
        this.requestManagers = requestManagers;
    }

    public DefaultBackgroundThread(final LogContext logContext,
                                   final Time time,
                                   final ConsumerConfig config,
                                   final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                   final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                   final ConsumerMetadata metadata,
                                   final ApiVersions apiVersions,
                                   final Metrics metrics,
                                   final SubscriptionState subscriptions,
                                   final GroupRebalanceConfig rebalanceConfig) {
        super(BACKGROUND_THREAD_NAME, true);

        try {
            this.log = logContext.logger(getClass());
            this.time = time;
            this.applicationEventQueue = applicationEventQueue;
            this.backgroundEventQueue = backgroundEventQueue;
            this.config = config;
            this.metadata = metadata;

            final FetchConfig<K, V> fetchConfig = createFetchConfig(config);
            final FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);

            final NetworkClient networkClient = ClientUtils.createNetworkClient(config,
                    metrics,
                    CONSUMER_METRIC_GROUP_PREFIX,
                    logContext,
                    apiVersions,
                    time,
                    CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
                    metadata,
                    fetchMetricsManager.throttleTimeSensor());

            this.networkClientDelegate = new NetworkClientDelegate(logContext, this.time, this.config, networkClient);
            final long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

            final NodeStatusDeterminator nodeStatusDeterminator = new NodeStatusDeterminator() {

                    @Override
                    public boolean isUnavailable(Node node) {
                        return networkClient.connectionFailed(node) && networkClient.connectionDelay(node, time.milliseconds()) > 0;
                    }

                    @Override
                    public void maybeThrowAuthFailure(Node node) {
                        AuthenticationException exception = networkClient.authenticationException(node);
                        if (exception != null)
                            throw exception;
                    }

            };

            final FetchRequestManager<K, V> fetchRequestManager = new FetchRequestManager<>(logContext,
                    time,
                    backgroundEventQueue,
                    metadata,
                    subscriptions,
                    fetchConfig,
                    fetchMetricsManager,
                    nodeStatusDeterminator,
                    retryBackoffMs);
            CoordinatorRequestManager coordinatorRequestManager = null;
            CommitRequestManager commitRequestManager = null;

            if (rebalanceConfig.groupId != null) {
                GroupState groupState = new GroupState(rebalanceConfig);
                coordinatorRequestManager = new CoordinatorRequestManager(this.time,
                        logContext,
                        retryBackoffMs,
                        backgroundEventQueue,
                        groupState.groupId);
                commitRequestManager = new CommitRequestManager(this.time,
                        logContext,
                        subscriptions,
                        config,
                        coordinatorRequestManager,
                        groupState);
            }

            this.requestManagers = new RequestManagers<>(coordinatorRequestManager,
                    commitRequestManager,
                    fetchRequestManager);
            this.applicationEventProcessor = new ApplicationEventProcessor<>(logContext,
                    backgroundEventQueue,
                    requestManagers,
                    metadata);
        } catch (final Exception e) {
            close();
            throw new KafkaException("Failed to construct background processor", e.getCause());
        }
    }

    @Override
    public void run() {
        if (closed)
            throw new IllegalStateException("Background consumer thread is closed");

        running = true;

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
            log.debug("Exited run loop");
        }
    }

    /**
     * Poll and process an {@link ApplicationEvent}. It performs the following tasks:
     * 1. Drains and try to process all the requests in the queue.
     * 2. Iterate through the registry, poll, and get the next poll time for the network poll
     * 3. Poll the networkClient to send and retrieve the response.
     */
    void runOnce() {
        if (!applicationEventQueue.isEmpty()) {
            LinkedList<ApplicationEvent> res = new LinkedList<>();
            this.applicationEventQueue.drainTo(res);

            for (ApplicationEvent event : res) {
                log.debug("Consuming application event: {}", event);
                Objects.requireNonNull(event);
                applicationEventProcessor.process(event);
            }
        }

        final long currentTimeMs = time.milliseconds();
        final long pollWaitTimeMs = requestManagers.entries().stream()
                .filter(Optional::isPresent)
                .map(m -> m.get().poll(currentTimeMs))
                .filter(Objects::nonNull)
                .map(this::handlePollResult)
                .reduce(MAX_POLL_TIMEOUT_MS, Math::min);
        networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs);
    }

    long handlePollResult(NetworkClientDelegate.PollResult res) {
        if (!res.unsentRequests.isEmpty()) {
            networkClientDelegate.addAll(res.unsentRequests);
        }
        return res.timeUntilNextPollMs;
    }

    public boolean isRunning() {
        return running;
    }

    public void wakeup() {
        networkClientDelegate.wakeup();
    }

    @Override
    public void close() {
        if (closed)
            return;

        closed = true;
        running = false;
        wakeup();
        Utils.closeQuietly(networkClientDelegate, "network client utils");
        Utils.closeQuietly(metadata, "consumer metadata client");
    }
}
