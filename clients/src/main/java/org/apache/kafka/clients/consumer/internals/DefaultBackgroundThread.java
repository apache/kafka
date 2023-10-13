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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredIsolationLevel;

/**
 * Background thread runnable that consumes {@code ApplicationEvent} and
 * produces {@code BackgroundEvent}. It uses an event loop to consume and
 * produce events, and poll the network client to handle network IO.
 * <p/>
 * It holds a reference to the {@link SubscriptionState}, which is
 * initialized by the polling thread.
 * <p/>
 * For processing application events that have been submitted to the
 * {@link #applicationEventQueue}, this relies on an {@link ApplicationEventProcessor}. Processing includes generating requests and
 * handling responses with the appropriate {@link RequestManager}. The network operations for
 * actually sending the requests is delegated to the {@link NetworkClientDelegate}
 * </li>
 */
public class DefaultBackgroundThread extends KafkaThread {
    private static final long MAX_POLL_TIMEOUT_MS = 5000;
    private static final String BACKGROUND_THREAD_NAME = "consumer_background_thread";
    private final Time time;
    private final Logger log;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final ConsumerMetadata metadata;
    private final ConsumerConfig config;
    // empty if groupId is null
    private final ApplicationEventProcessor applicationEventProcessor;
    private final NetworkClientDelegate networkClientDelegate;
    private final ErrorEventHandler errorEventHandler;
    private final GroupState groupState;
    private boolean running;

    private final RequestManagers requestManagers;

    // Visible for testing
    @SuppressWarnings("ParameterNumber")
    DefaultBackgroundThread(final Time time,
                            final ConsumerConfig config,
                            final LogContext logContext,
                            final BlockingQueue<ApplicationEvent> applicationEventQueue,
                            final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                            final ErrorEventHandler errorEventHandler,
                            final ApplicationEventProcessor processor,
                            final ConsumerMetadata metadata,
                            final NetworkClientDelegate networkClient,
                            final GroupState groupState,
                            final CoordinatorRequestManager coordinatorManager,
                            final CommitRequestManager commitRequestManager,
                            final OffsetsRequestManager offsetsRequestManager,
                            final TopicMetadataRequestManager topicMetadataRequestManager,
                            final HeartbeatRequestManager heartbeatRequestManager) {
        super(BACKGROUND_THREAD_NAME, true);
        this.time = time;
        this.running = true;
        this.log = logContext.logger(getClass());
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
        this.applicationEventProcessor = processor;
        this.config = config;
        this.metadata = metadata;
        this.networkClientDelegate = networkClient;
        this.errorEventHandler = errorEventHandler;
        this.groupState = groupState;
        this.requestManagers = new RequestManagers(
                offsetsRequestManager,
                topicMetadataRequestManager,
                Optional.ofNullable(coordinatorManager),
                Optional.ofNullable(commitRequestManager),
                Optional.ofNullable(heartbeatRequestManager));
    }

    public DefaultBackgroundThread(final Time time,
                                   final ConsumerConfig config,
                                   final GroupRebalanceConfig rebalanceConfig,
                                   final LogContext logContext,
                                   final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                   final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                   final ConsumerMetadata metadata,
                                   final SubscriptionState subscriptionState,
                                   final ApiVersions apiVersions,
                                   final Metrics metrics,
                                   final Sensor fetcherThrottleTimeSensor) {
        super(BACKGROUND_THREAD_NAME, true);
        requireNonNull(config);
        requireNonNull(rebalanceConfig);
        requireNonNull(logContext);
        requireNonNull(applicationEventQueue);
        requireNonNull(backgroundEventQueue);
        requireNonNull(metadata);
        requireNonNull(subscriptionState);
        try {
            this.time = time;
            this.log = logContext.logger(getClass());
            this.applicationEventQueue = applicationEventQueue;
            this.backgroundEventQueue = backgroundEventQueue;
            this.config = config;
            this.metadata = metadata;
            final NetworkClient networkClient = ClientUtils.createNetworkClient(config,
                metrics,
                CONSUMER_METRIC_GROUP_PREFIX,
                logContext,
                apiVersions,
                time,
                CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
                metadata,
                fetcherThrottleTimeSensor);
            this.networkClientDelegate = new NetworkClientDelegate(
                this.time,
                this.config,
                logContext,
                networkClient);
            this.running = true;
            this.errorEventHandler = new ErrorEventHandler(this.backgroundEventQueue);
            this.groupState = new GroupState(rebalanceConfig);
            long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
            long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
            final int requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);

            OffsetsRequestManager offsetsRequestManager =
                new OffsetsRequestManager(
                    subscriptionState,
                    metadata,
                    configuredIsolationLevel(config),
                    time,
                    retryBackoffMs,
                    requestTimeoutMs,
                    apiVersions,
                    networkClientDelegate,
                    logContext);
            CoordinatorRequestManager coordinatorRequestManager = null;
            CommitRequestManager commitRequestManager = null;
            TopicMetadataRequestManager topicMetadataRequestManger = new TopicMetadataRequestManager(
                logContext,
                config);
            HeartbeatRequestManager heartbeatRequestManager = null;

            // TODO: consolidate groupState and memberState
            if (groupState.groupId != null) {
                coordinatorRequestManager = new CoordinatorRequestManager(
                        this.time,
                        logContext,
                        retryBackoffMs,
                        retryBackoffMaxMs,
                        this.errorEventHandler,
                        groupState.groupId);
                commitRequestManager = new CommitRequestManager(
                        this.time,
                        logContext,
                        subscriptionState,
                        config,
                        coordinatorRequestManager,
                        groupState);
                MembershipManager membershipManager = new MembershipManagerImpl(groupState.groupId);
                heartbeatRequestManager = new HeartbeatRequestManager(
                        this.time,
                        logContext,
                        config,
                        coordinatorRequestManager,
                        subscriptionState,
                        membershipManager,
                        errorEventHandler);
            }

            this.requestManagers = new RequestManagers(
                offsetsRequestManager,
                topicMetadataRequestManger,
                Optional.ofNullable(coordinatorRequestManager),
                Optional.ofNullable(commitRequestManager),
                Optional.ofNullable(heartbeatRequestManager));
            this.applicationEventProcessor = new ApplicationEventProcessor(
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
            throw new KafkaException(t);
        } finally {
            close();
            log.debug("{} closed", getClass());
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
        return this.running;
    }

    public final void wakeup() {
        networkClientDelegate.wakeup();
    }

    public final void close() {
        this.running = false;
        this.wakeup();
        Utils.closeQuietly(networkClientDelegate, "network client utils");
        Utils.closeQuietly(metadata, "consumer metadata client");
    }
}
