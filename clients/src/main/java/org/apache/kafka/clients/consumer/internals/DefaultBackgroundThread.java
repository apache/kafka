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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * Background thread runnable that consumes {@code ApplicationEvent} and
 * produces {@code BackgroundEvent}. It uses an event loop to consume and
 * produce events, and poll the network client to handle network IO.
 * <p>
 * It holds a reference to the {@link SubscriptionState}, which is
 * initialized by the polling thread.
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

    private final Map<RequestManager.Type, Optional<RequestManager>> requestManagerRegistry;

    // Visible for testing
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
                            final CommitRequestManager commitRequestManager) {
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

        this.requestManagerRegistry = new HashMap<>();
        this.requestManagerRegistry.put(RequestManager.Type.COORDINATOR, Optional.ofNullable(coordinatorManager));
        this.requestManagerRegistry.put(RequestManager.Type.COMMIT, Optional.ofNullable(commitRequestManager));
    }
    public DefaultBackgroundThread(final Time time,
                                   final ConsumerConfig config,
                                   final GroupRebalanceConfig rebalanceConfig,
                                   final LogContext logContext,
                                   final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                   final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                   final ConsumerMetadata metadata,
                                   final KafkaClient networkClient) {
        super(BACKGROUND_THREAD_NAME, true);
        try {
            this.time = time;
            this.log = logContext.logger(getClass());
            this.applicationEventQueue = applicationEventQueue;
            this.backgroundEventQueue = backgroundEventQueue;
            this.config = config;
            // subscriptionState is initialized by the polling thread
            this.metadata = metadata;
            this.networkClientDelegate = new NetworkClientDelegate(
                    this.time,
                    this.config,
                    logContext,
                    networkClient);
            this.running = true;
            this.errorEventHandler = new ErrorEventHandler(this.backgroundEventQueue);
            this.groupState = new GroupState(rebalanceConfig);
            this.requestManagerRegistry = Collections.unmodifiableMap(buildRequestManagerRegistry(logContext));
            this.applicationEventProcessor = new ApplicationEventProcessor(backgroundEventQueue, requestManagerRegistry);
        } catch (final Exception e) {
            close();
            throw new KafkaException("Failed to construct background processor", e.getCause());
        }
    }

    private Map<RequestManager.Type, Optional<RequestManager>> buildRequestManagerRegistry(final LogContext logContext) {
        Map<RequestManager.Type, Optional<RequestManager>> registry = new HashMap<>();
        CoordinatorRequestManager coordinatorManager = groupState.groupId == null ?
                null :
                new CoordinatorRequestManager(
                        time,
                        logContext,
                        config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG),
                        errorEventHandler,
                        groupState.groupId);
        // Add subscriptionState
        CommitRequestManager commitRequestManager = coordinatorManager == null ?
                null :
                new CommitRequestManager(time,
                        logContext, null, config,
                        coordinatorManager,
                        groupState);
        registry.put(RequestManager.Type.COORDINATOR, Optional.ofNullable(coordinatorManager));
        registry.put(RequestManager.Type.COMMIT, Optional.ofNullable(commitRequestManager));
        return registry;
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
     * 1. Drains and try to process all the requests in the queue.
     * 2. Iterate through the registry, poll, and get the next poll time for the network poll
     * 3. Poll the networkClient to send and retrieve the response.
     */
    void runOnce() {
        drain();
        final long currentTimeMs = time.milliseconds();
        final long pollWaitTimeMs = requestManagerRegistry.values().stream()
                .filter(Optional::isPresent)
                .map(m -> m.get().poll(currentTimeMs))
                .map(this::handlePollResult)
                .reduce(MAX_POLL_TIMEOUT_MS, Math::min);
        networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs);
    }

    private void drain() {
        Queue<ApplicationEvent> events = pollApplicationEvent();
        for (ApplicationEvent event : events) {
            log.debug("Consuming application event: {}", event);
            consumeApplicationEvent(event);
        }
    }

    long handlePollResult(NetworkClientDelegate.PollResult res) {
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
