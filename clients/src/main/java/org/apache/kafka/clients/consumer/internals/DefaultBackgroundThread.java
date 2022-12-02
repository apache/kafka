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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.Optional;
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
    private static final String BACKGROUND_THREAD_NAME =
        "consumer_background_thread";
    private final Time time;
    private final Logger log;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final ConsumerMetadata metadata;
    private final ConsumerConfig config;
    private final CoordinatorRequestManager coordinatorManager;
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
                            final NetworkClientDelegate networkClient,
                            final CoordinatorRequestManager coordinatorManager) {
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
        this.coordinatorManager = coordinatorManager;
        this.errorEventHandler = errorEventHandler;
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
                    logContext,
                    networkClient);
            this.running = true;
            this.errorEventHandler = new ErrorEventHandler(this.backgroundEventQueue);
            this.coordinatorManager = new CoordinatorRequestManager(time,
                    logContext,
                    config,
                    errorEventHandler,
                    Optional.ofNullable(rebalanceConfig.groupId),
                    rebalanceConfig.rebalanceTimeoutMs);
            this.applicationEventProcessor = new ApplicationEventProcessor(
                    coordinatorManager,
                    backgroundEventQueue);
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
     * Process event from a single poll. It performs the following tasks sequentially:
     *  1. Try to poll and event from the queue, and try to process it.
     *  2. Try to find Coordinator if needed
     *  3. Try to send fetches.
     *  4. Poll the networkClient for outstanding requests. If non: poll until next
     *  iteration.
     */
    void runOnce() {
        Optional<ApplicationEvent> event = maybePollEvent();

        if (event.isPresent()) {
            log.debug("processing application event: {}", event);
            consumeApplicationEvent(event.get());
        }

        final long currentTimeMs = time.milliseconds();
        long pollWaitTimeMs = timeToNextHeartbeatMs();

        // TODO: Add a condition here, like shouldFindCoordinator in the future.  Since we don't always need to find
        //  the coordinator.
        if (coordinatorUnknown()) {
            pollWaitTimeMs = Math.min(pollWaitTimeMs, handlePollResult(coordinatorManager.poll(currentTimeMs)));
        }

        // if there are pending events to process, poll then continue without
        // blocking.
        if (!applicationEventQueue.isEmpty()) {
            networkClientDelegate.poll(time.timer(0), false);
            return;
        }
        // if there are no events to process, poll until timeout. The timeout
        // will be the minimum of the requestTimeoutMs, nextHeartBeatMs, and
        // nextMetadataUpdate. See NetworkClient.poll impl.
        networkClientDelegate.poll(time.timer(pollWaitTimeMs), false);
    }

    long handlePollResult(NetworkClientDelegate.PollResult res) {
        Objects.requireNonNull(res);
        if (!res.unsentRequests.isEmpty()) {
            networkClientDelegate.addAll(res.unsentRequests);
            return Long.MAX_VALUE;
        }
        return res.timeMsTillNextPoll;
    }

    /**
     * Check the coordinator if its connection is still active. Otherwise, mark it unknown and
     * return false.
     *
     * @return true if coordinator is active.
     */
    protected boolean coordinatorUnknown() {
        // If the current coordinator is unavailable, mark it unknown and disconnect it
        Node coordinator = coordinatorManager.coordinator();
        if (coordinator != null && networkClientDelegate.nodeUnavailable(coordinator)) {
            log.info("Requesting disconnect from last known coordinator {}", coordinator);
            networkClientDelegate.tryDisconnect(
                    this.coordinatorManager.markCoordinatorUnknown("coordinator unavailable"));
            return false;
        }
        return true;
    }

    private long timeToNextHeartbeatMs() {
        // TODO: implemented when heartbeat is added to the impl
        return 100;
    }

    private Optional<ApplicationEvent> maybePollEvent() {
        if (this.applicationEventQueue.isEmpty()) {
            return Optional.empty();
        }

        return Optional.ofNullable(this.applicationEventQueue.poll());
    }

    /**
     * ApplicationEvent are consumed here.
     *
     * @param event an {@link ApplicationEvent}
     * @return true when successfully consumed the event.
     */
    private boolean consumeApplicationEvent(final ApplicationEvent event) {
        log.debug("try consuming event: {}", Optional.ofNullable(event));
        Objects.requireNonNull(event);
        return applicationEventProcessor.process(event);
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
