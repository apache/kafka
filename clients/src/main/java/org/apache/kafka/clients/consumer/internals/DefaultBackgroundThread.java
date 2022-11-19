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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

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
    private final KafkaClient networkClient;
    private final ConsumerMetadata metadata;
    private final ConsumerConfig config;
    private final CoordinatorManager coordinatorManager;
    private final ApplicationEventProcessor applicationEventProcessor;
    private final NetworkClientUtils networkClientUtils;

    private boolean running;
    private Optional<ApplicationEvent> inflightEvent;
    private final AtomicReference<Optional<RuntimeException>> exception =
        new AtomicReference<>(Optional.empty());

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
            this.log = logContext.logger(DefaultBackgroundThread.class);
            this.applicationEventQueue = applicationEventQueue;
            this.backgroundEventQueue = backgroundEventQueue;
            this.config = config;
            this.inflightEvent = Optional.empty();
            // subscriptionState is initialized by the polling thread
            this.metadata = metadata;
            this.networkClient = networkClient;
            this.networkClientUtils = new NetworkClientUtils(
                    this.time,
                    networkClient);
            this.running = true;
            this.coordinatorManager = new CoordinatorManager(time,
                    logContext,
                    config,
                    backgroundEventQueue,
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
            if (t instanceof RuntimeException)
                this.exception.set(Optional.of((RuntimeException) t));
            else
                this.exception.set(Optional.of(new RuntimeException(t)));
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
        // TODO: we might not need the inflightEvent here
        this.inflightEvent = maybePollEvent();
        if (this.inflightEvent.isPresent()) {
            log.debug("processing application event: {}", this.inflightEvent);
        }
        if (this.inflightEvent.isPresent() && maybeConsumeInflightEvent(this.inflightEvent.get())) {
            // clear inflight event upon successful consumption
            this.inflightEvent = Optional.empty();
        }

        if (shouldFindCoordinator() && coordinatorUnknown()) {
            coordinatorManager.tryFindCoordinator().ifPresent(networkClientUtils::add);
        }

        // if there are pending events to process, poll then continue without
        // blocking.
        if (!applicationEventQueue.isEmpty() || inflightEvent.isPresent()) {
            handleNetworkResponses(networkClientUtils.poll());
            return;
        }
        // if there are no events to process, poll until timeout. The timeout
        // will be the minimum of the requestTimeoutMs, nextHeartBeatMs, and
        // nextMetadataUpdate. See NetworkClient.poll impl.
        handleNetworkResponses(networkClientUtils.poll(
                time.timer(timeToNextHeartbeatMs()), false));
    }

    /**
     * Get the coordinator if its connection is still active. Otherwise mark it unknown and
     * return null.
     *
     * @return the current coordinator or null if it is unknown
     */
    protected synchronized Node checkAndGetCoordinator(Node coordinator) {
        if (coordinator != null && networkClientUtils.isUnavailable(coordinator)) {
            this.coordinatorManager.markCoordinatorUnknown(true, "coordinator unavailable");
            return null;
        }
        return coordinator;
    }

    public boolean coordinatorUnknown() {
        return checkAndGetCoordinator(coordinatorManager.coordinator()) == null;
    }

    private boolean shouldFindCoordinator() {
        // TODO: add conditions for coordinator discovery. Example: when there are pending commits, or we have
        //  rebalance in progress.
        return true;
    }

    private void handleNetworkResponses(List<ClientResponse> res) {
        res.forEach(this::handleNetworkResponse);
    }

    private void handleNetworkResponse(ClientResponse res) {
        //res.requestHeader().correlationId() // incremented very time we send the request
        if (res.responseBody() instanceof FindCoordinatorResponse) {
            FindCoordinatorResponse response = (FindCoordinatorResponse) res.responseBody();
            coordinatorManager.onResponse(response);
        }
    }

    private long timeToNextHeartbeatMs() {
        // TODO: implemented when heartbeat is added to the impl
        return 100;
    }

    private Optional<ApplicationEvent> maybePollEvent() {
        if (this.inflightEvent.isPresent() || this.applicationEventQueue.isEmpty()) {
            return this.inflightEvent;
        }
        return Optional.ofNullable(this.applicationEventQueue.poll());
    }

    /**
     * ApplicationEvent are consumed here.
     *
     * @param event an {@link ApplicationEvent}
     * @return true when successfully consumed the event.
     */
    private boolean maybeConsumeInflightEvent(final ApplicationEvent event) {
        log.debug("try consuming event: {}", Optional.ofNullable(event));
        Objects.requireNonNull(event);
        return applicationEventProcessor.process(event);
    }

    /**
     * Processes {@link NoopApplicationEvent} and equeue a
     * {@link NoopBackgroundEvent}. This is intentionally left here for
     * demonstration purpose.
     *
     * @param event a {@link NoopApplicationEvent}
     */
    private void process(final NoopApplicationEvent event) {
        backgroundEventQueue.add(new NoopBackgroundEvent(event.message));
    }

    public boolean isRunning() {
        return this.running;
    }

    public void wakeup() {
        networkClientUtils.wakeup();
    }

    public void close() {
        this.running = false;
        this.wakeup();
        Utils.closeQuietly(networkClient, "network client");
        Utils.closeQuietly(metadata, "consumer metadata client");
    }
}
