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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

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
    private final ConsumerNetworkClient networkClient;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final Metrics metrics;
    private final ConsumerConfig config;

    private String clientId;
    private long retryBackoffMs;
    private int heartbeatIntervalMs;
    private boolean running;
    private Optional<ApplicationEvent> inflightEvent = Optional.empty();
    private final AtomicReference<Optional<RuntimeException>> exception =
        new AtomicReference<>(Optional.empty());

    public DefaultBackgroundThread(final Time time,
                                   final ConsumerConfig config,
                                   final LogContext logContext,
                                   final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                   final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                   final SubscriptionState subscriptions,
                                   final ConsumerMetadata metadata,
                                   final ConsumerNetworkClient networkClient,
                                   final Metrics metrics) {
        super(BACKGROUND_THREAD_NAME, true);
        try {
            this.time = time;
            this.log = logContext.logger(DefaultBackgroundThread.class);
            this.applicationEventQueue = applicationEventQueue;
            this.backgroundEventQueue = backgroundEventQueue;
            this.config = config;
            setConfig();
            this.inflightEvent = Optional.empty();
            // subscriptionState is initialized by the polling thread
            this.subscriptions = subscriptions;
            this.metadata = metadata;
            this.networkClient = networkClient;
            this.metrics = metrics;
            this.running = true;
        } catch (final Exception e) {
            // now propagate the exception
            close();
            throw new KafkaException("Failed to construct background processor", e);
        }
    }

    private void setConfig() {
        this.retryBackoffMs = this.config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        this.heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
    }

    @Override
    public void run() {
        try {
            log.debug("Background thread started");
            while (running) {
                try {
                    runOnce();
                } catch (final WakeupException e) {
                    log.debug(
                        "Exception thrown, background thread won't terminate",
                        e
                    );
                    // swallow the wakeup exception to prevent killing the
                    // background thread.
                }
            }
        } catch (final Throwable t) {
            log.error(
                "The background thread failed due to unexpected error",
                t
            );
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
     * Process event from a single poll
     */
    void runOnce() {
        this.inflightEvent = maybePollEvent();
        if (this.inflightEvent.isPresent()) {
            log.debug("processing application event: {}", this.inflightEvent);
        }
        if (this.inflightEvent.isPresent() && maybeConsumeInflightEvent(this.inflightEvent.get())) {
            // clear inflight event upon successful consumption
            this.inflightEvent = Optional.empty();
        }

        // if there are pending events to process, poll then continue without
        // blocking.
        if (!applicationEventQueue.isEmpty() || inflightEvent.isPresent()) {
            networkClient.poll(time.timer(0));
            return;
        }
        // if there are no events to process, poll until timeout. The timeout
        // will be the minimum of the requestTimeoutMs, nextHeartBeatMs, and
        // nextMetadataUpdate. See NetworkClient.poll impl.
        networkClient.poll(time.timer(timeToNextHeartbeatMs(time.milliseconds())));
    }

    private long timeToNextHeartbeatMs(final long nowMs) {
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
        return event.process();
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
        networkClient.wakeup();
    }

    public void close() {
        this.running = false;
        this.wakeup();
        Utils.closeQuietly(networkClient, "consumer network client");
        Utils.closeQuietly(metadata, "consumer metadata client");
    }
}
