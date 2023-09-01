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

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

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
    private final Supplier<ApplicationEventProcessor<K, V>> applicationEventProcessorSupplier;
    private final Supplier<NetworkClientDelegate> networkClientDelegateSupplier;
    private final Supplier<RequestManagers<K, V>> requestManagersSupplier;
    // empty if groupId is null
    private ApplicationEventProcessor<K, V> applicationEventProcessor;
    private NetworkClientDelegate networkClientDelegate;
    private RequestManagers<K, V> requestManagers;
    private volatile boolean running;
    private final IdempotentCloser closer = new IdempotentCloser();

    public DefaultBackgroundThread(Time time,
                                   LogContext logContext,
                                   BlockingQueue<ApplicationEvent> applicationEventQueue,
                                   Supplier<ApplicationEventProcessor<K, V>> applicationEventProcessorSupplier,
                                   Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                   Supplier<RequestManagers<K, V>> requestManagersSupplier) {
        super(BACKGROUND_THREAD_NAME, true);
        this.time = time;
        this.log = logContext.logger(getClass());
        this.applicationEventQueue = applicationEventQueue;
        this.applicationEventProcessorSupplier = applicationEventProcessorSupplier;
        this.networkClientDelegateSupplier = networkClientDelegateSupplier;
        this.requestManagersSupplier = requestManagersSupplier;
    }

    @Override
    public void run() {
        closer.maybeThrowIllegalStateException("Consumer background thread is already closed");
        running = true;

        try {
            log.debug("Background thread started");

            // Wait until we're securely in the background thread to initialize these objects...
            initializeResources();

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
            log.debug("Background thread closed");
        }
    }

    void initializeResources() {
        applicationEventProcessor = applicationEventProcessorSupplier.get();
        networkClientDelegate = networkClientDelegateSupplier.get();
        requestManagers = requestManagersSupplier.get();
    }

    /**
     * Poll and process an {@link ApplicationEvent}. It performs the following tasks:
     * 1. Drains and try to process all the requests in the queue.
     * 2. Iterate through the registry, poll, and get the next poll time for the network poll
     * 3. Poll the networkClient to send and retrieve the response.
     */
    void runOnce() {
        LinkedList<ApplicationEvent> events = new LinkedList<>();
        applicationEventQueue.drainTo(events);

        for (ApplicationEvent event : events) {
            log.trace("Dequeued event: {}", event);
            Objects.requireNonNull(event);
            applicationEventProcessor.process(event);
        }

        final long currentTimeMs = time.milliseconds();
        final long pollWaitTimeMs = requestManagers.entries().stream()
                .filter(Optional::isPresent)
                .map(m -> m.get().poll(currentTimeMs))
                .filter(Objects::nonNull)
                .map(this::handlePollResult)
                .reduce(MAX_POLL_TIMEOUT_MS, Math::min);
        networkClientDelegate.maybeTryConnect();
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
        if (networkClientDelegate != null)
            networkClientDelegate.wakeup();
    }

    @Override
    public void close() {
        closer.close(() -> {
            log.debug("Closing the consumer background thread");
            running = false;
            wakeup();
            Utils.closeQuietly(requestManagers, "Request managers client");
            Utils.closeQuietly(networkClientDelegate, "network client utils");
            log.debug("Closed the consumer background thread");
            drainAndComplete();
        }, () -> log.warn("The consumer background thread was previously closed"));
    }


    /**
     * It is possible for the background thread to close before complete processing all the events in the queue. In
     * this case, we need throw an exception to notify the user the consumer is closed.
     */
    private void drainAndComplete() {
        List<ApplicationEvent> incompletedEvents = new ArrayList<>();
        applicationEventQueue.drainTo(incompletedEvents);
        incompletedEvents.forEach(event -> {
            if (event instanceof CompletableApplicationEvent) {
                ((CompletableApplicationEvent<?>) event).future().completeExceptionally(
                    new KafkaException("The consumer is closed"));
            }
        });
        log.debug("Discarding {} events because the consumer is closed", incompletedEvents.size());
    }
}
