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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

/**
 * An {@link EventHandler} that uses a single background thread to consume {@link ApplicationEvent} and produce
 * {@link BackgroundEvent} from the {@link DefaultBackgroundThread}.
 */
public class DefaultEventHandler<K, V> implements EventHandler {

    private final Logger log;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final DefaultBackgroundThread<K, V> backgroundThread;
    private final IdempotentCloser closer = new IdempotentCloser();

    public DefaultEventHandler(final Time time,
                               final LogContext logContext,
                               final BlockingQueue<ApplicationEvent> applicationEventQueue,
                               final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                               final Supplier<ApplicationEventProcessor<K, V>> applicationEventProcessorSupplier,
                               final Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                               final Supplier<RequestManagers<K, V>> requestManagersSupplier) {
        this.log = logContext.logger(DefaultEventHandler.class);
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundThread = new DefaultBackgroundThread<>(time,
                logContext,
                applicationEventQueue,
                applicationEventProcessorSupplier,
                networkClientDelegateSupplier,
                requestManagersSupplier);
        this.backgroundThread.start();
    }

    @Override
    public boolean add(final ApplicationEvent event) {
        Objects.requireNonNull(event, "ApplicationEvent provided to add must be non-null");
        log.trace("Enqueued event: {}", event);
        backgroundThread.wakeup();
        return applicationEventQueue.add(event);
    }

    @Override
    public <T> T addAndGet(final CompletableApplicationEvent<T> event, final Timer timer) {
        Objects.requireNonNull(event, "CompletableApplicationEvent provided to addAndGet must be non-null");
        Objects.requireNonNull(timer, "Timer provided to addAndGet must be non-null");
        add(event);
        return event.get(timer);
    }

    public void close(final Duration timeout) {
        Objects.requireNonNull(timeout, "Duration provided to close must be non-null");

        closer.close(
                () ->  {
                    log.info("Closing the default consumer event handler");

                    try {
                        long timeoutMs = timeout.toMillis();

                        if (timeoutMs < 0)
                            throw new IllegalArgumentException("The timeout cannot be negative.");

                        backgroundThread.close();
                        log.info("The default consumer event handler was closed");
                    } catch (final Exception e) {
                        throw new KafkaException(e);
                    }
                },
                () -> log.info("The default consumer event handler was already closed"));
    }
}
