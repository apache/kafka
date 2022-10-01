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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * An {@code EventHandler} that uses a single background thread to consume {@code ApplicationEvent} and produce
 * {@code BackgroundEvent} from the {@ConsumerBackgroundThread}.
 */
public class DefaultEventHandler implements EventHandler {
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final BackgroundThreadRunnable runnable;
    private final KafkaThread backgroundThread;

    public DefaultEventHandler(ConsumerConfig config, LogContext logcontext,
                               SubscriptionState subscriptionState,
                               Metrics metrics,
                               ClusterResourceListeners clusterResourceListeners,
                               Sensor fetcherThrottleTimeSensor,
                               ApiVersions apiVersions) {
        this.applicationEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventQueue = new LinkedBlockingQueue<>();
        this.runnable = new DefaultBackgroundThreadRunnable(
                config,
                logcontext,
                applicationEventQueue,
                backgroundEventQueue,
                subscriptionState,
                apiVersions,
                metrics,
                clusterResourceListeners,
                fetcherThrottleTimeSensor);
        this.backgroundThread = new KafkaThread("consumer_background_thread", runnable, true);
        backgroundThread.start();

    }

    // VisibleForTesting
    DefaultEventHandler(BackgroundThreadRunnable runnable,
                        BlockingQueue<ApplicationEvent> applicationEventQueue,
                        BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        this.runnable = runnable;
        this.backgroundThread = new KafkaThread("consumer_background_thread", runnable, true);
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
        backgroundThread.start();
    }

    // VisibleForTesting
    DefaultEventHandler(Time time,
                        ConsumerConfig config,
                        BlockingQueue<ApplicationEvent> applicationEventQueue,
                        BlockingQueue<BackgroundEvent> backgroundEventQueue,
                        SubscriptionState subscriptionState,
                        ConsumerMetadata metadata,
                        ConsumerNetworkClient networkClient) {
        this.runnable = new DefaultBackgroundThreadRunnable(
                time,
                config,
                applicationEventQueue,
                backgroundEventQueue,
                subscriptionState,
                metadata,
                networkClient);
        this.backgroundThread = new KafkaThread("consumer_background_thread", runnable, true);
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
        backgroundThread.start();
    }

    @Override
    public Optional<BackgroundEvent> poll() {
        return Optional.ofNullable(backgroundEventQueue.poll());
    }

    @Override
    public boolean isEmpty() {
        return backgroundEventQueue.isEmpty();
    }

    @Override
    public boolean add(ApplicationEvent event) {
        try {
            return applicationEventQueue.add(event);
        } catch (IllegalStateException e) {
            // swallow the capacity restriction exception
            return false;
        }
    }

    public void close() {
        try {
            this.runnable.close();
            // close logic
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
