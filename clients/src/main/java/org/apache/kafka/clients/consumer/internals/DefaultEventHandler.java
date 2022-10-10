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

import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * An {@code EventHandler} that uses a single background thread to consume {@code ApplicationEvent} and produce
 * {@code BackgroundEvent} from the {@ConsumerBackgroundThread}.
 */
public class DefaultEventHandler implements EventHandler {
    private static final String METRIC_GRP_PREFIX = "consumer";
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final KafkaThread backgroundThread;

    public DefaultEventHandler(Time time,
                        ConsumerConfig config,
                        LogContext logContext,
                        BlockingQueue<ApplicationEvent> applicationEventQueue,
                        BlockingQueue<BackgroundEvent> backgroundEventQueue,
                        SubscriptionState subscriptionState,
                        ConsumerMetadata metadata,
                        ConsumerNetworkClient networkClient) {
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
        this.backgroundThread = new DefaultBackgroundThread(
                time,
                config,
                logContext,
                this.applicationEventQueue,
                this.backgroundEventQueue,
                subscriptionState,
                metadata,
                networkClient,
                new Metrics(time));
        backgroundThread.start();
    }

    // VisibleForTesting
    DefaultEventHandler(KafkaThread backgroundThread,
                        BlockingQueue<ApplicationEvent> applicationEventQueue,
                        BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        this.backgroundThread = backgroundThread;
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
        backgroundThread.start();
    }

    @Override
    public Optional<BackgroundEvent> poll() {
        return Optional.ofNullable(backgroundEventQueue.poll());
    }

    @Override
    public Optional<BackgroundEvent> poll(Duration timeout) {
        try {
            return Optional.ofNullable(backgroundEventQueue.poll(timeout.toMillis(),
                    TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
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

    private ConsumerMetadata bootstrapMetadata(
            LogContext logContext,
            ClusterResourceListeners clusterResourceListeners,
            ConsumerConfig config,
            SubscriptionState subscriptions) {
        ConsumerMetadata metadata = new ConsumerMetadata(
                config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
                !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG),
                config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                subscriptions,
                logContext, clusterResourceListeners);
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG));
        metadata.bootstrap(addresses);
        return metadata;
    }

    public void close() {
        try {
            // this.backgroundThread.interrupt();
            // close logic
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
