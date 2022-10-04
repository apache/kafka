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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

/**
 * The background process of the {@code DefaultEventHandler} that consumes {@code ApplicationEvent} and produces
 * {@code BackgroundEvent}. It owns the network client and handles all the network IO to the brokers.
 *
 * It holds a reference to the {@link SubscriptionState}, which is initialized by the polling thread.
 */
public class DefaultBackgroundThreadRunnable implements BackgroundThreadRunnable {
    private static final String CLIENT_ID_METRIC_TAG = "client-id";
    private static final String METRIC_GRP_PREFIX = "consumer";

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

    public DefaultBackgroundThreadRunnable(ConsumerConfig config,
                                           LogContext logContext,
                                           BlockingQueue<ApplicationEvent> applicationEventQueue,
                                           BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                           SubscriptionState subscriptions,
                                           ApiVersions apiVersions,
                                           Metrics metrics,
                                           ClusterResourceListeners clusterResourceListeners,
                                           Sensor fetcherThrottleTimeSensor) {
        this(Time.SYSTEM,
                config,
                logContext,
                applicationEventQueue,
                backgroundEventQueue,
                subscriptions,
                apiVersions,
                metrics,
                clusterResourceListeners,
                fetcherThrottleTimeSensor);
    }

    public DefaultBackgroundThreadRunnable(Time time,
                                           ConsumerConfig config,
                                           LogContext logContext,
                                           BlockingQueue<ApplicationEvent> applicationEventQueue,
                                           BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                           SubscriptionState subscriptions,
                                           ApiVersions apiVersions,
                                           Metrics metrics,
                                           ClusterResourceListeners clusterResourceListeners,
                                           Sensor fetcherThrottleTimeSensor) {
        try {
            this.time = time;
            this.log = logContext.logger(DefaultBackgroundThreadRunnable.class);
            this.applicationEventQueue = applicationEventQueue;
            this.backgroundEventQueue = backgroundEventQueue;
            this.config = config;
            setConfig();
            this.inflightEvent = Optional.empty();
            this.subscriptions = subscriptions; // subscriptionState is initialized in the polling thread and passed here.
            this.metrics = metrics;
            this.metadata = bootstrapMetadata(clusterResourceListeners, logContext);
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
            NetworkClient netClient = new NetworkClient(
                    new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, METRIC_GRP_PREFIX, channelBuilder, logContext),
                    metadata,
                    clientId,
                    100, // a fixed large enough value will suffice for max in-flight requests
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                    config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                    config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                    time,
                    true,
                    apiVersions,
                    fetcherThrottleTimeSensor,
                    logContext);
            this.networkClient = new ConsumerNetworkClient(
                    logContext,
                    netClient,
                    metadata,
                    time,
                    retryBackoffMs,
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                    heartbeatIntervalMs);
            this.running = true;
        } catch (Exception e) {
            // now propagate the exception
            close();
            throw new KafkaException("Failed to construct background processor", e);
        }
    }

    // VisibleForTesting
    DefaultBackgroundThreadRunnable(Time time,
                                    ConsumerConfig config,
                                    BlockingQueue<ApplicationEvent> applicationEventQueue,
                                    BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                    SubscriptionState subscriptions,
                                    ConsumerMetadata metadata,
                                    ConsumerNetworkClient client) {
        this.time = time;
        this.config = config;
        setConfig();
        this.log = LoggerFactory.getLogger(getClass());
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.networkClient = client;
        this.metrics = new Metrics();
        this.running = true;
    }

    private void setConfig() {
        this.retryBackoffMs = this.config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        this.heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
    }

    @Override
    public void run() {
        try {
            log.debug("{} started", getClass());
            while (running) {
                pollOnce();
                time.sleep(retryBackoffMs);
            }
        } catch (InterruptException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            // TODO: Define fine grain exceptions here
        } finally {
            log.debug("{} closed", getClass());
        }
    }

    /**
     * Process event from a single poll
     */
    void pollOnce() {
        this.inflightEvent = maybePollEvent();
        if (this.inflightEvent.isPresent() && maybeConsumeInflightEvent(this.inflightEvent.get())) {
            // clear inflight event upon successful consumption
            this.inflightEvent = Optional.empty();
        }
        networkClient.pollNoWakeup();
    }

    public Optional<ApplicationEvent> maybePollEvent() {
        if (this.inflightEvent.isPresent() || this.applicationEventQueue.isEmpty()) {
            return this.inflightEvent;
        }
        return Optional.ofNullable(this.applicationEventQueue.poll());
    }

    /**
     * ApplicationEvent are consumed here.
     * @param event an {@link ApplicationEvent}
     * @return true when successfully consumed the event.
     */
    public boolean maybeConsumeInflightEvent(ApplicationEvent event) {
        log.debug("try consuming event: {}", Optional.ofNullable(event));
        switch (event.type) {
            case NOOP:
                process((NoopApplicationEvent) event);
                return true;
            default:
                inflightEvent = Optional.empty();
                log.warn("unsupported event type: {}", event.type);
                return true;
        }
    }

    /**
     * Processes {@link NoopApplicationEvent} and equeue a {@link NoopBackgroundEvent}. This is intentionally left here
     * for demonstration purpose.
     * @param event a {@link NoopApplicationEvent}
     */
    private void process(NoopApplicationEvent event) {
        backgroundEventQueue.add(new NoopBackgroundEvent(event.message));
    }

    @Override
    public void close() {
        this.running = false;
        Utils.closeQuietly(networkClient, "consumer network client");
        Utils.closeQuietly(metadata, "consumer network client");
    }

    private ConsumerMetadata bootstrapMetadata(ClusterResourceListeners clusterResourceListeners, LogContext logContext) {
        ConsumerMetadata metadata = new ConsumerMetadata(retryBackoffMs,
                config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
                !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG),
                config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                this.subscriptions,
                logContext, clusterResourceListeners);
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG));
        metadata.bootstrap(addresses);
        return metadata;
    }
}
