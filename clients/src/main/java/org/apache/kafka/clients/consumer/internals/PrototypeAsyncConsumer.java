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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This prototype consumer uses the EventHandler to process application
 * events so that the network IO can be processed in a background thread. Visit
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/Proposal%3A+Consumer+Threading+Model+Refactor" >this document</a>
 * for detail implementation.
 */
public abstract class PrototypeAsyncConsumer<K, V> implements Consumer<K, V> {
    private static final String CLIENT_ID_METRIC_TAG = "client-id";
    private static final String JMX_PREFIX = "kafka.consumer";

    private final LogContext logContext;
    private final EventHandler eventHandler;
    private final Time time;
    private final Optional<String> groupId;
    private final String clientId;
    private final Logger log;
    private final SubscriptionState subscriptions;
    private final Metrics metrics;

    @SuppressWarnings("unchecked")
    public PrototypeAsyncConsumer(final Time time,
                                  final ConsumerConfig config,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer) {
        this.time = time;
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                GroupRebalanceConfig.ProtocolType.CONSUMER);
        this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
        this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);

        // If group.instance.id is set, we will append it to the log context.
        if (groupRebalanceConfig.groupInstanceId.isPresent()) {
            logContext = new LogContext("[Consumer instanceId=" + groupRebalanceConfig.groupInstanceId.get() +
                    ", clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] ");
        } else {
            logContext = new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] ");
        }
        this.log = logContext.logger(getClass());
        OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.valueOf(config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
        this.subscriptions = new SubscriptionState(logContext, offsetResetStrategy);
        this.metrics = buildMetrics(config, time, clientId);
        List<ConsumerInterceptor<K, V>> interceptorList = (List) config.getConfiguredInstances(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                ConsumerInterceptor.class,
                Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId));
        ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keyDeserializer,
                valueDeserializer, metrics.reporters(), interceptorList);
        this.eventHandler = new DefaultEventHandler(
                config,
                logContext,
                subscriptions,
                new ApiVersions(),
                this.metrics,
                clusterResourceListeners,
                null // this is coming from the fetcher, but we don't have one
        );
    }


    /**
     * poll implementation using {@link EventHandler}.
     *  1. Poll for background events. If there's a fetch response event, process the record and return it. If it is
     *  another type of event, process it.
     *  2. Send fetches if needed.
     *  If the timeout expires, return an empty ConsumerRecord.
     *
     * @param timeout timeout of the poll loop
     * @return ConsumerRecord.  It can be empty if time timeout expires.
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        try {
            do {
                if (!eventHandler.isEmpty()) {
                    final Optional<BackgroundEvent> backgroundEvent = eventHandler.poll();
                    // processEvent() may process 3 types of event:
                    // 1. Errors
                    // 2. Callback Invocation
                    // 3. Fetch responses
                    // Errors will be handled or rethrown.
                    // Callback invocation will trigger callback function execution, which is blocking until completion.
                    // Successful fetch responses will be added to the completedFetches in the fetcher, which will then
                    // be processed in the collectFetches().
                    backgroundEvent.ifPresent(event -> processEvent(event, timeout));
                }
                // The idea here is to have the background thread sending fetches autonomously, and the fetcher
                // uses the poll loop to retrieve successful fetchResponse and process them on the polling thread.
                final Fetch<K, V> fetch = collectFetches();
                if (!fetch.isEmpty()) {
                    return processFetchResults(fetch);
                }
                // We will wait for retryBackoffMs
            } while (time.timer(timeout).notExpired());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return ConsumerRecords.empty();
    }

    abstract void processEvent(final BackgroundEvent backgroundEvent,
                               final Duration timeout);

    abstract ConsumerRecords<K, V> processFetchResults(final Fetch<K, V> fetch);

    abstract Fetch<K, V> collectFetches();

    /**
     * This method sends a commit event to the EventHandler and return.
     */
    @Override
    public void commitAsync() {
        final ApplicationEvent commitEvent = new CommitApplicationEvent();
        eventHandler.add(commitEvent);
    }

    /**
     * This method sends a commit event to the EventHandler and waits for
     * the event to finish.
     *
     * @param timeout max wait time for the blocking operation.
     */
    @Override
    public void commitSync(final Duration timeout) {
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent();
        eventHandler.add(commitEvent);

        final CompletableFuture<Void> commitFuture = commitEvent.commitFuture;
        try {
            commitFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            throw new org.apache.kafka.common.errors.TimeoutException(
                     "timeout");
        } catch (final Exception e) {
            // handle exception here
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the current subscription.  or an empty set if no such call has
     * been made.
     * @return The set of topics currently subscribed to
     */
    public Set<String> subscription() {
        return Collections.unmodifiableSet(this.subscriptions.subscription());
    }

    /**
     * A stubbed ApplicationEvent for demonstration purpose
     */
    private class CommitApplicationEvent extends ApplicationEvent {
        // this is the stubbed commitAsyncEvents
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();

        public CommitApplicationEvent() {
        }

        @Override
        public boolean process() {
            return true;
        }
    }

    private static <K, V> ClusterResourceListeners configureClusterResourceListeners(
            final Deserializer<K> keyDeserializer,
            final Deserializer<V> valueDeserializer,
            final List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keyDeserializer);
        clusterResourceListeners.maybeAdd(valueDeserializer);
        return clusterResourceListeners;
    }

    private static Metrics buildMetrics(
            final ConsumerConfig config,
            final Time time,
            final String clientId) {
        Map<String, String> metricsTags = Collections.singletonMap(CLIENT_ID_METRIC_TAG, clientId);
        MetricConfig metricConfig = new MetricConfig()
                .samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricsTags);
        List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
        MetricsContext metricsContext = new KafkaMetricsContext(
                JMX_PREFIX,
                config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
        return new Metrics(metricConfig, reporters, time, metricsContext);
    }
}
