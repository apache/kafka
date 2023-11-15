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
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.utils.Timer;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public final class ConsumerUtils {

    /**
     * This configuration has only package-level visibility in {@link ConsumerConfig}, so it's inaccessible in the
     * internals package where most of its uses live. Attempts were made to move things around, but it was deemed
     * better to leave it as is.
     */
    static final String THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED = "internal.throw.on.fetch.stable.offset.unsupported";
    public static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;
    public static final String CONSUMER_JMX_PREFIX = "kafka.consumer";
    public static final String CONSUMER_METRIC_GROUP_PREFIX = "consumer";

    /**
     * A fixed, large enough value will suffice for max.
     */
    public static final int CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION = 100;

    private static final String CONSUMER_CLIENT_ID_METRIC_TAG = "client-id";
    private static final Logger log = LoggerFactory.getLogger(ConsumerUtils.class);

    public static ConsumerNetworkClient createConsumerNetworkClient(ConsumerConfig config,
                                                                    Metrics metrics,
                                                                    LogContext logContext,
                                                                    ApiVersions apiVersions,
                                                                    Time time,
                                                                    Metadata metadata,
                                                                    Sensor throttleTimeSensor,
                                                                    long retryBackoffMs) {
        NetworkClient netClient = ClientUtils.createNetworkClient(config,
                metrics,
                CONSUMER_METRIC_GROUP_PREFIX,
                logContext,
                apiVersions,
                time,
                CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
                metadata,
                throttleTimeSensor);

        // Will avoid blocking an extended period of time to prevent heartbeat thread starvation
        int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);

        return new ConsumerNetworkClient(
                logContext,
                netClient,
                metadata,
                time,
                retryBackoffMs,
                config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                heartbeatIntervalMs);
    }

    public static LogContext createLogContext(ConsumerConfig config, GroupRebalanceConfig groupRebalanceConfig) {
        Optional<String> groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);

        // If group.instance.id is set, we will append it to the log context.
        if (groupRebalanceConfig.groupInstanceId.isPresent()) {
            return new LogContext("[Consumer instanceId=" + groupRebalanceConfig.groupInstanceId.get() +
                    ", clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] ");
        } else {
            return new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] ");
        }
    }

    public static IsolationLevel configuredIsolationLevel(ConsumerConfig config) {
        String s = config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT);
        return IsolationLevel.valueOf(s);
    }

    public static SubscriptionState createSubscriptionState(ConsumerConfig config, LogContext logContext) {
        String s = config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT);
        OffsetResetStrategy strategy = OffsetResetStrategy.valueOf(s);
        return new SubscriptionState(logContext, strategy);
    }

    public static Metrics createMetrics(ConsumerConfig config, Time time) {
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
        Map<String, String> metricsTags = Collections.singletonMap(CONSUMER_CLIENT_ID_METRIC_TAG, clientId);
        MetricConfig metricConfig = new MetricConfig()
                .samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricsTags);
        List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
        MetricsContext metricsContext = new KafkaMetricsContext(CONSUMER_JMX_PREFIX,
                config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
        return new Metrics(metricConfig, reporters, time, metricsContext);
    }

    public static FetchMetricsManager createFetchMetricsManager(Metrics metrics) {
        Set<String> metricsTags = Collections.singleton(CONSUMER_CLIENT_ID_METRIC_TAG);
        FetchMetricsRegistry metricsRegistry = new FetchMetricsRegistry(metricsTags, CONSUMER_METRIC_GROUP_PREFIX);
        return new FetchMetricsManager(metrics, metricsRegistry);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> List<ConsumerInterceptor<K, V>> configuredConsumerInterceptors(ConsumerConfig config) {
        return (List<ConsumerInterceptor<K, V>>) ClientUtils.configuredInterceptors(config, ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptor.class);
    }

    /**
     * Update subscription state and metadata using the provided committed offsets:
     * <li>Update partition offsets with the committed offsets</li>
     * <li>Update the metadata with any newer leader epoch discovered in the committed offsets
     * metadata</li>
     * </p>
     * This will ignore any partition included in the <code>offsetsAndMetadata</code> parameter that
     * may no longer be assigned.
     *
     * @param offsetsAndMetadata Committed offsets and metadata to be used for updating the
     *                           subscription state and metadata object.
     * @param metadata           Metadata object to update with a new leader epoch if discovered in the
     *                           committed offsets' metadata.
     * @param subscriptions      Subscription state to update, setting partitions' offsets to the
     *                           committed offsets.
     */
    public static void refreshCommittedOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata,
                                               final ConsumerMetadata metadata,
                                               final SubscriptionState subscriptions) {
        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsAndMetadata.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata != null) {
                // first update the epoch if necessary
                entry.getValue().leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(entry.getKey(), epoch));

                // it's possible that the partition is no longer assigned when the response is received,
                // so we need to ignore seeking if that's the case
                if (subscriptions.isAssigned(tp)) {
                    final ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);
                    final SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                            offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(),
                            leaderAndEpoch);

                    subscriptions.seekUnvalidated(tp, position);

                    log.info("Setting offset for partition {} to the committed offset {}", tp, position);
                } else {
                    log.info("Ignoring the returned {} since its partition {} is no longer assigned",
                            offsetAndMetadata, tp);
                }
            }
        }
    }

    public static <T> T getResult(CompletableFuture<T> future, Timer timer) {
        try {
            return future.get(timer.remainingMs(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();

            if (t instanceof WakeupException)
                throw new WakeupException();
            else if (t instanceof KafkaException)
                throw (KafkaException) t;
            else
                throw new KafkaException(t);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new TimeoutException(e);
        }
    }
}
