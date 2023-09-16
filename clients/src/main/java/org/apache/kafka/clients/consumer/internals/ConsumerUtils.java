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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
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

    public static final String CONSUMER_JMX_PREFIX = "kafka.consumer";
    public static final String CONSUMER_METRIC_GROUP_PREFIX = "consumer";

    /**
     * A fixed, large enough value will suffice for max.
     */
    public static final int CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION = 100;

    private static final String CONSUMER_CLIENT_ID_METRIC_TAG = "client-id";

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

    public static <K, V> FetchConfig<K, V> createFetchConfig(ConsumerConfig config,
                                                             Deserializers<K, V> deserializers) {
        IsolationLevel isolationLevel = configuredIsolationLevel(config);
        return new FetchConfig<>(config, deserializers, isolationLevel);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> List<ConsumerInterceptor<K, V>> configuredConsumerInterceptors(ConsumerConfig config) {
        return (List<ConsumerInterceptor<K, V>>) ClientUtils.configuredInterceptors(config, ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptor.class);
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
