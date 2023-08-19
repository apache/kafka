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
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
        String groupId = String.valueOf(groupRebalanceConfig.groupId);
        String clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        String logPrefix;
        String groupInstanceId = groupRebalanceConfig.groupInstanceId.orElse(null);

        if (groupInstanceId != null) {
            // If group.instance.id is set, we will append it to the log context.
            logPrefix = String.format("[Consumer instanceId=%s, clientId=%s, groupId=%s] ", groupInstanceId, clientId, groupId);
        } else {
            logPrefix = String.format("[Consumer clientId=%s, groupId=%s] ", clientId, groupId);
        }

        return new LogContext(logPrefix);
    }

    public static IsolationLevel createIsolationLevel(ConsumerConfig config) {
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
                                                             Deserializer<K> keyDeserializer,
                                                             Deserializer<V> valueDeserializer) {
        IsolationLevel isolationLevel = createIsolationLevel(config);
        return new FetchConfig<>(config, keyDeserializer, valueDeserializer, isolationLevel);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> List<ConsumerInterceptor<K, V>> createConsumerInterceptors(ConsumerConfig config) {
        return ClientUtils.createConfiguredInterceptors(config,
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                ConsumerInterceptor.class);
    }

    @SuppressWarnings("unchecked")
    public static <K> Deserializer<K> createKeyDeserializer(ConsumerConfig config, Deserializer<K> keyDeserializer) {
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);

        if (keyDeserializer == null) {
            keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            keyDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), true);
        } else {
            config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        }

        return keyDeserializer;
    }

    @SuppressWarnings("unchecked")
    public static <V> Deserializer<V> createValueDeserializer(ConsumerConfig config, Deserializer<V> valueDeserializer) {
        String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);

        if (valueDeserializer == null) {
            valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            valueDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), false);
        } else {
            config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        }

        return valueDeserializer;
    }
}
