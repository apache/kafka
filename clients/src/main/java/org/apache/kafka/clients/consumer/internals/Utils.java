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

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class Utils {

    public static final String CONSUMER_JMX_PREFIX = "kafka.consumer";
    public static final String CONSUMER_METRIC_GROUP_PREFIX = "consumer";

    /**
     * A fixed, large enough value will suffice for max.
     */
    public static final int CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION = 100;

    private static final String CONSUMER_CLIENT_ID_METRIC_TAG = "client-id";

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

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

    public static IsolationLevel getConfiguredIsolationLevel(ConsumerConfig config) {
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

    public static <K, V> FetchConfig<K, V> createFetchConfig(ConsumerConfig config, Deserializers<K, V> deserializers) {
        IsolationLevel isolationLevel = getConfiguredIsolationLevel(config);
        return new FetchConfig<>(config, deserializers, isolationLevel);
    }

    public static <K, V> FetchConfig<K, V> createFetchConfig(ConsumerConfig config) {
        Deserializers<K, V> deserializers = new Deserializers<>(config);
        return createFetchConfig(config, deserializers);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> List<ConsumerInterceptor<K, V>> getConfiguredConsumerInterceptors(ConsumerConfig config) {
        return (List<ConsumerInterceptor<K, V>>) ClientUtils.getConfiguredInterceptors(config, ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptor.class);
    }

    public static boolean refreshCommittedOffsets(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                  final ConsumerMetadata metadata,
                                                  final SubscriptionState subscriptions) {
        if (offsets == null) return false;

        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
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
        return true;
    }

    final static class PartitionComparator implements Comparator<TopicPartition>, Serializable {
        private static final long serialVersionUID = 1L;
        private Map<String, List<String>> map;

        PartitionComparator(Map<String, List<String>> map) {
            this.map = map;
        }

        @Override
        public int compare(TopicPartition o1, TopicPartition o2) {
            int ret = map.get(o1.topic()).size() - map.get(o2.topic()).size();
            if (ret == 0) {
                ret = o1.topic().compareTo(o2.topic());
                if (ret == 0)
                    ret = o1.partition() - o2.partition();
            }
            return ret;
        }
    }

    public final static class TopicPartitionComparator implements Comparator<TopicPartition>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(TopicPartition topicPartition1, TopicPartition topicPartition2) {
            String topic1 = topicPartition1.topic();
            String topic2 = topicPartition2.topic();

            if (topic1.equals(topic2)) {
                return topicPartition1.partition() - topicPartition2.partition();
            } else {
                return topic1.compareTo(topic2);
            }
        }
    }
}
