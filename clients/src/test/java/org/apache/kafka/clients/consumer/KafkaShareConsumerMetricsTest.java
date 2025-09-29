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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_SHARE_METRIC_GROUP_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaShareConsumerMetricsTest {
    private final String topic = "test";
    private final Uuid topicId = Uuid.randomUuid();
    private final Map<String, Uuid> topicIds = Stream.of(
                    new AbstractMap.SimpleEntry<>(topic, topicId))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    private final Time time = new MockTime();
    private final SubscriptionState subscription = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST);
    private final String groupId = "mock-group";

    @Test
    public void testPollTimeMetrics() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 1));

        KafkaShareConsumer<String, String> consumer = newShareConsumer(time, client, subscription, metadata);
        consumer.subscribe(Collections.singletonList(topic));
        // MetricName objects to check
        Metrics metrics = consumer.metricsRegistry();
        MetricName lastPollSecondsAgoName = metrics.metricName("last-poll-seconds-ago", CONSUMER_SHARE_METRIC_GROUP_PREFIX + "-metrics");
        MetricName timeBetweenPollAvgName = metrics.metricName("time-between-poll-avg", CONSUMER_SHARE_METRIC_GROUP_PREFIX + "-metrics");
        MetricName timeBetweenPollMaxName = metrics.metricName("time-between-poll-max", CONSUMER_SHARE_METRIC_GROUP_PREFIX + "-metrics");
        // Test default values
        assertEquals(-1.0d, consumer.metrics().get(lastPollSecondsAgoName).metricValue());
        assertEquals(Double.NaN, consumer.metrics().get(timeBetweenPollAvgName).metricValue());
        assertEquals(Double.NaN, consumer.metrics().get(timeBetweenPollMaxName).metricValue());
        // Call first poll
        consumer.poll(Duration.ZERO);
        assertEquals(0.0d, consumer.metrics().get(lastPollSecondsAgoName).metricValue());
        assertEquals(0.0d, consumer.metrics().get(timeBetweenPollAvgName).metricValue());
        assertEquals(0.0d, consumer.metrics().get(timeBetweenPollMaxName).metricValue());
        // Advance time by 5,000 (total time = 5,000)
        time.sleep(5 * 1000L);
        assertEquals(5.0d, consumer.metrics().get(lastPollSecondsAgoName).metricValue());
        // Call second poll
        consumer.poll(Duration.ZERO);
        assertEquals(2.5 * 1000d, consumer.metrics().get(timeBetweenPollAvgName).metricValue());
        assertEquals(5 * 1000d, consumer.metrics().get(timeBetweenPollMaxName).metricValue());
        // Advance time by 10,000 (total time = 15,000)
        time.sleep(10 * 1000L);
        assertEquals(10.0d, consumer.metrics().get(lastPollSecondsAgoName).metricValue());
        // Call third poll
        consumer.poll(Duration.ZERO);
        assertEquals(5 * 1000d, consumer.metrics().get(timeBetweenPollAvgName).metricValue());
        assertEquals(10 * 1000d, consumer.metrics().get(timeBetweenPollMaxName).metricValue());
        // Advance time by 5,000 (total time = 20,000)
        time.sleep(5 * 1000L);
        assertEquals(5.0d, consumer.metrics().get(lastPollSecondsAgoName).metricValue());
        // Call fourth poll
        consumer.poll(Duration.ZERO);
        assertEquals(5 * 1000d, consumer.metrics().get(timeBetweenPollAvgName).metricValue());
        assertEquals(10 * 1000d, consumer.metrics().get(timeBetweenPollMaxName).metricValue());
    }

    @Test
    public void testPollIdleRatio() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 1));

        KafkaShareConsumer<String, String> consumer = newShareConsumer(time, client, subscription, metadata);
        // MetricName object to check
        Metrics metrics = consumer.metricsRegistry();
        MetricName pollIdleRatio = metrics.metricName("poll-idle-ratio-avg", CONSUMER_SHARE_METRIC_GROUP_PREFIX + "-metrics");
        // Test default value
        assertEquals(Double.NaN, consumer.metrics().get(pollIdleRatio).metricValue());

        // 1st poll
        // Spend 50ms in poll so value = 1.0
        consumer.kafkaShareConsumerMetrics().recordPollStart(time.milliseconds());
        time.sleep(50);
        consumer.kafkaShareConsumerMetrics().recordPollEnd(time.milliseconds());

        assertEquals(1.0d, consumer.metrics().get(pollIdleRatio).metricValue());

        // 2nd poll
        // Spend 50m outside poll and 0ms in poll so value = 0.0
        time.sleep(50);
        consumer.kafkaShareConsumerMetrics().recordPollStart(time.milliseconds());
        consumer.kafkaShareConsumerMetrics().recordPollEnd(time.milliseconds());

        // Avg of first two data points
        assertEquals((1.0d + 0.0d) / 2, consumer.metrics().get(pollIdleRatio).metricValue());

        // 3rd poll
        // Spend 25ms outside poll and 25ms in poll so value = 0.5
        time.sleep(25);
        consumer.kafkaShareConsumerMetrics().recordPollStart(time.milliseconds());
        time.sleep(25);
        consumer.kafkaShareConsumerMetrics().recordPollEnd(time.milliseconds());

        // Avg of three data points
        assertEquals((1.0d + 0.0d + 0.5d) / 3, consumer.metrics().get(pollIdleRatio).metricValue());
    }

    private static boolean consumerMetricPresent(KafkaShareConsumer<String, String> consumer, String name) {
        MetricName metricName = new MetricName(name, CONSUMER_SHARE_METRIC_GROUP_PREFIX + "-metrics", "", Collections.emptyMap());
        return consumer.metricsRegistry().metrics().containsKey(metricName);
    }

    @Test
    public void testClosingConsumerUnregistersConsumerMetrics() {
        Time time = new MockTime(1L);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 1));
        KafkaShareConsumer<String, String> consumer = newShareConsumer(time, client, subscription, metadata);
        consumer.subscribe(Collections.singletonList(topic));
        assertTrue(consumerMetricPresent(consumer, "last-poll-seconds-ago"));
        assertTrue(consumerMetricPresent(consumer, "time-between-poll-avg"));
        assertTrue(consumerMetricPresent(consumer, "time-between-poll-max"));
        consumer.close();
        assertFalse(consumerMetricPresent(consumer, "last-poll-seconds-ago"));
        assertFalse(consumerMetricPresent(consumer, "time-between-poll-avg"));
        assertFalse(consumerMetricPresent(consumer, "time-between-poll-max"));
    }

    private ConsumerMetadata createMetadata(SubscriptionState subscription) {
        return new ConsumerMetadata(0, 0, Long.MAX_VALUE, false, false,
                subscription, new LogContext(), new ClusterResourceListeners());
    }

    private KafkaShareConsumer<String, String> newShareConsumer(Time time,
                                                                KafkaClient client,
                                                                SubscriptionState subscription,
                                                                ConsumerMetadata metadata) {
        return newShareConsumer(
                time,
                client,
                subscription,
                metadata,
                groupId,
                Optional.of(new StringDeserializer())
        );
    }

    private KafkaShareConsumer<String, String> newShareConsumer(Time time,
                                                                KafkaClient client,
                                                                SubscriptionState subscriptions,
                                                                ConsumerMetadata metadata,
                                                                String groupId,
                                                                Optional<Deserializer<String>> valueDeserializerOpt) {
        String clientId = "mock-consumer";
        Deserializer<String> keyDeserializer = new StringDeserializer();
        Deserializer<String> valueDeserializer = valueDeserializerOpt.orElse(new StringDeserializer());
        LogContext logContext = new LogContext();
        ConsumerConfig config = newConsumerConfig(groupId, valueDeserializer);
        return new KafkaShareConsumer<>(
                logContext,
                clientId,
                groupId,
                config,
                keyDeserializer,
                valueDeserializer,
                time,
                client,
                subscriptions,
                metadata
        );
    }

    private ConsumerConfig newConsumerConfig(String groupId,
                                             Deserializer<String> valueDeserializer) {
        String clientId = "mock-consumer";
        long retryBackoffMs = 100;
        long retryBackoffMaxMs = 1000;
        int minBytes = 1;
        int maxBytes = Integer.MAX_VALUE;
        int maxWaitMs = 500;
        int fetchSize = 1024 * 1024;
        int maxPollRecords = Integer.MAX_VALUE;
        boolean checkCrcs = true;
        int rebalanceTimeoutMs = 60000;
        int defaultApiTimeoutMs = 60000;
        int requestTimeoutMs = defaultApiTimeoutMs / 2;

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.CHECK_CRCS_CONFIG, checkCrcs);
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        configs.put(ConsumerConfig.CLIENT_RACK_CONFIG, CommonClientConfigs.DEFAULT_CLIENT_RACK);
        configs.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutMs);
        configs.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maxBytes);
        configs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, maxWaitMs);
        configs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, minBytes);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, fetchSize);
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, rebalanceTimeoutMs);
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        configs.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        configs.put(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, retryBackoffMaxMs);
        configs.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());

        return new ConsumerConfig(configs);
    }
    private void initMetadata(MockClient mockClient, Map<String, Integer> partitionCounts) {
        Map<String, Uuid> metadataIds = new HashMap<>();
        for (String name : partitionCounts.keySet()) {
            metadataIds.put(name, topicIds.get(name));
        }
        MetadataResponse initialMetadata = RequestTestUtils.metadataUpdateWithIds(1, partitionCounts, metadataIds);

        mockClient.updateMetadata(initialMetadata);
    }
}
