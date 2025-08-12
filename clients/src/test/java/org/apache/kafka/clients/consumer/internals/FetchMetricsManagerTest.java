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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.clients.consumer.internals.FetchMetricsManager.topicPartitionTags;
import static org.apache.kafka.clients.consumer.internals.FetchMetricsManager.topicTags;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FetchMetricsManagerTest {

    private static final double EPSILON = 0.0001;

    private final Time time = new MockTime(1, 0, 0);
    private static final String TOPIC_NAME = "test";

    private Metrics metrics;
    private FetchMetricsRegistry metricsRegistry;
    private FetchMetricsManager metricsManager;


    @BeforeEach
    public void setup() {
        metrics = new Metrics(time);
        metricsRegistry = new FetchMetricsRegistry(metrics.config().tags().keySet(), "test");
        metricsManager = new FetchMetricsManager(metrics, metricsRegistry);
    }

    @AfterEach
    public void tearDown() {
        if (metrics != null) {
            metrics.close();
            metrics = null;
        }

        metricsManager = null;
    }

    @Test
    public void testLatency() {
        metricsManager.recordLatency("", 123);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordLatency("", 456);

        assertEquals(289.5, metricValue(metricsRegistry.fetchLatencyAvg), EPSILON);
        assertEquals(456, metricValue(metricsRegistry.fetchLatencyMax), EPSILON);
    }

    @Test
    public void testNodeLatency() {
        String connectionId = "0";
        MetricName nodeLatencyAvg = metrics.metricName("request-latency-avg", "group");
        MetricName nodeLatencyMax = metrics.metricName("request-latency-max", "group");
        registerNodeLatencyMetric(connectionId, nodeLatencyAvg, nodeLatencyMax);

        metricsManager.recordLatency(connectionId, 123);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordLatency(connectionId, 456);

        assertEquals(289.5, metricValue(metricsRegistry.fetchLatencyAvg), EPSILON);
        assertEquals(456, metricValue(metricsRegistry.fetchLatencyMax), EPSILON);

        assertEquals(289.5, metricValue(nodeLatencyAvg), EPSILON);
        assertEquals(456, metricValue(nodeLatencyMax), EPSILON);

        // Record metric against another node.
        metricsManager.recordLatency("1", 501);

        assertEquals(360, metricValue(metricsRegistry.fetchLatencyAvg), EPSILON);
        assertEquals(501, metricValue(metricsRegistry.fetchLatencyMax), EPSILON);
        // Node specific metric should not be affected.
        assertEquals(289.5, metricValue(nodeLatencyAvg), EPSILON);
        assertEquals(456, metricValue(nodeLatencyMax), EPSILON);
    }

    @Test
    public void testBytesFetched() {
        metricsManager.recordBytesFetched(2);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordBytesFetched(10);

        assertEquals(6, metricValue(metricsRegistry.fetchSizeAvg), EPSILON);
        assertEquals(10, metricValue(metricsRegistry.fetchSizeMax), EPSILON);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testBytesFetchedTopic() {
        String topicName1 = TOPIC_NAME;
        String topicName2 = "another.topic";
        Map<String, String> tags1 = Map.of("topic", topicName1);
        Map<String, String> tags2 = Map.of("topic", topicName2);
        Map<String, String> deprecatedTags = topicTags(topicName2);
        int initialMetricsSize = metrics.metrics().size();

        metricsManager.recordBytesFetched(topicName1, 2);
        // 4 new metrics shall be registered.
        assertEquals(4, metrics.metrics().size() - initialMetricsSize);
        metricsManager.recordBytesFetched(topicName2, 1);
        // Another 8 metrics get registered as deprecated metrics should be reported for topicName2.
        assertEquals(12, metrics.metrics().size() - initialMetricsSize);

        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordBytesFetched(topicName1, 10);
        metricsManager.recordBytesFetched(topicName2, 5);

        // Subsequent calls should not register new metrics.
        assertEquals(12, metrics.metrics().size() - initialMetricsSize);
        // Validate metrics for topicName1.
        assertEquals(6, metricValue(metricsRegistry.topicFetchSizeAvg, tags1), EPSILON);
        assertEquals(10, metricValue(metricsRegistry.topicFetchSizeMax, tags1), EPSILON);
        assertTrue(metricValue(metricsRegistry.topicBytesConsumedRate, tags1) > 0);
        assertEquals(12, metricValue(metricsRegistry.topicBytesConsumedTotal, tags1), EPSILON);
        // Validate metrics for topicName2.
        assertEquals(3, metricValue(metricsRegistry.topicFetchSizeAvg, tags2), EPSILON);
        assertEquals(5, metricValue(metricsRegistry.topicFetchSizeMax, tags2), EPSILON);
        assertTrue(metricValue(metricsRegistry.topicBytesConsumedRate, tags2) > 0);
        assertEquals(6, metricValue(metricsRegistry.topicBytesConsumedTotal, tags2), EPSILON);
        // Validate metrics for deprecated topic.
        assertEquals(3, metricValue(metricsRegistry.topicFetchSizeAvg, deprecatedTags), EPSILON);
        assertEquals(5, metricValue(metricsRegistry.topicFetchSizeMax, deprecatedTags), EPSILON);
        assertTrue(metricValue(metricsRegistry.topicBytesConsumedRate, deprecatedTags) > 0);
        assertEquals(6, metricValue(metricsRegistry.topicBytesConsumedTotal, deprecatedTags), EPSILON);
    }

    @Test
    public void testRecordsFetched() {
        metricsManager.recordRecordsFetched(3);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordRecordsFetched(15);

        assertEquals(9, metricValue(metricsRegistry.recordsPerRequestAvg), EPSILON);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testRecordsFetchedTopic() {
        String topicName1 = TOPIC_NAME;
        String topicName2 = "another.topic";
        Map<String, String> tags1 = Map.of("topic", topicName1);
        Map<String, String> tags2 = Map.of("topic", topicName2);
        Map<String, String> deprecatedTags = topicTags(topicName2);
        int initialMetricsSize = metrics.metrics().size();

        metricsManager.recordRecordsFetched(topicName1, 2);
        // 3 new metrics shall be registered.
        assertEquals(3, metrics.metrics().size() - initialMetricsSize);
        metricsManager.recordRecordsFetched(topicName2, 1);
        // Another 6 metrics get registered as deprecated metrics should be reported for topicName2.
        assertEquals(9, metrics.metrics().size() - initialMetricsSize);

        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordRecordsFetched(topicName1, 10);
        metricsManager.recordRecordsFetched(topicName2, 5);

        // Subsequent calls should not register new metrics.
        assertEquals(9, metrics.metrics().size() - initialMetricsSize);
        // Validate metrics for topicName1.
        assertEquals(6, metricValue(metricsRegistry.topicRecordsPerRequestAvg, tags1), EPSILON);
        assertTrue(metricValue(metricsRegistry.topicRecordsConsumedRate, tags1) > 0);
        assertEquals(12, metricValue(metricsRegistry.topicRecordsConsumedTotal, tags1), EPSILON);
        // Validate metrics for topicName2.
        assertEquals(3, metricValue(metricsRegistry.topicRecordsPerRequestAvg, tags2), EPSILON);
        assertTrue(metricValue(metricsRegistry.topicRecordsConsumedRate, tags2) > 0);
        assertEquals(6, metricValue(metricsRegistry.topicRecordsConsumedTotal, tags2), EPSILON);
        // Validate metrics for deprecated topic.
        assertEquals(3, metricValue(metricsRegistry.topicRecordsPerRequestAvg, deprecatedTags), EPSILON);
        assertTrue(metricValue(metricsRegistry.topicRecordsConsumedRate, deprecatedTags) > 0);
        assertEquals(6, metricValue(metricsRegistry.topicRecordsConsumedTotal, deprecatedTags), EPSILON);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testPartitionLag() {
        TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition tp2 = new TopicPartition("another.topic", 0);

        Map<String, String> tags1 = Map.of("topic", tp1.topic(), "partition", String.valueOf(tp1.partition()));
        Map<String, String> tags2 = Map.of("topic", tp2.topic(), "partition", String.valueOf(tp2.partition()));
        Map<String, String> deprecatedTags = topicPartitionTags(tp2);
        int initialMetricsSize = metrics.metrics().size();

        metricsManager.recordPartitionLag(tp1, 14);
        // 3 new metrics shall be registered.
        assertEquals(3, metrics.metrics().size() - initialMetricsSize);

        metricsManager.recordPartitionLag(tp1, 8);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordPartitionLag(tp1, 5);

        // Subsequent calls should not register new metrics.
        assertEquals(3, metrics.metrics().size() - initialMetricsSize);
        // Validate metrics for tp1.
        assertEquals(14, metricValue(metricsRegistry.recordsLagMax), EPSILON);
        assertEquals(5, metricValue(metricsRegistry.partitionRecordsLag, tags1), EPSILON);
        assertEquals(14, metricValue(metricsRegistry.partitionRecordsLagMax, tags1), EPSILON);
        assertEquals(9, metricValue(metricsRegistry.partitionRecordsLagAvg, tags1), EPSILON);

        metricsManager.recordPartitionLag(tp2, 7);
        // Another 6 metrics get registered as deprecated metrics should be reported for tp2.
        assertEquals(9, metrics.metrics().size() - initialMetricsSize);
        metricsManager.recordPartitionLag(tp2, 3);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordPartitionLag(tp2, 2);

        // Subsequent calls should not register new metrics.
        assertEquals(9, metrics.metrics().size() - initialMetricsSize);
        // Validate metrics for tp2.
        assertEquals(7, metricValue(metricsRegistry.recordsLagMax), EPSILON);
        assertEquals(2, metricValue(metricsRegistry.partitionRecordsLag, tags2), EPSILON);
        assertEquals(7, metricValue(metricsRegistry.partitionRecordsLagMax, tags2), EPSILON);
        assertEquals(4, metricValue(metricsRegistry.partitionRecordsLagAvg, tags2), EPSILON);
        // Validate metrics for deprecated topic.
        assertEquals(2, metricValue(metricsRegistry.partitionRecordsLag, deprecatedTags), EPSILON);
        assertEquals(7, metricValue(metricsRegistry.partitionRecordsLagMax, deprecatedTags), EPSILON);
        assertEquals(4, metricValue(metricsRegistry.partitionRecordsLagAvg, deprecatedTags), EPSILON);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testPartitionLead() {
        TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition tp2 = new TopicPartition("another.topic", 0);

        Map<String, String> tags1 = Map.of("topic", tp1.topic(), "partition", String.valueOf(tp1.partition()));
        Map<String, String> tags2 = Map.of("topic", tp2.topic(), "partition", String.valueOf(tp2.partition()));
        Map<String, String> deprecatedTags = topicPartitionTags(tp2);
        int initialMetricsSize = metrics.metrics().size();

        metricsManager.recordPartitionLead(tp1, 15);
        // 3 new metrics shall be registered.
        assertEquals(3, metrics.metrics().size() - initialMetricsSize);

        metricsManager.recordPartitionLead(tp1, 11);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordPartitionLead(tp1, 13);

        // Subsequent calls should not register new metrics.
        assertEquals(3, metrics.metrics().size() - initialMetricsSize);
        // Validate metrics for tp1.
        assertEquals(11, metricValue(metricsRegistry.recordsLeadMin), EPSILON);
        assertEquals(13, metricValue(metricsRegistry.partitionRecordsLead, tags1), EPSILON);
        assertEquals(11, metricValue(metricsRegistry.partitionRecordsLeadMin, tags1), EPSILON);
        assertEquals(13, metricValue(metricsRegistry.partitionRecordsLeadAvg, tags1), EPSILON);

        metricsManager.recordPartitionLead(tp2, 18);
        // Another 6 metrics get registered as deprecated metrics should be reported for tp2.
        assertEquals(9, metrics.metrics().size() - initialMetricsSize);

        metricsManager.recordPartitionLead(tp2, 12);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordPartitionLead(tp2, 15);

        // Subsequent calls should not register new metrics.
        assertEquals(9, metrics.metrics().size() - initialMetricsSize);
        // Validate metrics for tp2.
        assertEquals(12, metricValue(metricsRegistry.recordsLeadMin), EPSILON);
        assertEquals(15, metricValue(metricsRegistry.partitionRecordsLead, tags2), EPSILON);
        assertEquals(12, metricValue(metricsRegistry.partitionRecordsLeadMin, tags2), EPSILON);
        assertEquals(15, metricValue(metricsRegistry.partitionRecordsLeadAvg, tags2), EPSILON);
        // Validate metrics for deprecated topic.
        assertEquals(15, metricValue(metricsRegistry.partitionRecordsLead, deprecatedTags), EPSILON);
        assertEquals(12, metricValue(metricsRegistry.partitionRecordsLeadMin, deprecatedTags), EPSILON);
        assertEquals(15, metricValue(metricsRegistry.partitionRecordsLeadAvg, deprecatedTags), EPSILON);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testMaybeUpdateAssignment() {
        TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition tp2 = new TopicPartition("another.topic", 0);
        TopicPartition tp3 = new TopicPartition("another.topic", 1);
        int initialMetricsSize = metrics.metrics().size();

        SubscriptionState subscriptionState = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        subscriptionState.assignFromUser(Set.of(tp1));

        metricsManager.maybeUpdateAssignment(subscriptionState);
        // 1 new metrics shall be registered.
        assertEquals(1, metrics.metrics().size() - initialMetricsSize);

        subscriptionState.assignFromUser(Set.of(tp1, tp2));
        subscriptionState.updatePreferredReadReplica(tp2, 1, () -> 0L);
        metricsManager.maybeUpdateAssignment(subscriptionState);
        // Another 2 metrics get registered as deprecated metrics should be reported for tp2.
        assertEquals(3, metrics.metrics().size() - initialMetricsSize);

        Map<String, String> tags1 = Map.of("topic", tp1.topic(), "partition", String.valueOf(tp1.partition()));
        Map<String, String> tags2 = Map.of("topic", tp2.topic(), "partition", String.valueOf(tp2.partition()));
        Map<String, String> deprecatedTags = topicPartitionTags(tp2);
        // Validate preferred read replica metrics.
        assertEquals(-1, readReplicaMetricValue(metricsRegistry.partitionPreferredReadReplica, tags1), EPSILON);
        assertEquals(1, readReplicaMetricValue(metricsRegistry.partitionPreferredReadReplica, tags2), EPSILON);
        assertEquals(1, readReplicaMetricValue(metricsRegistry.partitionPreferredReadReplica, deprecatedTags), EPSILON);

        // Remove tp2 from subscription set.
        subscriptionState.assignFromUser(Set.of(tp1, tp3));
        metricsManager.maybeUpdateAssignment(subscriptionState);
        // Metrics count shall remain same as tp2 should be removed and tp3 gets added.
        assertEquals(3, metrics.metrics().size() - initialMetricsSize);

        // Remove all partitions.
        subscriptionState.assignFromUser(Set.of());
        metricsManager.maybeUpdateAssignment(subscriptionState);
        // Metrics count shall be same as initial count as all new metrics shall be removed.
        assertEquals(initialMetricsSize, metrics.metrics().size());
    }

    @Test
    public void testMaybeUpdateAssignmentWithAdditionalRegisteredMetrics() {
        TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition tp2 = new TopicPartition("another.topic", 0);
        TopicPartition tp3 = new TopicPartition("another.topic", 1);

        int initialMetricsSize = metrics.metrics().size();

        metricsManager.recordPartitionLag(tp1, 14);
        metricsManager.recordPartitionLead(tp1, 11);
        metricsManager.recordPartitionLag(tp2, 5);
        metricsManager.recordPartitionLead(tp2, 1);
        metricsManager.recordPartitionLag(tp3, 4);
        metricsManager.recordPartitionLead(tp3, 2);

        int additionalRegisteredMetricsSize = metrics.metrics().size();

        SubscriptionState subscriptionState = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        subscriptionState.assignFromUser(Set.of(tp1, tp2, tp3));
        metricsManager.maybeUpdateAssignment(subscriptionState);

        // 5 new metrics shall be registered.
        assertEquals(5, metrics.metrics().size() - additionalRegisteredMetricsSize);

        // Remove 1 partition which has deprecated metrics as well.
        subscriptionState.assignFromUser(Set.of(tp1, tp2));
        metricsManager.maybeUpdateAssignment(subscriptionState);
        // For tp2, 14 metrics will be unregistered. 3 for partition lag, 3 for partition lead, 1 for
        // preferred read replica and similarly 7 deprecated metrics. Hence, we should have 9 metrics
        // removed from additionalRegisteredMetricsSize.
        assertEquals(9, additionalRegisteredMetricsSize - metrics.metrics().size());

        // Remove all partitions.
        subscriptionState.assignFromUser(Set.of());
        metricsManager.maybeUpdateAssignment(subscriptionState);
        // Metrics count shall be same as initial count as all new metrics shall be removed.
        assertEquals(initialMetricsSize, metrics.metrics().size());
    }

    private void registerNodeLatencyMetric(String connectionId, MetricName nodeLatencyAvg, MetricName nodeLatencyMax) {
        String nodeTimeName = "node-" + connectionId + ".latency";
        Sensor nodeRequestTime = metrics.sensor(nodeTimeName);
        nodeRequestTime.add(nodeLatencyAvg, new Avg());
        nodeRequestTime.add(nodeLatencyMax, new Max());
    }

    private double metricValue(MetricNameTemplate name) {
        MetricName metricName = metrics.metricInstance(name);
        return metricValue(metricName);
    }

    private double metricValue(MetricNameTemplate name, Map<String, String> tags) {
        MetricName metricName = metrics.metricInstance(name, tags);
        return metricValue(metricName);
    }

    private double metricValue(MetricName metricName) {
        KafkaMetric metric = metrics.metric(metricName);
        return (Double) metric.metricValue();
    }

    private Integer readReplicaMetricValue(MetricNameTemplate name, Map<String, String> tags) {
        MetricName metricName = metrics.metricInstance(name, tags);
        KafkaMetric metric = metrics.metric(metricName);
        return (Integer) metric.metricValue();
    }
}
