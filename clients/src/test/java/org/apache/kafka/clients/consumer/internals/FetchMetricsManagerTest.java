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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.kafka.clients.consumer.internals.FetchMetricsManager.topicPartitionTags;
import static org.apache.kafka.clients.consumer.internals.FetchMetricsManager.topicTags;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FetchMetricsManagerTest {

    private static final double EPSILON = 0.0001;

    private final Time time = new MockTime(1, 0, 0);
    private final static String TOPIC_NAME = "test";
    private final static TopicPartition TP = new TopicPartition(TOPIC_NAME, 0);

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
        metricsManager.recordLatency(123);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordLatency(456);

        assertEquals(289.5, metricValue(metricsRegistry.fetchLatencyAvg), EPSILON);
        assertEquals(456, metricValue(metricsRegistry.fetchLatencyMax), EPSILON);
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
    public void testBytesFetchedTopic() {
        String topicName1 = TOPIC_NAME;
        String topicName2 = "another-topic";
        Map<String, String> tags1 = topicTags(topicName1);
        Map<String, String> tags2 = topicTags(topicName2);

        metricsManager.recordBytesFetched(topicName1, 2);
        metricsManager.recordBytesFetched(topicName2, 1);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordBytesFetched(topicName1, 10);
        metricsManager.recordBytesFetched(topicName2, 5);

        assertEquals(6, metricValue(metricsRegistry.topicFetchSizeAvg, tags1), EPSILON);
        assertEquals(10, metricValue(metricsRegistry.topicFetchSizeMax, tags1), EPSILON);
        assertEquals(3, metricValue(metricsRegistry.topicFetchSizeAvg, tags2), EPSILON);
        assertEquals(5, metricValue(metricsRegistry.topicFetchSizeMax, tags2), EPSILON);
    }

    @Test
    public void testRecordsFetched() {
        metricsManager.recordRecordsFetched(3);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordRecordsFetched(15);

        assertEquals(9, metricValue(metricsRegistry.recordsPerRequestAvg), EPSILON);
    }

    @Test
    public void testRecordsFetchedTopic() {
        String topicName1 = TOPIC_NAME;
        String topicName2 = "another-topic";
        Map<String, String> tags1 = topicTags(topicName1);
        Map<String, String> tags2 = topicTags(topicName2);

        metricsManager.recordRecordsFetched(topicName1, 2);
        metricsManager.recordRecordsFetched(topicName2, 1);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordRecordsFetched(topicName1, 10);
        metricsManager.recordRecordsFetched(topicName2, 5);

        assertEquals(6, metricValue(metricsRegistry.topicRecordsPerRequestAvg, tags1), EPSILON);
        assertEquals(3, metricValue(metricsRegistry.topicRecordsPerRequestAvg, tags2), EPSILON);
    }

    @Test
    public void testPartitionLag() {
        Map<String, String> tags = topicPartitionTags(TP);
        metricsManager.recordPartitionLag(TP, 14);
        metricsManager.recordPartitionLag(TP, 8);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordPartitionLag(TP, 5);

        assertEquals(14, metricValue(metricsRegistry.recordsLagMax), EPSILON);
        assertEquals(5, metricValue(metricsRegistry.partitionRecordsLag, tags), EPSILON);
        assertEquals(14, metricValue(metricsRegistry.partitionRecordsLagMax, tags), EPSILON);
        assertEquals(9, metricValue(metricsRegistry.partitionRecordsLagAvg, tags), EPSILON);
    }

    @Test
    public void testPartitionLead() {
        Map<String, String> tags = topicPartitionTags(TP);
        metricsManager.recordPartitionLead(TP, 15);
        metricsManager.recordPartitionLead(TP, 11);
        time.sleep(metrics.config().timeWindowMs() + 1);
        metricsManager.recordPartitionLead(TP, 13);

        assertEquals(11, metricValue(metricsRegistry.recordsLeadMin), EPSILON);
        assertEquals(13, metricValue(metricsRegistry.partitionRecordsLead, tags), EPSILON);
        assertEquals(11, metricValue(metricsRegistry.partitionRecordsLeadMin, tags), EPSILON);
        assertEquals(13, metricValue(metricsRegistry.partitionRecordsLeadAvg, tags), EPSILON);
    }

    private double metricValue(MetricNameTemplate name) {
        MetricName metricName = metrics.metricInstance(name);
        KafkaMetric metric = metrics.metric(metricName);
        return (Double) metric.metricValue();
    }

    private double metricValue(MetricNameTemplate name, Map<String, String> tags) {
        MetricName metricName = metrics.metricInstance(name, tags);
        KafkaMetric metric = metrics.metric(metricName);
        return (Double) metric.metricValue();
    }

}
