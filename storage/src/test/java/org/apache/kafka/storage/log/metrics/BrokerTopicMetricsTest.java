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
package org.apache.kafka.storage.log.metrics;

import org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BrokerTopicMetricsTest {
    private static final String TOPIC = "topic";
    private static final String TOPIC2 = "topic2";

    private final BrokerTopicStats brokerTopicStats = new BrokerTopicStats(true);
    private final BrokerTopicMetrics brokerTopicMetrics = brokerTopicStats.topicStats(TOPIC);
    private final BrokerTopicMetrics allTopicMetrics = brokerTopicStats.allTopicsStats();

    @AfterEach
    public void tearDown() {
        brokerTopicStats.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTopicStats(boolean systemRemoteStorageEnabled) {
        try (BrokerTopicStats stats = new BrokerTopicStats(systemRemoteStorageEnabled)) {
            BrokerTopicMetrics metrics = stats.topicStats(TOPIC);
            Set<String> gaugeMetrics = Set.of(
                RemoteStorageMetrics.REMOTE_LOG_SIZE_COMPUTATION_TIME_METRIC.getName(),
                RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName(),
                RemoteStorageMetrics.REMOTE_COPY_LAG_SEGMENTS_METRIC.getName(),
                RemoteStorageMetrics.REMOTE_DELETE_LAG_BYTES_METRIC.getName(),
                RemoteStorageMetrics.REMOTE_DELETE_LAG_SEGMENTS_METRIC.getName(),
                RemoteStorageMetrics.REMOTE_LOG_METADATA_COUNT_METRIC.getName(),
                RemoteStorageMetrics.REMOTE_LOG_SIZE_BYTES_METRIC.getName());

            RemoteStorageMetrics.brokerTopicStatsMetrics().forEach(metric -> {
                if (systemRemoteStorageEnabled) {
                    if (!gaugeMetrics.contains(metric.getName())) {
                        assertTrue(metrics.metricMapKeySet().contains(metric.getName()), "the metric is missing: " + metric.getName());
                    } else {
                        assertFalse(metrics.metricMapKeySet().contains(metric.getName()), "the metric should not appear: " + metric.getName());
                    }
                } else {
                    assertFalse(metrics.metricMapKeySet().contains(metric.getName()));
                }
            });
            gaugeMetrics.forEach(metricName -> {
                if (systemRemoteStorageEnabled) {
                    assertTrue(metrics.metricGaugeMap().containsKey(metricName), "The metric is missing:" + metricName);
                } else {
                    assertFalse(metrics.metricGaugeMap().containsKey(metricName), "The metric should not appear:" + metricName);
                }
            });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSingularCopyLagBytesMetric(boolean systemRemoteStorageEnabled) {
        try (BrokerTopicStats stats = new BrokerTopicStats(systemRemoteStorageEnabled)) {
            BrokerTopicMetrics metrics = stats.topicStats(TOPIC);

            if (systemRemoteStorageEnabled) {
                stats.recordRemoteCopyLagBytes(TOPIC, 0, 100);
                stats.recordRemoteCopyLagBytes(TOPIC, 1, 150);
                stats.recordRemoteCopyLagBytes(TOPIC, 2, 250);
                assertEquals(500, metrics.remoteCopyLagBytes());
                assertEquals(500, stats.allTopicsStats().remoteCopyLagBytes());
                stats.recordRemoteCopyLagBytes(TOPIC2, 0, 100);
                assertEquals(600, stats.allTopicsStats().remoteCopyLagBytes());
            } else {
                assertNull(metrics.metricGaugeMap().get(RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName()));
                assertNull(stats.allTopicsStats().metricGaugeMap().get(RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName()));
            }
        }
    }

    @Test
    public void testMultipleCopyLagBytesMetrics() {
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 1, 2);
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 2, 3);

        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 0, 4);
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 1, 5);
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 2, 6);

        assertEquals(15, brokerTopicMetrics.remoteCopyLagBytes());
        assertEquals(15, allTopicMetrics.remoteCopyLagBytes());
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC2, 2, 5);
        assertEquals(20, allTopicMetrics.remoteCopyLagBytes());
    }

    @Test
    public void testCopyLagBytesMetricWithPartitionExpansion() {
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteCopyLagBytes());
        assertEquals(3, allTopicMetrics.remoteCopyLagBytes());

        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 2, 3);

        assertEquals(6, brokerTopicMetrics.remoteCopyLagBytes());
        assertEquals(6, allTopicMetrics.remoteCopyLagBytes());
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC2, 0, 1);
        assertEquals(7, allTopicMetrics.remoteCopyLagBytes());
    }

    @Test
    public void testCopyLagBytesMetricWithPartitionShrinking() {
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteCopyLagBytes());
        assertEquals(3, allTopicMetrics.remoteCopyLagBytes());

        brokerTopicStats.removeRemoteCopyLagBytes(TOPIC, 1);

        assertEquals(1, brokerTopicMetrics.remoteCopyLagBytes());
        assertEquals(1, allTopicMetrics.remoteCopyLagBytes());

        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC2, 0, 1);
        assertEquals(2, allTopicMetrics.remoteCopyLagBytes());
    }

    @Test
    public void testCopyLagBytesMetricWithRemovingNonexistentPartitions() {
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteCopyLagBytes());
        assertEquals(3, allTopicMetrics.remoteCopyLagBytes());

        brokerTopicStats.removeRemoteCopyLagBytes(TOPIC, 3);

        assertEquals(3, brokerTopicMetrics.remoteCopyLagBytes());
        assertEquals(3, allTopicMetrics.remoteCopyLagBytes());
    }

    @Test
    public void testCopyLagBytesMetricClear() {
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteCopyLagBytes());
        assertEquals(3, allTopicMetrics.remoteCopyLagBytes());

        brokerTopicStats.close();

        assertEquals(0, brokerTopicMetrics.remoteCopyLagBytes());
        assertEquals(0, allTopicMetrics.remoteCopyLagBytes());

        brokerTopicStats.recordRemoteCopyLagBytes(TOPIC2, 0, 1);
        assertEquals(1, allTopicMetrics.remoteCopyLagBytes());
    }

    @Test
    public void testMultipleCopyLagSegmentsMetrics() {
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 1, 2);
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 2, 3);

        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 0, 4);
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 1, 5);
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 2, 6);

        assertEquals(15, brokerTopicMetrics.remoteCopyLagSegments());
        assertEquals(15, allTopicMetrics.remoteCopyLagSegments());

        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC2, 0, 1);
        assertEquals(16, allTopicMetrics.remoteCopyLagSegments());
    }

    @Test
    public void testCopyLagSegmentsMetricWithPartitionExpansion() {
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteCopyLagSegments());
        assertEquals(3, allTopicMetrics.remoteCopyLagSegments());

        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 2, 3);

        assertEquals(6, brokerTopicMetrics.remoteCopyLagSegments());
        assertEquals(6, allTopicMetrics.remoteCopyLagSegments());
    }

    @Test
    public void testCopyLagSegmentsMetricWithPartitionShrinking() {
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteCopyLagSegments());
        assertEquals(3, allTopicMetrics.remoteCopyLagSegments());

        brokerTopicStats.removeRemoteCopyLagSegments(TOPIC, 1);

        assertEquals(1, brokerTopicMetrics.remoteCopyLagSegments());
        assertEquals(1, allTopicMetrics.remoteCopyLagSegments());
    }

    @Test
    public void testCopyLagSegmentsMetricWithRemovingNonexistentPartitions() {
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteCopyLagSegments());
        assertEquals(3, allTopicMetrics.remoteCopyLagSegments());

        brokerTopicStats.removeRemoteCopyLagSegments(TOPIC, 3);

        assertEquals(3, brokerTopicMetrics.remoteCopyLagSegments());
        assertEquals(3, allTopicMetrics.remoteCopyLagSegments());
    }

    @Test
    public void testCopyLagSegmentsMetricClear() {
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteCopyLagSegments(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteCopyLagSegments());
        assertEquals(3, allTopicMetrics.remoteCopyLagSegments());

        brokerTopicStats.close();

        assertEquals(0, brokerTopicMetrics.remoteCopyLagSegments());
        assertEquals(0, allTopicMetrics.remoteCopyLagSegments());
    }

    @Test
    public void testMultipleDeleteLagBytesMetrics() {
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 1, 2);
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 2, 3);

        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 0, 4);
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 1, 5);
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 2, 6);

        assertEquals(15, brokerTopicMetrics.remoteDeleteLagBytes());
        assertEquals(15, allTopicMetrics.remoteDeleteLagBytes());

        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC2, 0, 1);
        assertEquals(16, allTopicMetrics.remoteDeleteLagBytes());
    }

    @Test
    public void testDeleteLagBytesMetricWithPartitionExpansion() {
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteDeleteLagBytes());
        assertEquals(3, allTopicMetrics.remoteDeleteLagBytes());

        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 2, 3);

        assertEquals(6, brokerTopicMetrics.remoteDeleteLagBytes());
        assertEquals(6, allTopicMetrics.remoteDeleteLagBytes());
    }

    @Test
    public void testDeleteLagBytesMetricWithPartitionShrinking() {
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteDeleteLagBytes());
        assertEquals(3, allTopicMetrics.remoteDeleteLagBytes());

        brokerTopicStats.removeRemoteDeleteLagBytes(TOPIC, 1);

        assertEquals(1, brokerTopicMetrics.remoteDeleteLagBytes());
        assertEquals(1, allTopicMetrics.remoteDeleteLagBytes());
    }

    @Test
    public void testDeleteLagBytesMetricWithRemovingNonexistentPartitions() {
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteDeleteLagBytes());
        assertEquals(3, allTopicMetrics.remoteDeleteLagBytes());

        brokerTopicStats.removeRemoteDeleteLagBytes(TOPIC, 3);

        assertEquals(3, brokerTopicMetrics.remoteDeleteLagBytes());
        assertEquals(3, allTopicMetrics.remoteDeleteLagBytes());
    }

    @Test
    public void testDeleteLagBytesMetricClear() {
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteDeleteLagBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteDeleteLagBytes());
        assertEquals(3, allTopicMetrics.remoteDeleteLagBytes());

        brokerTopicStats.close();

        assertEquals(0, brokerTopicMetrics.remoteDeleteLagBytes());
        assertEquals(0, allTopicMetrics.remoteDeleteLagBytes());
    }

    @Test
    public void testMultipleDeleteLagSegmentsMetrics() {
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 1, 2);
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 2, 3);

        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 0, 4);
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 1, 5);
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 2, 6);

        assertEquals(15, brokerTopicMetrics.remoteDeleteLagSegments());
        assertEquals(15, allTopicMetrics.remoteDeleteLagSegments());

        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC2, 1, 5);
        assertEquals(20, allTopicMetrics.remoteDeleteLagSegments());
    }

    @Test
    public void testDeleteLagSegmentsMetricWithPartitionExpansion() {
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteDeleteLagSegments());
        assertEquals(3, allTopicMetrics.remoteDeleteLagSegments());

        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 2, 3);

        assertEquals(6, brokerTopicMetrics.remoteDeleteLagSegments());
        assertEquals(6, allTopicMetrics.remoteDeleteLagSegments());
    }

    @Test
    public void testDeleteLagSegmentsMetricWithPartitionShrinking() {
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteDeleteLagSegments());
        assertEquals(3, allTopicMetrics.remoteDeleteLagSegments());

        brokerTopicStats.removeRemoteDeleteLagSegments(TOPIC, 1);

        assertEquals(1, brokerTopicMetrics.remoteDeleteLagSegments());
        assertEquals(1, allTopicMetrics.remoteDeleteLagSegments());
    }

    @Test
    public void testDeleteLagSegmentsMetricWithRemovingNonexistentPartitions() {
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteDeleteLagSegments());
        assertEquals(3, allTopicMetrics.remoteDeleteLagSegments());

        brokerTopicStats.removeRemoteDeleteLagSegments(TOPIC, 3);

        assertEquals(3, brokerTopicMetrics.remoteDeleteLagSegments());
        assertEquals(3, allTopicMetrics.remoteDeleteLagSegments());
    }

    @Test
    public void testDeleteLagSegmentsMetricClear() {
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteDeleteLagSegments(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteDeleteLagSegments());
        assertEquals(3, allTopicMetrics.remoteDeleteLagSegments());

        brokerTopicStats.close();

        assertEquals(0, brokerTopicMetrics.remoteDeleteLagSegments());
        assertEquals(0, allTopicMetrics.remoteDeleteLagSegments());
    }

    @Test
    public void testRemoteLogMetadataCount() {
        assertEquals(0, brokerTopicMetrics.remoteLogMetadataCount());
        assertEquals(0, allTopicMetrics.remoteLogMetadataCount());
        brokerTopicStats.recordRemoteLogMetadataCount(TOPIC, 0, 1);
        assertEquals(1, brokerTopicMetrics.remoteLogMetadataCount());
        assertEquals(1, allTopicMetrics.remoteLogMetadataCount());

        brokerTopicStats.recordRemoteLogMetadataCount(TOPIC, 1, 2);
        brokerTopicStats.recordRemoteLogMetadataCount(TOPIC, 2, 3);
        assertEquals(6, brokerTopicMetrics.remoteLogMetadataCount());
        assertEquals(6, allTopicMetrics.remoteLogMetadataCount());

        brokerTopicStats.close();

        assertEquals(0, brokerTopicMetrics.remoteLogMetadataCount());
        assertEquals(0, allTopicMetrics.remoteLogMetadataCount());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSingularLogSizeBytesMetric(boolean systemRemoteStorageEnabled) {
        try (BrokerTopicStats stats = new BrokerTopicStats(systemRemoteStorageEnabled)) {
            BrokerTopicMetrics metrics = stats.topicStats(TOPIC);
            if (systemRemoteStorageEnabled) {
                stats.recordRemoteLogSizeBytes(TOPIC, 0, 100);
                stats.recordRemoteLogSizeBytes(TOPIC, 1, 150);
                stats.recordRemoteLogSizeBytes(TOPIC, 2, 250);
                assertEquals(500, metrics.remoteLogSizeBytes());
                assertEquals(500, stats.allTopicsStats().remoteLogSizeBytes());

                stats.recordRemoteLogSizeBytes(TOPIC2, 0, 100);
                assertEquals(600, stats.allTopicsStats().remoteLogSizeBytes());
            } else {
                assertNull(metrics.metricGaugeMap().get(RemoteStorageMetrics.REMOTE_LOG_SIZE_BYTES_METRIC.getName()));
            }
        }
    }

    @Test
    public void testMultipleLogSizeBytesMetrics() {
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 1, 2);
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 2, 3);

        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 0, 4);
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 1, 5);
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 2, 6);

        assertEquals(15, brokerTopicMetrics.remoteLogSizeBytes());
        assertEquals(15, allTopicMetrics.remoteLogSizeBytes());

        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC2, 2, 5);
        assertEquals(20, allTopicMetrics.remoteLogSizeBytes());
    }

    @Test
    public void testLogSizeBytesMetricWithPartitionExpansion() {
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteLogSizeBytes());
        assertEquals(3, allTopicMetrics.remoteLogSizeBytes());

        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 2, 3);

        assertEquals(6, brokerTopicMetrics.remoteLogSizeBytes());
        assertEquals(6, allTopicMetrics.remoteLogSizeBytes());
    }

    @Test
    public void testLogSizeBytesMetricWithPartitionShrinking() {
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteLogSizeBytes());
        assertEquals(3, allTopicMetrics.remoteLogSizeBytes());

        brokerTopicStats.removeRemoteLogSizeBytes(TOPIC, 1);

        assertEquals(1, brokerTopicMetrics.remoteLogSizeBytes());
        assertEquals(1, allTopicMetrics.remoteLogSizeBytes());
    }

    @Test
    public void testLogSizeBytesMetricWithRemovingNonexistentPartitions() {
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteLogSizeBytes());
        assertEquals(3, allTopicMetrics.remoteLogSizeBytes());

        brokerTopicStats.removeRemoteLogSizeBytes(TOPIC, 3);

        assertEquals(3, brokerTopicMetrics.remoteLogSizeBytes());
        assertEquals(3, allTopicMetrics.remoteLogSizeBytes());
    }

    @Test
    public void testLogSizeBytesMetricClear() {
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 0, 1);
        brokerTopicStats.recordRemoteLogSizeBytes(TOPIC, 1, 2);

        assertEquals(3, brokerTopicMetrics.remoteLogSizeBytes());
        assertEquals(3, allTopicMetrics.remoteLogSizeBytes());

        brokerTopicStats.close();

        assertEquals(0, brokerTopicMetrics.remoteLogSizeBytes());
        assertEquals(0, allTopicMetrics.remoteLogSizeBytes());
    }

    @Test
    public void testGaugeClose() {
        String topic = "close-test-topic";
        try (BrokerTopicStats stats = new BrokerTopicStats(true)) {
            BrokerTopicMetrics metrics = stats.topicStats(topic);
            assertEquals(7, gaugeCount(topic));

            metrics.close();
            assertEquals(0, gaugeCount(topic));

            stats.recordRemoteCopyLagBytes(topic, 0, 1);
            stats.recordRemoteCopyLagSegments(topic, 0, 1);
            stats.recordRemoteDeleteLagBytes(topic, 0, 1);
            stats.recordRemoteDeleteLagSegments(topic, 0, 1);
            stats.recordRemoteLogMetadataCount(topic, 0, 1);
            stats.recordRemoteLogSizeComputationTime(topic, 0, 1);
            stats.recordRemoteLogSizeBytes(topic, 0, 1);
            assertEquals(7, gaugeCount(topic));
        }
    }

    private long gaugeCount(String topic) {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
            .filter(metricName -> metricName.getMBeanName().contains("topic=" + topic))
            .count();
    }
}
