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

package org.apache.kafka.controller;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.Set;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuorumControllerMetricsTest {
    private static final String EXPECTED_GROUP = "kafka.controller";
    @Test
    public void testKafkaControllerMetricNames() {
        String expectedType = "KafkaController";
        Set<String> expectedMetricNames = Utils.mkSet(
            "ActiveControllerCount",
            "FencedBrokerCount",
            "ActiveBrokerCount",
            "GlobalTopicCount",
            "GlobalPartitionCount",
            "OfflinePartitionsCount",
            "PreferredReplicaImbalanceCount",
            "MetadataErrorCount",
            "LastAppliedRecordLagMs",
            "LastAppliedRecordOffset",
            "LastAppliedRecordTimestamp",
            "LastCommittedRecordOffset"
        );
        assertMetricsCreatedAndRemovedUponClose(expectedType, expectedMetricNames);
    }

    @Test
    public void testControllerEventManagerMetricNames() {
        String expectedType = "ControllerEventManager";
        Set<String> expectedMetricNames = Utils.mkSet(
            "EventQueueTimeMs",
            "EventQueueProcessingTimeMs");
        assertMetricsCreatedAndRemovedUponClose(expectedType, expectedMetricNames);
    }

    @Test
    public void testUpdateEventQueueTime() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try {
            try (QuorumControllerMetrics quorumControllerMetrics = new QuorumControllerMetrics(registry, time)) {
                quorumControllerMetrics.updateEventQueueTime(1000);
                assertMetricHistogram(registry, metricName("ControllerEventManager", "EventQueueTimeMs"), 1, 1000);
            }
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void testUpdateEventQueueProcessingTime() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try {
            try (QuorumControllerMetrics quorumControllerMetrics = new QuorumControllerMetrics(registry, time)) {
                quorumControllerMetrics.updateEventQueueProcessingTime(1000);
                assertMetricHistogram(registry, metricName("ControllerEventManager", "EventQueueProcessingTimeMs"), 1, 1000);
            }
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void testLastAppliedRecordMetrics() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        time.sleep(1000);
        try {
            try (QuorumControllerMetrics quorumControllerMetrics = new QuorumControllerMetrics(registry, time)) {
                quorumControllerMetrics.setLastAppliedRecordOffset(100);
                quorumControllerMetrics.setLastAppliedRecordTimestamp(500);
                quorumControllerMetrics.setLastCommittedRecordOffset(50);

                @SuppressWarnings("unchecked")
                Gauge<Long> lastAppliedRecordOffset = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("KafkaController", "LastAppliedRecordOffset"));
                assertEquals(100, lastAppliedRecordOffset.value());

                @SuppressWarnings("unchecked")
                Gauge<Long> lastAppliedRecordTimestamp = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("KafkaController", "LastAppliedRecordTimestamp"));
                assertEquals(500, lastAppliedRecordTimestamp.value());

                @SuppressWarnings("unchecked")
                Gauge<Long> lastAppliedRecordLagMs = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("KafkaController", "LastAppliedRecordLagMs"));
                assertEquals(time.milliseconds() - 500, lastAppliedRecordLagMs.value());

                @SuppressWarnings("unchecked")
                Gauge<Long> lastCommittedRecordOffset = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("KafkaController", "LastCommittedRecordOffset"));
                assertEquals(50, lastCommittedRecordOffset.value());
            }
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void testMetadataErrorCount() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try {
            try (QuorumControllerMetrics quorumControllerMetrics = new QuorumControllerMetrics(registry, time)) {
                @SuppressWarnings("unchecked")
                Gauge<Integer> metadataErrorCount = (Gauge<Integer>) registry
                        .allMetrics()
                        .get(metricName("KafkaController", "MetadataErrorCount"));
                assertEquals(0, metadataErrorCount.value());
                quorumControllerMetrics.incrementMetadataErrorCount();
                assertEquals(1, metadataErrorCount.value());
            }
        } finally {
            registry.shutdown();
        }
    }

    private static void assertMetricsCreatedAndRemovedUponClose(String expectedType, Set<String> expectedMetricNames) {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try {
            try (QuorumControllerMetrics quorumControllerMetrics = new QuorumControllerMetrics(registry, time)) {
                assertMetricsCreated(registry, expectedMetricNames, expectedType);
            }
            assertMetricsRemoved(registry, expectedMetricNames, expectedType);
        } finally {
            registry.shutdown();
        }
    }

    private static void assertMetricHistogram(MetricsRegistry registry, MetricName metricName, long count, double sum) {
        Histogram histogram = (Histogram) registry.allMetrics().get(metricName);

        assertEquals(count, histogram.count());
        assertEquals(sum, histogram.sum(), .1);
    }

    private static MetricName metricName(String type, String name) {
        String mBeanName = String.format("kafka.controller:type=%s,name=%s", type, name);
        return new MetricName("kafka.controller", type, name, null, mBeanName);
    }

    private static void assertMetricsCreated(MetricsRegistry registry, Set<String> expectedMetricNames, String expectedType) {
        assertEquals(registry.allMetrics().keySet().stream()
                .filter(k -> k.getType() == expectedType).count(),
                expectedMetricNames.size());
        expectedMetricNames.forEach(expectedName -> {
            MetricName expectMetricName = metricName(expectedType, expectedName);
            assertTrue(registry.allMetrics().containsKey(expectMetricName), "Missing metric: " + expectMetricName);
        });
        registry.allMetrics().forEach((actualMetricName, actualMetric) -> {
            if (actualMetricName.getType() == expectedType) {
                assertTrue(expectedMetricNames.contains(actualMetricName.getName()), "Unexpected metric: " + actualMetricName);
            }
        });
    }

    private static void assertMetricsRemoved(MetricsRegistry registry, Set<String> expectedMetricNames, String expectedType) {
        expectedMetricNames.forEach(expectedName -> {
            MetricName expectMetricName = metricName(expectedType, expectedName);
            assertFalse(registry.allMetrics().containsKey(expectMetricName), "Found metric: " + expectMetricName);
        });
    }
}
