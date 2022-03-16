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

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.Set;
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
            "GlobalTopicCount",
            "GlobalPartitionCount",
            "OfflinePartitionsCount",
            "PreferredReplicaImbalanceCount");
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
        try {
            try (QuorumControllerMetrics quorumControllerMetrics = new QuorumControllerMetrics(registry)) {
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
        try {
            try (QuorumControllerMetrics quorumControllerMetrics = new QuorumControllerMetrics(registry)) {
                quorumControllerMetrics.updateEventQueueProcessingTime(1000);
                assertMetricHistogram(registry, metricName("ControllerEventManager", "EventQueueProcessingTimeMs"), 1, 1000);
            }
        } finally {
            registry.shutdown();
        }
    }

    private static void assertMetricsCreatedAndRemovedUponClose(String expectedType, Set<String> expectedMetricNames) {
        MetricsRegistry registry = new MetricsRegistry();
        try {
            try (QuorumControllerMetrics quorumControllerMetrics = new QuorumControllerMetrics(registry)) {
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
        expectedMetricNames.forEach(expectedName -> {
            MetricName expectMetricName = metricName(expectedType, expectedName);
            assertTrue(registry.allMetrics().containsKey(expectMetricName), "Missing metric: " + expectMetricName);
        });
    }

    private static void assertMetricsRemoved(MetricsRegistry registry, Set<String> expectedMetricNames, String expectedType) {
        expectedMetricNames.forEach(expectedName -> {
            MetricName expectMetricName = metricName(expectedType, expectedName);
            assertFalse(registry.allMetrics().containsKey(expectMetricName), "Found metric: " + expectMetricName);
        });
    }
}
