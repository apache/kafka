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

package org.apache.kafka.server.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NodeMetricsTest {
    @Test
    public void testMetricsExported() {
        Metrics metrics = new Metrics();
        String expectedGroup = "node-metrics";

        // Metric description is not use for metric name equality
        Set<MetricName> expectedMetrics = Set.of(
            new MetricName("maximum-supported-level", expectedGroup, "", Map.of("feature-name", "metadata-version")),
            new MetricName("minimum-supported-level", expectedGroup, "", Map.of("feature-name", "metadata-version")),
            new MetricName("maximum-supported-level", expectedGroup, "", Map.of("feature-name", "kraft-version")),
            new MetricName("minimum-supported-level", expectedGroup, "", Map.of("feature-name", "kraft-version")),
            new MetricName("maximum-supported-level", expectedGroup, "", Map.of("feature-name", "transaction-version")),
            new MetricName("minimum-supported-level", expectedGroup, "", Map.of("feature-name", "transaction-version")),
            new MetricName("maximum-supported-level", expectedGroup, "", Map.of("feature-name", "group-version")),
            new MetricName("minimum-supported-level", expectedGroup, "", Map.of("feature-name", "group-version")),
            new MetricName("maximum-supported-level", expectedGroup, "", Map.of("feature-name", "eligible-leader-replicas-version")),
            new MetricName("minimum-supported-level", expectedGroup, "", Map.of("feature-name", "eligible-leader-replicas-version")),
            new MetricName("maximum-supported-level", expectedGroup, "", Map.of("feature-name", "share-version")),
            new MetricName("minimum-supported-level", expectedGroup, "", Map.of("feature-name", "share-version")),
            new MetricName("maximum-supported-level", expectedGroup, "", Map.of("feature-name", "streams-version")),
            new MetricName("minimum-supported-level", expectedGroup, "", Map.of("feature-name", "streams-version"))

        );

        try (NodeMetrics ignored = new NodeMetrics(metrics, true)) {
            Map<MetricName, KafkaMetric> metricsMap = metrics.metrics().entrySet().stream()
                .filter(entry -> Objects.equals(entry.getKey().group(), expectedGroup))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            assertEquals(expectedMetrics.size(), metricsMap.size());
            metricsMap.forEach((name, metric) -> assertTrue(expectedMetrics.contains(name)));
        }

        Map<MetricName, KafkaMetric> metricsMap = metrics.metrics().entrySet().stream()
            .filter(entry -> Objects.equals(entry.getKey().group(), expectedGroup))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertEquals(0, metricsMap.size());
    }
}
