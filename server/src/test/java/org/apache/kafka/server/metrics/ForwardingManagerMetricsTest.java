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
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ForwardingManagerMetricsTest {
    @Test
    void testMetricsNames() {
        String expectedGroup = "ForwardingManager";
        Set<MetricName> expectedMetrics = Set.of(
            new MetricName("QueueTimeMs.p99", expectedGroup, "", Map.of()),
            new MetricName("QueueTimeMs.p999", expectedGroup, "", Map.of()),
            new MetricName("QueueLength", expectedGroup, "", Map.of()),
            new MetricName("RemoteTimeMs.p99", expectedGroup, "", Map.of()),
            new MetricName("RemoteTimeMs.p999", expectedGroup, "", Map.of())
        );

        try (Metrics metrics = new Metrics()) {
            Map<MetricName, ?> metricsMap = metrics.metrics().entrySet().stream()
                .filter(entry -> entry.getKey().group().equals(expectedGroup))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            assertEquals(0, metricsMap.size());

            try (ForwardingManagerMetrics ignore = new ForwardingManagerMetrics(metrics, 1000)) {
                metricsMap = metrics.metrics().entrySet().stream()
                    .filter(entry -> entry.getKey().group().equals(expectedGroup))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                assertEquals(expectedMetrics.size(), metricsMap.size());
                metricsMap.keySet().forEach(name ->
                    assertTrue(expectedMetrics.contains(name), "Metric " + name + " not found in expected set")
                );
            } finally {
                metricsMap = metrics.metrics().entrySet().stream()
                    .filter(entry -> entry.getKey().group().equals(expectedGroup))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                assertEquals(0, metricsMap.size());
            }
        }
    }

    @Test
    void testQueueTimeMs() {
        try (Metrics metrics = new Metrics();
             ForwardingManagerMetrics forwardingManagerMetrics = new ForwardingManagerMetrics(metrics, 1000)) {
            KafkaMetric queueTimeMsP99 = metrics.metrics().get(forwardingManagerMetrics.queueTimeMsHist().latencyP99Name());
            KafkaMetric queueTimeMsP999 = metrics.metrics().get(forwardingManagerMetrics.queueTimeMsHist().latencyP999Name());
            assertEquals(Double.NaN, (Double) queueTimeMsP99.metricValue());
            assertEquals(Double.NaN, (Double) queueTimeMsP999.metricValue());
            for (int i = 0; i < 1000; i++) {
                forwardingManagerMetrics.queueTimeMsHist().record(i);
            }
            assertEquals(990.0, (Double) queueTimeMsP99.metricValue(), 0.1);
            assertEquals(999.0, (Double) queueTimeMsP999.metricValue(), 0.1);
        }
    }

    @Test
    void testQueueLength() {
        try (Metrics metrics = new Metrics();
             ForwardingManagerMetrics forwardingManagerMetrics = new ForwardingManagerMetrics(metrics, 1000)) {
            KafkaMetric queueLength = metrics.metrics().get(forwardingManagerMetrics.queueLengthName());
            assertEquals(0, (Integer) queueLength.metricValue());
            forwardingManagerMetrics.incrementQueueLength();
            assertEquals(1, (Integer) queueLength.metricValue());
        }
    }

    @Test
    void testRemoteTimeMs() {
        try (Metrics metrics = new Metrics();
             ForwardingManagerMetrics forwardingManagerMetrics = new ForwardingManagerMetrics(metrics, 1000)) {
            KafkaMetric remoteTimeMsP99 = metrics.metrics().get(forwardingManagerMetrics.remoteTimeMsHist().latencyP99Name());
            KafkaMetric remoteTimeMsP999 = metrics.metrics().get(forwardingManagerMetrics.remoteTimeMsHist().latencyP999Name());
            assertEquals(Double.NaN, (Double) remoteTimeMsP99.metricValue());
            assertEquals(Double.NaN, (Double) remoteTimeMsP999.metricValue());
            for (int i = 0; i < 1000; i++) {
                forwardingManagerMetrics.remoteTimeMsHist().record(i);
            }
            assertEquals(990.0, (Double) remoteTimeMsP99.metricValue(), 0.1);
            assertEquals(999.0, (Double) remoteTimeMsP999.metricValue(), 0.1);
        }
    }

    @Test
    void testTimeoutMs() {
        long timeoutMs = 500;
        try (Metrics metrics = new Metrics();
             ForwardingManagerMetrics forwardingManagerMetrics = new ForwardingManagerMetrics(metrics, timeoutMs)) {
            KafkaMetric queueTimeMsP99 = metrics.metrics().get(forwardingManagerMetrics.queueTimeMsHist().latencyP99Name());
            KafkaMetric queueTimeMsP999 = metrics.metrics().get(forwardingManagerMetrics.queueTimeMsHist().latencyP999Name());
            assertEquals(Double.NaN, (Double) queueTimeMsP99.metricValue());
            assertEquals(Double.NaN, (Double) queueTimeMsP999.metricValue());
            for (int i = 0; i < 100; i++) {
                forwardingManagerMetrics.queueTimeMsHist().record(i);
            }
            forwardingManagerMetrics.queueTimeMsHist().record(1000);

            assertEquals(99.0, (Double) queueTimeMsP99.metricValue(), 0.1);
            assertEquals(timeoutMs * 0.999, (Double) queueTimeMsP999.metricValue(), 0.1);
        }
    }
}