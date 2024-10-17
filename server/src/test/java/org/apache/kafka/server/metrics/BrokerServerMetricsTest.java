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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.image.MetadataProvenance;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class BrokerServerMetricsTest {
    @Test
    public void testMetricsExported() throws Exception {
        Metrics metrics = new Metrics();
        String expectedGroup = "broker-metadata-metrics";

        // Metric description is not use for metric name equality
        Set<MetricName> expectedMetrics = new HashSet<>(Arrays.asList(
                new MetricName("last-applied-record-offset", expectedGroup, "", Collections.emptyMap()),
                new MetricName("last-applied-record-timestamp", expectedGroup, "", Collections.emptyMap()),
                new MetricName("last-applied-record-lag-ms", expectedGroup, "", Collections.emptyMap()),
                new MetricName("metadata-load-error-count", expectedGroup, "", Collections.emptyMap()),
                new MetricName("metadata-apply-error-count", expectedGroup, "", Collections.emptyMap())
        ));

        try (BrokerServerMetrics ignored = new BrokerServerMetrics(metrics)) {
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

    @Test
    public void testLastAppliedRecordOffset() throws Exception {
        Metrics metrics = new Metrics();
        try (BrokerServerMetrics brokerMetrics = new BrokerServerMetrics(metrics)) {
            KafkaMetric offsetMetric = metrics.metrics().get(brokerMetrics.lastAppliedRecordOffsetName());
            assertEquals((double) -1L, offsetMetric.metricValue());

            // Update metric value and check
            long expectedValue = 1000;
            brokerMetrics.updateLastAppliedImageProvenance(new MetadataProvenance(
                    expectedValue,
                    brokerMetrics.lastAppliedImageProvenance().get().lastContainedEpoch(),
                    brokerMetrics.lastAppliedTimestamp(),
                    true));
            assertEquals((double) expectedValue, offsetMetric.metricValue());
        }
    }

    @Test
    public void testLastAppliedRecordTimestamp() throws Exception {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        try (BrokerServerMetrics brokerMetrics = new BrokerServerMetrics(metrics)) {
            time.sleep(1000);
            KafkaMetric timestampMetric = metrics.metrics().get(brokerMetrics.lastAppliedRecordTimestampName());
            KafkaMetric lagMetric = metrics.metrics().get(brokerMetrics.lastAppliedRecordLagMsName());

            assertEquals((double) -1L, timestampMetric.metricValue());
            assertEquals((double) time.milliseconds() + 1, lagMetric.metricValue());

            // Update metric value and check
            long timestamp = 500L;

            brokerMetrics.updateLastAppliedImageProvenance(new MetadataProvenance(
                    brokerMetrics.lastAppliedOffset(),
                    brokerMetrics.lastAppliedImageProvenance().get().lastContainedEpoch(),
                    timestamp,
                    true));
            assertEquals((double) timestamp, timestampMetric.metricValue());
            assertEquals((double) time.milliseconds() - timestamp, lagMetric.metricValue());
        }
    }

    @Test
    public void testMetadataLoadErrorCount() throws Exception {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        try (BrokerServerMetrics brokerMetrics = new BrokerServerMetrics(metrics)) {
            KafkaMetric metadataLoadErrorCountMetric = metrics.metrics().get(brokerMetrics.metadataLoadErrorCountName());

            assertEquals((double) 0L, metadataLoadErrorCountMetric.metricValue());

            // Update metric value and check
            long errorCount = 100;
            brokerMetrics.metadataLoadErrorCount().set(errorCount);
            assertEquals((double) errorCount, metadataLoadErrorCountMetric.metricValue());
        }
    }

    @Test
    public void testMetadataApplyErrorCount() throws Exception {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        try (BrokerServerMetrics brokerMetrics = new BrokerServerMetrics(metrics)) {
            KafkaMetric metadataApplyErrorCountMetric = metrics.metrics().get(brokerMetrics.metadataApplyErrorCountName());

            assertEquals((double) 0L, metadataApplyErrorCountMetric.metricValue());

            // Update metric value and check
            long errorCount = 100;
            brokerMetrics.metadataApplyErrorCount().set(errorCount);
            assertEquals((double) errorCount, metadataApplyErrorCountMetric.metricValue());
        }
    }
}
