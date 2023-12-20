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
package org.apache.kafka.coordinator.group.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime.CoordinatorState;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.IntStream;

import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorRuntimeMetrics.METRICS_GROUP;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorRuntimeMetrics.NUM_PARTITIONS_METRIC_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GroupCoordinatorRuntimeMetricsTest {
    
    @Test
    public void testMetricNames() {
        Metrics metrics = new Metrics();

        HashSet<org.apache.kafka.common.MetricName> expectedMetrics = new HashSet<>(Arrays.asList(
            kafkaMetricName(metrics, NUM_PARTITIONS_METRIC_NAME, "state", "loading"),
            kafkaMetricName(metrics, NUM_PARTITIONS_METRIC_NAME, "state", "active"),
            kafkaMetricName(metrics, NUM_PARTITIONS_METRIC_NAME, "state", "failed"),
            metrics.metricName("event-queue-size", METRICS_GROUP),
            metrics.metricName("partition-load-time-max", METRICS_GROUP),
            metrics.metricName("partition-load-time-avg", METRICS_GROUP),
            metrics.metricName("thread-idle-ratio-min", METRICS_GROUP),
            metrics.metricName("thread-idle-ratio-avg", METRICS_GROUP)
        ));

        try (GroupCoordinatorRuntimeMetrics runtimeMetrics = new GroupCoordinatorRuntimeMetrics(metrics)) {
            runtimeMetrics.registerEventQueueSizeGauge(() -> 0);
            expectedMetrics.forEach(metricName -> assertTrue(metrics.metrics().containsKey(metricName)));
        }

        expectedMetrics.forEach(metricName -> assertFalse(metrics.metrics().containsKey(metricName)));
    }

    @Test
    public void testUpdateNumPartitionsMetrics() {
        Metrics metrics = new Metrics();

        try (GroupCoordinatorRuntimeMetrics runtimeMetrics = new GroupCoordinatorRuntimeMetrics(metrics)) {
            IntStream.range(0, 10)
                .forEach(__ -> runtimeMetrics.recordPartitionStateChange(CoordinatorState.INITIAL, CoordinatorState.LOADING));
            IntStream.range(0, 8)
                .forEach(__ -> runtimeMetrics.recordPartitionStateChange(CoordinatorState.LOADING, CoordinatorState.ACTIVE));
            IntStream.range(0, 8)
                .forEach(__ -> runtimeMetrics.recordPartitionStateChange(CoordinatorState.ACTIVE, CoordinatorState.FAILED));
            IntStream.range(0, 2)
                .forEach(__ -> runtimeMetrics.recordPartitionStateChange(CoordinatorState.FAILED, CoordinatorState.CLOSED));

            assertMetricGauge(metrics, kafkaMetricName(metrics, NUM_PARTITIONS_METRIC_NAME, "state", "loading"), 2);
            assertMetricGauge(metrics, kafkaMetricName(metrics, NUM_PARTITIONS_METRIC_NAME, "state", "active"), 0);
            assertMetricGauge(metrics, kafkaMetricName(metrics, NUM_PARTITIONS_METRIC_NAME, "state", "failed"), 6);
        }
    }

    @Test
    public void testPartitionLoadSensorMetrics() {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        try (GroupCoordinatorRuntimeMetrics runtimeMetrics = new GroupCoordinatorRuntimeMetrics(metrics)) {
            long startTimeMs = time.milliseconds();
            runtimeMetrics.recordPartitionLoadSensor(startTimeMs, startTimeMs + 1000);
            runtimeMetrics.recordPartitionLoadSensor(startTimeMs, startTimeMs + 2000);

            org.apache.kafka.common.MetricName metricName = metrics.metricName(
                "partition-load-time-avg", METRICS_GROUP);

            KafkaMetric metric = metrics.metrics().get(metricName);
            assertEquals(1500.0, metric.metricValue());

            metricName = metrics.metricName(
                "partition-load-time-max", METRICS_GROUP);
            metric = metrics.metrics().get(metricName);
            assertEquals(2000.0, metric.metricValue());
        }
    }

    @Test
    public void testThreadIdleRatioSensor() {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        try (GroupCoordinatorRuntimeMetrics runtimeMetrics = new GroupCoordinatorRuntimeMetrics(metrics)) {
            IntStream.range(0, 3).forEach(i -> runtimeMetrics.recordThreadIdleRatio(1.0 / (i + 1)));

            org.apache.kafka.common.MetricName metricName = metrics.metricName(
                "thread-idle-ratio-avg", METRICS_GROUP);

            KafkaMetric metric = metrics.metrics().get(metricName);
            assertEquals((11.0 / 6.0) / 3.0, metric.metricValue()); // (6/6 + 3/6 + 2/6) / 3

            metricName = metrics.metricName(
                "thread-idle-ratio-min", METRICS_GROUP);
            metric = metrics.metrics().get(metricName);
            assertEquals(1.0 / 3.0, metric.metricValue());
        }
    }

    @Test
    public void testEventQueueSize() {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        try (GroupCoordinatorRuntimeMetrics runtimeMetrics = new GroupCoordinatorRuntimeMetrics(metrics)) {
            runtimeMetrics.registerEventQueueSizeGauge(() -> 5);
            assertMetricGauge(metrics, kafkaMetricName(metrics, "event-queue-size"), 5);
        }
    }

    private static void assertMetricGauge(Metrics metrics, org.apache.kafka.common.MetricName metricName, long count) {
        assertEquals(count, (long) metrics.metric(metricName).metricValue());
    }

    private static com.yammer.metrics.core.MetricName yammerMetricName(String type, String name) {
        String mBeanName = String.format("kafka.coordinator.group:type=%s,name=%s", type, name);
        return new com.yammer.metrics.core.MetricName("kafka.coordinator.group", type, name, null, mBeanName);
    }

    private static MetricName kafkaMetricName(Metrics metrics, String name, String... keyValue) {
        return metrics.metricName(name, METRICS_GROUP, "", keyValue);
    }
}