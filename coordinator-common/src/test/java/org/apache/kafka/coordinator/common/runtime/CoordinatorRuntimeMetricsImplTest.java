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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.IntStream;

import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl.BATCH_FLUSH_TIME_METRIC_NAME;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl.EVENT_PROCESSING_TIME_METRIC_NAME;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl.EVENT_PURGATORY_TIME_METRIC_NAME;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl.EVENT_QUEUE_TIME_METRIC_NAME;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl.NUM_PARTITIONS_METRIC_NAME;
import static org.apache.kafka.coordinator.common.runtime.KafkaMetricHistogram.MAX_LATENCY_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CoordinatorRuntimeMetricsImplTest {

    private static final String METRICS_GROUP = "test-runtime-metrics";
    
    @Test
    public void testMetricNames() {
        Metrics metrics = new Metrics();

        HashSet<org.apache.kafka.common.MetricName> expectedMetrics = new HashSet<>(Arrays.asList(
            kafkaMetricName(metrics, NUM_PARTITIONS_METRIC_NAME, "state", "loading"),
            kafkaMetricName(metrics, NUM_PARTITIONS_METRIC_NAME, "state", "active"),
            kafkaMetricName(metrics, NUM_PARTITIONS_METRIC_NAME, "state", "failed"),
            kafkaMetricName(metrics, "event-queue-size"),
            kafkaMetricName(metrics, "partition-load-time-max"),
            kafkaMetricName(metrics, "partition-load-time-avg"),
            kafkaMetricName(metrics, "thread-idle-ratio-avg"),
            kafkaMetricName(metrics, "event-queue-time-ms-max"),
            kafkaMetricName(metrics, "event-queue-time-ms-p50"),
            kafkaMetricName(metrics, "event-queue-time-ms-p95"),
            kafkaMetricName(metrics, "event-queue-time-ms-p99"),
            kafkaMetricName(metrics, "event-queue-time-ms-p999"),
            kafkaMetricName(metrics, "event-processing-time-ms-max"),
            kafkaMetricName(metrics, "event-processing-time-ms-p50"),
            kafkaMetricName(metrics, "event-processing-time-ms-p95"),
            kafkaMetricName(metrics, "event-processing-time-ms-p99"),
            kafkaMetricName(metrics, "event-processing-time-ms-p999"),
            kafkaMetricName(metrics, "event-purgatory-time-ms-max"),
            kafkaMetricName(metrics, "event-purgatory-time-ms-p50"),
            kafkaMetricName(metrics, "event-purgatory-time-ms-p95"),
            kafkaMetricName(metrics, "event-purgatory-time-ms-p99"),
            kafkaMetricName(metrics, "event-purgatory-time-ms-p999"),
            kafkaMetricName(metrics, "batch-flush-time-ms-max"),
            kafkaMetricName(metrics, "batch-flush-time-ms-p50"),
            kafkaMetricName(metrics, "batch-flush-time-ms-p95"),
            kafkaMetricName(metrics, "batch-flush-time-ms-p99"),
            kafkaMetricName(metrics, "batch-flush-time-ms-p999")
        ));

        try (CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP)) {
            runtimeMetrics.registerEventQueueSizeGauge(() -> 0);
            expectedMetrics.forEach(metricName -> assertTrue(metrics.metrics().containsKey(metricName)));
        }

        expectedMetrics.forEach(metricName -> assertFalse(
            metrics.metrics().containsKey(metricName),
            "metrics did not expect to contain metricName: " + metricName + " after closing."
        ));
    }

    @Test
    public void testUpdateNumPartitionsMetrics() {
        Metrics metrics = new Metrics();

        try (CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP)) {
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

        try (CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP)) {
            long startTimeMs = time.milliseconds();
            runtimeMetrics.recordPartitionLoadSensor(startTimeMs, startTimeMs + 1000);
            runtimeMetrics.recordPartitionLoadSensor(startTimeMs, startTimeMs + 2000);

            org.apache.kafka.common.MetricName metricName = kafkaMetricName(metrics, "partition-load-time-avg");

            KafkaMetric metric = metrics.metrics().get(metricName);
            assertEquals(1500.0, metric.metricValue());

            metricName = kafkaMetricName(metrics, "partition-load-time-max");
            metric = metrics.metrics().get(metricName);
            assertEquals(2000.0, metric.metricValue());
        }
    }

    @Test
    public void testThreadIdleSensor() {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP);
        IntStream.range(0, 3).forEach(i -> runtimeMetrics.recordThreadIdleTime((i + 1) * 1000L));

        org.apache.kafka.common.MetricName metricName = kafkaMetricName(metrics, "thread-idle-ratio-avg");
        KafkaMetric metric = metrics.metrics().get(metricName);
        assertEquals(6 / 30.0, metric.metricValue()); // 'total_ms / window_ms'
    }

    @Test
    public void testEventQueueSize() {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        try (CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP)) {
            runtimeMetrics.registerEventQueueSizeGauge(() -> 5);
            assertMetricGauge(metrics, kafkaMetricName(metrics, "event-queue-size"), 5);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        EVENT_QUEUE_TIME_METRIC_NAME,
        EVENT_PROCESSING_TIME_METRIC_NAME,
        EVENT_PURGATORY_TIME_METRIC_NAME,
        BATCH_FLUSH_TIME_METRIC_NAME
    })
    public void testHistogramMetrics(String metricNamePrefix) {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP);

        IntStream.range(1, 1001).forEach(i -> {
            switch (metricNamePrefix) {
                case EVENT_QUEUE_TIME_METRIC_NAME:
                    runtimeMetrics.recordEventQueueTime(i);
                    break;
                case EVENT_PROCESSING_TIME_METRIC_NAME:
                    runtimeMetrics.recordEventProcessingTime(i);
                    break;
                case EVENT_PURGATORY_TIME_METRIC_NAME:
                    runtimeMetrics.recordEventPurgatoryTime(i);
                    break;
                case BATCH_FLUSH_TIME_METRIC_NAME:
                    runtimeMetrics.recordFlushTime(i);
            }
        });

        MetricName metricName = kafkaMetricName(metrics, metricNamePrefix + "-max");
        KafkaMetric metric = metrics.metrics().get(metricName);
        assertEquals(1000.0, metric.metricValue());

        metricName = kafkaMetricName(metrics, metricNamePrefix + "-p50");
        metric = metrics.metrics().get(metricName);
        assertEquals(500.0, metric.metricValue());

        metricName = kafkaMetricName(metrics, metricNamePrefix + "-p95");
        metric = metrics.metrics().get(metricName);
        assertEquals(950.0, metric.metricValue());

        metricName = kafkaMetricName(metrics, metricNamePrefix + "-p99");
        metric = metrics.metrics().get(metricName);
        assertEquals(990.0, metric.metricValue());

        metricName = kafkaMetricName(metrics, metricNamePrefix + "-p999");
        metric = metrics.metrics().get(metricName);
        assertEquals(999.0, metric.metricValue());
    }

    @Test
    public void testRecordEventPurgatoryTimeLimit() {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP);

        IntStream.range(1, 1001).forEach(__ -> runtimeMetrics.recordEventPurgatoryTime(MAX_LATENCY_MS + 1000L));

        MetricName metricName = kafkaMetricName(metrics, EVENT_PURGATORY_TIME_METRIC_NAME + "-max");
        KafkaMetric metric = metrics.metrics().get(metricName);
        long value = ((Double) metric.metricValue()).longValue();

        // 3 sigfigs in HdrHistogram is not precise enough.
        assertTrue(value >= MAX_LATENCY_MS && value < MAX_LATENCY_MS + 1000L);
    }

    private static void assertMetricGauge(Metrics metrics, org.apache.kafka.common.MetricName metricName, long count) {
        assertEquals(count, (long) metrics.metric(metricName).metricValue());
    }

    private static MetricName kafkaMetricName(Metrics metrics, String name, String... keyValue) {
        return metrics.metricName(name, METRICS_GROUP, "", keyValue);
    }
}
