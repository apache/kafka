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

import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl.BATCH_FLUSH_TIME_METRIC_NAME;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl.BATCH_LINGER_TIME_METRIC_NAME;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl.EVENT_PROCESSING_TIME_METRIC_NAME;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl.EVENT_PURGATORY_TIME_METRIC_NAME;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl.EVENT_QUEUE_TIME_METRIC_NAME;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl.NUM_PARTITIONS_METRIC_NAME;
import static org.apache.kafka.coordinator.common.runtime.KafkaMetricHistogram.MAX_LATENCY_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CoordinatorRuntimeMetricsImplTest {

    private static final String METRICS_GROUP = "test-runtime-metrics";
    private static final String OTHER_METRICS_GROUP = "test-runtime-metrics-2";

    @Test
    public void testMetricNames() {
        Metrics metrics = new Metrics();

        Set<org.apache.kafka.common.MetricName> expectedMetrics = Set.of(
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
            kafkaMetricName(metrics, "batch-linger-time-ms-max"),
            kafkaMetricName(metrics, "batch-linger-time-ms-p50"),
            kafkaMetricName(metrics, "batch-linger-time-ms-p95"),
            kafkaMetricName(metrics, "batch-linger-time-ms-p99"),
            kafkaMetricName(metrics, "batch-linger-time-ms-p999"),
            kafkaMetricName(metrics, "batch-flush-time-ms-max"),
            kafkaMetricName(metrics, "batch-flush-time-ms-p50"),
            kafkaMetricName(metrics, "batch-flush-time-ms-p95"),
            kafkaMetricName(metrics, "batch-flush-time-ms-p99"),
            kafkaMetricName(metrics, "batch-flush-time-ms-p999"),
            kafkaMetricName(metrics, "batch-flush-rate")
        );

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
    public void testNumPartitionsMetricsGroupIsolation() {
        Metrics metrics = new Metrics();

        try (CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP);
             CoordinatorRuntimeMetricsImpl runtimeMetrics2 = new CoordinatorRuntimeMetricsImpl(metrics, OTHER_METRICS_GROUP)) {
            IntStream.range(0, 3)
                .forEach(__ -> runtimeMetrics.recordPartitionStateChange(CoordinatorState.INITIAL, CoordinatorState.LOADING));
            IntStream.range(0, 2)
                .forEach(__ -> runtimeMetrics.recordPartitionStateChange(CoordinatorState.LOADING, CoordinatorState.ACTIVE));
            IntStream.range(0, 1)
                .forEach(__ -> runtimeMetrics.recordPartitionStateChange(CoordinatorState.ACTIVE, CoordinatorState.FAILED));

            for (String state : List.of("loading", "active", "failed")) {
                assertMetricGauge(metrics, kafkaMetricName(metrics, NUM_PARTITIONS_METRIC_NAME, "state", state), 1);
                assertMetricGauge(metrics, otherGroupKafkaMetricName(metrics, NUM_PARTITIONS_METRIC_NAME, "state", state), 0);
            }
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

    @ParameterizedTest
    @ValueSource(strings = {
        "partition-load-time-avg",
        "partition-load-time-max"
    })
    public void testPartitionLoadSensorMetricsGroupIsolation(String name) {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        try (CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP);
             CoordinatorRuntimeMetricsImpl runtimeMetrics2 = new CoordinatorRuntimeMetricsImpl(metrics, OTHER_METRICS_GROUP)) {
            long startTimeMs = time.milliseconds();
            runtimeMetrics.recordPartitionLoadSensor(startTimeMs, startTimeMs + 1000);

            org.apache.kafka.common.MetricName metricName = kafkaMetricName(metrics, name);
            org.apache.kafka.common.MetricName otherGroupMetricName = otherGroupKafkaMetricName(metrics, name);
            KafkaMetric metric = metrics.metrics().get(metricName);
            KafkaMetric otherMetric = metrics.metrics().get(otherGroupMetricName);
            assertNotEquals(Double.NaN, metric.metricValue());
            assertEquals(Double.NaN, otherMetric.metricValue());
        }
    }

    @Test
    public void testThreadIdleSensor() {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP);
        IntStream.range(0, 3).forEach(i -> runtimeMetrics.recordThreadIdleTime((i + 1) * 1000.0));

        org.apache.kafka.common.MetricName metricName = kafkaMetricName(metrics, "thread-idle-ratio-avg");
        KafkaMetric metric = metrics.metrics().get(metricName);
        assertEquals(6 / 30.0, metric.metricValue()); // 'total_ms / window_ms'
    }

    @Test
    public void testThreadIdleSensorMetricsGroupIsolation() {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        try (CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP);
             CoordinatorRuntimeMetricsImpl runtimeMetrics2 = new CoordinatorRuntimeMetricsImpl(metrics, OTHER_METRICS_GROUP)) {
            runtimeMetrics.recordThreadIdleTime(1000.0);

            org.apache.kafka.common.MetricName metricName = kafkaMetricName(metrics, "thread-idle-ratio-avg");
            org.apache.kafka.common.MetricName otherGroupMetricName = otherGroupKafkaMetricName(metrics, "thread-idle-ratio-avg");
            assertNotEquals(0.0, metrics.metrics().get(metricName).metricValue());
            assertEquals(0.0, metrics.metrics().get(otherGroupMetricName).metricValue());
        }
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

    @Test
    public void testEventQueueSizeMetricsGroupIsolation() {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        try (CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP);
             CoordinatorRuntimeMetricsImpl otherRuntimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, OTHER_METRICS_GROUP)) {
            runtimeMetrics.registerEventQueueSizeGauge(() -> 5);
            otherRuntimeMetrics.registerEventQueueSizeGauge(() -> 0);

            assertMetricGauge(metrics, kafkaMetricName(metrics, "event-queue-size"), 5);
            assertMetricGauge(metrics, otherGroupKafkaMetricName(metrics, "event-queue-size"), 0);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        EVENT_QUEUE_TIME_METRIC_NAME,
        EVENT_PROCESSING_TIME_METRIC_NAME,
        EVENT_PURGATORY_TIME_METRIC_NAME,
        BATCH_LINGER_TIME_METRIC_NAME,
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
                case BATCH_LINGER_TIME_METRIC_NAME:
                    runtimeMetrics.recordLingerTime(i);
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

    @ParameterizedTest
    @ValueSource(strings = {
        EVENT_QUEUE_TIME_METRIC_NAME,
        EVENT_PROCESSING_TIME_METRIC_NAME,
        EVENT_PURGATORY_TIME_METRIC_NAME,
        BATCH_FLUSH_TIME_METRIC_NAME
    })
    public void testHistogramMetricsGroupIsolation(String metricNamePrefix) {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        try (CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP);
             CoordinatorRuntimeMetricsImpl runtimeMetrics2 = new CoordinatorRuntimeMetricsImpl(metrics, OTHER_METRICS_GROUP)) {
            switch (metricNamePrefix) {
                case EVENT_QUEUE_TIME_METRIC_NAME:
                    runtimeMetrics.recordEventQueueTime(1000);
                    break;
                case EVENT_PROCESSING_TIME_METRIC_NAME:
                    runtimeMetrics.recordEventProcessingTime(1000);
                    break;
                case EVENT_PURGATORY_TIME_METRIC_NAME:
                    runtimeMetrics.recordEventPurgatoryTime(1000);
                    break;
                case BATCH_FLUSH_TIME_METRIC_NAME:
                    runtimeMetrics.recordFlushTime(1000);
            }

            // Check metric group isolation
            for (String suffix : List.of("-max", "-p50", "-p95", "-p99", "-p999")) {
                org.apache.kafka.common.MetricName metricName = kafkaMetricName(metrics, metricNamePrefix + suffix);
                org.apache.kafka.common.MetricName otherGroupMetricName = otherGroupKafkaMetricName(metrics, metricNamePrefix + suffix);
                KafkaMetric metric = metrics.metrics().get(metricName);
                KafkaMetric otherMetric = metrics.metrics().get(otherGroupMetricName);
                assertNotEquals(0.0, metric.metricValue());
                assertEquals(0.0, otherMetric.metricValue());
            }
        }
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

    @Test
    public void testFlushRateSensor() {
        Time time = new MockTime();
        Metrics metrics = new Metrics(time);

        CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP);
        IntStream.range(0, 3).forEach(i -> runtimeMetrics.recordFlushTime((i + 1) * 1000));

        org.apache.kafka.common.MetricName metricName = kafkaMetricName(metrics, "batch-flush-rate");
        KafkaMetric metric = metrics.metrics().get(metricName);
        assertEquals(0.1, metric.metricValue()); // 'total / window_s'
    }

    private static MetricName kafkaMetricName(Metrics metrics, String name, String... keyValue) {
        return metrics.metricName(name, METRICS_GROUP, "", keyValue);
    }

    private static MetricName otherGroupKafkaMetricName(Metrics metrics, String name, String... keyValue) {
        return metrics.metricName(name, OTHER_METRICS_GROUP, "", keyValue);
    }
}
