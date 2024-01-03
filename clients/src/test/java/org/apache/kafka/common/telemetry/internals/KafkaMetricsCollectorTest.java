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
package org.apache.kafka.common.telemetry.internals;

import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaMetricsCollectorTest {

    private static final String DOMAIN = "test.domain";

    private MetricName metricName;
    private Map<String, String> tags;
    private Metrics metrics;
    private MetricNamingStrategy<MetricName> metricNamingStrategy;
    private KafkaMetricsCollector collector;

    private TestEmitter testEmitter;
    private MockTime time;

    @BeforeEach
    public void setUp() {
        metrics = new Metrics();
        tags = Collections.singletonMap("tag", "value");
        metricName = metrics.metricName("name1", "group1", tags);
        time = new MockTime(0, 1000L, TimeUnit.MILLISECONDS.toNanos(1000L));
        testEmitter = new TestEmitter();

        // Define metric naming strategy.
        metricNamingStrategy = TelemetryMetricNamingConvention.getClientTelemetryMetricNamingStrategy(DOMAIN);

        // Define collector to test.
        collector = new KafkaMetricsCollector(
            metricNamingStrategy,
            time,
            Collections.emptySet()
        );

        // Add reporter to metrics.
        metrics.addReporter(getTestMetricsReporter());
    }

    @Test
    public void testMeasurableCounter() {
        Sensor sensor = metrics.sensor("test");
        sensor.add(metricName, new WindowedCount());

        sensor.record();
        sensor.record();

        time.sleep(60 * 1000L);

        // Collect metrics.
        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        // Should get exactly 2 Kafka measurables since Metrics always includes a count measurable.
        assertEquals(2, result.size());

        Metric counter = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1")).findFirst().get();

        assertTrue(counter.hasSum());
        assertEquals(tags, getTags(counter.getSum().getDataPoints(0).getAttributesList()));

        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, counter.getSum().getAggregationTemporality());
        assertTrue(counter.getSum().getIsMonotonic());
        NumberDataPoint point = counter.getSum().getDataPoints(0);
        assertEquals(2d, point.getAsDouble(), 0.0);
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(61L).getEpochSecond()) +
            Instant.ofEpochSecond(61L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(1L).getEpochSecond()) +
            Instant.ofEpochSecond(1L).getNano(), point.getStartTimeUnixNano());
    }

    @Test
    public void testMeasurableCounterDeltaMetrics() {
        Sensor sensor = metrics.sensor("test");
        sensor.add(metricName, new WindowedCount());

        sensor.record();
        sensor.record();

        time.sleep(60 * 1000L);

        // Collect delta metrics.
        testEmitter.onlyDeltaMetrics(true);
        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        // Should get exactly 2 Kafka measurables since Metrics always includes a count measurable.
        assertEquals(2, result.size());

        Metric counter = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1")).findFirst().get();

        assertTrue(counter.hasSum());
        assertEquals(tags, getTags(counter.getSum().getDataPoints(0).getAttributesList()));

        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, counter.getSum().getAggregationTemporality());
        assertTrue(counter.getSum().getIsMonotonic());
        NumberDataPoint point = counter.getSum().getDataPoints(0);
        assertEquals(2d, point.getAsDouble(), 0.0);
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(61L).getEpochSecond()) +
            Instant.ofEpochSecond(61L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(1L).getEpochSecond()) +
            Instant.ofEpochSecond(1L).getNano(), point.getStartTimeUnixNano());
    }

    @Test
    public void testMeasurableTotal() {
        Sensor sensor = metrics.sensor("test");
        sensor.add(metricName, new CumulativeSum());

        sensor.record(10L);
        sensor.record(5L);

        // Collect metrics.
        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        // Should get exactly 2 Kafka measurables since Metrics always includes a count measurable.
        assertEquals(2, result.size());

        Metric counter = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1")).findFirst().get();

        assertTrue(counter.hasSum());
        assertEquals(tags, getTags(counter.getSum().getDataPoints(0).getAttributesList()));
        assertEquals(15, counter.getSum().getDataPoints(0).getAsDouble(), 0.0);
    }

    @Test
    public void testMeasurableTotalDeltaMetrics() {
        Sensor sensor = metrics.sensor("test");
        sensor.add(metricName, new CumulativeSum());

        sensor.record(10L);
        sensor.record(5L);

        // Collect metrics.
        testEmitter.onlyDeltaMetrics(true);
        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        // Should get exactly 2 Kafka measurables since Metrics always includes a count measurable.
        assertEquals(2, result.size());

        Metric counter = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1")).findFirst().get();

        assertTrue(counter.hasSum());
        assertEquals(tags, getTags(counter.getSum().getDataPoints(0).getAttributesList()));
        assertEquals(15, counter.getSum().getDataPoints(0).getAsDouble(), 0.0);
    }

    @Test
    public void testMeasurableGauge() {
        metrics.addMetric(metricName, (config, now) -> 100.0);

        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        // Should get exactly 2 Kafka measurables since Metrics always includes a count measurable.
        assertEquals(2, result.size());

        Metric counter = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1")).findFirst().get();

        assertTrue(counter.hasGauge());
        assertEquals(tags, getTags(counter.getGauge().getDataPoints(0).getAttributesList()));
        assertEquals(100L, counter.getGauge().getDataPoints(0).getAsDouble(), 0.0);
    }

    @Test
    public void testNonMeasurable() {
        metrics.addMetric(metrics.metricName("float", "group1", tags), (Gauge<Float>) (config, now) -> 99f);
        metrics.addMetric(metrics.metricName("double", "group1", tags), (Gauge<Double>) (config, now) -> 99d);
        metrics.addMetric(metrics.metricName("int", "group1", tags), (Gauge<Integer>) (config, now) -> 100);
        metrics.addMetric(metrics.metricName("long", "group1", tags), (Gauge<Long>) (config, now) -> 100L);

        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        // Should get exactly 5 Kafka measurables since Metrics always includes a count measurable.
        assertEquals(5, result.size());

        result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.(float|double)")).forEach(
                doubleGauge -> {
                    assertTrue(doubleGauge.hasGauge());
                    assertEquals(tags, getTags(doubleGauge.getGauge().getDataPoints(0).getAttributesList()));
                    assertEquals(99d, doubleGauge.getGauge().getDataPoints(0).getAsDouble(), 0.0);
                });

        result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.(int|long)")).forEach(
                intGauge -> {
                    assertTrue(intGauge.hasGauge());
                    assertEquals(tags, getTags(intGauge.getGauge().getDataPoints(0).getAttributesList()));
                    assertEquals(100, intGauge.getGauge().getDataPoints(0).getAsDouble(), 0.0);
                });
    }

    @Test
    public void testMeasurableWithException() {
        metrics.addMetric(metricName, null, (config, now) -> {
            throw new RuntimeException();
        });

        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        //Verify only the global count of metrics exist
        assertEquals(1, result.size());
        // Group is registered as kafka-metrics-count
        assertEquals("test.domain.kafka.count.count", result.get(0).builder().build().getName());
        //Verify metrics with measure() method throw exception is not returned
        assertFalse(result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .anyMatch(metric -> metric.getName().equals("test.domain.group1.name1")));
    }

    @Test
    public void testMetricRemoval() {
        metrics.addMetric(metricName, (config, now) -> 100.0);

        collector.collect(testEmitter);
        assertEquals(2, testEmitter.emittedMetrics().size());

        metrics.removeMetric(metricName);
        assertFalse(collector.getTrackedMetrics().contains(metricNamingStrategy.metricKey(metricName)));

        // verify that the metric was removed.
        testEmitter.reset();
        collector.collect(testEmitter);
        List<SinglePointMetric> collected = testEmitter.emittedMetrics();
        assertEquals(1, collected.size());
        assertEquals("test.domain.kafka.count.count", collected.get(0).builder().build().getName());
    }

    @Test
    public void testSecondCollectCumulative() {
        Sensor sensor = metrics.sensor("test");
        sensor.add(metricName, new CumulativeSum());

        sensor.record();
        sensor.record();
        time.sleep(60 * 1000L);

        collector.collect(testEmitter);

        // Update it again by 5 and advance time by another 60 seconds.
        sensor.record();
        sensor.record();
        sensor.record();
        sensor.record();
        sensor.record();
        time.sleep(60 * 1000L);

        testEmitter.reset();
        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        assertEquals(2, result.size());


        Metric cumulative = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1")).findFirst().get();

        NumberDataPoint point = cumulative.getSum().getDataPoints(0);
        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, cumulative.getSum().getAggregationTemporality());
        assertTrue(cumulative.getSum().getIsMonotonic());
        assertEquals(7d, point.getAsDouble(), 0.0);
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(121L).getEpochSecond()) +
            Instant.ofEpochSecond(121L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(1L).getEpochSecond()) +
            Instant.ofEpochSecond(1L).getNano(), point.getStartTimeUnixNano());
    }

    @Test
    public void testSecondDeltaCollectDouble() {
        Sensor sensor = metrics.sensor("test");
        sensor.add(metricName, new CumulativeSum());

        sensor.record();
        sensor.record();
        time.sleep(60 * 1000L);

        testEmitter.onlyDeltaMetrics(true);
        collector.collect(testEmitter);

        // Update it again by 5 and advance time by another 60 seconds.
        sensor.record();
        sensor.record();
        sensor.record();
        sensor.record();
        sensor.record();
        time.sleep(60 * 1000L);

        testEmitter.reset();
        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        assertEquals(2, result.size());

        Metric cumulative = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1")).findFirst().get();

        NumberDataPoint point = cumulative.getSum().getDataPoints(0);
        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, cumulative.getSum().getAggregationTemporality());
        assertTrue(cumulative.getSum().getIsMonotonic());
        assertEquals(5d, point.getAsDouble(), 0.0);
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(121L).getEpochSecond()) +
            Instant.ofEpochSecond(121L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(61L).getEpochSecond()) +
            Instant.ofEpochSecond(61L).getNano(), point.getStartTimeUnixNano());
    }

    @Test
    public void testCollectFilter() {
        metrics.addMetric(metricName, (config, now) -> 100.0);

        testEmitter.reconfigurePredicate(k -> !k.key().name().endsWith(".count"));
        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        // Should get exactly 1 Kafka measurables because we excluded the count measurable
        assertEquals(1, result.size());

        Metric counter = result.get(0).builder().build();

        assertTrue(counter.hasGauge());
        assertEquals(100L, counter.getGauge().getDataPoints(0).getAsDouble(), 0.0);
    }

    @Test
    public void testCollectFilterWithCumulativeTemporality() {
        MetricName name1 = metrics.metricName("nonMeasurable", "group1", tags);
        MetricName name2 = metrics.metricName("windowed", "group1", tags);
        MetricName name3 = metrics.metricName("cumulative", "group1", tags);

        metrics.addMetric(name1, (Gauge<Double>) (config, now) -> 99d);

        Sensor sensor = metrics.sensor("test");
        sensor.add(name2, new WindowedCount());
        sensor.add(name3, new CumulativeSum());

        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        // no-filter shall result in all 4 data metrics.
        assertEquals(4, result.size());

        testEmitter.reset();
        testEmitter.reconfigurePredicate(k -> !k.key().name().endsWith(".count"));
        collector.collect(testEmitter);
        result = testEmitter.emittedMetrics();

        // Drop metrics for Count type.
        assertEquals(3, result.size());

        testEmitter.reset();
        testEmitter.reconfigurePredicate(k -> !k.key().name().endsWith(".nonmeasurable"));
        collector.collect(testEmitter);
        result = testEmitter.emittedMetrics();

        // Drop non-measurable metric.
        assertEquals(3, result.size());

        testEmitter.reset();
        testEmitter.reconfigurePredicate(key -> true);
        collector.collect(testEmitter);
        result = testEmitter.emittedMetrics();

        // Again no filter.
        assertEquals(4, result.size());
    }

    @Test
    public void testCollectFilterWithDeltaTemporality() {
        MetricName name1 = metrics.metricName("nonMeasurable", "group1", tags);
        MetricName name2 = metrics.metricName("windowed", "group1", tags);
        MetricName name3 = metrics.metricName("cumulative", "group1", tags);

        metrics.addMetric(name1, (Gauge<Double>) (config, now) -> 99d);

        Sensor sensor = metrics.sensor("test");
        sensor.add(name2, new WindowedCount());
        sensor.add(name3, new CumulativeSum());

        testEmitter.onlyDeltaMetrics(true);
        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();

        // no-filter shall result in all 4 data metrics.
        assertEquals(4, result.size());

        testEmitter.reset();
        testEmitter.reconfigurePredicate(k -> !k.key().name().endsWith(".count"));
        collector.collect(testEmitter);
        result = testEmitter.emittedMetrics();

        // Drop metrics for Count type.
        assertEquals(3, result.size());

        testEmitter.reset();
        testEmitter.reconfigurePredicate(k -> !k.key().name().endsWith(".nonmeasurable"));
        collector.collect(testEmitter);
        result = testEmitter.emittedMetrics();

        // Drop non-measurable metric.
        assertEquals(3, result.size());

        testEmitter.reset();
        testEmitter.reconfigurePredicate(key -> true);
        collector.collect(testEmitter);
        result = testEmitter.emittedMetrics();

        // Again no filter.
        assertEquals(4, result.size());
    }

    @Test
    public void testCollectMetricsWithTemporalityChange() {
        Sensor sensor = metrics.sensor("test");
        sensor.add(metricName, new WindowedCount());
        testEmitter.reconfigurePredicate(k -> !k.key().name().endsWith(".count"));

        // Emit metrics as cumulative, verify the current time is 60 seconds ahead of start time.
        sensor.record();
        time.sleep(60 * 1000L);
        collector.collect(testEmitter);

        List<SinglePointMetric> result = testEmitter.emittedMetrics();
        Metric counter = result.get(0).builder().build();
        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, counter.getSum().getAggregationTemporality());
        NumberDataPoint point = counter.getSum().getDataPoints(0);
        assertEquals(1d, point.getAsDouble());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(61L).getEpochSecond()) +
            Instant.ofEpochSecond(61L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(1L).getEpochSecond()) +
            Instant.ofEpochSecond(1L).getNano(), point.getStartTimeUnixNano());

        // Again emit metrics as cumulative, verify the start time is unchanged and current time is
        // advanced by 60 seconds again.
        time.sleep(60 * 1000L);
        sensor.record();
        testEmitter.reset();
        collector.collect(testEmitter);

        result = testEmitter.emittedMetrics();
        counter = result.get(0).builder().build();
        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, counter.getSum().getAggregationTemporality());
        point = counter.getSum().getDataPoints(0);
        assertEquals(2d, point.getAsDouble(), 0.0);
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(121L).getEpochSecond()) +
            Instant.ofEpochSecond(121L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(1L).getEpochSecond()) +
            Instant.ofEpochSecond(1L).getNano(), point.getStartTimeUnixNano());


        // Change Temporality. Emit metrics as delta, verify the temporality changes to delta and start time is reset to
        // current time.
        time.sleep(60 * 1000L);
        sensor.record();
        testEmitter.reset();
        testEmitter.onlyDeltaMetrics(true);
        collector.metricsReset();
        collector.collect(testEmitter);

        result = testEmitter.emittedMetrics();
        counter = result.get(0).builder().build();
        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, counter.getSum().getAggregationTemporality());
        point = counter.getSum().getDataPoints(0);
        assertEquals(3d, point.getAsDouble(), 0.0);
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(181L).getEpochSecond()) +
            Instant.ofEpochSecond(181L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(181L).getEpochSecond()) +
            Instant.ofEpochSecond(181L).getNano(), point.getStartTimeUnixNano());

        // Again emit metrics as delta, verify the start time is tracked properly and only delta value
        // is present on response.
        time.sleep(60 * 1000L);
        sensor.record();
        testEmitter.reset();
        collector.collect(testEmitter);

        result = testEmitter.emittedMetrics();
        counter = result.get(0).builder().build();
        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, counter.getSum().getAggregationTemporality());
        point = counter.getSum().getDataPoints(0);
        assertEquals(1d, point.getAsDouble(), 0.0);
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(241L).getEpochSecond()) +
            Instant.ofEpochSecond(241L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(181L).getEpochSecond()) +
            Instant.ofEpochSecond(181L).getNano(), point.getStartTimeUnixNano());

        // Change Temporality. Emit metrics as cumulative, verify the temporality changes to cumulative
        // and start time is reset to current time.
        time.sleep(60 * 1000L);
        sensor.record();
        testEmitter.reset();
        testEmitter.onlyDeltaMetrics(false);
        collector.metricsReset();
        collector.collect(testEmitter);

        result = testEmitter.emittedMetrics();
        counter = result.get(0).builder().build();
        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, counter.getSum().getAggregationTemporality());
        point = counter.getSum().getDataPoints(0);
        assertEquals(5d, point.getAsDouble(), 0.0);
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(301L).getEpochSecond()) +
            Instant.ofEpochSecond(301L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(301L).getEpochSecond()) +
            Instant.ofEpochSecond(301L).getNano(), point.getStartTimeUnixNano());
    }

    @Test
    public void testCollectMetricsWithExcludeLabels() {
        collector = new KafkaMetricsCollector(
            metricNamingStrategy,
            time,
            Collections.singleton("tag2")
        );

        tags = new HashMap<>();
        tags.put("tag1", "value1");
        tags.put("tag2", "value2");

        // Gauge metric.
        MetricName name1 = metrics.metricName("nonMeasurable", "group1", tags);
        metrics.addMetric(name1, (Gauge<Double>) (config, now) -> 99d);
        // Sum metric.
        MetricName name2 = metrics.metricName("counter", "group1", tags);
        Sensor sensor = metrics.sensor("counter");
        sensor.add(name2, new WindowedCount());
        sensor.record();
        testEmitter.reconfigurePredicate(k -> !k.key().name().endsWith(".count"));

        // Collect sum metrics
        collector.collect(testEmitter);
        List<SinglePointMetric> result = testEmitter.emittedMetrics();
        Metric metric = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(m -> m.getName().equals("test.domain.group1.nonmeasurable")).findFirst().get();

        assertEquals(1, metric.getGauge().getDataPointsCount());
        NumberDataPoint point = metric.getGauge().getDataPoints(0);
        assertEquals(1, point.getAttributesCount());
        assertEquals("tag1", point.getAttributes(0).getKey());
        assertEquals("value1", point.getAttributes(0).getValue().getStringValue());

        metric = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(m -> m.getName().equals("test.domain.group1.counter")).findFirst().get();

        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, metric.getSum().getAggregationTemporality());
        assertEquals(1, metric.getSum().getDataPointsCount());
        point = metric.getSum().getDataPoints(0);
        assertEquals(1, point.getAttributesCount());
        assertEquals("tag1", point.getAttributes(0).getKey());
        assertEquals("value1", point.getAttributes(0).getValue().getStringValue());

        testEmitter.reset();
        testEmitter.onlyDeltaMetrics(true);
        collector.collect(testEmitter);
        result = testEmitter.emittedMetrics();

        // Delta metrics.
        metric = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(m -> m.getName().equals("test.domain.group1.counter")).findFirst().get();

        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, metric.getSum().getAggregationTemporality());
        assertEquals(1, metric.getSum().getDataPointsCount());
        point = metric.getSum().getDataPoints(0);
        assertEquals(1, point.getAttributesCount());
        assertEquals("tag1", point.getAttributes(0).getKey());
        assertEquals("value1", point.getAttributes(0).getValue().getStringValue());
    }

    private MetricsReporter getTestMetricsReporter() {
        // Inline implementation of MetricsReporter for testing.
        return new MetricsReporter() {
            @Override
            public void init(List<KafkaMetric> metrics) {
                collector.init(metrics);
            }

            @Override
            public void metricChange(KafkaMetric metric) {
                collector.metricChange(metric);
            }

            @Override
            public void metricRemoval(KafkaMetric metric) {
                collector.metricRemoval(metric);
            }

            @Override
            public void close() {
                // do nothing
            }

            @Override
            public void configure(Map<String, ?> configs) {
                // do nothing
            }
        };
    }

    public static Map<String, String> getTags(List<KeyValue> attributes) {
        return attributes.stream()
            .filter(attr -> attr.getValue().hasStringValue())
            .collect(Collectors.toMap(
                KeyValue::getKey,
                attr -> attr.getValue().getStringValue()
            ));
    }
}