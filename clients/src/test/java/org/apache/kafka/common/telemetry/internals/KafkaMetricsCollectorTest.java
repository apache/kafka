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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaMetricsCollectorTest {

    private static final String DOMAIN = "test.domain";

    private MetricName metricName;
    private Map<String, String> tags;
    private Metrics metrics;
    private MetricNamingStrategy<MetricName> metricNamingStrategy;
    private KafkaMetricsCollector collector;

    private final TestEmitter emitter = new TestEmitter();
    private final Clock clock = mock(Clock.class);
    private final Instant timeReference = Instant.ofEpochMilli(1000L);

    @BeforeEach
    public void setUp() {
        metrics = new Metrics();
        tags = Collections.singletonMap("tag", "value");
        metricName = metrics.metricName("name1", "group1", tags);

        when(clock.instant()).thenReturn(timeReference);

        // Define metric naming strategy.
        metricNamingStrategy = TelemetryMetricNamingConvention.getClientTelemetryMetricNamingStrategy(DOMAIN);

        // Define collector to test.
        collector = new KafkaMetricsCollector(
            metricNamingStrategy,
            clock
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

        when(clock.instant()).thenReturn(timeReference.plusSeconds(60));

        // Collect metrics.
        collector.collect(emitter);
        List<SinglePointMetric> result = emitter.emittedMetrics();

        // Should get exactly 3 Kafka measurables since Metrics always includes a count measurable i.e.
        // sum, delta sum, count.
        assertEquals(3, result.size());

        Metric counter = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1")).findFirst().get();

        Metric delta = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1.delta")).findFirst().get();

        assertTrue(counter.hasSum());
        assertTrue(delta.hasSum());
        assertEquals(tags, getTags(counter.getSum().getDataPoints(0).getAttributesList()));
        assertEquals(tags, getTags(delta.getSum().getDataPoints(0).getAttributesList()));


        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, delta.getSum().getAggregationTemporality());
        assertTrue(delta.getSum().getIsMonotonic());
        NumberDataPoint point = delta.getSum().getDataPoints(0);
        assertEquals(2d, point.getAsDouble(), 0.0);
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(61L).getEpochSecond()) +
            Instant.ofEpochSecond(61L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(1L).getEpochSecond()) +
            Instant.ofEpochSecond(1L).getNano(), point.getStartTimeUnixNano());


        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, counter.getSum().getAggregationTemporality());
        assertTrue(counter.getSum().getIsMonotonic());
        point = counter.getSum().getDataPoints(0);
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
        collector.collect(emitter);
        List<SinglePointMetric> result = emitter.emittedMetrics();

        // Should get exactly 3 Kafka measurables since Metrics always includes a count measurable i.e.
        // sum, delta sum, count.
        assertEquals(3, result.size());

        Metric counter = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1")).findFirst().get();

        Metric delta = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1.delta")).findFirst().get();

        assertTrue(counter.hasSum());
        assertTrue(delta.hasSum());
        assertEquals(tags, getTags(counter.getSum().getDataPoints(0).getAttributesList()));
        assertEquals(tags, getTags(delta.getSum().getDataPoints(0).getAttributesList()));
        assertEquals(15, counter.getSum().getDataPoints(0).getAsDouble(), 0.0);
        assertEquals(15, delta.getSum().getDataPoints(0).getAsDouble(), 0.0);
    }

    @Test
    public void testMeasurableGauge() {
        metrics.addMetric(metricName, (config, now) -> 100.0);

        collector.collect(emitter);
        List<SinglePointMetric> result = emitter.emittedMetrics();

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

        collector.collect(emitter);
        List<SinglePointMetric> result = emitter.emittedMetrics();

        // Should get exactly 2 Kafka measurables since Metrics always includes a count measurable.
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

        collector.collect(emitter);
        List<SinglePointMetric> result = emitter.emittedMetrics();

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

        collector.collect(emitter);
        assertEquals(2, emitter.emittedMetrics().size());

        metrics.removeMetric(metricName);
        assertFalse(collector.getTrackedMetrics().contains(metricNamingStrategy.metricKey(metricName)));

        // verify that the metric was removed.
        emitter.reset();
        collector.collect(emitter);
        List<SinglePointMetric> collected = emitter.emittedMetrics();
        assertEquals(1, collected.size());
        assertEquals("test.domain.kafka.count.count", collected.get(0).builder().build().getName());
    }

    @Test
    public void testSecondDeltaCollectDouble() {
        Sensor sensor = metrics.sensor("test");
        sensor.add(metricName, new CumulativeSum());

        sensor.record();
        sensor.record();
        when(clock.instant()).thenReturn(timeReference.plusSeconds(60));

        collector.collect(emitter);

        // Update it again by 5 and advance time by another 60 seconds.
        sensor.record();
        sensor.record();
        sensor.record();
        sensor.record();
        sensor.record();
        when(clock.instant()).thenReturn(timeReference.plusSeconds(120));

        emitter.reset();
        collector.collect(emitter);
        List<SinglePointMetric> result = emitter.emittedMetrics();

        assertEquals(3, result.size());


        Metric cumulative = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1")).findFirst().get();

        Metric delta = result.stream()
            .flatMap(metrics -> Stream.of(metrics.builder().build()))
            .filter(metric -> metric.getName().equals("test.domain.group1.name1.delta")).findFirst().get();

        NumberDataPoint point = delta.getSum().getDataPoints(0);
        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, delta.getSum().getAggregationTemporality());
        assertTrue(delta.getSum().getIsMonotonic());
        assertEquals(5d, point.getAsDouble(), 0.0);
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(121L).getEpochSecond()) +
            Instant.ofEpochSecond(121L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(61L).getEpochSecond()) +
            Instant.ofEpochSecond(61L).getNano(), point.getStartTimeUnixNano());

        point = cumulative.getSum().getDataPoints(0);
        assertEquals(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, cumulative.getSum().getAggregationTemporality());
        assertTrue(cumulative.getSum().getIsMonotonic());
        assertEquals(7d, point.getAsDouble(), 0.0);
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(121L).getEpochSecond()) +
            Instant.ofEpochSecond(121L).getNano(), point.getTimeUnixNano());
        assertEquals(TimeUnit.SECONDS.toNanos(Instant.ofEpochSecond(1L).getEpochSecond()) +
            Instant.ofEpochSecond(1L).getNano(), point.getStartTimeUnixNano());
    }

    @Test
    public void testCollectFilter() {
        metrics.addMetric(metricName, (config, now) -> 100.0);

        emitter.reconfigurePredicate(k -> !k.key().getName().endsWith(".count"));
        collector.collect(emitter);
        List<SinglePointMetric> result = emitter.emittedMetrics();

        // Should get exactly 1 Kafka measurables because we excluded the count measurable
        assertEquals(1, result.size());

        Metric counter = result.get(0).builder().build();

        assertTrue(counter.hasGauge());
        assertEquals(100L, counter.getGauge().getDataPoints(0).getAsDouble(), 0.0);
    }

    @Test
    public void testCollectFilterDynamicPredicate() {
        metrics.addMetric(metricName, (config, now) -> 100.0);
        metrics.addMetric(metrics.metricName("name2", "group2", tags), (config, now) -> 100.0);

        collector.collect(emitter);
        List<SinglePointMetric> result = emitter.emittedMetrics();

        assertEquals(3, result.size());

        emitter.reset();
        emitter.reconfigurePredicate(k -> k.key().getName().endsWith(".count"));
        collector.collect(emitter);
        result = emitter.emittedMetrics();
        assertEquals(1, result.size());

        emitter.reset();
        emitter.reconfigurePredicate(k -> k.key().getName().contains("name"));
        collector.collect(emitter);
        result = emitter.emittedMetrics();
        assertEquals(2, result.size());

        emitter.reset();
        emitter.reconfigurePredicate(key -> true);
        collector.collect(emitter);
        result = emitter.emittedMetrics();
        assertEquals(3, result.size());
    }

    @Test
    public void testCollectFilterWithDerivedMetrics() {
        MetricName name1 = metrics.metricName("nonMeasurable", "group1", tags);
        MetricName name2 = metrics.metricName("windowed", "group1", tags);
        MetricName name3 = metrics.metricName("cumulative", "group1", tags);

        metrics.addMetric(name1, (Gauge<Double>) (config, now) -> 99d);

        Sensor sensor = metrics.sensor("test");
        sensor.add(name2, new WindowedCount());
        sensor.add(name3, new CumulativeSum());

        collector.collect(emitter);
        List<SinglePointMetric> result = emitter.emittedMetrics();

        // no-filter shall result in all 6 data metrics.
        assertEquals(6, result.size());

        emitter.reset();
        emitter.reconfigurePredicate(k -> !k.key().getName().endsWith(".count"));
        collector.collect(emitter);
        result = emitter.emittedMetrics();

        // Drop metrics for Count type (Measurable metric but other that Windowed or Cumulative).
        assertEquals(5, result.size());

        emitter.reset();
        emitter.reconfigurePredicate(k -> !k.key().getName().endsWith(".nonmeasurable"));
        collector.collect(emitter);
        result = emitter.emittedMetrics();

        // Drop non-measurable metric.
        assertEquals(5, result.size());

        emitter.reset();
        emitter.reconfigurePredicate(k -> !k.key().getName().endsWith(".delta"));
        collector.collect(emitter);
        result = emitter.emittedMetrics();

        // Drop all delta derived metrics.
        assertEquals(4, result.size());
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