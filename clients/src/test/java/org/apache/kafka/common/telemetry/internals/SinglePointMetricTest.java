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

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SinglePointMetricTest {

    private MetricKey metricKey;
    private Instant now;

    /*
     Test compares the metric representation from returned builder to ensure that the metric is
     constructed correctly.

     For example: Gauge metric with name "name" and double value 1.0 at certain time is represented as:

       name: "name"
          gauge {
            data_points {
              time_unix_nano: 1698063981021420000
              as_double: 1.0
            }
          }
     */

    @BeforeEach
    public void setUp() {
        metricKey = new MetricKey("name", Collections.emptyMap());
        now = Instant.now();
    }

    @Test
    public void testGaugeWithNumberValue() {
        SinglePointMetric gaugeNumber = SinglePointMetric.gauge(metricKey, Long.valueOf(1), now);
        assertEquals(getIntGaugeMetric("name", 1, now),
            gaugeNumber.builder().build());
    }

    @Test
    public void testGaugeWithDoubleValue() {
        SinglePointMetric gaugeNumber = SinglePointMetric.gauge(metricKey, 1.0, now);
        assertEquals(getDoubleGaugeMetric("name", 1.0, now),
            gaugeNumber.builder().build());
    }

    @Test
    public void testGaugeWithMetricTags() {
        MetricKey metricKey = new MetricKey("name", Collections.singletonMap("tag", "value"));
        SinglePointMetric gaugeNumber = SinglePointMetric.gauge(metricKey, 1.0, now);
        assertEquals(getDoubleGaugeMetric("name", 1.0, now,
                Collections.singletonMap("tag", "value")), gaugeNumber.builder().build());
    }

    @Test
    public void testSum() {
        SinglePointMetric sum = SinglePointMetric.sum(metricKey, 1.0, false, now);
        assertEquals(getSumMetric("name", 1.0, false, now), sum.builder().build());
    }

    @Test
    public void testSumWithStartTimeAndTags() {
        MetricKey metricKey = new MetricKey("name", Collections.singletonMap("tag", "value"));
        SinglePointMetric sum = SinglePointMetric.sum(metricKey, 1.0, true, now, now);
        assertEquals(getSumMetric("name", 1.0, true, now, now,
            Collections.singletonMap("tag", "value"), false), sum.builder().build());
    }

    @Test
    public void testDeltaSum() {
        SinglePointMetric sum = SinglePointMetric.deltaSum(metricKey, 1.0, true, now, now);
        assertEquals(getSumMetric("name", 1.0, true, now, now, Collections.emptyMap(),
            true), sum.builder().build());
    }

    private Metric getDoubleGaugeMetric(String name, Double value, Instant time) {
        return getDoubleGaugeMetric(name, value, time, Collections.emptyMap());
    }

    private Metric getDoubleGaugeMetric(String name, Double value, Instant time, Map<String, String> tags) {
        NumberDataPoint.Builder point = NumberDataPoint.newBuilder()
            .setTimeUnixNano(TimeUnit.SECONDS.toNanos(time.getEpochSecond()) + time.getNano())
            .setAsDouble(value);

        point.addAllAttributes(tags.entrySet().stream().map(
            entry -> KeyValue.newBuilder()
                .setKey(entry.getKey())
                .setValue(AnyValue.newBuilder().setStringValue(entry.getValue())).build()
        )::iterator);

        Metric.Builder metric = Metric.newBuilder().setName(name);
        metric.getGaugeBuilder().addDataPoints(point);
        return metric.build();
    }

    private Metric getIntGaugeMetric(String name, int value, Instant time) {
        NumberDataPoint.Builder point = NumberDataPoint.newBuilder()
            .setTimeUnixNano(TimeUnit.SECONDS.toNanos(time.getEpochSecond()) + time.getNano())
            .setAsInt(value);

        Metric.Builder metric = Metric.newBuilder().setName(name);
        metric.getGaugeBuilder().addDataPoints(point);
        return metric.build();
    }

    private Metric getSumMetric(String name, Double value, boolean monotonic, Instant time) {
        return getSumMetric(name, value, monotonic, time, null, Collections.emptyMap(), false);
    }

    private Metric getSumMetric(String name, Double value, boolean monotonic, Instant time,
        Instant startTime, Map<String, String> tags, boolean isDelta) {
        NumberDataPoint.Builder point = NumberDataPoint.newBuilder()
            .setTimeUnixNano(TimeUnit.SECONDS.toNanos(time.getEpochSecond()) + time.getNano())
            .setAsDouble(value);

        if (startTime != null) {
            point.setStartTimeUnixNano(TimeUnit.SECONDS.toNanos(startTime.getEpochSecond()) + startTime.getNano());
        }

        point.addAllAttributes(tags.entrySet().stream().map(
            entry -> KeyValue.newBuilder()
                .setKey(entry.getKey())
                .setValue(AnyValue.newBuilder().setStringValue(entry.getValue())).build()
        )::iterator);

        Metric.Builder metric = Metric.newBuilder().setName(name);
        metric.getSumBuilder()
            .setIsMonotonic(monotonic)
            .setAggregationTemporality(isDelta ? AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA :
                AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
            .addDataPoints(point);
        return metric.build();
    }
}
