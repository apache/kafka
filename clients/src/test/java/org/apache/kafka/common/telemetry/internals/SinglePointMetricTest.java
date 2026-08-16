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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SinglePointMetricTest {

    private MetricKey metricKey;
    private Instant now;

    /*
     Test compares the metric representation from the constructed SinglePointMetric to ensure that the
     metric is constructed correctly.

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
        SinglePointMetric gaugeNumber = SinglePointMetric.gauge(metricKey, Long.valueOf(1), now, Collections.emptySet());
        assertEquals("name", gaugeNumber.key().name());

        assertTrue(gaugeNumber.hasGauge());
        assertEquals(1, gaugeNumber.dataPointsCount());
        assertEquals(now.getEpochSecond() * Math.pow(10, 9) + now.getNano(), gaugeNumber.timeUnixNano());
        assertEquals(0, gaugeNumber.startTimeUnixNano());
        assertEquals(1, gaugeNumber.longValue());
        assertEquals(0, gaugeNumber.attributesCount());
    }

    @Test
    public void testGaugeWithDoubleValue() {
        SinglePointMetric gaugeNumber = SinglePointMetric.gauge(metricKey, 1.0, now, Collections.emptySet());
        assertEquals("name", gaugeNumber.key().name());

        assertTrue(gaugeNumber.hasGauge());
        assertEquals(1, gaugeNumber.dataPointsCount());
        assertEquals(now.getEpochSecond() * Math.pow(10, 9) + now.getNano(), gaugeNumber.timeUnixNano());
        assertEquals(0, gaugeNumber.startTimeUnixNano());
        assertEquals(1.0, gaugeNumber.doubleValue());
        assertEquals(0, gaugeNumber.attributesCount());
    }

    @Test
    public void testGaugeWithMetricTags() {
        MetricKey metricKey = new MetricKey("name", Collections.singletonMap("tag", "value"));
        SinglePointMetric gaugeNumber = SinglePointMetric.gauge(metricKey, 1.0, now, Collections.emptySet());
        assertEquals("name", gaugeNumber.key().name());

        assertTrue(gaugeNumber.hasGauge());
        assertEquals(1, gaugeNumber.dataPointsCount());
        assertEquals(now.getEpochSecond() * Math.pow(10, 9) + now.getNano(), gaugeNumber.timeUnixNano());
        assertEquals(0, gaugeNumber.startTimeUnixNano());
        assertEquals(1.0, gaugeNumber.doubleValue());
        assertEquals(1, gaugeNumber.attributesCount());
        assertEquals(Collections.singletonMap("tag", "value"), gaugeNumber.attributes());
    }

    @Test
    public void testGaugeNumberWithExcludeLabels() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        tags.put("tag2", "value2");
        MetricKey metricKey = new MetricKey("name", tags);

        SinglePointMetric gaugeNumber = SinglePointMetric.gauge(metricKey, Long.valueOf(1), now, Collections.singleton("random"));
        assertEquals(1, gaugeNumber.dataPointsCount());
        assertEquals(2, gaugeNumber.attributesCount());
        assertEquals(Map.of("tag1", "value1", "tag2", "value2"), gaugeNumber.attributes());

        gaugeNumber = SinglePointMetric.gauge(metricKey, Long.valueOf(1), now, Collections.singleton("tag1"));
        assertEquals(1, gaugeNumber.dataPointsCount());
        assertEquals(Collections.singletonMap("tag2", "value2"), gaugeNumber.attributes());

        gaugeNumber = SinglePointMetric.gauge(metricKey, Long.valueOf(1), now, tags.keySet());
        assertEquals(1, gaugeNumber.dataPointsCount());
        assertEquals(0, gaugeNumber.attributesCount());
    }

    @Test
    public void testGaugeDoubleWithExcludeLabels() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        tags.put("tag2", "value2");
        MetricKey metricKey = new MetricKey("name", tags);

        SinglePointMetric gaugeNumber = SinglePointMetric.gauge(metricKey, 1.0, now, Collections.singleton("random"));
        assertEquals(1, gaugeNumber.dataPointsCount());
        assertEquals(2, gaugeNumber.attributesCount());
        assertEquals(Map.of("tag1", "value1", "tag2", "value2"), gaugeNumber.attributes());

        gaugeNumber = SinglePointMetric.gauge(metricKey, 1.0, now, Collections.singleton("tag1"));
        assertEquals(1, gaugeNumber.dataPointsCount());
        assertEquals(Collections.singletonMap("tag2", "value2"), gaugeNumber.attributes());

        gaugeNumber = SinglePointMetric.gauge(metricKey, 1.0, now, tags.keySet());
        assertEquals(1, gaugeNumber.dataPointsCount());
        assertEquals(0, gaugeNumber.attributesCount());
    }

    @Test
    public void testSum() {
        SinglePointMetric sum = SinglePointMetric.sum(metricKey, 1.0, false, now, null, Collections.emptySet());
        assertEquals("name", sum.key().name());

        assertTrue(sum.hasSum());
        assertFalse(sum.isMonotonic());
        assertFalse(sum.isDeltaTemporality());
        assertEquals(1, sum.dataPointsCount());
        assertEquals(now.getEpochSecond() * Math.pow(10, 9) + now.getNano(), sum.timeUnixNano());
        assertEquals(0, sum.startTimeUnixNano());
        assertEquals(1.0, sum.doubleValue());
        assertEquals(0, sum.attributesCount());
    }

    @Test
    public void testSumWithStartTimeAndTags() {
        MetricKey metricKey = new MetricKey("name", Collections.singletonMap("tag", "value"));
        SinglePointMetric sum = SinglePointMetric.sum(metricKey, 1.0, true, now, now, Collections.emptySet());
        assertEquals("name", sum.key().name());

        assertTrue(sum.hasSum());
        assertTrue(sum.isMonotonic());
        assertFalse(sum.isDeltaTemporality());
        assertEquals(1, sum.dataPointsCount());
        assertEquals(now.getEpochSecond() * Math.pow(10, 9) + now.getNano(), sum.timeUnixNano());
        assertEquals(now.getEpochSecond() * Math.pow(10, 9) + now.getNano(), sum.startTimeUnixNano());
        assertEquals(1.0, sum.doubleValue());
        assertEquals(1, sum.attributesCount());
        assertEquals(Collections.singletonMap("tag", "value"), sum.attributes());
    }

    @Test
    public void testSumWithExcludeLabels() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        tags.put("tag2", "value2");
        MetricKey metricKey = new MetricKey("name", tags);

        SinglePointMetric sum = SinglePointMetric.sum(metricKey, 1.0, true, now, Collections.singleton("random"));
        assertEquals(1, sum.dataPointsCount());
        assertEquals(2, sum.attributesCount());
        assertEquals(Map.of("tag1", "value1", "tag2", "value2"), sum.attributes());

        sum = SinglePointMetric.sum(metricKey, 1.0, true, now, Collections.singleton("tag1"));
        assertEquals(1, sum.dataPointsCount());
        assertEquals(Collections.singletonMap("tag2", "value2"), sum.attributes());

        sum = SinglePointMetric.sum(metricKey, 1.0, true, now, tags.keySet());
        assertEquals(1, sum.dataPointsCount());
        assertEquals(0, sum.attributesCount());
    }

    @Test
    public void testDeltaSum() {
        SinglePointMetric sum = SinglePointMetric.deltaSum(metricKey, 1.0, true, now, now, Collections.emptySet());
        assertEquals("name", sum.key().name());

        assertTrue(sum.hasSum());
        assertTrue(sum.isMonotonic());
        assertTrue(sum.isDeltaTemporality());
        assertEquals(1, sum.dataPointsCount());
        assertEquals(now.getEpochSecond() * Math.pow(10, 9) + now.getNano(), sum.timeUnixNano());
        assertEquals(now.getEpochSecond() * Math.pow(10, 9) + now.getNano(), sum.startTimeUnixNano());
        assertEquals(1.0, sum.doubleValue());
        assertEquals(0, sum.attributesCount());
    }

    @Test
    public void testDeltaSumWithExcludeLabels() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        tags.put("tag2", "value2");
        MetricKey metricKey = new MetricKey("name", tags);

        SinglePointMetric sum = SinglePointMetric.deltaSum(metricKey, 1.0, true, now, now, Collections.singleton("random"));
        assertEquals(1, sum.dataPointsCount());
        assertEquals(2, sum.attributesCount());
        assertEquals(Map.of("tag1", "value1", "tag2", "value2"), sum.attributes());

        sum = SinglePointMetric.deltaSum(metricKey, 1.0, true, now, now, Collections.singleton("tag1"));
        assertEquals(1, sum.dataPointsCount());
        assertEquals(Collections.singletonMap("tag2", "value2"), sum.attributes());

        sum = SinglePointMetric.deltaSum(metricKey, 1.0, true, now, now, tags.keySet());
        assertEquals(1, sum.dataPointsCount());
        assertEquals(0, sum.attributesCount());
    }
}
