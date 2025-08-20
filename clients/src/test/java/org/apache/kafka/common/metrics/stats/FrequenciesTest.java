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
package org.apache.kafka.common.metrics.stats;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FrequenciesTest {

    private static final double DELTA = 0.0001d;
    private MetricConfig config;
    private Time time;
    private Metrics metrics;

    @BeforeEach
    public void setup() {
        config = new MetricConfig().eventWindow(50).samples(2);
        time = new MockTime();
        metrics = new Metrics(config, Collections.singletonList(new JmxReporter()), time, true);
    }

    @AfterEach
    public void tearDown() {
        metrics.close();
    }

    @Test
    public void testFrequencyCenterValueAboveMax() {
        assertThrows(IllegalArgumentException.class,
            () -> new Frequencies(4, 1.0, 4.0, freq("1", 1.0), freq("2", 20.0)));
    }

    @Test
    public void testFrequencyCenterValueBelowMin() {
        assertThrows(IllegalArgumentException.class,
            () -> new Frequencies(4, 1.0, 4.0, freq("1", 1.0), freq("2", -20.0)));
    }

    @Test
    public void testMoreFrequencyParametersThanBuckets() {
        assertThrows(IllegalArgumentException.class,
            () -> new Frequencies(1, 1.0, 4.0, freq("1", 1.0), freq("2", -20.0)));
    }

    @Test
    public void testBooleanFrequenciesStrategy1() {
        MetricName metricTrue = metricName("true");
        MetricName metricFalse = metricName("false");
        Frequencies frequencies = Frequencies.forBooleanValues(metricFalse, metricTrue);
        final NamedMeasurable falseMetric = frequencies.stats().get(0);
        final NamedMeasurable trueMetric = frequencies.stats().get(1);

        // Record 25 "false" and 75 "true"
        for (int i = 0; i != 25; ++i) {
            frequencies.record(config, 0.0, time.milliseconds());
        }
        for (int i = 0; i != 75; ++i) {
            frequencies.record(config, 1.0, time.milliseconds());
        }
        assertEquals(0.25, falseMetric.stat().measure(config, time.milliseconds()), DELTA);
        assertEquals(0.75, trueMetric.stat().measure(config, time.milliseconds()), DELTA);
    }

    @Test
    public void testBooleanFrequenciesStrategy2() {
        MetricName metricTrue = metricName("true");
        MetricName metricFalse = metricName("false");
        Frequencies frequencies = Frequencies.forBooleanValues(metricFalse, metricTrue);
        final NamedMeasurable falseMetric = frequencies.stats().get(0);
        final NamedMeasurable trueMetric = frequencies.stats().get(1);

        // Record 40 "false" and 60 "true"
        for (int i = 0; i != 40; ++i) {
            frequencies.record(config, 0.0, time.milliseconds());
        }
        for (int i = 0; i != 60; ++i) {
            frequencies.record(config, 1.0, time.milliseconds());
        }
        assertEquals(0.40, falseMetric.stat().measure(config, time.milliseconds()), DELTA);
        assertEquals(0.60, trueMetric.stat().measure(config, time.milliseconds()), DELTA);
    }

    @Test
    public void testWithMetricsStrategy1() {
        Frequencies frequencies = new Frequencies(4, 1.0, 4.0, freq("1", 1.0),
                freq("2", 2.0), freq("3", 3.0), freq("4", 4.0));
        Sensor sensor = metrics.sensor("test", config);
        sensor.add(frequencies);

        // Record 100 events uniformly between all buckets
        for (int i = 0; i < 100; ++i) {
            frequencies.record(config, i % 4 + 1, time.milliseconds());
        }
        assertEquals(0.25, metricValue("1"), DELTA);
        assertEquals(0.25, metricValue("2"), DELTA);
        assertEquals(0.25, metricValue("3"), DELTA);
        assertEquals(0.25, metricValue("4"), DELTA);
    }

    @Test
    public void testWithMetricsStrategy2() {
        Frequencies frequencies = new Frequencies(4, 1.0, 4.0, freq("1", 1.0),
                freq("2", 2.0), freq("3", 3.0), freq("4", 4.0));
        Sensor sensor = metrics.sensor("test", config);
        sensor.add(frequencies);

        // Record 100 events half-half between 1st and 2nd buckets
        for (int i = 0; i < 100; ++i) {
            frequencies.record(config, i % 2 + 1, time.milliseconds());
        }
        assertEquals(0.50, metricValue("1"), DELTA);
        assertEquals(0.50, metricValue("2"), DELTA);
        assertEquals(0.00, metricValue("3"), DELTA);
        assertEquals(0.00, metricValue("4"), DELTA);
    }

    @Test
    public void testWithMetricsStrategy3() {
        Frequencies frequencies = new Frequencies(4, 1.0, 4.0, freq("1", 1.0),
                freq("2", 2.0), freq("3", 3.0), freq("4", 4.0));
        Sensor sensor = metrics.sensor("test", config);
        sensor.add(frequencies);

        // Record 50 events half-half between 1st and 2nd buckets
        for (int i = 0; i < 50; ++i) {
            frequencies.record(config, i % 2 + 1, time.milliseconds());
        }
        // Record 50 events to 4th bucket
        for (int i = 0; i < 50; ++i) {
            frequencies.record(config, 4.0, time.milliseconds());
        }
        assertEquals(0.25, metricValue("1"), DELTA);
        assertEquals(0.25, metricValue("2"), DELTA);
        assertEquals(0.00, metricValue("3"), DELTA);
        assertEquals(0.50, metricValue("4"), DELTA);
    }

    private MetricName metricName(String name) {
        return new MetricName(name, "group-id", "desc", Collections.emptyMap());
    }

    private Frequency freq(String name, double value) {
        return new Frequency(metricName(name), value);
    }

    private double metricValue(String name) {
        return (double) metrics.metrics().get(metricName(name)).metricValue();
    }
}
