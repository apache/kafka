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

import org.apache.kafka.common.Metric;
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

import java.util.Arrays;
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
        metrics = new Metrics(config, Arrays.asList(new JmxReporter()), time, true);
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
    public void testBooleanFrequencies() {
        MetricName metricTrue = name("true");
        MetricName metricFalse = name("false");
        Frequencies frequencies = Frequencies.forBooleanValues(metricFalse, metricTrue);
        final NamedMeasurable falseMetric = frequencies.stats().get(0);
        final NamedMeasurable trueMetric = frequencies.stats().get(1);

        // Record 2 windows worth of values
        for (int i = 0; i != 25; ++i) {
            frequencies.record(config, 0.0, time.milliseconds());
        }
        for (int i = 0; i != 75; ++i) {
            frequencies.record(config, 1.0, time.milliseconds());
        }
        assertEquals(0.25, falseMetric.stat().measure(config, time.milliseconds()), DELTA);
        assertEquals(0.75, trueMetric.stat().measure(config, time.milliseconds()), DELTA);

        // Record 2 more windows worth of values
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
    public void testUseWithMetrics() {
        MetricName name1 = name("1");
        MetricName name2 = name("2");
        MetricName name3 = name("3");
        MetricName name4 = name("4");
        Frequencies frequencies = new Frequencies(4, 1.0, 4.0,
                                                  new Frequency(name1, 1.0),
                                                  new Frequency(name2, 2.0),
                                                  new Frequency(name3, 3.0),
                                                  new Frequency(name4, 4.0));
        Sensor sensor = metrics.sensor("test", config);
        sensor.add(frequencies);
        Metric metric1 = this.metrics.metrics().get(name1);
        Metric metric2 = this.metrics.metrics().get(name2);
        Metric metric3 = this.metrics.metrics().get(name3);
        Metric metric4 = this.metrics.metrics().get(name4);

        // Record 2 windows worth of values
        for (int i = 0; i != 100; ++i) {
            frequencies.record(config, i % 4 + 1, time.milliseconds());
        }
        assertEquals(0.25, (Double) metric1.metricValue(), DELTA);
        assertEquals(0.25, (Double) metric2.metricValue(), DELTA);
        assertEquals(0.25, (Double) metric3.metricValue(), DELTA);
        assertEquals(0.25, (Double) metric4.metricValue(), DELTA);

        // Record 2 windows worth of values
        for (int i = 0; i != 100; ++i) {
            frequencies.record(config, i % 2 + 1, time.milliseconds());
        }
        assertEquals(0.50, (Double) metric1.metricValue(), DELTA);
        assertEquals(0.50, (Double) metric2.metricValue(), DELTA);
        assertEquals(0.00, (Double) metric3.metricValue(), DELTA);
        assertEquals(0.00, (Double) metric4.metricValue(), DELTA);

        // Record 1 window worth of values to overlap with the last window
        // that is half 1.0 and half 2.0
        for (int i = 0; i != 50; ++i) {
            frequencies.record(config, 4.0, time.milliseconds());
        }
        assertEquals(0.25, (Double) metric1.metricValue(), DELTA);
        assertEquals(0.25, (Double) metric2.metricValue(), DELTA);
        assertEquals(0.00, (Double) metric3.metricValue(), DELTA);
        assertEquals(0.50, (Double) metric4.metricValue(), DELTA);
    }

    protected MetricName name(String metricName) {
        return new MetricName(metricName, "group-id", "desc", Collections.<String, String>emptyMap());
    }

    protected Frequency freq(String name, double value) {
        return new Frequency(name(name), value);
    }

}
