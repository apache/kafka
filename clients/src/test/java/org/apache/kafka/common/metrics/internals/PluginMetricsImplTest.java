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
package org.apache.kafka.common.metrics.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PluginMetricsImplTest {

    private final Map<String, String> extraTags = Collections.singletonMap("my-tag", "my-value");
    private Map<String, String> tags;
    private Metrics metrics;
    private int initialMetrics;

    @BeforeEach
    void setup() {
        metrics = new Metrics();
        initialMetrics = metrics.metrics().size();
        tags = new LinkedHashMap<>();
        tags.put("k1", "v1");
        tags.put("k2", "v2");
    }

    @Test
    void testMetricName() {
        PluginMetricsImpl pmi = new PluginMetricsImpl(metrics, tags);
        MetricName metricName = pmi.metricName("name", "description", extraTags);
        assertEquals("name", metricName.name());
        assertEquals("plugins", metricName.group());
        assertEquals("description", metricName.description());
        Map<String, String> expectedTags = new LinkedHashMap<>(tags);
        expectedTags.putAll(extraTags);
        assertEquals(expectedTags, metricName.tags());
    }

    @Test
    void testDuplicateTagName() {
        PluginMetricsImpl pmi = new PluginMetricsImpl(metrics, tags);
        assertThrows(IllegalArgumentException.class,
                () -> pmi.metricName("name", "description", Collections.singletonMap("k1", "value")));
    }

    @Test
    void testAddRemoveMetrics() {
        PluginMetricsImpl pmi = new PluginMetricsImpl(metrics, tags);
        MetricName metricName = pmi.metricName("name", "description", extraTags);
        pmi.addMetric(metricName, (Measurable) (config, now) -> 0.0);
        assertEquals(initialMetrics + 1, metrics.metrics().size());

        assertThrows(IllegalArgumentException.class, () -> pmi.addMetric(metricName, (Measurable) (config, now) -> 0.0));

        pmi.removeMetric(metricName);
        assertEquals(initialMetrics, metrics.metrics().size());

        assertThrows(IllegalArgumentException.class, () -> pmi.removeMetric(metricName));
    }

    @Test
    void testAddRemoveSensor() {
        PluginMetricsImpl pmi = new PluginMetricsImpl(metrics, tags);
        String sensorName = "my-sensor";
        MetricName metricName = pmi.metricName("name", "description", extraTags);
        Sensor sensor = pmi.addSensor(sensorName);
        assertEquals(initialMetrics, metrics.metrics().size());
        sensor.add(metricName, new Rate());
        sensor.add(metricName, new Max());
        assertEquals(initialMetrics + 1, metrics.metrics().size());

        assertThrows(IllegalArgumentException.class, () -> pmi.addSensor(sensorName));

        pmi.removeSensor(sensorName);
        assertEquals(initialMetrics, metrics.metrics().size());

        assertThrows(IllegalArgumentException.class, () -> pmi.removeSensor(sensorName));
    }

    @Test
    void testClose() throws IOException {
        PluginMetricsImpl pmi = new PluginMetricsImpl(metrics, tags);
        String sensorName = "my-sensor";
        MetricName metricName1 = pmi.metricName("name1", "description", extraTags);
        Sensor sensor = pmi.addSensor(sensorName);
        sensor.add(metricName1, new Rate());
        MetricName metricName2 = pmi.metricName("name2", "description", extraTags);
        pmi.addMetric(metricName2, (Measurable) (config, now) -> 1.0);

        assertEquals(initialMetrics + 2, metrics.metrics().size());
        pmi.close();
        assertEquals(initialMetrics, metrics.metrics().size());
    }
}
