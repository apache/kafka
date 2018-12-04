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
package org.apache.kafka.streams.processor.internals.metrics;


import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Count;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.Assert.assertEquals;

public class StreamsMetricsImplTest {

    @Test(expected = NullPointerException.class)
    public void testNullMetrics() {
        new StreamsMetricsImpl(null, "");
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveNullSensor() {
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
        streamsMetrics.removeSensor(null);
    }

    @Test
    public void testRemoveSensor() {
        final String sensorName = "sensor1";
        final String taskName = "task";
        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");

        final Sensor sensor1 = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor1);

        final Sensor sensor1a = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG, sensor1);
        streamsMetrics.removeSensor(sensor1a);

        final Sensor sensor2 = streamsMetrics.addLatencyAndThroughputSensor(taskName, scope, entity, operation, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor2);

        final Sensor sensor3 = streamsMetrics.addThroughputSensor(taskName, scope, entity, operation, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor3);

        assertEquals(Collections.emptyMap(), streamsMetrics.parentSensors());
    }

    @Test
    public void testMutiLevelSensorRemoval() {
        final Metrics registry = new Metrics();
        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(registry, "");
        for (final MetricName defaultMetric : registry.metrics().keySet()) {
            registry.removeMetric(defaultMetric);
        }

        final String taskName = "taskName";
        final String operation = "operation";
        final Map<String, String> threadTags = mkMap(mkEntry("threadkey", "value"));

        final Map<String, String> taskTags = mkMap(mkEntry("taskkey", "value"));

        final Sensor parent1 = metrics.threadLevelSensor(operation, Sensor.RecordingLevel.DEBUG);
        parent1.add(new MetricName("name", "group", "description", threadTags), new Count());

        assertEquals(1, registry.metrics().size());

        final Sensor sensor1 = metrics.taskLevelSensor(taskName, operation, Sensor.RecordingLevel.DEBUG, parent1);
        sensor1.add(new MetricName("name", "group", "description", taskTags), new Count());

        assertEquals(2, registry.metrics().size());

        metrics.removeAllTaskLevelSensors(taskName);

        assertEquals(1, registry.metrics().size());

        final Sensor parent2 = metrics.threadLevelSensor(operation, Sensor.RecordingLevel.DEBUG);
        parent2.add(new MetricName("name", "group", "description", threadTags), new Count());

        assertEquals(1, registry.metrics().size());

        final Sensor sensor2 = metrics.taskLevelSensor(taskName, operation, Sensor.RecordingLevel.DEBUG, parent2);
        sensor2.add(new MetricName("name", "group", "description", taskTags), new Count());

        assertEquals(2, registry.metrics().size());

        metrics.removeAllTaskLevelSensors(taskName);

        assertEquals(1, registry.metrics().size());

        metrics.removeAllThreadLevelSensors();

        assertEquals(0, registry.metrics().size());
    }

    @Test
    public void testLatencyMetrics() {
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
        final int defaultMetrics = streamsMetrics.metrics().size();

        final String taskName = "task";
        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";

        final Sensor sensor1 = streamsMetrics.addLatencyAndThroughputSensor(taskName, scope, entity, operation, Sensor.RecordingLevel.DEBUG);

        // 2 meters and 4 non-meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        final int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
        final int otherMetricsCount = 4;
        assertEquals(defaultMetrics + meterMetricsCount * 2 + otherMetricsCount, streamsMetrics.metrics().size());

        streamsMetrics.removeSensor(sensor1);
        assertEquals(defaultMetrics, streamsMetrics.metrics().size());
    }

    @Test
    public void testThroughputMetrics() {
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), "");
        final int defaultMetrics = streamsMetrics.metrics().size();

        final String taskName = "task";
        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";

        final Sensor sensor1 = streamsMetrics.addThroughputSensor(taskName, scope, entity, operation, Sensor.RecordingLevel.DEBUG);

        final int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
        // 2 meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        assertEquals(defaultMetrics + meterMetricsCount * 2, streamsMetrics.metrics().size());

        streamsMetrics.removeSensor(sensor1);
        assertEquals(defaultMetrics, streamsMetrics.metrics().size());
    }
}
