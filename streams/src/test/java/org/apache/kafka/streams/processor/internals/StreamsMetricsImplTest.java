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
package org.apache.kafka.streams.processor.internals;


import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StreamsMetricsImplTest {

    @Test(expected = NullPointerException.class)
    public void testNullMetrics() {
        final String groupName = "doesNotMatter";
        final Map<String, String> tags = new HashMap<>();
        new StreamsMetricsImpl(null, groupName, tags);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveNullSensor() {
        final String groupName = "doesNotMatter";
        final Map<String, String> tags = new HashMap<>();
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), groupName, tags);
        streamsMetrics.removeSensor(null);
    }

    @Test
    public void testRemoveSensor() {
        final String groupName = "doesNotMatter";
        final String sensorName = "sensor1";
        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";
        final Map<String, String> tags = new HashMap<>();
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), groupName, tags);

        final Sensor sensor1 = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor1);

        final Sensor sensor1a = streamsMetrics.addSensor(sensorName, Sensor.RecordingLevel.DEBUG, sensor1);
        streamsMetrics.removeSensor(sensor1a);

        final Sensor sensor2 = streamsMetrics.addLatencyAndThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor2);

        final Sensor sensor3 = streamsMetrics.addThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);
        streamsMetrics.removeSensor(sensor3);
    }

    @Test
    public void testLatencyMetrics() {
        final String groupName = "doesNotMatter";
        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";
        final Map<String, String> tags = new HashMap<>();
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), groupName, tags);

        final Sensor sensor1 = streamsMetrics.addLatencyAndThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);

        Map<MetricName, ? extends Metric> metrics = streamsMetrics.metrics();
        // 2 meters and 4 non-meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        final int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
        final int otherMetricsCount = 4;
        assertEquals(meterMetricsCount * 2 + otherMetricsCount + 1, metrics.size());

        streamsMetrics.removeSensor(sensor1);
        metrics = streamsMetrics.metrics();
        assertEquals(metrics.size(), 1);
    }

    @Test
    public void testThroughputMetrics() {
        final String groupName = "doesNotMatter";
        final String scope = "scope";
        final String entity = "entity";
        final String operation = "put";
        final Map<String, String> tags = new HashMap<>();
        final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(new Metrics(), groupName, tags);

        final Sensor sensor1 = streamsMetrics.addThroughputSensor(scope, entity, operation, Sensor.RecordingLevel.DEBUG);

        Map<MetricName, ? extends Metric> metrics = streamsMetrics.metrics();
        final int meterMetricsCount = 2; // Each Meter is a combination of a Rate and a Total
        // 2 meter metrics plus a common metric that keeps track of total registered metrics in Metrics() constructor
        assertEquals(meterMetricsCount * 2 + 1, metrics.size());

        streamsMetrics.removeSensor(sensor1);
        metrics = streamsMetrics.metrics();
        assertEquals(metrics.size(), 1);
    }
}
