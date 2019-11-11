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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.WindowedSum;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SensorAddingMetricsTest {

    private Metrics metrics;
    private MetricName metricName;
    private Sensor sensor;

    @Before
    public void setUp() {
        metrics = new Metrics();
        metricName = metrics.metricName("test-metric", "test-group");
        sensor = sensor(new Avg());
    }

    private Sensor sensor(MeasurableStat stat) {
        final Sensor sensor = metrics.sensor("sensor");
        sensor.add(metricName, stat);
        return sensor;
    }

    @Test
    public void itIsAllowedToAddSameMetricToSensor() {
        assertTrue(sensor.add(metricName, new Avg()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsException_whenAddedSameMetricToAnotherSensor() {
        metrics.sensor("another-sensor").add(metricName, new Avg());
    }

    @Test
    public void metricDoesNotChange_onceAdded() {
        sensor.add(metricName, new WindowedSum());

        assertEquals(1, sensor.metrics().size());
        assertEquals(Avg.class, sensor.metrics().get(0).measurable().getClass());
    }
}
