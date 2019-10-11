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
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SensorsHierarchyTest {
    private static final double DEFAULT_RECORDING_VALUE = 1.0;
    private static final double EPS = 0.000001;

    private MockTime time = new MockTime();
    private Metrics metrics;

    @Before
    public void setup() {
        metrics = new Metrics(new MetricConfig(), singletonList(new JmxReporter()), time, true);
    }

    @After
    public void tearDown() {
        this.metrics.close();
    }

    private Sensor sensor(String sensorName, MetricName name, Sensor... parents) {
        Sensor sensor = metrics.sensor(sensorName, parents);
        sensor.add(name, new WindowedCount());
        return sensor;
    }

    @Test
    public void testSensorsRecordingsPropagatesToParent() {
        Sensor parent1 = sensor("test.parent1", metrics.metricName("test.parent1.count", "grp1"));
        Sensor parent2 = sensor("test.parent2", metrics.metricName("test.parent2.count", "grp1"));
        Sensor child1 = sensor("test.child1", metrics.metricName("test.child1.count", "grp1"), parent1, parent2);
        Sensor child2 = sensor("test.child2", metrics.metricName("test.child2.count", "grp1"), parent1);
        Sensor grandchild = sensor("test.grandchild", metrics.metricName("test.grandchild.count", "grp1"), child1);

        /* increment each sensor one time */
        parent1.record();
        parent2.record();
        child1.record();
        child2.record();
        grandchild.record();

        /* each metric should have a count equal to one + its children's count */
        assertEquals(DEFAULT_RECORDING_VALUE, sensorValue(grandchild), EPS);
        assertEquals(DEFAULT_RECORDING_VALUE + childrenRecords(child1), sensorValue(child1), EPS);
        assertEquals(DEFAULT_RECORDING_VALUE, sensorValue(child2), EPS);
        assertEquals(DEFAULT_RECORDING_VALUE + childrenRecords(parent2), sensorValue(parent2), EPS);
        assertEquals(DEFAULT_RECORDING_VALUE + childrenRecords(parent1), sensorValue(parent1), EPS);
    }

    private double sensorValue(Sensor sensor) {
        return (double) sensor.metrics().get(0).metricValue();
    }

    private double childrenRecords(Sensor sensor) {
        return metrics.childrenSensors(sensor)
                .stream()
                .mapToDouble(this::sensorValue).sum();
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsException_whenParentsSensorsHaveTheSameGrandParent() {
        Sensor grandParent = metrics.sensor("grandParent");
        Sensor parent1 = metrics.sensor("parent1", grandParent);
        Sensor parent2 = metrics.sensor("parent2", grandParent);
        metrics.sensor("sensor", parent1, parent2);
    }

    @Test
    public void testRemoveChildSensor() {
        final Sensor parent = metrics.sensor("parent");
        final Sensor child = metrics.sensor("child", parent);

        assertEquals(singletonList(child), metrics.childrenSensors(parent));

        metrics.removeSensor("child");

        assertEquals(emptyList(), metrics.childrenSensors(parent));
    }

    @Test
    public void testRemoveSensor() {
        int size = metrics.metrics().size();
        Sensor parent1 = sensor("test.parent1", metrics.metricName("test.parent1.count", "grp1"));
        Sensor parent2 = sensor("test.parent2", metrics.metricName("test.parent2.count", "grp1"));
        sensor("test.child1", metrics.metricName("test.child1.count", "grp1"), parent1, parent2);
        Sensor child2 = sensor("test.child2", metrics.metricName("test.child2.count", "grp1"), parent2);
        sensor("test.gchild2", metrics.metricName("test.gchild2.count", "grp1"), child2);

        metrics.removeSensor("test.parent1");

        assertNull(metrics.getSensor("test.parent1"));
        assertNull(metrics.metric(metrics.metricName("test.parent1.count", "grp1")));
        assertNull(metrics.getSensor("test.child1"));
        assertNull(metrics.metric(metrics.metricName("test.child1.count", "grp1")));

        Sensor sensor = metrics.getSensor("test.gchild2");
        assertNotNull(sensor);
        metrics.removeSensor("test.gchild2");

        assertNull(metrics.getSensor("test.gchild2"));
        assertNull(metrics.metric(metrics.metricName("test.gchild2.count", "grp1")));

        sensor = metrics.getSensor("test.child2");
        assertNotNull(sensor);
        metrics.removeSensor("test.child2");

        assertNull(metrics.getSensor("test.child2"));
        assertNull(metrics.metric(metrics.metricName("test.child2.count", "grp1")));

        sensor = metrics.getSensor("test.parent2");
        assertNotNull(sensor);
        metrics.removeSensor("test.parent2");

        assertNull(metrics.getSensor("test.parent2"));
        assertNull(metrics.metric(metrics.metricName("test.parent2.count", "grp1")));

        assertEquals(size, metrics.metrics().size());
    }
}
