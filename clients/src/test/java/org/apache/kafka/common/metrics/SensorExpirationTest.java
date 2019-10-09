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
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SensorExpirationTest {

    private static final long INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS = 60L;
    private MetricConfig config;
    private Time mockTime;
    private Meter meter;

    @Before
    public void setUp() {
        config = new MetricConfig();
        mockTime = new MockTime();
        meter = new Meter(metricName("rate"), metricName("total"));
    }

    private MetricName metricName(String name) {
        return new MetricName(name, "test", "", emptyMap());
    }

    private Metrics expirationEnabledMetrics() {
        return new Metrics(config, singletonList(new JmxReporter()), mockTime, true);
    }

    private Sensor sensorWithExpiration(Metrics metrics) {
        return new Sensor(
                metrics,
                "sensor",
                null,
                config,
                mockTime,
                INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS,
                Sensor.RecordingLevel.INFO
        );
    }

    private void waitToExpire() {
        mockTime.sleep(TimeUnit.SECONDS.toMillis(INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS + 1));
    }

    @Test
    public void testExpiredSensor() {
        try (Metrics metrics = expirationEnabledMetrics()) {
            Sensor sensor = sensorWithExpiration(metrics);

            assertAddedMetrics(metrics, sensor);

            waitToExpire();

            assertNotAddedMetrics(metrics, sensor);
        }
    }

    private void assertAddedMetrics(Metrics metrics, Sensor sensor) {
        assertTrue(sensor.add(metrics.metricName("test1", "grp1"), new Avg()));
        assertTrue(sensor.add(meter));
    }

    private void assertNotAddedMetrics(Metrics metrics, Sensor sensor) {
        assertFalse(sensor.add(metrics.metricName("test3", "grp1"), new Avg()));
        assertFalse(sensor.add(meter));
    }
}
