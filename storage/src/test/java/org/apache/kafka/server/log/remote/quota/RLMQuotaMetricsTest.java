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

package org.apache.kafka.server.log.remote.quota;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RLMQuotaMetricsTest {
    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(new MetricConfig(), List.of(), time);

    @Test
    public void testNewSensorWhenExpired() {
        RLMQuotaMetrics rlmQuotaMetrics = new RLMQuotaMetrics(metrics, "metric", "group", "format", 5);
        Sensor sensor = rlmQuotaMetrics.sensor();
        Sensor sensorRepeat = rlmQuotaMetrics.sensor();

        // If the sensor has not expired we should reuse it.
        assertEquals(sensorRepeat, sensor);

        // The ExpireSensorTask calls removeSensor to remove expired sensors.
        metrics.removeSensor(sensor.name());

        // If the sensor has been removed, we should get a new one.
        Sensor newSensor = rlmQuotaMetrics.sensor();
        assertNotEquals(sensor, newSensor);
    }

    @Test
    public void testClose() {
        RLMQuotaMetrics quotaMetrics = new RLMQuotaMetrics(metrics, "metric", "group", "this is %s", 5);

        // Register the sensor
        quotaMetrics.sensor();
        var avg = metrics.metricName("metric" + "-avg", "group", "this is average");

        // Verify that metrics are created
        var result = metrics.metric(avg);
        assertNotNull(result);
        assertEquals(result.metricName().description(), avg.description());

        // Close the quotaMetrics instance
        quotaMetrics.close();

        // After closing, the metrics should be removed
        assertNull(metrics.metric(avg));
    }
}
