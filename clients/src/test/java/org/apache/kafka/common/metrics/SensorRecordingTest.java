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

import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SensorRecordingTest {
    @Test
    public void caseInsensitiveLookup() {
        assertEquals(RecordingLevel.forName("DeBuG"), RecordingLevel.DEBUG);
        assertEquals(RecordingLevel.forName("InfO"), RecordingLevel.INFO);
    }

    private MetricConfig config(RecordingLevel recordingLevel) {
        return new MetricConfig().recordLevel(recordingLevel);
    }

    private Sensor sensor(RecordingLevel recordingLevel, MetricConfig config) {
        return new Sensor(null, "sensor", null, config, new SystemTime(), 0, recordingLevel);
    }

    @Test
    public void sensorWithLowerSensivityThenConfig_shouldRecord() {
        assertTrue(sensor(RecordingLevel.INFO, config(RecordingLevel.DEBUG)).shouldRecord());
    }

    @Test
    public void sensorWithSameSensivityAsInConfig_shouldRecord() {
        assertTrue(sensor(RecordingLevel.INFO, config(RecordingLevel.INFO)).shouldRecord());
        assertTrue(sensor(RecordingLevel.DEBUG, config(RecordingLevel.DEBUG)).shouldRecord());
    }

    @Test
    public void sensorWithHigherSensivityThenInConfig_shouldNotRecord() {
        assertFalse(sensor(RecordingLevel.DEBUG, config(RecordingLevel.INFO)).shouldRecord());
    }
}
