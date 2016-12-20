/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.common.utils.SystemTime;
import org.junit.Test;

public class SensorTest {
    @Test
    public void testRecordLevelEnum() {
        Sensor.RecordLevel configLevel = Sensor.RecordLevel.INFO;
        assertTrue(Sensor.RecordLevel.INFO.shouldRecord(configLevel));
        assertFalse(Sensor.RecordLevel.DEBUG.shouldRecord(configLevel));

        configLevel = Sensor.RecordLevel.DEBUG;
        assertTrue(Sensor.RecordLevel.INFO.shouldRecord(configLevel));
        assertTrue(Sensor.RecordLevel.DEBUG.shouldRecord(configLevel));

        assertEquals(Sensor.RecordLevel.valueOf(Sensor.RecordLevel.DEBUG.toString()),
            Sensor.RecordLevel.DEBUG);
        assertEquals(Sensor.RecordLevel.valueOf(Sensor.RecordLevel.INFO.toString()),
            Sensor.RecordLevel.INFO);
    }

    @Test
    public void testShouldRecord() {
        MetricConfig debugConfig = new MetricConfig().recordLevel(Sensor.RecordLevel.DEBUG);
        MetricConfig infoConfig = new MetricConfig().recordLevel(Sensor.RecordLevel.INFO);

        Sensor infoSensor = new Sensor(null, "infoSensor", null, debugConfig, new SystemTime(),
            0, Sensor.RecordLevel.INFO);
        assertTrue(infoSensor.shouldRecord());
        infoSensor = new Sensor(null, "infoSensor", null, debugConfig, new SystemTime(),
            0, Sensor.RecordLevel.DEBUG);
        assertTrue(infoSensor.shouldRecord());

        Sensor debugSensor = new Sensor(null, "debugSensor", null, infoConfig, new SystemTime(),
            0, Sensor.RecordLevel.INFO);
        assertTrue(debugSensor.shouldRecord());
        debugSensor = new Sensor(null, "debugSensor", null, infoConfig, new SystemTime(),
            0, Sensor.RecordLevel.DEBUG);
        assertFalse(debugSensor.shouldRecord());
    }
}
