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

package org.apache.kafka.server.quota;

import org.apache.kafka.common.metrics.Sensor;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ClientSensorsTest {

    @Test
    void testConstructorWithValidParameters() {
        Map<String, String> metricTags = Map.of("client-id", "test-client", "user", "test-user");
        Sensor quotaSensor = mock(Sensor.class);
        Sensor throttleTimeSensor = mock(Sensor.class);

        ClientSensors clientSensors = new ClientSensors(metricTags, quotaSensor, throttleTimeSensor);

        assertEquals(metricTags, clientSensors.metricTags());
        assertEquals(quotaSensor, clientSensors.quotaSensor());
        assertEquals(throttleTimeSensor, clientSensors.throttleTimeSensor());
    }

    @Test
    void testConstructorPreservesInputOrder() {
        LinkedHashMap<String, String> orderedTags = new LinkedHashMap<>();
        orderedTags.put("first", "value1");
        orderedTags.put("second", "value2");
        orderedTags.put("third", "value3");
        orderedTags.put("fourth", "value4");
        Sensor quotaSensor = mock(Sensor.class);
        Sensor throttleTimeSensor = mock(Sensor.class);

        ClientSensors clientSensors = new ClientSensors(orderedTags, quotaSensor, throttleTimeSensor);

        Map<String, String> resultTags = clientSensors.metricTags();
        assertInstanceOf(LinkedHashMap.class, resultTags);
        
        // Convert to arrays to check order
        String[] expectedKeys = {"first", "second", "third", "fourth"};
        String[] actualKeys = resultTags.keySet().toArray(new String[0]);
        
        for (int i = 0; i < expectedKeys.length; i++) {
            assertEquals(expectedKeys[i], actualKeys[i], 
                "Key at position " + i + " should match expected order");
        }
    }

    @Test
    void testConstructorWithEmptyMap() {
        Map<String, String> emptyTags = Map.of();
        Sensor quotaSensor = mock(Sensor.class);
        Sensor throttleTimeSensor = mock(Sensor.class);

        ClientSensors clientSensors = new ClientSensors(emptyTags, quotaSensor, throttleTimeSensor);

        assertTrue(clientSensors.metricTags().isEmpty());
        assertEquals(quotaSensor, clientSensors.quotaSensor());
        assertEquals(throttleTimeSensor, clientSensors.throttleTimeSensor());
    }

    @Test
    void testConstructorThrowsExceptionWhenQuotaSensorIsNull() {
        assertThrows(NullPointerException.class,
            () -> new ClientSensors(Map.of("client-id", "test-client"), null, mock(Sensor.class)));
    }

    @Test
    void testConstructorThrowsExceptionWhenThrottleTimeSensorIsNull() {
        assertThrows(NullPointerException.class,
            () -> new ClientSensors(Map.of("client-id", "test-client"), mock(Sensor.class), null));
    }

    @Test
    void testConstructorThrowsExceptionWhenBothSensorsAreNull() {
        assertThrows(NullPointerException.class,
            () -> new ClientSensors(Map.of("client-id", "test-client"), null, null));
    }
}
