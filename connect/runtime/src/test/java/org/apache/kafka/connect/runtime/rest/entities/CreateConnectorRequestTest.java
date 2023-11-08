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
package org.apache.kafka.connect.runtime.rest.entities;

import org.apache.kafka.connect.runtime.TargetState;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CreateConnectorRequestTest {

    @Test
    public void testToTargetState() {
        assertEquals(TargetState.STARTED, CreateConnectorRequest.InitialState.RUNNING.toTargetState());
        assertEquals(TargetState.PAUSED, CreateConnectorRequest.InitialState.PAUSED.toTargetState());
        assertEquals(TargetState.STOPPED, CreateConnectorRequest.InitialState.STOPPED.toTargetState());

        CreateConnectorRequest createConnectorRequest = new CreateConnectorRequest("test-name", Collections.emptyMap(), null);
        assertNull(createConnectorRequest.initialTargetState());
    }

    @Test
    public void testForValue() {
        assertEquals(CreateConnectorRequest.InitialState.RUNNING, CreateConnectorRequest.InitialState.forValue("running"));
        assertEquals(CreateConnectorRequest.InitialState.RUNNING, CreateConnectorRequest.InitialState.forValue("Running"));
        assertEquals(CreateConnectorRequest.InitialState.RUNNING, CreateConnectorRequest.InitialState.forValue("RUNNING"));

        assertEquals(CreateConnectorRequest.InitialState.PAUSED, CreateConnectorRequest.InitialState.forValue("paused"));
        assertEquals(CreateConnectorRequest.InitialState.PAUSED, CreateConnectorRequest.InitialState.forValue("Paused"));
        assertEquals(CreateConnectorRequest.InitialState.PAUSED, CreateConnectorRequest.InitialState.forValue("PAUSED"));

        assertEquals(CreateConnectorRequest.InitialState.STOPPED, CreateConnectorRequest.InitialState.forValue("stopped"));
        assertEquals(CreateConnectorRequest.InitialState.STOPPED, CreateConnectorRequest.InitialState.forValue("Stopped"));
        assertEquals(CreateConnectorRequest.InitialState.STOPPED, CreateConnectorRequest.InitialState.forValue("STOPPED"));
    }
}
