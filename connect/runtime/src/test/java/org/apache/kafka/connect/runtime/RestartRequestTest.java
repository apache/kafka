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
package org.apache.kafka.connect.runtime;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RestartRequestTest {
    private static final String CONNECTOR_NAME = "foo";

    @Test
    public void forciblyRestartConnectorOnly() {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, false);
        assertTrue(restartRequest.forciblyRestartConnectorOnly());
        restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        assertFalse(restartRequest.forciblyRestartConnectorOnly());
        restartRequest = new RestartRequest(CONNECTOR_NAME, true, false);
        assertFalse(restartRequest.forciblyRestartConnectorOnly());
        restartRequest = new RestartRequest(CONNECTOR_NAME, true, true);
        assertFalse(restartRequest.forciblyRestartConnectorOnly());
    }

    @Test
    public void restartOnlyFailedConnector() {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, true, false);
        assertTrue(restartRequest.includeConnector(createConnectorStatus(AbstractStatus.State.FAILED)));
        assertFalse(restartRequest.includeConnector(createConnectorStatus(AbstractStatus.State.RUNNING)));
        assertFalse(restartRequest.includeConnector(createConnectorStatus(AbstractStatus.State.PAUSED)));
    }

    @Test
    public void restartAnyStatusConnector() {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, false);
        assertTrue(restartRequest.includeConnector(createConnectorStatus(AbstractStatus.State.FAILED)));
        assertTrue(restartRequest.includeConnector(createConnectorStatus(AbstractStatus.State.RUNNING)));
        assertTrue(restartRequest.includeConnector(createConnectorStatus(AbstractStatus.State.PAUSED)));
    }

    @Test
    public void restartOnlyFailedTasks() {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, true, true);
        assertTrue(restartRequest.includeTask(createTaskStatus(AbstractStatus.State.FAILED)));
        assertFalse(restartRequest.includeTask(createTaskStatus(AbstractStatus.State.RUNNING)));
        assertFalse(restartRequest.includeTask(createTaskStatus(AbstractStatus.State.PAUSED)));
    }

    @Test
    public void restartAnyStatusTasks() {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        assertTrue(restartRequest.includeTask(createTaskStatus(AbstractStatus.State.FAILED)));
        assertTrue(restartRequest.includeTask(createTaskStatus(AbstractStatus.State.RUNNING)));
        assertTrue(restartRequest.includeTask(createTaskStatus(AbstractStatus.State.PAUSED)));
    }

    @Test
    public void doNotRestartTasks() {
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, false);
        assertFalse(restartRequest.includeTask(createTaskStatus(AbstractStatus.State.FAILED)));
        assertFalse(restartRequest.includeTask(createTaskStatus(AbstractStatus.State.RUNNING)));

        restartRequest = new RestartRequest(CONNECTOR_NAME, true, false);
        assertFalse(restartRequest.includeTask(createTaskStatus(AbstractStatus.State.FAILED)));
        assertFalse(restartRequest.includeTask(createTaskStatus(AbstractStatus.State.RUNNING)));
    }


    private TaskStatus createTaskStatus(AbstractStatus.State state) {
        return new TaskStatus(null, state, null, 0);
    }

    private ConnectorStatus createConnectorStatus(AbstractStatus.State state) {
        return new ConnectorStatus(null, state, null, 0);
    }
}
