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

import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.TaskState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RestartPlanTest {
    private static final String CONNECTOR_NAME = "foo";

    @Test
    public void testRestartPlan() {
        ConnectorStateInfo.ConnectorState state = new ConnectorStateInfo.ConnectorState(
                AbstractStatus.State.RESTARTING.name(),
                "foo",
                null
        );
        List<TaskState> tasks = new ArrayList<>();
        tasks.add(new TaskState(1, AbstractStatus.State.RUNNING.name(), "worker1", null));
        tasks.add(new TaskState(2, AbstractStatus.State.PAUSED.name(), "worker1", null));
        tasks.add(new TaskState(3, AbstractStatus.State.RESTARTING.name(), "worker1", null));
        tasks.add(new TaskState(4, AbstractStatus.State.DESTROYED.name(), "worker1", null));
        tasks.add(new TaskState(5, AbstractStatus.State.RUNNING.name(), "worker1", null));
        tasks.add(new TaskState(6, AbstractStatus.State.RUNNING.name(), "worker1", null));
        ConnectorStateInfo connectorStateInfo = new ConnectorStateInfo(CONNECTOR_NAME, state, tasks, ConnectorType.SOURCE);

        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        RestartPlan restartPlan = new RestartPlan(restartRequest, connectorStateInfo);

        assertTrue(restartPlan.shouldRestartConnector());
        assertTrue(restartPlan.shouldRestartTasks());
        assertEquals(1, restartPlan.taskIdsToRestart().size());
        assertEquals(3, restartPlan.taskIdsToRestart().iterator().next().task());
        assertTrue(restartPlan.toString().contains("plan to restart connector"));
    }

    @Test
    public void testNoRestartsPlan() {
        ConnectorStateInfo.ConnectorState state = new ConnectorStateInfo.ConnectorState(
                AbstractStatus.State.RUNNING.name(),
                "foo",
                null
        );
        List<TaskState> tasks = new ArrayList<>();
        tasks.add(new TaskState(1, AbstractStatus.State.RUNNING.name(), "worker1", null));
        tasks.add(new TaskState(2, AbstractStatus.State.PAUSED.name(), "worker1", null));
        ConnectorStateInfo connectorStateInfo = new ConnectorStateInfo(CONNECTOR_NAME, state, tasks, ConnectorType.SOURCE);
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        RestartPlan restartPlan = new RestartPlan(restartRequest, connectorStateInfo);

        assertFalse(restartPlan.shouldRestartConnector());
        assertFalse(restartPlan.shouldRestartTasks());
        assertEquals(0, restartPlan.taskIdsToRestart().size());
        assertTrue(restartPlan.toString().contains("plan to restart 0 of"));
    }

    @Test
    public void testRestartsOnlyConnector() {
        ConnectorStateInfo.ConnectorState state = new ConnectorStateInfo.ConnectorState(
                AbstractStatus.State.RESTARTING.name(),
                "foo",
                null
        );
        List<TaskState> tasks = new ArrayList<>();
        tasks.add(new TaskState(1, AbstractStatus.State.RUNNING.name(), "worker1", null));
        tasks.add(new TaskState(2, AbstractStatus.State.PAUSED.name(), "worker1", null));
        ConnectorStateInfo connectorStateInfo = new ConnectorStateInfo(CONNECTOR_NAME, state, tasks, ConnectorType.SOURCE);
        RestartRequest restartRequest = new RestartRequest(CONNECTOR_NAME, false, true);
        RestartPlan restartPlan = new RestartPlan(restartRequest, connectorStateInfo);

        assertTrue(restartPlan.shouldRestartConnector());
        assertFalse(restartPlan.shouldRestartTasks());
        assertEquals(0, restartPlan.taskIdsToRestart().size());
    }
}
