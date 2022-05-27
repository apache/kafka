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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getClientStatesMap;
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.computeTasksToRemainingStandbys;
import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.pollClientAndMaybeAssignAndUpdateRemainingStandbyTasks;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class StandbyTaskAssignmentUtilsTest {

    private static final Set<TaskId> ACTIVE_TASKS = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

    private Map<UUID, ClientState> clients;
    private ConstrainedPrioritySet clientsByTaskLoad;

    @Before
    public void setup() {

        clients = getClientStatesMap(ACTIVE_TASKS.stream().map(StandbyTaskAssignmentUtilsTest::mkState).toArray(ClientState[]::new));
        clientsByTaskLoad = new ConstrainedPrioritySet(
            (client, task) -> !clients.get(client).hasAssignedTask(task),
            client -> clients.get(client).assignedTaskLoad()
        );
        clientsByTaskLoad.offerAll(clients.keySet());
    }

    @Test
    public void shouldReturnNumberOfStandbyTasksThatWereNotAssigned() {
        final Logger logMock = mock(Logger.class);
        final int numStandbyReplicas = 3;
        final Map<TaskId, Integer> tasksToRemainingStandbys = computeTasksToRemainingStandbys(numStandbyReplicas, ACTIVE_TASKS);

        tasksToRemainingStandbys.keySet().forEach(taskId -> pollClientAndMaybeAssignAndUpdateRemainingStandbyTasks(numStandbyReplicas,
                                                                                                                   clients,
                                                                                                                   tasksToRemainingStandbys,
                                                                                                                   clientsByTaskLoad,
                                                                                                                   taskId,
                                                                                                                   logMock));

        assertTrue(ACTIVE_TASKS.stream().allMatch(activeTask -> tasksToRemainingStandbys.get(activeTask) == 1));
        assertTrue(areStandbyTasksPresentForAllActiveTasks(2));
        verify(logMock, times(ACTIVE_TASKS.size())).warn(anyString(), anyInt(), anyInt(), any());
    }

    @Test
    public void shouldReturnZeroWhenAllStandbyTasksWereSuccessfullyAssigned() {
        final Logger logMock = mock(Logger.class);
        final int numStandbyReplicas = 1;
        final Map<TaskId, Integer> tasksToRemainingStandbys = computeTasksToRemainingStandbys(numStandbyReplicas, ACTIVE_TASKS);

        tasksToRemainingStandbys.keySet().forEach(taskId -> pollClientAndMaybeAssignAndUpdateRemainingStandbyTasks(numStandbyReplicas,
                                                                                                                   clients,
                                                                                                                   tasksToRemainingStandbys,
                                                                                                                   clientsByTaskLoad,
                                                                                                                   taskId,
                                                                                                                   logMock));

        assertTrue(ACTIVE_TASKS.stream().allMatch(activeTask -> tasksToRemainingStandbys.get(activeTask) == 0));
        assertTrue(areStandbyTasksPresentForAllActiveTasks(1));
        verifyNoInteractions(logMock);
    }

    @Test
    public void shouldComputeTasksToRemainingStandbys() {
        assertThat(
            computeTasksToRemainingStandbys(0, ACTIVE_TASKS),
            equalTo(
                ACTIVE_TASKS.stream().collect(Collectors.toMap(Function.identity(), it -> 0))
            )
        );
        assertThat(
            computeTasksToRemainingStandbys(5, ACTIVE_TASKS),
            equalTo(
                ACTIVE_TASKS.stream().collect(Collectors.toMap(Function.identity(), it -> 5))
            )
        );
    }

    private boolean areStandbyTasksPresentForAllActiveTasks(final int expectedNumberOfStandbyTasks) {
        return ACTIVE_TASKS.stream().allMatch(taskId -> clients.values()
                                                               .stream()
                                                               .filter(client -> client.hasStandbyTask(taskId))
                                                               .count() == expectedNumberOfStandbyTasks);
    }

    private static ClientState mkState(final TaskId... activeTasks) {
        return mkState(1, activeTasks);
    }

    private static ClientState mkState(final int capacity, final TaskId... activeTasks) {
        final ClientState clientState = new ClientState(capacity);
        for (final TaskId activeTask : activeTasks) {
            clientState.assignActive(activeTask);
        }
        return clientState;
    }
}