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

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_CLIENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getClientStatesMap;
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtils.computeTasksToRemainingStandbys;
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtils.hasClientsWithMoreAvailableCapacity;
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtils.pollClientAndMaybeAssignRemainingStandbyTasks;
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtils.shouldBalanceLoad;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TaskAssignmentUtilsTest {
    private static final Set<TaskId> ACTIVE_TASKS = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

    private Map<UUID, ClientState> clients;
    private ConstrainedPrioritySet clientsByTaskLoad;

    @Before
    public void setup() {
        clients = getClientStatesMap(ACTIVE_TASKS.stream().map(TaskAssignmentUtilsTest::mkState).toArray(ClientState[]::new));
        clientsByTaskLoad = new ConstrainedPrioritySet(
            (client, task) -> !clients.get(client).hasAssignedTask(task),
            client -> clients.get(client).assignedTaskLoad()
        );
        clientsByTaskLoad.offerAll(clients.keySet());
    }

    @Test
    public void shouldReturnCorrectBalanceLoadResult() {
        final ClientState client1 = clients.get(UUID_1);

        final ClientState client4 = mkState(2, TASK_1_0);
        clients.put(UUID_4, client4);
        assertTrue(shouldBalanceLoad(clients.values(), client1));

        client4.assignActive(TASK_1_1);
        client4.assignStandby(TASK_0_0);
        assertFalse(shouldBalanceLoad(clients.values(), client1));

        client4.unassignActive(TASK_1_1);
        client4.unassignStandby(TASK_0_0);
        final ClientState client5 = mkState(1);
        clients.put(UUID_5, client5);
        assertFalse(shouldBalanceLoad(clients.values(), client5));
    }

    @Test
    public void shouldReturnHasClientWithMoreAvailableCapacityResultCorrectly() {
        final ClientState client = clients.get(UUID_1);
        assertFalse(hasClientsWithMoreAvailableCapacity(clients.values(), client));

        client.assignActive(TASK_0_3);
        assertTrue(hasClientsWithMoreAvailableCapacity(clients.values(), client));
    }

    @Test
    public void shouldReturnNumberOfStandbyTasksThatWereNotAssigned() {
        final Map<TaskId, Integer> tasksToRemainingStandbys = computeTasksToRemainingStandbys(3, ACTIVE_TASKS);

        assertTrue(tasksToRemainingStandbys.keySet()
                                           .stream()
                                           .map(taskId -> pollClientAndMaybeAssignRemainingStandbyTasks(
                                               clients,
                                               tasksToRemainingStandbys,
                                               clientsByTaskLoad,
                                               taskId
                                           ))
                                           .allMatch(numRemainingStandbys -> numRemainingStandbys == 1));

        assertTrue(areStandbyTasksPresentForAllActiveTasks(2));
    }

    @Test
    public void shouldReturnZeroWhenAllStandbyTasksWereSuccessfullyAssigned() {
        final Map<TaskId, Integer> tasksToRemainingStandbys = computeTasksToRemainingStandbys(1, ACTIVE_TASKS);

        assertTrue(tasksToRemainingStandbys.keySet()
                                           .stream()
                                           .map(taskId -> pollClientAndMaybeAssignRemainingStandbyTasks(
                                               clients,
                                               tasksToRemainingStandbys,
                                               clientsByTaskLoad,
                                               taskId
                                           ))
                                           .allMatch(numRemainingStandbys -> numRemainingStandbys == 0));

        assertTrue(areStandbyTasksPresentForAllActiveTasks(1));
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
        final ClientState clientState = new ClientState(capacity, EMPTY_CLIENT_TAGS);
        for (final TaskId activeTask : activeTasks) {
            clientState.assignActive(activeTask);
        }
        return clientState;
    }
}