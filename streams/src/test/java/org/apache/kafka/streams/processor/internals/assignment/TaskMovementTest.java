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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_TASK_LIST;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getClientStatesMap;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.assignTaskMovements;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Test;

public class TaskMovementTest {
    private final ClientState client1 = new ClientState(1);
    private final ClientState client2 = new ClientState(1);
    private final ClientState client3 = new ClientState(1);

    private final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

    private final Map<UUID, List<TaskId>> emptyWarmupAssignment = mkMap(
        mkEntry(UUID_1, EMPTY_TASK_LIST),
        mkEntry(UUID_2, EMPTY_TASK_LIST),
        mkEntry(UUID_3, EMPTY_TASK_LIST)
    );

    @Test
    public void shouldAssignTasksToClientsAndReturnFalseWhenAllClientsCaughtUp() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, asList(TASK_0_1, TASK_1_1)),
            mkEntry(UUID_3, asList(TASK_0_2, TASK_1_2))
        );

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = new HashMap<>();
        for (final TaskId task : allTasks) {
            tasksToCaughtUpClients.put(task, mkSortedSet(UUID_1, UUID_2, UUID_3));
        }
        
        assertFalse(
            assignTaskMovements(
                balancedAssignment,
                tasksToCaughtUpClients,
                clientStates,
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas)
        );

        verifyClientStateAssignments(balancedAssignment, emptyWarmupAssignment);
    }

    @Test
    public void shouldAssignAllTasksToClientsAndReturnFalseIfNoClientsAreCaughtUp() {
        final int maxWarmupReplicas = 2;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, asList(TASK_0_1, TASK_1_1)),
            mkEntry(UUID_3, asList(TASK_0_2, TASK_1_2))
        );

        assertFalse(
            assignTaskMovements(
                balancedAssignment,
                emptyMap(),
                clientStates,
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas)
        );
        verifyClientStateAssignments(balancedAssignment, emptyWarmupAssignment);
    }

    @Test
    public void shouldMoveTasksToCaughtUpClientsAndAssignWarmupReplicasInTheirPlace() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, singletonList(TASK_0_0)),
            mkEntry(UUID_2, singletonList(TASK_0_1)),
            mkEntry(UUID_3, singletonList(TASK_0_2))
        );

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = new HashMap<>();
        tasksToCaughtUpClients.put(TASK_0_0, mkSortedSet(UUID_1));
        tasksToCaughtUpClients.put(TASK_0_1, mkSortedSet(UUID_3));
        tasksToCaughtUpClients.put(TASK_0_2, mkSortedSet(UUID_2));

        final Map<UUID, List<TaskId>> expectedActiveTaskAssignment = mkMap(
            mkEntry(UUID_1, singletonList(TASK_0_0)),
            mkEntry(UUID_2, singletonList(TASK_0_2)),
            mkEntry(UUID_3, singletonList(TASK_0_1))
        );

        final Map<UUID, List<TaskId>> expectedWarmupTaskAssignment = mkMap(
            mkEntry(UUID_1, EMPTY_TASK_LIST),
            mkEntry(UUID_2, singletonList(TASK_0_1)),
            mkEntry(UUID_3, singletonList(TASK_0_2))
        );

        assertTrue(
            assignTaskMovements(
                balancedAssignment,
                tasksToCaughtUpClients,
                clientStates,
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas)
        );
        verifyClientStateAssignments(expectedActiveTaskAssignment, expectedWarmupTaskAssignment);
    }

    @Test
    public void shouldProduceBalancedAndStateConstrainedAssignment() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, asList(TASK_0_1, TASK_1_1)),
            mkEntry(UUID_3, asList(TASK_0_2, TASK_1_2))
        );

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = new HashMap<>();
        tasksToCaughtUpClients.put(TASK_0_0, mkSortedSet(UUID_2, UUID_3));  // needs to be warmed up

        tasksToCaughtUpClients.put(TASK_0_1, mkSortedSet(UUID_1, UUID_3));  // needs to be warmed up

        tasksToCaughtUpClients.put(TASK_0_2, mkSortedSet(UUID_2));          // needs to be warmed up

        tasksToCaughtUpClients.put(TASK_1_1, mkSortedSet(UUID_1));  // needs to be warmed up

        final Map<UUID, List<TaskId>> expectedActiveTaskAssignment = mkMap(
            mkEntry(UUID_1, asList(TASK_1_0, TASK_1_1)),
            mkEntry(UUID_2, asList(TASK_0_2, TASK_0_0)),
            mkEntry(UUID_3, asList(TASK_0_1, TASK_1_2))
        );

        final Map<UUID, List<TaskId>> expectedWarmupTaskAssignment = mkMap(
            mkEntry(UUID_1, singletonList(TASK_0_0)),
            mkEntry(UUID_2, asList(TASK_0_1, TASK_1_1)),
            mkEntry(UUID_3, singletonList(TASK_0_2))
        );

        assertTrue(
            assignTaskMovements(
                balancedAssignment,
                tasksToCaughtUpClients,
                clientStates,
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas)
        );
        verifyClientStateAssignments(expectedActiveTaskAssignment, expectedWarmupTaskAssignment);
    }

    @Test
    public void shouldOnlyGetUpToMaxWarmupReplicasAndReturnTrue() {
        final int maxWarmupReplicas = 1;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, singletonList(TASK_0_0)),
            mkEntry(UUID_2, singletonList(TASK_0_1)),
            mkEntry(UUID_3, singletonList(TASK_0_2))
        );

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = new HashMap<>();
        tasksToCaughtUpClients.put(TASK_0_0, mkSortedSet(UUID_1));
        tasksToCaughtUpClients.put(TASK_0_1, mkSortedSet(UUID_3));
        tasksToCaughtUpClients.put(TASK_0_2, mkSortedSet(UUID_2));

        final Map<UUID, List<TaskId>> expectedActiveTaskAssignment = mkMap(
            mkEntry(UUID_1, singletonList(TASK_0_0)),
            mkEntry(UUID_2, singletonList(TASK_0_2)),
            mkEntry(UUID_3, singletonList(TASK_0_1))
        );

        final Map<UUID, List<TaskId>> expectedWarmupTaskAssignment = mkMap(
            mkEntry(UUID_1, EMPTY_TASK_LIST),
            mkEntry(UUID_2, singletonList(TASK_0_1)),
            mkEntry(UUID_3, EMPTY_TASK_LIST)
        );
        assertTrue(
            assignTaskMovements(
               balancedAssignment,
               tasksToCaughtUpClients,
               clientStates,
               getMapWithNumStandbys(allTasks, 1),
               maxWarmupReplicas)
        );

        verifyClientStateAssignments(expectedActiveTaskAssignment, expectedWarmupTaskAssignment);
    }

    @Test
    public void shouldNotCountPreviousStandbyTasksTowardsMaxWarmupReplicas() {
        final int maxWarmupReplicas = 1;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2);

        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, singletonList(TASK_0_0)),
            mkEntry(UUID_2, singletonList(TASK_0_1)),
            mkEntry(UUID_3, singletonList(TASK_0_2))
        );

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = new HashMap<>();
        tasksToCaughtUpClients.put(TASK_0_0, mkSortedSet(UUID_1));
        tasksToCaughtUpClients.put(TASK_0_1, mkSortedSet(UUID_3));
        tasksToCaughtUpClients.put(TASK_0_2, mkSortedSet(UUID_2));

        final Map<UUID, List<TaskId>> expectedActiveTaskAssignment = mkMap(
            mkEntry(UUID_1, singletonList(TASK_0_0)),
            mkEntry(UUID_2, singletonList(TASK_0_2)),
            mkEntry(UUID_3, singletonList(TASK_0_1))
        );

        final Map<UUID, List<TaskId>> expectedWarmupTaskAssignment = mkMap(
            mkEntry(UUID_1, EMPTY_TASK_LIST),
            mkEntry(UUID_2, singletonList(TASK_0_1)),
            mkEntry(UUID_3, singletonList(TASK_0_2))
        );

        client3.addPreviousStandbyTasks(singleton(TASK_0_2));

        assertTrue(
            assignTaskMovements(
                balancedAssignment,
                tasksToCaughtUpClients,
                clientStates,
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas)
        );

        verifyClientStateAssignments(expectedActiveTaskAssignment, expectedWarmupTaskAssignment);
    }

    private void verifyClientStateAssignments(final Map<UUID, List<TaskId>> expectedActiveTaskAssignment,
                                              final Map<UUID, List<TaskId>> expectedStandbyTaskAssignment) {
        for (final Map.Entry<UUID, ClientState> clientEntry : clientStates.entrySet()) {
            final UUID client = clientEntry.getKey();
            final ClientState state = clientEntry.getValue();
            
            assertThat(state.activeTasks(), equalTo(new HashSet<>(expectedActiveTaskAssignment.get(client))));
            assertThat(state.standbyTasks(), equalTo(new HashSet<>(expectedStandbyTaskAssignment.get(client))));
        }
    }

    private static Map<TaskId, Integer> getMapWithNumStandbys(final Set<TaskId> tasks, final int numStandbys) {
        return tasks.stream().collect(Collectors.toMap(task -> task, t -> numStandbys));
    }

}
