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
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_TASKS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.getMovements;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.streams.processor.TaskId;
import org.easymock.EasyMock;
import org.junit.Test;

public class TaskMovementTest {

    private final ClientState client1 = EasyMock.createMock(ClientState.class);
    private final ClientState client2 = EasyMock.createMock(ClientState.class);
    private final ClientState client3 = EasyMock.createMock(ClientState.class);

    @Test
    public void shouldGetMovementsFromStateConstrainedToBalancedAssignment() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_1_2)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1, TASK_1_0)),
            mkEntry(UUID_3, mkTaskList(TASK_0_2, TASK_1_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1, TASK_1_1)),
            mkEntry(UUID_3, mkTaskList(TASK_0_2, TASK_1_2))
        );
        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = getMapWithNoCaughtUpClients(
            mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2)
        );

        expectNoPreviousStandbys(client1, client2, client3);

        final Queue<TaskMovement> expectedMovements = new LinkedList<>();
        expectedMovements.add(new TaskMovement(TASK_1_2, UUID_1, UUID_3));
        expectedMovements.add(new TaskMovement(TASK_1_0, UUID_2, UUID_1));
        expectedMovements.add(new TaskMovement(TASK_1_1, UUID_3, UUID_2));

        assertThat(
            getMovements(
                stateConstrainedAssignment,
                balancedAssignment,
                tasksToCaughtUpClients,
                getClientStatesWithThreeClients(),
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas),
            equalTo(expectedMovements));
    }

    @Test
    public void shouldImmediatelyMoveTasksWithCaughtUpDestinationClients() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_1_2)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1, TASK_1_0)),
            mkEntry(UUID_3, mkTaskList(TASK_0_2, TASK_1_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1, TASK_1_1)),
            mkEntry(UUID_3, mkTaskList(TASK_0_2, TASK_1_2))
        );

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = getMapWithNoCaughtUpClients(allTasks);
        tasksToCaughtUpClients.get(TASK_1_0).add(UUID_1);

        expectNoPreviousStandbys(client1, client2, client3);

        final Queue<TaskMovement> expectedMovements = new LinkedList<>();
        expectedMovements.add(new TaskMovement(TASK_1_2, UUID_1, UUID_3));
        expectedMovements.add(new TaskMovement(TASK_1_1, UUID_3, UUID_2));


        assertThat(
            getMovements(
                stateConstrainedAssignment,
                balancedAssignment,
                tasksToCaughtUpClients,
                getClientStatesWithThreeClients(),
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas),
            equalTo(expectedMovements));

        assertFalse(stateConstrainedAssignment.get(UUID_2).contains(TASK_1_0));
        assertTrue(stateConstrainedAssignment.get(UUID_1).contains(TASK_1_0));
    }

    @Test
    public void shouldOnlyGetUpToMaxWarmupReplicaMovements() {
        final int maxWarmupReplicas = 1;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_1_2)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1, TASK_1_0)),
            mkEntry(UUID_3, mkTaskList(TASK_0_2, TASK_1_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1, TASK_1_1)),
            mkEntry(UUID_3, mkTaskList(TASK_0_2, TASK_1_2))
        );
        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = getMapWithNoCaughtUpClients(allTasks);

        expectNoPreviousStandbys(client1, client2, client3);

        final Queue<TaskMovement> expectedMovements = new LinkedList<>();
        expectedMovements.add(new TaskMovement(TASK_1_2, UUID_1, UUID_3));

        assertThat(
            getMovements(
                stateConstrainedAssignment,
                balancedAssignment,
                tasksToCaughtUpClients,
                getClientStatesWithThreeClients(),
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas),
            equalTo(expectedMovements));
    }

    @Test
    public void shouldNotCountPreviousStandbyTasksTowardsMaxWarmupReplicas() {
        final int maxWarmupReplicas = 1;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_1_2)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1, TASK_1_0)),
            mkEntry(UUID_3, mkTaskList(TASK_0_2, TASK_1_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1, TASK_1_1)),
            mkEntry(UUID_3, mkTaskList(TASK_0_2, TASK_1_2))
        );

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = getMapWithNoCaughtUpClients(allTasks);

        expectNoPreviousStandbys(client1, client2);
        expect(client3.prevStandbyTasks()).andStubReturn(singleton(TASK_1_2));
        replay(client3);

        final Queue<TaskMovement> expectedMovements = new LinkedList<>();
        expectedMovements.add(new TaskMovement(TASK_1_2, UUID_1, UUID_3));
        expectedMovements.add(new TaskMovement(TASK_1_0, UUID_2, UUID_1));

        assertThat(
            getMovements(
                stateConstrainedAssignment,
                balancedAssignment,
                tasksToCaughtUpClients,
                getClientStatesWithThreeClients(),
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas),
            equalTo(expectedMovements));
    }

    @Test
    public void shouldReturnEmptyMovementsWhenPassedEmptyTaskAssignments() {
        final int maxWarmupReplicas = 2;
        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, emptyList()),
            mkEntry(UUID_2, emptyList())
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, emptyList()),
            mkEntry(UUID_2, emptyList())
        );

        assertTrue(
            getMovements(
                stateConstrainedAssignment,
                balancedAssignment,
                emptyMap(),
                getClientStatesWithTwoClients(),
                emptyMap(),
                maxWarmupReplicas
            ).isEmpty());
    }

    @Test
    public void shouldReturnEmptyMovementsWhenPassedIdenticalTaskAssignments() {
        final int maxWarmupReplicas = 2;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);

        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1, TASK_1_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1, TASK_1_1))
        );

        assertTrue(
            getMovements(
                stateConstrainedAssignment,
                balancedAssignment,
                getMapWithNoCaughtUpClients(allTasks),
                getClientStatesWithTwoClients(),
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas
            ).isEmpty());
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfAssignmentsAreOfDifferentSize() {
        final int maxWarmupReplicas = 2;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);

        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_0_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_1_0)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1, TASK_1_1))
        );

        assertThrows(
            IllegalStateException.class,
            () -> getMovements(
                stateConstrainedAssignment,
                balancedAssignment,
                getMapWithNoCaughtUpClients(allTasks),
                getClientStatesWithTwoClients(),
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas)
        );
    }

    @Test
    public void shouldThrowIllegalStateExceptionWhenTaskHasNoDestinationClient() {
        final int maxWarmupReplicas = 2;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_1_0);

        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0, TASK_0_1)),
            mkEntry(UUID_2, mkTaskList(TASK_1_0))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(UUID_1, mkTaskList(TASK_0_0)),
            mkEntry(UUID_2, mkTaskList(TASK_0_1))
        );
        expectNoPreviousStandbys(client1, client2);

        assertThrows(
            IllegalStateException.class,
            () -> getMovements(
                stateConstrainedAssignment,
                balancedAssignment,
                getMapWithNoCaughtUpClients(allTasks),
                getClientStatesWithTwoClients(),
                getMapWithNumStandbys(allTasks, 1),
                maxWarmupReplicas)
        );
    }

    private static void expectNoPreviousStandbys(final ClientState... states) {
        for (final ClientState state : states) {
            expect(state.prevStandbyTasks()).andStubReturn(EMPTY_TASKS);
            replay(state);
        }
    }

    private static Map<TaskId, Integer> getMapWithNumStandbys(final Set<TaskId> tasks, final int numStandbys) {
        return tasks.stream().collect(Collectors.toMap(task -> task, t -> numStandbys));
    }

    private Map<UUID, ClientState> getClientStatesWithTwoClients() {
        return mkMap(
            mkEntry(UUID_1, client1),
            mkEntry(UUID_2, client2)
        );
    }

    private Map<UUID, ClientState> getClientStatesWithThreeClients() {
        return mkMap(
            mkEntry(UUID_1, client1),
            mkEntry(UUID_2, client2),
            mkEntry(UUID_3, client3)
        );
    }

    private static List<TaskId> mkTaskList(final TaskId... tasks) {
        return new ArrayList<>(asList(tasks));
    }

    private static Map<TaskId, SortedSet<UUID>> getMapWithNoCaughtUpClients(final Set<TaskId> tasks) {
        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = new HashMap<>();
        for (final TaskId task : tasks) {
            tasksToCaughtUpClients.put(task, new TreeSet<>());
        }
        return tasksToCaughtUpClients;
    }
}
