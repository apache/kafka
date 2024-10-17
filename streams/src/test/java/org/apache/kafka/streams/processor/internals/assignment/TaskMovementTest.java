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
import org.apache.kafka.streams.processor.assignment.ProcessId;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.emptySortedSet;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getClientStatesMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.hasProperty;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.assignActiveTaskMovements;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TaskMovementTest {
    @Test
    public void shouldAssignTasksToClientsAndReturnFalseWhenAllClientsCaughtUp() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients = new HashMap<>();
        final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag = new HashMap<>();
        for (final TaskId task : allTasks) {
            tasksToCaughtUpClients.put(task, mkSortedSet(PID_1, PID_2, PID_3));
            tasksToClientByLag.put(task, mkOrderedSet(PID_1, PID_2, PID_3));
        }

        final ClientState client1 = getClientStateWithActiveAssignment(Set.of(TASK_0_0, TASK_1_0), allTasks, allTasks);
        final ClientState client2 = getClientStateWithActiveAssignment(Set.of(TASK_0_1, TASK_1_1), allTasks, allTasks);
        final ClientState client3 = getClientStateWithActiveAssignment(Set.of(TASK_0_2, TASK_1_2), allTasks, allTasks);

        assertThat(
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
                tasksToClientByLag,
                getClientStatesMap(client1, client2, client3),
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas)
            ),
            is(0)
        );
    }

    @Test
    public void shouldAssignAllTasksToClientsAndReturnFalseIfNoClientsAreCaughtUp() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final ClientState client1 = getClientStateWithActiveAssignment(Set.of(TASK_0_0, TASK_1_0), Set.of(), allTasks);
        final ClientState client2 = getClientStateWithActiveAssignment(Set.of(TASK_0_1, TASK_1_1), Set.of(), allTasks);
        final ClientState client3 = getClientStateWithActiveAssignment(Set.of(TASK_0_2, TASK_1_2), Set.of(), allTasks);

        final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients = mkMap(
            mkEntry(TASK_0_0, emptySortedSet()),
            mkEntry(TASK_0_1, emptySortedSet()),
            mkEntry(TASK_0_2, emptySortedSet()),
            mkEntry(TASK_1_0, emptySortedSet()),
            mkEntry(TASK_1_1, emptySortedSet()),
            mkEntry(TASK_1_2, emptySortedSet())
        );
        final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag = mkMap(
            mkEntry(TASK_0_0, mkOrderedSet(PID_1, PID_2, PID_3)),
            mkEntry(TASK_0_1, mkOrderedSet(PID_1, PID_2, PID_3)),
            mkEntry(TASK_0_2, mkOrderedSet(PID_1, PID_2, PID_3)),
            mkEntry(TASK_1_0, mkOrderedSet(PID_1, PID_2, PID_3)),
            mkEntry(TASK_1_1, mkOrderedSet(PID_1, PID_2, PID_3)),
            mkEntry(TASK_1_2, mkOrderedSet(PID_1, PID_2, PID_3))
        );
        assertThat(
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
                tasksToClientByLag,
                getClientStatesMap(client1, client2, client3),
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas)
            ),
            is(0)
        );
    }

    @Test
    public void shouldMoveTasksToCaughtUpClientsAndAssignWarmupReplicasInTheirPlace() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState client1 = getClientStateWithActiveAssignment(Set.of(TASK_0_0), Set.of(TASK_0_0), allTasks);
        final ClientState client2 = getClientStateWithActiveAssignment(Set.of(TASK_0_1), Set.of(TASK_0_2), allTasks);
        final ClientState client3 = getClientStateWithActiveAssignment(Set.of(TASK_0_2), Set.of(TASK_0_1), allTasks);
        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients = mkMap(
            mkEntry(TASK_0_0, mkSortedSet(PID_1)),
            mkEntry(TASK_0_1, mkSortedSet(PID_3)),
            mkEntry(TASK_0_2, mkSortedSet(PID_2))
        );
        final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag = mkMap(
            mkEntry(TASK_0_0, mkOrderedSet(PID_1, PID_2, PID_3)),
            mkEntry(TASK_0_1, mkOrderedSet(PID_3, PID_1, PID_2)),
            mkEntry(TASK_0_2, mkOrderedSet(PID_2, PID_1, PID_3))
        );

        assertThat(
            "should have assigned movements",
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
                tasksToClientByLag,
                clientStates,
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas)
            ),
            is(2)
        );
        // The active tasks have changed to the ones that each client is caught up on
        assertThat(client1, hasProperty("activeTasks", ClientState::activeTasks, Set.of(TASK_0_0)));
        assertThat(client2, hasProperty("activeTasks", ClientState::activeTasks, Set.of(TASK_0_2)));
        assertThat(client3, hasProperty("activeTasks", ClientState::activeTasks, Set.of(TASK_0_1)));

        // we assigned warmups to migrate to the input active assignment
        assertThat(client1, hasProperty("standbyTasks", ClientState::standbyTasks, Set.of()));
        assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, Set.of(TASK_0_1)));
        assertThat(client3, hasProperty("standbyTasks", ClientState::standbyTasks, Set.of(TASK_0_2)));
    }

    @Test
    public void shouldMoveTasksToMostCaughtUpClientsAndAssignWarmupReplicasInTheirPlace() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Map<TaskId, Long> client1Lags = mkMap(mkEntry(TASK_0_0, 10000L), mkEntry(TASK_0_1, 20000L), mkEntry(TASK_0_2, 30000L));
        final Map<TaskId, Long> client2Lags = mkMap(mkEntry(TASK_0_2, 10000L), mkEntry(TASK_0_0, 20000L), mkEntry(TASK_0_1, 30000L));
        final Map<TaskId, Long> client3Lags = mkMap(mkEntry(TASK_0_1, 10000L), mkEntry(TASK_0_2, 20000L), mkEntry(TASK_0_0, 30000L));

        final ClientState client1 = getClientStateWithLags(Set.of(TASK_0_0), client1Lags);
        final ClientState client2 = getClientStateWithLags(Set.of(TASK_0_1), client2Lags);
        final ClientState client3 = getClientStateWithLags(Set.of(TASK_0_2), client3Lags);
        // To test when the task is already a standby on the most caught up node
        client3.assignStandby(TASK_0_1);
        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients = mkMap(
                mkEntry(TASK_0_0, mkSortedSet()),
                mkEntry(TASK_0_1, mkSortedSet()),
                mkEntry(TASK_0_2, mkSortedSet())
        );
        final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag = mkMap(
                mkEntry(TASK_0_0, mkOrderedSet(PID_1, PID_2, PID_3)),
                mkEntry(TASK_0_1, mkOrderedSet(PID_3, PID_1, PID_2)),
                mkEntry(TASK_0_2, mkOrderedSet(PID_2, PID_3, PID_1))
        );

        assertThat(
                "should have assigned movements",
                assignActiveTaskMovements(
                        tasksToCaughtUpClients,
                        tasksToClientByLag,
                        clientStates,
                        new TreeMap<>(),
                        new AtomicInteger(maxWarmupReplicas)
                ),
                is(2)
        );
        // The active tasks have changed to the ones that each client is most caught up on
        assertThat(client1, hasProperty("activeTasks", ClientState::activeTasks, Set.of(TASK_0_0)));
        assertThat(client2, hasProperty("activeTasks", ClientState::activeTasks, Set.of(TASK_0_2)));
        assertThat(client3, hasProperty("activeTasks", ClientState::activeTasks, Set.of(TASK_0_1)));

        // we assigned warmups to migrate to the input active assignment
        assertThat(client1, hasProperty("standbyTasks", ClientState::standbyTasks, Set.of()));
        assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, Set.of(TASK_0_1)));
        assertThat(client3, hasProperty("standbyTasks", ClientState::standbyTasks, Set.of(TASK_0_2)));
    }

    @Test
    public void shouldOnlyGetUpToMaxWarmupReplicasAndReturnTrue() {
        final int maxWarmupReplicas = 1;
        final Set<TaskId> allTasks = Set.of(TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState client1 = getClientStateWithActiveAssignment(Set.of(TASK_0_0), Set.of(TASK_0_0), allTasks);
        final ClientState client2 = getClientStateWithActiveAssignment(Set.of(TASK_0_1), Set.of(TASK_0_2), allTasks);
        final ClientState client3 = getClientStateWithActiveAssignment(Set.of(TASK_0_2), Set.of(TASK_0_1), allTasks);
        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients = mkMap(
            mkEntry(TASK_0_0, mkSortedSet(PID_1)),
            mkEntry(TASK_0_1, mkSortedSet(PID_3)),
            mkEntry(TASK_0_2, mkSortedSet(PID_2))
        );
        final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag = mkMap(
            mkEntry(TASK_0_0, mkOrderedSet(PID_1, PID_2, PID_3)),
            mkEntry(TASK_0_1, mkOrderedSet(PID_3, PID_1, PID_2)),
            mkEntry(TASK_0_2, mkOrderedSet(PID_2, PID_1, PID_3))
        );

        assertThat(
            "should have assigned movements",
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
                tasksToClientByLag,
                clientStates,
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas)
            ),
            is(2)
        );
        // The active tasks have changed to the ones that each client is caught up on
        assertThat(client1, hasProperty("activeTasks", ClientState::activeTasks, Set.of(TASK_0_0)));
        assertThat(client2, hasProperty("activeTasks", ClientState::activeTasks, Set.of(TASK_0_2)));
        assertThat(client3, hasProperty("activeTasks", ClientState::activeTasks, Set.of(TASK_0_1)));

        // we should only assign one warmup, and the task movement should have the highest priority
        assertThat(client1, hasProperty("standbyTasks", ClientState::standbyTasks, Set.of()));
        assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, Set.of(TASK_0_1)));
        assertThat(client3, hasProperty("standbyTasks", ClientState::standbyTasks, Set.of()));
    }

    @Test
    public void shouldNotCountPreviousStandbyTasksTowardsMaxWarmupReplicas() {
        final int maxWarmupReplicas = 0;
        final Set<TaskId> allTasks = Set.of(TASK_0_0);
        final ClientState client1 = getClientStateWithActiveAssignment(Set.of(), Set.of(TASK_0_0), allTasks);
        client1.assignStandby(TASK_0_0);
        final ClientState client2 = getClientStateWithActiveAssignment(Set.of(TASK_0_0), Set.of(), allTasks);
        final Map<ProcessId, ClientState> clientStates = getClientStatesMap(client1, client2);

        final Map<TaskId, SortedSet<ProcessId>> tasksToCaughtUpClients = mkMap(
            mkEntry(TASK_0_0, mkSortedSet(PID_1))
        );
        final Map<TaskId, SortedSet<ProcessId>> tasksToClientByLag = mkMap(
            mkEntry(TASK_0_0, mkOrderedSet(PID_1, PID_2))
        );

        assertThat(
            "should have assigned movements",
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
                tasksToClientByLag,
                clientStates,
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas)
            ),
            is(1)
        );
        // Even though we have no warmups allowed, we still let client1 take over active processing while
        // client2 "warms up" because client1 was a caught-up standby, so it can "trade" standby status with
        // the not-caught-up active client2.

        // I.e., when you have a caught-up standby and a not-caught-up active, you can just swap their roles
        // and not call it a "warmup".
        assertThat(client1, hasProperty("activeTasks", ClientState::activeTasks, Set.of(TASK_0_0)));
        assertThat(client2, hasProperty("activeTasks", ClientState::activeTasks, Set.of()));

        assertThat(client1, hasProperty("standbyTasks", ClientState::standbyTasks, Set.of()));
        assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, Set.of(TASK_0_0)));

    }

    private static ClientState getClientStateWithActiveAssignment(final Set<TaskId> activeTasks,
                                                                  final Set<TaskId> caughtUpTasks,
                                                                  final Set<TaskId> allTasks) {
        final Map<TaskId, Long> lags = new HashMap<>();
        for (final TaskId task : allTasks) {
            if (caughtUpTasks.contains(task)) {
                lags.put(task, 0L);
            } else {
                lags.put(task, 10000L);
            }
        }
        return getClientStateWithLags(activeTasks, lags);
    }

    private static ClientState getClientStateWithLags(final Set<TaskId> activeTasks,
                                                      final Map<TaskId, Long> taskLags) {
        final ClientState client1 = new ClientState(activeTasks, emptySet(), taskLags, emptyMap(), 1);
        client1.assignActiveTasks(activeTasks);
        return client1;
    }

    /**
     * Creates a SortedSet with the sort order being the order of elements in the parameter list
     */
    private static SortedSet<ProcessId> mkOrderedSet(final ProcessId... clients) {
        final List<ProcessId> clientList = asList(clients);
        final SortedSet<ProcessId> set = new TreeSet<>(Comparator.comparing(clientList::indexOf));
        set.addAll(clientList);
        return set;
    }

}
