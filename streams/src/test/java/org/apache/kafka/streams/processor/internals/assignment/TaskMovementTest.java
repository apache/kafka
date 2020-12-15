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
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySortedSet;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
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
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.hasProperty;
import static org.apache.kafka.streams.processor.internals.assignment.TaskMovement.assignActiveTaskMovements;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TaskMovementTest {
    @Test
    public void shouldAssignTasksToClientsAndReturnFalseWhenAllClientsCaughtUp() {
        final int maxWarmupReplicas = Integer.MAX_VALUE;
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_1_0, TASK_1_1, TASK_1_2);

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = new HashMap<>();
        for (final TaskId task : allTasks) {
            tasksToCaughtUpClients.put(task, mkSortedSet(UUID_1, UUID_2, UUID_3));
        }

        final ClientState client1 = getClientStateWithActiveAssignment(asList(TASK_0_0, TASK_1_0));
        final ClientState client2 = getClientStateWithActiveAssignment(asList(TASK_0_1, TASK_1_1));
        final ClientState client3 = getClientStateWithActiveAssignment(asList(TASK_0_2, TASK_1_2));

        assertThat(
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
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

        final ClientState client1 = getClientStateWithActiveAssignment(asList(TASK_0_0, TASK_1_0));
        final ClientState client2 = getClientStateWithActiveAssignment(asList(TASK_0_1, TASK_1_1));
        final ClientState client3 = getClientStateWithActiveAssignment(asList(TASK_0_2, TASK_1_2));

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = mkMap(
            mkEntry(TASK_0_0, emptySortedSet()),
            mkEntry(TASK_0_1, emptySortedSet()),
            mkEntry(TASK_0_2, emptySortedSet()),
            mkEntry(TASK_1_0, emptySortedSet()),
            mkEntry(TASK_1_1, emptySortedSet()),
            mkEntry(TASK_1_2, emptySortedSet())
        );
        assertThat(
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
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
        final ClientState client1 = getClientStateWithActiveAssignment(singletonList(TASK_0_0));
        final ClientState client2 = getClientStateWithActiveAssignment(singletonList(TASK_0_1));
        final ClientState client3 = getClientStateWithActiveAssignment(singletonList(TASK_0_2));
        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = mkMap(
            mkEntry(TASK_0_0, mkSortedSet(UUID_1)),
            mkEntry(TASK_0_1, mkSortedSet(UUID_3)),
            mkEntry(TASK_0_2, mkSortedSet(UUID_2))
        );

        assertThat(
            "should have assigned movements",
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
                clientStates,
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas)
            ),
            is(2)
        );
        // The active tasks have changed to the ones that each client is caught up on
        assertThat(client1, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_0)));
        assertThat(client2, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_2)));
        assertThat(client3, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_1)));

        // we assigned warmups to migrate to the input active assignment
        assertThat(client1, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet()));
        assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet(TASK_0_1)));
        assertThat(client3, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet(TASK_0_2)));
    }

    @Test
    public void shouldOnlyGetUpToMaxWarmupReplicasAndReturnTrue() {
        final int maxWarmupReplicas = 1;
        final ClientState client1 = getClientStateWithActiveAssignment(singletonList(TASK_0_0));
        final ClientState client2 = getClientStateWithActiveAssignment(singletonList(TASK_0_1));
        final ClientState client3 = getClientStateWithActiveAssignment(singletonList(TASK_0_2));
        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = mkMap(
            mkEntry(TASK_0_0, mkSortedSet(UUID_1)),
            mkEntry(TASK_0_1, mkSortedSet(UUID_3)),
            mkEntry(TASK_0_2, mkSortedSet(UUID_2))
        );

        assertThat(
            "should have assigned movements",
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
                clientStates,
                new TreeMap<>(),
                new AtomicInteger(maxWarmupReplicas)
            ),
            is(2)
        );
        // The active tasks have changed to the ones that each client is caught up on
        assertThat(client1, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_0)));
        assertThat(client2, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_2)));
        assertThat(client3, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_1)));

        // we should only assign one warmup, but it could be either one that needs to be migrated.
        assertThat(client1, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet()));
        try {
            assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet(TASK_0_1)));
            assertThat(client3, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet()));
        } catch (final AssertionError ignored) {
            assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet()));
            assertThat(client3, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet(TASK_0_2)));
        }
    }

    @Test
    public void shouldNotCountPreviousStandbyTasksTowardsMaxWarmupReplicas() {
        final int maxWarmupReplicas = 0;
        final ClientState client1 = getClientStateWithActiveAssignment(emptyList());
        client1.assignStandby(TASK_0_0);
        final ClientState client2 = getClientStateWithActiveAssignment(singletonList(TASK_0_0));
        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);

        final Map<TaskId, SortedSet<UUID>> tasksToCaughtUpClients = mkMap(
            mkEntry(TASK_0_0, mkSortedSet(UUID_1))
        );

        assertThat(
            "should have assigned movements",
            assignActiveTaskMovements(
                tasksToCaughtUpClients,
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
        assertThat(client1, hasProperty("activeTasks", ClientState::activeTasks, mkSet(TASK_0_0)));
        assertThat(client2, hasProperty("activeTasks", ClientState::activeTasks, mkSet()));

        assertThat(client1, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet()));
        assertThat(client2, hasProperty("standbyTasks", ClientState::standbyTasks, mkSet(TASK_0_0)));

    }

    private static ClientState getClientStateWithActiveAssignment(final Collection<TaskId> activeTasks) {
        final ClientState client1 = new ClientState(1);
        client1.assignActiveTasks(activeTasks);
        return client1;
    }

}
