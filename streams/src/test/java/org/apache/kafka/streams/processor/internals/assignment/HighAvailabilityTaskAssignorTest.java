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
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.emptyTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.task0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.task0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.task0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.task0_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.task1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.task1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.task1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.task2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.task2_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.task2_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuid1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuid2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuid3;
import static org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor.buildClientRankingsByTask;
import static org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor.computeBalanceFactor;
import static org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor.getMovements;
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor.Movement;
import org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor.RankedClient;
import org.easymock.EasyMock;
import org.junit.Test;

public class HighAvailabilityTaskAssignorTest {
    private long acceptableRecoveryLag = 100L;
    private int balanceFactor = 1;
    private int maxWarmupReplicas = 2;
    private int numStandbyReplicas = 0;
    private long probingRebalanceInterval = 60 * 1000L;

    private AssignmentConfigs configs;
    private Map<UUID, ClientState> clientStates = new HashMap<>();
    private Set<TaskId> allTasks = new HashSet<>();
    private Set<TaskId> statefulTasks = new HashSet<>();

    private HighAvailabilityTaskAssignor<UUID> taskAssignor;

    private void createTaskAssignor() {
        configs = new AssignmentConfigs(
            acceptableRecoveryLag,
            balanceFactor,
            maxWarmupReplicas,
            numStandbyReplicas,
            probingRebalanceInterval
        );
        taskAssignor = new HighAvailabilityTaskAssignor<>(clientStates, allTasks, statefulTasks, configs);
    }

    @Test
    public void shouldRankPreviousClientAboveEquallyCaughtUpClient() {
        final ClientState client1 = EasyMock.createMock(ClientState.class);
        final ClientState client2 = EasyMock.createMock(ClientState.class);

        expect(client1.lagFor(task0_0)).andReturn(Task.LATEST_OFFSET);
        expect(client2.lagFor(task0_0)).andReturn(0L);
        replay(client1, client2);

        final SortedSet<RankedClient<UUID>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(uuid1, Task.LATEST_OFFSET),
            new RankedClient<>(uuid2, 0L)
        );

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(uuid1, client1),
            mkEntry(uuid2, client2)
        );

        final Map<TaskId, SortedSet<RankedClient<UUID>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, acceptableRecoveryLag);

        final SortedSet<RankedClient<UUID>> clientRanking = statefulTasksToRankedCandidates.get(task0_0);

        EasyMock.verify(client1, client2);
        assertThat(clientRanking, equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankTaskWithUnknownOffsetSumBelowCaughtUpClientAndClientWithLargeLag() {
        final ClientState client1 = EasyMock.createMock(ClientState.class);
        final ClientState client2 = EasyMock.createMock(ClientState.class);
        final ClientState client3 = EasyMock.createMock(ClientState.class);

        expect(client1.lagFor(task0_0)).andReturn(UNKNOWN_OFFSET_SUM);
        expect(client2.lagFor(task0_0)).andReturn(50L);
        expect(client3.lagFor(task0_0)).andReturn(500L);
        replay(client1, client2, client3);

        final SortedSet<RankedClient<UUID>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(uuid2, 0L),
            new RankedClient<>(uuid1, 1L),
            new RankedClient<>(uuid3, 500L)
        );

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(uuid1, client1),
            mkEntry(uuid2, client2),
            mkEntry(uuid3, client3)
        );

        final Map<TaskId, SortedSet<RankedClient<UUID>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, acceptableRecoveryLag);

        final SortedSet<RankedClient<UUID>> clientRanking = statefulTasksToRankedCandidates.get(task0_0);

        EasyMock.verify(client1, client2, client3);
        assertThat(clientRanking, equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankAllClientsWithinAcceptableRecoveryLagWithRank0() {
        final ClientState client1 = EasyMock.createMock(ClientState.class);
        final ClientState client2 = EasyMock.createMock(ClientState.class);

        expect(client1.lagFor(task0_0)).andReturn(100L);
        expect(client2.lagFor(task0_0)).andReturn(0L);
        replay(client1, client2);

        final SortedSet<RankedClient<UUID>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(uuid1, 0L),
            new RankedClient<>(uuid2, 0L)
        );

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(uuid1, client1),
            mkEntry(uuid2, client2)
        );

        final Map<TaskId, SortedSet<RankedClient<UUID>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, acceptableRecoveryLag);

        EasyMock.verify(client1, client2);
        assertThat(statefulTasksToRankedCandidates.get(task0_0), equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankNotCaughtUpClientsAccordingToLag() {
        final ClientState client1 = EasyMock.createMock(ClientState.class);
        final ClientState client2 = EasyMock.createMock(ClientState.class);
        final ClientState client3 = EasyMock.createMock(ClientState.class);

        expect(client1.lagFor(task0_0)).andReturn(900L);
        expect(client2.lagFor(task0_0)).andReturn(800L);
        expect(client3.lagFor(task0_0)).andReturn(500L);
        replay(client1, client2, client3);

        final SortedSet<RankedClient<UUID>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(uuid3, 500L),
            new RankedClient<>(uuid2, 800L),
            new RankedClient<>(uuid1, 900L)
        );

        final Map<UUID, ClientState> states = mkMap(
            mkEntry(uuid1, client1),
            mkEntry(uuid2, client2),
            mkEntry(uuid3, client3)
        );

        final Map<TaskId, SortedSet<RankedClient<UUID>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, acceptableRecoveryLag);

        EasyMock.verify(client1, client2, client3);
        assertThat(statefulTasksToRankedCandidates.get(task0_0), equalTo(expectedClientRanking));
    }

    @Test
    public void testGetMovements() {
        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(uuid1, asList(task0_0, task1_2)),
            mkEntry(uuid2, asList(task0_1, task1_0)),
            mkEntry(uuid3, asList(task0_2, task1_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(uuid1, asList(task0_0, task1_0)),
            mkEntry(uuid2, asList(task0_1, task1_1)),
            mkEntry(uuid3, asList(task0_2, task1_2))
        );
        final Queue<Movement<UUID>> expectedMovements = new LinkedList<>();
        expectedMovements.add(new Movement<>(task1_2, uuid1, uuid3));
        expectedMovements.add(new Movement<>(task1_0, uuid2, uuid1));
        expectedMovements.add(new Movement<>(task1_1, uuid3, uuid2));

        assertThat(getMovements(stateConstrainedAssignment, balancedAssignment), equalTo(expectedMovements));
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfAssignmentsAreOfDifferentSize() {
        final Map<UUID, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(uuid1, asList(task0_0, task0_1))
        );
        final Map<UUID, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(uuid1, asList(task0_0, task1_0)),
            mkEntry(uuid2, asList(task0_1, task1_1))
        );
        assertThrows(IllegalStateException.class, () -> getMovements(stateConstrainedAssignment, balancedAssignment));
    }

    @Test
    public void testGetNumStandbyLeastLoadedCandidates() {
        //TODO-soph -- need to fix implementation first, account for capacity
    }

    @Test
    public void testGetNumStandbyLeastLoadedCandidatesWithInsufficientCapacity() {

    }

    @Test
    public void shouldDecidePreviousAssignmentIsInvalidIfThereAreUnassignedActiveTasks() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.prevActiveTasks()).andReturn(singleton(task0_0));
        expect(client1.prevStandbyTasks()).andStubReturn(emptyTasks);
        replay(client1);
        allTasks =  mkSet(task0_0, task0_1);
        clientStates = singletonMap(uuid1, client1);
        createTaskAssignor();

        assertFalse(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldDecidePreviousAssignmentIsInvalidIfThereAreUnassignedStandbyTasks() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.prevActiveTasks()).andStubReturn(singleton(task0_0));
        expect(client1.prevStandbyTasks()).andReturn(emptyTasks);
        replay(client1);
        allTasks =  mkSet(task0_0);
        statefulTasks =  mkSet(task0_0);
        clientStates = singletonMap(uuid1, client1);
        numStandbyReplicas = 1;
        createTaskAssignor();

        assertFalse(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldDecidePreviousAssignmentIsInvalidIfActiveTasksWasNotOnCaughtUpClient() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client2 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.prevStandbyTasks()).andStubReturn(emptyTasks);
        expect(client2.prevStandbyTasks()).andStubReturn(emptyTasks);

        expect(client1.prevActiveTasks()).andReturn(singleton(task0_0));
        expect(client2.prevActiveTasks()).andReturn(singleton(task0_1));
        expect(client1.lagFor(task0_0)).andReturn(500L);
        expect(client2.lagFor(task0_0)).andReturn(0L);
        replay(client1, client2);

        allTasks =  mkSet(task0_0, task0_1);
        statefulTasks =  mkSet(task0_0);
        clientStates = mkMap(
            mkEntry(uuid1, client1),
            mkEntry(uuid2, client2)
        );
        createTaskAssignor();

        assertFalse(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldDecidePreviousAssignmentIsValid() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client2 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.prevStandbyTasks()).andStubReturn(emptyTasks);
        expect(client2.prevStandbyTasks()).andStubReturn(emptyTasks);

        expect(client1.prevActiveTasks()).andReturn(singleton(task0_0));
        expect(client2.prevActiveTasks()).andReturn(singleton(task0_1));
        expect(client1.lagFor(task0_0)).andReturn(0L);
        expect(client2.lagFor(task0_0)).andReturn(0L);
        replay(client1, client2);

        allTasks =  mkSet(task0_0, task0_1);
        statefulTasks =  mkSet(task0_0);
        clientStates = mkMap(
            mkEntry(uuid1, client1),
            mkEntry(uuid2, client2)
        );
        createTaskAssignor();

        assertTrue(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldReturnTrueIfTaskHasNoCaughtUpClients() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.lagFor(task0_0)).andReturn(500L);
        replay(client1);
        allTasks =  mkSet(task0_0);
        statefulTasks =  mkSet(task0_0);
        clientStates = singletonMap(uuid1, client1);
        createTaskAssignor();

        assertTrue(taskAssignor.taskIsCaughtUpOnClient(task0_0, uuid1));
    }

    @Test
    public void shouldReturnTrueIfTaskIsCaughtUpOnClient() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.lagFor(task0_0)).andReturn(0L);
        allTasks =  mkSet(task0_0);
        statefulTasks =  mkSet(task0_0);
        clientStates = singletonMap(uuid1, client1);
        replay(client1);
        createTaskAssignor();

        assertTrue(taskAssignor.taskIsCaughtUpOnClient(task0_0, uuid1));
    }

    @Test
    public void shouldReturnFalseIfTaskWasNotCaughtUpOnClientButCaughtUpClientsExist() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client2 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.lagFor(task0_0)).andReturn(500L);
        expect(client2.lagFor(task0_0)).andReturn(0L);
        replay(client1, client2);
        allTasks =  mkSet(task0_0);
        statefulTasks =  mkSet(task0_0);
        clientStates = mkMap(
            mkEntry(uuid1, client1),
            mkEntry(uuid2, client2)
        );
        createTaskAssignor();

        assertFalse(taskAssignor.taskIsCaughtUpOnClient(task0_0, uuid1));
    }

    @Test
    public void shouldComputeBalanceFactorAsDifferenceBetweenMostAndLeastLoadedClients() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client2 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client3 = EasyMock.createNiceMock(ClientState.class);
        final Set<ClientState> states = mkSet(client1, client2, client3);

        expect(client1.capacity()).andReturn(1);
        expect(client1.activeTasks()).andReturn(mkSet(task0_0, task0_1, task0_2, task0_3));

        expect(client2.capacity()).andReturn(1);
        expect(client2.activeTasks()).andReturn(mkSet(task1_0, task1_1));

        expect(client3.capacity()).andReturn(1);
        expect(client3.activeTasks()).andReturn(mkSet(task2_0, task2_1, task2_3));

        replay(client1, client2, client3);
        assertThat(computeBalanceFactor(states, emptyTasks, ClientState::activeTasks), equalTo(2));
    }

    @Test
    public void shouldComputeBalanceFactorWithDifferentClientCapacities() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client2 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client3 = EasyMock.createNiceMock(ClientState.class);
        final Set<ClientState> states = mkSet(client1, client2, client3);

        // client 1: 4 tasks per thread
        expect(client1.capacity()).andReturn(1);
        expect(client1.activeTasks()).andReturn(mkSet(task0_0, task0_1, task0_2, task0_3));

        // client 2: 1 task per thread
        expect(client2.capacity()).andReturn(2);
        expect(client2.activeTasks()).andReturn(mkSet(task1_0, task1_1));

        // client 3: 1 task per thread
        expect(client3.capacity()).andReturn(3);
        expect(client3.activeTasks()).andReturn(mkSet(task2_0, task2_1, task2_3));

        replay(client1, client2, client3);
        assertThat(computeBalanceFactor(states, emptyTasks, ClientState::activeTasks), equalTo(3));
    }

    @Test
    public void shouldComputeBalanceFactorBasedOnStatefulTasks() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client2 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client3 = EasyMock.createNiceMock(ClientState.class);

        final Set<ClientState> states = mkSet(client1, client2, client3);
        final Set<TaskId> statelessTasks = mkSet(task0_0, task0_1);

        // client 1: 2 stateful tasks per thread
        expect(client1.capacity()).andReturn(1);
        expect(client1.activeTasks()).andReturn(mkSet(task0_0, task0_1, task0_2, task0_3));

        // client 2: 1 stateful task per thread
        expect(client2.capacity()).andReturn(2);
        expect(client2.activeTasks()).andReturn(mkSet(task1_0, task1_1));

        // client 3: 1 stateful task per thread
        expect(client3.capacity()).andReturn(3);
        expect(client3.activeTasks()).andReturn(mkSet(task2_0, task2_1, task2_3));

        replay(client1, client2, client3);
        assertThat(computeBalanceFactor(states, statelessTasks, ClientState::activeTasks), equalTo(1));
    }

    @Test
    public void shouldComputeBalanceFactorOfZeroWithOnlyOneClient() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.capacity()).andReturn(1);
        expect(client1.activeTasks()).andReturn(mkSet(task0_0, task0_1, task0_2, task0_3));
        replay(client1);
        assertThat(computeBalanceFactor(singleton(client1), emptyTasks, ClientState::activeTasks), equalTo(0));
    }

}
