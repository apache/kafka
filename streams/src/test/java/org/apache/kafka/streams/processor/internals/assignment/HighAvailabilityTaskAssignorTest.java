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
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.utils.Utils.mkSortedSet;
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
    private Map<String, ClientState> clientStates = new HashMap<>();
    private Set<TaskId> allTasks = new HashSet<>();
    private Set<TaskId> statefulTasks = new HashSet<>();

    private static final TaskId task0_0 = new TaskId(0, 0);
    private static final TaskId task0_1 = new TaskId(0, 1);
    private static final TaskId task0_2 = new TaskId(0, 2);
    private static final TaskId task0_3 = new TaskId(0, 3);
    private static final TaskId task1_0 = new TaskId(1, 0);
    private static final TaskId task1_1 = new TaskId(1, 1);
    private static final TaskId task1_2 = new TaskId(1, 2);
    private static final TaskId task1_3 = new TaskId(1, 3);
    private static final TaskId task2_0 = new TaskId(2, 0);
    private static final TaskId task2_1 = new TaskId(2, 1);
    private static final TaskId task2_3 = new TaskId(2, 3);

    private static final String String1 = "client1";
    private static final String String2 = "client2";
    private static final String String3 = "client3";

    private ClientState client1;
    private ClientState client2;
    private ClientState client3;

    private static final Set<TaskId> emptyTasks = emptySet();

    private HighAvailabilityTaskAssignor<String> taskAssignor;

    private void createTaskAssignor() {
        configs = new AssignmentConfigs(
            acceptableRecoveryLag,
            balanceFactor,
            maxWarmupReplicas,
            numStandbyReplicas,
            probingRebalanceInterval
        );
        taskAssignor = new HighAvailabilityTaskAssignor<>(
            clientStates,
            allTasks,
            statefulTasks,
            configs);
    }

    @Test
    public void shouldRankPreviousClientAboveEquallyCaughtUpClient() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.lagFor(task0_0)).andReturn(Task.LATEST_OFFSET);
        expect(client2.lagFor(task0_0)).andReturn(0L);
        replay(client1, client2);

        final SortedSet<RankedClient<String>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(String1, Task.LATEST_OFFSET),
            new RankedClient<>(String2, 0L)
        );

        final Map<String, ClientState> states = mkMap(
            mkEntry(String1, client1),
            mkEntry(String2, client2)
        );

        final Map<TaskId, SortedSet<RankedClient<String>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, acceptableRecoveryLag);

        final SortedSet<RankedClient<String>> clientRanking = statefulTasksToRankedCandidates.get(task0_0);

        EasyMock.verify(client1, client2);
        assertThat(clientRanking, equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankTaskWithUnknownOffsetSumBelowCaughtUpClientAndClientWithLargeLag() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        client3 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.lagFor(task0_0)).andReturn(UNKNOWN_OFFSET_SUM);
        expect(client2.lagFor(task0_0)).andReturn(50L);
        expect(client3.lagFor(task0_0)).andReturn(500L);
        replay(client1, client2, client3);

        final SortedSet<RankedClient<String>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(String2, 0L),
            new RankedClient<>(String1, 1L),
            new RankedClient<>(String3, 500L)
        );

        final Map<String, ClientState> states = mkMap(
            mkEntry(String1, client1),
            mkEntry(String2, client2),
            mkEntry(String3, client3)
        );

        final Map<TaskId, SortedSet<RankedClient<String>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, acceptableRecoveryLag);

        final SortedSet<RankedClient<String>> clientRanking = statefulTasksToRankedCandidates.get(task0_0);

        EasyMock.verify(client1, client2, client3);
        assertThat(clientRanking, equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankAllClientsWithinAcceptableRecoveryLagWithRank0() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.lagFor(task0_0)).andReturn(100L);
        expect(client2.lagFor(task0_0)).andReturn(0L);
        replay(client1, client2);

        final SortedSet<RankedClient<String>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(String1, 0L),
            new RankedClient<>(String2, 0L)
        );

        final Map<String, ClientState> states = mkMap(
            mkEntry(String1, client1),
            mkEntry(String2, client2)
        );

        final Map<TaskId, SortedSet<RankedClient<String>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, acceptableRecoveryLag);

        EasyMock.verify(client1, client2);
        assertThat(statefulTasksToRankedCandidates.get(task0_0), equalTo(expectedClientRanking));
    }

    @Test
    public void shouldRankNotCaughtUpClientsAccordingToLag() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        client3 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.lagFor(task0_0)).andReturn(900L);
        expect(client2.lagFor(task0_0)).andReturn(800L);
        expect(client3.lagFor(task0_0)).andReturn(500L);
        replay(client1, client2, client3);

        final SortedSet<RankedClient<String>> expectedClientRanking = mkSortedSet(
            new RankedClient<>(String3, 500L),
            new RankedClient<>(String2, 800L),
            new RankedClient<>(String1, 900L)
        );

        final Map<String, ClientState> states = mkMap(
            mkEntry(String1, client1),
            mkEntry(String2, client2),
            mkEntry(String3, client3)
        );

        final Map<TaskId, SortedSet<RankedClient<String>>> statefulTasksToRankedCandidates =
            buildClientRankingsByTask(singleton(task0_0), states, acceptableRecoveryLag);

        EasyMock.verify(client1, client2, client3);
        assertThat(statefulTasksToRankedCandidates.get(task0_0), equalTo(expectedClientRanking));
    }

    @Test
    public void shouldGetMovementsFromStateConstrainedToBalancedAssignment() {
        final Map<String, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(String1, asList(task0_0, task1_2)),
            mkEntry(String2, asList(task0_1, task1_0)),
            mkEntry(String3, asList(task0_2, task1_1))
        );
        final Map<String, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(String1, asList(task0_0, task1_0)),
            mkEntry(String2, asList(task0_1, task1_1)),
            mkEntry(String3, asList(task0_2, task1_2))
        );
        final Queue<Movement<String>> expectedMovements = new LinkedList<>();
        expectedMovements.add(new Movement<>(task1_2, String1, String3));
        expectedMovements.add(new Movement<>(task1_0, String2, String1));
        expectedMovements.add(new Movement<>(task1_1, String3, String2));

        assertThat(getMovements(stateConstrainedAssignment, balancedAssignment), equalTo(expectedMovements));
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfAssignmentsAreOfDifferentSize() {
        final Map<String, List<TaskId>> stateConstrainedAssignment = mkMap(
            mkEntry(String1, asList(task0_0, task0_1))
        );
        final Map<String, List<TaskId>> balancedAssignment = mkMap(
            mkEntry(String1, asList(task0_0, task1_0)),
            mkEntry(String2, asList(task0_1, task1_1))
        );
        assertThrows(IllegalStateException.class, () -> getMovements(stateConstrainedAssignment, balancedAssignment));
    }

    @Test
    public void shouldDecidePreviousAssignmentIsInvalidIfThereAreUnassignedActiveTasks() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.prevActiveTasks()).andReturn(singleton(task0_0));
        expect(client1.prevStandbyTasks()).andStubReturn(emptyTasks);
        replay(client1);
        allTasks =  mkSet(task0_0, task0_1);
        clientStates = singletonMap(String1, client1);
        createTaskAssignor();

        assertFalse(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldDecidePreviousAssignmentIsInvalidIfThereAreUnassignedStandbyTasks() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.prevActiveTasks()).andStubReturn(singleton(task0_0));
        expect(client1.prevStandbyTasks()).andReturn(emptyTasks);
        replay(client1);
        allTasks =  mkSet(task0_0);
        statefulTasks =  mkSet(task0_0);
        clientStates = singletonMap(String1, client1);
        numStandbyReplicas = 1;
        createTaskAssignor();

        assertFalse(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldDecidePreviousAssignmentIsInvalidIfActiveTasksWasNotOnCaughtUpClient() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
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
            mkEntry(String1, client1),
            mkEntry(String2, client2)
        );
        createTaskAssignor();

        assertFalse(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldDecidePreviousAssignmentIsValid() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
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
            mkEntry(String1, client1),
            mkEntry(String2, client2)
        );
        createTaskAssignor();

        assertTrue(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldReturnTrueIfTaskHasNoCaughtUpClients() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.lagFor(task0_0)).andReturn(500L);
        replay(client1);
        allTasks =  mkSet(task0_0);
        statefulTasks =  mkSet(task0_0);
        clientStates = singletonMap(String1, client1);
        createTaskAssignor();

        assertTrue(taskAssignor.taskIsCaughtUpOnClient(task0_0, String1));
    }

    @Test
    public void shouldReturnTrueIfTaskIsCaughtUpOnClient() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.lagFor(task0_0)).andReturn(0L);
        allTasks =  mkSet(task0_0);
        statefulTasks =  mkSet(task0_0);
        clientStates = singletonMap(String1, client1);
        replay(client1);
        createTaskAssignor();

        assertTrue(taskAssignor.taskIsCaughtUpOnClient(task0_0, String1));
    }

    @Test
    public void shouldReturnFalseIfTaskWasNotCaughtUpOnClientButCaughtUpClientsExist() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.lagFor(task0_0)).andReturn(500L);
        expect(client2.lagFor(task0_0)).andReturn(0L);
        replay(client1, client2);
        allTasks =  mkSet(task0_0);
        statefulTasks =  mkSet(task0_0);
        clientStates = mkMap(
            mkEntry(String1, client1),
            mkEntry(String2, client2)
        );
        createTaskAssignor();

        assertFalse(taskAssignor.taskIsCaughtUpOnClient(task0_0, String1));
    }

    @Test
    public void shouldComputeBalanceFactorAsDifferenceBetweenMostAndLeastLoadedClients() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        client3 = EasyMock.createNiceMock(ClientState.class);
        final Set<ClientState> states = mkSet(client1, client2, client3);
        final Set<TaskId> statefulTasks = mkSet(task0_0, task0_1, task0_2, task0_3, task1_0, task1_1, task2_0, task2_1, task2_3);

        expect(client1.capacity()).andReturn(1);
        expect(client1.activeTasks()).andReturn(mkSet(task0_0, task0_1, task0_2, task0_3));

        expect(client2.capacity()).andReturn(1);
        expect(client2.activeTasks()).andReturn(mkSet(task1_0, task1_1));

        expect(client3.capacity()).andReturn(1);
        expect(client3.activeTasks()).andReturn(mkSet(task2_0, task2_1, task2_3));

        replay(client1, client2, client3);
        assertThat(computeBalanceFactor(states, statefulTasks, ClientState::activeTasks), equalTo(2));
    }

    @Test
    public void shouldComputeBalanceFactorWithDifferentClientCapacities() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        client3 = EasyMock.createNiceMock(ClientState.class);
        final Set<ClientState> states = mkSet(client1, client2, client3);
        final Set<TaskId> statefulTasks = mkSet(task0_0, task0_1, task0_2, task0_3, task1_0, task1_1, task2_0, task2_1, task2_3);

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
        assertThat(computeBalanceFactor(states, statefulTasks, ClientState::activeTasks), equalTo(3));
    }

    @Test
    public void shouldComputeBalanceFactorBasedOnStatefulTasksOnly() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        client3 = EasyMock.createNiceMock(ClientState.class);
        final Set<ClientState> states = mkSet(client1, client2, client3);

        // 0_0 and 0_1 are stateless
        final Set<TaskId> statefulTasks = mkSet(task0_2, task0_3, task1_0, task1_1, task2_0, task2_1, task2_3);

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
        assertThat(computeBalanceFactor(states, statefulTasks, ClientState::activeTasks), equalTo(1));
    }

    @Test
    public void shouldComputeBalanceFactorOfZeroWithOnlyOneClient() {
        final Set<TaskId> statefulTasks = mkSet(task0_0, task0_1, task0_2, task0_3);
        client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.capacity()).andReturn(1);
        expect(client1.activeTasks()).andReturn(mkSet(task0_0, task0_1, task0_2, task0_3));
        replay(client1);
        assertThat(computeBalanceFactor(singleton(client1), statefulTasks, ClientState::activeTasks), equalTo(0));
    }

    @Test
    public void shouldAssignStandbysForStatefulTasks() {
        numStandbyReplicas = 1;
        allTasks = mkSet(task0_0, task0_1);
        statefulTasks = mkSet(task0_0, task0_1);

        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(task0_0));
        client2 = getMockClientWithPreviousCaughtUpTasks(mkSet(task0_1));

        clientStates = getClientStatesWithTwoClients();
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(task0_0)));
        assertThat(client2.activeTasks(), equalTo(mkSet(task0_1)));
        assertThat(client1.standbyTasks(), equalTo(mkSet(task0_1)));
        assertThat(client2.standbyTasks(), equalTo(mkSet(task0_0)));
    }

    @Test
    public void shouldNotAssignStandbysForStatelessTasks() {
        numStandbyReplicas = 1;
        allTasks = mkSet(task0_0, task0_1);
        statefulTasks = emptyTasks;

        client1 = getMockClientWithPreviousCaughtUpTasks(emptyTasks);
        client2 = getMockClientWithPreviousCaughtUpTasks(emptyTasks);

        clientStates = getClientStatesWithTwoClients();
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTaskCount(), equalTo(1));
        assertThat(client2.activeTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1, client2);
    }

    @Test
    public void shouldAssignWarmupReplicasEvenIfNoStandbyReplicasConfigured() {
        allTasks = mkSet(task0_0, task0_1);
        statefulTasks = mkSet(task0_0, task0_1);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(task0_0, task0_1));
        client2 = getMockClientWithPreviousCaughtUpTasks(emptyTasks);

        clientStates = getClientStatesWithTwoClients();
        createTaskAssignor();
        taskAssignor.assign();
        
        assertThat(client1.activeTasks(), equalTo(mkSet(task0_0, task0_1)));
        assertThat(client2.standbyTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
    }

    @Test
    public void shouldNotAssignMoreThanMaxWarmupReplicas() {
        maxWarmupReplicas = 1;
        allTasks = mkSet(task0_0, task0_1, task0_2, task0_3);
        statefulTasks = mkSet(task0_0, task0_1, task0_2, task0_3);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(task0_0, task0_1, task0_2, task0_3));
        client2 = getMockClientWithPreviousCaughtUpTasks(emptyTasks);

        clientStates = getClientStatesWithTwoClients();
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(task0_0, task0_1, task0_2, task0_3)));
        assertThat(client2.standbyTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
    }

    @Test
    public void shouldAssignStandbyReplicasInAdditionToWarmupReplicas() {
        numStandbyReplicas = 1;
        maxWarmupReplicas = 1;

        allTasks = mkSet(task0_0, task0_1, task0_2, task0_3);
        statefulTasks = mkSet(task0_0, task0_1, task0_2, task0_3);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(task0_0, task0_1, task0_2, task0_3));
        client2 = getMockClientWithPreviousCaughtUpTasks(emptyTasks);

        clientStates = getClientStatesWithTwoClients();
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(task0_0, task0_1, task0_2, task0_3)));
        assertThat(client2.standbyTasks(), equalTo(mkSet(task0_0, task0_1, task0_2, task0_3)));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
    }

    @Test
    public void shouldNotAssignAnyStandbysWithInsufficientCapacity() {
        numStandbyReplicas = 1;
        allTasks = mkSet(task0_0, task0_1);
        statefulTasks = mkSet(task0_0, task0_1);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(task0_0, task0_1));

        clientStates = getClientStatesWithOneClient();
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(task0_0, task0_1)));
        assertHasNoStandbyTasks(client1);
    }

    @Test
    public void shouldAssignActiveTasksToNotCaughtUpClientIfNoneExist() {
        numStandbyReplicas = 1;
        allTasks = mkSet(task0_0, task0_1);
        statefulTasks = mkSet(task0_0, task0_1);
        client1 = getMockClientWithPreviousCaughtUpTasks(emptyTasks);

        clientStates = getClientStatesWithOneClient();
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(task0_0, task0_1)));
        assertHasNoStandbyTasks(client1);
    }

    @Test
    public void shouldNotAssignMoreThanMaxWarmupReplicasWithStandbys() {
        numStandbyReplicas = 1;

        allTasks = mkSet(task0_0, task0_1, task0_2, task0_3);
        statefulTasks = mkSet(task0_0, task0_1, task0_2, task0_3);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(task0_0, task0_1, task0_2, task0_3));
        client2 = getMockClientWithPreviousCaughtUpTasks(emptyTasks);
        client3 = getMockClientWithPreviousCaughtUpTasks(emptyTasks);

        clientStates = getClientStatesWithThreeClients();
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTaskCount(), equalTo(4));
        assertThat(client2.standbyTaskCount(), equalTo(3));
        assertThat(client3.standbyTaskCount(), equalTo(3));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2, client3);
    }

    @Test
    public void shouldDistributeStatelessTasksToBalanceTotalActiveTaskLoad() {
        allTasks = mkSet(task0_0, task0_1, task0_2, task0_3, task1_0, task1_1, task1_2);
        statefulTasks = mkSet(task0_0, task0_1, task0_2, task0_3);

        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(task0_0, task0_1, task0_2, task0_3));
        client2 = getMockClientWithPreviousCaughtUpTasks(emptyTasks);

        clientStates = getClientStatesWithTwoClients();
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(task0_0, task0_1, task0_2, task0_3)));
        assertThat(client2.activeTasks(), equalTo(mkSet(task1_0, task1_1, task1_2)));
    }

    @Test
    public void shouldGiveEachClientAtLeastOneTask() {
        allTasks = mkSet(task0_0, task0_1, task0_2, task0_3, task1_0, task1_1, task1_2, task1_3, task2_0);
        statefulTasks = new HashSet<>(allTasks);
        client1 = getMockClientWithPreviousCaughtUpTasks(allTasks).withCapacity(1);
        client2 = getMockClientWithPreviousCaughtUpTasks(allTasks).withCapacity(100);

        clientStates = getClientStatesWithTwoClients();
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTaskCount(), equalTo(1));
        assertThat(client2.activeTaskCount(), equalTo(8));
    }

    @Test
    public void shouldDistributeStatefulActiveTasksOverAllClients() {
        allTasks = mkSet(task0_0, task0_1, task0_2, task0_3, task1_0, task1_1, task1_2, task1_3, task2_0); // 9 total
        statefulTasks = new HashSet<>(allTasks);
        client1 = getMockClientWithPreviousCaughtUpTasks(allTasks).withCapacity(10);
        client2 = getMockClientWithPreviousCaughtUpTasks(allTasks).withCapacity(5);
        client3 = getMockClientWithPreviousCaughtUpTasks(allTasks).withCapacity(1);

        clientStates = getClientStatesWithThreeClients();
        createTaskAssignor();
        taskAssignor.assign();

        assertFalse(client1.activeTasks().isEmpty());
        assertFalse(client2.activeTasks().isEmpty());
        assertFalse(client3.activeTasks().isEmpty());
    }

    @Test
    public void testAssignWithMultipleNumStandbys() {
        numStandbyReplicas = 1;

        //TODO
        allTasks = mkSet(task0_0, task0_1, task0_2, task0_3);
        statefulTasks = mkSet(task0_0, task0_1, task0_2, task0_3);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(task0_0, task0_1, task0_2, task0_3));
        client2 = getMockClientWithPreviousCaughtUpTasks(emptyTasks);

        clientStates = getClientStatesWithTwoClients();
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(task0_0, task0_1, task0_2, task0_3)));
        assertThat(client2.standbyTasks(), equalTo(mkSet(task0_0, task0_1, task0_2, task0_3)));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
    }

    @Test
    public void shouldReturnFalseIfPreviousAssignmentIsReused() {

    }

    @Test
    public void shouldReturnFalseIfNoWarmupTasksAreAssigned() {

    }

    @Test
    public void statefulActiveTaskAssignmentShouldBeDeterministic() {
        // TODO
    }

    private Map<String, ClientState> getClientStatesWithOneClient() {
        return singletonMap(String1, client1);
    }

    private Map<String, ClientState> getClientStatesWithTwoClients() {
        return mkMap(mkEntry(String1, client1), mkEntry(String2, client2));
    }

    private Map<String, ClientState> getClientStatesWithThreeClients() {
        return mkMap(mkEntry(String1, client1), mkEntry(String2, client2), mkEntry(String3, client3));
    }

    private static void assertHasNoActiveTasks(final ClientState... clients) {
        for (final ClientState client : clients) {
            assertTrue(client.activeTasks().isEmpty());
        }
    }

    private static void assertHasNoStandbyTasks(final ClientState... clients) {
        for (final ClientState client : clients) {
            assertTrue(client.standbyTasks().isEmpty());
        }
    }

    MockClientState getMockClientWithPreviousCaughtUpTasks(final Set<TaskId> statefulActiveTasks) {
        if (!statefulTasks.containsAll(statefulActiveTasks)) {
            throw new IllegalArgumentException("Need to initialize stateful tasks set before creating mock clients");
        }
        final Map<TaskId, Long> taskLags = new HashMap<>();
        for (final TaskId task : statefulTasks) {
            if (statefulActiveTasks.contains(task)) {
                taskLags.put(task, 0L);
            } else {
                taskLags.put(task, Long.MAX_VALUE);
            }
        }
        final MockClientState client = new MockClientState(1, taskLags);
        client.addPreviousActiveTasks(statefulActiveTasks);
        return client;
    }
    
    static class MockClientState extends ClientState {
        private final Map<TaskId, Long> taskLagTotals;

        private MockClientState(final int capacity,
                                final Map<TaskId, Long> taskLagTotals) {
            super(capacity);
            this.taskLagTotals = taskLagTotals;
        }
            
        @Override
        long lagFor(final TaskId task) {
            final Long totalLag = taskLagTotals.get(task);
            if (totalLag == null) {
                return Long.MAX_VALUE;
            } else {
                return totalLag;
            }
        }

        MockClientState withCapacity(final int capacity) {
            return new MockClientState(capacity, taskLagTotals);
        }
    }
}
