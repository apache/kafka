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
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_TASKS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getClientStatesMap;
import static org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor.computeBalanceFactor;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class HighAvailabilityTaskAssignorTest {
    private final AssignmentConfigs configWithoutStandbys = new AssignmentConfigs(
        /*acceptableRecoveryLag*/ 100L,
        /*maxWarmupReplicas*/ 2,
        /*numStandbyReplicas*/ 0,
        /*probingRebalanceIntervalMs*/ 60 * 1000L
    );

    private final AssignmentConfigs configWithStandbys = new AssignmentConfigs(
        /*acceptableRecoveryLag*/ 100L,
        /*maxWarmupReplicas*/ 2,
        /*numStandbyReplicas*/ 1,
        /*probingRebalanceIntervalMs*/ 60 * 1000L
    );

    @Test
    public void shouldComputeNewAssignmentIfThereAreUnassignedActiveTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final ClientState client1 = new ClientState(singleton(TASK_0_0), emptySet(), singletonMap(TASK_0_0, 0L), 1);
        final Map<UUID, ClientState> clientStates = singletonMap(UUID_1, client1);

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(clientStates,
                                                                                         allTasks,
                                                                                         singleton(TASK_0_0),
                                                                                         configWithoutStandbys);

        assertThat(clientStates.get(UUID_1).activeTasks(), not(singleton(TASK_0_0)));
        assertThat(clientStates.get(UUID_1).standbyTasks(), empty());
        assertThat(probingRebalanceNeeded, is(false));
    }

    @Test
    public void shouldComputeNewAssignmentIfThereAreUnassignedStandbyTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0);
        final ClientState client1 = new ClientState(singleton(TASK_0_0), emptySet(), singletonMap(TASK_0_0, 0L), 1);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), singletonMap(TASK_0_0, 0L), 1);
        final Map<UUID, ClientState> clientStates = mkMap(mkEntry(UUID_1, client1), mkEntry(UUID_2, client2));

        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(clientStates,
                                                                                         allTasks,
                                                                                         statefulTasks,
                                                                                         configWithStandbys);

        assertThat(clientStates.get(UUID_2).standbyTasks(), not(empty()));
        assertThat(probingRebalanceNeeded, is(false));
    }

    @Test
    public void shouldComputeNewAssignmentIfActiveTasksWasNotOnCaughtUpClient() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0);
        final ClientState client1 = new ClientState(singleton(TASK_0_0), emptySet(), singletonMap(TASK_0_0, 500L), 1);
        final ClientState client2 = new ClientState(singleton(TASK_0_1), emptySet(), singletonMap(TASK_0_0, 0L), 1);
        final Map<UUID, ClientState> clientStates = mkMap(
            mkEntry(UUID_1, client1),
            mkEntry(UUID_2, client2)
        );

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithoutStandbys);

        assertThat(clientStates.get(UUID_1).activeTasks(), is(singleton(TASK_0_1)));
        assertThat(clientStates.get(UUID_2).activeTasks(), is(singleton(TASK_0_0)));
        // we'll warm up task 0_0 on client1 because it's first in sorted order,
        // although this isn't an optimal convergence
        assertThat(probingRebalanceNeeded, is(true));
    }

    @Test
    public void shouldComputeBalanceFactorAsDifferenceBetweenMostAndLeastLoadedClients() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client2 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client3 = EasyMock.createNiceMock(ClientState.class);
        final Set<ClientState> states = mkSet(client1, client2, client3);
        final Set<TaskId> statefulTasks =
            mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_2_0, TASK_2_1, TASK_2_3);

        expect(client1.capacity()).andReturn(1);
        expect(client1.prevActiveTasks()).andReturn(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3));

        expect(client2.capacity()).andReturn(1);
        expect(client2.prevActiveTasks()).andReturn(mkSet(TASK_1_0, TASK_1_1));

        expect(client3.capacity()).andReturn(1);
        expect(client3.prevActiveTasks()).andReturn(mkSet(TASK_2_0, TASK_2_1, TASK_2_3));

        replay(client1, client2, client3);
        assertThat(computeBalanceFactor(states, statefulTasks), equalTo(2));
    }

    @Test
    public void shouldComputeBalanceFactorWithDifferentClientCapacities() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client2 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client3 = EasyMock.createNiceMock(ClientState.class);
        final Set<ClientState> states = mkSet(client1, client2, client3);
        final Set<TaskId> statefulTasks =
            mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_2_0, TASK_2_1, TASK_2_3);

        // client 1: 4 tasks per thread
        expect(client1.capacity()).andReturn(1);
        expect(client1.prevActiveTasks()).andReturn(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3));

        // client 2: 1 task per thread
        expect(client2.capacity()).andReturn(2);
        expect(client2.prevActiveTasks()).andReturn(mkSet(TASK_1_0, TASK_1_1));

        // client 3: 1 task per thread
        expect(client3.capacity()).andReturn(3);
        expect(client3.prevActiveTasks()).andReturn(mkSet(TASK_2_0, TASK_2_1, TASK_2_3));

        replay(client1, client2, client3);
        assertThat(computeBalanceFactor(states, statefulTasks), equalTo(3));
    }

    @Test
    public void shouldComputeBalanceFactorBasedOnStatefulTasksOnly() {
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client2 = EasyMock.createNiceMock(ClientState.class);
        final ClientState client3 = EasyMock.createNiceMock(ClientState.class);
        final Set<ClientState> states = mkSet(client1, client2, client3);

        // 0_0 and 0_1 are stateless
        final Set<TaskId> statefulTasks = mkSet(TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_2_0, TASK_2_1, TASK_2_3);

        // client 1: 2 stateful tasks per thread
        expect(client1.capacity()).andReturn(1);
        expect(client1.prevActiveTasks()).andReturn(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3));

        // client 2: 1 stateful task per thread
        expect(client2.capacity()).andReturn(2);
        expect(client2.prevActiveTasks()).andReturn(mkSet(TASK_1_0, TASK_1_1));

        // client 3: 1 stateful task per thread
        expect(client3.capacity()).andReturn(3);
        expect(client3.prevActiveTasks()).andReturn(mkSet(TASK_2_0, TASK_2_1, TASK_2_3));

        replay(client1, client2, client3);
        assertThat(computeBalanceFactor(states, statefulTasks), equalTo(1));
    }

    @Test
    public void shouldComputeBalanceFactorOfZeroWithOnlyOneClient() {
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final ClientState client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.capacity()).andReturn(1);
        expect(client1.prevActiveTasks()).andReturn(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3));
        replay(client1);
        assertThat(computeBalanceFactor(singleton(client1), statefulTasks), equalTo(0));
    }

    @Test
    public void shouldAssignStandbysForStatefulTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1);

        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0), statefulTasks);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_1), statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithStandbys);


        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0)));
        assertThat(client2.activeTasks(), equalTo(mkSet(TASK_0_1)));
        assertThat(client1.standbyTasks(), equalTo(mkSet(TASK_0_1)));
        assertThat(client2.standbyTasks(), equalTo(mkSet(TASK_0_0)));
        assertThat(probingRebalanceNeeded, is(false));
    }

    @Test
    public void shouldNotAssignStandbysForStatelessTasks() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;

        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithStandbys);


        assertThat(client1.activeTaskCount(), equalTo(1));
        assertThat(client2.activeTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1, client2);
        assertThat(probingRebalanceNeeded, is(false));
    }

    @Test
    public void shouldAssignWarmupReplicasEvenIfNoStandbyReplicasConfigured() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1), statefulTasks);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithoutStandbys);


        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
        assertThat(client2.standbyTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
        assertThat(probingRebalanceNeeded, is(true));
    }


    @Test
    public void shouldNotAssignMoreThanMaxWarmupReplicas() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3), statefulTasks);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTasks,
            statefulTasks,
            new AssignmentConfigs(
                /*acceptableRecoveryLag*/ 100L,
                /*maxWarmupReplicas*/ 1,
                /*numStandbyReplicas*/ 0,
                /*probingRebalanceIntervalMs*/ 60 * 1000L
            )
        );


        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertThat(client2.standbyTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
        assertThat(probingRebalanceNeeded, is(true));
    }

    @Test
    public void shouldNotAssignWarmupAndStandbyToTheSameClient() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3), statefulTasks);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded = new HighAvailabilityTaskAssignor().assign(
            clientStates,
            allTasks,
            statefulTasks,
            new AssignmentConfigs(
                /*acceptableRecoveryLag*/ 100L,
                /*maxWarmupReplicas*/ 1,
                /*numStandbyReplicas*/ 1,
                /*probingRebalanceIntervalMs*/ 60 * 1000L
            )
        );

        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertThat(client2.standbyTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
        assertThat(probingRebalanceNeeded, is(true));
    }

    @Test
    public void shouldNotAssignAnyStandbysWithInsufficientCapacity() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1), statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1);
        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithStandbys);

        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
        assertHasNoStandbyTasks(client1);
        assertThat(probingRebalanceNeeded, is(false));
    }

    @Test
    public void shouldAssignActiveTasksToNotCaughtUpClientIfNoneExist() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithStandbys);
        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
        assertHasNoStandbyTasks(client1);
        assertThat(probingRebalanceNeeded, is(false));
    }

    @Test
    public void shouldNotAssignMoreThanMaxWarmupReplicasWithStandbys() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(statefulTasks, statefulTasks);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);
        final ClientState client3 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithStandbys);
        assertThat(client1.activeTaskCount(), equalTo(4));
        assertThat(client2.standbyTaskCount(), equalTo(3)); // 1
        assertThat(client3.standbyTaskCount(), equalTo(3));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2, client3);
        assertThat(probingRebalanceNeeded, is(true));
    }

    @Test
    public void shouldDistributeStatelessTasksToBalanceTotalTaskLoad() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);

        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(statefulTasks, statefulTasks);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithStandbys);
        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_2)));
        assertHasNoStandbyTasks(client1);
        assertThat(client2.activeTasks(), equalTo(mkSet(TASK_1_1)));
        assertThat(client2.standbyTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertThat(probingRebalanceNeeded, is(true));
    }

    @Test
    public void shouldDistributeStatefulActiveTasksToAllClients() {
        final Set<TaskId> allTasks =
            mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3, TASK_2_0); // 9 total
        final Map<TaskId, Long> allTaskLags = allTasks.stream().collect(Collectors.toMap(t -> t, t -> 0L));
        final Set<TaskId> statefulTasks = new HashSet<>(allTasks);
        final ClientState client1 = new ClientState(emptySet(), emptySet(), allTaskLags, 100);
        final ClientState client2 = new ClientState(emptySet(), emptySet(), allTaskLags, 50);
        final ClientState client3 = new ClientState(emptySet(), emptySet(), allTaskLags, 1);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2, client3);

        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithoutStandbys);

        assertThat(client1.activeTasks(), not(empty()));
        assertThat(client2.activeTasks(), not(empty()));
        assertThat(client3.activeTasks(), not(empty()));
        assertThat(probingRebalanceNeeded, is(false));
    }

    @Test
    public void shouldReturnFalseIfPreviousAssignmentIsReused() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = new HashSet<>(allTasks);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_2), statefulTasks);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_1, TASK_0_3), statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithoutStandbys);

        assertThat(probingRebalanceNeeded, is(false));
        assertThat(client1.activeTasks(), equalTo(client1.prevActiveTasks()));
        assertThat(client2.activeTasks(), equalTo(client2.prevActiveTasks()));
    }

    @Test
    public void shouldReturnFalseIfNoWarmupTasksAreAssigned() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        final Set<TaskId> statefulTasks = EMPTY_TASKS;
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithoutStandbys);
        assertThat(probingRebalanceNeeded, is(false));
        assertHasNoStandbyTasks(client1, client2);
    }

    @Test
    public void shouldReturnTrueIfWarmupTasksAreAssigned() {
        final Set<TaskId> allTasks = mkSet(TASK_0_0, TASK_0_1);
        final Set<TaskId> statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        final ClientState client1 = getMockClientWithPreviousCaughtUpTasks(allTasks, statefulTasks);
        final ClientState client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS, statefulTasks);

        final Map<UUID, ClientState> clientStates = getClientStatesMap(client1, client2);
        final boolean probingRebalanceNeeded =
            new HighAvailabilityTaskAssignor().assign(clientStates, allTasks, statefulTasks, configWithoutStandbys);
        assertThat(probingRebalanceNeeded, is(true));
        assertThat(client2.standbyTaskCount(), equalTo(1));
    }

    private static void assertHasNoActiveTasks(final ClientState... clients) {
        for (final ClientState client : clients) {
            assertThat(client.activeTasks(), is(empty()));
        }
    }

    private static void assertHasNoStandbyTasks(final ClientState... clients) {
        for (final ClientState client : clients) {
            assertThat(client.standbyTasks(), is(empty()));
        }
    }

    private static ClientState getMockClientWithPreviousCaughtUpTasks(final Set<TaskId> statefulActiveTasks,
                                                                      final Set<TaskId> statefulTasks) {
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
        return new ClientState(statefulActiveTasks, emptySet(), taskLags, 1);
    }
}
