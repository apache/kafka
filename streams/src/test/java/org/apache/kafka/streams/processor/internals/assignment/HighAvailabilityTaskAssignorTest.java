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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.easymock.EasyMock;
import org.junit.Test;

public class HighAvailabilityTaskAssignorTest {
    private long acceptableRecoveryLag = 100L;
    private int balanceFactor = 1;
    private int maxWarmupReplicas = 2;
    private int numStandbyReplicas = 0;
    private long probingRebalanceInterval = 60 * 1000L;

    private Map<UUID, ClientState> clientStates = new HashMap<>();
    private Set<TaskId> allTasks = new HashSet<>();
    private Set<TaskId> statefulTasks = new HashSet<>();

    private ClientState client1;
    private ClientState client2;
    private ClientState client3;
    
    private HighAvailabilityTaskAssignor taskAssignor;

    private void createTaskAssignor() {
        final AssignmentConfigs configs = new AssignmentConfigs(
            acceptableRecoveryLag,
            balanceFactor,
            maxWarmupReplicas,
            numStandbyReplicas,
            probingRebalanceInterval
        );
        taskAssignor = new HighAvailabilityTaskAssignor(
            clientStates,
            allTasks,
            statefulTasks,
            configs);
    }

    @Test
    public void shouldDecidePreviousAssignmentIsInvalidIfThereAreUnassignedActiveTasks() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.prevActiveTasks()).andReturn(singleton(TASK_0_0));
        expect(client1.prevStandbyTasks()).andStubReturn(EMPTY_TASKS);
        replay(client1);
        allTasks =  mkSet(TASK_0_0, TASK_0_1);
        clientStates = singletonMap(UUID_1, client1);
        createTaskAssignor();

        assertFalse(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldDecidePreviousAssignmentIsInvalidIfThereAreUnassignedStandbyTasks() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.prevActiveTasks()).andStubReturn(singleton(TASK_0_0));
        expect(client1.prevStandbyTasks()).andReturn(EMPTY_TASKS);
        replay(client1);
        allTasks =  mkSet(TASK_0_0);
        statefulTasks =  mkSet(TASK_0_0);
        clientStates = singletonMap(UUID_1, client1);
        numStandbyReplicas = 1;
        createTaskAssignor();

        assertFalse(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldDecidePreviousAssignmentIsInvalidIfActiveTasksWasNotOnCaughtUpClient() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.prevStandbyTasks()).andStubReturn(EMPTY_TASKS);
        expect(client2.prevStandbyTasks()).andStubReturn(EMPTY_TASKS);

        expect(client1.prevActiveTasks()).andReturn(singleton(TASK_0_0));
        expect(client2.prevActiveTasks()).andReturn(singleton(TASK_0_1));
        expect(client1.lagFor(TASK_0_0)).andReturn(500L);
        expect(client2.lagFor(TASK_0_0)).andReturn(0L);
        replay(client1, client2);

        allTasks =  mkSet(TASK_0_0, TASK_0_1);
        statefulTasks =  mkSet(TASK_0_0);
        clientStates = mkMap(
            mkEntry(UUID_1, client1),
            mkEntry(UUID_2, client2)
        );
        createTaskAssignor();

        assertFalse(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldDecidePreviousAssignmentIsValid() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.prevStandbyTasks()).andStubReturn(EMPTY_TASKS);
        expect(client2.prevStandbyTasks()).andStubReturn(EMPTY_TASKS);

        expect(client1.prevActiveTasks()).andReturn(singleton(TASK_0_0));
        expect(client2.prevActiveTasks()).andReturn(singleton(TASK_0_1));
        expect(client1.lagFor(TASK_0_0)).andReturn(0L);
        expect(client2.lagFor(TASK_0_0)).andReturn(0L);
        replay(client1, client2);

        allTasks =  mkSet(TASK_0_0, TASK_0_1);
        statefulTasks =  mkSet(TASK_0_0);
        clientStates = mkMap(
            mkEntry(UUID_1, client1),
            mkEntry(UUID_2, client2)
        );
        createTaskAssignor();

        assertTrue(taskAssignor.previousAssignmentIsValid());
    }

    @Test
    public void shouldComputeBalanceFactorAsDifferenceBetweenMostAndLeastLoadedClients() {
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        client3 = EasyMock.createNiceMock(ClientState.class);
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
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        client3 = EasyMock.createNiceMock(ClientState.class);
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
        client1 = EasyMock.createNiceMock(ClientState.class);
        client2 = EasyMock.createNiceMock(ClientState.class);
        client3 = EasyMock.createNiceMock(ClientState.class);
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
        client1 = EasyMock.createNiceMock(ClientState.class);
        expect(client1.capacity()).andReturn(1);
        expect(client1.prevActiveTasks()).andReturn(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3));
        replay(client1);
        assertThat(computeBalanceFactor(singleton(client1), statefulTasks), equalTo(0));
    }

    @Test
    public void shouldAssignStandbysForStatefulTasks() {
        numStandbyReplicas = 1;
        allTasks = mkSet(TASK_0_0, TASK_0_1);
        statefulTasks = mkSet(TASK_0_0, TASK_0_1);

        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0));
        client2 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_1));

        clientStates = getClientStatesMap(client1, client2);
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0)));
        assertThat(client2.activeTasks(), equalTo(mkSet(TASK_0_1)));
        assertThat(client1.standbyTasks(), equalTo(mkSet(TASK_0_1)));
        assertThat(client2.standbyTasks(), equalTo(mkSet(TASK_0_0)));
    }

    @Test
    public void shouldNotAssignStandbysForStatelessTasks() {
        numStandbyReplicas = 1;
        allTasks = mkSet(TASK_0_0, TASK_0_1);
        statefulTasks = EMPTY_TASKS;

        client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);
        client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);

        clientStates = getClientStatesMap(client1, client2);
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTaskCount(), equalTo(1));
        assertThat(client2.activeTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1, client2);
    }

    @Test
    public void shouldAssignWarmupReplicasEvenIfNoStandbyReplicasConfigured() {
        allTasks = mkSet(TASK_0_0, TASK_0_1);
        statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1));
        client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);

        clientStates = getClientStatesMap(client1, client2);
        createTaskAssignor();
        taskAssignor.assign();
        
        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
        assertThat(client2.standbyTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
    }



    @Test
    public void shouldNotAssignMoreThanMaxWarmupReplicas() {
        maxWarmupReplicas = 1;
        allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3));
        client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);

        clientStates = getClientStatesMap(client1, client2);
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertThat(client2.standbyTaskCount(), equalTo(1));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
    }

    @Test
    public void shouldNotAssignWarmupAndStandbyToTheSameClient() {
        numStandbyReplicas = 1;
        maxWarmupReplicas = 1;

        allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3));
        client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);

        clientStates = getClientStatesMap(client1, client2);
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertThat(client2.standbyTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2);
    }

    @Test
    public void shouldNotAssignAnyStandbysWithInsufficientCapacity() {
        numStandbyReplicas = 1;
        allTasks = mkSet(TASK_0_0, TASK_0_1);
        statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1));

        clientStates = getClientStatesMap(client1);
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
        assertHasNoStandbyTasks(client1);
    }

    @Test
    public void shouldAssignActiveTasksToNotCaughtUpClientIfNoneExist() {
        numStandbyReplicas = 1;
        allTasks = mkSet(TASK_0_0, TASK_0_1);
        statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);

        clientStates = getClientStatesMap(client1);
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
        assertHasNoStandbyTasks(client1);
    }

    @Test
    public void shouldNotAssignMoreThanMaxWarmupReplicasWithStandbys() {
        numStandbyReplicas = 1;

        allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3));
        client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);
        client3 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);

        clientStates = getClientStatesMap(client1, client2, client3);
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTaskCount(), equalTo(4));
        assertThat(client2.standbyTaskCount(), equalTo(3)); // 1
        assertThat(client3.standbyTaskCount(), equalTo(3));
        assertHasNoStandbyTasks(client1);
        assertHasNoActiveTasks(client2, client3);
    }

    @Test
    public void shouldDistributeStatelessTasksToBalanceTotalTaskLoad() {
        numStandbyReplicas = 1;
        allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2);
        statefulTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);

        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3));
        client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);

        clientStates = getClientStatesMap(client1, client2);
        createTaskAssignor();
        taskAssignor.assign();

        assertThat(client1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_2)));
        assertHasNoStandbyTasks(client1);
        assertThat(client2.activeTasks(), equalTo(mkSet(TASK_1_1)));
        assertThat(client2.standbyTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
    }

    @Test
    public void shouldDistributeStatefulActiveTasksToAllClients() {
        allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3, TASK_2_0); // 9 total
        statefulTasks = new HashSet<>(allTasks);
        client1 = getMockClientWithPreviousCaughtUpTasks(allTasks).withCapacity(100);
        client2 = getMockClientWithPreviousCaughtUpTasks(allTasks).withCapacity(50);
        client3 = getMockClientWithPreviousCaughtUpTasks(allTasks).withCapacity(1);

        clientStates = getClientStatesMap(client1, client2, client3);
        createTaskAssignor();
        taskAssignor.assign();

        assertFalse(client1.activeTasks().isEmpty());
        assertFalse(client2.activeTasks().isEmpty());
        assertFalse(client3.activeTasks().isEmpty());
    }

    @Test
    public void shouldReturnFalseIfPreviousAssignmentIsReused() {
        allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        statefulTasks = new HashSet<>(allTasks);
        client1 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_0, TASK_0_2));
        client2 = getMockClientWithPreviousCaughtUpTasks(mkSet(TASK_0_1, TASK_0_3));

        clientStates = getClientStatesMap(client1, client2);
        createTaskAssignor();
        assertFalse(taskAssignor.assign());

        assertThat(client1.activeTasks(), equalTo(client1.prevActiveTasks()));
        assertThat(client2.activeTasks(), equalTo(client2.prevActiveTasks()));
    }

    @Test
    public void shouldReturnFalseIfNoWarmupTasksAreAssigned() {
        allTasks = mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        statefulTasks = EMPTY_TASKS;
        client1 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);
        client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);

        clientStates = getClientStatesMap(client1, client2);
        createTaskAssignor();
        assertFalse(taskAssignor.assign());
        assertHasNoStandbyTasks(client1, client2);
    }

    @Test
    public void shouldReturnTrueIfWarmupTasksAreAssigned() {
        allTasks = mkSet(TASK_0_0, TASK_0_1);
        statefulTasks = mkSet(TASK_0_0, TASK_0_1);
        client1 = getMockClientWithPreviousCaughtUpTasks(allTasks);
        client2 = getMockClientWithPreviousCaughtUpTasks(EMPTY_TASKS);

        clientStates = getClientStatesMap(client1, client2);
        createTaskAssignor();
        assertTrue(taskAssignor.assign());
        assertThat(client2.standbyTaskCount(), equalTo(1));
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

    private MockClientState getMockClientWithPreviousCaughtUpTasks(final Set<TaskId> statefulActiveTasks) {
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
