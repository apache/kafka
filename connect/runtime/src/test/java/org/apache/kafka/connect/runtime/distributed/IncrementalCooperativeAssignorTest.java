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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.ConnectorsAndTasks;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.runtime.distributed.ExtendedAssignment.duplicate;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V2;
import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class IncrementalCooperativeAssignorTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private WorkerCoordinator coordinator;

    @Captor
    ArgumentCaptor<Map<String, ExtendedAssignment>> assignmentsCapture;

    @Parameters
    public static Iterable<?> mode() {
        return Arrays.asList(new Object[][] {{CONNECT_PROTOCOL_V1, CONNECT_PROTOCOL_V2}});
    }

    @Parameter
    public short protocolVersion;

    private ClusterConfigState configState;
    private Map<String, ExtendedWorkerState> memberConfigs;
    private long offset;
    private String leader;
    private String leaderUrl;
    private Time time;
    private int rebalanceDelay;
    private IncrementalCooperativeAssignor assignor;
    private int rebalanceNum;
    Map<String, ExtendedAssignment> returnedAssignments;

    @Before
    public void setup() {
        leader = "worker1";
        leaderUrl = expectedLeaderUrl(leader);
        offset = 10;
        configState = clusterConfigState(offset, 2, 4);
        memberConfigs = memberConfigs(leader, offset, 1, 1);
        time = Time.SYSTEM;
        rebalanceDelay = DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT;
        initAssignor();
    }

    @After
    public void teardown() {
        verifyNoMoreInteractions(coordinator);
    }

    public void initAssignor() {
        assignor = Mockito.spy(new IncrementalCooperativeAssignor(
                new LogContext(),
                time,
                rebalanceDelay));
        assignor.previousGenerationId = 1000;
    }

    @Test
    public void testTaskAssignmentWhenWorkerJoins() {
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1");
        assertBalancedAndCompleteAllocation();

        // Second assignment with a second worker joining and all connectors running on previous worker
        addNewWorkers("worker2");
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 1, 4, "worker1", "worker2");

        // Third assignment after revocations
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(1, 4, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        // A fourth rebalance should not change assignments
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        verifyCoordinatorInteractions();
    }

    @Test
    public void testTaskAssignmentWhenWorkerLeavesPermanently() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        addNewWorkers("worker2");
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        // Second assignment with only one worker remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and the count
        // down for the rebalance delay starts
        removeWorkers("worker2");
        performStandardRebalance();
        assertDelay(rebalanceDelay, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 2);

        // Third (incidental) assignment with still only one worker in the group. Max delay has not
        // been reached yet
        performStandardRebalance();
        assertDelay(rebalanceDelay / 2, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 2 + 1);

        // Fourth assignment after delay expired
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(1, 4, 0, 0, "worker1");
        assertBalancedAndCompleteAllocation();

        verifyCoordinatorInteractions();
    }

    @Test
    public void testTaskAssignmentWhenWorkerBounces() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        addNewWorkers("worker2");
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        // Second assignment with only one worker remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and the count
        // down for the rebalance delay starts
        removeWorkers("worker2");
        performStandardRebalance();
        assertDelay(rebalanceDelay, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 2);

        // Third (incidental) assignment with still only one worker in the group. Max delay has not
        // been reached yet
        performStandardRebalance();
        assertDelay(rebalanceDelay / 2, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(rebalanceDelay / 4);

        // Fourth assignment with the second worker returning before the delay expires
        // Since the delay is still active, lost assignments are not reassigned yet
        addNewWorkers("worker2");
        performStandardRebalance();
        assertDelay(rebalanceDelay / 4, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");

        time.sleep(rebalanceDelay / 4);

        // Fifth assignment with the same two workers. The delay has expired, so the lost
        // assignments ought to be assigned to the worker that has appeared as returned.
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(1, 4, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        verifyCoordinatorInteractions();
    }

    @Test
    public void testTaskAssignmentWhenLeaderLeavesPermanently() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 3 workers and 2 connectors configured but not yet assigned
        addNewWorkers("worker2", "worker3");
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2", "worker3");
        assertBalancedAndCompleteAllocation();

        // Second assignment with two workers remaining in the group. The worker that left the
        // group was the leader. The new leader has no previous assignments and is not tracking a
        // delay upon a leader's exit
        removeWorkers("worker1");
        leader = "worker2";
        leaderUrl = expectedLeaderUrl(leader);
        // The fact that the leader bounces means that the assignor starts from a clean slate
        initAssignor();

        // Capture needs to be reset to point to the new assignor
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(1, 3, 0, 0, "worker2", "worker3");
        assertBalancedAndCompleteAllocation();

        // Third (incidental) assignment with still only one worker in the group.
        performStandardRebalance();
        assertAssignment(0, 0, 0, 0, "worker2", "worker3");
        assertBalancedAndCompleteAllocation();

        verifyCoordinatorInteractions();
    }

    @Test
    public void testTaskAssignmentWhenLeaderBounces() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 3 workers and 2 connectors configured but not yet assigned
        addNewWorkers("worker2", "worker3");
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2", "worker3");
        assertBalancedAndCompleteAllocation();

        // Second assignment with two workers remaining in the group. The worker that left the
        // group was the leader. The new leader has no previous assignments and is not tracking a
        // delay upon a leader's exit
        removeWorkers("worker1");
        leader = "worker2";
        leaderUrl = expectedLeaderUrl(leader);
        // The fact that the leader bounces means that the assignor starts from a clean slate
        initAssignor();

        // Capture needs to be reset to point to the new assignor
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(1, 3, 0, 0, "worker2", "worker3");
        assertBalancedAndCompleteAllocation();

        // Third assignment with the previous leader returning as a follower. In this case, the
        // arrival of the previous leader is treated as an arrival of a new worker. Reassignment
        // happens immediately, first with a revocation
        addNewWorkers("worker1");
        performStandardRebalance();
        assertAssignment(0, 0, 0, 2, "worker1", "worker2", "worker3");

        // Fourth assignment after revocations
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 2, 0, 0, "worker1", "worker2", "worker3");
        assertBalancedAndCompleteAllocation();

        verifyCoordinatorInteractions();
    }

    @Test
    public void testTaskAssignmentWhenFirstAssignmentAttemptFails() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doThrow(new RuntimeException("Unable to send computed assignment with SyncGroupRequest"))
                .when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        addNewWorkers("worker2");
        performFailedRebalance();
        // This was the assignment that should have been sent, but didn't make it all the way
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");

        // Second assignment happens with members returning the same assignments (memberConfigs)
        // as the first time. The assignor detects that the number of members did not change and
        // avoids the rebalance delay, treating the lost assignments as new assignments.
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        verifyCoordinatorInteractions();
    }

    @Test
    public void testTaskAssignmentWhenSubsequentAssignmentAttemptFails() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        addNewWorkers("worker2");
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doThrow(new RuntimeException("Unable to send computed assignment with SyncGroupRequest"))
                .when(assignor).serializeAssignments(assignmentsCapture.capture());

        // Second assignment triggered by a third worker joining. The computed assignment should
        // revoke tasks from the existing group. But the assignment won't be correctly delivered.
        addNewWorkers("worker3");
        performFailedRebalance();
        // This was the assignment that should have been sent, but didn't make it all the way
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 0, 2, "worker1", "worker2", "worker3");

        // Third assignment happens with members returning the same assignments (memberConfigs)
        // as the first time.
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 0, 2, "worker1", "worker2", "worker3");

        // Fourth assignment after revocations
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 2, 0, 0, "worker1", "worker2", "worker3");
        assertBalancedAndCompleteAllocation();

        verifyCoordinatorInteractions();
    }

    @Test
    public void testTaskAssignmentWhenSubsequentAssignmentAttemptFailsOutsideTheAssignor() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        addNewWorkers("worker2");
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        // Second assignment triggered by a third worker joining. The computed assignment should
        // revoke tasks from the existing group. But the assignment won't be correctly delivered
        // and sync group with fail on the leader worker.
        addNewWorkers("worker3");
        performFailedRebalance();
        // This was the assignment that should have been sent, but didn't make it all the way
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 0, 2, "worker1", "worker2", "worker3");

        // Third assignment happens with members returning the same assignments (memberConfigs)
        // as the first time.
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        performRebalanceWithMismatchedGeneration();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 0, 2, "worker1", "worker2", "worker3");

        // Fourth assignment after revocations
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 2, 0, 0, "worker1", "worker2", "worker3");
        assertBalancedAndCompleteAllocation();

        verifyCoordinatorInteractions();
    }

    @Test
    public void testTaskAssignmentWhenConnectorsAreDeleted() {
        configState = clusterConfigState(offset, 3, 4);
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        addNewWorkers("worker2");
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(3, 12, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        // Second assignment with an updated config state that reflects removal of a connector
        configState = clusterConfigState(offset + 1, 2, 4);
        when(coordinator.configSnapshot()).thenReturn(configState);
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 1, 4, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        verifyCoordinatorInteractions();
    }

    @Test
    public void testAssignConnectorsWhenBalanced() {
        int num = 2;
        List<WorkerLoad> existingAssignment = IntStream.range(0, 3)
                .mapToObj(i -> workerLoad("worker" + i, i * num, num, i * num, num))
                .collect(Collectors.toList());

        List<WorkerLoad> expectedAssignment = existingAssignment.stream()
                .map(wl -> new WorkerLoad.Builder(wl.worker()).withCopies(wl.connectors(), wl.tasks()).build())
                .collect(Collectors.toList());
        expectedAssignment.get(0).connectors().addAll(Arrays.asList("connector6", "connector9"));
        expectedAssignment.get(1).connectors().addAll(Arrays.asList("connector7", "connector10"));
        expectedAssignment.get(2).connectors().addAll(Arrays.asList("connector8"));

        List<String> newConnectors = newConnectors(6, 11);
        assignor.assignConnectors(existingAssignment, newConnectors);
        assertEquals(expectedAssignment, existingAssignment);
    }

    @Test
    public void testAssignTasksWhenBalanced() {
        int num = 2;
        List<WorkerLoad> existingAssignment = IntStream.range(0, 3)
                .mapToObj(i -> workerLoad("worker" + i, i * num, num, i * num, num))
                .collect(Collectors.toList());

        List<WorkerLoad> expectedAssignment = existingAssignment.stream()
                .map(wl -> new WorkerLoad.Builder(wl.worker()).withCopies(wl.connectors(), wl.tasks()).build())
                .collect(Collectors.toList());

        expectedAssignment.get(0).connectors().addAll(Arrays.asList("connector6", "connector9"));
        expectedAssignment.get(1).connectors().addAll(Arrays.asList("connector7", "connector10"));
        expectedAssignment.get(2).connectors().addAll(Arrays.asList("connector8"));

        expectedAssignment.get(0).tasks().addAll(Arrays.asList(new ConnectorTaskId("task", 6), new ConnectorTaskId("task", 9)));
        expectedAssignment.get(1).tasks().addAll(Arrays.asList(new ConnectorTaskId("task", 7), new ConnectorTaskId("task", 10)));
        expectedAssignment.get(2).tasks().addAll(Arrays.asList(new ConnectorTaskId("task", 8)));

        List<String> newConnectors = newConnectors(6, 11);
        assignor.assignConnectors(existingAssignment, newConnectors);
        List<ConnectorTaskId> newTasks = newTasks(6, 11);
        assignor.assignTasks(existingAssignment, newTasks);
        assertEquals(expectedAssignment, existingAssignment);
    }

    @Test
    public void testAssignConnectorsWhenImbalanced() {
        List<WorkerLoad> existingAssignment = new ArrayList<>();
        existingAssignment.add(workerLoad("worker0", 0, 2, 0, 2));
        existingAssignment.add(workerLoad("worker1", 2, 3, 2, 3));
        existingAssignment.add(workerLoad("worker2", 5, 4, 5, 4));
        existingAssignment.add(emptyWorkerLoad("worker3"));

        List<String> newConnectors = newConnectors(9, 24);
        List<ConnectorTaskId> newTasks = newTasks(9, 24);
        assignor.assignConnectors(existingAssignment, newConnectors);
        assignor.assignTasks(existingAssignment, newTasks);
        for (WorkerLoad worker : existingAssignment) {
            assertEquals(6, worker.connectorsSize());
            assertEquals(6, worker.tasksSize());
        }
    }

    @Test
    public void testLostAssignmentHandlingWhenWorkerBounces() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        assertTrue(assignor.candidateWorkersForReassignment.isEmpty());
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        Map<String, WorkerLoad> configuredAssignment = new HashMap<>();
        configuredAssignment.put("worker0", workerLoad("worker0", 0, 2, 0, 4));
        configuredAssignment.put("worker1", workerLoad("worker1", 2, 2, 4, 4));
        configuredAssignment.put("worker2", workerLoad("worker2", 4, 2, 8, 4));
        memberConfigs = memberConfigs(leader, offset, 0, 2);

        ConnectorsAndTasks newSubmissions = new ConnectorsAndTasks.Builder().build();

        // No lost assignments
        assignor.handleLostAssignments(new ConnectorsAndTasks.Builder().build(),
                newSubmissions,
                new ArrayList<>(configuredAssignment.values()),
                memberConfigs);

        assertThat("Wrong set of workers for reassignments",
                Collections.emptySet(),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberConfigs.keySet());
        String flakyWorker = "worker1";
        WorkerLoad lostLoad = workerLoad(flakyWorker, 2, 2, 4, 4);
        removeWorkers(flakyWorker);

        ConnectorsAndTasks lostAssignments = new ConnectorsAndTasks.Builder()
                .withCopies(lostLoad.connectors(), lostLoad.tasks()).build();

        // Lost assignments detected - No candidate worker has appeared yet (worker with no assignments)
        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        assertThat("Wrong set of workers for reassignments",
                Collections.emptySet(),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberConfigs.keySet());
        time.sleep(rebalanceDelay / 2);
        rebalanceDelay /= 2;

        // A new worker (probably returning worker) has joined
        configuredAssignment.put(flakyWorker, new WorkerLoad.Builder(flakyWorker).build());
        memberConfigs.put(flakyWorker, new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        assertThat("Wrong set of workers for reassignments",
                Collections.singleton(flakyWorker),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberConfigs.keySet());
        time.sleep(rebalanceDelay);

        // The new worker has still no assignments
        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        assertTrue("Wrong assignment of lost connectors",
                configuredAssignment.getOrDefault(flakyWorker, new WorkerLoad.Builder(flakyWorker).build())
                        .connectors()
                        .containsAll(lostAssignments.connectors()));
        assertTrue("Wrong assignment of lost tasks",
                configuredAssignment.getOrDefault(flakyWorker, new WorkerLoad.Builder(flakyWorker).build())
                        .tasks()
                        .containsAll(lostAssignments.tasks()));
        assertThat("Wrong set of workers for reassignments",
                Collections.emptySet(),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);
    }

    @Test
    public void testLostAssignmentHandlingWhenWorkerLeavesPermanently() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        assertTrue(assignor.candidateWorkersForReassignment.isEmpty());
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        Map<String, WorkerLoad> configuredAssignment = new HashMap<>();
        configuredAssignment.put("worker0", workerLoad("worker0", 0, 2, 0, 4));
        configuredAssignment.put("worker1", workerLoad("worker1", 2, 2, 4, 4));
        configuredAssignment.put("worker2", workerLoad("worker2", 4, 2, 8, 4));
        memberConfigs = memberConfigs(leader, offset, 0, 2);

        ConnectorsAndTasks newSubmissions = new ConnectorsAndTasks.Builder().build();

        // No lost assignments
        assignor.handleLostAssignments(new ConnectorsAndTasks.Builder().build(),
                newSubmissions,
                new ArrayList<>(configuredAssignment.values()),
                memberConfigs);

        assertThat("Wrong set of workers for reassignments",
                Collections.emptySet(),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberConfigs.keySet());
        String removedWorker = "worker1";
        WorkerLoad lostLoad = workerLoad(removedWorker, 2, 2, 4, 4);
        removeWorkers(removedWorker);

        ConnectorsAndTasks lostAssignments = new ConnectorsAndTasks.Builder()
                .withCopies(lostLoad.connectors(), lostLoad.tasks()).build();

        // Lost assignments detected - No candidate worker has appeared yet (worker with no assignments)
        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        assertThat("Wrong set of workers for reassignments",
                Collections.emptySet(),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberConfigs.keySet());
        time.sleep(rebalanceDelay / 2);
        rebalanceDelay /= 2;

        // No new worker has joined
        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        assertThat("Wrong set of workers for reassignments",
                Collections.emptySet(),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        time.sleep(rebalanceDelay);

        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        assertTrue("Wrong assignment of lost connectors",
                newSubmissions.connectors().containsAll(lostAssignments.connectors()));
        assertTrue("Wrong assignment of lost tasks",
                newSubmissions.tasks().containsAll(lostAssignments.tasks()));
        assertThat("Wrong set of workers for reassignments",
                Collections.emptySet(),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);
    }

    @Test
    public void testLostAssignmentHandlingWithMoreThanOneCandidates() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        assertTrue(assignor.candidateWorkersForReassignment.isEmpty());
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        Map<String, WorkerLoad> configuredAssignment = new HashMap<>();
        configuredAssignment.put("worker0", workerLoad("worker0", 0, 2, 0, 4));
        configuredAssignment.put("worker1", workerLoad("worker1", 2, 2, 4, 4));
        configuredAssignment.put("worker2", workerLoad("worker2", 4, 2, 8, 4));
        memberConfigs = memberConfigs(leader, offset, 0, 2);

        ConnectorsAndTasks newSubmissions = new ConnectorsAndTasks.Builder().build();

        // No lost assignments
        assignor.handleLostAssignments(new ConnectorsAndTasks.Builder().build(),
                newSubmissions,
                new ArrayList<>(configuredAssignment.values()),
                memberConfigs);

        assertThat("Wrong set of workers for reassignments",
                Collections.emptySet(),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberConfigs.keySet());
        String flakyWorker = "worker1";
        WorkerLoad lostLoad = workerLoad(flakyWorker, 2, 2, 4, 4);
        removeWorkers(flakyWorker);
        String newWorker = "worker3";

        ConnectorsAndTasks lostAssignments = new ConnectorsAndTasks.Builder()
                .withCopies(lostLoad.connectors(), lostLoad.tasks()).build();

        // Lost assignments detected - A new worker also has joined that is not the returning worker
        configuredAssignment.put(newWorker, new WorkerLoad.Builder(newWorker).build());
        memberConfigs.put(newWorker, new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        assertThat("Wrong set of workers for reassignments",
                Collections.singleton(newWorker),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberConfigs.keySet());
        time.sleep(rebalanceDelay / 2);
        rebalanceDelay /= 2;

        // Now two new workers have joined
        configuredAssignment.put(flakyWorker, new WorkerLoad.Builder(flakyWorker).build());
        memberConfigs.put(flakyWorker, new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        Set<String> expectedWorkers = new HashSet<>();
        expectedWorkers.addAll(Arrays.asList(newWorker, flakyWorker));
        assertThat("Wrong set of workers for reassignments",
                expectedWorkers,
                is(assignor.candidateWorkersForReassignment));
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberConfigs.keySet());
        time.sleep(rebalanceDelay);

        // The new workers have new assignments, other than the lost ones
        configuredAssignment.put(flakyWorker, workerLoad(flakyWorker, 6, 2, 8, 4));
        configuredAssignment.put(newWorker, workerLoad(newWorker, 8, 2, 12, 4));
        // we don't reflect these new assignments in memberConfigs currently because they are not
        // used in handleLostAssignments method
        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        // both the newWorkers would need to be considered for re assignment of connectors and tasks
        List<String> listOfConnectorsInLast2Workers = new ArrayList<>();
        listOfConnectorsInLast2Workers.addAll(configuredAssignment.getOrDefault(newWorker, new WorkerLoad.Builder(flakyWorker).build())
            .connectors());
        listOfConnectorsInLast2Workers.addAll(configuredAssignment.getOrDefault(flakyWorker, new WorkerLoad.Builder(flakyWorker).build())
            .connectors());
        List<ConnectorTaskId> listOfTasksInLast2Workers = new ArrayList<>();
        listOfTasksInLast2Workers.addAll(configuredAssignment.getOrDefault(newWorker, new WorkerLoad.Builder(flakyWorker).build())
            .tasks());
        listOfTasksInLast2Workers.addAll(configuredAssignment.getOrDefault(flakyWorker, new WorkerLoad.Builder(flakyWorker).build())
            .tasks());
        assertTrue("Wrong assignment of lost connectors",
            listOfConnectorsInLast2Workers.containsAll(lostAssignments.connectors()));
        assertTrue("Wrong assignment of lost tasks",
            listOfTasksInLast2Workers.containsAll(lostAssignments.tasks()));
        assertThat("Wrong set of workers for reassignments",
            Collections.emptySet(),
            is(assignor.candidateWorkersForReassignment));
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);
    }

    @Test
    public void testLostAssignmentHandlingWhenWorkerBouncesBackButFinallyLeaves() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        assertTrue(assignor.candidateWorkersForReassignment.isEmpty());
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        Map<String, WorkerLoad> configuredAssignment = new HashMap<>();
        configuredAssignment.put("worker0", workerLoad("worker0", 0, 2, 0, 4));
        configuredAssignment.put("worker1", workerLoad("worker1", 2, 2, 4, 4));
        configuredAssignment.put("worker2", workerLoad("worker2", 4, 2, 8, 4));
        memberConfigs = memberConfigs(leader, offset, 0, 2);

        ConnectorsAndTasks newSubmissions = new ConnectorsAndTasks.Builder().build();

        // No lost assignments
        assignor.handleLostAssignments(new ConnectorsAndTasks.Builder().build(),
                newSubmissions,
                new ArrayList<>(configuredAssignment.values()),
                memberConfigs);

        assertThat("Wrong set of workers for reassignments",
                Collections.emptySet(),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberConfigs.keySet());
        String veryFlakyWorker = "worker1";
        WorkerLoad lostLoad = workerLoad(veryFlakyWorker, 2, 2, 4, 4);
        removeWorkers(veryFlakyWorker);

        ConnectorsAndTasks lostAssignments = new ConnectorsAndTasks.Builder()
                .withCopies(lostLoad.connectors(), lostLoad.tasks()).build();

        // Lost assignments detected - No candidate worker has appeared yet (worker with no assignments)
        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        assertThat("Wrong set of workers for reassignments",
                Collections.emptySet(),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberConfigs.keySet());
        time.sleep(rebalanceDelay / 2);
        rebalanceDelay /= 2;

        // A new worker (probably returning worker) has joined
        configuredAssignment.put(veryFlakyWorker, new WorkerLoad.Builder(veryFlakyWorker).build());
        memberConfigs.put(veryFlakyWorker, new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        assertThat("Wrong set of workers for reassignments",
                Collections.singleton(veryFlakyWorker),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberConfigs.keySet());
        time.sleep(rebalanceDelay);

        // The returning worker leaves permanently after joining briefly during the delay
        configuredAssignment.remove(veryFlakyWorker);
        removeWorkers(veryFlakyWorker);
        assignor.handleLostAssignments(lostAssignments, newSubmissions,
                new ArrayList<>(configuredAssignment.values()), memberConfigs);

        assertTrue("Wrong assignment of lost connectors",
                newSubmissions.connectors().containsAll(lostAssignments.connectors()));
        assertTrue("Wrong assignment of lost tasks",
                newSubmissions.tasks().containsAll(lostAssignments.tasks()));
        assertThat("Wrong set of workers for reassignments",
                Collections.emptySet(),
                is(assignor.candidateWorkersForReassignment));
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);
    }

    @Test
    public void testTaskAssignmentWhenTasksDuplicatedInWorkerAssignment() {
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1");
        assertBalancedAndCompleteAllocation();

        // Second assignment with a second worker with duplicate assignment joining and all connectors running on previous worker
        ExtendedAssignment duplicatedWorkerAssignment = newExpandableAssignment();
        duplicatedWorkerAssignment.connectors().addAll(newConnectors(1, 2));
        duplicatedWorkerAssignment.tasks().addAll(newTasks("connector1", 0, 4));
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, duplicatedWorkerAssignment));
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 2, 8, "worker1", "worker2");

        // Third assignment after revocations
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(1, 4, 0, 2, "worker1", "worker2");

        // fourth rebalance after revocations
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 2, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        // Fifth rebalance should not change assignments
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        verifyCoordinatorInteractions();
    }

    @Test
    public void testDuplicatedAssignmentHandleWhenTheDuplicatedAssignmentsDeleted() {
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1");
        assertBalancedAndCompleteAllocation();

        // Delete connector1
        configState = clusterConfigState(offset, 2, 1, 4);
        when(coordinator.configSnapshot()).thenReturn(configState);

        // Second assignment with a second worker with duplicate assignment joining and the duplicated assignment is deleted at the same time
        ExtendedAssignment duplicatedWorkerAssignment = newExpandableAssignment();
        duplicatedWorkerAssignment.connectors().addAll(newConnectors(1, 2));
        duplicatedWorkerAssignment.tasks().addAll(newTasks("connector1", 0, 4));
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, duplicatedWorkerAssignment));
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 2, 8, "worker1", "worker2");

        // Third assignment after revocations
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 0, 2, "worker1", "worker2");

        // fourth rebalance after revocations
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 2, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        // Fifth rebalance should not change assignments
        performStandardRebalance();
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");
        assertBalancedAndCompleteAllocation();

        verifyCoordinatorInteractions();
    }

    private void performStandardRebalance() {
        performRebalance(false, false);
    }

    private void performFailedRebalance() {
        performRebalance(true, false);
    }

    private void performRebalanceWithMismatchedGeneration() {
        performRebalance(false, true);
    }

    private void performRebalance(boolean assignmentFailure, boolean expectGenerationMismatch) {
        expectGeneration(expectGenerationMismatch);
        // Member configs are tracked by the assignor; create a deep copy here so that modifications to our own memberConfigs field
        // are not accidentally propagated to the one used by the assignor
        Map<String, ExtendedWorkerState> memberConfigsCopy = memberConfigs.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> {
                    ExtendedWorkerState originalWorkerState = e.getValue();
                    return new ExtendedWorkerState(
                            originalWorkerState.url(),
                            originalWorkerState.offset(),
                            duplicate(originalWorkerState.assignment())
                    );
                }
        ));
        try {
            assignor.performTaskAssignment(leader, offset, memberConfigsCopy, coordinator, protocolVersion);
        } catch (RuntimeException e) {
            if (assignmentFailure) {
                RequestFuture.failure(e);
            } else {
                throw e;
            }
        }
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertNoReassignments();
        if (!assignmentFailure) {
            applyAssignments(leader, offset, returnedAssignments);
        }
    }

    private void expectGeneration(boolean expectMismatch) {
        when(coordinator.generationId())
                .thenReturn(assignor.previousGenerationId + 1)
                .thenReturn(assignor.previousGenerationId + 1);
        int lastCompletedGenerationId = expectMismatch ? assignor.previousGenerationId - 1 : assignor.previousGenerationId;
        when(coordinator.lastCompletedGenerationId()).thenReturn(lastCompletedGenerationId);
    }

    private void addNewWorkers(String... workers) {
        for (String worker : workers) {
            assertNull(
                    "Worker " + worker + " already exists",
                    memberConfigs.put(worker, new ExtendedWorkerState(leaderUrl, offset, newExpandableAssignment()))
            );
        }
    }

    private void removeWorkers(String... workers) {
        for (String worker : workers) {
            assertNotNull(
                    "Worker " + worker + " does not exist",
                    memberConfigs.remove(worker)
            );
        }
    }

    private WorkerLoad emptyWorkerLoad(String worker) {
        return new WorkerLoad.Builder(worker).build();
    }

    private WorkerLoad workerLoad(String worker, int connectorStart, int connectorNum,
                                  int taskStart, int taskNum) {
        return new WorkerLoad.Builder(worker).with(
                newConnectors(connectorStart, connectorStart + connectorNum),
                newTasks(taskStart, taskStart + taskNum)).build();
    }

    private static List<String> newConnectors(int start, int end) {
        return IntStream.range(start, end)
                .mapToObj(i -> "connector" + i)
                .collect(Collectors.toList());
    }

    private static List<ConnectorTaskId> newTasks(int start, int end) {
        return newTasks("task", start, end);
    }

    private static List<ConnectorTaskId> newTasks(String connectorName, int start, int end) {
        return IntStream.range(start, end)
                .mapToObj(i -> new ConnectorTaskId(connectorName, i))
                .collect(Collectors.toList());
    }

    private static ClusterConfigState clusterConfigState(long offset,
                                                         int connectorNum,
                                                         int taskNum) {
        return clusterConfigState(offset, 1, connectorNum, taskNum);
    }

    private static ClusterConfigState clusterConfigState(long offset,
                                                         int connectorStart,
                                                         int connectorNum,
                                                         int taskNum) {
        int connectorNumEnd = connectorStart + connectorNum - 1;
        return new ClusterConfigState(
                offset,
                null,
                connectorTaskCounts(connectorStart, connectorNumEnd, taskNum),
                connectorConfigs(connectorStart, connectorNumEnd),
                connectorTargetStates(connectorStart, connectorNumEnd, TargetState.STARTED),
                taskConfigs(0, connectorNum, connectorNum * taskNum),
                Collections.emptySet());
    }

    private static Map<String, ExtendedWorkerState> memberConfigs(String givenLeader,
                                                                  long givenOffset,
                                                                  int start,
                                                                  int connectorNum) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("worker" + i, new ExtendedWorkerState(expectedLeaderUrl(givenLeader), givenOffset, null)))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private static Map<String, Integer> connectorTaskCounts(int start,
                                                            int connectorNum,
                                                            int taskCounts) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("connector" + i, taskCounts))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private static Map<String, Map<String, String>> connectorConfigs(int start, int connectorNum) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("connector" + i, new HashMap<String, String>()))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private static Map<String, TargetState> connectorTargetStates(int start,
                                                                  int connectorNum,
                                                                  TargetState state) {
        return IntStream.range(start, connectorNum + 1)
                .mapToObj(i -> new SimpleEntry<>("connector" + i, state))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private static Map<ConnectorTaskId, Map<String, String>> taskConfigs(int start,
                                                                         int connectorNum,
                                                                         int taskNum) {
        return IntStream.range(start, taskNum + 1)
                .mapToObj(i -> new SimpleEntry<>(
                        new ConnectorTaskId("connector" + i / connectorNum + 1, i),
                        new HashMap<String, String>())
                ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private void applyAssignments(String leader, long offset, Map<String, ExtendedAssignment> newAssignments) {
        newAssignments.forEach((worker, newAssignment) -> {
            ExtendedAssignment workerAssignment = memberConfigs.containsKey(worker)
                    // Duplicate the assignment in order to be able to mutate its contents
                    ? duplicate(memberConfigs.get(worker).assignment())
                    : newExpandableAssignment();
            workerAssignment.connectors().removeAll(newAssignment.revokedConnectors());
            workerAssignment.connectors().addAll(newAssignment.connectors());
            workerAssignment.tasks().removeAll(newAssignment.revokedTasks());
            workerAssignment.tasks().addAll(newAssignment.tasks());
            memberConfigs.put(worker, new ExtendedWorkerState(expectedLeaderUrl(leader), offset, workerAssignment));
        });
    }

    private ExtendedAssignment newExpandableAssignment() {
        return new ExtendedAssignment(
                protocolVersion,
                ConnectProtocol.Assignment.NO_ERROR,
                leader,
                leaderUrl,
                offset,
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                0);
    }

    private static String expectedLeaderUrl(String givenLeader) {
        return "http://" + givenLeader + ":8083";
    }

    private void assertAssignment(int connectorNum, int taskNum,
                                  int revokedConnectorNum, int revokedTaskNum,
                                  String... workers) {
        assertThat("Wrong number of workers",
                returnedAssignments.keySet().size(),
                is(workers.length));
        assertThat("Wrong set of workers",
                new ArrayList<>(returnedAssignments.keySet()), hasItems(workers));
        assertThat("Wrong number of assigned connectors",
                returnedAssignments.values().stream().map(v -> v.connectors().size()).reduce(0, Integer::sum),
                is(connectorNum));
        assertThat("Wrong number of assigned tasks",
                returnedAssignments.values().stream().map(v -> v.tasks().size()).reduce(0, Integer::sum),
                is(taskNum));
        assertThat("Wrong number of revoked connectors",
                returnedAssignments.values().stream().map(v -> v.revokedConnectors().size()).reduce(0, Integer::sum),
                is(revokedConnectorNum));
        assertThat("Wrong number of revoked tasks",
                returnedAssignments.values().stream().map(v -> v.revokedTasks().size()).reduce(0, Integer::sum),
                is(revokedTaskNum));
    }

    private void assertDelay(int expectedDelay, Map<String, ExtendedAssignment> newAssignments) {
        newAssignments.values().stream()
                .forEach(a -> assertEquals(
                        "Wrong rebalance delay in " + a, expectedDelay, a.delay()));
    }

    private void assertNoReassignments() {
        Map<String, ExtendedAssignment> existingAssignments = memberConfigs.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().assignment()
        ));
        assertNoDuplicateInAssignment(existingAssignments);
        assertNoDuplicateInAssignment(returnedAssignments);

        List<String> existingConnectors = existingAssignments.values().stream()
                .flatMap(a -> a.connectors().stream())
                .collect(Collectors.toList());
        List<String> newConnectors = returnedAssignments.values().stream()
                .flatMap(a -> a.connectors().stream())
                .collect(Collectors.toList());

        List<ConnectorTaskId> existingTasks = existingAssignments.values().stream()
                .flatMap(a -> a.tasks().stream())
                .collect(Collectors.toList());

        List<ConnectorTaskId> newTasks = returnedAssignments.values().stream()
                .flatMap(a -> a.tasks().stream())
                .collect(Collectors.toList());

        existingConnectors.retainAll(newConnectors);
        assertThat("Found connectors in new assignment that already exist in current assignment",
                Collections.emptyList(),
                is(existingConnectors));
        existingTasks.retainAll(newTasks);
        assertThat("Found tasks in new assignment that already exist in current assignment",
                Collections.emptyList(),
                is(existingConnectors));
    }

    private void assertNoDuplicateInAssignment(Map<String, ExtendedAssignment> existingAssignment) {
        List<String> existingConnectors = existingAssignment.values().stream()
                .flatMap(a -> a.connectors().stream())
                .collect(Collectors.toList());
        Set<String> existingUniqueConnectors = new HashSet<>(existingConnectors);
        existingConnectors.removeAll(existingUniqueConnectors);
        assertThat("Connectors should be unique in assignments but duplicates where found",
                Collections.emptyList(),
                is(existingConnectors));

        List<ConnectorTaskId> existingTasks = existingAssignment.values().stream()
                .flatMap(a -> a.tasks().stream())
                .collect(Collectors.toList());
        Set<ConnectorTaskId> existingUniqueTasks = new HashSet<>(existingTasks);
        existingTasks.removeAll(existingUniqueTasks);
        assertThat("Tasks should be unique in assignments but duplicates where found",
                Collections.emptyList(),
                is(existingTasks));
    }

    private void assertBalancedAndCompleteAllocation() {
        assertBalancedAllocation();
        assertCompleteAllocation();
    }

    private void assertBalancedAllocation() {
        List<Integer> connectorCounts = memberConfigs.values().stream()
                .map(e -> e.assignment().connectors().size())
                .sorted()
                .collect(Collectors.toList());
        List<Integer> taskCounts = memberConfigs.values().stream()
                .map(e -> e.assignment().tasks().size())
                .sorted()
                .collect(Collectors.toList());

        int minConnectors = connectorCounts.get(0);
        int maxConnectors = connectorCounts.get(connectorCounts.size() - 1);

        int minTasks = taskCounts.get(0);
        int maxTasks = taskCounts.get(taskCounts.size() - 1);

        assertTrue(
                "Assignments are imbalanced. The spread of connectors across each worker is: " + connectorCounts,
                maxConnectors - minConnectors <= 1
        );
        assertTrue(
                "Assignments are imbalanced. The spread of tasks across each worker is: " + taskCounts,
                maxTasks - minTasks <= 1
        );
    }

    private void assertCompleteAllocation() {
        Set<String> allAssignedConnectors = memberConfigs.values().stream()
                .map(ExtendedWorkerState::assignment)
                .map(ConnectProtocol.Assignment::connectors)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        assertEquals(
                "The set of connectors assigned across the cluster does not match the set of connectors in the config topic",
                configState.connectors(),
                allAssignedConnectors
        );

        Map<String, Set<ConnectorTaskId>> allAssignedTasks = memberConfigs.values().stream()
                .map(ExtendedWorkerState::assignment)
                .map(ConnectProtocol.Assignment::tasks)
                .flatMap(Collection::stream)
                .collect(Collectors.groupingBy(ConnectorTaskId::connector, Collectors.toSet()));
        configState.connectors().forEach(connector ->
            assertEquals(
                    "The set of tasks assigned across the cluster for connector " + connector + " does not match the set of tasks in the config topic",
                    new HashSet<>(configState.tasks(connector)),
                    allAssignedTasks.get(connector)
            )
        );
    }

    private void verifyCoordinatorInteractions() {
        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
        verify(coordinator, times(2 * rebalanceNum)).generationId();
        verify(coordinator, times(rebalanceNum)).memberId();
        verify(coordinator, times(rebalanceNum)).lastCompletedGenerationId();
    }

}
