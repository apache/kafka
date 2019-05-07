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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.ExtendedWorkerState;
import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
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
    ArgumentCaptor<Map<String, ConnectAssignment>> assignmentsCapture;

    private ClusterConfigState configState;
    private Map<String, ExtendedWorkerState> memberConfigs;
    private Map<String, ExtendedWorkerState> expectedMemberConfigs;
    private long offset;
    private String leader;
    private String leaderUrl;
    private Time time;
    private int scheduledRebalanceMaxDelay;
    private IncrementalCooperativeAssignor assignor;
    private int rebalanceNum;
    Map<String, ConnectAssignment> assignments;
    Map<String, ConnectAssignment> returnedAssignments;

    @Before
    public void setup() {
        leader = "worker1";
        leaderUrl = expectedLeaderUrl(leader);
        offset = 10;
        configState = clusterConfigState(offset, 2, 4);
        memberConfigs = memberConfigs(leader, offset, 1, 1);
        time = Time.SYSTEM;
        scheduledRebalanceMaxDelay = DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT;
        assignments = new HashMap<>();
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
                scheduledRebalanceMaxDelay));
    }

    @Test
    public void testTaskAssignmentWhenWorkerJoins() {
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1");

        // Second assignment with a second worker joining and all connectors running on previous worker
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(0, 0, 1, 4, "worker1", "worker2");

        // Third assignment after revocations
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(1, 4, 0, 0, "worker1", "worker2");

        // A fourth rebalance should not change assignments
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
    }

    @Test
    public void testTaskAssignmentWhenWorkerLeavesPermanently() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");

        // Second assignment with only one worker remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and the count
        // down for the rebalance delay starts
        applyAssignments(returnedAssignments);
        assignments.remove("worker2");
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(scheduledRebalanceMaxDelay, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(scheduledRebalanceMaxDelay / 2);

        // Third (incidental) assignment with still only one worker in the group. Max delay has not
        // been reached yet
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(scheduledRebalanceMaxDelay / 2, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(scheduledRebalanceMaxDelay / 2 + 1);

        // Fourth assignment after delay expired
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(1, 4, 0, 0, "worker1");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
    }

    @Test
    public void testTaskAssignmentWhenWorkerBounces() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");

        // Second assignment with only one worker remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and the count
        // down for the rebalance delay starts
        applyAssignments(returnedAssignments);
        assignments.remove("worker2");
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(scheduledRebalanceMaxDelay, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(scheduledRebalanceMaxDelay / 2);

        // Third (incidental) assignment with still only one worker in the group. Max delay has not
        // been reached yet
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(scheduledRebalanceMaxDelay / 2, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1");

        time.sleep(scheduledRebalanceMaxDelay / 4);

        // Fourth assignment with the second worker returning before the delay expires
        // Since the delay is still active, lost assignments are not reassigned yet
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(scheduledRebalanceMaxDelay / 4, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");

        time.sleep(scheduledRebalanceMaxDelay / 4);

        // Fifth assignment with the same two workers. The delay has expired, so the lost
        // assignments ought to be assigned to the worker that has appeared as returned.
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(1, 4, 0, 0, "worker1", "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
    }

    @Test
    public void testTaskAssignmentWhenLeaderLeavesPermanently() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");

        // Second assignment with only one worker remaining in the group. The worker that left the
        // group was the leader. The new leader has no previous assignments and is not tracking a
        // delay upon a leader's exit
        applyAssignments(returnedAssignments);
        assignments.remove("worker1");
        leader = "worker2";
        leaderUrl = expectedLeaderUrl(leader);
        memberConfigs = memberConfigs(leader, offset, assignments);
        // The fact that the leader bounces means that the assignor starts from a clean slate
        initAssignor();

        // Capture needs to be reset to point to the new assignor
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(1, 4, 0, 0, "worker2");

        // Third (incidental) assignment with still only one worker in the group.
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
    }

    @Test
    public void testTaskAssignmentWhenLeaderBounces() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");

        // Second assignment with only one worker remaining in the group. The worker that left the
        // group was the leader. The new leader has no previous assignments and is not tracking a
        // delay upon a leader's exit
        applyAssignments(returnedAssignments);
        assignments.remove("worker1");
        leader = "worker2";
        leaderUrl = expectedLeaderUrl(leader);
        memberConfigs = memberConfigs(leader, offset, assignments);
        // The fact that the leader bounces means that the assignor starts from a clean slate
        initAssignor();

        // Capture needs to be reset to point to the new assignor
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(1, 4, 0, 0, "worker2");

        // Third assignment with the previous leader returning as a follower. In this case, the
        // arrival of the previous leader is treated as an arrival of a new worker. Reassignment
        // happens immediately, first with a revocation
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker1", new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(0, 0, 1, 4, "worker1", "worker2");

        // Fourth assignment after revocations
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(1, 4, 0, 0, "worker1", "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
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
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        try {
            assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        } catch (RuntimeException e) {
            RequestFuture.failure(e);
        }
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        // This was the assignment that should have been sent, but didn't make it after all the way
        assertDelay(0, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");

        // Second assignment happens with members returning the same assignments (memberConfigs)
        // as the first time. The assignor can not tell whether it was the assignment that failed
        // or the workers that were bounced. Therefore it goes into assignment freeze for
        // the new assignments for a rebalance delay period
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertDelay(scheduledRebalanceMaxDelay, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");

        time.sleep(scheduledRebalanceMaxDelay / 2);

        // Third (incidental) assignment with still only one worker in the group. Max delay has not
        // been reached yet
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(scheduledRebalanceMaxDelay / 2, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(0, 0, 0, 0, "worker1", "worker2");

        time.sleep(scheduledRebalanceMaxDelay / 2 + 1);

        // Fourth assignment after delay expired. Finally all the new assignments are assigned
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
    }

    @Test
    public void testTaskAssignmentWhenSubsequentAssignmentAttemptFails() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(2, 8, 0, 0, "worker1", "worker2");

        when(coordinator.configSnapshot()).thenReturn(configState);
        doThrow(new RuntimeException("Unable to send computed assignment with SyncGroupRequest"))
                .when(assignor).serializeAssignments(assignmentsCapture.capture());

        // Second assignment triggered by a third worker joining. The computed assignment should
        // revoke tasks from the existing group. But the assignment won't be correctly delivered.
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        memberConfigs.put("worker3", new ExtendedWorkerState(leaderUrl, offset, null));
        try {
            assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        } catch (RuntimeException e) {
            RequestFuture.failure(e);
        }
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        // This was the assignment that should have been sent, but didn't make it after all the way
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 0, 2, "worker1", "worker2", "worker3");

        // Third assignment happens with members returning the same assignments (memberConfigs)
        // as the first time.
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertDelay(0, returnedAssignments);
        assertAssignment(0, 0, 0, 2, "worker1", "worker2", "worker3");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
    }

    @Test
    public void testTaskAssignmentWhenConnectorsAreDeleted() {
        configState = clusterConfigState(offset, 3, 4);
        when(coordinator.configSnapshot()).thenReturn(configState);
        doReturn(Collections.EMPTY_MAP).when(assignor).serializeAssignments(assignmentsCapture.capture());

        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        memberConfigs.put("worker2", new ExtendedWorkerState(leaderUrl, offset, null));
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(3, 12, 0, 0, "worker1", "worker2");

        // Second assignment with an updated config state that reflects removal of a connector
        configState = clusterConfigState(offset + 1, 2, 4);
        when(coordinator.configSnapshot()).thenReturn(configState);
        applyAssignments(returnedAssignments);
        memberConfigs = memberConfigs(leader, offset, assignments);
        assignor.performTaskAssignment(leader, offset, memberConfigs, coordinator);
        ++rebalanceNum;
        returnedAssignments = assignmentsCapture.getValue();
        assertDelay(0, returnedAssignments);
        expectedMemberConfigs = memberConfigs(leader, offset, returnedAssignments);
        assertAssignment(0, 0, 1, 4, "worker1", "worker2");

        verify(coordinator, times(rebalanceNum)).configSnapshot();
        verify(coordinator, times(rebalanceNum)).leaderState(any());
    }

    @Test
    public void testAssignConnectorsWhenBalanced() {
        int num = 2;
        List<WorkerLoad> existingAssignment = IntStream.range(0, 3)
                .mapToObj(i -> workerLoad("worker" + i, i * num, num, i * num, num))
                .collect(Collectors.toList());

        List<WorkerLoad> expectedAssignment = existingAssignment.stream()
                .map(wl -> WorkerLoad.copy(wl.worker(), wl.connectors(), wl.tasks()))
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
                .map(wl -> WorkerLoad.copy(wl.worker(), wl.connectors(), wl.tasks()))
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

    private WorkerLoad emptyWorkerLoad(String worker) {
        return WorkerLoad.embed(worker, new ArrayList<>(), new ArrayList<>());
    }

    private WorkerLoad workerLoad(String worker, int connectorStart, int connectorNum,
                                  int taskStart, int taskNum) {
        return WorkerLoad.embed(worker,
                newConnectors(connectorStart, connectorStart + connectorNum),
                newTasks(taskStart, taskStart + taskNum));
    }

    private static List<String> newConnectors(int start, int end) {
        return IntStream.range(start, end)
                .mapToObj(i -> "connector" + i)
                .collect(Collectors.toList());
    }

    private static List<ConnectorTaskId> newTasks(int start, int end) {
        return IntStream.range(start, end)
                .mapToObj(i -> new ConnectorTaskId("task", i))
                .collect(Collectors.toList());
    }

    private static ClusterConfigState clusterConfigState(long offset,
                                                         int connectorNum,
                                                         int taskNum) {
        return new ClusterConfigState(
                offset,
                connectorTaskCounts(1, connectorNum, taskNum),
                connectorConfigs(1, connectorNum),
                connectorTargetStates(1, connectorNum, TargetState.STARTED),
                taskConfigs(0, connectorNum, connectorNum * taskNum),
                Collections.emptySet());
    }

    private static Map<String, ExtendedWorkerState> memberConfigs(String givenLeader,
                                                                  long givenOffset,
                                                                  Map<String, ConnectAssignment> givenAssignments) {
        return givenAssignments.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new ExtendedWorkerState(expectedLeaderUrl(givenLeader), givenOffset, e.getValue())));
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

    private void applyAssignments(Map<String, ConnectAssignment> newAssignments) {
        newAssignments.forEach((k, v) -> {
            assignments.computeIfAbsent(k, noop -> newExpandableAssignment())
                    .connectors()
                    .removeAll(v.revokedConnectors());
            assignments.computeIfAbsent(k, noop -> newExpandableAssignment())
                    .connectors()
                    .addAll(v.connectors());
            assignments.computeIfAbsent(k, noop -> newExpandableAssignment())
                    .tasks()
                    .removeAll(v.revokedTasks());
            assignments.computeIfAbsent(k, noop -> newExpandableAssignment())
                    .tasks()
                    .addAll(v.tasks());
        });
    }

    private ConnectAssignment newExpandableAssignment() {
        return new ConnectAssignment(
                CONNECT_PROTOCOL_V1,
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
        assertAssignment(leader, connectorNum, taskNum, revokedConnectorNum, revokedTaskNum, workers);
    }

    private void assertAssignment(String expectedLeader, int connectorNum, int taskNum,
                                  int revokedConnectorNum, int revokedTaskNum,
                                  String... workers) {
        assertThat("Wrong number of workers",
                expectedMemberConfigs.keySet().size(),
                is(workers.length));
        assertThat("Wrong set of workers",
                new ArrayList<>(expectedMemberConfigs.keySet()), hasItems(workers));
        assertThat("Wrong number of assigned connectors",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().connectors().size()).reduce(0, Integer::sum),
                is(connectorNum));
        assertThat("Wrong number of assigned tasks",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().tasks().size()).reduce(0, Integer::sum),
                is(taskNum));
        assertThat("Wrong number of revoked tasks",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().revokedConnectors().size()).reduce(0, Integer::sum),
                is(revokedConnectorNum));
        assertThat("Wrong number of revoked tasks",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().revokedTasks().size()).reduce(0, Integer::sum),
                is(revokedTaskNum));
        assertThat("Wrong leader in assignments",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().leader()).distinct().collect(Collectors.joining(", ")),
                is(expectedLeader));
        assertThat("Wrong leaderUrl in assignments",
                expectedMemberConfigs.values().stream().map(v -> v.assignment().leaderUrl()).distinct().collect(Collectors.joining(", ")),
                is(expectedLeaderUrl(expectedLeader)));
    }

    private void assertDelay(int expectedDelay, Map<String, ConnectAssignment> newAssignments) {
        newAssignments.values().stream()
                .forEach(a -> assertEquals(
                        "Wrong rebalance delay in " + a, expectedDelay, a.delay()));
    }
}
