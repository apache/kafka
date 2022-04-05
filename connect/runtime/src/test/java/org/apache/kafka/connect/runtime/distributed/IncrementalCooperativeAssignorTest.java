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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeAssignor.ClusterAssignment;
import static org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import static org.apache.kafka.connect.util.ConnectUtils.duplicatedElements;
import static org.apache.kafka.connect.util.ConnectUtils.transformValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IncrementalCooperativeAssignorTest {

    private Map<String, Integer> connectors;
    private Time time;
    private int rebalanceDelay;
    private IncrementalCooperativeAssignor assignor;
    private int generationId;
    private ClusterAssignment returnedAssignments;
    private Map<String, ConnectorsAndTasks> memberAssignments;

    @Before
    public void setup() {
        generationId = 1000;
        time = Time.SYSTEM;
        rebalanceDelay = DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT;
        connectors = new HashMap<>();
        addNewConnector("connector1", 4);
        addNewConnector("connector2", 4);
        memberAssignments = new HashMap<>();
        addNewEmptyWorkers("worker1");
        initAssignor();
    }

    public void initAssignor() {
        assignor = new IncrementalCooperativeAssignor(new LogContext(), time, rebalanceDelay);
        assignor.previousGenerationId = generationId;
    }

    @Test
    public void testTaskAssignmentWhenWorkerJoins() {
        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1");
        assertConnectorAllocations(2);
        assertTaskAllocations(8);
        assertBalancedAndCompleteAllocation();

        // Second assignment with a second worker joining and all connectors running on previous worker
        addNewEmptyWorkers("worker2");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(0, 1);
        assertTaskAllocations(0, 4);

        // Third assignment after revocations
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(4, 4);
        assertBalancedAndCompleteAllocation();

        // A fourth rebalance should not change assignments
        performStandardRebalance();
        assertDelay(0);
        assertEmptyAssignment();
    }

    @Test
    public void testTaskAssignmentWhenWorkerLeavesPermanently() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        addNewEmptyWorkers("worker2");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(4, 4);
        assertBalancedAndCompleteAllocation();

        // Second assignment with only one worker remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and the count
        // down for the rebalance delay starts
        removeWorkers("worker2");
        performStandardRebalance();
        assertDelay(rebalanceDelay);
        assertWorkers("worker1");
        assertEmptyAssignment();

        time.sleep(rebalanceDelay / 2);

        // Third (incidental) assignment with still only one worker in the group. Max delay has not
        // been reached yet
        performStandardRebalance();
        assertDelay(rebalanceDelay / 2);
        assertEmptyAssignment();

        time.sleep(rebalanceDelay / 2 + 1);

        // Fourth assignment after delay expired
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(2);
        assertTaskAllocations(8);
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testTaskAssignmentWhenWorkerBounces() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        addNewEmptyWorkers("worker2");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(4, 4);
        assertBalancedAndCompleteAllocation();

        // Second assignment with only one worker remaining in the group. The worker that left the
        // group was a follower. No re-assignments take place immediately and the count
        // down for the rebalance delay starts
        removeWorkers("worker2");
        performStandardRebalance();
        assertDelay(rebalanceDelay);
        assertWorkers("worker1");
        assertEmptyAssignment();

        time.sleep(rebalanceDelay / 2);

        // Third (incidental) assignment with still only one worker in the group. Max delay has not
        // been reached yet
        performStandardRebalance();
        assertDelay(rebalanceDelay / 2);
        assertEmptyAssignment();

        time.sleep(rebalanceDelay / 4);

        // Fourth assignment with the second worker returning before the delay expires
        // Since the delay is still active, lost assignments are not reassigned yet
        addNewEmptyWorkers("worker2");
        performStandardRebalance();
        assertDelay(rebalanceDelay / 4);
        assertWorkers("worker1", "worker2");
        assertEmptyAssignment();

        time.sleep(rebalanceDelay / 4);

        // Fifth assignment with the same two workers. The delay has expired, so the lost
        // assignments ought to be assigned to the worker that has appeared as returned.
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(4, 4);
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testTaskAssignmentWhenLeaderLeavesPermanently() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        // First assignment with 3 workers and 2 connectors configured but not yet assigned
        addNewEmptyWorkers("worker2", "worker3");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2", "worker3");
        assertConnectorAllocations(0, 1, 1);
        assertTaskAllocations(2, 3, 3);
        assertBalancedAndCompleteAllocation();

        // Second assignment with two workers remaining in the group. The worker that left the
        // group was the leader. The new leader has no previous assignments and is not tracking a
        // delay upon a leader's exit
        removeWorkers("worker1");
        // The fact that the leader bounces means that the assignor starts from a clean slate
        initAssignor();

        // Capture needs to be reset to point to the new assignor
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker2", "worker3");
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(4, 4);
        assertBalancedAndCompleteAllocation();

        // Third (incidental) assignment with still only two workers in the group.
        performStandardRebalance();
        assertDelay(0);
        assertEmptyAssignment();
    }

    @Test
    public void testTaskAssignmentWhenLeaderBounces() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        // First assignment with 3 workers and 2 connectors configured but not yet assigned
        addNewEmptyWorkers("worker2", "worker3");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2", "worker3");
        assertConnectorAllocations(0, 1, 1);
        assertTaskAllocations(2, 3, 3);
        assertBalancedAndCompleteAllocation();

        // Second assignment with two workers remaining in the group. The worker that left the
        // group was the leader. The new leader has no previous assignments and is not tracking a
        // delay upon a leader's exit
        removeWorkers("worker1");
        // The fact that the leader bounces means that the assignor starts from a clean slate
        initAssignor();

        // Capture needs to be reset to point to the new assignor
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker2", "worker3");
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(4, 4);
        assertBalancedAndCompleteAllocation();

        // Third assignment with the previous leader returning as a follower. In this case, the
        // arrival of the previous leader is treated as an arrival of a new worker. Reassignment
        // happens immediately, first with a revocation
        addNewEmptyWorkers("worker1");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2", "worker3");
        assertConnectorAllocations(0, 1, 1);
        assertTaskAllocations(0, 3, 3);

        // Fourth assignment after revocations
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(0, 1, 1);
        assertTaskAllocations(2, 3, 3);
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testTaskAssignmentWhenWorkerJoinAfterRevocation() {
        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1");
        assertConnectorAllocations(2);
        assertTaskAllocations(8);
        assertBalancedAndCompleteAllocation();

        // Second assignment with a second worker joining and all connectors running on previous worker
        addNewEmptyWorkers("worker2");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(0, 1);
        assertTaskAllocations(0, 4);

        // Third assignment with a third worker joining; we don't perform any load-balancing revocations
        // because we already performed a revocation from worker1 in the last round when worker2 joined
        // However, we do evenly allocate all connectors and tasks that were successfully revoked from worker1
        // across worker2 and worker3
        addNewEmptyWorkers("worker3");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2", "worker3");
        assertConnectorAllocations(0, 1, 1);
        assertTaskAllocations(2, 2, 4);

        // Fourth assignment with a fourth worker joining; load-balancing revocations should take place
        // since none were performed in the last round
        addNewEmptyWorkers("worker4");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2", "worker3", "worker4");
        assertConnectorAllocations(0, 0, 1, 1);
        assertTaskAllocations(0, 2, 2, 2);

        // Fifth assignment after revocations with no new workers; all connectors and tasks should be allocated
        // as evenly as possible across all workers in the cluster
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(0, 0, 1, 1);
        assertTaskAllocations(2, 2, 2, 2);

        assertStableAssignment();
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testTaskAssignmentWhenFirstAssignmentAttemptFails() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        addNewEmptyWorkers("worker2");
        performFailedRebalance();
        // This was the assignment that should have been sent, but didn't make it all the way
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(0, 0);
        assertTaskAllocations(0, 0);

        // Second assignment happens with members returning the same assignments (memberConfigs)
        // as the first time. The assignor detects that the number of members did not change and
        // avoids the rebalance delay, treating the lost assignments as new assignments.
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(4, 4);
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testTaskAssignmentWhenSubsequentAssignmentAttemptFails() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        addNewEmptyWorkers("worker2");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(4, 4);
        assertBalancedAndCompleteAllocation();

        // Second assignment triggered by a third worker joining. The computed assignment should
        // revoke tasks from the existing group. But the assignment won't be correctly delivered.
        addNewEmptyWorkers("worker3");
        performFailedRebalance();
        // This was the assignment that should have been sent, but didn't make it all the way
        assertDelay(0);
        assertWorkers("worker1", "worker2", "worker3");
        assertConnectorAllocations(0, 1, 1);
        assertTaskAllocations(0, 4, 4);

        // Third assignment happens with members returning the same assignments (memberConfigs)
        // as the first time.
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(0, 1, 1);
        assertTaskAllocations(0, 3, 3);

        // Fourth assignment after revocations
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(0, 1, 1);
        assertTaskAllocations(2, 3, 3);
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testTaskAssignmentWhenSubsequentAssignmentAttemptFailsOutsideTheAssignor() {
        // Customize assignor for this test case
        time = new MockTime();
        initAssignor();

        // First assignment with 2 workers and 2 connectors configured but not yet assigned
        addNewEmptyWorkers("worker2");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(4, 4);
        assertBalancedAndCompleteAllocation();

        // Second assignment triggered by a third worker joining. The computed assignment should
        // revoke tasks from the existing group. But the assignment won't be correctly delivered
        // and sync group with fail on the leader worker.
        addNewEmptyWorkers("worker3");
        performFailedRebalance();
        // This was the assignment that should have been sent, but didn't make it all the way
        assertDelay(0);
        assertWorkers("worker1", "worker2", "worker3");
        assertConnectorAllocations(0, 1, 1);
        assertTaskAllocations(0, 4, 4);

        // Third assignment happens with members returning the same assignments (memberConfigs)
        // as the first time.
        performRebalanceWithMismatchedGeneration();
        assertDelay(0);
        assertConnectorAllocations(0, 1, 1);
        assertTaskAllocations(0, 3, 3);

        // Fourth assignment after revocations
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(0, 1, 1);
        assertTaskAllocations(2, 3, 3);
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testTaskAssignmentWhenConnectorsAreDeleted() {
        addNewConnector("connector3", 4);

        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        addNewEmptyWorkers("worker2");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(1, 2);
        assertTaskAllocations(6, 6);
        assertBalancedAndCompleteAllocation();

        // Second assignment with an updated config state that reflects removal of a connector
        removeConnector("connector3");
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(4, 4);
        assertBalancedAndCompleteAllocation();
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
        expectedAssignment.get(0).connectors().addAll(Arrays.asList("connector10", "connector8"));
        expectedAssignment.get(1).connectors().addAll(Arrays.asList("connector6", "connector9"));
        expectedAssignment.get(2).connectors().addAll(Arrays.asList("connector7"));

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

        expectedAssignment.get(0).connectors().addAll(Arrays.asList("connector10", "connector8"));
        expectedAssignment.get(1).connectors().addAll(Arrays.asList("connector6", "connector9"));
        expectedAssignment.get(2).connectors().addAll(Arrays.asList("connector7"));

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
        time = new MockTime();
        initAssignor();

        assertTrue(assignor.candidateWorkersForReassignment.isEmpty());
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        Map<String, WorkerLoad> configuredAssignment = new HashMap<>();
        configuredAssignment.put("worker0", workerLoad("worker0", 0, 2, 0, 4));
        configuredAssignment.put("worker1", workerLoad("worker1", 2, 2, 4, 4));
        configuredAssignment.put("worker2", workerLoad("worker2", 4, 2, 8, 4));

        ConnectorsAndTasks.Builder lostAssignmentsToReassign = ConnectorsAndTasks.builder();

        // No lost assignments
        assignor.handleLostAssignments(ConnectorsAndTasks.EMPTY,
                lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertEquals("Wrong set of workers for reassignments",
                Collections.emptySet(),
                assignor.candidateWorkersForReassignment);
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        assignor.previousMembers = new HashSet<>(configuredAssignment.keySet());

        String flakyWorker = "worker1";
        WorkerLoad lostLoad = configuredAssignment.remove(flakyWorker);
        ConnectorsAndTasks lostAssignments = ConnectorsAndTasks.of(lostLoad);

        // Lost assignments detected - No candidate worker has appeared yet (worker with no assignments)
        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertEquals("Wrong set of workers for reassignments",
                Collections.emptySet(),
                assignor.candidateWorkersForReassignment);
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(configuredAssignment.keySet());
        time.sleep(rebalanceDelay / 2);
        rebalanceDelay /= 2;

        // A new worker (probably returning worker) has joined
        configuredAssignment.put(flakyWorker, new WorkerLoad.Builder(flakyWorker).build());
        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertEquals("Wrong set of workers for reassignments",
                Collections.singleton(flakyWorker),
                assignor.candidateWorkersForReassignment);
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(configuredAssignment.keySet());
        time.sleep(rebalanceDelay);

        // The new worker has still no assignments
        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertTrue("Wrong assignment of lost connectors",
                configuredAssignment.getOrDefault(flakyWorker, new WorkerLoad.Builder(flakyWorker).build())
                        .connectors()
                        .containsAll(lostAssignments.connectors()));
        assertTrue("Wrong assignment of lost tasks",
                configuredAssignment.getOrDefault(flakyWorker, new WorkerLoad.Builder(flakyWorker).build())
                        .tasks()
                        .containsAll(lostAssignments.tasks()));
        assertEquals("Wrong set of workers for reassignments",
                Collections.emptySet(),
                assignor.candidateWorkersForReassignment);
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

        ConnectorsAndTasks.Builder lostAssignmentsToReassign = ConnectorsAndTasks.builder();

        // No lost assignments
        assignor.handleLostAssignments(ConnectorsAndTasks.EMPTY,
                lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertEquals("Wrong set of workers for reassignments",
                Collections.emptySet(),
                assignor.candidateWorkersForReassignment);
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        assignor.previousMembers = new HashSet<>(configuredAssignment.keySet());

        String removedWorker = "worker1";
        WorkerLoad lostLoad = configuredAssignment.remove(removedWorker);
        ConnectorsAndTasks lostAssignments = ConnectorsAndTasks.of(lostLoad);

        // Lost assignments detected - No candidate worker has appeared yet (worker with no assignments)
        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertEquals("Wrong set of workers for reassignments",
                Collections.emptySet(),
                assignor.candidateWorkersForReassignment);
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(memberAssignments.keySet());
        time.sleep(rebalanceDelay / 2);
        rebalanceDelay /= 2;

        // No new worker has joined
        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertEquals("Wrong set of workers for reassignments",
                Collections.emptySet(),
                assignor.candidateWorkersForReassignment);
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        time.sleep(rebalanceDelay);

        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertTrue("Wrong assignment of lost connectors",
                lostAssignmentsToReassign.build().connectors().containsAll(lostAssignments.connectors()));
        assertTrue("Wrong assignment of lost tasks",
                lostAssignmentsToReassign.build().tasks().containsAll(lostAssignments.tasks()));
        assertEquals("Wrong set of workers for reassignments",
                Collections.emptySet(),
                assignor.candidateWorkersForReassignment);
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);
    }

    @Test
    public void testLostAssignmentHandlingWithMoreThanOneCandidate() {
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

        ConnectorsAndTasks.Builder lostAssignmentsToReassign = ConnectorsAndTasks.builder();

        // No lost assignments
        assignor.handleLostAssignments(ConnectorsAndTasks.EMPTY,
                lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertEquals("Wrong set of workers for reassignments",
                Collections.emptySet(),
                assignor.candidateWorkersForReassignment);
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        assignor.previousMembers = new HashSet<>(configuredAssignment.keySet());

        String flakyWorker = "worker1";
        WorkerLoad lostLoad = configuredAssignment.remove(flakyWorker);
        ConnectorsAndTasks lostAssignments = ConnectorsAndTasks.of(lostLoad);

        String newWorker = "worker3";
        configuredAssignment.put(newWorker, new WorkerLoad.Builder(newWorker).build());

        // Lost assignments detected - A new worker also has joined that is not the returning worker
        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertEquals("Wrong set of workers for reassignments",
                Collections.singleton(newWorker),
                assignor.candidateWorkersForReassignment);
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(configuredAssignment.keySet());
        time.sleep(rebalanceDelay / 2);
        rebalanceDelay /= 2;

        // Now two new workers have joined
        configuredAssignment.put(flakyWorker, new WorkerLoad.Builder(flakyWorker).build());
        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        Set<String> expectedWorkers = new HashSet<>();
        expectedWorkers.addAll(Arrays.asList(newWorker, flakyWorker));
        assertEquals("Wrong set of workers for reassignments",
                expectedWorkers,
                assignor.candidateWorkersForReassignment);
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(configuredAssignment.keySet());
        time.sleep(rebalanceDelay);

        // The new workers have new assignments, other than the lost ones
        configuredAssignment.put(flakyWorker, workerLoad(flakyWorker, 6, 2, 8, 4));
        configuredAssignment.put(newWorker, workerLoad(newWorker, 8, 2, 12, 4));
        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        // both the newWorkers would need to be considered for reassignment of connectors and tasks
        List<String> listOfConnectorsInLast2Workers = new ArrayList<>();
        listOfConnectorsInLast2Workers.addAll(configuredAssignment.get(newWorker).connectors());
        listOfConnectorsInLast2Workers.addAll(configuredAssignment.get(flakyWorker).connectors());
        List<ConnectorTaskId> listOfTasksInLast2Workers = new ArrayList<>();
        listOfTasksInLast2Workers.addAll(configuredAssignment.get(newWorker).tasks());
        listOfTasksInLast2Workers.addAll(configuredAssignment.get(flakyWorker).tasks());
        assertTrue("Wrong assignment of lost connectors",
            listOfConnectorsInLast2Workers.containsAll(lostAssignments.connectors()));
        assertTrue("Wrong assignment of lost tasks",
            listOfTasksInLast2Workers.containsAll(lostAssignments.tasks()));
        assertEquals("Wrong set of workers for reassignments",
            Collections.emptySet(),
            assignor.candidateWorkersForReassignment);
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

        ConnectorsAndTasks.Builder lostAssignmentsToReassign = ConnectorsAndTasks.builder();

        // No lost assignments
        assignor.handleLostAssignments(ConnectorsAndTasks.EMPTY,
                lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertEquals("Wrong set of workers for reassignments",
                Collections.emptySet(),
                assignor.candidateWorkersForReassignment);
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);

        assignor.previousMembers = new HashSet<>(configuredAssignment.keySet());

        String veryFlakyWorker = "worker1";
        WorkerLoad lostLoad = configuredAssignment.remove(veryFlakyWorker);
        ConnectorsAndTasks lostAssignments = ConnectorsAndTasks.of(lostLoad);

        // Lost assignments detected - No candidate worker has appeared yet (worker with no assignments)
        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertEquals("Wrong set of workers for reassignments",
                Collections.emptySet(),
                assignor.candidateWorkersForReassignment);
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(configuredAssignment.keySet());
        time.sleep(rebalanceDelay / 2);
        rebalanceDelay /= 2;

        // A new worker (probably returning worker) has joined
        configuredAssignment.put(veryFlakyWorker, new WorkerLoad.Builder(veryFlakyWorker).build());
        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertEquals("Wrong set of workers for reassignments",
                Collections.singleton(veryFlakyWorker),
                assignor.candidateWorkersForReassignment);
        assertEquals(time.milliseconds() + rebalanceDelay, assignor.scheduledRebalance);
        assertEquals(rebalanceDelay, assignor.delay);

        assignor.previousMembers = new HashSet<>(configuredAssignment.keySet());
        time.sleep(rebalanceDelay);

        // The returning worker leaves permanently after joining briefly during the delay
        configuredAssignment.remove(veryFlakyWorker);
        assignor.handleLostAssignments(lostAssignments, lostAssignmentsToReassign,
                new ArrayList<>(configuredAssignment.values()));

        assertTrue("Wrong assignment of lost connectors",
                lostAssignmentsToReassign.build().connectors().containsAll(lostAssignments.connectors()));
        assertTrue("Wrong assignment of lost tasks",
                lostAssignmentsToReassign.build().tasks().containsAll(lostAssignments.tasks()));
        assertEquals("Wrong set of workers for reassignments",
                Collections.emptySet(),
                assignor.candidateWorkersForReassignment);
        assertEquals(0, assignor.scheduledRebalance);
        assertEquals(0, assignor.delay);
    }

    @Test
    public void testTaskAssignmentWhenTasksDuplicatedInWorkerAssignment() {
        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1");
        assertConnectorAllocations(2);
        assertTaskAllocations(8);
        assertBalancedAndCompleteAllocation();

        // Second assignment with a second worker with duplicate assignment joining and all connectors running on previous worker
        addNewWorker("worker2", newConnectors(1, 2), newTasks("connector1", 0, 4));
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(0, 1);
        assertTaskAllocations(0, 4);

        // Third assignment after revocations
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(4, 4);

        // Fourth rebalance should not change assignments
        performStandardRebalance();
        assertDelay(0);
        assertEmptyAssignment();
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testDuplicatedAssignmentHandleWhenTheDuplicatedAssignmentsDeleted() {
        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1");
        assertConnectorAllocations(2);
        assertTaskAllocations(8);
        assertBalancedAndCompleteAllocation();

        // Delete connector1
        removeConnector("connector1");

        // Second assignment with a second worker with duplicate assignment joining and the duplicated assignment is deleted at the same time
        addNewWorker("worker2", newConnectors(1, 2), newTasks("connector1", 0, 4));
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(0, 1);
        assertTaskAllocations(0, 2);

        // Third assignment after revocations
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(0, 1);
        assertTaskAllocations(2, 2);

        // fourth rebalance should not change assignments
        performStandardRebalance();
        assertDelay(0);
        assertEmptyAssignment();
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testDuplicatedAssignmentsAndOtherConnectorDeletedInSameRound() {
        // First assignment with 1 worker and 2 connectors configured but not yet assigned
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1");
        assertConnectorAllocations(2);
        assertTaskAllocations(8);
        assertBalancedAndCompleteAllocation();

        // Delete a connector
        removeConnector("connector2");

        // Add a new worker, which is running a duplicate instance of connector1
        addNewWorker("worker2", newConnectors(1, 2), newTasks("connector1", 0, 4));
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        // The deleted connector (connector2) should be revoked from worker1, and the duplicated connector (connector1) should be revoked from both
        assertConnectorAllocations(0, 0);
        assertTaskAllocations(0, 0);

        // Third assignment after revocations have taken effect; the formerly-duplicated connector and its tasks should be evenly assigned across the cluster
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(0, 1);
        assertTaskAllocations(2, 2);

        // Fourth rebalance should not change assignments
        performStandardRebalance();
        assertDelay(0);
        assertEmptyAssignment();
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testNewConnectorAddedAtSameTimeAsWorkerAndThenRemoved() {
        // Start with three workers and four connectors, each with four tasks
        addNewConnector("connector3", 4);
        addNewConnector("connector4", 4);
        addNewEmptyWorkers("worker2", "worker3");

        performStandardRebalance();
        assertWorkers("worker1", "worker2", "worker3");
        assertDelay(0);
        assertConnectorAllocations(1, 1, 2);
        assertTaskAllocations(5, 5, 6);
        assertBalancedAndCompleteAllocation();

        // Add a new worker and, in the same round, two connectors, which should cause some revocations to take place in order to balance the cluster
        addNewEmptyWorkers("worker4");
        addNewConnector("connector5", 4);
        addNewConnector("connector6", 4);
        performStandardRebalance();
        assertWorkers("worker1", "worker2", "worker3", "worker4");
        assertDelay(0);

        // And in the next round, the revoked connectors and tasks should be redistributed across the cluster in order to achieve balance
        performStandardRebalance();
        assertWorkers("worker1", "worker2", "worker3", "worker4");
        assertBalancedAndCompleteAllocation();
        assertDelay(0);

        // Then, a connectors is deleted
        removeConnector("connector5");

        rebalanceUntilStable();
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testResizeConnectors() {
        // Start with two workers and two connectors, each with two tasks
        connectors.clear();
        addNewConnector("connector1", 2);
        addNewConnector("connector2", 2);
        addNewEmptyWorkers("worker2");

        performStandardRebalance();
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(1, 1);
        assertTaskAllocations(2, 2);
        assertBalancedAndCompleteAllocation();

        // Resize both connectors from two tasks to one
        resizeExistingConnector("connector1", 1);
        resizeExistingConnector("connector2", 1);

        rebalanceUntilStable();
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testAddWorkerThenRemoveConnector() {
        // Start with one worker and three connectors, each with four tasks
        addNewConnector("connector3", 4);

        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1");
        assertConnectorAllocations(3);
        assertTaskAllocations(12);
        assertBalancedAndCompleteAllocation();

        // Add a new worker, which should cause a connector and some tasks to be revoked from the existing worker
        addNewEmptyWorkers("worker2");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2");
        assertConnectorAllocations(0, 2);
        assertTaskAllocations(0, 6);

        // After a successful revocation, the next rebalance distributes the revoked connector and tasks to the new worker
        performStandardRebalance();
        assertDelay(0);
        assertConnectorAllocations(1, 2);
        assertTaskAllocations(6, 6);
        assertBalancedAndCompleteAllocation();

        // Finally, one of the connectors is deleted
        removeConnector("connector3");
        rebalanceUntilStable();
        assertBalancedAndCompleteAllocation();
    }

    @Test
    public void testNewWorkerAndNewTasksInSameRound() {
        connectors.clear();
        // Start with 1 connector running 40 tasks
        addNewConnector("connector1", 40);
        // And three workers
        addNewEmptyWorkers("worker2", "worker3");

        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2", "worker3");
        assertConnectorAllocations(0, 0, 1);
        assertTaskAllocations(13, 13, 14);

        // Add 2 tasks
        resizeExistingConnector("connector1", 42);
        // Add a worker
        addNewEmptyWorkers("worker4");
        performStandardRebalance();
        assertDelay(0);
        assertWorkers("worker1", "worker2", "worker3", "worker4");
        assertConnectorAllocations(0, 0, 0, 1);
        assertTaskAllocations(2, 10, 11, 11);

        // Rebalance once more as a follow-up to task revocation
        performStandardRebalance();
        assertConnectorAllocations(0, 0, 0, 1);
        assertTaskAllocations(10, 10, 11, 11);
        assertDelay(0);
        assertBalancedAndCompleteAllocation();

        assertStableAssignment();
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

    private void performRebalance(boolean assignmentFailure, boolean generationMismatch) {
        generationId++;
        int lastCompletedGenerationId = generationMismatch ? generationId - 2 : generationId - 1;
        try {
            Map<String, ConnectorsAndTasks> memberAssignmentsCopy = new HashMap<>(memberAssignments);
            returnedAssignments = assignor.performTaskAssignment(configState(), lastCompletedGenerationId, generationId, memberAssignmentsCopy);
        } catch (RuntimeException e) {
            if (assignmentFailure) {
                RequestFuture.failure(e);
            } else {
                throw e;
            }
        }
        assertNoRedundantAssignments();
        if (!assignmentFailure) {
            applyAssignments();
        }
    }

    /**
     * Rebalance until the latest assignment is a no-op (no newly-assigned or newly-revoked connectors or tasks,
     * and a delay of 0). At most two rounds of rebalance are expected to take place before a stable assignment
     * is reached; if more than two rounds take place without reaching stability, an exception is thrown.
     * <p>
     * This should not be invoked while a scheduled rebalance delay is active, as it will almost certainly
     * result in an exception being thrown.
     * @throws AssertionError if too many consecutive rebalances take place with no empty assignment
     */
    private void rebalanceUntilStable() {
        performStandardRebalance();
        if (!assignmentWasEmpty()) {
            performStandardRebalance();
            if (!assignmentWasEmpty()) {
                assertStableAssignment();
            }
        }
    }

    /**
     * Perform one extra round of rebalance and verify that it will be a no-op, indicating that the assignment
     * is stable.
     */
    private void assertStableAssignment() {
        performStandardRebalance();
        assertEmptyAssignment();
        assertDelay(0);
    }

    private boolean assignmentWasEmpty() {
        return ConnectUtils.combineCollections(returnedAssignments.newlyAssignedConnectors().values()).isEmpty()
                && ConnectUtils.combineCollections(returnedAssignments.newlyAssignedTasks().values()).isEmpty()
                && ConnectUtils.combineCollections(returnedAssignments.newlyRevokedConnectors().values()).isEmpty()
                && ConnectUtils.combineCollections(returnedAssignments.newlyRevokedTasks().values()).isEmpty()
                && assignor.delay == 0;
    }

    private void addNewEmptyWorkers(String... workers) {
        for (String worker : workers) {
            addNewWorker(worker, Collections.emptyList(), Collections.emptyList());
        }
    }

    private void addNewWorker(String worker, List<String> connectors, List<ConnectorTaskId> tasks) {
        ConnectorsAndTasks assignment = ConnectorsAndTasks.of(connectors, tasks);
        assertNull(
                "Worker " + worker + " already exists",
                memberAssignments.put(worker, assignment)
        );
    }

    private void removeWorkers(String... workers) {
        for (String worker : workers) {
            assertNotNull(
                    "Worker " + worker + " does not exist",
                    memberAssignments.remove(worker)
            );
        }
    }

    private static WorkerLoad emptyWorkerLoad(String worker) {
        return new WorkerLoad.Builder(worker).build();
    }

    private static WorkerLoad workerLoad(String worker, int connectorStart, int connectorNum,
                                  int taskStart, int taskNum) {
        return new WorkerLoad.Builder(worker).withCopies(
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

    private void addNewConnector(String connector, int taskCount) {
        assertNull(
                "Connector " + connector + " already exists",
                connectors.put(connector, taskCount)
        );
    }

    private void resizeExistingConnector(String connector, int taskCount) {
        assertNotNull(
                "Connector " + connector + " does not exist",
                connectors.put(connector, taskCount)
        );
    }

    private void removeConnector(String connector) {
        assertNotNull(
                "Connector " + connector + " does not exist",
                connectors.remove(connector)
        );
    }

    private ClusterConfigState configState() {
        Map<String, Integer> taskCounts = new HashMap<>(connectors);
        Map<String, Map<String, String>> connectorConfigs = transformValues(taskCounts, c -> Collections.emptyMap());
        Map<String, TargetState> targetStates = transformValues(taskCounts, c -> TargetState.STARTED);
        Map<ConnectorTaskId, Map<String, String>> taskConfigs = taskCounts.entrySet().stream()
                .flatMap(e -> IntStream.range(0, e.getValue()).mapToObj(i -> new ConnectorTaskId(e.getKey(), i)))
                .collect(Collectors.toMap(
                        Function.identity(),
                        connectorTaskId -> Collections.emptyMap()
                ));
        return new ClusterConfigState(
                16,
                null,
                taskCounts,
                connectorConfigs,
                targetStates,
                taskConfigs,
                Collections.emptySet());
    }

    private void applyAssignments() {
        Map<String, ConnectorsAndTasks> newMemberAssignments = new HashMap<>();
        returnedAssignments.allWorkers().forEach(worker -> {
            ConnectorsAndTasks workerAssignment = memberAssignments.getOrDefault(worker, ConnectorsAndTasks.EMPTY)
                    .toBuilder()
                    .removeConnectors(returnedAssignments.newlyRevokedConnectors(worker))
                    .addConnectors(returnedAssignments.newlyAssignedConnectors(worker))
                    .removeTasks(returnedAssignments.newlyRevokedTasks(worker))
                    .addTasks(returnedAssignments.newlyAssignedTasks(worker))
                    .build();

            assertEquals(
                    "Complete connector assignment for worker " + worker + " does not match expectations " +
                            "based on prior assignment and new revocations and assignments",
                    workerAssignment.connectors(),
                    returnedAssignments.allAssignedConnectors().get(worker)
            );
            assertEquals(
                    "Complete task assignment for worker " + worker + " does not match expectations " +
                            "based on prior assignment and new revocations and assignments",
                    workerAssignment.tasks(),
                    returnedAssignments.allAssignedTasks().get(worker)
            );

            newMemberAssignments.put(worker, workerAssignment);
        });
        memberAssignments = newMemberAssignments;
    }

    private void assertEmptyAssignment() {
        assertEquals(
                "No connectors should have been newly assigned during this round",
                Collections.emptyList(),
                ConnectUtils.combineCollections(returnedAssignments.newlyAssignedConnectors().values())
        );
        assertEquals(
                "No tasks should have been newly assigned during this round",
                Collections.emptyList(),
                ConnectUtils.combineCollections(returnedAssignments.newlyAssignedTasks().values())
        );
        assertEquals(
                "No connectors should have been revoked during this round",
                Collections.emptyList(),
                ConnectUtils.combineCollections(returnedAssignments.newlyRevokedConnectors().values())
        );
        assertEquals(
                "No tasks should have been revoked during this round",
                Collections.emptyList(),
                ConnectUtils.combineCollections(returnedAssignments.newlyRevokedTasks().values())
        );
    }

    private void assertWorkers(String... workers) {
        assertEquals(
                "Wrong set of workers",
                new HashSet<>(Arrays.asList(workers)),
                returnedAssignments.allWorkers()
        );
    }

    /**
     * Assert that the connector counts for each worker in the cluster match the expected counts.
     * For example, calling {@code assertConnectorAllocations(0, 0, 2, 3)} ensures that there are two
     * workers in the cluster that are assigned no connectors, one worker that is assigned two connectors,
     * and one worker that is assigned three connectors.
     */
    private void assertConnectorAllocations(int... connectorCounts) {
        assertAllocations("connectors", ConnectorsAndTasks::connectors, connectorCounts);
    }

    /**
     * Assert that the task counts for each worker in the cluster match the expected counts.
     * For example, calling {@code assertTaskAllocations(0, 0, 2, 3)} ensures that there are two
     * workers in the cluster that are assigned no tasks, one worker that is assigned two tasks,
     * and one worker that is assigned three tasks.
     */
    private void assertTaskAllocations(int... taskCounts) {
        assertAllocations("tasks", ConnectorsAndTasks::tasks, taskCounts);
    }

    private void assertAllocations(String allocated, Function<ConnectorsAndTasks, ? extends Collection<?>> allocation, int... rawExpectedAllocations) {
        List<Integer> expectedAllocations = IntStream.of(rawExpectedAllocations)
                .boxed()
                .sorted()
                .collect(Collectors.toList());
        List<Integer> actualAllocations = allocations(allocation);
        assertEquals(
                "Allocation of assigned " + allocated + " across cluster does not match expected counts",
                expectedAllocations,
                actualAllocations
        );
    }

    private List<Integer> allocations(Function<ConnectorsAndTasks, ? extends Collection<?>> allocation) {
        return memberAssignments.values().stream()
                .map(allocation)
                .map(Collection::size)
                .sorted()
                .collect(Collectors.toList());
    }

    private void assertDelay(int expectedDelay) {
        assertEquals(
                "Wrong rebalance delay",
                expectedDelay,
                assignor.delay
        );
    }

    /**
     * Ensure that no connectors or tasks that were already assigned during the previous round are newly assigned in this round,
     * that each newly-assigned connector and task is only assigned to a single worker, and that no connectors or tasks are both
     * assigned and revoked to/from the same worker.
     */
    private void assertNoRedundantAssignments() {
        List<String> newConnectors = ConnectUtils.combineCollections(returnedAssignments.newlyAssignedConnectors().values());
        List<ConnectorTaskId> newTasks = ConnectUtils.combineCollections(returnedAssignments.newlyAssignedTasks().values());
        assertNoDuplicates(
                newConnectors,
                "Connectors should be unique in assignments but duplicates were found; the set of newly-assigned connectors is " + newConnectors
        );
        assertNoDuplicates(
                newTasks,
                "Tasks should be unique in assignments but duplicates were found; the set of newly-assigned tasks is " + newTasks
        );

        List<String> existingConnectors = ConnectUtils.combineCollections(memberAssignments.values(), ConnectorsAndTasks::connectors);
        List<ConnectorTaskId> existingTasks = ConnectUtils.combineCollections(memberAssignments.values(), ConnectorsAndTasks::tasks);
        assertEquals("Found connectors in new assignment that already exist in current assignment",
                Collections.emptySet(),
                Utils.intersection(existingConnectors, newConnectors)
        );
        assertEquals("Found tasks in new assignment that already exist in current assignment",
                Collections.emptySet(),
                Utils.intersection(existingTasks, newTasks)
        );

        returnedAssignments.allWorkers().forEach(worker -> {
            assertEquals("Found connectors that were both assigned to and revoked from worker " + worker,
                    Collections.emptySet(),
                    Utils.intersection(returnedAssignments.newlyAssignedConnectors(worker), returnedAssignments.newlyRevokedConnectors(worker))
            );
            assertEquals("Found tasks that were both assigned to and revoked from worker " + worker,
                    Collections.emptySet(),
                    Utils.intersection(returnedAssignments.newlyAssignedTasks(worker), returnedAssignments.newlyRevokedTasks(worker))
            );
        });
    }

    private void assertBalancedAndCompleteAllocation() {
        assertBalancedAllocation();
        assertCompleteAllocation();
    }

    private void assertBalancedAllocation() {
        List<Integer> connectorCounts = allocations(ConnectorsAndTasks::connectors);
        List<Integer> taskCounts = allocations(ConnectorsAndTasks::tasks);

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
        List<String> allAssignedConnectors = ConnectUtils.combineCollections(memberAssignments.values(), ConnectorsAndTasks::connectors);
        assertEquals(
                "The set of connectors assigned across the cluster does not match the set of connectors in the config topic",
                connectors.keySet(),
                new HashSet<>(allAssignedConnectors)
        );

        Map<String, List<ConnectorTaskId>> allAssignedTasks = ConnectUtils.combineCollections(memberAssignments.values(), ConnectorsAndTasks::tasks)
                .stream()
                .collect(Collectors.groupingBy(ConnectorTaskId::connector, Collectors.toList()));

        connectors.forEach((connector, taskCount) -> {
            Set<ConnectorTaskId> expectedTasks = IntStream.range(0, taskCount)
                    .mapToObj(i -> new ConnectorTaskId(connector, i))
                    .collect(Collectors.toSet());
            assertEquals(
                    "The set of tasks assigned across the cluster for connector " + connector + " does not match the set of tasks in the config topic",
                    expectedTasks,
                    new HashSet<>(allAssignedTasks.get(connector))
            );
        });
    }

    private static <T> void assertNoDuplicates(List<T> collection, String assertionMessage) {
        assertEquals(
                assertionMessage,
                Collections.emptySet(),
                duplicatedElements(collection)
        );
    }

}
