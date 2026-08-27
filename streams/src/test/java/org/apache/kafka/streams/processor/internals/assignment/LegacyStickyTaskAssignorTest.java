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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_RACK_AWARE_ASSIGNMENT_TAGS;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.PID_6;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_6;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_3_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_3_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_3_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertBalancedTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.assertValidAssignment;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.copyClientStateMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getClusterForAllTopics;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getProcessRacksForAllProcess;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRackAwareTaskAssignor;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomClientState;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomCluster;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomProcessRacks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getRandomSubset;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTaskChangelogMapForAllTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTaskTopicPartitionMap;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTaskTopicPartitionMapForAllTasks;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.getTasksForTopicGroup;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.mockInternalTopicManagerForChangelog;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.mockInternalTopicManagerForRandomChangelog;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.verifyTaskPlacementWithRackAwareAssignor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;

public class LegacyStickyTaskAssignorTest {

    private final List<Integer> expectedTopicGroupIds = asList(1, 2);
    private final Time time = new MockTime();
    private final Map<ProcessId, ClientState> clients = new TreeMap<>();
    private boolean enableRackAwareTaskAssignor;

    public void setUp(final String rackAwareStrategy) {
        enableRackAwareTaskAssignor = !rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignOneActiveTaskToEachProcessWhenTaskCountSameAsProcessCount(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClient(PID_1, 1);
        createClient(PID_2, 1);
        createClient(PID_3, 1);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2);
        assertFalse(probingRebalanceNeeded);

        for (final ClientState clientState : clients.values()) {
            assertEquals(1, clientState.activeTaskCount());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithNoStandByTasks(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClient(PID_1, 2);
        createClient(PID_2, 2);
        createClient(PID_3, 2);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_1_0, TASK_1_1, TASK_2_2, TASK_2_0, TASK_2_1, TASK_1_2);
        assertFalse(probingRebalanceNeeded);

        assertActiveTaskTopicGroupIdsEvenlyDistributed();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithStandByTasks(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClient(PID_1, 2);
        createClient(PID_2, 2);
        createClient(PID_3, 2);

        final boolean probingRebalanceNeeded = assign(1, rackAwareStrategy, TASK_2_0, TASK_1_1, TASK_1_2, TASK_1_0, TASK_2_1, TASK_2_2);
        assertFalse(probingRebalanceNeeded);

        assertActiveTaskTopicGroupIdsEvenlyDistributed();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldNotMigrateActiveTaskToOtherProcess(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_1);

        assertFalse(assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2));

        assertTrue(clients.get(PID_1).activeTasks().contains(TASK_0_0));
        assertTrue(clients.get(PID_2).activeTasks().contains(TASK_0_1));
        assertEquals(asList(TASK_0_0, TASK_0_1, TASK_0_2), allActiveTasks());

        clients.clear();

        // flip the previous active tasks assignment around.
        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_1);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_2);

        assertFalse(assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2));

        assertTrue(clients.get(PID_1).activeTasks().contains(TASK_0_1));
        assertTrue(clients.get(PID_2).activeTasks().contains(TASK_0_2));
        assertEquals(asList(TASK_0_0, TASK_0_1, TASK_0_2), allActiveTasks());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldMigrateActiveTasksToNewProcessWithoutChangingAllAssignments(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0, TASK_0_2);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_1);
        createClient(PID_3, 1);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2);

        assertFalse(probingRebalanceNeeded);
        assertEquals(singleton(TASK_0_1), clients.get(PID_2).activeTasks());
        assertEquals(1, clients.get(PID_1).activeTasks().size());
        assertEquals(1, clients.get(PID_3).activeTasks().size());
        assertEquals(asList(TASK_0_0, TASK_0_1, TASK_0_2), allActiveTasks());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignBasedOnCapacity(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClient(PID_1, 1);
        createClient(PID_2, 2);
        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2);

        assertFalse(probingRebalanceNeeded);
        assertEquals(1, clients.get(PID_1).activeTasks().size());
        assertEquals(2, clients.get(PID_2).activeTasks().size());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignTasksEvenlyWithUnequalTopicGroupSizes(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5, TASK_1_0);

        createClient(PID_2, 1);

        assertFalse(assign(rackAwareStrategy, TASK_1_0, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5));

        final Set<TaskId> allTasks = new HashSet<>(asList(TASK_0_0, TASK_0_1, TASK_1_0, TASK_0_5, TASK_0_2, TASK_0_3, TASK_0_4));
        final Set<TaskId> client1Tasks = clients.get(PID_1).activeTasks();
        final Set<TaskId> client2Tasks = clients.get(PID_2).activeTasks();

        // one client should get 3 tasks and the other should have 4
        assertTrue((client1Tasks.size() == 3 && client2Tasks.size() == 4) ||
                (client1Tasks.size() == 4 && client2Tasks.size() == 3));
        allTasks.removeAll(client1Tasks);
        // client2 should have all the remaining tasks not assigned to client 1
        assertEquals(allTasks, client2Tasks);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldKeepActiveTaskStickinessWhenMoreClientThanActiveTasks(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_2);
        createClientWithPreviousActiveTasks(PID_3, 1, TASK_0_1);
        createClient(PID_4, 1);
        createClient(PID_5, 1);

        assertFalse(assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2));

        assertEquals(singleton(TASK_0_0), clients.get(PID_1).activeTasks());
        assertEquals(singleton(TASK_0_2), clients.get(PID_2).activeTasks());
        assertEquals(singleton(TASK_0_1), clients.get(PID_3).activeTasks());

        // change up the assignment and make sure it is still sticky
        clients.clear();
        createClient(PID_1, 1);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_0);
        createClient(PID_3, 1);
        createClientWithPreviousActiveTasks(PID_4, 1, TASK_0_2);
        createClientWithPreviousActiveTasks(PID_5, 1, TASK_0_1);

        assertFalse(assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2));

        assertEquals(singleton(TASK_0_0), clients.get(PID_2).activeTasks());
        assertEquals(singleton(TASK_0_2), clients.get(PID_4).activeTasks());
        assertEquals(singleton(TASK_0_1), clients.get(PID_5).activeTasks());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignTasksToClientWithPreviousStandbyTasks(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final ClientState client1 = createClient(PID_1, 1);
        client1.addPreviousStandbyTasks(Set.of(TASK_0_2));
        final ClientState client2 = createClient(PID_2, 1);
        client2.addPreviousStandbyTasks(Set.of(TASK_0_1));
        final ClientState client3 = createClient(PID_3, 1);
        client3.addPreviousStandbyTasks(Set.of(TASK_0_0));

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2);

        assertFalse(probingRebalanceNeeded);

        assertEquals(singleton(TASK_0_2), clients.get(PID_1).activeTasks());
        assertEquals(singleton(TASK_0_1), clients.get(PID_2).activeTasks());
        assertEquals(singleton(TASK_0_0), clients.get(PID_3).activeTasks());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignBasedOnCapacityWhenMultipleClientHaveStandbyTasks(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final ClientState c1 = createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0);
        c1.addPreviousStandbyTasks(Set.of(TASK_0_1));
        final ClientState c2 = createClientWithPreviousActiveTasks(PID_2, 2, TASK_0_2);
        c2.addPreviousStandbyTasks(Set.of(TASK_0_1));

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2);

        assertFalse(probingRebalanceNeeded);

        assertEquals(singleton(TASK_0_0), clients.get(PID_1).activeTasks());
        assertEquals(Set.of(TASK_0_2, TASK_0_1), clients.get(PID_2).activeTasks());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignStandbyTasksToDifferentClientThanCorrespondingActiveTaskIsAssignedTo(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_1);
        createClientWithPreviousActiveTasks(PID_3, 1, TASK_0_2);
        createClientWithPreviousActiveTasks(PID_4, 1, TASK_0_3);

        final boolean probingRebalanceNeeded = assign(1, rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        assertFalse(probingRebalanceNeeded);


        assertFalse(clients.get(PID_1).standbyTasks().contains(TASK_0_0));
        assertTrue(clients.get(PID_1).standbyTasks().size() <= 2);
        assertFalse(clients.get(PID_2).standbyTasks().contains(TASK_0_1));
        assertTrue(clients.get(PID_2).standbyTasks().size() <= 2);
        assertFalse(clients.get(PID_3).standbyTasks().contains(TASK_0_2));
        assertTrue(clients.get(PID_3).standbyTasks().size() <= 2);
        assertFalse(clients.get(PID_4).standbyTasks().contains(TASK_0_3));
        assertTrue(clients.get(PID_4).standbyTasks().size() <= 2);

        int nonEmptyStandbyTaskCount = 0;
        for (final ClientState clientState : clients.values()) {
            nonEmptyStandbyTaskCount += clientState.standbyTasks().isEmpty() ? 0 : 1;
        }

        assertTrue(nonEmptyStandbyTaskCount >= 3);
        assertEquals(asList(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3), allStandbyTasks());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignMultipleReplicasOfStandbyTask(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_1);
        createClientWithPreviousActiveTasks(PID_3, 1, TASK_0_2);

        final boolean probingRebalanceNeeded = assign(2, rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2);
        assertFalse(probingRebalanceNeeded);

        assertEquals(Set.of(TASK_0_1, TASK_0_2), clients.get(PID_1).standbyTasks());
        assertEquals(Set.of(TASK_0_2, TASK_0_0), clients.get(PID_2).standbyTasks());
        assertEquals(Set.of(TASK_0_0, TASK_0_1), clients.get(PID_3).standbyTasks());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldNotAssignStandbyTaskReplicasWhenNoClientAvailableWithoutHavingTheTaskAssigned(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClient(PID_1, 1);
        final boolean probingRebalanceNeeded = assign(1, rackAwareStrategy, TASK_0_0);
        assertFalse(probingRebalanceNeeded);
        assertEquals(0, clients.get(PID_1).standbyTasks().size());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignActiveAndStandbyTasks(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClient(PID_1, 1);
        createClient(PID_2, 1);
        createClient(PID_3, 1);

        final boolean probingRebalanceNeeded = assign(1, rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2);
        assertFalse(probingRebalanceNeeded);

        assertEquals(asList(TASK_0_0, TASK_0_1, TASK_0_2), allActiveTasks());
        assertEquals(asList(TASK_0_0, TASK_0_1, TASK_0_2), allStandbyTasks());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignAtLeastOneTaskToEachClientIfPossible(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClient(PID_1, 3);
        createClient(PID_2, 1);
        createClient(PID_3, 1);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2);
        assertFalse(probingRebalanceNeeded);
        assertEquals(1, clients.get(PID_1).assignedTaskCount());
        assertEquals(1, clients.get(PID_2).assignedTaskCount());
        assertEquals(1, clients.get(PID_3).assignedTaskCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignEachActiveTaskToOneClientWhenMoreClientsThanTasks(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClient(PID_1, 1);
        createClient(PID_2, 1);
        createClient(PID_3, 1);
        createClient(PID_4, 1);
        createClient(PID_5, 1);
        createClient(PID_6, 1);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2);
        assertFalse(probingRebalanceNeeded);

        assertEquals(asList(TASK_0_0, TASK_0_1, TASK_0_2), allActiveTasks());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldBalanceActiveAndStandbyTasksAcrossAvailableClients(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClient(PID_1, 1);
        createClient(PID_2, 1);
        createClient(PID_3, 1);
        createClient(PID_4, 1);
        createClient(PID_5, 1);
        createClient(PID_6, 1);

        final boolean probingRebalanceNeeded = assign(1, rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2);
        assertFalse(probingRebalanceNeeded);

        for (final ClientState clientState : clients.values()) {
            assertEquals(1, clientState.assignedTaskCount());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignMoreTasksToClientWithMoreCapacity(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClient(PID_2, 2);
        createClient(PID_1, 1);

        final boolean probingRebalanceNeeded = assign(
            rackAwareStrategy,
            TASK_0_0,
            TASK_0_1,
            TASK_0_2,
            TASK_1_0,
            TASK_1_1,
            TASK_1_2,
            TASK_2_0,
            TASK_2_1,
            TASK_2_2,
            TASK_3_0,
            TASK_3_1,
            TASK_3_2
        );

        assertFalse(probingRebalanceNeeded);
        assertEquals(8, clients.get(PID_2).assignedTaskCount());
        assertEquals(4, clients.get(PID_1).assignedTaskCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldEvenlyDistributeByTaskIdAndPartition(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClient(PID_1, 4);
        createClient(PID_2, 4);
        createClient(PID_3, 4);
        createClient(PID_4, 4);

        final List<TaskId> taskIds = new ArrayList<>();
        final TaskId[] taskIdArray = new TaskId[16];

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 8; j++) {
                taskIds.add(new TaskId(i, j));
            }
        }

        Collections.shuffle(taskIds);
        taskIds.toArray(taskIdArray);

        final int nodeSize = 5;
        final int topicSize = 2;
        final int partitionSize = 8;
        final int clientSize = 4;
        final Cluster cluster = getRandomCluster(nodeSize, topicSize, partitionSize);
        final Map<TaskId, Set<TopicPartition>> partitionsForTask = getTaskTopicPartitionMap(topicSize, partitionSize, false);
        final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask = getTaskTopicPartitionMap(topicSize, partitionSize, true);
        final Map<ProcessId, Map<String, Optional<String>>> racksForProcessConsumer = getRandomProcessRacks(clientSize, nodeSize);
        final InternalTopicManager internalTopicManager = mockInternalTopicManagerForRandomChangelog(nodeSize, topicSize, partitionSize);
        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            1,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = new RackAwareTaskAssignor(
            cluster,
            partitionsForTask,
            changelogPartitionsForTask,
            getTasksForTopicGroup(topicSize, partitionSize),
            racksForProcessConsumer,
            internalTopicManager,
            configs,
            time
        );

        final boolean probingRebalanceNeeded = assign(configs, rackAwareTaskAssignor, taskIdArray);
        assertFalse(probingRebalanceNeeded);

        Collections.sort(taskIds);
        final Set<TaskId> expectedClientOneAssignment = getExpectedTaskIdAssignment(taskIds, 0, 4, 8, 12);
        final Set<TaskId> expectedClientTwoAssignment = getExpectedTaskIdAssignment(taskIds, 1, 5, 9, 13);
        final Set<TaskId> expectedClientThreeAssignment = getExpectedTaskIdAssignment(taskIds, 2, 6, 10, 14);
        final Set<TaskId> expectedClientFourAssignment = getExpectedTaskIdAssignment(taskIds, 3, 7, 11, 15);

        final Map<ProcessId, Set<TaskId>> sortedAssignments = sortClientAssignments(clients);

        assertEquals(expectedClientOneAssignment, sortedAssignments.get(PID_1));
        assertEquals(expectedClientTwoAssignment, sortedAssignments.get(PID_2));
        assertEquals(expectedClientThreeAssignment, sortedAssignments.get(PID_3));
        assertEquals(expectedClientFourAssignment, sortedAssignments.get(PID_4));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldNotHaveSameAssignmentOnAnyTwoHosts(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final List<ProcessId> allProcessIds = asList(PID_1, PID_2, PID_3, PID_4);
        createClient(PID_1, 1);
        createClient(PID_2, 1);
        createClient(PID_3, 1);
        createClient(PID_4, 1);

        final boolean probingRebalanceNeeded = assign(1, rackAwareStrategy, TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertFalse(probingRebalanceNeeded);

        for (final ProcessId uuid : allProcessIds) {
            final Set<TaskId> taskIds = clients.get(uuid).assignedTasks();
            for (final ProcessId otherProcessId : allProcessIds) {
                if (!uuid.equals(otherProcessId)) {
                    assertNotEquals(taskIds, clients.get(otherProcessId).assignedTasks(), "clients shouldn't have same task assignment");
                }
            }

        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousActiveTasks(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final List<ProcessId> allProcessIds = asList(PID_1, PID_2, PID_3);
        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_1, TASK_0_2);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_3);
        createClientWithPreviousActiveTasks(PID_3, 1, TASK_0_0);
        createClient(PID_4, 1);

        final boolean probingRebalanceNeeded = assign(1, rackAwareStrategy, TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertFalse(probingRebalanceNeeded);

        for (final ProcessId uuid : allProcessIds) {
            final Set<TaskId> taskIds = clients.get(uuid).assignedTasks();
            for (final ProcessId otherProcessId : allProcessIds) {
                if (!uuid.equals(otherProcessId)) {
                    assertNotEquals(taskIds, clients.get(otherProcessId).assignedTasks(), "clients shouldn't have same task assignment");
                }
            }

        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousStandbyTasks(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final List<ProcessId> allProcessIds = asList(PID_1, PID_2, PID_3, PID_4);

        final ClientState c1 = createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_1, TASK_0_2);
        c1.addPreviousStandbyTasks(Set.of(TASK_0_3, TASK_0_0));
        final ClientState c2 = createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_3, TASK_0_0);
        c2.addPreviousStandbyTasks(Set.of(TASK_0_1, TASK_0_2));

        createClient(PID_3, 1);
        createClient(PID_4, 1);

        final boolean probingRebalanceNeeded = assign(1, rackAwareStrategy, TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertFalse(probingRebalanceNeeded);

        for (final ProcessId uuid : allProcessIds) {
            final Set<TaskId> taskIds = clients.get(uuid).assignedTasks();
            for (final ProcessId otherProcessId : allProcessIds) {
                if (!uuid.equals(otherProcessId)) {
                    assertNotEquals(taskIds, clients.get(otherProcessId).assignedTasks(), "clients shouldn't have same task assignment");
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldReBalanceTasksAcrossAllClientsWhenCapacityAndTaskCountTheSame(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClientWithPreviousActiveTasks(PID_3, 1, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        createClient(PID_1, 1);
        createClient(PID_2, 1);
        createClient(PID_4, 1);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertFalse(probingRebalanceNeeded);

        assertEquals(1, clients.get(PID_1).assignedTaskCount());
        assertEquals(1, clients.get(PID_2).assignedTaskCount());
        assertEquals(1, clients.get(PID_3).assignedTaskCount());
        assertEquals(1, clients.get(PID_4).assignedTaskCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldReBalanceTasksAcrossClientsWhenCapacityLessThanTaskCount(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClientWithPreviousActiveTasks(PID_3, 1, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        createClient(PID_1, 1);
        createClient(PID_2, 1);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertFalse(probingRebalanceNeeded);

        assertEquals(2, clients.get(PID_3).assignedTaskCount());
        assertEquals(1, clients.get(PID_1).assignedTaskCount());
        assertEquals(1, clients.get(PID_2).assignedTaskCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldRebalanceTasksToClientsBasedOnCapacity(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_0, TASK_0_3, TASK_0_2);
        createClient(PID_3, 2);
        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_2, TASK_0_3);
        assertFalse(probingRebalanceNeeded);
        assertEquals(1, clients.get(PID_2).assignedTaskCount());
        assertEquals(2, clients.get(PID_3).assignedTaskCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldMoveMinimalNumberOfTasksWhenPreviouslyAboveCapacityAndNewClientAdded(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final Set<TaskId> p1PrevTasks = new HashSet<>(List.of(TASK_0_0, TASK_0_2));
        final Set<TaskId> p2PrevTasks = Set.of(TASK_0_1, TASK_0_3);

        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0, TASK_0_2);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_1, TASK_0_3);
        createClientWithPreviousActiveTasks(PID_3, 1);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertFalse(probingRebalanceNeeded);

        final Set<TaskId> p3ActiveTasks = clients.get(PID_3).activeTasks();
        assertEquals(1, p3ActiveTasks.size());
        if (p1PrevTasks.removeAll(p3ActiveTasks)) {
            assertEquals(p2PrevTasks, clients.get(PID_2).activeTasks());
        } else {
            assertEquals(p1PrevTasks, clients.get(PID_1).activeTasks());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldNotMoveAnyTasksWhenNewTasksAdded(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0, TASK_0_1);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_2, TASK_0_3);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_3, TASK_0_1, TASK_0_4, TASK_0_2, TASK_0_0, TASK_0_5);
        assertFalse(probingRebalanceNeeded);

        assertTrue(clients.get(PID_1).activeTasks().containsAll(List.of(TASK_0_0, TASK_0_1)));
        assertTrue(clients.get(PID_2).activeTasks().containsAll(List.of(TASK_0_2, TASK_0_3)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignNewTasksToNewClientWhenPreviousTasksAssignedToOldClients(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);

        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_2, TASK_0_1);
        createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_0, TASK_0_3);
        createClient(PID_3, 1);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_3, TASK_0_1, TASK_0_4, TASK_0_2, TASK_0_0, TASK_0_5);
        assertFalse(probingRebalanceNeeded);

        assertTrue(clients.get(PID_1).activeTasks().containsAll(List.of(TASK_0_2, TASK_0_1)));
        assertTrue(clients.get(PID_2).activeTasks().containsAll(List.of(TASK_0_0, TASK_0_3)));
        assertTrue(clients.get(PID_3).activeTasks().containsAll(List.of(TASK_0_4, TASK_0_5)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignTasksNotPreviouslyActiveToNewClient(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final ClientState c1 = createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_1, TASK_1_2, TASK_1_3);
        c1.addPreviousStandbyTasks(Set.of(TASK_0_0, TASK_1_1, TASK_2_0, TASK_2_1, TASK_2_3));
        final ClientState c2 = createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_0, TASK_1_1, TASK_2_2);
        c2.addPreviousStandbyTasks(Set.of(TASK_0_1, TASK_1_0, TASK_0_2, TASK_2_0, TASK_0_3, TASK_1_2, TASK_2_1, TASK_1_3, TASK_2_3));
        final ClientState c3 = createClientWithPreviousActiveTasks(PID_3, 1, TASK_2_0, TASK_2_1, TASK_2_3);
        c3.addPreviousStandbyTasks(Set.of(TASK_0_2, TASK_1_2));

        final ClientState newClient = createClient(PID_4, 1);
        newClient.addPreviousStandbyTasks(Set.of(TASK_0_0, TASK_1_0, TASK_0_1, TASK_0_2, TASK_1_1, TASK_2_0, TASK_0_3, TASK_1_2, TASK_2_1, TASK_1_3, TASK_2_2, TASK_2_3));

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_1_0, TASK_0_1, TASK_0_2, TASK_1_1, TASK_2_0, TASK_0_3, TASK_1_2, TASK_2_1, TASK_1_3, TASK_2_2, TASK_2_3);
        assertFalse(probingRebalanceNeeded);

        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            assertEquals(Set.of(TASK_0_1, TASK_1_2, TASK_2_3), c1.activeTasks());
            assertEquals(Set.of(TASK_0_0, TASK_1_1, TASK_2_2), c2.activeTasks());
            assertEquals(Set.of(TASK_0_2, TASK_1_3, TASK_2_1), c3.activeTasks());
            assertEquals(Set.of(TASK_0_3, TASK_1_0, TASK_2_0), newClient.activeTasks());
        } else {
            assertEquals(Set.of(TASK_0_1, TASK_1_2, TASK_1_3), c1.activeTasks());
            assertEquals(Set.of(TASK_0_0, TASK_1_1, TASK_2_2), c2.activeTasks());
            assertEquals(Set.of(TASK_2_0, TASK_2_1, TASK_2_3), c3.activeTasks());
            assertEquals(Set.of(TASK_0_2, TASK_0_3, TASK_1_0), newClient.activeTasks());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignTasksNotPreviouslyActiveToMultipleNewClients(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final ClientState c1 = createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_1, TASK_1_2, TASK_1_3);
        c1.addPreviousStandbyTasks(Set.of(TASK_0_0, TASK_1_1, TASK_2_0, TASK_2_1, TASK_2_3));
        final ClientState c2 = createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_0, TASK_1_1, TASK_2_2);
        c2.addPreviousStandbyTasks(Set.of(TASK_0_1, TASK_1_0, TASK_0_2, TASK_2_0, TASK_0_3, TASK_1_2, TASK_2_1, TASK_1_3, TASK_2_3));

        final ClientState bounce1 = createClient(PID_3, 1);
        bounce1.addPreviousStandbyTasks(Set.of(TASK_2_0, TASK_2_1, TASK_2_3));

        final ClientState bounce2 = createClient(PID_4, 1);
        bounce2.addPreviousStandbyTasks(Set.of(TASK_0_2, TASK_0_3, TASK_1_0));

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_1_0, TASK_0_1, TASK_0_2, TASK_1_1, TASK_2_0, TASK_0_3, TASK_1_2, TASK_2_1, TASK_1_3, TASK_2_2, TASK_2_3);
        assertFalse(probingRebalanceNeeded);

        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            assertEquals(Set.of(TASK_0_1, TASK_1_2, TASK_2_3), c1.activeTasks());
            assertEquals(Set.of(TASK_0_0, TASK_1_1, TASK_2_2), c2.activeTasks());
            assertEquals(Set.of(TASK_0_2, TASK_1_3, TASK_2_1), bounce1.activeTasks());
            assertEquals(Set.of(TASK_0_3, TASK_1_0, TASK_2_0), bounce2.activeTasks());
        } else {
            assertEquals(Set.of(TASK_0_1, TASK_1_2, TASK_1_3), c1.activeTasks());
            assertEquals(Set.of(TASK_0_0, TASK_1_1, TASK_2_2), c2.activeTasks());
            assertEquals(Set.of(TASK_2_0, TASK_2_1, TASK_2_3), bounce1.activeTasks());
            assertEquals(Set.of(TASK_0_2, TASK_0_3, TASK_1_0), bounce2.activeTasks());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignTasksToNewClient(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_1, TASK_0_2);
        createClient(PID_2, 1);
        assertFalse(assign(rackAwareStrategy, TASK_0_1, TASK_0_2));
        assertEquals(1, clients.get(PID_1).activeTaskCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingClients(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final ClientState c1 = createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState c2 = createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_3, TASK_0_4, TASK_0_5);
        final ClientState newClient = createClient(PID_3, 1);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5);
        assertFalse(probingRebalanceNeeded);
        assertFalse(c1.activeTasks().contains(TASK_0_3));
        assertFalse(c1.activeTasks().contains(TASK_0_4));
        assertFalse(c1.activeTasks().contains(TASK_0_5));
        assertEquals(2, c1.activeTaskCount());
        assertFalse(c2.activeTasks().contains(TASK_0_0));
        assertFalse(c2.activeTasks().contains(TASK_0_1));
        assertFalse(c2.activeTasks().contains(TASK_0_2));
        assertEquals(2, c2.activeTaskCount());
        assertEquals(2, newClient.activeTaskCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingAndBouncedClients(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final ClientState c1 = createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_6);
        final ClientState c2 = createClient(PID_2, 1);
        c2.addPreviousStandbyTasks(Set.of(TASK_0_3, TASK_0_4, TASK_0_5));
        final ClientState newClient = createClient(PID_3, 1);

        final boolean probingRebalanceNeeded = assign(rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5, TASK_0_6);
        assertFalse(probingRebalanceNeeded);

        // it's possible for either client 1 or 2 to get three tasks since they both had three previously assigned
        assertFalse(c1.activeTasks().contains(TASK_0_3));
        assertFalse(c1.activeTasks().contains(TASK_0_4));
        assertFalse(c1.activeTasks().contains(TASK_0_5));
        assertTrue(c1.activeTaskCount() >= 2);
        assertFalse(c2.activeTasks().contains(TASK_0_0));
        assertFalse(c2.activeTasks().contains(TASK_0_1));
        assertFalse(c2.activeTasks().contains(TASK_0_2));
        assertTrue(c2.activeTaskCount() >= 2);
        assertEquals(2, newClient.activeTaskCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldViolateBalanceToPreserveActiveTaskStickiness(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final ClientState c1 = createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState c2 = createClient(PID_2, 1);

        final List<TaskId> taskIds = asList(TASK_0_0, TASK_0_1, TASK_0_2);
        Collections.shuffle(taskIds);

        final int nodeSize = 5;
        final int topicSize = 1;
        final int partitionSize = 3;
        final int clientSize = 2;
        final Cluster cluster = getRandomCluster(nodeSize, topicSize, partitionSize);
        final Map<TaskId, Set<TopicPartition>> partitionsForTask = getTaskTopicPartitionMap(topicSize, partitionSize, false);
        final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask = getTaskTopicPartitionMap(topicSize, partitionSize, true);
        final Map<ProcessId, Map<String, Optional<String>>> racksForProcessConsumer = getRandomProcessRacks(clientSize, nodeSize);
        final InternalTopicManager internalTopicManager = mockInternalTopicManagerForRandomChangelog(nodeSize, topicSize, partitionSize);

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            0,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = new RackAwareTaskAssignor(
            cluster,
            partitionsForTask,
            changelogPartitionsForTask,
            getTasksForTopicGroup(),
            racksForProcessConsumer,
            internalTopicManager,
            configs,
            time
        );

        final boolean probingRebalanceNeeded = new LegacyStickyTaskAssignor(true).assign(
            clients,
            new HashSet<>(taskIds),
            new HashSet<>(taskIds),
            rackAwareTaskAssignor,
            configs
        );
        assertFalse(probingRebalanceNeeded);

        assertEquals(Set.of(TASK_0_0, TASK_0_1, TASK_0_2), c1.activeTasks());
        assertTrue(c2.activeTasks().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldOptimizeStatefulAndStatelessTaskTraffic(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final ClientState c1 = createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState c2 = createClientWithPreviousActiveTasks(PID_2, 1, TASK_1_0, TASK_1_1, TASK_0_3, TASK_1_3);
        final ClientState c3 = createClientWithPreviousActiveTasks(PID_3, 1, TASK_1_2);

        final List<TaskId> taskIds = asList(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3);
        final List<TaskId> statefulTaskIds = asList(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);
        Collections.shuffle(taskIds);

        final Cluster cluster = getClusterForAllTopics();
        final Map<TaskId, Set<TopicPartition>> partitionsForTask = getTaskTopicPartitionMapForAllTasks();
        final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask = getTaskChangelogMapForAllTasks();
        final Map<ProcessId, Map<String, Optional<String>>> racksForProcessConsumer = getProcessRacksForAllProcess();
        final InternalTopicManager internalTopicManager = mockInternalTopicManagerForChangelog();

        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            1,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            10,
            1,
            rackAwareStrategy
        );
        final RackAwareTaskAssignor rackAwareTaskAssignor = new RackAwareTaskAssignor(
            cluster,
            partitionsForTask,
            changelogPartitionsForTask,
            getTasksForTopicGroup(),
            racksForProcessConsumer,
            internalTopicManager,
            configs,
            time
        );

        final boolean probingRebalanceNeeded = new LegacyStickyTaskAssignor().assign(
            clients,
            new HashSet<>(taskIds),
            new HashSet<>(statefulTaskIds),
            rackAwareTaskAssignor,
            configs
        );
        assertFalse(probingRebalanceNeeded);

        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)) {
            // Total cost for active stateful: 3
            // Total cost for active stateless: 0
            // Total cost for standby: 20
            assertEquals(Set.of(TASK_0_3, TASK_1_0, TASK_1_2), c1.activeTasks());
            assertEquals(Set.of(TASK_0_0, TASK_0_1), c1.standbyTasks());
            assertEquals(Set.of(TASK_0_0, TASK_0_2, TASK_1_1), c2.activeTasks());
            assertTrue(c2.standbyTasks().isEmpty());
            assertEquals(Set.of(TASK_0_1, TASK_1_3), c3.activeTasks());
            assertEquals(Set.of(TASK_1_0, TASK_1_1), c3.standbyTasks());
        } else if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            assertEquals(Set.of(TASK_0_0, TASK_0_3, TASK_1_2), c1.activeTasks());
            assertEquals(Set.of(TASK_1_0), c1.standbyTasks());
            assertEquals(Set.of(TASK_0_1, TASK_0_2, TASK_1_1), c2.activeTasks());
            assertEquals(Set.of(TASK_0_0), c2.standbyTasks());
            assertEquals(Set.of(TASK_1_0, TASK_1_3), c3.activeTasks());
            assertEquals(Set.of(TASK_0_1, TASK_1_1), c3.standbyTasks());
        } else {
            // Total cost for active stateful: 30
            // Total cost for active stateless: 40
            // Total cost for standby: 10
            assertEquals(Set.of(TASK_0_1, TASK_0_2, TASK_1_3), c1.activeTasks());
            assertEquals(Set.of(TASK_0_0), c1.standbyTasks());
            assertEquals(Set.of(TASK_0_3, TASK_1_0, TASK_1_1), c2.activeTasks());
            assertEquals(Set.of(TASK_0_1), c2.standbyTasks());
            assertEquals(Set.of(TASK_0_0, TASK_1_2), c3.activeTasks());
            assertEquals(Set.of(TASK_1_0, TASK_1_1), c3.standbyTasks());
        }

    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldAssignRandomInput(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        final int nodeSize = 50;
        final int tpSize = 60;
        final int partitionSize = 3;
        final int clientSize = 50;
        final int replicaCount = 1;
        final int maxCapacity = 3;
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(
            tpSize, partitionSize, false);
        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            replicaCount,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            10,
            1,
            rackAwareStrategy
        );

        final RackAwareTaskAssignor rackAwareTaskAssignor = spy(new RackAwareTaskAssignor(
            getRandomCluster(nodeSize, tpSize, partitionSize),
            taskTopicPartitionMap,
            getTaskTopicPartitionMap(tpSize, partitionSize, true),
            getTasksForTopicGroup(tpSize, partitionSize),
            getRandomProcessRacks(clientSize, nodeSize),
            mockInternalTopicManagerForRandomChangelog(nodeSize, tpSize, partitionSize),
            configs,
            time
        ));

        final SortedSet<TaskId> taskIds = (SortedSet<TaskId>) taskTopicPartitionMap.keySet();
        final List<Set<TaskId>> statefulAndStatelessTasks = getRandomSubset(taskIds, 2);
        final Set<TaskId> statefulTasks = statefulAndStatelessTasks.get(0);
        final Set<TaskId> statelessTasks = statefulAndStatelessTasks.get(1);
        final SortedMap<ProcessId, ClientState> clientStateMap = getRandomClientState(clientSize,
            tpSize, partitionSize, maxCapacity, false, statefulTasks);


        final boolean probing = new LegacyStickyTaskAssignor().assign(
            clientStateMap,
            taskIds,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        assertFalse(probing);
        assertValidAssignment(
            replicaCount,
            statefulTasks,
            statelessTasks,
            clientStateMap,
            new StringBuilder()
        );
        verifyTaskPlacementWithRackAwareAssignor(rackAwareTaskAssignor, taskIds, clientStateMap, true, enableRackAwareTaskAssignor);
        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            assertBalancedTasks(clientStateMap, 4);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldRemainOriginalAssignmentWithoutTrafficCostForMinCostStrategy(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        // This test tests that if the traffic cost is 0, we should have same assignment with or without
        // rack aware assignor enabled
        final int nodeSize = 50;
        final int tpSize = 60;
        final int partitionSize = 3;
        final int clientSize = 50;
        final int replicaCount = 1;
        final int maxCapacity = 3;
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(
            tpSize, partitionSize, false);
        final Cluster cluster = getRandomCluster(nodeSize, tpSize, partitionSize);
        final Map<TaskId, Set<TopicPartition>> taskChangelogTopicPartitionMap = getTaskTopicPartitionMap(tpSize, partitionSize, true);
        final Map<ProcessId, Map<String, Optional<String>>> processRackMap = getRandomProcessRacks(clientSize, nodeSize);
        final InternalTopicManager mockInternalTopicManager = mockInternalTopicManagerForRandomChangelog(nodeSize, tpSize, partitionSize);

        AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            replicaCount,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            0, // Override traffic cost to 0 to maintain original assignment
            10,
            rackAwareStrategy
        );

        RackAwareTaskAssignor rackAwareTaskAssignor = spy(new RackAwareTaskAssignor(
            cluster,
            taskTopicPartitionMap,
            taskChangelogTopicPartitionMap,
            getTasksForTopicGroup(tpSize, partitionSize),
            processRackMap,
            mockInternalTopicManager,
            configs,
            time
        ));

        final SortedSet<TaskId> taskIds = (SortedSet<TaskId>) taskTopicPartitionMap.keySet();
        final List<Set<TaskId>> statefulAndStatelessTasks = getRandomSubset(taskIds, 2);
        final Set<TaskId> statefulTasks = statefulAndStatelessTasks.get(0);
        final Set<TaskId> statelessTasks = statefulAndStatelessTasks.get(1);
        final SortedMap<ProcessId, ClientState> clientStateMap = getRandomClientState(clientSize,
            tpSize, partitionSize, maxCapacity, false, statefulTasks);

        new LegacyStickyTaskAssignor().assign(
            clientStateMap,
            taskIds,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        assertValidAssignment(1, statefulTasks, statelessTasks, clientStateMap, new StringBuilder());
        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE)) {
            return;
        }
        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            // Original assignment won't be maintained because we calculate the assignment using max flow first
            // in balance subtopology strategy
            assertBalancedTasks(clientStateMap, 4);
            return;
        }

        final SortedMap<ProcessId, ClientState> clientStateMapCopy = copyClientStateMap(clientStateMap);
        configs = new AssignmentConfigs(
            0L,
            1,
            replicaCount,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            0,
            10,
            StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE
        );

        rackAwareTaskAssignor = spy(new RackAwareTaskAssignor(
            cluster,
            taskTopicPartitionMap,
            taskChangelogTopicPartitionMap,
            getTasksForTopicGroup(tpSize, partitionSize),
            processRackMap,
            mockInternalTopicManager,
            configs,
            time
        ));

        new LegacyStickyTaskAssignor().assign(
            clientStateMapCopy,
            taskIds,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        for (final Map.Entry<ProcessId, ClientState> entry : clientStateMap.entrySet()) {
            assertEquals(clientStateMapCopy.get(entry.getKey()).statefulActiveTasks(), entry.getValue().statefulActiveTasks());
            assertEquals(clientStateMapCopy.get(entry.getKey()).standbyTasks(), entry.getValue().standbyTasks());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldReassignTasksWhenNewNodeJoinsWithExistingActiveAndStandbyAssignments(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);

        // Initial setup: Node 1 has active tasks 0,1 and standby tasks 2,3
        // Node 2 has active tasks 2,3 and standby tasks 0,1
        final ClientState node1 = createClientWithPreviousActiveTasks(PID_1, 1, TASK_0_0, TASK_0_1);
        node1.addPreviousStandbyTasks(Set.of(TASK_0_2, TASK_0_3));

        final ClientState node2 = createClientWithPreviousActiveTasks(PID_2, 1, TASK_0_2, TASK_0_3);
        node2.addPreviousStandbyTasks(Set.of(TASK_0_0, TASK_0_1));

        // Node 3 joins as new client
        final ClientState node3 = createClient(PID_3, 1);

        final boolean probingRebalanceNeeded = assign(1, rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        assertFalse(probingRebalanceNeeded);

        // Verify all active tasks are assigned
        final Set<TaskId> allAssignedActiveTasks = new HashSet<>();
        allAssignedActiveTasks.addAll(node1.activeTasks());
        allAssignedActiveTasks.addAll(node2.activeTasks());
        allAssignedActiveTasks.addAll(node3.activeTasks());
        assertEquals(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3), allAssignedActiveTasks);

        // Verify all standby tasks are assigned
        final Set<TaskId> allAssignedStandbyTasks = new HashSet<>();
        allAssignedStandbyTasks.addAll(node1.standbyTasks());
        allAssignedStandbyTasks.addAll(node2.standbyTasks());
        allAssignedStandbyTasks.addAll(node3.standbyTasks());
        assertEquals(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3), allAssignedStandbyTasks);

        // Verify each client has 1-2 active tasks and at most 3 tasks total
        assertTrue(node1.activeTasks().size() >= 1);
        assertTrue(node1.activeTasks().size() <= 2);
        assertTrue(node1.activeTasks().size() + node1.standbyTasks().size() <= 3);

        assertTrue(node2.activeTasks().size() >= 1);
        assertTrue(node2.activeTasks().size() <= 2);
        assertTrue(node2.activeTasks().size() + node2.standbyTasks().size() <= 3);

        assertTrue(node3.activeTasks().size() >= 1);
        assertTrue(node3.activeTasks().size() <= 2);
        assertTrue(node3.activeTasks().size() + node3.standbyTasks().size() <= 3);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY
    })
    public void shouldRangeAssignTasksWhenStartingEmpty(final String rackAwareStrategy) {
        setUp(rackAwareStrategy);
        
        // Two clients with capacity 1 each, starting empty (no previous tasks)
        createClient(PID_1, 1);
        createClient(PID_2, 1);
        
        // Two subtopologies with 2 tasks each (4 tasks total)
        final boolean probingRebalanceNeeded = assign(1, rackAwareStrategy, TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);
        assertFalse(probingRebalanceNeeded);
        
        // Each client should get one active task from each subtopology
        final ClientState client1 = clients.get(PID_1);
        final ClientState client2 = clients.get(PID_2);
        
        // Check that each client has one active task from subtopology 0
        final long client1Subtopology0ActiveCount = client1.activeTasks().stream()
            .filter(task -> task.subtopology() == 0)
            .count();
        final long client2Subtopology0ActiveCount = client2.activeTasks().stream()
            .filter(task -> task.subtopology() == 0)
            .count();
        assertEquals(1L, client1Subtopology0ActiveCount);
        assertEquals(1L, client2Subtopology0ActiveCount);
        
        // Check that each client has one active task from subtopology 1
        final long client1Subtopology1ActiveCount = client1.activeTasks().stream()
            .filter(task -> task.subtopology() == 1)
            .count();
        final long client2Subtopology1ActiveCount = client2.activeTasks().stream()
            .filter(task -> task.subtopology() == 1)
            .count();
        assertEquals(1L, client1Subtopology1ActiveCount);
        assertEquals(1L, client2Subtopology1ActiveCount);
        
        // Check that each client has one standby task from subtopology 0
        final long client1Subtopology0StandbyCount = client1.standbyTasks().stream()
            .filter(task -> task.subtopology() == 0)
            .count();
        final long client2Subtopology0StandbyCount = client2.standbyTasks().stream()
            .filter(task -> task.subtopology() == 0)
            .count();
        assertEquals(1L, client1Subtopology0StandbyCount);
        assertEquals(1L, client2Subtopology0StandbyCount);
        
        // Check that each client has one standby task from subtopology 1
        final long client1Subtopology1StandbyCount = client1.standbyTasks().stream()
            .filter(task -> task.subtopology() == 1)
            .count();
        final long client2Subtopology1StandbyCount = client2.standbyTasks().stream()
            .filter(task -> task.subtopology() == 1)
            .count();
        assertEquals(1L, client1Subtopology1StandbyCount);
        assertEquals(1L, client2Subtopology1StandbyCount);
    }

    private boolean assign(final String rackAwareStrategy, final TaskId... tasks) {
        return assign(0, rackAwareStrategy, tasks);
    }

    private boolean assign(final int numStandbys, final String rackAwareStrategy, final TaskId... tasks) {
        final List<TaskId> taskIds = asList(tasks);
        Collections.shuffle(taskIds);
        final AssignmentConfigs configs = new AssignmentConfigs(
            0L,
            1,
            numStandbys,
            60_000L,
            EMPTY_RACK_AWARE_ASSIGNMENT_TAGS,
            null,
            null,
            rackAwareStrategy
        );

        return assign(configs, getRackAwareTaskAssignor(configs, getTasksForTopicGroup()), tasks);
    }

    private boolean assign(final AssignmentConfigs configs, final RackAwareTaskAssignor rackAwareTaskAssignor, final TaskId... tasks) {
        final List<TaskId> taskIds = asList(tasks);
        Collections.shuffle(taskIds);
        return new LegacyStickyTaskAssignor().assign(
            clients,
            new HashSet<>(taskIds),
            new HashSet<>(taskIds),
            rackAwareTaskAssignor,
            configs
        );
    }

    private List<TaskId> allActiveTasks() {
        final List<TaskId> allActive = new ArrayList<>();
        for (final ClientState client : clients.values()) {
            allActive.addAll(client.activeTasks());
        }
        Collections.sort(allActive);
        return allActive;
    }

    private List<TaskId> allStandbyTasks() {
        final List<TaskId> tasks = new ArrayList<>();
        for (final ClientState client : clients.values()) {
            tasks.addAll(client.standbyTasks());
        }
        Collections.sort(tasks);
        return tasks;
    }

    private ClientState createClient(final ProcessId processId, final int capacity) {
        return createClientWithPreviousActiveTasks(processId, capacity);
    }

    private ClientState createClientWithPreviousActiveTasks(final ProcessId processId, final int capacity, final TaskId... taskIds) {
        final ClientState clientState = new ClientState(processId, capacity);
        clientState.addPreviousActiveTasks(Set.of(taskIds));
        clients.put(processId, clientState);
        return clientState;
    }

    private void assertActiveTaskTopicGroupIdsEvenlyDistributed() {
        for (final Map.Entry<ProcessId, ClientState> clientStateEntry : clients.entrySet()) {
            final List<Integer> topicGroupIds = new ArrayList<>();
            final Set<TaskId> activeTasks = clientStateEntry.getValue().activeTasks();
            for (final TaskId activeTask : activeTasks) {
                topicGroupIds.add(activeTask.subtopology());
            }
            Collections.sort(topicGroupIds);
            assertEquals(expectedTopicGroupIds, topicGroupIds);
        }
    }

    private static Map<ProcessId, Set<TaskId>> sortClientAssignments(final Map<ProcessId, ClientState> clients) {
        final Map<ProcessId, Set<TaskId>> sortedAssignments = new HashMap<>();
        for (final Map.Entry<ProcessId, ClientState> entry : clients.entrySet()) {
            final Set<TaskId> sorted = new TreeSet<>(entry.getValue().activeTasks());
            sortedAssignments.put(entry.getKey(), sorted);
        }
        return sortedAssignments;
    }

    private static Set<TaskId> getExpectedTaskIdAssignment(final List<TaskId> tasks, final int... indices) {
        final Set<TaskId> sortedAssignment = new TreeSet<>();
        for (final int index : indices) {
            sortedAssignment.add(tasks.get(index));
        }
        return sortedAssignment;
    }
}
