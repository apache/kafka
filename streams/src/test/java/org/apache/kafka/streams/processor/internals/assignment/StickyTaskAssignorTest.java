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

import java.util.Collection;
import java.util.Optional;
import java.util.SortedMap;
import java.util.SortedSet;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.EMPTY_RACK_AWARE_ASSIGNMENT_TAGS;
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
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_6;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.spy;

@RunWith(Parameterized.class)
public class StickyTaskAssignorTest {

    private final List<Integer> expectedTopicGroupIds = asList(1, 2);
    private final Time time = new MockTime();
    private final Map<UUID, ClientState> clients = new TreeMap<>();

    private boolean enableRackAwareTaskAssignor;

    @Parameter
    public String rackAwareStrategy;

    @Before
    public void setUp() {
        enableRackAwareTaskAssignor = !rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE);
    }

    @Parameterized.Parameters(name = "rackAwareStrategy={0}")
    public static Collection<Object[]> getParamStoreType() {
        return asList(new Object[][] {
            {StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE},
            {StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC},
            {StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY},
        });
    }

    @Test
    public void shouldAssignOneActiveTaskToEachProcessWhenTaskCountSameAsProcessCount() {
        createClient(UUID_1, 1);
        createClient(UUID_2, 1);
        createClient(UUID_3, 1);

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_1, TASK_0_2);
        assertThat(probingRebalanceNeeded, is(false));

        for (final ClientState clientState : clients.values()) {
            assertThat(clientState.activeTaskCount(), equalTo(1));
        }
    }

    @Test
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithNoStandByTasks() {
        createClient(UUID_1, 2);
        createClient(UUID_2, 2);
        createClient(UUID_3, 2);

        final boolean probingRebalanceNeeded = assign(TASK_1_0, TASK_1_1, TASK_2_2, TASK_2_0, TASK_2_1, TASK_1_2);
        assertThat(probingRebalanceNeeded, is(false));

        assertActiveTaskTopicGroupIdsEvenlyDistributed();
    }

    @Test
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithStandByTasks() {
        createClient(UUID_1, 2);
        createClient(UUID_2, 2);
        createClient(UUID_3, 2);

        final boolean probingRebalanceNeeded = assign(1, TASK_2_0, TASK_1_1, TASK_1_2, TASK_1_0, TASK_2_1, TASK_2_2);
        assertThat(probingRebalanceNeeded, is(false));

        assertActiveTaskTopicGroupIdsEvenlyDistributed();
    }

    @Test
    public void shouldNotMigrateActiveTaskToOtherProcess() {
        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0);
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_1);

        assertThat(assign(TASK_0_0, TASK_0_1, TASK_0_2), is(false));

        assertThat(clients.get(UUID_1).activeTasks(), hasItems(TASK_0_0));
        assertThat(clients.get(UUID_2).activeTasks(), hasItems(TASK_0_1));
        assertThat(allActiveTasks(), equalTo(asList(TASK_0_0, TASK_0_1, TASK_0_2)));

        clients.clear();

        // flip the previous active tasks assignment around.
        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_1);
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_2);

        assertThat(assign(TASK_0_0, TASK_0_1, TASK_0_2), is(false));

        assertThat(clients.get(UUID_1).activeTasks(), hasItems(TASK_0_1));
        assertThat(clients.get(UUID_2).activeTasks(), hasItems(TASK_0_2));
        assertThat(allActiveTasks(), equalTo(asList(TASK_0_0, TASK_0_1, TASK_0_2)));
    }

    @Test
    public void shouldMigrateActiveTasksToNewProcessWithoutChangingAllAssignments() {
        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0, TASK_0_2);
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_1);
        createClient(UUID_3, 1);

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_1, TASK_0_2);

        assertThat(probingRebalanceNeeded, is(false));
        assertThat(clients.get(UUID_2).activeTasks(), equalTo(singleton(TASK_0_1)));
        assertThat(clients.get(UUID_1).activeTasks().size(), equalTo(1));
        assertThat(clients.get(UUID_3).activeTasks().size(), equalTo(1));
        assertThat(allActiveTasks(), equalTo(asList(TASK_0_0, TASK_0_1, TASK_0_2)));
    }

    @Test
    public void shouldAssignBasedOnCapacity() {
        createClient(UUID_1, 1);
        createClient(UUID_2, 2);
        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_1, TASK_0_2);

        assertThat(probingRebalanceNeeded, is(false));
        assertThat(clients.get(UUID_1).activeTasks().size(), equalTo(1));
        assertThat(clients.get(UUID_2).activeTasks().size(), equalTo(2));
    }

    @Test
    public void shouldAssignTasksEvenlyWithUnequalTopicGroupSizes() {
        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5, TASK_1_0);

        createClient(UUID_2, 1);

        assertThat(assign(TASK_1_0, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5), is(false));

        final Set<TaskId> allTasks = new HashSet<>(asList(TASK_0_0, TASK_0_1, TASK_1_0, TASK_0_5, TASK_0_2, TASK_0_3, TASK_0_4));
        final Set<TaskId> client1Tasks = clients.get(UUID_1).activeTasks();
        final Set<TaskId> client2Tasks = clients.get(UUID_2).activeTasks();

        // one client should get 3 tasks and the other should have 4
        assertThat(
            (client1Tasks.size() == 3 && client2Tasks.size() == 4) ||
                (client1Tasks.size() == 4 && client2Tasks.size() == 3),
            is(true));
        allTasks.removeAll(client1Tasks);
        // client2 should have all the remaining tasks not assigned to client 1
        assertThat(client2Tasks, equalTo(allTasks));
    }

    @Test
    public void shouldKeepActiveTaskStickinessWhenMoreClientThanActiveTasks() {
        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0);
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_2);
        createClientWithPreviousActiveTasks(UUID_3, 1, TASK_0_1);
        createClient(UUID_4, 1);
        createClient(UUID_5, 1);

        assertThat(assign(TASK_0_0, TASK_0_1, TASK_0_2), is(false));

        assertThat(clients.get(UUID_1).activeTasks(), equalTo(singleton(TASK_0_0)));
        assertThat(clients.get(UUID_2).activeTasks(), equalTo(singleton(TASK_0_2)));
        assertThat(clients.get(UUID_3).activeTasks(), equalTo(singleton(TASK_0_1)));

        // change up the assignment and make sure it is still sticky
        clients.clear();
        createClient(UUID_1, 1);
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_0);
        createClient(UUID_3, 1);
        createClientWithPreviousActiveTasks(UUID_4, 1, TASK_0_2);
        createClientWithPreviousActiveTasks(UUID_5, 1, TASK_0_1);

        assertThat(assign(TASK_0_0, TASK_0_1, TASK_0_2), is(false));

        assertThat(clients.get(UUID_2).activeTasks(), equalTo(singleton(TASK_0_0)));
        assertThat(clients.get(UUID_4).activeTasks(), equalTo(singleton(TASK_0_2)));
        assertThat(clients.get(UUID_5).activeTasks(), equalTo(singleton(TASK_0_1)));
    }

    @Test
    public void shouldAssignTasksToClientWithPreviousStandbyTasks() {
        final ClientState client1 = createClient(UUID_1, 1);
        client1.addPreviousStandbyTasks(mkSet(TASK_0_2));
        final ClientState client2 = createClient(UUID_2, 1);
        client2.addPreviousStandbyTasks(mkSet(TASK_0_1));
        final ClientState client3 = createClient(UUID_3, 1);
        client3.addPreviousStandbyTasks(mkSet(TASK_0_0));

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_1, TASK_0_2);

        assertThat(probingRebalanceNeeded, is(false));

        assertThat(clients.get(UUID_1).activeTasks(), equalTo(singleton(TASK_0_2)));
        assertThat(clients.get(UUID_2).activeTasks(), equalTo(singleton(TASK_0_1)));
        assertThat(clients.get(UUID_3).activeTasks(), equalTo(singleton(TASK_0_0)));
    }

    @Test
    public void shouldAssignBasedOnCapacityWhenMultipleClientHaveStandbyTasks() {
        final ClientState c1 = createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0);
        c1.addPreviousStandbyTasks(mkSet(TASK_0_1));
        final ClientState c2 = createClientWithPreviousActiveTasks(UUID_2, 2, TASK_0_2);
        c2.addPreviousStandbyTasks(mkSet(TASK_0_1));

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_1, TASK_0_2);

        assertThat(probingRebalanceNeeded, is(false));

        assertThat(clients.get(UUID_1).activeTasks(), equalTo(singleton(TASK_0_0)));
        assertThat(clients.get(UUID_2).activeTasks(), equalTo(mkSet(TASK_0_2, TASK_0_1)));
    }

    @Test
    public void shouldAssignStandbyTasksToDifferentClientThanCorrespondingActiveTaskIsAssignedTo() {
        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0);
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_1);
        createClientWithPreviousActiveTasks(UUID_3, 1, TASK_0_2);
        createClientWithPreviousActiveTasks(UUID_4, 1, TASK_0_3);

        final boolean probingRebalanceNeeded = assign(1, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        assertThat(probingRebalanceNeeded, is(false));


        assertThat(clients.get(UUID_1).standbyTasks(), not(hasItems(TASK_0_0)));
        assertThat(clients.get(UUID_1).standbyTasks().size(), lessThanOrEqualTo(2));
        assertThat(clients.get(UUID_2).standbyTasks(), not(hasItems(TASK_0_1)));
        assertThat(clients.get(UUID_2).standbyTasks().size(), lessThanOrEqualTo(2));
        assertThat(clients.get(UUID_3).standbyTasks(), not(hasItems(TASK_0_2)));
        assertThat(clients.get(UUID_3).standbyTasks().size(), lessThanOrEqualTo(2));
        assertThat(clients.get(UUID_4).standbyTasks(), not(hasItems(TASK_0_3)));
        assertThat(clients.get(UUID_4).standbyTasks().size(), lessThanOrEqualTo(2));

        int nonEmptyStandbyTaskCount = 0;
        for (final ClientState clientState : clients.values()) {
            nonEmptyStandbyTaskCount += clientState.standbyTasks().isEmpty() ? 0 : 1;
        }

        assertThat(nonEmptyStandbyTaskCount, greaterThanOrEqualTo(3));
        assertThat(allStandbyTasks(), equalTo(asList(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
    }

    @Test
    public void shouldAssignMultipleReplicasOfStandbyTask() {
        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0);
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_1);
        createClientWithPreviousActiveTasks(UUID_3, 1, TASK_0_2);

        final boolean probingRebalanceNeeded = assign(2, TASK_0_0, TASK_0_1, TASK_0_2);
        assertThat(probingRebalanceNeeded, is(false));

        assertThat(clients.get(UUID_1).standbyTasks(), equalTo(mkSet(TASK_0_1, TASK_0_2)));
        assertThat(clients.get(UUID_2).standbyTasks(), equalTo(mkSet(TASK_0_2, TASK_0_0)));
        assertThat(clients.get(UUID_3).standbyTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
    }

    @Test
    public void shouldNotAssignStandbyTaskReplicasWhenNoClientAvailableWithoutHavingTheTaskAssigned() {
        createClient(UUID_1, 1);
        final boolean probingRebalanceNeeded = assign(1, TASK_0_0);
        assertThat(probingRebalanceNeeded, is(false));
        assertThat(clients.get(UUID_1).standbyTasks().size(), equalTo(0));
    }

    @Test
    public void shouldAssignActiveAndStandbyTasks() {
        createClient(UUID_1, 1);
        createClient(UUID_2, 1);
        createClient(UUID_3, 1);

        final boolean probingRebalanceNeeded = assign(1, TASK_0_0, TASK_0_1, TASK_0_2);
        assertThat(probingRebalanceNeeded, is(false));

        assertThat(allActiveTasks(), equalTo(asList(TASK_0_0, TASK_0_1, TASK_0_2)));
        assertThat(allStandbyTasks(), equalTo(asList(TASK_0_0, TASK_0_1, TASK_0_2)));
    }

    @Test
    public void shouldAssignAtLeastOneTaskToEachClientIfPossible() {
        createClient(UUID_1, 3);
        createClient(UUID_2, 1);
        createClient(UUID_3, 1);

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_1, TASK_0_2);
        assertThat(probingRebalanceNeeded, is(false));
        assertThat(clients.get(UUID_1).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(UUID_2).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(UUID_3).assignedTaskCount(), equalTo(1));
    }

    @Test
    public void shouldAssignEachActiveTaskToOneClientWhenMoreClientsThanTasks() {
        createClient(UUID_1, 1);
        createClient(UUID_2, 1);
        createClient(UUID_3, 1);
        createClient(UUID_4, 1);
        createClient(UUID_5, 1);
        createClient(UUID_6, 1);

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_1, TASK_0_2);
        assertThat(probingRebalanceNeeded, is(false));

        assertThat(allActiveTasks(), equalTo(asList(TASK_0_0, TASK_0_1, TASK_0_2)));
    }

    @Test
    public void shouldBalanceActiveAndStandbyTasksAcrossAvailableClients() {
        createClient(UUID_1, 1);
        createClient(UUID_2, 1);
        createClient(UUID_3, 1);
        createClient(UUID_4, 1);
        createClient(UUID_5, 1);
        createClient(UUID_6, 1);

        final boolean probingRebalanceNeeded = assign(1, TASK_0_0, TASK_0_1, TASK_0_2);
        assertThat(probingRebalanceNeeded, is(false));

        for (final ClientState clientState : clients.values()) {
            assertThat(clientState.assignedTaskCount(), equalTo(1));
        }
    }

    @Test
    public void shouldAssignMoreTasksToClientWithMoreCapacity() {
        createClient(UUID_2, 2);
        createClient(UUID_1, 1);

        final boolean probingRebalanceNeeded = assign(
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

        assertThat(probingRebalanceNeeded, is(false));
        assertThat(clients.get(UUID_2).assignedTaskCount(), equalTo(8));
        assertThat(clients.get(UUID_1).assignedTaskCount(), equalTo(4));
    }

    @Test
    public void shouldEvenlyDistributeByTaskIdAndPartition() {
        createClient(UUID_1, 4);
        createClient(UUID_2, 4);
        createClient(UUID_3, 4);
        createClient(UUID_4, 4);

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
        final Map<UUID, Map<String, Optional<String>>> racksForProcessConsumer = getRandomProcessRacks(clientSize, nodeSize);
        final InternalTopicManager internalTopicManager = mockInternalTopicManagerForRandomChangelog(nodeSize, topicSize, partitionSize);
        final AssignmentConfigs configs = new AssignorConfiguration.AssignmentConfigs(
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
        assertThat(probingRebalanceNeeded, is(false));

        Collections.sort(taskIds);
        final Set<TaskId> expectedClientOneAssignment = getExpectedTaskIdAssignment(taskIds, 0, 4, 8, 12);
        final Set<TaskId> expectedClientTwoAssignment = getExpectedTaskIdAssignment(taskIds, 1, 5, 9, 13);
        final Set<TaskId> expectedClientThreeAssignment = getExpectedTaskIdAssignment(taskIds, 2, 6, 10, 14);
        final Set<TaskId> expectedClientFourAssignment = getExpectedTaskIdAssignment(taskIds, 3, 7, 11, 15);

        final Map<UUID, Set<TaskId>> sortedAssignments = sortClientAssignments(clients);

        assertThat(sortedAssignments.get(UUID_1), equalTo(expectedClientOneAssignment));
        assertThat(sortedAssignments.get(UUID_2), equalTo(expectedClientTwoAssignment));
        assertThat(sortedAssignments.get(UUID_3), equalTo(expectedClientThreeAssignment));
        assertThat(sortedAssignments.get(UUID_4), equalTo(expectedClientFourAssignment));
    }

    @Test
    public void shouldNotHaveSameAssignmentOnAnyTwoHosts() {
        final List<UUID> allUUIDs = asList(UUID_1, UUID_2, UUID_3, UUID_4);
        createClient(UUID_1, 1);
        createClient(UUID_2, 1);
        createClient(UUID_3, 1);
        createClient(UUID_4, 1);

        final boolean probingRebalanceNeeded = assign(1, TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertThat(probingRebalanceNeeded, is(false));

        for (final UUID uuid : allUUIDs) {
            final Set<TaskId> taskIds = clients.get(uuid).assignedTasks();
            for (final UUID otherUUID : allUUIDs) {
                if (!uuid.equals(otherUUID)) {
                    assertThat("clients shouldn't have same task assignment", clients.get(otherUUID).assignedTasks(),
                               not(equalTo(taskIds)));
                }
            }

        }
    }

    @Test
    public void shouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousActiveTasks() {
        final List<UUID> allUUIDs = asList(UUID_1, UUID_2, UUID_3);
        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_1, TASK_0_2);
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_3);
        createClientWithPreviousActiveTasks(UUID_3, 1, TASK_0_0);
        createClient(UUID_4, 1);

        final boolean probingRebalanceNeeded = assign(1, TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertThat(probingRebalanceNeeded, is(false));

        for (final UUID uuid : allUUIDs) {
            final Set<TaskId> taskIds = clients.get(uuid).assignedTasks();
            for (final UUID otherUUID : allUUIDs) {
                if (!uuid.equals(otherUUID)) {
                    assertThat("clients shouldn't have same task assignment", clients.get(otherUUID).assignedTasks(),
                               not(equalTo(taskIds)));
                }
            }

        }
    }

    @Test
    public void shouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousStandbyTasks() {
        final List<UUID> allUUIDs = asList(UUID_1, UUID_2, UUID_3, UUID_4);

        final ClientState c1 = createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_1, TASK_0_2);
        c1.addPreviousStandbyTasks(mkSet(TASK_0_3, TASK_0_0));
        final ClientState c2 = createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_3, TASK_0_0);
        c2.addPreviousStandbyTasks(mkSet(TASK_0_1, TASK_0_2));

        createClient(UUID_3, 1);
        createClient(UUID_4, 1);

        final boolean probingRebalanceNeeded = assign(1, TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertThat(probingRebalanceNeeded, is(false));

        for (final UUID uuid : allUUIDs) {
            final Set<TaskId> taskIds = clients.get(uuid).assignedTasks();
            for (final UUID otherUUID : allUUIDs) {
                if (!uuid.equals(otherUUID)) {
                    assertThat("clients shouldn't have same task assignment", clients.get(otherUUID).assignedTasks(),
                               not(equalTo(taskIds)));
                }
            }
        }
    }

    @Test
    public void shouldReBalanceTasksAcrossAllClientsWhenCapacityAndTaskCountTheSame() {
        createClientWithPreviousActiveTasks(UUID_3, 1, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        createClient(UUID_1, 1);
        createClient(UUID_2, 1);
        createClient(UUID_4, 1);

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertThat(probingRebalanceNeeded, is(false));

        assertThat(clients.get(UUID_1).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(UUID_2).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(UUID_3).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(UUID_4).assignedTaskCount(), equalTo(1));
    }

    @Test
    public void shouldReBalanceTasksAcrossClientsWhenCapacityLessThanTaskCount() {
        createClientWithPreviousActiveTasks(UUID_3, 1, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3);
        createClient(UUID_1, 1);
        createClient(UUID_2, 1);

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertThat(probingRebalanceNeeded, is(false));

        assertThat(clients.get(UUID_3).assignedTaskCount(), equalTo(2));
        assertThat(clients.get(UUID_1).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(UUID_2).assignedTaskCount(), equalTo(1));
    }

    @Test
    public void shouldRebalanceTasksToClientsBasedOnCapacity() {
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_0, TASK_0_3, TASK_0_2);
        createClient(UUID_3, 2);
        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_2, TASK_0_3);
        assertThat(probingRebalanceNeeded, is(false));
        assertThat(clients.get(UUID_2).assignedTaskCount(), equalTo(1));
        assertThat(clients.get(UUID_3).assignedTaskCount(), equalTo(2));
    }

    @Test
    public void shouldMoveMinimalNumberOfTasksWhenPreviouslyAboveCapacityAndNewClientAdded() {
        final Set<TaskId> p1PrevTasks = mkSet(TASK_0_0, TASK_0_2);
        final Set<TaskId> p2PrevTasks = mkSet(TASK_0_1, TASK_0_3);

        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0, TASK_0_2);
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_1, TASK_0_3);
        createClientWithPreviousActiveTasks(UUID_3, 1);

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_2, TASK_0_1, TASK_0_3);
        assertThat(probingRebalanceNeeded, is(false));

        final Set<TaskId> p3ActiveTasks = clients.get(UUID_3).activeTasks();
        assertThat(p3ActiveTasks.size(), equalTo(1));
        if (p1PrevTasks.removeAll(p3ActiveTasks)) {
            assertThat(clients.get(UUID_2).activeTasks(), equalTo(p2PrevTasks));
        } else {
            assertThat(clients.get(UUID_1).activeTasks(), equalTo(p1PrevTasks));
        }
    }

    @Test
    public void shouldNotMoveAnyTasksWhenNewTasksAdded() {
        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0, TASK_0_1);
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_2, TASK_0_3);

        final boolean probingRebalanceNeeded = assign(TASK_0_3, TASK_0_1, TASK_0_4, TASK_0_2, TASK_0_0, TASK_0_5);
        assertThat(probingRebalanceNeeded, is(false));

        assertThat(clients.get(UUID_1).activeTasks(), hasItems(TASK_0_0, TASK_0_1));
        assertThat(clients.get(UUID_2).activeTasks(), hasItems(TASK_0_2, TASK_0_3));
    }

    @Test
    public void shouldAssignNewTasksToNewClientWhenPreviousTasksAssignedToOldClients() {

        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_2, TASK_0_1);
        createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_0, TASK_0_3);
        createClient(UUID_3, 1);

        final boolean probingRebalanceNeeded = assign(TASK_0_3, TASK_0_1, TASK_0_4, TASK_0_2, TASK_0_0, TASK_0_5);
        assertThat(probingRebalanceNeeded, is(false));

        assertThat(clients.get(UUID_1).activeTasks(), hasItems(TASK_0_2, TASK_0_1));
        assertThat(clients.get(UUID_2).activeTasks(), hasItems(TASK_0_0, TASK_0_3));
        assertThat(clients.get(UUID_3).activeTasks(), hasItems(TASK_0_4, TASK_0_5));
    }

    @Test
    public void shouldAssignTasksNotPreviouslyActiveToNewClient() {
        final ClientState c1 = createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_1, TASK_1_2, TASK_1_3);
        c1.addPreviousStandbyTasks(mkSet(TASK_0_0, TASK_1_1, TASK_2_0, TASK_2_1, TASK_2_3));
        final ClientState c2 = createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_0, TASK_1_1, TASK_2_2);
        c2.addPreviousStandbyTasks(mkSet(TASK_0_1, TASK_1_0, TASK_0_2, TASK_2_0, TASK_0_3, TASK_1_2, TASK_2_1, TASK_1_3, TASK_2_3));
        final ClientState c3 = createClientWithPreviousActiveTasks(UUID_3, 1, TASK_2_0, TASK_2_1, TASK_2_3);
        c3.addPreviousStandbyTasks(mkSet(TASK_0_2, TASK_1_2));

        final ClientState newClient = createClient(UUID_4, 1);
        newClient.addPreviousStandbyTasks(mkSet(TASK_0_0, TASK_1_0, TASK_0_1, TASK_0_2, TASK_1_1, TASK_2_0, TASK_0_3, TASK_1_2, TASK_2_1, TASK_1_3, TASK_2_2, TASK_2_3));

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_1_0, TASK_0_1, TASK_0_2, TASK_1_1, TASK_2_0, TASK_0_3, TASK_1_2, TASK_2_1, TASK_1_3, TASK_2_2, TASK_2_3);
        assertThat(probingRebalanceNeeded, is(false));

        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_1, TASK_1_2, TASK_2_3)));
            assertThat(c2.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_1_1, TASK_2_2)));
            assertThat(c3.activeTasks(), equalTo(mkSet(TASK_0_2, TASK_1_3, TASK_2_1)));
            assertThat(newClient.activeTasks(), equalTo(mkSet(TASK_0_3, TASK_1_0, TASK_2_0)));
        } else {
            assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_1, TASK_1_2, TASK_1_3)));
            assertThat(c2.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_1_1, TASK_2_2)));
            assertThat(c3.activeTasks(), equalTo(mkSet(TASK_2_0, TASK_2_1, TASK_2_3)));
            assertThat(newClient.activeTasks(), equalTo(mkSet(TASK_0_2, TASK_0_3, TASK_1_0)));
        }
    }

    @Test
    public void shouldAssignTasksNotPreviouslyActiveToMultipleNewClients() {
        final ClientState c1 = createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_1, TASK_1_2, TASK_1_3);
        c1.addPreviousStandbyTasks(mkSet(TASK_0_0, TASK_1_1, TASK_2_0, TASK_2_1, TASK_2_3));
        final ClientState c2 = createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_0, TASK_1_1, TASK_2_2);
        c2.addPreviousStandbyTasks(mkSet(TASK_0_1, TASK_1_0, TASK_0_2, TASK_2_0, TASK_0_3, TASK_1_2, TASK_2_1, TASK_1_3, TASK_2_3));

        final ClientState bounce1 = createClient(UUID_3, 1);
        bounce1.addPreviousStandbyTasks(mkSet(TASK_2_0, TASK_2_1, TASK_2_3));

        final ClientState bounce2 = createClient(UUID_4, 1);
        bounce2.addPreviousStandbyTasks(mkSet(TASK_0_2, TASK_0_3, TASK_1_0));

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_1_0, TASK_0_1, TASK_0_2, TASK_1_1, TASK_2_0, TASK_0_3, TASK_1_2, TASK_2_1, TASK_1_3, TASK_2_2, TASK_2_3);
        assertThat(probingRebalanceNeeded, is(false));

        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_1, TASK_1_2, TASK_2_3)));
            assertThat(c2.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_1_1, TASK_2_2)));
            assertThat(bounce1.activeTasks(), equalTo(mkSet(TASK_0_2, TASK_1_3, TASK_2_1)));
            assertThat(bounce2.activeTasks(), equalTo(mkSet(TASK_0_3, TASK_1_0, TASK_2_0)));
        } else {
            assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_1, TASK_1_2, TASK_1_3)));
            assertThat(c2.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_1_1, TASK_2_2)));
            assertThat(bounce1.activeTasks(), equalTo(mkSet(TASK_2_0, TASK_2_1, TASK_2_3)));
            assertThat(bounce2.activeTasks(), equalTo(mkSet(TASK_0_2, TASK_0_3, TASK_1_0)));
        }
    }

    @Test
    public void shouldAssignTasksToNewClient() {
        createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_1, TASK_0_2);
        createClient(UUID_2, 1);
        assertThat(assign(TASK_0_1, TASK_0_2), is(false));
        assertThat(clients.get(UUID_1).activeTaskCount(), equalTo(1));
    }

    @Test
    public void shouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingClients() {
        final ClientState c1 = createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState c2 = createClientWithPreviousActiveTasks(UUID_2, 1, TASK_0_3, TASK_0_4, TASK_0_5);
        final ClientState newClient = createClient(UUID_3, 1);

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5);
        assertThat(probingRebalanceNeeded, is(false));
        assertThat(c1.activeTasks(), not(hasItem(TASK_0_3)));
        assertThat(c1.activeTasks(), not(hasItem(TASK_0_4)));
        assertThat(c1.activeTasks(), not(hasItem(TASK_0_5)));
        assertThat(c1.activeTaskCount(), equalTo(2));
        assertThat(c2.activeTasks(), not(hasItems(TASK_0_0)));
        assertThat(c2.activeTasks(), not(hasItems(TASK_0_1)));
        assertThat(c2.activeTasks(), not(hasItems(TASK_0_2)));
        assertThat(c2.activeTaskCount(), equalTo(2));
        assertThat(newClient.activeTaskCount(), equalTo(2));
    }

    @Test
    public void shouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingAndBouncedClients() {
        final ClientState c1 = createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_6);
        final ClientState c2 = createClient(UUID_2, 1);
        c2.addPreviousStandbyTasks(mkSet(TASK_0_3, TASK_0_4, TASK_0_5));
        final ClientState newClient = createClient(UUID_3, 1);

        final boolean probingRebalanceNeeded = assign(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5, TASK_0_6);
        assertThat(probingRebalanceNeeded, is(false));

        // it's possible for either client 1 or 2 to get three tasks since they both had three previously assigned
        assertThat(c1.activeTasks(), not(hasItem(TASK_0_3)));
        assertThat(c1.activeTasks(), not(hasItem(TASK_0_4)));
        assertThat(c1.activeTasks(), not(hasItem(TASK_0_5)));
        assertThat(c1.activeTaskCount(), greaterThanOrEqualTo(2));
        assertThat(c2.activeTasks(), not(hasItems(TASK_0_0)));
        assertThat(c2.activeTasks(), not(hasItems(TASK_0_1)));
        assertThat(c2.activeTasks(), not(hasItems(TASK_0_2)));
        assertThat(c2.activeTaskCount(), greaterThanOrEqualTo(2));
        assertThat(newClient.activeTaskCount(), equalTo(2));
    }

    @Test
    public void shouldViolateBalanceToPreserveActiveTaskStickiness() {
        final ClientState c1 = createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState c2 = createClient(UUID_2, 1);

        final List<TaskId> taskIds = asList(TASK_0_0, TASK_0_1, TASK_0_2);
        Collections.shuffle(taskIds);

        final int nodeSize = 5;
        final int topicSize = 1;
        final int partitionSize = 3;
        final int clientSize = 2;
        final Cluster cluster = getRandomCluster(nodeSize, topicSize, partitionSize);
        final Map<TaskId, Set<TopicPartition>> partitionsForTask = getTaskTopicPartitionMap(topicSize, partitionSize, false);
        final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask = getTaskTopicPartitionMap(topicSize, partitionSize, true);
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = new HashMap<>();
        final Map<UUID, Map<String, Optional<String>>> racksForProcessConsumer = getRandomProcessRacks(clientSize, nodeSize);
        final InternalTopicManager internalTopicManager = mockInternalTopicManagerForRandomChangelog(nodeSize, topicSize, partitionSize);

        final AssignmentConfigs configs = new AssignorConfiguration.AssignmentConfigs(
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

        final boolean probingRebalanceNeeded = new StickyTaskAssignor(true).assign(
            clients,
            new HashSet<>(taskIds),
            new HashSet<>(taskIds),
            rackAwareTaskAssignor,
            configs
        );
        assertThat(probingRebalanceNeeded, is(false));

        assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2)));
        assertThat(c2.activeTasks(), empty());
    }

    @Test
    public void shouldOptimizeStatefulAndStatelessTaskTraffic() {
        final ClientState c1 = createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState c2 = createClientWithPreviousActiveTasks(UUID_2, 1, TASK_1_0, TASK_1_1, TASK_0_3, TASK_1_3);
        final ClientState c3 = createClientWithPreviousActiveTasks(UUID_3, 1, TASK_1_2);

        final List<TaskId> taskIds = asList(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_1_0, TASK_1_1, TASK_1_2, TASK_1_3);
        final List<TaskId> statefulTaskIds = asList(TASK_0_0, TASK_0_1, TASK_1_0, TASK_1_1);
        Collections.shuffle(taskIds);

        final Cluster cluster = getClusterForAllTopics();
        final Map<TaskId, Set<TopicPartition>> partitionsForTask = getTaskTopicPartitionMapForAllTasks();
        final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask = getTaskChangelogMapForAllTasks();
        final Map<UUID, Map<String, Optional<String>>> racksForProcessConsumer = getProcessRacksForAllProcess();
        final InternalTopicManager internalTopicManager = mockInternalTopicManagerForChangelog();

        final AssignmentConfigs configs = new AssignorConfiguration.AssignmentConfigs(
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

        final boolean probingRebalanceNeeded = new StickyTaskAssignor().assign(
            clients,
            new HashSet<>(taskIds),
            new HashSet<>(statefulTaskIds),
            rackAwareTaskAssignor,
            configs
        );
        assertThat(probingRebalanceNeeded, is(false));

        if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC)) {
            // Total cost for active stateful: 3
            // Total cost for active stateless: 0
            // Total cost for standby: 20
            assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_3, TASK_1_0, TASK_1_2)));
            assertThat(c1.standbyTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
            assertThat(c2.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_2, TASK_1_1)));
            assertThat(c2.standbyTasks(), empty());
            assertThat(c3.activeTasks(), equalTo(mkSet(TASK_0_1, TASK_1_3)));
            assertThat(c3.standbyTasks(), equalTo(mkSet(TASK_1_0, TASK_1_1)));
        } else if (rackAwareStrategy.equals(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY)) {
            assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_3, TASK_1_2)));
            assertThat(c1.standbyTasks(), equalTo(mkSet(TASK_1_0)));
            assertThat(c2.activeTasks(), equalTo(mkSet(TASK_0_1, TASK_0_2, TASK_1_1)));
            assertThat(c2.standbyTasks(), equalTo(mkSet(TASK_0_0)));
            assertThat(c3.activeTasks(), equalTo(mkSet(TASK_1_0, TASK_1_3)));
            assertThat(c3.standbyTasks(), equalTo(mkSet(TASK_0_1, TASK_1_1)));
        } else {
            // Total cost for active stateful: 30
            // Total cost for active stateless: 40
            // Total cost for standby: 10
            assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_1, TASK_0_2, TASK_1_3)));
            assertThat(c1.standbyTasks(), equalTo(mkSet(TASK_0_0)));
            assertThat(c2.activeTasks(), equalTo(mkSet(TASK_0_3, TASK_1_0, TASK_1_1)));
            assertThat(c2.standbyTasks(), equalTo(mkSet(TASK_0_1)));
            assertThat(c3.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_1_2)));
            assertThat(c3.standbyTasks(), equalTo(mkSet(TASK_1_0, TASK_1_1)));
        }

    }

    @Test
    public void shouldAssignRandomInput() {
        final int nodeSize = 50;
        final int tpSize = 60;
        final int partitionSize = 3;
        final int clientSize = 50;
        final int replicaCount = 1;
        final int maxCapacity = 3;
        final SortedMap<TaskId, Set<TopicPartition>> taskTopicPartitionMap = getTaskTopicPartitionMap(
            tpSize, partitionSize, false);
        final AssignmentConfigs configs = new AssignorConfiguration.AssignmentConfigs(
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
        final SortedMap<UUID, ClientState> clientStateMap = getRandomClientState(clientSize,
            tpSize, partitionSize, maxCapacity, false, statefulTasks);


        final boolean probing = new StickyTaskAssignor().assign(
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

    @Test
    public void shouldRemainOriginalAssignmentWithoutTrafficCostForMinCostStrategy() {
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
        final Map<UUID, Map<String, Optional<String>>> processRackMap = getRandomProcessRacks(clientSize, nodeSize);
        final InternalTopicManager mockInternalTopicManager = mockInternalTopicManagerForRandomChangelog(nodeSize, tpSize, partitionSize);

        AssignmentConfigs configs = new AssignorConfiguration.AssignmentConfigs(
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
        final SortedMap<UUID, ClientState> clientStateMap = getRandomClientState(clientSize,
            tpSize, partitionSize, maxCapacity, false, statefulTasks);

        new StickyTaskAssignor().assign(
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

        final SortedMap<UUID, ClientState> clientStateMapCopy = copyClientStateMap(clientStateMap);
        configs = new AssignorConfiguration.AssignmentConfigs(
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

        new StickyTaskAssignor().assign(
            clientStateMapCopy,
            taskIds,
            statefulTasks,
            rackAwareTaskAssignor,
            configs
        );

        for (final Map.Entry<UUID, ClientState> entry : clientStateMap.entrySet()) {
            assertThat(entry.getValue().statefulActiveTasks(), equalTo(clientStateMapCopy.get(entry.getKey()).statefulActiveTasks()));
            assertThat(entry.getValue().standbyTasks(), equalTo(clientStateMapCopy.get(entry.getKey()).standbyTasks()));
        }
    }

    private boolean assign(final TaskId... tasks) {
        return assign(0, tasks);
    }

    private boolean assign(final int numStandbys, final TaskId... tasks) {
        final List<TaskId> taskIds = asList(tasks);
        Collections.shuffle(taskIds);
        final AssignmentConfigs configs = new AssignorConfiguration.AssignmentConfigs(
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
        return new StickyTaskAssignor().assign(
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

    private ClientState createClient(final UUID processId, final int capacity) {
        return createClientWithPreviousActiveTasks(processId, capacity);
    }

    private ClientState createClientWithPreviousActiveTasks(final UUID processId, final int capacity, final TaskId... taskIds) {
        final ClientState clientState = new ClientState(processId, capacity);
        clientState.addPreviousActiveTasks(mkSet(taskIds));
        clients.put(processId, clientState);
        return clientState;
    }

    private void assertActiveTaskTopicGroupIdsEvenlyDistributed() {
        for (final Map.Entry<UUID, ClientState> clientStateEntry : clients.entrySet()) {
            final List<Integer> topicGroupIds = new ArrayList<>();
            final Set<TaskId> activeTasks = clientStateEntry.getValue().activeTasks();
            for (final TaskId activeTask : activeTasks) {
                topicGroupIds.add(activeTask.subtopology());
            }
            Collections.sort(topicGroupIds);
            assertThat(topicGroupIds, equalTo(expectedTopicGroupIds));
        }
    }

    private static Map<UUID, Set<TaskId>> sortClientAssignments(final Map<UUID, ClientState> clients) {
        final Map<UUID, Set<TaskId>> sortedAssignments = new HashMap<>();
        for (final Map.Entry<UUID, ClientState> entry : clients.entrySet()) {
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
