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

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.kafka.common.utils.Utils.mkSet;
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
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.UUID_6;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class StickyTaskAssignorTest {

    private final List<Integer> expectedTopicGroupIds = asList(1, 2);

    private final Map<UUID, ClientState> clients = new TreeMap<>();

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

        final Set<TaskId> expectedClientITasks = new HashSet<>(asList(TASK_0_0, TASK_0_1, TASK_1_0, TASK_0_5));
        final Set<TaskId> expectedClientIITasks = new HashSet<>(asList(TASK_0_2, TASK_0_3, TASK_0_4));


        assertThat(clients.get(UUID_1).activeTasks(), equalTo(expectedClientITasks));
        assertThat(clients.get(UUID_2).activeTasks(), equalTo(expectedClientIITasks));
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
            new TaskId(1, 0),
            new TaskId(1, 1),
            new TaskId(1, 2),
            new TaskId(2, 0),
            new TaskId(2, 1),
            new TaskId(2, 2),
            new TaskId(3, 0),
            new TaskId(3, 1),
            new TaskId(3, 2)
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

        for (int i = 1; i <= 2; i++) {
            for (int j = 0; j < 8; j++) {
                taskIds.add(new TaskId(i, j));
            }
        }

        Collections.shuffle(taskIds);
        taskIds.toArray(taskIdArray);

        final boolean probingRebalanceNeeded = assign(taskIdArray);
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

        assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_1, TASK_1_2, TASK_1_3)));
        assertThat(c2.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_1_1, TASK_2_2)));
        assertThat(c3.activeTasks(), equalTo(mkSet(TASK_2_0, TASK_2_1, TASK_2_3)));
        assertThat(newClient.activeTasks(), equalTo(mkSet(TASK_0_2, TASK_0_3, TASK_1_0)));
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

        assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_1, TASK_1_2, TASK_1_3)));
        assertThat(c2.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_1_1, TASK_2_2)));
        assertThat(bounce1.activeTasks(), equalTo(mkSet(TASK_2_0, TASK_2_1, TASK_2_3)));
        assertThat(bounce2.activeTasks(), equalTo(mkSet(TASK_0_2, TASK_0_3, TASK_1_0)));
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
        assertThat(c1.activeTasks(), not(hasItem(TASK_0_3)));
        assertThat(c1.activeTasks(), not(hasItem(TASK_0_4)));
        assertThat(c1.activeTasks(), not(hasItem(TASK_0_5)));
        assertThat(c1.activeTaskCount(), equalTo(3));
        assertThat(c2.activeTasks(), not(hasItems(TASK_0_0)));
        assertThat(c2.activeTasks(), not(hasItems(TASK_0_1)));
        assertThat(c2.activeTasks(), not(hasItems(TASK_0_2)));
        assertThat(c2.activeTaskCount(), equalTo(2));
        assertThat(newClient.activeTaskCount(), equalTo(2));
    }

    @Test
    public void shouldViolateBalanceToPreserveActiveTaskStickiness() {
        final ClientState c1 = createClientWithPreviousActiveTasks(UUID_1, 1, TASK_0_0, TASK_0_1, TASK_0_2);
        final ClientState c2 = createClient(UUID_2, 1);

        final List<TaskId> taskIds = asList(TASK_0_0, TASK_0_1, TASK_0_2);
        Collections.shuffle(taskIds);
        final boolean probingRebalanceNeeded = new StickyTaskAssignor(true).assign(
            clients,
            new HashSet<>(taskIds),
            new HashSet<>(taskIds),
            new AssignorConfiguration.AssignmentConfigs(0L, 1, 0, 60_000L)
        );
        assertThat(probingRebalanceNeeded, is(false));

        assertThat(c1.activeTasks(), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2)));
        assertThat(c2.activeTasks(), empty());
    }

    private boolean assign(final TaskId... tasks) {
        return assign(0, tasks);
    }

    private boolean assign(final int numStandbys, final TaskId... tasks) {
        final List<TaskId> taskIds = asList(tasks);
        Collections.shuffle(taskIds);
        return new StickyTaskAssignor().assign(
            clients,
            new HashSet<>(taskIds),
            new HashSet<>(taskIds),
            new AssignorConfiguration.AssignmentConfigs(0L, 1, numStandbys, 60_000L)
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
        final ClientState clientState = new ClientState(capacity);
        clientState.addPreviousActiveTasks(mkSet(taskIds));
        clients.put(processId, clientState);
        return clientState;
    }

    private void assertActiveTaskTopicGroupIdsEvenlyDistributed() {
        for (final Map.Entry<UUID, ClientState> clientStateEntry : clients.entrySet()) {
            final List<Integer> topicGroupIds = new ArrayList<>();
            final Set<TaskId> activeTasks = clientStateEntry.getValue().activeTasks();
            for (final TaskId activeTask : activeTasks) {
                topicGroupIds.add(activeTask.topicGroupId);
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
