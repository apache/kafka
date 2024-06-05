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
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask.Type.ACTIVE;
import static org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask.Type.STANDBY;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_1_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_2_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_3_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_3_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_3_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.uuidForInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.assignors.StickyTaskAssignor;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CustomStickyTaskAssignorTest {

    private TaskAssignor assignor;

    @Parameterized.Parameter
    public String rackAwareStrategy;

    @Parameterized.Parameters(name = "rackAwareStrategy={0}")
    public static Collection<Object[]> getParamStoreType() {
        return asList(new Object[][] {
            {StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE},
            {StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC},
            {StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY},
        });
    }

    @Before
    public void setUp() {
        assignor = new StickyTaskAssignor();
    }

    @Test
    public void shouldAssignOneActiveTaskToEachProcessWhenTaskCountSameAsProcessCount() {
        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty()),
            mkStreamState(2, 1, Optional.empty()),
            mkStreamState(3, 1, Optional.empty())
        );
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        for (final KafkaStreamsAssignment assignment : assignments.values()) {
            assertThat(assignment.tasks().size(), equalTo(1));
        }
    }

    @Test
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithNoStandByTasks() {
        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 2, Optional.empty()),
            mkStreamState(2, 2, Optional.empty()),
            mkStreamState(3, 2, Optional.empty())
        );
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_1_0, false),
            mkTaskInfo(TASK_1_1, false),
            mkTaskInfo(TASK_2_2, false),
            mkTaskInfo(TASK_2_0, false),
            mkTaskInfo(TASK_2_1, false),
            mkTaskInfo(TASK_1_2, false)
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        assertActiveTaskTopicGroupIdsEvenlyDistributed(assignments);
    }

    @Test
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithStandByTasks() {
        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 2, Optional.empty()),
            mkStreamState(2, 2, Optional.empty()),
            mkStreamState(3, 2, Optional.empty())
        );

        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_2_0, false),
            mkTaskInfo(TASK_1_1, false),
            mkTaskInfo(TASK_1_2, false),
            mkTaskInfo(TASK_1_0, false),
            mkTaskInfo(TASK_2_1, false),
            mkTaskInfo(TASK_2_2, false)
        );
        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1);
        assertActiveTaskTopicGroupIdsEvenlyDistributed(assignments);
    }


    @Test
    public void shouldNotMigrateActiveTaskToOtherProcess() {
        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), mkSet(TASK_0_0), mkSet()),
            mkStreamState(2, 1, Optional.empty(), mkSet(TASK_0_1), mkSet())
        );

        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        assertHasAssignment(assignments, 1, TASK_0_0, ACTIVE);
        assertHasAssignment(assignments, 2, TASK_0_1, ACTIVE);

        final Map<ProcessId, KafkaStreamsState> streamStates2 = mkMap(
            mkStreamState(1, 1, Optional.empty(), mkSet(TASK_0_1), mkSet()),
            mkStreamState(2, 1, Optional.empty(), mkSet(TASK_0_0), mkSet())
        );
        final Map<ProcessId, KafkaStreamsAssignment> assignments2 = assign(streamStates2, tasks);
        assertHasAssignment(assignments2, 1, TASK_0_1, ACTIVE);
        assertHasAssignment(assignments2, 2, TASK_0_0, ACTIVE);
    }

    @Test
    public void shouldMigrateActiveTasksToNewProcessWithoutChangingAllAssignments() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), mkSet(TASK_0_0, TASK_0_2), mkSet()),
            mkStreamState(2, 1, Optional.empty(), mkSet(TASK_0_1), mkSet()),
            mkStreamState(3, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        assertThat(assignments.get(processId(1)).tasks().values().size(), equalTo(1));
        assertThat(assignments.get(processId(2)).tasks().values().size(), equalTo(1));
        assertThat(assignments.get(processId(3)).tasks().values().size(), equalTo(1));

        assertHasAssignment(assignments, 2, TASK_0_1, ACTIVE);
    }

    @Test
    public void shouldAssignBasedOnCapacity() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );
        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty()),
            mkStreamState(2, 2, Optional.empty())
        );
        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        assertThat(assignments.get(processId(1)).tasks().values().size(), equalTo(1));
        assertThat(assignments.get(processId(2)).tasks().values().size(), equalTo(2));
    }

    @Test
    public void shouldAssignTasksEvenlyWithUnequalTopicGroupSizes() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_1_0, false),
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false),
            mkTaskInfo(TASK_0_3, false),
            mkTaskInfo(TASK_0_4, false),
            mkTaskInfo(TASK_0_5, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5, TASK_1_0), mkSet()),
            mkStreamState(2, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        final Set<TaskId> client1Tasks = assignments.get(processId(1)).tasks().values().stream()
            .filter(t -> t.type() == ACTIVE)
            .map(AssignedTask::id)
            .collect(Collectors.toSet());
        final Set<TaskId> client2Tasks = assignments.get(processId(2)).tasks().values().stream()
            .filter(t -> t.type() == ACTIVE)
            .map(AssignedTask::id)
            .collect(Collectors.toSet());

        final Set<TaskId> allTasks = tasks.keySet();

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
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), mkSet(TASK_0_0), mkSet()),
            mkStreamState(2, 1, Optional.empty(), mkSet(TASK_0_2), mkSet()),
            mkStreamState(3, 1, Optional.empty(), mkSet(TASK_0_1), mkSet()),
            mkStreamState(4, 1, Optional.empty()),
            mkStreamState(5, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        assertThat(assignments.get(processId(1)).tasks().size(), is(1));
        assertThat(assignments.get(processId(2)).tasks().size(), is(1));
        assertThat(assignments.get(processId(3)).tasks().size(), is(1));
        assertThat(assignments.get(processId(4)).tasks().size(), is(0));
        assertThat(assignments.get(processId(5)).tasks().size(), is(0));

        assertHasAssignment(assignments, 1, TASK_0_0, ACTIVE);
        assertHasAssignment(assignments, 2, TASK_0_2, ACTIVE);
        assertHasAssignment(assignments, 3, TASK_0_1, ACTIVE);


        final Map<ProcessId, KafkaStreamsState> streamStates2 = mkMap(
            mkStreamState(1, 1, Optional.empty()),
            mkStreamState(2, 1, Optional.empty()),
            mkStreamState(3, 1, Optional.empty(), mkSet(TASK_0_1), mkSet()),
            mkStreamState(4, 1, Optional.empty(), mkSet(TASK_0_0), mkSet()),
            mkStreamState(5, 1, Optional.empty(), mkSet(TASK_0_2), mkSet())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments2 = assign(streamStates2, tasks);
        assertThat(assignments2.get(processId(1)).tasks().size(), is(0));
        assertThat(assignments2.get(processId(2)).tasks().size(), is(0));
        assertThat(assignments2.get(processId(3)).tasks().size(), is(1));
        assertThat(assignments2.get(processId(4)).tasks().size(), is(1));
        assertThat(assignments2.get(processId(5)).tasks().size(), is(1));

        assertHasAssignment(assignments2, 3, TASK_0_1, ACTIVE);
        assertHasAssignment(assignments2, 4, TASK_0_0, ACTIVE);
        assertHasAssignment(assignments2, 5, TASK_0_2, ACTIVE);
    }

    @Test
    public void shouldAssignTasksToClientWithPreviousStandbyTasks() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), mkSet(), mkSet(TASK_0_2)),
            mkStreamState(2, 1, Optional.empty(), mkSet(), mkSet(TASK_0_1)),
            mkStreamState(3, 1, Optional.empty(), mkSet(), mkSet(TASK_0_0))
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        assertHasAssignment(assignments, 1, TASK_0_2, ACTIVE);
        assertHasAssignment(assignments, 2, TASK_0_1, ACTIVE);
        assertHasAssignment(assignments, 3, TASK_0_0, ACTIVE);
    }

    @Test
    public void shouldAssignBasedOnCapacityWhenMultipleClientHaveStandbyTasks() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), mkSet(TASK_0_0), mkSet(TASK_0_1)),
            mkStreamState(2, 2, Optional.empty(), mkSet(TASK_0_2), mkSet(TASK_0_1))
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        assertThat(assignments.get(processId(1)).tasks().size(), is(1));
        assertThat(assignments.get(processId(2)).tasks().size(), is(2));
        assertHasAssignment(assignments, 1, TASK_0_0, ACTIVE);
        assertHasAssignment(assignments, 2, TASK_0_1, ACTIVE);
        assertHasAssignment(assignments, 2, TASK_0_2, ACTIVE);
    }

    @Test
    public void shouldAssignStandbyTasksToDifferentClientThanCorrespondingActiveTaskIsAssignedTo() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true),
            mkTaskInfo(TASK_0_1, true),
            mkTaskInfo(TASK_0_2, true),
            mkTaskInfo(TASK_0_3, true)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), mkSet(TASK_0_0), mkSet()),
            mkStreamState(2, 1, Optional.empty(), mkSet(TASK_0_1), mkSet()),
            mkStreamState(3, 1, Optional.empty(), mkSet(TASK_0_2), mkSet()),
            mkStreamState(4, 1, Optional.empty(), mkSet(TASK_0_3), mkSet())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1);
        assertThat(standbyTasks(assignments, 1).size(), lessThanOrEqualTo(2));
        assertThat(standbyTasks(assignments, 2).size(), lessThanOrEqualTo(2));
        assertThat(standbyTasks(assignments, 3).size(), lessThanOrEqualTo(2));
        assertThat(standbyTasks(assignments, 4).size(), lessThanOrEqualTo(2));

        assertThat(standbyTasks(assignments, 1), not(hasItems(TASK_0_0)));
        assertThat(standbyTasks(assignments, 2), not(hasItems(TASK_0_1)));
        assertThat(standbyTasks(assignments, 3), not(hasItems(TASK_0_2)));
        assertThat(standbyTasks(assignments, 4), not(hasItems(TASK_0_3)));

        assertThat(activeTasks(assignments, 1), hasItems(TASK_0_0));
        assertThat(activeTasks(assignments, 2), hasItems(TASK_0_1));
        assertThat(activeTasks(assignments, 3), hasItems(TASK_0_2));
        assertThat(activeTasks(assignments, 4), hasItems(TASK_0_3));

        int nonEmptyStandbyTaskCount = 0;
        for (int i = 1; i <= 4; i++) {
            nonEmptyStandbyTaskCount += standbyTasks(assignments, i).isEmpty() ? 0 : 1;
        }

        assertThat(nonEmptyStandbyTaskCount, greaterThanOrEqualTo(3));

        final Set<TaskId> allStandbyTasks = allTasks(assignments).stream()
            .filter(t -> t.type() == STANDBY)
            .map(AssignedTask::id)
            .collect(Collectors.toSet());
        assertThat(allStandbyTasks, equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
    }


    @Test
    public void shouldAssignMultipleReplicasOfStandbyTask() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true),
            mkTaskInfo(TASK_0_1, true),
            mkTaskInfo(TASK_0_2, true)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), mkSet(TASK_0_0), mkSet()),
            mkStreamState(2, 1, Optional.empty(), mkSet(TASK_0_1), mkSet()),
            mkStreamState(3, 1, Optional.empty(), mkSet(TASK_0_2), mkSet())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 2);
        assertThat(activeTasks(assignments, 1), equalTo(mkSet(TASK_0_0)));
        assertThat(activeTasks(assignments, 2), equalTo(mkSet(TASK_0_1)));
        assertThat(activeTasks(assignments, 3), equalTo(mkSet(TASK_0_2)));

        assertThat(standbyTasks(assignments, 1), equalTo(mkSet(TASK_0_1, TASK_0_2)));
        assertThat(standbyTasks(assignments, 2), equalTo(mkSet(TASK_0_0, TASK_0_2)));
        assertThat(standbyTasks(assignments, 3), equalTo(mkSet(TASK_0_0, TASK_0_1)));
    }

    @Test
    public void shouldNotAssignStandbyTaskReplicasWhenNoClientAvailableWithoutHavingTheTaskAssigned() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), mkSet(TASK_0_0), mkSet())
        );
        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 2);
        assertThat(activeTasks(assignments, 1), equalTo(mkSet(TASK_0_0)));
        assertThat(standbyTasks(assignments, 1), equalTo(mkSet()));
    }

    @Test
    public void shouldAssignActiveAndStandbyTasks() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true),
            mkTaskInfo(TASK_0_1, true),
            mkTaskInfo(TASK_0_2, true)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty()),
            mkStreamState(2, 1, Optional.empty()),
            mkStreamState(3, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1);
        final Set<AssignedTask> allTasks = allTasks(assignments);
        assertThat(allTasks.stream().filter(t -> t.type() == ACTIVE).map(AssignedTask::id).collect(
            Collectors.toSet()), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2)));
        assertThat(allTasks.stream().filter(t -> t.type() == STANDBY).map(AssignedTask::id).collect(
            Collectors.toSet()), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2)));
    }

    @Test
    public void shouldAssignAtLeastOneTaskToEachClientIfPossible() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 3, Optional.empty()),
            mkStreamState(2, 1, Optional.empty()),
            mkStreamState(3, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        assertThat(activeTasks(assignments, 1).size(), is(1));
        assertThat(activeTasks(assignments, 2).size(), is(1));
        assertThat(activeTasks(assignments, 3).size(), is(1));
    }

    @Test
    public void shouldAssignEachActiveTaskToOneClientWhenMoreClientsThanTasks() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty()),
            mkStreamState(2, 1, Optional.empty()),
            mkStreamState(3, 1, Optional.empty()),
            mkStreamState(4, 1, Optional.empty()),
            mkStreamState(5, 1, Optional.empty()),
            mkStreamState(6, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        final Set<AssignedTask> allTasks = allTasks(assignments);
        assertThat(allTasks.stream().filter(t -> t.type() == ACTIVE).map(AssignedTask::id).collect(
            Collectors.toSet()), equalTo(mkSet(TASK_0_0, TASK_0_1, TASK_0_2)));
        assertThat(allTasks.stream().filter(t -> t.type() == STANDBY).map(AssignedTask::id).collect(
            Collectors.toSet()), equalTo(mkSet()));

        final int clientsWithATask = assignments.values().stream().mapToInt(assignment -> assignment.tasks().isEmpty() ? 0 : 1).sum();
        assertThat(clientsWithATask, greaterThanOrEqualTo(3));
    }

    @Test
    public void shouldBalanceActiveAndStandbyTasksAcrossAvailableClients() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true),
            mkTaskInfo(TASK_0_1, true),
            mkTaskInfo(TASK_0_2, true)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty()),
            mkStreamState(2, 1, Optional.empty()),
            mkStreamState(3, 1, Optional.empty()),
            mkStreamState(4, 1, Optional.empty()),
            mkStreamState(5, 1, Optional.empty()),
            mkStreamState(6, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1);
        for (final KafkaStreamsAssignment assignment : assignments.values()) {
            assertThat(assignment.tasks().values(), not(hasSize(0)));
        }
    }

    @Test
    public void shouldAssignMoreTasksToClientWithMoreCapacity() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false),
            mkTaskInfo(TASK_1_0, false),
            mkTaskInfo(TASK_1_1, false),
            mkTaskInfo(TASK_1_2, false),
            mkTaskInfo(TASK_2_0, false),
            mkTaskInfo(TASK_2_1, false),
            mkTaskInfo(TASK_2_2, false),
            mkTaskInfo(TASK_3_0, false),
            mkTaskInfo(TASK_3_1, false),
            mkTaskInfo(TASK_3_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty()),
            mkStreamState(2, 2, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks);
        assertThat(activeTasks(assignments, 1).size(), equalTo(4));
        assertThat(activeTasks(assignments, 2).size(), equalTo(8));
    }

    @Test
    public void shouldEvenlyDistributeByTaskIdAndPartition() {
        // TODO: port shouldEvenlyDistributeByTaskIdAndPartition from StickyTaskAssignorTest
    }

    @Test
    public void shouldNotHaveSameAssignmentOnAnyTwoHosts() {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true),
            mkTaskInfo(TASK_0_1, true),
            mkTaskInfo(TASK_0_2, true),
            mkTaskInfo(TASK_0_3, true)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty()),
            mkStreamState(2, 1, Optional.empty()),
            mkStreamState(3, 1, Optional.empty()),
            mkStreamState(4, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1);

        for (final KafkaStreamsState client1: streamStates.values()) {
            for (final KafkaStreamsState client2: streamStates.values()) {
                if (!client1.processId().equals(client2.processId())) {
                    final Set<TaskId> assignedTasks1 = assignments.get(client1.processId()).tasks().keySet();
                    final Set<TaskId> assignedTasks2 = assignments.get(client2.processId()).tasks().keySet();
                    assertThat("clients shouldn't have same task assignment", assignedTasks1,
                        not(equalTo(assignedTasks2)));
                }
            }
        }
    }

    // **************************
    private Map.Entry<ProcessId, KafkaStreamsState> mkStreamState(final int id,
                                                                  final int numProcessingThreads,
                                                                  final Optional<String> rackId) {
        return mkStreamState(id, numProcessingThreads, rackId, new HashSet<>(), new HashSet<>());
    }

    private Map.Entry<ProcessId, KafkaStreamsState> mkStreamState(final int id,
                                                                  final int numProcessingThreads,
                                                                  final Optional<String> rackId,
                                                                  final Set<TaskId> previousActiveTasks,
                                                                  final Set<TaskId> previousStandbyTasks) {
        final ProcessId processId = new ProcessId(uuidForInt(id));
        return mkEntry(processId, new KafkaStreamsStateImpl(
            processId,
            numProcessingThreads,
            mkMap(),
            new TreeSet<>(previousActiveTasks),
            new TreeSet<>(previousStandbyTasks),
            new TreeMap<>(),
            Optional.empty(),
            Optional.empty(),
            rackId
        ));
    }

    private Map.Entry<TaskId, TaskInfo> mkTaskInfo(final TaskId taskId, final boolean isStateful) {
        if (!isStateful) {
            return mkEntry(
                taskId,
                new DefaultTaskInfo(taskId, false, mkSet(), mkSet())
            );
        }
        return mkEntry(
            taskId,
            new DefaultTaskInfo(
                taskId,
                true,
                mkSet("test-statestore-1"),
                mkSet(
                    new DefaultTaskTopicPartition(
                        new TopicPartition("test-topic-1", taskId.partition()),
                        true,
                        true,
                        () -> { }
                    )
                )
            )
        );
    }

    private AssignmentConfigs defaultAssignmentConfigs(final int numStandbys) {
        return new AssignmentConfigs(
            0L,
            1,
            numStandbys,
            60_000L,
            Collections.emptyList(),
            OptionalInt.empty(),
            OptionalInt.empty(),
            rackAwareStrategy
        );
    }

    private Map<ProcessId, KafkaStreamsAssignment> assign(final Map<ProcessId, KafkaStreamsState> streamStates,
                                                      final Map<TaskId, TaskInfo> tasks) {
        return assign(streamStates, tasks, 0);
    }

    private Map<ProcessId, KafkaStreamsAssignment> assign(final Map<ProcessId, KafkaStreamsState> streamStates,
                                                      final Map<TaskId, TaskInfo> tasks,
                                                      final int numStandbys) {
        final ApplicationState applicationState = new TestApplicationState(
            defaultAssignmentConfigs(numStandbys),
            streamStates,
            tasks
        );
        return indexAssignment(assignor.assign(applicationState).assignment());
    }

    private ProcessId processId(final int id) {
        return new ProcessId(uuidForInt(id));
    }

    private Map<ProcessId, KafkaStreamsAssignment> indexAssignment(final Collection<KafkaStreamsAssignment> assignments) {
        return assignments.stream().collect(Collectors.toMap(KafkaStreamsAssignment::processId, assignment -> assignment));
    }

    private Set<TaskId> activeTasks(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                    final int client) {
        final KafkaStreamsAssignment assignment = assignments.getOrDefault(processId(client), null);
        if (assignment == null) {
            return mkSet();
        }
        return assignment.tasks().values().stream().filter(t -> t.type() == ACTIVE)
            .map(AssignedTask::id)
            .collect(Collectors.toSet());
    }

    private Set<TaskId> standbyTasks(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                    final int client) {
        final KafkaStreamsAssignment assignment = assignments.getOrDefault(processId(client), null);
        if (assignment == null) {
            return mkSet();
        }
        return assignment.tasks().values().stream().filter(t -> t.type() == STANDBY)
            .map(AssignedTask::id)
            .collect(Collectors.toSet());
    }

    private Set<AssignedTask> allTasks(final Map<ProcessId, KafkaStreamsAssignment> assignments) {
        final Set<AssignedTask> allTasks = new HashSet<>();
        assignments.values().forEach(assignment -> allTasks.addAll(assignment.tasks().values()));
        return allTasks;
    }

    private void assertHasAssignment(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                     final int client,
                                     final TaskId taskId,
                                     final AssignedTask.Type taskType) {
        final KafkaStreamsAssignment assignment = assignments.getOrDefault(processId(client), null);
        assertThat(assignment, notNullValue());
        final AssignedTask assignedTask = assignment.tasks().getOrDefault(taskId, null);
        assertThat(assignedTask, notNullValue());
        assertThat(assignedTask.id().equals(taskId), is(true));
        assertThat(assignedTask.type().equals(taskType), is(true));
    }

    private void assertActiveTaskTopicGroupIdsEvenlyDistributed(final Map<ProcessId, KafkaStreamsAssignment> assignments) {
        for (final KafkaStreamsAssignment assignment : assignments.values()) {
            final List<Integer> topicGroupIds = new ArrayList<>();
            final Set<TaskId> activeTasks = assignment.tasks().values().stream()
                .map(AssignedTask::id)
                .collect(Collectors.toSet());
            for (final TaskId activeTask : activeTasks) {
                topicGroupIds.add(activeTask.subtopology());
            }
            Collections.sort(topicGroupIds);
            assertThat(topicGroupIds, equalTo(asList(1, 2)));
        }
    }

    private static class TestApplicationState implements ApplicationState {

        private final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates;
        private final AssignmentConfigs assignmentConfigs;
        private final Map<TaskId, TaskInfo> tasks;

        private TestApplicationState(final AssignmentConfigs assignmentConfigs,
                                     final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates,
                                     final Map<TaskId, TaskInfo> tasks) {
            this.kafkaStreamsStates = kafkaStreamsStates;
            this.assignmentConfigs = assignmentConfigs;
            this.tasks = tasks;
        }

        @Override
        public Map<ProcessId, KafkaStreamsState> kafkaStreamsStates(final boolean computeTaskLags) {
            return kafkaStreamsStates;
        }

        @Override
        public AssignmentConfigs assignmentConfigs() {
            return assignmentConfigs;
        }

        @Override
        public Map<TaskId, TaskInfo> allTasks() {
            return tasks;
        }
    }
}
