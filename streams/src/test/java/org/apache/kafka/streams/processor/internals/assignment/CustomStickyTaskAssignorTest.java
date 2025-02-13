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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.assignment.TaskAssignor.TaskAssignment;
import org.apache.kafka.streams.processor.assignment.TaskInfo;
import org.apache.kafka.streams.processor.assignment.assignors.StickyTaskAssignor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkMap;
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
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtilsTest.mkStreamState;
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtilsTest.mkTaskInfo;
import static org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentUtilsTest.processId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class CustomStickyTaskAssignorTest {

    private TaskAssignor assignor;

    @BeforeEach
    public void setUp() {
        assignor = new StickyTaskAssignor();
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignOneActiveTaskToEachProcessWhenTaskCountSameAsProcessCount(final String rackAwareStrategy) {
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

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        for (final KafkaStreamsAssignment assignment : assignments.values()) {
            assertThat(assignment.tasks().size(), equalTo(1));
        }
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithNoStandByTasks(final String rackAwareStrategy) {
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

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertActiveTaskTopicGroupIdsEvenlyDistributed(assignments);
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignTopicGroupIdEvenlyAcrossClientsWithStandByTasks(final String rackAwareStrategy) {
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
        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1, rackAwareStrategy);
        assertActiveTaskTopicGroupIdsEvenlyDistributed(assignments);
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldNotMigrateActiveTaskToOtherProcess(final String rackAwareStrategy) {
        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), Set.of(TASK_0_0), Set.of()),
            mkStreamState(2, 1, Optional.empty(), Set.of(TASK_0_1), Set.of())
        );

        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertHasAssignment(assignments, 1, TASK_0_0, ACTIVE);
        assertHasAssignment(assignments, 2, TASK_0_1, ACTIVE);

        final Map<ProcessId, KafkaStreamsState> streamStates2 = mkMap(
            mkStreamState(1, 1, Optional.empty(), Set.of(TASK_0_1), Set.of()),
            mkStreamState(2, 1, Optional.empty(), Set.of(TASK_0_0), Set.of())
        );
        final Map<ProcessId, KafkaStreamsAssignment> assignments2 = assign(streamStates2, tasks, rackAwareStrategy);
        assertHasAssignment(assignments2, 1, TASK_0_1, ACTIVE);
        assertHasAssignment(assignments2, 2, TASK_0_0, ACTIVE);
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldMigrateActiveTasksToNewProcessWithoutChangingAllAssignments(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), Set.of(TASK_0_0, TASK_0_2), Set.of()),
            mkStreamState(2, 1, Optional.empty(), Set.of(TASK_0_1), Set.of()),
            mkStreamState(3, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertThat(assignments.get(processId(1)).tasks().values().size(), equalTo(1));
        assertThat(assignments.get(processId(2)).tasks().values().size(), equalTo(1));
        assertThat(assignments.get(processId(3)).tasks().values().size(), equalTo(1));

        assertHasAssignment(assignments, 2, TASK_0_1, ACTIVE);
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignBasedOnCapacity(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );
        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty()),
            mkStreamState(2, 2, Optional.empty())
        );
        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertThat(assignments.get(processId(1)).tasks().values().size(), equalTo(1));
        assertThat(assignments.get(processId(2)).tasks().values().size(), equalTo(2));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignTasksEvenlyWithUnequalTopicGroupSizes(final String rackAwareStrategy) {
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
            mkStreamState(1, 1, Optional.empty(), Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3, TASK_0_4, TASK_0_5, TASK_1_0), Set.of()),
            mkStreamState(2, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
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

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldKeepActiveTaskStickinessWhenMoreClientThanActiveTasks(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), Set.of(TASK_0_0), Set.of()),
            mkStreamState(2, 1, Optional.empty(), Set.of(TASK_0_2), Set.of()),
            mkStreamState(3, 1, Optional.empty(), Set.of(TASK_0_1), Set.of()),
            mkStreamState(4, 1, Optional.empty()),
            mkStreamState(5, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
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
            mkStreamState(3, 1, Optional.empty(), Set.of(TASK_0_1), Set.of()),
            mkStreamState(4, 1, Optional.empty(), Set.of(TASK_0_0), Set.of()),
            mkStreamState(5, 1, Optional.empty(), Set.of(TASK_0_2), Set.of())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments2 = assign(streamStates2, tasks, rackAwareStrategy);
        assertThat(assignments2.get(processId(1)).tasks().size(), is(0));
        assertThat(assignments2.get(processId(2)).tasks().size(), is(0));
        assertThat(assignments2.get(processId(3)).tasks().size(), is(1));
        assertThat(assignments2.get(processId(4)).tasks().size(), is(1));
        assertThat(assignments2.get(processId(5)).tasks().size(), is(1));

        assertHasAssignment(assignments2, 3, TASK_0_1, ACTIVE);
        assertHasAssignment(assignments2, 4, TASK_0_0, ACTIVE);
        assertHasAssignment(assignments2, 5, TASK_0_2, ACTIVE);
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignTasksToClientWithPreviousStandbyTasks(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), Set.of(), Set.of(TASK_0_2)),
            mkStreamState(2, 1, Optional.empty(), Set.of(), Set.of(TASK_0_1)),
            mkStreamState(3, 1, Optional.empty(), Set.of(), Set.of(TASK_0_0))
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertHasAssignment(assignments, 1, TASK_0_2, ACTIVE);
        assertHasAssignment(assignments, 2, TASK_0_1, ACTIVE);
        assertHasAssignment(assignments, 3, TASK_0_0, ACTIVE);
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignBasedOnCapacityWhenMultipleClientHaveStandbyTasks(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, false),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), Set.of(TASK_0_0), Set.of(TASK_0_1)),
            mkStreamState(2, 2, Optional.empty(), Set.of(TASK_0_2), Set.of(TASK_0_1))
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertThat(assignments.get(processId(1)).tasks().size(), is(1));
        assertThat(assignments.get(processId(2)).tasks().size(), is(2));
        assertHasAssignment(assignments, 1, TASK_0_0, ACTIVE);
        assertHasAssignment(assignments, 2, TASK_0_1, ACTIVE);
        assertHasAssignment(assignments, 2, TASK_0_2, ACTIVE);
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignStandbyTasksToDifferentClientThanCorrespondingActiveTaskIsAssignedTo(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true),
            mkTaskInfo(TASK_0_1, true),
            mkTaskInfo(TASK_0_2, true),
            mkTaskInfo(TASK_0_3, true)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), Set.of(TASK_0_0), Set.of()),
            mkStreamState(2, 1, Optional.empty(), Set.of(TASK_0_1), Set.of()),
            mkStreamState(3, 1, Optional.empty(), Set.of(TASK_0_2), Set.of()),
            mkStreamState(4, 1, Optional.empty(), Set.of(TASK_0_3), Set.of())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1, rackAwareStrategy);
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
        assertThat(allStandbyTasks, equalTo(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3)));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignMultipleReplicasOfStandbyTask(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true),
            mkTaskInfo(TASK_0_1, true),
            mkTaskInfo(TASK_0_2, true)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), Set.of(TASK_0_0), Set.of()),
            mkStreamState(2, 1, Optional.empty(), Set.of(TASK_0_1), Set.of()),
            mkStreamState(3, 1, Optional.empty(), Set.of(TASK_0_2), Set.of())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 2, rackAwareStrategy);
        assertThat(activeTasks(assignments, 1), equalTo(Set.of(TASK_0_0)));
        assertThat(activeTasks(assignments, 2), equalTo(Set.of(TASK_0_1)));
        assertThat(activeTasks(assignments, 3), equalTo(Set.of(TASK_0_2)));

        assertThat(standbyTasks(assignments, 1), equalTo(Set.of(TASK_0_1, TASK_0_2)));
        assertThat(standbyTasks(assignments, 2), equalTo(Set.of(TASK_0_0, TASK_0_2)));
        assertThat(standbyTasks(assignments, 3), equalTo(Set.of(TASK_0_0, TASK_0_1)));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldNotAssignStandbyTaskReplicasWhenNoClientAvailableWithoutHavingTheTaskAssigned(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), Set.of(TASK_0_0), Set.of())
        );
        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 2, rackAwareStrategy);
        assertThat(activeTasks(assignments, 1), equalTo(Set.of(TASK_0_0)));
        assertThat(standbyTasks(assignments, 1), equalTo(Set.of()));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignActiveAndStandbyTasks(final String rackAwareStrategy) {
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

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1, rackAwareStrategy);
        final List<AssignedTask> allTasks = allTasks(assignments);
        assertThat(allTasks.stream().filter(t -> t.type() == ACTIVE).map(AssignedTask::id).collect(
            Collectors.toSet()), equalTo(Set.of(TASK_0_0, TASK_0_1, TASK_0_2)));
        assertThat(allTasks.stream().filter(t -> t.type() == STANDBY).map(AssignedTask::id).collect(
            Collectors.toSet()), equalTo(Set.of(TASK_0_0, TASK_0_1, TASK_0_2)));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignAtLeastOneTaskToEachClientIfPossible(final String rackAwareStrategy) {
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

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertThat(activeTasks(assignments, 1).size(), is(1));
        assertThat(activeTasks(assignments, 2).size(), is(1));
        assertThat(activeTasks(assignments, 3).size(), is(1));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignEachActiveTaskToOneClientWhenMoreClientsThanTasks(final String rackAwareStrategy) {
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

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        final List<AssignedTask> allTasks = allTasks(assignments);
        assertThat(allTasks.stream().filter(t -> t.type() == ACTIVE).map(AssignedTask::id).collect(
            Collectors.toSet()), equalTo(Set.of(TASK_0_0, TASK_0_1, TASK_0_2)));
        assertThat(allTasks.stream().filter(t -> t.type() == STANDBY).map(AssignedTask::id).collect(
            Collectors.toSet()), equalTo(Set.of()));

        final int clientsWithATask = assignments.values().stream().mapToInt(assignment -> assignment.tasks().isEmpty() ? 0 : 1).sum();
        assertThat(clientsWithATask, greaterThanOrEqualTo(3));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldBalanceActiveAndStandbyTasksAcrossAvailableClients(final String rackAwareStrategy) {
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

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1, rackAwareStrategy);
        for (final KafkaStreamsAssignment assignment : assignments.values()) {
            assertThat(assignment.tasks().values(), not(hasSize(0)));
        }
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignMoreTasksToClientWithMoreCapacity(final String rackAwareStrategy) {
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

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, rackAwareStrategy);
        assertThat(activeTasks(assignments, 1).size(), equalTo(4));
        assertThat(activeTasks(assignments, 2).size(), equalTo(8));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @Test
    public void shouldEvenlyDistributeByTaskIdAndPartition() {
        // TODO: port shouldEvenlyDistributeByTaskIdAndPartition from StickyTaskAssignorTest
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldNotHaveSameAssignmentOnAnyTwoHosts(final String rackAwareStrategy) {
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

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1, rackAwareStrategy);

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

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousActiveTasks(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true),
            mkTaskInfo(TASK_0_1, true),
            mkTaskInfo(TASK_0_2, true),
            mkTaskInfo(TASK_0_3, true)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty(), Set.of(TASK_0_1, TASK_0_2), Set.of()),
            mkStreamState(2, 1, Optional.empty(), Set.of(TASK_0_3), Set.of()),
            mkStreamState(3, 1, Optional.empty(), Set.of(TASK_0_0), Set.of()),
            mkStreamState(4, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 1, rackAwareStrategy);

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

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldAssignMultipleStandbys(final String rackAwareStrategy) {
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false)
        );

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, 1, Optional.empty()),
            mkStreamState(2, 1, Optional.empty()),
            mkStreamState(3, 1, Optional.empty()),
            mkStreamState(4, 1, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, 3, rackAwareStrategy);
        assertThat(standbyTasks(assignments, 1), equalTo(Set.of()));
        assertThat(standbyTasks(assignments, 2), equalTo(Set.of(TASK_0_0)));
        assertThat(standbyTasks(assignments, 3), equalTo(Set.of(TASK_0_0)));
        assertThat(standbyTasks(assignments, 4), equalTo(Set.of(TASK_0_0)));
    }

    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void largeAssignmentShouldTerminateWithinAcceptableTime(final String rackAwareStrategy) {
        final int topicCount = 10;
        final int taskPerTopic = 30;
        final int numStandbys = 2;
        final int clientCount = 20;
        final int clientCapacity = 50;

        final Map<TaskId, TaskInfo> tasks = mkMap();
        for (int i = 0; i < topicCount; i++) {
            for (int j = 0; j < taskPerTopic; j++) {
                final TaskId newTaskId = new TaskId(i, j);
                final Set<String> partitionRacks = Set.of(
                    String.format("rack-%d", (i * j) % 31)
                );
                final Map.Entry<TaskId, TaskInfo> newTask = mkTaskInfo(newTaskId, true, partitionRacks);
                tasks.put(newTask.getKey(), newTask.getValue());
            }
        }

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap();
        for (int i = 0; i < clientCount; i++) {
            final Map.Entry<ProcessId, KafkaStreamsState> newClient = mkStreamState(
                i + 1,
                clientCapacity,
                Optional.of(String.format("rack-%d", i % 31)),
                Set.of(),
                Set.of()
            );
            streamStates.put(newClient.getKey(), newClient.getValue());
        }

        final AssignmentConfigs assignmentConfigs = new AssignmentConfigs(
            0L,
            1,
            numStandbys,
            60_000L,
            Collections.emptyList(),
            OptionalInt.of(1),
            OptionalInt.of(2),
            rackAwareStrategy
        );
        final Map<ProcessId, KafkaStreamsAssignment> assignments = assign(streamStates, tasks, assignmentConfigs);
        final List<TaskId> allActiveTasks = allTasks(assignments).stream().filter(t -> t.type() == ACTIVE)
            .map(AssignedTask::id)
            .collect(Collectors.toList());
        assertThat(allActiveTasks.size(), equalTo(topicCount * taskPerTopic));
        final List<TaskId> allStandbyTasks = allTasks(assignments).stream().filter(t -> t.type() == STANDBY)
            .map(AssignedTask::id)
            .collect(Collectors.toList());
        assertThat(allStandbyTasks.size(), equalTo(topicCount * taskPerTopic * numStandbys));
    }

    private Map<ProcessId, KafkaStreamsAssignment> assign(final Map<ProcessId, KafkaStreamsState> streamStates,
                                                          final Map<TaskId, TaskInfo> tasks,
                                                          final String rackAwareStrategy) {
        return assign(streamStates, tasks, 0, rackAwareStrategy);
    }

    private Map<ProcessId, KafkaStreamsAssignment> assign(final Map<ProcessId, KafkaStreamsState> streamStates,
                                                          final Map<TaskId, TaskInfo> tasks,
                                                          final int numStandbys,
                                                          final String rackAwareStrategy) {
        return assign(streamStates, tasks, defaultAssignmentConfigs(numStandbys, rackAwareStrategy));
    }

    private Map<ProcessId, KafkaStreamsAssignment> assign(final Map<ProcessId, KafkaStreamsState> streamStates,
                                                          final Map<TaskId, TaskInfo> tasks,
                                                          final AssignmentConfigs assignmentConfigs) {
        final ApplicationState applicationState = new TaskAssignmentUtilsTest.TestApplicationState(
            assignmentConfigs,
            streamStates,
            tasks
        );
        final TaskAssignment taskAssignment = assignor.assign(applicationState);
        final TaskAssignor.AssignmentError assignmentError = TaskAssignmentUtils.validateTaskAssignment(applicationState, taskAssignment);
        assertThat(assignmentError, equalTo(TaskAssignor.AssignmentError.NONE));
        return indexAssignment(taskAssignment.assignment());
    }

    public AssignmentConfigs defaultAssignmentConfigs(final int numStandbys, final String rackAwareStrategy) {
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

    private Map<ProcessId, KafkaStreamsAssignment> indexAssignment(final Collection<KafkaStreamsAssignment> assignments) {
        return assignments.stream().collect(Collectors.toMap(KafkaStreamsAssignment::processId, assignment -> assignment));
    }

    private Set<TaskId> activeTasks(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                    final int client) {
        final KafkaStreamsAssignment assignment = assignments.getOrDefault(processId(client), null);
        if (assignment == null) {
            return Set.of();
        }
        return assignment.tasks().values().stream().filter(t -> t.type() == ACTIVE)
            .map(AssignedTask::id)
            .collect(Collectors.toSet());
    }

    private Set<TaskId> standbyTasks(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                    final int client) {
        final KafkaStreamsAssignment assignment = assignments.getOrDefault(processId(client), null);
        if (assignment == null) {
            return Set.of();
        }
        return assignment.tasks().values().stream().filter(t -> t.type() == STANDBY)
            .map(AssignedTask::id)
            .collect(Collectors.toSet());
    }

    private List<AssignedTask> allTasks(final Map<ProcessId, KafkaStreamsAssignment> assignments) {
        final List<AssignedTask> allTasks = new ArrayList<>();
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
}
