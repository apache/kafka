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
import java.util.HashMap;
import java.util.HashSet;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
            assertEquals(1, assignment.tasks().size());
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
        assertEquals(1, assignments.get(processId(1)).tasks().size());
        assertEquals(1, assignments.get(processId(2)).tasks().size());
        assertEquals(1, assignments.get(processId(3)).tasks().size());

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
        assertEquals(1, assignments.get(processId(1)).tasks().size());
        assertEquals(2, assignments.get(processId(2)).tasks().size());
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
        final Set<TaskId> client1Tasks = taskIdsOfType(assignments.get(processId(1)).tasks().values(), ACTIVE);
        final Set<TaskId> client2Tasks = taskIdsOfType(assignments.get(processId(2)).tasks().values(), ACTIVE);

        final Set<TaskId> allTasks = tasks.keySet();

        // one client should get 3 tasks and the other should have 4
        assertTrue(
            (client1Tasks.size() == 3 && client2Tasks.size() == 4) ||
            (client1Tasks.size() == 4 && client2Tasks.size() == 3));
        allTasks.removeAll(client1Tasks);
        // client2 should have all the remaining tasks not assigned to client 1
        assertEquals(allTasks, client2Tasks);
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
        assertEquals(1, assignments.get(processId(1)).tasks().size());
        assertEquals(1, assignments.get(processId(2)).tasks().size());
        assertEquals(1, assignments.get(processId(3)).tasks().size());
        assertEquals(0, assignments.get(processId(4)).tasks().size());
        assertEquals(0, assignments.get(processId(5)).tasks().size());

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
        assertEquals(0, assignments2.get(processId(1)).tasks().size());
        assertEquals(0, assignments2.get(processId(2)).tasks().size());
        assertEquals(1, assignments2.get(processId(3)).tasks().size());
        assertEquals(1, assignments2.get(processId(4)).tasks().size());
        assertEquals(1, assignments2.get(processId(5)).tasks().size());

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
        assertEquals(1, assignments.get(processId(1)).tasks().size());
        assertEquals(2, assignments.get(processId(2)).tasks().size());
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
        assertTrue(standbyTasks(assignments, 1).size() <= 2);
        assertTrue(standbyTasks(assignments, 2).size() <= 2);
        assertTrue(standbyTasks(assignments, 3).size() <= 2);
        assertTrue(standbyTasks(assignments, 4).size() <= 2);

        assertFalse(standbyTasks(assignments, 1).contains(TASK_0_0));
        assertFalse(standbyTasks(assignments, 2).contains(TASK_0_1));
        assertFalse(standbyTasks(assignments, 3).contains(TASK_0_2));
        assertFalse(standbyTasks(assignments, 4).contains(TASK_0_3));

        assertTrue(activeTasks(assignments, 1).contains(TASK_0_0));
        assertTrue(activeTasks(assignments, 2).contains(TASK_0_1));
        assertTrue(activeTasks(assignments, 3).contains(TASK_0_2));
        assertTrue(activeTasks(assignments, 4).contains(TASK_0_3));

        int nonEmptyStandbyTaskCount = 0;
        for (int i = 1; i <= 4; i++) {
            nonEmptyStandbyTaskCount += standbyTasks(assignments, i).isEmpty() ? 0 : 1;
        }

        assertTrue(nonEmptyStandbyTaskCount >= 3);

        final Set<TaskId> allStandbyTasks = taskIdsOfType(allTasks(assignments), STANDBY);
        assertEquals(Set.of(TASK_0_0, TASK_0_1, TASK_0_2, TASK_0_3), allStandbyTasks);
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
        assertEquals(Set.of(TASK_0_0), activeTasks(assignments, 1));
        assertEquals(Set.of(TASK_0_1), activeTasks(assignments, 2));
        assertEquals(Set.of(TASK_0_2), activeTasks(assignments, 3));

        assertEquals(Set.of(TASK_0_1, TASK_0_2), standbyTasks(assignments, 1));
        assertEquals(Set.of(TASK_0_0, TASK_0_2), standbyTasks(assignments, 2));
        assertEquals(Set.of(TASK_0_0, TASK_0_1), standbyTasks(assignments, 3));
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
        assertEquals(Set.of(TASK_0_0), activeTasks(assignments, 1));
        assertEquals(Set.of(), standbyTasks(assignments, 1));
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
        assertEquals(Set.of(TASK_0_0, TASK_0_1, TASK_0_2), taskIdsOfType(allTasks, ACTIVE));
        assertEquals(Set.of(TASK_0_0, TASK_0_1, TASK_0_2), taskIdsOfType(allTasks, STANDBY));
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
        assertEquals(1, activeTasks(assignments, 1).size());
        assertEquals(1, activeTasks(assignments, 2).size());
        assertEquals(1, activeTasks(assignments, 3).size());
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
        assertEquals(Set.of(TASK_0_0, TASK_0_1, TASK_0_2), taskIdsOfType(allTasks, ACTIVE));
        assertEquals(Set.of(), taskIdsOfType(allTasks, STANDBY));

        final int clientsWithATask = assignments.values().stream().mapToInt(assignment -> assignment.tasks().isEmpty() ? 0 : 1).sum();
        assertTrue(clientsWithATask >= 3);
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
            assertFalse(assignment.tasks().isEmpty());
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
        assertEquals(4, activeTasks(assignments, 1).size());
        assertEquals(8, activeTasks(assignments, 2).size());
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
                    assertNotEquals(assignedTasks2, assignedTasks1, "clients shouldn't have same task assignment");
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
                    assertNotEquals(assignedTasks2, assignedTasks1, "clients shouldn't have same task assignment");
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
        assertEquals(Set.of(), standbyTasks(assignments, 1));
        assertEquals(Set.of(TASK_0_0), standbyTasks(assignments, 2));
        assertEquals(Set.of(TASK_0_0), standbyTasks(assignments, 3));
        assertEquals(Set.of(TASK_0_0), standbyTasks(assignments, 4));
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
        assertEquals(topicCount * taskPerTopic, allActiveTasks.size());
        final List<TaskId> allStandbyTasks = allTasks(assignments).stream().filter(t -> t.type() == STANDBY)
            .map(AssignedTask::id)
            .collect(Collectors.toList());
        assertEquals(topicCount * taskPerTopic * numStandbys, allStandbyTasks.size());
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
        assertEquals(TaskAssignor.AssignmentError.NONE, assignmentError);
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
        return taskIdsOfType(assignment.tasks().values(), ACTIVE);
    }

    private Set<TaskId> standbyTasks(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                    final int client) {
        final KafkaStreamsAssignment assignment = assignments.getOrDefault(processId(client), null);
        if (assignment == null) {
            return Set.of();
        }
        return taskIdsOfType(assignment.tasks().values(), STANDBY);
    }

    private Set<TaskId> taskIdsOfType(final Collection<AssignedTask> tasks,
                                      final AssignedTask.Type type) {
        return tasks.stream()
            .filter(t -> t.type() == type)
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
        assertNotNull(assignment);
        final AssignedTask assignedTask = assignment.tasks().getOrDefault(taskId, null);
        assertNotNull(assignedTask);
        assertEquals(assignedTask.id(), taskId);
        assertEquals(assignedTask.type(), taskType);
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
            assertEquals(asList(1, 2), topicGroupIds);
        }
    }

    // --- KAFKA-20198 regression tests ---

    @Test
    public void shouldRetainFairShareOfPreviousTasksWhenScalingUp() {
        final int numTasks = 450;
        final int threadsPerInstance = 10;
        final Map<TaskId, TaskInfo> tasks = buildStatelessTasks(numTasks);
        final Set<TaskId> firstHalf = buildTaskIdRange(0, numTasks / 2);

        final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
            mkStreamState(1, threadsPerInstance, Optional.empty(), firstHalf, Set.of()),
            mkStreamState(2, threadsPerInstance, Optional.empty())
        );

        final Map<ProcessId, KafkaStreamsAssignment> assignments =
            assign(streamStates, tasks, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE);

        final Set<TaskId> instance1Tasks = activeTasks(assignments, 1);
        final int retained = (int) firstHalf.stream().filter(instance1Tasks::contains).count();

        assertEquals(firstHalf.size(), retained,
            "Instance should retain all of its fair share tasks, but only retained " + retained + " of " + firstHalf.size());
    }

    @Test
    public void shouldConvergeWithinTwoRoundsWhenScalingUp() {
        // Reporter's scenario: 450 tasks with 10+10=20 threads.
        // 450/20 = 22.5, floor gives limit 220 per instance (10 task overflow),
        // causing repeated reassignment across rounds.
        final int numTasks = 450;
        final int maxRounds = 2;
        final Map<TaskId, TaskInfo> tasks = buildStatelessTasks(numTasks);

        Set<TaskId> instance1Prev = buildTaskIdRange(0, numTasks);
        Set<TaskId> instance2Prev = Set.of();

        for (int round = 1; round <= maxRounds; round++) {
            final Map<ProcessId, KafkaStreamsState> streamStates = mkMap(
                mkStreamState(1, 10, Optional.empty(), instance1Prev, Set.of()),
                mkStreamState(2, 10, Optional.empty(), instance2Prev, Set.of())
            );

            final Map<ProcessId, KafkaStreamsAssignment> assignments =
                assign(streamStates, tasks, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE);

            final Set<TaskId> newInstance1 = activeTasks(assignments, 1);
            final Set<TaskId> newInstance2 = activeTasks(assignments, 2);

            if (newInstance1.equals(instance1Prev) && newInstance2.equals(instance2Prev)) {
                return; // converged
            }
            instance1Prev = newInstance1;
            instance2Prev = newInstance2;
        }
        fail("Assignment did not converge within " + maxRounds + " rounds");
    }

    private static Map<TaskId, TaskInfo> buildStatelessTasks(final int count) {
        final Map<TaskId, TaskInfo> tasks = new HashMap<>();
        for (int i = 0; i < count; i++) {
            final TaskId taskId = new TaskId(0, i);
            tasks.put(taskId, mkTaskInfo(taskId, false).getValue());
        }
        return tasks;
    }

    private static Set<TaskId> buildTaskIdRange(final int from, final int to) {
        final Set<TaskId> set = new HashSet<>();
        for (int i = from; i < to; i++) {
            set.add(new TaskId(0, i));
        }
        return set;
    }
}
