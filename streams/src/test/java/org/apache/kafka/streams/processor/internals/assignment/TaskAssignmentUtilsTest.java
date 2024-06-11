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

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_0;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_3;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_4;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_5;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.processIdForInt;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils;
import org.apache.kafka.streams.processor.assignment.TaskInfo;
import org.apache.kafka.streams.processor.assignment.TaskTopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.Timeout;

public class TaskAssignmentUtilsTest {

    @Rule
    public Timeout timeout = new Timeout(30, TimeUnit.SECONDS);

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldOptimizeActiveTaskSimple(final String strategy) {
        final AssignmentConfigs assignmentConfigs = defaultAssignmentConfigs(
            strategy, 100, 1, 1, Collections.emptyList());
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true, mkSet("rack-2")),
            mkTaskInfo(TASK_0_1, true, mkSet("rack-1"))
        );
        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = mkMap(
            mkStreamState(1, 1, Optional.of("rack-1")),
            mkStreamState(2, 1, Optional.of("rack-2"))
        );
        final ApplicationState applicationState = new TestApplicationState(
            assignmentConfigs, kafkaStreamsStates, tasks);

        final Map<ProcessId, KafkaStreamsAssignment> assignments = mkMap(
            mkAssignment(AssignedTask.Type.ACTIVE, 1, TASK_0_0),
            mkAssignment(AssignedTask.Type.ACTIVE, 2, TASK_0_1)
        );

        TaskAssignmentUtils.optimizeRackAwareActiveTasks(
            applicationState, assignments, new TreeSet<>(tasks.keySet()));
        assertThat(assignments.size(), equalTo(2));
        assertThat(assignments.get(processId(1)).tasks().keySet(), equalTo(mkSet(TASK_0_1)));
        assertThat(assignments.get(processId(2)).tasks().keySet(), equalTo(mkSet(TASK_0_0)));

        // Repeated to make sure nothing gets shifted around after the first round of optimization.
        TaskAssignmentUtils.optimizeRackAwareActiveTasks(
            applicationState, assignments, new TreeSet<>(tasks.keySet()));
        assertThat(assignments.size(), equalTo(2));
        assertThat(assignments.get(processId(1)).tasks().keySet(), equalTo(mkSet(TASK_0_1)));
        assertThat(assignments.get(processId(2)).tasks().keySet(), equalTo(mkSet(TASK_0_0)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC,
        StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY,
    })
    public void shouldOptimizeStandbyTasksBasic(final String strategy) {
        final AssignmentConfigs assignmentConfigs = defaultAssignmentConfigs(
            strategy, 100, 1, 1, Collections.emptyList());
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true, mkSet("rack-2")),
            mkTaskInfo(TASK_0_1, true, mkSet("rack-3"))
        );
        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = mkMap(
            mkStreamState(1, 2, Optional.of("rack-1")),
            mkStreamState(2, 2, Optional.of("rack-2")),
            mkStreamState(3, 2, Optional.of("rack-3"))
        );
        final ApplicationState applicationState = new TestApplicationState(
            assignmentConfigs, kafkaStreamsStates, tasks);

        final Map<ProcessId, KafkaStreamsAssignment> assignments = mkMap(
            mkAssignment(AssignedTask.Type.ACTIVE, 1, TASK_0_0, TASK_0_1),
            mkAssignment(AssignedTask.Type.STANDBY, 2, TASK_0_1),
            mkAssignment(AssignedTask.Type.STANDBY, 3, TASK_0_0)
        );

        TaskAssignmentUtils.optimizeRackAwareStandbyTasks(applicationState, assignments);
        assertThat(assignments.size(), equalTo(3));
        assertThat(assignments.get(processId(1)).tasks().keySet(), equalTo(mkSet(TASK_0_0, TASK_0_1)));
        assertThat(assignments.get(processId(2)).tasks().keySet(), equalTo(mkSet(TASK_0_0)));
        assertThat(assignments.get(processId(3)).tasks().keySet(), equalTo(mkSet(TASK_0_1)));
    }

    @Test
    public void shouldAssignStandbyTasksWithClientTags() {
        final AssignmentConfigs assignmentConfigs = defaultAssignmentConfigs(
            StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE, 100, 1, 2, Collections.singletonList("az"));
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true)
        );
        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = mkMap(
            mkStreamState(1, 2, Optional.empty(), mkSet(), mkSet(), mkMap(
                mkEntry("az", "1")
            )),
            mkStreamState(2, 2, Optional.empty(), mkSet(), mkSet(), mkMap(
                mkEntry("az", "1")
            )),
            mkStreamState(3, 2, Optional.empty(), mkSet(), mkSet(), mkMap(
                mkEntry("az", "2")
            )),
            mkStreamState(4, 2, Optional.empty(), mkSet(), mkSet(), mkMap(
                mkEntry("az", "3")
            ))
        );
        final ApplicationState applicationState = new TestApplicationState(
            assignmentConfigs, kafkaStreamsStates, tasks);

        final Map<ProcessId, KafkaStreamsAssignment> assignments = mkMap(
            mkAssignment(AssignedTask.Type.ACTIVE, 1, TASK_0_0)
        );

        TaskAssignmentUtils.defaultStandbyTaskAssignment(applicationState, assignments);
        assertThat(assignments.size(), equalTo(4));
        assertThat(assignments.get(processId(1)).tasks().keySet(), equalTo(mkSet(TASK_0_0)));
        assertThat(assignments.get(processId(1)).tasks().get(TASK_0_0).type(), equalTo(AssignedTask.Type.ACTIVE));

        assertThat(assignments.get(processId(2)).tasks().keySet(), equalTo(mkSet()));
        assertThat(assignments.get(processId(3)).tasks().keySet(), equalTo(mkSet(TASK_0_0)));
        assertThat(assignments.get(processId(4)).tasks().keySet(), equalTo(mkSet(TASK_0_0)));
    }

    @Test
    public void shouldAssignStandbyTasksByClientLoad() {
        final AssignmentConfigs assignmentConfigs = defaultAssignmentConfigs(
            StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE, 100, 1, 3, Collections.emptyList());
        final Map<TaskId, TaskInfo> tasks = mkMap(
            mkTaskInfo(TASK_0_0, true),
            mkTaskInfo(TASK_0_1, false),
            mkTaskInfo(TASK_0_2, false),
            mkTaskInfo(TASK_0_3, false),
            mkTaskInfo(TASK_0_4, false),
            mkTaskInfo(TASK_0_5, false)
        );
        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = mkMap(
            mkStreamState(1, 5, Optional.empty(), mkSet(), mkSet()),
            mkStreamState(2, 5, Optional.empty(), mkSet(), mkSet()),
            mkStreamState(3, 5, Optional.empty(), mkSet(), mkSet()),
            mkStreamState(4, 5, Optional.empty(), mkSet(), mkSet()),
            mkStreamState(5, 5, Optional.empty(), mkSet(), mkSet())
        );
        final ApplicationState applicationState = new TestApplicationState(
            assignmentConfigs, kafkaStreamsStates, tasks);

        final Map<ProcessId, KafkaStreamsAssignment> assignments = mkMap(
            mkAssignment(AssignedTask.Type.ACTIVE, 1, TASK_0_0, TASK_0_1, TASK_0_2),
            mkAssignment(AssignedTask.Type.ACTIVE, 2, TASK_0_3, TASK_0_4, TASK_0_5)
        );

        TaskAssignmentUtils.defaultStandbyTaskAssignment(applicationState, assignments);
        assertThat(assignments.size(), equalTo(5));
        assertThat(assignments.get(processId(2)).tasks().keySet(), equalTo(mkSet(TASK_0_3, TASK_0_4, TASK_0_5)));
        assertThat(assignments.get(processId(3)).tasks().keySet(), equalTo(mkSet(TASK_0_0)));
        assertThat(assignments.get(processId(4)).tasks().keySet(), equalTo(mkSet(TASK_0_0)));
        assertThat(assignments.get(processId(5)).tasks().keySet(), equalTo(mkSet(TASK_0_0)));
    }

    public static class TestApplicationState implements ApplicationState {

        private final AssignmentConfigs assignmentConfigs;
        private final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates;
        private final Map<TaskId, TaskInfo> tasks;

        TestApplicationState(final AssignmentConfigs assignmentConfigs,
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

    public static Map.Entry<ProcessId, KafkaStreamsState> mkStreamState(final int id,
                                                                        final int numProcessingThreads,
                                                                        final Optional<String> rackId) {
        return mkStreamState(id, numProcessingThreads, rackId, new HashSet<>(), new HashSet<>(), mkMap());
    }

    public static Map.Entry<ProcessId, KafkaStreamsState> mkStreamState(final int id,
                                                                        final int numProcessingThreads,
                                                                        final Optional<String> rackId,
                                                                        final Set<TaskId> previousActiveTasks,
                                                                        final Set<TaskId> previousStandbyTasks) {
        return mkStreamState(id, numProcessingThreads, rackId, previousActiveTasks, previousStandbyTasks, mkMap());
    }

    public static Map.Entry<ProcessId, KafkaStreamsState> mkStreamState(final int id,
                                                                        final int numProcessingThreads,
                                                                        final Optional<String> rackId,
                                                                        final Set<TaskId> previousActiveTasks,
                                                                        final Set<TaskId> previousStandbyTasks,
                                                                        final Map<String, String> clientTags) {
        final ProcessId processId = processIdForInt(id);
        return mkEntry(processId, new DefaultKafkaStreamsState(
            processId,
            numProcessingThreads,
            clientTags,
            new TreeSet<>(previousActiveTasks),
            new TreeSet<>(previousStandbyTasks),
            new TreeMap<>(),
            Optional.empty(),
            Optional.empty(),
            rackId
        ));
    }

    public static ProcessId processId(final int id) {
        return processIdForInt(id);
    }

    public static Map.Entry<ProcessId, KafkaStreamsAssignment> mkAssignment(final AssignedTask.Type taskType,
                                                                            final int client,
                                                                            final TaskId... taskIds) {
        final ProcessId processId = processId(client);
        final Set<AssignedTask> assignedTasks = mkSet();
        for (final TaskId taskId : taskIds) {
            assignedTasks.add(new AssignedTask(taskId, taskType));
        }
        return mkEntry(
            processId,
            KafkaStreamsAssignment.of(
                processId,
                assignedTasks
            )
        );
    }

    public static Map.Entry<TaskId, TaskInfo> mkTaskInfo(final TaskId taskId, final boolean isStateful) {
        return mkTaskInfo(taskId, isStateful, null);
    }

    public static Map.Entry<TaskId, TaskInfo> mkTaskInfo(final TaskId taskId, final boolean isStateful, final Set<String> rackIds) {
        if (!isStateful) {
            return mkEntry(
                taskId,
                new DefaultTaskInfo(taskId, false, mkSet(), mkSet())
            );
        }

        final Set<DefaultTaskTopicPartition> partitions = new HashSet<>();
        partitions.add(new DefaultTaskTopicPartition(
            new TopicPartition(String.format("test-topic-%d", taskId.subtopology()), taskId.partition()),
            true,
            true,
            () -> {
                partitions.forEach(partition -> {
                    if (partition != null && rackIds != null) {
                        partition.annotateWithRackIds(rackIds);
                    }
                });
            }
        ));
        return mkEntry(
            taskId,
            new DefaultTaskInfo(
                taskId,
                true,
                mkSet(String.format("test-statestore-%d", taskId.subtopology())),
                partitions.stream().map(p -> (TaskTopicPartition) p).collect(Collectors.toSet())
            )
        );
    }

    public AssignmentConfigs defaultAssignmentConfigs(final String rackAwareStrategy,
                                                      final int trafficCost,
                                                      final int nonOverlapCost,
                                                      final int numStandbys,
                                                      final List<String> rackAwareAssignmentTags) {
        return new AssignmentConfigs(
            0L,
            1,
            numStandbys,
            60_000L,
            rackAwareAssignmentTags,
            OptionalInt.of(trafficCost),
            OptionalInt.of(nonOverlapCost),
            rackAwareStrategy
        );
    }

}
