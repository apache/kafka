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
package org.apache.kafka.streams.processor.assignment;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import org.apache.kafka.streams.processor.assignment.TaskAssignor.AssignmentError;
import org.apache.kafka.streams.processor.assignment.TaskAssignor.TaskAssignment;
import org.apache.kafka.streams.processor.internals.assignment.ConstrainedPrioritySet;
import org.apache.kafka.streams.processor.internals.assignment.Graph;
import org.apache.kafka.streams.processor.internals.assignment.MinTrafficGraphConstructor;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareGraphConstructor;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareGraphConstructorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor.STANDBY_OPTIMIZER_MAX_ITERATION;

/**
 * A set of utilities to help implement task assignment via the {@link TaskAssignor}
 */
public final class TaskAssignmentUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TaskAssignmentUtils.class);

    private TaskAssignmentUtils() {}

    /**
     * A simple config container for necessary parameters and optional overrides to apply when
     * running the active or standby task rack-aware optimizations.
     */
    public static final class RackAwareOptimizationParams {
        private final ApplicationState applicationState;
        private final Optional<Integer> trafficCostOverride;
        private final Optional<Integer> nonOverlapCostOverride;
        private final Optional<SortedSet<TaskId>> tasksToOptimize;

        private RackAwareOptimizationParams(final ApplicationState applicationState,
                                            final Optional<Integer> trafficCostOverride,
                                            final Optional<Integer> nonOverlapCostOverride,
                                            final Optional<SortedSet<TaskId>> tasksToOptimize) {
            this.applicationState = applicationState;
            this.trafficCostOverride = trafficCostOverride;
            this.nonOverlapCostOverride = nonOverlapCostOverride;
            this.tasksToOptimize = tasksToOptimize;
        }

        /**
         * Return a new config object with no overrides and the tasksToOptimize initialized to the set of all tasks in the given ApplicationState
         */
        public static RackAwareOptimizationParams of(final ApplicationState applicationState) {
            return new RackAwareOptimizationParams(applicationState, Optional.empty(), Optional.empty(), Optional.empty());
        }

        /**
         * Return a new config object with the tasksToOptimize set to all stateful tasks in the given ApplicationState
         */
        public RackAwareOptimizationParams forStatefulTasks() {
            final SortedSet<TaskId> tasks = applicationState.allTasks().values()
                .stream()
                .filter(TaskInfo::isStateful)
                .map(TaskInfo::id)
                .collect(Collectors.toCollection(TreeSet::new));
            return forTasks(tasks);
        }

        /**
         * Return a new config object with the tasksToOptimize set to all stateless tasks in the given ApplicationState
         */
        public RackAwareOptimizationParams forStatelessTasks() {
            final SortedSet<TaskId> tasks = applicationState.allTasks().values()
                .stream()
                .filter(taskInfo -> !taskInfo.isStateful())
                .map(TaskInfo::id)
                .collect(Collectors.toCollection(TreeSet::new));
            return forTasks(tasks);
        }

        /**
         * Return a new config object with the provided tasksToOptimize
         */
        public RackAwareOptimizationParams forTasks(final SortedSet<TaskId> tasksToOptimize) {
            return new RackAwareOptimizationParams(
                applicationState,
                trafficCostOverride,
                nonOverlapCostOverride,
                Optional.of(tasksToOptimize)
            );
        }

        /**
         * Return a new config object with the provided trafficCost override applied
         */
        public RackAwareOptimizationParams withTrafficCostOverride(final int trafficCostOverride) {
            return new RackAwareOptimizationParams(
                applicationState,
                Optional.of(trafficCostOverride),
                nonOverlapCostOverride,
                tasksToOptimize
            );
        }

        /**
         * Return a new config object with the provided nonOverlapCost override applied
         */
        public RackAwareOptimizationParams withNonOverlapCostOverride(final int nonOverlapCostOverride) {
            return new RackAwareOptimizationParams(
                applicationState,
                trafficCostOverride,
                Optional.of(nonOverlapCostOverride),
                tasksToOptimize
            );
        }
    }

    /**
     * Validate the passed-in {@link TaskAssignment} and return an {@link AssignmentError} representing the
     * first error detected in the assignment, or {@link AssignmentError#NONE} if the assignment passes the
     * verification check.
     * <p>
     * Note: this verification is performed automatically by the StreamsPartitionAssignor on the assignment
     * returned by the TaskAssignor, and the error returned to the assignor via the {@link TaskAssignor#onAssignmentComputed}
     * callback. Therefore, it is not required to call this manually from the {@link TaskAssignor#assign} method.
     * However, if an invalid assignment is returned it will fail the rebalance and kill the thread, so it may be useful to
     * utilize this method in an assignor to verify the assignment before returning it and fix any errors it finds.
     *
     * @param applicationState The application for which this task assignment is being assessed.
     * @param taskAssignment   The task assignment that will be validated.
     *
     * @return {@code AssignmentError.NONE} if the assignment created for this application is valid,
     *         or another {@code AssignmentError} otherwise.
     */
    public static AssignmentError validateTaskAssignment(final ApplicationState applicationState,
                                                         final TaskAssignment taskAssignment) {
        final Set<TaskId> taskIdsInInput = applicationState.allTasks().keySet();
        final Collection<KafkaStreamsAssignment> assignments = taskAssignment.assignment();
        final Map<TaskId, ProcessId> activeTasksInOutput = new HashMap<>();
        final Map<TaskId, ProcessId> standbyTasksInOutput = new HashMap<>();
        for (final KafkaStreamsAssignment assignment : assignments) {
            for (final KafkaStreamsAssignment.AssignedTask task : assignment.tasks().values()) {
                if (!taskIdsInInput.contains(task.id())) {
                    LOG.error("Assignment is invalid: task {} assigned to KafkaStreams client {} was unknown",
                        task.id(), assignment.processId().id());
                    return AssignmentError.UNKNOWN_TASK_ID;
                }

                if (activeTasksInOutput.containsKey(task.id()) && task.type() == KafkaStreamsAssignment.AssignedTask.Type.ACTIVE) {
                    LOG.error("Assignment is invalid: active task {} was assigned to multiple KafkaStreams clients: {} and {}",
                        task.id(), assignment.processId().id(), activeTasksInOutput.get(task.id()).id());
                    return AssignmentError.ACTIVE_TASK_ASSIGNED_MULTIPLE_TIMES;
                }

                if (task.type() == KafkaStreamsAssignment.AssignedTask.Type.ACTIVE) {
                    activeTasksInOutput.put(task.id(), assignment.processId());
                } else {
                    standbyTasksInOutput.put(task.id(), assignment.processId());
                }
            }
        }

        for (final TaskInfo task : applicationState.allTasks().values()) {
            if (!task.isStateful() && standbyTasksInOutput.containsKey(task.id())) {
                LOG.error("Assignment is invalid: standby task for stateless task {} was assigned to KafkaStreams client {}",
                    task.id(), standbyTasksInOutput.get(task.id()).id());
                return AssignmentError.INVALID_STANDBY_TASK;
            }
        }

        final Map<ProcessId, KafkaStreamsState> clientStates = applicationState.kafkaStreamsStates(false);
        final Set<ProcessId> clientsInOutput = assignments.stream().map(KafkaStreamsAssignment::processId)
            .collect(Collectors.toSet());
        for (final Map.Entry<ProcessId, KafkaStreamsState> entry : clientStates.entrySet()) {
            final ProcessId processIdInInput = entry.getKey();
            if (!clientsInOutput.contains(processIdInInput)) {
                LOG.error("Assignment is invalid: KafkaStreams client {} has no assignment", processIdInInput.id());
                return AssignmentError.MISSING_PROCESS_ID;
            }
        }

        for (final ProcessId processIdInOutput : clientsInOutput) {
            if (!clientStates.containsKey(processIdInOutput)) {
                LOG.error("Assignment is invalid: the KafkaStreams client {} is unknown", processIdInOutput.id());
                return AssignmentError.UNKNOWN_PROCESS_ID;
            }
        }

        return AssignmentError.NONE;
    }

    /**
     * Return a "no-op" assignment that just copies the previous assignment of tasks to KafkaStreams clients
     *
     * @param applicationState the metadata and other info describing the current application state
     *
     * @return a new map containing an assignment that replicates exactly the previous assignment reported
     *         in the applicationState
     */
    public static Map<ProcessId, KafkaStreamsAssignment> identityAssignment(final ApplicationState applicationState) {
        final Map<ProcessId, KafkaStreamsAssignment> assignments = new HashMap<>();
        applicationState.kafkaStreamsStates(false).forEach((processId, state) -> {
            final Set<AssignedTask> tasks = new HashSet<>();
            state.previousActiveTasks().forEach(taskId -> {
                tasks.add(new AssignedTask(taskId,
                    AssignedTask.Type.ACTIVE));
            });
            state.previousStandbyTasks().forEach(taskId -> {
                tasks.add(new AssignedTask(taskId,
                    AssignedTask.Type.STANDBY));
            });

            final KafkaStreamsAssignment newAssignment = KafkaStreamsAssignment.of(processId, tasks);
            assignments.put(processId, newAssignment);
        });
        return assignments;
    }

    /**
     * Assign standby tasks to KafkaStreams clients according to the default logic.
     * <p>
     * If rack-aware client tags are configured, the rack-aware standby task assignor will be used
     *
     * @param applicationState        the metadata and other info describing the current application state
     * @param kafkaStreamsAssignments the KafkaStreams client assignments to add standby tasks to
     */
    public static void defaultStandbyTaskAssignment(final ApplicationState applicationState,
                                                    final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments) {
        if (!applicationState.assignmentConfigs().rackAwareAssignmentTags().isEmpty()) {
            tagBasedStandbyTaskAssignment(applicationState, kafkaStreamsAssignments);
        } else {
            loadBasedStandbyTaskAssignment(applicationState, kafkaStreamsAssignments);
        }
    }

    /**
     * Optimize active task assignment for rack awareness. This optimization is based on the
     * {@link StreamsConfig#RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG trafficCost}
     * and {@link StreamsConfig#RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG nonOverlapCost}
     * configs which balance cross rack traffic minimization and task movement.
     * Setting {@code trafficCost} to a larger number reduces the overall cross rack traffic of the resulting
     * assignment, but can increase the number of tasks shuffled around between clients.
     * Setting {@code nonOverlapCost} to a larger number increases the affinity of tasks to their intended client
     * and reduces the amount by which the rack-aware optimization can shuffle tasks around, at the cost of higher
     * cross-rack traffic.
     * In an extreme case, if we set {@code nonOverlapCost} to 0 and @{code trafficCost} to a positive value,
     * the resulting assignment will have an absolute minimum of cross rack traffic. If we set {@code trafficCost} to 0,
     * and {@code nonOverlapCost} to a positive value, the resulting assignment will be identical to the input assignment.
     * <p>
     * Note: this method will modify the input {@link KafkaStreamsAssignment} objects and return the same map.
     * It does not make a copy of the map or the KafkaStreamsAssignment objects.
     * <p>
     * This method optimizes cross-rack traffic for active tasks only. For standby task optimization,
     * use {@link #optimizeRackAwareStandbyTasks}.
     * <p>
     * It is recommended to run this optimization before assigning any standby tasks, especially if you have configured
     * your KafkaStreams clients with assignment tags via the rack.aware.assignment.tags config since this method may
     * shuffle around active tasks without considering the client tags and can result in a violation of the original
     * client tag assignment's constraints.
     *
     * @param optimizationParams      optional configuration parameters to apply
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     */
    public static void optimizeRackAwareActiveTasks(final RackAwareOptimizationParams optimizationParams,
                                                    final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments) {
        final ApplicationState applicationState = optimizationParams.applicationState;
        final SortedSet<TaskId> activeTasksToOptimize = getTasksToOptimize(kafkaStreamsAssignments, optimizationParams, AssignedTask.Type.ACTIVE);
        if (activeTasksToOptimize.isEmpty()) {
            return;
        }

        if (!canPerformRackAwareOptimization(applicationState, optimizationParams, AssignedTask.Type.ACTIVE)) {
            return;
        }

        initializeAssignmentsForAllClients(applicationState, kafkaStreamsAssignments);

        final int crossRackTrafficCost =
            optimizationParams.trafficCostOverride.orElseGet(() -> applicationState.assignmentConfigs()
                .rackAwareTrafficCost()
                .getAsInt());
        final int nonOverlapCost =
            optimizationParams.nonOverlapCostOverride.orElseGet(() -> applicationState.assignmentConfigs()
                .rackAwareNonOverlapCost()
                .getAsInt());

        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = applicationState.kafkaStreamsStates(false);
        final List<TaskId> taskIds = new ArrayList<>(activeTasksToOptimize);

        final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId = applicationState.allTasks().values().stream()
            .filter(taskInfo -> activeTasksToOptimize.contains(taskInfo.id()))
            .collect(Collectors.toMap(TaskInfo::id, TaskInfo::topicPartitions));

        final List<ProcessId> clientIds = new ArrayList<>(kafkaStreamsStates.keySet());
        final long initialCost = computeTotalAssignmentCost(
            topicPartitionsByTaskId,
            taskIds,
            clientIds,
            kafkaStreamsAssignments,
            kafkaStreamsStates,
            crossRackTrafficCost,
            nonOverlapCost,
            false,
            false
        );

        LOG.info("Assignment before active task optimization has cost {}", initialCost);

        final RackAwareGraphConstructor<KafkaStreamsAssignment> graphConstructor = RackAwareGraphConstructorFactory.create(
            applicationState.assignmentConfigs().rackAwareAssignmentStrategy(), taskIds);

        final AssignmentGraph assignmentGraph = buildTaskGraph(
            kafkaStreamsAssignments,
            kafkaStreamsStates,
            taskIds,
            clientIds,
            topicPartitionsByTaskId,
            crossRackTrafficCost,
            nonOverlapCost,
            false,
            false,
            graphConstructor
        );

        assignmentGraph.graph.solveMinCostFlow();

        graphConstructor.assignTaskFromMinCostFlow(
            assignmentGraph.graph,
            clientIds,
            taskIds,
            kafkaStreamsAssignments,
            assignmentGraph.taskCountByClient,
            assignmentGraph.clientByTask,
            (assignment, taskId) -> assignment.assignTask(new AssignedTask(taskId, AssignedTask.Type.ACTIVE)),
            (assignment, taskId) -> assignment.removeTask(new AssignedTask(taskId, AssignedTask.Type.ACTIVE)),
            (assignment, taskId) -> assignment.tasks().containsKey(taskId) && assignment.tasks().get(taskId).type() == AssignedTask.Type.ACTIVE
        );
    }

    /**
     * Optimize standby task assignment for rack awareness. This optimization is based on the
     * {@link StreamsConfig#RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG trafficCost}
     * and {@link StreamsConfig#RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG nonOverlapCost}
     * configs which balance cross rack traffic minimization and task movement.
     * Setting {@code trafficCost} to a larger number reduces the overall cross rack traffic of the resulting
     * assignment, but can increase the number of tasks shuffled around between clients.
     * Setting {@code nonOverlapCost} to a larger number increases the affinity of tasks to their intended client
     * and reduces the amount by which the rack-aware optimization can shuffle tasks around, at the cost of higher
     * cross-rack traffic.
     * In an extreme case, if we set {@code nonOverlapCost} to 0 and @{code trafficCost} to a positive value,
     * the resulting assignment will have an absolute minimum of cross rack traffic. If we set {@code trafficCost} to 0,
     * and {@code nonOverlapCost} to a positive value, the resulting assignment will be identical to the input assignment.
     * <p>
     * Note: this method will modify the input {@link KafkaStreamsAssignment} objects and return the same map.
     * It does not make a copy of the map or the KafkaStreamsAssignment objects.
     * <p>
     * This method optimizes cross-rack traffic for standby tasks only. For active task optimization,
     * use {@link #optimizeRackAwareActiveTasks}.
     *
     * @param optimizationParams      optional configuration parameters to apply
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     */
    public static void optimizeRackAwareStandbyTasks(final RackAwareOptimizationParams optimizationParams,
                                                     final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments) {
        final ApplicationState applicationState = optimizationParams.applicationState;
        final SortedSet<TaskId> standbyTasksToOptimize = getTasksToOptimize(kafkaStreamsAssignments, optimizationParams, AssignedTask.Type.STANDBY);
        if (standbyTasksToOptimize.isEmpty()) {
            return;
        }

        if (!canPerformRackAwareOptimization(applicationState, optimizationParams, AssignedTask.Type.STANDBY)) {
            return;
        }

        initializeAssignmentsForAllClients(applicationState, kafkaStreamsAssignments);

        final int crossRackTrafficCost =
            optimizationParams.trafficCostOverride.orElseGet(() -> applicationState.assignmentConfigs()
                .rackAwareTrafficCost()
                .getAsInt());
        final int nonOverlapCost =
            optimizationParams.nonOverlapCostOverride.orElseGet(() -> applicationState.assignmentConfigs()
                .rackAwareNonOverlapCost()
                .getAsInt());

        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = applicationState.kafkaStreamsStates(false);

        final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId =
            applicationState.allTasks().values().stream().collect(Collectors.toMap(
                TaskInfo::id,
                t -> t.topicPartitions().stream().filter(TaskTopicPartition::isChangelog).collect(Collectors.toSet()))
            );

        final List<ProcessId> clientIds = new ArrayList<>(kafkaStreamsStates.keySet());
        final long initialCost = computeTotalAssignmentCost(
            topicPartitionsByTaskId,
            new ArrayList<>(standbyTasksToOptimize),
            clientIds,
            kafkaStreamsAssignments,
            kafkaStreamsStates,
            crossRackTrafficCost,
            nonOverlapCost,
            true,
            true
        );
        LOG.info("Assignment before standby task optimization has cost {}", initialCost);

        final MoveStandbyTaskPredicate moveablePredicate = getStandbyTaskMovePredicate(applicationState);
        final BiFunction<KafkaStreamsAssignment, KafkaStreamsAssignment, List<TaskId>> getMovableTasks = (source, destination) -> {
            return source.tasks().values().stream()
                .filter(task -> task.type() == AssignedTask.Type.STANDBY)
                .filter(task -> !destination.tasks().containsKey(task.id()))
                .filter(task -> {
                    final KafkaStreamsState sourceState = kafkaStreamsStates.get(source.processId());
                    final KafkaStreamsState destinationState = kafkaStreamsStates.get(source.processId());
                    return moveablePredicate.canMoveStandbyTask(sourceState, destinationState, task.id(), kafkaStreamsAssignments);
                })
                .map(AssignedTask::id)
                .sorted()
                .collect(Collectors.toList());
        };

        final long startTime = System.currentTimeMillis();
        boolean taskMoved = true;
        int round = 0;
        final RackAwareGraphConstructor<KafkaStreamsAssignment> graphConstructor = RackAwareGraphConstructorFactory.create(
            applicationState.assignmentConfigs().rackAwareAssignmentStrategy(), standbyTasksToOptimize);
        while (taskMoved && round < STANDBY_OPTIMIZER_MAX_ITERATION) {
            taskMoved = false;
            round++;
            for (int i = 0; i < kafkaStreamsAssignments.size(); i++) {
                final ProcessId clientId1 = clientIds.get(i);
                final KafkaStreamsAssignment assignment1 = kafkaStreamsAssignments.get(clientId1);
                for (int j = i + 1; j < kafkaStreamsAssignments.size(); j++) {
                    final ProcessId clientId2 = clientIds.get(j);
                    final KafkaStreamsAssignment assignment2 = kafkaStreamsAssignments.get(clientId2);

                    final String rack1 = kafkaStreamsStates.get(clientId1).rackId().get();
                    final String rack2 = kafkaStreamsStates.get(clientId2).rackId().get();
                    // Cross rack traffic can not be reduced if racks are the same
                    if (rack1.equals(rack2)) {
                        continue;
                    }

                    final List<TaskId> movable1 = getMovableTasks.apply(assignment1, assignment2);
                    final List<TaskId> movable2 = getMovableTasks.apply(assignment2, assignment1);

                    // There's no needed to optimize if one is empty because the optimization
                    // can only swap tasks to keep the client's load balanced
                    if (movable1.isEmpty() || movable2.isEmpty()) {
                        continue;
                    }

                    final List<TaskId> moveableTaskIds = Stream.concat(movable1.stream(), movable2.stream())
                        .sorted()
                        .collect(Collectors.toList());
                    final List<ProcessId> clientsInTaskRedistributionAttempt = Stream.of(clientId1, clientId2)
                        .sorted()
                        .collect(Collectors.toList());

                    final AssignmentGraph assignmentGraph = buildTaskGraph(
                        kafkaStreamsAssignments,
                        kafkaStreamsStates,
                        moveableTaskIds,
                        clientsInTaskRedistributionAttempt,
                        topicPartitionsByTaskId,
                        crossRackTrafficCost,
                        nonOverlapCost,
                        true,
                        true,
                        graphConstructor
                    );
                    assignmentGraph.graph.solveMinCostFlow();

                    taskMoved |= graphConstructor.assignTaskFromMinCostFlow(
                        assignmentGraph.graph,
                        clientsInTaskRedistributionAttempt,
                        moveableTaskIds,
                        kafkaStreamsAssignments,
                        assignmentGraph.taskCountByClient,
                        assignmentGraph.clientByTask,
                        (assignment, taskId) -> assignment.assignTask(new AssignedTask(taskId, AssignedTask.Type.STANDBY)),
                        (assignment, taskId) -> assignment.removeTask(new AssignedTask(taskId, AssignedTask.Type.STANDBY)),
                        (assignment, taskId) -> assignment.tasks().containsKey(taskId) && assignment.tasks().get(taskId).type() == AssignedTask.Type.STANDBY
                    );
                }
            }
        }
        final long finalCost = computeTotalAssignmentCost(
            topicPartitionsByTaskId,
            new ArrayList<>(standbyTasksToOptimize),
            clientIds,
            kafkaStreamsAssignments,
            kafkaStreamsStates,
            crossRackTrafficCost,
            nonOverlapCost,
            true,
            true
        );

        final long duration = System.currentTimeMillis() - startTime;
        LOG.info("Assignment after {} rounds and {} milliseconds for standby task optimization is {}\n with cost {}",
            round, duration, kafkaStreamsAssignments, finalCost);
    }

    private static long computeTotalAssignmentCost(final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId,
                                                   final List<TaskId> taskIds,
                                                   final List<ProcessId> clientList,
                                                   final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                                   final Map<ProcessId, KafkaStreamsState> clientStates,
                                                   final int crossRackTrafficCost,
                                                   final int nonOverlapCost,
                                                   final boolean hasReplica,
                                                   final boolean isStandby) {
        if (taskIds.isEmpty()) {
            return 0;
        }

        final RackAwareGraphConstructor<KafkaStreamsAssignment> graphConstructor = new MinTrafficGraphConstructor<>();
        final AssignmentGraph assignmentGraph = buildTaskGraph(
            assignments,
            clientStates,
            taskIds,
            clientList,
            topicPartitionsByTaskId,
            crossRackTrafficCost,
            nonOverlapCost,
            hasReplica,
            isStandby,
            graphConstructor
        );
        return assignmentGraph.graph.totalCost();
    }

    private static AssignmentGraph buildTaskGraph(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                                  final Map<ProcessId, KafkaStreamsState> clientStates,
                                                  final List<TaskId> taskIds,
                                                  final List<ProcessId> clientList,
                                                  final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId,
                                                  final int crossRackTrafficCost,
                                                  final int nonOverlapCost,
                                                  final boolean hasReplica,
                                                  final boolean isStandby,
                                                  final RackAwareGraphConstructor<KafkaStreamsAssignment> graphConstructor) {
        // Intentionally passed in empty -- these are actually outputs of the graph
        final Map<TaskId, ProcessId> clientByTask = new HashMap<>();
        final Map<ProcessId, Integer> taskCountByClient = new HashMap<>();

        final AssignedTask.Type taskType = isStandby ? AssignedTask.Type.STANDBY : AssignedTask.Type.ACTIVE;
        final Graph<Integer> graph = graphConstructor.constructTaskGraph(
            clientList,
            taskIds,
            assignments,
            clientByTask,
            taskCountByClient,
            (assignment, taskId) -> assignment.tasks().containsKey(taskId) && assignment.tasks().get(taskId).type() == taskType,
            (taskId, processId, inCurrentAssignment, unused0, unused1, unused2) -> {
                final String clientRack = clientStates.get(processId).rackId().get();
                final int assignmentChangeCost = !inCurrentAssignment ? nonOverlapCost : 0;
                final int trafficCost = getCrossRackTrafficCost(topicPartitionsByTaskId.get(taskId), clientRack, crossRackTrafficCost);
                return assignmentChangeCost + trafficCost;
            },
            crossRackTrafficCost,
            nonOverlapCost,
            hasReplica,
            isStandby
        );
        return new AssignmentGraph(graph, clientByTask, taskCountByClient);
    }

    /**
     * This internal structure is used to keep track of the graph solving outputs alongside the graph
     * structure itself.
     */
    private static final class AssignmentGraph {
        public final Graph<Integer> graph;
        public final Map<TaskId, ProcessId> clientByTask;
        public final Map<ProcessId, Integer> taskCountByClient;

        public AssignmentGraph(final Graph<Integer> graph,
                               final Map<TaskId, ProcessId> clientByTask,
                               final Map<ProcessId, Integer> taskCountByClient) {
            this.graph = graph;
            this.clientByTask = clientByTask;
            this.taskCountByClient = taskCountByClient;
        }
    }

    @FunctionalInterface
    public interface MoveStandbyTaskPredicate {
        boolean canMoveStandbyTask(final KafkaStreamsState source,
                                   final KafkaStreamsState destination,
                                   final TaskId taskId,
                                   final Map<ProcessId, KafkaStreamsAssignment> assignments);
    }

    /**
     *
     * @return the traffic cost of assigning this {@param task} to the client {@param streamsState}.
     */
    private static int getCrossRackTrafficCost(final Set<TaskTopicPartition> topicPartitions,
                                               final String clientRack,
                                               final int crossRackTrafficCost) {
        int cost = 0;
        for (final TaskTopicPartition topicPartition : topicPartitions) {
            final Optional<Set<String>> topicPartitionRacks = topicPartition.rackIds();
            if (!topicPartitionRacks.get().contains(clientRack)) {
                cost += crossRackTrafficCost;
            }
        }
        return cost;
    }

    /**
     *
     * @return whether the rack information is valid, and the {@code StreamsConfig#RACK_AWARE_ASSIGNMENT_STRATEGY_NONE}
     *         is set.
     */
    private static boolean canPerformRackAwareOptimization(final ApplicationState applicationState,
                                                           final RackAwareOptimizationParams optimizationParams,
                                                           final AssignedTask.Type taskType) {
        final AssignmentConfigs assignmentConfigs = applicationState.assignmentConfigs();
        final String rackAwareAssignmentStrategy = assignmentConfigs.rackAwareAssignmentStrategy();
        if (StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE.equals(rackAwareAssignmentStrategy)) {
            LOG.warn("Rack aware task assignment optimization disabled: rack aware strategy was set to {}",
                rackAwareAssignmentStrategy);
            return false;
        }

        if (assignmentConfigs.rackAwareTrafficCost().isEmpty()) {
            LOG.warn("Rack aware task assignment optimization unavailable: must configure {}", StreamsConfig.RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG);
            return false;
        }

        if (assignmentConfigs.rackAwareNonOverlapCost().isEmpty()) {
            LOG.warn("Rack aware task assignment optimization unavailable: must configure {}", StreamsConfig.RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG);
            return false;
        }

        return hasValidRackInformation(applicationState, taskType);
    }

    /**
     * This function returns whether the current application state has the required rack information
     * to make assignment decisions with.
     *
     * @param taskType the type of task that we are trying to validate rack information for.
     *
     * @return whether rack-aware assignment decisions can be made for this application.
     */
    private static boolean hasValidRackInformation(final ApplicationState applicationState,
                                                   final AssignedTask.Type taskType) {
        for (final KafkaStreamsState state : applicationState.kafkaStreamsStates(false).values()) {
            if (!hasValidRackInformation(state)) {
                return false;
            }
        }

        for (final TaskInfo task : applicationState.allTasks().values()) {
            if (!hasValidRackInformation(task, taskType)) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasValidRackInformation(final KafkaStreamsState state) {
        if (state.rackId().isEmpty()) {
            LOG.error("KafkaStreams client {} doesn't have a rack id configured.", state.processId().id());
            return false;
        }
        return true;
    }

    private static boolean hasValidRackInformation(final TaskInfo task,
                                                   final AssignedTask.Type taskType) {
        final Collection<TaskTopicPartition> topicPartitions = taskType == AssignedTask.Type.STANDBY
            ? task.topicPartitions().stream().filter(TaskTopicPartition::isChangelog).collect(Collectors.toSet())
            : task.topicPartitions();

        for (final TaskTopicPartition topicPartition : topicPartitions) {
            final Optional<Set<String>> racks = topicPartition.rackIds();
            if (racks.isEmpty() || racks.get().isEmpty()) {
                LOG.error("Topic partition {} for task {} does not have racks configured.", topicPartition, task.id());
                return false;
            }
        }
        return true;
    }

    private static Map<ProcessId, KafkaStreamsAssignment> tagBasedStandbyTaskAssignment(final ApplicationState applicationState,
                                                                                        final Map<ProcessId, KafkaStreamsAssignment> assignments) {
        initializeAssignmentsForAllClients(applicationState, assignments);

        final int numStandbyReplicas = applicationState.assignmentConfigs().numStandbyReplicas();
        final Map<ProcessId, KafkaStreamsState> streamStates = applicationState.kafkaStreamsStates(false);

        final Set<String> rackAwareAssignmentTags = new HashSet<>(applicationState.assignmentConfigs().rackAwareAssignmentTags());
        final TagStatistics tagStatistics = new TagStatistics(applicationState);

        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad = standbyTaskPriorityListByLoad(streamStates, assignments);

        final Set<TaskId> statefulTaskIds = applicationState.allTasks().values().stream()
            .filter(taskInfo -> taskInfo.topicPartitions().stream().anyMatch(TaskTopicPartition::isChangelog))
            .map(TaskInfo::id)
            .collect(Collectors.toSet());
        final Map<TaskId, Integer> tasksToRemainingStandbys = statefulTaskIds.stream()
            .collect(Collectors.toMap(Function.identity(), t -> numStandbyReplicas));

        final Map<TaskId, ProcessId> pendingStandbyTasksToClientId = new HashMap<>();
        for (final TaskId statefulTaskId : statefulTaskIds) {
            for (final KafkaStreamsAssignment assignment : assignments.values()) {
                if (assignment.tasks().containsKey(statefulTaskId) && assignment.tasks().get(statefulTaskId).type() == AssignedTask.Type.ACTIVE) {
                    assignStandbyTasksToClientsWithDifferentTags(
                        numStandbyReplicas,
                        standbyTaskClientsByTaskLoad,
                        statefulTaskId,
                        assignment.processId(),
                        rackAwareAssignmentTags,
                        streamStates,
                        assignments,
                        tasksToRemainingStandbys,
                        tagStatistics.tagKeyToValues,
                        tagStatistics.tagEntryToClients,
                        pendingStandbyTasksToClientId
                    );
                }
            }
        }

        if (!tasksToRemainingStandbys.isEmpty()) {
            assignPendingStandbyTasksToLeastLoadedClients(
                assignments,
                numStandbyReplicas,
                standbyTaskClientsByTaskLoad,
                tasksToRemainingStandbys);
        }

        return assignments;
    }

    private static Map<ProcessId, KafkaStreamsAssignment> loadBasedStandbyTaskAssignment(final ApplicationState applicationState,
                                                                                         final Map<ProcessId, KafkaStreamsAssignment> assignments) {
        initializeAssignmentsForAllClients(applicationState, assignments);

        final int numStandbyReplicas = applicationState.assignmentConfigs().numStandbyReplicas();
        final Map<ProcessId, KafkaStreamsState> streamStates = applicationState.kafkaStreamsStates(false);

        final Set<TaskId> statefulTaskIds = applicationState.allTasks().values().stream()
            .filter(taskInfo -> taskInfo.topicPartitions().stream().anyMatch(TaskTopicPartition::isChangelog))
            .map(TaskInfo::id)
            .collect(Collectors.toSet());
        final Map<TaskId, Integer> tasksToRemainingStandbys = statefulTaskIds.stream()
            .collect(Collectors.toMap(Function.identity(), t -> numStandbyReplicas));

        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad = standbyTaskPriorityListByLoad(streamStates, assignments);
        standbyTaskClientsByTaskLoad.offerAll(streamStates.keySet());
        for (final TaskId task : statefulTaskIds) {
            assignStandbyTasksForActiveTask(
                numStandbyReplicas,
                assignments,
                tasksToRemainingStandbys,
                standbyTaskClientsByTaskLoad,
                task
            );
        }
        return assignments;
    }

    private static void assignStandbyTasksForActiveTask(final int numStandbyReplicas,
                                                        final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                                        final Map<TaskId, Integer> tasksToRemainingStandbys,
                                                        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad,
                                                        final TaskId activeTaskId) {
        int numRemainingStandbys = tasksToRemainingStandbys.get(activeTaskId);
        while (numRemainingStandbys > 0) {
            final ProcessId client = standbyTaskClientsByTaskLoad.poll(activeTaskId);
            if (client == null) {
                break;
            }
            assignments.get(client).assignTask(new AssignedTask(activeTaskId, AssignedTask.Type.STANDBY));
            numRemainingStandbys--;
            standbyTaskClientsByTaskLoad.offer(client);
        }

        tasksToRemainingStandbys.put(activeTaskId, numRemainingStandbys);
        if (numRemainingStandbys > 0) {
            LOG.warn("Unable to assign {} of {} standby tasks for task [{}]. " +
                     "There is not enough available capacity. You should " +
                     "increase the number of application instances " +
                     "to maintain the requested number of standby replicas.",
                numRemainingStandbys, numStandbyReplicas, activeTaskId);
        }
    }

    private static void assignStandbyTasksToClientsWithDifferentTags(final int numberOfStandbyClients,
                                                                     final ConstrainedPrioritySet standbyTaskClientsByTaskLoad,
                                                                     final TaskId activeTaskId,
                                                                     final ProcessId activeClient,
                                                                     final Set<String> rackAwareAssignmentTags,
                                                                     final Map<ProcessId, KafkaStreamsState> clientStates,
                                                                     final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                                                     final Map<TaskId, Integer> tasksToRemainingStandbys,
                                                                     final Map<String, Set<String>> tagKeyToValues,
                                                                     final Map<KeyValue<String, String>, Set<ProcessId>> tagEntryToClients,
                                                                     final Map<TaskId, ProcessId> pendingStandbyTasksToClientId) {
        standbyTaskClientsByTaskLoad.offerAll(clientStates.keySet());

        // We set countOfUsedClients as 1 because client where active task is located has to be considered as used.
        int countOfUsedClients = 1;
        int numRemainingStandbys = tasksToRemainingStandbys.get(activeTaskId);

        final Map<KeyValue<String, String>, Set<ProcessId>> tagEntryToUsedClients = new HashMap<>();

        ProcessId lastUsedClient = activeClient;
        do {
            updateClientsOnAlreadyUsedTagEntries(
                clientStates.get(lastUsedClient),
                countOfUsedClients,
                rackAwareAssignmentTags,
                tagEntryToClients,
                tagKeyToValues,
                tagEntryToUsedClients
            );

            final ProcessId clientOnUnusedTagDimensions = standbyTaskClientsByTaskLoad.poll(
                activeTaskId, processId -> !isClientUsedOnAnyOfTheTagEntries(processId, tagEntryToUsedClients)
            );

            if (clientOnUnusedTagDimensions == null) {
                break;
            }

            final KafkaStreamsState clientStateOnUsedTagDimensions = clientStates.get(clientOnUnusedTagDimensions);
            countOfUsedClients++;
            numRemainingStandbys--;

            LOG.debug("Assigning {} out of {} standby tasks for an active task [{}] with client tags {}. " +
                      "Standby task client tags are {}.",
                numberOfStandbyClients - numRemainingStandbys, numberOfStandbyClients, activeTaskId,
                clientStates.get(activeClient).clientTags(),
                clientStateOnUsedTagDimensions.clientTags());

            assignments.get(clientStateOnUsedTagDimensions.processId()).assignTask(
                new AssignedTask(activeTaskId, AssignedTask.Type.STANDBY)
            );
            lastUsedClient = clientOnUnusedTagDimensions;
        } while (numRemainingStandbys > 0);

        if (numRemainingStandbys > 0) {
            pendingStandbyTasksToClientId.put(activeTaskId, activeClient);
            tasksToRemainingStandbys.put(activeTaskId, numRemainingStandbys);
            LOG.warn("Rack aware standby task assignment was not able to assign {} of {} standby tasks for the " +
                     "active task [{}] with the rack aware assignment tags {}. " +
                     "This may happen when there aren't enough application instances on different tag " +
                     "dimensions compared to an active and corresponding standby task. " +
                     "Consider launching application instances on different tag dimensions than [{}]. " +
                     "Standby task assignment will fall back to assigning standby tasks to the least loaded clients.",
                numRemainingStandbys, numberOfStandbyClients,
                activeTaskId, rackAwareAssignmentTags,
                clientStates.get(activeClient).clientTags());

        } else {
            tasksToRemainingStandbys.remove(activeTaskId);
        }
    }

    private static boolean isClientUsedOnAnyOfTheTagEntries(final ProcessId client,
                                                            final Map<KeyValue<String, String>, Set<ProcessId>> tagEntryToUsedClients) {
        return tagEntryToUsedClients.values().stream().anyMatch(usedClients -> usedClients.contains(client));
    }

    private static void updateClientsOnAlreadyUsedTagEntries(final KafkaStreamsState usedClient,
                                                             final int countOfUsedClients,
                                                             final Set<String> rackAwareAssignmentTags,
                                                             final Map<KeyValue<String, String>, Set<ProcessId>> tagEntryToClients,
                                                             final Map<String, Set<String>> tagKeyToValues,
                                                             final Map<KeyValue<String, String>, Set<ProcessId>> tagEntryToUsedClients) {
        final Map<String, String> usedClientTags = usedClient.clientTags();

        for (final Map.Entry<String, String> usedClientTagEntry : usedClientTags.entrySet()) {
            final String tagKey = usedClientTagEntry.getKey();

            if (!rackAwareAssignmentTags.contains(tagKey)) {
                LOG.warn("Client tag with key [{}] will be ignored when computing rack aware standby " +
                         "task assignment because it is not part of the configured rack awareness [{}].",
                    tagKey, rackAwareAssignmentTags);
                continue;
            }

            final Set<String> allTagValues = tagKeyToValues.get(tagKey);

            if (allTagValues.size() <= countOfUsedClients) {
                allTagValues.forEach(tagValue -> tagEntryToUsedClients.remove(new KeyValue<>(tagKey, tagValue)));
            } else {
                final String tagValue = usedClientTagEntry.getValue();
                final KeyValue<String, String> tagEntry = new KeyValue<>(tagKey, tagValue);
                final Set<ProcessId> clientsOnUsedTagValue = tagEntryToClients.get(tagEntry);
                tagEntryToUsedClients.put(tagEntry, clientsOnUsedTagValue);
            }
        }
    }

    private static MoveStandbyTaskPredicate getStandbyTaskMovePredicate(final ApplicationState applicationState) {
        final boolean hasRackAwareAssignmentTags = !applicationState.assignmentConfigs().rackAwareAssignmentTags().isEmpty();
        if (hasRackAwareAssignmentTags) {
            final BiConsumer<KafkaStreamsState, Set<KeyValue<String, String>>> addTags = (cs, tagSet) -> {
                final Map<String, String> tags = cs.clientTags();
                if (tags != null) {
                    tagSet.addAll(tags.entrySet().stream()
                        .map(entry -> KeyValue.pair(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList())
                    );
                }
            };

            final Map<ProcessId, KafkaStreamsState> clients = applicationState.kafkaStreamsStates(false);

            return (source, destination, sourceTask, kafkaStreamsAssignments) -> {
                final Set<KeyValue<String, String>> tagsWithSource = new HashSet<>();
                final Set<KeyValue<String, String>> tagsWithDestination = new HashSet<>();
                for (final KafkaStreamsAssignment assignment: kafkaStreamsAssignments.values()) {
                    final boolean hasAssignedTask = assignment.tasks().containsKey(sourceTask);
                    final boolean isSourceProcess = assignment.processId().equals(source.processId());
                    final boolean isDestinationProcess = assignment.processId().equals(destination.processId());
                    if (hasAssignedTask && !isSourceProcess && !isDestinationProcess) {
                        final KafkaStreamsState clientState = clients.get(assignment.processId());
                        addTags.accept(clientState, tagsWithSource);
                        addTags.accept(clientState, tagsWithDestination);
                    }
                }
                addTags.accept(source, tagsWithSource);
                addTags.accept(destination, tagsWithDestination);
                return tagsWithDestination.size() >= tagsWithSource.size();
            };
        } else {
            return (a, b, c, d) -> true;
        }
    }

    private static ConstrainedPrioritySet standbyTaskPriorityListByLoad(final Map<ProcessId, KafkaStreamsState> clientStates,
                                                                        final Map<ProcessId, KafkaStreamsAssignment> assignments) {
        return new ConstrainedPrioritySet(
            (processId, taskId) -> !assignments.get(processId).tasks().containsKey(taskId),
            processId -> {
                final double capacity = clientStates.get(processId).numProcessingThreads();
                final double numTasks = assignments.get(processId).tasks().size();
                return numTasks / capacity;
            }
        );
    }

    private static void assignPendingStandbyTasksToLeastLoadedClients(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                                                      final int numStandbyReplicas,
                                                                      final ConstrainedPrioritySet standbyTaskClientsByTaskLoad,
                                                                      final Map<TaskId, Integer> pendingStandbyTaskToNumberRemainingStandbys) {
        // We need to re offer all the clients to find the least loaded ones
        standbyTaskClientsByTaskLoad.offerAll(assignments.keySet());

        for (final Map.Entry<TaskId, Integer> pendingStandbyTaskAssignmentEntry : pendingStandbyTaskToNumberRemainingStandbys.entrySet()) {
            final TaskId activeTaskId = pendingStandbyTaskAssignmentEntry.getKey();

            assignStandbyTasksForActiveTask(
                numStandbyReplicas,
                assignments,
                pendingStandbyTaskToNumberRemainingStandbys,
                standbyTaskClientsByTaskLoad,
                activeTaskId
            );
        }
    }

    private static void initializeAssignmentsForAllClients(final ApplicationState applicationState,
                                                           final Map<ProcessId, KafkaStreamsAssignment> assignments) {
        for (final ProcessId processId : applicationState.kafkaStreamsStates(false).keySet()) {
            if (!assignments.containsKey(processId)) {
                assignments.put(processId, KafkaStreamsAssignment.of(processId, new HashSet<>()));
            }
        }
    }

    private static SortedSet<TaskId> getTasksToOptimize(final Map<ProcessId, KafkaStreamsAssignment> assignments,
                                                        final RackAwareOptimizationParams optimizationParams,
                                                        final AssignedTask.Type taskType) {
        if (optimizationParams != null && optimizationParams.tasksToOptimize.isPresent()) {
            return optimizationParams.tasksToOptimize.get();
        }

        return assignments.values().stream()
            .flatMap(r -> r.tasks().values().stream())
            .filter(task -> task.type() == taskType)
            .map(AssignedTask::id)
            .collect(Collectors.toCollection(TreeSet::new));
    }

    private static class TagStatistics {
        private final Map<String, Set<String>> tagKeyToValues;
        private final Map<KeyValue<String, String>, Set<ProcessId>> tagEntryToClients;

        public TagStatistics(final ApplicationState applicationState) {
            final Map<ProcessId, KafkaStreamsState> clientStates = applicationState.kafkaStreamsStates(false);

            final Map<String, Set<String>> tagKeyToValues = new HashMap<>();
            final Map<KeyValue<String, String>, Set<ProcessId>> tagEntryToClients = new HashMap<>();
            for (final KafkaStreamsState state : clientStates.values()) {
                state.clientTags().forEach((tagKey, tagValue) -> {
                    tagKeyToValues.computeIfAbsent(tagKey, ignored -> new HashSet<>()).add(tagValue);
                    tagEntryToClients.computeIfAbsent(new KeyValue<>(tagKey, tagValue), ignored -> new HashSet<>()).add(state.processId());
                });
            }

            this.tagKeyToValues = tagKeyToValues;
            this.tagEntryToClients = tagEntryToClients;
        }
    }
}
