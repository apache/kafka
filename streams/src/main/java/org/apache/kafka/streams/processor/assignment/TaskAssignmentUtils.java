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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import org.apache.kafka.streams.processor.internals.assignment.Graph;
import org.apache.kafka.streams.processor.internals.assignment.MinTrafficGraphConstructor;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareGraphConstructor;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareGraphConstructorFactory;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of utilities to help implement task assignment via the {@link TaskAssignor}
 */
public final class TaskAssignmentUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TaskAssignmentUtils.class);

    private TaskAssignmentUtils() {}

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
     *
     * @param applicationState        the metadata and other info describing the current application state
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     * @param tasks                   the set of tasks to reassign if possible. Must already be assigned to a KafkaStreams client
     *
     * @return a map with the KafkaStreamsAssignments updated to minimize cross-rack traffic for active tasks
     */
    public static Map<ProcessId, KafkaStreamsAssignment> optimizeRackAwareActiveTasks(
        final ApplicationState applicationState,
        final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments,
        final SortedSet<TaskId> tasks
    ) {
        if (tasks.isEmpty()) {
            return kafkaStreamsAssignments;
        }

        if (!canPerformRackAwareOptimization(applicationState, AssignedTask.Type.ACTIVE)) {
            return kafkaStreamsAssignments;
        }

        final int crossRackTrafficCost = applicationState.assignmentConfigs().rackAwareTrafficCost();
        final int nonOverlapCost = applicationState.assignmentConfigs().rackAwareNonOverlapCost();

        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = applicationState.kafkaStreamsStates(false);
        final List<TaskId> taskIds = new ArrayList<>(tasks);

        final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId = applicationState.allTasks().values().stream()
            .filter(taskInfo -> tasks.contains(taskInfo.id()))
            .collect(Collectors.toMap(TaskInfo::id, TaskInfo::topicPartitions));

        final Map<UUID, Optional<String>> clientRacks = new HashMap<>();
        final List<UUID> clientIds = new ArrayList<>();
        final Map<UUID, KafkaStreamsAssignment> assignmentsByUuid = new HashMap<>();

        for (final Map.Entry<ProcessId, KafkaStreamsAssignment> entry : kafkaStreamsAssignments.entrySet()) {
            final UUID uuid = entry.getKey().id();
            clientIds.add(uuid);
            clientRacks.put(uuid, kafkaStreamsStates.get(entry.getKey()).rackId());
        }

        final long initialCost = computeInitialCost(
            topicPartitionsByTaskId,
            taskIds,
            clientIds,
            assignmentsByUuid,
            clientRacks,
            crossRackTrafficCost,
            nonOverlapCost,
            false,
            false
        );

        LOG.info("Assignment before active task optimization has cost {}", initialCost);

        final RackAwareGraphConstructor<KafkaStreamsAssignment> graphConstructor = RackAwareGraphConstructorFactory.create(
            applicationState.assignmentConfigs().rackAwareAssignmentStrategy(), taskIds);

        final AssignmentGraph assignmentGraph = buildTaskGraph(
            assignmentsByUuid,
            clientRacks,
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
            assignmentsByUuid,
            assignmentGraph.taskCountByClient,
            assignmentGraph.clientByTask,
            (assignment, taskId) -> assignment.assignTask(new AssignedTask(taskId, AssignedTask.Type.ACTIVE)),
            (assignment, taskId) -> assignment.removeTask(new AssignedTask(taskId, AssignedTask.Type.ACTIVE)),
            (assignment, taskId) -> assignment.assignedTaskIds().contains(taskId)
        );

        return kafkaStreamsAssignments;
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
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     * @param applicationState        the metadata and other info describing the current application state
     *
     * @return a map with the KafkaStreamsAssignments updated to minimize cross-rack traffic for standby tasks
     */
    public static Map<ProcessId, KafkaStreamsAssignment> optimizeRackAwareStandbyTasks(final ApplicationState applicationState,
                                                                                       final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments) {
        if (!canPerformRackAwareOptimization(applicationState, AssignedTask.Type.STANDBY)) {
            return kafkaStreamsAssignments;
        }

        final int crossRackTrafficCost = applicationState.assignmentConfigs().rackAwareTrafficCost();
        final int nonOverlapCost = applicationState.assignmentConfigs().rackAwareNonOverlapCost();

        final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId =
            applicationState.allTasks().values().stream().collect(Collectors.toMap(
                TaskInfo::id,
                t -> t.topicPartitions().stream().filter(TaskTopicPartition::isChangelog).collect(Collectors.toSet()))
            );
        final List<TaskId> taskIds = new ArrayList<>(topicPartitionsByTaskId.keySet());

        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = applicationState.kafkaStreamsStates(false);

        final Map<UUID, Optional<String>> clientRacks = new HashMap<>();
        final List<UUID> clientIds = new ArrayList<>();
        final Map<UUID, KafkaStreamsAssignment> assignmentsByUuid = new HashMap<>();

        for (final Map.Entry<ProcessId, KafkaStreamsAssignment> entry : kafkaStreamsAssignments.entrySet()) {
            final UUID uuid = entry.getKey().id();
            clientIds.add(uuid);
            clientRacks.put(uuid, kafkaStreamsStates.get(entry.getKey()).rackId());
        }

        final long initialCost = computeInitialCost(
            topicPartitionsByTaskId,
            taskIds,
            clientIds,
            assignmentsByUuid,
            clientRacks,
            crossRackTrafficCost,
            nonOverlapCost,
            true,
            true
        );
        LOG.info("Assignment before standby task optimization has cost {}", initialCost);

        throw new UnsupportedOperationException("Not yet Implemented.");
    }

    private static long computeInitialCost(final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId,
                                           final List<TaskId> taskIds,
                                           final List<UUID> clientIds,
                                           final Map<UUID, KafkaStreamsAssignment> assignmentsByUuid,
                                           final Map<UUID, Optional<String>> clientRacks,
                                           final int crossRackTrafficCost,
                                           final int nonOverlapCost,
                                           final boolean hasReplica,
                                           final boolean isStandby) {
        if (taskIds.isEmpty()) {
            return 0;
        }

        final RackAwareGraphConstructor<KafkaStreamsAssignment> graphConstructor = new MinTrafficGraphConstructor<>();
        final AssignmentGraph assignmentGraph = buildTaskGraph(
            assignmentsByUuid,
            clientRacks,
            taskIds,
            clientIds,
            topicPartitionsByTaskId,
            crossRackTrafficCost,
            nonOverlapCost,
            hasReplica,
            isStandby,
            graphConstructor
        );
        return assignmentGraph.graph.totalCost();
    }

    private static AssignmentGraph buildTaskGraph(final Map<UUID, KafkaStreamsAssignment> assignmentsByUuid,
                                                  final Map<UUID, Optional<String>> clientRacks,
                                                  final List<TaskId> taskIds,
                                                  final List<UUID> clientList,
                                                  final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId,
                                                  final int crossRackTrafficCost,
                                                  final int nonOverlapCost,
                                                  final boolean hasReplica,
                                                  final boolean isStandby,
                                                  final RackAwareGraphConstructor<KafkaStreamsAssignment> graphConstructor) {
        // Intentionally passed in empty -- these are actually outputs of the graph
        final Map<TaskId, UUID> clientByTask = new HashMap<>();
        final Map<UUID, Integer> taskCountByClient = new HashMap<>();

        final Graph<Integer> graph = graphConstructor.constructTaskGraph(
            clientList,
            taskIds,
            assignmentsByUuid,
            clientByTask,
            taskCountByClient,
            (assignment, taskId) -> assignment.assignedTaskIds().contains(taskId),
            (taskId, processId, inCurrentAssignment, unused0, unused1, unused2) -> {
                final String clientRack = clientRacks.get(processId).get();
                final int assignmentChangeCost = !inCurrentAssignment ? nonOverlapCost : 0;
                return assignmentChangeCost + getCrossRackTrafficCost(topicPartitionsByTaskId.get(taskId), clientRack, crossRackTrafficCost);
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
        public final Map<TaskId, UUID> clientByTask;
        public final Map<UUID, Integer> taskCountByClient;

        public AssignmentGraph(final Graph<Integer> graph,
                               final Map<TaskId, UUID> clientByTask,
                               final Map<UUID, Integer> taskCountByClient) {
            this.graph = graph;
            this.clientByTask = clientByTask;
            this.taskCountByClient = taskCountByClient;
        }
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
                                                           final AssignedTask.Type taskType) {
        final String rackAwareAssignmentStrategy = applicationState.assignmentConfigs().rackAwareAssignmentStrategy();
        if (StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE.equals(rackAwareAssignmentStrategy)) {
            LOG.warn("KafkaStreams rack aware task assignment optimization was disabled in the StreamsConfig.");
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
        if (!state.rackId().isPresent()) {
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
            if (!racks.isPresent() || racks.get().isEmpty()) {
                LOG.error("Topic partition {} for task {} does not have racks configured.", topicPartition, task.id());
                return false;
            }
        }
        return true;
    }
}