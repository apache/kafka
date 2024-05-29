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

        if (!hasValidRackInformation(applicationState)) {
            LOG.warn("Cannot optimize active tasks with invalid rack information.");
            return kafkaStreamsAssignments;
        }

        final int crossRackTrafficCost = applicationState.assignmentConfigs().rackAwareTrafficCost();
        final int nonOverlapCost = applicationState.assignmentConfigs().rackAwareNonOverlapCost();

        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = applicationState.kafkaStreamsStates(false);
        final List<TaskId> taskIds = new ArrayList<>(tasks);

        // TODO: can be simplified once we change #allTasks to return a Map<TaskId, TaskInfo>
        //  then we can change the tasks input parameter to a List and flip the .filter step
        final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId = applicationState.allTasks().stream()
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

        final long initialCost = computeTaskCost(
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
    private static Map<ProcessId, KafkaStreamsAssignment> optimizeRackAwareStandbyTasks(final ApplicationState applicationState,
                                                                                       final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments) {
        if (!hasValidRackInformation(applicationState)) {
            LOG.warn("Cannot optimize standby tasks with invalid rack information.");
            return kafkaStreamsAssignments;
        }

        final int crossRackTrafficCost = applicationState.assignmentConfigs().rackAwareTrafficCost();
        final int nonOverlapCost = applicationState.assignmentConfigs().rackAwareNonOverlapCost();

        // TODO: can be simplified once we change #allTasks to return a Map<TaskId, TaskInfo>
        final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId =
            applicationState.allTasks().stream().collect(Collectors.toMap(
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

        final long currentCost = computeTaskCost(
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
        LOG.info("Assignment before standby task optimization has cost {}", currentCost);

        throw new UnsupportedOperationException("Not yet Implemented.");
    }

    private static long computeTaskCost(final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId,
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
                final int assignmentChangeCost = !inCurrentAssignment ? nonOverlapCost : 0;
                final Optional<String> clientRack = clientRacks.get(processId);
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
                                               final Optional<String> clientRack,
                                               final int crossRackTrafficCost) {
        if (!clientRack.isPresent()) {
            throw new IllegalStateException("Client doesn't have rack configured.");
        }

        int cost = 0;
        for (final TaskTopicPartition topicPartition : topicPartitions) {
            final Optional<Set<String>> topicPartitionRacks = topicPartition.rackIds();
            if (topicPartitionRacks == null || !topicPartitionRacks.isPresent()) {
                throw new IllegalStateException("TopicPartition " + topicPartition + " has no rack information.");
            }

            if (topicPartitionRacks.get().contains(clientRack.get())) {
                continue;
            }

            cost += crossRackTrafficCost;
        }
        return cost;
    }

    /**
     * This function returns whether the current application state has the required rack information
     * to make assignment decisions with.
     * This includes ensuring that every client has a known rack id, and that every topic partition for
     * every logical task that needs to be assigned also has known rack ids.
     * If a logical task has no source topic partitions, it will return false.
     * If standby tasks are configured, but a logical task has no changelog topic partitions, it will return false.
     *
     * @return whether rack-aware assignment decisions can be made for this application.
     */
    private static boolean hasValidRackInformation(final ApplicationState applicationState) {
        for (final KafkaStreamsState state : applicationState.kafkaStreamsStates(false).values()) {
            if (!hasValidRackInformation(state)) {
                return false;
            }
        }

        for (final TaskInfo task : applicationState.allTasks()) {
            if (!hasValidRackInformation(task)) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasValidRackInformation(final KafkaStreamsState state) {
        if (!state.rackId().isPresent()) {
            LOG.error("Client " + state.processId() + " doesn't have rack configured.");
            return false;
        }
        return true;
    }

    private static boolean hasValidRackInformation(final TaskInfo task) {
        for (final TaskTopicPartition topicPartition : task.topicPartitions()) {
            final Optional<Set<String>> racks = topicPartition.rackIds();
            if (!racks.isPresent() || racks.get().isEmpty()) {
                LOG.error("Topic partition {} for task {} does not have racks configured.", topicPartition, task.id());
                return false;
            }
        }
        return true;
    }
}