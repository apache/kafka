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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import org.apache.kafka.streams.processor.internals.assignment.Graph;
import org.apache.kafka.streams.processor.internals.assignment.MinTrafficGraphConstructor;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareGraphConstructor;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareGraphConstructorFactory;
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
    public static Map<ProcessId, KafkaStreamsAssignment> identityAssignment(
        final ApplicationState applicationState
    ) {
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
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     *
     * @return a new map containing the mappings from KafkaStreamsAssignments updated with the default
     *         standby assignment
     */
    public static Map<ProcessId, KafkaStreamsAssignment> defaultStandbyTaskAssignment(
        final ApplicationState applicationState,
        final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments
    ) {
        final long followupRebalanceDelay = applicationState.assignmentConfigs().probingRebalanceIntervalMs();
        final Instant followupRebalanceDeadline = Instant.now().plus(followupRebalanceDelay, ChronoUnit.MILLIS);
        throw new UnsupportedOperationException("Not Implemented.");
    }

    /**
     * Optimize the active task assignment for rack-awareness
     *
     * @param applicationState        the metadata and other info describing the current application state
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     * @param tasks                   the set of tasks to reassign if possible. Must already be assigned
     *                                to a KafkaStreams client
     *
     * @return a new map containing the mappings from KafkaStreamsAssignments updated with the default
     *         rack-aware assignment for active tasks
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
            throw new IllegalStateException("Cannot perform rack-aware assignment optimizations with invalid rack information.");
        }

        final int crossRackTrafficCost= applicationState.assignmentConfigs().rackAwareTrafficCost();
        final int nonOverlapCost = applicationState.assignmentConfigs().rackAwareNonOverlapCost();
        final long currentCost = computeTaskCost(
            applicationState.allTasks().stream().filter(taskInfo -> tasks.contains(taskInfo.id())).collect(
                Collectors.toSet()),
            applicationState.kafkaStreamsStates(false),
            crossRackTrafficCost,
            nonOverlapCost,
            false,
            false
        );
        LOG.info("Assignment before active task optimization has cost {}", currentCost);

        final List<UUID> clientIds = kafkaStreamsAssignments.keySet().stream().map(ProcessId::id).collect(
            Collectors.toList());
        final Map<ProcessId, KafkaStreamsState> kafkaStreamsStates = applicationState.kafkaStreamsStates(false);
        final Map<UUID, Optional<String>> clientRacks = kafkaStreamsStates.values().stream().collect(
                Collectors.toMap(state -> state.processId().id(), KafkaStreamsState::rackId));
        final Map<UUID, Set<TaskId>> previousTaskIdsByProcess = kafkaStreamsAssignments.values().stream()
            .collect(
            Collectors.toMap(
                assignment -> assignment.processId().id(),
                assignment -> {
                    return assignment.assignment().stream().map(AssignedTask::id)
                        .filter(tasks::contains)
                        .collect(Collectors.toSet());
                }
            )
        );
        final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId = applicationState.allTasks().stream()
            .filter(taskInfo -> tasks.contains(taskInfo.id()))
            .collect(Collectors.toMap(TaskInfo::id, TaskInfo::topicPartitions));

        final List<TaskId> taskIds = new ArrayList<>(tasks);
        final RackAwareGraphConstructor<UUID> graphConstructor = RackAwareGraphConstructorFactory.create(
            applicationState.assignmentConfigs().rackAwareAssignmentStrategy(), taskIds);
        final AssignmentGraph assignmentGraph = buildTaskGraph(
            clientIds,
            clientRacks,
            taskIds,
            previousTaskIdsByProcess,
            topicPartitionsByTaskId,
            crossRackTrafficCost,
            nonOverlapCost,
            false,
            false,
            graphConstructor
        );

        assignmentGraph.graph.solveMinCostFlow();

        final Map<UUID, Set<AssignedTask>> reassigned = new HashMap<>();
        final Map<UUID, Set<TaskId>> unassigned = new HashMap<>();
        final boolean taskMoved = graphConstructor.assignTaskFromMinCostFlow(
            assignmentGraph.graph,
            clientIds,
            taskIds,
            clientIds.stream().collect(Collectors.toMap(id -> id, id -> id)),
            assignmentGraph.taskCountByClient,
            assignmentGraph.clientByTask,
            (processId, taskId) -> {
                reassigned.computeIfAbsent(processId, k -> new HashSet<>());
                reassigned.get(processId).add(new AssignedTask(taskId, AssignedTask.Type.ACTIVE));
            },
            (processId, taskId) -> {
                unassigned.computeIfAbsent(processId, k -> new HashSet<>());
                unassigned.get(processId).add(taskId);
            },
            (processId, taskId) -> {
                return previousTaskIdsByProcess.get(processId).contains(taskId);
            }
        );

        return processTaskMoves(kafkaStreamsAssignments.values(), reassigned, unassigned);
    }

    /**
     * Optimize the standby task assignment for rack-awareness
     *
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     * @param applicationState        the metadata and other info describing the current application state
     *
     * @return a new map containing the mappings from KafkaStreamsAssignments updated with the default
     *         rack-aware assignment for standby tasks
     */
    public static Map<ProcessId, KafkaStreamsAssignment> optimizeRackAwareStandbyTasks(
        final ApplicationState applicationState,
        final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments
    ) {
        if (!hasValidRackInformation(applicationState)) {
            throw new IllegalStateException("Cannot perform rack-aware assignment optimizations with invalid rack information.");
        }

        final int crossRackTrafficCost= applicationState.assignmentConfigs().rackAwareTrafficCost();
        final int nonOverlapCost = applicationState.assignmentConfigs().rackAwareNonOverlapCost();
        final long currentCost = computeTaskCost(
            applicationState.allTasks(),
            applicationState.kafkaStreamsStates(false),
            crossRackTrafficCost,
            nonOverlapCost,
            true,
            true
        );
        LOG.info("Assignment before standby task optimization has cost {}", currentCost);
        throw new UnsupportedOperationException("Not Implemented.");
    }

    private static long computeTaskCost(final Set<TaskInfo> tasks,
                                        final Map<ProcessId, KafkaStreamsState> clients,
                                        final int crossRackTrafficCost,
                                        final int nonOverlapCost,
                                        final boolean hasReplica,
                                        final boolean isStandby) {
        if (tasks.isEmpty()) {
            return 0;
        }

        final List<UUID> clientIds = clients.keySet().stream().map(ProcessId::id).collect(
            Collectors.toList());

        final List<TaskId> taskIds = tasks.stream().map(TaskInfo::id).collect(Collectors.toList());
        final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId = tasks.stream().collect(
            Collectors.toMap(TaskInfo::id, TaskInfo::topicPartitions));

        final Map<UUID, Optional<String>> clientRacks = clients.values().stream().collect(
            Collectors.toMap(state -> state.processId().id(), KafkaStreamsState::rackId));

        final Map<UUID, Set<TaskId>> taskIdsByProcess = clients.values().stream().collect(
            Collectors.toMap(state -> state.processId().id(), state -> {
                if (isStandby) {
                    return state.previousStandbyTasks();
                }
                return state.previousActiveTasks();
            })
        );

        final RackAwareGraphConstructor<UUID> graphConstructor = new MinTrafficGraphConstructor<>();
        final AssignmentGraph assignmentGraph = buildTaskGraph(clientIds, clientRacks, taskIds, taskIdsByProcess, topicPartitionsByTaskId,
            crossRackTrafficCost, nonOverlapCost, hasReplica, isStandby, graphConstructor);
        return assignmentGraph.graph.totalCost();
    }

    private static AssignmentGraph buildTaskGraph(final List<UUID> clientIds,
                                                 final Map<UUID, Optional<String>> clientRacks,
                                                 final List<TaskId> taskIds,
                                                 final Map<UUID, Set<TaskId>> taskIdsByProcess,
                                                 final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId,
                                                 final int crossRackTrafficCost,
                                                 final int nonOverlapCost,
                                                 final boolean hasReplica,
                                                 final boolean isStandby,
                                                 final RackAwareGraphConstructor<UUID> graphConstructor) {
        final Map<UUID, UUID> clientsUuidByUuid = clientIds.stream().collect(Collectors.toMap(id -> id, id -> id));
        final Map<TaskId, UUID> clientByTask = new HashMap<>();
        final Map<UUID, Integer> taskCountByClient = new HashMap<>();
        final Graph<Integer> graph = graphConstructor.constructTaskGraph(
            clientIds,
            taskIds,
            clientsUuidByUuid,
            clientByTask,
            taskCountByClient,
            (processId, taskId) -> {
                return taskIdsByProcess.get(processId).contains(taskId);
            },
            (taskId, processId, inCurrentAssignment, unused0, unused1, unused2) -> {
                final int assignmentChangeCost = !inCurrentAssignment ? nonOverlapCost : 0;
                final Optional<String> clientRack = clientRacks.get(processId);
                final Set<TaskTopicPartition> topicPartitions = topicPartitionsByTaskId.get(taskId).stream().filter(tp -> {
                    return isStandby ? tp.isChangelog() : true;
                }).collect(Collectors.toSet());
                return (assignmentChangeCost + getCrossRackTrafficCost(topicPartitions, clientRack, crossRackTrafficCost));
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
    public static boolean hasValidRackInformation(final ApplicationState applicationState) {
        for (final KafkaStreamsState state : applicationState.kafkaStreamsStates(false).values()) {
            if (!hasValidRackInformation(state)) {
                return false;
            }
        }

        final boolean hasStandby = applicationState.assignmentConfigs().numStandbyReplicas() >= 1;
        for (final TaskInfo task : applicationState.allTasks()) {
            if (!hasValidRackInformation(task, hasStandby)) {
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

    private static boolean hasValidRackInformation(final TaskInfo task, final boolean hasStandby) {
        final Map<TopicPartition, Set<String>> racksByTopicPartition = task.partitionToRackIds();
        for (final TopicPartition topicPartition : task.sourceTopicPartitions()) {
            final Set<String> racks = racksByTopicPartition.getOrDefault(topicPartition, null);
            if (racks == null || racks.isEmpty()) {
                LOG.error("Topic partition {} for task {} does not have racks configured.", topicPartition, task.id());
                return false;
            }
        }

        for (final TopicPartition topicPartition : task.changelogTopicPartitions()) {
            final Set<String> racks = racksByTopicPartition.getOrDefault(topicPartition, null);
            if (racks == null || racks.isEmpty()) {
                LOG.error("Topic partition {} for task {} does not have racks configured.", topicPartition, task.id());
                return false;
            }
        }

        if (task.sourceTopicPartitions().isEmpty()) {
            LOG.error("Task {} has no source TopicPartitions.", task.id());
            return false;
        }

        if (hasStandby && task.changelogTopicPartitions().isEmpty()) {
            LOG.error("Task {} has no changelog TopicPartitions.", task.id());
            return false;
        }

        return true;
    }

    /**
     * This function returns a copy of the old collection of {@code KafkaStreamsAssignment} with tasks
     * moved according to the {@param reassigned} tasks and {@param unassigned} tasks.
     *
     * @param kafkaStreamsAssignments the collection to start from when moving tasks from process to process
     * @param reassigned              the map from process id to tasks that this client has newly been assigned
     * @param unassigned              the map from process id to tasks that this client has newly been unassigned
     *
     * @return the new mapping from processId to {@code KafkaStreamsAssignment}.
     */
    private static Map<ProcessId, KafkaStreamsAssignment> processTaskMoves(final Collection<KafkaStreamsAssignment> kafkaStreamsAssignments,
                                                                           final Map<UUID, Set<AssignedTask>> reassigned,
                                                                           final Map<UUID, Set<TaskId>> unassigned) {
        final Map<ProcessId, KafkaStreamsAssignment> optimizedResult = new HashMap<>();
        for (final KafkaStreamsAssignment oldAssignment : kafkaStreamsAssignments) {
            final ProcessId processId = oldAssignment.processId();
            final Set<AssignedTask> newTasks = reassigned.getOrDefault(processId.id(), new HashSet<>());
            final Set<TaskId> unassignedTasksForProcess = unassigned.getOrDefault(processId.id(), new HashSet<>());
            for (final AssignedTask previouslyAssignedTask : oldAssignment.assignment()) {
                if (unassignedTasksForProcess.contains(previouslyAssignedTask.id())) {
                    continue;
                }
                newTasks.add(previouslyAssignedTask);
            }

            final KafkaStreamsAssignment newAssignment = KafkaStreamsAssignment.of(processId, newTasks);
            if (oldAssignment.followupRebalanceDeadline().isPresent()) {
                optimizedResult.put(processId, newAssignment.withFollowupRebalance(
                    oldAssignment.followupRebalanceDeadline().get()));
            } else {
                optimizedResult.put(processId, newAssignment);
            }
        }
        return optimizedResult;
    }
}