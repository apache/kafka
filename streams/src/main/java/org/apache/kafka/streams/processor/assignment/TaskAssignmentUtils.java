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

import static org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor.STANDBY_OPTIMIZER_MAX_ITERATION;

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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import org.apache.kafka.streams.processor.internals.assignment.ConstrainedPrioritySet;
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
     * Assign standby tasks to KafkaStreams clients according to the default logic.
     * <p>
     * If rack-aware client tags are configured, the rack-aware standby task assignor will be used
     *
     * @param applicationState        the metadata and other info describing the current application state
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     *
     * @return a new map containing the mappings from KafkaStreamsAssignments updated with the default standby assignment
     */
    public static Map<ProcessId, KafkaStreamsAssignment> defaultStandbyTaskAssignment(final ApplicationState applicationState,
                                                                                      final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments) {
        if (!applicationState.assignmentConfigs().rackAwareAssignmentTags().isEmpty()) {
            return tagBasedStandbyTaskAssignment(applicationState, kafkaStreamsAssignments);
        } else if (canPerformRackAwareOptimization(applicationState, AssignedTask.Type.STANDBY)) {
            return tagBasedStandbyTaskAssignment(applicationState, kafkaStreamsAssignments);
        } else {
            return loadBasedStandbyTaskAssignment(applicationState, kafkaStreamsAssignments);
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
     *
     * @param applicationState        the metadata and other info describing the current application state
     * @param kafkaStreamsAssignments the current assignment of tasks to KafkaStreams clients
     * @param tasks                   the set of tasks to reassign if possible. Must already be assigned to a KafkaStreams client
     *
     * @return a map with the KafkaStreamsAssignments updated to minimize cross-rack traffic for active tasks
     */
    public static Map<ProcessId, KafkaStreamsAssignment> optimizeRackAwareActiveTasks(final ApplicationState applicationState,
                                                                                      final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments,
                                                                                      final SortedSet<TaskId> tasks) {
        if (tasks.isEmpty()) {
            return kafkaStreamsAssignments;
        }

        if (!canPerformRackAwareOptimization(applicationState, AssignedTask.Type.ACTIVE)) {
            return kafkaStreamsAssignments;
        }

        final int crossRackTrafficCost = applicationState.assignmentConfigs().rackAwareTrafficCost().get();
        final int nonOverlapCost = applicationState.assignmentConfigs().rackAwareNonOverlapCost().get();

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

        final long initialCost = computeTotalAssignmentCost(
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
            (assignment, taskId) -> assignment.tasks().containsKey(taskId)
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

        final int crossRackTrafficCost = applicationState.assignmentConfigs().rackAwareTrafficCost().get();
        final int nonOverlapCost = applicationState.assignmentConfigs().rackAwareNonOverlapCost().get();

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

        final long initialCost = computeTotalAssignmentCost(
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
            applicationState.assignmentConfigs().rackAwareAssignmentStrategy(), taskIds);
        while (taskMoved && round < STANDBY_OPTIMIZER_MAX_ITERATION) {
            taskMoved = false;
            round++;
            for (int i = 0; i < kafkaStreamsAssignments.size(); i++) {
                final UUID clientId1 = clientIds.get(i);
                final KafkaStreamsAssignment clientState1 = kafkaStreamsAssignments.get(new ProcessId(clientId1));
                for (int j = i + 1; j < kafkaStreamsAssignments.size(); j++) {
                    final UUID clientId2 = clientIds.get(i);
                    final KafkaStreamsAssignment clientState2 = kafkaStreamsAssignments.get(new ProcessId(clientId2));

                    final String rack1 = clientRacks.get(clientState1.processId().id()).get();
                    final String rack2 = clientRacks.get(clientState2.processId().id()).get();
                    // Cross rack traffic can not be reduced if racks are the same
                    if (rack1.equals(rack2)) {
                        continue;
                    }

                    final List<TaskId> movable1 = getMovableTasks.apply(clientState1, clientState2);
                    final List<TaskId> movable2 = getMovableTasks.apply(clientState2, clientState1);

                    // There's no needed to optimize if one is empty because the optimization
                    // can only swap tasks to keep the client's load balanced
                    if (movable1.isEmpty() || movable2.isEmpty()) {
                        continue;
                    }

                    final List<TaskId> taskIdList = Stream.concat(movable1.stream(), movable2.stream())
                        .sorted()
                        .collect(Collectors.toList());
                    final List<UUID> clients = Stream.of(clientId1, clientId2).sorted().collect(Collectors.toList());

                    final AssignmentGraph assignmentGraph = buildTaskGraph(
                        assignmentsByUuid,
                        clientRacks,
                        taskIdList,
                        clients,
                        topicPartitionsByTaskId,
                        crossRackTrafficCost,
                        nonOverlapCost,
                        false,
                        false,
                        graphConstructor
                    );
                    assignmentGraph.graph.solveMinCostFlow();

                    taskMoved |= graphConstructor.assignTaskFromMinCostFlow(
                        assignmentGraph.graph,
                        clientIds,
                        taskIds,
                        assignmentsByUuid,
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
            taskIds,
            clientIds,
            assignmentsByUuid,
            clientRacks,
            crossRackTrafficCost,
            nonOverlapCost,
            true,
            true
        );

        final long duration = System.currentTimeMillis() - startTime;
        LOG.info("Assignment after {} rounds and {} milliseconds for standby task optimization is {}\n with cost {}",
            round, duration, kafkaStreamsAssignments, finalCost);
        return kafkaStreamsAssignments;
    }

    private static long computeTotalAssignmentCost(final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsByTaskId,
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
            (assignment, taskId) -> assignment.tasks().containsKey(taskId),
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

    @FunctionalInterface
    public interface MoveStandbyTaskPredicate {
        boolean canMoveStandbyTask(final KafkaStreamsState source,
                                   final KafkaStreamsState destination,
                                   final TaskId taskId,
                                   final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignment);
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
        final AssignmentConfigs assignmentConfigs = applicationState.assignmentConfigs();
        final String rackAwareAssignmentStrategy = assignmentConfigs.rackAwareAssignmentStrategy();
        if (StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE.equals(rackAwareAssignmentStrategy)) {
            LOG.warn("Rack aware task assignment optimization disabled: rack aware strategy was set to {}",
                rackAwareAssignmentStrategy);
            return false;
        }

        if (!assignmentConfigs.rackAwareTrafficCost().isPresent()) {
            LOG.warn("Rack aware task assignment optimization unavailable: the traffic cost configuration was not set.");
            return false;
        }

        if (!assignmentConfigs.rackAwareNonOverlapCost().isPresent()) {
            LOG.warn("Rack aware task assignment optimization unavailable: the non-overlap cost configuration was not set.");
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

    private static Map<ProcessId, KafkaStreamsAssignment> tagBasedStandbyTaskAssignment(final ApplicationState applicationState,
                                                                                        final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments) {
        final int numStandbyReplicas = applicationState.assignmentConfigs().numStandbyReplicas();
        final Map<ProcessId, KafkaStreamsState> streamStates = applicationState.kafkaStreamsStates(false);

        final Set<String> rackAwareAssignmentTags = new HashSet<>(applicationState.assignmentConfigs().rackAwareAssignmentTags());
        final TagStatistics tagStatistics = new TagStatistics(applicationState);

        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad = standbyTaskPriorityListByLoad(streamStates, kafkaStreamsAssignments);

        final Set<TaskId> statefulTaskIds = applicationState.allTasks().values().stream()
            .filter(TaskInfo::isStateful)
            .map(TaskInfo::id)
            .collect(Collectors.toSet());
        final Map<TaskId, Integer> tasksToRemainingStandbys = statefulTaskIds.stream()
            .collect(Collectors.toMap(Function.identity(), t -> numStandbyReplicas));
        final Map<UUID, KafkaStreamsAssignment> clientsByUuid = kafkaStreamsAssignments.entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().id(),
            Map.Entry::getValue
        ));

        final Map<TaskId, ProcessId> pendingStandbyTasksToClientId = new HashMap<>();
        for (final TaskId statefulTaskId : statefulTaskIds) {
            for (final KafkaStreamsAssignment assignment : clientsByUuid.values()) {
                if (assignment.tasks().containsKey(statefulTaskId)) {
                    assignStandbyTasksToClientsWithDifferentTags(
                        numStandbyReplicas,
                        standbyTaskClientsByTaskLoad,
                        statefulTaskId,
                        assignment.processId(),
                        rackAwareAssignmentTags,
                        streamStates,
                        kafkaStreamsAssignments,
                        tasksToRemainingStandbys,
                        tagStatistics.tagKeyToValues,
                        tagStatistics.tagEntryToClients,
                        pendingStandbyTasksToClientId
                    );
                }
            }
        }

        if (!tasksToRemainingStandbys.isEmpty()) {
            assignPendingStandbyTasksToLeastLoadedClients(clientsByUuid,
                numStandbyReplicas,
                standbyTaskClientsByTaskLoad,
                tasksToRemainingStandbys);
        }

        return kafkaStreamsAssignments;
    }

    private static Map<ProcessId, KafkaStreamsAssignment> loadBasedStandbyTaskAssignment(final ApplicationState applicationState,
                                                                                         final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments) {
        final int numStandbyReplicas = applicationState.assignmentConfigs().numStandbyReplicas();
        final Map<ProcessId, KafkaStreamsState> streamStates = applicationState.kafkaStreamsStates(false);

        final Set<TaskId> statefulTaskIds = applicationState.allTasks().values().stream()
            .filter(TaskInfo::isStateful)
            .map(TaskInfo::id)
            .collect(Collectors.toSet());
        final Map<TaskId, Integer> tasksToRemainingStandbys = statefulTaskIds.stream()
            .collect(Collectors.toMap(Function.identity(), t -> numStandbyReplicas));
        final Map<UUID, KafkaStreamsAssignment> clients = kafkaStreamsAssignments.entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().id(),
            Map.Entry::getValue
        ));

        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad = standbyTaskPriorityListByLoad(streamStates, kafkaStreamsAssignments);
        standbyTaskClientsByTaskLoad.offerAll(streamStates.keySet().stream().map(ProcessId::id).collect(Collectors.toSet()));
        for (final TaskId task : statefulTaskIds) {
            assignStandbyTasksForActiveTask(
                numStandbyReplicas,
                clients,
                tasksToRemainingStandbys,
                standbyTaskClientsByTaskLoad,
                task
            );
        }
        return kafkaStreamsAssignments;
    }

    private static void assignStandbyTasksForActiveTask(final int numStandbyReplicas,
                                                        final Map<UUID, KafkaStreamsAssignment> clients,
                                                        final Map<TaskId, Integer> tasksToRemainingStandbys,
                                                        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad,
                                                        final TaskId activeTaskId) {
        int numRemainingStandbys = tasksToRemainingStandbys.get(activeTaskId);
        while (numRemainingStandbys > 0) {
            final UUID client = standbyTaskClientsByTaskLoad.poll(activeTaskId);
            if (client == null) {
                break;
            }
            clients.get(client).assignTask(new AssignedTask(activeTaskId, AssignedTask.Type.STANDBY));
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
                                                                     final ProcessId activeTaskClient,
                                                                     final Set<String> rackAwareAssignmentTags,
                                                                     final Map<ProcessId, KafkaStreamsState> clientStates,
                                                                     final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments,
                                                                     final Map<TaskId, Integer> tasksToRemainingStandbys,
                                                                     final Map<String, Set<String>> tagKeyToValues,
                                                                     final Map<KeyValue<String, String>, Set<ProcessId>> tagEntryToClients,
                                                                     final Map<TaskId, ProcessId> pendingStandbyTasksToClientId) {
        standbyTaskClientsByTaskLoad.offerAll(clientStates.keySet().stream()
            .map(ProcessId::id).collect(Collectors.toSet()));

        // We set countOfUsedClients as 1 because client where active task is located has to be considered as used.
        int countOfUsedClients = 1;
        int numRemainingStandbys = tasksToRemainingStandbys.get(activeTaskId);

        final Map<KeyValue<String, String>, Set<ProcessId>> tagEntryToUsedClients = new HashMap<>();

        ProcessId lastUsedClient = activeTaskClient;
        do {
            updateClientsOnAlreadyUsedTagEntries(
                clientStates.get(lastUsedClient),
                countOfUsedClients,
                rackAwareAssignmentTags,
                tagEntryToClients,
                tagKeyToValues,
                tagEntryToUsedClients
            );

            final UUID clientOnUnusedTagDimensions = standbyTaskClientsByTaskLoad.poll(
                activeTaskId, uuid -> !isClientUsedOnAnyOfTheTagEntries(new ProcessId(uuid), tagEntryToUsedClients)
            );

            if (clientOnUnusedTagDimensions == null) {
                break;
            }

            final KafkaStreamsState clientStateOnUsedTagDimensions = clientStates.get(new ProcessId(clientOnUnusedTagDimensions));
            countOfUsedClients++;
            numRemainingStandbys--;

            LOG.debug("Assigning {} out of {} standby tasks for an active task [{}] with client tags {}. " +
                      "Standby task client tags are {}.",
                numberOfStandbyClients - numRemainingStandbys, numberOfStandbyClients, activeTaskId,
                clientStates.get(activeTaskClient).clientTags(),
                clientStateOnUsedTagDimensions.clientTags());

            kafkaStreamsAssignments.get(clientStateOnUsedTagDimensions.processId()).assignTask(
                new AssignedTask(activeTaskId, AssignedTask.Type.STANDBY)
            );
            lastUsedClient = new ProcessId(clientOnUnusedTagDimensions);
        } while (numRemainingStandbys > 0);

        if (numRemainingStandbys > 0) {
            pendingStandbyTasksToClientId.put(activeTaskId, activeTaskClient);
            tasksToRemainingStandbys.put(activeTaskId, numRemainingStandbys);
            LOG.warn("Rack aware standby task assignment was not able to assign {} of {} standby tasks for the " +
                     "active task [{}] with the rack aware assignment tags {}. " +
                     "This may happen when there aren't enough application instances on different tag " +
                     "dimensions compared to an active and corresponding standby task. " +
                     "Consider launching application instances on different tag dimensions than [{}]. " +
                     "Standby task assignment will fall back to assigning standby tasks to the least loaded clients.",
                numRemainingStandbys, numberOfStandbyClients,
                activeTaskId, rackAwareAssignmentTags,
                clientStates.get(activeTaskClient).clientTags());

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

    private static ConstrainedPrioritySet standbyTaskPriorityListByLoad(final Map<ProcessId, KafkaStreamsState> streamStates,
                                                                        final Map<ProcessId, KafkaStreamsAssignment> kafkaStreamsAssignments) {
        return new ConstrainedPrioritySet(
            (processId, taskId) -> kafkaStreamsAssignments.get(new ProcessId(processId)).tasks().containsKey(taskId),
            processId -> {
                final double capacity = streamStates.get(new ProcessId(processId)).numProcessingThreads();
                final double numTasks = kafkaStreamsAssignments.get(new ProcessId(processId)).tasks().size();
                return numTasks / capacity;
            }
        );
    }

    private static void assignPendingStandbyTasksToLeastLoadedClients(final Map<UUID, KafkaStreamsAssignment> clients,
                                                                      final int numStandbyReplicas,
                                                                      final ConstrainedPrioritySet standbyTaskClientsByTaskLoad,
                                                                      final Map<TaskId, Integer> pendingStandbyTaskToNumberRemainingStandbys) {
        // We need to re offer all the clients to find the least loaded ones
        standbyTaskClientsByTaskLoad.offerAll(clients.keySet());

        for (final Map.Entry<TaskId, Integer> pendingStandbyTaskAssignmentEntry : pendingStandbyTaskToNumberRemainingStandbys.entrySet()) {
            final TaskId activeTaskId = pendingStandbyTaskAssignmentEntry.getKey();

            assignStandbyTasksForActiveTask(
                numStandbyReplicas,
                clients,
                pendingStandbyTaskToNumberRemainingStandbys,
                standbyTaskClientsByTaskLoad,
                activeTaskId
            );
        }
    }

    private static class TagStatistics {
        private final Map<String, Set<String>> tagKeyToValues;
        private final Map<KeyValue<String, String>, Set<ProcessId>> tagEntryToClients;

        private TagStatistics(final Map<String, Set<String>> tagKeyToValues,
                              final Map<KeyValue<String, String>, Set<ProcessId>> tagEntryToClients) {
            this.tagKeyToValues = tagKeyToValues;
            this.tagEntryToClients = tagEntryToClients;
        }

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