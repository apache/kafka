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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RackAwareTaskAssignor {

    @FunctionalInterface
    public interface MoveStandbyTaskPredicate {
        boolean canMove(final ClientState source,
                        final ClientState destination,
                        final TaskId taskId,
                        final Map<UUID, ClientState> clientStateMap);
    }

    @FunctionalInterface
    public interface CostFunction {
        int getCost(final TaskId taskId,
                    final UUID processId,
                    final boolean inCurrentAssignment,
                    final int trafficCost,
                    final int nonOverlapCost,
                    final boolean isStandby);
    }

    // For stateless tasks, it's ok to move them around. So we have 0 non_overlap_cost
    public static final int STATELESS_TRAFFIC_COST = 1;
    public static final int STATELESS_NON_OVERLAP_COST = 0;

    private static final Logger log = LoggerFactory.getLogger(RackAwareTaskAssignor.class);

    // This is number is picked based on testing. Usually the optimization for standby assignment
    // stops after 3 rounds
    private static final int STANDBY_OPTIMIZER_MAX_ITERATION = 4;

    private final Cluster fullMetadata;
    private final Map<TaskId, Set<TopicPartition>> partitionsForTask;
    private final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask;
    private final Map<Subtopology, Set<TaskId>> tasksForTopicGroup;
    private final AssignmentConfigs assignmentConfigs;
    private final Map<TopicPartition, Set<String>> racksForPartition;
    private final Map<UUID, String> racksForProcess;
    private final InternalTopicManager internalTopicManager;
    private final boolean validClientRack;
    private final Time time;
    private Boolean canEnable = null;

    public RackAwareTaskAssignor(final Cluster fullMetadata,
                                 final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                 final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask,
                                 final Map<Subtopology, Set<TaskId>> tasksForTopicGroup,
                                 final Map<UUID, Map<String, Optional<String>>> racksForProcessConsumer,
                                 final InternalTopicManager internalTopicManager,
                                 final AssignmentConfigs assignmentConfigs,
                                 final Time time) {
        this.fullMetadata = fullMetadata;
        this.partitionsForTask = partitionsForTask;
        this.changelogPartitionsForTask = changelogPartitionsForTask;
        this.tasksForTopicGroup = tasksForTopicGroup;
        this.internalTopicManager = internalTopicManager;
        this.assignmentConfigs = assignmentConfigs;
        this.racksForPartition = new HashMap<>();
        this.racksForProcess = new HashMap<>();
        this.time = Objects.requireNonNull(time, "Time was not specified");
        validClientRack = validateClientRack(racksForProcessConsumer);
    }

    public boolean validClientRack() {
        return validClientRack;
    }

    public synchronized boolean canEnableRackAwareAssignor() {
        if (StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE.equals(assignmentConfigs.rackAwareAssignmentStrategy)) {
            return false;
        }
        if (canEnable != null) {
            return canEnable;
        }
        canEnable = validClientRack && validateTopicPartitionRack(false);
        if (assignmentConfigs.numStandbyReplicas == 0 || !canEnable) {
            return canEnable;
        }

        canEnable = validateTopicPartitionRack(true);
        return canEnable;
    }

    // Visible for testing. This method also checks if all TopicPartitions exist in cluster
    public boolean populateTopicsToDescribe(final Set<String> topicsToDescribe, final boolean changelog) {
        if (changelog) {
            // Changelog topics are not in metadata, we need to describe them
            changelogPartitionsForTask.values().stream().flatMap(Collection::stream).forEach(tp -> topicsToDescribe.add(tp.topic()));
            return true;
        }

        // Make sure rackId exist for all TopicPartitions needed
        for (final Set<TopicPartition> topicPartitions : partitionsForTask.values()) {
            for (final TopicPartition topicPartition : topicPartitions) {
                final PartitionInfo partitionInfo = fullMetadata.partition(topicPartition);
                if (partitionInfo == null) {
                    log.error("TopicPartition {} doesn't exist in cluster", topicPartition);
                    return false;
                }
                final Node[] replica = partitionInfo.replicas();
                if (replica == null || replica.length == 0) {
                    topicsToDescribe.add(topicPartition.topic());
                    continue;
                }
                for (final Node node : replica) {
                    if (node.hasRack()) {
                        racksForPartition.computeIfAbsent(topicPartition, k -> new HashSet<>()).add(node.rack());
                    } else {
                        log.warn("Node {} for topic partition {} doesn't have rack", node, topicPartition);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private boolean validateTopicPartitionRack(final boolean changelogTopics) {
        // Make sure rackId exist for all TopicPartitions needed
        final Set<String> topicsToDescribe = new HashSet<>();
        if (!populateTopicsToDescribe(topicsToDescribe, changelogTopics)) {
            return false;
        }

        if (!topicsToDescribe.isEmpty()) {
            log.info("Fetching PartitionInfo for topics {}", topicsToDescribe);
            try {
                final Map<String, List<TopicPartitionInfo>> topicPartitionInfo = internalTopicManager.getTopicPartitionInfo(topicsToDescribe);
                if (topicsToDescribe.size() > topicPartitionInfo.size()) {
                    topicsToDescribe.removeAll(topicPartitionInfo.keySet());
                    log.error("Failed to describe topic for {}", topicsToDescribe);
                    return false;
                }
                for (final Map.Entry<String, List<TopicPartitionInfo>> entry : topicPartitionInfo.entrySet()) {
                    final List<TopicPartitionInfo> partitionInfos = entry.getValue();
                    for (final TopicPartitionInfo partitionInfo : partitionInfos) {
                        final int partition = partitionInfo.partition();
                        final List<Node> replicas = partitionInfo.replicas();
                        if (replicas == null || replicas.isEmpty()) {
                            log.error("No replicas found for topic partition {}: {}", entry.getKey(), partition);
                            return false;
                        }
                        final TopicPartition topicPartition = new TopicPartition(entry.getKey(), partition);
                        for (final Node node : replicas) {
                            if (node.hasRack()) {
                                racksForPartition.computeIfAbsent(topicPartition, k -> new HashSet<>()).add(node.rack());
                            } else {
                                return false;
                            }
                        }
                    }
                }
            } catch (final Exception e) {
                log.error("Failed to describe topics {}", topicsToDescribe, e);
                return false;
            }
        }
        return true;
    }

    private boolean validateClientRack(final Map<UUID, Map<String, Optional<String>>> racksForProcessConsumer) {
        if (racksForProcessConsumer == null) {
            return false;
        }
        /*
         * Check rack information is populated correctly in clients
         * 1. RackId exist for all clients
         * 2. Different consumerId for same process should have same rackId
         */
        for (final Map.Entry<UUID, Map<String, Optional<String>>> entry : racksForProcessConsumer.entrySet()) {
            final UUID processId = entry.getKey();
            KeyValue<String, String> previousRackInfo = null;
            for (final Map.Entry<String, Optional<String>> rackEntry : entry.getValue().entrySet()) {
                if (!rackEntry.getValue().isPresent()) {
                    if (!StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE.equals(assignmentConfigs.rackAwareAssignmentStrategy)) {
                        log.error(
                            String.format("RackId doesn't exist for process %s and consumer %s",
                                processId, rackEntry.getKey()));
                    }
                    return false;
                }
                if (previousRackInfo == null) {
                    previousRackInfo = KeyValue.pair(rackEntry.getKey(), rackEntry.getValue().get());
                } else if (!previousRackInfo.value.equals(rackEntry.getValue().get())) {
                    log.error(
                        String.format("Consumers %s and %s for same process %s has different rackId %s and %s. File a ticket for this bug",
                            previousRackInfo.key,
                            rackEntry.getKey(),
                            entry.getKey(),
                            previousRackInfo.value,
                            rackEntry.getValue().get()
                        )
                    );
                    return false;
                }
            }
            if (previousRackInfo == null) {
                if (!StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_NONE.equals(assignmentConfigs.rackAwareAssignmentStrategy)) {
                    log.error(String.format("RackId doesn't exist for process %s", processId));
                }
                return false;
            }
            racksForProcess.put(entry.getKey(), previousRackInfo.value);
        }

        return true;
    }

    public Map<UUID, String> racksForProcess() {
        return Collections.unmodifiableMap(racksForProcess);
    }

    public Map<TopicPartition, Set<String>> racksForPartition() {
        return Collections.unmodifiableMap(racksForPartition);
    }

    private int getCost(final TaskId taskId, final UUID processId, final boolean inCurrentAssignment, final int trafficCost, final int nonOverlapCost, final boolean isStandby) {
        final String clientRack = racksForProcess.get(processId);
        if (clientRack == null) {
            throw new IllegalStateException("Client " + processId + " doesn't have rack configured. Maybe forgot to call canEnableRackAwareAssignor first");
        }

        final Set<TopicPartition> topicPartitions = isStandby ? changelogPartitionsForTask.get(taskId) : partitionsForTask.get(taskId);
        if (topicPartitions == null || topicPartitions.isEmpty()) {
            throw new IllegalStateException("Task " + taskId + " has no TopicPartitions");
        }

        int cost = 0;
        for (final TopicPartition tp : topicPartitions) {
            final Set<String> tpRacks = racksForPartition.get(tp);
            if (tpRacks == null || tpRacks.isEmpty()) {
                throw new IllegalStateException("TopicPartition " + tp + " has no rack information. Maybe forgot to call canEnableRackAwareAssignor first");
            }
            if (!tpRacks.contains(clientRack)) {
                cost += trafficCost;
            }
        }

        if (!inCurrentAssignment) {
            cost += nonOverlapCost;
        }

        return cost;
    }

    /**
     * Compute the cost for the provided {@code activeTasks}. The passed in active tasks must be contained in {@code clientState}.
     */
    long activeTasksCost(final SortedSet<TaskId> activeTasks,
                         final SortedMap<UUID, ClientState> clientStates,
                         final int trafficCost,
                         final int nonOverlapCost) {
        return tasksCost(activeTasks, clientStates, trafficCost, nonOverlapCost, ClientState::hasActiveTask, false, false);
    }

    /**
     * Compute the cost for the provided {@code standbyTasks}. The passed in standby tasks must be contained in {@code clientState}.
     */
    long standByTasksCost(final SortedSet<TaskId> standbyTasks,
                          final SortedMap<UUID, ClientState> clientStates,
                          final int trafficCost,
                          final int nonOverlapCost) {
        return tasksCost(standbyTasks, clientStates, trafficCost, nonOverlapCost, ClientState::hasStandbyTask, true, true);
    }

    private long tasksCost(final SortedSet<TaskId> tasks,
                           final SortedMap<UUID, ClientState> clientStates,
                           final int trafficCost,
                           final int nonOverlapCost,
                           final BiPredicate<ClientState, TaskId> hasAssignedTask,
                           final boolean hasReplica,
                           final boolean isStandby) {
        if (tasks.isEmpty()) {
            return 0;
        }
        final List<UUID> clientList = new ArrayList<>(clientStates.keySet());
        final List<TaskId> taskIdList = new ArrayList<>(tasks);
        final Graph<Integer> graph = new MinTrafficGraphConstructor()
            .constructTaskGraph(
                clientList,
                taskIdList,
                clientStates,
                new HashMap<>(),
                new HashMap<>(),
                hasAssignedTask,
                this::getCost,
                trafficCost,
                nonOverlapCost,
                hasReplica,
                isStandby
            );
        return graph.totalCost();
    }

    /**
     * Optimize active task assignment for rack awareness. canEnableRackAwareAssignor must be called first.
     * {@code trafficCost} and {@code nonOverlapCost} balance cross rack traffic optimization and task movement.
     * If we set {@code trafficCost} to a larger number, we are more likely to compute an assignment with less
     * cross rack traffic. However, tasks may be shuffled a lot across clients. If we set {@code nonOverlapCost}
     * to a larger number, we are more likely to compute an assignment with similar to input assignment. However,
     * cross rack traffic can be higher. In extreme case, if we set {@code nonOverlapCost} to 0 and @{code trafficCost}
     * to a positive value, the computed assignment will be minimum for cross rack traffic. If we set {@code trafficCost} to 0,
     * and {@code nonOverlapCost} to a positive value, the computed assignment should be the same as input
     * @param activeTasks Tasks to reassign if needed. They must be assigned already in clientStates
     * @param clientStates Client states
     * @param trafficCost Cost of cross rack traffic for each TopicPartition
     * @param nonOverlapCost Cost of assign a task to a different client
     * @return Total cost after optimization
     */
    public long optimizeActiveTasks(final SortedSet<TaskId> activeTasks,
                                    final SortedMap<UUID, ClientState> clientStates,
                                    final int trafficCost,
                                    final int nonOverlapCost) {
        if (activeTasks.isEmpty()) {
            return 0;
        }

        log.info("Assignment before active task optimization is {}\n with cost {}", clientStates,
            activeTasksCost(activeTasks, clientStates, trafficCost, nonOverlapCost));

        final long startTime = time.milliseconds();
        final List<UUID> clientList = new ArrayList<>(clientStates.keySet());
        final List<TaskId> taskIdList = new ArrayList<>(activeTasks);
        final Map<TaskId, UUID> taskClientMap = new HashMap<>();
        final Map<UUID, Integer> originalAssignedTaskNumber = new HashMap<>();
        final RackAwareGraphConstructor graphConstructor = RackAwareGraphConstructorFactory.create(assignmentConfigs, tasksForTopicGroup);
        final Graph<Integer> graph = graphConstructor.constructTaskGraph(
            clientList,
            taskIdList,
            clientStates,
            taskClientMap,
            originalAssignedTaskNumber,
            ClientState::hasActiveTask,
            this::getCost,
            trafficCost,
            nonOverlapCost,
            false,
            false
        );

        graph.solveMinCostFlow();
        final long cost = graph.totalCost();

        graphConstructor.assignTaskFromMinCostFlow(graph, clientList, taskIdList, clientStates, originalAssignedTaskNumber,
            taskClientMap, ClientState::assignActive, ClientState::unassignActive, ClientState::hasActiveTask);

        final long duration = time.milliseconds() - startTime;
        log.info("Assignment after {} milliseconds for active task optimization is {}\n with cost {}", duration, clientStates, cost);
        return cost;
    }

    public long optimizeStandbyTasks(final SortedMap<UUID, ClientState> clientStates,
                                     final int trafficCost,
                                     final int nonOverlapCost,
                                     final MoveStandbyTaskPredicate moveStandbyTask) {
        final BiFunction<ClientState, ClientState, List<TaskId>> getMovableTasks = (source, destination) -> source.standbyTasks().stream()
            .filter(task -> !destination.hasAssignedTask(task))
            .filter(task -> moveStandbyTask.canMove(source, destination, task, clientStates))
            .sorted()
            .collect(Collectors.toList());

        final long startTime = time.milliseconds();
        final List<UUID> clientList = new ArrayList<>(clientStates.keySet());
        final SortedSet<TaskId> standbyTasks = new TreeSet<>();
        clientStates.values().forEach(clientState -> standbyTasks.addAll(clientState.standbyTasks()));

        log.info("Assignment before standby task optimization is {}\n with cost {}", clientStates,
            standByTasksCost(standbyTasks, clientStates, trafficCost, nonOverlapCost));

        boolean taskMoved = true;
        int round = 0;
        final RackAwareGraphConstructor graphConstructor = new MinTrafficGraphConstructor();
        while (taskMoved && round < STANDBY_OPTIMIZER_MAX_ITERATION) {
            taskMoved = false;
            round++;
            for (int i = 0; i < clientList.size(); i++) {
                final ClientState clientState1 = clientStates.get(clientList.get(i));
                for (int j = i + 1; j < clientList.size(); j++) {
                    final ClientState clientState2 = clientStates.get(clientList.get(j));

                    final String rack1 = racksForProcess.get(clientState1.processId());
                    final String rack2 = racksForProcess.get(clientState2.processId());
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

                    final List<TaskId> taskIdList = Stream.concat(movable1.stream(),
                            movable2.stream())
                        .sorted()
                        .collect(Collectors.toList());

                    final Map<TaskId, UUID> taskClientMap = new HashMap<>();
                    final List<UUID> clients = Stream.of(clientList.get(i), clientList.get(j))
                        .sorted().collect(
                            Collectors.toList());
                    final Map<UUID, Integer> originalAssignedTaskNumber = new HashMap<>();

                    final Graph<Integer> graph = graphConstructor.constructTaskGraph(
                        clients,
                        taskIdList,
                        clientStates,
                        taskClientMap,
                        originalAssignedTaskNumber,
                        ClientState::hasStandbyTask,
                        this::getCost,
                        trafficCost,
                        nonOverlapCost,
                        true,
                        true
                    );
                    graph.solveMinCostFlow();

                    taskMoved |= graphConstructor.assignTaskFromMinCostFlow(graph, clients, taskIdList, clientStates,
                        originalAssignedTaskNumber,
                        taskClientMap, ClientState::assignStandby, ClientState::unassignStandby,
                        ClientState::hasStandbyTask);
                }
            }
        }
        final long cost = standByTasksCost(standbyTasks, clientStates, trafficCost, nonOverlapCost);

        final long duration = time.milliseconds() - startTime;
        log.info("Assignment after {} rounds and {} milliseconds for standby task optimization is {}\n with cost {}", round, duration, clientStates, cost);
        return cost;
    }
}
