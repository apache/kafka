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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.UUID;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RackAwareTaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(RackAwareTaskAssignor.class);

    private static final int SOURCE_ID = -1;

    private final Cluster fullMetadata;
    private final Map<TaskId, Set<TopicPartition>> partitionsForTask;
    private final Map<UUID, Map<String, Optional<String>>> racksForProcess;
    private final AssignmentConfigs assignmentConfigs;
    private final Map<TopicPartition, Set<String>> racksForPartition;
    private final InternalTopicManager internalTopicManager;

    public RackAwareTaskAssignor(final Cluster fullMetadata,
                                 final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                 final Map<Subtopology, Set<TaskId>> tasksForTopicGroup,
                                 final Map<UUID, Map<String, Optional<String>>> racksForProcess,
                                 final InternalTopicManager internalTopicManager,
                                 final AssignmentConfigs assignmentConfigs) {
        this.fullMetadata = fullMetadata;
        this.partitionsForTask = partitionsForTask;
        this.racksForProcess = racksForProcess;
        this.internalTopicManager = internalTopicManager;
        this.assignmentConfigs = assignmentConfigs;
        this.racksForPartition = new HashMap<>();
    }

    public synchronized boolean canEnableRackAwareAssignor() {
        /*
        TODO: enable this after we add the config
        if (StreamsConfig.RACK_AWARE_ASSSIGNMENT_STRATEGY_NONE.equals(assignmentConfigs.rackAwareAssignmentStrategy)) {
            canEnableForActive = false;
            return false;
        }
         */

        if (!validateClientRack()) {
            return false;
        }

        return validateTopicPartitionRack();
        // TODO: add changelog topic, standby task validation
    }

    // Visible for testing. This method also checks if all TopicPartitions exist in cluster
    public boolean populateTopicsToDiscribe(final Set<String> topicsToDescribe) {
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

    private boolean validateTopicPartitionRack() {
        // Make sure rackId exist for all TopicPartitions needed
        final Set<String> topicsToDescribe = new HashSet<>();
        if (!populateTopicsToDiscribe(topicsToDescribe)) {
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

    // Visible for testing
    public boolean validateClientRack() {
        /*
         * Check rack information is populated correctly in clients
         * 1. RackId exist for all clients
         * 2. Different consumerId for same process should have same rackId
         */
        for (final Map.Entry<UUID, Map<String, Optional<String>>> entry : racksForProcess.entrySet()) {
            final UUID processId = entry.getKey();
            KeyValue<String, String> previousRackInfo = null;
            for (final Map.Entry<String, Optional<String>> rackEntry : entry.getValue().entrySet()) {
                if (!rackEntry.getValue().isPresent()) {
                    log.warn("RackId doesn't exist for process {} and consumer {}. Disable {}",
                        processId, rackEntry.getKey(), getClass().getName());
                    return false;
                }
                if (previousRackInfo == null) {
                    previousRackInfo = KeyValue.pair(rackEntry.getKey(), rackEntry.getValue().get());
                } else if (!previousRackInfo.value.equals(rackEntry.getValue().get())) {
                    log.error(
                        "Consumers {} and {} for same process {} has different rackId {} and {}. File a ticket for this bug. Disable {}",
                        previousRackInfo.key,
                        rackEntry.getKey(),
                        entry.getKey(),
                        previousRackInfo.value,
                        rackEntry.getValue().get(),
                        getClass().getName());
                    return false;
                }
            }
        }
        return true;
    }

    private int getCost(final TaskId taskId, final UUID processId, final boolean inCurrentAssignment, final int trafficCost, final int nonOverlapCost) {
        final Map<String, Optional<String>> clientRacks = racksForProcess.get(processId);
        if (clientRacks == null) {
            throw new IllegalStateException("Client " + processId + " doesn't exist in processRacks");
        }
        final Optional<Optional<String>> clientRackOpt = clientRacks.values().stream().filter(Optional::isPresent).findFirst();
        if (!clientRackOpt.isPresent() || !clientRackOpt.get().isPresent()) {
            throw new IllegalStateException("Client " + processId + " doesn't have rack configured. Maybe forgot to call canEnableRackAwareAssignor first");
        }

        final String clientRack = clientRackOpt.get().get();
        final Set<TopicPartition> topicPartitions = partitionsForTask.get(taskId);
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

    private static int getSinkID(final List<UUID> clientList, final List<TaskId> taskIdList) {
        return clientList.size() + taskIdList.size();
    }

    // For testing. canEnableRackAwareAssignor must be called first
    long activeTasksCost(final SortedMap<UUID, ClientState> clientStates, final SortedSet<TaskId> activeTasks, final int trafficCost, final int nonOverlapCost) {
        final List<UUID> clientList = new ArrayList<>(clientStates.keySet());
        final List<TaskId> taskIdList = new ArrayList<>(activeTasks);
        final Graph<Integer> graph = constructActiveTaskGraph(activeTasks, clientList, taskIdList,
            clientStates, new HashMap<>(), new HashMap<>(), trafficCost, nonOverlapCost);
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
     * @param clientStates Client states
     * @param activeTasks Tasks to reassign if needed. They must be assigned already in clientStates
     * @param trafficCost Cost of cross rack traffic for each TopicPartition
     * @param nonOverlapCost Cost of assign a task to a different client
     * @return Total cost after optimization
     */
    public long optimizeActiveTasks(final SortedMap<UUID, ClientState> clientStates,
                                    final SortedSet<TaskId> activeTasks,
                                    final int trafficCost,
                                    final int nonOverlapCost) {
        if (activeTasks.isEmpty()) {
            return 0;
        }

        final List<UUID> clientList = new ArrayList<>(clientStates.keySet());
        final List<TaskId> taskIdList = new ArrayList<>(activeTasks);
        final Map<TaskId, UUID> taskClientMap = new HashMap<>();
        final Map<UUID, Integer> originalAssignedTaskNumber = new HashMap<>();
        final Graph<Integer> graph = constructActiveTaskGraph(activeTasks, clientList, taskIdList,
            clientStates, taskClientMap, originalAssignedTaskNumber, trafficCost, nonOverlapCost);

        graph.solveMinCostFlow();
        final long cost = graph.totalCost();

        assignActiveTaskFromMinCostFlow(graph, activeTasks, clientList, taskIdList,
            clientStates, originalAssignedTaskNumber, taskClientMap);

        return cost;
    }

    private Graph<Integer> constructActiveTaskGraph(final SortedSet<TaskId> activeTasks,
                                                    final List<UUID> clientList,
                                                    final List<TaskId> taskIdList,
                                                    final Map<UUID, ClientState> clientStates,
                                                    final Map<TaskId, UUID> taskClientMap,
                                                    final Map<UUID, Integer> originalAssignedTaskNumber,
                                                    final int trafficCost,
                                                    final int nonOverlapCost) {
        final Graph<Integer> graph = new Graph<>();

        for (final TaskId taskId : activeTasks) {
            for (final Entry<UUID, ClientState> clientState : clientStates.entrySet()) {
                if (clientState.getValue().hasAssignedTask(taskId)) {
                    originalAssignedTaskNumber.merge(clientState.getKey(), 1, Integer::sum);
                }
            }
        }

        // Make task and client Node id in graph deterministic
        for (int taskNodeId = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            final TaskId taskId = taskIdList.get(taskNodeId);
            for (int j = 0; j < clientList.size(); j++) {
                final int clientNodeId = taskIdList.size() + j;
                final UUID processId = clientList.get(j);

                final int flow = clientStates.get(processId).hasAssignedTask(taskId) ? 1 : 0;
                final int cost = getCost(taskId, processId, flow == 1, trafficCost, nonOverlapCost);
                if (flow == 1) {
                    if (taskClientMap.containsKey(taskId)) {
                        throw new IllegalArgumentException("Task " + taskId + " assigned to multiple clients "
                            + processId + ", " + taskClientMap.get(taskId));
                    }
                    taskClientMap.put(taskId, processId);
                }

                graph.addEdge(taskNodeId, clientNodeId, 1, cost, flow);
            }
            if (!taskClientMap.containsKey(taskId)) {
                throw new IllegalArgumentException("Task " + taskId + " not assigned to any client");
            }
        }

        final int sinkId = getSinkID(clientList, taskIdList);
        for (int taskNodeId = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            graph.addEdge(SOURCE_ID, taskNodeId, 1, 0, 1);
        }

        // It's possible that some clients have 0 task assign. These clients will have 0 tasks assigned
        // even though it may have higher traffic cost. This is to maintain the original assigned task count
        for (int i = 0; i < clientList.size(); i++) {
            final int clientNodeId = taskIdList.size() + i;
            final int capacity = originalAssignedTaskNumber.getOrDefault(clientList.get(i), 0);
            // Flow equals to capacity for edges to sink
            graph.addEdge(clientNodeId, sinkId, capacity, 0, capacity);
        }

        graph.setSourceNode(SOURCE_ID);
        graph.setSinkNode(sinkId);

        return graph;
    }

    private void assignActiveTaskFromMinCostFlow(final Graph<Integer> graph,
                                                 final SortedSet<TaskId> activeTasks,
                                                 final List<UUID> clientList,
                                                 final List<TaskId> taskIdList,
                                                 final Map<UUID, ClientState> clientStates,
                                                 final Map<UUID, Integer> originalAssignedTaskNumber,
                                                 final Map<TaskId, UUID> taskClientMap) {
        int tasksAssigned = 0;
        for (int taskNodeId = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            final TaskId taskId = taskIdList.get(taskNodeId);
            final Map<Integer, Graph<Integer>.Edge> edges = graph.edges(taskNodeId);
            for (final Graph<Integer>.Edge edge : edges.values()) {
                if (edge.flow > 0) {
                    tasksAssigned++;
                    final int clientIndex = edge.destination - taskIdList.size();
                    final UUID processId = clientList.get(clientIndex);
                    final UUID originalProcessId = taskClientMap.get(taskId);

                    // Don't need to assign this task to other client
                    if (processId.equals(originalProcessId)) {
                        break;
                    }

                    clientStates.get(originalProcessId).unassignActive(taskId);
                    clientStates.get(processId).assignActive(taskId);
                }
            }
        }

        // Validate task assigned
        if (tasksAssigned != activeTasks.size()) {
            throw new IllegalStateException("Computed active task assignment number "
                + tasksAssigned + " is different size " + activeTasks.size());
        }

        // Validate original assigned task number matches
        final Map<UUID, Integer> assignedTaskNumber = new HashMap<>();
        for (final TaskId taskId : activeTasks) {
            for (final Entry<UUID, ClientState> clientState : clientStates.entrySet()) {
                if (clientState.getValue().hasAssignedTask(taskId)) {
                    assignedTaskNumber.merge(clientState.getKey(), 1, Integer::sum);
                }
            }
        }

        if (originalAssignedTaskNumber.size() != assignedTaskNumber.size()) {
            throw new IllegalStateException("There are " + originalAssignedTaskNumber.size() + " clients have "
                + " active tasks before assignment, but " + assignedTaskNumber.size() + " clients have"
                + " active tasks after assignment");
        }

        for (final Entry<UUID, Integer> originalCapacity : originalAssignedTaskNumber.entrySet()) {
            final int capacity = assignedTaskNumber.getOrDefault(originalCapacity.getKey(), 0);
            if (!Objects.equals(originalCapacity.getValue(), capacity)) {
                throw new IllegalStateException("There are " + originalCapacity.getValue() + " tasks assigned to"
                    + " client " + originalCapacity.getKey() + " before assignment, but " + capacity + " tasks "
                    + " are assigned to it after assignment");
            }
        }
    }
}
