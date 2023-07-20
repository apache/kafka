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
    private static final int DEFAULT_STATEFUL_TRAFFIC_COST = 10;
    private static final int DEFAULT_STATEFUL_NON_OVERLAP_COST = 1;

    private static final int DEFAULT_STATELESS_TRAFFIC_COST = 1;
    private static final int DEFAULT_STATELESS_NON_OVERLAP_COST = 0;

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

    private int getCost(final TaskId taskId, final UUID clientId, final boolean inCurrentAssignment, final boolean isStateful) {
        final Map<String, Optional<String>> clientRacks = racksForProcess.get(clientId);
        if (clientRacks == null) {
            throw new IllegalStateException("Client " + clientId + " doesn't exist in processRacks");
        }
        final Optional<Optional<String>> clientRackOpt = clientRacks.values().stream().filter(Optional::isPresent).findFirst();
        if (!clientRackOpt.isPresent() || !clientRackOpt.get().isPresent()) {
            throw new IllegalStateException("Client " + clientId + " doesn't have rack configured. Maybe forgot to call canEnableRackAwareAssignor first");
        }

        final String clientRack = clientRackOpt.get().get();
        final Set<TopicPartition> topicPartitions = partitionsForTask.get(taskId);
        if (topicPartitions == null) {
            throw new IllegalStateException("Task " + taskId + " has no TopicPartitions");
        }

        final int trafficCost = assignmentConfigs.trafficCost == null ? (isStateful ? DEFAULT_STATEFUL_TRAFFIC_COST : DEFAULT_STATELESS_TRAFFIC_COST)
            : assignmentConfigs.trafficCost;
        final int nonOverlapCost = assignmentConfigs.nonOverlapCost == null ? (isStateful ? DEFAULT_STATEFUL_NON_OVERLAP_COST : DEFAULT_STATELESS_NON_OVERLAP_COST)
            : assignmentConfigs.nonOverlapCost;

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

    // For testing. canEnableRackAwareAssignor must be called first
    long activeTasksCost(final SortedMap<UUID, ClientState> clientStates, final SortedSet<TaskId> statefulTasks, final boolean isStateful) {
        final List<UUID> clientList = new ArrayList<>(clientStates.keySet());
        final List<TaskId> taskIdList = new ArrayList<>(statefulTasks);
        final Map<TaskId, UUID> taskClientMap = new HashMap<>();
        final Map<UUID, Integer> clientCapacity = new HashMap<>();
        final Graph<Integer> graph = new Graph<>();

        constructStatefulActiveTaskGraph(graph, statefulTasks, clientList, taskIdList,
            clientStates, taskClientMap, clientCapacity, isStateful);

        final int sourceId = taskIdList.size() + clientList.size();
        final int sinkId = sourceId + 1;
        for (int taskNodeId = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            graph.addEdge(sourceId, taskNodeId, 1, 0, 1);
        }
        for (int i = 0; i < clientList.size(); i++) {
            final int capacity = clientCapacity.getOrDefault(clientList.get(i), 0);
            final int clientNodeId = taskIdList.size() + i;
            graph.addEdge(clientNodeId, sinkId, capacity, 0, capacity);
        }
        graph.setSourceNode(sourceId);
        graph.setSinkNode(sinkId);
        return graph.totalCost();
    }

    /**
     * Optimize active stateful task assignment for rack awareness. canEnableRackAwareAssignor must be called first
     * @param clientStates Client states
     * @param taskIds Tasks to reassign if needed. They must be assigned already in clientStates
     * @param isStateful Whether the tasks are stateful
     * @return Total cost after optimization
     */
    public long optimizeActiveTasks(final SortedMap<UUID, ClientState> clientStates,
                                    final SortedSet<TaskId> taskIds,
                                    final boolean isStateful) {
        if (taskIds.isEmpty()) {
            return 0;
        }

        final List<UUID> clientList = new ArrayList<>(clientStates.keySet());
        final List<TaskId> taskIdList = new ArrayList<>(taskIds);
        final Map<TaskId, UUID> taskClientMap = new HashMap<>();
        final Map<UUID, Integer> clientCapacity = new HashMap<>();
        final Graph<Integer> graph = new Graph<>();

        constructStatefulActiveTaskGraph(graph, taskIds, clientList, taskIdList,
            clientStates, taskClientMap, clientCapacity, isStateful);

        graph.solveMinCostFlow();
        final long cost = graph.totalCost();

        assignStatefulActiveTaskFromMinCostFlow(graph, taskIds, clientList, taskIdList,
            clientStates, clientCapacity, taskClientMap);

        return cost;
    }

    private void constructStatefulActiveTaskGraph(final Graph<Integer> graph,
                                                  final SortedSet<TaskId> statefulTasks,
                                                  final List<UUID> clientList,
                                                  final List<TaskId> taskIdList,
                                                  final Map<UUID, ClientState> clientStates,
                                                  final Map<TaskId, UUID> taskClientMap,
                                                  final Map<UUID, Integer> clientCapacity,
                                                  final boolean isStateful) {
        for (final TaskId taskId : statefulTasks) {
            for (final Entry<UUID, ClientState> clientState : clientStates.entrySet()) {
                if (clientState.getValue().hasAssignedTask(taskId)) {
                    clientCapacity.merge(clientState.getKey(), 1, Integer::sum);
                }
            }
        }

        // Make task and client Node id in graph deterministic
        for (int taskNodeId  = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            final TaskId taskId = taskIdList.get(taskNodeId);
            for (int j = 0; j < clientList.size(); j++) {
                final int clientNodeId = taskIdList.size() + j;
                final UUID clientId = clientList.get(j);

                final int flow = clientStates.get(clientId).hasAssignedTask(taskId) ? 1 : 0;
                final int cost = getCost(taskId, clientId, flow == 1, isStateful);
                if (flow == 1) {
                    if (taskClientMap.containsKey(taskId)) {
                        throw new IllegalArgumentException("Task " + taskId + " assigned to multiple clients "
                            + clientId + ", " + taskClientMap.get(taskId));
                    }
                    taskClientMap.put(taskId, clientId);
                }

                graph.addEdge(taskNodeId, clientNodeId, 1, cost, flow);
            }
            if (!taskClientMap.containsKey(taskId)) {
                throw new IllegalArgumentException("Task " + taskId + " not assigned to any client");
            }
        }

        final int sourceId = taskIdList.size() + clientList.size();
        final int sinkId = sourceId + 1;

        for (int taskNodeId = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            graph.addEdge(sourceId, taskNodeId, 1, 0, 1);
        }

        // It's possible that some clients have 0 task assign. These clients will have 0 tasks assigned
        // even though it may have lower traffic cost. This is to maintain the balance requirement.
        for (int i = 0; i < clientList.size(); i++) {
            final int clientNodeId = taskIdList.size() + i;
            final int capacity = clientCapacity.getOrDefault(clientList.get(i), 0);
            // Flow equals to capacity for edges to sink
            graph.addEdge(clientNodeId, sinkId, capacity, 0, capacity);
        }

        graph.setSourceNode(sourceId);
        graph.setSinkNode(sinkId);
    }

    private void assignStatefulActiveTaskFromMinCostFlow(final Graph<Integer> graph,
                                                         final SortedSet<TaskId> statefulTasks,
                                                         final List<UUID> clientList,
                                                         final List<TaskId> taskIdList,
                                                         final Map<UUID, ClientState> clientStates,
                                                         final Map<UUID, Integer> originalClientCapacity,
                                                         final Map<TaskId, UUID> taskClientMap) {
        int taskAssigned = 0;
        for (int taskNodeId = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            final TaskId taskId = taskIdList.get(taskNodeId);
            final Map<Integer, Graph<Integer>.Edge> edges = graph.edges(taskNodeId);
            for (final Graph<Integer>.Edge edge : edges.values()) {
                if (edge.flow > 0) {
                    taskAssigned++;
                    final int clientIndex = edge.destination - taskIdList.size();
                    final UUID clientId = clientList.get(clientIndex);
                    final UUID originalClientId = taskClientMap.get(taskId);

                    // Don't need to assign this task to other client
                    if (clientId.equals(originalClientId)) {
                        break;
                    }

                    final ClientState clientState = clientStates.get(clientId);
                    final ClientState originalClientState = clientStates.get(originalClientId);
                    originalClientState.unassignActive(taskId);
                    clientState.assignActive(taskId);
                }
            }
        }

        // Validate task assigned
        if (taskAssigned != statefulTasks.size()) {
            throw new IllegalStateException("Computed active stateful task assignment number "
                + taskAssigned + " is different statefulTasks size " + statefulTasks.size());
        }

        // Validate capacity constraint
        final Map<UUID, Integer> clientCapacity = new HashMap<>();
        for (final TaskId taskId : statefulTasks) {
            for (final Entry<UUID, ClientState> clientState : clientStates.entrySet()) {
                if (clientState.getValue().hasAssignedTask(taskId)) {
                    clientCapacity.merge(clientState.getKey(), 1, Integer::sum);
                }
            }
        }

        if (originalClientCapacity.size() != clientCapacity.size()) {
            throw new IllegalStateException("There are " + originalClientCapacity.size() + " clients have "
                + " active stateful tasks before assignment, but " + clientCapacity.size() + " clients have"
                + " active stateful tasks after assignment");
        }

        for (final Entry<UUID, Integer> originalCapacity : originalClientCapacity.entrySet()) {
            final int capacity = clientCapacity.getOrDefault(originalCapacity.getKey(), 0);
            if (!Objects.equals(originalCapacity.getValue(), capacity)) {
                throw new IllegalStateException("There are " + originalCapacity.getValue() + " tasks assigned to"
                    + " client " + originalCapacity.getKey() + " before assignment, but " + capacity + " tasks "
                    + " are assigned to it after assignment");
            }
        }
    }
}
