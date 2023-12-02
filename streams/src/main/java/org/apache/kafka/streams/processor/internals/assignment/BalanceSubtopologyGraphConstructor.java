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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor.CostFunction;

public class BalanceSubtopologyGraphConstructor implements RackAwareGraphConstructor {

    private final Map<Subtopology, Set<TaskId>> tasksForTopicGroup;

    public BalanceSubtopologyGraphConstructor(final Map<Subtopology, Set<TaskId>> tasksForTopicGroup) {
        this.tasksForTopicGroup = tasksForTopicGroup;
    }

    @Override
    public int getSinkNodeID(
        final List<TaskId> taskIdList,
        final List<UUID> clientList,
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup
    ) {
        return clientList.size() + taskIdList.size() + clientList.size() * tasksForTopicGroup.size();
    }


    @Override
    public int getClientNodeId(final int clientIndex, final List<TaskId> taskIdList, final List<UUID> clientList, final int topicGroupIndex) {
        return taskIdList.size() + clientList.size() * topicGroupIndex + clientIndex;
    }

    @Override
    public int getClientIndex(final int clientNodeId, final List<TaskId> taskIdList, final List<UUID> clientList, final int topicGroupIndex) {
        return clientNodeId - taskIdList.size() - clientList.size() * topicGroupIndex;
    }

    private static int getSecondStageClientNodeId(final List<TaskId> taskIdList, final List<UUID> clientList, final Map<Subtopology, Set<TaskId>> tasksForTopicGroup, final int clientIndex) {
        return taskIdList.size() + clientList.size() * tasksForTopicGroup.size() + clientIndex;
    }

    @Override
    public Graph<Integer> constructTaskGraph(
        final List<UUID> clientList,
        final List<TaskId> taskIdList,
        final Map<UUID, ClientState> clientStates,
        final Map<TaskId, UUID> taskClientMap,
        final Map<UUID, Integer> originalAssignedTaskNumber,
        final BiPredicate<ClientState, TaskId> hasAssignedTask,
        final CostFunction costFunction,
        final int trafficCost,
        final int nonOverlapCost,
        final boolean hasReplica,
        final boolean isStandby
    ) {
        final Graph<Integer> graph = new Graph<>();

        for (final TaskId taskId : taskIdList) {
            for (final Entry<UUID, ClientState> clientState : clientStates.entrySet()) {
                if (hasAssignedTask.test(clientState.getValue(), taskId)) {
                    originalAssignedTaskNumber.merge(clientState.getKey(), 1, Integer::sum);
                }
            }
        }

        // TODO: validate tasks in tasksForTopicGroup and taskIdList
        final SortedMap<Subtopology, Set<TaskId>> sortedTasksForTopicGroup = new TreeMap<>(tasksForTopicGroup);
        final int sinkId = getSinkNodeID(taskIdList, clientList, tasksForTopicGroup);

        int taskNodeId = 0;
        int topicGroupIndex = 0;
        for (final Entry<Subtopology, Set<TaskId>> kv : sortedTasksForTopicGroup.entrySet()) {
            final SortedSet<TaskId> taskIds = new TreeSet<>(kv.getValue());
            for (int clientIndex = 0; clientIndex < clientList.size(); clientIndex++) {
                final UUID processId = clientList.get(clientIndex);
                final int clientNodeId = getClientNodeId(clientIndex, taskIdList, clientList, topicGroupIndex);
                int startingTaskNodeId = taskNodeId;
                for (final TaskId taskId : taskIds) {
                    final boolean inCurrentAssignment = hasAssignedTask.test(clientStates.get(processId), taskId);
                    graph.addEdge(startingTaskNodeId, clientNodeId, 1, costFunction.getCost(taskId, processId, inCurrentAssignment, trafficCost, nonOverlapCost, isStandby), 0);
                    startingTaskNodeId++;
                    if (inCurrentAssignment) {
                        if (!hasReplica && taskClientMap.containsKey(taskId)) {
                            throw new IllegalArgumentException("Task " + taskId + " assigned to multiple clients "
                                + processId + ", " + taskClientMap.get(taskId));
                        }
                        taskClientMap.put(taskId, processId);
                    }
                }

                final int secondStageClientNodeId = getSecondStageClientNodeId(taskIdList, clientList, tasksForTopicGroup, clientIndex);
                final int capacity = originalAssignedTaskNumber.containsKey(processId) ? (int) Math.ceil(originalAssignedTaskNumber.get(processId) * 1.0 / taskIdList.size() * taskIds.size()) : 0;
                graph.addEdge(clientNodeId, secondStageClientNodeId, capacity, 0, 0);
            }

            taskNodeId += taskIds.size();
            topicGroupIndex++;
        }

        // Add edges from source to all tasks. Since they have same capacity, cost and flow, we can use their ids directly
        for (int i = 0; i < taskIdList.size(); i++) {
            graph.addEdge(SOURCE_ID, i, 1, 0, 0);
        }

        // Add sink
        for (int clientIndex = 0; clientIndex < clientList.size(); clientIndex++) {
            final UUID processId = clientList.get(clientIndex);
            final int capacity = originalAssignedTaskNumber.getOrDefault(processId, 0);
            final int secondStageClientNodeId = getSecondStageClientNodeId(taskIdList, clientList, tasksForTopicGroup, clientIndex);
            graph.addEdge(secondStageClientNodeId, sinkId, capacity, 0, 0);
        }

        graph.setSourceNode(SOURCE_ID);
        graph.setSinkNode(sinkId);

        // Run max flow algorithm to get a solution first
        final long maxFlow = graph.calculateMaxFlow();
        if (maxFlow != taskIdList.size()) {
            throw new IllegalStateException("max flow calculated: " + maxFlow + " doesn't match taskSize: " + taskIdList.size());
        }

        return graph;
    }

    @Override
    public boolean assignTaskFromMinCostFlow(
        final Graph<Integer> graph,
        final List<UUID> clientList,
        final List<TaskId> taskIdList,
        final Map<UUID, ClientState> clientStates,
        final Map<UUID, Integer> originalAssignedTaskNumber,
        final Map<TaskId, UUID> taskClientMap,
        final BiConsumer<ClientState, TaskId> assignTask,
        final BiConsumer<ClientState, TaskId> unAssignTask,
        final BiPredicate<ClientState, TaskId> hasAssignedTask
    ) {
        final SortedMap<Subtopology, Set<TaskId>> sortedTasksForTopicGroup = new TreeMap<>(tasksForTopicGroup);

        int taskNodeId = 0;
        int topicGroupIndex = 0;
        int tasksAssigned = 0;
        boolean taskMoved = false;
        for (final Entry<Subtopology, Set<TaskId>> kv : sortedTasksForTopicGroup.entrySet()) {
            final SortedSet<TaskId> taskIds = new TreeSet<>(kv.getValue());
            for (final TaskId taskId : taskIds) {
                final KeyValue<Boolean, Integer> movedAndAssigned = assignTaskToClient(graph, taskId, taskNodeId, topicGroupIndex,
                    clientStates, clientList, taskIdList, taskClientMap, assignTask, unAssignTask);
                taskMoved |= movedAndAssigned.key;
                tasksAssigned += movedAndAssigned.value;
                taskNodeId++;
            }
            topicGroupIndex++;
        }

        validateAssignedTask(taskIdList, tasksAssigned, clientStates, originalAssignedTaskNumber, hasAssignedTask);

        return taskMoved;
    }
}
