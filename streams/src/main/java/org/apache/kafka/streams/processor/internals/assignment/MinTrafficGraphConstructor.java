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
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor.CostFunction;

public class MinTrafficGraphConstructor<T> implements RackAwareGraphConstructor<T> {

    @Override
    public int getSinkNodeID(
        final List<TaskId> taskIdList,
        final List<UUID> clientList,
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup
    ) {
        return clientList.size() + taskIdList.size();
    }

    @Override
    public int getClientNodeId(final int clientIndex, final List<TaskId> taskIdList, final List<UUID> clientList, final int topicGroupIndex) {
        return clientIndex + taskIdList.size();
    }

    @Override
    public int getClientIndex(final int clientNodeId, final List<TaskId> taskIdList, final List<UUID> clientList, final int topicGroupIndex) {
        return clientNodeId - taskIdList.size();
    }

    @Override
    public Graph<Integer> constructTaskGraph(
        final List<UUID> clientList,
        final List<TaskId> taskIdList,
        final Map<UUID, T> clientStates,
        final Map<TaskId, UUID> taskClientMap,
        final Map<UUID, Integer> originalAssignedTaskNumber,
        final BiPredicate<T, TaskId> hasAssignedTask,
        final CostFunction costFunction,
        final int trafficCost,
        final int nonOverlapCost,
        final boolean hasReplica,
        final boolean isStandby
    ) {
        final Graph<Integer> graph = new Graph<>();

        for (final TaskId taskId : taskIdList) {
            for (final Entry<UUID, T> clientState : clientStates.entrySet()) {
                if (hasAssignedTask.test(clientState.getValue(), taskId)) {
                    originalAssignedTaskNumber.merge(clientState.getKey(), 1, Integer::sum);
                }
            }
        }

        // Make task and client Node id in graph deterministic
        for (int taskNodeId = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            final TaskId taskId = taskIdList.get(taskNodeId);
            for (int j = 0; j < clientList.size(); j++) {
                final int clientNodeId = getClientNodeId(j, taskIdList, null, -1);
                final UUID processId = clientList.get(j);

                final int flow = hasAssignedTask.test(clientStates.get(processId), taskId) ? 1 : 0;
                final int cost = costFunction.getCost(taskId, processId, flow == 1, trafficCost,
                    nonOverlapCost, isStandby);
                if (flow == 1) {
                    if (!hasReplica && taskClientMap.containsKey(taskId)) {
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

            // Add edge from source to task
            graph.addEdge(SOURCE_ID, taskNodeId, 1, 0, 1);
        }

        final int sinkId = getSinkNodeID(taskIdList, clientList, null);
        // It's possible that some clients have 0 task assign. These clients will have 0 tasks assigned
        // even though it may have higher traffic cost. This is to maintain the original assigned task count
        for (int i = 0; i < clientList.size(); i++) {
            final int clientNodeId = getClientNodeId(i, taskIdList, null, -1);
            final int capacity = originalAssignedTaskNumber.getOrDefault(clientList.get(i), 0);
            // Flow equals to capacity for edges to sink
            graph.addEdge(clientNodeId, sinkId, capacity, 0, capacity);
        }

        graph.setSourceNode(SOURCE_ID);
        graph.setSinkNode(sinkId);

        return graph;
    }

    @Override
    public boolean assignTaskFromMinCostFlow(
        final Graph<Integer> graph,
        final List<UUID> clientList,
        final List<TaskId> taskIdList,
        final Map<UUID, T> clientStates,
        final Map<UUID, Integer> originalAssignedTaskNumber,
        final Map<TaskId, UUID> taskClientMap,
        final BiConsumer<T, TaskId> assignTask,
        final BiConsumer<T, TaskId> unAssignTask,
        final BiPredicate<T, TaskId> hasAssignedTask
    ) {
        int tasksAssigned = 0;
        boolean taskMoved = false;
        for (int taskNodeId = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            final TaskId taskId = taskIdList.get(taskNodeId);
            final KeyValue<Boolean, Integer> movedAndAssigned = assignTaskToClient(graph, taskId, taskNodeId, -1,
                clientStates, clientList, taskIdList, taskClientMap, assignTask, unAssignTask);
            taskMoved |= movedAndAssigned.key;
            tasksAssigned += movedAndAssigned.value;
        }

        validateAssignedTask(taskIdList, tasksAssigned, clientStates, originalAssignedTaskNumber, hasAssignedTask);
        return taskMoved;
    }
}
