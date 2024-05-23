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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor.CostFunction;

/**
 * Construct graph for rack aware task assignor
 */
public interface RackAwareGraphConstructor<T> {
    int SOURCE_ID = -1;

    int getSinkNodeID(final List<TaskId> taskIdList, final List<UUID> clientList, final Map<Subtopology, Set<TaskId>> tasksForTopicGroup);

    int getClientNodeId(final int clientIndex, final List<TaskId> taskIdList, final List<UUID> clientList, final int topicGroupIndex);

    int getClientIndex(final int clientNodeId, final List<TaskId> taskIdList, final List<UUID> clientList, final int topicGroupIndex);

    Graph<Integer> constructTaskGraph(
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
        final boolean isStandby);

    boolean assignTaskFromMinCostFlow(
        final Graph<Integer> graph,
        final List<UUID> clientList,
        final List<TaskId> taskIdList,
        final Map<UUID, T> clientStates,
        final Map<UUID, Integer> originalAssignedTaskNumber,
        final Map<TaskId, UUID> taskClientMap,
        final BiConsumer<T, TaskId> assignTask,
        final BiConsumer<T, TaskId> unAssignTask,
        final BiPredicate<T, TaskId> hasAssignedTask);

    default KeyValue<Boolean, Integer> assignTaskToClient(
        final Graph<Integer> graph,
        final TaskId taskId,
        final int taskNodeId,
        final int topicGroupIndex,
        final Map<UUID, T> clientStates,
        final List<UUID> clientList,
        final List<TaskId> taskIdList,
        final Map<TaskId, UUID> taskClientMap,
        final BiConsumer<T, TaskId> assignTask,
        final BiConsumer<T, TaskId> unAssignTask
    ) {
        int tasksAssigned = 0;
        boolean taskMoved = false;
        final Map<Integer, Graph<Integer>.Edge> edges = graph.edges(taskNodeId);
        for (final Graph<Integer>.Edge edge : edges.values()) {
            if (edge.flow > 0) {
                tasksAssigned++;
                final int clientIndex = getClientIndex(edge.destination, taskIdList, clientList, topicGroupIndex);
                final UUID processId = clientList.get(clientIndex);
                final UUID originalProcessId = taskClientMap.get(taskId);

                // Don't need to assign this task to other client
                if (processId.equals(originalProcessId)) {
                    break;
                }

                unAssignTask.accept(clientStates.get(originalProcessId), taskId);
                assignTask.accept(clientStates.get(processId), taskId);
                taskMoved = true;
            }
        }
        return KeyValue.pair(taskMoved, tasksAssigned);
    }

    default void validateAssignedTask(
        final List<TaskId> taskIdList,
        final int tasksAssigned,
        final Map<UUID, T> clientStates,
        final Map<UUID, Integer> originalAssignedTaskNumber,
        final BiPredicate<T, TaskId> hasAssignedTask
    ) {
        // Validate task assigned
        if (tasksAssigned != taskIdList.size()) {
            throw new IllegalStateException("Computed active task assignment number "
                + tasksAssigned + " is different size " + taskIdList.size());
        }

        // Validate original assigned task number matches
        final Map<UUID, Integer> assignedTaskNumber = new HashMap<>();
        for (final TaskId taskId : taskIdList) {
            for (final Entry<UUID, T> clientState : clientStates.entrySet()) {
                if (hasAssignedTask.test(clientState.getValue(), taskId)) {
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
