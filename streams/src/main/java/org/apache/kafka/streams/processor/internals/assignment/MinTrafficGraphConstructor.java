package org.apache.kafka.streams.processor.internals.assignment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor.GetCostFunction;

public class MinTrafficGraphConstructor implements RackAwareGraphConstructor {

    @Override
    public Graph<Integer> constructTaskGraph(final List<UUID> clientList,
        final List<TaskId> taskIdList, final Map<UUID, ClientState> clientStates,
        final Map<TaskId, UUID> taskClientMap, final Map<UUID, Integer> originalAssignedTaskNumber,
        final BiPredicate<ClientState, TaskId> hasAssignedTask, final GetCostFunction getCostFunction, final int trafficCost,
        final int nonOverlapCost, final boolean hasReplica, final boolean isStandby) {

        final Graph<Integer> graph = new Graph<>();

        for (final TaskId taskId : taskIdList) {
            for (final Entry<UUID, ClientState> clientState : clientStates.entrySet()) {
                if (hasAssignedTask.test(clientState.getValue(), taskId)) {
                    originalAssignedTaskNumber.merge(clientState.getKey(), 1, Integer::sum);
                }
            }
        }

        // Make task and client Node id in graph deterministic
        for (int taskNodeId = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            final TaskId taskId = taskIdList.get(taskNodeId);
            for (int j = 0; j < clientList.size(); j++) {
                final int clientNodeId = RackAwareGraphConstructor.getClientNodeId(taskIdList, j);
                final UUID processId = clientList.get(j);

                final int flow = hasAssignedTask.test(clientStates.get(processId), taskId) ? 1 : 0;
                final int cost = getCostFunction.getCost(taskId, processId, flow == 1, trafficCost,
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

        final int sinkId = RackAwareGraphConstructor.getSinkNodeID(clientList, taskIdList);
        // It's possible that some clients have 0 task assign. These clients will have 0 tasks assigned
        // even though it may have higher traffic cost. This is to maintain the original assigned task count
        for (int i = 0; i < clientList.size(); i++) {
            final int clientNodeId = RackAwareGraphConstructor.getClientNodeId(taskIdList, i);
            final int capacity = originalAssignedTaskNumber.getOrDefault(clientList.get(i), 0);
            // Flow equals to capacity for edges to sink
            graph.addEdge(clientNodeId, sinkId, capacity, 0, capacity);
        }

        graph.setSourceNode(SOURCE_ID);
        graph.setSinkNode(sinkId);

        return graph;
    }

    @Override
    public boolean assignTaskFromMinCostFlow(final Graph<Integer> graph,
        final List<UUID> clientList, final List<TaskId> taskIdList,
        final Map<UUID, ClientState> clientStates,
        final Map<UUID, Integer> originalAssignedTaskNumber, final Map<TaskId, UUID> taskClientMap,
        final BiConsumer<ClientState, TaskId> assignTask,
        final BiConsumer<ClientState, TaskId> unAssignTask,
        final BiPredicate<ClientState, TaskId> hasAssignedTask) {

        int tasksAssigned = 0;
        boolean taskMoved = false;
        for (int taskNodeId = 0; taskNodeId < taskIdList.size(); taskNodeId++) {
            final TaskId taskId = taskIdList.get(taskNodeId);
            final Map<Integer, Graph<Integer>.Edge> edges = graph.edges(taskNodeId);
            for (final Graph<Integer>.Edge edge : edges.values()) {
                if (edge.flow > 0) {
                    tasksAssigned++;
                    final int clientIndex = RackAwareGraphConstructor.getClientIndex(taskIdList, edge.destination);
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
        }

        // Validate task assigned
        if (tasksAssigned != taskIdList.size()) {
            throw new IllegalStateException("Computed active task assignment number "
                + tasksAssigned + " is different size " + taskIdList.size());
        }

        // Validate original assigned task number matches
        final Map<UUID, Integer> assignedTaskNumber = new HashMap<>();
        for (final TaskId taskId : taskIdList) {
            for (final Entry<UUID, ClientState> clientState : clientStates.entrySet()) {
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

        return taskMoved;
    }
}
