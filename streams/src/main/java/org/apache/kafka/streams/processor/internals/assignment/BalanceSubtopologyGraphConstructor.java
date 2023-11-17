package org.apache.kafka.streams.processor.internals.assignment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor.GetCostFunction;

public class BalanceSubtopologyGraphConstructor implements RackAwareGraphConstructor {

    private final Map<Subtopology, Set<TaskId>> tasksForTopicGroup;

    public BalanceSubtopologyGraphConstructor(final Map<Subtopology, Set<TaskId>> tasksForTopicGroup) {
        this.tasksForTopicGroup = tasksForTopicGroup;
    }

    @Override
    public int getSinkNodeID(final List<TaskId> taskIdList, final List<UUID> clientList,
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup) {
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
    public Graph<Integer> constructTaskGraph(final List<UUID> clientList,
        final List<TaskId> taskIdList, final SortedMap<UUID, ClientState> clientStates,
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
                    final int flow = hasAssignedTask.test(clientStates.get(processId), taskId) ? 1 : 0;
                    graph.addEdge(startingTaskNodeId, clientNodeId, 1, getCostFunction.getCost(taskId, processId, false, trafficCost, nonOverlapCost, isStandby), flow);
                    graph.addEdge(SOURCE_ID, startingTaskNodeId, 1, 0, 0);
                    startingTaskNodeId++;
                }

                final int secondStageClientNodeId = getSecondStageClientNodeId(taskIdList, clientList, tasksForTopicGroup, clientIndex);
                final int capacity = (int) Math.ceil(originalAssignedTaskNumber.get(processId) * 1.0 / taskIdList.size() * taskIds.size());
                graph.addEdge(clientNodeId, secondStageClientNodeId, capacity, 0, 0);
            }

            taskNodeId += taskIds.size();
            topicGroupIndex++;
        }

        for (int clientIndex = 0; clientIndex < clientList.size(); clientIndex++) {
            final UUID processId = clientList.get(clientIndex);
            final int capacity = originalAssignedTaskNumber.get(processId);
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
    public boolean assignTaskFromMinCostFlow(final Graph<Integer> graph,
        final List<UUID> clientList, final List<TaskId> taskIdList,
        final Map<UUID, ClientState> clientStates,
        final Map<UUID, Integer> originalAssignedTaskNumber, final Map<TaskId, UUID> taskClientMap,
        final BiConsumer<ClientState, TaskId> assignTask,
        final BiConsumer<ClientState, TaskId> unAssignTask,
        final BiPredicate<ClientState, TaskId> hasAssignedTask) {

        final SortedMap<Subtopology, Set<TaskId>> sortedTasksForTopicGroup = new TreeMap<>(tasksForTopicGroup);

        int taskNodeId = 0;
        int topicGroupIndex = 0;
        int tasksAssigned = 0;
        boolean taskMoved = false;
        for (final Entry<Subtopology, Set<TaskId>> kv : sortedTasksForTopicGroup.entrySet()) {
            final SortedSet<TaskId> taskIds = new TreeSet<>(kv.getValue());
            for (final TaskId taskId : taskIds) {
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
                taskNodeId++;
            }
            topicGroupIndex++;
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
