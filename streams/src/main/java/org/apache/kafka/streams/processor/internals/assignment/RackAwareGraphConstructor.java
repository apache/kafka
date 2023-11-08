package org.apache.kafka.streams.processor.internals.assignment;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor.GetCostFunction;

/**
 * Construct graph for rack aware task assignor
 */
public interface RackAwareGraphConstructor {
    int SOURCE_ID = -1;

    static int getSinkNodeID(final List<UUID> clientList, final List<TaskId> taskIdList) {
        return clientList.size() + taskIdList.size();
    }

    static int getClientNodeId(final List<TaskId> taskIdList, final int clientIndex) {
        return clientIndex + taskIdList.size();
    }

    static int getClientIndex(final List<TaskId> taskIdList, final int clientNodeId) {
        return clientNodeId - taskIdList.size();
    }

    Graph<Integer> constructTaskGraph(final List<UUID> clientList,
        final List<TaskId> taskIdList,
        final Map<UUID, ClientState> clientStates,
        final Map<TaskId, UUID> taskClientMap,
        final Map<UUID, Integer> originalAssignedTaskNumber,
        final BiPredicate<ClientState, TaskId> hasAssignedTask,
        final GetCostFunction getCostFunction,
        final int trafficCost,
        final int nonOverlapCost,
        final boolean hasReplica,
        final boolean isStandby);

    boolean assignTaskFromMinCostFlow(final Graph<Integer> graph,
        final List<UUID> clientList,
        final List<TaskId> taskIdList,
        final Map<UUID, ClientState> clientStates,
        final Map<UUID, Integer> originalAssignedTaskNumber,
        final Map<TaskId, UUID> taskClientMap,
        final BiConsumer<ClientState, TaskId> assignTask,
        final BiConsumer<ClientState, TaskId> unAssignTask,
        final BiPredicate<ClientState, TaskId> hasAssignedTask);
}
