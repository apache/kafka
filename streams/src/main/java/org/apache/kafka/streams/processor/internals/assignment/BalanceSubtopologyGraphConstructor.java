package org.apache.kafka.streams.processor.internals.assignment;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor.GetCostFunction;

public class BalanceSubtopologyGraphConstructor implements RackAwareGraphConstructor {

    @Override
    public Graph<Integer> constructTaskGraph(final List<UUID> clientList,
        final List<TaskId> taskIdList, final Map<UUID, ClientState> clientStates,
        final Map<TaskId, UUID> taskClientMap, final Map<UUID, Integer> originalAssignedTaskNumber,
        final BiPredicate<ClientState, TaskId> hasAssignedTask, final GetCostFunction getCostFunction, final int trafficCost,
        final int nonOverlapCost, final boolean hasReplica, final boolean isStandby) {
        // TODO
        return null;
    }

    @Override
    public boolean assignTaskFromMinCostFlow(final Graph<Integer> graph,
        final List<UUID> clientList, final List<TaskId> taskIdList,
        final Map<UUID, ClientState> clientStates,
        final Map<UUID, Integer> originalAssignedTaskNumber, final Map<TaskId, UUID> taskClientMap,
        final BiConsumer<ClientState, TaskId> assignTask,
        final BiConsumer<ClientState, TaskId> unAssignTask,
        final BiPredicate<ClientState, TaskId> hasAssignedTask) {
        // TODO
        return false;
    }
}
