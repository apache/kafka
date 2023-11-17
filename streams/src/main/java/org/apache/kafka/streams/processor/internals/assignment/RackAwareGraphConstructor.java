package org.apache.kafka.streams.processor.internals.assignment;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor.GetCostFunction;

/**
 * Construct graph for rack aware task assignor
 */
public interface RackAwareGraphConstructor {
    int SOURCE_ID = -1;

    int getSinkNodeID(final List<TaskId> taskIdList, final List<UUID> clientList, final Map<Subtopology, Set<TaskId>> tasksForTopicGroup);

    int getClientNodeId(final int clientIndex, final List<TaskId> taskIdList, final List<UUID> clientList, final int topicGroupIndex);

    int getClientIndex(final int clientNodeId, final List<TaskId> taskIdList, final List<UUID> clientList, final int topicGroupIndex);

    Graph<Integer> constructTaskGraph(final List<UUID> clientList,
        final List<TaskId> taskIdList,
        final SortedMap<UUID, ClientState> clientStates,
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
