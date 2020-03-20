package org.apache.kafka.streams.processor.internals.assignment;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.RankedClient;

public class DefaultDataParallelBalancedAssignor<ID extends Comparable<ID>> {

    /**
     * My temporary stub while waiting for this assignor to be implemented, this should obviously be replaced as soon
     * as we have the real balanced assignor is merged to trunk. If I forget to remove this I owe you a beer
     */
    public Map<ID, List<TaskId>> assign(final SortedMap<TaskId, SortedSet<RankedClient<ID>>> statefulTasksToRankedClients,
                                        final int balanceFactor,
                                        final Map<ID, Integer> clientsToNumberOfStreamThreads) {
        return Collections.emptyMap();
    }
}
