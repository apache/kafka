package org.apache.kafka.storage.internals.log;

import java.util.List;

public record OngoingReassignmentState(
        List<Integer> addingReplicas,
        List<Integer> removingReplicas,
        List<Integer> replicas
) implements AssignmentState{
    
    @Override
    public int replicationFactor() {
        return (int)replicas.stream().filter(r -> !addingReplicas.contains(r)).count();
    }

    @Override
    public Boolean isAddingReplica(int brokerId) {
        return addingReplicas.contains(brokerId);
    }
}
