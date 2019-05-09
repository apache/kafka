package org.apache.kafka.common.replica;

import org.apache.kafka.common.TopicPartition;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RackAwareReplicaSelector implements ReplicaSelector {

    private final MostCaughtUpReplicaSelector tieBreaker = new MostCaughtUpReplicaSelector();

    @Override
    public Optional<ReplicaInfo> select(TopicPartition topicPartition, ClientMetadata clientMetadata, Set<ReplicaInfo> replicaInfos) {
        if (clientMetadata.rackId != null && !clientMetadata.rackId.isEmpty()) {
            Set<ReplicaInfo> sameRackReplicas = replicaInfos.stream()
                    .filter(replicaInfo -> clientMetadata.rackId.equalsIgnoreCase(replicaInfo.getEndpoint().rack()))
                    .collect(Collectors.toSet());
            if (sameRackReplicas.isEmpty()) {
                return tieBreaker.select(topicPartition, clientMetadata, replicaInfos);
            } else {
                Optional<ReplicaInfo> leader = ReplicaSelector.selectLeader(sameRackReplicas);
                if (leader.isPresent()) {
                    return leader;
                } else {
                    return tieBreaker.select(topicPartition, clientMetadata, sameRackReplicas);
                }
            }
        } else {
            return ReplicaSelector.selectLeader(replicaInfos);
        }
    }
}
