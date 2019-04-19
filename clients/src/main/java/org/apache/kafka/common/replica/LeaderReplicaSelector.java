package org.apache.kafka.common.replica;

import org.apache.kafka.common.TopicPartition;

import java.util.Optional;
import java.util.Set;

public class LeaderReplicaSelector implements ReplicaSelector {
    @Override
    public Optional<ReplicaInfo> select(TopicPartition topicPartition,
                                        ClientMetadata clientMetadata,
                                        Set<ReplicaInfo> replicaInfos) {
        return replicaInfos.stream().filter(ReplicaInfo::isLeader).findFirst();
    }
}
