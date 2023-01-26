package kafka.server;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.FetchRequest;

import java.util.Optional;

public class ReplicaAlterLogDirsTierStateMachine implements TierStateMachine {

    public PartitionFetchState start(TopicPartition topicPartition,
                              PartitionFetchState currentFetchState,
                              FetchRequest.PartitionData fetchPartitionData) throws Exception {
        // JBOD is not supported with tiered storage.
        throw new UnsupportedOperationException("Building remote log aux state not supported in ReplicaAlterLogDirsThread.");
    }

    public Optional<PartitionFetchState> maybeAdvanceState(TopicPartition topicPartition,
                                                           PartitionFetchState currentFetchState) {
        return Optional.empty();
    }
}
