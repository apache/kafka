package kafka.server;

import java.util.Optional;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;

/**
 * This interface defines the APIs needed to handle any state transitions
 * related to tiering in AbstractFetcherThread.
 */
public interface TierStateMachine {

    /**
     * Start the tier state machine for the provided topic partition.
     *
     * @param topicPartition the topic partition
     * @param currentFetchState the current PartitionFetchState which will
     *                          be used to derive the return value
     *
     * @return the new PartitionFetchState after the successful start of the
     *         tier state machine
     */
    PartitionFetchState start(TopicPartition topicPartition,
                              PartitionFetchState currentFetchState,
                              PartitionData fetchPartitionData) throws Exception;

    /**
     * Optionally advance the state of the tier state machine, based on the
     * current PartitionFetchState. The decision to advance the tier
     * state machine is implementation specific.
     *
     * @param topicPartition the topic partition
     * @param currentFetchState the current PartitionFetchState which will
     *                          be used to derive the return value
     *
     * @return the new PartitionFetchState if the tier state machine was advanced
     */
    Optional<PartitionFetchState> maybeAdvanceState(TopicPartition topicPartition,
                                                    PartitionFetchState currentFetchState);
}
