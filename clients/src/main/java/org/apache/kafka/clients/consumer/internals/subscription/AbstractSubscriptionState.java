package org.apache.kafka.clients.consumer.internals.subscription;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

abstract public class AbstractSubscriptionState {
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    private enum SubscriptionType {
        NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
    }

    /* the type of subscription */
    private SubscriptionType subscriptionType;

    /* the pattern user has requested */
    private Pattern subscribedPattern;

    /* the list of topics the user has requested */
    Set<String> subscription;

    /* The list of topics the group has subscribed to. This may include some topics which are not part
     * of `subscription` for the leader of a group since it is responsible for detecting metadata changes
     * which require a group rebalance. */
    private Set<String> groupSubscription;

    /* the partitions that are currently assigned, note that the order of partition matters (see FetchBuilder for more details) */
    protected final PartitionStates<TopicPartitionState> assignment;

    private int assignmentId = 0;

    public boolean assignFromUser(Set<TopicPartition> partitions) {
        //setSubscriptionType(SubscriptionState.SubscriptionType.USER_ASSIGNED);

        if (this.assignment.partitionSet().equals(partitions))
            return false;

        incAssignment();

        // update the subscribed topics
        Set<String> manualSubscribedTopics = new HashSet<>();
        Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
        for (TopicPartition partition : partitions) {
            TopicPartitionState state = assignment.stateValue(partition);
            if (state == null)
                state = new TopicPartitionState();
            partitionToState.put(partition, state);

            manualSubscribedTopics.add(partition.topic());
        }

        this.assignment.set(partitionToState);
        return changeSubscription(manualSubscribedTopics);
    }

    boolean changeSubscription(Set<String> topicsToSubscribe) {
        if (subscription.equals(topicsToSubscribe))
            return false;

        subscription = topicsToSubscribe;
        return true;
    }


    public AbstractSubscriptionState() {
        this.assignment = new PartitionStates<>();
    }

    public int incAssignment() {
        return assignmentId++;
    }

    public boolean hasAutoAssignedPartitions() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public Set<String> subscription() {
        if (hasAutoAssignedPartitions())
            return this.subscription;
        return Collections.emptySet();
    }

    public static class TopicPartitionState {
        private FetchPosition fetchPosition; // last consumed position
        private Position consumedPosition; // last consumed position
        private Position commitPosition; // last consumed position

        private Long highWatermark; // the high watermark from last fetch
        private Long logStartOffset; // the log start offset
        private Long lastStableOffset;
        private boolean paused;  // whether this partition has been paused by the user
        private boolean pendingRevocation;
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting
        private Long nextRetryTimeMs;
        private Integer preferredReadReplica;
        private Long preferredReadReplicaExpireTimeMs;
        private boolean endOffsetRequested;

        public Position consumedPosition() {
            return consumedPosition;
        }

        public boolean hasValidPosition() {
            // to be impl
            return true;
        }
    }

    static class Position {
        public long offset;
        public Optional<Integer> offsetEpoch;
    }

    public class FetchPosition extends Position {
        Optional<Integer> offsetEpoch;
        Metadata.LeaderAndEpoch currentLeader;

    }

}
