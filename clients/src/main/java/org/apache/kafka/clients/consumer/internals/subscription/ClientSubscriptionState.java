package org.apache.kafka.clients.consumer.internals.subscription;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ClientSubscriptionState extends AbstractSubscriptionState {
    /* Default offset reset strategy */
    private final OffsetResetStrategy defaultResetStrategy;
    private final LogContext logContext;

    // maybe we move this out of this class
    private ConsumerRebalanceListener listener;

    public ClientSubscriptionState(LogContext logContext, final OffsetResetStrategy strategy) {
        super();
        this.logContext = logContext;
        this.defaultResetStrategy = strategy;
    }

    public Map<TopicPartition, OffsetAndMetadata> consumedPosition() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        assignment.forEach((topicPartition, partitionState) -> {
            if (partitionState.hasValidPosition())
                allConsumed.put(topicPartition, new OffsetAndMetadata(partitionState.consumedPosition().offset,
                        partitionState.consumedPosition().offsetEpoch, ""));
        });
        return allConsumed;
    }

    public void updateConsumedPosition() {
        assignmentUpdated = true;
    }

    public boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public void subscribe(final Set<String> t1, ConsumerRebalanceListener listener) {
        assignmentUpdated = true;
        registerRebalanceListener(listener);
        //setSubscriptionType(SubscriptionState.SubscriptionType.AUTO_TOPICS);
        changeSubscription(t1);
    }

    private void registerRebalanceListener(ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        this.listener = listener;
    }


    public boolean isAssigned(TopicPartition partition) {
    }

    public SubscriptionState.FetchPosition validPosition(TopicPartition partition) {
    }
}
