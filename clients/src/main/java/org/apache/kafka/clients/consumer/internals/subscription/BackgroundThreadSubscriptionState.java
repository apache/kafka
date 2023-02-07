package org.apache.kafka.clients.consumer.internals.subscription;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class BackgroundThreadSubscriptionState extends AbstractSubscriptionState {
    public BackgroundThreadSubscriptionState() {
    }

    public void updateConsumedPosition(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    public void allConsumed() {
    }

    public void fetchablePartitions() {
    }

    public FetchPosition getFetchPosition(TopicPartition partition) {
        return null;
    }

    public void setFetchPosition() {
    }

    public void getCommitPosition() {
    }

    public void setCommitPosition() {
    }

    public void clearPreferredReadReplica() {
    }
}