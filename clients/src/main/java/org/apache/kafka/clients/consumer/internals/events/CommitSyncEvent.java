package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;

public class CommitSyncEvent extends CompletableApplicationEvent<Void> {

    private Map<TopicPartition, OffsetAndMetadata> offsets;

    public CommitSyncEvent(Map<TopicPartition, OffsetAndMetadata> offsets) {
        super(Type.COMMIT_SYNC);
        this.offsets = Collections.unmodifiableMap(offsets);
    }

    public Map<TopicPartition, OffsetAndMetadata> offsets() {
        return offsets;
    }

}
