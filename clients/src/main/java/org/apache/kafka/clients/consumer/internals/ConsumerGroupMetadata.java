package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.GroupMetadata;

import java.util.Optional;

public class ConsumerGroupMetadata implements GroupMetadata {

    private final int generation;
    private final String groupId;
    private final String memberId;
    private final Optional<String> groupInstanceId;

    public ConsumerGroupMetadata(int generation,
                                 String groupId,
                                 String memberId,
                                 Optional<String> groupInstanceId) {
        this.generation = generation;
        this.groupId = groupId;
        this.memberId = memberId;
        this.groupInstanceId = groupInstanceId;
    }

    public int generation() {
        return generation;
    }

    public String groupId() {
        return groupId;
    }

    public String memberId() {
        return memberId;
    }

    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }
}
