package org.apache.kafka.clients;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;

import java.util.*;


public class MetadataTestTopic {
    private String topic1 = "topic1";
    private String topic2 = "topic2";
    private Set<String> internalTopics;
    private Map<String, Uuid> topicIds;
    private Map<String, Integer> topicPartitionCounts;
    private TopicPartition tp11;
    private TopicPartition tp12;
    private TopicPartition tp21;
    private RequestTestUtils.PartitionMetadataSupplier metadataSupplier;
    private MetadataResponse.PartitionMetadata part11Metadata;
    private MetadataResponse.PartitionMetadata part12Metadata;
    private MetadataResponse.PartitionMetadata part2Metadata;
    private MetadataResponse.PartitionMetadata internalTopicMetadata;

    public MetadataTestTopic() {
        Set<String> internalTopics = Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME);

        TopicPartition tp11 = new TopicPartition(topic1, 0);
        TopicPartition tp12 = new TopicPartition(topic1, 1);
        TopicPartition tp21 = new TopicPartition(topic2, 0);
        TopicPartition internalPart = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0);

        MetadataResponse.PartitionMetadata part11Metadata = new MetadataResponse.PartitionMetadata(Errors.NONE, tp11, Optional.of(1), Optional.of(100), Arrays.asList(1, 2), Arrays.asList(1, 2), Arrays.asList(3));
        MetadataResponse.PartitionMetadata part12Metadata = new MetadataResponse.PartitionMetadata(Errors.NONE, tp12, Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Arrays.asList(1));
        MetadataResponse.PartitionMetadata part2Metadata = new MetadataResponse.PartitionMetadata(Errors.NONE, tp21, Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Arrays.asList(1));
        MetadataResponse.PartitionMetadata internalTopicMetadata = new MetadataResponse.PartitionMetadata(Errors.NONE, internalPart, Optional.of(2), Optional.of(200), Arrays.asList(2, 3), Arrays.asList(2, 3), Arrays.asList(1));


        Uuid topic1Id = Uuid.randomUuid();
        Uuid topic2Id = Uuid.randomUuid();
        Uuid internalTopicId = Uuid.randomUuid();


        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put(topic1, topic1Id);
        topicIds.put(topic2, topic2Id);
        topicIds.put(internalTopics.iterator().next(), internalTopicId);

        Map<String, Integer> topicPartitionCounts = new HashMap<>();
        topicPartitionCounts.put(topic1, 2);
        topicPartitionCounts.put(topic2, 1);
        topicPartitionCounts.put(internalTopics.iterator().next(), 1);

        RequestTestUtils.PartitionMetadataSupplier metadataSupplier = (error, partition, leaderId, leaderEpoch, replicas, isr, offlineReplicas) -> {
            if (partition.equals(tp11))
                return part11Metadata;
            else if (partition.equals(tp21))
                return part2Metadata;
            else if (partition.equals(tp12))
                return part12Metadata;
            else if (partition.equals(internalPart))
                return internalTopicMetadata;
            throw new RuntimeException("Unexpected partition " + partition);
        };

        this.topicIds = topicIds;
        this.topicPartitionCounts = topicPartitionCounts;
        this.tp11 = tp11;
        this.tp12 = tp12;
        this.tp21 = tp21;
        this.metadataSupplier = metadataSupplier;
        this.topicPartitionCounts = topicPartitionCounts;
        this.internalTopics = internalTopics;
    }
    public String getTopic1() {
        return topic1;
    }
    public String getTopic2() {
        return topic2;
    }
    public Set<String> getInternalTopics() {
        return internalTopics
    }
    public Map<String, Uuid> getTopicIds() {
        return topicIds;
    }
    public Map<String, Integer> getTopicPartitionCounts() {
        return topicPartitionCounts;
    }
    public TopicPartition getTp11() {
        return tp11;
    }
    public TopicPartition getTp12() {
        return tp12;
    }
    public TopicPartition getTp21() {
        return tp21;
    }
    public RequestTestUtils.PartitionMetadataSupplier getMetadataSupplier() {
        return metadataSupplier;
    }
    public MetadataResponse.PartitionMetadata getPart11Metadata() {
        return part11Metadata;
    }
    public MetadataResponse.PartitionMetadata getPart12Metadata() {
        return part12Metadata;
    }
    public MetadataResponse.PartitionMetadata getPart2Metadata() {
        return part2Metadata;
    }
    public MetadataResponse.PartitionMetadata getInternalTopicMetadata() {
        return internalTopicMetadata;
    }
}
