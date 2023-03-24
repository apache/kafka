package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CapturingTopicMigrationClient implements TopicMigrationClient {
    public List<String> deletedTopics = new ArrayList<>();
    public List<String> createdTopics = new ArrayList<>();
    public LinkedHashMap<String, Set<Integer>> updatedTopicPartitions = new LinkedHashMap<>();


    public void reset() {
        deletedTopics.clear();
    }

    @Override
    public void iterateTopics(TopicVisitor visitor) {

    }

    @Override
    public ZkMigrationLeadershipState deleteTopic(String topicName, ZkMigrationLeadershipState state) {
        deletedTopics.add(topicName);
        return state;
    }

    @Override
    public ZkMigrationLeadershipState createTopic(String topicName, Uuid topicId, Map<Integer, PartitionRegistration> topicPartitions, ZkMigrationLeadershipState state) {
        createdTopics.add(topicName);
        return state;
    }

    @Override
    public ZkMigrationLeadershipState updateTopicPartitions(Map<String, Map<Integer, PartitionRegistration>> topicPartitions, ZkMigrationLeadershipState state) {
        topicPartitions.forEach((topicName, partitionMap) ->
            updatedTopicPartitions.put(topicName, partitionMap.keySet())
        );
        return state;
    }
}
