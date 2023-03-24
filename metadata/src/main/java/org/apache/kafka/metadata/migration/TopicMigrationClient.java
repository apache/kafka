package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface TopicMigrationClient {
    interface TopicVisitor {
        void visitTopic(String topicName, Uuid topicId, Map<Integer, List<Integer>> assignments);
        void visitPartition(TopicIdPartition topicIdPartition, PartitionRegistration partitionRegistration);
        void visitConfigs(String topicName, Properties topicProps);
    }

    void iterateTopics(TopicVisitor visitor);

    ZkMigrationLeadershipState deleteTopic(
        String topicName,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState createTopic(
        String topicName,
        Uuid topicId,
        Map<Integer, PartitionRegistration> topicPartitions,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState updateTopicPartitions(
        Map<String, Map<Integer, PartitionRegistration>> topicPartitions,
        ZkMigrationLeadershipState state
    );
}
