package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface TopicMigrationClient {

    enum TopicVisitorInterest {
        TOPICS,
        PARTITIONS,
        CONFIGS
    }

    interface TopicVisitor {
        void visitTopic(String topicName, Uuid topicId, Map<Integer, List<Integer>> assignments);
        default void visitPartition(TopicIdPartition topicIdPartition, PartitionRegistration partitionRegistration) {

        }
        default void visitConfigs(String topicName, Properties topicProps) {

        }
    }

    void iterateTopics(EnumSet<TopicVisitorInterest> interests, TopicVisitor visitor);

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
