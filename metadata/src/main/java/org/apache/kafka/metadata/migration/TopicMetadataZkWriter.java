package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.image.ConfigurationImage;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;

public class TopicMetadataZkWriter {
    private final MigrationClient migrationClient;
    private final BiConsumer<String, KRaftMigrationOperation> operationConsumer;

    public TopicMetadataZkWriter(
            MigrationClient migrationClient,
        BiConsumer<String, KRaftMigrationOperation>  operationConsumer
    ) {
        this.migrationClient = migrationClient;
        this.operationConsumer = operationConsumer;
    }

    /**
     * Handle a snapshot of the topic metadata. This requires scanning through all the topics and partitions
     * in ZooKeeper to determine what has changed.
     */
    public void handleSnapshot(TopicsImage topicsImage, ConfigurationsImage configsImage) {
        Map<Uuid, String> deletedTopics = new HashMap<>();
        Set<Uuid> createdTopics = new HashSet<>(topicsImage.topicsById().keySet());
        Map<Uuid, Map<Integer, PartitionRegistration>> changedPartitions = new HashMap<>();
        Set<String> changedConfigs = new HashSet<>();

        migrationClient.topicClient().iterateTopics(new TopicMigrationClient.TopicVisitor() {
            @Override
            public void visitTopic(String topicName, Uuid topicId, Map<Integer, List<Integer>> assignments) {
                TopicImage topic = topicsImage.getTopic(topicId);
                if (topic == null) {
                    // If KRaft does not have this topic, it was deleted
                    deletedTopics.put(topicId, topicName);
                } else {
                    createdTopics.remove(topicId);
                }
            }

            @Override
            public void visitPartition(TopicIdPartition topicIdPartition, PartitionRegistration partitionRegistration) {
                TopicImage topic = topicsImage.getTopic(topicIdPartition.topicId());
                if (topic == null) {
                    return; // topic deleted in KRaft
                }

                // Check if the KRaft partition state changed
                PartitionRegistration kraftPartition = topic.partitions().get(topicIdPartition.partition());
                if (!kraftPartition.equals(partitionRegistration)) {
                    changedPartitions.computeIfAbsent(topicIdPartition.topicId(), __ -> new HashMap<>())
                        .put(topicIdPartition.partition(), kraftPartition);
                }
            }

            @Override
            public void visitConfigs(String topicName, Properties topicProps) {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
                Properties kraftProps = configsImage.configProperties(resource);
                if (!kraftProps.equals(topicProps)) {
                    changedConfigs.add(topicName);
                }
            }
        });

        createdTopics.forEach(topicId -> {
            TopicImage topic = topicsImage.getTopic(topicId);
            operationConsumer.accept(
                "Create Topic " + topic.name() + ", ID " + topicId,
                migrationState -> migrationClient.topicClient().createTopic(topic.name(), topicId, topic.partitions(), migrationState)
            );
        });

        deletedTopics.forEach((topicId, topicName) -> {
            operationConsumer.accept(
                "Delete Topic " + topicName + ", ID " + topicId,
                migrationState -> migrationClient.topicClient().deleteTopic(topicName, migrationState)
            );
            TopicImage topic = topicsImage.getTopic(topicId);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic.name());
            operationConsumer.accept(
                "Updating Configs for Topic " + topic.name() + ", ID " + topicId,
                migrationState -> migrationClient.configClient().deleteConfigs(resource, migrationState)
            );
        });

        changedPartitions.forEach((topicId, paritionMap) -> {
            TopicImage topic = topicsImage.getTopic(topicId);
            operationConsumer.accept(
                "Updating Partitions for Topic " + topic.name() + ", ID " + topicId,
                migrationState -> migrationClient.topicClient().updateTopicPartitions(
                    Collections.singletonMap(topic.name(), paritionMap),
                    migrationState));
        });

        changedConfigs.forEach(topicId -> {
            TopicImage topic = topicsImage.getTopic(topicId);
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic.name());
            Map<String, String> props = configsImage.configMapForResource(resource);
            operationConsumer.accept(
                "Updating Configs for Topic " + topic.name() + ", ID " + topicId,
                migrationState -> migrationClient.configClient().writeConfigs(resource, props, migrationState)
            );
        });

    }

    public void handleDelta(TopicsImage __, TopicsDelta topicsDelta) {
        topicsDelta.changedTopics().forEach((topicId, topicDelta) -> {
            if (topicsDelta.createdTopicIds().contains(topicId)) {
                operationConsumer.accept(
                    "Create Topic " + topicDelta.name() + ", ID " + topicId,
                    migrationState -> migrationClient.topicClient().createTopic(
                        topicDelta.name(),
                        topicId,
                        topicDelta.partitionChanges(),
                        migrationState));
            } else {
                operationConsumer.accept(
                    "Updating Partitions for Topic " + topicDelta.name() + ", ID " + topicId,
                    migrationState -> migrationClient.topicClient().updateTopicPartitions(
                        Collections.singletonMap(topicDelta.name(), topicDelta.partitionChanges()),
                        migrationState));
            }
        });
    }
}
