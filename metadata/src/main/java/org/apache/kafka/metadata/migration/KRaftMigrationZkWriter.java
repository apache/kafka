/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.image.ClientQuotasDelta;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ConfigurationsDelta;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class KRaftMigrationZkWriter {
    private final MigrationClient migrationClient;
    private final BiConsumer<String, KRaftMigrationOperation> operationConsumer;

    public KRaftMigrationZkWriter(
        MigrationClient migrationClient,
        BiConsumer<String, KRaftMigrationOperation>  operationConsumer
    ) {
        this.migrationClient = migrationClient;
        this.operationConsumer = operationConsumer;
    }

    public void handleSnapshot(MetadataImage image) {
        handleTopicsSnapshot(image.topics());
        handleConfigsSnapshot(image.configs());
        handleClientQuotasSnapshot(image.clientQuotas());
        operationConsumer.accept("Setting next producer ID", migrationState ->
            migrationClient.writeProducerId(image.producerIds().highestSeenProducerId(), migrationState));
    }

    public void handleDelta(MetadataImage image, MetadataDelta delta) {
        if (delta.topicsDelta() != null) {
            handleTopicsDelta(delta.topicsDelta());
        }
        if (delta.configsDelta() != null) {
            handleConfigsDelta(image.configs(), delta.configsDelta());
        }
        if (delta.clientQuotasDelta() != null) {
            handleClientQuotasDelta(image.clientQuotas(), delta.clientQuotasDelta());
        }
        if (delta.producerIdsDelta() != null) {
            operationConsumer.accept("Updating next producer ID", migrationState ->
                migrationClient.writeProducerId(delta.producerIdsDelta().nextProducerId(), migrationState));
        }
    }

    /**
     * Handle a snapshot of the topic metadata. This requires scanning through all the topics and partitions
     * in ZooKeeper to determine what has changed.
     */
    void handleTopicsSnapshot(TopicsImage topicsImage) {
        Map<Uuid, String> deletedTopics = new HashMap<>();
        Set<Uuid> createdTopics = new HashSet<>(topicsImage.topicsById().keySet());
        Map<Uuid, Map<Integer, PartitionRegistration>> changedPartitions = new HashMap<>();

        migrationClient.topicClient().iterateTopics(
            EnumSet.of(
                TopicMigrationClient.TopicVisitorInterest.TOPICS,
                TopicMigrationClient.TopicVisitorInterest.PARTITIONS),
            new TopicMigrationClient.TopicVisitor() {
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
    }

    void handleTopicsDelta(TopicsDelta topicsDelta) {
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

    void handleConfigsSnapshot(ConfigurationsImage configsImage) {
        Set<ConfigResource> brokersToUpdate = new HashSet<>();
        migrationClient.configClient().iterateBrokerConfigs((broker, configs) -> {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, broker);
            Map<String, String> kraftProps = configsImage.configMapForResource(brokerResource);
            if (!kraftProps.equals(configs)) {
                brokersToUpdate.add(brokerResource);
            }
        });

        brokersToUpdate.forEach(brokerResource -> {
            Map<String, String> props = configsImage.configMapForResource(brokerResource);
            if (props.isEmpty()) {
                operationConsumer.accept("Delete configs for broker " + brokerResource.name(), migrationState ->
                    migrationClient.configClient().deleteConfigs(brokerResource, migrationState));
            } else {
                operationConsumer.accept("Update configs for broker " + brokerResource.name(), migrationState ->
                    migrationClient.configClient().writeConfigs(brokerResource, props, migrationState));
            }
        });
    }

    void handleClientQuotasSnapshot(ClientQuotasImage image) {
        Set<ClientQuotaEntity> changedEntities = new HashSet<>();
        migrationClient.configClient().iterateClientQuotas((entityDataList, props) -> {
            Map<String, String> entityMap = new HashMap<>(2);
            entityDataList.forEach(entityData -> entityMap.put(entityData.entityType(), entityData.entityName()));
            ClientQuotaEntity entity = new ClientQuotaEntity(entityMap);
            if (!image.entities().get(entity).quotaMap().equals(props)) {
                changedEntities.add(entity);
            }
        });

        changedEntities.forEach(entity -> {
            Map<String, Double> quotaMap = image.entities().get(entity).quotaMap();
            operationConsumer.accept("Update client quotas for " + entity, migrationState ->
                migrationClient.configClient().writeClientQuotas(entity.entries(), quotaMap, migrationState));
        });
    }

    void handleConfigsDelta(ConfigurationsImage configsImage, ConfigurationsDelta configsDelta) {
        Set<ConfigResource> updatedResources = configsDelta.changes().keySet();
        updatedResources.forEach(configResource -> {
            Map<String, String> props = configsImage.configMapForResource(configResource);
            if (props.isEmpty()) {
                operationConsumer.accept("Delete configs for " + configResource, migrationState ->
                    migrationClient.configClient().deleteConfigs(configResource, migrationState));
            } else {
                operationConsumer.accept("Update configs for " + configResource, migrationState ->
                    migrationClient.configClient().writeConfigs(configResource, props, migrationState));
            }
        });
    }

    void handleClientQuotasDelta(ClientQuotasImage image, ClientQuotasDelta delta) {
        Set<ClientQuotaEntity> changedEntities = delta.changes().keySet();
        changedEntities.forEach(clientQuotaEntity -> {
            Map<String, Double> quotaMap = image.entities().get(clientQuotaEntity).quotaMap();
            operationConsumer.accept("Update client quotas for " + clientQuotaEntity, migrationState ->
                migrationClient.configClient().writeClientQuotas(clientQuotaEntity.entries(), quotaMap, migrationState));
        });
    }
}
