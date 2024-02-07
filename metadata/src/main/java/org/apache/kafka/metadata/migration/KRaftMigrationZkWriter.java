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

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils;
import org.apache.kafka.image.AclsDelta;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.ClientQuotaImage;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ConfigurationsDelta;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.DelegationTokenDelta;
import org.apache.kafka.image.DelegationTokenImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.ProducerIdsDelta;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.DelegationTokenData;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.ScramCredentialData;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.common.ProducerIdsBlock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class KRaftMigrationZkWriter {

    private static final String UPDATE_PRODUCER_ID = "UpdateProducerId";
    private static final String CREATE_TOPIC = "CreateTopic";
    private static final String UPDATE_TOPIC = "UpdateTopic";
    private static final String DELETE_TOPIC = "DeleteTopic";
    private static final String DELETE_PENDING_TOPIC_DELETION = "DeletePendingTopicDeletion";
    private static final String UPDATE_PARTITION = "UpdatePartition";
    private static final String DELETE_PARTITION = "DeletePartition";
    private static final String UPDATE_BROKER_CONFIG = "UpdateBrokerConfig";
    private static final String DELETE_BROKER_CONFIG = "DeleteBrokerConfig";
    private static final String UPDATE_TOPIC_CONFIG = "UpdateTopicConfig";
    private static final String DELETE_TOPIC_CONFIG = "DeleteTopicConfig";
    private static final String UPDATE_CLIENT_QUOTA = "UpdateClientQuota";
    private static final String UPDATE_ACL = "UpdateAcl";
    private static final String DELETE_ACL = "DeleteAcl";


    private final MigrationClient migrationClient;
    private final Consumer<String> errorLogger;

    public KRaftMigrationZkWriter(
        MigrationClient migrationClient,
        Consumer<String> errorLogger
    ) {
        this.migrationClient = migrationClient;
        this.errorLogger = errorLogger;
    }

    public void handleSnapshot(MetadataImage image, KRaftMigrationOperationConsumer operationConsumer) {
        handleTopicsSnapshot(image.topics(), operationConsumer);
        handleConfigsSnapshot(image.configs(), operationConsumer);
        handleClientQuotasSnapshot(image.clientQuotas(), image.scram(), operationConsumer);
        handleProducerIdSnapshot(image.producerIds(), operationConsumer);
        handleAclsSnapshot(image.acls(), operationConsumer);
        handleDelegationTokenSnapshot(image.delegationTokens(), operationConsumer);
    }

    public boolean handleDelta(
        MetadataImage previousImage,
        MetadataImage image,
        MetadataDelta delta,
        KRaftMigrationOperationConsumer operationConsumer
    ) {
        boolean updated = false;
        if (delta.topicsDelta() != null) {
            handleTopicsDelta(previousImage.topics().topicIdToNameView()::get, image.topics(), delta.topicsDelta(), operationConsumer);
            updated = true;
        }
        if (delta.configsDelta() != null) {
            handleConfigsDelta(image.configs(), delta.configsDelta(), operationConsumer);
            updated = true;
        }
        if ((delta.clientQuotasDelta() != null) || (delta.scramDelta() != null)) {
            handleClientQuotasDelta(image, delta, operationConsumer);
            updated = true;
        }
        if (delta.producerIdsDelta() != null) {
            handleProducerIdDelta(delta.producerIdsDelta(), operationConsumer);
            updated = true;
        }
        if (delta.aclsDelta() != null) {
            handleAclsDelta(previousImage.acls(), image.acls(), delta.aclsDelta(), operationConsumer);
            updated = true;
        }
        if (delta.delegationTokenDelta() != null) {
            handleDelegationTokenDelta(image.delegationTokens(), delta.delegationTokenDelta(), operationConsumer);
            updated = true;
        }
        return updated;
    }

    /**
     * Handle a snapshot of the topic metadata. This requires scanning through all the topics and partitions
     * in ZooKeeper to determine what has changed. Topic configs are not handled here since they exist in the
     * ConfigurationsImage.
     */
    void handleTopicsSnapshot(TopicsImage topicsImage, KRaftMigrationOperationConsumer operationConsumer) {
        Map<Uuid, String> deletedTopics = new HashMap<>();
        Set<Uuid> topicsInZk = new HashSet<>();
        Set<Uuid> newTopics = new HashSet<>(topicsImage.topicsById().keySet());
        Set<Uuid> changedTopics = new HashSet<>();
        Map<Uuid, Set<Integer>> partitionsInZk = new HashMap<>();
        Map<String, Set<Integer>> extraneousPartitionsInZk = new HashMap<>();
        Map<Uuid, Map<Integer, PartitionRegistration>> changedPartitions = new HashMap<>();
        Map<Uuid, Map<Integer, PartitionRegistration>> newPartitions = new HashMap<>();

        Set<String> pendingTopicDeletions = migrationClient.topicClient().readPendingTopicDeletions();
        if (!pendingTopicDeletions.isEmpty()) {
            operationConsumer.accept(
                DELETE_PENDING_TOPIC_DELETION,
                "Delete pending topic deletions",
                migrationState -> migrationClient.topicClient().clearPendingTopicDeletions(pendingTopicDeletions, migrationState)
            );
        }

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
                        if (!newTopics.remove(topicId)) return;
                        topicsInZk.add(topicId);
                    }
                }

                @Override
                public void visitPartition(TopicIdPartition topicIdPartition, PartitionRegistration partitionRegistration) {
                    TopicImage topic = topicsImage.getTopic(topicIdPartition.topicId());
                    if (topic == null) {
                        return; // The topic was deleted in KRaft. Handled by deletedTopics
                    }

                    // If there is failure in previous Zk writes, We could end up with Zookeeper
                    // containing with partial or without any partitions for existing topics. So
                    // accumulate the partition ids to check for any missing partitions in Zk.
                    partitionsInZk
                        .computeIfAbsent(topic.id(), __ -> new HashSet<>())
                        .add(topicIdPartition.partition());

                    // Check if the KRaft partition state changed
                    PartitionRegistration kraftPartition = topic.partitions().get(topicIdPartition.partition());
                    if (kraftPartition != null) {
                        if (!kraftPartition.equals(partitionRegistration)) {
                            changedPartitions.computeIfAbsent(topicIdPartition.topicId(), __ -> new HashMap<>())
                                .put(topicIdPartition.partition(), kraftPartition);
                        }

                        // Check if partition assignment has changed. This will need topic update.
                        if (!kraftPartition.hasSameAssignment(partitionRegistration)) {
                            changedTopics.add(topic.id());
                        }
                    }
                }
            });

        // Check for any partition changes in existing topics.
        topicsInZk.forEach(topicId -> {
            TopicImage topic = topicsImage.getTopic(topicId);
            Set<Integer> topicPartitionsInZk = partitionsInZk.computeIfAbsent(topicId, __ -> new HashSet<>());
            if (!topicPartitionsInZk.equals(topic.partitions().keySet())) {
                Map<Integer, PartitionRegistration> newTopicPartitions = new HashMap<>(topic.partitions());
                // Compute KRaft partitions that are not in ZK
                topicPartitionsInZk.forEach(newTopicPartitions::remove);
                newPartitions.put(topicId, newTopicPartitions);

                // Compute ZK partitions that are not in KRaft
                topicPartitionsInZk.removeAll(topic.partitions().keySet());
                if (!topicPartitionsInZk.isEmpty()) {
                    extraneousPartitionsInZk.put(topic.name(), topicPartitionsInZk);
                }
                changedTopics.add(topicId);
            }
        });

        newTopics.forEach(topicId -> {
            TopicImage topic = topicsImage.getTopic(topicId);
            operationConsumer.accept(
                CREATE_TOPIC,
                "Create Topic " + topic.name() + ", ID " + topicId,
                migrationState -> migrationClient.topicClient().createTopic(topic.name(), topicId, topic.partitions(), migrationState)
            );
        });

        changedTopics.forEach(topicId -> {
            TopicImage topic = topicsImage.getTopic(topicId);
            operationConsumer.accept(
                UPDATE_TOPIC,
                "Changed Topic " + topic.name() + ", ID " + topicId,
                migrationState -> migrationClient.topicClient().updateTopic(topic.name(), topicId, topic.partitions(), migrationState)
            );
        });

        deletedTopics.forEach((topicId, topicName) -> {
            operationConsumer.accept(
                DELETE_TOPIC,
                "Delete Topic " + topicName + ", ID " + topicId,
                migrationState -> migrationClient.topicClient().deleteTopic(topicName, migrationState)
            );
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            operationConsumer.accept(
                UPDATE_TOPIC_CONFIG,
                "Updating Configs for Topic " + topicName + ", ID " + topicId,
                migrationState -> migrationClient.configClient().deleteConfigs(resource, migrationState)
            );
        });

        newPartitions.forEach((topicId, partitionMap) -> {
            TopicImage topic = topicsImage.getTopic(topicId);
            operationConsumer.accept(
                UPDATE_PARTITION,
                "Creating additional partitions for Topic " + topic.name() + ", ID " + topicId,
                migrationState -> migrationClient.topicClient().updateTopicPartitions(
                    Collections.singletonMap(topic.name(), partitionMap),
                    migrationState));
        });

        changedPartitions.forEach((topicId, partitionMap) -> {
            TopicImage topic = topicsImage.getTopic(topicId);
            operationConsumer.accept(
                UPDATE_PARTITION,
                "Updating Partitions for Topic " + topic.name() + ", ID " + topicId,
                migrationState -> migrationClient.topicClient().updateTopicPartitions(
                    Collections.singletonMap(topic.name(), partitionMap),
                    migrationState));
        });

        extraneousPartitionsInZk.forEach((topicName, partitions) -> {
            operationConsumer.accept(
                DELETE_PARTITION,
                "Deleting extraneous Partitions " + partitions + " for Topic " + topicName,
                migrationState -> migrationClient.topicClient().deleteTopicPartitions(
                    Collections.singletonMap(topicName, partitions),
                    migrationState));
        });
    }

    void handleTopicsDelta(
        Function<Uuid, String> deletedTopicNameResolver,
        TopicsImage topicsImage,
        TopicsDelta topicsDelta,
        KRaftMigrationOperationConsumer operationConsumer
    ) {
        topicsDelta.deletedTopicIds().forEach(topicId -> {
            String name = deletedTopicNameResolver.apply(topicId);
            operationConsumer.accept(DELETE_TOPIC, "Deleting topic " + name + ", ID " + topicId,
                migrationState -> migrationClient.topicClient().deleteTopic(name, migrationState));
        });

        topicsDelta.changedTopics().forEach((topicId, topicDelta) -> {
            if (topicsDelta.createdTopicIds().contains(topicId)) {
                operationConsumer.accept(
                    CREATE_TOPIC,
                    "Create Topic " + topicDelta.name() + ", ID " + topicId,
                    migrationState -> migrationClient.topicClient().createTopic(
                        topicDelta.name(),
                        topicId,
                        topicDelta.partitionChanges(),
                        migrationState));
            } else {
                if (topicDelta.hasPartitionsWithAssignmentChanges())
                    operationConsumer.accept(
                        UPDATE_TOPIC,
                        "Updating Topic " + topicDelta.name() + ", ID " + topicId,
                        migrationState -> migrationClient.topicClient().updateTopic(
                            topicDelta.name(),
                            topicId,
                            topicsImage.getTopic(topicId).partitions(),
                            migrationState));
                Map<Integer, PartitionRegistration> newPartitions = new HashMap<>(topicDelta.newPartitions());
                Map<Integer, PartitionRegistration> changedPartitions = new HashMap<>(topicDelta.partitionChanges());
                if (!newPartitions.isEmpty()) {
                    operationConsumer.accept(
                        UPDATE_PARTITION,
                        "Create new partitions for Topic " + topicDelta.name() + ", ID " + topicId,
                        migrationState -> migrationClient.topicClient().createTopicPartitions(
                            Collections.singletonMap(topicDelta.name(), newPartitions),
                            migrationState));
                    newPartitions.keySet().forEach(changedPartitions::remove);
                }
                if (!changedPartitions.isEmpty()) {
                    // Need a final for the lambda
                    final Map<Integer, PartitionRegistration> finalChangedPartitions = changedPartitions;
                    operationConsumer.accept(
                        UPDATE_PARTITION,
                        "Updating Partitions for Topic " + topicDelta.name() + ", ID " + topicId,
                        migrationState -> migrationClient.topicClient().updateTopicPartitions(
                            Collections.singletonMap(topicDelta.name(), finalChangedPartitions),
                            migrationState));
                }
            }
        });
    }

    private String brokerOrTopicOpType(ConfigResource resource, String brokerOp, String topicOp) {
        if (resource.type().equals(ConfigResource.Type.BROKER)) {
            return brokerOp;
        } else {
            return topicOp;
        }
    }

    void handleConfigsSnapshot(ConfigurationsImage configsImage, KRaftMigrationOperationConsumer operationConsumer) {
        Set<ConfigResource> newResources = new HashSet<>();
        configsImage.resourceData().keySet().forEach(resource -> {
            if (EnumSet.of(ConfigResource.Type.BROKER, ConfigResource.Type.TOPIC).contains(resource.type())) {
                newResources.add(resource);
            } else {
                throw new RuntimeException("Unknown config resource type " + resource.type());
            }
        });
        Set<ConfigResource> resourcesToUpdate = new HashSet<>();
        BiConsumer<ConfigResource, Map<String, String>> processConfigsForResource = (ConfigResource resource, Map<String, String> configs) -> {
            newResources.remove(resource);
            Map<String, String> kraftProps = configsImage.configMapForResource(resource);
            if (!kraftProps.equals(configs)) {
                resourcesToUpdate.add(resource);
            }
        };

        migrationClient.configClient().iterateBrokerConfigs((broker, configs) -> {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, broker);
            processConfigsForResource.accept(brokerResource, configs);
        });
        migrationClient.configClient().iterateTopicConfigs((topic, configs) -> {
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            processConfigsForResource.accept(topicResource, configs);
        });

        newResources.forEach(resource -> {
            Map<String, String> props = configsImage.configMapForResource(resource);
            if (!props.isEmpty()) {
                String opType = brokerOrTopicOpType(resource, UPDATE_BROKER_CONFIG, UPDATE_TOPIC_CONFIG);
                operationConsumer.accept(opType, "Create configs for " + resource.type().name() + " " + resource.name(),
                    migrationState -> migrationClient.configClient().writeConfigs(resource, props, migrationState));
            }
        });

        resourcesToUpdate.forEach(resource -> {
            Map<String, String> props = configsImage.configMapForResource(resource);
            if (props.isEmpty()) {
                String opType = brokerOrTopicOpType(resource, DELETE_BROKER_CONFIG, DELETE_TOPIC_CONFIG);
                operationConsumer.accept(opType, "Delete configs for " + resource.type().name() + " " + resource.name(),
                    migrationState -> migrationClient.configClient().deleteConfigs(resource, migrationState));
            } else {
                String opType = brokerOrTopicOpType(resource, UPDATE_BROKER_CONFIG, UPDATE_TOPIC_CONFIG);
                operationConsumer.accept(opType, "Update configs for " + resource.type().name() + " " + resource.name(),
                    migrationState -> migrationClient.configClient().writeConfigs(resource, props, migrationState));
            }
        });
    }

    private Map<String, String> getScramCredentialStringsForUser(ScramImage image, String userName) {
        Map<String, String> userScramCredentialStrings = new HashMap<>();
        if (image != null) {
            image.mechanisms().forEach((scramMechanism, scramMechanismMap) -> {
                ScramCredentialData scramCredentialData = scramMechanismMap.get(userName);
                if (scramCredentialData != null) {
                    userScramCredentialStrings.put(scramMechanism.mechanismName(),
                        ScramCredentialUtils.credentialToString(scramCredentialData.toCredential(scramMechanism)));
                }
            });
        }
        return userScramCredentialStrings;
    }

    void handleClientQuotasSnapshot(ClientQuotasImage clientQuotasImage, ScramImage scramImage, KRaftMigrationOperationConsumer opConsumer) {
        Set<ClientQuotaEntity> changedNonUserEntities = new HashSet<>();
        Set<String> changedUsers = new HashSet<>();

        if (clientQuotasImage != null) {
            for (Entry<ClientQuotaEntity, ClientQuotaImage> entry : clientQuotasImage.entities().entrySet()) {
                ClientQuotaEntity entity = entry.getKey();
                if (entity.entries().containsKey(ClientQuotaEntity.USER) &&
                    !entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)) {
                    // Track regular user entities separately
                    // There should only be 1 entry in the list of type ClientQuotaEntity.USER
                    changedUsers.add(entity.entries().get(ClientQuotaEntity.USER));
                } else {
                    changedNonUserEntities.add(entity);
                }
            }
        }
        if (scramImage != null) {
            for (Entry<ScramMechanism, Map<String, ScramCredentialData>> mechanismEntry : scramImage.mechanisms().entrySet()) {
                for (Entry<String, ScramCredentialData> userEntry : mechanismEntry.getValue().entrySet()) {
                    changedUsers.add(userEntry.getKey());
                }
            }
        }
        migrationClient.configClient().iterateClientQuotas(new ConfigMigrationClient.ClientQuotaVisitor() {
            @Override
            public void visitClientQuota(List<ClientQuotaRecord.EntityData> entityDataList, Map<String, Double> quotas) {
                Map<String, String> entityMap = new HashMap<>(2);
                entityDataList.forEach(entityData -> entityMap.put(entityData.entityType(), entityData.entityName()));
                ClientQuotaEntity entity = new ClientQuotaEntity(entityMap);
                if (!clientQuotasImage.entities().getOrDefault(entity, ClientQuotaImage.EMPTY).quotaMap().equals(quotas)) {
                    if (entity.entries().containsKey(ClientQuotaEntity.USER) &&
                        !entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)) {
                        // Track regular user entities separately
                        changedUsers.add(entityMap.get(ClientQuotaEntity.USER));
                    } else {
                        changedNonUserEntities.add(entity);
                    }
                }
            }

            @Override
            public void visitScramCredential(String userName, ScramMechanism scramMechanism, ScramCredential scramCredential) {
                // For each ZK entity, see if it exists in the image and if it's equal
                ScramCredentialData data = scramImage.mechanisms().getOrDefault(scramMechanism, Collections.emptyMap()).get(userName);
                if (data == null || !data.toCredential(scramMechanism).equals(scramCredential)) {
                    changedUsers.add(userName);
                }
            }
        });

        changedNonUserEntities.forEach(entity -> {
            Map<String, Double> quotaMap = clientQuotasImage.entities().getOrDefault(entity, ClientQuotaImage.EMPTY).quotaMap();
            opConsumer.accept(UPDATE_CLIENT_QUOTA, "Update client quotas for " + entity, migrationState ->
                migrationClient.configClient().writeClientQuotas(entity.entries(), quotaMap, Collections.emptyMap(), migrationState));
        });

        changedUsers.forEach(userName -> {
            ClientQuotaEntity entity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, userName));
            Map<String, Double> quotaMap = clientQuotasImage.entities().
                getOrDefault(entity, ClientQuotaImage.EMPTY).quotaMap();
            Map<String, String> scramMap = getScramCredentialStringsForUser(scramImage, userName);
            opConsumer.accept(UPDATE_CLIENT_QUOTA, "Update client quotas for " + userName, migrationState ->
                migrationClient.configClient().writeClientQuotas(entity.entries(), quotaMap, scramMap, migrationState));
        });
    }

    void handleProducerIdSnapshot(ProducerIdsImage image, KRaftMigrationOperationConsumer operationConsumer) {
        if (image.isEmpty()) {
            // No producer IDs have been allocated, nothing to dual-write
            return;
        }
        Optional<ProducerIdsBlock> zkProducerId = migrationClient.readProducerId();
        if (zkProducerId.isPresent()) {
            if (zkProducerId.get().nextBlockFirstId() != image.nextProducerId()) {
                operationConsumer.accept(UPDATE_PRODUCER_ID, "Setting next producer ID", migrationState ->
                    migrationClient.writeProducerId(image.nextProducerId(), migrationState));
            }
        } else {
            operationConsumer.accept(UPDATE_PRODUCER_ID, "Setting next producer ID", migrationState ->
                migrationClient.writeProducerId(image.nextProducerId(), migrationState));
        }
    }

    void handleConfigsDelta(ConfigurationsImage configsImage, ConfigurationsDelta configsDelta, KRaftMigrationOperationConsumer operationConsumer) {
        Set<ConfigResource> updatedResources = configsDelta.changes().keySet();
        updatedResources.forEach(configResource -> {
            Map<String, String> props = configsImage.configMapForResource(configResource);
            if (props.isEmpty()) {
                operationConsumer.accept("DeleteConfig", "Delete configs for " + configResource, migrationState ->
                    migrationClient.configClient().deleteConfigs(configResource, migrationState));
            } else {
                operationConsumer.accept("UpdateConfig", "Update configs for " + configResource, migrationState ->
                    migrationClient.configClient().writeConfigs(configResource, props, migrationState));
            }
        });
    }

    void handleClientQuotasDelta(MetadataImage metadataImage, MetadataDelta metadataDelta, KRaftMigrationOperationConsumer operationConsumer) {
        if ((metadataDelta.clientQuotasDelta() != null) || (metadataDelta.scramDelta() != null)) {
            // A list of users with scram or quota changes
            HashSet<String> users = new HashSet<>();

            // Populate list with users with scram changes
            if (metadataDelta.scramDelta() != null) {
                metadataDelta.scramDelta().changes().forEach((scramMechanism, changes) -> {
                    changes.forEach((userName, changeOpt) -> users.add(userName));
                });
            }

            // Populate list with users with quota changes
            // and apply quota changes to all non-user quota changes
            if (metadataDelta.clientQuotasDelta() != null) {
                metadataDelta.clientQuotasDelta().changes().forEach((clientQuotaEntity, clientQuotaDelta) -> {
                    if ((clientQuotaEntity.entries().containsKey(ClientQuotaEntity.USER)) &&
                            (!clientQuotaEntity.entries().containsKey(ClientQuotaEntity.CLIENT_ID))) {
                        String userName = clientQuotaEntity.entries().get(ClientQuotaEntity.USER);
                        // Add clientQuotaEntity to list to process at the end
                        users.add(userName);
                    } else {
                        Map<String, Double> quotaMap = metadataImage.clientQuotas().entities().get(clientQuotaEntity).quotaMap();
                        operationConsumer.accept(UPDATE_CLIENT_QUOTA, "Updating client quota " + clientQuotaEntity, migrationState ->
                            migrationClient.configClient().writeClientQuotas(clientQuotaEntity.entries(), quotaMap, Collections.emptyMap(), migrationState));
                    }
                });
            }

            // Update user scram and quota data for each user with changes in either.
            users.forEach(userName -> {
                Map<String, String> userScramMap = getScramCredentialStringsForUser(metadataImage.scram(), userName);
                ClientQuotaEntity clientQuotaEntity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, userName));
                if ((metadataImage.clientQuotas() == null) ||
                    (metadataImage.clientQuotas().entities().get(clientQuotaEntity) == null)) {
                    operationConsumer.accept(UPDATE_CLIENT_QUOTA, "Updating scram credentials for " + clientQuotaEntity, migrationState ->
                        migrationClient.configClient().writeClientQuotas(clientQuotaEntity.entries(), Collections.emptyMap(), userScramMap, migrationState));
                } else {
                    Map<String, Double> quotaMap = metadataImage.clientQuotas().entities().get(clientQuotaEntity).quotaMap();
                    operationConsumer.accept(UPDATE_CLIENT_QUOTA, "Updating client quota for " + clientQuotaEntity, migrationState ->
                        migrationClient.configClient().writeClientQuotas(clientQuotaEntity.entries(), quotaMap, userScramMap, migrationState));
                }
            });
        }
    }

    void handleProducerIdDelta(ProducerIdsDelta delta, KRaftMigrationOperationConsumer operationConsumer) {
        operationConsumer.accept(UPDATE_PRODUCER_ID, "Setting next producer ID", migrationState ->
            migrationClient.writeProducerId(delta.nextProducerId(), migrationState));
    }

    private ResourcePattern resourcePatternFromAcl(StandardAcl acl) {
        return new ResourcePattern(acl.resourceType(), acl.resourceName(), acl.patternType());
    }

    void handleAclsSnapshot(AclsImage image, KRaftMigrationOperationConsumer operationConsumer) {
        // Need to compare contents of image with all ACLs in ZK and issue updates
        Map<ResourcePattern, Set<AccessControlEntry>> allAclsInSnapshot = new HashMap<>();

        image.acls().values().forEach(standardAcl -> {
            ResourcePattern resourcePattern = resourcePatternFromAcl(standardAcl);
            allAclsInSnapshot.computeIfAbsent(resourcePattern, __ -> new HashSet<>()).add(
                new AccessControlEntry(standardAcl.principal(), standardAcl.host(), standardAcl.operation(), standardAcl.permissionType())
            );
        });

        Set<ResourcePattern> newResources = new HashSet<>(allAclsInSnapshot.keySet());
        Set<ResourcePattern> resourcesToDelete = new HashSet<>();
        Map<ResourcePattern, Set<AccessControlEntry>> changedResources = new HashMap<>();
        migrationClient.aclClient().iterateAcls((resourcePattern, accessControlEntries) -> {
            newResources.remove(resourcePattern);
            if (!allAclsInSnapshot.containsKey(resourcePattern)) {
                resourcesToDelete.add(resourcePattern);
            } else {
                Set<AccessControlEntry> snapshotEntries = allAclsInSnapshot.get(resourcePattern);
                if (!snapshotEntries.equals(accessControlEntries)) {
                    changedResources.put(resourcePattern, snapshotEntries);
                }
            }
        });

        newResources.forEach(resourcePattern -> {
            // newResources is generated from allAclsInSnapshot, and we don't remove from that map, so this unguarded .get() is safe
            Set<AccessControlEntry> accessControlEntries = allAclsInSnapshot.get(resourcePattern);
            String name = "Writing " + accessControlEntries.size() + " for resource " + resourcePattern;
            operationConsumer.accept(UPDATE_ACL, name, migrationState ->
                migrationClient.aclClient().writeResourceAcls(resourcePattern, accessControlEntries, migrationState));
        });

        resourcesToDelete.forEach(deletedResource -> {
            String name = "Deleting resource " + deletedResource + " which has no ACLs in snapshot";
            operationConsumer.accept(DELETE_ACL, name, migrationState ->
                migrationClient.aclClient().deleteResource(deletedResource, migrationState));
        });

        changedResources.forEach((resourcePattern, accessControlEntries) -> {
            String name = "Writing " + accessControlEntries.size() + " for resource " + resourcePattern;
            operationConsumer.accept(UPDATE_ACL, name, migrationState ->
                migrationClient.aclClient().writeResourceAcls(resourcePattern, accessControlEntries, migrationState));
        });
    }

    void handleAclsDelta(AclsImage prevImage, AclsImage image, AclsDelta delta, KRaftMigrationOperationConsumer operationConsumer) {
        // Need to collect all ACLs for any changed resource pattern
        Map<ResourcePattern, List<AccessControlEntry>> aclsToWrite = new HashMap<>();
        delta.changes().forEach((aclId, aclChange) -> {
            if (aclChange.isPresent()) {
                ResourcePattern resourcePattern = resourcePatternFromAcl(aclChange.get());
                aclsToWrite.put(resourcePattern, new ArrayList<>());
            } else {
                // We need to look in the previous image to get deleted ACLs resource pattern
                StandardAcl deletedAcl = prevImage.acls().get(aclId);
                if (deletedAcl == null) {
                    errorLogger.accept("Cannot delete ACL " + aclId + " from ZK since it is missing from previous AclImage");
                } else {
                    ResourcePattern resourcePattern = resourcePatternFromAcl(deletedAcl);
                    aclsToWrite.put(resourcePattern, new ArrayList<>());
                }
            }
        });

        // Iterate through the new image to collect any ACLs for these changed resources
        image.acls().forEach((uuid, standardAcl) -> {
            ResourcePattern resourcePattern = resourcePatternFromAcl(standardAcl);
            List<AccessControlEntry> entries = aclsToWrite.get(resourcePattern);
            if (entries != null) {
                entries.add(new AccessControlEntry(standardAcl.principal(), standardAcl.host(), standardAcl.operation(), standardAcl.permissionType()));
            }
        });

        // If there are no more ACLs for a resource, delete it. Otherwise, update it with the new set of ACLs
        aclsToWrite.forEach((resourcePattern, accessControlEntries) -> {
            if (accessControlEntries.isEmpty()) {
                String name = "Deleting resource " + resourcePattern + " which has no more ACLs";
                operationConsumer.accept(DELETE_ACL, name, migrationState ->
                    migrationClient.aclClient().deleteResource(resourcePattern, migrationState));
            } else {
                String name = "Writing " + accessControlEntries.size() + " for resource " + resourcePattern;
                operationConsumer.accept(UPDATE_ACL, name, migrationState ->
                    migrationClient.aclClient().writeResourceAcls(resourcePattern, accessControlEntries, migrationState));
            }
        });
    }

    void handleDelegationTokenDelta(DelegationTokenImage image, DelegationTokenDelta delta, KRaftMigrationOperationConsumer operationConsumer) {
        Set<String> updatedTokens = delta.changes().keySet();
        updatedTokens.forEach(tokenId -> {
            DelegationTokenData tokenData = image.tokens().get(tokenId);
            if (tokenData == null) {
                operationConsumer.accept("DeleteDelegationToken", "Delete DelegationToken for " + tokenId, migrationState ->
                    migrationClient.delegationTokenClient().deleteDelegationToken(tokenId, migrationState));
            } else {
                operationConsumer.accept("UpdateDelegationToken", "Update DelegationToken for " + tokenId, migrationState ->
                    migrationClient.delegationTokenClient().writeDelegationToken(tokenId, tokenData.tokenInformation(), migrationState));
            }
        });
    }

    void handleDelegationTokenSnapshot(DelegationTokenImage image, KRaftMigrationOperationConsumer operationConsumer) {
        image.tokens().keySet().forEach(tokenId -> {
            DelegationTokenData tokenData = image.tokens().get(tokenId);
            operationConsumer.accept("UpdateDelegationToken", "Update DelegationToken for " + tokenId, migrationState ->
                migrationClient.delegationTokenClient().writeDelegationToken(tokenId, tokenData.tokenInformation(), migrationState));
        });

        List<String> tokens = migrationClient.delegationTokenClient().getDelegationTokens();
        tokens.forEach(tokenId -> {
            if (!image.tokens().containsKey(tokenId)) {
                operationConsumer.accept("DeleteDelegationToken", "Delete DelegationToken for " + tokenId, migrationState ->
                    migrationClient.delegationTokenClient().deleteDelegationToken(tokenId, migrationState));
            }
        });
    }
}
