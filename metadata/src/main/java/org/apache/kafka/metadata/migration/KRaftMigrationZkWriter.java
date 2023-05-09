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
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.ScramCredentialData;
import org.apache.kafka.metadata.authorizer.StandardAcl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        handleClientQuotasSnapshot(image.clientQuotas(), image.scram());
        operationConsumer.accept("Setting next producer ID", migrationState ->
            migrationClient.writeProducerId(image.producerIds().highestSeenProducerId(), migrationState));
        handleAclsSnapshot(image.acls());
    }

    public void handleDelta(MetadataImage previousImage, MetadataImage image, MetadataDelta delta) {
        if (delta.topicsDelta() != null) {
            handleTopicsDelta(previousImage.topics().topicIdToNameView()::get, delta.topicsDelta());
        }
        if (delta.configsDelta() != null) {
            handleConfigsDelta(image.configs(), delta.configsDelta());
        }
        if (delta.clientQuotasDelta() != null) {
            handleClientQuotasDelta(image, delta);
        }
        if (delta.producerIdsDelta() != null) {
            operationConsumer.accept("Updating next producer ID", migrationState ->
                migrationClient.writeProducerId(delta.producerIdsDelta().nextProducerId(), migrationState));
        }
        if (delta.aclsDelta() != null) {
            handleAclsDelta(image.acls(), delta.aclsDelta());
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
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            operationConsumer.accept(
                "Updating Configs for Topic " + topicName + ", ID " + topicId,
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

    void handleTopicsDelta(Function<Uuid, String> deletedTopicNameResolver, TopicsDelta topicsDelta) {
        topicsDelta.deletedTopicIds().forEach(topicId -> {
            String name = deletedTopicNameResolver.apply(topicId);
            operationConsumer.accept("Deleting topic " + name + ", ID " + topicId,
                migrationState -> migrationClient.topicClient().deleteTopic(name, migrationState));
        });

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

    void handleClientQuotasSnapshot(ClientQuotasImage clientQuotasImage, ScramImage scramImage) {
        Set<ClientQuotaEntity> changedNonUserEntities = new HashSet<>();
        Set<String> changedUsers = new HashSet<>();
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
            Map<String, Double> quotaMap = clientQuotasImage.entities().get(entity).quotaMap();
            operationConsumer.accept("Update client quotas for " + entity, migrationState ->
                migrationClient.configClient().writeClientQuotas(entity.entries(), quotaMap, Collections.emptyMap(), migrationState));
        });

        changedUsers.forEach(userName -> {
            ClientQuotaEntity entity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, userName));
            Map<String, Double> quotaMap = clientQuotasImage.entities().get(entity).quotaMap();
            Map<String, String> scramMap = getScramCredentialStringsForUser(scramImage, userName);
            operationConsumer.accept("Update scram credentials for " + userName, migrationState ->
                migrationClient.configClient().writeClientQuotas(entity.entries(), quotaMap, scramMap, migrationState));
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

    void handleClientQuotasDelta(MetadataImage metadataImage, MetadataDelta metadataDelta) {
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
                        operationConsumer.accept("Updating client quota " + clientQuotaEntity, migrationState ->
                            migrationClient.configClient().writeClientQuotas(clientQuotaEntity.entries(), quotaMap, Collections.emptyMap(), migrationState));
                    }
                });
            }

            // Update user scram and quota data for each user with changes in either.
            users.forEach(userName -> {
                Map<String, String> userScramMap = getScramCredentialStringsForUser(metadataImage.scram(), userName);
                ClientQuotaEntity clientQuotaEntity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, userName));
                if (metadataImage.clientQuotas() == null) {
                    operationConsumer.accept("Updating client quota " + clientQuotaEntity, migrationState ->
                        migrationClient.configClient().writeClientQuotas(clientQuotaEntity.entries(), Collections.emptyMap(), userScramMap, migrationState));
                } else {
                    Map<String, Double> quotaMap = metadataImage.clientQuotas().entities().get(clientQuotaEntity).quotaMap();
                    operationConsumer.accept("Updating client quota " + clientQuotaEntity, migrationState ->
                        migrationClient.configClient().writeClientQuotas(clientQuotaEntity.entries(), quotaMap, userScramMap, migrationState));
                }
            });
        }
    }

    private ResourcePattern resourcePatternFromAcl(StandardAcl acl) {
        return new ResourcePattern(acl.resourceType(), acl.resourceName(), acl.patternType());
    }

    void handleAclsSnapshot(AclsImage image) {
        // Need to compare contents of image with all ACLs in ZK and issue updates
        Map<ResourcePattern, Set<AccessControlEntry>> allAclsInSnapshot = new HashMap<>();

        image.acls().values().forEach(standardAcl -> {
            ResourcePattern resourcePattern = resourcePatternFromAcl(standardAcl);
            allAclsInSnapshot.computeIfAbsent(resourcePattern, __ -> new HashSet<>()).add(
                new AccessControlEntry(standardAcl.principal(), standardAcl.host(), standardAcl.operation(), standardAcl.permissionType())
            );
        });

        Set<ResourcePattern> resourcesToDelete = new HashSet<>();
        Map<ResourcePattern, Set<AccessControlEntry>> changedResources = new HashMap<>();
        migrationClient.aclClient().iterateAcls((resourcePattern, accessControlEntries) -> {
            if (!allAclsInSnapshot.containsKey(resourcePattern)) {
                resourcesToDelete.add(resourcePattern);
            } else {
                Set<AccessControlEntry> snapshotEntries = allAclsInSnapshot.get(resourcePattern);
                if (!snapshotEntries.equals(accessControlEntries)) {
                    changedResources.put(resourcePattern, snapshotEntries);
                }
            }
        });

        resourcesToDelete.forEach(deletedResource -> {
            String name = "Deleting resource " + deletedResource + " which has no ACLs in snapshot";
            operationConsumer.accept(name, migrationState ->
                migrationClient.aclClient().deleteResource(deletedResource, migrationState));
        });

        changedResources.forEach((resourcePattern, accessControlEntries) -> {
            String name = "Writing " + accessControlEntries.size() + " for resource " + resourcePattern;
            operationConsumer.accept(name, migrationState ->
                migrationClient.aclClient().writeResourceAcls(resourcePattern, accessControlEntries, migrationState));
        });
    }

    void handleAclsDelta(AclsImage image, AclsDelta delta) {
        // Compute the resource patterns that were changed
        Set<ResourcePattern> resourcesWithChangedAcls = delta.changes().values()
            .stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(this::resourcePatternFromAcl)
            .collect(Collectors.toSet());

        Set<ResourcePattern> resourcesWithDeletedAcls = delta.deleted()
            .stream()
            .map(this::resourcePatternFromAcl)
            .collect(Collectors.toSet());

        // Need to collect all ACLs for any changed resource pattern
        Map<ResourcePattern, List<AccessControlEntry>> aclsToWrite = new HashMap<>();
        image.acls().forEach((uuid, standardAcl) -> {
            ResourcePattern resourcePattern = resourcePatternFromAcl(standardAcl);
            boolean removed = resourcesWithDeletedAcls.remove(resourcePattern);
            // If a resource pattern is present in the delta as a changed or deleted acl, need to include it
            if (resourcesWithChangedAcls.contains(resourcePattern) || removed) {
                aclsToWrite.computeIfAbsent(resourcePattern, __ -> new ArrayList<>()).add(
                    new AccessControlEntry(standardAcl.principal(), standardAcl.host(), standardAcl.operation(), standardAcl.permissionType())
                );
            }
        });

        resourcesWithDeletedAcls.forEach(deletedResource -> {
            String name = "Deleting resource " + deletedResource + " which has no more ACLs";
            operationConsumer.accept(name, migrationState ->
                migrationClient.aclClient().deleteResource(deletedResource, migrationState));
        });

        aclsToWrite.forEach((resourcePattern, accessControlEntries) -> {
            String name = "Writing " + accessControlEntries.size() + " for resource " + resourcePattern;
            operationConsumer.accept(name, migrationState ->
                migrationClient.aclClient().writeResourceAcls(resourcePattern, accessControlEntries, migrationState));
        });
    }
}
