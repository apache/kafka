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

package org.apache.kafka.image;

import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.DelegationTokenRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterControllerRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveDelegationTokenRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.RemoveUserScramCredentialRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Optional;


/**
 * A change to the broker metadata image.
 */
public final class MetadataDelta {
    public static class Builder {
        private MetadataImage image = MetadataImage.EMPTY;

        public Builder setImage(MetadataImage image) {
            this.image = image;
            return this;
        }

        public MetadataDelta build() {
            return new MetadataDelta(image);
        }
    }

    private final MetadataImage image;

    private FeaturesDelta featuresDelta = null;

    private ClusterDelta clusterDelta = null;

    private TopicsDelta topicsDelta = null;

    private ConfigurationsDelta configsDelta = null;

    private ClientQuotasDelta clientQuotasDelta = null;

    private ProducerIdsDelta producerIdsDelta = null;

    private AclsDelta aclsDelta = null;

    private ScramDelta scramDelta = null;

    private DelegationTokenDelta delegationTokenDelta = null;

    public MetadataDelta(MetadataImage image) {
        this.image = image;
    }

    public MetadataImage image() {
        return image;
    }

    public FeaturesDelta featuresDelta() {
        return featuresDelta;
    }

    public FeaturesDelta getOrCreateFeaturesDelta() {
        if (featuresDelta == null) featuresDelta = new FeaturesDelta(image.features());
        return featuresDelta;
    }

    public ClusterDelta clusterDelta() {
        return clusterDelta;
    }

    public ClusterDelta getOrCreateClusterDelta() {
        if (clusterDelta == null) clusterDelta = new ClusterDelta(image.cluster());
        return clusterDelta;
    }

    public TopicsDelta topicsDelta() {
        return topicsDelta;
    }

    public TopicsDelta getOrCreateTopicsDelta() {
        if (topicsDelta == null) topicsDelta = new TopicsDelta(image.topics());
        return topicsDelta;
    }

    public ConfigurationsDelta configsDelta() {
        return configsDelta;
    }

    public ConfigurationsDelta getOrCreateConfigsDelta() {
        if (configsDelta == null) configsDelta = new ConfigurationsDelta(image.configs());
        return configsDelta;
    }

    public ClientQuotasDelta clientQuotasDelta() {
        return clientQuotasDelta;
    }

    public ClientQuotasDelta getOrCreateClientQuotasDelta() {
        if (clientQuotasDelta == null) clientQuotasDelta = new ClientQuotasDelta(image.clientQuotas());
        return clientQuotasDelta;
    }

    public ProducerIdsDelta producerIdsDelta() {
        return producerIdsDelta;
    }

    public ProducerIdsDelta getOrCreateProducerIdsDelta() {
        if (producerIdsDelta == null) {
            producerIdsDelta = new ProducerIdsDelta(image.producerIds());
        }
        return producerIdsDelta;
    }

    public AclsDelta aclsDelta() {
        return aclsDelta;
    }

    public AclsDelta getOrCreateAclsDelta() {
        if (aclsDelta == null) aclsDelta = new AclsDelta(image.acls());
        return aclsDelta;
    }

    public ScramDelta scramDelta() {
        return scramDelta;
    }

    public ScramDelta getOrCreateScramDelta() {
        if (scramDelta == null) scramDelta = new ScramDelta(image.scram());
        return scramDelta;
    }

    public DelegationTokenDelta delegationTokenDelta() {
        return delegationTokenDelta;
    }

    public DelegationTokenDelta getOrCreateDelegationTokenDelta() {
        if (delegationTokenDelta == null) delegationTokenDelta = new DelegationTokenDelta(image.delegationTokens());
        return delegationTokenDelta;
    }

    public Optional<MetadataVersion> metadataVersionChanged() {
        if (featuresDelta == null) {
            return Optional.empty();
        } else {
            return featuresDelta.metadataVersionChange();
        }
    }

    public void replay(ApiMessage record) {
        MetadataRecordType type = MetadataRecordType.fromId(record.apiKey());
        switch (type) {
            case REGISTER_BROKER_RECORD:
                replay((RegisterBrokerRecord) record);
                break;
            case UNREGISTER_BROKER_RECORD:
                replay((UnregisterBrokerRecord) record);
                break;
            case TOPIC_RECORD:
                replay((TopicRecord) record);
                break;
            case PARTITION_RECORD:
                replay((PartitionRecord) record);
                break;
            case CONFIG_RECORD:
                replay((ConfigRecord) record);
                break;
            case PARTITION_CHANGE_RECORD:
                replay((PartitionChangeRecord) record);
                break;
            case FENCE_BROKER_RECORD:
                replay((FenceBrokerRecord) record);
                break;
            case UNFENCE_BROKER_RECORD:
                replay((UnfenceBrokerRecord) record);
                break;
            case REMOVE_TOPIC_RECORD:
                replay((RemoveTopicRecord) record);
                break;
            case DELEGATION_TOKEN_RECORD:
                replay((DelegationTokenRecord) record);
                break;
            case USER_SCRAM_CREDENTIAL_RECORD:
                replay((UserScramCredentialRecord) record);
                break;
            case FEATURE_LEVEL_RECORD:
                replay((FeatureLevelRecord) record);
                break;
            case CLIENT_QUOTA_RECORD:
                replay((ClientQuotaRecord) record);
                break;
            case PRODUCER_IDS_RECORD:
                replay((ProducerIdsRecord) record);
                break;
            case BROKER_REGISTRATION_CHANGE_RECORD:
                replay((BrokerRegistrationChangeRecord) record);
                break;
            case ACCESS_CONTROL_ENTRY_RECORD:
                replay((AccessControlEntryRecord) record);
                break;
            case REMOVE_ACCESS_CONTROL_ENTRY_RECORD:
                replay((RemoveAccessControlEntryRecord) record);
                break;
            case REMOVE_USER_SCRAM_CREDENTIAL_RECORD:
                replay((RemoveUserScramCredentialRecord) record);
                break;
            case REMOVE_DELEGATION_TOKEN_RECORD:
                replay((RemoveDelegationTokenRecord) record);
                break;
            case NO_OP_RECORD:
                /* NoOpRecord is an empty record and doesn't need to be replayed beyond
                 * updating the highest offset and epoch.
                 */
                break;
            case ZK_MIGRATION_STATE_RECORD:
                replay((ZkMigrationStateRecord) record);
                break;
            case REGISTER_CONTROLLER_RECORD:
                replay((RegisterControllerRecord) record);
                break;
            default:
                throw new RuntimeException("Unknown metadata record type " + type);
        }
    }

    public void replay(RegisterBrokerRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    public void replay(UnregisterBrokerRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    public void replay(TopicRecord record) {
        getOrCreateTopicsDelta().replay(record);
    }

    public void replay(PartitionRecord record) {
        getOrCreateTopicsDelta().replay(record);
    }

    public void replay(ConfigRecord record) {
        getOrCreateConfigsDelta().replay(record);
    }

    public void replay(PartitionChangeRecord record) {
        getOrCreateTopicsDelta().replay(record);
    }

    public void replay(FenceBrokerRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    public void replay(UnfenceBrokerRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    public void replay(RemoveTopicRecord record) {
        String topicName = getOrCreateTopicsDelta().replay(record);
        getOrCreateConfigsDelta().replay(record, topicName);
    }

    public void replay(DelegationTokenRecord record) {
        getOrCreateDelegationTokenDelta().replay(record);
    }

    public void replay(RemoveDelegationTokenRecord record) {
        getOrCreateDelegationTokenDelta().replay(record);
    }

    public void replay(UserScramCredentialRecord record) {
        getOrCreateScramDelta().replay(record);
    }

    public void replay(FeatureLevelRecord record) {
        getOrCreateFeaturesDelta().replay(record);
        featuresDelta.metadataVersionChange().ifPresent(changedMetadataVersion -> {
            // If any feature flags change, need to immediately check if any metadata needs to be downgraded.
            getOrCreateClusterDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateConfigsDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateTopicsDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateClientQuotasDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateProducerIdsDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateAclsDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateScramDelta().handleMetadataVersionChange(changedMetadataVersion);
            getOrCreateDelegationTokenDelta().handleMetadataVersionChange(changedMetadataVersion);
        });
    }

    public void replay(BrokerRegistrationChangeRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    public void replay(ClientQuotaRecord record) {
        getOrCreateClientQuotasDelta().replay(record);
    }

    public void replay(ProducerIdsRecord record) {
        getOrCreateProducerIdsDelta().replay(record);
    }

    public void replay(AccessControlEntryRecord record) {
        getOrCreateAclsDelta().replay(record);
    }

    public void replay(RemoveAccessControlEntryRecord record) {
        getOrCreateAclsDelta().replay(record);
    }

    public void replay(RemoveUserScramCredentialRecord record) {
        getOrCreateScramDelta().replay(record);
    }

    public void replay(ZkMigrationStateRecord record) {
        getOrCreateFeaturesDelta().replay(record);
    }

    public void replay(RegisterControllerRecord record) {
        getOrCreateClusterDelta().replay(record);
    }

    /**
     * Create removal deltas for anything which was in the base image, but which was not
     * referenced in the snapshot records we just applied.
     */
    public void finishSnapshot() {
        getOrCreateFeaturesDelta().finishSnapshot();
        getOrCreateClusterDelta().finishSnapshot();
        getOrCreateTopicsDelta().finishSnapshot();
        getOrCreateConfigsDelta().finishSnapshot();
        getOrCreateClientQuotasDelta().finishSnapshot();
        getOrCreateProducerIdsDelta().finishSnapshot();
        getOrCreateAclsDelta().finishSnapshot();
        getOrCreateScramDelta().finishSnapshot();
        getOrCreateDelegationTokenDelta().finishSnapshot();
    }

    public MetadataImage apply(MetadataProvenance provenance) {
        FeaturesImage newFeatures;
        if (featuresDelta == null) {
            newFeatures = image.features();
        } else {
            newFeatures = featuresDelta.apply();
        }
        ClusterImage newCluster;
        if (clusterDelta == null) {
            newCluster = image.cluster();
        } else {
            newCluster = clusterDelta.apply();
        }
        TopicsImage newTopics;
        if (topicsDelta == null) {
            newTopics = image.topics();
        } else {
            newTopics = topicsDelta.apply();
        }
        ConfigurationsImage newConfigs;
        if (configsDelta == null) {
            newConfigs = image.configs();
        } else {
            newConfigs = configsDelta.apply();
        }
        ClientQuotasImage newClientQuotas;
        if (clientQuotasDelta == null) {
            newClientQuotas = image.clientQuotas();
        } else {
            newClientQuotas = clientQuotasDelta.apply();
        }
        ProducerIdsImage newProducerIds;
        if (producerIdsDelta == null) {
            newProducerIds = image.producerIds();
        } else {
            newProducerIds = producerIdsDelta.apply();
        }
        AclsImage newAcls;
        if (aclsDelta == null) {
            newAcls = image.acls();
        } else {
            newAcls = aclsDelta.apply();
        }
        ScramImage newScram;
        if (scramDelta == null) {
            newScram = image.scram();
        } else {
            newScram = scramDelta.apply();
        }
        DelegationTokenImage newDelegationTokens;
        if (delegationTokenDelta == null) {
            newDelegationTokens = image.delegationTokens();
        } else {
            newDelegationTokens = delegationTokenDelta.apply();
        }
        return new MetadataImage(
            provenance,
            newFeatures,
            newCluster,
            newTopics,
            newConfigs,
            newClientQuotas,
            newProducerIds,
            newAcls,
            newScram,
            newDelegationTokens
        );
    }

    @Override
    public String toString() {
        return "MetadataDelta(" +
            "featuresDelta=" + featuresDelta +
            ", clusterDelta=" + clusterDelta +
            ", topicsDelta=" + topicsDelta +
            ", configsDelta=" + configsDelta +
            ", clientQuotasDelta=" + clientQuotasDelta +
            ", producerIdsDelta=" + producerIdsDelta +
            ", aclsDelta=" + aclsDelta +
            ", scramDelta=" + scramDelta +
            ", delegationTokenDelta=" + delegationTokenDelta +
            ')';
    }
}
