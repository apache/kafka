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
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Optional;


/**
 * A change to the broker metadata image.
 */
public final class MetadataDelta {
    public static class Builder {
        private MetadataImage image = MetadataImage.EMPTY;
        private MetadataVersion metadataVersion = null;

        public Builder setImage(MetadataImage image) {
            this.image = image;
            return this;
        }

        public Builder setMetadataVersion(MetadataVersion metadataVersion) {
            this.metadataVersion = metadataVersion;
            return this;
        }

        public MetadataDelta build() {
            if (metadataVersion == null) metadataVersion = image.features().metadataVersion();
            return new MetadataDelta(image,
                    metadataVersion);
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

    private MetadataDelta(
        MetadataImage image,
        MetadataVersion metadataVersion
    ) {
        this.image = image;
        if (!image.features().metadataVersion().equals(metadataVersion)) {
            getOrCreateFeaturesDelta().replayMetadataVersionChange(metadataVersion);
        }
    }

    public void clear() {
        this.featuresDelta = null;
        this.clusterDelta = null;
        this.topicsDelta = null;
        this.configsDelta = null;
        this.clientQuotasDelta = null;
        this.producerIdsDelta = null;
        this.aclsDelta = null;
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
            case NO_OP_RECORD:
                /* NoOpRecord is an empty record and doesn't need to be replayed beyond
                 * updating the highest offset and epoch.
                 */
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

    public void setMetadataVersion(MetadataVersion metadataVersion) {
        getOrCreateFeaturesDelta().replay(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(metadataVersion.featureLevel()));
    }

    public void replay(FeatureLevelRecord record) {
        FeaturesDelta delta = getOrCreateFeaturesDelta();
        Optional<MetadataVersionChange> change = delta.calculateMetadataVersionChange(record);
        if (change.isPresent()) {
            //
            // If this record would change the metadata version, and the base image is not empty,
            // we throw an exception here. The caller is expected to handle this exception by saving
            // the image to a series of records with the new metadata version, and then reloading it.
            //
            // The rationale for handling it "out-of-band" like this is that:
            // 1) the broker and controller need to trigger a save of the metadata image right
            // before any version change, so we need to interrupt the normal control flow anyway.
            // 2) it is easier to maintain one downgrade/upgrade code path in the image write
            // functions than to try to handle every conceivable change that a metadata version
            // could bring in an event-driven fashion.
            // 3) metadata version changes are rare so the overhead of this is not a problem.
            //
            throw new MetadataVersionChangeException(change.get());
        }
        delta.replay(record);
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
        return new MetadataImage(
            provenance,
            newFeatures,
            newCluster,
            newTopics,
            newConfigs,
            newClientQuotas,
            newProducerIds,
            newAcls
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
            ')';
    }
}
