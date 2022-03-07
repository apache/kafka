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
import org.apache.kafka.common.metadata.RemoveFeatureLevelRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Iterator;
import java.util.List;


/**
 * A change to the broker metadata image.
 *
 * This class is thread-safe.
 */
public final class MetadataDelta {
    private final MetadataImage image;

    private long highestOffset;

    private int highestEpoch;

    private FeaturesDelta featuresDelta = null;

    private ClusterDelta clusterDelta = null;

    private TopicsDelta topicsDelta = null;

    private ConfigurationsDelta configsDelta = null;

    private ClientQuotasDelta clientQuotasDelta = null;

    private ProducerIdsDelta producerIdsDelta = null;

    private AclsDelta aclsDelta = null;

    public MetadataDelta(MetadataImage image) {
        this.image = image;
        this.highestOffset = image.highestOffsetAndEpoch().offset;
        this.highestEpoch = image.highestOffsetAndEpoch().epoch;
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

    public void read(long highestOffset, int highestEpoch, Iterator<List<ApiMessageAndVersion>> reader) {
        while (reader.hasNext()) {
            List<ApiMessageAndVersion> batch = reader.next();
            for (ApiMessageAndVersion messageAndVersion : batch) {
                replay(highestOffset, highestEpoch, messageAndVersion.message());
            }
        }
    }

    public void replay(long offset, int epoch, ApiMessage record) {
        highestOffset = offset;
        highestEpoch = epoch;

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
            case REMOVE_FEATURE_LEVEL_RECORD:
                replay((RemoveFeatureLevelRecord) record);
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
            default:
                throw new RuntimeException("Unknown metadata record type " + type);
        }
    }

    public void replay(RegisterBrokerRecord record) {
        if (clusterDelta == null) clusterDelta = new ClusterDelta(image.cluster());
        clusterDelta.replay(record);
    }

    public void replay(UnregisterBrokerRecord record) {
        if (clusterDelta == null) clusterDelta = new ClusterDelta(image.cluster());
        clusterDelta.replay(record);
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
        getOrCreateTopicsDelta().replay(record);
        String topicName = topicsDelta.replay(record);
        getOrCreateConfigsDelta().replay(record, topicName);
    }

    public void replay(FeatureLevelRecord record) {
        getOrCreateFeaturesDelta().replay(record);
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

    public void replay(RemoveFeatureLevelRecord record) {
        getOrCreateFeaturesDelta().replay(record);
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

    public MetadataImage apply() {
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
            new OffsetAndEpoch(highestOffset, highestEpoch),
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
            "highestOffset=" + highestOffset +
            ", highestEpoch=" + highestEpoch +
            ", featuresDelta=" + featuresDelta +
            ", clusterDelta=" + clusterDelta +
            ", topicsDelta=" + topicsDelta +
            ", configsDelta=" + configsDelta +
            ", clientQuotasDelta=" + clientQuotasDelta +
            ", producerIdsDelta=" + producerIdsDelta +
            ", aclsDelta=" + aclsDelta +
            ')';
    }
}
