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

import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveFeatureLevelRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
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

    private FeaturesDelta featuresDelta = null;

    private ClusterDelta clusterDelta = null;

    private TopicsDelta topicsDelta = null;

    private ConfigurationsDelta configsDelta = null;

    private ClientQuotasDelta clientQuotasDelta = null;

    public MetadataDelta(MetadataImage image) {
        this.image = image;
    }

    public MetadataImage image() {
        return image;
    }

    public FeaturesDelta featuresDelta() {
        return featuresDelta;
    }

    public ClusterDelta clusterDelta() {
        return clusterDelta;
    }

    public TopicsDelta topicsDelta() {
        return topicsDelta;
    }

    public ConfigurationsDelta configsDelta() {
        return configsDelta;
    }

    public ClientQuotasDelta clientQuotasDelta() {
        return clientQuotasDelta;
    }

    public void read(Iterator<List<ApiMessageAndVersion>> reader) {
        while (reader.hasNext()) {
            List<ApiMessageAndVersion> batch = reader.next();
            for (ApiMessageAndVersion messageAndVersion : batch) {
                replay(messageAndVersion.message());
            }
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
            case FEATURE_LEVEL_RECORD:
                replay((FeatureLevelRecord) record);
                break;
            case CLIENT_QUOTA_RECORD:
                replay((ClientQuotaRecord) record);
                break;
            case PRODUCER_IDS_RECORD:
                // Nothing to do.
                break;
            case REMOVE_FEATURE_LEVEL_RECORD:
                replay((RemoveFeatureLevelRecord) record);
                break;
            case BROKER_REGISTRATION_CHANGE_RECORD:
                replay((BrokerRegistrationChangeRecord) record);
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
        if (topicsDelta == null) topicsDelta = new TopicsDelta(image.topics());
        topicsDelta.replay(record);
    }

    public void replay(PartitionRecord record) {
        if (topicsDelta == null) topicsDelta = new TopicsDelta(image.topics());
        topicsDelta.replay(record);
    }

    public void replay(ConfigRecord record) {
        if (configsDelta == null) configsDelta = new ConfigurationsDelta(image.configs());
        configsDelta.replay(record);
    }

    public void replay(PartitionChangeRecord record) {
        if (topicsDelta == null) topicsDelta = new TopicsDelta(image.topics());
        topicsDelta.replay(record);
    }

    public void replay(FenceBrokerRecord record) {
        if (clusterDelta == null) clusterDelta = new ClusterDelta(image.cluster());
        clusterDelta.replay(record);
    }

    public void replay(UnfenceBrokerRecord record) {
        if (clusterDelta == null) clusterDelta = new ClusterDelta(image.cluster());
        clusterDelta.replay(record);
    }

    public void replay(RemoveTopicRecord record) {
        if (topicsDelta == null) topicsDelta = new TopicsDelta(image.topics());
        String topicName = topicsDelta.replay(record);
        if (configsDelta == null) configsDelta = new ConfigurationsDelta(image.configs());
        configsDelta.replay(record, topicName);
    }

    public void replay(FeatureLevelRecord record) {
        if (featuresDelta == null) featuresDelta = new FeaturesDelta(image.features());
        featuresDelta.replay(record);
    }

    public void replay(BrokerRegistrationChangeRecord record) {
        if (clusterDelta == null) clusterDelta = new ClusterDelta(image.cluster());
        clusterDelta.replay(record);
    }

    public void replay(ClientQuotaRecord record) {
        if (clientQuotasDelta == null) clientQuotasDelta = new ClientQuotasDelta(image.clientQuotas());
        clientQuotasDelta.replay(record);
    }

    public void replay(RemoveFeatureLevelRecord record) {
        if (featuresDelta == null) featuresDelta = new FeaturesDelta(image.features());
        featuresDelta.replay(record);
    }

    /**
     * Create removal deltas for anything which was in the base image, but which was not
     * referenced in the snapshot records we just applied.
     */
    public void finishSnapshot() {
        if (featuresDelta != null) featuresDelta.finishSnapshot();
        if (clusterDelta != null) clusterDelta.finishSnapshot();
        if (topicsDelta != null) topicsDelta.finishSnapshot();
        if (configsDelta != null) configsDelta.finishSnapshot();
        if (clientQuotasDelta != null) clientQuotasDelta.finishSnapshot();
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
        return new MetadataImage(newFeatures, newCluster, newTopics, newConfigs,
            newClientQuotas);
    }

    @Override
    public String toString() {
        return "MetadataDelta(" +
            "featuresDelta=" + featuresDelta +
            ", clusterDelta=" + clusterDelta +
            ", topicsDelta=" + topicsDelta +
            ", configsDelta=" + configsDelta +
            ", clientQuotasDelta=" + clientQuotasDelta +
            ')';
    }
}
