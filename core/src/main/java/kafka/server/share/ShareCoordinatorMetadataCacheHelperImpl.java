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

package kafka.server.share;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.dlq.ShareGroupDLQMetadataCacheHelper;
import org.apache.kafka.server.share.persister.ShareCoordinatorMetadataCacheHelper;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntSupplier;

public class ShareCoordinatorMetadataCacheHelperImpl implements ShareCoordinatorMetadataCacheHelper, ShareGroupDLQMetadataCacheHelper {
    private final MetadataCache metadataCache;
    private final Function<SharePartitionKey, Integer> keyToPartitionMapper;
    private final ListenerName interBrokerListenerName;
    private final GroupConfigManager groupConfigManager;
    private final IntSupplier messageMaxBytesSupplier;
    private final Logger log = LoggerFactory.getLogger(ShareCoordinatorMetadataCacheHelperImpl.class);

    public ShareCoordinatorMetadataCacheHelperImpl(
        MetadataCache metadataCache,
        Function<SharePartitionKey, Integer> keyToPartitionMapper,
        ListenerName interBrokerListenerName,
        GroupConfigManager groupConfigManager,
        IntSupplier messageMaxBytesSupplier
    ) {
        this.metadataCache = Objects.requireNonNull(metadataCache, "metadataCache must not be null");
        this.keyToPartitionMapper = Objects.requireNonNull(keyToPartitionMapper, "keyToPartitionMapper must not be null");
        this.interBrokerListenerName = Objects.requireNonNull(interBrokerListenerName, "interBrokerListenerName must not be null");
        this.groupConfigManager = Objects.requireNonNull(groupConfigManager, "groupConfigManager must not be null");
        this.messageMaxBytesSupplier = Objects.requireNonNull(messageMaxBytesSupplier, "messageMaxBytesSupplier must not be null");
    }

    @Override
    public boolean containsTopic(String topic) {
        try {
            return metadataCache.contains(topic);
        } catch (Exception e) {
            log.warn("Exception checking {} in metadata cache", topic, e);
        }
        return false;
    }

    @Override
    public Optional<String> shareGroupDlqTopic(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::errorsDLQTopicName);
    }

    @Override
    public boolean isDlqAutoTopicCreateEnabled() {
        return groupConfigManager.isDlqAutoTopicCreateEnabled();
    }

    @Override
    public Optional<String> shareGroupDlqTopicPrefix() {
        return groupConfigManager.shareGroupDlqTopicPrefix();
    }

    @Override
    public boolean isDlqEnabledOnTopic(String topic) {
        Properties props = metadataCache.topicConfig(topic);
        if (props == null) {
            return false;
        }
        try {
            return new LogConfig(props).getBoolean(TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG);
        } catch (ConfigException exe) {
            return false;
        }
    }

    @Override
    public int dlqTopicMaxMessageBytes(String topic) {
        Properties props = metadataCache.topicConfig(topic);
        // LogConfig defines its own static default for this key, so an explicit containsKey
        // check is needed to distinguish "no topic-level override" from "override present"
        // and fall back to the broker's configured message.max.bytes in the former case.
        if (props == null || !props.containsKey(TopicConfig.MAX_MESSAGE_BYTES_CONFIG)) {
            return messageMaxBytesSupplier.getAsInt();
        }
        try {
            return new LogConfig(props).getInt(TopicConfig.MAX_MESSAGE_BYTES_CONFIG);
        } catch (ConfigException exe) {
            return messageMaxBytesSupplier.getAsInt();
        }
    }

    @Override
    public boolean isShareGroupDlqCopyRecordEnabled(String groupId) {
        Optional<GroupConfig> groupConfig = groupConfigManager.groupConfig(groupId);
        return groupConfig.map(GroupConfig::errorsDLQCopyRecordEnable).orElse(false);
    }

    @Override
    public Node getShareCoordinator(SharePartitionKey key, String internalTopicName) {
        try {
            if (metadataCache.contains(internalTopicName)) {
                Set<String> topicSet = new HashSet<>();
                topicSet.add(internalTopicName);

                List<MetadataResponseData.MetadataResponseTopic> topicMetadata = metadataCache.getTopicMetadata(
                    topicSet,
                    interBrokerListenerName,
                    false,
                    false
                );

                if (topicMetadata == null || topicMetadata.isEmpty() || topicMetadata.get(0).errorCode() != Errors.NONE.code()) {
                    return Node.noNode();
                } else {
                    int partition = keyToPartitionMapper.apply(key);
                    Optional<MetadataResponseData.MetadataResponsePartition> response = topicMetadata.get(0).partitions().stream()
                        .filter(responsePart -> responsePart.partitionIndex() == partition
                            && responsePart.leaderId() != MetadataResponse.NO_LEADER_ID)
                        .findFirst();

                    if (response.isPresent()) {
                        return metadataCache.getAliveBrokerNode(response.get().leaderId(), interBrokerListenerName)
                            .orElse(Node.noNode());
                    } else {
                        return Node.noNode();
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Exception while getting share coordinator.", e);
        }
        return Node.noNode();
    }

    @Override
    public List<Node> getClusterNodes() {
        try {
            return metadataCache.getAliveBrokerNodes(interBrokerListenerName);
        } catch (Exception e) {
            log.warn("Exception while getting cluster nodes.", e);
        }
        return List.of();
    }

    @Override
    public Optional<String> topicName(Uuid topicId) {
        try {
            return metadataCache.getTopicName(topicId);
        } catch (Exception e) {
            log.warn("Exception while fetching topic name.", e);
        }
        return Optional.empty();
    }

    @Override
    public TopicPartitionData topicPartitionData(String topicName) {
        Uuid topicId = metadataCache.getTopicId(topicName);
        Optional<Integer> numPartitions = metadataCache.numPartitions(topicName);
        List<Node> partitionLeaders = new ArrayList<>();

        if (numPartitions.isPresent()) {
            for (int i = 0; i < numPartitions.get(); i++) {
                partitionLeaders.add(metadataCache.getPartitionLeaderEndpoint(topicName, i, interBrokerListenerName).orElse(null));
            }
        }

        return new TopicPartitionData(
            topicName,
            numPartitions,
            Optional.ofNullable(topicId == Uuid.ZERO_UUID ? null : topicId),
            partitionLeaders
        );
    }
}
