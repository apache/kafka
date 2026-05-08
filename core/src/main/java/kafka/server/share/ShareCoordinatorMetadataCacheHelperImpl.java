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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.dlq.ShareGroupDLQMetadataCacheHelper;
import org.apache.kafka.server.share.persister.ShareCoordinatorMetadataCacheHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

public class ShareCoordinatorMetadataCacheHelperImpl implements ShareCoordinatorMetadataCacheHelper, ShareGroupDLQMetadataCacheHelper {
    private final MetadataCache metadataCache;
    private final Function<SharePartitionKey, Integer> keyToPartitionMapper;
    private final ListenerName interBrokerListenerName;
    private final Logger log = LoggerFactory.getLogger(ShareCoordinatorMetadataCacheHelperImpl.class);

    public ShareCoordinatorMetadataCacheHelperImpl(
        MetadataCache metadataCache,
        Function<SharePartitionKey, Integer> keyToPartitionMapper,
        ListenerName interBrokerListenerName
    ) {
        this.metadataCache = Objects.requireNonNull(metadataCache, "metadataCache must not be null");
        this.keyToPartitionMapper = Objects.requireNonNull(keyToPartitionMapper, "keyToPartitionMapper must not be null");
        this.interBrokerListenerName = Objects.requireNonNull(interBrokerListenerName, "interBrokerListenerName must not be null");
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
        Properties props = metadataCache.groupConfig(groupId);
        if (props == null || props.isEmpty()) {
            return Optional.empty();
        }
        Object topicName = props.get(GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG);
        if (topicName instanceof String) {
            return Optional.of((String) topicName);
        }
        return Optional.empty();
    }

    @Override
    public boolean isDlqAutoTopicCreateEnabled() {
        Optional<Integer> someBrokerId = metadataCache.getRandomAliveBrokerId();
        if (someBrokerId.isEmpty() || someBrokerId.get() < 0) {
            return false;
        }
        Properties props = metadataCache.brokerConfig(someBrokerId.get());

        if (props == null || props.isEmpty()) {
            return false;
        }
        Object isEnabled = props.get(GroupCoordinatorConfig.ERRORS_DEADLETTERQUEUE_AUTO_CREATE_TOPICS_ENABLE_CONFIG);
        if (isEnabled instanceof Boolean) {
            return (boolean)  isEnabled;
        }
        return false;
    }

    @Override
    public Optional<String> shareGroupDlqTopicPrefix() {
        Optional<Integer> someBrokerId = metadataCache.getRandomAliveBrokerId();
        if (someBrokerId.isEmpty() || someBrokerId.get() < 0) {
            return Optional.empty();
        }
        Properties props = metadataCache.brokerConfig(someBrokerId.get());

        if (props == null || props.isEmpty()) {
            return Optional.empty();
        }
        Object topicPrefix = props.get(GroupCoordinatorConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_PREFIX_CONFIG);
        if (topicPrefix instanceof String) {
            return Optional.of((String) topicPrefix);
        }
        return Optional.empty();
    }

    @Override
    public boolean isDlqEnabledOnTopic(String topic) {
        Properties props = metadataCache.topicConfig(topic);
        if (props == null || props.isEmpty()) {
            return false;
        }
        Object isEnabled = props.get(TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG);
        if (isEnabled instanceof Boolean) {
            return (boolean) isEnabled;
        }
        return false;
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
            log.warn("Exception while getting share coordinator", e);
        }
        return Node.noNode();
    }

    @Override
    public List<Node> getClusterNodes() {
        try {
            return metadataCache.getAliveBrokerNodes(interBrokerListenerName);
        } catch (Exception e) {
            log.warn("Exception while getting cluster nodes", e);
        }
        return List.of();
    }
}
