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
package org.apache.kafka.common.requests;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicResolver;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OffsetFetchRequest extends AbstractRequest {

    private static final Logger log = LoggerFactory.getLogger(OffsetFetchRequest.class);

    private static final List<OffsetFetchRequestTopic> ALL_TOPIC_PARTITIONS = null;
    private static final List<OffsetFetchRequestTopics> ALL_TOPIC_PARTITIONS_BATCH = null;
    private final OffsetFetchRequestData data;

    public static class Builder extends AbstractRequest.Builder<OffsetFetchRequest> {

        public final OffsetFetchRequestData data;
        private final boolean throwOnFetchStableOffsetsUnsupported;
        private final TopicResolver topicResolver;
        private boolean canUseTopicIds = true;

        public Builder(String groupId,
                       boolean requireStable,
                       List<TopicPartition> partitions,
                       boolean throwOnFetchStableOffsetsUnsupported,
                       TopicResolver topicResolver) {
            super(ApiKeys.OFFSET_FETCH);
            this.topicResolver = topicResolver;

            final List<OffsetFetchRequestTopic> topics;
            if (partitions != null) {
                Map<String, OffsetFetchRequestTopic> offsetFetchRequestTopicMap = new HashMap<>();
                for (TopicPartition topicPartition : partitions) {
                    String topicName = topicPartition.topic();
                    OffsetFetchRequestTopic topic = offsetFetchRequestTopicMap.getOrDefault(
                        topicName, new OffsetFetchRequestTopic().setName(topicName));
                    topic.partitionIndexes().add(topicPartition.partition());
                    offsetFetchRequestTopicMap.put(topicName, topic);
                }
                topics = new ArrayList<>(offsetFetchRequestTopicMap.values());
            } else {
                // If passed in partition list is null, it is requesting offsets for all topic partitions.
                topics = ALL_TOPIC_PARTITIONS;
            }

            this.data = new OffsetFetchRequestData()
                            .setGroupId(groupId)
                            .setRequireStable(requireStable)
                            .setTopics(topics);
            this.throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported;
        }

        boolean isAllTopicPartitions() {
            return this.data.topics() == ALL_TOPIC_PARTITIONS;
        }

        public Builder(Map<String, List<TopicPartition>> groupIdToTopicPartitionMap,
                       boolean requireStable,
                       boolean throwOnFetchStableOffsetsUnsupported,
                       TopicResolver topicResolver) {
            super(ApiKeys.OFFSET_FETCH);
            this.topicResolver = topicResolver;

            List<OffsetFetchRequestGroup> groups = new ArrayList<>();
            for (Entry<String, List<TopicPartition>> entry : groupIdToTopicPartitionMap.entrySet()) {
                String groupName = entry.getKey();
                List<TopicPartition> tpList = entry.getValue();
                final List<OffsetFetchRequestTopics> topics;
                if (tpList != null) {
                    Map<String, OffsetFetchRequestTopics> offsetFetchRequestTopicMap =
                        new HashMap<>();
                    for (TopicPartition topicPartition : tpList) {
                        String topicName = topicPartition.topic();
                        Uuid topicId = resolveTopicId(topicName);
                        OffsetFetchRequestTopics topic = offsetFetchRequestTopicMap.getOrDefault(
                            topicName, new OffsetFetchRequestTopics().setName(topicName).setTopicId(topicId));
                        topic.partitionIndexes().add(topicPartition.partition());
                        offsetFetchRequestTopicMap.put(topicName, topic);
                    }
                    topics = new ArrayList<>(offsetFetchRequestTopicMap.values());
                } else {
                    topics = ALL_TOPIC_PARTITIONS_BATCH;
                }
                groups.add(new OffsetFetchRequestGroup()
                    .setGroupId(groupName)
                    .setTopics(topics));
            }
            this.data = new OffsetFetchRequestData()
                .setGroups(groups)
                .setRequireStable(requireStable);
            this.throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported;
        }

        @Override
        public OffsetFetchRequest build(short version) {
            if (isAllTopicPartitions() && version < 2) {
                throw new UnsupportedVersionException("The broker only supports OffsetFetchRequest " +
                    "v" + version + ", but we need v2 or newer to request all topic partitions.");
            }
            if (data.groups().size() > 1 && version < 8) {
                throw new NoBatchedOffsetFetchRequestException("Broker does not support"
                    + " batching groups for fetch offset request on version " + version);
            }
            if (data.requireStable() && version < 7) {
                if (throwOnFetchStableOffsetsUnsupported) {
                    throw new UnsupportedVersionException("Broker unexpectedly " +
                        "doesn't support requireStable flag on version " + version);
                } else {
                    log.trace("Fallback the requireStable flag to false as broker " +
                        "only supports OffsetFetchRequest version {}. Need " +
                        "v7 or newer to enable this feature", version);
                    data.setRequireStable(false);
                }
            }
            OffsetFetchRequestData requestData;

            // convert data to use the appropriate version since version 8 uses different format
            if (version < 8 && !data.groups().isEmpty()) {
                requestData = convertToV7AndBelow(data);
            } else if (version >= 8 && data.groups().isEmpty()) {
                requestData = convertToV8AndAbove(data);
            } else {
                requestData = data;
            }

            // canUseTopicIds <=> version >= 9 && all topics have a valid (non-zero) topic id.
            canUseTopicIds &= version >= 9;
            short requestVersion = canUseTopicIds ? version : (short) Math.min(version, 8);
            return new OffsetFetchRequest(requestData, requestVersion);
        }

        private Uuid resolveTopicId(String topicName) {
            Uuid topicId = topicResolver.getTopicId(topicName).orElse(Uuid.ZERO_UUID);
            if (topicId.equals(Uuid.ZERO_UUID)) {
                canUseTopicIds = false;
            }
            return topicId;
        }

        private OffsetFetchRequestData convertToV8AndAbove(OffsetFetchRequestData original) {
            String groupName = original.groupId();
            List<OffsetFetchRequestTopic> oldFormatTopics = original.topics();
            List<OffsetFetchRequestTopics> topics = null;
            if (oldFormatTopics != null) {
                topics = oldFormatTopics
                    .stream()
                    .map(t -> {
                        Uuid topicId = resolveTopicId(t.name());
                        return new OffsetFetchRequestTopics()
                            .setName(t.name())
                            .setTopicId(topicId)
                            .setPartitionIndexes(t.partitionIndexes());
                    })
                    .collect(Collectors.toList());
            }
            return new OffsetFetchRequestData()
                .setGroups(Collections.singletonList(
                    new OffsetFetchRequestGroup()
                        .setGroupId(groupName)
                        .setTopics(topics)))
                .setRequireStable(original.requireStable());
        }

        private static OffsetFetchRequestData convertToV7AndBelow(OffsetFetchRequestData original) {
            OffsetFetchRequestGroup group = original.groups().get(0);
            String groupName = group.groupId();
            List<OffsetFetchRequestTopics> topics = group.topics();
            List<OffsetFetchRequestTopic> oldFormatTopics = null;
            if (topics != null) {
                oldFormatTopics = topics
                    .stream()
                    .map(t ->
                        new OffsetFetchRequestTopic()
                            .setName(t.name())
                            .setPartitionIndexes(t.partitionIndexes()))
                    .collect(Collectors.toList());
            }
            OffsetFetchRequestData oldDataFormat = new OffsetFetchRequestData()
                .setGroupId(groupName)
                .setTopics(oldFormatTopics)
                .setRequireStable(original.requireStable());
            return oldDataFormat;
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }


    /**
     * Indicates that it is not possible to fetch consumer groups in batches with FetchOffset.
     * Instead consumer groups' offsets must be fetched one by one.
     */
    public static class NoBatchedOffsetFetchRequestException extends UnsupportedVersionException {
        private static final long serialVersionUID = 1L;

        public NoBatchedOffsetFetchRequestException(String message) {
            super(message);
        }
    }

    public List<TopicPartition> partitions() {
        if (isAllPartitions()) {
            return null;
        }
        List<TopicPartition> partitions = new ArrayList<>();
        for (OffsetFetchRequestTopic topic : data.topics()) {
            for (Integer partitionIndex : topic.partitionIndexes()) {
                partitions.add(new TopicPartition(topic.name(), partitionIndex));
            }
        }
        return partitions;
    }

    public String groupId() {
        return data.groupId();
    }

    public boolean requireStable() {
        return data.requireStable();
    }

    public List<OffsetFetchRequestData.OffsetFetchRequestGroup> groups() {
        if (version() >= 8) {
            return data.groups();
        } else {
            OffsetFetchRequestData.OffsetFetchRequestGroup group =
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId(data.groupId());

            if (data.topics() == null) {
                // If topics is null, it means that all topic-partitions should
                // be fetched hence we preserve it.
                group.setTopics(null);
            } else {
                // Otherwise, topics are translated to the new structure.
                data.topics().forEach(topic -> {
                    group.topics().add(new OffsetFetchRequestTopics()
                        .setName(topic.name())
                        .setPartitionIndexes(topic.partitionIndexes())
                    );
                });
            }

            return Collections.singletonList(group);
        }
    }

    public List<String> groupIds() {
        return data.groups()
            .stream()
            .map(OffsetFetchRequestGroup::groupId)
            .collect(Collectors.toList());
    }

    // Visible for tests.
    OffsetFetchRequest(OffsetFetchRequestData data, short version) {
        super(ApiKeys.OFFSET_FETCH, version);
        this.data = data;
    }

    public OffsetFetchResponse getErrorResponse(Errors error) {
        return getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, error);
    }

    public OffsetFetchResponse getErrorResponse(int throttleTimeMs, Errors error) {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> responsePartitions = new HashMap<>();
        if (version() < 2) {
            OffsetFetchResponse.PartitionData partitionError = new OffsetFetchResponse.PartitionData(
                    OffsetFetchResponse.INVALID_OFFSET,
                    Optional.empty(),
                    OffsetFetchResponse.NO_METADATA,
                    error);

            for (OffsetFetchRequestTopic topic : this.data.topics()) {
                for (int partitionIndex : topic.partitionIndexes()) {
                    responsePartitions.put(
                        new TopicPartition(topic.name(), partitionIndex), partitionError);
                }
            }
            return new OffsetFetchResponse(error, responsePartitions);
        }
        if (version() == 2) {
            return new OffsetFetchResponse(error, responsePartitions);
        }
        if (version() >= 3 && version() < 8) {
            return new OffsetFetchResponse(throttleTimeMs, error, responsePartitions);
        }
        List<String> groupIds = groupIds();
        Map<String, Errors> errorsMap = new HashMap<>(groupIds.size());
        Map<String, Map<TopicPartition, OffsetFetchResponse.PartitionData>> partitionMap =
            new HashMap<>(groupIds.size());
        for (String g : groupIds) {
            errorsMap.put(g, error);
            partitionMap.put(g, responsePartitions);
        }
        return new OffsetFetchResponse(throttleTimeMs, errorsMap, partitionMap);
    }

    @Override
    public OffsetFetchResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return getErrorResponse(throttleTimeMs, Errors.forException(e));
    }

    public static OffsetFetchRequest parse(ByteBuffer buffer, short version) {
        return new OffsetFetchRequest(new OffsetFetchRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public boolean isAllPartitions() {
        return data.topics() == ALL_TOPIC_PARTITIONS;
    }

    @Override
    public OffsetFetchRequestData data() {
        return data;
    }
}
