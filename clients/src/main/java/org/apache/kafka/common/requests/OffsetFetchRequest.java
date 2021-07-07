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

        public Builder(String groupId,
                       boolean requireStable,
                       List<TopicPartition> partitions,
                       boolean throwOnFetchStableOffsetsUnsupported) {
            super(ApiKeys.OFFSET_FETCH);

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
                       boolean throwOnFetchStableOffsetsUnsupported) {
            super(ApiKeys.OFFSET_FETCH);

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
                        OffsetFetchRequestTopics topic = offsetFetchRequestTopicMap.getOrDefault(
                            topicName, new OffsetFetchRequestTopics().setName(topicName));
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
            // convert data to use the appropriate version since version 8 uses different format
            if (version < 8) {
                OffsetFetchRequestData oldDataFormat = null;
                if (!data.groups().isEmpty()) {
                    OffsetFetchRequestGroup group = data.groups().get(0);
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
                    oldDataFormat = new OffsetFetchRequestData()
                        .setGroupId(groupName)
                        .setTopics(oldFormatTopics)
                        .setRequireStable(data.requireStable());
                }
                return new OffsetFetchRequest(oldDataFormat == null ? data : oldDataFormat, version);
            } else {
                if (data.groups().isEmpty()) {
                    String groupName = data.groupId();
                    List<OffsetFetchRequestTopic> oldFormatTopics = data.topics();
                    List<OffsetFetchRequestTopics> topics = null;
                    if (oldFormatTopics != null) {
                        topics = oldFormatTopics
                            .stream()
                            .map(t -> new OffsetFetchRequestTopics()
                                .setName(t.name())
                                .setPartitionIndexes(t.partitionIndexes()))
                            .collect(Collectors.toList());
                    }
                    OffsetFetchRequestData convertedDataFormat =
                        new OffsetFetchRequestData()
                            .setGroups(Collections.singletonList(
                                new OffsetFetchRequestGroup()
                                    .setGroupId(groupName)
                                    .setTopics(topics)))
                            .setRequireStable(data.requireStable());
                    return new OffsetFetchRequest(convertedDataFormat, version);
                }
            }
            return new OffsetFetchRequest(data, version);
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

    public Map<String, List<TopicPartition>> groupIdsToPartitions() {
        Map<String, List<TopicPartition>> groupIdsToPartitions = new HashMap<>();
        for (OffsetFetchRequestGroup group : data.groups()) {
            List<TopicPartition> tpList = null;
            if (group.topics() != ALL_TOPIC_PARTITIONS_BATCH) {
                tpList = new ArrayList<>();
                for (OffsetFetchRequestTopics topic : group.topics()) {
                    for (Integer partitionIndex : topic.partitionIndexes()) {
                        tpList.add(new TopicPartition(topic.name(), partitionIndex));
                    }
                }
            }
            groupIdsToPartitions.put(group.groupId(), tpList);
        }
        return groupIdsToPartitions;
    }

    public Map<String, List<OffsetFetchRequestTopics>> groupIdsToTopics() {
        Map<String, List<OffsetFetchRequestTopics>> groupIdsToTopics =
            new HashMap<>(data.groups().size());
        data.groups().forEach(g -> groupIdsToTopics.put(g.groupId(), g.topics()));
        return groupIdsToTopics;
    }

    public List<String> groupIds() {
        return data.groups()
            .stream()
            .map(OffsetFetchRequestGroup::groupId)
            .collect(Collectors.toList());
    }

    private OffsetFetchRequest(OffsetFetchRequestData data, short version) {
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

    public boolean isAllPartitionsForGroup(String groupId) {
        OffsetFetchRequestGroup group = data
            .groups()
            .stream()
            .filter(g -> g.groupId().equals(groupId))
            .collect(Collectors.toList())
            .get(0);
        return group.topics() == ALL_TOPIC_PARTITIONS_BATCH;
    }

    @Override
    public OffsetFetchRequestData data() {
        return data;
    }
}
