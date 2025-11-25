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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.record.RecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OffsetFetchRequest extends AbstractRequest {

    private static final Logger log = LoggerFactory.getLogger(OffsetFetchRequest.class);
    public static final short TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION = 2;
    public static final short REQUIRE_STABLE_OFFSET_MIN_VERSION = 7;
    public static final short BATCH_MIN_VERSION = 8;
    public static final short TOPIC_ID_MIN_VERSION = 10;

    private final OffsetFetchRequestData data;

    public static class Builder extends AbstractRequest.Builder<OffsetFetchRequest> {
        private final OffsetFetchRequestData data;
        private final boolean throwOnFetchStableOffsetsUnsupported;

        public static Builder forTopicIdsOrNames(
            OffsetFetchRequestData data,
            boolean throwOnFetchStableOffsetsUnsupported
        ) {
            return new Builder(
                data,
                throwOnFetchStableOffsetsUnsupported,
                ApiKeys.OFFSET_FETCH.oldestVersion(),
                ApiKeys.OFFSET_FETCH.latestVersion()
            );
        }

        public static Builder forTopicNames(
            OffsetFetchRequestData data,
            boolean throwOnFetchStableOffsetsUnsupported
        ) {
            return new Builder(
                data,
                throwOnFetchStableOffsetsUnsupported,
                ApiKeys.OFFSET_FETCH.oldestVersion(),
                (short) (TOPIC_ID_MIN_VERSION - 1)
            );
        }

        private Builder(
            OffsetFetchRequestData data,
            boolean throwOnFetchStableOffsetsUnsupported,
            short oldestAllowedVersion,
            short latestAllowedVersion
        ) {
            super(ApiKeys.OFFSET_FETCH, oldestAllowedVersion, latestAllowedVersion);
            this.data = data;
            this.throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported;
        }

        private void throwIfBatchingIsUnsupported(short version) {
            if (data.groups().size() > 1 && version < BATCH_MIN_VERSION) {
                throw new NoBatchedOffsetFetchRequestException("Broker does not support"
                    + " batching groups for fetch offset request on version " + version);
            }
        }

        private void throwIfStableOffsetsUnsupported(short version) {
            if (data.requireStable() && version < REQUIRE_STABLE_OFFSET_MIN_VERSION) {
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
        }

        private void throwIfMissingRequiredTopicIdentifiers(short version) {
            if (version < TOPIC_ID_MIN_VERSION) {
                data.groups().forEach(group -> {
                    if (group.topics() != null) {
                        group.topics().forEach(topic -> {
                            if (topic.name() == null || topic.name().isEmpty()) {
                                throw new UnsupportedVersionException("The broker offset fetch api version " +
                                    version + " does require usage of topic names.");
                            }
                        });
                    }
                });
            } else {
                data.groups().forEach(group -> {
                    if (group.topics() != null) {
                        group.topics().forEach(topic -> {
                            if (topic.topicId() == null || topic.topicId().equals(Uuid.ZERO_UUID)) {
                                throw new UnsupportedVersionException("The broker offset fetch api version " +
                                    version + " does require usage of topic ids.");
                            }
                        });
                    }
                });
            }
        }

        private void throwIfRequestingAllTopicsIsUnsupported(short version) {
            if (version < TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION) {
                data.groups().forEach(group -> {
                    if (group.topics() == null) {
                        throw new UnsupportedVersionException("The broker only supports OffsetFetchRequest " +
                            "v" + version + ", but we need v2 or newer to request all topic partitions.");
                    }
                });
            }
        }

        private OffsetFetchRequestData maybeDowngrade(short version) {
            // Convert data to use the appropriate version since version 8
            // uses different format.
            if (version >= BATCH_MIN_VERSION || data.groups().isEmpty()) return data;

            OffsetFetchRequestGroup group = data.groups().get(0);
            String groupName = group.groupId();
            List<OffsetFetchRequestTopics> topics = group.topics();
            List<OffsetFetchRequestTopic> oldFormatTopics = null;

            if (topics != null) {
                oldFormatTopics = topics
                    .stream()
                    .map(t -> new OffsetFetchRequestTopic()
                        .setName(t.name())
                        .setPartitionIndexes(t.partitionIndexes()))
                    .collect(Collectors.toList());
            }

            return new OffsetFetchRequestData()
                .setGroupId(groupName)
                .setTopics(oldFormatTopics)
                .setRequireStable(data.requireStable());
        }

        @Override
        public OffsetFetchRequest build(short version) {
            throwIfBatchingIsUnsupported(version);
            throwIfStableOffsetsUnsupported(version);
            throwIfMissingRequiredTopicIdentifiers(version);
            throwIfRequestingAllTopicsIsUnsupported(version);
            return new OffsetFetchRequest(maybeDowngrade(version), version);
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

    public String groupId() {
        return data.groupId();
    }

    public boolean requireStable() {
        return data.requireStable();
    }

    public List<OffsetFetchRequestData.OffsetFetchRequestGroup> groups() {
        if (version() >= BATCH_MIN_VERSION) {
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
                data.topics().forEach(topic ->
                    group.topics().add(new OffsetFetchRequestTopics()
                        .setName(topic.name())
                        .setPartitionIndexes(topic.partitionIndexes())
                    )
                );
            }

            return Collections.singletonList(group);
        }
    }

    public Map<String, List<TopicPartition>> groupIdsToPartitions() {
        Map<String, List<TopicPartition>> groupIdsToPartitions = new HashMap<>();
        for (OffsetFetchRequestGroup group : data.groups()) {
            List<TopicPartition> tpList = null;
            if (group.topics() != null) {
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

    @Override
    public OffsetFetchResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);

        if (version() < TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION) {
            // The response does not support top level error so we return each
            // partition with the error.
            return new OffsetFetchResponse(
                new OffsetFetchResponseData()
                    .setThrottleTimeMs(throttleTimeMs)
                    .setTopics(data.topics().stream().map(topic ->
                        new OffsetFetchResponseData.OffsetFetchResponseTopic()
                            .setName(topic.name())
                            .setPartitions(topic.partitionIndexes().stream().map(partition ->
                                new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                    .setPartitionIndex(partition)
                                    .setErrorCode(error.code())
                                    .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                                    .setMetadata(OffsetFetchResponse.NO_METADATA)
                                    .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                            ).collect(Collectors.toList()))
                    ).collect(Collectors.toList())),
                version()
            );
        } else if (version() < BATCH_MIN_VERSION) {
            // The response does not support multiple groups but it does support
            // top level error.
            return new OffsetFetchResponse(
                new OffsetFetchResponseData()
                    .setThrottleTimeMs(throttleTimeMs)
                    .setErrorCode(error.code()),
                version()
            );
        } else {
            // The response does support multiple groups so we provide a top level
            // error per group.
            return new OffsetFetchResponse(
                new OffsetFetchResponseData()
                    .setThrottleTimeMs(throttleTimeMs)
                    .setGroups(data.groups().stream().map(group ->
                        new OffsetFetchResponseData.OffsetFetchResponseGroup()
                            .setGroupId(group.groupId())
                            .setErrorCode(error.code())
                    ).collect(Collectors.toList())),
                version()
            );
        }
    }

    public static OffsetFetchRequest parse(Readable readable, short version) {
        return new OffsetFetchRequest(new OffsetFetchRequestData(readable, version), version);
    }

    public static boolean useTopicIds(short version) {
        return version >= TOPIC_ID_MIN_VERSION;
    }

    @Override
    public OffsetFetchRequestData data() {
        return data;
    }
}
