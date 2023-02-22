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
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Possible error codes:
 *
 *   - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 *   - {@link Errors#REQUEST_TIMED_OUT}
 *   - {@link Errors#OFFSET_METADATA_TOO_LARGE}
 *   - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 *   - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 *   - {@link Errors#NOT_COORDINATOR}
 *   - {@link Errors#ILLEGAL_GENERATION}
 *   - {@link Errors#UNKNOWN_MEMBER_ID}
 *   - {@link Errors#REBALANCE_IN_PROGRESS}
 *   - {@link Errors#INVALID_COMMIT_OFFSET_SIZE}
 *   - {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 *   - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 */
public class OffsetCommitResponse extends AbstractResponse {

    private final OffsetCommitResponseData data;

    public OffsetCommitResponse(OffsetCommitResponseData data) {
        super(ApiKeys.OFFSET_COMMIT);
        this.data = data;
    }

    public OffsetCommitResponse(int requestThrottleMs, Map<TopicPartition, Errors> responseData) {
        super(ApiKeys.OFFSET_COMMIT);
        Map<String, OffsetCommitResponseTopic>
                responseTopicDataMap = new HashMap<>();

        for (Map.Entry<TopicPartition, Errors> entry : responseData.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            String topicName = topicPartition.topic();

            OffsetCommitResponseTopic topic = responseTopicDataMap.getOrDefault(
                topicName, new OffsetCommitResponseTopic().setName(topicName));

            topic.partitions().add(new OffsetCommitResponsePartition()
                                       .setErrorCode(entry.getValue().code())
                                       .setPartitionIndex(topicPartition.partition()));
            responseTopicDataMap.put(topicName, topic);
        }

        data = new OffsetCommitResponseData()
                .setTopics(new ArrayList<>(responseTopicDataMap.values()))
                .setThrottleTimeMs(requestThrottleMs);
    }

    public OffsetCommitResponse(Map<TopicPartition, Errors> responseData) {
        this(DEFAULT_THROTTLE_TIME, responseData);
    }

    @Override
    public OffsetCommitResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(data.topics().stream().flatMap(topicResult ->
                topicResult.partitions().stream().map(partitionResult ->
                        Errors.forCode(partitionResult.errorCode()))));
    }

    public static OffsetCommitResponse parse(ByteBuffer buffer, short version) {
        return new OffsetCommitResponse(new OffsetCommitResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 4;
    }

    public static class Builder {
        OffsetCommitResponseData data = new OffsetCommitResponseData();
        HashMap<String, OffsetCommitResponseTopic> byTopicName = new HashMap<>();

        private OffsetCommitResponseTopic getOrCreateTopic(
            String topicName,
            Uuid topicId
        ) {
            OffsetCommitResponseTopic topic = byTopicName.get(topicName);
            if (topic == null) {
                topic = new OffsetCommitResponseTopic().setName(topicName).setTopicId(topicId);
                data.topics().add(topic);
                byTopicName.put(topicName, topic);

            } else {
                boolean idDefinedInArg = topicId != null && !Uuid.ZERO_UUID.equals(topicId);
                boolean idDefinedInCache = topic.topicId() != null && !Uuid.ZERO_UUID.equals(topic.topicId());

                if (idDefinedInArg && idDefinedInCache) {
                    if (!topicId.equals(topic.topicId())) {
                        throw new IllegalArgumentException("Topic " + topicName + " with ID " + topicId + " has " +
                                "already been registered with a different topic ID: " + topic.topicId());
                    }
                } else if (idDefinedInArg) {
                    topic.setTopicId(topicId);
                }
            }
            return topic;
        }

        /**
         * Adds the given partition to the {@link OffsetCommitResponseTopic} corresponding to the given topic
         * name and ID. A valid topic name must be provided when calling this method. If the topic ID argument
         * is defined, and the {@link OffsetCommitResponseTopic} for the given name already present in the
         * builder, and has a valid topic ID, a consistency check is performed to ensure both topic IDs are
         * equal. If a valid topic ID is provided with the arguments but not defined in the pre-existing
         * {@link OffsetCommitResponseTopic}, the ID is added to it to be used with responses version >= 9.
         */
        public Builder addPartition(
            String topicName,
            Uuid topicId,
            int partitionIndex,
            Errors error
        ) {
            if (Utils.isBlank(topicName)) {
                throw new IllegalArgumentException("Topic name must be provided when calling addPartition(). " +
                        "Use the method addPartitions() for undefined topics.");
            }

            final OffsetCommitResponseTopic topicResponse = getOrCreateTopic(topicName, topicId);

            topicResponse.partitions().add(new OffsetCommitResponsePartition()
                .setPartitionIndex(partitionIndex)
                .setErrorCode(error.code()));

            return this;
        }

        /**
         * Creates a new {@link OffsetCommitResponseTopic} object, populates it with the partitions resolved via
         * the {@code partitionIndex} function applied to the {@code partitions} list of user-defined objects.
         * <p></p>
         * This method does not validate that the provided topic name and ID are defined. It does not validate
         * that an {@link OffsetCommitResponseTopic} is already defined in the builder for either the topic name,
         * the topic ID, or both. Similarly, the topic name and ID provided will not be used in future calls
         * to the method {@link Builder#addPartition(String, Uuid, int, Errors)} to look-up and avoid duplication
         * of {@link OffsetCommitResponseTopic} entries in the builder.
         *
         * @return This builder.
         * @param <P> The type of the user-defined objects in the list of partitions. The index of the partition
         *           corresponding to each object is resolved via the function provided as argument.
         */
        public <P> Builder addPartitions(
            String topicName,
            Uuid topicId,
            List<P> partitions,
            Function<P, Integer> partitionIndex,
            Errors error
        ) {
            OffsetCommitResponseTopic topicResponse = new OffsetCommitResponseTopic()
                 .setName(topicName)
                 .setTopicId(topicId);

            data.topics().add(topicResponse);

            partitions.forEach(partition -> {
                topicResponse.partitions().add(new OffsetCommitResponsePartition()
                    .setPartitionIndex(partitionIndex.apply(partition))
                    .setErrorCode(error.code()));
            });

            return this;
        }

        public Builder merge(
            OffsetCommitResponseData newData
        ) {
            if (data.topics().isEmpty()) {
                // If the current data is empty, we can discard it and use the new data.
                data = newData;
            } else {
                // Otherwise, we have to merge them together.
                newData.topics().forEach(newTopic -> {
                    OffsetCommitResponseTopic existingTopic = byTopicName.get(newTopic.name());
                    if (existingTopic == null) {
                        // If no topic exists, we can directly copy the new topic data.
                        data.topics().add(newTopic);
                        byTopicName.put(newTopic.name(), newTopic);
                    } else {
                        // Otherwise, we add the partitions to the existing one. Note we
                        // expect non-overlapping partitions here as we don't verify
                        // if the partition is already in the list before adding it.
                        existingTopic.partitions().addAll(newTopic.partitions());
                    }
                });
            }

            return this;
        }

        public OffsetCommitResponse build(short version) {
            if (version >= 9) {
                data.topics().forEach(topic -> {
                    // Set the topic name to null if a topic ID for the topic is present.
                    if (!Uuid.ZERO_UUID.equals(topic.topicId())) {
                        topic.setName(null);
                    }
                });
            } else {
                data.topics().forEach(topic -> {
                    // Topic must be set to default for version < 9.
                    if (!Uuid.ZERO_UUID.equals(topic.topicId())) {
                        topic.setTopicId(Uuid.ZERO_UUID);
                    }
                    if (Utils.isBlank(topic.name())) {
                        throw new InvalidRequestException("Topic name must be provided for response version < 9.");
                    }
                });
            }
            return new OffsetCommitResponse(data);
        }
    }
}
