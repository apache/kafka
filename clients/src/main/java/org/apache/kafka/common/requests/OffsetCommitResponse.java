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
import org.apache.kafka.common.TopicResolver;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

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
 *   - {@link Errors#UNKNOWN_TOPIC_ID}
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
    private final short version;

    public OffsetCommitResponse(OffsetCommitResponseData data) {
        this(data, OffsetCommitResponseData.HIGHEST_SUPPORTED_VERSION);
    }

    public OffsetCommitResponse(OffsetCommitResponseData data, short version) {
        super(ApiKeys.OFFSET_COMMIT);
        this.data = data;
        this.version = version;
    }

    public OffsetCommitResponse(int requestThrottleMs, Map<TopicPartition, Errors> responseData, short version) {
        super(ApiKeys.OFFSET_COMMIT);
        this.version = version;
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

    public OffsetCommitResponse(Map<TopicPartition, Errors> responseData, short version) {
        this(DEFAULT_THROTTLE_TIME, responseData, version);
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
        return new OffsetCommitResponse(new OffsetCommitResponseData(new ByteBufferAccessor(buffer), version), version);
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

    public short version() {
        return version;
    }

    public static class Builder {
        OffsetCommitResponseData data = new OffsetCommitResponseData();
        HashMap<String, OffsetCommitResponseTopic> byTopicName = new HashMap<>();
        private final TopicResolver topicResolver;
        private final short version;

        public Builder(TopicResolver topicResolver, short version) {
            this.topicResolver = topicResolver;
            this.version = version;
        }

        private OffsetCommitResponseTopic getOrCreateTopic(
            String topicName,
            Uuid topicId
        ) {
            OffsetCommitResponseTopic topic = byTopicName.get(topicName);
            if (topic == null) {
                topic = new OffsetCommitResponseTopic().setName(topicName).setTopicId(topicId);
                data.topics().add(topic);
                byTopicName.put(topicName, topic);
            }
            return topic;
        }

        /**
         * The topic name must be resolved when calling this method. In the broker request handler, resolution
         * of topic names from topic ids happen before the {@link OffsetCommitResponse} is constructed via
         * this builder method.
         */
        public Builder addPartition(
            String topicName,
            Uuid topicId,
            int partitionIndex,
            Errors error
        ) {
            // Enforce this check to avoid silent collisions when an invalid topic name is provided.
            if (Utils.isBlank(topicName)) {
                throw new IllegalArgumentException("OffsetCommitResponse.Builder#addPartition() expects a valid " +
                    "topic name. Use the method addPartitions() when a topic name is undefined.");
            }

            final OffsetCommitResponseTopic topicResponse = getOrCreateTopic(topicName, topicId);

            topicResponse.partitions().add(new OffsetCommitResponsePartition()
                .setPartitionIndex(partitionIndex)
                .setErrorCode(error.code()));

            return this;
        }

        /**
         * This method must be called when a topic name cannot be resolved or is not known to the server.
         * Unlike {@link Builder#addPartition(String, Uuid, int, Errors)}, the topic is not cached to avoid
         * collisions based on an invalid topic name key.
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
            OffsetCommitResponseData newData,
            Logger logger
        ) {
            if (version >= 9) {
                // This method is called after the group coordinator committed the offsets. The group coordinator
                // provides the OffsetCommitResponseData it built in the process. As of now, this data does
                // not contain topic ids, so we resolve them here.
                newData.topics().forEach(topic -> {
                    Uuid topicId = topicResolver.getTopicId(topic.name()).orElse(Uuid.ZERO_UUID);
                    if (Uuid.ZERO_UUID.equals(topicId)) {
                        // This should not happen because topic names returned by the group coordinator should
                        // always be resolvable.
                        logger.debug("Unresolvable topic id for topic {} while preparing " +
                                "the OffsetCommitResponse", topic.name());
                    }
                    topic.setTopicId(topicId);
                });
            }
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

        public OffsetCommitResponse build() {
            return new OffsetCommitResponse(data, version);
        }
    }
}
