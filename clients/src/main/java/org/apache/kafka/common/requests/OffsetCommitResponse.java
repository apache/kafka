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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

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

    public static Builder<?> newBuilder(TopicResolver topicResolver, short version) {
        return version >= 9 ?
            new BuilderByTopicId(topicResolver, version)
            : new BuilderByTopicName(topicResolver, version);
    }

    public static abstract class Builder<T> {
        private final TopicResolver topicResolver;
        protected final short version;
        private OffsetCommitResponseData data = new OffsetCommitResponseData();
        private final Map<T, OffsetCommitResponseTopic> topics = new HashMap<>();

        protected Builder(TopicResolver topicResolver, short version) {
            this.topicResolver = topicResolver;
            this.version = version;
        }

        public final Builder<T> addPartition(String topicName, Uuid topicId, int partitionIndex, Errors error) {
            validate(topicName, topicId);
            OffsetCommitResponseTopic topicResponse = getOrCreateTopic(topicName, topicId);

            topicResponse.partitions().add(new OffsetCommitResponsePartition()
                .setPartitionIndex(partitionIndex)
                .setErrorCode(error.code()));

            return this;
        }

        public final <P> Builder<T> addPartitions(
                String topicName,
                Uuid topicId,
                List<P> partitions,
                Function<P, Integer> partitionIndex,
                Errors error
        ) {
            validate(topicName, topicId);
            OffsetCommitResponseTopic topicResponse = getOrCreateTopic(topicName, topicId);

            partitions.forEach(partition -> {
                topicResponse.partitions().add(new OffsetCommitResponsePartition()
                    .setPartitionIndex(partitionIndex.apply(partition))
                    .setErrorCode(error.code()));
            });

            return this;
        }

        public final Builder<T> merge(OffsetCommitResponseData newData) {
            if (data.topics().isEmpty()) {
                // If the current data is empty, we can discard it and use the new data.
                data = newData;
            } else {
                // Otherwise, we have to merge them together.
                newData.topics().forEach(newTopic -> {
                    validate(newTopic.name(), newTopic.topicId());

                    T topicKey = classifer(newTopic.name(), newTopic.topicId());
                    OffsetCommitResponseTopic existingTopic = topics.get(topicKey);

                    if (existingTopic == null) {
                        // If no topic exists, we can directly copy the new topic data.
                        data.topics().add(newTopic);
                        topics.put(topicKey, newTopic);
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

        public final OffsetCommitResponse build() {
            return new OffsetCommitResponse(data, version);
        }

        protected abstract void validate(String topicName, Uuid topicId);

        protected abstract T classifer(String topicName, Uuid topicId);

        private OffsetCommitResponseTopic getOrCreateTopic(String topicName, Uuid topicId) {
            T topicKey = classifer(topicName, topicId);
            OffsetCommitResponseTopic topic = topics.get(topicKey);
            if (topic == null) {
                topic = new OffsetCommitResponseTopic().setName(topicName).setTopicId(topicId);
                data.topics().add(topic);
                topics.put(topicKey, topic);
            }
            return topic;
        }
    }

    public static final class BuilderByTopicId extends Builder<Uuid> {
        protected BuilderByTopicId(TopicResolver topicResolver, short version) {
            super(topicResolver, version);
        }

        @Override
        protected void validate(String topicName, Uuid topicId) {
            if (topicId == null || Uuid.ZERO_UUID.equals(topicId))
                throw new UnsupportedVersionException("OffsetCommitResponse version " + version +
                        " does not support zero topic IDs.");
        }

        @Override
        protected Uuid classifer(String topicName, Uuid topicId) {
            return topicId;
        }
    }

    public static final class BuilderByTopicName extends Builder<String> {
        protected BuilderByTopicName(TopicResolver topicResolver, short version) {
            super(topicResolver, version);
        }

        @Override
        protected void validate(String topicName, Uuid topicId) {
            if (topicName == null)
                throw new UnsupportedVersionException("OffsetCommitResponse version " + version +
                        " does not support null topic names.");
        }

        @Override
        protected String classifer(String topicName, Uuid topicId) {
            return topicName;
        }
    }
}
