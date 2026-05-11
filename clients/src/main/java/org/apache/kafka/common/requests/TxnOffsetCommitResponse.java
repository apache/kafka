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
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Possible error codes:
 *
 *   - {@link Errors#INVALID_PRODUCER_EPOCH}
 *   - {@link Errors#NOT_COORDINATOR}
 *   - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 *   - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 *   - {@link Errors#OFFSET_METADATA_TOO_LARGE}
 *   - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 *   - {@link Errors#INVALID_COMMIT_OFFSET_SIZE}
 *   - {@link Errors#TRANSACTIONAL_ID_AUTHORIZATION_FAILED}
 *   - {@link Errors#UNSUPPORTED_FOR_MESSAGE_FORMAT}
 *   - {@link Errors#REQUEST_TIMED_OUT}
 *   - {@link Errors#UNKNOWN_MEMBER_ID}
 *   - {@link Errors#FENCED_INSTANCE_ID}
 *   - {@link Errors#ILLEGAL_GENERATION}
 */
public class TxnOffsetCommitResponse extends AbstractResponse {

    public static boolean useTopicIds(short version) {
        return version >= 6;
    }

    public static Builder newBuilder(boolean useTopicIds) {
        if (useTopicIds) {
            return new TopicIdBuilder();
        } else {
            return new TopicNameBuilder();
        }
    }

    public abstract static class Builder {
        protected TxnOffsetCommitResponseData data = new TxnOffsetCommitResponseData();

        protected abstract void add(
            TxnOffsetCommitResponseTopic topic
        );

        protected abstract TxnOffsetCommitResponseTopic get(
            Uuid topicId,
            String topicName
        );

        protected abstract TxnOffsetCommitResponseTopic getOrCreate(
            Uuid topicId,
            String topicName
        );

        public Builder addPartition(
            Uuid topicId,
            String topicName,
            int partitionIndex,
            Errors error
        ) {
            final TxnOffsetCommitResponseTopic topicResponse = getOrCreate(topicId, topicName);
            topicResponse.partitions().add(new TxnOffsetCommitResponsePartition()
                .setPartitionIndex(partitionIndex)
                .setErrorCode(error.code()));
            return this;
        }

        public Builder addPartitions(
            Uuid topicId,
            String topicName,
            List<TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition> partitions,
            Function<TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition, Integer> partitionIndex,
            Errors error
        ) {
            final TxnOffsetCommitResponseTopic topicResponse = getOrCreate(topicId, topicName);
            partitions.forEach(partition ->
                topicResponse.partitions().add(new TxnOffsetCommitResponsePartition()
                    .setPartitionIndex(partitionIndex.apply(partition))
                    .setErrorCode(error.code()))
            );
            return this;
        }

        public Builder merge(
            TxnOffsetCommitResponseData newData
        ) {
            if (data.topics().isEmpty()) {
                // If the current data is empty, we can discard it and use the new data.
                data = newData;
            } else {
                // Otherwise, we have to merge them together.
                newData.topics().forEach(newTopic -> {
                    TxnOffsetCommitResponseTopic existingTopic = get(newTopic.topicId(), newTopic.name());
                    if (existingTopic == null) {
                        // If no topic exists, we can directly copy the new topic data.
                        add(newTopic);
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

        public TxnOffsetCommitResponse build() {
            return new TxnOffsetCommitResponse(data);
        }
    }

    public static class TopicIdBuilder extends Builder {
        private final HashMap<Uuid, TxnOffsetCommitResponseTopic> byTopicId = new HashMap<>();

        @Override
        protected void add(TxnOffsetCommitResponseTopic topic) {
            throwIfTopicIdIsNull(topic.topicId());
            data.topics().add(topic);
            byTopicId.put(topic.topicId(), topic);
        }

        @Override
        protected TxnOffsetCommitResponseTopic get(Uuid topicId, String topicName) {
            throwIfTopicIdIsNull(topicId);
            return byTopicId.get(topicId);
        }

        @Override
        protected TxnOffsetCommitResponseTopic getOrCreate(Uuid topicId, String topicName) {
            throwIfTopicIdIsNull(topicId);
            TxnOffsetCommitResponseTopic topic = byTopicId.get(topicId);
            if (topic == null) {
                topic = new TxnOffsetCommitResponseTopic()
                    .setName(topicName)
                    .setTopicId(topicId);
                data.topics().add(topic);
                byTopicId.put(topicId, topic);
            }
            return topic;
        }

        private static void throwIfTopicIdIsNull(Uuid topicId) {
            if (topicId == null) {
                throw new IllegalArgumentException("TopicId cannot be null.");
            }
        }
    }

    public static class TopicNameBuilder extends Builder {
        private final HashMap<String, TxnOffsetCommitResponseTopic> byTopicName = new HashMap<>();

        @Override
        protected void add(TxnOffsetCommitResponseTopic topic) {
            throwIfTopicNameIsNull(topic.name());
            data.topics().add(topic);
            byTopicName.put(topic.name(), topic);
        }

        @Override
        protected TxnOffsetCommitResponseTopic get(Uuid topicId, String topicName) {
            throwIfTopicNameIsNull(topicName);
            return byTopicName.get(topicName);
        }

        @Override
        protected TxnOffsetCommitResponseTopic getOrCreate(Uuid topicId, String topicName) {
            throwIfTopicNameIsNull(topicName);
            TxnOffsetCommitResponseTopic topic = byTopicName.get(topicName);
            if (topic == null) {
                topic = new TxnOffsetCommitResponseTopic()
                    .setName(topicName)
                    .setTopicId(topicId);
                data.topics().add(topic);
                byTopicName.put(topicName, topic);
            }
            return topic;
        }

        private static void throwIfTopicNameIsNull(String topicName) {
            if (topicName == null) {
                throw new IllegalArgumentException("TopicName cannot be null.");
            }
        }
    }

    private final TxnOffsetCommitResponseData data;

    public TxnOffsetCommitResponse(TxnOffsetCommitResponseData data) {
        super(ApiKeys.TXN_OFFSET_COMMIT);
        this.data = data;
    }

    public TxnOffsetCommitResponse(int requestThrottleMs, Map<TopicPartition, Errors> responseData) {
        super(ApiKeys.TXN_OFFSET_COMMIT);
        Map<String, TxnOffsetCommitResponseTopic> responseTopicDataMap = new HashMap<>();

        for (Map.Entry<TopicPartition, Errors> entry : responseData.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            String topicName = topicPartition.topic();

            TxnOffsetCommitResponseTopic topic = responseTopicDataMap.getOrDefault(
                topicName, new TxnOffsetCommitResponseTopic().setName(topicName));

            topic.partitions().add(new TxnOffsetCommitResponsePartition()
                                       .setErrorCode(entry.getValue().code())
                                       .setPartitionIndex(topicPartition.partition())
            );
            responseTopicDataMap.put(topicName, topic);
        }

        data = new TxnOffsetCommitResponseData()
                   .setTopics(new ArrayList<>(responseTopicDataMap.values()))
                   .setThrottleTimeMs(requestThrottleMs);
    }

    @Override
    public TxnOffsetCommitResponseData data() {
        return data;
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
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(data.topics().stream().flatMap(topic ->
                topic.partitions().stream().map(partition ->
                        Errors.forCode(partition.errorCode()))));
    }

    public static TxnOffsetCommitResponse parse(Readable readable, short version) {
        return new TxnOffsetCommitResponse(new TxnOffsetCommitResponseData(readable, version));
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
