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
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.record.RecordBatch;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Possible error codes:
 *
 * - {@link Errors#UNSUPPORTED_FOR_MESSAGE_FORMAT} If the message format does not support lookup by timestamp
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED} If the user does not have DESCRIBE access to a requested topic
 * - {@link Errors#REPLICA_NOT_AVAILABLE} If the request is received by a broker with version < 2.6 which is not a replica
 * - {@link Errors#NOT_LEADER_OR_FOLLOWER} If the broker is not a leader or follower and either the provided leader epoch
 *     matches the known leader epoch on the broker or is empty
 * - {@link Errors#FENCED_LEADER_EPOCH} If the epoch is lower than the broker's epoch
 * - {@link Errors#UNKNOWN_LEADER_EPOCH} If the epoch is larger than the broker's epoch
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} If the broker does not have metadata for a topic or partition
 * - {@link Errors#KAFKA_STORAGE_ERROR} If the log directory for one of the requested partitions is offline
 * - {@link Errors#UNKNOWN_SERVER_ERROR} For any unexpected errors
 * - {@link Errors#LEADER_NOT_AVAILABLE} The leader's HW has not caught up after recent election (v4 protocol)
 * - {@link Errors#OFFSET_NOT_AVAILABLE} The leader's HW has not caught up after recent election (v5+ protocol)
 */
public class ListOffsetsResponse extends AbstractResponse {
    public static final long UNKNOWN_TIMESTAMP = -1L;
    public static final long UNKNOWN_OFFSET = -1L;
    public static final int UNKNOWN_EPOCH = RecordBatch.NO_PARTITION_LEADER_EPOCH;

    private final ListOffsetsResponseData data;

    public ListOffsetsResponse(ListOffsetsResponseData data) {
        super(ApiKeys.LIST_OFFSETS);
        this.data = data;
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
    public ListOffsetsResponseData data() {
        return data;
    }

    public List<ListOffsetsTopicResponse> topics() {
        return data.topics();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new EnumMap<>(Errors.class);
        topics().forEach(topic ->
            topic.partitions().forEach(partition ->
                updateErrorCounts(errorCounts, Errors.forCode(partition.errorCode()))
            )
        );
        return errorCounts;
    }

    public static ListOffsetsResponse parse(Readable readable, short version) {
        return new ListOffsetsResponse(new ListOffsetsResponseData(readable, version));
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 3;
    }

    public static boolean useTopicIds(short version) {
        return version >= 12;
    }

    public static Builder newBuilder(boolean useTopicIds) {
        if (useTopicIds) {
            return new TopicIdBuilder();
        } else {
            return new TopicNameBuilder();
        }
    }

    public static ListOffsetsTopicResponse singletonListOffsetsTopicResponse(TopicPartition tp, Errors error, long timestamp, long offset, int epoch) {
        return new ListOffsetsTopicResponse()
                 .setName(tp.topic())
                 .setPartitions(Collections.singletonList(new ListOffsetsPartitionResponse()
                         .setPartitionIndex(tp.partition())
                         .setErrorCode(error.code())
                         .setTimestamp(timestamp)
                         .setOffset(offset)
                         .setLeaderEpoch(epoch)));
    }

    public abstract static class Builder {
        protected ListOffsetsResponseData data = new ListOffsetsResponseData();

        protected abstract void add(
                ListOffsetsTopicResponse topic
        );

        protected abstract ListOffsetsTopicResponse get(
                Uuid topicId,
                String topicName
        );

        protected abstract ListOffsetsTopicResponse getOrCreate(
                Uuid topicId,
                String topicName
        );

        public Builder addPartition(
                Uuid topicId,
                String topicName,
                int partitionIndex,
                Errors error
        ) {
            final ListOffsetsTopicResponse topicResponse = getOrCreate(topicId, topicName);
            topicResponse.partitions().add(new ListOffsetsPartitionResponse()
                    .setPartitionIndex(partitionIndex)
                    .setErrorCode(error.code()));
            return this;
        }

        public <P> Builder addPartitions(
                Uuid topicId,
                String topicName,
                List<P> partitions,
                Function<P, Integer> partitionIndex,
                Errors error
        ) {
            final ListOffsetsTopicResponse topicResponse = getOrCreate(topicId, topicName);
            partitions.forEach(partition ->
                    topicResponse.partitions().add(new ListOffsetsPartitionResponse()
                            .setPartitionIndex(partitionIndex.apply(partition))
                            .setErrorCode(error.code()))
            );
            return this;
        }

        public Builder merge(
                ListOffsetsResponseData newData
        ) {
            if (data.topics().isEmpty()) {
                // If the current data is empty, we can discard it and use the new data.
                data = newData;
            } else {
                // Otherwise, we have to merge them together.
                newData.topics().forEach(newTopic -> {
                    ListOffsetsTopicResponse existingTopic = get(newTopic.topicId(), newTopic.name());
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

        public ListOffsetsResponse build() {
            return new ListOffsetsResponse(data);
        }

    }

    public static class TopicIdBuilder extends Builder {
        private final HashMap<Uuid, ListOffsetsTopicResponse> byTopicId = new HashMap<>();

        @Override
        protected void add(ListOffsetsTopicResponse topic) {
            throwIfTopicIdIsNull(topic.topicId());
            data.topics().add(topic);
            byTopicId.put(topic.topicId(), topic);
        }

        @Override
        protected ListOffsetsTopicResponse get(Uuid topicId, String topicName) {
            throwIfTopicIdIsNull(topicId);
            return byTopicId.get(topicId);
        }

        @Override
        protected ListOffsetsResponseData.ListOffsetsTopicResponse getOrCreate(Uuid topicId, String topicName) {
            throwIfTopicIdIsNull(topicId);
            ListOffsetsResponseData.ListOffsetsTopicResponse topic = byTopicId.get(topicId);
            if (topic == null) {
                topic = new ListOffsetsResponseData.ListOffsetsTopicResponse()
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
        private final HashMap<String, ListOffsetsTopicResponse> byTopicName = new HashMap<>();

        @Override
        protected void add(ListOffsetsTopicResponse topic) {
            throwIfTopicNameIsNull(topic.name());
            data.topics().add(topic);
            byTopicName.put(topic.name(), topic);
        }

        @Override
        protected ListOffsetsTopicResponse get(Uuid topicId, String topicName) {
            throwIfTopicNameIsNull(topicName);
            return byTopicName.get(topicName);
        }

        @Override
        protected ListOffsetsTopicResponse getOrCreate(Uuid topicId, String topicName) {
            throwIfTopicNameIsNull(topicName);
            ListOffsetsTopicResponse topic = byTopicName.get(topicName);
            if (topic == null) {
                topic = new ListOffsetsTopicResponse()
                        .setName(topicName)
                        .setTopicId(topicId);
                data.topics().add(topic);
                byTopicName.put(topicName, topic);
            }
            return topic;
        }

        private void throwIfTopicNameIsNull(String topicName) {
            if (topicName == null) {
                throw new IllegalArgumentException("TopicName cannot be null.");
            }
        }
    }

}