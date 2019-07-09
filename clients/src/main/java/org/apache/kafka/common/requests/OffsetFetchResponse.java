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
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Possible error codes:
 *
 * - Partition errors:
 *   - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 *   - {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 *
 * - Group or coordinator errors:
 *   - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 *   - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 *   - {@link Errors#NOT_COORDINATOR}
 *   - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 */
public class OffsetFetchResponse extends AbstractResponse {
    public static final long INVALID_OFFSET = -1L;
    public static final String NO_METADATA = "";
    public static final PartitionData UNKNOWN_PARTITION = new PartitionData(INVALID_OFFSET,
                                                                            Optional.empty(),
                                                                            NO_METADATA,
                                                                            Errors.UNKNOWN_TOPIC_OR_PARTITION);
    public static final PartitionData UNAUTHORIZED_PARTITION = new PartitionData(INVALID_OFFSET,
                                                                                 Optional.empty(),
                                                                                 NO_METADATA,
                                                                                 Errors.TOPIC_AUTHORIZATION_FAILED);

    public final OffsetFetchResponseData data;

    public static final class PartitionData {
        public final long offset;
        public final String metadata;
        public final Errors error;
        public final Optional<Integer> leaderEpoch;

        public PartitionData(long offset,
                             Optional<Integer> leaderEpoch,
                             String metadata,
                             Errors error) {
            this.offset = offset;
            this.leaderEpoch = leaderEpoch;
            this.metadata = metadata;
            this.error = error;
        }

        public boolean hasError() {
            return this.error != Errors.NONE;
        }
    }

    /**
     * Constructor for all versions without throttle time.
     * @param error Potential coordinator or group level error code (for api version 2 and later)
     * @param responseData Fetched offset information grouped by topic-partition
     */
    public OffsetFetchResponse(Errors error, Map<TopicPartition, PartitionData> responseData) {
        this(DEFAULT_THROTTLE_TIME, error, responseData);
    }

    /**
     * Constructor with throttle time
     * @param throttleTimeMs The time in milliseconds that this response was throttled
     * @param error Potential coordinator or group level error code (for api version 2 and later)
     * @param responseData Fetched offset information grouped by topic-partition
     */
    public OffsetFetchResponse(int throttleTimeMs, Errors error, Map<TopicPartition, PartitionData> responseData) {
        Map<String, OffsetFetchResponseTopic> offsetFetchResponseTopicMap = new HashMap<>();
        for (Map.Entry<TopicPartition, PartitionData> entry : responseData.entrySet()) {
            String topicName = entry.getKey().topic();
            OffsetFetchResponseTopic topic = offsetFetchResponseTopicMap.getOrDefault(
                topicName, new OffsetFetchResponseTopic().setName(topicName));
            PartitionData partitionData = entry.getValue();
            topic.partitions().add(new OffsetFetchResponsePartition()
                                       .setPartitionIndex(entry.getKey().partition())
                                       .setErrorCode(partitionData.error.code())
                                       .setCommittedOffset(partitionData.offset)
                                       .setCommittedLeaderEpoch(partitionData.leaderEpoch.orElse(0))
                                       .setMetadata(partitionData.metadata)
            );
            offsetFetchResponseTopicMap.put(topicName, topic);
        }

        this.data = new OffsetFetchResponseData()
            .setTopics(new ArrayList<>(offsetFetchResponseTopicMap.values()))
            .setErrorCode(error.code())
            .setThrottleTimeMs(throttleTimeMs);
    }

    public OffsetFetchResponse(Struct struct, short version) {
        this.data = new OffsetFetchResponseData(struct, version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public boolean hasError() {
        return this.data.errorCode() != Errors.NONE.code();
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<TopicPartition, Errors> errorMap = new HashMap<>();
        for (OffsetFetchResponseTopic topic : data.topics()) {
            for (OffsetFetchResponsePartition partition : topic.partitions()) {
                errorMap.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                             Errors.forCode(partition.errorCode()));
            }
        }
        return errorCounts(errorMap);
    }

    public Map<TopicPartition, PartitionData> responseData() {
        Map<TopicPartition, PartitionData> responseData = new HashMap<>();
        for (OffsetFetchResponseTopic topic : data.topics()) {
            for (OffsetFetchResponsePartition partition : topic.partitions()) {
                responseData.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                                 new PartitionData(partition.committedOffset(),
                                                   Optional.of(partition.committedLeaderEpoch()),
                                                   partition.metadata(),
                                                   Errors.forCode(partition.errorCode()))
                );
            }

        }
        return responseData;
    }

    public static OffsetFetchResponse parse(ByteBuffer buffer, short version) {
        return new OffsetFetchResponse(ApiKeys.OFFSET_FETCH.parseResponse(version, buffer), version);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 4;
    }
}
