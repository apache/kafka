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
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Possible error codes:
 *
 * UNKNOWN_TOPIC_OR_PARTITION (3)
 * REQUEST_TIMED_OUT (7)
 * OFFSET_METADATA_TOO_LARGE (12)
 * COORDINATOR_LOAD_IN_PROGRESS (14)
 * GROUP_COORDINATOR_NOT_AVAILABLE (15)
 * NOT_COORDINATOR (16)
 * ILLEGAL_GENERATION (22)
 * UNKNOWN_MEMBER_ID (25)
 * REBALANCE_IN_PROGRESS (27)
 * INVALID_COMMIT_OFFSET_SIZE (28)
 * TOPIC_AUTHORIZATION_FAILED (29)
 * GROUP_AUTHORIZATION_FAILED (30)
 */
public class OffsetCommitResponse extends AbstractResponse {

    private final OffsetCommitResponseData data;

    public OffsetCommitResponse(OffsetCommitResponseData data) {
        this.data = data;
    }

    public OffsetCommitResponse(int requestThrottleMs, Map<TopicPartition, Errors> responseData) {
        Map<String, OffsetCommitResponseData.OffsetCommitResponseTopic>
                responseTopicDataMap = new HashMap<>();

        for (Map.Entry<TopicPartition, Errors> entry : responseData.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            String topicName = topicPartition.topic();

            OffsetCommitResponseData.OffsetCommitResponseTopic topic = responseTopicDataMap
                    .getOrDefault(topicName, new OffsetCommitResponseData.OffsetCommitResponseTopic());

            if (topic.name().equals("")) {
                topic.setName(topicName);
            }
            topic.partitions().add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
                    .setErrorCode(entry.getValue().code())
                    .setPartitionIndex(topicPartition.partition())
            );
            responseTopicDataMap.put(topicName, topic);
        }

        data = new OffsetCommitResponseData()
                .setTopics(new ArrayList<>(responseTopicDataMap.values()))
                .setThrottleTimeMs(requestThrottleMs);
    }

    public OffsetCommitResponse(Map<TopicPartition, Errors> responseData) {
        this(DEFAULT_THROTTLE_TIME, responseData);
    }

    public OffsetCommitResponse(Struct struct) {
        short latestVersion = (short) (OffsetCommitResponseData.SCHEMAS.length - 1);
        this.data = new OffsetCommitResponseData(struct, latestVersion);
    }

    public OffsetCommitResponse(Struct struct, short version) {
        this.data = new OffsetCommitResponseData(struct, version);
    }

    public OffsetCommitResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<TopicPartition, Errors> errorMap = new HashMap<>();
        for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : data.topics()) {
            for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                errorMap.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                        Errors.forCode(partition.errorCode()));
            }

        }
        return errorCounts(errorMap);
    }

    public static OffsetCommitResponse parse(ByteBuffer buffer, short version) {
        return new OffsetCommitResponse(ApiKeys.OFFSET_COMMIT.parseResponse(version, buffer), version);
    }

    @Override
    public Struct toStruct(short version) {
        return data.toStruct(version);
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
    public boolean shouldClientThrottle(short version) {
        return version >= 4;
    }
}
