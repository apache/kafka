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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

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
    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("responses",
            "Responses by topic for committed partitions");

    // topic level fields
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partition_responses",
            "Responses for committed partitions");

    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            PARTITION_ID,
            ERROR_CODE);

    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V0);

    private static final Schema OFFSET_COMMIT_RESPONSE_V0 = new Schema(
            TOPICS_V0);

    // V1 adds timestamp and group membership information (generation and memberId) to the request
    private static final Schema OFFSET_COMMIT_RESPONSE_V1 = OFFSET_COMMIT_RESPONSE_V0;

    // V2 adds retention time to the request
    private static final Schema OFFSET_COMMIT_RESPONSE_V2 = OFFSET_COMMIT_RESPONSE_V0;

    // V3 adds throttle time
    private static final Schema OFFSET_COMMIT_RESPONSE_V3 = new Schema(
            THROTTLE_TIME_MS,
            TOPICS_V0);

    // V4 bump used to indicate that on quota violation brokers send out responses before throttling.
    private static final Schema OFFSET_COMMIT_RESPONSE_V4 = OFFSET_COMMIT_RESPONSE_V3;

    // V5 removes retention time from the request
    private static final Schema OFFSET_COMMIT_RESPONSE_V5 = OFFSET_COMMIT_RESPONSE_V4;

    // V6 adds leader epoch to the request
    private static final Schema OFFSET_COMMIT_RESPONSE_V6 = OFFSET_COMMIT_RESPONSE_V5;

    public static Schema[] schemaVersions() {
        return new Schema[] {OFFSET_COMMIT_RESPONSE_V0, OFFSET_COMMIT_RESPONSE_V1, OFFSET_COMMIT_RESPONSE_V2,
            OFFSET_COMMIT_RESPONSE_V3, OFFSET_COMMIT_RESPONSE_V4, OFFSET_COMMIT_RESPONSE_V5, OFFSET_COMMIT_RESPONSE_V6};
    }

    private final Map<TopicPartition, Errors> responseData;
    private final int throttleTimeMs;

    public OffsetCommitResponse(Map<TopicPartition, Errors> responseData) {
        this(DEFAULT_THROTTLE_TIME, responseData);
    }

    public OffsetCommitResponse(int throttleTimeMs, Map<TopicPartition, Errors> responseData) {
        this.throttleTimeMs = throttleTimeMs;
        this.responseData = responseData;
    }

    public OffsetCommitResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        responseData = new HashMap<>();
        for (Object topicResponseObj : struct.get(TOPICS)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.get(PARTITIONS)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.get(PARTITION_ID);
                Errors error = Errors.forCode(partitionResponse.get(ERROR_CODE));
                responseData.put(new TopicPartition(topic, partition), error);
            }
        }
    }

    @Override
    public Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.OFFSET_COMMIT.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);

        Map<String, Map<Integer, Errors>> topicsData = CollectionUtils.groupPartitionDataByTopic(responseData);
        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Errors>> entries: topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS);
            topicData.set(TOPIC_NAME, entries.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, Errors> partitionEntry : entries.getValue().entrySet()) {
                Struct partitionData = topicData.instance(PARTITIONS);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(ERROR_CODE, partitionEntry.getValue().code());
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS, topicArray.toArray());

        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<TopicPartition, Errors> responseData() {
        return responseData;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(responseData);
    }

    public static OffsetCommitResponse parse(ByteBuffer buffer, short version) {
        return new OffsetCommitResponse(ApiKeys.OFFSET_COMMIT.parseResponse(version, buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 4;
    }
}
