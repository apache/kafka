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
import org.apache.kafka.common.protocol.types.ArrayOf;
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

public class OffsetCommitResponse extends AbstractResponse {

    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level fields
    private static final String PARTITIONS_KEY_NAME = "partition_responses";

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

    private static final Schema OFFSET_COMMIT_RESPONSE_PARTITION_V0 = new Schema(
            PARTITION_ID,
            ERROR_CODE);

    private static final Schema OFFSET_COMMIT_RESPONSE_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_COMMIT_RESPONSE_PARTITION_V0)));

    private static final Schema OFFSET_COMMIT_RESPONSE_V0 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(OFFSET_COMMIT_RESPONSE_TOPIC_V0)));


    /* The response types for V0, V1 and V2 of OFFSET_COMMIT_REQUEST are the same. */
    private static final Schema OFFSET_COMMIT_RESPONSE_V1 = OFFSET_COMMIT_RESPONSE_V0;
    private static final Schema OFFSET_COMMIT_RESPONSE_V2 = OFFSET_COMMIT_RESPONSE_V0;

    private static final Schema OFFSET_COMMIT_RESPONSE_V3 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(OFFSET_COMMIT_RESPONSE_TOPIC_V0)));

    public static Schema[] schemaVersions() {
        return new Schema[] {OFFSET_COMMIT_RESPONSE_V0, OFFSET_COMMIT_RESPONSE_V1, OFFSET_COMMIT_RESPONSE_V2,
            OFFSET_COMMIT_RESPONSE_V3};
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
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
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

        Map<String, Map<Integer, Errors>> topicsData = CollectionUtils.groupDataByTopic(responseData);
        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Errors>> entries: topicsData.entrySet()) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_NAME, entries.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, Errors> partitionEntry : entries.getValue().entrySet()) {
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(ERROR_CODE, partitionEntry.getValue().code());
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicArray.toArray());

        return struct;
    }

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

}
