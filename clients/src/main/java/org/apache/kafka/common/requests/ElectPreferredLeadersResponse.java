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
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

public class ElectPreferredLeadersResponse extends AbstractResponse {

    private static final String REPLICA_ELECTION_RESULT_KEY_NAME = "replica_election_result";
    private static final String PARTITION_RESULTS_KEY_NAME = "partition_results";

    public static final Schema ELECT_PREFERRED_LEADERS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(REPLICA_ELECTION_RESULT_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITION_RESULTS_KEY_NAME, new ArrayOf(
                            new Schema(
                                    PARTITION_ID,
                                    ERROR_CODE,
                                    ERROR_MESSAGE)),
                            "The results for each partition")))));

    public static Schema[] schemaVersions() {
        return new Schema[]{ELECT_PREFERRED_LEADERS_RESPONSE_V0};
    }

    private final int throttleTimeMs;
    private final Map<TopicPartition, ApiError> errors;

    public ElectPreferredLeadersResponse(int throttleTimeMs, Map<TopicPartition, ApiError> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;
    }

    public ElectPreferredLeadersResponse(Struct struct) {
        throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        Object[] resourcesArray = struct.getArray(REPLICA_ELECTION_RESULT_KEY_NAME);
        errors = new HashMap<>(resourcesArray.length);
        for (Object partitionObj : resourcesArray) {
            Struct topicStruct = (Struct) partitionObj;
            String topicName = topicStruct.get(TOPIC_NAME);
            Object[] partitionResults = topicStruct.getArray(PARTITION_RESULTS_KEY_NAME);
            for (Object partitionResult : partitionResults) {
                Struct partitionStruct = (Struct) partitionResult;
                int partitionId = partitionStruct.get(PARTITION_ID);
                ApiError error = new ApiError(partitionStruct);
                errors.put(new TopicPartition(topicName, partitionId), error);
            }
        }
    }

    public Map<TopicPartition, ApiError> errors() {
        return errors;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return apiErrorCounts(errors);
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.ELECT_PREFERRED_LEADERS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        Map<String, Map<Integer, ApiError>> groupedErrors = CollectionUtils.groupDataByTopic(errors);
        List<Struct> replicaElectionResultList = new ArrayList<>(errors.size());
        for (Map.Entry<String, Map<Integer, ApiError>> topicToErrors : groupedErrors.entrySet()) {
            Struct topicStruct = struct.instance(REPLICA_ELECTION_RESULT_KEY_NAME);
            topicStruct.set(TOPIC_NAME, topicToErrors.getKey());
            List<Struct> partitionResultList = new ArrayList<>(topicToErrors.getValue().size());
            for (Map.Entry<Integer, ApiError> partitionToError : topicToErrors.getValue().entrySet()) {
                Struct partitionResultStruct = topicStruct.instance(PARTITION_RESULTS_KEY_NAME);
                partitionResultStruct.set(PARTITION_ID, partitionToError.getKey());
                partitionToError.getValue().write(partitionResultStruct);
                partitionResultList.add(partitionResultStruct);
            }
            topicStruct.set(PARTITION_RESULTS_KEY_NAME, partitionResultList.toArray());
            replicaElectionResultList.add(topicStruct);
        }
        struct.set(REPLICA_ELECTION_RESULT_KEY_NAME, replicaElectionResultList.toArray(new Struct[0]));
        return struct;
    }

    public static ElectPreferredLeadersResponse parse(ByteBuffer buffer, short version) {
        return new ElectPreferredLeadersResponse(ApiKeys.ELECT_PREFERRED_LEADERS.parseResponse(version, buffer));
    }

}
