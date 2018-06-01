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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

public class LeaderAndIsrResponse extends AbstractResponse {
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final Schema LEADER_AND_ISR_RESPONSE_PARTITION_V0 = new Schema(
            TOPIC_NAME,
            PARTITION_ID,
            ERROR_CODE);
    private static final Schema LEADER_AND_ISR_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(LEADER_AND_ISR_RESPONSE_PARTITION_V0)));

    // LeaderAndIsrResponse V1 may receive KAFKA_STORAGE_ERROR in the response
    private static final Schema LEADER_AND_ISR_RESPONSE_V1 = LEADER_AND_ISR_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{LEADER_AND_ISR_RESPONSE_V0, LEADER_AND_ISR_RESPONSE_V1};
    }

    /**
     * Possible error code:
     *
     * STALE_CONTROLLER_EPOCH (11)
     */
    private final Errors error;

    private final Map<TopicPartition, Errors> responses;

    public LeaderAndIsrResponse(Errors error, Map<TopicPartition, Errors> responses) {
        this.responses = responses;
        this.error = error;
    }

    public LeaderAndIsrResponse(Struct struct) {
        responses = new HashMap<>();
        for (Object responseDataObj : struct.getArray(PARTITIONS_KEY_NAME)) {
            Struct responseData = (Struct) responseDataObj;
            String topic = responseData.get(TOPIC_NAME);
            int partition = responseData.get(PARTITION_ID);
            Errors error = Errors.forCode(responseData.get(ERROR_CODE));
            responses.put(new TopicPartition(topic, partition), error);
        }

        error = Errors.forCode(struct.get(ERROR_CODE));
    }

    public Map<TopicPartition, Errors> responses() {
        return responses;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        if (error != Errors.NONE)
            // Minor optimization since the top-level error applies to all partitions
            return Collections.singletonMap(error, responses.size());
        return errorCounts(responses);
    }

    public static LeaderAndIsrResponse parse(ByteBuffer buffer, short version) {
        return new LeaderAndIsrResponse(ApiKeys.LEADER_AND_ISR.parseResponse(version, buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.LEADER_AND_ISR.responseSchema(version));

        List<Struct> responseDatas = new ArrayList<>(responses.size());
        for (Map.Entry<TopicPartition, Errors> response : responses.entrySet()) {
            Struct partitionData = struct.instance(PARTITIONS_KEY_NAME);
            TopicPartition partition = response.getKey();
            partitionData.set(TOPIC_NAME, partition.topic());
            partitionData.set(PARTITION_ID, partition.partition());
            partitionData.set(ERROR_CODE, response.getValue().code());
            responseDatas.add(partitionData);
        }

        struct.set(PARTITIONS_KEY_NAME, responseDatas.toArray());
        struct.set(ERROR_CODE, error.code());

        return struct;
    }

    @Override
    public String toString() {
        return "LeaderAndIsrResponse(" +
                "responses=" + responses +
                ", error=" + error +
                ")";
    }

}
