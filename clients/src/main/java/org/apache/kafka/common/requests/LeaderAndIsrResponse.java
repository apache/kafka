/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LeaderAndIsrResponse extends AbstractRequestResponse {
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.LEADER_AND_ISR.id);

    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final String PARTITIONS_TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_PARTITION_KEY_NAME = "partition";
    private static final String PARTITIONS_ERROR_CODE_KEY_NAME = "error_code";

    /**
     * Possible error code:
     *
     * STALE_CONTROLLER_EPOCH (11)
     */
    private final short errorCode;

    private final Map<TopicPartition, Short> responses;

    public LeaderAndIsrResponse(Map<TopicPartition, Short> responses) {
        this(Errors.NONE.code(), responses);
    }

    public LeaderAndIsrResponse(short errorCode, Map<TopicPartition, Short> responses) {
        super(new Struct(CURRENT_SCHEMA));

        struct.set(ERROR_CODE_KEY_NAME, errorCode);

        List<Struct> responseDatas = new ArrayList<>(responses.size());
        for (Map.Entry<TopicPartition, Short> response : responses.entrySet()) {
            Struct partitionData = struct.instance(PARTITIONS_KEY_NAME);
            TopicPartition partition = response.getKey();
            partitionData.set(PARTITIONS_TOPIC_KEY_NAME, partition.topic());
            partitionData.set(PARTITIONS_PARTITION_KEY_NAME, partition.partition());
            partitionData.set(PARTITIONS_ERROR_CODE_KEY_NAME, response.getValue());
            responseDatas.add(partitionData);
        }

        struct.set(PARTITIONS_KEY_NAME, responseDatas.toArray());
        struct.set(ERROR_CODE_KEY_NAME, errorCode);

        this.responses = responses;
        this.errorCode = errorCode;
    }

    public LeaderAndIsrResponse(Struct struct) {
        super(struct);

        responses = new HashMap<>();
        for (Object responseDataObj : struct.getArray(PARTITIONS_KEY_NAME)) {
            Struct responseData = (Struct) responseDataObj;
            String topic = responseData.getString(PARTITIONS_TOPIC_KEY_NAME);
            int partition = responseData.getInt(PARTITIONS_PARTITION_KEY_NAME);
            short errorCode = responseData.getShort(PARTITIONS_ERROR_CODE_KEY_NAME);
            responses.put(new TopicPartition(topic, partition), errorCode);
        }

        errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
    }

    public Map<TopicPartition, Short> responses() {
        return responses;
    }

    public short errorCode() {
        return errorCode;
    }

    public static LeaderAndIsrResponse parse(ByteBuffer buffer, int version) {
        return new LeaderAndIsrResponse(ProtoUtils.parseResponse(ApiKeys.LEADER_AND_ISR.id, version, buffer));
    }

    public static LeaderAndIsrResponse parse(ByteBuffer buffer) {
        return new LeaderAndIsrResponse(CURRENT_SCHEMA.read(buffer));
    }

}
