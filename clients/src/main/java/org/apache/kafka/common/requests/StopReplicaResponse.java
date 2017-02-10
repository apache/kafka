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

public class StopReplicaResponse extends AbstractResponse {
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.STOP_REPLICA.id);

    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final String PARTITIONS_TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_PARTITION_KEY_NAME = "partition";
    private static final String PARTITIONS_ERROR_CODE_KEY_NAME = "error_code";

    private final Map<TopicPartition, Errors> responses;
    private final Errors error;

    /**
     * Possible error code:
     *
     * STALE_CONTROLLER_EPOCH (11)
     */

    public StopReplicaResponse(Map<TopicPartition, Errors> responses) {
        this(Errors.NONE, responses);
    }

    public StopReplicaResponse(Errors error, Map<TopicPartition, Errors> responses) {
        super(new Struct(CURRENT_SCHEMA));

        List<Struct> responseDatas = new ArrayList<>(responses.size());
        for (Map.Entry<TopicPartition, Errors> response : responses.entrySet()) {
            Struct partitionData = struct.instance(PARTITIONS_KEY_NAME);
            TopicPartition partition = response.getKey();
            partitionData.set(PARTITIONS_TOPIC_KEY_NAME, partition.topic());
            partitionData.set(PARTITIONS_PARTITION_KEY_NAME, partition.partition());
            partitionData.set(PARTITIONS_ERROR_CODE_KEY_NAME, response.getValue().code());
            responseDatas.add(partitionData);
        }

        struct.set(PARTITIONS_KEY_NAME, responseDatas.toArray());
        struct.set(ERROR_CODE_KEY_NAME, error.code());

        this.responses = responses;
        this.error = error;
    }

    public StopReplicaResponse(Struct struct) {
        super(struct);

        responses = new HashMap<>();
        for (Object responseDataObj : struct.getArray(PARTITIONS_KEY_NAME)) {
            Struct responseData = (Struct) responseDataObj;
            String topic = responseData.getString(PARTITIONS_TOPIC_KEY_NAME);
            int partition = responseData.getInt(PARTITIONS_PARTITION_KEY_NAME);
            Errors error = Errors.forCode(responseData.getShort(PARTITIONS_ERROR_CODE_KEY_NAME));
            responses.put(new TopicPartition(topic, partition), error);
        }

        error = Errors.forCode(struct.getShort(ERROR_CODE_KEY_NAME));
    }

    public Map<TopicPartition, Errors> responses() {
        return responses;
    }

    public Errors error() {
        return error;
    }

    public static StopReplicaResponse parse(ByteBuffer buffer, int versionId) {
        return new StopReplicaResponse(ProtoUtils.parseRequest(ApiKeys.STOP_REPLICA.id, versionId, buffer));
    }

    public static StopReplicaResponse parse(ByteBuffer buffer) {
        return new StopReplicaResponse(CURRENT_SCHEMA.read(buffer));
    }
}
