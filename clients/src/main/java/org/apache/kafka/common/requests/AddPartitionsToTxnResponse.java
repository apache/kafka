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
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AddPartitionsToTxnResponse extends AbstractResponse {
    private static final String THROTTLE_TIME_KEY_NAME = "throttle_time_ms";
    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String ERRORS_KEY_NAME = "errors";
    private static final String TOPIC_NAME = "topic";
    private static final String PARTITION = "partition";
    private static final String PARTITION_ERRORS = "partition_errors";

    private final int throttleTimeMs;

    // Possible error codes:
    //   NotCoordinator
    //   CoordinatorNotAvailable
    //   CoordinatorLoadInProgress
    //   InvalidTxnState
    //   InvalidPidMapping
    //   TopicAuthorizationFailed
    //   InvalidProducerEpoch
    //   UnknownTopicOrPartition
    //   TopicAuthorizationFailed
    private final Map<TopicPartition, Errors> errors;

    public AddPartitionsToTxnResponse(int throttleTimeMs, Map<TopicPartition, Errors> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;
    }

    public AddPartitionsToTxnResponse(Struct struct) {
        this.throttleTimeMs = struct.getInt(THROTTLE_TIME_KEY_NAME);
        errors = new HashMap<>();
        for (Object topic : struct.getArray(ERRORS_KEY_NAME)) {
            Struct topicStruct = (Struct) topic;
            final String topicName = topicStruct.getString(TOPIC_NAME);
            for (Object partition : topicStruct.getArray(PARTITION_ERRORS)) {
                Struct partitionStruct = (Struct) partition;
                TopicPartition topicPartition = new TopicPartition(topicName, partitionStruct.getInt(PARTITION));
                errors.put(topicPartition, Errors.forCode(partitionStruct.getShort(ERROR_CODE_KEY_NAME)));
            }
        }
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<TopicPartition, Errors> errors() {
        return errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.ADD_PARTITIONS_TO_TXN.responseSchema(version));
        struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);

        Map<String, Map<Integer, Errors>> errorsByTopic = CollectionUtils.groupDataByTopic(errors);
        List<Struct> topics = new ArrayList<>(errorsByTopic.size());
        for (Map.Entry<String, Map<Integer, Errors>> entry : errorsByTopic.entrySet()) {
            Struct topicErrorCodes = struct.instance(ERRORS_KEY_NAME);
            topicErrorCodes.set(TOPIC_NAME, entry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, Errors> partitionErrors : entry.getValue().entrySet()) {
                final Struct partitionData = topicErrorCodes.instance(PARTITION_ERRORS)
                        .set(PARTITION, partitionErrors.getKey())
                        .set(ERROR_CODE_KEY_NAME, partitionErrors.getValue().code());
                partitionArray.add(partitionData);

            }
            topicErrorCodes.set(PARTITION_ERRORS, partitionArray.toArray());
            topics.add(topicErrorCodes);
        }
        struct.set(ERRORS_KEY_NAME, topics.toArray());
        return struct;
    }

    public static AddPartitionsToTxnResponse parse(ByteBuffer buffer, short version) {
        return new AddPartitionsToTxnResponse(ApiKeys.ADD_PARTITIONS_TO_TXN.parseResponse(version, buffer));
    }
}
