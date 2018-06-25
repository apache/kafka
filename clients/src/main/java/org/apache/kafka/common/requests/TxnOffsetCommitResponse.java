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
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

public class TxnOffsetCommitResponse extends AbstractResponse {
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final Schema TXN_OFFSET_COMMIT_PARTITION_ERROR_RESPONSE_V0 = new Schema(
            PARTITION_ID,
            ERROR_CODE);

    private static final Schema TXN_OFFSET_COMMIT_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(TOPICS_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITIONS_KEY_NAME, new ArrayOf(TXN_OFFSET_COMMIT_PARTITION_ERROR_RESPONSE_V0)))),
                    "Errors per partition from writing markers."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema TXN_OFFSET_COMMIT_RESPONSE_V1 = TXN_OFFSET_COMMIT_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{TXN_OFFSET_COMMIT_RESPONSE_V0, TXN_OFFSET_COMMIT_RESPONSE_V1};
    }

    // Possible error codes:
    //   InvalidProducerEpoch
    //   NotCoordinator
    //   CoordinatorNotAvailable
    //   CoordinatorLoadInProgress
    //   OffsetMetadataTooLarge
    //   GroupAuthorizationFailed
    //   InvalidCommitOffsetSize
    //   TransactionalIdAuthorizationFailed
    //   RequestTimedOut

    private final Map<TopicPartition, Errors> errors;
    private final int throttleTimeMs;

    public TxnOffsetCommitResponse(int throttleTimeMs, Map<TopicPartition, Errors> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;
    }

    public TxnOffsetCommitResponse(Struct struct) {
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        Map<TopicPartition, Errors> errors = new HashMap<>();
        Object[] topicPartitionsArray = struct.getArray(TOPICS_KEY_NAME);
        for (Object topicPartitionObj : topicPartitionsArray) {
            Struct topicPartitionStruct = (Struct) topicPartitionObj;
            String topic = topicPartitionStruct.get(TOPIC_NAME);
            for (Object partitionObj : topicPartitionStruct.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionStruct = (Struct) partitionObj;
                Integer partition = partitionStruct.get(PARTITION_ID);
                Errors error = Errors.forCode(partitionStruct.get(ERROR_CODE));
                errors.put(new TopicPartition(topic, partition), error);
            }
        }
        this.errors = errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.TXN_OFFSET_COMMIT.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        Map<String, Map<Integer, Errors>> mappedPartitions = CollectionUtils.groupDataByTopic(errors);
        Object[] partitionsArray = new Object[mappedPartitions.size()];
        int i = 0;
        for (Map.Entry<String, Map<Integer, Errors>> topicAndPartitions : mappedPartitions.entrySet()) {
            Struct topicPartitionsStruct = struct.instance(TOPICS_KEY_NAME);
            topicPartitionsStruct.set(TOPIC_NAME, topicAndPartitions.getKey());
            Map<Integer, Errors> partitionAndErrors = topicAndPartitions.getValue();

            Object[] partitionAndErrorsArray = new Object[partitionAndErrors.size()];
            int j = 0;
            for (Map.Entry<Integer, Errors> partitionAndError : partitionAndErrors.entrySet()) {
                Struct partitionAndErrorStruct = topicPartitionsStruct.instance(PARTITIONS_KEY_NAME);
                partitionAndErrorStruct.set(PARTITION_ID, partitionAndError.getKey());
                partitionAndErrorStruct.set(ERROR_CODE, partitionAndError.getValue().code());
                partitionAndErrorsArray[j++] = partitionAndErrorStruct;
            }
            topicPartitionsStruct.set(PARTITIONS_KEY_NAME, partitionAndErrorsArray);
            partitionsArray[i++] = topicPartitionsStruct;
        }

        struct.set(TOPICS_KEY_NAME, partitionsArray);
        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<TopicPartition, Errors> errors() {
        return errors;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(errors);
    }

    public static TxnOffsetCommitResponse parse(ByteBuffer buffer, short version) {
        return new TxnOffsetCommitResponse(ApiKeys.TXN_OFFSET_COMMIT.parseResponse(version, buffer));
    }

    @Override
    public String toString() {
        return "TxnOffsetCommitResponse(" +
                "errors=" + errors +
                ", throttleTimeMs=" + throttleTimeMs +
                ')';
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
