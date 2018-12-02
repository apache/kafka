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


public class AlterReplicaLogDirsResponse extends AbstractResponse {

    // request level key names
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level key names
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final Schema ALTER_REPLICA_LOG_DIRS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(TOPICS_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITIONS_KEY_NAME, new ArrayOf(new Schema(
                            PARTITION_ID,
                            ERROR_CODE)))))));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema ALTER_REPLICA_LOG_DIRS_RESPONSE_V1 = ALTER_REPLICA_LOG_DIRS_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{ALTER_REPLICA_LOG_DIRS_RESPONSE_V0, ALTER_REPLICA_LOG_DIRS_RESPONSE_V1};
    }

    /**
     * Possible error code:
     *
     * LOG_DIR_NOT_FOUND (57)
     * KAFKA_STORAGE_ERROR (56)
     * REPLICA_NOT_AVAILABLE (9)
     * UNKNOWN (-1)
     */
    private final Map<TopicPartition, Errors> responses;
    private final int throttleTimeMs;

    public AlterReplicaLogDirsResponse(Struct struct) {
        throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        responses = new HashMap<>();
        for (Object topicStructObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicStruct = (Struct) topicStructObj;
            String topic = topicStruct.get(TOPIC_NAME);
            for (Object partitionStructObj : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionStruct = (Struct) partitionStructObj;
                int partition = partitionStruct.get(PARTITION_ID);
                Errors error = Errors.forCode(partitionStruct.get(ERROR_CODE));
                responses.put(new TopicPartition(topic, partition), error);
            }
        }
    }

    /**
     * Constructor for version 0.
     */
    public AlterReplicaLogDirsResponse(int throttleTimeMs, Map<TopicPartition, Errors> responses) {
        this.throttleTimeMs = throttleTimeMs;
        this.responses = responses;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.ALTER_REPLICA_LOG_DIRS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        Map<String, Map<Integer, Errors>> responsesByTopic = CollectionUtils.groupPartitionDataByTopic(responses);
        List<Struct> topicStructArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Errors>> responsesByTopicEntry : responsesByTopic.entrySet()) {
            Struct topicStruct = struct.instance(TOPICS_KEY_NAME);
            topicStruct.set(TOPIC_NAME, responsesByTopicEntry.getKey());
            List<Struct> partitionStructArray = new ArrayList<>();
            for (Map.Entry<Integer, Errors> responsesByPartitionEntry : responsesByTopicEntry.getValue().entrySet()) {
                Struct partitionStruct = topicStruct.instance(PARTITIONS_KEY_NAME);
                Errors response = responsesByPartitionEntry.getValue();
                partitionStruct.set(PARTITION_ID, responsesByPartitionEntry.getKey());
                partitionStruct.set(ERROR_CODE, response.code());
                partitionStructArray.add(partitionStruct);
            }
            topicStruct.set(PARTITIONS_KEY_NAME, partitionStructArray.toArray());
            topicStructArray.add(topicStruct);
        }
        struct.set(TOPICS_KEY_NAME, topicStructArray.toArray());
        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<TopicPartition, Errors> responses() {
        return this.responses;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(responses);
    }

    public static AlterReplicaLogDirsResponse parse(ByteBuffer buffer, short version) {
        return new AlterReplicaLogDirsResponse(ApiKeys.ALTER_REPLICA_LOG_DIRS.responseSchema(version).read(buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
