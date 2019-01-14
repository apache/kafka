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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

public class ControlledShutdownResponse extends AbstractResponse {

    private static final String PARTITIONS_REMAINING_KEY_NAME = "partitions_remaining";

    private static final Schema CONTROLLED_SHUTDOWN_PARTITION_V0 = new Schema(
            TOPIC_NAME,
            PARTITION_ID);

    private static final Schema CONTROLLED_SHUTDOWN_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(PARTITIONS_REMAINING_KEY_NAME, new ArrayOf(CONTROLLED_SHUTDOWN_PARTITION_V0), "The partitions " +
                    "that the broker still leads."));

    private static final Schema CONTROLLED_SHUTDOWN_RESPONSE_V1 = CONTROLLED_SHUTDOWN_RESPONSE_V0;
    private static final Schema CONTROLLED_SHUTDOWN_RESPONSE_V2 = CONTROLLED_SHUTDOWN_RESPONSE_V1;

    public static Schema[] schemaVersions() {
        return new Schema[]{CONTROLLED_SHUTDOWN_RESPONSE_V0, CONTROLLED_SHUTDOWN_RESPONSE_V1, CONTROLLED_SHUTDOWN_RESPONSE_V2};
    }

    /**
     * Possible error codes:
     *
     * UNKNOWN(-1) (this is because IllegalStateException may be thrown in `KafkaController.shutdownBroker`, it would be good to improve this)
     * BROKER_NOT_AVAILABLE(8)
     * STALE_CONTROLLER_EPOCH(11)
     */
    private final Errors error;

    private final Set<TopicPartition> partitionsRemaining;

    public ControlledShutdownResponse(Errors error, Set<TopicPartition> partitionsRemaining) {
        this.error = error;
        this.partitionsRemaining = partitionsRemaining;
    }

    public ControlledShutdownResponse(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
        Set<TopicPartition> partitions = new HashSet<>();
        for (Object topicPartitionObj : struct.getArray(PARTITIONS_REMAINING_KEY_NAME)) {
            Struct topicPartition = (Struct) topicPartitionObj;
            String topic = topicPartition.get(TOPIC_NAME);
            int partition = topicPartition.get(PARTITION_ID);
            partitions.add(new TopicPartition(topic, partition));
        }
        partitionsRemaining = partitions;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    public Set<TopicPartition> partitionsRemaining() {
        return partitionsRemaining;
    }

    public static ControlledShutdownResponse parse(ByteBuffer buffer, short version) {
        return new ControlledShutdownResponse(ApiKeys.CONTROLLED_SHUTDOWN.parseResponse(version, buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CONTROLLED_SHUTDOWN.responseSchema(version));
        struct.set(ERROR_CODE, error.code());

        List<Struct> partitionsRemainingList = new ArrayList<>(partitionsRemaining.size());
        for (TopicPartition topicPartition : partitionsRemaining) {
            Struct topicPartitionStruct = struct.instance(PARTITIONS_REMAINING_KEY_NAME);
            topicPartitionStruct.set(TOPIC_NAME, topicPartition.topic());
            topicPartitionStruct.set(PARTITION_ID, topicPartition.partition());
            partitionsRemainingList.add(topicPartitionStruct);
        }
        struct.set(PARTITIONS_REMAINING_KEY_NAME, partitionsRemainingList.toArray());

        return struct;
    }
}
