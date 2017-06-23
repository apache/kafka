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

public class DeleteRecordsResponse extends AbstractResponse {

    public static final long INVALID_LOW_WATERMARK = -1L;

    // request level key names
    private static final String THROTTLE_TIME_KEY_NAME = "throttle_time_ms";
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level key names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level key names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String LOW_WATERMARK_KEY_NAME = "low_watermark";
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    private final int throttleTimeMs;
    private final Map<TopicPartition, PartitionResponse> responses;

    /**
     * Possible error code:
     *
     * OFFSET_OUT_OF_RANGE (1)
     * UNKNOWN_TOPIC_OR_PARTITION (3)
     * NOT_LEADER_FOR_PARTITION (6)
     * REQUEST_TIMED_OUT (7)
     * NOT_ENOUGH_REPLICAS (19)
     * UNKNOWN (-1)
     */

    public static final class PartitionResponse {
        public long lowWatermark;
        public Errors error;

        public PartitionResponse(long lowWatermark, Errors error) {
            this.lowWatermark = lowWatermark;
            this.error = error;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append('{')
                   .append(",low_watermark: ")
                   .append(lowWatermark)
                   .append("error: ")
                   .append(error.toString())
                   .append('}');
            return builder.toString();
        }
    }

    public DeleteRecordsResponse(Struct struct) {
        this.throttleTimeMs = struct.hasField(THROTTLE_TIME_KEY_NAME) ? struct.getInt(THROTTLE_TIME_KEY_NAME) : DEFAULT_THROTTLE_TIME;
        responses = new HashMap<>();
        for (Object topicStructObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicStruct = (Struct) topicStructObj;
            String topic = topicStruct.getString(TOPIC_KEY_NAME);
            for (Object partitionStructObj : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionStruct = (Struct) partitionStructObj;
                int partition = partitionStruct.getInt(PARTITION_KEY_NAME);
                long lowWatermark = partitionStruct.getLong(LOW_WATERMARK_KEY_NAME);
                Errors error = Errors.forCode(partitionStruct.getShort(ERROR_CODE_KEY_NAME));
                responses.put(new TopicPartition(topic, partition), new PartitionResponse(lowWatermark, error));
            }
        }
    }

    /**
     * Constructor for version 0.
     */
    public DeleteRecordsResponse(int throttleTimeMs, Map<TopicPartition, PartitionResponse> responses) {
        this.throttleTimeMs = throttleTimeMs;
        this.responses = responses;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DELETE_RECORDS.responseSchema(version));
        if (struct.hasField(THROTTLE_TIME_KEY_NAME))
            struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);
        Map<String, Map<Integer, PartitionResponse>> responsesByTopic = CollectionUtils.groupDataByTopic(responses);
        List<Struct> topicStructArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionResponse>> responsesByTopicEntry : responsesByTopic.entrySet()) {
            Struct topicStruct = struct.instance(TOPICS_KEY_NAME);
            topicStruct.set(TOPIC_KEY_NAME, responsesByTopicEntry.getKey());
            List<Struct> partitionStructArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionResponse> responsesByPartitionEntry : responsesByTopicEntry.getValue().entrySet()) {
                Struct partitionStruct = topicStruct.instance(PARTITIONS_KEY_NAME);
                PartitionResponse response = responsesByPartitionEntry.getValue();
                partitionStruct.set(PARTITION_KEY_NAME, responsesByPartitionEntry.getKey());
                partitionStruct.set(LOW_WATERMARK_KEY_NAME, response.lowWatermark);
                partitionStruct.set(ERROR_CODE_KEY_NAME, response.error.code());
                partitionStructArray.add(partitionStruct);
            }
            topicStruct.set(PARTITIONS_KEY_NAME, partitionStructArray.toArray());
            topicStructArray.add(topicStruct);
        }
        struct.set(TOPICS_KEY_NAME, topicStructArray.toArray());
        return struct;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<TopicPartition, PartitionResponse> responses() {
        return this.responses;
    }

    public static DeleteRecordsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteRecordsResponse(ApiKeys.DELETE_RECORDS.responseSchema(version).read(buffer));
    }
}