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
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This wrapper supports both v0 and v1 of ProduceResponse.
 */
public class ProduceResponse extends AbstractRequestResponse {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.PRODUCE.id);
    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITION_RESPONSES_KEY_NAME = "partition_responses";
    private static final String THROTTLE_TIME_KEY_NAME = "throttle_time_ms";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    public static final long INVALID_OFFSET = -1L;
    public static final int DEFAULT_THROTTLE_TIME = 0;

    /**
     * Possible error code:
     *
     * TODO
     */

    private static final String BASE_OFFSET_KEY_NAME = "base_offset";

    private final Map<TopicPartition, PartitionResponse> responses;
    private final int throttleTime;

    /**
     * Constructor for Version 0
     * @param responses Produced data grouped by topic-partition
     */
    public ProduceResponse(Map<TopicPartition, PartitionResponse> responses) {
        super(new Struct(ProtoUtils.responseSchema(ApiKeys.PRODUCE.id, 0)));
        initCommonFields(responses);
        this.responses = responses;
        this.throttleTime = DEFAULT_THROTTLE_TIME;
    }

    /**
     * Constructor for Version 1
     * @param responses Produced data grouped by topic-partition
     * @param throttleTime Time in milliseconds the response was throttled
     */
    public ProduceResponse(Map<TopicPartition, PartitionResponse> responses, int throttleTime) {
        super(new Struct(CURRENT_SCHEMA));
        initCommonFields(responses);
        struct.set(THROTTLE_TIME_KEY_NAME, throttleTime);
        this.responses = responses;
        this.throttleTime = throttleTime;
    }

    public ProduceResponse(Struct struct) {
        super(struct);
        responses = new HashMap<TopicPartition, PartitionResponse>();
        for (Object topicResponse : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicRespStruct = (Struct) topicResponse;
            String topic = topicRespStruct.getString(TOPIC_KEY_NAME);
            for (Object partResponse : topicRespStruct.getArray(PARTITION_RESPONSES_KEY_NAME)) {
                Struct partRespStruct = (Struct) partResponse;
                int partition = partRespStruct.getInt(PARTITION_KEY_NAME);
                short errorCode = partRespStruct.getShort(ERROR_CODE_KEY_NAME);
                long offset = partRespStruct.getLong(BASE_OFFSET_KEY_NAME);
                TopicPartition tp = new TopicPartition(topic, partition);
                responses.put(tp, new PartitionResponse(errorCode, offset));
            }
        }
        this.throttleTime = struct.getInt(THROTTLE_TIME_KEY_NAME);
    }

    private void initCommonFields(Map<TopicPartition, PartitionResponse> responses) {
        Map<String, Map<Integer, PartitionResponse>> responseByTopic = CollectionUtils.groupDataByTopic(responses);
        List<Struct> topicDatas = new ArrayList<Struct>(responseByTopic.size());
        for (Map.Entry<String, Map<Integer, PartitionResponse>> entry : responseByTopic.entrySet()) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, entry.getKey());
            List<Struct> partitionArray = new ArrayList<Struct>();
            for (Map.Entry<Integer, PartitionResponse> partitionEntry : entry.getValue().entrySet()) {
                PartitionResponse part = partitionEntry.getValue();
                Struct partStruct = topicData.instance(PARTITION_RESPONSES_KEY_NAME).set(PARTITION_KEY_NAME,
                                                                                         partitionEntry.getKey()).set(
                    ERROR_CODE_KEY_NAME, part.errorCode).set(BASE_OFFSET_KEY_NAME, part.baseOffset);
                partitionArray.add(partStruct);
            }
            topicData.set(PARTITION_RESPONSES_KEY_NAME, partitionArray.toArray());
            topicDatas.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicDatas.toArray());
    }

    public Map<TopicPartition, PartitionResponse> responses() {
        return this.responses;
    }

    public int getThrottleTime() {
        return this.throttleTime;
    }

    public static final class PartitionResponse {
        public short errorCode;
        public long baseOffset;

        public PartitionResponse(short errorCode, long baseOffset) {
            this.errorCode = errorCode;
            this.baseOffset = baseOffset;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append('{');
            b.append("error: ");
            b.append(errorCode);
            b.append(",offset: ");
            b.append(baseOffset);
            b.append('}');
            return b.toString();
        }
    }

    public static ProduceResponse parse(ByteBuffer buffer) {
        return new ProduceResponse(CURRENT_SCHEMA.read(buffer));
    }
}
