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

public class ProduceResponse extends AbstractRequestResponse {
    private static Schema curSchema = ProtoUtils.currentResponseSchema(ApiKeys.PRODUCE.id);
    private static String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static String TOPIC_KEY_NAME = "topic";
    private static String PARTITION_RESPONSES_KEY_NAME = "partition_responses";

    // partition level field names
    private static String PARTITION_KEY_NAME = "partition";
    private static String ERROR_CODE_KEY_NAME = "error_code";
    private static String BASE_OFFSET_KEY_NAME = "base_offset";

    private final Map<TopicPartition, PartitionResponse> responses;

    public ProduceResponse(Map<TopicPartition, PartitionResponse> responses) {
        super(new Struct(curSchema));
        Map<String, Map<Integer, PartitionResponse>> responseByTopic = CollectionUtils.groupDataByTopic(responses);
        List<Struct> topicDatas = new ArrayList<Struct>(responseByTopic.size());
        for (Map.Entry<String, Map<Integer, PartitionResponse>> entry : responseByTopic.entrySet()) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, entry.getKey());
            List<Struct> partitionArray = new ArrayList<Struct>();
            for (Map.Entry<Integer, PartitionResponse> partitionEntry : entry.getValue().entrySet()) {
                PartitionResponse part = partitionEntry.getValue();
                Struct partStruct = topicData.instance(PARTITION_RESPONSES_KEY_NAME)
                                       .set(PARTITION_KEY_NAME, partitionEntry.getKey())
                                       .set(ERROR_CODE_KEY_NAME, part.errorCode)
                                       .set(BASE_OFFSET_KEY_NAME, part.baseOffset);
                partitionArray.add(partStruct);
            }
            topicData.set(PARTITION_RESPONSES_KEY_NAME, partitionArray.toArray());
            topicDatas.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicDatas.toArray());
        this.responses = responses;
    }

    public ProduceResponse(Struct struct) {
        super(struct);
        responses = new HashMap<TopicPartition, PartitionResponse>();
        for (Object topicResponse : struct.getArray("responses")) {
            Struct topicRespStruct = (Struct) topicResponse;
            String topic = topicRespStruct.getString("topic");
            for (Object partResponse : topicRespStruct.getArray("partition_responses")) {
                Struct partRespStruct = (Struct) partResponse;
                int partition = partRespStruct.getInt("partition");
                short errorCode = partRespStruct.getShort("error_code");
                long offset = partRespStruct.getLong("base_offset");
                TopicPartition tp = new TopicPartition(topic, partition);
                responses.put(tp, new PartitionResponse(errorCode, offset));
            }
        }
    }

    public Map<TopicPartition, PartitionResponse> responses() {
        return this.responses;
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
        return new ProduceResponse(((Struct) curSchema.read(buffer)));
    }
}
