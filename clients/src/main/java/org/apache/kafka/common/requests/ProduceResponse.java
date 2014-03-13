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
import org.apache.kafka.common.protocol.types.Struct;

import java.util.HashMap;
import java.util.Map;

public class ProduceResponse {
    public class PartitionResponse {
        public int partitionId;
        public short errorCode;
        public long baseOffset;

        public PartitionResponse(int partitionId, short errorCode, long baseOffset) {
            this.partitionId = partitionId;
            this.errorCode = errorCode;
            this.baseOffset = baseOffset;
        }
        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append('{');
            b.append("pid: " + partitionId);
            b.append(",error: " + errorCode);
            b.append(",offset: " + baseOffset);
            b.append('}');
            return b.toString();
        }
    }

    private final Map<String, Map<TopicPartition, PartitionResponse>> responses;

    public ProduceResponse(Struct struct) {
        responses = new HashMap<String, Map<TopicPartition, PartitionResponse>>();
        for (Object topicResponse : (Object[]) struct.get("responses")) {
            Struct topicRespStruct = (Struct) topicResponse;
            String topic = (String) topicRespStruct.get("topic");
            Map<TopicPartition, PartitionResponse> topicResponses = new HashMap<TopicPartition, PartitionResponse>();
            for (Object partResponse : (Object[]) topicRespStruct.get("partition_responses")) {
                Struct partRespStruct = (Struct) partResponse;
                int partition = (Integer) partRespStruct.get("partition");
                short errorCode = (Short) partRespStruct.get("error_code");
                long offset = (Long) partRespStruct.get("base_offset");
                TopicPartition tp = new TopicPartition(topic, partition);
                topicResponses.put(tp, new PartitionResponse(partition, errorCode, offset));
            }
            responses.put(topic, topicResponses);
        }
    }

    public Map<String, Map<TopicPartition, PartitionResponse>> responses() {
        return this.responses;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('{');
        boolean isFirst = true;
        for (Map<TopicPartition, PartitionResponse> response : responses.values()) {
            for (Map.Entry<TopicPartition, PartitionResponse> entry : response.entrySet()) {
                if (isFirst)
                    isFirst = false;
                else
                    b.append(',');
                b.append(entry.getKey() + " : " + entry.getValue());
            }
        }
        b.append('}');
        return b.toString();
    }
}
