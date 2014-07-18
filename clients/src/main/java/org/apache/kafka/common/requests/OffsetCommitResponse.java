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

public class OffsetCommitResponse extends AbstractRequestResponse {
    public static Schema curSchema = ProtoUtils.currentResponseSchema(ApiKeys.OFFSET_COMMIT.id);
    private static String RESPONSES_KEY_NAME = "responses";

    // topic level fields
    private static String TOPIC_KEY_NAME = "topic";
    private static String PARTITIONS_KEY_NAME = "partition_responses";

    // partition level fields
    private static String PARTITION_KEY_NAME = "partition";
    private static String ERROR_CODE_KEY_NAME = "error_code";

    private final Map<TopicPartition, Short> responseData;

    public OffsetCommitResponse(Map<TopicPartition, Short> responseData) {
        super(new Struct(curSchema));

        Map<String, Map<Integer, Short>> topicsData = CollectionUtils.groupDataByTopic(responseData);

        List<Struct> topicArray = new ArrayList<Struct>();
        for (Map.Entry<String, Map<Integer, Short>> entries: topicsData.entrySet()) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, entries.getKey());
            List<Struct> partitionArray = new ArrayList<Struct>();
            for (Map.Entry<Integer, Short> partitionEntry : entries.getValue().entrySet()) {
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionData.set(ERROR_CODE_KEY_NAME, partitionEntry.getValue());
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicArray.toArray());
        this.responseData = responseData;
    }

    public OffsetCommitResponse(Struct struct) {
        super(struct);
        responseData = new HashMap<TopicPartition, Short>();
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                short errorCode = partitionResponse.getShort(ERROR_CODE_KEY_NAME);
                responseData.put(new TopicPartition(topic, partition), errorCode);
            }
        }
    }

    public Map<TopicPartition, Short> responseData() {
        return responseData;
    }

    public static OffsetCommitResponse parse(ByteBuffer buffer) {
        return new OffsetCommitResponse(((Struct) curSchema.read(buffer)));
    }
}
