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

public class OffsetFetchResponse extends AbstractRequestResponse {
    public static Schema curSchema = ProtoUtils.currentResponseSchema(ApiKeys.OFFSET_FETCH.id);
    private static String RESPONSES_KEY_NAME = "responses";

    // topic level fields
    private static String TOPIC_KEY_NAME = "topic";
    private static String PARTITIONS_KEY_NAME = "partition_responses";

    // partition level fields
    private static String PARTITION_KEY_NAME = "partition";
    private static String COMMIT_OFFSET_KEY_NAME = "offset";
    private static String METADATA_KEY_NAME = "metadata";
    private static String ERROR_CODE_KEY_NAME = "error_code";

    private final Map<TopicPartition,PartitionData> responseData;

    public static final class PartitionData {
        public final long offset;
        public final String metadata;
        public final short errorCode;

        public PartitionData(long offset, String metadata, short errorCode) {
            this.offset = offset;
            this.metadata = metadata;
            this.errorCode = errorCode;
        }
    }

    public OffsetFetchResponse(Map<TopicPartition, PartitionData> responseData) {
        super(new Struct(curSchema));

        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupDataByTopic(responseData);

        List<Struct> topicArray = new ArrayList<Struct>();
        for (Map.Entry<String, Map<Integer, PartitionData>> entries: topicsData.entrySet()) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, entries.getKey());
            List<Struct> partitionArray = new ArrayList<Struct>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : entries.getValue().entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionData.set(COMMIT_OFFSET_KEY_NAME, fetchPartitionData.offset);
                partitionData.set(METADATA_KEY_NAME, fetchPartitionData.metadata);
                partitionData.set(ERROR_CODE_KEY_NAME, fetchPartitionData.errorCode);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicArray.toArray());
        this.responseData = responseData;
    }

    public OffsetFetchResponse(Struct struct) {
        super(struct);
        responseData = new HashMap<TopicPartition, PartitionData>();
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                long offset = partitionResponse.getLong(COMMIT_OFFSET_KEY_NAME);
                String metadata = partitionResponse.getString(METADATA_KEY_NAME);
                short errorCode = partitionResponse.getShort(ERROR_CODE_KEY_NAME);
                PartitionData partitionData = new PartitionData(offset, metadata, errorCode);
                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
    }

    public Map<TopicPartition, PartitionData> responseData() {
        return responseData;
    }

    public static OffsetFetchResponse parse(ByteBuffer buffer) {
        return new OffsetFetchResponse(((Struct) curSchema.read(buffer)));
    }
}
