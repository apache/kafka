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
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT64;

public class OffsetsForLeaderEpochResponse extends AbstractResponse {
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String PARTITIONS_KEY_NAME = "partitions";
    private static final String END_OFFSET_KEY_NAME = "end_offset";

    private static final Schema OFFSET_FOR_LEADER_EPOCH_RESPONSE_PARTITION_V0 = new Schema(
            ERROR_CODE,
            PARTITION_ID,
            new Field(END_OFFSET_KEY_NAME, INT64, "The end offset"));
    private static final Schema OFFSET_FOR_LEADER_EPOCH_RESPONSE_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_FOR_LEADER_EPOCH_RESPONSE_PARTITION_V0)));
    private static final Schema OFFSET_FOR_LEADER_EPOCH_RESPONSE_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_FOR_LEADER_EPOCH_RESPONSE_TOPIC_V0),
                    "An array of topics for which we have leader offsets for some requested Partition Leader Epoch"));

    public static Schema[] schemaVersions() {
        return new Schema[]{OFFSET_FOR_LEADER_EPOCH_RESPONSE_V0};
    }

    private Map<TopicPartition, EpochEndOffset> epochEndOffsetsByPartition;

    public OffsetsForLeaderEpochResponse(Struct struct) {
        epochEndOffsetsByPartition = new HashMap<>();
        for (Object topicAndEpocsObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicAndEpochs = (Struct) topicAndEpocsObj;
            String topic = topicAndEpochs.get(TOPIC_NAME);
            for (Object partitionAndEpochObj : topicAndEpochs.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionAndEpoch = (Struct) partitionAndEpochObj;
                Errors error = Errors.forCode(partitionAndEpoch.get(ERROR_CODE));
                int partitionId = partitionAndEpoch.get(PARTITION_ID);
                TopicPartition tp = new TopicPartition(topic, partitionId);
                long endOffset = partitionAndEpoch.getLong(END_OFFSET_KEY_NAME);
                epochEndOffsetsByPartition.put(tp, new EpochEndOffset(error, endOffset));
            }
        }
    }

    public OffsetsForLeaderEpochResponse(Map<TopicPartition, EpochEndOffset> epochsByTopic) {
        this.epochEndOffsetsByPartition = epochsByTopic;
    }

    public Map<TopicPartition, EpochEndOffset> responses() {
        return epochEndOffsetsByPartition;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (EpochEndOffset response : epochEndOffsetsByPartition.values())
            updateErrorCounts(errorCounts, response.error());
        return errorCounts;
    }

    public static OffsetsForLeaderEpochResponse parse(ByteBuffer buffer, short versionId) {
        return new OffsetsForLeaderEpochResponse(ApiKeys.OFFSET_FOR_LEADER_EPOCH.responseSchema(versionId).read(buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct responseStruct = new Struct(ApiKeys.OFFSET_FOR_LEADER_EPOCH.responseSchema(version));

        Map<String, Map<Integer, EpochEndOffset>> endOffsetsByTopic = CollectionUtils.groupDataByTopic(epochEndOffsetsByPartition);

        List<Struct> topics = new ArrayList<>(endOffsetsByTopic.size());
        for (Map.Entry<String, Map<Integer, EpochEndOffset>> topicToPartitionEpochs : endOffsetsByTopic.entrySet()) {
            Struct topicStruct = responseStruct.instance(TOPICS_KEY_NAME);
            topicStruct.set(TOPIC_NAME, topicToPartitionEpochs.getKey());
            Map<Integer, EpochEndOffset> partitionEpochs = topicToPartitionEpochs.getValue();
            List<Struct> partitions = new ArrayList<>();
            for (Map.Entry<Integer, EpochEndOffset> partitionEndOffset : partitionEpochs.entrySet()) {
                Struct partitionStruct = topicStruct.instance(PARTITIONS_KEY_NAME);
                partitionStruct.set(ERROR_CODE, partitionEndOffset.getValue().error().code());
                partitionStruct.set(PARTITION_ID, partitionEndOffset.getKey());
                partitionStruct.set(END_OFFSET_KEY_NAME, partitionEndOffset.getValue().endOffset());
                partitions.add(partitionStruct);
            }
            topicStruct.set(PARTITIONS_KEY_NAME, partitions.toArray());
            topics.add(topicStruct);
        }
        responseStruct.set(TOPICS_KEY_NAME, topics.toArray());
        return responseStruct;
    }
}
