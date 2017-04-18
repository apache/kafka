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

public class OffsetsForLeaderEpochResponse extends AbstractResponse {
    public static final String TOPICS = "topics";
    public static final String TOPIC = "topic";
    public static final String PARTITIONS = "partitions";
    public static final String ERROR_CODE = "error_code";
    public static final String PARTITION_ID = "partition_id";
    public static final String END_OFFSET = "end_offset";

    private Map<TopicPartition, EpochEndOffset> epochEndOffsetsByPartition;

    public OffsetsForLeaderEpochResponse(Struct struct) {
        epochEndOffsetsByPartition = new HashMap<>();
        for (Object topicAndEpocsObj : struct.getArray(TOPICS)) {
            Struct topicAndEpochs = (Struct) topicAndEpocsObj;
            String topic = topicAndEpochs.getString(TOPIC);
            for (Object partitionAndEpochObj : topicAndEpochs.getArray(PARTITIONS)) {
                Struct partitionAndEpoch = (Struct) partitionAndEpochObj;
                Errors error = Errors.forCode(partitionAndEpoch.getShort(ERROR_CODE));
                int partitionId = partitionAndEpoch.getInt(PARTITION_ID);
                TopicPartition tp = new TopicPartition(topic, partitionId);
                long endOffset = partitionAndEpoch.getLong(END_OFFSET);
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

    public static OffsetsForLeaderEpochResponse parse(ByteBuffer buffer, short versionId) {
        return new OffsetsForLeaderEpochResponse(ApiKeys.OFFSET_FOR_LEADER_EPOCH.responseSchema(versionId).read(buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct responseStruct = new Struct(ApiKeys.OFFSET_FOR_LEADER_EPOCH.responseSchema(version));

        Map<String, Map<Integer, EpochEndOffset>> endOffsetsByTopic = CollectionUtils.groupDataByTopic(epochEndOffsetsByPartition);

        List<Struct> topics = new ArrayList<>(endOffsetsByTopic.size());
        for (Map.Entry<String, Map<Integer, EpochEndOffset>> topicToPartitionEpochs : endOffsetsByTopic.entrySet()) {
            Struct topicStruct = responseStruct.instance(TOPICS);
            topicStruct.set(TOPIC, topicToPartitionEpochs.getKey());
            Map<Integer, EpochEndOffset> partitionEpochs = topicToPartitionEpochs.getValue();
            List<Struct> partitions = new ArrayList<>();
            for (Map.Entry<Integer, EpochEndOffset> partitionEndOffset : partitionEpochs.entrySet()) {
                Struct partitionStruct = topicStruct.instance(PARTITIONS);
                partitionStruct.set(ERROR_CODE, partitionEndOffset.getValue().error().code());
                partitionStruct.set(PARTITION_ID, partitionEndOffset.getKey());
                partitionStruct.set(END_OFFSET, partitionEndOffset.getValue().endOffset());
                partitions.add(partitionStruct);
            }
            topicStruct.set(PARTITIONS, partitions.toArray());
            topics.add(topicStruct);
        }
        responseStruct.set(TOPICS, topics.toArray());
        return responseStruct;
    }
}
