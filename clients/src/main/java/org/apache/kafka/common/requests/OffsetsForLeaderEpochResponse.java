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
        for (Object t : struct.getArray(TOPICS)) {
            Struct topicAndEpochs = (Struct) t;
            String topic = topicAndEpochs.getString(TOPIC);
            for (Object e : topicAndEpochs.getArray(PARTITIONS)) {
                Struct partitionAndEpoch = (Struct) e;
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
        Struct struct = new Struct(ApiKeys.OFFSET_FOR_LEADER_EPOCH.responseSchema(version));

        //Group by topic
        Map<String, List<PartitionEndOffset>> topicsToPartitionEndOffsets = new HashMap<>();
        for (TopicPartition tp : epochEndOffsetsByPartition.keySet()) {
            List<PartitionEndOffset> partitionEndOffsets = topicsToPartitionEndOffsets.get(tp.topic());
            if (partitionEndOffsets == null)
                partitionEndOffsets = new ArrayList<>();
            partitionEndOffsets.add(new PartitionEndOffset(tp.partition(), epochEndOffsetsByPartition.get(tp)));
            topicsToPartitionEndOffsets.put(tp.topic(), partitionEndOffsets);
        }

        //Write struct
        List<Struct> topics = new ArrayList<>(topicsToPartitionEndOffsets.size());
        for (Map.Entry<String, List<PartitionEndOffset>> topicEpochs : topicsToPartitionEndOffsets.entrySet()) {
            Struct partition = struct.instance(TOPICS);
            String topic = topicEpochs.getKey();
            partition.set(TOPIC, topic);
            List<PartitionEndOffset> paritionEpochs = topicEpochs.getValue();
            List<Struct> paritions = new ArrayList<>(paritionEpochs.size());
            for (PartitionEndOffset peo : paritionEpochs) {
                Struct partitionRow = partition.instance(PARTITIONS);
                partitionRow.set(ERROR_CODE, peo.epochEndOffset.error().code());
                partitionRow.set(PARTITION_ID, peo.partition);
                partitionRow.set(END_OFFSET, peo.epochEndOffset.endOffset());
                paritions.add(partitionRow);
            }

            partition.set(PARTITIONS, paritions.toArray());
            topics.add(partition);
        }
        struct.set(TOPICS, topics.toArray());
        return struct;
    }

    private class PartitionEndOffset {
        private int partition;
        private EpochEndOffset epochEndOffset;

        PartitionEndOffset(int partition, EpochEndOffset epochEndOffset) {
            this.partition = partition;
            this.epochEndOffset = epochEndOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PartitionEndOffset that = (PartitionEndOffset) o;

            if (partition != that.partition) return false;
            return epochEndOffset != null ? epochEndOffset.equals(that.epochEndOffset) : that.epochEndOffset == null;
        }

        @Override
        public int hashCode() {
            int result = partition;
            result = 31 * result + (epochEndOffset != null ? epochEndOffset.hashCode() : 0);
            return result;
        }
    }
}