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

public class OffsetsForLeaderEpochRequest extends AbstractRequest {
    public static final String TOPICS = "topics";
    public static final String TOPIC = "topic";
    public static final String PARTITIONS = "partitions";
    public static final String PARTITION_ID = "partition_id";
    public static final String LEADER_EPOCH = "leader_epoch";

    private Map<TopicPartition, Integer> epochsByPartition;

    public Map<TopicPartition, Integer> epochsByTopicPartition() {
        return epochsByPartition;
    }

    public static class Builder extends AbstractRequest.Builder<OffsetsForLeaderEpochRequest> {
        private Map<TopicPartition, Integer> epochsByPartition = new HashMap();

        public Builder() {
            super(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
        }

        public Builder(Map<TopicPartition, Integer> epochsByPartition) {
            super(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
            this.epochsByPartition = epochsByPartition;
        }

        public Builder add(TopicPartition topicPartition, Integer epoch) {
            epochsByPartition.put(topicPartition, epoch);
            return this;
        }

        @Override
        public OffsetsForLeaderEpochRequest build(short version) {
            return new OffsetsForLeaderEpochRequest(epochsByPartition, version);
        }

        public static OffsetsForLeaderEpochRequest parse(ByteBuffer buffer, short version) {
            return new OffsetsForLeaderEpochRequest(ApiKeys.OFFSET_FOR_LEADER_EPOCH.parseRequest(version, buffer), version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=OffsetForLeaderEpochRequest, ").
                    append("epochsByTopic=").append(epochsByPartition).
                    append(")");
            return bld.toString();
        }
    }

    public OffsetsForLeaderEpochRequest(Map<TopicPartition, Integer> epochsByPartition, short version) {
        super(version);
        this.epochsByPartition = epochsByPartition;
    }

    public OffsetsForLeaderEpochRequest(Struct struct, short version) {
        super(version);
        epochsByPartition = new HashMap<>();
        for (Object t : struct.getArray(TOPICS)) {
            Struct topicAndEpochs = (Struct) t;
            String topic = topicAndEpochs.getString(TOPIC);
            for (Object e : topicAndEpochs.getArray(PARTITIONS)) {
                Struct partitionAndEpoch = (Struct) e;
                int partitionId = partitionAndEpoch.getInt(PARTITION_ID);
                int epoch = partitionAndEpoch.getInt(LEADER_EPOCH);
                TopicPartition tp = new TopicPartition(topic, partitionId);
                epochsByPartition.put(tp, epoch);
            }
        }
    }

    public static OffsetsForLeaderEpochRequest parse(ByteBuffer buffer, short versionId) {
        return new OffsetsForLeaderEpochRequest(ApiKeys.OFFSET_FOR_LEADER_EPOCH.parseRequest(versionId, buffer), versionId);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.OFFSET_FOR_LEADER_EPOCH.requestSchema(version()));

        //Group by topic
        Map<String, List<PartitionLeaderEpoch>> topicsToPartitionEpochs = new HashMap<>();
        for (TopicPartition tp : epochsByPartition.keySet()) {
            List<PartitionLeaderEpoch> partitionEndOffsets = topicsToPartitionEpochs.get(tp.topic());
            if (partitionEndOffsets == null)
                partitionEndOffsets = new ArrayList<>();
            partitionEndOffsets.add(new PartitionLeaderEpoch(tp.partition(), epochsByPartition.get(tp)));
            topicsToPartitionEpochs.put(tp.topic(), partitionEndOffsets);
        }

        List<Struct> topics = new ArrayList<>();
        for (Map.Entry<String, List<PartitionLeaderEpoch>> topicEpochs : topicsToPartitionEpochs.entrySet()) {
            Struct partition = struct.instance(TOPICS);
            String topic = topicEpochs.getKey();
            partition.set(TOPIC, topic);
            List<PartitionLeaderEpoch> partitionLeaderEpoches = topicEpochs.getValue();
            List<Struct> partitions = new ArrayList<>(partitionLeaderEpoches.size());
            for (PartitionLeaderEpoch partitionLeaderEpoch : partitionLeaderEpoches) {
                Struct partitionRow = partition.instance(PARTITIONS);
                partitionRow.set(PARTITION_ID, partitionLeaderEpoch.partitionId);
                partitionRow.set(LEADER_EPOCH, partitionLeaderEpoch.epoch);
                partitions.add(partitionRow);
            }
            partition.set(PARTITIONS, partitions.toArray());
            topics.add(partition);
        }
        struct.set(TOPICS, topics.toArray());
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        Errors error = Errors.forException(e);
        Map<TopicPartition, EpochEndOffset> errorResponse = new HashMap();
        for (TopicPartition tp : epochsByPartition.keySet()) {
            errorResponse.put(tp, new EpochEndOffset(error, EpochEndOffset.UNDEFINED_EPOCH_OFFSET));
        }
        return new OffsetsForLeaderEpochResponse(errorResponse);
    }

    private static class PartitionLeaderEpoch {
        final int partitionId;
        final int epoch;

        public PartitionLeaderEpoch(int partitionId, int epoch) {
            this.partitionId = partitionId;
            this.epoch = epoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PartitionLeaderEpoch other = (PartitionLeaderEpoch) o;

            if (partitionId != other.partitionId) return false;
            return epoch == other.epoch;
        }

        @Override
        public int hashCode() {
            int result = partitionId;
            result = 31 * result + epoch;
            return result;
        }
    }
}
