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
        for (Object topicAndEpochsObj : struct.getArray(TOPICS)) {
            Struct topicAndEpochs = (Struct) topicAndEpochsObj;
            String topic = topicAndEpochs.getString(TOPIC);
            for (Object partitionAndEpochObj : topicAndEpochs.getArray(PARTITIONS)) {
                Struct partitionAndEpoch = (Struct) partitionAndEpochObj;
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
        Struct requestStruct = new Struct(ApiKeys.OFFSET_FOR_LEADER_EPOCH.requestSchema(version()));

        Map<String, Map<Integer, Integer>> topicsToPartitionEpochs = CollectionUtils.groupDataByTopic(epochsByPartition);

        List<Struct> topics = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Integer>> topicToEpochs : topicsToPartitionEpochs.entrySet()) {
            Struct topicsStruct = requestStruct.instance(TOPICS);
            topicsStruct.set(TOPIC, topicToEpochs.getKey());
            List<Struct> partitions = new ArrayList<>();
            for (Map.Entry<Integer, Integer> partitionEpoch : topicToEpochs.getValue().entrySet()) {
                Struct partitionStruct = topicsStruct.instance(PARTITIONS);
                partitionStruct.set(PARTITION_ID, partitionEpoch.getKey());
                partitionStruct.set(LEADER_EPOCH, partitionEpoch.getValue());
                partitions.add(partitionStruct);
            }
            topicsStruct.set(PARTITIONS, partitions.toArray());
            topics.add(topicsStruct);
        }
        requestStruct.set(TOPICS, topics.toArray());
        return requestStruct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        Map<TopicPartition, EpochEndOffset> errorResponse = new HashMap();
        for (TopicPartition tp : epochsByPartition.keySet()) {
            errorResponse.put(tp, new EpochEndOffset(error, EpochEndOffset.UNDEFINED_EPOCH_OFFSET));
        }
        return new OffsetsForLeaderEpochResponse(errorResponse);
    }
}
