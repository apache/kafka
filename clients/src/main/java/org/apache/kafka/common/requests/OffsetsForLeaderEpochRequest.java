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

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;

public class OffsetsForLeaderEpochRequest extends AbstractRequest {
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String PARTITIONS_KEY_NAME = "partitions";
    private static final String LEADER_EPOCH = "leader_epoch";

    /* Offsets for Leader Epoch api */
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_PARTITION_V0 = new Schema(
            PARTITION_ID,
            new Field(LEADER_EPOCH, INT32, "The epoch"));
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_FOR_LEADER_EPOCH_REQUEST_PARTITION_V0)));
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_FOR_LEADER_EPOCH_REQUEST_TOPIC_V0), "An array of topics to get epochs for"));

    public static Schema[] schemaVersions() {
        return new Schema[]{OFFSET_FOR_LEADER_EPOCH_REQUEST_V0};
    }

    private Map<TopicPartition, Integer> epochsByPartition;

    public Map<TopicPartition, Integer> epochsByTopicPartition() {
        return epochsByPartition;
    }

    public static class Builder extends AbstractRequest.Builder<OffsetsForLeaderEpochRequest> {
        private Map<TopicPartition, Integer> epochsByPartition = new HashMap<>();

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
        for (Object topicAndEpochsObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicAndEpochs = (Struct) topicAndEpochsObj;
            String topic = topicAndEpochs.get(TOPIC_NAME);
            for (Object partitionAndEpochObj : topicAndEpochs.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionAndEpoch = (Struct) partitionAndEpochObj;
                int partitionId = partitionAndEpoch.get(PARTITION_ID);
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
            Struct topicsStruct = requestStruct.instance(TOPICS_KEY_NAME);
            topicsStruct.set(TOPIC_NAME, topicToEpochs.getKey());
            List<Struct> partitions = new ArrayList<>();
            for (Map.Entry<Integer, Integer> partitionEpoch : topicToEpochs.getValue().entrySet()) {
                Struct partitionStruct = topicsStruct.instance(PARTITIONS_KEY_NAME);
                partitionStruct.set(PARTITION_ID, partitionEpoch.getKey());
                partitionStruct.set(LEADER_EPOCH, partitionEpoch.getValue());
                partitions.add(partitionStruct);
            }
            topicsStruct.set(PARTITIONS_KEY_NAME, partitions.toArray());
            topics.add(topicsStruct);
        }
        requestStruct.set(TOPICS_KEY_NAME, topics.toArray());
        return requestStruct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        Map<TopicPartition, EpochEndOffset> errorResponse = new HashMap<>();
        for (TopicPartition tp : epochsByPartition.keySet()) {
            errorResponse.put(tp, new EpochEndOffset(error, EpochEndOffset.UNDEFINED_EPOCH_OFFSET));
        }
        return new OffsetsForLeaderEpochResponse(errorResponse);
    }
}
