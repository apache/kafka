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
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.CURRENT_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

public class OffsetsForLeaderEpochRequest extends AbstractRequest {
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final Field.Int32 LEADER_EPOCH = new Field.Int32("leader_epoch",
            "The epoch to lookup an offset for.");

    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_PARTITION_V0 = new Schema(
            PARTITION_ID,
            LEADER_EPOCH);
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_FOR_LEADER_EPOCH_REQUEST_PARTITION_V0)));
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_FOR_LEADER_EPOCH_REQUEST_TOPIC_V0), "An array of topics to get epochs for"));

    // V1 request is the same as v0. Per-partition leader epoch has been added to response
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_V1 = OFFSET_FOR_LEADER_EPOCH_REQUEST_V0;

    // V2 adds the current leader epoch to support fencing
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_PARTITION_V2 = new Schema(
            PARTITION_ID,
            CURRENT_LEADER_EPOCH,
            LEADER_EPOCH);
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_TOPIC_V2 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(OFFSET_FOR_LEADER_EPOCH_REQUEST_PARTITION_V2)));
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_V2 = new Schema(
            new Field(TOPICS_KEY_NAME, new ArrayOf(OFFSET_FOR_LEADER_EPOCH_REQUEST_TOPIC_V2), "An array of topics to get epochs for"));

    public static Schema[] schemaVersions() {
        return new Schema[]{OFFSET_FOR_LEADER_EPOCH_REQUEST_V0, OFFSET_FOR_LEADER_EPOCH_REQUEST_V1,
            OFFSET_FOR_LEADER_EPOCH_REQUEST_V2};
    }

    private Map<TopicPartition, PartitionData> epochsByPartition;

    public Map<TopicPartition, PartitionData> epochsByTopicPartition() {
        return epochsByPartition;
    }

    public static class Builder extends AbstractRequest.Builder<OffsetsForLeaderEpochRequest> {
        private Map<TopicPartition, PartitionData> epochsByPartition;

        public Builder(short version) {
            this(version, new HashMap<>());
        }

        public Builder(short version, Map<TopicPartition, PartitionData> epochsByPartition) {
            super(ApiKeys.OFFSET_FOR_LEADER_EPOCH, version);
            this.epochsByPartition = epochsByPartition;
        }

        public Builder add(TopicPartition topicPartition, Integer currentEpoch, Integer searchEpoch) {
            epochsByPartition.put(topicPartition, new PartitionData(currentEpoch, searchEpoch));
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

    public OffsetsForLeaderEpochRequest(Map<TopicPartition, PartitionData> epochsByPartition, short version) {
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
                int searchEpoch = partitionAndEpoch.get(LEADER_EPOCH);
                int currentEpoch = partitionAndEpoch.getOrElse(CURRENT_LEADER_EPOCH,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH);

                TopicPartition tp = new TopicPartition(topic, partitionId);
                epochsByPartition.put(tp, new PartitionData(currentEpoch, searchEpoch));
            }
        }
    }

    public static OffsetsForLeaderEpochRequest parse(ByteBuffer buffer, short versionId) {
        return new OffsetsForLeaderEpochRequest(ApiKeys.OFFSET_FOR_LEADER_EPOCH.parseRequest(versionId, buffer), versionId);
    }

    @Override
    protected Struct toStruct() {
        Struct requestStruct = new Struct(ApiKeys.OFFSET_FOR_LEADER_EPOCH.requestSchema(version()));

        Map<String, Map<Integer, PartitionData>> topicsToPartitionEpochs = CollectionUtils.groupPartitionsByTopic(epochsByPartition);

        List<Struct> topics = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicToEpochs : topicsToPartitionEpochs.entrySet()) {
            Struct topicsStruct = requestStruct.instance(TOPICS_KEY_NAME);
            topicsStruct.set(TOPIC_NAME, topicToEpochs.getKey());
            List<Struct> partitions = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEpoch : topicToEpochs.getValue().entrySet()) {
                Struct partitionStruct = topicsStruct.instance(PARTITIONS_KEY_NAME);
                partitionStruct.set(PARTITION_ID, partitionEpoch.getKey());

                PartitionData partitionData = partitionEpoch.getValue();
                partitionStruct.set(LEADER_EPOCH, partitionData.searchLeaderEpoch);

                // Current leader epoch introduced in v2
                partitionStruct.setIfExists(CURRENT_LEADER_EPOCH, partitionData.currentLeaderEpoch);
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
            errorResponse.put(tp, new EpochEndOffset(
                error, EpochEndOffset.UNDEFINED_EPOCH, EpochEndOffset.UNDEFINED_EPOCH_OFFSET));
        }
        return new OffsetsForLeaderEpochResponse(errorResponse);
    }

    public static class PartitionData {
        public final int currentLeaderEpoch;
        public final int searchLeaderEpoch;

        public PartitionData(int currentLeaderEpoch, int searchLeaderEpoch) {
            this.currentLeaderEpoch = currentLeaderEpoch;
            this.searchLeaderEpoch = searchLeaderEpoch;
        }
    }
}
