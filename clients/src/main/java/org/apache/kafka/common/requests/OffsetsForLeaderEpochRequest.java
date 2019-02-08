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
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.protocol.CommonFields.CURRENT_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

public class OffsetsForLeaderEpochRequest extends AbstractRequest {
    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("topics",
            "An array of topics to get epochs for");
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partitions",
            "An array of partitions to get epochs for");

    private static final Field.Int32 LEADER_EPOCH = new Field.Int32("leader_epoch",
            "The epoch to lookup an offset for.");

    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            PARTITION_ID,
            LEADER_EPOCH);
    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V0);
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_V0 = new Schema(
            TOPICS_V0);

    // V1 request is the same as v0. Per-partition leader epoch has been added to response
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_V1 = OFFSET_FOR_LEADER_EPOCH_REQUEST_V0;

    // V2 adds the current leader epoch to support fencing and the addition of the throttle time in the response
    private static final Field PARTITIONS_V2 = PARTITIONS.withFields(
            PARTITION_ID,
            CURRENT_LEADER_EPOCH,
            LEADER_EPOCH);
    private static final Field TOPICS_V2 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V2);
    private static final Schema OFFSET_FOR_LEADER_EPOCH_REQUEST_V2 = new Schema(
            TOPICS_V2);

    public static Schema[] schemaVersions() {
        return new Schema[]{OFFSET_FOR_LEADER_EPOCH_REQUEST_V0, OFFSET_FOR_LEADER_EPOCH_REQUEST_V1,
            OFFSET_FOR_LEADER_EPOCH_REQUEST_V2};
    }

    private Map<TopicPartition, PartitionData> epochsByPartition;

    public Map<TopicPartition, PartitionData> epochsByTopicPartition() {
        return epochsByPartition;
    }

    public static class Builder extends AbstractRequest.Builder<OffsetsForLeaderEpochRequest> {
        private final Map<TopicPartition, PartitionData> epochsByPartition;

        public Builder(short version, Map<TopicPartition, PartitionData> epochsByPartition) {
            super(ApiKeys.OFFSET_FOR_LEADER_EPOCH, version);
            this.epochsByPartition = epochsByPartition;
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
            bld.append("OffsetsForLeaderEpochRequest(").
                    append("epochsByPartition=").append(epochsByPartition).
                    append(")");
            return bld.toString();
        }
    }

    public OffsetsForLeaderEpochRequest(Map<TopicPartition, PartitionData> epochsByPartition, short version) {
        super(ApiKeys.OFFSET_FOR_LEADER_EPOCH, version);
        this.epochsByPartition = epochsByPartition;
    }

    public OffsetsForLeaderEpochRequest(Struct struct, short version) {
        super(ApiKeys.OFFSET_FOR_LEADER_EPOCH, version);
        epochsByPartition = new HashMap<>();
        for (Object topicAndEpochsObj : struct.get(TOPICS)) {
            Struct topicAndEpochs = (Struct) topicAndEpochsObj;
            String topic = topicAndEpochs.get(TOPIC_NAME);
            for (Object partitionAndEpochObj : topicAndEpochs.get(PARTITIONS)) {
                Struct partitionAndEpoch = (Struct) partitionAndEpochObj;
                int partitionId = partitionAndEpoch.get(PARTITION_ID);
                int leaderEpoch = partitionAndEpoch.get(LEADER_EPOCH);
                Optional<Integer> currentEpoch = RequestUtils.getLeaderEpoch(partitionAndEpoch, CURRENT_LEADER_EPOCH);
                TopicPartition tp = new TopicPartition(topic, partitionId);
                epochsByPartition.put(tp, new PartitionData(currentEpoch, leaderEpoch));
            }
        }
    }

    public static OffsetsForLeaderEpochRequest parse(ByteBuffer buffer, short versionId) {
        return new OffsetsForLeaderEpochRequest(ApiKeys.OFFSET_FOR_LEADER_EPOCH.parseRequest(versionId, buffer), versionId);
    }

    @Override
    protected Struct toStruct() {
        Struct requestStruct = new Struct(ApiKeys.OFFSET_FOR_LEADER_EPOCH.requestSchema(version()));

        Map<String, Map<Integer, PartitionData>> topicsToPartitionEpochs = CollectionUtils.groupPartitionDataByTopic(epochsByPartition);

        List<Struct> topics = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicToEpochs : topicsToPartitionEpochs.entrySet()) {
            Struct topicsStruct = requestStruct.instance(TOPICS);
            topicsStruct.set(TOPIC_NAME, topicToEpochs.getKey());
            List<Struct> partitions = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEpoch : topicToEpochs.getValue().entrySet()) {
                Struct partitionStruct = topicsStruct.instance(PARTITIONS);
                partitionStruct.set(PARTITION_ID, partitionEpoch.getKey());

                PartitionData partitionData = partitionEpoch.getValue();
                partitionStruct.set(LEADER_EPOCH, partitionData.leaderEpoch);

                // Current leader epoch introduced in v2
                RequestUtils.setLeaderEpochIfExists(partitionStruct, CURRENT_LEADER_EPOCH, partitionData.currentLeaderEpoch);
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
        Map<TopicPartition, EpochEndOffset> errorResponse = new HashMap<>();
        for (TopicPartition tp : epochsByPartition.keySet()) {
            errorResponse.put(tp, new EpochEndOffset(
                error, EpochEndOffset.UNDEFINED_EPOCH, EpochEndOffset.UNDEFINED_EPOCH_OFFSET));
        }
        return new OffsetsForLeaderEpochResponse(throttleTimeMs, errorResponse);
    }

    public static class PartitionData {
        public final Optional<Integer> currentLeaderEpoch;
        public final int leaderEpoch;

        public PartitionData(Optional<Integer> currentLeaderEpoch, int leaderEpoch) {
            this.currentLeaderEpoch = currentLeaderEpoch;
            this.leaderEpoch = leaderEpoch;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(currentLeaderEpoch=").append(currentLeaderEpoch).
                append(", leaderEpoch=").append(leaderEpoch).
                append(")");
            return bld.toString();
        }
    }
}
