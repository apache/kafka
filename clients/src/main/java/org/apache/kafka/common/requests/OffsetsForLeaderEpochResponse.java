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
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderPartitionResult;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Possible error codes:
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED} If the user does not have DESCRIBE access to a requested topic
 * - {@link Errors#REPLICA_NOT_AVAILABLE} If the request is received by a broker with version < 2.6 which is not a replica
 * - {@link Errors#NOT_LEADER_OR_FOLLOWER} If the broker is not a leader or follower and either the provided leader epoch
 *     matches the known leader epoch on the broker or is empty
 * - {@link Errors#FENCED_LEADER_EPOCH} If the epoch is lower than the broker's epoch
 * - {@link Errors#UNKNOWN_LEADER_EPOCH} If the epoch is larger than the broker's epoch
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} If the broker does not have metadata for a topic or partition
 * - {@link Errors#KAFKA_STORAGE_ERROR} If the log directory for one of the requested partitions is offline
 * - {@link Errors#UNKNOWN_SERVER_ERROR} For any unexpected errors
 */
public class OffsetsForLeaderEpochResponse extends AbstractResponse {

    private final OffsetForLeaderEpochResponseData data;

    public OffsetsForLeaderEpochResponse(OffsetForLeaderEpochResponseData data) {
        this.data = data;
    }

    public OffsetsForLeaderEpochResponse(Struct struct, short version) {
        data = new OffsetForLeaderEpochResponseData(struct, version);
    }

    public OffsetsForLeaderEpochResponse(Map<TopicPartition, EpochEndOffset> offsets) {
        this(0, offsets);
    }

    public OffsetsForLeaderEpochResponse(int throttleTimeMs, Map<TopicPartition, EpochEndOffset> offsets) {
        data = new OffsetForLeaderEpochResponseData();
        data.setThrottleTimeMs(throttleTimeMs);

        offsets.forEach((tp, offset) -> {
            OffsetForLeaderTopicResult topic = data.topics().find(tp.topic());
            if (topic == null) {
                topic = new OffsetForLeaderTopicResult().setTopic(tp.topic());
                data.topics().add(topic);
            }
            topic.partitions().add(new OffsetForLeaderPartitionResult()
                .setPartition(tp.partition())
                .setErrorCode(offset.error().code())
                .setLeaderEpoch(offset.leaderEpoch())
                .setEndOffset(offset.endOffset()));
        });
    }

    public OffsetForLeaderEpochResponseData data() {
        return data;
    }

    public Map<TopicPartition, EpochEndOffset> responses() {
        Map<TopicPartition, EpochEndOffset> epochEndOffsetsByPartition = new HashMap<>();

        data.topics().forEach(topic ->
            topic.partitions().forEach(partition ->
                epochEndOffsetsByPartition.put(
                    new TopicPartition(topic.topic(), partition.partition()),
                    new EpochEndOffset(
                        Errors.forCode(partition.errorCode()),
                        partition.leaderEpoch(),
                        partition.endOffset()))));

        return epochEndOffsetsByPartition;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.topics().forEach(topic ->
            topic.partitions().forEach(partition ->
                updateErrorCounts(errorCounts, Errors.forCode(partition.errorCode()))));
        return errorCounts;
    }

    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public static OffsetsForLeaderEpochResponse parse(ByteBuffer buffer, short version) {
        return new OffsetsForLeaderEpochResponse(ApiKeys.OFFSET_FOR_LEADER_EPOCH.responseSchema(version).read(buffer), version);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
