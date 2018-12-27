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
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.LEADER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT64;

/**
 * Possible error codes:
 *
 * - {@link Errors#UNSUPPORTED_FOR_MESSAGE_FORMAT} If the message format does not support lookup by timestamp
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED} If the user does not have DESCRIBE access to a requested topic
 * - {@link Errors#REPLICA_NOT_AVAILABLE} If the request is received by a broker which is not a replica
 * - {@link Errors#NOT_LEADER_FOR_PARTITION} If the broker is not a leader and either the provided leader epoch
 *     matches the known leader epoch on the broker or is empty
 * - {@link Errors#FENCED_LEADER_EPOCH} If the epoch is lower than the broker's epoch
 * - {@link Errors#UNKNOWN_LEADER_EPOCH} If the epoch is larger than the broker's epoch
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} If the broker does not have metadata for a topic or partition
 * - {@link Errors#KAFKA_STORAGE_ERROR} If the log directory for one of the requested partitions is offline
 * - {@link Errors#UNKNOWN_SERVER_ERROR} For any unexpected errors
 * - {@link Errors#LEADER_NOT_AVAILABLE} The leader's HW has not caught up after recent election (v4 protocol)
 * - {@link Errors#OFFSET_NOT_AVAILABLE} The leader's HW has not caught up after recent election (v5+ protocol)
 */
public class ListOffsetResponse extends AbstractResponse {
    public static final long UNKNOWN_TIMESTAMP = -1L;
    public static final long UNKNOWN_OFFSET = -1L;

    // top level fields
    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("responses",
            "The listed offsets by topic");

    // topic level fields
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partition_responses",
            "The listed offsets by partition");

    // partition level fields
    // This key is only used by ListOffsetResponse v0
    @Deprecated
    private static final Field.Array OFFSETS = new Field.Array("offsets'", INT64, "A list of offsets.");
    private static final Field.Int64 TIMESTAMP = new Field.Int64("timestamp",
            "The timestamp associated with the returned offset");
    private static final Field.Int64 OFFSET = new Field.Int64("offset",
            "The offset found");

    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            PARTITION_ID,
            ERROR_CODE,
            OFFSETS);

    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V0);

    private static final Schema LIST_OFFSET_RESPONSE_V0 = new Schema(
            TOPICS_V0);

    // V1 bumped for the removal of the offsets array
    private static final Field PARTITIONS_V1 = PARTITIONS.withFields(
            PARTITION_ID,
            ERROR_CODE,
            TIMESTAMP,
            OFFSET);

    private static final Field TOPICS_V1 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V1);

    private static final Schema LIST_OFFSET_RESPONSE_V1 = new Schema(
            TOPICS_V1);

    // V2 bumped for the addition of the throttle time
    private static final Schema LIST_OFFSET_RESPONSE_V2 = new Schema(
            THROTTLE_TIME_MS,
            TOPICS_V1);

    // V3 bumped to indicate that on quota violation brokers send out responses before throttling.
    private static final Schema LIST_OFFSET_RESPONSE_V3 = LIST_OFFSET_RESPONSE_V2;

    // V4 bumped for the addition of the current leader epoch in the request schema and the
    // leader epoch in the response partition data
    private static final Field PARTITIONS_V4 = PARTITIONS.withFields(
            PARTITION_ID,
            ERROR_CODE,
            TIMESTAMP,
            OFFSET,
            LEADER_EPOCH);

    private static final Field TOPICS_V4 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V4);

    private static final Schema LIST_OFFSET_RESPONSE_V4 = new Schema(
            THROTTLE_TIME_MS,
            TOPICS_V4);

    private static final Schema LIST_OFFSET_RESPONSE_V5 = LIST_OFFSET_RESPONSE_V4;

    public static Schema[] schemaVersions() {
        return new Schema[] {LIST_OFFSET_RESPONSE_V0, LIST_OFFSET_RESPONSE_V1, LIST_OFFSET_RESPONSE_V2,
            LIST_OFFSET_RESPONSE_V3, LIST_OFFSET_RESPONSE_V4, LIST_OFFSET_RESPONSE_V5};
    }

    public static final class PartitionData {
        public final Errors error;
        // The offsets list is only used in ListOffsetResponse v0.
        @Deprecated
        public final List<Long> offsets;
        public final Long timestamp;
        public final Long offset;
        public final Optional<Integer> leaderEpoch;

        /**
         * Constructor for ListOffsetResponse v0
         */
        @Deprecated
        public PartitionData(Errors error, List<Long> offsets) {
            this.error = error;
            this.offsets = offsets;
            this.timestamp = null;
            this.offset = null;
            this.leaderEpoch = Optional.empty();
        }

        /**
         * Constructor for ListOffsetResponse v1
         */
        public PartitionData(Errors error, long timestamp, long offset, Optional<Integer> leaderEpoch) {
            this.error = error;
            this.timestamp = timestamp;
            this.offset = offset;
            this.offsets = null;
            this.leaderEpoch = leaderEpoch;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("PartitionData(").
                    append("errorCode: ").append((int) error.code());

            if (offsets == null) {
                bld.append(", timestamp: ").append(timestamp).
                        append(", offset: ").append(offset).
                        append(", leaderEpoch: ").append(leaderEpoch);
            } else {
                bld.append(", offsets: ").
                        append("[").
                        append(Utils.join(this.offsets, ",")).
                        append("]");
            }
            bld.append(")");
            return bld.toString();
        }
    }

    private final int throttleTimeMs;
    private final Map<TopicPartition, PartitionData> responseData;

    /**
     * Constructor for all versions without throttle time
     */
    public ListOffsetResponse(Map<TopicPartition, PartitionData> responseData) {
        this(DEFAULT_THROTTLE_TIME, responseData);
    }

    public ListOffsetResponse(int throttleTimeMs, Map<TopicPartition, PartitionData> responseData) {
        this.throttleTimeMs = throttleTimeMs;
        this.responseData = responseData;
    }

    public ListOffsetResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        responseData = new HashMap<>();
        for (Object topicResponseObj : struct.get(TOPICS)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.get(PARTITIONS)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.get(PARTITION_ID);
                Errors error = Errors.forCode(partitionResponse.get(ERROR_CODE));
                PartitionData partitionData;
                if (partitionResponse.hasField(OFFSETS)) {
                    Object[] offsets = partitionResponse.get(OFFSETS);
                    List<Long> offsetsList = new ArrayList<>();
                    for (Object offset : offsets)
                        offsetsList.add((Long) offset);
                    partitionData = new PartitionData(error, offsetsList);
                } else {
                    long timestamp = partitionResponse.get(TIMESTAMP);
                    long offset = partitionResponse.get(OFFSET);
                    Optional<Integer> leaderEpoch = RequestUtils.getLeaderEpoch(partitionResponse, LEADER_EPOCH);
                    partitionData = new PartitionData(error, timestamp, offset, leaderEpoch);
                }
                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<TopicPartition, PartitionData> responseData() {
        return responseData;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (PartitionData response : responseData.values())
            updateErrorCounts(errorCounts, response.error);
        return errorCounts;
    }

    public static ListOffsetResponse parse(ByteBuffer buffer, short version) {
        return new ListOffsetResponse(ApiKeys.LIST_OFFSETS.parseResponse(version, buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.LIST_OFFSETS.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupPartitionDataByTopic(responseData);

        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS);
            topicData.set(TOPIC_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.getValue().entrySet()) {
                PartitionData offsetPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(ERROR_CODE, offsetPartitionData.error.code());
                if (version == 0) {
                    partitionData.set(OFFSETS, offsetPartitionData.offsets.toArray());
                } else {
                    partitionData.set(TIMESTAMP, offsetPartitionData.timestamp);
                    partitionData.set(OFFSET, offsetPartitionData.offset);
                    RequestUtils.setLeaderEpochIfExists(partitionData, LEADER_EPOCH, offsetPartitionData.leaderEpoch);
                }
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS, topicArray.toArray());

        return struct;
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("(type=ListOffsetResponse")
            .append(", throttleTimeMs=").append(throttleTimeMs)
            .append(", responseData=").append(responseData)
            .append(")");
        return bld.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 3;
    }
}
