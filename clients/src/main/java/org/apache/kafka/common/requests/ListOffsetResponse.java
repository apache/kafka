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
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT64;

public class ListOffsetResponse extends AbstractResponse {
    public static final long UNKNOWN_TIMESTAMP = -1L;
    public static final long UNKNOWN_OFFSET = -1L;

    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String PARTITIONS_KEY_NAME = "partition_responses";

    /**
     * Possible error code:
     *
     *  UNKNOWN_TOPIC_OR_PARTITION (3)
     *  NOT_LEADER_FOR_PARTITION (6)
     *  UNSUPPORTED_FOR_MESSAGE_FORMAT (43)
     *  UNKNOWN (-1)
     */

    // This key is only used by ListOffsetResponse v0
    @Deprecated
    private static final String OFFSETS_KEY_NAME = "offsets";
    private static final String TIMESTAMP_KEY_NAME = "timestamp";
    private static final String OFFSET_KEY_NAME = "offset";

    private static final Schema LIST_OFFSET_RESPONSE_PARTITION_V0 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            new Field(OFFSETS_KEY_NAME, new ArrayOf(INT64), "A list of offsets."));

    private static final Schema LIST_OFFSET_RESPONSE_PARTITION_V1 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            new Field(TIMESTAMP_KEY_NAME, INT64, "The timestamp associated with the returned offset"),
            new Field(OFFSET_KEY_NAME, INT64, "offset found"));

    private static final Schema LIST_OFFSET_RESPONSE_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(LIST_OFFSET_RESPONSE_PARTITION_V0)));

    private static final Schema LIST_OFFSET_RESPONSE_TOPIC_V1 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(LIST_OFFSET_RESPONSE_PARTITION_V1)));

    private static final Schema LIST_OFFSET_RESPONSE_V0 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(LIST_OFFSET_RESPONSE_TOPIC_V0)));

    private static final Schema LIST_OFFSET_RESPONSE_V1 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(LIST_OFFSET_RESPONSE_TOPIC_V1)));
    private static final Schema LIST_OFFSET_RESPONSE_V2 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(LIST_OFFSET_RESPONSE_TOPIC_V1)));

    public static Schema[] schemaVersions() {
        return new Schema[] {LIST_OFFSET_RESPONSE_V0, LIST_OFFSET_RESPONSE_V1, LIST_OFFSET_RESPONSE_V2};
    }

    public static final class PartitionData {
        public final Errors error;
        // The offsets list is only used in ListOffsetResponse v0.
        @Deprecated
        public final List<Long> offsets;
        public final Long timestamp;
        public final Long offset;

        /**
         * Constructor for ListOffsetResponse v0
         */
        @Deprecated
        public PartitionData(Errors error, List<Long> offsets) {
            this.error = error;
            this.offsets = offsets;
            this.timestamp = null;
            this.offset = null;
        }

        /**
         * Constructor for ListOffsetResponse v1
         */
        public PartitionData(Errors error, long timestamp, long offset) {
            this.error = error;
            this.timestamp = timestamp;
            this.offset = offset;
            this.offsets = null;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("PartitionData{").
                append("errorCode: ").append((int) error.code()).
                append(", timestamp: ").append(timestamp).
                append(", offset: ").append(offset).
                append(", offsets: ");
            if (offsets == null) {
                bld.append(offsets);
            } else {
                bld.append("[").append(Utils.join(this.offsets, ",")).append("]");
            }
            bld.append("}");
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
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.get(PARTITION_ID);
                Errors error = Errors.forCode(partitionResponse.get(ERROR_CODE));
                PartitionData partitionData;
                if (partitionResponse.hasField(OFFSETS_KEY_NAME)) {
                    Object[] offsets = partitionResponse.getArray(OFFSETS_KEY_NAME);
                    List<Long> offsetsList = new ArrayList<>();
                    for (Object offset : offsets)
                        offsetsList.add((Long) offset);
                    partitionData = new PartitionData(error, offsetsList);
                } else {
                    long timestamp = partitionResponse.getLong(TIMESTAMP_KEY_NAME);
                    long offset = partitionResponse.getLong(OFFSET_KEY_NAME);
                    partitionData = new PartitionData(error, timestamp, offset);
                }
                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
    }

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
        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupDataByTopic(responseData);

        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.getValue().entrySet()) {
                PartitionData offsetPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(ERROR_CODE, offsetPartitionData.error.code());
                if (version == 0)
                    partitionData.set(OFFSETS_KEY_NAME, offsetPartitionData.offsets.toArray());
                else {
                    partitionData.set(TIMESTAMP_KEY_NAME, offsetPartitionData.timestamp);
                    partitionData.set(OFFSET_KEY_NAME, offsetPartitionData.offset);
                }
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicArray.toArray());

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
}
