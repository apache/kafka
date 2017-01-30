/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListOffsetResponse extends AbstractResponse {
    public static final long UNKNOWN_TIMESTAMP = -1L;
    public static final long UNKNOWN_OFFSET = -1L;
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.LIST_OFFSETS.id);
    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partition_responses";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String ERROR_CODE_KEY_NAME = "error_code";

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

    private final Map<TopicPartition, PartitionData> responseData;

    public static final class PartitionData {
        public final short errorCode;
        // The offsets list is only used in ListOffsetResponse v0.
        @Deprecated
        public final List<Long> offsets;
        public final Long timestamp;
        public final Long offset;

        /**
         * Constructor for ListOffsetResponse v0
         */
        @Deprecated
        public PartitionData(short errorCode, List<Long> offsets) {
            this.errorCode = errorCode;
            this.offsets = offsets;
            this.timestamp = null;
            this.offset = null;
        }

        /**
         * Constructor for ListOffsetResponse v1
         */
        public PartitionData(short errorCode, long timestamp, long offset) {
            this.errorCode = errorCode;
            this.timestamp = timestamp;
            this.offset = offset;
            this.offsets = null;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("PartitionData{").
                append("errorCode: ").append((int) errorCode).
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

    /**
     * Constructor for ListOffsetResponse v0.
     */
    @Deprecated
    public ListOffsetResponse(Map<TopicPartition, PartitionData> responseData) {
        this(responseData, 0);
    }

    public ListOffsetResponse(Map<TopicPartition, PartitionData> responseData, int version) {
        super(new Struct(ProtoUtils.responseSchema(ApiKeys.LIST_OFFSETS.id, version)));
        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupDataByTopic(responseData);

        List<Struct> topicArray = new ArrayList<Struct>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<Struct>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.getValue().entrySet()) {
                PartitionData offsetPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionData.set(ERROR_CODE_KEY_NAME, offsetPartitionData.errorCode);
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
        this.responseData = responseData;
    }

    public ListOffsetResponse(Struct struct) {
        super(struct);
        responseData = new HashMap<TopicPartition, PartitionData>();
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                short errorCode = partitionResponse.getShort(ERROR_CODE_KEY_NAME);
                PartitionData partitionData;
                if (partitionResponse.hasField(OFFSETS_KEY_NAME)) {
                    Object[] offsets = partitionResponse.getArray(OFFSETS_KEY_NAME);
                    List<Long> offsetsList = new ArrayList<Long>();
                    for (Object offset : offsets)
                        offsetsList.add((Long) offset);
                    partitionData = new PartitionData(errorCode, offsetsList);
                } else {
                    long timestamp = partitionResponse.getLong(TIMESTAMP_KEY_NAME);
                    long offset = partitionResponse.getLong(OFFSET_KEY_NAME);
                    partitionData = new PartitionData(errorCode, timestamp, offset);
                }
                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
    }

    public Map<TopicPartition, PartitionData> responseData() {
        return responseData;
    }

    public static ListOffsetResponse parse(ByteBuffer buffer) {
        return new ListOffsetResponse(CURRENT_SCHEMA.read(buffer));
    }

    public static ListOffsetResponse parse(ByteBuffer buffer, int version) {
        return new ListOffsetResponse(ProtoUtils.responseSchema(ApiKeys.LIST_OFFSETS.id, version).read(buffer));
    }
}
