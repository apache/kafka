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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This wrapper supports both v0 and v1 of FetchResponse.
 */
public class FetchResponse extends AbstractRequestResponse {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.FETCH.id);
    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partition_responses";
    private static final String THROTTLE_TIME_KEY_NAME = "throttle_time_ms";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    // Default throttle time
    private static final int DEFAULT_THROTTLE_TIME = 0;

  /**
     * Possible error code:
     *
     *  OFFSET_OUT_OF_RANGE (1)
     *  UNKNOWN_TOPIC_OR_PARTITION (3)
     *  NOT_LEADER_FOR_PARTITION (6)
     *  REPLICA_NOT_AVAILABLE (9)
     *  UNKNOWN (-1)
     */

    private static final String HIGH_WATERMARK_KEY_NAME = "high_watermark";
    private static final String RECORD_SET_KEY_NAME = "record_set";

    public static final long INVALID_HIGHWATERMARK = -1L;
    public static final ByteBuffer EMPTY_RECORD_SET = ByteBuffer.allocate(0);

    private final LinkedHashMap<TopicPartition, PartitionData> responseData;
    private final int throttleTime;

    public static final class PartitionData {
        public final short errorCode;
        public final long highWatermark;
        public final ByteBuffer recordSet;

        public PartitionData(short errorCode, long highWatermark, ByteBuffer recordSet) {
            this.errorCode = errorCode;
            this.highWatermark = highWatermark;
            this.recordSet = recordSet;
        }
    }

    /**
     * Constructor for Version 0
     * @param responseData fetched data grouped by topic-partition
     */
    public FetchResponse(Map<TopicPartition, PartitionData> responseData) {
        this(0, new LinkedHashMap<>(responseData), DEFAULT_THROTTLE_TIME);
    }

    /**
     * Constructor for Version 1 and 2
     * @param responseData fetched data grouped by topic-partition
     * @param throttleTime Time in milliseconds the response was throttled
     */
    public FetchResponse(Map<TopicPartition, PartitionData> responseData, int throttleTime) {
        // the schema for versions 1 and 2 is the same, so we pick 2 here
        this(2, new LinkedHashMap<>(responseData), throttleTime);
    }

    /**
     * Constructor for Version 3
     * @param responseData fetched data grouped by topic-partition
     * @param throttleTime Time in milliseconds the response was throttled
     */
    public FetchResponse(LinkedHashMap<TopicPartition, PartitionData> responseData, int throttleTime) {
        this(3, responseData, throttleTime);
    }

    private FetchResponse(int version, LinkedHashMap<TopicPartition, PartitionData> responseData, int throttleTime) {
        super(new Struct(ProtoUtils.responseSchema(ApiKeys.FETCH.id, version)));

        List<FetchRequest.TopicAndPartitionData<PartitionData>> topicsData = FetchRequest.TopicAndPartitionData.batchByTopic(responseData);
        List<Struct> topicArray = new ArrayList<>();
        for (FetchRequest.TopicAndPartitionData<PartitionData> topicEntry: topicsData) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.partitions.entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionData.set(ERROR_CODE_KEY_NAME, fetchPartitionData.errorCode);
                partitionData.set(HIGH_WATERMARK_KEY_NAME, fetchPartitionData.highWatermark);
                partitionData.set(RECORD_SET_KEY_NAME, fetchPartitionData.recordSet);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicArray.toArray());

        if (version >= 1)
            struct.set(THROTTLE_TIME_KEY_NAME, throttleTime);

        this.responseData = responseData;
        this.throttleTime = throttleTime;
    }

    public FetchResponse(Struct struct) {
        super(struct);
        LinkedHashMap<TopicPartition, PartitionData> responseData = new LinkedHashMap<>();
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                short errorCode = partitionResponse.getShort(ERROR_CODE_KEY_NAME);
                long highWatermark = partitionResponse.getLong(HIGH_WATERMARK_KEY_NAME);
                ByteBuffer recordSet = partitionResponse.getBytes(RECORD_SET_KEY_NAME);
                PartitionData partitionData = new PartitionData(errorCode, highWatermark, recordSet);
                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
        this.responseData = responseData;
        this.throttleTime = struct.hasField(THROTTLE_TIME_KEY_NAME) ? struct.getInt(THROTTLE_TIME_KEY_NAME) : DEFAULT_THROTTLE_TIME;
    }

    public LinkedHashMap<TopicPartition, PartitionData> responseData() {
        return responseData;
    }

    public int getThrottleTime() {
        return this.throttleTime;
    }

    public static FetchResponse parse(ByteBuffer buffer) {
        return new FetchResponse(CURRENT_SCHEMA.read(buffer));
    }

    public static FetchResponse parse(ByteBuffer buffer, int version) {
        return new FetchResponse(ProtoUtils.responseSchema(ApiKeys.FETCH.id, version).read(buffer));
    }
}
