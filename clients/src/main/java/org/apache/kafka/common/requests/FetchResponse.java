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
import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.MultiSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.Records;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This wrapper supports all versions of the Fetch API
 */
public class FetchResponse extends AbstractResponse {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.FETCH.id);
    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partition_responses";
    private static final String THROTTLE_TIME_KEY_NAME = "throttle_time_ms";

    // partition level field names
    private static final String PARTITION_HEADER_KEY_NAME = "partition_header";
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    // Default throttle time
    private static final int DEFAULT_THROTTLE_TIME = 0;

    /**
     * Possible error codes:
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

    private final LinkedHashMap<TopicPartition, PartitionData> responseData;
    private final int throttleTime;

    public static final class PartitionData {
        public final short errorCode;
        public final long highWatermark;
        public final Records records;

        public PartitionData(short errorCode, long highWatermark, Records records) {
            this.errorCode = errorCode;
            this.highWatermark = highWatermark;
            this.records = records;
        }

        @Override
        public String toString() {
            return "(errorCode=" + errorCode + ", highWaterMark=" + highWatermark +
                    ", records=" + records + ")";
        }
    }

    /**
     * Constructor for version 3.
     *
     * The entries in `responseData` should be in the same order as the entries in `FetchRequest.fetchData`.
     *
     * @param responseData fetched data grouped by topic-partition
     * @param throttleTime Time in milliseconds the response was throttled
     */
    public FetchResponse(LinkedHashMap<TopicPartition, PartitionData> responseData, int throttleTime) {
        this(3, responseData, throttleTime);
    }

    /**
     * Constructor for all versions.
     *
     * From version 3, the entries in `responseData` should be in the same order as the entries in
     * `FetchRequest.fetchData`.
     *
     * @param responseData fetched data grouped by topic-partition
     * @param throttleTime Time in milliseconds the response was throttled
     */
    public FetchResponse(int version, LinkedHashMap<TopicPartition, PartitionData> responseData, int throttleTime) {
        super(writeStruct(new Struct(ProtoUtils.responseSchema(ApiKeys.FETCH.id, version)), version, responseData,
                throttleTime));
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
                Struct partitionResponseHeader = partitionResponse.getStruct(PARTITION_HEADER_KEY_NAME);
                int partition = partitionResponseHeader.getInt(PARTITION_KEY_NAME);
                short errorCode = partitionResponseHeader.getShort(ERROR_CODE_KEY_NAME);
                long highWatermark = partitionResponseHeader.getLong(HIGH_WATERMARK_KEY_NAME);
                Records records = partitionResponse.getRecords(RECORD_SET_KEY_NAME);
                PartitionData partitionData = new PartitionData(errorCode, highWatermark, records);
                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
        this.responseData = responseData;
        this.throttleTime = struct.hasField(THROTTLE_TIME_KEY_NAME) ? struct.getInt(THROTTLE_TIME_KEY_NAME) : DEFAULT_THROTTLE_TIME;
    }

    @Override
    public Send toSend(String dest, RequestHeader requestHeader) {
        ResponseHeader responseHeader = new ResponseHeader(requestHeader.correlationId());

        // write the total size and the response header
        ByteBuffer buffer = ByteBuffer.allocate(responseHeader.sizeOf() + 4);
        buffer.putInt(responseHeader.sizeOf() + struct.sizeOf());
        responseHeader.writeTo(buffer);
        buffer.rewind();

        List<Send> sends = new ArrayList<>();
        sends.add(new ByteBufferSend(dest, buffer));
        addResponseData(dest, sends);
        return new MultiSend(dest, sends);
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

    private void addResponseData(String dest, List<Send> sends) {
        Object[] allTopicData = struct.getArray(RESPONSES_KEY_NAME);

        if (struct.hasField(THROTTLE_TIME_KEY_NAME)) {
            int throttleTime = struct.getInt(THROTTLE_TIME_KEY_NAME);
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(throttleTime);
            buffer.putInt(allTopicData.length);
            buffer.rewind();
            sends.add(new ByteBufferSend(dest, buffer));
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(allTopicData.length);
            buffer.rewind();
            sends.add(new ByteBufferSend(dest, buffer));
        }

        for (Object topicData : allTopicData)
            addTopicData(dest, sends, (Struct) topicData);
    }

    private void addTopicData(String dest, List<Send> sends, Struct topicData) {
        String topic = topicData.getString(TOPIC_KEY_NAME);
        Object[] allPartitionData = topicData.getArray(PARTITIONS_KEY_NAME);

        // include the topic header and the count for the number of partitions
        ByteBuffer buffer = ByteBuffer.allocate(Type.STRING.sizeOf(topic) + 4);
        Type.STRING.write(buffer, topic);
        buffer.putInt(allPartitionData.length);
        buffer.rewind();
        sends.add(new ByteBufferSend(dest, buffer));

        for (Object partitionData : allPartitionData)
            addPartitionData(dest, sends, (Struct) partitionData);
    }

    private void addPartitionData(String dest, List<Send> sends, Struct partitionData) {
        Struct header = partitionData.getStruct(PARTITION_HEADER_KEY_NAME);
        Records records = partitionData.getRecords(RECORD_SET_KEY_NAME);

        // include the partition header and the size of the record set
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + 4);
        header.writeTo(buffer);
        buffer.putInt(records.sizeInBytes());
        buffer.rewind();
        sends.add(new ByteBufferSend(dest, buffer));

        // finally the send for the record set itself
        sends.add(new RecordsSend(dest, records));
    }

    private static Struct writeStruct(Struct struct,
                                      int version,
                                      LinkedHashMap<TopicPartition, PartitionData> responseData,
                                      int throttleTime) {
        List<FetchRequest.TopicAndPartitionData<PartitionData>> topicsData = FetchRequest.TopicAndPartitionData.batchByTopic(responseData);
        List<Struct> topicArray = new ArrayList<>();
        for (FetchRequest.TopicAndPartitionData<PartitionData> topicEntry: topicsData) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.partitions.entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                Struct partitionDataHeader = partitionData.instance(PARTITION_HEADER_KEY_NAME);
                partitionDataHeader.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionDataHeader.set(ERROR_CODE_KEY_NAME, fetchPartitionData.errorCode);
                partitionDataHeader.set(HIGH_WATERMARK_KEY_NAME, fetchPartitionData.highWatermark);
                partitionData.set(PARTITION_HEADER_KEY_NAME, partitionDataHeader);
                partitionData.set(RECORD_SET_KEY_NAME, fetchPartitionData.records);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicArray.toArray());

        if (version >= 1)
            struct.set(THROTTLE_TIME_KEY_NAME, throttleTime);

        return struct;
    }

    public static int sizeOf(int version, LinkedHashMap<TopicPartition, PartitionData> responseData) {
        Struct struct = new Struct(ProtoUtils.responseSchema(ApiKeys.FETCH.id, version));
        writeStruct(struct, version, responseData, 0);
        return 4 + struct.sizeOf();
    }

}
