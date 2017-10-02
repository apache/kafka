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

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT64;

/**
 * This wrapper supports both v0 and v1 of ProduceResponse.
 */
public class ProduceResponse extends AbstractResponse {

    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String PARTITION_RESPONSES_KEY_NAME = "partition_responses";

    public static final long INVALID_OFFSET = -1L;

    /**
     * Possible error code:
     *
     * CORRUPT_MESSAGE (2)
     * UNKNOWN_TOPIC_OR_PARTITION (3)
     * NOT_LEADER_FOR_PARTITION (6)
     * MESSAGE_TOO_LARGE (10)
     * INVALID_TOPIC (17)
     * RECORD_LIST_TOO_LARGE (18)
     * NOT_ENOUGH_REPLICAS (19)
     * NOT_ENOUGH_REPLICAS_AFTER_APPEND (20)
     * INVALID_REQUIRED_ACKS (21)
     * TOPIC_AUTHORIZATION_FAILED (29)
     * UNSUPPORTED_FOR_MESSAGE_FORMAT (43)
     * INVALID_PRODUCER_EPOCH (47)
     * CLUSTER_AUTHORIZATION_FAILED (31)
     * TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)
     */

    private static final String BASE_OFFSET_KEY_NAME = "base_offset";
    private static final String LOG_APPEND_TIME_KEY_NAME = "log_append_time";
    private static final String LOG_START_OFFSET_KEY_NAME = "log_start_offset";

    private static final Field.Int64 LOG_START_OFFSET_FIELD = new Field.Int64(LOG_START_OFFSET_KEY_NAME,
            "The start offset of the log at the time this produce response was created", INVALID_OFFSET);

    private static final Schema PRODUCE_RESPONSE_V0 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITION_RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                            PARTITION_ID,
                            ERROR_CODE,
                            new Field(BASE_OFFSET_KEY_NAME, INT64))))))));

    private static final Schema PRODUCE_RESPONSE_V1 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITION_RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                            PARTITION_ID,
                            ERROR_CODE,
                            new Field(BASE_OFFSET_KEY_NAME, INT64))))))),
            THROTTLE_TIME_MS);

    /**
     * PRODUCE_RESPONSE_V2 added a timestamp field in the per partition response status.
     * The timestamp is log append time if the topic is configured to use log append time. Or it is NoTimestamp when create
     * time is used for the topic.
     */
    private static final Schema PRODUCE_RESPONSE_V2 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITION_RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                            PARTITION_ID,
                            ERROR_CODE,
                            new Field(BASE_OFFSET_KEY_NAME, INT64),
                            new Field(LOG_APPEND_TIME_KEY_NAME, INT64, "The timestamp returned by broker after appending " +
                                    "the messages. If CreateTime is used for the topic, the timestamp will be -1. " +
                                    "If LogAppendTime is used for the topic, the timestamp will be " +
                                    "the broker local time when the messages are appended."))))))),
            THROTTLE_TIME_MS);

    private static final Schema PRODUCE_RESPONSE_V3 = PRODUCE_RESPONSE_V2;

    /**
     * The body of PRODUCE_RESPONSE_V4 is the same as PRODUCE_RESPONSE_V3.
     * The version number is bumped up to indicate that the client supports KafkaStorageException.
     * The KafkaStorageException will be translated to NotLeaderForPartitionException in the response if version <= 3
     */
    private static final Schema PRODUCE_RESPONSE_V4 = PRODUCE_RESPONSE_V3;


    /**
     * Add in the log_start_offset field to the partition response to filter out spurious OutOfOrderSequencExceptions
     * on the client.
     */
    public static final Schema PRODUCE_RESPONSE_V5 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITION_RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                            PARTITION_ID,
                            ERROR_CODE,
                            new Field(BASE_OFFSET_KEY_NAME, INT64),
                            new Field(LOG_APPEND_TIME_KEY_NAME, INT64, "The timestamp returned by broker after appending " +
                                    "the messages. If CreateTime is used for the topic, the timestamp will be -1. " +
                                    "If LogAppendTime is used for the topic, the timestamp will be the broker local " +
                                    "time when the messages are appended."),
                            LOG_START_OFFSET_FIELD)))))),
            THROTTLE_TIME_MS);


    public static Schema[] schemaVersions() {
        return new Schema[]{PRODUCE_RESPONSE_V0, PRODUCE_RESPONSE_V1, PRODUCE_RESPONSE_V2, PRODUCE_RESPONSE_V3,
            PRODUCE_RESPONSE_V4, PRODUCE_RESPONSE_V5};
    }

    private final Map<TopicPartition, PartitionResponse> responses;
    private final int throttleTime;

    /**
     * Constructor for Version 0
     * @param responses Produced data grouped by topic-partition
     */
    public ProduceResponse(Map<TopicPartition, PartitionResponse> responses) {
        this(responses, DEFAULT_THROTTLE_TIME);
    }

    /**
     * Constructor for the latest version
     * @param responses Produced data grouped by topic-partition
     * @param throttleTime Time in milliseconds the response was throttled
     */
    public ProduceResponse(Map<TopicPartition, PartitionResponse> responses, int throttleTime) {
        this.responses = responses;
        this.throttleTime = throttleTime;
    }

    /**
     * Constructor from a {@link Struct}.
     */
    public ProduceResponse(Struct struct) {
        responses = new HashMap<>();
        for (Object topicResponse : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicRespStruct = (Struct) topicResponse;
            String topic = topicRespStruct.get(TOPIC_NAME);
            for (Object partResponse : topicRespStruct.getArray(PARTITION_RESPONSES_KEY_NAME)) {
                Struct partRespStruct = (Struct) partResponse;
                int partition = partRespStruct.get(PARTITION_ID);
                Errors error = Errors.forCode(partRespStruct.get(ERROR_CODE));
                long offset = partRespStruct.getLong(BASE_OFFSET_KEY_NAME);
                long logAppendTime = partRespStruct.getLong(LOG_APPEND_TIME_KEY_NAME);
                long logStartOffset = partRespStruct.getOrElse(LOG_START_OFFSET_FIELD, INVALID_OFFSET);
                TopicPartition tp = new TopicPartition(topic, partition);
                responses.put(tp, new PartitionResponse(error, offset, logAppendTime, logStartOffset));
            }
        }
        this.throttleTime = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.PRODUCE.responseSchema(version));

        Map<String, Map<Integer, PartitionResponse>> responseByTopic = CollectionUtils.groupDataByTopic(responses);
        List<Struct> topicDatas = new ArrayList<>(responseByTopic.size());
        for (Map.Entry<String, Map<Integer, PartitionResponse>> entry : responseByTopic.entrySet()) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_NAME, entry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionResponse> partitionEntry : entry.getValue().entrySet()) {
                PartitionResponse part = partitionEntry.getValue();
                short errorCode = part.error.code();
                // If producer sends ProduceRequest V3 or earlier, the client library is not guaranteed to recognize the error code
                // for KafkaStorageException. In this case the client library will translate KafkaStorageException to
                // UnknownServerException which is not retriable. We can ensure that producer will update metadata and retry
                // by converting the KafkaStorageException to NotLeaderForPartitionException in the response if ProduceRequest version <= 3
                if (errorCode == Errors.KAFKA_STORAGE_ERROR.code() && version <= 3)
                    errorCode = Errors.NOT_LEADER_FOR_PARTITION.code();
                Struct partStruct = topicData.instance(PARTITION_RESPONSES_KEY_NAME)
                        .set(PARTITION_ID, partitionEntry.getKey())
                        .set(ERROR_CODE, errorCode)
                        .set(BASE_OFFSET_KEY_NAME, part.baseOffset);
                if (partStruct.hasField(LOG_APPEND_TIME_KEY_NAME))
                    partStruct.set(LOG_APPEND_TIME_KEY_NAME, part.logAppendTime);
                partStruct.setIfExists(LOG_START_OFFSET_FIELD, part.logStartOffset);
                partitionArray.add(partStruct);
            }
            topicData.set(PARTITION_RESPONSES_KEY_NAME, partitionArray.toArray());
            topicDatas.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicDatas.toArray());
        struct.setIfExists(THROTTLE_TIME_MS, throttleTime);

        return struct;
    }

    public Map<TopicPartition, PartitionResponse> responses() {
        return this.responses;
    }

    public int getThrottleTime() {
        return this.throttleTime;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (PartitionResponse response : responses.values())
            updateErrorCounts(errorCounts, response.error);
        return errorCounts;
    }

    public static final class PartitionResponse {
        public Errors error;
        public long baseOffset;
        public long logAppendTime;
        public long logStartOffset;

        public PartitionResponse(Errors error) {
            this(error, INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, INVALID_OFFSET);
        }

        public PartitionResponse(Errors error, long baseOffset, long logAppendTime, long logStartOffset) {
            this.error = error;
            this.baseOffset = baseOffset;
            this.logAppendTime = logAppendTime;
            this.logStartOffset = logStartOffset;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append('{');
            b.append("error: ");
            b.append(error);
            b.append(",offset: ");
            b.append(baseOffset);
            b.append(",logAppendTime: ");
            b.append(logAppendTime);
            b.append(", logStartOffset: ");
            b.append(logStartOffset);
            b.append('}');
            return b.toString();
        }
    }

    public static ProduceResponse parse(ByteBuffer buffer, short version) {
        return new ProduceResponse(ApiKeys.PRODUCE.responseSchema(version).read(buffer));
    }

}
