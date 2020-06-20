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
import java.util.Collections;
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
     * {@link Errors#CORRUPT_MESSAGE}
     * {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
     * {@link Errors#NOT_LEADER_FOR_PARTITION}
     * {@link Errors#MESSAGE_TOO_LARGE}
     * {@link Errors#INVALID_TOPIC_EXCEPTION}
     * {@link Errors#RECORD_LIST_TOO_LARGE}
     * {@link Errors#NOT_ENOUGH_REPLICAS}
     * {@link Errors#NOT_ENOUGH_REPLICAS_AFTER_APPEND}
     * {@link Errors#INVALID_REQUIRED_ACKS}
     * {@link Errors#TOPIC_AUTHORIZATION_FAILED}
     * {@link Errors#UNSUPPORTED_FOR_MESSAGE_FORMAT}
     * {@link Errors#INVALID_PRODUCER_EPOCH}
     * {@link Errors#CLUSTER_AUTHORIZATION_FAILED}
     * {@link Errors#TRANSACTIONAL_ID_AUTHORIZATION_FAILED}
     * {@link Errors#INVALID_RECORD}
     */

    private static final String BASE_OFFSET_KEY_NAME = "base_offset";
    private static final String LOG_APPEND_TIME_KEY_NAME = "log_append_time";
    private static final String LOG_START_OFFSET_KEY_NAME = "log_start_offset";
    private static final String RECORD_ERRORS_KEY_NAME = "record_errors";
    private static final String BATCH_INDEX_KEY_NAME = "batch_index";
    private static final String BATCH_INDEX_ERROR_MESSAGE_KEY_NAME = "batch_index_error_message";
    private static final String ERROR_MESSAGE_KEY_NAME = "error_message";

    private static final Field.Int64 LOG_START_OFFSET_FIELD = new Field.Int64(LOG_START_OFFSET_KEY_NAME,
            "The start offset of the log at the time this produce response was created", INVALID_OFFSET);
    private static final Field.NullableStr BATCH_INDEX_ERROR_MESSAGE_FIELD = new Field.NullableStr(BATCH_INDEX_ERROR_MESSAGE_KEY_NAME,
            "The error message of the record that caused the batch to be dropped");
    private static final Field.NullableStr ERROR_MESSAGE_FIELD = new Field.NullableStr(ERROR_MESSAGE_KEY_NAME,
            "The global error message summarizing the common root cause of the records that caused the batch to be dropped");

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

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema PRODUCE_RESPONSE_V6 = PRODUCE_RESPONSE_V5;

    /**
     * V7 bumped up to indicate ZStandard capability. (see KIP-110)
     */
    private static final Schema PRODUCE_RESPONSE_V7 = PRODUCE_RESPONSE_V6;

    /**
     * V8 adds record_errors and error_message. (see KIP-467)
     */
    public static final Schema PRODUCE_RESPONSE_V8 = new Schema(
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
                            LOG_START_OFFSET_FIELD,
                            new Field(RECORD_ERRORS_KEY_NAME, new ArrayOf(new Schema(
                                    new Field.Int32(BATCH_INDEX_KEY_NAME, "The batch index of the record " +
                                            "that caused the batch to be dropped"),
                                    BATCH_INDEX_ERROR_MESSAGE_FIELD
                            )), "The batch indices of records that caused the batch to be dropped"),
                            ERROR_MESSAGE_FIELD)))))),
            THROTTLE_TIME_MS);

    public static Schema[] schemaVersions() {
        return new Schema[]{PRODUCE_RESPONSE_V0, PRODUCE_RESPONSE_V1, PRODUCE_RESPONSE_V2, PRODUCE_RESPONSE_V3,
            PRODUCE_RESPONSE_V4, PRODUCE_RESPONSE_V5, PRODUCE_RESPONSE_V6, PRODUCE_RESPONSE_V7, PRODUCE_RESPONSE_V8};
    }

    private final Map<TopicPartition, PartitionResponse> responses;
    private final int throttleTimeMs;

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
     * @param throttleTimeMs Time in milliseconds the response was throttled
     */
    public ProduceResponse(Map<TopicPartition, PartitionResponse> responses, int throttleTimeMs) {
        this.responses = responses;
        this.throttleTimeMs = throttleTimeMs;
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
                long logAppendTime = partRespStruct.hasField(LOG_APPEND_TIME_KEY_NAME) ?
                        partRespStruct.getLong(LOG_APPEND_TIME_KEY_NAME) : RecordBatch.NO_TIMESTAMP;
                long logStartOffset = partRespStruct.getOrElse(LOG_START_OFFSET_FIELD, INVALID_OFFSET);

                List<RecordError> recordErrors = Collections.emptyList();
                if (partRespStruct.hasField(RECORD_ERRORS_KEY_NAME)) {
                    Object[] recordErrorsArray = partRespStruct.getArray(RECORD_ERRORS_KEY_NAME);
                    if (recordErrorsArray.length > 0) {
                        recordErrors = new ArrayList<>(recordErrorsArray.length);
                        for (Object indexAndMessage : recordErrorsArray) {
                            Struct indexAndMessageStruct = (Struct) indexAndMessage;
                            recordErrors.add(new RecordError(
                                    indexAndMessageStruct.getInt(BATCH_INDEX_KEY_NAME),
                                    indexAndMessageStruct.get(BATCH_INDEX_ERROR_MESSAGE_FIELD)
                            ));
                        }
                    }
                }

                String errorMessage = partRespStruct.getOrElse(ERROR_MESSAGE_FIELD, null);
                TopicPartition tp = new TopicPartition(topic, partition);
                responses.put(tp, new PartitionResponse(error, offset, logAppendTime, logStartOffset, recordErrors, errorMessage));
            }
        }
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.PRODUCE.responseSchema(version));

        Map<String, Map<Integer, PartitionResponse>> responseByTopic = CollectionUtils.groupPartitionDataByTopic(responses);
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
                partStruct.setIfExists(LOG_APPEND_TIME_KEY_NAME, part.logAppendTime);
                partStruct.setIfExists(LOG_START_OFFSET_FIELD, part.logStartOffset);

                if (partStruct.hasField(RECORD_ERRORS_KEY_NAME)) {
                    List<Struct> recordErrors = Collections.emptyList();
                    if (!part.recordErrors.isEmpty()) {
                        recordErrors = new ArrayList<>();
                        for (RecordError indexAndMessage : part.recordErrors) {
                            Struct indexAndMessageStruct = partStruct.instance(RECORD_ERRORS_KEY_NAME)
                                    .set(BATCH_INDEX_KEY_NAME, indexAndMessage.batchIndex)
                                    .set(BATCH_INDEX_ERROR_MESSAGE_FIELD, indexAndMessage.message);
                            recordErrors.add(indexAndMessageStruct);
                        }
                    }
                    partStruct.set(RECORD_ERRORS_KEY_NAME, recordErrors.toArray());
                }

                partStruct.setIfExists(ERROR_MESSAGE_FIELD, part.errorMessage);
                partitionArray.add(partStruct);
            }
            topicData.set(PARTITION_RESPONSES_KEY_NAME, partitionArray.toArray());
            topicDatas.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicDatas.toArray());
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);

        return struct;
    }

    public Map<TopicPartition, PartitionResponse> responses() {
        return this.responses;
    }

    @Override
    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        responses.values().forEach(response ->
            updateErrorCounts(errorCounts, response.error)
        );
        return errorCounts;
    }

    public static final class PartitionResponse {
        public Errors error;
        public long baseOffset;
        public long logAppendTime;
        public long logStartOffset;
        public List<RecordError> recordErrors;
        public String errorMessage;

        public PartitionResponse(Errors error) {
            this(error, INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, INVALID_OFFSET);
        }

        public PartitionResponse(Errors error, long baseOffset, long logAppendTime, long logStartOffset) {
            this(error, baseOffset, logAppendTime, logStartOffset, Collections.emptyList(), null);
        }

        public PartitionResponse(Errors error, long baseOffset, long logAppendTime, long logStartOffset, List<RecordError> recordErrors) {
            this(error, baseOffset, logAppendTime, logStartOffset, recordErrors, null);
        }

        public PartitionResponse(Errors error, long baseOffset, long logAppendTime, long logStartOffset, List<RecordError> recordErrors, String errorMessage) {
            this.error = error;
            this.baseOffset = baseOffset;
            this.logAppendTime = logAppendTime;
            this.logStartOffset = logStartOffset;
            this.recordErrors = recordErrors;
            this.errorMessage = errorMessage;
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
            b.append(", recordErrors: ");
            b.append(recordErrors);
            b.append(", errorMessage: ");
            if (errorMessage != null) {
                b.append(errorMessage);
            } else {
                b.append("null");
            }
            b.append('}');
            return b.toString();
        }
    }

    public static final class RecordError {
        public final int batchIndex;
        public final String message;

        public RecordError(int batchIndex, String message) {
            this.batchIndex = batchIndex;
            this.message = message;
        }

        public RecordError(int batchIndex) {
            this.batchIndex = batchIndex;
            this.message = null;
        }

    }

    public static ProduceResponse parse(ByteBuffer buffer, short version) {
        return new ProduceResponse(ApiKeys.PRODUCE.responseSchema(version).read(buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 6;
    }
}
