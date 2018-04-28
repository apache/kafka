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
import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SerializableRecords;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.RECORDS;
import static org.apache.kafka.common.protocol.types.Type.STRING;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

/**
 * Generic FetchResponse implementation.
 * @param <T> Type of partition data in this fetch response.
 */
public abstract class AbstractFetchResponse<T extends AbstractFetchResponse.AbstractPartitionData> extends AbstractResponse {
    protected static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    protected static final String PARTITIONS_KEY_NAME = "partition_responses";

    // partition level field names
    protected static final String PARTITION_HEADER_KEY_NAME = "partition_header";
    protected static final String HIGH_WATERMARK_KEY_NAME = "high_watermark";
    protected static final String LAST_STABLE_OFFSET_KEY_NAME = "last_stable_offset";
    protected static final String LOG_START_OFFSET_KEY_NAME = "log_start_offset";
    protected static final String ABORTED_TRANSACTIONS_KEY_NAME = "aborted_transactions";
    protected static final String RECORD_SET_KEY_NAME = "record_set";

    // aborted transaction field names
    protected static final String PRODUCER_ID_KEY_NAME = "producer_id";
    protected static final String FIRST_OFFSET_KEY_NAME = "first_offset";

    protected static final Schema FETCH_RESPONSE_PARTITION_HEADER_V0 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            new Field(HIGH_WATERMARK_KEY_NAME, INT64, "Last committed offset."));
    protected static final Schema FETCH_RESPONSE_PARTITION_V0 = new Schema(
            new Field(PARTITION_HEADER_KEY_NAME, FETCH_RESPONSE_PARTITION_HEADER_V0),
            new Field(RECORD_SET_KEY_NAME, RECORDS));

    protected static final Schema FETCH_RESPONSE_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_RESPONSE_PARTITION_V0)));

    protected static final Schema FETCH_RESPONSE_V0 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V0)));

    protected static final Schema FETCH_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V0)));
    // Even though fetch response v2 has the same protocol as v1, the record set in the response is different. In v1,
    // record set only includes messages of v0 (magic byte 0). In v2, record set can include messages of v0 and v1
    // (magic byte 0 and 1). For details, see Records, RecordBatch and Record.
    protected static final Schema FETCH_RESPONSE_V2 = FETCH_RESPONSE_V1;

    // The partition ordering is now relevant - partitions will be processed in order they appear in request.
    protected static final Schema FETCH_RESPONSE_V3 = FETCH_RESPONSE_V2;

    // The v4 Fetch Response adds features for transactional consumption (the aborted transaction list and the
    // last stable offset). It also exposes messages with magic v2 (along with older formats).
    protected static final Schema FETCH_RESPONSE_ABORTED_TRANSACTION_V4 = new Schema(
            new Field(PRODUCER_ID_KEY_NAME, INT64, "The producer id associated with the aborted transactions"),
            new Field(FIRST_OFFSET_KEY_NAME, INT64, "The first offset in the aborted transaction"));

    protected static final Schema FETCH_RESPONSE_ABORTED_TRANSACTION_V5 = FETCH_RESPONSE_ABORTED_TRANSACTION_V4;

    protected static final Schema FETCH_RESPONSE_PARTITION_HEADER_V4 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            new Field(HIGH_WATERMARK_KEY_NAME, INT64, "Last committed offset."),
            new Field(LAST_STABLE_OFFSET_KEY_NAME, INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state " +
                    "of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
            new Field(ABORTED_TRANSACTIONS_KEY_NAME, ArrayOf.nullable(FETCH_RESPONSE_ABORTED_TRANSACTION_V4)));

    // FETCH_RESPONSE_PARTITION_HEADER_V5 added log_start_offset field - the earliest available offset of partition data that can be consumed.
    protected static final Schema FETCH_RESPONSE_PARTITION_HEADER_V5 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            new Field(HIGH_WATERMARK_KEY_NAME, INT64, "Last committed offset."),
            new Field(LAST_STABLE_OFFSET_KEY_NAME, INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state " +
                    "of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
            new Field(LOG_START_OFFSET_KEY_NAME, INT64, "Earliest available offset."),
            new Field(ABORTED_TRANSACTIONS_KEY_NAME, ArrayOf.nullable(FETCH_RESPONSE_ABORTED_TRANSACTION_V5)));

    protected static final Schema FETCH_RESPONSE_PARTITION_V4 = new Schema(
            new Field(PARTITION_HEADER_KEY_NAME, FETCH_RESPONSE_PARTITION_HEADER_V4),
            new Field(RECORD_SET_KEY_NAME, RECORDS));

    protected static final Schema FETCH_RESPONSE_PARTITION_V5 = new Schema(
            new Field(PARTITION_HEADER_KEY_NAME, FETCH_RESPONSE_PARTITION_HEADER_V5),
            new Field(RECORD_SET_KEY_NAME, RECORDS));

    protected static final Schema FETCH_RESPONSE_TOPIC_V4 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_RESPONSE_PARTITION_V4)));

    protected static final Schema FETCH_RESPONSE_TOPIC_V5 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_RESPONSE_PARTITION_V5)));

    protected static final Schema FETCH_RESPONSE_V4 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V4)));

    protected static final Schema FETCH_RESPONSE_V5 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V5)));

    /**
     * The body of FETCH_RESPONSE_V6 is the same as FETCH_RESPONSE_V5.
     * The version number is bumped up to indicate that the client supports KafkaStorageException.
     * The KafkaStorageException will be translated to NotLeaderForPartitionException in the response if version <= 5
     */
    protected static final Schema FETCH_RESPONSE_V6 = FETCH_RESPONSE_V5;

    // FETCH_RESPONSE_V7 added incremental fetch responses and a top-level error code.
    public static final Field.Int32 SESSION_ID = new Field.Int32("session_id", "The fetch session ID");

    private static final Schema FETCH_RESPONSE_V7 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            SESSION_ID,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V5)));

    public static Schema[] schemaVersions() {
        return new Schema[] {FETCH_RESPONSE_V0, FETCH_RESPONSE_V1, FETCH_RESPONSE_V2,
                             FETCH_RESPONSE_V3, FETCH_RESPONSE_V4, FETCH_RESPONSE_V5,
                             FETCH_RESPONSE_V6, FETCH_RESPONSE_V7};
    }

    public static final long INVALID_HIGHWATERMARK = -1L;
    public static final long INVALID_LAST_STABLE_OFFSET = -1L;
    public static final long INVALID_LOG_START_OFFSET = -1L;

    /**
     * Possible error codes:
     *
     *  OFFSET_OUT_OF_RANGE (1)
     *  UNKNOWN_TOPIC_OR_PARTITION (3)
     *  NOT_LEADER_FOR_PARTITION (6)
     *  REPLICA_NOT_AVAILABLE (9)
     *  UNKNOWN (-1)
     */

    private final int throttleTimeMs;
    private final Errors error;
    private final int sessionId;
    private final LinkedHashMap<TopicPartition, T> responseData;

    public static final class AbortedTransaction {
        public final long producerId;
        public final long firstOffset;

        public AbortedTransaction(long producerId, long firstOffset) {
            this.producerId = producerId;
            this.firstOffset = firstOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            AbortedTransaction that = (AbortedTransaction) o;

            return producerId == that.producerId && firstOffset == that.firstOffset;
        }

        @Override
        public int hashCode() {
            int result = (int) (producerId ^ (producerId >>> 32));
            result = 31 * result + (int) (firstOffset ^ (firstOffset >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "(producerId=" + producerId + ", firstOffset=" + firstOffset + ")";
        }
    }

    protected static class AbstractPartitionData<T extends SerializableRecords> {
        public final Errors error;
        public final long highWatermark;
        public final long lastStableOffset;
        public final long logStartOffset;
        public final List<AbortedTransaction> abortedTransactions;
        private final T records;

        public AbstractPartitionData(Errors error,
                                     long highWatermark,
                                     long lastStableOffset,
                                     long logStartOffset,
                                     List<AbortedTransaction> abortedTransactions,
                                     T records) {
            this.error = error;
            this.highWatermark = highWatermark;
            this.lastStableOffset = lastStableOffset;
            this.logStartOffset = logStartOffset;
            this.abortedTransactions = abortedTransactions;
            this.records = records;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            AbstractPartitionData that = (AbstractPartitionData) o;

            return error == that.error &&
                    highWatermark == that.highWatermark &&
                    lastStableOffset == that.lastStableOffset &&
                    logStartOffset == that.logStartOffset &&
                    (abortedTransactions == null ? that.abortedTransactions == null : abortedTransactions.equals(that.abortedTransactions)) &&
                    (records == null ? that.records == null : records.equals(that.records));
        }

        @Override
        public int hashCode() {
            int result = error != null ? error.hashCode() : 0;
            result = 31 * result + (int) (highWatermark ^ (highWatermark >>> 32));
            result = 31 * result + (int) (lastStableOffset ^ (lastStableOffset >>> 32));
            result = 31 * result + (int) (logStartOffset ^ (logStartOffset >>> 32));
            result = 31 * result + (abortedTransactions != null ? abortedTransactions.hashCode() : 0);
            result = 31 * result + (records != null ? records.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "(error=" + error +
                    ", highWaterMark=" + highWatermark +
                    ", lastStableOffset = " + lastStableOffset +
                    ", logStartOffset = " + logStartOffset +
                    ", abortedTransactions = " + abortedTransactions +
                    ", recordsSizeInBytes=" + records.sizeInBytes() + ")";
        }

        public T records() {
            return records;
        }
    }

    public static class SerializablePartitionData extends AbstractPartitionData<SerializableRecords> {
        public SerializablePartitionData(Errors error,
                                         long highWatermark,
                                         long lastStableOffset,
                                         long logStartOffset,
                                         List<AbortedTransaction> abortedTransactions,
                                         SerializableRecords records) {
            super(error, highWatermark, lastStableOffset, logStartOffset, abortedTransactions, records);
        }
    }

    public static class PartitionData extends SerializablePartitionData {
        public PartitionData(Errors error,
                             long highWatermark,
                             long lastStableOffset,
                             long logStartOffset,
                             List<AbortedTransaction> abortedTransactions,
                             Records records) {
            super(error, highWatermark, lastStableOffset, logStartOffset, abortedTransactions, records);
        }

        @Override
        public Records records() {
            return (Records) (super.records());
        }
    }

    /**
     * From version 3 or later, the entries in `responseData` should be in the same order as the entries in
     * `FetchRequest.fetchData`.
     *
     * @param error             The top-level error code.
     * @param responseData      The fetched data grouped by partition.
     * @param throttleTimeMs    The time in milliseconds that the response was throttled
     * @param sessionId         The fetch session id.
     */
    public AbstractFetchResponse(Errors error, LinkedHashMap<TopicPartition, T> responseData,
                                 int throttleTimeMs, int sessionId) {
        this.error = error;
        this.responseData = responseData;
        this.throttleTimeMs = throttleTimeMs;
        this.sessionId = sessionId;
    }

    @Override
    public Struct toStruct(short version) {
        return toStruct(version, throttleTimeMs, error, responseData.entrySet().iterator(), sessionId);
    }

    @Override
    protected Send toSend(String dest, ResponseHeader responseHeader, short apiVersion) {
        Struct responseHeaderStruct = responseHeader.toStruct();
        Struct responseBodyStruct = toStruct(apiVersion);

        // write the total size and the response header
        ByteBuffer buffer = ByteBuffer.allocate(responseHeaderStruct.sizeOf() + 4);
        buffer.putInt(responseHeaderStruct.sizeOf() + responseBodyStruct.sizeOf());
        responseHeaderStruct.writeTo(buffer);
        buffer.rewind();

        Queue<Send> sends = new ArrayDeque<>();
        sends.add(new ByteBufferSend(dest, buffer));
        addResponseData(responseBodyStruct, throttleTimeMs, dest, sends);
        return new MultiRecordsSend(dest, sends);
    }

    public Errors error() {
        return error;
    }

    public LinkedHashMap<TopicPartition, T> responseData() {
        return responseData;
    }

    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }

    public int sessionId() {
        return sessionId;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (T response : responseData.values())
            updateErrorCounts(errorCounts, response.error);
        return errorCounts;
    }

    private static void addResponseData(Struct struct, int throttleTimeMs, String dest, Queue<Send> sends) {
        Object[] allTopicData = struct.getArray(RESPONSES_KEY_NAME);

        if (struct.hasField(ERROR_CODE)) {
            ByteBuffer buffer = ByteBuffer.allocate(14);
            buffer.putInt(throttleTimeMs);
            buffer.putShort(struct.get(ERROR_CODE));
            buffer.putInt(struct.get(SESSION_ID));
            buffer.putInt(allTopicData.length);
            buffer.rewind();
            sends.add(new ByteBufferSend(dest, buffer));
        } else if (struct.hasField(THROTTLE_TIME_MS)) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putInt(throttleTimeMs);
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

    private static void addTopicData(String dest, Queue<Send> sends, Struct topicData) {
        String topic = topicData.get(TOPIC_NAME);
        Object[] allPartitionData = topicData.getArray(PARTITIONS_KEY_NAME);

        // include the topic header and the count for the number of partitions
        ByteBuffer buffer = ByteBuffer.allocate(STRING.sizeOf(topic) + 4);
        STRING.write(buffer, topic);
        buffer.putInt(allPartitionData.length);
        buffer.rewind();
        sends.add(new ByteBufferSend(dest, buffer));

        for (Object partitionData : allPartitionData)
            addPartitionData(dest, sends, (Struct) partitionData);
    }

    private static void addPartitionData(String dest, Queue<Send> sends, Struct partitionData) {
        Struct header = partitionData.getStruct(PARTITION_HEADER_KEY_NAME);
        SerializableRecords records = partitionData.getRecords(RECORD_SET_KEY_NAME);

        // include the partition header and the size of the record set
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + 4);
        header.writeTo(buffer);
        buffer.putInt(records.sizeInBytes());
        buffer.rewind();
        sends.add(new ByteBufferSend(dest, buffer));

        // finally the send for the record set itself
        sends.add(new RecordsSend(dest, records));
    }

    private static <T extends AbstractPartitionData> Struct toStruct(short version,
                                                                     int throttleTimeMs,
                                                                     Errors error,
                                                                     Iterator<Map.Entry<TopicPartition, T>> partIterator,
                                                                     int sessionId) {
        Struct struct = new Struct(ApiKeys.FETCH.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        struct.setIfExists(ERROR_CODE, error.code());
        struct.setIfExists(SESSION_ID, sessionId);
        List<FetchRequest.TopicAndPartitionData<T>> topicsData =
                FetchRequest.TopicAndPartitionData.batchByTopic(partIterator);
        List<Struct> topicArray = new ArrayList<>();
        for (FetchRequest.TopicAndPartitionData<T> topicEntry : topicsData) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, T> partitionEntry : topicEntry.partitions.entrySet()) {
                T fetchPartitionData = partitionEntry.getValue();
                short errorCode = fetchPartitionData.error.code();
                // If consumer sends FetchRequest V5 or earlier, the client library is not guaranteed to recognize the error code
                // for KafkaStorageException. In this case the client library will translate KafkaStorageException to
                // UnknownServerException which is not retriable. We can ensure that consumer will update metadata and retry
                // by converting the KafkaStorageException to NotLeaderForPartitionException in the response if FetchRequest version <= 5
                if (errorCode == Errors.KAFKA_STORAGE_ERROR.code() && version <= 5)
                    errorCode = Errors.NOT_LEADER_FOR_PARTITION.code();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                Struct partitionDataHeader = partitionData.instance(PARTITION_HEADER_KEY_NAME);
                partitionDataHeader.set(PARTITION_ID, partitionEntry.getKey());
                partitionDataHeader.set(ERROR_CODE, errorCode);
                partitionDataHeader.set(HIGH_WATERMARK_KEY_NAME, fetchPartitionData.highWatermark);

                if (partitionDataHeader.hasField(LAST_STABLE_OFFSET_KEY_NAME)) {
                    partitionDataHeader.set(LAST_STABLE_OFFSET_KEY_NAME, fetchPartitionData.lastStableOffset);

                    if (fetchPartitionData.abortedTransactions == null) {
                        partitionDataHeader.set(ABORTED_TRANSACTIONS_KEY_NAME, null);
                    } else {
                        List<Struct> abortedTransactionStructs = new ArrayList<>(fetchPartitionData.abortedTransactions.size());
                        List<AbortedTransaction> abortedTransactions = fetchPartitionData.abortedTransactions;
                        for (AbortedTransaction abortedTransaction : abortedTransactions) {
                            Struct abortedTransactionStruct = partitionDataHeader.instance(ABORTED_TRANSACTIONS_KEY_NAME);
                            abortedTransactionStruct.set(PRODUCER_ID_KEY_NAME, abortedTransaction.producerId);
                            abortedTransactionStruct.set(FIRST_OFFSET_KEY_NAME, abortedTransaction.firstOffset);
                            abortedTransactionStructs.add(abortedTransactionStruct);
                        }
                        partitionDataHeader.set(ABORTED_TRANSACTIONS_KEY_NAME, abortedTransactionStructs.toArray());
                    }
                }
                if (partitionDataHeader.hasField(LOG_START_OFFSET_KEY_NAME))
                    partitionDataHeader.set(LOG_START_OFFSET_KEY_NAME, fetchPartitionData.logStartOffset);

                partitionData.set(PARTITION_HEADER_KEY_NAME, partitionDataHeader);
                partitionData.set(RECORD_SET_KEY_NAME, fetchPartitionData.records());
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicArray.toArray());
        return struct;
    }

    /**
     * Convenience method to find the size of a response.
     *
     * @param version       The version of the response to use.
     * @param partIterator  The partition iterator.
     * @return              The response size in bytes.
     */
    public static <T extends AbstractPartitionData> int sizeOf(short version,
                                                               Iterator<Map.Entry<TopicPartition, T>> partIterator) {
        // Since the throttleTimeMs and metadata field sizes are constant and fixed, we can
        // use arbitrary values here without affecting the result.
        return 4 + toStruct(version, 0, Errors.NONE, partIterator, INVALID_SESSION_ID).sizeOf();
    }
}
