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
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MultiRecordsSend;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Predicate;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.RECORDS;
import static org.apache.kafka.common.protocol.types.Type.STRING;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

/**
 * This wrapper supports all versions of the Fetch API
 *
 * Possible error codes:
 *
 * - {@link Errors#OFFSET_OUT_OF_RANGE} If the fetch offset is out of range for a requested partition
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED} If the user does not have READ access to a requested topic
 * - {@link Errors#REPLICA_NOT_AVAILABLE} If the request is received by a broker which is not a replica
 * - {@link Errors#NOT_LEADER_FOR_PARTITION} If the broker is not a leader and either the provided leader epoch
 *     matches the known leader epoch on the broker or is empty
 * - {@link Errors#FENCED_LEADER_EPOCH} If the epoch is lower than the broker's epoch
 * - {@link Errors#UNKNOWN_LEADER_EPOCH} If the epoch is larger than the broker's epoch
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} If the broker does not have metadata for a topic or partition
 * - {@link Errors#KAFKA_STORAGE_ERROR} If the log directory for one of the requested partitions is offline
 * - {@link Errors#UNSUPPORTED_COMPRESSION_TYPE} If a fetched topic is using a compression type which is
 *     not supported by the fetch request version
 * - {@link Errors#UNKNOWN_SERVER_ERROR} For any unexpected errors
 */
public class FetchResponse<T extends BaseRecords> extends AbstractResponse {

    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String PARTITIONS_KEY_NAME = "partition_responses";

    // partition level fields
    private static final Field.Int64 HIGH_WATERMARK = new Field.Int64("high_watermark",
            "Last committed offset.");
    private static final Field.Int64 LOG_START_OFFSET = new Field.Int64("log_start_offset",
            "Earliest available offset.");
    private static final Field.Int32 PREFERRED_READ_REPLICA = new Field.Int32("preferred_read_replica",
            "The ID of the replica that the consumer should prefer.");

    private static final String PARTITION_HEADER_KEY_NAME = "partition_header";
    private static final String ABORTED_TRANSACTIONS_KEY_NAME = "aborted_transactions";
    private static final String RECORD_SET_KEY_NAME = "record_set";

    private static final Schema FETCH_RESPONSE_PARTITION_HEADER_V0 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            HIGH_WATERMARK);
    private static final Schema FETCH_RESPONSE_PARTITION_V0 = new Schema(
            new Field(PARTITION_HEADER_KEY_NAME, FETCH_RESPONSE_PARTITION_HEADER_V0),
            new Field(RECORD_SET_KEY_NAME, RECORDS));

    private static final Schema FETCH_RESPONSE_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_RESPONSE_PARTITION_V0)));

    private static final Schema FETCH_RESPONSE_V0 = new Schema(
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V0)));

    // V1 bumped for the addition of the throttle time
    private static final Schema FETCH_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V0)));

    // V2 bumped to indicate the client support message format V1 which uses relative offset and has timestamp.
    private static final Schema FETCH_RESPONSE_V2 = FETCH_RESPONSE_V1;

    // V3 bumped for addition of top-levl max_bytes field and to indicate that partition ordering is relevant
    private static final Schema FETCH_RESPONSE_V3 = FETCH_RESPONSE_V2;

    // V4 adds features for transactional consumption (the aborted transaction list and the
    // last stable offset). It also exposes messages with magic v2 (along with older formats).
    // aborted transaction field names
    private static final Field.Int64 LAST_STABLE_OFFSET = new Field.Int64("last_stable_offset",
            "The last stable offset (or LSO) of the partition. This is the last offset such that the state " +
                    "of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)");
    private static final Field.Int64 PRODUCER_ID = new Field.Int64("producer_id",
            "The producer id associated with the aborted transactions");
    private static final Field.Int64 FIRST_OFFSET = new Field.Int64("first_offset",
            "The first offset in the aborted transaction");

    private static final Schema FETCH_RESPONSE_ABORTED_TRANSACTION_V4 = new Schema(
            PRODUCER_ID,
            FIRST_OFFSET);

    private static final Schema FETCH_RESPONSE_PARTITION_HEADER_V4 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            HIGH_WATERMARK,
            LAST_STABLE_OFFSET,
            new Field(ABORTED_TRANSACTIONS_KEY_NAME, ArrayOf.nullable(FETCH_RESPONSE_ABORTED_TRANSACTION_V4)));

    // V5 added log_start_offset field - the earliest available offset of partition data that can be consumed.
    private static final Schema FETCH_RESPONSE_PARTITION_HEADER_V5 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            HIGH_WATERMARK,
            LAST_STABLE_OFFSET,
            LOG_START_OFFSET,
            new Field(ABORTED_TRANSACTIONS_KEY_NAME, ArrayOf.nullable(FETCH_RESPONSE_ABORTED_TRANSACTION_V4)));

    // Introduced in V11 to support read from followers (KIP-392)
    private static final Schema FETCH_RESPONSE_PARTITION_HEADER_V6 = new Schema(
            PARTITION_ID,
            ERROR_CODE,
            HIGH_WATERMARK,
            LAST_STABLE_OFFSET,
            LOG_START_OFFSET,
            new Field(ABORTED_TRANSACTIONS_KEY_NAME, ArrayOf.nullable(FETCH_RESPONSE_ABORTED_TRANSACTION_V4)),
            PREFERRED_READ_REPLICA);

    private static final Schema FETCH_RESPONSE_PARTITION_V4 = new Schema(
            new Field(PARTITION_HEADER_KEY_NAME, FETCH_RESPONSE_PARTITION_HEADER_V4),
            new Field(RECORD_SET_KEY_NAME, RECORDS));

    private static final Schema FETCH_RESPONSE_PARTITION_V5 = new Schema(
            new Field(PARTITION_HEADER_KEY_NAME, FETCH_RESPONSE_PARTITION_HEADER_V5),
            new Field(RECORD_SET_KEY_NAME, RECORDS));

    private static final Schema FETCH_RESPONSE_PARTITION_V6 = new Schema(
            new Field(PARTITION_HEADER_KEY_NAME, FETCH_RESPONSE_PARTITION_HEADER_V6),
            new Field(RECORD_SET_KEY_NAME, RECORDS));

    private static final Schema FETCH_RESPONSE_TOPIC_V4 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_RESPONSE_PARTITION_V4)));

    private static final Schema FETCH_RESPONSE_TOPIC_V5 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_RESPONSE_PARTITION_V5)));

    private static final Schema FETCH_RESPONSE_TOPIC_V6 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(FETCH_RESPONSE_PARTITION_V6)));

    private static final Schema FETCH_RESPONSE_V4 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V4)));

    private static final Schema FETCH_RESPONSE_V5 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V5)));

    // V6 bumped up to indicate that the client supports KafkaStorageException. The KafkaStorageException will
    // be translated to NotLeaderForPartitionException in the response if version <= 5
    private static final Schema FETCH_RESPONSE_V6 = FETCH_RESPONSE_V5;

    // V7 added incremental fetch responses and a top-level error code.
    private static final Field.Int32 SESSION_ID = new Field.Int32("session_id", "The fetch session ID");

    private static final Schema FETCH_RESPONSE_V7 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            SESSION_ID,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V5)));

    // V8 bump used to indicate that on quota violation brokers send out responses before throttling.
    private static final Schema FETCH_RESPONSE_V8 = FETCH_RESPONSE_V7;

    // V9 adds the current leader epoch (see KIP-320)
    private static final Schema FETCH_RESPONSE_V9 = FETCH_RESPONSE_V8;

    // V10 bumped up to indicate ZStandard capability. (see KIP-110)
    private static final Schema FETCH_RESPONSE_V10 = FETCH_RESPONSE_V9;

    // V11 added preferred read replica for each partition response to support read from followers (KIP-392)
    private static final Schema FETCH_RESPONSE_V11 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            SESSION_ID,
            new Field(RESPONSES_KEY_NAME, new ArrayOf(FETCH_RESPONSE_TOPIC_V6)));

    public static Schema[] schemaVersions() {
        return new Schema[] {FETCH_RESPONSE_V0, FETCH_RESPONSE_V1, FETCH_RESPONSE_V2,
            FETCH_RESPONSE_V3, FETCH_RESPONSE_V4, FETCH_RESPONSE_V5, FETCH_RESPONSE_V6,
            FETCH_RESPONSE_V7, FETCH_RESPONSE_V8, FETCH_RESPONSE_V9, FETCH_RESPONSE_V10,
            FETCH_RESPONSE_V11};
    }

    public static final long INVALID_HIGHWATERMARK = -1L;
    public static final long INVALID_LAST_STABLE_OFFSET = -1L;
    public static final long INVALID_LOG_START_OFFSET = -1L;
    public static final int INVALID_PREFERRED_REPLICA_ID = -1;

    private final int throttleTimeMs;
    private final Errors error;
    private final int sessionId;
    private final LinkedHashMap<TopicPartition, PartitionData<T>> responseData;

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
            int result = Long.hashCode(producerId);
            result = 31 * result + Long.hashCode(firstOffset);
            return result;
        }

        @Override
        public String toString() {
            return "(producerId=" + producerId + ", firstOffset=" + firstOffset + ")";
        }
    }

    public static final class PartitionData<T extends BaseRecords> {
        public final Errors error;
        public final long highWatermark;
        public final long lastStableOffset;
        public final long logStartOffset;
        public final Optional<Integer> preferredReadReplica;
        public final List<AbortedTransaction> abortedTransactions;
        public final T records;

        public PartitionData(Errors error,
                             long highWatermark,
                             long lastStableOffset,
                             long logStartOffset,
                             Optional<Integer> preferredReadReplica,
                             List<AbortedTransaction> abortedTransactions,
                             T records) {
            this.error = error;
            this.highWatermark = highWatermark;
            this.lastStableOffset = lastStableOffset;
            this.logStartOffset = logStartOffset;
            this.preferredReadReplica = preferredReadReplica;
            this.abortedTransactions = abortedTransactions;
            this.records = records;
        }

        public PartitionData(Errors error,
                             long highWatermark,
                             long lastStableOffset,
                             long logStartOffset,
                             List<AbortedTransaction> abortedTransactions,
                             T records) {
            this(error, highWatermark, lastStableOffset, logStartOffset, Optional.empty(), abortedTransactions, records);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PartitionData that = (PartitionData) o;

            return error == that.error &&
                    highWatermark == that.highWatermark &&
                    lastStableOffset == that.lastStableOffset &&
                    logStartOffset == that.logStartOffset &&
                    Objects.equals(preferredReadReplica, that.preferredReadReplica) &&
                    Objects.equals(abortedTransactions, that.abortedTransactions) &&
                    Objects.equals(records, that.records);
        }

        @Override
        public int hashCode() {
            int result = error != null ? error.hashCode() : 0;
            result = 31 * result + Long.hashCode(highWatermark);
            result = 31 * result + Long.hashCode(lastStableOffset);
            result = 31 * result + Long.hashCode(logStartOffset);
            result = 31 * result + Objects.hashCode(preferredReadReplica);
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
                    ", preferredReadReplica = " + preferredReadReplica.map(Object::toString).orElse("absent") +
                    ", abortedTransactions = " + abortedTransactions +
                    ", recordsSizeInBytes=" + records.sizeInBytes() + ")";
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
    public FetchResponse(Errors error,
                         LinkedHashMap<TopicPartition, PartitionData<T>> responseData,
                         int throttleTimeMs,
                         int sessionId) {
        this.error = error;
        this.responseData = responseData;
        this.throttleTimeMs = throttleTimeMs;
        this.sessionId = sessionId;
    }

    public static FetchResponse<MemoryRecords> parse(Struct struct) {
        LinkedHashMap<TopicPartition, PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                Struct partitionResponseHeader = partitionResponse.getStruct(PARTITION_HEADER_KEY_NAME);
                int partition = partitionResponseHeader.get(PARTITION_ID);
                Errors error = Errors.forCode(partitionResponseHeader.get(ERROR_CODE));
                long highWatermark = partitionResponseHeader.get(HIGH_WATERMARK);
                long lastStableOffset = partitionResponseHeader.getOrElse(LAST_STABLE_OFFSET, INVALID_LAST_STABLE_OFFSET);
                long logStartOffset = partitionResponseHeader.getOrElse(LOG_START_OFFSET, INVALID_LOG_START_OFFSET);
                Optional<Integer> preferredReadReplica = Optional.of(
                    partitionResponseHeader.getOrElse(PREFERRED_READ_REPLICA, INVALID_PREFERRED_REPLICA_ID)
                ).filter(Predicate.isEqual(INVALID_PREFERRED_REPLICA_ID).negate());

                BaseRecords baseRecords = partitionResponse.getRecords(RECORD_SET_KEY_NAME);
                if (!(baseRecords instanceof MemoryRecords))
                    throw new IllegalStateException("Unknown records type found: " + baseRecords.getClass());
                MemoryRecords records = (MemoryRecords) baseRecords;

                List<AbortedTransaction> abortedTransactions = null;
                if (partitionResponseHeader.hasField(ABORTED_TRANSACTIONS_KEY_NAME)) {
                    Object[] abortedTransactionsArray = partitionResponseHeader.getArray(ABORTED_TRANSACTIONS_KEY_NAME);
                    if (abortedTransactionsArray != null) {
                        abortedTransactions = new ArrayList<>(abortedTransactionsArray.length);
                        for (Object abortedTransactionObj : abortedTransactionsArray) {
                            Struct abortedTransactionStruct = (Struct) abortedTransactionObj;
                            long producerId = abortedTransactionStruct.get(PRODUCER_ID);
                            long firstOffset = abortedTransactionStruct.get(FIRST_OFFSET);
                            abortedTransactions.add(new AbortedTransaction(producerId, firstOffset));
                        }
                    }
                }

                PartitionData<MemoryRecords> partitionData = new PartitionData<>(error, highWatermark, lastStableOffset,
                        logStartOffset, preferredReadReplica, abortedTransactions, records);
                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
        return new FetchResponse<>(Errors.forCode(struct.getOrElse(ERROR_CODE, (short) 0)), responseData,
                struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME), struct.getOrElse(SESSION_ID, INVALID_SESSION_ID));
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

    public LinkedHashMap<TopicPartition, PartitionData<T>> responseData() {
        return responseData;
    }

    @Override
    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }

    public int sessionId() {
        return sessionId;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (PartitionData response : responseData.values())
            updateErrorCounts(errorCounts, response.error);
        return errorCounts;
    }

    public static FetchResponse<MemoryRecords> parse(ByteBuffer buffer, short version) {
        return parse(ApiKeys.FETCH.responseSchema(version).read(buffer));
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
        BaseRecords records = partitionData.getRecords(RECORD_SET_KEY_NAME);

        // include the partition header and the size of the record set
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + 4);
        header.writeTo(buffer);
        buffer.putInt(records.sizeInBytes());
        buffer.rewind();
        sends.add(new ByteBufferSend(dest, buffer));

        // finally the send for the record set itself
        sends.add(records.toSend(dest));
    }

    private static <T extends BaseRecords> Struct toStruct(short version, int throttleTimeMs, Errors error,
                                                       Iterator<Map.Entry<TopicPartition, PartitionData<T>>> partIterator,
                                                       int sessionId) {
        Struct struct = new Struct(ApiKeys.FETCH.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        struct.setIfExists(ERROR_CODE, error.code());
        struct.setIfExists(SESSION_ID, sessionId);
        List<FetchRequest.TopicAndPartitionData<PartitionData<T>>> topicsData =
                FetchRequest.TopicAndPartitionData.batchByTopic(partIterator);
        List<Struct> topicArray = new ArrayList<>();
        for (FetchRequest.TopicAndPartitionData<PartitionData<T>> topicEntry: topicsData) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData<T>> partitionEntry : topicEntry.partitions.entrySet()) {
                PartitionData<T> fetchPartitionData = partitionEntry.getValue();
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
                partitionDataHeader.set(HIGH_WATERMARK, fetchPartitionData.highWatermark);

                if (partitionDataHeader.hasField(LAST_STABLE_OFFSET)) {
                    partitionDataHeader.set(LAST_STABLE_OFFSET, fetchPartitionData.lastStableOffset);

                    if (fetchPartitionData.abortedTransactions == null) {
                        partitionDataHeader.set(ABORTED_TRANSACTIONS_KEY_NAME, null);
                    } else {
                        List<Struct> abortedTransactionStructs = new ArrayList<>(fetchPartitionData.abortedTransactions.size());
                        for (AbortedTransaction abortedTransaction : fetchPartitionData.abortedTransactions) {
                            Struct abortedTransactionStruct = partitionDataHeader.instance(ABORTED_TRANSACTIONS_KEY_NAME);
                            abortedTransactionStruct.set(PRODUCER_ID, abortedTransaction.producerId);
                            abortedTransactionStruct.set(FIRST_OFFSET, abortedTransaction.firstOffset);
                            abortedTransactionStructs.add(abortedTransactionStruct);
                        }
                        partitionDataHeader.set(ABORTED_TRANSACTIONS_KEY_NAME, abortedTransactionStructs.toArray());
                    }
                }
                partitionDataHeader.setIfExists(LOG_START_OFFSET, fetchPartitionData.logStartOffset);
                partitionDataHeader.setIfExists(PREFERRED_READ_REPLICA, fetchPartitionData.preferredReadReplica.orElse(-1));
                partitionData.set(PARTITION_HEADER_KEY_NAME, partitionDataHeader);
                partitionData.set(RECORD_SET_KEY_NAME, fetchPartitionData.records);
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
    public static <T extends BaseRecords> int sizeOf(short version,
                                                     Iterator<Map.Entry<TopicPartition, PartitionData<T>>> partIterator) {
        // Since the throttleTimeMs and metadata field sizes are constant and fixed, we can
        // use arbitrary values here without affecting the result.
        return 4 + toStruct(version, 0, Errors.NONE, partIterator, INVALID_SESSION_ID).sizeOf();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 8;
    }
}
