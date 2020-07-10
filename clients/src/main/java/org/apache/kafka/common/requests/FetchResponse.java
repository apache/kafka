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
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.RecordsReader;
import org.apache.kafka.common.protocol.RecordsWriter;
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
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.RECORDS;
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
 * - {@link Errors#CORRUPT_MESSAGE} If corrupt message encountered, e.g. when the broker scans the log to find
 *     the fetch offset after the index lookup
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

    public static final long INVALID_HIGHWATERMARK = -1L;
    public static final long INVALID_LAST_STABLE_OFFSET = -1L;
    public static final long INVALID_LOG_START_OFFSET = -1L;
    public static final int INVALID_PREFERRED_REPLICA_ID = -1;

    private final FetchResponseData fetchResponseData;
    private final LinkedHashMap<TopicPartition, PartitionData<T>> responseDataMap;

    @Override
    public FetchResponseData data() {
        return fetchResponseData;
    }


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

        static AbortedTransaction fromMessage(FetchResponseData.AbortedTransaction abortedTransaction) {
            return new AbortedTransaction(abortedTransaction.producerId(), abortedTransaction.firstOffset());
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
            this.error = error;
            this.highWatermark = highWatermark;
            this.lastStableOffset = lastStableOffset;
            this.logStartOffset = logStartOffset;
            this.preferredReadReplica = Optional.empty();
            this.abortedTransactions = abortedTransactions;
            this.records = records;
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
        this.fetchResponseData = toMessage(throttleTimeMs, error, responseData.entrySet().iterator(), sessionId);
        this.responseDataMap = responseData;
    }

    public FetchResponse(FetchResponseData fetchResponseData) {
        this.fetchResponseData = fetchResponseData;
        this.responseDataMap = toResponseDataMap(fetchResponseData);
    }

    @Override
    public Struct toStruct(short version) {
        return fetchResponseData.toStruct(version);
    }

    @Override
    protected Send toSend(String dest, ResponseHeader responseHeader, short apiVersion) {
        // Generate the Sends for the response fields and records
        ArrayDeque<Send> sends = new ArrayDeque<>();
        RecordsWriter writer = new RecordsWriter(dest, sends::add);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        fetchResponseData.size(cache, apiVersion);
        fetchResponseData.write(writer, cache, apiVersion);
        writer.flush();

        // Compute the total size of all the Sends and write it out along with the header in the first Send
        ResponseHeaderData responseHeaderData = responseHeader.data();

        //Struct responseHeaderStruct = responseHeader.toStruct();
        int headerSize = responseHeaderData.size(cache, responseHeader.headerVersion());
        int bodySize = (int) sends.stream().mapToLong(Send::size).sum();

        ByteBuffer buffer = ByteBuffer.allocate(headerSize + 4);
        ByteBufferAccessor headerWriter = new ByteBufferAccessor(buffer);

        // Write out the size and header
        buffer.putInt(headerSize + bodySize);
        responseHeaderData.write(headerWriter, cache, responseHeader.headerVersion());

        // Rewind the buffer and set this the first Send in the MultiRecordsSend
        buffer.rewind();
        sends.addFirst(new ByteBufferSend(dest, buffer));

        return new MultiRecordsSend(dest, sends);
    }

    public Errors error() {
        return Errors.forCode(fetchResponseData.errorCode());
    }

    public LinkedHashMap<TopicPartition, PartitionData<T>> responseData() {
        return responseDataMap;
    }

    @Override
    public int throttleTimeMs() {
        return fetchResponseData.throttleTimeMs();
    }

    public int sessionId() {
        return fetchResponseData.sessionId();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        responseDataMap.values().forEach(response ->
            updateErrorCounts(errorCounts, response.error)
        );
        return errorCounts;
    }

    public static FetchResponse<MemoryRecords> parse(ByteBuffer buffer, short version) {
        FetchResponseData fetchResponseData = new FetchResponseData();
        RecordsReader reader = new RecordsReader(buffer);
        fetchResponseData.read(reader, version);
        return new FetchResponse<>(fetchResponseData);
    }

    @SuppressWarnings("unchecked")
    private static <T extends BaseRecords> LinkedHashMap<TopicPartition, PartitionData<T>> toResponseDataMap(
            FetchResponseData message) {
        LinkedHashMap<TopicPartition, PartitionData<T>> responseMap = new LinkedHashMap<>();
        message.responses().forEach(topicResponse -> {
            topicResponse.partitionResponses().forEach(partitionResponse -> {
                FetchResponseData.PartitionHeader partitionHeader = partitionResponse.partitionHeader();
                TopicPartition tp = new TopicPartition(topicResponse.topic(), partitionHeader.partition());

                Optional<Integer> preferredReplica = Optional.of(partitionHeader.preferredReadReplica())
                    .filter(replicaId -> replicaId != INVALID_PREFERRED_REPLICA_ID);

                final List<AbortedTransaction> abortedTransactions;
                if (partitionHeader.abortedTransactions() == null) {
                    abortedTransactions = null;
                } else {
                    abortedTransactions = partitionHeader.abortedTransactions().stream()
                            .map(AbortedTransaction::fromMessage)
                            .collect(Collectors.toList());
                }
                PartitionData<T> partitionData = new PartitionData<>(
                    Errors.forCode(partitionHeader.errorCode()),
                      partitionHeader.highWatermark(),
                      partitionHeader.lastStableOffset(),
                      partitionHeader.logStartOffset(),
                    preferredReplica,
                    abortedTransactions,
                    (T)partitionResponse.recordSet()
                );

                responseMap.put(tp, partitionData);
            });
        });
        return responseMap;
    }

    private static <T extends BaseRecords> FetchResponseData toMessage(int throttleTimeMs, Errors error,
                                                                       Iterator<Map.Entry<TopicPartition, PartitionData<T>>> partIterator,
                                                                       int sessionId) {
        FetchResponseData message = new FetchResponseData();
        message.setThrottleTimeMs(throttleTimeMs);
        message.setErrorCode(error.code());
        message.setSessionId(sessionId);

        List<FetchResponseData.FetchableTopicResponse> topicResponseList = new ArrayList<>();
        List<FetchRequest.TopicAndPartitionData<PartitionData<T>>> topicsData =
                FetchRequest.TopicAndPartitionData.batchByTopic(partIterator);
        topicsData.forEach(partitionDataTopicAndPartitionData -> {
            List<FetchResponseData.FetchablePartitionResponse> partitionResponses = new ArrayList<>();
            partitionDataTopicAndPartitionData.partitions.forEach((partitionId, partitionData) -> {
                FetchResponseData.FetchablePartitionResponse partitionResponse =
                    new FetchResponseData.FetchablePartitionResponse();
                FetchResponseData.PartitionHeader partitionHeader = new FetchResponseData.PartitionHeader();
                partitionHeader.setPartition(partitionId)
                        .setErrorCode(partitionData.error.code()).setHighWatermark(partitionData.highWatermark)
                        .setHighWatermark(partitionData.highWatermark)
                        .setLastStableOffset(partitionData.lastStableOffset)
                        .setLogStartOffset(partitionData.logStartOffset);
                if (partitionData.abortedTransactions != null) {
                    partitionHeader.setAbortedTransactions(partitionData.abortedTransactions.stream().map(
                            aborted -> new FetchResponseData.AbortedTransaction()
                                    .setProducerId(aborted.producerId)
                                    .setFirstOffset(aborted.firstOffset))
                            .collect(Collectors.toList()));
                } else {
                    partitionHeader.setAbortedTransactions(null);
                }
                partitionHeader
                    .setPreferredReadReplica(partitionData.preferredReadReplica.orElse(INVALID_PREFERRED_REPLICA_ID));
                partitionResponse.setPartitionHeader(partitionHeader);
                partitionResponse.setRecordSet(partitionData.records);
                partitionResponses.add(partitionResponse);
            });
            topicResponseList.add(new FetchResponseData.FetchableTopicResponse()
                .setTopic(partitionDataTopicAndPartitionData.topic)
                .setPartitionResponses(partitionResponses));
        });

        message.setResponses(topicResponseList);
        return message;
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
        FetchResponseData data = toMessage(0, Errors.NONE, partIterator, INVALID_SESSION_ID);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        return 4 + data.size(cache, version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 8;
    }
}
