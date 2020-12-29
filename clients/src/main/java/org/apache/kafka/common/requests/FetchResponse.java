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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

/**
 * This wrapper supports all versions of the Fetch API
 *
 * Possible error codes:
 *
 * - {@link Errors#OFFSET_OUT_OF_RANGE} If the fetch offset is out of range for a requested partition
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED} If the user does not have READ access to a requested topic
 * - {@link Errors#REPLICA_NOT_AVAILABLE} If the request is received by a broker with version < 2.6 which is not a replica
 * - {@link Errors#NOT_LEADER_OR_FOLLOWER} If the broker is not a leader or follower and either the provided leader epoch
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

    public static FetchResponseData.FetchablePartitionResponse partitionResponse(Errors error) {
        return new FetchResponseData.FetchablePartitionResponse()
                .setErrorCode(error.code())
                .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK)
                .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                .setLogStartOffset(FetchResponse.INVALID_LOG_START_OFFSET)
                .setAbortedTransactions(null)
                .setRecordSet(MemoryRecords.EMPTY)
                .setPreferredReadReplica(FetchResponse.INVALID_PREFERRED_REPLICA_ID);
    }

    public static final long INVALID_HIGH_WATERMARK = -1L;
    public static final long INVALID_LAST_STABLE_OFFSET = -1L;
    public static final long INVALID_LOG_START_OFFSET = -1L;
    public static final int INVALID_PREFERRED_REPLICA_ID = -1;

    private final FetchResponseData data;
    private final LinkedHashMap<TopicPartition, PartitionData<T>> responseDataMap;

    @Override
    public FetchResponseData data() {
        return data;
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
        this(error, throttleTimeMs, sessionId, responseData.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
            entry -> entry.getValue().partitionResponse, (o1, o2) -> {
                throw new RuntimeException("this is impossible");
            }, LinkedHashMap::new)));
    }

    public FetchResponse(Errors error,
                         int throttleTimeMs,
                         int sessionId,
                         LinkedHashMap<TopicPartition, FetchResponseData.FetchablePartitionResponse> responseData) {
        this(new FetchResponseData()
                .setSessionId(sessionId)
                .setErrorCode(error.code())
                .setThrottleTimeMs(throttleTimeMs)
                .setResponses(responseData.entrySet().stream().map(entry -> new FetchResponseData.FetchableTopicResponse()
                        .setTopic(entry.getKey().topic())
                        .setPartitionResponses(Collections.singletonList(entry.getValue().setPartition(entry.getKey().partition()))))
                        .collect(Collectors.toList())));
    }

    public FetchResponse(FetchResponseData fetchResponseData) {
        super(ApiKeys.FETCH);
        this.data = fetchResponseData;
        this.responseDataMap = new LinkedHashMap<>();
        fetchResponseData.responses().forEach(topicResponse ->
                topicResponse.partitionResponses().forEach(partitionResponse ->
                        responseDataMap.put(new TopicPartition(topicResponse.topic(), partitionResponse.partition()), new PartitionData<>(partitionResponse)))
        );
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    public LinkedHashMap<TopicPartition, PartitionData<T>> responseData() {
        return responseDataMap;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public int sessionId() {
        return data.sessionId();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        updateErrorCounts(errorCounts, error());
        responseDataMap.values().forEach(response -> updateErrorCounts(errorCounts, response.error()));
        return errorCounts;
    }

    public static FetchResponse<MemoryRecords> parse(ByteBuffer buffer, short version) {
        return new FetchResponse<>(new FetchResponseData(new ByteBufferAccessor(buffer), version));
    }

    /**
     * Convenience method to find the size of a response.
     *
     * @param version       The version of the response to use.
     * @param partIterator  The partition iterator.
     * @return              The response size in bytes.
     */
    public static <T extends Records> int sizeOf(short version,
                                                 Iterator<Map.Entry<TopicPartition, PartitionData<T>>> partIterator) {
        // Since the throttleTimeMs and metadata field sizes are constant and fixed, we can
        // use arbitrary values here without affecting the result.
        LinkedHashMap<TopicPartition, PartitionData<T>> data = new LinkedHashMap<>();
        partIterator.forEachRemaining(entry -> data.put(entry.getKey(), entry.getValue()));
        ObjectSerializationCache cache = new ObjectSerializationCache();
        return 4 + new FetchResponse<>(Errors.NONE, data, 0, INVALID_SESSION_ID).data.size(cache, version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 8;
    }


    public static final class PartitionData<T extends BaseRecords> {

        private final FetchResponseData.FetchablePartitionResponse partitionResponse;

        public PartitionData(FetchResponseData.FetchablePartitionResponse partitionResponse) {
            this.partitionResponse = partitionResponse;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PartitionData that = (PartitionData) o;

            return this.partitionResponse.equals(that.partitionResponse);
        }

        @Override
        public int hashCode() {
            return this.partitionResponse.hashCode();
        }

        @Override
        public String toString() {
            return "(error=" + error() +
                    ", highWaterMark=" + highWatermark() +
                    ", lastStableOffset = " + lastStableOffset() +
                    ", logStartOffset = " + logStartOffset() +
                    ", preferredReadReplica = " + preferredReadReplica().map(Object::toString).orElse("absent") +
                    ", abortedTransactions = " + abortedTransactions() +
                    ", divergingEpoch =" + divergingEpoch().map(Object::toString).orElse("absent") +
                    ", recordsSizeInBytes=" + records().sizeInBytes() + ")";
        }

        public Errors error() {
            return Errors.forCode(partitionResponse.errorCode());
        }

        public long highWatermark() {
            return partitionResponse.highWatermark();
        }

        public long lastStableOffset() {
            return partitionResponse.lastStableOffset();
        }

        public long logStartOffset() {
            return partitionResponse.logStartOffset();
        }

        public Optional<Integer> preferredReadReplica() {
            return partitionResponse.preferredReadReplica() == INVALID_PREFERRED_REPLICA_ID ? Optional.empty()
                    : Optional.of(partitionResponse.preferredReadReplica());
        }

        public List<FetchResponseData.AbortedTransaction> abortedTransactions() {
            return partitionResponse.abortedTransactions();
        }

        public Optional<FetchResponseData.EpochEndOffset> divergingEpoch() {
            return partitionResponse.divergingEpoch().epoch() < 0 ? Optional.empty()
                    : Optional.of(partitionResponse.divergingEpoch());
        }

        @SuppressWarnings("unchecked")
        public T records() {
            return (T) partitionResponse.recordSet();
        }
    }
}