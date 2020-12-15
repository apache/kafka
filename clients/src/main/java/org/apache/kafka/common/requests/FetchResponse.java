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

import java.nio.ByteBuffer;
import java.util.ArrayList;
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

    public static final long INVALID_HIGHWATERMARK = -1L;
    public static final long INVALID_LAST_STABLE_OFFSET = -1L;
    public static final long INVALID_LOG_START_OFFSET = -1L;
    public static final int INVALID_PREFERRED_REPLICA_ID = -1;

    private final FetchResponseData data;
    private final LinkedHashMap<TopicPartition, PartitionData<T>> responseDataMap;

    @Override
    public FetchResponseData data() {
        return data;
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
        private final FetchResponseData.FetchablePartitionResponse partitionResponse;

        // Derived fields
        private final Optional<Integer> preferredReplica;
        private final List<AbortedTransaction> abortedTransactions;
        private final Errors error;

        private PartitionData(FetchResponseData.FetchablePartitionResponse partitionResponse) {
            // We partially construct FetchablePartitionResponse since we don't know the partition ID at this point
            // When we convert the PartitionData (and other fields) into FetchResponseData down in toMessage, we
            // set the partition IDs.
            this.partitionResponse = partitionResponse;
            this.preferredReplica = Optional.of(partitionResponse.preferredReadReplica())
                .filter(replicaId -> replicaId != INVALID_PREFERRED_REPLICA_ID);

            if (partitionResponse.abortedTransactions() == null) {
                this.abortedTransactions = null;
            } else {
                this.abortedTransactions = partitionResponse.abortedTransactions().stream()
                    .map(AbortedTransaction::fromMessage)
                    .collect(Collectors.toList());
            }

            this.error = Errors.forCode(partitionResponse.errorCode());
        }

        public PartitionData(Errors error,
                             long highWatermark,
                             long lastStableOffset,
                             long logStartOffset,
                             Optional<Integer> preferredReadReplica,
                             List<AbortedTransaction> abortedTransactions,
                             Optional<FetchResponseData.EpochEndOffset> divergingEpoch,
                             T records) {
            this.preferredReplica = preferredReadReplica;
            this.abortedTransactions = abortedTransactions;
            this.error = error;

            FetchResponseData.FetchablePartitionResponse partitionResponse =
                new FetchResponseData.FetchablePartitionResponse();
            partitionResponse.setErrorCode(error.code())
                .setHighWatermark(highWatermark)
                .setLastStableOffset(lastStableOffset)
                .setLogStartOffset(logStartOffset);
            if (abortedTransactions != null) {
                partitionResponse.setAbortedTransactions(abortedTransactions.stream().map(
                    aborted -> new FetchResponseData.AbortedTransaction()
                        .setProducerId(aborted.producerId)
                        .setFirstOffset(aborted.firstOffset))
                    .collect(Collectors.toList()));
            } else {
                partitionResponse.setAbortedTransactions(null);
            }
            partitionResponse.setPreferredReadReplica(preferredReadReplica.orElse(INVALID_PREFERRED_REPLICA_ID));
            partitionResponse.setRecordSet(records);
            divergingEpoch.ifPresent(partitionResponse::setDivergingEpoch);

            this.partitionResponse = partitionResponse;
        }

        public PartitionData(Errors error,
                             long highWatermark,
                             long lastStableOffset,
                             long logStartOffset,
                             Optional<Integer> preferredReadReplica,
                             List<AbortedTransaction> abortedTransactions,
                             T records) {
            this(error, highWatermark, lastStableOffset, logStartOffset, preferredReadReplica,
                abortedTransactions, Optional.empty(), records);
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
                    ", divergingEpoch =" + divergingEpoch() +
                    ", recordsSizeInBytes=" + records().sizeInBytes() + ")";
        }

        public Errors error() {
            return error;
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
            return preferredReplica;
        }

        public List<AbortedTransaction> abortedTransactions() {
            return abortedTransactions;
        }

        public Optional<FetchResponseData.EpochEndOffset> divergingEpoch() {
            FetchResponseData.EpochEndOffset epochEndOffset = partitionResponse.divergingEpoch();
            if (epochEndOffset.epoch() < 0) {
                return Optional.empty();
            } else {
                return Optional.of(epochEndOffset);
            }
        }

        @SuppressWarnings("unchecked")
        public T records() {
            return (T) partitionResponse.recordSet();
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
        super(ApiKeys.FETCH);
        this.data = toMessage(throttleTimeMs, error, responseData.entrySet().iterator(), sessionId);
        this.responseDataMap = responseData;
    }

    public FetchResponse(FetchResponseData fetchResponseData) {
        super(ApiKeys.FETCH);
        this.data = fetchResponseData;
        this.responseDataMap = toResponseDataMap(fetchResponseData);
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
        responseDataMap.values().forEach(response ->
            updateErrorCounts(errorCounts, response.error())
        );
        return errorCounts;
    }

    public static FetchResponse<MemoryRecords> parse(ByteBuffer buffer, short version) {
        return new FetchResponse<>(new FetchResponseData(new ByteBufferAccessor(buffer), version));
    }

    @SuppressWarnings("unchecked")
    private static <T extends BaseRecords> LinkedHashMap<TopicPartition, PartitionData<T>> toResponseDataMap(
            FetchResponseData message) {
        LinkedHashMap<TopicPartition, PartitionData<T>> responseMap = new LinkedHashMap<>();
        message.responses().forEach(topicResponse -> {
            topicResponse.partitionResponses().forEach(partitionResponse -> {
                TopicPartition tp = new TopicPartition(topicResponse.topic(), partitionResponse.partition());
                PartitionData<T> partitionData = new PartitionData<>(partitionResponse);
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
                // Since PartitionData alone doesn't know the partition ID, we set it here
                partitionData.partitionResponse.setPartition(partitionId);
                partitionResponses.add(partitionData.partitionResponse);
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
