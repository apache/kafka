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
import org.apache.kafka.common.record.MemoryRecords;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
public class FetchResponse extends AbstractResponse {
    public static final long INVALID_HIGH_WATERMARK = -1L;
    public static final long INVALID_LAST_STABLE_OFFSET = -1L;
    public static final long INVALID_LOG_START_OFFSET = -1L;
    public static final int INVALID_PREFERRED_REPLICA_ID = -1;

    private final FetchResponseData data;
    private final LinkedHashMap<TopicPartition, FetchResponseData.FetchablePartitionResponse> dataByTopicPartition;

    @Override
    public FetchResponseData data() {
        return data;
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
        this.dataByTopicPartition = new LinkedHashMap<>();
        fetchResponseData.responses().forEach(topicResponse ->
            topicResponse.partitionResponses().forEach(partitionResponse ->
                dataByTopicPartition.put(new TopicPartition(topicResponse.topic(), partitionResponse.partition()), partitionResponse))
        );

    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    public LinkedHashMap<TopicPartition, FetchResponseData.FetchablePartitionResponse> dataByTopicPartition() {
        return dataByTopicPartition;
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
        dataByTopicPartition.values().forEach(response -> updateErrorCounts(errorCounts, Errors.forCode(response.errorCode())));
        return errorCounts;
    }

    public static FetchResponse parse(ByteBuffer buffer, short version) {
        return new FetchResponse(new FetchResponseData(new ByteBufferAccessor(buffer), version));
    }

    /**
     * Convenience method to find the size of a response.
     *
     * @param version       The version of the response to use.
     * @param partIterator  The partition iterator.
     * @return              The response size in bytes.
     */
    public static int sizeOf(short version,
                             Iterator<Map.Entry<TopicPartition, FetchResponseData.FetchablePartitionResponse>> partIterator) {
        // Since the throttleTimeMs and metadata field sizes are constant and fixed, we can
        // use arbitrary values here without affecting the result.
        LinkedHashMap<TopicPartition, FetchResponseData.FetchablePartitionResponse> data = new LinkedHashMap<>();
        partIterator.forEachRemaining(entry -> data.put(entry.getKey(), entry.getValue()));
        ObjectSerializationCache cache = new ObjectSerializationCache();
        return 4 + new FetchResponse(Errors.NONE, 0, INVALID_SESSION_ID, data).data.size(cache, version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 8;
    }

    public static Optional<FetchResponseData.EpochEndOffset> divergingEpoch(FetchResponseData.FetchablePartitionResponse partitionResponse) {
        return partitionResponse.divergingEpoch().epoch() < 0 ? Optional.empty()
                : Optional.of(partitionResponse.divergingEpoch());
    }

    public static Optional<Integer> preferredReadReplica(FetchResponseData.FetchablePartitionResponse partitionResponse) {
        return partitionResponse.preferredReadReplica() == INVALID_PREFERRED_REPLICA_ID ? Optional.empty()
                : Optional.of(partitionResponse.preferredReadReplica());
    }

    public static FetchResponseData.FetchablePartitionResponse partitionResponse(int partition, Errors error) {
        return new FetchResponseData.FetchablePartitionResponse()
                .setPartition(partition)
                .setErrorCode(error.code())
                .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK)
                .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                .setLogStartOffset(FetchResponse.INVALID_LOG_START_OFFSET)
                .setAbortedTransactions(null)
                .setRecordSet(MemoryRecords.EMPTY)
                .setPreferredReadReplica(FetchResponse.INVALID_PREFERRED_REPLICA_ID);
    }
}