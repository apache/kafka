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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
 * - {@link Errors#UNKNOWN_TOPIC_ID} If the request contains a topic ID unknown to the broker
 * - {@link Errors#FETCH_SESSION_TOPIC_ID_ERROR} If the request version supports topic IDs but the session does not or vice versa,
 *     or a topic ID in the request is inconsistent with a topic ID in the session
 * - {@link Errors#INCONSISTENT_TOPIC_ID} If a topic ID in the session does not match the topic ID in the log
 * - {@link Errors#UNKNOWN_SERVER_ERROR} For any unexpected errors
 */
public class FetchResponse extends AbstractResponse {
    public static final long INVALID_HIGH_WATERMARK = -1L;
    public static final long INVALID_LAST_STABLE_OFFSET = -1L;
    public static final long INVALID_LOG_START_OFFSET = -1L;
    public static final int INVALID_PREFERRED_REPLICA_ID = -1;

    private final FetchResponseData data;
    // we build responseData when needed.
    private volatile LinkedHashMap<TopicPartition, FetchResponseData.PartitionData> responseData = null;

    @Override
    public FetchResponseData data() {
        return data;
    }

    /**
     * From version 3 or later, the authorized and existing entries in `FetchRequest.fetchData` should be in the same order in `responseData`.
     * Version 13 introduces topic IDs which can lead to a few new errors. If there is any unknown topic ID in the request, the
     * response will contain a partition-level UNKNOWN_TOPIC_ID error for that partition.
     * If a request's topic ID usage is inconsistent with the session, we will return a top level FETCH_SESSION_TOPIC_ID_ERROR error.
     * We may also return INCONSISTENT_TOPIC_ID error as a partition-level error when a partition in the session has a topic ID
     * inconsistent with the log.
     */
    public FetchResponse(FetchResponseData fetchResponseData) {
        super(ApiKeys.FETCH);
        this.data = fetchResponseData;
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    public LinkedHashMap<TopicPartition, FetchResponseData.PartitionData> responseData(Map<Uuid, String> topicNames, short version) {
        if (responseData == null) {
            synchronized (this) {
                if (responseData == null) {
                    responseData = new LinkedHashMap<>();
                    data.responses().forEach(topicResponse -> {
                        String name;
                        if (version < 13) {
                            name = topicResponse.topic();
                        } else {
                            name = topicNames.get(topicResponse.topicId());
                        }
                        if (name != null) {
                            topicResponse.partitions().forEach(partition ->
                                responseData.put(new TopicPartition(name, partition.partitionIndex()), partition));
                        }
                    });
                }
            }
        }
        return responseData;
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
        data.responses().forEach(topicResponse ->
            topicResponse.partitions().forEach(partition ->
                updateErrorCounts(errorCounts, Errors.forCode(partition.errorCode())))
        );
        return errorCounts;
    }

    public static FetchResponse parse(ByteBuffer buffer, short version) {
        return new FetchResponse(new FetchResponseData(new ByteBufferAccessor(buffer), version));
    }

    // Fetch versions 13 and above should have topic IDs for all topics.
    // Fetch versions < 13 should return the empty set.
    public Set<Uuid> topicIds() {
        return data.responses().stream().map(FetchResponseData.FetchableTopicResponse::topicId).filter(id -> !id.equals(Uuid.ZERO_UUID)).collect(Collectors.toSet());
    }

    /**
     * Convenience method to find the size of a response.
     *
     * @param version       The version of the response to use.
     * @param partIterator  The partition iterator.
     * @return              The response size in bytes.
     */
    public static int sizeOf(short version,
                             Iterator<Map.Entry<TopicIdPartition,
                             FetchResponseData.PartitionData>> partIterator) {
        // Since the throttleTimeMs and metadata field sizes are constant and fixed, we can
        // use arbitrary values here without affecting the result.
        FetchResponseData data = toMessage(Errors.NONE, 0, INVALID_SESSION_ID, partIterator);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        return 4 + data.size(cache, version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 8;
    }

    public static Optional<FetchResponseData.EpochEndOffset> divergingEpoch(FetchResponseData.PartitionData partitionResponse) {
        return partitionResponse.divergingEpoch().epoch() < 0 ? Optional.empty()
                : Optional.of(partitionResponse.divergingEpoch());
    }

    public static boolean isDivergingEpoch(FetchResponseData.PartitionData partitionResponse) {
        return partitionResponse.divergingEpoch().epoch() >= 0;
    }

    public static Optional<Integer> preferredReadReplica(FetchResponseData.PartitionData partitionResponse) {
        return partitionResponse.preferredReadReplica() == INVALID_PREFERRED_REPLICA_ID ? Optional.empty()
                : Optional.of(partitionResponse.preferredReadReplica());
    }

    public static boolean isPreferredReplica(FetchResponseData.PartitionData partitionResponse) {
        return partitionResponse.preferredReadReplica() != INVALID_PREFERRED_REPLICA_ID;
    }

    public static FetchResponseData.PartitionData partitionResponse(TopicIdPartition topicIdPartition, Errors error) {
        return partitionResponse(topicIdPartition.topicPartition().partition(), error);
    }

    public static FetchResponseData.PartitionData partitionResponse(int partition, Errors error) {
        return new FetchResponseData.PartitionData()
            .setPartitionIndex(partition)
            .setErrorCode(error.code())
            .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK);
    }

    /**
     * Returns `partition.records` as `Records` (instead of `BaseRecords`). If `records` is `null`, returns `MemoryRecords.EMPTY`.
     *
     * If this response was deserialized after a fetch, this method should never fail. An example where this would
     * fail is a down-converted response (e.g. LazyDownConversionRecords) on the broker (before it's serialized and
     * sent on the wire).
     *
     * @param partition partition data
     * @return Records or empty record if the records in PartitionData is null.
     */
    public static Records recordsOrFail(FetchResponseData.PartitionData partition) {
        if (partition.records() == null) return MemoryRecords.EMPTY;
        if (partition.records() instanceof Records) return (Records) partition.records();
        throw new ClassCastException("The record type is " + partition.records().getClass().getSimpleName() + ", which is not a subtype of " +
            Records.class.getSimpleName() + ". This method is only safe to call if the `FetchResponse` was deserialized from bytes.");
    }

    /**
     * @return The size in bytes of the records. 0 is returned if records of input partition is null.
     */
    public static int recordsSize(FetchResponseData.PartitionData partition) {
        return partition.records() == null ? 0 : partition.records().sizeInBytes();
    }

    // TODO: remove as a part of KAFKA-12410
    public static FetchResponse of(Errors error,
                                   int throttleTimeMs,
                                   int sessionId,
                                   LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> responseData) {
        return new FetchResponse(toMessage(error, throttleTimeMs, sessionId, responseData.entrySet().iterator()));
    }

    private static boolean matchingTopic(FetchResponseData.FetchableTopicResponse previousTopic, TopicIdPartition currentTopic) {
        if (previousTopic == null)
            return false;
        if (!previousTopic.topicId().equals(Uuid.ZERO_UUID))
            return previousTopic.topicId().equals(currentTopic.topicId());
        else
            return previousTopic.topic().equals(currentTopic.topicPartition().topic());

    }

    private static FetchResponseData toMessage(Errors error,
                                               int throttleTimeMs,
                                               int sessionId,
                                               Iterator<Map.Entry<TopicIdPartition, FetchResponseData.PartitionData>> partIterator) {
        List<FetchResponseData.FetchableTopicResponse> topicResponseList = new ArrayList<>();
        while (partIterator.hasNext()) {
            Map.Entry<TopicIdPartition, FetchResponseData.PartitionData> entry = partIterator.next();
            FetchResponseData.PartitionData partitionData = entry.getValue();
            // Since PartitionData alone doesn't know the partition ID, we set it here
            partitionData.setPartitionIndex(entry.getKey().topicPartition().partition());
            // We have to keep the order of input topic-partition. Hence, we batch the partitions only if the last
            // batch is in the same topic group.
            FetchResponseData.FetchableTopicResponse previousTopic = topicResponseList.isEmpty() ? null
                : topicResponseList.get(topicResponseList.size() - 1);
            if (matchingTopic(previousTopic, entry.getKey()))
                previousTopic.partitions().add(partitionData);
            else {
                List<FetchResponseData.PartitionData> partitionResponses = new ArrayList<>();
                partitionResponses.add(partitionData);
                topicResponseList.add(new FetchResponseData.FetchableTopicResponse()
                    .setTopic(entry.getKey().topicPartition().topic())
                    .setTopicId(entry.getKey().topicId())
                    .setPartitions(partitionResponses));
            }
        }

        return new FetchResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code())
            .setSessionId(sessionId)
            .setResponses(topicResponseList);
    }
}