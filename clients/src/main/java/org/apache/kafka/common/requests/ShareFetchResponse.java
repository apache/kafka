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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Records;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Possible error codes.
 * - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 * - {@link Errors#NOT_LEADER_OR_FOLLOWER}
 * - {@link Errors#UNKNOWN_TOPIC_ID}
 * - {@link Errors#INVALID_RECORD_STATE}
 * - {@link Errors#KAFKA_STORAGE_ERROR}
 * - {@link Errors#CORRUPT_MESSAGE}
 * - {@link Errors#INVALID_REQUEST}
 * - {@link Errors#UNKNOWN_SERVER_ERROR}
 */
public class ShareFetchResponse extends AbstractResponse {

    private final ShareFetchResponseData data;

    private ShareFetchResponse(ShareFetchResponseData data) {
        super(ApiKeys.SHARE_FETCH);
        this.data = data;
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public ShareFetchResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new EnumMap<>(Errors.class);
        updateErrorCounts(counts, Errors.forCode(data.errorCode()));
        data.responses().forEach(
                topic -> topic.partitions().forEach(
                        partition -> updateErrorCounts(counts, Errors.forCode(partition.errorCode()))
                )
        );
        return counts;
    }

    public LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData(Map<Uuid, String> topicNames) {
        final LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData = new LinkedHashMap<>();
        data.responses().forEach(topicResponse -> {
            String name = topicNames.get(topicResponse.topicId());
            if (name != null) {
                topicResponse.partitions().forEach(partitionData -> responseData.put(new TopicIdPartition(topicResponse.topicId(),
                        new TopicPartition(name, partitionData.partitionIndex())), partitionData));
            }
        });
        return responseData;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    /**
     * Creates a {@link org.apache.kafka.common.requests.ShareFetchResponse} from the given byte buffer.
     * Unlike {@link org.apache.kafka.common.requests.ShareFetchResponse#of(Errors, int, LinkedHashMap, List, int)},
     * this method doesn't convert null records to {@link org.apache.kafka.common.record.internal.MemoryRecords#EMPTY}.
     *
     * <p><strong>This method should only be used in client-side.</strong></p>
     */
    public static ShareFetchResponse parse(Readable readable, short version) {
        return new ShareFetchResponse(
                new ShareFetchResponseData(readable, version)
        );
    }

    /**
     * Returns `partition.records` as `Records` (instead of `BaseRecords`). If `records` is `null`, returns `MemoryRecords.EMPTY`.
     *
     * @param partition partition data
     * @return Records or empty record if the records in PartitionData is null.
     */
    public static Records recordsOrFail(ShareFetchResponseData.PartitionData partition) {
        if (partition.records() == null) return MemoryRecords.EMPTY;
        if (partition.records() instanceof Records) return (Records) partition.records();
        throw new ClassCastException("The record type is " + partition.records().getClass().getSimpleName() + ", which is not a subtype of " +
                Records.class.getSimpleName() + ". This method is only safe to call if the `ShareFetchResponse` was deserialized from bytes.");
    }

    /**
     * Convenience method to find the size of a response.
     *
     * @param version       The version of the request
     * @param partIterator  The partition iterator.
     * @return              The response size in bytes.
     */
    public static int sizeOf(short version,
                             Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> partIterator) {
        // Since the throttleTimeMs and metadata field sizes are constant and fixed, we can
        // use arbitrary values here without affecting the result.
        ShareFetchResponseData data = toMessage(Errors.NONE, 0, partIterator, Collections.emptyList(), 0);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        return 4 + data.size(cache, version);
    }

    /**
     * @return The size in bytes of the records. 0 is returned if records of input partition is null.
     */
    public static int recordsSize(ShareFetchResponseData.PartitionData partition) {
        return partition.records() == null ? 0 : partition.records().sizeInBytes();
    }

    /**
     * Creates a {@link org.apache.kafka.common.requests.ShareFetchResponse} from the given data.
     * This method converts null records to {@link org.apache.kafka.common.record.internal.MemoryRecords#EMPTY}
     * to ensure consistent record representation in the response.
     *
     * <p><strong>This method should only be used in server-side.</strong></p>
     */
    public static ShareFetchResponse of(Errors error,
                                        int throttleTimeMs,
                                        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData,
                                        List<Node> nodeEndpoints, int acquisitionLockTimeout) {
        return new ShareFetchResponse(toMessage(error, throttleTimeMs, responseData.entrySet().iterator(), nodeEndpoints, acquisitionLockTimeout));
    }

    private static ShareFetchResponseData toMessage(Errors error, int throttleTimeMs,
                                                   Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> partIterator,
                                                   List<Node> nodeEndpoints, int acquisitionLockTimeout) {
        ShareFetchResponseData.ShareFetchableTopicResponseCollection topicResponses = new ShareFetchResponseData.ShareFetchableTopicResponseCollection();
        while (partIterator.hasNext()) {
            Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> entry = partIterator.next();
            ShareFetchResponseData.PartitionData partitionData = entry.getValue();
            // Since PartitionData alone doesn't know the partition ID, we set it here
            partitionData.setPartitionIndex(entry.getKey().topicPartition().partition());
            // To protect the clients from failing due to null records,
            // we always convert null records to MemoryRecords.EMPTY
            // We will propose a KIP to change the schema definitions in the future
            if (partitionData.records() == null)
                partitionData.setRecords(MemoryRecords.EMPTY);
            // Checking if the topic is already present in the map
            ShareFetchResponseData.ShareFetchableTopicResponse topicResponse = topicResponses.find(entry.getKey().topicId());
            if (topicResponse == null) {
                topicResponse = new ShareFetchResponseData.ShareFetchableTopicResponse()
                        .setTopicId(entry.getKey().topicId())
                        .setPartitions(new ArrayList<>());
                topicResponses.add(topicResponse);
            }
            topicResponse.partitions().add(partitionData);
        }
        ShareFetchResponseData data = new ShareFetchResponseData();
        // KafkaApis should only pass in node endpoints on error, otherwise this should be an empty list
        nodeEndpoints.forEach(endpoint -> data.nodeEndpoints().add(
                new ShareFetchResponseData.NodeEndpoint()
                        .setNodeId(endpoint.id())
                        .setHost(endpoint.host())
                        .setPort(endpoint.port())
                        .setRack(endpoint.rack())));
        return data.setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code())
                .setAcquisitionLockTimeoutMs(acquisitionLockTimeout)
                .setResponses(topicResponses);
    }

    public static ShareFetchResponseData.PartitionData partitionResponse(TopicIdPartition topicIdPartition, Errors error) {
        return partitionResponse(topicIdPartition.topicPartition().partition(), error);
    }

    private static ShareFetchResponseData.PartitionData partitionResponse(int partition, Errors error) {
        return new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(partition)
                .setErrorCode(error.code())
                .setRecords(MemoryRecords.EMPTY);
    }
}
