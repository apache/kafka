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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Convenience class for making asynchronous requests to the ListOffsets API
 */
public class ListOffsetsClient extends AsyncClient<
        ListOffsetsClient.RequestData,
        ListOffsetRequest,
        ListOffsetResponse,
        ListOffsetsClient.ResultData> {


    public ListOffsetsClient(ConsumerNetworkClient client, LogContext logContext) {
        super(client, logContext);
    }

    @Override
    protected AbstractRequest.Builder<ListOffsetRequest> prepareRequest(Node node, RequestData requestData) {
        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
                .forConsumer(requestData.requireTimestamp, requestData.isolationLevel)
                .setTargetTimes(requestData.timestampsToSearch);
        return builder;
    }

    /**
     * Callback for the response of list offset.
     * @param requestData The request data including the mapping from partitions to target timestamps
     * @param response The response from the server.
     * @return The result of the request represented by a {@link ResultData} object. Note that any partition-level
     *         errors will fail the entire response. The one exception is UNSUPPORTED_FOR_MESSAGE_FORMAT, which
     *         indicates that the broker does not support the v1 message format. Partitions with this particular error
     *         are simply left out of the result map. Note that the corresponding timestamp value of each partition may
     *         be null only for v0. In v1 and later the ListOffset API would not return a null timestamp (-1 is returned
     *         instead when necessary).
     */
    @Override
    protected ResultData handleResponse(Node node, RequestData requestData, ListOffsetResponse response) {

        // Handle the response, translate to something more usable
        Map<TopicPartition, ListOffsetData> fetchedOffsets = new HashMap<>();
        Set<TopicPartition> partitionsToRetry = new HashSet<>();
        Set<String> unauthorizedTopics = new HashSet<>();

        Logger log = logger();

        for (Map.Entry<TopicPartition, ListOffsetRequest.PartitionData> entry : requestData.timestampsToSearch.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            ListOffsetResponse.PartitionData partitionData = response.responseData().get(topicPartition);
            if (partitionData == null) {
                logger().warn("Missing partition {} from response, ignoring", topicPartition);
                partitionsToRetry.add(topicPartition);
                continue;
            }
            Errors error = partitionData.error;
            if (error == Errors.NONE) {
                handleOkPartitionResponse(topicPartition, partitionData, fetchedOffsets);
            } else if (error == Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT) {
                // The message format on the broker side is before 0.10.0, which means it does not
                // support timestamps. We treat this case the same as if we weren't able to find an
                // offset corresponding to the requested timestamp and leave it out of the result.
                log.debug("Cannot search by timestamp for partition {} because the message format version " +
                        "is before 0.10.0", topicPartition);
            } else if (error == Errors.NOT_LEADER_FOR_PARTITION ||
                    error == Errors.REPLICA_NOT_AVAILABLE ||
                    error == Errors.KAFKA_STORAGE_ERROR ||
                    error == Errors.OFFSET_NOT_AVAILABLE ||
                    error == Errors.LEADER_NOT_AVAILABLE) {
                log.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                        topicPartition, error);
                partitionsToRetry.add(topicPartition);
            } else if (error == Errors.FENCED_LEADER_EPOCH ||
                    error == Errors.UNKNOWN_LEADER_EPOCH) {
                log.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                        topicPartition, error);
                partitionsToRetry.add(topicPartition);
            } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                log.warn("Received unknown topic or partition error in ListOffset request for partition {}", topicPartition);
                partitionsToRetry.add(topicPartition);
            } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                unauthorizedTopics.add(topicPartition.topic());
            } else {
                log.warn("Attempt to fetch offsets for partition {} failed due to: {}, retrying.", topicPartition, error.message());
                partitionsToRetry.add(topicPartition);
            }
        }

        if (!unauthorizedTopics.isEmpty())
            throw new TopicAuthorizationException(unauthorizedTopics);
        else
            return new ResultData(fetchedOffsets, partitionsToRetry);
    }

    @SuppressWarnings("deprecation")
    private void handleOkPartitionResponse(TopicPartition topicPartition,
                            ListOffsetResponse.PartitionData partitionData,
                            Map<TopicPartition, ListOffsetData> fetchedOffsets) {
        Logger log = logger();
        if (partitionData.offsets != null) {
            // Handle v0 response
            long offset;
            if (partitionData.offsets.size() > 1) {
                throw new IllegalStateException("Unexpected partitionData response of length " +
                        partitionData.offsets.size());
            } else if (partitionData.offsets.isEmpty()) {
                offset = ListOffsetResponse.UNKNOWN_OFFSET;
            } else {
                offset = partitionData.offsets.get(0);
            }
            log.debug("Handling v0 ListOffsetResponse response for {}. Fetched offset {}",
                    topicPartition, offset);
            if (offset != ListOffsetResponse.UNKNOWN_OFFSET) {
                ListOffsetData offsetData = new ListOffsetData(offset, null, Optional.empty());
                fetchedOffsets.put(topicPartition, offsetData);
            }
        } else {
            // Handle v1 and later response
            log.debug("Handling ListOffsetResponse response for {}. Fetched offset {}, timestamp {}",
                    topicPartition, partitionData.offset, partitionData.timestamp);
            if (partitionData.offset != ListOffsetResponse.UNKNOWN_OFFSET) {
                ListOffsetData offsetData = new ListOffsetData(partitionData.offset, partitionData.timestamp,
                        partitionData.leaderEpoch);
                fetchedOffsets.put(topicPartition, offsetData);
            }
        }
    }

    static class RequestData {
        final Map<TopicPartition, ListOffsetRequest.PartitionData> timestampsToSearch;
        final boolean requireTimestamp;
        final IsolationLevel isolationLevel;

        /**
         * @param timestampsToSearch The mapping from partitions to the target timestamps.
         * @param requireTimestamp  True if we require a timestamp in the response.
         * @param isolationLevel The isolation level of the consumer making this request.
         * @return A response which can be polled to obtain the corresponding timestamps and offsets.
         */
        RequestData(Map<TopicPartition, ListOffsetRequest.PartitionData> timestampsToSearch, boolean requireTimestamp,
                    IsolationLevel isolationLevel) {
            this.timestampsToSearch = timestampsToSearch;
            this.requireTimestamp = requireTimestamp;
            this.isolationLevel = isolationLevel;
        }
    }

    static class ResultData {
        final Map<TopicPartition, ListOffsetData> fetchedOffsets;
        final Set<TopicPartition> partitionsToRetry;

        public ResultData(Map<TopicPartition, ListOffsetData> fetchedOffsets, Set<TopicPartition> partitionsNeedingRetry) {
            this.fetchedOffsets = fetchedOffsets;
            this.partitionsToRetry = partitionsNeedingRetry;
        }

        public ResultData() {
            this.fetchedOffsets = new HashMap<>();
            this.partitionsToRetry = new HashSet<>();
        }
    }

    static class ListOffsetData {
        final long offset;
        final Long timestamp; //  null if the broker does not support returning timestamps
        final Optional<Integer> leaderEpoch; // empty if the leader epoch is not known

        ListOffsetData(long offset, Long timestamp, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.leaderEpoch = leaderEpoch;
        }
    }
}
