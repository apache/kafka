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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class ListOffsetsHandler extends Batched<TopicPartition, ListOffsetsResultInfo> {

    private final Map<TopicPartition, Long> offsetTimestampsByPartition;
    private final ListOffsetsOptions options;
    private final Logger log;
    private final AdminApiLookupStrategy<TopicPartition> lookupStrategy;
    private final int defaultApiTimeoutMs;

    public ListOffsetsHandler(
        Map<TopicPartition, Long> offsetTimestampsByPartition,
        ListOffsetsOptions options,
        LogContext logContext,
        int defaultApiTimeoutMs
    ) {
        this.offsetTimestampsByPartition = offsetTimestampsByPartition;
        this.options = options;
        this.log = logContext.logger(ListOffsetsHandler.class);
        this.lookupStrategy = new PartitionLeaderStrategy(logContext, false);
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
    }

    @Override
    public String apiName() {
        return "listOffsets";
    }

    @Override
    public AdminApiLookupStrategy<TopicPartition> lookupStrategy() {
        return this.lookupStrategy;
    }

    @Override
    ListOffsetsRequest.Builder buildBatchedRequest(int brokerId, Set<TopicPartition> keys) {
        Map<String, ListOffsetsTopic> topicsByName = CollectionUtils.groupPartitionsByTopic(
            keys,
            topicName -> new ListOffsetsTopic().setName(topicName),
            (listOffsetsTopic, partitionId) -> {
                TopicPartition topicPartition = new TopicPartition(listOffsetsTopic.name(), partitionId);
                long offsetTimestamp = offsetTimestampsByPartition.get(topicPartition);
                listOffsetsTopic.partitions().add(
                    new ListOffsetsPartition()
                        .setPartitionIndex(partitionId)
                        .setTimestamp(offsetTimestamp));
            });
        boolean supportsMaxTimestamp = keys
            .stream()
            .anyMatch(key -> offsetTimestampsByPartition.get(key) == ListOffsetsRequest.MAX_TIMESTAMP);

        boolean requireEarliestLocalTimestamp = keys
                .stream()
                .anyMatch(key -> offsetTimestampsByPartition.get(key) == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP);

        boolean requireTieredStorageTimestamp = keys
            .stream()
            .anyMatch(key -> offsetTimestampsByPartition.get(key) == ListOffsetsRequest.LATEST_TIERED_TIMESTAMP);

        boolean requireEarliestPendingUploadTimestamp = keys
            .stream()
            .anyMatch(key -> offsetTimestampsByPartition.get(key) == ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP);

        int timeoutMs = options.timeoutMs() != null ? options.timeoutMs() : defaultApiTimeoutMs;
        return ListOffsetsRequest.Builder.forConsumer(true,
                        options.isolationLevel(),
                        supportsMaxTimestamp,
                        requireEarliestLocalTimestamp,
                        requireTieredStorageTimestamp,
                        requireEarliestPendingUploadTimestamp)
                .setTargetTimes(new ArrayList<>(topicsByName.values()))
                .setTimeoutMs(timeoutMs);
    }

    @Override
    public ApiResult<TopicPartition, ListOffsetsResultInfo> handleResponse(
        Node broker,
        Set<TopicPartition> keys,
        AbstractResponse abstractResponse
    ) {
        ListOffsetsResponse response = (ListOffsetsResponse) abstractResponse;
        Map<TopicPartition, ListOffsetsResultInfo> completed = new HashMap<>();
        Map<TopicPartition, Throwable> failed = new HashMap<>();
        List<TopicPartition> unmapped = new ArrayList<>();
        Set<TopicPartition> retriable = new HashSet<>();

        for (ListOffsetsTopicResponse topic : response.topics()) {
            for (ListOffsetsPartitionResponse partition : topic.partitions()) {
                TopicPartition topicPartition = new TopicPartition(topic.name(), partition.partitionIndex());
                Errors error = Errors.forCode(partition.errorCode());
                if (!offsetTimestampsByPartition.containsKey(topicPartition)) {
                    log.warn("ListOffsets response includes unknown topic partition {}", topicPartition);
                } else if (error == Errors.NONE) {
                    Optional<Integer> leaderEpoch = (partition.leaderEpoch() == ListOffsetsResponse.UNKNOWN_EPOCH)
                        ? Optional.empty()
                        : Optional.of(partition.leaderEpoch());
                    completed.put(
                        topicPartition,
                        new ListOffsetsResultInfo(partition.offset(), partition.timestamp(), leaderEpoch));
                } else {
                    handlePartitionError(topicPartition, error, failed, unmapped, retriable);
                }
            }
        }

        // Sanity-check if the current leader for these partitions returned results for all of them
        for (TopicPartition topicPartition : keys) {
            if (unmapped.isEmpty()
                && !completed.containsKey(topicPartition)
                && !failed.containsKey(topicPartition)
                && !retriable.contains(topicPartition)
            ) {
                ApiException sanityCheckException = new ApiException(
                    "The response from broker " + broker.id() +
                        " did not contain a result for topic partition " + topicPartition);
                log.error(
                    "ListOffsets request for topic partition {} failed sanity check",
                    topicPartition,
                    sanityCheckException);
                failed.put(topicPartition, sanityCheckException);
            }
        }

        return new ApiResult<>(completed, failed, unmapped);
    }

    private void handlePartitionError(
        TopicPartition topicPartition,
        Errors error,
        Map<TopicPartition, Throwable> failed,
        List<TopicPartition> unmapped,
        Set<TopicPartition> retriable
    ) {
        if (error == Errors.NOT_LEADER_OR_FOLLOWER || error == Errors.LEADER_NOT_AVAILABLE) {
            log.debug(
                "ListOffsets lookup request for topic partition {} will be retried due to invalid leader metadata {}",
                topicPartition,
                error);
            unmapped.add(topicPartition);
        } else if (error.exception() instanceof RetriableException) {
            log.debug(
                "ListOffsets fulfillment request for topic partition {} will be retried due to {}",
                topicPartition,
                error);
            retriable.add(topicPartition);
        } else {
            log.error(
                "ListOffsets request for topic partition {} failed due to an unexpected error {}",
                topicPartition,
                error);
            failed.put(topicPartition, error.exception());
        }
    }

    @Override
    public Map<TopicPartition, Throwable> handleUnsupportedVersionException(
        int brokerId, UnsupportedVersionException exception, Set<TopicPartition> keys
    ) {
        Map<TopicPartition, Throwable> timestampPartitions = new HashMap<>();
        // From newest to oldest version, so we can find the oldest version that doesn't support the TopicPartition
        timestampPartitions.putAll(handleUnsupportedListOffsets(brokerId, exception, keys, ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP));
        timestampPartitions.putAll(handleUnsupportedListOffsets(brokerId, exception, keys, ListOffsetsRequest.LATEST_TIERED_TIMESTAMP));
        timestampPartitions.putAll(handleUnsupportedListOffsets(brokerId, exception, keys, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP));
        timestampPartitions.putAll(handleUnsupportedListOffsets(brokerId, exception, keys, ListOffsetsRequest.MAX_TIMESTAMP));

        // If there are no partitions with timestampType specs the UnsupportedVersionException cannot be handled
        // and all partitions should be failed here.
        // Otherwise, just the partitions with timestampType specs should be failed here and the fulfillment stage
        // will later be retried for the potentially empty set of partitions with non-timestampType specs.
        if (timestampPartitions.isEmpty()) {
            return keys.stream().collect(Collectors.toMap(k -> k, k -> exception));
        }

        return timestampPartitions;
    }

    private Map<TopicPartition, Throwable> handleUnsupportedListOffsets(
        int brokerId, UnsupportedVersionException exception, Set<TopicPartition> keys, long timestampType
    ) {
        log.warn("Broker " + brokerId + " does not support " + timestampToString(timestampType) + " offset specs");
        Map<TopicPartition, Throwable> timestampPartitions = new HashMap<>();
        for (TopicPartition topicPartition : keys) {
            Long offsetTimestamp = offsetTimestampsByPartition.get(topicPartition);
            if (offsetTimestamp == timestampType) {
                timestampPartitions.put(topicPartition, exception);
            }
        }

        return timestampPartitions;
    }


    public static PartitionLeaderStrategy.PartitionLeaderFuture<ListOffsetsResultInfo> newFuture(
        Collection<TopicPartition> topicPartitions,
        Map<TopicPartition, Integer> partitionLeaderCache
    ) {
        return new PartitionLeaderStrategy.PartitionLeaderFuture<>(new HashSet<>(topicPartitions), partitionLeaderCache);
    }

    private static String timestampToString(long timestamp) {
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
            return "EARLIEST_TIMESTAMP";
        } else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return "LATEST_TIMESTAMP";
        } else if (timestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
            return "MAX_TIMESTAMP";
        } else if (timestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
            return "EARLIEST_LOCAL_TIMESTAMP";
        } else if (timestamp == ListOffsetsRequest.LATEST_TIERED_TIMESTAMP) {
            return "LATEST_TIERED_TIMESTAMP";
        } else if (timestamp == ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP) {
            return "EARLIEST_PENDING_UPLOAD_TIMESTAMP";
        } else {
            return "UNKNOWN_TIMESTAMP";
        }
    }

    // Visible for test
    public static long getOffsetFromSpec(OffsetSpec offsetSpec) {
        if (offsetSpec instanceof OffsetSpec.TimestampSpec) {
            return ((OffsetSpec.TimestampSpec) offsetSpec).timestamp();
        } else if (offsetSpec instanceof OffsetSpec.EarliestSpec) {
            return ListOffsetsRequest.EARLIEST_TIMESTAMP;
        } else if (offsetSpec instanceof OffsetSpec.MaxTimestampSpec) {
            return ListOffsetsRequest.MAX_TIMESTAMP;
        } else if (offsetSpec instanceof OffsetSpec.EarliestLocalSpec) {
            return ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP;
        } else if (offsetSpec instanceof OffsetSpec.LatestTieredSpec) {
            return ListOffsetsRequest.LATEST_TIERED_TIMESTAMP;
        } else if (offsetSpec instanceof OffsetSpec.EarliestPendingUploadSpec) {
            return ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP;
        }
        return ListOffsetsRequest.LATEST_TIMESTAMP;
    }

    // A reverse function to get an OffsetSpec from a long offset.
    // This function only works for special, constant offset values.
    public static OffsetSpec getSpecFromOffset(long offset) {
        if (offset == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
            return new OffsetSpec.EarliestSpec();
        } else if (offset == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return new OffsetSpec.LatestSpec();
        } else if (offset == ListOffsetsRequest.MAX_TIMESTAMP) {
            return new OffsetSpec.MaxTimestampSpec();
        } else if (offset == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
            return new OffsetSpec.EarliestLocalSpec();
        } else if (offset == ListOffsetsRequest.LATEST_TIERED_TIMESTAMP) {
            return new OffsetSpec.LatestTieredSpec();
        } else if (offset == ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP) {
            return new OffsetSpec.EarliestPendingUploadSpec();
        }
        return OffsetSpec.forTimestamp(offset);
    }
}
