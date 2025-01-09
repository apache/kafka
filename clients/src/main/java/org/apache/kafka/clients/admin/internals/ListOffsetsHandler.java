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

        int timeoutMs = options.timeoutMs() != null ? options.timeoutMs() : defaultApiTimeoutMs;
        return ListOffsetsRequest.Builder.forConsumer(true,
                        options.isolationLevel(),
                        supportsMaxTimestamp,
                        requireEarliestLocalTimestamp,
                        requireTieredStorageTimestamp)
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
        log.warn("Broker " + brokerId + " does not support MAX_TIMESTAMP offset specs");
        Map<TopicPartition, Throwable> maxTimestampPartitions = new HashMap<>();
        for (TopicPartition topicPartition : keys) {
            Long offsetTimestamp = offsetTimestampsByPartition.get(topicPartition);
            if (offsetTimestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
                maxTimestampPartitions.put(topicPartition, exception);
            }
        }
        // If there are no partitions with MAX_TIMESTAMP specs the UnsupportedVersionException cannot be handled
        // and all partitions should be failed here.
        // Otherwise, just the partitions with MAX_TIMESTAMP specs should be failed here and the fulfillment stage
        // will later be retried for the potentially empty set of partitions with non-MAX_TIMESTAMP specs.
        if (maxTimestampPartitions.isEmpty()) {
            return keys.stream().collect(Collectors.toMap(k -> k, k -> exception));
        } else {
            return maxTimestampPartitions;
        }
    }

    public static PartitionLeaderStrategy.PartitionLeaderFuture<ListOffsetsResultInfo> newFuture(
        Collection<TopicPartition> topicPartitions,
        Map<TopicPartition, Integer> partitionLeaderCache
    ) {
        return new PartitionLeaderStrategy.PartitionLeaderFuture<>(new HashSet<>(topicPartitions), partitionLeaderCache);
    }
}
