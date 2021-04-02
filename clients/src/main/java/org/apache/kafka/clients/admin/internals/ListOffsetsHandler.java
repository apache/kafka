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

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.Errors.LEADER_NOT_AVAILABLE;

public class ListOffsetsHandler implements AdminApiHandler<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> {
    private final LogContext logContext;
    private final Logger log;
    private final Map<TopicPartition, Long> topicPartitionOffsets;
    private final IsolationLevel isolationLevel;
    private boolean supportsMaxTimestamp = true;

    public ListOffsetsHandler(
        Map<TopicPartition, Long> topicPartitionOffsets,
        LogContext logContext,
        IsolationLevel isolationLevel
    ) {
        this.topicPartitionOffsets = Collections.unmodifiableMap(topicPartitionOffsets);
        this.log = logContext.logger(ListOffsetsHandler.class);
        this.logContext = logContext;
        this.isolationLevel = isolationLevel;
        this.supportsMaxTimestamp = topicPartitionOffsets.values().stream()
            .anyMatch(timestamp -> timestamp == ListOffsetsRequest.MAX_TIMESTAMP);
    }

    public static AdminApiFuture.SimpleAdminApiFuture<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> newFuture(
            Collection<TopicPartition> topicPartitions
    ) {
        return AdminApiFuture.forKeys(new HashSet<>(topicPartitions));
    }

    @Override
    public String apiName() {
        return "listOffsets";
    }

    @Override
    public AdminApiLookupStrategy<TopicPartition> lookupStrategy() {
        return new PartitionLeaderStrategy(logContext);
    }

    @Override
    public ListOffsetsRequest.Builder buildRequest(
        int brokerId,
        Set<TopicPartition> topicPartitions
    ) {

        List<ListOffsetsRequestData.ListOffsetsTopic> listOffsetsTopics = new ArrayList<>(CollectionUtils.groupTopicPartitionsByTopic(
            topicPartitions,
            topic -> new ListOffsetsRequestData.ListOffsetsTopic().setName(topic),
            (topicRequest, tp) -> topicRequest.partitions()
                .add(new ListOffsetsRequestData.ListOffsetsPartition()
                    .setPartitionIndex(tp.partition())
                    .setTimestamp(topicPartitionOffsets.get(tp))
                )
        ).values());

        return ListOffsetsRequest.Builder
            .forConsumer(true, isolationLevel, supportsMaxTimestamp)
            .setTargetTimes(listOffsetsTopics);
    }

    private void handlePartitionError(
        TopicPartition topicPartition,
        ApiError apiError,
        Map<TopicPartition, Throwable> failed,
        List<TopicPartition> unmapped
    ) {
        Errors error = Errors.forCode(apiError.error().code());
        if (error.exception() instanceof InvalidMetadataException) {
            log.debug("Invalid metadata error in `ListOffsets` response for partition {}. " +
                "Will retry later.", topicPartition);
            if (error == Errors.NOT_LEADER_OR_FOLLOWER || error == LEADER_NOT_AVAILABLE)
                unmapped.add(topicPartition);
        } else {
            log.error("Unexpected error in `ListOffsets` response for partition {}",
                topicPartition, apiError.exception());
            failed.put(topicPartition, apiError.error().exception("Failed to list offsets " +
                "for partition " + topicPartition + " due to unexpected error"));
        }
    }

    @Override
    public ApiResult<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> handleResponse(
        Node broker,
        Set<TopicPartition> keys,
        AbstractResponse abstractResponse
    ) {
        ListOffsetsResponse response = (ListOffsetsResponse) abstractResponse;
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> completed = new HashMap<>();
        Map<TopicPartition, Throwable> failed = new HashMap<>();
        List<TopicPartition> unmapped = new ArrayList<>();

        for (ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse : response.data().topics()) {
            for (ListOffsetsResponseData.ListOffsetsPartitionResponse partitionResponse : topicResponse.partitions()) {
                TopicPartition topicPartition = new TopicPartition(
                    topicResponse.name(), partitionResponse.partitionIndex());

                Errors error = Errors.forCode(partitionResponse.errorCode());
                if (error != Errors.NONE) {
                    ApiError apiError = new ApiError(error);
                    handlePartitionError(topicPartition, apiError, failed, unmapped);
                    continue;
                }

                Optional<Integer> leaderEpoch = (partitionResponse.leaderEpoch() == ListOffsetsResponse.UNKNOWN_EPOCH)
                    ? Optional.empty()
                    : Optional.of(partitionResponse.leaderEpoch());

                ListOffsetsResult.ListOffsetsResultInfo resultInfo = new ListOffsetsResult.ListOffsetsResultInfo(
                    partitionResponse.offset(),
                    partitionResponse.timestamp(),
                    leaderEpoch
                );

                completed.put(topicPartition, resultInfo);
            }
        }
        return new ApiResult<>(completed, failed, unmapped);
    }

    @Override
    public Map<TopicPartition, Throwable> handleUnsupportedVersion(
        AdminApiDriver.RequestSpec<TopicPartition> spec,
        UnsupportedVersionException t
    ) {

        if (supportsMaxTimestamp) {
            supportsMaxTimestamp = false;

            Map<TopicPartition, Throwable> failedMaxTimestamp = new HashMap<>();

            // fail any unsupported futures and remove partitions from the downgraded retry
            boolean foundMaxTimestampPartition = false;
            for (Map.Entry<TopicPartition, Long> entry: topicPartitionOffsets.entrySet())
                if (entry.getValue() == ListOffsetsRequest.MAX_TIMESTAMP) {
                    foundMaxTimestampPartition = true;
                    failedMaxTimestamp.put(entry.getKey(), new UnsupportedVersionException(
                        "Broker " + spec.scope.destinationBrokerId() + " does not support MAX_TIMESTAMP offset spec"));
                }

            if (foundMaxTimestampPartition)
                return failedMaxTimestamp;
        }

        return spec.keys.stream().collect(Collectors.toMap(
            Function.identity(),
            key -> t
        ));
    }
}
