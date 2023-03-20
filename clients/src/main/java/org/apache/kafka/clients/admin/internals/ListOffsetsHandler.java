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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.OffsetSpec.TimestampSpec;
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Batched;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidMetadataException;
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
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public final class ListOffsetsHandler extends Batched<TopicPartition, ListOffsetsResultInfo> {

    private final ListOffsetsOptions options;
    private final Map<TopicPartition, OffsetSpec> offsetSpecsByPartition;
    private final Logger log;
    private final AdminApiLookupStrategy<TopicPartition> lookupStrategy;

    private volatile boolean skipMaxTimestampRequests = false;

    public ListOffsetsHandler(
        Map<TopicPartition, OffsetSpec> offsetSpecsByPartition,
        ListOffsetsOptions options,
        LogContext logContext
    ) {
        this.offsetSpecsByPartition = offsetSpecsByPartition;
        this.options = options;
        this.log = logContext.logger(ListOffsetsHandler.class);
        this.lookupStrategy = new PartitionLeaderStrategy(logContext);
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
        final Map<String, ListOffsetsTopic> topicsByName = new HashMap<>();
        boolean supportsMaxTimestamp = false;
        for (TopicPartition topicPartition : keys) {
            OffsetSpec offsetSpec = offsetSpecsByPartition.get(topicPartition);
            long offsetQuery = getOffsetFromSpec(offsetSpec);
            if (offsetQuery == ListOffsetsRequest.MAX_TIMESTAMP) {
                if (skipMaxTimestampRequests) {
                    continue;
                }
                supportsMaxTimestamp = true;
            }
            ListOffsetsTopic topic = topicsByName.computeIfAbsent(
                topicPartition.topic(), topicName -> new ListOffsetsTopic().setName(topicName));
            topic.partitions().add(
                new ListOffsetsPartition()
                    .setPartitionIndex(topicPartition.partition())
                    .setTimestamp(offsetQuery));
        }

        return ListOffsetsRequest.Builder
            .forConsumer(true, options.isolationLevel(), supportsMaxTimestamp)
            .setTargetTimes(new ArrayList<>(topicsByName.values()));
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
                OffsetSpec offsetRequestSpec = offsetSpecsByPartition.get(topicPartition);
                if (offsetRequestSpec == null) {
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

        maybeMarkOtherFailures(
            broker,
            keys,
            failed,
            p -> !unmapped.isEmpty() || completed.containsKey(p) || failed.containsKey(p) || retriable.contains(p));

        return new ApiResult<>(completed, failed, unmapped);
    }

    private void handlePartitionError(
        TopicPartition topicPartition,
        Errors error,
        Map<TopicPartition, Throwable> failed,
        List<TopicPartition> unmapped,
        Set<TopicPartition> retriable
    ) {
        ApiException apiException = error.exception();
        if (apiException instanceof InvalidMetadataException) {
            log.debug(
                "ListOffsets lookup request for topic partition {} will be retried due to invalid metadata",
                topicPartition,
                apiException);
            unmapped.add(topicPartition);
        } else if (apiException instanceof RetriableException) {
            log.debug(
                "ListOffsets fulfillment request for topic partition {} will be retried due to {}",
                topicPartition,
                apiException);
            retriable.add(topicPartition);
        } else {
            log.error(
                "ListOffsets request for topic partition {} failed due to an unexpected error",
                topicPartition,
                apiException);
            failed.put(topicPartition, apiException);
        }
    }

    private void maybeMarkOtherFailures(
        Node broker,
        Set<TopicPartition> keys,
        Map<TopicPartition, Throwable> failed,
        Predicate<TopicPartition> sanityCheck
    ) {
        Exception unsupportedMaxTimestampSpecException = new UnsupportedVersionException(
            "Broker " + broker.id() + " does not support MAX_TIMESTAMP offset spec");
        for (TopicPartition topicPartition : keys) {
            // Fail any MAX_TIMESTAMP requests if we had to downgrade the original request
            if (skipMaxTimestampRequests
                && getOffsetFromSpec(
                offsetSpecsByPartition.get(topicPartition)) == ListOffsetsRequest.MAX_TIMESTAMP
            ) {
                log.error(
                    "ListOffsets request for topic partition {} failed due to unsupported version",
                    topicPartition,
                    unsupportedMaxTimestampSpecException);
                failed.put(topicPartition, unsupportedMaxTimestampSpecException);
            // Sanity-check if the current leader for these partitions returned results for all of them
            } else if (!sanityCheck.test(topicPartition)) {
                ApiException sanityCheckException = new ApiException(
                    "The response from broker " + broker.id() +
                        " did not contain a result for topic partition " + topicPartition);
                log.error(
                    "ListOffsets request for topic partition {} failed sanity check",
                    topicPartition,
                    unsupportedMaxTimestampSpecException);
                failed.put(topicPartition, sanityCheckException);
            }
        }
    }

    @Override
    public boolean handleUnsupportedVersionException(
        UnsupportedVersionException exception, boolean isFulfillmentStage) {
        // An UnsupportedVersionException can be addressed only in the fulfillment stage if it's caused
        // by a MAX_TIMESTAMP spec for some partition and there is at least one other partition with
        // a non-MAX_TIMESTAMP spec for which the fulfillment stage can be retried.
        if (!isFulfillmentStage) {
            return false;
        }
        boolean containsMaxTimestampSpec = false;
        boolean containsNonMaxTimestampSpec = false;
        for (Map.Entry<TopicPartition, OffsetSpec> entry : offsetSpecsByPartition.entrySet()) {
            if (getOffsetFromSpec(entry.getValue()) == ListOffsetsRequest.MAX_TIMESTAMP) {
                containsMaxTimestampSpec = true;
            } else {
                containsNonMaxTimestampSpec = true;
            }
            if (containsMaxTimestampSpec && containsNonMaxTimestampSpec) {
                break;
            }
        }
        if (containsMaxTimestampSpec && containsNonMaxTimestampSpec) {
            skipMaxTimestampRequests = true;
            return true;
        } else {
            return false;
        }
    }

    public static SimpleAdminApiFuture<TopicPartition, ListOffsetsResultInfo> newFuture(
        Collection<TopicPartition> topicPartitions
    ) {
        return AdminApiFuture.forKeys(new HashSet<>(topicPartitions));
    }

    // Visible for testing
    static long getOffsetFromSpec(OffsetSpec offsetSpec) {
        if (offsetSpec instanceof TimestampSpec) {
            return ((TimestampSpec) offsetSpec).timestamp();
        } else if (offsetSpec instanceof OffsetSpec.EarliestSpec) {
            return ListOffsetsRequest.EARLIEST_TIMESTAMP;
        } else if (offsetSpec instanceof OffsetSpec.MaxTimestampSpec) {
            return ListOffsetsRequest.MAX_TIMESTAMP;
        }
        return ListOffsetsRequest.LATEST_TIMESTAMP;
    }
}
