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

import org.apache.kafka.clients.ApiVersion;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class is used to handle diverging offset. There are three scenarios.
 * 1) Fetch protocol (version >= 12): the fetch response indicates the largest epoch end offset
 * 2) OffsetsForLeaderEpoch protocol (version >=3): explicitly send request to find the diverging offset
 * 3) none: no validation or truncation detection
 */
class DivergingOffsetDetector {

    private enum DetectorMode {
        OFFSET_FOR_LEADER_EPOCH, FETCH_RESPONSE, NONE
    }

    private final Logger log;
    private final Time time;
    private final long retryBackoffMs;
    private final long requestTimeoutMs;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final ConsumerNetworkClient client;
    private final ApiVersions apiVersions;
    private final OffsetsForLeaderEpochClient offsetsForLeaderEpochClient;
    private final AtomicReference<RuntimeException> cachedException = new AtomicReference<>();

    DivergingOffsetDetector(LogContext logContext,
                            Time time,
                            long retryBackoffMs,
                            long requestTimeoutMs,
                            SubscriptionState subscriptions,
                            ConsumerMetadata metadata,
                            ConsumerNetworkClient client,
                            ApiVersions apiVersions) {
        this.log = logContext.logger(DivergingOffsetDetector.class);
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.client = client;
        this.apiVersions = apiVersions;
        this.offsetsForLeaderEpochClient = new OffsetsForLeaderEpochClient(client, logContext);
    }

    Optional<RuntimeException> pollException() {
        return Optional.ofNullable(cachedException.getAndSet(null));
    }

    /**
     * For each partition which needs validation, make an asynchronous request to get the end-offsets for the partition
     * with the epoch less than or equal to the epoch the partition last saw.
     * <p>
     * Requests are grouped by Node for efficiency.
     */

    void validateOffsetsAsync() {
        Map<Node, Map<TopicPartition, SubscriptionState.FetchPosition>> regrouped = subscriptions
            .partitionsNeedingValidation(time.milliseconds())
            .stream()
            .filter(tp -> subscriptions.position(tp) != null && subscriptions.position(tp).currentLeader.leader.isPresent())
            .collect(Collectors.groupingBy(tp -> subscriptions.position(tp).currentLeader.leader.get(),
                    Collectors.toMap(Function.identity(), subscriptions::position)));

        long nextResetTimeMs = time.milliseconds() + requestTimeoutMs;
        regrouped.forEach((node, fetchPositions) -> {
            if (node.isEmpty()) {
                metadata.requestUpdate();
                return;
            }

            NodeApiVersions nodeApiVersions = apiVersions.get(node.idString());
            if (nodeApiVersions == null) {
                client.tryConnect(node);
                return;
            }

            DetectorMode detectorMode = detectorMode(nodeApiVersions);
            if (detectorMode != DetectorMode.OFFSET_FOR_LEADER_EPOCH) {
                String reason = detectorMode == DetectorMode.FETCH_RESPONSE
                        ? "the broker supports the `Fetch` protocol with truncation detection (introduced in Kafka 2.8)"
                        : "the broker does not support the required protocol version (introduced in Kafka 2.3)";
                log.debug("Skipping validation of fetch offsets for partitions {} since {}}", fetchPositions.keySet(), reason);
                fetchPositions.keySet().forEach(subscriptions::completeValidation);
                return;
            }

            subscriptions.setNextAllowedRetry(fetchPositions.keySet(), nextResetTimeMs);

            RequestFuture<OffsetsForLeaderEpochClient.OffsetForEpochResult> future =
                    offsetsForLeaderEpochClient.sendAsyncRequest(node, fetchPositions);

            future.addListener(new RequestFutureListener<OffsetsForLeaderEpochClient.OffsetForEpochResult>() {
                @Override
                public void onSuccess(OffsetsForLeaderEpochClient.OffsetForEpochResult offsetsResult) {
                    if (!offsetsResult.partitionsToRetry().isEmpty()) {
                        subscriptions.setNextAllowedRetry(offsetsResult.partitionsToRetry(), time.milliseconds() + retryBackoffMs);
                        metadata.requestUpdate();
                    }

                    // For each OffsetsForLeader response, check if the end-offset is lower than our current offset
                    // for the partition. If so, it means we have experienced log truncation and need to reposition
                    // that partition's offset.
                    //
                    // In addition, check whether the returned offset and epoch are valid. If not, then we should reset
                    // its offset if reset policy is configured, or throw out of range exception.
                    List<SubscriptionState.LogTruncation> truncations = offsetsResult.endOffsets().entrySet().stream()
                        .map(entry -> subscriptions.maybeCompleteValidation(entry.getKey(),
                                fetchPositions.get(entry.getKey()), entry.getValue()))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());

                    if (!truncations.isEmpty()) maybeSetException(buildLogTruncationException(truncations));
                }

                @Override
                public void onFailure(RuntimeException e) {
                    subscriptions.requestFailed(fetchPositions.keySet(), time.milliseconds() + retryBackoffMs);
                    metadata.requestUpdate();

                    if (!(e instanceof RetriableException)) maybeSetException(e);
                }
            });
        });
    }

    void validateDivergingEpoch(LinkedHashMap<TopicPartition, FetchResponse.PartitionData<Records>> data) {
        List<SubscriptionState.LogTruncation> truncations = data.entrySet().stream().map(entry -> {
            // skip the unsubscribed partitions
            SubscriptionState.FetchPosition position = subscriptions.positionOrNull(entry.getKey());
            return position == null ? Optional.<SubscriptionState.LogTruncation>empty() :
                    // Empty diverging epoch means the broker does not support truncation detection by fetch protocol
                    entry.getValue().divergingEpoch().flatMap(de ->
                            subscriptions.validateOffset(entry.getKey(), position,
                                    new OffsetForLeaderEpochResponseData.EpochEndOffset()
                                            .setErrorCode(Errors.NONE.code())
                                            .setPartition(entry.getKey().partition())
                                            .setEndOffset(de.endOffset())
                                            .setLeaderEpoch(de.epoch())));
        }).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());

        if (!truncations.isEmpty()) maybeSetException(buildLogTruncationException(truncations));
    }

    private void maybeSetException(RuntimeException e) {
        if (!cachedException.compareAndSet(null, e))
            log.error("Discarding error in validating offset because another error is pending", e);
    }

    static boolean supportOffsetForLeaderEpoch(NodeApiVersions nodeApiVersions) {
        ApiVersion apiVersion = nodeApiVersions.apiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
        return apiVersion != null && apiVersion.maxVersion >= 3;
    }

    private static boolean supportFetchResponse(NodeApiVersions nodeApiVersions) {
        ApiVersion apiVersion = nodeApiVersions.apiVersion(ApiKeys.FETCH);
        return apiVersion != null && apiVersion.maxVersion >= 12;
    }

    private static LogTruncationException buildLogTruncationException(List<SubscriptionState.LogTruncation> truncations) {
        Map<TopicPartition, OffsetAndMetadata> divergentOffsets = new HashMap<>();
        Map<TopicPartition, Long> truncatedFetchOffsets = new HashMap<>();
        truncations.forEach(truncation -> {
            truncation.divergentOffsetOpt.ifPresent(divergentOffset ->
                    divergentOffsets.put(truncation.topicPartition, divergentOffset));
            truncatedFetchOffsets.put(truncation.topicPartition, truncation.fetchPosition.offset);
        });
        return new LogTruncationException("Detected truncated partitions: " + truncations,
                truncatedFetchOffsets, divergentOffsets);
    }

    private static DetectorMode detectorMode(NodeApiVersions nodeApiVersions) {
        if (supportFetchResponse(nodeApiVersions)) return DetectorMode.FETCH_RESPONSE;
        else if (supportOffsetForLeaderEpoch(nodeApiVersions)) return DetectorMode.OFFSET_FOR_LEADER_EPOCH;
        else return DetectorMode.NONE;
    }
}
