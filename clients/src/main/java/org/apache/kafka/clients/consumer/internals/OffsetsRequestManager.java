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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetData;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetResult;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.hasUsableOffsetForLeaderEpochVersion;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.regroupFetchPositionsByLeader;

/**
 * Manager responsible for building the following requests to retrieve partition offsets, and
 * processing its responses.
 * <ul>
 *      <li>ListOffset request</li>
 *      <li>OffsetForLeaderEpoch request</li>
 * </ul>
 * Requests are kept in-memory ready to be sent on the next call to {@link #poll(long)}.
 * <br>
 * Partition leadership information required to build ListOffset requests is retrieved from the
 * {@link ConsumerMetadata}, so this implements {@link ClusterResourceListener} to get notified
 * when the cluster metadata is updated.
 */
public class OffsetsRequestManager implements RequestManager, ClusterResourceListener {

    private final ConsumerMetadata metadata;
    private final IsolationLevel isolationLevel;
    private final Logger log;
    private final OffsetFetcherUtils offsetFetcherUtils;
    private final SubscriptionState subscriptionState;

    private final Set<ListOffsetsRequestState> requestsToRetry;
    private final List<NetworkClientDelegate.UnsentRequest> requestsToSend;
    private final long requestTimeoutMs;
    private final Time time;
    private final ApiVersions apiVersions;
    private final NetworkClientDelegate networkClientDelegate;
    private final BackgroundEventHandler backgroundEventHandler;

    @SuppressWarnings("this-escape")
    public OffsetsRequestManager(final SubscriptionState subscriptionState,
                                 final ConsumerMetadata metadata,
                                 final IsolationLevel isolationLevel,
                                 final Time time,
                                 final long retryBackoffMs,
                                 final long requestTimeoutMs,
                                 final ApiVersions apiVersions,
                                 final NetworkClientDelegate networkClientDelegate,
                                 final BackgroundEventHandler backgroundEventHandler,
                                 final LogContext logContext) {
        requireNonNull(subscriptionState);
        requireNonNull(metadata);
        requireNonNull(isolationLevel);
        requireNonNull(time);
        requireNonNull(apiVersions);
        requireNonNull(networkClientDelegate);
        requireNonNull(backgroundEventHandler);
        requireNonNull(logContext);

        this.metadata = metadata;
        this.isolationLevel = isolationLevel;
        this.log = logContext.logger(getClass());
        this.requestsToRetry = new HashSet<>();
        this.requestsToSend = new ArrayList<>();
        this.subscriptionState = subscriptionState;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.apiVersions = apiVersions;
        this.networkClientDelegate = networkClientDelegate;
        this.backgroundEventHandler = backgroundEventHandler;
        this.offsetFetcherUtils = new OffsetFetcherUtils(logContext, metadata, subscriptionState,
                time, retryBackoffMs, apiVersions);
        // Register the cluster metadata update callback. Note this only relies on the
        // requestsToRetry initialized above, and won't be invoked until all managers are
        // initialized and the network thread started.
        this.metadata.addClusterUpdateListener(this);
    }

    /**
     * Determine if there are pending fetch offsets requests to be sent and build a
     * {@link NetworkClientDelegate.PollResult}
     * containing it.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        // Copy the outgoing request list and clear it.
        List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>(requestsToSend);
        requestsToSend.clear();
        return new NetworkClientDelegate.PollResult(unsentRequests);
    }

    /**
     * Retrieve offsets for the given partitions and timestamp. For each partition, this will
     * retrieve the offset of the first message whose timestamp is greater than or equals to the
     * target timestamp.
     *
     * @param timestampsToSearch Partitions and target timestamps to get offsets for
     * @param requireTimestamps  True if this should fail with an UnsupportedVersionException if the
     *                           broker does not support fetching precise timestamps for offsets
     * @return Future containing the map of {@link TopicPartition} and {@link OffsetAndTimestamp}
     * found .The future will complete when the requests responses are received and
     * processed, following a call to {@link #poll(long)}
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> fetchOffsets(
            Map<TopicPartition, Long> timestampsToSearch,
            boolean requireTimestamps) {
        if (timestampsToSearch.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        metadata.addTransientTopics(OffsetFetcherUtils.topicsForPartitions(timestampsToSearch.keySet()));
        ListOffsetsRequestState listOffsetsRequestState = new ListOffsetsRequestState(
                timestampsToSearch,
                requireTimestamps,
                offsetFetcherUtils,
                isolationLevel);
        listOffsetsRequestState.globalResult.whenComplete((result, error) -> {
            metadata.clearTransientTopics();
            if (error != null) {
                log.debug("Fetch offsets completed with error for partitions and timestamps {}.",
                        timestampsToSearch, error);
            } else {
                log.debug("Fetch offsets completed successfully for partitions and timestamps {}." +
                        " Result {}", timestampsToSearch, result);
            }
        });

        prepareFetchOffsetsRequests(timestampsToSearch, requireTimestamps, listOffsetsRequestState);
        return listOffsetsRequestState.globalResult.thenApply(
                result -> OffsetFetcherUtils.buildOffsetsForTimeInternalResult(
                        timestampsToSearch,
                        result.fetchedOffsets));
    }

    /**
     * Reset offsets for all assigned partitions that require it. Offsets will be reset
     * with timestamps according to the reset strategy defined for each partition. This will
     * generate ListOffsets requests for the partitions and timestamps, and enqueue them to be sent
     * on the next call to {@link #poll(long)}.
     *
     * <p/>
     *
     * When a response is received, positions are updated in-memory, on the subscription state. If
     * an error is received in the response, it will be saved to be thrown on the next call to
     * this function (ex. {@link org.apache.kafka.common.errors.TopicAuthorizationException})
     */
    public CompletableFuture<Void> resetPositionsIfNeeded() {
        Map<TopicPartition, Long> offsetResetTimestamps;

        try {
            offsetResetTimestamps = offsetFetcherUtils.getOffsetResetTimestamp();
        } catch (Exception e) {
            backgroundEventHandler.add(new ErrorEvent(e));
            return CompletableFuture.completedFuture(null);
        }

        if (offsetResetTimestamps.isEmpty())
            return CompletableFuture.completedFuture(null);

        return sendListOffsetsRequestsAndResetPositions(offsetResetTimestamps);
    }

    /**
     * Validate positions for all assigned partitions for which a leader change has been detected.
     * This will generate OffsetsForLeaderEpoch requests for the partitions, with the known offset
     * epoch and current leader epoch. It will enqueue the generated requests, to be sent on the
     * next call to {@link #poll(long)}.
     *
     * <p/>
     *
     * When a response is received, positions are validated and, if a log truncation is
     * detected, a {@link LogTruncationException} will be saved in memory, to be thrown on the
     * next call to this function.
     */
    public CompletableFuture<Void> validatePositionsIfNeeded() {
        Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate =
                offsetFetcherUtils.getPartitionsToValidate();
        if (partitionsToValidate.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return sendOffsetsForLeaderEpochRequestsAndValidatePositions(partitionsToValidate);
    }

    /**
     * Generate requests for partitions with known leaders. Update the listOffsetsRequestState by adding
     * partitions with unknown leader to the listOffsetsRequestState.remainingToSearch
     */
    private void prepareFetchOffsetsRequests(final Map<TopicPartition, Long> timestampsToSearch,
                                             final boolean requireTimestamps,
                                             final ListOffsetsRequestState listOffsetsRequestState) {
        try {
            List<NetworkClientDelegate.UnsentRequest> unsentRequests = buildListOffsetsRequests(
                    timestampsToSearch, requireTimestamps, listOffsetsRequestState);
            requestsToSend.addAll(unsentRequests);
        } catch (StaleMetadataException e) {
            requestsToRetry.add(listOffsetsRequestState);
        }
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        // Retry requests that were awaiting a metadata update. Process a copy of the list to
        // avoid errors, given that the list of requestsToRetry may be modified from the
        // fetchOffsetsByTimes call if any of the requests being retried fails
        List<ListOffsetsRequestState> requestsToProcess = new ArrayList<>(requestsToRetry);
        requestsToRetry.clear();
        requestsToProcess.forEach(requestState -> {
            Map<TopicPartition, Long> timestampsToSearch =
                    new HashMap<>(requestState.remainingToSearch);
            requestState.remainingToSearch.clear();
            prepareFetchOffsetsRequests(timestampsToSearch, requestState.requireTimestamps, requestState);
        });
    }

    /**
     * Build ListOffsets requests to fetch offsets by target times for the specified partitions.
     *
     * @param timestampsToSearch the mapping between partitions and target time
     * @param requireTimestamps  true if we should fail with an UnsupportedVersionException if the broker does
     *                           not support fetching precise timestamps for offsets
     * @return A list of
     * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest}
     * that can be polled to obtain the corresponding timestamps and offsets.
     */
    private List<NetworkClientDelegate.UnsentRequest> buildListOffsetsRequests(
            final Map<TopicPartition, Long> timestampsToSearch,
            final boolean requireTimestamps,
            final ListOffsetsRequestState listOffsetsRequestState) {
        log.debug("Building ListOffsets request for partitions {}", timestampsToSearch);
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, Optional.of(listOffsetsRequestState));
        if (timestampsToSearchByNode.isEmpty()) {
            throw new StaleMetadataException();
        }

        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();
        MultiNodeRequest multiNodeRequest = new MultiNodeRequest(timestampsToSearchByNode.size());
        multiNodeRequest.onComplete((multiNodeResult, error) -> {
            // Done sending request to a set of known leaders
            if (error == null) {
                listOffsetsRequestState.fetchedOffsets.putAll(multiNodeResult.fetchedOffsets);
                listOffsetsRequestState.addPartitionsToRetry(multiNodeResult.partitionsToRetry);
                offsetFetcherUtils.updateSubscriptionState(multiNodeResult.fetchedOffsets,
                        isolationLevel);

                if (listOffsetsRequestState.remainingToSearch.isEmpty()) {
                    ListOffsetResult listOffsetResult =
                            new ListOffsetResult(listOffsetsRequestState.fetchedOffsets,
                                    listOffsetsRequestState.remainingToSearch.keySet());
                    listOffsetsRequestState.globalResult.complete(listOffsetResult);
                } else {
                    requestsToRetry.add(listOffsetsRequestState);
                }
            } else {
                log.debug("ListOffsets request failed with error", error);
                listOffsetsRequestState.globalResult.completeExceptionally(error);
            }
        });

        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            Node node = entry.getKey();
            CompletableFuture<ListOffsetResult> partialResult = buildListOffsetRequestToNode(
                    node,
                    entry.getValue(),
                    requireTimestamps,
                    unsentRequests);

            partialResult.whenComplete((result, error) -> {
                if (error != null) {
                    multiNodeRequest.resultFuture.completeExceptionally(error);
                } else {
                    multiNodeRequest.addPartialResult(result);
                }
            });
        }
        return unsentRequests;
    }

    /**
     * Build ListOffsets request to send to a specific broker for the partitions and
     * target timestamps. This also adds the request to the list of unsentRequests.
     */
    private CompletableFuture<ListOffsetResult> buildListOffsetRequestToNode(
            Node node,
            Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> targetTimes,
            boolean requireTimestamps,
            List<NetworkClientDelegate.UnsentRequest> unsentRequests) {
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                .forConsumer(requireTimestamps, isolationLevel, false)
                .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(targetTimes));

        log.debug("Creating ListOffset request {} for broker {} to reset positions", builder,
                node);

        NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
                builder,
                Optional.ofNullable(node));
        unsentRequests.add(unsentRequest);
        CompletableFuture<ListOffsetResult> result = new CompletableFuture<>();
        unsentRequest.whenComplete((response, error) -> {
            if (error != null) {
                log.debug("Sending ListOffset request {} to broker {} failed",
                        builder,
                        node,
                        error);
                result.completeExceptionally(error);
            } else {
                ListOffsetsResponse lor = (ListOffsetsResponse) response.responseBody();
                log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
                try {
                    ListOffsetResult listOffsetResult = offsetFetcherUtils.handleListOffsetResponse(lor);
                    result.complete(listOffsetResult);
                } catch (RuntimeException e) {
                    result.completeExceptionally(e);
                }
            }
        });
        return result;
    }

    /**
     * Make asynchronous ListOffsets request to fetch offsets by target times for the specified
     * partitions. Use the retrieved offsets to reset positions in the subscription state.
     * This also adds the request to the list of unsentRequests.
     *
     * @param timestampsToSearch the mapping between partitions and target time
     * @return A {@link CompletableFuture} which completes when the requests are
     * complete.
     */
    private CompletableFuture<Void> sendListOffsetsRequestsAndResetPositions(
            final Map<TopicPartition, Long> timestampsToSearch) {
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, Optional.empty());

        final AtomicInteger expectedResponses = new AtomicInteger(0);
        final CompletableFuture<Void> globalResult = new CompletableFuture<>();
        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();

        timestampsToSearchByNode.forEach((node, resetTimestamps) -> {
            subscriptionState.setNextAllowedRetry(resetTimestamps.keySet(),
                    time.milliseconds() + requestTimeoutMs);

            CompletableFuture<ListOffsetResult> partialResult = buildListOffsetRequestToNode(
                    node,
                    resetTimestamps,
                    false,
                    unsentRequests);

            partialResult.whenComplete((result, error) -> {
                if (error == null) {
                    offsetFetcherUtils.onSuccessfulResponseForResettingPositions(resetTimestamps,
                            result);
                } else {
                    RuntimeException e;
                    if (error instanceof RuntimeException) {
                        e = (RuntimeException) error;
                    } else {
                        e = new RuntimeException("Unexpected failure in ListOffsets request for " +
                                "resetting positions", error);
                    }
                    offsetFetcherUtils.onFailedResponseForResettingPositions(resetTimestamps, e);
                }
                if (expectedResponses.decrementAndGet() == 0) {
                    globalResult.complete(null);
                }
            });
        });

        if (unsentRequests.isEmpty()) {
            globalResult.complete(null);
        } else {
            expectedResponses.set(unsentRequests.size());
            requestsToSend.addAll(unsentRequests);
        }

        return globalResult;
    }

    /**
     * For each partition that needs validation, make an asynchronous request to get the end-offsets
     * for the partition with the epoch less than or equal to the epoch the partition last saw.
     * <p/>
     * Requests are grouped by Node for efficiency.
     * This also adds the request to the list of unsentRequests.
     *
     * @param partitionsToValidate a map of topic-partition positions to validate
     * @return A {@link CompletableFuture} which completes when the requests are
     * complete.

     */
    private CompletableFuture<Void> sendOffsetsForLeaderEpochRequestsAndValidatePositions(
            Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate) {

        final Map<Node, Map<TopicPartition, SubscriptionState.FetchPosition>> regrouped =
                regroupFetchPositionsByLeader(partitionsToValidate);

        long nextResetTimeMs = time.milliseconds() + requestTimeoutMs;
        final AtomicInteger expectedResponses = new AtomicInteger(0);
        final CompletableFuture<Void> globalResult = new CompletableFuture<>();
        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();
        regrouped.forEach((node, fetchPositions) -> {

            if (node.isEmpty()) {
                metadata.requestUpdate(true);
                return;
            }

            NodeApiVersions nodeApiVersions = apiVersions.get(node.idString());
            if (nodeApiVersions == null) {
                networkClientDelegate.tryConnect(node);
                return;
            }

            if (!hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                log.debug("Skipping validation of fetch offsets for partitions {} since the broker does not " +
                                "support the required protocol version (introduced in Kafka 2.3)",
                        fetchPositions.keySet());
                for (TopicPartition partition : fetchPositions.keySet()) {
                    subscriptionState.completeValidation(partition);
                }
                return;
            }

            subscriptionState.setNextAllowedRetry(fetchPositions.keySet(), nextResetTimeMs);

            CompletableFuture<OffsetsForLeaderEpochUtils.OffsetForEpochResult> partialResult =
                    buildOffsetsForLeaderEpochRequestToNode(node, fetchPositions, unsentRequests);

            partialResult.whenComplete((offsetsResult, error) -> {
                if (error == null) {
                    offsetFetcherUtils.onSuccessfulResponseForValidatingPositions(fetchPositions,
                            offsetsResult);
                } else {
                    RuntimeException e;
                    if (error instanceof RuntimeException) {
                        e = (RuntimeException) error;
                    } else {
                        e = new RuntimeException("Unexpected failure in OffsetsForLeaderEpoch " +
                                "request for validating positions", error);
                    }
                    offsetFetcherUtils.onFailedResponseForValidatingPositions(fetchPositions, e);
                }
                if (expectedResponses.decrementAndGet() == 0) {
                    globalResult.complete(null);
                }
            });
        });

        if (unsentRequests.isEmpty()) {
            globalResult.complete(null);
        } else {
            expectedResponses.set(unsentRequests.size());
            requestsToSend.addAll(unsentRequests);
        }

        return globalResult;
    }

    /**
     * Build OffsetsForLeaderEpoch request to send to a specific broker for the partitions and
     * positions to fetch. This also adds the request to the list of unsentRequests.
     */
    private CompletableFuture<OffsetsForLeaderEpochUtils.OffsetForEpochResult> buildOffsetsForLeaderEpochRequestToNode(
            final Node node,
            final Map<TopicPartition, SubscriptionState.FetchPosition> fetchPositions,
            List<NetworkClientDelegate.UnsentRequest> unsentRequests) {
        AbstractRequest.Builder<OffsetsForLeaderEpochRequest> builder =
                OffsetsForLeaderEpochUtils.prepareRequest(fetchPositions);

        log.debug("Creating OffsetsForLeaderEpoch request request {} to broker {}", builder, node);

        NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
                builder,
                Optional.ofNullable(node));
        unsentRequests.add(unsentRequest);
        CompletableFuture<OffsetsForLeaderEpochUtils.OffsetForEpochResult> result = new CompletableFuture<>();
        unsentRequest.whenComplete((response, error) -> {
            if (error != null) {
                log.debug("Sending OffsetsForLeaderEpoch request {} to broker {} failed",
                        builder,
                        node,
                        error);
                result.completeExceptionally(error);
            } else {
                OffsetsForLeaderEpochResponse offsetsForLeaderEpochResponse = (OffsetsForLeaderEpochResponse) response.responseBody();
                log.trace("Received OffsetsForLeaderEpoch response {} from broker {}", offsetsForLeaderEpochResponse, node);
                try {
                    OffsetsForLeaderEpochUtils.OffsetForEpochResult listOffsetResult =
                            OffsetsForLeaderEpochUtils.handleResponse(fetchPositions, offsetsForLeaderEpochResponse);
                    result.complete(listOffsetResult);
                } catch (RuntimeException e) {
                    result.completeExceptionally(e);
                }
            }
        });
        return result;
    }

    private static class ListOffsetsRequestState {

        private final Map<TopicPartition, Long> timestampsToSearch;
        private final Map<TopicPartition, ListOffsetData> fetchedOffsets;
        private final Map<TopicPartition, Long> remainingToSearch;
        private final CompletableFuture<ListOffsetResult> globalResult;
        final boolean requireTimestamps;
        final OffsetFetcherUtils offsetFetcherUtils;
        final IsolationLevel isolationLevel;

        private ListOffsetsRequestState(Map<TopicPartition, Long> timestampsToSearch,
                                        boolean requireTimestamps,
                                        OffsetFetcherUtils offsetFetcherUtils,
                                        IsolationLevel isolationLevel) {
            remainingToSearch = new HashMap<>();
            fetchedOffsets = new HashMap<>();
            globalResult = new CompletableFuture<>();

            this.timestampsToSearch = timestampsToSearch;
            this.requireTimestamps = requireTimestamps;
            this.offsetFetcherUtils = offsetFetcherUtils;
            this.isolationLevel = isolationLevel;
        }

        private void addPartitionsToRetry(Set<TopicPartition> partitionsToRetry) {
            remainingToSearch.putAll(partitionsToRetry.stream()
                    .collect(Collectors.toMap(tp -> tp, timestampsToSearch::get)));
        }
    }

    private static class MultiNodeRequest {
        final Map<TopicPartition, ListOffsetData> fetchedTimestampOffsets;
        final Set<TopicPartition> partitionsToRetry;
        final AtomicInteger expectedResponses;
        final CompletableFuture<ListOffsetResult> resultFuture;

        private MultiNodeRequest(int nodeCount) {
            fetchedTimestampOffsets = new HashMap<>();
            partitionsToRetry = new HashSet<>();
            expectedResponses = new AtomicInteger(nodeCount);
            resultFuture = new CompletableFuture<>();
        }

        private void onComplete(BiConsumer<? super ListOffsetResult, ? super Throwable> action) {
            resultFuture.whenComplete(action);
        }

        private void addPartialResult(ListOffsetResult partialResult) {
            try {
                fetchedTimestampOffsets.putAll(partialResult.fetchedOffsets);
                partitionsToRetry.addAll(partialResult.partitionsToRetry);

                if (expectedResponses.decrementAndGet() == 0) {
                    ListOffsetResult result =
                            new ListOffsetResult(fetchedTimestampOffsets,
                                    partitionsToRetry);
                    resultFuture.complete(result);
                }
            } catch (RuntimeException e) {
                resultFuture.completeExceptionally(e);
            }
        }
    }

    /**
     * Group partitions by leader. Topic partitions from `timestampsToSearch` for which
     * the leader is not known are kept as `remainingToSearch` in the `listOffsetsRequestState`
     *
     * @param timestampsToSearch      The mapping from partitions to the target timestamps
     * @param listOffsetsRequestState Optional request state that will be extended by adding to its
     *                                `remainingToSearch` map all partitions for which the
     *                                request cannot be performed due to unknown leader (need
     *                                metadata update).
     */
    private Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> groupListOffsetRequests(
            final Map<TopicPartition, Long> timestampsToSearch,
            final Optional<ListOffsetsRequestState> listOffsetsRequestState) {
        final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> partitionDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            TopicPartition tp = entry.getKey();
            Long offset = entry.getValue();
            Metadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);

            if (!leaderAndEpoch.leader.isPresent()) {
                log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset);
                metadata.requestUpdate(true);
                listOffsetsRequestState.ifPresent(offsetsRequestState -> offsetsRequestState.remainingToSearch.put(tp, offset));
            } else {
                int currentLeaderEpoch = leaderAndEpoch.epoch.orElse(ListOffsetsResponse.UNKNOWN_EPOCH);
                partitionDataMap.put(tp, new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(tp.partition())
                        .setTimestamp(offset)
                        .setCurrentLeaderEpoch(currentLeaderEpoch));
            }
        }
        return offsetFetcherUtils.regroupPartitionMapByNode(partitionDataMap);
    }

    // Visible for testing
    int requestsToRetry() {
        return requestsToRetry.size();
    }

    // Visible for testing
    int requestsToSend() {
        return requestsToSend.size();
    }
}
