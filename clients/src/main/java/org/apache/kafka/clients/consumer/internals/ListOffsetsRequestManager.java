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
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetData;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetResult;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
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

/**
 * Manager responsible for building requests to retrieve partition offsets (see
 * {@link #fetchOffsets(Map, boolean)}). Requests are kept in-memory ready to be sent on the next
 * call to {@link #poll(long)}.
 * <p>
 * Partition leadership information required to build the requests is retrieved from the
 * {@link ConsumerMetadata}, so this implements {@link ClusterResourceListener} to get notified
 * when the cluster metadata is updated.
 */
public class ListOffsetsRequestManager implements RequestManager, ClusterResourceListener {

    private final ConsumerMetadata metadata;
    private final IsolationLevel isolationLevel;
    private final Logger log;
    private final OffsetFetcherUtils offsetFetcherUtils;
    private final SubscriptionState subscriptionState;

    private final Set<ListOffsetsRequestState> requestsToRetry;
    private final List<NetworkClientDelegate.UnsentRequest> requestsToSend;
    private final long requestTimeoutMs;
    private final Time time;

    public ListOffsetsRequestManager(final SubscriptionState subscriptionState,
                                     final ConsumerMetadata metadata,
                                     final IsolationLevel isolationLevel,
                                     final Time time,
                                     final long retryBackoffMs,
                                     final long requestTimeoutMs,
                                     final ApiVersions apiVersions,
                                     final LogContext logContext) {
        requireNonNull(subscriptionState);
        requireNonNull(metadata);
        requireNonNull(isolationLevel);
        requireNonNull(time);
        requireNonNull(apiVersions);
        requireNonNull(logContext);

        this.metadata = metadata;
        this.metadata.addClusterUpdateListener(this);
        this.isolationLevel = isolationLevel;
        this.log = logContext.logger(getClass());
        this.requestsToRetry = new HashSet<>();
        this.requestsToSend = new ArrayList<>();
        this.subscriptionState = subscriptionState;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.offsetFetcherUtils = new OffsetFetcherUtils(logContext, metadata, subscriptionState,
                time, retryBackoffMs, apiVersions);
    }

    /**
     * Determine if a there are pending fetch offsets requests to be sent and build a
     * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult}
     * containing it.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        if (requestsToSend.isEmpty()) {
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList());
        }

        NetworkClientDelegate.PollResult pollResult =
                new NetworkClientDelegate.PollResult(Long.MAX_VALUE, new ArrayList<>(requestsToSend));
        this.requestsToSend.clear();

        return pollResult;
    }

    /**
     * Retrieve offsets for the given partitions and timestamp.
     *
     * @param timestampsToSearch Partitions and target timestamps to get offsets for
     * @param requireTimestamps  True if this should fail with an UnsupportedVersionException if the
     *                           broker does not support fetching precise timestamps for offsets
     * @return Future containing the map of {@link TopicPartition} and {@link OffsetAndTimestamp}
     * found (offset of the first message whose timestamp is greater than or equals to the target
     * timestamp).The future will complete when the requests responses are received and
     * processed, following a call to {@link #poll(long)}
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> fetchOffsets(
            final Map<TopicPartition, Long> timestampsToSearch,
            final boolean requireTimestamps) {
        if (timestampsToSearch.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        metadata.addTransientTopics(offsetFetcherUtils.topicsForPartitions(timestampsToSearch.keySet()));

        ListOffsetsRequestState listOffsetsRequestState = new ListOffsetsRequestState(
                timestampsToSearch,
                requireTimestamps,
                offsetFetcherUtils,
                isolationLevel);
        listOffsetsRequestState.globalResult.whenComplete((result, error) -> {
            metadata.clearTransientTopics();
            if (error != null) {
                log.error("Fetch offsets completed with error for partitions and timestamps {}.",
                        timestampsToSearch, error);
            } else {
                log.debug("Fetch offsets completed successfully for partitions and timestamps {}." +
                        " Result {}", timestampsToSearch, result);
            }
        });

        fetchOffsetsByTimes(timestampsToSearch, requireTimestamps, listOffsetsRequestState);

        return listOffsetsRequestState.globalResult.thenApply(result ->
                offsetFetcherUtils.buildOffsetsForTimesResult(timestampsToSearch, result.fetchedOffsets));
    }

    /**
     * Reset offsets for all assigned partitions that require it. Offsets will be reset
     * with timestamps according to the reset strategy defined for each partition.
     * <p>
     * This may throw exception from previous offset fetch if there is one, ex.
     * TopicAuthorizationException
     */
    public void resetPositionsIfNeeded() {
        Map<TopicPartition, Long> offsetResetTimestamps = offsetFetcherUtils.getOffsetResetTimestamp();

        if (offsetResetTimestamps.isEmpty())
            return;

        List<NetworkClientDelegate.UnsentRequest> unsentRequests = sendListOffsetsRequestsAndResetPositions(offsetResetTimestamps);
        requestsToSend.addAll(unsentRequests);
    }

    /**
     * Generate requests for partitions with known leaders. Update the listOffsetsRequestState by adding
     * partitions with unknown leader to the listOffsetsRequestState.remainingToSearch
     */
    private void fetchOffsetsByTimes(final Map<TopicPartition, Long> timestampsToSearch,
                                     final boolean requireTimestamps,
                                     final ListOffsetsRequestState listOffsetsRequestState) {
        if (timestampsToSearch.isEmpty()) {
            // Early return if empty map to avoid wrongfully raising StaleMetadataException on
            // empty grouping
            return;
        }
        try {
            List<NetworkClientDelegate.UnsentRequest> unsentRequests = sendListOffsetsRequests(
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
            fetchOffsetsByTimes(timestampsToSearch, requestState.requireTimestamps, requestState);
        });
    }

    /**
     * Search the offsets by target times for the specified partitions.
     *
     * @param partitionResetTimestamps the mapping between partitions and target time
     * @param requireTimestamps        true if we should fail with an UnsupportedVersionException if the broker does
     *                                 not support fetching precise timestamps for offsets
     * @return A list of
     * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest}
     * that can be polled to obtain the corresponding timestamps and offsets.
     */
    private List<NetworkClientDelegate.UnsentRequest> sendListOffsetsRequests(
            final Map<TopicPartition, Long> partitionResetTimestamps,
            final boolean requireTimestamps,
            final ListOffsetsRequestState listOffsetsRequestState) {
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> partitionResetTimestampsByNode =
                groupListOffsetRequests(partitionResetTimestamps, listOffsetsRequestState);
        if (partitionResetTimestampsByNode.isEmpty()) {
            throw new StaleMetadataException();
        }

        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();
        MultiNodeRequest multiNodeRequest = new MultiNodeRequest(partitionResetTimestampsByNode.size());
        multiNodeRequest.onComplete((multiNodeResult, error) -> {
            // Done sending request to a set of known leaders
            if (error == null) {
                listOffsetsRequestState.fetchedOffsets.putAll(multiNodeResult.fetchedOffsets);
                listOffsetsRequestState.addPartitionsToRetry(multiNodeResult.partitionsToRetry);
                offsetFetcherUtils.updateSubscriptionState(multiNodeResult.fetchedOffsets,
                        isolationLevel);

                if (listOffsetsRequestState.remainingToSearch.size() == 0) {
                    ListOffsetResult listOffsetResult =
                            new ListOffsetResult(listOffsetsRequestState.fetchedOffsets,
                                    listOffsetsRequestState.remainingToSearch.keySet());
                    listOffsetsRequestState.globalResult.complete(listOffsetResult);
                } else {
                    requestsToRetry.add(listOffsetsRequestState);
                }
            } else {
                log.debug("List offsets request failed with error", error);
                listOffsetsRequestState.globalResult.completeExceptionally(error);
            }
        });

        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> entry : partitionResetTimestampsByNode.entrySet()) {
            Node node = entry.getKey();

            CompletableFuture<ListOffsetResult> partialResult = sendListOffsetRequestToNode(
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
     * Build ListOffsetRequest to send to a specific broker for the partitions and
     * target timestamps. This also adds the request to the list of unsentRequests.
     **/
    private CompletableFuture<ListOffsetResult> sendListOffsetRequestToNode(
            Node node,
            Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> targetTimes,
            boolean requireTimestamps,
            List<NetworkClientDelegate.UnsentRequest> unsentRequests) {
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                .forConsumer(requireTimestamps, isolationLevel, false)
                .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(targetTimes));

        log.debug("Creating ListOffsetRequest {} for broker {} to reset positions", builder,
                node);

        NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
                builder,
                Optional.ofNullable(node));
        unsentRequests.add(unsentRequest);
        CompletableFuture<ListOffsetResult> result = new CompletableFuture<>();
        unsentRequest.future().whenComplete((response, error) -> {
            if (error != null) {
                log.error("Sending ListOffsetRequest {} to broker {} failed",
                        builder,
                        node,
                        error);
                result.completeExceptionally(error);
            } else {
                ListOffsetsResponse lor = (ListOffsetsResponse) response.responseBody();
                log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
                try {
                    ListOffsetResult listOffsetResult =
                            offsetFetcherUtils.handleListOffsetResponse(lor);
                    result.complete(listOffsetResult);
                } catch (RuntimeException e) {
                    result.completeExceptionally(e);
                }
            }
        });
        return result;
    }

    private List<NetworkClientDelegate.UnsentRequest> sendListOffsetsRequestsAndResetPositions(
            final Map<TopicPartition, Long> timestampsToSearch) {
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, null);

        final List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();

        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            Node node = entry.getKey();
            final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> resetTimestamps = entry.getValue();
            subscriptionState.setNextAllowedRetry(resetTimestamps.keySet(),
                    time.milliseconds() + requestTimeoutMs);

            CompletableFuture<ListOffsetResult> partialResult = sendListOffsetRequestToNode(
                    node,
                    entry.getValue(),
                    false,
                    unsentRequests);

            partialResult.whenComplete((result, error) -> {
                if (error == null) {
                    offsetFetcherUtils.onSuccessfulRequestForResettingPositions(resetTimestamps,
                            result);
                } else {
                    if (error instanceof RuntimeException) {
                        offsetFetcherUtils.onFailedRequestForResettingPositions(resetTimestamps,
                                (RuntimeException) error);
                    } else {
                        log.error("Unexpected failure in list offset request for resetting " +
                                "positions", error);
                    }
                }
            });
        }
        return unsentRequests;
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
     * @param listOffsetsRequestState Request state that will be extended by adding to its
     *                                `remainingToSearch` map all partitions for which the
     *                                request cannot be performed due to unknown leader (need
     *                                metadata update).
     */
    private Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> groupListOffsetRequests(
            final Map<TopicPartition, Long> timestampsToSearch,
            final ListOffsetsRequestState listOffsetsRequestState) {
        final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> partitionDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            TopicPartition tp = entry.getKey();
            Long offset = entry.getValue();
            Metadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);

            if (!leaderAndEpoch.leader.isPresent()) {
                log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset);
                metadata.requestUpdate();
                if (listOffsetsRequestState != null) {
                    listOffsetsRequestState.remainingToSearch.put(tp, offset);
                }
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
