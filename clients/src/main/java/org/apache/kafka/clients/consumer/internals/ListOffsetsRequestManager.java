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
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Build requests for retrieving partition offsets from partition leaders (see
 * {@link #fetchOffsets(Set, long, boolean)}). Requests are kept in-memory
 * ready to be sent on the next call to {@link #poll(long)}.
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

    private final Set<ListOffsetsRequestState> requestsToRetry;
    private final List<NetworkClientDelegate.UnsentRequest> requestsToSend;

    public ListOffsetsRequestManager(final SubscriptionState subscriptionState,
                                     final ConsumerMetadata metadata,
                                     final IsolationLevel isolationLevel,
                                     final Time time,
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
        this.offsetFetcherUtils = new OffsetFetcherUtils(logContext, metadata, subscriptionState,
                time, apiVersions);
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
     * @param partitions        Partitions to get offsets for
     * @param timestamp         Target time to look offsets for
     * @param requireTimestamps True if this should fail with an UnsupportedVersionException if
     *                          the broker does not support fetching precise timestamps for offsets
     * @return Future containing the map of offsets retrieved for each partition. The future will
     * complete when the responses for the requests are received and processed following a call
     * to {@link #poll(long)}
     */
    public CompletableFuture<Map<TopicPartition, Long>> fetchOffsets(final Set<TopicPartition> partitions,
                                                                     final long timestamp,
                                                                     final boolean requireTimestamps) {
        metadata.addTransientTopics(offsetFetcherUtils.topicsForPartitions(partitions));

        Map<TopicPartition, Long> timestampsToSearch = partitions.stream()
                .collect(Collectors.toMap(Function.identity(), tp -> timestamp));
        ListOffsetsRequestState listOffsetsRequestState = new ListOffsetsRequestState(
                timestampsToSearch,
                requireTimestamps,
                offsetFetcherUtils,
                isolationLevel);
        listOffsetsRequestState.globalResult.whenComplete((result, error) -> {
            metadata.clearTransientTopics();
            log.debug("Fetch offsets completed for partitions {} and timestamp {}. Result {}, " +
                    "error", partitions, timestamp, result, error);
        });

        fetchOffsetsByTimes(timestampsToSearch, requireTimestamps, listOffsetsRequestState);

        return listOffsetsRequestState.globalResult;
    }

    /**
     * Generate requests for partitions with known leaders. Update the listOffsetsRequestState by adding
     * partitions with unknown leader to the listOffsetsRequestState.remainingToSearch
     */
    private void fetchOffsetsByTimes(final Map<TopicPartition, Long> timestampsToSearch,
                                     final boolean requireTimestamps,
                                     final ListOffsetsRequestState listOffsetsRequestState) {
        try {
            List<NetworkClientDelegate.UnsentRequest> unsentRequests =
                    sendListOffsetsRequests(timestampsToSearch, requireTimestamps, listOffsetsRequestState);
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
     * @param timestampsToSearch the mapping between partitions and target time
     * @param requireTimestamps  true if we should fail with an UnsupportedVersionException if the broker does
     *                           not support fetching precise timestamps for offsets
     * @return A list of
     * {@link org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest}
     * that can be polled to obtain the corresponding timestamps and offsets.
     */
    private List<NetworkClientDelegate.UnsentRequest> sendListOffsetsRequests(
            final Map<TopicPartition, Long> timestampsToSearch,
            final boolean requireTimestamps,
            final ListOffsetsRequestState listOffsetsRequestState) {
        Map<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, listOffsetsRequestState);
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

                if (listOffsetsRequestState.remainingToSearch.size() == 0) {
                    ListOffsetResult listOffsetResult =
                            new ListOffsetResult(listOffsetsRequestState.fetchedOffsets,
                                    listOffsetsRequestState.remainingToSearch.keySet());
                    listOffsetsRequestState.globalResult.complete(listOffsetResult.offsetAndMetadataMap());
                } else {
                    requestsToRetry.add(listOffsetsRequestState);
                }
            } else {
                log.debug("List offsets request failed with error", error);
                listOffsetsRequestState.globalResult.completeExceptionally(error);
            }
        });

        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            Node node = entry.getKey();
            ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                    .forConsumer(requireTimestamps, isolationLevel, false)
                    .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(entry.getValue()));

            log.debug("Creating ListOffsetRequest {} for broker {}", builder, node);

            NetworkClientDelegate.UnsentRequest unsentRequest = new NetworkClientDelegate.UnsentRequest(
                    builder,
                    Optional.ofNullable(node));
            unsentRequests.add(unsentRequest);
            unsentRequest.future().whenComplete((response, error) -> {
                if (error != null) {
                    log.error("Sending ListOffsetRequest {} to broker {} failed",
                            builder,
                            node,
                            error);
                    multiNodeRequest.resultFuture.completeExceptionally(error);
                } else {
                    ListOffsetsResponse lor = (ListOffsetsResponse) response.responseBody();
                    log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
                    onSendToNodeSuccess(lor, multiNodeRequest);
                }
            });
        }
        return unsentRequests;
    }

    private void onSendToNodeSuccess(final ListOffsetsResponse response,
                                     final MultiNodeRequest multiNodeRequest) {
        try {
            ListOffsetResult partialResult = offsetFetcherUtils.handleListOffsetResponse(response);
            multiNodeRequest.fetchedTimestampOffsets.putAll(partialResult.fetchedOffsets);
            multiNodeRequest.partitionsToRetry.addAll(partialResult.partitionsToRetry);

            if (multiNodeRequest.expectedResponses.decrementAndGet() == 0) {
                ListOffsetResult result =
                        new ListOffsetResult(multiNodeRequest.fetchedTimestampOffsets,
                                multiNodeRequest.partitionsToRetry);
                multiNodeRequest.resultFuture.complete(result);
            }
        } catch (RuntimeException e) {
            multiNodeRequest.resultFuture.completeExceptionally(e);
        }
    }

    private static class ListOffsetsRequestState {

        private final Map<TopicPartition, Long> timestampsToSearch;
        private final Map<TopicPartition, ListOffsetData> fetchedOffsets;
        private final Map<TopicPartition, Long> remainingToSearch;
        private final CompletableFuture<Map<TopicPartition, Long>> globalResult;
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
    }

    /**
     * Group partitions by leader. Topic partitions from `timestampsToSearch` for which
     * the leader is not known are kept as `remainingToSearch` in the `listOffsetsRequestState`
     *
     * @param timestampsToSearch      The mapping from partitions to the target timestamps
     * @param listOffsetsRequestState Request state that will be extended by adding to its
     *                                `remainingToSearch` map all partitions for which the
     *                                request cannot be performed due to unknown leader (need
     *                                metadata update)
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
                listOffsetsRequestState.remainingToSearch.put(tp, offset);
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
