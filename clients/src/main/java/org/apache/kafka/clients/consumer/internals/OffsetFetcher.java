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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetData;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetResult;
import org.apache.kafka.clients.consumer.internals.OffsetsForLeaderEpochUtils.OffsetForEpochResult;
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.buildOffsetsForTimesResult;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.hasUsableOffsetForLeaderEpochVersion;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.regroupFetchPositionsByLeader;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.topicsForPartitions;

/**
 * {@link OffsetFetcher} is responsible for fetching the {@link OffsetAndTimestamp offsets} for
 * a given set of {@link TopicPartition topic and partition pairs} and for validation and resetting of positions,
 * as needed.
 */
public class OffsetFetcher {

    private final Logger log;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final ConsumerNetworkClient client;
    private final Time time;
    private final int requestTimeoutMs;
    private final IsolationLevel isolationLevel;
    private final OffsetsForLeaderEpochClient offsetsForLeaderEpochClient;
    private final ApiVersions apiVersions;
    private final OffsetFetcherUtils offsetFetcherUtils;

    public OffsetFetcher(LogContext logContext,
                         ConsumerNetworkClient client,
                         ConsumerMetadata metadata,
                         SubscriptionState subscriptions,
                         Time time,
                         long retryBackoffMs,
                         int requestTimeoutMs,
                         IsolationLevel isolationLevel,
                         ApiVersions apiVersions) {
        this.log = logContext.logger(getClass());
        this.time = time;
        this.client = client;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.requestTimeoutMs = requestTimeoutMs;
        this.isolationLevel = isolationLevel;
        this.apiVersions = apiVersions;
        this.offsetsForLeaderEpochClient = new OffsetsForLeaderEpochClient(client, logContext);
        this.offsetFetcherUtils = new OffsetFetcherUtils(logContext, metadata, subscriptions,
                time, retryBackoffMs, apiVersions);
    }

    /**
     * Reset offsets for all assigned partitions that require it.
     *
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset strategy is defined
     *                                                                         and one or more partitions aren't awaiting a seekToBeginning() or seekToEnd().
     */
    public void resetPositionsIfNeeded() {
        Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap =
                offsetFetcherUtils.getOffsetResetStrategyForPartitions();

        if (partitionAutoOffsetResetStrategyMap.isEmpty())
            return;

        resetPositionsAsync(partitionAutoOffsetResetStrategyMap);
    }

    /**
     * Validate offsets for all assigned partitions for which a leader change has been detected.
     */
    public void validatePositionsIfNeeded() {
        Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate =
                offsetFetcherUtils.getPartitionsToValidate();

        validatePositionsAsync(partitionsToValidate);
    }

    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                   Timer timer) {
        metadata.addTransientTopics(topicsForPartitions(timestampsToSearch.keySet()));

        try {
            Map<TopicPartition, ListOffsetData> fetchedOffsets = fetchOffsetsByTimes(timestampsToSearch,
                    timer, true).fetchedOffsets;

            return buildOffsetsForTimesResult(timestampsToSearch, fetchedOffsets);
        } finally {
            metadata.clearTransientTopics();
        }
    }

    private ListOffsetResult fetchOffsetsByTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                 Timer timer,
                                                 boolean requireTimestamps) {
        ListOffsetResult result = new ListOffsetResult();
        if (timestampsToSearch.isEmpty())
            return result;

        Map<TopicPartition, Long> remainingToSearch = new HashMap<>(timestampsToSearch);
        do {
            RequestFuture<ListOffsetResult> future = sendListOffsetsRequests(remainingToSearch, requireTimestamps);

            future.addListener(new RequestFutureListener<>() {
                @Override
                public void onSuccess(ListOffsetResult value) {
                    synchronized (future) {
                        result.fetchedOffsets.putAll(value.fetchedOffsets);
                        remainingToSearch.keySet().retainAll(value.partitionsToRetry);

                        offsetFetcherUtils.updateSubscriptionState(value.fetchedOffsets, isolationLevel);
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    if (!(e instanceof RetriableException)) {
                        throw future.exception();
                    }
                }
            });

            // if timeout is set to zero, do not try to poll the network client at all
            // and return empty immediately; otherwise try to get the results synchronously
            // and throw timeout exception if it cannot complete in time
            if (timer.timeoutMs() == 0L)
                return result;

            client.poll(future, timer);

            if (!future.isDone()) {
                break;
            } else if (remainingToSearch.isEmpty()) {
                return result;
            } else {
                client.awaitMetadataUpdate(timer);
            }
        } while (timer.notExpired());

        throw new TimeoutException("Failed to get offsets by times in " + timer.elapsedMs() + "ms");
    }

    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Timer timer) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timer);
    }

    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Timer timer) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.LATEST_TIMESTAMP, timer);
    }

    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp,
                                                           Timer timer) {
        metadata.addTransientTopics(topicsForPartitions(partitions));
        try {
            Map<TopicPartition, Long> timestampsToSearch = partitions.stream()
                    .distinct()
                    .collect(Collectors.toMap(Function.identity(), tp -> timestamp));

            ListOffsetResult result = fetchOffsetsByTimes(timestampsToSearch, timer, false);

            return result.fetchedOffsets.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset));
        } finally {
            metadata.clearTransientTopics();
        }
    }

    private void resetPositionsAsync(Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap) {
        Map<TopicPartition, Long> partitionResetTimestamps = partitionAutoOffsetResetStrategyMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().timestamp().get()));
        Map<Node, Map<TopicPartition, ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(partitionResetTimestamps, new HashSet<>());
        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            Node node = entry.getKey();
            final Map<TopicPartition, ListOffsetsPartition> resetTimestamps = entry.getValue();
            subscriptions.setNextAllowedRetry(resetTimestamps.keySet(), time.milliseconds() + requestTimeoutMs);

            RequestFuture<ListOffsetResult> future = sendListOffsetRequest(node, resetTimestamps, false);
            future.addListener(new RequestFutureListener<>() {
                @Override
                public void onSuccess(ListOffsetResult result) {
                    offsetFetcherUtils.onSuccessfulResponseForResettingPositions(result, partitionAutoOffsetResetStrategyMap);
                }

                @Override
                public void onFailure(RuntimeException e) {
                    offsetFetcherUtils.onFailedResponseForResettingPositions(resetTimestamps, e);
                }
            });
        }
    }

    /**
     * For each partition which needs validation, make an asynchronous request to get the end-offsets for the partition
     * with the epoch less than or equal to the epoch the partition last saw.
     *
     * <p/>
     *
     * Requests are grouped by Node for efficiency.
     */
    private void validatePositionsAsync(Map<TopicPartition, FetchPosition> partitionsToValidate) {
        final Map<Node, Map<TopicPartition, FetchPosition>> regrouped = regroupFetchPositionsByLeader(partitionsToValidate);

        long nextResetTimeMs = time.milliseconds() + requestTimeoutMs;
        regrouped.forEach((node, fetchPositions) -> {
            if (node.isEmpty()) {
                metadata.requestUpdate(true);
                return;
            }

            NodeApiVersions nodeApiVersions = apiVersions.get(node.idString());
            if (nodeApiVersions == null) {
                client.tryConnect(node);
                return;
            }

            if (!hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                log.debug("Skipping validation of fetch offsets for partitions {} since the broker does not " +
                                "support the required protocol version (introduced in Kafka 2.3)",
                        fetchPositions.keySet());
                for (TopicPartition partition : fetchPositions.keySet()) {
                    subscriptions.completeValidation(partition);
                }
                return;
            }

            subscriptions.setNextAllowedRetry(fetchPositions.keySet(), nextResetTimeMs);

            RequestFuture<OffsetForEpochResult> future =
                    offsetsForLeaderEpochClient.sendAsyncRequest(node, fetchPositions);

            future.addListener(new RequestFutureListener<>() {
                @Override
                public void onSuccess(OffsetForEpochResult offsetsResult) {
                    offsetFetcherUtils.onSuccessfulResponseForValidatingPositions(fetchPositions,
                            offsetsResult);
                }

                @Override
                public void onFailure(RuntimeException e) {
                    offsetFetcherUtils.onFailedResponseForValidatingPositions(fetchPositions, e);
                }
            });
        });
    }

    /**
     * Search the offsets by target times for the specified partitions.
     *
     * @param timestampsToSearch the mapping between partitions and target time
     * @param requireTimestamps  true if we should fail with an UnsupportedVersionException if the broker does
     *                           not support fetching precise timestamps for offsets
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private RequestFuture<ListOffsetResult> sendListOffsetsRequests(final Map<TopicPartition, Long> timestampsToSearch,
                                                                    final boolean requireTimestamps) {
        final Set<TopicPartition> partitionsToRetry = new HashSet<>();
        Map<Node, Map<TopicPartition, ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, partitionsToRetry);
        if (timestampsToSearchByNode.isEmpty())
            return RequestFuture.failure(new StaleMetadataException());

        final RequestFuture<ListOffsetResult> listOffsetRequestsFuture = new RequestFuture<>();
        final Map<TopicPartition, ListOffsetData> fetchedTimestampOffsets = new HashMap<>();
        final AtomicInteger remainingResponses = new AtomicInteger(timestampsToSearchByNode.size());

        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            RequestFuture<ListOffsetResult> future = sendListOffsetRequest(entry.getKey(), entry.getValue(), requireTimestamps);
            future.addListener(new RequestFutureListener<>() {
                @Override
                public void onSuccess(ListOffsetResult partialResult) {
                    synchronized (listOffsetRequestsFuture) {
                        fetchedTimestampOffsets.putAll(partialResult.fetchedOffsets);
                        partitionsToRetry.addAll(partialResult.partitionsToRetry);

                        if (remainingResponses.decrementAndGet() == 0 && !listOffsetRequestsFuture.isDone()) {
                            ListOffsetResult result = new ListOffsetResult(fetchedTimestampOffsets, partitionsToRetry);
                            listOffsetRequestsFuture.complete(result);
                        }
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    synchronized (listOffsetRequestsFuture) {
                        if (!listOffsetRequestsFuture.isDone())
                            listOffsetRequestsFuture.raise(e);
                    }
                }
            });
        }
        return listOffsetRequestsFuture;
    }

    /**
     * Groups timestamps to search by node for topic partitions in `timestampsToSearch` that have
     * leaders available. Topic partitions from `timestampsToSearch` that do not have their leader
     * available are added to `partitionsToRetry`
     *
     * @param timestampsToSearch The mapping from partitions to the target timestamps
     * @param partitionsToRetry  A set of topic partitions that will be extended with partitions
     *                           that need metadata update or re-connect to the leader.
     */
    private Map<Node, Map<TopicPartition, ListOffsetsPartition>> groupListOffsetRequests(
            Map<TopicPartition, Long> timestampsToSearch,
            Set<TopicPartition> partitionsToRetry) {
        final Map<TopicPartition, ListOffsetsPartition> partitionDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            TopicPartition tp = entry.getKey();
            Long offset = entry.getValue();
            Metadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);

            if (leaderAndEpoch.leader.isEmpty()) {
                log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset);
                metadata.requestUpdate(true);
                partitionsToRetry.add(tp);
            } else {
                Node leader = leaderAndEpoch.leader.get();
                if (client.isUnavailable(leader)) {
                    client.maybeThrowAuthFailure(leader);

                    // The connection has failed and we need to await the backoff period before we can
                    // try again. No need to request a metadata update since the disconnect will have
                    // done so already.
                    log.debug("Leader {} for partition {} is unavailable for fetching offset until reconnect backoff expires",
                            leader, tp);
                    partitionsToRetry.add(tp);
                } else {
                    int currentLeaderEpoch = leaderAndEpoch.epoch.orElse(ListOffsetsResponse.UNKNOWN_EPOCH);
                    partitionDataMap.put(tp, new ListOffsetsPartition()
                            .setPartitionIndex(tp.partition())
                            .setTimestamp(offset)
                            .setCurrentLeaderEpoch(currentLeaderEpoch));
                }
            }
        }
        return offsetFetcherUtils.regroupPartitionMapByNode(partitionDataMap);
    }

    /**
     * Send the ListOffsetRequest to a specific broker for the partitions and target timestamps.
     *
     * @param node               The node to send the ListOffsetRequest to.
     * @param timestampsToSearch The mapping from partitions to the target timestamps.
     * @param requireTimestamp   True if we require a timestamp in the response.
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private RequestFuture<ListOffsetResult> sendListOffsetRequest(final Node node,
                                                                  final Map<TopicPartition, ListOffsetsPartition> timestampsToSearch,
                                                                  boolean requireTimestamp) {
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                .forConsumer(requireTimestamp, isolationLevel)
                .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(timestampsToSearch))
                .setTimeoutMs(requestTimeoutMs);

        log.debug("Sending ListOffsetRequest {} to broker {}", builder, node);
        return client.send(node, builder)
                .compose(new RequestFutureAdapter<>() {
                    @Override
                    public void onSuccess(ClientResponse response, RequestFuture<ListOffsetResult> future) {
                        ListOffsetsResponse lor = (ListOffsetsResponse) response.responseBody();
                        log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
                        handleListOffsetResponse(lor, future);
                    }
                });
    }

    /**
     * Callback for the response of the list offset call above.
     *
     * @param listOffsetsResponse The response from the server.
     * @param future              The future to be completed when the response returns. Note that any partition-level errors will
     *                            generally fail the entire future result. The one exception is UNSUPPORTED_FOR_MESSAGE_FORMAT,
     *                            which indicates that the broker does not support the v1 message format. Partitions with this
     *                            particular error are simply left out of the future map. Note that the corresponding timestamp
     *                            value of each partition may be null only for v0. In v1 and later the ListOffset API would not
     *                            return a null timestamp (-1 is returned instead when necessary).
     */
    private void handleListOffsetResponse(ListOffsetsResponse listOffsetsResponse,
                                          RequestFuture<ListOffsetResult> future) {
        try {
            ListOffsetResult result = offsetFetcherUtils.handleListOffsetResponse(listOffsetsResponse);
            future.complete(result);
        } catch (RuntimeException e) {
            future.raise(e);
        }
    }

    /**
     * If we have seen new metadata (as tracked by {@link org.apache.kafka.clients.Metadata#updateVersion()}), then
     * we should check that all the assignments have a valid position.
     */
    public void validatePositionsOnMetadataChange() {
        offsetFetcherUtils.validatePositionsOnMetadataChange();
    }
}