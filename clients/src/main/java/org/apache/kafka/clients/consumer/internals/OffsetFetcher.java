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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.OffsetsForLeaderEpochClient.OffsetForEpochResult;
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

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
    private final long retryBackoffMs;
    private final long requestTimeoutMs;
    private final IsolationLevel isolationLevel;
    private final AtomicReference<RuntimeException> cachedListOffsetsException = new AtomicReference<>();
    private final AtomicReference<RuntimeException> cachedOffsetForLeaderException = new AtomicReference<>();
    private final OffsetsForLeaderEpochClient offsetsForLeaderEpochClient;
    private final ApiVersions apiVersions;
    private final AtomicInteger metadataUpdateVersion = new AtomicInteger(-1);

    public OffsetFetcher(LogContext logContext,
                         ConsumerNetworkClient client,
                         ConsumerMetadata metadata,
                         SubscriptionState subscriptions,
                         Time time,
                         long retryBackoffMs,
                         long requestTimeoutMs,
                         IsolationLevel isolationLevel,
                         ApiVersions apiVersions) {
        this.log = logContext.logger(getClass());
        this.time = time;
        this.client = client;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.isolationLevel = isolationLevel;
        this.apiVersions = apiVersions;
        this.offsetsForLeaderEpochClient = new OffsetsForLeaderEpochClient(client, logContext);
    }

    /**
     * Represents data about an offset returned by a broker.
     */
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

    private Long offsetResetStrategyTimestamp(final TopicPartition partition) {
        OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
        if (strategy == OffsetResetStrategy.EARLIEST)
            return ListOffsetsRequest.EARLIEST_TIMESTAMP;
        else if (strategy == OffsetResetStrategy.LATEST)
            return ListOffsetsRequest.LATEST_TIMESTAMP;
        else
            return null;
    }

    private OffsetResetStrategy timestampToOffsetResetStrategy(long timestamp) {
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP)
            return OffsetResetStrategy.EARLIEST;
        else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP)
            return OffsetResetStrategy.LATEST;
        else
            return null;
    }

    /**
     * Reset offsets for all assigned partitions that require it.
     *
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset strategy is defined
     *   and one or more partitions aren't awaiting a seekToBeginning() or seekToEnd().
     */
    public void resetPositionsIfNeeded() {
        // Raise exception from previous offset fetch if there is one
        RuntimeException exception = cachedListOffsetsException.getAndSet(null);
        if (exception != null)
            throw exception;

        Set<TopicPartition> partitions = subscriptions.partitionsNeedingReset(time.milliseconds());
        if (partitions.isEmpty())
            return;

        final Map<TopicPartition, Long> offsetResetTimestamps = new HashMap<>();
        for (final TopicPartition partition : partitions) {
            Long timestamp = offsetResetStrategyTimestamp(partition);
            if (timestamp != null)
                offsetResetTimestamps.put(partition, timestamp);
        }

        resetPositionsAsync(offsetResetTimestamps);
    }

    /**
     * Validate offsets for all assigned partitions for which a leader change has been detected.
     */
    public void validatePositionsIfNeeded() {
        RuntimeException exception = cachedOffsetForLeaderException.getAndSet(null);
        if (exception != null)
            throw exception;

        // Validate each partition against the current leader and epoch
        // If we see a new metadata version, check all partitions
        validatePositionsOnMetadataChange();

        // Collect positions needing validation, with backoff
        Map<TopicPartition, FetchPosition> partitionsToValidate = subscriptions
                .partitionsNeedingValidation(time.milliseconds())
                .stream()
                .filter(tp -> subscriptions.position(tp) != null)
                .collect(Collectors.toMap(Function.identity(), subscriptions::position));

        validatePositionsAsync(partitionsToValidate);
    }

    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                   Timer timer) {
        metadata.addTransientTopics(topicsForPartitions(timestampsToSearch.keySet()));

        try {
            Map<TopicPartition, ListOffsetData> fetchedOffsets = fetchOffsetsByTimes(timestampsToSearch,
                    timer, true).fetchedOffsets;

            HashMap<TopicPartition, OffsetAndTimestamp> offsetsByTimes = new HashMap<>(timestampsToSearch.size());
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet())
                offsetsByTimes.put(entry.getKey(), null);

            for (Map.Entry<TopicPartition, ListOffsetData> entry : fetchedOffsets.entrySet()) {
                // 'entry.getValue().timestamp' will not be null since we are guaranteed
                // to work with a v1 (or later) ListOffset request
                ListOffsetData offsetData = entry.getValue();
                offsetsByTimes.put(entry.getKey(), new OffsetAndTimestamp(offsetData.offset, offsetData.timestamp,
                        offsetData.leaderEpoch));
            }

            return offsetsByTimes;
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

            future.addListener(new RequestFutureListener<ListOffsetResult>() {
                @Override
                public void onSuccess(ListOffsetResult value) {
                    synchronized (future) {
                        result.fetchedOffsets.putAll(value.fetchedOffsets);
                        remainingToSearch.keySet().retainAll(value.partitionsToRetry);

                        for (final Map.Entry<TopicPartition, ListOffsetData> entry: value.fetchedOffsets.entrySet()) {
                            final TopicPartition partition = entry.getKey();

                            // if the interested partitions are part of the subscriptions, use the returned offset to update
                            // the subscription state as well:
                            //   * with read-committed, the returned offset would be LSO;
                            //   * with read-uncommitted, the returned offset would be HW;
                            if (subscriptions.isAssigned(partition)) {
                                final long offset = entry.getValue().offset;
                                if (isolationLevel == IsolationLevel.READ_COMMITTED) {
                                    log.trace("Updating last stable offset for partition {} to {}", partition, offset);
                                    subscriptions.updateLastStableOffset(partition, offset);
                                } else {
                                    log.trace("Updating high watermark for partition {} to {}", partition, offset);
                                    subscriptions.updateHighWatermark(partition, offset);
                                }
                            }
                        }
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
            // and throw timeout exception if cannot complete in time
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

    // Visible for testing
    void resetPositionIfNeeded(TopicPartition partition, OffsetResetStrategy requestedResetStrategy, ListOffsetData offsetData) {
        FetchPosition position = new FetchPosition(
                offsetData.offset,
                Optional.empty(), // This will ensure we skip validation
                metadata.currentLeader(partition));
        offsetData.leaderEpoch.ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(partition, epoch));
        subscriptions.maybeSeekUnvalidated(partition, position, requestedResetStrategy);
    }

    private void resetPositionsAsync(Map<TopicPartition, Long> partitionResetTimestamps) {
        Map<Node, Map<TopicPartition, ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(partitionResetTimestamps, new HashSet<>());
        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            Node node = entry.getKey();
            final Map<TopicPartition, ListOffsetsPartition> resetTimestamps = entry.getValue();
            subscriptions.setNextAllowedRetry(resetTimestamps.keySet(), time.milliseconds() + requestTimeoutMs);

            RequestFuture<ListOffsetResult> future = sendListOffsetRequest(node, resetTimestamps, false);
            future.addListener(new RequestFutureListener<ListOffsetResult>() {
                @Override
                public void onSuccess(ListOffsetResult result) {
                    if (!result.partitionsToRetry.isEmpty()) {
                        subscriptions.requestFailed(result.partitionsToRetry, time.milliseconds() + retryBackoffMs);
                        metadata.requestUpdate();
                    }

                    for (Map.Entry<TopicPartition, ListOffsetData> fetchedOffset : result.fetchedOffsets.entrySet()) {
                        TopicPartition partition = fetchedOffset.getKey();
                        ListOffsetData offsetData = fetchedOffset.getValue();
                        ListOffsetsPartition requestedReset = resetTimestamps.get(partition);
                        resetPositionIfNeeded(partition, timestampToOffsetResetStrategy(requestedReset.timestamp()), offsetData);
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    subscriptions.requestFailed(resetTimestamps.keySet(), time.milliseconds() + retryBackoffMs);
                    metadata.requestUpdate();

                    if (!(e instanceof RetriableException) && !cachedListOffsetsException.compareAndSet(null, e))
                        log.error("Discarding error in ListOffsetResponse because another error is pending", e);
                }
            });
        }
    }

    static boolean hasUsableOffsetForLeaderEpochVersion(NodeApiVersions nodeApiVersions) {
        ApiVersion apiVersion = nodeApiVersions.apiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
        if (apiVersion == null)
            return false;

        return OffsetsForLeaderEpochRequest.supportsTopicPermission(apiVersion.maxVersion());
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
        final Map<Node, Map<TopicPartition, FetchPosition>> regrouped =
                regroupFetchPositionsByLeader(partitionsToValidate);

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

            future.addListener(new RequestFutureListener<OffsetForEpochResult>() {
                @Override
                public void onSuccess(OffsetForEpochResult offsetsResult) {
                    List<SubscriptionState.LogTruncation> truncations = new ArrayList<>();
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
                    offsetsResult.endOffsets().forEach((topicPartition, respEndOffset) -> {
                        FetchPosition requestPosition = fetchPositions.get(topicPartition);
                        Optional<SubscriptionState.LogTruncation> truncationOpt =
                                subscriptions.maybeCompleteValidation(topicPartition, requestPosition, respEndOffset);
                        truncationOpt.ifPresent(truncations::add);
                    });

                    if (!truncations.isEmpty()) {
                        maybeSetOffsetForLeaderException(buildLogTruncationException(truncations));
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    subscriptions.requestFailed(fetchPositions.keySet(), time.milliseconds() + retryBackoffMs);
                    metadata.requestUpdate();

                    if (!(e instanceof RetriableException)) {
                        maybeSetOffsetForLeaderException(e);
                    }
                }
            });
        });
    }

    private LogTruncationException buildLogTruncationException(List<SubscriptionState.LogTruncation> truncations) {
        Map<TopicPartition, OffsetAndMetadata> divergentOffsets = new HashMap<>();
        Map<TopicPartition, Long> truncatedFetchOffsets = new HashMap<>();
        for (SubscriptionState.LogTruncation truncation : truncations) {
            truncation.divergentOffsetOpt.ifPresent(divergentOffset ->
                    divergentOffsets.put(truncation.topicPartition, divergentOffset));
            truncatedFetchOffsets.put(truncation.topicPartition, truncation.fetchPosition.offset);
        }
        return new LogTruncationException("Detected truncated partitions: " + truncations,
                truncatedFetchOffsets, divergentOffsets);
    }

    private void maybeSetOffsetForLeaderException(RuntimeException e) {
        if (!cachedOffsetForLeaderException.compareAndSet(null, e)) {
            log.error("Discarding error in OffsetsForLeaderEpoch because another error is pending", e);
        }
    }

    /**
     * Search the offsets by target times for the specified partitions.
     *
     * @param timestampsToSearch the mapping between partitions and target time
     * @param requireTimestamps true if we should fail with an UnsupportedVersionException if the broker does
     *                         not support fetching precise timestamps for offsets
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
            future.addListener(new RequestFutureListener<ListOffsetResult>() {
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
     * @param timestampsToSearch The mapping from partitions ot the target timestamps
     * @param partitionsToRetry A set of topic partitions that will be extended with partitions
     *                          that need metadata update or re-connect to the leader.
     */
    private Map<Node, Map<TopicPartition, ListOffsetsPartition>> groupListOffsetRequests(
            Map<TopicPartition, Long> timestampsToSearch,
            Set<TopicPartition> partitionsToRetry) {
        final Map<TopicPartition, ListOffsetsPartition> partitionDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry: timestampsToSearch.entrySet()) {
            TopicPartition tp  = entry.getKey();
            Long offset = entry.getValue();
            Metadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);

            if (!leaderAndEpoch.leader.isPresent()) {
                log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset);
                metadata.requestUpdate();
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
        return regroupPartitionMapByNode(partitionDataMap);
    }

    /**
     * Send the ListOffsetRequest to a specific broker for the partitions and target timestamps.
     *
     * @param node The node to send the ListOffsetRequest to.
     * @param timestampsToSearch The mapping from partitions to the target timestamps.
     * @param requireTimestamp  True if we require a timestamp in the response.
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private RequestFuture<ListOffsetResult> sendListOffsetRequest(final Node node,
                                                                  final Map<TopicPartition, ListOffsetsPartition> timestampsToSearch,
                                                                  boolean requireTimestamp) {
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                .forConsumer(requireTimestamp, isolationLevel, false)
                .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(timestampsToSearch));

        log.debug("Sending ListOffsetRequest {} to broker {}", builder, node);
        return client.send(node, builder)
                .compose(new RequestFutureAdapter<ClientResponse, ListOffsetResult>() {
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
     * @param listOffsetsResponse The response from the server.
     * @param future The future to be completed when the response returns. Note that any partition-level errors will
     *               generally fail the entire future result. The one exception is UNSUPPORTED_FOR_MESSAGE_FORMAT,
     *               which indicates that the broker does not support the v1 message format. Partitions with this
     *               particular error are simply left out of the future map. Note that the corresponding timestamp
     *               value of each partition may be null only for v0. In v1 and later the ListOffset API would not
     *               return a null timestamp (-1 is returned instead when necessary).
     */
    private void handleListOffsetResponse(ListOffsetsResponse listOffsetsResponse,
                                          RequestFuture<ListOffsetResult> future) {
        Map<TopicPartition, ListOffsetData> fetchedOffsets = new HashMap<>();
        Set<TopicPartition> partitionsToRetry = new HashSet<>();
        Set<String> unauthorizedTopics = new HashSet<>();

        for (ListOffsetsTopicResponse topic : listOffsetsResponse.topics()) {
            for (ListOffsetsPartitionResponse partition : topic.partitions()) {
                TopicPartition topicPartition = new TopicPartition(topic.name(), partition.partitionIndex());
                Errors error = Errors.forCode(partition.errorCode());
                switch (error) {
                    case NONE:
                        if (!partition.oldStyleOffsets().isEmpty()) {
                            // Handle v0 response with offsets
                            long offset;
                            if (partition.oldStyleOffsets().size() > 1) {
                                future.raise(new IllegalStateException("Unexpected partitionData response of length " +
                                        partition.oldStyleOffsets().size()));
                                return;
                            } else {
                                offset = partition.oldStyleOffsets().get(0);
                            }
                            log.debug("Handling v0 ListOffsetResponse response for {}. Fetched offset {}",
                                    topicPartition, offset);
                            if (offset != ListOffsetsResponse.UNKNOWN_OFFSET) {
                                ListOffsetData offsetData = new ListOffsetData(offset, null, Optional.empty());
                                fetchedOffsets.put(topicPartition, offsetData);
                            }
                        } else {
                            // Handle v1 and later response or v0 without offsets
                            log.debug("Handling ListOffsetResponse response for {}. Fetched offset {}, timestamp {}",
                                    topicPartition, partition.offset(), partition.timestamp());
                            if (partition.offset() != ListOffsetsResponse.UNKNOWN_OFFSET) {
                                Optional<Integer> leaderEpoch = (partition.leaderEpoch() == ListOffsetsResponse.UNKNOWN_EPOCH)
                                        ? Optional.empty()
                                        : Optional.of(partition.leaderEpoch());
                                ListOffsetData offsetData = new ListOffsetData(partition.offset(), partition.timestamp(),
                                        leaderEpoch);
                                fetchedOffsets.put(topicPartition, offsetData);
                            }
                        }
                        break;
                    case UNSUPPORTED_FOR_MESSAGE_FORMAT:
                        // The message format on the broker side is before 0.10.0, which means it does not
                        // support timestamps. We treat this case the same as if we weren't able to find an
                        // offset corresponding to the requested timestamp and leave it out of the result.
                        log.debug("Cannot search by timestamp for partition {} because the message format version " +
                                "is before 0.10.0", topicPartition);
                        break;
                    case NOT_LEADER_OR_FOLLOWER:
                    case REPLICA_NOT_AVAILABLE:
                    case KAFKA_STORAGE_ERROR:
                    case OFFSET_NOT_AVAILABLE:
                    case LEADER_NOT_AVAILABLE:
                    case FENCED_LEADER_EPOCH:
                    case UNKNOWN_LEADER_EPOCH:
                        log.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                                topicPartition, error);
                        partitionsToRetry.add(topicPartition);
                        break;
                    case UNKNOWN_TOPIC_OR_PARTITION:
                        log.warn("Received unknown topic or partition error in ListOffset request for partition {}", topicPartition);
                        partitionsToRetry.add(topicPartition);
                        break;
                    case TOPIC_AUTHORIZATION_FAILED:
                        unauthorizedTopics.add(topicPartition.topic());
                        break;
                    default:
                        log.warn("Attempt to fetch offsets for partition {} failed due to unexpected exception: {}, retrying.",
                                topicPartition, error.message());
                        partitionsToRetry.add(topicPartition);
                }
            }
        }

        if (!unauthorizedTopics.isEmpty())
            future.raise(new TopicAuthorizationException(unauthorizedTopics));
        else
            future.complete(new ListOffsetResult(fetchedOffsets, partitionsToRetry));
    }

    static class ListOffsetResult {
        private final Map<TopicPartition, ListOffsetData> fetchedOffsets;
        private final Set<TopicPartition> partitionsToRetry;

        ListOffsetResult(Map<TopicPartition, ListOffsetData> fetchedOffsets, Set<TopicPartition> partitionsNeedingRetry) {
            this.fetchedOffsets = fetchedOffsets;
            this.partitionsToRetry = partitionsNeedingRetry;
        }

        ListOffsetResult() {
            this.fetchedOffsets = new HashMap<>();
            this.partitionsToRetry = new HashSet<>();
        }
    }

    /**
     * If we have seen new metadata (as tracked by {@link org.apache.kafka.clients.Metadata#updateVersion()}), then
     * we should check that all the assignments have a valid position.
     */
    public void validatePositionsOnMetadataChange() {
        int newMetadataUpdateVersion = metadata.updateVersion();
        if (metadataUpdateVersion.getAndSet(newMetadataUpdateVersion) != newMetadataUpdateVersion) {
            subscriptions.assignedPartitions().forEach(topicPartition -> {
                ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(topicPartition);
                subscriptions.maybeValidatePositionForCurrentLeader(apiVersions, topicPartition, leaderAndEpoch);
            });
        }
    }

    private Map<Node, Map<TopicPartition, FetchPosition>> regroupFetchPositionsByLeader(
            Map<TopicPartition, FetchPosition> partitionMap) {
        return partitionMap.entrySet()
                .stream()
                .filter(entry -> entry.getValue().currentLeader.leader.isPresent())
                .collect(Collectors.groupingBy(entry -> entry.getValue().currentLeader.leader.get(),
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private <T> Map<Node, Map<TopicPartition, T>> regroupPartitionMapByNode(Map<TopicPartition, T> partitionMap) {
        return partitionMap.entrySet()
                .stream()
                .collect(Collectors.groupingBy(entry -> metadata.fetch().leaderFor(entry.getKey()),
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private Set<String> topicsForPartitions(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());
    }

}
