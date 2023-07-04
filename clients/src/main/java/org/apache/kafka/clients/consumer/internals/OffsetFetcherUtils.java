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
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility functions for fetching offsets, validating and resetting positions.
 */
class OffsetFetcherUtils {
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptionState;
    private final Time time;
    private final long retryBackoffMs;
    private final ApiVersions apiVersions;
    private final Logger log;

    private final AtomicReference<RuntimeException> cachedOffsetForLeaderException = new AtomicReference<>();
    private final AtomicReference<RuntimeException> cachedListOffsetsException = new AtomicReference<>();
    private final AtomicInteger metadataUpdateVersion = new AtomicInteger(-1);

    OffsetFetcherUtils(LogContext logContext,
                       ConsumerMetadata metadata,
                       SubscriptionState subscriptionState,
                       Time time,
                       long retryBackoffMs,
                       ApiVersions apiVersions) {
        this.log = logContext.logger(getClass());
        this.metadata = metadata;
        this.subscriptionState = subscriptionState;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.apiVersions = apiVersions;
    }

    /**
     * Callback for the response of the list offset call.
     *
     * @param listOffsetsResponse The response from the server.
     * @return {@link OffsetFetcherUtils.ListOffsetResult} extracted from the response, containing the fetched offsets
     * and partitions to retry.
     */
    OffsetFetcherUtils.ListOffsetResult handleListOffsetResponse(ListOffsetsResponse listOffsetsResponse) {
        Map<TopicPartition, OffsetFetcherUtils.ListOffsetData> fetchedOffsets = new HashMap<>();
        Set<TopicPartition> partitionsToRetry = new HashSet<>();
        Set<String> unauthorizedTopics = new HashSet<>();

        for (ListOffsetsResponseData.ListOffsetsTopicResponse topic : listOffsetsResponse.topics()) {
            for (ListOffsetsResponseData.ListOffsetsPartitionResponse partition : topic.partitions()) {
                TopicPartition topicPartition = new TopicPartition(topic.name(), partition.partitionIndex());
                Errors error = Errors.forCode(partition.errorCode());
                switch (error) {
                    case NONE:
                        if (!partition.oldStyleOffsets().isEmpty()) {
                            // Handle v0 response with offsets
                            long offset;
                            if (partition.oldStyleOffsets().size() > 1) {
                                throw new IllegalStateException("Unexpected partitionData response of length " +
                                        partition.oldStyleOffsets().size());
                            } else {
                                offset = partition.oldStyleOffsets().get(0);
                            }
                            log.debug("Handling v0 ListOffsetResponse response for {}. Fetched offset {}",
                                    topicPartition, offset);
                            if (offset != ListOffsetsResponse.UNKNOWN_OFFSET) {
                                OffsetFetcherUtils.ListOffsetData offsetData = new OffsetFetcherUtils.ListOffsetData(offset, null, Optional.empty());
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
                                OffsetFetcherUtils.ListOffsetData offsetData = new OffsetFetcherUtils.ListOffsetData(partition.offset(), partition.timestamp(),
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
            throw new TopicAuthorizationException(unauthorizedTopics);
        else
            return new OffsetFetcherUtils.ListOffsetResult(fetchedOffsets, partitionsToRetry);
    }

    <T> Map<Node, Map<TopicPartition, T>> regroupPartitionMapByNode(Map<TopicPartition, T> partitionMap) {
        return partitionMap.entrySet()
                .stream()
                .collect(Collectors.groupingBy(entry -> metadata.fetch().leaderFor(entry.getKey()),
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    Map<TopicPartition, SubscriptionState.FetchPosition> getPartitionsToValidate() {
        RuntimeException exception = cachedOffsetForLeaderException.getAndSet(null);
        if (exception != null)
            throw exception;

        // Validate each partition against the current leader and epoch
        // If we see a new metadata version, check all partitions
        validatePositionsOnMetadataChange();

        // Collect positions needing validation, with backoff
        return subscriptionState
                .partitionsNeedingValidation(time.milliseconds())
                .stream()
                .filter(tp -> subscriptionState.position(tp) != null)
                .collect(Collectors.toMap(Function.identity(), subscriptionState::position));
    }

    void maybeSetOffsetForLeaderException(RuntimeException e) {
        if (!cachedOffsetForLeaderException.compareAndSet(null, e)) {
            log.error("Discarding error in OffsetsForLeaderEpoch because another error is pending", e);
        }
    }

    /**
     * If we have seen new metadata (as tracked by {@link org.apache.kafka.clients.Metadata#updateVersion()}), then
     * we should check that all the assignments have a valid position.
     */
    void validatePositionsOnMetadataChange() {
        int newMetadataUpdateVersion = metadata.updateVersion();
        if (metadataUpdateVersion.getAndSet(newMetadataUpdateVersion) != newMetadataUpdateVersion) {
            subscriptionState.assignedPartitions().forEach(topicPartition -> {
                ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(topicPartition);
                subscriptionState.maybeValidatePositionForCurrentLeader(apiVersions, topicPartition, leaderAndEpoch);
            });
        }
    }

    Map<TopicPartition, Long> getOffsetResetTimestamp() {
        // Raise exception from previous offset fetch if there is one
        RuntimeException exception = cachedListOffsetsException.getAndSet(null);
        if (exception != null)
            throw exception;

        Set<TopicPartition> partitions = subscriptionState.partitionsNeedingReset(time.milliseconds());
        final Map<TopicPartition, Long> offsetResetTimestamps = new HashMap<>();
        for (final TopicPartition partition : partitions) {
            Long timestamp = offsetResetStrategyTimestamp(partition);
            if (timestamp != null)
                offsetResetTimestamps.put(partition, timestamp);
        }

        return offsetResetTimestamps;
    }

    static Map<TopicPartition, OffsetAndTimestamp> buildOffsetsForTimesResult(final Map<TopicPartition, Long> timestampsToSearch,
                                                                       final Map<TopicPartition, ListOffsetData> fetchedOffsets) {
        HashMap<TopicPartition, OffsetAndTimestamp> offsetsByTimes = new HashMap<>(timestampsToSearch.size());
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet())
            offsetsByTimes.put(entry.getKey(), null);

        for (Map.Entry<TopicPartition, ListOffsetData> entry : fetchedOffsets.entrySet()) {
            // 'entry.getValue().timestamp' will not be null since we are guaranteed
            // to work with a v1 (or later) ListOffset request
            ListOffsetData offsetData = entry.getValue();
            offsetsByTimes.put(entry.getKey(), new OffsetAndTimestamp(offsetData.offset, offsetData.timestamp, offsetData.leaderEpoch));
        }

        return offsetsByTimes;
    }

    private Long offsetResetStrategyTimestamp(final TopicPartition partition) {
        OffsetResetStrategy strategy = subscriptionState.resetStrategy(partition);
        if (strategy == OffsetResetStrategy.EARLIEST)
            return ListOffsetsRequest.EARLIEST_TIMESTAMP;
        else if (strategy == OffsetResetStrategy.LATEST)
            return ListOffsetsRequest.LATEST_TIMESTAMP;
        else
            return null;
    }

    static Set<String> topicsForPartitions(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());
    }

    void updateSubscriptionState(Map<TopicPartition, OffsetFetcherUtils.ListOffsetData> fetchedOffsets,
                                 IsolationLevel isolationLevel) {
        for (final Map.Entry<TopicPartition, ListOffsetData> entry : fetchedOffsets.entrySet()) {
            final TopicPartition partition = entry.getKey();

            // if the interested partitions are part of the subscriptions, use the returned offset to update
            // the subscription state as well:
            //   * with read-committed, the returned offset would be LSO;
            //   * with read-uncommitted, the returned offset would be HW;
            if (subscriptionState.isAssigned(partition)) {
                final long offset = entry.getValue().offset;
                if (isolationLevel == IsolationLevel.READ_COMMITTED) {
                    log.trace("Updating last stable offset for partition {} to {}", partition, offset);
                    subscriptionState.updateLastStableOffset(partition, offset);
                } else {
                    log.trace("Updating high watermark for partition {} to {}", partition, offset);
                    subscriptionState.updateHighWatermark(partition, offset);
                }
            }
        }
    }

    static OffsetResetStrategy timestampToOffsetResetStrategy(long timestamp) {
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP)
            return OffsetResetStrategy.EARLIEST;
        else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP)
            return OffsetResetStrategy.LATEST;
        else
            return null;
    }

    void onSuccessfulRequestForResettingPositions(
            final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> resetTimestamps,
            final ListOffsetResult result) {
        if (!result.partitionsToRetry.isEmpty()) {
            subscriptionState.requestFailed(result.partitionsToRetry, time.milliseconds() + retryBackoffMs);
            metadata.requestUpdate();
        }

        for (Map.Entry<TopicPartition, ListOffsetData> fetchedOffset : result.fetchedOffsets.entrySet()) {
            TopicPartition partition = fetchedOffset.getKey();
            ListOffsetData offsetData = fetchedOffset.getValue();
            ListOffsetsRequestData.ListOffsetsPartition requestedReset = resetTimestamps.get(partition);
            resetPositionIfNeeded(
                    partition,
                    timestampToOffsetResetStrategy(requestedReset.timestamp()),
                    offsetData);
        }
    }

    void onFailedRequestForResettingPositions(
            final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> resetTimestamps,
            final RuntimeException error) {
        subscriptionState.requestFailed(resetTimestamps.keySet(), time.milliseconds() + retryBackoffMs);
        metadata.requestUpdate();

        if (!(error instanceof RetriableException) && !cachedListOffsetsException.compareAndSet(null,
                error))
            log.error("Discarding error in ListOffsetResponse because another error is pending", error);
    }

    // Visible for testing
    void resetPositionIfNeeded(TopicPartition partition, OffsetResetStrategy requestedResetStrategy,
                               ListOffsetData offsetData) {
        SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                offsetData.offset,
                Optional.empty(), // This will ensure we skip validation
                metadata.currentLeader(partition));
        offsetData.leaderEpoch.ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(partition, epoch));
        subscriptionState.maybeSeekUnvalidated(partition, position, requestedResetStrategy);
    }

    static Map<Node, Map<TopicPartition, SubscriptionState.FetchPosition>> regroupFetchPositionsByLeader(
            Map<TopicPartition, SubscriptionState.FetchPosition> partitionMap) {
        return partitionMap.entrySet()
                .stream()
                .filter(entry -> entry.getValue().currentLeader.leader.isPresent())
                .collect(Collectors.groupingBy(entry -> entry.getValue().currentLeader.leader.get(),
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    static boolean hasUsableOffsetForLeaderEpochVersion(NodeApiVersions nodeApiVersions) {
        ApiVersionsResponseData.ApiVersion apiVersion = nodeApiVersions.apiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
        if (apiVersion == null)
            return false;

        return OffsetsForLeaderEpochRequest.supportsTopicPermission(apiVersion.maxVersion());
    }

    static class ListOffsetResult {
        final Map<TopicPartition, OffsetFetcherUtils.ListOffsetData> fetchedOffsets;
        final Set<TopicPartition> partitionsToRetry;

        ListOffsetResult(Map<TopicPartition, OffsetFetcherUtils.ListOffsetData> fetchedOffsets,
                         Set<TopicPartition> partitionsNeedingRetry) {
            this.fetchedOffsets = fetchedOffsets;
            this.partitionsToRetry = partitionsNeedingRetry;
        }

        ListOffsetResult() {
            this.fetchedOffsets = new HashMap<>();
            this.partitionsToRetry = new HashSet<>();
        }
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
}