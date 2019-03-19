package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

class OffsetsForLeaderEpochFetcher {

    private static final Logger log = LoggerFactory.getLogger(OffsetsForLeaderEpochFetcher.class);

    private final Metadata metadata;
    private final SubscriptionState subscriptions;
    private final ConsumerNetworkClient client;
    private final Time time;
    private final long requestTimeoutMs;
    private final long retryBackoffMs;
    private final AtomicReference<RuntimeException> cachedOffsetForLeaderException = new AtomicReference<>();

    OffsetsForLeaderEpochFetcher(Metadata metadata, SubscriptionState subscriptions, ConsumerNetworkClient client,
                                 Time time, long requestTimeoutMs, long retryBackoffMs) {
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.client = client;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
    }

    private <T> Map<Node, Map<TopicPartition, T>> regroupPartitionMapByNode(Map<TopicPartition, T> partitionMap) {
        return partitionMap.entrySet()
                .stream()
                .collect(Collectors.groupingBy(entry -> metadata.fetch().leaderFor(entry.getKey()),
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    void clearAndMaybeThrowException() {
        RuntimeException exception = cachedOffsetForLeaderException.getAndSet(null);
        if (exception != null)
            throw exception;
    }

    void validateOffsetsAsync() {
        Set<TopicPartition> partitions = subscriptions.partitionsNeedingValidation(time.milliseconds());
        Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> validationData = new HashMap<>(partitions.size());
        partitions.forEach(partition -> {
            SubscriptionState.FetchPosition fetchPosition = subscriptions.position(partition);
            if (fetchPosition.lastFetchEpoch.isPresent()) {
                OffsetsForLeaderEpochRequest.PartitionData partitionData = new OffsetsForLeaderEpochRequest.PartitionData(
                        fetchPosition.currentLeader.epoch, fetchPosition.lastFetchEpoch.get()
                );
                validationData.put(partition, partitionData);
            }
        });


        final Map<Node, Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData>> regrouped =
                regroupPartitionMapByNode(validationData);

        regrouped.forEach((node, dataMap) -> {
            subscriptions.setNextAllowedRetry(dataMap.keySet(), time.milliseconds() + requestTimeoutMs);
            RequestFuture<OffsetForEpochResult> future = sendOffsetForLeaderEpochRequest(node, dataMap);
            future.addListener(new RequestFutureListener<OffsetForEpochResult>() {
                @Override
                public void onSuccess(OffsetForEpochResult offsetsResult) {
                    Map<TopicPartition, OffsetAndMetadata> truncationWithoutResetPolicy = new HashMap<>();
                    offsetsResult.endOffsets.forEach((respTopicPartition, respEndOffset) -> {
                        if (!offsetsResult.partitionsToRetry.isEmpty()) {
                            subscriptions.setNextAllowedRetry(offsetsResult.partitionsToRetry, time.milliseconds() + retryBackoffMs);
                            metadata.requestUpdate();
                        }

                        // For each OffsetsForLeader response, check for truncation. This occurs iff the end offset is lower
                        // than our current offset.
                        if (subscriptions.awaitingValidation(respTopicPartition)) {
                            SubscriptionState.FetchPosition currentPosition = subscriptions.position(respTopicPartition);
                            if (respEndOffset.endOffset() < currentPosition.offset) {
                                if (subscriptions.hasDefaultOffsetResetPolicy()) {
                                    Metadata.LeaderAndEpoch currentLeader = metadata.leaderAndEpoch(respTopicPartition);
                                    SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                                            respEndOffset.endOffset(), Optional.of(respEndOffset.leaderEpoch()), currentLeader);
                                    log.info("Truncation detected for partition {}, resetting offset to {}", respTopicPartition, newPosition);
                                    subscriptions.position(respTopicPartition, newPosition);
                                } else {
                                    log.warn("Truncation detected for partition {}, but no reset policy is set", respTopicPartition);
                                    truncationWithoutResetPolicy.put(respTopicPartition, new OffsetAndMetadata(
                                            respEndOffset.endOffset(), Optional.of(respEndOffset.leaderEpoch()), null));
                                }
                            } else {
                                // Offset is fine, clear the validation state
                                subscriptions.validate(respTopicPartition);
                            }
                        }
                    });
                    if (!truncationWithoutResetPolicy.isEmpty()) {
                        throw new LogTruncationException(truncationWithoutResetPolicy);
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    subscriptions.resetFailed(dataMap.keySet(), time.milliseconds() + retryBackoffMs);
                    metadata.requestUpdate();

                    if (!(e instanceof RetriableException) && !cachedOffsetForLeaderException.compareAndSet(null, e))
                        log.error("Discarding error in ListOffsetResponse because another error is pending", e);
                }
            });
        });
    }

    private RequestFuture<OffsetForEpochResult> sendOffsetForLeaderEpochRequest(
            Node node, Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> requestData) {

        OffsetsForLeaderEpochRequest.Builder builder = new OffsetsForLeaderEpochRequest.Builder(
                ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(), requestData);

        return client.send(node, builder).compose(new RequestFutureAdapter<ClientResponse, OffsetForEpochResult>() {
            @Override
            public void onSuccess(ClientResponse response, RequestFuture<OffsetForEpochResult> future) {
                OffsetsForLeaderEpochResponse ofler = (OffsetsForLeaderEpochResponse) response.responseBody();
                log.trace("Received OffsetsForLeaderEpochResponse {} from broker {}", ofler, node);

                Set<TopicPartition> partitionsToRetry = new HashSet<>();
                Set<String> unauthorizedTopics = new HashSet<>();
                Map<TopicPartition, EpochEndOffset> endOffsets = new HashMap<>();

                for (TopicPartition topicPartition : requestData.keySet()) {
                    EpochEndOffset epochEndOffset = ofler.responses().get(topicPartition);
                    if (epochEndOffset == null) {
                        log.warn("Missing partition {} from response, ignoring", topicPartition);
                        continue;
                    }
                    Errors error = epochEndOffset.error();
                    if (error == Errors.NONE) {
                        log.debug("Handling OffsetsForLeaderEpoch response for {}. Got offset {} for epoch {}",
                                topicPartition, epochEndOffset.endOffset(), epochEndOffset.leaderEpoch());
                        endOffsets.put(topicPartition, epochEndOffset);
                    } else if (error == Errors.NOT_LEADER_FOR_PARTITION ||
                            error == Errors.REPLICA_NOT_AVAILABLE ||
                            error == Errors.KAFKA_STORAGE_ERROR ||
                            error == Errors.OFFSET_NOT_AVAILABLE ||
                            error == Errors.LEADER_NOT_AVAILABLE) {
                        log.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                                topicPartition, error);
                        partitionsToRetry.add(topicPartition);
                    } else if (error == Errors.FENCED_LEADER_EPOCH ||
                            error == Errors.UNKNOWN_LEADER_EPOCH) {
                        log.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                                topicPartition, error);
                        partitionsToRetry.add(topicPartition);
                    } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        log.warn("Received unknown topic or partition error in ListOffset request for partition {}", topicPartition);
                        partitionsToRetry.add(topicPartition);
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        unauthorizedTopics.add(topicPartition.topic());
                    } else {
                        log.warn("Attempt to fetch offsets for partition {} failed due to: {}, retrying.", topicPartition, error.message());
                        partitionsToRetry.add(topicPartition);
                    }
                }

                if (!unauthorizedTopics.isEmpty())
                    future.raise(new TopicAuthorizationException(unauthorizedTopics));
                else
                    future.complete(new OffsetForEpochResult(endOffsets, partitionsToRetry));
            }
        });
    }

    static class OffsetForEpochResult {
        private final Map<TopicPartition, EpochEndOffset> endOffsets;
        private final Set<TopicPartition> partitionsToRetry;

        public OffsetForEpochResult(Map<TopicPartition, EpochEndOffset> endOffsets, Set<TopicPartition> partitionsNeedingRetry) {
            this.endOffsets = endOffsets;
            this.partitionsToRetry = partitionsNeedingRetry;
        }
    }
}
