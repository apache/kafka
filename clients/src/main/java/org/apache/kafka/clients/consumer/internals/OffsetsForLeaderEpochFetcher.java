package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

class OffsetsForLeaderEpochFetcher {

    private static final Logger log = LoggerFactory.getLogger(OffsetsForLeaderEpochFetcher.class);

    private final Metadata metadata;
    private final SubscriptionState subscriptions;
    private final Time time;
    private final long requestTimeoutMs;
    private final long retryBackoffMs;
    private final AtomicReference<RuntimeException> cachedOffsetForLeaderException = new AtomicReference<>();
    private final OffsetsForLeaderEpochClient asyncClient;

    OffsetsForLeaderEpochFetcher(Metadata metadata, SubscriptionState subscriptions, ConsumerNetworkClient client,
                                 Time time, long requestTimeoutMs, long retryBackoffMs) {
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.asyncClient = new OffsetsForLeaderEpochClient(client);
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
        Map<TopicPartition, SubscriptionState.FetchPosition> positionMap = partitions.stream()
                .collect(Collectors.toMap(Function.identity(), subscriptions::position));

        final Map<Node, Map<TopicPartition, SubscriptionState.FetchPosition>> regrouped =
                regroupPartitionMapByNode(positionMap);

        regrouped.forEach((node, dataMap) -> {
            subscriptions.setNextAllowedRetry(dataMap.keySet(), time.milliseconds() + requestTimeoutMs);

            RequestFuture<OffsetsForLeaderEpochClient.OffsetForEpochResult> future = asyncClient.sendAsyncRequest(node, positionMap);
            future.addListener(new RequestFutureListener<OffsetsForLeaderEpochClient.OffsetForEpochResult>() {
                @Override
                public void onSuccess(OffsetsForLeaderEpochClient.OffsetForEpochResult offsetsResult) {
                    Map<TopicPartition, OffsetAndMetadata> truncationWithoutResetPolicy = new HashMap<>();
                    offsetsResult.endOffsets().forEach((respTopicPartition, respEndOffset) -> {
                        if (!offsetsResult.partitionsToRetry().isEmpty()) {
                            subscriptions.setNextAllowedRetry(offsetsResult.partitionsToRetry(), time.milliseconds() + retryBackoffMs);
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
}
