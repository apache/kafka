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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static org.apache.kafka.clients.consumer.internals.FetchUtils.requestMetadataUpdate;

/**
 * {@code FetchCollector} operates at the {@link RecordBatch} level, as that is what is stored in the
 * {@link FetchBuffer}. Each {@link org.apache.kafka.common.record.Record} in the {@link RecordBatch} is converted
 * to a {@link ConsumerRecord} and added to the returned {@link Fetch}.
 *
 * @param <K> Record key type
 * @param <V> Record value type
 */
public class FetchCollector<K, V> {

    private final Logger log;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final FetchConfig fetchConfig;
    private final Deserializers<K, V> deserializers;
    private final FetchMetricsManager metricsManager;
    private final Time time;

    public FetchCollector(final LogContext logContext,
                          final ConsumerMetadata metadata,
                          final SubscriptionState subscriptions,
                          final FetchConfig fetchConfig,
                          final Deserializers<K, V> deserializers,
                          final FetchMetricsManager metricsManager,
                          final Time time) {
        this.log = logContext.logger(FetchCollector.class);
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.fetchConfig = fetchConfig;
        this.deserializers = deserializers;
        this.metricsManager = metricsManager;
        this.time = time;
    }

    /**
     * Return the fetched {@link ConsumerRecord records}, empty the {@link FetchBuffer record buffer}, and
     * update the consumed position.
     *
     * </p>
     *
     * NOTE: returning an {@link Fetch#empty() empty} fetch guarantees the consumed position is not updated.
     *
     * @param fetchBuffer {@link FetchBuffer} from which to retrieve the {@link ConsumerRecord records}
     *
     * @return A {@link Fetch} for the requested partitions
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     *         the defaultResetPolicy is NONE
     * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
     */
    public Fetch<K, V> collectFetch(final FetchBuffer fetchBuffer) {
        final Fetch<K, V> fetch = Fetch.empty();
        final Queue<CompletedFetch> pausedCompletedFetches = new ArrayDeque<>();
        int recordsRemaining = fetchConfig.maxPollRecords;

        try {
            while (recordsRemaining > 0) {
                final CompletedFetch nextInLineFetch = fetchBuffer.nextInLineFetch();

                if (nextInLineFetch == null || nextInLineFetch.isConsumed()) {
                    final CompletedFetch completedFetch = fetchBuffer.peek();

                    if (completedFetch == null)
                        break;

                    if (!completedFetch.isInitialized()) {
                        try {
                            fetchBuffer.setNextInLineFetch(initialize(completedFetch));
                        } catch (Exception e) {
                            // Remove a completedFetch upon a parse with exception if (1) it contains no completedFetch, and
                            // (2) there are no fetched completedFetch with actual content preceding this exception.
                            // The first condition ensures that the completedFetches is not stuck with the same completedFetch
                            // in cases such as the TopicAuthorizationException, and the second condition ensures that no
                            // potential data loss due to an exception in a following record.
                            if (fetch.isEmpty() && FetchResponse.recordsOrFail(completedFetch.partitionData).sizeInBytes() == 0)
                                fetchBuffer.poll();

                            throw e;
                        }
                    } else {
                        fetchBuffer.setNextInLineFetch(completedFetch);
                    }

                    fetchBuffer.poll();
                } else if (subscriptions.isPaused(nextInLineFetch.partition)) {
                    // when the partition is paused we add the records back to the completedFetches queue instead of draining
                    // them so that they can be returned on a subsequent poll if the partition is resumed at that time
                    log.debug("Skipping fetching records for assigned partition {} because it is paused", nextInLineFetch.partition);
                    pausedCompletedFetches.add(nextInLineFetch);
                    fetchBuffer.setNextInLineFetch(null);
                } else {
                    final Fetch<K, V> nextFetch = fetchRecords(nextInLineFetch);
                    recordsRemaining -= nextFetch.numRecords();
                    fetch.add(nextFetch);
                }
            }
        } catch (KafkaException e) {
            if (fetch.isEmpty())
                throw e;
        } finally {
            // add any polled completed fetches for paused partitions back to the completed fetches queue to be
            // re-evaluated in the next poll
            fetchBuffer.addAll(pausedCompletedFetches);
        }

        return fetch;
    }

    private Fetch<K, V> fetchRecords(final CompletedFetch nextInLineFetch) {
        final TopicPartition tp = nextInLineFetch.partition;

        if (!subscriptions.isAssigned(tp)) {
            // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition {} since it is no longer assigned", tp);
        } else if (!subscriptions.isFetchable(tp)) {
            // this can happen when a partition is paused before fetched records are returned to the consumer's
            // poll call or if the offset is being reset
            log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable", tp);
        } else {
            SubscriptionState.FetchPosition position = subscriptions.position(tp);

            if (position == null)
                throw new IllegalStateException("Missing position for fetchable partition " + tp);

            if (nextInLineFetch.nextFetchOffset() == position.offset) {
                List<ConsumerRecord<K, V>> partRecords = nextInLineFetch.fetchRecords(fetchConfig,
                        deserializers,
                        fetchConfig.maxPollRecords);

                log.trace("Returning {} fetched records at offset {} for assigned partition {}",
                        partRecords.size(), position, tp);

                boolean positionAdvanced = false;

                if (nextInLineFetch.nextFetchOffset() > position.offset) {
                    SubscriptionState.FetchPosition nextPosition = new SubscriptionState.FetchPosition(
                            nextInLineFetch.nextFetchOffset(),
                            nextInLineFetch.lastEpoch(),
                            position.currentLeader);
                    log.trace("Updating fetch position from {} to {} for partition {} and returning {} records from `poll()`",
                            position, nextPosition, tp, partRecords.size());
                    subscriptions.position(tp, nextPosition);
                    positionAdvanced = true;
                }

                Long partitionLag = subscriptions.partitionLag(tp, fetchConfig.isolationLevel);
                if (partitionLag != null)
                    metricsManager.recordPartitionLag(tp, partitionLag);

                Long lead = subscriptions.partitionLead(tp);
                if (lead != null) {
                    metricsManager.recordPartitionLead(tp, lead);
                }

                return Fetch.forPartition(tp, partRecords, positionAdvanced);
            } else {
                // these records aren't next in line based on the last consumed position, ignore them
                // they must be from an obsolete request
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        tp, nextInLineFetch.nextFetchOffset(), position);
            }
        }

        log.trace("Draining fetched records for partition {}", tp);
        nextInLineFetch.drain();

        return Fetch.empty();
    }

    /**
     * Initialize a CompletedFetch object.
     */
    protected CompletedFetch initialize(final CompletedFetch completedFetch) {
        final TopicPartition tp = completedFetch.partition;
        final Errors error = Errors.forCode(completedFetch.partitionData.errorCode());
        boolean recordMetrics = true;

        try {
            if (!subscriptions.hasValidPosition(tp)) {
                // this can happen when a rebalance happened while fetch is still in-flight
                log.debug("Ignoring fetched records for partition {} since it no longer has valid position", tp);
                return null;
            } else if (error == Errors.NONE) {
                final CompletedFetch ret = handleInitializeSuccess(completedFetch);
                recordMetrics = ret == null;
                return ret;
            } else {
                handleInitializeErrors(completedFetch, error);
                return null;
            }
        } finally {
            if (recordMetrics) {
                completedFetch.recordAggregatedMetrics(0, 0);
            }

            if (error != Errors.NONE)
                // we move the partition to the end if there was an error. This way, it's more likely that partitions for
                // the same topic can remain together (allowing for more efficient serialization).
                subscriptions.movePartitionToEnd(tp);
        }
    }

    private CompletedFetch handleInitializeSuccess(final CompletedFetch completedFetch) {
        final TopicPartition tp = completedFetch.partition;
        final long fetchOffset = completedFetch.nextFetchOffset();

        // we are interested in this fetch only if the beginning offset matches the
        // current consumed position
        SubscriptionState.FetchPosition position = subscriptions.position(tp);
        if (position == null || position.offset != fetchOffset) {
            log.debug("Discarding stale fetch response for partition {} since its offset {} does not match " +
                    "the expected offset {}", tp, fetchOffset, position);
            return null;
        }

        final FetchResponseData.PartitionData partition = completedFetch.partitionData;
        log.trace("Preparing to read {} bytes of data for partition {} with offset {}",
                FetchResponse.recordsSize(partition), tp, position);
        Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partition).batches().iterator();

        if (!batches.hasNext() && FetchResponse.recordsSize(partition) > 0) {
            if (completedFetch.requestVersion < 3) {
                // Implement the pre KIP-74 behavior of throwing a RecordTooLargeException.
                Map<TopicPartition, Long> recordTooLargePartitions = Collections.singletonMap(tp, fetchOffset);
                throw new RecordTooLargeException("There are some messages at [Partition=Offset]: " +
                        recordTooLargePartitions + " whose size is larger than the fetch size " + fetchConfig.fetchSize +
                        " and hence cannot be returned. Please considering upgrading your broker to 0.10.1.0 or " +
                        "newer to avoid this issue. Alternately, increase the fetch size on the client (using " +
                        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG + ")",
                        recordTooLargePartitions);
            } else {
                // This should not happen with brokers that support FetchRequest/Response V3 or higher (i.e. KIP-74)
                throw new KafkaException("Failed to make progress reading messages at " + tp + "=" +
                        fetchOffset + ". Received a non-empty fetch response from the server, but no " +
                        "complete records were found.");
            }
        }

        if (partition.highWatermark() >= 0) {
            log.trace("Updating high watermark for partition {} to {}", tp, partition.highWatermark());
            subscriptions.updateHighWatermark(tp, partition.highWatermark());
        }

        if (partition.logStartOffset() >= 0) {
            log.trace("Updating log start offset for partition {} to {}", tp, partition.logStartOffset());
            subscriptions.updateLogStartOffset(tp, partition.logStartOffset());
        }

        if (partition.lastStableOffset() >= 0) {
            log.trace("Updating last stable offset for partition {} to {}", tp, partition.lastStableOffset());
            subscriptions.updateLastStableOffset(tp, partition.lastStableOffset());
        }

        if (FetchResponse.isPreferredReplica(partition)) {
            subscriptions.updatePreferredReadReplica(completedFetch.partition, partition.preferredReadReplica(), () -> {
                long expireTimeMs = time.milliseconds() + metadata.metadataExpireMs();
                log.debug("Updating preferred read replica for partition {} to {}, set to expire at {}",
                        tp, partition.preferredReadReplica(), expireTimeMs);
                return expireTimeMs;
            });
        }

        completedFetch.setInitialized();
        return completedFetch;
    }

    private void handleInitializeErrors(final CompletedFetch completedFetch, final Errors error) {
        final TopicPartition tp = completedFetch.partition;
        final long fetchOffset = completedFetch.nextFetchOffset();

        if (error == Errors.NOT_LEADER_OR_FOLLOWER ||
                error == Errors.REPLICA_NOT_AVAILABLE ||
                error == Errors.KAFKA_STORAGE_ERROR ||
                error == Errors.FENCED_LEADER_EPOCH ||
                error == Errors.OFFSET_NOT_AVAILABLE) {
            log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
            requestMetadataUpdate(metadata, subscriptions, tp);
        } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            log.warn("Received unknown topic or partition error in fetch for partition {}", tp);
            requestMetadataUpdate(metadata, subscriptions, tp);
        } else if (error == Errors.UNKNOWN_TOPIC_ID) {
            log.warn("Received unknown topic ID error in fetch for partition {}", tp);
            requestMetadataUpdate(metadata, subscriptions, tp);
        } else if (error == Errors.INCONSISTENT_TOPIC_ID) {
            log.warn("Received inconsistent topic ID error in fetch for partition {}", tp);
            requestMetadataUpdate(metadata, subscriptions, tp);
        } else if (error == Errors.OFFSET_OUT_OF_RANGE) {
            Optional<Integer> clearedReplicaId = subscriptions.clearPreferredReadReplica(tp);

            if (!clearedReplicaId.isPresent()) {
                // If there's no preferred replica to clear, we're fetching from the leader so handle this error normally
                SubscriptionState.FetchPosition position = subscriptions.position(tp);

                if (position == null || fetchOffset != position.offset) {
                    log.debug("Discarding stale fetch response for partition {} since the fetched offset {} " +
                            "does not match the current offset {}", tp, fetchOffset, position);
                } else {
                    String errorMessage = "Fetch position " + position + " is out of range for partition " + tp;

                    if (subscriptions.hasDefaultOffsetResetPolicy()) {
                        log.info("{}, resetting offset", errorMessage);
                        subscriptions.requestOffsetReset(tp);
                    } else {
                        log.info("{}, raising error to the application since no reset policy is configured", errorMessage);
                        throw new OffsetOutOfRangeException(errorMessage,
                                Collections.singletonMap(tp, position.offset));
                    }
                }
            } else {
                log.debug("Unset the preferred read replica {} for partition {} since we got {} when fetching {}",
                        clearedReplicaId.get(), tp, error, fetchOffset);
            }
        } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
            //we log the actual partition and not just the topic to help with ACL propagation issues in large clusters
            log.warn("Not authorized to read from partition {}.", tp);
            throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
        } else if (error == Errors.UNKNOWN_LEADER_EPOCH) {
            log.debug("Received unknown leader epoch error in fetch for partition {}", tp);
        } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
            log.warn("Unknown server error while fetching offset {} for topic-partition {}",
                    fetchOffset, tp);
        } else if (error == Errors.CORRUPT_MESSAGE) {
            throw new KafkaException("Encountered corrupt message when fetching offset "
                    + fetchOffset
                    + " for topic-partition "
                    + tp);
        } else {
            throw new IllegalStateException("Unexpected error code "
                    + error.code()
                    + " while fetching at offset "
                    + fetchOffset
                    + " from topic-partition " + tp);
        }
    }
}
