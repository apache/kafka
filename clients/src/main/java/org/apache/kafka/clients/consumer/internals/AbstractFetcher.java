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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.FetchSessionHandler.FetchRequestData;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

/**
 * This class manages the fetching process with the brokers.
 */
public abstract class AbstractFetcher<K, V> {

    private final Logger log;
    protected final FetchContext<K, V> fetchContext;
    protected final ApiVersions apiVersions;
    protected final ConcurrentLinkedQueue<CompletedFetch<K, V>> completedFetches;
    protected final Map<Integer, FetchSessionHandler> sessionHandlers;
    protected final Set<Integer> nodesWithPendingFetchRequests;
    protected final AtomicInteger metadataUpdateVersion = new AtomicInteger(-1);
    protected CompletedFetch<K, V> nextInLineFetch = null;

    public AbstractFetcher(FetchContext<K, V> fetchContext, ApiVersions apiVersions) {
        this.log = fetchContext.logContext.logger(AbstractFetcher.class);
        this.fetchContext = fetchContext;
        this.apiVersions = apiVersions;
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.sessionHandlers = new HashMap<>();
        this.nodesWithPendingFetchRequests = new HashSet<>();
    }

    protected abstract boolean isUnavailable(Node node);

    protected abstract void maybeThrowAuthFailure(Node node);

    protected FetchRequest.Builder createFetchRequest(final FetchRequestData data) {
        final short maxVersion;

        if (!data.canUseTopicIds()) {
            maxVersion = (short) 12;
        } else {
            maxVersion = ApiKeys.FETCH.latestVersion();
        }

        return FetchRequest.Builder
            .forConsumer(maxVersion, fetchContext.maxWaitMs, fetchContext.minBytes, data.toSend())
            .isolationLevel(fetchContext.isolationLevel)
            .setMaxBytes(fetchContext.maxBytes)
            .metadata(data.metadata())
            .removed(data.toForget())
            .replaced(data.toReplace())
            .rackId(fetchContext.clientRackId);
    }

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     * <p/>
     * NOTE: returning an {@link Fetch#isEmpty empty} fetch guarantees the consumed position is not updated.
     *
     * @return A {@link Fetch} for the requested partitions
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     *         the defaultResetPolicy is NONE
     * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
     */
    public Fetch<K, V> collectFetch(Metadata metadata, SubscriptionState subscriptions) {
        Fetch<K, V> fetch = Fetch.empty();
        Queue<CompletedFetch<K, V>> pausedCompletedFetches = new ArrayDeque<>();
        int recordsRemaining = fetchContext.maxPollRecords;

        try {
            while (recordsRemaining > 0) {
                if (nextInLineFetch == null || nextInLineFetch.isConsumed()) {
                    CompletedFetch<K, V> records = completedFetches.peek();
                    if (records == null) break;

                    if (!records.isInitialized()) {
                        try {
                            nextInLineFetch = initializeCompletedFetch(metadata, subscriptions, records);
                        } catch (Exception e) {
                            // Remove a completedFetch upon a parse with exception if (1) it contains no records, and
                            // (2) there are no fetched records with actual content preceding this exception.
                            // The first condition ensures that the completedFetches is not stuck with the same completedFetch
                            // in cases such as the TopicAuthorizationException, and the second condition ensures that no
                            // potential data loss due to an exception in a following record.
                            FetchResponseData.PartitionData partition = records.partitionData();
                            if (fetch.isEmpty() && FetchResponse.recordsOrFail(partition).sizeInBytes() == 0) {
                                completedFetches.poll();
                            }
                            throw e;
                        }
                    } else {
                        nextInLineFetch = records;
                    }
                    completedFetches.poll();
                } else if (subscriptions.isPaused(nextInLineFetch.partition())) {
                    // when the partition is paused we add the records back to the completedFetches queue instead of draining
                    // them so that they can be returned on a subsequent poll if the partition is resumed at that time
                    log.debug("Skipping fetching records for assigned partition {} because it is paused", nextInLineFetch.partition());
                    pausedCompletedFetches.add(nextInLineFetch);
                    nextInLineFetch = null;
                } else {
                    Fetch<K, V> nextFetch = fetchRecords(subscriptions, nextInLineFetch, recordsRemaining);
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
            completedFetches.addAll(pausedCompletedFetches);
        }

        return fetch;
    }

    protected Fetch<K, V> fetchRecords(SubscriptionState subscriptions, CompletedFetch<K, V> completedFetch, int maxRecords) {
        if (!subscriptions.isAssigned(completedFetch.partition())) {
            // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition {} since it is no longer assigned",
                    completedFetch.partition());
        } else if (!subscriptions.isFetchable(completedFetch.partition())) {
            // this can happen when a partition is paused before fetched records are returned to the consumer's
            // poll call or if the offset is being reset
            log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable",
                    completedFetch.partition());
        } else {
            FetchPosition position = subscriptions.position(completedFetch.partition());
            if (position == null) {
                throw new IllegalStateException("Missing position for fetchable partition " + completedFetch.partition());
            }

            if (completedFetch.nextFetchOffset() == position.offset) {
                List<ConsumerRecord<K, V>> partRecords = completedFetch.fetchRecords(subscriptions, maxRecords);

                log.trace("Returning {} fetched records at offset {} for assigned partition {}",
                        partRecords.size(), position, completedFetch.partition());

                boolean positionAdvanced = false;

                if (completedFetch.nextFetchOffset() > position.offset) {
                    FetchPosition nextPosition = new FetchPosition(
                            completedFetch.nextFetchOffset(),
                            completedFetch.lastEpoch(),
                            position.currentLeader);
                    log.trace("Updating fetch position from {} to {} for partition {} and returning {} records from `poll()`",
                            position, nextPosition, completedFetch.partition(), partRecords.size());
                    subscriptions.position(completedFetch.partition(), nextPosition);
                    positionAdvanced = true;
                }

                Long partitionLag = subscriptions.partitionLag(completedFetch.partition(), fetchContext.isolationLevel);
                if (partitionLag != null)
                    fetchContext.fetchManagerMetrics.recordPartitionLag(completedFetch.partition(), partitionLag);

                Long lead = subscriptions.partitionLead(completedFetch.partition());
                if (lead != null) {
                    fetchContext.fetchManagerMetrics.recordPartitionLead(completedFetch.partition(), lead);
                }

                return Fetch.forPartition(completedFetch.partition(), partRecords, positionAdvanced);
            } else {
                // these records aren't next in line based on the last consumed position, ignore them
                // they must be from an obsolete request
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        completedFetch.partition(), completedFetch.nextFetchOffset(), position);
            }
        }

        log.trace("Draining fetched records for partition {}", completedFetch.partition());
        completedFetch.drain(subscriptions);

        return Fetch.empty();
    }

    protected List<TopicPartition> fetchablePartitions(SubscriptionState subscriptions) {
        Set<TopicPartition> exclude = new HashSet<>();
        if (nextInLineFetch != null && !nextInLineFetch.isConsumed()) {
            exclude.add(nextInLineFetch.partition());
        }
        for (CompletedFetch<K, V> completedFetch : completedFetches) {
            exclude.add(completedFetch.partition());
        }
        return subscriptions.fetchablePartitions(tp -> !exclude.contains(tp));
    }

    /**
     * Determine which replica to read from.
     */
    Node selectReadReplica(Metadata metadata, SubscriptionState subscriptions, TopicPartition partition, Node leaderReplica, long currentTimeMs) {
        Optional<Integer> nodeId = subscriptions.preferredReadReplica(partition, currentTimeMs);
        if (nodeId.isPresent()) {
            Optional<Node> node = nodeId.flatMap(id -> metadata.fetch().nodeIfOnline(partition, id));
            if (node.isPresent()) {
                return node.get();
            } else {
                log.trace("Not fetching from {} for partition {} since it is marked offline or is missing from our metadata," +
                          " using the leader instead.", nodeId, partition);
                // Note that this condition may happen due to stale metadata, so we clear preferred replica and
                // refresh metadata.
                requestMetadataUpdate(metadata, subscriptions, partition);
                return leaderReplica;
            }
        } else {
            return leaderReplica;
        }
    }

    /**
     * If we have seen new metadata (as tracked by {@link Metadata#updateVersion()}), then
     * we should check that all of the assignments have a valid position.
     */
    protected void validatePositionsOnMetadataChange(Metadata metadata,
                                                     SubscriptionState subscriptions) {
        int newMetadataUpdateVersion = metadata.updateVersion();
        if (metadataUpdateVersion.getAndSet(newMetadataUpdateVersion) != newMetadataUpdateVersion) {
            subscriptions.assignedPartitions().forEach(topicPartition -> {
                ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(topicPartition);
                subscriptions.maybeValidatePositionForCurrentLeader(apiVersions, topicPartition, leaderAndEpoch);
            });
        }
    }

    protected void handleSuccess(Metadata metadata,
                                 Node fetchTarget,
                                 ClientResponse resp,
                                 FetchRequestData data) {
        FetchSessionHandler handler = sessionHandler(fetchTarget.id());
        FetchResponse response = (FetchResponse) resp.responseBody();

        if (handler == null) {
            log.error("Unable to find FetchSessionHandler for node {}. Ignoring fetch response.",
                fetchTarget.id());
            return;
        }

        if (!handler.handleResponse(response, resp.requestHeader().apiVersion())) {
            if (response.error() == Errors.FETCH_SESSION_TOPIC_ID_ERROR) {
                metadata.requestUpdate();
            }

            return;
        }

        Map<TopicPartition, PartitionData> responseData = response.responseData(handler.sessionTopicNames(), resp.requestHeader().apiVersion());
        Set<TopicPartition> partitions = new HashSet<>(responseData.keySet());
        FetchResponseMetricAggregator metricAggregator = new FetchResponseMetricAggregator(fetchContext.fetchManagerMetrics, partitions);
        short responseVersion = resp.requestHeader().apiVersion();

        for (Map.Entry<TopicPartition, PartitionData> entry : responseData.entrySet()) {
            TopicPartition partition = entry.getKey();
            FetchRequest.PartitionData requestData = data.sessionPartitions().get(partition);

            if (requestData == null) {
                String message;

                if (data.metadata().isFull()) {
                    message = MessageFormatter.arrayFormat(
                        "Response for missing full request partition: partition={}; metadata={}",
                        new Object[]{partition, data.metadata()}).getMessage();
                } else {
                    message = MessageFormatter.arrayFormat(
                        "Response for missing session request partition: partition={}; metadata={}; toSend={}; toForget={}; toReplace={}",
                        new Object[]{partition, data.metadata(), data.toSend(), data.toForget(), data.toReplace()}).getMessage();
                }

                // Received fetch response for missing session partition
                throw new IllegalStateException(message);
            }

            long fetchOffset = requestData.fetchOffset;
            PartitionData partitionData = entry.getValue();

            log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
                fetchContext.isolationLevel, fetchOffset, partition, partitionData);

            Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partitionData).batches().iterator();
            CompletedFetch<K, V> completedFetch = new CompletedFetch<>(fetchContext,
                partition,
                partitionData,
                metricAggregator,
                batches,
                fetchOffset,
                responseVersion);

            completedFetches.add(completedFetch);
        }

        fetchContext.fetchManagerMetrics.fetchLatency.record(resp.requestLatencyMs());
    }

    protected void handleError(final SubscriptionState subscriptions,
                               final Node fetchTarget,
                               final Throwable t) {
        FetchSessionHandler handler = sessionHandler(fetchTarget.id());

        if (handler != null) {
            handler.handleError(t);
            handler.sessionTopicPartitions().forEach(subscriptions::clearPreferredReadReplica);
        }
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions
     * that have no existing requests in flight.
     */
    protected Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests(Metadata metadata, SubscriptionState subscriptions) {
        // Update metrics in case there was an assignment change
        fetchContext.fetchManagerMetrics.maybeUpdateAssignment(subscriptions);

        Map<Node, FetchSessionHandler.Builder> fetchable = new LinkedHashMap<>();

        validatePositionsOnMetadataChange(metadata, subscriptions);

        long currentTimeMs = fetchContext.time.milliseconds();
        Map<String, Uuid> topicIds = metadata.topicIds();

        for (TopicPartition partition : fetchablePartitions(subscriptions)) {
            FetchPosition position = subscriptions.position(partition);
            if (position == null) {
                throw new IllegalStateException("Missing position for fetchable partition " + partition);
            }

            Optional<Node> leaderOpt = position.currentLeader.leader;
            if (!leaderOpt.isPresent()) {
                log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
                metadata.requestUpdate();
                continue;
            }

            // Use the preferred read replica if set, otherwise the partition's leader
            Node node = selectReadReplica(metadata, subscriptions, partition, leaderOpt.get(), currentTimeMs);
            if (isUnavailable(node)) {
                maybeThrowAuthFailure(node);

                // If we try to send during the reconnect backoff window, then the request is just
                // going to be failed anyway before being sent, so skip the send for now
                log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
            } else if (this.nodesWithPendingFetchRequests.contains(node.id())) {
                log.trace("Skipping fetch for partition {} because previous request to {} has not been processed", partition, node);
            } else {
                // if there is a leader and no in-flight requests, issue a new fetch
                FetchSessionHandler.Builder builder = fetchable.get(node);
                if (builder == null) {
                    int id = node.id();
                    FetchSessionHandler handler = sessionHandler(id);
                    if (handler == null) {
                        handler = new FetchSessionHandler(fetchContext.logContext, id);
                        sessionHandlers.put(id, handler);
                    }
                    builder = handler.newBuilder();
                    fetchable.put(node, builder);
                }
                builder.add(partition, new FetchRequest.PartitionData(
                    topicIds.getOrDefault(partition.topic(), Uuid.ZERO_UUID),
                    position.offset,
                    FetchRequest.INVALID_LOG_START_OFFSET,
                    fetchContext.fetchSize,
                    position.currentLeader.epoch,
                    Optional.empty()));

                log.debug("Added {} fetch request for partition {} at position {} to node {}", fetchContext.isolationLevel,
                    partition, position, node);
            }
        }

        Map<Node, FetchSessionHandler.FetchRequestData> reqs = new LinkedHashMap<>();
        for (Map.Entry<Node, FetchSessionHandler.Builder> entry : fetchable.entrySet()) {
            reqs.put(entry.getKey(), entry.getValue().build());
        }
        return reqs;
    }

    /**
     * Initialize a CompletedFetch object.
     */
    protected CompletedFetch<K, V> initializeCompletedFetch(Metadata metadata, SubscriptionState subscriptions, CompletedFetch<K, V> nextCompletedFetch) {
        TopicPartition tp = nextCompletedFetch.partition();
        FetchResponseData.PartitionData partition = nextCompletedFetch.partitionData();
        long fetchOffset = nextCompletedFetch.nextFetchOffset();
        CompletedFetch<K, V> completedFetch = null;
        Errors error = Errors.forCode(partition.errorCode());

        try {
            if (!subscriptions.hasValidPosition(tp)) {
                // this can happen when a rebalance happened while fetch is still in-flight
                log.debug("Ignoring fetched records for partition {} since it no longer has valid position", tp);
            } else if (error == Errors.NONE) {
                // we are interested in this fetch only if the beginning offset matches the
                // current consumed position
                FetchPosition position = subscriptions.position(tp);
                if (position == null || position.offset != fetchOffset) {
                    log.debug("Discarding stale fetch response for partition {} since its offset {} does not match " +
                            "the expected offset {}", tp, fetchOffset, position);
                    return null;
                }

                log.trace("Preparing to read {} bytes of data for partition {} with offset {}",
                        FetchResponse.recordsSize(partition), tp, position);
                Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partition).batches().iterator();
                completedFetch = nextCompletedFetch;

                if (!batches.hasNext() && FetchResponse.recordsSize(partition) > 0) {
                    if (completedFetch.responseVersion() < 3) {
                        // Implement the pre KIP-74 behavior of throwing a RecordTooLargeException.
                        Map<TopicPartition, Long> recordTooLargePartitions = Collections.singletonMap(tp, fetchOffset);
                        throw new RecordTooLargeException("There are some messages at [Partition=Offset]: " +
                                recordTooLargePartitions + " whose size is larger than the fetch size " + fetchContext.fetchSize +
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
                    subscriptions.updatePreferredReadReplica(completedFetch.partition(), partition.preferredReadReplica(), () -> {
                        long expireTimeMs = fetchContext.time.milliseconds() + metadata.metadataExpireMs();
                        log.debug("Updating preferred read replica for partition {} to {}, set to expire at {}",
                                tp, partition.preferredReadReplica(), expireTimeMs);
                        return expireTimeMs;
                    });
                }

                nextCompletedFetch.setInitialized(true);
            } else if (error == Errors.NOT_LEADER_OR_FOLLOWER ||
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
                    FetchPosition position = subscriptions.position(tp);
                    if (position == null || fetchOffset != position.offset) {
                        log.debug("Discarding stale fetch response for partition {} since the fetched offset {} " +
                                "does not match the current offset {}", tp, fetchOffset, position);
                    } else {
                        handleOffsetOutOfRange(subscriptions, position, tp);
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
        } finally {
            if (completedFetch == null)
                nextCompletedFetch.recordMetrics(0, 0);

            if (error != Errors.NONE)
                // we move the partition to the end if there was an error. This way, it's more likely that partitions for
                // the same topic can remain together (allowing for more efficient serialization).
                subscriptions.movePartitionToEnd(tp);
        }

        return completedFetch;
    }

    private void handleOffsetOutOfRange(SubscriptionState subscriptions, FetchPosition fetchPosition, TopicPartition topicPartition) {
        String errorMessage = "Fetch position " + fetchPosition + " is out of range for partition " + topicPartition;
        if (subscriptions.hasDefaultOffsetResetPolicy()) {
            log.info("{}, resetting offset", errorMessage);
            subscriptions.requestOffsetReset(topicPartition);
        } else {
            log.info("{}, raising error to the application since no reset policy is configured", errorMessage);
            throw new OffsetOutOfRangeException(errorMessage,
                Collections.singletonMap(topicPartition, fetchPosition.offset));
        }
    }

    /**
     * Clear the buffered data which are not a part of newly assigned partitions
     *
     * @param assignedPartitions  newly assigned {@link TopicPartition}
     */
    public void clearBufferedDataForUnassignedPartitions(SubscriptionState subscriptions, Collection<TopicPartition> assignedPartitions) {
        Iterator<CompletedFetch<K, V>> completedFetchesItr = completedFetches.iterator();
        while (completedFetchesItr.hasNext()) {
            CompletedFetch<K, V> records = completedFetchesItr.next();
            TopicPartition tp = records.partition();
            if (!assignedPartitions.contains(tp)) {
                records.drain(subscriptions);
                completedFetchesItr.remove();
            }
        }

        if (nextInLineFetch != null && !assignedPartitions.contains(nextInLineFetch.partition())) {
            nextInLineFetch.drain(subscriptions);
            nextInLineFetch = null;
        }
    }

    /**
     * Clear the buffered data which are not a part of newly assigned topics
     *
     * @param assignedTopics  newly assigned topics
     */
    public void clearBufferedDataForUnassignedTopics(SubscriptionState subscriptions, Collection<String> assignedTopics) {
        Set<TopicPartition> currentTopicPartitions = new HashSet<>();
        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            if (assignedTopics.contains(tp.topic())) {
                currentTopicPartitions.add(tp);
            }
        }
        clearBufferedDataForUnassignedPartitions(subscriptions, currentTopicPartitions);
    }

    // Visible for testing
    protected FetchSessionHandler sessionHandler(int node) {
        return sessionHandlers.get(node);
    }

    public static Sensor throttleTimeSensor(Metrics metrics, FetcherMetricsRegistry metricsRegistry) {
        Sensor fetchThrottleTimeSensor = metrics.sensor("fetch-throttle-time");
        fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeAvg), new Avg());

        fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeMax), new Max());

        return fetchThrottleTimeSensor;
    }

    public void close(SubscriptionState subscriptions) {
        if (nextInLineFetch != null)
            nextInLineFetch.drain(subscriptions);

        fetchContext.decompressionBufferSupplier.close();
    }

    protected void requestMetadataUpdate(Metadata metadata, SubscriptionState subscriptions, TopicPartition topicPartition) {
        metadata.requestUpdate();
        subscriptions.clearPreferredReadReplica(topicPartition);
    }

}
