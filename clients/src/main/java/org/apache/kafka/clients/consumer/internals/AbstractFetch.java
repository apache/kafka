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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.ArrayList;
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

/**
 * {@code AbstractFetch} represents the basic state and logic for record fetching processing.
 * @param <K> Type for the message key
 * @param <V> Type for the message value
 */
public abstract class AbstractFetch<K, V> implements Closeable {

    private final Logger log;
    protected final LogContext logContext;
    protected final ConsumerNetworkClient client;
    protected final ConsumerMetadata metadata;
    protected final SubscriptionState subscriptions;
    protected final FetchConfig<K, V> fetchConfig;
    protected final Time time;
    protected final FetchMetricsManager metricsManager;

    private final BufferSupplier decompressionBufferSupplier;
    private final ConcurrentLinkedQueue<CompletedFetch<K, V>> completedFetches;
    private final Map<Integer, FetchSessionHandler> sessionHandlers;
    private final Set<Integer> nodesWithPendingFetchRequests;

    private CompletedFetch<K, V> nextInLineFetch;

    public AbstractFetch(final LogContext logContext,
                         final ConsumerNetworkClient client,
                         final ConsumerMetadata metadata,
                         final SubscriptionState subscriptions,
                         final FetchConfig<K, V> fetchConfig,
                         final FetchMetricsManager metricsManager,
                         final Time time) {
        this.log = logContext.logger(AbstractFetch.class);
        this.logContext = logContext;
        this.client = client;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.fetchConfig = fetchConfig;
        this.decompressionBufferSupplier = BufferSupplier.create();
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.sessionHandlers = new HashMap<>();
        this.nodesWithPendingFetchRequests = new HashSet<>();
        this.metricsManager = metricsManager;
        this.time = time;
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     *
     * @return true if there are completed fetches, false otherwise
     */
    boolean hasCompletedFetches() {
        return !completedFetches.isEmpty();
    }

    /**
     * Return whether we have any completed fetches that are fetchable. This method is thread-safe.
     * @return true if there are completed fetches that can be returned, false otherwise
     */
    public boolean hasAvailableFetches() {
        return completedFetches.stream().anyMatch(fetch -> subscriptions.isFetchable(fetch.partition));
    }

    /**
     * Implements the core logic for a successful fetch request/response.
     *
     * @param fetchTarget {@link Node} from which the fetch data was requested
     * @param data {@link FetchSessionHandler.FetchRequestData} that represents the session data
     * @param resp {@link ClientResponse} from which the {@link FetchResponse} will be retrieved
     */
    protected void handleFetchResponse(final Node fetchTarget,
                                       final FetchSessionHandler.FetchRequestData data,
                                       final ClientResponse resp) {
        try {
            final FetchResponse response = (FetchResponse) resp.responseBody();
            final FetchSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler == null) {
                log.error("Unable to find FetchSessionHandler for node {}. Ignoring fetch response.",
                        fetchTarget.id());
                return;
            }

            final short requestVersion = resp.requestHeader().apiVersion();

            if (!handler.handleResponse(response, requestVersion)) {
                if (response.error() == Errors.FETCH_SESSION_TOPIC_ID_ERROR) {
                    metadata.requestUpdate();
                }

                return;
            }

            final Map<TopicPartition, FetchResponseData.PartitionData> responseData = response.responseData(handler.sessionTopicNames(), requestVersion);
            final Set<TopicPartition> partitions = new HashSet<>(responseData.keySet());
            final FetchMetricsAggregator metricAggregator = new FetchMetricsAggregator(metricsManager, partitions);

            for (Map.Entry<TopicPartition, FetchResponseData.PartitionData> entry : responseData.entrySet()) {
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
                FetchResponseData.PartitionData partitionData = entry.getValue();

                log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
                        fetchConfig.isolationLevel, fetchOffset, partition, partitionData);

                CompletedFetch<K, V> completedFetch = new CompletedFetch<>(
                        logContext,
                        subscriptions,
                        fetchConfig,
                        decompressionBufferSupplier,
                        partition,
                        partitionData,
                        metricAggregator,
                        fetchOffset,
                        requestVersion);
                completedFetches.add(completedFetch);
            }

            metricsManager.recordLatency(resp.requestLatencyMs());
        } finally {
            log.debug("Removing pending request for node {}", fetchTarget);
            nodesWithPendingFetchRequests.remove(fetchTarget.id());
        }
    }

    /**
     * Implements the core logic for a failed fetch request/response.
     *
     * @param fetchTarget {@link Node} from which the fetch data was requested
     * @param e {@link RuntimeException} representing the error that resulted in the failure
     */
    protected void handleFetchResponse(final Node fetchTarget, final RuntimeException e) {
        try {
            final FetchSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler != null) {
                handler.handleError(e);
                handler.sessionTopicPartitions().forEach(subscriptions::clearPreferredReadReplica);
            }
        } finally {
            log.debug("Removing pending request for node {}", fetchTarget);
            nodesWithPendingFetchRequests.remove(fetchTarget.id());
        }
    }

    /**
     * Creates a new {@link FetchRequest fetch request} in preparation for sending to the Kafka cluster.
     *
     * @param fetchTarget {@link Node} from which the fetch data will be requested
     * @param requestData {@link FetchSessionHandler.FetchRequestData} that represents the session data
     * @return {@link FetchRequest.Builder} that can be submitted to the broker
     */
    protected FetchRequest.Builder createFetchRequest(final Node fetchTarget,
                                                      final FetchSessionHandler.FetchRequestData requestData) {
        // Version 12 is the maximum version that could be used without topic IDs. See FetchRequest.json for schema
        // changelog.
        final short maxVersion = requestData.canUseTopicIds() ? ApiKeys.FETCH.latestVersion() : (short) 12;

        final FetchRequest.Builder request = FetchRequest.Builder
                .forConsumer(maxVersion, fetchConfig.maxWaitMs, fetchConfig.minBytes, requestData.toSend())
                .isolationLevel(fetchConfig.isolationLevel)
                .setMaxBytes(fetchConfig.maxBytes)
                .metadata(requestData.metadata())
                .removed(requestData.toForget())
                .replaced(requestData.toReplace())
                .rackId(fetchConfig.clientRackId);

        log.debug("Sending {} {} to broker {}", fetchConfig.isolationLevel, requestData, fetchTarget);

        // We add the node to the set of nodes with pending fetch requests before adding the
        // listener because the future may have been fulfilled on another thread (e.g. during a
        // disconnection being handled by the heartbeat thread) which will mean the listener
        // will be invoked synchronously.
        log.debug("Adding pending request for node {}", fetchTarget);
        nodesWithPendingFetchRequests.add(fetchTarget.id());

        return request;
    }

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     *
     * </p>
     *
     * NOTE: returning an {@link Fetch#isEmpty empty} fetch guarantees the consumed position is not updated.
     *
     * @return A {@link Fetch} for the requested partitions
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     *         the defaultResetPolicy is NONE
     * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
     */
    public Fetch<K, V> collectFetch() {
        Fetch<K, V> fetch = Fetch.empty();
        Queue<CompletedFetch<K, V>> pausedCompletedFetches = new ArrayDeque<>();
        int recordsRemaining = fetchConfig.maxPollRecords;

        try {
            while (recordsRemaining > 0) {
                if (nextInLineFetch == null || nextInLineFetch.isConsumed) {
                    CompletedFetch<K, V> records = completedFetches.peek();
                    if (records == null) break;

                    if (!records.initialized) {
                        try {
                            nextInLineFetch = initializeCompletedFetch(records);
                        } catch (Exception e) {
                            // Remove a completedFetch upon a parse with exception if (1) it contains no records, and
                            // (2) there are no fetched records with actual content preceding this exception.
                            // The first condition ensures that the completedFetches is not stuck with the same completedFetch
                            // in cases such as the TopicAuthorizationException, and the second condition ensures that no
                            // potential data loss due to an exception in a following record.
                            if (fetch.isEmpty() && FetchResponse.recordsOrFail(records.partitionData).sizeInBytes() == 0) {
                                completedFetches.poll();
                            }
                            throw e;
                        }
                    } else {
                        nextInLineFetch = records;
                    }
                    completedFetches.poll();
                } else if (subscriptions.isPaused(nextInLineFetch.partition)) {
                    // when the partition is paused we add the records back to the completedFetches queue instead of draining
                    // them so that they can be returned on a subsequent poll if the partition is resumed at that time
                    log.debug("Skipping fetching records for assigned partition {} because it is paused", nextInLineFetch.partition);
                    pausedCompletedFetches.add(nextInLineFetch);
                    nextInLineFetch = null;
                } else {
                    Fetch<K, V> nextFetch = fetchRecords(recordsRemaining);
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

    private Fetch<K, V> fetchRecords(final int maxRecords) {
        if (!subscriptions.isAssigned(nextInLineFetch.partition)) {
            // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition {} since it is no longer assigned",
                    nextInLineFetch.partition);
        } else if (!subscriptions.isFetchable(nextInLineFetch.partition)) {
            // this can happen when a partition is paused before fetched records are returned to the consumer's
            // poll call or if the offset is being reset
            log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable",
                    nextInLineFetch.partition);
        } else {
            SubscriptionState.FetchPosition position = subscriptions.position(nextInLineFetch.partition);
            if (position == null) {
                throw new IllegalStateException("Missing position for fetchable partition " + nextInLineFetch.partition);
            }

            if (nextInLineFetch.nextFetchOffset == position.offset) {
                List<ConsumerRecord<K, V>> partRecords = nextInLineFetch.fetchRecords(maxRecords);

                log.trace("Returning {} fetched records at offset {} for assigned partition {}",
                        partRecords.size(), position, nextInLineFetch.partition);

                boolean positionAdvanced = false;

                if (nextInLineFetch.nextFetchOffset > position.offset) {
                    SubscriptionState.FetchPosition nextPosition = new SubscriptionState.FetchPosition(
                            nextInLineFetch.nextFetchOffset,
                            nextInLineFetch.lastEpoch,
                            position.currentLeader);
                    log.trace("Updating fetch position from {} to {} for partition {} and returning {} records from `poll()`",
                            position, nextPosition, nextInLineFetch.partition, partRecords.size());
                    subscriptions.position(nextInLineFetch.partition, nextPosition);
                    positionAdvanced = true;
                }

                Long partitionLag = subscriptions.partitionLag(nextInLineFetch.partition, fetchConfig.isolationLevel);
                if (partitionLag != null)
                    metricsManager.recordPartitionLag(nextInLineFetch.partition, partitionLag);

                Long lead = subscriptions.partitionLead(nextInLineFetch.partition);
                if (lead != null) {
                    metricsManager.recordPartitionLead(nextInLineFetch.partition, lead);
                }

                return Fetch.forPartition(nextInLineFetch.partition, partRecords, positionAdvanced);
            } else {
                // these records aren't next in line based on the last consumed position, ignore them
                // they must be from an obsolete request
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        nextInLineFetch.partition, nextInLineFetch.nextFetchOffset, position);
            }
        }

        log.trace("Draining fetched records for partition {}", nextInLineFetch.partition);
        nextInLineFetch.drain();

        return Fetch.empty();
    }

    private List<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> exclude = new HashSet<>();
        if (nextInLineFetch != null && !nextInLineFetch.isConsumed) {
            exclude.add(nextInLineFetch.partition);
        }
        for (CompletedFetch<K, V> completedFetch : completedFetches) {
            exclude.add(completedFetch.partition);
        }
        return subscriptions.fetchablePartitions(tp -> !exclude.contains(tp));
    }

    /**
     * Determine from which replica to read: the <i>preferred</i> or the <i>leader</i>. The preferred replica is used
     * iff:
     *
     * <ul>
     *     <li>A preferred replica was previously set</li>
     *     <li>We're still within the lease time for the preferred replica</li>
     *     <li>The replica is still online/available</li>
     * </ul>
     *
     * If any of the above are not met, the leader node is returned.
     *
     * @param partition {@link TopicPartition} for which we want to fetch data
     * @param leaderReplica {@link Node} for the leader of the given partition
     * @param currentTimeMs Current time in milliseconds; used to determine if we're within the optional lease window
     * @return Replic {@link Node node} from which to request the data
     * @see SubscriptionState#preferredReadReplica
     * @see SubscriptionState#updatePreferredReadReplica
     */
    Node selectReadReplica(final TopicPartition partition, final Node leaderReplica, final long currentTimeMs) {
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
                requestMetadataUpdate(partition);
                return leaderReplica;
            }
        } else {
            return leaderReplica;
        }
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions
     * that have no existing requests in flight.
     */
    protected Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests() {
        // Update metrics in case there was an assignment change
        metricsManager.maybeUpdateAssignment(subscriptions);

        Map<Node, FetchSessionHandler.Builder> fetchable = new LinkedHashMap<>();
        long currentTimeMs = time.milliseconds();
        Map<String, Uuid> topicIds = metadata.topicIds();

        for (TopicPartition partition : fetchablePartitions()) {
            SubscriptionState.FetchPosition position = subscriptions.position(partition);

            if (position == null)
                throw new IllegalStateException("Missing position for fetchable partition " + partition);

            Optional<Node> leaderOpt = position.currentLeader.leader;

            if (!leaderOpt.isPresent()) {
                log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
                metadata.requestUpdate();
                continue;
            }

            // Use the preferred read replica if set, otherwise the partition's leader
            Node node = selectReadReplica(partition, leaderOpt.get(), currentTimeMs);

            if (client.isUnavailable(node)) {
                client.maybeThrowAuthFailure(node);

                // If we try to send during the reconnect backoff window, then the request is just
                // going to be failed anyway before being sent, so skip sending the request for now
                log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
            } else if (nodesWithPendingFetchRequests.contains(node.id())) {
                log.trace("Skipping fetch for partition {} because previous request to {} has not been processed", partition, node);
            } else {
                // if there is a leader and no in-flight requests, issue a new fetch
                FetchSessionHandler.Builder builder = fetchable.computeIfAbsent(node, k -> {
                    FetchSessionHandler fetchSessionHandler = sessionHandlers.computeIfAbsent(node.id(), n -> new FetchSessionHandler(logContext, n));
                    return fetchSessionHandler.newBuilder();
                });
                Uuid topicId = topicIds.getOrDefault(partition.topic(), Uuid.ZERO_UUID);
                FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(topicId,
                        position.offset,
                        FetchRequest.INVALID_LOG_START_OFFSET,
                        fetchConfig.fetchSize,
                        position.currentLeader.epoch,
                        Optional.empty());
                builder.add(partition, partitionData);

                log.debug("Added {} fetch request for partition {} at position {} to node {}", fetchConfig.isolationLevel,
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
    private CompletedFetch<K, V> initializeCompletedFetch(final CompletedFetch<K, V> completedFetch) {
        final TopicPartition tp = completedFetch.partition;
        final Errors error = Errors.forCode(completedFetch.partitionData.errorCode());
        boolean recordMetrics = true;

        try {
            if (!subscriptions.hasValidPosition(tp)) {
                // this can happen when a rebalance happened while fetch is still in-flight
                log.debug("Ignoring fetched records for partition {} since it no longer has valid position", tp);
                return null;
            } else if (error == Errors.NONE) {
                final CompletedFetch<K, V> ret = handleInitializeCompletedFetchSuccess(completedFetch);
                recordMetrics = ret == null;
                return ret;
            } else {
                handleInitializeCompletedFetchErrors(completedFetch, error);
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

    private CompletedFetch<K, V> handleInitializeCompletedFetchSuccess(final CompletedFetch<K, V> completedFetch) {
        final TopicPartition tp = completedFetch.partition;
        final long fetchOffset = completedFetch.nextFetchOffset;

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

        completedFetch.initialized = true;
        return completedFetch;
    }

    private void handleInitializeCompletedFetchErrors(final CompletedFetch<K, V> completedFetch,
                                                      final Errors error) {
        final TopicPartition tp = completedFetch.partition;
        final long fetchOffset = completedFetch.nextFetchOffset;

        if (error == Errors.NOT_LEADER_OR_FOLLOWER ||
                error == Errors.REPLICA_NOT_AVAILABLE ||
                error == Errors.KAFKA_STORAGE_ERROR ||
                error == Errors.FENCED_LEADER_EPOCH ||
                error == Errors.OFFSET_NOT_AVAILABLE) {
            log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
            requestMetadataUpdate(tp);
        } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            log.warn("Received unknown topic or partition error in fetch for partition {}", tp);
            requestMetadataUpdate(tp);
        } else if (error == Errors.UNKNOWN_TOPIC_ID) {
            log.warn("Received unknown topic ID error in fetch for partition {}", tp);
            requestMetadataUpdate(tp);
        } else if (error == Errors.INCONSISTENT_TOPIC_ID) {
            log.warn("Received inconsistent topic ID error in fetch for partition {}", tp);
            requestMetadataUpdate(tp);
        } else if (error == Errors.OFFSET_OUT_OF_RANGE) {
            Optional<Integer> clearedReplicaId = subscriptions.clearPreferredReadReplica(tp);

            if (!clearedReplicaId.isPresent()) {
                // If there's no preferred replica to clear, we're fetching from the leader so handle this error normally
                SubscriptionState.FetchPosition position = subscriptions.position(tp);

                if (position == null || fetchOffset != position.offset) {
                    log.debug("Discarding stale fetch response for partition {} since the fetched offset {} " +
                            "does not match the current offset {}", tp, fetchOffset, position);
                } else {
                    handleOffsetOutOfRange(position, tp);
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

    private void handleOffsetOutOfRange(final SubscriptionState.FetchPosition fetchPosition,
                                        final TopicPartition topicPartition) {
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
     * Clear the buffered data which are not a part of newly assigned partitions. Any previously
     * {@link CompletedFetch fetched data} is dropped if it is for a partition that is no longer in
     * {@code assignedPartitions}.
     *
     * @param assignedPartitions Newly-assigned {@link TopicPartition}
     */
    public void clearBufferedDataForUnassignedPartitions(final Collection<TopicPartition> assignedPartitions) {
        final Iterator<CompletedFetch<K, V>> completedFetchesItr = completedFetches.iterator();

        while (completedFetchesItr.hasNext()) {
            final CompletedFetch<K, V> completedFetch = completedFetchesItr.next();
            final TopicPartition tp = completedFetch.partition;

            if (!assignedPartitions.contains(tp)) {
                log.debug("Removing {} from buffered data as it is no longer an assigned partition", tp);
                completedFetch.drain();
                completedFetchesItr.remove();
            }
        }

        if (nextInLineFetch != null && !assignedPartitions.contains(nextInLineFetch.partition)) {
            nextInLineFetch.drain();
            nextInLineFetch = null;
        }
    }

    /**
     * Clear the buffered data which are not a part of newly assigned topics
     *
     * @param assignedTopics  newly assigned topics
     */
    public void clearBufferedDataForUnassignedTopics(Collection<String> assignedTopics) {
        final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            if (assignedTopics.contains(tp.topic())) {
                currentTopicPartitions.add(tp);
            }
        }

        clearBufferedDataForUnassignedPartitions(currentTopicPartitions);
    }

    protected FetchSessionHandler sessionHandler(int node) {
        return sessionHandlers.get(node);
    }

    // Visible for testing
    void maybeCloseFetchSessions(final Timer timer) {
        final Cluster cluster = metadata.fetch();
        final List<RequestFuture<ClientResponse>> requestFutures = new ArrayList<>();

        sessionHandlers.forEach((fetchTargetNodeId, sessionHandler) -> {
            // set the session handler to notify close. This will set the next metadata request to send close message.
            sessionHandler.notifyClose();

            final int sessionId = sessionHandler.sessionId();
            // FetchTargetNode may not be available as it may have disconnected the connection. In such cases, we will
            // skip sending the close request.
            final Node fetchTarget = cluster.nodeById(fetchTargetNodeId);
            if (fetchTarget == null || client.isUnavailable(fetchTarget)) {
                log.debug("Skip sending close session request to broker {} since it is not reachable", fetchTarget);
                return;
            }

            final FetchRequest.Builder request = createFetchRequest(fetchTarget, sessionHandler.newBuilder().build());
            final RequestFuture<ClientResponse> responseFuture = client.send(fetchTarget, request);

            responseFuture.addListener(new RequestFutureListener<ClientResponse>() {
                @Override
                public void onSuccess(ClientResponse value) {
                    log.debug("Successfully sent a close message for fetch session: {} to node: {}", sessionId, fetchTarget);
                }

                @Override
                public void onFailure(RuntimeException e) {
                    log.debug("Unable to a close message for fetch session: {} to node: {}. " +
                            "This may result in unnecessary fetch sessions at the broker.", sessionId, fetchTarget, e);
                }
            });

            requestFutures.add(responseFuture);
        });

        // Poll to ensure that request has been written to the socket. Wait until either the timer has expired or until
        // all requests have received a response.
        while (timer.notExpired() && !requestFutures.stream().allMatch(RequestFuture::isDone)) {
            client.poll(timer, null, true);
        }

        if (!requestFutures.stream().allMatch(RequestFuture::isDone)) {
            // we ran out of time before completing all futures. It is ok since we don't want to block the shutdown
            // here.
            log.debug("All requests couldn't be sent in the specific timeout period {}ms. " +
                    "This may result in unnecessary fetch sessions at the broker. Consider increasing the timeout passed for " +
                    "KafkaConsumer.close(Duration timeout)", timer.timeoutMs());
        }
    }

    public void close(final Timer timer) {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();

        if (nextInLineFetch != null) {
            nextInLineFetch.drain();
            nextInLineFetch = null;
        }

        maybeCloseFetchSessions(timer);
        Utils.closeQuietly(decompressionBufferSupplier, "decompressionBufferSupplier");
        sessionHandlers.clear();
    }

    @Override
    public void close() {
        close(time.timer(0));
    }

    private void requestMetadataUpdate(final TopicPartition topicPartition) {
        metadata.requestUpdate();
        subscriptions.clearPreferredReadReplica(topicPartition);
    }
}