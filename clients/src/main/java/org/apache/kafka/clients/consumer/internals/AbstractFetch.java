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
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
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
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.FetchUtils.requestMetadataUpdate;

/**
 * {@code AbstractFetch} represents the basic state and logic for record fetching processing.
 */
public abstract class AbstractFetch implements Closeable {

    private final Logger log;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();
    protected final LogContext logContext;
    protected final ConsumerMetadata metadata;
    protected final SubscriptionState subscriptions;
    protected final FetchConfig fetchConfig;
    protected final Time time;
    protected final FetchMetricsManager metricsManager;
    protected final FetchBuffer fetchBuffer;
    protected final BufferSupplier decompressionBufferSupplier;
    protected final Set<Integer> nodesWithPendingFetchRequests;

    private final Map<Integer, FetchSessionHandler> sessionHandlers;

    private final ApiVersions apiVersions;

    public AbstractFetch(final LogContext logContext,
                         final ConsumerMetadata metadata,
                         final SubscriptionState subscriptions,
                         final FetchConfig fetchConfig,
                         final FetchBuffer fetchBuffer,
                         final FetchMetricsManager metricsManager,
                         final Time time,
                         final ApiVersions apiVersions) {
        this.log = logContext.logger(AbstractFetch.class);
        this.logContext = logContext;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.fetchConfig = fetchConfig;
        this.fetchBuffer = fetchBuffer;
        this.decompressionBufferSupplier = BufferSupplier.create();
        this.sessionHandlers = new HashMap<>();
        this.nodesWithPendingFetchRequests = new HashSet<>();
        this.metricsManager = metricsManager;
        this.time = time;
        this.apiVersions = apiVersions;
    }

    /**
     * Check if the node is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     *
     * @param node {@link Node} to check for availability
     * @see NetworkClientUtils#isUnavailable(KafkaClient, Node, Time)
     */
    protected abstract boolean isUnavailable(Node node);

    /**
     * Checks for an authentication error on a given node and throws the exception if it exists.
     *
     * @param node {@link Node} to check for a previous {@link AuthenticationException}; if found it is thrown
     * @see NetworkClientUtils#maybeThrowAuthFailure(KafkaClient, Node)
     */
    protected abstract void maybeThrowAuthFailure(Node node);

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     *
     * @return true if there are completed fetches, false otherwise
     */
    boolean hasCompletedFetches() {
        return !fetchBuffer.isEmpty();
    }

    /**
     * Return whether we have any completed fetches that are fetchable. This method is thread-safe.
     * @return true if there are completed fetches that can be returned, false otherwise
     */
    public boolean hasAvailableFetches() {
        return fetchBuffer.hasCompletedFetches(fetch -> subscriptions.isFetchable(fetch.partition));
    }

    /**
     * Implements the core logic for a successful fetch response.
     *
     * @param fetchTarget {@link Node} from which the fetch data was requested
     * @param data {@link FetchSessionHandler.FetchRequestData} that represents the session data
     * @param resp {@link ClientResponse} from which the {@link FetchResponse} will be retrieved
     */
    protected void handleFetchSuccess(final Node fetchTarget,
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
                    metadata.requestUpdate(false);
                }

                return;
            }

            final Map<TopicPartition, FetchResponseData.PartitionData> responseData = response.responseData(handler.sessionTopicNames(), requestVersion);
            final Set<TopicPartition> partitions = new HashSet<>(responseData.keySet());
            final FetchMetricsAggregator metricAggregator = new FetchMetricsAggregator(metricsManager, partitions);

            Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();
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

                Errors partitionError = Errors.forCode(partitionData.errorCode());
                if (partitionError == Errors.NOT_LEADER_OR_FOLLOWER || partitionError == Errors.FENCED_LEADER_EPOCH) {
                    log.debug("For {}, received error {}, with leaderIdAndEpoch {}", partition, partitionError, partitionData.currentLeader());
                    if (partitionData.currentLeader().leaderId() != -1 && partitionData.currentLeader().leaderEpoch() != -1) {
                        partitionsWithUpdatedLeaderInfo.put(partition, new Metadata.LeaderIdAndEpoch(
                            Optional.of(partitionData.currentLeader().leaderId()), Optional.of(partitionData.currentLeader().leaderEpoch())));
                    }
                }

                CompletedFetch completedFetch = new CompletedFetch(
                        logContext,
                        subscriptions,
                        decompressionBufferSupplier,
                        partition,
                        partitionData,
                        metricAggregator,
                        fetchOffset,
                        requestVersion);
                fetchBuffer.add(completedFetch);
            }

            if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                List<Node> leaderNodes = response.data().nodeEndpoints().stream()
                    .map(e -> new Node(e.nodeId(), e.host(), e.port(), e.rack()))
                    .filter(e -> !e.equals(Node.noNode()))
                    .collect(Collectors.toList());
                Set<TopicPartition> updatedPartitions = metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
                updatedPartitions.forEach(
                    tp -> {
                        log.debug("For {}, as the leader was updated, position will be validated.", tp);
                        subscriptions.maybeValidatePositionForCurrentLeader(apiVersions, tp, metadata.currentLeader(tp));
                    }
                );
            }

            metricsManager.recordLatency(resp.destination(), resp.requestLatencyMs());
        } finally {
            removePendingFetchRequest(fetchTarget, data.metadata().sessionId());
        }
    }

    /**
     * Implements the core logic for a failed fetch response.
     *
     * @param fetchTarget {@link Node} from which the fetch data was requested
     * @param data        {@link FetchSessionHandler.FetchRequestData} from request
     * @param t           {@link Throwable} representing the error that resulted in the failure
     */
    protected void handleFetchFailure(final Node fetchTarget,
                                      final FetchSessionHandler.FetchRequestData data,
                                      final Throwable t) {
        try {
            final FetchSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler != null) {
                handler.handleError(t);
                handler.sessionTopicPartitions().forEach(subscriptions::clearPreferredReadReplica);
            }
        } finally {
            removePendingFetchRequest(fetchTarget, data.metadata().sessionId());
        }
    }

    protected void handleCloseFetchSessionSuccess(final Node fetchTarget,
                                                  final FetchSessionHandler.FetchRequestData data,
                                                  final ClientResponse ignored) {
        int sessionId = data.metadata().sessionId();
        removePendingFetchRequest(fetchTarget, sessionId);
        log.debug("Successfully sent a close message for fetch session: {} to node: {}", sessionId, fetchTarget);
    }

    public void handleCloseFetchSessionFailure(final Node fetchTarget,
                                               final FetchSessionHandler.FetchRequestData data,
                                               final Throwable t) {
        int sessionId = data.metadata().sessionId();
        removePendingFetchRequest(fetchTarget, sessionId);
        log.debug("Unable to send a close message for fetch session: {} to node: {}. " +
                "This may result in unnecessary fetch sessions at the broker.", sessionId, fetchTarget, t);
    }

    private void removePendingFetchRequest(Node fetchTarget, int sessionId) {
        log.debug("Removing pending request for fetch session: {} for node: {}", sessionId, fetchTarget);
        nodesWithPendingFetchRequests.remove(fetchTarget.id());
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
     * Return the set of <em>fetchable</em> partitions, which are the set of partitions to which we are subscribed,
     * but <em>excluding</em> any partitions for which we still have buffered data. The idea is that since the user
     * has yet to process the data for the partition that has already been fetched, we should not go send for more data
     * until the previously-fetched data has been processed.
     *
     * @param buffered The set of partitions we have in our buffer
     * @return {@link Set} of {@link TopicPartition topic partitions} for which we should fetch data
     */
    private Set<TopicPartition> fetchablePartitions(Set<TopicPartition> buffered) {
        // This is the test that returns true if the partition is *not* buffered
        Predicate<TopicPartition> isNotBuffered = tp -> !buffered.contains(tp);

        // Return all partitions that are in an otherwise fetchable state *and* for which we don't already have some
        // messages sitting in our buffer.
        return new HashSet<>(subscriptions.fetchablePartitions(isNotBuffered));
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
     * @return Replica {@link Node node} from which to request the data
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
                requestMetadataUpdate(metadata, subscriptions, partition);
                return leaderReplica;
            }
        } else {
            return leaderReplica;
        }
    }

    protected Map<Node, FetchSessionHandler.FetchRequestData> prepareCloseFetchSessionRequests() {
        final Cluster cluster = metadata.fetch();
        Map<Node, FetchSessionHandler.Builder> fetchable = new HashMap<>();

        sessionHandlers.forEach((fetchTargetNodeId, sessionHandler) -> {
            // set the session handler to notify close. This will set the next metadata request to send close message.
            sessionHandler.notifyClose();

            // FetchTargetNode may not be available as it may have disconnected the connection. In such cases, we will
            // skip sending the close request.
            final Node fetchTarget = cluster.nodeById(fetchTargetNodeId);

            if (fetchTarget == null || isUnavailable(fetchTarget)) {
                log.debug("Skip sending close session request to broker {} since it is not reachable", fetchTarget);
                return;
            }

            fetchable.put(fetchTarget, sessionHandler.newBuilder());
        });

        return fetchable.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()));
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions
     * that have no existing requests in flight.
     */
    protected Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests() {
        // Update metrics in case there was an assignment change
        metricsManager.maybeUpdateAssignment(subscriptions);

        Map<Node, FetchSessionHandler.Builder> fetchable = new HashMap<>();
        long currentTimeMs = time.milliseconds();
        Map<String, Uuid> topicIds = metadata.topicIds();

        // This is the set of partitions that have buffered data
        Set<TopicPartition> buffered = Collections.unmodifiableSet(fetchBuffer.bufferedPartitions());

        // This is the set of partitions that do not have buffered data
        Set<TopicPartition> unbuffered = fetchablePartitions(buffered);

        if (unbuffered.isEmpty()) {
            // If there are no partitions that don't already have data locally buffered, there's no need to issue
            // any fetch requests at the present time.
            return Collections.emptyMap();
        }

        Set<Integer> bufferedNodes = bufferedNodes(buffered, currentTimeMs);

        for (TopicPartition partition : unbuffered) {
            SubscriptionState.FetchPosition position = positionForPartition(partition);
            Optional<Node> nodeOpt = maybeNodeForPosition(partition, position, currentTimeMs);

            if (nodeOpt.isEmpty())
                continue;

            Node node = nodeOpt.get();

            if (isUnavailable(node)) {
                maybeThrowAuthFailure(node);

                // If we try to send during the reconnect backoff window, then the request is just
                // going to be failed anyway before being sent, so skip sending the request for now
                log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
            } else if (nodesWithPendingFetchRequests.contains(node.id())) {
                // If there's already an inflight request for this node, don't issue another request.
                log.trace("Skipping fetch for partition {} because previous request to {} has not been processed", partition, node);
            } else if (bufferedNodes.contains(node.id())) {
                // While a node has buffered data, don't fetch other partition data from it. Because the buffered
                // partitions are not included in the fetch request, those partitions will be inadvertently dropped
                // from the broker fetch session cache. In some cases, that could lead to the entire fetch session
                // being evicted.
                log.trace("Skipping fetch for partition {} because its leader node {} hosts buffered partitions", partition, node);
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

        return fetchable.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()));
    }

    /**
     * Simple utility method that returns a {@link SubscriptionState.FetchPosition position} for the partition. If
     * no position exists, an {@link IllegalStateException} is thrown.
     */
    private SubscriptionState.FetchPosition positionForPartition(TopicPartition partition) {
        SubscriptionState.FetchPosition position = subscriptions.position(partition);

        if (position == null)
            throw new IllegalStateException("Missing position for fetchable partition " + partition);

        return position;
    }

    /**
     * Retrieves the node from which to fetch the partition data. If the given
     * {@link SubscriptionState.FetchPosition position} does not have a current
     * {@link Metadata.LeaderAndEpoch#leader leader} defined the method will return {@link Optional#empty()}.
     *
     * @return Three options: 1) {@link Optional#empty()} if the position's leader is empty, 2) the
     * {@link #selectReadReplica(TopicPartition, Node, long) read replica, if defined}, or 3) the position's
     * {@link Metadata.LeaderAndEpoch#leader leader}
     */
    private Optional<Node> maybeNodeForPosition(TopicPartition partition,
                                                SubscriptionState.FetchPosition position,
                                                long currentTimeMs) {
        Optional<Node> leaderOpt = position.currentLeader.leader;

        if (leaderOpt.isEmpty()) {
            log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
            metadata.requestUpdate(false);
            return Optional.empty();
        }

        // Use the preferred read replica if set, otherwise the partition's leader
        Node node = selectReadReplica(partition, leaderOpt.get(), currentTimeMs);
        return Optional.of(node);
    }

    /**
     * Returns the set of IDs for {@link Node}s to which fetch requests should <em>not</em> be sent.
     *
     * <p>
     * When a partition has buffered data in {@link FetchBuffer}, that means that at some point in the <em>past</em>,
     * the following steps occurred:
     *
     * <ol>
     *     <li>The client submitted a fetch request to the partition's leader</li>
     *     <li>The leader responded with data</li>
     *     <li>The client received a response from the leader and stored that data in memory</li>
     * </ol>
     *
     * But it's possible that at the <em>current</em> point in time, that same partition might not be in a fetchable
     * state. For example:
     *
     * <ul>
     *     <li>
     *         The partition is no longer assigned to the client. This also includes when the partition assignment
     *         is either {@link SubscriptionState#markPendingRevocation(Set) pending revocation} or
     *         {@link SubscriptionState#markPendingOnAssignedCallback(Collection, boolean) pending assignment}.
     *     </li>
     *     <li>
     *         The client {@link Consumer#pause(Collection) paused} the partition. A paused partition remains in
     *         the fetch buffer, because {@link FetchCollector#collectFetch(FetchBuffer)} explicitly skips over
     *         paused partitions and does not return them to the user.
     *     </li>
     *     <li>
     *         The partition does not have a valid position on the client. This could be due to the partition
     *         awaiting validation or awaiting reset.
     *     </li>
     * </ul>
     *
     * For those reasons, a partition that was <em>previously</em> in a fetchable state might not <em>currently</em>
     * be in a fetchable state.
     * </p>
     *
     * <p>
     * Here's why this is important—in a production system, a given leader node serves as a leader for many partitions.
     * From the client's perspective, it's possible that a node has a mix of both fetchable and unfetchable partitions.
     * When the client determines which nodes to skip and which to fetch from, it's important that unfetchable
     * partitions don't block fetchable partitions from being fetched.
     * </p>
     *
     * <p>
     * So, when it's determined that a buffered partition is not in a fetchable state, it should be skipped.
     * Otherwise, its node would end up in the set of nodes with buffered data and no fetch would be requested.
     * </p>
     *
     * @param partitions Buffered partitions
     * @param currentTimeMs Current timestamp
     *
     * @return Set of zero or more IDs for leader nodes of buffered partitions
     */
    private Set<Integer> bufferedNodes(Set<TopicPartition> partitions, long currentTimeMs) {
        Set<Integer> ids = new HashSet<>();

        for (TopicPartition partition : partitions) {
            if (!subscriptions.isFetchable(partition))
                continue;

            SubscriptionState.FetchPosition position = positionForPartition(partition);
            Optional<Node> nodeOpt = maybeNodeForPosition(partition, position, currentTimeMs);
            nodeOpt.ifPresent(node -> ids.add(node.id()));
        }

        return ids;
    }

    // Visible for testing
    protected FetchSessionHandler sessionHandler(int node) {
        return sessionHandlers.get(node);
    }

    /**
     * This method is called by {@link #close(Timer)} which is guarded by the {@link IdempotentCloser}) such as to only
     * be executed once the first time that any of the {@link #close()} methods are called. Subclasses can override
     * this method without the need for extra synchronization at the instance level.
     *
     * @param timer Timer to enforce time limit
     */
    // Visible for testing
    protected void closeInternal(Timer timer) {
        // we do not need to re-enable wake-ups since we are closing already
        Utils.closeQuietly(fetchBuffer, "fetchBuffer");
        Utils.closeQuietly(decompressionBufferSupplier, "decompressionBufferSupplier");
    }

    public void close(final Timer timer) {
        idempotentCloser.close(() -> closeInternal(timer));
    }

    @Override
    public void close() {
        close(time.timer(Duration.ZERO));
    }

    /**
     * Defines the contract for handling fetch responses from brokers.
     * @param <T> Type of response, usually either {@link ClientResponse} or {@link Throwable}
     */
    @FunctionalInterface
    protected interface ResponseHandler<T> {

        /**
         * Handle the response from the given {@link Node target}
         */
        void handle(Node target, FetchSessionHandler.FetchRequestData data, T response);
    }
}