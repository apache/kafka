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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.events.CreateFetchRequestsEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * {@code FetchRequestManager} is responsible for generating {@link FetchRequest} that represent the
 * {@link SubscriptionState#fetchablePartitions(Predicate)} based on the user's topic subscription/partition
 * assignment.
 */
public class FetchRequestManager extends AbstractFetch implements RequestManager {

    private final Logger log;
    private final NetworkClientDelegate networkClientDelegate;
    private CompletableFuture<Void> pendingFetchRequestFuture;

    FetchRequestManager(final LogContext logContext,
                        final Time time,
                        final ConsumerMetadata metadata,
                        final SubscriptionState subscriptions,
                        final FetchConfig fetchConfig,
                        final FetchBuffer fetchBuffer,
                        final FetchMetricsManager metricsManager,
                        final NetworkClientDelegate networkClientDelegate,
                        final ApiVersions apiVersions) {
        super(logContext, metadata, subscriptions, fetchConfig, fetchBuffer, metricsManager, time, apiVersions);
        this.log = logContext.logger(FetchRequestManager.class);
        this.networkClientDelegate = networkClientDelegate;
    }

    @Override
    protected boolean isUnavailable(Node node) {
        return networkClientDelegate.isUnavailable(node);
    }

    @Override
    protected void maybeThrowAuthFailure(Node node) {
        networkClientDelegate.maybeThrowAuthFailure(node);
    }

    /**
     * Signals the {@link Consumer} wants requests be created for the broker nodes to fetch the next
     * batch of records.
     *
     * @see CreateFetchRequestsEvent
     * @return Future on which the caller can wait to ensure that the requests have been created
     */
    public CompletableFuture<Void> createFetchRequests() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (pendingFetchRequestFuture != null) {
            // In this case, we have an outstanding fetch request, so chain the newly created future to be
            // completed when the "pending" future is completed.
            pendingFetchRequestFuture.whenComplete((value, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(value);
                }
            });
        } else {
            pendingFetchRequestFuture = future;
        }

        return future;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PollResult poll(long currentTimeMs) {
        return pollInternal(
            this::prepareFetchRequests,
            this::handleFetchSuccess,
            this::handleFetchFailure
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PollResult pollOnClose(long currentTimeMs) {
        // There needs to be a pending fetch request for pollInternal to create the requests.
        createFetchRequests();

        // TODO: move the logic to poll to handle signal close
        return pollInternal(
                this::prepareCloseFetchSessionRequests,
                this::handleCloseFetchSessionSuccess,
                this::handleCloseFetchSessionFailure
        );
    }

    /**
     * Creates the {@link PollResult poll result} that contains a list of zero or more
     * {@link FetchRequest.Builder fetch requests}.
     *
     * @param fetchRequestPreparer {@link FetchRequestPreparer} to generate a {@link Map} of {@link Node nodes}
     *                             to their {@link FetchSessionHandler.FetchRequestData}
     * @param successHandler       {@link ResponseHandler Handler for successful responses}
     * @param errorHandler         {@link ResponseHandler Handler for failure responses}
     * @return {@link PollResult}
     */
    private PollResult pollInternal(FetchRequestPreparer fetchRequestPreparer,
                                    ResponseHandler<ClientResponse> successHandler,
                                    ResponseHandler<Throwable> errorHandler) {
        if (pendingFetchRequestFuture == null) {
            // If no explicit request for creating fetch requests was issued, just short-circuit.
            return PollResult.EMPTY;
        }

        try {
            Map<Node, FetchSessionHandler.FetchRequestData> fetchRequests = fetchRequestPreparer.prepare();

            List<UnsentRequest> requests = fetchRequests.entrySet().stream().map(entry -> {
                final Node fetchTarget = entry.getKey();
                final FetchSessionHandler.FetchRequestData data = entry.getValue();
                final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
                final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                    if (error != null)
                        errorHandler.handle(fetchTarget, data, error);
                    else
                        successHandler.handle(fetchTarget, data, clientResponse);
                };

                return new UnsentRequest(request, Optional.of(fetchTarget)).whenComplete(responseHandler);
            }).collect(Collectors.toList());

            pendingFetchRequestFuture.complete(null);
            return new PollResult(requests);
        } catch (Throwable t) {
            // A "dummy" poll result is returned here rather than rethrowing the error because any error
            // that is thrown from any RequestManager.poll() method interrupts the polling of the other
            // request managers.
            pendingFetchRequestFuture.completeExceptionally(t);
            return PollResult.EMPTY;
        } finally {
            pendingFetchRequestFuture = null;
        }
    }

    @Override
    protected Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests_option2() {
        // Update metrics in case there was an assignment change
        metricsManager.maybeUpdateAssignment(subscriptions);

        Map<Node, FetchSessionHandler.Builder> fetchable = new HashMap<>();
        long currentTimeMs = time.milliseconds();
        Map<String, Uuid> topicIds = metadata.topicIds();

        // This is the set of partitions that have buffered data
        Set<TopicPartition> buffered = Collections.unmodifiableSet(fetchBuffer.bufferedPartitions());

        // -------------------------------------------------------------------------------------------------------------
        //
        //  #######  ########  ######## ####  #######  ##    ##        #######
        // ##     ## ##     ##    ##     ##  ##     ## ###   ##       ##     ##
        // ##     ## ##     ##    ##     ##  ##     ## ####  ##              ##
        // ##     ## ########     ##     ##  ##     ## ## ## ##        #######
        // ##     ## ##           ##     ##  ##     ## ##  ####       ##
        // ##     ## ##           ##     ##  ##     ## ##   ###       ##
        //  #######  ##           ##    ####  #######  ##    ##       #########
        //
        // -------------------------------------------------------------------------------------------------------------
        // Option 2's does not send *any* requests when there is any buffered data.
        // -------------------------------------------------------------------------------------------------------------
        if (!buffered.isEmpty())
            return Collections.emptyMap();

        // This is the set of partitions that do not have buffered data
        Set<TopicPartition> unbuffered = Set.copyOf(subscriptions.fetchablePartitions(tp -> !buffered.contains(tp)));

        if (unbuffered.isEmpty())
            return Collections.emptyMap();

        // The first loop is the same logic as in the ClassicKafkaConsumer.
        for (TopicPartition partition : unbuffered) {
            SubscriptionState.FetchPosition position = subscriptions.position(partition);

            if (position == null)
                throw new IllegalStateException("Missing position for fetchable partition " + partition);

            Optional<Node> leaderOpt = position.currentLeader.leader;

            if (leaderOpt.isEmpty()) {
                log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
                metadata.requestUpdate(false);
                continue;
            }

            // Use the preferred read replica if set, otherwise the partition's leader
            Node node = selectReadReplica(partition, leaderOpt.get(), currentTimeMs);

            if (isUnavailable(node)) {
                maybeThrowAuthFailure(node);

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

        return fetchable.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()));
    }

    @SuppressWarnings("NPathComplexity")
    @Override
    protected Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests_option3() {
        // Update metrics in case there was an assignment change
        metricsManager.maybeUpdateAssignment(subscriptions);

        Map<Node, FetchSessionHandler.Builder> fetchable = new HashMap<>();
        long currentTimeMs = time.milliseconds();
        Map<String, Uuid> topicIds = metadata.topicIds();

        // This is the set of partitions that have buffered data
        Set<TopicPartition> buffered = Collections.unmodifiableSet(fetchBuffer.bufferedPartitions());

        // This is the set of partitions that do not have buffered data
        Set<TopicPartition> unbuffered = Set.copyOf(subscriptions.fetchablePartitions(tp -> !buffered.contains(tp)));

        // If there aren't any partitions that *aren't* buffered, return.
        if (unbuffered.isEmpty())
            return Collections.emptyMap();

        // The first loop is the same logic as in the ClassicKafkaConsumer.
        for (TopicPartition partition : unbuffered) {
            SubscriptionState.FetchPosition position = subscriptions.position(partition);

            if (position == null)
                throw new IllegalStateException("Missing position for fetchable partition " + partition);

            Optional<Node> leaderOpt = position.currentLeader.leader;

            if (leaderOpt.isEmpty()) {
                log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
                metadata.requestUpdate(false);
                continue;
            }

            // Use the preferred read replica if set, otherwise the partition's leader
            Node node = selectReadReplica(partition, leaderOpt.get(), currentTimeMs);

            if (isUnavailable(node)) {
                maybeThrowAuthFailure(node);

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

        // -------------------------------------------------------------------------------------------------------------
        //
        //  #######  ########  ######## ####  #######  ##    ##        #######
        // ##     ## ##     ##    ##     ##  ##     ## ###   ##       ##     ##
        // ##     ## ##     ##    ##     ##  ##     ## ####  ##              ##
        // ##     ## ########     ##     ##  ##     ## ## ## ##        #######
        // ##     ## ##           ##     ##  ##     ## ##  ####              ##
        // ##     ## ##           ##     ##  ##     ## ##   ###       ##     ##
        //  #######  ##           ##    ####  #######  ##    ##        #######
        //
        // -------------------------------------------------------------------------------------------------------------
        // Option 3 adds another loop over all the partitions *with* buffered data. These partitions are included
        // in the fetch requests so that they aren't inadvertently put into the "remove" set. Partitions in the
        // "remove" set are removed from the broker's mirrored cache of fetchable partitions. In some cases, that
        // could then lead to the eviction of the broker's fetch session.
        //
        // In an effort to avoid buffering too much data locally, partitions that already have buffered data only
        // request a nominal fetch size of 1 byte.
        // -------------------------------------------------------------------------------------------------------------
        for (TopicPartition partition : buffered) {
            if (!subscriptions.isAssigned(partition)) {
                // It's possible that a partition with buffered data from a previous request is now no longer
                // assigned to the consumer, in which case just skip this partition.
                continue;
            }

            SubscriptionState.FetchPosition position = subscriptions.position(partition);

            // This shouldn't be possible, but since SubscriptionState is currently shared in more than one
            // thread, caution should be exercised.
            if (position == null)
                continue;

            Optional<Node> leaderOpt = position.currentLeader.leader;

            if (leaderOpt.isEmpty())
                continue;

            // Use the preferred read replica if set, otherwise the partition's leader
            Node node = selectReadReplica(partition, leaderOpt.get(), currentTimeMs);

            if (!fetchable.containsKey(node))
                continue;

            FetchSessionHandler fsh = sessionHandler(node.id());

            // If there isn't a valid session for the node in question, there's no need to add the "nominal fetch"
            // in the request. It's OK to skip this node
            if (fsh == null || fsh.sessionId() == FetchMetadata.INVALID_SESSION_ID)
                continue;

            FetchSessionHandler.Builder builder = fetchable.computeIfAbsent(node, k -> {
                FetchSessionHandler fetchSessionHandler = sessionHandlers.computeIfAbsent(node.id(), n -> new FetchSessionHandler(logContext, n));
                return fetchSessionHandler.newBuilder();
            });
            Uuid topicId = topicIds.getOrDefault(partition.topic(), Uuid.ZERO_UUID);
            FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(topicId,
                position.offset,
                FetchRequest.INVALID_LOG_START_OFFSET,
                1,
                position.currentLeader.epoch,
                Optional.empty());
            builder.add(partition, partitionData);

            log.debug("Added {} fetch request for buffered partition {} at position {} to node {}", fetchConfig.isolationLevel,
                partition, position, node);
        }

        return fetchable.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()));
    }

    /**
     * Simple functional interface to all passing in a method reference for improved readability.
     */
    @FunctionalInterface
    protected interface FetchRequestPreparer {

        Map<Node, FetchSessionHandler.FetchRequestData> prepare();
    }
}
