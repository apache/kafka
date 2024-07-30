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
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementCommitCallbackEvent;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * {@code ShareConsumeRequestManager} is responsible for generating {@link ShareFetchRequest} and
 * {@link ShareAcknowledgeRequest} to fetch and acknowledge records being delivered for a consumer
 * in a share group.
 */
@SuppressWarnings("NPathComplexity")
public class ShareConsumeRequestManager implements RequestManager, MemberStateListener, Closeable {
    private final Time time;
    private final Logger log;
    private final LogContext logContext;
    private final String groupId;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final FetchConfig fetchConfig;
    protected final ShareFetchBuffer shareFetchBuffer;
    private final BackgroundEventHandler backgroundEventHandler;
    private final Map<Integer, ShareSessionHandler> sessionHandlers;
    private final Set<Integer> nodesWithPendingRequests;
    private final ShareFetchMetricsManager metricsManager;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();
    private Uuid memberId;
    private boolean fetchMoreRecords = false;
    private final Map<TopicIdPartition, Acknowledgements> fetchAcknowledgementsMap;
    private final Map<Integer, Pair<AcknowledgeRequestState>> acknowledgeRequestStates;
    private final long retryBackoffMs;
    private final long retryBackoffMaxMs;
    private boolean closing = false;
    private final CompletableFuture<Void> closeFuture;
    private boolean isAsyncDone = false;

    ShareConsumeRequestManager(final Time time,
                               final LogContext logContext,
                               final String groupId,
                               final ConsumerMetadata metadata,
                               final SubscriptionState subscriptions,
                               final FetchConfig fetchConfig,
                               final ShareFetchBuffer shareFetchBuffer,
                               final BackgroundEventHandler backgroundEventHandler,
                               final ShareFetchMetricsManager metricsManager,
                               final long retryBackoffMs,
                               final long retryBackoffMaxMs) {
        this.time = time;
        this.log = logContext.logger(ShareConsumeRequestManager.class);
        this.logContext = logContext;
        this.groupId = groupId;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.fetchConfig = fetchConfig;
        this.shareFetchBuffer = shareFetchBuffer;
        this.backgroundEventHandler = backgroundEventHandler;
        this.metricsManager = metricsManager;
        this.retryBackoffMs = retryBackoffMs;
        this.retryBackoffMaxMs = retryBackoffMaxMs;
        this.sessionHandlers = new HashMap<>();
        this.nodesWithPendingRequests = new HashSet<>();
        this.acknowledgeRequestStates = new HashMap<>();
        this.fetchAcknowledgementsMap = new HashMap<>();
        this.closeFuture = new CompletableFuture<>();
    }

    @Override
    public PollResult poll(long currentTimeMs) {
        if (memberId == null) {
            return PollResult.EMPTY;
        }

        // Send any pending acknowledgements before fetching more records.
        PollResult pollResult = processAcknowledgements(currentTimeMs);
        if (pollResult != null) {
            return pollResult;
        }

        if (!fetchMoreRecords || closing) {
            return PollResult.EMPTY;
        }

        Map<Node, ShareSessionHandler> handlerMap = new HashMap<>();
        Map<String, Uuid> topicIds = metadata.topicIds();
        for (TopicPartition partition : partitionsToFetch()) {
            Optional<Node> leaderOpt = metadata.currentLeader(partition).leader;

            if (!leaderOpt.isPresent()) {
                log.debug("Requesting metadata update for partition {} since current leader node is missing", partition);
                metadata.requestUpdate(false);
                continue;
            }

            Uuid topicId = topicIds.get(partition.topic());
            if (topicId == null) {
                log.debug("Requesting metadata update for partition {} since topic ID is missing", partition);
                metadata.requestUpdate(false);
                continue;
            }

            Node node = leaderOpt.get();
            if (nodesWithPendingRequests.contains(node.id())) {
                log.trace("Skipping fetch for partition {} because previous fetch request to {} has not been processed", partition, node.id());
            } else {
                // if there is a leader and no in-flight requests, issue a new fetch
                ShareSessionHandler handler = handlerMap.computeIfAbsent(node,
                        k -> sessionHandlers.computeIfAbsent(node.id(), n -> new ShareSessionHandler(logContext, n, memberId)));

                TopicIdPartition tip = new TopicIdPartition(topicId, partition);
                Acknowledgements acknowledgementsToSend = fetchAcknowledgementsMap.get(tip);
                if (acknowledgementsToSend != null) {
                    metricsManager.recordAcknowledgementSent(acknowledgementsToSend.size());
                }
                handler.addPartitionToFetch(tip, acknowledgementsToSend);

                log.debug("Added fetch request for partition {} to node {}", partition, node.id());
            }
        }

        Map<Node, ShareFetchRequest.Builder> builderMap = new LinkedHashMap<>();
        for (Map.Entry<Node, ShareSessionHandler> entry : handlerMap.entrySet()) {
            builderMap.put(entry.getKey(), entry.getValue().newShareFetchBuilder(groupId, fetchConfig));
        }

        List<UnsentRequest> requests = builderMap.entrySet().stream().map(entry -> {
            Node target = entry.getKey();
            log.trace("Building ShareFetch request to send to node {}", target.id());
            ShareFetchRequest.Builder requestBuilder = entry.getValue();

            nodesWithPendingRequests.add(target.id());

            BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                if (error != null) {
                    handleShareFetchFailure(target, requestBuilder.data(), error);
                } else {
                    handleShareFetchSuccess(target, requestBuilder.data(), clientResponse);
                }
            };
            return new UnsentRequest(requestBuilder, Optional.of(target)).whenComplete(responseHandler);
        }).collect(Collectors.toList());

        return new PollResult(requests);
    }

    public void fetch(Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        if (!fetchMoreRecords) {
            log.debug("Fetch more data");
            fetchMoreRecords = true;
        }
        acknowledgementsMap.forEach((tip, acks) -> fetchAcknowledgementsMap.merge(tip, acks, Acknowledgements::merge));
    }

    /**
     * Process acknowledgeRequestStates and prepares a list of acknowledgements to be sent in the poll().
     *
     * @param currentTimeMs the current time in ms.
     *
     * @return the PollResult containing zero or more acknowledgements.
     */
    private PollResult processAcknowledgements(long currentTimeMs) {
        List<UnsentRequest> unsentRequests = new ArrayList<>();
        for (Pair<AcknowledgeRequestState> requestStates : acknowledgeRequestStates.values()) {
            isAsyncDone = false;
            // For commitAsync
            maybeBuildRequest(requestStates.getFirst(), currentTimeMs, true).ifPresent(unsentRequests::add);

            // Check to ensure we start processing commitSync/close only if there are no commitAsync requests left to process.
            if (isAsyncDone)
                maybeBuildRequest(requestStates.getSecond(), currentTimeMs, false).ifPresent(unsentRequests::add);
        }

        PollResult pollResult = null;
        if (!unsentRequests.isEmpty()) {
            pollResult = new PollResult(unsentRequests);
        } else if (areAnyAcknowledgementsLeft()) {
            // Return empty result until all the acknowledgement request states are processed
            pollResult = PollResult.EMPTY;
        } else if (closing) {
            if (!closeFuture.isDone()) {
                log.trace("Completing acknowledgement on close");
                closeFuture.complete(null);
            }
            pollResult = PollResult.EMPTY;
        }

        return pollResult;
    }

    private Optional<UnsentRequest> maybeBuildRequest(AcknowledgeRequestState acknowledgeRequestState, long currentTimeMs, boolean onCommitAsync) {
        if (acknowledgeRequestState == null || (!closing && acknowledgeRequestState.acknowledgementsToSend.isEmpty() && acknowledgeRequestState.incompleteAcknowledgements.isEmpty())) {
            if (onCommitAsync) {
                isAsyncDone = true;
            }
            return Optional.empty();
        } else if (!acknowledgeRequestState.maybeExpire()) {
            if (nodesWithPendingRequests.contains(acknowledgeRequestState.nodeId)) {
                log.debug("Skipping acknowledge request because previous request to {} has not been processed", acknowledgeRequestState.nodeId);
            } else {
                if (acknowledgeRequestState.canSendRequest(currentTimeMs)) {
                    acknowledgeRequestState.onSendAttempt(currentTimeMs);
                    if (onCommitAsync) {
                        isAsyncDone = true;
                    }
                    return Optional.of(acknowledgeRequestState.buildRequest(currentTimeMs));
                }
            }
        } else {
            // Fill in TimeoutException
            for (TopicIdPartition tip : acknowledgeRequestState.incompleteAcknowledgements.keySet()) {
                metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getIncompleteAcknowledgementsCount(tip));
                acknowledgeRequestState.handleAcknowledgeTimedOut(tip);
            }
            acknowledgeRequestState.incompleteAcknowledgements.clear();
            isAsyncDone = true;
        }
        return Optional.empty();
    }

    private boolean areAnyAcknowledgementsLeft() {
        boolean areAnyAcksLeft = false;
        Iterator<Map.Entry<Integer, Pair<AcknowledgeRequestState>>> iterator = acknowledgeRequestStates.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, Pair<AcknowledgeRequestState>> acknowledgeRequestStatePair = iterator.next();
            if (isRequestStateEmpty(acknowledgeRequestStatePair.getValue().getFirst()) && isRequestStateEmpty(acknowledgeRequestStatePair.getValue().getSecond())) {
                areAnyAcksLeft = true;
            } else if (!closing) {
                acknowledgeRequestStates.remove(acknowledgeRequestStatePair.getKey());
            }
        }
        if (!acknowledgeRequestStates.isEmpty()) areAnyAcksLeft = true;
        return areAnyAcksLeft;
    }

    private boolean isRequestStateEmpty(AcknowledgeRequestState acknowledgeRequestState) {
        return acknowledgeRequestState == null
                || !acknowledgeRequestState.acknowledgementsToSend.isEmpty()
                || !acknowledgeRequestState.incompleteAcknowledgements.isEmpty()
                || !acknowledgeRequestState.inFlightAcknowledgements.isEmpty();
    }

    /**
     * Enqueue an AcknowledgeRequestState to be picked up on the next poll
     *
     * @param acknowledgementsMap The acknowledgements to commit
     * @param deadlineMs          Time until which the request will be retried if it fails with
     *                            an expected retriable error.
     *
     * @return The future which completes when the acknowledgements finished
     */
    public CompletableFuture<Map<TopicIdPartition, Acknowledgements>> commitSync(
            final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap,
            final long deadlineMs) {
        final AtomicInteger resultCount = new AtomicInteger();
        final CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future = new CompletableFuture<>();
        final ResultHandler resultHandler = new ResultHandler(resultCount, Optional.of(future));

        final Cluster cluster = metadata.fetch();

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                Map<TopicIdPartition, Acknowledgements> acknowledgementsMapForNode = new HashMap<>();
                for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                    Acknowledgements acknowledgements = acknowledgementsMap.get(tip);
                    if (acknowledgements != null) {
                        acknowledgementsMapForNode.put(tip, acknowledgements);

                        metricsManager.recordAcknowledgementSent(acknowledgements.size());
                        log.debug("Added sync acknowledge request for partition {} to node {}", tip.topicPartition(), node.id());
                        resultCount.incrementAndGet();
                    }
                }
                acknowledgeRequestStates.putIfAbsent(nodeId, new Pair<>(null, null));

                // There can only be one commitSync()/close() happening at a time. So per node, there will be one acknowledge request state representing commitSync() and close().
                acknowledgeRequestStates.put(nodeId, new Pair<>(acknowledgeRequestStates.get(nodeId).getFirst(), new AcknowledgeRequestState(logContext,
                        ShareConsumeRequestManager.class.getSimpleName() + ":1",
                        deadlineMs,
                        retryBackoffMs,
                        retryBackoffMaxMs,
                        sessionHandler,
                        nodeId,
                        acknowledgementsMapForNode,
                        this::handleShareAcknowledgeSuccess,
                        this::handleShareAcknowledgeFailure,
                        resultHandler
                )));
            }
        });

        resultHandler.completeIfEmpty();
        return future;
    }

    /**
     * Enqueue an AcknowledgeRequestState to be picked up on the next poll.
     *
     * @param acknowledgementsMap The acknowledgements to commit
     */
    public void commitAsync(final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        final Cluster cluster = metadata.fetch();
        final AtomicInteger resultCount = new AtomicInteger();
        final ResultHandler resultHandler = new ResultHandler(resultCount, Optional.empty());

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                Map<TopicIdPartition, Acknowledgements> acknowledgementsMapForNode = new HashMap<>();

                acknowledgeRequestStates.putIfAbsent(nodeId, new Pair<>(null, null));

                for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                    Acknowledgements acknowledgements = acknowledgementsMap.get(tip);
                    if (acknowledgements != null) {
                        acknowledgementsMapForNode.put(tip, acknowledgements);

                        metricsManager.recordAcknowledgementSent(acknowledgements.size());
                        log.debug("Added async acknowledge request for partition {} to node {}", tip.topicPartition(), node.id());
                        resultCount.incrementAndGet();
                        if (acknowledgeRequestStates.get(nodeId).getFirst() == null) {
                            acknowledgeRequestStates.replace(nodeId, new Pair<>(new AcknowledgeRequestState(logContext,
                                    ShareConsumeRequestManager.class.getSimpleName() + ":2",
                                    Long.MAX_VALUE,
                                    retryBackoffMs,
                                    retryBackoffMaxMs,
                                    sessionHandler,
                                    nodeId,
                                    acknowledgementsMapForNode,
                                    this::handleShareAcknowledgeSuccess,
                                    this::handleShareAcknowledgeFailure,
                                    resultHandler
                            ), acknowledgeRequestStates.get(nodeId).getSecond()));
                        } else {
                            Acknowledgements prevAcks = acknowledgeRequestStates.get(nodeId).getFirst().acknowledgementsToSend.putIfAbsent(tip, acknowledgements);
                            if (prevAcks != null) {
                                acknowledgeRequestStates.get(nodeId).getFirst().acknowledgementsToSend.get(tip).merge(acknowledgements);
                            }
                        }
                    }
                }
            }
        });

        resultHandler.completeIfEmpty();
    }

    /**
     * Enqueue the final AcknowledgeRequestState used to commit the final acknowledgements and
     * close the share sessions.
     *
     * @param acknowledgementsMap The acknowledgements to commit
     * @param deadlineMs          Time until which the request will be retried if it fails with
     *                            an expected retriable error.
     *
     * @return The future which completes when the acknowledgements finished
     */
    public CompletableFuture<Void> acknowledgeOnClose(final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap,
                                                      final long deadlineMs) {
        final Cluster cluster = metadata.fetch();
        final AtomicInteger resultCount = new AtomicInteger();
        final ResultHandler resultHandler = new ResultHandler(resultCount, Optional.empty());

        closing = true;

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                Map<TopicIdPartition, Acknowledgements> acknowledgementsMapForNode = new HashMap<>();
                for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                    Acknowledgements acknowledgements = acknowledgementsMap.get(tip);
                    if (acknowledgements != null) {
                        acknowledgementsMapForNode.put(tip, acknowledgements);

                        metricsManager.recordAcknowledgementSent(acknowledgements.size());
                        log.debug("Added closing acknowledge request for partition {} to node {}", tip.topicPartition(), node.id());
                        resultCount.incrementAndGet();
                    }
                }

                acknowledgeRequestStates.putIfAbsent(nodeId, new Pair<>(null, null));
                // There can only be one commitSync()/close() happening at a time. So per node, there will be one acknowledge request state.
                acknowledgeRequestStates.put(nodeId, new Pair<>(acknowledgeRequestStates.get(nodeId).getFirst(),
                        new AcknowledgeRequestState(logContext,
                                ShareConsumeRequestManager.class.getSimpleName() + ":1",
                                deadlineMs,
                                retryBackoffMs,
                                retryBackoffMaxMs,
                                sessionHandler,
                                nodeId,
                                acknowledgementsMapForNode,
                                this::handleShareAcknowledgeCloseSuccess,
                                this::handleShareAcknowledgeCloseFailure,
                                resultHandler,
                                true
                        )));
            }
        });

        resultHandler.completeIfEmpty();
        return closeFuture;
    }

    private void handleShareFetchSuccess(Node fetchTarget,
                                         @SuppressWarnings("unused") ShareFetchRequestData requestData,
                                         ClientResponse resp) {
        try {
            log.debug("Completed ShareFetch request from node {} successfully", fetchTarget.id());
            final ShareFetchResponse response = (ShareFetchResponse) resp.responseBody();
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler == null) {
                log.error("Unable to find ShareSessionHandler for node {}. Ignoring ShareFetch response.",
                        fetchTarget.id());
                return;
            }

            final short requestVersion = resp.requestHeader().apiVersion();

            if (!handler.handleResponse(response, requestVersion)) {
                if (response.error() == Errors.UNKNOWN_TOPIC_ID) {
                    metadata.requestUpdate(false);
                }
                return;
            }

            final Map<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData = new LinkedHashMap<>();

            response.data().responses().forEach(topicResponse ->
                    topicResponse.partitions().forEach(partition ->
                            responseData.put(new TopicIdPartition(topicResponse.topicId(),
                                    partition.partitionIndex(),
                                    metadata.topicNames().get(topicResponse.topicId())), partition)));

            final Set<TopicPartition> partitions = responseData.keySet().stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet());
            final ShareFetchMetricsAggregator shareFetchMetricsAggregator = new ShareFetchMetricsAggregator(metricsManager, partitions);

            for (Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> entry : responseData.entrySet()) {
                TopicIdPartition tip = entry.getKey();

                ShareFetchResponseData.PartitionData partitionData = entry.getValue();

                log.debug("ShareFetch for partition {} returned fetch data {}", tip, partitionData);

                Acknowledgements acks = fetchAcknowledgementsMap.remove(tip);
                if (acks != null) {
                    if (partitionData.acknowledgeErrorCode() != Errors.NONE.code()) {
                        metricsManager.recordFailedAcknowledgements(acks.size());
                    }
                    acks.setAcknowledgeErrorCode(Errors.forCode(partitionData.acknowledgeErrorCode()));
                    Map<TopicIdPartition, Acknowledgements> acksMap = Collections.singletonMap(tip, acks);
                    ShareAcknowledgementCommitCallbackEvent event = new ShareAcknowledgementCommitCallbackEvent(acksMap);
                    backgroundEventHandler.add(event);
                }

                ShareCompletedFetch completedFetch = new ShareCompletedFetch(
                        logContext,
                        BufferSupplier.create(),
                        tip,
                        partitionData,
                        shareFetchMetricsAggregator,
                        requestVersion);
                shareFetchBuffer.add(completedFetch);

                if (!partitionData.acquiredRecords().isEmpty()) {
                    fetchMoreRecords = false;
                }
            }

            metricsManager.recordLatency(resp.requestLatencyMs());
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareFetchFailure(Node fetchTarget,
                                         ShareFetchRequestData requestData,
                                         Throwable error) {
        try {
            log.debug("Completed ShareFetch request from node {} unsuccessfully {}", fetchTarget.id(), Errors.forException(error));
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());
            if (handler != null) {
                handler.handleError(error);
            }

            requestData.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));

                Acknowledgements acks = fetchAcknowledgementsMap.remove(tip);
                if (acks != null) {
                    metricsManager.recordFailedAcknowledgements(acks.size());
                    acks.setAcknowledgeErrorCode(Errors.forException(error));
                    Map<TopicIdPartition, Acknowledgements> acksMap = Collections.singletonMap(tip, acks);
                    ShareAcknowledgementCommitCallbackEvent event = new ShareAcknowledgementCommitCallbackEvent(acksMap);
                    backgroundEventHandler.add(event);
                }
            }));
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareAcknowledgeSuccess(Node fetchTarget,
                                               ShareAcknowledgeRequestData requestData,
                                               AcknowledgeRequestState acknowledgeRequestState,
                                               ClientResponse resp,
                                               long currentTimeMs) {
        try {
            log.debug("Completed ShareAcknowledge request from node {} successfully", fetchTarget.id());
            final ShareAcknowledgeResponse response = (ShareAcknowledgeResponse) resp.responseBody();
            final ShareSessionHandler handler = acknowledgeRequestState.sessionHandler();

            final short requestVersion = resp.requestHeader().apiVersion();

            if (!handler.handleResponse(response, requestVersion)) {
                acknowledgeRequestState.onFailedAttempt(currentTimeMs);
                if (response.error().exception() instanceof RetriableException && !closing) {
                    // We retry the request until the timer expires, unless we are closing.
                    acknowledgeRequestState.retryRequest();
                }
            } else {

                AtomicBoolean shouldRetry = new AtomicBoolean(false);
                // Check all partition level error codes
                response.data().responses().forEach(shareAcknowledgeTopicResponse -> shareAcknowledgeTopicResponse.partitions().forEach(partitionData -> {
                    Errors partitionError = Errors.forCode(partitionData.errorCode());
                    TopicIdPartition tip = new TopicIdPartition(shareAcknowledgeTopicResponse.topicId(),
                            partitionData.partitionIndex(),
                            metadata.topicNames().get(shareAcknowledgeTopicResponse.topicId()));
                    if (partitionError.exception() != null) {
                        if (partitionError.exception() instanceof RetriableException && !closing) {
                            // Move to incomplete acknowledgements to retry
                            acknowledgeRequestState.moveToIncompleteAcks(tip);
                            shouldRetry.set(true);
                        } else {
                            metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                            acknowledgeRequestState.handleAcknowledgeErrorCode(tip, partitionError);
                        }
                    } else {
                        acknowledgeRequestState.handleAcknowledgeErrorCode(tip, partitionError);
                    }
                }));

                if (shouldRetry.get()) {
                    acknowledgeRequestState.onFailedAttempt(currentTimeMs);
                } else {
                    acknowledgeRequestState.onSuccessfulAttempt(currentTimeMs);
                }
                acknowledgeRequestState.processingComplete();
            }
            metricsManager.recordLatency(resp.requestLatencyMs());
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareAcknowledgeFailure(Node fetchTarget,
                                               ShareAcknowledgeRequestData requestData,
                                               AcknowledgeRequestState acknowledgeRequestState,
                                               Throwable error,
                                               long currentTimeMs) {
        try {
            log.debug("Completed ShareAcknowledge request from node {} unsuccessfully {}", fetchTarget.id(), Errors.forException(error));
            acknowledgeRequestState.sessionHandler().handleError(error);

            requestData.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));
                metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forException(error));
            }));
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareAcknowledgeCloseSuccess(Node fetchTarget,
                                                    ShareAcknowledgeRequestData requestData,
                                                    AcknowledgeRequestState acknowledgeRequestState,
                                                    ClientResponse resp,
                                                    long currentTimeMs) {
        try {
            log.debug("Completed ShareAcknowledge on close request from node {} successfully", fetchTarget.id());
            final ShareAcknowledgeResponse response = (ShareAcknowledgeResponse) resp.responseBody();

            response.data().responses().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));
                if (partition.errorCode() != Errors.NONE.code()) {
                    metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                }
                acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forCode(partition.errorCode()));
            }));

            acknowledgeRequestState.onSuccessfulAttempt(currentTimeMs);
            metricsManager.recordLatency(resp.requestLatencyMs());
            acknowledgeRequestState.processingComplete();
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());
            sessionHandlers.remove(fetchTarget.id());
        }
    }

    private void handleShareAcknowledgeCloseFailure(Node fetchTarget,
                                                    ShareAcknowledgeRequestData requestData,
                                                    AcknowledgeRequestState acknowledgeRequestState,
                                                    Throwable error,
                                                    long currentTimeMs) {
        try {
            log.debug("Completed ShareAcknowledge on close request from node {} unsuccessfully {}", fetchTarget.id(), Errors.forException(error));
            acknowledgeRequestState.sessionHandler().handleError(error);

            requestData.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));
                metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forException(error));
            }));
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());
            sessionHandlers.remove(fetchTarget.id());
        }
    }

    private List<TopicPartition> partitionsToFetch() {
        return subscriptions.fetchablePartitions(tp -> true);
    }

    public ShareSessionHandler sessionHandler(int node) {
        return sessionHandlers.get(node);
    }

    boolean hasCompletedFetches() {
        return !shareFetchBuffer.isEmpty();
    }

    protected void closeInternal() {
        Utils.closeQuietly(shareFetchBuffer, "shareFetchBuffer");
    }

    public void close() {
        idempotentCloser.close(this::closeInternal);
    }

    @Override
    public void onMemberEpochUpdated(Optional<Integer> memberEpochOpt, Optional<String> memberIdOpt) {
        memberIdOpt.ifPresent(s -> memberId = Uuid.fromString(s));
    }

    /**
     * Represents a request to acknowledge delivery that can be retried or aborted.
     */
    public class AcknowledgeRequestState extends TimedRequestState {

        /**
         * The share session handler.
         */
        private final ShareSessionHandler sessionHandler;

        /**
         * The node to send the request to.
         */
        private final int nodeId;

        /**
         * The map of acknowledgements to send
         */
        private final Map<TopicIdPartition, Acknowledgements> acknowledgementsToSend;

        /**
         * The map of acknowledgements to be retried in the next attempt.
         */
        private final Map<TopicIdPartition, Acknowledgements> incompleteAcknowledgements;

        /**
         * The in-flight acknowledgements
         */
        private final Map<TopicIdPartition, Acknowledgements> inFlightAcknowledgements;

        /**
         * The handler to call on a successful response from ShareAcknowledge.
         */
        private final ResponseHandler<ClientResponse> successHandler;

        /**
         * The handler to call on a failed response from ShareAcknowledge.
         */
        private final ResponseHandler<Throwable> errorHandler;

        /**
         * This handles completing a future when all results are known.
         */
        private final ResultHandler resultHandler;

        /**
         * Whether this is the final acknowledge request state before the consumer closes.
         */
        private final boolean onClose;

        AcknowledgeRequestState(LogContext logContext,
                                String owner,
                                long deadlineMs,
                                long retryBackoffMs,
                                long retryBackoffMaxMs,
                                ShareSessionHandler sessionHandler,
                                int nodeId,
                                Map<TopicIdPartition, Acknowledgements> acknowledgementsMap,
                                ResponseHandler<ClientResponse> successHandler,
                                ResponseHandler<Throwable> errorHandler,
                                ResultHandler resultHandler) {
            this(logContext, owner, deadlineMs, retryBackoffMs, retryBackoffMaxMs, sessionHandler, nodeId,
                    acknowledgementsMap, successHandler, errorHandler, resultHandler, false);
        }

        AcknowledgeRequestState(LogContext logContext,
                                String owner,
                                long deadlineMs,
                                long retryBackoffMs,
                                long retryBackoffMaxMs,
                                ShareSessionHandler sessionHandler,
                                int nodeId,
                                Map<TopicIdPartition, Acknowledgements> acknowledgementsMap,
                                ResponseHandler<ClientResponse> successHandler,
                                ResponseHandler<Throwable> errorHandler,
                                ResultHandler resultHandler,
                                boolean onClose) {
            super(logContext, owner, retryBackoffMs, retryBackoffMaxMs, deadlineTimer(time, deadlineMs));
            this.sessionHandler = sessionHandler;
            this.nodeId = nodeId;
            this.successHandler = successHandler;
            this.errorHandler = errorHandler;
            this.acknowledgementsToSend = acknowledgementsMap;
            this.resultHandler = resultHandler;
            this.onClose = onClose;
            this.inFlightAcknowledgements = new HashMap<>();
            this.incompleteAcknowledgements = new HashMap<>();
        }

        UnsentRequest buildRequest(long currentTimeMs) {
            // If this is the closing request, close the share session by setting the final epoch
            if (onClose) {
                sessionHandler.notifyClose();
            }

            Map<TopicIdPartition, Acknowledgements> finalAcknowledgementsToSend = new HashMap<>(
                    incompleteAcknowledgements.isEmpty() ? acknowledgementsToSend : incompleteAcknowledgements);

            for (Map.Entry<TopicIdPartition, Acknowledgements> entry : finalAcknowledgementsToSend.entrySet()) {
                sessionHandler.addPartitionToFetch(entry.getKey(), entry.getValue());
            }

            ShareAcknowledgeRequest.Builder requestBuilder = sessionHandler.newShareAcknowledgeBuilder(groupId, fetchConfig);
            Node nodeToSend = metadata.fetch().nodeById(nodeId);

            nodesWithPendingRequests.add(nodeId);

            BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                if (error != null) {
                    onFailedAttempt(currentTimeMs);
                    errorHandler.handle(nodeToSend, requestBuilder.data(), this, error, currentTimeMs);
                    processingComplete();
                } else {
                    successHandler.handle(nodeToSend, requestBuilder.data(), this, clientResponse, currentTimeMs);
                    if (onClose && !closeFuture.isDone()) {
                        closeFuture.complete(null);
                    }
                }
            };

            if (requestBuilder == null) {
                handleSessionErrorCode(Errors.SHARE_SESSION_NOT_FOUND);
                return null;
            } else {
                inFlightAcknowledgements.putAll(finalAcknowledgementsToSend);
                if (incompleteAcknowledgements.isEmpty()) {
                    acknowledgementsToSend.clear();
                } else {
                    incompleteAcknowledgements.clear();
                }
                return new UnsentRequest(requestBuilder, Optional.of(nodeToSend)).whenComplete(responseHandler);
            }
        }

        int getInFlightAcknowledgementsCount(TopicIdPartition tip) {
            Acknowledgements acks = inFlightAcknowledgements.get(tip);
            if (acks == null) {
                return 0;
            } else {
                return acks.size();
            }
        }

        int getIncompleteAcknowledgementsCount(TopicIdPartition tip) {
            Acknowledgements acks = incompleteAcknowledgements.get(tip);
            if (acks == null) {
                return 0;
            } else {
                return acks.size();
            }
        }

        int getAcknowledgementsToSendCount(TopicIdPartition tip) {
            Acknowledgements acks = acknowledgementsToSend.get(tip);
            if (acks == null) {
                return 0;
            } else {
                return acks.size();
            }
        }

        /**
         * Sets the error code in the acknowledgements and sends the response
         * through a background event.
         */
        void handleAcknowledgeErrorCode(TopicIdPartition tip, Errors acknowledgeErrorCode) {
            Acknowledgements acks;
            if (acknowledgeErrorCode == Errors.REQUEST_TIMED_OUT) {
                acks = incompleteAcknowledgements.get(tip);
            } else {
                acks = inFlightAcknowledgements.get(tip);
            }
            if (acks != null) {
                acks.setAcknowledgeErrorCode(acknowledgeErrorCode);
            }
            resultHandler.complete(tip, acks);
        }

        /**
         * Sets the error code for the acknowledgements which were timed out
         * after some retries.
         */
        void handleAcknowledgeTimedOut(TopicIdPartition tip) {
            Acknowledgements acks = incompleteAcknowledgements.get(tip);
            if (acks != null) {
                acks.setAcknowledgeErrorCode(Errors.REQUEST_TIMED_OUT);
            }
            resultHandler.complete(tip, acks);
        }

        /**
         * Set the error code for all remaining acknowledgements in the event
         * of a session error which prevents the remains acknowledgements from
         * being sent.
         */
        void handleSessionErrorCode(Errors errorCode) {
            inFlightAcknowledgements.forEach((tip, acks) -> {
                if (acks != null) {
                    acks.setAcknowledgeErrorCode(errorCode);
                }
                resultHandler.complete(tip, acks);
            });
            processingComplete();
        }

        ShareSessionHandler sessionHandler() {
            return sessionHandler;
        }

        void processingComplete() {
            inFlightAcknowledgements.clear();
            resultHandler.completeIfEmpty();
        }

        void retryRequest() {
            incompleteAcknowledgements.putAll(inFlightAcknowledgements);
            inFlightAcknowledgements.clear();
        }

        boolean maybeExpire() {
            return numAttempts > 0 && isExpired();
        }

        public void moveToIncompleteAcks(TopicIdPartition tip) {
            Acknowledgements acks = inFlightAcknowledgements.remove(tip);
            if (acks != null) {
                Acknowledgements existingAcks = incompleteAcknowledgements.putIfAbsent(tip, acks);
                if (existingAcks != null) {
                    incompleteAcknowledgements.get(tip).merge(acks);
                }
            }
        }
    }

    /**
     * Defines the contract for handling responses from brokers.
     * @param <T> Type of response, usually either {@link ClientResponse} or {@link Throwable}
     */
    @FunctionalInterface
    private interface ResponseHandler<T> {
        /**
         * Handle the response from the given {@link Node target}
         */
        void handle(Node target, ShareAcknowledgeRequestData request, AcknowledgeRequestState requestState, T response, long currentTimeMs);
    }

    /**
     * Sends a ShareAcknowledgeCommitCallback event to the application when it is done
     * processing all the remaining acknowledgement request states.
     * Also manages completing the future for synchronous acknowledgement commit by counting
     * down the results as they are known and completing the future at the end.
     */
    class ResultHandler {
        private final Map<TopicIdPartition, Acknowledgements> result;
        private final AtomicInteger remainingResults;
        private final Optional<CompletableFuture<Map<TopicIdPartition, Acknowledgements>>> future;

        ResultHandler(final AtomicInteger remainingResults,
                      final Optional<CompletableFuture<Map<TopicIdPartition, Acknowledgements>>> future) {
            result = new HashMap<>();
            this.remainingResults = remainingResults;
            this.future = future;
        }

        /**
         * Handle the result of a ShareAcknowledge request sent to one or more nodes and
         * signal the completion when all results are known.
         */
        public void complete(TopicIdPartition partition, Acknowledgements acknowledgements) {
            if (acknowledgements != null) {
                result.put(partition, acknowledgements);
            }
            if (remainingResults.decrementAndGet() == 0) {
                ShareAcknowledgementCommitCallbackEvent event = new ShareAcknowledgementCommitCallbackEvent(result);
                backgroundEventHandler.add(event);
                future.ifPresent(future -> future.complete(result));
            }
        }

        /**
         * Handles the case where there are no results pending after initialization.
         */
        public void completeIfEmpty() {
            if (remainingResults.get() == 0) {
                future.ifPresent(future -> future.complete(result));
            }
        }
    }

    static class Pair<V> {
        private V first;
        private V second;

        public Pair(V first, V second) {
            this.first = first;
            this.second = second;
        }

        public void setFirst(V first) {
            this.first = first;
        }

        public void setSecond(V second) {
            this.second = second;
        }

        public V getFirst() {
            return first;
        }

        public V getSecond() {
            return second;
        }
    }

    Pair<AcknowledgeRequestState> requestStates(int nodeId) {
        return acknowledgeRequestStates.get(nodeId);
    }
}