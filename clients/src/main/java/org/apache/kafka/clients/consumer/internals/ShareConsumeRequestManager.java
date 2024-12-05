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
import org.apache.kafka.clients.Metadata;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
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
@SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
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
    private final Map<TopicIdPartition, Acknowledgements> fetchAcknowledgementsToSend;
    private final Map<TopicIdPartition, Acknowledgements> fetchAcknowledgementsInFlight;
    private final Map<Integer, Tuple<AcknowledgeRequestState>> acknowledgeRequestStates;
    private final long retryBackoffMs;
    private final long retryBackoffMaxMs;
    private boolean closing = false;
    private final CompletableFuture<Void> closeFuture;
    private boolean isAcknowledgementCommitCallbackRegistered = false;
    private final Map<IdAndPartition, String> topicNamesMap = new HashMap<>();

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
        this.fetchAcknowledgementsToSend = new HashMap<>();
        this.fetchAcknowledgementsInFlight = new HashMap<>();
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

        if (!fetchMoreRecords) {
            return PollResult.EMPTY;
        }

        Map<Node, ShareSessionHandler> handlerMap = new HashMap<>();
        Map<String, Uuid> topicIds = metadata.topicIds();
        Set<TopicIdPartition> fetchedPartitions = new HashSet<>();
        for (TopicPartition partition : partitionsToFetch()) {
            Optional<Node> leaderOpt = metadata.currentLeader(partition).leader;

            if (leaderOpt.isEmpty()) {
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
                Acknowledgements acknowledgementsToSend = fetchAcknowledgementsToSend.remove(tip);
                if (acknowledgementsToSend != null) {
                    metricsManager.recordAcknowledgementSent(acknowledgementsToSend.size());
                    fetchAcknowledgementsInFlight.put(tip, acknowledgementsToSend);
                }
                handler.addPartitionToFetch(tip, acknowledgementsToSend);
                fetchedPartitions.add(tip);
                topicNamesMap.putIfAbsent(new IdAndPartition(tip.topicId(), tip.partition()), tip.topic());

                log.debug("Added fetch request for partition {} to node {}", tip, node.id());
            }
        }

        // Map storing the list of partitions to forget in the upcoming request.
        Map<Node, List<TopicIdPartition>> partitionsToForgetMap = new HashMap<>();
        Cluster cluster = metadata.fetch();
        // Iterating over the session handlers to see if there are acknowledgements to be sent for partitions
        // which are no longer part of the current subscription.
        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                if (nodesWithPendingRequests.contains(node.id())) {
                    log.trace("Skipping fetch because previous fetch request to {} has not been processed", node.id());
                } else {
                    for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                        if (!fetchedPartitions.contains(tip)) {
                            Acknowledgements acknowledgementsToSend = fetchAcknowledgementsToSend.remove(tip);

                            if (acknowledgementsToSend != null) {
                                metricsManager.recordAcknowledgementSent(acknowledgementsToSend.size());
                                fetchAcknowledgementsInFlight.put(tip, acknowledgementsToSend);
                            }

                            sessionHandler.addPartitionToFetch(tip, acknowledgementsToSend);
                            partitionsToForgetMap.putIfAbsent(node, new ArrayList<>());
                            partitionsToForgetMap.get(node).add(tip);

                            topicNamesMap.putIfAbsent(new IdAndPartition(tip.topicId(), tip.partition()), tip.topic());
                            fetchedPartitions.add(tip);
                            log.debug("Added fetch request for previously subscribed partition {} to node {}", tip, node.id());
                        }
                    }
                }
            }
        });

        Map<Node, ShareFetchRequest.Builder> builderMap = new LinkedHashMap<>();
        for (Map.Entry<Node, ShareSessionHandler> entry : handlerMap.entrySet()) {
            ShareFetchRequest.Builder builder = entry.getValue().newShareFetchBuilder(groupId, fetchConfig);
            Node node = entry.getKey();

            if (partitionsToForgetMap.containsKey(node)) {
                if (builder.data().forgottenTopicsData() == null) {
                    builder.data().setForgottenTopicsData(new ArrayList<>());
                }
                builder.updateForgottenData(partitionsToForgetMap.get(node));
            }

            builderMap.put(node, builder);
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

        // The acknowledgements sent via ShareFetch are stored in this map.
        acknowledgementsMap.forEach((tip, acks) -> fetchAcknowledgementsToSend.merge(tip, acks, Acknowledgements::merge));
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
        AtomicBoolean isAsyncSent = new AtomicBoolean();
        for (Map.Entry<Integer, Tuple<AcknowledgeRequestState>> requestStates : acknowledgeRequestStates.entrySet()) {
            int nodeId = requestStates.getKey();

            if (!isNodeFree(nodeId)) {
                log.trace("Skipping acknowledge request because previous request to {} has not been processed, so acks are not sent", nodeId);
            } else {
                isAsyncSent.set(false);
                // First, the acknowledgements from commitAsync is sent.
                maybeBuildRequest(requestStates.getValue().getAsyncRequest(), currentTimeMs, true, isAsyncSent).ifPresent(unsentRequests::add);

                // Check to ensure we start processing commitSync/close only if there are no commitAsync requests left to process.
                if (isAsyncSent.get()) {
                    if (!isNodeFree(nodeId)) {
                        log.trace("Skipping acknowledge request because previous request to {} has not been processed, so acks are not sent", nodeId);
                        continue;
                    }

                    // We try to process the close request only if we have processed the async and the sync requests for the node.
                    if (requestStates.getValue().getSyncRequestQueue() == null) {
                        AcknowledgeRequestState closeRequestState = requestStates.getValue().getCloseRequest();

                        maybeBuildRequest(closeRequestState, currentTimeMs, false, isAsyncSent).ifPresent(unsentRequests::add);
                    } else {
                        // Processing the acknowledgements from commitSync
                        for (AcknowledgeRequestState acknowledgeRequestState : requestStates.getValue().getSyncRequestQueue()) {
                            maybeBuildRequest(acknowledgeRequestState, currentTimeMs, false, isAsyncSent).ifPresent(unsentRequests::add);
                        }
                    }
                }
            }

        }

        PollResult pollResult = null;
        if (!unsentRequests.isEmpty()) {
            pollResult = new PollResult(unsentRequests);
        } else if (checkAndRemoveCompletedAcknowledgements()) {
            // Return empty result until all the acknowledgement request states are processed
            pollResult = PollResult.EMPTY;
        } else if (closing) {
            if (!closeFuture.isDone()) {
                closeFuture.complete(null);
            }
            pollResult = PollResult.EMPTY;
        }
        return pollResult;
    }

    private boolean isNodeFree(int nodeId) {
        return !nodesWithPendingRequests.contains(nodeId);
    }

    public void setAcknowledgementCommitCallbackRegistered(boolean isAcknowledgementCommitCallbackRegistered) {
        this.isAcknowledgementCommitCallbackRegistered = isAcknowledgementCommitCallbackRegistered;
    }

    private void maybeSendShareAcknowledgeCommitCallbackEvent(Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        if (isAcknowledgementCommitCallbackRegistered) {
            ShareAcknowledgementCommitCallbackEvent event = new ShareAcknowledgementCommitCallbackEvent(acknowledgementsMap);
            backgroundEventHandler.add(event);
        }
    }

    /**
     *
     * @param acknowledgeRequestState Contains the acknowledgements to be sent.
     * @param currentTimeMs The current time in ms.
     * @param onCommitAsync Boolean to denote if the acknowledgements came from a commitAsync or not.
     * @param isAsyncSent Boolean to indicate if the async request has been sent.
     *
     * @return Returns the request if it was built.
     */
    private Optional<UnsentRequest> maybeBuildRequest(AcknowledgeRequestState acknowledgeRequestState,
                                                      long currentTimeMs,
                                                      boolean onCommitAsync,
                                                      AtomicBoolean isAsyncSent) {
        boolean asyncSent = true;
        try {
            if (acknowledgeRequestState == null || (!acknowledgeRequestState.onClose() && acknowledgeRequestState.isEmpty())) {
                return Optional.empty();
            }

            if (acknowledgeRequestState.maybeExpire()) {
                // Fill in TimeoutException
                for (TopicIdPartition tip : acknowledgeRequestState.incompleteAcknowledgements.keySet()) {
                    metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getIncompleteAcknowledgementsCount(tip));
                    acknowledgeRequestState.handleAcknowledgeTimedOut(tip);
                }
                acknowledgeRequestState.incompleteAcknowledgements.clear();
                return Optional.empty();
            }

            if (!acknowledgeRequestState.canSendRequest(currentTimeMs)) {
                // We wait for the backoff before we can send this request.
                asyncSent = false;
                return Optional.empty();
            }

            UnsentRequest request = acknowledgeRequestState.buildRequest();
            if (request == null) {
                asyncSent = false;
                return Optional.empty();
            }

            acknowledgeRequestState.onSendAttempt(currentTimeMs);
            return Optional.of(request);
        } finally {
            if (onCommitAsync) {
                isAsyncSent.set(asyncSent);
            }
        }
    }

    /**
     * Prunes the empty acknowledgementRequestStates in {@link #acknowledgeRequestStates}
     *
     * @return Returns true if there are still any acknowledgements left to be processed.
     */
    private boolean checkAndRemoveCompletedAcknowledgements() {
        boolean areAnyAcksLeft = false;
        Iterator<Map.Entry<Integer, Tuple<AcknowledgeRequestState>>> iterator = acknowledgeRequestStates.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, Tuple<AcknowledgeRequestState>> acknowledgeRequestStatePair = iterator.next();
            boolean areAsyncAcksLeft = true, areSyncAcksLeft = true;
            if (!isRequestStateInProgress(acknowledgeRequestStatePair.getValue().getAsyncRequest())) {
                acknowledgeRequestStatePair.getValue().setAsyncRequest(null);
                areAsyncAcksLeft = false;
            }

            if (!areRequestStatesInProgress(acknowledgeRequestStatePair.getValue().getSyncRequestQueue())) {
                acknowledgeRequestStatePair.getValue().nullifySyncRequestQueue();
                areSyncAcksLeft = false;
            }

            if (!isRequestStateInProgress(acknowledgeRequestStatePair.getValue().getCloseRequest())) {
                acknowledgeRequestStatePair.getValue().setCloseRequest(null);
            }

            if (areAsyncAcksLeft || areSyncAcksLeft) {
                areAnyAcksLeft = true;
            } else if (acknowledgeRequestStatePair.getValue().getCloseRequest() == null) {
                iterator.remove();
            }
        }

        if (!acknowledgeRequestStates.isEmpty()) areAnyAcksLeft = true;
        return areAnyAcksLeft;
    }

    private boolean isRequestStateInProgress(AcknowledgeRequestState acknowledgeRequestState) {
        if (acknowledgeRequestState == null) {
            return false;
        } else if (acknowledgeRequestState.onClose()) {
            return !acknowledgeRequestState.isProcessed;
        } else {
            return !(acknowledgeRequestState.isEmpty());
        }
    }

    private boolean areRequestStatesInProgress(Queue<AcknowledgeRequestState> acknowledgeRequestStates) {
        if (acknowledgeRequestStates == null) return false;
        for (AcknowledgeRequestState acknowledgeRequestState : acknowledgeRequestStates) {
            if (isRequestStateInProgress(acknowledgeRequestState)) {
                return true;
            }
        }
        return false;
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
                acknowledgeRequestStates.putIfAbsent(nodeId, new Tuple<>(null, null, null));

                // Add the incoming commitSync() request to the queue.
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

                acknowledgeRequestStates.get(nodeId).addSyncRequest(new AcknowledgeRequestState(logContext,
                        ShareConsumeRequestManager.class.getSimpleName() + ":1",
                        deadlineMs,
                        retryBackoffMs,
                        retryBackoffMaxMs,
                        sessionHandler,
                        nodeId,
                        acknowledgementsMapForNode,
                        resultHandler,
                        AcknowledgeRequestType.COMMIT_SYNC
                ));
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
        final ResultHandler resultHandler = new ResultHandler(Optional.empty());

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                Map<TopicIdPartition, Acknowledgements> acknowledgementsMapForNode = new HashMap<>();

                acknowledgeRequestStates.putIfAbsent(nodeId, new Tuple<>(null, null, null));

                for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                    Acknowledgements acknowledgements = acknowledgementsMap.get(tip);
                    if (acknowledgements != null) {
                        acknowledgementsMapForNode.put(tip, acknowledgements);

                        metricsManager.recordAcknowledgementSent(acknowledgements.size());
                        log.debug("Added async acknowledge request for partition {} to node {}", tip.topicPartition(), node.id());
                        AcknowledgeRequestState asyncRequestState = acknowledgeRequestStates.get(nodeId).getAsyncRequest();
                        if (asyncRequestState == null) {
                            acknowledgeRequestStates.get(nodeId).setAsyncRequest(new AcknowledgeRequestState(logContext,
                                    ShareConsumeRequestManager.class.getSimpleName() + ":2",
                                    Long.MAX_VALUE,
                                    retryBackoffMs,
                                    retryBackoffMaxMs,
                                    sessionHandler,
                                    nodeId,
                                    acknowledgementsMapForNode,
                                    resultHandler,
                                    AcknowledgeRequestType.COMMIT_ASYNC
                            ));
                        } else {
                            Acknowledgements prevAcks = asyncRequestState.acknowledgementsToSend.putIfAbsent(tip, acknowledgements);
                            if (prevAcks != null) {
                                asyncRequestState.acknowledgementsToSend.get(tip).merge(acknowledgements);
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
                    Acknowledgements acknowledgements = acknowledgementsMap.getOrDefault(tip, Acknowledgements.empty());

                    Acknowledgements acksFromShareFetch = fetchAcknowledgementsToSend.remove(tip);

                    if (acksFromShareFetch != null) {
                        acknowledgements.merge(acksFromShareFetch);
                    }

                    if (acknowledgements != null && !acknowledgements.isEmpty()) {
                        acknowledgementsMapForNode.put(tip, acknowledgements);

                        metricsManager.recordAcknowledgementSent(acknowledgements.size());
                        log.debug("Added closing acknowledge request for partition {} to node {}", tip.topicPartition(), node.id());
                        resultCount.incrementAndGet();
                    }
                }

                acknowledgeRequestStates.putIfAbsent(nodeId, new Tuple<>(null, null, null));

                // Ensure there is no close() request already present as they are blocking calls
                // and only one request can be active at a time.
                if (acknowledgeRequestStates.get(nodeId).getCloseRequest() != null && !acknowledgeRequestStates.get(nodeId).getCloseRequest().isEmpty()) {
                    log.error("Attempt to call close() when there is an existing close request for node {}-{}", node.id(), acknowledgeRequestStates.get(nodeId).getSyncRequestQueue());
                    closeFuture.completeExceptionally(
                            new IllegalStateException("Attempt to call close() when there is an existing close request for node : " + node.id()));
                } else {
                    // There can only be one close() happening at a time. So per node, there will be one acknowledge request state.
                    acknowledgeRequestStates.get(nodeId).setCloseRequest(new AcknowledgeRequestState(logContext,
                            ShareConsumeRequestManager.class.getSimpleName() + ":3",
                            deadlineMs,
                            retryBackoffMs,
                            retryBackoffMaxMs,
                            sessionHandler,
                            nodeId,
                            acknowledgementsMapForNode,
                            resultHandler,
                            AcknowledgeRequestType.CLOSE
                    ));

                }
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
                                    metadata.topicNames().getOrDefault(topicResponse.topicId(),
                                            topicNamesMap.remove(new IdAndPartition(topicResponse.topicId(), partition.partitionIndex())))), partition))
            );

            final Set<TopicPartition> partitions = responseData.keySet().stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet());
            final ShareFetchMetricsAggregator shareFetchMetricsAggregator = new ShareFetchMetricsAggregator(metricsManager, partitions);

            Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();
            for (Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> entry : responseData.entrySet()) {
                TopicIdPartition tip = entry.getKey();

                ShareFetchResponseData.PartitionData partitionData = entry.getValue();

                log.debug("ShareFetch for partition {} returned fetch data {}", tip, partitionData);

                Acknowledgements acks = fetchAcknowledgementsInFlight.remove(tip);
                if (acks != null) {
                    if (partitionData.acknowledgeErrorCode() != Errors.NONE.code()) {
                        metricsManager.recordFailedAcknowledgements(acks.size());
                    }
                    acks.setAcknowledgeErrorCode(Errors.forCode(partitionData.acknowledgeErrorCode()));
                    Map<TopicIdPartition, Acknowledgements> acksMap = Collections.singletonMap(tip, acks);
                    maybeSendShareAcknowledgeCommitCallbackEvent(acksMap);
                }

                Errors partitionError = Errors.forCode(partitionData.errorCode());
                if (partitionError == Errors.NOT_LEADER_OR_FOLLOWER || partitionError == Errors.FENCED_LEADER_EPOCH) {
                    log.debug("For {}, received error {}, with leaderIdAndEpoch {}", tip, partitionError, partitionData.currentLeader());
                    if (partitionData.currentLeader().leaderId() != -1 && partitionData.currentLeader().leaderEpoch() != -1) {
                        partitionsWithUpdatedLeaderInfo.put(tip.topicPartition(), new Metadata.LeaderIdAndEpoch(
                            Optional.of(partitionData.currentLeader().leaderId()), Optional.of(partitionData.currentLeader().leaderEpoch())));
                    }
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

            if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                List<Node> leaderNodes = response.data().nodeEndpoints().stream()
                    .map(e -> new Node(e.nodeId(), e.host(), e.port(), e.rack()))
                    .filter(e -> !e.equals(Node.noNode()))
                    .collect(Collectors.toList());
                metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
            }

            metricsManager.recordLatency(resp.destination(), resp.requestLatencyMs());
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

                Acknowledgements acks = fetchAcknowledgementsInFlight.remove(tip);
                if (acks != null) {
                    metricsManager.recordFailedAcknowledgements(acks.size());
                    acks.setAcknowledgeErrorCode(Errors.forException(error));
                    Map<TopicIdPartition, Acknowledgements> acksMap = Collections.singletonMap(tip, acks);
                    maybeSendShareAcknowledgeCommitCallbackEvent(acksMap);
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
                                               long responseCompletionTimeMs) {
        try {
            log.debug("Completed ShareAcknowledge request from node {} successfully", fetchTarget.id());
            ShareAcknowledgeResponse response = (ShareAcknowledgeResponse) resp.responseBody();

            Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();

            if (acknowledgeRequestState.onClose()) {
                response.data().responses().forEach(topic -> topic.partitions().forEach(partition -> {
                    TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                            partition.partitionIndex(),
                            metadata.topicNames().get(topic.topicId()));
                    if (partition.errorCode() != Errors.NONE.code()) {
                        metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                    }
                    acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forCode(partition.errorCode()));
                }));

                acknowledgeRequestState.onSuccessfulAttempt(responseCompletionTimeMs);
                acknowledgeRequestState.processingComplete();

            } else {
                if (!acknowledgeRequestState.sessionHandler.handleResponse(response, resp.requestHeader().apiVersion())) {
                    // Received a response-level error code.
                    acknowledgeRequestState.onFailedAttempt(responseCompletionTimeMs);

                    if (response.error().exception() instanceof RetriableException) {
                        // We retry the request until the timer expires, unless we are closing.
                        acknowledgeRequestState.moveAllToIncompleteAcks();
                    } else {
                        response.data().responses().forEach(shareAcknowledgeTopicResponse -> shareAcknowledgeTopicResponse.partitions().forEach(partitionData -> {
                            TopicIdPartition tip = new TopicIdPartition(shareAcknowledgeTopicResponse.topicId(),
                                    partitionData.partitionIndex(),
                                    metadata.topicNames().get(shareAcknowledgeTopicResponse.topicId()));

                            acknowledgeRequestState.handleAcknowledgeErrorCode(tip, response.error());
                        }));
                        acknowledgeRequestState.processingComplete();
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
                            boolean retry = false;

                            if (partitionError == Errors.NOT_LEADER_OR_FOLLOWER || partitionError == Errors.FENCED_LEADER_EPOCH) {
                                // If the leader has changed, there's no point in retrying the operation because the acquisition locks
                                // will have been released.
                                TopicPartition tp = new TopicPartition(metadata.topicNames().get(shareAcknowledgeTopicResponse.topicId()), partitionData.partitionIndex());

                                log.debug("For {}, received error {}, with leaderIdAndEpoch {}", tp, partitionError, partitionData.currentLeader());
                                if (partitionData.currentLeader().leaderId() != -1 && partitionData.currentLeader().leaderEpoch() != -1) {
                                    partitionsWithUpdatedLeaderInfo.put(tp, new Metadata.LeaderIdAndEpoch(
                                        Optional.of(partitionData.currentLeader().leaderId()), Optional.of(partitionData.currentLeader().leaderEpoch())));
                                }
                            } else if (partitionError.exception() instanceof RetriableException) {
                                retry = true;
                            }

                            if (retry) {
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
                        acknowledgeRequestState.onFailedAttempt(responseCompletionTimeMs);
                    } else {
                        acknowledgeRequestState.onSuccessfulAttempt(responseCompletionTimeMs);
                        acknowledgeRequestState.processingComplete();
                    }
                }
            }

            if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                List<Node> leaderNodes = response.data().nodeEndpoints().stream()
                    .map(e -> new Node(e.nodeId(), e.host(), e.port(), e.rack()))
                    .filter(e -> !e.equals(Node.noNode()))
                    .collect(Collectors.toList());
                metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
            }

            if (acknowledgeRequestState.isProcessed) {
                metricsManager.recordLatency(resp.destination(), resp.requestLatencyMs());
            }
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());

            if (acknowledgeRequestState.onClose()) {
                log.debug("Removing node from ShareSession {}", fetchTarget.id());
                sessionHandlers.remove(fetchTarget.id());
            }
        }
    }

    private void handleShareAcknowledgeFailure(Node fetchTarget,
                                               ShareAcknowledgeRequestData requestData,
                                               AcknowledgeRequestState acknowledgeRequestState,
                                               Throwable error,
                                               long responseCompletionTimeMs) {
        try {
            log.debug("Completed ShareAcknowledge request from node {} unsuccessfully {}", fetchTarget.id(), Errors.forException(error));
            acknowledgeRequestState.sessionHandler().handleError(error);
            acknowledgeRequestState.onFailedAttempt(responseCompletionTimeMs);

            requestData.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));
                metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forException(error));
            }));

            acknowledgeRequestState.processingComplete();
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());

            if (acknowledgeRequestState.onClose()) {
                log.debug("Removing node from ShareSession {}", fetchTarget.id());
                sessionHandlers.remove(fetchTarget.id());
            }
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
    public void onMemberEpochUpdated(Optional<Integer> memberEpochOpt, String memberId) {
        this.memberId = Uuid.fromString(memberId);
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
         * This handles completing a future when all results are known.
         */
        private final ResultHandler resultHandler;

        /**
         * Indicates whether this was part of commitAsync, commitSync or close operation.
         */
        private final AcknowledgeRequestType requestType;

        /**
         * Boolean to indicate if the request has been processed.
         * <p>
         * Set to true once we process the response and do not retry the request.
         * <p>
         * Initialized to false every time we build a request.
         */
        private boolean isProcessed;

        AcknowledgeRequestState(LogContext logContext,
                                String owner,
                                long deadlineMs,
                                long retryBackoffMs,
                                long retryBackoffMaxMs,
                                ShareSessionHandler sessionHandler,
                                int nodeId,
                                Map<TopicIdPartition, Acknowledgements> acknowledgementsMap,
                                ResultHandler resultHandler,
                                AcknowledgeRequestType acknowledgeRequestType) {
            super(logContext, owner, retryBackoffMs, retryBackoffMaxMs, deadlineTimer(time, deadlineMs));
            this.sessionHandler = sessionHandler;
            this.nodeId = nodeId;
            this.acknowledgementsToSend = acknowledgementsMap;
            this.resultHandler = resultHandler;
            this.inFlightAcknowledgements = new HashMap<>();
            this.incompleteAcknowledgements = new HashMap<>();
            this.requestType = acknowledgeRequestType;
            this.isProcessed = false;
        }

        UnsentRequest buildRequest() {
            // If this is the closing request, close the share session by setting the final epoch
            if (onClose()) {
                sessionHandler.notifyClose();
            }

            Map<TopicIdPartition, Acknowledgements> finalAcknowledgementsToSend = new HashMap<>(
                    incompleteAcknowledgements.isEmpty() ? acknowledgementsToSend : incompleteAcknowledgements);

            for (Map.Entry<TopicIdPartition, Acknowledgements> entry : finalAcknowledgementsToSend.entrySet()) {
                sessionHandler.addPartitionToFetch(entry.getKey(), entry.getValue());
            }

            ShareAcknowledgeRequest.Builder requestBuilder = sessionHandler.newShareAcknowledgeBuilder(groupId, fetchConfig);
            Node nodeToSend = metadata.fetch().nodeById(nodeId);

            log.trace("Building acknowledgements to send : {}", finalAcknowledgementsToSend);
            nodesWithPendingRequests.add(nodeId);
            isProcessed = false;

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

                UnsentRequest unsentRequest = new UnsentRequest(requestBuilder, Optional.of(nodeToSend));
                BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                    if (error != null) {
                        handleShareAcknowledgeFailure(nodeToSend, requestBuilder.data(), this, error, unsentRequest.handler().completionTimeMs());
                    } else {
                        handleShareAcknowledgeSuccess(nodeToSend, requestBuilder.data(), this, clientResponse, unsentRequest.handler().completionTimeMs());
                    }
                };
                return unsentRequest.whenComplete(responseHandler);
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

        boolean isEmpty() {
            return acknowledgementsToSend.isEmpty() &&
                    incompleteAcknowledgements.isEmpty() &&
                    inFlightAcknowledgements.isEmpty();
        }

        /**
         * Sets the error code in the acknowledgements and sends the response
         * through a background event.
         */
        void handleAcknowledgeErrorCode(TopicIdPartition tip, Errors acknowledgeErrorCode) {
            Acknowledgements acks = inFlightAcknowledgements.get(tip);
            if (acks != null) {
                acks.setAcknowledgeErrorCode(acknowledgeErrorCode);
            }
            resultHandler.complete(tip, acks, onCommitAsync());
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
            resultHandler.complete(tip, acks, onCommitAsync());
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
                resultHandler.complete(tip, acks, onCommitAsync());
            });
            processingComplete();
        }

        ShareSessionHandler sessionHandler() {
            return sessionHandler;
        }

        void processingComplete() {
            inFlightAcknowledgements.clear();
            resultHandler.completeIfEmpty();
            isProcessed = true;
        }

        /**
         * Moves all the in-flight acknowledgements to incomplete acknowledgements to retry
         * in the next request.
         */
        void moveAllToIncompleteAcks() {
            incompleteAcknowledgements.putAll(inFlightAcknowledgements);
            inFlightAcknowledgements.clear();
        }

        boolean maybeExpire() {
            return numAttempts > 0 && isExpired();
        }

        /**
         * Moves the in-flight acknowledgements for a given partition to incomplete acknowledgements to retry
         * in the next request.
         */
        public void moveToIncompleteAcks(TopicIdPartition tip) {
            Acknowledgements acks = inFlightAcknowledgements.remove(tip);
            if (acks != null) {
                incompleteAcknowledgements.put(tip, acks);
            }
        }

        public boolean onClose() {
            return requestType == AcknowledgeRequestType.CLOSE;
        }

        public boolean onCommitAsync() {
            return requestType == AcknowledgeRequestType.COMMIT_ASYNC;
        }
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

        ResultHandler(final Optional<CompletableFuture<Map<TopicIdPartition, Acknowledgements>>> future) {
            this(null, future);
        }

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
        public void complete(TopicIdPartition partition, Acknowledgements acknowledgements, boolean isCommitAsync) {
            if (acknowledgements != null) {
                result.put(partition, acknowledgements);
            }
            // For commitAsync, we do not wait for other results to complete, we prepare a background event
            // for every ShareAcknowledgeResponse.
            // For commitAsync, we send out a background event for every TopicIdPartition, so we use a singletonMap each time.
            if (isCommitAsync) {
                maybeSendShareAcknowledgeCommitCallbackEvent(Collections.singletonMap(partition, acknowledgements));
            } else if (remainingResults != null && remainingResults.decrementAndGet() == 0) {
                maybeSendShareAcknowledgeCommitCallbackEvent(result);
                future.ifPresent(future -> future.complete(result));
            }
        }

        /**
         * Handles the case where there are no results pending after initialization.
         */
        public void completeIfEmpty() {
            if (remainingResults != null && remainingResults.get() == 0) {
                future.ifPresent(future -> future.complete(result));
            }
        }
    }

    static class Tuple<V> {
        private V asyncRequest;
        private Queue<V> syncRequestQueue;
        private V closeRequest;

        public Tuple(V asyncRequest, Queue<V> syncRequestQueue, V closeRequest) {
            this.asyncRequest = asyncRequest;
            this.syncRequestQueue = syncRequestQueue;
            this.closeRequest = closeRequest;
        }

        public void setAsyncRequest(V asyncRequest) {
            this.asyncRequest = asyncRequest;
        }

        public void nullifySyncRequestQueue() {
            this.syncRequestQueue = null;
        }

        public void addSyncRequest(V syncRequest) {
            if (syncRequestQueue == null) {
                syncRequestQueue = new LinkedList<>();
            }
            this.syncRequestQueue.add(syncRequest);
        }

        public void setCloseRequest(V closeRequest) {
            this.closeRequest = closeRequest;
        }

        public V getAsyncRequest() {
            return asyncRequest;
        }

        public Queue<V> getSyncRequestQueue() {
            return syncRequestQueue;
        }

        public V getCloseRequest() {
            return closeRequest;
        }
    }

    Tuple<AcknowledgeRequestState> requestStates(int nodeId) {
        return acknowledgeRequestStates.get(nodeId);
    }

    static class IdAndPartition {
        private final Uuid topicId;
        private final int partitionIndex;

        IdAndPartition(Uuid topicId, int partitionIndex) {
            this.topicId = topicId;
            this.partitionIndex = partitionIndex;
        }

        int getPartitionIndex() {
            return partitionIndex;
        }

        Uuid getTopicId() {
            return topicId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicId, partitionIndex);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IdAndPartition that = (IdAndPartition) o;
            return Objects.equals(topicId, that.topicId) &&
                    partitionIndex == that.partitionIndex;
        }
    }

    public enum AcknowledgeRequestType {
        COMMIT_ASYNC((byte) 0),
        COMMIT_SYNC((byte) 1),
        CLOSE((byte) 2);

        public final byte id;

        AcknowledgeRequestType(byte id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }

    }
}
