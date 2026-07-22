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
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
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
 *
 * <p>The request manager keeps track of which share sessions contain which topic-partitions. This information will
 * generally match the leaders in the metadata, but when the fetchable partitions or leadership change, there can be
 * short-lived differences. This is intentional to ensure that topic-partitions which are no longer being
 * fetched and topic-partitions which have changed leaders are cleanly removed from their former share sessions.
 * It is all slightly complicated by the fact that the fetchable partitions are represented by TopicPartition
 * (without topic ID) and, when a topic is recreated, its topic ID changes. All state in share sessions is tracked by
 * topic ID so the current mapping from topic name to topic ID as represented in the share sessions is also maintained
 * in the request manager, and again there can be short-lived differences between this and the cluster metadata.
 */
@SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
public class ShareConsumeRequestManager implements RequestManager, MemberStateListener, Closeable {
    private final Time time;
    private final Logger log;
    private final LogContext logContext;
    private final String groupId;
    private final ShareConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final ShareFetchConfig shareFetchConfig;
    protected final ShareFetchBuffer shareFetchBuffer;
    private final ShareAcknowledgementEventHandler acknowledgeEventHandler;
    private final Map<Integer, ShareSessionHandler> sessionHandlers;
    private final Set<Integer> nodesWithPendingRequests;
    private final ShareFetchMetricsManager metricsManager;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();
    private Uuid memberId;
    private boolean fetchMoreRecords = false;
    private final AtomicInteger fetchRecordsNodeId = new AtomicInteger(-1);
    private final Map<TopicPartition, TopicIdPartition> shareSessionTopicIdMap;
    private final Map<TopicIdPartition, LeaderIdAndEpoch> shareSessionLeaderMap;
    private final Map<Integer, Map<TopicIdPartition, Acknowledgements>> fetchAcknowledgementsToSend;
    private final Map<Integer, Map<TopicIdPartition, Acknowledgements>> fetchAcknowledgementsInFlight;
    private final Map<Integer, Tuple<AcknowledgeRequestState>> acknowledgeRequestStates;
    private final long retryBackoffMs;
    private final long retryBackoffMaxMs;
    private boolean closing = false;
    private final CompletableFuture<Void> closeFuture;
    private boolean isAcknowledgementCommitCallbackRegistered = false;
    private final Map<IdAndPartition, String> topicNamesMap = new HashMap<>();
    private static final String INVALID_RESPONSE = "Acknowledgement not successful due to invalid response from broker";

    ShareConsumeRequestManager(final Time time,
                               final LogContext logContext,
                               final String groupId,
                               final ShareConsumerMetadata metadata,
                               final SubscriptionState subscriptions,
                               final ShareFetchConfig shareFetchConfig,
                               final ShareFetchBuffer shareFetchBuffer,
                               final ShareAcknowledgementEventHandler acknowledgeEventHandler,
                               final ShareFetchMetricsManager metricsManager,
                               final long retryBackoffMs,
                               final long retryBackoffMaxMs) {
        this.time = time;
        this.log = logContext.logger(ShareConsumeRequestManager.class);
        this.logContext = logContext;
        this.groupId = groupId;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.shareFetchConfig = shareFetchConfig;
        this.shareFetchBuffer = shareFetchBuffer;
        this.acknowledgeEventHandler = acknowledgeEventHandler;
        this.metricsManager = metricsManager;
        this.retryBackoffMs = retryBackoffMs;
        this.retryBackoffMaxMs = retryBackoffMaxMs;
        this.sessionHandlers = new HashMap<>();
        this.nodesWithPendingRequests = new HashSet<>();
        this.acknowledgeRequestStates = new HashMap<>();
        this.shareSessionTopicIdMap = new HashMap<>();
        this.shareSessionLeaderMap = new HashMap<>();
        this.fetchAcknowledgementsToSend = new HashMap<>();
        this.fetchAcknowledgementsInFlight = new HashMap<>();
        this.closeFuture = new CompletableFuture<>();
    }

    @Override
    public PollResult poll(long currentTimeMs) {
        if (memberId == null) {
            if (closing && !closeFuture.isDone()) {
                closeFuture.complete(null);
            }
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

        // Iterate over the partitions to fetch, building a map from partition to leader node ID
        Map<Node, ShareSessionHandler> handlerMap = new HashMap<>();
        Cluster cluster = metadata.fetch();
        Map<String, Uuid> topicIds = metadata.topicIds();
        for (TopicPartition partition : partitionsToFetch()) {
            TopicIdPartition tip = shareSessionTopicIdMap.get(partition);
            if (tip == null) {
                Uuid topicId = topicIds.get(partition.topic());
                if (topicId == null) {
                    log.debug("Requesting metadata update for partition {} since topic ID is missing", partition);
                    metadata.requestUpdate(false);
                    continue;
                }

                tip = new TopicIdPartition(topicId, partition);
                shareSessionTopicIdMap.put(partition, tip);
            }

            LeaderIdAndEpoch leader = shareSessionLeaderMap.get(tip);
            if (leader == null || cluster.nodeById(leader.leaderId) == null) {
                Metadata.LeaderAndEpoch leaderOpt = metadata.currentLeader(partition);
                if (leaderOpt.leader.isEmpty() || cluster.nodeById(leaderOpt.leader.get().id()) == null) {
                    log.debug("Requesting metadata update for partition {} since current leader node is missing", partition);
                    metadata.requestUpdate(false);
                    shareSessionLeaderMap.remove(tip);
                    continue;
                }

                shareSessionLeaderMap.put(tip, new LeaderIdAndEpoch(leaderOpt.leader.get().id(), leaderOpt.epoch.orElse(-1)));
            }
        }

        Set<Integer> missingNodes = new HashSet<>();
        for (TopicPartition partition : partitionsToFetch()) {
            TopicIdPartition tip = shareSessionTopicIdMap.get(partition);
            if (tip == null) {
                continue;
            }
            LeaderIdAndEpoch leader = shareSessionLeaderMap.get(tip);
            if (leader == null) {
                continue;
            }
            int nodeId = leader.leaderId;
            Node node = cluster.nodeById(nodeId);
            if (node == null) {
                shareSessionLeaderMap.remove(tip);
                missingNodes.add(nodeId);
                continue;
            }
            if (nodesWithPendingRequests.contains(nodeId)) {
                log.trace("Skipping fetch for partition {} because previous request to {} has not been processed", partition, nodeId);
            } else {
                ShareSessionHandler handler = handlerMap.computeIfAbsent(node,
                    k -> sessionHandlers.computeIfAbsent(nodeId, n -> new ShareSessionHandler(logContext, n, memberId)));

                Acknowledgements acknowledgementsToSend = null;

                Map<TopicIdPartition, Acknowledgements> nodeAcksFromFetchMap = fetchAcknowledgementsToSend.get(nodeId);
                if (nodeAcksFromFetchMap != null) {
                    acknowledgementsToSend = nodeAcksFromFetchMap.remove(tip);

                    if (acknowledgementsToSend != null) {
                        // Check if the share session epoch is valid for sending acknowledgements.
                        if (!maybeAddAcknowledgements(handler, node, tip, acknowledgementsToSend)) {
                            acknowledgementsToSend = null;
                        }
                    }
                }

                handler.addPartitionToFetch(tip, acknowledgementsToSend);
                topicNamesMap.putIfAbsent(new IdAndPartition(tip.topicId(), tip.partition()), tip.topic());

                // If we have not chosen a node for fetching records yet, choose now, and rotate the assigned partitions
                // so the next poll starts on a different partition. This is only applicable for record_limit mode.
                if (isShareAcquireModeRecordLimit() && fetchRecordsNodeId.compareAndSet(-1, nodeId)) {
                    subscriptions.movePartitionToEnd(partition);
                }

                log.debug("Added fetch request for partition {} to node {}", tip, nodeId);
            }
        }

        // Iterate over the session handlers to see if there are acknowledgements to be sent for partitions
        // which are no longer part of the current subscription or which have disappeared from the metadata.
        // Also include session handlers which have no fetchable partitions so the share sessions can be
        // brought up to date.
        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Node node = cluster.nodeById(nodeId);
            if (node == null) {
                missingNodes.add(nodeId);
                return;
            }
            if (nodesWithPendingRequests.contains(nodeId)) {
                log.trace("Skipping fetch because previous request to {} has not been processed", nodeId);
            } else {
                prepareRemainingSessionHandlerForRequest(node, sessionHandler, handlerMap);
            }
        });

        // Iterate over the share session handlers and build a list of UnsentRequests.
        List<UnsentRequest> requests = handlerMap.entrySet().stream().map(entry -> {
            Node target = entry.getKey();
            ShareSessionHandler handler = entry.getValue();

            // For record_limit mode, we only send a full ShareFetch to a single node at a time. We prepare to build ShareFetch
            // requests for all nodes with session handlers to permit piggybacking of acknowledgements, and also to adjust the
            // topic-partitions in the share session, but if the request would contain neither of those, it can be skipped.
            boolean canSkipIfRequestEmpty = isShareAcquireModeRecordLimit() && target.id() != fetchRecordsNodeId.get();

            ShareFetchRequest.Builder requestBuilder = handler.newShareFetchBuilder(groupId, shareFetchConfig, canSkipIfRequestEmpty);
            if (requestBuilder == null) {
                log.trace("Skipping ShareFetch request to send to node {}", target.id());
                return null;
            }

            log.trace("Building ShareFetch request to send to node {}", target.id());

            nodesWithPendingRequests.add(target.id());

            BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                if (error != null) {
                    handleShareFetchFailure(target, requestBuilder.data(), error);
                } else {
                    handleShareFetchSuccess(target, requestBuilder.data(), clientResponse);
                }
            };
            return new UnsentRequest(requestBuilder, Optional.of(target)).whenComplete(responseHandler);
        }).filter(Objects::nonNull).collect(Collectors.toList());

        // Remove session handlers for nodes which are now missing from the cluster metadata.
        if (!missingNodes.isEmpty()) {
            removeSessionHandlersForMissingNodes(missingNodes);
        }

        return new PollResult(requests);
    }

    private boolean isShareAcquireModeRecordLimit() {
        return shareFetchConfig.shareAcquireMode == ShareAcquireMode.RECORD_LIMIT;
    }

    /**
     * Add acknowledgements for a topic-partition to the node's in-flight acknowledgements.
     * If we cannot add acknowledgements, they are completed with {@link Errors#NETWORK_EXCEPTION} exception.
     * This probably indicates the connection to the leader broker was lost, but then re-established without a
     * leadership change, in which case the acknowledgements fail.
     *
     * @return True if we can add acknowledgements to the share session.
     */
    private boolean maybeAddAcknowledgements(ShareSessionHandler handler,
                                             Node node,
                                             TopicIdPartition tip,
                                             Acknowledgements acknowledgements) {
        if (handler.isNewSession()) {
            // Failing the acknowledgements as we cannot have piggybacked acknowledgements in the initial ShareFetchRequest.
            log.debug("Cannot send acknowledgements on initial epoch for ShareSession for partition {}", tip);
            acknowledgements.complete(Errors.NETWORK_EXCEPTION.exception());
            maybeSendShareAcknowledgementEvent(Map.of(tip, acknowledgements), true, Optional.empty());
            return false;
        } else {
            metricsManager.recordAcknowledgementSent(acknowledgements.size());
            fetchAcknowledgementsInFlight.computeIfAbsent(node.id(), k -> new HashMap<>()).put(tip, acknowledgements);
            return true;
        }
    }

    /**
     * Iterate over the session handlers to see if there are acknowledgements to be sent for partitions
     * which are no longer part of the current subscription or which have disappeared from the metadata.
     * Also include session handlers which have no fetchable partitions so the share sessions can be
     * brought up to date.
     */
    private void prepareRemainingSessionHandlerForRequest(Node node, ShareSessionHandler sessionHandler, Map<Node, ShareSessionHandler> handlerMap) {
        Map<TopicIdPartition, Acknowledgements> nodeAcksFromFetchMap = fetchAcknowledgementsToSend.get(node.id());
        if (nodeAcksFromFetchMap != null) {
            nodeAcksFromFetchMap.forEach((tip, acks) -> {
                if (!isLeaderKnownToHaveChanged(node.id(), tip)) {
                    // Check if the share session epoch is valid for sending acknowledgements.
                    if (!maybeAddAcknowledgements(sessionHandler, node, tip, acks)) {
                        return;
                    }

                    sessionHandler.addPartitionToAcknowledgeOnly(tip, acks);

                    topicNamesMap.putIfAbsent(new IdAndPartition(tip.topicId(), tip.partition()), tip.topic());
                    log.debug("Added fetch request for previously subscribed partition {} to node {}", tip, node.id());
                } else {
                    log.debug("Leader for the partition is down or has changed, failing acknowledgements for partition {}", tip);
                    acks.complete(Errors.NETWORK_EXCEPTION.exception());
                    maybeSendShareAcknowledgementEvent(Map.of(tip, acks), true, Optional.empty());
                }
            });

            nodeAcksFromFetchMap.clear();
            handlerMap.put(node, sessionHandler);
        } else {
            handlerMap.putIfAbsent(node, sessionHandler);
        }
    }

    /**
     * Remove session handlers for nodes which are now missing from the cluster metadata. Only nodes with no
     * in-flight requests are removed, since those requests will complete. Piggyback acknowledgements for the
     * missing nodes are completed with {@link Errors#NETWORK_EXCEPTION}.
     */
    private void removeSessionHandlersForMissingNodes(Set<Integer> missingNodes) {
        Cluster cluster = metadata.fetch();
        missingNodes.forEach(nodeId -> {
            Node node = cluster.nodeById(nodeId);
            if (node == null && !nodesWithPendingRequests.contains(nodeId)) {
                Map<TopicIdPartition, Acknowledgements> nodeAcksFromFetchMap = fetchAcknowledgementsToSend.remove(nodeId);
                if (nodeAcksFromFetchMap != null) {
                    nodeAcksFromFetchMap.forEach((tip, acks) -> {
                        log.debug("Node {} is no longer in the cluster metadata, failing acknowledgements for partition {}", nodeId, tip);
                        acks.complete(Errors.NETWORK_EXCEPTION.exception());
                        maybeSendShareAcknowledgementEvent(Map.of(tip, acks), true, Optional.empty());
                    });
                }

                log.debug("Removing session handler for node {} which is no longer in the cluster metadata", nodeId);
                sessionHandlers.remove(nodeId);
            }
        });
    }

    public void fetch(Map<TopicIdPartition, NodeAcknowledgements> acknowledgementsMap) {
        if (!fetchMoreRecords) {
            log.debug("Fetch more data");
            fetchMoreRecords = true;
        }

        // Store the acknowledgements and send them in the next ShareFetch.
        processAcknowledgementsMap(acknowledgementsMap);
    }

    private void processAcknowledgementsMap(Map<TopicIdPartition, NodeAcknowledgements> acknowledgementsMap) {
        acknowledgementsMap.forEach((tip, nodeAcks) -> {
            int nodeId = nodeAcks.nodeId();
            Map<TopicIdPartition, Acknowledgements> currentNodeAcknowledgementsMap = fetchAcknowledgementsToSend.get(nodeId);
            if (currentNodeAcknowledgementsMap != null) {
                Acknowledgements currentAcknowledgementsForNode = currentNodeAcknowledgementsMap.get(tip);
                if (currentAcknowledgementsForNode != null) {
                    currentAcknowledgementsForNode.merge(nodeAcks.acknowledgements());
                } else {
                    currentNodeAcknowledgementsMap.put(tip, nodeAcks.acknowledgements());
                }
            } else {
                Map<TopicIdPartition, Acknowledgements> nodeAcknowledgementsMap = new HashMap<>();
                nodeAcknowledgementsMap.put(tip, nodeAcks.acknowledgements());
                fetchAcknowledgementsToSend.put(nodeId, nodeAcknowledgementsMap);
            }
        });
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

                // First, the acknowledgements from commitAsync are sent.
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
                            if (!isNodeFree(nodeId)) {
                                log.trace("Skipping acknowledge request because previous request to {} has not been processed, so acks are not sent", nodeId);
                                break;
                            }
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

    private void maybeSendShareAcknowledgementEvent(Map<TopicIdPartition, Acknowledgements> acknowledgementsMap,
                                                    boolean checkForRenewAcknowledgements,
                                                    Optional<Integer> acquisitionLockTimeoutMs) {
        if (isAcknowledgementCommitCallbackRegistered || checkForRenewAcknowledgements) {
            ShareAcknowledgementEvent event = new ShareAcknowledgementEvent(acknowledgementsMap, checkForRenewAcknowledgements, acquisitionLockTimeoutMs);
            acknowledgeEventHandler.add(event);
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
            if (acknowledgeRequestState == null ||
                    (!acknowledgeRequestState.isCloseRequest() && acknowledgeRequestState.isEmpty()) ||
                    (acknowledgeRequestState.isCloseRequest() && acknowledgeRequestState.isProcessed)) {
                return Optional.empty();
            }

            if (acknowledgeRequestState.maybeExpire()) {
                // Fill in TimeoutException
                for (TopicIdPartition tip : acknowledgeRequestState.incompleteAcknowledgements.keySet()) {
                    metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getIncompleteAcknowledgementsCount(tip));
                    acknowledgeRequestState.handleAcknowledgeTimedOut(tip);
                }
                acknowledgeRequestState.incompleteAcknowledgements.clear();
                // Reset timer for any future processing on the same request state.
                acknowledgeRequestState.maybeResetTimerAndRequestState();
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
        } else if (acknowledgeRequestState.isCloseRequest()) {
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
            final Map<TopicIdPartition, NodeAcknowledgements> acknowledgementsMap,
            final long deadlineMs) {
        final Cluster cluster = metadata.fetch();
        final AtomicInteger resultCount = new AtomicInteger();
        final CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future = new CompletableFuture<>();
        final ResultHandler resultHandler = new ResultHandler(resultCount, Optional.of(future));

        Map<Integer, Map<TopicIdPartition, Acknowledgements>> acknowledgementsMapAllNodes = new HashMap<>();
        Map<TopicIdPartition, Acknowledgements> acknowledgementsMapCannotSend = new HashMap<>();
        Map<TopicIdPartition, Errors> acknowledgementsMapCannotSendErrors = new HashMap<>();
        acknowledgementsMap.forEach((tip, nodeAcks) -> {
            if ((cluster.nodeById(nodeAcks.nodeId()) == null) || isLeaderKnownToHaveChanged(nodeAcks.nodeId(), tip)) {
                Acknowledgements prevAcks = acknowledgementsMapCannotSend.putIfAbsent(tip, nodeAcks.acknowledgements());
                if (prevAcks != null) {
                    prevAcks.merge(nodeAcks.acknowledgements());
                } else {
                    acknowledgementsMapCannotSendErrors.put(tip, acknowledgementsCannotBeSentError(nodeAcks.nodeId(), tip));
                }
            } else {
                Map<TopicIdPartition, Acknowledgements> acksMap = acknowledgementsMapAllNodes.computeIfAbsent(nodeAcks.nodeId(), k -> new HashMap<>());
                Acknowledgements prevAcks = acksMap.putIfAbsent(tip, nodeAcks.acknowledgements());
                if (prevAcks != null) {
                    prevAcks.merge(nodeAcks.acknowledgements());
                }
            }
        });

        resultCount.addAndGet(acknowledgementsMapCannotSend.size());

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Map<TopicIdPartition, Acknowledgements> nodeAcknowledgements = acknowledgementsMapAllNodes.get(nodeId);
            if (nodeAcknowledgements == null)
                return;

            Map<TopicIdPartition, Acknowledgements> acknowledgementsMapToSend = new HashMap<>();

            acknowledgeRequestStates.putIfAbsent(nodeId, new Tuple<>(null, null, null));

            // Add the incoming commitSync() request to the queue.
            for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                Acknowledgements acknowledgements = nodeAcknowledgements.remove(tip);
                if (acknowledgements != null) {
                    acknowledgementsMapToSend.put(tip, acknowledgements);
                    resultCount.incrementAndGet();

                    metricsManager.recordAcknowledgementSent(acknowledgements.size());
                    log.debug("Added sync acknowledge request for partition {} to node {}", tip.topicPartition(), nodeId);
                }
            }

            resultCount.addAndGet(nodeAcknowledgements.size());
            nodeAcknowledgements.forEach((tip, acks) -> {
                acks.complete(Errors.NOT_LEADER_OR_FOLLOWER.exception());
                resultHandler.complete(tip, acks, AcknowledgeRequestType.COMMIT_SYNC, true, Optional.empty());
            });

            if (!acknowledgementsMapToSend.isEmpty()) {
                acknowledgeRequestStates.get(nodeId).addSyncRequest(new AcknowledgeRequestState(logContext,
                    ShareConsumeRequestManager.class.getSimpleName() + ":1",
                    deadlineMs,
                    retryBackoffMs,
                    retryBackoffMaxMs,
                    sessionHandler,
                    nodeId,
                    acknowledgementsMapToSend,
                    resultHandler,
                    AcknowledgeRequestType.COMMIT_SYNC
                ));
            }
        });

        acknowledgementsMapCannotSend.forEach((tip, acks) -> {
            acks.complete(acknowledgementsMapCannotSendErrors.get(tip).exception());
            resultHandler.complete(tip, acks, AcknowledgeRequestType.COMMIT_SYNC, true, Optional.empty());
        });

        resultHandler.completeIfEmpty();
        return future;
    }

    /**
     * Enqueue an AcknowledgeRequestState to be picked up on the next poll.
     *
     * @param acknowledgementsMap The acknowledgements to commit
     * @param deadlineMs          Time until which the request will be retried if it fails with
     *                            an expected retriable error.
     */
    public void commitAsync(
            final Map<TopicIdPartition, NodeAcknowledgements> acknowledgementsMap,
            final long deadlineMs) {
        final Cluster cluster = metadata.fetch();
        final ResultHandler resultHandler = new ResultHandler(Optional.empty());

        Map<Integer, Map<TopicIdPartition, Acknowledgements>> acknowledgementsMapAllNodes = new HashMap<>();
        acknowledgementsMap.forEach((tip, nodeAcks) -> {
            if ((cluster.nodeById(nodeAcks.nodeId()) == null) || isLeaderKnownToHaveChanged(nodeAcks.nodeId(), tip)) {
                log.debug("Leader for the partition is down or has changed, failing acknowledgements for partition {}", tip);
                nodeAcks.acknowledgements().complete(acknowledgementsCannotBeSentError(nodeAcks.nodeId(), tip).exception());
                maybeSendShareAcknowledgementEvent(Map.of(tip, nodeAcks.acknowledgements()), true, Optional.empty());
            } else {
                Map<TopicIdPartition, Acknowledgements> acksMap = acknowledgementsMapAllNodes.computeIfAbsent(nodeAcks.nodeId(), k -> new HashMap<>());
                Acknowledgements prevAcks = acksMap.putIfAbsent(tip, nodeAcks.acknowledgements());
                if (prevAcks != null) {
                    prevAcks.merge(nodeAcks.acknowledgements());
                }
            }
        });

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Map<TopicIdPartition, Acknowledgements> nodeAcknowledgements = acknowledgementsMapAllNodes.get(nodeId);
            if (nodeAcknowledgements == null)
                return;

            Map<TopicIdPartition, Acknowledgements> acknowledgementsMapForNode = new HashMap<>();

            acknowledgeRequestStates.putIfAbsent(nodeId, new Tuple<>(null, null, null));

            for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                Acknowledgements acknowledgements = nodeAcknowledgements.remove(tip);
                if (acknowledgements != null) {
                    acknowledgementsMapForNode.put(tip, acknowledgements);

                    metricsManager.recordAcknowledgementSent(acknowledgements.size());
                    log.debug("Added async acknowledge request for partition {} to node {}", tip.topicPartition(), nodeId);
                    AcknowledgeRequestState asyncRequestState = acknowledgeRequestStates.get(nodeId).getAsyncRequest();
                    if (asyncRequestState == null) {
                        acknowledgeRequestStates.get(nodeId).setAsyncRequest(new AcknowledgeRequestState(logContext,
                            ShareConsumeRequestManager.class.getSimpleName() + ":2",
                            deadlineMs,
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

            nodeAcknowledgements.forEach((tip, acks) -> {
                acks.complete(Errors.NOT_LEADER_OR_FOLLOWER.exception());
                maybeSendShareAcknowledgementEvent(Map.of(tip, acks), true, Optional.empty());
            });
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
    public CompletableFuture<Void> acknowledgeOnClose(
            final Map<TopicIdPartition, NodeAcknowledgements> acknowledgementsMap,
            final long deadlineMs) {
        final Cluster cluster = metadata.fetch();
        final AtomicInteger resultCount = new AtomicInteger();
        final ResultHandler resultHandler = new ResultHandler(resultCount, Optional.empty());

        closing = true;

        Map<Integer, Map<TopicIdPartition, Acknowledgements>> acknowledgementsMapAllNodes = new HashMap<>();
        acknowledgementsMap.forEach((tip, nodeAcks) -> {
            if ((cluster.nodeById(nodeAcks.nodeId()) == null) || isLeaderKnownToHaveChanged(nodeAcks.nodeId(), tip)) {
                nodeAcks.acknowledgements().complete(acknowledgementsCannotBeSentError(nodeAcks.nodeId(), tip).exception());
                maybeSendShareAcknowledgementEvent(Map.of(tip, nodeAcks.acknowledgements()), true, Optional.empty());
            } else {
                Map<TopicIdPartition, Acknowledgements> acksMap = acknowledgementsMapAllNodes.computeIfAbsent(nodeAcks.nodeId(), k -> new HashMap<>());
                Acknowledgements prevAcks = acksMap.putIfAbsent(tip, nodeAcks.acknowledgements());
                if (prevAcks != null) {
                    prevAcks.merge(nodeAcks.acknowledgements());
                }
            }
        });

        // Add any waiting piggyback acknowledgements.
        fetchAcknowledgementsToSend.forEach((nodeId, nodeAcks) ->
            nodeAcks.forEach((tip, acks) -> {
                if ((cluster.nodeById(nodeId) == null) || isLeaderKnownToHaveChanged(nodeId, tip)) {
                    acks.complete(acknowledgementsCannotBeSentError(nodeId, tip).exception());
                    maybeSendShareAcknowledgementEvent(Map.of(tip, acks), true, Optional.empty());
                } else {
                    Map<TopicIdPartition, Acknowledgements> acksMap = acknowledgementsMapAllNodes.computeIfAbsent(nodeId, k -> new HashMap<>());
                    Acknowledgements prevAcks = acksMap.putIfAbsent(tip, acks);
                    if (prevAcks != null) {
                        prevAcks.merge(acks);
                    }
                }
            })
        );

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Map<TopicIdPartition, Acknowledgements> nodeAcknowledgements = acknowledgementsMapAllNodes.get(nodeId);
            if (nodeAcknowledgements != null) {
                nodeAcknowledgements.forEach((tip, acknowledgements) -> {
                    resultCount.incrementAndGet();

                    metricsManager.recordAcknowledgementSent(acknowledgements.size());
                    log.debug("Added closing acknowledge request for partition {} to node {}", tip.topicPartition(), nodeId);
                });
            } else {
                nodeAcknowledgements = new HashMap<>();
            }

            acknowledgeRequestStates.putIfAbsent(nodeId, new Tuple<>(null, null, null));

            // Ensure there is no close() request already present as they are blocking calls
            // and only one request can be active at a time.
            if (acknowledgeRequestStates.get(nodeId).getCloseRequest() != null && isRequestStateInProgress(acknowledgeRequestStates.get(nodeId).getCloseRequest())) {
                log.error("Attempt to call close() when there is an existing close request for node {}-{}", nodeId, acknowledgeRequestStates.get(nodeId).getSyncRequestQueue());
                nodeAcknowledgements.forEach((tip, acks) -> {
                    acks.complete(Errors.NOT_LEADER_OR_FOLLOWER.exception());
                    maybeSendShareAcknowledgementEvent(Map.of(tip, acks), true, Optional.empty());
                });
                closeFuture.completeExceptionally(new IllegalStateException("Attempt to call close() when there is an existing close request for node " + nodeId));
            } else {
                // There can only be one close() happening at a time. So per node, there will be one acknowledge request state.
                acknowledgeRequestStates.get(nodeId).setCloseRequest(
                    new AcknowledgeRequestState(logContext,
                        ShareConsumeRequestManager.class.getSimpleName() + ":3",
                        deadlineMs,
                        retryBackoffMs,
                        retryBackoffMaxMs,
                        sessionHandler,
                        nodeId,
                        nodeAcknowledgements,
                        resultHandler,
                        AcknowledgeRequestType.CLOSE
                ));
            }
        });

        resultHandler.completeIfEmpty();
        return closeFuture;
    }

    /**
     * The method checks whether the leader for a topicIdPartition has changed.
     *
     * @param nodeId The previous leader for the partition.
     * @param topicIdPartition The TopicIdPartition to check.
     * @return Returns true if leader information is available and leader has changed.
     * If the leader information is not available or if the leader has not changed, it returns false.
     */
    private boolean isLeaderKnownToHaveChanged(int nodeId, TopicIdPartition topicIdPartition) {
        Optional<Node> leaderNode = metadata.currentLeader(topicIdPartition.topicPartition()).leader;
        if (leaderNode.isPresent()) {
            if (leaderNode.get().id() != nodeId) {
                log.debug("Node {} is no longer the leader for partition {}, failing acknowledgements", nodeId, topicIdPartition);
                return true;
            }
        } else {
            log.debug("No leader found for partition {}", topicIdPartition);
            metadata.requestUpdate(false);
            return false;
        }
        return false;
    }

    private void handleShareFetchSuccess(Node fetchTarget,
                                         ShareFetchRequestData requestData,
                                         ClientResponse resp) {
        try {
            log.debug("Completed ShareFetch request from node {} successfully", fetchTarget.id());
            final ShareFetchResponse response = (ShareFetchResponse) resp.responseBody();
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

            metricsManager.recordLatency(resp.destination(), resp.requestLatencyMs());

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
                // Complete any in-flight acknowledgements with the error code from the response, unless the share session was lost,
                // in which case they are failed with NETWORK_EXCEPTION reflecting a loss of connectivity.
                final Errors ackError;
                if (response.error() == Errors.SHARE_SESSION_NOT_FOUND || response.error() == Errors.INVALID_SHARE_SESSION_EPOCH) {
                    ackError = Errors.NETWORK_EXCEPTION;
                } else {
                    ackError = response.error();
                }
                Map<TopicIdPartition, Acknowledgements> nodeAcknowledgementsInFlight = fetchAcknowledgementsInFlight.remove(fetchTarget.id());
                if (nodeAcknowledgementsInFlight != null && !nodeAcknowledgementsInFlight.isEmpty()) {
                    nodeAcknowledgementsInFlight.forEach((tip, acks) -> {
                        acks.complete(ackError.exception());
                        metricsManager.recordFailedAcknowledgements(acks.size());
                    });
                    maybeSendShareAcknowledgementEvent(nodeAcknowledgementsInFlight, requestData.isRenewAck(), Optional.empty());
                }
                return;
            }

            final Map<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData = new LinkedHashMap<>();
            final Optional<Integer> responseAcquisitionLockTimeoutMs = response.data().acquisitionLockTimeoutMs() > 0
                ? Optional.of(response.data().acquisitionLockTimeoutMs()) : Optional.empty();

            response.data().responses().forEach(topicResponse ->
                topicResponse.partitions().forEach(partition -> {
                    TopicIdPartition tip = lookupTopicId(topicResponse.topicId(), partition.partitionIndex());
                    if (tip != null) {
                        responseData.put(tip, partition);
                    }
                })
            );

            final Set<TopicPartition> partitions = responseData.keySet().stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet());
            final ShareFetchMetricsAggregator shareFetchMetricsAggregator = new ShareFetchMetricsAggregator(metricsManager, partitions);

            List<ShareCompletedFetch> completedFetches = new ArrayList<>(responseData.size());
            Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();
            for (Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> entry : responseData.entrySet()) {
                TopicIdPartition tip = entry.getKey();

                ShareFetchResponseData.PartitionData partitionData = entry.getValue();

                log.debug("ShareFetch for partition {} returned fetch data {}", tip, partitionData);

                Map<TopicIdPartition, Acknowledgements> nodeAcknowledgementsInFlight = fetchAcknowledgementsInFlight.get(fetchTarget.id());
                if (nodeAcknowledgementsInFlight != null) {
                    Acknowledgements acks = nodeAcknowledgementsInFlight.remove(tip);
                    if (acks != null) {
                        if (partitionData.acknowledgeErrorCode() != Errors.NONE.code()) {
                            metricsManager.recordFailedAcknowledgements(acks.size());
                        }
                        acks.complete(Errors.forCode(partitionData.acknowledgeErrorCode())
                                .exception(partitionData.acknowledgeErrorMessage()));
                        Map<TopicIdPartition, Acknowledgements> acksMap = Map.of(tip, acks);
                        maybeSendShareAcknowledgementEvent(acksMap, requestData.isRenewAck(), responseAcquisitionLockTimeoutMs);
                    }
                }

                Errors partitionError = Errors.forCode(partitionData.errorCode());
                if (partitionError == Errors.NOT_LEADER_OR_FOLLOWER || partitionError == Errors.FENCED_LEADER_EPOCH) {
                    log.debug("For {}, received error {}, with leaderIdAndEpoch {} in ShareFetch", tip, partitionError, partitionData.currentLeader());
                    if (partitionData.currentLeader().leaderId() != -1 && partitionData.currentLeader().leaderEpoch() != -1) {
                        partitionsWithUpdatedLeaderInfo.put(tip.topicPartition(), new Metadata.LeaderIdAndEpoch(
                            Optional.of(partitionData.currentLeader().leaderId()), Optional.of(partitionData.currentLeader().leaderEpoch())));

                        maybeUpdateLeaderCache(tip, partitionData.currentLeader().leaderId(), partitionData.currentLeader().leaderEpoch());
                    } else {
                        shareSessionLeaderMap.remove(tip);
                    }
                } else if (partitionError == Errors.UNKNOWN_TOPIC_OR_PARTITION || partitionError == Errors.UNKNOWN_TOPIC_ID) {
                    shareSessionLeaderMap.remove(tip);
                    shareSessionTopicIdMap.remove(tip.topicPartition());
                }

                completedFetches.add(
                    new ShareCompletedFetch(
                        logContext,
                        BufferSupplier.create(),
                        fetchTarget.id(),
                        tip,
                        partitionData,
                        responseAcquisitionLockTimeoutMs,
                        shareFetchMetricsAggregator,
                        requestVersion)
                );

                if (!partitionData.acquiredRecords().isEmpty()) {
                    fetchMoreRecords = false;
                }
            }

            if (!completedFetches.isEmpty()) {
                shareFetchBuffer.add(completedFetches);
            }

            // Handle any acknowledgements which were not received in the response for this node.
            if (fetchAcknowledgementsInFlight.get(fetchTarget.id()) != null) {
                fetchAcknowledgementsInFlight.remove(fetchTarget.id()).forEach((partition, acknowledgements) -> {
                    acknowledgements.complete(new InvalidRecordStateException(INVALID_RESPONSE));
                    maybeSendShareAcknowledgementEvent(Map.of(partition, acknowledgements), true, Optional.empty());
                });
            }

            if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                List<Node> leaderNodes = response.data().nodeEndpoints().stream()
                    .map(e -> new Node(e.nodeId(), e.host(), e.port(), e.rack()))
                    .filter(e -> !e.equals(Node.noNode()))
                    .collect(Collectors.toList());
                metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
            }
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget.id());
            if (isShareAcquireModeRecordLimit()) {
                fetchRecordsNodeId.compareAndSet(fetchTarget.id(), -1);
            }
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
                TopicIdPartition tip = lookupTopicId(topic.topicId(), partition.partitionIndex());
                if (tip == null) {
                    return;
                }

                Map<TopicIdPartition, Acknowledgements> nodeAcknowledgementsInFlight = fetchAcknowledgementsInFlight.get(fetchTarget.id());
                if (nodeAcknowledgementsInFlight != null) {
                    Acknowledgements acks = nodeAcknowledgementsInFlight.remove(tip);
                    if (acks != null) {
                        metricsManager.recordFailedAcknowledgements(acks.size());
                        if (error instanceof KafkaException) {
                            acks.complete((KafkaException) error);
                        } else {
                            acks.complete(Errors.UNKNOWN_SERVER_ERROR.exception());
                        }
                        Map<TopicIdPartition, Acknowledgements> acksMap = Map.of(tip, acks);
                        maybeSendShareAcknowledgementEvent(acksMap, requestData.isRenewAck(), Optional.empty());
                    }
                }
            }));
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget.id());
            if (isShareAcquireModeRecordLimit()) {
                fetchRecordsNodeId.compareAndSet(fetchTarget.id(), -1);
            }
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
            final Optional<Integer> responseAcquisitionLockTimeoutMs = response.data().acquisitionLockTimeoutMs() > 0
                ? Optional.of(response.data().acquisitionLockTimeoutMs()) : Optional.empty();

            Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo = new HashMap<>();

            if (acknowledgeRequestState.isCloseRequest()) {
                response.data().responses().forEach(topicResponse -> topicResponse.partitions().forEach(partitionData -> {
                    TopicIdPartition tip = lookupTopicId(topicResponse.topicId(), partitionData.partitionIndex());
                    if (tip == null) {
                        return;
                    }

                    if (partitionData.errorCode() != Errors.NONE.code()) {
                        metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                    }

                    acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forCode(partitionData.errorCode()), requestData.isRenewAck(), responseAcquisitionLockTimeoutMs);
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
                        acknowledgeRequestState.processPendingInFlightAcknowledgements(response.error().exception());
                        acknowledgeRequestState.processingComplete();
                    }
                } else {
                    AtomicBoolean shouldRetry = new AtomicBoolean(false);

                    // Check all partition-level error codes.
                    response.data().responses().forEach(topicResponse -> topicResponse.partitions().forEach(partitionData -> {
                        Errors partitionError = Errors.forCode(partitionData.errorCode());
                        TopicIdPartition tip = lookupTopicId(topicResponse.topicId(), partitionData.partitionIndex());
                        if (tip == null) {
                            return;
                        }

                        handlePartitionError(partitionData, partitionsWithUpdatedLeaderInfo, acknowledgeRequestState,
                            partitionError, tip, shouldRetry, requestData.isRenewAck(), responseAcquisitionLockTimeoutMs);
                    }));

                    processRetryLogic(acknowledgeRequestState, shouldRetry, responseCompletionTimeMs);
                }
            }

            if (!partitionsWithUpdatedLeaderInfo.isEmpty()) {
                List<Node> leaderNodes = response.data().nodeEndpoints().stream()
                    .map(e -> new Node(e.nodeId(), e.host(), e.port(), e.rack()))
                    .filter(e -> !e.equals(Node.noNode()))
                    .collect(Collectors.toList());
                metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes);
            }

        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());

            if (acknowledgeRequestState.isCloseRequest()) {
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
                TopicIdPartition tip = lookupTopicId(topic.topicId(), partition.partitionIndex());
                if (tip == null) {
                    return;
                }

                metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forException(error), requestData.isRenewAck(), Optional.empty());
            }));

            acknowledgeRequestState.processingComplete();
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget.id());
            nodesWithPendingRequests.remove(fetchTarget.id());

            if (acknowledgeRequestState.isCloseRequest()) {
                log.debug("Removing node from ShareSession {}", fetchTarget.id());
                sessionHandlers.remove(fetchTarget.id());
            }
        }
    }

    private void handlePartitionError(ShareAcknowledgeResponseData.PartitionData partitionData,
                                      Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo,
                                      AcknowledgeRequestState acknowledgeRequestState,
                                      Errors partitionError,
                                      TopicIdPartition tip,
                                      AtomicBoolean shouldRetry,
                                      boolean isRenewAck,
                                      Optional<Integer> acquisitionLockTimeoutMs) {
        if (partitionError.exception() != null) {
            boolean retry = false;
            if (partitionError == Errors.NOT_LEADER_OR_FOLLOWER || partitionError == Errors.FENCED_LEADER_EPOCH) {
                // If the leader has changed, there's no point in retrying the operation because the acquisition locks
                // will have been released. Instead, these records will be re-delivered once they get timed out on the broker.
                // If the topic or partition has been deleted, we do not retry the failed acknowledgements.
                updateLeaderInfoMap(partitionData, partitionsWithUpdatedLeaderInfo, partitionError, tip);
            } else if (partitionError == Errors.UNKNOWN_TOPIC_OR_PARTITION || partitionError == Errors.UNKNOWN_TOPIC_ID) {
                shareSessionLeaderMap.remove(tip);
                shareSessionTopicIdMap.remove(tip.topicPartition());
            } else if (partitionError.exception() instanceof RetriableException) {
                retry = true;
            }

            if (retry) {
                if (acknowledgeRequestState.moveToIncompleteAcks(tip)) {
                    shouldRetry.set(true);
                }
            } else {
                metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getInFlightAcknowledgementsCount(tip));
                acknowledgeRequestState.handleAcknowledgeErrorCode(tip, partitionError, isRenewAck, Optional.empty());
            }
        } else {
            acknowledgeRequestState.handleAcknowledgeErrorCode(tip, partitionError, isRenewAck, acquisitionLockTimeoutMs);
        }
    }

    private void processRetryLogic(AcknowledgeRequestState acknowledgeRequestState,
                                   AtomicBoolean shouldRetry,
                                   long responseCompletionTimeMs) {
        if (shouldRetry.get()) {
            acknowledgeRequestState.onFailedAttempt(responseCompletionTimeMs);

            // Check for any acknowledgements that did not receive a response.
            // These acknowledgements are failed with InvalidRecordStateException.
            acknowledgeRequestState.processPendingInFlightAcknowledgements(new InvalidRecordStateException(INVALID_RESPONSE));
        } else {
            acknowledgeRequestState.onSuccessfulAttempt(responseCompletionTimeMs);
            acknowledgeRequestState.processingComplete();
        }
    }

    private void updateLeaderInfoMap(ShareAcknowledgeResponseData.PartitionData partitionData,
                                  Map<TopicPartition, Metadata.LeaderIdAndEpoch> partitionsWithUpdatedLeaderInfo,
                                  Errors partitionError,
                                  TopicIdPartition tip) {

        log.debug("For {}, received error {}, with leaderIdAndEpoch {} in ShareAcknowledge", tip, partitionError, partitionData.currentLeader());
        if (partitionData.currentLeader().leaderId() != -1 && partitionData.currentLeader().leaderEpoch() != -1) {
            partitionsWithUpdatedLeaderInfo.put(tip.topicPartition(),
                new Metadata.LeaderIdAndEpoch(
                    Optional.of(partitionData.currentLeader().leaderId()),
                    Optional.of(partitionData.currentLeader().leaderEpoch())
                ));

            maybeUpdateLeaderCache(tip, partitionData.currentLeader().leaderId(), partitionData.currentLeader().leaderEpoch());
        } else {
            shareSessionLeaderMap.remove(tip);
        }
    }

    /**
     * Update the cache leader for a partition in a share session, only if the new leader epoch is newer than the
     * currently cached epoch. This mirrors the rules applied by {@link Metadata#updateLastSeenEpochIfNewer(TopicPartition, int)}.
     * A stale entry is never overwritten by an older epoch.
     */
    private void maybeUpdateLeaderCache(TopicIdPartition tip, int leaderId, int leaderEpoch) {
        LeaderIdAndEpoch oldLeader = shareSessionLeaderMap.get(tip);
        if ((oldLeader == null) || (leaderEpoch > oldLeader.epoch)) {
            shareSessionLeaderMap.put(tip, new LeaderIdAndEpoch(leaderId, leaderEpoch));
        }
    }

    /**
     * Chooses the error used to fail acknowledgements which could not be sent to the node hosting the share session.
     * If leadership has moved to a different live broker, the error is {@link Errors#NOT_LEADER_OR_FOLLOWER}.
     * Otherwise, the error is {@link Errors#NETWORK_EXCEPTION}.
     */
    private Errors acknowledgementsCannotBeSentError(int nodeId, TopicIdPartition tip) {
        return isLeaderKnownToHaveChanged(nodeId, tip) ? Errors.NOT_LEADER_OR_FOLLOWER : Errors.NETWORK_EXCEPTION;
    }

    private TopicIdPartition lookupTopicId(Uuid topicId, int partitionIndex) {
        String topicName = metadata.topicNames().get(topicId);
        if (topicName == null) {
            topicName = topicNamesMap.remove(new IdAndPartition(topicId, partitionIndex));
        }
        if (topicName == null) {
            log.error("Topic name not found in metadata for topicId {} and partitionIndex {}", topicId, partitionIndex);
            return null;
        }
        return new TopicIdPartition(topicId, partitionIndex, topicName);
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
        Utils.closeQuietly(metricsManager, "shareFetchMetricsManager");
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

        /**
         * Timeout in milliseconds indicating how long the request would be retried if it fails with a retriable exception.
         */
        private final long timeoutMs;

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
            this.timeoutMs = remainingMs();
        }

        UnsentRequest buildRequest() {
            // If the node is no longer in the cluster metadata, we can never send to it
            Node nodeToSend = metadata.fetch().nodeById(nodeId);
            if (nodeToSend == null) {
                failPendingAcknowledgementsCannotBeSent();
                return null;
            }

            // If this is the closing request, close the share session by setting the final epoch
            if (isCloseRequest()) {
                sessionHandler.notifyClose();
            }

            Map<TopicIdPartition, Acknowledgements> finalAcknowledgementsToSend = new HashMap<>(
                incompleteAcknowledgements.isEmpty() ? acknowledgementsToSend : incompleteAcknowledgements);

            for (Map.Entry<TopicIdPartition, Acknowledgements> entry : finalAcknowledgementsToSend.entrySet()) {
                sessionHandler.addPartitionToFetch(entry.getKey(), entry.getValue());
            }

            ShareAcknowledgeRequest.Builder requestBuilder = sessionHandler.newShareAcknowledgeBuilder(groupId);

            isProcessed = false;

            if (requestBuilder == null) {
                failPendingAcknowledgementsCannotBeSent();
                return null;
            }

            nodesWithPendingRequests.add(nodeId);

            log.trace("Building acknowledgements to send : {}", finalAcknowledgementsToSend);

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
         * Resets the timer with the configured timeout and resets the RequestState.
         * This is only applicable for commitAsync() requests as these states could be re-used.
         */
        void maybeResetTimerAndRequestState() {
            if (requestType == AcknowledgeRequestType.COMMIT_ASYNC) {
                resetTimeout(timeoutMs);
                reset();
            }
        }

        /**
         * Sets the error code in the acknowledgements and sends the response
         * through a background event.
         */
        void handleAcknowledgeErrorCode(TopicIdPartition tip, Errors acknowledgeErrorCode, boolean checkForRenewAcknowledgements, Optional<Integer> acquisitionLockTimeoutMs) {
            Acknowledgements acks = inFlightAcknowledgements.remove(tip);
            if (acks != null) {
                acks.complete(acknowledgeErrorCode.exception());
                resultHandler.complete(tip, acks, requestType, checkForRenewAcknowledgements, acquisitionLockTimeoutMs);
            } else {
                log.error("Invalid partition {} received in ShareAcknowledge response", tip);
            }
        }

        /**
         * Sets the error code for the acknowledgements which were timed out
         * after some retries.
         */
        void handleAcknowledgeTimedOut(TopicIdPartition tip) {
            Acknowledgements acks = incompleteAcknowledgements.get(tip);
            if (acks != null) {
                acks.complete(Errors.REQUEST_TIMED_OUT.exception());
                // We do not know whether this is a renew ack, but handling the error as if it were, will ensure
                // that we do not leave dangling acknowledgements
                resultHandler.complete(tip, acks, requestType, true, Optional.empty());
            }
        }

        /**
         * Fail all remaining acknowledgements when they cannot be sent. This can happen when network connectivity
         * is lost with the leader and the share session is lost, they are failed with {@link Errors#NETWORK_EXCEPTION}.
         * If the leader has moved to a different live broker, they are failed with {@link Errors#NOT_LEADER_OR_FOLLOWER}.
         */
        void failPendingAcknowledgementsCannotBeSent() {
            Map<TopicIdPartition, Acknowledgements> acknowledgementsMapToClear =
                incompleteAcknowledgements.isEmpty() ? acknowledgementsToSend : incompleteAcknowledgements;

            acknowledgementsMapToClear.forEach((tip, acks) -> {
                if (acks != null) {
                    acks.complete(acknowledgementsCannotBeSentError(nodeId, tip).exception());
                }
                // We do not know whether this is a renew ack, but handling the error as if it were, will ensure
                // that we do not leave dangling acknowledgements
                resultHandler.complete(tip, acks, requestType, true, Optional.empty());
            });
            acknowledgementsMapToClear.clear();
            processingComplete();
        }

        ShareSessionHandler sessionHandler() {
            return sessionHandler;
        }

        void processingComplete() {
            // If there are any pending inFlightAcknowledgements after processing the response, we fail them with an InvalidRecordStateException.
            processPendingInFlightAcknowledgements(new InvalidRecordStateException(INVALID_RESPONSE));
            resultHandler.completeIfEmpty();
            isProcessed = true;
            maybeResetTimerAndRequestState();
        }

        /**
         * Fail any existing in-flight acknowledgements with the given exception and clear the map.
         * We also send a background event to update {@link org.apache.kafka.clients.consumer.AcknowledgementCommitCallback }
         */
        private void processPendingInFlightAcknowledgements(KafkaException exception) {
            if (!inFlightAcknowledgements.isEmpty()) {
                inFlightAcknowledgements.forEach((partition, acknowledgements) -> {
                    acknowledgements.complete(exception);
                    // We do not know whether this is a renew ack, but handling the error as if it were, will ensure
                    // that we do not leave dangling acknowledgements
                    resultHandler.complete(partition, acknowledgements, requestType, true, Optional.empty());
                });
                inFlightAcknowledgements.clear();
            }
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
         *
         * @param tip The TopicIdPartition for which we move the acknowledgements.
         * @return True if the partition was sent in the request.
         * <p> False if the partition was not part of the request, we log an error and ignore such partitions. </p>
         */
        public boolean moveToIncompleteAcks(TopicIdPartition tip) {
            Acknowledgements acks = inFlightAcknowledgements.remove(tip);
            if (acks != null) {
                incompleteAcknowledgements.put(tip, acks);
                return true;
            } else {
                log.error("Invalid partition {} received in ShareAcknowledge response", tip);
                return false;
            }
        }

        public boolean isCloseRequest() {
            return requestType == AcknowledgeRequestType.CLOSE;
        }
    }

    /**
     * Sends a ShareAcknowledgementEvent event to the application when it is done
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
        public void complete(TopicIdPartition partition, Acknowledgements acknowledgements, AcknowledgeRequestType type, boolean checkForRenewAcknowledgements, Optional<Integer> acquisitionLockTimeoutMs) {
            if (type.equals(AcknowledgeRequestType.COMMIT_ASYNC)) {
                if (acknowledgements != null) {
                    maybeSendShareAcknowledgementEvent(Map.of(partition, acknowledgements), checkForRenewAcknowledgements, acquisitionLockTimeoutMs);
                }
            } else {
                if (acknowledgements != null) {
                    result.put(partition, acknowledgements);
                }
                if (remainingResults != null && remainingResults.decrementAndGet() == 0) {
                    maybeSendShareAcknowledgementEvent(result, checkForRenewAcknowledgements, acquisitionLockTimeoutMs);
                    future.ifPresent(future -> future.complete(result));
                }
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

    static class LeaderIdAndEpoch {
        private final int leaderId;
        private final int epoch;

        LeaderIdAndEpoch(int leaderId, int epoch) {
            this.leaderId = leaderId;
            this.epoch = epoch;
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
