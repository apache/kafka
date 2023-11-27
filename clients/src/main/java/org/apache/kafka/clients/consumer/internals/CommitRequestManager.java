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
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.events.AutoCommitCompletionBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;
import static org.apache.kafka.common.protocol.Errors.COORDINATOR_LOAD_IN_PROGRESS;
import static org.apache.kafka.common.protocol.Errors.COORDINATOR_NOT_AVAILABLE;
import static org.apache.kafka.common.protocol.Errors.NOT_COORDINATOR;
import static org.apache.kafka.common.protocol.Errors.REQUEST_TIMED_OUT;

public class CommitRequestManager implements RequestManager, MemberStateListener {

    private final SubscriptionState subscriptions;
    private final LogContext logContext;
    private final Logger log;
    private final Optional<AutoCommitState> autoCommitState;
    private final BackgroundEventHandler backgroundEventHandler;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final long retryBackoffMs;
    private final String groupId;
    private final Optional<String> groupInstanceId;
    private final long retryBackoffMaxMs;
    // For testing only
    private final OptionalDouble jitter;
    private final boolean throwOnFetchStableOffsetUnsupported;
    final PendingRequests pendingRequests;

    /**
     * True if the member is not currently subscribed to the consumer group (never subscribed, or
     * unsubscribed). This will indicate that it should not expect to receive new member ID or
     * epoch.
     */
    private boolean unsubscribed;

    /**
     * List of requests that have failed due to {@link Errors#UNKNOWN_MEMBER_ID}, that will be 
     * retried when a new member ID is received via {@link #onMemberIdUpdated(String, int)}.
     */
    private final List<RetriableRequestState> requestsWaitingForMemberId;

    /**
     * List of requests that have failed due to {@link Errors#STALE_MEMBER_EPOCH}, that will be 
     * retried when a new member epoch is received via {@link #onMemberEpochUpdated(int, String)}.
     */
    private final List<RetriableRequestState> requestsWaitingForMemberEpoch;

    public CommitRequestManager(
            final Time time,
            final LogContext logContext,
            final SubscriptionState subscriptions,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final BackgroundEventHandler backgroundEventHandler,
            final String groupId,
            final Optional<String> groupInstanceId) {
        this(time, logContext, subscriptions, config, coordinatorRequestManager,
                backgroundEventHandler, groupId,
                groupInstanceId, config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG), OptionalDouble.empty());
    }

    // Visible for testing
    CommitRequestManager(
        final Time time,
        final LogContext logContext,
        final SubscriptionState subscriptions,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final BackgroundEventHandler backgroundEventHandler,
        final String groupId,
        final Optional<String> groupInstanceId,
        final long retryBackoffMs,
        final long retryBackoffMaxMs,
        final OptionalDouble jitter) {
        Objects.requireNonNull(coordinatorRequestManager, "Coordinator is needed upon committing offsets");
        this.logContext = logContext;
        this.log = logContext.logger(getClass());
        this.pendingRequests = new PendingRequests();
        if (config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            final long autoCommitInterval =
                    Integer.toUnsignedLong(config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            this.autoCommitState = Optional.of(new AutoCommitState(time, autoCommitInterval));
        } else {
            this.autoCommitState = Optional.empty();
        }
        this.backgroundEventHandler = backgroundEventHandler;
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.groupId = groupId;
        this.groupInstanceId = groupInstanceId;
        this.subscriptions = subscriptions;
        this.retryBackoffMs = retryBackoffMs;
        this.retryBackoffMaxMs = retryBackoffMaxMs;
        this.jitter = jitter;
        this.throwOnFetchStableOffsetUnsupported = config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
        this.requestsWaitingForMemberId = new ArrayList<>();
        this.requestsWaitingForMemberEpoch = new ArrayList<>();
    }

    /**
     * Poll for the {@link OffsetFetchRequest} and {@link OffsetCommitRequest} request if there's any. The function will
     * also try to autocommit the offsets, if feature is enabled.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        // poll only when the coordinator node is known.
        if (!coordinatorRequestManager.coordinator().isPresent())
            return EMPTY;

        maybeAutoCommitAllConsumed();
        if (!pendingRequests.hasUnsentRequests())
            return EMPTY;

        List<NetworkClientDelegate.UnsentRequest> requests = pendingRequests.drain(currentTimeMs);
        // min of the remainingBackoffMs of all the request that are still backing off
        final long timeUntilNextPoll = Math.min(
                findMinTime(unsentOffsetCommitRequests(), currentTimeMs),
                findMinTime(unsentOffsetFetchRequests(), currentTimeMs));
        return new NetworkClientDelegate.PollResult(timeUntilNextPoll, requests);
    }

    private static long findMinTime(final Collection<? extends RequestState> requests, final long currentTimeMs) {
        return requests.stream()
                .mapToLong(request -> request.remainingBackoffMs(currentTimeMs))
                .min()
                .orElse(Long.MAX_VALUE);
    }

    /**
     * Generate a request to commit offsets if auto-commit is enabled. The request will be
     * returned to be sent out on the next call to {@link #poll(long)}. This will only generate a
     * request if there is no other commit request already in-flight, and if the commit interval
     * has elapsed.
     *
     * @param offsets Offsets to commit
     * @return Future that will complete when a response is received for the request, or a
     * completed future if no request is generated.
     */
    public CompletableFuture<Void> maybeAutoCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (!autoCommitState.isPresent()) {
            return CompletableFuture.completedFuture(null);
        }

        AutoCommitState autocommit = autoCommitState.get();
        if (!autocommit.canSendAutocommit()) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> result = sendAutoCommit(offsets);
        autocommit.resetTimer();
        autocommit.setInflightCommitStatus(true);
        return result;
    }

    /**
     * If auto-commit is enabled, this will generate a commit offsets request for all assigned
     * partitions and their current positions.
     *
     * @return Future that will complete when a response is received for the request, or a
     * completed future if no request is generated.
     */
    public CompletableFuture<Void> maybeAutoCommitAllConsumed() {
        return maybeAutoCommit(subscriptions.allConsumed());
    }

    /**
     * The consumer needs to send an auto commit during the shutdown if autocommit is enabled.
     */
    Optional<NetworkClientDelegate.UnsentRequest> maybeCreateAutoCommitRequest() {
        if (!autoCommitState.isPresent()) {
            return Optional.empty();
        }

        OffsetCommitRequestState request = pendingRequests.createOffsetCommitRequest(subscriptions.allConsumed(), jitter);
        request.future.whenComplete(autoCommitCallback(subscriptions.allConsumed()));
        return Optional.of(request.toUnsentRequest());
    }

    private CompletableFuture<Void> sendAutoCommit(final Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets) {
        log.debug("Enqueuing autocommit offsets: {}", allConsumedOffsets);
        return addOffsetCommitRequest(allConsumedOffsets).whenComplete(autoCommitCallback(allConsumedOffsets));
    }

    private BiConsumer<? super Void, ? super Throwable> autoCommitCallback(final Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets) {
        return (response, throwable) -> {
            autoCommitState.ifPresent(autoCommitState -> autoCommitState.setInflightCommitStatus(false));
            if (throwable == null) {
                // We need to notify the application thread to execute OffsetCommitCallback
                backgroundEventHandler.add(new AutoCommitCompletionBackgroundEvent());
                log.debug("Completed asynchronous auto-commit of offsets {}", allConsumedOffsets);
            } else if (throwable instanceof RetriableCommitFailedException) {
                log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}",
                        allConsumedOffsets, throwable.getMessage());
            } else {
                log.warn("Asynchronous auto-commit of offsets {} failed", allConsumedOffsets, throwable);
            }
        };
    }

    /**
     * Handles {@link org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent}. It creates an
     * {@link OffsetCommitRequestState} and enqueue it to send later.
     */
    public CompletableFuture<Void> addOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        return pendingRequests.addOffsetCommitRequest(offsets).future;
    }

    /**
     * Handles {@link org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent}. It creates an
     * {@link OffsetFetchRequestState} and enqueue it to send later.
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> addOffsetFetchRequest(final Set<TopicPartition> partitions) {
        return pendingRequests.addOffsetFetchRequest(partitions);
    }

    public void updateAutoCommitTimer(final long currentTimeMs) {
        this.autoCommitState.ifPresent(t -> t.ack(currentTimeMs));
    }

    // Visible for testing
    Queue<OffsetCommitRequestState> unsentOffsetCommitRequests() {
        return pendingRequests.unsentOffsetCommits;
    }

    private List<OffsetFetchRequestState> unsentOffsetFetchRequests() {
        return pendingRequests.unsentOffsetFetches;
    }

    private void handleCoordinatorDisconnect(Throwable exception, long currentTimeMs) {
        if (exception instanceof DisconnectException) {
            coordinatorRequestManager.markCoordinatorUnknown(exception.getMessage(), currentTimeMs);
        }
    }

    /**
     * Retry or cancel requests waiting for new member ID or epoch, according to the member new
     * state. If the new state indicates that the member has failed, all requests waiting for new
     * member ID or epoch will be cancelled. If the new state indicates that the member is leaving
     * the group, all requests waiting for new member ID or epoch will be retried without
     * including any member ID or epoch information.
     *
     * @param state New state for the member
     */
    @Override
    public void onStateChange(MemberState state) {
        unsubscribed = state == MemberState.UNSUBSCRIBED || state == MemberState.PREPARE_LEAVING;
        if (unsubscribed) {
            // The member is not part of the group. It could be the case that it was in the group
            // but left, so if there are requests waiting for a new member ID/epoch to retry,
            // retry them now without any member ID/epoch information in it.
            retryRequestsWaitingForMemberId(Optional.empty(), Optional.empty());
            retryRequestsWaitingForMemberEpoch(Optional.empty(), Optional.empty());
        } else if (state == MemberState.FATAL) {
            requestsWaitingForMemberId.forEach(fetchRequest ->
                    fetchRequest.abortRetry("The member is in an unrecoverable state."));
            requestsWaitingForMemberEpoch.forEach(fetchRequest ->
                    fetchRequest.abortRetry("The member is in an unrecoverable state."));
        }
    }

    /**
     * Retry requests that have previously fail due to {@link Errors#UNKNOWN_MEMBER_ID}.
     *
     * @param memberId New member ID received by the member. To be included in the new request.
     * @param epoch    Latest member epoch received. To be included in the new request.
     */
    @Override
    public void onMemberIdUpdated(String memberId, int epoch) {
        // Retry the request now that there is a new member ID (including the latest epoch too)
        log.debug("Retrying failed requests now using new member ID {} and epoch {}",
                memberId, epoch);
        retryRequestsWaitingForMemberId(Optional.of(memberId), Optional.of(epoch));
    }

    /**
     * Retry requests that have previously fail due to {@link Errors#STALE_MEMBER_EPOCH}.
     *
     * @param epoch New member epoch received. To be included in the new request.
     * @param memberId Current member ID. To be included in the new request.
     */
    @Override
    public void onMemberEpochUpdated(int epoch, String memberId) {
        // Retry the request now that there is a new member epoch (including the member ID too)
        log.debug("Retrying failed requests now using new member ID {} and epoch {}",
                memberId, epoch);
        retryRequestsWaitingForMemberEpoch(Optional.of(memberId), Optional.of(epoch));
    }

    private void retryRequestsWaitingForMemberId(Optional<String> memberId,
                                                 Optional<Integer> epoch) {
        requestsWaitingForMemberId.forEach(retriableRequest -> retriableRequest.retryOnMemberIdOrEpochUpdate(memberId, epoch));
    }

    private void retryRequestsWaitingForMemberEpoch(Optional<String> memberId,
                                                    Optional<Integer> epoch) {
        requestsWaitingForMemberEpoch.forEach(retriableRequest -> retriableRequest.retryOnMemberIdOrEpochUpdate(memberId, epoch));
    }

    private void retryWhenMemberIdUpdated(RetriableRequestState requestState) {
        pendingRequests.removeInflightRequest(requestState);
        requestsWaitingForMemberId.add(requestState);
    }

    private void retryWhenMemberEpochUpdated(RetriableRequestState requestState) {
        pendingRequests.removeInflightRequest(requestState);
        requestsWaitingForMemberEpoch.add(requestState);
    }

    /**
     * Handle a {@link Errors#STALE_MEMBER_EPOCH} error received in an OffsetFetch or
     * OffsetCommit request. This will keep the request to be retried when a new member epoch is
     * received. If the member is not part of the group anymore, this will retry the request
     * right away without including any member epoch.
     */
    private void handleStaleMemberEpochError(RetriableRequestState requestState) {
        if (unsubscribed) {
            log.debug("Request {} failed with {} for member ID {}. Member already left the group " +
                            "so retrying request without member epoch.",
                    requestState.requestName(), Errors.STALE_MEMBER_EPOCH,
                    requestState.memberId.orElse(
                            "null"));
            requestState.retryOnMemberIdOrEpochUpdate(Optional.empty(), Optional.empty());
        } else {
            log.debug("Request {} failed with {} for member ID {}. The request will be retried " +
                    "after receiving a new member epoch.", requestState,
                    Errors.STALE_MEMBER_EPOCH, requestState.memberId.orElse("null"));
            retryWhenMemberEpochUpdated(requestState);
        }
    }

    /**
     * Handle an {@link Errors#UNKNOWN_MEMBER_ID} error received in an OffsetFetch or
     * OffsetCommit request. This will keep the request to be retried when a new member ID is
     * received. If the member is not part of the group anymore, this will retry the request
     * right away without including any member ID.
     */
    private void handleUnknownMemberIdError(RetriableRequestState requestState) {
        if (unsubscribed) {
            log.debug("Request {} failed with {} but member already left the group. Retrying the " +
                            "request without member ID.",
                    requestState.requestName(), Errors.UNKNOWN_MEMBER_ID);
            requestState.retryOnMemberIdOrEpochUpdate(Optional.empty(), Optional.empty());
        } else {
            log.debug("Request {} failed with {}. The request will be retried after receiving a " +
                            "new member ID.",
                    requestState.requestName(), Errors.UNKNOWN_MEMBER_ID);
            retryWhenMemberIdUpdated(requestState);
        }
    }


    /**
     * @return True if auto-commit is enabled as defined in the config {@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG}
     */
    public boolean autoCommitEnabled() {
        return autoCommitState.isPresent();
    }

    /**
     * Reset the auto-commit timer so that the next auto-commit is sent out on the interval
     * starting from now. If auto-commit is not enabled this will perform no action.
     */
    public void resetAutoCommitTimer() {
        autoCommitState.ifPresent(AutoCommitState::resetTimer);
    }

    /**
     * Drains the inflight offsetCommits during shutdown because we want to make sure all pending commits are sent
     * before closing.
     */
    @Override
    public NetworkClientDelegate.PollResult pollOnClose() {
        if (!pendingRequests.hasUnsentRequests() || !coordinatorRequestManager.coordinator().isPresent())
            return EMPTY;

        List<NetworkClientDelegate.UnsentRequest> requests = pendingRequests.drainOnClose();
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, requests);
    }

    private class OffsetCommitRequestState extends RetriableRequestState {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final String groupId;
        private final Optional<String> groupInstanceId;

        private final CompletableFuture<Void> future;

        OffsetCommitRequestState(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                 final String groupId,
                                 final Optional<String> groupInstanceId,
                                 final long retryBackoffMs,
                                 final long retryBackoffMaxMs) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs, retryBackoffMaxMs);
            this.offsets = offsets;
            this.groupId = groupId;
            this.groupInstanceId = groupInstanceId;
            this.future = new CompletableFuture<>();
        }

        // Visible for testing
        OffsetCommitRequestState(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                 final String groupId,
                                 final Optional<String> groupInstanceId,
                                 final long retryBackoffMs,
                                 final long retryBackoffMaxMs,
                                 final double jitter) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs, 2, retryBackoffMaxMs, jitter);
            this.offsets = offsets;
            this.groupId = groupId;
            this.groupInstanceId = groupInstanceId;
            this.future = new CompletableFuture<>();
        }

        public NetworkClientDelegate.UnsentRequest toUnsentRequest() {
            Map<String, OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopicDataMap = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = entry.getValue();

                OffsetCommitRequestData.OffsetCommitRequestTopic topic = requestTopicDataMap
                    .getOrDefault(topicPartition.topic(),
                        new OffsetCommitRequestData.OffsetCommitRequestTopic()
                            .setName(topicPartition.topic())
                    );

                topic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                    .setPartitionIndex(topicPartition.partition())
                    .setCommittedOffset(offsetAndMetadata.offset())
                    .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setCommittedMetadata(offsetAndMetadata.metadata())
                );
                requestTopicDataMap.put(topicPartition.topic(), topic);
            }

            OffsetCommitRequestData data = new OffsetCommitRequestData()
                    .setGroupId(this.groupId)
                    .setGroupInstanceId(groupInstanceId.orElse(null))
                    .setTopics(new ArrayList<>(requestTopicDataMap.values()));
            if (memberId.isPresent()) {
                data = data.setMemberId(memberId.get());
            }
            if (memberEpoch.isPresent()) {
                data = data.setGenerationIdOrMemberEpoch(memberEpoch.get());
            }

            OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(data);

            NetworkClientDelegate.UnsentRequest resp = new NetworkClientDelegate.UnsentRequest(
                builder,
                coordinatorRequestManager.coordinator());
            resp.whenComplete(
                (response, throwable) -> {
                    try {
                        if (throwable == null) {
                            onResponse(response);
                        } else {
                            onError(throwable, resp.handler().completionTimeMs());
                        }
                    } catch (Throwable t) {
                        log.error("Unexpected error when completing offset commit: {}", this, t);
                        future.completeExceptionally(t);
                    }
                });
            return resp;
        }

        public void onError(final Throwable exception, final long currentTimeMs) {
            if (exception instanceof RetriableException) {
                handleCoordinatorDisconnect(exception, currentTimeMs);
                retry(currentTimeMs);
            }
        }

        public void onResponse(final ClientResponse response) {
            long responseTime = response.receivedTimeMs();
            OffsetCommitResponse commitResponse = (OffsetCommitResponse) response.responseBody();
            Set<String> unauthorizedTopics = new HashSet<>();
            for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : commitResponse.data().topics()) {
                for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                    TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                    OffsetAndMetadata offsetAndMetadata = offsets.get(tp);
                    long offset = offsetAndMetadata.offset();
                    Errors error = Errors.forCode(partition.errorCode());
                    if (error == Errors.NONE) {
                        log.debug("OffsetCommit {} for partition {}", offset, tp);
                        continue;
                    }

                    if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        // Collect all unauthorized topics before failing
                        unauthorizedTopics.add(tp.topic());
                    } else if (error.exception() instanceof RetriableException) {
                        log.warn("OffsetCommit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        handleRetriableError(error, response);
                        retry(responseTime);
                        return;
                    } else {
                        log.error("OffsetCommit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        handleFatalError(error);
                        return;
                    }
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("OffsetCommit failed due to not authorized to commit to topics {}", unauthorizedTopics);
                future.completeExceptionally(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }

        private void handleRetriableError(Errors error, ClientResponse response) {
            if (error == COORDINATOR_NOT_AVAILABLE ||
                error == NOT_COORDINATOR ||
                error == REQUEST_TIMED_OUT) {
                coordinatorRequestManager.markCoordinatorUnknown(error.message(), response.receivedTimeMs());
            }
        }

        private void retry(final long currentTimeMs) {
            onFailedAttempt(currentTimeMs);
            pendingRequests.addOffsetCommitRequest(this);
        }

        private void handleFatalError(final Errors error) {
            switch (error) {
                case GROUP_AUTHORIZATION_FAILED:
                    future.completeExceptionally(GroupAuthorizationException.forGroupId(groupId));
                    break;
                case OFFSET_METADATA_TOO_LARGE:
                case INVALID_COMMIT_OFFSET_SIZE:
                    future.completeExceptionally(error.exception());
                    break;
                case FENCED_INSTANCE_ID:
                    log.info("OffsetCommit failed due to group instance id {} fenced: {}", groupInstanceId, error.message());
                    future.completeExceptionally(new CommitFailedException());
                    break;
                case UNKNOWN_MEMBER_ID:
                    log.info("OffsetCommit failed due to unknown member id: {}", error.message());
                    handleUnknownMemberIdError(this);
                    break;
                case STALE_MEMBER_EPOCH:
                    log.info("OffsetCommit failed due to stale member epoch: {}", error.message());
                    handleStaleMemberEpochError(this);
                    break;
                default:
                    future.completeExceptionally(new KafkaException("Unexpected error in commit:" +
                            " " + error.message()));
                    break;
            }
        }

        @Override
        void abortRetry(String cause) {
            future.completeExceptionally(new KafkaException("Offset commit waiting for new member" +
                    " ID or epoch cannot be retried. " + cause));
        }

        /**
         * Reset timers and add request to the list of pending requests, to make sure it is sent
         * out on the next poll iteration, without applying any backoff.
         */
        @Override
        public void retryOnMemberIdOrEpochUpdate(Optional<String> memberId,
                                                 Optional<Integer> memberEpoch) {
            this.memberId = memberId;
            this.memberEpoch = memberEpoch;
            reset();
            pendingRequests.addOffsetCommitRequest(this);
        }

        @Override
        public String requestName() {
            return ApiKeys.OFFSET_COMMIT.name();
        }
    }

    /**
     * Represents a request that can be retried or aborted, based on member ID and epoch
     * information.
     */
    abstract static class RetriableRequestState extends RequestState {

        /**
         * Member ID to be included in the request if present.
         */
        Optional<String> memberId;

        /**
         * Member epoch to be included in the request if present.
         */
        Optional<Integer> memberEpoch;

        RetriableRequestState(LogContext logContext, String owner, long retryBackoffMs, long retryBackoffMaxMs) {
            super(logContext, owner, retryBackoffMs, retryBackoffMaxMs);
            this.memberId = Optional.empty();
            this.memberEpoch = Optional.empty();
        }

        // Visible for testing
        RetriableRequestState(LogContext logContext, String owner, long retryBackoffMs, int retryBackoffExpBase,
                              long retryBackoffMaxMs, double jitter) {
            super(logContext, owner, retryBackoffMs, retryBackoffExpBase, retryBackoffMaxMs, jitter);
            this.memberId = Optional.empty();
            this.memberEpoch = Optional.empty();
        }

        abstract void abortRetry(String cause);

        abstract void retryOnMemberIdOrEpochUpdate(Optional<String> memberId, Optional<Integer> memberEpoch);

        abstract String requestName();
    }

    class OffsetFetchRequestState extends RetriableRequestState {

        /**
         * Partitions to get committed offsets for.
         */
        public final Set<TopicPartition> requestedPartitions;

        private final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future;

        public OffsetFetchRequestState(final Set<TopicPartition> partitions,
                                       final long retryBackoffMs,
                                       final long retryBackoffMaxMs) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs, retryBackoffMaxMs);
            this.requestedPartitions = partitions;
            this.future = new CompletableFuture<>();
        }

        public OffsetFetchRequestState(final Set<TopicPartition> partitions,
                                       final long retryBackoffMs,
                                       final long retryBackoffMaxMs,
                                       final double jitter) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs, 2, retryBackoffMaxMs, jitter);
            this.requestedPartitions = partitions;
            this.future = new CompletableFuture<>();
        }

        public boolean sameRequest(final OffsetFetchRequestState request) {
            return requestedPartitions.equals(request.requestedPartitions);
        }

        public NetworkClientDelegate.UnsentRequest toUnsentRequest() {
            OffsetFetchRequest.Builder builder;
            if (memberId.isPresent() && memberEpoch.isPresent()) {
                builder = new OffsetFetchRequest.Builder(
                        groupId,
                        memberId.get(),
                        memberEpoch.get(),
                        true,
                        new ArrayList<>(this.requestedPartitions),
                        throwOnFetchStableOffsetUnsupported);
            } else {
                // Building request without passing member ID/epoch to leave the logic to choose
                // default values when not present on the request builder.
                builder = new OffsetFetchRequest.Builder(
                        groupId,
                        true,
                        new ArrayList<>(this.requestedPartitions),
                        throwOnFetchStableOffsetUnsupported);
            }
            return new NetworkClientDelegate.UnsentRequest(builder, coordinatorRequestManager.coordinator())
                    .whenComplete((r, t) -> onResponse(r.receivedTimeMs(), (OffsetFetchResponse) r.responseBody()));
        }

        /**
         * Handle request responses, including successful and failed.
         */
        public void onResponse(
                final long currentTimeMs,
                final OffsetFetchResponse response) {
            Errors responseError = response.groupLevelError(groupId);
            if (responseError != Errors.NONE) {
                onFailure(currentTimeMs, responseError);
                return;
            }
            onSuccess(currentTimeMs, response);
        }

        /**
         * Handle failed responses. This will retry if the error is retriable, or complete the
         * result future exceptionally in the case of non-recoverable or unexpected errors.
         */
        private void onFailure(final long currentTimeMs,
                               final Errors responseError) {
            handleCoordinatorDisconnect(responseError.exception(), currentTimeMs);
            log.debug("Offset fetch failed: {}", responseError.message());
            if (responseError == COORDINATOR_LOAD_IN_PROGRESS) {
                retry(currentTimeMs);
            } else if (responseError == Errors.NOT_COORDINATOR) {
                // re-discover the coordinator and retry
                coordinatorRequestManager.markCoordinatorUnknown("error response " + responseError.name(), currentTimeMs);
                retry(currentTimeMs);
            } else if (responseError == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.completeExceptionally(GroupAuthorizationException.forGroupId(groupId));
            } else if (responseError == Errors.STALE_MEMBER_EPOCH) {
                handleStaleMemberEpochError(this);
            } else if (responseError == Errors.UNKNOWN_MEMBER_ID) {
                handleUnknownMemberIdError(this);
            } else {
                future.completeExceptionally(new KafkaException("Unexpected error in fetch offset response: " + responseError.message()));
            }
        }

        private void retry(final long currentTimeMs) {
            onFailedAttempt(currentTimeMs);
            pendingRequests.addOffsetFetchRequest(this);
        }

        private void onSuccess(final long currentTimeMs,
                               final OffsetFetchResponse response) {
            Set<String> unauthorizedTopics = null;
            Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData =
                    response.partitionDataMap(groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(responseData.size());
            Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : responseData.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                if (partitionData.hasError()) {
                    Errors error = partitionData.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.completeExceptionally(new KafkaException("Topic or Partition " + tp + " does " +
                                "not " +
                                "exist"));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        unauthorizedTopics.add(tp.topic());
                    } else if (error == Errors.UNSTABLE_OFFSET_COMMIT) {
                        unstableTxnOffsetTopicPartitions.add(tp);
                    } else {
                        future.completeExceptionally(new KafkaException("Unexpected error in fetch offset " +
                                "response for partition " + tp + ": " + error.message()));
                        return;
                    }
                } else if (partitionData.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch);
                    // if there's no committed offset, record as null
                    offsets.put(tp, new OffsetAndMetadata(partitionData.offset, partitionData.leaderEpoch, partitionData.metadata));
                } else {
                    log.info("Found no committed offset for partition {}", tp);
                    offsets.put(tp, null);
                }
            }

            if (unauthorizedTopics != null) {
                future.completeExceptionally(new TopicAuthorizationException(unauthorizedTopics));
            } else if (!unstableTxnOffsetTopicPartitions.isEmpty()) {
                // TODO: Optimization question: Do we need to retry all partitions upon a single partition error?
                log.info("The following partitions still have unstable offsets " +
                        "which are not cleared on the broker side: {}" +
                        ", this could be either " +
                        "transactional offsets waiting for completion, or " +
                        "normal offsets waiting for replication after appending to local log", unstableTxnOffsetTopicPartitions);
                retry(currentTimeMs);
            } else {
                future.complete(offsets);
            }
        }

        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> chainFuture(
                final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> otherFuture) {
            return this.future.whenComplete((r, t) -> {
                if (t != null) {
                    otherFuture.completeExceptionally(t);
                } else {
                    otherFuture.complete(r);
                }
            });
        }

        @Override
        public String toString() {
            return "OffsetFetchRequestState{" +
                    "requestedPartitions=" + requestedPartitions +
                    ", memberId=" + memberId.orElse("undefined") +
                    ", memberEpoch=" + (memberEpoch.isPresent() ? memberEpoch.get() : "undefined") +
                    ", future=" + future +
                    ", " + toStringBase() +
                    '}';
        }

        @Override
        void abortRetry(String cause) {
            future.completeExceptionally(new KafkaException("Offset fetch waiting for new member " +
                    "ID or epoch cannot be retried. " + cause));
        }

        /**
         * Reset timers and add request to the list of pending requests, to make sure it is sent
         * out on the next poll iteration, without applying any backoff.
         */
        @Override
        public void retryOnMemberIdOrEpochUpdate(Optional<String> memberId,
                                                 Optional<Integer> memberEpoch) {
            this.memberId = memberId;
            this.memberEpoch = memberEpoch;
            reset();
            pendingRequests.addOffsetFetchRequest(this);
        }

        @Override
        public String requestName() {
            return ApiKeys.OFFSET_FETCH.name();
        }
    }

    /**
     * <p>This is used to stage the unsent {@link OffsetCommitRequestState} and {@link OffsetFetchRequestState}.
     * <li>unsentOffsetCommits holds the offset commit requests that have not been sent out</>
     * <li>unsentOffsetFetches holds the offset fetch requests that have not been sent out</li>
     * <li>inflightOffsetFetches holds the offset fetch requests that have been sent out but not completed</>.
     * <p>
     * {@code addOffsetFetchRequest} dedupes the requests to avoid sending the same requests.
     */

    class PendingRequests {
        // Queue is used to ensure the sequence of commit
        Queue<OffsetCommitRequestState> unsentOffsetCommits = new LinkedList<>();
        List<OffsetFetchRequestState> unsentOffsetFetches = new ArrayList<>();
        List<OffsetFetchRequestState> inflightOffsetFetches = new ArrayList<>();

        // Visible for testing
        boolean hasUnsentRequests() {
            return !unsentOffsetCommits.isEmpty() || !unsentOffsetFetches.isEmpty();
        }

        OffsetCommitRequestState addOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
            // TODO: Dedupe committing the same offsets to the same partitions
            return addOffsetCommitRequest(createOffsetCommitRequest(offsets, jitter));
        }

        OffsetCommitRequestState addOffsetCommitRequest(OffsetCommitRequestState request) {
            unsentOffsetCommits.add(request);
            return request;
        }

        OffsetCommitRequestState createOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                           final OptionalDouble jitter) {
            return jitter.isPresent() ?
                    new OffsetCommitRequestState(
                        offsets,
                            groupId,
                            groupInstanceId,
                        retryBackoffMs,
                        retryBackoffMaxMs,
                        jitter.getAsDouble()) :
                    new OffsetCommitRequestState(
                        offsets,
                            groupId,
                            groupInstanceId,
                        retryBackoffMs,
                        retryBackoffMaxMs);
        }

        /**
         * <p>Adding an offset fetch request to the outgoing buffer.  If the same request was made, we chain the future
         * to the existing one.
         *
         * <p>If the request is new, it invokes a callback to remove itself from the {@code inflightOffsetFetches}
         * upon completion.
         */
        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> addOffsetFetchRequest(final OffsetFetchRequestState request) {
            Optional<OffsetFetchRequestState> dupe =
                    unsentOffsetFetches.stream().filter(r -> r.sameRequest(request)).findAny();
            Optional<OffsetFetchRequestState> inflight =
                    inflightOffsetFetches.stream().filter(r -> r.sameRequest(request)).findAny();

            if (dupe.isPresent() || inflight.isPresent()) {
                log.info("Duplicated OffsetFetchRequest: " + request.requestedPartitions);
                dupe.orElseGet(() -> inflight.get()).chainFuture(request.future);
            } else {
                // remove the request from the outbound buffer: inflightOffsetFetches
                request.future.whenComplete((r, t) -> {
                    if (!inflightOffsetFetches.remove(request)) {
                        log.warn("A duplicated, inflight, request was identified, but unable to find it in the " +
                                "outbound buffer:" + request);
                    }
                });
                this.unsentOffsetFetches.add(request);
            }
            return request.future;
        }

        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> addOffsetFetchRequest(final Set<TopicPartition> partitions) {
            OffsetFetchRequestState request = jitter.isPresent() ?
                    new OffsetFetchRequestState(
                            partitions,
                            retryBackoffMs,
                            retryBackoffMaxMs,
                            jitter.getAsDouble()) :
                    new OffsetFetchRequestState(
                            partitions,
                            retryBackoffMs,
                            retryBackoffMaxMs);
            return addOffsetFetchRequest(request);
        }

        /**
         * Remove inflight request if this is an OffsetFetch request. For OffsetCommit requests no
         * in-flights are kept so no action required.
         */
        private void removeInflightRequest(RetriableRequestState request) {
            if (request instanceof OffsetFetchRequestState) {
                boolean deleted = inflightOffsetFetches.remove(request);
                log.debug("Attempt to delete request {} from outbound buffer. Result: {}", request,
                        deleted ? "deleted" : "not found");
            }
        }

        /**
         * Clear {@code unsentOffsetCommits} and moves all the sendable request in {@code
         * unsentOffsetFetches} to the {@code inflightOffsetFetches} to bookkeep all the inflight
         * requests. Note: Sendable requests are determined by their timer as we are expecting
         * backoff on failed attempt. See {@link RequestState}.
         */
        List<NetworkClientDelegate.UnsentRequest> drain(final long currentTimeMs) {
            List<NetworkClientDelegate.UnsentRequest> unsentRequests = new ArrayList<>();

            // not ready to sent request
            List<OffsetCommitRequestState> unreadyCommitRequests = unsentOffsetCommits.stream()
                .filter(request -> !request.canSendRequest(currentTimeMs))
                .collect(Collectors.toList());

            // Add all unsent offset commit requests to the unsentRequests list
            unsentRequests.addAll(
                    unsentOffsetCommits.stream()
                        .filter(request -> request.canSendRequest(currentTimeMs))
                        .peek(request -> request.onSendAttempt(currentTimeMs))
                        .map(OffsetCommitRequestState::toUnsentRequest)
                        .collect(Collectors.toList()));

            // Partition the unsent offset fetch requests into sendable and non-sendable lists
            Map<Boolean, List<OffsetFetchRequestState>> partitionedBySendability =
                    unsentOffsetFetches.stream()
                            .collect(Collectors.partitioningBy(request -> request.canSendRequest(currentTimeMs)));

            // Add all sendable offset fetch requests to the unsentRequests list and to the inflightOffsetFetches list
            for (OffsetFetchRequestState request : partitionedBySendability.get(true)) {
                request.onSendAttempt(currentTimeMs);
                unsentRequests.add(request.toUnsentRequest());
                inflightOffsetFetches.add(request);
            }

            // Clear the unsent offset commit and fetch lists and add all non-sendable offset fetch requests to the unsentOffsetFetches list
            clearAll();
            unsentOffsetFetches.addAll(partitionedBySendability.get(false));
            unsentOffsetCommits.addAll(unreadyCommitRequests);

            return Collections.unmodifiableList(unsentRequests);
        }

        private void clearAll() {
            unsentOffsetCommits.clear();
            unsentOffsetFetches.clear();
        }

        private List<NetworkClientDelegate.UnsentRequest> drainOnClose() {
            ArrayList<NetworkClientDelegate.UnsentRequest> res = new ArrayList<>();
            res.addAll(unsentOffsetCommits.stream().map(OffsetCommitRequestState::toUnsentRequest).collect(Collectors.toList()));
            clearAll();
            return res;
        }
    }

    /**
     * Encapsulates the state of auto-committing and manages the auto-commit timer.
     */
    private static class AutoCommitState {
        private final Timer timer;
        private final long autoCommitInterval;
        private boolean hasInflightCommit;

        public AutoCommitState(
                final Time time,
                final long autoCommitInterval) {
            this.autoCommitInterval = autoCommitInterval;
            this.timer = time.timer(autoCommitInterval);
            this.hasInflightCommit = false;
        }

        public boolean canSendAutocommit() {
            return !this.hasInflightCommit && this.timer.isExpired();
        }

        public void resetTimer() {
            this.timer.reset(autoCommitInterval);
        }

        public void ack(final long currentTimeMs) {
            this.timer.update(currentTimeMs);
        }

        public void setInflightCommitStatus(final boolean inflightCommitStatus) {
            this.hasInflightCommit = inflightCommitStatus;
        }
    }
}
