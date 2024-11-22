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
import org.apache.kafka.clients.consumer.internals.metrics.OffsetCommitMetricsManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnstableOffsetCommitException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
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

public class CommitRequestManager implements RequestManager, MemberStateListener {
    private final Time time;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final LogContext logContext;
    private final Logger log;
    private final Optional<AutoCommitState> autoCommitState;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker;
    private final OffsetCommitMetricsManager metricsManager;
    private final long retryBackoffMs;
    private final String groupId;
    private final Optional<String> groupInstanceId;
    private final long retryBackoffMaxMs;
    // For testing only
    private final OptionalDouble jitter;
    private final boolean throwOnFetchStableOffsetUnsupported;
    final PendingRequests pendingRequests;
    private boolean closing = false;

    /**
     * Last member epoch sent in a commit request. Empty if no epoch was included in the last
     * request. Used for logging.
     */
    private Optional<Integer> lastEpochSentOnCommit;

    /**
     *  The member ID and latest member epoch received via the {@link MemberStateListener#onMemberEpochUpdated(Optional, String)},
     *  to be included in the OffsetFetch and OffsetCommit requests. This will have
     *  the latest memberEpoch received from the broker.
     */
    private final MemberInfo memberInfo;

    public CommitRequestManager(
        final Time time,
        final LogContext logContext,
        final SubscriptionState subscriptions,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker,
        final String groupId,
        final Optional<String> groupInstanceId,
        final Metrics metrics,
        final ConsumerMetadata metadata) {
        this(time,
            logContext,
            subscriptions,
            config,
            coordinatorRequestManager,
            offsetCommitCallbackInvoker,
            groupId,
            groupInstanceId,
            config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG),
            config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG),
            OptionalDouble.empty(),
            metrics,
            metadata);
    }

    // Visible for testing
    CommitRequestManager(
        final Time time,
        final LogContext logContext,
        final SubscriptionState subscriptions,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker,
        final String groupId,
        final Optional<String> groupInstanceId,
        final long retryBackoffMs,
        final long retryBackoffMaxMs,
        final OptionalDouble jitter,
        final Metrics metrics,
        final ConsumerMetadata metadata) {
        Objects.requireNonNull(coordinatorRequestManager, "Coordinator is needed upon committing offsets");
        this.time = time;
        this.logContext = logContext;
        this.log = logContext.logger(getClass());
        this.pendingRequests = new PendingRequests();
        if (config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            final long autoCommitInterval =
                Integer.toUnsignedLong(config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            this.autoCommitState = Optional.of(new AutoCommitState(time, autoCommitInterval, logContext));
        } else {
            this.autoCommitState = Optional.empty();
        }
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.groupId = groupId;
        this.groupInstanceId = groupInstanceId;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.retryBackoffMaxMs = retryBackoffMaxMs;
        this.jitter = jitter;
        this.throwOnFetchStableOffsetUnsupported = config.getBoolean(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
        this.memberInfo = new MemberInfo();
        this.metricsManager = new OffsetCommitMetricsManager(metrics);
        this.offsetCommitCallbackInvoker = offsetCommitCallbackInvoker;
        this.lastEpochSentOnCommit = Optional.empty();
    }

    /**
     * Poll for the {@link OffsetFetchRequest} and {@link OffsetCommitRequest} request if there's any. The function will
     * also try to autocommit the offsets, if feature is enabled.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        // poll only when the coordinator node is known.
        if (coordinatorRequestManager.coordinator().isEmpty())
            return EMPTY;

        if (closing) {
            return drainPendingOffsetCommitRequests();
        }

        maybeAutoCommitAsync();
        if (!pendingRequests.hasUnsentRequests())
            return EMPTY;

        List<NetworkClientDelegate.UnsentRequest> requests = pendingRequests.drain(currentTimeMs);
        // min of the remainingBackoffMs of all the request that are still backing off
        final long timeUntilNextPoll = Math.min(
            findMinTime(unsentOffsetCommitRequests(), currentTimeMs),
            findMinTime(unsentOffsetFetchRequests(), currentTimeMs));
        return new NetworkClientDelegate.PollResult(timeUntilNextPoll, requests);
    }

    @Override
    public void signalClose() {
        closing = true;
    }

    /**
     * Returns the delay for which the application thread can safely wait before it should be responsive
     * to results from the request managers. For example, the subscription state can change when heartbeats
     * are sent, so blocking for longer than the heartbeat interval might mean the application thread is not
     * responsive to changes.
     */
    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        return autoCommitState.map(ac -> ac.remainingMs(currentTimeMs)).orElse(Long.MAX_VALUE);
    }

    private static long findMinTime(final Collection<? extends RequestState> requests, final long currentTimeMs) {
        return requests.stream()
            .mapToLong(request -> request.remainingBackoffMs(currentTimeMs))
            .min()
            .orElse(Long.MAX_VALUE);
    }

    private KafkaException maybeWrapAsTimeoutException(Throwable t) {
        if (t instanceof TimeoutException)
            return (TimeoutException) t;
        else
            return new TimeoutException(t);
    }

    /**
     * Generate a request to commit consumed offsets. Add the request to the queue of pending
     * requests to be sent out on the next call to {@link #poll(long)}. If there are empty
     * offsets to commit, no request will be generated and a completed future will be returned.
     *
     * @param requestState Commit request
     * @return Future containing the offsets that were committed, or an error if the request
     * failed.
     */
    private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> requestAutoCommit(final OffsetCommitRequestState requestState) {
        AutoCommitState autocommit = autoCommitState.get();
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result;
        if (requestState.offsets.isEmpty()) {
            result = CompletableFuture.completedFuture(Collections.emptyMap());
        } else {
            autocommit.setInflightCommitStatus(true);
            OffsetCommitRequestState request = pendingRequests.addOffsetCommitRequest(requestState);
            result = request.future;
            result.whenComplete(autoCommitCallback(request.offsets));
        }
        return result;
    }

    /**
     * If auto-commit is enabled, and the auto-commit interval has expired, this will generate and
     * enqueue a request to commit all consumed offsets, and will reset the auto-commit timer to the
     * interval. The request will be sent on the next call to {@link #poll(long)}.
     * <p/>
     * If the request completes with a retriable error, this will reset the auto-commit timer with
     * the exponential backoff. If it fails with a non-retriable error, no action is taken, so
     * the next commit will be generated when the interval expires.
     * <p/>
     * This will not generate a new commit request if a previous one hasn't received a response.
     * In that case, the next auto-commit request will be sent on the next call to poll, after a
     * response for the in-flight is received.
     */
    public void maybeAutoCommitAsync() {
        if (autoCommitEnabled() && autoCommitState.get().shouldAutoCommit()) {
            OffsetCommitRequestState requestState = createOffsetCommitRequest(
                subscriptions.allConsumed(),
                Long.MAX_VALUE);
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result = requestAutoCommit(requestState);
            // Reset timer to the interval (even if no request was generated), but ensure that if
            // the request completes with a retriable error, the timer is reset to send the next
            // auto-commit after the backoff expires.
            resetAutoCommitTimer();
            maybeResetTimerWithBackoff(result);
        }
    }

    /**
     * Reset auto-commit timer to retry with backoff if the future failed with a RetriableCommitFailedException.
     */
    private void maybeResetTimerWithBackoff(final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result) {
        result.whenComplete((offsets, error) -> {
            if (error != null) {
                if (error instanceof RetriableCommitFailedException) {
                    log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error.", offsets, error);
                    resetAutoCommitTimer(retryBackoffMs);
                } else {
                    log.debug("Asynchronous auto-commit of offsets {} failed: {}", offsets, error.getMessage());
                }
            } else {
                log.debug("Completed asynchronous auto-commit of offsets {}", offsets);
            }
        });
    }

    /**
     * Commit consumed offsets if auto-commit is enabled, regardless of the auto-commit interval.
     * This is used for committing offsets before revoking partitions. This will retry committing
     * the latest offsets until the request succeeds, fails with a fatal error, or the timeout
     * expires. Note that:
     * <ul>
     *     <li>Considers {@link Errors#STALE_MEMBER_EPOCH} as a retriable error, and will retry it
     *     including the member ID and latest member epoch received from the broker.</li>
     *     <li>Considers {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} as a fatal error, and will not
     *     retry it although the error extends RetriableException. The reason is that if a topic
     *     or partition is deleted, revocation would not finish in time since the auto commit would keep retrying.</li>
     * </ul>
     *
     * Also note that this will generate a commit request even if there is another one in-flight,
     * generated by the auto-commit on the interval logic, to ensure that the latest offsets are
     * committed before revoking partitions.
     *
     * @return Future that will complete when the offsets are successfully committed. It will
     * complete exceptionally if the commit fails with a non-retriable error, or if the retry
     * timeout expires.
     */
    public CompletableFuture<Void> maybeAutoCommitSyncBeforeRevocation(final long deadlineMs) {
        if (!autoCommitEnabled()) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> result = new CompletableFuture<>();
        OffsetCommitRequestState requestState =
            createOffsetCommitRequest(subscriptions.allConsumed(), deadlineMs);
        autoCommitSyncBeforeRevocationWithRetries(requestState, result);
        return result;
    }

    private void autoCommitSyncBeforeRevocationWithRetries(OffsetCommitRequestState requestAttempt,
                                                           CompletableFuture<Void> result) {
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitAttempt = requestAutoCommit(requestAttempt);
        commitAttempt.whenComplete((committedOffsets, error) -> {
            if (error == null) {
                result.complete(null);
            } else {
                if (error instanceof RetriableException || isStaleEpochErrorAndValidEpochAvailable(error)) {
                    if (requestAttempt.isExpired()) {
                        log.debug("Auto-commit sync before revocation timed out and won't be retried anymore");
                        result.completeExceptionally(maybeWrapAsTimeoutException(error));
                    } else if (error instanceof UnknownTopicOrPartitionException) {
                        log.debug("Auto-commit sync before revocation failed because topic or partition were deleted");
                        result.completeExceptionally(error);
                    } else {
                        // Make sure the auto-commit is retried with the latest offsets
                        log.debug("Member {} will retry auto-commit of latest offsets after receiving retriable error {}",
                            memberInfo.memberId,
                            error.getMessage());
                        requestAttempt.offsets = subscriptions.allConsumed();
                        requestAttempt.resetFuture();
                        autoCommitSyncBeforeRevocationWithRetries(requestAttempt, result);
                    }
                } else {
                    log.debug("Auto-commit sync before revocation failed with non-retriable error", error);
                    result.completeExceptionally(error);
                }
            }
        });
    }

    /**
     * Clear the inflight auto-commit flag and log auto-commit completion status.
     */
    private BiConsumer<? super Map<TopicPartition, OffsetAndMetadata>, ? super Throwable> autoCommitCallback(final Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets) {
        return (response, throwable) -> {
            autoCommitState.ifPresent(autoCommitState -> autoCommitState.setInflightCommitStatus(false));
            if (throwable == null) {
                offsetCommitCallbackInvoker.enqueueInterceptorInvocation(allConsumedOffsets);
                log.debug("Completed auto-commit of offsets {}", allConsumedOffsets);
            } else if (throwable instanceof RetriableCommitFailedException) {
                log.debug("Auto-commit of offsets {} failed due to retriable error: {}",
                        allConsumedOffsets, throwable.getMessage());
            } else {
                log.warn("Auto-commit of offsets {} failed", allConsumedOffsets, throwable);
            }
        };
    }

    /**
     * Generate a request to commit offsets without retrying, even if it fails with a retriable
     * error. The generated request will be added to the queue to be sent on the next call to
     * {@link #poll(long)}.
     *
     * @param offsets Offsets to commit per partition.
     * @return Future that will complete when a response is received, successfully or
     * exceptionally depending on the response. If the request fails with a retriable error, the
     * future will be completed with a {@link RetriableCommitFailedException}.
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitAsync(final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets) {
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = offsets.orElseGet(subscriptions::allConsumed);
        if (commitOffsets.isEmpty()) {
            log.debug("Skipping commit of empty offsets");
            return CompletableFuture.completedFuture(Map.of());
        }
        maybeUpdateLastSeenEpochIfNewer(commitOffsets);
        OffsetCommitRequestState commitRequest = createOffsetCommitRequest(commitOffsets, Long.MAX_VALUE);
        pendingRequests.addOffsetCommitRequest(commitRequest);

        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> asyncCommitResult = new CompletableFuture<>();
        commitRequest.future.whenComplete((committedOffsets, error) -> {
            if (error != null) {
                asyncCommitResult.completeExceptionally(commitAsyncExceptionForError(error));
            } else {
                asyncCommitResult.complete(commitOffsets);
            }
        });
        return asyncCommitResult;
    }

    /**
     * Commit offsets, retrying on expected retriable errors while the retry timeout hasn't expired.
     *
     * @param offsets    Offsets to commit
     * @param deadlineMs Time until which the request will be retried if it fails with
     *                   an expected retriable error.
     * @return Future that will complete when a successful response
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> commitSync(final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets,
                                                                                final long deadlineMs) {
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = offsets.orElseGet(subscriptions::allConsumed);
        if (commitOffsets.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }
        maybeUpdateLastSeenEpochIfNewer(commitOffsets);
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result = new CompletableFuture<>();
        OffsetCommitRequestState requestState = createOffsetCommitRequest(commitOffsets, deadlineMs);
        commitSyncWithRetries(requestState, result);
        return result;
    }

    private OffsetCommitRequestState createOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                               final long deadlineMs) {
        return jitter.isPresent() ?
            new OffsetCommitRequestState(
                offsets,
                groupId,
                groupInstanceId,
                deadlineMs,
                retryBackoffMs,
                retryBackoffMaxMs,
                jitter.getAsDouble(),
                memberInfo) :
            new OffsetCommitRequestState(
                offsets,
                groupId,
                groupInstanceId,
                deadlineMs,
                retryBackoffMs,
                retryBackoffMaxMs,
                memberInfo);
    }

    private void commitSyncWithRetries(OffsetCommitRequestState requestAttempt,
                                       CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result) {
        pendingRequests.addOffsetCommitRequest(requestAttempt);

        // Retry the same commit request while it fails with RetriableException and the retry
        // timeout hasn't expired.
        requestAttempt.future.whenComplete((res, error) -> {
            if (error == null) {
                result.complete(requestAttempt.offsets);
            } else {
                if (error instanceof RetriableException) {
                    if (requestAttempt.isExpired()) {
                        log.info("OffsetCommit timeout expired so it won't be retried anymore");
                        result.completeExceptionally(maybeWrapAsTimeoutException(error));
                    } else {
                        requestAttempt.resetFuture();
                        commitSyncWithRetries(requestAttempt, result);
                    }
                } else {
                    result.completeExceptionally(commitSyncExceptionForError(error));
                }
            }
        });
    }

    private Throwable commitSyncExceptionForError(Throwable error) {
        if (error instanceof StaleMemberEpochException) {
            return new CommitFailedException("OffsetCommit failed with stale member epoch."
                + Errors.STALE_MEMBER_EPOCH.message());
        }
        return error;
    }

    private Throwable commitAsyncExceptionForError(Throwable error) {
        if (error instanceof RetriableException) {
            return new RetriableCommitFailedException(error);
        }
        return error;
    }

    /**
     * Enqueue a request to fetch committed offsets, that will be sent on the next call to {@link #poll(long)}.
     *
     * @param partitions       Partitions to fetch offsets for.
     * @param deadlineMs       Time until which the request should be retried if it fails
     *                         with expected retriable errors.
     * @return Future that will complete when a successful response is received, or the request
     * fails and cannot be retried. Note that the request is retried whenever it fails with
     * retriable expected error and the retry time hasn't expired.
     */
    public CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> fetchOffsets(
        final Set<TopicPartition> partitions,
        final long deadlineMs) {
        if (partitions.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result = new CompletableFuture<>();
        OffsetFetchRequestState request = createOffsetFetchRequest(partitions, deadlineMs);
        fetchOffsetsWithRetries(request, result);
        return result;
    }

    // Visible for testing
    OffsetFetchRequestState createOffsetFetchRequest(final Set<TopicPartition> partitions,
                                                             final long deadlineMs) {
        return jitter.isPresent() ?
            new OffsetFetchRequestState(
                partitions,
                retryBackoffMs,
                retryBackoffMaxMs,
                deadlineMs,
                jitter.getAsDouble(),
                memberInfo) :
            new OffsetFetchRequestState(
                partitions,
                retryBackoffMs,
                retryBackoffMaxMs,
                deadlineMs,
                memberInfo);
    }

    private void fetchOffsetsWithRetries(final OffsetFetchRequestState fetchRequest,
                                         final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> result) {
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> currentResult = pendingRequests.addOffsetFetchRequest(fetchRequest);

        // Retry the same fetch request while it fails with RetriableException and the retry timeout hasn't expired.
        currentResult.whenComplete((res, error) -> {
            boolean inflightRemoved = pendingRequests.inflightOffsetFetches.remove(fetchRequest);
            if (!inflightRemoved) {
                log.warn("A duplicated, inflight, request was identified, but unable to find it in the " +
                    "outbound buffer:" + fetchRequest);
            }
            if (error == null) {
                maybeUpdateLastSeenEpochIfNewer(res);
                result.complete(res);
            } else {
                if (error instanceof RetriableException || isStaleEpochErrorAndValidEpochAvailable(error)) {
                    if (fetchRequest.isExpired()) {
                        log.debug("OffsetFetch request for {} timed out and won't be retried anymore", fetchRequest.requestedPartitions);
                        result.completeExceptionally(maybeWrapAsTimeoutException(error));
                    } else {
                        fetchRequest.resetFuture();
                        fetchOffsetsWithRetries(fetchRequest, result);
                    }
                } else
                    result.completeExceptionally(error);
            }
        });
    }

    private boolean isStaleEpochErrorAndValidEpochAvailable(Throwable error) {
        return error instanceof StaleMemberEpochException && memberInfo.memberEpoch.isPresent();
    }

    public void updateAutoCommitTimer(final long currentTimeMs) {
        this.autoCommitState.ifPresent(t -> t.updateTimer(currentTimeMs));
    }

    // Visible for testing
    Queue<OffsetCommitRequestState> unsentOffsetCommitRequests() {
        return pendingRequests.unsentOffsetCommits;
    }

    private List<OffsetFetchRequestState> unsentOffsetFetchRequests() {
        return pendingRequests.unsentOffsetFetches;
    }

    /**
     * Update latest member epoch used by the member.
     *
     * @param memberEpoch New member epoch received. To be included in the new request.
     * @param memberId Current member ID. To be included in the new request.
     */
    @Override
    public void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId) {
        if (memberEpoch.isEmpty() && memberInfo.memberEpoch.isPresent()) {
            log.info("Member {} won't include epoch in following offset " +
                "commit/fetch requests because it has left the group.", memberInfo.memberId);
        }
        memberInfo.memberId = memberId;
        memberInfo.memberEpoch = memberEpoch;
    }

    /**
     * @return True if auto-commit is enabled as defined in the config {@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG}
     */
    public boolean autoCommitEnabled() {
        return autoCommitState.isPresent();
    }

    /**
     * Reset the auto-commit timer to the auto-commit interval, so that the next auto-commit is
     * sent out on the interval starting from now. If auto-commit is not enabled this will
     * perform no action.
     */
    public void resetAutoCommitTimer() {
        autoCommitState.ifPresent(AutoCommitState::resetTimer);
    }

    /**
     * Reset the auto-commit timer to the provided time (backoff), so that the next auto-commit is
     * sent out then. If auto-commit is not enabled this will perform no action.
     */
    public void resetAutoCommitTimer(long retryBackoffMs) {
        autoCommitState.ifPresent(s -> s.resetTimer(retryBackoffMs));
    }

    /**
     * Drains the inflight offsetCommits during shutdown because we want to make sure all pending commits are sent
     * before closing.
     */
    public NetworkClientDelegate.PollResult drainPendingOffsetCommitRequests() {
        if (pendingRequests.unsentOffsetCommits.isEmpty())
            return EMPTY;
        List<NetworkClientDelegate.UnsentRequest> requests = pendingRequests.drainPendingCommits();
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, requests);
    }

    private void maybeUpdateLastSeenEpochIfNewer(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((topicPartition, offsetAndMetadata) -> {
            if (offsetAndMetadata != null)
                offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
        });
    }

    class OffsetCommitRequestState extends RetriableRequestState {
        private Map<TopicPartition, OffsetAndMetadata> offsets;
        private final String groupId;
        private final Optional<String> groupInstanceId;

        /**
         * Future containing the offsets that were committed. It completes when a response is
         * received for the commit request.
         */
        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future;

        OffsetCommitRequestState(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                 final String groupId,
                                 final Optional<String> groupInstanceId,
                                 final long deadlineMs,
                                 final long retryBackoffMs,
                                 final long retryBackoffMaxMs,
                                 final MemberInfo memberInfo) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs,
                retryBackoffMaxMs, memberInfo, deadlineTimer(time, deadlineMs));
            this.offsets = offsets;
            this.groupId = groupId;
            this.groupInstanceId = groupInstanceId;
            this.future = new CompletableFuture<>();
        }

        // Visible for testing
        OffsetCommitRequestState(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                 final String groupId,
                                 final Optional<String> groupInstanceId,
                                 final long deadlineMs,
                                 final long retryBackoffMs,
                                 final long retryBackoffMaxMs,
                                 final double jitter,
                                 final MemberInfo memberInfo) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs, 2,
                retryBackoffMaxMs, jitter, memberInfo, deadlineTimer(time, deadlineMs));
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
            data = data.setMemberId(memberInfo.memberId);
            if (memberInfo.memberEpoch.isPresent()) {
                data = data.setGenerationIdOrMemberEpoch(memberInfo.memberEpoch.get());
                lastEpochSentOnCommit = memberInfo.memberEpoch;
            } else {
                lastEpochSentOnCommit = Optional.empty();
            }

            OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(data);

            return buildRequestWithResponseHandling(builder);
        }

        /**
         * Handle OffsetCommitResponse. This will complete the request future successfully if no
         * errors are found in the response. If the response contains errors, this will:
         *   - handle expected errors and fail the future with specific exceptions depending on the error
         *   - fail the future with a non-recoverable KafkaException for all unexpected errors (even if retriable)
         */
        @Override
        public void onResponse(final ClientResponse response) {
            metricsManager.recordRequestLatency(response.requestLatencyMs());
            long currentTimeMs = response.receivedTimeMs();
            OffsetCommitResponse commitResponse = (OffsetCommitResponse) response.responseBody();
            Set<String> unauthorizedTopics = new HashSet<>();
            boolean failedRequestRegistered = false;
            for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : commitResponse.data().topics()) {
                for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                    TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());

                    Errors error = Errors.forCode(partition.errorCode());
                    if (error == Errors.NONE) {
                        OffsetAndMetadata offsetAndMetadata = offsets.get(tp);
                        long offset = offsetAndMetadata.offset();
                        log.debug("OffsetCommit completed successfully for offset {} partition {}", offset, tp);
                        continue;
                    }

                    if (!failedRequestRegistered) {
                        onFailedAttempt(currentTimeMs);
                        failedRequestRegistered = true;
                    }

                    if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                        future.completeExceptionally(GroupAuthorizationException.forGroupId(groupId));
                        return;
                    } else if (error == Errors.COORDINATOR_NOT_AVAILABLE ||
                        error == Errors.NOT_COORDINATOR ||
                        error == Errors.REQUEST_TIMED_OUT) {
                        coordinatorRequestManager.markCoordinatorUnknown(error.message(), currentTimeMs);
                        future.completeExceptionally(error.exception());
                        return;
                    } else if (error == Errors.OFFSET_METADATA_TOO_LARGE ||
                        error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                        future.completeExceptionally(error.exception());
                        return;
                    } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS ||
                        error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        // just retry
                        future.completeExceptionally(error.exception());
                        return;
                    } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                        log.error("OffsetCommit failed with {}", error);
                        future.completeExceptionally(new CommitFailedException("OffsetCommit " +
                            "failed with unknown member ID. " + error.message()));
                        return;
                    } else if (error == Errors.STALE_MEMBER_EPOCH) {
                        log.error("OffsetCommit failed for member {} with stale member epoch error. Last epoch sent: {}",
                            memberInfo.memberId,
                            lastEpochSentOnCommit.isPresent() ? lastEpochSentOnCommit.get() : "undefined");
                        future.completeExceptionally(error.exception());
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        // Collect all unauthorized topics before failing
                        unauthorizedTopics.add(tp.topic());
                    } else {
                        // Fail with a non-retriable KafkaException for all unexpected errors
                        // (even if they are retriable)
                        future.completeExceptionally(new KafkaException("Unexpected error in commit: " + error.message()));
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

        @Override
        String requestDescription() {
            return "OffsetCommit request for offsets " + offsets;
        }

        @Override
        CompletableFuture<?> future() {
            return future;
        }

        void resetFuture() {
            future = new CompletableFuture<>();
        }

        @Override
        void removeRequest() {
            if (!unsentOffsetCommitRequests().remove(this)) {
                log.warn("OffsetCommit request to remove not found in the outbound buffer: {}", this);
            }
        }
    }

    // Visible for testing
    Optional<Integer> lastEpochSentOnCommit() {
        return lastEpochSentOnCommit;
    }

    /**
     * Represents a request that can be retried or aborted, based on member ID and epoch
     * information.
     */
    abstract class RetriableRequestState extends TimedRequestState {

        /**
         * Member info (ID and epoch) to be included in the request if present.
         */
        final MemberInfo memberInfo;

        RetriableRequestState(LogContext logContext, String owner, long retryBackoffMs,
                              long retryBackoffMaxMs, MemberInfo memberInfo, Timer timer) {
            super(logContext, owner, retryBackoffMs, retryBackoffMaxMs, timer);
            this.memberInfo = memberInfo;
        }

        // Visible for testing
        RetriableRequestState(LogContext logContext, String owner, long retryBackoffMs, int retryBackoffExpBase,
                              long retryBackoffMaxMs, double jitter, MemberInfo memberInfo, Timer timer) {
            super(logContext, owner, retryBackoffMs, retryBackoffExpBase, retryBackoffMaxMs, jitter, timer);
            this.memberInfo = memberInfo;
        }

        /**
         * @return String containing the request name and arguments, to be used for logging
         * purposes.
         */
        abstract String requestDescription();

        /**
         * @return Future that will complete with the request response or failure.
         */
        abstract CompletableFuture<?> future();

        /**
         * Complete the request future with a TimeoutException if the request has been sent out
         * at least once and the timeout has been reached.
         */
        void maybeExpire() {
            if (numAttempts > 0 && isExpired()) {
                removeRequest();
                future().completeExceptionally(new TimeoutException(requestDescription() +
                    " could not complete before timeout expired."));
            }
        }

        /**
         * Build request with the given builder, including response handling logic.
         */
        NetworkClientDelegate.UnsentRequest buildRequestWithResponseHandling(final AbstractRequest.Builder<?> builder) {
            NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
                builder,
                coordinatorRequestManager.coordinator()
            );
            request.whenComplete(
                (response, throwable) -> {
                    long completionTimeMs = request.handler().completionTimeMs();
                    handleClientResponse(response, throwable, completionTimeMs);
                });
            return request;
        }

        private void handleClientResponse(final ClientResponse response,
                                          final Throwable error,
                                          final long requestCompletionTimeMs) {
            try {
                if (error == null) {
                    onResponse(response);
                } else {
                    log.debug("{} completed with error", requestDescription(), error);
                    onFailedAttempt(requestCompletionTimeMs);
                    coordinatorRequestManager.handleCoordinatorDisconnect(error, requestCompletionTimeMs);
                    future().completeExceptionally(error);
                }
            } catch (Throwable t) {
                log.error("Unexpected error handling response for {}", requestDescription(), t);
                future().completeExceptionally(t);
            }
        }

        @Override
        public String toStringBase() {
            return super.toStringBase() + ", " + memberInfo;
        }

        abstract void onResponse(final ClientResponse response);

        abstract void removeRequest();
    }

    class OffsetFetchRequestState extends RetriableRequestState {

        /**
         * Partitions to get committed offsets for.
         */
        public final Set<TopicPartition> requestedPartitions;

        /**
         * Future with the result of the request. This can be reset using {@link #resetFuture()}
         * to get a new result when the request is retried.
         */
        private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future;

        public OffsetFetchRequestState(final Set<TopicPartition> partitions,
                                       final long retryBackoffMs,
                                       final long retryBackoffMaxMs,
                                       final long deadlineMs,
                                       final MemberInfo memberInfo) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs,
                retryBackoffMaxMs, memberInfo, deadlineTimer(time, deadlineMs));
            this.requestedPartitions = partitions;
            this.future = new CompletableFuture<>();
        }

        public OffsetFetchRequestState(final Set<TopicPartition> partitions,
                                       final long retryBackoffMs,
                                       final long retryBackoffMaxMs,
                                       final long deadlineMs,
                                       final double jitter,
                                       final MemberInfo memberInfo) {
            super(logContext, CommitRequestManager.class.getSimpleName(), retryBackoffMs, 2,
                retryBackoffMaxMs, jitter, memberInfo, deadlineTimer(time, deadlineMs));
            this.requestedPartitions = partitions;
            this.future = new CompletableFuture<>();
        }

        public boolean sameRequest(final OffsetFetchRequestState request) {
            return requestedPartitions.equals(request.requestedPartitions);
        }

        public NetworkClientDelegate.UnsentRequest toUnsentRequest() {

            OffsetFetchRequest.Builder builder = memberInfo.memberEpoch.
                map(epoch -> new OffsetFetchRequest.Builder(
                    groupId,
                    memberInfo.memberId,
                    epoch,
                    true,
                    new ArrayList<>(this.requestedPartitions),
                    throwOnFetchStableOffsetUnsupported))
                // Building request without passing member ID/epoch to leave the logic to choose
                // default values when not present on the request builder.
                .orElseGet(() -> new OffsetFetchRequest.Builder(
                    groupId,
                    true,
                    new ArrayList<>(this.requestedPartitions),
                    throwOnFetchStableOffsetUnsupported));
            return buildRequestWithResponseHandling(builder);
        }

        /**
         * Handle OffsetFetch response, including successful and failed.
         */
        @Override
        void onResponse(final ClientResponse response) {
            long currentTimeMs = response.receivedTimeMs();
            OffsetFetchResponse fetchResponse = (OffsetFetchResponse) response.responseBody();
            Errors responseError = fetchResponse.groupLevelError(groupId);
            if (responseError != Errors.NONE) {
                onFailure(currentTimeMs, responseError);
                return;
            }
            onSuccess(currentTimeMs, fetchResponse);
        }

        /**
         * Handle failed responses. This will retry if the error is retriable, or complete the
         * result future exceptionally in the case of non-recoverable or unexpected errors.
         */
        private void onFailure(final long currentTimeMs,
                               final Errors responseError) {
            log.debug("Offset fetch failed: {}", responseError.message());
            onFailedAttempt(currentTimeMs);
            ApiException exception = responseError.exception();
            if (responseError == COORDINATOR_LOAD_IN_PROGRESS) {
                future.completeExceptionally(exception);
            } else if (responseError == Errors.UNKNOWN_MEMBER_ID) {
                log.error("OffsetFetch failed with {} because the member is not part of the group" +
                    " anymore.", responseError);
                future.completeExceptionally(exception);
            } else if (responseError == Errors.STALE_MEMBER_EPOCH) {
                log.error("OffsetFetch failed with {} and the consumer is not part " +
                    "of the group anymore (it probably left the group, got fenced" +
                    " or failed). The request cannot be retried and will fail.", responseError);
                future.completeExceptionally(exception);
            } else if (responseError == Errors.NOT_COORDINATOR || responseError == Errors.COORDINATOR_NOT_AVAILABLE) {
                // Re-discover the coordinator and retry
                coordinatorRequestManager.markCoordinatorUnknown("error response " + responseError.name(), currentTimeMs);
                future.completeExceptionally(exception);
            } else if (exception instanceof RetriableException) {
                future.completeExceptionally(exception);
            } else if (responseError == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.completeExceptionally(GroupAuthorizationException.forGroupId(groupId));
            } else {
                // Fail with a non-retriable KafkaException for all unexpected errors
                future.completeExceptionally(new KafkaException("Unexpected error in fetch offset response: " + responseError.message()));
            }
        }

        @Override
        String requestDescription() {
            return "OffsetFetch request for partitions " + requestedPartitions;
        }

        @Override
        CompletableFuture<?> future() {
            return future;
        }

        void resetFuture() {
            future = new CompletableFuture<>();
        }

        @Override
        void removeRequest() {
            if (!unsentOffsetFetchRequests().remove(this)) {
                log.warn("OffsetFetch request to remove not found in the outbound buffer: {}", this);
            }
        }

        /**
         * Handle OffsetFetch response that has no group level errors. This will look for
         * partition level errors and fail the future accordingly, also recording a failed request
         * attempt. If no partition level errors are found, this will complete the future with the
         * offsets contained in the response, and record a successful request attempt.
         */
        private void onSuccess(final long currentTimeMs,
                               final OffsetFetchResponse response) {
            Set<String> unauthorizedTopics = null;
            Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData =
                    response.partitionDataMap(groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(responseData.size());
            Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();
            boolean failedRequestRegistered = false;
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : responseData.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                if (partitionData.hasError()) {
                    Errors error = partitionData.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (!failedRequestRegistered) {
                        onFailedAttempt(currentTimeMs);
                        failedRequestRegistered = true;
                    }

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.completeExceptionally(new KafkaException("Topic or Partition " + tp + " does not exist"));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        unauthorizedTopics.add(tp.topic());
                    } else if (error == Errors.UNSTABLE_OFFSET_COMMIT) {
                        unstableTxnOffsetTopicPartitions.add(tp);
                    } else {
                        // Fail with a non-retriable KafkaException for all unexpected partition
                        // errors (even if they are retriable)
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
                future.completeExceptionally(new UnstableOffsetCommitException("There are " +
                    "unstable offsets for the requested topic partitions"));
            } else {
                onSuccessfulAttempt(currentTimeMs);
                future.complete(offsets);
            }
        }

        private void chainFuture(
            final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> otherFuture) {
            this.future.whenComplete((r, t) -> {
                if (t != null) {
                    otherFuture.completeExceptionally(t);
                } else {
                    otherFuture.complete(r);
                }
            });
        }

        @Override
        public String toStringBase() {
            return super.toStringBase() +
                    ", requestedPartitions=" + requestedPartitions;
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

        /**
         * Add a commit request to the queue, so that it's sent out on the next call to
         * {@link #poll(long)}. This is used from all commits (sync, async, auto-commit).
         */
        OffsetCommitRequestState addOffsetCommitRequest(OffsetCommitRequestState request) {
            log.debug("Enqueuing OffsetCommit request for offsets: {}", request.offsets);
            unsentOffsetCommits.add(request);
            return request;
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
                log.debug("Duplicated unsent offset fetch request found for partitions: {}", request.requestedPartitions);
                dupe.orElseGet(inflight::get).chainFuture(request.future);
            } else {
                log.debug("Enqueuing offset fetch request for partitions: {}", request.requestedPartitions);
                this.unsentOffsetFetches.add(request);
            }
            return request.future;
        }

        /**
         * Clear {@code unsentOffsetCommits} and moves all the sendable request in {@code
         * unsentOffsetFetches} to the {@code inflightOffsetFetches} to bookkeep all the inflight
         * requests. Note: Sendable requests are determined by their timer as we are expecting
         * backoff on failed attempt. See {@link RequestState}.
         */
        List<NetworkClientDelegate.UnsentRequest> drain(final long currentTimeMs) {
            // not ready to sent request
            List<OffsetCommitRequestState> unreadyCommitRequests = unsentOffsetCommits.stream()
                .filter(request -> !request.canSendRequest(currentTimeMs))
                .collect(Collectors.toList());

            failAndRemoveExpiredCommitRequests();

            // Add all unsent offset commit requests to the unsentRequests list
            List<NetworkClientDelegate.UnsentRequest> unsentRequests = unsentOffsetCommits.stream()
                .filter(request -> request.canSendRequest(currentTimeMs))
                .peek(request -> request.onSendAttempt(currentTimeMs))
                .map(OffsetCommitRequestState::toUnsentRequest)
                .collect(Collectors.toCollection(ArrayList::new));

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

        /**
         * Find the unsent commit requests that have expired, remove them and complete their
         * futures with a TimeoutException.
         */
        private void failAndRemoveExpiredCommitRequests() {
            Queue<OffsetCommitRequestState> requestsToPurge = new LinkedList<>(unsentOffsetCommits);
            requestsToPurge.forEach(RetriableRequestState::maybeExpire);
        }

        private void clearAll() {
            unsentOffsetCommits.clear();
            unsentOffsetFetches.clear();
        }

        private List<NetworkClientDelegate.UnsentRequest> drainPendingCommits() {
            List<NetworkClientDelegate.UnsentRequest> res = unsentOffsetCommits.stream()
                .map(OffsetCommitRequestState::toUnsentRequest)
                .collect(Collectors.toCollection(ArrayList::new));
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

        private final Logger log;

        public AutoCommitState(
                final Time time,
                final long autoCommitInterval,
                final LogContext logContext) {
            this.autoCommitInterval = autoCommitInterval;
            this.timer = time.timer(autoCommitInterval);
            this.hasInflightCommit = false;
            this.log = logContext.logger(getClass());
        }

        public boolean shouldAutoCommit() {
            if (!this.timer.isExpired()) {
                return false;
            }
            if (this.hasInflightCommit) {
                log.trace("Skipping auto-commit on the interval because a previous one is still in-flight.");
                return false;
            }
            return true;
        }

        public void resetTimer() {
            this.timer.reset(autoCommitInterval);
        }

        public void resetTimer(long retryBackoffMs) {
            this.timer.reset(retryBackoffMs);
        }

        public long remainingMs(final long currentTimeMs) {
            this.timer.update(currentTimeMs);
            return this.timer.remainingMs();
        }

        public void updateTimer(final long currentTimeMs) {
            this.timer.update(currentTimeMs);
        }

        public void setInflightCommitStatus(final boolean inflightCommitStatus) {
            this.hasInflightCommit = inflightCommitStatus;
        }
    }

    static class MemberInfo {
        String memberId = "";
        Optional<Integer> memberEpoch = Optional.empty();

        @Override
        public String toString() {
            return "memberId=" + memberId +
                    ", memberEpoch=" + (memberEpoch.isPresent() ? memberEpoch.get() : "undefined");
        }
    }
}
