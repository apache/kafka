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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.MembershipManager.LocalAssignment;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * <p>Manages the request creation and response handling for the heartbeat. The module creates a
 * {@link ConsumerGroupHeartbeatRequest} using the state stored in the {@link MembershipManager} and enqueue it to
 * the network queue to be sent out. Once the response is received, the module will update the state in the
 * {@link MembershipManager} and handle any errors.</p>
 *
 * <p>The manager will try to send a heartbeat when the member is in {@link MemberState#STABLE},
 * {@link MemberState#JOINING}, or {@link MemberState#RECONCILING}. Which mean the member is either in a stable
 * group, is trying to join a group, or is in the process of reconciling the assignment changes.</p>
 *
 * <p>If the member got kick out of a group, it will try to give up the current assignment by invoking {@code
 * OnPartitionsLost} because reattempting to join again with a zero epoch.</p>
 *
 * <p>If the member does not have groupId configured or encountering fatal exceptions, a heartbeat will not be sent.</p>
 *
 * <p>If the coordinator not is not found, we will skip sending the heartbeat and try to find a coordinator first.</p>
 *
 * <p>If the heartbeat failed due to retriable errors, such as, TimeoutException. The subsequent attempt will be
 * backoff exponentially.</p>
 *
 * <p>When the member completes the assignment reconciliation, the {@link HeartbeatRequestState} will be reset so
 * that a heartbeat will be sent in the next event loop.</p>
 *
 * <p>See {@link HeartbeatRequestState} for more details.</p>
 */
public class HeartbeatRequestManager implements RequestManager {

    private final Logger logger;

    /**
     * Time that the group coordinator will wait on member to revoke its partitions. This is provided by the group
     * coordinator in the heartbeat
     */
    private final int maxPollIntervalMs;

    /**
     * CoordinatorRequestManager manages the connection to the group coordinator
     */
    private final CoordinatorRequestManager coordinatorRequestManager;

    /**
     * HeartbeatRequestState manages heartbeat request timing and retries
     */
    private final HeartbeatRequestState heartbeatRequestState;

    /*
     * HeartbeatState manages building the heartbeat requests correctly
     */
    private final HeartbeatState heartbeatState;

    /**
     * MembershipManager manages member's essential attributes like epoch and id, and its rebalance state
     */
    private final MembershipManager membershipManager;

    /**
     * ErrorEventHandler allows the background thread to propagate errors back to the user
     */
    private final BackgroundEventHandler backgroundEventHandler;

    /**
     * Timer for tracking the time since the last consumer poll.  If the timer expires, the consumer will stop
     * sending heartbeat until the next poll.
     */
    private final Timer pollTimer;

    /**
     * Holding the heartbeat sensor to measure heartbeat timing and response latency
     */
    private final HeartbeatMetricsManager metricsManager;

    public HeartbeatRequestManager(
        final LogContext logContext,
        final Time time,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final SubscriptionState subscriptions,
        final MembershipManager membershipManager,
        final BackgroundEventHandler backgroundEventHandler,
        final Metrics metrics) {
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.logger = logContext.logger(getClass());
        this.membershipManager = membershipManager;
        this.backgroundEventHandler = backgroundEventHandler;
        this.maxPollIntervalMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.heartbeatState = new HeartbeatState(subscriptions, membershipManager, maxPollIntervalMs);
        this.heartbeatRequestState = new HeartbeatRequestState(logContext, time, 0, retryBackoffMs,
            retryBackoffMaxMs, maxPollIntervalMs);
        this.pollTimer = time.timer(maxPollIntervalMs);
        this.metricsManager = new HeartbeatMetricsManager(metrics);
    }

    // Visible for testing
    HeartbeatRequestManager(
        final LogContext logContext,
        final Timer timer,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final MembershipManager membershipManager,
        final HeartbeatState heartbeatState,
        final HeartbeatRequestState heartbeatRequestState,
        final BackgroundEventHandler backgroundEventHandler,
        final Metrics metrics) {
        this.logger = logContext.logger(this.getClass());
        this.maxPollIntervalMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.heartbeatRequestState = heartbeatRequestState;
        this.heartbeatState = heartbeatState;
        this.membershipManager = membershipManager;
        this.backgroundEventHandler = backgroundEventHandler;
        this.pollTimer = timer;
        this.metricsManager = new HeartbeatMetricsManager(metrics);
    }

    /**
     * This will build a heartbeat request if one must be sent, determined based on the member
     * state. A heartbeat is sent in the following situations:
     * <ol>
     *     <li>Member is part of the consumer group or wants to join it.</li>
     *     <li>The heartbeat interval has expired, or the member is in a state that indicates
     *     that it should heartbeat without waiting for the interval.</li>
     * </ol>
     * This will also determine the maximum wait time until the next poll based on the member's
     * state.
     * <ol>
     *     <li>If the member is without a coordinator or is in a failed state, the timer is set
     *     to Long.MAX_VALUE, as there's no need to send a heartbeat.</li>
     *     <li>If the member cannot send a heartbeat due to either exponential backoff, it will
     *     return the remaining time left on the backoff timer.</li>
     *     <li>If the member's heartbeat timer has not expired, It will return the remaining time
     *     left on the heartbeat timer.</li>
     *     <li>If the member can send a heartbeat, the timer is set to the current heartbeat interval.</li>
     * </ol>
     *
     * @return {@link PollResult} that includes a heartbeat request if one must be sent, and the
     * time to wait until the next poll.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        if (!coordinatorRequestManager.coordinator().isPresent() ||
            membershipManager.shouldSkipHeartbeat()) {
            membershipManager.onHeartbeatRequestSkipped();
            return NetworkClientDelegate.PollResult.EMPTY;
        }
        pollTimer.update(currentTimeMs);
        if (pollTimer.isExpired() && !membershipManager.isLeavingGroup()) {
            logger.warn("Consumer poll timeout has expired. This means the time between " +
                "subsequent calls to poll() was longer than the configured max.poll.interval.ms, " +
                "which typically implies that the poll loop is spending too much time processing " +
                "messages. You can address this either by increasing max.poll.interval.ms or by " +
                "reducing the maximum size of batches returned in poll() with max.poll.records.");

            membershipManager.transitionToSendingLeaveGroup(true);
            NetworkClientDelegate.UnsentRequest leaveHeartbeat = makeHeartbeatRequest(currentTimeMs, true);

            // We can ignore the leave response because we can join before or after receiving the response.
            heartbeatRequestState.reset();
            heartbeatState.reset();
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(leaveHeartbeat));
        }

        boolean heartbeatNow = membershipManager.shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight();
        if (!heartbeatRequestState.canSendRequest(currentTimeMs) && !heartbeatNow) {
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.timeToNextHeartbeatMs(currentTimeMs));
        }

        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(currentTimeMs, false);
        return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
    }

    /**
     * Returns the {@link MembershipManager} that this request manager is using to track the state of the group.
     * This is provided so that the {@link ApplicationEventProcessor} can access the state for querying or updating.
     */
    public MembershipManager membershipManager() {
        return membershipManager;
    }

    /**
     * Returns the delay for which the application thread can safely wait before it should be responsive
     * to results from the request managers. For example, the subscription state can change when heartbeats
     * are sent, so blocking for longer than the heartbeat interval might mean the application thread is not
     * responsive to changes.
     *
     * Similarly, we may have to unblock the application thread to send a `PollApplicationEvent` to make sure
     * our poll timer will not expire while we are polling.
     *
     * <p>In the event that heartbeats are currently being skipped, this still returns the next heartbeat
     * delay rather than {@code Long.MAX_VALUE} so that the application thread remains responsive.
     */
    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        pollTimer.update(currentTimeMs);
        if (
            pollTimer.isExpired() ||
                (membershipManager.shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight())
        ) {
            return 0L;
        }
        return Math.min(pollTimer.remainingMs() / 2, heartbeatRequestState.timeToNextHeartbeatMs(currentTimeMs));
    }

    /**
     * Reset the poll timer, indicating that the user has called consumer.poll(). If the member
     * is in {@link MemberState#STALE} state due to expired poll timer, this will transition the
     * member to {@link MemberState#JOINING}, so that it rejoins the group.
     */
    public void resetPollTimer(final long pollMs) {
        pollTimer.update(pollMs);
        if (pollTimer.isExpired()) {
            logger.warn("Time between subsequent calls to poll() was longer than the configured " +
                "max.poll.interval.ms, exceeded approximately by {} ms.", pollTimer.isExpiredBy());
            membershipManager.maybeRejoinStaleMember();
        }
        pollTimer.reset(maxPollIntervalMs);
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final long currentTimeMs,
                                                                     final boolean ignoreResponse) {
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(ignoreResponse);
        heartbeatRequestState.onSendAttempt(currentTimeMs);
        membershipManager.onHeartbeatRequestSent();
        metricsManager.recordHeartbeatSentMs(currentTimeMs);
        heartbeatRequestState.resetTimer();
        return request;
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final boolean ignoreResponse) {
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            new ConsumerGroupHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
            coordinatorRequestManager.coordinator());
        if (ignoreResponse)
            return logResponse(request);
        else
            return request.whenComplete((response, exception) -> {
                long completionTimeMs = request.handler().completionTimeMs();
                if (response != null) {
                    metricsManager.recordRequestLatency(response.requestLatencyMs());
                    onResponse((ConsumerGroupHeartbeatResponse) response.responseBody(), completionTimeMs);
                } else {
                    onFailure(exception, completionTimeMs);
                }
            });
    }

    private NetworkClientDelegate.UnsentRequest logResponse(final NetworkClientDelegate.UnsentRequest request) {
        return request.whenComplete((response, exception) -> {
            if (response != null) {
                metricsManager.recordRequestLatency(response.requestLatencyMs());
                Errors error =
                    Errors.forCode(((ConsumerGroupHeartbeatResponse) response.responseBody()).data().errorCode());
                if (error == Errors.NONE)
                    logger.debug("GroupHeartbeat responded successfully: {}", response);
                else
                    logger.error("GroupHeartbeat failed because of {}: {}", error, response);
            } else {
                logger.error("GroupHeartbeat failed because of unexpected exception.", exception);
            }
        });
    }

    private void onFailure(final Throwable exception, final long responseTimeMs) {
        this.heartbeatRequestState.onFailedAttempt(responseTimeMs);
        this.heartbeatState.reset();
        if (exception instanceof RetriableException) {
            String message = String.format("GroupHeartbeatRequest failed because of the retriable exception. " +
                    "Will retry in %s ms: %s",
                heartbeatRequestState.remainingBackoffMs(responseTimeMs),
                exception.getMessage());
            logger.debug(message);
        } else {
            logger.error("GroupHeartbeatRequest failed due to fatal error: " + exception.getMessage());
            handleFatalFailure(exception);
        }
    }

    private void onResponse(final ConsumerGroupHeartbeatResponse response, long currentTimeMs) {
        if (Errors.forCode(response.data().errorCode()) == Errors.NONE) {
            heartbeatRequestState.updateHeartbeatIntervalMs(response.data().heartbeatIntervalMs());
            heartbeatRequestState.onSuccessfulAttempt(currentTimeMs);
            membershipManager.onHeartbeatSuccess(response.data());
            return;
        }
        onErrorResponse(response, currentTimeMs);
    }

    private void onErrorResponse(final ConsumerGroupHeartbeatResponse response,
                                 final long currentTimeMs) {
        Errors error = Errors.forCode(response.data().errorCode());
        String errorMessage = response.data().errorMessage();
        String message;

        this.heartbeatState.reset();
        this.heartbeatRequestState.onFailedAttempt(currentTimeMs);
        membershipManager.onHeartbeatFailure();

        switch (error) {
            case NOT_COORDINATOR:
                // the manager should retry immediately when the coordinator node becomes available again
                message = String.format("GroupHeartbeatRequest failed because the group coordinator %s is incorrect. " +
                                "Will attempt to find the coordinator again and retry",
                        coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next HB is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_NOT_AVAILABLE:
                message = String.format("GroupHeartbeatRequest failed because the group coordinator %s is not available. " +
                                "Will attempt to find the coordinator again and retry",
                        coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next HB is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // the manager will backoff and retry
                message = String.format("GroupHeartbeatRequest failed because the group coordinator %s is still loading." +
                                "Will retry",
                        coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                break;

            case GROUP_AUTHORIZATION_FAILED:
                GroupAuthorizationException exception =
                        GroupAuthorizationException.forGroupId(membershipManager.groupId());
                logger.error("GroupHeartbeatRequest failed due to group authorization failure: {}", exception.getMessage());
                handleFatalFailure(error.exception(exception.getMessage()));
                break;

            case UNRELEASED_INSTANCE_ID:
                logger.error("GroupHeartbeatRequest failed due to unreleased instance id {}: {}",
                        membershipManager.groupInstanceId().orElse("null"), errorMessage);
                handleFatalFailure(Errors.UNRELEASED_INSTANCE_ID.exception(errorMessage));
                break;

            case INVALID_REQUEST:
            case GROUP_MAX_SIZE_REACHED:
            case UNSUPPORTED_ASSIGNOR:
            case UNSUPPORTED_VERSION:
                logger.error("GroupHeartbeatRequest failed due to {}: {}", error, errorMessage);
                handleFatalFailure(error.exception(errorMessage));
                break;

            case FENCED_MEMBER_EPOCH:
                message = String.format("GroupHeartbeatRequest failed for member %s because epoch %s is fenced.",
                        membershipManager.memberId(), membershipManager.memberEpoch());
                logInfo(message, response, currentTimeMs);
                membershipManager.transitionToFenced();
                // Skip backoff so that a next HB to rejoin is sent as soon as the fenced member releases its assignment
                heartbeatRequestState.reset();
                break;

            case UNKNOWN_MEMBER_ID:
                message = String.format("GroupHeartbeatRequest failed because member %s is unknown.",
                        membershipManager.memberId());
                logInfo(message, response, currentTimeMs);
                membershipManager.transitionToFenced();
                // Skip backoff so that a next HB to rejoin is sent as soon as the fenced member releases its assignment
                heartbeatRequestState.reset();
                break;

            default:
                // If the manager receives an unknown error - there could be a bug in the code or a new error code
                logger.error("GroupHeartbeatRequest failed due to unexpected error {}: {}", error, errorMessage);
                handleFatalFailure(error.exception(errorMessage));
                break;
        }
    }

    private void logInfo(final String message,
                         final ConsumerGroupHeartbeatResponse response,
                         final long currentTimeMs) {
        logger.info("{} in {}ms: {}",
            message,
            heartbeatRequestState.remainingBackoffMs(currentTimeMs),
            response.data().errorMessage());
    }

    private void handleFatalFailure(Throwable error) {
        backgroundEventHandler.add(new ErrorEvent(error));
        membershipManager.transitionToFatal();
    }

    /**
     * Represents the state of a heartbeat request, including logic for timing, retries, and exponential backoff. The
     * object extends {@link RequestState} to enable exponential backoff and duplicated request handling. The two fields
     * that it holds are:
     */
    static class HeartbeatRequestState extends RequestState {
        /**
         *  heartbeatTimer tracks the time since the last heartbeat was sent
         */
        private final Timer heartbeatTimer;

        /**
         * The heartbeat interval which is acquired/updated through the heartbeat request
         */
        private long heartbeatIntervalMs;

        public HeartbeatRequestState(
            final LogContext logContext,
            final Time time,
            final long heartbeatIntervalMs,
            final long retryBackoffMs,
            final long retryBackoffMaxMs,
            final double jitter) {
            super(logContext, HeartbeatRequestState.class.getName(), retryBackoffMs, 2, retryBackoffMaxMs, jitter);
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.heartbeatTimer = time.timer(heartbeatIntervalMs);
        }

        private void update(final long currentTimeMs) {
            this.heartbeatTimer.update(currentTimeMs);
        }

        public void resetTimer() {
            this.heartbeatTimer.reset(heartbeatIntervalMs);
        }

        /**
         * Check if a heartbeat request should be sent on the current time. A heartbeat should be
         * sent if the heartbeat timer has expired, backoff has expired, and there is no request
         * in-flight.
         */
        @Override
        public boolean canSendRequest(final long currentTimeMs) {
            update(currentTimeMs);
            return heartbeatTimer.isExpired() && super.canSendRequest(currentTimeMs);
        }

        public long timeToNextHeartbeatMs(final long currentTimeMs) {
            if (heartbeatTimer.isExpired()) {
                return this.remainingBackoffMs(currentTimeMs);
            }
            return heartbeatTimer.remainingMs();
        }

        public void onFailedAttempt(final long currentTimeMs) {
            // Reset timer to allow sending HB after a failure without waiting for the interval.
            // After a failure, a next HB may be needed with backoff (ex. errors that lead to
            // retries, like coordinator load error), or immediately (ex. errors that lead to
            // rejoining, like fencing errors).
            heartbeatTimer.reset(0);
            super.onFailedAttempt(currentTimeMs);
        }

        private void updateHeartbeatIntervalMs(final long heartbeatIntervalMs) {
            if (this.heartbeatIntervalMs == heartbeatIntervalMs) {
                // no need to update the timer if the interval hasn't changed
                return;
            }
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.heartbeatTimer.updateAndReset(heartbeatIntervalMs);
        }
    }

    /**
     * Builds the heartbeat requests correctly, ensuring that all information is sent according to
     * the protocol, but subsequent requests do not send information which has not changed. This
     * is important to ensure that reconciliation completes successfully.
     */
    static class HeartbeatState {
        private final SubscriptionState subscriptions;
        private final MembershipManager membershipManager;
        private final int rebalanceTimeoutMs;
        private final SentFields sentFields;

        public HeartbeatState(
            final SubscriptionState subscriptions,
            final MembershipManager membershipManager,
            final int rebalanceTimeoutMs) {
            this.subscriptions = subscriptions;
            this.membershipManager = membershipManager;
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            this.sentFields = new SentFields();
        }


        public void reset() {
            sentFields.reset();
        }

        public ConsumerGroupHeartbeatRequestData buildRequestData() {
            ConsumerGroupHeartbeatRequestData data = new ConsumerGroupHeartbeatRequestData();

            // GroupId - always sent
            data.setGroupId(membershipManager.groupId());

            // MemberId - always sent, empty until it has been received from the coordinator
            data.setMemberId(membershipManager.memberId());

            // MemberEpoch - always sent
            data.setMemberEpoch(membershipManager.memberEpoch());

            // InstanceId - set if present
            membershipManager.groupInstanceId().ifPresent(data::setInstanceId);

            boolean sendAllFields = membershipManager.state() == MemberState.JOINING;

            // RebalanceTimeoutMs - only sent when joining or if it has changed since the last heartbeat
            if (sendAllFields || sentFields.rebalanceTimeoutMs != rebalanceTimeoutMs) {
                data.setRebalanceTimeoutMs(rebalanceTimeoutMs);
                sentFields.rebalanceTimeoutMs = rebalanceTimeoutMs;
            }

            // SubscribedTopicNames - only sent if has changed since the last heartbeat
            TreeSet<String> subscribedTopicNames = new TreeSet<>(this.subscriptions.subscription());
            if (sendAllFields || !subscribedTopicNames.equals(sentFields.subscribedTopicNames)) {
                data.setSubscribedTopicNames(new ArrayList<>(this.subscriptions.subscription()));
                sentFields.subscribedTopicNames = subscribedTopicNames;
            }

            // ServerAssignor - sent when joining or if it has changed since the last heartbeat
            this.membershipManager.serverAssignor().ifPresent(serverAssignor -> {
                if (sendAllFields || !serverAssignor.equals(sentFields.serverAssignor)) {
                    data.setServerAssignor(serverAssignor);
                    sentFields.serverAssignor = serverAssignor;
                }
            });

            // ClientAssignors - not supported yet

            // TopicPartitions - sent when joining or with the first heartbeat after a new assignment from
            // the server was reconciled. This is ensured by resending the topic partitions whenever the
            // local assignment, including its local epoch is changed (although the local epoch is not sent
            // in the heartbeat).
            LocalAssignment local = membershipManager.currentAssignment();
            if (sendAllFields || !local.equals(sentFields.localAssignment)) {
                List<ConsumerGroupHeartbeatRequestData.TopicPartitions> topicPartitions =
                    buildTopicPartitionsList(local.partitions);
                data.setTopicPartitions(topicPartitions);
                sentFields.localAssignment = local;
            }

            return data;
        }

        private List<ConsumerGroupHeartbeatRequestData.TopicPartitions> buildTopicPartitionsList(Map<Uuid, SortedSet<Integer>> topicIdPartitions) {
            return topicIdPartitions.entrySet().stream().map(
                    entry -> new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(entry.getKey())
                        .setPartitions(new ArrayList<>(entry.getValue())))
                .collect(Collectors.toList());
        }

        // Fields of ConsumerHeartbeatRequest sent in the most recent request
        static class SentFields {
            private int rebalanceTimeoutMs = -1;
            private TreeSet<String> subscribedTopicNames = null;
            private String serverAssignor = null;
            private LocalAssignment localAssignment = null;

            SentFields() {}

            void reset() {
                subscribedTopicNames = null;
                rebalanceTimeoutMs = -1;
                serverAssignor = null;
                localAssignment = null;
            }
        }
    }
}
