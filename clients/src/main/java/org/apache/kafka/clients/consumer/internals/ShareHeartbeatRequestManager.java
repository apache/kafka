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
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeSet;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_SHARE_METRIC_GROUP_PREFIX;

/**
 * <p>Manages the request creation and response handling for the heartbeat of a share group. The module creates a
 * {@link ShareGroupHeartbeatRequest} using the state stored in the {@link ShareMembershipManager} and enqueues it to
 * the network queue to be sent out. Once the response is received, the module will update the state in the
 * {@link ShareMembershipManager} and handle any errors.</p>
 *
 * <p>The manager will try to send a heartbeat when the member is in {@link MemberState#STABLE},
 * {@link MemberState#JOINING}, or {@link MemberState#RECONCILING}. Which mean the member is either in a stable
 * group, is trying to join a group, or is in the process of reconciling the assignment changes.</p>
 *
 * <p>If the member got kick out of a group, it will give up the current assignment and join again with a zero epoch.</p>
 *
 * <p>If the member does not have groupId configured or encounters fatal exceptions, a heartbeat will not be sent.</p>
 *
 * <p>If the coordinator is not found, we will skip sending the heartbeat and try to find a coordinator first.</p>
 *
 * <p>If the heartbeat failed due to retriable errors, such as, TimeoutException, the subsequent attempt will be
 * backoff exponentially.</p>
 *
 * <p>When the member completes the assignment reconciliation, the {@link HeartbeatRequestState} will be reset so
 * that a heartbeat will be sent in the next event loop.</p>
 *
 * <p>See {@link HeartbeatRequestState} for more details.</p>
 */
public class ShareHeartbeatRequestManager implements RequestManager {

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
     * ShareMembershipManager manages member's essential attributes like epoch and id, and its rebalance state
     */
    private final ShareMembershipManager shareMembershipManager;

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

    public ShareHeartbeatRequestManager(
            final LogContext logContext,
            final Time time,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final SubscriptionState subscriptions,
            final ShareMembershipManager shareMembershipManager,
            final BackgroundEventHandler backgroundEventHandler,
            final Metrics metrics) {
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.logger = logContext.logger(getClass());
        this.shareMembershipManager = shareMembershipManager;
        this.backgroundEventHandler = backgroundEventHandler;
        this.maxPollIntervalMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.heartbeatState = new HeartbeatState(subscriptions, shareMembershipManager);
        this.heartbeatRequestState = new HeartbeatRequestState(logContext, time, 0, retryBackoffMs,
                retryBackoffMaxMs, maxPollIntervalMs);
        this.pollTimer = time.timer(maxPollIntervalMs);
        this.metricsManager = new HeartbeatMetricsManager(metrics, CONSUMER_SHARE_METRIC_GROUP_PREFIX);
    }

    // Visible for testing
    ShareHeartbeatRequestManager(
            final LogContext logContext,
            final Timer timer,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final ShareMembershipManager shareMembershipManager,
            final HeartbeatState heartbeatState,
            final HeartbeatRequestState heartbeatRequestState,
            final BackgroundEventHandler backgroundEventHandler,
            final Metrics metrics) {
        this.logger = logContext.logger(this.getClass());
        this.maxPollIntervalMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.heartbeatRequestState = heartbeatRequestState;
        this.heartbeatState = heartbeatState;
        this.shareMembershipManager = shareMembershipManager;
        this.backgroundEventHandler = backgroundEventHandler;
        this.pollTimer = timer;
        this.metricsManager = new HeartbeatMetricsManager(metrics, CONSUMER_SHARE_METRIC_GROUP_PREFIX);
    }

    /**
     * This will build a heartbeat request if one must be sent, determined based on the member
     * state. A heartbeat is sent in the following situations:
     * <ol>
     *     <li>Member is part of the share group or wants to join it.</li>
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
                shareMembershipManager.shouldSkipHeartbeat() ||
                pollTimer.isExpired()) {
            shareMembershipManager.onHeartbeatRequestSkipped();
            return NetworkClientDelegate.PollResult.EMPTY;
        }
        pollTimer.update(currentTimeMs);
        if (pollTimer.isExpired() && !shareMembershipManager.isLeavingGroup()) {
            logger.warn("Share consumer poll timeout has expired. This means the time between subsequent calls to poll() " +
                    "was longer than the configured max.poll.interval.ms, which typically implies that " +
                    "the poll loop is spending too much time processing messages. You can address this " +
                    "either by increasing max.poll.interval.ms or by reducing the maximum size of batches " +
                    "returned in poll() with max.poll.records.");

            shareMembershipManager.transitionToSendingLeaveGroup(true);
            NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(currentTimeMs, true);

            // We can ignore the leave response because we can join before or after receiving the response.
            heartbeatRequestState.reset();
            heartbeatState.reset();
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
        }

        boolean heartbeatNow = shareMembershipManager.shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight();
        if (!heartbeatRequestState.canSendRequest(currentTimeMs) && !heartbeatNow) {
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.timeToNextHeartbeatMs(currentTimeMs));
        }

        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(currentTimeMs, false);
        return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
    }

    /**
     * Returns the {@link ShareMembershipManager} that this request manager is using to track the state of the group.
     * This is provided so that the {@link ApplicationEventProcessor} can access the state for querying or updating.
     */
    public ShareMembershipManager membershipManager() {
        return shareMembershipManager;
    }

    /**
     * Returns the delay for which the application thread can safely wait before it should be responsive
     * to results from the request managers. For example, the subscription state can change when heartbeats
     * are sent, so blocking for longer than the heartbeat interval might mean the application thread is not
     * responsive to changes.
     *
     * <p>Similarly, we may have to unblock the application thread to send a `PollApplicationEvent` to make sure
     * our poll timer will not expire while we are polling.
     *
     * <p>In the event that heartbeats are currently being skipped, this still returns the next heartbeat
     * delay rather than {@code Long.MAX_VALUE} so that the application thread remains responsive.
     */
    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        pollTimer.update(currentTimeMs);
        if (pollTimer.isExpired() ||
                (shareMembershipManager.shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight())) {
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
        if (pollTimer.isExpired()) {
            logger.debug("Poll timer has been reset after it had expired");
            shareMembershipManager.maybeRejoinStaleMember();
        }
        pollTimer.update(pollMs);
        pollTimer.reset(maxPollIntervalMs);
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final long currentTimeMs,
                                                                     final boolean ignoreResponse) {
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(ignoreResponse);
        heartbeatRequestState.onSendAttempt(currentTimeMs);
        shareMembershipManager.onHeartbeatRequestSent();
        metricsManager.recordHeartbeatSentMs(currentTimeMs);
        heartbeatRequestState.resetTimer();
        return request;
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final boolean ignoreResponse) {
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
                new ShareGroupHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
                coordinatorRequestManager.coordinator());
        if (ignoreResponse)
            return logResponse(request);
        else
            return request.whenComplete((response, exception) -> {
                long completionTimeMs = request.handler().completionTimeMs();
                if (response != null) {
                    metricsManager.recordRequestLatency(response.requestLatencyMs());
                    onResponse((ShareGroupHeartbeatResponse) response.responseBody(), completionTimeMs);
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
                        Errors.forCode(((ShareGroupHeartbeatResponse) response.responseBody()).data().errorCode());
                if (error == Errors.NONE)
                    logger.debug("ShareGroupHeartbeat responded successfully: {}", response);
                else
                    logger.error("ShareGroupHeartbeat failed because of {}: {}", error, response);
            } else {
                logger.error("ShareGroupHeartbeat failed because of unexpected exception.", exception);
            }
        });
    }

    private void onFailure(final Throwable exception, final long responseTimeMs) {
        this.heartbeatRequestState.onFailedAttempt(responseTimeMs);
        this.heartbeatState.reset();
        if (exception instanceof RetriableException) {
            String message = String.format("ShareGroupHeartbeatRequest failed because of retriable exception. " +
                            "Will retry in %s ms: %s",
                    heartbeatRequestState.remainingBackoffMs(responseTimeMs),
                    exception.getMessage());
            logger.debug(message);
        } else {
            logger.error("ShareGroupHeartbeatRequest failed due to fatal error: {}", exception.getMessage());
            handleFatalFailure(exception);
        }
    }

    private void onResponse(final ShareGroupHeartbeatResponse response, long currentTimeMs) {
        if (Errors.forCode(response.data().errorCode()) == Errors.NONE) {
            heartbeatRequestState.updateHeartbeatIntervalMs(response.data().heartbeatIntervalMs());
            heartbeatRequestState.onSuccessfulAttempt(currentTimeMs);
            shareMembershipManager.onHeartbeatSuccess(response.data());
            return;
        }
        onErrorResponse(response, currentTimeMs);
    }

    private void onErrorResponse(final ShareGroupHeartbeatResponse response,
                                 final long currentTimeMs) {
        Errors error = Errors.forCode(response.data().errorCode());
        String errorMessage = response.data().errorMessage();
        String message;

        this.heartbeatState.reset();
        this.heartbeatRequestState.onFailedAttempt(currentTimeMs);

        switch (error) {
            case NOT_COORDINATOR:
                // the manager should retry immediately when the coordinator node becomes available again
                message = String.format("ShareGroupHeartbeatRequest failed because the group coordinator %s is incorrect. " +
                                "Will attempt to find the coordinator again and retry",
                        coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next heartbeat is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_NOT_AVAILABLE:
                message = String.format("ShareGroupHeartbeatRequest failed because the group coordinator %s is not available. " +
                                "Will attempt to find the coordinator again and retry",
                        coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next heartbeat is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // the manager will backoff and retry
                message = String.format("ShareGroupHeartbeatRequest failed because the group coordinator %s is still loading." +
                                "Will retry",
                        coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                break;

            case GROUP_AUTHORIZATION_FAILED:
                GroupAuthorizationException exception =
                        GroupAuthorizationException.forGroupId(shareMembershipManager.groupId());
                logger.error("ShareGroupHeartbeatRequest failed due to group authorization failure: {}", exception.getMessage());
                handleFatalFailure(error.exception(exception.getMessage()));
                break;

            case INVALID_REQUEST:
            case GROUP_MAX_SIZE_REACHED:
            case UNSUPPORTED_VERSION:
                logger.error("ShareGroupHeartbeatRequest failed due to {}: {}", error, errorMessage);
                handleFatalFailure(error.exception(errorMessage));
                break;

            case UNKNOWN_MEMBER_ID:
                message = String.format("ShareGroupHeartbeatRequest failed because member %s is unknown.",
                        shareMembershipManager.memberId());
                logInfo(message, response, currentTimeMs);
                shareMembershipManager.transitionToFenced();
                // Skip backoff so that the next heartbeat is sent soon
                heartbeatRequestState.reset();
                break;

            default:
                // If the manager receives an unknown error - there could be a bug in the code or a new error code
                logger.error("ShareGroupHeartbeatRequest failed due to unexpected error {}: {}", error, errorMessage);
                handleFatalFailure(error.exception(errorMessage));
                break;
        }
    }

    private void logInfo(final String message,
                         final ShareGroupHeartbeatResponse response,
                         final long currentTimeMs) {
        logger.info("{} in {}ms: {}",
                message,
                heartbeatRequestState.remainingBackoffMs(currentTimeMs),
                response.data().errorMessage());
    }

    private void handleFatalFailure(Throwable error) {
        backgroundEventHandler.add(new ErrorEvent(error));
        shareMembershipManager.transitionToFatal();
    }

    /**
     * Represents the state of a heartbeat request, including logic for timing, retries, and exponential backoff. The
     * object extends {@link RequestState} to enable exponential backoff and duplicated request handling.
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

        @Override
        public String toStringBase() {
            return super.toStringBase() +
                    ", heartbeatTimer=" + heartbeatTimer +
                    ", heartbeatIntervalMs=" + heartbeatIntervalMs;
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
        private final ShareMembershipManager shareMembershipManager;
        private final SentFields sentFields;

        public HeartbeatState(
                final SubscriptionState subscriptions,
                final ShareMembershipManager shareMembershipManager) {
            this.subscriptions = subscriptions;
            this.shareMembershipManager = shareMembershipManager;
            this.sentFields = new SentFields();
        }

        public void reset() {
            sentFields.reset();
        }

        public ShareGroupHeartbeatRequestData buildRequestData() {
            ShareGroupHeartbeatRequestData data = new ShareGroupHeartbeatRequestData();

            // GroupId - always sent
            data.setGroupId(shareMembershipManager.groupId());

            // MemberId - always sent, empty until it has been received from the coordinator
            data.setMemberId(shareMembershipManager.memberId());

            // MemberEpoch - always sent
            data.setMemberEpoch(shareMembershipManager.memberEpoch());

            // RackId - only sent the first time, because it does not change
            if (sentFields.rackId == null) {
                data.setRackId(shareMembershipManager.rackId());
                sentFields.rackId = shareMembershipManager.rackId();
            }

            boolean sendAllFields = shareMembershipManager.state() == MemberState.JOINING;

            // SubscribedTopicNames - only sent when joining or if it has changed since the last heartbeat
            TreeSet<String> subscribedTopicNames = new TreeSet<>(this.subscriptions.subscription());
            if (sendAllFields || !subscribedTopicNames.equals(sentFields.subscribedTopicNames)) {
                data.setSubscribedTopicNames(new ArrayList<>(this.subscriptions.subscription()));
                sentFields.subscribedTopicNames = subscribedTopicNames;
            }

            return data;
        }

        // Fields of ShareGroupHeartbeatRequest sent in the most recent request
        static class SentFields {
            private String rackId = null;
            private TreeSet<String> subscribedTopicNames = null;

            SentFields() {}

            void reset() {
                rackId = null;
                subscribedTopicNames = null;
            }
        }
    }
}
