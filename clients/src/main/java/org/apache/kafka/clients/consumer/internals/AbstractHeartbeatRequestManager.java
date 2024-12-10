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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.util.Collections;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * <p>Manages the request creation and response handling for the heartbeat. The module creates a
 * heartbeat request using the state stored in the membership manager and enqueues it to
 * the network queue to be sent out. Once the response is received, it updates the state in the
 * membership manager and handles any errors.
 *
 * <p>The heartbeat manager generates heartbeat requests based on the member state. It's also responsible
 * for the timing of the heartbeat requests to ensure they are sent according to the heartbeat interval
 * (while the member state is stable) or on demand (while the member is acknowledging an assignment or
 * leaving the group).
 *
 * <p>If the member got kicked out of a group, it will try to give up the current assignment by invoking {@code
 * OnPartitionsLost} before attempting to join again with a zero epoch.
 *
 * <p>If the coordinator not is not found, we will skip sending the heartbeat and try to find a coordinator first.
 *
 * <p>When the member completes the assignment reconciliation, the {@link HeartbeatRequestState} will be reset so
 * that a heartbeat will be sent in the next event loop.
 *
 * <p>The class variable R is the response for the specific group's heartbeat RPC.
 */
public abstract class AbstractHeartbeatRequestManager<R extends AbstractResponse> implements RequestManager {

    protected final Logger logger;

    /**
     * Time that the group coordinator will wait on member to revoke its partitions. This is provided by the group
     * coordinator in the heartbeat
     */
    protected final int maxPollIntervalMs;

    /**
     * CoordinatorRequestManager manages the connection to the group coordinator
     */
    protected final CoordinatorRequestManager coordinatorRequestManager;

    /**
     * HeartbeatRequestState manages heartbeat request timing and retries
     */
    private final HeartbeatRequestState heartbeatRequestState;

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

    public static final String CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG = "The cluster does not support the new CONSUMER " +
        "group protocol. Set group.protocol=classic on the consumer configs to revert to the CLASSIC protocol " +
        "until the cluster is upgraded.";

    AbstractHeartbeatRequestManager(
            final LogContext logContext,
            final Time time,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final BackgroundEventHandler backgroundEventHandler,
            final HeartbeatMetricsManager metricsManager) {
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.logger = logContext.logger(getClass());
        this.backgroundEventHandler = backgroundEventHandler;
        this.maxPollIntervalMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.heartbeatRequestState = new HeartbeatRequestState(logContext, time, 0, retryBackoffMs,
                retryBackoffMaxMs, maxPollIntervalMs);
        this.pollTimer = time.timer(maxPollIntervalMs);
        this.metricsManager = metricsManager;
    }

    AbstractHeartbeatRequestManager(
            final LogContext logContext,
            final Timer timer,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final HeartbeatRequestState heartbeatRequestState,
            final BackgroundEventHandler backgroundEventHandler,
            final HeartbeatMetricsManager metricsManager) {
        this.logger = logContext.logger(this.getClass());
        this.maxPollIntervalMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.heartbeatRequestState = heartbeatRequestState;
        this.backgroundEventHandler = backgroundEventHandler;
        this.pollTimer = timer;
        this.metricsManager = metricsManager;
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
        if (coordinatorRequestManager.coordinator().isEmpty() || membershipManager().shouldSkipHeartbeat()) {
            membershipManager().onHeartbeatRequestSkipped();
            return NetworkClientDelegate.PollResult.EMPTY;
        }
        pollTimer.update(currentTimeMs);
        if (pollTimer.isExpired() && !membershipManager().isLeavingGroup()) {
            logger.warn("Consumer poll timeout has expired. This means the time between " +
                "subsequent calls to poll() was longer than the configured max.poll.interval.ms, " +
                "which typically implies that the poll loop is spending too much time processing " +
                "messages. You can address this either by increasing max.poll.interval.ms or by " +
                "reducing the maximum size of batches returned in poll() with max.poll.records.");

            membershipManager().transitionToSendingLeaveGroup(true);
            NetworkClientDelegate.UnsentRequest leaveHeartbeat = makeHeartbeatRequest(currentTimeMs, true);

            // We can ignore the leave response because we can join before or after receiving the response.
            heartbeatRequestState.reset();
            resetHeartbeatState();
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(leaveHeartbeat));
        }

        // Case 1: The member is leaving
        boolean heartbeatNow = membershipManager().state() == MemberState.LEAVING ||
            // Case 2: The member state indicates it should send a heartbeat without waiting for the interval,
            // and there is no heartbeat request currently in-flight
            (membershipManager().shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight());

        if (!heartbeatRequestState.canSendRequest(currentTimeMs) && !heartbeatNow) {
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.timeToNextHeartbeatMs(currentTimeMs));
        }

        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(currentTimeMs, false);
        return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
    }

    /**
     * Returns the {@link AbstractMembershipManager} that this request manager is using to track the state of the group.
     * This is provided so that the {@link ApplicationEventProcessor} can access the state for querying or updating.
     */
    public abstract AbstractMembershipManager<R> membershipManager();

    /**
     * Generate a heartbeat request to leave the group if the state is still LEAVING when this is
     * called to close the consumer.
     * <p/>
     * Note that when closing the consumer, even though an event to Unsubscribe is generated
     * (triggers callbacks and sends leave group), it could be the case that the Unsubscribe event
     * processing does not complete in time and moves on to close the managers (ex. calls to
     * close with zero timeout). So we could end up on this pollOnClose with the member in
     * {@link MemberState#PREPARE_LEAVING} (ex. app thread did not have the time to process the
     * event to execute callbacks), or {@link MemberState#LEAVING} (ex. the leave request could
     * not be sent due to coordinator not available at that time). In all cases, the pollOnClose
     * will be triggered right before sending the final requests, so we ensure that we generate
     * the request to leave if needed.
     *
     * @param currentTimeMs The current system time in milliseconds at which the method was called
     * @return PollResult containing the request to send
     */
    @Override
    public PollResult pollOnClose(long currentTimeMs) {
        if (membershipManager().isLeavingGroup()) {
            NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(currentTimeMs, true);
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
        }
        return EMPTY;
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
        if (pollTimer.isExpired() || (membershipManager().shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight())) {
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
                "max.poll.interval.ms, exceeded approximately by {} ms. Member {} will rejoin the group now.",
                pollTimer.isExpiredBy(), membershipManager().memberId());
            membershipManager().maybeRejoinStaleMember();
        }
        pollTimer.reset(maxPollIntervalMs);
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final long currentTimeMs, final boolean ignoreResponse) {
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(ignoreResponse);
        heartbeatRequestState.onSendAttempt(currentTimeMs);
        membershipManager().onHeartbeatRequestGenerated();
        metricsManager.recordHeartbeatSentMs(currentTimeMs);
        heartbeatRequestState.resetTimer();
        return request;
    }

    @SuppressWarnings("unchecked")
    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final boolean ignoreResponse) {
        NetworkClientDelegate.UnsentRequest request = buildHeartbeatRequest();
        if (ignoreResponse)
            return logResponse(request);
        else
            return request.whenComplete((response, exception) -> {
                long completionTimeMs = request.handler().completionTimeMs();
                if (response != null) {
                    metricsManager.recordRequestLatency(response.requestLatencyMs());
                    onResponse((R) response.responseBody(), completionTimeMs);
                } else {
                    onFailure(exception, completionTimeMs);
                }
            });
    }

    @SuppressWarnings("unchecked")
    private NetworkClientDelegate.UnsentRequest logResponse(final NetworkClientDelegate.UnsentRequest request) {
        return request.whenComplete((response, exception) -> {
            if (response != null) {
                metricsManager.recordRequestLatency(response.requestLatencyMs());
                Errors error = errorForResponse((R) response.responseBody());
                if (error == Errors.NONE)
                    logger.debug("{} responded successfully: {}", heartbeatRequestName(), response);
                else
                    logger.error("{} failed because of {}: {}", heartbeatRequestName(), error, response);
            } else {
                logger.error("{} failed because of unexpected exception.", heartbeatRequestName(), exception);
            }
        });
    }

    private void onFailure(final Throwable exception, final long responseTimeMs) {
        this.heartbeatRequestState.onFailedAttempt(responseTimeMs);
        resetHeartbeatState();
        if (exception instanceof RetriableException) {
            coordinatorRequestManager.handleCoordinatorDisconnect(exception, responseTimeMs);
            String message = String.format("%s failed because of the retriable exception. Will retry in %s ms: %s",
                heartbeatRequestName(),
                heartbeatRequestState.remainingBackoffMs(responseTimeMs),
                exception.getMessage());
            logger.debug(message);
        } else {
            logger.error("{} failed due to fatal error: {}", heartbeatRequestName(), exception.getMessage());
            if (isHBApiUnsupportedErrorMsg(exception)) {
                // This is expected to be the case where building the request fails because the node does not support
                // the API. Propagate custom message.
                handleFatalFailure(new UnsupportedVersionException(CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG, exception));
            } else {
                // This is the case where building the request fails even though the node supports the API (ex.
                // required version 1 not available when regex in use).
                handleFatalFailure(exception);
            }
        }
        // Notify the group manager about the failure after all errors have been handled and propagated.
        membershipManager().onHeartbeatFailure(exception instanceof RetriableException);
    }

    /***
     * @return True if the exception is the UnsupportedVersion generated on the client, before sending the request,
     * when checking if the API is available on the broker.
     */
    private boolean isHBApiUnsupportedErrorMsg(Throwable exception) {
        return exception instanceof UnsupportedVersionException &&
            exception.getMessage().equals("The node does not support " + ApiKeys.CONSUMER_GROUP_HEARTBEAT);
    }

    private void onResponse(final R response, final long currentTimeMs) {
        if (errorForResponse(response) == Errors.NONE) {
            heartbeatRequestState.updateHeartbeatIntervalMs(heartbeatIntervalForResponse(response));
            heartbeatRequestState.onSuccessfulAttempt(currentTimeMs);
            membershipManager().onHeartbeatSuccess(response);
            return;
        }
        onErrorResponse(response, currentTimeMs);
    }

    private void onErrorResponse(final R response, final long currentTimeMs) {
        Errors error = errorForResponse(response);
        String errorMessage = errorMessageForResponse(response);
        String message;

        resetHeartbeatState();
        this.heartbeatRequestState.onFailedAttempt(currentTimeMs);

        switch (error) {
            case NOT_COORDINATOR:
                // the manager should retry immediately when the coordinator node becomes available again
                message = String.format("%s failed because the group coordinator %s is incorrect. " +
                                "Will attempt to find the coordinator again and retry",
                        heartbeatRequestName(), coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next HB is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_NOT_AVAILABLE:
                message = String.format("%s failed because the group coordinator %s is not available. " +
                                "Will attempt to find the coordinator again and retry",
                        heartbeatRequestName(), coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next HB is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // the manager will backoff and retry
                message = String.format("%s failed because the group coordinator %s is still loading. Will retry",
                        heartbeatRequestName(), coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                break;

            case GROUP_AUTHORIZATION_FAILED:
                GroupAuthorizationException exception =
                        GroupAuthorizationException.forGroupId(membershipManager().groupId());
                logger.error("{} failed due to group authorization failure: {}",
                        heartbeatRequestName(), exception.getMessage());
                handleFatalFailure(error.exception(exception.getMessage()));
                break;

            case INVALID_REQUEST:
            case GROUP_MAX_SIZE_REACHED:
            case UNSUPPORTED_ASSIGNOR:
                logger.error("{} failed due to {}: {}", heartbeatRequestName(), error, errorMessage);
                handleFatalFailure(error.exception(errorMessage));
                break;

            case FENCED_MEMBER_EPOCH:
                message = String.format("%s failed for member %s because epoch %s is fenced.",
                        heartbeatRequestName(), membershipManager().memberId(), membershipManager().memberEpoch());
                logInfo(message, response, currentTimeMs);
                membershipManager().transitionToFenced();
                // Skip backoff so that a next HB to rejoin is sent as soon as the fenced member releases its assignment
                heartbeatRequestState.reset();
                break;

            case UNKNOWN_MEMBER_ID:
                message = String.format("%s failed because member %s is unknown.",
                        heartbeatRequestName(), membershipManager().memberId());
                logInfo(message, response, currentTimeMs);
                membershipManager().transitionToFenced();
                // Skip backoff so that a next HB to rejoin is sent as soon as the fenced member releases its assignment
                heartbeatRequestState.reset();
                break;

            case INVALID_REGULAR_EXPRESSION:
                logger.error("{} failed due to {}: {}", heartbeatRequestName(), error, errorMessage);
                handleFatalFailure(error.exception("Invalid RE2J SubscriptionPattern provided in the call to " +
                    "subscribe. " + errorMessage));
                break;

            default:
                if (!handleSpecificError(response, currentTimeMs)) {
                    // If the manager receives an unknown error - there could be a bug in the code or a new error code
                    logger.error("{} failed due to unexpected error {}: {}", heartbeatRequestName(), error, errorMessage);
                    handleFatalFailure(error.exception(errorMessage));
                }
                break;
        }

        // Notify the group manager about the failure after all errors have been handled and propagated.
        membershipManager().onHeartbeatFailure(false);
    }

    protected void logInfo(final String message, final R response, final long currentTimeMs) {
        logger.info("{} in {}ms: {}",
            message,
            heartbeatRequestState.remainingBackoffMs(currentTimeMs),
            errorMessageForResponse(response));
    }

    protected void handleFatalFailure(Throwable error) {
        backgroundEventHandler.add(new ErrorEvent(error));
        membershipManager().transitionToFatal();
    }


    /**
     * Error handling specific to a group type.
     *
     * @param response The heartbeat response
     * @param currentTimeMs Current time
     * @return true if the error was handled, else false
     */
    public boolean handleSpecificError(final R response, final long currentTimeMs) {
        return false;
    }

    /**
     * Resets the heartbeat state.
     */
    public abstract void resetHeartbeatState();

    /**
     * Builds a heartbeat request using the heartbeat state to follow the protocol faithfully.
     *
     * @return The heartbeat request
     */
    public abstract NetworkClientDelegate.UnsentRequest buildHeartbeatRequest();

    /**
     * Returns the heartbeat RPC request name to be used for logging.
     *
     * @return The heartbeat RPC request name
     */
    public abstract String heartbeatRequestName();

    /**
     * Returns the error for the response.
     *
     * @param response The heartbeat response
     * @return The error {@link Errors}
     */
    public abstract Errors errorForResponse(R response);

    /**
     * Returns the error message for the response.
     *
     * @param response The heartbeat response
     * @return The error message
     */
    public abstract String errorMessageForResponse(R response);

    /**
     * Returns the heartbeat interval for the response.
     *
     * @param response The heartbeat response
     * @return The heartbeat interval
     */
    public abstract long heartbeatIntervalForResponse(R response);

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

        HeartbeatRequestState(
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

        void resetTimer() {
            this.heartbeatTimer.reset(heartbeatIntervalMs);
        }

        @Override
        public String toStringBase() {
            return super.toStringBase() +
                ", remainingMs=" + heartbeatTimer.remainingMs() +
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

        long timeToNextHeartbeatMs(final long currentTimeMs) {
            if (heartbeatTimer.isExpired()) {
                return this.remainingBackoffMs(currentTimeMs);
            }
            return heartbeatTimer.remainingMs();
        }

        @Override
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
}
