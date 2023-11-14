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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;

/**
 * <p>Manages the request creation and response handling for the heartbeat. The module creates a
 * {@link ConsumerGroupHeartbeatRequest} using the state stored in the {@link MembershipManager} and enqueue it to
 * the network queue to be sent out. Once the response is received, the module will update the state in the
 * {@link MembershipManager} and handle any errors.</p>
 *
 * <p>The manager will try to send a heartbeat when the member is in {@link MemberState#STABLE},
 * {@link MemberState#UNJOINED}, or {@link MemberState#RECONCILING}. Which mean the member is either in a stable
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
    private final Time time;

    /**
     * Time that the group coordinator will wait on member to revoke its partitions. This is provided by the group
     * coordinator in the heartbeat
     */
    private final int rebalanceTimeoutMs;

    /**
     * CoordinatorRequestManager manages the connection to the group coordinator
     */
    private final CoordinatorRequestManager coordinatorRequestManager;

    /**
     * SubscriptionState tracks the topic, partition and offset of the member
     */
    private final SubscriptionState subscriptions;

    /**
     * HeartbeatRequestState manages heartbeat request timing and retries
     */
    private final HeartbeatRequestState heartbeatRequestState;

    /**
     * MembershipManager manages member's essential attributes like epoch and id, and its rebalance state
     */
    private final MembershipManager membershipManager;

    /**
     * ErrorEventHandler allows the background thread to propagate errors back to the user
     */
    private final BackgroundEventHandler backgroundEventHandler;

    public HeartbeatRequestManager(
        final LogContext logContext,
        final Time time,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final SubscriptionState subscriptions,
        final MembershipManager membershipManager,
        final BackgroundEventHandler backgroundEventHandler) {
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.time = time;
        this.logger = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.membershipManager = membershipManager;
        this.backgroundEventHandler = backgroundEventHandler;
        this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.heartbeatRequestState = new HeartbeatRequestState(logContext, time, 0, retryBackoffMs,
            retryBackoffMaxMs, rebalanceTimeoutMs);
    }

    // Visible for testing
    HeartbeatRequestManager(
        final LogContext logContext,
        final Time time,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final SubscriptionState subscriptions,
        final MembershipManager membershipManager,
        final HeartbeatRequestState heartbeatRequestState,
        final BackgroundEventHandler backgroundEventHandler) {
        this.logger = logContext.logger(this.getClass());
        this.time = time;
        this.subscriptions = subscriptions;
        this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.heartbeatRequestState = heartbeatRequestState;
        this.membershipManager = membershipManager;
        this.backgroundEventHandler = backgroundEventHandler;
    }

    /**
     * Determines the maximum wait time until the next poll based on the member's state, and creates a heartbeat
     * request.
     * <ol>
     *     <li>If the member is without a coordinator or is in a failed state, the timer is set to Long.MAX_VALUE, as there's no need to send a heartbeat.</li>
     *     <li>If the member cannot send a heartbeat due to either exponential backoff, it will return the remaining time left on the backoff timer.</li>
     *     <li>If the member's heartbeat timer has not expired, It will return the remaining time left on the
     *     heartbeat timer.</li>
     *     <li>If the member can send a heartbeat, the timer is set to the current heartbeat interval.</li>
     * </ol>
     */
    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        if (!coordinatorRequestManager.coordinator().isPresent() || !membershipManager.shouldSendHeartbeat())
            return NetworkClientDelegate.PollResult.EMPTY;

        // TODO: We will need to send a heartbeat response after partitions being revoke. This needs to be
        //  implemented either with or after the partition reconciliation logic.
        if (!heartbeatRequestState.canSendRequest(currentTimeMs))
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.nextHeartbeatMs(currentTimeMs));

        this.heartbeatRequestState.onSendAttempt(currentTimeMs);
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest();
        return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest() {
        // TODO: We only need to send the rebalanceTimeoutMs field once unless the first request failed.
        ConsumerGroupHeartbeatRequestData data = new ConsumerGroupHeartbeatRequestData()
            .setGroupId(membershipManager.groupId())
            .setMemberEpoch(membershipManager.memberEpoch())
            .setMemberId(membershipManager.memberId())
            .setRebalanceTimeoutMs(rebalanceTimeoutMs);

        membershipManager.groupInstanceId().ifPresent(data::setInstanceId);

        if (this.subscriptions.hasPatternSubscription()) {
            // TODO: Pass the string to the GC if server side regex is used.
        } else {
            data.setSubscribedTopicNames(new ArrayList<>(this.subscriptions.subscription()));
        }

        this.membershipManager.serverAssignor().ifPresent(data::setServerAssignor);

        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            new ConsumerGroupHeartbeatRequest.Builder(data),
            coordinatorRequestManager.coordinator());
        return request.whenComplete((response, exception) -> {
            if (response != null) {
                onResponse((ConsumerGroupHeartbeatResponse) response.responseBody(), request.handler().completionTimeMs());
            } else {
                onFailure(exception, request.handler().completionTimeMs());
            }
        });
    }

    private void onFailure(final Throwable exception, final long responseTimeMs) {
        this.heartbeatRequestState.onFailedAttempt(responseTimeMs);
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
            this.heartbeatRequestState.updateHeartbeatIntervalMs(response.data().heartbeatIntervalMs());
            this.heartbeatRequestState.onSuccessfulAttempt(currentTimeMs);
            this.heartbeatRequestState.resetTimer();
            this.membershipManager.updateState(response.data());
            return;
        }
        onErrorResponse(response, currentTimeMs);
    }

    private void onErrorResponse(final ConsumerGroupHeartbeatResponse response,
                                 final long currentTimeMs) {
        Errors error = Errors.forCode(response.data().errorCode());
        String errorMessage = response.data().errorMessage();
        String message;
        // TODO: upon encountering a fatal/fenced error, trigger onPartitionLost logic to give up the current
        //  assignments.
        switch (error) {
            case NOT_COORDINATOR:
                // the manager should retry immediately when the coordinator node becomes available again
                message = String.format("GroupHeartbeatRequest failed because the group coordinator %s is incorrect. " +
                                "Will attempt to find the coordinator again and retry",
                        coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                break;

            case COORDINATOR_NOT_AVAILABLE:
                message = String.format("GroupHeartbeatRequest failed because the group coordinator %s is not available. " +
                                "Will attempt to find the coordinator again and retry",
                        coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // the manager will backoff and retry
                message = String.format("GroupHeartbeatRequest failed because the group coordinator %s is still loading." +
                                "Will retry",
                        coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                heartbeatRequestState.onFailedAttempt(currentTimeMs);
                break;

            case GROUP_AUTHORIZATION_FAILED:
                GroupAuthorizationException exception =
                        GroupAuthorizationException.forGroupId(membershipManager.groupId());
                logger.error("GroupHeartbeatRequest failed due to group authorization failure: {}", exception.getMessage());
                handleFatalFailure(error.exception(exception.getMessage()));
                break;

            case UNRELEASED_INSTANCE_ID:
                logger.error("GroupHeartbeatRequest failed due to the instance id {} was not released: {}",
                        membershipManager.groupInstanceId().orElse("null"), errorMessage);
                handleFatalFailure(Errors.UNRELEASED_INSTANCE_ID.exception(errorMessage));
                break;

            case INVALID_REQUEST:
            case GROUP_MAX_SIZE_REACHED:
            case UNSUPPORTED_ASSIGNOR:
            case UNSUPPORTED_VERSION:
                logger.error("GroupHeartbeatRequest failed due to error: {}", error);
                handleFatalFailure(error.exception(errorMessage));
                break;

            case FENCED_MEMBER_EPOCH:
                message = String.format("GroupHeartbeatRequest failed because member ID %s with epoch %s is invalid. " +
                                "Will abandon all partitions and rejoin the group",
                        membershipManager.memberId(), membershipManager.memberEpoch());
                logInfo(message, response, currentTimeMs);
                membershipManager.transitionToFenced();
                break;

            case UNKNOWN_MEMBER_ID:
                message = String.format("GroupHeartbeatRequest failed because member of unknown ID %s with epoch %s is invalid. " +
                                "Will abandon all partitions and rejoin the group",
                        membershipManager.memberId(), membershipManager.memberEpoch());
                logInfo(message, response, currentTimeMs);
                membershipManager.transitionToFenced();
                break;

            default:
                // If the manager receives an unknown error - there could be a bug in the code or a new error code
                logger.error("GroupHeartbeatRequest failed due to unexpected error: {}", error);
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
        backgroundEventHandler.add(new ErrorBackgroundEvent(error));
        membershipManager.transitionToFailed();
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
            final long retryBackoffMaxMs) {
            super(logContext, HeartbeatRequestState.class.getName(), retryBackoffMs, retryBackoffMaxMs);
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.heartbeatTimer = time.timer(heartbeatIntervalMs);
        }

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
        public boolean canSendRequest(final long currentTimeMs) {
            update(currentTimeMs);
            return heartbeatTimer.isExpired() && super.canSendRequest(currentTimeMs);
        }

        public long nextHeartbeatMs(final long currentTimeMs) {
            if (heartbeatTimer.remainingMs() == 0) {
                return this.remainingBackoffMs(currentTimeMs);
            }
            return heartbeatTimer.remainingMs();
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
