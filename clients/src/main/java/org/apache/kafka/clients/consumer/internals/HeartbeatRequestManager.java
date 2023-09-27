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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>Manages the request creation and response handling for the heartbeat. The module creates a
 * {@link ConsumerGroupHeartbeatRequest} using the state stored in the {@link MembershipManager} and enqueue it to
 * the network queue to be sent out. Once the response is received, the module will update the state in the
 * {@link MembershipManager} and handle any errors.</p>
 *
 * <p>The manager only emits heartbeat when the member is in a group, tries to join or rejoin a group.
 * If the member does not have groupId configured, got kicked out of the group, or encountering fatal exceptions, the
 * heartbeat will not be sent.</p>
 *
 * <p>If the coordinator not is not found, we will skip sending the heartbeat and try to find a coordinator first.</p>
 *
 * <p>If the heartbeat failed due to retriable errors, such as, TimeoutException. The subsequent attempt will be
 * backoff exponentially.</p>
 *
 * <p>If the member completes the assignment changes, i.e. revocation and assignment, a heartbeat request will be
 * sent in the next event loop.</p>
 *
 * <p>See {@link HeartbeatRequestState} for more details.</p>
 */
public class HeartbeatRequestManager implements RequestManager {
    private final Logger logger;
    private final Set<Errors> fatalErrors = new HashSet<>(Arrays.asList(
        Errors.GROUP_AUTHORIZATION_FAILED,
        Errors.INVALID_REQUEST,
        Errors.GROUP_MAX_SIZE_REACHED,
        Errors.UNSUPPORTED_ASSIGNOR,
        Errors.UNRELEASED_INSTANCE_ID));

    private final int rebalanceTimeoutMs;

    private final CoordinatorRequestManager coordinatorRequestManager;
    private final SubscriptionState subscriptions;
    private final HeartbeatRequestState heartbeatRequestState;
    private final MembershipManager membershipManager;
    private final ErrorEventHandler nonRetriableErrorHandler;

    public HeartbeatRequestManager(
        final Time time,
        final LogContext logContext,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final SubscriptionState subscriptions,
        final MembershipManager membershipManager,
        final ErrorEventHandler nonRetriableErrorHandler) {
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.logger = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.membershipManager = membershipManager;
        this.nonRetriableErrorHandler = nonRetriableErrorHandler;
        this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.heartbeatRequestState = new HeartbeatRequestState(logContext, time, 0, retryBackoffMs,
            retryBackoffMaxMs, rebalanceTimeoutMs);
    }

    // Visible for testing
    HeartbeatRequestManager(
        final LogContext logContext,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final SubscriptionState subscriptions,
        final MembershipManager membershipManager,
        final HeartbeatRequestState heartbeatRequestState,
        final ErrorEventHandler nonRetriableErrorHandler) {
        this.logger = logContext.logger(this.getClass());
        this.subscriptions = subscriptions;
        this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.heartbeatRequestState = heartbeatRequestState;
        this.membershipManager = membershipManager;
        this.nonRetriableErrorHandler = nonRetriableErrorHandler;
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
        if (!coordinatorRequestManager.coordinator().isPresent() || !membershipManager.shouldSendHeartbeat()) {
            return new NetworkClientDelegate.PollResult(
                Long.MAX_VALUE, Collections.emptyList());
        }

        // TODO: We will need to send a heartbeat response after partitions being revoke. This needs to be
        //  implemented either with or after the partition reconciliation logic.
        if (!heartbeatRequestState.canSendRequest(currentTimeMs)) {
            return new NetworkClientDelegate.PollResult(
                heartbeatRequestState.nextHeartbeatMs(currentTimeMs),
                Collections.emptyList());
        }
        this.heartbeatRequestState.onSendAttempt(currentTimeMs);
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest();
        return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest() {
        ConsumerGroupHeartbeatRequestData data = new ConsumerGroupHeartbeatRequestData()
            .setGroupId(membershipManager.groupId())
            .setMemberEpoch(membershipManager.memberEpoch())
            .setMemberId(membershipManager.memberId())
            .setRebalanceTimeoutMs(rebalanceTimeoutMs);

        membershipManager.groupInstanceId().ifPresent(data::setInstanceId);

        if (this.subscriptions.hasPatternSubscription()) {
            // We haven't discsussed how Regex is stored in the consumer. We could do it in the subscriptionState
            // , in the memberStateManager, or here.
            // data.setSubscribedTopicRegex(regex)
        } else {
            data.setSubscribedTopicNames(new ArrayList<>(this.subscriptions.subscription()));
        }

        this.membershipManager.assignorSelection().serverAssignor().ifPresent(data::setServerAssignor);

        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            new ConsumerGroupHeartbeatRequest.Builder(data),
            coordinatorRequestManager.coordinator());

        request.future().whenComplete((response, exception) -> {
            if (exception == null) {
                onResponse((ConsumerGroupHeartbeatResponse) response.responseBody(), response.receivedTimeMs());
            } else {
                onFailure(exception, response.receivedTimeMs());
            }
        });
        return request;
    }

    private void onFailure(final Throwable exception, final long responseTimeMs) {
        this.heartbeatRequestState.onFailedAttempt(responseTimeMs);
        logger.warn("Failed to send heartbeat to coordinator node {} due to error: {}",
                coordinatorRequestManager.coordinator(), exception.getMessage());
    }

    private void onResponse(final ConsumerGroupHeartbeatResponse response, long currentTimeMs) {
        if (Errors.forCode(response.data().errorCode()) == Errors.NONE) {
            this.heartbeatRequestState.updateHeartbeatIntervalMs(response.data().heartbeatIntervalMs());
            this.heartbeatRequestState.onSuccessfulAttempt(currentTimeMs);
            this.heartbeatRequestState.resetTimer();
            try {
                membershipManager.updateState(response.data());
            } catch (KafkaException e) {
                logger.error("Received unexpected error in heartbeat response: {}", e.getMessage());
            }
            return;
        }

        onErrorResponse(response, currentTimeMs);
    }

    private void onErrorResponse(final ConsumerGroupHeartbeatResponse response,
                                 final long currentTimeMs) {
        this.heartbeatRequestState.onFailedAttempt(currentTimeMs);
        Errors error = Errors.forCode(response.data().errorCode());
        if (error == Errors.NOT_COORDINATOR || error == Errors.COORDINATOR_NOT_AVAILABLE) {
            String errorMessage = String.format("Coordinator node {} is either not started or not valid. Retrying",
                    coordinatorRequestManager.coordinator());
            logInfo(errorMessage, response, currentTimeMs);
            coordinatorRequestManager.markCoordinatorUnknown(response.data().errorMessage(), currentTimeMs);
        } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
            // retry
            String errorMessage = String.format("Heartbeat was not successful because the coordinator node {} is loading. Retrying",
                    coordinatorRequestManager.coordinator());
            logInfo(errorMessage, response, currentTimeMs);
        } else {
            onFatalErrorResponse(response);
        }
    }

    private void onFatalErrorResponse(final ConsumerGroupHeartbeatResponse response) {
        final Errors responseError = Errors.forCode(response.data().errorCode());
        maybeTransitionToFailureState(responseError);

        if (responseError == Errors.GROUP_AUTHORIZATION_FAILED) {
            GroupAuthorizationException error = GroupAuthorizationException.forGroupId(membershipManager.groupId());
            logger.error("GroupHeartbeatRequest failed due to group authorization failure: {}", error.getMessage());
            nonRetriableErrorHandler.handle(error);
        } else if (responseError == Errors.INVALID_REQUEST) {
            logger.error("GroupHeartbeatRequest failed due to invalid request error: {}",
                response.data().errorMessage());
            nonRetriableErrorHandler.handle(Errors.INVALID_REQUEST.exception());
        } else if (responseError == Errors.GROUP_MAX_SIZE_REACHED) {
            logger.error("GroupHeartbeatRequest failed due to the max group size limit: {}",
                response.data().errorMessage());
            nonRetriableErrorHandler.handle(responseError.exception());
        } else if (responseError == Errors.UNSUPPORTED_ASSIGNOR) {
            logger.error("GroupHeartbeatRequest failed due to unsupported assignor {}: {}",
                membershipManager.assignorSelection(), response.data().errorMessage());
            nonRetriableErrorHandler.handle(Errors.UNSUPPORTED_ASSIGNOR.exception());
        } else if (responseError == Errors.UNRELEASED_INSTANCE_ID) {
            logger.error("GroupHeartbeatRequest failed due to the instance id {} was not released: {}",
                membershipManager.groupInstanceId().orElse("null"),
                response.data().errorMessage());
            nonRetriableErrorHandler.handle(Errors.UNRELEASED_INSTANCE_ID.exception());
        } else if (responseError == Errors.FENCED_MEMBER_EPOCH ||
                responseError == Errors.UNKNOWN_MEMBER_ID) {
            membershipManager.fenceMember();
        } else {
            // If the manager receives an unknown error - there could be a bug in the code or a new error code
            logger.error("GroupHeartbeatRequest failed due to unexpected error: {}", responseError);
            membershipManager.transitionToFailure();
        }
    }

    private void maybeTransitionToFailureState(Errors error) {
        if (fatalErrors.contains(error)) {
            membershipManager.transitionToFailure();
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

    static class HeartbeatRequestState extends RequestState {
        long heartbeatIntervalMs;
        final Timer heartbeatTimer;

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
            long heartbeatIntervalMs,
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
