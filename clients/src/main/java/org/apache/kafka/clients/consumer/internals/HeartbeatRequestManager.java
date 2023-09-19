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
import java.util.Collections;

/**
 * Manages the request creation and response handling for the heartbeat. The module creates a {@link ConsumerGroupHeartbeatRequest}
 * using the state stored in the {@link MembershipManager} and enqueue it to the network queue to be sent out. Once
 * the response is received, the module will update the state in the {@link MembershipManager} and handle any errors.
 *
 * The manager only emits heartbeat when the member is in a group. If the member is not in a group, or the
 * coordinator is lost, the heartbeat won't be sent.
 *
 * If the heartbeat request fails, the module will trigger the exponential backoff, and resend the request. See
 * {@link HeartbeatRequestState} for more details.
 */
public class HeartbeatRequestManager implements RequestManager {
    private final Time time;
    private final Logger logger;

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
        this.time = time;
        this.logger = logContext.logger(HeartbeatRequestManager.class);
        this.subscriptions = subscriptions;
        this.membershipManager = membershipManager;
        this.nonRetriableErrorHandler = nonRetriableErrorHandler;
        this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);

        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.heartbeatRequestState = new HeartbeatRequestState(logContext, time, -1, retryBackoffMs,
            retryBackoffMaxMs, rebalanceTimeoutMs);
    }

    // Visible for testing
    HeartbeatRequestManager(
        final Time time,
        final LogContext logContext,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final SubscriptionState subscriptions,
        final MembershipManager membershipManager,
        final HeartbeatRequestState heartbeatRequestState,
        final ErrorEventHandler nonRetriableErrorHandler) {
        this.time = time;
        this.logger = logContext.logger(this.getClass());
        this.subscriptions = subscriptions;
        this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.heartbeatRequestState = heartbeatRequestState;
        this.membershipManager = membershipManager;
        this.nonRetriableErrorHandler = nonRetriableErrorHandler;
    }

    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        if (!coordinatorRequestManager.coordinator().isPresent() || !membershipManager.shouldSendHeartbeat()) {
            return new NetworkClientDelegate.PollResult(
                Long.MAX_VALUE, Collections.emptyList());
        }

        if (!heartbeatRequestState.canSendRequest(currentTimeMs)) {
            return new NetworkClientDelegate.PollResult(
                heartbeatRequestState.nextHeartbeatMs(currentTimeMs),
                Collections.emptyList());
        }
        this.heartbeatRequestState.onSendAttempt(currentTimeMs);
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest();
        // return Long.MAX_VALUE because we will update the timer when the response is received
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.singletonList(request));
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
        logger.debug("failed sending heartbeat due to {}", exception.getMessage());
    }

    private void onResponse(final ConsumerGroupHeartbeatResponse response, long currentTimeMs) {
        if (response.data().errorCode() == Errors.NONE.code()) {
            this.heartbeatRequestState.updateHeartbeatIntervalMs(response.data().heartbeatIntervalMs());
            this.heartbeatRequestState.onSuccessfulAttempt(currentTimeMs);
            this.heartbeatRequestState.reset();
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
        short errorCode = response.data().errorCode();
        if (errorCode == Errors.NOT_COORDINATOR.code() || errorCode == Errors.COORDINATOR_NOT_AVAILABLE.code()) {
            logInfo("Coordinator is either not started or not valid. Retrying", response, currentTimeMs);
            coordinatorRequestManager.markCoordinatorUnknown(response.data().errorMessage(), currentTimeMs);
        } else if (errorCode == Errors.COORDINATOR_LOAD_IN_PROGRESS.code()) {
            // retry
            logInfo("Coordinator {} is loading. Retrying", response, currentTimeMs);
        } else {
            onFatalErrorResponse(response);
        }
    }

    private void onFatalErrorResponse(final ConsumerGroupHeartbeatResponse response) {
        final short errorCode = response.data().errorCode();
        if (errorCode == Errors.GROUP_AUTHORIZATION_FAILED.code()) {
            GroupAuthorizationException error = GroupAuthorizationException.forGroupId(membershipManager.groupId());
            logger.error("GroupHeartbeatRequest failed due to group authorization failure: {}", error.getMessage());
            nonRetriableErrorHandler.handle(error);
        } else if (errorCode == Errors.INVALID_REQUEST.code()) {
            logger.error("GroupHeartbeatRequest failed due to fatal error: {}", response.data().errorMessage());
            nonRetriableErrorHandler.handle(Errors.INVALID_REQUEST.exception());
        } else if (errorCode == Errors.GROUP_MAX_SIZE_REACHED.code()) {
            logger.error("GroupHeartbeatRequest failed due to the max group size limit: {}",
                response.data().errorMessage());
            nonRetriableErrorHandler.handle(Errors.GROUP_MAX_SIZE_REACHED.exception());
        } else if (errorCode == Errors.UNSUPPORTED_ASSIGNOR.code()) {
            logger.error("GroupHeartbeatRequest failed due to unsupported assignor {}: {}",
                membershipManager.assignorSelection(), response.data().errorMessage());
            nonRetriableErrorHandler.handle(Errors.UNSUPPORTED_ASSIGNOR.exception());
        } else if (errorCode == Errors.UNRELEASED_INSTANCE_ID.code()) {
            logger.error("GroupHeartbeatRequest failed due to the instance id {} was not released: {}",
                membershipManager.groupInstanceId().orElse("null"),
                response.data().errorMessage());
            nonRetriableErrorHandler.handle(Errors.UNRELEASED_INSTANCE_ID.exception());
        }
        membershipManager.onFatalError(response.data().errorCode());
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

        @Override
        public void reset() {
            super.reset();
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
