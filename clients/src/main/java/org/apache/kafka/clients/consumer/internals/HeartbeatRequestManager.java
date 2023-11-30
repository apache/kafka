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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final int rebalanceTimeoutMs;

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

    public HeartbeatRequestManager(
        final LogContext logContext,
        final Time time,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final SubscriptionState subscriptions,
        final MembershipManager membershipManager,
        final BackgroundEventHandler backgroundEventHandler) {
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.logger = logContext.logger(getClass());
        this.membershipManager = membershipManager;
        this.backgroundEventHandler = backgroundEventHandler;
        this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.heartbeatState = new HeartbeatState(subscriptions, membershipManager, rebalanceTimeoutMs);
        this.heartbeatRequestState = new HeartbeatRequestState(logContext, time, 0, retryBackoffMs,
            retryBackoffMaxMs, rebalanceTimeoutMs);
    }

    // Visible for testing
    HeartbeatRequestManager(
        final LogContext logContext,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final MembershipManager membershipManager,
        final HeartbeatState heartbeatState,
        final HeartbeatRequestState heartbeatRequestState,
        final BackgroundEventHandler backgroundEventHandler) {
        this.logger = logContext.logger(this.getClass());
        this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.heartbeatRequestState = heartbeatRequestState;
        this.heartbeatState = heartbeatState;
        this.membershipManager = membershipManager;
        this.backgroundEventHandler = backgroundEventHandler;
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
        if (!coordinatorRequestManager.coordinator().isPresent() || membershipManager.shouldSkipHeartbeat()) {
            membershipManager.onHeartbeatRequestSkipped();
            return NetworkClientDelegate.PollResult.EMPTY;
        }

        boolean heartbeatNow = membershipManager.shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight();

        if (!heartbeatRequestState.canSendRequest(currentTimeMs) && !heartbeatNow) {
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.nextHeartbeatMs(currentTimeMs));
        }

        heartbeatRequestState.onSendAttempt(currentTimeMs);
        membershipManager.onHeartbeatRequestSent();
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest();
        return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
    }

    /**
     * Returns the delay for which the application thread can safely wait before it should be responsive
     * to results from the request managers. For example, the subscription state can change when heartbeats
     * are sent, so blocking for longer than the heartbeat interval might mean the application thread is not
     * responsive to changes.
     *
     * <p>In the event that heartbeats are currently being skipped, this still returns the next heartbeat
     * delay rather than {@code Long.MAX_VALUE} so that the application thread remains responsive.
     */
    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        boolean heartbeatNow = membershipManager.shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight();
        return heartbeatNow ? 0L : heartbeatRequestState.nextHeartbeatMs(currentTimeMs);
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest() {
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            new ConsumerGroupHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
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
            this.heartbeatRequestState.updateHeartbeatIntervalMs(response.data().heartbeatIntervalMs());
            this.heartbeatRequestState.onSuccessfulAttempt(currentTimeMs);
            this.heartbeatRequestState.resetTimer();
            this.membershipManager.onHeartbeatResponseReceived(response.data());
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

            // InstanceId - only sent if has changed since the last heartbeat
            membershipManager.groupInstanceId().ifPresent(groupInstanceId -> {
                if (!groupInstanceId.equals(sentFields.instanceId)) {
                    data.setInstanceId(groupInstanceId);
                    sentFields.instanceId = groupInstanceId;
                }
            });

            // RebalanceTimeoutMs - only sent if has changed since the last heartbeat
            if (sentFields.rebalanceTimeoutMs != rebalanceTimeoutMs) {
                data.setRebalanceTimeoutMs(rebalanceTimeoutMs);
                sentFields.rebalanceTimeoutMs = rebalanceTimeoutMs;
            }

            if (!this.subscriptions.hasPatternSubscription()) {
                // SubscribedTopicNames - only sent if has changed since the last heartbeat
                TreeSet<String> subscribedTopicNames = new TreeSet<>(this.subscriptions.subscription());
                if (!subscribedTopicNames.equals(sentFields.subscribedTopicNames)) {
                    data.setSubscribedTopicNames(new ArrayList<>(this.subscriptions.subscription()));
                    sentFields.subscribedTopicNames = subscribedTopicNames;
                }
            } else {
                // SubscribedTopicRegex - only sent if has changed since the last heartbeat
                //                      - not supported yet
            }

            // ServerAssignor - only sent if has changed since the last heartbeat
            this.membershipManager.serverAssignor().ifPresent(serverAssignor -> {
                if (!serverAssignor.equals(sentFields.serverAssignor)) {
                    data.setServerAssignor(serverAssignor);
                    sentFields.serverAssignor = serverAssignor;
                }
            });

            // ClientAssignors - not supported yet

            // TopicPartitions - only sent if has changed since the last heartbeat
            //   Note that TopicIdPartition.toString is being avoided here so that
            //   the string consists of just the topic ID and the partition. 
            //   When an assignment is received, we might not yet know the topic name
            //   and then it is learnt subsequently by a metadata update.
            TreeSet<String> assignedPartitions = membershipManager.currentAssignment().stream()
                    .map(tp -> tp.topicId() + "-" + tp.partition())
                    .collect(Collectors.toCollection(TreeSet::new));
            if (!assignedPartitions.equals(sentFields.topicPartitions)) {
                List<ConsumerGroupHeartbeatRequestData.TopicPartitions> topicPartitions =
                        buildTopicPartitionsList(membershipManager.currentAssignment());
                data.setTopicPartitions(topicPartitions);
                sentFields.topicPartitions = assignedPartitions;
            }

            return data;
        }

        private List<ConsumerGroupHeartbeatRequestData.TopicPartitions> buildTopicPartitionsList(Set<TopicIdPartition> topicIdPartitions) {
            List<ConsumerGroupHeartbeatRequestData.TopicPartitions> result = new ArrayList<>();
            Map<Uuid, List<Integer>> partitionsPerTopicId = new HashMap<>();
            for (TopicIdPartition topicIdPartition : topicIdPartitions) {
                Uuid topicId = topicIdPartition.topicId();
                partitionsPerTopicId.computeIfAbsent(topicId, __ -> new ArrayList<>()).add(topicIdPartition.partition());
            }
            for (Map.Entry<Uuid, List<Integer>> entry : partitionsPerTopicId.entrySet()) {
                Uuid topicId = entry.getKey();
                List<Integer> partitions = entry.getValue();
                result.add(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(topicId)
                        .setPartitions(partitions));
            }
            return result;
        }

        // Fields of ConsumerHeartbeatRequest sent in the most recent request
        static class SentFields {
            private String instanceId = null;
            private int rebalanceTimeoutMs = -1;
            private TreeSet<String> subscribedTopicNames = null;
            private String serverAssignor = null;
            private TreeSet<String> topicPartitions = null;

            SentFields() {}

            void reset() {
                instanceId = null;
                rebalanceTimeoutMs = -1;
                subscribedTopicNames = null;
                serverAssignor = null;
                topicPartitions = null;
            }
        }
    }
}
