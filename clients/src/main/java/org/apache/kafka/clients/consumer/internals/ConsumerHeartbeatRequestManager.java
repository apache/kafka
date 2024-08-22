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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * <p>Manages the request creation and response handling for the heartbeat of a consumer group. The module creates a
 * {@link ConsumerGroupHeartbeatRequest} using the state stored in the {@link ConsumerMembershipManager} and enqueue it to
 * the network queue to be sent out. Once the response is received, it updates the state in the
 * {@link ConsumerMembershipManager} and handles any errors.</p>
 *
 * <p>The manager will try to send a heartbeat when the member is in {@link MemberState#STABLE},
 * {@link MemberState#JOINING}, or {@link MemberState#RECONCILING}, which means the member is either in a stable
 * group, is trying to join a group, or is in the process of reconciling the assignment changes.</p>
 *
 * <p>If the member got kicked out of a group, it will try to give up the current assignment by invoking {@code
 * OnPartitionsLost} before attempting to join again with a zero epoch.</p>
 *
 * <p>If the member does not have groupId configured or encountering fatal exceptions, a heartbeat will not be sent.</p>
 *
 * <p>If the coordinator not is not found, we will skip sending the heartbeat and try to find a coordinator first.</p>
 *
 * <p>If the heartbeat failed due to retriable errors, such as TimeoutException, the subsequent attempt will be
 * backed off exponentially.</p>
 *
 * <p>When the member completes the assignment reconciliation, the {@link HeartbeatRequestState} will be reset so
 * that a heartbeat will be sent in the next event loop.</p>
 *
 * <p>See {@link AbstractHeartbeatRequestManager.HeartbeatRequestState} for more details.</p>
 */
public class ConsumerHeartbeatRequestManager extends AbstractHeartbeatRequestManager<ConsumerGroupHeartbeatResponse, ConsumerGroupHeartbeatResponseData> {

    /**
     * Membership manager for consumer groups
     */
    private final ConsumerMembershipManager membershipManager;

    /**
     * HeartbeatState manages building the heartbeat requests correctly
     */
    private final HeartbeatState heartbeatState;

    public ConsumerHeartbeatRequestManager(
            final LogContext logContext,
            final Time time,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final SubscriptionState subscriptions,
            final ConsumerMembershipManager membershipManager,
            final BackgroundEventHandler backgroundEventHandler,
            final Metrics metrics) {
        super(logContext, time, config, coordinatorRequestManager, backgroundEventHandler,
                new HeartbeatMetricsManager(metrics));
        this.membershipManager = membershipManager;
        this.heartbeatState = new HeartbeatState(subscriptions, membershipManager, maxPollIntervalMs);
    }

    // Visible for testing
    ConsumerHeartbeatRequestManager(
            final LogContext logContext,
            final Timer timer,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final ConsumerMembershipManager membershipManager,
            final HeartbeatState heartbeatState,
            final AbstractHeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState,
            final BackgroundEventHandler backgroundEventHandler,
            final Metrics metrics) {
        super(logContext, timer, config, coordinatorRequestManager, heartbeatRequestState, backgroundEventHandler,
                new HeartbeatMetricsManager(metrics));
        this.membershipManager = membershipManager;
        this.heartbeatState = heartbeatState;
    }

    public boolean handleSpecificError(final ConsumerGroupHeartbeatResponse response,
                                       final long currentTimeMs) {
        Errors error = Errors.forCode(errorCodeForResponse(response));
        String errorMessage = errorMessageForResponse(response);
        boolean errorHandled;

        switch (error) {
            case UNRELEASED_INSTANCE_ID:
                logger.error("{} failed due to unreleased instance id {}: {}",
                        heartbeatRequestName(), membershipManager.groupInstanceId().orElse("null"), errorMessage);
                handleFatalFailure(error.exception(errorMessage));
                errorHandled = true;
                break;

            case FENCED_INSTANCE_ID:
                logger.error("{} failed due to fenced instance id {}: {}. " +
                                "This is expected in the case that the member was removed from the group " +
                                "by an admin client, and another member joined using the same group instance id.",
                        heartbeatRequestName(), membershipManager.groupInstanceId().orElse("null"), errorMessage);
                handleFatalFailure(error.exception(errorMessage));
                errorHandled = true;
                break;

            default:
                errorHandled = false;
        }
        return errorHandled;
    }

    @Override
    public void resetHeartbeatState() {
        heartbeatState.reset();
    }

    @Override
    public NetworkClientDelegate.UnsentRequest buildHeartbeatRequest() {
        return new NetworkClientDelegate.UnsentRequest(
                new ConsumerGroupHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
                coordinatorRequestManager.coordinator());
    }

    @Override
    public String heartbeatRequestName() {
        return "ConsumerGroupHeartbeatRequest";
    }

    @Override
    public short errorCodeForResponse(ConsumerGroupHeartbeatResponse response) {
        return response.data().errorCode();
    }

    @Override
    public String errorMessageForResponse(ConsumerGroupHeartbeatResponse response) {
        return response.data().errorMessage();
    }

    @Override
    public long heartbeatIntervalForResponse(ConsumerGroupHeartbeatResponse response) {
        return response.data().heartbeatIntervalMs();
    }

    @Override
    public ConsumerGroupHeartbeatResponseData responseData(ConsumerGroupHeartbeatResponse response) {
        return response.data();
    }

    @Override
    public ConsumerMembershipManager membershipManager() {
        return membershipManager;
    }

    /**
     * Builds the heartbeat requests correctly, ensuring that all information is sent according to
     * the protocol, but subsequent requests do not send information which has not changed. This
     * is important to ensure that reconciliation completes successfully.
     */
    static class HeartbeatState {
        private final SubscriptionState subscriptions;
        private final ConsumerMembershipManager membershipManager;
        private final int rebalanceTimeoutMs;
        private final SentFields sentFields;

        public HeartbeatState(
                final SubscriptionState subscriptions,
                final ConsumerMembershipManager membershipManager,
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

            // SubscribedTopicNames - only sent if it has changed since the last heartbeat
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
            AbstractMembershipManager.LocalAssignment local = membershipManager.currentAssignment();
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
            private AbstractMembershipManager.LocalAssignment localAssignment = null;

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
