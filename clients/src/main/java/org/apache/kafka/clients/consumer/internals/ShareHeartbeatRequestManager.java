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
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.util.ArrayList;
import java.util.TreeSet;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_SHARE_METRIC_GROUP_PREFIX;

/**
 * <p>Manages the request creation and response handling for the heartbeat of a share group. The module creates a
 * {@link ShareGroupHeartbeatRequest} using the state stored in the {@link ShareMembershipManager} and enqueue it to
 * the network queue to be sent out. Once the response is received, it updates the state in the
 * {@link ShareMembershipManager} and handles any errors.</p>
 *
 * <p>The manager will try to send a heartbeat when the member is in {@link MemberState#STABLE},
 * {@link MemberState#JOINING}, or {@link MemberState#RECONCILING}, which means the member is either in a stable
 * group, is trying to join a group, or is in the process of reconciling the assignment changes.</p>
 *
 * <p>If the member got kicked out of a group, it will attempt to join again with a zero epoch.</p>
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
public class ShareHeartbeatRequestManager extends AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse, ShareGroupHeartbeatResponseData> {

    /**
     * Membership manager for consumer groups
     */
    private final ShareMembershipManager membershipManager;

    /*
     * HeartbeatState manages building the heartbeat requests correctly
     */
    private final HeartbeatState heartbeatState;

    public ShareHeartbeatRequestManager(
            final LogContext logContext,
            final Time time,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final SubscriptionState subscriptions,
            final ShareMembershipManager membershipManager,
            final BackgroundEventHandler backgroundEventHandler,
            final Metrics metrics) {
        super(logContext, time, config, coordinatorRequestManager, backgroundEventHandler,
                new HeartbeatMetricsManager(metrics, CONSUMER_SHARE_METRIC_GROUP_PREFIX));
        this.membershipManager = membershipManager;
        this.heartbeatState = new HeartbeatState(subscriptions, membershipManager);
    }

    // Visible for testing
    ShareHeartbeatRequestManager(
            final LogContext logContext,
            final Timer timer,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final ShareMembershipManager membershipManager,
            final HeartbeatState heartbeatState,
            final AbstractHeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState,
            final BackgroundEventHandler backgroundEventHandler,
            final Metrics metrics) {
        super(logContext, timer, config, coordinatorRequestManager, heartbeatRequestState, backgroundEventHandler,
                new HeartbeatMetricsManager(metrics, CONSUMER_SHARE_METRIC_GROUP_PREFIX));
        this.membershipManager = membershipManager;
        this.heartbeatState = heartbeatState;
    }

    @Override
    public void resetHeartbeatState() {
        heartbeatState.reset();
    }

    @Override
    public NetworkClientDelegate.UnsentRequest buildHeartbeatRequest() {
        return new NetworkClientDelegate.UnsentRequest(
                new ShareGroupHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
                coordinatorRequestManager.coordinator());
    }

    @Override
    public String heartbeatRequestName() {
        return "ShareGroupHeartbeatRequest";
    }

    @Override
    public short errorCodeForResponse(ShareGroupHeartbeatResponse response) {
        return response.data().errorCode();
    }

    @Override
    public String errorMessageForResponse(ShareGroupHeartbeatResponse response) {
        return response.data().errorMessage();
    }

    @Override
    public long heartbeatIntervalForResponse(ShareGroupHeartbeatResponse response) {
        return response.data().heartbeatIntervalMs();
    }

    @Override
    public ShareGroupHeartbeatResponseData responseData(ShareGroupHeartbeatResponse response) {
        return response.data();
    }

    @Override
    public ShareMembershipManager membershipManager() {
        return membershipManager;
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
