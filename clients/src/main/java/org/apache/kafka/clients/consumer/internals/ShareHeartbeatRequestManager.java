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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.util.ArrayList;
import java.util.TreeSet;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_SHARE_METRIC_GROUP_PREFIX;

/**
 * This is the heartbeat request manager for share groups.
 *
 * <p>See {@link AbstractHeartbeatRequestManager} for more details.</p>
 */
public class ShareHeartbeatRequestManager extends AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse> {

    /**
     * Membership manager for share groups
     */
    private final ShareMembershipManager membershipManager;

    /**
     * HeartbeatState manages building the heartbeat requests correctly
     */
    private final HeartbeatState heartbeatState;

    public static final String SHARE_PROTOCOL_NOT_SUPPORTED_MSG = "The cluster does not support the share group protocol. " +
        "To use share groups, the cluster must have the share group protocol enabled.";

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

    /**
     * {@inheritDoc}
     */
    @Override
    public void resetHeartbeatState() {
        heartbeatState.reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NetworkClientDelegate.UnsentRequest buildHeartbeatRequest() {
        return new NetworkClientDelegate.UnsentRequest(
            new ShareGroupHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
            coordinatorRequestManager.coordinator());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String heartbeatRequestName() {
        return "ShareGroupHeartbeatRequest";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Errors errorForResponse(ShareGroupHeartbeatResponse response) {
        return Errors.forCode(response.data().errorCode());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String errorMessageForResponse(ShareGroupHeartbeatResponse response) {
        return response.data().errorMessage();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long heartbeatIntervalForResponse(ShareGroupHeartbeatResponse response) {
        return response.data().heartbeatIntervalMs();
    }

    /**
     * {@inheritDoc}
     */
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

            // MemberId - always sent, it will be generated at Consumer startup.
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

    @Override
    public boolean handleSpecificError(final ShareGroupHeartbeatResponse response, final long currentTimeMs) {
        Errors error = errorForResponse(response);
        boolean errorHandled;

        switch (error) {
            // Broker responded with HB not supported, meaning the new protocol is not enabled, so propagate
            // custom message for it. Note that the case where the protocol is not supported at all should fail
            // on the client side when building the request and checking supporting APIs (handled on onFailure).
            case UNSUPPORTED_VERSION:
                logger.error("{} failed due to {}: {}", heartbeatRequestName(), error, SHARE_PROTOCOL_NOT_SUPPORTED_MSG);
                handleFatalFailure(error.exception(SHARE_PROTOCOL_NOT_SUPPORTED_MSG));
                errorHandled = true;
                break;

            default:
                errorHandled = false;
        }
        return errorHandled;
    }
}
