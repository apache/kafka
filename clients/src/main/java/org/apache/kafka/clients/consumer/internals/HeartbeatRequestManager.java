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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class HeartbeatRequestManager implements RequestManager {
    final CoordinatorRequestManager coordinatorRequestManager;
    private final Time time;
    private final LogContext logContext;
    private final HeartbeatRequestState heartbeatRequestState;
    private final MemberState memberState;
    private final SubscriptionState subscriptions;
    private final int rebalanceTimeoutMs;
    private final Logger logger;


    public HeartbeatRequestManager(
        final Time time,
        final LogContext logContext,
        final long retryBackoffMs,
        final long heartbeatIntervalMs,
        final int rebalanceTimeoutMs,
        final MemberState memberState,
        final CoordinatorRequestManager coordinatorRequestManager,
        final SubscriptionState subscriptions){
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.time = time;
        this.logContext = logContext;
        this.heartbeatRequestState = new HeartbeatRequestState(heartbeatIntervalMs, retryBackoffMs);
        this.memberState = memberState;
        this.subscriptions = subscriptions;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.logger = logContext.logger(HeartbeatRequestManager.class);
    }

    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        if (!shouldHeartbeat(currentTimeMs)) {
            return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList());
        }
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(currentTimeMs);
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.singletonList(request));
    }

    boolean shouldHeartbeat(final long currentTimeMs) {
        return !this.memberState.unjoined() &&
            heartbeatRequestState.canSendRequest(currentTimeMs);
    }

    public NetworkClientDelegate.UnsentRequest makeHeartbeatRequestOnJoin(final long now) {
        ConsumerGroupHeartbeatRequestData data = new ConsumerGroupHeartbeatRequestData();
        this.heartbeatRequestState.onSendAttempt(now);

        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            new ConsumerGroupHeartbeatRequest.Builder(data),
            coordinatorRequestManager.coordinator());

        request.future().whenComplete((response, exception) -> {
            final long responseTimeMs = time.milliseconds();
            if (exception != null) {
                onSuccess((ConsumerGroupHeartbeatResponse) response.responseBody(), responseTimeMs);
            } else {
                onFailure(responseTimeMs);
            }
        });
        return request;
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final long now) {
        ConsumerGroupHeartbeatRequestData data = new ConsumerGroupHeartbeatRequestData()
            .setGroupId(this.memberState.groupId)
            .setMemberEpoch(this.memberState.memberEpoch)
            .setMemberId(this.memberState.memberId.orElse(""))
            .setRebalanceTimeoutMs(rebalanceTimeoutMs);

        this.memberState.groupInstanceId.ifPresent(data::setInstanceId);

        if (this.subscriptions.hasPatternSubscription()) {
            // data.setSubscribedTopicRegex(this.subscriptions.groupSubscription())
        } else {
            data.setSubscribedTopicNames(new ArrayList<>(this.subscriptions.subscription()));
        }

        if (this.memberState.assignor.type == MemberState.AssignorSelector.Type.CLIENT) {
            data.setClientAssignors(buildAssignors((List<MemberState.Assignor>) memberState.assignor.activeAssignor)); // todo fix
        } else {
            data.setServerAssignor((String) memberState.assignor.activeAssignor);
        }

        this.heartbeatRequestState.onSendAttempt(now);

        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            new ConsumerGroupHeartbeatRequest.Builder(data),
            coordinatorRequestManager.coordinator());

        request.future().whenComplete((response, exception) -> {
            final long responseTimeMs = time.milliseconds();
            if (exception != null) {
                onSuccess((ConsumerGroupHeartbeatResponse) response.responseBody(), responseTimeMs);
            } else {
                onFailure(responseTimeMs);
            }
        });
        return request;
    }

    private List<ConsumerGroupHeartbeatRequestData.Assignor> buildAssignors(final List<MemberState.Assignor> activeAssignor) {
        return activeAssignor.stream().map( a -> {
            ConsumerGroupHeartbeatRequestData.Assignor assignor = new ConsumerGroupHeartbeatRequestData.Assignor()
                .setName(a.name)
                .setReason(a.reason)
                .setMinimumVersion(a.minVersion)
                .setMaximumVersion(a.maxVersion)
                .setMetadataVersion(a.version)
                .setMetadataBytes(a.metadata);
            return assignor;
        }).collect(Collectors.toList());
    }

    private void onFailure(final long responseTimeMs) {
        this.heartbeatRequestState.onFailedAttempt(responseTimeMs);
    }

    private void onSuccess(final ConsumerGroupHeartbeatResponse response, long responseTimeMs) {
        if (response.data().errorCode() == Errors.NONE.code()) {
            this.heartbeatRequestState.onSuccessfulAttempt(responseTimeMs);
            try {
                this.memberState.maybeUpdateOnHeartbeatResponse(response.data());
            } catch (KafkaException e) {
                logger.error("Received unexpected error in heartbeat response: {}", e.getMessage());
            }
            return;
        }

        onError(response, responseTimeMs);
    }

    private void onError(ConsumerGroupHeartbeatResponse response, long responseTimeMs) {
        short errorCode = response.data().errorCode();
        this.heartbeatRequestState.onFailedAttempt(responseTimeMs);
        if (errorCode == Errors.FENCED_MEMBER_EPOCH.code()) {
            revokePartitions(subscriptions.assignedPartitions());
            memberState.reset();
        }

        if (errorCode == Errors.UNRELEASED_INSTANCE_ID.code()) {
            failFatally(Errors.UNRELEASED_INSTANCE_ID);
        }

        if (errorCode == Errors.UNSUPPORTED_ASSIGNOR.code()) {
            failFatally(Errors.UNSUPPORTED_ASSIGNOR);
        }

        if (errorCode == Errors.GROUP_AUTHORIZATION_FAILED.code()) {

        }

        if (errorCode == Errors.NOT_COORDINATOR.code()) {
            // mark the coordinator unknown and retry on the next poll
        }

        if (errorCode == Errors.COORDINATOR_NOT_AVAILABLE.code()) {
            // mark the coordinator unknown and retry on the next poll
        }

        if (errorCode == Errors.COORDINATOR_LOAD_IN_PROGRESS.code()) {
            // don't do anything. just retry on the next poll
        }

        if (errorCode == Errors.INVALID_REQUEST.code()) {
            failFatally(Errors.INVALID_REQUEST);
        }

        if (errorCode == Errors.UNKNOWN_MEMBER_ID.code()) {
            revokePartitions(subscriptions.assignedPartitions());
            memberState.reset();
        }
    }

    private void failFatally(final Errors error) {
        // TODO: send user a fatal failure via queue
    }

    private void revokePartitions(final Set<TopicPartition> partitions) {
        // TODO: need to implement the callback mechanism
    }

    class HeartbeatRequestState extends RequestState {
        final long heartbeatIntervalMs;
        final Timer heartbeatTimer;

        public HeartbeatRequestState(
            final long heartbeatIntervalMs,
            final long retryBackoffMs
        ) {
            super(retryBackoffMs);
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.heartbeatTimer = time.timer(heartbeatIntervalMs);
        }

        private void update(final long currentTimeMs) {
            this.heartbeatTimer.update(currentTimeMs);
        }

        @Override
        public boolean canSendRequest(final long currentTimeMs) {
            update(currentTimeMs);
            return heartbeatTimer.isExpired() && super.canSendRequest(currentTimeMs);
        }
    }
}
