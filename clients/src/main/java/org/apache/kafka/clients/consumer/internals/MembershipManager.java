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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A stateful object tracking the state of a single member in relationship to a consumer group:
 * <p/>
 * Responsible for:
 * <li>Keeping member state</li>
 * <li>Keeping assignment for the member</li>
 * <li>Computing assignment for the group if the member is required to do so<li/>
 */
public interface MembershipManager {

    /**
     * @return Group ID of the consumer group the member is part of (or wants to be part of).
     */
    String groupId();

    /**
     * @return Instance ID used by the member when joining the group. If non-empty, it will indicate that
     * this is a static member.
     */
    Optional<String> groupInstanceId();

    /**
     * @return Member ID assigned by the server to this member when it joins the consumer group.
     */
    String memberId();

    /**
     * @return Current epoch of the member, maintained by the server.
     */
    int memberEpoch();

    /**
     * @return Current state of this member in relationship to a consumer group, as defined in
     * {@link MemberState}.
     */
    MemberState state();

    /**
     * Update member info and transition member state based on a successful heartbeat response.
     *
     * @param response Heartbeat response to extract member info and errors from.
     */
    void onHeartbeatResponseReceived(ConsumerGroupHeartbeatResponseData response);

    /**
     * Update state when a heartbeat is sent out. This will transition out of the states that end
     * when a heartbeat request is sent, without waiting for a response (ex.
     * {@link MemberState#SENDING_ACK_FOR_RECONCILED_ASSIGNMENT} and
     * {@link MemberState#SENDING_LEAVE_REQUEST}.
     */
    void onHeartbeatRequestSent();

    /**
     * @return Server-side assignor implementation configured for the member, that will be sent
     * out to the server to be used. If empty, then the server will select the assignor.
     */
    Optional<String> serverAssignor();

    /**
     * @return Current assignment for the member.
     */
    Set<TopicPartition> currentAssignment();

    /**
     * Transition to the {@link MemberState#JOINING} state, indicating that the member will
     * try to join the group on the next heartbeat request. This is expected to be invoked when
     * the user calls the subscribe API, or when the member wants to rejoin after getting fenced.
     */
    void transitionToJoining();

    /**
     * Transition the member to the FENCED state, where the member will release the assignment by
     * calling the onPartitionsLost callback, and when the callback completes, it will transition
     * to {@link MemberState#JOINING} to rejoin the group. This is expected to be invoked when
     * the heartbeat returns a FENCED_MEMBER_EPOCH or UNKNOWN_MEMBER_ID error.
     */
    void transitionToFenced();

    /**
     * Transition the member to the FAILED state and update the member info as required. This is
     * invoked when un-recoverable errors occur (ex. when the heartbeat returns a non-retriable
     * error)
     */
    void transitionToFatal();

    /**
     * Release assignment and transition to {@link MemberState#LEAVING} so that a heartbeat
     * request is sent indicating the broker that the member wants to leave the group. This is
     * expected to be invoked when the user calls the unsubscribe API.
     *
     * @return Future that will complete when the callback execution completes and the heartbeat
     * to leave the group has been sent out.
     */
    CompletableFuture<Void> leaveGroup();

    /**
     * @return True if the member should send heartbeat to the coordinator without waiting for
     * the interval.
     */
    boolean shouldHeartbeatNow();

    /**
     * @return True if the member should skip sending heartbeat to the coordinator. This could be
     * the case then the member is not in a group, or when it failed with a fatal error.
     */
    boolean shouldSkipHeartbeat();
}
