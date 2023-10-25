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

import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;

import java.util.Optional;

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
     * Update member info and transition member state based on a heartbeat response.
     *
     * @param response Heartbeat response to extract member info and errors from.
     */
    void updateState(ConsumerGroupHeartbeatResponseData response);

    /**
     * @return Server-side assignor implementation configured for the member, that will be sent
     * out to the server to be used. If empty, then the server will select the assignor.
     */
    Optional<String> serverAssignor();

    /**
     * @return Current assignment for the member.
     */
    ConsumerGroupHeartbeatResponseData.Assignment currentAssignment();

    /**
     * Update the assignment for the member, indicating that the provided assignment is the new
     * current assignment.
     */
    void onTargetAssignmentProcessComplete(ConsumerGroupHeartbeatResponseData.Assignment assignment);

    /**
     * Transition the member to the FENCED state and update the member info as required. This is
     * only invoked when the heartbeat returns a FENCED_MEMBER_EPOCH or UNKNOWN_MEMBER_ID error.
     * code.
     */
    void transitionToFenced();

    /**
     * Transition the member to the FAILED state and update the member info as required. This is
     * invoked when un-recoverable errors occur (ex. when the heartbeat returns a non-retriable
     * error or when errors occur while executing the user-provided callbacks)
     */
    void transitionToFailed();

    /**
     * @return True if the member should send heartbeat to the coordinator.
     */
    boolean shouldSendHeartbeat();
}
