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
import org.apache.kafka.common.protocol.Errors;

import java.util.Optional;

/**
 * Manages group membership for a single member.
 * Responsible for:
 * <li>Keeping member state</li>
 * <li>Keeping assignment for the member</li>
 * <li>Computing assignment for the group if the member is required to do so<li/>
 */
public interface MembershipManager {

    /**
     * ID of the consumer group the member is part of (or wants to be part of).
     */
    String groupId();

    /**
     * Instance ID used by the member when joining the group. If non-empty, it will indicate that
     * this is a static member.
     */
    Optional<String> groupInstanceId();

    /**
     * Member ID assigned by the server to this member when it joins the consumer group.
     */
    String memberId();

    /**
     * Current epoch of the member, maintained by the server.
     */
    int memberEpoch();

    /**
     * Current state of this member a part of the consumer group, as defined in {@link MemberState}.
     */
    MemberState state();

    /**
     * Update the member info and transition the member state based on a heartbeat response
     *
     * @param response Heartbeat response to extract member info and errors from.
     * @return Error found in the heartbeat response. It will return {@link Optional#empty()} if
     * no errors are found.
     */
    Optional<Errors> updateState(ConsumerGroupHeartbeatResponseData response);

    /**
     * Returns the {@link AssignorSelection} for the member
     */
    AssignorSelection assignorSelection();

    /**
     * Returns the current assignment for the member
     */
    ConsumerGroupHeartbeatResponseData.Assignment assignment();

    /**
     * Update the assignment for the member, indicating that the provided assignment is the new
     * current assignment.
     */
    void updateAssignment(ConsumerGroupHeartbeatResponseData.Assignment assignment);
}