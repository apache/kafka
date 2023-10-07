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
 * Manages group membership for a single member.
 * Responsible for:
 * <li>Keeping member state</li>
 * <li>Keeping assignment for the member</li>
 * <li>Computing assignment for the group if the member is required to do so<li/>
 */
public interface MembershipManager {

    String groupId();

    Optional<String> groupInstanceId();

    String memberId();

    int memberEpoch();

    MemberState state();

    /**
     * Update the current state of the member based on a heartbeat response
     */
    void updateState(ConsumerGroupHeartbeatResponseData response);

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
