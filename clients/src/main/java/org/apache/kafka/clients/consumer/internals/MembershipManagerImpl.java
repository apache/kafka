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

import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.List;
import java.util.Optional;

/**
 * Membership manager that maintains group membership following the new consumer group protocol.
 * It sends periodic heartbeat requests according to the defined interval.
 * <p>
 * Heartbeat responses are processed to update the member state, and process assignments received.
 */
public class MembershipManagerImpl implements MembershipManager {

    private String groupId;
    private Optional<String> groupInstanceId;
    private String memberId;
    private int memberEpoch;
    private MemberState state;
    private AssignorSelector assignorSelector;
    private ConsumerGroupHeartbeatResponseData.Assignment assignment;

    public MembershipManagerImpl(String groupId) {
        this.groupId = groupId;
        this.state = MemberState.UNJOINED;
    }

    public MembershipManagerImpl(String groupId, String groupInstanceId) {
        this(groupId);
        this.groupInstanceId = Optional.ofNullable(groupInstanceId);
    }

    public void setupClientAssignors(List<ConsumerGroupHeartbeatRequestData.Assignor> clientAssignors) {
        // TODO: double check that no validation is required here, given that a member could
        //  start using client side assignors at any given time.
        assignorSelector = AssignorSelector.newClientAssignors(clientAssignors);
    }

    private void transitionTo(MemberState nextState) {
        if (!nextState.getPreviousValidStates().contains(state)) {
            // TODO: handle invalid state transition
            throw new RuntimeException(String.format("Invalid state transition from %s to %s",
                    state, nextState));
        }
        this.state = nextState;
    }

    @Override
    public String groupId() {
        return groupId;
    }

    @Override
    public String groupInstanceId() {
        // TODO: review empty vs null instance id
        return groupInstanceId.orElse(null);
    }

    @Override
    public String memberId() {
        return memberId;
    }

    @Override
    public int memberEpoch() {
        return memberEpoch;
    }

    @Override
    public void updateStateOnHeartbeatResponse(ConsumerGroupHeartbeatResponseData response) {
        if (response.errorCode() == Errors.NONE.code()) {
            // Successful heartbeat response. Extract metadata and assignment
            this.memberId = response.memberId();
            this.memberEpoch = response.memberEpoch();
            assignment = response.assignment();
            // TODO: validate response & process assignment
            transitionTo(MemberState.STABLE);
        } else {
            // TODO: error handling
            handleHeartbeatError(response);
            transitionTo(MemberState.UNJOINED);
        }
    }

    private void handleHeartbeatError(ConsumerGroupHeartbeatResponseData responseData) {
        if (responseData.errorCode() == Errors.FENCED_MEMBER_EPOCH.code() || responseData.errorCode() == Errors.UNKNOWN_MEMBER_ID.code()) {
            resetMemberIdAndEpoch();
        }
    }

    private void resetMemberIdAndEpoch() {
        this.memberId = "";
        this.memberEpoch = 0;
    }

    @Override
    public MemberState state() {
        return state;
    }

    @Override
    public AssignorSelector.Type assignorType() {
        return assignorSelector.type;
    }

    @Override
    public String serverAssignor() {
        return null;
    }

    @Override
    public List<ConsumerGroupHeartbeatRequestData.Assignor> clientAssignors() {
        return null;
    }


}
