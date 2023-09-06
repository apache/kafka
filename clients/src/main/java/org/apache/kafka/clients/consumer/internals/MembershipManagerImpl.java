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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Membership manager that maintains group membership for a single member following the new
 * consumer group protocol.
 * <p/>
 * This keeps membership state and assignment updated in-memory, based on the heartbeat responses
 * the member receives. It is also responsible for computing assignment for the group based on
 * the metadata, if the member has been selected by the broker to do so.
 */
public class MembershipManagerImpl implements MembershipManager {

    private final String groupId;
    private Optional<String> groupInstanceId;
    private String memberId;
    private int memberEpoch;
    private MemberState state;
    private AssignorSelection assignorSelection;

    /**
     * Assignment that the member received from the server and successfully processed
     */
    private ConsumerGroupHeartbeatResponseData.Assignment currentAssignment;

    /**
     * List of assignments that the member received from the server but hasn't processed yet
     */
    private final List<ConsumerGroupHeartbeatResponseData.Assignment> targetAssignments;

    public MembershipManagerImpl(String groupId) {
        this.groupId = groupId;
        this.state = MemberState.UNJOINED;
        this.assignorSelection = AssignorSelection.defaultAssignor();
        this.targetAssignments = new ArrayList<>();
    }

    public MembershipManagerImpl(String groupId, String groupInstanceId, AssignorSelection assignorSelection) {
        this(groupId);
        this.groupInstanceId = Optional.ofNullable(groupInstanceId);
        setAssignorSelection(assignorSelection);
    }

    /**
     * Update assignor selection for the member.
     *
     * @param assignorSelection New assignor selection
     * @throws IllegalArgumentException If the provided assignor selection is null
     */
    public void setAssignorSelection(AssignorSelection assignorSelection) {
        if (assignorSelection == null) {
            throw new IllegalArgumentException("Assignor selection cannot be null");
        }
        this.assignorSelection = assignorSelection;
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
    public void updateState(ConsumerGroupHeartbeatResponseData response) {
        if (response.errorCode() == Errors.NONE.code()) {
            this.memberId = response.memberId();
            this.memberEpoch = response.memberEpoch();
            targetAssignments.add(response.assignment());
            transitionTo(MemberState.PROCESSING_ASSIGNMENT);
        } else {
            if (response.errorCode() == Errors.FENCED_MEMBER_EPOCH.code() || response.errorCode() == Errors.UNKNOWN_MEMBER_ID.code()) {
                resetMemberIdAndEpoch();
                transitionTo(MemberState.UNJOINED);
            } else if (response.errorCode() == Errors.UNRELEASED_INSTANCE_ID.code()) {
                transitionTo(MemberState.FAILED);
            }
        }
    }

    public void onAssignmentProcessSuccess(ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        currentAssignment = assignment;
        targetAssignments.remove(assignment);
        transitionTo(MemberState.STABLE);
    }

    public void onAssignmentProcessFailure(Throwable error) {
        // TODO: handle failure scenario when the member was not able to process the assignment
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
    public AssignorSelection assignorSelection() {
        return this.assignorSelection;
    }

    @Override
    public ConsumerGroupHeartbeatResponseData.Assignment assignment() {
        return this.currentAssignment;
    }

    @Override
    public void updateAssignment(ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        this.currentAssignment = assignment;
    }

    @Override
    public Object computeAssignment(Object groupState) {
        throw new UnsupportedOperationException("Client side assignment computation not supported" +
                " yet.");
    }

}
