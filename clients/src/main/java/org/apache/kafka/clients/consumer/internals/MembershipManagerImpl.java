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
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Optional;

/**
 * Membership manager that maintains group membership for a single member, following the new
 * consumer group protocol.
 * <p/>
 * This is responsible for:
 * <li>Keeping member info (ex. member id, member epoch, assignment, etc.)</li>
 * <li>Keeping member state as defined in {@link MemberState}.</li>
 * <p/>
 * Member info and state are updated based on the heartbeat responses the member receives.
 */
public class MembershipManagerImpl implements MembershipManager {

    /**
     * ID of the consumer group the member will be part of, provided when creating the current
     * membership manager.
     */
    private final String groupId;

    /**
     * Group instance ID to be used by the member, provided when creating the current membership manager.
     */
    private final Optional<String> groupInstanceId;

    /**
     * Member ID assigned by the server to the member, received in a heartbeat response when
     * joining the group specified in {@link #groupId}
     */
    private String memberId;

    /**
     * Current epoch of the member. It will be set to 0 by the member, and provided to the server
     * on the heartbeat request, to join the group. It will be then maintained by the server,
     * incremented as the member reconciles and acknowledges the assignments it receives.
     */
    private int memberEpoch;

    /**
     * Current state of this member as part of the consumer group, as defined in {@link MemberState}
     */
    private MemberState state;

    /**
     * Assignor selection configured for the member, that will be sent out to the server on the
     * {@link ConsumerGroupHeartbeatRequest}. If empty, then the server will select the assignor
     * to use.
     */
    private Optional<AssignorSelection> assignorSelection;

    /**
     * Assignment that the member received from the server and successfully processed.
     */
    private ConsumerGroupHeartbeatResponseData.Assignment currentAssignment;

    /**
     * Assignment that the member received from the server but hasn't completely processed
     * yet.
     */
    private Optional<ConsumerGroupHeartbeatResponseData.Assignment> targetAssignment;

    /**
     * slf4j logger.
     */
    private final Logger log;

    public MembershipManagerImpl(String groupId, LogContext logContext) {
        this(groupId, null, null, logContext);
    }

    public MembershipManagerImpl(String groupId,
                                 String groupInstanceId,
                                 AssignorSelection assignorSelection,
                                 LogContext logContext) {
        if (groupId == null) {
            throw new IllegalArgumentException("Group ID cannot be null.");
        }
        this.groupId = groupId;
        this.state = MemberState.UNJOINED;
        this.assignorSelection = Optional.ofNullable(assignorSelection);
        this.groupInstanceId = Optional.ofNullable(groupInstanceId);
        this.targetAssignment = Optional.empty();
        this.log = logContext.logger(MembershipManagerImpl.class);
    }

    /**
     * Update assignor selection for the member.
     *
     * @param assignorSelection New assignor selection. If empty is provided, this will
     *                          effectively clear the previous assignor selection defined for the
     *                          member.
     * @throws IllegalArgumentException If the provided optional assignor selection is null.
     */
    public void setAssignorSelection(Optional<AssignorSelection> assignorSelection) {
        if (assignorSelection == null) {
            throw new IllegalArgumentException("Optional assignor selection cannot be null");
        }
        this.assignorSelection = assignorSelection;
    }

    /**
     * Update the member state, setting it to the nextState only if it is a valid transition.
     *
     * @throws IllegalStateException If transitioning from the member {@link #state} to the
     *                               nextState is not allowed as defined in {@link MemberState}.
     */
    private void transitionTo(MemberState nextState) {
        if (!this.state.equals(nextState) && !nextState.getPreviousValidStates().contains(state)) {
            throw new IllegalStateException(String.format("Invalid state transition from %s to %s",
                    state, nextState));
        }
        log.trace("Member %s state transition from %s to %s", memberId, state, nextState);
        this.state = nextState;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String groupId() {
        return groupId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String memberId() {
        return memberId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int memberEpoch() {
        return memberEpoch;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateState(ConsumerGroupHeartbeatResponseData response) {
        if (response.errorCode() != Errors.NONE.code()) {
            throw new IllegalArgumentException("Invalid response with error");
        }
        this.memberId = response.memberId();
        this.memberEpoch = response.memberEpoch();
        ConsumerGroupHeartbeatResponseData.Assignment assignment = response.assignment();
        if (assignment != null) {
            setTargetAssignment(assignment);
        }
        maybeTransitionToStable();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fenceMember() {
        resetEpoch();
        transitionTo(MemberState.FENCED);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void failMember() {
        transitionTo(MemberState.FAILED);
    }

    /**
     * Transition to {@link MemberState#STABLE} only if there are no target assignments left to
     * reconcile. Transition to {@link MemberState#RECONCILING} otherwise.
     */
    private boolean maybeTransitionToStable() {
        if (!hasPendingTargetAssignment()) {
            transitionTo(MemberState.STABLE);
        } else {
            transitionTo(MemberState.RECONCILING);
        }
        return state.equals(MemberState.STABLE);
    }

    /**
     * Take new target assignment received from the server and set it as targetAssignment to be
     * processed. If an assignment is already in process this newTargetAssignment will be ignored
     * for now.
     */
    private void setTargetAssignment(ConsumerGroupHeartbeatResponseData.Assignment newTargetAssignment) {
        if (!targetAssignment.isPresent()) {
            targetAssignment = Optional.of(newTargetAssignment);
        } else {
            log.debug("Temporarily ignoring assignment %s received while member %s is still " +
                    "processing a previous assignment.", newTargetAssignment, memberId);
        }
    }

    /**
     * Returns true if the member has a target assignment being processed.
     */
    private boolean hasPendingTargetAssignment() {
        return targetAssignment.isPresent();
    }

    private void resetEpoch() {
        this.memberEpoch = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MemberState state() {
        return state;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<AssignorSelection> assignorSelection() {
        return this.assignorSelection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerGroupHeartbeatResponseData.Assignment currentAssignment() {
        return this.currentAssignment;
    }


    /**
     * Assignment that the member received from the server but hasn't completely processed yet.
     */
    // VisibleForTesting
    Optional<ConsumerGroupHeartbeatResponseData.Assignment> targetAssignment() {
        return targetAssignment;
    }

    /**
     * This indicates that the reconciliation of the target assignment has been successfully
     * completed, so it will make it effective by assigning it to the current assignment.
     *
     * @params error Exception found while executing the user-provided callbacks to process the
     * target assignment. Empty optional if no errors occurred.
     */
    @Override
    public void onTargetAssignmentProcessComplete(Optional<Throwable> error) {
        if (!targetAssignment.isPresent()) {
            throw new IllegalStateException("Unexpected empty target assignment when completing " +
                    "reconciliation process.");
        }
        if (!error.isPresent()) {
            this.currentAssignment = targetAssignment.get();
            targetAssignment = Optional.empty();
            maybeTransitionToStable();
        } else {
            log.debug("Updating state after assignment process failed");
            transitionTo(MemberState.FAILED);
        }
    }
}