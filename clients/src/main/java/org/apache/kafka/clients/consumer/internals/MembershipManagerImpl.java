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

import org.apache.kafka.common.errors.RetriableException;
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
     * ID of the consumer group the member will be part of., provided when creating the current
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
     * Current state of this member a part of the consumer group, as defined in {@link MemberState}
     */
    private MemberState state;

    /**
     * Assignor type selection for the member. If non-null, the member will send its selection to
     * the server on the {@link ConsumerGroupHeartbeatRequest}. If null, the server will select a
     * default assignor for the member, which the member does not need to track.
     */
    private AssignorSelection assignorSelection;

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
     * Latest assignment that the member received from the server while a {@link #targetAssignment}
     * was in process.
     */
    private Optional<ConsumerGroupHeartbeatResponseData.Assignment> nextTargetAssignment;

    /**
     * slf4j logger.
     */
    private final Logger log;

    public MembershipManagerImpl(String groupId, LogContext logContext) {
        this(groupId, null, null, logContext);
    }

    public MembershipManagerImpl(String groupId, String groupInstanceId,
                                 AssignorSelection assignorSelection, LogContext logContext) {
        if (groupId == null) {
            throw new IllegalArgumentException("Group ID cannot be null.");
        }
        this.groupId = groupId;
        this.state = MemberState.UNJOINED;
        this.assignorSelection = assignorSelection;
        this.groupInstanceId = Optional.ofNullable(groupInstanceId);
        this.targetAssignment = Optional.empty();
        this.nextTargetAssignment = Optional.empty();
        this.log = logContext.logger(MembershipManagerImpl.class);
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
        if (!this.state.equals(nextState) && !nextState.getPreviousValidStates().contains(state)) {
            throw new IllegalStateException(String.format("Invalid state transition from %s to %s",
                    state, nextState));
        }
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
    public Optional<Errors> updateState(ConsumerGroupHeartbeatResponseData response) {
        if (response.errorCode() == Errors.NONE.code()) {
            this.memberId = response.memberId();
            this.memberEpoch = response.memberEpoch();
            ConsumerGroupHeartbeatResponseData.Assignment assignment = response.assignment();
            if (assignment != null) {
                setTargetAssignment(assignment);
            }
            maybeTransitionToStable();
            return Optional.empty();
        } else {
            if (response.errorCode() == Errors.FENCED_MEMBER_EPOCH.code() || response.errorCode() == Errors.UNKNOWN_MEMBER_ID.code()) {
                resetEpoch();
                transitionTo(MemberState.FENCED);
            } else {
                if (Errors.forCode(response.errorCode()).exception() instanceof RetriableException) {
                    log.debug(String.format("Leaving member state %s unchanged after retriable " +
                                    "error %s received in heartbeat response for member %s",
                            state, Errors.forCode(response.errorCode()).exceptionName(), memberId));
                } else {
                    transitionTo(MemberState.FAILED);
                }
            }
            return Optional.of(Errors.forCode(response.errorCode()));
        }
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

    private void setTargetAssignment(ConsumerGroupHeartbeatResponseData.Assignment newTargetAssignment) {
        if (!targetAssignment.isPresent()) {
            targetAssignment = Optional.of(newTargetAssignment);
        } else {
            // Keep the latest next target assignment
            nextTargetAssignment = Optional.of(newTargetAssignment);
        }
    }

    private boolean hasPendingTargetAssignment() {
        return targetAssignment.isPresent() || nextTargetAssignment.isPresent();
    }

    /**
     * Update state and assignment as the member has successfully processed a new target
     * assignment.
     * This indicates the end of the reconciliation phase for the member, and makes the target
     * assignment the new current assignment.
     *
     * @param assignment Target assignment the member was able to successfully process
     */
    public void onAssignmentProcessSuccess(ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        updateAssignment(assignment);
        transitionTo(MemberState.STABLE);
    }

    /**
     * Update state and member info as the member was not able to process the assignment, due to
     * errors in the execution of the user-provided callbacks.
     *
     * @param error Exception found during the execution of the user-provided callbacks
     */
    public void onAssignmentProcessFailure(Throwable error) {
        transitionTo(MemberState.FAILED);
        // TODO: update member info appropriately, to clear up whatever shouldn't be kept in
        //  this unrecoverable state
    }

    private void resetEpoch() {
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

    // VisibleForTesting
    Optional<ConsumerGroupHeartbeatResponseData.Assignment> targetAssignment() {
        return targetAssignment;
    }

    // VisibleForTesting
    Optional<ConsumerGroupHeartbeatResponseData.Assignment> nextTargetAssignment() {
        return nextTargetAssignment;
    }

    /**
     * Set the current assignment for the member. This indicates that the reconciliation of the
     * target assignment has been successfully completed.
     * This will clear the {@link #targetAssignment}, and take on the
     * {@link #nextTargetAssignment} if any.
     *
     * @param assignment Assignment that has been successfully processed as part of the
     *                   reconciliation process.
     */
    @Override
    public void updateAssignment(ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        this.currentAssignment = assignment;
        if (!nextTargetAssignment.isPresent()) {
            targetAssignment = Optional.empty();
        } else {
            targetAssignment = Optional.of(nextTargetAssignment.get());
            nextTargetAssignment = Optional.empty();
        }
        maybeTransitionToStable();
    }
}