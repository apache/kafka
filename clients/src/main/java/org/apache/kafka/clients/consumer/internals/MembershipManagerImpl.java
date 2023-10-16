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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Membership manager that maintains group membership for a single member following the new
 * consumer group protocol.
 * <p/>
 * This keeps membership state and assignment updated in-memory, based on the heartbeat responses
 * the member receives. It is also responsible for computing assignment for the group based on
 * the metadata, if the member has been selected by the broker to do so.
 */
public class MembershipManagerImpl implements MembershipManager {

    /**
     * ID of the consumer group.
     */
    private final String groupId;

    /**
     * Instance ID to use by this member. If not empty, this indicates that it is a static member.
     */
    private final Optional<String> groupInstanceId;

    /**
     * Member ID received from the broker.
     */
    private String memberId;

    /**
     * Current member epoch. This will be updated with the latest member epoch received from the
     * broker, or reset to be sent to the broker when the member wants to re-join or leave the
     * group.
     */
    private int memberEpoch;

    /**
     * Current state of the member.
     */
    private MemberState state;

    /**
     * Assignor selection that will indicate if server or client side assignors are in use, and
     * the specific assignor implementation. This will default to server-side assignor, with null
     * default implementation, and in that case the broker will choose the default implementation
     * to use.
     */
    private AssignorSelection assignorSelection;

    /**
     * Futures that will be completed when a new member ID is received in the Heartbeat response.
     */
    private final List<CompletableFuture<Void>> memberIdUpdateWatchers;

    /**
     * Futures that will be completed when a new member epoch is received in the Heartbeat response.
     */
    private final List<CompletableFuture<Void>> memberEpochUpdateWatchers;

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

    public MembershipManagerImpl(String groupId) {
        this(groupId, null, null);
    }

    public MembershipManagerImpl(String groupId, String groupInstanceId, AssignorSelection assignorSelection) {
        this.groupId = groupId;
        this.state = MemberState.UNJOINED;
        if (assignorSelection == null) {
            setAssignorSelection(AssignorSelection.defaultAssignor());
        } else {
            setAssignorSelection(assignorSelection);
        }
        this.groupInstanceId = Optional.ofNullable(groupInstanceId);
        this.targetAssignment = Optional.empty();
        this.nextTargetAssignment = Optional.empty();
        this.memberIdUpdateWatchers = new ArrayList<>();
        this.memberEpochUpdateWatchers = new ArrayList<>();
    }

    /**
     * Update assignor selection for the member.
     *
     * @param assignorSelection New assignor selection
     * @throws IllegalArgumentException If the provided assignor selection is null
     */
    public final void setAssignorSelection(AssignorSelection assignorSelection) {
        if (assignorSelection == null) {
            throw new IllegalArgumentException("Assignor selection cannot be null");
        }
        this.assignorSelection = assignorSelection;
    }

    private void transitionTo(MemberState nextState) {
        if (!this.state.equals(nextState) && !nextState.getPreviousValidStates().contains(state)) {
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
    public Optional<String> groupInstanceId() {
        return groupInstanceId;
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
    public void leaveGroup() {
        memberEpoch = leaveGroupEpoch();
        transitionTo(MemberState.NOT_IN_GROUP);
        failAllUpdateWatchers();
    }

    /**
     * Return the epoch to use in the Heartbeat request to indicate that the member wants to
     * leave the group. Should be -2 if this is a static member, or -1 in any other case.
     */
    private int leaveGroupEpoch() {
        return groupInstanceId.isPresent() ? -2 : -1;
    }

    @Override
    public void updateState(ConsumerGroupHeartbeatResponseData response) {
        if (response.errorCode() != Errors.NONE.code()) {
            String errorMessage = String.format(
                    "Unexpected error in Heartbeat response. Expected no error, but received: %s",
                    Errors.forCode(response.errorCode())
            );
            throw new IllegalStateException(errorMessage);
        }
        if (!Objects.equals(memberId, response.memberId())) {
            // New member ID received
            this.memberId = response.memberId();
            notifyMemberIdChange();
        }

        if (this.memberEpoch != response.memberEpoch()) {
            // New member epoch received
            this.memberEpoch = response.memberEpoch();
            notifyMemberEpochChange();
        }

        ConsumerGroupHeartbeatResponseData.Assignment assignment = response.assignment();
        if (assignment != null) {
            setTargetAssignment(assignment);
        }
        maybeTransitionToStable();
    }

    @Override
    public void transitionToFenced() {
        resetEpoch();
        transitionTo(MemberState.FENCED);
    }

    @Override
    public void transitionToFailed() {
        transitionTo(MemberState.FAILED);
        // This is an unrecoverable state so all futures that were waiting for member ID and
        // epoch updates should be completed exceptionally.
        failAllUpdateWatchers();
    }

    private void notifyMemberIdChange() {
        memberIdUpdateWatchers.forEach(f -> f.complete(null));
        memberIdUpdateWatchers.clear();
    }

    private void notifyMemberEpochChange() {
        memberEpochUpdateWatchers.forEach(f -> f.complete(null));
        memberEpochUpdateWatchers.clear();
    }

    private void failAllUpdateWatchers() {
        memberIdUpdateWatchers.forEach(f -> f.completeExceptionally(new KafkaException("Member " +
                "failed with unrecoverable error")));
        memberIdUpdateWatchers.clear();

        memberEpochUpdateWatchers.forEach(f -> f.completeExceptionally(new KafkaException("Member" +
                " failed with unrecoverable error")));
        memberEpochUpdateWatchers.clear();
    }

    @Override
    public boolean shouldSendHeartbeat() {
        return state() != MemberState.FAILED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> registerForMemberIdUpdate() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        memberIdUpdateWatchers.add(future);
        return future;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> registerForMemberEpochUpdate() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        memberEpochUpdateWatchers.add(future);
        return future;
    }

    // VisibleForTesting
    int memberIdUpdateWatcherCount() {
        return memberIdUpdateWatchers.size();
    }

    // VisibleForTesting
    int memberEpochUpdateWatcherCount() {
        return memberEpochUpdateWatchers.size();
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
