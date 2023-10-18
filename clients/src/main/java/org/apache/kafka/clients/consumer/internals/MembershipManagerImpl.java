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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
    private final Optional<String> groupInstanceId;
    private String memberId;
    private int memberEpoch;
    private MemberState state;
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
     * Metadata that allows us to create the partitions needed for {@link ConsumerRebalanceListener}.
     */
    private final ConsumerMetadata metadata;
    /**
     * AssignmentReconciler that handles updates to partition assignments.
     */
    private final AssignmentReconciler assignmentReconciler;


    public MembershipManagerImpl(ConsumerMetadata metadata,
                                 String groupId,
                                 AssignmentReconciler assignmentReconciler) {
        this(metadata, groupId, null, null, assignmentReconciler);
    }

    public MembershipManagerImpl(ConsumerMetadata metadata,
                                 String groupId,
                                 String groupInstanceId,
                                 AssignorSelection assignorSelection,
                                 AssignmentReconciler assignmentReconciler) {
        this.metadata = metadata;
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
        this.assignmentReconciler = assignmentReconciler;
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
    public void updateState(ConsumerGroupHeartbeatResponseData response) {
        if (response.errorCode() != Errors.NONE.code()) {
            String errorMessage = String.format(
                    "Unexpected error in Heartbeat response. Expected no error, but received: %s",
                    Errors.forCode(response.errorCode())
            );
            throw new IllegalStateException(errorMessage);
        }
        this.memberId = response.memberId();
        this.memberEpoch = response.memberEpoch();
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
        assignmentReconciler.startLost();
    }

    @Override
    public void transitionToFailed() {
        transitionTo(MemberState.FAILED);
    }

    @Override
    public boolean shouldSendHeartbeat() {
        return state() != MemberState.FAILED;
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
            startReconciliation();
        }
        return state.equals(MemberState.STABLE);
    }

    private void startReconciliation() {
        if (!targetAssignment.isPresent())
            return;

        SortedSet<TopicPartition> targetPartitions = new TreeSet<>(new Utils.TopicPartitionComparator());

        for (ConsumerGroupHeartbeatResponseData.TopicPartitions topicPartitions : targetAssignment.get().topicPartitions()) {
            Uuid topicId = topicPartitions.topicId();
            String topicName = metadata.topicNames().get(topicId);

            // TODO... I don't think this is right...
            if (topicName == null)
                throw new UnknownTopicIdException("A topic name for the topic ID " + topicId + " was not found in the local metadata cache");

            for (Integer partition : topicPartitions.partitions()) {
                targetPartitions.add(new TopicPartition(topicName, partition));
            }
        }

        assignmentReconciler.startReconcile(targetPartitions);
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

    @Override
    public void completeReconcile(Set<TopicPartition> revokedPartitions,
                                  Set<TopicPartition> assignedPartitions,
                                  Optional<KafkaException> callbackError) {
        if (callbackError.isPresent()) {
            // TODO: how to react to callback errors?
        }

        assignmentReconciler.completeReconcile(revokedPartitions, assignedPartitions);
        transitionTo(MemberState.STABLE);
        // TODO: update state to signal the HeartbeatRequestManager to send an ACK heartbeat
    }

    @Override
    public void completeLost(Set<TopicPartition> lostPartitions, Optional<KafkaException> callbackError) {
        if (callbackError.isPresent()) {
            // TODO: how to react to callback errors?
        }

        assignmentReconciler.completeLost(lostPartitions);
        transitionTo(MemberState.UNJOINED);
        // TODO: update state to signal the HeartbeatRequestManager to send an ACK heartbeat
    }
}
