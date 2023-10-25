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
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
     * Logger.
     */
    private final Logger log;

    /**
     * For reconciling assignments.
     */
    private final AssignmentReconciler assignmentReconciler;

    /**
     * Metadata that allows us to create the partitions needed for {@link ConsumerRebalanceListener}.
     */
    private final ConsumerMetadata metadata;

    /**
     * Group ID of the consumer group the member will be part of, provided when creating the current
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
     * incremented as the member reconciles and acknowledges the assignments it receives. It will
     * be reset to 0 if the member gets fenced.
     */
    private int memberEpoch;

    /**
     * Current state of this member as part of the consumer group, as defined in {@link MemberState}
     */
    private MemberState state;

    /**
     * Name of the server-side assignor this member has configured to use. It will be sent
     * out to the server on the {@link ConsumerGroupHeartbeatRequest}. If not defined, the server
     * will select the assignor implementation to use.
     */
    private final Optional<String> serverAssignor;

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

    public MembershipManagerImpl(LogContext logContext,
                                 AssignmentReconciler assignmentReconciler,
                                 ConsumerMetadata metadata,
                                 String groupId,
                                 Optional<String> groupInstanceId,
                                 Optional<String> serverAssignor) {
        this.log = logContext.logger(MembershipManagerImpl.class);
        this.assignmentReconciler = assignmentReconciler;
        this.metadata = metadata;
        this.groupId = groupId;
        this.state = MemberState.UNJOINED;
        this.serverAssignor = serverAssignor;
        this.groupInstanceId = groupInstanceId;
        this.targetAssignment = Optional.empty();
        this.nextTargetAssignment = Optional.empty();
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
        log.trace("Member {} transitioned from {} to {}.", memberId, state, nextState);
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
            String errorMessage = String.format(
                    "Unexpected error in Heartbeat response. Expected no error, but received: %s",
                    Errors.forCode(response.errorCode())
            );
            throw new IllegalArgumentException(errorMessage);
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
    public void transitionToFenced() {
        resetEpoch();
        transitionTo(MemberState.FENCED);
        assignmentReconciler.startLost();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transitionToFailed() {
        log.error("Member {} transitioned to {} state", memberId, MemberState.FAILED);
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

    /**
     * Take new target assignment received from the server and set it as targetAssignment to be
     * processed. Following the consumer group protocol, the server won't send a new target
     * member while a previous one hasn't been acknowledged by the member, so this will fail
     * if a target assignment already exists.
     *
     * @throws IllegalStateException If a target assignment already exists.
     */
    private void setTargetAssignment(ConsumerGroupHeartbeatResponseData.Assignment newTargetAssignment) {
        if (!targetAssignment.isPresent()) {
            log.info("Member {} accepted new target assignment {} to reconcile", memberId, newTargetAssignment);
            targetAssignment = Optional.of(newTargetAssignment);
        } else {
            transitionToFailed();
            throw new IllegalStateException("Cannot set new target assignment because a " +
                    "previous one pending to be reconciled already exists.");
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
    public Optional<String> serverAssignor() {
        return this.serverAssignor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerGroupHeartbeatResponseData.Assignment currentAssignment() {
        return this.currentAssignment;
    }


    /**
     * @return Assignment that the member received from the server but hasn't completely processed
     * yet. Visible for testing.
     */
    @Override
    public Optional<ConsumerGroupHeartbeatResponseData.Assignment> targetAssignment() {
        return targetAssignment;
    }

    /**
     * This indicates that the reconciliation of the target assignment has been successfully
     * completed, so it will make it effective by assigning it to the current assignment.
     *
     * @params Assignment that has been successfully reconciled. This is expected to
     * match the target assignment defined in {@link #targetAssignment()}
     */
    @Override
    public void onTargetAssignmentProcessComplete(ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        if (assignment == null) {
            throw new IllegalArgumentException("Assignment cannot be null");
        }
        if (!assignment.equals(targetAssignment.orElse(null))) {
            // This could be simplified to remove the assignment param and just assume that what
            // was reconciled was the targetAssignment, but keeping it explicit and failing fast
            // here to uncover any issues in the interaction of the assignment processing logic
            // and this.
            throw new IllegalStateException(String.format("Reconciled assignment %s does not " +
                            "match the expected target assignment %s", assignment,
                    targetAssignment.orElse(null)));
        }
        this.currentAssignment = assignment;
        targetAssignment = Optional.empty();
        transitionTo(MemberState.STABLE);
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
