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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * The CurrentAssignmentBuilder class encapsulates the reconciliation engine of the
 * consumer group protocol. Given the current state of a member and a desired or target
 * assignment state, the state machine takes the necessary steps to converge them.
 *
 * State Machine:
 * - STABLE:
 *   The member is fully reconciled to the desired target assignment.
 *
 *   Valid Transitions:
 *   - STABLE -> STABLE
 *     When a new target assignment is installed but the assignment of the member
 *     has not changed, the member transitions to the next epoch and remains in
 *     the STABLE state.
 *
 *   - STABLE -> UNACKNOWLEDGED_ASSIGNMENT
 *     When a new target assignment is installed and a new assignment is computed
 *     for the member, he transitions to the UNACKNOWLEDGED_ASSIGNMENT state.
 *
 *     If the next assignment contains partitions to be revoked, the member stays
 *     in his current epoch. Otherwise, he transitions to the target epoch.
 *
 *   - STABLE -> UNRELEASED_PARTITIONS
 *     When a new target assignment is installed and all the newly assigned partitions
 *     are not available yet, he transitions to the UNRELEASED_PARTITIONS state
 *     and waits until at least one of them is available.
 *
 * - UNACKNOWLEDGED_ASSIGNMENT:
 *   The member has received a new assignment from the group coordinator but
 *   he has not acknowledged it yet. The member is removed from the group if
 *   he does not acknowledge it within the rebalance timeout.
 *
 *   Valid Transitions:
 *   - UNACKNOWLEDGED_ASSIGNMENT -> STABLE
 *     When the assignment is acknowledged, the member transitions to the STABLE
 *     state if it is fully reconciled.
 *
 *   - UNACKNOWLEDGED_ASSIGNMENT -> UNACKNOWLEDGED_ASSIGNMENT
 *     When the assignment is acknowledged, the member remains in the
 *     UNACKNOWLEDGED_ASSIGNMENT state if a new assignment is computed.
 *
 *     If the next assignment contains partitions to be revoked, the member stays
 *     in his current epoch. Otherwise, he transitions to the target epoch.
 *
 *   - UNACKNOWLEDGED_ASSIGNMENT -> UNRELEASED_PARTITIONS
 *     When the assignment is acknowledged, the member transitions to the
 *     UNRELEASED_PARTITIONS if newly assigned partitions are not available yet.
 *
 * - UNRELEASED_PARTITIONS:
 *   The member's reconciliation cannot progress because newly assigned partitions
 *   are still owned by other members in the group. They are not released yet.
 *
 *   Valid Transitions:
 *   - UNRELEASED_PARTITIONS -> STABLE
 *     The member may transition to the STABLE state if the partitions that he
 *     was awaiting on are no longer in the desired target assignment.
 *
 *   - UNRELEASED_PARTITIONS -> UNACKNOWLEDGED_ASSIGNMENT
 *     When at least one partition become available, a new assignment is computed
 *     for the member and he transitions to the UNACKNOWLEDGED_ASSIGNMENT state.
 *
 *     If the next assignment contains partitions to be revoked, the member stays
 *     in his current epoch. Otherwise, he transitions to the target epoch.
 *
 *   - UNRELEASED_PARTITIONS -> UNRELEASED_PARTITIONS
 *     The member remains in the UNRELEASED_PARTITIONS state until at least one
 *     partition become available.
 */
public class CurrentAssignmentBuilder {
    /**
     * The consumer group member which is reconciled.
     */
    private final ConsumerGroupMember member;

    /**
     * The target assignment epoch.
     */
    private int targetAssignmentEpoch;

    /**
     * The target assignment.
     */
    private Assignment targetAssignment;

    /**
     * A function which returns the current epoch of a topic-partition or -1 if the
     * topic-partition is not assigned. The current epoch is the epoch of the current owner.
     */
    private BiFunction<Uuid, Integer, Integer> currentPartitionEpoch;

    /**
     * The partitions owned by the consumer. This is directly provided by the member in the
     * ConsumerGroupHeartbeat request.
     */
    private List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions;

    /**
     * Constructs the CurrentAssignmentBuilder based on the current state of the
     * provided consumer group member.
     *
     * @param member The consumer group member that must be reconciled.
     */
    public CurrentAssignmentBuilder(ConsumerGroupMember member) {
        this.member = Objects.requireNonNull(member);
    }

    /**
     * Sets the target assignment epoch and the target assignment that the
     * consumer group member must be reconciled to.
     *
     * @param targetAssignmentEpoch The target assignment epoch.
     * @param targetAssignment      The target assignment.
     * @return This object.
     */
    public CurrentAssignmentBuilder withTargetAssignment(
        int targetAssignmentEpoch,
        Assignment targetAssignment
    ) {
        this.targetAssignmentEpoch = targetAssignmentEpoch;
        this.targetAssignment = Objects.requireNonNull(targetAssignment);
        return this;
    }

    /**
     * Sets a BiFunction which allows to retrieve the current epoch of a
     * partition. This is used by the state machine to determine if a
     * partition is free or still used by another member.
     *
     * @param currentPartitionEpoch A BiFunction which gets the epoch of a
     *                              topic id / partitions id pair.
     * @return This object.
     */
    public CurrentAssignmentBuilder withCurrentPartitionEpoch(
        BiFunction<Uuid, Integer, Integer> currentPartitionEpoch
    ) {
        this.currentPartitionEpoch = Objects.requireNonNull(currentPartitionEpoch);
        return this;
    }

    /**
     * Sets the partitions currently owned by the member. This comes directly
     * from the last ConsumerGroupHeartbeat request. This is used to determine
     * if the member has revoked the necessary partitions.
     *
     * @param ownedTopicPartitions A list of topic-partitions.
     * @return This object.
     */
    public CurrentAssignmentBuilder withOwnedTopicPartitions(
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) {
        this.ownedTopicPartitions = ownedTopicPartitions;
        return this;
    }

    /**
     * Builds the next state for the member or keep the current one if it
     * is not possible to move forward with the current state.
     *
     * @return A new ConsumerGroupMember or the current one.
     */
    public ConsumerGroupMember build() {
        switch (member.state()) {
            case STABLE:
                if (member.memberEpoch() != targetAssignmentEpoch) {
                    return computeNextAssignment();
                } else {
                    return member;
                }

            case UNACKNOWLEDGED_ASSIGNMENT:
                if (matchesAssignedPartitions(ownedTopicPartitions)) {
                    return computeNextAssignment();
                } else {
                    return member;
                }

            case UNRELEASED_PARTITIONS:
                return computeNextAssignment();
        }

        return member;
    }

    /**
     * Computes the next assignment.
     *
     * @return A new ConsumerGroupMember.
     */
    private ConsumerGroupMember computeNextAssignment() {
        boolean hasUnreleasedPartitions = false;
        Map<Uuid, Set<Integer>> newAssignedPartitions = new HashMap<>();
        Map<Uuid, Set<Integer>> newPartitionsPendingRevocation = new HashMap<>();
        Map<Uuid, Set<Integer>> newPartitionsPendingAssignment = new HashMap<>();

        Set<Uuid> allTopicIds = new HashSet<>(targetAssignment.partitions().keySet());
        allTopicIds.addAll(member.assignedPartitions().keySet());

        for (Uuid topicId : allTopicIds) {
            Set<Integer> target = targetAssignment.partitions()
                .getOrDefault(topicId, Collections.emptySet());
            Set<Integer> currentAssignedPartitions = member.assignedPartitions()
                .getOrDefault(topicId, Collections.emptySet());

            // Assigned_1 = Assigned_0 ∩ Target
            Set<Integer> assignedPartitions = new HashSet<>(currentAssignedPartitions);
            assignedPartitions.retainAll(target);

            // Pending_Revocation_1 = Assigned_0 - Assigned_1
            Set<Integer> partitionsPendingRevocation = new HashSet<>(currentAssignedPartitions);
            partitionsPendingRevocation.removeAll(assignedPartitions);

            // Pending_Assignment_1 = Target - Assigned_1 - Unreleased Partitions
            Set<Integer> partitionsPendingAssignment = new HashSet<>(target);
            partitionsPendingAssignment.removeAll(assignedPartitions);
            hasUnreleasedPartitions = partitionsPendingAssignment.removeIf(partitionId ->
                currentPartitionEpoch.apply(topicId, partitionId) != -1
            ) || hasUnreleasedPartitions;

            if (!assignedPartitions.isEmpty()) {
                newAssignedPartitions.put(topicId, assignedPartitions);
            }

            if (!partitionsPendingRevocation.isEmpty()) {
                newPartitionsPendingRevocation.put(topicId, partitionsPendingRevocation);
            }

            if (!partitionsPendingAssignment.isEmpty()) {
                newPartitionsPendingAssignment.put(topicId, partitionsPendingAssignment);
            }
        }

        if (!newPartitionsPendingRevocation.isEmpty()) {
            // If there are partitions to be revoked, the member remains in its current
            // epoch and requests the revocation of those partitions. It transitions to
            // the UNACKNOWLEDGED_ASSIGNMENT state to wait until the new assignment is
            // acknowledged.
            return new ConsumerGroupMember.Builder(member)
                .setState(ConsumerGroupMember.MemberState.UNACKNOWLEDGED_ASSIGNMENT)
                .setAssignedPartitions(newAssignedPartitions)
                .setRevokedPartitions(newPartitionsPendingRevocation)
                .build();
        } else if (!newPartitionsPendingAssignment.isEmpty()) {
            // If there are partitions to be assigned, the member transitions to the
            // target epoch and requests the assignment of those partitions. Note that
            // the partitions are directly added to the assigned partitions set. The
            // member transitions to the UNACKNOWLEDGED_ASSIGNMENT state to wait until
            // the new assignment is acknowledged.
            newPartitionsPendingAssignment.forEach((topicId, partitions) -> {
                newAssignedPartitions
                    .computeIfAbsent(topicId, __ -> new HashSet<>())
                    .addAll(partitions);
            });
            return new ConsumerGroupMember.Builder(member)
                .setState(ConsumerGroupMember.MemberState.UNACKNOWLEDGED_ASSIGNMENT)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setRevokedPartitions(Collections.emptyMap())
                .build();
        } else if (hasUnreleasedPartitions) {
            // If there are no partitions to be revoked nor to be assigned but some
            // partitions are not available yet, the member transitions to the target
            // epoch, to the UNRELEASED_PARTITIONS state and waits.
            return new ConsumerGroupMember.Builder(member)
                .setState(ConsumerGroupMember.MemberState.UNRELEASED_PARTITIONS)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setRevokedPartitions(Collections.emptyMap())
                .build();
        } else {
            // Otherwise, the member transitions to the target epoch and to the
            // STABLE state.
            return new ConsumerGroupMember.Builder(member)
                .setState(ConsumerGroupMember.MemberState.STABLE)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setRevokedPartitions(Collections.emptyMap())
                .build();
        }
    }

    /**
     * Checks whether the owned topic partitions passed by the member to the state
     * machine via the ConsumerGroupHeartbeat request corresponds to the assigned
     * partitions.
     *
     * @param ownedTopicPartitions The topic partitions owned by the remove client.
     * @return A boolean indicating if the owned partitions matches the Assigned set.
     */
    private boolean matchesAssignedPartitions(
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) {
        if (ownedTopicPartitions == null) return false;
        if (ownedTopicPartitions.size() != member.assignedPartitions().size()) return false;

        for (ConsumerGroupHeartbeatRequestData.TopicPartitions topicPartitions : ownedTopicPartitions) {
            Set<Integer> partitions = member.assignedPartitions().get(topicPartitions.topicId());
            if (partitions == null) return false;
            for (Integer partitionId : topicPartitions.partitions()) {
                if (!partitions.contains(partitionId)) return false;
            }
        }

        return true;
    }
}
