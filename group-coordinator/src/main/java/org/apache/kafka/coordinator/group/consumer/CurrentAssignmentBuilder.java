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
import org.apache.kafka.common.errors.FencedMemberEpochException;
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
                // When the member is in the STABLE state, we verify if a newer
                // epoch (or target assignment) is available. If it is, we can
                // reconcile the member towards it. Otherwise, we return.
                if (member.memberEpoch() != targetAssignmentEpoch) {
                    return computeNextAssignment(
                        member.memberEpoch(),
                        member.assignedPartitions()
                    );
                } else {
                    return member;
                }

            case UNREVOKED_PARTITIONS:
                // When the member is in the UNREVOKED_PARTITIONS state, we wait
                // until the member has revoked the necessary partitions. They are
                // considered revoked when they are not anymore reported in the
                // owned partitions set in the ConsumerGroupHeartbeat API.

                // If the member does not provide its owned partitions. We cannot
                // progress.
                if (ownedTopicPartitions == null) {
                    return member;
                }

                // If the member provides its owned partitions. We verify if it still
                // owns any of the revoked partitions. If it does, we cannot progress.
                for (ConsumerGroupHeartbeatRequestData.TopicPartitions topicPartitions : ownedTopicPartitions) {
                    for (Integer partitionId : topicPartitions.partitions()) {
                        boolean stillHasRevokedPartition = member
                            .partitionsPendingRevocation()
                            .getOrDefault(topicPartitions.topicId(), Collections.emptySet())
                            .contains(partitionId);
                        if (stillHasRevokedPartition) {
                            return member;
                        }
                    }
                }

                // When the member has revoked all the pending partitions, it can
                // transition to the next epoch (current + 1) and we can reconcile
                // its state towards the latest target assignment.
                return computeNextAssignment(
                    member.memberEpoch() + 1,
                    member.assignedPartitions()
                );

            case UNRELEASED_PARTITIONS:
                // When the member is in the UNRELEASED_PARTITIONS, we reconcile the
                // member towards the latest target assignment. This will assign any
                // of the unreleased partitions when they become available.
                return computeNextAssignment(
                    member.memberEpoch(),
                    member.assignedPartitions()
                );

            case UNKNOWN:
                // We could only end up in this state if a new state is added in the
                // future and the group coordinator is downgraded. In this case, the
                // best option is to fence the member to force it to rejoin the group
                // without any partitions and to reconcile it again from scratch.
                if (ownedTopicPartitions == null || !ownedTopicPartitions.isEmpty()) {
                    throw new FencedMemberEpochException("The consumer group member is in a unknown state. "
                        + "The member must abandon all its partitions and rejoin.");
                }

                return computeNextAssignment(
                    targetAssignmentEpoch,
                    member.assignedPartitions()
                );
        }

        return member;
    }

    /**
     * Computes the next assignment.
     *
     * @param memberEpoch               The epoch of the member to use. This may be different
     *                                  from the epoch in {@link CurrentAssignmentBuilder#member}.
     * @param memberAssignedPartitions  The assigned partitions of the member to use.
     * @return A new ConsumerGroupMember.
     */
    private ConsumerGroupMember computeNextAssignment(
        int memberEpoch,
        Map<Uuid, Set<Integer>> memberAssignedPartitions
    ) {
        boolean hasUnreleasedPartitions = false;
        Map<Uuid, Set<Integer>> newAssignedPartitions = new HashMap<>();
        Map<Uuid, Set<Integer>> newPartitionsPendingRevocation = new HashMap<>();
        Map<Uuid, Set<Integer>> newPartitionsPendingAssignment = new HashMap<>();

        Set<Uuid> allTopicIds = new HashSet<>(targetAssignment.partitions().keySet());
        allTopicIds.addAll(memberAssignedPartitions.keySet());

        for (Uuid topicId : allTopicIds) {
            Set<Integer> target = targetAssignment.partitions()
                .getOrDefault(topicId, Collections.emptySet());
            Set<Integer> currentAssignedPartitions = memberAssignedPartitions
                .getOrDefault(topicId, Collections.emptySet());

            // New Assigned Partitions = Previous Assigned Partitions ∩ Target
            Set<Integer> assignedPartitions = new HashSet<>(currentAssignedPartitions);
            assignedPartitions.retainAll(target);

            // Partitions Pending Revocation = Previous Assigned Partitions - New Assigned Partitions
            Set<Integer> partitionsPendingRevocation = new HashSet<>(currentAssignedPartitions);
            partitionsPendingRevocation.removeAll(assignedPartitions);

            // Partitions Pending Assignment = Target - New Assigned Partitions - Unreleased Partitions
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
            // the UNREVOKED_PARTITIONS state to wait until the client acknowledges the
            // revocation of the partitions.
            return new ConsumerGroupMember.Builder(member)
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .updateMemberEpoch(memberEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(newPartitionsPendingRevocation)
                .build();
        } else if (!newPartitionsPendingAssignment.isEmpty()) {
            // If there are partitions to be assigned, the member transitions to the
            // target epoch and requests the assignment of those partitions. Note that
            // the partitions are directly added to the assigned partitions set. The
            // member transitions to the STABLE state or to the UNRELEASED_PARTITIONS
            // state depending on whether there are unreleased partitions or not.
            newPartitionsPendingAssignment.forEach((topicId, partitions) -> newAssignedPartitions
                .computeIfAbsent(topicId, __ -> new HashSet<>())
                .addAll(partitions));
            MemberState newState = hasUnreleasedPartitions ? MemberState.UNRELEASED_PARTITIONS : MemberState.STABLE;
            return new ConsumerGroupMember.Builder(member)
                .setState(newState)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(Collections.emptyMap())
                .build();
        } else if (hasUnreleasedPartitions) {
            // If there are no partitions to be revoked nor to be assigned but some
            // partitions are not available yet, the member transitions to the target
            // epoch, to the UNRELEASED_PARTITIONS state and waits.
            return new ConsumerGroupMember.Builder(member)
                .setState(MemberState.UNRELEASED_PARTITIONS)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(Collections.emptyMap())
                .build();
        } else {
            // Otherwise, the member transitions to the target epoch and to the
            // STABLE state.
            return new ConsumerGroupMember.Builder(member)
                .setState(MemberState.STABLE)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(Collections.emptyMap())
                .build();
        }
    }
}
