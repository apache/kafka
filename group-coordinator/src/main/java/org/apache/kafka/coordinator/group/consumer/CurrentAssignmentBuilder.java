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
import java.util.Iterator;
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
 * The member state has the following properties:
 * - Current Epoch:
 *   The current epoch of the member.
 *
 * - Next Epoch:
 *   The desired epoch of the member. It corresponds to the epoch of the target/desired assignment.
 *   The member transitions to this epoch when it has revoked the partitions that it does not own
 *   or if it does not have to revoke any.
 *
 * - Previous Epoch:
 *   The epoch of the member when the state was last updated.
 *
 * - Assigned Partitions:
 *   The set of partitions currently assigned to the member. This represents what the member should have.
 *
 * - Partitions Pending Revocation:
 *   The set of partitions that the member should revoke before it can transition to the next state.
 *
 * - Partitions Pending Assignment:
 *   The set of partitions that the member will eventually receive. The partitions in this set are
 *   still owned by other members in the group.
 *
 * The state machine has three states:
 * - REVOKING:
 *   This state means that the member must revoke partitions before it can transition to the next epoch
 *   and thus start receiving new partitions. This is to guarantee that offsets of revoked partitions
 *   are committed with the current epoch. The member transitions to the next state only when it has
 *   acknowledged the revocation.
 *
 * - ASSIGNING:
 *   This state means that the member waits on partitions which are still owned by other members in the
 *   group. It remains in this state until they are all freed up.
 *
 * - STABLE:
 *   This state means that the member has received all its assigned partitions.
 *
 * The reconciliation process is started or re-started whenever a new target assignment is installed;
 * the epoch of the new target assignment is different from the next epoch of the member. In this transient
 * state, the assigned partitions, the partitions pending revocation and the partitions pending assignment
 * are updated. If the partitions pending revocation is not empty, the state machine transitions to
 * REVOKING; if partitions pending assignment is not empty, it transitions to ASSIGNING; otherwise it
 * transitions to STABLE.
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
        // A new target assignment has been installed, we need to restart
        // the reconciliation loop from the beginning.
        if (targetAssignmentEpoch != member.targetMemberEpoch()) {
            return transitionToNewTargetAssignmentState();
        }

        switch (member.state()) {
            // Check if the partitions have been revoked by the member.
            case REVOKING:
                return maybeTransitionFromRevokingToAssigningOrStable();

            // Check if pending partitions have been freed up.
            case ASSIGNING:
                return maybeTransitionFromAssigningToAssigningOrStable();

            // Nothing to do.
            case STABLE:
                return member;
        }

        return member;
    }

    /**
     * Transitions to NewTargetAssignment state. This is a transient state where
     * we compute the assigned partitions, the partitions pending revocation,
     * the partitions pending assignment, and transition to the next state.
     *
     * @return A new ConsumerGroupMember.
     */
    private ConsumerGroupMember transitionToNewTargetAssignmentState() {
        Map<Uuid, Set<Integer>> newAssignedPartitions = new HashMap<>();
        Map<Uuid, Set<Integer>> newPartitionsPendingRevocation = new HashMap<>();
        Map<Uuid, Set<Integer>> newPartitionsPendingAssignment = new HashMap<>();

        // Compute the combined set of topics.
        Set<Uuid> allTopicIds = new HashSet<>(targetAssignment.partitions().keySet());
        allTopicIds.addAll(member.assignedPartitions().keySet());
        allTopicIds.addAll(member.partitionsPendingRevocation().keySet());
        allTopicIds.addAll(member.partitionsPendingAssignment().keySet());

        for (Uuid topicId : allTopicIds) {
            Set<Integer> target = targetAssignment.partitions()
                .getOrDefault(topicId, Collections.emptySet());
            Set<Integer> currentAssignedPartitions = member.assignedPartitions()
                .getOrDefault(topicId, Collections.emptySet());
            Set<Integer> currentRevokingPartitions = member.partitionsPendingRevocation()
                .getOrDefault(topicId, Collections.emptySet());

            // Assigned_1 = (Assigned_0 + Pending_Revocation_0) ∩ Target
            // Assigned_0 + Pending_Revocation_0 is used here because the partitions
            // being revoked are still owned until the revocation is acknowledged.
            Set<Integer> assignedPartitions = new HashSet<>(currentAssignedPartitions);
            assignedPartitions.addAll(currentRevokingPartitions);
            assignedPartitions.retainAll(target);

            // Pending_Revocation_1 = (Assigned_0 + Pending_Revocation_0) - Assigned_1
            // Assigned_0 + Pending_Revocation_0 is used here because the partitions
            // being revoked are still owned until the revocation is acknowledged.
            Set<Integer> partitionsPendingRevocation = new HashSet<>(currentAssignedPartitions);
            partitionsPendingRevocation.addAll(currentRevokingPartitions);
            partitionsPendingRevocation.removeAll(assignedPartitions);

            // Pending_Assignment_1 = Target - Assigned_1
            Set<Integer> partitionsPendingAssignment = new HashSet<>(target);
            partitionsPendingAssignment.removeAll(assignedPartitions);

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
            // If the partition pending revocation set is not empty, we transition the
            // member to revoking and keep the current epoch. The transition to the new
            // state is done when the member is updated.
            return new ConsumerGroupMember.Builder(member)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(newPartitionsPendingRevocation)
                .setPartitionsPendingAssignment(newPartitionsPendingAssignment)
                .setTargetMemberEpoch(targetAssignmentEpoch)
                .build();
        } else {
            if (!newPartitionsPendingAssignment.isEmpty()) {
                // If the partitions pending assignment set is not empty, we check
                // if some or all partitions are free to use. If they are, we move
                // them to the partitions assigned set.
                maybeAssignPendingPartitions(newAssignedPartitions, newPartitionsPendingAssignment);
            }

            // We transition to the target epoch. If the partitions pending assignment
            // set is empty, the member transition to stable, otherwise to assigning.
            // The transition to the new state is done when the member is updated.
            return new ConsumerGroupMember.Builder(member)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(Collections.emptyMap())
                .setPartitionsPendingAssignment(newPartitionsPendingAssignment)
                .setPreviousMemberEpoch(member.memberEpoch())
                .setMemberEpoch(targetAssignmentEpoch)
                .setTargetMemberEpoch(targetAssignmentEpoch)
                .build();
        }
    }

    /**
     * Tries to transition from Revoke to Assigning or Stable. This is only
     * possible when the member acknowledges that it only owns the partition
     * in the assigned partitions.
     *
     * @return A new ConsumerGroupMember with the new state or the current one
     *         if the member stays in the current state.
     */
    private ConsumerGroupMember maybeTransitionFromRevokingToAssigningOrStable() {
        if (member.partitionsPendingRevocation().isEmpty() || matchesAssignedPartitions(ownedTopicPartitions)) {
            Map<Uuid, Set<Integer>> newAssignedPartitions = deepCopy(member.assignedPartitions());
            Map<Uuid, Set<Integer>> newPartitionsPendingAssignment = deepCopy(member.partitionsPendingAssignment());

            if (!newPartitionsPendingAssignment.isEmpty()) {
                // If the partitions pending assignment set is not empty, we check
                // if some or all partitions are free to use. If they are, we move
                // them to the assigned set.
                maybeAssignPendingPartitions(newAssignedPartitions, newPartitionsPendingAssignment);
            }

            // We transition to the target epoch. If the partitions pending assignment
            // set is empty, the member transition to stable, otherwise to assigning.
            // The transition to the new state is done when the member is updated.
            return new ConsumerGroupMember.Builder(member)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(Collections.emptyMap())
                .setPartitionsPendingAssignment(newPartitionsPendingAssignment)
                .setPreviousMemberEpoch(member.memberEpoch())
                .setMemberEpoch(targetAssignmentEpoch)
                .setTargetMemberEpoch(targetAssignmentEpoch)
                .build();
        } else {
            return member;
        }
    }

    /**
     * Tries to transition from Assigning to Assigning or Stable. This is only
     * possible when one or more partitions in the partitions pending assignment
     * set have been freed up by other members in the group.
     *
     * @return A new ConsumerGroupMember with the new state or the current one
     *         if the member stays in the current state.
     */
    private ConsumerGroupMember maybeTransitionFromAssigningToAssigningOrStable() {
        Map<Uuid, Set<Integer>> newAssignedPartitions = deepCopy(member.assignedPartitions());
        Map<Uuid, Set<Integer>> newPartitionsPendingAssignment = deepCopy(member.partitionsPendingAssignment());

        // If any partition can transition from assigning to assigned, we update
        // the member. Otherwise, we return the current one. The transition to the
        // new state is done when the member is updated.
        if (maybeAssignPendingPartitions(newAssignedPartitions, newPartitionsPendingAssignment)) {
            return new ConsumerGroupMember.Builder(member)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(Collections.emptyMap())
                .setPartitionsPendingAssignment(newPartitionsPendingAssignment)
                .setPreviousMemberEpoch(member.memberEpoch())
                .setMemberEpoch(targetAssignmentEpoch)
                .setTargetMemberEpoch(targetAssignmentEpoch)
                .build();
        } else {
            return member;
        }
    }

    /**
     * Tries to move partitions from the partitions pending assignment set to
     * the partitions assigned set if they are no longer owned.
     *
     * @param newAssignedPartitions             The assigned partitions.
     * @param newPartitionsPendingAssignment    The partitions pending assignment.
     * @return A boolean indicating if any partitions were moved.
     */
    private boolean maybeAssignPendingPartitions(
        Map<Uuid, Set<Integer>> newAssignedPartitions,
        Map<Uuid, Set<Integer>> newPartitionsPendingAssignment
    ) {
        boolean changed = false;

        Iterator<Map.Entry<Uuid, Set<Integer>>> assigningSetIterator =
            newPartitionsPendingAssignment.entrySet().iterator();

        while (assigningSetIterator.hasNext()) {
            Map.Entry<Uuid, Set<Integer>> pair = assigningSetIterator.next();
            Uuid topicId = pair.getKey();
            Set<Integer> assigning = pair.getValue();

            Iterator<Integer> assigningIterator = assigning.iterator();
            while (assigningIterator.hasNext()) {
                Integer partitionId = assigningIterator.next();

                // A partition can be assigned to this member iff it has been
                // released by its previous owner. This is signaled by -1.
                Integer partitionEpoch = currentPartitionEpoch.apply(topicId, partitionId);
                if (partitionEpoch == -1) {
                    assigningIterator.remove();
                    put(newAssignedPartitions, topicId, partitionId);
                    changed = true;
                }
            }

            if (assigning.isEmpty()) {
                assigningSetIterator.remove();
            }
        }

        return changed;
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

    /**
     * Makes a deep copy of an assignment map.
     *
     * @param map The Map to copy.
     * @return The copy.
     */
    private Map<Uuid, Set<Integer>> deepCopy(Map<Uuid, Set<Integer>> map) {
        Map<Uuid, Set<Integer>> copy = new HashMap<>();
        map.forEach((topicId, partitions) -> copy.put(topicId, new HashSet<>(partitions)));
        return copy;
    }

    /**
     * Puts the given TopicId and Partitions to the given map.
     */
    private void put(
        Map<Uuid, Set<Integer>> map,
        Uuid topicId,
        Integer partitionId
    ) {
        map.compute(topicId, (__, partitionsOrNull) -> {
            if (partitionsOrNull == null) partitionsOrNull = new HashSet<>();
            partitionsOrNull.add(partitionId);
            return partitionsOrNull;
        });
    }
}
