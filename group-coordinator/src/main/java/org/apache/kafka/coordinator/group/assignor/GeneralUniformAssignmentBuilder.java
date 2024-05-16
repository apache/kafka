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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.common.TopicIdPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * The general uniform assignment builder is used to generate the target assignment for a consumer group with
 * at least one of its members subscribed to a different set of topics.
 *
 * Assignments are done according to the following principles:
 *
 * <li> Balance:          Ensure partitions are distributed equally among all members.
 *                        The difference in assignments sizes between any two members
 *                        should not exceed one partition. </li>
 * <li> Stickiness:       Minimize partition movements among members by retaining
 *                        as much of the existing assignment as possible. </li>
 *
 * This assignment builder prioritizes the above properties in the following order:
 *      Balance > Stickiness.
 */
public class GeneralUniformAssignmentBuilder extends AbstractUniformAssignmentBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(GeneralUniformAssignmentBuilder.class);

    /**
     * The member metadata obtained from the assignment specification.
     */
    private final Map<String, AssignmentMemberSpec> members;

    /**
     * The topic and partition metadata describer.
     */
    private final SubscribedTopicDescriber subscribedTopicDescriber;

    /**
     * The list of all the topic Ids that the consumer group is subscribed to.
     */
    private final Set<Uuid> subscribedTopicIds;

    /**
     * List of subscribed members for each topic.
     */
    private final Map<Uuid, List<String>> membersPerTopic;

    /**
     * The new assignment that will be returned.
     */
    private final Map<String, MemberAssignment> targetAssignment;

    /**
     * The partitions that still need to be assigned.
     */
    private final Set<TopicIdPartition> unassignedPartitions;

    /**
     * All the partitions that have been retained from the existing assignment.
     */
    private final Set<TopicIdPartition> assignedStickyPartitions;

    /**
     * Manages assignments to members based on their current assignment size and maximum allowed assignment size.
     */
    private final AssignmentManager assignmentManager;

    /**
     * List of all the members sorted by their respective assignment sizes.
     */
    private final TreeSet<String> sortedMembersByAssignmentSize;

    /**
     * Tracks the owner of each partition in the target assignment.
     */
    private final Map<TopicIdPartition, String> partitionOwnerInTargetAssignment;

    /**
     * Handles all operations related to partition movements during a reassignment for balancing the target assignment.
     */
    private final PartitionMovements partitionMovements;

    public GeneralUniformAssignmentBuilder(AssignmentSpec assignmentSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.members = assignmentSpec.members();
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.subscribedTopicIds = new HashSet<>();
        this.membersPerTopic = new HashMap<>();
        this.targetAssignment = new HashMap<>();
        members.forEach((memberId, memberMetadata) ->
            memberMetadata.subscribedTopicIds().forEach(topicId -> {
                // Check if the subscribed topic exists.
                int partitionCount = subscribedTopicDescriber.numPartitions(topicId);
                if (partitionCount == -1) {
                    throw new PartitionAssignorException(
                        "Members are subscribed to topic " + topicId + " which doesn't exist in the topic metadata."
                    );
                }
                subscribedTopicIds.add(topicId);
                membersPerTopic.computeIfAbsent(topicId, k -> new ArrayList<>()).add(memberId);
                targetAssignment.put(memberId, new MemberAssignment(new HashMap<>()));
            })
        );
        this.unassignedPartitions = new HashSet<>(topicIdPartitions(subscribedTopicIds, subscribedTopicDescriber));
        this.assignedStickyPartitions = new HashSet<>();
        this.assignmentManager = new AssignmentManager(this.members, this.subscribedTopicDescriber);
        this.sortedMembersByAssignmentSize = assignmentManager.sortMembersByAssignmentSize(members.keySet());
        this.partitionOwnerInTargetAssignment = new HashMap<>();
        this.partitionMovements = new PartitionMovements();
    }

    /**
     * Here's the step-by-step breakdown of the assignment process:
     *
     * <li> Retain partitions from the existing assignments a.k.a sticky partitions. </li>
     * <li> Allocate all the remaining unassigned partitions to the members in a balanced manner.</li>
     * <li> Iterate through the assignment until it is balanced. </li>
     */
    @Override
    protected GroupAssignment buildAssignment() {
        if (subscribedTopicIds.isEmpty()) {
            LOG.info("The subscription list is empty, returning an empty assignment");
            return new GroupAssignment(Collections.emptyMap());
        }

        // All existing partitions are retained until max assignment size.
        assignStickyPartitions();

        unassignedPartitionsAssignment();

        balance();

        return new GroupAssignment(targetAssignment);
    }

    /**
     * <li> TopicIdPartitions are sorted in descending order based on the value:
     *       totalPartitions/number of subscribed members. </li>
     * <li> If the above value is the same then topicIdPartitions are sorted in
     *      ascending order of number of subscribers. </li>
     * <li> If both criteria are the same, sort in ascending order of the partition Id.
     *      This last criteria is for predictability of the assignments. </li>
     *
     * @param topicIdPartitions       The topic partitions that need to be sorted.
     * @return A list of sorted topic partitions.
     */
    private List<TopicIdPartition> sortTopicIdPartitions(Collection<TopicIdPartition> topicIdPartitions) {
        Comparator<TopicIdPartition> comparator = Comparator
            .comparingDouble((TopicIdPartition topicIdPartition) -> {
                int totalPartitions = subscribedTopicDescriber.numPartitions(topicIdPartition.topicId());
                int totalSubscribers = membersPerTopic.get(topicIdPartition.topicId()).size();
                return (double) totalPartitions / totalSubscribers;
            })
            .reversed()
            .thenComparingInt(topicIdPartition -> membersPerTopic.get(topicIdPartition.topicId()).size())
            .thenComparingInt(TopicIdPartition::partitionId);

        return topicIdPartitions.stream()
            .sorted(comparator)
            .collect(Collectors.toList());
    }

    /**
     * Gets a set of partitions that are to be retained from the existing assignment. This includes:
     * <li> Partitions from topics that are still present in both the new subscriptions and the topic metadata. </li>
     */
    private void assignStickyPartitions() {
        members.forEach((memberId, assignmentMemberSpec) ->
            assignmentMemberSpec.assignedPartitions().forEach((topicId, currentAssignment) -> {
                if (assignmentMemberSpec.subscribedTopicIds().contains(topicId)) {
                    currentAssignment.forEach(partition -> {
                        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, partition);
                        assignmentManager.addPartitionToTargetAssignment(topicIdPartition, memberId);
                        assignedStickyPartitions.add(topicIdPartition);
                    });
                } else {
                    LOG.debug("The topic " + topicId + " is no longer present in the subscribed topics list");
                }
            })
        );
    }

    /**
     * Allocates the remaining unassigned partitions to members in a balanced manner.
     * <li> Partitions are sorted to maximize the probability of a balanced assignment. </li>
     * <li> Sort members in ascending order of their current target assignment sizes
     *      to ensure the least filled member gets the partition first. </li>
     */
    private void unassignedPartitionsAssignment() {
        List<TopicIdPartition> sortedPartitions = sortTopicIdPartitions(unassignedPartitions);

        for (TopicIdPartition partition : sortedPartitions) {
            TreeSet<String> sortedMembers = assignmentManager.sortMembersByAssignmentSize(
                membersPerTopic.get(partition.topicId())
            );

            for (String member : sortedMembers) {
                if (assignmentManager.maybeAssignPartitionToMember(partition, member)) {
                    break;
                }
            }
        }
    }

    /**
     * If a topic has two or more potential members it is subject to reassignment.
     *
     * @return true if the topic can participate in reassignment, false otherwise.
     */
    private boolean canTopicParticipateInReassignment(Uuid topicId) {
        return membersPerTopic.get(topicId).size() >= 2;
    }

    /**
     * If a member is not assigned all its potential partitions it is subject to reassignment.
     * If any of the partitions assigned to a member is subject to reassignment, the member itself
     * is subject to reassignment.
     *
     * @return true if the member can participate in reassignment, false otherwise.
     */
    private boolean canMemberParticipateInReassignment(String memberId) {
        Set<Uuid> assignedTopicIds = targetAssignment.get(memberId).targetPartitions().keySet();

        int currentAssignmentSize = assignmentManager.targetAssignmentSize(memberId);
        int maxAssignmentSize = assignmentManager.maxAssignmentSize(memberId);

        if (currentAssignmentSize > maxAssignmentSize)
            LOG.error("The member {} is assigned more partitions than the maximum possible.", memberId);

        if (currentAssignmentSize < maxAssignmentSize)
            return true;

        for (Uuid topicId : assignedTopicIds) {
            if (canTopicParticipateInReassignment(topicId))
                return true;
        }
        return false;
    }

    /**
     * Checks if the current assignments of partitions to members is balanced.
     *
     * Balance is determined by first checking if the difference in the number of partitions assigned
     * to any two members is one. If this is not true, it verifies that no member can
     * receive additional partitions without disrupting the balance.
     *
     * @return true if the assignment is balanced, false otherwise.
     */

    private boolean isBalanced() {
        int min = assignmentManager.targetAssignmentSize(sortedMembersByAssignmentSize.first());
        int max = assignmentManager.targetAssignmentSize(sortedMembersByAssignmentSize.last());

        // If minimum and maximum numbers of partitions assigned to members differ by at most one return true.
        if (min >= max - 1)
            return true;

        // Ensure that members without a complete set of topic partitions cannot receive any additional partitions.
        // This maintains balance. Start by checking members with the fewest assigned partitions to see if they can take more.
        for (String member : sortedMembersByAssignmentSize) {
            int memberPartitionCount = assignmentManager.targetAssignmentSize(member);

            // Skip if this member already has all the topic partitions it can get.
            int maxAssignmentSize = assignmentManager.maxAssignmentSize(member);
            if (memberPartitionCount == maxAssignmentSize)
                continue;

            // Otherwise make sure it cannot get any more partitions.
            for (Uuid topicId : members.get(member).subscribedTopicIds()) {
                Set<Integer> assignedPartitions = targetAssignment.get(member).targetPartitions().get(topicId);
                for (int i = 0; i < subscribedTopicDescriber.numPartitions(topicId); i++) {
                    TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, i);
                    if (assignedPartitions == null || !assignedPartitions.contains(i)) {
                        String otherMember = partitionOwnerInTargetAssignment.get(topicIdPartition);
                        int otherMemberPartitionCount = assignmentManager.targetAssignmentSize(otherMember);
                        if (memberPartitionCount + 1 < otherMemberPartitionCount) {
                            LOG.debug("{} can be moved from member {} to member {} for a more balanced assignment.",
                                topicIdPartition, otherMember, member);
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * Balance the current assignment after the initial round of assignments have completed.
     */
    private void balance() {
        if (!unassignedPartitions.isEmpty())
            throw new PartitionAssignorException("Some partitions were left unassigned");
        // Refill unassigned partitions with all the topicId partitions.
        unassignedPartitions.addAll(topicIdPartitions(subscribedTopicIds, subscribedTopicDescriber));

        // Narrow down the reassignment scope to only those partitions that can actually be reassigned.
        Set<TopicIdPartition> fixedPartitions = new HashSet<>();
        for (Uuid topicId : subscribedTopicIds) {
            if (!canTopicParticipateInReassignment(topicId)) {
                for (int i = 0; i < subscribedTopicDescriber.numPartitions(topicId); i++) {
                    fixedPartitions.add(new TopicIdPartition(topicId, i));
                }
            }
        }
        unassignedPartitions.removeAll(fixedPartitions);

        // Narrow down the reassignment scope to only those members that are subject to reassignment.
        for (String member : members.keySet()) {
            if (!canMemberParticipateInReassignment(member)) {
                sortedMembersByAssignmentSize.remove(member);
            }
        }

        // If all the partitions are fixed i.e. unassigned partitions is empty there is no point of re-balancing.
        if (!unassignedPartitions.isEmpty()) performReassignments();
    }

    /**
     * Performs reassignments of partitions to balance the load across members.
     * This method iteratively reassigns partitions until no further moves can improve the balance.
     *
     * The method uses a do-while loop to ensure at least one pass over the partitions and continues
     * reassigning as long as there are modifications to the current assignments. It checks for balance
     * after each reassignment and exits if the balance is achieved.
     *
     * @throws PartitionAssignorException if there are inconsistencies in expected members per partition
     *         or if a partition is expected to already be assigned but isn't.
     */
    private void performReassignments() {
        boolean modified;
        boolean reassignmentOccurred;
        // Repeat reassignment until no partition can be moved to improve the balance.
        do {
            modified = false;
            reassignmentOccurred = false;
            // Reassign all reassignable partitions sorted in descending order
            // by totalPartitions/number of subscribed members,
            // until the full list is processed or a balance is achieved.
            List<TopicIdPartition> reassignablePartitions = sortTopicIdPartitions(unassignedPartitions);

            for (TopicIdPartition reassignablePartition : reassignablePartitions) {
                // Only check if there is any change in balance if any moves were made.
                if (reassignmentOccurred && isBalanced()) {
                    return;
                }
                reassignmentOccurred = false;

                // The topicIdPartition must have at least two members.
                if (membersPerTopic.get(reassignablePartition.topicId()).size() <= 1)
                    throw new PartitionAssignorException(String.format("Expected more than one potential member for " +
                        "topicIdPartition '%s'", reassignablePartition)
                    );

                // The topicIdPartition must have a current target owner.
                String currentTargetOwner = partitionOwnerInTargetAssignment.get(reassignablePartition);
                if (currentTargetOwner == null)
                    throw new PartitionAssignorException(String.format("Expected topicIdPartition '%s' to be assigned " +
                        "to a member", reassignablePartition)
                    );

                for (String otherMember : membersPerTopic.get(reassignablePartition.topicId())) {
                    if (assignmentManager.targetAssignmentSize(currentTargetOwner) > assignmentManager.targetAssignmentSize(otherMember) + 1) {
                        reassignPartition(reassignablePartition);
                        modified = true;
                        reassignmentOccurred = true;
                        break;
                    }
                }
            }
        } while (modified);
    }

    /**
     * Reassigns a partition to an eligible member with the fewest current target assignments.
     * <ul>
     *   <li> Iterates over members sorted by ascending assignment size. </li>
     *   <li> Selects the first member subscribed to the partition's topic. </li>
     * </ul>
     *
     * @param partition         The partition to reassign.
     * @throws AssertionError   If no subscribed member is found.
     */
    private void reassignPartition(TopicIdPartition partition) {
        // Find the new member with the least assignment size.
        String newOwner = null;
        for (String anotherMember : sortedMembersByAssignmentSize) {
            if (members.get(anotherMember).subscribedTopicIds().contains(partition.topicId())) {
                newOwner = anotherMember;
                break;
            }
        }

        if (newOwner == null) {
            throw new PartitionAssignorException("No suitable new owner was found for the partition" + partition);
        }

        reassignPartition(partition, newOwner);
    }

    /**
     * Reassigns the given partition to a new member while considering partition movements and stickiness.
     * <p>
     * This method performs the following actions:
     * <ol>
     *   <li> Determines the current owner of the partition. </li>
     *   <li> Identifies the correct partition to move, adhering to stickiness constraints. </li>
     *   <li> Processes the partition movement to the new member. </li>
     * </ol>
     *
     * @param partition     The {@link TopicIdPartition} to be reassigned.
     * @param newMember     The Id of the member to which the partition should be reassigned.
     */
    private void reassignPartition(TopicIdPartition partition, String newMember) {
        String member = partitionOwnerInTargetAssignment.get(partition);
        // Find the correct partition movement considering the stickiness requirement.
        TopicIdPartition partitionToBeMoved = partitionMovements.computeActualPartitionToBeMoved(
            partition,
            member,
            newMember
        );
        processPartitionMovement(partitionToBeMoved, newMember);
    }

    private void processPartitionMovement(TopicIdPartition topicIdPartition, String newMember) {
        String oldMember = partitionOwnerInTargetAssignment.get(topicIdPartition);

        partitionMovements.movePartition(topicIdPartition, oldMember, newMember);

        assignmentManager.removePartitionFromTargetAssignment(topicIdPartition, oldMember);
        assignmentManager.addPartitionToTargetAssignment(topicIdPartition, newMember);
    }

    /**
     * This class represents a pair of member Ids involved in a partition reassignment.
     * Each pair contains a source and a destination member Id.
     * It normally corresponds to a particular partition or topic, and indicates that the particular partition or some
     * partition of the particular topic was moved from the source member to the destination member during the rebalance.
     */
    private static class MemberPair {
        private final String srcMemberId;
        private final String dstMemberId;

        MemberPair(String srcMemberId, String dstMemberId) {
            this.srcMemberId = srcMemberId;
            this.dstMemberId = dstMemberId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.srcMemberId == null) ? 0 : this.srcMemberId.hashCode());
            result = prime * result + ((this.dstMemberId == null) ? 0 : this.dstMemberId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;

            if (!getClass().isInstance(obj))
                return false;

            MemberPair otherPair = (MemberPair) obj;
            return this.srcMemberId.equals(otherPair.srcMemberId) && this.dstMemberId.equals(otherPair.dstMemberId);
        }

        @Override
        public String toString() {
            return "MemberPair(" +
                "srcMemberId='" + srcMemberId + '\'' +
                ", dstMemberId='" + dstMemberId + '\'' +
                ')';
        }
    }

    /**
     * This class maintains some data structures to simplify lookup of partition movements among members.
     * During a partition rebalance, it keeps track of partition movements corresponding to each topic,
     * and also possible movement (in form a <code>MemberPair</code> object) for each partition.
     */
    private static class PartitionMovements {
        private final Map<Uuid, Map<MemberPair, Set<TopicIdPartition>>> partitionMovementsByTopic = new HashMap<>();
        private final Map<TopicIdPartition, MemberPair> partitionMovementsByPartition = new HashMap<>();

        private MemberPair removeMovementRecordOfPartition(TopicIdPartition partition) {
            MemberPair pair = partitionMovementsByPartition.remove(partition);

            Uuid topic = partition.topicId();
            Map<MemberPair, Set<TopicIdPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            partitionMovementsForThisTopic.get(pair).remove(partition);
            if (partitionMovementsForThisTopic.get(pair).isEmpty())
                partitionMovementsForThisTopic.remove(pair);
            if (partitionMovementsByTopic.get(topic).isEmpty())
                partitionMovementsByTopic.remove(topic);

            return pair;
        }

        private void addPartitionMovementRecord(TopicIdPartition partition, MemberPair pair) {
            partitionMovementsByPartition.put(partition, pair);

            Uuid topic = partition.topicId();
            if (!partitionMovementsByTopic.containsKey(topic))
                partitionMovementsByTopic.put(topic, new HashMap<>());

            Map<MemberPair, Set<TopicIdPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            if (!partitionMovementsForThisTopic.containsKey(pair))
                partitionMovementsForThisTopic.put(pair, new HashSet<>());

            partitionMovementsForThisTopic.get(pair).add(partition);
        }

        private void movePartition(TopicIdPartition partition, String oldOwner, String newOwner) {
            MemberPair pair = new MemberPair(oldOwner, newOwner);

            if (partitionMovementsByPartition.containsKey(partition)) {
                // This partition was previously moved.
                MemberPair existingPair = removeMovementRecordOfPartition(partition);
                if (existingPair.dstMemberId.equals(oldOwner)) {
                    throw new PartitionAssignorException("Mismatch in partition movement record with respect to " +
                        "partition ownership during a rebalance"
                    );
                }
                if (!existingPair.srcMemberId.equals(newOwner)) {
                    // The partition is not moving back to its previous member.
                    addPartitionMovementRecord(partition, new MemberPair(existingPair.srcMemberId, newOwner));
                }
            } else
                addPartitionMovementRecord(partition, pair);
        }

        /**
         * Computes the actual partition to be moved based on the current and proposed partition owners.
         * This method determines the appropriate partition movement, considering existing partition movements
         * and constraints within a topic.
         *
         * @param partition         The {@link TopicIdPartition} object representing the partition to be moved.
         * @param oldOwner          The memberId of the current owner of the partition.
         * @param newOwner          The memberId of the proposed new owner of the partition.
         * @return The {@link TopicIdPartition} that should be moved, based on existing movement patterns
         *         and ownership. Returns the original partition if no specific movement pattern applies.
         * @throws PartitionAssignorException if the old owner does not match the expected value for the partition.
         */
        private TopicIdPartition computeActualPartitionToBeMoved(
            TopicIdPartition partition,
            String oldOwner,
            String newOwner
        ) {
            Uuid topic = partition.topicId();

            if (!partitionMovementsByTopic.containsKey(topic))
                return partition;

            if (partitionMovementsByPartition.containsKey(partition)) {
                String expectedOldOwner = partitionMovementsByPartition.get(partition).dstMemberId;
                if (!oldOwner.equals(expectedOldOwner)) {
                    throw new PartitionAssignorException("Old owner does not match expected value for partition: " + partition);
                }
                oldOwner = partitionMovementsByPartition.get(partition).srcMemberId;
            }

            Map<MemberPair, Set<TopicIdPartition>> partitionMovementsForThisTopic = partitionMovementsByTopic.get(topic);
            MemberPair reversePair = new MemberPair(newOwner, oldOwner);
            if (!partitionMovementsForThisTopic.containsKey(reversePair))
                return partition;

            return partitionMovementsForThisTopic.get(reversePair).iterator().next();
        }
    }

    /**
     * Manages assignments to members based on their current assignment size and maximum allowed assignment size.
     */
    private class AssignmentManager {
        private final Map<String, MemberAssignmentData> membersWithAssignmentSizes = new HashMap<>();

        /**
         * Represents the assignment metadata for a member.
         */
        private class MemberAssignmentData {
            final String memberId;
            int currentAssignmentSize = 0;
            int maxAssignmentSize;

            /**
             * Constructs a MemberAssignmentData with the given member Id.
             *
             * @param memberId The Id of the member.
             */
            MemberAssignmentData(String memberId) {
                this.memberId = memberId;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                MemberAssignmentData that = (MemberAssignmentData) o;
                return memberId.equals(that.memberId);
            }

            @Override
            public int hashCode() {
                return Objects.hash(memberId);
            }

            @Override
            public String toString() {
                return "MemberAssignmentData(" +
                    "memberId='" + memberId + '\'' +
                    ", currentAssignmentSize=" + currentAssignmentSize +
                    ", maxAssignmentSize=" + maxAssignmentSize +
                    ')';
            }
        }

        /**
         * Initializes an AssignmentManager, setting up the necessary data structures.
         */
        public AssignmentManager(Map<String, AssignmentMemberSpec> members, SubscribedTopicDescriber subscribedTopicDescriber) {
            members.forEach((memberId, member) -> {
                int maxSize = member.subscribedTopicIds().stream()
                    .mapToInt(subscribedTopicDescriber::numPartitions)
                    .sum();

                MemberAssignmentData memberAssignmentData = membersWithAssignmentSizes
                    .computeIfAbsent(memberId, MemberAssignmentData::new);
                memberAssignmentData.maxAssignmentSize = maxSize;
                memberAssignmentData.currentAssignmentSize = 0;
            });
        }

        /**
         * @param memberId       The member Id.
         * @return The current assignment size for the given member.
         */
        private int targetAssignmentSize(String memberId) {
            MemberAssignmentData memberData = this.membersWithAssignmentSizes.get(memberId);
            if (memberData == null) {
                LOG.warn("Member Id {} not found", memberId);
                return 0;
            }
            return memberData.currentAssignmentSize;
        }

        /**
         * @param memberId      The member Id.
         * @return The maximum assignment size for the given member.
         */
        private int maxAssignmentSize(String memberId) {
            MemberAssignmentData memberData = this.membersWithAssignmentSizes.get(memberId);
            if (memberData == null) {
                LOG.warn("Member Id {} not found", memberId);
                return 0;
            }
            return memberData.maxAssignmentSize;
        }

        /**
         * @param memberId      The member Id.
         * @return If the given member is at maximum capacity.
         */
        private boolean isMemberAtMaxCapacity(String memberId) {
            return targetAssignmentSize(memberId) >= maxAssignmentSize(memberId);
        }

        /**
         * @param memberId      The member Id.
         * Increment the current target assignment size for the member.
         */
        private void incrementTargetAssignmentSize(String memberId) {
            MemberAssignmentData memberData = this.membersWithAssignmentSizes.get(memberId);
            if (memberData == null) {
                LOG.warn("Member Id {} not found", memberId);
                return;
            }
            memberData.currentAssignmentSize++;
        }

        /**
         * @param memberId      The member Id.
         * Decrement the current target assignment size for the member, if it's assignment size is greater than zero.
         */
        private void decrementTargetAssignmentSize(String memberId) {
            MemberAssignmentData memberData = this.membersWithAssignmentSizes.get(memberId);
            if (memberData == null) {
                LOG.warn("Member Id {} not found", memberId);
                return;
            }

            if (memberData.currentAssignmentSize > 0) {
                memberData.currentAssignmentSize--;
            }
        }

        /**
         * Assigns partition to member if eligible.
         *
         * @param topicIdPartition      The partition to be assigned.
         * @param memberId              The Id of the member.
         * @return true if the partition was assigned, false otherwise.
         */
        private boolean maybeAssignPartitionToMember(
            TopicIdPartition topicIdPartition,
            String memberId
        ) {
            // If member is not subscribed to the partition's topic, return false without assigning.
            if (!members.get(memberId).subscribedTopicIds().contains(topicIdPartition.topicId())) {
                return false;
            }

            // If the member's current assignment is already at max, return false without assigning.
            if (isMemberAtMaxCapacity(memberId)) {
                return false;
            }

            addPartitionToTargetAssignment(topicIdPartition, memberId);
            return true;
        }

        /**
         * Assigns a partition to a member, updates the current assignment size,
         * and updates relevant data structures.
         *
         * @param topicIdPartition      The partition to be assigned.
         * @param memberId              Member that the partition needs to be added to.
         */
        private void addPartitionToTargetAssignment(TopicIdPartition topicIdPartition, String memberId) {
            addPartitionToAssignment(
                targetAssignment,
                memberId,
                topicIdPartition.topicId(),
                topicIdPartition.partitionId()
            );

            partitionOwnerInTargetAssignment.put(topicIdPartition, memberId);
            // Remove the member's assignment data from the queue to update it.
            sortedMembersByAssignmentSize.remove(memberId);
            assignmentManager.incrementTargetAssignmentSize(memberId);

            // Update current assignment size and re-add to queue if needed.
            if (!isMemberAtMaxCapacity(memberId)) {
                sortedMembersByAssignmentSize.add(memberId);
            }

            unassignedPartitions.remove(topicIdPartition);
        }

        /**
         * Revokes the partition from a member, updates the current target assignment size,
         * and other relevant data structures.
         *
         * @param topicIdPartition      The partition to be revoked.
         * @param memberId              Member that the partition needs to be revoked from.
         */
        private void removePartitionFromTargetAssignment(TopicIdPartition topicIdPartition, String memberId) {
            Map<Uuid, Set<Integer>> targetPartitionsMap = targetAssignment.get(memberId).targetPartitions();
            Set<Integer> partitionsSet = targetPartitionsMap.get(topicIdPartition.topicId());
            // Remove the partition from the assignment, if there are no more partitions from a particular topic,
            // remove the topic from the assignment as well.
            if (partitionsSet != null) {
                partitionsSet.remove(topicIdPartition.partitionId());
                if (partitionsSet.isEmpty()) {
                    targetPartitionsMap.remove(topicIdPartition.topicId());
                }
            }

            partitionOwnerInTargetAssignment.remove(topicIdPartition, memberId);
            // Remove the member's assignment data from the set to update it.
            sortedMembersByAssignmentSize.remove(memberId);
            assignmentManager.decrementTargetAssignmentSize(memberId);

            // Update current assignment size and re-add to set if needed.
            if (!isMemberAtMaxCapacity(memberId)) {
                sortedMembersByAssignmentSize.add(memberId);
            }
        }

        /**
         * Sorts members in ascending order based on their current target assignment size.
         * Members that have reached their max assignment size are removed.
         *
         * @param memberIds     Member Ids that need to be sorted.
         * @return A set that maintains the order of members by assignment size.
         */
        private TreeSet<String> sortMembersByAssignmentSize(Collection<String> memberIds) {
            Comparator<String> comparator = Comparator
                .comparingInt((String memberId) -> membersWithAssignmentSizes.get(memberId).currentAssignmentSize)
                .thenComparing(memberId -> memberId);

            return memberIds.stream()
                .filter(memberId -> {
                    MemberAssignmentData memberData = membersWithAssignmentSizes.get(memberId);
                    return memberData.currentAssignmentSize < memberData.maxAssignmentSize;
                })
                .collect(Collectors.toCollection(() -> new TreeSet<>(comparator)));
        }
    }
}
