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
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The homogeneous uniform assignment builder is used to generate the target assignment for a consumer group with
 * all its members subscribed to the same set of topics.
 *
 * Assignments are done according to the following principles:
 *
 * <li> Balance:          Ensure partitions are distributed equally among all members.
 *                        The difference in assignments sizes between any two members
 *                        should not exceed one partition. </li>
 * <li> Stickiness:       Minimize partition movements among members by retaining
 *                        as much of the existing assignment as possible. </li>
 *
 * The assignment builder prioritizes the properties in the following order:
 *      Balance > Stickiness.
 */
public class UniformHomogeneousAssignmentBuilder {
    /**
     * The assignment specification which includes member metadata.
     */
    private final GroupSpec groupSpec;

    /**
     * The topic and partition metadata describer.
     */
    private final SubscribedTopicDescriber subscribedTopicDescriber;

    /**
     * The set of topic Ids that the consumer group is subscribed to.
     */
    private final Set<Uuid> subscribedTopicIds;

    /**
     * The members that are below their quota.
     */
    private final List<MemberWithRemainingQuota> unfilledMembers;

    /**
     * The partitions that still need to be assigned.
     * Initially this contains all the subscribed topics' partitions.
     */
    private final List<TopicIdPartition> unassignedPartitions;

    /**
     * The target assignment.
     */
    private final Map<String, MemberAssignment> targetAssignment;

    /**
     * The minimum number of partitions that a member must have.
     * Minimum quota = total partitions / total members.
     */
    private int minimumMemberQuota;

    /**
     * The number of members to receive an extra partition beyond the minimum quota.
     * Example: If there are 11 partitions to be distributed among 3 members,
     *          each member gets 3 (11 / 3) [minQuota] partitions and 2 (11 % 3) members get an extra partition.
     */
    private int remainingMembersToGetAnExtraPartition;

    UniformHomogeneousAssignmentBuilder(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.groupSpec = groupSpec;
        this.subscribedTopicDescriber = subscribedTopicDescriber;
        this.subscribedTopicIds = new HashSet<>(groupSpec.memberSubscription(groupSpec.memberIds().iterator().next())
            .subscribedTopicIds());
        this.unfilledMembers = new ArrayList<>();
        this.unassignedPartitions = new ArrayList<>();

        this.targetAssignment = new HashMap<>();
    }

    /**
     * Compute the new assignment for the group.
     */
    public GroupAssignment build() throws PartitionAssignorException {
        if (subscribedTopicIds.isEmpty()) {
            return new GroupAssignment(Collections.emptyMap());
        }

        // Compute the list of unassigned partitions.
        int totalPartitionsCount = 0;
        for (Uuid topicId : subscribedTopicIds) {
            int partitionCount = subscribedTopicDescriber.numPartitions(topicId);
            if (partitionCount == -1) {
                throw new PartitionAssignorException(
                    "Members are subscribed to topic " + topicId + " which doesn't exist in the topic metadata."
                );
            } else {
                for (int i = 0; i < partitionCount; i++) {
                    if (!groupSpec.isPartitionAssigned(topicId, i)) {
                        unassignedPartitions.add(new TopicIdPartition(topicId, i));
                    }
                }
                totalPartitionsCount += partitionCount;
            }
        }

        // Compute the minimum required quota per member and the number of members
        // that should receive an extra partition.
        int numberOfMembers = groupSpec.memberIds().size();
        minimumMemberQuota = totalPartitionsCount / numberOfMembers;
        remainingMembersToGetAnExtraPartition = totalPartitionsCount % numberOfMembers;

        // Revoke the partitions that either are not part of the member's subscriptions or
        // exceed the maximum quota assigned to each member.
        maybeRevokePartitions();

        // Assign the unassigned partitions to the members with space.
        assignRemainingPartitions();

        return new GroupAssignment(targetAssignment);
    }

    /**
     * Revoke the partitions that either are not part of the member's subscriptions or
     * exceed the maximum quota assigned to each member.
     *
     * This method ensures that the original assignment is not copied if it is not
     * altered.
     */
    private void maybeRevokePartitions() {
        for (String memberId : groupSpec.memberIds()) {
            Map<Uuid, Set<Integer>> oldAssignment = groupSpec.memberAssignment(memberId).partitions();
            Map<Uuid, Set<Integer>> newAssignment = null;

            // The assignor expects to receive the assignment as an immutable map. It leverages
            // this knowledge in order to avoid having to copy all assignments.
            if (!AssignorHelpers.isImmutableMap(oldAssignment)) {
                throw new IllegalStateException("The assignor expect an immutable map.");
            }

            int quota = minimumMemberQuota;
            if (remainingMembersToGetAnExtraPartition > 0) {
                quota++;
                remainingMembersToGetAnExtraPartition--;
            }

            for (Map.Entry<Uuid, Set<Integer>> topicPartitions : oldAssignment.entrySet()) {
                Uuid topicId = topicPartitions.getKey();
                Set<Integer> partitions = topicPartitions.getValue();

                if (subscribedTopicIds.contains(topicId)) {
                    if (partitions.size() <= quota) {
                        quota -= partitions.size();
                    } else {
                        for (Integer partition : partitions) {
                            if (quota > 0) {
                                quota--;
                            } else {
                                if (newAssignment == null) {
                                    // If the new assignment is null, we create a deep copy of the
                                    // original assignment so that we can alter it.
                                    newAssignment = AssignorHelpers.deepCopyAssignment(oldAssignment);
                                }
                                // Remove the partition from the new assignment.
                                Set<Integer> parts = newAssignment.get(topicId);
                                parts.remove(partition);
                                if (parts.isEmpty()) {
                                    newAssignment.remove(topicId);
                                }
                                // Add the partition to the unassigned set to be re-assigned later on.
                                unassignedPartitions.add(new TopicIdPartition(topicId, partition));
                            }
                        }
                    }
                } else {
                    if (newAssignment == null) {
                        // If the new assignment is null, we create a deep copy of the
                        // original assignment so that we can alter it.
                        newAssignment = AssignorHelpers.deepCopyAssignment(oldAssignment);
                    }
                    // Remove the entire topic.
                    newAssignment.remove(topicId);
                }
            }

            if (quota > 0) {
                unfilledMembers.add(new MemberWithRemainingQuota(memberId, quota));
            }

            if (newAssignment == null) {
                targetAssignment.put(memberId, new MemberAssignmentImpl(oldAssignment));
            } else {
                targetAssignment.put(memberId, new MemberAssignmentImpl(newAssignment));
            }
        }
    }

    /**
     * Assign the unassigned partitions to the unfilled members.
     */
    private void assignRemainingPartitions() {
        int unassignedPartitionIndex = 0;

        for (MemberWithRemainingQuota unfilledMember : unfilledMembers) {
            String memberId = unfilledMember.memberId;
            int remainingQuota = unfilledMember.remainingQuota;

            Map<Uuid, Set<Integer>> newAssignment = targetAssignment.get(memberId).partitions();
            if (AssignorHelpers.isImmutableMap(newAssignment)) {
                // If the new assignment is immutable, we must create a deep copy of it
                // before altering it.
                newAssignment = AssignorHelpers.deepCopyAssignment(newAssignment);
                targetAssignment.put(memberId, new MemberAssignmentImpl(newAssignment));
            }

            for (int i = 0; i < remainingQuota && unassignedPartitionIndex < unassignedPartitions.size(); i++) {
                TopicIdPartition unassignedTopicIdPartition = unassignedPartitions.get(unassignedPartitionIndex);
                unassignedPartitionIndex++;
                newAssignment
                    .computeIfAbsent(unassignedTopicIdPartition.topicId(), __ -> new HashSet<>())
                    .add(unassignedTopicIdPartition.partitionId());
            }
        }

        if (unassignedPartitionIndex < unassignedPartitions.size()) {
            throw new PartitionAssignorException("Partitions were left unassigned");
        }
    }

    private static class MemberWithRemainingQuota {
        final String memberId;
        final int remainingQuota;

        MemberWithRemainingQuota(
            String memberId,
            int remainingQuota
        ) {
            this.memberId = memberId;
            this.remainingQuota = remainingQuota;
        }
    }
}
