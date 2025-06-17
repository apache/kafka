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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.assignor.SimpleAssignor.computeTargetPartitions;
import static org.apache.kafka.coordinator.group.assignor.SimpleAssignor.currentAssignment;
import static org.apache.kafka.coordinator.group.assignor.SimpleAssignor.newHashMap;
import static org.apache.kafka.coordinator.group.assignor.SimpleAssignor.newHashSet;

/**
 * The homogeneous simple assignment builder is used to generate the target assignment for a share group with
 * all its members subscribed to the same set of topics.
 * <p>
 * Assignments are done according to the following principles:
 * <ol>
 *   <li>Balance:          Ensure partitions are distributed equally among all members.
 *                         The difference in assignments sizes between any two members
 *                         should not exceed one partition.</li>
 *   <li>Stickiness:       Minimize partition movements among members by retaining
 *                         as much of the existing assignment as possible.</li>
 * </ol>
 * <p>
 * Balance is prioritized above stickiness.
 */
public class SimpleHomogeneousAssignmentBuilder {

    /**
     * The group metadata specification.
     */
    private final GroupSpec groupSpec;

    /**
     * The list of all the topic Ids that the share group is subscribed to.
     */
    private final Set<Uuid> subscribedTopicIds;

    /**
     * The list of all the topic-partitions assignable for the share group.
     */
    private final List<TopicIdPartition> targetPartitions;

    /**
     * The current assignment from topic partition to members.
     */
    private final Map<TopicIdPartition, List<String>> currentAssignment;

    /**
     * The number of members in the share group.
     */
    private final int numGroupMembers;

    /**
     * The desired sharing for each target partition.
     * For entirely balanced assignment, we would expect (numTargetPartitions / numGroupMembers) partitions per member, rounded upwards.
     * That can be expressed as:  Math.ceil(numTargetPartitions / (double) numGroupMembers)
     */
    private final int desiredSharing;

    /**
     * The desired number of assignments for each share group member.
     */
    private final Map<String, Integer> desiredAssignmentCount;

    /**
     * The share group assignment from the group metadata specification at the start of the assignment operation.
     */
    private final Map<String, Map<Uuid, Set<Integer>>> oldGroupAssignment;

    /**
     * The share group assignment calculated iteratively by the assignment operation. Entries in this map override those
     * in the old group assignment map.
     */
    private final Map<String, Map<Uuid, Set<Integer>>> newGroupAssignment;

    /**
     * The final assignment keyed by topic-partition.
     */
    private final Map<TopicIdPartition, Set<String>> finalAssignmentByPartition;

    /**
     * The final assignment keyed by member ID.
     */
    private final Map<String, Set<TopicIdPartition>> finalAssignmentByMember;

    /**
     * The set of members which have too few assigned partitions.
     */
    private final Set<String> unfilledMembers;

    /**
     * The set of members which have too many assigned partitions.
     */
    private final Set<String> overfilledMembers;

    public SimpleHomogeneousAssignmentBuilder(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.groupSpec = groupSpec;
        this.subscribedTopicIds = groupSpec.memberSubscription(groupSpec.memberIds().iterator().next()).subscribedTopicIds();

        this.targetPartitions = computeTargetPartitions(groupSpec, subscribedTopicIds, subscribedTopicDescriber);

        this.currentAssignment = currentAssignment(groupSpec);

        this.numGroupMembers = groupSpec.memberIds().size();
        int numTargetPartitions = targetPartitions.size();
        if (numTargetPartitions == 0) {
            this.desiredSharing = 0;
        } else {
            this.desiredSharing = (numGroupMembers + numTargetPartitions - 1) / numTargetPartitions;
        }
        this.desiredAssignmentCount = newHashMap(numGroupMembers);
        this.oldGroupAssignment = newHashMap(numGroupMembers);
        this.newGroupAssignment = newHashMap(numGroupMembers);
        this.finalAssignmentByPartition = newHashMap(numTargetPartitions);
        this.finalAssignmentByMember = newHashMap(numGroupMembers);
        this.unfilledMembers = newHashSet(numGroupMembers);
        this.overfilledMembers = newHashSet(numGroupMembers);

        // Extract the old group assignment from the group metadata specification.
        groupSpec.memberIds().forEach(memberId -> oldGroupAssignment.put(memberId, groupSpec.memberAssignment(memberId).partitions()));

        // Calculate the desired number of assignments for each member.
        // The precise desired assignment count per target partition. This can be a fractional number.
        // We would expect (numGroupMembers / numTargetPartitions) assignments per partition, rounded upwards.
        // Using integer arithmetic:  (numGroupMembers + numTargetPartitions - 1) / numTargetPartitions
        double preciseDesiredAssignmentCount = desiredSharing * numTargetPartitions / (double) numGroupMembers;
        int membersAnalyzed = 0;
        for (Map.Entry<String, Map<Uuid, Set<Integer>>> entry : oldGroupAssignment.entrySet()) {
            String memberId = entry.getKey();
            int desiredAssignmentCountForMember = (int) Math.ceil(preciseDesiredAssignmentCount * (double) (++membersAnalyzed)) -
                (int) Math.ceil(preciseDesiredAssignmentCount * (double) (membersAnalyzed - 1));
            desiredAssignmentCount.put(memberId, desiredAssignmentCountForMember);
        }
    }

    /**
     * Here's the step-by-step breakdown of the assignment process:
     * <ol>
     *   <li>Revoke partitions from the existing assignment that are no longer part of each member's
     *     subscriptions.</li>
     *   <li>Revoke any partitions which are shared more than desired.</li>
     *   <li>Revoke partitions from members which have too many partitions.</li>
     *   <li>Assign any partitions which have insufficient members assigned.</li>
     * </ol>
     */
    public GroupAssignment build() {
        if (subscribedTopicIds.isEmpty()) {
            return new GroupAssignment(Map.of());
        }

        revokeUnsubscribedPartitions();

        maybeRevokeOversharedPartitions();

        maybeRevokeOverfilledMembers();

        // Add in any partitions which are currently not in the assignment.
        targetPartitions.forEach(topicPartition -> finalAssignmentByPartition.computeIfAbsent(topicPartition, k -> new HashSet<>()));

        assignRemainingPartitions();

        // Combine the old and the new group assignments to give the result.
        Map<String, MemberAssignment> targetAssignment = newHashMap(numGroupMembers);
        groupSpec.memberIds().forEach(memberId -> {
            Map<Uuid, Set<Integer>> memberAssignment = newGroupAssignment.get(memberId);
            if (memberAssignment == null) {
                targetAssignment.put(memberId, new MemberAssignmentImpl(oldGroupAssignment.get(memberId)));
            } else {
                targetAssignment.put(memberId, new MemberAssignmentImpl(memberAssignment));
            }
        });

        return new GroupAssignment(targetAssignment);
    }

    /**
     * Examine the members from the current assignment, making sure that no member has too many assigned partitions.
     * When looking at the current assignment, we need to only consider the topics in the current assignment that are
     * also being subscribed in the new assignment.
     */
    private void revokeUnsubscribedPartitions() {
        for (Map.Entry<String, Map<Uuid, Set<Integer>>> entry : oldGroupAssignment.entrySet()) {
            String memberId = entry.getKey();
            Map<Uuid, Set<Integer>> oldMemberAssignment = entry.getValue();
            Map<Uuid, Set<Integer>> newMemberAssignment = null;
            int memberAssignedPartitions = 0;
            int desiredAssignmentCountForMember = desiredAssignmentCount.get(memberId);

            for (Map.Entry<Uuid, Set<Integer>> oldMemberPartitions : oldMemberAssignment.entrySet()) {
                Uuid topicId = oldMemberPartitions.getKey();
                Set<Integer> assignedPartitions = oldMemberPartitions.getValue();

                if (subscribedTopicIds.contains(topicId)) {
                    for (int partition : assignedPartitions) {
                        TopicIdPartition topicPartition = new TopicIdPartition(topicId, partition);
                        memberAssignedPartitions++;
                        finalAssignmentByPartition.computeIfAbsent(topicPartition, k -> new HashSet<>()).add(memberId);
                        finalAssignmentByMember.computeIfAbsent(memberId, k -> new HashSet<>()).add(topicPartition);
                        if (memberAssignedPartitions >= desiredAssignmentCountForMember) {
                            if (newMemberAssignment == null) {
                                // If the new assignment is null, we create a deep copy of the
                                // original assignment so that we can alter it.
                                newMemberAssignment = AssignorHelpers.deepCopyAssignment(oldMemberAssignment);
                            }
                        }
                    }
                } else {
                    if (newMemberAssignment == null) {
                        // If the new member assignment is null, we create a deep copy of the
                        // original assignment so we can alter it.
                        newMemberAssignment = AssignorHelpers.deepCopyAssignment(oldMemberAssignment);
                    }
                    // Remove the entire topic.
                    newMemberAssignment.remove(topicId);
                }
            }

            if (newMemberAssignment != null) {
                newGroupAssignment.put(memberId, newMemberAssignment);
            }

            if (memberAssignedPartitions < desiredAssignmentCountForMember) {
                unfilledMembers.add(memberId);
            } else if (memberAssignedPartitions > desiredAssignmentCountForMember) {
                overfilledMembers.add(memberId);
            }
        }
    }

    /**
     * Revoke any over-shared partitions. Prefer to revoke from overfilled members first.
     */
    private void maybeRevokeOversharedPartitions() {
        // Remove any over-shared partitions.
        currentAssignment.forEach((topicPartition, assignedMembers) -> {
            int assignedMemberCount = assignedMembers.size();
            if (assignedMemberCount > desiredSharing) {
                for (String memberId : assignedMembers) {
                    if (overfilledMembers.contains(memberId)) {
                        newGroupAssignment.get(memberId).get(topicPartition.topicId()).remove(topicPartition.partitionId());
                        assignedMemberCount--;
                        finalAssignmentByPartition.get(topicPartition).remove(memberId);
                        finalAssignmentByMember.get(memberId).remove(topicPartition);
                        unfilledMembers.add(memberId);
                    }
                    if (assignedMemberCount <= desiredSharing) {
                        break;
                    }
                }
            }
            if (assignedMemberCount > desiredSharing) {
                for (String memberId : assignedMembers) {
                    if (!overfilledMembers.contains(memberId)) {
                        Map<Uuid, Set<Integer>> newMemberAssignment = newGroupAssignment.get(memberId);
                        if (newMemberAssignment == null) {
                            newMemberAssignment = AssignorHelpers.deepCopyAssignment(oldGroupAssignment.get(memberId));
                            newGroupAssignment.put(memberId, newMemberAssignment);
                        }
                        newMemberAssignment.get(topicPartition.topicId()).remove(topicPartition.partitionId());
                        assignedMemberCount--;
                        finalAssignmentByPartition.get(topicPartition).remove(memberId);
                        finalAssignmentByMember.get(memberId).remove(topicPartition);
                        unfilledMembers.add(memberId);
                    }
                    if (assignedMemberCount <= desiredSharing) {
                        break;
                    }
                }
            }
        });

    }

    /**
     * Revoke partitions from members which are still overfilled.
     */
    private void maybeRevokeOverfilledMembers() {
        overfilledMembers.forEach(memberId -> {
            int desiredAssignmentCountForMember = desiredAssignmentCount.get(memberId);
            Set<TopicIdPartition> finalAssignmentForMember = finalAssignmentByMember.get(memberId);
            if (finalAssignmentForMember.size() > desiredAssignmentCountForMember) {
                Iterator<TopicIdPartition> iterator = finalAssignmentForMember.iterator();
                while (iterator.hasNext()) {
                    TopicIdPartition topicPartition = iterator.next();
                    finalAssignmentByPartition.get(topicPartition).remove(memberId);
                    iterator.remove();
                    if (finalAssignmentForMember.size() == desiredAssignmentCountForMember) {
                        break;
                    }
                }
            }
        });
    }

    /**
     * Assign partitions to unfilled members.
     */
    private void assignRemainingPartitions() {
        // Finally, round-robin assignment for under-assigned partitions.
        Iterator<String> memberIdIterator = unfilledMembers.iterator();
        for (Map.Entry<TopicIdPartition, Set<String>> partitionAssignment : finalAssignmentByPartition.entrySet()) {
            TopicIdPartition topicIdPartition = partitionAssignment.getKey();
            Set<String> membersAssigned = partitionAssignment.getValue();

            while (membersAssigned.size() < desiredSharing) {
                if (!memberIdIterator.hasNext()) {
                    memberIdIterator = unfilledMembers.iterator();
                    if (unfilledMembers.isEmpty()) {
                        // This should never happen, but guarding against an infinite loop
                        throw new PartitionAssignorException("Inconsistent number of member IDs");
                    }
                }

                String memberId = memberIdIterator.next();
                if (!membersAssigned.contains(memberId)) {
                    Map<Uuid, Set<Integer>> newMemberAssignment = newGroupAssignment.get(memberId);
                    if (newMemberAssignment == null) {
                        newMemberAssignment = AssignorHelpers.deepCopyAssignment(oldGroupAssignment.get(memberId));
                        newGroupAssignment.put(memberId, newMemberAssignment);
                    }
                    newMemberAssignment.computeIfAbsent(topicIdPartition.topicId(), k -> new HashSet<>()).add(topicIdPartition.partitionId());
                    if (newMemberAssignment.get(topicIdPartition.topicId()).size() > desiredAssignmentCount.get(memberId)) {
                        memberIdIterator.remove();
                    }
                    membersAssigned.add(memberId);
                }
            }
        }
    }
}
