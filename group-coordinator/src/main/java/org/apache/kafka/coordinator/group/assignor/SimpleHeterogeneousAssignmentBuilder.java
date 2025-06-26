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
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The heterogeneous simple assignment builder is used to generate the target assignment for a share group with
 * at least one of its members subscribed to a different set of topics.
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
public class SimpleHeterogeneousAssignmentBuilder {

    /**
     * The list of all the topic Ids that the share group is subscribed to.
     */
    private final Set<Uuid> subscribedTopicIds;

    /**
     * The list of members in the consumer group.
     */
    private final List<String> memberIds;

    /**
     * Maps member ids to their indices in the memberIds list.
     */
    private final Map<String, Integer> memberIndices;

    /**
     * Subscribed members by topic.
     */
    private final Map<Uuid, List<Integer>> subscribedMembersByTopic;

    /**
     * The list of all the topic-partitions assignable for the share group, grouped by topic.
     */
    private final Map<Uuid, List<TopicIdPartition>> targetPartitionsByTopic;

    /**
     * The number of members in the share group.
     */
    private final int numGroupMembers;

    /**
     * The number of subscribed topics.
     */
    private final int numSubscribedTopics;

    /**
     * The desired sharing for each target partition.
     * For entirely balanced assignment, we would expect (numTargetPartitions / numGroupMembers) partitions per member, rounded upwards.
     * That can be expressed as:  Math.ceil(numTargetPartitions / (double) numGroupMembers)
     */
    private final Map<Uuid, Integer> desiredSharingByTopic;

    /**
     * The desired number of assignments for each share group member.
     * <p>
     * Members are stored as integer indices into the memberIds array.
     */
    private final Map<Uuid, int[]> desiredAssignmentCountByTopic;

    /**
     * The share group assignment from the group metadata specification at the start of the assignment operation.
     * <p>
     * Members are stored as integer indices into the memberIds array.
     */
    private final Map<Integer, Map<Uuid, Set<Integer>>> oldGroupAssignment;

    /**
     * The share group assignment calculated iteratively by the assignment operation. Entries in this map override those
     * in the old group assignment map.
     * <p>
     * Members are stored as integer indices into the memberIds array.
     */
    private final Map<Integer, Map<Uuid, Set<Integer>>> newGroupAssignment;

    /**
     * The final assignment keyed by topic-partition mapping to member.
     * <p>
     * Members are stored as integer indices into the memberIds array.
     */
    private final Map<TopicIdPartition, Set<Integer>> finalAssignmentByPartition;

    /**
     * The final assignment keyed by member ID mapping to topic-partitions.
     * <p>
     * Members are stored as integer indices into the memberIds array.
     */
    private final Map<Integer, Set<TopicIdPartition>> finalAssignmentByMember;

    /**
     * The set of members which have too few assigned partitions.
     * <p>
     * Members are stored as integer indices into the memberIds array.
     */
    private final Map<Uuid, Set<Integer>> unfilledMembersByTopic;

    /**
     * The set of members which have too many assigned partitions.
     * <p>
     * Members are stored as integer indices into the memberIds array.
     */
//    private final Map<Uuid, Set<Integer>> overfilledMembersByTopic;

    public SimpleHeterogeneousAssignmentBuilder(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) {
        this.subscribedTopicIds = groupSpec.memberSubscription(groupSpec.memberIds().iterator().next()).subscribedTopicIds();
        this.numSubscribedTopics = subscribedTopicIds.size();

        // Number the members 0 to M - 1.
        this.numGroupMembers = groupSpec.memberIds().size();
        this.memberIds = new ArrayList<>(groupSpec.memberIds());
        this.memberIndices = AssignorHelpers.newHashMap(numGroupMembers);
        for (int memberIndex = 0; memberIndex < numGroupMembers; memberIndex++) {
            memberIndices.put(memberIds.get(memberIndex), memberIndex);
        }

        this.targetPartitionsByTopic = computeTargetPartitions(groupSpec, subscribedTopicIds, subscribedTopicDescriber);
        this.subscribedMembersByTopic = computeSubscribedMembers(groupSpec, subscribedTopicIds, memberIndices);

        int totalTargetPartitions = 0;
        this.desiredSharingByTopic = AssignorHelpers.newHashMap(numSubscribedTopics);
        this.desiredAssignmentCountByTopic = AssignorHelpers.newHashMap(numSubscribedTopics);
        for (Map.Entry<Uuid, List<TopicIdPartition>> topicTargetPartitions : targetPartitionsByTopic.entrySet()) {
            Uuid topicId = topicTargetPartitions.getKey();
            List<TopicIdPartition> targetPartitions = topicTargetPartitions.getValue();
            int numTargetPartitions = targetPartitions.size();
            List<Integer> subscribedMembers = subscribedMembersByTopic.get(topicId);
            int numSubscribedMembers = subscribedMembers.size();
            int desiredSharing;
            if (numTargetPartitions == 0) {
                desiredSharing = 0;
            } else {
                desiredSharing = (numSubscribedMembers + numTargetPartitions - 1) / numTargetPartitions;
            }
            this.desiredSharingByTopic.put(topicId, desiredSharing);
            totalTargetPartitions += numTargetPartitions;

            // Calculate the desired number of assignments for each member.
            // The precise desired assignment count per target partition. This can be a fractional number.
            // We would expect (numSubscribedMembers / numTargetPartitions) assignments per partition, rounded upwards.
            // Using integer arithmetic:  (numSubscribedMembers + numTargetPartitions - 1) / numTargetPartitions
            double preciseDesiredAssignmentCount = desiredSharing * numTargetPartitions / (double) numSubscribedMembers;
            int[] desiredAssignmentCount = new int[numGroupMembers];
            for (int memberIndex = 0; memberIndex < numSubscribedMembers; memberIndex++) {
                desiredAssignmentCount[subscribedMembers.get(memberIndex)] =
                    (int) Math.ceil(preciseDesiredAssignmentCount * (double) (memberIndex + 1)) -
                        (int) Math.ceil(preciseDesiredAssignmentCount * (double) memberIndex);
            }
            desiredAssignmentCountByTopic.put(topicId, desiredAssignmentCount);
        }
        this.oldGroupAssignment = AssignorHelpers.newHashMap(numGroupMembers);
        this.newGroupAssignment = AssignorHelpers.newHashMap(numGroupMembers);
        this.finalAssignmentByPartition = AssignorHelpers.newHashMap(totalTargetPartitions);
        this.finalAssignmentByMember = AssignorHelpers.newHashMap(numGroupMembers);
        this.unfilledMembersByTopic = AssignorHelpers.newHashMap(numSubscribedTopics);
//        this.overfilledMembersByTopic = AssignorHelpers.newHashMap(numSubscribedTopics);

        // Extract the old group assignment from the group metadata specification.
        groupSpec.memberIds().forEach(memberId -> {
            int memberIndex = memberIndices.get(memberId);
            oldGroupAssignment.put(memberIndex, groupSpec.memberAssignment(memberId).partitions());
        });
    }

    /**
     * Here's the step-by-step breakdown of the assignment process:
     * <ol>
     *   <li>Revoke partitions from the existing assignment that are no longer part of each member's subscriptions.</li>
     *   <li>Revoke partitions from members which have too many partitions.</li>
     *   <li>Revoke any partitions which are shared more than desired.</li>
     *   <li>Assign any partitions which have insufficient members assigned.</li>
     * </ol>
     */
    public GroupAssignment build() {
        if (subscribedTopicIds.isEmpty()) {
            return new GroupAssignment(Map.of());
        }

        // The order of steps here is not that significant, but assignRemainingPartitions must go last.
        revokeUnassignablePartitions();

        subscribedTopicIds.forEach(topicId -> {
            revokeOverfilledMembers(topicId);

            revokeOversharedPartitions(topicId);

            // Add in any partitions which are currently not in the assignment.
            targetPartitionsByTopic.get(topicId).forEach(topicPartition -> finalAssignmentByPartition.computeIfAbsent(topicPartition, k -> new HashSet<>()));

            assignRemainingPartitions(topicId);
        });

        // Combine the old and the new group assignments to give the result.
        Map<String, MemberAssignment> targetAssignment = AssignorHelpers.newHashMap(numGroupMembers);
        for (int memberIndex = 0; memberIndex < numGroupMembers; memberIndex++) {
            Map<Uuid, Set<Integer>> memberAssignment = newGroupAssignment.get(memberIndex);
            if (memberAssignment == null) {
                targetAssignment.put(memberIds.get(memberIndex), new MemberAssignmentImpl(oldGroupAssignment.get(memberIndex)));
            } else {
                targetAssignment.put(memberIds.get(memberIndex), new MemberAssignmentImpl(memberAssignment));
            }
        }

        return new GroupAssignment(targetAssignment);
    }

    /**
     * Examine the members from the current assignment, making sure that no member has too many assigned partitions.
     * When looking at the current assignment, we need to only consider the topics in the current assignment that are
     * also being subscribed in the new assignment.
     */
    private void revokeUnassignablePartitions() {
        for (Map.Entry<Integer, Map<Uuid, Set<Integer>>> entry : oldGroupAssignment.entrySet()) {
            Integer memberIndex = entry.getKey();
            Map<Uuid, Set<Integer>> oldMemberAssignment = entry.getValue();
            Map<Uuid, Set<Integer>> newMemberAssignment = null;

            for (Map.Entry<Uuid, Set<Integer>> oldMemberPartitions : oldMemberAssignment.entrySet()) {
                Uuid topicId = oldMemberPartitions.getKey();
                Set<Integer> assignedPartitions = oldMemberPartitions.getValue();

                if (subscribedTopicIds.contains(topicId)) {
                    for (int partition : assignedPartitions) {
                        TopicIdPartition topicPartition = new TopicIdPartition(topicId, partition);
                        finalAssignmentByPartition.computeIfAbsent(topicPartition, k -> new HashSet<>()).add(memberIndex);
                        finalAssignmentByMember.computeIfAbsent(memberIndex, k -> new HashSet<>()).add(topicPartition);
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
                newGroupAssignment.put(memberIndex, newMemberAssignment);
            }
        }
    }

    /**
     * Revoke partitions from members which are overfilled.
     */
    private void revokeOverfilledMembers(Uuid topicId) {
        List<Integer> subscribedMembers = subscribedMembersByTopic.get(topicId);
        int[] desiredAssignmentCounts = desiredAssignmentCountByTopic.get(topicId);
        subscribedMembers.forEach(memberIndex -> {
            int memberDesiredAssignmentCount = desiredAssignmentCounts[memberIndex];
        });

//        Set<Integer> overfilledMembers = overfilledMembersByTopic.get(topicId);
//        if (overfilledMembers.isEmpty())
//            return;
//
//        overfilledMembers.forEach(memberIndex -> {
//            int memberDesiredAssignmentCount = desiredAssignmentCountByTopic.get(topicId)[memberIndex];
//            Set<TopicIdPartition> memberFinalAssignment = finalAssignmentByMember.get(memberIndex);
//            if (memberFinalAssignment.size() > memberDesiredAssignmentCount) {
//                Iterator<TopicIdPartition> iterator = memberFinalAssignment.iterator();
//                while (iterator.hasNext()) {
//                    TopicIdPartition topicPartition = iterator.next();
//                    newGroupAssignment.get(memberIndex).get(topicPartition.topicId()).remove(topicPartition.partitionId());
//                    finalAssignmentByPartition.get(topicPartition).remove(memberIndex);
//                    iterator.remove();
//                    if (memberFinalAssignment.size() == memberDesiredAssignmentCount) {
//                        break;
//                    }
//                }
//            }
//        });
    }

    /**
     * Revoke any over-shared partitions.
     */
    private void revokeOversharedPartitions(Uuid topicId) {
        finalAssignmentByPartition.forEach((topicPartition, assignedMembers) -> {
            int assignedMemberCount = assignedMembers.size();
            if (assignedMemberCount > desiredSharingByTopic.get(topicId)) {
                Iterator<Integer> assignedMemberIterator = assignedMembers.iterator();
                while (assignedMemberIterator.hasNext()) {
                    Integer memberIndex = assignedMemberIterator.next();
                    int desiredSharing = desiredSharingByTopic.get(topicId);
                    Map<Uuid, Set<Integer>> newMemberAssignment = newGroupAssignment.get(memberIndex);
                    if (newMemberAssignment == null) {
                        newMemberAssignment = AssignorHelpers.deepCopyAssignment(oldGroupAssignment.get(memberIndex));
                        newGroupAssignment.put(memberIndex, newMemberAssignment);
                    }
                    Set<Integer> partitions = newMemberAssignment.get(topicPartition.topicId());
                    if (partitions != null) {
                        if (partitions.remove(topicPartition.partitionId())) {
                            assignedMemberCount--;
                            assignedMemberIterator.remove();
                            finalAssignmentByMember.get(memberIndex).remove(topicPartition);
                            unfilledMembersByTopic.get(topicId).add(memberIndex);
                        }
                    }
                    if (assignedMemberCount <= desiredSharing) {
                        break;
                    }
                }
            }
        });
    }

    /**
     * Assign partitions to unfilled members. It repeatedly iterates through the unfilled members while running
     * once thrown the set of partitions. When a partition is found that has insufficient sharing, it attempts to assign
     * to one of the partitions.
     * <p>
     * There is one tricky cases here and that's where a partition wants another assignment, but none of the unfilled
     * members are able to take it (because they already have that partition). In this situation, we just accept that
     * no additional assignments for this partition could be made and carry on. In theory, a different shuffling of the
     * partitions would be able to achieve better balance, but it's harmless tolerating a slight imbalance in this case.
     * <p>
     * Note that finalAssignmentByMember is not maintained by this method which is expected to be the final step in the
     * computation.
     */
    private void assignRemainingPartitions(Uuid topicId) {
        Set<Integer> unfilledMembers = unfilledMembersByTopic.get(topicId);
        if (unfilledMembersByTopic.isEmpty())
            return;

        Iterator<Integer> memberIterator = unfilledMembers.iterator();
        boolean partitionAssignedForThisIterator = false;
        for (Map.Entry<TopicIdPartition, Set<Integer>> partitionAssignment : finalAssignmentByPartition.entrySet()) {
            TopicIdPartition topicPartition = partitionAssignment.getKey();
            Set<Integer> membersAssigned = partitionAssignment.getValue();

            int desiredSharing = desiredSharingByTopic.get(topicPartition.topicId());
            if (membersAssigned.size() < desiredSharing) {
                int assignmentsToMake = desiredSharing - membersAssigned.size();
                while (assignmentsToMake > 0) {
                    if (!memberIterator.hasNext()) {
                        if (!partitionAssignedForThisIterator) {
                            break;
                        }
                        memberIterator = unfilledMembers.iterator();
                        partitionAssignedForThisIterator = false;
                    }
                    int memberIndex = memberIterator.next();
                    if (!membersAssigned.contains(memberIndex)) {
                        Map<Uuid, Set<Integer>> newMemberAssignment = newGroupAssignment.get(memberIndex);
                        if (newMemberAssignment == null) {
                            newMemberAssignment = AssignorHelpers.deepCopyAssignment(oldGroupAssignment.get(memberIndex));
                            newGroupAssignment.put(memberIndex, newMemberAssignment);
                        }
                        newMemberAssignment.computeIfAbsent(topicPartition.topicId(), k -> new HashSet<>()).add(topicPartition.partitionId());
                        finalAssignmentByMember.computeIfAbsent(memberIndex, k -> new HashSet<>()).add(topicPartition);
                        assignmentsToMake--;
                        partitionAssignedForThisIterator = true;
                        if (finalAssignmentByMember.get(memberIndex).size() >= desiredAssignmentCountByTopic.get(topicId)[memberIndex]) {
                            memberIterator.remove();
                        }
                    }
                }
            }

            if (unfilledMembers.isEmpty()) {
                break;
            }
        }
    }

    /**
     * Computes the list of target partitions which can be assigned to members. This list includes all partitions
     * for the subscribed topic IDs, with the additional check that they must be assignable.
     * @param groupSpec                 The assignment spec which includes member metadata.
     * @param subscribedTopicIds        The set of subscribed topic IDs.
     * @param subscribedTopicDescriber  The topic and partition metadata describer.
     * @return The list of target partitions, grouped by topic.
     */
    private static Map<Uuid, List<TopicIdPartition>> computeTargetPartitions(
        GroupSpec groupSpec,
        Set<Uuid> subscribedTopicIds,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) {
        Map<Uuid, List<TopicIdPartition>> targetPartitionsByTopic = AssignorHelpers.newHashMap(subscribedTopicIds.size());
        subscribedTopicIds.forEach(topicId -> {
            int numPartitions = subscribedTopicDescriber.numPartitions(topicId);
            if (numPartitions == -1) {
                throw new PartitionAssignorException(
                    "Members are subscribed to topic " + topicId + " which doesn't exist in the topic metadata."
                );
            }

            List<TopicIdPartition> targetPartitions = new ArrayList<>(numPartitions);
            for (int partition = 0; partition < numPartitions; partition++) {
                if (groupSpec.isPartitionAssignable(topicId, partition)) {
                    targetPartitions.add(new TopicIdPartition(topicId, partition));
                }
            }
            targetPartitionsByTopic.put(topicId, targetPartitions);
        });

        return targetPartitionsByTopic;
    }

    private static Map<Uuid, List<Integer>> computeSubscribedMembers(
        GroupSpec groupSpec,
        Set<Uuid> subscribedTopicIds,
        Map<String, Integer> memberIndices
    ) {
        Map<Uuid, List<Integer>> subscribedMembersByTopic = AssignorHelpers.newHashMap(subscribedTopicIds.size());
        groupSpec.memberIds().forEach(memberId -> {
            int memberIndex = memberIndices.get(memberId);
            MemberSubscription memberSubscription = groupSpec.memberSubscription(memberId);
            memberSubscription.subscribedTopicIds().forEach(topicId -> {
                subscribedMembersByTopic.computeIfAbsent(topicId, k -> new ArrayList<>()).add(memberIndex);
            });
        });

        return subscribedMembersByTopic;
    }
}
