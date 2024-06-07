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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.assignor.SubscriptionType.HOMOGENEOUS;

public class RangeAssignor implements ConsumerGroupPartitionAssignor {
    public static final String RANGE_ASSIGNOR_NAME = "range";

    @Override
    public String name() {
        return RANGE_ASSIGNOR_NAME;
    }

    private static final Class<?> UNMODIFIABLE_MAP_CLASS = Collections.unmodifiableMap(new HashMap<>()).getClass();
    private static final Class<?> EMPTY_MAP_CLASS = Collections.emptyMap().getClass();

    /**
     * Helper class to represent a member with remaining partitions to meet the quota.
     */
    private static class MemberWithRemainingAssignments {
        private final String memberId;
        private final int remaining;

        public MemberWithRemainingAssignments(String memberId, int remaining) {
            this.memberId = memberId;
            this.remaining = remaining;
        }
    }

    /**
     * Generate a map of topic Ids to collections of members subscribed to them.
     *
     * @param groupSpec                     The specification required for group assignments.
     * @param subscribedTopicDescriber      The metadata describer for subscribed topics and clusters.
     * @return A map of topic Ids to collections of member Ids subscribed to them.
     *
     * @throws PartitionAssignorException If a member is subscribed to a non-existent topic.
     */
    private Map<Uuid, Collection<String>> membersPerTopic(
        final GroupSpec groupSpec,
        final SubscribedTopicDescriber subscribedTopicDescriber
    ) {
        Map<Uuid, Collection<String>> membersPerTopic = new HashMap<>();

        // Handle homogeneous subscriptions
        if (groupSpec.subscriptionType().equals(HOMOGENEOUS)) {
            Collection<String> allMembers = groupSpec.memberIds();
            Collection<Uuid> topics = groupSpec.memberSubscription(groupSpec.memberIds().iterator().next())
                .subscribedTopicIds();

            for (Uuid topicId : topics) {
                if (subscribedTopicDescriber.numPartitions(topicId) == -1) {
                    throw new PartitionAssignorException("Member is subscribed to a non-existent topic");
                }
                membersPerTopic.put(topicId, allMembers);
            }
        } else {
            // Handle heterogeneous subscriptions
            groupSpec.memberIds().forEach(memberId -> {
                Collection<Uuid> topics = groupSpec.memberSubscription(memberId).subscribedTopicIds();
                for (Uuid topicId : topics) {
                    if (subscribedTopicDescriber.numPartitions(topicId) == -1) {
                        throw new PartitionAssignorException("Member is subscribed to a non-existent topic");
                    }
                    membersPerTopic
                        .computeIfAbsent(topicId, k -> new ArrayList<>())
                        .add(memberId);
                }
            });
        }

        return membersPerTopic;
    }

    /**
     * Assign partitions to members according to the range assignor logic.
     *
     * @param groupSpec                     The specification required for group assignments.
     * @param subscribedTopicDescriber      The metadata describer for subscribed topics and clusters.
     * @return The group assignment.
     *
     * @throws PartitionAssignorException If there is an error during partition assignment.
     */
    @Override
    public GroupAssignment assign(
        final GroupSpec groupSpec,
        final SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        Map<String, MemberAssignment> newTargetAssignment = new HashMap<>();

        // Generate a map of topic Ids to collections of members subscribed to them
        Map<Uuid, Collection<String>> membersPerTopic = membersPerTopic(
            groupSpec,
            subscribedTopicDescriber
        );

        System.out.println("Members per topic" + membersPerTopic);

        // For each topic, assign partitions to members
        membersPerTopic.forEach((topicId, membersForTopic) -> {
            int numPartitionsForTopic = subscribedTopicDescriber.numPartitions(topicId);
            int minRequiredQuota = numPartitionsForTopic / membersForTopic.size();
            int numMembersWithExtraPartition = numPartitionsForTopic % membersForTopic.size();

            Set<Integer> assignedStickyPartitionsForTopic = new HashSet<>();
            List<MemberWithRemainingAssignments> potentiallyUnfilledMembers = new ArrayList<>();

            maybeRevokePartitions(
                topicId,
                membersForTopic,
                minRequiredQuota,
                numMembersWithExtraPartition,
                groupSpec,
                newTargetAssignment,
                assignedStickyPartitionsForTopic,
                potentiallyUnfilledMembers
            );

            System.out.println("New Target Assignment after sticky" + newTargetAssignment);

            assignRemainingPartitions(
                topicId,
                numPartitionsForTopic,
                assignedStickyPartitionsForTopic,
                potentiallyUnfilledMembers,
                newTargetAssignment
            );

            System.out.println("New Target Assignment after everything" + newTargetAssignment);
        });
        return new GroupAssignment(newTargetAssignment);
    }

    private void maybeRevokePartitions(
        Uuid topicId,
        Collection<String> membersForTopic,
        int minRequiredQuota,
        int numMembersWithExtraPartition,
        GroupSpec groupSpec,
        Map<String, MemberAssignment> newTargetAssignment,
        Set<Integer> assignedStickyPartitionsForTopic,
        List<MemberWithRemainingAssignments> potentiallyUnfilledMembers
    ) {
        for (String memberId : membersForTopic) {
            Map<Uuid, Set<Integer>> oldAssignment = groupSpec.memberAssignment(memberId);
            MemberAssignment newMemberAssignment = newTargetAssignment.get(memberId);
            boolean isNewAssignmentCreated = false;
            Map<Uuid, Set<Integer>> newAssignment = newMemberAssignment != null ?
                newMemberAssignment.targetPartitions() : null;

            // Ensure the old assignment is immutable
            if (!isImmutableMap(oldAssignment)) {
                throw new IllegalStateException("The assignor expects an immutable map.");
            }

            Set<Integer> assignedPartitionsForTopic = oldAssignment.getOrDefault(topicId, Collections.emptySet());
            int currentAssignmentSize = assignedPartitionsForTopic.size();
            int quota = minRequiredQuota;

            System.out.println("Assigned partitions for topic " + topicId + " at member " + memberId + " is " + assignedPartitionsForTopic);
            if (numMembersWithExtraPartition > 0) {
                quota++;
                numMembersWithExtraPartition--;
            }

            if (currentAssignmentSize == quota) {
                assignedStickyPartitionsForTopic.addAll(assignedPartitionsForTopic);
            } else {
                if (newAssignment == null) {
                    // Create a deep copy if newAssignment is still null
                    // Remove entries for topics that are no longer part of the member's subscription list.
                    newAssignment = deepCopyWithRevokation(
                        oldAssignment,
                        memberId,
                        groupSpec
                    );
                    isNewAssignmentCreated = true;
                    System.out.println("Create new assignment for member " + memberId + "at topic " + topicId);
                }
                if (currentAssignmentSize > quota) {
                    // Sort partitions to ensure the same partitions are removed in each iteration
                    // to facilitate efficient co-partitioning.
                    List<Integer> assignedPartitionsList = new ArrayList<>(assignedPartitionsForTopic);
                    Collections.sort(assignedPartitionsList);

                    // Add partitions to sticky partitions list up till the quota.
                    assignedStickyPartitionsForTopic.addAll(assignedPartitionsList.subList(0, quota));

                    // Get the sublist of partitions past the quota to remove from the assignment.
                    List<Integer> partitionsToRemove = assignedPartitionsList.subList(quota, assignedPartitionsList.size());

                    // Remove these partitions from the new assignment of the member.
                    newAssignment.get(topicId).removeAll(partitionsToRemove);
                } else {
                    assignedStickyPartitionsForTopic.addAll(assignedPartitionsForTopic);
                    quota -= currentAssignmentSize;

                    // Members that haven't met the quota yet need to be tracked.
                    MemberWithRemainingAssignments newPair = new MemberWithRemainingAssignments(memberId, quota);
                    potentiallyUnfilledMembers.add(newPair);
                }
            }
            System.out.println("Assigned sticky partitions for topic " + topicId + "are " + assignedStickyPartitionsForTopic);
            // Use the old assignment if newAssignment is null, otherwise use the new assignment
            if (newTargetAssignment.get(memberId) == null && newAssignment == null) {
                for (Map.Entry<Uuid, Set<Integer>> entry : oldAssignment.entrySet()) {
                    Uuid subscribedTopicId = entry.getKey();
                    if (!groupSpec.memberSubscription(memberId).subscribedTopicIds().contains(subscribedTopicId)) {
                        newAssignment = deepCopyWithRevokation(
                            oldAssignment,
                            memberId,
                            groupSpec
                        );
                        isNewAssignmentCreated = true;
                        break;
                    }
                }
                if (!isNewAssignmentCreated) newTargetAssignment.put(memberId, new MemberAssignment(oldAssignment));
            }
            if (isNewAssignmentCreated) {
                newTargetAssignment.put(memberId, new MemberAssignment(newAssignment));
            }
        }

        if (numMembersWithExtraPartition > 0) {
            throw new PartitionAssignorException("There was an error in calculating quotas during the assignment process");
        }
    }

    private void assignRemainingPartitions(
        Uuid topicId,
        int numPartitionsForTopic,
        Set<Integer> assignedStickyPartitionsForTopic,
        List<MemberWithRemainingAssignments> potentiallyUnfilledMembers,
        Map<String, MemberAssignment> newTargetAssignment
    ) {
        // Collect unassigned partitions.
        List<Integer> unassignedPartitionsForTopic = new ArrayList<>();
        for (int i = 0; i < numPartitionsForTopic; i++) {
            if (!assignedStickyPartitionsForTopic.contains(i)) {
                unassignedPartitionsForTopic.add(i);
            }
        }
        System.out.println("unassigned partitions for topic" + topicId + "is " + unassignedPartitionsForTopic);

        // Assign unassigned partitions to potentially unfilled members.
        int unassignedPartitionsListStartPointer = 0;
        for (MemberWithRemainingAssignments pair : potentiallyUnfilledMembers) {
            String memberId = pair.memberId;
            int remaining = pair.remaining;
            if (remaining > 0) {
                List<Integer> partitionsToAssign = unassignedPartitionsForTopic
                    .subList(unassignedPartitionsListStartPointer, unassignedPartitionsListStartPointer + remaining);
                unassignedPartitionsListStartPointer += remaining;

                Map<Uuid, Set<Integer>> newAssignment = newTargetAssignment.get(memberId).targetPartitions();
                newAssignment.computeIfAbsent(topicId, k -> new HashSet<>()).addAll(partitionsToAssign);
            }
        }
    }

    /**
     * Checks if a map is immutable.
     *
     * @param map       The map to check.
     * @return True if the map is immutable, otherwise false.
     */
    private static boolean isImmutableMap(Map<?, ?> map) {
        return UNMODIFIABLE_MAP_CLASS.isInstance(map) || EMPTY_MAP_CLASS.isInstance(map);
    }

    /**
     * Creates a deep copy of a map.
     *
     * @param map       The map to copy.
     * @param memberId  The member Id.
     * @return A deep copy of the map without unsubscribed topicIds.
     */
    private static Map<Uuid, Set<Integer>> deepCopyWithRevokation(
        Map<Uuid, Set<Integer>> map,
        String memberId,
        GroupSpec groupSpec
    ) {
        Map<Uuid, Set<Integer>> copy = new HashMap<>(map.size());
        for (Map.Entry<Uuid, Set<Integer>> entry : map.entrySet()) {
            Uuid topicId = entry.getKey();
            if (groupSpec.memberSubscription(memberId).subscribedTopicIds().contains(topicId)) {
                copy.put(entry.getKey(), new HashSet<>(entry.getValue()));
            }
        }
        return copy;
    }
}
