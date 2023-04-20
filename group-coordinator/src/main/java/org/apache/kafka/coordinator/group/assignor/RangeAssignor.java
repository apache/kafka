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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.min;

/**
 * The Server Side Sticky Range Assignor inherits properties of both the range assignor and the sticky assignor.
 * Properties are as follows:
 * <ol>
 * <li> Each member must get at least one partition for every topic that it is subscribed to. The only exception is when
 *      the number of subscribed members is greater than the number of partitions for that topic. (Range) </li>
 * <li> Partitions should be assigned to members in a way that facilitates the join operation when required. (Range) </li>
 *    This can only be done if every member is subscribed to the same topics and the topics are co-partitioned.
 *    Two streams are co-partitioned if the following conditions are met:
 *    <ul>
 * <li> The keys must have the same schemas.
 * <li> The topics involved must have the same number of partitions.
 *    </ul>
 * <li> Members should retain as much as their previous assignment as possible to reduce the number of partition movements during reassignment. (Sticky) </li>
 * </ol>
 */
public class RangeAssignor implements PartitionAssignor {

    private static final Logger log = LoggerFactory.getLogger(RangeAssignor.class);

    public static final String RANGE_ASSIGNOR_NAME = "range";

    @Override
    public String name() {
        return RANGE_ASSIGNOR_NAME;
    }

    static class RemainingAssignmentsForMember<String, Integer> {
        private final String memberId;
        private final Integer remaining;

        public RemainingAssignmentsForMember(String memberId, Integer remaining) {
            this.memberId = memberId;
            this.remaining = remaining;
        }

        public String memberId() {
            return memberId;
        }

        public Integer remaining() {
            return remaining;
        }

    }

    private Map<Uuid, List<String>> membersPerTopic(final AssignmentSpec assignmentSpec) {
        Map<Uuid, List<String>> membersPerTopic = new HashMap<>();
        Map<String, AssignmentMemberSpec> membersData = assignmentSpec.members();

        membersData.forEach((memberId, memberMetadata) -> {
            Collection<Uuid> topics = memberMetadata.subscribedTopicIds();
            for (Uuid topicId: topics) {
                // Only topics that are present in both the subscribed topics list and the topic metadata should be considered for assignment.
                if (assignmentSpec.topics().containsKey(topicId)) {
                    membersPerTopic
                            .computeIfAbsent(topicId, k -> new ArrayList<>())
                            .add(memberId);
                } else {
                    log.info(memberId + " subscribed to topic " + topicId + " which doesn't exist in the topic metadata");
                }
            }
        });

        return membersPerTopic;
    }

    private Map<Uuid, List<Integer>> getUnassignedPartitionsPerTopic(final AssignmentSpec assignmentSpec, Map<Uuid, Set<Integer>> assignedStickyPartitionsPerTopic) {
        Map<Uuid, List<Integer>> unassignedPartitionsPerTopic = new HashMap<>();
        Map<Uuid, AssignmentTopicMetadata> topicsMetadata = assignmentSpec.topics();

        topicsMetadata.forEach((topicId, assignmentTopicMetadata) -> {
            ArrayList<Integer> unassignedPartitionsForTopic = new ArrayList<>();
            int numPartitions = assignmentTopicMetadata.numPartitions();
            // The partitions will be in ascending order within the list of unassigned partitions per topic.
            Set<Integer> assignedStickyPartitionsForTopic = assignedStickyPartitionsPerTopic.getOrDefault(topicId, new HashSet<>());
            for (int i = 0; i < numPartitions; i++) {
                if (!assignedStickyPartitionsForTopic.contains(i)) {
                    unassignedPartitionsForTopic.add(i);
                }
            }
            unassignedPartitionsPerTopic.put(topicId, unassignedPartitionsForTopic);
        });

        return unassignedPartitionsPerTopic;
    }

/**
 * <p>The algorithm includes the following steps:
 * <ol>
 * <li> Generate a map of <code>membersPerTopic</code> using the given member subscriptions.</li>
 * <li> Generate a list of members (<code>potentiallyUnfilledMembers</code>) that have not met the minimum required quota for assignment AND
 * get a list of sticky partitions that we want to retain in the new assignment.</li>
 * <li> Add members from the <code>potentiallyUnfilled</code> list to the <code>Unfilled</code> list if they haven't met the total required quota i.e. minimum number of partitions per member + 1 (if member is designated to receive one of the excess partitions) </li>
 * <li> Generate a list of unassigned partitions by calculating the difference between total partitions and already assigned (sticky) partitions </li>
 * <li> Iterate through unfilled members and assign partitions from the unassigned partitions </li>
 * </ol>
 * </p>
 */
    @Override
    public GroupAssignment assign(final AssignmentSpec assignmentSpec) throws PartitionAssignorException {
        Map<String, Map<Uuid, Set<Integer>>> membersWithNewAssignmentPerTopic = new HashMap<>();
        // Step 1
        Map<Uuid, List<String>> membersPerTopic = membersPerTopic(assignmentSpec);
        // Step 2
        Map<Uuid, List<RemainingAssignmentsForMember<String, Integer>>> unfilledMembersPerTopic = new HashMap<>();
        Map<Uuid, Set<Integer>> assignedStickyPartitionsPerTopic = new HashMap<>();

        membersPerTopic.forEach((topicId, membersForTopic) -> {
            // For each topic we have a temporary list of members stored in potentiallyUnfilledMembers.
            // The list is populated with members that satisfy one of the two conditions:
            // 1) Members that already have the minimum required number of partitions i.e. total partitions divided by total subscribed members
            //    BUT they could be assigned an extra partition later on. Extra partitions exist if total partitions % number of members is greater than 0.
            //    In this case we add the member to the unfilled members map iff an extra partition needs to be assigned to it.
            // 2) Members that don't have the minimum required partitions, so irrespective of whether they get an extra partition or not they get added to the unfilled map later.
            List<RemainingAssignmentsForMember<String, Integer>> potentiallyUnfilledMembers = new ArrayList<>();

            AssignmentTopicMetadata topicData = assignmentSpec.topics().get(topicId);
            int numPartitionsForTopic = topicData.numPartitions();

            // Idle members case : When the number of members subscribed to a topic is greater than the total number of Partitions,
            // all members get assigned via the "extra partitions" logic since minRequiredQuota = 0.
            int minRequiredQuota = numPartitionsForTopic / membersForTopic.size();
            // Each member can get only ONE extra partition per topic after receiving the minimum quota.
            int numMembersWithExtraPartition = numPartitionsForTopic % membersForTopic.size();

            for (String memberId: membersForTopic) {
                // The partitions need to be in numeric order since we want the same partition numbers from each topic
                // to go to the same member in case of co-partitioned topics to facilitate joins.
                Set<Integer> assignedPartitionsForTopic = assignmentSpec.members().get(memberId).assignedPartitions().getOrDefault(topicId, Collections.emptySet());

                int currentAssignmentSize = assignedPartitionsForTopic.size();
                List<Integer> currentAssignmentListForTopic = new ArrayList<>(assignedPartitionsForTopic);

                // If there were previously assigned partitions present, we want to retain them.
                if (currentAssignmentSize > 0) {
                    // We either need to retain currentSize number of partitions when currentSize < required OR required number of partitions otherwise.
                    int retainedPartitionsCount = min(currentAssignmentSize, minRequiredQuota);
                    Collections.sort(currentAssignmentListForTopic);
                    for (int i = 0; i < retainedPartitionsCount; i++) {
                        assignedStickyPartitionsPerTopic
                                .computeIfAbsent(topicId, k -> new HashSet<>())
                                .add(currentAssignmentListForTopic.get(i));
                        membersWithNewAssignmentPerTopic
                                .computeIfAbsent(memberId, k -> new HashMap<>())
                                .computeIfAbsent(topicId, k -> new HashSet<>())
                                .add(currentAssignmentListForTopic.get(i));
                    }
                }

                // Number of partitions left to reach the minRequiredQuota.
                int remaining = minRequiredQuota - currentAssignmentSize;

                // There are 3 cases w.r.t value of remaining
                // 1) remaining < 0 this means that the member has more than the min required amount.
                if (remaining < 0 && numMembersWithExtraPartition > 0) {
                    // In order to remain as sticky as possible, since the order of members can be different, we want the members that already had extra
                    // partitions to retain them if it's still required, instead of assigning the extras to the first few members directly.
                    numMembersWithExtraPartition--;
                    // Since we already added the minimumRequiredQuota of partitions in the previous step (until minReq - 1), we just need to
                    // add the extra partition that will be present at the index right after min quota was satisfied.
                    assignedStickyPartitionsPerTopic
                            .computeIfAbsent(topicId, k -> new HashSet<>())
                            .add(currentAssignmentListForTopic.get(minRequiredQuota));
                    membersWithNewAssignmentPerTopic
                            .computeIfAbsent(memberId, k -> new HashMap<>())
                            .computeIfAbsent(topicId, k -> new HashSet<>())
                            .add(currentAssignmentListForTopic.get(minRequiredQuota));
                } else {
                    // 2) If remaining = 0 it has min req partitions but there is scope for getting an extra partition later on, so it is a potentialUnfilledMember.
                    // 3) If remaining > 0 it doesn't even have the min required partitions, and it definitely is unfilled, so it should be added to potentialUnfilledMembers.
                    RemainingAssignmentsForMember<String, Integer> newPair = new RemainingAssignmentsForMember<>(memberId, remaining);
                    potentiallyUnfilledMembers.add(newPair);
                }
            }

            // Step 3
            // If remaining > 0 after increasing the required quota due to the extra partition, add potentially unfilled member to the unfilled members list.
            for (RemainingAssignmentsForMember<String, Integer> pair : potentiallyUnfilledMembers) {
                String memberId = pair.memberId();
                Integer remaining = pair.remaining();
                if (numMembersWithExtraPartition > 0) {
                    remaining++;
                    numMembersWithExtraPartition--;
                }
                if (remaining > 0) {
                    RemainingAssignmentsForMember<String, Integer> newPair = new RemainingAssignmentsForMember<>(memberId, remaining);
                    unfilledMembersPerTopic
                            .computeIfAbsent(topicId, k -> new ArrayList<>())
                            .add(newPair);
                }
            }
        });

        // Step 4
        // Find the difference between the total partitions per topic and the already assigned sticky partitions for the topic to get unassigned partitions.
        Map<Uuid, List<Integer>> unassignedPartitionsPerTopic = getUnassignedPartitionsPerTopic(assignmentSpec, assignedStickyPartitionsPerTopic);

        // Step 5
        // Iterate through the unfilled members list and assign the remaining number of partitions from the unassignedPartitions list.
        unfilledMembersPerTopic.forEach((topicId, unfilledMembersForTopic) -> {
            int unassignedPartitionsListStartPointer = 0;
            for (RemainingAssignmentsForMember<String, Integer> remainingAssignmentsForMember : unfilledMembersForTopic) {
                String memberId = remainingAssignmentsForMember.memberId();
                int remaining = remainingAssignmentsForMember.remaining();
                List<Integer> partitionsToAssign = unassignedPartitionsPerTopic.get(topicId).subList(unassignedPartitionsListStartPointer, unassignedPartitionsListStartPointer + remaining);
                unassignedPartitionsListStartPointer += remaining;
                membersWithNewAssignmentPerTopic
                        .computeIfAbsent(memberId, k -> new HashMap<>())
                        .computeIfAbsent(topicId, k -> new HashSet<>())
                        .addAll(partitionsToAssign);
            }
        });

        // Consolidate the maps into MemberAssignment and then finally map each member to a MemberAssignment.
        Map<String, MemberAssignment> membersWithNewAssignment = new HashMap<>();

        membersWithNewAssignmentPerTopic.forEach((memberId, assignmentPerTopic) -> membersWithNewAssignment.put(memberId, new MemberAssignment(assignmentPerTopic)));

        return new GroupAssignment(membersWithNewAssignment);
    }
}

