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

import static java.lang.Math.min;

/**
 * This Range Assignor inherits properties of both the range assignor and the sticky assignor.
 * The properties are as follows:
 * <ol>
 *      <li> Each member must get at least one partition from every topic that it is subscribed to.
 *           The only exception is when the number of subscribed members is greater than the
 *           number of partitions for that topic. (Range) </li>
 *      <li> Partitions should be assigned to members in a way that facilitates the join operation when required. (Range)
 *           This can only be done if every member is subscribed to the same topics and the topics are co-partitioned.
 *           Two streams are co-partitioned if the following conditions are met:
 *           <ul>
 *              <li> The keys must have the same schemas. </li>
 *              <li> The topics involved must have the same number of partitions. </li>
 *           </ul>
 *      </li>
 *      <li> Members should retain as much of their previous assignment as possible to reduce the number of partition
 *           movements during reassignment. (Sticky) </li>
 * </ol>
 */
public class RangeAssignor implements PartitionAssignor {
    public static final String RANGE_ASSIGNOR_NAME = "range";

    @Override
    public String name() {
        return RANGE_ASSIGNOR_NAME;
    }

    /**
     * Pair of memberId and remaining partitions to meet the quota.
     */
    private static class MemberWithRemainingAssignments {
        /**
         * Member Id.
         */
        private final String memberId;

        /**
         * Number of partitions required to meet the assignment quota.
         */
        private final int remaining;

        public MemberWithRemainingAssignments(String memberId, int remaining) {
            this.memberId = memberId;
            this.remaining = remaining;
        }
    }

    /**
     * Returns a map of topic Ids to a list of members subscribed to them,
     * based on the given assignment specification and metadata.
     *
     * @param assignmentSpec           The specification for member assignments.
     * @param subscribedTopicDescriber The metadata describer for subscribed topics and clusters.
     * @return A map of topic Ids to a list of member Ids subscribed to them.
     *
     * @throws PartitionAssignorException If a member is subscribed to a non-existent topic.
     */
    private Map<Uuid, List<String>> membersPerTopic(final AssignmentSpec assignmentSpec, final SubscribedTopicDescriber subscribedTopicDescriber) {
        Map<Uuid, List<String>> membersPerTopic = new HashMap<>();
        Map<String, AssignmentMemberSpec> membersData = assignmentSpec.members();

        membersData.forEach((memberId, memberMetadata) -> {
            Collection<Uuid> topics = memberMetadata.subscribedTopicIds();
            for (Uuid topicId : topics) {
                if (subscribedTopicDescriber.numPartitions(topicId) == -1) {
                    throw new PartitionAssignorException("Member is subscribed to a non-existent topic");
                }
                membersPerTopic
                    .computeIfAbsent(topicId, k -> new ArrayList<>())
                    .add(memberId);
            }
        });

        return membersPerTopic;
    }

    /**
     * The algorithm includes the following steps:
     * <ol>
     *      <li> Generate a map of members per topic using the given member subscriptions. </li>
     *      <li> Generate a list of members called potentially unfilled members, which consists of members that have not
     *           met the minimum required quota of partitions for the assignment AND get a list called assigned sticky
     *           partitions for topic, which has the partitions that will be retained in the new assignment. </li>
     *      <li> Generate a list of unassigned partitions by calculating the difference between the total partitions
     *           for the topic and the assigned (sticky) partitions. </li>
     *      <li> Find members from the potentially unfilled members list that haven't met the total required quota
     *           i.e. minRequiredQuota + 1, if the member is designated to receive one of the excess partitions OR
     *           minRequiredQuota otherwise. </li>
     *      <li> Assign partitions to them in ranges from the unassigned partitions per topic
     *           based on the remaining partitions value. </li>
     * </ol>
     */
    @Override
    public GroupAssignment assign(final AssignmentSpec assignmentSpec, final SubscribedTopicDescriber subscribedTopicDescriber) throws PartitionAssignorException {
        Map<String, MemberAssignment> newAssignment = new HashMap<>();

        // Step 1
        Map<Uuid, List<String>> membersPerTopic = membersPerTopic(assignmentSpec, subscribedTopicDescriber);

        membersPerTopic.forEach((topicId, membersForTopic) -> {
            int numPartitionsForTopic = subscribedTopicDescriber.numPartitions(topicId);
            int minRequiredQuota = numPartitionsForTopic / membersForTopic.size();
            // Each member can get only ONE extra partition per topic after receiving the minimum quota.
            int numMembersWithExtraPartition = numPartitionsForTopic % membersForTopic.size();

            // Step 2
            Set<Integer> assignedStickyPartitionsForTopic = new HashSet<>();
            List<MemberWithRemainingAssignments> potentiallyUnfilledMembers = new ArrayList<>();

            for (String memberId : membersForTopic) {
                Set<Integer> assignedPartitionsForTopic = assignmentSpec.members().get(memberId)
                    .assignedPartitions().getOrDefault(topicId, Collections.emptySet());

                int currentAssignmentSize = assignedPartitionsForTopic.size();
                List<Integer> currentAssignmentListForTopic = new ArrayList<>(assignedPartitionsForTopic);

                // If there were partitions from this topic that were previously assigned to this member, retain as many as possible.
                // Sort the current assignment in ascending order since we want the same partition numbers from each topic
                // to go to the same member, in order to facilitate joins in case of co-partitioned topics.
                if (currentAssignmentSize > 0) {
                    int retainedPartitionsCount = min(currentAssignmentSize, minRequiredQuota);
                    Collections.sort(currentAssignmentListForTopic);
                    for (int i = 0; i < retainedPartitionsCount; i++) {
                        assignedStickyPartitionsForTopic
                            .add(currentAssignmentListForTopic.get(i));
                        newAssignment.computeIfAbsent(memberId, k -> new MemberAssignment(new HashMap<>()))
                            .targetPartitions()
                            .computeIfAbsent(topicId, k -> new HashSet<>())
                            .add(currentAssignmentListForTopic.get(i));
                    }
                }

                // Number of partitions required to meet the minRequiredQuota.
                // There are 3 cases w.r.t the value of remaining:
                // 1) remaining < 0: this means that the member has more than the min required amount.
                // 2) If remaining = 0: member has the minimum required partitions, but it may get an extra partition, so it is a potentially unfilled member.
                // 3) If remaining > 0: member doesn't have the minimum required partitions, so it should be added to potentiallyUnfilledMembers.
                int remaining = minRequiredQuota - currentAssignmentSize;

                // Retain extra partitions as well when applicable.
                if (remaining < 0 && numMembersWithExtraPartition > 0) {
                    numMembersWithExtraPartition--;
                    // Since we already added the minimumRequiredQuota of partitions in the previous step (until minReq - 1), we just need to
                    // add the extra partition that will be present at the index right after min quota was satisfied.
                    assignedStickyPartitionsForTopic
                        .add(currentAssignmentListForTopic.get(minRequiredQuota));
                    newAssignment.computeIfAbsent(memberId, k -> new MemberAssignment(new HashMap<>()))
                        .targetPartitions()
                        .computeIfAbsent(topicId, k -> new HashSet<>())
                        .add(currentAssignmentListForTopic.get(minRequiredQuota));
                } else {
                    MemberWithRemainingAssignments newPair = new MemberWithRemainingAssignments(memberId, remaining);
                    potentiallyUnfilledMembers.add(newPair);
                }
            }

            // Step 3
            // Find the difference between the total partitions per topic and the already assigned sticky partitions for the topic to get the unassigned partitions.
            // List of unassigned partitions for topic contains the partitions in ascending order.
            List<Integer> unassignedPartitionsForTopic = new ArrayList<>();
            for (int i = 0; i < numPartitionsForTopic; i++) {
                if (!assignedStickyPartitionsForTopic.contains(i)) {
                    unassignedPartitionsForTopic.add(i);
                }
            }

            // Step 4 and Step 5
            // Account for the extra partitions if necessary and increase the required quota by 1.
            // If remaining > 0 after increasing the required quota, assign the remaining number of partitions from the unassigned partitions list.
            int unassignedPartitionsListStartPointer = 0;
            for (MemberWithRemainingAssignments pair : potentiallyUnfilledMembers) {
                String memberId = pair.memberId;
                int remaining = pair.remaining;
                if (numMembersWithExtraPartition > 0) {
                    remaining++;
                    numMembersWithExtraPartition--;
                }
                if (remaining > 0) {
                    List<Integer> partitionsToAssign = unassignedPartitionsForTopic
                        .subList(unassignedPartitionsListStartPointer, unassignedPartitionsListStartPointer + remaining);
                    unassignedPartitionsListStartPointer += remaining;
                    newAssignment.computeIfAbsent(memberId, k -> new MemberAssignment(new HashMap<>()))
                        .targetPartitions()
                        .computeIfAbsent(topicId, k -> new HashSet<>())
                        .addAll(partitionsToAssign);
                }
            }
        });

        return new GroupAssignment(newAssignment);
    }
}

