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
import org.apache.kafka.coordinator.group.common.RackAwareTopicIdPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.min;

/**
 * <p>
 * Only used when all members have identical subscriptions.
 * Steps followed to get the most sticky and balanced assignment possible :-
 * <ol>
 *      <li> In case of a reassignment i.e. when a previous assignment exists: </li>
 *      <ul>
 *          <li> Obtain a valid prev assignment by selecting the assignments that have topics present in both the topic metadata and the members subscriptions.</li>
 *          <li> Get sticky partitions from the prev valid assignment using the newly decided quotas.</li>
 *      </ul>
 *      <li> Obtain the unassigned partitions from the difference between total partitions and assigned sticky partitions.</li>
 *      <li> Obtain a list of potentially unfilled members based on the minimum quotas.</li>
 *      <li> Populate the unfilled members map (member, remaining) after accounting for the additional partitions that might have to be assigned. </li>
 *      <li> Allocate all unassigned partitions to the unfilled members. </li>
 * </ol>
 * </p>
 */

public class OptimizedAssignmentBuilder extends UniformAssignor.AbstractAssignmentBuilder {
    private static final Logger log = LoggerFactory.getLogger(OptimizedAssignmentBuilder.class);
    // Subscription list is the same for all members.
    private final Collection<Uuid> validSubscriptionList;
    private Integer totalValidPartitionsCount;
    // The minimum required quota that each member needs to meet.
    private final int minQuota;
    // The expected number of members receiving one more than the minQuota partitions.
    private int expectedNumMembersWithExtraPartition;
    // Members that haven't met the min quota OR that have met the min quota but could potentially get an extra partition.
    // Map<memberId, remaining> where remaining = number of partitions remaining to meet the min Quota.
    private final Map<String, Integer> potentiallyUnfilledMembers;
    // Members that need to be assigned the remaining number of partitions including extra partitions.
    private Map<String, Integer> unfilledMembers;
    // Partitions that need to be retained from the members' previous assignments.
    private final Map<String, List<RackAwareTopicIdPartition>> assignedStickyPartitionsPerMember;
    // Partitions that are available to be assigned, computed by taking the difference between total partitions and assigned sticky partitions.
    private List<RackAwareTopicIdPartition> unassignedPartitions;

    private final Map<String, List<RackAwareTopicIdPartition>> finalAssignment;

    OptimizedAssignmentBuilder(AssignmentSpec assignmentSpec) {
        super(assignmentSpec);

        validSubscriptionList = new ArrayList<>();
        Collection<Uuid> givenSubscriptionList = assignmentSpec.members().values().iterator().next().subscribedTopicIds();
        System.out.println("subscription list is" + givenSubscriptionList);
        // Only add topicIds from the subscription list that are still present in the topicMetadata.
        for (Uuid topicId : givenSubscriptionList) {
            if (assignmentSpec.topics().containsKey(topicId)) {
                validSubscriptionList.add(topicId);
            } else {
                log.info("The subscribed topic : " + topicId + " doesn't exist in the topic metadata ");
            }
        }

        totalValidPartitionsCount = 0;
        for (Uuid topicId : validSubscriptionList) {
            totalValidPartitionsCount += assignmentSpec.topics().get(topicId).numPartitions();
        }

        int numberMembers = metadataPerMember.size();

        minQuota = (int) Math.floor(((double) totalValidPartitionsCount) / numberMembers);
        expectedNumMembersWithExtraPartition = totalValidPartitionsCount % numberMembers;

        potentiallyUnfilledMembers = new HashMap<>();
        unfilledMembers = new HashMap<>();
        assignedStickyPartitionsPerMember = new HashMap<>();
        finalAssignment = new HashMap<>();
    }

    @Override
    Map<String, List<RackAwareTopicIdPartition>> build() {
        if (validSubscriptionList.isEmpty()) {
            log.info("Valid subscriptions list is empty, returning empty assignment");
            return new HashMap<>();
        }

        List<RackAwareTopicIdPartition> allAssignedStickyPartitions = getAssignedStickyPartitions();

        addAssignedStickyPartitionsToNewAssignment();

        unassignedPartitions = getUnassignedPartitions(allAssignedStickyPartitions);

        unfilledMembers = getUnfilledMembers();

        if (!ensureTotalUnassignedPartitionsEqualsTotalRemainingAssignments()) {
            log.warn("Number of available partitions is not equal to the total requirement");
        }

        allocateUnassignedPartitions();

        return finalAssignment;
    }

    // Keep the partitions in the assignment only if they are still part of the new topic metadata and the members subscriptions.
    // Convert the assignment format to a list of a new data structure which contains the topic id and partition as a unit along with rackIds if necessary.
    private List<RackAwareTopicIdPartition> getValidCurrentAssignment(AssignmentMemberSpec assignmentMemberSpec) {
        List<RackAwareTopicIdPartition> validCurrentAssignmentList = new ArrayList<>();
        System.out.println("assignment member spec" + assignmentMemberSpec);
        for (Map.Entry<Uuid, Set<Integer>> currentAssignment : assignmentMemberSpec.assignedPartitions().entrySet()) {
            Uuid topicId = currentAssignment.getKey();
            List<Integer> currentAssignmentList = new ArrayList<>(currentAssignment.getValue());

            if (metadataPerTopic.containsKey(topicId) && validSubscriptionList.contains(topicId)) {
                for (Integer partition : currentAssignmentList) {
                    validCurrentAssignmentList.add(new RackAwareTopicIdPartition(topicId, partition, null));
                }
            } else if (!metadataPerTopic.containsKey(topicId)) {
                throw new PartitionAssignorException("Members are subscribed to topic " +
                        topicId + " which doesn't exist in the topic metadata");
            }
        }
        return validCurrentAssignmentList;
    }

    // Returns all the previously assigned partitions that we want to retain.
    // Fills potentially unfilled members based on the remaining number of partitions required to meet the minQuota.
    private List<RackAwareTopicIdPartition> getAssignedStickyPartitions() {
        List<RackAwareTopicIdPartition> allAssignedStickyPartitions = new ArrayList<>();

        for (Map.Entry<String, AssignmentMemberSpec> assignmentMemberSpecEntry : metadataPerMember.entrySet()) {
            String memberId = assignmentMemberSpecEntry.getKey();
            List<RackAwareTopicIdPartition> assignedStickyListForMember = new ArrayList<>();

            // Remove all the topics that aren't in the subscriptions or the topic metadata anymore.
            List<RackAwareTopicIdPartition> validCurrentAssignment = getValidCurrentAssignment(metadataPerMember.get(memberId));
            System.out.print("valid current assignment" + validCurrentAssignment);
            int currentAssignmentSize = validCurrentAssignment.size();
            int remaining = minQuota - currentAssignmentSize;

            if (currentAssignmentSize > 0) {
                // We either need to retain currentSize number of partitions when currentSize < required OR required number of partitions otherwise.
                int retainedPartitionsCount = min(currentAssignmentSize, minQuota);
                for (int i = 0; i < retainedPartitionsCount; i++) {
                    assignedStickyListForMember.add(validCurrentAssignment.get(i));
                }
                // The last index from the previous step is at int retainedPartitionsCount which is where the extra partition resides.
                if (remaining < 0 && expectedNumMembersWithExtraPartition > 0) {
                    assignedStickyListForMember.add(validCurrentAssignment.get(retainedPartitionsCount));
                    expectedNumMembersWithExtraPartition--;
                }
                assignedStickyPartitionsPerMember.put(memberId, assignedStickyListForMember);
                allAssignedStickyPartitions.addAll(assignedStickyListForMember);
            }
            if (remaining >= 0) {
                potentiallyUnfilledMembers.put(memberId, remaining);
            }

        }
        System.out.print("all assigned sticky partitions " + allAssignedStickyPartitions);
        return allAssignedStickyPartitions;
    }

    private void addAssignedStickyPartitionsToNewAssignment() {
        for (String memberId : metadataPerMember.keySet()) {
            finalAssignment.computeIfAbsent(memberId, k -> new ArrayList<>());
            if (assignedStickyPartitionsPerMember.containsKey(memberId)) {
                finalAssignment.get(memberId).addAll(assignedStickyPartitionsPerMember.get(memberId));
            }
        }
    }

    private boolean ensureTotalUnassignedPartitionsEqualsTotalRemainingAssignments() {
        int totalRemaining = 0;
        for (Map.Entry<String, Integer> unfilledEntry  : unfilledMembers.entrySet()) {
            totalRemaining += unfilledEntry.getValue();
        }
        return totalRemaining == unassignedPartitions.size();
    }

    // The unfilled members map has members mapped to the remaining partitions number = max allocation to this member.
    // The algorithm below assigns each member partitions in a round-robin fashion up to its max limit
    private void allocateUnassignedPartitions() {
        // Since the map doesn't guarantee order we need a list of memberIds to map each member to a particular index
        List<String> memberIds = new ArrayList<>(unfilledMembers.keySet());
        int[] currentIndexForMember = new int[memberIds.size()];

        for (String memberId : memberIds) {
            finalAssignment.computeIfAbsent(memberId, k -> new ArrayList<>());
        }

        int numMembers = unfilledMembers.size();
        for (int i = 0; i < unassignedPartitions.size(); i++) {
            int memberIndex = i % numMembers;
            int memberLimit = unfilledMembers.get(memberIds.get(memberIndex));
            // If the current member has reached its limit, find a member that has more space available in its assignment
            while (currentIndexForMember[memberIndex] >= memberLimit) {
                memberIndex = (memberIndex + 1) % numMembers;
                memberLimit = unfilledMembers.get(memberIds.get(memberIndex));
            }
            if (currentIndexForMember[memberIndex] < memberLimit) {
                finalAssignment.get(memberIds.get(memberIndex)).add(currentIndexForMember[memberIndex]++, unassignedPartitions.get(i));
            }
        }
    }

    private Map<String, Integer> getUnfilledMembers() {
        Map<String, Integer> unfilledMembers = new HashMap<>();
        for (Map.Entry<String, Integer> potentiallyUnfilledMemberEntry : potentiallyUnfilledMembers.entrySet()) {
            String memberId = potentiallyUnfilledMemberEntry.getKey();
            Integer remaining = potentiallyUnfilledMemberEntry.getValue();
            if (expectedNumMembersWithExtraPartition > 0) {
                remaining++;
                expectedNumMembersWithExtraPartition--;
            }
            // If remaining is still 0 because there were no more members required to get an extra partition, we don't add it to the unfilled list.
            if (remaining > 0) {
                unfilledMembers.put(memberId, remaining);
            }
        }
        return unfilledMembers;
    }

    private List<RackAwareTopicIdPartition> getUnassignedPartitions(List<RackAwareTopicIdPartition> allAssignedStickyPartitions) {
        List<RackAwareTopicIdPartition> unassignedPartitions = new ArrayList<>();
        // We only care about the topics that the members are subscribed to.
        List<Uuid> sortedAllTopics = new ArrayList<>(validSubscriptionList);
        Collections.sort(sortedAllTopics);
        if (allAssignedStickyPartitions.isEmpty()) {
            return getAllTopicPartitions(sortedAllTopics);
        }
        Collections.sort(allAssignedStickyPartitions, Comparator.comparing(RackAwareTopicIdPartition::topicId).thenComparing(RackAwareTopicIdPartition::partition));
        // Use two pointer approach and get the partitions that are in the total partitions but not in the assigned partitions.
        boolean shouldAddDirectly = false;
        Iterator<RackAwareTopicIdPartition> sortedAssignedPartitionsIter = allAssignedStickyPartitions.iterator();
        RackAwareTopicIdPartition nextAssignedPartition = sortedAssignedPartitionsIter.next();

        for (Uuid topic : sortedAllTopics) {
            int partitionCount = metadataPerTopic.get(topic).numPartitions();

            for (int i = 0; i < partitionCount; i++) {
                if (shouldAddDirectly || !(nextAssignedPartition.topicId().equals(topic) && nextAssignedPartition.partition() == i)) {
                    unassignedPartitions.add(new RackAwareTopicIdPartition(topic, i, null));
                } else {
                    // This partition is in assignedPartitions, don't add to unassignedPartitions, just get next assigned partition.
                    if (sortedAssignedPartitionsIter.hasNext()) {
                        nextAssignedPartition = sortedAssignedPartitionsIter.next();
                    } else {
                        // Add the remaining directly since there is no more sortedAssignedPartitions.
                        shouldAddDirectly = true;
                    }
                }
            }
        }
        return unassignedPartitions;
    }
}
