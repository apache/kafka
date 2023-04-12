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
import org.apache.kafka.coordinator.group.common.TopicIdToPartition;
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
 * Only used when all consumers have identical subscriptions
 * Steps followed to get the most sticky and balanced assignment possible :-
 * 1) In case of a reassignment where a previous assignment exists:
 *      a) Obtain a valid prev assignment by selecting the assignments that have topics present in both the topic metadata and the consumers subscriptions.
 *      b) Get sticky partitions using the newly decided quotas from the prev valid assignment.
 * 2) Get unassigned partitions from the difference between total partitions and assigned sticky partitions.
 * 3) Get a list of potentially unfilled consumers based on the minimum quotas.
 * 4) Populate the unfilled consumers map (consumer, remaining) after accounting for the additional partitions that have to be assigned.
 * 5) Allocate all unassigned partitions to the unfilled consumers first
 */

public class OptimizedAssignmentBuilder extends UniformAssignor.AbstractAssignmentBuilder {
    private static final Logger log = LoggerFactory.getLogger(OptimizedAssignmentBuilder.class);
        // Subscription list is same for all consumers
    private final Collection<Uuid> validSubscriptionList;

    private Integer totalValidPartitionsCount;
    private final int minQuota;
        // the expected number of members receiving more than minQuota partitions (zero when minQuota == maxQuota)
    private int expectedNumMembersWithExtraPartition;
    // Consumers which haven't met the min quota OR have met the min Quota but could potentially get an extra partition
    // Map<MemberId, Remaining> where Remaining = number of partitions remaining to meet the min Quota
    private final Map<String, Integer> potentiallyUnfilledConsumers;
    // Consumers that need to be assigning remaining number of partitions including extra partitions
    private Map<String, Integer> unfilledConsumers;
    // Partitions that we want to retain from the members previous assignment
    private final Map<String, List<TopicIdToPartition>> assignedStickyPartitionsPerMember;
    // Partitions that are available to be assigned, computed by taking the difference between total partitions and assigned sticky partitions
    private List<TopicIdToPartition> unassignedPartitions;

    private Map<String, List<TopicIdToPartition>> fullAssignment;

    OptimizedAssignmentBuilder(AssignmentSpec assignmentSpec) {
        super(assignmentSpec);

        validSubscriptionList = new ArrayList<>();
        List<Uuid> givenSubscriptionList = assignmentSpec.members.values().iterator().next().subscribedTopics;
        // Only add topicIds from the subscription list that are still present in the topicMetadata
        for (Uuid topicId : givenSubscriptionList) {
            if (assignmentSpec.topics.containsKey(topicId)) {
                validSubscriptionList.add(topicId);
            } else {
                log.info("The subscribed topic : " + topicId + " doesn't exist in the topic metadata ");
            }
        }
        System.out.println("subscribed topics list is " + validSubscriptionList);
        totalValidPartitionsCount = 0;
        for (Uuid topicId : validSubscriptionList) {
            totalValidPartitionsCount += assignmentSpec.topics.get(topicId).numPartitions;
        }
        System.out.println("total valid partitions count " + totalValidPartitionsCount);
        int numberOfConsumers = metadataPerMember.size();
        minQuota = (int) Math.floor(((double) totalValidPartitionsCount) / numberOfConsumers);
        expectedNumMembersWithExtraPartition = totalValidPartitionsCount % numberOfConsumers;
        potentiallyUnfilledConsumers = new HashMap<>();
        unfilledConsumers = new HashMap<>();
        assignedStickyPartitionsPerMember = new HashMap<>();
        fullAssignment = new HashMap<>();

    }

    @Override
    Map<String, List<TopicIdToPartition>> build() {
        if (log.isDebugEnabled()) {
            log.debug("Performing constrained assign with MetadataPerTopic: {}, metadataPerMember: {}.",
                    metadataPerMember, metadataPerTopic);
        }
        if (validSubscriptionList.isEmpty()) {
            log.info("Valid subscriptions list is empty, returning empty assignment");
            return new HashMap<>();
        }
        List<TopicIdToPartition> allAssignedStickyPartitions = getAssignedStickyPartitions();
        System.out.println("All assigned sticky Partitions = " + allAssignedStickyPartitions);

        addAssignedStickyPartitionsToNewAssignment();
        System.out.println("Full assignment after filling with sticky partitions " + fullAssignment);

        unassignedPartitions = getUnassignedPartitions(allAssignedStickyPartitions);
        System.out.println("Unassigned partitions " + unassignedPartitions);

        unfilledConsumers = getUnfilledConsumers();
        System.out.println("Unfilled consumers " + unfilledConsumers);

        if (!ensureTotalUnassignedPartitionsEqualsTotalRemainingAssignments()) {
            log.error("Number of available partitions is not equal to the total requirement");
        }

        allocateUnassignedPartitions();
        System.out.println("After filling the unfilled ones with available partitions the assignment is " + fullAssignment);
        // assign the assignedStickyPartitions, do computeIfAbsent since unfilled doesn't have all the consumers
        return fullAssignment;
    }

    // Keep the partitions in the assignment only if they are still part of the new topic metadata and the consumers subscriptions.
    private List<TopicIdToPartition> getValidCurrentAssignment(AssignmentMemberSpec assignmentMemberSpec) {
        List<TopicIdToPartition> validCurrentAssignmentList = new ArrayList<>();
        for (Map.Entry<Uuid, Set<Integer>> currentAssignment : assignmentMemberSpec.currentAssignmentPerTopic.entrySet()) {
            Uuid topicId = currentAssignment.getKey();
            List<Integer> currentAssignmentList = new ArrayList<>(currentAssignment.getValue());
            if (metadataPerTopic.containsKey(topicId) && validSubscriptionList.contains(topicId)) {
                for (Integer partition : currentAssignmentList) {
                    validCurrentAssignmentList.add(new TopicIdToPartition(topicId, partition, null));
                }
            }
        }
        return validCurrentAssignmentList;
    }

    // Returns all the previously assigned partitions that we want to retain
    // Fills potentially unfilled consumers based on the remaining number of partitions required to meet the minQuota
    private List<TopicIdToPartition> getAssignedStickyPartitions() {
        List<TopicIdToPartition> allAssignedStickyPartitions = new ArrayList<>();
        for (Map.Entry<String, AssignmentMemberSpec> assignmentMemberSpecEntry : metadataPerMember.entrySet()) {
            String memberId = assignmentMemberSpecEntry.getKey();
            List<TopicIdToPartition> assignedStickyListForMember = new ArrayList<>();
            // Remove all the topics that aren't in the subscriptions or the topic metadata anymore
            List<TopicIdToPartition> validCurrentAssignment = getValidCurrentAssignment(metadataPerMember.get(memberId));
            System.out.println("valid current assignment for member " + memberId + " is " + validCurrentAssignment);
            int currentAssignmentSize = validCurrentAssignment.size();

            int remaining = minQuota - currentAssignmentSize;

            if (currentAssignmentSize > 0) {
                // We either need to retain currentSize number of partitions when currentSize < required OR required number of partitions otherwise.
                int retainedPartitionsCount = min(currentAssignmentSize, minQuota);
                for (int i = 0; i < retainedPartitionsCount; i++) {
                    assignedStickyListForMember.add(validCurrentAssignment.get(i));
                }
                if (remaining < 0 && expectedNumMembersWithExtraPartition > 0) {
                    assignedStickyListForMember.add(validCurrentAssignment.get(retainedPartitionsCount));
                    expectedNumMembersWithExtraPartition--;
                }
                assignedStickyPartitionsPerMember.put(memberId, assignedStickyListForMember);
                allAssignedStickyPartitions.addAll(assignedStickyListForMember);
            }
            if (remaining >= 0) {
                potentiallyUnfilledConsumers.put(memberId, remaining);
            }

        }
        System.out.println(" Potentially unfilled consumers " + potentiallyUnfilledConsumers);
        return allAssignedStickyPartitions;
    }

    private void addAssignedStickyPartitionsToNewAssignment() {
        for (String memberId : metadataPerMember.keySet()) {
            fullAssignment.computeIfAbsent(memberId, k -> new ArrayList<>());
            if (assignedStickyPartitionsPerMember.containsKey(memberId)) {
                fullAssignment.get(memberId).addAll(assignedStickyPartitionsPerMember.get(memberId));
            }
        }
    }

    private boolean ensureTotalUnassignedPartitionsEqualsTotalRemainingAssignments() {
        int totalRemaining = 0;
        for (Map.Entry<String, Integer> unfilledEntry  : unfilledConsumers.entrySet()) {
            totalRemaining += unfilledEntry.getValue();
        }
        return totalRemaining == unassignedPartitions.size();
    }

    // The unfilled map has consumers mapped to the remaining partitions number = max allocation to this consumer
    // The algorithm below assigns each consumer partitions in a round-robin fashion up to its max limit
    private void allocateUnassignedPartitions() {
        // Since the map doesn't guarantee order we need a list of consumerIds to map each consumer to a particular index
        List<String> consumerIds = new ArrayList<>(unfilledConsumers.keySet());
        int[] currentIndexForConsumer = new int[consumerIds.size()];

        for (String consumerId : consumerIds) {
            fullAssignment.computeIfAbsent(consumerId, k -> new ArrayList<>());
        }

        int numConsumers = unfilledConsumers.size();
        for (int i = 0; i < unassignedPartitions.size(); i++) {
            int consumerIndex = i % numConsumers;
            int consumerLimit = unfilledConsumers.get(consumerIds.get(consumerIndex));
            // If the current consumer has reached its limit, find a consumer that has more space available in its assignment
            while (currentIndexForConsumer[consumerIndex] >= consumerLimit) {
                consumerIndex = (consumerIndex + 1) % numConsumers;
                consumerLimit = unfilledConsumers.get(consumerIds.get(consumerIndex));
            }
            if (currentIndexForConsumer[consumerIndex] < consumerLimit) {
                fullAssignment.get(consumerIds.get(consumerIndex)).add(currentIndexForConsumer[consumerIndex]++, unassignedPartitions.get(i));
            }
        }
    }
    private Map<String, Integer> getUnfilledConsumers() {
        Map<String, Integer> unfilledConsumers = new HashMap<>();
        for (Map.Entry<String, Integer> potentiallyUnfilledConsumerEntry : potentiallyUnfilledConsumers.entrySet()) {
            String memberId = potentiallyUnfilledConsumerEntry.getKey();
            Integer remaining = potentiallyUnfilledConsumerEntry.getValue();
            if (expectedNumMembersWithExtraPartition > 0) {
                remaining++;
                expectedNumMembersWithExtraPartition--;
            }
            // If remaining is 0 since there were no more consumers required to get an extra partition we don't add it to the unfilled list
            if (remaining > 0) {
                unfilledConsumers.put(memberId, remaining);
            }
        }
        return unfilledConsumers;
    }
    private List<TopicIdToPartition> getUnassignedPartitions(List<TopicIdToPartition> allAssignedStickyPartitions) {
        List<TopicIdToPartition> unassignedPartitions = new ArrayList<>();
        // We only care about the topics that the consumers are subscribed to
        List<Uuid> sortedAllTopics = new ArrayList<>(validSubscriptionList);
        Collections.sort(sortedAllTopics);
        if (allAssignedStickyPartitions.isEmpty()) {
            return getAllTopicPartitions(sortedAllTopics);
        }
        Collections.sort(allAssignedStickyPartitions, Comparator.comparing(TopicIdToPartition::topicId).thenComparing(TopicIdToPartition::partition));
        // use two pointer approach and get the partitions that are in total but not in assigned
        boolean shouldAddDirectly = false;
        Iterator<TopicIdToPartition> sortedAssignedPartitionsIter = allAssignedStickyPartitions.iterator();
        TopicIdToPartition nextAssignedPartition = sortedAssignedPartitionsIter.next();

        for (Uuid topic : sortedAllTopics) {
            int partitionCount = metadataPerTopic.get(topic).numPartitions;

            for (int i = 0; i < partitionCount; i++) {
                if (shouldAddDirectly || !(nextAssignedPartition.topicId().equals(topic) && nextAssignedPartition.partition() == i)) {
                    unassignedPartitions.add(new TopicIdToPartition(topic, i, null));
                } else {
                    // this partition is in assignedPartitions, don't add to unassignedPartitions, just get next assigned partition
                    if (sortedAssignedPartitionsIter.hasNext()) {
                        nextAssignedPartition = sortedAssignedPartitionsIter.next();
                    } else {
                        // add the remaining directly since there is no more sortedAssignedPartitions
                        shouldAddDirectly = true;
                    }
                }
            }
        }
        return unassignedPartitions;
    }
}
