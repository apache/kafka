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

import org.apache.kafka.coordinator.group.common.TopicIdToPartition;
import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.lang.Math.min;

public class UniformAssignor implements PartitionAssignor {

    private static final Logger log = LoggerFactory.getLogger(UniformAssignor.class);
    public static final String UNIFORM_ASSIGNOR_NAME = "uniform";
    @Override
    public String name() {
        return UNIFORM_ASSIGNOR_NAME;
    }

    @Override
    public GroupAssignment assign(AssignmentSpec assignmentSpec) throws PartitionAssignorException {
        AbstractAssignmentBuilder assignmentBuilder = null;
        if (allSubscriptionsEqual(assignmentSpec.members)) {
            log.debug("Detected that all consumers were subscribed to same set of topics, invoking the "
                    + "optimized assignment algorithm");
            assignmentBuilder = new OptimisedAssignmentBuilder(assignmentSpec);
        }
        Map<String, List<TopicIdToPartition>> assignmentInTopicIdToPartitionFormat = assignmentBuilder.build();
        return assignmentBuilder.assignmentInCorrectFormat(assignmentInTopicIdToPartitionFormat);
    }

    private boolean allSubscriptionsEqual(Map<String, AssignmentMemberSpec> members) {
        boolean areAllSubscriptionsEqual = true;
        List<Uuid> firstSubscriptionList = members.values().iterator().next().subscribedTopics;
        for (AssignmentMemberSpec memberSpec : members.values()) {
            if (!firstSubscriptionList.equals(memberSpec.subscribedTopics)) {
                areAllSubscriptionsEqual = false;
                break;
            }
        }
        return areAllSubscriptionsEqual;
    }
    private static abstract class AbstractAssignmentBuilder {

        final Map<Uuid, AssignmentTopicMetadata> metadataPerTopic;
        final Map<String, AssignmentMemberSpec> metadataPerMember;
        final int totalPartitionsCount;

        AbstractAssignmentBuilder(AssignmentSpec assignmentSpec) {
            this.metadataPerTopic = assignmentSpec.topics;
            this.metadataPerMember = assignmentSpec.members;
            this.totalPartitionsCount = getTotalPartitionsCount(assignmentSpec.topics);
        }

        /**
         * Builds the assignment.
         *
         * @return Map from each member to the list of partitions assigned to them.
         */
        abstract Map<String, List<TopicIdToPartition>> build();

        protected GroupAssignment assignmentInCorrectFormat(Map<String, List<TopicIdToPartition>> computedAssignment) {
            Map<String, MemberAssignment> members = new HashMap<>();
            for (String member : metadataPerMember.keySet()) {
                List<TopicIdToPartition> assignment = computedAssignment.get(member);
                Map<Uuid, Set<Integer>> topicToSetOfPartitions = new HashMap<>();
                for (TopicIdToPartition topicIdPartition : assignment) {
                    Uuid topicId = topicIdPartition.topicId();
                    Integer partition = topicIdPartition.partition();
                    topicToSetOfPartitions.computeIfAbsent(topicId, k -> new HashSet<>());
                    topicToSetOfPartitions.get(topicId).add(partition);
                }
                members.put(member, new MemberAssignment(topicToSetOfPartitions));
            }
            return new GroupAssignment(members);
        }
        protected Integer getTotalPartitionsCount(Map<Uuid, AssignmentTopicMetadata> topicMetadataMap) {
            int totalPartitionsCount = 0;
            for (AssignmentTopicMetadata topicMetadata : topicMetadataMap.values()) {
                totalPartitionsCount += topicMetadata.numPartitions;
            }
            return totalPartitionsCount;
        }

        protected List<TopicIdToPartition> getAllTopicPartitions(List<Uuid> listAllTopics) {
            List<TopicIdToPartition> allPartitions = new ArrayList<>(totalPartitionsCount);
            for (Uuid topic : listAllTopics) {
                int partitionCount = metadataPerTopic.get(topic).numPartitions;
                for (int i = 0; i < partitionCount; ++i) {
                    allPartitions.add(new TopicIdToPartition(topic, i, null));
                }
            }
            return allPartitions;
        }
    }

    /**
     * Only used when all consumers have identical subscriptions
     *
     * Steps followed to get the most sticky and balanced assignment possible :-
     * 1) In case of a reassignment where a previous assignment exists:
     *      a) Obtain a valid prev assignment by selecting the assignments that have topics present in both the topic metadata and the consumers subscriptions.
     *      b) Get sticky partitions using the newly decided quotas from the prev valid assignment.
     * 2) Get unassigned partitions from the difference between total partitions and assigned sticky partitions.
     * 3) Get a list of potentially unfilled consumers based on the minimum quotas.
     * 4) Populate the unfilled consumers map (consumer, remaining) after accounting for the additional partitions that have to be assigned.
     * 5) Allocate all unassigned partitions to the unfilled consumers first
     */
    private class OptimisedAssignmentBuilder extends AbstractAssignmentBuilder {
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

        OptimisedAssignmentBuilder(AssignmentSpec assignmentSpec) {
            super(assignmentSpec);
            int numberOfConsumers = metadataPerMember.size();
            minQuota = (int) Math.floor(((double) totalPartitionsCount) / numberOfConsumers);
            expectedNumMembersWithExtraPartition = totalPartitionsCount % numberOfConsumers;

            potentiallyUnfilledConsumers = new HashMap<>();
            unfilledConsumers = new HashMap<>();
            assignedStickyPartitionsPerMember = new HashMap<>();
        }

        @Override
        Map<String, List<TopicIdToPartition>> build() {
            if (log.isDebugEnabled()) {
                log.debug("Performing constrained assign with MetadataPerTopic: {}, metadataPerMember: {}.",
                        metadataPerMember, metadataPerTopic);
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
        private List<TopicIdToPartition> getValidCurrentAssignment(String memberId, AssignmentMemberSpec assignmentMemberSpec) {
            List<TopicIdToPartition> validCurrentAssignmentList = new ArrayList<>();
            for (Map.Entry<Uuid, Set<Integer>> currentAssignment : assignmentMemberSpec.currentAssignmentPerTopic.entrySet()) {
                Uuid topicId = currentAssignment.getKey();
                List<Integer> currentAssignmentList = new ArrayList<>(currentAssignment.getValue());
                if (metadataPerTopic.containsKey(topicId) && metadataPerMember.get(memberId).subscribedTopics.contains(topicId)) {
                    for (Integer partition : currentAssignmentList) {
                        validCurrentAssignmentList.add(new TopicIdToPartition(topicId, partition, null));
                    }
                } else {
                    assignmentMemberSpec.currentAssignmentPerTopic.remove(topicId);
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
                List<TopicIdToPartition> validCurrentAssignment = getValidCurrentAssignment(memberId, metadataPerMember.get(memberId));
                System.out.println("valid current assignment is " + validCurrentAssignment);
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
            return allAssignedStickyPartitions;
        }

        private void addAssignedStickyPartitionsToNewAssignment() {
            for (String memberId : metadataPerMember.keySet()) {
                fullAssignment.computeIfAbsent(memberId, k -> new ArrayList<>());
                List<TopicIdToPartition> assignmentForMember = new ArrayList<>();
                if (assignedStickyPartitionsPerMember.containsKey(memberId)) {
                    fullAssignment.get(memberId).addAll(assignedStickyPartitionsPerMember.get(memberId));
                }
            }
        }

        private boolean ensureTotalUnassignedPartitionsEqualsTotalRemainingAssignments() {
            int totalRemaining = 0;
            for (String consumer : unfilledConsumers.keySet()) {
                totalRemaining += unfilledConsumers.get(consumer);
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
            List<Uuid> sortedAllTopics = new ArrayList<>(metadataPerTopic.keySet());
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
}
