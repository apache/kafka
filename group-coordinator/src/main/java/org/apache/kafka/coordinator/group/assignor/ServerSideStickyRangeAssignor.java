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

/**
 * <p>The Server Side Sticky Range Assignor inherits properties of both the range assignor and the sticky assignor.
 * Properties are :-
 * <ul>
 * <li> 1) Each consumer must get at least one partition per topic that it is subscribed to whenever the number of consumers is
 *    less than or equal to the number of partitions for that topic. (Range) </li>
 * <li> 2) Partitions should be assigned to consumers in a way that facilitates join operations where required. (Range) </li>
 *    This can only be done if the topics are co-partitioned in the first place
 *    Co-partitioned:-
 *    Two streams are co-partitioned if the following conditions are met:-
 * ->The keys must have the same schemas
 * ->The topics involved must have the same number of partitions
 * <li> 3) Consumers should retain as much as their previous assignment as possible. (Sticky) </li>
 * </ul>
 * </p>
 *
 * <p>The algorithm works mainly in 5 steps described below
 * <ul>
 * <li> 1) Get a map of the consumersPerTopic created using the member subscriptions.</li>
 * <li> 2) Get a list of consumers (potentiallyUnfilled) that have not met the minimum required quota for assignment AND
 * get a list of sticky partitions that we want to retain in the new assignment.</li>
 * <li> 3) Add consumers from potentiallyUnfilled to Unfilled if they haven't met the total required quota = minQuota + (if necessary) extraPartition </li>
 * <li> 4) Get a list of available partitions by calculating the difference between total partitions and assigned sticky partitions </li>
 * <li> 5) Iterate through unfilled consumers and assign partitions from available partitions </li>
 * </ul>
 * </p>
 *
 */
package org.apache.kafka.coordinator.group.assignor;


import org.apache.kafka.common.Uuid;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Collections;

import static java.lang.Math.min;

public class ServerSideStickyRangeAssignor implements PartitionAssignor {

    public static final String RANGE_ASSIGNOR_NAME = "range-sticky";

    @Override
    public String name() {
        return RANGE_ASSIGNOR_NAME;
    }

    protected static <K, V> void putList(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.computeIfAbsent(key, k -> new ArrayList<>());
        list.add(value);
    }

    protected static <K, V> void putSet(Map<K, Set<V>> map, K key, V value) {
        Set<V> set = map.computeIfAbsent(key, k -> new HashSet<>());
        set.add(value);
    }

    static class Pair<T, U> {
        private final T first;
        private final U second;

        public Pair(T first, U second) {
            this.first = first;
            this.second = second;
        }

        public T getFirst() {
            return first;
        }

        public U getSecond() {
            return second;
        }

        @Override
        public String toString() {
            return "(" + first + ", " + second + ")";
        }
    }

    // Returns a map of the list of consumers per Topic (keyed by topicId)
    private Map<Uuid, List<String>> consumersPerTopic(AssignmentSpec assignmentSpec) {
        Map<Uuid, List<String>> mapTopicsToConsumers = new HashMap<>();
        Map<String, AssignmentMemberSpec> membersData = assignmentSpec.members;

        for (Map.Entry<String, AssignmentMemberSpec> memberEntry : membersData.entrySet()) {
            String memberId = memberEntry.getKey();
            AssignmentMemberSpec memberMetadata = memberEntry.getValue();
            List<Uuid> topics = memberMetadata.subscribedTopics;
            for (Uuid topicId: topics) {
                putList(mapTopicsToConsumers, topicId, memberId);
            }
        }
        return mapTopicsToConsumers;
    }

    private Map<Uuid, List<Integer>> getAvailablePartitionsPerTopic(AssignmentSpec assignmentSpec, Map<Uuid, Set<Integer>> assignedStickyPartitionsPerTopic) {
        Map<Uuid, List<Integer>> availablePartitionsPerTopic = new HashMap<>();
        Map<Uuid, AssignmentTopicMetadata> topicsMetadata = assignmentSpec.topics;
        // Iterate through the topics map provided in assignmentSpec
        for (Map.Entry<Uuid, AssignmentTopicMetadata> topicMetadataEntry : topicsMetadata.entrySet()) {
            Uuid topicId = topicMetadataEntry.getKey();
            availablePartitionsPerTopic.put(topicId, new ArrayList<>());
            int numPartitions = topicsMetadata.get(topicId).numPartitions;
            // since the loop iterates from 0 to n, the partitions will be in ascending order within the list of available partitions per topic
            Set<Integer> assignedStickyPartitionsForTopic = assignedStickyPartitionsPerTopic.get(topicId);
            for (int i = 0; i < numPartitions; i++) {
                if (assignedStickyPartitionsForTopic == null || !assignedStickyPartitionsForTopic.contains(i)) {
                    availablePartitionsPerTopic.get(topicId).add(i);
                }
            }
        }
        return availablePartitionsPerTopic;
    }

    @Override
    public GroupAssignment assign(AssignmentSpec assignmentSpec) throws PartitionAssignorException {
        Map<String, Map<Uuid, Set<Integer>>> membersWithNewAssignmentPerTopic = new HashMap<>();
        // Step 1
        Map<Uuid, List<String>> consumersPerTopic = consumersPerTopic(assignmentSpec);
        // Step 2
        Map<Uuid, List<Pair<String, Integer>>> unfilledConsumersPerTopic = new HashMap<>();
        Map<Uuid, Set<Integer>> assignedStickyPartitionsPerTopic = new HashMap<>();

        for (Map.Entry<Uuid, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            Uuid topicId = topicEntry.getKey();
            // For each topic we have a temporary list of consumers stored in potentiallyUnfilledConsumers.
            // The list is populated with consumers that satisfy one of the two conditions :-
            // 1) Consumers that have the minimum required number of partitions .i.e numPartitionsPerConsumer BUT they could be assigned an extra partition later on.
            //    In this case we add the consumer to the unfilled consumers map iff an extra partition needs to be assigned to it.
            // 2) Consumers that don't have the minimum required partitions, so irrespective of whether they get an extra partition or not they get added to the unfilled map later.
            List<Pair<String, Integer>> potentiallyUnfilledConsumers = new ArrayList<>();
            List<String> consumersForTopic = topicEntry.getValue();

            AssignmentTopicMetadata topicData = assignmentSpec.topics.get(topicId);
            int numPartitionsForTopic = topicData.numPartitions;
            int numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size();
            // Each consumer can get only one extra partition per topic after receiving the minimum quota = numPartitionsPerConsumer
            int numConsumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size();

            for (String memberId: consumersForTopic) {
                // Size of the older assignment, this will be 0 when assign is called for the first time.
                // The older assignment is required when a reassignment occurs to ensure stickiness.
                int currentAssignmentSize = 0;
                List<Integer> currentAssignmentListForTopic = new ArrayList<>();
                // Convert the set to a list first and sort the partitions in numeric order since we want the same partition numbers from each topic
                // to go to the same consumer in case of co-partitioned topics.
                Set<Integer> currentAssignmentSetForTopic =  assignmentSpec.members.get(memberId).currentAssignmentPerTopic.get(topicId);
                // We want to make sure that the currentAssignment is not null to avoid a null pointer exception
                if (currentAssignmentSetForTopic != null) {
                    currentAssignmentListForTopic = new ArrayList<>(currentAssignmentSetForTopic);
                    currentAssignmentSize = currentAssignmentListForTopic.size();
                }
                // Initially, minRequiredQuota is the minimum number of partitions that each consumer should get.
                // The value is at least 1 unless numConsumers subscribed is greater than numPartitions. In such cases, all consumers get assigned "extra partitions".
                int minRequiredQuota = numPartitionsPerConsumer;

                // If there are previously assigned partitions present, we want to retain
                if (currentAssignmentSize > 0) {
                    // We either need to retain currentSize number of partitions when currentSize < required OR required number of partitions otherwise.
                    int retainedPartitionsCount = min(currentAssignmentSize, minRequiredQuota);
                    Collections.sort(currentAssignmentListForTopic);
                    for (int i = 0; i < retainedPartitionsCount; i++) {
                        putSet(assignedStickyPartitionsPerTopic, topicId, currentAssignmentListForTopic.get(i));
                        membersWithNewAssignmentPerTopic.computeIfAbsent(memberId, k -> new HashMap<>()).computeIfAbsent(topicId, k -> new HashSet<>()).add(currentAssignmentListForTopic.get(i));
                    }
                }
                // Number of partitions left to reach the minRequiredQuota
                int remaining = minRequiredQuota - currentAssignmentSize;

                // There are 3 cases w.r.t value of remaining
                // 1) remaining < 0 this means that the consumer has more than the min required amount.
                // It could have an extra partition, so we check for that.
                if (remaining < 0 && numConsumersWithExtraPartition > 0) {
                    // In order to remain as sticky as possible, since the order of members can be different, we want the consumers that already had extra
                    // partitions to retain them if it's still required, instead of assigning the extras to the first few consumers directly.
                    // Ex:- If two consumers out of 3 are supposed to get an extra partition and currently 1 of them already has the extra, we want this consumer
                    // to retain it first and later if we have partitions left they will be assigned to the first few consumers and the unfilled map is updated
                    numConsumersWithExtraPartition--;
                    // Since we already added the minimumRequiredQuota of partitions in the previous step (until minReq - 1), we just need to
                    // add the extra partition which will be present at the index right after min quota is satisfied.
                    putSet(assignedStickyPartitionsPerTopic, topicId, currentAssignmentListForTopic.get(minRequiredQuota));
                    membersWithNewAssignmentPerTopic.computeIfAbsent(memberId, k -> new HashMap<>()).computeIfAbsent(topicId, k -> new HashSet<>()).add(currentAssignmentListForTopic.get(minRequiredQuota));
                } else {
                    // 3) If remaining = 0 it has min req partitions but there is scope for getting an extra partition later on, so it is a potentialUnfilledConsumer.
                    // 4) If remaining > 0 it doesn't even have the min required partitions, and it definitely is unfilled so it should be added to potentialUnfilledConsumers.
                    Pair<String, Integer> newPair = new Pair<>(memberId, remaining);
                    potentiallyUnfilledConsumers.add(newPair);
                }
            }

            // Step 3
            // Iterate through potentially unfilled consumers and if remaining > 0 after considering the extra partitions assignment, add to the unfilled list.
            for (Pair<String, Integer> pair : potentiallyUnfilledConsumers) {
                String memberId = pair.getFirst();
                Integer remaining = pair.getSecond();
                if (numConsumersWithExtraPartition > 0) {
                    remaining++;
                    numConsumersWithExtraPartition--;
                }
                if (remaining > 0) {
                    Pair<String, Integer> newPair = new Pair<>(memberId, remaining);
                    putList(unfilledConsumersPerTopic, topicId, newPair);
                }
            }
        }

        // Step 4
        // Find the difference between the total partitions per topic and the already assigned sticky partitions for the topic to get available partitions.
        Map<Uuid, List<Integer>> availablePartitionsPerTopic = getAvailablePartitionsPerTopic(assignmentSpec, assignedStickyPartitionsPerTopic);

        // Step 5
        // Iterate through the unfilled consumers list and assign remaining number of partitions from the availablePartitions list
        for (Map.Entry<Uuid, List<Pair<String, Integer>>> unfilledEntry : unfilledConsumersPerTopic.entrySet()) {
            Uuid topicId = unfilledEntry.getKey();
            List<Pair<String, Integer>> unfilledConsumersForTopic = unfilledEntry.getValue();
            int newStartPointer = 0;
            for (Pair<String, Integer> stringIntegerPair : unfilledConsumersForTopic) {
                String memberId = stringIntegerPair.getFirst();
                int remaining = stringIntegerPair.getSecond();
                // assign the first few partitions from the list and then delete them from the availablePartitions list for this topic
                List<Integer> subList = availablePartitionsPerTopic.get(topicId).subList(newStartPointer, newStartPointer + remaining);
                newStartPointer += remaining;
                membersWithNewAssignmentPerTopic.computeIfAbsent(memberId, k -> new HashMap<>()).computeIfAbsent(topicId, k -> new HashSet<>()).addAll(subList);
            }
        }

        // Consolidate the maps into MemberAssignment and then finally map each consumer to a MemberAssignment.
        Map<String, MemberAssignment> membersWithNewAssignment = new HashMap<>();
        for (Map.Entry<String, Map<Uuid, Set<Integer>>> consumer : membersWithNewAssignmentPerTopic.entrySet()) {
            String consumerId = consumer.getKey();
            Map<Uuid, Set<Integer>> assignmentPerTopic = consumer.getValue();
            membersWithNewAssignment.computeIfAbsent(consumerId, k -> new MemberAssignment(assignmentPerTopic));
        }

        return new GroupAssignment(membersWithNewAssignment);
    }
}

