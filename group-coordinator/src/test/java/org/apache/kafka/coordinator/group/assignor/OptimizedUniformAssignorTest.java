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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OptimizedUniformAssignorTest {
    private final UniformAssignor assignor = new UniformAssignor();
    private final Uuid topic1Uuid = Uuid.fromString("T1-A4s3VTwiI5CTbEp6POw");
    private final Uuid topic2Uuid = Uuid.fromString("T2-B4s3VTwiI5YHbPp6YUe");
    private final Uuid topic3Uuid = Uuid.fromString("T3-CU8fVTLCz5YMkLoDQsa");

    private final String consumerA = "A";
    private final String consumerB = "B";
    private final String consumerC = "C";

    @Test
    public void testOneConsumerNoTopicSubscription() {
        Map<Uuid, AssignmentTopicMetadata> topics = Collections.singletonMap(topic1Uuid, new AssignmentTopicMetadata(3));
        Map<String, AssignmentMemberSpec> members = Collections.singletonMap(
            consumerA,
            new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Collections.emptyMap()
            )
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec);

        assertEquals(Collections.emptyMap(), groupAssignment.members());
    }

    @Test
    public void testOneConsumerSubscribedToNonexistentTopic() {
        Map<Uuid, AssignmentTopicMetadata> topics = Collections.singletonMap(topic1Uuid, new AssignmentTopicMetadata(3));
        Map<String, AssignmentMemberSpec> members = Collections.singletonMap(
            consumerA,
            new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                Collections.singletonList(topic2Uuid),
                Collections.emptyMap()
            )
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);

        assertThrows(PartitionAssignorException.class,
                () -> assignor.assign(assignmentSpec));
    }

    @Test
    public void testFirstAssignmentTwoConsumersSubscribedToTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2),
            mkTopicAssignment(topic3Uuid, 1)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic3Uuid, 0)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testFirstAssignmentNumConsumersGreaterThanTotalNumPartitions() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        // Topic 3 has 2 partitions but three consumers subscribed to it - one of them will not get an assignment
        topics.put(topic3Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic3Uuid),
            Collections.emptyMap()
        ));

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic3Uuid),
            Collections.emptyMap()
        ));

        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic3Uuid, 0)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic3Uuid, 1)
        ));

        expectedAssignment.put(consumerC,
            Collections.emptyMap()
        );

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentForTwoConsumersTwoTopicsGivenUnbalancedPrevAssignment() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1),
                mkTopicAssignment(topic2Uuid, 0, 1)
            )
        );

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 2)
            )
        );

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic2Uuid, 0)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 1, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenPartitionsAreAddedForTwoConsumersTwoTopics() {
        // Simulating adding partition to T1 and T2 - originally T1 -> 3 Partitions and T2 -> 3 Partitions
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(6));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(5));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 2),
                mkTopicAssignment(topic2Uuid, 0)
            )
        );

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 1),
                mkTopicAssignment(topic2Uuid, 1, 2)
            )
        );

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2, 4),
            mkTopicAssignment(topic2Uuid, 0, 3, 4)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1, 3, 5),
            mkTopicAssignment(topic2Uuid, 1, 2)
        ));

        // TOPIC WISE THE LOAD COULD BE SPLIT BETTER, check what the order is
        System.out.println("Computed assignment is " + computedAssignment);
        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerAddedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Arrays.asList(0, 2)));
        currentAssignmentForA.put(topic2Uuid, new HashSet<>(Collections.singletonList(0)));
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, currentAssignmentForA));
        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, new HashSet<>(Collections.singletonList(1)));
        currentAssignmentForB.put(topic2Uuid, new HashSet<>(Arrays.asList(1, 2)));
        members.computeIfAbsent(consumerB, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, currentAssignmentForB));
        // Add a new consumer to trigger a re-assignment
        // Consumer C
        List<Uuid> subscribedTopicsC = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        members.put(consumerC, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsC, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        // Consumer A
        assertStickinessForMember(2, members.get(consumerA).assignedPartitions(), computedAssignment.members().get(consumerA).targetPartitions());
        // Consumer B
        assertStickinessForMember(2, members.get(consumerB).assignedPartitions(), computedAssignment.members().get(consumerB).targetPartitions());

        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerRemovedAfterInitialAssignmentWithThreeConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Collections.singletonList(0)));
        currentAssignmentForA.put(topic2Uuid, new HashSet<>(Collections.singletonList(0)));
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, currentAssignmentForA));
        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, new HashSet<>(Collections.singletonList(1)));
        currentAssignmentForB.put(topic2Uuid, new HashSet<>(Collections.singletonList(1)));
        members.computeIfAbsent(consumerB, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, currentAssignmentForB));
        // Consumer C was removed

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        // Consumer A
        assertStickinessForMember(2, members.get(consumerA).assignedPartitions(), computedAssignment.members().get(consumerA).targetPartitions());
        // Consumer B
        assertStickinessForMember(2, members.get(consumerB).assignedPartitions(), computedAssignment.members().get(consumerB).targetPartitions());

        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneSubscriptionRemovedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(2));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        // New subscription is list is T2
        List<Uuid> subscribedTopicsA = new ArrayList<>(Collections.singletonList(topic2Uuid));
        List<Uuid> subscribedTopicsB = new ArrayList<>(Collections.singletonList(topic2Uuid));
        // Initial subscriptions were [T1, T2]
        // Consumer A
        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Collections.singletonList(0)));
        currentAssignmentForA.put(topic2Uuid, new HashSet<>(Collections.singletonList(0)));
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, currentAssignmentForA));
        // Consumer B
        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, new HashSet<>(Collections.singletonList(1)));
        currentAssignmentForB.put(topic2Uuid, new HashSet<>(Collections.singletonList(1)));
        members.computeIfAbsent(consumerB, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, currentAssignmentForB));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        // Consumer A
        assertStickinessForMember(1, members.get(consumerA).assignedPartitions(), computedAssignment.members().get(consumerA).targetPartitions());
        // Consumer B
        assertStickinessForMember(1, members.get(consumerB).assignedPartitions(), computedAssignment.members().get(consumerB).targetPartitions());

        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneSubscriptionAddedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(2));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(2));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        // New subscription is list is T2
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid));
        List<Uuid> subscribedTopicsB = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid));
        // Initial subscriptions were [T1, T2]
        // Consumer A
        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Collections.singletonList(0)));
        currentAssignmentForA.put(topic2Uuid, new HashSet<>(Collections.singletonList(0)));
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, currentAssignmentForA));
        // Consumer B
        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, new HashSet<>(Collections.singletonList(1)));
        currentAssignmentForB.put(topic2Uuid, new HashSet<>(Collections.singletonList(1)));
        members.computeIfAbsent(consumerB, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, currentAssignmentForB));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        // Consumer A
        assertStickinessForMember(2, members.get(consumerA).assignedPartitions(), computedAssignment.members().get(consumerA).targetPartitions());
        // Consumer B
        assertStickinessForMember(2, members.get(consumerB).assignedPartitions(), computedAssignment.members().get(consumerB).targetPartitions());

        checkValidityAndBalance(members, computedAssignment);
    }

    /**
     * Verifies that the given assignment is valid with respect to the given subscriptions
     * Validity requirements:
     * - each consumer is subscribed to topics of all partitions assigned to it, and
     * - each partition is assigned to no more than one consumer
     * Balance requirements:
     * - the assignment is fully balanced (the numbers of topic partitions assigned to consumers differ by at most one), or
     * - there is no topic partition that can be moved from one consumer to another with 2+ fewer topic partitions
     *
     * @param members: members data structure from assignmentSpec
     * @param computedGroupAssignment: given assignment for balance check
     */
    private void checkValidityAndBalance(Map<String, AssignmentMemberSpec> members, GroupAssignment computedGroupAssignment) {
        List<String> consumers = new ArrayList<>(computedGroupAssignment.members().keySet());
        int numConsumers = consumers.size();
        List<Integer> totalAssignmentSizesOfAllConsumers = new ArrayList<>(consumers.size());
        for (String consumer : consumers) {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = computedGroupAssignment.members().get(consumer).targetPartitions();
            int sum = computedAssignmentForMember.values().stream().mapToInt(Set::size).sum();
            totalAssignmentSizesOfAllConsumers.add(sum);
        }

        for (int i = 0; i < numConsumers; i++) {
            String consumerId = consumers.get(i);
            Map<Uuid, Set<Integer>> computedAssignmentForMember = computedGroupAssignment.members().get(consumerId).targetPartitions();
            // Each consumer is subscribed to topics of all the partitions assigned to it
            for (Uuid topicId : computedAssignmentForMember.keySet()) {
                // Check if the topic exists in the subscription
                assertTrue(members.get(consumerId).subscribedTopicIds().contains(topicId),
                        "Error: Partitions for topic " + topicId + " are assigned to consumer " + consumerId +
                                " but it is not part of the consumers subscription ");
            }

            for (int j = i + 1; j < numConsumers; j++) {
                String otherConsumerId = consumers.get(j);
                Map<Uuid, Set<Integer>> computedAssignmentForOtherMember = computedGroupAssignment.members().get(otherConsumerId).targetPartitions();
                // Each partition should be assigned to at most one member
                for (Uuid topicId : computedAssignmentForMember.keySet()) {
                    Set<Integer> intersection = new HashSet<>();
                    if (computedAssignmentForOtherMember.containsKey(topicId)) {
                        intersection = new HashSet<>(computedAssignmentForMember.get(topicId));
                        intersection.retainAll(computedAssignmentForOtherMember.get(topicId));
                    }
                    assertTrue(intersection.isEmpty(), "Error : Consumer 1 " + consumerId + " and Consumer 2 " + otherConsumerId +
                            "have common partitions assigned to them " + computedAssignmentForOtherMember.get(topicId));
                }

                // Difference in the sizes of any two partitions should be 1 at max
                int size1 = totalAssignmentSizesOfAllConsumers.get(i);
                int size2 = totalAssignmentSizesOfAllConsumers.get(j);
                assertTrue(Math.abs(size1 - size2) <= 1, "Size of one assignment is greater than the other assignment by more than one partition " + size1 + " " + size2 + "abs = " + Math.abs(size1 - size2));
            }
        }
    }
    /**
     * Verifies that each member has the expected number of sticky partitions. Function has to be called per member.
     *
     * @param expectedNumberOfStickyPartitionsForMember: the number of partitions that we expect to retain from the prev assignment
     * @param prevAssignmentForMember: previous assignment of the member
     * @param computedAssignmentForMember: computed assignment of the member
     */
    private void assertStickinessForMember(Integer expectedNumberOfStickyPartitionsForMember, Map<Uuid, Set<Integer>> prevAssignmentForMember, Map<Uuid, Set<Integer>> computedAssignmentForMember) {
        int numberOfStickyPartitions = 0;
        for (Uuid topicId : computedAssignmentForMember.keySet()) {
            Set<Integer> intersection = prevAssignmentForMember.getOrDefault(topicId, new HashSet<>());
            intersection.retainAll(computedAssignmentForMember.get(topicId));
            numberOfStickyPartitions += intersection.size();
        }
        System.out.println("number of sticky partitions " + numberOfStickyPartitions);
        assertTrue(numberOfStickyPartitions >= expectedNumberOfStickyPartitionsForMember, "Expected number of sticky partitions haven't been retained");
    }

    private void assertAssignment(Map<String, Map<Uuid, Set<Integer>>> expectedAssignment, GroupAssignment computedGroupAssignment) {
        assertEquals(expectedAssignment.size(), computedGroupAssignment.members().size());
        for (String memberId : computedGroupAssignment.members().keySet()) {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = computedGroupAssignment.members().get(memberId).targetPartitions();
            assertEquals(expectedAssignment.get(memberId), computedAssignmentForMember);
        }
    }
}
