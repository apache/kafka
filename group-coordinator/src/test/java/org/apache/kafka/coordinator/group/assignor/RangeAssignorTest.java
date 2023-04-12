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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangeAssignorTest {

    private final RangeAssignor assignor = new RangeAssignor();

    private final String topic1Name = "topic1";
    private final Uuid topic1Uuid = Uuid.randomUuid();

    private final String topic2Name = "topic2";
    private final Uuid topic2Uuid = Uuid.randomUuid();

    private final String topic3Name = "topic3";
    private final Uuid topic3Uuid = Uuid.randomUuid();

    private final String consumerA = "A";
    private final String consumerB = "B";
    private final String consumerC = "C";

    @Test
    public void testOneConsumerNoTopic() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 3));
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        List<Uuid> subscribedTopics = new ArrayList<>();
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopics, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec);

        assertTrue(groupAssignment.getMembers().isEmpty());
    }

    @Test
    public void testFirstAssignmentTwoConsumersTwoTopicsSameSubscriptions() {
        // A -> T1, T3 // B -> T1, T3 // T1 -> 3 Partitions // T3 -> 2 Partitions
        // Topics
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(topic3Name, 2));
        // Members
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic3Uuid));
        members.put(consumerA, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, new HashMap<>()));
        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Arrays.asList(topic1Uuid, topic3Uuid));
        members.put(consumerB, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(0, 1)));
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(2)));
        // Topic 3 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic3Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(0)));
        expectedAssignment.computeIfAbsent(topic3Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(1)));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentThreeConsumersThreeTopicsDifferentSubscriptions() {
        // A -> T1, T2 // B -> T3 // C -> T2, T3 // T1 -> 3 Partitions // T2 -> 3 Partitions // T3 -> 2 Partitions
        // Topics
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(topic2Name, 3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(topic3Name, 2));
        // Members
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        members.put(consumerA, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, new HashMap<>()));
        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Collections.singletonList(topic3Uuid));
        members.put(consumerB, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, new HashMap<>()));
        // Consumer C
        List<Uuid> subscribedTopicsC = new ArrayList<>(Arrays.asList(topic2Uuid, topic3Uuid));
        members.put(consumerC, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsC, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(0, 1, 2)));
        // Topic 2 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(0, 1)));
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(2)));
        // Topic 3 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic3Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(0)));
        expectedAssignment.computeIfAbsent(topic3Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(1)));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentNumConsumersGreaterThanNumPartitions() {
        // Topics
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 3));
        // Topic 3 has 2 partitions but three consumers subscribed to it - one of them will not get an assignment
        topics.put(topic3Uuid, new AssignmentTopicMetadata(topic3Name, 2));
        // Members
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic3Uuid));
        members.put(consumerA, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, new HashMap<>()));
        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Arrays.asList(topic1Uuid, topic3Uuid));
        members.put(consumerB, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, new HashMap<>()));
        // Consumer C
        List<Uuid> subscribedTopicsC = new ArrayList<>(Arrays.asList(topic1Uuid, topic3Uuid));
        members.put(consumerC, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsC, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(0)));
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(1)));
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(2)));
        // Topic 2 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic3Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(0)));
        expectedAssignment.computeIfAbsent(topic3Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(1)));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentNumConsumersGreaterThanNumPartitionsWhenOneConsumerAdded() {
        // Topics
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 2));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(topic2Name, 2));
        // Members
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Add a new consumer to trigger a re-assignment
        // Consumer C
        List<Uuid> subscribedTopicsC = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        members.put(consumerC, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsC, new HashMap<>()));
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

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(0)));
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(1)));
        // Topic 2 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(0)));
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(1)));

        // Consumer C shouldn't get any assignment, due to stickiness A, B retain their assignments
        assertNull(computedAssignment.getMembers().get(consumerC));
        assertAssignment(expectedAssignment, computedAssignment);
        assertCoPartitionJoinProperty(computedAssignment);
    }

    @Test
    public void testReassignmentWhenOnePartitionAddedForTwoConsumersTwoTopics() {
        // T1, T2 both have 3 partitions each when first assignment was calculated -> currentAssignmentForX
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Arrays.asList(0, 1)));
        currentAssignmentForA.put(topic2Uuid, new HashSet<>(Arrays.asList(0, 1)));
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, currentAssignmentForA));

        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, new HashSet<>(Collections.singletonList(2)));
        currentAssignmentForB.put(topic2Uuid, new HashSet<>(Collections.singletonList(2)));
        members.computeIfAbsent(consumerB, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, currentAssignmentForB));

        // Simulating adding a partition - originally T1 -> 3 Partitions and T2 -> 3 Partitions
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 4));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(topic2Name, 4));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(0, 1)));
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(2, 3)));
        // Topic 2 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(0, 1)));
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(2, 3)));

        assertAssignment(expectedAssignment, computedAssignment);
        assertCoPartitionJoinProperty(computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerAddedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(topic2Name, 3));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Arrays.asList(0, 1)));
        currentAssignmentForA.put(topic2Uuid, new HashSet<>(Arrays.asList(0, 1)));
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, currentAssignmentForA));
        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, new HashSet<>(Collections.singletonList(2)));
        currentAssignmentForB.put(topic2Uuid, new HashSet<>(Collections.singletonList(2)));
        members.computeIfAbsent(consumerB, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, currentAssignmentForB));
        // Add a new consumer to trigger a re-assignment
        // Consumer C
        List<Uuid> subscribedTopicsC = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        members.put(consumerC, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsC, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(0)));
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(2)));
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(1)));
        // Topic 2 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(0)));
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(2)));
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(1)));

        assertAssignment(expectedAssignment, computedAssignment);
        assertCoPartitionJoinProperty(computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerRemovedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(topic2Name, 3));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A was removed

        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid));
        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, new HashSet<>(Collections.singletonList(2)));
        currentAssignmentForB.put(topic2Uuid, new HashSet<>(Collections.singletonList(2)));
        members.computeIfAbsent(consumerB, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, currentAssignmentForB));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(0, 1, 2)));
        // Topic 2 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(0, 1, 2)));

        assertAssignment(expectedAssignment, computedAssignment);
        assertCoPartitionJoinProperty(computedAssignment);
    }

    @Test
    public void testReassignmentWhenMultipleSubscriptionsRemovedAfterInitialAssignmentWithThreeConsumersTwoTopics() {

        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(topic2Name, 3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(topic3Name, 2));

        // Let initial subscriptions be A -> T1, T2 // B -> T2 // C -> T2, T3
        // Change the subscriptions to A -> T1 // B -> T1, T2, T3 // C -> T2
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Arrays.asList(0, 1, 2)));
        currentAssignmentForA.put(topic2Uuid, new HashSet<>(Collections.singletonList(0)));
        // Change subscriptions
        List<Uuid> subscribedTopicsA = new ArrayList<>(Collections.singletonList(topic1Uuid));
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, currentAssignmentForA));
        // Consumer B
        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic2Uuid, new HashSet<>(Collections.singletonList(1)));
        // Change subscriptions
        List<Uuid> subscribedTopicsB = new ArrayList<>(Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid));
        members.computeIfAbsent(consumerB, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, currentAssignmentForB));
        // Consumer C
        Map<Uuid, Set<Integer>> currentAssignmentForC = new HashMap<>();
        currentAssignmentForC.put(topic2Uuid, new HashSet<>(Collections.singletonList(2)));
        currentAssignmentForC.put(topic3Uuid, new HashSet<>(Arrays.asList(0, 1)));
        // Change subscriptions
        List<Uuid> subscribedTopicsC = new ArrayList<>(Collections.singletonList(topic2Uuid));
        members.put(consumerC, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsC, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(0, 1)));
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(2)));
        // Topic 2 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(0, 1)));
        expectedAssignment.computeIfAbsent(topic2Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(2)));
        // Topic 3 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic3Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(0, 1)));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    // We have a set of sets with the partitions that should be distributed amongst the consumers, if it exists then remove it from the set.
    private void assertAssignment(Map<Uuid, Set<Set<Integer>>> expectedAssignment, GroupAssignment computedGroupAssignment) {
        for (MemberAssignment member : computedGroupAssignment.getMembers().values()) {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = member.getAssignmentPerTopic();
            for (Map.Entry<Uuid, Set<Integer>> assignmentForTopic : computedAssignmentForMember.entrySet()) {
                Uuid topicId = assignmentForTopic.getKey();
                Set<Integer> assignmentPartitionsSet = assignmentForTopic.getValue();
                assertTrue(expectedAssignment.get(topicId).contains(assignmentPartitionsSet));
                expectedAssignment.remove(assignmentPartitionsSet);
            }
        }
    }

    private void assertCoPartitionJoinProperty(GroupAssignment groupAssignment) {
        for (MemberAssignment member : groupAssignment.getMembers().values()) {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = member.getAssignmentPerTopic();
            Set<Integer> compareSet = new HashSet<>();
            for (Set<Integer> partitionsForTopicSet : computedAssignmentForMember.values()) {
                if (compareSet.isEmpty()) {
                    compareSet = partitionsForTopicSet;
                }
                assertEquals(compareSet, partitionsForTopicSet);
            }
        }
    }
}
