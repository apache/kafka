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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangeAssignorTest {
    private final RangeAssignor assignor = new RangeAssignor();
    private final Uuid topic1Uuid = Uuid.randomUuid();
    private final Uuid topic2Uuid = Uuid.randomUuid();
    private final Uuid topic3Uuid = Uuid.randomUuid();
    private final String consumerA = "A";
    private final String consumerB = "B";
    private final String consumerC = "C";

    @Test
    public void testOneConsumerNoTopic() {
        Map<Uuid, AssignmentTopicMetadata> topics = Collections.singletonMap(topic1Uuid, new AssignmentTopicMetadata(3));
        Map<String, AssignmentMemberSpec> members = Collections.singletonMap(
            consumerA,
            new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Collections.emptyMap())
            );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec);

        assertTrue(groupAssignment.members().isEmpty());
    }

    @Test
    public void testOneConsumerNonExistentTopic() {
        Map<Uuid, AssignmentTopicMetadata> topics = Collections.singletonMap(topic1Uuid, new AssignmentTopicMetadata(3));
        Map<String, AssignmentMemberSpec> members = Collections.singletonMap(
            consumerA,
            new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                Collections.singletonList(topic2Uuid),
                Collections.emptyMap())
            );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec);

        assertTrue(groupAssignment.members().isEmpty());
    }

    @Test
    public void testFirstAssignmentTwoConsumersTwoTopicsSameSubscriptions() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Initial Subscriptions are: A -> T1, T3 | B -> T1, T3

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap())
        );

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap())
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        mkAssignment(expectedAssignment, topic1Uuid, Arrays.asList(0, 1));
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(2));
        // Topic 3 Partitions Assignment
        mkAssignment(expectedAssignment, topic3Uuid, Collections.singleton(0));
        mkAssignment(expectedAssignment, topic3Uuid, Collections.singleton(1));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentThreeConsumersThreeTopicsDifferentSubscriptions() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Initial Subscriptions: A -> T1, T2 | B -> T3 | C -> T2, T3

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap())
        );

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic3Uuid),
            Collections.emptyMap())
        );

        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic2Uuid, topic3Uuid),
            Collections.emptyMap())
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        mkAssignment(expectedAssignment, topic1Uuid, Arrays.asList(0, 1, 2));
        // Topic 2 Partitions Assignment
        mkAssignment(expectedAssignment, topic2Uuid, Arrays.asList(0, 1));
        mkAssignment(expectedAssignment, topic2Uuid, Collections.singleton(2));
        // Topic 3 Partitions Assignment
        mkAssignment(expectedAssignment, topic3Uuid, Collections.singleton(0));
        mkAssignment(expectedAssignment, topic3Uuid, Collections.singleton(1));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentNumConsumersGreaterThanNumPartitions() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Initial Subscriptions: A -> T1, T3 | B -> T1, T3 | C -> T1, T3

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap())
        );

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap())
        );

        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap())
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 3 has 2 partitions but three consumers subscribed to it - one of them will not get a partition.
        // Topic 1 Partitions Assignment
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(0));
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(1));
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(2));
        // Topic 2 Partitions Assignment
        mkAssignment(expectedAssignment, topic3Uuid, Collections.singleton(0));
        mkAssignment(expectedAssignment, topic3Uuid, Collections.singleton(1));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentNumConsumersGreaterThanNumPartitionsWhenOneConsumerAdded() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(2));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Initial Subscriptions: A -> T1, T2 | B -> T1, T2 | C -> T1, T2

        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, Collections.singleton(0));
        currentAssignmentForA.put(topic2Uuid, Collections.singleton(0));
        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA)
        );

        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, Collections.singleton(1));
        currentAssignmentForB.put(topic2Uuid, Collections.singleton(1));
        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB)
        );

        // Add a new consumer to trigger a re-assignment
        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap())
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(0));
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(1));
        // Topic 2 Partitions Assignment
        mkAssignment(expectedAssignment, topic2Uuid, Collections.singleton(0));
        mkAssignment(expectedAssignment, topic2Uuid, Collections.singleton(1));

        // Test for stickiness
        assertEquals(computedAssignment.members().get(consumerA)
            .targetPartitions(), new HashMap<>(currentAssignmentForA),
            "Stickiness test failed for consumer A");
        assertEquals(computedAssignment.members().get(consumerB)
            .targetPartitions(), new HashMap<>(currentAssignmentForB),
            "Stickiness test failed for consumer B");

        // Consumer C shouldn't get any assignment, due to stickiness A, B retain their assignments
        assertNull(computedAssignment.members().get(consumerC));
        assertAssignment(expectedAssignment, computedAssignment);
        assertCoPartitionJoinProperty(computedAssignment);
    }

    @Test
    public void testReassignmentWhenOnePartitionAddedForTwoConsumersTwoTopics() {
        // Simulating adding a partition - originally T1 -> 3 Partitions and T2 -> 3 Partitions
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(4));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(4));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Initial Subscriptions: A -> T1, T2 | B -> T1, T2

        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Arrays.asList(0, 1)));
        currentAssignmentForA.put(topic2Uuid, new HashSet<>(Arrays.asList(0, 1)));
        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA)
        );

        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, Collections.singleton(2));
        currentAssignmentForB.put(topic2Uuid, Collections.singleton(2));
        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB)
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        mkAssignment(expectedAssignment, topic1Uuid, Arrays.asList(0, 1));
        mkAssignment(expectedAssignment, topic1Uuid, Arrays.asList(2, 3));
        // Topic 2 Partitions Assignment
        mkAssignment(expectedAssignment, topic2Uuid, Arrays.asList(0, 1));
        mkAssignment(expectedAssignment, topic2Uuid, Arrays.asList(2, 3));

        // Test for stickiness
        assertEquals(computedAssignment.members().get(consumerA)
            .targetPartitions(), new HashMap<>(currentAssignmentForA),
            "Stickiness test failed for consumer A");
        // Implicitly stickiness is checked for B also since the other set of assigned partitions is (2,3)

        assertAssignment(expectedAssignment, computedAssignment);
        assertCoPartitionJoinProperty(computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerAddedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Initial Subscriptions: A -> T1, T2 | B -> T1, T2 | C -> T1, T2

        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Arrays.asList(0, 1)));
        currentAssignmentForA.put(topic2Uuid, new HashSet<>(Arrays.asList(0, 1)));
        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA)
        );

        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, Collections.singleton(2));
        currentAssignmentForB.put(topic2Uuid, Collections.singleton(2));
        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB)
        );

        // Add a new consumer to trigger a re-assignment
        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(0));
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(2));
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(1));
        // Topic 2 Partitions Assignment
        mkAssignment(expectedAssignment, topic2Uuid, Collections.singleton(0));
        mkAssignment(expectedAssignment, topic2Uuid, Collections.singleton(2));
        mkAssignment(expectedAssignment, topic2Uuid, Collections.singleton(1));

        // Test for stickiness
        assertEquals(computedAssignment.members().get(consumerB)
            .targetPartitions(), new HashMap<>(currentAssignmentForB),
            "Stickiness test failed for Consumer B");
        assertTrue(computedAssignment.members().get(consumerA)
            .targetPartitions().get(topic1Uuid).contains(0),
            "Stickiness test failed for Consumer A");

        assertAssignment(expectedAssignment, computedAssignment);
        assertCoPartitionJoinProperty(computedAssignment);
    }
    @Test
    public void testReassignmentWhenOneConsumerAddedAndOnePartitionAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        // Add a new partition to topic 1, initially T1 -> 3 partitions
        topics.put(topic1Uuid, new AssignmentTopicMetadata(4));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Initial Subscriptions: A -> T1, T2 | B -> T1, T2 | C -> T1, T2

        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Arrays.asList(0, 1)));
        currentAssignmentForA.put(topic2Uuid, new HashSet<>(Arrays.asList(0, 1)));
        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA)
        );

        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, Collections.singleton(2));
        currentAssignmentForB.put(topic2Uuid, Collections.singleton(2));
        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB)
        );

        // Add a new consumer to trigger a re-assignment
        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic1Uuid),
            Collections.emptyMap()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        mkAssignment(expectedAssignment, topic1Uuid, Arrays.asList(0, 1));
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(2));
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(3));
        // Topic 2 Partitions Assignment
        // Since the new consumer isn't subscribed to topic 2 the assignment shouldn't change
        mkAssignment(expectedAssignment, topic2Uuid, Arrays.asList(0, 1));
        mkAssignment(expectedAssignment, topic2Uuid, Collections.singleton(2));
        System.out.println("assignment is " + computedAssignment);
        // Test for stickiness
        assertEquals(computedAssignment.members().get(consumerA)
                        .targetPartitions(), new HashMap<>(currentAssignmentForA),
                "Stickiness test failed for Consumer A");
        assertEquals(computedAssignment.members().get(consumerB)
                        .targetPartitions(), new HashMap<>(currentAssignmentForB),
                "Stickiness test failed for Consumer B");

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerRemovedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A was removed

        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic1Uuid, new HashSet<>(Collections.singletonList(2)));
        currentAssignmentForB.put(topic2Uuid, new HashSet<>(Collections.singletonList(2)));
        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB)
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        mkAssignment(expectedAssignment, topic1Uuid, Arrays.asList(0, 1, 2));
        // Topic 2 Partitions Assignment
        mkAssignment(expectedAssignment, topic2Uuid, Arrays.asList(0, 1, 2));

        assertAssignment(expectedAssignment, computedAssignment);
        assertCoPartitionJoinProperty(computedAssignment);
    }

    @Test
    public void testReassignmentWhenMultipleSubscriptionsRemovedAfterInitialAssignmentWithThreeConsumersTwoTopics() {

        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Let initial subscriptions be A -> T1, T2 // B -> T2 // C -> T2, T3
        // Change the subscriptions to A -> T1 // B -> T1, T2, T3 // C -> T2

        Map<Uuid, Set<Integer>> currentAssignmentForA = new HashMap<>();
        currentAssignmentForA.put(topic1Uuid, new HashSet<>(Arrays.asList(0, 1, 2)));
        currentAssignmentForA.put(topic2Uuid, Collections.singleton(0));
        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic1Uuid),
            currentAssignmentForA)
        );

        Map<Uuid, Set<Integer>> currentAssignmentForB = new HashMap<>();
        currentAssignmentForB.put(topic2Uuid, Collections.singleton(1));
        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            currentAssignmentForB)
        );

        Map<Uuid, Set<Integer>> currentAssignmentForC = new HashMap<>();
        currentAssignmentForC.put(topic2Uuid, Collections.singleton(2));
        currentAssignmentForC.put(topic3Uuid, new HashSet<>(Arrays.asList(0, 1)));
        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic2Uuid),
            currentAssignmentForC)
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        mkAssignment(expectedAssignment, topic1Uuid, Arrays.asList(0, 1));
        mkAssignment(expectedAssignment, topic1Uuid, Collections.singleton(2));
        // Topic 2 Partitions Assignment
        mkAssignment(expectedAssignment, topic2Uuid, Arrays.asList(0, 1));
        mkAssignment(expectedAssignment, topic2Uuid, Collections.singleton(2));
        // Topic 3 Partitions Assignment
        mkAssignment(expectedAssignment, topic3Uuid, Arrays.asList(0, 1));

        // Test for stickiness
        assertTrue(computedAssignment.members().get(consumerC)
            .targetPartitions().get(topic2Uuid).contains(2),
            "Stickiness test failed for Consumer C");
        assertTrue(computedAssignment.members().get(consumerA)
            .targetPartitions().get(topic1Uuid).containsAll(Arrays.asList(0, 1)),
            "Stickiness test failed for Consumer A");

        assertAssignment(expectedAssignment, computedAssignment);
    }

    private void mkAssignment(Map<Uuid, Set<Set<Integer>>> expectedAssignment, Uuid topicId, Collection<Integer> partitions) {
        expectedAssignment.computeIfAbsent(topicId, k -> new HashSet<>()).add(new HashSet<>(partitions));
    }

    // We have a set of sets with the partitions that should be distributed amongst the consumers, if it exists then remove it from the set.
    // The test is done like this since the order in which members are assigned partitions isn't guaranteed. We are just testing if the computed
    // assignment contains the expected set of partitions, irrespective of which member got them.
    private void assertAssignment(Map<Uuid, Set<Set<Integer>>> expectedAssignment, GroupAssignment computedGroupAssignment) {
        for (MemberAssignment member : computedGroupAssignment.members().values()) {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = member.targetPartitions();
            for (Map.Entry<Uuid, Set<Integer>> assignmentForTopic : computedAssignmentForMember.entrySet()) {
                Uuid topicId = assignmentForTopic.getKey();
                Set<Integer> assignmentPartitionsSet = assignmentForTopic.getValue();
                assertTrue(expectedAssignment.get(topicId).contains(assignmentPartitionsSet));
                expectedAssignment.remove(assignmentPartitionsSet);
            }
        }
    }

    private void assertCoPartitionJoinProperty(GroupAssignment groupAssignment) {
        for (MemberAssignment member : groupAssignment.members().values()) {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = member.targetPartitions();
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
