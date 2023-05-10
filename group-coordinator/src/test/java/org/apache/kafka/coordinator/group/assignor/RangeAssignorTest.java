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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
                Collections.emptyMap()
            )
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec);

        assertEquals(Collections.emptyMap(), groupAssignment.members());
    }

    @Test
    public void testOneConsumerSubscribedToNonExistentTopic() {
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
    public void testFirstAssignmentTwoConsumersTwoTopicsSameSubscriptions() {
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
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic3Uuid, 0)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic3Uuid, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentThreeConsumersThreeTopicsDifferentSubscriptions() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()
        ));

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic3Uuid),
            Collections.emptyMap()
        ));

        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2),
            mkTopicAssignment(topic2Uuid, 0, 1)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic3Uuid, 0)
        ));

        expectedAssignment.put(consumerC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 2),
            mkTopicAssignment(topic3Uuid, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentNumConsumersGreaterThanNumPartitions() {
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

        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 3 has 2 partitions but three consumers subscribed to it - one of them will not get a partition.
        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic3Uuid, 0)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic3Uuid, 1)
        ));

        expectedAssignment.put(consumerC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentNumConsumersGreaterThanNumPartitionsWhenOneConsumerAdded() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(2));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0)
        );

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1)
        );

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        // Add a new consumer to trigger a re-assignment
        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1)
        ));

        // Consumer C shouldn't get any assignment, due to stickiness A, B retain their assignments
        assertNull(computedAssignment.members().get(consumerC));
        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOnePartitionAddedForTwoConsumersTwoTopics() {
        // Simulating adding a partition - originally T1 -> 3 Partitions and T2 -> 3 Partitions
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(4));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(4));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic2Uuid, 0, 1)
        );

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 2)
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
            mkTopicAssignment(topic2Uuid, 0, 1)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2, 3),
            mkTopicAssignment(topic2Uuid, 2, 3)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerAddedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic2Uuid, 0, 1)
        );

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 2)
        );

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        // Add a new consumer to trigger a re-assignment
        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 2)
        ));

        expectedAssignment.put(consumerC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerAddedAndOnePartitionAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        // Add a new partition to topic 1, initially T1 -> 3 partitions
        topics.put(topic1Uuid, new AssignmentTopicMetadata(4));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic2Uuid, 0, 1)
        );

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 2)
        );

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        // Add a new consumer to trigger a re-assignment
        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic1Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic2Uuid, 0, 1)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 2)
        ));

        expectedAssignment.put(consumerC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 3)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerRemovedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        // Consumer A was removed

        Map<Uuid, Set<Integer>> currentAssignmentForB = mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 2)
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

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2),
            mkTopicAssignment(topic2Uuid, 0, 1, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenMultipleSubscriptionsRemovedAfterInitialAssignmentWithThreeConsumersTwoTopics() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic2Uuid, new AssignmentTopicMetadata(3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(2));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        // Let initial subscriptions be A -> T1, T2 // B -> T2 // C -> T2, T3
        // Change the subscriptions to A -> T1 // B -> T1, T2, T3 // C -> T2

        Map<Uuid, Set<Integer>> currentAssignmentForA = mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2),
            mkTopicAssignment(topic2Uuid, 0)
        );

        members.put(consumerA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic1Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = mkAssignment(
            mkTopicAssignment(topic2Uuid, 1)
        );

        members.put(consumerB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            currentAssignmentForB
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForC = mkAssignment(
            mkTopicAssignment(topic2Uuid, 2),
            mkTopicAssignment(topic3Uuid, 0, 1)
        );

        members.put(consumerC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic2Uuid),
            currentAssignmentForC
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();

        expectedAssignment.put(consumerA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1)
        ));

        expectedAssignment.put(consumerB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 0, 1),
            mkTopicAssignment(topic3Uuid, 0, 1)
        ));

        expectedAssignment.put(consumerC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    private void assertAssignment(Map<String, Map<Uuid, Set<Integer>>> expectedAssignment, GroupAssignment computedGroupAssignment) {
        assertEquals(expectedAssignment.size(), computedGroupAssignment.members().size());
        for (String memberId : computedGroupAssignment.members().keySet()) {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = computedGroupAssignment.members().get(memberId).targetPartitions();
            assertEquals(expectedAssignment.get(memberId), computedAssignmentForMember);
        }
    }
}
