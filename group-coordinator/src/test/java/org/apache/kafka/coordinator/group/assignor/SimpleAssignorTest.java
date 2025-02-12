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
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.apache.kafka.coordinator.group.assignor.SimpleAssignor.TargetPartition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleAssignorTest {

    private static final Uuid TOPIC_1_UUID = Uuid.randomUuid();
    private static final Uuid TOPIC_2_UUID = Uuid.randomUuid();
    private static final Uuid TOPIC_3_UUID = Uuid.randomUuid();
    private static final Uuid TOPIC_4_UUID = Uuid.randomUuid();
    private static final String TOPIC_1_NAME = "topic1";
    private static final String TOPIC_3_NAME = "topic3";
    private static final String MEMBER_A = "A";
    private static final String MEMBER_B = "B";

    private final SimpleAssignor assignor = new SimpleAssignor();

    @Test
    public void testName() {
        assertEquals("simple", assignor.name());
    }

    @Test
    public void testAssignWithEmptyMembers() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Collections.emptyMap()
        );

        GroupSpec groupSpec = new GroupSpecImpl(
            Collections.emptyMap(),
            HOMOGENEOUS,
            Collections.emptyMap()
        );

        GroupAssignment groupAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertEquals(Collections.emptyMap(), groupAssignment.members());

        groupSpec = new GroupSpecImpl(
            Collections.emptyMap(),
            HETEROGENEOUS,
            Collections.emptyMap()
        );
        groupAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );
        assertEquals(Collections.emptyMap(), groupAssignment.members());
    }

    @Test
    public void testAssignWithNoSubscribedTopic() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Collections.singletonMap(
                TOPIC_1_UUID,
                new TopicMetadata(
                    TOPIC_1_UUID,
                    TOPIC_1_NAME,
                    3
                )
            )
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = Collections.singletonMap(
            MEMBER_A,
            new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                Collections.emptySet(),
                Assignment.EMPTY
            )
        );

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            Collections.emptyMap()
        );

        GroupAssignment groupAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertEquals(Collections.emptyMap(), groupAssignment.members());
    }

    @Test
    public void testAssignWithSubscribedToNonExistentTopic() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Collections.singletonMap(
                TOPIC_1_UUID,
                new TopicMetadata(
                    TOPIC_1_UUID,
                    TOPIC_1_NAME,
                    3
                )
            )
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = Collections.singletonMap(
            MEMBER_A,
            new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                Set.of(TOPIC_2_UUID),
                Assignment.EMPTY
            )
        );

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            Collections.emptyMap()
        );

        assertThrows(PartitionAssignorException.class,
            () -> assignor.assign(groupSpec, subscribedTopicMetadata));
    }

    @Test
    public void testAssignWithTwoMembersAndTwoTopicsHomogeneous() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(TOPIC_1_UUID, new TopicMetadata(
            TOPIC_1_UUID,
            TOPIC_1_NAME,
            3
        ));
        topicMetadata.put(TOPIC_3_UUID, new TopicMetadata(
            TOPIC_3_UUID,
            TOPIC_3_NAME,
            2
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        Set<Uuid> topicsSubscription = new LinkedHashSet<>();
        topicsSubscription.add(TOPIC_1_UUID);
        topicsSubscription.add(TOPIC_3_UUID);

        members.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            topicsSubscription,
            Assignment.EMPTY
        ));

        members.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            topicsSubscription,
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            Collections.emptyMap()
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        // Hashcode of MEMBER_A is 65. Hashcode of MEMBER_B is 66.
        // T1:0 -> MEMBER_A and T1:1 -> MEMBER_B by hash assignment.
        // T1:2, T3:1 -> MEMBER_A and T3:0 -> MEMBER_B by round-robin assignment.
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(MEMBER_A, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 0, 2),
            mkTopicAssignment(TOPIC_3_UUID, 1)
        ));
        expectedAssignment.put(MEMBER_B, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 1),
            mkTopicAssignment(TOPIC_3_UUID, 0)
        ));

        // T1: 3 partitions + T3: 2 partitions = 5 partitions
        assertEveryPartitionGetsAssignment(5, computedAssignment);
        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testAssignWithThreeMembersThreeTopicsHeterogeneous() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(TOPIC_1_UUID, new TopicMetadata(
            TOPIC_1_UUID,
            TOPIC_1_NAME,
            3
        ));

        topicMetadata.put(TOPIC_2_UUID, new TopicMetadata(
            TOPIC_2_UUID,
            "topic2",
            3
        ));
        topicMetadata.put(TOPIC_3_UUID, new TopicMetadata(
            TOPIC_3_UUID,
            TOPIC_3_NAME,
            2
        ));

        Set<Uuid> memberATopicsSubscription = new LinkedHashSet<>();
        memberATopicsSubscription.add(TOPIC_1_UUID);
        memberATopicsSubscription.add(TOPIC_2_UUID);

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            memberATopicsSubscription,
            Assignment.EMPTY
        ));

        members.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_3_UUID),
            Assignment.EMPTY
        ));

        String memberC = "C";
        Set<Uuid> memberCTopicsSubscription = new LinkedHashSet<>();
        memberCTopicsSubscription.add(TOPIC_2_UUID);
        memberCTopicsSubscription.add(TOPIC_3_UUID);
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            memberCTopicsSubscription,
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            Collections.emptyMap()
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        // Hashcode of MEMBER_A is 65. Hashcode of MEMBER_B is 66. Hashcode of MEMBER_C is 67.
        // T2:2 -> member_A, T3:0 -> member_B, T2:2 -> member_C by hash assignment.
        // T1:0, T1:1, T1:2, T2:0 -> member_A, T3:1, -> member_B, T2:1 -> member_C by round-robin assignment.
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(MEMBER_A, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 0, 1, 2),
            mkTopicAssignment(TOPIC_2_UUID, 0, 2)
        ));
        expectedAssignment.put(MEMBER_B, mkAssignment(
            mkTopicAssignment(TOPIC_3_UUID, 0, 1)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(TOPIC_2_UUID, 1, 2)
        ));

        // T1: 3 partitions + T2: 3 partitions + T3: 2 partitions = 8 partitions
        assertEveryPartitionGetsAssignment(8, computedAssignment);
        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testAssignWithOneMemberNoAssignedTopicHeterogeneous() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(TOPIC_1_UUID, new TopicMetadata(
            TOPIC_1_UUID,
            TOPIC_1_NAME,
            3
        ));

        topicMetadata.put(TOPIC_2_UUID, new TopicMetadata(
            TOPIC_2_UUID,
            "topic2",
            2
        ));

        Set<Uuid> memberATopicsSubscription = new LinkedHashSet<>();
        memberATopicsSubscription.add(TOPIC_1_UUID);
        memberATopicsSubscription.add(TOPIC_2_UUID);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            memberATopicsSubscription,
            Assignment.EMPTY
        ));

        members.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.emptySet(),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            Collections.emptyMap()
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(MEMBER_A, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 0, 1, 2),
            mkTopicAssignment(TOPIC_2_UUID, 0, 1)));
        expectedAssignment.put(MEMBER_B, mkAssignment());

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testMemberHashAssignment() {
        // hashcode for "member1" is 948881623.
        String member1 = "member1";
        // hashcode for "member2" is 948881624.
        String member2 = "member2";
        // hashcode for "member3" is 948881625.
        String member3 = "member3";
        // hashcode for "member4" is 948881626.
        String member4 = "member4";
        // hashcode for "AaAaAaAa" is -540425984 to test with negative hashcode.
        String member5 = "AaAaAaAa";
        List<String> members = Arrays.asList(member1, member2, member3, member4, member5);

        TargetPartition partition1 = new TargetPartition(TOPIC_1_UUID, 0);
        TargetPartition partition2 = new TargetPartition(TOPIC_2_UUID, 0);
        TargetPartition partition3 = new TargetPartition(TOPIC_3_UUID, 0);
        List<TargetPartition> partitions = Arrays.asList(partition1, partition2, partition3);

        Map<TargetPartition, List<String>> computedAssignment = new HashMap<>();
        assignor.memberHashAssignment(partitions, members, computedAssignment);

        Map<TargetPartition, List<String>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(partition1, Collections.singletonList(member3));
        expectedAssignment.put(partition2, Arrays.asList(member1, member4));
        expectedAssignment.put(partition3, Arrays.asList(member2, member5));
        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testRoundRobinAssignment() {
        String member1 = "member1";
        String member2 = "member2";
        List<String> members = Arrays.asList(member1, member2);
        TargetPartition partition1 = new TargetPartition(TOPIC_1_UUID, 0);
        TargetPartition partition2 = new TargetPartition(TOPIC_2_UUID, 0);
        TargetPartition partition3 = new TargetPartition(TOPIC_3_UUID, 0);
        TargetPartition partition4 = new TargetPartition(TOPIC_4_UUID, 0);
        List<TargetPartition> unassignedPartitions = Arrays.asList(partition2, partition3, partition4);

        Map<TargetPartition, List<String>> assignment = new HashMap<>();
        assignment.put(partition1, Collections.singletonList(member1));

        assignor.roundRobinAssignment(members, unassignedPartitions, assignment);
        Map<TargetPartition, List<String>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(partition1, Collections.singletonList(member1));
        expectedAssignment.put(partition2, Collections.singletonList(member1));
        expectedAssignment.put(partition3, Collections.singletonList(member2));
        expectedAssignment.put(partition4, Collections.singletonList(member1));

        assertAssignment(expectedAssignment, assignment);
    }

    private void assertAssignment(
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment,
        GroupAssignment computedGroupAssignment
    ) {
        assertEquals(expectedAssignment.size(), computedGroupAssignment.members().size());
        for (String memberId : computedGroupAssignment.members().keySet()) {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = computedGroupAssignment.members().get(memberId).partitions();
            assertEquals(expectedAssignment.get(memberId), computedAssignmentForMember);
        }
    }

    private void assertAssignment(
        Map<TargetPartition, List<String>> expectedAssignment,
        Map<TargetPartition, List<String>> computedAssignment
    ) {
        assertEquals(expectedAssignment.size(), computedAssignment.size());
        expectedAssignment.forEach((targetPartition, members) -> {
            List<String> computedMembers = computedAssignment.getOrDefault(targetPartition, Collections.emptyList());
            assertEquals(members.size(), computedMembers.size());
            members.forEach(member -> assertTrue(computedMembers.contains(member)));
        });
    }

    private void assertEveryPartitionGetsAssignment(
        int expectedPartitions,
        GroupAssignment computedGroupAssignment
    ) {
        Map<String, MemberAssignment> memberAssignments = computedGroupAssignment.members();
        Set<TargetPartition> topicPartitionAssignments = new HashSet<>();
        memberAssignments.values().forEach(memberAssignment -> {
            Map<Uuid, Set<Integer>> targetPartitions = memberAssignment.partitions();
            targetPartitions.forEach((topicId, partitions) ->
                partitions.forEach(partition -> topicPartitionAssignments.add(new TargetPartition(topicId, partition)))
            );
        });
        assertEquals(expectedPartitions, topicPartitionAssignments.size());
    }
}
