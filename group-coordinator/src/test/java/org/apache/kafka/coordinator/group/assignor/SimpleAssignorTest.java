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
import org.apache.kafka.server.common.TopicIdPartition;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleAssignorTest {

    private static final Uuid TOPIC_1_UUID = Uuid.randomUuid();
    private static final Uuid TOPIC_2_UUID = Uuid.randomUuid();
    private static final Uuid TOPIC_3_UUID = Uuid.randomUuid();
    private static final Uuid TOPIC_4_UUID = Uuid.randomUuid();
    private static final String TOPIC_1_NAME = "topic1";
    private static final String TOPIC_2_NAME = "topic2";
    private static final String TOPIC_3_NAME = "topic3";
    private static final String TOPIC_4_NAME = "topic4";
    private static final String MEMBER_A = "A";
    private static final String MEMBER_B = "B";
    private static final String MEMBER_C = "C";

    private final SimpleAssignor assignor = new SimpleAssignor();

    @Test
    public void testName() {
        assertEquals("simple", assignor.name());
    }

    @Test
    public void testAssignWithEmptyMembers() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Map.of()
        );

        GroupSpec groupSpec = new GroupSpecImpl(
            Map.of(),
            HOMOGENEOUS,
            Map.of()
        );

        GroupAssignment groupAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertEquals(Map.of(), groupAssignment.members());

        groupSpec = new GroupSpecImpl(
            Map.of(),
            HETEROGENEOUS,
            Map.of()
        );
        groupAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );
        assertEquals(Map.of(), groupAssignment.members());
    }

    @Test
    public void testAssignWithNoSubscribedTopic() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Map.of(
                TOPIC_1_UUID,
                new TopicMetadata(
                    TOPIC_1_UUID,
                    TOPIC_1_NAME,
                    3
                )
            )
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = Map.of(
            MEMBER_A,
            new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                Set.of(),
                Assignment.EMPTY
            )
        );

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            Map.of()
        );

        GroupAssignment groupAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertEquals(Map.of(), groupAssignment.members());
    }

    @Test
    public void testAssignWithSubscribedToNonExistentTopic() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Map.of(
                TOPIC_1_UUID,
                new TopicMetadata(
                    TOPIC_1_UUID,
                    TOPIC_1_NAME,
                    3
                )
            )
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = Map.of(
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
            Map.of()
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

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();

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
            Map.of()
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        // Hashcode of MEMBER_A is 65. Hashcode of MEMBER_B is 66.
        // Step 1 -> T1:0 -> MEMBER_A and T1:1 -> MEMBER_B by hash assignment.
        // Step 2 -> T1:2, T3:1 -> MEMBER_A and T3:0 -> MEMBER_B by round-robin assignment.
        // Step 3 -> no new assignment gets added by current assignment since it is empty.
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

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();
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

        Set<Uuid> memberCTopicsSubscription = new LinkedHashSet<>();
        memberCTopicsSubscription.add(TOPIC_2_UUID);
        memberCTopicsSubscription.add(TOPIC_3_UUID);
        members.put(MEMBER_C, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            memberCTopicsSubscription,
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            Map.of()
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        // Hashcode of MEMBER_A is 65. Hashcode of MEMBER_B is 66. Hashcode of MEMBER_C is 67.
        // Step 1 -> T2:2 -> member_A, T3:0 -> member_B, T2:2 -> member_C by hash assignment.
        // Step 2 -> T1:0, T1:1, T1:2, T2:0 -> member_A, T3:1, -> member_B, T2:1 -> member_C by round-robin assignment.
        // Step 3 -> no new assignment gets added by current assignment since it is empty.
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(MEMBER_A, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 0, 1, 2),
            mkTopicAssignment(TOPIC_2_UUID, 0, 2)
        ));
        expectedAssignment.put(MEMBER_B, mkAssignment(
            mkTopicAssignment(TOPIC_3_UUID, 0, 1)
        ));
        expectedAssignment.put(MEMBER_C, mkAssignment(
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
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();
        members.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            memberATopicsSubscription,
            Assignment.EMPTY
        ));

        members.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            Map.of()
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

        // T1: 3 partitions + T2: 2 partitions = 5 partitions
        assertEveryPartitionGetsAssignment(5, computedAssignment);
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
        List<String> members = List.of(member1, member2, member3, member4, member5);

        TopicIdPartition partition1 = new TopicIdPartition(TOPIC_1_UUID, 0);
        TopicIdPartition partition2 = new TopicIdPartition(TOPIC_2_UUID, 0);
        TopicIdPartition partition3 = new TopicIdPartition(TOPIC_3_UUID, 0);
        List<TopicIdPartition> partitions = List.of(partition1, partition2, partition3);

        Map<TopicIdPartition, List<String>> computedAssignment = new HashMap<>();
        assignor.memberHashAssignment(members, partitions, computedAssignment);

        Map<TopicIdPartition, List<String>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(partition1, List.of(member3));
        expectedAssignment.put(partition2, List.of(member1, member4));
        expectedAssignment.put(partition3, List.of(member2, member5));
        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testRoundRobinAssignment() {
        String member1 = "member1";
        String member2 = "member2";
        List<String> members = List.of(member1, member2);
        TopicIdPartition partition1 = new TopicIdPartition(TOPIC_1_UUID, 0);
        TopicIdPartition partition2 = new TopicIdPartition(TOPIC_2_UUID, 0);
        TopicIdPartition partition3 = new TopicIdPartition(TOPIC_3_UUID, 0);
        TopicIdPartition partition4 = new TopicIdPartition(TOPIC_4_UUID, 0);
        List<TopicIdPartition> unassignedPartitions = List.of(partition2, partition3, partition4);

        Map<TopicIdPartition, List<String>> assignment = new HashMap<>();
        assignment.put(partition1, List.of(member1));

        assignor.roundRobinAssignment(members, unassignedPartitions, assignment);
        Map<TopicIdPartition, List<String>> expectedAssignment = Map.of(
            partition1, List.of(member1),
            partition2, List.of(member1),
            partition3, List.of(member2),
            partition4, List.of(member1)
        );

        assertAssignment(expectedAssignment, assignment);
    }

    @Test
    public void testRoundRobinAssignmentWithCount() {
        String member1 = "member1";
        String member2 = "member2";
        List<String> members = List.of(member1, member2);
        TopicIdPartition partition1 = new TopicIdPartition(TOPIC_1_UUID, 0);
        TopicIdPartition partition2 = new TopicIdPartition(TOPIC_2_UUID, 0);
        TopicIdPartition partition3 = new TopicIdPartition(TOPIC_3_UUID, 0);
        TopicIdPartition partition4 = new TopicIdPartition(TOPIC_4_UUID, 0);
        List<TopicIdPartition> unassignedPartitions = List.of(partition2, partition3, partition4);

        Map<String, Set<TopicIdPartition>> assignment = new HashMap<>();
        assignment.put(member1, new HashSet<>(Set.of(partition1)));
        assignment.put(member2, new HashSet<>(Set.of(partition1)));

        assignor.roundRobinAssignmentWithCount(members, unassignedPartitions, assignment, 2);
        Map<String, Set<TopicIdPartition>> expectedAssignment = Map.of(
            member1, Set.of(partition1, partition2, partition4),
            member2, Set.of(partition1, partition3)
        );

        assertFinalAssignment(expectedAssignment, assignment);
    }

    @Test
    public void testRoundRobinAssignmentWithCountTooManyPartitions() {
        String member1 = "member1";
        String member2 = "member2";
        List<String> members = List.of(member1, member2);
        TopicIdPartition partition1 = new TopicIdPartition(TOPIC_1_UUID, 0);
        TopicIdPartition partition2 = new TopicIdPartition(TOPIC_2_UUID, 0);
        TopicIdPartition partition3 = new TopicIdPartition(TOPIC_3_UUID, 0);
        TopicIdPartition partition4 = new TopicIdPartition(TOPIC_4_UUID, 0);
        TopicIdPartition partition5 = new TopicIdPartition(TOPIC_4_UUID, 1);
        TopicIdPartition partition6 = new TopicIdPartition(TOPIC_4_UUID, 2);
        List<TopicIdPartition> unassignedPartitions = List.of(partition2, partition3, partition4, partition5, partition6);

        Map<String, Set<TopicIdPartition>> assignment = new HashMap<>();
        assignment.put(member1, new HashSet<>(Set.of(partition1)));
        assignment.put(member2, new HashSet<>(Set.of(partition1)));

        assertThrows(PartitionAssignorException.class,
            () -> assignor.roundRobinAssignmentWithCount(members, unassignedPartitions, assignment, 2));
    }

    @Test
    public void testAssignWithCurrentAssignmentHomogeneous() {
        // Current assignment setup - Two members A, B subscribing to T1 and T2.
        Map<Uuid, TopicMetadata> topicMetadata1 = new HashMap<>();
        topicMetadata1.put(TOPIC_1_UUID, new TopicMetadata(
            TOPIC_1_UUID,
            TOPIC_1_NAME,
            3
        ));
        topicMetadata1.put(TOPIC_2_UUID, new TopicMetadata(
            TOPIC_2_UUID,
            TOPIC_2_NAME,
            2
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members1 = new HashMap<>();

        Set<Uuid> topicsSubscription1 = new LinkedHashSet<>();
        topicsSubscription1.add(TOPIC_1_UUID);
        topicsSubscription1.add(TOPIC_2_UUID);

        members1.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            topicsSubscription1,
            Assignment.EMPTY
        ));

        members1.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            topicsSubscription1,
            Assignment.EMPTY
        ));

        GroupSpec groupSpec1 = new GroupSpecImpl(
            members1,
            HOMOGENEOUS,
            Map.of()
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata1 = new SubscribedTopicDescriberImpl(topicMetadata1);

        GroupAssignment computedAssignment1 = assignor.assign(
            groupSpec1,
            subscribedTopicMetadata1
        );

        // Hashcode of MEMBER_A is 65. Hashcode of MEMBER_B is 66.
        // Step 1 -> T1:0 -> MEMBER_A and T1:1 -> MEMBER_B by hash assignment.
        // Step 2 -> T1:2, T2:1 -> MEMBER_A and T2:0 -> MEMBER_B by round-robin assignment.
        // Step 3 -> no new assignment gets added by current assignment since it is empty.
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment1 = new HashMap<>();
        expectedAssignment1.put(MEMBER_A, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 0, 2),
            mkTopicAssignment(TOPIC_2_UUID, 1)
        ));
        expectedAssignment1.put(MEMBER_B, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 1),
            mkTopicAssignment(TOPIC_2_UUID, 0)
        ));

        // T1: 3 partitions + T2: 2 partitions = 5 partitions
        assertEveryPartitionGetsAssignment(5, computedAssignment1);
        assertAssignment(expectedAssignment1, computedAssignment1);

        // New assignment setup - Three members A, B, C subscribing to T2 and T3.
        Map<Uuid, TopicMetadata> topicMetadata2 = new HashMap<>();
        topicMetadata2.put(TOPIC_2_UUID, new TopicMetadata(
            TOPIC_2_UUID,
            TOPIC_2_NAME,
            2
        ));
        topicMetadata2.put(TOPIC_3_UUID, new TopicMetadata(
            TOPIC_3_UUID,
            TOPIC_3_NAME,
            3
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members2 = new HashMap<>();

        Set<Uuid> topicsSubscription2 = new LinkedHashSet<>();
        topicsSubscription2.add(TOPIC_2_UUID);
        topicsSubscription2.add(TOPIC_3_UUID);

        members2.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            topicsSubscription2,
            // Utilizing the assignment from current assignment
            new Assignment(mkAssignment(
                mkTopicAssignment(TOPIC_1_UUID, 0, 2),
                mkTopicAssignment(TOPIC_2_UUID, 1)))
        ));

        members2.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            topicsSubscription2,
            new Assignment(mkAssignment(
                mkTopicAssignment(TOPIC_1_UUID, 1),
                mkTopicAssignment(TOPIC_2_UUID, 0)))
        ));

        members2.put(MEMBER_C, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            topicsSubscription2,
            Assignment.EMPTY
        ));

        GroupSpec groupSpec2 = new GroupSpecImpl(
            members2,
            HOMOGENEOUS,
            Map.of()
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata2 = new SubscribedTopicDescriberImpl(topicMetadata2);

        GroupAssignment computedAssignment2 = assignor.assign(
            groupSpec2,
            subscribedTopicMetadata2
        );

        // Hashcode of MEMBER_A is 65. Hashcode of MEMBER_B is 66. Hashcode of MEMBER_C is 67.
        // Step 1 -> T2:0 -> MEMBER_A, T2:1 -> MEMBER_B, T3:0 -> MEMBER_C by hash assignment
        // Step 2 -> T3:1 -> MEMBER_A, T3:2 -> MEMBER_B by round-robin assignment
        // Step 3 -> no new addition by current assignment since T2:0 and T2:1 were already a part of new assignment.
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment2 = new HashMap<>();
        expectedAssignment2.put(MEMBER_A, mkAssignment(
            mkTopicAssignment(TOPIC_2_UUID, 0),
            mkTopicAssignment(TOPIC_3_UUID, 1)
        ));
        expectedAssignment2.put(MEMBER_B, mkAssignment(
            mkTopicAssignment(TOPIC_2_UUID, 1),
            mkTopicAssignment(TOPIC_3_UUID, 2)
        ));
        expectedAssignment2.put(MEMBER_C, mkAssignment(
            mkTopicAssignment(TOPIC_3_UUID, 0)
        ));

        // T2: 2 partitions + T3: 3 partitions = 5 partitions
        assertEveryPartitionGetsAssignment(5, computedAssignment2);
        assertAssignment(expectedAssignment2, computedAssignment2);
    }

    @Test
    public void testAssignWithCurrentAssignmentHeterogeneous() {
        // Current assignment setup - 3 members A - {T1, T2}, B - {T3}, C - {T2, T3}.
        Map<Uuid, TopicMetadata> topicMetadata1 = new HashMap<>();
        topicMetadata1.put(TOPIC_1_UUID, new TopicMetadata(
            TOPIC_1_UUID,
            TOPIC_1_NAME,
            3
        ));

        topicMetadata1.put(TOPIC_2_UUID, new TopicMetadata(
            TOPIC_2_UUID,
            TOPIC_2_NAME,
            3
        ));
        topicMetadata1.put(TOPIC_3_UUID, new TopicMetadata(
            TOPIC_3_UUID,
            TOPIC_3_NAME,
            2
        ));

        Set<Uuid> memberATopicsSubscription1 = new LinkedHashSet<>();
        memberATopicsSubscription1.add(TOPIC_1_UUID);
        memberATopicsSubscription1.add(TOPIC_2_UUID);

        Map<String, MemberSubscriptionAndAssignmentImpl> members1 = new HashMap<>();
        members1.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            memberATopicsSubscription1,
            Assignment.EMPTY
        ));

        members1.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_3_UUID),
            Assignment.EMPTY
        ));

        Set<Uuid> memberCTopicsSubscription1 = new LinkedHashSet<>();
        memberCTopicsSubscription1.add(TOPIC_2_UUID);
        memberCTopicsSubscription1.add(TOPIC_3_UUID);
        members1.put(MEMBER_C, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            memberCTopicsSubscription1,
            Assignment.EMPTY
        ));

        GroupSpec groupSpec1 = new GroupSpecImpl(
            members1,
            HETEROGENEOUS,
            Map.of()
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata1 = new SubscribedTopicDescriberImpl(topicMetadata1);

        GroupAssignment computedAssignment1 = assignor.assign(
            groupSpec1,
            subscribedTopicMetadata1
        );

        // Hashcode of MEMBER_A is 65. Hashcode of MEMBER_B is 66. Hashcode of MEMBER_C is 67.
        // Step 1 -> T2:2 -> member_A, T3:0 -> member_B, T2:2 -> member_C by hash assignment.
        // Step 2 -> T1:0, T1:1, T1:2, T2:0 -> member_A, T3:1, -> member_B, T2:1 -> member_C by round-robin assignment.
        // Step 3 -> no new assignment gets added by current assignment since it is empty.
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment1 = new HashMap<>();
        expectedAssignment1.put(MEMBER_A, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 0, 1, 2),
            mkTopicAssignment(TOPIC_2_UUID, 0, 2)
        ));
        expectedAssignment1.put(MEMBER_B, mkAssignment(
            mkTopicAssignment(TOPIC_3_UUID, 0, 1)
        ));
        expectedAssignment1.put(MEMBER_C, mkAssignment(
            mkTopicAssignment(TOPIC_2_UUID, 1, 2)
        ));

        // T1: 3 partitions + T2: 3 partitions + T3: 2 partitions = 8 partitions
        assertEveryPartitionGetsAssignment(8, computedAssignment1);
        assertAssignment(expectedAssignment1, computedAssignment1);

        // New assignment setup - 2 members A - {T1, T2, T3}, B - {T3, T4}.

        Map<Uuid, TopicMetadata> topicMetadata2 = new HashMap<>();
        topicMetadata2.put(TOPIC_1_UUID, new TopicMetadata(
            TOPIC_1_UUID,
            TOPIC_1_NAME,
            3
        ));
        topicMetadata2.put(TOPIC_2_UUID, new TopicMetadata(
            TOPIC_2_UUID,
            TOPIC_2_NAME,
            3
        ));
        topicMetadata2.put(TOPIC_3_UUID, new TopicMetadata(
            TOPIC_3_UUID,
            TOPIC_3_NAME,
            2
        ));
        topicMetadata2.put(TOPIC_4_UUID, new TopicMetadata(
            TOPIC_4_UUID,
            TOPIC_4_NAME,
            1
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members2 = new HashMap<>();

        Set<Uuid> memberATopicsSubscription2 = new LinkedHashSet<>();
        memberATopicsSubscription2.add(TOPIC_1_UUID);
        memberATopicsSubscription2.add(TOPIC_2_UUID);
        memberATopicsSubscription2.add(TOPIC_3_UUID);

        Set<Uuid> memberBTopicsSubscription2 = new LinkedHashSet<>();
        memberBTopicsSubscription2.add(TOPIC_3_UUID);
        memberBTopicsSubscription2.add(TOPIC_4_UUID);

        members2.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            memberATopicsSubscription2,
            new Assignment(mkAssignment(
                mkTopicAssignment(TOPIC_1_UUID, 0, 1, 2),
                mkTopicAssignment(TOPIC_2_UUID, 0, 2)))
        ));

        members2.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            memberBTopicsSubscription2,
            new Assignment(mkAssignment(
                mkTopicAssignment(TOPIC_3_UUID, 0, 1)))
        ));

        GroupSpec groupSpec2 = new GroupSpecImpl(
            members2,
            HETEROGENEOUS,
            Map.of()
        );

        SubscribedTopicDescriberImpl subscribedTopicMetadata2 = new SubscribedTopicDescriberImpl(topicMetadata2);

        GroupAssignment computedAssignment2 = assignor.assign(
            groupSpec2,
            subscribedTopicMetadata2
        );

        // Hashcode of MEMBER_A is 65. Hashcode of MEMBER_B is 66.
        // Step 1 -> T1:1 -> member_A, T3:0 -> member_B by hash assignment.
        // Step 2 -> T2:1 -> member_A, T4:0 -> member_B by round-robin assignment.
        // Step 3 -> T1:0, T1:2, T2:0 -> member_A,  T3:1 -> member_B by current assignment.
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment2 = new HashMap<>();
        expectedAssignment2.put(MEMBER_A, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 0, 1, 2),
            mkTopicAssignment(TOPIC_2_UUID, 0, 1, 2)
        ));
        expectedAssignment2.put(MEMBER_B, mkAssignment(
            mkTopicAssignment(TOPIC_3_UUID, 0, 1),
            mkTopicAssignment(TOPIC_4_UUID, 0)
        ));

        // T1: 3 partitions + T2: 3 partitions + T3: 2 partitions + T4: 1 partition = 9 partitions
        assertEveryPartitionGetsAssignment(9, computedAssignment2);
        assertAssignment(expectedAssignment2, computedAssignment2);
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
        Map<TopicIdPartition, List<String>> expectedAssignment,
        Map<TopicIdPartition, List<String>> computedAssignment
    ) {
        assertEquals(expectedAssignment.size(), computedAssignment.size());
        expectedAssignment.forEach((topicIdPartition, members) -> {
            List<String> computedMembers = computedAssignment.getOrDefault(topicIdPartition, List.of());
            assertEquals(members.size(), computedMembers.size());
            members.forEach(member -> assertTrue(computedMembers.contains(member)));
        });
    }

    private void assertFinalAssignment(
        Map<String, Set<TopicIdPartition>> expectedAssignment,
        Map<String, Set<TopicIdPartition>> computedAssignment
    ) {
        assertEquals(expectedAssignment.size(), computedAssignment.size());
        expectedAssignment.forEach((memberId, partitions) -> {
            Set<TopicIdPartition> computedPartitions = computedAssignment.getOrDefault(memberId, Set.of());
            assertEquals(partitions.size(), computedPartitions.size());
            partitions.forEach(member -> assertTrue(computedPartitions.contains(member)));
        });
    }

    private void assertEveryPartitionGetsAssignment(
        int expectedPartitions,
        GroupAssignment computedGroupAssignment
    ) {
        Map<String, MemberAssignment> memberAssignments = computedGroupAssignment.members();
        Set<TopicIdPartition> topicPartitionAssignments = new HashSet<>();
        memberAssignments.values().forEach(memberAssignment -> {
            Map<Uuid, Set<Integer>> topicIdPartitions = memberAssignment.partitions();
            topicIdPartitions.forEach((topicId, partitions) ->
                partitions.forEach(partition -> topicPartitionAssignments.add(new TopicIdPartition(topicId, partition)))
            );
        });
        assertEquals(expectedPartitions, topicPartitionAssignments.size());
    }
}
