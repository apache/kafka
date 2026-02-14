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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.TopicIdPartition;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
            CoordinatorMetadataImage.EMPTY
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
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 3)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
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
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 3)
            .build();
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
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
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 3)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, 2)
            .build();

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
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertEveryPartitionGetsAssignment(5, computedAssignment);
    }

    @Test
    public void testAssignWithTwoMembersAndTwoTopicsHomogeneousWithAllowedMap() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 3)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, 3)
            .build();

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
            Map.of(),
            Optional.of(
                Map.of(
                    TOPIC_1_UUID, Set.of(0, 1, 2),
                    TOPIC_3_UUID, Set.of(0, 1)    // but not 2
                )
            )
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertEveryPartitionGetsAssignment(5, computedAssignment);
    }

    @Test
    public void testAssignWithTwoMembersAndTwoTopicsHomogeneousWithNonAssignableTopic() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 3)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, 2)
            .build();

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
            Map.of(),
            Optional.of(
                Map.of(TOPIC_1_UUID, Set.of(0, 1, 2))
            )
        );

        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertEveryPartitionGetsAssignment(3, computedAssignment);
    }

    @Test
    public void testAssignWithThreeMembersThreeTopicsHeterogeneous() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 3)
            .addTopic(TOPIC_2_UUID, TOPIC_2_NAME, 3)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, 2)
            .build();

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
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        // T1: 3 partitions + T2: 3 partitions + T3: 2 partitions = 8 partitions
        assertEveryPartitionGetsAssignment(8, computedAssignment);
    }

    @Test
    public void testAssignWithThreeMembersThreeTopicsHeterogeneousWithAllowedMap() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 3)
            .addTopic(TOPIC_2_UUID, TOPIC_2_NAME, 3)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, 2)
            .build();

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
            Map.of(),
            Optional.of(
                Map.of(
                    TOPIC_1_UUID, Set.of(0, 1), // but not 2
                    TOPIC_2_UUID, Set.of(0, 2), // but not 1
                    TOPIC_3_UUID, Set.of(1)     // but not 0
                )
            )
        );

        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        // T1: 2 partitions + T2: 2 partitions + T3: 1 partition = 5 partitions
        assertEveryPartitionGetsAssignment(5, computedAssignment);
    }

    @Test
    public void testAssignWithThreeMembersThreeTopicsHeterogeneousWithNonAssignableTopic() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 3)
            .addTopic(TOPIC_2_UUID, TOPIC_2_NAME, 3)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, 2)    // non-assignable
            .build();

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
            Map.of(),
            Optional.of(
                Map.of(
                    TOPIC_1_UUID, Set.of(0, 1, 2),
                    TOPIC_2_UUID, Set.of(0, 1, 2)
                )
            )
        );

        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(MEMBER_A, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 0, 1, 2),
            mkTopicAssignment(TOPIC_2_UUID, 0, 2)
        ));
        expectedAssignment.put(MEMBER_B, Map.of());
        expectedAssignment.put(MEMBER_C, mkAssignment(
            mkTopicAssignment(TOPIC_2_UUID, 1)
        ));

        // T1: 3 partitions + T2: 3 partitions + T3: 2 partitions(non-assignable) = 6 partitions
        assertEveryPartitionGetsAssignment(6, computedAssignment);
        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testAssignWithOneMemberNoAssignedTopicHeterogeneous() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 3)
            .addTopic(TOPIC_2_UUID, TOPIC_2_NAME, 2)
            .build();

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
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

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
    public void testIncrementalAssignmentIncreasingMembersHomogeneous() {
        final int numPartitions = 24;
        final int numMembers = 101;

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, numPartitions)
            .build();

        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Set<Uuid> topicsSubscription = new LinkedHashSet<>();
        topicsSubscription.add(TOPIC_1_UUID);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();

        SimpleAssignor assignor = new SimpleAssignor();

        // Increase the number of members one a time, checking that the partitions are assigned as expected
        for (int member = 0; member < numMembers; member++) {
            String newMemberId = "M" + member;
            members.put(newMemberId, new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                topicsSubscription,
                Assignment.EMPTY
            ));

            GroupSpec groupSpec = new GroupSpecImpl(
                members,
                HOMOGENEOUS,
                new HashMap<>()
            );

            GroupAssignment computedAssignment = assignor.assign(groupSpec, subscribedTopicMetadata);
            assertEveryPartitionGetsAssignment(numPartitions, computedAssignment);

            computedAssignment.members().forEach((memberId, partitions) -> members.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                topicsSubscription,
                new Assignment(partitions.partitions())
            )));
        }
    }

    @Test
    public void testIncrementalAssignmentDecreasingMembersHomogeneous() {
        final int numPartitions = 24;
        final int numMembers = 101;

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, numPartitions)
            .build();

        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage)
        );

        Set<Uuid> topicsSubscription = new LinkedHashSet<>();
        topicsSubscription.add(TOPIC_1_UUID);
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();

        SimpleAssignor assignor = new SimpleAssignor();

        for (int member = 0; member < numMembers; member++) {
            String newMemberId = "M" + member;
            members.put(newMemberId, new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                topicsSubscription,
                Assignment.EMPTY
            ));
        }

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            new HashMap<>()
        );

        GroupAssignment computedAssignment = assignor.assign(groupSpec, subscribedTopicMetadata);
        assertEveryPartitionGetsAssignment(numPartitions, computedAssignment);

        for (int member = 0; member < numMembers; member++) {
            String newMemberId = "M" + member;
            members.put(newMemberId, new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                topicsSubscription,
                new Assignment(computedAssignment.members().get(newMemberId).partitions()))
            );
        }

        // Decrease the number of members one a time, checking that the partitions are assigned as expected
        for (int member = numMembers - 1; member > 0; member--) {
            String newMemberId = "M" + member;
            members.remove(newMemberId);

            groupSpec = new GroupSpecImpl(
                members,
                HOMOGENEOUS,
                new HashMap<>()
            );

            computedAssignment = assignor.assign(groupSpec, subscribedTopicMetadata);
            assertEveryPartitionGetsAssignment(numPartitions, computedAssignment);

            computedAssignment.members().forEach((memberId, partitions) -> members.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                topicsSubscription,
                new Assignment(partitions.partitions())
            )));
        }
    }

    @Test
    public void testAssignWithCurrentAssignmentHeterogeneous() {
        // Current assignment setup - 3 members A - {T1, T2}, B - {T3}, C - {T2, T3}.
        MetadataImage metadataImage1 = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 3)
            .addTopic(TOPIC_2_UUID, TOPIC_2_NAME, 3)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, 2)
            .build();

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
        SubscribedTopicDescriberImpl subscribedTopicMetadata1 = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage1)
        );

        GroupAssignment computedAssignment1 = assignor.assign(
            groupSpec1,
            subscribedTopicMetadata1
        );

        assertEveryPartitionGetsAssignment(8, computedAssignment1);

        // New assignment setup - 2 members A - {T1, T2, T3}, B - {T3, T4}.

        MetadataImage metadataImage2 = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, 3)
            .addTopic(TOPIC_2_UUID, TOPIC_2_NAME, 3)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, 2)
            .addTopic(TOPIC_4_UUID, TOPIC_4_NAME, 1)
            .build();

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

        SubscribedTopicDescriberImpl subscribedTopicMetadata2 = new SubscribedTopicDescriberImpl(
            new KRaftCoordinatorMetadataImage(metadataImage2)
        );

        GroupAssignment computedAssignment2 = assignor.assign(
            groupSpec2,
            subscribedTopicMetadata2
        );

        assertEveryPartitionGetsAssignment(9, computedAssignment2);
    }

    @Test
    public void testIncrementalAssignmentIncreasingMembersHeterogeneous() {
        final int numPartitions = 24;
        final int numMembers = 101;

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, numPartitions / 2)
            .addTopic(TOPIC_2_UUID, TOPIC_2_NAME, numPartitions / 3)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, numPartitions / 6)
            .buildCoordinatorMetadataImage();

        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            metadataImage
        );

        ArrayList<Set<Uuid>> topicsSubscriptions = new ArrayList<>(3);
        Set<Uuid> topicsSubscription1 = new LinkedHashSet<>();
        topicsSubscription1.add(TOPIC_1_UUID);
        topicsSubscription1.add(TOPIC_2_UUID);
        topicsSubscription1.add(TOPIC_3_UUID);
        topicsSubscriptions.add(topicsSubscription1);
        Set<Uuid> topicsSubscription2 = new LinkedHashSet<>();
        topicsSubscription2.add(TOPIC_2_UUID);
        topicsSubscriptions.add(topicsSubscription2);
        Set<Uuid> topicsSubscription3 = new LinkedHashSet<>();
        topicsSubscription3.add(TOPIC_3_UUID);
        topicsSubscriptions.add(topicsSubscription3);
        Set<Uuid> topicsSubscription4 = new LinkedHashSet<>();
        topicsSubscription4.add(TOPIC_1_UUID);
        topicsSubscription4.add(TOPIC_2_UUID);
        topicsSubscriptions.add(topicsSubscription4);
        int numTopicsSubscriptions = 4;

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();

        SimpleAssignor assignor = new SimpleAssignor();

        // Increase the number of members one a time, checking that the partitions are assigned as expected
        for (int member = 0; member < numMembers; member++) {
            String newMemberId = "M" + member;
            members.put(newMemberId, new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                topicsSubscriptions.get(member % numTopicsSubscriptions),
                Assignment.EMPTY
            ));

            GroupSpec groupSpec = new GroupSpecImpl(
                members,
                HETEROGENEOUS,
                new HashMap<>()
            );

            GroupAssignment computedAssignment = assignor.assign(groupSpec, subscribedTopicMetadata);
            assertEveryPartitionGetsAssignment(numPartitions, computedAssignment);

            for (int m = 0; m < member; m++) {
                String memberId = "M" + m;
                members.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                    Optional.empty(),
                    Optional.empty(),
                    topicsSubscriptions.get(m % numTopicsSubscriptions),
                    new Assignment(computedAssignment.members().get(memberId).partitions())
                ));
            }
        }
    }

    @Test
    public void testIncrementalAssignmentDecreasingMembersHeterogeneous() {
        final int numPartitions = 24;
        final int numMembers = 101;

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(TOPIC_1_UUID, TOPIC_1_NAME, numPartitions / 2)
            .addTopic(TOPIC_2_UUID, TOPIC_2_NAME, numPartitions / 3)
            .addTopic(TOPIC_3_UUID, TOPIC_3_NAME, numPartitions / 6)
            .buildCoordinatorMetadataImage();

        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            metadataImage
        );

        ArrayList<Set<Uuid>> topicsSubscriptions = new ArrayList<>(3);
        Set<Uuid> topicsSubscription1 = new LinkedHashSet<>();
        topicsSubscription1.add(TOPIC_1_UUID);
        topicsSubscription1.add(TOPIC_2_UUID);
        topicsSubscription1.add(TOPIC_3_UUID);
        topicsSubscriptions.add(topicsSubscription1);
        Set<Uuid> topicsSubscription2 = new LinkedHashSet<>();
        topicsSubscription2.add(TOPIC_2_UUID);
        topicsSubscriptions.add(topicsSubscription2);
        Set<Uuid> topicsSubscription3 = new LinkedHashSet<>();
        topicsSubscription3.add(TOPIC_3_UUID);
        topicsSubscriptions.add(topicsSubscription3);
        Set<Uuid> topicsSubscription4 = new LinkedHashSet<>();
        topicsSubscription4.add(TOPIC_1_UUID);
        topicsSubscription4.add(TOPIC_2_UUID);
        topicsSubscriptions.add(topicsSubscription4);
        int numTopicsSubscriptions = 4;

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new HashMap<>();

        SimpleAssignor assignor = new SimpleAssignor();

        for (int member = 0; member < numMembers; member++) {
            String newMemberId = "M" + member;
            members.put(newMemberId, new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                topicsSubscriptions.get(member % numTopicsSubscriptions),
                Assignment.EMPTY
            ));
        }

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            new HashMap<>()
        );

        GroupAssignment computedAssignment = assignor.assign(groupSpec, subscribedTopicMetadata);
        assertEveryPartitionGetsAssignment(numPartitions, computedAssignment);

        for (int member = 0; member < numMembers; member++) {
            String newMemberId = "M" + member;
            members.put(newMemberId, new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                topicsSubscriptions.get(member % numTopicsSubscriptions),
                new Assignment(computedAssignment.members().get(newMemberId).partitions()))
            );
        }

        // Decrease the number of members one a time, checking that the partitions are assigned as expected
        for (int member = numMembers - 1; member > 0; member--) {
            String newMemberId = "M" + member;
            members.remove(newMemberId);

            groupSpec = new GroupSpecImpl(
                members,
                HETEROGENEOUS,
                new HashMap<>()
            );

            computedAssignment = assignor.assign(groupSpec, subscribedTopicMetadata);
            assertEveryPartitionGetsAssignment(numPartitions, computedAssignment);

            for (int m = 0; m < member; m++) {
                String memberId = "M" + m;
                members.put(memberId, new MemberSubscriptionAndAssignmentImpl(
                    Optional.empty(),
                    Optional.empty(),
                    topicsSubscriptions.get(m % numTopicsSubscriptions),
                    new Assignment(computedAssignment.members().get(memberId).partitions())
                ));
            }
        }
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
