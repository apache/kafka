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
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.assertAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.invertedTargetAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpersTest.mkMapOfPartitionRacks;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UniformHeterogeneousAssignmentBuilderTest {
    private final UniformAssignor assignor = new UniformAssignor();
    private final Uuid topic1Uuid = Uuid.fromString("T1-A4s3VTwiI5CTbEp6POw");
    private final Uuid topic2Uuid = Uuid.fromString("T2-B4s3VTwiI5YHbPp6YUe");
    private final Uuid topic3Uuid = Uuid.fromString("T3-CU8fVTLCz5YMkLoDQsa");
    private final Uuid topic4Uuid = Uuid.fromString("T4-Tw9fVTLCz5HbPp6YQsa");
    private final String topic1Name = "topic1";
    private final String topic2Name = "topic2";
    private final String topic3Name = "topic3";
    private final String topic4Name = "topic4";
    private final String memberA = "A";
    private final String memberB = "B";
    private final String memberC = "C";

    @Test
    public void testTwoMembersNoTopicSubscription() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Collections.singletonMap(
                topic1Uuid,
                new TopicMetadata(
                    topic1Uuid,
                    topic1Name,
                    3,
                    mkMapOfPartitionRacks(3)
                )
            )
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.emptySet(),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
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

        GroupAssignment groupAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertEquals(Collections.emptyMap(), groupAssignment.members());
    }

    @Test
    public void testTwoMembersSubscribedToNonexistentTopics() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Collections.singletonMap(
                topic1Uuid,
                new TopicMetadata(
                    topic1Uuid,
                    topic1Name,
                    3,
                    mkMapOfPartitionRacks(3)
                )
            )
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.singleton(topic3Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.singleton(topic2Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            Collections.emptyMap()
        );

        assertThrows(
            PartitionAssignorException.class,
            () -> assignor.assign(groupSpec, subscribedTopicMetadata));
    }

    @Test
    public void testFirstAssignmentTwoMembersTwoTopics() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3,
            mkMapOfPartitionRacks(3)
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            6,
            mkMapOfPartitionRacks(6)
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            mkSet(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.singleton(topic3Uuid),
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
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2),
            mkTopicAssignment(topic3Uuid, 3, 5)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic3Uuid, 0, 1, 2, 4)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentNumMembersGreaterThanTotalNumPartitions() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            1,
            mkMapOfPartitionRacks(1)
        ));
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            2,
            mkMapOfPartitionRacks(2)
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.singleton(topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.singleton(topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.singleton(topic1Uuid),
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

        // Topic 3 has 2 partitions but three members subscribed to it - one of them should not get an assignment.
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic3Uuid, 0)
        ));
        expectedAssignment.put(memberB,
            Collections.emptyMap()
        );
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentForTwoMembersThreeTopicsGivenUnbalancedPrevAssignment() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            6,
            mkMapOfPartitionRacks(6)
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            4,
            mkMapOfPartitionRacks(4)
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            4,
            mkMapOfPartitionRacks(4)
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.singleton(topic1Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1, 2)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 3),
                mkTopicAssignment(topic2Uuid, 0)
            ))
        ));

        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid, topic3Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 4, 5),
                mkTopicAssignment(topic2Uuid, 1, 2, 3),
                mkTopicAssignment(topic3Uuid, 0, 1, 2, 3)
            ))
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2, 3, 4)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 1, 2, 3)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 5),
            mkTopicAssignment(topic3Uuid, 0, 1, 2, 3)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenPartitionsAreAddedForTwoMembers() {
        // Simulating adding partitions to T1, T2, T3 - originally T1 -> 4, T2 -> 3, T3 -> 2, T4 -> 3
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            6,
            mkMapOfPartitionRacks(6)
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            5,
            mkMapOfPartitionRacks(5)
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            3,
            mkMapOfPartitionRacks(3)
        ));
        topicMetadata.put(topic4Uuid, new TopicMetadata(
            topic4Uuid,
            topic4Name,
            3,
            mkMapOfPartitionRacks(3)
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            mkSet(topic1Uuid, topic3Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1, 2, 3),
                mkTopicAssignment(topic3Uuid, 0, 1)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid, topic3Uuid, topic4Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic2Uuid, 0, 1, 2),
                mkTopicAssignment(topic4Uuid, 0, 1, 2)
            ))
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2, 3, 4, 5),
            mkTopicAssignment(topic3Uuid, 0, 1, 2)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 1, 2, 3, 4),
            mkTopicAssignment(topic4Uuid, 0, 1, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneMemberAddedAndPartitionsAddedTwoMembersTwoTopics() {
        // Initially T1 -> 3, T2 -> 3 partitions.
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            6,
            mkMapOfPartitionRacks(6)
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            7,
            mkMapOfPartitionRacks(7)
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.singleton(topic1Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 2),
                mkTopicAssignment(topic2Uuid, 0)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 1),
                mkTopicAssignment(topic2Uuid, 1, 2)
            ))
        ));

        // Add a new member to trigger a re-assignment.
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2, 3, 4, 5)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1, 2, 5)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 3, 4, 6)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneMemberRemovedAfterInitialAssignmentWithThreeMembersThreeTopics() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3,
            mkMapOfPartitionRacks(3)
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            8,
            mkMapOfPartitionRacks(4)
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            3,
            mkMapOfPartitionRacks(3)
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            mkSet(topic1Uuid, topic3Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1, 2),
                mkTopicAssignment(topic3Uuid, 0, 1)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.singleton(topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic2Uuid, 3, 4, 5, 6)
            ))
        ));

        // Member C was removed

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2),
            mkTopicAssignment(topic3Uuid, 0, 1, 2)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 1, 2, 3, 4, 5, 6, 7)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneSubscriptionRemovedAfterInitialAssignmentWithTwoMembersTwoTopics() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3,
            mkMapOfPartitionRacks(3)
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            5,
            mkMapOfPartitionRacks(5)
        ));

        // Initial subscriptions were [T1, T2]
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.singleton(topic1Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 2),
                mkTopicAssignment(topic2Uuid, 1, 3)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 1),
                mkTopicAssignment(topic2Uuid, 0, 2, 4)
            ))
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 1, 2, 3, 4)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentWithTwoMembersIncludingOneWithoutSubscriptions() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3,
            mkMapOfPartitionRacks(3)
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            mkSet(topic1Uuid),
            Assignment.EMPTY
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
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
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2)
        ));
        expectedAssignment.put(memberB, Collections.emptyMap());

        assertAssignment(expectedAssignment, computedAssignment);
    }
}
