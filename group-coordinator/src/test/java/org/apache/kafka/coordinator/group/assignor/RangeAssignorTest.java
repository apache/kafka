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
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
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

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.invertedTargetAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RangeAssignorTest {
    private final RangeAssignor assignor = new RangeAssignor();
    private final Uuid topic1Uuid = Uuid.randomUuid();
    private final String topic1Name = "topic1";
    private final Uuid topic2Uuid = Uuid.randomUuid();
    private final String topic2Name = "topic2";
    private final Uuid topic3Uuid = Uuid.randomUuid();
    private final String topic3Name = "topic3";
    private final String memberA = "A";
    private final String memberB = "B";
    private final String memberC = "C";

    @Test
    public void testOneMemberNoTopic() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Collections.singletonMap(
                topic1Uuid,
                new TopicMetadata(
                    topic1Uuid,
                    topic1Name,
                    3
                )
            )
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = Collections.singletonMap(
            memberA,
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

        Map<String, MemberAssignment> expectedAssignment = Collections.singletonMap(
            memberA,
            new MemberAssignmentImpl(Collections.emptyMap())
        );

        assertEquals(expectedAssignment, groupAssignment.members());
    }

    @Test
    public void testOneMemberSubscribedToNonExistentTopic() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Collections.singletonMap(
                topic1Uuid,
                new TopicMetadata(
                    topic1Uuid,
                    topic1Name,
                    3
                )
            )
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = Collections.singletonMap(
            memberA,
            new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Optional.empty(),
                Set.of(topic2Uuid),
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
    public void testFirstAssignmentTwoMembersTwoTopicsSameSubscriptions() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic3Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic3Uuid, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentThreeMembersThreeTopicsDifferentSubscriptions() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            3
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            Assignment.EMPTY
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic2Uuid, topic3Uuid),
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
            mkTopicAssignment(topic1Uuid, 0, 1, 2),
            mkTopicAssignment(topic2Uuid, 0, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic3Uuid, 0)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 2),
            mkTopicAssignment(topic3Uuid, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentNumMembersGreaterThanNumPartitions() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 3 has 2 partitions but three Members subscribed to it - one of them will not get a partition.
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic3Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic3Uuid, 1)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testStaticMembership() throws PartitionAssignorException {
        SubscribedTopicDescriber subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Collections.singletonMap(
                topic1Uuid,
                new TopicMetadata(
                    topic1Uuid,
                    topic1Name,
                    3
                )
            )
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("instanceA"),
            Collections.singleton(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("instanceB"),
            Collections.singleton(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            SubscriptionType.HOMOGENEOUS,
            invertedTargetAssignment(members)
        );

        GroupAssignment initialAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        // Remove static memberA and add it back with a different member Id but same instance Id.
        members.remove(memberA);
        members.put("memberA1", new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("instanceA"),
            Collections.singleton(topic1Uuid),
            Assignment.EMPTY
        ));

        groupSpec = new GroupSpecImpl(
            members,
            SubscriptionType.HOMOGENEOUS,
            invertedTargetAssignment(members)
        );

        GroupAssignment reassignedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        // Assert that the assignment did not change
        assertEquals(
            initialAssignment.members().get(memberA).partitions(),
            reassignedAssignment.members().get("memberA1").partitions()
        );

        assertEquals(
            initialAssignment.members().get(memberB).partitions(),
            reassignedAssignment.members().get(memberB).partitions()
        );
    }

    @Test
    public void testMixedStaticMembership() throws PartitionAssignorException {
        SubscribedTopicDescriber subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Collections.singletonMap(
                topic1Uuid,
                new TopicMetadata(
                    topic1Uuid,
                    topic1Name,
                    5
                )
            )
        );

        // Initialize members with instance Ids.
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("instanceA"),
            Collections.singleton(topic1Uuid),
            Assignment.EMPTY
        ));
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("instanceC"),
            Collections.singleton(topic1Uuid),
            Assignment.EMPTY
        ));

        // Initialize member without an instance Id.
        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Collections.singleton(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            SubscriptionType.HOMOGENEOUS,
            invertedTargetAssignment(members)
        );

        GroupAssignment initialAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        // Remove memberA and add it back with a different member Id but same instance Id.
        members.remove(memberA);
        members.put("memberA1", new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.of("instanceA"),
            Collections.singleton(topic1Uuid),
            Assignment.EMPTY
        ));

        groupSpec = new GroupSpecImpl(
            members,
            SubscriptionType.HOMOGENEOUS,
            invertedTargetAssignment(members)
        );

        GroupAssignment reassignedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        // Assert that the assignments did not change.
        assertEquals(
            initialAssignment.members().get(memberA).partitions(),
            reassignedAssignment.members().get("memberA1").partitions()
        );

        assertEquals(
            initialAssignment.members().get(memberB).partitions(),
            reassignedAssignment.members().get(memberB).partitions()
        );

        assertEquals(
            initialAssignment.members().get(memberC).partitions(),
            reassignedAssignment.members().get(memberC).partitions()
        );
    }

    @Test
    public void testReassignmentNumMembersGreaterThanNumPartitionsWhenOneMemberAdded() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            2
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            2
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0),
                mkTopicAssignment(topic2Uuid, 0)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 1),
                mkTopicAssignment(topic2Uuid, 1)
            ))
        ));

        // Add a new Member to trigger a re-assignment
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1)
        ));
        // Member C shouldn't get any assignment.
        expectedAssignment.put(memberC, Collections.emptyMap());

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOnePartitionAddedForTwoMembersTwoTopics() {
        // Simulating adding a partition - originally T1 -> 3 Partitions and T2 -> 3 Partitions
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            4
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            4
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1),
                mkTopicAssignment(topic2Uuid, 0, 1)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 2)
            ))
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic2Uuid, 0, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2, 3),
            mkTopicAssignment(topic2Uuid, 2, 3)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneMemberAddedAfterInitialAssignmentWithTwoMembersTwoTopics() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            3
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1),
                mkTopicAssignment(topic2Uuid, 0, 1)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 2)
            ))
        ));

        // Add a new Member to trigger a re-assignment
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneMemberAddedAndOnePartitionAfterInitialAssignmentWithTwoMembersTwoTopics() {
        // Add a new partition to topic 1, initially T1 -> 3 partitions
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            4
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            3
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1),
                mkTopicAssignment(topic2Uuid, 0, 1)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 2)
            ))
        ));

        // Add a new Member to trigger a re-assignment
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
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
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic2Uuid, 0, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 2)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 3)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneMemberRemovedAfterInitialAssignmentWithTwoMembersTwoTopics() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            3
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        // Member A was removed

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 2)
            ))
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2),
            mkTopicAssignment(topic2Uuid, 0, 1, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenMultipleSubscriptionsRemovedAfterInitialAssignmentWithThreeMembersTwoTopics() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            3
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2
        ));

        // Let initial subscriptions be A -> T1, T2 // B -> T2 // C -> T2, T3
        // Change the subscriptions to A -> T1 // B -> T1, T2, T3 // C -> T2
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1, 2),
                mkTopicAssignment(topic2Uuid, 0)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic1Uuid, topic2Uuid, topic3Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic2Uuid, 1)
            ))
        ));

        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic2Uuid, 2),
                mkTopicAssignment(topic3Uuid, 0, 1)
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
            mkTopicAssignment(topic1Uuid, 0, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 0, 1),
            mkTopicAssignment(topic3Uuid, 0, 1)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    /**
     * Asserts that the computed group assignment matches the expected assignment.
     *
     * @param expectedAssignment       A map representing the expected assignment for each member.
     *                                 The key is the member Id and the value is another map where
     *                                 the key is the topic Uuid and the value is a set of assigned partition Ids.
     * @param computedGroupAssignment  The computed group assignment to be checked against the expected assignment.
     *                                 Contains the actual assignments for each member.
     */
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
}
