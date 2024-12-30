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

        members.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_1_UUID, TOPIC_3_UUID),
            Assignment.EMPTY
        ));

        members.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_1_UUID, TOPIC_3_UUID),
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

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(MEMBER_A, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 0, 1, 2),
            mkTopicAssignment(TOPIC_3_UUID, 0, 1)
        ));
        expectedAssignment.put(MEMBER_B, mkAssignment(
            mkTopicAssignment(TOPIC_1_UUID, 0, 1, 2),
            mkTopicAssignment(TOPIC_3_UUID, 0, 1)
        ));

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

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_1_UUID, TOPIC_2_UUID),
            Assignment.EMPTY
        ));

        members.put(MEMBER_B, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_3_UUID),
            Assignment.EMPTY
        ));

        String memberC = "C";
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_2_UUID, TOPIC_3_UUID),
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
            mkTopicAssignment(TOPIC_2_UUID, 0, 1, 2)
        ));
        expectedAssignment.put(MEMBER_B, mkAssignment(
            mkTopicAssignment(TOPIC_3_UUID, 0, 1)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(TOPIC_2_UUID, 0, 1, 2),
            mkTopicAssignment(TOPIC_3_UUID, 0, 1)
        ));

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

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();
        members.put(MEMBER_A, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(TOPIC_1_UUID, TOPIC_2_UUID),
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
            mkTopicAssignment(TOPIC_2_UUID, 0, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
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
}
