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
import org.apache.kafka.coordinator.group.consumer.SubscribedTopicMetadata;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
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

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.assertAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.RecordHelpersTest.mkMapOfPartitionRacks;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OptimizedUniformAssignmentBuilderTest {
    private final UniformAssignor assignor = new UniformAssignor();
    private final Uuid topic1Uuid = Uuid.fromString("T1-A4s3VTwiI5CTbEp6POw");
    private final Uuid topic2Uuid = Uuid.fromString("T2-B4s3VTwiI5YHbPp6YUe");
    private final Uuid topic3Uuid = Uuid.fromString("T3-CU8fVTLCz5YMkLoDQsa");
    private final String topic1Name = "topic1";
    private final String topic2Name = "topic2";
    private final String topic3Name = "topic3";
    private final String memberA = "A";
    private final String memberB = "B";
    private final String memberC = "C";

    @Test
    public void testOneMemberNoTopicSubscription() {
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(
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

        Map<String, AssignmentMemberSpec> members = Collections.singletonMap(
            memberA,
            new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                Collections.emptyList(),
                Collections.emptyMap()
            )
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        assertEquals(Collections.emptyMap(), groupAssignment.members());
    }

    @Test
    public void testOneMemberSubscribedToNonexistentTopic() {
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(
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

        Map<String, AssignmentMemberSpec> members = Collections.singletonMap(
            memberA,
            new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                Collections.singletonList(topic2Uuid),
                Collections.emptyMap()
            )
        );

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);

        assertThrows(PartitionAssignorException.class,
            () -> assignor.assign(assignmentSpec, subscribedTopicMetadata));
    }

    @Test
    public void testFirstAssignmentTwoMembersTwoTopicsNoMemberRacks() {
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
            2,
            mkMapOfPartitionRacks(2)
        ));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap()
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2),
            mkTopicAssignment(topic3Uuid, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic3Uuid, 0)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testFirstAssignmentTwoMembersTwoTopicsNoPartitionRacks() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2,
            Collections.emptyMap()
        ));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack1"),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap()
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2),
            mkTopicAssignment(topic3Uuid, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic3Uuid, 0)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testFirstAssignmentThreeMembersThreeTopicsWithMemberAndPartitionRacks() {
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
            3,
            mkMapOfPartitionRacks(3)
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2,
            mkMapOfPartitionRacks(2)
        ));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack1"),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));
        members.put(memberC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack3"),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0),
            mkTopicAssignment(topic3Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1),
            mkTopicAssignment(topic3Uuid, 1)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testFirstAssignmentThreeMembersThreeTopicsWithMemberAndPartitionRacks2() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            2,
            mkMapOfPartitionRacks(2)
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            2,
            mkMapOfPartitionRacks(2)
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2,
            mkMapOfPartitionRacks(2)
        ));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack0"),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack1"),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));
        members.put(memberC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic3Uuid, 0)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 1),
            mkTopicAssignment(topic3Uuid, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testFirstAssignmentThreeMembersThreeTopicsWithSomeMemberAndPartitionRacks() {
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
            3,
            mkMapOfPartitionRacks(3)
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2,
            Collections.emptyMap()
        ));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));
        members.put(memberC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 2),
            mkTopicAssignment(topic3Uuid, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1, 2),
            mkTopicAssignment(topic2Uuid, 1)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0),
            mkTopicAssignment(topic3Uuid, 0)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testFirstAssignmentNumMembersGreaterThanTotalNumPartitions() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2,
            mkMapOfPartitionRacks(2)
        ));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic3Uuid),
            Collections.emptyMap()
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic3Uuid),
            Collections.emptyMap()
        ));
        members.put(memberC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        // Topic 3 has 2 partitions but three members subscribed to it - one of them should not get an assignment.
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic3Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic3Uuid, 1)
        ));
        expectedAssignment.put(memberC,
            Collections.emptyMap()
        );

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testValidityAndBalanceForLargeSampleSet() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        for (int i = 1; i < 100; i++) {
            Uuid topicId = Uuid.randomUuid();
            topicMetadata.put(topicId, new TopicMetadata(
                topicId,
                "topic-" + i,
                3,
                mkMapOfPartitionRacks(3)
            ));
        }

        List<Uuid> subscribedTopics = new ArrayList<>(topicMetadata.keySet());

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        for (int i = 1; i < 50; i++) {
            members.put("member" + i, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                subscribedTopics,
                Collections.emptyMap()
            ));
        }

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentForTwoMembersTwoTopicsGivenUnbalancedPrevAssignment() {
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
            3,
            mkMapOfPartitionRacks(3)
        ));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1),
                mkTopicAssignment(topic2Uuid, 0, 1)
            )
        );
        members.put(memberA, new AssignmentMemberSpec(
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
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic2Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 1, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentOnRackChangesWithMemberAndPartitionRacks() {
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
            3,
            mkMapOfPartitionRacks(3)
        ));

        // Initially A was in rack 1 and B was in rack 2, now let's switch them.
        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1),
                mkTopicAssignment(topic2Uuid, 0)
            )
        );
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 1, 2)
            )
        );
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack1"),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1, 2),
            mkTopicAssignment(topic2Uuid, 2)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 1, 0)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentOnAddingPartitionsWithMemberAndPartitionRacks() {
        // Initially T1,T2 had 3 partitions.
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            5,
            mkMapOfPartitionRacks(5)
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            5,
            mkMapOfPartitionRacks(5)
        ));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0),
                mkTopicAssignment(topic2Uuid, 0, 2)
            )
        );
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack0"),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 1, 2),
                mkTopicAssignment(topic2Uuid, 1)
            )
        );
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack1"),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 3),
            mkTopicAssignment(topic2Uuid, 0, 3, 2)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1, 4, 2),
            mkTopicAssignment(topic2Uuid, 1, 4)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenPartitionsAreAddedForTwoMembersTwoTopics() {
        // Simulating adding partition to T1 and T2 - originally T1 -> 3 Partitions and T2 -> 3 Partitions
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

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 2),
                mkTopicAssignment(topic2Uuid, 0)
            )
        );
        members.put(memberA, new AssignmentMemberSpec(
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
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2, 3, 5),
            mkTopicAssignment(topic2Uuid, 0, 4)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1, 4),
            mkTopicAssignment(topic2Uuid, 1, 2, 3)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneMemberAddedAfterInitialAssignmentWithTwoMembersTwoTopics() {
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
            3,
            mkMapOfPartitionRacks(3)
        ));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2),
            mkTopicAssignment(topic2Uuid, 0)
        ));
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = new TreeMap<>(mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1, 2)
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        // Add a new member to trigger a re-assignment.
        members.put(memberC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentOnAddingMemberWithRackAndPartitionRacks() {
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
            3,
            mkMapOfPartitionRacks(3)
        ));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0),
                mkTopicAssignment(topic2Uuid, 0, 2)
            )
        );
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack0"),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 1, 2),
                mkTopicAssignment(topic2Uuid, 1)
            )
        );
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack1"),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        // New member added.
        members.put(memberC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

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
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneMemberRemovedAfterInitialAssignmentWithThreeMembersTwoTopics() {
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
            3,
            mkMapOfPartitionRacks(3)
        ));

        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0)
        );
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1)
        );
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        // Member C was removed

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2),
            mkTopicAssignment(topic2Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneSubscriptionRemovedAfterInitialAssignmentWithTwoMembersTwoTopics() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            2,
            mkMapOfPartitionRacks(2)
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            2,
            mkMapOfPartitionRacks(2)
        ));

        // Initial subscriptions were [T1, T2]
        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0)
        );
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic2Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1)
        );
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic2Uuid),
            currentAssignmentForB
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneSubscriptionRemovedWithMemberAndPartitionRacks() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            5,
            mkMapOfPartitionRacks(3)
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            2,
            mkMapOfPartitionRacks(3)
        ));

        // Initial subscriptions were [T1, T2].
        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 2, 4),
                mkTopicAssignment(topic2Uuid, 0)
            )
        );
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack0"),
            Collections.singletonList(topic1Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 1, 3),
                mkTopicAssignment(topic2Uuid, 1)
            )
        );
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack1"),
            Collections.singletonList(topic1Uuid),
            currentAssignmentForB
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2, 4)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1, 3)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
        checkValidityAndBalance(members, computedAssignment);
    }

    /**
     * Verifies that the given assignment is valid with respect to the given subscriptions.
     * Validity requirements:
     *      - each member is subscribed to topics of all partitions assigned to it, and
     *      - each partition is assigned to no more than one member.
     * Balance requirements:
     *      - the assignment is fully balanced (the numbers of topic partitions assigned to members differ by at most one), or
     *      - there is no topic partition that can be moved from one member to another with 2+ fewer topic partitions.
     *
     * @param members                   Members data structure from the assignment Spec.
     * @param computedGroupAssignment   Assignment computed by the uniform assignor.
     */
    private void checkValidityAndBalance(
        Map<String, AssignmentMemberSpec> members,
        GroupAssignment computedGroupAssignment
    ) {
        List<String> membersList = new ArrayList<>(computedGroupAssignment.members().keySet());
        int numMembers = membersList.size();
        List<Integer> totalAssignmentSizesOfAllMembers = new ArrayList<>(membersList.size());
        membersList.forEach(member -> {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = computedGroupAssignment
                .members().get(member).targetPartitions();
            int sum = computedAssignmentForMember.values().stream().mapToInt(Set::size).sum();
            totalAssignmentSizesOfAllMembers.add(sum);
        });

        for (int i = 0; i < numMembers; i++) {
            String memberId = membersList.get(i);
            Map<Uuid, Set<Integer>> computedAssignmentForMember =
                computedGroupAssignment.members().get(memberId).targetPartitions();
            // Each member is subscribed to topics of all the partitions assigned to it.
            computedAssignmentForMember.keySet().forEach(topicId -> {
                // Check if the topic exists in the subscription.
                assertTrue(members.get(memberId).subscribedTopicIds().contains(topicId),
                        "Error: Partitions for topic " + topicId + " are assigned to member " + memberId +
                                " but it is not part of the members subscription ");
            });

            for (int j = i + 1; j < numMembers; j++) {
                String otherMemberId = membersList.get(j);
                Map<Uuid, Set<Integer>> computedAssignmentForOtherMember = computedGroupAssignment
                    .members().get(otherMemberId).targetPartitions();
                // Each partition should be assigned to at most one member
                computedAssignmentForMember.keySet().forEach(topicId -> {
                    Set<Integer> intersection = new HashSet<>();
                    if (computedAssignmentForOtherMember.containsKey(topicId)) {
                        intersection = new HashSet<>(computedAssignmentForMember.get(topicId));
                        intersection.retainAll(computedAssignmentForOtherMember.get(topicId));
                    }
                    assertTrue(
                        intersection.isEmpty(),
                        "Error : Member 1 " + memberId + " and Member 2 " + otherMemberId +
                            "have common partitions assigned to them " + computedAssignmentForOtherMember.get(topicId)
                    );
                });

                // Difference in the sizes of any two partitions should be 1 at max
                int size1 = totalAssignmentSizesOfAllMembers.get(i);
                int size2 = totalAssignmentSizesOfAllMembers.get(j);
                assertTrue(
                    Math.abs(size1 - size2) <= 1,
                    "Size of one assignment is greater than the other assignment by more than one partition "
                        + size1 + " " + size2 + "abs = " + Math.abs(size1 - size2)
                );
            }
        }
    }
}
