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

import javax.swing.text.html.Option;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

public class GeneralUniformAssignmentBuilderTest {
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

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyMap()
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        assertEquals(Collections.emptyMap(), groupAssignment.members());
    }

    @Test
    public void testTwoMembersSubscribedToNonexistentTopics() {
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
            Collections.singletonList(topic2Uuid),
            Collections.emptyMap()
        ));

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
            6,
            mkMapOfPartitionRacks(6)
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
            Collections.singletonList(topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

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
    public void testFirstAssignmentThreeMembersThreeTopicsNoPartitionRacks() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            6,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            8,
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
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Collections.singletonList(topic2Uuid),
            Collections.emptyMap()
        ));
        members.put(memberC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack3"),
            Arrays.asList(topic1Uuid, topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic2Uuid, 2, 4, 6),
            mkTopicAssignment(topic1Uuid, 0, 4)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 1, 3, 5, 7)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1, 2, 3, 5),
            mkTopicAssignment(topic3Uuid, 0, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
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
            Collections.singletonList(topic1Uuid),
            Collections.emptyMap()
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Arrays.asList(topic1Uuid, topic2Uuid),
            Collections.emptyMap()
        ));
        members.put(memberC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack3"),
            Collections.singletonList(topic3Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 1, 2)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic3Uuid, 0, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testRT1SMALLSETFirstAssignmentSixTopicsNineMembersWithMemberAndPartitionRacks() {
        // 9 MEMBERS || 120 PARTITIONS || 6 TOPICS || RACK
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();

        // Create 6 topics, each with 20 partitions.
        for (int i = 1; i <= 6; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid, topicName, 20, mkMapOfPartitionRacks(20)));
        }

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        // Create 9 members and distribute topics among them.
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());
        int topicIndex = 0;
        for (int i = 1; i <= 9; i++) {
            String memberName = "member" + i;
            String rackName = "rack" + i;

            // Assign two topics to each member, allowing for repetition.
            List<Uuid> assignedTopics = new ArrayList<>();
            for (int j = 0; j < 2; j++, topicIndex++) {
                assignedTopics.add(topicUuids.get(topicIndex % topicUuids.size()));
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.of(rackName),
                assignedTopics,
                Collections.emptyMap()));
        }

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);
    }

    @Test
    public void testRT1SMALLSETFirstAssignmentSixTopicsNineMembersNoRack() {
        // 9 MEMBERS || 120 PARTITIONS || 6 TOPICS || NO RACK
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();

        // Create 6 topics, each with 20 partitions.
        for (int i = 1; i <= 6; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid, topicName, 20, Collections.emptyMap()));
        }

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        // Create 9 members and distribute topics among them.
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());
        int topicIndex = 0;
        for (int i = 1; i <= 9; i++) {
            String memberName = "member" + i;

            // Assign two topics to each member, allowing for repetition.
            List<Uuid> assignedTopics = new ArrayList<>();
            for (int j = 0; j < 2; j++, topicIndex++) {
                assignedTopics.add(topicUuids.get(topicIndex % topicUuids.size()));
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                assignedTopics,
                Collections.emptyMap()));
        }

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);
    }

    @Test
    public void testRT1LARGESETReAssignmentSixTopicsNineMembersNoRack() {
        // 2K MEMBERS || 2K PARTITIONS || 50 TOPICS || NO RACK
        // ADD 3 MEMBERS || ADD NEW TOPIC WITH 100 PARTITIONS AND UPDATE ALL MEMBER SUBSCRIPTIONS WITH THIS NEW TOPIC
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        List<Uuid> topicIds = new ArrayList<>();

        // Create 50 topics with a total of 2000 partitions
        int totalPartitions = 2000;
        int partitionsPerTopic = totalPartitions / 50;
        for (int i = 1; i <= 50; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid, topicName, partitionsPerTopic, Collections.emptyMap()));
        }

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        // Create 2000 consumers and distribute topics among them
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());
        int consumers = 2000;
        int topicsPerConsumer = 50 / (consumers / topicUuids.size());

        for (int i = 1; i <= consumers; i++) {
            String memberName = "consumer" + i;
            String rackName = "rack" + (i % 50);

            // Distribute topics among consumers
            List<Uuid> assignedTopics = new ArrayList<>();
            for (int j = 0; j < topicsPerConsumer; j++) {
                int topicIndex = (i + j * consumers / topicsPerConsumer) % topicUuids.size();
                assignedTopics.add(topicUuids.get(topicIndex));
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                assignedTopics,
                Collections.emptyMap()));
        }

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        // Update members with the initial assignment and subscribe them to the new topic
        Uuid newTopicUuid = Uuid.randomUuid();
        String newTopicName = "newTopic";
        topicMetadata.put(newTopicUuid, new TopicMetadata(newTopicUuid, newTopicName, 100, Collections.emptyMap()));

        for (Map.Entry<String, MemberAssignment> entry : computedAssignment.members().entrySet()) {
            String memberId = entry.getKey();
            MemberAssignment memberAssignment = entry.getValue();

            Map<Uuid, Set<Integer>> currentAssignment = memberAssignment.targetPartitions();
            List<Uuid> updatedSubscriptions = new ArrayList<>(members.get(memberId).subscribedTopicIds());
            updatedSubscriptions.add(newTopicUuid);

            members.put(memberId, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                updatedSubscriptions,
                currentAssignment
            ));
        }

        // Add 3 new members and subscribe them to all the topics.
        for (int i = 1; i <= 3; i++) {
            String newMemberId = "newMember" + i;
            members.put(newMemberId, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                topicIds,
                Collections.emptyMap()
            ));
        }

        // Re-assign with updated members and topics
        AssignmentSpec updatedAssignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata updatedSubscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment rebalancedAssignment = assignor.assign(updatedAssignmentSpec, updatedSubscribedTopicMetadata);
    }

    @Test
    public void testRT1LARGESETReAssignmentSixTopicsNineMembersYesRack() {
        // 2K MEMBERS || 2K PARTITIONS || 50 TOPICS || RACK
        // ADD 3 MEMBERS || ADD NEW TOPIC WITH 100 PARTITIONS AND UPDATE ALL MEMBER SUBSCRIPTIONS WITH THIS NEW TOPIC
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        List<Uuid> topicIds = new ArrayList<>();

        // Create 50 topics with a total of 2000 partitions
        int totalPartitions = 2000;
        int partitionsPerTopic = totalPartitions / 50;
        for (int i = 1; i <= 50; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid, topicName, partitionsPerTopic, mkMapOfPartitionRacks(partitionsPerTopic)));
        }

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        // Create 2000 consumers and distribute topics among them
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());
        int consumers = 2000;
        int topicsPerConsumer = 50 / (consumers / topicUuids.size());

        for (int i = 1; i <= consumers; i++) {
            String memberName = "consumer" + i;
            String rackName = "rack" + (i % 50);

            // Distribute topics among consumers
            List<Uuid> assignedTopics = new ArrayList<>();
            for (int j = 0; j < topicsPerConsumer; j++) {
                int topicIndex = (i + j * consumers / topicsPerConsumer) % topicUuids.size();
                assignedTopics.add(topicUuids.get(topicIndex));
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.of(rackName),
                assignedTopics,
                Collections.emptyMap()));
        }

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        // Update members with the initial assignment and subscribe them to the new topic
        Uuid newTopicUuid = Uuid.randomUuid();
        String newTopicName = "newTopic";
        topicMetadata.put(newTopicUuid, new TopicMetadata(newTopicUuid, newTopicName, 100, Collections.emptyMap()));

        for (Map.Entry<String, MemberAssignment> entry : computedAssignment.members().entrySet()) {
            String memberId = entry.getKey();
            MemberAssignment memberAssignment = entry.getValue();

            Map<Uuid, Set<Integer>> currentAssignment = memberAssignment.targetPartitions();
            List<Uuid> updatedSubscriptions = new ArrayList<>(members.get(memberId).subscribedTopicIds());
            Optional<String> rackId = members.get(memberId).rackId();
            updatedSubscriptions.add(newTopicUuid);

            members.put(memberId, new AssignmentMemberSpec(
                Optional.empty(),
                rackId,
                updatedSubscriptions,
                currentAssignment
            ));
        }

        // Add 3 new members and subscribe them to all the topics.
        for (int i = 1; i <= 3; i++) {
            String newMemberId = "newMember" + i;
            String rackName = "rack" + (i % 50);
            members.put(newMemberId, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.of(rackName),
                topicIds,
                Collections.emptyMap()
            ));
        }

        // Re-assign with updated members and topics
        AssignmentSpec updatedAssignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata updatedSubscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment rebalancedAssignment = assignor.assign(updatedAssignmentSpec, updatedSubscribedTopicMetadata);
    }

    @Test
    public void testRT1LARGESETReAssignment2k2k500TopicsYesRack() {
        // 2K MEMBERS || 2K PARTITIONS || 500 TOPICS || RACK
        // ADD 3 MEMBERS || ADD NEW TOPIC WITH 100 PARTITIONS AND UPDATE ALL MEMBER SUBSCRIPTIONS WITH THIS NEW TOPIC
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        List<Uuid> topicIds = new ArrayList<>();

        // Create 500 topics with a total of 2000 partitions
        int totalPartitions = 2000;
        int partitionsPerTopic = totalPartitions / 500;
        for (int i = 1; i <= 500; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid, topicName, partitionsPerTopic, mkMapOfPartitionRacks(partitionsPerTopic)));
        }

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        // Create 2000 consumers and distribute topics among them
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());
        int consumers = 2000;
        int topicsPerConsumer = 500 / (consumers / topicUuids.size());

        for (int i = 1; i <= consumers; i++) {
            String memberName = "consumer" + i;
            String rackName = "rack" + (i % 50);

            // Distribute topics among consumers
            List<Uuid> assignedTopics = new ArrayList<>();
            for (int j = 0; j < topicsPerConsumer; j++) {
                int topicIndex = (i + j * consumers / topicsPerConsumer) % topicUuids.size();
                assignedTopics.add(topicUuids.get(topicIndex));
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.of(rackName),
                assignedTopics,
                Collections.emptyMap()));
        }

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        // Update members with the initial assignment and subscribe them to the new topic
        Uuid newTopicUuid = Uuid.randomUuid();
        String newTopicName = "newTopic";
        topicMetadata.put(newTopicUuid, new TopicMetadata(newTopicUuid, newTopicName, 100, Collections.emptyMap()));

        for (Map.Entry<String, MemberAssignment> entry : computedAssignment.members().entrySet()) {
            String memberId = entry.getKey();
            MemberAssignment memberAssignment = entry.getValue();

            Map<Uuid, Set<Integer>> currentAssignment = memberAssignment.targetPartitions();
            List<Uuid> updatedSubscriptions = new ArrayList<>(members.get(memberId).subscribedTopicIds());
            Optional<String> rackId = members.get(memberId).rackId();
            updatedSubscriptions.add(newTopicUuid);

            members.put(memberId, new AssignmentMemberSpec(
                Optional.empty(),
                rackId,
                updatedSubscriptions,
                currentAssignment
            ));
        }

        // Add 3 new members and subscribe them to all the topics.
        for (int i = 1; i <= 3; i++) {
            String newMemberId = "newMember" + i;
            String rackName = "rack" + (i % 50);
            members.put(newMemberId, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.of(rackName),
                topicIds,
                Collections.emptyMap()
            ));
        }

        // Re-assign with updated members and topics
        AssignmentSpec updatedAssignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata updatedSubscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment rebalancedAssignment = assignor.assign(updatedAssignmentSpec, updatedSubscribedTopicMetadata);
    }

    @Test
    public void testRT1LARGESETReAssignment2k2k500TopicsNoRack() {
        // 2K MEMBERS || 2K PARTITIONS || 500 TOPICS || NO RACK
        // ADD 3 MEMBERS || ADD NEW TOPIC WITH 100 PARTITIONS AND UPDATE ALL MEMBER SUBSCRIPTIONS WITH THIS NEW TOPIC
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        List<Uuid> topicIds = new ArrayList<>();

        // Create 500 topics with 2000 partitions each

        int partitionsPerTopic = 2000;
        for (int i = 1; i <= 500; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            topicIds.add(topicUuid);
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid, topicName, partitionsPerTopic, Collections.emptyMap()));
        }

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        // Create 2000 consumers and distribute topics among them
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());
        int consumers = 2000;

        for (int i = 1; i <= consumers; i++) {
            String memberName = "consumer" + i;

            // Distribute topics among consumers
            List<Uuid> assignedTopics = new ArrayList<>();
            if ( i == consumers - 1) {
                assignedTopics.add(topicUuids.get(0));
                assignedTopics.add(topicUuids.get(1));
            } else {
                assignedTopics = topicUuids;
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                assignedTopics,
                Collections.emptyMap()));
        }

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        // Update members with the initial assignment and subscribe them to the new topic
        Uuid newTopicUuid = Uuid.randomUuid();
        String newTopicName = "newTopic";
        topicMetadata.put(newTopicUuid, new TopicMetadata(newTopicUuid, newTopicName, 100, Collections.emptyMap()));

        for (Map.Entry<String, MemberAssignment> entry : computedAssignment.members().entrySet()) {
            String memberId = entry.getKey();
            MemberAssignment memberAssignment = entry.getValue();

            Map<Uuid, Set<Integer>> currentAssignment = memberAssignment.targetPartitions();
            List<Uuid> updatedSubscriptions = new ArrayList<>(members.get(memberId).subscribedTopicIds());
            updatedSubscriptions.add(newTopicUuid);

            members.put(memberId, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                updatedSubscriptions,
                currentAssignment
            ));
        }

        // Add 3 new members and subscribe them to all the topics.
        for (int i = 1; i <= 3; i++) {
            String newMemberId = "newMember" + i;
            String rackName = "rack" + (i % 50);
            members.put(newMemberId, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.of(rackName),
                topicIds,
                Collections.emptyMap()
            ));
        }

        // Re-assign with updated members and topics
        AssignmentSpec updatedAssignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata updatedSubscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment rebalancedAssignment = assignor.assign(updatedAssignmentSpec, updatedSubscribedTopicMetadata);
    }

    @Test
    public void testRT1LARGESETFiftyTopicsTwoThousandPartitionsTwoThousandConsumers() {
        // 2k MEMBERS || 2k PARTITIONS || 50 TOPICS || RACK
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();

        // Create 50 topics with a total of 2000 partitions
        int totalPartitions = 2000;
        int partitionsPerTopic = totalPartitions / 50;
        for (int i = 1; i <= 50; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid, topicName, partitionsPerTopic, mkMapOfPartitionRacks(partitionsPerTopic)));
        }

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        // Create 2000 consumers and distribute topics among them
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());
        int consumers = 2000;
        int topicsPerConsumer = 50 / (consumers / topicUuids.size());

        for (int i = 1; i <= consumers; i++) {
            String memberName = "consumer" + i;
            String rackName = "rack" + (i % 50);

            // Distribute topics among consumers
            List<Uuid> assignedTopics = new ArrayList<>();
            for (int j = 0; j < topicsPerConsumer; j++) {
                int topicIndex = (i + j * consumers / topicsPerConsumer) % topicUuids.size();
                assignedTopics.add(topicUuids.get(topicIndex));
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.of(rackName),
                assignedTopics,
                Collections.emptyMap()));
        }

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);
    }

    @Test
    public void testRT1LARGESETFiftyTopicsTwoThousandPartitionsTwoThousandConsumersNoRack() {
        // 2k MEMBERS || 2k PARTITIONS || 50 TOPICS || NO RACK
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();

        // Create 50 topics with a total of 2000 partitions
        int totalPartitions = 2000;
        int partitionsPerTopic = totalPartitions / 50;
        for (int i = 1; i <= 50; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid, topicName, partitionsPerTopic, Collections.emptyMap()));
        }

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        // Create 2000 consumers and distribute topics among them
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());
        int consumers = 2000;
        int topicsPerConsumer = 50 / (consumers / topicUuids.size());

        for (int i = 1; i <= consumers; i++) {
            String memberName = "consumer" + i;
            String rackName = "rack" + (i % 50);

            // Distribute topics among consumers
            List<Uuid> assignedTopics = new ArrayList<>();
            for (int j = 0; j < topicsPerConsumer; j++) {
                int topicIndex = (i + j * consumers / topicsPerConsumer) % topicUuids.size();
                assignedTopics.add(topicUuids.get(topicIndex));
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                assignedTopics,
                Collections.emptyMap()));
        }

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);
    }

    @Test
    public void testRT1LARGESETFiveHundoTopicsTwoThousandPartitionsTwoThousandConsumers() {
        // 2k MEMBERS || 2k PARTITIONS || 500 TOPICS || RACK
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();

        // Create 500 topics with a total of 2000 partitions
        int totalPartitions = 2000;
        int partitionsPerTopic = totalPartitions / 500;
        for (int i = 1; i <= 500; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid, topicName, partitionsPerTopic, mkMapOfPartitionRacks(partitionsPerTopic)));
        }

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        // Create 2000 consumers and distribute topics among them
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());
        int consumers = 2000;
        int topicsPerConsumer = 500 / (consumers / topicUuids.size());

        for (int i = 1; i <= consumers; i++) {
            String memberName = "consumer" + i;
            String rackName = "rack" + (i % 50);

            // Distribute topics among consumers
            List<Uuid> assignedTopics = new ArrayList<>();
            for (int j = 0; j < topicsPerConsumer; j++) {
                int topicIndex = (i + j * consumers / topicsPerConsumer) % topicUuids.size();
                assignedTopics.add(topicUuids.get(topicIndex));
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.of(rackName),
                assignedTopics,
                Collections.emptyMap()));
        }

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);
    }

    @Test
    public void testRT1LARGESETFiveHundoTopicsTwoThousandPartitionsTwoThousandConsumersNoRack() {
        // 2k MEMBERS || 2k PARTITIONS || 500 TOPICS || NO RACK
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();

        // Create 500 topics with a total of 2000 partitions
        int totalPartitions = 2000;
        int partitionsPerTopic = totalPartitions / 500;
        for (int i = 1; i <= 500; i++) {
            Uuid topicUuid = Uuid.randomUuid();
            String topicName = "topic" + i;
            topicMetadata.put(topicUuid, new TopicMetadata(
                topicUuid, topicName, partitionsPerTopic, Collections.emptyMap()));
        }

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        // Create 2000 consumers and distribute topics among them
        List<Uuid> topicUuids = new ArrayList<>(topicMetadata.keySet());
        int consumers = 2000;
        int topicsPerConsumer = 500 / (consumers / topicUuids.size());

        for (int i = 1; i <= consumers; i++) {
            String memberName = "consumer" + i;
            String rackName = "rack" + (i % 50);

            // Distribute topics among consumers
            List<Uuid> assignedTopics = new ArrayList<>();
            for (int j = 0; j < topicsPerConsumer; j++) {
                int topicIndex = (i + j * consumers / topicsPerConsumer) % topicUuids.size();
                assignedTopics.add(topicUuids.get(topicIndex));
            }

            members.put(memberName, new AssignmentMemberSpec(
                Optional.empty(),
                Optional.empty(),
                assignedTopics,
                Collections.emptyMap()));
        }

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);
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
            Arrays.asList(topic2Uuid, topic3Uuid),
            Collections.emptyMap()
        ));
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Collections.singletonList(topic2Uuid),
            Collections.emptyMap()
        ));
        members.put(memberC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic1Uuid),
            Collections.emptyMap()
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0),
            mkTopicAssignment(topic3Uuid, 0, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 1, 2)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2)
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
            Collections.singletonList(topic1Uuid),
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

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1, 2)
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
                mkTopicAssignment(topic1Uuid, 3),
                mkTopicAssignment(topic2Uuid, 0)
            )
        );
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack1"),
            Arrays.asList(topic1Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForC = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 4, 5),
                mkTopicAssignment(topic2Uuid, 1, 2, 3),
                mkTopicAssignment(topic3Uuid, 0, 1, 2, 3)
            )
        );
        members.put(memberC, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid),
            currentAssignmentForC
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

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
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            3,
            mkMapOfPartitionRacks(3)
        ));

        // Initially A was in rack 1 and B was in rack 2, now let's switch them.
        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1),
                mkTopicAssignment(topic3Uuid, 0, 1, 2)
            )
        );
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack2"),
            Arrays.asList(topic1Uuid, topic3Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 0, 1, 2)
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
            mkTopicAssignment(topic3Uuid, 0, 1, 2)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0, 1, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentOnAddingPartitionsWithMemberAndPartitionRacks() {
        // Initially they had 3 partitions each.
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
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            4,
            mkMapOfPartitionRacks(4)
        ));
        topicMetadata.put(topic4Uuid, new TopicMetadata(
            topic4Uuid,
            topic4Name,
            4,
            mkMapOfPartitionRacks(4)
        ));

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1, 2),
                mkTopicAssignment(topic4Uuid, 0, 1, 2)
            )
        );
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack0"),
            Arrays.asList(topic1Uuid, topic4Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic2Uuid, 0, 1, 2),
                mkTopicAssignment(topic3Uuid, 0, 1, 2)
            )
        );
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack1"),
            Arrays.asList(topic3Uuid, topic2Uuid),
            currentAssignmentForB
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2, 3, 4),
            mkTopicAssignment(topic4Uuid, 0, 1, 2, 3)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 1, 2, 3, 4),
            mkTopicAssignment(topic3Uuid, 0, 1, 2, 3)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenPartitionsAreAddedForTwoMembersNoMemberRack() {
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

        Map<String, AssignmentMemberSpec> members = new TreeMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1, 2, 3),
                mkTopicAssignment(topic3Uuid, 0, 1)
            )
        );
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = new TreeMap<>(
            mkAssignment(
                mkTopicAssignment(topic2Uuid, 0, 1, 2),
                mkTopicAssignment(topic4Uuid, 0, 1, 2)
            )
        );
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic2Uuid, topic3Uuid, topic4Uuid),
            currentAssignmentForB
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

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

        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = new TreeMap<>(mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2),
            mkTopicAssignment(topic2Uuid, 0)
        ));
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic1Uuid),
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
    public void testReassignmentOnAddingMemberWithRackAndPartitionRacks() {
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
                mkTopicAssignment(topic1Uuid, 0, 1, 2, 3, 4, 5)
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
                mkTopicAssignment(topic2Uuid, 0, 1, 2, 3, 4)
            )
        );
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.of("rack1"),
            Collections.singletonList(topic2Uuid),
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
            mkTopicAssignment(topic1Uuid, 0, 3, 4)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 1, 3, 4)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1, 2, 5),
            mkTopicAssignment(topic2Uuid, 2)
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

        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2),
            mkTopicAssignment(topic3Uuid, 0, 1)
        );
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Arrays.asList(topic1Uuid, topic3Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = mkAssignment(
            mkTopicAssignment(topic2Uuid, 3, 4, 5, 6)
        );
        members.put(memberB, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic2Uuid),
            currentAssignmentForB
        ));

        // Member C was removed

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

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
        Map<String, AssignmentMemberSpec> members = new HashMap<>();

        Map<Uuid, Set<Integer>> currentAssignmentForA = mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 2),
            mkTopicAssignment(topic2Uuid, 1, 3)
        );
        members.put(memberA, new AssignmentMemberSpec(
            Optional.empty(),
            Optional.empty(),
            Collections.singletonList(topic1Uuid),
            currentAssignmentForA
        ));

        Map<Uuid, Set<Integer>> currentAssignmentForB = mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 0, 2, 4)
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
            mkTopicAssignment(topic1Uuid, 0, 1, 2)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 1, 2, 3, 4)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneSubscriptionRemovedWithMemberAndPartitionRacks() {
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
            2,
            mkMapOfPartitionRacks(2)
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
            Arrays.asList(topic1Uuid, topic2Uuid),
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
            Collections.singletonList(topic2Uuid),
            currentAssignmentForB
        ));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members);
        SubscribedTopicMetadata subscribedTopicMetadata = new SubscribedTopicMetadata(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(assignmentSpec, subscribedTopicMetadata);

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2, 3, 4)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic2Uuid, 0, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }
}
