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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class UniformAssignorTest {
    private final UniformAssignor assignor = new UniformAssignor();

    private final String topic1Name = "topic1";
    private final Uuid topic1Uuid = Uuid.randomUuid();

    private final String topic2Name = "topic2";
    private final Uuid topic2Uuid = Uuid.randomUuid();

    private final String topic3Name = "topic3";
    private final Uuid topic3Uuid = Uuid.randomUuid();

    private final String consumerA = "A";
    private final String consumerB = "B";
    private final String consumerC = "C";

    @Test
    public void testOneConsumerNoTopicSubscription() {
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 3));
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        List<Uuid> subscribedTopics = new ArrayList<>();
        members.computeIfAbsent(consumerA, k -> new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopics, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment groupAssignment = assignor.assign(assignmentSpec);

        assertTrue(groupAssignment.getMembers().isEmpty());
    }

    @Test
    public void testFirstAssignmentTwoConsumersTwoTopics() {
        // A -> T1, T3 // B -> T1, T3 // T1 -> 3 Partitions // T3 -> 2 Partitions
        // Topics
        Map<Uuid, AssignmentTopicMetadata> topics = new HashMap<>();
        topics.put(topic1Uuid, new AssignmentTopicMetadata(topic1Name, 3));
        topics.put(topic3Uuid, new AssignmentTopicMetadata(topic3Name, 2));
        // Members
        Map<String, AssignmentMemberSpec> members = new HashMap<>();
        // Consumer A
        List<Uuid> subscribedTopicsA = new ArrayList<>(Arrays.asList(topic1Uuid, topic3Uuid));
        members.put(consumerA, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsA, new HashMap<>()));
        // Consumer B
        List<Uuid> subscribedTopicsB = new ArrayList<>(Arrays.asList(topic1Uuid, topic3Uuid));
        members.put(consumerB, new AssignmentMemberSpec(Optional.empty(), Optional.empty(), subscribedTopicsB, new HashMap<>()));

        AssignmentSpec assignmentSpec = new AssignmentSpec(members, topics);
        GroupAssignment computedAssignment = assignor.assign(assignmentSpec);

        Map<Uuid, Set<Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 1 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Arrays.asList(0, 1)));
        expectedAssignment.computeIfAbsent(topic1Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(2)));
        // Topic 3 Partitions Assignment
        expectedAssignment.computeIfAbsent(topic3Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(0)));
        expectedAssignment.computeIfAbsent(topic3Uuid, k -> new HashSet<>()).add(new HashSet<>(Collections.singletonList(1)));

        //assertAssignment(expectedAssignment, computedAssignment);
    }

}
