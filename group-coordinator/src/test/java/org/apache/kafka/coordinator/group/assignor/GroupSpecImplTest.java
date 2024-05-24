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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class GroupSpecImplTest {

    private Map<String, MemberSubscriptionSpec> members;
    private SubscriptionType subscriptionType;
    private Map<String, Map<Uuid, Set<Integer>>> assignedPartitions;
    private Map<Uuid, Map<Integer, String>> invertedTargetAssignment;
    private GroupSpecImpl groupSpec;
    private Uuid topicId;
    private String testMember;

    @BeforeEach
    void setUp() {
        members = new HashMap<>();
        subscriptionType = SubscriptionType.HOMOGENEOUS;
        assignedPartitions = new HashMap<>();
        invertedTargetAssignment = new HashMap<>();
        topicId = Uuid.randomUuid();

        members.put(testMember,  new MemberSubscriptionSpecImpl(
            Optional.empty(),
            new HashSet<>(Collections.singletonList(topicId))
        ));

        groupSpec = new GroupSpecImpl(
            members,
            subscriptionType,
            assignedPartitions,
            invertedTargetAssignment
        );
    }

    @Test
    void testMembers() {
        assertEquals(members, groupSpec.memberSubscriptions());
    }

    @Test
    void testSubscriptionType() {
        assertEquals(subscriptionType, groupSpec.subscriptionType());
    }

    @Test
    void testCurrentMemberAssignment() {
        Map<Uuid, Set<Integer>> topicPartitions = new HashMap<>();
        topicPartitions.put(
            topicId,
            new HashSet<>(Arrays.asList(0, 1))
        );
        assignedPartitions.put("test-member", topicPartitions);

        assertEquals(topicPartitions, groupSpec.currentMemberAssignment("test-member"));
        assertEquals(Collections.emptyMap(), groupSpec.currentMemberAssignment("unknown-member"));
    }

    @Test
    void testIsPartitionAssigned() {
        Map<Integer, String> partitionMap = new HashMap<>();
        partitionMap.put(1, "test-member");
        invertedTargetAssignment.put(topicId, partitionMap);

        assertTrue(groupSpec.isPartitionAssigned(topicId, 1));
        assertFalse(groupSpec.isPartitionAssigned(topicId, 2));
        assertFalse(groupSpec.isPartitionAssigned(Uuid.randomUuid(), 2));
    }
}
