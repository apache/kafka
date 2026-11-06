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
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class GroupSpecImplTest {
    private static final String TEST_MEMBER = "test-member";
    private CoordinatorMetadataImage metadataImage;
    private Map<String, MemberSubscriptionAndAssignmentImpl> members;
    private SubscriptionType subscriptionType;
    private GroupSpecImpl groupSpec;
    private Uuid topicId;

    @BeforeEach
    void setUp() {
        topicId = Uuid.randomUuid();
        metadataImage = new MetadataImageBuilder()
            .addTopic(topicId, "topic", 3)
            .buildCoordinatorMetadataImage();
        members = new HashMap<>();
        subscriptionType = SubscriptionType.HOMOGENEOUS;

        members.put(TEST_MEMBER,  new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topicId),
            Assignment.EMPTY
        ));

        groupSpec = new GroupSpecImpl(
            metadataImage,
            members,
            subscriptionType
        );
    }

    @Test
    void testMemberIds() {
        assertEquals(members.keySet(), groupSpec.memberIds());
    }

    @Test
    void testSubscriptionType() {
        assertEquals(subscriptionType, groupSpec.subscriptionType());
    }

    @Test
    void testIsPartitionAssigned() {
        Uuid deletedTopicId = Uuid.randomUuid();
        members.put(TEST_MEMBER, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topicId, deletedTopicId),
            new Assignment(Map.of(
                // Partitions -1 and 3 do not exist.
                topicId, Set.of(-1, 0, 2, 3),
                // The deleted topic is not in the metadata image, so none of its partitions exist.
                deletedTopicId, Set.of(0)
            ))
        ));

        assertFalse(groupSpec.isPartitionAssigned(topicId, -1));
        assertTrue(groupSpec.isPartitionAssigned(topicId, 0));
        assertFalse(groupSpec.isPartitionAssigned(topicId, 1));
        assertTrue(groupSpec.isPartitionAssigned(topicId, 2));
        assertFalse(groupSpec.isPartitionAssigned(topicId, 3));
        assertFalse(groupSpec.isPartitionAssigned(deletedTopicId, 0));
    }

    @Test
    void testMemberSubscription() {
        assertEquals(members.get(TEST_MEMBER), groupSpec.memberSubscription(TEST_MEMBER));
        assertThrows(IllegalArgumentException.class, () -> groupSpec.memberSubscription("unknown-member"));
    }

    @Test
    void testMemberAssignment() {
        Map<Uuid, Set<Integer>> topicPartitions = new HashMap<>();
        topicPartitions.put(
            topicId,
            Set.of(0, 1)
        );
        members.put(TEST_MEMBER, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            Optional.empty(),
            Set.of(topicId),
            new Assignment(topicPartitions)
        ));

        assertEquals(topicPartitions, groupSpec.memberAssignment(TEST_MEMBER).partitions());
        assertEquals(Map.of(), groupSpec.memberAssignment("unknown-member").partitions());
    }

    @Test
    void testIsPartitionAssignable() {
        // Empty allowed map.
        assertTrue(groupSpec.isPartitionAssignable(topicId, 1));

        // Allowed map with data.
        groupSpec = new GroupSpecImpl(
            metadataImage,
            members,
            subscriptionType,
            Optional.of(Map.of(topicId, Set.of(0)))
        );

        assertTrue(groupSpec.isPartitionAssignable(topicId, 0));
        assertFalse(groupSpec.isPartitionAssignable(topicId, 1));
    }
}
