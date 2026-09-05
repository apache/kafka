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
import org.apache.kafka.server.common.TopicIdPartition;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AssignorHelpersTest {
    private static final Uuid TOPIC_ID = Uuid.randomUuid();

    @Test
    void testIsRackMatchWithEmptyMemberRackId() {
        assertFalse(AssignorHelpers.isRackMatch(Optional.empty(), Set.of("rack1", "rack2")));
    }

    @Test
    void testIsRackMatchWithMatchingRack() {
        assertTrue(AssignorHelpers.isRackMatch(Optional.of("rack1"), Set.of("rack1", "rack2")));
    }

    @Test
    void testIsRackMatchWithNonMatchingRack() {
        assertFalse(AssignorHelpers.isRackMatch(Optional.of("rack3"), Set.of("rack1", "rack2")));
    }

    @Test
    void testIsRackMatchWithEmptyPartitionRacks() {
        assertFalse(AssignorHelpers.isRackMatch(Optional.of("rack1"), Set.of()));
    }

    @Test
    void testUseRackAwareAssignmentWithEmptyMemberRacks() {
        Set<String> allMemberRacks = Set.of();
        Set<String> allPartitionRacks = Set.of("rack1", "rack2");
        Map<TopicIdPartition, Set<String>> racksPerPartition = Map.of(
            new TopicIdPartition(TOPIC_ID, 0), Set.of("rack1"),
            new TopicIdPartition(TOPIC_ID, 1), Set.of("rack2")
        );

        assertFalse(AssignorHelpers.useRackAwareAssignment(allMemberRacks, allPartitionRacks, racksPerPartition));
    }

    @Test
    void testUseRackAwareAssignmentWithDisjointRacks() {
        Set<String> allMemberRacks = Set.of("rack1", "rack2");
        Set<String> allPartitionRacks = Set.of("rack3", "rack4");
        Map<TopicIdPartition, Set<String>> racksPerPartition = Map.of(
            new TopicIdPartition(TOPIC_ID, 0), Set.of("rack3"),
            new TopicIdPartition(TOPIC_ID, 1), Set.of("rack4")
        );

        assertFalse(AssignorHelpers.useRackAwareAssignment(allMemberRacks, allPartitionRacks, racksPerPartition));
    }

    @Test
    void testUseRackAwareAssignmentWithAllPartitionsHavingSameRacks() {
        Set<String> allMemberRacks = Set.of("rack1", "rack2");
        Set<String> allPartitionRacks = Set.of("rack1", "rack2");
        Map<TopicIdPartition, Set<String>> racksPerPartition = Map.of(
            new TopicIdPartition(TOPIC_ID, 0), Set.of("rack1", "rack2"),
            new TopicIdPartition(TOPIC_ID, 1), Set.of("rack1", "rack2")
        );

        assertFalse(AssignorHelpers.useRackAwareAssignment(allMemberRacks, allPartitionRacks, racksPerPartition));
    }

    @Test
    void testUseRackAwareAssignmentWithDifferentRacksPerPartition() {
        Set<String> allMemberRacks = Set.of("rack1", "rack2");
        Set<String> allPartitionRacks = Set.of("rack1", "rack2");
        Map<TopicIdPartition, Set<String>> racksPerPartition = Map.of(
            new TopicIdPartition(TOPIC_ID, 0), Set.of("rack1"),
            new TopicIdPartition(TOPIC_ID, 1), Set.of("rack2")
        );

        assertTrue(AssignorHelpers.useRackAwareAssignment(allMemberRacks, allPartitionRacks, racksPerPartition));
    }
}
