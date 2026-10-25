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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.AssignorHelpers;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssignmentTest {

    @Test
    public void testPartitionsCannotBeNull() {
        assertThrows(NullPointerException.class, () -> Assignment.of(null));
        assertThrows(NullPointerException.class, () -> Assignment.copyOf(null));
    }

    @Test
    public void testCopyOfIsolatedFromSource() {
        Uuid topicId = Uuid.randomUuid();
        Set<Integer> partitionIds = new HashSet<>(Set.of(0, 1));

        Map<Uuid, Set<Integer>> partitions = new HashMap<>();
        partitions.put(topicId, partitionIds);

        Assignment assignment = Assignment.copyOf(partitions);

        Map<Uuid, Set<Integer>> expectedPartitions = Map.of(topicId, Set.of(0, 1));
        partitionIds.remove(0);
        assertEquals(expectedPartitions, assignment.partitions());

        partitions.clear();
        assertEquals(expectedPartitions, assignment.partitions());
    }

    @Test
    public void testOfWrapsMapAndReusesPartitionSets() {
        Uuid topicId = Uuid.randomUuid();

        HashMap<Uuid, Set<Integer>> partitionsWithMutable = new HashMap<>();
        HashMap<Uuid, Set<Integer>> partitionsWithImmutable = new HashMap<>();

        partitionsWithMutable.put(topicId, new HashSet<>(Set.of(0, 1)));
        partitionsWithImmutable.put(topicId, Set.of(0, 1));

        Assignment assignmentWithMutableSet = Assignment.of(partitionsWithMutable);
        Assignment assignmentWithImmutableSet = Assignment.of(partitionsWithImmutable);

        assertSame(partitionsWithMutable.get(topicId), assignmentWithMutableSet.partitions().get(topicId));
        assertTrue(AssignorHelpers.isImmutableMap(assignmentWithMutableSet.partitions()));
        assertThrows(UnsupportedOperationException.class, () -> assignmentWithMutableSet.partitions().clear());
        assertDoesNotThrow(() -> assignmentWithMutableSet.partitions().get(topicId).clear());

        assertSame(partitionsWithImmutable.get(topicId), assignmentWithImmutableSet.partitions().get(topicId));
        assertTrue(AssignorHelpers.isImmutableMap(assignmentWithImmutableSet.partitions()));
        assertThrows(UnsupportedOperationException.class, () -> assignmentWithImmutableSet.partitions().clear());
    }

    @Test
    public void testCopyOfPartitionsImmutable() {
        Uuid topicId = Uuid.randomUuid();

        HashMap<Uuid, Set<Integer>> partitionsWithMutable = new HashMap<>();
        HashMap<Uuid, Set<Integer>> partitionsWithImmutable = new HashMap<>();

        partitionsWithMutable.put(topicId, new HashSet<>(Set.of(0, 1)));
        partitionsWithImmutable.put(topicId, Set.of(0, 1));

        Assignment assignmentWithMutableSet = Assignment.copyOf(partitionsWithMutable);
        Assignment assignmentWithImmutableSet = Assignment.copyOf(partitionsWithImmutable);

        assertEquals(partitionsWithMutable, assignmentWithMutableSet.partitions());
        assertTrue(AssignorHelpers.isImmutableMap(assignmentWithMutableSet.partitions()));
        assertThrows(UnsupportedOperationException.class, () -> assignmentWithMutableSet.partitions().clear());
        assertThrows(UnsupportedOperationException.class, () -> assignmentWithMutableSet.partitions().get(topicId).clear());

        assertEquals(partitionsWithImmutable, assignmentWithImmutableSet.partitions());
        assertTrue(AssignorHelpers.isImmutableMap(assignmentWithImmutableSet.partitions()));
        assertThrows(UnsupportedOperationException.class, () -> assignmentWithImmutableSet.partitions().clear());
        assertThrows(UnsupportedOperationException.class, () -> assignmentWithImmutableSet.partitions().get(topicId).clear());
    }

    @Test
    public void testAttributes() {
        Map<Uuid, Set<Integer>> partitions = mkAssignment(
            mkTopicAssignment(Uuid.randomUuid(), 1, 2, 3)
        );
        Assignment assignment = Assignment.copyOf(partitions);
        assertEquals(partitions, assignment.partitions());
    }

    @Test
    public void testFromTargetAssignmentRecord() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        List<ConsumerGroupTargetAssignmentMemberValue.TopicPartition> partitions = new ArrayList<>();
        partitions.add(new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
            .setTopicId(topicId1)
            .setPartitions(Arrays.asList(1, 2, 3)));
        partitions.add(new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
            .setTopicId(topicId2)
            .setPartitions(Arrays.asList(4, 5, 6)));

        ConsumerGroupTargetAssignmentMemberValue record = new ConsumerGroupTargetAssignmentMemberValue()
            .setTopicPartitions(partitions);

        Assignment assignment = Assignment.fromRecord(record);

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ), assignment.partitions());
    }

    @Test
    public void testEquals() {
        Map<Uuid, Set<Integer>> partitions = mkAssignment(
            mkTopicAssignment(Uuid.randomUuid(), 1, 2, 3)
        );

        assertEquals(Assignment.copyOf(partitions), Assignment.copyOf(partitions));
    }
}
