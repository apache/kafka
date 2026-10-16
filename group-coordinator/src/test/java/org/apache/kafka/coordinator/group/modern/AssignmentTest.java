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
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssignmentTest {

    @Test
    public void testPartitionsCannotBeNull() {
        assertThrows(NullPointerException.class, () -> Assignment.of(null));
        assertThrows(NullPointerException.class, () -> Assignment.copyOf(null));
    }

    @Test
    public void testCopyOfPartitionsImmutable() {
        Uuid topicId = Uuid.randomUuid();
        Map<Uuid, Set<Integer>> partitions = new HashMap<>();
        partitions.put(topicId, new HashSet<>(Set.of(0, 1)));

        Assignment assignment = Assignment.copyOf(partitions);

        assertThrows(UnsupportedOperationException.class, () -> assignment.partitions().clear());
        assertThrows(UnsupportedOperationException.class, () -> assignment.partitions().get(topicId).remove(0));
        assertEquals(Map.of(topicId, Set.of(0, 1)), partitions);
    }

    @Test
    public void testCopyOfIsolatedFromSourceSet() {
        Uuid topicId = Uuid.randomUuid();
        Set<Integer> partitionIds = new HashSet<>(Set.of(0, 1));
        Map<Uuid, Set<Integer>> partitions = new HashMap<>();
        partitions.put(topicId, partitionIds);

        Assignment assignment = Assignment.copyOf(partitions);

        partitionIds.remove(0);

        assertEquals(Map.of(topicId, Set.of(0, 1)), assignment.partitions());
    }

    @Test
    public void testCopyOfIsolatedFromSourceMap() {
        Uuid topicId = Uuid.randomUuid();
        Map<Uuid, Set<Integer>> partitions = new HashMap<>();
        partitions.put(topicId, new HashSet<>(Set.of(0, 1)));

        Assignment assignment = Assignment.copyOf(partitions);

        partitions.clear();

        assertEquals(Map.of(topicId, Set.of(0, 1)), assignment.partitions());
    }

    @Test
    public void testOfReusesImmutablePartitionSets() {
        Uuid topicId = Uuid.randomUuid();
        Map<Uuid, Set<Integer>> partitions = mkAssignment(mkTopicAssignment(topicId, 0, 1));

        Assignment assignment = Assignment.of(partitions);

        assertEquals(partitions, assignment.partitions());
        assertSame(partitions.get(topicId), assignment.partitions().get(topicId));
        assertThrows(UnsupportedOperationException.class, () -> assignment.partitions().clear());
        assertThrows(UnsupportedOperationException.class, () -> assignment.partitions().get(topicId).remove(0));
    }

    @Test
    public void testAttributes() {
        Map<Uuid, Set<Integer>> partitions = mkAssignment(
            mkTopicAssignment(Uuid.randomUuid(), 1, 2, 3)
        );
        Assignment assignment = Assignment.copyOf(partitions);
        assertEquals(partitions, assignment.partitions());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFromTargetAssignmentRecord(boolean shareGroup) {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        List<Integer> partitionIds1 = new ArrayList<>(List.of(1, 2, 3));
        List<Integer> partitionIds2 = new ArrayList<>(List.of(4, 5, 6));

        Assignment assignment;
        if (shareGroup) {
            ShareGroupTargetAssignmentMemberValue record = new ShareGroupTargetAssignmentMemberValue()
                .setTopicPartitions(new ArrayList<>(List.of(
                    new ShareGroupTargetAssignmentMemberValue.TopicPartition()
                        .setTopicId(topicId1)
                        .setPartitions(partitionIds1),
                    new ShareGroupTargetAssignmentMemberValue.TopicPartition()
                        .setTopicId(topicId2)
                        .setPartitions(partitionIds2)
                )));
            assignment = Assignment.fromRecord(record);
            record.topicPartitions().clear();
        } else {
            ConsumerGroupTargetAssignmentMemberValue record = new ConsumerGroupTargetAssignmentMemberValue()
                .setTopicPartitions(new ArrayList<>(List.of(
                    new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                        .setTopicId(topicId1)
                        .setPartitions(partitionIds1),
                    new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                        .setTopicId(topicId2)
                        .setPartitions(partitionIds2)
                )));
            assignment = Assignment.fromRecord(record);
            record.topicPartitions().clear();
        }
        partitionIds1.clear();
        partitionIds2.clear();

        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ), assignment.partitions());
        assertThrows(UnsupportedOperationException.class, () -> assignment.partitions().clear());
        assertThrows(UnsupportedOperationException.class, () -> assignment.partitions().get(topicId1).remove(1));
        assertThrows(UnsupportedOperationException.class, () -> assignment.partitions().get(topicId2).remove(4));
    }

    @Test
    public void testEquals() {
        Map<Uuid, Set<Integer>> partitions = mkAssignment(
            mkTopicAssignment(Uuid.randomUuid(), 1, 2, 3)
        );

        assertEquals(Assignment.copyOf(partitions), Assignment.copyOf(partitions));
    }
}
