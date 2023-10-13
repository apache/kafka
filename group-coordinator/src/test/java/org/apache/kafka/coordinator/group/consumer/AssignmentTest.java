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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssignmentTest {

    @Test
    public void testPartitionsAndMetadataCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new Assignment(
            (byte) 1,
            null,
            new VersionedMetadata(
                (short) 1,
                ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
            )
        ));

        assertThrows(NullPointerException.class, () -> new Assignment(
            (byte) 1,
            mkAssignment(
                mkTopicAssignment(Uuid.randomUuid(), 1, 2, 3)
            ),
            null
        ));
    }

    @Test
    public void testAttributes() {
        Map<Uuid, Set<Integer>> partitions = mkAssignment(
            mkTopicAssignment(Uuid.randomUuid(), 1, 2, 3)
        );

        VersionedMetadata metadata = new VersionedMetadata(
            (short) 1,
            ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
        );

        Assignment assignment = new Assignment(
            (byte) 1,
            partitions,
            metadata
        );

        assertEquals((byte) 1, assignment.error());
        assertEquals(partitions, assignment.partitions());
        assertEquals(metadata, assignment.metadata());
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
            .setError((byte) 1)
            .setTopicPartitions(partitions)
            .setMetadataVersion((short) 2)
            .setMetadataBytes("foo".getBytes(StandardCharsets.UTF_8));

        Assignment assignment = Assignment.fromRecord(record);

        assertEquals((short) 1, assignment.error());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ), assignment.partitions());
        assertEquals(new VersionedMetadata(
            (short) 2,
            ByteBuffer.wrap("foo".getBytes(StandardCharsets.UTF_8))
        ), assignment.metadata());
    }

    @Test
    public void testEquals() {
        Map<Uuid, Set<Integer>> partitions = mkAssignment(
            mkTopicAssignment(Uuid.randomUuid(), 1, 2, 3)
        );

        VersionedMetadata metadata = new VersionedMetadata(
            (short) 1,
            ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
        );

        Assignment assignment = new Assignment(
            (byte) 1,
            partitions,
            metadata
        );

        assertEquals(new Assignment(
            (byte) 1,
            partitions,
            metadata
        ), assignment);

        assertNotEquals(new Assignment(
            (byte) 1,
            Collections.emptyMap(),
            metadata
        ), assignment);
    }
}
