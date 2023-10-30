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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CurrentAssignmentBuilderTest {

    @Test
    public void testStableToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(ConsumerGroupMember.MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3),
                    mkTopicAssignment(topicId2, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToUnacknowledgedAssignmentWithPartitionsToRevoke() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId2, 5, 6, 7))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(ConsumerGroupMember.MemberState.UNACKNOWLEDGED_ASSIGNMENT)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3),
                    mkTopicAssignment(topicId2, 5, 6)))
                .setRevokedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 1),
                    mkTopicAssignment(topicId2, 4)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToUnacknowledgedAssignmentWithNoPartitionsToRevoke() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3, 4),
                mkTopicAssignment(topicId2, 4, 5, 6, 7))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(ConsumerGroupMember.MemberState.UNACKNOWLEDGED_ASSIGNMENT)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3, 4),
                    mkTopicAssignment(topicId2, 4, 5, 6, 7)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToUnreleasedPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3, 4),
                mkTopicAssignment(topicId2, 4, 5, 6, 7))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(ConsumerGroupMember.MemberState.UNRELEASED_PARTITIONS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3),
                    mkTopicAssignment(topicId2, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testRemainsInUnacknowledgedAssignmentUntilAcknowledgementIsReceived() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.UNACKNOWLEDGED_ASSIGNMENT)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        assertEquals(
            member,
            new CurrentAssignmentBuilder(member)
                .withTargetAssignment(11, new Assignment(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3),
                    mkTopicAssignment(topicId2, 4, 5, 6))))
                .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
                .withOwnedTopicPartitions(null)
                .build()
        );

        assertEquals(
            member,
            new CurrentAssignmentBuilder(member)
                .withTargetAssignment(11, new Assignment(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3),
                    mkTopicAssignment(topicId2, 4, 5, 6))))
                .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
                .withOwnedTopicPartitions(Collections.emptyList())
                .build()
        );

        assertEquals(
            member,
            new CurrentAssignmentBuilder(member)
                .withTargetAssignment(11, new Assignment(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3),
                    mkTopicAssignment(topicId2, 4, 5, 6))))
                .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
                .withOwnedTopicPartitions(Collections.singletonList(
                    new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(topicId1)
                        .setPartitions(Arrays.asList(1, 2, 3))))
                .build()
        );
    }

    @Test
    public void testUnacknowledgedAssignmentToStableWithRevokedPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.UNACKNOWLEDGED_ASSIGNMENT)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .setRevokedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1),
                mkTopicAssignment(topicId2, 4)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId1)
                    .setPartitions(Arrays.asList(2, 3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId2)
                    .setPartitions(Arrays.asList(5, 6))))
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(ConsumerGroupMember.MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3),
                    mkTopicAssignment(topicId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnacknowledgedAssignmentToStableWithoutRevokedPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.UNACKNOWLEDGED_ASSIGNMENT)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId1)
                    .setPartitions(Arrays.asList(2, 3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId2)
                    .setPartitions(Arrays.asList(5, 6))))
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(ConsumerGroupMember.MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3),
                    mkTopicAssignment(topicId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnacknowledgedAssignmentToUnacknowledgedAssignment() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.UNACKNOWLEDGED_ASSIGNMENT)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId2, 5, 6, 7))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId1)
                    .setPartitions(Arrays.asList(2, 3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId2)
                    .setPartitions(Arrays.asList(5, 6))))
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(ConsumerGroupMember.MemberState.UNACKNOWLEDGED_ASSIGNMENT)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3, 4),
                    mkTopicAssignment(topicId2, 5, 6, 7)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnacknowledgedAssignmentToUnreleasedPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.UNACKNOWLEDGED_ASSIGNMENT)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId2, 5, 6, 7))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .withOwnedTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId1)
                    .setPartitions(Arrays.asList(2, 3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId2)
                    .setPartitions(Arrays.asList(5, 6))))
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(ConsumerGroupMember.MemberState.UNRELEASED_PARTITIONS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3),
                    mkTopicAssignment(topicId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedPartitionsToStabled() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(ConsumerGroupMember.MemberState.STABLE)
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3),
                    mkTopicAssignment(topicId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedPartitionsToUnacknowledgedAssignment() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId2, 5, 6, 7))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(ConsumerGroupMember.MemberState.UNACKNOWLEDGED_ASSIGNMENT)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3, 4),
                    mkTopicAssignment(topicId2, 5, 6, 7)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedPartitionsToUnreleasedPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(ConsumerGroupMember.MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId2, 5, 6, 7))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build();

        assertEquals(member, updatedMember);
    }
}
