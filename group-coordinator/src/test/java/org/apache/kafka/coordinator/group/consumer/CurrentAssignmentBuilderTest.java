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
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CurrentAssignmentBuilderTest {

    @Test
    public void testStableToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
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
                .setState(MemberState.STABLE)
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
    public void testStableToStableWithNewPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
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
                .setState(MemberState.STABLE)
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
    public void testStableToUnrevokedPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
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
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3),
                    mkTopicAssignment(topicId2, 5, 6)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(topicId1, 1),
                    mkTopicAssignment(topicId2, 4)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToUnreleasedPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
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
                .setState(MemberState.UNRELEASED_PARTITIONS)
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
    public void testUnrevokedPartitionsToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
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
                .setState(MemberState.STABLE)
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
    public void testRemainsInUnrevokedPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 1),
                mkTopicAssignment(topicId2, 4)))
            .build();

        CurrentAssignmentBuilder currentAssignmentBuilder = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1);

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedTopicPartitions(null)
                .build()
        );

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedTopicPartitions(Arrays.asList(
                    new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(topicId1)
                        .setPartitions(Arrays.asList(1, 2, 3)),
                    new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(topicId2)
                        .setPartitions(Arrays.asList(5, 6))))
                .build()
        );

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedTopicPartitions(Arrays.asList(
                    new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(topicId1)
                        .setPartitions(Arrays.asList(2, 3)),
                    new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(topicId2)
                        .setPartitions(Arrays.asList(4, 5, 6))))
                .build()
        );
    }

    @Test
    public void testUnrevokedPartitionsToUnrevokedPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 1),
                mkTopicAssignment(topicId2, 4)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6))))
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
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 3),
                    mkTopicAssignment(topicId2, 6)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(topicId1, 2),
                    mkTopicAssignment(topicId2, 5)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnrevokedPartitionsToUnreleasedPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_PARTITIONS)
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
                .setState(MemberState.UNRELEASED_PARTITIONS)
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
    public void testUnreleasedPartitionsToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_PARTITIONS)
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
                .setState(MemberState.STABLE)
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
    public void testUnreleasedPartitionsToStableWithNewPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_PARTITIONS)
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
                .setState(MemberState.STABLE)
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
            .setState(MemberState.UNRELEASED_PARTITIONS)
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

    @Test
    public void testUnreleasedPartitionsToUnrevokedPartitions() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 3),
                    mkTopicAssignment(topicId2, 6)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(topicId1, 2),
                    mkTopicAssignment(topicId2, 5)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnknownState() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNKNOWN)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 2),
                mkTopicAssignment(topicId2, 5)))
            .build();

        // When the member is in an unknown state, the member is first to force
        // a reset of the client side member state.
        assertThrows(FencedMemberEpochException.class, () -> new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build());

        // Then the member rejoins with no owned partitions.
        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> 11)
            .withOwnedTopicPartitions(Collections.emptyList())
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 3),
                    mkTopicAssignment(topicId2, 6)))
                .build(),
            updatedMember
        );
    }
}
