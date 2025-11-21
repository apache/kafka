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
package org.apache.kafka.coordinator.group.modern.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CurrentAssignmentBuilderTest {

    @Test
    public void testStableToStable() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
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
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3),
                    mkTopicAssignment(topicId2, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToStableWithNewPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
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
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3, 4),
                    mkTopicAssignment(topicId2, 4, 5, 6, 7)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToUnrevokedPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
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
                .setSubscribedTopicNames(List.of(topic1, topic2))
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
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
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
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3),
                    mkTopicAssignment(topicId2, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToUnreleasedPartitionsWithOwnedPartitionsNotHavingRevokedPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 7))))
            .withCurrentPartitionEpoch((topicId, __) ->
                topicId2.equals(topicId) ? 10 : -1
            )
            .withOwnedTopicPartitions(List.of())
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(MemberState.UNRELEASED_PARTITIONS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3),
                    mkTopicAssignment(topicId2, 4, 5)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnrevokedPartitionsToStable() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 1),
                mkTopicAssignment(topicId2, 4)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
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
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3),
                    mkTopicAssignment(topicId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testRemainsInUnrevokedPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 1),
                mkTopicAssignment(topicId2, 4)))
            .build();

        CurrentAssignmentBuilder currentAssignmentBuilder = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
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

    @ParameterizedTest
    @CsvSource({
        "10, 12, 11",
        "10, 10, 10", // The member epoch must not advance past the target assignment epoch.
    })
    public void testUnrevokedPartitionsToUnrevokedPartitions(int memberEpoch, int targetAssignmentEpoch, int expectedMemberEpoch) {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 1),
                mkTopicAssignment(topicId2, 4)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(targetAssignmentEpoch, new Assignment(mkAssignment(
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
                .setMemberEpoch(expectedMemberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setSubscribedTopicNames(List.of(topic1, topic2))
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
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
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
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3),
                    mkTopicAssignment(topicId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnrevokedPartitionsToStableWithReturnedPartitionsPendingRevocation() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                // Partition 4 is pending revocation by the member but is back in the latest target
                // assignment.
                mkTopicAssignment(topicId1, 4)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId2, 5, 6, 7))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> {
                if (topicId.equals(topicId1)) {
                    // Partitions 2 and 3 are in the member's current assignment.
                    // Partition 4 is pending revocation by the member.
                    switch (partitionId) {
                        case 2:
                        case 3:
                        case 4:
                            return 10;
                    }
                } else if (topicId.equals(topicId2)) {
                    // Partitions 5 and 6 are in the member's current assignment.
                    switch (partitionId) {
                        case 5:
                        case 6:
                            return 10;
                    }
                }
                return -1;
            })
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
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(10)
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3, 4),
                    mkTopicAssignment(topicId2, 5, 6, 7)))
                .setPartitionsPendingRevocation(Map.of())
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedPartitionsToStable() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
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
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3),
                    mkTopicAssignment(topicId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedPartitionsToStableWithNewPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
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
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 2, 3, 4),
                    mkTopicAssignment(topicId2, 5, 6, 7)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedPartitionsToUnreleasedPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3, 4),
                mkTopicAssignment(topicId2, 5, 6, 7))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build();

        assertEquals(member, updatedMember);
    }

    @Test
    public void testUnreleasedPartitionsToUnrevokedPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
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
                .setSubscribedTopicNames(List.of(topic1, topic2))
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
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNKNOWN)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setSubscribedTopicNames(List.of(topic1, topic2))
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
            .withMetadataImage(metadataImage)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build());

        // Then the member rejoins with no owned partitions.
        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6))))
            .withCurrentPartitionEpoch((topicId, partitionId) -> 11)
            .withOwnedTopicPartitions(List.of())
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(11)
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 3),
                    mkTopicAssignment(topicId2, 6)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @CsvSource({
        "10, 11, 11, false", // When advancing to a new target assignment, the assignment should
        "10, 11, 11, true",  // always take the subscription into account.
        "10, 10, 10, true",
    })
    public void testStableToStableWithAssignmentTopicsNoLongerInSubscription(
        int memberEpoch,
        int targetAssignmentEpoch,
        int expectedMemberEpoch,
        boolean hasSubscriptionChanged
    ) {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setSubscribedTopicNames(List.of(topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(targetAssignmentEpoch, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6))))
            .withHasSubscriptionChanged(hasSubscriptionChanged)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId2)
                    .setPartitions(Arrays.asList(4, 5, 6))))
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(expectedMemberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setSubscribedTopicNames(List.of(topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId2, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @CsvSource({
        "10, 11, 10, false", // When advancing to a new target assignment, the assignment should always
        "10, 11, 10, true",  // take the subscription into account.
        "10, 10, 10, true"
    })
    public void testStableToUnrevokedPartitionsWithAssignmentTopicsNoLongerInSubscription(
        int memberEpoch,
        int targetAssignmentEpoch,
        int expectedMemberEpoch,
        boolean hasSubscriptionChanged
    ) {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setSubscribedTopicNames(List.of(topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(targetAssignmentEpoch, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6))))
            .withHasSubscriptionChanged(hasSubscriptionChanged)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId1)
                    .setPartitions(Arrays.asList(1, 2, 3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId2)
                    .setPartitions(Arrays.asList(4, 5, 6))))
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(expectedMemberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setSubscribedTopicNames(List.of(topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId2, 4, 5, 6)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testRemainsInUnrevokedPartitionsWithAssignmentTopicsNoLongerInSubscription() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 2, 3),
                mkTopicAssignment(topicId2, 5, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 1),
                mkTopicAssignment(topicId2, 4)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 1, 3, 4),
                mkTopicAssignment(topicId2, 6, 7))))
            .withHasSubscriptionChanged(true)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId1)
                    .setPartitions(Arrays.asList(1, 2, 3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(topicId2)
                    .setPartitions(Arrays.asList(4, 5, 6))))
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setSubscribedTopicNames(List.of(topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId2, 5, 6)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3),
                    mkTopicAssignment(topicId2, 4)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testSubscribedTopicNameAndUnresolvedRegularExpression() {
        String fooTopic = "foo";
        String barTopic = "bar";
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopic, 10)
            .addTopic(barTopicId, barTopic, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(fooTopic))
            .setSubscribedTopicRegex("bar*")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3),
                mkTopicAssignment(barTopicId, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(10, new Assignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3),
                mkTopicAssignment(barTopicId, 4, 5, 6))))
            .withHasSubscriptionChanged(true)
            .withResolvedRegularExpressions(Map.of())
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(Arrays.asList(1, 2, 3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(Arrays.asList(4, 5, 6))))
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setSubscribedTopicNames(List.of(fooTopic))
                .setSubscribedTopicRegex("bar*")
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 1, 2, 3)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(barTopicId, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnresolvedRegularExpression() {
        String fooTopic = "foo";
        String barTopic = "bar";
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopic, 10)
            .addTopic(barTopicId, barTopic, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of())
            .setSubscribedTopicRegex("bar*")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3),
                mkTopicAssignment(barTopicId, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(10, new Assignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3),
                mkTopicAssignment(barTopicId, 4, 5, 6))))
            .withHasSubscriptionChanged(true)
            .withResolvedRegularExpressions(Map.of())
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(Arrays.asList(1, 2, 3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(Arrays.asList(4, 5, 6))))
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setSubscribedTopicNames(List.of())
                .setSubscribedTopicRegex("bar*")
                .setAssignedPartitions(mkAssignment())
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(fooTopicId, 1, 2, 3),
                    mkTopicAssignment(barTopicId, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testSubscribedTopicNameAndResolvedRegularExpression() {
        String fooTopic = "foo";
        String barTopic = "bar";
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopic, 10)
            .addTopic(barTopicId, barTopic, 10)
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(fooTopic))
            .setSubscribedTopicRegex("bar*")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3),
                mkTopicAssignment(barTopicId, 4, 5, 6)))
            .build();

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(10, new Assignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3),
                mkTopicAssignment(barTopicId, 4, 5, 6))))
            .withHasSubscriptionChanged(true)
            .withResolvedRegularExpressions(Map.of(
                "bar*", new ResolvedRegularExpression(
                    Set.of("bar"),
                    12345L,
                    0L
                )
            ))
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(Arrays.asList(1, 2, 3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(Arrays.asList(4, 5, 6))))
            .build();

        assertEquals(
            new ConsumerGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setSubscribedTopicNames(List.of(fooTopic))
                .setSubscribedTopicRegex("bar*")
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 1, 2, 3),
                    mkTopicAssignment(barTopicId, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }
}
