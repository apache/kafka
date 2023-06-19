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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CurrentAssignmentBuilderTest {

    @Test
    public void testTransitionFromNewTargetToRevoke() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, member.state());

        Assignment targetAssignment = new Assignment(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(10, updatedMember.memberEpoch());
        assertEquals(11, updatedMember.targetMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3),
            mkTopicAssignment(topicId2, 6)
        ), updatedMember.assignedPartitions());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2),
            mkTopicAssignment(topicId2, 4, 5)
        ), updatedMember.partitionsPendingRevocation());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 4, 5),
            mkTopicAssignment(topicId2, 7, 8)
        ), updatedMember.partitionsPendingAssignment());
    }

    @Test
    public void testTransitionFromNewTargetToAssigning() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, member.state());

        Assignment targetAssignment = new Assignment(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3, 4, 5),
            mkTopicAssignment(topicId2, 4, 5, 6, 7, 8)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build();

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(11, updatedMember.memberEpoch());
        assertEquals(11, updatedMember.targetMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ), updatedMember.assignedPartitions());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingRevocation());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 4, 5),
            mkTopicAssignment(topicId2, 7, 8)
        ), updatedMember.partitionsPendingAssignment());
    }

    @Test
    public void testTransitionFromNewTargetToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, member.state());

        Assignment targetAssignment = new Assignment(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(11, updatedMember.memberEpoch());
        assertEquals(11, updatedMember.targetMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ), updatedMember.assignedPartitions());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingRevocation());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingAssignment());
    }

    private static Stream<Arguments> ownedTopicPartitionsArguments() {
        return Stream.of(
            // Field not set in the heartbeat request.
            null,
            // Owned partitions does not match the assigned partitions.
            Collections.emptyList()
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("ownedTopicPartitionsArguments")
    public void testTransitionFromRevokeToRevoke(
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, member.state());

        Assignment targetAssignment = new Assignment(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(ownedTopicPartitions)
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(10, updatedMember.memberEpoch());
        assertEquals(11, updatedMember.targetMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3),
            mkTopicAssignment(topicId2, 6)
        ), updatedMember.assignedPartitions());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2),
            mkTopicAssignment(topicId2, 4, 5)
        ), updatedMember.partitionsPendingRevocation());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 4, 5),
            mkTopicAssignment(topicId2, 7, 8)
        ), updatedMember.partitionsPendingAssignment());
    }

    @Test
    public void testTransitionFromRevokeToAssigning() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, member.state());

        Assignment targetAssignment = new Assignment(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .withOwnedTopicPartitions(requestFromAssignment(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6))))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(11, updatedMember.memberEpoch());
        assertEquals(11, updatedMember.targetMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3),
            mkTopicAssignment(topicId2, 6)
        ), updatedMember.assignedPartitions());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingRevocation());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 4, 5),
            mkTopicAssignment(topicId2, 7, 8)
        ), updatedMember.partitionsPendingAssignment());
    }

    @Test
    public void testTransitionFromRevokeToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, member.state());

        Assignment targetAssignment = new Assignment(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(requestFromAssignment(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6))))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(11, updatedMember.memberEpoch());
        assertEquals(11, updatedMember.targetMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ), updatedMember.assignedPartitions());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingRevocation());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingAssignment());
    }

    @Test
    public void testTransitionFromRevokeToStableWhenPartitionsPendingRevocationAreReassignedBeforeBeingRevoked() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, member.state());

        // A new target assignment is computed (epoch 12) before the partitions
        // pending revocation are revoked by the member and those partitions
        // have been reassigned to the member. In this case, the member
        // can keep them a jump to epoch 12.
        Assignment targetAssignment = new Assignment(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(12, updatedMember.memberEpoch());
        assertEquals(12, updatedMember.targetMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ), updatedMember.assignedPartitions());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingRevocation());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingAssignment());
    }

    @Test
    public void testTransitionFromAssigningToAssigning() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(11)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, member.state());

        Assignment targetAssignment = new Assignment(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> {
                if (topicId.equals(topicId1))
                    return -1;
                else
                    return 10;
            })
            .build();

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(11, updatedMember.memberEpoch());
        assertEquals(11, updatedMember.targetMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6)
        ), updatedMember.assignedPartitions());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingRevocation());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId2, 7, 8)
        ), updatedMember.partitionsPendingAssignment());
    }

    @Test
    public void testTransitionFromAssigningToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(11)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, member.state());

        Assignment targetAssignment = new Assignment(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(11, updatedMember.memberEpoch());
        assertEquals(11, updatedMember.targetMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ), updatedMember.assignedPartitions());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingRevocation());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingAssignment());
    }

    @Test
    public void testTransitionFromStableToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 3, 4, 5),
                mkTopicAssignment(topicId2, 6, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, member.state());

        Assignment targetAssignment = new Assignment(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, updatedMember.state());
        assertEquals(11, updatedMember.previousMemberEpoch());
        assertEquals(11, updatedMember.memberEpoch());
        assertEquals(11, updatedMember.targetMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ), updatedMember.assignedPartitions());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingRevocation());
        assertEquals(Collections.emptyMap(), updatedMember.partitionsPendingAssignment());
    }

    @Test
    public void testNewTargetRestartReconciliation() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, member.state());

        Assignment targetAssignment = new Assignment(mkAssignment(
            mkTopicAssignment(topicId1, 6, 7, 8),
            mkTopicAssignment(topicId2, 9, 10, 11)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(10, updatedMember.memberEpoch());
        assertEquals(12, updatedMember.targetMemberEpoch());
        assertEquals(Collections.emptyMap(), updatedMember.assignedPartitions());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ), updatedMember.partitionsPendingRevocation());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 6, 7, 8),
            mkTopicAssignment(topicId2, 9, 10, 11)
        ), updatedMember.partitionsPendingAssignment());
    }

    private static List<ConsumerGroupHeartbeatRequestData.TopicPartitions> requestFromAssignment(
        Map<Uuid, Set<Integer>> assignment
    ) {
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> topicPartitions = new ArrayList<>();

        assignment.forEach((topicId, partitions) -> {
            ConsumerGroupHeartbeatRequestData.TopicPartitions topic = new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                .setTopicId(topicId)
                .setPartitions(new ArrayList<>(partitions));
            topicPartitions.add(topic);
        });

        return topicPartitions;
    }
}
