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
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.consumer.MemberAssignment;
import org.apache.kafka.coordinator.group.consumer.CurrentAssignmentBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.consumer.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.consumer.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CurrentAssignmentBuilderTest {

    @Test
    public void testTransitionFromNewTargetToRevoke() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setNextMemberEpoch(10)
            .setAssigned(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, member.state());

        MemberAssignment targetAssignment = new MemberAssignment(mkAssignment(
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
        assertEquals(11, updatedMember.nextMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3),
            mkTopicAssignment(topicId2, 6)
        ), updatedMember.assigned());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2),
            mkTopicAssignment(topicId2, 4, 5)
        ), updatedMember.revoking());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 4, 5),
            mkTopicAssignment(topicId2, 7, 8)
        ), updatedMember.assigning());
    }

    @Test
    public void testTransitionFromNewTargetToAssigning() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setNextMemberEpoch(10)
            .setAssigned(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, member.state());

        MemberAssignment targetAssignment = new MemberAssignment(mkAssignment(
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
        assertEquals(11, updatedMember.nextMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ), updatedMember.assigned());
        assertEquals(Collections.emptyMap(), updatedMember.revoking());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 4, 5),
            mkTopicAssignment(topicId2, 7, 8)
        ), updatedMember.assigning());
    }

    @Test
    public void testTransitionFromNewTargetToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setNextMemberEpoch(10)
            .setAssigned(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, member.state());

        MemberAssignment targetAssignment = new MemberAssignment(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(10, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> 10)
            .build();

        assertEquals(ConsumerGroupMember.MemberState.STABLE, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(10, updatedMember.memberEpoch());
        assertEquals(10, updatedMember.nextMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ), updatedMember.assigned());
        assertEquals(Collections.emptyMap(), updatedMember.revoking());
        assertEquals(Collections.emptyMap(), updatedMember.assigning());
    }

    @Test
    public void testTransitionFromRevokeToRevokeWithNull() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setNextMemberEpoch(11)
            .setAssigned(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setRevoking(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)))
            .setAssigning(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, member.state());

        MemberAssignment targetAssignment = new MemberAssignment(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(null) // The client has not revoked yet.
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(10, updatedMember.memberEpoch());
        assertEquals(11, updatedMember.nextMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3),
            mkTopicAssignment(topicId2, 6)
        ), updatedMember.assigned());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2),
            mkTopicAssignment(topicId2, 4, 5)
        ), updatedMember.revoking());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 4, 5),
            mkTopicAssignment(topicId2, 7, 8)
        ), updatedMember.assigning());
    }

    @Test
    public void testTransitionFromRevokeToRevokeWithEmptyList() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setNextMemberEpoch(11)
            .setAssigned(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setRevoking(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)))
            .setAssigning(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, member.state());

        MemberAssignment targetAssignment = new MemberAssignment(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ));

        ConsumerGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, targetAssignment)
            .withCurrentPartitionEpoch((topicId, partitionId) -> -1)
            .withOwnedTopicPartitions(Collections.emptyList()) // The client has not revoked yet.
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, updatedMember.state());
        assertEquals(10, updatedMember.previousMemberEpoch());
        assertEquals(10, updatedMember.memberEpoch());
        assertEquals(11, updatedMember.nextMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3),
            mkTopicAssignment(topicId2, 6)
        ), updatedMember.assigned());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2),
            mkTopicAssignment(topicId2, 4, 5)
        ), updatedMember.revoking());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 4, 5),
            mkTopicAssignment(topicId2, 7, 8)
        ), updatedMember.assigning());
    }

    @Test
    public void testTransitionFromRevokeToAssigning() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setNextMemberEpoch(11)
            .setAssigned(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setRevoking(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)))
            .setAssigning(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, member.state());

        MemberAssignment targetAssignment = new MemberAssignment(mkAssignment(
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
        assertEquals(11, updatedMember.nextMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3),
            mkTopicAssignment(topicId2, 6)
        ), updatedMember.assigned());
        assertEquals(Collections.emptyMap(), updatedMember.revoking());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 4, 5),
            mkTopicAssignment(topicId2, 7, 8)
        ), updatedMember.assigning());
    }

    @Test
    public void testTransitionFromRevokeToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setNextMemberEpoch(11)
            .setAssigned(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setRevoking(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)))
            .setAssigning(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, member.state());

        MemberAssignment targetAssignment = new MemberAssignment(mkAssignment(
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
        assertEquals(11, updatedMember.nextMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ), updatedMember.assigned());
        assertEquals(Collections.emptyMap(), updatedMember.revoking());
        assertEquals(Collections.emptyMap(), updatedMember.assigning());
    }

    @Test
    public void testTransitionFromAssigningToAssigning() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(11)
            .setNextMemberEpoch(11)
            .setAssigned(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setAssigning(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, member.state());

        MemberAssignment targetAssignment = new MemberAssignment(mkAssignment(
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
        assertEquals(11, updatedMember.nextMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6)
        ), updatedMember.assigned());
        assertEquals(Collections.emptyMap(), updatedMember.revoking());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId2, 7, 8)
        ), updatedMember.assigning());
    }

    @Test
    public void testTransitionFromAssigningToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(11)
            .setNextMemberEpoch(11)
            .setAssigned(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setAssigning(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, member.state());

        MemberAssignment targetAssignment = new MemberAssignment(mkAssignment(
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
        assertEquals(11, updatedMember.nextMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ), updatedMember.assigned());
        assertEquals(Collections.emptyMap(), updatedMember.revoking());
        assertEquals(Collections.emptyMap(), updatedMember.assigning());
    }

    @Test
    public void testTransitionFromStableToStable() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setNextMemberEpoch(11)
            .setAssigned(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setAssigning(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, member.state());

        MemberAssignment targetAssignment = new MemberAssignment(mkAssignment(
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
        assertEquals(11, updatedMember.nextMemberEpoch());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 3, 4, 5),
            mkTopicAssignment(topicId2, 6, 7, 8)
        ), updatedMember.assigned());
        assertEquals(Collections.emptyMap(), updatedMember.revoking());
        assertEquals(Collections.emptyMap(), updatedMember.assigning());
    }

    @Test
    public void testNewTargetRestartReconciliation() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setNextMemberEpoch(11)
            .setAssigned(mkAssignment(
                mkTopicAssignment(topicId1, 3),
                mkTopicAssignment(topicId2, 6)))
            .setRevoking(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2),
                mkTopicAssignment(topicId2, 4, 5)))
            .setAssigning(mkAssignment(
                mkTopicAssignment(topicId1, 4, 5),
                mkTopicAssignment(topicId2, 7, 8)))
            .build();

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, member.state());

        MemberAssignment targetAssignment = new MemberAssignment(mkAssignment(
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
        assertEquals(12, updatedMember.nextMemberEpoch());
        assertEquals(Collections.emptyMap(), updatedMember.assigned());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 1, 2, 3),
            mkTopicAssignment(topicId2, 4, 5, 6)
        ), updatedMember.revoking());
        assertEquals(mkAssignment(
            mkTopicAssignment(topicId1, 6, 7, 8),
            mkTopicAssignment(topicId2, 9, 10, 11)
        ), updatedMember.assigning());
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
