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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTaskAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CurrentAssignmentBuilderTest {

    @Test
    public void testStableToStable() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1, 2, 3),
                mkTaskAssignment(subtopologyId2, 4, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1, 2, 3),
                mkTaskAssignment(subtopologyId2, 4, 5, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToStableWithNewTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1, 2, 3),
                mkTaskAssignment(subtopologyId2, 4, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1, 2, 3, 4),
                mkTaskAssignment(subtopologyId2, 4, 5, 6, 7))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 1, 2, 3, 4),
                    mkTaskAssignment(subtopologyId2, 4, 5, 6, 7)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToUnrevokedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1, 2, 3),
                mkTaskAssignment(subtopologyId2, 4, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3, 4),
                mkTaskAssignment(subtopologyId2, 5, 6, 7))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 5, 6)))
                .setActiveTasksPendingRevocation(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 1),
                    mkTaskAssignment(subtopologyId2, 4)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToUnreleasedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1, 2, 3),
                mkTaskAssignment(subtopologyId2, 4, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1, 2, 3, 4),
                mkTaskAssignment(subtopologyId2, 4, 5, 6, 7))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToUnreleasedTasksWithOwnedTasksNotHavingRevokedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1, 2, 3),
                mkTaskAssignment(subtopologyId2, 4, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1, 2, 3),
                mkTaskAssignment(subtopologyId2, 4, 5, 7))))
            .withCurrentActiveTaskEpoch((subtopologyId, __) ->
                subtopologyId2.equals(subtopologyId) ? 10 : -1
            )
            .withOwnedActiveTasks(Collections.emptyList())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 4, 5)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnrevokedTasksToStable() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .setActiveTasksPendingRevocation(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1),
                mkTaskAssignment(subtopologyId2, 4)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1)
            .withOwnedActiveTasks(Arrays.asList(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId1)
                    .setPartitions(Arrays.asList(2, 3)),
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId2)
                    .setPartitions(Arrays.asList(5, 6))))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testRemainsInUnrevokedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .setActiveTasksPendingRevocation(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1),
                mkTaskAssignment(subtopologyId2, 4)))
            .build();

        org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder currentAssignmentBuilder = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(
            member)
            .withTargetAssignment(12, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1);

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedActiveTasks(null)
                .build()
        );

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedActiveTasks(Arrays.asList(
                    new StreamsGroupHeartbeatRequestData.TaskIds()
                        .setSubtopology(subtopologyId1)
                        .setPartitions(Arrays.asList(1, 2, 3)),
                    new StreamsGroupHeartbeatRequestData.TaskIds()
                        .setSubtopology(subtopologyId2)
                        .setPartitions(Arrays.asList(5, 6))))
                .build()
        );

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedActiveTasks(Arrays.asList(
                    new StreamsGroupHeartbeatRequestData.TaskIds()
                        .setSubtopology(subtopologyId1)
                        .setPartitions(Arrays.asList(2, 3)),
                    new StreamsGroupHeartbeatRequestData.TaskIds()
                        .setSubtopology(subtopologyId2)
                        .setPartitions(Arrays.asList(4, 5, 6))))
                .build()
        );
    }

    @Test
    public void testUnrevokedTasksToUnrevokedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .setActiveTasksPendingRevocation(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1),
                mkTaskAssignment(subtopologyId2, 4)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1)
            .withOwnedActiveTasks(Arrays.asList(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId1)
                    .setPartitions(Arrays.asList(2, 3)),
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId2)
                    .setPartitions(Arrays.asList(5, 6))))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 3),
                    mkTaskAssignment(subtopologyId2, 6)))
                .setActiveTasksPendingRevocation(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2),
                    mkTaskAssignment(subtopologyId2, 5)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnrevokedTasksToUnreleasedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3, 4),
                mkTaskAssignment(subtopologyId2, 5, 6, 7))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .withOwnedActiveTasks(Arrays.asList(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId1)
                    .setPartitions(Arrays.asList(2, 3)),
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId2)
                    .setPartitions(Arrays.asList(5, 6))))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedTasksToStable() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(11)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedTasksToStableWithNewTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3, 4),
                mkTaskAssignment(subtopologyId2, 5, 6, 7))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2, 3, 4),
                    mkTaskAssignment(subtopologyId2, 5, 6, 7)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedTasksToUnreleasedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3, 4),
                mkTaskAssignment(subtopologyId2, 5, 6, 7))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .build();

        assertEquals(member, updatedMember);
    }

    @Test
    public void testUnreleasedTasksToUnrevokedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 3),
                    mkTaskAssignment(subtopologyId2, 6)))
                .setActiveTasksPendingRevocation(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2),
                    mkTaskAssignment(subtopologyId2, 5)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnknownState() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNKNOWN)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6)))
            .setActiveTasksPendingRevocation(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2),
                mkTaskAssignment(subtopologyId2, 5)))
            .build();

        // When the member is in an unknown state, the member is first to force
        // a reset of the client side member state.
        assertThrows(FencedMemberEpochException.class, () -> new org.apache.kafka.coordinator.group.streams.CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new org.apache.kafka.coordinator.group.streams.Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .build());

        // Then the member rejoins with no owned tasks.
        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 11)
            .withOwnedActiveTasks(Collections.emptyList())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(11)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 3),
                    mkTaskAssignment(subtopologyId2, 6)))
                .build(),
            updatedMember
        );
    }
}
