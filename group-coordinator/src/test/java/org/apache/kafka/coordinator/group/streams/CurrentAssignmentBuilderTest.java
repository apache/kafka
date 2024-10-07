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
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CurrentAssignmentBuilderTest {

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToStable(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member =
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setProcessId("process")
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setAssignment(
                    mkAssignment(
                        taskRole,
                        mkTasks(subtopologyId1, 1, 2),
                        mkTasks(subtopologyId2, 3, 4)))
                .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 1, 2),
                mkTasks(subtopologyId2, 3, 4)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> "process")
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setProcessId("process")
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignment(mkAssignment(
                    taskRole,
                    mkTasks(subtopologyId1, 1, 2),
                    mkTasks(subtopologyId2, 3, 4)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToStableWithNewTasks(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setProcessId("process")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 1, 2),
                mkTasks(subtopologyId2, 3, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 1, 2, 4),
                mkTasks(subtopologyId2, 3, 4, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setProcessId("process")
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 1, 2, 4),
                    mkTasks(subtopologyId2, 3, 4, 7)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToUnrevokedTasks(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setProcessId("process")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 1, 2),
                mkTasks(subtopologyId2, 3, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 4, 5)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId("process")
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 2),
                    mkTasks(subtopologyId2, 4)))
                .setAssignmentPendingRevocation(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 1),
                    mkTasks(subtopologyId2, 3)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToUnreleasedTasks(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setProcessId("process")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 1, 2),
                mkTasks(subtopologyId2, 3, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 1, 2, 4),
                mkTasks(subtopologyId2, 3, 4, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> "process")
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNRELEASED_TASKS)
                .setProcessId("process")
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 1, 2),
                    mkTasks(subtopologyId2, 3, 4)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToUnreleasedTasksWithOwnedTasksNotHavingRevokedTasks(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setProcessId("process")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 1, 2),
                mkTasks(subtopologyId2, 3, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 1, 2),
                mkTasks(subtopologyId2, 3, 5)))
            .withCurrentActiveTaskProcessId((subtopologyId, __) ->
                subtopologyId2.equals(subtopologyId) ? "process" : null
            )
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .withOwnedAssignment(mkAssignment(taskRole))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNRELEASED_TASKS)
                .setProcessId("process")
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 1, 2),
                    mkTasks(subtopologyId2, 3)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnrevokedTasksToStable(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId("process")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .setAssignmentPendingRevocation(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 1),
                mkTasks(subtopologyId2, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .withOwnedAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setProcessId("process")
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 2, 3),
                    mkTasks(subtopologyId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testRemainsInUnrevokedTasks(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId("process")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .setAssignmentPendingRevocation(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 1),
                mkTasks(subtopologyId2, 4)))
            .build();

        CurrentAssignmentBuilder currentAssignmentBuilder = new CurrentAssignmentBuilder(
            member)
            .withTargetAssignment(12, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 3),
                mkTasks(subtopologyId2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet());

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedAssignment(null)
                .build()
        );

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 1, 2, 3),
                    mkTasks(subtopologyId2, 5, 6)))
                .build()
        );

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 2, 3),
                    mkTasks(subtopologyId2, 4, 5, 6)))
                .build()
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnrevokedTasksToUnrevokedTasks(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId("process")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .setAssignmentPendingRevocation(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 1),
                mkTasks(subtopologyId2, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 3),
                mkTasks(subtopologyId2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withOwnedAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId("process")
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 3),
                    mkTasks(subtopologyId2, 6)))
                .setAssignmentPendingRevocation(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 2),
                    mkTasks(subtopologyId2, 5)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnrevokedTasksToUnreleasedTasks(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId("process")
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3, 4),
                mkTasks(subtopologyId2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> "process")
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .withOwnedActiveTasks(Arrays.asList(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopologyId1)
                    .setPartitions(Arrays.asList(2, 3)),
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopologyId2)
                    .setPartitions(Arrays.asList(5, 6))))
            .withOwnedStandbyTasks(Collections.emptyList())
            .withOwnedWarmupTasks(Collections.emptyList())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNRELEASED_TASKS)
                .setProcessId("process")
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 2, 3),
                    mkTasks(subtopologyId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToStable(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process1")
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> "process")
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.singleton("process"))
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) ->
                Collections.singleton("process"))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setProcessId("process1")
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(11)
                .setAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 2, 3),
                    mkTasks(subtopologyId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToStableWithNewTasks(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process1")
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3, 4),
                mkTasks(subtopologyId2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setProcessId("process1")
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 2, 3, 4),
                    mkTasks(subtopologyId2, 5, 6, 7)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToUnreleasedTasks(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process")
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3, 4),
                mkTasks(subtopologyId2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> "process")
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.singleton("process"))
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.singleton("process"))
            .build();

        assertEquals(member, updatedMember);
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToUnreleasedTasksOtherUnreleasedTaskRole(TaskRole taskRole) {
        // The unreleased task is owned by a task of a different role on the same process.
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process")
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3, 4),
                mkTasks(subtopologyId2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> (taskRole == TaskRole.STANDBY)
                    ? Collections.emptySet() : Collections.singleton("process"))
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> (taskRole == TaskRole.STANDBY)
                    ? Collections.singleton("process") : Collections.emptySet())
            .build();

        assertEquals(member, updatedMember);
    }

    @Test
    public void testUnreleasedTasksToUnreleasedTasksAnyActiveOwner() {
        // The unreleased task remains unreleased, because it is owned by any other instance in
        // an active role, no matter the process.
        // The task that is not unreleased can be assigned.
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process")
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignment(mkAssignment(TaskRole.ACTIVE,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember expectedMember = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process")
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignment(mkAssignment(TaskRole.ACTIVE,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6, 7)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, mkAssignment(TaskRole.ACTIVE,
                mkTasks(subtopologyId1, 2, 3, 4),
                mkTasks(subtopologyId2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) ->
                (subtopologyId.equals(subtopologyId1) && partitionId == 4) ? "anyOtherProcess"
                    : null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(expectedMember, updatedMember);
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToUnrevokedTasks(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process1")
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2, 3),
                mkTasks(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 3),
                mkTasks(subtopologyId2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> "process")
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId("process1")
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 3),
                    mkTasks(subtopologyId2, 6)))
                .setAssignmentPendingRevocation(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 2),
                    mkTasks(subtopologyId2, 5)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnknownState(TaskRole taskRole) {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNKNOWN)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setProcessId("process")
            .setAssignment(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 3),
                mkTasks(subtopologyId2, 6)))
            .setAssignmentPendingRevocation(mkAssignment(taskRole,
                mkTasks(subtopologyId1, 2),
                mkTasks(subtopologyId2, 5)))
            .build();

        // When the member is in an unknown state, the member is first to force
        // a reset of the client side member state.
        assertThrows(FencedMemberEpochException.class, () -> new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 3),
                mkTasks(subtopologyId2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> "process")
            .build());

        // Then the member rejoins with no owned tasks.
        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, mkAssignment(taskRole,
                mkTasks(subtopologyId1, 3),
                mkTasks(subtopologyId2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> "process")
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .withOwnedAssignment(mkAssignment(taskRole))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setProcessId("process")
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(11)
                .setAssignment(mkAssignment(taskRole,
                    mkTasks(subtopologyId1, 3),
                    mkTasks(subtopologyId2, 6)))
                .build(),
            updatedMember
        );
    }
}
