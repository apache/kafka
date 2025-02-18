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
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;

import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTuple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CurrentAssignmentBuilderTest {

    private static final String SUBTOPOLOGY_ID1 = Uuid.randomUuid().toString();
    private static final String SUBTOPOLOGY_ID2 = Uuid.randomUuid().toString();
    private static final String PROCESS_ID = "process_id";
    private static final String MEMBER_NAME = "member";

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToStable(TaskRole taskRole) {
        final int memberEpoch = 10;

        StreamsGroupMember member =
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(
                    mkTasksTuple(
                        taskRole,
                        mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                        mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(
                    taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToStableAtTargetEpoch(TaskRole taskRole) {
        final int memberEpoch = 10;

        StreamsGroupMember member =
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(
                    mkTasksTuple(
                        taskRole,
                        mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                        mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(
                    taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToStableWithNewTasks(TaskRole taskRole) {
        final int memberEpoch = 10;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.STABLE)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2, 4),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2, 4),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4, 7)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToUnrevokedTasks(TaskRole taskRole) {
        final int memberEpoch = 10;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.STABLE)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 4, 5)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 4)))
                .setTasksPendingRevocation(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1),
                    mkTasks(SUBTOPOLOGY_ID2, 3)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToUnrevokedWithEmptyAssignment(TaskRole taskRole) {
        final int memberEpoch = 10;

        StreamsGroupMember member =
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(
                    mkTasksTuple(
                        taskRole,
                        mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                        mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, TasksTuple.EMPTY)
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(TasksTuple.EMPTY)
                .setTasksPendingRevocation(
                    mkTasksTuple(
                        taskRole,
                        mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                        mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToUnreleasedTasks(TaskRole taskRole) {
        final int memberEpoch = 10;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.STABLE)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2, 4),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNRELEASED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToUnreleasedTasksWithOwnedTasksNotHavingRevokedTasks(TaskRole taskRole) {
        final int memberEpoch = 10;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.STABLE)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 5)))
            .withCurrentActiveTaskProcessId((subtopologyId, __) ->
                SUBTOPOLOGY_ID2.equals(subtopologyId) ? PROCESS_ID : null
            )
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .withOwnedAssignment(mkTasksTuple(taskRole))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNRELEASED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnrevokedTasksToStable(TaskRole taskRole) {
        final int memberEpoch = 10;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1),
                mkTasks(SUBTOPOLOGY_ID2, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .withOwnedAssignment(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testRemainsInUnrevokedTasks(TaskRole taskRole) {
        final int memberEpoch = 10;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1),
                mkTasks(SUBTOPOLOGY_ID2, 4)))
            .build();

        CurrentAssignmentBuilder currentAssignmentBuilder = new CurrentAssignmentBuilder(
            member)
            .withTargetAssignment(memberEpoch + 2, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
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
                .withOwnedAssignment(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
                .build()
        );

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedAssignment(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 4, 5, 6)))
                .build()
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnrevokedTasksToUnrevokedTasks(TaskRole taskRole) {
        final int memberEpoch = 10;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1),
                mkTasks(SUBTOPOLOGY_ID2, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 2, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withOwnedAssignment(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 6)))
                .setTasksPendingRevocation(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 5)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnrevokedTasksToUnreleasedTasks(TaskRole taskRole) {
        final int memberEpoch = 11;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch - 1)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1),
                mkTasks(SUBTOPOLOGY_ID2, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .withOwnedAssignment(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6))
            )
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNRELEASED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToStable(TaskRole taskRole) {
        final int memberEpoch = 11;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process1")
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.singleton(PROCESS_ID))
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) ->
                Collections.singleton(PROCESS_ID))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId("process1")
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToStableWithNewTasks(TaskRole taskRole) {
        int memberEpoch = 11;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process1")
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId("process1")
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToUnreleasedTasks(TaskRole taskRole) {
        int memberEpoch = 11;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.singleton(PROCESS_ID))
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.singleton(PROCESS_ID))
            .build();

        assertEquals(member, updatedMember);
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToUnreleasedTasksOtherUnreleasedTaskRole(TaskRole taskRole) {
        int memberEpoch = 11;

        // The unreleased task is owned by a task of a different role on the same process.
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> (taskRole == TaskRole.STANDBY)
                    ? Collections.emptySet() : Collections.singleton(PROCESS_ID))
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> (taskRole == TaskRole.STANDBY)
                    ? Collections.singleton(PROCESS_ID) : Collections.emptySet())
            .build();

        assertEquals(member, updatedMember);
    }

    @Test
    public void testUnreleasedTasksToUnreleasedTasksAnyActiveOwner() {
        int memberEpoch = 11;

        // The unreleased task remains unreleased, because it is owned by any other instance in
        // an active role, no matter the process.
        // The task that is not unreleased can be assigned.
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .build();

        StreamsGroupMember expectedMember = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) ->
                (subtopologyId.equals(SUBTOPOLOGY_ID1) && partitionId == 4) ? "anyOtherProcess"
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
        int memberEpoch = 11;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process1")
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 4),
                mkTasks(SUBTOPOLOGY_ID2, 7)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId("process1")
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 6)))
                .setTasksPendingRevocation(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 5)))
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnknownState(TaskRole taskRole) {
        int memberEpoch = 11;

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNKNOWN)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setProcessId(PROCESS_ID)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .setTasksPendingRevocation(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 5)))
            .build();

        // When the member is in an unknown state, the member is first to force
        // a reset of the client side member state.
        assertThrows(FencedMemberEpochException.class, () -> new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .build());

        // Then the member rejoins with no owned tasks.
        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Collections.emptySet())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Collections.emptySet())
            .withOwnedAssignment(mkTasksTuple(taskRole))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 6)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .build(),
            updatedMember
        );
    }
}
