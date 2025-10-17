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

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTuple;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTupleWithCommonEpoch;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTupleWithEpochs;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksWithEpochs;
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
                    mkTasksTupleWithCommonEpoch(
                        taskRole,
                        memberEpoch,
                        mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                        mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(
                    taskRole,
                    memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
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
                    mkTasksTupleWithCommonEpoch(
                        taskRole,
                        memberEpoch,
                        mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                        mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(
                    taskRole,
                    memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
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
            .setAssignedTasks(mkTasksTupleWithEpochs(taskRole,
                mkTasksWithEpochs(SUBTOPOLOGY_ID1, Map.of(1, 9, 2, 8)),
                mkTasksWithEpochs(SUBTOPOLOGY_ID2, Map.of(3, 9, 4, 8))))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2, 4),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithEpochs(taskRole,
                    mkTasksWithEpochs(SUBTOPOLOGY_ID1,  Map.of(1, 9, 2, 8, 4, memberEpoch + 1)),
                    mkTasksWithEpochs(SUBTOPOLOGY_ID2, Map.of(3, 9, 4, 8, 7, memberEpoch + 1))))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 4, 5)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 4)))
                .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
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
                    mkTasksTupleWithCommonEpoch(
                        taskRole,
                        memberEpoch,
                        mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                        mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, TasksTuple.EMPTY)
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
                .setTasksPendingRevocation(
                    mkTasksTupleWithCommonEpoch(
                        taskRole,
                        memberEpoch,
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2, 4),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNRELEASED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 5)))
            .withCurrentActiveTaskProcessId((subtopologyId, __) ->
                SUBTOPOLOGY_ID2.equals(subtopologyId) ? PROCESS_ID : null
            )
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withOwnedAssignment(mkTasksTuple(taskRole))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNRELEASED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 1),
                mkTasks(SUBTOPOLOGY_ID2, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
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
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
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
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of());

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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
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
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 6)))
                .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 1),
                mkTasks(SUBTOPOLOGY_ID2, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
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
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of(PROCESS_ID))
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) ->
                Set.of(PROCESS_ID))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId("process1")
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId("process1")
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of(PROCESS_ID))
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of(PROCESS_ID))
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> (taskRole == TaskRole.STANDBY)
                    ? Set.of() : Set.of(PROCESS_ID))
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> (taskRole == TaskRole.STANDBY)
                    ? Set.of(PROCESS_ID) : Set.of())
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        StreamsGroupMember expectedMember = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) ->
                (subtopologyId.equals(SUBTOPOLOGY_ID1) && partitionId == 4) ? "anyOtherProcess"
                    : null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 4),
                mkTasks(SUBTOPOLOGY_ID2, 7)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId("process1")
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 6)))
                .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
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
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
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
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withOwnedAssignment(mkTasksTuple(taskRole))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(taskRole, memberEpoch,
                    mkTasks(SUBTOPOLOGY_ID1, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 6)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build(),
            updatedMember
        );
    }

    /**
     * Tests epoch assignment from assigned tasks.
     * This tests the first lookup path in addEpochsToTasks where epochs are found in assigned tasks.
     */
    @Test
    public void testEpochLookupFromAssignedTasks() {
        final int memberEpoch = 10;

        // Create a member with tasks that have specific epochs in assigned tasks
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.STABLE)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                mkTasksWithEpochs(SUBTOPOLOGY_ID1, Map.of(1, 5, 2, 6)),
                mkTasksWithEpochs(SUBTOPOLOGY_ID2, Map.of(3, 7, 4, 8))))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        // Same tasks in target assignment should retain their epochs from assigned tasks
        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        // Verify that epochs are preserved from assigned tasks
        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                    mkTasksWithEpochs(SUBTOPOLOGY_ID1, Map.of(1, 5, 2, 6)),
                    mkTasksWithEpochs(SUBTOPOLOGY_ID2, Map.of(3, 7, 4, 8))))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build(),
            updatedMember
        );
    }

    /**
     * Tests epoch assignment from tasks pending revocation.
     * This tests the second lookup path in addEpochsToTasks where epochs are found in pending revocation.
     * When tasks pending revocation are brought back into the target assignment after the member revokes them,
     * the epochs should be preserved from the pending revocation.
     */
    @Test
    public void testEpochLookupFromPendingRevocation() {
        final int memberEpoch = 10;

        // Create a member in UNREVOKED_TASKS state with:
        // - Some tasks assigned (5, 6) with epochs (9, 10)
        // - Some tasks pending revocation (1, 2, 3, 4) with specific epochs (5, 6, 7, 8)
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                mkTasksWithEpochs(SUBTOPOLOGY_ID1, Map.of(5, 9)),
                mkTasksWithEpochs(SUBTOPOLOGY_ID2, Map.of(6, 10))))
            .setTasksPendingRevocation(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                mkTasksWithEpochs(SUBTOPOLOGY_ID1, Map.of(1, 5, 2, 6)),
                mkTasksWithEpochs(SUBTOPOLOGY_ID2, Map.of(3, 7, 4, 8))))
            .build();

        // Target assignment brings back the pending tasks (1, 2, 3, 4) along with current assigned (5, 6)
        // The member has revoked the pending tasks (they're not in owned assignment)
        // So it should transition to next epoch and assign the tasks
        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2, 5),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withOwnedAssignment(mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 5),  // Only owns the assigned tasks, not the pending revocation
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .build();

        // Verify that:
        // - Tasks from pending revocation (1@5, 2@6, 3@7, 4@8) retain their epochs when re-assigned
        // - Tasks from assigned (5@9, 6@10) retain their epochs
        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                    mkTasksWithEpochs(SUBTOPOLOGY_ID1, Map.of(1, 5, 2, 6, 5, 9)),
                    mkTasksWithEpochs(SUBTOPOLOGY_ID2, Map.of(3, 7, 4, 8, 6, 10))))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build(),
            updatedMember
        );
    }

    /**
     * Tests fallback to default epoch.
     * This tests the third path in addEpochsToTasks where epochs are not found in assigned or pending,
     * so the default epoch (target assignment epoch) is used.
     */
    @Test
    public void testEpochFallbackToDefault() {
        final int memberEpoch = 10;

        // Create a member with empty assignments
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.STABLE)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        // New tasks are assigned
        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        // Verify that all tasks use the default epoch (memberEpoch + 1)
        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, memberEpoch + 1,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build(),
            updatedMember
        );
    }

    /**
     * Tests mixed epoch assignment scenarios.
     * This tests all three paths in addEpochsToTasks:
     * - Some epochs from assigned tasks
     * - Some epochs from pending revocation
     * - Some epochs fallback to default
     */
    @Test
    public void testMixedEpochLookup() {
        final int memberEpoch = 10;

        // Create a member with:
        // - Tasks 1, 2 in assigned with epochs 5, 6
        // - Tasks 3, 4 in pending revocation with epochs 7, 8
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                mkTasksWithEpochs(SUBTOPOLOGY_ID1, Map.of(1, 5, 2, 6))))
            .setTasksPendingRevocation(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                mkTasksWithEpochs(SUBTOPOLOGY_ID2, Map.of(3, 7, 4, 8))))
            .build();

        // Target assignment includes:
        // - Task 1 from assigned (should use epoch 5)
        // - Task 3 from pending revocation (should use epoch 7)
        // - Task 5 (new task, should use default epoch memberEpoch + 1 = 11)
        // The member revokes tasks 2, 3, 4 (not in owned), transitions to next epoch
        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 1),
                mkTasks(SUBTOPOLOGY_ID2, 3, 5)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withOwnedAssignment(mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 1)))  // Only owns task 1 (task 2 is being revoked, tasks 3,4 already revoked)
            .build();

        // Verify mixed epoch assignment:
        // - Task 1 from SUBTOPOLOGY_ID1 should have epoch 5 (from assigned)
        // - Task 3 from SUBTOPOLOGY_ID2 should have epoch 7 (from pending revocation)
        // - Task 5 from SUBTOPOLOGY_ID2 should have epoch 11 (default)
        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                    mkTasksWithEpochs(SUBTOPOLOGY_ID1, Map.of(1, 5)),
                    mkTasksWithEpochs(SUBTOPOLOGY_ID2, Map.of(3, 7, 5, memberEpoch + 1))))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build(),
            updatedMember
        );
    }

    /**
     * Tests that assigned tasks take precedence over pending revocation.
     * If a task exists in both assigned and pending revocation, the epoch from assigned should be used.
     */
    @Test
    public void testAssignedTasksHavePrecedenceOverPendingRevocation() {
        final int memberEpoch = 10;

        // Create a member with task 1 in both assigned (epoch 5) and pending revocation (epoch 7)
        // This shouldn't normally happen, but tests the precedence logic
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.STABLE)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                mkTasksWithEpochs(SUBTOPOLOGY_ID1, Map.of(1, 5, 2, 6))))
            .setTasksPendingRevocation(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                mkTasksWithEpochs(SUBTOPOLOGY_ID1, Map.of(1, 7))))
            .build();

        // Target assignment includes task 1
        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        // Verify that task 1 uses epoch 5 from assigned, not epoch 7 from pending revocation
        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                    mkTasksWithEpochs(SUBTOPOLOGY_ID1, Map.of(1, 5, 2, 6))))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build(),
            updatedMember
        );
    }
}
