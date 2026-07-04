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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.coordinator.group.streams.MockTaskAssignor;
import org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupBuilder;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil;
import org.apache.kafka.coordinator.group.streams.TasksTuple;
import org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordsEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertResponseEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertUnorderedRecordsEquals;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupSessionTimeoutKey;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.getDefaultAssignmentConfigs;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.mkResponseTasks;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticHeartbeat;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticJoinHeartbeat;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.streamsGroupMemberBuilderWithDefaults;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.streamsTopicFixture;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTupleWithEpochs;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksWithEpochs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamsGroupMixedGroupMetadataManagerTest {

    private static final int DEFAULT_GROUP_EPOCH = 10;

    @Test
    public void testDynamicJoinFailsAtMaxSizeWhileStaticMemberIsTemporarilyLeftAndDynamicMemberStillExists() {
        // STREAMS_GROUP_MAX_SIZE_CONFIG is 2. 
        // There are 2 members. (1 dynamic member, 1 static member)
        //   - one static member leaves temporarily with -2 epoch.
        //   - one dynamic member alive in group.
        // Another dynamic member try to join.

        String groupId = "fooup";
        String staticMemberId = Uuid.randomUuid().toString();
        String staticInstanceId = Uuid.randomUuid().toString();
        String dynamicMemberId = Uuid.randomUuid().toString();
        String newDynamicMemberId = Uuid.randomUuid().toString();
        StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(List.of());

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new MetadataImageBuilder().buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, 2)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(staticMemberId, staticInstanceId)
                    .setMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(10)
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(dynamicMemberId)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .build())
                .withTargetAssignmentEpoch(10)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology)))
            .build();

        assertThrows(GroupMaxSizeReachedException.class, () ->
            context.streamsGroupHeartbeat(
                staticJoinHeartbeat(groupId, newDynamicMemberId, null, "new-process-id")
                    .setRebalanceTimeoutMs(1500)
                    .setTopology(topology)
            )
        );
    }

    @Test
    public void testStaticRejoinSucceedsAtMaxSizeWhileDynamicMemberStillExists() {
        // Scenario
        // STREAMS_GROUP_MAX_SIZE_CONFIG is 2. 
        // There are 2 members. (1 dynamic member, 1 static member)
        //   - static member leaves temporarily with -2 epoch.
        //   - dynamic member alive in group
        // static member try to rejoin.

        int groupEpoch = 10;
        String groupId = "fooup";

        String subtopologyId = "subtopology-1";
        StreamsGroupTestUtil.StreamsTopicFixture topic = streamsTopicFixture(subtopologyId, "foo", 4);

        String oldStaticMemberId = Uuid.randomUuid().toString();
        String newStaticMemberId = Uuid.randomUuid().toString();
        String staticInstanceId = Uuid.randomUuid().toString();
        String dynamicMemberId = Uuid.randomUuid().toString();

        TasksTupleWithEpochs staticAssignedTasks = topic.assignedTasks(groupEpoch, 0, 1);
        TasksTuple staticTargetAssignment = topic.targetAssignment(0, 1);

        TasksTupleWithEpochs dynamicAssignedTasks = topic.assignedTasks(groupEpoch, 2, 3);
        TasksTuple dynamicTargetAssignment = topic.targetAssignment(2, 3);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, 2)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(oldStaticMemberId, staticInstanceId)
                    .setMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(10)
                    .setAssignedTasks(staticAssignedTasks)
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(dynamicMemberId)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(dynamicAssignedTasks)
                    .build())
                .withTargetAssignment(oldStaticMemberId, staticTargetAssignment)
                .withTargetAssignment(dynamicMemberId, dynamicTargetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .build();


        // static member rejoins.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, newStaticMemberId, staticInstanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(newStaticMemberId)
                .setMemberEpoch(groupEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(mkResponseTasks(subtopologyId, 0, 1))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()),
            rejoinResult.response().data()
        );

        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
        assertFalse(group.hasMember(oldStaticMemberId));
        assertTrue(group.hasMember(newStaticMemberId));
        assertEquals(newStaticMemberId, group.staticMember(staticInstanceId).memberId());

        assertTrue(group.hasMember(dynamicMemberId));
        assertEquals(dynamicAssignedTasks, group.getMemberOrThrow(dynamicMemberId).assignedTasks());
        assertEquals(dynamicTargetAssignment, group.targetAssignment(dynamicMemberId, Optional.empty()));
        assertEquals(groupEpoch, group.groupEpoch());
    }

    @Test
    public void testDynamicJoinSucceedsAfterTemporarilyLeftStaticMemberSessionTimeoutInMixedGroup() {
        // Scenario:
        // STREAMS_GROUP_MAX_SIZE_CONFIG is 2.
        // There are 2 members. (1 dynamic member, 1 static member)
        //   - static member leaves temporarily with -2 epoch.
        //   - dynamic member alive in group.
        // After the static member session timeout expires, a new dynamic member can join.

        int groupEpoch = 10;
        int timeoutGroupEpoch = groupEpoch + 1;
        int joinGroupEpoch = timeoutGroupEpoch + 1;

        String groupId = "fooup";

        String subtopologyId = "subtopology-1";
        StreamsGroupTestUtil.StreamsTopicFixture topic = streamsTopicFixture(subtopologyId, "foo", 4);
        StreamsGroupHeartbeatRequestData.Topology topology = topic.topology();

        String staticMemberId = Uuid.randomUuid().toString();
        String staticInstanceId = Uuid.randomUuid().toString();
        String dynamicMemberId = Uuid.randomUuid().toString();
        String newDynamicMemberId = Uuid.randomUuid().toString();

        String staticProcessId = "static-process-id";
        String dynamicProcessId = "dynamic-process-id";
        String newDynamicProcessId = "new-dynamic-process-id";

        TasksTupleWithEpochs staticAssignedTasks = topic.assignedTasks(groupEpoch, 0, 1);
        TasksTuple staticTargetAssignment = topic.targetAssignment(0, 1);

        TasksTupleWithEpochs dynamicAssignedTasks = topic.assignedTasks(groupEpoch, 2, 3);
        TasksTuple dynamicTargetAssignment = topic.targetAssignment(2, 3);

        long groupMetadataHash = topic.metadataHash();

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(topic.metadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, 2)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(staticMemberId, staticInstanceId)
                    .setProcessId(staticProcessId)
                    .setMemberEpoch(groupEpoch)
                    .setPreviousMemberEpoch(groupEpoch - 1)
                    .setAssignedTasks(staticAssignedTasks)
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(dynamicMemberId)
                    .setProcessId(dynamicProcessId)
                    .setMemberEpoch(groupEpoch)
                    .setPreviousMemberEpoch(groupEpoch - 1)
                    .setAssignedTasks(dynamicAssignedTasks)
                    .build())
                .withTargetAssignment(staticMemberId, staticTargetAssignment)
                .withTargetAssignment(dynamicMemberId, dynamicTargetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(groupMetadataHash)
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .build();
        context.onLoaded();

        // static member leaves with epoch -2.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, staticMemberId, staticInstanceId, StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(staticMemberId)
                .setMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setStatus(List.of())
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null), 
            leaveResult.response().data()
        );

        // To prevent session timeout from dynamic member.
        // Sleep 1, and dynamic member send a heartbeat.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(1));
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> dynamicHeartbeatResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, dynamicMemberId, null, groupEpoch)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(dynamicMemberId)
                .setMemberEpoch(groupEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null),
            dynamicHeartbeatResult.response().data());
        assertTrue(dynamicHeartbeatResult.records().isEmpty());

        // static member session timeout.
        context.assertSessionTimeout(groupId, staticMemberId, 45000 - 1);
        List<MockCoordinatorTimer.ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(45000 - 1);

        List<CoordinatorRecord> expectedTimeoutRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, staticMemberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, staticMemberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, staticMemberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId, timeoutGroupEpoch, groupMetadataHash, 0, getDefaultAssignmentConfigs(), -1, -1
            )
        );

        assertEquals(
            List.of(new MockCoordinatorTimer.ExpiredTimeout<>(
                groupSessionTimeoutKey(groupId, staticMemberId),
                new CoordinatorResult<>(expectedTimeoutRecords)
            )),
            timeouts
        );

        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
        assertFalse(group.hasMember(staticMemberId));
        assertTrue(group.hasMember(dynamicMemberId));
        assertEquals(timeoutGroupEpoch, group.groupEpoch());

        assignor.prepareGroupAssignment(Map.of(
            dynamicMemberId, dynamicTargetAssignment,
            newDynamicMemberId, staticTargetAssignment
        ));

        // new dynamic member try to join.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> joinResult = context.streamsGroupHeartbeat(
            staticJoinHeartbeat(groupId, newDynamicMemberId, null, newDynamicProcessId)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(newDynamicMemberId)
                .setMemberEpoch(joinGroupEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(mkResponseTasks(subtopologyId, 0, 1))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()),
            joinResult.response().data()
        );

        StreamsGroupMember expectedJoiningDynamicMember = streamsGroupMemberBuilderWithDefaults(newDynamicMemberId)
            .setProcessId(newDynamicProcessId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .build();

        StreamsGroupMember expectedReconciledDynamicMember = streamsGroupMemberBuilderWithDefaults(newDynamicMemberId)
            .setProcessId(newDynamicProcessId)
            .setMemberEpoch(joinGroupEpoch)
            .setPreviousMemberEpoch(0)
            .setAssignedTasks(mkTasksTupleWithEpochs(
                TaskRole.ACTIVE,
                mkTasksWithEpochs(subtopologyId, Map.of(
                    0, joinGroupEpoch,
                    1, joinGroupEpoch
                ))
            ))
            .build();

        List<CoordinatorRecord> expectedJoinRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedJoiningDynamicMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId,
                joinGroupEpoch,
                groupMetadataHash,
                0,
                getDefaultAssignmentConfigs(),
                -1,
                -1
            ),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, newDynamicMemberId, staticTargetAssignment),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, joinGroupEpoch, context.time.milliseconds()),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedReconciledDynamicMember)
        );

        assertRecordsEquals(expectedJoinRecords, joinResult.records());

        assertTrue(group.hasMember(dynamicMemberId));
        assertTrue(group.hasMember(newDynamicMemberId));
        assertEquals(joinGroupEpoch, group.groupEpoch());
        assertEquals(staticTargetAssignment, group.targetAssignment(newDynamicMemberId, Optional.empty()));
        assertEquals(dynamicTargetAssignment, group.targetAssignment(dynamicMemberId, Optional.empty()));
    }

    @Test
    public void testStaticTemporaryLeaveDoesNotTransferTasksToExistingDynamicMember() {
        // Scenario
        // STREAMS_GROUP_MAX_SIZE_CONFIG is 2. 
        // There are 2 members. (1 dynamic member, 1 static member)
        //   - static member leaves temporarily with -2 epoch.
        //   - dynamic member alive in group
        // If dynamic send a heartbeat, there is no new assignment. 
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String staticMemberId = Uuid.randomUuid().toString();
        String staticInstanceId = Uuid.randomUuid().toString();
        String dynamicMemberId = Uuid.randomUuid().toString();


        StreamsGroupTestUtil.StreamsTopicFixture topic = streamsTopicFixture("subtopology-1", "foo", 3);

        TasksTupleWithEpochs staticAssignedTasks = topic.assignedTasks(groupEpoch, 0, 1);
        TasksTuple staticTargetAssignment = topic.targetAssignment(0, 1);

        TasksTupleWithEpochs dynamicAssignedTasks = topic.assignedTasks(groupEpoch, 2);
        TasksTuple dynamicTargetAssignment = topic.targetAssignment(2);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(staticMemberId, staticInstanceId)
                    .setMemberEpoch(groupEpoch)
                    .setPreviousMemberEpoch(groupEpoch - 1)
                    .setAssignedTasks(staticAssignedTasks)
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(dynamicMemberId)
                    .setMemberEpoch(groupEpoch)
                    .setPreviousMemberEpoch(groupEpoch - 1)
                    .setAssignedTasks(dynamicAssignedTasks)
                    .build())
                .withTargetAssignment(staticMemberId, staticTargetAssignment)
                .withTargetAssignment(dynamicMemberId, dynamicTargetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
            )
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        // static member leaves with epoch -2.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult =
            context.streamsGroupHeartbeat(staticHeartbeat(
                groupId,
                staticMemberId,
                staticInstanceId,
                LEAVE_GROUP_STATIC_MEMBER_EPOCH
            ));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(staticMemberId)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setStatus(List.of())
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null),
            leaveResult.response().data()
        );

        // dynamic member send a heartbeat.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> dynamicHeartbeatResult =
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(dynamicMemberId)
                    .setMemberEpoch(groupEpoch)
            );

        // There is no new assignment.
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(dynamicMemberId)
                .setMemberEpoch(groupEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null),
            dynamicHeartbeatResult.response().data());
        assertTrue(dynamicHeartbeatResult.records().isEmpty());

        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
        assertEquals(dynamicAssignedTasks, group.getMemberOrThrow(dynamicMemberId).assignedTasks());
        assertEquals(dynamicTargetAssignment, group.targetAssignment(dynamicMemberId, Optional.empty()));
        assertEquals(groupEpoch, group.groupEpoch());
    }

    @Test
    public void testStaticRejoinWithUpdatedProcessIdRecomputesTargetAssignmentAndDynamicMemberReconcilesInMixedGroup() {
        // Scenario:
        // There are 2 members.
        //   - static member left temporarily with -2 epoch.
        //   - dynamic member alive in group.
        // When the same static instance rejoins with a different processId, the coordinator
        // bumps the group epoch and recomputes the target assignment.
        // The dynamic member keeps its current assignment until its next heartbeat.
        // When the dynamic member sends the next heartbeat, it reconciles to the recomputed target assignment.

        String groupId = "fooup";
        int groupEpoch = 10;
        int bumpedGroupEpoch = groupEpoch + 1;

        String subtopologyId = "subtopology-1";
        StreamsGroupTestUtil.StreamsTopicFixture topic = streamsTopicFixture(subtopologyId, "foo", 4);

        String oldStaticMemberId = Uuid.randomUuid().toString();
        String rejoinStaticMemberId = Uuid.randomUuid().toString();
        String staticInstanceId = Uuid.randomUuid().toString();
        String dynamicMemberId = Uuid.randomUuid().toString();

        String oldProcessId = "old-process-id";
        String newProcessId = "new-process-id";

        // Initial assignment
        TasksTupleWithEpochs staticAssignedTasks = topic.assignedTasks(groupEpoch, 0, 1);
        TasksTuple oldStaticTargetAssignment = topic.targetAssignment(0, 1);

        TasksTupleWithEpochs dynamicAssignedTasks = topic.assignedTasks(groupEpoch, 2, 3);
        TasksTuple oldDynamicTargetAssignment = topic.targetAssignment(2, 3);

        // Recomputed target assignment after static member rejoins with updated processId
        TasksTuple newStaticTargetAssignment = topic.targetAssignment(0);
        TasksTuple newDynamicTargetAssignment = topic.targetAssignment(1, 2, 3);

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        assignor.prepareGroupAssignment(Map.of(
            rejoinStaticMemberId, newStaticTargetAssignment,
            dynamicMemberId, newDynamicTargetAssignment
        ));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(oldStaticMemberId, staticInstanceId)
                    .setMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(groupEpoch)
                    .setProcessId(oldProcessId)
                    .setAssignedTasks(staticAssignedTasks)
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(dynamicMemberId)
                    .setMemberEpoch(groupEpoch)
                    .setPreviousMemberEpoch(groupEpoch - 1)
                    .setAssignedTasks(dynamicAssignedTasks)
                    .build())
                .withTargetAssignment(oldStaticMemberId, oldStaticTargetAssignment)
                .withTargetAssignment(dynamicMemberId, oldDynamicTargetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        // static member try to rejoin with new process id.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
            staticJoinHeartbeat(groupId, rejoinStaticMemberId, staticInstanceId, newProcessId)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(rejoinStaticMemberId)
                .setMemberEpoch(bumpedGroupEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(topic.responseTasks(0))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()),
            rejoinResult.response().data()
        );

        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);

        // group epoch should be bumped up.
        assertEquals(bumpedGroupEpoch, group.groupEpoch());

        // static member should be replaced.
        assertFalse(group.hasMember(oldStaticMemberId));
        assertTrue(group.hasMember(rejoinStaticMemberId));
        assertEquals(rejoinStaticMemberId, group.staticMember(staticInstanceId).memberId());

        // Because group epoch is bumped up, target assignment should be recomputed.
        assertEquals(newStaticTargetAssignment, group.targetAssignment(rejoinStaticMemberId, Optional.of(staticInstanceId)));
        assertEquals(newDynamicTargetAssignment, group.targetAssignment(dynamicMemberId, Optional.empty()));
        assertEquals(dynamicAssignedTasks, group.getMemberOrThrow(dynamicMemberId).assignedTasks());
        assertEquals(groupEpoch, group.getMemberOrThrow(dynamicMemberId).memberEpoch());

        StreamsGroupMember expectedCopiedStaticMember = streamsGroupMemberBuilderWithDefaults(rejoinStaticMemberId, staticInstanceId)
            .setProcessId(oldProcessId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setAssignedTasks(staticAssignedTasks)
            .build();

        StreamsGroupMember expectedUpdatedStaticMember = streamsGroupMemberBuilderWithDefaults(rejoinStaticMemberId, staticInstanceId)
            .setProcessId(newProcessId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setAssignedTasks(staticAssignedTasks)
            .build();

        StreamsGroupMember expectedReconciledStaticMember = streamsGroupMemberBuilderWithDefaults(rejoinStaticMemberId, staticInstanceId)
            .setProcessId(newProcessId)
            .setMemberEpoch(bumpedGroupEpoch)
            .setPreviousMemberEpoch(0)
            .setAssignedTasks(mkTasksTupleWithEpochs(
                TaskAssignmentTestUtil.TaskRole.ACTIVE,
                mkTasksWithEpochs(subtopologyId, Map.of(0, groupEpoch))
            ))
            .build();
        
        List<CoordinatorRecord> expectedRecordsBeforeRecomputedAssignments = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, oldStaticMemberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, oldStaticMemberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, oldStaticMemberId),

            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedCopiedStaticMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, rejoinStaticMemberId,
                oldStaticTargetAssignment),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedCopiedStaticMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedUpdatedStaticMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId,
                bumpedGroupEpoch,
                topic.metadataHash(),
                0,
                getDefaultAssignmentConfigs(),
                -1,
                -1
            )
        );

        List<CoordinatorRecord> expectedRecomputedTargetAssignmentRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, rejoinStaticMemberId,
                newStaticTargetAssignment),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, dynamicMemberId,
                newDynamicTargetAssignment)
        );

        List<CoordinatorRecord> expectedRecordsAfterRecomputedAssignments = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, bumpedGroupEpoch,
                context.time.milliseconds()),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedReconciledStaticMember)
        );

        assertRecordsEquals(
            expectedRecordsBeforeRecomputedAssignments,
            rejoinResult.records().subList(0, 8)
        );
        assertUnorderedRecordsEquals(
            List.of(expectedRecomputedTargetAssignmentRecords),
            rejoinResult.records().subList(8, 10)
        );
        assertRecordsEquals(
            expectedRecordsAfterRecomputedAssignments,
            rejoinResult.records().subList(10, 12)
        );

        // dynamic member send a heartbeat and reconciles to the new target assignment.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> dynamicHeartbeatResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, dynamicMemberId, null, groupEpoch)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(dynamicMemberId)
                .setMemberEpoch(bumpedGroupEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(topic.responseTasks(1, 2, 3))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()),
            dynamicHeartbeatResult.response().data()
        );

        StreamsGroupMember expectedUpdatedDynamicMember = streamsGroupMemberBuilderWithDefaults(dynamicMemberId)
            .setMemberEpoch(bumpedGroupEpoch)
            .setPreviousMemberEpoch(groupEpoch)
            .setAssignedTasks(mkTasksTupleWithEpochs(
                TaskAssignmentTestUtil.TaskRole.ACTIVE,
                mkTasksWithEpochs(subtopologyId, Map.of(
                    1, bumpedGroupEpoch,
                    2, groupEpoch,
                    3, groupEpoch
                ))
            ))
            .build();

        List<CoordinatorRecord> expectedRecordsAfterDynamicHeartbeat = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedUpdatedDynamicMember)
        );
        assertRecordsEquals(expectedRecordsAfterDynamicHeartbeat, dynamicHeartbeatResult.records());

        assertEquals(expectedUpdatedDynamicMember.assignedTasks(), group.getMemberOrThrow(dynamicMemberId).assignedTasks());
        assertEquals(bumpedGroupEpoch, group.getMemberOrThrow(dynamicMemberId).memberEpoch());
        assertEquals(newDynamicTargetAssignment, group.targetAssignment(dynamicMemberId, Optional.empty()));
    }

    @Test
    public void testStaticRejoinWithSameProcessIdDoesNotBumpEpochAndDynamicHeartbeatRemainsNoOpInMixedGroup() {
        // Scenario:
        // There are 2 members. 
        //   - static member left temporarily with -2 epoch.
        //   - dynamic member alive in group.
        // When the same static instance rejoins with the same processId, the coordinator
        // does not bump the group epoch or recompute the target assignment.
        // When the dynamic member sends a heartbeat, there is still no new assignment.

        int groupEpoch = DEFAULT_GROUP_EPOCH;
        String groupId = "fooup";

        String oldStaticMemberId = Uuid.randomUuid().toString();
        String newStaticMemberId = Uuid.randomUuid().toString();
        String staticInstanceId = Uuid.randomUuid().toString();
        String dynamicMemberId = Uuid.randomUuid().toString();

        String staticProcessId = "static-process-id";
        String dynamicProcessId = "dynamic-process-id";

        StreamsGroupTestUtil.StreamsTopicFixture topic = streamsTopicFixture("subtopology-1", "foo", 4);

        TasksTupleWithEpochs staticAssignedTasks = topic.assignedTasks(groupEpoch, 0, 1);
        TasksTuple staticTargetAssignment = topic.targetAssignment(0, 1);

        TasksTupleWithEpochs dynamicAssignedTasks = topic.assignedTasks(groupEpoch, 2, 3);
        TasksTuple dynamicTargetAssignment = topic.targetAssignment(2, 3);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(oldStaticMemberId, staticInstanceId)
                    .setProcessId(staticProcessId)
                    .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(groupEpoch)
                    .setAssignedTasks(staticAssignedTasks)
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(dynamicMemberId)
                    .setProcessId(dynamicProcessId)
                    .setMemberEpoch(groupEpoch)
                    .setPreviousMemberEpoch(groupEpoch - 1)
                    .setAssignedTasks(dynamicAssignedTasks)
                    .build())
                .withTargetAssignment(oldStaticMemberId, staticTargetAssignment)
                .withTargetAssignment(dynamicMemberId, dynamicTargetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        // static member try to rejoin with same process id.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
            staticJoinHeartbeat(groupId, newStaticMemberId, staticInstanceId, staticProcessId)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(newStaticMemberId)
                .setMemberEpoch(groupEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(topic.responseTasks(0, 1))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()),
            rejoinResult.response().data()
        );

        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);

        // no group epoch bump up.
        assertEquals(groupEpoch, group.groupEpoch());
        assertFalse(group.hasMember(oldStaticMemberId));
        assertTrue(group.hasMember(newStaticMemberId));
        assertTrue(group.hasMember(dynamicMemberId));
        assertEquals(newStaticMemberId, group.staticMember(staticInstanceId).memberId());
        assertEquals(staticTargetAssignment, group.targetAssignment(newStaticMemberId, Optional.of(staticInstanceId)));
        assertEquals(dynamicTargetAssignment, group.targetAssignment(dynamicMemberId, Optional.empty()));

        StreamsGroupMember expectedCopiedStaticMember = streamsGroupMemberBuilderWithDefaults(newStaticMemberId, staticInstanceId)
            .setProcessId(staticProcessId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setAssignedTasks(staticAssignedTasks)
            .build();

        StreamsGroupMember expectedRejoinedStaticMember = streamsGroupMemberBuilderWithDefaults(newStaticMemberId, staticInstanceId)
            .setProcessId(staticProcessId)
            .setMemberEpoch(groupEpoch)
            .setPreviousMemberEpoch(0)
            .setAssignedTasks(staticAssignedTasks)
            .build();

        // no new target assignment.
        List<CoordinatorRecord> expectedRejoinRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, oldStaticMemberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, oldStaticMemberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, oldStaticMemberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedCopiedStaticMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, newStaticMemberId, staticTargetAssignment),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedCopiedStaticMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedRejoinedStaticMember)
        );

        assertRecordsEquals(expectedRejoinRecords, rejoinResult.records());

        // dynamic member send a heartbeat request
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> dynamicHeartbeatResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, dynamicMemberId, null, groupEpoch)
        );

        // no new target assignment.
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(dynamicMemberId)
                .setMemberEpoch(groupEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null),
            dynamicHeartbeatResult.response().data());
        assertTrue(dynamicHeartbeatResult.records().isEmpty());

        assertEquals(dynamicAssignedTasks, group.getMemberOrThrow(dynamicMemberId).assignedTasks());
        assertEquals(groupEpoch, group.getMemberOrThrow(dynamicMemberId).memberEpoch());
        assertEquals(dynamicTargetAssignment, group.targetAssignment(dynamicMemberId, Optional.empty()));
    }

    @Test
    public void testOldStaticMemberIdIsFencedAfterReplacementInMixedGroup() {
        // Scenario:
        // There are 2 members.
        //   - static member left temporarily with -2 epoch.
        //   - dynamic member alive in group.
        // When the static member rejoins with a new memberId, the old memberId is fenced.
        int groupEpoch = DEFAULT_GROUP_EPOCH;
        String groupId = "fooup";

        String oldStaticMemberId = Uuid.randomUuid().toString();
        String newStaticMemberId = Uuid.randomUuid().toString();
        String staticInstanceId = Uuid.randomUuid().toString();
        String dynamicMemberId = Uuid.randomUuid().toString();

        String staticProcessId = "static-process-id";
        String dynamicProcessId = "dynamic-process-id";

        StreamsGroupTestUtil.StreamsTopicFixture topic = streamsTopicFixture("subtopology-1", "foo", 4);

        TasksTupleWithEpochs staticAssignedTasks = topic.assignedTasks(groupEpoch, 0, 1);
        TasksTuple staticTargetAssignment = topic.targetAssignment(0, 1);

        TasksTupleWithEpochs dynamicAssignedTasks = topic.assignedTasks(groupEpoch, 2, 3);
        TasksTuple dynamicTargetAssignment = topic.targetAssignment(2, 3);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(oldStaticMemberId, staticInstanceId)
                    .setProcessId(staticProcessId)
                    .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(groupEpoch)
                    .setAssignedTasks(staticAssignedTasks)
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(dynamicMemberId)
                    .setProcessId(dynamicProcessId)
                    .setMemberEpoch(groupEpoch)
                    .setPreviousMemberEpoch(groupEpoch - 1)
                    .setAssignedTasks(dynamicAssignedTasks)
                    .build())
                .withTargetAssignment(oldStaticMemberId, staticTargetAssignment)
                .withTargetAssignment(dynamicMemberId, dynamicTargetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();
        
        // static member try to rejoin with new member id.
        context.streamsGroupHeartbeat(
            staticJoinHeartbeat(groupId, newStaticMemberId, staticInstanceId, staticProcessId)
        );

        // stale static member send a heartbeat with stale member id. 
        assertThrows(FencedInstanceIdException.class, () ->
            context.streamsGroupHeartbeat(staticHeartbeat(groupId, oldStaticMemberId, staticInstanceId, groupEpoch))
        );

        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
        assertFalse(group.hasMember(oldStaticMemberId));
        assertTrue(group.hasMember(newStaticMemberId));
        assertTrue(group.hasMember(dynamicMemberId));
        assertEquals(newStaticMemberId, group.staticMember(staticInstanceId).memberId());
    }
}