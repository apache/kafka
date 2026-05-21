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
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.coordinator.group.StreamsGroupTestUtil.StreamsTopicFixture;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataValue;
import org.apache.kafka.coordinator.group.streams.MemberState;
import org.apache.kafka.coordinator.group.streams.MockTaskAssignor;
import org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupBuilder;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.TasksTuple;
import org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordsEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertResponseEquals;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupSessionTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_PROCESS_ID;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.contextWithStreamsGroup;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.getDefaultAssignmentConfigs;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.heartbeatResponseWithActiveTasks;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.heartbeatResponseWithNullTasks;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.resetAssignedTasksEpochsToZero;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticHeartbeat;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticJoinHeartbeat;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticLeaveResponse;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticLeaveResponseWithNullTasks;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.streamsGroupMemberBuilderWithDefaults;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.streamsTopicFixture;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamsGroupStaticMemberGroupMetadataManagerTest {

    private static final int DEFAULT_MEMBER_EPOCH = 10;
    private static final int DEFAULT_GROUP_EPOCH = 10;

    @Test
    public void testUnknownStaticMemberLeaveStreamsGroup() {
        StaticMemberFixtureWith2Members fixture = new StaticMemberFixtureWith2Members(2);

        fixture.assertLeaveFails(
                fixture.staticLeaveRequest("unknown-member-id", "unknown-instance-id"),
                UnknownMemberIdException.class
        );
    }

    @Test
    public void testStreamsStaticJoinWithNewInstanceAtMaxSizeThrowsGroupMaxSizeReached() {
        // With max.size=2 already reached, 
        // joining with a new static instanceId must throw GroupMaxSizeReachedException.
        int streamsGroupMaxSize = 2;
        StaticMemberFixtureWith2Members fixture = new StaticMemberFixtureWith2Members(streamsGroupMaxSize);

        fixture.assertJoinFails(
                fixture.newMemberJoinsWithNewInstanceId(),
                GroupMaxSizeReachedException.class
        );
    }

    @Test
    public void testStreamsStaticRejoinWithLeaveGroupStaticEpochAtMaxSizeSucceeds() {
        // If a static member is in leave epoch (-2), 
        // rejoining with the same memberId/instanceId is allowed even at max size.
        int streamsGroupMaxSize = 2;
        StaticMemberFixtureWith2Members fixture = new StaticMemberFixtureWith2Members(streamsGroupMaxSize);

        fixture.assertJoinSucceeds(
                fixture.leftMemberRejoinsWithSameInstanceId()
        );
    }

    @Test
    public void testStreamsStaticJoinWithUnreleasedInstanceThrowsUnreleasedInstanceIdAtMaxSize() {
        // If an active static member (epoch=10) still owns the instanceId, 
        // a different memberId joining with that instanceId must fail even at max size.
        int streamsGroupMaxSize = 2;
        StaticMemberFixtureWith2Members fixture = new StaticMemberFixtureWith2Members(streamsGroupMaxSize);

        fixture.assertJoinFails(
                fixture.newMemberJoinsWithActiveInstanceId(),
                UnreleasedInstanceIdException.class
        );
    }

    @ParameterizedTest
    @MethodSource("staticMemberReusedInstanceErrorCases")
    public void testStaticMemberSendHeartbeatWithVariousEpochThenThrowError(int whenMemberEpoch, Class<? extends Exception> expectedException) {
        StaticMemberFixtureWith2Members fixture = new StaticMemberFixtureWith2Members(3);
        // WHEN: same instance id is reused with mismatched member identity/epoch. THEN: expected exception is thrown.
        fixture.assertHeartbeatFails(
                fixture.newMemberHeartbeatsWithActiveInstanceId(whenMemberEpoch),
                expectedException
        );
    }

    private static Stream<Arguments> staticMemberReusedInstanceErrorCases() {
        return Stream.of(
                Arguments.of(0, UnreleasedInstanceIdException.class), // static member try to join when static member already existed, then throw UnreleasedInstanceIdException.
                Arguments.of(1000, FencedInstanceIdException.class)   // static member try to send bigger epoch when static member already existed, then throw FencedInstanceIdException.
        );
    }

    @Test
    public void testStaticMemberJoinThenRevokeAndReceiveTasks() {
        int enoughMaxSize = 100;
        testStaticMemberJoinThenRevokeAndReceiveTasksWith2Members(enoughMaxSize);
    }

    @Test
    public void testStaticMemberJoinThenRevokeAndReceiveTasksInMaxSizeBoundary() {
        int boundarySize = 2;
        testStaticMemberJoinThenRevokeAndReceiveTasksWith2Members(boundarySize);
    }

    private void testStaticMemberJoinThenRevokeAndReceiveTasksWith2Members(int maxSize) {
        String groupId = "fooup";

        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String otherMemberId2 = Uuid.randomUuid().toString();

        String instanceId1 = Uuid.randomUuid().toString();
        String instanceId2 = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, maxSize)
                .withStreamsGroupTaskAssignors(List.of(assignor))
                .withMetadataImage(topic.metadataImage())
                .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                        .withMember(StreamsGroupTestUtil.streamsGroupMemberBuilderWithDefaults(memberId1)
                                .setInstanceId(instanceId1)
                                .setMemberEpoch(10)
                                .setPreviousMemberEpoch(9)
                                .setAssignedTasks(topic.assignedTasks(10, 0, 1, 2, 3))
                                .build())
                        .withTargetAssignment(memberId1, topic.targetAssignment(0, 1, 2, 3))
                        .withTargetAssignmentEpoch(10)
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                        .withMetadataHash(topic.metadataHash())
                        .withValidatedTopologyEpoch(0)
                )
                .build();

        // Next target assignment after member2 joins.
        assignor.prepareGroupAssignment(Map.of(
                memberId1, topic.targetAssignment(0, 1),
                memberId2, topic.targetAssignment(2, 3)
        ));

        // 1) Static member2 joins. It gets no active tasks yet because member1 still owns them.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> joinResult = context.streamsGroupHeartbeat(
                staticJoinHeartbeat(groupId, memberId2, instanceId2, topic)
        );

        assertResponseEquals(
                heartbeatResponseWithActiveTasks(memberId2, 11, List.of()),
                joinResult.response().data()
        );

        // 2) member1 receives revocation instruction: keep only [0,1].
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> revokeInstructionResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setInstanceId(instanceId1)
                        .setMemberId(memberId1)
                        .setMemberEpoch(10)
        );

        assertResponseEquals(heartbeatResponseWithActiveTasks(memberId1, 10, topic, 0, 1), revokeInstructionResult.response().data());

        // 3) member1 acknowledges revocation by reporting owned active tasks [0,1].
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> revokeAckResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId1, instanceId1, 10)
                        .setActiveTasks(List.of(
                                new StreamsGroupHeartbeatRequestData.TaskIds()
                                        .setSubtopologyId("subtopology1")
                                        .setPartitions(List.of(0, 1))
                        ))
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
        );

        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(memberId1)
                        .setMemberEpoch(11)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setStatus(List.of()),
                revokeAckResult.response().data()
        );

        // 4) member2 heartbeats again and now receives [2,3].
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> member2ReceiveResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId2, instanceId2, 11)
        );

        assertResponseEquals(heartbeatResponseWithActiveTasks(memberId2, 11, topic, 2, 3), member2ReceiveResult.response().data());

        // 5) member2 leave.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> member2LeaveResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId2, instanceId2, LEAVE_GROUP_STATIC_MEMBER_EPOCH)
        );

        assertResponseEquals(
                staticLeaveResponseWithNullTasks(memberId2, LEAVE_GROUP_STATIC_MEMBER_EPOCH).setHeartbeatIntervalMs(0),
                member2LeaveResult.response().data()
        );

        // 6) member2 re-join with other memberId.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> member2rejoinResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, otherMemberId2, instanceId2, JOIN_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(
                heartbeatResponseWithActiveTasks(otherMemberId2, 11, topic, 2, 3),
                member2rejoinResult.response().data()
        );
    }

    @Test
    public void testStaticMemberRejoinWithUpdatedProcessIdBumpsStreamsGroupEpoch() {
        String groupId = "fooup";
        int groupEpoch = DEFAULT_GROUP_EPOCH;
        int bumpedGroupEpoch = groupEpoch + 1;

        String oldMemberId = Uuid.randomUuid().toString();
        String rejoinMemberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        String oldProcessId = "old-process-id";
        String newProcessId = "new-process-id";

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(groupEpoch, 0, 1, 2, 3);
        TasksTuple targetAssignment = topic.targetAssignment(0, 1, 2, 3);

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, assignor, group -> group
                .withMember(streamsGroupMemberBuilderWithDefaults(oldMemberId, instanceId)
                        .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                        .setPreviousMemberEpoch(groupEpoch)
                        .setProcessId(oldProcessId)
                        .setAssignedTasks(assignedTasks)
                        .build())
                .withTargetAssignment(oldMemberId, targetAssignment));

        assignor.prepareGroupAssignment(Map.of(rejoinMemberId, targetAssignment));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticJoinHeartbeat(groupId, rejoinMemberId, instanceId, newProcessId)
        );

        assertEquals(rejoinMemberId, result.response().data().memberId());
        assertEquals(bumpedGroupEpoch, result.response().data().memberEpoch());

        CoordinatorRecord metadataRecord = result.records().stream()
                .filter(record -> record.key() instanceof StreamsGroupMetadataKey)
                .findFirst()
                .orElse(null);

        assertNotNull(metadataRecord, "Expected a StreamsGroupMetadata record when static member config changes.");
        StreamsGroupMetadataValue metadataValue = (StreamsGroupMetadataValue) metadataRecord.value().message();
        assertEquals(bumpedGroupEpoch, metadataValue.epoch());

        assertTrue(result.records().contains(
                StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                        groupId,
                        bumpedGroupEpoch,
                        topic.metadataHash(),
                        0,
                        getDefaultAssignmentConfigs()
                )
        ));
    }

    @Test
    public void testStaticMemberLeaveWithMinusOneFencesMemberAndBumpsStreamsGroupEpoch() {
        String groupId = "fooup";
        int groupEpoch = DEFAULT_GROUP_EPOCH;
        int bumpedGroupEpoch = groupEpoch + 1;

        String memberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(groupEpoch, 0, 1, 2, 3);
        TasksTuple targetAssignment = topic.targetAssignment(0, 1, 2, 3);

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                        .setMemberEpoch(groupEpoch)
                        .setPreviousMemberEpoch(groupEpoch - 1)
                        .setAssignedTasks(assignedTasks)
                        .build())
                .withTargetAssignment(memberId, targetAssignment));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId, instanceId, LEAVE_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(staticLeaveResponse(memberId, LEAVE_GROUP_MEMBER_EPOCH), result.response().data());

        assertRecordsEquals(
                List.of(
                        StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, bumpedGroupEpoch, topic.metadataHash(), 0, getDefaultAssignmentConfigs()),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, bumpedGroupEpoch, 0L)
                ),
                result.records()
        );
    }

    @Test
    public void testStaticMemberLeaveWithLeaveGroupStaticMemberEpoch() {
        // GIVEN
        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(groupEpoch, 0, 1, 2, 3);
        TasksTupleWithEpochs pendingRevocationTasks = topic.assignedTasks(groupEpoch, 2, 3);

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                        .setMemberEpoch(memberEpoch)
                        .setPreviousMemberEpoch(memberEpoch - 1)
                        .setAssignedTasks(assignedTasks)
                        .setTasksPendingRevocation(pendingRevocationTasks)
                        .build())
                .withTargetAssignment(memberId, topic.targetAssignment(0, 1, 2, 3)));

        // WHEN
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId, instanceId, leaveEpoch)
        );

        // THEN
        assertResponseEquals(staticLeaveResponse(memberId, leaveEpoch), result.response().data());

        // No group epoch bump.
        // Member epoch should be -2.
        // task still remain. 
        // pendingRevocationTasks should be EMPTY.
        StreamsGroupMember expectedMemberInResponse = streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                .setMemberEpoch(leaveEpoch)
                .setPreviousMemberEpoch(memberEpoch - 1)
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .setAssignedTasks(resetAssignedTasksEpochsToZero(assignedTasks))
                .build();
        assertRecordsEquals(
                List.of(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMemberInResponse)),
                result.records()
        );
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());
    }

    @Test
    public void testStaticMemberLeaveWithLeaveGroupStaticMemberEpochThenShouldBeIdempotence() {
        // GIVEN
        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(groupEpoch, 0, 1, 2, 3);
        TasksTuple targetAssignment = topic.targetAssignment(0, 1, 2, 3);

        StreamsGroupMember alreadyLeftStaticMember = streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                .setMemberEpoch(leaveEpoch)
                .setPreviousMemberEpoch(memberEpoch - 1)
                .setAssignedTasks(resetAssignedTasksEpochsToZero(assignedTasks))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(alreadyLeftStaticMember)
                .withTargetAssignment(memberId, targetAssignment));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result =
                context.streamsGroupHeartbeat(staticHeartbeat(groupId, memberId, instanceId, leaveEpoch));


        // THEN
        assertResponseEquals(staticLeaveResponse(memberId, leaveEpoch), result.response().data());

        assertRecordsEquals(
                List.of(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, alreadyLeftStaticMember)),
                result.records()
        );
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());
    }

    @Test
    public void testStaticMemberLeaveWithLeaveGroupStaticMemberEpochAndRejoinWithNewMemberId() {
        String instanceId = Uuid.randomUuid().toString();
        String oldMemberId = Uuid.randomUuid().toString();
        String newMemberId = Uuid.randomUuid().toString();

        verifyStaticMemberLeaveAndRejoinNoGroupBump(instanceId, oldMemberId, newMemberId);
    }

    @Test
    public void testStaticMemberLeaveWithLeaveGroupStaticMemberEpochAndRejoinWithSameMemberId() {
        String instanceId = Uuid.randomUuid().toString();
        String memberId = Uuid.randomUuid().toString();

        verifyStaticMemberLeaveAndRejoinNoGroupBump(instanceId, memberId, memberId);
    }

    private void verifyStaticMemberLeaveAndRejoinNoGroupBump(String instanceId, String oldMemberId, String newMemberId) {
        /*
         * Verifies:
         * 1. leave(-2) does not bump group epoch.
         * 2. rejoin restores member epoch and assignment.
         * 3. replacement/tombstone records are written as expected.
         */

        // GIVEN
        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";

        String subtopology1 = "subtopology1";
        StreamsTopicFixture topic = streamsTopicFixture(subtopology1, "foo", 4);

        // GIVEN Task
        TasksTupleWithEpochs givenAssignedTasks = topic.assignedTasks(memberEpoch, 0, 1, 2, 3);
        TasksTuple givenTargetAssignment = topic.targetAssignment(0, 1, 2, 3);

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(streamsGroupMemberBuilderWithDefaults(oldMemberId, instanceId)
                        .setMemberEpoch(memberEpoch)
                        .setPreviousMemberEpoch(memberEpoch - 1)
                        .setAssignedTasks(givenAssignedTasks)
                        .build())
                .withTargetAssignment(oldMemberId, givenTargetAssignment));

        // WHEN1 : normal heart beat.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> normalHeartbeatResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, oldMemberId, instanceId, memberEpoch)
        );

        // THEN1 : 
        // - all tasks should be null because assigned tasks unchanged.
        // - Keep the group epoch.
        assertResponseEquals(heartbeatResponseWithNullTasks(oldMemberId, memberEpoch), normalHeartbeatResult.response().data());
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());


        // WHEN2 : Stream Member leave with -2
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, oldMemberId, instanceId, leaveEpoch)
        );

        // THEN2
        // - Keep the group epoch.
        assertResponseEquals(staticLeaveResponseWithNullTasks(oldMemberId, leaveEpoch), leaveResult.response().data());
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());

        // WHEN3 : Streams Member rejoin with other memberId
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, newMemberId, instanceId, JOIN_GROUP_MEMBER_EPOCH)
        );

        // THEN3 : 
        // - Inherit previous member's member epoch, and assigned tasks.
        // - Keep the member epoch bump.
        // - Keep the group epoch bump.
        assertResponseEquals(heartbeatResponseWithActiveTasks(newMemberId, memberEpoch, topic, 0, 1, 2, 3), rejoinResult.response().data());
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());

        StreamsGroupMember newJoinStaticMember = streamsGroupMemberBuilderWithDefaults(newMemberId, instanceId)
                .setMemberEpoch(JOIN_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(JOIN_GROUP_MEMBER_EPOCH)
                .setAssignedTasks(resetAssignedTasksEpochsToZero(givenAssignedTasks))
                .build();

        StreamsGroupMember withPrevMemberId = streamsGroupMemberBuilderWithDefaults(newMemberId, instanceId)
                .setMemberEpoch(memberEpoch) // 0 -> 10
                .setPreviousMemberEpoch(0) //  0 -> 0
                .setAssignedTasks(resetAssignedTasksEpochsToZero(givenAssignedTasks))
                .build();

        assertRecordsEquals(
                List.of(
                        StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, oldMemberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, oldMemberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, oldMemberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, newJoinStaticMember),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, newJoinStaticMember.memberId(), givenTargetAssignment),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, newJoinStaticMember),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, withPrevMemberId)
                ),
                rejoinResult.records()
        );
    }

    @Test
    public void testStaticMemberLeaveWithLeaveGroupStaticMemberEpochFromUnrevokedState() {
        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String instanceId = Uuid.randomUuid().toString();
        String memberId = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(memberEpoch, 0, 1);
        TasksTupleWithEpochs tasksPendingRevocation = topic.assignedTasks(memberEpoch, 2, 3);
        TasksTuple targetAssignment = topic.targetAssignment(0, 1);

        StreamsGroupMember unrevokedMember = streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch - 1)
                .setState(MemberState.UNREVOKED_TASKS)
                .setAssignedTasks(assignedTasks)
                .setTasksPendingRevocation(tasksPendingRevocation)
                .build();

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(unrevokedMember)
                .withTargetAssignment(memberId, targetAssignment));

        // WHEN
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId, instanceId, leaveEpoch)
        );

        // THEN
        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                .setMemberEpoch(leaveEpoch)
                .setPreviousMemberEpoch(memberEpoch - 1)
                .setAssignedTasks(resetAssignedTasksEpochsToZero(assignedTasks))
                .build();

        assertResponseEquals(staticLeaveResponseWithNullTasks(memberId, leaveEpoch), result.response().data());
        assertRecordsEquals(
                List.of(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)),
                result.records()
        );
        assertEquals(MemberState.STABLE, context.streamsGroupMemberState(groupId, memberId));
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());
    }

    @Test
    public void testStaticMemberRejoinsAfterTemporaryLeave() {
        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String instanceId = Uuid.randomUuid().toString();
        String oldMemberId = Uuid.randomUuid().toString();
        String newMemberId = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(memberEpoch, 0, 1);
        TasksTuple targetAssignment = topic.targetAssignment(0, 1);

        StreamsGroupMember temporarilyLeftMember = streamsGroupMemberBuilderWithDefaults(oldMemberId, instanceId)
                .setMemberEpoch(leaveEpoch)
                .setPreviousMemberEpoch(memberEpoch - 1)
                .setAssignedTasks(resetAssignedTasksEpochsToZero(assignedTasks))
                .build();

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(temporarilyLeftMember)
                .withTargetAssignment(oldMemberId, targetAssignment));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, newMemberId, instanceId, JOIN_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(heartbeatResponseWithActiveTasks(newMemberId, memberEpoch, topic, 0, 1), result.response().data());
        assertEquals(MemberState.STABLE, context.streamsGroupMemberState(groupId, newMemberId));
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());
    }

    @Test
    public void testStaticMemberLeaveWithLeaveGroupStaticMemberEpochFromUnreleasedState() {
        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String instanceId = Uuid.randomUuid().toString();
        String memberId = Uuid.randomUuid().toString();
        String processId = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 3);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(memberEpoch, 0, 1);
        TasksTuple targetAssignment = topic.targetAssignment(0, 1, 2);

        StreamsGroupMember unreleasedMember = streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                .setProcessId(processId)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch - 1)
                .setState(MemberState.UNRELEASED_TASKS)
                .setAssignedTasks(assignedTasks)
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(unreleasedMember)
                .withTargetAssignment(memberId, targetAssignment));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId, instanceId, leaveEpoch)
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                .setProcessId(processId)
                .setMemberEpoch(leaveEpoch)
                .setPreviousMemberEpoch(memberEpoch - 1)
                .setState(MemberState.UNRELEASED_TASKS)
                .setAssignedTasks(resetAssignedTasksEpochsToZero(assignedTasks))
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();

        assertResponseEquals(staticLeaveResponseWithNullTasks(memberId, leaveEpoch), result.response().data());
        assertRecordsEquals(
                List.of(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)),
                result.records()
        );
        assertEquals(MemberState.UNRELEASED_TASKS, context.streamsGroupMemberState(groupId, memberId));
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());
    }

    @Test
    public void testStaticMemberRejoinsAfterTemporaryLeaveFromUnreleasedState() {
        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String instanceId = Uuid.randomUuid().toString();
        String oldMemberId = "old-member-id";
        String newMemberId = "new-member-id";
        String otherMemberId = "other-member-id";
        String oldProcessId = "old-process-id";
        String otherProcessId = "other-process-id";

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 3);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(memberEpoch, 0, 1);
        TasksTupleWithEpochs otherTasksPendingRevocation = topic.assignedTasks(memberEpoch, 2);
        TasksTuple targetAssignment = topic.targetAssignment(0, 1, 2);

        StreamsGroupMember temporarilyLeftMember = StreamsGroupTestUtil.streamsGroupMemberBuilderWithDefaults(oldMemberId, instanceId)
                .setProcessId(oldProcessId)
                .setMemberEpoch(leaveEpoch)
                .setPreviousMemberEpoch(memberEpoch - 1)
                .setState(MemberState.UNRELEASED_TASKS)
                .setAssignedTasks(assignedTasks)
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();

        StreamsGroupMember otherMember = StreamsGroupTestUtil.streamsGroupMemberBuilderWithDefaults(otherMemberId)
                .setProcessId(otherProcessId)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch - 1)
                .setState(MemberState.UNREVOKED_TASKS)
                .setTasksPendingRevocation(otherTasksPendingRevocation)
                .build();

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(temporarilyLeftMember)
                .withMember(otherMember)
                .withTargetAssignment(oldMemberId, targetAssignment)
                .withTargetAssignment(otherMemberId, TasksTuple.EMPTY));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, newMemberId, instanceId, JOIN_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(heartbeatResponseWithActiveTasks(newMemberId, memberEpoch, topic, 0, 1), result.response().data());
        assertEquals(MemberState.UNRELEASED_TASKS, context.streamsGroupMemberState(groupId, newMemberId));
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());
    }


    @Test
    public void testStaticMemberLeaveWithLeaveGroupStaticMemberEpochFromUnrevokedStateAllowsUnreleasedMemberToProgress() {
        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String instanceId = Uuid.randomUuid().toString();
        String leavingMemberId = Uuid.randomUuid().toString();
        String waitingMemberId = Uuid.randomUuid().toString();

        String subtopology1 = "subtopology1";
        StreamsTopicFixture topic = streamsTopicFixture(subtopology1, "foo", 3);

        // GIVEN Tasks
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(memberEpoch, 0, 1);
        TasksTupleWithEpochs tasksPendingRevocation = topic.assignedTasks(memberEpoch, 2);
        TasksTuple leavingTargetAssignment = topic.targetAssignment(0, 1);
        TasksTuple waitingTargetAssignment = topic.targetAssignment(2);

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(streamsGroupMemberBuilderWithDefaults(leavingMemberId, instanceId)
                        .setMemberEpoch(memberEpoch)
                        .setPreviousMemberEpoch(memberEpoch - 1)
                        .setState(MemberState.UNREVOKED_TASKS)
                        .setAssignedTasks(assignedTasks)
                        .setTasksPendingRevocation(tasksPendingRevocation)
                        .build())
                .withMember(StreamsGroupTestUtil.streamsGroupMemberBuilderWithDefaults(waitingMemberId)
                        .setMemberEpoch(memberEpoch)
                        .setPreviousMemberEpoch(memberEpoch)
                        .setState(MemberState.UNRELEASED_TASKS)
                        .build())
                .withTargetAssignment(leavingMemberId, leavingTargetAssignment)
                .withTargetAssignment(waitingMemberId, waitingTargetAssignment));

        // WHEN1 - leave
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, leavingMemberId, instanceId, leaveEpoch)
        );

        // THEN1
        StreamsGroupHeartbeatResponseData expectedLeavingResponse = staticLeaveResponseWithNullTasks(leavingMemberId, leaveEpoch);
        assertResponseEquals(expectedLeavingResponse, leaveResult.response().data());
        List<CoordinatorRecord> expectedRecordsTriggeredByLeave = List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId,
                        streamsGroupMemberBuilderWithDefaults(leavingMemberId, instanceId)
                                .setMemberEpoch(leaveEpoch)
                                .setPreviousMemberEpoch(memberEpoch - 1)
                                .setAssignedTasks(resetAssignedTasksEpochsToZero(assignedTasks))
                                .build()));
        assertRecordsEquals(expectedRecordsTriggeredByLeave, leaveResult.records());
        assertEquals(MemberState.STABLE, context.streamsGroupMemberState(groupId, leavingMemberId));

        // When2 - Waiting member send a heartbeat expecting get unreleased tasks.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> waitingMemberResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, waitingMemberId, null, memberEpoch)
        );

        // THEN2
        StreamsGroupHeartbeatResponseData expectedWaitngMemberResponse = heartbeatResponseWithActiveTasks(waitingMemberId, memberEpoch, topic, 2);
        assertResponseEquals(expectedWaitngMemberResponse, waitingMemberResult.response().data());

        List<CoordinatorRecord> expectedRecordsTriggeredByWaitngMember = List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId,
                        StreamsGroupTestUtil.streamsGroupMemberBuilderWithDefaults(waitingMemberId)
                                .setMemberEpoch(memberEpoch)
                                .setPreviousMemberEpoch(memberEpoch)
                                .setAssignedTasks(topic.assignedTasks(memberEpoch, 2))
                                .build())
        );
        assertRecordsEquals(expectedRecordsTriggeredByWaitngMember, waitingMemberResult.records());

        assertEquals(MemberState.STABLE, context.streamsGroupMemberState(groupId, waitingMemberId));
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());
    }


    @Test
    public void testStaticMemberLeaveWithLeaveGroupStaticMemberEpochAndRejoinAndOtherRackIdThenGroupBumpOccur() {
        // GIVEN
        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String rackId = Uuid.randomUuid().toString();
        String memberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);

        // GIVEN Task
        TasksTupleWithEpochs givenAssignedTasks = topic.assignedTasks(memberEpoch, 0, 1, 2, 3);
        TasksTuple givenTargetAssignment = topic.targetAssignment(0, 1, 2, 3);

        // GIVEN Assignor
        MockTaskAssignor assignor = new MockTaskAssignor("sticky");

        long groupMetadataHash = topic.metadataHash();

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, assignor, group -> group
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                        .setMemberEpoch(memberEpoch)
                        .setRackId(rackId)
                        .setPreviousMemberEpoch(memberEpoch - 1)
                        .setAssignedTasks(givenAssignedTasks)
                        .build())
                .withTargetAssignment(memberId, givenTargetAssignment));

        // WHEN1 : normal heart beat.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> normalHeartbeatResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId, instanceId, memberEpoch)
                        .setRackId(rackId)
        );

        // THEN1 : 
        // - all tasks should be null because assigned tasks unchanged.
        // - Keep the group epoch.
        assertResponseEquals(heartbeatResponseWithNullTasks(memberId, memberEpoch), normalHeartbeatResult.response().data());
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());


        // WHEN2 : Stream Member leave with -2
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId, instanceId, leaveEpoch)
                        .setRackId(rackId)
        );

        // THEN2
        // - Keep the group epoch.
        assertResponseEquals(staticLeaveResponseWithNullTasks(memberId, leaveEpoch), leaveResult.response().data());
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());

        // GIVEN3
        String newMemberId = Uuid.randomUuid().toString();
        String newRackId = Uuid.randomUuid().toString();
        assignor.prepareGroupAssignment(Map.of(newMemberId, givenTargetAssignment));

        int bumpedGroupEpoch = groupEpoch + 1;
        int bumpedMemberEpoch = memberEpoch + 1;

        // WHEN3 : Streams Member rejoin with other memberId and rackId
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, newMemberId, instanceId, JOIN_GROUP_MEMBER_EPOCH)
                        .setRackId(newRackId)
        );

        // THEN3 : 
        // - Inherit previous member's member epoch, and assigned tasks.
        // - member epoch should be bumped.
        // - group epoch should be bumped.
        assertResponseEquals(heartbeatResponseWithActiveTasks(newMemberId, bumpedMemberEpoch, topic, 0, 1, 2, 3), rejoinResult.response().data());
        assertEquals(bumpedGroupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());

        StreamsGroupMember transationStaticInitMember = streamsGroupMemberBuilderWithDefaults(newMemberId, instanceId)
                .setMemberEpoch(JOIN_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(JOIN_GROUP_MEMBER_EPOCH)
                .setRackId(rackId)
                .setAssignedTasks(resetAssignedTasksEpochsToZero(givenAssignedTasks))
                .build();

        StreamsGroupMember newJoinStaticMember = streamsGroupMemberBuilderWithDefaults(newMemberId, instanceId)
                .setMemberEpoch(JOIN_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(JOIN_GROUP_MEMBER_EPOCH)
                .setRackId(newRackId)
                .setAssignedTasks(resetAssignedTasksEpochsToZero(givenAssignedTasks))
                .build();

        StreamsGroupMember reconciledMember = streamsGroupMemberBuilderWithDefaults(newMemberId, instanceId)
                .setMemberEpoch(bumpedMemberEpoch)
                .setPreviousMemberEpoch(0)
                .setRackId(newRackId)
                .setAssignedTasks(resetAssignedTasksEpochsToZero(givenAssignedTasks))
                .build();

        assertRecordsEquals(
                List.of(
                        // From eplaceStreamsMembers
                        StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, transationStaticInitMember),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, transationStaticInitMember.memberId(), givenTargetAssignment),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, transationStaticInitMember),

                        // From hasStreamsMemberMetadataChanged
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, newJoinStaticMember),

                        StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, bumpedGroupEpoch, groupMetadataHash, 0, getDefaultAssignmentConfigs()),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, newJoinStaticMember.memberId(), givenTargetAssignment),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, bumpedGroupEpoch, context.time.milliseconds()),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, reconciledMember)
                ),
                rejoinResult.records()
        );
    }


    @Test
    public void testStaticMemberRejoinWritesReplacementRecordsInStreamsGroup() {
        int groupEpoch = DEFAULT_GROUP_EPOCH;


        String groupId = "fooup";
        String oldMemberId = Uuid.randomUuid().toString();
        String rejoinMemberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);
        TasksTuple oldTargetAssignment = topic.targetAssignment(0, 1, 2, 3);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(groupEpoch, 0, 1, 2, 3);

        StreamsGroupMember oldMember = streamsGroupMemberBuilderWithDefaults(oldMemberId, instanceId)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setPreviousMemberEpoch(groupEpoch)
                .setAssignedTasks(resetAssignedTasksEpochsToZero(assignedTasks))
                .build();

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(oldMember)
                .withTargetAssignment(oldMemberId, oldTargetAssignment));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticJoinHeartbeat(groupId, rejoinMemberId, instanceId, DEFAULT_PROCESS_ID)
        );

        assertEquals(rejoinMemberId, result.response().data().memberId());
        assertEquals(groupEpoch, result.response().data().memberEpoch());

        StreamsGroupMember expectedCopiedMember = new StreamsGroupMember.Builder(oldMember, rejoinMemberId)
                .setMemberEpoch(JOIN_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(JOIN_GROUP_MEMBER_EPOCH)
                .build();

        assertTrue(result.records().contains(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, oldMemberId)
        ));
        assertTrue(result.records().contains(
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, oldMemberId)
        ));
        assertTrue(result.records().contains(
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, oldMemberId)
        ));
        assertTrue(result.records().contains(
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedCopiedMember)
        ));
        assertTrue(result.records().contains(
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, rejoinMemberId, oldTargetAssignment)
        ));
        assertTrue(result.records().contains(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedCopiedMember)
        ));
    }

    @Test
    public void testStaticMemberLeaveWithMismatchedMemberIdThrowsFencedInstanceIdInStreamsGroup() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String differentMemberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                        .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                                .setMemberEpoch(10)
                                .setPreviousMemberEpoch(9)
                                .build())
                        .withTargetAssignmentEpoch(10)
                )
                .build();

        assertThrows(FencedInstanceIdException.class, () ->
                context.streamsGroupHeartbeat(
                        staticHeartbeat(groupId, differentMemberId, instanceId, LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                ));
    }

    @Test
    public void testUnknownStaticMemberHeartbeatWithPositiveEpochThrowsUnknownMemberIdInStreamsGroup() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();
        String unknownInstanceId = Uuid.randomUuid().toString();
        String unknownMemberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                        .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                                .setMemberEpoch(10)
                                .setPreviousMemberEpoch(9)
                                .build())
                        .withTargetAssignmentEpoch(10)
                )
                .build();

        UnknownMemberIdException e = assertThrows(UnknownMemberIdException.class, () ->
                context.streamsGroupHeartbeat(staticHeartbeat(groupId, unknownMemberId, unknownInstanceId, 1))
        );
        assertEquals(String.format("Instance id %s is unknown.", unknownInstanceId), e.getMessage());
    }

    @ParameterizedTest
    @MethodSource("ownedActiveTasksAtPreviousEpochCases")
    public void testStreamsStaticMemberHeartbeatWithPreviousEpochAndOwnedActiveTasks(
            List<Integer> requestAssignedTaskIds, Class<? extends Exception> expectedException
    ) {
        Integer[] givenAssignedTasksIds = new Integer[]{0, 1, 2, 3};

        verifyStreamsStaticMemberHeartbeatWithOwnedActiveTasksAtPreviousEpoch(
                givenAssignedTasksIds,
                requestAssignedTaskIds,
                expectedException
        );
    }

    private static Stream<Arguments> ownedActiveTasksAtPreviousEpochCases() {
        return Stream.of(
                Arguments.of(List.of(0, 1, 2), null), // Subset Owned Active Tasks
                Arguments.of(List.of(0, 1, 2, 3), null), // Exact Owned Active Tasks
                Arguments.of(List.of(0, 1, 2, 3, 4), FencedMemberEpochException.class) // Non Subset active tasks
        );
    }

    private void verifyStreamsStaticMemberHeartbeatWithOwnedActiveTasksAtPreviousEpoch(
            Integer[] givenTaskIds,
            List<Integer> requestAssignedTaskIds,
            Class<? extends Exception> expectedException) {
        int groupEpoch = 10;
        int partitionSize = 5;
        int currentMemberEpoch = 10;
        int previousMemberEpoch = 9;
        int requestMemberEpoch = 9;

        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        // GIVEN TASK and Topology
        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", partitionSize);
        TasksTupleWithEpochs givenAssignedTask = topic.assignedTasks(groupEpoch, givenTaskIds);

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                        .setMemberEpoch(currentMemberEpoch)
                        .setPreviousMemberEpoch(previousMemberEpoch)
                        .setAssignedTasks(givenAssignedTask)
                        .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                        .build())
                .withTargetAssignment(memberId, topic.targetAssignment(givenTaskIds)));

        // WHEN
        StreamsGroupHeartbeatRequestData requestData = staticHeartbeat(groupId, memberId, instanceId, requestMemberEpoch)
                .setProcessId("process-id")
                .setRebalanceTimeoutMs(1500)
                .setTopology(topic.topology())
                .setActiveTasks(topic.requestTasks(requestAssignedTaskIds))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of());

        // THEN
        if (expectedException != null) {
            assertThrows(expectedException, () -> context.streamsGroupHeartbeat(requestData));
        } else {
            assertDoesNotThrow(() -> context.streamsGroupHeartbeat(requestData));
        }
    }

    @Test
    public void testStreamsStaticMemberTemporaryLeaveSessionTimeoutExpiration() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);
        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(assignor))
                .withMetadataImage(topic.metadataImage())
                .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
                .build();

        assignor.prepareGroupAssignment(Map.of(memberId, topic.targetAssignment(0, 1, 2, 3)));

        // WHEN1 : static member joins (session timeout should be scheduled)
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> firstJoinResult = context.streamsGroupHeartbeat(
                staticJoinHeartbeat(groupId, memberId, instanceId, topic).setRebalanceTimeoutMs(90000)
        );

        // THEN1
        // - member epoch should be bumped up.
        // - session timeout should be 45000ms.
        assertEquals(2, firstJoinResult.response().data().memberEpoch());
        context.assertSessionTimeout(groupId, memberId, 45000);

        // WHEN2: static member leaves temporarily.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> temporaryLeaveResult = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId, instanceId, LEAVE_GROUP_STATIC_MEMBER_EPOCH)
        );

        // THEN2: 
        // member epoch should be -2.
        // session timeout still 45000ms.
        assertResponseEquals(staticLeaveResponse(memberId, LEAVE_GROUP_STATIC_MEMBER_EPOCH), temporaryLeaveResult.response().data());
        context.assertSessionTimeout(groupId, memberId, 45000);

        // WHEN3: no rejoin, session timeout expires.
        List<MockCoordinatorTimer.ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(45000 + 1);

        // THEN3 
        List<CoordinatorRecord> expectedRecords = List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, 3, topic.metadataHash(), 0, getDefaultAssignmentConfigs()),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 3, 0L)
        );
        assertEquals(
                List.of(new MockCoordinatorTimer.ExpiredTimeout<>(
                        groupSessionTimeoutKey(groupId, memberId),
                        new CoordinatorResult<>(expectedRecords)
                )),
                timeouts
        );
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testStaticMemberJoinEmptyStreamsGroupRegistersStaticMember1() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(List.of());
        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        assignor.prepareGroupAssignment(Map.of(memberId, TasksTuple.EMPTY));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withMetadataImage(new MetadataImageBuilder().buildCoordinatorMetadataImage())
                .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
                .withStreamsGroupTaskAssignors(List.of(assignor))
                .build();

        // There is no group at all.
        assertThrows(GroupIdNotFoundException.class, () ->
                context.groupMetadataManager.streamsGroup(groupId));

        // WHEN
        context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId, instanceId, JOIN_GROUP_MEMBER_EPOCH)
                        .setProcessId(DEFAULT_PROCESS_ID)
                        .setRebalanceTimeoutMs(1500)
                        .setTopology(topology)
                        .setActiveTasks(List.of())
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
        );

        // THEN
        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
        assertEquals(memberId, group.staticMember(instanceId).memberId());
        assertEquals(Optional.of(instanceId), group.getMemberOrThrow(memberId).instanceId());
    }

    @ParameterizedTest
    @MethodSource("userEndpointTestCases")
    public void testStaticMemberRejoinUpdatesUserEndpointInformationEpoch(
            StreamsGroupHeartbeatRequestData.Endpoint firstUserEndpoint,
            int firstExpectedUserEndpointEpoch,
            List<StreamsGroupHeartbeatResponseData.EndpointToPartitions> firstExpectedPartitionsByUserEndpoint,
            StreamsGroupMemberMetadataValue.Endpoint firstExpectedUserEndpointMetadata,

            StreamsGroupHeartbeatRequestData.Endpoint secondUserEndpoint,
            int secondExpectedUserEndpointEpoch,
            List<StreamsGroupHeartbeatResponseData.EndpointToPartitions> secondExpectedPartitionsByUserEndpoint,
            StreamsGroupMemberMetadataValue.Endpoint secondExpectedUserEndpointMetadata
    ) {
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;
        int bumpedEpoch = memberEpoch + 1;

        String groupId = "fooup";
        String instanceId = Uuid.randomUuid().toString();
        String oldMemberId = "old-member-id";
        String rejoinMemberId = "new-member-id";

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 3);
        TasksTuple targetAssignment = topic.targetAssignment(0, 1, 2);


        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        assignor.prepareGroupAssignment(Map.of(oldMemberId, topic.targetAssignment(0, 1, 2)));

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, assignor, 0, group -> group
                .withTargetAssignment(oldMemberId, targetAssignment));

        assertEquals(0, context.groupMetadataManager.streamsGroup(groupId).endpointInformationEpoch());

        // First Join -> First Input
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticJoinHeartbeat(groupId, oldMemberId, instanceId, topic)
                        .setUserEndpoint(firstUserEndpoint) // first input
        );

        // First Check
        assertResponseEquals(
                heartbeatResponseWithActiveTasks(oldMemberId, bumpedEpoch, topic, 0, 1, 2)
                        .setEndpointInformationEpoch(firstExpectedUserEndpointEpoch) // first endpoint epoch
                        .setPartitionsByUserEndpoint(firstExpectedPartitionsByUserEndpoint), // first partitions by user endpoint
                result.response().data()
        );

        if (firstExpectedUserEndpointMetadata != null) {
            assertEquals(firstExpectedUserEndpointMetadata, context.groupMetadataManager.streamsGroup(groupId).getMemberOrThrow(oldMemberId).userEndpoint().get());
        } else {
            assertTrue(context.groupMetadataManager.streamsGroup(groupId).getMemberOrThrow(oldMemberId).userEndpoint().isEmpty());
        }
        assertEquals(firstExpectedUserEndpointEpoch, context.groupMetadataManager.streamsGroup(groupId).endpointInformationEpoch());

        // static leave
        context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, oldMemberId, instanceId, StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH)
        );

        // second - static member rejoins
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
                staticJoinHeartbeat(groupId, rejoinMemberId, instanceId, topic)
                        .setUserEndpoint(secondUserEndpoint)
        );

        // second check.
        assertResponseEquals(
                heartbeatResponseWithActiveTasks(rejoinMemberId, bumpedEpoch, topic, 0, 1, 2)
                        .setEndpointInformationEpoch(secondExpectedUserEndpointEpoch)
                        .setPartitionsByUserEndpoint(secondExpectedPartitionsByUserEndpoint),
                rejoinResult.response().data()
        );

        if (secondExpectedUserEndpointMetadata != null) {
            assertEquals(secondExpectedUserEndpointMetadata, context.groupMetadataManager.streamsGroup(groupId).getMemberOrThrow(rejoinMemberId).userEndpoint().get());
        } else {
            assertTrue(context.groupMetadataManager.streamsGroup(groupId).getMemberOrThrow(rejoinMemberId).userEndpoint().isEmpty());
        }
        assertEquals(secondExpectedUserEndpointEpoch, context.groupMetadataManager.streamsGroup(groupId).endpointInformationEpoch());
    }

    private static Stream<Arguments> userEndpointTestCases() {
        return Stream.of(
                Arguments.of(
                        null, // firstInput
                        0, // first endpoint Epoch
                        null, // first partitionsByUserEndpoint 
                        null, // first group metadata userEndpoint
                        userEndpoint("bar.com", 8080), // second input
                        1, // second endpoint epoch
                        buildEndpoints("bar.com", 8080, "foo", List.of(0, 1, 2)), // second partitionsByUserEndpoint
                        userEndpointForMetadata("bar.com", 8080)
                ),
                Arguments.of(
                        null, // firstInput
                        0, // first endpoint Epoch
                        null, // first partitionsByUserEndpoint 
                        null, // first group metadata userEndpoint
                        null, // second input
                        0, // second endpoint epoch
                        null, // second partitionsByUserEndpoint
                        null
                ),
                Arguments.of(
                        userEndpoint("foo.com", 8080), // firstInput
                        1, // first endpoint Epoch
                        buildEndpoints("foo.com", 8080, "foo", List.of(0, 1, 2)), // first partitionsByUserEndpoint 
                        userEndpointForMetadata("foo.com", 8080), // first group metadata userEndpoint
                        null, // second input
                        2, // second endpoint epoch
                        List.of(), // second partitionsByUserEndpoint
                        null
                ),
                Arguments.of(
                        userEndpoint("foo.com", 8080), // firstInput
                        1, // first endpoint Epoch
                        buildEndpoints("foo.com", 8080, "foo", List.of(0, 1, 2)), // first partitionsByUserEndpoint 
                        userEndpointForMetadata("foo.com", 8080), // first group metadata userEndpoint
                        userEndpoint("foo.com", 8080), // second input
                        1, // second endpoint epoch
                        buildEndpoints("foo.com", 8080, "foo", List.of(0, 1, 2)), // second partitionsByUserEndpoint
                        userEndpointForMetadata("foo.com", 8080)
                ),
                Arguments.of(
                        userEndpoint("foo.com", 8080), // firstInput
                        1, // first endpoint Epoch
                        buildEndpoints("foo.com", 8080, "foo", List.of(0, 1, 2)), // first partitionsByUserEndpoint 
                        userEndpointForMetadata("foo.com", 8080), // first group metadata userEndpoint
                        userEndpoint("bar.com", 8080), // second input
                        2, // second endpoint epoch
                        buildEndpoints("bar.com", 8080, "foo", List.of(0, 1, 2)), // second partitionsByUserEndpoint
                        userEndpointForMetadata("bar.com", 8080)
                )
        );
    }

    private static class StaticMemberFixtureWith2Members {
        // a single static member is active, the other static member leaves with -2. 
        private static final String GROUP_ID = "streams-group";
        private static final int GROUP_EPOCH = 10;
        private static final int MEMBER_EPOCH = 10;
        private static final int PREVIOUS_MEMBER_EPOCH = 9;
        private static final int REBALANCE_TIMEOUT_MS = 1500;

        private final String activeMemberId = "active-member";
        private final String activeInstanceId = "active-instance";

        private final String leftMemberId = "left-member";
        private final String leftInstanceId = "left-instance";

        private final String newMemberId = "new-member";
        private final String newInstanceId = "new-instance";

        private final StreamsGroupHeartbeatRequestData.Topology topology =
                new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(List.of());

        private final GroupMetadataManagerTestContext context;

        private StaticMemberFixtureWith2Members(int streamsGroupMaxSize) {
            MockTaskAssignor assignor = new MockTaskAssignor("sticky");
            assignor.prepareGroupAssignment(Map.of(activeMemberId, TasksTuple.EMPTY));

            this.context = new GroupMetadataManagerTestContext.Builder()
                    .withStreamsGroupTaskAssignors(List.of(assignor))
                    .withMetadataImage(new MetadataImageBuilder().buildCoordinatorMetadataImage())
                    .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, streamsGroupMaxSize)
                    .withStreamsGroup(new StreamsGroupBuilder(GROUP_ID, GROUP_EPOCH)
                            .withMember(streamsGroupMemberBuilderWithDefaults(activeMemberId, activeInstanceId)
                                    .setMemberEpoch(MEMBER_EPOCH)
                                    .setPreviousMemberEpoch(PREVIOUS_MEMBER_EPOCH)
                                    .build())
                            .withMember(streamsGroupMemberBuilderWithDefaults(leftMemberId, leftInstanceId)
                                    .setMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                                    .setPreviousMemberEpoch(PREVIOUS_MEMBER_EPOCH)
                                    .build())
                            .withTargetAssignmentEpoch(GROUP_EPOCH)
                            .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                    )
                    .build();
        }

        private StreamsGroupHeartbeatRequestData newMemberJoinsWithNewInstanceId() {
            return joinRequest(newMemberId, newInstanceId);
        }

        private StreamsGroupHeartbeatRequestData leftMemberRejoinsWithSameInstanceId() {
            return joinRequest(leftMemberId, leftInstanceId);
        }

        private StreamsGroupHeartbeatRequestData newMemberJoinsWithActiveInstanceId() {
            return joinRequest(newMemberId, activeInstanceId);
        }

        private StreamsGroupHeartbeatRequestData newMemberHeartbeatsWithActiveInstanceId(int memberEpoch) {
            return request(newMemberId, activeInstanceId, memberEpoch);
        }

        private StreamsGroupHeartbeatRequestData joinRequest(String memberId, String instanceId) {
            return request(memberId, instanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH);
        }

        private StreamsGroupHeartbeatRequestData staticLeaveRequest(String memberId, String instanceId) {
            return request(memberId, instanceId, StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH);
        }

        private StreamsGroupHeartbeatRequestData request(String memberId, String instanceId, int memberEpoch) {
            return new StreamsGroupHeartbeatRequestData()
                    .setGroupId(GROUP_ID)
                    .setInstanceId(instanceId)
                    .setMemberId(memberId)
                    .setMemberEpoch(memberEpoch)
                    .setProcessId(DEFAULT_PROCESS_ID)
                    .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of());
        }

        private void assertJoinSucceeds(StreamsGroupHeartbeatRequestData request) {
            assertDoesNotThrow(() -> context.streamsGroupHeartbeat(request));
        }

        private void assertJoinFails(StreamsGroupHeartbeatRequestData request, Class<? extends Exception> expectedException) {
            assertHeartbeatFails(request, expectedException);
        }

        private void assertHeartbeatFails(StreamsGroupHeartbeatRequestData request, Class<? extends Exception> expectedException
        ) {
            assertThrows(expectedException, () -> context.streamsGroupHeartbeat(request));
        }

        private void assertLeaveFails(StreamsGroupHeartbeatRequestData request, Class<? extends Exception> expectedException
        ) {
            assertHeartbeatFails(request, expectedException);
        }
    }

    private static StreamsGroupHeartbeatRequestData.Endpoint userEndpoint(String host, int port) {
        return new StreamsGroupHeartbeatRequestData.Endpoint()
                .setHost(host)
                .setPort(port);
    }

    private static List<StreamsGroupHeartbeatResponseData.EndpointToPartitions> buildEndpoints(String host, int port, String topic, List<Integer> partitions) {
        List<StreamsGroupHeartbeatResponseData.EndpointToPartitions> endpoints = new ArrayList<>();
        endpoints.add(new StreamsGroupHeartbeatResponseData.EndpointToPartitions()
                .setUserEndpoint(new StreamsGroupHeartbeatResponseData.Endpoint()
                        .setHost(host)
                        .setPort(port))
                .setActivePartitions(List.of(topicPartition(topic, partitions))));
        return endpoints;
    }

    private static StreamsGroupMemberMetadataValue.Endpoint userEndpointForMetadata(String host, int port) {
        return new StreamsGroupMemberMetadataValue.Endpoint().setHost(host).setPort(port);
    }

    private static StreamsGroupHeartbeatResponseData.TopicPartition topicPartition(String topic, List<Integer> partitions) {
        return new StreamsGroupHeartbeatResponseData.TopicPartition().setTopic(topic).setPartitions(partitions);
    }

}