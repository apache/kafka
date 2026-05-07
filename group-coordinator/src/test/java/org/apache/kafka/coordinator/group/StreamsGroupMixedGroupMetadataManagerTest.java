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
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.coordinator.common.runtime.*;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataValue;
import org.apache.kafka.coordinator.group.streams.*;
import org.apache.kafka.coordinator.group.streams.MemberState;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.*;
import static org.apache.kafka.coordinator.group.Assertions.*;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupSessionTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ADDRESS;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ID;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_PROCESS_ID;
import static org.apache.kafka.coordinator.group.Utils.computeGroupHash;
import static org.apache.kafka.coordinator.group.Utils.computeTopicHash;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

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
                fixture.joiningNewStaticInstance(),
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
                fixture.rejoiningLeftStaticMember()
        );
    }

    @Test
    public void testStreamsStaticJoinWithUnreleasedInstanceThrowsUnreleasedInstanceIdAtMaxSize() {
        // If an active static member (epoch=10) still owns the instanceId, 
        // a different memberId joining with that instanceId must fail even at max size.
        int streamsGroupMaxSize = 2;
        StaticMemberFixtureWith2Members fixture = new StaticMemberFixtureWith2Members(streamsGroupMaxSize);
        
        fixture.assertJoinFails(
                fixture.joiningWithActiveInstance(),
                UnreleasedInstanceIdException.class
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
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(assignor))
                .withMetadataImage(topic.metadataImage)
                .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                        .withMember(streamsGroupMemberBuilderWithDefaults(oldMemberId, instanceId)
                                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                                .setPreviousMemberEpoch(groupEpoch)
                                .setProcessId(oldProcessId)
                                .setAssignedTasks(assignedTasks)
                                .build())
                        .withTargetAssignment(oldMemberId, targetAssignment)
                        .withTargetAssignmentEpoch(groupEpoch)
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology))
                        .withMetadataHash(topic.metadataHash)
                        .withValidatedTopologyEpoch(0)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
                )
                .build();

        assignor.prepareGroupAssignment(Map.of(rejoinMemberId, targetAssignment));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, rejoinMemberId, instanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                        .setProcessId(newProcessId)
                        .setActiveTasks(List.of())
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
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
                        topic.metadataHash,
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
        
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(topic.metadataImage)
                .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                        .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                                .setMemberEpoch(groupEpoch)
                                .setPreviousMemberEpoch(groupEpoch - 1)
                                .setAssignedTasks(assignedTasks)
                                .build())
                        .withTargetAssignment(memberId, targetAssignment)
                        .withTargetAssignmentEpoch(groupEpoch)
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(topic.metadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
                )
                .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, memberId, instanceId, LEAVE_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(staticLeaveResponse(memberId, LEAVE_GROUP_MEMBER_EPOCH), result.response().data());

        assertRecordsEquals(
                List.of(
                        StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, bumpedGroupEpoch, topic.metadataHash, 0, getDefaultAssignmentConfigs()),
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
        Map.Entry<String, Set<Integer>> assignedTaskEntries = topic.tasks(0, 1, 2, 3);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(groupEpoch, 0, 1, 2, 3);
        TasksTupleWithEpochs pendingRevocationTasks = topic.assignedTasks(groupEpoch, 2, 3);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(topic.metadataImage)
                .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                        .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                                .setMemberEpoch(memberEpoch)
                                .setPreviousMemberEpoch(memberEpoch - 1)
                                .setAssignedTasks(assignedTasks)
                                .setTasksPendingRevocation(pendingRevocationTasks)
                                .build())
                        .withTargetAssignment(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, assignedTaskEntries))
                        .withTargetAssignmentEpoch(groupEpoch)
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(topic.metadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
                .build();

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
                .setAssignedTasks(assignedTasks)
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
                .setAssignedTasks(assignedTasks)
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();
        

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(topic.metadataImage)
                .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                        .withMember(alreadyLeftStaticMember)
                        .withTargetAssignment(memberId, targetAssignment)
                        .withTargetAssignmentEpoch(groupEpoch)
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(topic.metadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
                )
                .build();

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
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(List.of(
                new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        // GIVEN Task
        Map.Entry<String, Set<Integer>> givenTaskEntries = TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3);
        TasksTupleWithEpochs givenAssignedTasks = mkTasksTupleWithCommonEpoch(TaskAssignmentTestUtil.TaskRole.ACTIVE, 10, givenTaskEntries);
        TasksTuple givenTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(TaskAssignmentTestUtil.TaskRole.ACTIVE, givenTaskEntries);
        List<StreamsGroupHeartbeatResponseData.TaskIds> taskIds = List.of(
                new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2, 3)
                        )
        );


        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 4)
                .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage)
        ));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(metadataImage)
                .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                        .withMember(streamsGroupMemberBuilderWithDefaults(oldMemberId, instanceId)
                                .setMemberEpoch(memberEpoch)
                                .setPreviousMemberEpoch(memberEpoch - 1)
                                .setAssignedTasks(givenAssignedTasks)
                                .build())
                        .withTargetAssignment(oldMemberId, givenTargetAssignment)
                        .withTargetAssignmentEpoch(groupEpoch)
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(groupMetadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
                )
                .build();

        // WHEN1 : normal heart beat.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> normalHeartbeatResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setInstanceId(instanceId)
                        .setMemberId(oldMemberId)
                        .setMemberEpoch(memberEpoch)
        );

        // THEN1 : 
        // - all tasks should be null because assigned tasks unchanged.
        // - Keep the group epoch.
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberEpoch(memberEpoch)
                        .setMemberId(oldMemberId)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(null)
                        .setWarmupTasks(null)
                        .setStandbyTasks(null),
                normalHeartbeatResult.response().data()
        );
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());


        // WHEN2 : Stream Member leave with -2
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setInstanceId(instanceId)
                        .setMemberId(oldMemberId)
                        .setMemberEpoch(leaveEpoch)
        );

        // THEN2
        // - Keep the group epoch.
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(oldMemberId)
                        .setMemberEpoch(leaveEpoch)
                        .setActiveTasks(null)
                        .setWarmupTasks(null)
                        .setStandbyTasks(null)
                        .setStatus(List.of()),
                leaveResult.response().data()
        );
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());

        // WHEN3 : Streams Member rejoin with other memberId
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setInstanceId(instanceId)
                        .setMemberId(newMemberId)
                        .setMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
        );

        // THEN3 : 
        // - Inherit previous member's member epoch, and assigned tasks.
        // - Keep the member epoch bump.
        // - Keep the group epoch bump.
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(newMemberId)
                        .setMemberEpoch(memberEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(taskIds)
                        .setWarmupTasks(List.of())
                        .setStandbyTasks(List.of()),
                rejoinResult.response().data()
        );
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());

        StreamsGroupMember newJoinStaticMember = streamsGroupMemberBuilderWithDefaults(newMemberId, instanceId)
                .setMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                .setAssignedTasks(givenAssignedTasks)
                .build();

        StreamsGroupMember withPrevMemberId = streamsGroupMemberBuilderWithDefaults(newMemberId, instanceId)
                .setMemberEpoch(memberEpoch) // 0 -> 10
                .setPreviousMemberEpoch(0) //  0 -> 0
                .setAssignedTasks(givenAssignedTasks)
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
                .setAssignedTasks(assignedTasks)
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
                .setAssignedTasks(assignedTasks)
                .build();

        GroupMetadataManagerTestContext context = contextWithStreamsGroup(groupId, groupEpoch, topic, group -> group
                .withMember(temporarilyLeftMember)
                .withTargetAssignment(oldMemberId, targetAssignment));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, newMemberId, instanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(newMemberId)
                        .setMemberEpoch(memberEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(topic.responseTasks(0, 1))
                        .setWarmupTasks(List.of())
                        .setStandbyTasks(List.of()),
                result.response().data()
        );
        assertEquals(
                org.apache.kafka.coordinator.group.streams.MemberState.STABLE,
                context.streamsGroupMemberState(groupId, newMemberId)
        );
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
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
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
                .setAssignedTasks(assignedTasks)
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

        StreamsGroupMember temporarilyLeftMember = streamsGroupMemberBuilderWithDefaults(oldMemberId, instanceId)
                .setProcessId(oldProcessId)
                .setMemberEpoch(leaveEpoch)
                .setPreviousMemberEpoch(memberEpoch - 1)
                .setState(MemberState.UNRELEASED_TASKS)
                .setAssignedTasks(assignedTasks)
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .build();

        StreamsGroupMember otherMember = streamsGroupMemberBuilderWithDefaults(otherMemberId)
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
                staticHeartbeat(groupId, newMemberId, instanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(newMemberId)
                        .setMemberEpoch(memberEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(topic.responseTasks(0, 1))
                        .setWarmupTasks(List.of())
                        .setStandbyTasks(List.of()),
                result.response().data()
        );
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
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(List.of(
                new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        // GIVEN Tasks
        Map.Entry<String, Set<Integer>> leavingAssignedTasks = TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1);
        Map.Entry<String, Set<Integer>> leavingRevokedTasks = TaskAssignmentTestUtil.mkTasks(subtopology1, 2);
        TasksTupleWithEpochs assignedTasks = mkTasksTupleWithCommonEpoch(
                TaskAssignmentTestUtil.TaskRole.ACTIVE, memberEpoch, leavingAssignedTasks
        );
        TasksTupleWithEpochs tasksPendingRevocation = mkTasksTupleWithCommonEpoch(
                TaskAssignmentTestUtil.TaskRole.ACTIVE, memberEpoch, leavingRevokedTasks
        );
        TasksTuple leavingTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(
                TaskAssignmentTestUtil.TaskRole.ACTIVE, leavingAssignedTasks
        );
        TasksTuple waitingTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(
                TaskAssignmentTestUtil.TaskRole.ACTIVE, leavingRevokedTasks
        );

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 3)
                .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage)
        ));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(metadataImage)
                .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                        .withMember(streamsGroupMemberBuilderWithDefaults(leavingMemberId, instanceId)
                                .setMemberEpoch(memberEpoch)
                                .setPreviousMemberEpoch(memberEpoch - 1)
                                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
                                .setAssignedTasks(assignedTasks)
                                .setTasksPendingRevocation(tasksPendingRevocation)
                                .build())
                        .withMember(streamsGroupMemberBuilderWithDefaults(waitingMemberId)
                                .setMemberEpoch(memberEpoch)
                                .setPreviousMemberEpoch(memberEpoch)
                                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
                                .build())
                        .withTargetAssignment(leavingMemberId, leavingTargetAssignment)
                        .withTargetAssignment(waitingMemberId, waitingTargetAssignment)
                        .withTargetAssignmentEpoch(groupEpoch)
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(groupMetadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
                )
                .build();

        // WHEN1 - leave
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setInstanceId(instanceId)
                        .setMemberId(leavingMemberId)
                        .setMemberEpoch(leaveEpoch)
        );

        // THEN1
        StreamsGroupHeartbeatResponseData expectedLeavingResponse = new StreamsGroupHeartbeatResponseData()
                .setMemberId(leavingMemberId)
                .setMemberEpoch(leaveEpoch)
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null)
                .setStatus(List.of());
        assertResponseEquals(expectedLeavingResponse, leaveResult.response().data());
        List<CoordinatorRecord> expectedRecordsTriggeredByLeave = List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId,
                        streamsGroupMemberBuilderWithDefaults(leavingMemberId, instanceId)
                                .setMemberEpoch(leaveEpoch)
                                .setPreviousMemberEpoch(memberEpoch - 1)
                                .setAssignedTasks(assignedTasks)
                                .build()));
        assertRecordsEquals(expectedRecordsTriggeredByLeave, leaveResult.records());
        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.STABLE, context.streamsGroupMemberState(groupId, leavingMemberId));

        // When2 - Waiting member send a heartbeat expecting get unreleased tasks.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> waitingMemberResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setMemberId(waitingMemberId)
                        .setMemberEpoch(memberEpoch)
        );

        // THEN2
        StreamsGroupHeartbeatResponseData expectedWaitngMemberResponse = new StreamsGroupHeartbeatResponseData()
                .setMemberId(waitingMemberId)
                .setMemberEpoch(memberEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setActiveTasks(List.of(
                        new StreamsGroupHeartbeatResponseData.TaskIds()
                                .setSubtopologyId(subtopology1)
                                .setPartitions(List.of(2))
                ))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of());
        assertResponseEquals(expectedWaitngMemberResponse, waitingMemberResult.response().data());


        List<CoordinatorRecord> expectedRecordsTriggeredByWaitngMember = List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId,
                        streamsGroupMemberBuilderWithDefaults(waitingMemberId)
                                .setMemberEpoch(memberEpoch)
                                .setPreviousMemberEpoch(memberEpoch)
                                .setAssignedTasks(mkTasksTupleWithCommonEpoch(
                                        TaskAssignmentTestUtil.TaskRole.ACTIVE, memberEpoch, leavingRevokedTasks
                                ))
                                .build()));
        assertRecordsEquals(expectedRecordsTriggeredByWaitngMember, waitingMemberResult.records());

        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.STABLE, context.streamsGroupMemberState(groupId, waitingMemberId));
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

        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(List.of(
                new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        // GIVEN Task
        Map.Entry<String, Set<Integer>> givenTaskEntries = TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3);
        TasksTupleWithEpochs givenAssignedTasks = mkTasksTupleWithCommonEpoch(TaskAssignmentTestUtil.TaskRole.ACTIVE, 10, givenTaskEntries);
        TasksTuple givenTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(TaskAssignmentTestUtil.TaskRole.ACTIVE, givenTaskEntries);
        List<StreamsGroupHeartbeatResponseData.TaskIds> taskIds = List.of(
                new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2, 3)
                        )
        );

        // GIVEN Assignor
        MockTaskAssignor assignor = new MockTaskAssignor("sticky");


        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 4)
                .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage)
        ));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(assignor))
                .withMetadataImage(metadataImage)
                .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                        .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                                .setMemberEpoch(memberEpoch)
                                .setRackId(rackId)
                                .setPreviousMemberEpoch(memberEpoch - 1)
                                .setAssignedTasks(givenAssignedTasks)
                                .build())
                        .withTargetAssignment(memberId, givenTargetAssignment)
                        .withTargetAssignmentEpoch(groupEpoch)
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(groupMetadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
                )
                .build();

        // WHEN1 : normal heart beat.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> normalHeartbeatResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setRackId(rackId)
                        .setInstanceId(instanceId)
                        .setMemberId(memberId)
                        .setMemberEpoch(memberEpoch)
        );

        // THEN1 : 
        // - all tasks should be null because assigned tasks unchanged.
        // - Keep the group epoch.
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberEpoch(memberEpoch)
                        .setMemberId(memberId)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(null)
                        .setWarmupTasks(null)
                        .setStandbyTasks(null),
                normalHeartbeatResult.response().data()
        );
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());


        // WHEN2 : Stream Member leave with -2
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setRackId(rackId)
                        .setInstanceId(instanceId)
                        .setMemberId(memberId)
                        .setMemberEpoch(leaveEpoch)
        );

        // THEN2
        // - Keep the group epoch.
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(memberId)
                        .setMemberEpoch(leaveEpoch)
                        .setActiveTasks(null)
                        .setWarmupTasks(null)
                        .setStandbyTasks(null)
                        .setStatus(List.of()),
                leaveResult.response().data()
        );
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());

        // GIVEN3
        String newMemberId = Uuid.randomUuid().toString();
        String newRackId = Uuid.randomUuid().toString();
        assignor.prepareGroupAssignment(Map.of(newMemberId, givenTargetAssignment));

        int bumpedGroupEpoch = groupEpoch + 1;
        int bumpedMemberEpoch = memberEpoch + 1;

        // WHEN3 : Streams Member rejoin with other memberId and rackId
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setRackId(newRackId)
                        .setInstanceId(instanceId)
                        .setMemberId(newMemberId)
                        .setMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
        );

        // THEN3 : 
        // - Inherit previous member's member epoch, and assigned tasks.
        // - member epoch should be bumped.
        // - group epoch should be bumped.
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(newMemberId)
                        .setMemberEpoch(bumpedMemberEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(taskIds)
                        .setWarmupTasks(List.of())
                        .setStandbyTasks(List.of()),
                rejoinResult.response().data()
        );
        assertEquals(bumpedGroupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());

        StreamsGroupMember transationStaticInitMember = streamsGroupMemberBuilderWithDefaults(newMemberId, instanceId)
                .setMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                .setRackId(rackId)
                .setAssignedTasks(givenAssignedTasks)
                .build();

        StreamsGroupMember newJoinStaticMember = streamsGroupMemberBuilderWithDefaults(newMemberId, instanceId)
                .setMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                .setRackId(newRackId)
                .setAssignedTasks(givenAssignedTasks)
                .build();

        StreamsGroupMember reconciledMember = streamsGroupMemberBuilderWithDefaults(newMemberId, instanceId)
                .setMemberEpoch(bumpedMemberEpoch)
                .setPreviousMemberEpoch(0)
                .setRackId(newRackId)
                .setAssignedTasks(givenAssignedTasks)
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
                .setAssignedTasks(assignedTasks)
                .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(topic.metadataImage)
                .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                        .withMember(oldMember)
                        .withTargetAssignment(oldMemberId, oldTargetAssignment)
                        .withTargetAssignmentEpoch(groupEpoch)
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(topic.metadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
                .build();


        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, rejoinMemberId, instanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                        .setProcessId(DEFAULT_PROCESS_ID)
                        .setActiveTasks(List.of())
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
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
                        new StreamsGroupHeartbeatRequestData()
                                .setGroupId(groupId)
                                .setInstanceId(instanceId)
                                .setMemberId(differentMemberId)
                                .setMemberEpoch(-2)
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
                context.streamsGroupHeartbeat(
                        new StreamsGroupHeartbeatRequestData()
                                .setGroupId(groupId)
                                .setInstanceId(unknownInstanceId)
                                .setMemberId(unknownMemberId)
                                .setMemberEpoch(1)
                ));
        assertEquals(String.format("Instance id %s is unknown.", unknownInstanceId), e.getMessage());
    }

    private void testStaticMemberJoinThenRevokeAndReceiveTasksWith2Members(int maxSize) {
        String groupId = "fooup";

        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String otherMemberId2 = Uuid.randomUuid().toString();

        String instanceId1 = Uuid.randomUuid().toString();
        String instanceId2 = Uuid.randomUuid().toString();

        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(List.of(
                new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 4)
                .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage)
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, maxSize)
                .withStreamsGroupTaskAssignors(List.of(assignor))
                .withMetadataImage(metadataImage)
                .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                        .withMember(streamsGroupMemberBuilderWithDefaults(memberId1)
                                .setInstanceId(instanceId1)
                                .setMemberEpoch(10)
                                .setPreviousMemberEpoch(9)
                                .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskAssignmentTestUtil.TaskRole.ACTIVE, 10,
                                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3)))
                                .build())
                        .withTargetAssignment(memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskAssignmentTestUtil.TaskRole.ACTIVE,
                                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3)))
                        .withTargetAssignmentEpoch(10)
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                        .withMetadataHash(groupMetadataHash)
                        .withValidatedTopologyEpoch(0)
                )
                .build();

        // Next target assignment after member2 joins.
        assignor.prepareGroupAssignment(Map.of(
                memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskAssignmentTestUtil.TaskRole.ACTIVE,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1)
                ),
                memberId2, TaskAssignmentTestUtil.mkTasksTuple(TaskAssignmentTestUtil.TaskRole.ACTIVE,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 2, 3)
                )
        ));

        // 1) Static member2 joins. It gets no active tasks yet because member1 still owns them.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> joinResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setInstanceId(instanceId2)
                        .setMemberId(memberId2)
                        .setMemberEpoch(0)
                        .setRebalanceTimeoutMs(1500)
                        .setTopology(topology)
                        .setProcessId(DEFAULT_PROCESS_ID)
                        .setActiveTasks(List.of())
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
        );

        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(memberId2)
                        .setMemberEpoch(11)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(List.of())
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
                        .setStatus(List.of()),
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

        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(memberId1)
                        .setMemberEpoch(10)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(List.of(
                                new StreamsGroupHeartbeatResponseData.TaskIds()
                                        .setSubtopologyId(subtopology1)
                                        .setPartitions(List.of(0, 1))
                        ))
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
                        .setStatus(List.of()),
                revokeInstructionResult.response().data()
        );

        // 3) member1 acknowledges revocation by reporting owned active tasks [0,1].
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> revokeAckResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setInstanceId(instanceId1)
                        .setMemberId(memberId1)
                        .setMemberEpoch(10)
                        .setActiveTasks(List.of(
                                new StreamsGroupHeartbeatRequestData.TaskIds()
                                        .setSubtopologyId(subtopology1)
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
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setInstanceId(instanceId2)
                        .setMemberId(memberId2)
                        .setMemberEpoch(11)
        );

        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(memberId2)
                        .setMemberEpoch(11)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(List.of(
                                new StreamsGroupHeartbeatResponseData.TaskIds()
                                        .setSubtopologyId(subtopology1)
                                        .setPartitions(List.of(2, 3))
                        ))
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
                        .setStatus(List.of()),
                member2ReceiveResult.response().data()
        );

        // 5) member2 leave.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> member2LeaveResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setInstanceId(instanceId2)
                        .setMemberId(memberId2)
                        .setMemberEpoch(-2)
        );

        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(memberId2)
                        .setMemberEpoch(-2)
                        .setHeartbeatIntervalMs(0)
                        .setActiveTasks(null)
                        .setStandbyTasks(null)
                        .setWarmupTasks(null)
                        .setStatus(List.of()),
                member2LeaveResult.response().data()
        );

        // 6) member2 re-join with other memberId.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> member2rejoinResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setInstanceId(instanceId2)
                        .setMemberId(otherMemberId2)
                        .setMemberEpoch(0)
        );

        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(otherMemberId2)
                        .setMemberEpoch(11)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(List.of(
                                new StreamsGroupHeartbeatResponseData.TaskIds()
                                        .setSubtopologyId(subtopology1)
                                        .setPartitions(List.of(2, 3))
                        ))
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
                        .setStatus(List.of()),
                member2rejoinResult.response().data()
        );
    }

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
                        new StreamsGroupHeartbeatRequestData()
                                .setGroupId(groupId)
                                .setMemberId(newDynamicMemberId)
                                .setMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                                .setProcessId("new-process-id")
                                .setRebalanceTimeoutMs(1500)
                                .setTopology(topology)
                                .setActiveTasks(List.of())
                                .setStandbyTasks(List.of())
                                .setWarmupTasks(List.of())
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
        String topicName = "foo";
        Uuid topicId = Uuid.randomUuid();
        StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(List.of(
                new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId(subtopologyId).setSourceTopics(List.of(topicName))
        ));

        String oldStaticMemberId = Uuid.randomUuid().toString();
        String newStaticMemberId = Uuid.randomUuid().toString();
        String staticInstanceId = Uuid.randomUuid().toString();
        String dynamicMemberId = Uuid.randomUuid().toString();

        // GIVEN Task for static member
        Map.Entry<String, Set<Integer>> staticTaskEntries = TaskAssignmentTestUtil.mkTasks(subtopologyId, 0, 1);
        TasksTupleWithEpochs staticAssignedTasks = mkTasksTupleWithCommonEpoch(TaskAssignmentTestUtil.TaskRole.ACTIVE, groupEpoch, staticTaskEntries);
        TasksTuple staticTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(TaskAssignmentTestUtil.TaskRole.ACTIVE, staticTaskEntries);

        // GIVEN Task for dynamic member
        Map.Entry<String, Set<Integer>> dynamicTaskEntries = TaskAssignmentTestUtil.mkTasks(subtopologyId,2, 3);
        TasksTupleWithEpochs dynamicAssignedTasks = mkTasksTupleWithCommonEpoch(TaskAssignmentTestUtil.TaskRole.ACTIVE, groupEpoch, dynamicTaskEntries);
        TasksTuple dynamicTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(TaskAssignmentTestUtil.TaskRole.ACTIVE, dynamicTaskEntries);

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
                .addTopic(topicId, topicName, 4)
                .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
                topicName, computeTopicHash(topicName, metadataImage)
        ));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(metadataImage)
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
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(groupMetadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
                .build();

        // WHEN - static member rejoin.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setInstanceId(staticInstanceId)
                        .setMemberId(newStaticMemberId)
                        .setMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
        );

        // THEN - At the maxsize, static member still can rejoin if static member leaves with epoch -2.
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(newStaticMemberId)
                        .setMemberEpoch(groupEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(mkResponseTasks(subtopologyId, 0, 1))
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of()),
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



        StreamsTopicFixture topic = streamsTopicFixture("subtopology-1", "foo", 3);

        // GIVEN Task for static member
        TasksTupleWithEpochs staticAssignedTasks = topic.assignedTasks(groupEpoch, 0, 1);
        TasksTuple staticTargetAssignment = topic.targetAssignment(0, 1);

        // GIVEN Task for dynamic member
        TasksTupleWithEpochs dynamicAssignedTasks = topic.assignedTasks(groupEpoch, 2);
        TasksTuple dynamicTargetAssignment = topic.targetAssignment(2);


        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(topic.metadataImage)
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
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(topic.metadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
                .build();

        // WHEN - static member leaves with epoch -2.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult =
                context.streamsGroupHeartbeat(staticHeartbeat(
                        groupId,
                        staticMemberId,
                        staticInstanceId,
                        LEAVE_GROUP_STATIC_MEMBER_EPOCH
                ));

        // THEN
        assertResponseEquals(
                staticLeaveResponseWithNullTasks(staticMemberId, LEAVE_GROUP_STATIC_MEMBER_EPOCH),
                leaveResult.response().data()
        );

        // WHEN2 - dynamic member send a heartbeat.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> dynamicHeartbeatResult =
                context.streamsGroupHeartbeat(
                        new StreamsGroupHeartbeatRequestData()
                                .setGroupId(groupId)
                                .setMemberId(dynamicMemberId)
                                .setMemberEpoch(groupEpoch)
                );

        // THEN2 : There is no new assignment.
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(dynamicMemberId)
                        .setMemberEpoch(groupEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(null)
                        .setWarmupTasks(null)
                        .setStandbyTasks(null),
                dynamicHeartbeatResult.response().data()
        );
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
        String topicName = "foo";
        Uuid topicId = Uuid.randomUuid();
        StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(List.of(
                new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId(subtopologyId).setSourceTopics(List.of(topicName))
        ));

        String oldStaticMemberId = Uuid.randomUuid().toString();
        String rejoinStaticMemberId = Uuid.randomUuid().toString();
        String staticInstanceId = Uuid.randomUuid().toString();
        String dynamicMemberId = Uuid.randomUuid().toString();

        String oldProcessId = "old-process-id";
        String newProcessId = "new-process-id";

        // Initial assignment
        Map.Entry<String, Set<Integer>> staticTaskEntries = TaskAssignmentTestUtil.mkTasks(subtopologyId, 0, 1);
        TasksTupleWithEpochs staticAssignedTasks = mkTasksTupleWithCommonEpoch(TaskAssignmentTestUtil.TaskRole.ACTIVE, groupEpoch, staticTaskEntries);
        TasksTuple oldStaticTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(TaskAssignmentTestUtil.TaskRole.ACTIVE, staticTaskEntries);

        Map.Entry<String, Set<Integer>> dynamicTaskEntries = TaskAssignmentTestUtil.mkTasks(subtopologyId, 2, 3);
        TasksTupleWithEpochs dynamicAssignedTasks = mkTasksTupleWithCommonEpoch(TaskAssignmentTestUtil.TaskRole.ACTIVE, groupEpoch, dynamicTaskEntries);
        TasksTuple oldDynamicTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(TaskAssignmentTestUtil.TaskRole.ACTIVE, dynamicTaskEntries);

        // Recomputed target assignment after static member rejoins with updated processId
        TasksTuple newStaticTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(
                TaskAssignmentTestUtil.TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopologyId, 0)
        );
        TasksTuple newDynamicTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(
                TaskAssignmentTestUtil.TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopologyId, 1, 2, 3)
        );

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
                .addTopic(topicId, topicName, 4)
                .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
                topicName, computeTopicHash(topicName, metadataImage)
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        assignor.prepareGroupAssignment(Map.of(
                rejoinStaticMemberId, newStaticTargetAssignment,
                dynamicMemberId, newDynamicTargetAssignment
        ));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(assignor))
                .withMetadataImage(metadataImage)
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
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(groupMetadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
                .build();

        // WHEN 1: static member try to rejoin with new process id.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult =
                context.streamsGroupHeartbeat(
                        new StreamsGroupHeartbeatRequestData()
                                .setGroupId(groupId)
                                .setInstanceId(staticInstanceId)
                                .setMemberId(rejoinStaticMemberId)
                                .setMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                                .setProcessId(newProcessId)
                                .setActiveTasks(List.of())
                                .setStandbyTasks(List.of())
                                .setWarmupTasks(List.of())
                );

        // THEN 1:
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(rejoinStaticMemberId)
                        .setMemberEpoch(bumpedGroupEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(mkResponseTasks(subtopologyId, 0))
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of()),
                rejoinResult.response().data()
        );

        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);

        // group epoch should be bumped up.
        assertEquals(bumpedGroupEpoch, group.groupEpoch());

        // static member should be replaced.
        assertFalse(group.hasMember(oldStaticMemberId));
        assertTrue(group.hasMember(rejoinStaticMemberId));
        assertEquals(rejoinStaticMemberId, group.staticMember(staticInstanceId).memberId());

        // Because group epoch is bumped up, target assignment should be recomupted.
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
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, rejoinStaticMemberId, oldStaticTargetAssignment),
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedCopiedStaticMember),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedUpdatedStaticMember),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, bumpedGroupEpoch, groupMetadataHash, 0, getDefaultAssignmentConfigs()
                )
        );

        List<CoordinatorRecord> expectedRecomputedTargetAssignmentRecords = List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, rejoinStaticMemberId, newStaticTargetAssignment),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, dynamicMemberId, newDynamicTargetAssignment)
        );

        List<CoordinatorRecord> expectedRecordsAfterRecomputedAssignments = List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, bumpedGroupEpoch, context.time.milliseconds()),
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

        // WHEN 2: dynamic member send a heartbeat and reconciles to the new target assignment.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> dynamicHeartbeatResult =
                context.streamsGroupHeartbeat(
                        new StreamsGroupHeartbeatRequestData()
                                .setGroupId(groupId)
                                .setMemberId(dynamicMemberId)
                                .setMemberEpoch(groupEpoch)
                );

        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(dynamicMemberId)
                        .setMemberEpoch(bumpedGroupEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(mkResponseTasks(subtopologyId, 1, 2, 3))
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of()),
                dynamicHeartbeatResult.response().data()
        );

        StreamsGroupMember expectedUpdatedDynamicMember = streamsGroupMemberBuilderWithDefaults(dynamicMemberId)
                .setMemberEpoch(bumpedGroupEpoch)
                .setPreviousMemberEpoch(groupEpoch)
                .setAssignedTasks(mkTasksTupleWithEpochs(
                        TaskRole.ACTIVE,
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

        StreamsTopicFixture topic = streamsTopicFixture("subtopology-1", "foo", 4);

        // GIVEN Tasks
        TasksTupleWithEpochs staticAssignedTasks = topic.assignedTasks(groupEpoch, 0, 1);
        TasksTuple staticTargetAssignment = topic.targetAssignment(0, 1);

        TasksTupleWithEpochs dynamicAssignedTasks = topic.assignedTasks(groupEpoch, 2, 3);
        TasksTuple dynamicTargetAssignment = topic.targetAssignment(2, 3);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(topic.metadataImage)
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
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(topic.metadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
                .build();

        // WHEN1 : static member try to rejoin with same process id.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult =
                context.streamsGroupHeartbeat(
                        staticHeartbeat(groupId, newStaticMemberId, staticInstanceId,
                                StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                                .setProcessId(staticProcessId)
                                .setActiveTasks(List.of())
                                .setStandbyTasks(List.of())
                                .setWarmupTasks(List.of())
                );


        // THEN1
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(newStaticMemberId)
                        .setMemberEpoch(groupEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(topic.responseTasks(0, 1))
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of()),
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

        // WHEN2 - dynamic member send a heartbeat request
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> dynamicHeartbeatResult =
                context.streamsGroupHeartbeat(
                        new StreamsGroupHeartbeatRequestData()
                                .setGroupId(groupId)
                                .setMemberId(dynamicMemberId)
                                .setMemberEpoch(groupEpoch)
                );

        // THEN2 - no new target assignment.
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(dynamicMemberId)
                        .setMemberEpoch(groupEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(null)
                        .setStandbyTasks(null)
                        .setWarmupTasks(null),
                dynamicHeartbeatResult.response().data()
        );
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

        StreamsTopicFixture topic = streamsTopicFixture("subtopology-1", "foo", 4);

        TasksTupleWithEpochs staticAssignedTasks = topic.assignedTasks(groupEpoch, 0, 1);
        TasksTuple staticTargetAssignment = topic.targetAssignment(0, 1);

        TasksTupleWithEpochs dynamicAssignedTasks = topic.assignedTasks(groupEpoch, 2, 3);
        TasksTuple dynamicTargetAssignment = topic.targetAssignment(2, 3);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(topic.metadataImage)
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
                        .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology))
                        .withValidatedTopologyEpoch(0)
                        .withMetadataHash(topic.metadataHash)
                        .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
                .build();

        // WHEN1 - static member try to rejoin with new member id.
        context.streamsGroupHeartbeat(
                staticHeartbeat(groupId, newStaticMemberId, staticInstanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                        .setProcessId(staticProcessId)
                        .setActiveTasks(List.of())
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
        );

        // WHEN2 + THEN2 - stale static member send a heartbeat with stale member id. 
        assertThrows(FencedInstanceIdException.class, () ->
                context.streamsGroupHeartbeat(staticHeartbeat(groupId, oldStaticMemberId, staticInstanceId, groupEpoch))
        );


        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
        assertFalse(group.hasMember(oldStaticMemberId));
        assertTrue(group.hasMember(newStaticMemberId));
        assertTrue(group.hasMember(dynamicMemberId));
        assertEquals(newStaticMemberId, group.staticMember(staticInstanceId).memberId());
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
        String topicName = "foo";
        Uuid topicId = Uuid.randomUuid();
        StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(List.of(
                new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId(subtopologyId).setSourceTopics(List.of(topicName))
        ));

        String staticMemberId = Uuid.randomUuid().toString();
        String staticInstanceId = Uuid.randomUuid().toString();
        String dynamicMemberId = Uuid.randomUuid().toString();
        String newDynamicMemberId = Uuid.randomUuid().toString();

        String staticProcessId = "static-process-id";
        String dynamicProcessId = "dynamic-process-id";
        String newDynamicProcessId = "new-dynamic-process-id";

        // GIVEN assignment
        Map.Entry<String, Set<Integer>> staticTaskEntries = TaskAssignmentTestUtil.mkTasks(subtopologyId, 0, 1);
        TasksTupleWithEpochs staticAssignedTasks = mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, groupEpoch, staticTaskEntries);
        TasksTuple staticTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, staticTaskEntries);

        Map.Entry<String, Set<Integer>> dynamicTaskEntries = TaskAssignmentTestUtil.mkTasks(subtopologyId, 2, 3);
        TasksTupleWithEpochs dynamicAssignedTasks = mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, groupEpoch, dynamicTaskEntries);
        TasksTuple dynamicTargetAssignment = TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, dynamicTaskEntries);

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
                .addTopic(topicId, topicName, 4)
                .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
                topicName, computeTopicHash(topicName, metadataImage)
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(assignor))
                .withMetadataImage(metadataImage)
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

        // WHEN1 - static member leaves with epoch -2.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult =
                context.streamsGroupHeartbeat(
                        new StreamsGroupHeartbeatRequestData()
                                .setGroupId(groupId)
                                .setInstanceId(staticInstanceId)
                                .setMemberId(staticMemberId)
                                .setMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                );

        // THEN1
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(staticMemberId)
                        .setMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                        .setActiveTasks(null)
                        .setWarmupTasks(null)
                        .setStandbyTasks(null)
                        .setStatus(List.of()),
                leaveResult.response().data()
        );

        // To prevent session timeout from dynamic member.
        // Sleep 1, and dynamic member send a heartbeat.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(1));
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> dynamicHeartbeatResult =
                context.streamsGroupHeartbeat(
                        new StreamsGroupHeartbeatRequestData()
                                .setGroupId(groupId)
                                .setMemberId(dynamicMemberId)
                                .setMemberEpoch(groupEpoch)
                );

        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(dynamicMemberId)
                        .setMemberEpoch(groupEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(null)
                        .setStandbyTasks(null)
                        .setWarmupTasks(null),
                dynamicHeartbeatResult.response().data()
        );
        assertTrue(dynamicHeartbeatResult.records().isEmpty());

        // WHEN2: static member session timeout.
        context.assertSessionTimeout(groupId, staticMemberId, 45000 - 1);
        List<MockCoordinatorTimer.ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(45000 - 1);

        // THEN2
        List<CoordinatorRecord> expectedTimeoutRecords = List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, staticMemberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, staticMemberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, staticMemberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                        groupId, timeoutGroupEpoch, groupMetadataHash, 0,getDefaultAssignmentConfigs()
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

        // WHEN3 - new dynamic member try to join.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> joinResult =
                context.streamsGroupHeartbeat(
                        new StreamsGroupHeartbeatRequestData()
                                .setGroupId(groupId)
                                .setMemberId(newDynamicMemberId)
                                .setMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                                .setProcessId(newDynamicProcessId)
                                .setRebalanceTimeoutMs(1500)
                                .setTopology(topology)
                                .setActiveTasks(List.of())
                                .setStandbyTasks(List.of())
                                .setWarmupTasks(List.of())
                );

        // THEN3 : accept join.
        assertResponseEquals(
                new StreamsGroupHeartbeatResponseData()
                        .setMemberId(newDynamicMemberId)
                        .setMemberEpoch(joinGroupEpoch)
                        .setHeartbeatIntervalMs(5000)
                        .setTaskOffsetIntervalMs(60000)
                        .setActiveTasks(mkResponseTasks(subtopologyId, 0, 1))
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of()),
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
                        getDefaultAssignmentConfigs()
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

        private StreamsGroupHeartbeatRequestData joiningNewStaticInstance() {
            return joinRequest(newMemberId, newInstanceId);
        }

        private StreamsGroupHeartbeatRequestData rejoiningLeftStaticMember() {
            return joinRequest(leftMemberId, leftInstanceId);
        }

        private StreamsGroupHeartbeatRequestData joiningWithActiveInstance() {
            return joinRequest(newMemberId, activeInstanceId);
        }

        private StreamsGroupHeartbeatRequestData joinRequest(
                String memberId,
                String instanceId
        ) {
            return new StreamsGroupHeartbeatRequestData()
                    .setGroupId(GROUP_ID)
                    .setInstanceId(instanceId)
                    .setMemberId(memberId)
                    .setMemberEpoch(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                    .setProcessId(DEFAULT_PROCESS_ID)
                    .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of());
        }

        private StreamsGroupHeartbeatRequestData staticLeaveRequest(
                String memberId,
                String instanceId
        ) {
            return new StreamsGroupHeartbeatRequestData()
                    .setGroupId(GROUP_ID)
                    .setInstanceId(instanceId)
                    .setMemberId(memberId)
                    .setMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setProcessId(DEFAULT_PROCESS_ID)
                    .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of());
        }

        private StreamsGroupHeartbeatRequestData leaveRequest(
                String memberId,
                String instanceId
        ) {
            return new StreamsGroupHeartbeatRequestData()
                    .setGroupId(GROUP_ID)
                    .setInstanceId(instanceId)
                    .setMemberId(memberId)
                    .setMemberEpoch(StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH)
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

        private void assertLeaveSucceeds(StreamsGroupHeartbeatRequestData request) {
            assertJoinSucceeds(request);
        }

        private void assertJoinFails(
                StreamsGroupHeartbeatRequestData request,
                Class<? extends Exception> expectedException
        ) {
            assertThrows(expectedException, () -> context.streamsGroupHeartbeat(request));
        }

        private void assertLeaveFails(
                StreamsGroupHeartbeatRequestData request,
                Class<? extends Exception> expectedException
        ) {
            assertJoinFails(request, expectedException);
        }
    }

    private StreamsGroupMember.Builder streamsGroupMemberBuilderWithDefaults(String memberId) {
        return streamsGroupMemberBuilderWithDefaults(memberId, null);
    }
    
    private static StreamsGroupMember.Builder streamsGroupMemberBuilderWithDefaults(String memberId, String instanceId) {
        return new StreamsGroupMember.Builder(memberId)
                .setMemberEpoch(1)
                .setPreviousMemberEpoch(0)
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                .setRackId(null)
                .setInstanceId(instanceId)
                .setRebalanceTimeoutMs(1500)
                .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .setTopologyEpoch(0)
                .setClientTags(Map.of())
                .setClientId(DEFAULT_CLIENT_ID)
                .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                .setProcessId(DEFAULT_PROCESS_ID)
                .setUserEndpoint(null);
    }

    /**
     * Returns the default assignment configurations that would be used by the system.
     * This matches what streamsGroupAssignmentConfigs() would return.
     */
    private Map<String, String> getDefaultAssignmentConfigs() {
        // Use the same default value as GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT
        return new TreeMap<>(Map.of(
                "num.standby.replicas", String.valueOf(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT)
        ));
    }

    private static List<StreamsGroupHeartbeatResponseData.TaskIds> mkResponseTasks(
            String subtopologyId,
            Integer... partitions
    ) {
        return List.of(
                new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopologyId)
                        .setPartitions(Arrays.asList(partitions))
        );
    }
    
    
    
    
    
    /// /////////////

    private static final int DEFAULT_REBALANCE_TIMEOUT_MS = 1500;

    private static StreamsTopicFixture streamsTopicFixture(String subtopologyId, String topicName, int partitions) {
        return new StreamsTopicFixture(subtopologyId, topicName, partitions);
    }

    private static StreamsGroupHeartbeatRequestData staticHeartbeat(String groupId, String memberId, String instanceId, int memberEpoch) {
        return new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setInstanceId(instanceId)
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch);
    }

    private static StreamsGroupHeartbeatRequestData staticJoinHeartbeat(String groupId, String memberId, String instanceId, StreamsTopicFixture topic) {
        return staticHeartbeat(groupId, memberId, instanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                .setProcessId(DEFAULT_PROCESS_ID)
                .setRebalanceTimeoutMs(DEFAULT_REBALANCE_TIMEOUT_MS)
                .setTopology(topic.topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of());
    }

    private static StreamsGroupHeartbeatResponseData staticLeaveResponse(String memberId, int leaveEpoch) {
        return new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(leaveEpoch)
                .setStatus(List.of());
    }


    private static StreamsGroupHeartbeatResponseData staticLeaveResponseWithNullTasks(String memberId, int leaveEpoch) {
        return staticLeaveResponse(memberId, leaveEpoch)
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null);
    }


    private static class StreamsTopicFixture {
        private final String subtopologyId;
        private final String topicName;
        private final Uuid topicId;
        private final StreamsGroupHeartbeatRequestData.Topology topology;
        private final CoordinatorMetadataImage metadataImage;
        private final long metadataHash;

        private StreamsTopicFixture(
                String subtopologyId,
                String topicName,
                int partitions
        ) {
            this.subtopologyId = subtopologyId;
            this.topicName = topicName;
            this.topicId = Uuid.randomUuid();
            this.topology = new StreamsGroupHeartbeatRequestData.Topology()
                    .setSubtopologies(List.of(
                            new StreamsGroupHeartbeatRequestData.Subtopology()
                                    .setSubtopologyId(subtopologyId)
                                    .setSourceTopics(List.of(topicName))
                    ));
            this.metadataImage = new MetadataImageBuilder()
                    .addTopic(topicId, topicName, partitions)
                    .buildCoordinatorMetadataImage();
            this.metadataHash = computeGroupHash(Map.of(
                    topicName,
                    computeTopicHash(topicName, metadataImage)
            ));
        }

        private Map.Entry<String, Set<Integer>> tasks(Integer... partitions) {
            return TaskAssignmentTestUtil.mkTasks(subtopologyId, partitions);
        }

        private TasksTuple targetAssignment(Integer... partitions) {
            return TaskAssignmentTestUtil.mkTasksTuple(
                    TaskRole.ACTIVE,
                    tasks(partitions)
            );
        }

        private TasksTupleWithEpochs assignedTasks(
                int epoch,
                Integer... partitions
        ) {
            return mkTasksTupleWithCommonEpoch(
                    TaskRole.ACTIVE,
                    epoch,
                    tasks(partitions)
            );
        }

        private List<StreamsGroupHeartbeatResponseData.TaskIds> responseTasks(Integer... partitions) {
            return mkResponseTasks(subtopologyId, partitions);
        }
    }

    private GroupMetadataManagerTestContext contextWithStreamsGroup(
            String groupId,
            int groupEpoch,
            StreamsTopicFixture topic,
            java.util.function.UnaryOperator<StreamsGroupBuilder> configureGroup
    ) {
        StreamsGroupBuilder group = new StreamsGroupBuilder(groupId, groupEpoch)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash)
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs());

        return new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
                .withMetadataImage(topic.metadataImage)
                .withStreamsGroup(configureGroup.apply(group))
                .build();
    }
}