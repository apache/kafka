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
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataKey;
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

import java.net.InetAddress;
import java.net.UnknownHostException;
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
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ADDRESS;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ID;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_PROCESS_ID;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.getDefaultAssignmentConfigs;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.resetAssignedTasksEpochsToZero;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticHeartbeat;
import static org.apache.kafka.coordinator.group.StreamsGroupTestUtil.staticJoinHeartbeat;
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
        String groupId = "streams-group";

        String activeMemberId = "active-member";
        String activeInstanceId = "active-instance";

        String unknownMemberId = "unknown-member-id";
        String unknownInstanceId = "unknown-instance-id";

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 1);
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, DEFAULT_GROUP_EPOCH)
                .withMember(streamsGroupMemberBuilderWithDefaults(activeMemberId, activeInstanceId)
                    .setMemberEpoch(DEFAULT_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(DEFAULT_MEMBER_EPOCH - 1)
                    .build())
                .withTargetAssignmentEpoch(DEFAULT_GROUP_EPOCH)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        assertThrows(
            UnknownMemberIdException.class,
            () -> context.streamsGroupHeartbeat(staticHeartbeat(
                groupId,
                unknownMemberId,
                unknownInstanceId,
                LEAVE_GROUP_STATIC_MEMBER_EPOCH
            ))
        );
    }

    @Test
    public void testStreamsStaticJoinWithNewInstanceAtMaxSizeThrowsGroupMaxSizeReached() {
        // With max.size=2 already reached,
        // joining with a new static instanceId must throw GroupMaxSizeReachedException.
        int streamsGroupMaxSize = 2;
        String groupId = "streams-group";

        String activeMemberId = "active-member";
        String activeInstanceId = "active-instance";

        String leftMemberId = "left-member";
        String leftInstanceId = "left-instance";

        String newMemberId = "new-member";
        String newInstanceId = "new-instance";

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 1);

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        assignor.prepareGroupAssignment(Map.of(activeMemberId, TasksTuple.EMPTY));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(topic.metadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, streamsGroupMaxSize)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, DEFAULT_GROUP_EPOCH)
                .withMember(streamsGroupMemberBuilderWithDefaults(activeMemberId, activeInstanceId)
                    .setMemberEpoch(DEFAULT_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(DEFAULT_MEMBER_EPOCH - 1)
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(leftMemberId, leftInstanceId)
                    .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(DEFAULT_MEMBER_EPOCH - 1)
                    .build())
                .withTargetAssignmentEpoch(DEFAULT_GROUP_EPOCH)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withMetadataHash(topic.metadataHash())
                .withValidatedTopologyEpoch(0)
            )
            .build();

        assertThrows(
            GroupMaxSizeReachedException.class,
            () -> context.streamsGroupHeartbeat(staticJoinHeartbeat(
                groupId,
                newMemberId,
                newInstanceId,
                topic
            ))
        );
        
    }

    @Test
    public void testStreamsStaticRejoinWithLeaveGroupStaticEpochAtMaxSizeSucceeds() {
        // If a static member is in leave epoch (-2),
        // rejoining with the same memberId/instanceId is allowed even at max size.
        int streamsGroupMaxSize = 2;
        String groupId = "streams-group";

        String activeMemberId = "active-member";
        String activeInstanceId = "active-instance";

        String leftMemberId = "left-member";
        String leftInstanceId = "left-instance";
        String newJoinMemberId = "new-join-member";

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 1);

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        assignor.prepareGroupAssignment(Map.of(
            activeMemberId, TasksTuple.EMPTY,
            leftMemberId, TasksTuple.EMPTY
        ));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(topic.metadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, streamsGroupMaxSize)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, DEFAULT_GROUP_EPOCH)
                .withMember(streamsGroupMemberBuilderWithDefaults(activeMemberId, activeInstanceId)
                    .setMemberEpoch(DEFAULT_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(DEFAULT_MEMBER_EPOCH - 1)
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(leftMemberId, leftInstanceId)
                    .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(DEFAULT_MEMBER_EPOCH - 1)
                    .build())
                .withTargetAssignmentEpoch(DEFAULT_GROUP_EPOCH)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withMetadataHash(topic.metadataHash())
                .withValidatedTopologyEpoch(0)
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
            )
            .build();

        assertDoesNotThrow(() -> context.streamsGroupHeartbeat(staticJoinHeartbeat(
            groupId,
            newJoinMemberId,
            leftInstanceId,
            topic
        )));

    }

    @Test
    public void testStreamsStaticJoinWithUnreleasedInstanceThrowsUnreleasedInstanceIdAtMaxSize() {
        // If an active static member (epoch=10) still owns the instanceId,
        // a different memberId joining with that instanceId must fail even at max size.
        int streamsGroupMaxSize = 1;
        String groupId = "streams-group";

        String activeMemberId = "active-member";
        String activeInstanceId = "active-instance";

        String newMemberId = "new-member";

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 1);

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        assignor.prepareGroupAssignment(Map.of(activeMemberId, TasksTuple.EMPTY));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(topic.metadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, streamsGroupMaxSize)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, DEFAULT_GROUP_EPOCH)
                .withMember(streamsGroupMemberBuilderWithDefaults(activeMemberId, activeInstanceId)
                    .setMemberEpoch(DEFAULT_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(DEFAULT_MEMBER_EPOCH - 1)
                    .build())
                .withTargetAssignmentEpoch(DEFAULT_GROUP_EPOCH)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withMetadataHash(topic.metadataHash())
                .withValidatedTopologyEpoch(0)
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
            )
            .build();

        assertThrows(
            UnreleasedInstanceIdException.class,
            () -> context.streamsGroupHeartbeat(staticJoinHeartbeat(
                groupId,
                newMemberId,
                activeInstanceId,
                topic
            ))
        );
    }
    
    @ParameterizedTest
    @MethodSource("staticMemberReusedInstanceErrorCases")
    public void testStaticMemberSendHeartbeatWithVariousEpochThenThrowError(
        int heartbeatMemberEpoch,
        Class<? extends Exception> expectedException
    ) {
        // same instance id is reused with mismatched member identity/epoch. 
        String groupId = "streams-group";

        String activeMemberId = "active-member";
        String activeInstanceId = "active-instance";
        String newMemberId = "new-member";

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 1);

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        assignor.prepareGroupAssignment(Map.of(activeMemberId, TasksTuple.EMPTY));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(topic.metadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, 2)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, DEFAULT_GROUP_EPOCH)
                .withMember(streamsGroupMemberBuilderWithDefaults(activeMemberId, activeInstanceId)
                    .setMemberEpoch(DEFAULT_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(DEFAULT_MEMBER_EPOCH - 1)
                    .build())
                .withTargetAssignmentEpoch(DEFAULT_GROUP_EPOCH)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withMetadataHash(topic.metadataHash())
                .withValidatedTopologyEpoch(0)
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
            )
            .build();

        assertThrows(
            expectedException,
            () -> context.streamsGroupHeartbeat(staticHeartbeat(
                groupId,
                newMemberId,
                activeInstanceId,
                heartbeatMemberEpoch
            ))
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
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(List.of())
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()),
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
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(topic.responseTasks(0, 1))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()),
            revokeInstructionResult.response().data()
        );

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
                .setAcceptableRecoveryLag(10000)
                .setStatus(List.of()),
            revokeAckResult.response().data()
        );

        // 4) member2 heartbeats again and now receives [2,3].
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> member2ReceiveResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, memberId2, instanceId2, 11)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(topic.responseTasks(2, 3))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()), 
            member2ReceiveResult.response().data()
        );

        // 5) member2 leave.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> member2LeaveResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, memberId2, instanceId2, LEAVE_GROUP_STATIC_MEMBER_EPOCH)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setStatus(List.of())
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null)
                .setHeartbeatIntervalMs(0),
            member2LeaveResult.response().data()
        );

        // 6) member2 re-join with other memberId.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> member2rejoinResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, otherMemberId2, instanceId2, JOIN_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(otherMemberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(topic.responseTasks(2, 3))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()),
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
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(oldMemberId, instanceId)
                    .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(groupEpoch)
                    .setProcessId(oldProcessId)
                    .setAssignedTasks(assignedTasks)
                    .build())
                .withTargetAssignment(oldMemberId, targetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

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
                getDefaultAssignmentConfigs(),
                -1,
                -1
            )
        ));
    }

    @Test
    public void testStaticMemberRejoinWithSameProcessIdDoesNotBumpStreamsGroupEpoch() throws UnknownHostException {
        String groupId = "fooup";
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String oldMemberId = Uuid.randomUuid().toString();
        String rejoinMemberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        String processId = "process-id";
        String newClientId = "new-client-id";
        InetAddress newClientAddress = InetAddress.getByName("127.0.0.2");

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(groupEpoch, 0, 1, 2, 3);
        TasksTuple targetAssignment = topic.targetAssignment(0, 1, 2, 3);

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        assignor.prepareGroupAssignment(Map.of(rejoinMemberId, targetAssignment));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(oldMemberId, instanceId)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setPreviousMemberEpoch(groupEpoch)
                    .setProcessId(processId)
                    .setAssignedTasks(assignedTasks)
                    .build())
                .withTargetAssignment(oldMemberId, targetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            staticJoinHeartbeat(groupId, rejoinMemberId, instanceId, processId),
            newClientId,
            newClientAddress
        );
        
        assertEquals(rejoinMemberId, result.response().data().memberId());
        assertEquals(groupEpoch, result.response().data().memberEpoch());

        // The static member rejoined with a new member id while the assignment is up to date, so the
        // target assignment is reused from the persisted state. Its relabelling to the new member id
        // is queued but not yet replayed, so the rejoining member must still get its assignment back
        // (not an empty one).
        assertEquals(topic.responseTasks(0, 1, 2, 3), result.response().data().activeTasks());

        Optional<StreamsGroupMemberMetadataValue> updatedMemberMetadataValue = result.records().stream()
            .filter(record -> record.key() instanceof StreamsGroupMemberMetadataKey)
            .filter(record -> ((StreamsGroupMemberMetadataKey) record.key()).memberId().equals(rejoinMemberId))
            .filter(record -> record.value() != null)
            .map(record -> (StreamsGroupMemberMetadataValue) record.value().message())
            .filter(value -> newClientId.equals(value.clientId()))
            .filter(value -> newClientAddress.toString().equals(value.clientHost()))
            .findFirst();

        assertTrue(
            updatedMemberMetadataValue.isPresent(),
            "Expected a StreamsGroupMemberMetadata record with the updated client id/host."
        );

        assertTrue(
            result.records().stream().noneMatch(record -> record.key() instanceof StreamsGroupMetadataKey),
            "Expected no StreamsGroupMetadata record when only client id/host changes."
        );

        assertEquals(groupEpoch, result.response().data().memberEpoch());
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
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                    .setMemberEpoch(groupEpoch)
                    .setPreviousMemberEpoch(groupEpoch - 1)
                    .setAssignedTasks(assignedTasks)
                    .build())
                .withTargetAssignment(memberId, targetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, memberId, instanceId, LEAVE_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setStatus(List.of()),
            result.response().data()
        );

        assertRecordsEquals(
            List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, bumpedGroupEpoch, topic.metadataHash(), 0, getDefaultAssignmentConfigs(), -1, -1),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, bumpedGroupEpoch, 0L)
            ),
            result.records()
        );
    }

    @Test
    public void testStaticMemberLeaveWithLeaveGroupStaticMemberEpoch() {
        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(groupEpoch, 0, 1, 2, 3);
        TasksTupleWithEpochs pendingRevocationTasks = topic.assignedTasks(groupEpoch, 2, 3);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                    .setMemberEpoch(memberEpoch)
                    .setPreviousMemberEpoch(memberEpoch - 1)
                    .setAssignedTasks(assignedTasks)
                    .setTasksPendingRevocation(pendingRevocationTasks)
                    .build())
                .withTargetAssignment(memberId, topic.targetAssignment(0, 1, 2, 3))
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, memberId, instanceId, leaveEpoch)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(leaveEpoch)
                .setStatus(List.of()), 
            result.response().data()
        );

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

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(alreadyLeftStaticMember)
                .withTargetAssignment(memberId, targetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();
        
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(staticHeartbeat(groupId, memberId, instanceId, leaveEpoch));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(leaveEpoch)
                .setStatus(List.of()), 
            result.response().data()
        );

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

        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";

        String subtopology1 = "subtopology1";
        StreamsTopicFixture topic = streamsTopicFixture(subtopology1, "foo", 4);

        TasksTupleWithEpochs givenAssignedTasks = topic.assignedTasks(memberEpoch, 0, 1, 2, 3);
        TasksTuple givenTargetAssignment = topic.targetAssignment(0, 1, 2, 3);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(oldMemberId, instanceId)
                    .setMemberEpoch(memberEpoch)
                    .setPreviousMemberEpoch(memberEpoch - 1)
                    .setAssignedTasks(givenAssignedTasks)
                    .build())
                .withTargetAssignment(oldMemberId, givenTargetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();
        
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> normalHeartbeatResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, oldMemberId, instanceId, memberEpoch)
        );

         
        // all tasks should be null because assigned tasks unchanged.
        // Keep the group epoch.
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(oldMemberId)
                .setMemberEpoch(memberEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null),
            normalHeartbeatResult.response().data());
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());


        // Stream member leaves with epoch -2.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, oldMemberId, instanceId, leaveEpoch)
        );

        // Keep the group epoch.
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(oldMemberId)
                .setMemberEpoch(leaveEpoch)
                .setStatus(List.of())
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null), 
            leaveResult.response().data()
        );
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());

        // Streams Member rejoin with other memberId
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, newMemberId, instanceId, JOIN_GROUP_MEMBER_EPOCH)
        );

        // Inherit previous member's member epoch, and assigned tasks.
        // Keep the member epoch bump.
        // Keep the group epoch bump.
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(newMemberId)
                .setMemberEpoch(memberEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(topic.responseTasks(0, 1, 2, 3))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()), 
            rejoinResult.response().data()
        );
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

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(unrevokedMember)
                .withTargetAssignment(memberId, targetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, memberId, instanceId, leaveEpoch)
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
            .setMemberEpoch(leaveEpoch)
            .setState(MemberState.UNREVOKED_TASKS)
            .setPreviousMemberEpoch(memberEpoch - 1)
            .setAssignedTasks(resetAssignedTasksEpochsToZero(assignedTasks))
            .build();

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(leaveEpoch)
                .setStatus(List.of())
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null),
            result.response().data()
        );
        assertRecordsEquals(
            List.of(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)),
            result.records()
        );
        assertEquals(MemberState.UNREVOKED_TASKS, context.streamsGroupMemberState(groupId, memberId));
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

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(temporarilyLeftMember)
                .withTargetAssignment(oldMemberId, targetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, newMemberId, instanceId, JOIN_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(newMemberId)
                .setMemberEpoch(memberEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(topic.responseTasks(0, 1))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()), 
            result.response().data()
        );
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

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(unreleasedMember)
                .withTargetAssignment(memberId, targetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

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

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(leaveEpoch)
                .setStatus(List.of())
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null), 
            result.response().data()
        );
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

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(temporarilyLeftMember)
                .withMember(otherMember)
                .withTargetAssignment(oldMemberId, targetAssignment)
                .withTargetAssignment(otherMemberId, TasksTuple.EMPTY)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, newMemberId, instanceId, JOIN_GROUP_MEMBER_EPOCH)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(newMemberId)
                .setMemberEpoch(memberEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
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
        StreamsTopicFixture topic = streamsTopicFixture(subtopology1, "foo", 3);

        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(memberEpoch, 0, 1);
        TasksTupleWithEpochs tasksPendingRevocation = topic.assignedTasks(memberEpoch, 2);
        TasksTuple leavingTargetAssignment = topic.targetAssignment(0, 1);
        TasksTuple waitingTargetAssignment = topic.targetAssignment(2);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
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
                .withTargetAssignment(waitingMemberId, waitingTargetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, leavingMemberId, instanceId, leaveEpoch)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(leavingMemberId)
                .setMemberEpoch(leaveEpoch)
                .setStatus(List.of())
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null),
            leaveResult.response().data()
        );
        List<CoordinatorRecord> expectedRecordsTriggeredByLeave = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId,
                streamsGroupMemberBuilderWithDefaults(leavingMemberId, instanceId)
                    .setMemberEpoch(leaveEpoch)
                    .setState(MemberState.UNREVOKED_TASKS)
                    .setPreviousMemberEpoch(memberEpoch - 1)
                    .setAssignedTasks(resetAssignedTasksEpochsToZero(assignedTasks))
                    .build()));
        assertRecordsEquals(expectedRecordsTriggeredByLeave, leaveResult.records());
        assertEquals(MemberState.UNREVOKED_TASKS, context.streamsGroupMemberState(groupId, leavingMemberId));

        // Waiting member send a heartbeat expecting get unreleased tasks.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> waitingMemberResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, waitingMemberId, null, memberEpoch)
        );

        StreamsGroupHeartbeatResponseData expectedWaitingMemberResponse = new StreamsGroupHeartbeatResponseData()
            .setMemberId(waitingMemberId)
            .setMemberEpoch(memberEpoch)
            .setHeartbeatIntervalMs(5000)
            .setTaskOffsetIntervalMs(60000)
            .setAcceptableRecoveryLag(10000)
            .setActiveTasks(topic.responseTasks(2))
            .setWarmupTasks(List.of())
            .setStandbyTasks(List.of());
        assertResponseEquals(expectedWaitingMemberResponse, waitingMemberResult.response().data());

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
        int leaveEpoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        int memberEpoch = DEFAULT_MEMBER_EPOCH;
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String rackId = Uuid.randomUuid().toString();
        String memberId = Uuid.randomUuid().toString();
        String instanceId = Uuid.randomUuid().toString();

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);

        TasksTupleWithEpochs givenAssignedTasks = topic.assignedTasks(memberEpoch, 0, 1, 2, 3);
        TasksTuple givenTargetAssignment = topic.targetAssignment(0, 1, 2, 3);

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                    .setMemberEpoch(memberEpoch)
                    .setRackId(rackId)
                    .setPreviousMemberEpoch(memberEpoch - 1)
                    .setAssignedTasks(givenAssignedTasks)
                    .build())
                .withTargetAssignment(memberId, givenTargetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> normalHeartbeatResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, memberId, instanceId, memberEpoch)
                .setRackId(rackId)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null), 
            normalHeartbeatResult.response().data());
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> leaveResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, memberId, instanceId, leaveEpoch)
                .setRackId(rackId)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(leaveEpoch)
                .setStatus(List.of())
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null), 
            leaveResult.response().data()
        );
        assertEquals(groupEpoch, context.groupMetadataManager.streamsGroup(groupId).groupEpoch());

        String newMemberId = Uuid.randomUuid().toString();
        String newRackId = Uuid.randomUuid().toString();
        assignor.prepareGroupAssignment(Map.of(newMemberId, givenTargetAssignment));

        int bumpedGroupEpoch = groupEpoch + 1;
        int bumpedMemberEpoch = memberEpoch + 1;

        // Streams Member rejoin with other memberId and rackId
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, newMemberId, instanceId, JOIN_GROUP_MEMBER_EPOCH)
                .setRackId(newRackId)
        );

        // Inherit previous member's member epoch, and assigned tasks.
        // member epoch should be bumped.
        // group epoch should be bumped.
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(newMemberId)
                .setMemberEpoch(bumpedMemberEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(topic.responseTasks(0, 1, 2, 3))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of()), 
            rejoinResult.response().data()
        );
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
                // From replaceStreamsMembers
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, transationStaticInitMember),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, transationStaticInitMember.memberId(), givenTargetAssignment),
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, transationStaticInitMember),

                // From hasStreamsMemberMetadataChanged
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, newJoinStaticMember),

                StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, bumpedGroupEpoch, topic.metadataHash(), 0, getDefaultAssignmentConfigs(), -1, -1),
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

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(oldMember)
                .withTargetAssignment(oldMemberId, oldTargetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

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
    public void testStaticMemberRejoinsWithSameMemberIdAndDifferentInstanceId() {
        int groupEpoch = DEFAULT_GROUP_EPOCH;

        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String instanceId1 = "instance-1";
        String instanceId2 = "instance-2";

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", 4);
        TasksTuple targetAssignment = topic.targetAssignment(0, 1, 2, 3);
        TasksTupleWithEpochs assignedTasks = topic.assignedTasks(groupEpoch, 0, 1, 2, 3);

        // Streams group with one static member using instanceId1.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId1)
                    .setMemberEpoch(groupEpoch)
                    .setPreviousMemberEpoch(groupEpoch - 1)
                    .setAssignedTasks(assignedTasks)
                    .build())
                .withTargetAssignment(memberId, targetAssignment)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();

        assertEquals(
            Map.of(instanceId1, memberId),
            context.groupMetadataManager.streamsGroup(groupId).staticMembers()
        );

        // Member rejoins with the same member id and a different instance id.
        context.streamsGroupHeartbeat(
            staticJoinHeartbeat(groupId, memberId, instanceId2, topic)
        );

        assertEquals(
            Map.of(instanceId2, memberId),
            context.groupMetadataManager.streamsGroup(groupId).staticMembers()
        );
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
            )
        );
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

        StreamsTopicFixture topic = streamsTopicFixture("subtopology1", "foo", partitionSize);
        TasksTupleWithEpochs givenAssignedTask = topic.assignedTasks(groupEpoch, givenTaskIds);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(new MockTaskAssignor("sticky")))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId, instanceId)
                    .setMemberEpoch(currentMemberEpoch)
                    .setPreviousMemberEpoch(previousMemberEpoch)
                    .setAssignedTasks(givenAssignedTask)
                    .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                    .build())
                .withTargetAssignment(memberId, topic.targetAssignment(givenTaskIds))
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT)
            .build();
        
        StreamsGroupHeartbeatRequestData requestData = staticHeartbeat(groupId, memberId, instanceId, requestMemberEpoch)
            .setProcessId("process-id")
            .setRebalanceTimeoutMs(1500)
            .setTopology(topic.topology())
            .setActiveTasks(topic.requestTasks(requestAssignedTaskIds))
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of());

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

        // static member joins (session timeout should be scheduled)
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> firstJoinResult = context.streamsGroupHeartbeat(
            staticJoinHeartbeat(groupId, memberId, instanceId, topic).setRebalanceTimeoutMs(90000)
        );

        // member epoch should be bumped up.
        // session timeout should be 45000ms.
        assertEquals(2, firstJoinResult.response().data().memberEpoch());
        context.assertSessionTimeout(groupId, memberId, 45000);

        // static member leaves temporarily.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> temporaryLeaveResult = context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, memberId, instanceId, LEAVE_GROUP_STATIC_MEMBER_EPOCH)
        );

        // member epoch should be -2.
        // session timeout still 45000ms.
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setStatus(List.of()), 
            temporaryLeaveResult.response().data()
        );
        context.assertSessionTimeout(groupId, memberId, 45000);

        // no rejoin, session timeout expires.
        List<MockCoordinatorTimer.ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(45000 + 1);

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, 3, topic.metadataHash(), 0, getDefaultAssignmentConfigs(), -1, -1),
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
            context.groupMetadataManager.streamsGroup(groupId)
        );

        context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, memberId, instanceId, JOIN_GROUP_MEMBER_EPOCH)
                .setProcessId(DEFAULT_PROCESS_ID)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
        );

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

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(topic.metadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, groupEpoch)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withTargetAssignment(oldMemberId, targetAssignment)
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs()))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();
        
        assertEquals(0, context.groupMetadataManager.streamsGroup(groupId).endpointInformationEpoch());

        // First Join -> First Input
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            staticJoinHeartbeat(groupId, oldMemberId, instanceId, topic)
                .setUserEndpoint(firstUserEndpoint) // first input
        );

        // First Check
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(oldMemberId)
                .setMemberEpoch(bumpedEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(topic.responseTasks(0, 1, 2))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of())
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

        // static member leaves with epoch -2.
        context.streamsGroupHeartbeat(
            staticHeartbeat(groupId, oldMemberId, instanceId, StreamsGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH)
        );

        // static member rejoins.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> rejoinResult = context.streamsGroupHeartbeat(
            staticJoinHeartbeat(groupId, rejoinMemberId, instanceId, topic)
                .setUserEndpoint(secondUserEndpoint)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(rejoinMemberId)
                .setMemberEpoch(bumpedEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setAcceptableRecoveryLag(10000)
                .setActiveTasks(topic.responseTasks(0, 1, 2))
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of())
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