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
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.streams.MockTaskAssignor;
import org.apache.kafka.coordinator.group.streams.StreamsGroupBuilder;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.TasksTuple;
import org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ADDRESS;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ID;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_PROCESS_ID;
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
                fixture.joinRequest("newMemberId", fixture.activeInstanceId),
                UnreleasedInstanceIdException.class
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
}