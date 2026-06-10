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
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.CopartitionGroup;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Subtopology;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.TopicInfo;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Topology;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer.ExpiredTimeout;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer.ScheduledTimeout;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue.Endpoint;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.streams.MockTaskAssignor;
import org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState;
import org.apache.kafka.coordinator.group.streams.StreamsGroupBuilder;
import org.apache.kafka.coordinator.group.streams.StreamsGroupDescribeResult;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import org.apache.kafka.coordinator.group.streams.TasksTuple;
import org.apache.kafka.coordinator.group.streams.TasksTupleWithEpochs;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorException;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordsEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertResponseEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertUnorderedRecordsEquals;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupRebalanceTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupSessionTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ADDRESS;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ID;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_PROCESS_ID;
import static org.apache.kafka.coordinator.group.Utils.computeGroupHash;
import static org.apache.kafka.coordinator.group.Utils.computeTopicHash;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTupleWithCommonEpoch;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTupleWithEpochs;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksWithEpochs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link GroupMetadataManager} streams-group (KIP-1071) behaviour:
 * streams heartbeat, topology validation, task assignment, describe, and streams-group
 * record replay ({@code testReplayStreamsGroup*}).
 */
public class GroupMetadataManagerStreamsGroupTest {

    @Test
    public void testStreamsGroupMetadataReplayRoundTripsTopologyDescriptionEpochs() {
        // KIP-1331: replay must read storedDescriptionTopologyEpoch and failedDescriptionTopologyEpoch from the record
        // and apply them to the in-memory streams group.
        String groupId = "streams-group";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();

        // Initial replay sets both epochs.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
            groupId, 1, 0L, -1, Map.of(), 7, 5));

        StreamsGroup group = context.groupMetadataManager.getStreamsGroupOrThrow(groupId);
        assertEquals(7, group.storedDescriptionTopologyEpoch());
        assertEquals(5, group.failedDescriptionTopologyEpoch());

        // A subsequent replay carrying defaults (-1, -1) overwrites — the latest record wins.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
            groupId, 2, 0L, -1, Map.of(), -1, -1));
        assertEquals(-1, group.storedDescriptionTopologyEpoch());
        assertEquals(-1, group.failedDescriptionTopologyEpoch());
    }

    @Test
    public void testStreamsGroupDescribeSurfacesStoredDescriptionTopologyEpoch() {
        // KIP-1331: describe bundles per-group storedDescriptionTopologyEpoch so the service layer can decide
        // whether to consult the topology description plugin.
        String groupId = "streams-group";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();

        StreamsGroupTopologyValue topology = new StreamsGroupTopologyValue().setEpoch(0);
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(groupId, topology));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
            groupId, 1, 0L, -1, Map.of(), 9, -1));
        // Describe reads at lastCommittedOffset, so commit to make the replay visible.
        context.commit();

        StreamsGroupDescribeResult result = context.groupMetadataManager.streamsGroupDescribe(
            List.of(groupId, "missing-group"), context.lastCommittedOffset);

        // Two described groups: one found, one not.
        assertEquals(2, result.describedGroups().size());
        // Only the found group contributes to the epoch map.
        assertEquals(Map.of(groupId, 9), result.storedDescriptionTopologyEpochs());
    }

    @Test
    public void testStreamsGroupDescribeReadsStoredDescriptionTopologyEpochAtCommittedOffset() {
        // KIP-1331: describe must read storedDescriptionTopologyEpoch at committedOffset so the bundled result is a
        // single consistent snapshot — describedGroup and storedDescriptionTopologyEpoch must agree on which write
        // the reader is observing. Writing an uncommitted record that flips the value to a new epoch
        // must not leak into a describe issued at the old committedOffset.
        String groupId = "streams-group";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();

        StreamsGroupTopologyValue topology = new StreamsGroupTopologyValue().setEpoch(0);
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(groupId, topology));
        // First metadata record: stored=3.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
            groupId, 1, 0L, -1, Map.of(), 3, -1));
        context.commit();
        long offsetWithThree = context.lastCommittedOffset;

        // Apply a newer (uncommitted) record bumping stored to 7.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
            groupId, 2, 0L, -1, Map.of(), 7, -1));

        // Describing at the older committed offset must see the old value (3), not the uncommitted 7.
        StreamsGroupDescribeResult oldSnapshot = context.groupMetadataManager.streamsGroupDescribe(
            List.of(groupId), offsetWithThree);
        assertEquals(Map.of(groupId, 3), oldSnapshot.storedDescriptionTopologyEpochs());

        // After committing, describing at the new committed offset reflects the new value.
        context.commit();
        StreamsGroupDescribeResult newSnapshot = context.groupMetadataManager.streamsGroupDescribe(
            List.of(groupId), context.lastCommittedOffset);
        assertEquals(Map.of(groupId, 7), newSnapshot.storedDescriptionTopologyEpochs());
    }

    @Test
    public void testValidateStreamsGroupMemberThrowsWhenGroupAbsent() {
        // KIP-1331: validateStreamsGroupMember surfaces GROUP_ID_NOT_FOUND for the upcoming
        // StreamsGroupTopologyDescriptionUpdate handler.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();
        assertThrows(GroupIdNotFoundException.class,
            () -> context.groupMetadataManager.validateStreamsGroupMember(
                "nonexistent", "m1", context.lastCommittedOffset));
    }

    @Test
    public void testValidateStreamsGroupMemberThrowsWhenMemberAbsent() {
        // KIP-1331: validateStreamsGroupMember surfaces UNKNOWN_MEMBER_ID for the upcoming
        // StreamsGroupTopologyDescriptionUpdate handler.
        String groupId = "streams-group";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();
        // Replaying a metadata record materializes a streams group with no members; commit so the
        // group is visible at lastCommittedOffset.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
            groupId, 1, 0L, -1, Map.of(), -1, -1));
        context.commit();

        assertThrows(UnknownMemberIdException.class,
            () -> context.groupMetadataManager.validateStreamsGroupMember(
                groupId, "stranger", context.lastCommittedOffset));
    }

    @Test
    public void testValidateStreamsGroupMemberDoesNotSeeUncommittedFence() {
        // KIP-1331: validateStreamsGroupMember reads at committedOffset, so an uncommitted fence
        // (member tombstone) must not make a still-committed member appear unknown.
        String groupId = "streams-group";
        String memberId = "m1";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();

        // Commit the group + a member. Members need both metadata and current-assignment records
        // to be fully materialized on replay.
        StreamsGroupMember member = streamsGroupMemberBuilderWithDefaults(memberId).build();
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
            groupId, 1, 0L, -1, Map.of(), -1, -1));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, member));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, member));
        context.commit();
        long committedWithMember = context.lastCommittedOffset;

        // Apply uncommitted tombstones that fence the member (same order as removeStreamsMember).
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId));

        // Validating at the still-committed offset must succeed; the uncommitted tombstone is invisible.
        StreamsGroupMember resolved = context.groupMetadataManager.validateStreamsGroupMember(
            groupId, memberId, committedWithMember);
        assertEquals(memberId, resolved.memberId());

        // Latest in-memory state, by contrast, sees the tombstone — verify by querying with Long.MAX_VALUE.
        assertThrows(UnknownMemberIdException.class,
            () -> context.groupMetadataManager.validateStreamsGroupMember(groupId, memberId, Long.MAX_VALUE));
    }

    @Test
    public void testStreamsGroupMemberCanRejoinWithEpochZero() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Topology topology = new Topology()
            .setEpoch(1)
            .setSubtopologies(List.of(
                new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
            ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Set up a Streams group member with epoch 100.
        StreamsGroupMember member = streamsGroupMemberBuilderWithDefaults(memberId)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setTopologyEpoch(1)
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 100, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)))
            .build();

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, member));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(groupId, topology));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, 100, computeGroupHash(Map.of(
            fooTopicName, fooTopicHash
        )), 1, new TreeMap<>(Map.of("num.standby.replicas", "0")), -1, -1));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId,
            TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)
            )));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 100, 12345L));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, member));

        // Member rejoins with epoch=0 - should succeed per KIP-848.
        // Since the topology/metadata hasn't changed, group epoch stays at 100.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(100)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );
    }

    @Test
    public void testOnLoadedWithStreamsGroup() {
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build()))
            .withStreamsGroup(new StreamsGroupBuilder("foo", 10)
                .withMember(new StreamsGroupMember.Builder("foo-1")
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
                    .setMemberEpoch(9)
                    .setPreviousMemberEpoch(9)
                    .setProcessId("process-id")
                    .setRackId(null)
                    .setInstanceId(null)
                    .setRebalanceTimeoutMs(100)
                    .setClientTags(new HashMap<>())
                    .setTopologyEpoch(1)
                    .setUserEndpoint(new Endpoint().setHost("localhost").setPort(1500))
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 9,
                        TaskAssignmentTestUtil.mkTasks(fooTopicName, 0, 1, 2)))
                    .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 9,
                        TaskAssignmentTestUtil.mkTasks(fooTopicName, 3, 4, 5)))
                    .build())
                .withMember(new StreamsGroupMember.Builder("foo-2")
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setProcessId("process-id")
                    .setRackId(null)
                    .setInstanceId(null)
                    .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
                    .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                    .setRebalanceTimeoutMs(100)
                    .setClientTags(new HashMap<>())
                    .setTopologyEpoch(1)
                    .setUserEndpoint(new Endpoint().setHost("localhost").setPort(1500))
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .build())
                .withTargetAssignment("foo-1", TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(fooTopicName, 3, 4, 5)))
                .withTargetAssignmentEpoch(10))
            .build();

        // Let's assume that all the records have been replayed and now
        // onLoaded is called to signal it.
        context.groupMetadataManager.onLoaded();

        // All members should have a session timeout in place.
        assertNotNull(context.timer.timeout(groupSessionTimeoutKey("foo", "foo-1")));
        assertNotNull(context.timer.timeout(groupSessionTimeoutKey("foo", "foo-2")));

        // foo-1 should also have a revocation timeout in place.
        assertNotNull(context.timer.timeout(groupRebalanceTimeoutKey("foo", "foo-1")));
    }

    @Test
    public void testUpdateStreamsGroupSizeCounter() {
        List<String> groupIds = new ArrayList<>();
        IntStream.range(0, 5).forEach(i -> groupIds.add("group-" + i));
        List<String> streamsMemberIds = List.of("streams-member-id-0", "streams-member-id-1", "streams-member-id-2", "streams-member-id-3");

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroup(new StreamsGroupBuilder(groupIds.get(0), 10)) // Empty group
            .withStreamsGroup(new StreamsGroupBuilder(groupIds.get(1), 10) // Stable group
                .withTargetAssignmentEpoch(10)
                .withTopology(new StreamsTopology(1, Map.of()))
                .withValidatedTopologyEpoch(1)
                .withMember(streamsGroupMemberBuilderWithDefaults(streamsMemberIds.get(0))
                    .setMemberEpoch(10)
                    .build()))
            .withStreamsGroup(new StreamsGroupBuilder(groupIds.get(2), 10) // Assigning group
                .withTargetAssignmentEpoch(9)
                .withTopology(new StreamsTopology(1, Map.of()))
                .withValidatedTopologyEpoch(1)
                .withMember(streamsGroupMemberBuilderWithDefaults(streamsMemberIds.get(1))
                    .setMemberEpoch(9)
                    .build()))
            .withStreamsGroup(new StreamsGroupBuilder(groupIds.get(3), 10) // Reconciling group
                .withTargetAssignmentEpoch(10)
                .withTopology(new StreamsTopology(1, Map.of()))
                .withValidatedTopologyEpoch(1)
                .withMember(streamsGroupMemberBuilderWithDefaults(streamsMemberIds.get(2))
                    .setMemberEpoch(9)
                    .build()))
            .withStreamsGroup(new StreamsGroupBuilder(groupIds.get(4), 10) // NotReady group
                .withTargetAssignmentEpoch(10)
                .withTopology(new StreamsTopology(1, Map.of()))
                .withMember(streamsGroupMemberBuilderWithDefaults(streamsMemberIds.get(3))
                    .build()))
            .build();

        context.groupMetadataManager.updateGroupSizeCounter();
        verify(context.metrics, times(1)).setStreamsGroupGauges(eq(Utils.mkMap(
            Utils.mkEntry(StreamsGroup.StreamsGroupState.EMPTY, 1L),
            Utils.mkEntry(StreamsGroup.StreamsGroupState.ASSIGNING, 1L),
            Utils.mkEntry(StreamsGroup.StreamsGroupState.RECONCILING, 1L),
            Utils.mkEntry(StreamsGroup.StreamsGroupState.NOT_READY, 1L),
            Utils.mkEntry(StreamsGroup.StreamsGroupState.STABLE, 1L)
        )));

        context.groupMetadataManager.getStreamsGroupOrThrow(groupIds.get(1))
            .removeMember(streamsMemberIds.get(0));
        context.groupMetadataManager.getStreamsGroupOrThrow(groupIds.get(3))
            .updateMember(streamsGroupMemberBuilderWithDefaults(streamsMemberIds.get(2)).setMemberEpoch(10).build());

        context.groupMetadataManager.updateGroupSizeCounter();
        verify(context.metrics, times(1)).setStreamsGroupGauges(eq(Utils.mkMap(
            Utils.mkEntry(StreamsGroup.StreamsGroupState.EMPTY, 2L),
            Utils.mkEntry(StreamsGroup.StreamsGroupState.ASSIGNING, 1L),
            Utils.mkEntry(StreamsGroup.StreamsGroupState.NOT_READY, 1L),
            Utils.mkEntry(StreamsGroup.StreamsGroupState.STABLE, 1L)
        )));
    }

    private StreamsGroupMember.Builder streamsGroupMemberBuilderWithDefaults(String memberId) {
        return new StreamsGroupMember.Builder(memberId)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setRackId(null)
            .setInstanceId(null)
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

    @Test
    public void testStreamsGroupDescribeNoErrors() {
        List<String> streamsGroupIds = Arrays.asList("group-id-1", "group-id-2");
        int epoch = 10;
        String memberId = "member-id";
        StreamsGroupMember.Builder memberBuilder = streamsGroupMemberBuilderWithDefaults(memberId)
            .setClientTags(Map.of("clientTag", "clientValue"))
            .setProcessId("processId")
            .setMemberEpoch(epoch)
            .setPreviousMemberEpoch(epoch - 1);
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        StreamsTopology topology = new StreamsTopology(
            0,
            Map.of(subtopology1,
                new StreamsGroupTopologyValue.Subtopology()
                    .setSubtopologyId(subtopology1)
                    .setSourceTopics(List.of(fooTopicName))
            )
        );

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroup(new StreamsGroupBuilder(streamsGroupIds.get(0), epoch)
                .withTopology(topology)
            )
            .withStreamsGroup(new StreamsGroupBuilder(streamsGroupIds.get(1), epoch)
                .withMember(memberBuilder.build())
                .withTopology(topology)
            )
            .build();

        StreamsGroupDescribeResponseData.Topology expectedTopology =
            new StreamsGroupDescribeResponseData.Topology()
                .setEpoch(0)
                .setSubtopologies(List.of(
                    new StreamsGroupDescribeResponseData.Subtopology()
                        .setSubtopologyId(subtopology1)
                        .setSourceTopics(List.of(fooTopicName))
                ));

        List<StreamsGroupDescribeResponseData.DescribedGroup> expected = Arrays.asList(
            new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(epoch)
                .setGroupId(streamsGroupIds.get(0))
                .setGroupState(StreamsGroupState.EMPTY.toString())
                .setAssignmentEpoch(1)
                .setTopology(expectedTopology),
            new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(epoch)
                .setGroupId(streamsGroupIds.get(1))
                .setMembers(List.of(
                    memberBuilder.build().asStreamsGroupDescribeMember(
                        TasksTuple.EMPTY
                    )
                ))
                .setTopology(expectedTopology)
                .setGroupState(StreamsGroupState.NOT_READY.toString())
                .setAssignmentEpoch(1)
        );
        List<StreamsGroupDescribeResponseData.DescribedGroup> actual = context.sendStreamsGroupDescribe(streamsGroupIds);

        assertEquals(expected, actual);
    }

    @Test
    public void testStreamsGroupDescribeWithErrors() {
        String groupId = "groupId";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();

        List<StreamsGroupDescribeResponseData.DescribedGroup> actual = context.sendStreamsGroupDescribe(List.of(groupId));
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
            .setErrorMessage("Group groupId not found.");
        List<StreamsGroupDescribeResponseData.DescribedGroup> expected = List.of(describedGroup);

        assertEquals(expected, actual);
    }

    @Test
    public void testStreamsGroupDescribeBeforeAndAfterCommittingOffset() {
        String streamsGroupId = "streamsGroupId";
        int epoch = 10;
        String memberId1 = "memberId1";
        String memberId2 = "memberId2";
        String subtopologyId = "subtopology1";
        String fooTopicName = "foo";
        StreamsGroupTopologyValue topology = new StreamsGroupTopologyValue()
            .setEpoch(0)
            .setSubtopologies(
                List.of(
                    new StreamsGroupTopologyValue.Subtopology()
                        .setSubtopologyId(subtopologyId)
                        .setSourceTopics(List.of(fooTopicName))
                )
            );

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();

        StreamsGroupMember.Builder memberBuilder1 = streamsGroupMemberBuilderWithDefaults(memberId1);
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(streamsGroupId, memberBuilder1.build()));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(streamsGroupId, memberBuilder1.build()));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(streamsGroupId, epoch + 1, 0, -1, Map.of(), -1, -1));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(streamsGroupId, topology));

        TasksTuple assignment = new TasksTuple(
            Map.of(subtopologyId, Set.of(0, 1)),
            Map.of(subtopologyId, Set.of(0, 1)),
            Map.of(subtopologyId, Set.of(0, 1))
        );

        StreamsGroupMember.Builder memberBuilder2 = streamsGroupMemberBuilderWithDefaults(memberId2);
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(streamsGroupId, memberBuilder2.build()));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(streamsGroupId, memberId2, assignment));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(streamsGroupId, epoch + 1, 12345L));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(streamsGroupId, memberBuilder2.build()));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(streamsGroupId, epoch + 2, 0, 0, Map.of(), -1, -1));

        List<StreamsGroupDescribeResponseData.DescribedGroup> actual = context.groupMetadataManager.streamsGroupDescribe(List.of(streamsGroupId), context.lastCommittedOffset).describedGroups();
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(streamsGroupId)
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
            .setErrorMessage("Group streamsGroupId not found.");
        assertEquals(1, actual.size());
        assertEquals(describedGroup, actual.get(0));

        // Commit the offset and test again
        context.commit();

        actual = context.groupMetadataManager.streamsGroupDescribe(List.of(streamsGroupId), context.lastCommittedOffset).describedGroups();
        describedGroup = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(streamsGroupId)
            .setMembers(Arrays.asList(
                memberBuilder1.build().asStreamsGroupDescribeMember(TasksTuple.EMPTY),
                memberBuilder2.build().asStreamsGroupDescribeMember(assignment)
            ))
            .setTopology(
                new StreamsGroupDescribeResponseData.Topology()
                    .setEpoch(0)
                    .setSubtopologies(
                        List.of(
                            new StreamsGroupDescribeResponseData.Subtopology()
                                .setSubtopologyId(subtopologyId)
                                .setSourceTopics(List.of(fooTopicName))
                        )
                    )
            )
            .setGroupState(StreamsGroup.StreamsGroupState.ASSIGNING.toString())
            .setGroupEpoch(epoch + 2)
            .setAssignmentEpoch(epoch + 1);
        assertEquals(1, actual.size());
        assertEquals(describedGroup, actual.get(0));
    }

    @Test
    public void testStreamsGroupDeleteCancelsInitialRebalanceTimer() {
        String groupId = "streams-group-id";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 1000)
            .build();

        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1))));

        context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        String timerKey = GroupMetadataManager.streamsInitialRebalanceKey(groupId);
        assertTrue(context.timer.isScheduled(timerKey), "Timer should be scheduled after first member joins");

        List<CoordinatorRecord> records = new ArrayList<>();
        context.groupMetadataManager.createGroupTombstoneRecordsAndCancelTimers(groupId, records);

        assertFalse(context.timer.isScheduled(timerKey), "Timer should be cancelled after group deletion");

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataTombstoneRecord(groupId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupEpochTombstoneRecord(groupId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecordTombstone(groupId)
        );
        assertEquals(expectedRecords, records);
    }

    @Test
    public void testUnknownStreamsGroupId() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupIdNotFoundException e = assertThrows(GroupIdNotFoundException.class, () ->
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(100) // Epoch must be > 0.
                    .setRebalanceTimeoutMs(1500)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())));
        assertEquals("Streams group fooup not found.", e.getMessage());
    }

    @Test
    public void testUnknownMemberIdJoinsStreamsGroup() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        Topology topology = new Topology();

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .build();

        assignor.prepareGroupAssignment(Map.of(memberId, TasksTuple.EMPTY));

        // A first member joins to create the group.
        context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        // The second member is rejected because the member id is unknown and
        // the member epoch is not zero.
        final String memberId2 = Uuid.randomUuid().toString();
        UnknownMemberIdException e = assertThrows(UnknownMemberIdException.class, () ->
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId2)
                    .setMemberEpoch(1)
                    .setRebalanceTimeoutMs(1500)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())));
        assertEquals(String.format("Member %s is not a member of group %s.", memberId2, groupId), e.getMessage());
    }

    @Test
    public void testStreamsGroupMemberEpochValidation() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .buildCoordinatorMetadataImage())
            .build();
        assignor.prepareGroupAssignment(Map.of(memberId, TasksTuple.EMPTY));

        StreamsGroupMember member = streamsGroupMemberBuilderWithDefaults(memberId)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 100, TaskAssignmentTestUtil.mkTasks(subtopology1, 1, 2, 3)))
            .build();

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, member));

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, 100, 0, 0, Map.of(), -1, -1));

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(groupId, topology));

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId,
            TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 1, 2, 3)
            )));

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 100, 12345L));

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, member));

        // Member epoch is greater than the expected epoch.
        FencedMemberEpochException e1 = assertThrows(FencedMemberEpochException.class, () ->
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(200)
                    .setRebalanceTimeoutMs(1500)));
        assertEquals("The streams group member has a greater member epoch (200) than the one known by the group coordinator (100). "
            + "The member must abandon all its partitions and rejoin.", e1.getMessage());

        // Member epoch is smaller than the expected epoch.
        FencedMemberEpochException e2 = assertThrows(FencedMemberEpochException.class, () ->
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(50)
                    .setRebalanceTimeoutMs(1500)));
        assertEquals("The streams group member has a smaller member epoch (50) than the one known by the group coordinator (100). "
            + "The member must abandon all its partitions and rejoin.", e2.getMessage());

        // Member joins with previous epoch but without providing tasks.
        FencedMemberEpochException e3 = assertThrows(FencedMemberEpochException.class, () ->
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(99)
                    .setRebalanceTimeoutMs(1500)));
        assertEquals("The streams group member has a smaller member epoch (99) than the one known by the group coordinator (100). "
            + "The member must abandon all its partitions and rejoin.", e3.getMessage());

        // Member joins with previous epoch and has a subset of the owned tasks.
        // This is accepted as the response with the bumped epoch may have been lost.
        // In this case, we provide back the correct epoch to the member.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(99)
                .setRebalanceTimeoutMs(1500)
                .setActiveTasks(List.of(new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopology1)
                    .setPartitions(List.of(1, 2))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));
        assertEquals(100, result.response().data().memberEpoch());
    }

    @Test
    public void testStreamsOwnedTasksValidation() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String subtopologyMissing = "subtopologyMissing";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 3)
                .buildCoordinatorMetadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)))
                    .build())
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withTargetAssignment(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)))
                .withTargetAssignmentEpoch(10)
            )
            .build();

        InvalidRequestException e1 = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatRequestData.TaskIds()
                        .setSubtopologyId(subtopologyMissing)
                        .setPartitions(List.of(0))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())));
        assertEquals("Subtopology subtopologyMissing does not exist in the topology.", e1.getMessage());

        InvalidRequestException e2 = assertThrows(InvalidRequestException.class, () -> context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatRequestData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(3))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())));
        assertEquals("Task 3 for subtopology subtopology1 is invalid. Number of tasks for this subtopology: 3", e2.getMessage());
    }

    @Test
    public void testStreamsNewMemberIsRejectedWithMaximumMembersIsReached() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();
        Topology topology = new Topology().setSubtopologies(List.of());

        // Create a context with one streams group containing two members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new MetadataImageBuilder().buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, 2)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .build())
                .withTargetAssignmentEpoch(10)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
            )
            .build();

        assertThrows(GroupMaxSizeReachedException.class, () ->
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId3)
                    .setMemberEpoch(0)
                    .setProcessId("process-id")
                    .setRebalanceTimeoutMs(1500)
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())
            ));
    }

    @Test
    public void testMemberJoinsEmptyStreamsGroup() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();

        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String subtopology2 = "subtopology2";
        String barTopicName = "bar";
        Uuid barTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName)),
            new Subtopology().setSubtopologyId(subtopology2).setSourceTopics(List.of(barTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, metadataImage),
            barTopicName, computeTopicHash(barTopicName, metadataImage)
        ));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        assignor.prepareGroupAssignment(Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
            TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5),
            TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1, 2)
        )));

        assertThrows(GroupIdNotFoundException.class, () ->
            context.groupMetadataManager.streamsGroup(groupId));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setProcessId("process-id")
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2, 3, 4, 5)),
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology2)
                        .setPartitions(List.of(0, 1, 2))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(1500)
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 2,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5),
                TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1, 2)))
            
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(groupId, topology),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId,
                2,
                groupMetadataHash,
                0,
                new TreeMap<>(Map.of(
                    "num.standby.replicas", "0"
                )),
                -1,
                -1
            ),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId,
                TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5),
                    TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1, 2)
                )),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testJoinEmptyStreamsGroupAndDescribe() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();

        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        assignor.prepareGroupAssignment(Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
            TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)
        )));

        assertThrows(GroupIdNotFoundException.class, () ->
            context.groupMetadataManager.streamsGroup(groupId));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setProcessId("process-id")
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(1500)
            .setAssignedTasks(TaskAssignmentTestUtil.mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 1,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
            .build();

        // Commit the offset, so that the latest state will be described below
        context.commit();

        List<StreamsGroupDescribeResponseData.DescribedGroup> actualDescribedGroups = context.groupMetadataManager.streamsGroupDescribe(List.of(groupId), context.lastCommittedOffset).describedGroups();
        StreamsGroupDescribeResponseData.DescribedGroup expectedDescribedGroup = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setAssignmentEpoch(2)
            .setTopology(
                new StreamsGroupDescribeResponseData.Topology()
                    .setEpoch(0)
                    .setSubtopologies(List.of(
                        new StreamsGroupDescribeResponseData.Subtopology()
                            .setSubtopologyId(subtopology1)
                            .setSourceTopics(List.of(fooTopicName))
                    ))
            )
            .setMembers(Collections.singletonList(
                expectedMember.asStreamsGroupDescribeMember(TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
            ))
            .setGroupState(StreamsGroupState.STABLE.toString())
            .setGroupEpoch(2);
        assertEquals(1, actualDescribedGroups.size());
        assertEquals(expectedDescribedGroup, actualDescribedGroups.get(0));
    }

    @Test
    public void testStreamsGroupMemberJoiningWithMissingSourceTopic() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String subtopology2 = "subtopology2";
        String barTopicName = "bar";
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName)),
            new Subtopology().setSubtopologyId(subtopology2).setSourceTopics(List.of(barTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        // Member joins the streams group.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setProcessId(DEFAULT_PROCESS_ID)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertEquals(
            Map.of(),
            result.response().creatableTopics()
        );
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of(new StreamsGroupHeartbeatResponseData.Status()
                    .setStatusCode(Status.MISSING_SOURCE_TOPICS.code())
                    .setStatusDetail("Source topics bar are missing.")))
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(1500)
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(groupId, topology),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId,
                2,
                computeGroupHash(Map.of(fooTopicName, computeTopicHash(fooTopicName, metadataImage))),
                -1,
                new TreeMap<>(Map.of(
                    "num.standby.replicas", "0"
                )),
                -1,
                -1
            ),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId, TasksTuple.EMPTY),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testStreamsGroupMemberJoiningWithMissingInternalTopic() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Topology topology = new Topology().setSubtopologies(List.of(
                new Subtopology()
                    .setSubtopologyId(subtopology1)
                    .setSourceTopics(List.of(fooTopicName))
                    .setStateChangelogTopics(List.of(new TopicInfo().setName(barTopicName)))
            )
        );

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        // Member joins the streams group.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setProcessId(DEFAULT_PROCESS_ID)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertEquals(
            Map.of(barTopicName,
                new CreatableTopic()
                    .setName(barTopicName)
                    .setNumPartitions(6)
                    .setReplicationFactor((short) -1)
            ),
            result.response().creatableTopics()
        );
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of(new StreamsGroupHeartbeatResponseData.Status()
                    .setStatusCode(Status.MISSING_INTERNAL_TOPICS.code())
                    .setStatusDetail("Internal topics are missing: bar")))
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(1500)
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(groupId, topology),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId,
                2,
                computeGroupHash(Map.of(fooTopicName, computeTopicHash(fooTopicName, metadataImage))),
                -1,
                new TreeMap<>(Map.of(
                    "num.standby.replicas", "0"
                )),
                -1,
                -1
            ),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId, TasksTuple.EMPTY),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertEquals(StreamsGroupState.NOT_READY, context.streamsGroupState(groupId));
        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testStreamsGroupMemberJoiningWithIncorrectlyPartitionedTopic() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid barTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
                new Subtopology()
                    .setSubtopologyId(subtopology1)
                    .setSourceTopics(List.of(fooTopicName, barTopicName))
                    .setCopartitionGroups(List.of(new CopartitionGroup().setSourceTopics(List.of((short) 0, (short) 1))))
            )
        );

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage();
        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        // Member joins the streams group.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setProcessId(DEFAULT_PROCESS_ID)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertEquals(
            Map.of(),
            result.response().creatableTopics()
        );
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of(new StreamsGroupHeartbeatResponseData.Status()
                    .setStatusCode(Status.INCORRECTLY_PARTITIONED_TOPICS.code())
                    .setStatusDetail("Following topics do not have the same number of partitions: [{bar=3, foo=6}]")))
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(1500)
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(groupId, topology),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId,
                2,
                computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                )),
                -1,
                new TreeMap<>(Map.of(
                    "num.standby.replicas", "0"
                )),
                -1,
                -1
            ),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId, TasksTuple.EMPTY),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertEquals(StreamsGroupState.NOT_READY, context.streamsGroupState(groupId));
        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testStreamsGroupMemberJoiningWithStaleTopology() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid barTopicId = Uuid.randomUuid();
        Topology topology0 = new Topology().setEpoch(0).setSubtopologies(List.of(
                new Subtopology()
                    .setSubtopologyId(subtopology1)
                    .setSourceTopics(List.of(fooTopicName))
            )
        );
        Topology topology1 = new Topology().setEpoch(1).setSubtopologies(List.of(
                new Subtopology()
                    .setSubtopologyId(subtopology1)
                    .setSourceTopics(List.of(fooTopicName, barTopicName))
            )
        );

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage();
        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .withStreamsGroup(
                new StreamsGroupBuilder(groupId, 10)
                    .withTopology(StreamsTopology.fromHeartbeatRequest(topology1))
                    .withValidatedTopologyEpoch(1)
            )
            .build();

        assignor.prepareGroupAssignment(new org.apache.kafka.coordinator.group.streams.assignor.GroupAssignment(Map.of(
            memberId, org.apache.kafka.coordinator.group.streams.assignor.MemberAssignment.empty()
        )));

        // Member joins the streams group.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology0)
                .setProcessId(DEFAULT_PROCESS_ID)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertEquals(
            Map.of(),
            result.response().creatableTopics()
        );
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of(new StreamsGroupHeartbeatResponseData.Status()
                    .setStatusCode(Status.STALE_TOPOLOGY.code())
                    .setStatusDetail("The member's topology epoch 0 is behind the group's topology epoch 1.")))
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(1500)
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId,
                11,
                computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                )),
                1,
                new TreeMap<>(Map.of(
                    "num.standby.replicas", "0"
                )),
                -1,
                -1
            ),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId, TasksTuple.EMPTY),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testStreamsGroupMemberRequestingShutdownApplication() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(fooTopicName, computeTopicHash(fooTopicName, metadataImage)));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId1)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)))
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId2)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 3, 4, 5)))
                    .build())
                .withTargetAssignment(memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)))
                .withTargetAssignment(memberId2, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 3, 4, 5)))
                .withTargetAssignmentEpoch(10)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withMetadataHash(groupMetadataHash)
                .withValidatedTopologyEpoch(0)
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
            )
            .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result1 = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setShutdownApplication(true)
        );

        String statusDetail = String.format("Streams group member %s encountered a fatal error and requested a shutdown for the entire application.", memberId1);

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setStatus(List.of(
                    new StreamsGroupHeartbeatResponseData.Status()
                        .setStatusCode(Status.SHUTDOWN_APPLICATION.code())
                        .setStatusDetail(statusDetail)
                ))
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result1.response().data()
        );
        assertRecordsEquals(List.of(), result1.records());

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result2 = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setStatus(List.of(
                    new StreamsGroupHeartbeatResponseData.Status()
                        .setStatusCode(Status.SHUTDOWN_APPLICATION.code())
                        .setStatusDetail(statusDetail)
                ))
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result2.response().data()
        );

        assertRecordsEquals(List.of(), result2.records());
    }

    @Test
    public void testStreamsGroupMemberRequestingShutdownApplicationUponLeaving() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .buildCoordinatorMetadataImage();

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        long metadataHash = computeGroupHash(Map.of(fooTopicName, computeTopicHash(fooTopicName, metadataImage)));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId1)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId2)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .build())
                .withTargetAssignmentEpoch(10)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(metadataHash)
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs())
            )
            .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result1 = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setShutdownApplication(true)
        );

        String statusDetail = String.format("Streams group member %s encountered a fatal error and requested a shutdown for the entire application.", memberId1);

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setStatus(List.of()),
            result1.response().data()
        );
        assertRecordsEquals(
            List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId1),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, 11, metadataHash, 0, getDefaultAssignmentConfigs(), -1, -1)
            ),
            result1.records()
        );

        assignor.prepareGroupAssignment(Map.of());

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result2 = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setStatus(List.of(
                    new StreamsGroupHeartbeatResponseData.Status()
                        .setStatusCode(Status.SHUTDOWN_APPLICATION.code())
                        .setStatusDetail(statusDetail)
                ))
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result2.response().data()
        );
    }

    @Test
    public void testStreamsUpdatingMemberMetadataTriggersNewTargetAssignment() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String subtopology2 = "subtopology2";
        String barTopicName = "bar";
        Uuid barTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName)),
            new Subtopology().setSubtopologyId(subtopology2).setSourceTopics(List.of(barTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage();

        long groupMetadataHash = computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, metadataImage),
            barTopicName, computeTopicHash(barTopicName, metadataImage)
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withTargetAssignment(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
                .withTargetAssignmentEpoch(10)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withMetadataHash(groupMetadataHash)
            )
            .build();

        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5),
                TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1, 2)
            ))
        );

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10)
                .setProcessId("process-id2")
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2, 3, 4, 5)),
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology2)
                        .setPartitions(List.of(0, 1, 2))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                mkTasksWithEpochs(subtopology1,
                    Map.of(
                        0, 10,
                        1, 10,
                        2, 10,
                        3, 10,
                        4, 10,
                        5, 10
                    )),
                mkTasksWithEpochs(subtopology2,
                    Map.of(
                        0, 11,
                        1, 11,
                        2, 11
                    ))))
            .setProcessId("process-id2")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedMember),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId,
                11,
                groupMetadataHash,
                0,
                new TreeMap<>(Map.of(
                    "num.standby.replicas", "0"
                )),
                -1,
                -1
            ),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId,
                TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5),
                    TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1, 2)
                )),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testStreamsUpdatingPartitionMetadataTriggersNewTargetAssignment() {
        int changedPartitionCount = 6; // New partition count for the topic.
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String subtopology2 = "subtopology2";
        String barTopicName = "bar";
        Uuid barTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName)),
            new Subtopology().setSubtopologyId(subtopology2).setSourceTopics(List.of(barTopicName))
        ));

        CoordinatorMetadataImage newMetadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, changedPartitionCount)
            .buildCoordinatorMetadataImage();

        CoordinatorMetadataImage oldMetadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage();
        long oldGroupMetadataHash = computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, oldMetadataImage),
            barTopicName, computeTopicHash(barTopicName, oldMetadataImage)
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(newMetadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withTargetAssignment(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
                .withTargetAssignmentEpoch(10)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withMetadataHash(oldGroupMetadataHash)
            )
            .build();

        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5),
                TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1, 2)
            ))
        );

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10)
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2, 3, 4, 5)),
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology2)
                        .setPartitions(List.of(0, 1, 2))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                mkTasksWithEpochs(subtopology1,
                    Map.of(
                        0, 10,
                        1, 10,
                        2, 10,
                        3, 10,
                        4, 10,
                        5, 10
                    )),
                mkTasksWithEpochs(subtopology2,
                    Map.of(
                        0, 11,
                        1, 11,
                        2, 11
                    ))))
            .setProcessId("process-id2")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId,
                11,
                computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, newMetadataImage),
                    barTopicName, computeTopicHash(barTopicName, newMetadataImage)
                )),
                0,
                new TreeMap<>(Map.of(
                    "num.standby.replicas", "0"
                )),
                -1,
                -1
            ),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId,
                TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5),
                    TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1, 2)
                )),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testStreamsNewJoiningMemberTriggersNewTargetAssignment() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String subtopology2 = "subtopology2";
        String barTopicName = "bar";
        Uuid barTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName)),
            new Subtopology().setSubtopologyId(subtopology2).setSourceTopics(List.of(barTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage();

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .buildCoordinatorMetadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId1)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2),
                        TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1)))
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 3, 4, 5),
                        TaskAssignmentTestUtil.mkTasks(subtopology2, 2)))
                    .build())
                .withTargetAssignment(memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2),
                    TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1)))
                .withTargetAssignment(memberId2, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 3, 4, 5),
                    TaskAssignmentTestUtil.mkTasks(subtopology2, 2)))
                .withTargetAssignmentEpoch(10)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                )))
            )
            .build();

        assignor.prepareGroupAssignment(Map.of(
            memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1),
                TaskAssignmentTestUtil.mkTasks(subtopology2, 0)
            ),
            memberId2, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 2, 3),
                TaskAssignmentTestUtil.mkTasks(subtopology2, 1)
            ),
            memberId3, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 4, 5),
                TaskAssignmentTestUtil.mkTasks(subtopology2, 2)
            )
        ));

        // Member 3 joins the streams group.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setProcessId(DEFAULT_PROCESS_ID)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),

            result.response().data()
        );

    }

    @Test
    public void testStreamsLeavingMemberRemovesMemberAndBumpsGroupEpoch() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String subtopology2 = "subtopology2";
        String barTopicName = "bar";
        Uuid barTopicId = Uuid.randomUuid();

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .buildCoordinatorMetadataImage())
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2),
                        TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1)))
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 3, 4, 5),
                        TaskAssignmentTestUtil.mkTasks(subtopology2, 2)))
                    .build())
                .withTargetAssignment(memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2),
                    TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1)))
                .withTargetAssignment(memberId2, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 3, 4, 5),
                    TaskAssignmentTestUtil.mkTasks(subtopology2, 2)))
                .withTargetAssignmentEpoch(10))
            .build();

        // Member 2 leaves the streams group.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(1500)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setStatus(List.of()),
            result.response().data()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId2),
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, 11, 0, -1, Map.of(), -1, -1)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testStreamsGroupHeartbeatPartialResponseWhenNothingChanges() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1))));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result;

        // A full response should be sent back on joining.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        // Otherwise, a partial response should be sent back.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().data().memberEpoch()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );
    }

    @Test
    public void testStreamsGroupHeartbeatResponseVersion0() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1))));

        // Send version 0 heartbeat request
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()),
            (short) 0 // Version 0
        );

        StreamsGroupHeartbeatResult response = result.response();
        StreamsGroupHeartbeatResponseData data = response.data();

        assertEquals(0, data.acceptableRecoveryLagLegacy(),
            "Version 0 response should NOT include acceptableRecoveryLagLegacy (should be default 0)");
        // It's ok for version0 to set `acceptableRecoveryLag` because the field is marked as `ignorable`
        assertEquals(10_000L, data.acceptableRecoveryLag(),
            "Version 0 response should NOT include acceptableRecoveryLag (should be default 10_000L)");

        // Verify other fields are set correctly
        assertEquals(memberId, data.memberId());
        assertEquals(2, data.memberEpoch());
        assertEquals(5000, data.heartbeatIntervalMs());
        assertEquals(60_000, data.taskOffsetIntervalMs());

        // Verify that AcceptableRecoveryLag (versions: "1+", ignorable: true) is dropped when the data is serialized
        // at version 0 and deserialized again, since a version-0 receiver would not know about it.
        ByteBufferAccessor serializedV0 = MessageUtil.toByteBufferAccessor(data, (short) 0);
        StreamsGroupHeartbeatResponseData deserializedData = new StreamsGroupHeartbeatResponseData();
        deserializedData.read(serializedV0, (short) 0);

        assertEquals(0, deserializedData.acceptableRecoveryLagLegacy(),
            "AcceptableRecoveryLagLegacy must survive the version-0 roundtrip unchanged");
        assertEquals(-1L, deserializedData.acceptableRecoveryLag(),
            "AcceptableRecoveryLag (ignorable, versions 1+) must be absent after version-0 roundtrip; should revert to default -1, even if it was set to default 10_000L");
    }

    @Test
    public void testStreamsGroupHeartbeatResponseVersion1() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1))));

        // Send version 1 heartbeat request
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()),
            (short) 1  // Version 1
        );

        // Version 1 response should have AcceptableRecoveryLag (int64) set, not AcceptableRecoveryLagLegacy (int32)
        assertEquals(0, result.response().data().acceptableRecoveryLagLegacy(),
            "Version 1 response should NOT include acceptableRecoveryLagLegacy (should be default 0)");
        assertEquals(10_000L, result.response().data().acceptableRecoveryLag(),
            "Version 1 response should include acceptableRecoveryLag");

        // Verify other fields are set correctly
        assertEquals(memberId, result.response().data().memberId());
        assertEquals(2, result.response().data().memberEpoch());
        assertEquals(5000, result.response().data().heartbeatIntervalMs());
        assertEquals(60_000, result.response().data().taskOffsetIntervalMs());
    }

    @Test
    public void testStreamsGroupHeartbeatAlwaysSetsStatus() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1))));

        // Heartbeat with no errors should still have status field set to empty list.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        // Verify that status is always set, even when empty.
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        // Verify status field is present in subsequent heartbeats as well.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().data().memberEpoch()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );
    }

    @Test
    public void testStreamsInitialRebalanceDelayEmptyDuringDelayAssignsAfterTimer() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 1000)
            .build();

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result;

        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setPartitionsByUserEndpoint(null)
                .setEndpointInformationEpoch(0)
                .setStatus(List.of(
                    new StreamsGroupHeartbeatResponseData.Status()
                        .setStatusCode(Status.ASSIGNMENT_DELAYED.code())
                        .setStatusDetail("Assignment delayed due to the configured initial rebalance delay.")
                ))
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assignor.prepareGroupAssignment(
                Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1))));

        context.sleep(10000);

        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );
    }

    @Test
    public void testStreamsRebalanceDelayWhenJoiningEmptyGroupWithNonZeroEpoch() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 1000)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10))
            .build();

        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
        assertTrue(group.isEmpty());
        assertEquals(10, group.groupEpoch());

        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1))));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result;

        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(10000)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        int memberEpoch = result.response().data().memberEpoch();
        assertTrue(result.response().data().activeTasks().isEmpty());

        context.sleep(2000);

        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertFalse(result.response().data().activeTasks().isEmpty());
    }

    @Test
    public void testStreamsReconciliationProcess() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String subtopology2 = "subtopology2";
        String barTopicName = "bar";
        Uuid barTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName)),
            new Subtopology().setSubtopologyId(subtopology2).setSourceTopics(List.of(barTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, metadataImage),
            barTopicName, computeTopicHash(barTopicName, metadataImage)
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2),
                        TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1)))
                    .build())
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 3, 4, 5),
                        TaskAssignmentTestUtil.mkTasks(subtopology2, 2)))
                    .build())
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withTargetAssignment(memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2),
                    TaskAssignmentTestUtil.mkTasks(subtopology2, 0, 1)))
                .withTargetAssignment(memberId2, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 3, 4, 5),
                    TaskAssignmentTestUtil.mkTasks(subtopology2, 2)))
                .withTargetAssignmentEpoch(10)
                .withMetadataHash(groupMetadataHash)
                .withValidatedTopologyEpoch(0)
            )
            .build();

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(Map.of(
            memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1),
                TaskAssignmentTestUtil.mkTasks(subtopology2, 0)
            ),
            memberId2, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 2, 3),
                TaskAssignmentTestUtil.mkTasks(subtopology2, 2)
            ),
            memberId3, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 4, 5),
                TaskAssignmentTestUtil.mkTasks(subtopology2, 1)
            )
        ));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result;

        // Members in the group are in Stable state.
        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.STABLE, context.streamsGroupMemberState(groupId, memberId1));
        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.STABLE, context.streamsGroupMemberState(groupId, memberId2));
        assertEquals(StreamsGroup.StreamsGroupState.STABLE, context.streamsGroupState(groupId));

        // Member 3 joins the group. This triggers the computation of a new target assignment
        // for the group. Member 3 does not get any assigned tasks yet because they are
        // all owned by other members. However, it transitions to epoch 11 and the
        // Unreleased Tasks state.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        // We only check the last record as the subscription/target assignment updates are
        // already covered by other tests.
        assertRecordEquals(
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberId3)
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(0)
                .build()),
            result.records().get(result.records().size() - 1)
        );

        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS,
            context.streamsGroupMemberState(groupId, memberId3));
        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, context.streamsGroupState(groupId));

        // Member 1 heartbeats. It remains at epoch 10 but transitions to Unrevoked Tasks
        // state until it acknowledges the revocation of its tasks. The response contains the new
        // assignment without the tasks that must be revoked.
        result = context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1)),
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology2)
                        .setPartitions(List.of(0))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertRecordsEquals(List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberId1)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1),
                        TaskAssignmentTestUtil.mkTasks(subtopology2, 0)))
                    .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 2),
                        TaskAssignmentTestUtil.mkTasks(subtopology2, 1)))
                    .build())),
            result.records()
        );

        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS,
            context.streamsGroupMemberState(groupId, memberId1));
        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, context.streamsGroupState(groupId));

        // Member 2 heartbeats. It remains at epoch 10 but transitions to Unrevoked Tasks
        // state until it acknowledges the revocation of its tasks. The response contains the new
        // assignment without the tasks that must be revoked.
        result = context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(3)),
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology2)
                        .setPartitions(List.of(2))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertRecordsEquals(List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberId2)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 3),
                        TaskAssignmentTestUtil.mkTasks(subtopology2, 2)))
                    .setTasksPendingRevocation(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 4, 5)))
                    .build())),
            result.records()
        );

        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS,
            context.streamsGroupMemberState(groupId, memberId2));
        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, context.streamsGroupState(groupId));

        // Member 3 heartbeats. The response does not contain any assignment
        // because the member is still waiting on other members to revoke tasks.
        result = context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertRecordsEquals(List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberId3)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
                    .setMemberEpoch(11)
                    .setPreviousMemberEpoch(11)
                    .build())),
            result.records()
        );

        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS,
            context.streamsGroupMemberState(groupId, memberId3));
        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, context.streamsGroupState(groupId));

        // Member 1 acknowledges the revocation of the tasks. It does so by providing the
        // tasks that it still owns in the request. This allows him to transition to epoch 11
        // and to the Stable state.
        result = context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10)
            .setActiveTasks(List.of(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopology1)
                    .setPartitions(List.of(0, 1)),
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopology2)
                    .setPartitions(List.of(0))
            ))
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertRecordsEquals(List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberId1)
                    .setMemberEpoch(11)
                    .setPreviousMemberEpoch(10)
                    // Assignment epoch not bumped
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1),
                        TaskAssignmentTestUtil.mkTasks(subtopology2, 0)))
                    .build())),
            result.records()
        );

        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.STABLE, context.streamsGroupMemberState(groupId, memberId1));
        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, context.streamsGroupState(groupId));

        // Member 2 heartbeats but without acknowledging the revocation yet. This is basically a no-op.
        result = context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertEquals(List.of(), result.records());
        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS,
            context.streamsGroupMemberState(groupId, memberId2));
        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, context.streamsGroupState(groupId));

        // Member 3 heartbeats. It receives the tasks revoked by member 1 but remains
        // in Unreleased tasks state because it still waits on other tasks.
        result = context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology2)
                        .setPartitions(List.of(1))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertRecordsEquals(List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberId3)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
                    .setMemberEpoch(11)
                    .setPreviousMemberEpoch(11)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 11,
                        TaskAssignmentTestUtil.mkTasks(subtopology2, 1)))
                    .build())),
            result.records()
        );

        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS,
            context.streamsGroupMemberState(groupId, memberId3));
        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, context.streamsGroupState(groupId));

        // Member 3 heartbeats. Member 2 has not acknowledged the revocation of its tasks so
        // member keeps its current assignment.
        result = context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertEquals(List.of(), result.records());
        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS,
            context.streamsGroupMemberState(groupId, memberId3));
        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, context.streamsGroupState(groupId));

        // Member 2 acknowledges the revocation of the tasks. It does so by providing the
        // tasks that it still owns in the request. This allows him to transition to epoch 11
        // and to the Stable state.
        result = context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10)
            .setActiveTasks(List.of(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopology1)
                    .setPartitions(List.of(3)),
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopology2)
                    .setPartitions(List.of(2))
            ))
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of())
        );

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(2, 3)),
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology2)
                        .setPartitions(List.of(2))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertRecordsEquals(List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberId2)
                    .setMemberEpoch(11)
                    .setPreviousMemberEpoch(10)
                    // Assignment epoch of previous tasks is preserved, new tasks gets new assignment epoch
                    .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                        mkTasksWithEpochs(subtopology1, Map.of(2, 11, 3, 10)),
                        mkTasksWithEpochs(subtopology2, Map.of(2, 10))
                    ))
                    .build())),
            result.records()
        );

        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.STABLE, context.streamsGroupMemberState(groupId, memberId2));
        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, context.streamsGroupState(groupId));

        // Member 3 heartbeats to acknowledge its current assignment. It receives all its tasks and
        // transitions to Stable state.
        result = context.streamsGroupHeartbeat(new StreamsGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11)
            .setActiveTasks(List.of(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopology2)
                    .setPartitions(List.of(1))))
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(4, 5)),
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology2)
                        .setPartitions(List.of(1))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertRecordsEquals(List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberId3)
                    .setMemberEpoch(11)
                    .setPreviousMemberEpoch(11)
                    // All tasks were assigned in epoch 11
                    .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                        mkTasksWithEpochs(subtopology1, Map.of(4, 11, 5, 11)),
                        mkTasksWithEpochs(subtopology2, Map.of(1, 11))))
                    .build())),
            result.records()
        );

        assertEquals(org.apache.kafka.coordinator.group.streams.MemberState.STABLE, context.streamsGroupMemberState(groupId, memberId3));
        assertEquals(StreamsGroup.StreamsGroupState.STABLE, context.streamsGroupState(groupId));
    }

    @Test
    public void testStreamsStreamsGroupStates() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String subtopology2 = "subtopology2";
        String barTopicName = "bar";
        Uuid barTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName)),
            new Subtopology().setSubtopologyId(subtopology2).setSourceTopics(List.of(barTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage();

        long groupMetadataHash = computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, metadataImage),
            barTopicName, computeTopicHash(barTopicName, metadataImage)
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10))
            .build();

        assertEquals(StreamsGroup.StreamsGroupState.EMPTY, context.streamsGroupState(groupId));

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(groupId, topology));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberId1)
            .build()));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, 11, groupMetadataHash, -1, Map.of(), -1, -1));

        assertEquals(StreamsGroupState.NOT_READY, context.streamsGroupState(groupId));

        context.groupMetadataManager.getStreamsGroupOrThrow(groupId)
            .setValidatedTopologyEpoch(0);

        assertEquals(StreamsGroup.StreamsGroupState.ASSIGNING, context.streamsGroupState(groupId));

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId1,
            TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 1, 2, 3))));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 11, 12345L));

        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, context.streamsGroupState(groupId));

        context.replay(
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberId1)
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNREVOKED_TASKS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedTasks(
                    mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 11,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 1, 2, 3)))
                .build()));

        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, context.streamsGroupState(groupId));

        context.replay(
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberId1)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedTasks(
                    mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 11,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 1, 2, 3)))
                .build()));

        assertEquals(StreamsGroup.StreamsGroupState.STABLE, context.streamsGroupState(groupId));
    }

    @Test
    public void testStreamsTaskAssignorExceptionOnRegularHeartbeat() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        String subtopology2 = "subtopology2";
        String barTopicName = "bar";
        Uuid barTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName)),
            new Subtopology().setSubtopologyId(subtopology2).setSourceTopics(List.of(barTopicName))
        ));

        TaskAssignor assignor = mock(TaskAssignor.class);
        when(assignor.name()).thenReturn("sticky");
        when(assignor.assign(any(), any())).thenThrow(new TaskAssignorException("Assignment failed."));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        // Member 1 joins the streams group. The request fails because the
        // target assignment computation failed.
        UnknownServerException e = assertThrows(UnknownServerException.class, () ->
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(1500)
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())));
        assertEquals("Failed to compute a new target assignment for epoch 2: Assignment failed.", e.getMessage());
    }

    @Test
    public void testStreamsPartitionMetadataRefreshedAfterGroupIsLoaded() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)))
                    .build())
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withTargetAssignment(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)))
                .withTargetAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    // foo only has 3 tasks stored in the metadata but foo has
                    // 6 partitions the metadata image.
                    fooTopicName, computeTopicHash(
                        fooTopicName,
                        new MetadataImageBuilder()
                            .addTopic(fooTopicId, fooTopicName, 3)
                            .buildCoordinatorMetadataImage())
                ))))
            .build();

        // The metadata refresh flag should be true.
        StreamsGroup streamsGroup = context.groupMetadataManager
            .streamsGroup(groupId);
        assertTrue(streamsGroup.hasMetadataExpired(context.time.milliseconds()));

        // Prepare the assignment result.
        assignor.prepareGroupAssignment(Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
            TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)
        )));

        // Heartbeat.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        // The member gets tasks 3, 4 and 5 assigned.
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedTasks(mkTasksTupleWithEpochs(TaskRole.ACTIVE,
                mkTasksWithEpochs(subtopology1,
                    Map.of(
                        0, 10, // 0, 1, 2 were already assigned in epoch 10
                        1, 10,
                        2, 10,
                        3, 11, // 3, 4, 5 were added in epoch 11
                        4, 11,
                        5, 11
                    ))
            ))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId,
                11,
                computeGroupHash(Map.of(fooTopicName, computeTopicHash(fooTopicName, metadataImage))),
                0,
                new TreeMap<>(Map.of(
                    "num.standby.replicas", "0"
                )),
                -1,
                -1
            ),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId,
                TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)
                )),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Check next refresh time.
        assertFalse(streamsGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + Integer.MAX_VALUE, streamsGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, streamsGroup.metadataRefreshDeadline().epoch);
    }

    @Test
    public void testStreamsPartitionMetadataRefreshedAgainAfterWriteFailure() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 11,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)))
                    .build())
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withTargetAssignment(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)))
                .withTargetAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    // foo only has 3 partitions stored in the metadata but foo has
                    // 6 partitions the metadata image.
                    fooTopicName, computeTopicHash(
                        fooTopicName,
                        new MetadataImageBuilder()
                            .addTopic(fooTopicId, fooTopicName, 3)
                            .buildCoordinatorMetadataImage())
                ))))
            .build();

        // The metadata refresh flag should be true.
        StreamsGroup streamsGroup = context.groupMetadataManager
            .streamsGroup(groupId);
        assertTrue(streamsGroup.hasMetadataExpired(context.time.milliseconds()));

        // Prepare the assignment result.
        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)
            ))
        );

        // Heartbeat.
        context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        // The metadata refresh flag is set to a future time.
        assertFalse(streamsGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + Integer.MAX_VALUE, streamsGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, streamsGroup.metadataRefreshDeadline().epoch);

        // Rollback the uncommitted changes. This does not rollback the metadata flag
        // because it is not using a timeline data structure.
        context.rollback();

        // However, the next heartbeat should detect the divergence based on the epoch and trigger
        // a metadata refresh.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        // The member gets tasks 3, 4 and 5 assigned.
        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        StreamsGroupMember expectedMember = streamsGroupMemberBuilderWithDefaults(memberId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 11,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                groupId,
                11,
                computeGroupHash(Map.of(fooTopicName, computeTopicHash(fooTopicName, metadataImage))),
                0,
                new TreeMap<>(Map.of(
                    "num.standby.replicas", "0"
                )),
                -1,
                -1
            ),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId,
                TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)
                )),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Check next refresh time.
        assertFalse(streamsGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + Integer.MAX_VALUE, streamsGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, streamsGroup.metadataRefreshDeadline().epoch);
    }

    @Test
    public void testStreamsSessionTimeoutLifecycle() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .buildCoordinatorMetadataImage())
                .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        assignor.prepareGroupAssignment(Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
            TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)
        )));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result =
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of()));
        assertEquals(2, result.response().data().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().data().heartbeatIntervalMs())
        );

        // Session timer is rescheduled on second heartbeat.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().data().memberEpoch()));
        assertEquals(2, result.response().data().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().data().heartbeatIntervalMs())
        );

        // Session timer is cancelled on leave.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH));
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, result.response().data().memberEpoch());

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testStreamsSessionTimeoutExpiration() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));
        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, metadataImage)
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        assignor.prepareGroupAssignment(Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
            TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)
        )));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result =
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of()));
        assertEquals(2, result.response().data().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time past the session timeout.
        List<ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(45000 + 1);

        // Verify the expired timeout.
        assertEquals(
            List.of(new ExpiredTimeout<>(
                groupSessionTimeoutKey(groupId, memberId),
                new CoordinatorResult<>(
                    List.of(
                        StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                            groupId,
                            3,
                            groupMetadataHash,
                            0,
                            new TreeMap<>(Map.of(
                                "num.standby.replicas", "0"
                            )),
                            -1,
                            -1
                        ),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(
                            groupId, 3, 0L)
                    )
                )
            )),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testStreamsRebalanceTimeoutLifecycle() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 3)
                .buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        assignor.prepareGroupAssignment(Map.of(memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
            TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)
        )));

        // Member 1 joins the group.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result =
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(12000)
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertEquals(
            List.of(),
            context.sleep(result.response().data().heartbeatIntervalMs())
        );

        // Prepare next assignment.
        assignor.prepareGroupAssignment(Map.of(
            memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1)
            ),
            memberId2, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 2)
            )
        ));

        // Member 2 joins the group.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(90000)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(3)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertEquals(
            List.of(),
            context.sleep(result.response().data().heartbeatIntervalMs())
        );

        // Member 1 heartbeats and transitions to unrevoked tasks. The rebalance timeout
        // is scheduled.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setRebalanceTimeoutMs(12000));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        // Verify that there is a revocation timeout. Keep a reference
        // to the timeout for later.
        ScheduledTimeout<CoordinatorRecord> scheduledTimeout =
            context.assertRebalanceTimeout(groupId, memberId1, 12000);

        assertEquals(
            List.of(),
            context.sleep(result.response().data().heartbeatIntervalMs())
        );

        // Member 1 acks the revocation. The revocation timeout is cancelled.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setActiveTasks(List.of(new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopology1)
                    .setPartitions(List.of(0, 1))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(3)
                .setHeartbeatIntervalMs(5000)
                .setEndpointInformationEpoch(0)
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        // Verify that there is not revocation timeout.
        context.assertNoRebalanceTimeout(groupId, memberId1);

        // Execute the scheduled revocation timeout captured earlier to simulate a
        // stale timeout. This should be a no-op.
        assertEquals(List.of(), scheduledTimeout.operation().generateRecords().records());
    }

    @Test
    public void testStreamsRebalanceTimeoutExpiration() {
        final int rebalanceTimeoutMs = 10000;
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));
        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, metadataImage)
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        assignor.prepareGroupAssignment(
            Map.of(memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2))));

        // Member 1 joins the group.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result =
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(rebalanceTimeoutMs) // Use timeout smaller than session timeout.
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertEquals(
            List.of(),
            context.sleep(result.response().data().heartbeatIntervalMs())
        );

        // Prepare next assignment.
        assignor.prepareGroupAssignment(Map.of(
            memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1)
            ),
            memberId2, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 2)
            )
        ));

        // Member 2 joins the group.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(rebalanceTimeoutMs)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(3)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        assertEquals(
            List.of(),
            context.sleep(result.response().data().heartbeatIntervalMs())
        );

        // Member 1 heartbeats and transitions to revoking. The revocation timeout
        // is scheduled.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(2));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        // Advance time past the revocation timeout.
        List<ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(rebalanceTimeoutMs + 1);

        // Verify the expired timeout.
        assertEquals(
            List.of(new ExpiredTimeout<>(
                groupRebalanceTimeoutKey(groupId, memberId1),
                new CoordinatorResult<>(
                    List.of(
                        StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId, memberId1),
                        StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                            groupId,
                            4,
                            groupMetadataHash,
                            0,
                            new TreeMap<>(Map.of(
                                "num.standby.replicas", "0"
                            )),
                            -1,
                            -1
                        )
                    )
                )
            )),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId1);
        context.assertNoRebalanceTimeout(groupId, memberId1);
    }

    @Test
    public void testStreamsOnMetadataUpdate() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();

        // Topology of group 1 uses a and b.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord("group1",
            new Topology().setSubtopologies(List.of(
                new Subtopology().setSubtopologyId("subtopology1")
                    .setSourceTopics(List.of("a"))
                    .setRepartitionSourceTopics(List.of(new TopicInfo().setName("b"))
            ))
        )));

        // Topology of group 2 uses b and c.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord("group2",
            new Topology().setSubtopologies(List.of(
                new Subtopology().setSubtopologyId("subtopology2")
                    .setSourceTopics(List.of("b"))
                    .setStateChangelogTopics(List.of(new TopicInfo().setName("c")))
            ))
        ));

        // Topology of group 3 uses d.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord("group3",
            new Topology().setSubtopologies(List.of(
                new Subtopology().setSubtopologyId("subtopology3")
                    .setSourceTopics(List.of("d"))
            ))
        ));

        // Topology of group 4 subscribes to e.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord("group4",
            new Topology().setSubtopologies(List.of(
                new Subtopology().setSubtopologyId("subtopology4")
                    .setSourceTopics(List.of("e"))
            ))
        ));

        // Topology of group 5 subscribes to f.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord("group5",
            new Topology().setSubtopologies(List.of(
                new Subtopology().setSubtopologyId("subtopology5")
                    .setSourceTopics(List.of("f"))
            ))
        ));

        // Ensures that all refresh flags are set to the future.
        List.of("group1", "group2", "group3", "group4", "group5").forEach(groupId -> {
            StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
            group.setMetadataRefreshDeadline(context.time.milliseconds() + 5000L, 0);
            assertFalse(group.hasMetadataExpired(context.time.milliseconds()));
        });

        // Update the metadata image.
        Uuid topicA = Uuid.randomUuid();
        Uuid topicB = Uuid.randomUuid();
        Uuid topicC = Uuid.randomUuid();
        Uuid topicD = Uuid.randomUuid();
        Uuid topicE = Uuid.randomUuid();

        // Create a first base image with topic a, b, c and d.
        MetadataDelta delta = new MetadataDelta.Builder()
            .setImage(MetadataImage.EMPTY)
            .build();
        delta.replay(new TopicRecord().setTopicId(topicA).setName("a"));
        delta.replay(new PartitionRecord().setTopicId(topicA).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicB).setName("b"));
        delta.replay(new PartitionRecord().setTopicId(topicB).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicC).setName("c"));
        delta.replay(new PartitionRecord().setTopicId(topicC).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicD).setName("d"));
        delta.replay(new PartitionRecord().setTopicId(topicD).setPartitionId(0));
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);

        // Create a delta which updates topic B, deletes topic D and creates topic E.
        delta = new MetadataDelta.Builder()
            .setImage(image)
            .build();
        delta.replay(new PartitionRecord().setTopicId(topicB).setPartitionId(2));
        delta.replay(new RemoveTopicRecord().setTopicId(topicD));
        delta.replay(new TopicRecord().setTopicId(topicE).setName("e"));
        delta.replay(new PartitionRecord().setTopicId(topicE).setPartitionId(1));
        image = delta.apply(MetadataProvenance.EMPTY);

        // Update metadata image with the delta.
        context.groupMetadataManager.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(delta), new KRaftCoordinatorMetadataImage(image));

        // Verify the groups.
        List.of("group1", "group2", "group3", "group4").forEach(groupId -> {
            StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
            assertTrue(group.hasMetadataExpired(context.time.milliseconds()), groupId);
        });

        List.of("group5").forEach(groupId -> {
            StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
            assertFalse(group.hasMetadataExpired(context.time.milliseconds()));
        });

        // Verify image.
        assertEquals(new KRaftCoordinatorMetadataImage(image), context.groupMetadataManager.image());
    }

    @Test
    public void testStreamsGroupEndpointInformationOnlyWhenEpochGreater() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
                new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(assignor))
                .withMetadataImage(new MetadataImageBuilder()
                        .addTopic(fooTopicId, fooTopicName, 2)
                        .buildCoordinatorMetadataImage())
                .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
                .build();

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(
                Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1))));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result;

        // A full response should be sent back on joining.
        result = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setMemberId(memberId)
                        .setMemberEpoch(0)
                        .setRebalanceTimeoutMs(1500)
                        .setTopology(topology)
                        .setActiveTasks(List.of())
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
                        .setUserEndpoint(new StreamsGroupHeartbeatRequestData.Endpoint().setHost("localhost").setPort(9092))
                        .setEndpointInformationEpoch(0));

        StreamsGroupHeartbeatResponseData.EndpointToPartitions expectedEndpointToPartitions = new StreamsGroupHeartbeatResponseData.EndpointToPartitions()
            .setUserEndpoint(new StreamsGroupHeartbeatResponseData.Endpoint().setHost("localhost").setPort(9092))
            .setActivePartitions(List.of(new StreamsGroupHeartbeatResponseData.TopicPartition().setTopic("foo").setPartitions(List.of(0, 1))))
            .setStandbyPartitions(List.of());

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                        new StreamsGroupHeartbeatResponseData.TaskIds()
                                .setSubtopologyId(subtopology1)
                                .setPartitions(List.of(0, 1))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setPartitionsByUserEndpoint(List.of(expectedEndpointToPartitions))
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        result = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setMemberId(memberId)
                        .setUserEndpoint(new StreamsGroupHeartbeatRequestData.Endpoint().setHost("localhost").setPort(9092))
                        .setMemberEpoch(result.response().data().memberEpoch())
                        .setEndpointInformationEpoch(result.response().data().endpointInformationEpoch()));

        assertNull(result.response().data().partitionsByUserEndpoint());
    }

    @Test
    public void testStreamsGroupEndpointInformationIncludesNewMember() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
                new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(assignor))
                .withMetadataImage(new MetadataImageBuilder()
                        .addTopic(fooTopicId, fooTopicName, 4)
                        .buildCoordinatorMetadataImage())
                .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
                .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, 0)
                .build();

        // Prepare assignment for first member
        assignor.prepareGroupAssignment(
                Map.of(memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1))));

        // First member joins
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setMemberId(memberId1)
                        .setMemberEpoch(0)
                        .setRebalanceTimeoutMs(1500)
                        .setTopology(topology)
                        .setActiveTasks(List.of())
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
                        .setUserEndpoint(new StreamsGroupHeartbeatRequestData.Endpoint().setHost("host1").setPort(9092))
                        .setEndpointInformationEpoch(0));

        assertEquals(2, result.response().data().memberEpoch());

        // Prepare assignment for both members
        assignor.prepareGroupAssignment(
                Map.of(
                        memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1)),
                        memberId2, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE, TaskAssignmentTestUtil.mkTasks(subtopology1, 2, 3))
                ));

        // Second member joins
        result = context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                        .setGroupId(groupId)
                        .setMemberId(memberId2)
                        .setMemberEpoch(0)
                        .setRebalanceTimeoutMs(1500)
                        .setTopology(topology)
                        .setActiveTasks(List.of())
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of())
                        .setUserEndpoint(new StreamsGroupHeartbeatRequestData.Endpoint().setHost("host2").setPort(9093)));

        // The response should include endpoint information because the member's epoch (0) differs from the group's (1)
        assertNotNull(result.response().data().partitionsByUserEndpoint());
        List<StreamsGroupHeartbeatResponseData.EndpointToPartitions> endpointsList = result.response().data().partitionsByUserEndpoint();
        assertEquals(2, endpointsList.size(), "Should include both members in endpoint information");

        // Sort by port for consistent ordering
        endpointsList.sort(Comparator.comparingInt(e -> e.userEndpoint().port()));

        // Verify first member's endpoint
        StreamsGroupHeartbeatResponseData.EndpointToPartitions member1Endpoint = endpointsList.get(0);
        assertEquals("host1", member1Endpoint.userEndpoint().host());
        assertEquals(9092, member1Endpoint.userEndpoint().port());
        assertEquals(1, member1Endpoint.activePartitions().size());
        StreamsGroupHeartbeatResponseData.TopicPartition member1Topic = member1Endpoint.activePartitions().get(0);
        assertEquals("foo", member1Topic.topic());
        List<Integer> member1Partitions = new ArrayList<>(member1Topic.partitions());
        Collections.sort(member1Partitions);
        assertEquals(List.of(0, 1), member1Partitions);

        // Verify second member's endpoint (the new member)
        StreamsGroupHeartbeatResponseData.EndpointToPartitions member2Endpoint = endpointsList.get(1);
        assertEquals("host2", member2Endpoint.userEndpoint().host());
        assertEquals(9093, member2Endpoint.userEndpoint().port());
        assertEquals(1, member2Endpoint.activePartitions().size());
        StreamsGroupHeartbeatResponseData.TopicPartition member2Topic = member2Endpoint.activePartitions().get(0);
        assertEquals("foo", member2Topic.topic());
        List<Integer> member2Partitions = new ArrayList<>(member2Topic.partitions());
        Collections.sort(member2Partitions);
        assertEquals(List.of(2, 3), member2Partitions);
    }

    @Test
    public void testStreamsGroupEpochIncreaseWithNumStandbyReplicasConfigChanges() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();

        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, 0)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(TaskAssignmentTestUtil.mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withTargetAssignment(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
                .withTargetAssignmentEpoch(10)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withValidatedTopologyEpoch(0)
            )
            .build();

        // Change the group-level num.standby.replicas config
        Properties newConfig = new Properties();
        newConfig.put(GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG, "2");
        context.groupConfigManager.updateGroupConfig(groupId, newConfig);

        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5),
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2)))
        );

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10)
                .setActiveTasks(List.of(new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopology1)
                    .setPartitions(List.of(0, 1, 2))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology1)
                        .setPartitions(List.of(0, 1, 2))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000),
            result.response().data()
        );

        // Find the StreamsGroupMetadata record
        CoordinatorRecord metadataRecord = result.records().stream()
            .filter(record -> record.key() instanceof StreamsGroupMetadataKey)
            .findFirst()
            .orElse(null);

        assertNotNull(metadataRecord, "Expected a StreamsGroupMetadata record");
        // Verify the metadata record contains the updated assignment config
        StreamsGroupMetadataValue metadataValue = (StreamsGroupMetadataValue) metadataRecord.value().message();
        assertEquals(11, metadataValue.epoch());

        // Verify the assignment config contains the updated value
        List<StreamsGroupMetadataValue.LastAssignmentConfig> assignmentConfigs = metadataValue.lastAssignmentConfigs();
        assertFalse(assignmentConfigs.isEmpty(), "Expected assignment configs to be present");

        StreamsGroupMetadataValue.LastAssignmentConfig standbyReplicasConfig = assignmentConfigs.stream()
            .filter(config -> "num.standby.replicas".equals(config.key()))
            .findFirst()
            .orElse(null);

        assertNotNull(standbyReplicasConfig, "Expected num.standby.replicas config to be present");
        assertEquals("2", standbyReplicasConfig.value());

        // Verify that group epoch was bumped
        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
        int newGroupEpoch = group.groupEpoch();
        assertEquals(11, newGroupEpoch);
        assertEquals("2", group.lastAssignmentConfigs().get("num.standby.replicas"));
    }

    @Test
    public void testStreamsGroupEpochShouldNotIncreaseWithAcceptableRecoveryLagConfigChange() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();

        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 10)
                .withMember(streamsGroupMemberBuilderWithDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setAssignedTasks(TaskAssignmentTestUtil.mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 10,
                        TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withTargetAssignment(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                    TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
                .withTargetAssignmentEpoch(10)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topology))
                .withValidatedTopologyEpoch(0)
            )
            .build();

        // Change the group-level acceptable.recovery.lag config
        Properties newConfig = new Properties();
        newConfig.put(GroupConfig.STREAMS_ACCEPTABLE_RECOVERY_LAG_CONFIG, "50000");
        context.groupConfigManager.updateGroupConfig(groupId, newConfig);

        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2, 3, 4, 5)))
        );

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10)
                .setActiveTasks(List.of(new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(subtopology1)
                    .setPartitions(List.of(0, 1, 2, 3, 4, 5))))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(50_000L),
            result.response().data()
        );

        // Find the StreamsGroupMetadata record
        CoordinatorRecord metadataRecord = result.records().stream()
            .filter(record -> record.key() instanceof StreamsGroupMetadataKey)
            .findFirst()
            .orElse(null);

        assertNotNull(metadataRecord, "Expected a StreamsGroupMetadata record");
        // Verify the metadata record contains the updated assignment config
        StreamsGroupMetadataValue metadataValue = (StreamsGroupMetadataValue) metadataRecord.value().message();
        assertEquals(11, metadataValue.epoch());

        // Verify the assignment config does not store acceptable.recovery.lag
        List<StreamsGroupMetadataValue.LastAssignmentConfig> assignmentConfigs = metadataValue.lastAssignmentConfigs();
        assertFalse(assignmentConfigs.isEmpty(), "Expected assignment configs to be present");

        StreamsGroupMetadataValue.LastAssignmentConfig recoveryLagConfig = assignmentConfigs.stream()
            .filter(c -> "acceptable.recovery.lag".equals(c.key()))
            .findFirst()
            .orElse(null);

        assertNull(recoveryLagConfig, "Expected acceptable.recovery.lag to be null");

        // Verify that group epoch stays the same
        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
        int newGroupEpoch = group.groupEpoch();
        assertEquals(11, newGroupEpoch);
    }

    @Test
    public void testStreamsGroupHeartbeatWithNonEmptyClassicGroup() {
        String classicGroupId = "classic-group-id";
        String memberId = Uuid.randomUuid().toString();

        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();
        ClassicGroup classicGroup = new ClassicGroup(
            new LogContext(),
            classicGroupId,
            EMPTY,
            context.time
        );
        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(classicGroup, classicGroup.groupAssignment()));

        context.groupMetadataManager.getOrMaybeCreateClassicGroup(classicGroupId, false).transitionTo(PREPARING_REBALANCE);
        assertThrows(GroupIdNotFoundException.class, () ->
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(classicGroupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(12000)
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())));
    }

    @Test
    public void testStreamsGroupHeartbeatWithEmptyClassicGroup() {
        String classicGroupId = "classic-group-id";
        String memberId = Uuid.randomUuid().toString();
        String fooTopicName = "foo";
        String subtopology1 = "subtopology1";
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();
        ClassicGroup classicGroup = new ClassicGroup(
            new LogContext(),
            classicGroupId,
            EMPTY,
            context.time
        );
        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(classicGroup, classicGroup.groupAssignment()));

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(classicGroupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(12000)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        StreamsGroupMember expectedMember = StreamsGroupMember.Builder.withDefaults(memberId)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .setRebalanceTimeoutMs(12000)
            .setTopologyEpoch(0)
            .build();

        assertEquals(Errors.NONE.code(), result.response().data().errorCode());
        assertEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(classicGroupId),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(classicGroupId, expectedMember),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(classicGroupId, topology),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
                    classicGroupId,
                    2,
                    0,
                    -1,
                    new TreeMap<>(Map.of(
                        "num.standby.replicas", "0"
                    )),
                    -1,
                    -1
                ),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(classicGroupId, memberId, TasksTuple.EMPTY),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(classicGroupId, 2, context.time.milliseconds()),
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(classicGroupId, expectedMember)
            ),
            result.records()
        );
        assertEquals(
            Group.GroupType.STREAMS,
            context.groupMetadataManager.streamsGroup(classicGroupId).type()
        );
    }

    @Test
    public void testClassicGroupJoinWithEmptyStreamsGroup() throws Exception {
        String streamsGroupId = "streams-group-id";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroup(new StreamsGroupBuilder(streamsGroupId, 10))
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(streamsGroupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request, true);

        List<CoordinatorRecord> expectedRecords = List.of(
            StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataTombstoneRecord(streamsGroupId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupEpochTombstoneRecord(streamsGroupId),
            StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecordTombstone(streamsGroupId)
        );

        assertEquals(Errors.MEMBER_ID_REQUIRED.code(), joinResult.joinFuture.get().errorCode());
        assertEquals(expectedRecords, joinResult.records.subList(0, expectedRecords.size()));
        assertEquals(
            Group.GroupType.CLASSIC,
            context.groupMetadataManager.getOrMaybeCreateClassicGroup(streamsGroupId, false).type()
        );
    }

    @Test
    public void testClassicGroupJoinWithNonEmptyStreamsGroup() throws Exception {
        String streamsGroupId = "streams-group-id";
        String memberId = Uuid.randomUuid().toString();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroup(new StreamsGroupBuilder(streamsGroupId, 10)
                .withMember(StreamsGroupMember.Builder.withDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .build()))
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(streamsGroupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testStreamsGroupDynamicConfigs() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .buildCoordinatorMetadataImage())
                .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2))));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result =
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(10000)
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of()));
        assertEquals(2, result.response().data().memberEpoch());
        assertEquals(
            Map.of(
                "num.standby.replicas", "0"
            ),
            assignor.lastPassedAssignmentConfigs()
        );

        // Verify heartbeat interval
        assertEquals(GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT, result.response().data().heartbeatIntervalMs());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_DEFAULT);

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().data().heartbeatIntervalMs())
        );

        // Dynamic update group config
        Properties newGroupConfig = new Properties();
        newGroupConfig.put(STREAMS_SESSION_TIMEOUT_MS_CONFIG, 50000);
        newGroupConfig.put(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        newGroupConfig.put(STREAMS_NUM_STANDBY_REPLICAS_CONFIG, 2);
        context.updateGroupConfig(groupId, newGroupConfig);

        // Session timer is rescheduled on second heartbeat, new assignment with new parameter is calculated.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().data().memberEpoch())
                .setRackId("bla"));

        // Verify heartbeat interval
        assertEquals(10000, result.response().data().heartbeatIntervalMs());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 50000);

        // Verify that the new number of standby replicas is used
        assertEquals(
            Map.of(
                "num.standby.replicas", "2"
            ),
            assignor.lastPassedAssignmentConfigs()
        );

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().data().heartbeatIntervalMs())
        );

        // Session timer is cancelled on leave.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH));
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, result.response().data().memberEpoch());

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testStreamsGroupEvaluatedConfigs() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        String subtopology1 = "subtopology1";
        String fooTopicName = "foo";
        Uuid fooTopicId = Uuid.randomUuid();
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology1).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .buildCoordinatorMetadataImage())
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .build();

        assignor.prepareGroupAssignment(
            Map.of(memberId, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology1, 0, 1, 2))));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result =
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(10000)
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of()));
        assertEquals(2, result.response().data().memberEpoch());

        // Verify default heartbeat interval, session timeout, num.standby.replicas, task.offset.interval before config update.
        assertEquals(GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT,
            result.response().data().heartbeatIntervalMs());
        context.assertSessionTimeout(groupId, memberId,
            GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_DEFAULT);
        assertEquals(
            Map.of(
                "num.standby.replicas", String.valueOf(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT)
            ),
            assignor.lastPassedAssignmentConfigs());
        assertEquals(GroupCoordinatorConfig.STREAMS_GROUP_TASK_OFFSET_INTERVAL_MS_DEFAULT,
            result.response().data().taskOffsetIntervalMs());
        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().data().heartbeatIntervalMs())
        );

        // Dynamic update group config with out-of-range values.
        // Session timeout 70000 exceeds max 60000; heartbeat interval 1 is below min 5000;
        // num standby replicas 100 exceeds max 2.
        // task offset interval 100 is below min 15000.
        Properties newGroupConfig = new Properties();
        newGroupConfig.put(STREAMS_SESSION_TIMEOUT_MS_CONFIG, 70000);
        newGroupConfig.put(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG, 1);
        newGroupConfig.put(STREAMS_NUM_STANDBY_REPLICAS_CONFIG, 100);
        newGroupConfig.put(STREAMS_TASK_OFFSET_INTERVAL_MS_CONFIG, 100);
        context.updateGroupConfig(groupId, newGroupConfig);

        // Session timer is rescheduled on second heartbeat, new assignment with evaluated parameter is calculated.
        result = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().data().memberEpoch())
                .setRackId("bla"));

        // Verify heartbeat interval is evaluated to min.
        assertEquals(GroupCoordinatorConfig.STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT,
            result.response().data().heartbeatIntervalMs());

        // Verify session timeout is evaluated to max.
        context.assertSessionTimeout(groupId, memberId,
            GroupCoordinatorConfig.STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT);

        // Verify that the number of standby replicas is evaluated to max,
        // and task offset interval is evaluated to min
        assertEquals(
            Map.of(
                "num.standby.replicas", String.valueOf(GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_DEFAULT)
            ),
            assignor.lastPassedAssignmentConfigs());
        assertEquals(GroupCoordinatorConfig.STREAMS_GROUP_MIN_TASK_OFFSET_INTERVAL_MS_DEFAULT,
            result.response().data().taskOffsetIntervalMs());
    }

    @Test
    public void testReplayStreamsGroupMemberMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setClientId("clientid")
            .setClientHost("clienthost")
            .setRackId("rackid")
            .setInstanceId("instanceid")
            .setRebalanceTimeoutMs(1000)
            .setTopologyEpoch(10)
            .setProcessId("processid")
            .setUserEndpoint(new Endpoint().setHost("localhost").setPort(9999))
            .setClientTags(Map.of("key", "value"))
            .build();

        // The group and the member are created if they do not exist.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord("foo", member));
        assertEquals(member, context.groupMetadataManager.streamsGroup("foo").getMemberOrThrow("member"));
    }

    @Test
    public void testReplayStreamsGroupMemberMetadataWithSimpleClassicGroup() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // A simple classic group is created when replaying offset commits without a group.
        // This simulates the scenario where offset commit records are replayed before streams
        // group records after log compaction has cleaned up the group metadata tombstone.
        context.groupMetadataManager.getOrMaybeCreateClassicGroup("foo", true);

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setClientId("clientid")
            .setClientHost("clienthost")
            .setRackId("rackid")
            .setInstanceId("instanceid")
            .setRebalanceTimeoutMs(1000)
            .setTopologyEpoch(10)
            .setProcessId("processid")
            .setUserEndpoint(new Endpoint().setHost("localhost").setPort(9999))
            .setClientTags(Map.of("key", "value"))
            .build();

        // The simple classic group should be replaced by a streams group.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord("foo", member));
        assertEquals(member, context.groupMetadataManager.streamsGroup("foo").getMemberOrThrow("member"));
    }

    @Test
    public void testReplayStreamsGroupMemberMetadataTombstoneNotExisting() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group still exists but the member is already gone. Replaying the
        // StreamsGroupMemberMetadata tombstone should be a no-op.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord("foo", 10, 0, 0, Map.of("num.standby.replicas", "0"), -1, -1));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord("foo", "m1"));
        assertThrows(UnknownMemberIdException.class, () -> context.groupMetadataManager.streamsGroup("foo").getMemberOrThrow("m1"));

        // The group may not exist at all. Replaying the StreamsGroupMemberMetadata tombstone
        // should be a no-op.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord("bar", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("bar"));
    }

    @Test
    public void testReplayStreamsGroupMemberMetadataTombstoneExisting() {
        final TasksTuple tasks =
            new TasksTuple(
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 0, 1, 2)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 3, 4, 5)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 6, 7, 8))
            );
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroup(
                new StreamsGroupBuilder("foo", 10)
                    .withMember(streamsGroupMemberBuilderWithDefaults("m1").build())
                    .withTargetAssignment("m1", tasks)
            )
            .build();

        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord("foo", "m1")));
        assertEquals("Received a tombstone record to delete member m1 but did not receive "
                + "StreamsGroupCurrentMemberAssignmentValue tombstone.",
            e.getMessage());

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord("foo", "m1"));

        IllegalStateException e2 = assertThrows(IllegalStateException.class,
            () -> context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord("foo", "m1")));
        assertEquals("Received a tombstone record to delete member m1 but did not receive "
                + "StreamsGroupTargetAssignmentMetadataValue tombstone.",
            e2.getMessage());

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord("foo", "m1"));

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord("foo", "m1"));

        assertFalse(context.groupMetadataManager.streamsGroup("foo").hasMember("m1"));
    }

    @Test
    public void testReplayStreamsGroupMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord("foo", 10, 0, 0, Map.of("num.standby.replicas", "0"), -1, -1));
        assertEquals(10, context.groupMetadataManager.streamsGroup("foo").groupEpoch());
    }

    @Test
    public void testReplayStreamsGroupEpochTombstoneNotExisting() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the StreamsGroupMetadata tombstone
        // should be a no-op.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupEpochTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("foo"));
    }

    @Test
    public void testReplayStreamsGroupEpochTombstoneExisting() {
        final TasksTuple tasks =
            new TasksTuple(
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 0, 1, 2)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 3, 4, 5)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 6, 7, 8))
            );
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroup(
                new StreamsGroupBuilder("foo", 10)
                    .withTargetAssignmentEpoch(10)
                    .withMember(streamsGroupMemberBuilderWithDefaults("m1").build())
                    .withTargetAssignment("m1", tasks)
            )
            .build();

        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupEpochTombstoneRecord("foo")));
        assertEquals("Received a tombstone record to delete group foo but the group still has 1 members.",
            e.getMessage());

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord("foo", "m1"));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord("foo", "m1"));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord("foo", "m1"));

        IllegalStateException e2 = assertThrows(IllegalStateException.class,
            () -> context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupEpochTombstoneRecord("foo")));
        assertEquals("Received a tombstone record to delete group foo but did not receive StreamsGroupTargetAssignmentMetadataValue tombstone.",
            e2.getMessage());

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataTombstoneRecord("foo"));

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupEpochTombstoneRecord("foo"));

        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("foo"));
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMember() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        final TasksTuple tasks =
            new TasksTuple(
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 0, 1, 2)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 3, 4, 5)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 6, 7, 8))
            );
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord("foo", "m1", tasks));
        assertEquals(tasks.activeTasks(), context.groupMetadataManager.streamsGroup("foo").targetAssignment("m1").activeTasks());
        assertEquals(tasks.standbyTasks(), context.groupMetadataManager.streamsGroup("foo").targetAssignment("m1").standbyTasks());
        assertEquals(tasks.warmupTasks(), context.groupMetadataManager.streamsGroup("foo").targetAssignment("m1").warmupTasks());
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMemberTombstoneNonExisting() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the StreamsGroupTargetAssignmentMember tombstone
        // should be a no-op.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord("foo", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("foo"));
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMemberTombstoneExisting() {
        final TasksTuple tasks =
            new TasksTuple(
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 0, 1, 2)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 3, 4, 5)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 6, 7, 8))
            );
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroup(new StreamsGroupBuilder("foo", 10).withTargetAssignment("m1", tasks))
            .build();

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord("foo", "m1"));

        assertTrue(context.groupMetadataManager.streamsGroup("foo").targetAssignment("m1").isEmpty());
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord("foo", 10, 12345L));
        assertEquals(10, context.groupMetadataManager.streamsGroup("foo").assignmentEpoch());
        assertEquals(12345L, context.groupMetadataManager.streamsGroup("foo").assignmentTimestamp());
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMetadataTombstoneNotExisting() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the StreamsGroupTargetAssignmentMetadata tombstone
        // should be a no-op.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("foo"));
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMetadataTombstoneExisting() {
        final TasksTuple tasks =
            new TasksTuple(
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 0, 1, 2)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 3, 4, 5)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 6, 7, 8))
            );

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroup(
                new StreamsGroupBuilder("foo", 10)
                    .withTargetAssignmentEpoch(10)
                    .withTargetAssignmentTimestamp(12345L)
                    .withTargetAssignment("m1", tasks)
            )
            .build();

        IllegalStateException e = assertThrows(
            IllegalStateException.class,
            () -> context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataTombstoneRecord("foo"))
        );
        assertEquals("Received a tombstone record to delete target assignment of foo but the assignment still has 1 members.",
            e.getMessage());

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord("foo", "m1"));

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataTombstoneRecord("foo"));

        assertEquals(-1, context.groupMetadataManager.streamsGroup("foo").assignmentEpoch());
        assertEquals(0L, context.groupMetadataManager.streamsGroup("foo").assignmentTimestamp());
    }

    @Test
    public void testReplayStreamsGroupCurrentMemberAssignment() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
            .setAssignedTasks(new TasksTupleWithEpochs(
                TaskAssignmentTestUtil.mkTasksPerSubtopologyWithCommonEpoch(10,
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 0, 1, 2)
                ),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(TaskAssignmentTestUtil.mkTasks("subtopology-1", 3, 4, 5)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(TaskAssignmentTestUtil.mkTasks("subtopology-1", 6, 7, 8))
            ))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        // The group and the member are created if they do not exist.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord("bar", member));
        assertEquals(member, context.groupMetadataManager.streamsGroup("bar").getMemberOrThrow("member"));
    }

    @Test
    public void testReplayStreamsGroupCurrentMemberAssignmentTombstoneNotExisting() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group still exists, but the member is already gone. Replaying the
        // StreamsGroupCurrentMemberAssignment tombstone should be a no-op.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord("foo", 10, 0, 0, Map.of("num.standby.replicas", "0"), -1, -1));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord("foo", "m1"));
        assertThrows(UnknownMemberIdException.class, () -> context.groupMetadataManager.streamsGroup("foo").getMemberOrThrow("m1"));

        // The group may not exist at all. Replaying the StreamsGroupCurrentMemberAssignment tombstone
        // should be a no-op.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord("bar", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("bar"));
    }

    @Test
    public void testReplayStreamsGroupCurrentMemberAssignmentTombstoneExisting() {
        final TasksTupleWithEpochs tasks =
            new TasksTupleWithEpochs(
                TaskAssignmentTestUtil.mkTasksPerSubtopologyWithCommonEpoch(1,
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 0, 1, 2)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 3, 4, 5)),
                TaskAssignmentTestUtil.mkTasksPerSubtopology(
                    TaskAssignmentTestUtil.mkTasks("subtopology-1", 6, 7, 8))
            );
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroup(
                new StreamsGroupBuilder("foo", 10)
                    .withMember(
                        streamsGroupMemberBuilderWithDefaults("m1")
                            .setAssignedTasks(tasks)
                            .build()
                    )
            )
            .build();

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord("foo", "m1"));

        final StreamsGroupMember member = context.groupMetadataManager.streamsGroup("foo").getMemberOrThrow("m1");
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, member.memberEpoch());
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, member.previousMemberEpoch());
        assertTrue(member.assignedTasks().isEmpty());
        assertTrue(member.tasksPendingRevocation().isEmpty());
    }

    @Test
    public void testReplayStreamsGroupTopology() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        StreamsGroupTopologyValue topology = new StreamsGroupTopologyValue()
            .setEpoch(12)
            .setSubtopologies(
                List.of(
                    new StreamsGroupTopologyValue.Subtopology()
                        .setSubtopologyId("subtopology-1")
                        .setSourceTopics(List.of("source-topic"))
                        .setRepartitionSinkTopics(List.of("sink-topic"))
                )
            );

        // The group and the topology are created if they do not exist.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord("bar", topology));
        final Optional<StreamsTopology> actualTopology = context.groupMetadataManager.streamsGroup("bar").topology();
        assertTrue(actualTopology.isPresent(), "topology should be set");
        assertEquals(topology.epoch(), actualTopology.get().topologyEpoch());
        assertEquals(topology.subtopologies().size(), actualTopology.get().subtopologies().size());
        assertEquals(
            topology.subtopologies().iterator().next(),
            actualTopology.get().subtopologies().values().iterator().next()
        );
    }

    @Test
    public void testReplayStreamsGroupTopologyTombstoneNotExists() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group still exists, but the member is already gone. Replaying the
        // StreamsGroupTopology tombstone should be a no-op.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord("foo", 10, 0, 0, Map.of("num.standby.replicas", "0"), -1, -1));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecordTombstone("foo"));
        assertTrue(context.groupMetadataManager.streamsGroup("foo").topology().isEmpty());

        // The group may not exist at all. Replaying the StreamsGroupTopology tombstone
        // should be a no-op.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecordTombstone("bar"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.streamsGroup("bar"));
    }

    @Test
    public void testReplayStreamsGroupTopologyTombstoneExists() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withStreamsGroup(
                new StreamsGroupBuilder("foo", 10)
                    .withTopology(new StreamsTopology(10, Map.of()))
            )
            .build();

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecordTombstone("foo"));

        assertTrue(context.groupMetadataManager.streamsGroup("foo").topology().isEmpty());
    }

    @Test
    public void testReplayStreamsGroupCurrentMemberAssignmentWithCompaction() {
        String groupId = "fooup";
        String memberIdA = "memberIdA";
        String memberIdB = "memberIdB";
        String processIdA = "processIdA";
        String processIdB = "processIdB";
        String subtopologyId = "subtopology";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();
        // Initialize members with process Ids.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, 
            streamsGroupMemberBuilderWithDefaults(memberIdA)
                .setProcessId(processIdA)
                .build()));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, 
            streamsGroupMemberBuilderWithDefaults(memberIdB)
                .setProcessId(processIdB)
                .build()));

        // This test enacts the following scenario:
        // 1. Member A is assigned task 0.
        // 2. Member A is unassigned task 0 [record removed by compaction].
        // 3. Member B is assigned task 0. 
        // 4. Member A is assigned task 1. 
        // If record 2 is processed, there are no issues, however with compaction it is possible that 
        // unassignment records are removed. We would like to not fail in these cases.
        // Therefore we will allow assignments to owned tasks as long as the epoch is larger.

        // Assign task 0 to member A.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberIdA)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedTasks(TaskAssignmentTestUtil.mkTasksTupleWithEpochs(TaskRole.ACTIVE, 
                    TaskAssignmentTestUtil.mkTasksWithEpochs(subtopologyId, Map.of(0, 11))))
            .build()));

        // Task 0's owner is replaced by member B at epoch 12.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberIdB)
            .setMemberEpoch(12)
            .setPreviousMemberEpoch(11)
            .setAssignedTasks(TaskAssignmentTestUtil.mkTasksTupleWithEpochs(TaskRole.ACTIVE, 
                    TaskAssignmentTestUtil.mkTasksWithEpochs(subtopologyId, Map.of(0, 12))))
            .build()));

        // Task 0 must remain with member B at epoch 12 even though member A has just been unassigned task 0.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberIdA)
            .setMemberEpoch(13)
            .setPreviousMemberEpoch(12)
            .setAssignedTasks(TaskAssignmentTestUtil.mkTasksTupleWithEpochs(TaskRole.ACTIVE, 
                    TaskAssignmentTestUtil.mkTasksWithEpochs(subtopologyId, Map.of(1, 13))))
            .build()));

        // Verify task 1 is assigned to member A and task 0 to member B.
        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
        assertEquals(processIdA, group.currentActiveTaskProcessId(subtopologyId, 1));
        assertEquals(processIdB, group.currentActiveTaskProcessId(subtopologyId, 0));
    }

    @Test
    public void testReplayStreamsGroupCurrentMemberAssignmentUnownedTopologyWithCompaction() {
        String groupId = "fooup";
        String memberIdA = "memberIdA";
        String memberIdB = "memberIdB";
        String processIdA = "processIdA";
        String processIdB = "processIdB";
        String subtopologyFoo = "subtopologyFoo";
        String subtopologyBar = "subtopologyBar";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();
        // Initialize members with process Ids.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, 
            streamsGroupMemberBuilderWithDefaults(memberIdA)
                .setProcessId(processIdA)
                .build()));
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, 
            streamsGroupMemberBuilderWithDefaults(memberIdB)
                .setProcessId(processIdB)
                .build()));

        // This test enacts the following scenario:
        // 1. Member A is assigned task foo-0.
        // 2. Member A is unassigned task foo-0 [record removed by compaction].
        // 3. Member B is assigned task foo-0.
        // 4. Member B is unassigned task foo-0. 
        // 5. Member A is assigned task bar-0. 
        // This is a legitimate set of assignments but with compaction the unassignment record can be skipped.
        // This can lead to conflicts from updating an owned subtopology in step 3 and attempting to remove
        // nonexistent ownership in step 5. We want to ensure removing ownership from a 
        // completely unowned subtopology in step 5 is allowed.  

        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberIdA)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedTasks(TaskAssignmentTestUtil.mkTasksTupleWithEpochs(TaskRole.ACTIVE, 
                TaskAssignmentTestUtil.mkTasksWithEpochs(subtopologyFoo, Map.of(0, 11))))
            .build()));

        // foo-0's owner is replaced by member B at epoch 12.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberIdB)
            .setMemberEpoch(12)
            .setPreviousMemberEpoch(11)
            .setAssignedTasks(TaskAssignmentTestUtil.mkTasksTupleWithEpochs(TaskRole.ACTIVE, 
                TaskAssignmentTestUtil.mkTasksWithEpochs(subtopologyFoo, Map.of(0, 12))))
            .build()));

        // foo becomes unowned
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberIdB)
            .setMemberEpoch(13)
            .setPreviousMemberEpoch(12)
            .build()));

        // Member A is unassigned foo-0.
        context.replay(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, streamsGroupMemberBuilderWithDefaults(memberIdA)
            .setMemberEpoch(14)
            .setPreviousMemberEpoch(13)
            .setAssignedTasks(TaskAssignmentTestUtil.mkTasksTupleWithEpochs(TaskRole.ACTIVE, 
                TaskAssignmentTestUtil.mkTasksWithEpochs(subtopologyBar, Map.of(0, 14))))
            .build()));

        // Verify foo-0 is unassigned and bar-0 is assigned to member A.
        StreamsGroup group = context.groupMetadataManager.streamsGroup(groupId);
        assertNull(group.currentActiveTaskProcessId(subtopologyFoo, 0));
        assertEquals(processIdA, group.currentActiveTaskProcessId(subtopologyBar, 0));
    }

    @Test
    public void testStreamsGroupAssignmentInterval() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        String subtopology = "subtopology";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Topology topology = new Topology().setSubtopologies(List.of(
            new Subtopology().setSubtopologyId(subtopology).setSourceTopics(List.of(fooTopicName))
        ));

        MockTaskAssignor assignor = new MockTaskAssignor("sticky");

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0)
            .withConfig(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, 5000)
            .withStreamsGroupTaskAssignors(List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Member 1 joins the group and gets an assignment immediately.
        assignor.prepareGroupAssignment(Map.of(memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
            TaskAssignmentTestUtil.mkTasks(subtopology, 0, 1, 2, 3, 4, 5)
        )));
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result1 = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(0)
                .setProcessId("process-id")
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of(
                    new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopology)
                        .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                ))
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000L),
            result1.response().data()
        );

        StreamsGroupMember expectedMember1 = streamsGroupMemberBuilderWithDefaults(memberId1)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setAssignedTasks(mkTasksTupleWithCommonEpoch(TaskRole.ACTIVE, 2,
                TaskAssignmentTestUtil.mkTasks(subtopology, 0, 1, 2, 3, 4, 5)))
            .build();

        assertRecordsEquals(
            List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedMember1),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(groupId, topology),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, 2, computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage)
                )), 0, new TreeMap<>(Map.of(
                    "num.standby.replicas", "0"
                )), -1, -1),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId1,
                    TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                        TaskAssignmentTestUtil.mkTasks(subtopology, 0, 1, 2, 3, 4, 5)
                    )),
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember1)
            ),
            result1.records()
        );

        // Wait until just before the expected delay.
        context.time.sleep(4995);

        // Member 2 joins the group and gets no assignment.
        assignor.prepareGroupAssignment(Map.of(
            memberId1, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology, 0, 1, 2)
            ),
            memberId2, TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                TaskAssignmentTestUtil.mkTasks(subtopology, 3, 4, 5)
            )
        ));
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result2 = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setProcessId("process-id")
                .setRebalanceTimeoutMs(1500)
                .setTopology(topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of()));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setEndpointInformationEpoch(0)
                .setStatus(List.of(new StreamsGroupHeartbeatResponseData.Status()
                    .setStatusCode(Status.ASSIGNMENT_DELAYED.code())
                    .setStatusDetail("Assignment delayed due to the configured assignment interval.")))
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000L),
            result2.response().data()
        );

        StreamsGroupMember expectedMember2 = streamsGroupMemberBuilderWithDefaults(memberId2)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .build();

        assertRecordsEquals(
            List.of(
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, expectedMember2),
                StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, 3, computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage)
                )), 0, new TreeMap<>(Map.of(
                    "num.standby.replicas", "0"
                )), -1, -1),
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember2)
            ),
            result2.records()
        );

        // Wait a little more. The next target assignment can be computed now.
        context.time.sleep(10);

        // The next target assignment is computed.
        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result3 = context.streamsGroupHeartbeat(
            new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(2));

        assertResponseEquals(
            new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(3)
                .setHeartbeatIntervalMs(5000)
                .setEndpointInformationEpoch(0)
                .setStatus(List.of())
                .setTaskOffsetIntervalMs(60_000)
                .setAcceptableRecoveryLag(10_000L),
            result3.response().data()
        );

        StreamsGroupMember expectedMember3 = streamsGroupMemberBuilderWithDefaults(memberId2)
            .setState(org.apache.kafka.coordinator.group.streams.MemberState.UNRELEASED_TASKS)
            .setMemberEpoch(3)
            .setPreviousMemberEpoch(2)
            .build();

        assertUnorderedRecordsEquals(
            List.of(
                List.of(
                    StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId1,
                        TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                            TaskAssignmentTestUtil.mkTasks(subtopology, 0, 1, 2)
                        )),
                    StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId2,
                        TaskAssignmentTestUtil.mkTasksTuple(TaskRole.ACTIVE,
                            TaskAssignmentTestUtil.mkTasks(subtopology, 3, 4, 5)
                        ))
                ),
                List.of(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(groupId, 3, context.time.milliseconds())),
                List.of(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, expectedMember3))
            ),
            result3.records()
        );
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

}
