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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.OffsetAndMetadata;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;
import org.apache.kafka.coordinator.group.streams.topics.InternalTopicManager;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksPerSubtopology;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTuple;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamsGroupTest {

    private static final LogContext LOG_CONTEXT = new LogContext();

    private StreamsGroup createStreamsGroup(String groupId) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        return new StreamsGroup(
            LOG_CONTEXT,
            snapshotRegistry,
            groupId,
            mock(GroupCoordinatorMetricsShard.class)
        );
    }

    @Test
    public void testGetOrCreateMember() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember member;

        // Create a member.
        member = streamsGroup.getOrMaybeCreateMember("member-id", true);
        assertEquals("member-id", member.memberId());

        // Add member to the group.
        streamsGroup.updateMember(member);

        // Get that member back.
        member = streamsGroup.getOrMaybeCreateMember("member-id", false);
        assertEquals("member-id", member.memberId());

        assertThrows(UnknownMemberIdException.class, () ->
            streamsGroup.getOrMaybeCreateMember("does-not-exist", false));
    }

    @Test
    public void testUpdateMember() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember member;

        member = streamsGroup.getOrMaybeCreateMember("member", true);

        member = new StreamsGroupMember.Builder(member).build();

        streamsGroup.updateMember(member);

        assertEquals(member, streamsGroup.getOrMaybeCreateMember("member", false));
    }

    @Test
    public void testNoStaticMember() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        // Create a new member which is not static
        streamsGroup.getOrMaybeCreateMember("member", true);
        assertNull(streamsGroup.staticMember("instance-id"));
    }

    @Test
    public void testGetStaticMemberByInstanceId() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember member;

        member = streamsGroup.getOrMaybeCreateMember("member", true);

        member = new StreamsGroupMember.Builder(member)
            .setInstanceId("instance")
            .build();

        streamsGroup.updateMember(member);

        assertEquals(member, streamsGroup.staticMember("instance"));
        assertEquals(member, streamsGroup.getOrMaybeCreateMember("member", false));
        assertEquals(member.memberId(), streamsGroup.staticMemberId("instance"));
    }

    @Test
    public void testRemoveMember() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        StreamsGroupMember member = streamsGroup.getOrMaybeCreateMember("member", true);
        streamsGroup.updateMember(member);
        assertTrue(streamsGroup.hasMember("member"));

        streamsGroup.removeMember("member");
        assertFalse(streamsGroup.hasMember("member"));

    }

    @Test
    public void testRemoveStaticMember() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setInstanceId("instance")
            .build();

        streamsGroup.updateMember(member);
        assertTrue(streamsGroup.hasMember("member"));

        streamsGroup.removeMember("member");
        assertFalse(streamsGroup.hasMember("member"));
        assertNull(streamsGroup.staticMember("instance"));
        assertNull(streamsGroup.staticMemberId("instance"));
    }

    @Test
    public void testUpdatingMemberUpdatesProcessId() {
        String fooSubtopology = "foo-sub";
        String barSubtopology = "bar-sub";
        String zarSubtopology = "zar-sub";

        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember member;

        member = new StreamsGroupMember.Builder("member")
            .setProcessId("process")
            .setAssignedTasks(
                new TasksTuple(
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 1)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 2)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 3))
                )
            )
            .setTasksPendingRevocation(
                new TasksTuple(
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 4)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 5)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 6))
                )
            )
            .build();

        streamsGroup.updateMember(member);

        assertEquals("process", streamsGroup.currentActiveTaskProcessId(fooSubtopology, 1));
        assertEquals(Collections.singleton("process"),
            streamsGroup.currentStandbyTaskProcessIds(fooSubtopology, 2));
        assertEquals(Collections.singleton("process"),
            streamsGroup.currentWarmupTaskProcessIds(fooSubtopology, 3));
        assertEquals("process", streamsGroup.currentActiveTaskProcessId(barSubtopology, 4));
        assertEquals(Collections.singleton("process"),
            streamsGroup.currentStandbyTaskProcessIds(barSubtopology, 5));
        assertEquals(Collections.singleton("process"),
            streamsGroup.currentWarmupTaskProcessIds(barSubtopology, 6));
        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 7));
        assertEquals(Collections.emptySet(),
            streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 8));
        assertEquals(Collections.emptySet(),
            streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 9));

        member = new StreamsGroupMember.Builder(member)
            .setProcessId("process1")
            .setAssignedTasks(
                new TasksTuple(
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 1)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 2)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 3))
                )
            )
            .setTasksPendingRevocation(
                new TasksTuple(
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 4)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 5)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 6))
                )
            )
            .build();

        streamsGroup.updateMember(member);

        assertEquals("process1", streamsGroup.currentActiveTaskProcessId(fooSubtopology, 1));
        assertEquals(Collections.singleton("process1"),
            streamsGroup.currentStandbyTaskProcessIds(fooSubtopology, 2));
        assertEquals(Collections.singleton("process1"),
            streamsGroup.currentWarmupTaskProcessIds(fooSubtopology, 3));
        assertEquals("process1", streamsGroup.currentActiveTaskProcessId(barSubtopology, 4));
        assertEquals(Collections.singleton("process1"),
            streamsGroup.currentStandbyTaskProcessIds(barSubtopology, 5));
        assertEquals(Collections.singleton("process1"),
            streamsGroup.currentWarmupTaskProcessIds(barSubtopology, 6));
        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 7));
        assertEquals(Collections.emptySet(),
            streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 8));
        assertEquals(Collections.emptySet(),
            streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 9));
    }

    @Test
    public void testUpdatingMemberUpdatesTaskProcessIdWhenPartitionIsReassignedBeforeBeingRevoked() {
        String fooSubtopologyId = "foo-sub";

        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember member;

        member = new StreamsGroupMember.Builder("member")
            .setProcessId("process")
            .setAssignedTasks(
                new TasksTuple(
                    emptyMap(),
                    emptyMap(),
                    emptyMap()
                )
            )
            .setTasksPendingRevocation(
                new TasksTuple(
                    mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 2)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 3))
                )
            )
            .build();

        streamsGroup.updateMember(member);

        assertEquals("process", streamsGroup.currentActiveTaskProcessId(fooSubtopologyId, 1));

        member = new StreamsGroupMember.Builder(member)
            .setProcessId("process1")
            .setAssignedTasks(
                new TasksTuple(
                    mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 2)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 3))
                )
            )
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .build();

        streamsGroup.updateMember(member);

        assertEquals("process1", streamsGroup.currentActiveTaskProcessId(fooSubtopologyId, 1));
    }

    @Test
    public void testUpdatingMemberUpdatesTaskProcessIdWhenPartitionIsNotReleased() {
        String fooSubtopologyId = "foo-sub";
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        StreamsGroupMember m1 = new StreamsGroupMember.Builder("m1")
            .setProcessId("process")
            .setAssignedTasks(
                new TasksTuple(
                    mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)),
                    emptyMap(),
                    emptyMap()
                )
            )
            .build();

        streamsGroup.updateMember(m1);

        StreamsGroupMember m2 = new StreamsGroupMember.Builder("m2")
            .setProcessId("process")
            .setAssignedTasks(
                new TasksTuple(
                    mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)),
                    emptyMap(),
                    emptyMap()
                )
            )
            .build();

        // m2 should not be able to acquire foo-1 because the partition is
        // still owned by another member.
        assertThrows(IllegalStateException.class, () -> streamsGroup.updateMember(m2));
    }


    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testRemoveTaskProcessIds(TaskRole taskRole) {
        String fooSubtopologyId = "foo-sub";
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        // Removing should fail because there is no epoch set.
        assertThrows(IllegalStateException.class, () -> streamsGroup.removeTaskProcessIds(
            mkTasksTuple(taskRole, mkTasks(fooSubtopologyId, 1)),
            "process"
        ));

        StreamsGroupMember m1 = new StreamsGroupMember.Builder("m1")
            .setProcessId("process")
            .setAssignedTasks(mkTasksTuple(taskRole, mkTasks(fooSubtopologyId, 1)))
            .build();

        streamsGroup.updateMember(m1);

        // Removing should fail because the expected epoch is incorrect.
        assertThrows(IllegalStateException.class, () -> streamsGroup.removeTaskProcessIds(
            mkTasksTuple(taskRole, mkTasks(fooSubtopologyId, 1)),
            "process1"
        ));
    }

    @Test
    public void testAddTaskProcessIds() {
        String fooSubtopologyId = "foo-sub";
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        streamsGroup.addTaskProcessId(
            new TasksTuple(
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)),
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 2)),
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 3))
            ),
            "process"
        );

        // Changing the epoch should fail because the owner of the partition
        // should remove it first.
        assertThrows(IllegalStateException.class, () -> streamsGroup.addTaskProcessId(
            new TasksTuple(
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)),
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 2)),
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 3))
            ),
            "process"
        ));
    }

    @Test
    public void testDeletingMemberRemovesProcessId() {
        String fooSubtopology = "foo-sub";
        String barSubtopology = "bar-sub";
        String zarSubtopology = "zar-sub";

        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember member;

        member = new StreamsGroupMember.Builder("member")
            .setProcessId("process")
            .setAssignedTasks(
                new TasksTuple(
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 1)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 2)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 3))
                )
            )
            .setTasksPendingRevocation(
                new TasksTuple(
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 4)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 5)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 6))
                )
            )
            .build();

        streamsGroup.updateMember(member);

        assertEquals("process", streamsGroup.currentActiveTaskProcessId(fooSubtopology, 1));
        assertEquals(Collections.singleton("process"), streamsGroup.currentStandbyTaskProcessIds(fooSubtopology, 2));
        assertEquals(Collections.singleton("process"), streamsGroup.currentWarmupTaskProcessIds(fooSubtopology, 3));
        assertEquals("process", streamsGroup.currentActiveTaskProcessId(barSubtopology, 4));
        assertEquals(Collections.singleton("process"), streamsGroup.currentStandbyTaskProcessIds(barSubtopology, 5));
        assertEquals(Collections.singleton("process"), streamsGroup.currentWarmupTaskProcessIds(barSubtopology, 6));
        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 7));
        assertEquals(Collections.emptySet(), streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 8));
        assertEquals(Collections.emptySet(), streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 9));

        streamsGroup.removeMember(member.memberId());

        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 1));
        assertEquals(Collections.emptySet(), streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 2));
        assertEquals(Collections.emptySet(), streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 3));
        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 3));
        assertEquals(Collections.emptySet(), streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 4));
        assertEquals(Collections.emptySet(), streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 5));
        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 7));
        assertEquals(Collections.emptySet(), streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 8));
        assertEquals(Collections.emptySet(), streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 9));
    }

    @Test
    public void testGroupState() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");
        assertEquals(StreamsGroupState.EMPTY, streamsGroup.state());

        StreamsGroupMember member1 = new StreamsGroupMember.Builder("member1")
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .build();

        streamsGroup.updateMember(member1);
        streamsGroup.setGroupEpoch(1);

        assertEquals(MemberState.STABLE, member1.state());
        assertEquals(StreamsGroup.StreamsGroupState.NOT_READY, streamsGroup.state());

        streamsGroup.setTopology(new StreamsTopology(1, Collections.emptyMap()));

        assertEquals(MemberState.STABLE, member1.state());
        assertEquals(StreamsGroup.StreamsGroupState.ASSIGNING, streamsGroup.state());

        StreamsGroupMember member2 = new StreamsGroupMember.Builder("member2")
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .build();

        streamsGroup.updateMember(member2);
        streamsGroup.setGroupEpoch(2);

        assertEquals(MemberState.STABLE, member2.state());
        assertEquals(StreamsGroup.StreamsGroupState.ASSIGNING, streamsGroup.state());

        streamsGroup.setTargetAssignmentEpoch(2);

        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, streamsGroup.state());

        member1 = new StreamsGroupMember.Builder(member1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .build();

        streamsGroup.updateMember(member1);

        assertEquals(MemberState.STABLE, member1.state());
        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, streamsGroup.state());

        // Member 2 is not stable so the group stays in reconciling state.
        member2 = new StreamsGroupMember.Builder(member2)
            .setState(MemberState.UNREVOKED_TASKS)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .build();

        streamsGroup.updateMember(member2);

        assertEquals(MemberState.UNREVOKED_TASKS, member2.state());
        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, streamsGroup.state());

        member2 = new StreamsGroupMember.Builder(member2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .build();

        streamsGroup.updateMember(member2);

        assertEquals(MemberState.STABLE, member2.state());
        assertEquals(StreamsGroup.StreamsGroupState.STABLE, streamsGroup.state());

        streamsGroup.removeMember("member1");
        streamsGroup.removeMember("member2");

        assertEquals(StreamsGroup.StreamsGroupState.EMPTY, streamsGroup.state());
    }

    @Test
    public void testMetadataRefreshDeadline() {
        MockTime time = new MockTime();
        StreamsGroup group = createStreamsGroup("group-foo");

        // Group epoch starts at 0.
        assertEquals(0, group.groupEpoch());

        // The refresh time deadline should be empty when the group is created or loaded.
        assertTrue(group.hasMetadataExpired(time.milliseconds()));
        assertEquals(0L, group.metadataRefreshDeadline().deadlineMs);
        assertEquals(0, group.metadataRefreshDeadline().epoch);

        // Set the refresh deadline. The metadata remains valid because the deadline
        // has not past and the group epoch is correct.
        group.setMetadataRefreshDeadline(time.milliseconds() + 1000, group.groupEpoch());
        assertFalse(group.hasMetadataExpired(time.milliseconds()));
        assertEquals(time.milliseconds() + 1000, group.metadataRefreshDeadline().deadlineMs);
        assertEquals(group.groupEpoch(), group.metadataRefreshDeadline().epoch);

        // Advance past the deadline. The metadata should have expired.
        time.sleep(1001L);
        assertTrue(group.hasMetadataExpired(time.milliseconds()));

        // Set the refresh time deadline with a higher group epoch. The metadata is considered
        // as expired because the group epoch attached to the deadline is higher than the
        // current group epoch.
        group.setMetadataRefreshDeadline(time.milliseconds() + 1000, group.groupEpoch() + 1);
        assertTrue(group.hasMetadataExpired(time.milliseconds()));
        assertEquals(time.milliseconds() + 1000, group.metadataRefreshDeadline().deadlineMs);
        assertEquals(group.groupEpoch() + 1, group.metadataRefreshDeadline().epoch);

        // Advance the group epoch.
        group.setGroupEpoch(group.groupEpoch() + 1);

        // Set the refresh deadline. The metadata remains valid because the deadline
        // has not past and the group epoch is correct.
        group.setMetadataRefreshDeadline(time.milliseconds() + 1000, group.groupEpoch());
        assertFalse(group.hasMetadataExpired(time.milliseconds()));
        assertEquals(time.milliseconds() + 1000, group.metadataRefreshDeadline().deadlineMs);
        assertEquals(group.groupEpoch(), group.metadataRefreshDeadline().epoch);

        // Request metadata refresh. The metadata expires immediately.
        group.requestMetadataRefresh();
        assertTrue(group.hasMetadataExpired(time.milliseconds()));
        assertEquals(0L, group.metadataRefreshDeadline().deadlineMs);
        assertEquals(0, group.metadataRefreshDeadline().epoch);
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.TXN_OFFSET_COMMIT)
    public void testValidateTransactionalOffsetCommit(short version) {
        boolean isTransactional = true;
        StreamsGroup group = createStreamsGroup("group-foo");


        // Simulate a call from the admin client without member ID and member epoch.
        // This should pass only if the group is empty.
        group.validateOffsetCommit("", "", -1, isTransactional, version);

        // The member does not exist.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("member-id", null, 0, isTransactional, version));

        // Create a member.
        group.updateMember(new StreamsGroupMember.Builder("member-id").setMemberEpoch(0).build());

        // A call from the admin client should fail as the group is not empty.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("", "", -1, isTransactional, version));

        // The member epoch is stale.
        assertThrows(StaleMemberEpochException.class, () ->
            group.validateOffsetCommit("member-id", "", 10, isTransactional, version));

        // This should succeed.
        group.validateOffsetCommit("member-id", "", 0, isTransactional, version);

        // This should succeed.
        group.validateOffsetCommit("", null, -1, isTransactional, version);
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testValidateOffsetCommit(short version) {
        boolean isTransactional = false;
        StreamsGroup group = createStreamsGroup("group-foo");

        // Simulate a call from the admin client without member ID and member epoch.
        // This should pass only if the group is empty.
        group.validateOffsetCommit("", "", -1, isTransactional, version);

        // The member does not exist.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("member-id", null, 0, isTransactional, version));

        // Create members.
        group.updateMember(
            new StreamsGroupMember
                .Builder("new-protocol-member-id").setMemberEpoch(0).build()
        );

        // A call from the admin client should fail as the group is not empty.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("", "", -1, isTransactional, version));
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("", null, -1, isTransactional, version));

        // The member epoch is stale.
        if (version >= 9) {
            assertThrows(StaleMemberEpochException.class, () ->
                group.validateOffsetCommit("new-protocol-member-id", "", 10, isTransactional, version));
        } else {
            assertThrows(UnsupportedVersionException.class, () ->
                group.validateOffsetCommit("new-protocol-member-id", "", 10, isTransactional, version));
        }

        // This should succeed.
        if (version >= 9) {
            group.validateOffsetCommit("new-protocol-member-id", "", 0, isTransactional, version);
        } else {
            assertThrows(UnsupportedVersionException.class, () ->
                group.validateOffsetCommit("new-protocol-member-id", "", 0, isTransactional, version));
        }
    }

    @Test
    public void testAsListedGroup() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        StreamsGroup group = new StreamsGroup(
            LOG_CONTEXT,
            snapshotRegistry,
            "group-foo",
            mock(GroupCoordinatorMetricsShard.class)
        );
        group.setGroupEpoch(1);
        group.setTopology(new StreamsTopology(1, Collections.emptyMap()));
        group.setTargetAssignmentEpoch(1);
        group.updateMember(new StreamsGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .build());
        snapshotRegistry.idempotentCreateSnapshot(1);

        ListGroupsResponseData.ListedGroup listedGroup = group.asListedGroup(1);

        assertEquals("group-foo", listedGroup.groupId());
        assertEquals("streams", listedGroup.protocolType());
        assertEquals("Reconciling", listedGroup.groupState());
        assertEquals("streams", listedGroup.groupType());
    }

    @Test
    public void testValidateOffsetFetch() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        StreamsGroup group = new StreamsGroup(
            LOG_CONTEXT,
            snapshotRegistry,
            "group-foo",
            mock(GroupCoordinatorMetricsShard.class)
        );

        // Simulate a call from the admin client without member ID and member epoch.
        group.validateOffsetFetch(null, -1, Long.MAX_VALUE);

        // The member does not exist.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetFetch("member-id", 0, Long.MAX_VALUE));

        // Create a member.
        snapshotRegistry.idempotentCreateSnapshot(0);
        group.updateMember(new StreamsGroupMember.Builder("member-id").setMemberEpoch(0).build());

        // The member does not exist at last committed offset 0.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetFetch("member-id", 0, 0));

        // The member exists but the epoch is stale when the last committed offset is not considered.
        assertThrows(StaleMemberEpochException.class, () ->
            group.validateOffsetFetch("member-id", 10, Long.MAX_VALUE));

        // This should succeed.
        group.validateOffsetFetch("member-id", 0, Long.MAX_VALUE);
    }

    @Test
    public void testValidateDeleteGroup() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        assertEquals(StreamsGroupState.EMPTY, streamsGroup.state());
        assertDoesNotThrow(streamsGroup::validateDeleteGroup);

        StreamsGroupMember member1 = new StreamsGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .build();
        streamsGroup.updateMember(member1);

        assertEquals(StreamsGroup.StreamsGroupState.NOT_READY, streamsGroup.state());
        assertThrows(GroupNotEmptyException.class, streamsGroup::validateDeleteGroup);

        streamsGroup.setTopology(new StreamsTopology(1, Collections.emptyMap()));

        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, streamsGroup.state());
        assertThrows(GroupNotEmptyException.class, streamsGroup::validateDeleteGroup);

        streamsGroup.setGroupEpoch(1);

        assertEquals(StreamsGroup.StreamsGroupState.ASSIGNING, streamsGroup.state());
        assertThrows(GroupNotEmptyException.class, streamsGroup::validateDeleteGroup);

        streamsGroup.setTargetAssignmentEpoch(1);

        assertEquals(StreamsGroup.StreamsGroupState.STABLE, streamsGroup.state());
        assertThrows(GroupNotEmptyException.class, streamsGroup::validateDeleteGroup);

        streamsGroup.removeMember("member1");
        assertEquals(StreamsGroup.StreamsGroupState.EMPTY, streamsGroup.state());
        assertDoesNotThrow(streamsGroup::validateDeleteGroup);
    }

    @Test
    public void testOffsetExpirationCondition() {
        long currentTimestamp = 30000L;
        long commitTimestamp = 20000L;
        long offsetsRetentionMs = 10000L;
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(15000L, OptionalInt.empty(), "", commitTimestamp, OptionalLong.empty());
        StreamsGroup group = new StreamsGroup(LOG_CONTEXT, new SnapshotRegistry(LOG_CONTEXT), "group-id", mock(GroupCoordinatorMetricsShard.class));

        Optional<OffsetExpirationCondition> offsetExpirationCondition = group.offsetExpirationCondition();
        assertTrue(offsetExpirationCondition.isPresent());

        OffsetExpirationConditionImpl condition = (OffsetExpirationConditionImpl) offsetExpirationCondition.get();
        assertEquals(commitTimestamp, condition.baseTimestamp().apply(offsetAndMetadata));
        assertTrue(condition.isOffsetExpired(offsetAndMetadata, currentTimestamp, offsetsRetentionMs));
    }

    @Test
    public void testAsDescribedGroup() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        StreamsGroup group = new StreamsGroup(LOG_CONTEXT, snapshotRegistry, "group-id-1", mock(GroupCoordinatorMetricsShard.class));
        snapshotRegistry.idempotentCreateSnapshot(0);
        assertEquals(StreamsGroup.StreamsGroupState.EMPTY.toString(), group.stateAsString(0));

        group.setGroupEpoch(1);
        group.setTopology(new StreamsTopology(1, Collections.emptyMap()));
        group.setTargetAssignmentEpoch(1);
        group.updateMember(new StreamsGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setInstanceId("instance1")
            .setRackId("rack1")
            .setClientId("client1")
            .setClientHost("host1")
            .setRebalanceTimeoutMs(1000)
            .setTopologyEpoch(1)
            .setProcessId("process1")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host1").setPort(9092))
            .setClientTags(Collections.singletonMap("tag1", "value1"))
            .setAssignedTasks(new TasksTuple(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()))
            .setTasksPendingRevocation(new TasksTuple(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()))
            .build());
        group.updateMember(new StreamsGroupMember.Builder("member2")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setInstanceId("instance2")
            .setRackId("rack2")
            .setClientId("client2")
            .setClientHost("host2")
            .setRebalanceTimeoutMs(1000)
            .setTopologyEpoch(1)
            .setProcessId("process2")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host2").setPort(9092))
            .setClientTags(Collections.singletonMap("tag2", "value2"))
            .setAssignedTasks(new TasksTuple(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()))
            .setTasksPendingRevocation(new TasksTuple(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()))
            .build());
        snapshotRegistry.idempotentCreateSnapshot(1);

        StreamsGroupDescribeResponseData.DescribedGroup expected = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId("group-id-1")
            .setGroupState(StreamsGroup.StreamsGroupState.STABLE.toString())
            .setGroupEpoch(1)
            .setTopology(new StreamsGroupDescribeResponseData.Topology().setEpoch(1).setSubtopologies(Collections.emptyList()))
            .setAssignmentEpoch(1)
            .setMembers(Arrays.asList(
                new StreamsGroupDescribeResponseData.Member()
                    .setMemberId("member1")
                    .setMemberEpoch(1)
                    .setInstanceId("instance1")
                    .setRackId("rack1")
                    .setClientId("client1")
                    .setClientHost("host1")
                    .setTopologyEpoch(1)
                    .setProcessId("process1")
                    .setUserEndpoint(new StreamsGroupDescribeResponseData.Endpoint().setHost("host1").setPort(9092))
                    .setClientTags(Collections.singletonList(new StreamsGroupDescribeResponseData.KeyValue().setKey("tag1").setValue("value1")))
                    .setAssignment(new StreamsGroupDescribeResponseData.Assignment())
                    .setTargetAssignment(new StreamsGroupDescribeResponseData.Assignment()),
                new StreamsGroupDescribeResponseData.Member()
                    .setMemberId("member2")
                    .setMemberEpoch(1)
                    .setInstanceId("instance2")
                    .setRackId("rack2")
                    .setClientId("client2")
                    .setClientHost("host2")
                    .setTopologyEpoch(1)
                    .setProcessId("process2")
                    .setUserEndpoint(new StreamsGroupDescribeResponseData.Endpoint().setHost("host2").setPort(9092))
                    .setClientTags(Collections.singletonList(new StreamsGroupDescribeResponseData.KeyValue().setKey("tag2").setValue("value2")))
                    .setAssignment(new StreamsGroupDescribeResponseData.Assignment())
                    .setTargetAssignment(new StreamsGroupDescribeResponseData.Assignment())
            ));
        StreamsGroupDescribeResponseData.DescribedGroup actual = group.asDescribedGroup(1);

        assertEquals(expected, actual);
    }

    @Test
    public void testStateTransitionMetrics() {
        // Confirm metrics is not updated when a new StreamsGroup is created but only when the group transitions
        // its state.
        GroupCoordinatorMetricsShard metrics = mock(GroupCoordinatorMetricsShard.class);
        StreamsGroup streamsGroup = new StreamsGroup(
            LOG_CONTEXT,
            new SnapshotRegistry(new LogContext()),
            "group-id",
            metrics
        );

        assertEquals(StreamsGroup.StreamsGroupState.EMPTY, streamsGroup.state());
        verify(metrics, times(0)).onStreamsGroupStateTransition(null, StreamsGroup.StreamsGroupState.EMPTY);

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .build();

        streamsGroup.updateMember(member);

        assertEquals(StreamsGroup.StreamsGroupState.NOT_READY, streamsGroup.state());
        verify(metrics, times(1)).onStreamsGroupStateTransition(StreamsGroup.StreamsGroupState.EMPTY, StreamsGroup.StreamsGroupState.NOT_READY);

        streamsGroup.setTopology(new StreamsTopology(1, Collections.emptyMap()));

        assertEquals(StreamsGroup.StreamsGroupState.RECONCILING, streamsGroup.state());
        verify(metrics, times(1)).onStreamsGroupStateTransition(StreamsGroup.StreamsGroupState.NOT_READY, StreamsGroup.StreamsGroupState.RECONCILING);

        streamsGroup.setGroupEpoch(1);

        assertEquals(StreamsGroup.StreamsGroupState.ASSIGNING, streamsGroup.state());
        verify(metrics, times(1)).onStreamsGroupStateTransition(StreamsGroup.StreamsGroupState.RECONCILING, StreamsGroup.StreamsGroupState.ASSIGNING);

        streamsGroup.setTargetAssignmentEpoch(1);

        assertEquals(StreamsGroup.StreamsGroupState.STABLE, streamsGroup.state());
        verify(metrics, times(1)).onStreamsGroupStateTransition(StreamsGroup.StreamsGroupState.ASSIGNING, StreamsGroup.StreamsGroupState.STABLE);

        streamsGroup.removeMember("member");

        assertEquals(StreamsGroup.StreamsGroupState.EMPTY, streamsGroup.state());
        verify(metrics, times(1)).onStreamsGroupStateTransition(StreamsGroup.StreamsGroupState.STABLE, StreamsGroup.StreamsGroupState.EMPTY);
    }

    @Test
    public void testIsInStatesCaseInsensitiveAndUnderscored() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        GroupCoordinatorMetricsShard metricsShard = new GroupCoordinatorMetricsShard(
            snapshotRegistry,
            emptyMap(),
            new TopicPartition("__consumer_offsets", 0)
        );
        StreamsGroup group = new StreamsGroup(LOG_CONTEXT, snapshotRegistry, "group-foo", metricsShard);
        snapshotRegistry.idempotentCreateSnapshot(0);
        assertTrue(group.isInStates(Collections.singleton("empty"), 0));
        assertFalse(group.isInStates(Collections.singleton("Empty"), 0));

        group.updateMember(new StreamsGroupMember.Builder("member1")
            .build());
        snapshotRegistry.idempotentCreateSnapshot(1);
        assertTrue(group.isInStates(Collections.singleton("empty"), 0));
        assertTrue(group.isInStates(Collections.singleton("not_ready"), 1));
        assertFalse(group.isInStates(Collections.singleton("empty"), 1));
    }

    @Test
    public void testSetTopologyUpdatesStateAndConfiguredTopology() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        GroupCoordinatorMetricsShard metricsShard = mock(GroupCoordinatorMetricsShard.class);
        StreamsGroup streamsGroup = new StreamsGroup(LOG_CONTEXT, snapshotRegistry, "test-group", metricsShard);

        StreamsTopology topology = new StreamsTopology(1, Collections.emptyMap());

        ConfiguredTopology topo = mock(ConfiguredTopology.class);
        when(topo.isReady()).thenReturn(true);

        try (MockedStatic<InternalTopicManager> mocked = mockStatic(InternalTopicManager.class)) {
            mocked.when(() -> InternalTopicManager.configureTopics(any(), eq(topology), eq(Map.of()))).thenReturn(topo);
            streamsGroup.setTopology(topology);
            mocked.verify(() -> InternalTopicManager.configureTopics(any(), eq(topology), eq(Map.of())));
        }

        Optional<ConfiguredTopology> configuredTopology = streamsGroup.configuredTopology();
        assertTrue(configuredTopology.isPresent(), "Configured topology should be present");
        assertEquals(StreamsGroupState.EMPTY, streamsGroup.state());

        streamsGroup.updateMember(new StreamsGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .build());

        assertEquals(StreamsGroupState.RECONCILING, streamsGroup.state());
    }

    @Test
    public void testSetPartitionMetadataUpdatesStateAndConfiguredTopology() {
        Uuid topicUuid = Uuid.randomUuid();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        GroupCoordinatorMetricsShard metricsShard = mock(GroupCoordinatorMetricsShard.class);
        StreamsGroup streamsGroup = new StreamsGroup(LOG_CONTEXT, snapshotRegistry, "test-group", metricsShard);

        assertEquals(StreamsGroup.StreamsGroupState.EMPTY, streamsGroup.state());

        Map<String, TopicMetadata> partitionMetadata = new HashMap<>();
        partitionMetadata.put("topic1", new TopicMetadata(topicUuid, "topic1", 1));

        try (MockedStatic<InternalTopicManager> mocked = mockStatic(InternalTopicManager.class)) {
            streamsGroup.setPartitionMetadata(partitionMetadata);
            mocked.verify(() -> InternalTopicManager.configureTopics(any(), any(), any()), never());
        }

        assertTrue(streamsGroup.configuredTopology().isEmpty(), "Configured topology should not be present");
        assertEquals(partitionMetadata, streamsGroup.partitionMetadata());

        StreamsTopology topology = new StreamsTopology(1, Collections.emptyMap());
        streamsGroup.setTopology(topology);
        ConfiguredTopology topo = mock(ConfiguredTopology.class);
        when(topo.isReady()).thenReturn(true);

        try (MockedStatic<InternalTopicManager> mocked = mockStatic(InternalTopicManager.class)) {
            mocked.when(() -> InternalTopicManager.configureTopics(any(), eq(topology), eq(partitionMetadata))).thenReturn(topo);
            streamsGroup.setPartitionMetadata(partitionMetadata);
            mocked.verify(() -> InternalTopicManager.configureTopics(any(), eq(topology), eq(partitionMetadata)));
        }

        Optional<ConfiguredTopology> configuredTopology = streamsGroup.configuredTopology();
        assertTrue(configuredTopology.isPresent(), "Configured topology should be present");
        assertEquals(topo, configuredTopology.get());
        assertEquals(partitionMetadata, streamsGroup.partitionMetadata());
        assertEquals(StreamsGroupState.EMPTY, streamsGroup.state());

        streamsGroup.updateMember(new StreamsGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .build());

        assertEquals(StreamsGroupState.RECONCILING, streamsGroup.state());
    }

    @Test
    public void testComputePartitionMetadata() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        StreamsGroup streamsGroup = new StreamsGroup(
            LOG_CONTEXT,
            snapshotRegistry,
            "group-foo",
            mock(GroupCoordinatorMetricsShard.class)
        );
        TopicsImage topicsImage = mock(TopicsImage.class);
        TopicImage topicImage = mock(TopicImage.class);
        when(topicImage.id()).thenReturn(Uuid.randomUuid());
        when(topicImage.name()).thenReturn("topic1");
        when(topicImage.partitions()).thenReturn(Collections.singletonMap(0, null));
        when(topicsImage.getTopic("topic1")).thenReturn(topicImage);
        StreamsTopology topology = mock(StreamsTopology.class);
        when(topology.requiredTopics()).thenReturn(Collections.singleton("topic1"));

        Map<String, TopicMetadata> partitionMetadata = streamsGroup.computePartitionMetadata(topicsImage, topology);

        assertEquals(1, partitionMetadata.size());
        assertTrue(partitionMetadata.containsKey("topic1"));
        TopicMetadata topicMetadata = partitionMetadata.get("topic1");
        assertNotNull(topicMetadata);
        assertEquals(topicImage.id(), topicMetadata.id());
        assertEquals("topic1", topicMetadata.name());
        assertEquals(1, topicMetadata.numPartitions());
    }

    @Test
    void testCreateGroupTombstoneRecords() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        StreamsGroup streamsGroup = new StreamsGroup(
            LOG_CONTEXT,
            snapshotRegistry,
            "test-group",
            mock(GroupCoordinatorMetricsShard.class)
        );
        streamsGroup.updateMember(new StreamsGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .build());
        List<CoordinatorRecord> records = new ArrayList<>();

        streamsGroup.createGroupTombstoneRecords(records);

        assertEquals(7, records.size());
        for (CoordinatorRecord record : records) {
            assertNotNull(record.key());
            assertNull(record.value());
        }
        final Set<ApiMessage> keys = records.stream().map(CoordinatorRecord::key).collect(Collectors.toSet());
        assertTrue(keys.contains(new StreamsGroupMetadataKey().setGroupId("test-group")));
        assertTrue(keys.contains(new StreamsGroupTargetAssignmentMetadataKey().setGroupId("test-group")));
        assertTrue(keys.contains(new StreamsGroupPartitionMetadataKey().setGroupId("test-group")));
        assertTrue(keys.contains(new StreamsGroupTopologyKey().setGroupId("test-group")));
        assertTrue(keys.contains(new StreamsGroupMemberMetadataKey().setGroupId("test-group").setMemberId("member1")));
        assertTrue(keys.contains(new StreamsGroupTargetAssignmentMemberKey().setGroupId("test-group").setMemberId("member1")));
        assertTrue(keys.contains(new StreamsGroupCurrentMemberAssignmentKey().setGroupId("test-group").setMemberId("member1")));
    }

    @Test
    public void testIsSubscribedToTopic() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        GroupCoordinatorMetricsShard metricsShard = mock(GroupCoordinatorMetricsShard.class);
        StreamsGroup streamsGroup = new StreamsGroup(logContext, snapshotRegistry, "test-group", metricsShard);

        assertFalse(streamsGroup.isSubscribedToTopic("test-topic1"));
        assertFalse(streamsGroup.isSubscribedToTopic("test-topic2"));
        assertFalse(streamsGroup.isSubscribedToTopic("non-existent-topic"));

        streamsGroup.setTopology(
            new StreamsTopology(1,
                Map.of("test-subtopology",
                    new StreamsGroupTopologyValue.Subtopology()
                        .setSubtopologyId("test-subtopology")
                        .setSourceTopics(List.of("test-topic1"))
                        .setRepartitionSourceTopics(List.of(new StreamsGroupTopologyValue.TopicInfo().setName("test-topic2")))
                        .setRepartitionSinkTopics(List.of("test-topic2"))
                )
            )
        );

        assertFalse(streamsGroup.isSubscribedToTopic("test-topic1"));
        assertFalse(streamsGroup.isSubscribedToTopic("test-topic2"));
        assertFalse(streamsGroup.isSubscribedToTopic("non-existent-topic"));

        streamsGroup.setPartitionMetadata(
            Map.of(
                "test-topic1", new TopicMetadata(Uuid.randomUuid(), "test-topic1", 1),
                "test-topic2", new TopicMetadata(Uuid.randomUuid(), "test-topic2", 1)
            )
        );

        assertTrue(streamsGroup.isSubscribedToTopic("test-topic1"));
        assertTrue(streamsGroup.isSubscribedToTopic("test-topic2"));
        assertFalse(streamsGroup.isSubscribedToTopic("non-existent-topic"));
    }
}