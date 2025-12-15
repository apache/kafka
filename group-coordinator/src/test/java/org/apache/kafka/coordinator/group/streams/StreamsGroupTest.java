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
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.CommitPartitionValidator;
import org.apache.kafka.coordinator.group.OffsetAndMetadata;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksPerSubtopology;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksPerSubtopologyWithCommonEpoch;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamsGroupTest {

    private static final LogContext LOG_CONTEXT = new LogContext();

    private StreamsGroup createStreamsGroup(String groupId) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        return new StreamsGroup(
            LOG_CONTEXT,
            snapshotRegistry,
            groupId
        );
    }

    @Test
    public void testGetOrCreateUninitializedMember() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember uninitializedMember = new StreamsGroupMember.Builder("member-id").build();
        StreamsGroupMember member = streamsGroup.getOrCreateUninitializedMember("member-id");

        assertEquals(uninitializedMember, member);

        StreamsGroupMember updatedMember = new StreamsGroupMember.Builder(member).setInstanceId("unique-new-id").build();
        streamsGroup.updateMember(updatedMember);

        assertEquals(updatedMember, streamsGroup.getOrCreateUninitializedMember("member-id"));
        assertNotEquals(uninitializedMember, streamsGroup.getOrCreateUninitializedMember("member-id"));
    }

    @Test
    public void testGetOrCreateDefaultMember() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember defaultMember = StreamsGroupMember.Builder.withDefaults("member-id").build();
        StreamsGroupMember member = streamsGroup.getOrCreateDefaultMember("member-id");

        assertEquals(defaultMember, member);

        StreamsGroupMember updatedMember = new StreamsGroupMember.Builder(member).setInstanceId("unique-new-id").build();
        streamsGroup.updateMember(updatedMember);

        assertEquals(updatedMember, streamsGroup.getOrCreateDefaultMember("member-id"));
        assertNotEquals(defaultMember, streamsGroup.getOrCreateDefaultMember("member-id"));
    }

    @Test
    public void testGetMemberOrThrow() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember member;

        // Create a member.
        member = streamsGroup.getOrCreateDefaultMember("member-id");
        assertEquals("member-id", member.memberId());

        // Add member to the group.
        streamsGroup.updateMember(member);

        // Get that member back.
        member = streamsGroup.getMemberOrThrow("member-id");
        assertEquals("member-id", member.memberId());

        assertThrows(UnknownMemberIdException.class, () ->
            streamsGroup.getMemberOrThrow("does-not-exist"));
    }

    @Test
    public void testUpdateMember() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember member;

        member = streamsGroup.getOrCreateDefaultMember("member");

        member = new StreamsGroupMember.Builder(member).build();

        streamsGroup.updateMember(member);

        assertEquals(member, streamsGroup.getMemberOrThrow("member"));
    }

    @Test
    public void testNoStaticMember() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        // Create a new member which is not static
        streamsGroup.getOrCreateDefaultMember("member");
        assertNull(streamsGroup.staticMember("instance-id"));
    }

    @Test
    public void testGetStaticMemberByInstanceId() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember member;

        member = streamsGroup.getOrCreateDefaultMember("member");

        member = new StreamsGroupMember.Builder(member)
            .setInstanceId("instance")
            .build();

        streamsGroup.updateMember(member);

        assertEquals(member, streamsGroup.staticMember("instance"));
        assertEquals(member, streamsGroup.getMemberOrThrow("member"));
        assertEquals(member.memberId(), streamsGroup.staticMemberId("instance"));
    }

    @Test
    public void testRemoveMember() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        StreamsGroupMember member = streamsGroup.getOrCreateDefaultMember("member");
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
                new TasksTupleWithEpochs(
                    mkTasksPerSubtopologyWithCommonEpoch(10, mkTasks(fooSubtopology, 1)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 2)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 3))
                )
            )
            .setTasksPendingRevocation(
                new TasksTupleWithEpochs(
                    mkTasksPerSubtopologyWithCommonEpoch(10, mkTasks(barSubtopology, 4)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 5)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 6))
                )
            )
            .build();

        streamsGroup.updateMember(member);

        assertEquals("process", streamsGroup.currentActiveTaskProcessId(fooSubtopology, 1));
        assertEquals(Set.of("process"),
            streamsGroup.currentStandbyTaskProcessIds(fooSubtopology, 2));
        assertEquals(Set.of("process"),
            streamsGroup.currentWarmupTaskProcessIds(fooSubtopology, 3));
        assertEquals("process", streamsGroup.currentActiveTaskProcessId(barSubtopology, 4));
        assertEquals(Set.of("process"),
            streamsGroup.currentStandbyTaskProcessIds(barSubtopology, 5));
        assertEquals(Set.of("process"),
            streamsGroup.currentWarmupTaskProcessIds(barSubtopology, 6));
        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 7));
        assertEquals(Set.of(),
            streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 8));
        assertEquals(Set.of(),
            streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 9));

        member = new StreamsGroupMember.Builder(member)
            .setProcessId("process1")
            .setAssignedTasks(
                new TasksTupleWithEpochs(
                    mkTasksPerSubtopologyWithCommonEpoch(10, mkTasks(fooSubtopology, 1)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 2)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 3))
                )
            )
            .setTasksPendingRevocation(
                new TasksTupleWithEpochs(
                    mkTasksPerSubtopologyWithCommonEpoch(10, mkTasks(barSubtopology, 4)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 5)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 6))
                )
            )
            .build();

        streamsGroup.updateMember(member);

        assertEquals("process1", streamsGroup.currentActiveTaskProcessId(fooSubtopology, 1));
        assertEquals(Set.of("process1"),
            streamsGroup.currentStandbyTaskProcessIds(fooSubtopology, 2));
        assertEquals(Set.of("process1"),
            streamsGroup.currentWarmupTaskProcessIds(fooSubtopology, 3));
        assertEquals("process1", streamsGroup.currentActiveTaskProcessId(barSubtopology, 4));
        assertEquals(Set.of("process1"),
            streamsGroup.currentStandbyTaskProcessIds(barSubtopology, 5));
        assertEquals(Set.of("process1"),
            streamsGroup.currentWarmupTaskProcessIds(barSubtopology, 6));
        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 7));
        assertEquals(Set.of(),
            streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 8));
        assertEquals(Set.of(),
            streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 9));
    }

    @Test
    public void testUpdatingMemberUpdatesTaskProcessIdWhenPartitionIsReassignedBeforeBeingRevoked() {
        String fooSubtopologyId = "foo-sub";

        StreamsGroup streamsGroup = createStreamsGroup("foo");
        StreamsGroupMember member;

        member = new StreamsGroupMember.Builder("member")
            .setProcessId("process")
            .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
            .setTasksPendingRevocation(
                new TasksTupleWithEpochs(
                    mkTasksPerSubtopologyWithCommonEpoch(10, mkTasks(fooSubtopologyId, 1)),
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
                new TasksTupleWithEpochs(
                    mkTasksPerSubtopologyWithCommonEpoch(10, mkTasks(fooSubtopologyId, 1)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 2)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 3))
                )
            )
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build();

        streamsGroup.updateMember(member);

        assertEquals("process1", streamsGroup.currentActiveTaskProcessId(fooSubtopologyId, 1));
    }

    @Test
    public void testUpdatingMemberUpdatesTaskProcessIdWhenPartitionIsNotReleased() {
        String fooSubtopologyId = "foo-sub";
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        StreamsGroupMember m1 = new StreamsGroupMember.Builder("m1")
            .setProcessId("process1")
            .setAssignedTasks(
                new TasksTupleWithEpochs(
                    mkTasksPerSubtopologyWithCommonEpoch(10, mkTasks(fooSubtopologyId, 1)),
                    Map.of(),
                    Map.of()
                )
            )
            .build();

        streamsGroup.updateMember(m1);

        StreamsGroupMember m2 = new StreamsGroupMember.Builder("m2")
            .setProcessId("process2")
            .setAssignedTasks(
                new TasksTupleWithEpochs(
                    mkTasksPerSubtopologyWithCommonEpoch(9, mkTasks(fooSubtopologyId, 1)),
                    Map.of(),
                    Map.of()
                )
            )
            .build();

        // We allow m2 to acquire foo-1 despite the fact that m1 has ownership because the processId is different.
        streamsGroup.updateMember(m2);

        assertEquals("process2", streamsGroup.currentActiveTaskProcessId(fooSubtopologyId, 1));
    }


    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testRemoveTaskProcessIds(TaskRole taskRole) {
        String fooSubtopologyId = "foo-sub";
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        // Removing should be a no-op when there is no process id set.
        streamsGroup.removeTaskProcessIds(
            TaskAssignmentTestUtil.mkTasksTupleWithCommonEpoch(taskRole, 10, mkTasks(fooSubtopologyId, 1)),
            "process"
        );

        StreamsGroupMember m1 = new StreamsGroupMember.Builder("m1")
            .setProcessId("process")
            .setAssignedTasks(TaskAssignmentTestUtil.mkTasksTupleWithCommonEpoch(taskRole, 10, mkTasks(fooSubtopologyId, 1)))
            .build();

        streamsGroup.updateMember(m1);

        // Removing with incorrect process id should do nothing. 
        // A debug message is logged, no exception is thrown.
        streamsGroup.removeTaskProcessIds(
            TaskAssignmentTestUtil.mkTasksTupleWithCommonEpoch(taskRole, 9, mkTasks(fooSubtopologyId, 1)),
            "process1"
        );
        if (taskRole == TaskRole.ACTIVE) {
            assertEquals("process", streamsGroup.currentActiveTaskProcessId(fooSubtopologyId, 1));
        }
    }

    @Test
    public void testAddTaskProcessIds() {
        String fooSubtopologyId = "foo-sub";
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        streamsGroup.addTaskProcessId(
            new TasksTupleWithEpochs(
                mkTasksPerSubtopologyWithCommonEpoch(10, mkTasks(fooSubtopologyId, 1)),
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 2)),
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 3))
            ),
            "process"
        );

        // We allow replacing with a different process id.
        streamsGroup.addTaskProcessId(
            new TasksTupleWithEpochs(
                mkTasksPerSubtopologyWithCommonEpoch(10, mkTasks(fooSubtopologyId, 1)),
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 2)),
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 3))
            ),
            "process2"
        );

        assertEquals("process2", streamsGroup.currentActiveTaskProcessId(fooSubtopologyId, 1));
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
                new TasksTupleWithEpochs(
                    mkTasksPerSubtopologyWithCommonEpoch(10, mkTasks(fooSubtopology, 1)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 2)),
                    mkTasksPerSubtopology(mkTasks(fooSubtopology, 3))
                )
            )
            .setTasksPendingRevocation(
                new TasksTupleWithEpochs(
                    mkTasksPerSubtopologyWithCommonEpoch(10, mkTasks(barSubtopology, 4)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 5)),
                    mkTasksPerSubtopology(mkTasks(barSubtopology, 6))
                )
            )
            .build();

        streamsGroup.updateMember(member);

        assertEquals("process", streamsGroup.currentActiveTaskProcessId(fooSubtopology, 1));
        assertEquals(Set.of("process"), streamsGroup.currentStandbyTaskProcessIds(fooSubtopology, 2));
        assertEquals(Set.of("process"), streamsGroup.currentWarmupTaskProcessIds(fooSubtopology, 3));
        assertEquals("process", streamsGroup.currentActiveTaskProcessId(barSubtopology, 4));
        assertEquals(Set.of("process"), streamsGroup.currentStandbyTaskProcessIds(barSubtopology, 5));
        assertEquals(Set.of("process"), streamsGroup.currentWarmupTaskProcessIds(barSubtopology, 6));
        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 7));
        assertEquals(Set.of(), streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 8));
        assertEquals(Set.of(), streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 9));

        streamsGroup.removeMember(member.memberId());

        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 1));
        assertEquals(Set.of(), streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 2));
        assertEquals(Set.of(), streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 3));
        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 3));
        assertEquals(Set.of(), streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 4));
        assertEquals(Set.of(), streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 5));
        assertNull(streamsGroup.currentActiveTaskProcessId(zarSubtopology, 7));
        assertEquals(Set.of(), streamsGroup.currentStandbyTaskProcessIds(zarSubtopology, 8));
        assertEquals(Set.of(), streamsGroup.currentWarmupTaskProcessIds(zarSubtopology, 9));
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

        streamsGroup.setTopology(new StreamsTopology(1, Map.of()));
        streamsGroup.setConfiguredTopology(new ConfiguredTopology(1, 0, Optional.of(new TreeMap<>()), Map.of(), Optional.empty()));
        streamsGroup.setValidatedTopologyEpoch(1);

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

        // The member epoch is stale (newer than current).
        if (version >= 9) {
            assertThrows(StaleMemberEpochException.class, () ->
                group.validateOffsetCommit("new-protocol-member-id", "", 10, isTransactional, version));
        } else {
            assertThrows(UnsupportedVersionException.class, () ->
                group.validateOffsetCommit("new-protocol-member-id", "", 10, isTransactional, version));
        }

        // This should succeed (matching member epoch).
        if (version >= 9) {
            group.validateOffsetCommit("new-protocol-member-id", "", 0, isTransactional, version);
        } else {
            assertThrows(UnsupportedVersionException.class, () ->
                group.validateOffsetCommit("new-protocol-member-id", "", 0, isTransactional, version));
        }
    }

    @Test
    public void testValidateOffsetCommitWithOlderEpoch() {
        StreamsGroup group = createStreamsGroup("group-foo");
        
        group.setTopology(new StreamsTopology(1, Map.of("0", new StreamsGroupTopologyValue.Subtopology()
            .setSubtopologyId("0")
            .setSourceTopics(List.of("input-topic")))));
        
        group.updateMember(new StreamsGroupMember.Builder("member-1")
            .setMemberEpoch(2)
            .setAssignedTasks(new TasksTupleWithEpochs(
                Map.of("0", Map.of(0, 2, 1, 1)),
                Map.of(), Map.of()))
            .build());
        
        CommitPartitionValidator validator = group.validateOffsetCommit(
            "member-1", "", 1, false, ApiKeys.OFFSET_COMMIT.latestVersion());
        
        // Received epoch (1) < assignment epoch (2) should throw
        assertThrows(StaleMemberEpochException.class, () ->
            validator.validate("input-topic", Uuid.ZERO_UUID, 0));
    }

    @Test
    public void testValidateOffsetCommitWithOlderEpochMissingTopology() {
        StreamsGroup group = createStreamsGroup("group-foo");
        
        group.updateMember(new StreamsGroupMember.Builder("member-1")
            .setMemberEpoch(2)
            .build());
        
        // Topology is retrieved when creating validator, so exception is thrown here
        assertThrows(StaleMemberEpochException.class, () ->
            group.validateOffsetCommit("member-1", "", 1, false, ApiKeys.OFFSET_COMMIT.latestVersion()));
    }

    @Test
    public void testValidateOffsetCommitWithOlderEpochMissingSubtopology() {
        StreamsGroup group = createStreamsGroup("group-foo");
        
        group.setTopology(new StreamsTopology(1, Map.of("0", new StreamsGroupTopologyValue.Subtopology()
            .setSubtopologyId("0")
            .setSourceTopics(List.of("input-topic")))));
        
        group.updateMember(new StreamsGroupMember.Builder("member-1")
            .setMemberEpoch(2)
            .build());
        
        CommitPartitionValidator validator = group.validateOffsetCommit(
            "member-1", "", 1, false, ApiKeys.OFFSET_COMMIT.latestVersion());
        
        assertThrows(StaleMemberEpochException.class, () ->
            validator.validate("unknown-topic", Uuid.ZERO_UUID, 0));
    }

    @Test
    public void testValidateOffsetCommitWithOlderEpochUnassignedPartition() {
        StreamsGroup group = createStreamsGroup("group-foo");
        
        group.setTopology(new StreamsTopology(1, Map.of("0", new StreamsGroupTopologyValue.Subtopology()
            .setSubtopologyId("0")
            .setSourceTopics(List.of("input-topic")))));
        
        group.updateMember(new StreamsGroupMember.Builder("member-1")
            .setMemberEpoch(2)
            .setAssignedTasks(new TasksTupleWithEpochs(
                Map.of("0", Map.of(0, 1)),
                Map.of(), Map.of()))
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build());
        
        CommitPartitionValidator validator = group.validateOffsetCommit(
            "member-1", "", 1, false, ApiKeys.OFFSET_COMMIT.latestVersion());

        // Partition 1 not assigned should throw
        assertThrows(StaleMemberEpochException.class, () ->
            validator.validate("input-topic", Uuid.ZERO_UUID, 1));
    }

    @Test
    public void testValidateOffsetCommitWithOlderEpochValidAssignment() {
        StreamsGroup group = createStreamsGroup("group-foo");
        
        group.setTopology(new StreamsTopology(1, Map.of("0", new StreamsGroupTopologyValue.Subtopology()
            .setSubtopologyId("0")
            .setSourceTopics(List.of("input-topic")))));
        
        group.updateMember(new StreamsGroupMember.Builder("member-1")
            .setMemberEpoch(5)
            .setAssignedTasks(new TasksTupleWithEpochs(
                Map.of("0", Map.of(0, 2, 1, 2)),
                Map.of(), Map.of()))
            .build());
        
        CommitPartitionValidator validator = group.validateOffsetCommit(
            "member-1", "", 2, false, ApiKeys.OFFSET_COMMIT.latestVersion());
        
        // Received epoch 2 == assignment epoch 2 should succeed
        validator.validate("input-topic", Uuid.ZERO_UUID, 0);
        validator.validate("input-topic", Uuid.ZERO_UUID, 1);
    }

    @Test
    public void testAsListedGroup() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        StreamsGroup group = new StreamsGroup(
            LOG_CONTEXT,
            snapshotRegistry,
            "group-foo"
        );
        group.setGroupEpoch(1);
        group.setTopology(new StreamsTopology(1, Map.of()));
        group.setValidatedTopologyEpoch(1);
        group.setConfiguredTopology(new ConfiguredTopology(1, 0, Optional.of(new TreeMap<>()), Map.of(), Optional.empty()));
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
            "group-foo"
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

        streamsGroup.setTopology(new StreamsTopology(1, Map.of()));
        streamsGroup.setConfiguredTopology(new ConfiguredTopology(1, 0, Optional.of(new TreeMap<>()), Map.of(), Optional.empty()));
        streamsGroup.setValidatedTopologyEpoch(1);

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
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(15000L, OptionalInt.empty(), "", commitTimestamp, OptionalLong.empty(), Uuid.ZERO_UUID);
        StreamsGroup group = new StreamsGroup(LOG_CONTEXT, new SnapshotRegistry(LOG_CONTEXT), "group-id");

        Optional<OffsetExpirationCondition> offsetExpirationCondition = group.offsetExpirationCondition();
        assertTrue(offsetExpirationCondition.isPresent());

        OffsetExpirationConditionImpl condition = (OffsetExpirationConditionImpl) offsetExpirationCondition.get();
        assertEquals(commitTimestamp, condition.baseTimestamp().apply(offsetAndMetadata));
        assertTrue(condition.isOffsetExpired(offsetAndMetadata, currentTimestamp, offsetsRetentionMs));
    }

    @Test
    public void testAsDescribedGroup() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        StreamsGroup group = new StreamsGroup(LOG_CONTEXT, snapshotRegistry, "group-id-1");
        snapshotRegistry.idempotentCreateSnapshot(0);
        assertEquals(StreamsGroup.StreamsGroupState.EMPTY.toString(), group.stateAsString(0));

        group.setGroupEpoch(1);
        group.setTopology(new StreamsTopology(1, Map.of()));
        group.setConfiguredTopology(new ConfiguredTopology(1, 0, Optional.of(new TreeMap<>()), Map.of(), Optional.empty()));
        group.setValidatedTopologyEpoch(1);
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
            .setClientTags(Map.of("tag1", "value1"))
            .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
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
            .setClientTags(Map.of("tag2", "value2"))
            .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build());
        snapshotRegistry.idempotentCreateSnapshot(1);

        StreamsGroupDescribeResponseData.DescribedGroup expected = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId("group-id-1")
            .setGroupState(StreamsGroup.StreamsGroupState.STABLE.toString())
            .setGroupEpoch(1)
            .setTopology(new StreamsGroupDescribeResponseData.Topology().setEpoch(1).setSubtopologies(List.of()))
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
                    .setClientTags(List.of(new StreamsGroupDescribeResponseData.KeyValue().setKey("tag1").setValue("value1")))
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
                    .setClientTags(List.of(new StreamsGroupDescribeResponseData.KeyValue().setKey("tag2").setValue("value2")))
                    .setAssignment(new StreamsGroupDescribeResponseData.Assignment())
                    .setTargetAssignment(new StreamsGroupDescribeResponseData.Assignment())
            ));
        StreamsGroupDescribeResponseData.DescribedGroup actual = group.asDescribedGroup(1);

        assertEquals(expected, actual);
    }

    @Test
    public void testIsInStatesCaseInsensitiveAndUnderscored() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        StreamsGroup group = new StreamsGroup(LOG_CONTEXT, snapshotRegistry, "group-foo");
        snapshotRegistry.idempotentCreateSnapshot(0);
        assertTrue(group.isInStates(Set.of("empty"), 0));
        assertFalse(group.isInStates(Set.of("Empty"), 0));

        group.updateMember(new StreamsGroupMember.Builder("member1")
            .build());
        snapshotRegistry.idempotentCreateSnapshot(1);
        assertTrue(group.isInStates(Set.of("empty"), 0));
        assertTrue(group.isInStates(Set.of("not_ready"), 1));
        assertFalse(group.isInStates(Set.of("empty"), 1));
    }

    @Test
    public void testComputeMetadataHash() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        StreamsGroup streamsGroup = new StreamsGroup(
            LOG_CONTEXT,
            snapshotRegistry,
            "group-foo"
        );

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "topic1", 1)
            .build();

        StreamsTopology topology = mock(StreamsTopology.class);
        when(topology.requiredTopics()).thenReturn(Set.of("topic1"));

        long metadataHash = streamsGroup.computeMetadataHash(new KRaftCoordinatorMetadataImage(metadataImage), new HashMap<>(), topology);
        // The metadata hash means no topic.
        assertNotEquals(0, metadataHash);
    }

    @Test
    void testCreateGroupTombstoneRecords() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(LOG_CONTEXT);
        StreamsGroup streamsGroup = new StreamsGroup(
            LOG_CONTEXT,
            snapshotRegistry,
            "test-group"
        );
        streamsGroup.updateMember(new StreamsGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .build());
        List<CoordinatorRecord> records = new ArrayList<>();

        streamsGroup.createGroupTombstoneRecords(records);

        assertEquals(6, records.size());
        for (CoordinatorRecord record : records) {
            assertNotNull(record.key());
            assertNull(record.value());
        }
        final Set<ApiMessage> keys = records.stream().map(CoordinatorRecord::key).collect(Collectors.toSet());
        assertTrue(keys.contains(new StreamsGroupMetadataKey().setGroupId("test-group")));
        assertTrue(keys.contains(new StreamsGroupTargetAssignmentMetadataKey().setGroupId("test-group")));
        assertTrue(keys.contains(new StreamsGroupTopologyKey().setGroupId("test-group")));
        assertTrue(keys.contains(new StreamsGroupMemberMetadataKey().setGroupId("test-group").setMemberId("member1")));
        assertTrue(keys.contains(new StreamsGroupTargetAssignmentMemberKey().setGroupId("test-group").setMemberId("member1")));
        assertTrue(keys.contains(new StreamsGroupCurrentMemberAssignmentKey().setGroupId("test-group").setMemberId("member1")));
    }

    @Test
    public void testIsSubscribedToTopic() {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        StreamsGroup streamsGroup = new StreamsGroup(logContext, snapshotRegistry, "test-group");

        assertFalse(streamsGroup.isSubscribedToTopic("test-topic1"));
        assertFalse(streamsGroup.isSubscribedToTopic("test-topic2"));
        assertFalse(streamsGroup.isSubscribedToTopic("non-existent-topic"));

        StreamsTopology topology = new StreamsTopology(1,
            Map.of("test-subtopology",
                new StreamsGroupTopologyValue.Subtopology()
                    .setSubtopologyId("test-subtopology")
                    .setSourceTopics(List.of("test-topic1"))
                    .setRepartitionSourceTopics(List.of(new StreamsGroupTopologyValue.TopicInfo().setName("test-topic2")))
                    .setRepartitionSinkTopics(List.of("test-topic2"))
            ));
        streamsGroup.setTopology(topology);

        streamsGroup.updateMember(streamsGroup.getOrCreateDefaultMember("member-id"));
        
        assertTrue(streamsGroup.isSubscribedToTopic("test-topic1"));
        assertTrue(streamsGroup.isSubscribedToTopic("test-topic2"));
        assertFalse(streamsGroup.isSubscribedToTopic("non-existent-topic"));

        streamsGroup.removeMember("member-id");

        assertFalse(streamsGroup.isSubscribedToTopic("test-topic1"));
        assertFalse(streamsGroup.isSubscribedToTopic("test-topic2"));
        assertFalse(streamsGroup.isSubscribedToTopic("non-existent-topic"));
    }

    @Test
    public void testShutdownRequestedMethods() {
        String memberId1 = "test-member-id1";
        String memberId2 = "test-member-id2";
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        StreamsGroup streamsGroup = new StreamsGroup(logContext, snapshotRegistry, "test-group");

        streamsGroup.updateMember(streamsGroup.getOrCreateDefaultMember(memberId1));
        streamsGroup.updateMember(streamsGroup.getOrCreateDefaultMember(memberId2));

        // Initially, shutdown should not be requested
        assertTrue(streamsGroup.getShutdownRequestMemberId().isEmpty());

        // Set shutdown requested
        streamsGroup.setShutdownRequestMemberId(memberId1);
        assertEquals(Optional.of(memberId1), streamsGroup.getShutdownRequestMemberId());

        // Setting shutdown requested again will be ignored
        streamsGroup.setShutdownRequestMemberId(memberId2);
        assertEquals(Optional.of(memberId1), streamsGroup.getShutdownRequestMemberId());

        // As long as group not empty, remain in shutdown requested state
        streamsGroup.removeMember(memberId1);
        assertEquals(Optional.of(memberId1), streamsGroup.getShutdownRequestMemberId());

        // As soon as the group is empty, clear the shutdown requested state
        streamsGroup.removeMember(memberId2);
        assertEquals(Optional.empty(), streamsGroup.getShutdownRequestMemberId());
    }

    @Test
    public void testAsDescribedGroupWithStreamsTopologyHavingSubtopologies() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        StreamsGroup group = new StreamsGroup(LOG_CONTEXT, snapshotRegistry, "group-id-with-topology");
        snapshotRegistry.idempotentCreateSnapshot(0);

        // Create a topology with subtopologies
        Map<String, StreamsGroupTopologyValue.Subtopology> subtopologies = Map.of(
            "sub-1", new StreamsGroupTopologyValue.Subtopology()
                .setSubtopologyId("sub-1")
                .setSourceTopics(List.of("input-topic"))
                .setRepartitionSourceTopics(List.of(
                    new StreamsGroupTopologyValue.TopicInfo().setName("repartition-topic")
                ))
                .setStateChangelogTopics(List.of(
                    new StreamsGroupTopologyValue.TopicInfo().setName("changelog-topic")
                ))
        );

        group.setGroupEpoch(2);
        group.setTopology(new StreamsTopology(2, subtopologies));
        group.setTargetAssignmentEpoch(2);
        group.updateMember(new StreamsGroupMember.Builder("member1")
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .setState(MemberState.STABLE)
            .setInstanceId("instance1")
            .setRackId("rack1")
            .setClientId("client1")
            .setClientHost("host1")
            .setRebalanceTimeoutMs(1000)
            .setTopologyEpoch(2)
            .setProcessId("process1")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host1").setPort(9092))
            .setClientTags(Map.of("tag1", "value1"))
            .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
            .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
            .build());
        snapshotRegistry.idempotentCreateSnapshot(1);

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = group.asDescribedGroup(1);

        assertEquals("group-id-with-topology", describedGroup.groupId());
        assertEquals(StreamsGroup.StreamsGroupState.NOT_READY.toString(), describedGroup.groupState());
        assertEquals(2, describedGroup.groupEpoch());
        assertEquals(2, describedGroup.assignmentEpoch());

        // Verify topology is correctly described
        assertNotNull(describedGroup.topology());
        assertEquals(2, describedGroup.topology().epoch());
        assertEquals(1, describedGroup.topology().subtopologies().size());

        StreamsGroupDescribeResponseData.Subtopology subtopology = describedGroup.topology().subtopologies().get(0);
        assertEquals("sub-1", subtopology.subtopologyId());
        assertEquals(List.of("input-topic"), subtopology.sourceTopics());
        assertEquals(1, subtopology.repartitionSourceTopics().size());
        assertEquals("repartition-topic", subtopology.repartitionSourceTopics().get(0).name());
        assertEquals(1, subtopology.stateChangelogTopics().size());
        assertEquals("changelog-topic", subtopology.stateChangelogTopics().get(0).name());

        assertEquals(1, describedGroup.members().size());
        assertEquals("member1", describedGroup.members().get(0).memberId());
    }

    @Test
    public void testAsDescribedGroupPrefersConfiguredTopologyOverStreamsTopology() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        StreamsGroup group = new StreamsGroup(LOG_CONTEXT, snapshotRegistry, "group-id-configured");
        snapshotRegistry.idempotentCreateSnapshot(0);

        // Create both StreamsTopology and ConfiguredTopology
        Map<String, StreamsGroupTopologyValue.Subtopology> subtopologies = Map.of(
            "sub-1", new StreamsGroupTopologyValue.Subtopology()
                .setSubtopologyId("sub-1")
                .setSourceTopics(List.of("streams-topic"))
        );

        group.setGroupEpoch(3);
        group.setTopology(new StreamsTopology(2, subtopologies));
        group.setConfiguredTopology(new ConfiguredTopology(3, 0, Optional.of(new TreeMap<>()), Map.of(), Optional.empty()));
        group.setTargetAssignmentEpoch(3);
        snapshotRegistry.idempotentCreateSnapshot(1);

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = group.asDescribedGroup(1);

        // Should prefer ConfiguredTopology over StreamsTopology
        assertNotNull(describedGroup.topology());
        assertEquals(3, describedGroup.topology().epoch()); // ConfiguredTopology epoch
        assertEquals(0, describedGroup.topology().subtopologies().size()); // Empty configured topology
    }

    @Test
    public void testAsDescribedGroupFallbackToStreamsTopologyWhenConfiguredTopologyEmpty() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        StreamsGroup group = new StreamsGroup(LOG_CONTEXT, snapshotRegistry, "group-id-fallback");
        snapshotRegistry.idempotentCreateSnapshot(0);

        // Create StreamsTopology with subtopologies
        Map<String, StreamsGroupTopologyValue.Subtopology> subtopologies = Map.of(
            "sub-1", new StreamsGroupTopologyValue.Subtopology()
                .setSubtopologyId("sub-1")
                .setSourceTopics(List.of("fallback-topic"))
        );

        group.setGroupEpoch(4);
        group.setTopology(new StreamsTopology(4, subtopologies));
        // No ConfiguredTopology set, so should fallback to StreamsTopology
        group.setTargetAssignmentEpoch(4);
        snapshotRegistry.idempotentCreateSnapshot(1);

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = group.asDescribedGroup(1);

        // Should use StreamsTopology when ConfiguredTopology is not available
        assertNotNull(describedGroup.topology());
        assertEquals(4, describedGroup.topology().epoch()); // StreamsTopology epoch
        assertEquals(1, describedGroup.topology().subtopologies().size());
        assertEquals("sub-1", describedGroup.topology().subtopologies().get(0).subtopologyId());
        assertEquals(List.of("fallback-topic"), describedGroup.topology().subtopologies().get(0).sourceTopics());
    }

    @Test
    public void testCancelTimers() {
        StreamsGroup streamsGroup = createStreamsGroup("test-group");
        CoordinatorTimer<Void, CoordinatorRecord> timer = mock(CoordinatorTimer.class);

        streamsGroup.cancelTimers(timer);

        verify(timer).cancel("initial-rebalance-timeout-test-group");
    }
}
