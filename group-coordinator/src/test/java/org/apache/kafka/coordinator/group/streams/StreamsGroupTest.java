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
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.coordinator.group.OffsetAndMetadata;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksPerSubtopology;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class StreamsGroupTest {

    private StreamsGroup createStreamsGroup(String groupId) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        return new StreamsGroup(
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
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(fooSubtopology, 1)))
            .setAssignedStandbyTasks(mkTasksPerSubtopology(mkTasks(fooSubtopology, 2)))
            .setAssignedWarmupTasks(mkTasksPerSubtopology(mkTasks(fooSubtopology, 3)))
            .setActiveTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(barSubtopology, 4)))
            .setStandbyTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(barSubtopology, 5)))
            .setWarmupTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(barSubtopology, 6)))
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
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(fooSubtopology, 1)))
            .setAssignedStandbyTasks(mkTasksPerSubtopology(mkTasks(fooSubtopology, 2)))
            .setAssignedWarmupTasks(mkTasksPerSubtopology(mkTasks(fooSubtopology, 3)))
            .setActiveTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(barSubtopology, 4)))
            .setStandbyTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(barSubtopology, 5)))
            .setWarmupTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(barSubtopology, 6)))
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
            .setAssignedActiveTasks(emptyMap())
            .setAssignedStandbyTasks(emptyMap())
            .setAssignedWarmupTasks(emptyMap())
            .setActiveTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)))
            .setStandbyTasksPendingRevocation(
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 2)))
            .setWarmupTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 3)))
            .build();

        streamsGroup.updateMember(member);

        assertEquals("process", streamsGroup.currentActiveTaskProcessId(fooSubtopologyId, 1));

        member = new StreamsGroupMember.Builder(member)
            .setProcessId("process1")
            .setAssignedActiveTasks(
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)))
            .setAssignedStandbyTasks(
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 2)))
            .setAssignedWarmupTasks(
                mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 3)))
            .setActiveTasksPendingRevocation(emptyMap())
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
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)))
            .setAssignedStandbyTasks(emptyMap())
            .setAssignedWarmupTasks(emptyMap())
            .build();

        streamsGroup.updateMember(m1);

        StreamsGroupMember m2 = new StreamsGroupMember.Builder("m2")
            .setProcessId("process")
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)))
            .setAssignedStandbyTasks(emptyMap())
            .setAssignedWarmupTasks(emptyMap())
            .build();

        // m2 should not be able to acquire foo-1 because the partition is
        // still owned by another member.
        assertThrows(IllegalStateException.class, () -> streamsGroup.updateMember(m2));
    }

    @Test
    public void testRemoveActiveTaskProcessIds() {
        String fooSubtopologyId = "foo-sub";
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        // Removing should fail because there is no epoch set.
        assertThrows(IllegalStateException.class, () -> streamsGroup.removeActiveTaskProcessIds(
            mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)),
            "process"
        ));

        StreamsGroupMember m1 = new StreamsGroupMember.Builder("m1")
            .setProcessId("process")
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)))
            .build();

        streamsGroup.updateMember(m1);

        // Removing should fail because the expected epoch is incorrect.
        assertThrows(IllegalStateException.class, () -> streamsGroup.removeActiveTaskProcessIds(
            mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)),
            "process1"
        ));
    }

    @Test
    public void testAddTaskProcessIds() {
        String fooSubtopologyId = "foo-sub";
        StreamsGroup streamsGroup = createStreamsGroup("foo");

        streamsGroup.addTaskProcessId(
            mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)),
            mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 2)),
            mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 3)),
            "process"
        );

        // Changing the epoch should fail because the owner of the partition
        // should remove it first.
        assertThrows(IllegalStateException.class, () -> streamsGroup.addTaskProcessId(
            mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 1)),
            mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 2)),
            mkTasksPerSubtopology(mkTasks(fooSubtopologyId, 3)),
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
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(fooSubtopology, 1)))
            .setAssignedStandbyTasks(mkTasksPerSubtopology(mkTasks(fooSubtopology, 2)))
            .setAssignedWarmupTasks(mkTasksPerSubtopology(mkTasks(fooSubtopology, 3)))
            .setActiveTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(barSubtopology, 4)))
            .setStandbyTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(barSubtopology, 5)))
            .setWarmupTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(barSubtopology, 6)))
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
    public void testWaitingOnUnreleasedActiveTask() {
        String fooSubtopology = "foo-sub";
        String barSubtopology = "bar-sub";
        String zarSubtopology = "zar-sub";
        String memberId1 = "m1";
        String memberId2 = "m2";

        StreamsGroup streamsGroup = createStreamsGroup("foo");
        streamsGroup.updateTargetAssignment(memberId1,
            new Assignment(
                mkTasksPerSubtopology(
                    mkTasks(fooSubtopology, 1, 2, 3),
                    mkTasks(zarSubtopology, 7, 8, 9)
                ),
                emptyMap(),
                emptyMap())
        );

        StreamsGroupMember member1 = new StreamsGroupMember.Builder(memberId1)
            .setMemberEpoch(10)
            .setProcessId("process")
            .setState(MemberState.UNRELEASED_TASKS)
            .setAssignedActiveTasks(mkTasksPerSubtopology(
                mkTasks(fooSubtopology, 1, 2, 3)
            ))
            .setActiveTasksPendingRevocation(mkTasksPerSubtopology(
                mkTasks(barSubtopology, 4, 5, 6)
            ))
            .build();
        streamsGroup.updateMember(member1);

        assertFalse(streamsGroup.waitingOnUnreleasedActiveTasks(member1));

        StreamsGroupMember member2 = new StreamsGroupMember.Builder(memberId2)
            .setMemberEpoch(10)
            .setProcessId("process")
            .setActiveTasksPendingRevocation(mkTasksPerSubtopology(mkTasks(zarSubtopology, 7)))
            .build();
        streamsGroup.updateMember(member2);

        assertTrue(streamsGroup.waitingOnUnreleasedActiveTasks(member1));
    }

    @Test
    public void testGroupState() {
        StreamsGroup streamsGroup = createStreamsGroup("foo");
        assertEquals(StreamsGroup.StreamsGroupState.INITIALIZING, streamsGroup.state());

        StreamsGroupMember member1 = new StreamsGroupMember.Builder("member1")
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .build();

        streamsGroup.updateMember(member1);
        streamsGroup.setGroupEpoch(1);

        assertEquals(MemberState.STABLE, member1.state());
        assertEquals(StreamsGroup.StreamsGroupState.INITIALIZING, streamsGroup.state());

        streamsGroup.setTopology(new StreamsTopology("topology-id", Collections.emptyMap()));

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
    public void testUpdateInvertedAssignment() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        GroupCoordinatorMetricsShard metricsShard = mock(GroupCoordinatorMetricsShard.class);
        StreamsGroup streamsGroup = new StreamsGroup(snapshotRegistry, "test-group", metricsShard);
        String subtopologyId = "foo-sub";
        String memberId1 = "member1";
        String memberId2 = "member2";

        // Initial assignment for member1
        Assignment initialAssignment = new Assignment(
            mkTasksPerSubtopology(mkTasks(subtopologyId, 0)),
            emptyMap(),
            emptyMap()
        );
        streamsGroup.updateTargetAssignment(memberId1, initialAssignment);

        // Verify that partition 0 is assigned to member1.
        assertEquals(
            mkMap(
                mkEntry(subtopologyId, mkMap(mkEntry(0, memberId1)))
            ),
            streamsGroup.invertedTargetActiveTasksAssignment()
        );

        // New assignment for member1
        Assignment newAssignment = new Assignment(
            mkTasksPerSubtopology(mkTasks(subtopologyId, 1)),
            emptyMap(),
            emptyMap()
        );
        streamsGroup.updateTargetAssignment(memberId1, newAssignment);

        // Verify that partition 0 is no longer assigned and partition 1 is assigned to member1
        assertEquals(
            mkMap(
                mkEntry(subtopologyId, mkMap(mkEntry(1, memberId1)))
            ),
            streamsGroup.invertedTargetActiveTasksAssignment()
        );

        // New assignment for member2 to add partition 1
        Assignment newAssignment2 = new Assignment(
            mkTasksPerSubtopology(mkTasks(subtopologyId, 1)),
            emptyMap(),
            emptyMap()
        );
        streamsGroup.updateTargetAssignment(memberId2, newAssignment);

        // Verify that partition 1 is assigned to member2
        assertEquals(
            mkMap(
                mkEntry(subtopologyId, mkMap(mkEntry(1, memberId2)))
            ),
            streamsGroup.invertedTargetActiveTasksAssignment()
        );

        // New assignment for member1 to revoke partition 1 and assign partition 0
        Assignment newAssignment1 = new Assignment(
            mkTasksPerSubtopology(mkTasks(subtopologyId, 0)),
            emptyMap(),
            emptyMap()
        );
        streamsGroup.updateTargetAssignment(memberId1, newAssignment1);

        // Verify that partition 1 is still assigned to member2 and partition 0 is assigned to member1
        assertEquals(
            mkMap(
                mkEntry(subtopologyId, mkMap(
                    mkEntry(0, memberId1),
                    mkEntry(1, memberId2)
                ))
            ),
            streamsGroup.invertedTargetActiveTasksAssignment()
        );

        // Test remove target assignment for member1
        streamsGroup.removeTargetAssignment(memberId1);

        // Verify that partition 0 is no longer assigned and partition 1 is still assigned to member2
        assertEquals(
            mkMap(
                mkEntry(subtopologyId, mkMap(mkEntry(1, memberId2)))
            ),
            streamsGroup.invertedTargetActiveTasksAssignment()
        );
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
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testValidateOffsetCommit(short version) {
        boolean isTransactional = false;
        StreamsGroup group = createStreamsGroup("group-foo");

        // Simulate a call from the admin client without member id and member epoch.
        // This should pass only if the group is empty.
        group.validateOffsetCommit("", "", -1, isTransactional, version);

        // The member does not exist.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("member-id", null, 0, isTransactional, version));

        // Create a member.
        group.updateMember(new StreamsGroupMember.Builder("member-id").build());

        // A call from the admin client should fail as the group is not empty.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("", "", -1, isTransactional, version));

        // The member epoch is stale.
        assertThrows(StaleMemberEpochException.class, () ->
            group.validateOffsetCommit("member-id", "", 10, isTransactional, version));

        // This should succeed.
        group.validateOffsetCommit("member-id", "", 0, isTransactional, version);
    }

    @Test
    public void testValidateOffsetFetch() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        StreamsGroup group = new StreamsGroup(
            snapshotRegistry,
            "group-foo",
            mock(GroupCoordinatorMetricsShard.class)
        );

        // Simulate a call from the admin client without member id and member epoch.
        group.validateOffsetFetch(null, -1, Long.MAX_VALUE);

        // The member does not exist.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetFetch("member-id", 0, Long.MAX_VALUE));

        // Create a member.
        snapshotRegistry.idempotentCreateSnapshot(0);
        group.updateMember(new StreamsGroupMember.Builder("member-id").build());

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

        assertEquals(StreamsGroup.StreamsGroupState.INITIALIZING, streamsGroup.state());
        assertDoesNotThrow(streamsGroup::validateDeleteGroup);

        StreamsGroupMember member1 = new StreamsGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .build();
        streamsGroup.updateMember(member1);
        streamsGroup.setTopology(new StreamsTopology("topology-id", Collections.emptyMap()));

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
        StreamsGroup group = new StreamsGroup(new SnapshotRegistry(new LogContext()), "group-id", mock(GroupCoordinatorMetricsShard.class));

        Optional<OffsetExpirationCondition> offsetExpirationCondition = group.offsetExpirationCondition();
        assertTrue(offsetExpirationCondition.isPresent());

        OffsetExpirationConditionImpl condition = (OffsetExpirationConditionImpl) offsetExpirationCondition.get();
        assertEquals(commitTimestamp, condition.baseTimestamp().apply(offsetAndMetadata));
        assertTrue(condition.isOffsetExpired(offsetAndMetadata, currentTimestamp, offsetsRetentionMs));
    }

    @Test
    public void testIsInStatesCaseInsensitive() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        GroupCoordinatorMetricsShard metricsShard = new GroupCoordinatorMetricsShard(
            snapshotRegistry,
            emptyMap(),
            new TopicPartition("__consumer_offsets", 0)
        );
        StreamsGroup group = new StreamsGroup(snapshotRegistry, "group-foo", metricsShard);
        snapshotRegistry.idempotentCreateSnapshot(0);
        assertTrue(group.isInStates(Collections.singleton("initializing"), 0));
        assertFalse(group.isInStates(Collections.singleton("Initializing"), 0));

        group.setTopology(new StreamsTopology("topology-id", Collections.emptyMap()));
        snapshotRegistry.idempotentCreateSnapshot(1);
        assertTrue(group.isInStates(Collections.singleton("initializing"), 0));
        assertTrue(group.isInStates(Collections.singleton("empty"), 1));
        assertFalse(group.isInStates(Collections.singleton("initializing"), 1));
    }
}