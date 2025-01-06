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
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue.TaskIds;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue.KeyValue;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksPerSubtopology;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamsGroupMemberTest {

    @Test
    public void testBuilderWithMemberIdIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember.Builder((String) null).build()
        );
        assertEquals("memberId cannot be null", exception.getMessage());
    }

    @Test
    public void testBuilderWithMemberIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember.Builder((StreamsGroupMember) null).build()
        );
        assertEquals("member cannot be null", exception.getMessage());
    }

    @Test
    public void testBuilderWithDefaults() {
        final String memberId = Uuid.randomUuid().toString();
        StreamsGroupMember member = new StreamsGroupMember.Builder(memberId).build();

        assertEquals(memberId, member.memberId());
        assertEquals(0, member.memberEpoch());
        assertEquals(-1, member.previousMemberEpoch());
        assertEquals(MemberState.STABLE, member.state());
        assertTrue(member.instanceId().isEmpty());
        assertTrue(member.rackId().isEmpty());
        assertEquals(-1, member.rebalanceTimeoutMs());
        assertEquals("", member.clientId());
        assertEquals("", member.clientHost());
        assertEquals(-1, member.topologyEpoch());
        assertTrue(member.processId().isEmpty());
        assertTrue(member.userEndpoint().isEmpty());
        assertEquals(Collections.emptyMap(), member.clientTags());
        assertEquals(Collections.emptyMap(), member.assignedActiveTasks());
        assertEquals(Collections.emptyMap(), member.assignedStandbyTasks());
        assertEquals(Collections.emptyMap(), member.assignedWarmupTasks());
        assertEquals(Collections.emptyMap(), member.activeTasksPendingRevocation());
        assertEquals(Collections.emptyMap(), member.standbyTasksPendingRevocation());
        assertEquals(Collections.emptyMap(), member.warmupTasksPendingRevocation());
    }

    @Test
    public void testBuilderNewMember() {
        final String memberId = "member-id";
        final int memberEpoch = 10;
        final int previousMemberEpoch = 9;
        final MemberState state = MemberState.UNRELEASED_TASKS;
        final String instanceId = "instance-id";
        final String rackId = "rack-id";
        final int rebalanceTimeout = 5000;
        final String clientId = "client-id";
        final String hostname = "hostname";
        final int topologyEpoch = 3;
        final String processId = "process-id";
        final String subtopology1 = "subtopology1";
        final String subtopology2 = "subtopology2";
        final StreamsGroupMemberMetadataValue.Endpoint userEndpoint =
            new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090);
        final Map<String, String> clientTags = mkMap(mkEntry("client", "tag"));
        final Map<String, Set<Integer>> assignedActiveTasks = mkTasksPerSubtopology(mkTasks(subtopology1, 1, 2, 3));
        final Map<String, Set<Integer>> assignedStandbyTasks = mkTasksPerSubtopology(mkTasks(subtopology2, 6, 5, 4));
        final Map<String, Set<Integer>> assignedWarmupTasks = mkTasksPerSubtopology(mkTasks(subtopology1, 7, 8, 9));
        final Map<String, Set<Integer>> activeTasksPendingRevocation = mkTasksPerSubtopology(mkTasks(subtopology2, 3, 2, 1));
        final Map<String, Set<Integer>> standbyTasksPendingRevocation = mkTasksPerSubtopology(mkTasks(subtopology1, 4, 5, 6));
        final Map<String, Set<Integer>> warmupTasksPendingRevocation = mkTasksPerSubtopology(mkTasks(subtopology2, 9, 8, 7));
        StreamsGroupMember member = new StreamsGroupMember.Builder(memberId)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(previousMemberEpoch)
            .setState(state)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setRebalanceTimeoutMs(rebalanceTimeout)
            .setClientId(clientId)
            .setClientHost(hostname)
            .setTopologyEpoch(topologyEpoch)
            .setProcessId(processId)
            .setUserEndpoint(userEndpoint)
            .setClientTags(clientTags)
            .setAssignedActiveTasks(assignedActiveTasks)
            .setAssignedStandbyTasks(assignedStandbyTasks)
            .setAssignedWarmupTasks(assignedWarmupTasks)
            .setActiveTasksPendingRevocation(activeTasksPendingRevocation)
            .setStandbyTasksPendingRevocation(standbyTasksPendingRevocation)
            .setWarmupTasksPendingRevocation(warmupTasksPendingRevocation)
            .build();

        assertEquals(memberId, member.memberId());
        assertEquals(memberEpoch, member.memberEpoch());
        assertEquals(previousMemberEpoch, member.previousMemberEpoch());
        assertEquals(state, member.state());
        assertEquals(Optional.of(instanceId), member.instanceId());
        assertEquals(Optional.of(rackId), member.rackId());
        assertEquals(clientId, member.clientId());
        assertEquals(hostname, member.clientHost());
        assertEquals(topologyEpoch, member.topologyEpoch());
        assertEquals(processId, member.processId());
        assertEquals(Optional.of(userEndpoint), member.userEndpoint());
        assertEquals(clientTags, member.clientTags());
        assertEquals(assignedActiveTasks, member.assignedActiveTasks());
        assertEquals(assignedStandbyTasks, member.assignedStandbyTasks());
        assertEquals(assignedWarmupTasks, member.assignedWarmupTasks());
        assertEquals(activeTasksPendingRevocation, member.activeTasksPendingRevocation());
        assertEquals(standbyTasksPendingRevocation, member.standbyTasksPendingRevocation());
        assertEquals(warmupTasksPendingRevocation, member.warmupTasksPendingRevocation());
    }

    @Test
    public void testBuilderUpdateWithStreamsGroupMemberMetadataValue() {
        StreamsGroupMemberMetadataValue record = new StreamsGroupMemberMetadataValue()
            .setClientId("client-id")
            .setClientHost("host-id")
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(1000)
            .setTopologyEpoch(3)
            .setProcessId("process-id")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
            .setClientTags(Collections.singletonList(new KeyValue().setKey("client").setValue("tag")));

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals(record.clientId(), member.clientId());
        assertEquals(record.clientHost(), member.clientHost());
        assertEquals(Optional.of(record.instanceId()), member.instanceId());
        assertEquals(Optional.of(record.rackId()), member.rackId());
        assertEquals(record.rebalanceTimeoutMs(), member.rebalanceTimeoutMs());
        assertEquals(record.topologyEpoch(), member.topologyEpoch());
        assertEquals(record.processId(), member.processId());
        assertEquals(Optional.of(record.userEndpoint()), member.userEndpoint());
        assertEquals(
            record.clientTags().stream().collect(Collectors.toMap(KeyValue::key, KeyValue::value)),
            member.clientTags()
        );
        assertEquals("member-id", member.memberId());
        assertEquals(0, member.memberEpoch());
        assertEquals(-1, member.previousMemberEpoch());
        assertEquals(MemberState.STABLE, member.state());
        assertEquals(Collections.emptyMap(), member.assignedActiveTasks());
        assertEquals(Collections.emptyMap(), member.assignedStandbyTasks());
        assertEquals(Collections.emptyMap(), member.assignedWarmupTasks());
        assertEquals(Collections.emptyMap(), member.activeTasksPendingRevocation());
        assertEquals(Collections.emptyMap(), member.standbyTasksPendingRevocation());
        assertEquals(Collections.emptyMap(), member.warmupTasksPendingRevocation());
    }

    @Test
    public void testBuilderUpdateWithConsumerGroupCurrentMemberAssignmentValue() {
        final String subtopology1 = "subtopology-id1";
        final String subtopology2 = "subtopology-id2";
        final List<Integer> partitions1 = Arrays.asList(1, 2);
        final List<Integer> partitions2 = Arrays.asList(3, 4);
        final List<Integer> partitions3 = Arrays.asList(5, 6);
        final List<Integer> partitions4 = Arrays.asList(7, 8);
        final List<Integer> partitions5 = Arrays.asList(9, 10);
        final List<Integer> partitions6 = Arrays.asList(11, 12);

        StreamsGroupCurrentMemberAssignmentValue record = new StreamsGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setState((byte) 2)
            .setActiveTasks(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology1)
                .setPartitions(partitions1))
            )
            .setStandbyTasks(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology2)
                .setPartitions(partitions2))
            )
            .setWarmupTasks(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology1)
                .setPartitions(partitions3))
            )
            .setActiveTasksPendingRevocation(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology2)
                .setPartitions(partitions4))
            )
            .setStandbyTasksPendingRevocation(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology1)
                .setPartitions(partitions5))
            )
            .setWarmupTasksPendingRevocation(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology2)
                .setPartitions(partitions6))
            );

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals(record.memberEpoch(), member.memberEpoch());
        assertEquals(record.previousMemberEpoch(), member.previousMemberEpoch());
        assertEquals(MemberState.fromValue(record.state()), member.state());
        assertEquals(
            Map.of(subtopology1, new HashSet<>(partitions1)),
            member.assignedActiveTasks()
        );
        assertEquals(
            Map.of(subtopology2, new HashSet<>(partitions2)),
            member.assignedStandbyTasks()
        );
        assertEquals(
            Map.of(subtopology1, new HashSet<>(partitions3)),
            member.assignedWarmupTasks()
        );
        assertEquals(
            Map.of(subtopology2, new HashSet<>(partitions4)),
            member.activeTasksPendingRevocation()
        );
        assertEquals(
            Map.of(subtopology1, new HashSet<>(partitions5)),
            member.standbyTasksPendingRevocation()
        );
        assertEquals(
            Map.of(subtopology2, new HashSet<>(partitions6)),
            member.warmupTasksPendingRevocation()
        );
        assertEquals("member-id", member.memberId());
        assertTrue(member.instanceId().isEmpty());
        assertTrue(member.rackId().isEmpty());
        assertEquals(-1, member.rebalanceTimeoutMs());
        assertEquals("", member.clientId());
        assertEquals("", member.clientHost());
        assertEquals(-1, member.topologyEpoch());
        assertTrue(member.processId().isEmpty());
        assertTrue(member.userEndpoint().isEmpty());
        assertEquals(Collections.emptyMap(), member.clientTags());
    }

    @Test
    public void testBuilderMaybeUpdateMember() {
        final String subtopology1 = "subtopology-id1";
        final String subtopology2 = "subtopology-id2";

        final StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setTopologyEpoch(3)
            .setProcessId("process-id")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(subtopology1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkTasksPerSubtopology(mkTasks(subtopology2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkTasksPerSubtopology(mkTasks(subtopology1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(
                mkTasksPerSubtopology(mkTasks(subtopology2, 3, 2, 1)))
            .build();

        // This is a no-op.
        StreamsGroupMember updatedMember = new StreamsGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.empty())
            .maybeUpdateInstanceId(Optional.empty())
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.empty())
            .maybeUpdateProcessId(Optional.empty())
            .maybeUpdateTopologyEpoch(OptionalInt.empty())
            .maybeUpdateUserEndpoint(Optional.empty())
            .maybeUpdateClientTags(Optional.empty())
            .build();

        assertEquals(member, updatedMember);

        final String newRackId = "new" + member.rackId();
        final String newInstanceId = "new" + member.instanceId();
        final long newRebalanceTimeout = member.rebalanceTimeoutMs() + 1000;
        final String newProcessId = "new" + member.processId();
        final int newTopologyEpoch = member.topologyEpoch() + 1;
        final StreamsGroupMemberMetadataValue.Endpoint newUserEndpoint =
            new StreamsGroupMemberMetadataValue.Endpoint().setHost(member.userEndpoint().get().host() + "2").setPort(9090);
        final Map<String, String> newClientTags = new HashMap<>(member.clientTags());
        newClientTags.put("client2", "tag2");

        updatedMember = new StreamsGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.of(newRackId))
            .maybeUpdateInstanceId(Optional.of(newInstanceId))
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.of(6000))
            .maybeUpdateProcessId(Optional.of(newProcessId))
            .maybeUpdateTopologyEpoch(OptionalInt.of(newTopologyEpoch))
            .maybeUpdateUserEndpoint(Optional.of(newUserEndpoint))
            .maybeUpdateClientTags(Optional.of(newClientTags))
            .build();

        assertEquals(Optional.of(newRackId), updatedMember.rackId());
        assertEquals(Optional.of(newInstanceId), updatedMember.instanceId());
        assertEquals(newRebalanceTimeout, updatedMember.rebalanceTimeoutMs());
        assertEquals(newProcessId, updatedMember.processId());
        assertEquals(newTopologyEpoch, updatedMember.topologyEpoch());
        assertEquals(Optional.of(newUserEndpoint), updatedMember.userEndpoint());
        assertEquals(newClientTags, updatedMember.clientTags());
        assertEquals(member.memberId(), updatedMember.memberId());
        assertEquals(member.memberEpoch(), updatedMember.memberEpoch());
        assertEquals(member.previousMemberEpoch(), updatedMember.previousMemberEpoch());
        assertEquals(member.state(), updatedMember.state());
        assertEquals(member.clientId(), updatedMember.clientId());
        assertEquals(member.clientHost(), updatedMember.clientHost());
        assertEquals(member.assignedActiveTasks(), updatedMember.assignedActiveTasks());
        assertEquals(member.assignedStandbyTasks(), updatedMember.assignedStandbyTasks());
        assertEquals(member.assignedWarmupTasks(), updatedMember.assignedWarmupTasks());
        assertEquals(member.activeTasksPendingRevocation(), updatedMember.activeTasksPendingRevocation());
        assertEquals(member.standbyTasksPendingRevocation(), updatedMember.standbyTasksPendingRevocation());
        assertEquals(member.warmupTasksPendingRevocation(), updatedMember.warmupTasksPendingRevocation());
    }

    @Test
    public void testBuilderUpdateMemberEpoch() {
        final StreamsGroupMember member = new StreamsGroupMember.Builder("member-id").build();

        final int newMemberEpoch = member.memberEpoch() + 1;
        final StreamsGroupMember updatedMember = new StreamsGroupMember.Builder(member)
            .updateMemberEpoch(newMemberEpoch)
            .build();

        assertEquals(member.memberId(), updatedMember.memberId());
        assertEquals(newMemberEpoch, updatedMember.memberEpoch());
        // The previous member epoch becomes the old current member epoch.
        assertEquals(member.memberEpoch(), updatedMember.previousMemberEpoch());
        assertEquals(member.state(), updatedMember.state());
        assertEquals(member.instanceId(), updatedMember.instanceId());
        assertEquals(member.rackId(), updatedMember.rackId());
        assertEquals(member.rebalanceTimeoutMs(), updatedMember.rebalanceTimeoutMs());
        assertEquals(member.clientId(), updatedMember.clientId());
        assertEquals(member.clientHost(), updatedMember.clientHost());
        assertEquals(member.topologyEpoch(), updatedMember.topologyEpoch());
        assertTrue(member.processId().isEmpty());
        assertTrue(member.userEndpoint().isEmpty());
        assertEquals(member.clientTags(), updatedMember.clientTags());
        assertEquals(member.assignedActiveTasks(), updatedMember.assignedActiveTasks());
        assertEquals(member.assignedStandbyTasks(), updatedMember.assignedStandbyTasks());
        assertEquals(member.assignedWarmupTasks(), updatedMember.assignedWarmupTasks());
        assertEquals(member.activeTasksPendingRevocation(), updatedMember.activeTasksPendingRevocation());
        assertEquals(member.standbyTasksPendingRevocation(), updatedMember.standbyTasksPendingRevocation());
        assertEquals(member.warmupTasksPendingRevocation(), updatedMember.warmupTasksPendingRevocation());
    }

    @Test
    public void testConstructorWithMemberIdIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                null,
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("memberId cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithMemberStateIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                null,
                Optional.empty(),
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("state cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithInstanceIdIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                null,
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("instanceId cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithRackIdIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                null,
                "",
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("rackId cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithClientIdIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                null,
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("clientId cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithClientHostIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                "",
                null,
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("clientHost cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithProcessIdIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                null,
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("processId cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithUserEndpointIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                "",
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("userEndpoint cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithClientTagsIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("clientTags cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithAssignedActiveTasksIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("assignedActiveTasks cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithAssignedStandbyTasksIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("assignedStandbyTasks cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithAssignedWarmupTasksIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("assignedWarmupTasks cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithActiveTasksPendingRevocationIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                null,
                Collections.emptyMap(),
                Collections.emptyMap()
            )
        );
        assertEquals("activeTasksPendingRevocation cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithStandbyTasksPendingRevocationIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                null,
                Collections.emptyMap()
            )
        );
        assertEquals("standbyTasksPendingRevocation cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorWithWarmupTasksPendingRevocationIsNull() {
        final Exception exception = assertThrows(
            NullPointerException.class,
            () -> new StreamsGroupMember(
                "",
                0,
                -1,
                MemberState.STABLE,
                Optional.empty(),
                Optional.empty(),
                "",
                "",
                -1,
                -1,
                "",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                null
            )
        );
        assertEquals("warmupTasksPendingRevocation cannot be null", exception.getMessage());
    }

    @Test
    public void testReturnUnmodifiableFields() {
        final StreamsGroupMember member = new StreamsGroupMember(
            "",
            0,
            -1,
            MemberState.STABLE,
            Optional.empty(),
            Optional.empty(),
            "",
            "",
            -1,
            -1,
            "",
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertThrows(UnsupportedOperationException.class, () -> member.clientTags().put("not allowed", ""));
        assertThrows(UnsupportedOperationException.class, () -> member.assignedActiveTasks().put("not allowed", Collections.emptySet()));
        assertThrows(UnsupportedOperationException.class, () -> member.assignedStandbyTasks().put("not allowed", Collections.emptySet()));
        assertThrows(UnsupportedOperationException.class, () -> member.assignedWarmupTasks().put("not allowed", Collections.emptySet()));
        assertThrows(UnsupportedOperationException.class, () -> member.activeTasksPendingRevocation().put("not allowed", Collections.emptySet()));
        assertThrows(UnsupportedOperationException.class, () -> member.standbyTasksPendingRevocation().put("not allowed", Collections.emptySet()));
        assertThrows(UnsupportedOperationException.class, () -> member.warmupTasksPendingRevocation().put("not allowed", Collections.emptySet()));
    }

    @Test
    public void testAsStreamsGroupDescribeMember() {
        String subTopology1 = Uuid.randomUuid().toString();
        String subTopology2 = Uuid.randomUuid().toString();
        String subTopology3 = Uuid.randomUuid().toString();
        List<Integer> assignedTasks1 = Arrays.asList(0, 1, 2);
        List<Integer> assignedTasks2 = Arrays.asList(3, 4, 5);
        List<Integer> assignedTasks3 = Arrays.asList(6, 7, 8);
        int epoch = 10;
        String memberId = Uuid.randomUuid().toString();
        String clientId = "clientId";
        String instanceId = "instanceId";
        String rackId = "rackId";
        String clientHost = "clientHost";
        String processId = "processId";
        int topologyEpoch = 3;
        Map<String, String> clientTags = Collections.singletonMap("key", "value");
        Assignment targetAssignment = new Assignment(
            mkMap(mkEntry(subTopology1, new HashSet<>(assignedTasks3))),
            mkMap(mkEntry(subTopology2, new HashSet<>(assignedTasks2))),
            mkMap(mkEntry(subTopology3, new HashSet<>(assignedTasks1)))
        );
        StreamsGroupMember member = new StreamsGroupMember.Builder(memberId)
            .setMemberEpoch(epoch)
            .setPreviousMemberEpoch(epoch - 1)
            .setClientId(clientId)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setClientHost(clientHost)
            .setProcessId(processId)
            .setTopologyEpoch(topologyEpoch)
            .setClientTags(clientTags)
            .setAssignedActiveTasks(
                mkMap(mkEntry(subTopology1, new HashSet<>(assignedTasks1)))
            )
            .setAssignedStandbyTasks(
                mkMap(mkEntry(subTopology2, new HashSet<>(assignedTasks2)))
            )
            .setAssignedWarmupTasks(
                mkMap(mkEntry(subTopology3, new HashSet<>(assignedTasks3)))
            )
            .setUserEndpoint(
                new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090)
            )
            .build();

        StreamsGroupDescribeResponseData.Member actual = member.asStreamsGroupDescribeMember(targetAssignment);
        StreamsGroupDescribeResponseData.Member expected = new StreamsGroupDescribeResponseData.Member()
            .setMemberId(memberId)
            .setMemberEpoch(epoch)
            .setClientId(clientId)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setClientHost(clientHost)
            .setProcessId(processId)
            .setTopologyEpoch(topologyEpoch)
            .setClientTags(Collections.singletonList(new StreamsGroupDescribeResponseData.KeyValue().setKey("key").setValue("value")))
            .setAssignment(
                new StreamsGroupDescribeResponseData.Assignment()
                    .setActiveTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology1)
                        .setPartitions(assignedTasks1)))
                    .setStandbyTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology2)
                        .setPartitions(assignedTasks2)))
                    .setWarmupTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology3)
                        .setPartitions(assignedTasks3)))
            )
            .setTargetAssignment(
                new StreamsGroupDescribeResponseData.Assignment()
                    .setActiveTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology1)
                        .setPartitions(assignedTasks3)))
                    .setStandbyTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology2)
                        .setPartitions(assignedTasks2)))
                    .setWarmupTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology3)
                        .setPartitions(assignedTasks1)))
            )
            .setUserEndpoint(new StreamsGroupDescribeResponseData.Endpoint().setHost("host").setPort(9090));

        assertEquals(expected, actual);
    }

    @Test
    public void testAsStreamsGroupDescribeWithTargetAssignmentNull() {
        StreamsGroupMember member = new StreamsGroupMember.Builder(Uuid.randomUuid().toString())
            .build();

        StreamsGroupDescribeResponseData.Member streamsGroupDescribeMember = member.asStreamsGroupDescribeMember(
            null);

        assertEquals(new StreamsGroupDescribeResponseData.Assignment(), streamsGroupDescribeMember.targetAssignment());
    }
}
