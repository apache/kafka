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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StreamsGroupMemberTest {

    private static final String MEMBER_ID = "member-id";
    private static final int MEMBER_EPOCH = 10;
    private static final int PREVIOUS_MEMBER_EPOCH = 9;
    private static final MemberState STATE = MemberState.UNRELEASED_TASKS;
    private static final String INSTANCE_ID = "instance-id";
    private static final String RACK_ID = "rack-id";
    private static final int REBALANCE_TIMEOUT = 5000;
    private static final String CLIENT_ID = "client-id";
    private static final String HOSTNAME = "hostname";
    private static final int TOPOLOGY_EPOCH = 3;
    private static final String PROCESS_ID = "process-id";
    private static final String SUBTOPOLOGY1 = "subtopology1";
    private static final String SUBTOPOLOGY2 = "subtopology2";
    private static final String SUBTOPOLOGY3 = "subtopology3";
    private static final StreamsGroupMemberMetadataValue.Endpoint USER_ENDPOINT =
        new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090);
    private static final String CLIENT_TAG_KEY = "client";
    private static final String CLIENT_TAG_VALUE = "tag";
    private static final Map<String, String> CLIENT_TAGS = mkMap(mkEntry(CLIENT_TAG_KEY, CLIENT_TAG_VALUE));
    private static final List<Integer> TASKS1 = List.of(1, 2, 3);
    private static final List<Integer> TASKS2 = List.of(4, 5, 6);
    private static final List<Integer> TASKS3 = List.of(7, 8);
    private static final List<Integer> TASKS4 = List.of(3, 2, 1);
    private static final List<Integer> TASKS5 = List.of(6, 5, 4);
    private static final List<Integer> TASKS6 = List.of(9, 7);
    private static final Map<String, Set<Integer>> ASSIGNED_ACTIVE_TASKS = mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY1, TASKS1.toArray(Integer[]::new)));
    private static final Map<String, Set<Integer>> ASSIGNED_STANDBY_TASKS = mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY2, TASKS2.toArray(Integer[]::new)));
    private static final Map<String, Set<Integer>> ASSIGNED_WARMUP_TASKS = mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY1, TASKS3.toArray(Integer[]::new)));
    private static final Map<String, Set<Integer>> ACTIVE_TASKS_PENDING_REVOCATION = mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY2, TASKS4.toArray(Integer[]::new)));
    private static final Map<String, Set<Integer>> STANDBY_TASKS_PENDING_REVOCATION = mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY1, TASKS5.toArray(Integer[]::new)));
    private static final Map<String, Set<Integer>> WARMUP_TASKS_PENDING_REVOCATION = mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY2, TASKS6.toArray(Integer[]::new)));

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
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_ID).build();

        assertEquals(MEMBER_ID, member.memberId());
        assertNull(member.memberEpoch());
        assertNull(member.previousMemberEpoch());
        assertNull(member.state());
        assertNull(member.instanceId());
        assertNull(member.rackId());
        assertNull(member.rebalanceTimeoutMs());
        assertNull(member.clientId());
        assertNull(member.clientHost());
        assertNull(member.topologyEpoch());
        assertNull(member.processId());
        assertNull(member.userEndpoint());
        assertNull(member.clientTags());
        assertNull(member.assignedActiveTasks());
        assertNull(member.assignedStandbyTasks());
        assertNull(member.assignedWarmupTasks());
        assertNull(member.activeTasksPendingRevocation());
        assertNull(member.standbyTasksPendingRevocation());
        assertNull(member.warmupTasksPendingRevocation());
    }

    @Test
    public void testBuilderNewMember() {
        StreamsGroupMember member = createStreamsGroupMember();

        assertEquals(MEMBER_ID, member.memberId());
        assertEquals(MEMBER_EPOCH, member.memberEpoch());
        assertEquals(PREVIOUS_MEMBER_EPOCH, member.previousMemberEpoch());
        assertEquals(STATE, member.state());
        assertEquals(Optional.of(INSTANCE_ID), member.instanceId());
        assertEquals(Optional.of(RACK_ID), member.rackId());
        assertEquals(CLIENT_ID, member.clientId());
        assertEquals(HOSTNAME, member.clientHost());
        assertEquals(TOPOLOGY_EPOCH, member.topologyEpoch());
        assertEquals(PROCESS_ID, member.processId());
        assertEquals(Optional.of(USER_ENDPOINT), member.userEndpoint());
        assertEquals(CLIENT_TAGS, member.clientTags());
        assertEquals(ASSIGNED_ACTIVE_TASKS, member.assignedActiveTasks());
        assertEquals(ASSIGNED_STANDBY_TASKS, member.assignedStandbyTasks());
        assertEquals(ASSIGNED_WARMUP_TASKS, member.assignedWarmupTasks());
        assertEquals(ACTIVE_TASKS_PENDING_REVOCATION, member.activeTasksPendingRevocation());
        assertEquals(STANDBY_TASKS_PENDING_REVOCATION, member.standbyTasksPendingRevocation());
        assertEquals(WARMUP_TASKS_PENDING_REVOCATION, member.warmupTasksPendingRevocation());
    }

    @Test
    public void testBuilderUpdateWithStreamsGroupMemberMetadataValue() {
        StreamsGroupMemberMetadataValue record = new StreamsGroupMemberMetadataValue()
            .setClientId(CLIENT_ID)
            .setClientHost(HOSTNAME)
            .setInstanceId(INSTANCE_ID)
            .setRackId(RACK_ID)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT)
            .setTopologyEpoch(TOPOLOGY_EPOCH)
            .setProcessId(PROCESS_ID)
            .setUserEndpoint(USER_ENDPOINT)
            .setClientTags(CLIENT_TAGS.entrySet().stream()
                .map(e -> new KeyValue().setKey(e.getKey()).setValue(e.getValue()))
                .collect(Collectors.toList()));

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
        assertEquals(MEMBER_ID, member.memberId());
        assertNull(member.memberEpoch());
        assertNull(member.previousMemberEpoch());
        assertNull(member.state());
        assertNull(member.assignedActiveTasks());
        assertNull(member.assignedStandbyTasks());
        assertNull(member.assignedWarmupTasks());
        assertNull(member.activeTasksPendingRevocation());
        assertNull(member.standbyTasksPendingRevocation());
        assertNull(member.warmupTasksPendingRevocation());
    }

    @Test
    public void testBuilderUpdateWithConsumerGroupCurrentMemberAssignmentValue() {
        StreamsGroupCurrentMemberAssignmentValue record = new StreamsGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(MEMBER_EPOCH)
            .setPreviousMemberEpoch(PREVIOUS_MEMBER_EPOCH)
            .setState(STATE.value())
            .setActiveTasks(List.of(new TaskIds().setSubtopologyId(SUBTOPOLOGY1).setPartitions(TASKS1)))
            .setStandbyTasks(List.of(new TaskIds().setSubtopologyId(SUBTOPOLOGY2).setPartitions(TASKS2)))
            .setWarmupTasks(List.of(new TaskIds().setSubtopologyId(SUBTOPOLOGY1).setPartitions(TASKS3)))
            .setActiveTasksPendingRevocation(List.of(new TaskIds().setSubtopologyId(SUBTOPOLOGY2).setPartitions(TASKS4)))
            .setStandbyTasksPendingRevocation(List.of(new TaskIds().setSubtopologyId(SUBTOPOLOGY1).setPartitions(TASKS5)))
            .setWarmupTasksPendingRevocation(List.of(new TaskIds().setSubtopologyId(SUBTOPOLOGY2).setPartitions(TASKS6)));

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_ID)
            .updateWith(record)
            .build();

        assertEquals(MEMBER_ID, member.memberId());
        assertEquals(record.memberEpoch(), member.memberEpoch());
        assertEquals(record.previousMemberEpoch(), member.previousMemberEpoch());
        assertEquals(MemberState.fromValue(record.state()), member.state());
        assertEquals(ASSIGNED_ACTIVE_TASKS, member.assignedActiveTasks());
        assertEquals(ASSIGNED_STANDBY_TASKS, member.assignedStandbyTasks());
        assertEquals(ASSIGNED_WARMUP_TASKS, member.assignedWarmupTasks());
        assertEquals(ACTIVE_TASKS_PENDING_REVOCATION, member.activeTasksPendingRevocation());
        assertEquals(STANDBY_TASKS_PENDING_REVOCATION, member.standbyTasksPendingRevocation());
        assertEquals(WARMUP_TASKS_PENDING_REVOCATION, member.warmupTasksPendingRevocation());
        assertNull(member.instanceId());
        assertNull(member.rackId());
        assertNull(member.rebalanceTimeoutMs());
        assertNull(member.clientId());
        assertNull(member.clientHost());
        assertNull(member.topologyEpoch());
        assertNull(member.processId());
        assertNull(member.userEndpoint());
        assertNull(member.clientTags());
    }

    @Test
    public void testBuilderMaybeUpdateMember() {
        final StreamsGroupMember member = createStreamsGroupMember();

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
        final Integer newRebalanceTimeout = member.rebalanceTimeoutMs() + 1000;
        final String newProcessId = "new" + member.processId();
        final Integer newTopologyEpoch = member.topologyEpoch() + 1;
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
        final StreamsGroupMember member = createStreamsGroupMember();

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
        assertEquals(member.processId(), updatedMember.processId());
        assertEquals(member.userEndpoint(), updatedMember.userEndpoint());
        assertEquals(member.clientTags(), updatedMember.clientTags());
        assertEquals(member.assignedActiveTasks(), updatedMember.assignedActiveTasks());
        assertEquals(member.assignedStandbyTasks(), updatedMember.assignedStandbyTasks());
        assertEquals(member.assignedWarmupTasks(), updatedMember.assignedWarmupTasks());
        assertEquals(member.activeTasksPendingRevocation(), updatedMember.activeTasksPendingRevocation());
        assertEquals(member.standbyTasksPendingRevocation(), updatedMember.standbyTasksPendingRevocation());
        assertEquals(member.warmupTasksPendingRevocation(), updatedMember.warmupTasksPendingRevocation());
    }

    @Test
    public void testReturnUnmodifiableFields() {
        final StreamsGroupMember member = createStreamsGroupMember();

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
        final StreamsGroupMember member = createStreamsGroupMember();
        List<Integer> assignedTasks1 = Arrays.asList(10, 11, 12);
        List<Integer> assignedTasks2 = Arrays.asList(13, 14, 15);
        List<Integer> assignedTasks3 = Arrays.asList(16, 17, 18);
        Assignment targetAssignment = new Assignment(
            mkMap(mkEntry(SUBTOPOLOGY1, new HashSet<>(assignedTasks3))),
            mkMap(mkEntry(SUBTOPOLOGY2, new HashSet<>(assignedTasks2))),
            mkMap(mkEntry(SUBTOPOLOGY3, new HashSet<>(assignedTasks1)))
        );

        StreamsGroupDescribeResponseData.Member actual = member.asStreamsGroupDescribeMember(targetAssignment);
        StreamsGroupDescribeResponseData.Member expected = new StreamsGroupDescribeResponseData.Member()
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setClientId(CLIENT_ID)
            .setInstanceId(INSTANCE_ID)
            .setRackId(RACK_ID)
            .setClientHost(HOSTNAME)
            .setProcessId(PROCESS_ID)
            .setTopologyEpoch(TOPOLOGY_EPOCH)
            .setClientTags(List.of(
                new StreamsGroupDescribeResponseData.KeyValue().setKey(CLIENT_TAG_KEY).setValue(CLIENT_TAG_VALUE))
            )
            .setAssignment(
                new StreamsGroupDescribeResponseData.Assignment()
                    .setActiveTasks(List.of(
                        new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY1)
                            .setPartitions(TASKS1))
                    )
                    .setStandbyTasks(List.of(
                        new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY2)
                            .setPartitions(TASKS2))
                    )
                    .setWarmupTasks(List.of(
                        new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY1)
                            .setPartitions(TASKS3))
                    )
            )
            .setTargetAssignment(
                new StreamsGroupDescribeResponseData.Assignment()
                    .setActiveTasks(List.of(
                        new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY1)
                            .setPartitions(assignedTasks3))
                    )
                    .setStandbyTasks(List.of(
                        new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY2)
                            .setPartitions(assignedTasks2))
                    )
                    .setWarmupTasks(List.of(
                        new StreamsGroupDescribeResponseData.TaskIds()
                            .setSubtopologyId(SUBTOPOLOGY3)
                            .setPartitions(assignedTasks1))
                    )
            )
            .setUserEndpoint(new StreamsGroupDescribeResponseData.Endpoint()
                .setHost(USER_ENDPOINT.host())
                .setPort(USER_ENDPOINT.port())
            );

        assertEquals(expected, actual);
    }

    @Test
    public void testAsStreamsGroupDescribeWithTargetAssignmentNull() {
        final StreamsGroupMember member = createStreamsGroupMember();
        StreamsGroupDescribeResponseData.Member streamsGroupDescribeMember = member.asStreamsGroupDescribeMember(null);

        assertEquals(new StreamsGroupDescribeResponseData.Assignment(), streamsGroupDescribeMember.targetAssignment());
    }

    private StreamsGroupMember createStreamsGroupMember() {
        return new StreamsGroupMember.Builder(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setPreviousMemberEpoch(PREVIOUS_MEMBER_EPOCH)
            .setState(STATE)
            .setInstanceId(INSTANCE_ID)
            .setRackId(RACK_ID)
            .setRebalanceTimeoutMs(REBALANCE_TIMEOUT)
            .setClientId(CLIENT_ID)
            .setClientHost(HOSTNAME)
            .setTopologyEpoch(TOPOLOGY_EPOCH)
            .setProcessId(PROCESS_ID)
            .setUserEndpoint(USER_ENDPOINT)
            .setClientTags(CLIENT_TAGS)
            .setAssignedActiveTasks(ASSIGNED_ACTIVE_TASKS)
            .setAssignedStandbyTasks(ASSIGNED_STANDBY_TASKS)
            .setAssignedWarmupTasks(ASSIGNED_WARMUP_TASKS)
            .setActiveTasksPendingRevocation(ACTIVE_TASKS_PENDING_REVOCATION)
            .setStandbyTasksPendingRevocation(STANDBY_TASKS_PENDING_REVOCATION)
            .setWarmupTasksPendingRevocation(WARMUP_TASKS_PENDING_REVOCATION)
            .build();
    }
}
