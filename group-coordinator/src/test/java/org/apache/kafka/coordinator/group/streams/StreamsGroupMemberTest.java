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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksPerSubtopology;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class StreamsGroupMemberTest {

    @Test
    public void testNewMember() {
        String subtopologyId1 = "subtopology-1";
        String subtopologyId2 = "subtopology-2";

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setTopologyId("topology-hash")
            .setProcessId("process-id")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(subtopologyId1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkTasksPerSubtopology(mkTasks(subtopologyId2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkTasksPerSubtopology(mkTasks(subtopologyId1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(
                mkTasksPerSubtopology(mkTasks(subtopologyId2, 3, 2, 1)))
            .build();

        assertEquals("member-id", member.memberId());
        assertEquals(10, member.memberEpoch());
        assertEquals(9, member.previousMemberEpoch());
        assertEquals("instance-id", member.instanceId());
        assertEquals("rack-id", member.rackId());
        assertEquals("client-id", member.clientId());
        assertEquals("hostname", member.clientHost());
        assertEquals("topology-hash", member.topologyId());
        assertEquals("process-id", member.processId());
        assertEquals(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090), member.userEndpoint());
        assertEquals(
            mkMap(mkEntry("client", "tag")),
            member.clientTags()
        );
        assertEquals(
            mkTasksPerSubtopology(mkTasks(subtopologyId1, 1, 2, 3)),
            member.assignedActiveTasks()
        );
        assertEquals(
            mkTasksPerSubtopology(mkTasks(subtopologyId2, 6, 5, 4)),
            member.assignedStandbyTasks()
        );
        assertEquals(
            mkTasksPerSubtopology(mkTasks(subtopologyId1, 7, 8, 9)),
            member.assignedWarmupTasks()
        );
        assertEquals(
            mkTasksPerSubtopology(mkTasks(subtopologyId2, 3, 2, 1)),
            member.activeTasksPendingRevocation()
        );
    }

    @Test
    public void testEquals() {
        String subtopologyId1 = "subtopology-1";
        String subtopologyId2 = "subtopology-2";

        StreamsGroupMember member1 = new StreamsGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setTopologyId("topology-hash")
            .setProcessId("process-id")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(subtopologyId1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkTasksPerSubtopology(mkTasks(subtopologyId2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkTasksPerSubtopology(mkTasks(subtopologyId1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(
                mkTasksPerSubtopology(mkTasks(subtopologyId2, 3, 2, 1)))
            .build();

        StreamsGroupMember member2 = new StreamsGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setTopologyId("topology-hash")
            .setProcessId("process-id")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(subtopologyId1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkTasksPerSubtopology(mkTasks(subtopologyId2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkTasksPerSubtopology(mkTasks(subtopologyId1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(
                mkTasksPerSubtopology(mkTasks(subtopologyId2, 3, 2, 1)))
            .build();

        StreamsGroupMember member3 = new StreamsGroupMember.Builder("member3-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setTopologyId("topology-hash")
            .setProcessId("process-id")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(subtopologyId1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkTasksPerSubtopology(mkTasks(subtopologyId2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkTasksPerSubtopology(mkTasks(subtopologyId1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(
                mkTasksPerSubtopology(mkTasks(subtopologyId2, 3, 2, 1)))
            .build();

        assertEquals(member1, member2);
        assertNotEquals(member1, member3);
    }

    @Test
    public void testUpdateMember() {
        String subtopologyId1 = "subtopology-1";
        String subtopologyId2 = "subtopology-2";

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setTopologyId("topology-hash")
            .setProcessId("process-id")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(subtopologyId1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkTasksPerSubtopology(mkTasks(subtopologyId2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkTasksPerSubtopology(mkTasks(subtopologyId1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(
                mkTasksPerSubtopology(mkTasks(subtopologyId2, 3, 2, 1)))
            .build();

        // This is a no-op.
        StreamsGroupMember updatedMember = new StreamsGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.empty())
            .maybeUpdateInstanceId(Optional.empty())
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.empty())
            .build();

        assertEquals(member, updatedMember);

        updatedMember = new StreamsGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.of("new-rack-id"))
            .maybeUpdateInstanceId(Optional.of("new-instance-id"))
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.of(6000))
            .build();

        assertEquals("new-instance-id", updatedMember.instanceId());
        assertEquals("new-rack-id", updatedMember.rackId());
        assertEquals(6000, updatedMember.rebalanceTimeoutMs());
    }

    @Test
    public void testUpdateWithStreamsGroupMemberMetadataValue() {
        StreamsGroupMemberMetadataValue record = new StreamsGroupMemberMetadataValue()
            .setClientId("client-id")
            .setClientHost("host-id")
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(1000)
            .setTopologyId("topology-hash")
            .setProcessId("process-id")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
            .setClientTags(Collections.singletonList(new KeyValue().setKey("client").setValue("tag")));

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals("instance-id", member.instanceId());
        assertEquals("rack-id", member.rackId());
        assertEquals("client-id", member.clientId());
        assertEquals("host-id", member.clientHost());
        assertEquals(1000, member.rebalanceTimeoutMs());
        assertEquals("topology-hash", member.topologyId());
        assertEquals("process-id", member.processId());
        assertEquals(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090), member.userEndpoint());
        assertEquals(
            mkMap(mkEntry("client", "tag")),
            member.clientTags()
        );
    }

    @Test
    public void testUpdateWithConsumerGroupCurrentMemberAssignmentValue() {
        String subtopologyId1 = "subtopology-1";
        String subtopologyId2 = "subtopology-2";

        StreamsGroupCurrentMemberAssignmentValue record = new StreamsGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setState((byte) 1)
            .setActiveTasks(Collections.singletonList(new TaskIds()
                .setSubtopology(subtopologyId1)
                .setPartitions(Arrays.asList(1, 2)))
            )
            .setStandbyTasks(Collections.singletonList(new TaskIds()
                .setSubtopology(subtopologyId2)
                .setPartitions(Arrays.asList(3, 4)))
            )
            .setWarmupTasks(Collections.singletonList(new TaskIds()
                .setSubtopology(subtopologyId1)
                .setPartitions(Arrays.asList(5, 6)))
            )
            .setActiveTasksPendingRevocation(Collections.singletonList(new TaskIds()
                .setSubtopology(subtopologyId2)
                .setPartitions(Arrays.asList(7, 8)))
            )
            .setStandbyTasksPendingRevocation(Collections.singletonList(new TaskIds()
                .setSubtopology(subtopologyId1)
                .setPartitions(Arrays.asList(9, 10)))
            )
            .setWarmupTasksPendingRevocation(Collections.singletonList(new TaskIds()
                .setSubtopology(subtopologyId2)
                .setPartitions(Arrays.asList(11, 12)))
            );

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals(10, member.memberEpoch());
        assertEquals(9, member.previousMemberEpoch());
        assertEquals(MemberState.STABLE, member.state());
        assertEquals(mkTasksPerSubtopology(mkTasks(subtopologyId1, 1, 2)), member.assignedActiveTasks());
        assertEquals(mkTasksPerSubtopology(mkTasks(subtopologyId2, 3, 4)), member.assignedStandbyTasks());
        assertEquals(mkTasksPerSubtopology(mkTasks(subtopologyId1, 5, 6)), member.assignedWarmupTasks());
        assertEquals(mkTasksPerSubtopology(mkTasks(subtopologyId2, 7, 8)), member.activeTasksPendingRevocation());
        assertEquals(mkTasksPerSubtopology(mkTasks(subtopologyId1, 9, 10)), member.standbyTasksPendingRevocation());
        assertEquals(mkTasksPerSubtopology(mkTasks(subtopologyId2, 11, 12)), member.warmupTasksPendingRevocation());
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
        StreamsGroupCurrentMemberAssignmentValue record = new StreamsGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(epoch)
            .setPreviousMemberEpoch(epoch - 1)
            .setActiveTasks(Collections.singletonList(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                .setSubtopology(subTopology1)
                .setPartitions(assignedTasks1)))
            .setStandbyTasks(Collections.singletonList(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                .setSubtopology(subTopology2)
                .setPartitions(assignedTasks2)))
            .setWarmupTasks(Collections.singletonList(new StreamsGroupCurrentMemberAssignmentValue.TaskIds()
                .setSubtopology(subTopology3)
                .setPartitions(assignedTasks3)));
        String memberId = Uuid.randomUuid().toString();
        String clientId = "clientId";
        String instanceId = "instanceId";
        String rackId = "rackId";
        String clientHost = "clientHost";
        String processId = "processId";
        String topologyId = "topologyId";
        Map<String, String> clientTags = Collections.singletonMap("key", "value");
        org.apache.kafka.coordinator.group.streams.Assignment targetAssignment = new org.apache.kafka.coordinator.group.streams.Assignment(
            mkMap(mkEntry(subTopology1, new HashSet<>(assignedTasks3))),
            mkMap(mkEntry(subTopology2, new HashSet<>(assignedTasks2))),
            mkMap(mkEntry(subTopology3, new HashSet<>(assignedTasks1)))
        );
        StreamsGroupMember member = new StreamsGroupMember.Builder(memberId)
            .updateWith(record)
            .setClientId(clientId)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setClientHost(clientHost)
            .setProcessId(processId)
            .setTopologyId(topologyId)
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
            .setTopologyId(topologyId)
            .setClientTags(Collections.singletonList(new StreamsGroupDescribeResponseData.KeyValue().setKey("key").setValue("value")))
            .setAssignment(
                new StreamsGroupDescribeResponseData.Assignment()
                    .setActiveTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopology(subTopology1)
                        .setPartitions(assignedTasks1)))
                    .setStandbyTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopology(subTopology2)
                        .setPartitions(assignedTasks2)))
                    .setWarmupTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopology(subTopology3)
                        .setPartitions(assignedTasks3)))
            )
            .setTargetAssignment(
                new StreamsGroupDescribeResponseData.Assignment()
                    .setActiveTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopology(subTopology1)
                        .setPartitions(assignedTasks3)))
                    .setStandbyTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopology(subTopology2)
                        .setPartitions(assignedTasks2)))
                    .setWarmupTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopology(subTopology3)
                        .setPartitions(assignedTasks1)))
            );
        // TODO: Add TaskOffsets

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
