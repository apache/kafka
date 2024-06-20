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

import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue.TaskIds;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue.KeyValue;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTaskAssignment;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
            .setTopologyHash("topology-hash".getBytes())
            .setAssignor("assignor")
            .setProcessId("process-id")
            .setHostInfo(new StreamsGroupMemberMetadataValue.HostInfo().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setUserData("user-data".getBytes())
            .setAssignmentConfigs(mkMap(mkEntry("assignment", "config")))
            .setAssignedActiveTasks(mkAssignment(mkTaskAssignment(subtopologyId1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkAssignment(mkTaskAssignment(subtopologyId2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkAssignment(mkTaskAssignment(subtopologyId1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(mkAssignment(mkTaskAssignment(subtopologyId2, 3, 2, 1)))
            .build();

        assertEquals("member-id", member.memberId());
        assertEquals(10, member.memberEpoch());
        assertEquals(9, member.previousMemberEpoch());
        assertEquals("instance-id", member.instanceId());
        assertEquals("rack-id", member.rackId());
        assertEquals("client-id", member.clientId());
        assertEquals("hostname", member.clientHost());
        assertArrayEquals("topology-hash".getBytes(), member.topologyHash());
        assertEquals("assignor", member.assignor().get());
        assertEquals("process-id", member.processId());
        assertEquals(new StreamsGroupMemberMetadataValue.HostInfo().setHost("host").setPort(9090), member.hostInfo());
        assertEquals(
            mkMap(mkEntry("client", "tag")),
            member.clientTags()
        );
        assertArrayEquals("user-data".getBytes(), member.userData());
        assertEquals(
            mkMap(mkEntry("assignment", "config")),
            member.assignmentConfigs()
        );
        assertEquals(
            mkAssignment(mkTaskAssignment(subtopologyId1, 1, 2, 3)),
            member.assignedActiveTasks()
        );
        assertEquals(
            mkAssignment(mkTaskAssignment(subtopologyId2, 6, 5, 4)),
            member.assignedStandbyTasks()
        );
        assertEquals(
            mkAssignment(mkTaskAssignment(subtopologyId1, 7, 8, 9)),
            member.assignedWarmupTasks()
        );
        assertEquals(
            mkAssignment(mkTaskAssignment(subtopologyId2, 3, 2, 1)),
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
            .setTopologyHash("topology-hash".getBytes())
            .setAssignor("assignor")
            .setProcessId("process-id")
            .setHostInfo(new StreamsGroupMemberMetadataValue.HostInfo().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setUserData("user-data".getBytes())
            .setAssignmentConfigs(mkMap(mkEntry("assignment", "config")))
            .setAssignedActiveTasks(mkAssignment(mkTaskAssignment(subtopologyId1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkAssignment(mkTaskAssignment(subtopologyId2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkAssignment(mkTaskAssignment(subtopologyId1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(mkAssignment(mkTaskAssignment(subtopologyId2, 3, 2, 1)))
            .build();

        StreamsGroupMember member2 = new StreamsGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setTopologyHash("topology-hash".getBytes())
            .setAssignor("assignor")
            .setProcessId("process-id")
            .setHostInfo(new StreamsGroupMemberMetadataValue.HostInfo().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setUserData("user-data".getBytes())
            .setAssignmentConfigs(mkMap(mkEntry("assignment", "config")))
            .setAssignedActiveTasks(mkAssignment(mkTaskAssignment(subtopologyId1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkAssignment(mkTaskAssignment(subtopologyId2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkAssignment(mkTaskAssignment(subtopologyId1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(mkAssignment(mkTaskAssignment(subtopologyId2, 3, 2, 1)))
            .build();

        StreamsGroupMember member3 = new StreamsGroupMember.Builder("member3-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setTopologyHash("topology-hash".getBytes())
            .setAssignor("assignor")
            .setProcessId("process-id")
            .setHostInfo(new StreamsGroupMemberMetadataValue.HostInfo().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setUserData("user-data".getBytes())
            .setAssignmentConfigs(mkMap(mkEntry("assignment", "config")))
            .setAssignedActiveTasks(mkAssignment(mkTaskAssignment(subtopologyId1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkAssignment(mkTaskAssignment(subtopologyId2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkAssignment(mkTaskAssignment(subtopologyId1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(mkAssignment(mkTaskAssignment(subtopologyId2, 3, 2, 1)))
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
            .setTopologyHash("topology-hash".getBytes())
            .setAssignor("assignor")
            .setProcessId("process-id")
            .setHostInfo(new StreamsGroupMemberMetadataValue.HostInfo().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setUserData("user-data".getBytes())
            .setAssignmentConfigs(mkMap(mkEntry("assignment", "config")))
            .setAssignedActiveTasks(mkAssignment(mkTaskAssignment(subtopologyId1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkAssignment(mkTaskAssignment(subtopologyId2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkAssignment(mkTaskAssignment(subtopologyId1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(mkAssignment(mkTaskAssignment(subtopologyId2, 3, 2, 1)))
            .build();

        // This is a no-op.
        StreamsGroupMember updatedMember = new StreamsGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.empty())
            .maybeUpdateInstanceId(Optional.empty())
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.empty())
            .maybeUpdateAssignor(Optional.empty())
            .build();

        assertEquals(member, updatedMember);

        updatedMember = new StreamsGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.of("new-rack-id"))
            .maybeUpdateInstanceId(Optional.of("new-instance-id"))
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.of(6000))
            .maybeUpdateAssignor(Optional.of("new-assignor"))
            .build();

        assertEquals("new-instance-id", updatedMember.instanceId());
        assertEquals("new-rack-id", updatedMember.rackId());
        assertEquals(6000, updatedMember.rebalanceTimeoutMs());
        assertEquals("new-assignor", updatedMember.assignor().get());
    }

    @Test
    public void testUpdateWithStreamsGroupMemberMetadataValue() {
        StreamsGroupMemberMetadataValue record = new StreamsGroupMemberMetadataValue()
            .setClientId("client-id")
            .setClientHost("host-id")
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(1000)
            .setTopologyHash("topology-hash".getBytes())
            .setAssignor("assignor")
            .setProcessId("process-id")
            .setHostInfo(new StreamsGroupMemberMetadataValue.HostInfo().setHost("host").setPort(9090))
            .setClientTags(Collections.singletonList(new KeyValue().setKey("client").setValue("tag")))
            .setUserData("user-data".getBytes())
            .setAssignmentConfigs(Collections.singletonList(new KeyValue().setKey("assignment").setValue("config")));

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals("instance-id", member.instanceId());
        assertEquals("rack-id", member.rackId());
        assertEquals("client-id", member.clientId());
        assertEquals("host-id", member.clientHost());
        assertEquals(1000, member.rebalanceTimeoutMs());
        assertArrayEquals("topology-hash".getBytes(), member.topologyHash());
        assertEquals("assignor", member.assignor().get());
        assertEquals("process-id", member.processId());
        assertEquals(new StreamsGroupMemberMetadataValue.HostInfo().setHost("host").setPort(9090), member.hostInfo());
        assertEquals(
            mkMap(mkEntry("client", "tag")),
            member.clientTags()
        );
        assertArrayEquals("user-data".getBytes(), member.userData());
        assertEquals(
            mkMap(mkEntry("assignment", "config")),
            member.assignmentConfigs()
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
                .setPartitions(Arrays.asList(1, 2, 3)))
            )
            .setStandbyTasks(Collections.singletonList(new TaskIds()
                .setSubtopology(subtopologyId2)
                .setPartitions(Arrays.asList(6, 5, 4)))
            )
            .setWarmupTasks(Collections.singletonList(new TaskIds()
                .setSubtopology(subtopologyId1)
                .setPartitions(Arrays.asList(7, 8, 9)))
            )
            .setActiveTasksPendingRevocation(Collections.singletonList(new TaskIds()
                .setSubtopology(subtopologyId2)
                .setPartitions(Arrays.asList(2, 3, 1)))
            );

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals(10, member.memberEpoch());
        assertEquals(9, member.previousMemberEpoch());
        assertEquals(MemberState.STABLE, member.state());
        assertEquals(mkAssignment(mkTaskAssignment(subtopologyId1, 1, 2, 3)), member.assignedActiveTasks());
        assertEquals(mkAssignment(mkTaskAssignment(subtopologyId2, 6, 5, 4)), member.assignedStandbyTasks());
        assertEquals(mkAssignment(mkTaskAssignment(subtopologyId1, 7, 8, 9)), member.assignedWarmupTasks());
        assertEquals(mkAssignment(mkTaskAssignment(subtopologyId2, 2, 3, 1)), member.activeTasksPendingRevocation());
    }
}
