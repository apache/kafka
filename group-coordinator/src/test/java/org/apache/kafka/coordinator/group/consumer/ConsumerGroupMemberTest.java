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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerGroupMemberTest {

    @Test
    public void testNewMember() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setTargetMemberEpoch(11)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignorName("range")
            .setClientAssignors(Collections.singletonList(
                new ClientAssignor(
                    "assignor",
                    (byte) 0,
                    (byte) 0,
                    (byte) 1,
                    new VersionedMetadata(
                        (byte) 1,
                        ByteBuffer.allocate(0)))))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(topicId3, 7, 8, 9)))
            .build();

        assertEquals("member-id", member.memberId());
        assertEquals(10, member.memberEpoch());
        assertEquals(9, member.previousMemberEpoch());
        assertEquals(11, member.targetMemberEpoch());
        assertEquals("instance-id", member.instanceId());
        assertEquals("rack-id", member.rackId());
        assertEquals("client-id", member.clientId());
        assertEquals("hostname", member.clientHost());
        // Names are sorted.
        assertEquals(Arrays.asList("bar", "foo"), member.subscribedTopicNames());
        assertEquals("regex", member.subscribedTopicRegex());
        assertEquals("range", member.serverAssignorName().get());
        assertEquals(
            Collections.singletonList(
                new ClientAssignor(
                    "assignor",
                    (byte) 0,
                    (byte) 0,
                    (byte) 1,
                    new VersionedMetadata(
                        (byte) 1,
                        ByteBuffer.allocate(0)))),
            member.clientAssignors());
        assertEquals(mkAssignment(mkTopicAssignment(topicId1, 1, 2, 3)), member.assignedPartitions());
        assertEquals(mkAssignment(mkTopicAssignment(topicId2, 4, 5, 6)), member.partitionsPendingRevocation());
        assertEquals(mkAssignment(mkTopicAssignment(topicId3, 7, 8, 9)), member.partitionsPendingAssignment());
    }

    @Test
    public void testEquals() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setTargetMemberEpoch(11)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignorName("range")
            .setClientAssignors(Collections.singletonList(
                new ClientAssignor(
                    "assignor",
                    (byte) 0,
                    (byte) 0,
                    (byte) 1,
                    new VersionedMetadata(
                        (byte) 1,
                        ByteBuffer.allocate(0)))))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(topicId3, 7, 8, 9)))
            .build();

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setTargetMemberEpoch(11)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignorName("range")
            .setClientAssignors(Collections.singletonList(
                new ClientAssignor(
                    "assignor",
                    (byte) 0,
                    (byte) 0,
                    (byte) 1,
                    new VersionedMetadata(
                        (byte) 1,
                        ByteBuffer.allocate(0)))))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(topicId3, 7, 8, 9)))
            .build();

        assertEquals(member1, member2);
    }

    @Test
    public void testUpdateMember() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setTargetMemberEpoch(11)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignorName("range")
            .setClientAssignors(Collections.singletonList(
                new ClientAssignor(
                    "assignor",
                    (byte) 0,
                    (byte) 0,
                    (byte) 1,
                    new VersionedMetadata(
                        (byte) 1,
                        ByteBuffer.allocate(0)))))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(topicId3, 7, 8, 9)))
            .build();

        // This is a no-op.
        ConsumerGroupMember updatedMember = new ConsumerGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.empty())
            .maybeUpdateInstanceId(Optional.empty())
            .maybeUpdateServerAssignorName(Optional.empty())
            .maybeUpdateSubscribedTopicNames(Optional.empty())
            .maybeUpdateSubscribedTopicRegex(Optional.empty())
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.empty())
            .maybeUpdateClientAssignors(Optional.empty())
            .build();

        assertEquals(member, updatedMember);

        updatedMember = new ConsumerGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.of("new-rack-id"))
            .maybeUpdateInstanceId(Optional.of("new-instance-id"))
            .maybeUpdateServerAssignorName(Optional.of("new-assignor"))
            .maybeUpdateSubscribedTopicNames(Optional.of(Arrays.asList("zar")))
            .maybeUpdateSubscribedTopicRegex(Optional.of("new-regex"))
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.of(6000))
            .maybeUpdateClientAssignors(Optional.of(Collections.emptyList()))
            .build();

        assertEquals("new-instance-id", updatedMember.instanceId());
        assertEquals("new-rack-id", updatedMember.rackId());
        // Names are sorted.
        assertEquals(Arrays.asList("zar"), updatedMember.subscribedTopicNames());
        assertEquals("new-regex", updatedMember.subscribedTopicRegex());
        assertEquals("new-assignor", updatedMember.serverAssignorName().get());
        assertEquals(Collections.emptyList(), updatedMember.clientAssignors());
    }

    @Test
    public void testUpdateWithConsumerGroupMemberMetadataValue() {
        ConsumerGroupMemberMetadataValue record = new ConsumerGroupMemberMetadataValue()
            .setAssignors(Collections.singletonList(new ConsumerGroupMemberMetadataValue.Assignor()
                .setName("client")
                .setMinimumVersion((short) 0)
                .setMaximumVersion((short) 2)
                .setVersion((short) 1)
                .setMetadata(new byte[0])))
            .setServerAssignor("range")
            .setClientId("client-id")
            .setClientHost("host-id")
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(1000)
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setSubscribedTopicRegex("regex");

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals("instance-id", member.instanceId());
        assertEquals("rack-id", member.rackId());
        assertEquals("client-id", member.clientId());
        assertEquals("host-id", member.clientHost());
        // Names are sorted.
        assertEquals(Arrays.asList("bar", "foo"), member.subscribedTopicNames());
        assertEquals("regex", member.subscribedTopicRegex());
        assertEquals("range", member.serverAssignorName().get());
        assertEquals(
            Collections.singletonList(
                new ClientAssignor(
                    "client",
                    (byte) 0,
                    (byte) 0,
                    (byte) 2,
                    new VersionedMetadata(
                        (byte) 1,
                        ByteBuffer.allocate(0)))),
            member.clientAssignors());
    }

    @Test
    public void testUpdateWithConsumerGroupCurrentMemberAssignmentValue() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();

        ConsumerGroupCurrentMemberAssignmentValue record = new ConsumerGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(Collections.singletonList(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId1)
                .setPartitions(Arrays.asList(0, 1, 2))))
            .setPartitionsPendingRevocation(Collections.singletonList(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId2)
                .setPartitions(Arrays.asList(3, 4, 5))))
            .setPartitionsPendingAssignment(Collections.singletonList(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId3)
                .setPartitions(Arrays.asList(6, 7, 8))));

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals(10, member.memberEpoch());
        assertEquals(9, member.previousMemberEpoch());
        assertEquals(11, member.targetMemberEpoch());
        assertEquals(mkAssignment(mkTopicAssignment(topicId1, 0, 1, 2)), member.assignedPartitions());
        assertEquals(mkAssignment(mkTopicAssignment(topicId2, 3, 4, 5)), member.partitionsPendingRevocation());
        assertEquals(mkAssignment(mkTopicAssignment(topicId3, 6, 7, 8)), member.partitionsPendingAssignment());
    }

    @Test
    public void testAsConsumerGroupDescribeMember() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();
        List<Integer> assignedPartitions = Arrays.asList(0, 1, 2);
        int epoch = 10;
        ConsumerGroupCurrentMemberAssignmentValue record = new ConsumerGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(epoch)
            .setPreviousMemberEpoch(epoch - 1)
            .setTargetMemberEpoch(epoch + 1)
            .setAssignedPartitions(Collections.singletonList(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId1)
                .setPartitions(assignedPartitions)))
            .setPartitionsPendingRevocation(Collections.singletonList(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId2)
                .setPartitions(Arrays.asList(3, 4, 5))))
            .setPartitionsPendingAssignment(Collections.singletonList(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId3)
                .setPartitions(Arrays.asList(6, 7, 8))));
        String memberId = Uuid.randomUuid().toString();
        String clientId = "clientId";
        String instanceId = "instanceId";
        String rackId = "rackId";
        String clientHost = "clientHost";
        List<String> subscribedTopicNames = Arrays.asList("topic1", "topic2");
        String subscribedTopicRegex = "topic.*";
        Map<Uuid, Set<Integer>> assignmentMap = new HashMap<>();
        assignmentMap.put(Uuid.randomUuid(), new HashSet<>());
        Assignment targetAssignment = new Assignment(assignmentMap);
        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .updateWith(record)
            .setClientId(clientId)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setClientHost(clientHost)
            .setSubscribedTopicNames(subscribedTopicNames)
            .setSubscribedTopicRegex(subscribedTopicRegex)
            .build();

        ConsumerGroupDescribeResponseData.Member actual = member.asConsumerGroupDescribeMember(targetAssignment);
        ConsumerGroupDescribeResponseData.Member expected = new ConsumerGroupDescribeResponseData.Member()
            .setMemberId(memberId)
            .setMemberEpoch(epoch)
            .setClientId(clientId)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setClientHost(clientHost)
            .setSubscribedTopicNames(subscribedTopicNames)
            .setSubscribedTopicRegex(subscribedTopicRegex)
            .setAssignment(
                new ConsumerGroupDescribeResponseData.Assignment()
                    .setTopicPartitions(Collections.singletonList(new ConsumerGroupDescribeResponseData.TopicPartitions().setTopicId(topicId1).setPartitions(assignedPartitions)))
            )
            .setTargetAssignment(
                new ConsumerGroupDescribeResponseData.Assignment()
                    .setTopicPartitions(targetAssignment.partitions().entrySet().stream().map(
                        item -> new ConsumerGroupDescribeResponseData.TopicPartitions()
                            .setTopicId(item.getKey())
                            .setPartitions(new ArrayList<>(item.getValue()))
                    ).collect(Collectors.toList()))
            );

        assertEquals(expected, actual);
    }

    @Test
    public void testAsConsumerGroupDescribeWithTargetAssignmentNull() {
        ConsumerGroupMember member = new ConsumerGroupMember.Builder(Uuid.randomUuid().toString())
            .build();

        ConsumerGroupDescribeResponseData.Member consumerGroupDescribeMember = member.asConsumerGroupDescribeMember(null);

        assertEquals(new ConsumerGroupDescribeResponseData.Assignment(), consumerGroupDescribeMember.targetAssignment());
    }
}
