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
package org.apache.kafka.coordinator.group.modern.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.coordinator.group.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.image.MetadataImage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerGroupMemberTest {

    @Test
    public void testNewMember() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(toClassicProtocolCollection("range")))
            .build();

        assertEquals("member-id", member.memberId());
        assertEquals(10, member.memberEpoch());
        assertEquals(9, member.previousMemberEpoch());
        assertEquals("instance-id", member.instanceId());
        assertEquals("rack-id", member.rackId());
        assertEquals("client-id", member.clientId());
        assertEquals("hostname", member.clientHost());
        assertEquals(Set.of("bar", "foo"), member.subscribedTopicNames());
        assertEquals("regex", member.subscribedTopicRegex());
        assertEquals("range", member.serverAssignorName().get());
        assertEquals(mkAssignment(mkTopicAssignment(topicId1, 1, 2, 3)), member.assignedPartitions());
        assertEquals(mkAssignment(mkTopicAssignment(topicId2, 4, 5, 6)), member.partitionsPendingRevocation());
        assertEquals(
            new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(toClassicProtocolCollection("range")),
            member.classicMemberMetadata().get()
        );
        assertEquals(toClassicProtocolCollection("range"), member.supportedClassicProtocols().get());
    }

    @Test
    public void testEquals() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(toClassicProtocolCollection("range")))
            .build();

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(toClassicProtocolCollection("range")))
            .build();

        assertEquals(member1, member2);
    }

    @Test
    public void testUpdateMember() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(toClassicProtocolCollection("range")))
            .build();

        // This is a no-op.
        ConsumerGroupMember updatedMember = new ConsumerGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.empty())
            .maybeUpdateInstanceId(Optional.empty())
            .maybeUpdateServerAssignorName(Optional.empty())
            .maybeUpdateSubscribedTopicNames(Optional.empty())
            .maybeUpdateSubscribedTopicRegex(Optional.empty())
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.empty())
            .build();

        assertEquals(member, updatedMember);

        updatedMember = new ConsumerGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.of("new-rack-id"))
            .maybeUpdateInstanceId(Optional.of("new-instance-id"))
            .maybeUpdateServerAssignorName(Optional.of("new-assignor"))
            .maybeUpdateSubscribedTopicNames(Optional.of(Collections.singletonList("zar")))
            .maybeUpdateSubscribedTopicRegex(Optional.of("new-regex"))
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.of(6000))
            .build();

        assertEquals("new-instance-id", updatedMember.instanceId());
        assertEquals("new-rack-id", updatedMember.rackId());
        // Names are sorted.
        assertEquals(Set.of("zar"), updatedMember.subscribedTopicNames());
        assertEquals("new-regex", updatedMember.subscribedTopicRegex());
        assertEquals("new-assignor", updatedMember.serverAssignorName().get());
    }

    @Test
    public void testUpdateWithConsumerGroupMemberMetadataValue() {
        ConsumerGroupMemberMetadataValue record = new ConsumerGroupMemberMetadataValue()
            .setServerAssignor("range")
            .setClientId("client-id")
            .setClientHost("host-id")
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(1000)
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setSubscribedTopicRegex("regex")
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(toClassicProtocolCollection("range")));

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals("instance-id", member.instanceId());
        assertEquals("rack-id", member.rackId());
        assertEquals("client-id", member.clientId());
        assertEquals("host-id", member.clientHost());
        assertEquals(Set.of("bar", "foo"), member.subscribedTopicNames());
        assertEquals("regex", member.subscribedTopicRegex());
        assertEquals("range", member.serverAssignorName().get());
        assertEquals(
            new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(toClassicProtocolCollection("range")),
            member.classicMemberMetadata().get()
        );
    }

    @Test
    public void testUpdateWithConsumerGroupCurrentMemberAssignmentValue() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        ConsumerGroupCurrentMemberAssignmentValue record = new ConsumerGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setAssignedPartitions(Collections.singletonList(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId1)
                .setPartitions(Arrays.asList(0, 1, 2))))
            .setPartitionsPendingRevocation(Collections.singletonList(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId2)
                .setPartitions(Arrays.asList(3, 4, 5))));

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals(10, member.memberEpoch());
        assertEquals(9, member.previousMemberEpoch());
        assertEquals(mkAssignment(mkTopicAssignment(topicId1, 0, 1, 2)), member.assignedPartitions());
        assertEquals(mkAssignment(mkTopicAssignment(topicId2, 3, 4, 5)), member.partitionsPendingRevocation());
    }

    @ParameterizedTest(name = "{displayName}.withClassicMemberMetadata={0}")
    @ValueSource(booleans = {true, false})
    public void testAsConsumerGroupDescribeMember(boolean withClassicMemberMetadata) {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();
        Uuid topicId4 = Uuid.randomUuid();
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, "topic1", 3)
            .addTopic(topicId2, "topic2", 3)
            .addTopic(topicId3, "topic3", 3)
            .addTopic(topicId4, "topic4", 3)
            .build();
        List<Integer> assignedPartitions = Arrays.asList(0, 1, 2);
        int epoch = 10;
        ConsumerGroupCurrentMemberAssignmentValue record = new ConsumerGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(epoch)
            .setPreviousMemberEpoch(epoch - 1)
            .setAssignedPartitions(Collections.singletonList(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId1)
                .setPartitions(assignedPartitions)))
            .setPartitionsPendingRevocation(Collections.singletonList(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(topicId2)
                .setPartitions(Arrays.asList(3, 4, 5))));
        String memberId = Uuid.randomUuid().toString();
        String clientId = "clientId";
        String instanceId = "instanceId";
        String rackId = "rackId";
        String clientHost = "clientHost";
        List<String> subscribedTopicNames = Arrays.asList("topic1", "topic2");
        String subscribedTopicRegex = "topic.*";
        Map<Uuid, Set<Integer>> assignmentMap = new HashMap<>();
        assignmentMap.put(topicId4, new HashSet<>(assignedPartitions));
        Assignment targetAssignment = new Assignment(assignmentMap);
        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .updateWith(record)
            .setClientId(clientId)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setClientHost(clientHost)
            .setSubscribedTopicNames(subscribedTopicNames)
            .setSubscribedTopicRegex(subscribedTopicRegex)
            .setClassicMemberMetadata(withClassicMemberMetadata ? new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(toClassicProtocolCollection("range")) : null)
            .build();

        ConsumerGroupDescribeResponseData.Member actual = member.asConsumerGroupDescribeMember(targetAssignment, metadataImage.topics());
        ConsumerGroupDescribeResponseData.Member expected = new ConsumerGroupDescribeResponseData.Member()
            .setMemberId(memberId)
            .setMemberEpoch(epoch)
            .setClientId(clientId)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setClientHost(clientHost)
            .setSubscribedTopicNames(new ArrayList<>(subscribedTopicNames))
            .setSubscribedTopicRegex(subscribedTopicRegex)
            .setAssignment(
                new ConsumerGroupDescribeResponseData.Assignment()
                    .setTopicPartitions(Collections.singletonList(new ConsumerGroupDescribeResponseData.TopicPartitions()
                        .setTopicId(topicId1)
                        .setTopicName("topic1")
                        .setPartitions(assignedPartitions)
                    ))
            )
            .setTargetAssignment(
                new ConsumerGroupDescribeResponseData.Assignment()
                    .setTopicPartitions(targetAssignment.partitions().entrySet().stream().map(
                        item -> new ConsumerGroupDescribeResponseData.TopicPartitions()
                            .setTopicId(item.getKey())
                            .setTopicName("topic4")
                            .setPartitions(new ArrayList<>(item.getValue()))
                    ).collect(Collectors.toList()))
            )
            .setMemberType(withClassicMemberMetadata ? (byte) 0 : (byte) 1);

        assertEquals(expected, actual);
    }

    @Test
    public void testAsConsumerGroupDescribeWithTargetAssignmentNull() {
        ConsumerGroupMember member = new ConsumerGroupMember.Builder(Uuid.randomUuid().toString())
            .build();

        ConsumerGroupDescribeResponseData.Member consumerGroupDescribeMember = member.asConsumerGroupDescribeMember(
            null, new MetadataImageBuilder().build().topics());

        assertEquals(new ConsumerGroupDescribeResponseData.Assignment(), consumerGroupDescribeMember.targetAssignment());
    }

    @Test
    public void testAsConsumerGroupDescribeWithTopicNameNotFound() {
        Uuid memberId = Uuid.randomUuid();
        ConsumerGroupCurrentMemberAssignmentValue record = new ConsumerGroupCurrentMemberAssignmentValue()
            .setAssignedPartitions(Collections.singletonList(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(Uuid.randomUuid())
                .setPartitions(Arrays.asList(0, 1, 2))));
        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId.toString())
            .updateWith(record)
            .build();

        ConsumerGroupDescribeResponseData.Member expected = new ConsumerGroupDescribeResponseData.Member()
            .setMemberId(memberId.toString())
            .setSubscribedTopicRegex("")
            .setMemberType((byte) 1);
        ConsumerGroupDescribeResponseData.Member actual = member.asConsumerGroupDescribeMember(null,
            new MetadataImageBuilder()
                .addTopic(Uuid.randomUuid(), "foo", 3)
                .build().topics()
        );
        assertEquals(expected, actual);
    }

    @Test
    public void testClassicProtocolListFromJoinRequestProtocolCollection() {
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();
        protocols.addAll(Arrays.asList(
            new JoinGroupRequestData.JoinGroupRequestProtocol()
                .setName("range")
                .setMetadata(new byte[]{1, 2, 3})
        ));

        assertEquals(
            toClassicProtocolCollection("range"),
            classicProtocolListFromJoinRequestProtocolCollection(protocols)
        );
    }

    private List<ConsumerGroupMemberMetadataValue.ClassicProtocol> toClassicProtocolCollection(String name) {
        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = new ArrayList<>();
        protocols.add(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName(name)
                .setMetadata(new byte[]{1, 2, 3})
        );
        return protocols;
    }
}
