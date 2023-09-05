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
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.consumer.ClientAssignor;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
import org.apache.kafka.coordinator.group.consumer.VersionedMetadata;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generic.GenericGroup;
import org.apache.kafka.coordinator.group.generic.GenericGroupMember;
import org.apache.kafka.coordinator.group.generic.GenericGroupState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.AssertionFailedError;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkSortedAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkSortedTopicAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTest.assertUnorderedListEquals;
import static org.apache.kafka.coordinator.group.RecordHelpers.newCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupEpochRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupEpochTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupSubscriptionMetadataTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentEpochRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentEpochTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentTombstoneRecord;
import static org.junit.jupiter.api.AssertionFailureBuilder.assertionFailure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class RecordHelpersTest {

    @Test
    public void testNewMemberSubscriptionRecord() {
        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(Arrays.asList("foo", "zar", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignorName("range")
            .setClientAssignors(Collections.singletonList(new ClientAssignor(
                "assignor",
                (byte) 0,
                (byte) 1,
                (byte) 10,
                new VersionedMetadata(
                    (byte) 5,
                    ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))))))
            .build();

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 5),
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataValue()
                    .setInstanceId("instance-id")
                    .setRackId("rack-id")
                    .setRebalanceTimeoutMs(5000)
                    .setClientId("client-id")
                    .setClientHost("client-host")
                    .setSubscribedTopicNames(Arrays.asList("bar", "foo", "zar"))
                    .setSubscribedTopicRegex("regex")
                    .setServerAssignor("range")
                    .setAssignors(Collections.singletonList(new ConsumerGroupMemberMetadataValue.Assignor()
                        .setName("assignor")
                        .setMinimumVersion((short) 1)
                        .setMaximumVersion((short) 10)
                        .setVersion((short) 5)
                        .setMetadata("hello".getBytes(StandardCharsets.UTF_8)))),
                (short) 0));

        assertEquals(expectedRecord, newMemberSubscriptionRecord(
            "group-id",
            member
        ));
    }

    @Test
    public void testNewMemberSubscriptionTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 5
            ),
            null);

        assertEquals(expectedRecord, newMemberSubscriptionTombstoneRecord(
            "group-id",
            "member-id"
        ));
    }

    @Test
    public void testNewGroupSubscriptionMetadataRecord() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        Map<String, TopicMetadata> subscriptionMetadata = new LinkedHashMap<>();

        subscriptionMetadata.put("foo", new TopicMetadata(
            fooTopicId,
            "foo",
            10,
            mkMapOfPartitionRacks(10)
        ));
        subscriptionMetadata.put("bar", new TopicMetadata(
            barTopicId,
            "bar",
            20,
            mkMapOfPartitionRacks(20)
        ));

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupPartitionMetadataKey()
                    .setGroupId("group-id"),
                (short) 4
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupPartitionMetadataValue()
                    .setTopics(Arrays.asList(
                        new ConsumerGroupPartitionMetadataValue.TopicMetadata()
                            .setTopicId(fooTopicId)
                            .setTopicName("foo")
                            .setNumPartitions(10)
                            .setPartitionMetadata(mkListOfPartitionRacks(10)),
                        new ConsumerGroupPartitionMetadataValue.TopicMetadata()
                            .setTopicId(barTopicId)
                            .setTopicName("bar")
                            .setNumPartitions(20)
                            .setPartitionMetadata(mkListOfPartitionRacks(20)))),
                (short) 0));

        assertRecordEquals(expectedRecord, newGroupSubscriptionMetadataRecord(
            "group-id",
            subscriptionMetadata
        ));
    }

    @Test
    public void testNewGroupSubscriptionMetadataTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupPartitionMetadataKey()
                    .setGroupId("group-id"),
                (short) 4
            ),
            null);

        assertEquals(expectedRecord, newGroupSubscriptionMetadataTombstoneRecord(
            "group-id"
        ));
    }

    @Test
    public void testEmptyPartitionMetadataWhenRacksUnavailableGroupSubscriptionMetadataRecord() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        Map<String, TopicMetadata> subscriptionMetadata = new LinkedHashMap<>();

        subscriptionMetadata.put("foo", new TopicMetadata(
            fooTopicId,
            "foo",
            10,
            Collections.emptyMap()
        ));
        subscriptionMetadata.put("bar", new TopicMetadata(
            barTopicId,
            "bar",
            20,
            Collections.emptyMap()
        ));

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupPartitionMetadataKey()
                    .setGroupId("group-id"),
                (short) 4
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupPartitionMetadataValue()
                    .setTopics(Arrays.asList(
                        new ConsumerGroupPartitionMetadataValue.TopicMetadata()
                            .setTopicId(fooTopicId)
                            .setTopicName("foo")
                            .setNumPartitions(10)
                            .setPartitionMetadata(Collections.emptyList()),
                        new ConsumerGroupPartitionMetadataValue.TopicMetadata()
                            .setTopicId(barTopicId)
                            .setTopicName("bar")
                            .setNumPartitions(20)
                            .setPartitionMetadata(Collections.emptyList()))),
                (short) 0));

        assertRecordEquals(expectedRecord, newGroupSubscriptionMetadataRecord(
            "group-id",
            subscriptionMetadata
        ));
    }

    @Test
    public void testNewGroupEpochRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey()
                    .setGroupId("group-id"),
                (short) 3),
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataValue()
                    .setEpoch(10),
                (short) 0));

        assertEquals(expectedRecord, newGroupEpochRecord(
            "group-id",
            10
        ));
    }

    @Test
    public void testNewGroupEpochTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey()
                    .setGroupId("group-id"),
                (short) 3),
            null);

        assertEquals(expectedRecord, newGroupEpochTombstoneRecord(
            "group-id"
        ));
    }

    @Test
    public void testNewTargetAssignmentRecord() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        Map<Uuid, Set<Integer>> partitions = mkSortedAssignment(
            mkTopicAssignment(topicId1, 11, 12, 13),
            mkTopicAssignment(topicId2, 21, 22, 23)
        );

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 7),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberValue()
                    .setTopicPartitions(Arrays.asList(
                        new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                            .setTopicId(topicId1)
                            .setPartitions(Arrays.asList(11, 12, 13)),
                        new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                            .setTopicId(topicId2)
                            .setPartitions(Arrays.asList(21, 22, 23)))),
                (short) 0));

        assertEquals(expectedRecord, newTargetAssignmentRecord(
            "group-id",
            "member-id",
            partitions
        ));
    }

    @Test
    public void testNewTargetAssignmentTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 7),
            null);

        assertEquals(expectedRecord, newTargetAssignmentTombstoneRecord(
            "group-id",
            "member-id"
        ));
    }

    @Test
    public void testNewTargetAssignmentEpochRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataKey()
                    .setGroupId("group-id"),
                (short) 6),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataValue()
                    .setAssignmentEpoch(10),
                (short) 0));

        assertEquals(expectedRecord, newTargetAssignmentEpochRecord(
            "group-id",
            10
        ));
    }

    @Test
    public void testNewTargetAssignmentEpochTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataKey()
                    .setGroupId("group-id"),
                (short) 6),
            null);

        assertEquals(expectedRecord, newTargetAssignmentEpochTombstoneRecord(
            "group-id"
        ));
    }

    @Test
    public void testNewCurrentAssignmentRecord() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        Map<Uuid, Set<Integer>> assigned = mkSortedAssignment(
            mkSortedTopicAssignment(topicId1, 11, 12, 13),
            mkSortedTopicAssignment(topicId2, 21, 22, 23)
        );

        Map<Uuid, Set<Integer>> revoking = mkSortedAssignment(
            mkSortedTopicAssignment(topicId1, 14, 15, 16),
            mkSortedTopicAssignment(topicId2, 24, 25, 26)
        );

        Map<Uuid, Set<Integer>> assigning = mkSortedAssignment(
            mkSortedTopicAssignment(topicId1, 17, 18, 19),
            mkSortedTopicAssignment(topicId2, 27, 28, 29)
        );

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 8),
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentValue()
                    .setMemberEpoch(22)
                    .setPreviousMemberEpoch(21)
                    .setTargetMemberEpoch(23)
                    .setAssignedPartitions(Arrays.asList(
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId1)
                            .setPartitions(Arrays.asList(11, 12, 13)),
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId2)
                            .setPartitions(Arrays.asList(21, 22, 23))))
                    .setPartitionsPendingRevocation(Arrays.asList(
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId1)
                            .setPartitions(Arrays.asList(14, 15, 16)),
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId2)
                            .setPartitions(Arrays.asList(24, 25, 26))))
                    .setPartitionsPendingAssignment(Arrays.asList(
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId1)
                            .setPartitions(Arrays.asList(17, 18, 19)),
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId2)
                            .setPartitions(Arrays.asList(27, 28, 29)))),
                (short) 0));

        assertEquals(expectedRecord, newCurrentAssignmentRecord(
            "group-id",
            new ConsumerGroupMember.Builder("member-id")
                .setMemberEpoch(22)
                .setPreviousMemberEpoch(21)
                .setTargetMemberEpoch(23)
                .setAssignedPartitions(assigned)
                .setPartitionsPendingRevocation(revoking)
                .setPartitionsPendingAssignment(assigning)
                .build()
        ));
    }

    @Test
    public void testNewCurrentAssignmentTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 8),
            null);

        assertEquals(expectedRecord, newCurrentAssignmentTombstoneRecord(
            "group-id",
            "member-id"
        ));
    }

    private static Stream<Arguments> metadataToExpectedGroupMetadataValue() {
        return Stream.of(
            Arguments.arguments(MetadataVersion.IBP_0_10_0_IV0, (short) 0),
            Arguments.arguments(MetadataVersion.IBP_1_1_IV0, (short) 1),
            Arguments.arguments(MetadataVersion.IBP_2_2_IV0, (short) 2),
            Arguments.arguments(MetadataVersion.IBP_3_5_IV0, (short) 3)
        );
    }

    @ParameterizedTest
    @MethodSource("metadataToExpectedGroupMetadataValue")
    public void testNewGroupMetadataRecord(
        MetadataVersion metadataVersion,
        short expectedGroupMetadataValueVersion
    ) {
        Time time = new MockTime();

        List<GroupMetadataValue.MemberMetadata> expectedMembers = new ArrayList<>();
        expectedMembers.add(
            new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-1")
                .setClientId("client-1")
                .setClientHost("host-1")
                .setRebalanceTimeout(1000)
                .setSessionTimeout(1500)
                .setGroupInstanceId("group-instance-1")
                .setSubscription(new byte[]{0, 1})
                .setAssignment(new byte[]{1, 2})
        );

        expectedMembers.add(
            new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-2")
                .setClientId("client-2")
                .setClientHost("host-2")
                .setRebalanceTimeout(1000)
                .setSessionTimeout(1500)
                .setGroupInstanceId("group-instance-2")
                .setSubscription(new byte[]{1, 2})
                .setAssignment(new byte[]{2, 3})
        );

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new GroupMetadataKey()
                    .setGroup("group-id"),
                (short) 2),
            new ApiMessageAndVersion(
                new GroupMetadataValue()
                    .setProtocol("range")
                    .setProtocolType("consumer")
                    .setLeader("member-1")
                    .setGeneration(1)
                    .setCurrentStateTimestamp(time.milliseconds())
                    .setMembers(expectedMembers),
                expectedGroupMetadataValueVersion));

        GenericGroup group = new GenericGroup(
            new LogContext(),
            "group-id",
            GenericGroupState.PREPARING_REBALANCE,
            time
        );

        Map<String, byte[]> assignment = new HashMap<>();

        expectedMembers.forEach(member -> {
            JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
            protocols.add(new JoinGroupRequestProtocol()
                .setName("range")
                .setMetadata(member.subscription()));

            group.add(new GenericGroupMember(
                member.memberId(),
                Optional.of(member.groupInstanceId()),
                member.clientId(),
                member.clientHost(),
                member.rebalanceTimeout(),
                member.sessionTimeout(),
                "consumer",
                protocols,
                GenericGroupMember.EMPTY_ASSIGNMENT
            ));

            assignment.put(member.memberId(), member.assignment());
        });

        group.initNextGeneration();
        Record groupMetadataRecord = RecordHelpers.newGroupMetadataRecord(
            group,
            assignment,
            metadataVersion
        );

        assertEquals(expectedRecord, groupMetadataRecord);
    }

    @Test
    public void testNewGroupMetadataTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new GroupMetadataKey()
                    .setGroup("group-id"),
                (short) 2),
            null);

        Record groupMetadataRecord = RecordHelpers.newGroupMetadataTombstoneRecord("group-id");
        assertEquals(expectedRecord, groupMetadataRecord);
    }

    @Test
    public void testNewGroupMetadataRecordThrowsWhenNullSubscription() {
        Time time = new MockTime();

        List<GroupMetadataValue.MemberMetadata> expectedMembers = new ArrayList<>();
        expectedMembers.add(
            new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-1")
                .setClientId("client-1")
                .setClientHost("host-1")
                .setRebalanceTimeout(1000)
                .setSessionTimeout(1500)
                .setGroupInstanceId("group-instance-1")
                .setSubscription(new byte[]{0, 1})
                .setAssignment(new byte[]{1, 2})
        );

        GenericGroup group = new GenericGroup(
            new LogContext(),
            "group-id",
            GenericGroupState.PREPARING_REBALANCE,
            time
        );

        expectedMembers.forEach(member -> {
            JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
            protocols.add(new JoinGroupRequestProtocol()
                .setName("range")
                .setMetadata(null));

            group.add(new GenericGroupMember(
                member.memberId(),
                Optional.of(member.groupInstanceId()),
                member.clientId(),
                member.clientHost(),
                member.rebalanceTimeout(),
                member.sessionTimeout(),
                "consumer",
                protocols,
                member.assignment()
            ));
        });

        assertThrows(IllegalStateException.class, () ->
            RecordHelpers.newGroupMetadataRecord(
                group,
                Collections.emptyMap(),
                MetadataVersion.IBP_3_5_IV2
            ));
    }

    @Test
    public void testNewGroupMetadataRecordThrowsWhenEmptyAssignment() {
        Time time = new MockTime();

        List<GroupMetadataValue.MemberMetadata> expectedMembers = new ArrayList<>();
        expectedMembers.add(
            new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-1")
                .setClientId("client-1")
                .setClientHost("host-1")
                .setRebalanceTimeout(1000)
                .setSessionTimeout(1500)
                .setGroupInstanceId("group-instance-1")
                .setSubscription(new byte[]{0, 1})
                .setAssignment(null)
        );

        GenericGroup group = new GenericGroup(
            new LogContext(),
            "group-id",
            GenericGroupState.PREPARING_REBALANCE,
            time
        );

        expectedMembers.forEach(member -> {
            JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
            protocols.add(new JoinGroupRequestProtocol()
                .setName("range")
                .setMetadata(member.subscription()));

            group.add(new GenericGroupMember(
                member.memberId(),
                Optional.of(member.groupInstanceId()),
                member.clientId(),
                member.clientHost(),
                member.rebalanceTimeout(),
                member.sessionTimeout(),
                "consumer",
                protocols,
                member.assignment()
            ));
        });

        assertThrows(IllegalStateException.class, () ->
            RecordHelpers.newGroupMetadataRecord(
                group,
                Collections.emptyMap(),
                MetadataVersion.IBP_3_5_IV2
            ));
    }
      
    @ParameterizedTest
    @MethodSource("metadataToExpectedGroupMetadataValue")
    public void testEmptyGroupMetadataRecord(
        MetadataVersion metadataVersion,
        short expectedGroupMetadataValueVersion
    ) {
        Time time = new MockTime();

        List<GroupMetadataValue.MemberMetadata> expectedMembers = Collections.emptyList();

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new GroupMetadataKey()
                    .setGroup("group-id"),
                (short) 2),
            new ApiMessageAndVersion(
                new GroupMetadataValue()
                    .setProtocol(null)
                    .setProtocolType("")
                    .setLeader(null)
                    .setGeneration(0)
                    .setCurrentStateTimestamp(time.milliseconds())
                    .setMembers(expectedMembers),
                expectedGroupMetadataValueVersion));

        GenericGroup group = new GenericGroup(
            new LogContext(),
            "group-id",
            GenericGroupState.PREPARING_REBALANCE,
            time
        );

        group.initNextGeneration();
        Record groupMetadataRecord = RecordHelpers.newEmptyGroupMetadataRecord(
            group,
            metadataVersion
        );

        assertEquals(expectedRecord, groupMetadataRecord);
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testNewOffsetCommitRecord(MetadataVersion metadataVersion) {
        OffsetCommitKey key = new OffsetCommitKey()
            .setGroup("group-id")
            .setTopic("foo")
            .setPartition(1);
        OffsetCommitValue value = new OffsetCommitValue()
            .setOffset(100L)
            .setLeaderEpoch(10)
            .setMetadata("metadata")
            .setCommitTimestamp(1234L)
            .setExpireTimestamp(-1L);

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                key,
                (short) 1),
            new ApiMessageAndVersion(
                value,
                metadataVersion.offsetCommitValueVersion(false)
            )
        );

        assertEquals(expectedRecord, RecordHelpers.newOffsetCommitRecord(
            "group-id",
            "foo",
            1,
            new OffsetAndMetadata(
                100L,
                OptionalInt.of(10),
                "metadata",
                1234L,
                OptionalLong.empty()),
            metadataVersion
        ));

        value.setLeaderEpoch(-1);

        assertEquals(expectedRecord, RecordHelpers.newOffsetCommitRecord(
            "group-id",
            "foo",
            1,
            new OffsetAndMetadata(
                100L,
                OptionalInt.empty(),
                "metadata",
                1234L,
                OptionalLong.empty()),
            metadataVersion
        ));
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testNewOffsetCommitRecordWithExpireTimestamp(MetadataVersion metadataVersion) {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new OffsetCommitKey()
                    .setGroup("group-id")
                    .setTopic("foo")
                    .setPartition(1),
                (short) 1),
            new ApiMessageAndVersion(
                new OffsetCommitValue()
                    .setOffset(100L)
                    .setLeaderEpoch(10)
                    .setMetadata("metadata")
                    .setCommitTimestamp(1234L)
                    .setExpireTimestamp(5678L),
                (short) 1 // When expire timestamp is set, it is always version 1.
            )
        );

        assertEquals(expectedRecord, RecordHelpers.newOffsetCommitRecord(
            "group-id",
            "foo",
            1,
            new OffsetAndMetadata(
                100L,
                OptionalInt.of(10),
                "metadata",
                1234L,
                OptionalLong.of(5678L)),
            metadataVersion
        ));
    }

    @Test
    public void testNewOffsetCommitTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new OffsetCommitKey()
                    .setGroup("group-id")
                    .setTopic("foo")
                    .setPartition(1),
                (short) 1),
            null);

        Record record = RecordHelpers.newOffsetCommitTombstoneRecord("group-id", "foo", 1);
        assertEquals(expectedRecord, record);
    }

    /**
     * Creates a list of values to be added to the record and assigns partitions to racks for testing.
     *
     * @param numPartitions The number of partitions for the topic.
     *
     * For testing purposes, the following criteria are used:
     *      - Number of replicas for each partition: 2
     *      - Number of racks available to the cluster: 4
     */
    public static List<ConsumerGroupPartitionMetadataValue.PartitionMetadata> mkListOfPartitionRacks(int numPartitions) {
        List<ConsumerGroupPartitionMetadataValue.PartitionMetadata> partitionRacks = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            List<String> racks = new ArrayList<>(Arrays.asList("rack" + i % 4, "rack" + (i + 1) % 4));
            partitionRacks.add(
                new ConsumerGroupPartitionMetadataValue.PartitionMetadata()
                    .setPartition(i)
                    .setRacks(racks)
            );
        }
        return partitionRacks;
    }

    /**
     * Creates a map of partitions to racks for testing.
     *
     * @param numPartitions The number of partitions for the topic.
     *
     * For testing purposes, the following criteria are used:
     *      - Number of replicas for each partition: 2
     *      - Number of racks available to the cluster: 4
     */
    public static Map<Integer, Set<String>> mkMapOfPartitionRacks(int numPartitions) {
        Map<Integer, Set<String>> partitionRacks = new HashMap<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            partitionRacks.put(i, new HashSet<>(Arrays.asList("rack" + i % 4, "rack" + (i + 1) % 4)));
        }
        return partitionRacks;
    }

    /**
     * Asserts whether the two provided lists of records are equal.
     *
     * @param expectedRecords   The expected list of records.
     * @param actualRecords     The actual list of records.
     */
    public static void assertRecordsEquals(
        List<Record> expectedRecords,
        List<Record> actualRecords
    ) {
        try {
            assertEquals(expectedRecords.size(), actualRecords.size());

            for (int i = 0; i < expectedRecords.size(); i++) {
                Record expectedRecord = expectedRecords.get(i);
                Record actualRecord = actualRecords.get(i);
                assertRecordEquals(expectedRecord, actualRecord);
            }
        } catch (AssertionFailedError e) {
            assertionFailure()
                .expected(expectedRecords)
                .actual(actualRecords)
                .buildAndThrow();
        }
    }

    /**
     * Asserts if the two provided records are equal.
     *
     * @param expectedRecord   The expected record.
     * @param actualRecord     The actual record.
     */
    public static void assertRecordEquals(
        Record expectedRecord,
        Record actualRecord
    ) {
        try {
            assertApiMessageAndVersionEquals(expectedRecord.key(), actualRecord.key());
            assertApiMessageAndVersionEquals(expectedRecord.value(), actualRecord.value());
        } catch (AssertionFailedError e) {
            assertionFailure()
                .expected(expectedRecord)
                .actual(actualRecord)
                .buildAndThrow();
        }
    }

    private static void assertApiMessageAndVersionEquals(
        ApiMessageAndVersion expected,
        ApiMessageAndVersion actual
    ) {
        if (expected == actual) return;

        assertEquals(expected.version(), actual.version());

        if (actual.message() instanceof ConsumerGroupCurrentMemberAssignmentValue) {
            // The order of the topics stored in ConsumerGroupCurrentMemberAssignmentValue is not
            // always guaranteed. Therefore, we need a special comparator.
            ConsumerGroupCurrentMemberAssignmentValue expectedValue =
                (ConsumerGroupCurrentMemberAssignmentValue) expected.message();
            ConsumerGroupCurrentMemberAssignmentValue actualValue =
                (ConsumerGroupCurrentMemberAssignmentValue) actual.message();

            assertEquals(expectedValue.memberEpoch(), actualValue.memberEpoch());
            assertEquals(expectedValue.previousMemberEpoch(), actualValue.previousMemberEpoch());
            assertEquals(expectedValue.targetMemberEpoch(), actualValue.targetMemberEpoch());
            assertEquals(expectedValue.error(), actualValue.error());
            assertEquals(expectedValue.metadataVersion(), actualValue.metadataVersion());
            assertEquals(expectedValue.metadataBytes(), actualValue.metadataBytes());

            // We transform those to Maps before comparing them.
            assertEquals(fromTopicPartitions(expectedValue.assignedPartitions()),
                fromTopicPartitions(actualValue.assignedPartitions()));
            assertEquals(fromTopicPartitions(expectedValue.partitionsPendingRevocation()),
                fromTopicPartitions(actualValue.partitionsPendingRevocation()));
            assertEquals(fromTopicPartitions(expectedValue.partitionsPendingAssignment()),
                fromTopicPartitions(actualValue.partitionsPendingAssignment()));
        } else if (actual.message() instanceof ConsumerGroupPartitionMetadataValue) {
            // The order of the racks stored in the PartitionMetadata of the ConsumerGroupPartitionMetadataValue
            // is not always guaranteed. Therefore, we need a special comparator.
            ConsumerGroupPartitionMetadataValue expectedValue =
                (ConsumerGroupPartitionMetadataValue) expected.message();
            ConsumerGroupPartitionMetadataValue actualValue =
                (ConsumerGroupPartitionMetadataValue) actual.message();

            List<ConsumerGroupPartitionMetadataValue.TopicMetadata> expectedTopicMetadataList =
                expectedValue.topics();
            List<ConsumerGroupPartitionMetadataValue.TopicMetadata> actualTopicMetadataList =
                actualValue.topics();

            if (expectedTopicMetadataList.size() != actualTopicMetadataList.size()) {
                fail("Topic metadata lists have different sizes");
            }

            for (int i = 0; i < expectedTopicMetadataList.size(); i++) {
                ConsumerGroupPartitionMetadataValue.TopicMetadata expectedTopicMetadata =
                    expectedTopicMetadataList.get(i);
                ConsumerGroupPartitionMetadataValue.TopicMetadata actualTopicMetadata =
                    actualTopicMetadataList.get(i);

                assertEquals(expectedTopicMetadata.topicId(), actualTopicMetadata.topicId());
                assertEquals(expectedTopicMetadata.topicName(), actualTopicMetadata.topicName());
                assertEquals(expectedTopicMetadata.numPartitions(), actualTopicMetadata.numPartitions());

                List<ConsumerGroupPartitionMetadataValue.PartitionMetadata> expectedPartitionMetadataList =
                    expectedTopicMetadata.partitionMetadata();
                List<ConsumerGroupPartitionMetadataValue.PartitionMetadata> actualPartitionMetadataList =
                    actualTopicMetadata.partitionMetadata();

                // If the list is empty, rack information wasn't available for any replica of
                // the partition and hence, the entry wasn't added to the record.
                if (expectedPartitionMetadataList.size() != actualPartitionMetadataList.size()) {
                    fail("Partition metadata lists have different sizes");
                } else if (!expectedPartitionMetadataList.isEmpty() && !actualPartitionMetadataList.isEmpty()) {
                    for (int j = 0; j < expectedTopicMetadataList.size(); j++) {
                        ConsumerGroupPartitionMetadataValue.PartitionMetadata expectedPartitionMetadata =
                            expectedPartitionMetadataList.get(j);
                        ConsumerGroupPartitionMetadataValue.PartitionMetadata actualPartitionMetadata =
                            actualPartitionMetadataList.get(j);

                        assertEquals(expectedPartitionMetadata.partition(), actualPartitionMetadata.partition());
                        assertUnorderedListEquals(expectedPartitionMetadata.racks(), actualPartitionMetadata.racks());
                    }
                }
            }
        } else {
            assertEquals(expected.message(), actual.message());
        }
    }

    private static Map<Uuid, Set<Integer>> fromTopicPartitions(
        List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> assignment
    ) {
        Map<Uuid, Set<Integer>> assignmentMap = new HashMap<>();
        assignment.forEach(topicPartitions ->
            assignmentMap.put(topicPartitions.topicId(), new HashSet<>(topicPartitions.partitions())));
        return assignmentMap;
    }
}
