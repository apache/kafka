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

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidRegularExpression;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupBuilder;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.InitMapValue;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupBuilder;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsGroupBuilder;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.TasksTuple;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordsEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertResponseEquals;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupRebalanceTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupSessionTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ADDRESS;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ID;
import static org.apache.kafka.coordinator.group.Utils.computeGroupHash;
import static org.apache.kafka.coordinator.group.Utils.computeTopicHash;
import static org.apache.kafka.coordinator.group.Utils.toAssignmentWithEpochs;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.DEAD;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.SHARE_GROUP_REBALANCES_SENSOR_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link GroupMetadataManager} behaviour that is genuinely group-type-agnostic:
 * member-id generation, metadata-image plumbing, list/describe, dynamic broker/group
 * config wiring, and isolation tests that verify one group type's API correctly rejects
 * operations on a different group type ({@code testFooGroup*On*Group}).
 *
 * <p>If a new test exercises a single group type ({@code Consumer}, {@code Classic},
 * {@code Share}, {@code Streams}), put it in the corresponding
 * {@code GroupMetadataManager<Type>GroupTest} class instead.
 */
public class GroupMetadataManagerTest {

    @Test
    public void testConsumerHeartbeatRegexValidation() {
        String memberId = Uuid.randomUuid().toString();
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        // Subscribing with an invalid regular expression fails.
        Exception ex = assertThrows(InvalidRegularExpression.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(Uuid.randomUuid().toString())
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("[")
                .setTopicPartitions(List.of())));
        assertEquals("SubscribedTopicRegex `[` is not a valid regular expression: missing closing ].", ex.getMessage());

        // Subscribing with a valid regular expression succeeds.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex(".*")
                .setTopicPartitions(List.of()));
        assertEquals(1, result.response().memberEpoch());

        // Updating the subscription to an invalid regular expression fails.
        assertThrows(InvalidRegularExpression.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("[")
                .setTopicPartitions(List.of())));
        assertEquals("SubscribedTopicRegex `[` is not a valid regular expression: missing closing ].", ex.getMessage());

        // Updating the subscription to topic names succeeds (checking when the regex becomes null).
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(List.of()));
        assertEquals(2, result.response().memberEpoch());
    }

    @Test
    public void testMemberIdGeneration() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of()
        ));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            // The consumer generates its own Member ID starting from version 1 of the ConsumerGroupHeartbeat RPC.
            // Therefore, this test case is specific to earlier versions of the RPC.
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("group-foo")
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()),
            (short) 0
        );

        // Verify that a member id was generated for the new member.
        String memberId = result.response().memberId();
        assertNotNull(memberId);
        assertNotEquals("", memberId);

        // The response should get a bumped epoch and should not
        // contain any assignment because we did not provide
        // topics metadata.
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );
    }

    @Test
    public void testUnknownGroupId() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        assertThrows(GroupIdNotFoundException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(100) // Epoch must be > 0.
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setTopicPartitions(List.of())));
    }

    @Test
    public void testTopicHashIsRemoveFromCacheIfNoGroupSubscribesIt() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addRacks()
            .buildCoordinatorMetadataImage();
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 2))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 2, computeGroupHash(Map.of(
                fooTopicName, fooTopicHash
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
        assertEquals(Map.of(fooTopicName, fooTopicHash), context.groupMetadataManager.topicHashCache());

        // Use LEAVE_GROUP_MEMBER_EPOCH to leave group, so there is no group subscribes to foo.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(-1)
                .setHeartbeatIntervalMs(0),
            result.response()
        );

        expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 3, 0),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 3, 0L)
        );
        assertRecordsEquals(expectedRecords, result.records());
        assertEquals(Map.of(), context.groupMetadataManager.topicHashCache());
    }

    @Test
    public void testNewRacksDataInMetadataImageTriggersEpochBump() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .build();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 10))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage)))
                )))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Update metadata image with racks.
        CoordinatorMetadataImage newMetadataImage = new MetadataImageBuilder(metadataImage)
            .addTopic(fooTopicId, fooTopicName, 6)
            .addRacks()
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(
            newMetadataImage.emptyDelta(), newMetadataImage
        );
        // If a topic is updated, related topic hash is cleanup.
        assertEquals(Map.of(), context.groupMetadataManager.topicHashCache());

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 10))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, newMetadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testRemoveTopicCleanupTopicHash() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .build();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 10))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName,
                    computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage)))
                )))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(Map.of()))
        ));

        // Remove foo topic from metadata image.
        MetadataDelta delta = new MetadataDelta.Builder()
            .setImage(metadataImage)
            .build();
        delta.replay(new RemoveTopicRecord().setTopicId(fooTopicId));
        MetadataImage newMetadataImage = delta.apply(MetadataProvenance.EMPTY);

        context.groupMetadataManager.onMetadataUpdate(
            new KRaftCoordinatorMetadataDelta(new MetadataDelta.Builder()
                .setImage(newMetadataImage)
                .build()), new KRaftCoordinatorMetadataImage(newMetadataImage)
        );
        // If a topic is removed, related topic hash is cleanup.
        assertEquals(Map.of(), context.groupMetadataManager.topicHashCache());

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setPartitionsPendingRevocation(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 10))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, 0),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, Map.of()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testSubscriptionUpgradeToMetadataHash() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addRacks()
            .build();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 10))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        ConsumerGroupPartitionMetadataValue consumerGroupPartitionMetadataValue = new ConsumerGroupPartitionMetadataValue();
        consumerGroupPartitionMetadataValue.topics().add(new ConsumerGroupPartitionMetadataValue.TopicMetadata()
            .setTopicId(fooTopicId)
            .setTopicName(fooTopicName)
            .setNumPartitions(6));
        context.replay(CoordinatorRecord.record(
            new ConsumerGroupPartitionMetadataKey().setGroupId(groupId),
            new ApiMessageAndVersion(consumerGroupPartitionMetadataValue, (short) 0)
        ));
        context.commit();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 10))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage))
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testGroupIdsByTopics() {
        String groupId1 = "group1";
        String groupId2 = "group2";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        assertEquals(Set.of(), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 1 subscribes to foo and bar.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m1")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .build()));

        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 2 subscribes to foo, bar and zar.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 1 subscribes to bar and zar.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m2")
                .setSubscribedTopicNames(List.of("bar", "zar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to foo and bar.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 1 is removed.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId1, "group1-m1"));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId1, "group1-m1"));

        assertEquals(Set.of(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M1 in group 2 subscribes to nothing.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(List.of())
                .build()));

        assertEquals(Set.of(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to foo.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(List.of("foo"))
                .build()));

        assertEquals(Set.of(groupId2), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to nothing.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(List.of())
                .build()));

        assertEquals(Set.of(), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), context.groupMetadataManager.groupsSubscribedToTopic("zar"));

        // M2 in group 1 subscribes to nothing.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m2")
                .setSubscribedTopicNames(List.of())
                .build()));

        assertEquals(Set.of(), context.groupMetadataManager.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(), context.groupMetadataManager.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(), context.groupMetadataManager.groupsSubscribedToTopic("zar"));
    }

    @Test
    public void testOnMetadataUpdateWithEmptyDelta() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new MockPartitionAssignor("range")))
            .build();

        MetadataDelta delta = new MetadataDelta.Builder()
            .setImage(MetadataImage.EMPTY)
            .build();
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);

        context.groupMetadataManager.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(delta), new KRaftCoordinatorMetadataImage(image));
        assertEquals(new KRaftCoordinatorMetadataImage(image), context.groupMetadataManager.image());
    }

    @Test
    public void testOnMetadataUpdate() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // M1 in group 1 subscribes to a and b.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group1",
            new ConsumerGroupMember.Builder("group1-m1")
                .setSubscribedTopicNames(List.of("a", "b"))
                .build()));

        // M1 in group 2 subscribes to b and c.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group2",
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(List.of("b", "c"))
                .build()));

        // M1 in group 3 subscribes to d.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group3",
            new ConsumerGroupMember.Builder("group3-m1")
                .setSubscribedTopicNames(List.of("d"))
                .build()));

        // M1 in group 4 subscribes to e.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group4",
            new ConsumerGroupMember.Builder("group4-m1")
                .setSubscribedTopicNames(List.of("e"))
                .build()));

        // M1 in group 5 subscribes to f.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("group5",
            new ConsumerGroupMember.Builder("group5-m1")
                .setSubscribedTopicNames(List.of("f"))
                .build()));

        // Ensures that all refresh flags are set to the future.
        List.of("group1", "group2", "group3", "group4", "group5").forEach(groupId -> {
            ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
            group.setMetadataRefreshDeadline(context.time.milliseconds() + 5000L, 0);
            assertFalse(group.hasMetadataExpired(context.time.milliseconds()));
        });

        // Update the metadata image.
        Uuid topicA = Uuid.randomUuid();
        Uuid topicB = Uuid.randomUuid();
        Uuid topicC = Uuid.randomUuid();
        Uuid topicD = Uuid.randomUuid();
        Uuid topicE = Uuid.randomUuid();

        // Create a first base image with topic a, b, c and d.
        MetadataDelta delta = new MetadataDelta.Builder()
            .setImage(MetadataImage.EMPTY)
            .build();
        delta.replay(new TopicRecord().setTopicId(topicA).setName("a"));
        delta.replay(new PartitionRecord().setTopicId(topicA).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicB).setName("b"));
        delta.replay(new PartitionRecord().setTopicId(topicB).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicC).setName("c"));
        delta.replay(new PartitionRecord().setTopicId(topicC).setPartitionId(0));
        delta.replay(new TopicRecord().setTopicId(topicD).setName("d"));
        delta.replay(new PartitionRecord().setTopicId(topicD).setPartitionId(0));
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);

        // Create a delta which updates topic B, deletes topic D and creates topic E.
        delta = new MetadataDelta.Builder()
            .setImage(image)
            .build();
        delta.replay(new PartitionRecord().setTopicId(topicB).setPartitionId(2));
        delta.replay(new RemoveTopicRecord().setTopicId(topicD));
        delta.replay(new TopicRecord().setTopicId(topicE).setName("e"));
        delta.replay(new PartitionRecord().setTopicId(topicE).setPartitionId(1));
        image = delta.apply(MetadataProvenance.EMPTY);

        // Update metadata image with the delta.
        context.groupMetadataManager.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(delta), new KRaftCoordinatorMetadataImage(image));

        // Verify the groups.
        List.of("group1", "group2", "group3", "group4").forEach(groupId -> {
            ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
            assertTrue(group.hasMetadataExpired(context.time.milliseconds()), groupId);
        });

        List.of("group5").forEach(groupId -> {
            ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
            assertFalse(group.hasMetadataExpired(context.time.milliseconds()));
        });

        // Verify image.
        assertEquals(new KRaftCoordinatorMetadataImage(image), context.groupMetadataManager.image());
    }

    @Test
    public void testOnLoaded() {
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .build();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .withConsumerGroup(new ConsumerGroupBuilder("foo", 10)
                .withMember(new ConsumerGroupMember.Builder("foo-1")
                    .setState(MemberState.UNREVOKED_PARTITIONS)
                    .setMemberEpoch(9)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
                    .setPartitionsPendingRevocation(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5)), 9))
                    .build())
                .withMember(new ConsumerGroupMember.Builder("foo-2")
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .build())
                .withAssignment("foo-1", mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage))
                ))))
            .build();

        // Let's assume that all the records have been replayed and now
        // onLoaded is called to signal it.
        context.groupMetadataManager.onLoaded();

        // All members should have a session timeout in place.
        assertNotNull(context.timer.timeout(groupSessionTimeoutKey("foo", "foo-1")));
        assertNotNull(context.timer.timeout(groupSessionTimeoutKey("foo", "foo-2")));

        // foo-1 should also have a revocation timeout in place.
        assertNotNull(context.timer.timeout(groupRebalanceTimeoutKey("foo", "foo-1")));
    }

    @Test
    public void testUpdateGroupSizeCounter() {
        List<String> groupIds = new ArrayList<>();
        IntStream.range(0, 8).forEach(i -> groupIds.add("group-" + i));
        List<String> consumerMemberIds = List.of("consumer-member-id-0", "consumer-member-id-1", "consumer-member-id-2");

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupIds.get(0), 10)) // Empty group
            .withConsumerGroup(new ConsumerGroupBuilder(groupIds.get(1), 10) // Stable group
                .withAssignmentEpoch(10)
                .withMember(new ConsumerGroupMember.Builder(consumerMemberIds.get(0))
                    .setMemberEpoch(10)
                    .build()))
            .withConsumerGroup(new ConsumerGroupBuilder(groupIds.get(2), 10) // Assigning group
                .withAssignmentEpoch(9)
                .withMember(new ConsumerGroupMember.Builder(consumerMemberIds.get(1))
                    .setMemberEpoch(9)
                    .build()))
            .withConsumerGroup(new ConsumerGroupBuilder(groupIds.get(3), 10) // Reconciling group
                .withAssignmentEpoch(10)
                .withMember(new ConsumerGroupMember.Builder(consumerMemberIds.get(2))
                    .setMemberEpoch(9)
                    .build()))
            .build();

        ClassicGroup group4 = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupIds.get(4), true);
        ClassicGroup group5 = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupIds.get(5), true);
        ClassicGroup group6 = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupIds.get(6), true);
        ClassicGroup group7 = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupIds.get(7), true);

        context.groupMetadataManager.updateGroupSizeCounter();
        verify(context.metrics, times(1)).setClassicGroupGauges(eq(Utils.mkMap(
            Utils.mkEntry(ClassicGroupState.EMPTY, 4L)
        )));
        verify(context.metrics, times(1)).setConsumerGroupGauges(eq(Utils.mkMap(
            Utils.mkEntry(ConsumerGroup.ConsumerGroupState.EMPTY, 1L),
            Utils.mkEntry(ConsumerGroup.ConsumerGroupState.ASSIGNING, 1L),
            Utils.mkEntry(ConsumerGroup.ConsumerGroupState.RECONCILING, 1L),
            Utils.mkEntry(ConsumerGroup.ConsumerGroupState.STABLE, 1L)
        )));

        group4.transitionTo(PREPARING_REBALANCE);
        group5.transitionTo(PREPARING_REBALANCE);
        group5.transitionTo(COMPLETING_REBALANCE);
        group6.transitionTo(PREPARING_REBALANCE);
        group6.transitionTo(COMPLETING_REBALANCE);
        group6.transitionTo(STABLE);
        group7.transitionTo(DEAD);

        context.groupMetadataManager.getOrMaybeCreateConsumerGroup(groupIds.get(1), false, List.of())
            .removeMember(consumerMemberIds.get(0));
        context.groupMetadataManager.getOrMaybeCreateConsumerGroup(groupIds.get(3), false, List.of())
            .updateMember(new ConsumerGroupMember.Builder(consumerMemberIds.get(2)).setMemberEpoch(10).build());

        context.groupMetadataManager.updateGroupSizeCounter();
        verify(context.metrics, times(1)).setClassicGroupGauges(eq(Utils.mkMap(
            Utils.mkEntry(ClassicGroupState.PREPARING_REBALANCE, 1L),
            Utils.mkEntry(ClassicGroupState.COMPLETING_REBALANCE, 1L),
            Utils.mkEntry(ClassicGroupState.STABLE, 1L),
            Utils.mkEntry(ClassicGroupState.DEAD, 1L)
        )));
        verify(context.metrics, times(1)).setConsumerGroupGauges(eq(Utils.mkMap(
            Utils.mkEntry(ConsumerGroup.ConsumerGroupState.EMPTY, 2L),
            Utils.mkEntry(ConsumerGroup.ConsumerGroupState.ASSIGNING, 1L),
            Utils.mkEntry(ConsumerGroup.ConsumerGroupState.STABLE, 1L)
        )));
    }

    @Test
    public void testListGroups() {
        String consumerGroupId = "consumer-group-id";
        String classicGroupId = "classic-group-id";
        String shareGroupId = "share-group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withShareGroupAssignor(assignor)
            .withConsumerGroup(new ConsumerGroupBuilder(consumerGroupId, 10))
            .build();

        // Create one classic group record.
        context.replay(GroupMetadataManagerTestContext.newGroupMetadataRecord(
            classicGroupId,
            new GroupMetadataValue()
                .setMembers(List.of())
                .setGeneration(2)
                .setLeader(null)
                .setProtocolType("classic")
                .setProtocol("range")
                .setCurrentStateTimestamp(context.time.milliseconds())));
        // Create one share group record.
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(shareGroupId, 6, 0));
        context.commit();
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(classicGroupId, false);
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(consumerGroupId, new ConsumerGroupMember.Builder(memberId1)
            .setSubscribedTopicNames(List.of(fooTopicName))
            .build()));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(consumerGroupId, 11, 0));

        // Test list group response without a group state or group type filter.
        Map<String, ListGroupsResponseData.ListedGroup> actualAllGroupMap =
            context.sendListGroups(List.of(), List.of()).stream()
                .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        Map<String, ListGroupsResponseData.ListedGroup> expectAllGroupMap =
            Stream.of(
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(classicGroup.groupId())
                    .setProtocolType("classic")
                    .setGroupState(EMPTY.toString())
                    .setGroupType(Group.GroupType.CLASSIC.toString()),
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(consumerGroupId)
                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                    .setGroupState(ConsumerGroup.ConsumerGroupState.EMPTY.toString())
                    .setGroupType(Group.GroupType.CONSUMER.toString()),
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(shareGroupId)
                    .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                    .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                    .setGroupType(Group.GroupType.SHARE.toString())
            ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // List group with case-insensitive ‘empty’.
        actualAllGroupMap =
            context.sendListGroups(List.of("empty"), List.of())
                .stream().collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        context.commit();

        // Test list group response to check assigning state in the consumer group.
        actualAllGroupMap = context.sendListGroups(List.of("assigning"), List.of()).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap =
            Stream.of(
                new ListGroupsResponseData.ListedGroup()
                    .setGroupId(consumerGroupId)
                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                    .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
                    .setGroupType(Group.GroupType.CONSUMER.toString())
            ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with group state filter and no group type filter.
        actualAllGroupMap = context.sendListGroups(List.of("Empty"), List.of()).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(classicGroup.groupId())
                .setProtocolType("classic")
                .setGroupState(EMPTY.toString())
                .setGroupType(Group.GroupType.CLASSIC.toString()),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(shareGroupId)
                .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                .setGroupType(Group.GroupType.SHARE.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with no group state filter and with group type filter.
        actualAllGroupMap = context.sendListGroups(List.of(), List.of(Group.GroupType.CLASSIC.toString())).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(classicGroup.groupId())
                .setProtocolType("classic")
                .setGroupState(EMPTY.toString())
                .setGroupType(Group.GroupType.CLASSIC.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with no group state filter and with group type filter in a different case.
        actualAllGroupMap = context.sendListGroups(List.of(), List.of("Consumer")).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(consumerGroupId)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
                .setGroupType(Group.GroupType.CONSUMER.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        actualAllGroupMap = context.sendListGroups(List.of(), List.of("Share")).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(shareGroupId)
                .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                .setGroupType(Group.GroupType.SHARE.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        actualAllGroupMap = context.sendListGroups(List.of("empty", "Assigning"), List.of()).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Stream.of(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(classicGroup.groupId())
                .setProtocolType(Group.GroupType.CLASSIC.toString())
                .setGroupState(EMPTY.toString())
                .setGroupType(Group.GroupType.CLASSIC.toString()),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(consumerGroupId)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
                .setGroupType(Group.GroupType.CONSUMER.toString()),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId(shareGroupId)
                .setProtocolType(ShareGroup.PROTOCOL_TYPE)
                .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                .setGroupType(Group.GroupType.SHARE.toString())
        ).collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with no group state filter and with invalid group type filter .
        actualAllGroupMap = context.sendListGroups(List.of(), List.of("Invalid")).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Map.of();

        assertEquals(expectAllGroupMap, actualAllGroupMap);

        // Test list group response with invalid group state filter and with no group type filter .
        actualAllGroupMap = context.sendListGroups(List.of("Invalid"), List.of()).stream()
            .collect(Collectors.toMap(ListGroupsResponseData.ListedGroup::groupId, Function.identity()));
        expectAllGroupMap = Map.of();

        assertEquals(expectAllGroupMap, actualAllGroupMap);
    }

    @Test
    public void testDescribeGroupStable() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataValue.MemberMetadata memberMetadata = new GroupMetadataValue.MemberMetadata()
            .setMemberId("member-id")
            .setGroupInstanceId("group-instance-id")
            .setClientHost("client-host")
            .setClientId("client-id")
            .setAssignment(new byte[]{0})
            .setSubscription(new byte[]{0, 1, 2});
        GroupMetadataValue groupMetadataValue = new GroupMetadataValue()
            .setMembers(List.of(memberMetadata))
            .setProtocolType("consumer")
            .setProtocol("range")
            .setCurrentStateTimestamp(context.time.milliseconds());

        context.replay(GroupMetadataManagerTestContext.newGroupMetadataRecord(
            "group-id",
            groupMetadataValue
        ));
        context.verifyDescribeGroupsReturnsDeadGroup("group-id");
        context.commit();

        List<DescribeGroupsResponseData.DescribedGroup> expectedDescribedGroups = List.of(
            new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setGroupState(STABLE.toString())
                .setProtocolType(groupMetadataValue.protocolType())
                .setProtocolData(groupMetadataValue.protocol())
                .setMembers(List.of(
                    new DescribeGroupsResponseData.DescribedGroupMember()
                        .setMemberId(memberMetadata.memberId())
                        .setGroupInstanceId(memberMetadata.groupInstanceId())
                        .setClientId(memberMetadata.clientId())
                        .setClientHost(memberMetadata.clientHost())
                        .setMemberMetadata(memberMetadata.subscription())
                        .setMemberAssignment(memberMetadata.assignment())
                ))
        );

        List<DescribeGroupsResponseData.DescribedGroup> describedGroups =
            context.describeGroups(List.of("group-id"));

        assertEquals(expectedDescribedGroups, describedGroups);
    }

    @Test
    public void testDescribeGroupRebalancing() throws Exception {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        GroupMetadataValue.MemberMetadata memberMetadata = new GroupMetadataValue.MemberMetadata()
            .setMemberId("member-id")
            .setGroupInstanceId("group-instance-id")
            .setClientHost("client-host")
            .setClientId("client-id")
            .setAssignment(new byte[]{0})
            .setSubscription(new byte[]{0, 1, 2});
        GroupMetadataValue groupMetadataValue = new GroupMetadataValue()
            .setMembers(List.of(memberMetadata))
            .setProtocolType("consumer")
            .setProtocol("range")
            .setCurrentStateTimestamp(context.time.milliseconds());

        context.replay(GroupMetadataManagerTestContext.newGroupMetadataRecord(
            "group-id",
            groupMetadataValue
        ));
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false);
        context.groupMetadataManager.prepareRebalance(group, "trigger rebalance");

        context.verifyDescribeGroupsReturnsDeadGroup("group-id");
        context.commit();

        List<DescribeGroupsResponseData.DescribedGroup> expectedDescribedGroups = List.of(
            new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setGroupState(PREPARING_REBALANCE.toString())
                .setProtocolType(groupMetadataValue.protocolType())
                .setProtocolData("")
                .setMembers(List.of(
                    new DescribeGroupsResponseData.DescribedGroupMember()
                        .setMemberId(memberMetadata.memberId())
                        .setGroupInstanceId(memberMetadata.groupInstanceId())
                        .setClientId(memberMetadata.clientId())
                        .setClientHost(memberMetadata.clientHost())
                        .setMemberAssignment(memberMetadata.assignment())
                ))
        );

        List<DescribeGroupsResponseData.DescribedGroup> describedGroups =
            context.describeGroups(List.of("group-id"));

        assertEquals(expectedDescribedGroups, describedGroups);
    }

    @Test
    public void testDescribeGroupsGroupIdNotFoundException() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.verifyDescribeGroupsReturnsDeadGroup("group-id");
    }

    @Test
    public void testDescribeGroupsBeforeV6GroupIdNotFoundException() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();
        context.verifyDescribeGroupsBeforeV6ReturnsDeadGroup("group-id");
    }

    @Test
    public void testDynamicBrokerAndGroupConfigs() {
        testDynamicBrokerAndGroupConfig(
            GroupMetadataManager::consumerGroupAssignmentIntervalMs,
            GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
            GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG,
            2000, 1500, 1000, 500
        );
        testDynamicBrokerAndGroupConfig(
            GroupMetadataManager::consumerGroupAssignorOffloadEnable,
            GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNOR_OFFLOAD_ENABLE_CONFIG,
            GroupConfig.CONSUMER_ASSIGNOR_OFFLOAD_ENABLE_CONFIG,
            true, false, true, false
        );
        testDynamicBrokerAndGroupConfig(
            GroupMetadataManager::shareGroupAssignmentIntervalMs,
            GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
            GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG,
            2000, 1500, 1000, 500
        );
        testDynamicBrokerAndGroupConfig(
            GroupMetadataManager::shareGroupAssignorOffloadEnable,
            GroupCoordinatorConfig.SHARE_GROUP_ASSIGNOR_OFFLOAD_ENABLE_CONFIG,
            GroupConfig.SHARE_ASSIGNOR_OFFLOAD_ENABLE_CONFIG,
            true, false, true, false
        );
        testDynamicBrokerAndGroupConfig(
            GroupMetadataManager::streamsGroupAssignmentIntervalMs,
            GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG,
            GroupConfig.STREAMS_ASSIGNMENT_INTERVAL_MS_CONFIG,
            2000, 1500, 1000, 500
        );
        testDynamicBrokerAndGroupConfig(
            GroupMetadataManager::streamsGroupAssignorOffloadEnable,
            GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNOR_OFFLOAD_ENABLE_CONFIG,
            GroupConfig.STREAMS_ASSIGNOR_OFFLOAD_ENABLE_CONFIG,
            true, false, true, false
        );
    }

    private <V> void testDynamicBrokerAndGroupConfig(
        BiFunction<GroupMetadataManager, String, V> getValue,
        String brokerConfigKey,
        String groupConfigKey,
        V initial,
        V brokerOverride1,
        V brokerOverride2,
        V groupOverride
    ) {
        class DynamicConfig extends AbstractConfig {
            private final Map<String, Object> overrides = new HashMap<>();

            DynamicConfig(Map<?, ?> props) {
                super(
                    Utils.mergeConfigs(List.of(
                        GroupCoordinatorConfig.CONFIG_DEF,
                        ShareGroupConfig.CONFIG_DEF
                    )),
                    props,
                    false
                );
            }

            @Override
            protected Object get(String key) {
                return overrides.getOrDefault(key, super.get(key));
            }

            void put(String key, Object value) {
                overrides.put(key, value);
            }
        }

        MockTime time = new MockTime(0, 0, 0);
        DynamicConfig kafkaConfig = new DynamicConfig(Map.of(brokerConfigKey, initial));
        GroupCoordinatorConfig groupCoordinatorConfig = new GroupCoordinatorConfig(kafkaConfig);
        ShareGroupConfig shareGroupConfig = new ShareGroupConfig(kafkaConfig);
        GroupConfigManager groupConfigManager = new GroupConfigManager(
            groupCoordinatorConfig,
            shareGroupConfig
        );
        GroupMetadataManager groupMetadataManager = new GroupMetadataManager.Builder()
            .withTime(time)
            .withTimer(new MockCoordinatorTimer<>(time))
            .withExecutor(new MockCoordinatorExecutor<>())
            .withConfig(groupCoordinatorConfig)
            .withGroupCoordinatorMetricsShard(mock(GroupCoordinatorMetricsShard.class))
            .withGroupConfigManager(groupConfigManager)
            .build();

        String groupId = "test-group";
        assertEquals(initial, getValue.apply(groupMetadataManager, groupId));

        // Set broker-level override.
        kafkaConfig.put(brokerConfigKey, brokerOverride1);
        assertEquals(brokerOverride1, getValue.apply(groupMetadataManager, groupId));

        // Create a group config entry.
        Properties groupConfig = new Properties();
        groupConfig.put(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, 2000);
        groupConfigManager.updateGroupConfig(groupId, groupConfig);

        // Check that broker-level overrides still work. The group config must not bake in the value.
        assertEquals(brokerOverride1, getValue.apply(groupMetadataManager, groupId));
        kafkaConfig.put(brokerConfigKey, brokerOverride2);
        assertEquals(brokerOverride2, getValue.apply(groupMetadataManager, groupId));

        // Set group-level override.
        groupConfig.put(groupConfigKey, String.valueOf(groupOverride));
        groupConfigManager.updateGroupConfig(groupId, groupConfig);
        assertEquals(groupOverride, getValue.apply(groupMetadataManager, groupId));

        // Remove group-level override.
        groupConfig.remove(groupConfigKey);
        groupConfigManager.updateGroupConfig(groupId, groupConfig);
        assertEquals(brokerOverride2, getValue.apply(groupMetadataManager, groupId));
    }

    @Test
    public void testConsumerGroupHeartbeatOnShareGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .withShareGroup(new ShareGroupBuilder(groupId, 1)
                .withMember(new ShareGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .build())
                .withAssignment(memberId, mkAssignment())
                .withAssignmentEpoch(1))
            .build();

        assertThrows(GroupIdNotFoundException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(memberId)
                .setGroupId(groupId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of())));
    }

    @Test
    public void testClassicGroupJoinOnShareGroup() throws Exception {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .withShareGroup(new ShareGroupBuilder(groupId, 1)
                .withMember(new ShareGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .build())
                .withAssignment(memberId, mkAssignment())
                .withAssignmentEpoch(1))
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(new JoinGroupRequestProtocolCollection(0))
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testClassicGroupSyncOnShareGroup() throws Exception {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .withShareGroup(new ShareGroupBuilder(groupId, 1)
                .withMember(new ShareGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .build())
                .withAssignment(memberId, mkAssignment())
                .withAssignmentEpoch(1))
            .build();

        SyncGroupRequestData request = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId(groupId)
            .withGenerationId(1)
            .withMemberId(memberId)
            .build();

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(request);

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), syncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testClassicGroupLeaveOnShareGroup() throws Exception {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
                .withShareGroup(new ShareGroupBuilder(groupId, 1)
                    .withMember(new ShareGroupMember.Builder(memberId)
                        .setState(MemberState.STABLE)
                        .setMemberEpoch(1)
                        .setPreviousMemberEpoch(0)
                        .setClientId(DEFAULT_CLIENT_ID)
                        .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                        .setSubscribedTopicNames(List.of("foo"))
                        .build())
                .withAssignment(memberId, mkAssignment())
                .withAssignmentEpoch(1))
                .build();

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
            .setGroupId(groupId)
            .setMembers(List.of(
                new MemberIdentity()
                    .setMemberId(memberId)))));
    }

    @Test
    public void testConsumerGroupDescribeOnShareGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .withShareGroup(new ShareGroupBuilder(groupId, 1)
                .withMember(new ShareGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .build())
                .withAssignment(memberId, mkAssignment())
                .withAssignmentEpoch(1))
            .build();

        List<ConsumerGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                .setErrorMessage("Group " + groupId + " is not a consumer group.")
        );

        List<ConsumerGroupDescribeResponseData.DescribedGroup> actual = context.sendConsumerGroupDescribe(List.of(groupId));
        assertEquals(expected, actual);
    }

    @Test
    public void testShareGroupHeartbeatOnConsumerGroup() {
        String groupId = "group-foo";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .buildCoordinatorMetadataImage())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        assertThrows(GroupIdNotFoundException.class, () ->
            context.shareGroupHeartbeat(
                new ShareGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(Uuid.randomUuid().toString())
                    .setMemberEpoch(1)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));
        verify(context.metrics, times(0)).record(SHARE_GROUP_REBALANCES_SENSOR_NAME);
    }

    @Test
    public void testShareGroupDescribeOnConsumerGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        int epoch = 10;
        String topicName = "topicName";
        ConsumerGroupMember.Builder memberBuilder = new ConsumerGroupMember.Builder(memberId)
            .setSubscribedTopicNames(List.of(topicName))
            .setServerAssignorName("assignorName");

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, epoch)
                .withMember(memberBuilder.build()))
            .build();

        List<ShareGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                .setErrorMessage("Group " + groupId + " is not a share group.")
        );

        List<ShareGroupDescribeResponseData.DescribedGroup> actual = context.sendShareGroupDescribe(List.of(groupId));
        assertEquals(expected, actual);
    }

    @Test
    public void testConsumerGroupHeartbeatOnStreamsGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 1)
                .withMember(StreamsGroupMember.Builder.withDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .build())
                .withTargetAssignment(memberId, TasksTuple.EMPTY)
                .withTargetAssignmentEpoch(1))
            .build();

        assertThrows(GroupIdNotFoundException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(memberId)
                .setGroupId(groupId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testShareGroupHeartbeatOnStreamsGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 1)
                .withMember(StreamsGroupMember.Builder.withDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .build())
                .withTargetAssignment(memberId, TasksTuple.EMPTY)
                .withTargetAssignmentEpoch(1))
            .build();

        assertThrows(GroupIdNotFoundException.class, () -> context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setMemberId(memberId)
                .setGroupId(groupId)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of("foo", "bar"))));
        verify(context.metrics, times(0)).record(SHARE_GROUP_REBALANCES_SENSOR_NAME);
    }

    @Test
    public void testClassicGroupJoinOnStreamsGroup() throws Exception {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 1)
                .withMember(StreamsGroupMember.Builder.withDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .build())
                .withTargetAssignment(memberId, TasksTuple.EMPTY)
                .withTargetAssignmentEpoch(1))
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("consumer")
            .withProtocols(new JoinGroupRequestProtocolCollection(0))
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL.code(), joinResult.joinFuture.get().errorCode());
    }

    @Test
    public void testClassicGroupSyncOnStreamsGroup() throws Exception {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 1)
                .withMember(StreamsGroupMember.Builder.withDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .build())
                .withTargetAssignment(memberId, TasksTuple.EMPTY)
                .withTargetAssignmentEpoch(1))
            .build();

        SyncGroupRequestData request = new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
            .withGroupId(groupId)
            .withGenerationId(1)
            .withMemberId(memberId)
            .build();

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(request);

        assertTrue(syncResult.records.isEmpty());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.code(), syncResult.syncFuture.get().errorCode());
    }

    @Test
    public void testClassicGroupLeaveOnStreamsGroup() throws Exception {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 1)
                .withMember(StreamsGroupMember.Builder.withDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .build())
                .withTargetAssignment(memberId, TasksTuple.EMPTY)
                .withTargetAssignmentEpoch(1))
            .build();

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId(groupId)
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId(memberId)))));
    }

    @Test
    public void testConsumerGroupDescribeOnStreamsGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 1)
                .withMember(StreamsGroupMember.Builder.withDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .build())
                .withTargetAssignment(memberId, TasksTuple.EMPTY)
                .withTargetAssignmentEpoch(1))
            .build();

        List<ConsumerGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                .setErrorMessage("Group " + groupId + " is not a consumer group.")
        );

        List<ConsumerGroupDescribeResponseData.DescribedGroup> actual = context.sendConsumerGroupDescribe(List.of(groupId));
        assertEquals(expected, actual);
    }

    @Test
    public void testShareGroupDescribeOnStreamsGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .withStreamsGroup(new StreamsGroupBuilder(groupId, 1)
                .withMember(StreamsGroupMember.Builder.withDefaults(memberId)
                    .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                    .setMemberEpoch(1)
                    .setPreviousMemberEpoch(0)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .build())
                .withTargetAssignment(memberId, TasksTuple.EMPTY)
                .withTargetAssignmentEpoch(1))
            .build();

        List<ShareGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                .setErrorMessage("Group " + groupId + " is not a share group.")
        );

        List<ShareGroupDescribeResponseData.DescribedGroup> actual = context.sendShareGroupDescribe(List.of(groupId));
        assertEquals(expected, actual);
    }

    @Test
    public void testStreamsGroupHeartbeatOnConsumerGroup() {
        String groupId = "group-foo";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .buildCoordinatorMetadataImage())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        assertThrows(GroupIdNotFoundException.class, () ->
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(Uuid.randomUuid().toString())
                    .setMemberEpoch(1)));
    }

    @Test
    public void testStreamsGroupDescribeOnConsumerGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        int epoch = 10;
        String topicName = "topicName";
        ConsumerGroupMember.Builder memberBuilder = new ConsumerGroupMember.Builder(memberId)
            .setSubscribedTopicNames(List.of(topicName))
            .setServerAssignorName("assignorName");

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, epoch)
                .withMember(memberBuilder.build()))
            .build();

        List<StreamsGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                .setErrorMessage("Group " + groupId + " is not a streams group.")
        );

        List<StreamsGroupDescribeResponseData.DescribedGroup> actual = context.sendStreamsGroupDescribe(List.of(groupId));
        assertEquals(expected, actual);
    }

    @Test
    public void testStreamsGroupHeartbeatOnShareGroup() {
        String groupId = "group-foo";
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        // Share group with one member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .buildCoordinatorMetadataImage())
            .withShareGroup(new ShareGroupBuilder(groupId, 10)
                .withMember(new ShareGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        assertThrows(GroupIdNotFoundException.class, () ->
            context.streamsGroupHeartbeat(
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(Uuid.randomUuid().toString())
                    .setMemberEpoch(1)));
    }

    @Test
    public void testStreamsGroupDescribeOnShareGroup() {
        String groupId = "group-foo";
        String memberId = Uuid.randomUuid().toString();

        int epoch = 10;
        String topicName = "topicName";
        ShareGroupMember.Builder memberBuilder = new ShareGroupMember.Builder(memberId)
            .setSubscribedTopicNames(List.of(topicName));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroup(new ShareGroupBuilder(groupId, epoch)
                .withMember(memberBuilder.build()))
            .build();

        List<StreamsGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                .setErrorMessage("Group " + groupId + " is not a streams group.")
        );

        List<StreamsGroupDescribeResponseData.DescribedGroup> actual = context.sendStreamsGroupDescribe(List.of(groupId));
        assertEquals(expected, actual);
    }

    @Test
    public void testSubscribedTopicsChangeMap() {
        String topicName = "foo";
        Uuid topicId = Uuid.randomUuid();
        int partitions = 1;
        String groupId = "foogrp";
        MockTime time = new MockTime();
        int initRetryTimeoutMs = 10;

        MockPartitionAssignor assignor = new MockPartitionAssignor("simple");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withTime(time)
            .withConfig(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, initRetryTimeoutMs - 1)
            .withConfig(GroupCoordinatorConfig.SHARE_GROUP_INITIALIZE_RETRY_INTERVAL_MS_CONFIG, initRetryTimeoutMs)
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(topicId, topicName, partitions)
                .buildCoordinatorMetadataImage())
            .build();

        // Empty on empty subscription topics
        assertEquals(
            Map.of(),
            context.groupMetadataManager.subscribedTopicsChangeMap(groupId, Set.of())
        );

        long timeNow = time.milliseconds() + 100;
        time.setCurrentTimeMs(timeNow);
        assertEquals(
            Map.of(
                topicId, new InitMapValue(topicName, Set.of(0), timeNow)
            ),
            context.groupMetadataManager.subscribedTopicsChangeMap(groupId, Set.of(
                topicName
            ))
        );

        // Calculates correct diff respecting both initialized and initializing maps.
        String t1Name = "t1";
        Uuid t1Id = Uuid.randomUuid();
        String t2Name = "t2";
        Uuid t2Id = Uuid.randomUuid();
        String t3Name = "t3";
        Uuid t3Id = Uuid.randomUuid();

        context.groupMetadataManager.replay(
            new ShareGroupMetadataKey()
                .setGroupId(groupId),
            new ShareGroupMetadataValue()
                .setEpoch(0)
        );

        context.groupMetadataManager.replay(
            new ShareGroupStatePartitionMetadataKey()
                .setGroupId(groupId),
            new ShareGroupStatePartitionMetadataValue()
                .setInitializingTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t1Id)
                        .setTopicName(t1Name)
                        .setPartitions(List.of(0, 1))
                ))
                .setInitializedTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t2Id)
                        .setTopicName(t2Name)
                        .setPartitions(List.of(0, 1, 2))
                ))
                .setDeletingTopics(List.of())
        );

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(t1Id, t1Name, 2)
            .addTopic(t2Id, t2Name, 2)
            .addTopic(t3Id, t3Name, 3)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(metadataImage.emptyDelta(), metadataImage);

        // Since t1 is initializing and t2 is initialized due to replay above.
        timeNow = timeNow + initRetryTimeoutMs + 1;
        time.setCurrentTimeMs(timeNow);
        assertEquals(
            Map.of(
                t1Id, new InitMapValue(t1Name, Set.of(0, 1), timeNow),      // initializing
                t3Id, new InitMapValue(t3Name, Set.of(0, 1, 2), timeNow)    // initialized
            ),
            context.groupMetadataManager.subscribedTopicsChangeMap(groupId, Set.of(
                t1Name,
                t2Name,
                t3Name
            ))
        );

        assertEquals(Map.of(t2Id, Set.of(0, 1, 2)), context.groupMetadataManager.initializedShareGroupPartitions(groupId));
    }

    @Test
    public void testUninitializeTopics() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("simple");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        String groupId = "shareGroupId";
        Uuid t1Id = Uuid.randomUuid();
        String t1Name = "t1Name";
        Uuid t2Id = Uuid.randomUuid();
        String t2Name = "t2Name";

        // No records if topics to be uninitialized are not in metadata info.
        CoordinatorResult<Void, CoordinatorRecord> result = context.groupMetadataManager.uninitializeShareGroupState(groupId, Map.of(t1Id, Set.of(0)));
        assertEquals(
            List.of(),
            result.records()
        );

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(t1Id, t1Name, 2)
            .addTopic(t2Id, t2Name, 3)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(image.emptyDelta(), image);

        // Cleanup happens from initializing state only.
        context.groupMetadataManager.replay(
            new ShareGroupMetadataKey()
                .setGroupId(groupId),
            new ShareGroupMetadataValue()
                .setEpoch(0)
        );
        context.groupMetadataManager.replay(
            new ShareGroupStatePartitionMetadataKey()
                .setGroupId(groupId),
            new ShareGroupStatePartitionMetadataValue()
                .setInitializingTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t1Id)
                        .setTopicName(t1Name)
                        .setPartitions(List.of(0, 1))
                ))
                .setInitializedTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t2Id)
                        .setTopicName(t2Name)
                        .setPartitions(List.of(0, 1, 2))
                ))
                .setDeletingTopics(List.of())
        );
        result = context.groupMetadataManager.uninitializeShareGroupState(groupId, Map.of(t1Id, Set.of(0, 1)));
        Set<Integer> partitions = new LinkedHashSet<>(List.of(0, 1, 2));
        assertEquals(
            List.of(newShareGroupStatePartitionMetadataRecord(groupId, Map.of(), Map.of(t2Id, new InitMapValue(t2Name, partitions, 1)), Map.of())),
            result.records()
        );
    }

    @Test
    public void testCombineInitMaps() {
        // Both empty.
        Map<Uuid, InitMapValue> m1 = Map.of();
        Map<Uuid, InitMapValue> m2 = Map.of();

        assertEquals(Map.of(), GroupMetadataManager.combineInitMaps(m1, m2));

        Uuid t1Id = Uuid.randomUuid();
        String t1Name = "t1";
        Uuid t2Id = Uuid.randomUuid();
        String t2Name = "t2";

        // m1 non-empty, m2 empty.
        m1 = Map.of(t1Id, new InitMapValue(t1Name, Set.of(0), 1));
        assertEquals(m1, GroupMetadataManager.combineInitMaps(m1, m2));

        // m1 empty, m2 non-empty.
        m1 = Map.of();
        m2 = Map.of(t1Id, new InitMapValue(t1Name, Set.of(0), 1));
        assertEquals(m2, GroupMetadataManager.combineInitMaps(m1, m2));

        // m1 non-empty, m2 non-empty.
        m1 = Map.of(t1Id, new InitMapValue(t1Name, Set.of(0), 1));
        m2 = Map.of(t2Id, new InitMapValue(t2Name, Set.of(0), 1));
        assertEquals(Map.of(
            t1Id, new InitMapValue(t1Name, Set.of(0), 1),
            t2Id, new InitMapValue(t2Name, Set.of(0), 1)
        ), GroupMetadataManager.combineInitMaps(m1, m2));

        // m1 non-empty, m2 non-empty (differ by partition)
        m1 = Map.of(t1Id, new InitMapValue(t1Name, Set.of(0), 1));
        m2 = Map.of(t1Id, new InitMapValue(t1Name, Set.of(1), 2));
        assertEquals(Map.of(t1Id, new InitMapValue(t1Name, Set.of(0, 1), 2)), GroupMetadataManager.combineInitMaps(m1, m2));

        // m1 and m2 exactly same
        m1 = Map.of(t1Id, new InitMapValue(t1Name, Set.of(0), 1));
        m2 = Map.of(t1Id, new InitMapValue(t1Name, Set.of(0), 1));
        assertEquals(Map.of(t1Id, new InitMapValue(t1Name, Set.of(0), 1)), GroupMetadataManager.combineInitMaps(m1, m2));
    }

}
