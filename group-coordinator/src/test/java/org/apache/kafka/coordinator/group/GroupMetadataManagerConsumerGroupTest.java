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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnreleasedInstanceIdException;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer.ExpiredTimeout;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer.ScheduledTimeout;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupBuilder;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordsEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertResponseEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertUnorderedRecordsEquals;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignmentWithEpochs;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignmentWithEpochs;
import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.classicGroupHeartbeatKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.classicGroupJoinKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.consumerGroupJoinKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupRebalanceTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupSessionTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ADDRESS;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ID;
import static org.apache.kafka.coordinator.group.Utils.computeGroupHash;
import static org.apache.kafka.coordinator.group.Utils.computeTopicHash;
import static org.apache.kafka.coordinator.group.Utils.toAssignmentWithEpochs;
import static org.apache.kafka.coordinator.group.Utils.toAssignmentWithoutEpochs;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.CONSUMER_GROUP_REBALANCES_SENSOR_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link GroupMetadataManager} consumer-group (KIP-848) behaviour:
 * heartbeat, reconciliation, regex resolution, target assignment, describe, and
 * the classic-protocol-member migration paths that end up running consumer-group
 * code paths (e.g. {@code testClassicGroupSyncToConsumerGroup*},
 * {@code testClassicGroupHeartbeatToConsumerGroup*}).
 *
 * <p>Replay tests for consumer-group records ({@code testReplayConsumerGroup*})
 * also live here.
 */
public class GroupMetadataManagerConsumerGroupTest {

    @Test
    public void testUnknownMemberIdJoinsConsumerGroup() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .build();

        // A first member joins to create the group.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor(NoOpPartitionAssignor.NAME)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()));

        // The second member is rejected because the member id is unknown and
        // the member epoch is not zero.
        assertThrows(UnknownMemberIdException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(Uuid.randomUuid().toString())
                    .setMemberEpoch(1)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setTopicPartitions(List.of())));
    }

    @Test
    public void testConsumerGroupMemberEpochValidation() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)), 100))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 100, 0));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3)
        )));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 100, 12345L));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member));

        // Member epoch is greater than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(200)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member epoch is smaller than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(50)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member joins with previous epoch but without providing partitions.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(99)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member joins with previous epoch and has a subset of the owned partitions. This
        // is accepted as the response with the bumped epoch may have been lost. In this
        // case, we provide back the correct epoch to the member.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(99)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(1, 2)))));
        assertEquals(100, result.response().memberEpoch());
    }

    @Test
    public void testMemberCanRejoinWithEpochZeroInStableState() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Member is in STABLE state with epoch 100.
        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2)), 100))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 100, computeGroupHash(Map.of(
            fooTopicName, fooTopicHash
        ))));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 0, 1, 2)
        )));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 100, 12345L));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member));

        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId));

        // Member rejoins with epoch=0 - should succeed per KIP-848.
        // Since the member is STABLE with the same subscription and assignment,
        // the group epoch should not bump and the member gets their current state back.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(100)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2))))),
            result.response()
        );
    }

    @Test
    public void testMemberCanRejoinWithEpochZeroInUnrevokedPartitionsState() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Member is in UNREVOKED_PARTITIONS state with epoch 100.
        // The group has advanced to epoch 101 with a new target assignment [0, 1].
        // The member still has partition 2 pending revocation.
        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1)), 11))
            .setPartitionsPendingRevocation(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 2)), 11))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 101, computeGroupHash(Map.of(
            fooTopicName, fooTopicHash
        ))));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 0, 1)
        )));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 101, 12345L));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member));

        assertEquals(MemberState.UNREVOKED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId));

        // Member rejoins with epoch=0 - should succeed per KIP-848.
        // The member advances to epoch 101 and gets their target assignment [0, 1].
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(101)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1))))),
            result.response()
        );
    }

    @Test
    public void testMemberCanRejoinWithEpochZeroInUnreleasedPartitionsState() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Member is in UNRELEASED_PARTITIONS state with epoch 100.
        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0)), 11))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 100, computeGroupHash(Map.of(
            fooTopicName, fooTopicHash
        ))));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 0, 1, 2)
        )));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 100, 12345L));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member));

        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId));

        // Member rejoins with epoch=0 - should succeed per KIP-848.
        // Since the subscription/metadata hasn't changed, group epoch stays at 100.
        // The member gets the target assignment [0, 1, 2].
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(100)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2))))),
            result.response()
        );
    }

    @Test
    public void testDuplicateFullHeartbeatInStableState() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Member is in STABLE state with epoch 100.
        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2)), 100))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 100, computeGroupHash(Map.of(
            fooTopicName, fooTopicHash
        ))));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 0, 1, 2)
        )));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 100, 12345L));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member));

        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId));

        // Create full request with current epoch.
        ConsumerGroupHeartbeatRequestData fullRequest = new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(100)
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignor("range")
            .setTopicPartitions(List.of(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(0, 1, 2))));

        // First heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result1 =
            context.consumerGroupHeartbeat(fullRequest);

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(100)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2))))),
            result1.response()
        );

        // Duplicate heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result2 =
            context.consumerGroupHeartbeat(fullRequest);

        // Verify duplicate produces same response with no records.
        assertResponseEquals(result1.response(), result2.response());
        assertEquals(List.of(), result2.records());
        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId));
    }

    @Test
    public void testDuplicateFullHeartbeatInUnrevokedPartitionsState() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Member is in UNREVOKED_PARTITIONS state with epoch 100.
        // Target assignment is [0, 1], but member still owns [0, 1, 2].
        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1)), 11))
            .setPartitionsPendingRevocation(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 2)), 11))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 101, computeGroupHash(Map.of(
            fooTopicName, fooTopicHash
        ))));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 0, 1)
        )));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 101, 12345L));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member));

        assertEquals(MemberState.UNREVOKED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId));

        // Create full request with current epoch. Member still reports owning all partitions.
        ConsumerGroupHeartbeatRequestData fullRequest = new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(100)
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignor("range")
            .setTopicPartitions(List.of(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(0, 1, 2))));

        // First heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result1 =
            context.consumerGroupHeartbeat(fullRequest);

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(100)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1))))),
            result1.response()
        );

        assertEquals(MemberState.UNREVOKED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId));

        // Duplicate heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result2 =
            context.consumerGroupHeartbeat(fullRequest);

        // Verify duplicate produces same response with no records.
        assertResponseEquals(result1.response(), result2.response());
        assertEquals(List.of(), result2.records());
        assertEquals(MemberState.UNREVOKED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId));
    }

    @Test
    public void testDuplicateFullHeartbeatInUnreleasedPartitionsState() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Member 1 is in UNRELEASED_PARTITIONS state with epoch 100.
        // Member 1 has [0] assigned but target is [0, 1, 2].
        // Member 2 still owns [1, 2] and needs to revoke them.
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0)), 11))
            .build();

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(99)
            .setPreviousMemberEpoch(98)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(Map.of())
            .setPartitionsPendingRevocation(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2)), 11))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member1));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member2));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 100, computeGroupHash(Map.of(
            fooTopicName, fooTopicHash
        ))));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
            mkTopicAssignment(fooTopicId, 0, 1, 2)
        )));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment()));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 100, 12345L));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member1));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member2));

        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId1));

        // Create full request with current epoch.
        ConsumerGroupHeartbeatRequestData fullRequest = new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(100)
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignor("range")
            .setTopicPartitions(List.of(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(0))));

        // First heartbeat. Member is UNRELEASED_PARTITIONS so response includes current assignment.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result1 =
            context.consumerGroupHeartbeat(fullRequest);

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(100)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0))))),
            result1.response()
        );

        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId1));

        // Duplicate heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result2 =
            context.consumerGroupHeartbeat(fullRequest);

        // Verify duplicate produces same response with no records.
        assertResponseEquals(result1.response(), result2.response());
        assertEquals(List.of(), result2.records());
        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId1));
    }

    @Test
    public void testDuplicateFullHeartbeatWithRevocationAck() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Member is in UNREVOKED_PARTITIONS state with epoch 100.
        // Target assignment is [0, 1], member needs to revoke [2].
        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1)), 11))
            .setPartitionsPendingRevocation(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 2)), 11))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 101, computeGroupHash(Map.of(
            fooTopicName, fooTopicHash
        ))));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 0, 1)
        )));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 101, 12345L));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member));

        assertEquals(MemberState.UNREVOKED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId));

        // Create full request acknowledging revocation (only owns [0, 1]).
        ConsumerGroupHeartbeatRequestData fullRequest = new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setMemberEpoch(100)
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignor("range")
            .setTopicPartitions(List.of(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(0, 1))));

        // First heartbeat acknowledges revocation and transitions to STABLE.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result1 =
            context.consumerGroupHeartbeat(fullRequest);

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(101)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1))))),
            result1.response()
        );

        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId));

        // Duplicate heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result2 =
            context.consumerGroupHeartbeat(fullRequest);

        // Verify duplicate produces same response with no records.
        assertResponseEquals(result1.response(), result2.response());
        assertEquals(List.of(), result2.records());
        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId));
    }

    @Test
    public void testMemberJoinsEmptyConsumerGroup() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )))
        ));

        assertThrows(GroupIdNotFoundException.class, () ->
            context.groupMetadataManager.consumerGroup(groupId));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
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
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0, 1, 2))
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
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)), 2))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 2, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                barTopicName, computeTopicHash(barTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testUpdatingSubscriptionTriggersNewTargetAssignment() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
        long barTopicHash = computeTopicHash(barTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
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
                .withMetadataHash(computeGroupHash(Map.of(fooTopicName, fooTopicHash))))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )))
        ));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10)
                .setSubscribedTopicNames(List.of("foo", "bar")));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0, 1, 2))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignmentWithEpochs(
                mkTopicAssignmentWithEpochs(fooTopicId, 10, 0, 1, 2, 3, 4, 5),
                mkTopicAssignmentWithEpochs(barTopicId, 11, 0, 1, 2)))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, fooTopicHash,
                barTopicName, barTopicHash
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testNewJoiningMemberTriggersNewTargetAssignment() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .build();
        long groupMetadataHash = computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage)),
            barTopicName, computeTopicHash(barTopicName, new KRaftCoordinatorMetadataImage(metadataImage))
        ));

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(groupMetadataHash))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(barTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3),
                mkTopicAssignment(barTopicId, 1)
            )),
            memberId3, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 2)
            ))
        )));

        // Member 3 joins the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(memberId3)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .build();

        assertUnorderedRecordsEquals(
            List.of(
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember3)),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, groupMetadataHash)),
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1),
                        mkTopicAssignment(barTopicId, 0)
                    )),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                        mkTopicAssignment(fooTopicId, 2, 3),
                        mkTopicAssignment(barTopicId, 1)
                    )),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId3, mkAssignment(
                        mkTopicAssignment(fooTopicId, 4, 5),
                        mkTopicAssignment(barTopicId, 2)
                    ))
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds())),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember3))
            ),
            result.records()
        );
    }

    @Test
    public void testLeavingMemberBumpsGroupEpoch() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addTopic(zarTopicId, zarTopicName, 1)
            .addRacks()
            .build();
        long fooTopicHash = computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage));
        long barTopicHash = computeTopicHash(barTopicName, new KRaftCoordinatorMetadataImage(metadataImage));
        long zarTopicHash = computeTopicHash(zarTopicName, new KRaftCoordinatorMetadataImage(metadataImage));

        // Consumer group with two members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    // Use zar only here to ensure that metadata needs to be recomputed.
                    .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash,
                    zarTopicName, zarTopicHash
                ))))
            .build();

        // Member 2 leaves the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH),
            result.response()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
            // Group metadata hash is recomputed because zar is no longer there.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, fooTopicHash,
                barTopicName, barTopicHash
            )))
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testGroupEpochBumpWhenNewStaticMemberJoins() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .build();
        long groupMetadataHash = computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage)),
            barTopicName, computeTopicHash(barTopicName, new KRaftCoordinatorMetadataImage(metadataImage))
        ));

        // Consumer group with two static members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
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
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    // Use zar only here to ensure that metadata needs to be recomputed.
                    .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(groupMetadataHash))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(barTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3),
                mkTopicAssignment(barTopicId, 1)
            )),
            memberId3, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 2)
            ))
        )));

        // Member 3 joins the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setInstanceId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(memberId3)
            .setMemberEpoch(11)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setInstanceId(memberId3)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .build();

        assertUnorderedRecordsEquals(
            List.of(
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember3)),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, groupMetadataHash)),
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1),
                        mkTopicAssignment(barTopicId, 0)
                    )),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                        mkTopicAssignment(fooTopicId, 2, 3),
                        mkTopicAssignment(barTopicId, 1)
                    )),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId3, mkAssignment(
                        mkTopicAssignment(fooTopicId, 4, 5),
                        mkTopicAssignment(barTopicId, 2)
                    ))
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds())),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember3))
            ),
            result.records()
        );
    }

    @Test
    public void testStaticMemberGetsBackAssignmentUponRejoin() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String member2RejoinId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setInstanceId(memberId1)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)), 10))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setInstanceId(memberId2)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)), 10))
            .build();

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .build();

        // Consumer group with two static members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage)),
                    barTopicName, computeTopicHash(barTopicName, new KRaftCoordinatorMetadataImage(metadataImage))
                ))))
            .build();

        // Member 2 leaves the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setInstanceId(memberId2)
                .setMemberEpoch(-2)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()));

        // Member epoch of the response would be set to -2.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(-2),
            result.response()
        );

        // The departing static member will have it's epoch set to -2.
        ConsumerGroupMember member2UpdatedEpoch = new ConsumerGroupMember.Builder(member2)
            .setMemberEpoch(-2)
            .setPartitionsPendingRevocation(Map.of())
            .resetAssignedPartitionsEpochsToZero()
            .build();

        assertEquals(1, result.records().size());
        assertRecordEquals(result.records().get(0), GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member2UpdatedEpoch));

        // Member 2 rejoins the group with the same instance id.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> rejoinResult = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(member2RejoinId)
                .setGroupId(groupId)
                .setInstanceId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(member2RejoinId)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(2))
                    ))),
            rejoinResult.response()
        );

        ConsumerGroupMember expectedCopiedMember = new ConsumerGroupMember.Builder(member2RejoinId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setInstanceId(memberId2)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)), 0))
            .build();

        ConsumerGroupMember expectedRejoinedMember = new ConsumerGroupMember.Builder(member2RejoinId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(0)
            .setInstanceId(memberId2)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)), 0))
            .build();

        List<CoordinatorRecord> expectedRecordsAfterRejoin = List.of(
            // The previous member is deleted.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),

            // The previous member is replaced by the new one.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedCopiedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, member2RejoinId, mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2))),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedCopiedMember),

            // The new member is updated.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedRejoinedMember)
        );

        assertRecordsEquals(expectedRecordsAfterRejoin, rejoinResult.records());
        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId2);
        context.assertNoRebalanceTimeout(groupId, memberId2);
    }

    @Test
    public void testStaticMemberRejoinsWithNewSubscribedTopics() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String member2RejoinId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setInstanceId("instance-id-1")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setInstanceId("instance-id-2")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)), 10))
            .build();

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .build();
        long fooTopicHash = computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage));
        long barTopicHash = computeTopicHash(barTopicName, new KRaftCoordinatorMetadataImage(metadataImage));

        // Consumer group with two static members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash
                ))))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2)
            )),
            member2RejoinId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            ))
        )));

        // Member 2 leaves the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setInstanceId("instance-id-2")
                .setMemberEpoch(-2));

        // Member epoch of the response would be set to -2.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(-2),
            result.response()
        );

        // The departing static member will have it's epoch set to -2.
        ConsumerGroupMember member2UpdatedEpoch = new ConsumerGroupMember.Builder(member2)
            .setMemberEpoch(-2)
            .setPartitionsPendingRevocation(Map.of())
            .resetAssignedPartitionsEpochsToZero()
            .build();

        assertEquals(1, result.records().size());
        assertRecordEquals(result.records().get(0), GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member2UpdatedEpoch));

        // Member 2 rejoins the group with the same instance id.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> rejoinResult = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(member2RejoinId)
                .setGroupId(groupId)
                .setInstanceId("instance-id-2")
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo", "bar")) // bar is new.
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(member2RejoinId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0, 1, 2))
                    ))),
            rejoinResult.response()
        );

        ConsumerGroupMember expectedCopiedMember = new ConsumerGroupMember.Builder(member2RejoinId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setInstanceId("instance-id-2")
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)), 0))
            .build();

        // foo partitions retain epoch 0 (from reset), bar partitions get epoch 11 (newly assigned)
        ConsumerGroupMember expectedRejoinedMember = new ConsumerGroupMember.Builder(member2RejoinId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(0)
            .setInstanceId("instance-id-2")
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignmentWithEpochs(
                mkTopicAssignmentWithEpochs(fooTopicId, 0, 3, 4, 5),
                mkTopicAssignmentWithEpochs(barTopicId, 11, 0, 1, 2)
            ))
            .build();

        List<CoordinatorRecord> expectedRecordsAfterRejoin = List.of(
            // The previous member is deleted.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),

            // The new member is created as a copy of the previous one but
            // with its new member id and new epochs.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedCopiedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, member2RejoinId, mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5))),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedCopiedMember),

            // As the new member as a different subscribed topic set, a rebalance is triggered.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedRejoinedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, fooTopicHash,
                barTopicName, barTopicHash
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, member2RejoinId, mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedRejoinedMember)
        );

        assertRecordsEquals(expectedRecordsAfterRejoin, rejoinResult.records());
        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId2);
        context.assertNoRebalanceTimeout(groupId, memberId2);
    }

    @Test
    public void testStaticMembersRejoinWithNewServerAssignor() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();
        String member2RejoinId = Uuid.randomUuid().toString();
        String member3RejoinId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor uniformAssignor = new MockPartitionAssignor("uniform");
        MockPartitionAssignor rangeAssignor = new MockPartitionAssignor("range");

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setInstanceId("instance-id-1")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("uniform")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)), 10))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setInstanceId("instance-id-2")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("uniform")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3)), 10))
            .build();
        ConsumerGroupMember member3 = new ConsumerGroupMember.Builder(memberId3)
            .setState(MemberState.STABLE)
            .setInstanceId("instance-id-3")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("uniform")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5)), 10))
            .build();

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addRacks()
            .build();

        // Consumer group with three static members using the uniform assignor.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(uniformAssignor, rangeAssignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withMember(member3)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 2, 3)))
                .withAssignment(memberId3, mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage))
                ))))
            .build();

        // All three members leave the consumer group.
        for (var entry : List.of(
            Map.entry(memberId1, "instance-id-1"),
            Map.entry(memberId2, "instance-id-2"),
            Map.entry(memberId3, "instance-id-3")
        )) {
            CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(entry.getKey())
                    .setInstanceId(entry.getValue())
                    .setMemberEpoch(-2));

            assertResponseEquals(
                new ConsumerGroupHeartbeatResponseData()
                    .setMemberId(entry.getKey())
                    .setMemberEpoch(-2),
                result.response()
            );
        }

        // Member 2 rejoins with "range" assignor. The preferred assignor is still "uniform"
        // (counts: uniform=2, range=1) so the group epoch is not bumped. Member 2 gets back
        // its existing assignment at epoch 10.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> rejoinResult2 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(member2RejoinId)
                .setGroupId(groupId)
                .setInstanceId("instance-id-2")
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(member2RejoinId)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(2, 3))
                    ))),
            rejoinResult2.response()
        );

        // Member 3 rejoins with "range" assignor. The preferred assignor shifts to "range"
        // (counts: uniform=1, range=2) so the group epoch is bumped to 11 and a new target
        // assignment is computed using the range assignor.
        rangeAssignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)
            )),
            member2RejoinId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3)
            )),
            member3RejoinId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5)
            ))
        )));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> rejoinResult3 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setMemberId(member3RejoinId)
                .setGroupId(groupId)
                .setInstanceId("instance-id-3")
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(List.of()));

        // Verify that the group epoch was bumped to 11 and the member got the new assignment.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(member3RejoinId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(4, 5))
                    ))),
            rejoinResult3.response()
        );
    }

    @Test
    public void testNoGroupEpochBumpWhenStaticMemberTemporarilyLeaves() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setInstanceId(memberId1)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)), 10))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setInstanceId(memberId2)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            // Use zar only here to ensure that metadata needs to be recomputed.
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)), 10))
            .build();

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .build();

        // Consumer group with two static members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage)),
                    barTopicName, computeTopicHash(barTopicName, new KRaftCoordinatorMetadataImage(metadataImage))
                ))))
            .build();

        // Member 2 leaves the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setInstanceId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()));

        // member epoch of the response would be set to -2
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH),
            result.response()
        );

        ConsumerGroupMember member2UpdatedEpoch = new ConsumerGroupMember
            .Builder(member2)
            .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
            .setPartitionsPendingRevocation(Map.of())
            .resetAssignedPartitionsEpochsToZero()
            .build();

        assertEquals(1, result.records().size());
        assertRecordEquals(result.records().get(0), GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member2UpdatedEpoch));
    }

    @Test
    public void testLeavingStaticMemberBumpsGroupEpoch() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addTopic(zarTopicId, zarTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
        long barTopicHash = computeTopicHash(barTopicName, metadataImage);
        long zarTopicHash = computeTopicHash(zarTopicName, metadataImage);

        // Consumer group with two static members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
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
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setInstanceId(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    // Use zar only here to ensure that metadata needs to be recomputed.
                    .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash,
                    zarTopicName, zarTopicHash
                ))))
            .build();

        // Member 2 leaves the consumer group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setInstanceId(memberId2)
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH),
            result.response()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
            // Group metadata hash is recomputed because zar is no longer there.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, fooTopicHash,
                barTopicName, barTopicHash
            )))
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testShouldThrownUnreleasedInstanceIdExceptionWhenNewMemberJoinsWithInUseInstanceId() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
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
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName,
                    computeTopicHash(fooTopicName, metadataImage))
                )))
            .build();

        // Member 2 joins the consumer group with an in-use instance id.
        assertThrows(UnreleasedInstanceIdException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setInstanceId(memberId1)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of())));
    }

    @Test
    public void testShouldThrownUnknownMemberIdExceptionWhenUnknownStaticMemberJoins() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
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
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName,
                    computeTopicHash(fooTopicName, metadataImage))
                )))
            .build();

        // Member 2 joins the consumer group with a non-zero epoch
        assertThrows(UnknownMemberIdException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setInstanceId(memberId2)
                .setMemberEpoch(10)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of())));
    }

    @Test
    public void testShouldThrowFencedInstanceIdExceptionWhenStaticMemberWithDifferentMemberIdJoins() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
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
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName,
                    computeTopicHash(fooTopicName, metadataImage))
                )))
            .build();

        assertThrows(FencedInstanceIdException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId("unknown-" + memberId1)
                .setInstanceId(memberId1)
                .setMemberEpoch(11)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of())));
    }

    @Test
    public void testConsumerGroupMemberEpochValidationForStaticMember() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setInstanceId(memberId)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)), 100))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, member));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 100, 0));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3)
        )));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 100, 12345L));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, member));

        // Member epoch is greater than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setInstanceId(memberId)
                    .setMemberEpoch(200)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member epoch is smaller than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setInstanceId(memberId)
                    .setMemberEpoch(50)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member joins with previous epoch but without providing partitions.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setInstanceId(memberId)
                    .setMemberEpoch(99)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));

        // Member joins with previous epoch and has a subset of the owned partitions. This
        // is accepted as the response with the bumped epoch may have been lost. In this
        // case, we provide back the correct epoch to the member.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setInstanceId(memberId)
                .setMemberEpoch(99)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(1, 2)))));
        assertEquals(100, result.response().memberEpoch());
    }

    @Test
    public void testShouldThrowUnknownMemberIdExceptionWhenUnknownStaticMemberLeaves() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
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
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName,
                    computeTopicHash(fooTopicName, metadataImage))
                )))
            .build();

        assertThrows(UnknownMemberIdException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setInstanceId("unknown-" + memberId1)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of())));
    }

    @Test
    public void testShouldThrowFencedInstanceIdExceptionWhenStaticMemberWithDifferentMemberIdLeaves() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        // Consumer group with one static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
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
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName,
                    computeTopicHash(fooTopicName, metadataImage))
                )))
            .build();

        assertThrows(FencedInstanceIdException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId("unknown-" + memberId1)
                .setInstanceId(memberId1)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of())));
    }

    @Test
    public void testConsumerGroupHeartbeatFullResponse() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        // Create a context with an empty consumer group.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .addRacks()
                .buildCoordinatorMetadataImage())
            .build();

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1))))
        ));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result;

        // A full response should be sent back on joining.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setServerAssignor("range")
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
                            .setPartitions(List.of(0, 1))))),
            result.response()
        );

        // Otherwise, a partial response should be sent back.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        // A full response should be sent back when the member sends
        // a full request again with topic names set.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch())
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setServerAssignor("range")
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
                            .setPartitions(List.of(0, 1))))),
            result.response()
        );

        // A full response should be sent back when the member sends
        // a full request again with regex set.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch())
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo.*")
                .setServerAssignor("range")
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
                            .setPartitions(List.of(0, 1))))),
            result.response()
        );
    }

    @Test
    public void testReconciliationProcess() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Create a context with one consumer group containing two members.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(barTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3),
                mkTopicAssignment(barTopicId, 2)
            )),
            memberId3, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 1)
            ))
        )));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result;

        // Members in the group are in Stable state.
        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));

        // Member 3 joins the group. This triggers the computation of a new target assignment
        // for the group. Member 3 does not get any assigned partitions yet because they are
        // all owned by other members. However, it transitions to epoch 11 and the
        // Unreleased Partitions state.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        // We only check the last record as the subscription/target assignment updates are
        // already covered by other tests.
        assertRecordEquals(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setState(MemberState.UNRELEASED_PARTITIONS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(0)
                .build()),
            result.records().get(result.records().size() - 1)
        );

        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 1 heartbeats. It remains at epoch 10 but transitions to Unrevoked Partitions
        // state until it acknowledges the revocation of its partitions. The response contains the new
        // assignment without the partitions that must be revoked.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0))
                    ))),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1),
                    mkTopicAssignment(barTopicId, 0)), 10))
                .setPartitionsPendingRevocation(toAssignmentWithEpochs(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2),
                    mkTopicAssignment(barTopicId, 1)), 10))
                .build())),
            result.records()
        );

        assertEquals(MemberState.UNREVOKED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 heartbeats. It remains at epoch 10 but transitions to Unrevoked Partitions
        // state until it acknowledges the revocation of its partitions. The response contains the new
        // assignment without the partitions that must be revoked.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(3)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(2))
                    ))),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                    mkTopicAssignment(fooTopicId, 3),
                    mkTopicAssignment(barTopicId, 2)), 10))
                .setPartitionsPendingRevocation(toAssignmentWithEpochs(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5)), 10))
                .build())),
            result.records()
        );

        assertEquals(MemberState.UNREVOKED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. The response does not contain any assignment
        // because the member is still waiting on other members to revoke partitions.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setState(MemberState.UNRELEASED_PARTITIONS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .build())),
            result.records()
        );

        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 1 acknowledges the revocation of the partitions. It does so by providing the
        // partitions that it still owns in the request. This allows him to transition to epoch 11
        // and to the Stable state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10)
            .setTopicPartitions(List.of(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(0, 1)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(List.of(0))
            )));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                .setState(MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1),
                    mkTopicAssignment(barTopicId, 0)), 10))
                .build())),
            result.records()
        );

        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 heartbeats but without acknowledging the revocation yet. This is basically a no-op.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertEquals(List.of(), result.records());
        assertEquals(MemberState.UNREVOKED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. It receives the partitions revoked by member 1 but remains
        // in Unreleased Partitions state because it still waits on other partitions.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(1))))),
            result.response()
        );

        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setState(MemberState.UNRELEASED_PARTITIONS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                    mkTopicAssignment(barTopicId, 1)), 11))
                .build())),
            result.records()
        );

        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. Member 2 has not acknowledged the revocation of its partition so
        // member keeps its current assignment.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertEquals(List.of(), result.records());
        assertEquals(MemberState.UNRELEASED_PARTITIONS, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 acknowledges the revocation of the partitions. It does so by providing the
        // partitions that it still owns in the request. This allows him to transition to epoch 11
        // and to the Stable state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10)
            .setTopicPartitions(List.of(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(List.of(2))
            )));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(2, 3)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(2))
                    ))),
            result.response()
        );

        // member2: partition 3 (fooTopicId) and 2 (barTopicId) were retained from epoch 10,
        // partition 2 (fooTopicId) is newly assigned at epoch 11
        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setState(MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedPartitions(mkAssignmentWithEpochs(
                    mkTopicAssignmentWithEpochs(fooTopicId, 11, 2),
                    mkTopicAssignmentWithEpochs(fooTopicId, 10, 3),
                    mkTopicAssignmentWithEpochs(barTopicId, 10, 2)
                ))
                .build())),
            result.records()
        );

        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats to acknowledge its current assignment. It receives all its partitions and
        // transitions to Stable state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11)
            .setTopicPartitions(List.of(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(List.of(1)))));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(1))))),
            result.response()
        );

        // member3: all partitions are newly assigned at epoch 11
        assertRecordsEquals(List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setState(MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5),
                    mkTopicAssignment(barTopicId, 1)), 11))
                .build())),
            result.records()
        );

        assertEquals(MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));
    }

    @Test
    public void testNewMemberIsRejectedWithMaximumMembersIsReached() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage();

        // Create a context with one consumer group containing two members.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG, 2)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();

        assertThrows(GroupMaxSizeReachedException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId3)
                    .setMemberEpoch(0)
                    .setServerAssignor("range")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setTopicPartitions(List.of())));
    }

    @Test
    public void testConsumerGroupStates() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10))
            .build();

        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, context.consumerGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setSubscribedTopicNames(List.of(fooTopicName))
            .build()));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, metadataImage)
        ))));

        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, context.consumerGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3))));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, 12345L));

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)), 10))
            .build()));

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)), 10))
            .build()));

        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));
    }

    @Test
    public void testPartitionAssignorExceptionOnRegularHeartbeat() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        ConsumerGroupPartitionAssignor assignor = mock(ConsumerGroupPartitionAssignor.class);
        when(assignor.name()).thenReturn("range");
        when(assignor.assign(any(), any())).thenThrow(new PartitionAssignorException("Assignment failed."));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .buildCoordinatorMetadataImage())
            .build();

        // Member 1 joins the consumer group. The request fails because the
        // target assignment computation failed.
        assertThrows(UnknownServerException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignor("range")
                    .setTopicPartitions(List.of())));
    }

    @Test
    public void testSubscriptionMetadataRefreshedAfterGroupIsLoaded() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Create a context with one consumer group containing one member.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, new MetadataImageBuilder()
                        // foo only has 3 partitions stored in the metadata but foo has
                        // 6 partitions the metadata image.
                        .addTopic(fooTopicId, fooTopicName, 3)
                        .addRacks()
                        .buildCoordinatorMetadataImage())
                ))))
            .build();

        // The metadata refresh flag should be true.
        ConsumerGroup consumerGroup = context.groupMetadataManager
            .consumerGroup(groupId);
        assertTrue(consumerGroup.hasMetadataExpired(context.time.milliseconds()));

        // Prepare the assignment result.
        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        // The member gets partitions 3, 4 and 5 assigned.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
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
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignmentWithEpochs(
                mkTopicAssignmentWithEpochs(fooTopicId, 10, 0, 1, 2),
                mkTopicAssignmentWithEpochs(fooTopicId, 11, 3, 4, 5)))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Check next refresh time.
        assertFalse(consumerGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + Integer.MAX_VALUE, consumerGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, consumerGroup.metadataRefreshDeadline().epoch);
    }

    @Test
    public void testSubscriptionMetadataRefreshedAgainAfterWriteFailure() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Create a context with one consumer group containing one member.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, new MetadataImageBuilder()
                        // foo only has 3 partitions stored in the metadata but foo has
                        // 6 partitions the metadata image.
                        .addTopic(fooTopicId, fooTopicName, 3)
                        .addRacks()
                        .buildCoordinatorMetadataImage())
                ))))
            .build();

        // The metadata refresh flag should be true.
        ConsumerGroup consumerGroup = context.groupMetadataManager
            .consumerGroup(groupId);
        assertTrue(consumerGroup.hasMetadataExpired(context.time.milliseconds()));

        // Prepare the assignment result.
        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Heartbeat.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));

        // The metadata refresh flag is set to a future time.
        assertFalse(consumerGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + Integer.MAX_VALUE, consumerGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, consumerGroup.metadataRefreshDeadline().epoch);

        // Rollback the uncommitted changes. This does not rollback the metadata flag
        // because it is not using a timeline data structure.
        context.rollback();

        // However, the next heartbeat should detect the divergence based on the epoch and trigger
        // a metadata refresh.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10));


        // The member gets partitions 3, 4 and 5 assigned.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
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
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignmentWithEpochs(
                    mkTopicAssignmentWithEpochs(fooTopicId, 10, 0, 1, 2),
                    mkTopicAssignmentWithEpochs(fooTopicId, 11, 3, 4, 5)))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Check next refresh time.
        assertFalse(consumerGroup.hasMetadataExpired(context.time.milliseconds()));
        assertEquals(context.time.milliseconds() + Integer.MAX_VALUE, consumerGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(11, consumerGroup.metadataRefreshDeadline().epoch);
    }

    @Test
    public void testSessionTimeoutLifecycle() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build()))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(List.of()));
        assertEquals(2, result.response().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Session timer is rescheduled on second heartbeat.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch()));
        assertEquals(2, result.response().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Session timer is cancelled on leave.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH));
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, result.response().memberEpoch());

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testSessionTimeoutExpiration() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build()))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(List.of()));
        assertEquals(2, result.response().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time past the session timeout.
        List<ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(45000 + 1);

        // Verify the expired timeout.
        assertEquals(
            List.of(new ExpiredTimeout<>(
                groupSessionTimeoutKey(groupId, memberId),
                new CoordinatorResult<>(
                    List.of(
                        GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 3, 0),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 3, 0L)
                    )
                )
            )),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testSessionTimeoutExpirationStaticMember() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .build()))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setInstanceId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(List.of()));
        assertEquals(2, result.response().memberEpoch());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Static member sends a temporary leave group request
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setInstanceId(memberId)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(90000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(List.of()));

        assertEquals(-2, result.response().memberEpoch());

        // Verify that there is still a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time past the session timeout. No static member joined back as a replacement
        List<ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(45000 + 1);

        // Verify the expired timeout.
        assertEquals(
            List.of(new ExpiredTimeout<>(
                groupSessionTimeoutKey(groupId, memberId),
                new CoordinatorResult<>(
                    List.of(
                        GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 3, 0),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 3, 0L)
                    )
                )
            )),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testRebalanceTimeoutLifecycle() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 3)
                .addRacks()
                .build()))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(memberId1, new MemberAssignmentImpl(mkAssignment(
            mkTopicAssignment(fooTopicId, 0, 1, 2)
        )))));

        // Member 1 joins the group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(180000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2))))),
            result.response()
        );

        assertEquals(
            List.of(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Prepare next assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 2)
            ))
        )));

        // Member 2 joins the group.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(90000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(3)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        assertEquals(
            List.of(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Member 1 heartbeats and transitions to unrevoked partitions. The rebalance timeout
        // is scheduled.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setRebalanceTimeoutMs(12000)
                .setSubscribedTopicNames(List.of("foo")));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1))))),
            result.response()
        );

        // Verify that there is a revocation timeout. Keep a reference
        // to the timeout for later.
        ScheduledTimeout<CoordinatorRecord> scheduledTimeout =
            context.assertRebalanceTimeout(groupId, memberId1, 12000);

        assertEquals(
            List.of(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Member 1 acks the revocation. The revocation timeout is cancelled.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setTopicPartitions(List.of(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(List.of(0, 1)))));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(3)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        // Verify that there is not revocation timeout.
        context.assertNoRebalanceTimeout(groupId, memberId1);

        // Execute the scheduled revocation timeout captured earlier to simulate a
        // stale timeout. This should be a no-op.
        assertEquals(List.of(), scheduledTimeout.operation().generateRecords().records());
    }

    @Test
    public void testRebalanceTimeoutExpiration() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 3)
            .addRacks()
            .build();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId1, new MemberAssignmentImpl(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2))))
        ));

        // Member 1 joins the group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(10000) // Use timeout smaller than session timeout.
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2))))),
            result.response()
        );

        assertEquals(
            List.of(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Prepare next assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 2)
            ))
        )));

        // Member 2 joins the group.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(10000)
                .setSubscribedTopicNames(List.of("foo"))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(3)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        assertEquals(
            List.of(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Member 1 heartbeats and transitions to revoking. The revocation timeout
        // is scheduled.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(2));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1))))),
            result.response()
        );

        // Advance time past the revocation timeout.
        List<ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(10000 + 1);

        // Verify the expired timeout.
        assertEquals(
            List.of(new ExpiredTimeout<>(
                groupRebalanceTimeoutKey(groupId, memberId1),
                new CoordinatorResult<>(
                    List.of(
                        GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                        GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 4, computeGroupHash(Map.of(
                            fooTopicName, computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage))
                        )))
                    )
                )
            )),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId1);
        context.assertNoRebalanceTimeout(groupId, memberId1);
    }

    @Test
    public void testConsumerGroupDescribeNoErrors() {
        List<String> consumerGroupIds = List.of("group-id-1", "group-id-2");
        int epoch = 10;
        String memberId = "member-id";
        String topicName = "topicName";
        ConsumerGroupMember.Builder memberBuilder = new ConsumerGroupMember.Builder(memberId)
            .setSubscribedTopicNames(List.of(topicName))
            .setServerAssignorName("assignorName");

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withConsumerGroup(new ConsumerGroupBuilder(consumerGroupIds.get(0), epoch))
            .withConsumerGroup(new ConsumerGroupBuilder(consumerGroupIds.get(1), epoch)
                .withMember(memberBuilder.build()))
            .build();

        List<ConsumerGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(epoch)
                .setGroupId(consumerGroupIds.get(0))
                .setGroupState(ConsumerGroup.ConsumerGroupState.EMPTY.toString())
                .setAssignorName("range")
                .setAssignmentEpoch(1),
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(epoch)
                .setGroupId(consumerGroupIds.get(1))
                .setMembers(List.of(
                    memberBuilder.build().asConsumerGroupDescribeMember(
                        new Assignment(Map.of()),
                        new MetadataImageBuilder().buildCoordinatorMetadataImage()
                    )
                ))
                .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
                .setAssignorName("assignorName")
                .setAssignmentEpoch(1)
        );
        List<ConsumerGroupDescribeResponseData.DescribedGroup> actual = context.sendConsumerGroupDescribe(consumerGroupIds);

        assertEquals(expected, actual);
    }

    @Test
    public void testConsumerGroupDescribeWithErrors() {
        String groupId = "groupId";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        List<ConsumerGroupDescribeResponseData.DescribedGroup> actual = context.sendConsumerGroupDescribe(List.of(groupId));
        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
            .setErrorMessage("Group " + groupId + " not found.");
        List<ConsumerGroupDescribeResponseData.DescribedGroup> expected = List.of(
            describedGroup
        );

        assertEquals(expected, actual);
    }

    @Test
    public void testConsumerGroupDescribeBeforeAndAfterCommittingOffset() {
        String consumerGroupId = "consumerGroupId";
        int epoch = 10;
        String memberId1 = "memberId1";
        String memberId2 = "memberId2";
        String topicName = "topicName";
        Uuid topicId = Uuid.randomUuid();
        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId, topicName, 3)
            .buildCoordinatorMetadataImage();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        ConsumerGroupMember.Builder memberBuilder1 = new ConsumerGroupMember.Builder(memberId1)
            .setSubscribedTopicNames(List.of(topicName));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(consumerGroupId, memberBuilder1.build()));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(consumerGroupId, epoch + 1, 0));

        Map<Uuid, Set<Integer>> assignmentMap = Map.of(topicId, Set.of());

        ConsumerGroupMember.Builder memberBuilder2 = new ConsumerGroupMember.Builder(memberId2);
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(consumerGroupId, memberBuilder2.build()));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(consumerGroupId, memberId2, assignmentMap));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(consumerGroupId, epoch + 1, 12345L));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(consumerGroupId, memberBuilder2.build()));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(consumerGroupId, epoch + 2, 0));

        List<ConsumerGroupDescribeResponseData.DescribedGroup> actual = context.groupMetadataManager.consumerGroupDescribe(List.of(consumerGroupId), context.lastCommittedOffset);
        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(consumerGroupId)
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
            .setErrorMessage("Group " + consumerGroupId + " not found.");
        List<ConsumerGroupDescribeResponseData.DescribedGroup> expected = List.of(
            describedGroup
        );
        assertEquals(expected, actual);

        // Commit the offset and test again
        context.commit();

        actual = context.groupMetadataManager.consumerGroupDescribe(List.of(consumerGroupId), context.lastCommittedOffset);
        describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(consumerGroupId)
            .setMembers(List.of(
                memberBuilder1.build().asConsumerGroupDescribeMember(new Assignment(Map.of()), metadataImage),
                memberBuilder2.build().asConsumerGroupDescribeMember(new Assignment(assignmentMap), metadataImage)
            ))
            .setGroupState(ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())
            .setAssignorName("range")
            .setGroupEpoch(epoch + 2)
            .setAssignmentEpoch(epoch + 1);
        expected = List.of(
            describedGroup
        );
        assertEquals(expected, actual);
    }

    @Test
    public void testConsumerGroupDelete() {
        String groupId = "group-id";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId)
        );
        List<CoordinatorRecord> records = new ArrayList<>();
        context.groupMetadataManager.createGroupTombstoneRecordsAndCancelTimers("group-id", records);
        assertEquals(expectedRecords, records);
    }

    @Test
    public void testConsumerGroupMaybeDelete() {
        String groupId = "group-id";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId)
        );
        List<CoordinatorRecord> records = new ArrayList<>();
        context.groupMetadataManager.maybeDeleteGroup(groupId, records);
        assertEquals(expectedRecords, records);

        records = new ArrayList<>();
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .build()));
        context.groupMetadataManager.maybeDeleteGroup(groupId, records);
        assertEquals(List.of(), records);
    }

    @Test
    public void testConsumerGroupRebalanceSensor() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addRacks()
                .buildCoordinatorMetadataImage())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )))
        ));

        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()));

        verify(context.metrics).record(CONSUMER_GROUP_REBALANCES_SENSOR_NAME);
    }

    @Test
    public void testConsumerGroupHeartbeatWithNonEmptyClassicGroup() {
        String classicGroupId = "classic-group-id";
        String memberId = Uuid.randomUuid().toString();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .build();
        ClassicGroup classicGroup = new ClassicGroup(
            new LogContext(),
            classicGroupId,
            EMPTY,
            context.time
        );
        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(classicGroup, classicGroup.groupAssignment()));

        context.groupMetadataManager.getOrMaybeCreateClassicGroup(classicGroupId, false).transitionTo(PREPARING_REBALANCE);
        assertThrows(GroupIdNotFoundException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(classicGroupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setServerAssignor(NoOpPartitionAssignor.NAME)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setTopicPartitions(List.of())));
    }

    @Test
    public void testConsumerGroupHeartbeatWithEmptyClassicGroup() {
        String classicGroupId = "classic-group-id";
        String memberId = Uuid.randomUuid().toString();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .build();
        ClassicGroup classicGroup = new ClassicGroup(
            new LogContext(),
            classicGroupId,
            EMPTY,
            context.time
        );
        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(classicGroup, classicGroup.groupAssignment()));

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(classicGroupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor(NoOpPartitionAssignor.NAME)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()));

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setRebalanceTimeoutMs(5000)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setAssignedPartitions(Map.of())
            .build();

        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(classicGroupId),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(classicGroupId, expectedMember),
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(classicGroupId, 2, 0),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(classicGroupId, memberId, Map.of()),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(classicGroupId, 2, context.time.milliseconds()),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(classicGroupId, expectedMember)
            ),
            result.records()
        );
        assertEquals(
            Group.GroupType.CONSUMER,
            context.groupMetadataManager.consumerGroup(classicGroupId).type()
        );
    }

    @Test
    public void testConsumerGroupHeartbeatWithStableClassicGroup() {
        String groupId = "group-id";
        String memberId1 = "member-id-1";
        String memberId2 = "member-id-2";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0)
            ))
        )));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 1)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName),
                null,
                List.of(
                    new TopicPartition(fooTopicName, 0),
                    new TopicPartition(barTopicName, 0)
                )
            ))))
        );

        Map<String, byte[]> assignments = Map.of(
            memberId1,
            Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(barTopicName, 0)
            ))))
        );

        // Create a stable classic group with member 1.
        ClassicGroup group = context.createClassicGroup(groupId);
        group.setProtocolName(Optional.of("range"));
        group.add(
            new ClassicGroupMember(
                memberId1,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols,
                assignments.get(memberId1)
            )
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);

        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignments));
        context.commit();
        group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);

        // A new member 2 with new protocol joins the classic group, triggering the upgrade.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                .setTopicPartitions(List.of()));

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols))
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0),
                mkTopicAssignment(barTopicId, 0)), 0))
            .build();

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setServerAssignorName("range")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(5000)
            .setAssignedPartitions(Map.of())
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The existing classic group tombstone.
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId),

            // Create the new consumer group with member 1.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 0, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                barTopicName, computeTopicHash(barTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, toAssignmentWithoutEpochs(expectedMember1.assignedPartitions())),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 0, 0),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1),

            // Member 2 joins the new consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),

            // Newly joining member 2 bumps the group epoch. A new target assignment is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 1, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                barTopicName, computeTopicHash(barTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, assignor.targetPartitions(memberId2)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, assignor.targetPartitions(memberId1)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 1, context.time.milliseconds()),

            // Member 2 has no pending revoking partition. Bump its member epoch and transition to UNRELEASED_PARTITIONS.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
        );

        assertRecordsEquals(expectedRecords, result.records());

        context.assertSessionTimeout(groupId, memberId1, expectedMember1.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId2, 45000);

        // Simulate a failed replay. The context is rolled back and the group is converted back to the classic group.
        context.rollback();
        assertEquals(group, context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false));
    }

    @Test
    public void testConsumerGroupHeartbeatWithPreparingRebalanceClassicGroup() throws Exception {
        String groupId = "group-id";
        String memberId1 = "member-id-1";
        String memberId2 = "member-id-2";
        String memberId3 = "member-id-3";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0)
            )),
            memberId3, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)
            ))
        )));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols1 = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols1.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName),
                null,
                List.of(
                    new TopicPartition(fooTopicName, 0),
                    new TopicPartition(fooTopicName, 1)
                )
            ))))
        );

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols2 = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols2.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName),
                null,
                List.of(new TopicPartition(barTopicName, 0))
            ))))
        );

        Map<String, byte[]> assignments = Map.of(
            memberId1, Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(fooTopicName, 1)
            )))),
            memberId2, Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
                new TopicPartition(barTopicName, 0)
            ))))
        );

        // Construct a stable group with two members.
        ClassicGroup group = context.createClassicGroup(groupId);
        group.setProtocolName(Optional.of("range"));
        group.add(
            new ClassicGroupMember(
                memberId1,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols1,
                assignments.get(memberId1)
            )
        );
        group.add(
            new ClassicGroupMember(
                memberId2,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols2,
                assignments.get(memberId2)
            )
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);

        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignments));
        context.commit();
        group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);

        // The leader rejoins, triggering a rebalance.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId1)
                .withProtocols(protocols1)
                .withSessionTimeoutMs(5000)
                .withRebalanceTimeoutMs(10000)
                .build()
        );
        assertTrue(group.isInState(PREPARING_REBALANCE));

        // Another new member 3 joins with new protocol, triggering the upgrade.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupHeartbeatResult = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                .setTopicPartitions(List.of()));

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols1))
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)), 0))
            .build();

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols2))
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(barTopicId, 0)), 0))
            .build();

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(memberId3)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setServerAssignorName("range")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(5000)
            .setAssignedPartitions(Map.of())
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The existing classic group tombstone.
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId),

            // Create the new consumer group with member 1 and member 2.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),

            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 0, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                barTopicName, computeTopicHash(barTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, toAssignmentWithoutEpochs(expectedMember1.assignedPartitions())),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, toAssignmentWithoutEpochs(expectedMember2.assignedPartitions())),

            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 0, 0),

            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2),

            // Member 3 joins the new consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember3),

            // Newly joining member 3 bumps the group epoch. A new target assignment is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 1, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                barTopicName, computeTopicHash(barTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, assignor.targetPartitions(memberId1)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId3, assignor.targetPartitions(memberId3)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 1, context.time.milliseconds()),

            // Member 3 has no pending revoking partition. Bump its member epoch and transition to UNRELEASED_PARTITIONS.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember3)
        );

        assertRecordsEquals(expectedRecords, consumerGroupHeartbeatResult.records());
        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), joinResult.joinFuture.get().errorCode());

        context.assertSessionTimeout(groupId, memberId1, expectedMember1.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId2, expectedMember2.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId3, 45000);

        // Simulate a failed replay. The context is rolled back and the group is converted back to the classic group.
        context.rollback();
        assertEquals(group, context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false));
    }

    /**
     * Supplies the {@link Arguments} to {@link #testConsumerGroupHeartbeatWithCustomAssignorClassicGroup(ByteBuffer, boolean)}.
     */
    private static Stream<Arguments> testConsumerGroupHeartbeatWithCustomAssignorClassicGroupSource() {
        return Stream.of(
            Arguments.of(null, true),
            Arguments.of(ByteBuffer.allocate(0), true),
            Arguments.of(ByteBuffer.allocate(1), false)
        );
    }

    @ParameterizedTest
    @MethodSource("testConsumerGroupHeartbeatWithCustomAssignorClassicGroupSource")
    public void testConsumerGroupHeartbeatWithCustomAssignorClassicGroup(ByteBuffer userData, boolean expectUpgrade) {
        String groupId = "group-id";
        String memberId1 = "member-id-1";
        String memberId2 = "member-id-2";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0)
            ))
        )));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 1)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName),
                null,
                List.of(
                    new TopicPartition(fooTopicName, 0),
                    new TopicPartition(barTopicName, 0)
                )
            ))))
        );

        Map<String, byte[]> assignments = Map.of(
            memberId1,
            Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(barTopicName, 0)
            ), userData)))
        );

        // Create a stable classic group with member 1.
        ClassicGroup group = context.createClassicGroup(groupId);
        group.setProtocolName(Optional.of("range"));
        group.add(
            new ClassicGroupMember(
                memberId1,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols,
                assignments.get(memberId1)
            )
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);

        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignments));
        context.commit();
        group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);

        // A new member 2 with new protocol joins the classic group, triggering the upgrade.
        ConsumerGroupHeartbeatRequestData consumerGroupHeartbeatRequestData =
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                .setTopicPartitions(List.of());

        if (expectUpgrade) {
            context.consumerGroupHeartbeat(consumerGroupHeartbeatRequestData);
        } else {
            Exception ex = assertThrows(GroupIdNotFoundException.class, () -> context.consumerGroupHeartbeat(consumerGroupHeartbeatRequestData));
            assertEquals(
                "Cannot upgrade classic group group-id to consumer group because an unsupported custom assignor is in use. " +
                "Please refer to the documentation or switch to a default assignor before re-attempting the upgrade.", ex.getMessage());
        }
    }

    @Test
    public void testConsumerGroupHeartbeatWithStableClassicGroupFailsOnMalformedProtocol() {
        String groupId = "group-id";
        String memberId1 = "member-id-1";
        String memberId2 = "member-id-2";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Throws RuntimeException when read
        byte[] poisonMetadata = new byte[]{
            0, 1,                                              // version (int16) = 1
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF // topics array length (int32) = -1
        };

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(poisonMetadata));

        Map<String, byte[]> assignments = Map.of(
            memberId1,
            Utils.toArray(ConsumerProtocol.serializeAssignment(
                new ConsumerPartitionAssignor.Assignment(List.of(new TopicPartition(fooTopicName, 0)))))
        );

        ClassicGroup group = context.createClassicGroup(groupId);
        group.setProtocolName(Optional.of("range"));
        group.add(
            new ClassicGroupMember(
                memberId1,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols,
                assignments.get(memberId1)
            )
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);

        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignments));
        context.commit();

        // A new member 2 with the new protocol joins the classic group, triggering the upgrade.
        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setRebalanceTimeoutMs(5000)
            .setServerAssignor("range")
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setTopicPartitions(List.of());

        Exception ex = assertThrows(GroupIdNotFoundException.class,
            () -> context.consumerGroupHeartbeat(request));
        assertEquals(
            "Cannot upgrade classic group group-id to consumer group because the embedded consumer protocol is malformed.",
            ex.getMessage()
        );
    }

    @Test
    public void testClassicGroupJoinToConsumerGroupFailsOnMalformedSubscriptionMetadata() {
        String groupId = "group-id";
        String existingMemberId = Uuid.randomUuid().toString();
        String newMemberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();

        ConsumerGroupMember existingMember = new ConsumerGroupMember.Builder(existingMemberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)), 10))
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new MockPartitionAssignor("range")))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(existingMember)
                .withAssignment(existingMemberId, mkAssignment(mkTopicAssignment(fooTopicId, 0)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage)
                ))))
            .build();

        // Throws RuntimeException when read.
        byte[] poisonMetadata = new byte[]{
            0, 1,                                              // version (int16) = 1
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF // topics array length (int32) = -1
        };
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(poisonMetadata));

        JoinGroupRequestData joinRequest = new JoinGroupRequestData()
            .setGroupId(groupId)
            .setMemberId(newMemberId)
            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .setProtocols(protocols)
            .setSessionTimeoutMs(5000)
            .setRebalanceTimeoutMs(45000);

        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> context.sendClassicGroupJoin(joinRequest));
        assertEquals("Malformed embedded consumer protocol in subscription deserialization.", ex.getMessage());
    }

    @Test
    public void testConsumerGroupHeartbeatToClassicGroupFromExistingStaticMember() {
        String groupId = "group-id";
        String memberId = "member-id";
        String instanceId = "instance-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(metadataImage)
            .build();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName(NoOpPartitionAssignor.NAME)
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName),
                null,
                List.of(new TopicPartition(fooTopicName, 0))
            ))))
        );

        Map<String, byte[]> assignments = Map.of(
            memberId,
            Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(
                List.of(new TopicPartition(fooTopicName, 0))
            )))
        );

        // Create a stable classic group with a static member.
        ClassicGroup group = context.createClassicGroup(groupId);
        group.setProtocolName(Optional.of(NoOpPartitionAssignor.NAME));
        group.add(
            new ClassicGroupMember(
                memberId,
                Optional.of(instanceId),
                DEFAULT_CLIENT_ID,
                DEFAULT_CLIENT_ADDRESS.toString(),
                10000,
                5000,
                "consumer",
                protocols,
                assignments.get(memberId)
            )
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.initNextGeneration();
        group.transitionTo(STABLE);

        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignments));
        context.commit();

        // The static member rejoins with new protocol after a restart, triggering the upgrade.
        String newMemberId = Uuid.randomUuid().toString();
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(newMemberId)
                .setInstanceId(instanceId)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor(NoOpPartitionAssignor.NAME)
                .setSubscribedTopicNames(List.of(fooTopicName))
                .setTopicPartitions(List.of()),
            ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion()
        );

        ConsumerGroupMember expectedClassicMember = new ConsumerGroupMember.Builder(memberId)
            .setInstanceId(instanceId)
            .setMemberEpoch(group.generationId())
            .setPreviousMemberEpoch(group.generationId())
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols))
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)), group.generationId()))
            .build();

        // The memberId is generated by the consumer and should be retained
        // for the entire lifetime of the process until termination.
        assertEquals(
            newMemberId,
            result.response().memberId(),
            "Server should not generate a new memberId since the consumer has already generated its own."
        );

        ConsumerGroupMember expectedReplacingConsumerMember = new ConsumerGroupMember.Builder(newMemberId)
            .setInstanceId(instanceId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setClientId(expectedClassicMember.clientId())
            .setClientHost(expectedClassicMember.clientHost())
            .setSubscribedTopicNames(new ArrayList<>(expectedClassicMember.subscribedTopicNames()))
            .setRebalanceTimeoutMs(expectedClassicMember.rebalanceTimeoutMs())
            .setAssignedPartitions(toAssignmentWithEpochs(toAssignmentWithoutEpochs(expectedClassicMember.assignedPartitions()), group.generationId()))
            .setClassicMemberMetadata(expectedClassicMember.classicMemberMetadata().get())
            .build();

        ConsumerGroupMember expectedFinalConsumerMember = new ConsumerGroupMember.Builder(expectedReplacingConsumerMember)
            .setMemberEpoch(group.generationId())
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(5000)
            .setClassicMemberMetadata(null)
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The existing classic group tombstone.
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId),

            // Create the new consumer group with the static member.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedClassicMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, group.generationId(), computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, toAssignmentWithoutEpochs(expectedClassicMember.assignedPartitions())),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, group.generationId(), 0),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedClassicMember),

            // Remove the static member because the rejoining member replaces it.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),

            // Create the new static member.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedReplacingConsumerMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, newMemberId, mkAssignment(mkTopicAssignment(fooTopicId, 0))),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedReplacingConsumerMember),

            // The static member rejoins the new consumer group with the same instance id and
            // takes the assignment of the previous member. No new target assignment is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedFinalConsumerMember),

            // The newly created static member takes the assignment from the existing member.
            // Bump its member epoch and transition to STABLE.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedFinalConsumerMember)
        );

        assertEquals(expectedRecords.size(), result.records().size());
        assertRecordsEquals(expectedRecords.subList(0, 5), result.records().subList(0, 5));
        assertRecordsEquals(expectedRecords.subList(5, 10), result.records().subList(5, 10));

        assertRecordsEquals(expectedRecords, result.records());
        context.assertSessionTimeout(groupId, newMemberId, 45000);
    }

    @Test
    public void testConsumerGroupHeartbeatToClassicGroupWithEmptyAssignmentMember() throws ExecutionException, InterruptedException {
        String groupId = "group-id";
        String memberId2 = "member-id-2";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 1)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(metadataImage)
            .build();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName)
            ))))
        );

        // Member 1 joins, creating a new classic group.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withProtocols(protocols)
                .withSessionTimeoutMs(5000)
                .withRebalanceTimeoutMs(10000)
                .build()
        );

        // Triggering completion of the rebalance.
        // Member 1 has never synced so its assignment is empty.
        context.sleep(3000 + 1);
        String memberId1 = joinResult.joinFuture.get().memberId();
        ClassicGroup group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        // A new member 2 with new protocol joins the classic group, triggering the upgrade.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor(NoOpPartitionAssignor.NAME)
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                .setTopicPartitions(List.of()));

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(1)
            .setState(MemberState.STABLE)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols))
            )
            .setAssignedPartitions(Map.of())
            .build();

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(5000)
            .setAssignedPartitions(Map.of())
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The existing classic group tombstone.
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId),

            // Create the new consumer group with member 1.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 1, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                barTopicName, computeTopicHash(barTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, toAssignmentWithoutEpochs(expectedMember1.assignedPartitions())),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 1, 0),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1),

            // Member 2 joins the new consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),

            // Newly joining member 2 bumps the group epoch. A new target assignment is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 2, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                barTopicName, computeTopicHash(barTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, Map.of()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),

            // Member 2 has no pending revoking partition or pending release partition.
            // Bump its member epoch and transition to STABLE.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
        );

        assertRecordsEquals(expectedRecords, result.records());

        context.assertSessionTimeout(groupId, memberId1, expectedMember1.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId2, 45000);
    }

    @Test
    public void testConsumerGroupHeartbeatFromExistingClassicStaticMember() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String instanceId1 = "instance-id-1";
        String instanceId2 = "instance-id-2";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2),
                        new TopicPartition(barTopicName, 0),
                        new TopicPartition(barTopicName, 1)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setInstanceId(instanceId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)), 10))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setInstanceId(instanceId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Consumer group with two static members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();

        // The member 1 with the classic protocol upgrades, heartbeating with new protocol.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setInstanceId(instanceId1)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor(NoOpPartitionAssignor.NAME)
                .setSubscribedTopicNames(new ArrayList<>(member1.subscribedTopicNames()))
                .setTopicPartitions(List.of()),
            ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion()
        );


        // The memberId is generated by the consumer itself, the consumer should retain this memberId
        // for its entire lifetime until the process terminates.
        assertEquals(
            memberId1,
            result.response().memberId(),
            "Server should not generate a new memberId since the consumer has already generated its own."
        );

        ConsumerGroupMember expectedReplacingConsumerMember = new ConsumerGroupMember.Builder(memberId1)
            .setInstanceId(instanceId1)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.STABLE)
            .setClientId(member1.clientId())
            .setClientHost(member1.clientHost())
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setSubscribedTopicNames(new ArrayList<>(member1.subscribedTopicNames()))
            .setRebalanceTimeoutMs(member1.rebalanceTimeoutMs())
            .setAssignedPartitions(toAssignmentWithEpochs(toAssignmentWithoutEpochs(member1.assignedPartitions()), 10))
            .setClassicMemberMetadata(member1.classicMemberMetadata().get())
            .build();

        ConsumerGroupMember expectedFinalConsumerMember = new ConsumerGroupMember.Builder(expectedReplacingConsumerMember)
            .setMemberEpoch(10)
            .setRebalanceTimeoutMs(5000)
            .setClassicMemberMetadata(null)
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // Remove the existing static member 1 because the rejoining member replaces it.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),

            // Create the new static member 1.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedReplacingConsumerMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, toAssignmentWithoutEpochs(member1.assignedPartitions())),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedReplacingConsumerMember),

            // The static member rejoins the new consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedFinalConsumerMember),

            // The newly created static member 1 takes the assignment from the existing member 1.
            // Bump its member epoch and transition to STABLE.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedFinalConsumerMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
        context.assertSessionTimeout(groupId, memberId1, 45000);
    }

    @Test
    public void testConsumerGroupHeartbeatWithCompletingRebalanceClassicGroup() throws Exception {
        String groupId = "group-id";
        String memberId1 = "member-id-1";
        String memberId2 = "member-id-2";
        String memberId3 = "member-id-3";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0)
            )),
            memberId3, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)
            ))
        )));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.UPGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols1 = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols1.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName),
                null,
                List.of(
                    new TopicPartition(fooTopicName, 0),
                    new TopicPartition(fooTopicName, 1)
                )
            ))))
        );

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols2 = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols2.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName, barTopicName),
                null,
                List.of(new TopicPartition(barTopicName, 0))
            ))))
        );

        Map<String, byte[]> assignments = Map.of(
            memberId1, Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(fooTopicName, 1)
            )))),
            memberId2, Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
                new TopicPartition(barTopicName, 0)
            ))))
        );

        // Construct a stable group with two members.
        ClassicGroup group = context.createClassicGroup(groupId);
        group.setProtocolName(Optional.of("range"));
        group.add(
            new ClassicGroupMember(
                memberId1,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols1,
                assignments.get(memberId1)
            )
        );
        group.add(
            new ClassicGroupMember(
                memberId2,
                Optional.empty(),
                "client-id",
                "client-host",
                10000,
                5000,
                "consumer",
                protocols2,
                assignments.get(memberId2)
            )
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);

        context.replay(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(group, assignments));
        context.commit();
        group = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);

        // The leader rejoins, triggering a rebalance.
        context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId1)
                .withProtocols(protocols1)
                .withSessionTimeoutMs(5000)
                .withRebalanceTimeoutMs(10000)
                .build()
        );

        // The follower rejoins. All members have rejoined so the group transitions to COMPLETING_REBALANCE state.
        context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId2)
                .withProtocols(protocols2)
                .withSessionTimeoutMs(5000)
                .withRebalanceTimeoutMs(10000)
                .build()
        );
        assertTrue(group.isInState(COMPLETING_REBALANCE));

        GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId("group-id")
                .withMemberId(memberId2)
                .withGenerationId(1)
                .build());

        // Another new member 3 joins with new protocol, triggering the upgrade.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> consumerGroupHeartbeatResult = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                .setTopicPartitions(List.of()));

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(1)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols1))
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)), 1))
            .build();

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(1)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(10000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols2))
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(barTopicId, 0)), 1))
            .build();

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(memberId3)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setServerAssignorName("range")
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(5000)
            .setAssignedPartitions(Map.of())
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The existing classic group tombstone.
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId),

            // Create the new consumer group with member 1 and member 2.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),

            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 1, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                barTopicName, computeTopicHash(barTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, toAssignmentWithoutEpochs(expectedMember1.assignedPartitions())),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, toAssignmentWithoutEpochs(expectedMember2.assignedPartitions())),

            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 1, 0),

            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2),

            // Member 3 joins the new consumer group.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember3),

            // Newly joining member 3 bumps the group epoch. A new target assignment is computed.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 2, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                barTopicName, computeTopicHash(barTopicName, metadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, assignor.targetPartitions(memberId1)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId3, assignor.targetPartitions(memberId3)),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),

            // Member 3 has no pending revoking partition. Bump its member epoch and transition to UNRELEASED_PARTITIONS.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember3)
        );

        assertRecordsEquals(expectedRecords, consumerGroupHeartbeatResult.records());
        assertTrue(syncResult.syncFuture.isDone());
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), syncResult.syncFuture.get().errorCode());

        context.assertSessionTimeout(groupId, memberId1, expectedMember1.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId2, expectedMember2.classicProtocolSessionTimeout().get());
        context.assertSessionTimeout(groupId, memberId3, 45000);

        // Simulate a failed replay. The context is rolled back and the group is converted back to the classic group.
        context.rollback();
        assertEquals(group, context.groupMetadataManager.getOrMaybeCreateClassicGroup("group-id", false));
    }

    @Test
    public void testLastConsumerProtocolMemberLeavingConsumerGroup() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2),
                        new TopicPartition(barTopicName, 0),
                        new TopicPartition(barTopicName, 1)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)), 10))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();

        ConsumerGroup consumerGroup = context.groupMetadataManager.consumerGroup(groupId);

        // Member 2 leaves the consumer group, triggering the downgrade.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()));


        byte[] assignment = Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
            new TopicPartition(fooTopicName, 0),
            new TopicPartition(fooTopicName, 1),
            new TopicPartition(fooTopicName, 2),
            new TopicPartition(barTopicName, 0),
            new TopicPartition(barTopicName, 1)
        ))));
        Map<String, byte[]> assignments = Map.of(memberId1, assignment);

        ClassicGroup expectedClassicGroup = new ClassicGroup(
            new LogContext(),
            groupId,
            STABLE,
            context.time,
            10,
            Optional.of(ConsumerProtocol.PROTOCOL_TYPE),
            Optional.of("range"),
            Optional.of(memberId1),
            Optional.of(context.time.milliseconds())
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                memberId1,
                Optional.ofNullable(member1.instanceId()),
                member1.clientId(),
                member1.clientHost(),
                member1.rebalanceTimeoutMs(),
                member1.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                member1.supportedJoinGroupRequestProtocols(),
                assignment
            )
        );

        assertUnorderedRecordsEquals(
            List.of(
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2)
                ),
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2)
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataTombstoneRecord(groupId)),
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2)
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId)),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId)),
                List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(expectedClassicGroup, assignments))
            ),
            result.records()
        );

        // The new classic member 1 has a heartbeat timeout.
        ScheduledTimeout<CoordinatorRecord> heartbeatTimeout = context.timer.timeout(
            classicGroupHeartbeatKey(groupId, memberId1)
        );
        assertNotNull(heartbeatTimeout);
        // The new rebalance has a groupJoin timeout.
        ScheduledTimeout<CoordinatorRecord> groupJoinTimeout = context.timer.timeout(
            classicGroupJoinKey(groupId)
        );
        assertNotNull(groupJoinTimeout);

        // A new rebalance is triggered.
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(classicGroup.isInState(PREPARING_REBALANCE));

        // Simulate a failed write to the log.
        context.rollback();

        // The group is reverted back to the consumer group.
        assertEquals(consumerGroup, context.groupMetadataManager.consumerGroup(groupId));
    }

    @Test
    public void testLastConsumerProtocolMemberSessionTimeoutInConsumerGroup() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2),
                        new TopicPartition(barTopicName, 0),
                        new TopicPartition(barTopicName, 1)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)), 10))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();

        // Session timer is scheduled on the heartbeat.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of()));

        // Verify that there is a session timeout.
        context.assertSessionTimeout(groupId, memberId2, 45000);

        // Advance time past the session timeout.
        // Member 2 should be fenced from the group, thus triggering the downgrade.
        ExpiredTimeout<CoordinatorRecord> timeout = context.sleep(45000 + 1).get(0);
        assertEquals(groupSessionTimeoutKey(groupId, memberId2), timeout.key());

        byte[] assignment = Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
            new TopicPartition(fooTopicName, 0),
            new TopicPartition(fooTopicName, 1),
            new TopicPartition(fooTopicName, 2),
            new TopicPartition(barTopicName, 0),
            new TopicPartition(barTopicName, 1)
        ))));
        Map<String, byte[]> assignments = Map.of(memberId1, assignment);

        ClassicGroup expectedClassicGroup = new ClassicGroup(
            new LogContext(),
            groupId,
            STABLE,
            context.time,
            10,
            Optional.of(ConsumerProtocol.PROTOCOL_TYPE),
            Optional.of("range"),
            Optional.of(memberId1),
            Optional.of(context.time.milliseconds())
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                memberId1,
                Optional.ofNullable(member1.instanceId()),
                member1.clientId(),
                member1.clientHost(),
                member1.rebalanceTimeoutMs(),
                member1.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                member1.supportedJoinGroupRequestProtocols(),
                assignment
            )
        );

        assertUnorderedRecordsEquals(
            List.of(
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2)
                ),
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2)
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataTombstoneRecord(groupId)),
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2)
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId)),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId)),
                List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(expectedClassicGroup, assignments))
            ),
            timeout.result().records()
        );

        // The new classic member 1 has a heartbeat timeout.
        ScheduledTimeout<CoordinatorRecord> heartbeatTimeout = context.timer.timeout(
            classicGroupHeartbeatKey(groupId, memberId1)
        );
        assertNotNull(heartbeatTimeout);
        // The new rebalance has a groupJoin timeout.
        ScheduledTimeout<CoordinatorRecord> groupJoinTimeout = context.timer.timeout(
            classicGroupJoinKey(groupId)
        );
        assertNotNull(groupJoinTimeout);

        // A new rebalance is triggered.
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(classicGroup.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testLastConsumerProtocolMemberRebalanceTimeoutInConsumerGroup() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2),
                        new TopicPartition(barTopicName, 0),
                        new TopicPartition(barTopicName, 1)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(30000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)), 10))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(30000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5),
                mkTopicAssignment(barTopicId, 2)), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addTopic(zarTopicId, zarTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage),
                    zarTopicName, computeTopicHash(zarTopicName, metadataImage)
                ))))
            .build();

        // Prepare the new assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)
            ))
        )));

        // Member 2 heartbeats with a different subscribedTopicNames. The assignor computes a new assignment
        // where member 2 will need to revoke topic partition bar-2 thus transitions to the REVOKING state.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setTopicPartitions(List.of(
                    new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(fooTopicId)
                        .setPartitions(List.of(3, 4, 5)),
                    new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(barTopicId)
                        .setPartitions(List.of(2))
                ))
        );

        // Verify that there is a rebalance timeout.
        context.assertRebalanceTimeout(groupId, memberId2, 30000);

        // Advance time past the session timeout.
        // Member 2 should be fenced from the group, thus triggering the downgrade.
        ExpiredTimeout<CoordinatorRecord> timeout = context.sleep(30000 + 1).get(0);
        assertEquals(groupRebalanceTimeoutKey(groupId, memberId2), timeout.key());

        byte[] assignment = Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
            new TopicPartition(fooTopicName, 0),
            new TopicPartition(fooTopicName, 1),
            new TopicPartition(fooTopicName, 2),
            new TopicPartition(barTopicName, 0),
            new TopicPartition(barTopicName, 1)
        ))));
        Map<String, byte[]> assignments = Map.of(memberId1, assignment);

        ClassicGroup expectedClassicGroup = new ClassicGroup(
            new LogContext(),
            groupId,
            STABLE,
            context.time,
            11,
            Optional.of(ConsumerProtocol.PROTOCOL_TYPE),
            Optional.of("range"),
            Optional.of(memberId1),
            Optional.of(context.time.milliseconds())
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                memberId1,
                Optional.ofNullable(member1.instanceId()),
                member1.clientId(),
                member1.clientHost(),
                member1.rebalanceTimeoutMs(),
                member1.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                member1.supportedJoinGroupRequestProtocols(),
                assignment
            )
        );

        assertUnorderedRecordsEquals(
            List.of(
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2)
                ),
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2)
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataTombstoneRecord(groupId)),
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2)
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId)),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId)),
                List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(expectedClassicGroup, assignments))
            ),
            timeout.result().records()
        );

        // The new classic member 1 has a heartbeat timeout.
        ScheduledTimeout<CoordinatorRecord> heartbeatTimeout = context.timer.timeout(
            classicGroupHeartbeatKey(groupId, memberId1)
        );
        assertNotNull(heartbeatTimeout);
        // The new rebalance has a groupJoin timeout.
        ScheduledTimeout<CoordinatorRecord> groupJoinTimeout = context.timer.timeout(
            classicGroupJoinKey(groupId)
        );
        assertNotNull(groupJoinTimeout);

        // A new rebalance is triggered.
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(classicGroup.isInState(PREPARING_REBALANCE));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLastStaticConsumerProtocolMemberReplacedByClassicProtocolMember(
        boolean isSubscriptionChanged
    ) throws ExecutionException, InterruptedException {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String oldMemberId2 = Uuid.randomUuid().toString();
        String instanceId = "instance-id";

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols1 = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName(NoOpPartitionAssignor.NAME)
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2),
                        new TopicPartition(barTopicName, 0),
                        new TopicPartition(barTopicName, 1)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols1)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)), 10))
            .build();
        ConsumerGroupMember oldMember2 = new ConsumerGroupMember.Builder(oldMemberId2)
            .setInstanceId(instanceId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 2)
            .addRacks()
            .buildCoordinatorMetadataImage();
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
        long barTopicHash = computeTopicHash(barTopicName, metadataImage);

        // Consumer group with two members.
        // Member 1 uses the classic protocol and static member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(oldMember2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(oldMemberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();

        context.groupMetadataManager.consumerGroup(groupId).setMetadataRefreshDeadline(Long.MAX_VALUE, 10);

        // A new member using classic protocol with the same instance id joins, scheduling the downgrade.
        byte[] protocolsMetadata2 = Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
            isSubscriptionChanged ? List.of(fooTopicName, barTopicName) : List.of(fooTopicName))));
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols2 =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols2.add(new JoinGroupRequestProtocol()
            .setName(NoOpPartitionAssignor.NAME)
            .setMetadata(protocolsMetadata2));
        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId(instanceId)
            .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .withProtocols(protocols2)
            .build();
        GroupMetadataManagerTestContext.JoinResult result = context.sendClassicGroupJoin(joinRequest);
        result.appendFuture.complete(null);
        String newMemberId2 = result.joinFuture.get().memberId();

        ConsumerGroupMember expectedNewConsumerMember2 = new ConsumerGroupMember.Builder(oldMember2, newMemberId2)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .build();
        ConsumerGroupMember expectedNewClassicMember2 = new ConsumerGroupMember.Builder(oldMember2, newMemberId2)
            .setPreviousMemberEpoch(0)
            .setMemberEpoch(isSubscriptionChanged ? 11 : 10)
            .setSubscribedTopicNames(isSubscriptionChanged ? List.of(fooTopicName, barTopicName) : List.of(fooTopicName))
            .setRebalanceTimeoutMs(joinRequest.rebalanceTimeoutMs())
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(joinRequest.sessionTimeoutMs())
                    .setSupportedProtocols(List.of(new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                        .setName(NoOpPartitionAssignor.NAME)
                        .setMetadata(protocolsMetadata2)))
            ).build();

        byte[] assignment1 = Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
            new TopicPartition(fooTopicName, 0),
            new TopicPartition(fooTopicName, 1),
            new TopicPartition(fooTopicName, 2),
            new TopicPartition(barTopicName, 0),
            new TopicPartition(barTopicName, 1)
        ))));
        byte[] assignment2 = Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
            new TopicPartition(fooTopicName, 3),
            new TopicPartition(fooTopicName, 4),
            new TopicPartition(fooTopicName, 5)
        ))));
        Map<String, byte[]> assignments = Map.of(
            memberId1, assignment1,
            newMemberId2, assignment2
        );

        ClassicGroup expectedClassicGroup = new ClassicGroup(
            new LogContext(),
            groupId,
            STABLE,
            context.time,
            10,
            Optional.of(ConsumerProtocol.PROTOCOL_TYPE),
            Optional.of(NoOpPartitionAssignor.NAME),
            Optional.of(memberId1),
            Optional.of(context.time.milliseconds())
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                memberId1,
                Optional.ofNullable(member1.instanceId()),
                member1.clientId(),
                member1.clientHost(),
                member1.rebalanceTimeoutMs(),
                member1.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                member1.supportedJoinGroupRequestProtocols(),
                assignment1
            )
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                newMemberId2,
                Optional.ofNullable(oldMember2.instanceId()),
                DEFAULT_CLIENT_ID,
                DEFAULT_CLIENT_ADDRESS.toString(),
                joinRequest.rebalanceTimeoutMs(),
                joinRequest.sessionTimeoutMs(),
                joinRequest.protocolType(),
                joinRequest.protocols(),
                assignment2
            )
        );

        // The leader of the classic group is not deterministic.
        String leader = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false).leaderOrNull();
        assertTrue(Set.of(memberId1, newMemberId2).contains(leader));
        expectedClassicGroup.setLeaderId(Optional.of(leader));

        List<List<CoordinatorRecord>> replacingRecords = List.of(
            // Remove the existing member 2 that uses the consumer protocol.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, oldMemberId2)),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, oldMemberId2)),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, oldMemberId2)),

            // Create the new member 2 that uses the consumer protocol.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedNewConsumerMember2)),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, newMemberId2, toAssignmentWithoutEpochs(expectedNewConsumerMember2.assignedPartitions()))),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedNewConsumerMember2))
        );

        List<List<CoordinatorRecord>> memberUpdateRecords;
        if (isSubscriptionChanged) {
            memberUpdateRecords = List.of(
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedNewClassicMember2)),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash
                ))))
            );
        } else {
            memberUpdateRecords = List.of(
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedNewClassicMember2)),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedNewClassicMember2))
            );
        }

        List<List<CoordinatorRecord>> downgradeRecords = List.of(
            // Remove member 1, member 2 and the consumer group.
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, newMemberId2)
            ),
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, newMemberId2)
            ),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataTombstoneRecord(groupId)),
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, newMemberId2)
            ),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId)),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId)),

            // Create the classic group.
            List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(expectedClassicGroup, assignments))
        );

        assertUnorderedRecordsEquals(
            Stream.of(replacingRecords, memberUpdateRecords, downgradeRecords)
                .flatMap(List::stream)
                .collect(Collectors.toList()),
            result.records
        );

        // The new classic member 1 has a heartbeat timeout.
        ScheduledTimeout<CoordinatorRecord> heartbeatTimeout = context.timer.timeout(
            classicGroupHeartbeatKey(groupId, memberId1)
        );
        assertNotNull(heartbeatTimeout);

        // If the subscription is changed, a rebalance is triggered.
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        if (isSubscriptionChanged) {
            assertTrue(classicGroup.isInState(PREPARING_REBALANCE));
        } else {
            assertTrue(classicGroup.isInState(STABLE));
        }
    }

    @Test
    public void testLastStaticConsumerProtocolMemberReplacedByClassicProtocolMemberWhenTargetAssignmentIsStale() throws ExecutionException, InterruptedException {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String oldMemberId2 = Uuid.randomUuid().toString();
        String instanceId = "instance-id";

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols1 = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName(NoOpPartitionAssignor.NAME)
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2),
                        new TopicPartition(barTopicName, 0),
                        new TopicPartition(barTopicName, 1)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols1)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0, 1)), 10))
            .build();
        ConsumerGroupMember oldMember2 = new ConsumerGroupMember.Builder(oldMemberId2)
            .setInstanceId(instanceId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 2)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and static member 2 uses the consumer protocol.
        // Member 2 has just changed subscription from foo to bar and the new assignment has not
        // been computed yet.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 11)
                .withMember(member1)
                .withMember(oldMember2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(oldMemberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();

        // A new member using classic protocol with the same instance id joins, scheduling the downgrade.
        byte[] protocolsMetadata2 = Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
            List.of(fooTopicName, barTopicName))));
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols2 =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols2.add(new JoinGroupRequestProtocol()
            .setName(NoOpPartitionAssignor.NAME)
            .setMetadata(protocolsMetadata2));
        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId(instanceId)
            .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .withProtocols(protocols2)
            .build();
        GroupMetadataManagerTestContext.JoinResult result = context.sendClassicGroupJoin(joinRequest);
        result.appendFuture.complete(null);
        result.joinFuture.get();

        // A rebalance is triggered.
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(classicGroup.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testLastStaticConsumerProtocolMemberReplacedByClassicProtocolMemberWhenTargetAssignmentIsMissing() throws ExecutionException, InterruptedException {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String oldMemberId2 = Uuid.randomUuid().toString();
        String instanceId = "instance-id";

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols1 = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName(NoOpPartitionAssignor.NAME)
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of()
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols1)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(), 10))
            .build();
        ConsumerGroupMember oldMember2 = new ConsumerGroupMember.Builder(oldMemberId2)
            .setInstanceId(instanceId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setServerAssignorName(NoOpPartitionAssignor.NAME)
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1)), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 2)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and static member 2 uses the consumer protocol.
        // Member 1 has just joined and does not have an assignment yet.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 11)
                .withMember(member1)
                .withMember(oldMember2)
                .withAssignment(oldMemberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();

        // A new member using classic protocol with the same instance id joins, scheduling the downgrade.
        byte[] protocolsMetadata2 = Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
            List.of(fooTopicName, barTopicName))));
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols2 =
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(1);
        protocols2.add(new JoinGroupRequestProtocol()
            .setName(NoOpPartitionAssignor.NAME)
            .setMetadata(protocolsMetadata2));
        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId(instanceId)
            .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .withProtocols(protocols2)
            .build();
        GroupMetadataManagerTestContext.JoinResult result = context.sendClassicGroupJoin(joinRequest);
        result.appendFuture.complete(null);
        result.joinFuture.get();

        // A rebalance is triggered.
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(classicGroup.isInState(PREPARING_REBALANCE));
    }

    @Test
    public void testJoiningConsumerGroupThrowsExceptionIfGroupOverMaxSize() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .build()))
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG, 1)
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withDefaultProtocolTypeAndProtocols()
            .build();

        Exception ex = assertThrows(GroupMaxSizeReachedException.class, () -> context.sendClassicGroupJoin(request));
        assertEquals("The consumer group has reached its maximum capacity of 1 members.", ex.getMessage());
    }

    @Test
    public void testJoiningConsumerGroupThrowsExceptionIfProtocolIsNotSupported() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                                GroupMetadataManagerTestContext.toProtocols("roundrobin")
                            ))
                    )
                    .build()))
            .build();

        JoinGroupRequestData requestWithEmptyProtocols = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .withDefaultProtocolTypeAndProtocols()
            .build();
        assertThrows(InconsistentGroupProtocolException.class, () -> context.sendClassicGroupJoin(requestWithEmptyProtocols));

        JoinGroupRequestData requestWithInvalidProtocolType = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withProtocolType("connect")
            .withDefaultProtocolTypeAndProtocols()
            .build();
        assertThrows(InconsistentGroupProtocolException.class, () -> context.sendClassicGroupJoin(requestWithInvalidProtocolType));
    }

    @Test
    public void testJoiningConsumerGroupWithNewDynamicMember() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        for (short version = ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION; version <= ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION; version++) {
            String memberId = Uuid.randomUuid().toString();
            MockPartitionAssignor assignor = new MockPartitionAssignor("range");

            CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .addTopic(barTopicId, barTopicName, 1)
                .addRacks()
                .buildCoordinatorMetadataImage();
            long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
            long barTopicHash = computeTopicHash(barTopicName, metadataImage);

            GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
                .withMetadataImage(metadataImage)
                .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                    .withMember(new ConsumerGroupMember.Builder(memberId)
                        .setState(MemberState.STABLE)
                        .setMemberEpoch(10)
                        .setPreviousMemberEpoch(10)
                        .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                            mkTopicAssignment(fooTopicId, 0, 1)), 10))
                        .build())
                    .withAssignment(memberId, mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)))
                    .withAssignmentEpoch(10)
                    .withMetadataHash(computeGroupHash(Map.of(
                        fooTopicName, fooTopicHash
                    ))))
                .build();

            JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(UNKNOWN_MEMBER_ID)
                .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                    List.of(fooTopicName, barTopicName),
                    List.of(),
                    version))
                .build();

            // The first round of join request gets the new member id.
            GroupMetadataManagerTestContext.JoinResult firstJoinResult = context.sendClassicGroupJoin(
                request,
                true
            );
            assertTrue(firstJoinResult.records.isEmpty());
            // Simulate a successful write to the log.
            firstJoinResult.appendFuture.complete(null);

            assertTrue(firstJoinResult.joinFuture.isDone());
            assertEquals(Errors.MEMBER_ID_REQUIRED.code(), firstJoinResult.joinFuture.get().errorCode());
            String newMemberId = firstJoinResult.joinFuture.get().memberId();
            assertNotEquals("", newMemberId);

            assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
                memberId, new MemberAssignmentImpl(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0)
                )),
                newMemberId, new MemberAssignmentImpl(mkAssignment(
                    mkTopicAssignment(barTopicId, 0)
                ))
            )));

            JoinGroupRequestData secondRequest = new JoinGroupRequestData()
                .setGroupId(request.groupId())
                .setMemberId(newMemberId)
                .setProtocolType(request.protocolType())
                .setProtocols(request.protocols())
                .setSessionTimeoutMs(request.sessionTimeoutMs())
                .setRebalanceTimeoutMs(request.rebalanceTimeoutMs())
                .setReason(request.reason());

            // Send second join group request for a new dynamic member with the new member id.
            GroupMetadataManagerTestContext.JoinResult secondJoinResult = context.sendClassicGroupJoin(
                secondRequest,
                true
            );

            ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(newMemberId)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(0)
                .setState(MemberState.STABLE)
                .setClientId(DEFAULT_CLIENT_ID)
                .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                .setRebalanceTimeoutMs(500)
                .setAssignedPartitions(toAssignmentWithEpochs(assignor.targetPartitions(newMemberId), 11))
                .setClassicMemberMetadata(
                    new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                        .setSessionTimeoutMs(request.sessionTimeoutMs())
                        .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(request.protocols()))
                )
                .build();

            assertUnorderedRecordsEquals(
                List.of(
                    List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember)),
                    List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                        fooTopicName, fooTopicHash,
                        barTopicName, barTopicHash
                    )))),

                    List.of(
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId, assignor.targetPartitions(memberId)),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, newMemberId, assignor.targetPartitions(newMemberId))
                    ),
                    List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds())),

                    List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember))
                ),
                secondJoinResult.records
            );

            secondJoinResult.appendFuture.complete(null);
            assertTrue(secondJoinResult.joinFuture.isDone());
            assertEquals(
                new JoinGroupResponseData()
                    .setMemberId(newMemberId)
                    .setGenerationId(11)
                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                    .setProtocolName("range"),
                secondJoinResult.joinFuture.get()
            );

            context.assertSessionTimeout(groupId, newMemberId, request.sessionTimeoutMs());
            context.assertSyncTimeout(groupId, newMemberId, request.rebalanceTimeoutMs());
        }
    }

    @Test
    public void testJoiningConsumerGroupFailingToPersistRecords() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        String memberId = Uuid.randomUuid().toString();
        String newMemberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)
            )),
            newMemberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)
            ))
        )));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addRacks()
            .buildCoordinatorMetadataImage();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)), 10))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage)
                ))))
            .build();
        context.commit();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(newMemberId)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName),
                List.of()))
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        // Simulate a failed write to the log.
        joinResult.appendFuture.completeExceptionally(new NotLeaderOrFollowerException());
        context.rollback();

        context.assertNoSessionTimeout(groupId, newMemberId);
        context.assertNoSyncTimeout(groupId, newMemberId);
        assertFalse(context.groupMetadataManager.consumerGroup(groupId).hasMember(newMemberId));
    }

    @Test
    public void testJoiningConsumerGroupWithNewStaticMember() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        String memberId = Uuid.randomUuid().toString();
        String instanceId = "instance-id";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
        long barTopicHash = computeTopicHash(barTopicName, metadataImage);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)), 10))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(fooTopicName, fooTopicHash))))
            .build();

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId(instanceId)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName),
                List.of()))
            .build();

        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        // Simulate a successful write to log.
        joinResult.appendFuture.complete(null);
        String newMemberId = joinResult.joinFuture.get().memberId();
        assertNotEquals("", newMemberId);

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(newMemberId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(0)
            .setInstanceId(instanceId)
            .setState(MemberState.STABLE)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
            .setRebalanceTimeoutMs(500)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(request.protocols()))
            )
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, fooTopicHash,
                barTopicName, barTopicHash
            ))),

            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, newMemberId, Map.of()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),

            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );
        assertRecordsEquals(expectedRecords, joinResult.records);

        assertTrue(joinResult.joinFuture.isDone());
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(newMemberId)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResult.joinFuture.get()
        );

        context.assertSessionTimeout(groupId, newMemberId, request.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, newMemberId, request.rebalanceTimeoutMs());
    }

    @Test
    public void testJoiningConsumerGroupReplacingExistingStaticMember() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addRacks()
            .buildCoordinatorMetadataImage();

        String memberId = Uuid.randomUuid().toString();
        String instanceId = "instance-id";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DISABLED.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new NoOpPartitionAssignor()))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setInstanceId(instanceId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setSubscribedTopicNames(List.of(fooTopicName))
                    .setRebalanceTimeoutMs(500)
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)), 10))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage)
                ))))
            .build();
        context.groupMetadataManager.consumerGroup(groupId).setMetadataRefreshDeadline(Long.MAX_VALUE, 10);

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId(instanceId)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName),
                List.of()))
            .build();

        // The static member joins with UNKNOWN_MEMBER_ID.
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(
            request,
            true
        );

        // Simulate a successful write to log.
        joinResult.appendFuture.complete(null);
        String newMemberId = joinResult.joinFuture.get().memberId();
        assertNotEquals("", newMemberId);

        ConsumerGroupMember expectedCopiedMember = new ConsumerGroupMember.Builder(newMemberId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setInstanceId(instanceId)
            .setState(MemberState.STABLE)
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)), 10))
            .setRebalanceTimeoutMs(500)
            .build();

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(newMemberId)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(0)
            .setInstanceId(instanceId)
            .setState(MemberState.STABLE)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1)), 10))
            .setRebalanceTimeoutMs(500)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(request.protocols())))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // Remove the old static member.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),

            // Replace the old static member by the new static member.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedCopiedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, newMemberId, mkAssignment(mkTopicAssignment(fooTopicId, 0, 1))),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedCopiedMember),

            // Updated the new static member.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember)
        );
        assertRecordsEquals(expectedRecords, joinResult.records);
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(newMemberId)
                .setGenerationId(10)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResult.joinFuture.get()
        );

        context.assertSessionTimeout(groupId, newMemberId, request.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, newMemberId, request.rebalanceTimeoutMs());
    }

    @Test
    public void testJoiningConsumerGroupWithExistingStaticMemberAndNewSubscription() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String instanceId = "instance-id";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addTopic(barTopicId, barTopicName, 1)
            .addTopic(zarTopicId, zarTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();
        long groupMetadataHash = computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, metadataImage),
            barTopicName, computeTopicHash(barTopicName, metadataImage),
            zarTopicName, computeTopicHash(zarTopicName, metadataImage)
        ));

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setInstanceId(instanceId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0),
                        mkTopicAssignment(barTopicId, 0)), 10))
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                                GroupMetadataManagerTestContext.toConsumerProtocol(
                                    List.of(fooTopicName, barTopicName),
                                    List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(fooTopicName, 1))
                                )
                            ))
                    )
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 1)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0),
                    mkTopicAssignment(barTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 1)))
                .withAssignmentEpoch(10)
                .withMetadataHash(groupMetadataHash))
            .build();
        ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
        group.setMetadataRefreshDeadline(Long.MAX_VALUE, 11);

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0),
                mkTopicAssignment(zarTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0),
                mkTopicAssignment(fooTopicId, 1)
            ))
        )));

        // Member 1 rejoins with a new subscription list.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName, zarTopicName),
                List.of()))
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult = context.sendClassicGroupJoin(request);

        // foo partition 0 retains epoch 10 (from original), zar partition 0 gets epoch 11 (newly assigned)
        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId1)
            .setInstanceId(instanceId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setRebalanceTimeoutMs(500)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setState(MemberState.STABLE)
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName, zarTopicName))
            .setAssignedPartitions(mkAssignmentWithEpochs(
                mkTopicAssignmentWithEpochs(fooTopicId, 10, 0),
                mkTopicAssignmentWithEpochs(zarTopicId, 11, 0)
            ))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                        GroupMetadataManagerTestContext.toConsumerProtocol(
                            List.of(fooTopicName, barTopicName, zarTopicName),
                            List.of()
                        )
                    ))
            )
            .build();

        assertUnorderedRecordsEquals(
            List.of(
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember)),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, groupMetadataHash)),
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                        mkTopicAssignment(fooTopicId, 0),
                        mkTopicAssignment(zarTopicId, 0))),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                        mkTopicAssignment(barTopicId, 0),
                        mkTopicAssignment(fooTopicId, 1)))
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds())),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember))
            ),
            joinResult.records
        );

        joinResult.appendFuture.complete(null);
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResult.joinFuture.get()
        );
        context.assertSessionTimeout(groupId, memberId1, request.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request.rebalanceTimeoutMs());
    }

    @Test
    public void testStaticMemberJoiningConsumerGroupWithUnknownInstanceId() throws Exception {
        String groupId = "group-id";
        String instanceId = "instance-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String fooTopicName = "foo";
        String barTopicName = "bar";

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName),
                List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(fooTopicName, 1))
            );
        // Set up a ConsumerGroup with no static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols))
                    )
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .build()))
            .build();

        // The member joins with an instance id.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withGroupInstanceId(instanceId)
            .withProtocols(protocols)
            .build();

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupJoin(request));
    }

    @Test
    public void testStaticMemberJoiningConsumerGroupWithUnmatchedMemberId() throws Exception {
        String groupId = "group-id";
        String instanceId = "instance-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String fooTopicName = "foo";
        String barTopicName = "bar";

        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocols =
            GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName),
                List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(fooTopicName, 1))
            );
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setInstanceId(instanceId)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(protocols))
                    )
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .build()))
            .build();

        // The member joins with the same instance id and a different member id.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(Uuid.randomUuid().toString())
            .withGroupInstanceId(instanceId)
            .withProtocols(protocols)
            .build();

        assertThrows(FencedInstanceIdException.class, () -> context.sendClassicGroupJoin(request));
    }

    @Test
    public void testReconciliationInJoiningConsumerGroupWithEagerProtocol() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addTopic(barTopicId, barTopicName, 1)
            .addTopic(zarTopicId, zarTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
        long barTopicHash = computeTopicHash(barTopicName, metadataImage);
        long zarTopicHash = computeTopicHash(zarTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0),
                        mkTopicAssignment(barTopicId, 0)), 10))
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                                GroupMetadataManagerTestContext.toConsumerProtocol(
                                    List.of(fooTopicName, barTopicName),
                                    List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(barTopicName, 0))
                                )
                            ))
                    )
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 1)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0),
                    mkTopicAssignment(barTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 1)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash
                ))))
            .build();
        ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
        group.setMetadataRefreshDeadline(Long.MAX_VALUE, 11);

        // Prepare the new target assignment.
        // Member 1 will need to revoke bar-0, and member 2 will need to revoke foo-1.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(zarTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0)
            ))
        )));

        // Member 1 rejoins with a new subscription list and an empty owned
        // partition, and transitions to UNRELEASED_PARTITIONS.
        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withSessionTimeoutMs(5000)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName, zarTopicName),
                List.of()))
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult1 = context.sendClassicGroupJoin(request);

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setRebalanceTimeoutMs(500)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName, zarTopicName))
            .setAssignedPartitions(Map.of(
                fooTopicId, Map.of(0, 10),
                zarTopicId, Map.of(0, 11)))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                        GroupMetadataManagerTestContext.toConsumerProtocol(
                            List.of(fooTopicName, barTopicName, zarTopicName),
                            List.of()
                        )
                    ))
            )
            .build();

        assertUnorderedRecordsEquals(
            List.of(
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1)),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash,
                    zarTopicName, zarTopicHash
                )))),

                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1),
                        mkTopicAssignment(zarTopicId, 0))),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                        mkTopicAssignment(barTopicId, 0)))
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds())),

                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1))
            ),
            joinResult1.records
        );

        assertEquals(expectedMember1.state(), group.getOrMaybeCreateMember(memberId1, false).state());

        joinResult1.appendFuture.complete(null);
        JoinGroupResponseData joinResponse1 = joinResult1.joinFuture.get();
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResponse1
        );
        context.assertSessionTimeout(groupId, memberId1, request.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request.rebalanceTimeoutMs());

        // Member 1 sends sync request to get the assigned partitions.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            joinResponse1.memberId(),
            joinResponse1.generationId(),
            joinResponse1.protocolName(),
            joinResponse1.protocolType(),
            List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(zarTopicName, 0)
            )
        );

        // Member 2 heartbeats to confirm revoking foo-1.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setTopicPartitions(List.of())
        );

        // Member 1 heartbeats to be notified to rejoin.
        assertEquals(
            Errors.REBALANCE_IN_PROGRESS.code(),
            context.sendClassicGroupHeartbeat(
                new HeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setGenerationId(joinResponse1.generationId())
            ).response().errorCode()
        );
        context.assertJoinTimeout(groupId, memberId1, 500);

        // Member 1 rejoins to transition from UNRELEASED_PARTITIONS to STABLE.
        GroupMetadataManagerTestContext.JoinResult joinResult2 = context.sendClassicGroupJoin(request);
        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(expectedMember1)
            .setState(MemberState.STABLE)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(
                mkAssignmentWithEpochs(
                    mkTopicAssignmentWithEpochs(fooTopicId, 10, 0),
                    mkTopicAssignmentWithEpochs(fooTopicId, 11, 1),
                    mkTopicAssignmentWithEpochs(zarTopicId, 11, 0)))
            .build();

        assertRecordsEquals(
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)),
            joinResult2.records
        );
        assertEquals(expectedMember2.state(), group.getOrMaybeCreateMember(memberId1, false).state());

        joinResult2.appendFuture.complete(null);
        context.assertNoJoinTimeout(groupId, memberId1);
        JoinGroupResponseData joinResponse2 = joinResult2.joinFuture.get();
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResponse2
        );
        context.assertSessionTimeout(groupId, memberId1, request.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request.rebalanceTimeoutMs());

        // Member 1 sends sync request to get the assigned partitions.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            joinResponse2.memberId(),
            joinResponse2.generationId(),
            joinResponse2.protocolName(),
            joinResponse2.protocolType(),
            List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(fooTopicName, 1),
                new TopicPartition(zarTopicName, 0)
            )
        );
    }

    @Test
    public void testReconciliationInJoiningConsumerGroupWithCooperativeProtocol() throws Exception {
        String groupId = "group-id";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addTopic(barTopicId, barTopicName, 1)
            .addTopic(zarTopicId, zarTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
        long barTopicHash = computeTopicHash(barTopicName, metadataImage);
        long zarTopicHash = computeTopicHash(zarTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0),
                        mkTopicAssignment(barTopicId, 0)), 10))
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                                GroupMetadataManagerTestContext.toConsumerProtocol(
                                    List.of(fooTopicName, barTopicName),
                                    List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(barTopicName, 0))
                                )
                            ))
                    )
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setRebalanceTimeoutMs(500)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of(fooTopicName, barTopicName))
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 1)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0),
                    mkTopicAssignment(barTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 1)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash
                ))))
            .build();
        ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
        group.setMetadataRefreshDeadline(Long.MAX_VALUE, 11);

        // Prepare the new target assignment.
        // Member 1 will need to revoke bar-0, and member 2 will need to revoke foo-1.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(zarTopicId, 0)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0)
            ))
        )));

        // Member 1 rejoins with a new subscription list and transitions to UNREVOKED_PARTITIONS.
        JoinGroupRequestData request1 = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withSessionTimeoutMs(5000)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName, zarTopicName),
                List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(barTopicName, 0))))
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult1 = context.sendClassicGroupJoin(request1);

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setRebalanceTimeoutMs(500)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setSubscribedTopicNames(List.of(fooTopicName, barTopicName, zarTopicName))
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)), 10))
            .setPartitionsPendingRevocation(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(barTopicId, 0)), 10))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request1.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                        GroupMetadataManagerTestContext.toConsumerProtocol(
                            List.of(fooTopicName, barTopicName, zarTopicName),
                            List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(barTopicName, 0))
                        )
                    ))
            )
            .build();

        assertUnorderedRecordsEquals(
            List.of(
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1)),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash,
                    zarTopicName, zarTopicHash
                )))),

                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1),
                        mkTopicAssignment(zarTopicId, 0))),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                        mkTopicAssignment(barTopicId, 0)))
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds())),

                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1))
            ),
            joinResult1.records
        );

        assertEquals(expectedMember1.state(), group.getOrMaybeCreateMember(memberId1, false).state());

        joinResult1.appendFuture.complete(null);
        JoinGroupResponseData joinResponse1 = joinResult1.joinFuture.get();
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(10)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResponse1
        );
        context.assertSessionTimeout(groupId, memberId1, request1.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request1.rebalanceTimeoutMs());

        // Member 1 sends sync request to get the assigned partitions.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            joinResponse1.memberId(),
            joinResponse1.generationId(),
            joinResponse1.protocolName(),
            joinResponse1.protocolType(),
            List.of(new TopicPartition(fooTopicName, 0))
        );

        // Member 1 heartbeats to be notified to rejoin.
        assertEquals(
            Errors.REBALANCE_IN_PROGRESS.code(),
            context.sendClassicGroupHeartbeat(
                new HeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setGenerationId(joinResponse1.generationId())
            ).response().errorCode()
        );
        context.assertJoinTimeout(groupId, memberId1, 500);

        // Member 1 rejoins to transition from UNREVOKED_PARTITIONS to UNRELEASED_PARTITIONS.
        JoinGroupRequestData request2 = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withSessionTimeoutMs(5000)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName, zarTopicName),
                List.of(new TopicPartition(fooTopicName, 0))))
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult2 = context.sendClassicGroupJoin(request2);

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(expectedMember1)
            .setMemberEpoch(11)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setPartitionsPendingRevocation(Map.of())
            .setAssignedPartitions(Map.of(
                fooTopicId, Map.of(0, 10),
                zarTopicId, Map.of(0, 11)))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request2.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                        GroupMetadataManagerTestContext.toConsumerProtocol(
                            List.of(fooTopicName, barTopicName, zarTopicName),
                            List.of(new TopicPartition(fooTopicName, 0))
                        )
                    ))
            )
            .build();

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
            ),
            joinResult2.records
        );
        assertEquals(expectedMember2.state(), group.getOrMaybeCreateMember(memberId1, false).state());

        joinResult2.appendFuture.complete(null);
        context.assertNoJoinTimeout(groupId, memberId1);
        JoinGroupResponseData joinResponse2 = joinResult2.joinFuture.get();
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResponse2
        );
        context.assertSessionTimeout(groupId, memberId1, request2.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request2.rebalanceTimeoutMs());

        // Member 1 sends sync request to get the assigned partitions.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            joinResponse2.memberId(),
            joinResponse2.generationId(),
            joinResponse2.protocolName(),
            joinResponse2.protocolType(),
            List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(zarTopicName, 0)
            )
        );

        // Member 2 heartbeats to confirm revoking foo-1.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setTopicPartitions(List.of())
        );

        // Member 1 heartbeats to be notified to rejoin.
        assertEquals(
            Errors.REBALANCE_IN_PROGRESS.code(),
            context.sendClassicGroupHeartbeat(
                new HeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setGenerationId(joinResponse2.generationId())
            ).response().errorCode()
        );
        context.assertJoinTimeout(groupId, memberId1, 500);

        // Member 1 rejoins to transition from UNRELEASED_PARTITIONS to STABLE.
        JoinGroupRequestData request3 = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(memberId1)
            .withSessionTimeoutMs(5000)
            .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                List.of(fooTopicName, barTopicName, zarTopicName),
                List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(zarTopicName, 0))))
            .build();
        GroupMetadataManagerTestContext.JoinResult joinResult3 = context.sendClassicGroupJoin(request3);

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(expectedMember2)
            .setState(MemberState.STABLE)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(
                mkAssignmentWithEpochs(
                    mkTopicAssignmentWithEpochs(fooTopicId, 10, 0),
                    mkTopicAssignmentWithEpochs(fooTopicId, 11, 1),
                    mkTopicAssignmentWithEpochs(zarTopicId, 11, 0)
                ))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(request3.sessionTimeoutMs())
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                        GroupMetadataManagerTestContext.toConsumerProtocol(
                            List.of(fooTopicName, barTopicName, zarTopicName),
                            List.of(new TopicPartition(fooTopicName, 0), new TopicPartition(zarTopicName, 0))
                        )
                    ))
            )
            .build();

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember3),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember3)
            ),
            joinResult3.records
        );
        assertEquals(expectedMember3.state(), group.getOrMaybeCreateMember(memberId1, false).state());

        joinResult3.appendFuture.complete(null);
        context.assertNoJoinTimeout(groupId, memberId1);
        JoinGroupResponseData joinResponse3 = joinResult3.joinFuture.get();
        assertEquals(
            new JoinGroupResponseData()
                .setMemberId(memberId1)
                .setGenerationId(11)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setProtocolName("range"),
            joinResponse3
        );
        context.assertSessionTimeout(groupId, memberId1, request3.sessionTimeoutMs());
        context.assertSyncTimeout(groupId, memberId1, request3.rebalanceTimeoutMs());

        // Member 1 sends sync request to get the assigned partitions.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            joinResponse3.memberId(),
            joinResponse3.generationId(),
            joinResponse3.protocolName(),
            joinResponse3.protocolType(),
            List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(fooTopicName, 1),
                new TopicPartition(zarTopicName, 0)
            )
        );
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupWithAllConsumerProtocolVersions() throws Exception {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        for (short version = ConsumerProtocolAssignment.LOWEST_SUPPORTED_VERSION; version <= ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION; version++) {
            List<TopicPartition> topicPartitions = List.of(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(fooTopicName, 1),
                new TopicPartition(fooTopicName, 2),
                new TopicPartition(barTopicName, 0),
                new TopicPartition(barTopicName, 1)
            );

            List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
                new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                    .setName("range")
                    .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                        new ConsumerPartitionAssignor.Subscription(
                            List.of(fooTopicName, barTopicName),
                            null,
                            topicPartitions
                        ),
                        version
                    )))
            );

            ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
                .setState(MemberState.STABLE)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(9)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setClassicMemberMetadata(
                    new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                        .setSessionTimeoutMs(5000)
                        .setSupportedProtocols(protocols)
                )
                .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)), 10))
                .build();
            ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
                .setState(MemberState.STABLE)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(9)
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)), 10))
                .build();

            // Consumer group with two members.
            // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
            GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
                .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
                .withMetadataImage(new MetadataImageBuilder()
                    .addTopic(fooTopicId, fooTopicName, 6)
                    .addTopic(barTopicId, barTopicName, 3)
                    .addRacks()
                    .buildCoordinatorMetadataImage())
                .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                    .withMember(member1)
                    .withMember(member2)
                    .withAssignment(memberId1, mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .withAssignment(memberId2, mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .withAssignmentEpoch(10))
                .build();

            context.verifyClassicGroupSyncToConsumerGroup(
                groupId,
                memberId1,
                10,
                "range",
                ConsumerProtocol.PROTOCOL_TYPE,
                topicPartitions,
                version
            );
        }
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupWithUnknownMemberId() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();

        // Consumer group with a member that doesn't use the classic protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .build()))
            .build();

        // Request with unknown member id.
        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(Uuid.randomUuid().toString())
                .withGenerationId(10)
                .withProtocolName("range")
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .build())
        );

        // Request with unknown instance id.
        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGroupInstanceId("unknown-instance-id")
                .withGenerationId(10)
                .withProtocolName("range")
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .build())
        );

        // Request with member id that doesn't use the classic protocol.
        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(10)
                .withProtocolName("range")
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .build())
        );
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupWithFencedInstanceId() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        String instanceId = "instance-id";

        // Consumer group with a static member.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setInstanceId(instanceId)
                    .build()))
            .build();

        assertThrows(FencedInstanceIdException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(Uuid.randomUuid().toString())
                .withGroupInstanceId(instanceId)
                .withGenerationId(10)
                .withProtocolName("range")
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .build())
        );
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupWithInconsistentGroupProtocol() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        List.of()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(protocols)
                    )
                    .setMemberEpoch(10)
                    .build()))
            .build();

        // Request with unmatched protocol name.
        assertThrows(InconsistentGroupProtocolException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(10)
                .withProtocolName("roundrobin")
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .build())
        );

        // Request with unmatched protocol type.
        assertThrows(InconsistentGroupProtocolException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(10)
                .withProtocolName("range")
                .withProtocolType("connect")
                .build())
        );

        // Request with null protocol type or null protocol name won't fail the validation.
        context.verifyClassicGroupSyncToConsumerGroup(
            groupId,
            memberId,
            10,
            null,
            null,
            List.of()
        );
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupWithIllegalGeneration() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        List.of()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(protocols)
                    )
                    .setMemberEpoch(10)
                    .build()))
            .build();

        assertThrows(IllegalGenerationException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(9)
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .withProtocolName("range")
                .build())
        );
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupRebalanceInProgress() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        List.of()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol.
        // The target assignment epoch is greater than the member epoch.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 11)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setRebalanceTimeoutMs(10000)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(protocols)
                    )
                    .setMemberEpoch(10)
                    .build())
                .withAssignmentEpoch(11))
            .build();

        assertThrows(RebalanceInProgressException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId)
                .withGenerationId(10)
                .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .withProtocolName("range")
                .build())
        );
        context.assertJoinTimeout(groupId, memberId, 10000);
    }

    @Test
    public void testClassicGroupSyncToConsumerGroupDuringAssignmentDelay() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(barTopicName))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        // Member 2 has just changed subscription from foo to bar and the new assignment has not
        // been computed yet.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 11)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();

        // Member 1 is not told to rebalance yet.
        assertDoesNotThrow(() -> {
            GroupMetadataManagerTestContext.SyncResult syncResult = context.sendClassicGroupSync(
                new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                    .withGroupId(groupId)
                    .withMemberId(memberId1)
                    .withGenerationId(10)
                    .build()
            );
            syncResult.appendFuture.complete(null);
        });

        // Member 2 heartbeats and triggers a new assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0, 1, 2)
            ))
        )));
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result2 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10));
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0, 1, 2))
                    ))),
            result2.response()
        );

        // Member 1 is told to rebalance now that the new assignment is available.
        assertThrows(RebalanceInProgressException.class, () -> context.sendClassicGroupSync(
            new GroupMetadataManagerTestContext.SyncGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId1)
                .withGenerationId(10)
                .build())
        );
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerGroupMaintainsSession() throws Exception {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        int sessionTimeout = 5000;

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        List.of()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
            .withMember(new ConsumerGroupMember.Builder(memberId)
                .setClassicMemberMetadata(
                    new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                        .setSessionTimeoutMs(sessionTimeout)
                        .setSupportedProtocols(protocols)
                )
                .setMemberEpoch(10)
                .build()))
            .build();

        // Heartbeat to schedule the session timeout.
        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setGenerationId(10);
        context.sendClassicGroupHeartbeat(request);
        context.assertSessionTimeout(groupId, memberId, sessionTimeout);

        // Advance clock by 1/2 of session timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(sessionTimeout / 2));

        HeartbeatResponseData heartbeatResponse = context.sendClassicGroupHeartbeat(request).response();
        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
        context.assertSessionTimeout(groupId, memberId, sessionTimeout);

        // Advance clock by 1/2 of session timeout.
        GroupMetadataManagerTestContext.assertNoOrEmptyResult(context.sleep(sessionTimeout / 2));

        heartbeatResponse = context.sendClassicGroupHeartbeat(request).response();
        assertEquals(Errors.NONE.code(), heartbeatResponse.errorCode());
        context.assertSessionTimeout(groupId, memberId, sessionTimeout);
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerGroupRebalanceInProgress() throws Exception {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        int sessionTimeout = 5000;
        int rebalanceTimeout = 10000;

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        List.of()
                    )
                )))
        );

        // Member 1 has a member epoch smaller than the target assignment epoch.
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setRebalanceTimeoutMs(rebalanceTimeout)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(sessionTimeout)
                    .setSupportedProtocols(protocols)
            )
            .setMemberEpoch(9)
            .build();

        // Member 2 has unrevoked partition.
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setRebalanceTimeoutMs(rebalanceTimeout)
            .setPartitionsPendingRevocation(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0)), 10))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(sessionTimeout)
                    .setSupportedProtocols(protocols)
            )
            .setMemberEpoch(10)
            .build();

        // Member 3 is in UNRELEASED_PARTITIONS and all the partitions in its target assignment are free.
        ConsumerGroupMember member3 = new ConsumerGroupMember.Builder(memberId3)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setRebalanceTimeoutMs(rebalanceTimeout)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(barTopicId, 0)), 10))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(sessionTimeout)
                    .setSupportedProtocols(protocols)
            )
            .setMemberEpoch(10)
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withMember(member3)
                .withAssignment(memberId3, mkAssignment(mkTopicAssignment(barTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        List.of(memberId1, memberId2, memberId3).forEach(memberId -> {
            CoordinatorResult<HeartbeatResponseData, CoordinatorRecord> heartbeatResult = context.sendClassicGroupHeartbeat(
                new HeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setGenerationId(memberId.equals(memberId1) ? 9 : 10)
            );
            assertEquals(List.of(), heartbeatResult.records());
            assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), heartbeatResult.response().errorCode());
            context.assertSessionTimeout(groupId, memberId, sessionTimeout);
            context.assertJoinTimeout(groupId, memberId, rebalanceTimeout);
        });
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerWithUnknownMember() {
        String groupId = "group-id";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10))
            .build();

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId("unknown-member-id")
                .setGenerationId(10)
        ));

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId("unknown-member-id")
                .setGroupInstanceId("unknown-instance-id")
                .setGenerationId(10)
        ));
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerWithFencedInstanceId() {
        String groupId = "group-id";
        String memberId = "member-id";
        String instanceId = "instance-id";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setInstanceId(instanceId)
                    .setMemberEpoch(10)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(List.of())
                    )
                    .build()))
            .build();

        assertThrows(FencedInstanceIdException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId("unknown-member-id")
                .setGroupInstanceId(instanceId)
                .setGenerationId(10)
        ));
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerWithIllegalGenerationId() {
        String groupId = "group-id";
        String memberId = "member-id";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setMemberEpoch(10)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(List.of())
                    )
                    .build()))
            .build();

        assertThrows(IllegalGenerationException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setGenerationId(9)
        ));
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerWithMemberNotUsingClassicProtocol() {
        String groupId = "group-id";
        String memberId = "member-id";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setMemberEpoch(10)
                    .build()))
            .build();

        assertThrows(UnknownMemberIdException.class, () -> context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setGenerationId(10)
        ));
    }

    @Test
    public void testClassicGroupHeartbeatToConsumerGroupDuringAssignmentDelay() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName),
                    null,
                    List.of(
                        new TopicPartition(fooTopicName, 0),
                        new TopicPartition(fooTopicName, 1),
                        new TopicPartition(fooTopicName, 2)
                    )
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(barTopicName))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Consumer group with two members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        // Member 2 has just changed subscription from foo to bar and the new assignment has not
        // been computed yet.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 11)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();

        // Member 1 is not told to rebalance yet.
        HeartbeatResponseData heartbeatResponse1 = context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setGenerationId(10)
        ).response();
        assertEquals(Errors.NONE.code(), heartbeatResponse1.errorCode());

        // Member 2 heartbeats and triggers a new assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(barTopicId, 0, 1, 2)
            ))
        )));
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10));
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0, 1, 2))
                    ))),
            result.response()
        );

        // Member 1 is told to rebalance now that the new assignment is available.
        HeartbeatResponseData heartbeatResponse2 = context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setGenerationId(10)
        ).response();
        assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), heartbeatResponse2.errorCode());
    }

    @Test
    public void testConsumerGroupMemberUsingClassicProtocolFencedWhenSessionTimeout() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        int sessionTimeout = 5000;

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        List.of()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(sessionTimeout)
                            .setSupportedProtocols(protocols)
                    )
                    .setMemberEpoch(10)
                    .build()))
            .build();

        // Heartbeat to schedule the session timeout.
        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setGenerationId(10);
        context.sendClassicGroupHeartbeat(request);
        context.assertSessionTimeout(groupId, memberId, sessionTimeout);

        // Advance clock by session timeout + 1.
        List<ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(sessionTimeout + 1);

        // The member is fenced from the group.
        assertEquals(1, timeouts.size());
        ExpiredTimeout<CoordinatorRecord> timeout = timeouts.get(0);
        assertEquals(groupSessionTimeoutKey(groupId, memberId), timeout.key());
        assertRecordsEquals(
            List.of(
                // The member is removed.
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),

                // The group epoch is bumped.
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, 0),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, 0L)
            ),
            timeout.result().records()
        );
    }

    @Test
    public void testConsumerGroupMemberUsingClassicProtocolFencedWhenJoinTimeout() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();
        int rebalanceTimeout = 500;

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(
                    new ConsumerPartitionAssignor.Subscription(
                        List.of("foo"),
                        null,
                        List.of()
                    )
                )))
        );

        // Consumer group with a member using the classic protocol whose member epoch is smaller than the target assignment epoch.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setRebalanceTimeoutMs(rebalanceTimeout)
                    .setClassicMemberMetadata(
                        new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                            .setSessionTimeoutMs(5000)
                            .setSupportedProtocols(protocols)
                    )
                    .setMemberEpoch(9)
                    .build())
                .withAssignmentEpoch(10))
            .build();

        // Heartbeat to schedule the join timeout.
        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId)
            .setGenerationId(9);
        assertEquals(
            Errors.REBALANCE_IN_PROGRESS.code(),
            context.sendClassicGroupHeartbeat(request).response().errorCode()
        );
        context.assertSessionTimeout(groupId, memberId, 5000);
        context.assertJoinTimeout(groupId, memberId, rebalanceTimeout);

        // Advance clock by rebalance timeout + 1.
        List<ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(rebalanceTimeout + 1);

        // The member is fenced from the group.
        assertEquals(1, timeouts.size());
        ExpiredTimeout<CoordinatorRecord> timeout = timeouts.get(0);
        assertEquals(consumerGroupJoinKey(groupId, memberId), timeout.key());
        assertRecordsEquals(
            List.of(
                // The member is removed.
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId),

                // The group epoch is bumped.
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, 0),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, 0L)
            ),
            timeout.result().records()
        );
    }

    @Test
    public void testConsumerGroupMemberUsingClassicProtocolBatchLeaveGroup() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();
        String instanceId2 = "instance-id-2";
        String instanceId3 = "instance-id-3";

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocol1 = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(new TopicPartition(fooTopicName, 0))
                ))))
        );
        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocol2 = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(new TopicPartition(fooTopicName, 1))
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocol1)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0)), 11))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setInstanceId(instanceId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(9)
            .setPreviousMemberEpoch(8)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocol2)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 1)), 10))
            .build();
        ConsumerGroupMember member3 = new ConsumerGroupMember.Builder(memberId3)
            .setInstanceId(instanceId3)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(barTopicId, 0)), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Consumer group with three members.
        // Dynamic member 1 uses the classic protocol.
        // Static member 2 uses the classic protocol.
        // Static member 3 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withMember(member3)
                .withAssignment(memberId1, mkAssignment(mkTopicAssignment(fooTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(mkTopicAssignment(fooTopicId, 1)))
                .withAssignment(memberId3, mkAssignment(mkTopicAssignment(barTopicId, 0)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();
        context.groupMetadataManager.consumerGroup(groupId).setMetadataRefreshDeadline(Long.MAX_VALUE, 10);

        // Member 1 joins to schedule the sync timeout and the heartbeat timeout.
        context.sendClassicGroupJoin(
            new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
                .withGroupId(groupId)
                .withMemberId(memberId1)
                .withRebalanceTimeoutMs(member1.rebalanceTimeoutMs())
                .withSessionTimeoutMs(member1.classicMemberMetadata().get().sessionTimeoutMs())
                .withProtocols(GroupMetadataManagerTestContext.toConsumerProtocol(
                    List.of(fooTopicName, barTopicName),
                    List.of(new TopicPartition(fooTopicName, 0))))
                .build()
        ).appendFuture.complete(null);
        context.assertSyncTimeout(groupId, memberId1, member1.rebalanceTimeoutMs());
        context.assertSessionTimeout(groupId, memberId1, member1.classicMemberMetadata().get().sessionTimeoutMs());

        // Member 2 heartbeats to schedule the join timeout and the heartbeat timeout.
        context.sendClassicGroupHeartbeat(
            new HeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setGenerationId(9)
        );
        context.assertJoinTimeout(groupId, memberId2, member2.rebalanceTimeoutMs());
        context.assertSessionTimeout(groupId, memberId2, member2.classicMemberMetadata().get().sessionTimeoutMs());

        // Member 1, member 2 and member 3 leave the group.
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    // Valid member id.
                    new MemberIdentity()
                        .setMemberId(memberId1)
                        .setGroupInstanceId(null),
                    new MemberIdentity()
                        .setMemberId(UNKNOWN_MEMBER_ID)
                        .setGroupInstanceId(instanceId2),
                    // Member that doesn't use the classic protocol.
                    new MemberIdentity()
                        .setMemberId(memberId3)
                        .setGroupInstanceId(instanceId3),
                    // Unknown member id.
                    new MemberIdentity()
                        .setMemberId("unknown-member-id")
                        .setGroupInstanceId(null),
                    new MemberIdentity()
                        .setMemberId(UNKNOWN_MEMBER_ID)
                        .setGroupInstanceId("unknown-instance-id"),
                    // Fenced instance id.
                    new MemberIdentity()
                        .setMemberId("unknown-member-id")
                        .setGroupInstanceId(instanceId3)
                ))
        );

        assertEquals(
            new LeaveGroupResponseData()
                .setMembers(List.of(
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(null)
                        .setMemberId(memberId1),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(instanceId2)
                        .setMemberId(UNKNOWN_MEMBER_ID),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(instanceId3)
                        .setMemberId(memberId3),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(null)
                        .setMemberId("unknown-member-id")
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId("unknown-instance-id")
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId("unknown-member-id")
                        .setGroupInstanceId(instanceId3)
                        .setErrorCode(Errors.FENCED_INSTANCE_ID.code())
                )),
            leaveResult.response()
        );

        List<List<CoordinatorRecord>> expectedRecords = List.of(
            List.of(
                // Remove member 1
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
                // Remove member 2.
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
                // Remove member 3.
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId3),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId3),
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId3)
            ),
            // Bump the group epoch.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, 0)),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, 0L))
        );
        assertUnorderedRecordsEquals(expectedRecords, leaveResult.records());

        context.assertNoSessionTimeout(groupId, memberId1);
        context.assertNoSyncTimeout(groupId, memberId1);
        context.assertNoSessionTimeout(groupId, memberId2);
        context.assertNoJoinTimeout(groupId, memberId2);
    }

    @Test
    public void testConsumerGroupMemberUsingClassicProtocolBatchLeaveGroupUpdatingSubscriptionMetadata() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocol = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(new TopicPartition(fooTopicName, 0))
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocol)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0)), 11))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(barTopicId, 0)), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
        long barTopicHash = computeTopicHash(barTopicName, metadataImage);

        // Consumer group with two members.
        // Member 1 uses the classic protocol and member 2 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withAssignment(memberId1, mkAssignment(mkTopicAssignment(fooTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(mkTopicAssignment(barTopicId, 0)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash
                ))))
            .build();
        context.groupMetadataManager.consumerGroup(groupId).setMetadataRefreshDeadline(Long.MAX_VALUE, 10);

        // Member 1 leaves the group.
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId(memberId1)
                ))
        );

        assertEquals(
            new LeaveGroupResponseData()
                .setMembers(List.of(
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(null)
                        .setMemberId(memberId1)
                )),
            leaveResult.response()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            // Remove member 1
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
            // Bump the group epoch.
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, fooTopicHash
            )))
        );
        assertEquals(expectedRecords, leaveResult.records());
    }

    @Test
    public void testClassicGroupLeaveToConsumerGroupWithoutValidLeaveGroupMember() {
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();

        // Consumer group.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .build()))
            .build();

        // Send leave request without valid member.
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId("unknown-member-id")
                ))
        );

        assertEquals(
            new LeaveGroupResponseData()
                .setMembers(List.of(
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(null)
                        .setMemberId("unknown-member-id")
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                )),
            leaveResult.response()
        );

        assertEquals(List.of(), leaveResult.records());
    }

    @Test
    public void testLastConsumerProtocolMemberLeavingConsumerGroupByAdminApi() {
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();
        String memberId4 = Uuid.randomUuid().toString();
        String instanceId2 = "instance-id-2";
        String instanceId4 = "instance-id-4";

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocol1 = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(new TopicPartition(fooTopicName, 0))
                ))))
        );
        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocol2 = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                    List.of(fooTopicName, barTopicName),
                    null,
                    List.of(new TopicPartition(fooTopicName, 1))
                ))))
        );

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocol1)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0)), 11))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setInstanceId(instanceId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocol2)
            )
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 1)), 10))
            .build();
        ConsumerGroupMember member3 = new ConsumerGroupMember.Builder(memberId3)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(barTopicId, 0)), 10))
            .build();
        ConsumerGroupMember member4 = new ConsumerGroupMember.Builder(memberId4)
            .setInstanceId(instanceId4)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(barTopicId, 1)), 10))
            .build();

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addTopic(barTopicId, barTopicName, 2)
            .addRacks()
            .buildCoordinatorMetadataImage();

        // Consumer group with four members.
        // Dynamic member 1 uses the classic protocol.
        // Static member 2 uses the classic protocol.
        // Dynamic member 3 uses the consumer protocol.
        // Static member 4 uses the consumer protocol.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(member1)
                .withMember(member2)
                .withMember(member3)
                .withMember(member4)
                .withAssignment(memberId1, mkAssignment(mkTopicAssignment(fooTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(mkTopicAssignment(fooTopicId, 1)))
                .withAssignment(memberId3, mkAssignment(mkTopicAssignment(barTopicId, 0)))
                .withAssignment(memberId4, mkAssignment(mkTopicAssignment(barTopicId, 1)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                ))))
            .build();

        ConsumerGroup consumerGroup = context.groupMetadataManager.consumerGroup(groupId);

        // Member 2, member 3 and member 4 leave the group, triggering the downgrade.
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> leaveResult = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId("group-id")
                .setMembers(List.of(
                    // Static classic member 2.
                    new MemberIdentity()
                        .setMemberId(memberId2)
                        .setGroupInstanceId(null),
                    // Dynamic consumer member 3.
                    new MemberIdentity()
                        .setMemberId(memberId3)
                        .setGroupInstanceId(null),
                    // Static consumer member 4, by group instance id.
                    new MemberIdentity()
                        .setMemberId(UNKNOWN_MEMBER_ID)
                        .setGroupInstanceId(instanceId4)
                ))
        );

        assertEquals(
            new LeaveGroupResponseData()
                .setMembers(List.of(
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(null)
                        .setMemberId(memberId2),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(null)
                        .setMemberId(memberId3),
                    new LeaveGroupResponseData.MemberResponse()
                        .setGroupInstanceId(instanceId4)
                        .setMemberId(UNKNOWN_MEMBER_ID)
                )),
            leaveResult.response()
        );


        byte[] assignment = Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(
            new TopicPartition(fooTopicName, 0)
        ))));
        Map<String, byte[]> assignments = Map.of(memberId1, assignment);

        ClassicGroup expectedClassicGroup = new ClassicGroup(
            new LogContext(),
            groupId,
            STABLE,
            context.time,
            10,
            Optional.of(ConsumerProtocol.PROTOCOL_TYPE),
            Optional.of("range"),
            Optional.of(memberId1),
            Optional.of(context.time.milliseconds())
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                memberId1,
                Optional.ofNullable(member1.instanceId()),
                member1.clientId(),
                member1.clientHost(),
                member1.rebalanceTimeoutMs(),
                member1.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                member1.supportedJoinGroupRequestProtocols(),
                assignment
            )
        );

        assertUnorderedRecordsEquals(
            List.of(
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId3),
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId4)
                ),
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId3),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId4)
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataTombstoneRecord(groupId)),
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId3),
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId4)
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId)),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId)),
                List.of(GroupCoordinatorRecordHelpers.newGroupMetadataRecord(expectedClassicGroup, assignments))
            ),
            leaveResult.records()
        );

        // The new classic member 1 has a heartbeat timeout.
        ScheduledTimeout<CoordinatorRecord> heartbeatTimeout = context.timer.timeout(
            classicGroupHeartbeatKey(groupId, memberId1)
        );
        assertNotNull(heartbeatTimeout);
        // The new rebalance has a groupJoin timeout.
        ScheduledTimeout<CoordinatorRecord> groupJoinTimeout = context.timer.timeout(
            classicGroupJoinKey(groupId)
        );
        assertNotNull(groupJoinTimeout);

        // A new rebalance is triggered.
        ClassicGroup classicGroup = context.groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, false);
        assertTrue(classicGroup.isInState(PREPARING_REBALANCE));

        // Simulate a failed write to the log.
        context.rollback();

        // The group is reverted back to the consumer group.
        assertEquals(consumerGroup, context.groupMetadataManager.consumerGroup(groupId));
    }

    @Test
    public void testNoConversionWhenSizeExceedsClassicMaxGroupSize() throws Exception {
        String groupId = "group-id";
        String nonClassicMemberId = "1";

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = List.of(
            new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                .setName("range")
                .setMetadata(new byte[0])
        );

        ConsumerGroupMember member = new ConsumerGroupMember.Builder(nonClassicMemberId).build();
        ConsumerGroupMember classicMember1 = new ConsumerGroupMember.Builder("2")
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata().setSupportedProtocols(protocols))
            .build();
        ConsumerGroupMember classicMember2 = new ConsumerGroupMember.Builder("3")
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata().setSupportedProtocols(protocols))
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, 1)
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DOWNGRADE.toString())
            .withConsumerGroup(
                new ConsumerGroupBuilder(groupId, 10)
                    .withMember(member)
                    .withMember(classicMember1)
                    .withMember(classicMember2)
            )
            .build();

        assertEquals(Group.GroupType.CONSUMER, context.groupMetadataManager.group(groupId).type());

        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(nonClassicMemberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setRebalanceTimeoutMs(5000)
        );

        assertEquals(Group.GroupType.CONSUMER, context.groupMetadataManager.group(groupId).type());
    }

    @Test
    public void testConsumerGroupDynamicConfigs() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .buildCoordinatorMetadataImage())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(List.of()));
        assertEquals(2, result.response().memberEpoch());

        // Verify heartbeat interval
        assertEquals(5000, result.response().heartbeatIntervalMs());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Dynamic update group config
        Properties newGroupConfig = new Properties();
        newGroupConfig.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, 50000);
        newGroupConfig.put(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        context.updateGroupConfig(groupId, newGroupConfig);

        // Session timer is rescheduled on second heartbeat.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch()));
        assertEquals(2, result.response().memberEpoch());

        // Verify heartbeat interval
        assertEquals(10000, result.response().heartbeatIntervalMs());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 50000);

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Session timer is cancelled on leave.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH));
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, result.response().memberEpoch());

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testConsumerGroupEvaluatedConfigs() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addRacks()
                .buildCoordinatorMetadataImage())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(memberId, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )))
        ));

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result =
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(90000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setTopicPartitions(List.of()));
        assertEquals(2, result.response().memberEpoch());

        // Verify default heartbeat interval and session timeout before config update.
        assertEquals(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT,
            result.response().heartbeatIntervalMs());
        context.assertSessionTimeout(groupId, memberId,
            GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT);

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().heartbeatIntervalMs())
        );

        // Dynamic update group config with out-of-range values.
        // Session timeout 70000 exceeds max 60000; heartbeat interval 1 is below min 5000.
        Properties newGroupConfig = new Properties();
        newGroupConfig.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, 70000);
        newGroupConfig.put(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, 1);
        context.updateGroupConfig(groupId, newGroupConfig);

        // Session timer is rescheduled on second heartbeat.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().memberEpoch()));
        assertEquals(2, result.response().memberEpoch());

        // Verify heartbeat interval is evaluated to min.
        assertEquals(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT,
            result.response().heartbeatIntervalMs());

        // Verify session timeout is evaluated to max.
        context.assertSessionTimeout(groupId, memberId,
            GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT);
    }

    @Test
    public void testReplayConsumerGroupMemberMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setClientId("clientid")
            .setClientHost("clienthost")
            .setServerAssignorName("range")
            .setRackId("rackid")
            .setSubscribedTopicNames(List.of("foo"))
            .build();

        // The group and the member are created if they do not exist.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("foo", member));
        assertEquals(member, context.groupMetadataManager.consumerGroup("foo").getOrMaybeCreateMember("member", false));
    }

    @Test
    public void testReplayConsumerGroupMemberMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group still exists but the member is already gone. Replaying the
        // ConsumerGroupMemberMetadata tombstone should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("foo", 10, 0));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord("foo", "m1"));
        assertThrows(UnknownMemberIdException.class, () -> context.groupMetadataManager.consumerGroup("foo").getOrMaybeCreateMember("m1", false));

        // The group may not exist at all. Replaying the ConsumerGroupMemberMetadata tombstone
        // should a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord("bar", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("bar"));
    }

    @Test
    public void testReplayConsumerGroupMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("foo", 10, 0));
        assertEquals(10, context.groupMetadataManager.consumerGroup("foo").groupEpoch());
    }

    @Test
    public void testReplayConsumerGroupMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the ConsumerGroupMetadata tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupPartitionMetadata() {
        String groupId = "foo";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        ConsumerGroupPartitionMetadataValue consumerGroupPartitionMetadataValue = new ConsumerGroupPartitionMetadataValue();
        consumerGroupPartitionMetadataValue.topics().add(new ConsumerGroupPartitionMetadataValue.TopicMetadata()
            .setTopicId(Uuid.randomUuid())
            .setTopicName("bar")
            .setNumPartitions(10));
        context.replay(CoordinatorRecord.record(
            new ConsumerGroupPartitionMetadataKey().setGroupId(groupId),
            new ApiMessageAndVersion(consumerGroupPartitionMetadataValue, (short) 0)
        ));
        assertTrue(context.groupMetadataManager.consumerGroup(groupId).hasSubscriptionMetadataRecord());
    }

    @Test
    public void testReplayConsumerGroupPartitionMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the ConsumerGroupPartitionMetadata tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupPartitionMetadataTombstoneWithExistentGroup() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConsumerGroup(new ConsumerGroupBuilder("foo", 10))
            .build();

        // The group exists. Replaying the ConsumerGroupPartitionMetadata tombstone
        // doesn't set addSubscriptionMetadataTombstoneRecord flag.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord("foo"));
        assertFalse(context.groupMetadataManager.consumerGroup("foo").hasSubscriptionMetadataRecord());
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMember() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        Map<Uuid, Set<Integer>> assignment = mkAssignment(
            mkTopicAssignment(Uuid.randomUuid(), 0, 1, 2)
        );

        // The group is created if it does not exist.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("foo", "m1", assignment));
        assertEquals(assignment, context.groupMetadataManager.consumerGroup("foo").targetAssignment("m1").partitions());
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMemberTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the ConsumerGroupTargetAssignmentMember tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord("foo", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("foo", 10, 12345L));
        assertEquals(10, context.groupMetadataManager.consumerGroup("foo").assignmentEpoch());
        assertEquals(12345L, context.groupMetadataManager.consumerGroup("foo").assignmentTimestamp());
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the ConsumerGroupTargetAssignmentMetadata tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMetadataTombstoneExisting() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // Create the group by replaying a value record.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord("foo", 10, 12345L));
        assertEquals(10, context.groupMetadataManager.consumerGroup("foo").assignmentEpoch());
        assertEquals(12345L, context.groupMetadataManager.consumerGroup("foo").assignmentTimestamp());

        // Replay the tombstone. It should reset both the epoch and the timestamp.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataTombstoneRecord("foo"));
        assertEquals(-1, context.groupMetadataManager.consumerGroup("foo").assignmentEpoch());
        assertEquals(0L, context.groupMetadataManager.consumerGroup("foo").assignmentTimestamp());
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignment() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        Uuid topicId = Uuid.randomUuid();
        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(topicId, 0, 1, 2)), 10))
            .build();

        // The group and the member are created if they do not exist.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord("bar", member));
        assertEquals(member, context.groupMetadataManager.consumerGroup("bar").getOrMaybeCreateMember("member", false));
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignmentTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group still exists but the member is already gone. Replaying the
        // ConsumerGroupCurrentMemberAssignment tombstone should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("foo", 10, 0));
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord("foo", "m1"));
        assertThrows(UnknownMemberIdException.class, () -> context.groupMetadataManager.consumerGroup("foo").getOrMaybeCreateMember("m1", false));

        // The group may not exist at all. Replaying the ConsumerGroupCurrentMemberAssignment tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord("bar", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("bar"));
    }

    @Test
    public void testReplayConsumerGroupRegularExpression() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        ResolvedRegularExpression resolvedRegularExpression = new ResolvedRegularExpression(
            Set.of("abc", "abcd"),
            10L,
            12345L
        );

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
            "foo",
            "abc*",
            resolvedRegularExpression
        ));

        assertEquals(
            Optional.of(resolvedRegularExpression),
            context.groupMetadataManager.consumerGroup("foo").resolvedRegularExpression("abc*")
        );

        assertEquals(
            Set.of("foo"),
            context.groupMetadataManager.groupsSubscribedToTopic("abc")
        );
    }

    @Test
    public void testReplayConsumerGroupRegularExpressionTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the ConsumerGroupRegularExpression tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone("foo", "abc*"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.consumerGroup("foo"));

        // Otherwise, it should remove the regular expression.
        ResolvedRegularExpression resolvedRegularExpression = new ResolvedRegularExpression(
            Set.of("abc", "abcd"),
            10L,
            12345L
        );

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
            "foo",
            "abc*",
            resolvedRegularExpression
        ));

        assertEquals(
            Set.of("foo"),
            context.groupMetadataManager.groupsSubscribedToTopic("abc")
        );

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(
            "foo",
            "abc*"
        ));

        assertEquals(
            Optional.empty(),
            context.groupMetadataManager.consumerGroup("foo").resolvedRegularExpression("abc*")
        );

        assertEquals(
            Set.of(),
            context.groupMetadataManager.groupsSubscribedToTopic("abc")
        );
    }

    @Test
    public void testConsumerGroupMemberPicksUpExistingResolvedRegularExpression() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        ConsumerGroupPartitionAssignor assignor = mock(ConsumerGroupPartitionAssignor.class);
        when(assignor.name()).thenReturn("range");
        when(assignor.assign(any(), any())).thenAnswer(answer -> {
            GroupSpec spec = answer.getArgument(0);

            List.of(memberId1, memberId2).forEach(memberId ->
                assertEquals(
                    Set.of(fooTopicId),
                    spec.memberSubscription(memberId).subscribedTopicIds(),
                    String.format("Member %s has unexpected subscribed topic ids", memberId)
                )
            );

            return new GroupAssignment(Map.of(
                memberId1, new MemberAssignmentImpl(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0)
                )),
                memberId2, new MemberAssignmentImpl(mkAssignment(
                    mkTopicAssignment(fooTopicId, 1)
                ))
            ));
        });

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 2)
                .buildCoordinatorMetadataImage())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)), 10))
                    .build())
                .withResolvedRegularExpression("foo*", new ResolvedRegularExpression(
                    Set.of(fooTopicName),
                    100L,
                    12345L))
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1)))
                .withAssignmentEpoch(10))
            .build();

        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(10000)
                .setSubscribedTopicRegex("foo*")
                .setTopicPartitions(List.of()));

        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );
    }

    @Test
    public void testConsumerGroupMemberJoinsWithNewRegex() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage(12345L);
        long groupMetadataHash = computeGroupHash(Map.of(
            fooTopicName, computeTopicHash(fooTopicName, metadataImage)
        ));

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withMetadataHash(groupMetadataHash))
            .build();

        // Member 2 joins the consumer group with a new regular expression.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*")
                .setServerAssignor("range")
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*")
            .setServerAssignorName("range")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Execute pending tasks.
        List<MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord>> tasks = context.processTasks();
        assertEquals(
            List.of(
                new MockCoordinatorExecutor.ExecutorResult<>(
                    groupId + "-regex",
                    new CoordinatorResult<>(List.of(
                        // The resolution of the new regex is persisted.
                        GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                            groupId,
                            "foo*",
                            new ResolvedRegularExpression(
                                Set.of("foo"),
                                12345L,
                                context.time.milliseconds()
                            )
                        ),
                        // The group epoch is bumped.
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, groupMetadataHash)
                    ))
                )
            ),
            tasks
        );
    }

    @Test
    public void testConsumerGroupMemberJoinsWithUpdatedRegex() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage(12345L);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        // Member 1 updates its new regular expression.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result1 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*|bar*")
                .setServerAssignor("range")
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of())
                ),
            result1.response()
        );

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*|bar*")
            .setServerAssignorName("range")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The member subscription is updated.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            // The previous regular expression is deleted.
            GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*"),
            // The member assignment is updated.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1)
        );

        assertRecordsEquals(expectedRecords, result1.records());

        // Execute pending tasks.
        List<MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord>> tasks = context.processTasks();
        assertEquals(1, tasks.size());

        MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord> task = tasks.get(0);
        assertEquals(groupId + "-regex", task.key());
        assertRecordsEquals(
            List.of(
                // The resolution of the new regex is persisted.
                GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                    groupId,
                    "foo*|bar*",
                    new ResolvedRegularExpression(
                        Set.of("foo", "bar"),
                        12345L,
                        context.time.milliseconds()
                    )
                ),
                // The group epoch is bumped.
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                )))
            ),
            task.result().records()
        );

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            ))
        )));

        // Member heartbeats again with the same regex.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result2 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*|bar*")
                .setServerAssignor("range")
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0, 1, 2))))),
            result2.response()
        );

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*|bar*")
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)), 11))
            .build();

        expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
        );

        assertRecordsEquals(expectedRecords, result2.records());
    }

    @Test
    public void testConsumerGroupMemberJoinsWithRegexAndUpdatesItBeforeResolutionCompleted() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage(12345L);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Member 1 joins.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*")
                .setServerAssignor("range")
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*")
            .setServerAssignorName("range")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The member subscription is created.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),

            // The group is initialized at group epoch 1. Since the group epoch is not bumped until
            // regex resolution has completed, no consumer group metadata record is created.
            // Similarly, the target assignment is initialized at epoch 1 with an empty assignment
            // and not updated until regex resolution has completed, so no target assignment records
            // are created.

            // The member current state is created.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // The task is scheduled.
        assertTrue(context.executor.isScheduled(groupId + "-regex"));

        // The member updates its regex before the resolution of the previous one completes.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setSubscribedTopicRegex("foo*|bar*"));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(1)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*|bar*")
            .setServerAssignorName("range")
            .build();

        expectedRecords = List.of(
            // The member subscription is updated.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            // The previous regex is deleted.
            GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*"),
            // The previous member epoch is updated.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1)
        );

        assertRecordsEquals(expectedRecords, result.records());

        // The task is still scheduled.
        assertTrue(context.executor.isScheduled(groupId + "-regex"));
        assertEquals(1, context.executor.size());

        // Execute the pending tasks.
        List<MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord>> tasks = context.processTasks();
        assertEquals(1, tasks.size());

        // The pending task was a no-op.
        MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord> task = tasks.get(0);
        assertEquals(groupId + "-regex", task.key());
        assertRecordsEquals(List.of(), task.result().records());

        // The member heartbeats again. It triggers a new resolution.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setSubscribedTopicRegex("foo*|bar*"));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertTrue(context.executor.isScheduled(groupId + "-regex"));
        assertEquals(1, context.executor.size());

        // Execute pending tasks.
        tasks = context.processTasks();
        assertEquals(1, tasks.size());

        task = tasks.get(0);
        assertEquals(groupId + "-regex", task.key());
        assertRecordsEquals(
            List.of(
                // The resolution of the new regex is persisted.
                GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                    groupId,
                    "foo*|bar*",
                    new ResolvedRegularExpression(
                        Set.of("foo", "bar"),
                        12345L,
                        context.time.milliseconds()
                    )
                ),
                // The group epoch is bumped.
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 2, computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                    barTopicName, computeTopicHash(barTopicName, metadataImage)
                )))
            ),
            task.result().records()
        );
    }

    @Test
    public void testConsumerGroupMemberJoinRefreshesExpiredRegexes() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid foooTopicId = Uuid.randomUuid();
        String foooTopicName = "fooo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));

        MetadataImage image = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .build(1L);
        long fooTopicHash = computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(image));
        long barTopicHash = computeTopicHash(barTopicName, new KRaftCoordinatorMetadataImage(image));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(new KRaftCoordinatorMetadataImage(image))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("bar*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(barTopicId, 0, 1, 2)), 10))
                    .build())
                .withResolvedRegularExpression("foo*", new ResolvedRegularExpression(
                    Set.of(fooTopicName), 0L, 0L))
                .withResolvedRegularExpression("bar*", new ResolvedRegularExpression(
                    Set.of(barTopicName), 0L, 0L))
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(barTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash
                ))))
            .build();

        // Update metadata image.
        MetadataImage newImage = new MetadataImageBuilder(image)
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addTopic(foooTopicId, foooTopicName, 1)
            .build(2L);

        context.groupMetadataManager.onMetadataUpdate(
            new KRaftCoordinatorMetadataDelta(new MetadataDelta.Builder()
                .setImage(newImage)
                .build()), new KRaftCoordinatorMetadataImage(newImage)
        );

        // A member heartbeats.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(10));

        // The task is NOT scheduled.
        assertFalse(context.executor.isScheduled(groupId + "-regex"));

        // Advance past the batching interval.
        context.sleep(11000L);

        // A member heartbeats.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(10));

        // The task is scheduled.
        assertTrue(context.executor.isScheduled(groupId + "-regex"));

        // Execute the pending tasks.
        List<MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord>> tasks = context.processTasks();
        assertEquals(1, tasks.size());

        // Execute pending tasks.
        MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord> task = tasks.get(0);
        assertEquals(groupId + "-regex", task.key());

        assertUnorderedRecordsEquals(
            List.of(
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                        groupId,
                        "foo*",
                        new ResolvedRegularExpression(
                            Set.of(fooTopicName, foooTopicName),
                            2L,
                            context.time.milliseconds()
                        )
                    ),
                    GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                        groupId,
                        "bar*",
                        new ResolvedRegularExpression(
                            Set.of(barTopicName),
                            2L,
                            context.time.milliseconds()
                        )
                    )
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash,
                    foooTopicName, computeTopicHash(foooTopicName, new KRaftCoordinatorMetadataImage(newImage))
                ))))
            ),
            task.result().records()
        );
    }

    @Test
    public void testConsumerGroupMemberClearsRegex() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage(12345L);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        // Member 1 updates its new regular expression.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("")
                .setServerAssignor("range")
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of())
                ),
            result.response()
        );

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("")
            .setServerAssignorName("range")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            // previous expression is deleted
            GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*"),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, 0),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, Map.of()),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testConsumerMemberWithRegexReplacedByClassicMemberWithSameSubscription() {
        String groupId = "fooup";
        String instanceId = "instance-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 1)
            .buildCoordinatorMetadataImage(12345L);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));

        // Member 1 is a static member with both regex and topic name subscription
        // Member 2 uses topic name subscription.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setInstanceId(instanceId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("bar*")
                    .setSubscribedTopicNames(List.of(fooTopicName))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of(fooTopicName))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withResolvedRegularExpression("bar*", new ResolvedRegularExpression(
                    Set.of(barTopicName), 0L, 0L)))
            .build();
        ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
        group.setMetadataRefreshDeadline(Long.MAX_VALUE, 10);

        // Member 1 is replaced by a classic member with the same instance id.
        JoinGroupRequestProtocolCollection joinProtocols = new JoinGroupRequestProtocolCollection();
        joinProtocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of(fooTopicName)
            ))))
        );
        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId(instanceId)
            .withRebalanceTimeoutMs(5000)
            .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .withProtocols(joinProtocols)
            .build();
        GroupMetadataManagerTestContext.JoinResult result = context.sendClassicGroupJoin(joinRequest);

        ConsumerGroupMember newMember1 = group.staticMember(instanceId);

        ConsumerGroupMember expectedCopiedMember = new ConsumerGroupMember.Builder(newMember1.memberId())
            .setState(MemberState.STABLE)
            .setInstanceId(instanceId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("bar*") // Still uses regex subscription.
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2),
                mkTopicAssignment(barTopicId, 0)), 10))
            .build();

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(newMember1.memberId())
            .setState(MemberState.STABLE)
            .setInstanceId(instanceId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("") // empty regex subscription
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setServerAssignorName("range")
            .setAssignedPartitions(Map.of()) // empty assignment
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(500)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(joinRequest.protocols()))
            )
            .build();

        List<List<CoordinatorRecord>> expectedRecords = List.of(
            // The previous member is deleted.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1)),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1)),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1)),
            // The previous member is replaced by the new one.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedCopiedMember)),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, expectedCopiedMember.memberId(), toAssignmentWithoutEpochs(expectedCopiedMember.assignedPartitions()))),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedCopiedMember)),
            // The member subscription is updated.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1)),
            // The regex is tombstoned.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "bar*")),
            // The group epoch is bumped.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage)
            )))),
            // The target assignment is updated.
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, expectedMember1.memberId(), Map.of()),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, Map.of())
            ),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds())),
            // The member assignment is updated.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1))
        );

        assertUnorderedRecordsEquals(
            expectedRecords,
            result.records
        );
    }

    @Test
    public void testConsumerMemberWithRegexReplacedByClassicMemberWithChangedSubscription() {
        String groupId = "fooup";
        String instanceId = "instance-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage(12345L);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));

        // Member 1 is a static member with regex subscription and
        // Member 2 uses topic name subscription.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setInstanceId(instanceId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of(fooTopicName))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withResolvedRegularExpression("foo*", new ResolvedRegularExpression(
                    Set.of(fooTopicName), 0L, 0L)))
            .build();
        ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
        group.setMetadataRefreshDeadline(Long.MAX_VALUE, 10);

        // Member 1 is replaced by a classic member with the same instance id.
        JoinGroupRequestProtocolCollection joinProtocols = new JoinGroupRequestProtocolCollection();
        joinProtocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                List.of()
            ))))
        );
        JoinGroupRequestData joinRequest = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId(groupId)
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withGroupInstanceId(instanceId)
            .withRebalanceTimeoutMs(5000)
            .withProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
            .withProtocols(joinProtocols)
            .build();
        GroupMetadataManagerTestContext.JoinResult result = context.sendClassicGroupJoin(joinRequest);

        ConsumerGroupMember newMember1 = group.staticMember(instanceId);

        ConsumerGroupMember expectedCopiedMember = new ConsumerGroupMember.Builder(newMember1.memberId())
            .setState(MemberState.STABLE)
            .setInstanceId(instanceId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*") // Still uses regex subscription.
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
            .build();

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(newMember1.memberId())
            .setState(MemberState.STABLE)
            .setInstanceId(instanceId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("") // empty regex subscription
            .setServerAssignorName("range")
            .setAssignedPartitions(Map.of()) // empty assignment
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(500)
                    .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(joinRequest.protocols()))
            )
            .build();

        List<List<CoordinatorRecord>> expectedRecords = List.of(
            // The previous member is deleted.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1)),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1)),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1)),
            // The previous member is replaced by the new one.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedCopiedMember)),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, expectedCopiedMember.memberId(), toAssignmentWithoutEpochs(expectedCopiedMember.assignedPartitions()))),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedCopiedMember)),
            // The member subscription is updated.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1)),
            // The regex is tombstoned.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*")),
            // The group epoch is bumped.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage)
            )))),
            // The target assignment is updated.
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, expectedMember1.memberId(), Map.of()),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, Map.of())
            ),
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds())),
            // The member assignment is updated.
            List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1))
        );

        assertUnorderedRecordsEquals(
            expectedRecords,
            result.records
        );
    }

    @Test
    public void testConsumerGroupMemberJoinsWithRegexWithTopicAuthorizationFailure() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        String barTopicName = "bar";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage(12345L);
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
        long barTopicHash = computeTopicHash(barTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withAuthorizerPlugin(authorizerPlugin)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withResolvedRegularExpression("foo*", new ResolvedRegularExpression(
                    Set.of(fooTopicName), 0L, 0L))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(fooTopicName, fooTopicHash))))
            .build();

        // sleep for more than REGEX_BATCH_REFRESH_MIN_INTERVAL_MS
        context.time.sleep(10001L);

        Map<String, AuthorizationResult> acls = new HashMap<>();
        acls.put(fooTopicName, AuthorizationResult.ALLOWED);
        acls.put(barTopicName, AuthorizationResult.DENIED);
        when(authorizer.authorize(any(), any())).thenAnswer(invocation -> {
            List<Action> actions = invocation.getArgument(1);
            return actions.stream()
                .map(action -> acls.getOrDefault(action.resourcePattern().name(), AuthorizationResult.DENIED))
                .collect(Collectors.toList());
        });

        // Member 2 heartbeats with a different regular expression.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result1 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*|bar*")
                .setServerAssignor("range")
                .setTopicPartitions(List.of()),
            ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion()
        );

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of())),
            result1.response()
        );

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*|bar*")
            .setServerAssignorName("range")
            .build();

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),
                GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*"),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
            ),
            result1.records()
        );

        // Execute pending tasks.
        assertEquals(
            List.of(
                new MockCoordinatorExecutor.ExecutorResult<>(
                    groupId + "-regex",
                    new CoordinatorResult<>(List.of(
                        // The resolution of the new regex is persisted.
                        GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                            groupId,
                            "foo*|bar*",
                            new ResolvedRegularExpression(
                                Set.of("foo"),
                                12345L,
                                context.time.milliseconds()
                            )
                        ),
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                            fooTopicName, fooTopicHash
                        )))
                    ))
                )
            ),
            context.processTasks()
        );

        // sleep for more than REGEX_BATCH_REFRESH_MIN_INTERVAL_MS
        context.time.sleep(10001L);

        // Access to the bar topic is granted.
        acls.put(barTopicName, AuthorizationResult.ALLOWED);
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)
            ))
        )));

        // Member 2 heartbeats again with a new regex.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result2 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo|bar*")
                .setServerAssignor("range")
                .setTopicPartitions(List.of()),
            ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion()
        );

        expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo|bar*")
            .setServerAssignorName("range")
            .setAssignedPartitions(Map.of())
            .build();

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of())),
            result2.response()
        );

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),
                GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*|bar*"),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
            ),
            result2.records()
        );

        // A regex refresh is triggered and the bar topic is included.
        assertRecordsEquals(
            List.of(
                // The resolution of the new regex is persisted.
                GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                    groupId,
                    "foo|bar*",
                    new ResolvedRegularExpression(
                        Set.of("foo", "bar"),
                        12345L,
                        context.time.milliseconds()
                    )
                ),
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 12, computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash
                )))
            ),
            context.processTasks().get(0).result().records()
        );
    }

    @Test
    public void testConsumerGroupMemberJoinsRefreshTopicAuthorization() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        String barTopicName = "bar";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage(12345L);
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
        long barTopicHash = computeTopicHash(barTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        Authorizer authorizer = mock(Authorizer.class);
        Plugin<Authorizer> authorizerPlugin = Plugin.wrapInstance(authorizer, null, "authorizer.class.name");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_REGEX_REFRESH_INTERVAL_MS_CONFIG, 60000)
            .withMetadataImage(metadataImage)
            .withAuthorizerPlugin(authorizerPlugin)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withResolvedRegularExpression("foo*", new ResolvedRegularExpression(
                    Set.of(fooTopicName), 0L, 0L))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(fooTopicName, fooTopicHash))))
            .build();

        // sleep for more than REGEX_BATCH_REFRESH_MIN_INTERVAL_MS
        context.time.sleep(10001L);

        Map<String, AuthorizationResult> acls = new HashMap<>();
        acls.put(fooTopicName, AuthorizationResult.ALLOWED);
        acls.put(barTopicName, AuthorizationResult.DENIED);
        when(authorizer.authorize(any(), any())).thenAnswer(invocation -> {
            List<Action> actions = invocation.getArgument(1);
            return actions.stream()
                .map(action -> acls.getOrDefault(action.resourcePattern().name(), AuthorizationResult.DENIED))
                .collect(Collectors.toList());
        });

        // Member 2 heartbeats with a different regular expression.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result1 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*|bar*")
                .setServerAssignor("range")
                .setTopicPartitions(List.of()),
            ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion()
        );

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of())),
            result1.response()
        );

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*|bar*")
            .setServerAssignorName("range")
            .build();

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),
                GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*"),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
            ),
            result1.records()
        );

        // Execute pending tasks.
        assertEquals(
            List.of(
                new MockCoordinatorExecutor.ExecutorResult<>(
                    groupId + "-regex",
                    new CoordinatorResult<>(List.of(
                        // The resolution of the new regex is persisted.
                        GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                            groupId,
                            "foo*|bar*",
                            new ResolvedRegularExpression(
                                Set.of("foo"),
                                12345L,
                                context.time.milliseconds()
                            )
                        ),
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                            fooTopicName, fooTopicHash
                        )))
                    ))
                )
            ),
            context.processTasks()
        );

        // sleep for more than REGEX_REFRESH_INTERVAL_MS
        context.time.sleep(60001L);

        // Access to the bar topic is granted.
        acls.put(barTopicName, AuthorizationResult.ALLOWED);
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)
            ))
        )));

        // Member 2 heartbeats again with the same regex.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result2 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*|bar*")
                .setServerAssignor("range")
                .setTopicPartitions(List.of()),
            ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion()
        );

        expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*|bar*")
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)), 11))
            .build();

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(3, 4, 5))))),
            result2.response()
        );

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
            ),
            result2.records()
        );

        // A regex refresh is triggered and the bar topic is included.
        assertRecordsEquals(
            List.of(
                // The resolution of the new regex is persisted.
                GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                    groupId,
                    "foo*|bar*",
                    new ResolvedRegularExpression(
                        Set.of("foo", "bar"),
                        12345L,
                        context.time.milliseconds()
                    )
                ),
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 12, computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash
                )))
            ),
            context.processTasks().get(0).result().records()
        );
    }

    @Test
    public void testStaticConsumerGroupMemberJoinsWithUpdatedRegex() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String instanceId = "instance-id";

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage(12345L);

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setInstanceId(instanceId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*|bar*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 0, 1, 2)), 10))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Static member temporarily leaves the group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result1 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setInstanceId(instanceId)
                .setMemberId(memberId1)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
        );

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH),
            result1.response()
        );

        // Static member joins the group with an updated regular expression.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result2 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setInstanceId(instanceId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*")
                .setServerAssignor("range")
                .setTopicPartitions(List.of()));

        // The returned assignment does not contain topics not in the current regular expression.
        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of())
                ),
            result2.response()
        );

        ConsumerGroupMember expectedCopiedMember = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setInstanceId(instanceId)
            .setMemberEpoch(0)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*|bar*")
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)), 0))
            .build();

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setInstanceId(instanceId)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*")
            .setServerAssignorName("range")
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            // The previous member is deleted.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
            // The previous member is replaced by the new one.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedCopiedMember),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedCopiedMember),
            // The member subscription is updated.
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
            // The previous regular expression is deleted.
            GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*|bar*"),
            // The member assignment is updated.
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1)
        );

        assertRecordsEquals(expectedRecords, result2.records());

        // Execute pending tasks.
        List<MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord>> tasks = context.processTasks();
        assertEquals(1, tasks.size());

        MockCoordinatorExecutor.ExecutorResult<CoordinatorRecord> task = tasks.get(0);
        assertEquals(groupId + "-regex", task.key());
        assertRecordsEquals(
            List.of(
                // The resolution of the new regex is persisted.
                GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
                    groupId,
                    "foo*",
                    new ResolvedRegularExpression(
                        Set.of("foo"),
                        12345L,
                        context.time.milliseconds()
                    )
                ),
                // The group epoch is bumped.
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage)
                )))
            ),
            task.result().records()
        );

        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            ))
        )));

        // Member heartbeats again with the same regex.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result3 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setInstanceId(instanceId)
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicRegex("foo*")
                .setServerAssignor("range")
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5))))),
            result3.response()
        );

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setInstanceId(instanceId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicRegex("foo*|bar*")
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 11))
            .build();

        expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 11, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
        );

        assertRecordsEquals(expectedRecords, result3.records());
    }

    @Test
    public void testResolvedRegularExpressionsRemovedWhenMembersLeaveOrFenced() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage(1L);
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
        long barTopicHash = computeTopicHash(barTopicName, metadataImage);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("bar*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(barTopicId, 0, 1, 2)), 10))
                    .build())
                .withResolvedRegularExpression("foo*", new ResolvedRegularExpression(
                    Set.of(fooTopicName), 0L, 0L))
                .withResolvedRegularExpression("bar*", new ResolvedRegularExpression(
                    Set.of(barTopicName), 0L, 0L))
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(barTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash
                ))))
            .build();

        // Setup the timers.
        context.onLoaded();

        // Member 1 leaves the group.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(-1));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(-1),
            result.response()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*"),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                barTopicName, barTopicHash
            )))
        );

        assertRecordsEquals(expectedRecords, result.records());

        // Member 2 is fenced due to reaching the session timeout.
        context.assertSessionTimeout(groupId, memberId2, 45000);
        List<ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(45000 + 1);

        // Verify the expired timeout.
        assertEquals(
            List.of(new ExpiredTimeout<>(
                groupSessionTimeoutKey(groupId, memberId2),
                new CoordinatorResult<>(
                    List.of(
                        GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
                        GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
                        GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "bar*"),
                        GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 12, 0),
                        GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 12, 0L)
                    )
                )
            )),
            timeouts
        );
    }

    @Test
    public void testResolvedRegularExpressionsRemovedWhenConsumerMembersRemovedByAdminApi() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();
        String memberId4 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .buildCoordinatorMetadataImage(1L);
        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);
        long barTopicHash = computeTopicHash(barTopicName, metadataImage);

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setInstanceId(memberId1)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setInstanceId(memberId2)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("foo*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId3)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setInstanceId(memberId3)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("bar*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(barTopicId, 0, 1)), 10))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId4)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setInstanceId(memberId4)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicRegex("bar*")
                    .setServerAssignorName("range")
                    .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                        mkTopicAssignment(barTopicId, 2)), 10))
                    .build())
                .withResolvedRegularExpression("foo*", new ResolvedRegularExpression(
                    Set.of(fooTopicName), 0L, 0L))
                .withResolvedRegularExpression("bar*", new ResolvedRegularExpression(
                    Set.of(barTopicName), 0L, 0L))
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(barTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, fooTopicHash,
                    barTopicName, barTopicHash
                ))))
            .build();

        // Remove members.
        CoordinatorResult<LeaveGroupResponseData, CoordinatorRecord> result = context.sendClassicGroupLeave(
            new LeaveGroupRequestData()
                .setGroupId(groupId)
                .setMembers(List.of(
                    new MemberIdentity()
                        .setMemberId(memberId1)
                        .setGroupInstanceId(null),
                    new MemberIdentity()
                        .setMemberId(memberId2)
                        .setGroupInstanceId(memberId2),
                    new MemberIdentity()
                        .setMemberId(UNKNOWN_MEMBER_ID)
                        .setGroupInstanceId(memberId3)
                ))
        );

        assertEquals(
            new LeaveGroupResponseData()
                .setMembers(List.of(
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId(memberId1)
                        .setGroupInstanceId(null),
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId(memberId2)
                        .setGroupInstanceId(memberId2),
                    new LeaveGroupResponseData.MemberResponse()
                        .setMemberId(UNKNOWN_MEMBER_ID)
                        .setGroupInstanceId(memberId3)
                )),
            result.response()
        );

        assertUnorderedRecordsEquals(
            List.of(
                List.of(
                    // Remove member 1.
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
                    // Remove member 2.
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
                    // Remove member 3.
                    GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId3),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId3),
                    GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId3)
                ),
                // Remove regex.
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, "foo*")),
                // Bumped epoch.
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                    barTopicName, barTopicHash
                ))))
            ),
            result.records()
        );
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignmentWithCompaction() {
        String groupId = "fooup";
        String memberIdA = "memberIdA";
        String memberIdB = "memberIdB";
        Uuid topicId = Uuid.randomUuid();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();

        // This test enacts the following scenario:
        // 1. Member A is assigned partition 0.
        // 2. Member A is unassigned partition 0 [record removed by compaction].
        // 3. Member B is assigned partition 0. 
        // 4. Member A is assigned partition 1. 
        // If record 2 is processed, there are no issues, however with compaction it is possible that 
        // unassignment records are removed. We would like to not fail in these cases.
        // Therefore we will allow assignments to owned partitions as long as the epoch is larger. 

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberIdA)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(topicId, 0)), 11))
            .build()));

        // Partition 0's owner is replaced by member B at epoch 12.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberIdB)
            .setState(MemberState.STABLE)
            .setMemberEpoch(12)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(topicId, 0)), 12))
            .build()));

        // Partition 0 must remain with member B at epoch 12 even though member A has just been unassigned partition 0.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberIdA)
            .setState(MemberState.STABLE)
            .setMemberEpoch(13)
            .setPreviousMemberEpoch(12)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(topicId, 1)), 13))
            .build()));

        // Verify partition epochs.
        ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
        assertEquals(12, group.currentPartitionEpoch(topicId, 0));
        assertEquals(13, group.currentPartitionEpoch(topicId, 1));
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignmentUnownedTopicWithCompaction() {
        String groupId = "fooup";
        String memberIdA = "memberIdA";
        String memberIdB = "memberIdB";
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();

        // This test enacts the following scenario:
        // 1. Member A is assigned partition foo-0.
        // 2. Member A is unassigned partition foo-0 [record removed by compaction].
        // 3. Member B is assigned partition foo-0.
        // 4. Member B is unassigned partition foo-0. 
        // 5. Member A is assigned partition bar-0. 
        // This is a legitimate set of assignments but with compaction the unassignment record can be skipped.
        // This can lead to conflicts from updating an owned partition in step 3 and attempting 
        // to remove nonexistent ownership in step 5. We want to ensure removing ownership from a 
        // completely unowned partition in step 5 is allowed.  

        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberIdA)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0)), 11))
            .build()));

        // foo-0's owner is replaced by member B at epoch 12.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberIdB)
            .setState(MemberState.STABLE)
            .setMemberEpoch(12)
            .setPreviousMemberEpoch(11)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(fooTopicId, 0)), 11))
            .build()));

        // foo becomes unowned.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberIdB)
            .setState(MemberState.STABLE)
            .setMemberEpoch(13)
            .setPreviousMemberEpoch(12)
            .build()));

        // Member A is unassigned foo-0.
        context.replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberIdA)
            .setState(MemberState.STABLE)
            .setMemberEpoch(14)
            .setPreviousMemberEpoch(13)
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(mkTopicAssignment(barTopicId, 0)), 14))
            .build()));

        // Verify foo-0 is unowned and bar-0 is owned by member A at epoch 14.
        ConsumerGroup group = context.groupMetadataManager.consumerGroup(groupId);
        assertEquals(-1, group.currentPartitionEpoch(fooTopicId, 0));
        assertEquals(14, group.currentPartitionEpoch(barTopicId, 0));
    }

    @Test
    public void testCanComputeNextTargetAssignmentWithNoPreviousAssignmentTimestamp() {
        // The next target assignment can always be computed when there is no previous assignment timestamp.
        assertTrue(GroupMetadataManager.canComputeNextTargetAssignment(0, 1000, 0));
    }

    @Test
    public void testCanComputeNextTargetAssignmentWithZeroAssignmentIntervalMs() {
        // The next target assignment can always be computed when the assignment interval is zero.
        assertTrue(GroupMetadataManager.canComputeNextTargetAssignment(1000000, 0, 0));
        assertTrue(GroupMetadataManager.canComputeNextTargetAssignment(1000000, 0, 999999));
        assertTrue(GroupMetadataManager.canComputeNextTargetAssignment(1000000, 0, 1000000));
    }

    @Test
    public void testCanComputeNextTargetAssignment() {
        // The next target assignment cannot be computed before the timestamp of the end of the previous assignment computation.
        assertFalse(GroupMetadataManager.canComputeNextTargetAssignment(1000000, 5000, 0));
        assertFalse(GroupMetadataManager.canComputeNextTargetAssignment(1000000, 5000, 999999));

        // The next target assignment cannot be computed before the assignment interval has elapsed.
        assertFalse(GroupMetadataManager.canComputeNextTargetAssignment(1000000, 5000, 1000000));
        assertFalse(GroupMetadataManager.canComputeNextTargetAssignment(1000000, 5000, 1004999));

        // The next target assignment can be computed after the assignment interval has elapsed.
        assertTrue(GroupMetadataManager.canComputeNextTargetAssignment(1000000, 5000, 1005000));
        assertTrue(GroupMetadataManager.canComputeNextTargetAssignment(1000000, 5000, 1007500));
    }

    @Test
    public void testConsumerGroupAssignmentInterval() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, 5000)
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .withMetadataImage(metadataImage)
            .build();

        // Member 1 joins the group and gets an assignment immediately.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            ))
        )));
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result1 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of(fooTopicName))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                    ))),
            result1.response()
        );

        ConsumerGroupMember expectedMember1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)), 2))
            .build();

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember1),
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 2, computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage)
                ))),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
                )),
                GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember1)
            ),
            result1.records()
        );

        // Wait until just before the expected delay.
        context.time.sleep(4995);

        // Member 2 joins the group and gets no assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 3, 4, 5)
            ))
        )));
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result2 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setServerAssignor("range")
                .setSubscribedTopicNames(List.of(fooTopicName))
                .setTopicPartitions(List.of()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of())),
            result2.response()
        );

        ConsumerGroupMember expectedMember2 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(), 2))
            .build();

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId, expectedMember2),
                GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId, 3, computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage)
                ))),
                GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember2)
            ),
            result2.records()
        );

        // Wait a little more. The next target assignment can be computed now.
        context.time.sleep(10);

        // The next target assignment is computed.
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result3 = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(2));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(3)
                .setHeartbeatIntervalMs(5000),
            result3.response()
        );

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(memberId2)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setMemberEpoch(3)
            .setPreviousMemberEpoch(2)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setServerAssignorName("range")
            .setAssignedPartitions(toAssignmentWithEpochs(mkAssignment(), 2))
            .build();

        assertUnorderedRecordsEquals(
            List.of(
                List.of(
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)
                    )),
                    GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5)
                    ))
                ),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(groupId, 3, context.time.milliseconds())),
                List.of(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId, expectedMember3))
            ),
            result3.records()
        );
    }

}
