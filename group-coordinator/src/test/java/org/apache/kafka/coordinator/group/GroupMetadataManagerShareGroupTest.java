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
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer.ExpiredTimeout;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.SubscriptionCount;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.InitMapValue;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupBuilder;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupMember;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.share.persister.DeleteShareGroupStateParameters;
import org.apache.kafka.server.share.persister.InitializeShareGroupStateParameters;
import org.apache.kafka.server.share.persister.PartitionIdData;
import org.apache.kafka.server.share.persister.PartitionStateData;
import org.apache.kafka.server.share.persister.TopicData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordsEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertResponseEquals;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord;
import static org.apache.kafka.coordinator.group.GroupMetadataManager.groupSessionTimeoutKey;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ADDRESS;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ID;
import static org.apache.kafka.coordinator.group.Utils.computeGroupHash;
import static org.apache.kafka.coordinator.group.Utils.computeTopicHash;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.SHARE_GROUP_REBALANCES_SENSOR_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link GroupMetadataManager} share-group (KIP-932) behaviour:
 * share-group heartbeat, share-state initialize/delete, share-partition offset deletion,
 * describe, and share-group record replay ({@code testReplayShareGroup*}).
 */
public class GroupMetadataManagerShareGroupTest {

    @Test
    public void testShareGroupMemberCanRejoinWithEpochZero() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 3)
            .addRacks()
            .buildCoordinatorMetadataImage();

        long fooTopicHash = computeTopicHash(fooTopicName, metadataImage);

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(metadataImage)
            .build();

        // Set up a Share group member with epoch 100.
        ShareGroupMember member = new ShareGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 0, 1, 2)))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionRecord(groupId, member));
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 100, computeGroupHash(Map.of(
            fooTopicName, fooTopicHash
        ))));
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 0, 1, 2)
        )));
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord(groupId, 100, 12345L));
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord(groupId, member));

        // Member rejoins with epoch=0 - should succeed.
        // Since the subscription/metadata hasn't changed, group epoch stays at 100.
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of(fooTopicName)));

        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(100)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ShareGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2))))),
            result.response().getKey()
        );
    }

    @Test
    public void testSessionTimeoutExpirationForShareMember() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
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
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result =
            context.shareGroupHeartbeat(
                new ShareGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setSubscribedTopicNames(List.of("foo")));
        assertEquals(2, result.response().getKey().memberEpoch());

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
                        GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionTombstoneRecord(groupId, memberId),
                        GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 3, 0),
                        GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord(groupId, 3, 0L)
                    )
                )
            )),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
    }

    @Test
    public void testOnLoadedSessionTimeoutExpirationForShareMember() {
        String groupId = "group";
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        String memberId = "foo-1";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new KRaftCoordinatorMetadataImage(metadataImage))
            .withShareGroup(new ShareGroupBuilder(groupId, 10)
                .withMember(new ShareGroupMember.Builder(memberId)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(9)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, new KRaftCoordinatorMetadataImage(metadataImage))
                ))))
            .build();

        // Let's assume that all the records have been replayed and now
        // onLoaded is called to signal it.
        context.groupMetadataManager.onLoaded();

        // All members should have a session timeout in place.
        assertNotNull(context.timer.timeout(groupSessionTimeoutKey(groupId, memberId)));

        // Advance time past the session timeout.
        List<ExpiredTimeout<CoordinatorRecord>> timeouts = context.sleep(45000 + 1);

        // Verify the expired timeout.
        assertEquals(
            List.of(
                new ExpiredTimeout<>(
                    groupSessionTimeoutKey(groupId, memberId),
                    new CoordinatorResult<>(
                        List.of(
                            GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentTombstoneRecord(groupId, memberId),
                            GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentTombstoneRecord(groupId, memberId),
                            GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionTombstoneRecord(groupId, memberId),
                            GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 11, 0),
                            GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord(groupId, 11, 0L)
                        )
                    )
                )
            ),
            timeouts
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
    }

    @Test
    public void testOnLoadedWithShareGroup() {
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new KRaftCoordinatorMetadataImage(new MetadataImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build()))
            .withShareGroup(new ShareGroupBuilder("foo", 10)
                .withMember(new ShareGroupMember.Builder("foo-1")
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(9)
                    .setPreviousMemberEpoch(9)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withMember(new ShareGroupMember.Builder("foo-2")
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(10)
                    .setClientId(DEFAULT_CLIENT_ID)
                    .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                    .setSubscribedTopicNames(List.of("foo"))
                    .build())
                .withAssignment("foo-1", mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        // Let's assume that all the records have been replayed and now
        // onLoaded is called to signal it.
        context.groupMetadataManager.onLoaded();

        // All members should have a session timeout in place.
        assertNotNull(context.timer.timeout(groupSessionTimeoutKey("foo", "foo-1")));
        assertNotNull(context.timer.timeout(groupSessionTimeoutKey("foo", "foo-2")));
    }

    @Test
    public void testShareGroupDescribeRequest() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder().build();

        // GroupId is not required
        List<ShareGroupDescribeResponseData.DescribedGroup> groups = context.sendShareGroupDescribe(List.of());
        assertEquals(0, groups.size());

        // Group id not found
        groups = context.sendShareGroupDescribe(List.of("unknown-group"));
        assertEquals(1, groups.size());
        assertEquals(Errors.GROUP_ID_NOT_FOUND.code(), groups.get(0).errorCode());
    }

    @Test
    public void testShareGroupDescribeNoErrors() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("share-range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of()
        ));

        List<String> groupIds = List.of("group-id-1", "group-id-2");
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupIds.get(0), 100, 0));
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupIds.get(1), 15, 0));

        Uuid topicId = Uuid.randomUuid();
        String topicName = "foo";
        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId, topicName, 1)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(image.emptyDelta(), image);

        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupIds.get(1))
                .setMemberId(Uuid.randomUuid().toString())
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of(topicName)));

        // Verify that a member id was generated for the new member.
        String memberId = result.response().getKey().memberId();
        assertNotNull(memberId);
        context.commit();

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(
                topicId,
                Set.of(0)
            ),
            groupIds.get(1),
            16,
            true
        );

        List<ShareGroupDescribeResponseData.DescribedGroup> expected = List.of(
            new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(100)
                .setAssignmentEpoch(1)
                .setGroupId(groupIds.get(0))
                .setGroupState(ShareGroup.ShareGroupState.EMPTY.toString())
                .setAssignorName("share-range"),
            new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupEpoch(16)
                .setAssignmentEpoch(16)
                .setGroupId(groupIds.get(1))
                .setMembers(List.of(
                    new ShareGroupMember.Builder(memberId)
                        .setMemberEpoch(16)
                        .setClientId("client")
                        .setClientHost("localhost/127.0.0.1")
                        .setSubscribedTopicNames(List.of(topicName))
                        .build()
                        .asShareGroupDescribeMember(
                            new MetadataImageBuilder().buildCoordinatorMetadataImage()
                        )
                ))
                .setGroupState(ShareGroup.ShareGroupState.STABLE.toString())
                .setAssignorName("share-range")
        );
        List<ShareGroupDescribeResponseData.DescribedGroup> actual = context.sendShareGroupDescribe(groupIds);

        assertEquals(expected, actual);
    }

    @Test
    public void testShareGroupMemberIdGeneration() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(CoordinatorMetadataImage.EMPTY)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of()
        ));

        String memberId = Uuid.randomUuid().toString();
        Uuid topicId1 = Uuid.randomUuid();
        String topicName1 = "foo";
        Uuid topicId2 = Uuid.randomUuid();
        String topicName2 = "bar";
        String groupId = "group-foo";

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId1, topicName1, 1)
            .addTopic(topicId2, topicName2, 1)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(image.emptyDelta(), image);

        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of(topicName1, topicName2)));

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(
                topicId1,
                Set.of(0),
                topicId2,
                Set.of(0)
            ),
            groupId,
            2,
            true
        );

        assertEquals(
            memberId,
            result.response().getKey().memberId(),
            "MemberId should remain unchanged, as the server does not generate a new one since the consumer generates its own."
        );

        // The response should get a bumped epoch and should not
        // contain any assignment because we did not provide
        // topics metadata.
        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ShareGroupHeartbeatResponseData.Assignment()),
            result.response().getKey()
        );
    }

    @Test
    public void testShareGroupUnknownGroupId() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        assertThrows(GroupIdNotFoundException.class, () ->
            context.shareGroupHeartbeat(
                new ShareGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(100) // Epoch must be > 0.
                    .setSubscribedTopicNames(List.of("foo", "bar"))));
    }

    @Test
    public void testShareGroupUnknownMemberIdJoins() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(new NoOpPartitionAssignor())
            .build();

        Uuid topicId1 = Uuid.randomUuid();
        String topicName1 = "foo";
        Uuid topicId2 = Uuid.randomUuid();
        String topicName2 = "bar";

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId1, topicName1, 1)
            .addTopic(topicId2, topicName2, 1)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(image.emptyDelta(), image);

        // A first member joins to create the group.
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of(topicName1, topicName2)));

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(
                topicId1,
                Set.of(0),
                topicId2,
                Set.of(0)
            ),
            groupId,
            2,
            true
        );

        // The second member is rejected because the member id is unknown and
        // the member epoch is not zero.
        assertThrows(UnknownMemberIdException.class, () ->
            context.shareGroupHeartbeat(
                new ShareGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(Uuid.randomUuid().toString())
                    .setMemberEpoch(1)
                    .setSubscribedTopicNames(List.of("foo", "bar"))));
    }

    @Test
    public void testShareGroupMemberJoinsEmptyGroupWithAssignments() {
        String groupId = "fooup";
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
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

        assertThrows(GroupIdNotFoundException.class, () ->
            context.groupMetadataManager.shareGroup(groupId));

        MetadataImage image = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .build();
        
        CoordinatorMetadataImage coordinatorMetadataImage = new KRaftCoordinatorMetadataImage(image);

        MetadataDelta delta = new MetadataDelta.Builder()
            .setImage(image)
            .build();

        context.groupMetadataManager.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(delta), coordinatorMetadataImage);

        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of("foo", "bar")));

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(
                fooTopicId,
                Set.of(0, 1, 2, 3, 4, 5),
                barTopicId,
                Set.of(0, 1, 2)
            ),
            groupId,
            2,
            true
        );

        assertResponseEquals(
            new ShareGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ShareGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5)),
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(List.of(0, 1, 2))
                    ))),
            result.response().getKey()
        );

        ShareGroupMember expectedMember = new ShareGroupMember.Builder(memberId)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            ))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 2, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, coordinatorMetadataImage),
                barTopicName, computeTopicHash(barTopicName, coordinatorMetadataImage)
            ))),
            GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),
            GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord(groupId, expectedMember),
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(groupId, mkShareGroupStateMap(List.of(
                    mkShareGroupStateMetadataEntry(fooTopicId, fooTopicName, List.of(0, 1, 2, 3, 4, 5)),
                    mkShareGroupStateMetadataEntry(barTopicId, barTopicName, List.of(0, 1, 2))
                )),
                Map.of(),
                Map.of()
            )
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    private Map<Uuid, InitMapValue> mkShareGroupStateMap(List<Map.Entry<Uuid, Map.Entry<String, Set<Integer>>>> entries) {
        Map<Uuid, InitMapValue> map = new HashMap<>();
        for (Map.Entry<Uuid, Map.Entry<String, Set<Integer>>> entry : entries) {
            map.put(entry.getKey(), new InitMapValue(entry.getValue().getKey(), entry.getValue().getValue(), 1));
        }
        return map;
    }

    private Map.Entry<Uuid, Map.Entry<String, Set<Integer>>> mkShareGroupStateMetadataEntry(Uuid topicId, String topicName, List<Integer> partitions) {
        return Map.entry(
            topicId,
            Map.entry(topicName, new LinkedHashSet<>(partitions))
        );
    }

    @Test
    public void testShareGroupLeavingMemberBumpsGroupEpoch() {
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

        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .addTopic(barTopicId, barTopicName, 3)
            .addTopic(zarTopicId, zarTopicName, 1)
            .addRacks()
            .buildCoordinatorMetadataImage();

        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(metadataImage)
            .withShareGroup(new ShareGroupBuilder(groupId, 10)
                .withMember(new ShareGroupMember.Builder(memberId1)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ShareGroupMember.Builder(memberId2)
                    .setState(MemberState.STABLE)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    // Use zar only here to ensure that metadata needs to be recomputed.
                    .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
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
                    barTopicName, computeTopicHash(barTopicName, metadataImage),
                    zarTopicName, computeTopicHash(zarTopicName, metadataImage)
                ))))
            .build();

        // Member 2 leaves the consumer group.
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName)));

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(),
            "",
            -1,
            false
        );

        assertResponseEquals(
            new ShareGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH),
            result.response().getKey()
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentTombstoneRecord(groupId, memberId2),
            GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionTombstoneRecord(groupId, memberId2),
            // Subscription metadata is recomputed because zar is no longer there.
            GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 11, computeGroupHash(Map.of(
                fooTopicName, computeTopicHash(fooTopicName, metadataImage),
                barTopicName, computeTopicHash(barTopicName, metadataImage)
            )))
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testShareGroupNewMemberIsRejectedWithMaximumMembersIsReached() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        // A share group cannot have pre-defined members and member metadata as members and assignments
        // are not persisted.
        MockPartitionAssignor assignor = new MockPartitionAssignor("share");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withConfig(GroupCoordinatorConfig.SHARE_GROUP_MAX_SIZE_CONFIG, 1)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of()
        ));

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 100, 0));

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 1)
            .addTopic(barTopicId, barTopicName, 1)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(image.emptyDelta(), image);

        // Member 1 joins the group.
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of(fooTopicName, barTopicName)));
        assertEquals(101, result.response().getKey().memberEpoch());

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(
                fooTopicId,
                Set.of(0),
                barTopicId,
                Set.of(0)
            ),
            groupId,
            101,
            true
        );

        // Member 2 joins the group.
        assertThrows(GroupMaxSizeReachedException.class, () -> context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of("foo", "bar"))));
    }

    @Test
    public void testShareGroupDeleteTombstones() {
        String groupId = "share-group-id";
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroup(new ShareGroupBuilder(groupId, 10))
            .build();

        List<CoordinatorRecord> expectedRecords = List.of(
            GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newShareGroupEpochTombstoneRecord(groupId)
        );
        List<CoordinatorRecord> records = new ArrayList<>();
        context.groupMetadataManager.createGroupTombstoneRecordsAndCancelTimers("share-group-id", records);
        assertEquals(expectedRecords, records);
    }

    @Test
    public void testShareGroupStates() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("share-range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withShareGroup(new ShareGroupBuilder(groupId, 10))
            .build();

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 10, 0));

        assertEquals(ShareGroup.ShareGroupState.EMPTY, context.shareGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionRecord(groupId, new ShareGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setSubscribedTopicNames(List.of(fooTopicName))
            .build()));
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 11, 0));

        assertEquals(ShareGroup.ShareGroupState.STABLE, context.shareGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3))));
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord(groupId, 11, 12345L));

        assertEquals(ShareGroup.ShareGroupState.STABLE, context.shareGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord(groupId, new ShareGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2)))
            .build()));

        assertEquals(ShareGroup.ShareGroupState.STABLE, context.shareGroupState(groupId));

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord(groupId, new ShareGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .build()));

        assertEquals(ShareGroup.ShareGroupState.STABLE, context.shareGroupState(groupId));
    }

    @Test
    public void testShareGroupDynamicConfigs() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("simple");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
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

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(image.emptyDelta(), image);

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result =
            context.shareGroupHeartbeat(
                new ShareGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setSubscribedTopicNames(List.of("foo")));
        assertEquals(2, result.response().getKey().memberEpoch());

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(
                fooTopicId,
                Set.of(0, 1, 2, 3, 4, 5)
            ),
            groupId,
            2,
            true
        );

        // Verify heartbeat interval
        assertEquals(5000, result.response().getKey().heartbeatIntervalMs());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 45000);

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().getKey().heartbeatIntervalMs())
        );

        // Dynamic update group config
        Properties newGroupConfig = new Properties();
        newGroupConfig.put(SHARE_SESSION_TIMEOUT_MS_CONFIG, 50000);
        newGroupConfig.put(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        context.updateGroupConfig(groupId, newGroupConfig);

        context.groupMetadataManager.replay(
            new ShareGroupStatePartitionMetadataKey()
                .setGroupId(groupId),
            new ShareGroupStatePartitionMetadataValue()
                .setInitializedTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(fooTopicId)
                        .setTopicName(fooTopicName)
                        .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                ))
                .setDeletingTopics(List.of())
        );

        // Session timer is rescheduled on second heartbeat.
        result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().getKey().memberEpoch()));
        assertEquals(2, result.response().getKey().memberEpoch());

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(),
            "",
            0,
            false
        );

        // Verify heartbeat interval
        assertEquals(10000, result.response().getKey().heartbeatIntervalMs());

        // Verify that there is a session time.
        context.assertSessionTimeout(groupId, memberId, 50000);

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().getKey().heartbeatIntervalMs())
        );

        // Session timer is cancelled on leave.
        result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH));
        assertEquals(LEAVE_GROUP_MEMBER_EPOCH, result.response().getKey().memberEpoch());

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(),
            "",
            0,
            false
        );

        // Verify that there are no timers.
        context.assertNoSessionTimeout(groupId, memberId);
        context.assertNoRebalanceTimeout(groupId, memberId);
    }

    @Test
    public void testShareGroupEvaluatedConfigs() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("simple");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
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

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 6)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(image.emptyDelta(), image);

        // Session timer is scheduled on first heartbeat.
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result =
            context.shareGroupHeartbeat(
                new ShareGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(0)
                    .setSubscribedTopicNames(List.of("foo")));
        assertEquals(2, result.response().getKey().memberEpoch());

        // Verify default heartbeat interval and session timeout before config update.
        assertEquals(GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT,
            result.response().getKey().heartbeatIntervalMs());
        context.assertSessionTimeout(groupId, memberId,
            GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_DEFAULT);

        // Advance time.
        assertEquals(
            List.of(),
            context.sleep(result.response().getKey().heartbeatIntervalMs())
        );

        // Dynamic update group config with out-of-range values.
        // Session timeout 70000 exceeds max 60000; heartbeat interval 1 is below min 5000.
        Properties newGroupConfig = new Properties();
        newGroupConfig.put(SHARE_SESSION_TIMEOUT_MS_CONFIG, 70000);
        newGroupConfig.put(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, 1);
        context.updateGroupConfig(groupId, newGroupConfig);

        // Replay ShareGroupStatePartitionMetadata required before second heartbeat.
        context.groupMetadataManager.replay(
            new ShareGroupStatePartitionMetadataKey()
                .setGroupId(groupId),
            new ShareGroupStatePartitionMetadataValue()
                .setInitializedTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(fooTopicId)
                        .setTopicName(fooTopicName)
                        .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                ))
                .setDeletingTopics(List.of())
        );

        // Session timer is rescheduled on second heartbeat.
        result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(result.response().getKey().memberEpoch()));
        assertEquals(2, result.response().getKey().memberEpoch());

        // Verify heartbeat interval is evaluated to min.
        assertEquals(GroupCoordinatorConfig.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT,
            result.response().getKey().heartbeatIntervalMs());

        // Verify session timeout is evaluated to max.
        context.assertSessionTimeout(groupId, memberId,
            GroupCoordinatorConfig.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT);
    }

    @Test
    public void testReplayShareGroupTargetAssignmentMetadata() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group is created if it does not exist.
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord("foo", 10, 12345L));
        assertEquals(10, context.groupMetadataManager.shareGroup("foo").assignmentEpoch());
        assertEquals(12345L, context.groupMetadataManager.shareGroup("foo").assignmentTimestamp());
    }

    @Test
    public void testReplayShareGroupTargetAssignmentMetadataTombstone() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // The group may not exist at all. Replaying the ShareGroupTargetAssignmentMetadata tombstone
        // should be a no-op.
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> context.groupMetadataManager.shareGroup("foo"));
    }

    @Test
    public void testReplayShareGroupTargetAssignmentMetadataTombstoneExisting() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .build();

        // Create the group by replaying a value record.
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord("foo", 10, 12345L));
        assertEquals(10, context.groupMetadataManager.shareGroup("foo").assignmentEpoch());
        assertEquals(12345L, context.groupMetadataManager.shareGroup("foo").assignmentTimestamp());

        // Replay the tombstone. It should reset both the epoch and the timestamp.
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataTombstoneRecord("foo"));
        assertEquals(-1, context.groupMetadataManager.shareGroup("foo").assignmentEpoch());
        assertEquals(0L, context.groupMetadataManager.shareGroup("foo").assignmentTimestamp());
    }

    @Test
    public void testShareGroupDeleteRequestNoDeletingTopics() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        Uuid t1Uuid = Uuid.randomUuid();
        Uuid t2Uuid = Uuid.randomUuid();
        String t1Name = "t1";
        String t2Name = "t2";

        String groupId = "share-group";
        ShareGroup shareGroup = mock(ShareGroup.class);
        when(shareGroup.groupId()).thenReturn(groupId);
        when(shareGroup.isEmpty()).thenReturn(false);

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(t1Uuid, t1Name, 2)
            .addTopic(t2Uuid, t2Name, 2)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(image.emptyDelta(), image);

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 0, 0));

        context.replay(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(t1Uuid, new InitMapValue(t1Name, Set.of(0, 1), 1)),
                Map.of(t2Uuid, new InitMapValue(t2Name, Set.of(0, 1), 1)),
                Map.of()
            )
        );

        context.commit();

        Map<Uuid, Set<Integer>> expectedTopicPartitionMap = Map.of(
            t1Uuid, Set.of(0, 1),
            t2Uuid, Set.of(0, 1)
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(),
                Map.of(t1Uuid, t1Name, t2Uuid, t2Name)
            )
        );

        List<CoordinatorRecord> records = new ArrayList<>();
        Optional<DeleteShareGroupStateParameters> params = context.groupMetadataManager.shareGroupBuildPartitionDeleteRequest(groupId, records);
        verifyShareGroupDeleteRequest(
            params,
            expectedTopicPartitionMap,
            groupId,
            true
        );
        assertRecordsEquals(expectedRecords, records);
    }

    @Test
    public void testShareGroupDeleteRequestWithAlreadyDeletingTopics() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        Uuid t1Uuid = Uuid.randomUuid();
        Uuid t2Uuid = Uuid.randomUuid();
        Uuid t3Uuid = Uuid.randomUuid();
        String t1Name = "t1";
        String t2Name = "t2";
        String t3Name = "t3";

        String groupId = "share-group";
        ShareGroup shareGroup = mock(ShareGroup.class);
        when(shareGroup.groupId()).thenReturn(groupId);
        when(shareGroup.isEmpty()).thenReturn(false);

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(t1Uuid, t1Name, 2)
            .addTopic(t2Uuid, t2Name, 2)
            .addTopic(t3Uuid, t3Name, 2)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(image.emptyDelta(), image);

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 0, 0));

        context.replay(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(t1Uuid, new InitMapValue(t1Name, Set.of(0, 1), 1)),
                Map.of(t2Uuid, new InitMapValue(t2Name, Set.of(0, 1), 1)),
                Map.of(t3Uuid, t3Name)
            )
        );

        context.commit();

        Map<Uuid, Set<Integer>> expectedTopicPartitionMap = Map.of(
            t1Uuid, Set.of(0, 1),
            t2Uuid, Set.of(0, 1),
            t3Uuid, Set.of(0, 1)
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(),
                Map.of(t1Uuid, t1Name, t2Uuid, t2Name, t3Uuid, t3Name)  // Existing deleting topics should be included here.
            )
        );

        List<CoordinatorRecord> records = new ArrayList<>();
        Optional<DeleteShareGroupStateParameters> params = context.groupMetadataManager.shareGroupBuildPartitionDeleteRequest(groupId, records);
        verifyShareGroupDeleteRequest(
            params,
            expectedTopicPartitionMap,
            groupId,
            true
        );
        assertRecordsEquals(expectedRecords, records);
    }

    @Test
    public void testShareGroupDeleteRequestWithAlreadyDeletingTopicsButNotInMetadata() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        Uuid t1Uuid = Uuid.randomUuid();
        Uuid t2Uuid = Uuid.randomUuid();
        Uuid t3Uuid = Uuid.randomUuid();
        String t1Name = "t1";
        String t2Name = "t2";
        String t3Name = "t3";

        String groupId = "share-group";
        ShareGroup shareGroup = mock(ShareGroup.class);
        when(shareGroup.groupId()).thenReturn(groupId);
        when(shareGroup.isEmpty()).thenReturn(false);

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(t1Uuid, t1Name, 2)
            .addTopic(t2Uuid, t2Name, 2)
//            .addTopic(t3Uuid, t3Name, 2)  // Simulate deleting topic not present in metadata image.
            .buildCoordinatorMetadataImage();

        CoordinatorMetadataDelta delta = image.emptyDelta();
        context.groupMetadataManager.onMetadataUpdate(delta, image);

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 0, 0));

        context.replay(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(t1Uuid, new InitMapValue(t1Name, Set.of(0, 1), 1)),
                Map.of(t2Uuid, new InitMapValue(t2Name, Set.of(0, 1), 1)),
                Map.of(t3Uuid, t3Name)
            )
        );

        context.commit();

        Map<Uuid, Set<Integer>> expectedTopicPartitionMap = Map.of(
            t1Uuid, Set.of(0, 1),
            t2Uuid, Set.of(0, 1)
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(),
                Map.of(t1Uuid, t1Name, t2Uuid, t2Name)  // Existing deleting topics should be ignored.
            )
        );

        List<CoordinatorRecord> records = new ArrayList<>();
        Optional<DeleteShareGroupStateParameters> params = context.groupMetadataManager.shareGroupBuildPartitionDeleteRequest(groupId, records);
        verifyShareGroupDeleteRequest(
            params,
            expectedTopicPartitionMap,
            groupId,
            true
        );
        assertRecordsEquals(expectedRecords, records);
    }

    @Test
    public void testShareGroupDeleteRequestWithAlreadyDeletingTopicsButMetadataIsEmpty() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(assignor))
            .build();

        Uuid t1Uuid = Uuid.randomUuid();
        Uuid t2Uuid = Uuid.randomUuid();
        Uuid t3Uuid = Uuid.randomUuid();
        String t1Name = "t1";
        String t2Name = "t2";
        String t3Name = "t3";

        String groupId = "share-group";
        ShareGroup shareGroup = mock(ShareGroup.class);
        when(shareGroup.groupId()).thenReturn(groupId);
        when(shareGroup.isEmpty()).thenReturn(false);

        CoordinatorMetadataImage image = CoordinatorMetadataImage.EMPTY;
        CoordinatorMetadataDelta delta = image.emptyDelta();
        context.groupMetadataManager.onMetadataUpdate(delta, image);

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 0, 0));

        context.replay(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(t1Uuid, new InitMapValue(t1Name, Set.of(0, 1), 1)),
                Map.of(t2Uuid, new InitMapValue(t2Name, Set.of(0, 1), 1)),
                Map.of(t3Uuid, t3Name)
            )
        );

        context.commit();

        Map<Uuid, Set<Integer>> expectedTopicPartitionMap = Map.of(
            t1Uuid, Set.of(0, 1),
            t2Uuid, Set.of(0, 1)
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(),
                Map.of(t1Uuid, t1Name, t2Uuid, t2Name)  // Existing deleting topics should be ignored.
            )
        );

        List<CoordinatorRecord> records = new ArrayList<>();
        Optional<DeleteShareGroupStateParameters> params = context.groupMetadataManager.shareGroupBuildPartitionDeleteRequest(groupId, records);
        verifyShareGroupDeleteRequest(
            params,
            expectedTopicPartitionMap,
            groupId,
            true
        );
        assertRecordsEquals(expectedRecords, records);
    }

    @Test
    public void testSharePartitionsEligibleForOffsetDeletionSuccess() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        String groupId = "share-group";
        String topicName1 = "topic-1";
        String topicName2 = "topic-2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId1, topicName1, 3)
            .addTopic(topicId2, topicName2, 2)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(mock(CoordinatorMetadataDelta.class), image);

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 0, 0));

        context.replay(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(
                    topicId1, new InitMapValue(topicName1, Set.of(0, 1, 2), 1),
                    topicId2, new InitMapValue(topicName2, Set.of(0, 1), 1)
                ),
                Map.of()
            )
        );

        context.commit();

        List<DeleteShareGroupStateRequestData.DeleteStateData> expectedResult = List.of(
            new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(topicId1)
                .setPartitions(List.of(
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(0),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(1),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(2)
                )),
            new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(topicId2)
                .setPartitions(List.of(
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(0),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(1)
                ))
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(),
                Map.of(topicId1, topicName1, topicId2, topicName2)
            )
        );

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName1),
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName2)
            ));
        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList = new ArrayList<>();

        List<CoordinatorRecord> records = new ArrayList<>();

        List<DeleteShareGroupStateRequestData.DeleteStateData> result =
            context.groupMetadataManager.sharePartitionsEligibleForOffsetDeletion(groupId, requestData, errorTopicResponseList, records);

        assertTrue(errorTopicResponseList.isEmpty());
        assertEquals(expectedResult, result);
        assertRecordsEquals(expectedRecords, records);
    }

    @Test
    public void testSharePartitionsEligibleForOffsetDeletionContainsDeletingTopics() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        String groupId = "share-group";
        String topicName1 = "topic-1";
        String topicName2 = "topic-2";
        String topicName3 = "topic-3";
        String topicName4 = "topic-4";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        Uuid topicId3 = Uuid.randomUuid();
        Uuid topicId4 = Uuid.randomUuid();

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId1, topicName1, 3)
            .addTopic(topicId2, topicName2, 2)
            .addTopic(topicId3, topicName3, 2)
            .addTopic(topicId4, topicName4, 2)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(mock(CoordinatorMetadataDelta.class), image);

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 0, 0));

        context.replay(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(
                    topicId1, new InitMapValue(topicName1, Set.of(0, 1, 2), 1),
                    topicId2, new InitMapValue(topicName2, Set.of(0, 1), 1)
                ),
                Map.of(
                    topicId3, topicName3,
                    topicId4, topicName4
                )
            )
        );

        context.commit();

        // Because "topic-4" not a part of the request data, it will not be added to the result, even though it is part
        // of the deletingTopics set. "topic-3" isn't currently initialized for the group, but since it is part of the
        // deletingTopics set, it will still be included in the result and tried to get deleted by the persister.
        List<DeleteShareGroupStateRequestData.DeleteStateData> expectedResult = List.of(
            new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(topicId1)
                .setPartitions(List.of(
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(0),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(1),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(2)
                )),
            new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(topicId2)
                .setPartitions(List.of(
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(0),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(1)
                )),
            new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(topicId3)
                .setPartitions(List.of(
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(0),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(1)
                ))
        );

        // The ShareGroupStatePartitionMetadata record will contain all 4 topics in the deletingTopics list
        List<CoordinatorRecord> expectedRecords = List.of(
            newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(),
                Map.of(
                    topicId1, topicName1,
                    topicId2, topicName2,
                    topicId3, topicName3,
                    topicId4, topicName4
                )
            )
        );

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName1),
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName2),
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName3)
            ));
        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList = new ArrayList<>();

        List<CoordinatorRecord> records = new ArrayList<>();

        List<DeleteShareGroupStateRequestData.DeleteStateData> result =
            context.groupMetadataManager.sharePartitionsEligibleForOffsetDeletion(groupId, requestData, errorTopicResponseList, records);

        assertTrue(errorTopicResponseList.isEmpty());
        assertEquals(expectedResult, result);
        assertRecordsEquals(expectedRecords, records);
    }

    @Test
    public void testSharePartitionsEligibleForOffsetDeletionErrorTopics() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        String groupId = "share-group";
        String topicName1 = "topic-1";
        String topicName2 = "topic-2";
        Uuid topicId1 = Uuid.randomUuid();

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId1, topicName1, 3)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(mock(CoordinatorMetadataDelta.class), image);

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 0, 0));

        context.replay(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(topicId1, new InitMapValue(topicName1, Set.of(0, 1, 2), 1)),
                Map.of()
            )
        );

        context.commit();

        List<DeleteShareGroupStateRequestData.DeleteStateData> expectedResult = List.of(
            new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(topicId1)
                .setPartitions(List.of(
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(0),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(1),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(2)
                ))
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(),
                Map.of(topicId1, topicName1)
            )
        );

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName1),
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName2)
            ));
        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList = new ArrayList<>();

        List<CoordinatorRecord> records = new ArrayList<>();

        List<DeleteShareGroupStateRequestData.DeleteStateData> result =
            context.groupMetadataManager.sharePartitionsEligibleForOffsetDeletion(groupId, requestData, errorTopicResponseList, records);

        assertEquals(
            List.of(
                new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(topicName2)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message())
            ),
            errorTopicResponseList
        );
        assertEquals(expectedResult, result);
        assertRecordsEquals(expectedRecords, records);
    }

    @Test
    public void testSharePartitionsEligibleForOffsetDeletionUninitializedTopics() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        String groupId = "share-group";
        String topicName1 = "topic-1";
        String topicName2 = "topic-2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId1, topicName1, 3)
            .addTopic(topicId2, topicName2, 2)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(mock(CoordinatorMetadataDelta.class), image);

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 0, 0));

        context.replay(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(topicId2, new InitMapValue(topicName2, Set.of(0, 1), 1)),
                Map.of(topicId1, new InitMapValue(topicName1, Set.of(0, 1, 2), 1)),
                Map.of()
            )
        );

        context.commit();

        List<DeleteShareGroupStateRequestData.DeleteStateData> expectedResult = List.of(
            new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(topicId1)
                .setPartitions(List.of(
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(0),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(1),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(2)
                ))
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(topicId2, new InitMapValue(topicName2, Set.of(0, 1), 1)),
                Map.of(),
                Map.of(topicId1, topicName1)
            )
        );

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName1),
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName2)
            ));
        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList = new ArrayList<>();

        List<CoordinatorRecord> records = new ArrayList<>();

        List<DeleteShareGroupStateRequestData.DeleteStateData> result =
            context.groupMetadataManager.sharePartitionsEligibleForOffsetDeletion(groupId, requestData, errorTopicResponseList, records);

        assertEquals(
            List.of(
                new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(topicName2)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage("There is no offset information to delete.")
            ),
            errorTopicResponseList
        );
        assertEquals(expectedResult, result);
        assertRecordsEquals(expectedRecords, records);
    }

    @Test
    public void testSharePartitionsEligibleForOffsetDeletionUninitializedAndErrorTopics() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        String groupId = "share-group";
        String topicName1 = "topic-1";
        String topicName2 = "topic-2";
        String topicName3 = "topic-3";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId1, topicName1, 3)
            .addTopic(topicId2, topicName2, 2)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(mock(CoordinatorMetadataDelta.class), image);

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 0, 0));

        context.replay(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(topicId2, new InitMapValue(topicName2, Set.of(0, 1), 1)),
                Map.of(topicId1, new InitMapValue(topicName1, Set.of(0, 1, 2), 1)),
                Map.of()
            )
        );

        context.commit();

        List<DeleteShareGroupStateRequestData.DeleteStateData> expectedResult = List.of(
            new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(topicId1)
                .setPartitions(List.of(
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(0),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(1),
                    new DeleteShareGroupStateRequestData.PartitionData()
                        .setPartition(2)
                ))
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(topicId2, new InitMapValue(topicName2, Set.of(0, 1), 1)),
                Map.of(),
                Map.of(topicId1, topicName1)
            )
        );

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName1),
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName2),
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(topicName3)
            ));
        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> errorTopicResponseList = new ArrayList<>();

        List<CoordinatorRecord> records = new ArrayList<>();

        List<DeleteShareGroupStateRequestData.DeleteStateData> result =
            context.groupMetadataManager.sharePartitionsEligibleForOffsetDeletion(groupId, requestData, errorTopicResponseList, records);

        assertEquals(
            List.of(
                new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(topicName2)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage("There is no offset information to delete."),
                new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(topicName3)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message())
            ),
            errorTopicResponseList
        );
        assertEquals(expectedResult, result);
        assertRecordsEquals(expectedRecords, records);
    }

    @Test
    public void testCompleteDeleteShareGroupOffsets() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        String groupId = "share-group";
        String topicName1 = "topic-1";
        String topicName2 = "topic-2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId1, topicName1, 3)
            .addTopic(topicId2, topicName2, 2)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(mock(CoordinatorMetadataDelta.class), image);

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 0, 0));

        context.replay(
            GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(),
                Map.of(
                    topicId1, topicName1,
                    topicId2, topicName2
                )
            )
        );

        context.commit();

        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> expectedResult = List.of(
            new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                .setTopicId(topicId1)
                .setTopicName(topicName1)
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage(null),
            new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                .setTopicId(topicId2)
                .setTopicName(topicName2)
                .setErrorCode(Errors.NONE.code())
                .setErrorMessage(null)
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            newShareGroupStatePartitionMetadataRecord(
                groupId,
                Map.of(),
                Map.of(),
                Map.of()
            )
        );

        Map<Uuid, String> topics = Map.of(
            topicId1, topicName1,
            topicId2, topicName2
        );

        List<CoordinatorRecord> records = new ArrayList<>();

        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> result =
            context.groupMetadataManager.completeDeleteShareGroupOffsets(groupId, topics, records);

        assertEquals(convertResponseTopicListToMap(expectedResult), convertResponseTopicListToMap(result));
        assertRecordsEquals(expectedRecords, records);
    }

    @Test
    public void testCompleteDeleteShareGroupOffsetsEmptyResult() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        String groupId = "share-group";
        String topicName1 = "topic-1";
        String topicName2 = "topic-2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId1, topicName1, 3)
            .addTopic(topicId2, topicName2, 2)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(mock(CoordinatorMetadataDelta.class), image);

        context.replay(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 0, 0));

        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> expectedResult = List.of();

        List<CoordinatorRecord> expectedRecords = List.of();

        Map<Uuid, String> topics = Map.of(
            topicId1, topicName1,
            topicId2, topicName2
        );

        List<CoordinatorRecord> records = new ArrayList<>();

        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> result =
            context.groupMetadataManager.completeDeleteShareGroupOffsets(groupId, topics, records);

        assertEquals(convertResponseTopicListToMap(expectedResult), convertResponseTopicListToMap(result));
        assertRecordsEquals(expectedRecords, records);
    }

    @Test
    public void testShareGroupHeartbeatInitializeOnPartitionUpdate() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        Uuid t1Uuid = Uuid.randomUuid();
        String t1Name = "t1";
        Uuid t2Uuid = Uuid.randomUuid();
        String t2Name = "t2";
        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(t1Uuid, "t1", 2)
            .addTopic(t2Uuid, "t2", 2)
            .buildCoordinatorMetadataImage();

        String groupId = "share-group";

        context.groupMetadataManager.onMetadataUpdate(mock(CoordinatorMetadataDelta.class), image);

        Uuid memberId = Uuid.randomUuid();
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId.toString())
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of(t1Name, t2Name)));

        CoordinatorRecord expected = newShareGroupStatePartitionMetadataRecord(
            groupId,
            mkShareGroupStateMap(List.of(
                mkShareGroupStateMetadataEntry(t1Uuid, t1Name, List.of(0, 1)),
                mkShareGroupStateMetadataEntry(t2Uuid, t2Name, List.of(0, 1))
            )),
            Map.of(),
            Map.of()
        );

        Optional<CoordinatorRecord> actual = result.records().stream().filter(record -> record.key() instanceof ShareGroupStatePartitionMetadataKey)
            .findAny();
        assertTrue(actual.isPresent());
        assertRecordEquals(expected, actual.get());

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(
                t1Uuid,
                Set.of(0, 1),
                t2Uuid,
                Set.of(0, 1)
            ),
            groupId,
            2,
            true
        );

        context.groupMetadataManager.replay(
            new ShareGroupStatePartitionMetadataKey()
                .setGroupId(groupId),
            new ShareGroupStatePartitionMetadataValue()
                .setInitializingTopics(List.of())
                .setInitializedTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t1Uuid)
                        .setTopicName(t1Name)
                        .setPartitions(List.of(0, 1)),
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t2Uuid)
                        .setTopicName(t2Name)
                        .setPartitions(List.of(0, 1))
                ))
                .setDeletingTopics(List.of())
        );

        // Partition increase
        image = new MetadataImageBuilder()
            .addTopic(t1Uuid, "t1", 4)
            .addTopic(t2Uuid, "t2", 2)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(mock(CoordinatorMetadataDelta.class), image);

        assignor.prepareGroupAssignment(new GroupAssignment(
            Map.of(
                memberId.toString(),
                new MemberAssignmentImpl(
                    Map.of(
                        t1Uuid,
                        Set.of(0, 1, 2, 3),
                        t2Uuid,
                        Set.of(0, 1)
                    )
                )
            )
        ));

        result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId.toString())
                .setMemberEpoch(2)
                .setSubscribedTopicNames(null));

        expected = newShareGroupStatePartitionMetadataRecord(
            groupId,
            mkShareGroupStateMap(List.of(
                mkShareGroupStateMetadataEntry(t1Uuid, t1Name, List.of(2, 3))
            )),
            mkShareGroupStateMap(List.of(
                mkShareGroupStateMetadataEntry(t1Uuid, t1Name, List.of(0, 1)),
                mkShareGroupStateMetadataEntry(t2Uuid, t2Name, List.of(0, 1))
            )),
            Map.of()
        );

        actual = result.records().stream().filter(record -> record.key() instanceof ShareGroupStatePartitionMetadataKey)
            .findAny();
        assertTrue(actual.isPresent());
        assertRecordEquals(expected, actual.get());

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(
                t1Uuid,
                Set.of(2, 3)
            ),
            groupId,
            3,
            true
        );

        assertEquals(Map.of(t1Uuid, Set.of(0, 1), t2Uuid, Set.of(0, 1)), context.groupMetadataManager.initializedShareGroupPartitions(groupId));
        verify(context.metrics, times(2)).record(SHARE_GROUP_REBALANCES_SENSOR_NAME);
    }

    @Test
    public void testShareGroupHeartbeatDoesNotBumpGroupEpochDuringAssignmentDelay() {
        Uuid t1Uuid = Uuid.randomUuid();
        String t1Name = "t1";
        CoordinatorMetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(t1Uuid, t1Name, 2)
            .buildCoordinatorMetadataImage();

        String groupId = "share-group";
        String memberId = Uuid.randomUuid().toString();

        ShareGroupMember member = new ShareGroupMember.Builder(memberId)
            .setState(MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(t1Name))
            .setAssignedPartitions(Map.of())
            .build();

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(metadataImage)
            .withShareGroup(new ShareGroupBuilder(groupId, 2)
                .withMember(member)
                .withAssignment(memberId, mkAssignment())
                .withAssignmentEpoch(2)
                // Suppress assignments.
                .withAssignmentTimestamp(Integer.MAX_VALUE)
                .withMetadataHash(computeGroupHash(Map.of(
                    t1Name, computeTopicHash(t1Name, metadataImage)
                ))))
            .build();

        // t1-0 and t1-1 are initialized and not yet assigned.
        context.groupMetadataManager.replay(
            new ShareGroupStatePartitionMetadataKey()
                .setGroupId(groupId),
            new ShareGroupStatePartitionMetadataValue()
                .setInitializingTopics(List.of())
                .setInitializedTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t1Uuid)
                        .setTopicName(t1Name)
                        .setPartitions(List.of(0, 1))
                ))
                .setDeletingTopics(List.of())
        );

        // Group epoch is bumped on the next heartbeat.
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result1 = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(2));

        assertEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 3, computeGroupHash(Map.of(
                    t1Name, computeTopicHash(t1Name, metadataImage)
                )))
            ),
            result1.records()
        );

        // Group epoch is not bumped again.
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result2 = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(2));

        assertEquals(
            List.of(),
            result2.records()
        );
    }

    @Test
    public void testShareGroupHeartbeatPersisterRequestWithInitializing() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        MockTime time = new MockTime();
        int initRetryTimeoutMs = 10;
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withTime(time)
            .withConfig(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, initRetryTimeoutMs - 1)
            .withConfig(GroupCoordinatorConfig.SHARE_GROUP_INITIALIZE_RETRY_INTERVAL_MS_CONFIG, initRetryTimeoutMs)
            .build();

        Uuid t1Uuid = Uuid.randomUuid();
        String t1Name = "t1";
        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(t1Uuid, t1Name, 2)
            .buildCoordinatorMetadataImage();

        String groupId = "share-group";

        context.groupMetadataManager.onMetadataUpdate(mock(CoordinatorMetadataDelta.class), image);
        context.groupMetadataManager.replay(
            new ShareGroupMetadataKey()
                .setGroupId(groupId),
            new ShareGroupMetadataValue()
                .setEpoch(1)
        );
        context.groupMetadataManager.replay(
            new ShareGroupStatePartitionMetadataKey()
                .setGroupId(groupId),
            new ShareGroupStatePartitionMetadataValue()
                .setInitializingTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t1Uuid)
                        .setTopicName(t1Name)
                        .setPartitions(List.of(0, 1))
                ))
                .setInitializedTopics(List.of())
                .setDeletingTopics(List.of())
        );

        Uuid memberId = Uuid.randomUuid();
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId.toString())
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of(t1Name)));

        assertFalse(result.records().contains(
            newShareGroupStatePartitionMetadataRecord(groupId, mkShareGroupStateMap(List.of(
                    mkShareGroupStateMetadataEntry(t1Uuid, t1Name, List.of(0, 1))
                )),
                Map.of(),
                Map.of()
            ))
        );

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(t1Uuid, Set.of(0, 1)),
            groupId,
            2,
            false
        );

        // Manipulate time so the initializing topic becomes eligible for retry
        context.groupMetadataManager.replay(
            new ShareGroupStatePartitionMetadataKey()
                .setGroupId(groupId),
            new ShareGroupStatePartitionMetadataValue()
                .setInitializingTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t1Uuid)
                        .setTopicName(t1Name)
                        .setPartitions(List.of(0, 1))
                ))
                .setInitializedTopics(List.of())
                .setDeletingTopics(List.of())
        );

        long timeNow = time.milliseconds() + initRetryTimeoutMs + 1;
        time.setCurrentTimeMs(timeNow);
        memberId = Uuid.randomUuid();
        result = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId.toString())
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of(t1Name)));

        assertTrue(result.records().contains(
            newShareGroupStatePartitionMetadataRecord(groupId, mkShareGroupStateMap(List.of(
                    mkShareGroupStateMetadataEntry(t1Uuid, t1Name, List.of(0, 1))
                )),
                Map.of(),
                Map.of()
            ))
        );

        verifyShareGroupHeartbeatInitializeRequest(
            result.response().getValue(),
            Map.of(t1Uuid, Set.of(0, 1)),
            groupId,
            3,
            true
        );
        verify(context.metrics, times(2)).record(SHARE_GROUP_REBALANCES_SENSOR_NAME);
    }

    @Test
    public void testShareGroupInitializingClearsCommonDeleting() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        Uuid t1Uuid = Uuid.randomUuid();
        String t1Name = "t1";
        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(t1Uuid, t1Name, 2)
            .buildCoordinatorMetadataImage();

        String groupId = "share-group";

        context.groupMetadataManager.onMetadataUpdate(mock(CoordinatorMetadataDelta.class), image);
        context.groupMetadataManager.replay(
            new ShareGroupMetadataKey()
                .setGroupId(groupId),
            new ShareGroupMetadataValue()
                .setEpoch(0)
        );

        // Replay a deleting record.
        context.groupMetadataManager.replay(
            new ShareGroupStatePartitionMetadataKey()
                .setGroupId(groupId),
            new ShareGroupStatePartitionMetadataValue()
                .setInitializingTopics(List.of())
                .setInitializedTopics(List.of())
                .setDeletingTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicInfo()
                        .setTopicId(t1Uuid)
                        .setTopicName(t1Name)
                ))
        );

        List<CoordinatorRecord> records = new ArrayList<>();
        context.groupMetadataManager.addInitializingTopicsRecords(groupId, records, Map.of(t1Uuid, new InitMapValue(t1Name, Set.of(0, 1), 1)));

        List<CoordinatorRecord> expectedRecords = List.of(
            CoordinatorRecord.record(
                new ShareGroupStatePartitionMetadataKey()
                    .setGroupId(groupId),
                new ApiMessageAndVersion(
                    new ShareGroupStatePartitionMetadataValue()
                        .setInitializingTopics(List.of(
                            new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                                .setTopicId(t1Uuid)
                                .setTopicName(t1Name)
                                .setPartitions(List.of(0, 1))
                        ))
                        .setInitializedTopics(List.of())
                        .setDeletingTopics(List.of()),
                    (short) 0
                )
            )
        );

        assertEquals(expectedRecords, records);
    }

    @Test
    public void testShareGroupInitializeSuccess() {
        String groupId = "groupId";
        Uuid topicId = Uuid.randomUuid();
        String topicName = "t1";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .withMetadataImage(new MetadataImageBuilder()
                .addTopic(topicId, topicName, 2)
                .buildCoordinatorMetadataImage()
            )
            .build();

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
                        .setTopicId(topicId)
                        .setTopicName(topicName)
                        .setPartitions(List.of(0, 1))
                ))
                .setInitializedTopics(List.of())
                .setDeletingTopics(List.of())
        );

        Map<Uuid, Set<Integer>> snapshotMetadataInitializeMap = Map.of(
            topicId,
            Set.of(0, 1)
        );

        Map<Uuid, InitMapValue> snapshotMetadataInitializeRecordMap = Map.of(
            topicId,
            new InitMapValue(
                topicName,
                Set.of(0, 1),
                context.time.milliseconds()
            )
        );

        CoordinatorResult<Void, CoordinatorRecord> result = context.groupMetadataManager.initializeShareGroupState(groupId, snapshotMetadataInitializeMap);

        CoordinatorRecord record = newShareGroupStatePartitionMetadataRecord(groupId, Map.of(), snapshotMetadataInitializeRecordMap, Map.of());

        assertNull(result.response());
        assertEquals(1, result.records().size());
        assertRecordEquals(record, result.records().get(0));
        // Make sure the timeline map is not modified yet.
        assertEquals(snapshotMetadataInitializeRecordMap, context.groupMetadataManager.shareGroupStatePartitionMetadata().get(groupId).initializingTopics());
    }

    @Test
    public void testShareGroupInitializeEmptyMap() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        String groupId = "groupId";
        context.groupMetadataManager.replay(
            new ShareGroupMetadataKey()
                .setGroupId(groupId),
            new ShareGroupMetadataValue()
                .setEpoch(0)
        );

        CoordinatorResult<Void, CoordinatorRecord> result = context.groupMetadataManager.initializeShareGroupState(groupId, Map.of());

        assertNull(result.response());
        assertEquals(List.of(), result.records());

        result = context.groupMetadataManager.initializeShareGroupState(groupId, null);

        assertNull(result.response());
        assertEquals(List.of(), result.records());

        assertEquals(Map.of(), context.groupMetadataManager.initializedShareGroupPartitions(groupId));
    }

    @Test
    public void testMaybeCleanupShareGroupStateEmptyTopicIds() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("simple");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        CoordinatorResult<Void, CoordinatorRecord> expectedResults = new CoordinatorResult<>(List.of());
        assertEquals(expectedResults, context.groupMetadataManager.maybeCleanupShareGroupState(Set.of()));

        context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        Set<Uuid> topicIds = Set.of(Uuid.randomUuid());
        assertEquals(expectedResults, context.groupMetadataManager.maybeCleanupShareGroupState(topicIds));
    }

    @Test
    public void testMaybeCleanupShareGroupStateInitDeletedTopicsPresent() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("simple");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        String groupId = "sharegroup";
        Uuid t1Id = Uuid.randomUuid();
        String t1Name = "t1";
        Uuid t2Id = Uuid.randomUuid();
        String t2Name = "t2";
        Uuid t3Id = Uuid.randomUuid();
        String t3Name = "t3";
        Uuid t4Id = Uuid.randomUuid();
        String t4Name = "t4";
        Uuid t5Id = Uuid.randomUuid();
        String t5Name = "t5";
        Uuid t6Id = Uuid.randomUuid();
        String t6Name = "t6";

        CoordinatorMetadataImage image = new MetadataImageBuilder()
            .addTopic(t1Id, t1Name, 2)
            .addTopic(t2Id, t2Name, 3)
            .addTopic(t3Id, t3Name, 3)
            .addTopic(t4Id, t4Name, 3)
            .addTopic(t5Id, t5Name, 3)
            .addTopic(t6Id, t6Name, 3)
            .buildCoordinatorMetadataImage();

        context.groupMetadataManager.onMetadataUpdate(image.emptyDelta(), image);

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
                        .setPartitions(List.of(0, 1)),
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t3Id)
                        .setTopicName(t3Name)
                        .setPartitions(List.of(0, 1, 2))
                ))
                .setInitializedTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t2Id)
                        .setTopicName(t2Name)
                        .setPartitions(List.of(0, 1, 2)),
                    new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                        .setTopicId(t4Id)
                        .setTopicName(t4Name)
                        .setPartitions(List.of(0, 1, 2))
                ))
                .setDeletingTopics(List.of(
                    new ShareGroupStatePartitionMetadataValue.TopicInfo()
                        .setTopicId(t5Id)
                        .setTopicName(t5Name),
                    new ShareGroupStatePartitionMetadataValue.TopicInfo()
                        .setTopicId(t6Id)
                        .setTopicName(t6Name)
                ))
        );

        List<CoordinatorRecord> expectedRecords = List.of(
            CoordinatorRecord.record(
                new ShareGroupStatePartitionMetadataKey()
                    .setGroupId(groupId),
                new ApiMessageAndVersion(
                    new ShareGroupStatePartitionMetadataValue()
                        .setInitializingTopics(List.of(
                            new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                                .setTopicId(t3Id)
                                .setTopicName(t3Name)
                                .setPartitions(List.of(0, 1, 2))
                        ))
                        .setInitializedTopics(List.of(
                            new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                                .setTopicId(t4Id)
                                .setTopicName(t4Name)
                                .setPartitions(List.of(0, 1, 2))
                        ))
                        .setDeletingTopics(List.of(
                            new ShareGroupStatePartitionMetadataValue.TopicInfo()
                                .setTopicId(t5Id)
                                .setTopicName(t5Name)
                        )),
                    (short) 0
                )
            )
        );

        CoordinatorResult<Void, CoordinatorRecord> expectedResult = new CoordinatorResult<>(expectedRecords);
        assertEquals(expectedResult, context.groupMetadataManager.maybeCleanupShareGroupState(Set.of(t1Id, t2Id, t6Id)));
    }

    private static Stream<CoordinatorRecord> shareGroupRecords() {
        String groupId = "groupId";
        String memberId = Uuid.randomUuid().toString();

        return Stream.of(
            // Tombstones
            CoordinatorRecord.tombstone(
                new ShareGroupMemberMetadataKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
            ),
            CoordinatorRecord.tombstone(
                new ShareGroupMetadataKey()
                    .setGroupId(groupId)
            ),
            CoordinatorRecord.tombstone(
                new ShareGroupTargetAssignmentMemberKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
            ),
            CoordinatorRecord.tombstone(
                new ShareGroupTargetAssignmentMetadataKey()
                    .setGroupId(groupId)
            ),
            CoordinatorRecord.tombstone(
                new ShareGroupCurrentMemberAssignmentKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
            ),
            CoordinatorRecord.tombstone(
                new ShareGroupStatePartitionMetadataKey()
                    .setGroupId(groupId)
            ),
            // Data
            CoordinatorRecord.record(
                new ShareGroupMemberMetadataKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                new ApiMessageAndVersion(
                    new ShareGroupMemberMetadataValue()
                        .setSubscribedTopicNames(List.of("tp1")),
                    (short) 10
                )
            ),
            CoordinatorRecord.record(
                new ShareGroupMetadataKey()
                    .setGroupId(groupId),
                new ApiMessageAndVersion(
                    new ShareGroupMetadataValue()
                        .setEpoch(1)
                        .setMetadataHash(2L),
                    (short) 11
                )
            ),
            CoordinatorRecord.record(
                new ShareGroupTargetAssignmentMetadataKey()
                    .setGroupId(groupId),
                new ApiMessageAndVersion(
                    new ShareGroupTargetAssignmentMetadataValue()
                        .setAssignmentEpoch(5),
                    (short) 12
                )
            ),
            CoordinatorRecord.record(
                new ShareGroupTargetAssignmentMemberKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                new ApiMessageAndVersion(new ShareGroupTargetAssignmentMemberValue()
                    .setTopicPartitions(List.of(
                        new ShareGroupTargetAssignmentMemberValue.TopicPartition()
                            .setTopicId(Uuid.randomUuid())
                            .setPartitions(List.of(0, 1, 2))
                    )),
                    (short) 13
                )
            ),
            CoordinatorRecord.record(
                new ShareGroupCurrentMemberAssignmentKey()
                    .setGroupId(groupId)
                    .setMemberId(memberId),
                new ApiMessageAndVersion(new ShareGroupCurrentMemberAssignmentValue()
                    .setAssignedPartitions(List.of(
                            new ShareGroupCurrentMemberAssignmentValue.TopicPartitions()
                                .setTopicId(Uuid.randomUuid())
                                .setPartitions(List.of(0, 1, 2))
                        )
                    )
                    .setMemberEpoch(5)
                    .setPreviousMemberEpoch(4)
                    .setState((byte) 0),
                    (short) 14
                )
            ),
            CoordinatorRecord.record(
                new ShareGroupStatePartitionMetadataKey()
                    .setGroupId(groupId),
                new ApiMessageAndVersion(new ShareGroupStatePartitionMetadataValue()
                    .setInitializingTopics(List.of())
                    .setInitializedTopics(List.of())
                    .setDeletingTopics(List.of()),
                    (short) 15
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("shareGroupRecords")
    public void testShareGroupRecordsNoExceptionOnReplay(CoordinatorRecord record) {
        MockPartitionAssignor assignor = new MockPartitionAssignor("simple");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = spy(new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build());

        assertDoesNotThrow(() -> context.replay(record));
    }

    private record PendingAssignmentCase(
        String description,
        String groupId,
        ShareGroup group,
        boolean expectedValue,
        Runnable assertions
    ) {
    }

    private static Stream<Function<GroupMetadataManagerTestContext, PendingAssignmentCase>> generatePendingAssignmentCases() {
        String groupId1 = "groupId";
        Uuid tid1 = Uuid.randomUuid();
        String tName1 = "t1";
        Uuid tid2 = Uuid.randomUuid();
        String tName2 = "t2";

        return Stream.of(
            (GroupMetadataManagerTestContext context) -> {
                ShareGroup group = mock(ShareGroup.class);
                when(group.isEmpty()).thenReturn(true);
                return new PendingAssignmentCase("Group is empty", groupId1, group, false, () -> {
                    verify(group, times(0)).groupId();
                    verify(group).isEmpty();
                });
            },
            (GroupMetadataManagerTestContext context) -> {
                ShareGroup group = mock(ShareGroup.class);
                when(group.groupId()).thenReturn(groupId1);
                when(group.isEmpty()).thenReturn(false);
                return new PendingAssignmentCase("Group not in metadata", groupId1, group, false, () -> {
                    verify(group).groupId();
                    verify(group).isEmpty();
                });
            },
            (GroupMetadataManagerTestContext context) -> {
                ShareGroup group = mock(ShareGroup.class);
                when(group.groupId()).thenReturn(groupId1);
                when(group.isEmpty()).thenReturn(false);
                context.groupMetadataManager.replay(
                    new ShareGroupStatePartitionMetadataKey()
                        .setGroupId(groupId1),
                    new ShareGroupStatePartitionMetadataValue()
                );
                context.commit();
                return new PendingAssignmentCase("Group metadata initialized topics empty", groupId1, group, false, () -> {
                    verify(group).groupId();
                    verify(group).isEmpty();
                });
            },
            (GroupMetadataManagerTestContext context) -> {
                ShareGroup group = mock(ShareGroup.class);
                when(group.groupId()).thenReturn(groupId1);
                when(group.isEmpty()).thenReturn(false);
                when(group.subscribedTopicNames()).thenReturn(Map.of());
                context.groupMetadataManager.replay(
                    new ShareGroupStatePartitionMetadataKey()
                        .setGroupId(groupId1),
                    new ShareGroupStatePartitionMetadataValue()
                        .setInitializedTopics(List.of(
                            new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                                .setTopicName(tName1)
                                .setTopicId(tid1)
                                .setPartitions(List.of(0, 1))
                        ))
                );
                context.commit();
                return new PendingAssignmentCase("Empty group subscription", groupId1, group, false, () -> {
                    verify(group).groupId();
                    verify(group).isEmpty();
                    verify(group).subscribedTopicNames();
                });
            },
            (GroupMetadataManagerTestContext context) -> {
                ShareGroup group = mock(ShareGroup.class);
                when(group.groupId()).thenReturn(groupId1);
                when(group.isEmpty()).thenReturn(false);
                when(group.subscribedTopicNames()).thenReturn(Map.of(tName2, new SubscriptionCount(1, 1)));
                when(group.targetAssignment()).thenReturn(Map.of());
                context.groupMetadataManager.replay(
                    new ShareGroupStatePartitionMetadataKey()
                        .setGroupId(groupId1),
                    new ShareGroupStatePartitionMetadataValue()
                        .setInitializedTopics(List.of(
                            new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                                .setTopicName(tName1)
                                .setTopicId(tid1)
                                .setPartitions(List.of(0, 1))
                        ))
                );
                context.commit();
                return new PendingAssignmentCase("Subscribed topics not in metadata and empty assignment.", groupId1, group, false, () -> {
                    verify(group).groupId();
                    verify(group).isEmpty();
                    verify(group).subscribedTopicNames();
                    verify(group).targetAssignment();
                });
            },
            (GroupMetadataManagerTestContext context) -> {
                ShareGroup group = mock(ShareGroup.class);
                when(group.groupId()).thenReturn(groupId1);
                when(group.isEmpty()).thenReturn(false);
                when(group.subscribedTopicNames()).thenReturn(Map.of(tName1, new SubscriptionCount(1, 1)));
                when(group.targetAssignment()).thenReturn(Map.of(tName1, new Assignment(Map.of(tid1, Set.of(0, 1)))));
                context.groupMetadataManager.replay(
                    new ShareGroupStatePartitionMetadataKey()
                        .setGroupId(groupId1),
                    new ShareGroupStatePartitionMetadataValue()
                        .setInitializedTopics(List.of(
                            new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                                .setTopicName(tName1)
                                .setTopicId(tid1)
                                .setPartitions(List.of(0, 1))
                        ))
                );
                context.commit();
                return new PendingAssignmentCase("Subscribed topics in metadata and assigned partitions match.", groupId1, group, false, () -> {
                    verify(group).groupId();
                    verify(group).isEmpty();
                    verify(group).subscribedTopicNames();
                    verify(group).targetAssignment();
                });
            },
            (GroupMetadataManagerTestContext context) -> {
                ShareGroup group = mock(ShareGroup.class);
                when(group.groupId()).thenReturn(groupId1);
                when(group.isEmpty()).thenReturn(false);
                when(group.subscribedTopicNames()).thenReturn(Map.of(tName1, new SubscriptionCount(1, 1)));
                when(group.targetAssignment()).thenReturn(Map.of(tName1, new Assignment(Map.of(tid1, Set.of(0)))));
                context.groupMetadataManager.replay(
                    new ShareGroupStatePartitionMetadataKey()
                        .setGroupId(groupId1),
                    new ShareGroupStatePartitionMetadataValue()
                        .setInitializedTopics(List.of(
                            new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                                .setTopicName(tName1)
                                .setTopicId(tid1)
                                .setPartitions(List.of(0, 1))
                        ))
                );
                context.commit();
                return new PendingAssignmentCase("Subscribed topics in metadata but assigned partitions differ.", groupId1, group, true, () -> {
                    verify(group).groupId();
                    verify(group).isEmpty();
                    verify(group).subscribedTopicNames();
                    verify(group).targetAssignment();
                });
            },
            (GroupMetadataManagerTestContext context) -> {
                ShareGroup group = mock(ShareGroup.class);
                when(group.groupId()).thenReturn(groupId1);
                when(group.isEmpty()).thenReturn(false);
                when(group.subscribedTopicNames()).thenReturn(Map.of(tName1, new SubscriptionCount(1, 1)));
                when(group.targetAssignment()).thenReturn(Map.of(
                    tName1, new Assignment(Map.of(tid1, Set.of(0, 1))),
                    tName2, new Assignment(Map.of(tid2, Set.of(0)))
                ));
                context.groupMetadataManager.replay(
                    new ShareGroupStatePartitionMetadataKey()
                        .setGroupId(groupId1),
                    new ShareGroupStatePartitionMetadataValue()
                        .setInitializedTopics(List.of(
                            new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                                .setTopicName(tName1)
                                .setTopicId(tid1)
                                .setPartitions(List.of(0, 1)),
                            new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
                                .setTopicName(tName2)
                                .setTopicId(tid2)
                                .setPartitions(List.of(0))
                        ))
                );
                context.commit();
                return new PendingAssignmentCase("Subscribed topics in metadata but assigned has other topics too.", groupId1, group, false, () -> {
                    verify(group).groupId();
                    verify(group).isEmpty();
                    verify(group).subscribedTopicNames();
                    verify(group).targetAssignment();
                });
            }
        );
    }

    @SuppressWarnings("ClassEscapesDefinedScope")
    @ParameterizedTest
    @MethodSource("generatePendingAssignmentCases")
    public void testShareGroupPendingAssignments(Function<GroupMetadataManagerTestContext, PendingAssignmentCase> testCase) {
        MockPartitionAssignor assignor = new MockPartitionAssignor("simple");
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of()));
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withShareGroupAssignor(assignor)
            .build();

        PendingAssignmentCase test = testCase.apply(context);
        assertEquals(test.expectedValue, context.groupMetadataManager.initializedAssignmentPending(test.group), test.description);
        test.assertions.run();
    }

    @Test
    public void testShareGroupAssignmentInterval() {
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
            .withConfig(GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, 5000)
            .withShareGroupAssignor(assignor)
            .withMetadataImage(metadataImage)
            .build();

        // Member 1 joins the group and gets an assignment immediately.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            ))
        )));
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result1 = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId1)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of(fooTopicName)));

        assertResponseEquals(
            new ShareGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ShareGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                    ))),
            result1.response().getKey()
        );

        ShareGroupMember expectedMember1 = new ShareGroupMember.Builder(memberId1)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
            .build();

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionRecord(groupId, expectedMember1),
                GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 2, computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage)
                ))),
                GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
                )),
                GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord(groupId, 2, context.time.milliseconds()),
                GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord(groupId, expectedMember1),
                GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(groupId, mkShareGroupStateMap(List.of(
                        mkShareGroupStateMetadataEntry(fooTopicId, fooTopicName, List.of(0, 1, 2, 3, 4, 5))
                    )),
                    Map.of(),
                    Map.of()
                )
            ),
            result1.records()
        );

        // Wait until just before the expected delay.
        context.time.sleep(4995);

        // Member 2 joins the group and gets no assignment.
        assignor.prepareGroupAssignment(new GroupAssignment(Map.of(
            memberId1, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            )),
            memberId2, new MemberAssignmentImpl(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
            ))
        )));
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result2 = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setSubscribedTopicNames(List.of(fooTopicName)));

        assertResponseEquals(
            new ShareGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(2)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ShareGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of())),
            result2.response().getKey()
        );

        ShareGroupMember expectedMember2 = new ShareGroupMember.Builder(memberId2)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(0)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setAssignedPartitions(mkAssignment())
            .build();

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionRecord(groupId, expectedMember2),
                GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, 3, computeGroupHash(Map.of(
                    fooTopicName, computeTopicHash(fooTopicName, metadataImage)
                ))),
                GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord(groupId, expectedMember2)
            ),
            result2.records()
        );

        // Wait a little more. The next target assignment can be computed now.
        context.time.sleep(10);

        // The next target assignment is computed.
        CoordinatorResult<Map.Entry<ShareGroupHeartbeatResponseData, Optional<InitializeShareGroupStateParameters>>, CoordinatorRecord> result3 = context.shareGroupHeartbeat(
            new ShareGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(2));

        assertResponseEquals(
            new ShareGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(3)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ShareGroupHeartbeatResponseData.Assignment()
                    .setTopicPartitions(List.of(
                        new ShareGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(List.of(0, 1, 2, 3, 4, 5))
                    ))),
            result3.response().getKey()
        );

        ShareGroupMember expectedMember3 = new ShareGroupMember.Builder(memberId2)
            .setMemberEpoch(3)
            .setPreviousMemberEpoch(2)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(fooTopicName))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
            .build();

        assertRecordsEquals(
            List.of(
                GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)
                )),
                GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord(groupId, 3, context.time.milliseconds()),
                GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord(groupId, expectedMember3)
            ),
            result3.records()
        );
    }

    private void verifyShareGroupHeartbeatInitializeRequest(
        Optional<InitializeShareGroupStateParameters> initRequest,
        Map<Uuid, Set<Integer>> expectedTopicPartitionsMap,
        String groupId,
        int stateEpoch,
        boolean shouldExist
    ) {
        if (shouldExist) {
            assertTrue(initRequest.isPresent());
            InitializeShareGroupStateParameters request = initRequest.get();
            assertEquals(groupId, request.groupTopicPartitionData().groupId());
            Map<Uuid, Set<Integer>> actualTopicPartitionsMap = new HashMap<>();
            for (TopicData<PartitionStateData> topicData : request.groupTopicPartitionData().topicsData()) {
                actualTopicPartitionsMap.computeIfAbsent(topicData.topicId(), k -> new HashSet<>())
                    .addAll(topicData.partitions().stream().map(partitionData -> {
                        assertEquals(stateEpoch, partitionData.stateEpoch());
                        assertEquals(-1, partitionData.startOffset());
                        return partitionData.partition();
                    }).toList());
            }
            assertEquals(expectedTopicPartitionsMap, actualTopicPartitionsMap);
        } else {
            assertTrue(initRequest.isEmpty());
        }
    }

    private void verifyShareGroupDeleteRequest(
        Optional<DeleteShareGroupStateParameters> deleteRequest,
        Map<Uuid, Set<Integer>> expectedTopicPartitionsMap,
        String groupId,
        boolean shouldExist
    ) {
        if (shouldExist) {
            assertTrue(deleteRequest.isPresent());
            DeleteShareGroupStateParameters request = deleteRequest.get();
            assertEquals(groupId, request.groupTopicPartitionData().groupId());
            Map<Uuid, Set<Integer>> actualTopicPartitionsMap = new HashMap<>();
            for (TopicData<PartitionIdData> topicData : request.groupTopicPartitionData().topicsData()) {
                actualTopicPartitionsMap.computeIfAbsent(topicData.topicId(), k -> new HashSet<>())
                    .addAll(topicData.partitions().stream().map(PartitionIdData::partition).toList());
            }
            assertEquals(expectedTopicPartitionsMap, actualTopicPartitionsMap);
        } else {
            assertTrue(deleteRequest.isEmpty());
        }
    }

    private Map<Uuid, DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> convertResponseTopicListToMap(
        List<DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic> responseTopics
    ) {
        return responseTopics.stream()
            .collect(Collectors.toMap(DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic::topicId, Function.identity()));
    }

}
