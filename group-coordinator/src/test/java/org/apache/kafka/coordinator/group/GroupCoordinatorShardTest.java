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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.LegacyOffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.LegacyOffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyKey;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.share.persister.DeleteShareGroupStateParameters;
import org.apache.kafka.server.share.persister.GroupTopicPartitionData;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PartitionIdData;
import org.apache.kafka.server.share.persister.TopicData;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.common.runtime.TestUtil.requestContext;
import static org.apache.kafka.coordinator.group.GroupCoordinatorShard.DEFAULT_GROUP_GAUGES_UPDATE_INTERVAL_MS;
import static org.apache.kafka.coordinator.group.GroupCoordinatorShard.GROUP_EXPIRATION_KEY;
import static org.apache.kafka.coordinator.group.GroupCoordinatorShard.GROUP_SIZE_COUNTER_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"ClassFanOutComplexity"})
public class GroupCoordinatorShardTest {

    @Test
    public void testConsumerGroupHeartbeat() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        RequestContext context = requestContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT);
        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData();
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, CoordinatorRecord> result = new CoordinatorResult<>(
            Collections.emptyList(),
            new ConsumerGroupHeartbeatResponseData()
        );

        when(groupMetadataManager.consumerGroupHeartbeat(
            context,
            request
        )).thenReturn(result);

        assertEquals(result, coordinator.consumerGroupHeartbeat(context, request));
    }

    @Test
    public void testCommitOffset() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        RequestContext context = requestContext(ApiKeys.OFFSET_COMMIT);
        OffsetCommitRequestData request = new OffsetCommitRequestData();
        CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> result = new CoordinatorResult<>(
            Collections.emptyList(),
            new OffsetCommitResponseData()
        );

        when(offsetMetadataManager.commitOffset(
            context,
            request
        )).thenReturn(result);

        assertEquals(result, coordinator.commitOffset(context, request));
    }

    @Test
    public void testCommitTransactionalOffset() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(new MockTime()),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        RequestContext context = requestContext(ApiKeys.TXN_OFFSET_COMMIT);
        TxnOffsetCommitRequestData request = new TxnOffsetCommitRequestData();
        CoordinatorResult<TxnOffsetCommitResponseData, CoordinatorRecord> result = new CoordinatorResult<>(
            Collections.emptyList(),
            new TxnOffsetCommitResponseData()
        );

        when(offsetMetadataManager.commitTransactionalOffset(
            context,
            request
        )).thenReturn(result);

        assertEquals(result, coordinator.commitTransactionalOffset(context, request));
    }

    @Test
    public void testDeleteGroups() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            mock(CoordinatorMetrics.class),
            mock(CoordinatorMetricsShard.class)
        );

        RequestContext context = requestContext(ApiKeys.DELETE_GROUPS);
        List<String> groupIds = Arrays.asList("group-id-1", "group-id-2");
        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection = new DeleteGroupsResponseData.DeletableGroupResultCollection();
        List<CoordinatorRecord> expectedRecords = new ArrayList<>();
        for (String groupId : groupIds) {
            expectedResultCollection.add(new DeleteGroupsResponseData.DeletableGroupResult().setGroupId(groupId));
            expectedRecords.addAll(Arrays.asList(
                GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(groupId, "topic-name", 0),
                GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId)
            ));
        }

        CoordinatorResult<DeleteGroupsResponseData.DeletableGroupResultCollection, CoordinatorRecord> expectedResult = new CoordinatorResult<>(
            expectedRecords,
            expectedResultCollection
        );

        when(offsetMetadataManager.deleteAllOffsets(anyString(), anyList())).thenAnswer(invocation -> {
            String groupId = invocation.getArgument(0);
            List<CoordinatorRecord> records = invocation.getArgument(1);
            records.add(GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(groupId, "topic-name", 0));
            return 1;
        });
        // Mockito#when only stubs method returning non-void value, so we use Mockito#doAnswer instead.
        doAnswer(invocation -> {
            String groupId = invocation.getArgument(0);
            List<CoordinatorRecord> records = invocation.getArgument(1);
            records.add(GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId));
            return null;
        }).when(groupMetadataManager).createGroupTombstoneRecords(anyString(), anyList());

        CoordinatorResult<DeleteGroupsResponseData.DeletableGroupResultCollection, CoordinatorRecord> coordinatorResult =
            coordinator.deleteGroups(context, groupIds);

        for (String groupId : groupIds) {
            verify(groupMetadataManager, times(1)).validateDeleteGroup(ArgumentMatchers.eq(groupId));
            verify(groupMetadataManager, times(1)).createGroupTombstoneRecords(ArgumentMatchers.eq(groupId), anyList());
            verify(offsetMetadataManager, times(1)).deleteAllOffsets(ArgumentMatchers.eq(groupId), anyList());
        }
        assertEquals(expectedResult, coordinatorResult);
    }

    @Test
    public void testDeleteGroupsInvalidGroupId() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            mock(CoordinatorMetrics.class),
            mock(CoordinatorMetricsShard.class)
        );

        RequestContext context = requestContext(ApiKeys.DELETE_GROUPS);
        List<String> groupIds = Arrays.asList("group-id-1", "group-id-2", "group-id-3");

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection(Arrays.asList(
                new DeleteGroupsResponseData.DeletableGroupResult()
                    .setGroupId("group-id-1"),
                new DeleteGroupsResponseData.DeletableGroupResult()
                    .setGroupId("group-id-2")
                    .setErrorCode(Errors.INVALID_GROUP_ID.code()),
                new DeleteGroupsResponseData.DeletableGroupResult()
                    .setGroupId("group-id-3")
            ).iterator());
        List<CoordinatorRecord> expectedRecords = Arrays.asList(
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group-id-1", "topic-name", 0),
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord("group-id-1"),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group-id-3", "topic-name", 0),
            GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord("group-id-3")
        );
        CoordinatorResult<DeleteGroupsResponseData.DeletableGroupResultCollection, CoordinatorRecord> expectedResult = new CoordinatorResult<>(
            expectedRecords,
            expectedResultCollection
        );

        // Mockito#when only stubs method returning non-void value, so we use Mockito#doAnswer and Mockito#doThrow instead.
        doThrow(Errors.INVALID_GROUP_ID.exception())
            .when(groupMetadataManager).validateDeleteGroup(ArgumentMatchers.eq("group-id-2"));
        doAnswer(invocation -> {
            String groupId = invocation.getArgument(0);
            List<CoordinatorRecord> records = invocation.getArgument(1);
            records.add(GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(groupId, "topic-name", 0));
            return null;
        }).when(offsetMetadataManager).deleteAllOffsets(anyString(), anyList());
        doAnswer(invocation -> {
            String groupId = invocation.getArgument(0);
            List<CoordinatorRecord> records = invocation.getArgument(1);
            records.add(GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord(groupId));
            return null;
        }).when(groupMetadataManager).createGroupTombstoneRecords(anyString(), anyList());

        CoordinatorResult<DeleteGroupsResponseData.DeletableGroupResultCollection, CoordinatorRecord> coordinatorResult =
            coordinator.deleteGroups(context, groupIds);

        for (String groupId : groupIds) {
            verify(groupMetadataManager, times(1)).validateDeleteGroup(eq(groupId));
            if (!groupId.equals("group-id-2")) {
                verify(groupMetadataManager, times(1)).createGroupTombstoneRecords(eq(groupId), anyList());
                verify(offsetMetadataManager, times(1)).deleteAllOffsets(eq(groupId), anyList());
            }
        }
        assertEquals(expectedResult, coordinatorResult);
    }

    @Test
    public void testReplayOffsetCommit() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        OffsetCommitKey key = new OffsetCommitKey()
            .setGroup("goo")
            .setTopic("foo")
            .setPartition(0);
        OffsetCommitValue value = new OffsetCommitValue()
            .setOffset(100L)
            .setCommitTimestamp(12345L)
            .setExpireTimestamp(6789L)
            .setMetadata("Metadata")
            .setLeaderEpoch(10);

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            new LegacyOffsetCommitKey()
                .setGroup("goo")
                .setTopic("foo")
                .setPartition(0),
            new ApiMessageAndVersion(
                new LegacyOffsetCommitValue()
                    .setOffset(100L)
                    .setCommitTimestamp(12345L)
                    .setMetadata("Metadata"),
                (short) 0
            )
        ));

        coordinator.replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(offsetMetadataManager, times(1)).replay(
            0L,
            RecordBatch.NO_PRODUCER_ID,
            new OffsetCommitKey()
                .setGroup("goo")
                .setTopic("foo")
                .setPartition(0),
            new OffsetCommitValue()
                .setOffset(100L)
                .setCommitTimestamp(12345L)
                .setMetadata("Metadata")
        );

        verify(offsetMetadataManager, times(1)).replay(
            1L,
            RecordBatch.NO_PRODUCER_ID,
            key,
            value
        );
    }

    @Test
    public void testReplayTransactionalOffsetCommit() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(new MockTime()),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        OffsetCommitKey key = new OffsetCommitKey()
            .setGroup("goo")
            .setTopic("foo")
            .setPartition(0);
        OffsetCommitValue value = new OffsetCommitValue()
            .setOffset(100L)
            .setCommitTimestamp(12345L)
            .setExpireTimestamp(6789L)
            .setMetadata("Metadata")
            .setLeaderEpoch(10);

        coordinator.replay(0L, 100L, (short) 0, CoordinatorRecord.record(
            new LegacyOffsetCommitKey()
                .setGroup("goo")
                .setTopic("foo")
                .setPartition(0),
            new ApiMessageAndVersion(
                new LegacyOffsetCommitValue()
                    .setOffset(100L)
                    .setCommitTimestamp(12345L)
                    .setMetadata("Metadata"),
                (short) 0
            )
        ));

        coordinator.replay(1L, 101L, (short) 1, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(offsetMetadataManager, times(1)).replay(
            0L,
            100L,
            new OffsetCommitKey()
                .setGroup("goo")
                .setTopic("foo")
                .setPartition(0),
            new OffsetCommitValue()
                .setOffset(100L)
                .setCommitTimestamp(12345L)
                .setMetadata("Metadata")
        );

        verify(offsetMetadataManager, times(1)).replay(
            1L,
            101L,
            key,
            value
        );
    }

    @Test
    public void testReplayOffsetCommitWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        OffsetCommitKey key = new OffsetCommitKey()
            .setGroup("goo")
            .setTopic("foo")
            .setPartition(0);

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            new LegacyOffsetCommitKey()
                .setGroup("goo")
                .setTopic("foo")
                .setPartition(0)
        ));

        coordinator.replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(offsetMetadataManager, times(1)).replay(
            0L,
            RecordBatch.NO_PRODUCER_ID,
            key,
            null
        );

        verify(offsetMetadataManager, times(1)).replay(
            1L,
            RecordBatch.NO_PRODUCER_ID,
            key,
            null
        );
    }

    @Test
    public void testReplayConsumerGroupMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupMetadataKey key = new ConsumerGroupMetadataKey();
        ConsumerGroupMetadataValue value = new ConsumerGroupMetadataValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupMetadataKey key = new ConsumerGroupMetadataKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayConsumerGroupPartitionMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupPartitionMetadataKey key = new ConsumerGroupPartitionMetadataKey();
        ConsumerGroupPartitionMetadataValue value = new ConsumerGroupPartitionMetadataValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupPartitionMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupPartitionMetadataKey key = new ConsumerGroupPartitionMetadataKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayConsumerGroupMemberMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupMemberMetadataKey key = new ConsumerGroupMemberMetadataKey();
        ConsumerGroupMemberMetadataValue value = new ConsumerGroupMemberMetadataValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupMemberMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupMemberMetadataKey key = new ConsumerGroupMemberMetadataKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupTargetAssignmentMetadataKey key = new ConsumerGroupTargetAssignmentMetadataKey();
        ConsumerGroupTargetAssignmentMetadataValue value = new ConsumerGroupTargetAssignmentMetadataValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupTargetAssignmentMetadataKey key = new ConsumerGroupTargetAssignmentMetadataKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMember() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupTargetAssignmentMemberKey key = new ConsumerGroupTargetAssignmentMemberKey();
        ConsumerGroupTargetAssignmentMemberValue value = new ConsumerGroupTargetAssignmentMemberValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMemberKeyWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupTargetAssignmentMemberKey key = new ConsumerGroupTargetAssignmentMemberKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignment() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupCurrentMemberAssignmentKey key = new ConsumerGroupCurrentMemberAssignmentKey();
        ConsumerGroupCurrentMemberAssignmentValue value = new ConsumerGroupCurrentMemberAssignmentValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignmentWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupCurrentMemberAssignmentKey key = new ConsumerGroupCurrentMemberAssignmentKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayStreamsGroupMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );
        StreamsGroupMetadataKey key = new StreamsGroupMetadataKey();
        StreamsGroupMetadataValue value = new StreamsGroupMetadataValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager).replay(key, value);
    }

    @Test
    public void testReplayStreamsGroupMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupMetadataKey key = new StreamsGroupMetadataKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager).replay(key, null);
    }


    @Test
    public void testReplayStreamsGroupTopology() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupTopologyKey key = new StreamsGroupTopologyKey();
        StreamsGroupTopologyValue value = new StreamsGroupTopologyValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager).replay(key, value);
    }

    @Test
    public void testReplayStreamsGroupTopologyWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupTopologyKey key = new StreamsGroupTopologyKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager).replay(key, null);
    }

    @Test
    public void testReplayStreamsGroupPartitionMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupPartitionMetadataKey key = new StreamsGroupPartitionMetadataKey();
        StreamsGroupPartitionMetadataValue value = new StreamsGroupPartitionMetadataValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager).replay(key, value);
    }

    @Test
    public void testReplayStreamsGroupPartitionMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupPartitionMetadataKey key = new StreamsGroupPartitionMetadataKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager).replay(key, null);
    }

    @Test
    public void testReplayStreamsGroupMemberMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupMemberMetadataKey key = new StreamsGroupMemberMetadataKey();
        StreamsGroupMemberMetadataValue value = new StreamsGroupMemberMetadataValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager).replay(key, value);
    }

    @Test
    public void testReplayStreamsGroupMemberMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupMemberMetadataKey key = new StreamsGroupMemberMetadataKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager).replay(key, null);
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupTargetAssignmentMetadataKey key = new StreamsGroupTargetAssignmentMetadataKey();
        StreamsGroupTargetAssignmentMetadataValue value = new StreamsGroupTargetAssignmentMetadataValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager).replay(key, value);
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupTargetAssignmentMetadataKey key = new StreamsGroupTargetAssignmentMetadataKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager).replay(key, null);
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMember() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupTargetAssignmentMemberKey key = new StreamsGroupTargetAssignmentMemberKey();
        StreamsGroupTargetAssignmentMemberValue value = new StreamsGroupTargetAssignmentMemberValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager).replay(key, value);
    }

    @Test
    public void testReplayStreamsGroupTargetAssignmentMemberKeyWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupTargetAssignmentMemberKey key = new StreamsGroupTargetAssignmentMemberKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager).replay(key, null);
    }

    @Test
    public void testReplayStreamsGroupCurrentMemberAssignment() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupCurrentMemberAssignmentKey key = new StreamsGroupCurrentMemberAssignmentKey();
        StreamsGroupCurrentMemberAssignmentValue value = new StreamsGroupCurrentMemberAssignmentValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager).replay(key, value);
    }

    @Test
    public void testReplayStreamsGroupCurrentMemberAssignmentWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        StreamsGroupCurrentMemberAssignmentKey key = new StreamsGroupCurrentMemberAssignmentKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager).replay(key, null);
    }
    
    @Test
    public void testReplayKeyCannotBeNull() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        assertThrows(NullPointerException.class, () ->
            coordinator.replay(
                0L,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                CoordinatorRecord.record(null, null))
        );
    }

    @Test
    public void testOnLoaded() {
        MetadataImage image = MetadataImage.EMPTY;
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        coordinator.onLoaded(image);

        verify(groupMetadataManager, times(1)).onNewMetadataImage(
            eq(image),
            any()
        );

        verify(groupMetadataManager, times(1)).onLoaded();
    }

    @Test
    public void testReplayGroupMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        GroupMetadataKey key = new GroupMetadataKey();
        GroupMetadataValue value = new GroupMetadataValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 4)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayGroupMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        GroupMetadataKey key = new GroupMetadataKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testScheduleCleanupGroupMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        Time mockTime = new MockTime();
        MockCoordinatorTimer<Void, CoordinatorRecord> timer = new MockCoordinatorTimer<>(mockTime);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            mockTime,
            timer,
            GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 1000L, 24 * 60),
            mock(CoordinatorMetrics.class),
            mock(CoordinatorMetricsShard.class)
        );
        MetadataImage image = MetadataImage.EMPTY;

        // Confirm the cleanup is scheduled when the coordinator is initially loaded.
        coordinator.onLoaded(image);
        assertTrue(timer.contains(GROUP_EXPIRATION_KEY));

        // Confirm that it is rescheduled after completion.
        mockTime.sleep(1000L);
        List<MockCoordinatorTimer.ExpiredTimeout<Void, CoordinatorRecord>> tasks = timer.poll();
        assertEquals(1, tasks.size());
        assertTrue(timer.contains(GROUP_EXPIRATION_KEY));

        coordinator.onUnloaded();
        assertFalse(timer.contains(GROUP_EXPIRATION_KEY));
    }

    @Test
    public void testCleanupGroupMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        Time mockTime = new MockTime();
        MockCoordinatorTimer<Void, CoordinatorRecord> timer = new MockCoordinatorTimer<>(mockTime);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            mockTime,
            timer,
            mock(GroupCoordinatorConfig.class),
            mock(CoordinatorMetrics.class),
            mock(CoordinatorMetricsShard.class)
        );

        CoordinatorRecord offsetCommitTombstone = GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group-id", "topic", 0);
        CoordinatorRecord groupMetadataTombstone = GroupCoordinatorRecordHelpers.newGroupMetadataTombstoneRecord("group-id");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<CoordinatorRecord>> recordsCapture = ArgumentCaptor.forClass(List.class);

        when(groupMetadataManager.groupIds()).thenReturn(Set.of("group-id", "other-group-id"));
        when(offsetMetadataManager.cleanupExpiredOffsets(eq("group-id"), recordsCapture.capture()))
            .thenAnswer(invocation -> {
                List<CoordinatorRecord> records = recordsCapture.getValue();
                records.add(offsetCommitTombstone);
                return true;
            });
        when(offsetMetadataManager.cleanupExpiredOffsets("other-group-id", Collections.emptyList())).thenReturn(false);
        doAnswer(invocation -> {
            List<CoordinatorRecord> records = recordsCapture.getValue();
            records.add(groupMetadataTombstone);
            return null;
        }).when(groupMetadataManager).maybeDeleteGroup(eq("group-id"), recordsCapture.capture());

        assertFalse(timer.contains(GROUP_EXPIRATION_KEY));
        CoordinatorResult<Void, CoordinatorRecord> result = coordinator.cleanupGroupMetadata();
        assertTrue(timer.contains(GROUP_EXPIRATION_KEY));

        List<CoordinatorRecord> expectedRecords = Arrays.asList(offsetCommitTombstone, groupMetadataTombstone);
        assertEquals(expectedRecords, result.records());
        assertNull(result.response());
        assertNull(result.appendFuture());

        verify(groupMetadataManager, times(1)).groupIds();
        verify(offsetMetadataManager, times(1)).cleanupExpiredOffsets(eq("group-id"), any());
        verify(offsetMetadataManager, times(1)).cleanupExpiredOffsets(eq("other-group-id"), any());
        verify(groupMetadataManager, times(1)).maybeDeleteGroup(eq("group-id"), any());
        verify(groupMetadataManager, times(0)).maybeDeleteGroup(eq("other-group-id"), any());
    }

    @Test
    public void testScheduleGroupSizeCounter() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        MockTime time = new MockTime();
        MockCoordinatorTimer<Void, CoordinatorRecord> timer = new MockCoordinatorTimer<>(time);
        GroupCoordinatorConfig config = mock(GroupCoordinatorConfig.class);
        when(config.offsetsRetentionCheckIntervalMs()).thenReturn(60 * 60 * 1000L);

        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            timer,
            config,
            coordinatorMetrics,
            metricsShard
        );
        coordinator.onLoaded(MetadataImage.EMPTY);

        // The counter is scheduled.
        assertEquals(
            DEFAULT_GROUP_GAUGES_UPDATE_INTERVAL_MS,
            timer.timeout(GROUP_SIZE_COUNTER_KEY).deadlineMs - time.milliseconds()
        );

        // Advance the timer to trigger the update.
        time.sleep(DEFAULT_GROUP_GAUGES_UPDATE_INTERVAL_MS + 1);
        timer.poll();
        verify(groupMetadataManager, times(1)).updateGroupSizeCounter();

        // The counter is scheduled.
        assertEquals(
            DEFAULT_GROUP_GAUGES_UPDATE_INTERVAL_MS,
            timer.timeout(GROUP_SIZE_COUNTER_KEY).deadlineMs - time.milliseconds()
        );
    }

    @ParameterizedTest
    @EnumSource(value = TransactionResult.class)
    public void testReplayEndTransactionMarker(TransactionResult result) {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        coordinator.replayEndTransactionMarker(
            100L,
            (short) 5,
            result
        );

        verify(offsetMetadataManager, times(1)).replayEndTransactionMarker(
            100L,
            result
        );
    }

    @Test
    public void testOnPartitionsDeleted() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        List<CoordinatorRecord> records = Collections.singletonList(GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(
            "group",
            "foo",
            0
        ));

        when(offsetMetadataManager.onPartitionsDeleted(
            Collections.singletonList(new TopicPartition("foo", 0))
        )).thenReturn(records);

        CoordinatorResult<Void, CoordinatorRecord> result = coordinator.onPartitionsDeleted(
            Collections.singletonList(new TopicPartition("foo", 0))
        );

        assertEquals(records, result.records());
        assertNull(result.response());
    }

    @Test
    public void testOnUnloaded() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        Time mockTime = new MockTime();
        MockCoordinatorTimer<Void, CoordinatorRecord> timer = new MockCoordinatorTimer<>(mockTime);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            mockTime,
            timer,
            GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 1000L, 24 * 60),
            mock(CoordinatorMetrics.class),
            mock(CoordinatorMetricsShard.class)
        );

        coordinator.onUnloaded();
        assertEquals(0, timer.size());
        verify(groupMetadataManager, times(1)).onUnloaded();
    }

    @Test
    public void testShareGroupHeartbeat() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        RequestContext context = requestContext(ApiKeys.SHARE_GROUP_HEARTBEAT);
        ShareGroupHeartbeatRequestData request = new ShareGroupHeartbeatRequestData();
        CoordinatorResult<ShareGroupHeartbeatResponseData, CoordinatorRecord> result = new CoordinatorResult<>(
            Collections.emptyList(),
            new ShareGroupHeartbeatResponseData()
        );

        when(groupMetadataManager.shareGroupHeartbeat(
            context,
            request
        )).thenReturn(result);

        assertEquals(result, coordinator.shareGroupHeartbeat(context, request));
    }

    @Test
    public void testReplayShareGroupMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ShareGroupMetadataKey key = new ShareGroupMetadataKey();
        ShareGroupMetadataValue value = new ShareGroupMetadataValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayShareGroupMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ShareGroupMetadataKey key = new ShareGroupMetadataKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayShareGroupMemberMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ShareGroupMemberMetadataKey key = new ShareGroupMemberMetadataKey();
        ShareGroupMemberMetadataValue value = new ShareGroupMemberMetadataValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayShareGroupMemberMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ShareGroupMemberMetadataKey key = new ShareGroupMemberMetadataKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayConsumerGroupRegularExpression() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupRegularExpressionKey key = new ConsumerGroupRegularExpressionKey()
            .setGroupId("group")
            .setRegularExpression("ab*");

        ConsumerGroupRegularExpressionValue value = new ConsumerGroupRegularExpressionValue()
            .setTopics(Arrays.asList("abc", "abcd"))
            .setVersion(10L)
            .setTimestamp(12345L);

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.record(
            key,
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupRegularExpressionTombstone() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ConsumerGroupRegularExpressionKey key = new ConsumerGroupRegularExpressionKey()
            .setGroupId("group")
            .setRegularExpression("ab*");

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, CoordinatorRecord.tombstone(
            key
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testSharePartitionDeleteRequests() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ShareGroup shareGroup = new ShareGroup(new SnapshotRegistry(mock(LogContext.class)), "share-group");

        when(groupMetadataManager.shareGroup(eq("share-group"))).thenReturn(shareGroup);
        when(groupMetadataManager.shareGroup(eq("non-share-group"))).thenThrow(GroupIdNotFoundException.class);

        TopicData<PartitionIdData> topicData = new TopicData<>(Uuid.randomUuid(),
            List.of(
                PartitionFactory.newPartitionIdData(0),
                PartitionFactory.newPartitionIdData(1)
            ));

        DeleteShareGroupStateParameters params = new DeleteShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdData>()
                .setGroupId("share-group")
                .setTopicsData(List.of(topicData))
                .build())
            .build();

        when(groupMetadataManager.shareGroupBuildPartitionDeleteRequest(eq(shareGroup))).thenReturn(Optional.of(params));

        CoordinatorResult<Map<String, Map.Entry<DeleteShareGroupStateParameters, Errors>>, CoordinatorRecord> expectedResult =
            new CoordinatorResult<>(List.of(), Map.of("share-group", Map.entry(params, Errors.NONE)));


        assertEquals(expectedResult, coordinator.sharePartitionDeleteRequests(List.of("share-group", "non-share-group")));
        verify(groupMetadataManager, times(1)).shareGroup(eq("share-group"));
        verify(groupMetadataManager, times(1)).shareGroup(eq("non-share-group"));
        verify(groupMetadataManager, times(1)).shareGroupBuildPartitionDeleteRequest(eq(shareGroup));

        // empty list
        Mockito.reset(groupMetadataManager);
        expectedResult = new CoordinatorResult<>(List.of(), Map.of());
        assertEquals(
            expectedResult,
            coordinator.sharePartitionDeleteRequests(List.of())
        );

        verify(groupMetadataManager, times(0)).group(eq("share-group"));
        verify(groupMetadataManager, times(0)).group(eq("non-share-group"));
        verify(groupMetadataManager, times(0)).shareGroupBuildPartitionDeleteRequest(eq(shareGroup));
    }

    @Test
    public void testSharePartitionDeleteRequestsNonEmptyShareGroup() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        GroupCoordinatorShard coordinator = new GroupCoordinatorShard(
            new LogContext(),
            groupMetadataManager,
            offsetMetadataManager,
            Time.SYSTEM,
            new MockCoordinatorTimer<>(Time.SYSTEM),
            mock(GroupCoordinatorConfig.class),
            coordinatorMetrics,
            metricsShard
        );

        ShareGroup shareGroup = mock(ShareGroup.class);
        doThrow(new GroupNotEmptyException("bad stuff")).when(shareGroup).validateDeleteGroup();

        when(groupMetadataManager.shareGroup(eq("share-group"))).thenReturn(shareGroup);

        CoordinatorResult<Map<String, Map.Entry<DeleteShareGroupStateParameters, Errors>>, CoordinatorRecord> expectedResult =
            new CoordinatorResult<>(List.of(), Map.of("share-group", Map.entry(DeleteShareGroupStateParameters.EMPTY_PARAMS,
                Errors.forException(new GroupNotEmptyException("bad stuff")))
            ));
        assertEquals(expectedResult, coordinator.sharePartitionDeleteRequests(List.of("share-group")));
        verify(groupMetadataManager, times(1)).shareGroup(eq("share-group"));
        // Not called because of NON-EMPTY group.
        verify(groupMetadataManager, times(0)).shareGroupBuildPartitionDeleteRequest(eq(shareGroup));

        // empty list
        Mockito.reset(groupMetadataManager);
        expectedResult = new CoordinatorResult<>(List.of(), Map.of());
        assertEquals(
            expectedResult,
            coordinator.sharePartitionDeleteRequests(List.of())
        );

        verify(groupMetadataManager, times(0)).group(eq("share-group"));
        verify(groupMetadataManager, times(0)).shareGroupBuildPartitionDeleteRequest(eq(shareGroup));
    }
}
