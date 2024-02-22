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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
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
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.coordinator.group.GroupCoordinatorShard.GROUP_EXPIRATION_KEY;
import static org.apache.kafka.coordinator.group.TestUtil.requestContext;
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
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result = new CoordinatorResult<>(
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
        CoordinatorResult<OffsetCommitResponseData, Record> result = new CoordinatorResult<>(
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
        CoordinatorResult<TxnOffsetCommitResponseData, Record> result = new CoordinatorResult<>(
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
        List<Record> expectedRecords = new ArrayList<>();
        for (String groupId : groupIds) {
            expectedResultCollection.add(new DeleteGroupsResponseData.DeletableGroupResult().setGroupId(groupId));
            expectedRecords.addAll(Arrays.asList(
                RecordHelpers.newOffsetCommitTombstoneRecord(groupId, "topic-name", 0),
                RecordHelpers.newGroupMetadataTombstoneRecord(groupId)
            ));
        }

        CoordinatorResult<DeleteGroupsResponseData.DeletableGroupResultCollection, Record> expectedResult = new CoordinatorResult<>(
            expectedRecords,
            expectedResultCollection
        );

        when(offsetMetadataManager.deleteAllOffsets(anyString(), anyList())).thenAnswer(invocation -> {
            String groupId = invocation.getArgument(0);
            List<Record> records = invocation.getArgument(1);
            records.add(RecordHelpers.newOffsetCommitTombstoneRecord(groupId, "topic-name", 0));
            return 1;
        });
        // Mockito#when only stubs method returning non-void value, so we use Mockito#doAnswer instead.
        doAnswer(invocation -> {
            String groupId = invocation.getArgument(0);
            List<Record> records = invocation.getArgument(1);
            records.add(RecordHelpers.newGroupMetadataTombstoneRecord(groupId));
            return null;
        }).when(groupMetadataManager).deleteGroup(anyString(), anyList());

        CoordinatorResult<DeleteGroupsResponseData.DeletableGroupResultCollection, Record> coordinatorResult =
            coordinator.deleteGroups(context, groupIds);

        for (String groupId : groupIds) {
            verify(groupMetadataManager, times(1)).validateDeleteGroup(ArgumentMatchers.eq(groupId));
            verify(groupMetadataManager, times(1)).deleteGroup(ArgumentMatchers.eq(groupId), anyList());
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
        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newOffsetCommitTombstoneRecord("group-id-1", "topic-name", 0),
            RecordHelpers.newGroupMetadataTombstoneRecord("group-id-1"),
            RecordHelpers.newOffsetCommitTombstoneRecord("group-id-3", "topic-name", 0),
            RecordHelpers.newGroupMetadataTombstoneRecord("group-id-3")
        );
        CoordinatorResult<DeleteGroupsResponseData.DeletableGroupResultCollection, Record> expectedResult = new CoordinatorResult<>(
            expectedRecords,
            expectedResultCollection
        );

        // Mockito#when only stubs method returning non-void value, so we use Mockito#doAnswer and Mockito#doThrow instead.
        doThrow(Errors.INVALID_GROUP_ID.exception())
            .when(groupMetadataManager).validateDeleteGroup(ArgumentMatchers.eq("group-id-2"));
        doAnswer(invocation -> {
            String groupId = invocation.getArgument(0);
            List<Record> records = invocation.getArgument(1);
            records.add(RecordHelpers.newOffsetCommitTombstoneRecord(groupId, "topic-name", 0));
            return null;
        }).when(offsetMetadataManager).deleteAllOffsets(anyString(), anyList());
        doAnswer(invocation -> {
            String groupId = invocation.getArgument(0);
            List<Record> records = invocation.getArgument(1);
            records.add(RecordHelpers.newGroupMetadataTombstoneRecord(groupId));
            return null;
        }).when(groupMetadataManager).deleteGroup(anyString(), anyList());

        CoordinatorResult<DeleteGroupsResponseData.DeletableGroupResultCollection, Record> coordinatorResult =
            coordinator.deleteGroups(context, groupIds);

        for (String groupId : groupIds) {
            verify(groupMetadataManager, times(1)).validateDeleteGroup(eq(groupId));
            if (!groupId.equals("group-id-2")) {
                verify(groupMetadataManager, times(1)).deleteGroup(eq(groupId), anyList());
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

        OffsetCommitKey key = new OffsetCommitKey();
        OffsetCommitValue value = new OffsetCommitValue();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 0),
            new ApiMessageAndVersion(value, (short) 0)
        ));

        coordinator.replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 1),
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(offsetMetadataManager, times(1)).replay(
            0L,
            RecordBatch.NO_PRODUCER_ID,
            key,
            value
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

        OffsetCommitKey key = new OffsetCommitKey();
        OffsetCommitValue value = new OffsetCommitValue();

        coordinator.replay(0L, 100L, (short) 0, new Record(
            new ApiMessageAndVersion(key, (short) 0),
            new ApiMessageAndVersion(value, (short) 0)
        ));

        coordinator.replay(1L, 101L, (short) 1, new Record(
            new ApiMessageAndVersion(key, (short) 1),
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(offsetMetadataManager, times(1)).replay(
            0L,
            100L,
            key,
            value
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

        OffsetCommitKey key = new OffsetCommitKey();

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 0),
            null
        ));

        coordinator.replay(1L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 1),
            null
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 3),
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 3),
            null
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 4),
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 4),
            null
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 5),
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 5),
            null
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 6),
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 6),
            null
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 7),
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 7),
            null
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 8),
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 8),
            null
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
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
                new Record(null, null))
        );
    }

    @Test
    public void testReplayWithUnsupportedVersion() {
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

        assertThrows(IllegalStateException.class, () ->
            coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
                new ApiMessageAndVersion(key, (short) 255),
                new ApiMessageAndVersion(value, (short) 0)
            ))
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 2),
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

        coordinator.replay(0L, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, new Record(
            new ApiMessageAndVersion(key, (short) 2),
            null
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testScheduleCleanupGroupMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        OffsetMetadataManager offsetMetadataManager = mock(OffsetMetadataManager.class);
        Time mockTime = new MockTime();
        MockCoordinatorTimer<Void, Record> timer = new MockCoordinatorTimer<>(mockTime);
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
        List<MockCoordinatorTimer.ExpiredTimeout<Void, Record>> tasks = timer.poll();
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
        MockCoordinatorTimer<Void, Record> timer = new MockCoordinatorTimer<>(mockTime);
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

        Record offsetCommitTombstone = RecordHelpers.newOffsetCommitTombstoneRecord("group-id", "topic", 0);
        Record groupMetadataTombstone = RecordHelpers.newGroupMetadataTombstoneRecord("group-id");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Record>> recordsCapture = ArgumentCaptor.forClass(List.class);

        when(groupMetadataManager.groupIds()).thenReturn(mkSet("group-id", "other-group-id"));
        when(offsetMetadataManager.cleanupExpiredOffsets(eq("group-id"), recordsCapture.capture()))
            .thenAnswer(invocation -> {
                List<Record> records = recordsCapture.getValue();
                records.add(offsetCommitTombstone);
                return true;
            });
        when(offsetMetadataManager.cleanupExpiredOffsets("other-group-id", Collections.emptyList())).thenReturn(false);
        doAnswer(invocation -> {
            List<Record> records = recordsCapture.getValue();
            records.add(groupMetadataTombstone);
            return null;
        }).when(groupMetadataManager).maybeDeleteGroup(eq("group-id"), recordsCapture.capture());

        assertFalse(timer.contains(GROUP_EXPIRATION_KEY));
        CoordinatorResult<Void, Record> result = coordinator.cleanupGroupMetadata();
        assertTrue(timer.contains(GROUP_EXPIRATION_KEY));

        List<Record> expectedRecords = Arrays.asList(offsetCommitTombstone, groupMetadataTombstone);
        assertEquals(expectedRecords, result.records());
        assertNull(result.response());
        assertNull(result.appendFuture());

        verify(groupMetadataManager, times(1)).groupIds();
        verify(offsetMetadataManager, times(1)).cleanupExpiredOffsets(eq("group-id"), any());
        verify(offsetMetadataManager, times(1)).cleanupExpiredOffsets(eq("other-group-id"), any());
        verify(groupMetadataManager, times(1)).maybeDeleteGroup(eq("group-id"), any());
        verify(groupMetadataManager, times(0)).maybeDeleteGroup(eq("other-group-id"), any());
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

        List<Record> records = Collections.singletonList(RecordHelpers.newOffsetCommitTombstoneRecord(
            "group",
            "foo",
            0
        ));

        when(offsetMetadataManager.onPartitionsDeleted(
            Collections.singletonList(new TopicPartition("foo", 0))
        )).thenReturn(records);

        CoordinatorResult<Void, Record> result = coordinator.onPartitionsDeleted(
            Collections.singletonList(new TopicPartition("foo", 0))
        );

        assertEquals(records, result.records());
        assertNull(result.response());
    }
}
