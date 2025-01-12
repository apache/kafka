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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.config.ShareCoordinatorConfig;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PersisterStateBatch;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShareCoordinatorShardTest {

    private static final String GROUP_ID = "group1";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final int PARTITION = 0;

    public static class ShareCoordinatorShardBuilder {
        private final LogContext logContext = new LogContext();
        private ShareCoordinatorConfig config = null;
        private final CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        private final CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        private final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        private MetadataImage metadataImage = null;
        private Map<String, String> configOverrides = new HashMap<>();
        ShareCoordinatorOffsetsManager offsetsManager = mock(ShareCoordinatorOffsetsManager.class);

        ShareCoordinatorShard build() {
            if (metadataImage == null) metadataImage = mock(MetadataImage.class, RETURNS_DEEP_STUBS);
            if (config == null) {
                config = ShareCoordinatorTestConfig.createConfig(ShareCoordinatorTestConfig.testConfigMap(configOverrides));
            }

            ShareCoordinatorShard shard = new ShareCoordinatorShard(
                logContext,
                config,
                coordinatorMetrics,
                metricsShard,
                snapshotRegistry,
                offsetsManager
            );
            when(metadataImage.topics().getTopic((Uuid) any())).thenReturn(mock(TopicImage.class));
            when(metadataImage.topics().getPartition(any(), anyInt())).thenReturn(mock(PartitionRegistration.class));
            shard.onNewMetadataImage(metadataImage, null);
            return shard;
        }

        public ShareCoordinatorShardBuilder setConfigOverrides(Map<String, String> configOverrides) {
            this.configOverrides = configOverrides;
            return this;
        }

        public ShareCoordinatorShardBuilder setOffsetsManager(ShareCoordinatorOffsetsManager offsetsManager) {
            this.offsetsManager = offsetsManager;
            return this;
        }
    }

    private void writeAndReplayDefaultRecord(ShareCoordinatorShard shard) {
        writeAndReplayRecord(shard, 0);
    }

    private void writeAndReplayRecord(ShareCoordinatorShard shard, int leaderEpoch) {

        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(leaderEpoch)
                    .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));
    }

    @Test
    public void testReplayWithShareSnapshot() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        long offset = 0;
        long producerId = 0;
        short producerEpoch = 0;

        int leaderEpoch = 1;

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        CoordinatorRecord record1 = new CoordinatorRecord(
            new ApiMessageAndVersion(
                new ShareSnapshotKey()
                    .setGroupId(GROUP_ID)
                    .setTopicId(TOPIC_ID)
                    .setPartition(PARTITION),
                (short) 0
            ),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(leaderEpoch)
                    .setStateBatches(Collections.singletonList(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(10)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        CoordinatorRecord record2 = new CoordinatorRecord(
            new ApiMessageAndVersion(
                new ShareSnapshotKey()
                    .setGroupId(GROUP_ID)
                    .setTopicId(TOPIC_ID)
                    .setPartition(PARTITION),
                (short) 0
            ),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(1)
                    .setStateEpoch(1)
                    .setLeaderEpoch(leaderEpoch + 1)
                    .setStateBatches(Collections.singletonList(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(11)
                            .setLastOffset(12)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        // First replay should populate values in otherwise empty shareStateMap and leaderMap.
        shard.replay(offset, producerId, producerEpoch, record1);

        assertEquals(groupOffset(record1.value().message()),
            shard.getShareStateMapValue(shareCoordinatorKey));
        assertEquals(leaderEpoch, shard.getLeaderMapValue(shareCoordinatorKey));


        // Second replay should update the existing values in shareStateMap and leaderMap.
        shard.replay(offset + 1, producerId, producerEpoch, record2);

        assertEquals(groupOffset(record2.value().message()), shard.getShareStateMapValue(shareCoordinatorKey));
        assertEquals(leaderEpoch + 1, shard.getLeaderMapValue(shareCoordinatorKey));
    }

    @Test
    public void testWriteStateSuccess() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        List<CoordinatorRecord> expectedRecords = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request.topics().get(0).partitions().get(0))
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertEquals(groupOffset(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request.topics().get(0).partitions().get(0))
        ).value().message()), shard.getShareStateMapValue(shareCoordinatorKey));
        assertEquals(0, shard.getLeaderMapValue(shareCoordinatorKey));
        verify(shard.getMetricsShard()).record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
    }

    @Test
    public void testSubsequentWriteStateSnapshotEpochUpdatesSuccessfully() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        WriteShareGroupStateRequestData request1 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        WriteShareGroupStateRequestData request2 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(11)
                        .setLastOffset(20)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request1);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        List<CoordinatorRecord> expectedRecords = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request1.topics().get(0).partitions().get(0))
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertEquals(groupOffset(expectedRecords.get(0).value().message()),
            shard.getShareStateMapValue(shareCoordinatorKey));
        assertEquals(0, shard.getLeaderMapValue(shareCoordinatorKey));

        result = shard.writeState(request2);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        // The snapshot epoch here will be 1 since this is a snapshot update record,
        // and it refers to parent share snapshot.
        expectedRecords = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotUpdateRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request2.topics().get(0).partitions().get(0), 0)
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        ShareGroupOffset incrementalUpdate = groupOffset(expectedRecords.get(0).value().message());
        ShareGroupOffset combinedState = shard.getShareStateMapValue(shareCoordinatorKey);
        assertEquals(incrementalUpdate.snapshotEpoch(), combinedState.snapshotEpoch());
        assertEquals(incrementalUpdate.leaderEpoch(), combinedState.leaderEpoch());
        assertEquals(incrementalUpdate.startOffset(), combinedState.startOffset());
        // The batches should have combined to 1 since same state.
        assertEquals(Collections.singletonList(new PersisterStateBatch(0, 20, (byte) 0, (short) 1)),
            combinedState.stateBatches());
        assertEquals(0, shard.getLeaderMapValue(shareCoordinatorKey));
    }

    @Test
    public void testWriteStateInvalidRequestData() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        int partition = -1;

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(partition)
                    .setStartOffset(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request);

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, partition, Errors.INVALID_REQUEST, ShareCoordinatorShard.NEGATIVE_PARTITION_ID.getMessage());
        List<CoordinatorRecord> expectedRecords = Collections.emptyList();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertNull(shard.getShareStateMapValue(shareCoordinatorKey));
        assertNull(shard.getLeaderMapValue(shareCoordinatorKey));
    }

    @Test
    public void testWriteNullMetadataImage() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();
        shard.onNewMetadataImage(null, null);

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(0)
                    .setStartOffset(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request);

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
        List<CoordinatorRecord> expectedRecords = Collections.emptyList();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertNull(shard.getShareStateMapValue(shareCoordinatorKey));
        assertNull(shard.getLeaderMapValue(shareCoordinatorKey));
    }

    @Test
    public void testWriteStateFencedLeaderEpochError() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        WriteShareGroupStateRequestData request1 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(5)
                    .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        WriteShareGroupStateRequestData request2 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(3) // Lower leader epoch in the second request.
                    .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(11)
                        .setLastOffset(20)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request1);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        List<CoordinatorRecord> expectedRecords = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request1.topics().get(0).partitions().get(0))
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertEquals(groupOffset(expectedRecords.get(0).value().message()),
            shard.getShareStateMapValue(shareCoordinatorKey));
        assertEquals(5, shard.getLeaderMapValue(shareCoordinatorKey));

        result = shard.writeState(request2);

        // Since the leader epoch in the second request was lower than the one in the first request, FENCED_LEADER_EPOCH error is expected.
        expectedData = WriteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, PARTITION, Errors.FENCED_LEADER_EPOCH, Errors.FENCED_LEADER_EPOCH.message());
        expectedRecords = Collections.emptyList();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        // No changes to the leaderMap.
        assertEquals(5, shard.getLeaderMapValue(shareCoordinatorKey));
    }

    @Test
    public void testWriteStateFencedStateEpochError() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        WriteShareGroupStateRequestData request1 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setStateEpoch(1)
                    .setLeaderEpoch(5)
                    .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        WriteShareGroupStateRequestData request2 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setStateEpoch(0)   // Lower state epoch in the second request.
                    .setLeaderEpoch(5)
                    .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(11)
                        .setLastOffset(20)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request1);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        List<CoordinatorRecord> expectedRecords = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request1.topics().get(0).partitions().get(0))
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertEquals(groupOffset(expectedRecords.get(0).value().message()),
            shard.getShareStateMapValue(shareCoordinatorKey));
        assertEquals(5, shard.getLeaderMapValue(shareCoordinatorKey));

        result = shard.writeState(request2);

        // Since the leader epoch in the second request was lower than the one in the first request, FENCED_LEADER_EPOCH error is expected.
        expectedData = WriteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, PARTITION, Errors.FENCED_STATE_EPOCH, Errors.FENCED_STATE_EPOCH.message());
        expectedRecords = Collections.emptyList();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        // No changes to the stateEpochMap.
        assertEquals(1, shard.getStateEpochMapValue(shareCoordinatorKey));
    }

    @Test
    public void testReadStateSuccess() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        SharePartitionKey coordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        writeAndReplayDefaultRecord(shard);

        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(1)))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request);

        assertEquals(ReadShareGroupStateResponse.toResponseData(
            TOPIC_ID,
            PARTITION,
            0,
            0,
            Collections.singletonList(new ReadShareGroupStateResponseData.StateBatch()
                .setFirstOffset(0)
                .setLastOffset(10)
                .setDeliveryCount((short) 1)
                .setDeliveryState((byte) 0)
            )
        ), result.response());

        assertEquals(0, shard.getLeaderMapValue(coordinatorKey));
    }

    @Test
    public void testReadStateInvalidRequestData() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        int partition = -1;

        writeAndReplayDefaultRecord(shard);

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(partition)
                    .setLeaderEpoch(5)))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request);

        ReadShareGroupStateResponseData expectedData = ReadShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, partition, Errors.INVALID_REQUEST, ShareCoordinatorShard.NEGATIVE_PARTITION_ID.getMessage());

        assertEquals(expectedData, result.response());

        // Leader epoch should not be changed because the request failed.
        assertEquals(0, shard.getLeaderMapValue(shareCoordinatorKey));
    }

    @Test
    public void testReadNullMetadataImage() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        writeAndReplayDefaultRecord(shard);

        shard.onNewMetadataImage(null, null);

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(0)
                    .setLeaderEpoch(5)))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request);

        ReadShareGroupStateResponseData expectedData = ReadShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());

        assertEquals(expectedData, result.response());

        // Leader epoch should not be changed because the request failed.
        assertEquals(0, shard.getLeaderMapValue(shareCoordinatorKey));
    }

    @Test
    public void testReadStateFencedLeaderEpochError() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        int leaderEpoch = 5;

        writeAndReplayRecord(shard, leaderEpoch); // leaderEpoch in the leaderMap will be 5.

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(3))))); // Lower leaderEpoch than the one stored in leaderMap.

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request);

        ReadShareGroupStateResponseData expectedData = ReadShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID,
            PARTITION,
            Errors.FENCED_LEADER_EPOCH,
            Errors.FENCED_LEADER_EPOCH.message());

        assertEquals(expectedData, result.response());

        assertEquals(leaderEpoch, shard.getLeaderMapValue(shareCoordinatorKey));
    }

    @Test
    public void testNonSequentialBatchUpdates() {
        //        startOffset: 100
        //        Batch1 {
        //            firstOffset: 100
        //            lastOffset: 109
        //            deliverState: Acquired
        //            deliverCount: 1
        //        }
        //        Batch2 {
        //            firstOffset: 110
        //            lastOffset: 119
        //            deliverState: Acquired
        //            deliverCount: 2
        //        }
        //        Batch3 {
        //            firstOffset: 120
        //            lastOffset: 129
        //            deliverState: Acquired
        //            deliverCount: 0
        //        }
        //
        //        -Share leader acks batch 1 and sends the state of batch 1 to Share Coordinator.
        //        -Share leader advances startOffset to 110.
        //        -Share leader acks batch 3 and sends the new startOffset and the state of batch 3 to share coordinator.
        //        -Share coordinator writes the snapshot with startOffset 110 and batch 3.
        //        -batch2 should NOT be lost
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder()
            .setConfigOverrides(Collections.singletonMap(ShareCoordinatorConfig.SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT_CONFIG, "0"))
            .build();

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        // Set initial state.
        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(100)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(Arrays.asList(
                        new WriteShareGroupStateRequestData.StateBatch()    //b1
                            .setFirstOffset(100)
                            .setLastOffset(109)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 1),   //acquired
                        new WriteShareGroupStateRequestData.StateBatch()    //b2
                            .setFirstOffset(110)
                            .setLastOffset(119)
                            .setDeliveryCount((short) 2)
                            .setDeliveryState((byte) 1),   //acquired
                        new WriteShareGroupStateRequestData.StateBatch()    //b3
                            .setFirstOffset(120)
                            .setLastOffset(129)
                            .setDeliveryCount((short) 0)
                            .setDeliveryState((byte) 1)))   //acquired
                ))
            ));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        List<CoordinatorRecord> expectedRecords = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request.topics().get(0).partitions().get(0))
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertEquals(groupOffset(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request.topics().get(0).partitions().get(0))
        ).value().message()), shard.getShareStateMapValue(shareCoordinatorKey));
        assertEquals(0, shard.getLeaderMapValue(shareCoordinatorKey));
        verify(shard.getMetricsShard()).record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);

        // Acknowledge b1.
        WriteShareGroupStateRequestData requestUpdateB1 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(-1)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(Collections.singletonList(
                        new WriteShareGroupStateRequestData.StateBatch()    //b1
                            .setFirstOffset(100)
                            .setLastOffset(109)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 2)))   // Acked
                ))
            ));

        result = shard.writeState(requestUpdateB1);
        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        // Ack batch 3 and move start offset.
        WriteShareGroupStateRequestData requestUpdateStartOffsetAndB3 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(110)    // 100 -> 110
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(Collections.singletonList(
                        new WriteShareGroupStateRequestData.StateBatch()    //b3
                            .setFirstOffset(120)
                            .setLastOffset(129)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 2)))   //Acked
                ))
            ));

        result = shard.writeState(requestUpdateStartOffsetAndB3);
        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        WriteShareGroupStateResponseData expectedDataFinal = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        ShareGroupOffset offsetFinal = new ShareGroupOffset.Builder()
            .setStartOffset(110)
            .setLeaderEpoch(0)
            .setStateEpoch(0)
            .setSnapshotEpoch(2)    // since 2nd share snapshot
            .setStateBatches(Arrays.asList(
                new PersisterStateBatch(110, 119, (byte) 1, (short) 2),  // b2 not lost
                new PersisterStateBatch(120, 129, (byte) 2, (short) 1)
            ))
            .build();
        List<CoordinatorRecord> expectedRecordsFinal = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, offsetFinal
        ));

        assertEquals(expectedDataFinal, result.response());
        assertEquals(expectedRecordsFinal, result.records());

        assertEquals(groupOffset(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, offsetFinal
        ).value().message()), shard.getShareStateMapValue(shareCoordinatorKey));
        assertEquals(0, shard.getLeaderMapValue(shareCoordinatorKey));
        verify(shard.getMetricsShard(), times(3)).record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
    }

    @Test
    public void testLastRedundantOffset() {
        ShareCoordinatorOffsetsManager manager = mock(ShareCoordinatorOffsetsManager.class);
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder()
            .setOffsetsManager(manager)
            .build();

        when(manager.lastRedundantOffset()).thenReturn(Optional.of(10L));
        assertEquals(new CoordinatorResult<>(Collections.emptyList(), Optional.of(10L)), shard.lastRedundantOffset());
    }

    @Test
    public void testReadStateLeaderEpochUpdateSuccess() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(2)
                ))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        ReadShareGroupStateResponseData expectedData = ReadShareGroupStateResponse.toResponseData(
            TOPIC_ID, PARTITION,
            PartitionFactory.UNINITIALIZED_START_OFFSET,
            PartitionFactory.DEFAULT_STATE_EPOCH,
            Collections.emptyList());
        List<CoordinatorRecord> expectedRecords = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, new ShareGroupOffset.Builder()
                .setStartOffset(PartitionFactory.UNINITIALIZED_START_OFFSET)
                .setLeaderEpoch(2)
                .setStateBatches(Collections.emptyList())
                .setSnapshotEpoch(0)
                .setStateEpoch(PartitionFactory.DEFAULT_STATE_EPOCH)
                .build()
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertEquals(groupOffset(expectedRecords.get(0).value().message()), shard.getShareStateMapValue(shareCoordinatorKey));
        assertEquals(2, shard.getLeaderMapValue(shareCoordinatorKey));
        verify(shard.getMetricsShard()).record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
    }

    @Test
    public void testReadStateLeaderEpochUpdateNoUpdate() {
        ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

        ReadShareGroupStateRequestData request1 = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(2)
                ))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request1);
        assertFalse(result.records().isEmpty());    // Record generated.

        // Apply record to update soft state.
        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        ReadShareGroupStateRequestData request2 = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(-1)
                ))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result2 = shard.readStateAndMaybeUpdateLeaderEpoch(request2);

        assertTrue(result2.records().isEmpty());    // Leader epoch -1 - no update.

        ReadShareGroupStateRequestData request3 = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(-1)
                ))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result3 = shard.readStateAndMaybeUpdateLeaderEpoch(request3);

        assertTrue(result3.records().isEmpty());    // Same leader epoch - no update.
        verify(shard.getMetricsShard()).record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
    }

    private static ShareGroupOffset groupOffset(ApiMessage record) {
        if (record instanceof ShareSnapshotValue) {
            return ShareGroupOffset.fromRecord((ShareSnapshotValue) record);
        }
        return ShareGroupOffset.fromRecord((ShareUpdateValue) record);
    }
}
