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
import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
import org.apache.kafka.common.message.DeleteShareGroupStateResponseData;
import org.apache.kafka.common.message.InitializeShareGroupStateRequestData;
import org.apache.kafka.common.message.InitializeShareGroupStateResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteShareGroupStateResponse;
import org.apache.kafka.common.requests.InitializeShareGroupStateResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateSummaryResponse;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PersisterStateBatch;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("ClassDataAbstractionCoupling")
class ShareCoordinatorShardTest {

    private static final String GROUP_ID = "group1";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final Uuid TOPIC_ID_2 = Uuid.randomUuid();
    private static final int PARTITION = 0;
    private static final SharePartitionKey SHARE_PARTITION_KEY = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);
    private static final Time TIME = new MockTime();

    public static class ShareCoordinatorShardBuilder {
        private final LogContext logContext = new LogContext();
        private ShareCoordinatorConfig config = null;
        private final CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
        private final CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
        private final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        private CoordinatorMetadataImage metadataImage = null;
        private Map<String, String> configOverrides = new HashMap<>();
        ShareCoordinatorOffsetsManager offsetsManager = mock(ShareCoordinatorOffsetsManager.class);
        private Time time;

        ShareCoordinatorShard build() {
            if (metadataImage == null) metadataImage = mock(CoordinatorMetadataImage.class, RETURNS_DEEP_STUBS);
            if (config == null) {
                config = ShareCoordinatorTestConfig.createConfig(ShareCoordinatorTestConfig.testConfigMap(configOverrides));
            }

            ShareCoordinatorShard shard = new ShareCoordinatorShard(
                logContext,
                config,
                coordinatorMetrics,
                metricsShard,
                snapshotRegistry,
                offsetsManager,
                time == null ? TIME : time
            );

            var topicMetadata = mock(CoordinatorMetadataImage.TopicMetadata.class);
            when(topicMetadata.partitionCount()).thenReturn(PARTITION + 1);
            when(metadataImage.topicMetadata((Uuid) any())).thenReturn(Optional.of(topicMetadata));
            shard.onMetadataUpdate(null, metadataImage);
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

        public ShareCoordinatorShardBuilder setTime(Time time) {
            this.time = time;
            return this;
        }
    }

    private void writeAndReplayDefaultRecord(ShareCoordinatorShard shard) {
        writeAndReplayRecord(shard, 0);
    }

    private void writeAndReplayRecord(ShareCoordinatorShard shard, int leaderEpoch) {
        // Read is necessary before write!
        ReadShareGroupStateRequestData readRequest = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(leaderEpoch)
                ))
            ));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(readRequest);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(leaderEpoch)
                    .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result2 = shard.writeState(request);

        shard.replay(0L, 0L, (short) 0, result2.records().get(0));
    }

    private ShareCoordinatorShard shard;

    @BeforeEach
    public void setUp() {
        shard = new ShareCoordinatorShardBuilder().build();
    }

    @Test
    public void testReplayWithShareSnapshot() {
        long offset = 0;
        long producerId = 0;
        short producerEpoch = 0;

        int leaderEpoch = 1;

        SharePartitionKey shareCoordinatorKey = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, PARTITION);

        CoordinatorRecord record1 = CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(PARTITION),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(0)
                    .setStateEpoch(0)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setLeaderEpoch(leaderEpoch)
                    .setStateBatches(List.of(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(10)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        CoordinatorRecord record2 = CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(PARTITION),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(1)
                    .setStateEpoch(1)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(13)
                    .setLeaderEpoch(leaderEpoch + 1)
                    .setStateBatches(List.of(
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
    public void testWriteFailsOnUninitializedPartition() {
        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request);

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, PARTITION,
            Errors.INVALID_REQUEST,
            ShareCoordinatorShard.WRITE_UNINITIALIZED_SHARE_PARTITION.getMessage()
        );
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
        assertNull(shard.getShareStateMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testWriteStateSuccess() {
        initSharePartition(shard, SHARE_PARTITION_KEY);
        writeAndReplayRecord(shard, 0);

        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        List<CoordinatorRecord> expectedRecords = List.of(ShareCoordinatorRecordHelpers.newShareUpdateRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request.topics().get(0).partitions().get(0), TIME.milliseconds())
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertEquals(groupOffset(ShareCoordinatorRecordHelpers.newShareUpdateRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request.topics().get(0).partitions().get(0), TIME.milliseconds())
        ).value().message()), shard.getShareStateMapValue(SHARE_PARTITION_KEY));
        assertEquals(0, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
        verify(shard.getMetricsShard(), times(3)).record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
    }

    @Test
    public void testSubsequentWriteStateSnapshotEpochUpdatesSuccessfully() {
        initSharePartition(shard, SHARE_PARTITION_KEY);
        writeAndReplayRecord(shard, 0);

        WriteShareGroupStateRequestData request1 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        WriteShareGroupStateRequestData request2 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(21)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(11)
                        .setLastOffset(20)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request1);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        List<CoordinatorRecord> expectedRecords = List.of(ShareCoordinatorRecordHelpers.newShareUpdateRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request1.topics().get(0).partitions().get(0), TIME.milliseconds())
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertEquals(groupOffset(expectedRecords.get(0).value().message()),
            shard.getShareStateMapValue(SHARE_PARTITION_KEY));
        assertEquals(0, shard.getLeaderMapValue(SHARE_PARTITION_KEY));

        result = shard.writeState(request2);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        // The snapshot epoch here will be 1 since this is a snapshot update record,
        // and it refers to parent share snapshot.
        expectedRecords = List.of(ShareCoordinatorRecordHelpers.newShareUpdateRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request2.topics().get(0).partitions().get(0), TIME.milliseconds())
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        ShareGroupOffset incrementalUpdate = groupOffset(expectedRecords.get(0).value().message());
        ShareGroupOffset combinedState = shard.getShareStateMapValue(SHARE_PARTITION_KEY);
        assertEquals(incrementalUpdate.snapshotEpoch(), combinedState.snapshotEpoch());
        assertEquals(incrementalUpdate.leaderEpoch(), combinedState.leaderEpoch());
        assertEquals(incrementalUpdate.startOffset(), combinedState.startOffset());
        // The batches should have combined to 1 since same state.
        assertEquals(List.of(new PersisterStateBatch(0, 20, (byte) 0, (short) 1)),
            combinedState.stateBatches());
        assertEquals(0, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testWriteStateInvalidRequestData() {
        int partition = -1;

        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(partition)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request);

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, partition, Errors.INVALID_REQUEST, ShareCoordinatorShard.NEGATIVE_PARTITION_ID.getMessage());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertNull(shard.getShareStateMapValue(SHARE_PARTITION_KEY));
        assertNull(shard.getLeaderMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testWriteNullMetadataImage() {
        initSharePartition(shard, SHARE_PARTITION_KEY);

        shard.onMetadataUpdate(null, null);

        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(0)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request);

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
        assertEquals(-1, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testWriteStateFencedLeaderEpochError() {
        initSharePartition(shard, SHARE_PARTITION_KEY);
        writeAndReplayRecord(shard, 1);

        WriteShareGroupStateRequestData request1 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request1);

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, PARTITION, Errors.FENCED_LEADER_EPOCH, Errors.FENCED_LEADER_EPOCH.message());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
        assertEquals(1, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testWriteStateFencedStateEpochError() {
        initSharePartition(shard, SHARE_PARTITION_KEY, 1);

        WriteShareGroupStateRequestData request1 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(10)
                    .setStateEpoch(0)   // Lower state epoch in the second request.
                    .setLeaderEpoch(5)
                    .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                        .setFirstOffset(11)
                        .setLastOffset(20)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))))));

        CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> result = shard.writeState(request1);

        WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, PARTITION, Errors.FENCED_STATE_EPOCH, Errors.FENCED_STATE_EPOCH.message());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
        assertNotEquals(5, shard.getLeaderMapValue(SHARE_PARTITION_KEY));

        // No changes to the stateEpochMap.
        assertEquals(1, shard.getStateEpochMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testReadFailsOnUninitializedPartition() {
        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(1)))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request);

        assertEquals(ReadShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID,
            PARTITION,
            Errors.INVALID_REQUEST,
            ShareCoordinatorShard.READ_UNINITIALIZED_SHARE_PARTITION.getMessage()
        ), result.response());

        assertNull(shard.getLeaderMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testReadStateSuccess() {
        initSharePartition(shard, SHARE_PARTITION_KEY);
        writeAndReplayDefaultRecord(shard);

        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(1)))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request);

        assertEquals(ReadShareGroupStateResponse.toResponseData(
            TOPIC_ID,
            PARTITION,
            0,
            0,
            List.of(new ReadShareGroupStateResponseData.StateBatch()
                .setFirstOffset(0)
                .setLastOffset(10)
                .setDeliveryCount((short) 1)
                .setDeliveryState((byte) 0)
            )
        ), result.response());

        assertEquals(0, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testReadStateSummarySuccess() {
        initSharePartition(shard, SHARE_PARTITION_KEY);
        writeAndReplayDefaultRecord(shard);

        ReadShareGroupStateSummaryRequestData request = new ReadShareGroupStateSummaryRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(1)))));

        CoordinatorResult<ReadShareGroupStateSummaryResponseData, CoordinatorRecord> result = shard.readStateSummary(request);

        assertEquals(ReadShareGroupStateSummaryResponse.toResponseData(
            TOPIC_ID,
            PARTITION,
            0,
            0,
            0,
            0
        ), result.response());

        assertEquals(0, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testReadStateInvalidRequestData() {
        initSharePartition(shard, SHARE_PARTITION_KEY);
        writeAndReplayDefaultRecord(shard);

        int partition = -1;
        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(partition)
                    .setLeaderEpoch(5)))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request);

        ReadShareGroupStateResponseData expectedData = ReadShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, partition, Errors.INVALID_REQUEST, ShareCoordinatorShard.NEGATIVE_PARTITION_ID.getMessage());

        assertEquals(expectedData, result.response());

        // Leader epoch should not be changed because the request failed.
        assertEquals(0, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testReadStateSummaryInvalidRequestData() {
        initSharePartition(shard, SHARE_PARTITION_KEY);
        writeAndReplayDefaultRecord(shard);

        int partition = -1;
        ReadShareGroupStateSummaryRequestData request = new ReadShareGroupStateSummaryRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()
                    .setPartition(partition)
                    .setLeaderEpoch(5)))));

        CoordinatorResult<ReadShareGroupStateSummaryResponseData, CoordinatorRecord> result = shard.readStateSummary(request);

        ReadShareGroupStateSummaryResponseData expectedData = ReadShareGroupStateSummaryResponse.toErrorResponseData(
            TOPIC_ID, partition, Errors.INVALID_REQUEST, ShareCoordinatorShard.NEGATIVE_PARTITION_ID.getMessage());

        assertEquals(expectedData, result.response());

        // Leader epoch should not be changed because the request failed.
        assertEquals(0, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testReadNullMetadataImage() {
        initSharePartition(shard, SHARE_PARTITION_KEY);
        writeAndReplayDefaultRecord(shard);

        shard.onMetadataUpdate(null, null);

        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(0)
                    .setLeaderEpoch(5)))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request);

        ReadShareGroupStateResponseData expectedData = ReadShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());

        assertEquals(expectedData, result.response());

        // Leader epoch should not be changed because the request failed.
        assertEquals(0, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testReadStateFencedLeaderEpochError() {
        initSharePartition(shard, SHARE_PARTITION_KEY);

        int leaderEpoch = 5;
        writeAndReplayRecord(shard, leaderEpoch); // leaderEpoch in the leaderMap will be 5.

        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(3))))); // Lower leaderEpoch than the one stored in leaderMap.

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request);

        ReadShareGroupStateResponseData expectedData = ReadShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID,
            PARTITION,
            Errors.FENCED_LEADER_EPOCH,
            Errors.FENCED_LEADER_EPOCH.message());

        assertEquals(expectedData, result.response());

        assertEquals(leaderEpoch, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
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
        shard = new ShareCoordinatorShardBuilder()
            .setConfigOverrides(Map.of(ShareCoordinatorConfig.SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT_CONFIG, "0"))
            .build();

        initSharePartition(shard, SHARE_PARTITION_KEY);
        writeAndReplayRecord(shard, 0);

        // Set initial state.
        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(100)
                    .setDeliveryCompleteCount(0)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(List.of(
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
        List<CoordinatorRecord> expectedRecords = List.of(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request.topics().get(0).partitions().get(0), 3, TIME.milliseconds())
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertEquals(groupOffset(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request.topics().get(0).partitions().get(0), 3, TIME.milliseconds())
        ).value().message()), shard.getShareStateMapValue(SHARE_PARTITION_KEY));
        assertEquals(0, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
        verify(shard.getMetricsShard(), times(3)).record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);

        // Acknowledge b1.
        WriteShareGroupStateRequestData requestUpdateB1 = new WriteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(-1)
                    .setDeliveryCompleteCount(10)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(List.of(
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
            .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(110)    // 100 -> 110
                    .setDeliveryCompleteCount(10)
                    .setStateEpoch(0)
                    .setLeaderEpoch(0)
                    .setStateBatches(List.of(
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
            .setDeliveryCompleteCount(10)
            .setLeaderEpoch(0)
            .setStateEpoch(0)
            .setSnapshotEpoch(5)    // since subsequent share snapshot
            .setStateBatches(List.of(
                new PersisterStateBatch(110, 119, (byte) 1, (short) 2),  // b2 not lost
                new PersisterStateBatch(120, 129, (byte) 2, (short) 1)
            ))
            .setCreateTimestamp(TIME.milliseconds())
            .setWriteTimestamp(TIME.milliseconds())
            .build();
        List<CoordinatorRecord> expectedRecordsFinal = List.of(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, offsetFinal
        ));

        assertEquals(expectedDataFinal, result.response());
        assertEquals(expectedRecordsFinal, result.records());

        assertEquals(groupOffset(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, PARTITION, offsetFinal
        ).value().message()), shard.getShareStateMapValue(SHARE_PARTITION_KEY));
        assertEquals(0, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
        verify(shard.getMetricsShard(), times(5)).record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
    }

    @Test
    public void testLastRedundantOffset() {
        ShareCoordinatorOffsetsManager manager = mock(ShareCoordinatorOffsetsManager.class);
        shard = new ShareCoordinatorShardBuilder()
            .setOffsetsManager(manager)
            .build();

        when(manager.lastRedundantOffset()).thenReturn(Optional.of(10L));
        assertEquals(new CoordinatorResult<>(List.of(), Optional.of(10L)), shard.lastRedundantOffset());
    }

    @Test
    public void testReadStateLeaderEpochUpdateSuccess() {
        initSharePartition(shard, SHARE_PARTITION_KEY);

        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(2)
                ))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request);

        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        ReadShareGroupStateResponseData expectedData = ReadShareGroupStateResponse.toResponseData(
            TOPIC_ID, PARTITION,
            PartitionFactory.UNINITIALIZED_START_OFFSET,
            PartitionFactory.DEFAULT_STATE_EPOCH,
            List.of());
        List<CoordinatorRecord> expectedRecords = List.of(ShareCoordinatorRecordHelpers.newShareUpdateRecord(
            GROUP_ID, TOPIC_ID, PARTITION, new ShareGroupOffset.Builder()
                .setStartOffset(PartitionFactory.UNINITIALIZED_START_OFFSET)
                .setDeliveryCompleteCount(PartitionFactory.UNINITIALIZED_DELIVERY_COMPLETE_COUNT)
                .setLeaderEpoch(2)
                .setStateBatches(List.of())
                .setSnapshotEpoch(0)
                .setStateEpoch(PartitionFactory.DEFAULT_STATE_EPOCH)
                .setCreateTimestamp(TIME.milliseconds())
                .setWriteTimestamp(TIME.milliseconds())
                .build()
        ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertEquals(groupOffset(expectedRecords.get(0).value().message()), shard.getShareStateMapValue(SHARE_PARTITION_KEY));
        assertEquals(2, shard.getLeaderMapValue(SHARE_PARTITION_KEY));
        verify(shard.getMetricsShard()).record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
    }

    @Test
    public void testReadStateLeaderEpochUpdateNoUpdate() {
        initSharePartition(shard, SHARE_PARTITION_KEY);

        ReadShareGroupStateRequestData request1 = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(2)
                ))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result = shard.readStateAndMaybeUpdateLeaderEpoch(request1);
        assertFalse(result.records().isEmpty());    // Record generated.

        // Apply record to update soft state.
        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        ReadShareGroupStateRequestData request2 = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(-1)
                ))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result2 = shard.readStateAndMaybeUpdateLeaderEpoch(request2);

        assertTrue(result2.records().isEmpty());    // Leader epoch -1 - no update.

        ReadShareGroupStateRequestData request3 = new ReadShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setLeaderEpoch(-1)
                ))));

        CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord> result3 = shard.readStateAndMaybeUpdateLeaderEpoch(request3);

        assertTrue(result3.records().isEmpty());    // Same leader epoch - no update.
        verify(shard.getMetricsShard()).record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
    }

    @Test
    public void testDeleteStateSuccess() {
        DeleteShareGroupStateRequestData request = new DeleteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new DeleteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)))));

        // Apply a record to the state machine so that delete can be verified.
        CoordinatorRecord record = ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID,
            TOPIC_ID,
            PARTITION,
            new ShareGroupOffset.Builder()
                .setSnapshotEpoch(0)
                .setStateEpoch(0)
                .setLeaderEpoch(0)
                .setStateBatches(List.of(
                        new PersisterStateBatch(
                            0,
                            10,
                            (byte) 0,
                            (short) 1
                        )
                    )
                )
                .build()
        );
        shard.replay(0L, 0L, (short) 0, record);
        assertNotNull(shard.getShareStateMapValue(SHARE_PARTITION_KEY));
        assertNotNull(shard.getLeaderMapValue(SHARE_PARTITION_KEY));
        assertNotNull(shard.getStateEpochMapValue(SHARE_PARTITION_KEY));

        CoordinatorResult<DeleteShareGroupStateResponseData, CoordinatorRecord> result = shard.deleteState(request);

        // Apply tombstone.
        shard.replay(0L, 0L, (short) 0, result.records().get(0));

        DeleteShareGroupStateResponseData expectedData = DeleteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        List<CoordinatorRecord> expectedRecords = List.of(
            ShareCoordinatorRecordHelpers.newShareStateTombstoneRecord(
                GROUP_ID, TOPIC_ID, PARTITION)
        );

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertNull(shard.getShareStateMapValue(SHARE_PARTITION_KEY));
        assertNull(shard.getLeaderMapValue(SHARE_PARTITION_KEY));
        assertNull(shard.getStateEpochMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testDeleteStateUninitializedRecord() {
        DeleteShareGroupStateRequestData request = new DeleteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new DeleteShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)))));

        CoordinatorResult<DeleteShareGroupStateResponseData, CoordinatorRecord> result = shard.deleteState(request);

        assertNull(shard.getShareStateMapValue(SHARE_PARTITION_KEY));
        assertNull(shard.getLeaderMapValue(SHARE_PARTITION_KEY));
        assertNull(shard.getStateEpochMapValue(SHARE_PARTITION_KEY));

        DeleteShareGroupStateResponseData expectedData = DeleteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);

        assertEquals(expectedData, result.response());
        assertEquals(List.of(), result.records());
    }

    @Test
    public void testDeleteStateInvalidRequestData() {
        // invalid partition
        int partition = -1;

        DeleteShareGroupStateRequestData request = new DeleteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new DeleteShareGroupStateRequestData.PartitionData()
                    .setPartition(partition)))));

        CoordinatorResult<DeleteShareGroupStateResponseData, CoordinatorRecord> result = shard.deleteState(request);

        DeleteShareGroupStateResponseData expectedData = DeleteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, partition, Errors.INVALID_REQUEST, ShareCoordinatorShard.NEGATIVE_PARTITION_ID.getMessage());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
        assertEquals(expectedRecords, result.records());
    }

    @Test
    public void testDeleteNullMetadataImage() {
        shard.onMetadataUpdate(null, null);

        DeleteShareGroupStateRequestData request = new DeleteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new DeleteShareGroupStateRequestData.PartitionData()
                    .setPartition(0)))));

        CoordinatorResult<DeleteShareGroupStateResponseData, CoordinatorRecord> result = shard.deleteState(request);

        DeleteShareGroupStateResponseData expectedData = DeleteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
    }

    @Test
    public void testDeleteTopicIdNonExistentInMetadataImage() {
        MetadataImage image = mock(MetadataImage.class);
        shard.onMetadataUpdate(null, new KRaftCoordinatorMetadataImage(image));

        DeleteShareGroupStateRequestData request = new DeleteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new DeleteShareGroupStateRequestData.PartitionData()
                    .setPartition(0)))));

        // topic id not found in cache
        TopicsImage topicsImage = mock(TopicsImage.class);
        when(topicsImage.getTopic(eq(TOPIC_ID))).thenReturn(
            null
        );
        when(image.topics()).thenReturn(
            topicsImage
        );
        CoordinatorResult<DeleteShareGroupStateResponseData, CoordinatorRecord> result = shard.deleteState(request);

        DeleteShareGroupStateResponseData expectedData = DeleteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
        verify(topicsImage, times(1)).getTopic(eq(TOPIC_ID));
    }

    @Test
    public void testDeletePartitionIdNonExistentInMetadataImage() {
        MetadataImage image = mock(MetadataImage.class);
        when(image.cluster()).thenReturn(mock(ClusterImage.class));
        shard.onMetadataUpdate(null, new KRaftCoordinatorMetadataImage(image));

        DeleteShareGroupStateRequestData request = new DeleteShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new DeleteShareGroupStateRequestData.PartitionData()
                    .setPartition(0)))));

        // topic id found in cache
        TopicsImage topicsImage = mock(TopicsImage.class);
        TopicImage topicImage = mock(TopicImage.class);
        when(topicImage.partitions()).thenReturn(Map.of());
        when(topicsImage.getTopic(eq(TOPIC_ID))).thenReturn(topicImage);
        when(image.topics()).thenReturn(topicsImage);

        // partition id not found
        when(topicsImage.getPartition(eq(TOPIC_ID), eq(0))).thenReturn(
            null
        );
        CoordinatorResult<DeleteShareGroupStateResponseData, CoordinatorRecord> result = shard.deleteState(request);

        DeleteShareGroupStateResponseData expectedData = DeleteShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
        verify(topicsImage, times(1)).getTopic(eq(TOPIC_ID));
        verify(topicImage, times(1)).partitions();
    }

    @Test
    public void testInitializeStateSuccess() {
        InitializeShareGroupStateRequestData request = new InitializeShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new InitializeShareGroupStateRequestData.InitializeStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new InitializeShareGroupStateRequestData.PartitionData()
                    .setPartition(PARTITION)
                    .setStartOffset(10)
                    .setStateEpoch(5)))
            ));

        assertNull(shard.getShareStateMapValue(SHARE_PARTITION_KEY));
        assertNull(shard.getStateEpochMapValue(SHARE_PARTITION_KEY));

        CoordinatorResult<InitializeShareGroupStateResponseData, CoordinatorRecord> result = shard.initializeState(request);
        result.records().forEach(record -> shard.replay(0L, 0L, (short) 0, record));

        InitializeShareGroupStateResponseData expectedData = InitializeShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
        List<CoordinatorRecord> expectedRecords = List.of(
            ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
                GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request.topics().get(0).partitions().get(0), TIME.milliseconds())
            ));

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        assertNotNull(shard.getShareStateMapValue(SHARE_PARTITION_KEY));
        assertNotNull(shard.getStateEpochMapValue(SHARE_PARTITION_KEY));
    }

    @Test
    public void testInitializeStateInvalidRequestData() {
        // invalid partition
        int partition = -1;

        InitializeShareGroupStateRequestData request = new InitializeShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new InitializeShareGroupStateRequestData.InitializeStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new InitializeShareGroupStateRequestData.PartitionData()
                    .setPartition(partition)
                ))
            ));

        CoordinatorResult<InitializeShareGroupStateResponseData, CoordinatorRecord> result = shard.initializeState(request);

        InitializeShareGroupStateResponseData expectedData = InitializeShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, partition, Errors.INVALID_REQUEST, ShareCoordinatorShard.NEGATIVE_PARTITION_ID.getMessage());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());

        // invalid state epoch
        partition = 0;
        shard.replay(0L, 0L, (short) 0, ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
            GROUP_ID, TOPIC_ID, partition, new ShareGroupOffset.Builder()
                .setStateEpoch(5)
                .setSnapshotEpoch(0)
                .setStateBatches(List.of())
                .build()
        ));

        request = new InitializeShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new InitializeShareGroupStateRequestData.InitializeStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new InitializeShareGroupStateRequestData.PartitionData()
                    .setPartition(partition)
                ))
            ));

        result = shard.initializeState(request);

        expectedData = InitializeShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, partition, Errors.FENCED_STATE_EPOCH, Errors.FENCED_STATE_EPOCH.exception().getMessage());
        expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
    }

    @Test
    public void testInitializeNullMetadataImage() {
        shard.onMetadataUpdate(null, null);

        InitializeShareGroupStateRequestData request = new InitializeShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new InitializeShareGroupStateRequestData.InitializeStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new InitializeShareGroupStateRequestData.PartitionData()
                    .setPartition(0)
                ))
            ));

        CoordinatorResult<InitializeShareGroupStateResponseData, CoordinatorRecord> result = shard.initializeState(request);

        InitializeShareGroupStateResponseData expectedData = InitializeShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
    }

    @Test
    public void testInitializeTopicIdNonExistentInMetadataImage() {
        MetadataImage image = mock(MetadataImage.class);
        shard.onMetadataUpdate(null, new KRaftCoordinatorMetadataImage(image));

        InitializeShareGroupStateRequestData request = new InitializeShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new InitializeShareGroupStateRequestData.InitializeStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new InitializeShareGroupStateRequestData.PartitionData()
                    .setPartition(0)
                ))
            ));

        // topic id not found in cache
        TopicsImage topicsImage = mock(TopicsImage.class);
        when(topicsImage.getTopic(eq(TOPIC_ID))).thenReturn(null);
        when(image.topics()).thenReturn(topicsImage);
        CoordinatorResult<InitializeShareGroupStateResponseData, CoordinatorRecord> result = shard.initializeState(request);

        InitializeShareGroupStateResponseData expectedData = InitializeShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
        verify(topicsImage, times(1)).getTopic(eq(TOPIC_ID));
    }

    @Test
    public void testInitializePartitionIdNonExistentInMetadataImage() {
        MetadataImage image = mock(MetadataImage.class);
        when(image.cluster()).thenReturn(mock(ClusterImage.class));
        shard.onMetadataUpdate(null, new KRaftCoordinatorMetadataImage(image));

        InitializeShareGroupStateRequestData request = new InitializeShareGroupStateRequestData()
            .setGroupId(GROUP_ID)
            .setTopics(List.of(new InitializeShareGroupStateRequestData.InitializeStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new InitializeShareGroupStateRequestData.PartitionData()
                    .setPartition(0)
                ))
            ));

        // topic id found in cache
        TopicImage topicImage = mock(TopicImage.class);
        when(topicImage.partitions()).thenReturn(Map.of());
        TopicsImage topicsImage = mock(TopicsImage.class);
        when(topicsImage.getTopic(eq(TOPIC_ID))).thenReturn(topicImage);
        when(image.topics()).thenReturn(topicsImage);

        // partition id not found
        when(topicsImage.getPartition(eq(TOPIC_ID), eq(0))).thenReturn(null);
        CoordinatorResult<InitializeShareGroupStateResponseData, CoordinatorRecord> result = shard.initializeState(request);

        InitializeShareGroupStateResponseData expectedData = InitializeShareGroupStateResponse.toErrorResponseData(
            TOPIC_ID, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
        List<CoordinatorRecord> expectedRecords = List.of();

        assertEquals(expectedData, result.response());
        assertEquals(expectedRecords, result.records());
        verify(topicsImage, times(1)).getTopic(eq(TOPIC_ID));
        verify(topicImage, times(1)).partitions();
    }

    @Test
    public void testSnapshotColdPartitionsNoEligiblePartitions() {
        MetadataImage image = mock(MetadataImage.class);
        shard.onMetadataUpdate(null, new KRaftCoordinatorMetadataImage(image));
        int offset = 0;
        int producerId = 0;
        short producerEpoch = 0;
        int leaderEpoch = 0;

        long timestamp = TIME.milliseconds();

        CoordinatorRecord record1 = CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(0),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(0)
                    .setStateEpoch(0)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setLeaderEpoch(leaderEpoch)
                    .setCreateTimestamp(timestamp)
                    .setWriteTimestamp(timestamp)
                    .setStateBatches(List.of(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(10)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        CoordinatorRecord record2 = CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(1),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(0)
                    .setStateEpoch(0)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setLeaderEpoch(leaderEpoch)
                    .setCreateTimestamp(timestamp)
                    .setWriteTimestamp(timestamp)
                    .setStateBatches(List.of(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(10)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        shard.replay(offset, producerId, producerEpoch, record1);
        shard.replay(offset + 1, producerId, producerEpoch, record2);

        assertNotNull(shard.getShareStateMapValue(SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, 0)));
        assertNotNull(shard.getShareStateMapValue(SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, 1)));

        TIME.sleep(5000);   // Less than config.

        assertEquals(0, shard.snapshotColdPartitions().records().size());
    }

    @Test
    public void testSnapshotColdPartitionsSnapshotUpdateNotConsidered() {
        MetadataImage image = mock(MetadataImage.class);
        shard.onMetadataUpdate(null, new KRaftCoordinatorMetadataImage(image));
        int offset = 0;
        int producerId = 0;
        short producerEpoch = 0;
        int leaderEpoch = 0;

        long timestamp = TIME.milliseconds();

        CoordinatorRecord record1 = CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(0),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(0)
                    .setStateEpoch(0)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setLeaderEpoch(leaderEpoch)
                    .setCreateTimestamp(timestamp)
                    .setWriteTimestamp(timestamp)
                    .setStateBatches(List.of(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(10)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        SharePartitionKey key = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, 0);

        shard.replay(offset, producerId, producerEpoch, record1);
        assertNotNull(shard.getShareStateMapValue(key));

        long sleep = 12000;
        TIME.sleep(sleep);

        List<CoordinatorRecord> expectedRecords = List.of(
            CoordinatorRecord.record(
                new ShareSnapshotKey()
                    .setGroupId(GROUP_ID)
                    .setTopicId(TOPIC_ID)
                    .setPartition(0),
                new ApiMessageAndVersion(
                    new ShareSnapshotValue()
                        .setSnapshotEpoch(1)
                        .setStateEpoch(0)
                        .setStartOffset(0)
                        .setDeliveryCompleteCount(11)
                        .setLeaderEpoch(leaderEpoch)
                        .setCreateTimestamp(timestamp)
                        .setWriteTimestamp(timestamp + sleep)
                        .setStateBatches(List.of(
                            new ShareSnapshotValue.StateBatch()
                                .setFirstOffset(0)
                                .setLastOffset(10)
                                .setDeliveryCount((short) 1)
                                .setDeliveryState((byte) 0))),
                    (short) 0
                )
            )
        );

        assertEquals(expectedRecords, shard.snapshotColdPartitions().records());

        shard.replay(offset + 1, producerId, producerEpoch, expectedRecords.get(0));
        assertNotNull(shard.getShareStateMapValue(key));

        CoordinatorRecord record2 = CoordinatorRecord.record(
            new ShareUpdateKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(0),
            new ApiMessageAndVersion(
                new ShareUpdateValue()
                    .setSnapshotEpoch(0)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setLeaderEpoch(leaderEpoch)
                    .setStateBatches(List.of(
                        new ShareUpdateValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(10)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        shard.replay(offset + 2, producerId, producerEpoch, record2);

        TIME.sleep(sleep);

        assertNotNull(shard.getShareStateMapValue(key));
        assertEquals(timestamp + sleep, shard.getShareStateMapValue(key).writeTimestamp()); // No snapshot since update has no time info.
    }

    @Test
    public void testSnapshotColdPartitionsDoesNotPerpetuallySnapshot() {
        MetadataImage image = mock(MetadataImage.class);
        shard.onMetadataUpdate(null, new KRaftCoordinatorMetadataImage(image));
        int offset = 0;
        int producerId = 0;
        short producerEpoch = 0;
        int leaderEpoch = 0;

        long timestamp = TIME.milliseconds();

        CoordinatorRecord record1 = CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(0),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(0)
                    .setStateEpoch(0)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setLeaderEpoch(leaderEpoch)
                    .setCreateTimestamp(timestamp)
                    .setWriteTimestamp(timestamp)
                    .setStateBatches(List.of(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(10)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        shard.replay(offset, producerId, producerEpoch, record1);
        assertNotNull(shard.getShareStateMapValue(SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, 0)));

        long sleep = 12000;
        TIME.sleep(sleep);

        List<CoordinatorRecord> expectedRecords = List.of(
            CoordinatorRecord.record(
                new ShareSnapshotKey()
                    .setGroupId(GROUP_ID)
                    .setTopicId(TOPIC_ID)
                    .setPartition(0),
                new ApiMessageAndVersion(
                    new ShareSnapshotValue()
                        .setSnapshotEpoch(1)
                        .setStateEpoch(0)
                        .setStartOffset(0)
                        .setDeliveryCompleteCount(11)
                        .setLeaderEpoch(leaderEpoch)
                        .setCreateTimestamp(timestamp)
                        .setWriteTimestamp(timestamp + sleep)
                        .setStateBatches(List.of(
                            new ShareSnapshotValue.StateBatch()
                                .setFirstOffset(0)
                                .setLastOffset(10)
                                .setDeliveryCount((short) 1)
                                .setDeliveryState((byte) 0))),
                    (short) 0
                )
            )
        );

        assertEquals(expectedRecords, shard.snapshotColdPartitions().records());

        shard.replay(offset + 1, producerId, producerEpoch, expectedRecords.get(0));
        assertNotNull(shard.getShareStateMapValue(SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, 0)));

        // Since all existing snapshots are already snapshotted, no new records will be created.
        TIME.sleep(12000);

        assertEquals(0, shard.snapshotColdPartitions().records().size());
    }

    @Test
    public void testSnapshotColdPartitionsPartialEligiblePartitions() {
        MetadataImage image = mock(MetadataImage.class);
        shard.onMetadataUpdate(null, new KRaftCoordinatorMetadataImage(image));
        int offset = 0;
        int producerId = 0;
        short producerEpoch = 0;
        int leaderEpoch = 0;
        SharePartitionKey key0 = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, 0);
        SharePartitionKey key1 = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, 1);

        long timestamp = TIME.milliseconds();
        int record1SnapshotEpoch = 0;

        CoordinatorRecord record1 = CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(0),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(record1SnapshotEpoch)
                    .setStateEpoch(0)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setLeaderEpoch(leaderEpoch)
                    .setCreateTimestamp(timestamp)
                    .setWriteTimestamp(timestamp)
                    .setStateBatches(List.of(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(10)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        long delta = 15000; // 15 seconds

        CoordinatorRecord record2 = CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(1),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(0)
                    .setStateEpoch(0)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setLeaderEpoch(leaderEpoch)
                    .setCreateTimestamp(timestamp + delta)
                    .setWriteTimestamp(timestamp + delta)
                    .setStateBatches(List.of(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(10)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        shard.replay(offset, producerId, producerEpoch, record1);
        shard.replay(offset + 1, producerId, producerEpoch, record2);

        assertNotNull(shard.getShareStateMapValue(key0));
        assertNotNull(shard.getShareStateMapValue(key1));
        assertEquals(timestamp, shard.getShareStateMapValue(key0).writeTimestamp());
        assertEquals(timestamp + delta, shard.getShareStateMapValue(key1).writeTimestamp());

        long sleep = 12000;
        TIME.sleep(sleep);  // Record 1 is eligible now.

        List<CoordinatorRecord> expectedRecords = List.of(
            CoordinatorRecord.record(
                new ShareSnapshotKey()
                    .setGroupId(GROUP_ID)
                    .setTopicId(TOPIC_ID)
                    .setPartition(0),
                new ApiMessageAndVersion(
                    new ShareSnapshotValue()
                        .setSnapshotEpoch(record1SnapshotEpoch + 1)
                        .setStateEpoch(0)
                        .setStartOffset(0)
                        .setDeliveryCompleteCount(11)
                        .setLeaderEpoch(leaderEpoch)
                        .setCreateTimestamp(timestamp)
                        .setWriteTimestamp(timestamp + sleep)
                        .setStateBatches(List.of(
                            new ShareSnapshotValue.StateBatch()
                                .setFirstOffset(0)
                                .setLastOffset(10)
                                .setDeliveryCount((short) 1)
                                .setDeliveryState((byte) 0))),
                    (short) 0
                )
            )
        );

        List<CoordinatorRecord> records = shard.snapshotColdPartitions().records();
        assertEquals(expectedRecords, records);

        shard.replay(offset + 2, producerId, producerEpoch, records.get(0));

        assertEquals(timestamp + delta, shard.getShareStateMapValue(key1).writeTimestamp());
        assertEquals(timestamp + sleep, shard.getShareStateMapValue(key0).writeTimestamp());
    }

    @Test
    public void testOnTopicsDeletedEmptyTopicIds() {
        CoordinatorResult<Void, CoordinatorRecord> expectedResult = new CoordinatorResult<>(List.of());
        assertEquals(expectedResult, shard.maybeCleanupShareState(Set.of()));

        // No internal state map.
        shard = new ShareCoordinatorShardBuilder().build();
        assertEquals(expectedResult, shard.maybeCleanupShareState(Set.of(Uuid.randomUuid())));
    }

    @Test
    public void testOnTopicsDeletedTopicIds() {
        MetadataImage image = mock(MetadataImage.class);
        shard.onMetadataUpdate(null, new KRaftCoordinatorMetadataImage(image));

        int offset = 0;
        int producerId = 0;
        short producerEpoch = 0;
        int leaderEpoch = 0;
        SharePartitionKey key1 = SharePartitionKey.getInstance(GROUP_ID, TOPIC_ID, 0);

        long timestamp = TIME.milliseconds();
        int record1SnapshotEpoch = 0;

        CoordinatorRecord record1 = CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(0),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(record1SnapshotEpoch)
                    .setStateEpoch(0)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setLeaderEpoch(leaderEpoch)
                    .setCreateTimestamp(timestamp)
                    .setWriteTimestamp(timestamp)
                    .setStateBatches(List.of(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(10)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        CoordinatorRecord record2 = CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID_2)
                .setPartition(0),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setSnapshotEpoch(record1SnapshotEpoch)
                    .setStateEpoch(0)
                    .setStartOffset(0)
                    .setDeliveryCompleteCount(11)
                    .setLeaderEpoch(leaderEpoch)
                    .setCreateTimestamp(timestamp)
                    .setWriteTimestamp(timestamp)
                    .setStateBatches(List.of(
                        new ShareSnapshotValue.StateBatch()
                            .setFirstOffset(0)
                            .setLastOffset(10)
                            .setDeliveryCount((short) 1)
                            .setDeliveryState((byte) 0))),
                (short) 0
            )
        );

        shard.replay(offset, producerId, producerEpoch, record1);
        shard.replay(offset + 1, producerId, producerEpoch, record2);

        CoordinatorResult<Void, CoordinatorRecord> expectedResult = new CoordinatorResult<>(List.of(
            ShareCoordinatorRecordHelpers.newShareStateTombstoneRecord(key1.groupId(), key1.topicId(), key1.partition())
        ));

        assertEquals(expectedResult, shard.maybeCleanupShareState(Set.of(TOPIC_ID)));
    }

    private static ShareGroupOffset groupOffset(ApiMessage record) {
        if (record instanceof ShareSnapshotValue) {
            return ShareGroupOffset.fromRecord((ShareSnapshotValue) record);
        }
        return ShareGroupOffset.fromRecord((ShareUpdateValue) record);
    }

    private void initSharePartition(ShareCoordinatorShard shard, SharePartitionKey key) {
        initSharePartition(shard, key, 0);
    }
    private void initSharePartition(ShareCoordinatorShard shard, SharePartitionKey key, int stateEpoch) {
        shard.replay(0L, 0L, (short) 0, CoordinatorRecord.record(
            new ShareSnapshotKey()
                .setGroupId(key.groupId())
                .setTopicId(key.topicId())
                .setPartition(key.partition()),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setStateEpoch(stateEpoch)
                    .setLeaderEpoch(-1)
                    .setStartOffset(-1)
                    .setDeliveryCompleteCount(-1),
                (short) 0
            )
        ));
    }
}
