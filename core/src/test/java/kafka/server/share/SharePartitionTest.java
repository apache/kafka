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
package kafka.server.share;

import kafka.server.ReplicaManager;
import kafka.server.share.SharePartition.GapWindow;
import kafka.server.share.SharePartition.SharePartitionState;
import kafka.server.share.SharePartitionManager.SharePartitionListener;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ShareAcquireMode;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.ShareGroupAutoOffsetResetStrategy;
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.fetch.AcquisitionLockTimerTask;
import org.apache.kafka.server.share.fetch.DelayedShareFetchGroupKey;
import org.apache.kafka.server.share.fetch.InFlightBatch;
import org.apache.kafka.server.share.fetch.InFlightState;
import org.apache.kafka.server.share.fetch.RecordState;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.metrics.SharePartitionMetrics;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.persister.PersisterStateBatch;
import org.apache.kafka.server.share.persister.ReadShareGroupStateResult;
import org.apache.kafka.server.share.persister.TopicData;
import org.apache.kafka.server.share.persister.WriteShareGroupStateResult;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static kafka.server.share.SharePartition.EMPTY_MEMBER_ID;
import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.memoryRecordsBuilder;
import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.yammerMetricValue;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("resource")
public class SharePartitionTest {

    private static final String ACQUISITION_LOCK_NEVER_GOT_RELEASED = "Acquisition lock never got released.";
    private static final String GROUP_ID = "test-group";
    private static final int MAX_DELIVERY_COUNT = 5;
    private static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
    private static final String MEMBER_ID = "member-1";
    private static final Time MOCK_TIME = new MockTime();
    private static final short MAX_IN_FLIGHT_RECORDS = 200;
    private static final int ACQUISITION_LOCK_TIMEOUT_MS = 100;
    private static final int DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS = 120;
    private static final int BATCH_SIZE = 500;
    private static final int DEFAULT_FETCH_OFFSET = 0;
    private static final int MAX_FETCH_RECORDS = Integer.MAX_VALUE;
    private static final byte ACKNOWLEDGE_TYPE_GAP_ID = 0;
    private static final FetchIsolation FETCH_ISOLATION_HWM = FetchIsolation.HIGH_WATERMARK;
    private static Timer mockTimer;
    private SharePartitionMetrics sharePartitionMetrics;

    @BeforeEach
    public void setUp() {
        kafka.utils.TestUtils.clearYammerMetrics();
        mockTimer = new MockTimer();
        sharePartitionMetrics = new SharePartitionMetrics(GROUP_ID, TOPIC_ID_PARTITION.topic(), TOPIC_ID_PARTITION.partition());
    }

    @AfterEach
    public void tearDown() throws Exception {
        mockTimer.close();
        sharePartitionMetrics.close();
    }

    @Test
    public void testMaybeInitialize() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(5, sharePartition.startOffset());
        assertEquals(15, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(5, sharePartition.nextFetchOffset());

        assertEquals(2, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));
        assertNotNull(sharePartition.cachedState().get(11L));

        assertEquals(10, sharePartition.cachedState().get(5L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(2, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());

        assertEquals(15, sharePartition.cachedState().get(11L).lastOffset());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(3, sharePartition.cachedState().get(11L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(11L).offsetState());

        // deliveryCompleteCount is incremented by the number of ACKNOWLEDGED and ARCHIVED records in readState result.
        assertEquals(5, sharePartition.deliveryCompleteCount());

        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_BATCH_COUNT).intValue() == 2,
            "In-flight batch count should be 2.");
        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_MESSAGE_COUNT).longValue() == 11,
            "In-flight message count should be 11.");
        assertEquals(11, sharePartitionMetrics.inFlightBatchMessageCount().sum());
        assertEquals(2, sharePartitionMetrics.inFlightBatchMessageCount().count());
        assertEquals(5, sharePartitionMetrics.inFlightBatchMessageCount().min());
        assertEquals(6, sharePartitionMetrics.inFlightBatchMessageCount().max());
    }

    @Test
    public void testMaybeInitializeDefaultStartEpochGroupConfigReturnsEarliest() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        GroupConfig groupConfig = Mockito.mock(GroupConfig.class);
        Mockito.when(groupConfigManager.groupConfig(GROUP_ID)).thenReturn(Optional.of(groupConfig));
        Mockito.when(groupConfig.shareAutoOffsetReset()).thenReturn(ShareGroupAutoOffsetResetStrategy.EARLIEST);

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(-1L, 0L, Optional.empty());
        Mockito.doReturn(new OffsetResultHolder(Optional.of(timestampAndOffset), Optional.empty())).
            when(replicaManager).fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withGroupConfigManager(groupConfigManager)
            .withReplicaManager(replicaManager)
            .build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        // replicaManager.fetchOffsetForTimestamp should be called with "ListOffsetsRequest.EARLIEST_TIMESTAMP"
        Mockito.verify(replicaManager).fetchOffsetForTimestamp(
            Mockito.any(TopicPartition.class),
            Mockito.eq(ListOffsetsRequest.EARLIEST_TIMESTAMP),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyBoolean()
        );

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());
        assertEquals(PartitionFactory.DEFAULT_STATE_EPOCH, sharePartition.stateEpoch());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeDefaultStartEpochGroupConfigReturnsLatest() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        GroupConfig groupConfig = Mockito.mock(GroupConfig.class);
        Mockito.when(groupConfigManager.groupConfig(GROUP_ID)).thenReturn(Optional.of(groupConfig));
        Mockito.when(groupConfig.shareAutoOffsetReset()).thenReturn(ShareGroupAutoOffsetResetStrategy.LATEST);

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(-1L, 15L, Optional.empty());
        Mockito.doReturn(new OffsetResultHolder(Optional.of(timestampAndOffset), Optional.empty())).
            when(replicaManager).fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withGroupConfigManager(groupConfigManager)
            .withReplicaManager(replicaManager)
            .build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        // replicaManager.fetchOffsetForTimestamp should be called with "ListOffsetsRequest.LATEST_TIMESTAMP"
        Mockito.verify(replicaManager).fetchOffsetForTimestamp(
            Mockito.any(TopicPartition.class),
            Mockito.eq(ListOffsetsRequest.LATEST_TIMESTAMP),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyBoolean()
        );

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(15, sharePartition.endOffset());
        assertEquals(PartitionFactory.DEFAULT_STATE_EPOCH, sharePartition.stateEpoch());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeDefaultStartEpochGroupConfigReturnsByDuration()
        throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        GroupConfig groupConfig = Mockito.mock(GroupConfig.class);
        Mockito.when(groupConfigManager.groupConfig(GROUP_ID)).thenReturn(Optional.of(groupConfig));

        // Since the timestamp() of duration based strategy is not deterministic, we need to mock the ShareGroupAutoOffsetResetStrategy.
        // mock: final ShareGroupAutoOffsetResetStrategy resetStrategy = ShareGroupAutoOffsetResetStrategy.fromString("by_duration:PT1H");
        final ShareGroupAutoOffsetResetStrategy resetStrategy = Mockito.mock(ShareGroupAutoOffsetResetStrategy.class);
        final long expectedTimestamp = MOCK_TIME.milliseconds() - TimeUnit.HOURS.toMillis(1);
        Mockito.when(resetStrategy.type()).thenReturn(ShareGroupAutoOffsetResetStrategy.StrategyType.BY_DURATION);
        Mockito.when(resetStrategy.timestamp()).thenReturn(expectedTimestamp);

        Mockito.when(groupConfig.shareAutoOffsetReset()).thenReturn(resetStrategy);

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(
            MOCK_TIME.milliseconds() - TimeUnit.HOURS.toMillis(1), 15L, Optional.empty());
        Mockito.doReturn(new OffsetResultHolder(Optional.of(timestampAndOffset), Optional.empty())).
            when(replicaManager).fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withGroupConfigManager(groupConfigManager)
            .withReplicaManager(replicaManager)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        // replicaManager.fetchOffsetForTimestamp should be called with the (current time - 1 hour)
        Mockito.verify(replicaManager).fetchOffsetForTimestamp(
            Mockito.any(TopicPartition.class),
            Mockito.eq(expectedTimestamp),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyBoolean()
        );

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(15, sharePartition.endOffset());
        assertEquals(PartitionFactory.DEFAULT_STATE_EPOCH, sharePartition.stateEpoch());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_BATCH_COUNT).intValue() == 0,
            "In-flight batch count should be 0.");
        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_MESSAGE_COUNT).longValue() == 0,
            "In-flight message count should be 0.");
    }

    @Test
    public void testMaybeInitializeDefaultStartEpochGroupConfigNotPresent() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        Mockito.when(groupConfigManager.groupConfig(GROUP_ID)).thenReturn(Optional.empty());

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(-1L, 15L, Optional.empty());
        Mockito.doReturn(new OffsetResultHolder(Optional.of(timestampAndOffset), Optional.empty())).
            when(replicaManager).fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withGroupConfigManager(groupConfigManager)
            .withReplicaManager(replicaManager)
            .build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        // replicaManager.fetchOffsetForTimestamp should be called with "ListOffsetsRequest.LATEST_TIMESTAMP"
        Mockito.verify(replicaManager).fetchOffsetForTimestamp(
            Mockito.any(TopicPartition.class),
            Mockito.eq(ListOffsetsRequest.LATEST_TIMESTAMP),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyBoolean()
        );

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(15, sharePartition.endOffset());
        assertEquals(PartitionFactory.DEFAULT_STATE_EPOCH, sharePartition.stateEpoch());
    }

    @Test
    public void testMaybeInitializeFetchOffsetForLatestTimestampThrowsError() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        Mockito.when(groupConfigManager.groupConfig(GROUP_ID)).thenReturn(Optional.empty());

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        Mockito.when(replicaManager.fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean()))
            .thenThrow(new RuntimeException("fetch offsets exception"));

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withGroupConfigManager(groupConfigManager)
            .withReplicaManager(replicaManager)
            .build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());

        // replicaManager.fetchOffsetForTimestamp should be called with "ListOffsetsRequest.LATEST_TIMESTAMP"
        Mockito.verify(replicaManager).fetchOffsetForTimestamp(
            Mockito.any(TopicPartition.class),
            Mockito.eq(ListOffsetsRequest.LATEST_TIMESTAMP),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyBoolean()
        );

        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeFetchOffsetForEarliestTimestampThrowsError() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        GroupConfig groupConfig = Mockito.mock(GroupConfig.class);
        Mockito.when(groupConfigManager.groupConfig(GROUP_ID)).thenReturn(Optional.of(groupConfig));
        Mockito.when(groupConfig.shareAutoOffsetReset()).thenReturn(ShareGroupAutoOffsetResetStrategy.EARLIEST);

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        Mockito.when(replicaManager.fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean()))
            .thenThrow(new RuntimeException("fetch offsets exception"));

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withGroupConfigManager(groupConfigManager)
            .withReplicaManager(replicaManager)
            .build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());

        // replicaManager.fetchOffsetForTimestamp should be called with "ListOffsetsRequest.EARLIEST_TIMESTAMP"
        Mockito.verify(replicaManager).fetchOffsetForTimestamp(
            Mockito.any(TopicPartition.class),
            Mockito.eq(ListOffsetsRequest.EARLIEST_TIMESTAMP),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyBoolean()
        );

        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeFetchOffsetForByDurationThrowsError() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        GroupConfig groupConfig = Mockito.mock(GroupConfig.class);
        Mockito.when(groupConfigManager.groupConfig(GROUP_ID)).thenReturn(Optional.of(groupConfig));

        // We need to mock the ShareGroupAutoOffsetResetStrategy as the timestamp() of duration based strategy is not deterministic.
        // final ShareGroupAutoOffsetResetStrategy resetStrategy = ShareGroupAutoOffsetResetStrategy.fromString("by_duration:PT1H");
        final ShareGroupAutoOffsetResetStrategy resetStrategy = Mockito.mock(ShareGroupAutoOffsetResetStrategy.class);
        final long expectedTimestamp = MOCK_TIME.milliseconds() - TimeUnit.HOURS.toMillis(1);
        Mockito.when(groupConfig.shareAutoOffsetReset()).thenReturn(resetStrategy);

        Mockito.when(resetStrategy.type()).thenReturn(ShareGroupAutoOffsetResetStrategy.StrategyType.BY_DURATION);
        Mockito.when(resetStrategy.timestamp()).thenReturn(expectedTimestamp);

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        Mockito.when(replicaManager.fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean()))
            .thenThrow(new RuntimeException("fetch offsets exception"));

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withGroupConfigManager(groupConfigManager)
            .withReplicaManager(replicaManager)
            .build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());

        Mockito.verify(replicaManager).fetchOffsetForTimestamp(
            Mockito.any(TopicPartition.class),
            Mockito.eq(expectedTimestamp),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyBoolean()
        );

        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeSharePartitionAgain() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());

        // Initialize again, no need to send mock persister response again as the state is already initialized.
        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());

        // Verify the persister read state is called only once.
        Mockito.verify(persister, Mockito.times(1)).readState(Mockito.any());
    }

    @Test
    public void testMaybeInitializeSharePartitionAgainConcurrentRequests() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        // No need to send mock persister response again as only 1 thread should read state from persister.
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<CompletableFuture<Void>> results = new ArrayList<>(10);

        try {
            for (int i = 0; i < 10; i++) {
                executorService.submit(() -> {
                    results.add(sharePartition.maybeInitialize());
                });
            }
        } finally {
            if (!executorService.awaitTermination(30, TimeUnit.MILLISECONDS))
                executorService.shutdown();
        }
        assertTrue(results.stream().allMatch(CompletableFuture::isDone));
        assertFalse(results.stream().allMatch(CompletableFuture::isCompletedExceptionally));

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        // Verify the persister read state is called only once.
        Mockito.verify(persister, Mockito.times(1)).readState(Mockito.any());
    }

    @Test
    public void testMaybeInitializeWithEmptyStateBatches() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.NONE.code(), Errors.NONE.message(), List.of()))))
        );
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertTrue(sharePartition.cachedState().isEmpty());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(10, sharePartition.endOffset());
        assertEquals(5, sharePartition.stateEpoch());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeWithErrorPartitionResponse() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);

        // Mock NOT_COORDINATOR error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.NOT_COORDINATOR.code(), Errors.NOT_COORDINATOR.message(),
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(CoordinatorNotAvailableException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock COORDINATOR_NOT_AVAILABLE error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.COORDINATOR_NOT_AVAILABLE.code(), Errors.COORDINATOR_NOT_AVAILABLE.message(),
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(CoordinatorNotAvailableException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock COORDINATOR_LOAD_IN_PROGRESS error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.COORDINATOR_LOAD_IN_PROGRESS.code(), Errors.COORDINATOR_LOAD_IN_PROGRESS.message(),
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(CoordinatorNotAvailableException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock GROUP_ID_NOT_FOUND error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message(),
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(GroupIdNotFoundException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock UNKNOWN_TOPIC_OR_PARTITION error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), Errors.UNKNOWN_TOPIC_OR_PARTITION.message(),
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(UnknownTopicOrPartitionException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock FENCED_STATE_EPOCH error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.FENCED_STATE_EPOCH.code(), Errors.FENCED_STATE_EPOCH.message(),
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(NotLeaderOrFollowerException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock FENCED_LEADER_EPOCH error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.FENCED_LEADER_EPOCH.code(), Errors.FENCED_LEADER_EPOCH.message(),
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(NotLeaderOrFollowerException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock UNKNOWN_SERVER_ERROR error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.UNKNOWN_SERVER_ERROR.code(), Errors.UNKNOWN_SERVER_ERROR.message(),
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(UnknownServerException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock NETWORK_EXCEPTION error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.NETWORK_EXCEPTION.code(), Errors.NETWORK_EXCEPTION.message(),
                    List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(UnknownServerException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithInvalidStartOffsetStateBatches() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 6L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithInvalidTopicIdResponse() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(Uuid.randomUuid(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithInvalidPartitionResponse() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(1, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithNoOpStatePersister() {
        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(-1L, 0L, Optional.empty());
        Mockito.doReturn(new OffsetResultHolder(Optional.of(timestampAndOffset), Optional.empty())).
            when(replicaManager).fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        SharePartition sharePartition = SharePartitionBuilder.builder().withReplicaManager(replicaManager).build();
        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertTrue(sharePartition.cachedState().isEmpty());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());
        assertEquals(0, sharePartition.stateEpoch());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithNullResponse() {
        Persister persister = Mockito.mock(Persister.class);
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithNullTopicsData() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(null);
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithEmptyTopicsData() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of());
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithReadException() {
        Persister persister = Mockito.mock(Persister.class);
        // Complete the future exceptionally for read state.
        Mockito.when(persister.readState(Mockito.any())).thenReturn(FutureUtils.failedFuture(new RuntimeException("Read exception")));
        SharePartition sharePartition1 = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition1.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(RuntimeException.class, result);
        assertEquals(SharePartitionState.FAILED, sharePartition1.partitionState());

        persister = Mockito.mock(Persister.class);
        // Throw exception for read state.
        Mockito.when(persister.readState(Mockito.any())).thenThrow(new RuntimeException("Read exception"));
        SharePartition sharePartition2 = SharePartitionBuilder.builder().withPersister(persister).build();

        assertThrows(RuntimeException.class, sharePartition2::maybeInitialize);
    }

    @Test
    public void testMaybeInitializeFencedSharePartition() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        // Mark the share partition as fenced.
        sharePartition.markFenced();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(LeaderNotAvailableException.class, result);
        assertEquals(SharePartitionState.FENCED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeStateBatchesWithGapAtBeginning() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 10L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 20L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 10 to 14
                        new PersisterStateBatch(21L, 30L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(10, sharePartition.nextFetchOffset());

        assertEquals(2, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(15L));
        assertNotNull(sharePartition.cachedState().get(21L));

        assertEquals(20, sharePartition.cachedState().get(15L).lastOffset());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(2, sharePartition.cachedState().get(15L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(15L).offsetState());

        assertEquals(30, sharePartition.cachedState().get(21L).lastOffset());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(21L).batchState());
        assertEquals(3, sharePartition.cachedState().get(21L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(21L).offsetState());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        assertEquals(10, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(30, persisterReadResultGapWindow.endOffset());

        // deliveryCompleteCount is incremented by the number of ACKNOWLEDGED and ARCHIVED records in readState result.
        assertEquals(16, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeStateBatchesWithMultipleGaps() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 10L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 20L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 10 to 14
                        new PersisterStateBatch(30L, 40L, RecordState.ARCHIVED.id, (short) 3))))))); // There is a gap from 21 to 29
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(40, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(10, sharePartition.nextFetchOffset());

        assertEquals(2, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(15L));
        assertNotNull(sharePartition.cachedState().get(30L));

        assertEquals(20, sharePartition.cachedState().get(15L).lastOffset());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(2, sharePartition.cachedState().get(15L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(15L).offsetState());

        assertEquals(40, sharePartition.cachedState().get(30L).lastOffset());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(30L).batchState());
        assertEquals(3, sharePartition.cachedState().get(30L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(30L).offsetState());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        assertEquals(10, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());

        // deliveryCompleteCount is incremented by the number of ACKNOWLEDGED and ARCHIVED records in readState result.
        assertEquals(17, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeStateBatchesWithGapNotAtBeginning() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 15L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 20L, RecordState.ACKNOWLEDGED.id, (short) 2),
                        new PersisterStateBatch(30L, 40L, RecordState.ARCHIVED.id, (short) 3))))))); // There is a gap from 21 to 29
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        // The start offset will be moved to 21, since the offsets 15 to 20 are acknowledged, and will be removed
        // from cached state in the maybeUpdateCachedStateAndOffsets method
        assertEquals(21, sharePartition.startOffset());
        assertEquals(40, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(21, sharePartition.nextFetchOffset());

        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(30L));

        assertEquals(40, sharePartition.cachedState().get(30L).lastOffset());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(30L).batchState());
        assertEquals(3, sharePartition.cachedState().get(30L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(30L).offsetState());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        assertEquals(21, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());
        assertEquals(11, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeStateBatchesWithoutGaps() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 15L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 20L, RecordState.ACKNOWLEDGED.id, (short) 2),
                        new PersisterStateBatch(21L, 30L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertTrue(sharePartition.cachedState().isEmpty());
        assertEquals(31, sharePartition.startOffset());
        assertEquals(31, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(31, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();

        // Since there are no gaps present in the readState response, persisterReadResultGapWindow should be null
        assertNull(persisterReadResultGapWindow);
    }

    @Test
    public void testMaybeInitializeAndAcquire() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 10L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 18L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(20L, 22L, RecordState.ARCHIVED.id, (short) 2),
                        new PersisterStateBatch(26L, 30L, RecordState.AVAILABLE.id, (short) 1)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(10, sharePartition.nextFetchOffset());

        assertEquals(18, sharePartition.cachedState().get(15L).lastOffset());
        assertEquals(22, sharePartition.cachedState().get(20L).lastOffset());
        assertEquals(30, sharePartition.cachedState().get(26L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(10L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(30L, sharePartition.persisterReadResultGapWindow().endOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Create a single batch record that covers the entire range from 10 to 30 of initial read gap.
        // The records in the batch are from 10 to 49.
        MemoryRecords records = memoryRecords(10, 40);
        // Set max fetch records to 1, records will be acquired till the first gap is encountered.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                1,
                10,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            5);

        assertArrayEquals(expectedAcquiredRecord(10, 14, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(4, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(15L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Send the same batch again to acquire the next set of records.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                10,
                15,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            13);

        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(15, 18, 3));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(19, 19, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(23, 25, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(26, 30, 2));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(31, sharePartition.nextFetchOffset());
        assertEquals(6, sharePartition.cachedState().size());
        assertEquals(19, sharePartition.cachedState().get(19L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(19L).batchState());
        assertEquals(1, sharePartition.cachedState().get(19L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(19L).offsetState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(23, sharePartition.cachedState().get(23L).firstOffset());
        assertEquals(25, sharePartition.cachedState().get(23L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(23L).batchState());
        assertEquals(1, sharePartition.cachedState().get(23L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(26L).batchState());
        assertEquals(30L, sharePartition.endOffset());
        // As all the gaps are now filled, the persisterReadResultGapWindow should be null.
        assertNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Now initial read gap is filled, so the complete batch can be acquired despite max fetch records being 1.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                1,
                31,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            19);

        assertArrayEquals(expectedAcquiredRecord(31, 49, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(50, sharePartition.nextFetchOffset());
        assertEquals(7, sharePartition.cachedState().size());
        assertEquals(31, sharePartition.cachedState().get(31L).firstOffset());
        assertEquals(49, sharePartition.cachedState().get(31L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(31L).batchState());
        assertEquals(1, sharePartition.cachedState().get(31L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(31L).offsetState());
        assertEquals(49L, sharePartition.endOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeAndAcquireWithHigherMaxFetchRecords() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 10L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 18L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(20L, 22L, RecordState.ARCHIVED.id, (short) 2),
                        new PersisterStateBatch(26L, 30L, RecordState.AVAILABLE.id, (short) 1)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(10, sharePartition.nextFetchOffset());

        assertEquals(18, sharePartition.cachedState().get(15L).lastOffset());
        assertEquals(22, sharePartition.cachedState().get(20L).lastOffset());
        assertEquals(30, sharePartition.cachedState().get(26L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(10L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(30L, sharePartition.persisterReadResultGapWindow().endOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Create a single batch record that covers the entire range from 10 to 30 of initial read gap.
        // The records in the batch are from 10 to 49.
        MemoryRecords records = memoryRecords(10, 40);
        // Set max fetch records to 500, all records should be acquired.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                10,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            37);

        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(10, 14, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(15, 18, 3));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(19, 19, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(23, 25, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(26, 30, 2));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(31, 49, 1));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(50, sharePartition.nextFetchOffset());
        assertEquals(7, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).lastOffset());
        assertEquals(23, sharePartition.cachedState().get(23L).firstOffset());
        assertEquals(25, sharePartition.cachedState().get(23L).lastOffset());
        assertEquals(31, sharePartition.cachedState().get(31L).firstOffset());
        assertEquals(49, sharePartition.cachedState().get(31L).lastOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(19L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(23L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(26L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(31L).batchState());
        assertEquals(49L, sharePartition.endOffset());
        // As all the gaps are now filled, the persisterReadResultGapWindow should be null.
        assertNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(3, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeAndAcquireWithFetchBatchLastOffsetWithinCachedBatch() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 10L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 18L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(20L, 22L, RecordState.ARCHIVED.id, (short) 2),
                        new PersisterStateBatch(26L, 30L, RecordState.AVAILABLE.id, (short) 1)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(10, sharePartition.nextFetchOffset());

        assertEquals(18, sharePartition.cachedState().get(15L).lastOffset());
        assertEquals(22, sharePartition.cachedState().get(20L).lastOffset());
        assertEquals(30, sharePartition.cachedState().get(26L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(10L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(30L, sharePartition.persisterReadResultGapWindow().endOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Create a single batch record that ends in between the cached batch and the fetch offset is
        // post startOffset.
        MemoryRecords records = memoryRecords(12, 16);
        // Set max fetch records to 500, records should be acquired till the last offset of the fetched batch.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                10,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            13);

        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(12, 14, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(15, 18, 3));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(19, 19, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(23, 25, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecords(26, 27, 2));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(28, sharePartition.nextFetchOffset());
        assertEquals(6, sharePartition.cachedState().size());
        assertEquals(12, sharePartition.cachedState().get(12L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(12L).lastOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).lastOffset());
        assertEquals(23, sharePartition.cachedState().get(23L).firstOffset());
        assertEquals(25, sharePartition.cachedState().get(23L).lastOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(12L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(19L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(23L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(26L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(26L).offsetState().get(26L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(26L).offsetState().get(27L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).offsetState().get(28L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).offsetState().get(29L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).offsetState().get(30L).state());
        assertEquals(30L, sharePartition.endOffset());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(28L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeAndAcquireWithFetchBatchPriorStartOffset() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 10L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 18L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(20L, 22L, RecordState.ARCHIVED.id, (short) 2),
                        new PersisterStateBatch(26L, 30L, RecordState.AVAILABLE.id, (short) 1)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(10, sharePartition.nextFetchOffset());

        assertEquals(18, sharePartition.cachedState().get(15L).lastOffset());
        assertEquals(22, sharePartition.cachedState().get(20L).lastOffset());
        assertEquals(30, sharePartition.cachedState().get(26L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(10L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(30L, sharePartition.persisterReadResultGapWindow().endOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Create a single batch record where first offset is prior startOffset.
        MemoryRecords records = memoryRecords(6, 16);
        // Set max fetch records to 500, records should be acquired till the last offset of the fetched batch.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                10,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            10);

        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(10, 14, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(15, 18, 3));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(19, 19, 1));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(23, sharePartition.nextFetchOffset());
        assertEquals(5, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).lastOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(19L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertEquals(30L, sharePartition.endOffset());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(20L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeAndAcquireWithMultipleBatches() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 18L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(20L, 22L, RecordState.ARCHIVED.id, (short) 2),
                        new PersisterStateBatch(26L, 30L, RecordState.AVAILABLE.id, (short) 1)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(5, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(5, sharePartition.nextFetchOffset());

        assertEquals(18, sharePartition.cachedState().get(15L).lastOffset());
        assertEquals(22, sharePartition.cachedState().get(20L).lastOffset());
        assertEquals(30, sharePartition.cachedState().get(26L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(5L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(30L, sharePartition.persisterReadResultGapWindow().endOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Create multiple batch records that covers the entire range from 5 to 30 of initial read gap.
        // The records in the batch are from 5 to 49.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 5, 2).close();
        memoryRecordsBuilder(buffer, 8, 1).close();
        memoryRecordsBuilder(buffer, 10, 2).close();
        memoryRecordsBuilder(buffer, 13, 6).close();
        memoryRecordsBuilder(buffer, 19, 3).close();
        memoryRecordsBuilder(buffer, 22, 9).close();
        memoryRecordsBuilder(buffer, 31, 19).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Set max fetch records to 1, records will be acquired till the first gap is encountered.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                1,
                5L,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            2);

        assertArrayEquals(expectedAcquiredRecord(5, 6, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(7, sharePartition.nextFetchOffset());
        assertEquals(4, sharePartition.cachedState().size());
        assertEquals(5, sharePartition.cachedState().get(5L).firstOffset());
        assertEquals(6, sharePartition.cachedState().get(5L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(7L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Remove first batch from the records as the fetch offset has moved forward to 7 offset.
        List<RecordBatch> batch = TestUtils.toList(records.batches());
        records = records.slice(batch.get(0).sizeInBytes(), records.sizeInBytes() - batch.get(0).sizeInBytes());
        // Send the batch again to acquire the next set of records.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                3,
                7L,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            4);

        assertArrayEquals(expectedAcquiredRecord(8, 11, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(5, sharePartition.cachedState().size());
        assertEquals(8, sharePartition.cachedState().get(8L).firstOffset());
        assertEquals(11, sharePartition.cachedState().get(8L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(8L).batchState());
        assertEquals(1, sharePartition.cachedState().get(8L).batchDeliveryCount());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertEquals(30L, sharePartition.endOffset());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(12L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Remove the next 2 batches from the records as the fetch offset has moved forward to 12 offset.
        int size = batch.get(1).sizeInBytes() + batch.get(2).sizeInBytes();
        records = records.slice(size, records.sizeInBytes() - size);
        // Send the records with 8 as max fetch records to acquire new and existing cached batches.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                8,
                12,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            10);

        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(13, 14, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(15, 18, 3));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(19, 19, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(23, 25, 1));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(26, sharePartition.nextFetchOffset());
        assertEquals(8, sharePartition.cachedState().size());
        assertEquals(13, sharePartition.cachedState().get(13L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(13L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(13L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(19, sharePartition.cachedState().get(19L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(19L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(23, sharePartition.cachedState().get(23L).firstOffset());
        assertEquals(25, sharePartition.cachedState().get(23L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(23L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertEquals(30L, sharePartition.endOffset());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(26L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Remove the next 2 batches from the records as the fetch offset has moved forward to 26 offset.
        // Do not remove the 5th batch as it's only partially acquired.
        size = batch.get(3).sizeInBytes() + batch.get(4).sizeInBytes();
        records = records.slice(size, records.sizeInBytes() - size);
        // Send the records with 10 as max fetch records to acquire the existing and till end of the
        // fetched data.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                10,
                26,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            24);

        expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(26, 30, 2));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(31, 49, 1));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(50, sharePartition.nextFetchOffset());
        assertEquals(9, sharePartition.cachedState().size());
        assertEquals(31, sharePartition.cachedState().get(31L).firstOffset());
        assertEquals(49, sharePartition.cachedState().get(31L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(31L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(26L).batchState());
        assertEquals(49L, sharePartition.endOffset());
        // As all the gaps are now filled, the persisterReadResultGapWindow should be null.
        assertNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(3, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeAndAcquireWithMultipleBatchesAndLastOffsetWithinCachedBatch() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 18L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(20L, 22L, RecordState.ARCHIVED.id, (short) 2),
                        new PersisterStateBatch(26L, 30L, RecordState.AVAILABLE.id, (short) 1)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(5, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(5, sharePartition.nextFetchOffset());

        assertEquals(18, sharePartition.cachedState().get(15L).lastOffset());
        assertEquals(22, sharePartition.cachedState().get(20L).lastOffset());
        assertEquals(30, sharePartition.cachedState().get(26L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(5L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(30L, sharePartition.persisterReadResultGapWindow().endOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Create multiple batch records that ends in between the cached batch and the fetch offset is
        // post startOffset.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 7, 2).close();
        memoryRecordsBuilder(buffer, 10, 2).close();
        memoryRecordsBuilder(buffer, 13, 6).close();
        // Though 19 offset is a gap but still be acquired.
        memoryRecordsBuilder(buffer, 20, 8).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Set max fetch records to 500, records should be acquired till the last offset of the fetched batch.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                5,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            18);

        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(7, 14, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(15, 18, 3));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(19, 19, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(23, 25, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecords(26, 27, 2));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(28, sharePartition.nextFetchOffset());
        assertEquals(6, sharePartition.cachedState().size());
        assertEquals(7, sharePartition.cachedState().get(7L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(7L).lastOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).lastOffset());
        assertEquals(23, sharePartition.cachedState().get(23L).firstOffset());
        assertEquals(25, sharePartition.cachedState().get(23L).lastOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(19L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(23L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(26L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(26L).offsetState().get(26L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(26L).offsetState().get(27L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).offsetState().get(28L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).offsetState().get(29L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).offsetState().get(30L).state());
        assertEquals(30L, sharePartition.endOffset());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(28L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeInitializeAndAcquireWithMultipleBatchesPriorStartOffset() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 10L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 18L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(20L, 22L, RecordState.ARCHIVED.id, (short) 2),
                        new PersisterStateBatch(26L, 30L, RecordState.AVAILABLE.id, (short) 1)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(10, sharePartition.nextFetchOffset());

        assertEquals(18, sharePartition.cachedState().get(15L).lastOffset());
        assertEquals(22, sharePartition.cachedState().get(20L).lastOffset());
        assertEquals(30, sharePartition.cachedState().get(26L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(10L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(30L, sharePartition.persisterReadResultGapWindow().endOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Create multiple batch records where multiple batches base offsets are prior startOffset.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 3, 2).close();
        memoryRecordsBuilder(buffer, 6, 1).close();
        memoryRecordsBuilder(buffer, 8, 4).close();
        memoryRecordsBuilder(buffer, 13, 10).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Set max fetch records to 500, records should be acquired till the last offset of the fetched batch.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                10,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            10);

        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(10, 14, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(15, 18, 3));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(19, 19, 1));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(23, sharePartition.nextFetchOffset());
        assertEquals(5, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(19L).lastOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(19L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertEquals(30L, sharePartition.endOffset());
        assertNotNull(sharePartition.persisterReadResultGapWindow());
        assertEquals(20L, sharePartition.persisterReadResultGapWindow().gapStartOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquireSingleRecord() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .build();
        MemoryRecords records = memoryRecords(1);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 1);

        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(1, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(0, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());

        // deliveryCompleteCount will not be changed because no record went to a Terminal state.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_BATCH_COUNT).intValue() == 1,
            "In-flight batch count should be 1.");
        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_MESSAGE_COUNT).longValue() == 1,
            "In-flight message count should be 1.");
        assertEquals(1, sharePartitionMetrics.inFlightBatchMessageCount().sum());
    }

    @Test
    public void testAcquireMultipleRecords() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .build();
        MemoryRecords records = memoryRecords(10, 5);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 3L, 5);

        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());

        // deliveryCompleteCount will not be changed because no record went to a Terminal state.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_BATCH_COUNT).intValue() == 1,
            "In-flight batch count should be 1.");
        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_MESSAGE_COUNT).longValue() == 5,
            "In-flight message count should be 5.");
        assertEquals(5, sharePartitionMetrics.inFlightBatchMessageCount().sum());
    }

    @Test
    public void testAcquireWithMaxFetchRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        // Less-number of records than max fetch records.
        MemoryRecords records = memoryRecords(5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            10,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            5);

        assertArrayEquals(expectedAcquiredRecord(0, 4, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(4, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());

        // More-number of records than max fetch records, but from 0 offset hence previous 10 records
        // should be ignored and new full batch till end should be acquired.
        records = memoryRecords(25);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            10,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            20);

        assertArrayEquals(expectedAcquiredRecord(5, 24, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(5, sharePartition.cachedState().get(5L).firstOffset());
        assertEquals(24, sharePartition.cachedState().get(5L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
    }

    @Test
    public void testAcquireWithMultipleBatchesAndMaxFetchRecords() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .build();

        // Create 3 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 10, 5).close();
        memoryRecordsBuilder(buffer, 15, 15).close();
        memoryRecordsBuilder(buffer, 30, 15).close();

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Acquire 10 records.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            10,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records, 10),
            FETCH_ISOLATION_HWM),
            20);

        // Validate 2 batches are fetched one with 5 records and other till end of batch, third batch
        // should be skipped.
        assertArrayEquals(expectedAcquiredRecord(10, 29, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(30, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(29, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());

        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_BATCH_COUNT).intValue() == 1,
            "In-flight batch count should be 1.");
        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_MESSAGE_COUNT).longValue() == 20,
            "In-flight message count should be 20.");
        assertEquals(20, sharePartitionMetrics.inFlightBatchMessageCount().sum());
    }

    @Test
    public void testAcquireMultipleRecordsWithOverlapAndNewBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(5);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 3, 5);

        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());

        // Add records from 0-9 offsets, 5-9 should be acquired and 0-4 should be ignored.
        records = memoryRecords(10);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 3, 5);

        assertArrayEquals(expectedAcquiredRecords(memoryRecords(5, 5), 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
    }

    @Test
    public void testAcquireSameBatchAgain() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(10, 5);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 3, 5);

        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 3, 0);

        // No records should be returned as the batch is already acquired.
        assertEquals(0, acquiredRecordsList.size());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Send subset of the same batch again, no records should be returned.
        MemoryRecords subsetRecords = memoryRecords(10, 2);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition, subsetRecords, 3, 0);

        // No records should be returned as the batch is already acquired.
        assertEquals(0, acquiredRecordsList.size());
        assertEquals(15, sharePartition.nextFetchOffset());
        // Cache shouldn't be tracking per offset records
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcquireWithEmptyFetchRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(
            sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                MAX_FETCH_RECORDS,
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData(MemoryRecords.EMPTY),
                FETCH_ISOLATION_HWM),
            0
        );

        assertEquals(0, acquiredRecordsList.size());
        assertEquals(0, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcquireWithBatchSizeAndSingleBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        // Single batch has more records than batch size. Hence, only a single batch exceeding the batch size
        // should be acquired.
        MemoryRecords records = memoryRecords(5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            2 /* Batch size */,
            10,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            5);

        assertArrayEquals(expectedAcquiredRecord(0, 4, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(4, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testAcquireWithBatchSizeAndMultipleBatches() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        // Create 4 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 2, 5).close();
        memoryRecordsBuilder(buffer, 10, 5).close();
        memoryRecordsBuilder(buffer, 15, 7).close();
        memoryRecordsBuilder(buffer, 22, 6).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            5 /* Batch size */,
            100,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            26 /* Gap of 3 records will also be added to first batch */);

        // Fetch expected records from 4 batches, but change the first expected record to include gap offsets.
        List<AcquiredRecords> expectedAcquiredRecords = expectedAcquiredRecords(records, 1);
        expectedAcquiredRecords.remove(0);
        expectedAcquiredRecords.addAll(0, expectedAcquiredRecord(2, 9, 1));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(28, sharePartition.nextFetchOffset());
        assertEquals(4, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().containsKey(2L));
        assertTrue(sharePartition.cachedState().containsKey(10L));
        assertTrue(sharePartition.cachedState().containsKey(15L));
        assertTrue(sharePartition.cachedState().containsKey(22L));
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(22L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(22L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(2L).batchDeliveryCount());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
        assertEquals(1, sharePartition.cachedState().get(22L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(2L).offsetState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(15L).offsetState());
        assertNull(sharePartition.cachedState().get(22L).offsetState());
    }

    @Test
    public void testAcquireWithBatchSizeAndMaxFetchRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        // Create 3 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 0, 5).close();
        memoryRecordsBuilder(buffer, 5, 15).close();
        memoryRecordsBuilder(buffer, 20, 15).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                2 /* Batch size */,
                10,
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            20);

        List<AcquiredRecords> expectedAcquiredRecords = expectedAcquiredRecords(records, 1);
        // The last batch should be ignored as it exceeds the max fetch records.
        expectedAcquiredRecords.remove(2);

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(4, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(5, sharePartition.cachedState().get(5L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(5L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
    }

    @Test
    public void testAcquireSingleBatchWithBatchSizeAndEndOffsetLargerThanBatchFirstOffset() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        sharePartition.updateCacheAndOffsets(8L);

        MemoryRecords records = memoryRecords(5, 10);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                5 /* Batch size */,
                100,
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            7 /* Acquisition of records starts post endOffset */);

        // Fetch expected single batch, but change the first offset as per endOffset.
        assertArrayEquals(expectedAcquiredRecord(8, 14, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().containsKey(8L));
    }

    @Test
    public void testAcquireWithBatchSizeAndEndOffsetLargerThanBatchFirstOffset()
        throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .build();
        sharePartition.updateCacheAndOffsets(4L);

        // Create 2 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 2, 8).close();
        memoryRecordsBuilder(buffer, 10, 7).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                5 /* Batch size */,
                100,
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            13 /* Acquisition of records starts post endOffset */);

        // Fetch expected records from 2 batches, but change the first batch's first offset as per endOffset.
        List<AcquiredRecords> expectedAcquiredRecords = expectedAcquiredRecords(records, 1);
        expectedAcquiredRecords.remove(0);
        expectedAcquiredRecords.addAll(0, expectedAcquiredRecord(4, 9, 1));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(17, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().containsKey(4L));
        assertTrue(sharePartition.cachedState().containsKey(10L));

        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_BATCH_COUNT).intValue() == 2,
            "In-flight batch count should be 2.");
        TestUtils.waitForCondition(() -> yammerMetricValue(SharePartitionMetrics.IN_FLIGHT_MESSAGE_COUNT).longValue() == 13,
            "In-flight message count should be 13.");
        assertEquals(13, sharePartitionMetrics.inFlightBatchMessageCount().sum());
        assertEquals(2, sharePartitionMetrics.inFlightBatchMessageCount().count());
        assertEquals(6, sharePartitionMetrics.inFlightBatchMessageCount().min());
        assertEquals(7, sharePartitionMetrics.inFlightBatchMessageCount().max());
    }

    @Test
    public void testAcquireBatchSkipWithBatchSizeAndEndOffsetLargerThanFirstBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        sharePartition.updateCacheAndOffsets(12L);

        // Create 2 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 2, 8).close();
        memoryRecordsBuilder(buffer, 10, 7).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                5 /* Batch size */,
                100,
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            5 /* Acquisition of records starts post endOffset */);

        // First batch should be skipped and fetch should result a single batch (second batch), but
        // change the first offset of acquired batch as per endOffset.
        assertArrayEquals(expectedAcquiredRecord(12, 16, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(17, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().containsKey(12L));
    }

    @Test
    public void testAcquireWithMaxInFlightRecordsAndTryAcquireNewBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .withMaxInflightRecords(20)
            .build();

        // Acquire records, all 10 records should be acquired as within maxInflightRecords limit.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500 /* Max fetch records */,
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData(memoryRecords(10), 0),
                FETCH_ISOLATION_HWM),
            10);
        // Validate all 10 records will be acquired as the maxInFlightRecords is 20.
        assertArrayEquals(expectedAcquiredRecord(0, 9, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());

        // Create 4 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 10, 5).close();
        memoryRecordsBuilder(buffer, 15, 10).close();
        memoryRecordsBuilder(buffer, 25, 5).close();
        memoryRecordsBuilder(buffer, 30, 2).close();

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        // Acquire records, should be acquired till maxInFlightRecords i.e. 20 records. As second batch
        // is ending at 24 offset, hence additional 15 records will be acquired.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500 /* Max fetch records */,
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData(records, 0),
                FETCH_ISOLATION_HWM),
            15);

        // Validate 2 batches are fetched one with 5 records and other till end of batch, third batch
        // should be skipped.
        assertArrayEquals(expectedAcquiredRecord(10, 24, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(25, sharePartition.nextFetchOffset());

        // Should not acquire any records as the share partition is at capacity and fetch offset is beyond
        // the end offset.
        fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500 /* Max fetch records */,
                25 /* Fetch Offset */,
                fetchPartitionData(memoryRecords(25, 10), 10),
                FETCH_ISOLATION_HWM),
            0);

        assertEquals(25, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcquireWithMaxInFlightRecordsAndReleaseLastOffset() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .withMaxInflightRecords(20)
            .build();

        // Create 4 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 10, 5).close();
        memoryRecordsBuilder(buffer, 15, 10).close();
        memoryRecordsBuilder(buffer, 25, 5).close();
        memoryRecordsBuilder(buffer, 30, 3).close();

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Acquire records, should be acquired till maxInFlightRecords i.e. 20 records till 29 offset.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500 /* Max fetch records */,
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData(records, 10),
                FETCH_ISOLATION_HWM),
            20);

        // Validate 3 batches are fetched and fourth batch should be skipped. Max in-flight records
        // limit is reached.
        assertArrayEquals(expectedAcquiredRecord(10, 29, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(30, sharePartition.nextFetchOffset());

        // Release middle batch.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(15, 19, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());
        // Validate the nextFetchOffset is updated to 15.
        assertEquals(15, sharePartition.nextFetchOffset());

        // The complete released batch should be acquired but not the last batch, starting at offset 30,
        // as the lastOffset is adjusted according to the endOffset.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500 /* Max fetch records */,
                15 /* Fetch Offset */,
                fetchPartitionData(records, 10),
                FETCH_ISOLATION_HWM),
            5);

        // Validate 1 batch is fetched, with 5 records till end of batch, last available batch should
        // not be acquired
        assertArrayEquals(expectedAcquiredRecords(15, 19, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(30, sharePartition.nextFetchOffset());

        // Release last offset of the acquired batch. Only 1 record should be released and later acquired.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(29, 29, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());
        // Validate the nextFetchOffset is updated to 29.
        assertEquals(29, sharePartition.nextFetchOffset());

        // Only the last record of the acquired batch should be acquired again.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500 /* Max fetch records */,
                29 /* Fetch Offset */,
                fetchPartitionData(records, 10),
                FETCH_ISOLATION_HWM),
            1);

        // Validate 1 record is acquired.
        assertArrayEquals(expectedAcquiredRecord(29, 29, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcquireWithMaxInFlightRecordsReleaseBatchAndAcquireSubsetRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .withMaxInflightRecords(20)
            .build();

        // Acquire records, should be acquired till maxInFlightRecords i.e. 25 records till 24 offset.
        fetchAcquiredRecords(sharePartition, memoryRecords(5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(5, 10), 10);
        fetchAcquiredRecords(sharePartition, memoryRecords(15, 10), 10);

        // Validate 3 batches are fetched and fourth batch should be skipped. Max in-flight records
        // limit is reached.
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(4, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(5, sharePartition.cachedState().get(5L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(5L).lastOffset());
        assertEquals(15, sharePartition.cachedState().get(15L).firstOffset());
        assertEquals(24, sharePartition.cachedState().get(15L).lastOffset());
        assertEquals(25, sharePartition.nextFetchOffset());

        // Release middle batch.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(5, 14, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());
        // Validate the nextFetchOffset is updated to 5.
        assertEquals(5, sharePartition.nextFetchOffset());

        // The complete released batch should be acquired but not any other batch as the lastOffset
        // is adjusted according to the minimum of fetched batch and endOffset.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500 /* Max fetch records */,
                5 /* Fetch Offset */,
                fetchPartitionData(memoryRecords(5, 10), 0),
                FETCH_ISOLATION_HWM),
            10);

        // Validate 1 batch is fetched, with 10 records till end of batch.
        assertArrayEquals(expectedAcquiredRecord(5, 14, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(25, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcquireWithMaxInFlightRecordsReleaseBatchAndAcquireSubsetRecordsOverlap() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .withMaxInflightRecords(20)
            .build();

        // Acquire records, should be acquired till maxInFlightRecords i.e. 25 records till 24 offset.
        fetchAcquiredRecords(sharePartition, memoryRecords(5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(15, 10), 10);

        // Validate 4 batches are fetched and fourth batch should be skipped. Max in-flight records
        // limit is reached.
        assertEquals(4, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(4, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(5, sharePartition.cachedState().get(5L).firstOffset());
        assertEquals(9, sharePartition.cachedState().get(5L).lastOffset());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(15, sharePartition.cachedState().get(15L).firstOffset());
        assertEquals(24, sharePartition.cachedState().get(15L).lastOffset());
        assertEquals(25, sharePartition.nextFetchOffset());

        // Release only 1 middle batch.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());
        // Validate the nextFetchOffset is updated to 5.
        assertEquals(5, sharePartition.nextFetchOffset());

        // Adjust the max fetch records to 6 so it's just 1 record more than the released batch size.
        // This shall not impact the acquired records as only the released batch should be acquired.
        // However, this previously caused an issue where the subset of records were acquired from the
        // next batch due to incorrect calculation.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                6 /* Max fetch records */,
                5 /* Fetch Offset */,
                fetchPartitionData(memoryRecords(5, 5), 0),
                FETCH_ISOLATION_HWM),
            5);

        // Validate 1 batch is fetched, with 5 records till end of batch.
        assertArrayEquals(expectedAcquiredRecord(5, 9, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(25, sharePartition.nextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetInitialState() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        assertEquals(0, sharePartition.nextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWithCachedStateAcquired() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        fetchAcquiredRecords(sharePartition, memoryRecords(5), 2, 5);
        assertEquals(5, sharePartition.nextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWithFindAndCachedStateEmpty() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        sharePartition.updateFindNextFetchOffset(true);
        assertTrue(sharePartition.findNextFetchOffset());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertFalse(sharePartition.findNextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWithFindAndCachedState() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        sharePartition.updateFindNextFetchOffset(true);
        assertTrue(sharePartition.findNextFetchOffset());

        fetchAcquiredRecords(sharePartition, memoryRecords(5), 5);

        assertEquals(5, sharePartition.nextFetchOffset());
        assertFalse(sharePartition.findNextFetchOffset());
    }

    @Test
    public void testCanAcquireRecordsWithEmptyCache() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightRecords(1).build();
        assertTrue(sharePartition.canAcquireRecords());
    }

    @Test
    public void testCanAcquireRecordsWithCachedDataAndLimitNotReached() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightRecords(6)
            .withState(SharePartitionState.ACTIVE)
            .build();
        fetchAcquiredRecords(sharePartition, memoryRecords(5), 5);
        // Limit not reached as only 6 in-flight records is the limit.
        assertTrue(sharePartition.canAcquireRecords());
    }

    @Test
    public void testCanAcquireRecordsWithCachedDataAndLimitReached() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightRecords(1)
            .withState(SharePartitionState.ACTIVE)
            .build();
        fetchAcquiredRecords(sharePartition, memoryRecords(5), 5);
        // Limit reached as only one in-flight record is the limit.
        assertFalse(sharePartition.canAcquireRecords());
    }

    @Test
    public void testMaybeAcquireAndReleaseFetchLock() {
        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(-1L, 0L, Optional.empty());
        Mockito.doReturn(new OffsetResultHolder(Optional.of(timestampAndOffset), Optional.empty())).
            when(replicaManager).fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        Time time = mock(Time.class);
        when(time.hiResClockMs())
            .thenReturn(100L) // for tracking loadTimeMs
            .thenReturn(110L) // for time when lock is acquired
            .thenReturn(120L) // for time when lock is released
            .thenReturn(140L) // for subsequent lock acquire
            .thenReturn(170L); // for subsequent lock release
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withReplicaManager(replicaManager)
            .withTime(time)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .build();

        Uuid fetchId = Uuid.randomUuid();

        sharePartition.maybeInitialize();
        assertTrue(sharePartition.maybeAcquireFetchLock(fetchId));
        // Lock cannot be acquired again, as already acquired.
        assertFalse(sharePartition.maybeAcquireFetchLock(fetchId));
        // Release the lock.
        sharePartition.releaseFetchLock(fetchId);

        assertEquals(1, sharePartitionMetrics.fetchLockTimeMs().count());
        assertEquals(10, sharePartitionMetrics.fetchLockTimeMs().sum());
        assertEquals(1, sharePartitionMetrics.fetchLockRatio().count());
        // Since first request didn't have any lock idle wait time, the ratio should be 1.
        assertEquals(100, sharePartitionMetrics.fetchLockRatio().mean());

        // Lock can be acquired again.
        assertTrue(sharePartition.maybeAcquireFetchLock(fetchId));
        // Release lock to update metrics and verify.
        sharePartition.releaseFetchLock(fetchId);

        assertEquals(2, sharePartitionMetrics.fetchLockTimeMs().count());
        assertEquals(40, sharePartitionMetrics.fetchLockTimeMs().sum());
        assertEquals(2, sharePartitionMetrics.fetchLockRatio().count());
        // Since the second request had 20ms of idle wait time, the ratio should be 0.6 and mean as 0.8.
        assertEquals(80, sharePartitionMetrics.fetchLockRatio().mean());
    }

    @Test
    public void testRecordFetchLockRatioMetric() {
        Time time = mock(Time.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withTime(time)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .build();

        // Acquired time and last lock acquisition time is 0;
        sharePartition.recordFetchLockRatioMetric(0);
        assertEquals(1, sharePartitionMetrics.fetchLockRatio().count());
        assertEquals(100, sharePartitionMetrics.fetchLockRatio().mean());

        when(time.hiResClockMs())
            .thenReturn(10L) // for time when lock is acquired
            .thenReturn(80L) // for time when lock is released
            .thenReturn(160L); // to update lock idle duration while acquiring lock again.

        Uuid fetchId = Uuid.randomUuid();
        assertTrue(sharePartition.maybeAcquireFetchLock(fetchId));
        sharePartition.releaseFetchLock(fetchId);
        // Acquired time is 70 but last lock acquisition time was still 0, as it's the first request
        // when last acquisition time was recorded. The last acquisition time should be updated to 80.
        assertEquals(2, sharePartitionMetrics.fetchLockRatio().count());
        assertEquals(100, sharePartitionMetrics.fetchLockRatio().mean());

        assertTrue(sharePartition.maybeAcquireFetchLock(fetchId));
        // Update metric again with 0 as acquire time and 80 as idle duration ms.
        sharePartition.recordFetchLockRatioMetric(0);
        assertEquals(3, sharePartitionMetrics.fetchLockRatio().count());
        // Mean should be (100+100+1)/3 = 67, as when idle duration is 80, the ratio should be 1.
        assertEquals(67, sharePartitionMetrics.fetchLockRatio().mean());

        // Update metric again with 10 as acquire time and 80 as idle duration ms.
        sharePartition.recordFetchLockRatioMetric(10);
        assertEquals(4, sharePartitionMetrics.fetchLockRatio().count());
        // Mean should be (100+100+1+11)/4 = 53, as when idle time is 80 and acquire time 10, the ratio should be 11.
        assertEquals(53, sharePartitionMetrics.fetchLockRatio().mean());
    }

    @Test
    public void testAcknowledgeSingleRecordBatch() {
        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withReplicaManager(replicaManager)
            .withState(SharePartitionState.ACTIVE)
            .build();

        MemoryRecords records1 = memoryRecords(1);
        MemoryRecords records2 = memoryRecords(1, 1);

        // Another batch is acquired because if there is only 1 batch, and it is acknowledged, the batch will be removed from cachedState
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records1, 1);
        assertEquals(1, acquiredRecordsList.size());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records2, 1);
        assertEquals(1, acquiredRecordsList.size());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(1, 1, List.of(AcknowledgeType.ACCEPT.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        assertEquals(2, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(1L).batchState());
        assertEquals(1, sharePartition.cachedState().get(1L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(1L).offsetState());
        // Should not invoke completeDelayedShareFetchRequest as the first offset is not acknowledged yet.
        Mockito.verify(replicaManager, Mockito.times(0))
            .completeDelayedShareFetchRequest(new DelayedShareFetchGroupKey(GROUP_ID, TOPIC_ID_PARTITION));
        assertEquals(1, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcknowledgeMultipleRecordBatch() {
        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withReplicaManager(replicaManager)
            .withState(SharePartitionState.ACTIVE)
            .build();
        MemoryRecords records = memoryRecords(5, 10);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 10);
        assertEquals(1, acquiredRecordsList.size());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(5, 14, List.of(AcknowledgeType.ACCEPT.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.cachedState().size());
        // Should invoke completeDelayedShareFetchRequest as the start offset is moved.
        Mockito.verify(replicaManager, Mockito.times(1))
            .completeDelayedShareFetchRequest(new DelayedShareFetchGroupKey(GROUP_ID, TOPIC_ID_PARTITION));
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcknowledgeMultipleRecordBatchWithGapOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(5, 2);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(10, 5);
        // Gap from 15-17 offsets.
        recordsBuilder.appendWithOffset(18, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records1, 2);

        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records2, 9);

        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(19, sharePartition.nextFetchOffset());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(
                new ShareAcknowledgementBatch(5, 6, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(10, 18, List.of(
                    AcknowledgeType.RELEASE.id, AcknowledgeType.RELEASE.id, AcknowledgeType.RELEASE.id,
                    AcknowledgeType.RELEASE.id, AcknowledgeType.RELEASE.id, ACKNOWLEDGE_TYPE_GAP_ID,
                    ACKNOWLEDGE_TYPE_GAP_ID, ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.ACCEPT.id
                ))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // Out of the 11 records acquired, 3 are GAP and 1 is ACCEPTED, which are Terminal. Thus deliveryCompleteCount
        // will be 4
        assertEquals(4, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcknowledgeMultipleSubsetRecordBatchWithGapOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(5, 2);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(10, 2);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records1, 2);

        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records2, 11);

        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Acknowledging over subset of both batch with subset of gap offsets.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(6, 18, List.of(
                AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id,
                AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id,
                ACKNOWLEDGE_TYPE_GAP_ID, ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.ACCEPT.id,
                ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.ACCEPT.id, ACKNOWLEDGE_TYPE_GAP_ID,
                AcknowledgeType.ACCEPT.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        assertEquals(21, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // After acknowledgements, records at offsets 6, and 10 -> 18 are in Terminal state.
        assertEquals(10, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcknowledgeOutOfRangeCachedData() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        // Acknowledge a batch when cache is empty.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(0, 15, List.of(AcknowledgeType.REJECT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRecordStateException.class, ackResult);

        MemoryRecords records = memoryRecords(5, 5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 5);

        assertEquals(1, acquiredRecordsList.size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(20, 25, List.of(AcknowledgeType.REJECT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRequestException.class, ackResult);
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcknowledgeOutOfRangeCachedDataFirstBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        // Create data for the batch with offsets 0-4.
        MemoryRecords records = memoryRecords(5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 5);

        assertEquals(1, acquiredRecordsList.size());

        // Create data for the batch with offsets 20-24.
        records = memoryRecords(20, 5);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 5);

        assertEquals(1, acquiredRecordsList.size());

        // Acknowledge a batch when first batch violates the range.
        List<ShareAcknowledgementBatch> acknowledgeBatches = List.of(
            new ShareAcknowledgementBatch(0, 10, List.of(AcknowledgeType.ACCEPT.id)),
            new ShareAcknowledgementBatch(20, 24, List.of(AcknowledgeType.ACCEPT.id)));
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID, acknowledgeBatches);
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRequestException.class, ackResult);

        // Create data for the batch with offsets 5-10.
        records = memoryRecords(5, 6);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 6);

        assertEquals(1, acquiredRecordsList.size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Previous failed acknowledge request should succeed now.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID, acknowledgeBatches);
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());
        // After the acknowledgments are successful, the cache is updated. Since all record batches are in Terminal state,
        // the cache is cleared and thus deliveryCompleteCount is set as 0.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcknowledgeWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(5, 5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 5);

        assertEquals(1, acquiredRecordsList.size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            "member-2",
            List.of(new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.REJECT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRecordStateException.class, ackResult);
    }

    @Test
    public void testAcknowledgeWhenOffsetNotAcquired() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(5, 5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 5);

        assertEquals(1, acquiredRecordsList.size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        // All records are RELEASED, so none of them moved to a Terminal state.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Acknowledge the same batch again but with ACCEPT type.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.ACCEPT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRecordStateException.class, ackResult);

        // Re-acquire the same batch and then acknowledge subset with ACCEPT type.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 5);

        assertEquals(1, acquiredRecordsList.size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(6, 8, List.of(AcknowledgeType.REJECT.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Re-acknowledge the subset batch with REJECT type.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(6, 8, List.of(AcknowledgeType.REJECT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRecordStateException.class, ackResult);
        assertEquals(3, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcknowledgeRollbackWithFullBatchError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(10, 5);
        MemoryRecords records3 = memoryRecords(15, 5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records1, 5);

        assertEquals(1, acquiredRecordsList.size());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records2, 5);

        assertEquals(1, acquiredRecordsList.size());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records3, 5);

        assertEquals(1, acquiredRecordsList.size());
        // Cached data with offset 5-19 should exist.
        assertEquals(3, sharePartition.cachedState().size());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(
                new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(10, 14, List.of(AcknowledgeType.ACCEPT.id)),
                new ShareAcknowledgementBatch(15, 19, List.of(AcknowledgeType.ACCEPT.id)),
                // Add another batch which should fail the request.
                new ShareAcknowledgementBatch(15, 19, List.of(AcknowledgeType.ACCEPT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRecordStateException.class, ackResult);

        // Check the state of the cache. The state should be acquired itself.
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcknowledgeRollbackWithSubsetError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(10, 5);
        MemoryRecords records3 = memoryRecords(15, 5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records1, 5);

        assertEquals(1, acquiredRecordsList.size());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records2, 5);

        assertEquals(1, acquiredRecordsList.size());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records3, 5);

        assertEquals(1, acquiredRecordsList.size());
        // Cached data with offset 5-19 should exist.
        assertEquals(3, sharePartition.cachedState().size());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(
                new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(10, 14, List.of(AcknowledgeType.ACCEPT.id)),
                new ShareAcknowledgementBatch(15, 19, List.of(AcknowledgeType.ACCEPT.id)),
                // Add another batch which should fail the request.
                new ShareAcknowledgementBatch(16, 19, List.of(AcknowledgeType.ACCEPT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRecordStateException.class, ackResult);

        // Check the state of the cache. The state should be acquired itself.
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        // Though the last batch is subset but the offset state map will not be exploded as the batch is
        // not in acquired state due to previous batch acknowledgement.
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquireReleasedRecord() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(10, 5);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 5);

        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(12, 13, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Send the same fetch request batch again but only 2 offsets should come as acquired.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 2);

        assertArrayEquals(expectedAcquiredRecords(12, 13, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquireReleasedRecordMultipleBatches() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(10, 5);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(15, 5);
        // Third fetch request with 5 records starting from offset 23, gap of 3 offsets.
        MemoryRecords records3 = memoryRecords(23, 5);
        // Fourth fetch request with 5 records starting from offset 28.
        MemoryRecords records4 = memoryRecords(28, 5);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records1, 5);

        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records2, 5);

        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records3, 5);

        assertArrayEquals(expectedAcquiredRecords(records3, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(28, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records4, 5);

        assertArrayEquals(expectedAcquiredRecords(records4, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(33, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(23L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(28L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(15L).offsetState());
        assertNull(sharePartition.cachedState().get(23L).offsetState());
        assertNull(sharePartition.cachedState().get(28L).offsetState());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(12, 30, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(4, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertNull(sharePartition.cachedState().get(15L).offsetState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(23L).batchState());
        assertNull(sharePartition.cachedState().get(23L).offsetState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(23L).batchMemberId());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(28L).batchState());
        assertNotNull(sharePartition.cachedState().get(28L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(28L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(29L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(30L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(31L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(32L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(28L).offsetState());

        // Send next batch from offset 12, only 3 records should be acquired.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records1, 3);

        assertArrayEquals(expectedAcquiredRecords(12, 14, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Though record2 batch exists to acquire but send batch record3, it should be acquired but
        // next fetch offset should not move.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records3, 5);

        assertArrayEquals(expectedAcquiredRecords(records3, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Acquire partial records from batch 2.
        MemoryRecords subsetRecords = memoryRecords(17, 2);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition, subsetRecords, 2);

        assertArrayEquals(expectedAcquiredRecords(17, 18, 2).toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(15, sharePartition.nextFetchOffset());

        // Acquire partial records from record 4 to further test if the next fetch offset move
        // accordingly once complete record 2 is also acquired.
        subsetRecords = memoryRecords(28, 1);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition, subsetRecords, 1);

        assertArrayEquals(expectedAcquiredRecords(28, 28, 2).toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(15, sharePartition.nextFetchOffset());

        // Try to acquire complete record 2 though it's already partially acquired, the next fetch
        // offset should move.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records2, 3);

        // Offset 15,16 and 19 should be acquired.
        List<AcquiredRecords> expectedAcquiredRecords = expectedAcquiredRecords(15, 16, 2);
        expectedAcquiredRecords.addAll(expectedAcquiredRecords(19, 19, 2));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(29, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcquireGapAtBeginningAndRecordsFetchedFromGap() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(31L, 40L, RecordState.ARCHIVED.id, (short) 1)
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(20, sharePartition.deliveryCompleteCount());

        // All records fetched are part of the gap. The gap is from 11 to 20, fetched offsets are 11 to 15.
        MemoryRecords records = memoryRecords(11, 5);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 5);

        assertArrayEquals(expectedAcquiredRecord(11, 15, 1).toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(40, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(16, sharePartition.nextFetchOffset());
        assertEquals(20, sharePartition.deliveryCompleteCount());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        // After records are acquired, the persisterReadResultGapWindow should be updated
        assertEquals(16, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());
    }

    @Test
    public void testAcquireGapAtBeginningAndFetchedRecordsOverlapInFlightBatches() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(31L, 40L, RecordState.ARCHIVED.id, (short) 1)
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(20, sharePartition.deliveryCompleteCount());

        // Fetched offsets overlap the inFlight batches. The gap is from 11 to 20, but fetched records are from 11 to 25.
        MemoryRecords records = memoryRecords(11, 15);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 10);

        assertArrayEquals(expectedAcquiredRecord(11, 20, 1).toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(40, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(41, sharePartition.nextFetchOffset());
        assertEquals(20, sharePartition.deliveryCompleteCount());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        // After records are acquired, the persisterReadResultGapWindow should be updated
        assertEquals(21, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());
    }

    @Test
    public void testAcquireGapAtBeginningAndFetchedRecordsOverlapInFlightAvailableBatches() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.AVAILABLE.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(31L, 40L, RecordState.ARCHIVED.id, (short) 1)
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(10, sharePartition.deliveryCompleteCount());

        // Fetched offsets overlap the inFlight batches. The gap is from 11 to 20, but fetched records are from 11 to 25.
        MemoryRecords records = memoryRecords(11, 15);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 15);

        // The gap from 11 to 20 will be acquired. Since the next batch is AVAILABLE, and we records fetched from replica manager
        // overlap with the next batch, some records from the next batch will also be acquired
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(11, 20, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(21, 21, 3));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(22, 22, 3));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(23, 23, 3));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(24, 24, 3));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(25, 25, 3));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(40, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(26, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.deliveryCompleteCount());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        // After records are acquired, the persisterReadResultGapWindow should be updated
        assertEquals(26, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());
    }

    @Test
    public void testAcquireWhenCachedStateContainsGapsAndRecordsFetchedFromNonGapOffset() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(11L, 20L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(31L, 40L, RecordState.ARCHIVED.id, (short) 1) // There is a gap from 21-30
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(10, sharePartition.deliveryCompleteCount());

        // Fetched records are part of inFlightBatch 11-20 with state AVAILABLE. Fetched offsets also overlap the
        // inFlight batches. The gap is from 11 to 20, but fetched records are from 11 to 25.
        MemoryRecords records = memoryRecords(11, 15);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 15);

        // 2 different batches will be acquired this time (11-20 and 21-25). The first batch will have delivery count 3
        // as previous deliveryCount was 2. The second batch will have delivery count 1 as it is acquired for the first time.
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(11, 20, 3));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(21, 25, 1));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(40, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(26, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.deliveryCompleteCount());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        // After records are acquired, the persisterReadResultGapWindow should be updated
        assertEquals(26, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());
    }

    @Test
    public void testAcquireGapAtBeginningAndFetchedRecordsOverlapMultipleInFlightBatches() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(41L, 50L, RecordState.AVAILABLE.id, (short) 1), // There is a gap from 31 to 40
                        new PersisterStateBatch(61L, 70L, RecordState.ARCHIVED.id, (short) 1), // There is a gap from 51 to 60
                        new PersisterStateBatch(81L, 90L, RecordState.AVAILABLE.id, (short) 1) // There is a gap from 71 to 80
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(20, sharePartition.deliveryCompleteCount());

        MemoryRecords records = memoryRecords(11, 75);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 55);

        // Acquired batches will contain the following ->
        // 1. 11-20 (gap offsets)
        // 2. 31-40 (gap offsets)
        // 3. 41-50 (AVAILABLE batch in cachedState)
        // 4. 51-60 (gap offsets)
        // 5. 71-80 (gap offsets)
        // 6. 81-85 (AVAILABLE batch in cachedState). These will be acquired as separate batches because we are breaking a batch in the cachedState
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(11, 20, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(31, 40, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(41, 50, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(51, 60, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(71, 80, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(81, 81, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(82, 82, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(83, 83, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(84, 84, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(85, 85, 2));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(90, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(86, sharePartition.nextFetchOffset());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        assertEquals(20, sharePartition.deliveryCompleteCount());

        // After records are acquired, the persisterReadResultGapWindow should be updated
        assertEquals(86, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(90, persisterReadResultGapWindow.endOffset());
    }

    @Test
    public void testAcquireGapAtBeginningAndFetchedRecordsEndJustBeforeGap() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.AVAILABLE.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(41L, 50L, RecordState.ACKNOWLEDGED.id, (short) 1), // There is a gap from 31 to 40
                        new PersisterStateBatch(61L, 70L, RecordState.ARCHIVED.id, (short) 1) // There is a gap from 51 to 60
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(20, sharePartition.deliveryCompleteCount());

        MemoryRecords records = memoryRecords(11, 20);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 20);

        // Acquired batches will contain the following ->
        // 1. 11-20 (gap offsets)
        // 2. 21-30 (AVAILABLE batch in cachedState)
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(11, 20, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(21, 30, 3));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(70, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(31, sharePartition.nextFetchOffset());
        assertEquals(20, sharePartition.deliveryCompleteCount());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        // After records are acquired, the persisterReadResultGapWindow should be updated
        assertEquals(31, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(70, persisterReadResultGapWindow.endOffset());
    }

    @Test
    public void testAcquireGapAtBeginningAndFetchedRecordsIncludeGapOffsetsAtEnd() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(41L, 50L, RecordState.AVAILABLE.id, (short) 1), // There is a gap from 31 to 40
                        new PersisterStateBatch(61L, 70L, RecordState.ARCHIVED.id, (short) 1), // There is a gap from 51 to 60
                        new PersisterStateBatch(81L, 90L, RecordState.AVAILABLE.id, (short) 1) // There is a gap from 71 to 80
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(20, sharePartition.deliveryCompleteCount());

        MemoryRecords records = memoryRecords(11, 65);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 45);

        // Acquired batches will contain the following ->
        // 1. 11-20 (gap offsets)
        // 2. 31-40 (gap offsets)
        // 3. 41-50 (AVAILABLE batch in cachedState)
        // 4. 51-60 (gap offsets)
        // 5. 71-75 (gap offsets). The gap is from 71 to 80, but the fetched records end at 75. These gap offsets will be acquired as a single batch
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(11, 20, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(31, 40, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(41, 50, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(51, 60, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(71, 75, 1));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(90, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(76, sharePartition.nextFetchOffset());
        assertEquals(20, sharePartition.deliveryCompleteCount());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        // After records are acquired, the persisterReadResultGapWindow should be updated
        assertEquals(76, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(90, persisterReadResultGapWindow.endOffset());
    }


    @Test
    public void testAcquireWhenRecordsFetchedFromGapAndMaxFetchRecordsIsExceeded() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(11L, 20L, RecordState.ACKNOWLEDGED.id, (short) 2),
                        new PersisterStateBatch(31L, 40L, RecordState.ARCHIVED.id, (short) 1) // There is a gap from 21-30
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        // After initialization is successful, the startOffset can move ahead because the very first batch in cached state
        // is in a Terminal state (11 -> 20 ACKNOWLEDGED). Thus, start offset will move past it and the only batch remaining
        // in cached state will be (31 -> 40) ARCHIVED. This, instead of 20, deliveryCompleteCount is 10.
        assertEquals(10, sharePartition.deliveryCompleteCount());

        // Creating 3 batches of records with a total of 8 records
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 21, 3).close();
        memoryRecordsBuilder(buffer, 24, 3).close();
        memoryRecordsBuilder(buffer, 27, 2).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                6, // maxFetchRecords is less than the number of records fetched
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            6);

        // Since max fetch records (6) is less than the number of records fetched (8), only 6 records will be acquired
        assertArrayEquals(expectedAcquiredRecord(21, 26, 1).toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(21, sharePartition.startOffset());
        assertEquals(40, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(27, sharePartition.nextFetchOffset());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        assertEquals(27, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());
    }

    @Test
    public void testAcquireMaxFetchRecordsExceededAfterAcquiringGaps() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.AVAILABLE.id, (short) 2), // There is a gap from 11-20
                        new PersisterStateBatch(31L, 40L, RecordState.ARCHIVED.id, (short) 1)
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(10, sharePartition.deliveryCompleteCount());

        // Creating 3 batches of records with a total of 8 records
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 11, 10).close();
        memoryRecordsBuilder(buffer, 21, 10).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                8, // maxFetchRecords is less than the number of records fetched
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            10);

        assertArrayEquals(expectedAcquiredRecord(11, 20, 1).toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(40, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(21, sharePartition.nextFetchOffset());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        assertEquals(21, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());
    }

    @Test
    public void testAcquireMaxFetchRecordsExceededBeforeAcquiringGaps() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(11L, 20L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(31L, 40L, RecordState.AVAILABLE.id, (short) 1) // There is a gap from 21-30
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Creating 3 batches of records with a total of 8 records
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 11, 10).close();
        memoryRecordsBuilder(buffer, 21, 20).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                8, // maxFetchRecords is less than the number of records fetched
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            10);

        assertArrayEquals(expectedAcquiredRecord(11, 20, 3).toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(40, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(21, sharePartition.nextFetchOffset());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        assertEquals(21, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());
    }

    @Test
    public void testAcquireWhenRecordsFetchedFromGapAndPartitionContainsNaturalGaps() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 10L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 20L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 10 to 14
                        new PersisterStateBatch(30L, 40L, RecordState.ARCHIVED.id, (short) 1) // There is a gap from 21-29
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(17, sharePartition.deliveryCompleteCount());

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 10, 11).close();
        memoryRecordsBuilder(buffer, 30, 21).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 24);

        // Acquired batches will contain the following ->
        // 1. 10-14 (gap offsets)
        // 2. 21-29 (gap offsets)
        // 3. 41-50 (gap offsets)
        // The offsets fetched from partition include a natural gap from 21 to 29. The cached state also contain the
        // gap from 21 to 29. But since the broker does not parse the fetched records, the broker is not aware of this
        // natural gap. In this case, the gap will be acquired, and it is the client's responsibility to inform the
        // broker about this gap.
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(10, 14, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(21, 29, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(41, 50, 1));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(50, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(51, sharePartition.nextFetchOffset());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNull(persisterReadResultGapWindow);
    }

    @Test
    public void testAcquireCachedStateInitialGapMatchesWithActualPartitionGap() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(41L, 50L, RecordState.ARCHIVED.id, (short) 1) // There is a gap from 31-40
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(20, sharePartition.deliveryCompleteCount());

        // Creating 2 batches starting from 21, such that there is a natural gap from 11 to 20
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 21, 15).close();
        memoryRecordsBuilder(buffer, 36, 25).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 20);

        // Acquired batches will contain the following ->
        // 1. 31-40 (gap offsets)
        // 2. 51-60 (new offsets)
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(31, 40, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(51, 60, 1));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(60, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(61, sharePartition.nextFetchOffset());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNull(persisterReadResultGapWindow);
    }

    @Test
    public void testAcquireCachedStateInitialGapOverlapsWithActualPartitionGap() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(41L, 50L, RecordState.ARCHIVED.id, (short) 1) // There is a gap from 31-40
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(20, sharePartition.deliveryCompleteCount());

        // Creating 2 batches starting from 16, such that there is a natural gap from 11 to 15
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 16, 20).close();
        memoryRecordsBuilder(buffer, 36, 25).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 25);

        // Acquired batches will contain the following ->
        // 1. 16-20 (gap offsets)
        // 2. 31-40 (gap offsets)
        // 3. 51-60 (new offsets)
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(16, 20, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(31, 40, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(51, 60, 1));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(16, sharePartition.startOffset());
        assertEquals(60, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(61, sharePartition.nextFetchOffset());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNull(persisterReadResultGapWindow);
    }

    @Test
    public void testAcquireCachedStateGapInBetweenOverlapsWithActualPartitionGap() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(41L, 50L, RecordState.ARCHIVED.id, (short) 1) // There is a gap from 31-40
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(20, sharePartition.deliveryCompleteCount());

        // Creating 3 batches starting from 11, such that there is a natural gap from 26 to 30
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 11, 10).close();
        memoryRecordsBuilder(buffer, 21, 15).close();
        memoryRecordsBuilder(buffer, 41, 20).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 30);

        // Acquired batches will contain the following ->
        // 1. 11-20 (gap offsets)
        // 2. 31-40 (gap offsets)
        // 3. 51-60 (new offsets)
        // The entire gap of 31 to 40 will be acquired even when the fetched records only contain offsets 31 to 36 because
        // we rely on the client to inform the broker about these natural gaps in the partition log
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(11, 20, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(31, 40, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(51, 60, 1));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(60, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(61, sharePartition.nextFetchOffset());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNull(persisterReadResultGapWindow);
    }

    @Test
    public void testAcquireWhenRecordsFetchedAfterGapsAreFetched() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(11L, 20L, RecordState.ACKNOWLEDGED.id, (short) 2),
                        new PersisterStateBatch(31L, 40L, RecordState.ARCHIVED.id, (short) 1) // There is a gap from 21 to 30
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(10, sharePartition.deliveryCompleteCount());

        // Fetched records are from 21 to 35
        MemoryRecords records = memoryRecords(21, 15);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 10);

        // Since the gap if only from 21 to 30 and the next batch is ARCHIVED, only 10 gap offsets will be acquired as a single batch
        assertArrayEquals(expectedAcquiredRecord(21, 30, 1).toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(21, sharePartition.startOffset());
        assertEquals(40, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(41, sharePartition.nextFetchOffset());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        assertEquals(31, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());

        // Fetching from the  nextFetchOffset so that endOffset moves ahead
        records = memoryRecords(41, 15);

        acquiredRecordsList = fetchAcquiredRecords(sharePartition, records, 15);

        assertArrayEquals(expectedAcquiredRecord(41, 55, 1).toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(21, sharePartition.startOffset());
        assertEquals(55, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(56, sharePartition.nextFetchOffset());

        // Since the endOffset is now moved ahead, the persisterReadResultGapWindow should be empty
        persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNull(persisterReadResultGapWindow);
    }

    @Test
    public void testAcquisitionLockForAcquiringSingleRecord() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .build();
        fetchAcquiredRecords(sharePartition, memoryRecords(1), 1);

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.nextFetchOffset() == 0 &&
                        sharePartition.cachedState().get(0L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(0L).batchDeliveryCount() == 1 &&
                        sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask() == null &&
                        sharePartition.timer().size() == 0,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(0L, List.of())));

        assertEquals(1, sharePartitionMetrics.acquisitionLockTimeoutPerSec().count());
        assertTrue(sharePartitionMetrics.acquisitionLockTimeoutPerSec().meanRate() > 0);
        // Since the delivery attempts are not exhausted, the deliveryCompleteCount will still be 0 as the state
        // of the record is AVAILABLE.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockForAcquiringMultipleRecords() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .build();
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0
                        && sharePartition.nextFetchOffset() == 10
                        && sharePartition.cachedState().get(10L).batchState() == RecordState.AVAILABLE
                        && sharePartition.cachedState().get(10L).batchDeliveryCount() == 1
                        && sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(10L, List.of())));

        assertEquals(5, sharePartitionMetrics.acquisitionLockTimeoutPerSec().count());
        assertTrue(sharePartitionMetrics.acquisitionLockTimeoutPerSec().meanRate() > 0);
        // Since the delivery attempts are not exhausted, the deliveryCompleteCount will still be 0 as the state
        // of the record is AVAILABLE.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockForAcquiringMultipleRecordsWithOverlapAndNewBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .withSharePartitionMetrics(sharePartitionMetrics)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5), 5);

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Add records from 0-9 offsets, 5-9 should be acquired and 0-4 should be ignored.
        fetchAcquiredRecords(sharePartition, memoryRecords(10), 5);

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Allowing acquisition lock to expire. The acquisition lock timeout will cause release of records for all the acquired records.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 0 &&
                        sharePartition.cachedState().get(0L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(5L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask() == null &&
                        sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(0L, List.of(), 5L, List.of())));

        assertEquals(10, sharePartitionMetrics.acquisitionLockTimeoutPerSec().count());
        assertTrue(sharePartitionMetrics.acquisitionLockTimeoutPerSec().meanRate() > 0);
        // Since the delivery attempts are not exhausted, the deliveryCompleteCount will still be 0 as the state
        // of the record is AVAILABLE.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockForAcquiringSameBatchAgain() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 10 &&
                        sharePartition.cachedState().get(10L).batchState() == RecordState.AVAILABLE,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(10L, List.of())));

        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Acquire the same batch again.
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        // Acquisition lock timeout task should be created on re-acquire action.
        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingSingleRecordBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(1), 1);

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(0, 0, List.of(AcknowledgeType.RELEASE.id))));

        assertNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());

        // Allowing acquisition lock to expire. This will not cause any change to cached state map since the batch is already acknowledged.
        // Hence, the acquisition lock timeout task would be cancelled already.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 0 &&
                        sharePartition.cachedState().get(0L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(0L).batchDeliveryCount() == 1 &&
                        sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(0L, List.of())));
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleRecordBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 10), 10);

        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(5, 14, List.of(AcknowledgeType.RELEASE.id))));

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire. This will not cause any change to cached state map since the batch is already acknowledged.
        // Hence, the acquisition lock timeout task would be cancelled already.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 5 &&
                        sharePartition.cachedState().get(5L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(5L).batchDeliveryCount() == 1 &&
                        sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of())));
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleRecordBatchWithGapOffsets() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();
        MemoryRecords records1 = memoryRecords(5, 2);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(10, 5);
        // Gap from 15-17 offsets.
        recordsBuilder.appendWithOffset(18, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();
        MemoryRecords records3 = memoryRecords(1, 2);

        fetchAcquiredRecords(sharePartition, records3, 2);

        assertNotNull(sharePartition.cachedState().get(1L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        fetchAcquiredRecords(sharePartition, records1, 2);

        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        fetchAcquiredRecords(sharePartition, records2, 9);

        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());

        sharePartition.acknowledge(MEMBER_ID,
                // Do not send gap offsets to verify that they are ignored and accepted as per client ack.
                List.of(new ShareAcknowledgementBatch(5, 18, List.of(AcknowledgeType.ACCEPT.id))));

        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(1L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());
        // All the acquired records except 1 -> 2 have been acknowledged.
        assertEquals(11, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire. The acquisition lock timeout will cause release of records for batch with starting offset 1.
        // Since, other records have been acknowledged.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 1 &&
                        sharePartition.cachedState().get(1L).batchAcquisitionLockTimeoutTask() == null &&
                        sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask() == null &&
                        sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(1L, List.of(), 5L, List.of(), 10L, List.of())));

        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(1L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(11, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockForAcquiringSubsetBatchAgain() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(10, 8), 8);

        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 10 &&
                        sharePartition.cachedState().size() == 1 &&
                        sharePartition.cachedState().get(10L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(10L, List.of())));

        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Acquire subset of records again.
        fetchAcquiredRecords(sharePartition, memoryRecords(12, 3), 3);

        // Acquisition lock timeout task should be created only on offsets which have been acquired again.
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());

        // Allowing acquisition lock to expire for the acquired subset batch.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> {
                    Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
                    expectedOffsetStateMap.put(10L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(11L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 2, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 2, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 2, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(15L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(16L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(17L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));

                    return sharePartition.timer().size() == 0 &&
                            sharePartition.nextFetchOffset() == 10 &&
                            expectedOffsetStateMap.equals(sharePartition.cachedState().get(10L).offsetState());
                },
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(10L, List.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L))));
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleSubsetRecordBatchWithGapOffsets() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();
        MemoryRecords records1 = memoryRecords(5, 2);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(10, 2);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        fetchAcquiredRecords(sharePartition, records1, 2);

        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        fetchAcquiredRecords(sharePartition, records2, 11);
        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Acknowledging over subset of both batch with subset of gap offsets.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(
                        6, 18, List.of(
                        AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id,
                        AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id,
                        ACKNOWLEDGE_TYPE_GAP_ID, ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.ACCEPT.id,
                        ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.ACCEPT.id, ACKNOWLEDGE_TYPE_GAP_ID,
                        AcknowledgeType.ACCEPT.id))));

        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());

        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(18L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(19L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(20L).acquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());
        assertEquals(10, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire for the offsets that have not been acknowledged yet.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> {
                    Map<Long, InFlightState> expectedOffsetStateMap1 = new HashMap<>();
                    expectedOffsetStateMap1.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap1.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));

                    Map<Long, InFlightState> expectedOffsetStateMap2 = new HashMap<>();
                    expectedOffsetStateMap2.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap2.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap2.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap2.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap2.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap2.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap2.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap2.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap2.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap2.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap2.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));

                    return sharePartition.timer().size() == 0 &&
                            sharePartition.nextFetchOffset() == 5 &&
                            expectedOffsetStateMap1.equals(sharePartition.cachedState().get(5L).offsetState()) &&
                            expectedOffsetStateMap2.equals(sharePartition.cachedState().get(10L).offsetState());
                },
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of(5L, 6L), 10L, List.of(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L))));

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());

        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(18L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(19L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(20L).acquisitionLockTimeoutTask());
        assertEquals(10, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockTimeoutCauseMaxDeliveryCountExceed() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2) // Only 2 delivery attempts will be made before archiving the records
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Adding memoryRecords(10) in the sharePartition to make sure that SPSO doesn't move forward when delivery count of records2
        // exceed the max delivery count.
        fetchAcquiredRecords(sharePartition, memoryRecords(10), 10);

        fetchAcquiredRecords(sharePartition, memoryRecords(10, 10), 10);

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 0 &&
                        sharePartition.cachedState().get(10L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(10L).batchDeliveryCount() == 1 &&
                        sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(10L, List.of())));

        assertEquals(0, sharePartition.deliveryCompleteCount());

        fetchAcquiredRecords(sharePartition, memoryRecords(10, 10), 10);

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(2, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire to archive the records that reach max delivery count.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> sharePartition.timer().size() == 0 &&
                    sharePartition.nextFetchOffset() == 0 &&
                    // After the second delivery attempt fails to acknowledge the record correctly, the record should be archived.
                    sharePartition.cachedState().get(10L).batchState() == RecordState.ARCHIVED &&
                    sharePartition.cachedState().get(10L).batchDeliveryCount() == 2 &&
                    sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(10L, List.of())));
        // After the acquisition lock expires for the second time, the records should be archived as the max delivery count is reached.
        assertEquals(10, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockTimeoutCauseSPSOMoveForward() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2) // Only 2 delivery attempts will be made before archiving the records
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(10), 10);

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 0 &&
                        sharePartition.cachedState().get(0L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(0L).batchDeliveryCount() == 1 &&
                        sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(0L, List.of())));

        assertEquals(0, sharePartition.deliveryCompleteCount());

        fetchAcquiredRecords(sharePartition, memoryRecords(5), 5);

        assertNotNull(sharePartition.cachedState().get(0L).offsetState().get(0L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState().get(1L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState().get(2L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState().get(3L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState().get(4L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(9L).acquisitionLockTimeoutTask());

        // Allowing acquisition lock to expire to archive the records that reach max delivery count.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> {
                    Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
                    expectedOffsetStateMap.put(0L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(1L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(2L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(3L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(4L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(8L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(9L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));

                    return sharePartition.timer().size() == 0 && sharePartition.nextFetchOffset() == 5 &&
                            expectedOffsetStateMap.equals(sharePartition.cachedState().get(0L).offsetState());
                },
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(0L, List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L))));

        assertNull(sharePartition.cachedState().get(0L).offsetState().get(0L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(1L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(2L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(3L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(4L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(9L).acquisitionLockTimeoutTask());

        // Since only first 5 records from the batch are archived, the batch remains in the cachedState, but the
        // start offset is updated
        assertEquals(5, sharePartition.startOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockTimeoutCauseSPSOMoveForwardAndClearCachedState() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2) // Only 2 delivery attempts will be made before archiving the records
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(10), 10);

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 0 &&
                        sharePartition.cachedState().get(0L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(0L, List.of())));

        assertEquals(0, sharePartition.deliveryCompleteCount());

        fetchAcquiredRecords(sharePartition, memoryRecords(10), 10);

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire to archive the records that reach max delivery count.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        // After the second failed attempt to acknowledge the record batch successfully, the record batch is archived.
                        // Since this is the first batch in the share partition, SPSO moves forward and the cachedState is cleared
                        sharePartition.cachedState().isEmpty() &&
                        sharePartition.nextFetchOffset() == 10,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of()));
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcknowledgeAfterAcquisitionLockTimeout() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);

        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 5 &&
                        sharePartition.cachedState().get(5L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of())));

        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Acknowledge with ACCEPT type should throw InvalidRecordStateException since they've been released due to acquisition lock timeout.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.ACCEPT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRecordStateException.class, ackResult);
        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Try acknowledging with REJECT type should throw InvalidRecordStateException since they've been released due to acquisition lock timeout.
        ackResult = sharePartition.acknowledge(MEMBER_ID,
                List.of(new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.REJECT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRecordStateException.class, ackResult);
        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockAfterDifferentAcknowledges() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);

        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Acknowledge with RELEASE type.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(5, 6, List.of(AcknowledgeType.RELEASE.id))));

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Acknowledge with ACCEPT type.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(8, 9, List.of(AcknowledgeType.ACCEPT.id))));

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());
        assertEquals(2, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire will only affect the offsets that have not been acknowledged yet.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> {
                    // Check cached state.
                    Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
                    expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));

                    return sharePartition.timer().size() == 0 && sharePartition.nextFetchOffset() == 5 &&
                            expectedOffsetStateMap.equals(sharePartition.cachedState().get(5L).offsetState());
                },
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of(5L, 6L, 7L, 8L, 9L))));

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockOnBatchWithWriteShareGroupStateFailure() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 10), 10);

        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());

        // Allowing acquisition lock to expire. Even if write share group state RPC fails, state transition still happens.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 5 &&
                        sharePartition.cachedState().size() == 1 &&
                        sharePartition.cachedState().get(5L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of())));
    }

    @Test
    public void testDeliveryCompleteCountOnLockExpiryAndWriteFailureOnBatchLastDelivery() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .withMaxDeliveryCount(2)
            .build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 10), 10);
        fetchAcquiredRecords(sharePartition, memoryRecords(15, 10), 10);

        assertEquals(2, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(15L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire. Even if write share group state RPC fails, state transition still happens.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> sharePartition.timer().size() == 0 &&
                sharePartition.nextFetchOffset() == 5 &&
                sharePartition.cachedState().size() == 2 &&
                sharePartition.cachedState().get(15L).batchState() == RecordState.AVAILABLE &&
                sharePartition.cachedState().get(15L).batchAcquisitionLockTimeoutTask() == null,
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of())));
        assertEquals(0, sharePartition.deliveryCompleteCount());

        fetchAcquiredRecords(sharePartition, memoryRecords(15, 10), 10);
        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(15L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        // Allowing acquisition lock to expire. Even if write share group state RPC fails, state transition still happens.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> sharePartition.timer().size() == 0 &&
                sharePartition.nextFetchOffset() == 5 &&
                sharePartition.cachedState().size() == 2 &&
                sharePartition.cachedState().get(15L).batchState() == RecordState.ARCHIVED &&
                sharePartition.cachedState().get(15L).batchAcquisitionLockTimeoutTask() == null,
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of())));

        // Even though the write state call failed, the records are still archived and deliveryCompleteCount is updated.
        assertEquals(10, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testDeliveryCompleteCountOnLockExpiryAndWriteFailureOnOffsetLastDelivery() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .withMaxDeliveryCount(2)
            .build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns true for acknowledge to pass.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 6), 6);

        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());

        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(8, 9, List.of(AcknowledgeType.ACCEPT.id))));

        assertEquals(2, sharePartition.deliveryCompleteCount());

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        // Allowing acquisition lock to expire. Even if write share group state RPC fails, state transition still happens.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> {
                Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
                expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(10L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                return sharePartition.timer().size() == 0 && sharePartition.cachedState().size() == 1 &&
                    expectedOffsetStateMap.equals(sharePartition.cachedState().get(5L).offsetState());
            },
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of(5L, 6L, 7L, 8L, 9L, 10L))));

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.deliveryCompleteCount());

        fetchAcquiredRecords(sharePartition, memoryRecords(10, 1), 1);

        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(10L).acquisitionLockTimeoutTask());

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        // Allowing acquisition lock to expire. Even if write share group state RPC fails, state transition still happens.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> {
                Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
                expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
                return sharePartition.timer().size() == 0 && sharePartition.cachedState().size() == 1 &&
                    expectedOffsetStateMap.equals(sharePartition.cachedState().get(5L).offsetState());
            },
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of(5L, 6L, 7L, 10L))));

        // Even though the write state call failed, the record is still archived and deliveryCompleteCount is updated.
        assertEquals(3, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockOnOffsetWithWriteShareGroupStateFailure() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns true for acknowledge to pass.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 6), 6);

        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());

        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(8, 9, List.of(AcknowledgeType.ACCEPT.id))));

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        // Allowing acquisition lock to expire. Even if write share group state RPC fails, state transition still happens.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> {
                    Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
                    expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                    expectedOffsetStateMap.put(10L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                    return sharePartition.timer().size() == 0 && sharePartition.cachedState().size() == 1 &&
                            expectedOffsetStateMap.equals(sharePartition.cachedState().get(5L).offsetState());
                },
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of(5L, 6L, 7L, 8L, 9L, 10L))));

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(10L).acquisitionLockTimeoutTask());
    }

    @Test
    public void testReleaseSingleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(1), 1);
        assertEquals(0, sharePartition.deliveryCompleteCount());

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        // Release delivery count.
        assertEquals(0, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testReleaseMultipleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 10), 10);
        assertEquals(0, sharePartition.deliveryCompleteCount());

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(0, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testReleaseMultipleAcknowledgedRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records0 = memoryRecords(5);
        MemoryRecords records1 = memoryRecords(5, 2);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecords records2 = memoryRecords(10, 9);

        fetchAcquiredRecords(sharePartition, records0, 5);
        fetchAcquiredRecords(sharePartition, records1, 2);
        fetchAcquiredRecords(sharePartition, records2, 9);

        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(5, 18, List.of(AcknowledgeType.ACCEPT.id))));
        // After the acknowledgements, the cached state has 11 Terminal records ->
        // (5 -> 6)
        // (10 -> 18)
        assertEquals(11, sharePartition.deliveryCompleteCount());

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(11, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testReleaseAcknowledgedMultipleSubsetRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(5, 2);

        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(10, 2);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        fetchAcquiredRecords(sharePartition, records1, 2);
        fetchAcquiredRecords(sharePartition, records2, 11);

        // Acknowledging over subset of both batch with subset of gap offsets.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(6, 18, List.of(
                AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id,
                AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id,
                ACKNOWLEDGE_TYPE_GAP_ID, ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.ACCEPT.id,
                ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.ACCEPT.id, ACKNOWLEDGE_TYPE_GAP_ID,
                AcknowledgeType.ACCEPT.id))));

        // After the acknowledgements, the cached state has 10 Terminal records ->
        // 6
        // (10 -> 18)
        assertEquals(10, sharePartition.deliveryCompleteCount());

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
        assertEquals(10, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testReleaseAcquiredRecordsWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(5, 1);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(10, 2);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        sharePartition.acquire("member-2", ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 5, fetchPartitionData(records1), FETCH_ISOLATION_HWM);
        sharePartition.acquire(MEMBER_ID, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 10, fetchPartitionData(records2), FETCH_ISOLATION_HWM);

        // Acknowledging over subset of second batch with subset of gap offsets.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(10, 18, List.of(
                AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id, ACKNOWLEDGE_TYPE_GAP_ID, ACKNOWLEDGE_TYPE_GAP_ID,
                AcknowledgeType.ACCEPT.id, ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.ACCEPT.id, ACKNOWLEDGE_TYPE_GAP_ID,
                AcknowledgeType.ACCEPT.id))));

        // After the acknowledgements, the cached state has 9 Terminal records ->
        // (10 -> 18)
        assertEquals(9, sharePartition.deliveryCompleteCount());

        // Release acquired records for "member-1".
        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(19, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
        assertEquals(9, sharePartition.deliveryCompleteCount());

        // Release acquired records for "member-2".
        releaseResult = sharePartition.releaseAcquiredRecords("member-2");
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        // Check cached state.
        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
        assertEquals(9, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testReleaseAcquiredRecordsWithAnotherMemberAndSubsetAcknowledged() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(5, 2);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(10, 2);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        sharePartition.acquire("member-2", ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 5, fetchPartitionData(records1), FETCH_ISOLATION_HWM);
        sharePartition.acquire(MEMBER_ID, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 10, fetchPartitionData(records2), FETCH_ISOLATION_HWM);

        // Acknowledging over subset of second batch with subset of gap offsets.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(10, 18, List.of(
                AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id, ACKNOWLEDGE_TYPE_GAP_ID, ACKNOWLEDGE_TYPE_GAP_ID,
                AcknowledgeType.ACCEPT.id, ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.ACCEPT.id, ACKNOWLEDGE_TYPE_GAP_ID,
                AcknowledgeType.ACCEPT.id))));

        // After the acknowledgements, the cached state has 9 Terminal records ->
        // (10 -> 18)
        assertEquals(9, sharePartition.deliveryCompleteCount());

        // Release acquired records for "member-1".
        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(19, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
        assertEquals(9, sharePartition.deliveryCompleteCount());

        // Ack subset of records by "member-2".
        sharePartition.acknowledge("member-2",
                List.of(new ShareAcknowledgementBatch(5, 5, List.of(AcknowledgeType.ACCEPT.id))));
        // After the acknowledgements, the startOffset will be upadated to 6, since offset 5 is Terminal. Hence
        // deliveryCompleteCount will remain 9.
        assertEquals(9, sharePartition.deliveryCompleteCount());

        // Release acquired records for "member-2".
        releaseResult = sharePartition.releaseAcquiredRecords("member-2");
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(6, sharePartition.nextFetchOffset());
        // Check cached state.
        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
        assertEquals(9, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testReleaseAcquiredRecordsForEmptyCachedData() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        // Release a batch when cache is empty.
        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testReleaseAcquiredRecordsAfterDifferentAcknowledges() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);

        sharePartition.acknowledge(MEMBER_ID,
                List.of(new ShareAcknowledgementBatch(5, 6, List.of(AcknowledgeType.RELEASE.id))));

        sharePartition.acknowledge(MEMBER_ID,
                List.of(new ShareAcknowledgementBatch(8, 9, List.of(AcknowledgeType.ACCEPT.id))));

        assertEquals(2, sharePartition.deliveryCompleteCount());

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());
        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
        assertEquals(2, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaxDeliveryCountLimitNotExceededForRecordsSubsetAfterReleaseAcquiredRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxDeliveryCount(2)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(10), 10);

        MemoryRecords records2 = memoryRecords(10, 5);
        fetchAcquiredRecords(sharePartition, records2, 5);

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(10, 14, List.of(AcknowledgeType.RELEASE.id))));
        assertEquals(0, sharePartition.deliveryCompleteCount());

        fetchAcquiredRecords(sharePartition, records2, 5);

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaxDeliveryCountLimitNotExceededForRecordsSubsetAfterReleaseAcquiredRecordsSubset() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxDeliveryCount(2)
            .withState(SharePartitionState.ACTIVE)
            .build();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(10, 5);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(15, 5);
        // third fetch request with 5 records starting from offset20.
        MemoryRecords records3 = memoryRecords(20, 5);

        fetchAcquiredRecords(sharePartition, records1, 5);
        fetchAcquiredRecords(sharePartition, records2, 5);
        fetchAcquiredRecords(sharePartition, records3, 5);

        sharePartition.acknowledge(MEMBER_ID, new ArrayList<>(List.of(
                new ShareAcknowledgementBatch(13, 16, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(17, 19, List.of(AcknowledgeType.REJECT.id)),
                new ShareAcknowledgementBatch(20, 24, List.of(AcknowledgeType.RELEASE.id))
        )));

        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Send next batch from offset 13, only 2 records should be acquired.
        fetchAcquiredRecords(sharePartition, records1, 2);

        // Send next batch from offset 15, only 2 records should be acquired.
        fetchAcquiredRecords(sharePartition, records2, 2);
        fetchAcquiredRecords(sharePartition, records3, 5);

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(15L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(20L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(20L).batchMemberId());
        assertNull(sharePartition.cachedState().get(20L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(15L).offsetState());
        assertEquals(3, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetCacheCleared() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxDeliveryCount(2)
            .withState(SharePartitionState.ACTIVE)
            .build();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(10, 5);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(15, 5);
        // Third fetch request with 5 records starting from offset 20.
        MemoryRecords records3 = memoryRecords(20, 5);

        fetchAcquiredRecords(sharePartition, records1, 5);
        fetchAcquiredRecords(sharePartition, records2, 5);
        fetchAcquiredRecords(sharePartition, records3, 5);

        sharePartition.acknowledge(MEMBER_ID, new ArrayList<>(List.of(
                new ShareAcknowledgementBatch(10, 12, List.of(AcknowledgeType.ACCEPT.id)),
                new ShareAcknowledgementBatch(13, 16, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(17, 19, List.of(AcknowledgeType.REJECT.id)),
                new ShareAcknowledgementBatch(20, 24, List.of(AcknowledgeType.RELEASE.id))
        )));

        // After acknowledgements, since offsets 10 -> 12 are at the start of the caches state and are in Terminal state,
        // the start offset will be updated to 13. From the remaining offstes in flight, only records (17 -> 19) are in Terminal state.
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // Send next batch from offset 13, only 2 records should be acquired.
        fetchAcquiredRecords(sharePartition, records1, 2);
        // Send next batch from offset 15, only 2 records should be acquired.
        fetchAcquiredRecords(sharePartition, records2, 2);
        fetchAcquiredRecords(sharePartition, records3, 5);

        sharePartition.acknowledge(MEMBER_ID, new ArrayList<>(List.of(
            new ShareAcknowledgementBatch(13, 16, List.of(AcknowledgeType.RELEASE.id)),
            new ShareAcknowledgementBatch(20, 24, List.of(AcknowledgeType.RELEASE.id))
        )));

        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testReleaseAcquiredRecordsSubsetWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 7), 7);

        sharePartition.acknowledge(MEMBER_ID,
                List.of(new ShareAcknowledgementBatch(5, 7, List.of(AcknowledgeType.ACCEPT.id))));

        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Release acquired records subset with another member.
        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords("member-2");
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());
        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testReleaseBatchWithWriteShareGroupStateFailure() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 10), 10);

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertTrue(releaseResult.isCompletedExceptionally());
        assertFutureThrows(GroupIdNotFoundException.class, releaseResult);

        // Due to failure in writeShareGroupState, the cached state should not be updated.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testReleaseOffsetWithWriteShareGroupStateFailure() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns true for acknowledge to pass.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 6), 6);

        sharePartition.acknowledge(MEMBER_ID,
                List.of(new ShareAcknowledgementBatch(8, 9, List.of(AcknowledgeType.ACCEPT.id))));

        assertEquals(2, sharePartition.deliveryCompleteCount());

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertTrue(releaseResult.isCompletedExceptionally());
        assertFutureThrows(GroupIdNotFoundException.class, releaseResult);

        // Due to failure in writeShareGroupState, the cached state should not be updated.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(5L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(6L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(7L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).offsetState().get(8L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).offsetState().get(9L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(10L).state());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(5L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(6L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(7L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(8L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(9L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(10L).memberId());
        assertEquals(2, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockOnReleasingMultipleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 10), 10);

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(0, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        // Acquisition lock timer task would be cancelled by the release acquired records operation.
        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockOnReleasingAcknowledgedMultipleSubsetRecordBatchWithGapOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();
        MemoryRecords records1 = memoryRecords(5, 2);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(10, 2);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        fetchAcquiredRecords(sharePartition, records1, 2);
        fetchAcquiredRecords(sharePartition, records2, 11);

        // Acknowledging over subset of both batch with subset of gap offsets.
        sharePartition.acknowledge(MEMBER_ID,
                List.of(new ShareAcknowledgementBatch(6, 18, List.of(
                        AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id,
                        AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id,
                        ACKNOWLEDGE_TYPE_GAP_ID, ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.ACCEPT.id,
                        ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.ACCEPT.id, ACKNOWLEDGE_TYPE_GAP_ID,
                        AcknowledgeType.ACCEPT.id))));

        assertEquals(10, sharePartition.deliveryCompleteCount());

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // Acquisition lock timer task would be cancelled by the release acquired records operation.
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());

        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(18L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(19L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(20L).acquisitionLockTimeoutTask());

        assertEquals(0, sharePartition.timer().size());
        assertEquals(10, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testDeliveryCompleteCountWhenStaleBatchesAreArchived() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(11L, 20L, RecordState.AVAILABLE.id, (short) 1),
                        new PersisterStateBatch(21L, 30L, RecordState.AVAILABLE.id, (short) 1)
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(11, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Fetched records are from 21 to 30
        MemoryRecords records = memoryRecords(21, 10);

        // The fetch offset is set as 11, which is the next fetch offset, but the returned records are from 21 onwards.
        // This means there is a gap in the partition from 11 to 20. In this case, the batch 11 to 20 will be archived
        // during the acquire operation.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            MAX_FETCH_RECORDS,
            11,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM), 10);

        assertArrayEquals(expectedAcquiredRecord(21, 30, 2).toArray(), acquiredRecordsList.toArray());

        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(21L).batchState());

        // Since the records 11 -> 20 are ARCHIVED, deliveryCompleteCount will be 10.
        assertEquals(10, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testDeliveryCompleteCountWhenStaleOffsetsAreArchived() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(11L, 20L, RecordState.AVAILABLE.id, (short) 1),
                        new PersisterStateBatch(21L, 30L, RecordState.AVAILABLE.id, (short) 1)
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(11, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Fetched records are from 21 to 30
        MemoryRecords records = memoryRecords(16, 15);

        // The fetch offset is set as 11, which is the next fetch offset, but the returned records are from 16 onwards.
        // This means there is a gap in the partition from 11 to 16. In this case, the offsets 11 to 15 will be archived
        // during the acquire operation.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            MAX_FETCH_RECORDS,
            11,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM), 15);

        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(16, 16, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(17, 17, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(18, 18, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(19, 19, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(20, 20, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(21, 30, 2));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(11L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(12L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(13L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(14L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(15L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(21L).batchState());

        // Since the records 11 -> 15 are ARCHIVED, deliveryCompleteCount will be 5.
        assertEquals(5, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testLsoMovementOnInitializationSharePartition() {
        // LSO is at 0.
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        sharePartition.updateCacheAndOffsets(0);
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());

        // LSO is at 5.
        sharePartition.updateCacheAndOffsets(5);
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(5, sharePartition.startOffset());
        assertEquals(5, sharePartition.endOffset());
    }

    @Test
    public void testLsoMovementForArchivingBatches() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(12, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(17, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(22, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(27, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(32, 5), 5);

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(2, 6, List.of(AcknowledgeType.ACCEPT.id)),
                new ShareAcknowledgementBatch(12, 16, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(22, 26, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(27, 31, List.of(AcknowledgeType.REJECT.id))
        ));

        // After the acknowledgements, the records in Terminal state are ->
        // 27 -> 31: ARCHIVED
        // Records 2 -> 6 are ACKNOWLEDGED, but since they are at the start of the cache, the start offset will be moved to 7.
        assertEquals(5, sharePartition.deliveryCompleteCount());

        // LSO is at 20.
        sharePartition.updateCacheAndOffsets(20);

        assertEquals(22, sharePartition.nextFetchOffset());
        assertEquals(20, sharePartition.startOffset());
        assertEquals(36, sharePartition.endOffset());

        // For cached state corresponding to entry 2, the batch state will be ACKNOWLEDGED, hence it will be cleared as part of acknowledgement.
        assertEquals(6, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());
        assertNotNull(sharePartition.cachedState().get(7L).batchAcquisitionLockTimeoutTask());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(12L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(12L).batchState());
        assertNull(sharePartition.cachedState().get(12L).batchAcquisitionLockTimeoutTask());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(17L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(17L).batchState());
        assertNotNull(sharePartition.cachedState().get(17L).batchAcquisitionLockTimeoutTask());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(22L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(22L).batchState());
        assertNull(sharePartition.cachedState().get(22L).batchAcquisitionLockTimeoutTask());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(27L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(27L).batchState());
        assertNull(sharePartition.cachedState().get(27L).batchAcquisitionLockTimeoutTask());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(32L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(32L).batchState());
        assertNotNull(sharePartition.cachedState().get(32L).batchAcquisitionLockTimeoutTask());
        // After the LSO is moved, AVAILABLE batches are ARCHIVED. Thus, the records 12 -> 16 will be ARCHIVED. Since
        // these are prior to the new startOffset, deliveryCompleteCount remains the same.
        assertEquals(5, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testLsoMovementPostArchivedBatches() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(12, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(17, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(22, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(27, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(32, 5), 5);


        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(2, 6, List.of(AcknowledgeType.RELEASE.id)),
            new ShareAcknowledgementBatch(12, 16, List.of(AcknowledgeType.REJECT.id)),
            new ShareAcknowledgementBatch(22, 26, List.of(AcknowledgeType.RELEASE.id)),
            new ShareAcknowledgementBatch(27, 31, List.of(AcknowledgeType.REJECT.id))
        ));

        // After the acknowledgements, the records in Terminal state are ->
        // 12 -> 16: ARCHIVED
        // 27 -> 31: ARCHIVED
        assertEquals(10, sharePartition.deliveryCompleteCount());

        // LSO is at 20.
        sharePartition.updateCacheAndOffsets(20);

        assertEquals(22, sharePartition.nextFetchOffset());
        assertEquals(20, sharePartition.startOffset());
        assertEquals(36, sharePartition.endOffset());

        assertEquals(7, sharePartition.cachedState().size());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(2L).batchState());
        assertNull(sharePartition.cachedState().get(2L).batchAcquisitionLockTimeoutTask());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());
        assertNotNull(sharePartition.cachedState().get(7L).batchAcquisitionLockTimeoutTask());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(12L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(12L).batchState());
        assertNull(sharePartition.cachedState().get(12L).batchAcquisitionLockTimeoutTask());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(17L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(17L).batchState());
        assertNotNull(sharePartition.cachedState().get(17L).batchAcquisitionLockTimeoutTask());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(22L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(22L).batchState());
        assertNull(sharePartition.cachedState().get(22L).batchAcquisitionLockTimeoutTask());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(27L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(27L).batchState());
        assertNull(sharePartition.cachedState().get(27L).batchAcquisitionLockTimeoutTask());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(32L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(32L).batchState());
        assertNotNull(sharePartition.cachedState().get(32L).batchAcquisitionLockTimeoutTask());
        // After the LSO is moved, the number of Terminal records between old and new start offsets is calculated.
        // In this case it is 5 (for the records 12 -> 16). Thus, deliveryCompleteCount is decremented by 5.
        assertEquals(5, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testLsoMovementPostArchivedRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(12, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(17, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(22, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(27, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(32, 5), 5);


        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(2, 6, List.of(AcknowledgeType.RELEASE.id)),
            new ShareAcknowledgementBatch(12, 16, List.of(AcknowledgeType.REJECT.id)),
            new ShareAcknowledgementBatch(19, 21, List.of(AcknowledgeType.REJECT.id)),
            new ShareAcknowledgementBatch(22, 26, List.of(AcknowledgeType.RELEASE.id)),
            new ShareAcknowledgementBatch(27, 31, List.of(AcknowledgeType.REJECT.id))
        ));

        // After the acknowledgements, the records in Terminal state are ->
        // 12 -> 16: ARCHIVED
        // 19 -> 21: ARCHIVED
        // 27 -> 31: ARCHIVED
        assertEquals(13, sharePartition.deliveryCompleteCount());

        // LSO is at 20.
        sharePartition.updateCacheAndOffsets(20);

        assertEquals(22, sharePartition.nextFetchOffset());
        assertEquals(20, sharePartition.startOffset());
        assertEquals(36, sharePartition.endOffset());

        assertEquals(7, sharePartition.cachedState().size());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(2L).batchState());
        assertNull(sharePartition.cachedState().get(2L).batchAcquisitionLockTimeoutTask());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());
        assertNotNull(sharePartition.cachedState().get(7L).batchAcquisitionLockTimeoutTask());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(12L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(12L).batchState());
        assertNull(sharePartition.cachedState().get(12L).batchAcquisitionLockTimeoutTask());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(17L).offsetState().get(17L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(17L).offsetState().get(18L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(17L).offsetState().get(19L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(17L).offsetState().get(20L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(17L).offsetState().get(21L).state());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(22L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(22L).batchState());
        assertNull(sharePartition.cachedState().get(22L).batchAcquisitionLockTimeoutTask());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(27L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(27L).batchState());
        assertNull(sharePartition.cachedState().get(27L).batchAcquisitionLockTimeoutTask());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(32L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(32L).batchState());
        assertNotNull(sharePartition.cachedState().get(32L).batchAcquisitionLockTimeoutTask());
        // After the LSO is moved, the number of Terminal records between old and new start offsets is calculated.
        // In this case it is 6 (for the records 12 -> 16 and 19). Thus, deliveryCompleteCount is decremented by 6.
        assertEquals(7, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testLsoMovementForArchivingAllAvailableBatches() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        // A client acquires 4 batches, 11 -> 20, 21 -> 30, 31 -> 40, 41 -> 50.
        fetchAcquiredRecords(sharePartition, memoryRecords(11, 10), 10);
        fetchAcquiredRecords(sharePartition, memoryRecords(21, 10), 10);
        fetchAcquiredRecords(sharePartition, memoryRecords(31, 10), 10);
        fetchAcquiredRecords(sharePartition, memoryRecords(41, 10), 10);

        // After the acknowledgements, the state of share partition will be:
        // 1. 11 -> 20: AVAILABLE
        // 2. 21 -> 30: ACQUIRED
        // 3. 31 -> 40: AVAILABLE
        // 4. 41 -> 50: ACQUIRED
        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(11, 20, List.of(AcknowledgeType.RELEASE.id)),
            new ShareAcknowledgementBatch(31, 40, List.of(AcknowledgeType.RELEASE.id))
        ));

        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Move the LSO to 41. When the LSO moves ahead, all batches that are AVAILABLE before the new LSO will be ARCHIVED.
        // Thus, the state of the share partition will be:
        // 1. 11 -> 20: ARCHIVED
        // 2. 21 -> 30: ACQUIRED
        // 3. 31 -> 40: ARCHIVED
        // 4. 41 -> 50: ACQUIRED
        // Note, the records that are in ACQUIRED state will remain in ACQUIRED state and will be transitioned to a Terminal
        // state when the corresponding acquisition lock timer task expires.
        sharePartition.updateCacheAndOffsets(41);

        assertEquals(51, sharePartition.nextFetchOffset());
        assertEquals(41, sharePartition.startOffset());
        assertEquals(50, sharePartition.endOffset());

        assertEquals(4, sharePartition.cachedState().size());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(21L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(31L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(41L).batchState());
        // There are no records in flight in Terminal state.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // The client acknowledges the batch 21 -> 30. Since this batch is before the LSO, nothing will be done and these
        // records will remain in the ACQUIRED state.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(21L, 30L, List.of(AcknowledgeType.RELEASE.id))));
        // The acknowledgements make no difference to in flight records.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // The batch is still in ACQUIRED state.
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(21L).batchState());

        // Once the acquisition lock timer task for the batch 21 -> 30 is expired, these records will directly be
        // ARCHIVED.
        sharePartition.cachedState().get(21L).batchAcquisitionLockTimeoutTask().run();
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(21L).batchState());
        // Even when the acquisition lock expires, this happens for records before the LSO, hence in flight terminal records remain 0.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testLsoMovementForArchivingAllAvailableOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        // A client acquires 4 batches, 11 -> 20, 21 -> 30, 31 -> 40, 41 -> 50.
        fetchAcquiredRecords(sharePartition, memoryRecords(11, 10), 10);
        fetchAcquiredRecords(sharePartition, memoryRecords(21, 10), 10);
        fetchAcquiredRecords(sharePartition, memoryRecords(31, 10), 10);
        fetchAcquiredRecords(sharePartition, memoryRecords(41, 10), 10);

        // After the acknowledgements, the share partition state will be:
        // 1. 11 -> 20: AVAILABLE
        // 2. 21 -> 30: ACQUIRED
        // 3. 31 -> 40: AVAILABLE
        // 4. 41 -> 50: ACQUIRED
        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(11, 20, List.of(AcknowledgeType.RELEASE.id)),
            new ShareAcknowledgementBatch(31, 40, List.of(AcknowledgeType.RELEASE.id))
        ));

        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Move the LSO to 36. When the LSO moves ahead, all records that are AVAILABLE before the new LSO will be ARCHIVED.
        // Thus, the state of the share partition will be:
        // 1. 11 -> 20: ARCHIVED
        // 2. 21 -> 30: ACQUIRED
        // 3. 31 -> 35: ARCHIVED
        // 3. 36 -> 40: AVAILABLE
        // 4. 41 -> 50: ACQUIRED
        // Note, the records that are in ACQUIRED state will remain in ACQUIRED state and will be transitioned to a Terminal
        // state when the corresponding acquisition lock timer task expires.
        sharePartition.updateCacheAndOffsets(36);

        assertEquals(36, sharePartition.nextFetchOffset());
        assertEquals(36, sharePartition.startOffset());
        assertEquals(50, sharePartition.endOffset());

        assertEquals(4, sharePartition.cachedState().size());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(21L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(31L).offsetState().get(31L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(31L).offsetState().get(32L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(31L).offsetState().get(33L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(31L).offsetState().get(34L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(31L).offsetState().get(35L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(31L).offsetState().get(36L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(31L).offsetState().get(37L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(31L).offsetState().get(38L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(31L).offsetState().get(39L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(31L).offsetState().get(40L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(41L).batchState());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // The client acknowledges the batch 21 -> 30. Since this batch is before the LSO, nothing will be done and these
        // records will remain in the ACQUIRED state.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(21L, 30L, List.of(AcknowledgeType.RELEASE.id))));
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // The batch is still in ACQUIRED state.
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(21L).batchState());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Once the acquisition lock timer task for the batch 21 -> 30 is expired, these records will directly be
        // ARCHIVED.
        sharePartition.cachedState().get(21L).batchAcquisitionLockTimeoutTask().run();
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(21L).batchState());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testLsoMovementForArchivingOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(4, 8, List.of(AcknowledgeType.ACCEPT.id))));

        // LSO at is 5.
        sharePartition.updateCacheAndOffsets(5);
        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(5, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        // Check cached offset state map.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));

        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(7L).offsetState());
        assertNull(sharePartition.cachedState().get(7L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(7L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(7L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(7L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(7L).offsetState().get(11L).acquisitionLockTimeoutTask());

        // Check cached offset state map.
        expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(2L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(3L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(4L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));

        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(2L).offsetState());
        assertNotNull(sharePartition.cachedState().get(2L).offsetState().get(2L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(2L).offsetState().get(3L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(2L).offsetState().get(4L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(2L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(2L).offsetState().get(6L).acquisitionLockTimeoutTask());
    }

    @Test
    public void testLsoMovementForArchivingOffsetsWithStartAndEndBatchesNotFullMatches() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        // LSO is at 4.
        sharePartition.updateCacheAndOffsets(4);

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(4, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());

        // LSO is at 8.
        sharePartition.updateCacheAndOffsets(8);

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(8, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());
    }

    @Test
    public void testLsoMovementForArchivingOffsetsWithStartOffsetNotFullMatches() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        // LSO is at 4.
        sharePartition.updateCacheAndOffsets(4);

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(4, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        // LSO is at 7.
        sharePartition.updateCacheAndOffsets(7);

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(7, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());
    }

    @Test
    public void testLsoMovementForArchivingOffsetsWithStartOffsetNotFullMatchesPostAcceptAcknowledgement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        // LSO is at 4.
        sharePartition.updateCacheAndOffsets(4);

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(4, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        // Acknowledge with ACCEPT action.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(7, 8, List.of(AcknowledgeType.ACCEPT.id))));

        // LSO is at 7.
        sharePartition.updateCacheAndOffsets(7);

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(7, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        // Check cached offset state map.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(7L).offsetState());
    }

    @Test
    public void testLsoMovementForArchivingOffsetsWithStartOffsetNotFullMatchesPostReleaseAcknowledgement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        // LSO is at 4.
        sharePartition.updateCacheAndOffsets(4);

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(4, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        // Acknowledge with RELEASE action.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(7, 8, List.of(AcknowledgeType.RELEASE.id))));

        // LSO is at 7.
        sharePartition.updateCacheAndOffsets(7);

        assertEquals(7, sharePartition.nextFetchOffset());
        assertEquals(7, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        // Check cached offset state map.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(7L).offsetState());
    }

    @Test
    public void testLsoMovementToEndOffset() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        // Acknowledge with RELEASE action.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(7, 8, List.of(AcknowledgeType.RELEASE.id))));

        // LSO is at 11.
        sharePartition.updateCacheAndOffsets(11);

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        // Check cached offset state map.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(7L).offsetState());
    }

    @Test
    public void testLsoMovementToEndOffsetWhereEndOffsetIsAvailable() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        // Acknowledge with RELEASE action.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(7, 8, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(11, 11, List.of(AcknowledgeType.RELEASE.id))));

        // LSO is at 11.
        sharePartition.updateCacheAndOffsets(11);

        assertEquals(11, sharePartition.nextFetchOffset());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        // Check cached offset state map.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(7L).offsetState());
    }

    @Test
    public void testLsoMovementAheadOfEndOffsetPostAcknowledgement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        // Acknowledge with RELEASE action.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(7, 8, List.of(AcknowledgeType.RELEASE.id))));

        // LSO is at 12.
        sharePartition.updateCacheAndOffsets(12);

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(12, sharePartition.startOffset());
        assertEquals(12, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        // Check cached offset state map.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(7L).offsetState());
    }

    @Test
    public void testLsoMovementAheadOfEndOffset() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        // LSO is at 14.
        sharePartition.updateCacheAndOffsets(14);

        assertEquals(14, sharePartition.nextFetchOffset());
        assertEquals(14, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(7L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());
    }

    @Test
    public void testLsoMovementWithGapsInCachedStateMap() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        MemoryRecords records1 = memoryRecords(2, 5);
        // Gap of 7-9.
        MemoryRecords records2 = memoryRecords(10, 5);
        // Gap of 15-19.
        MemoryRecords records3 = memoryRecords(20, 5);

        fetchAcquiredRecords(sharePartition, records1, 5);
        fetchAcquiredRecords(sharePartition, records2, 5);
        fetchAcquiredRecords(sharePartition, records3, 5);

        // LSO is at 18.
        sharePartition.updateCacheAndOffsets(18);

        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(18, sharePartition.startOffset());
        assertEquals(24, sharePartition.endOffset());
        assertEquals(3, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
    }

    @Test
    public void testLsoMovementWithGapsInCachedStateMapAndAcknowledgedBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        MemoryRecords records1 = memoryRecords(2, 5);
        // Gap of 7-9.
        MemoryRecords records2 = memoryRecords(10, 5);

        fetchAcquiredRecords(sharePartition, records1, 5);
        fetchAcquiredRecords(sharePartition, records2, 5);

        // Acknowledge with RELEASE action.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(10, 14, List.of(AcknowledgeType.RELEASE.id))));

        // LSO is at 10.
        sharePartition.updateCacheAndOffsets(10);

        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
    }

    @Test
    public void testLsoMovementPostGapsInAcknowledgements() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        MemoryRecords records1 = memoryRecords(5, 2);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(10, 5);
        // Gap from 15-17 offsets.
        recordsBuilder.appendWithOffset(18, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        fetchAcquiredRecords(sharePartition, records1, 2);
        fetchAcquiredRecords(sharePartition, records2, 9);

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(5, 6, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(10, 18, List.of(
                        AcknowledgeType.RELEASE.id, AcknowledgeType.RELEASE.id, AcknowledgeType.RELEASE.id,
                        AcknowledgeType.RELEASE.id, AcknowledgeType.RELEASE.id, ACKNOWLEDGE_TYPE_GAP_ID,
                        ACKNOWLEDGE_TYPE_GAP_ID, ACKNOWLEDGE_TYPE_GAP_ID, AcknowledgeType.RELEASE.id
                ))));

        // LSO is at 18.
        sharePartition.updateCacheAndOffsets(18);

        assertEquals(18, sharePartition.nextFetchOffset());
        assertEquals(18, sharePartition.startOffset());
        assertEquals(18, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(5L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testTerminalRecordsNotUpdatedWhenBatchesBeforeStartOffsetAreExpired() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(11L, 20L, RecordState.AVAILABLE.id, (short) 1),
                        new PersisterStateBatch(21L, 30L, RecordState.AVAILABLE.id, (short) 1)
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(11, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Fetched records are from 11 to 20
        MemoryRecords records = memoryRecords(10, 11);

        // A member acquired the available records 11 -> 20.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            MAX_FETCH_RECORDS,
            records.batches().iterator().next().baseOffset(),
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM), 10);

        assertArrayEquals(expectedAcquiredRecord(11, 20, 2).toArray(), acquiredRecordsList.toArray());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());

        // Move the LSO to 21.
        sharePartition.updateCacheAndOffsets(21);

        // After the LSO is moved to 21, all the records after new Start offset are in non-Terminal states. Thus,
        // deliveryCompleteCount is not changed.
        assertEquals(0, sharePartition.deliveryCompleteCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());

        // Acknowledge the acquired records. Since these records are present before the startOffset, these acknowledgements
        // will simply be ignored.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(11L, 20L, List.of(AcknowledgeType.RELEASE.id))));

        // Since the acknowledgements are ignored, the deliveryCompleteCount should not change.
        assertEquals(0, sharePartition.deliveryCompleteCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());

        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));

        // Expiring the acquisition lock timer task of the ACQUIRED batch.
        sharePartition.cachedState().get(11L).batchAcquisitionLockTimeoutTask().run();

        // After the acquisition lock timer task is expired, the records present before the startOffset are directly
        // moved to the ARCHIVED state.
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());
        // Since these records are present before the start offset, the deliveryCompleteCount should not change.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testTerminalRecordsNotUpdatedWhenOffsetsBeforeStartOffsetAreExpired() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(11L, 20L, RecordState.AVAILABLE.id, (short) 1),
                        new PersisterStateBatch(21L, 30L, RecordState.AVAILABLE.id, (short) 1)
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(11, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Fetched records are from 11 to 20
        MemoryRecords records = memoryRecords(10, 11);

        // A member acquired the available records 11 -> 20.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            MAX_FETCH_RECORDS,
            records.batches().iterator().next().baseOffset(),
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM), 10);

        assertArrayEquals(expectedAcquiredRecord(11, 20, 2).toArray(), acquiredRecordsList.toArray());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());

        // Move the LSO to 16.
        sharePartition.updateCacheAndOffsets(16);

        // After the LSO is moved to 21, all the records after new Start offset are in non-Terminal states. Thus,
        // deliveryCompleteCount is not changed.
        assertEquals(0, sharePartition.deliveryCompleteCount());
        assertEquals(16, sharePartition.startOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());

        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));

        // Expiring the acquisition lock timer task of the ACQUIRED batch.
        sharePartition.cachedState().get(11L).batchAcquisitionLockTimeoutTask().run();

        // Since these records are present before the start offset, the deliveryCompleteCount should not change.
        assertEquals(0, sharePartition.deliveryCompleteCount());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(11L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(12L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(13L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(14L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(15L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(16L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(17L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(18L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(19L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(20L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());
    }

    @Test
    public void testTerminalRecordsNotUpdatedWhenOffsetsBeforeStartOffsetAreExpiredAfterAcknowledgements() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(11L, 20L, RecordState.AVAILABLE.id, (short) 1),
                        new PersisterStateBatch(21L, 30L, RecordState.AVAILABLE.id, (short) 1)
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(11, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Fetched records are from 11 to 20
        MemoryRecords records = memoryRecords(10, 11);

        // A member acquired the available records 11 -> 20.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            MAX_FETCH_RECORDS,
            records.batches().iterator().next().baseOffset(),
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM), 10);

        assertArrayEquals(expectedAcquiredRecord(11, 20, 2).toArray(), acquiredRecordsList.toArray());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());

        // Move the LSO to 16.
        sharePartition.updateCacheAndOffsets(16);

        // There are no Terminal records between start offset and end offset.
        assertEquals(0, sharePartition.deliveryCompleteCount());
        assertEquals(16, sharePartition.startOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));

        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        // Acknowledge the acquired records. Only those records that are after the startOffset will be acknowledged.
        // In this case, records 11 -> 15 will remain in the ACQUIRED state, while records 16 -> 20 will be RELEASED.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(11L, 20L, List.of(AcknowledgeType.RELEASE.id))));

        assertEquals(0, sharePartition.deliveryCompleteCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).offsetState().get(11L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).offsetState().get(12L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).offsetState().get(13L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).offsetState().get(14L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(11L).offsetState().get(15L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(16L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(17L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(18L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(19L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(20L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());

        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));

        // Expiring the acquisition lock timer task of the ACQUIRED records.
        sharePartition.cachedState().get(11L).offsetState().get(11L).acquisitionLockTimeoutTask().run();
        sharePartition.cachedState().get(11L).offsetState().get(12L).acquisitionLockTimeoutTask().run();
        sharePartition.cachedState().get(11L).offsetState().get(13L).acquisitionLockTimeoutTask().run();
        sharePartition.cachedState().get(11L).offsetState().get(14L).acquisitionLockTimeoutTask().run();
        sharePartition.cachedState().get(11L).offsetState().get(15L).acquisitionLockTimeoutTask().run();

        // Since these records are present before the start offset, the deliveryCompleteCount should not change.
        assertEquals(0, sharePartition.deliveryCompleteCount());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(11L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(12L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(13L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(14L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).offsetState().get(15L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(16L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(17L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(18L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(19L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(11L).offsetState().get(20L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());
    }

    @Test
    public void testReleaseAcquiredRecordsBatchesPostStartOffsetMovement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        sharePartition.acquire("member-2", ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 15, fetchPartitionData(memoryRecords(15, 5)), FETCH_ISOLATION_HWM);

        fetchAcquiredRecords(sharePartition, memoryRecords(20, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(25, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(30, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(35, 5), 5);

        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Acknowledge records.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(6, 7, List.of(AcknowledgeType.ACCEPT.id)),
                new ShareAcknowledgementBatch(8, 8, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(25, 29, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(35, 37, List.of(AcknowledgeType.RELEASE.id))
        ));

        assertEquals(2, sharePartition.deliveryCompleteCount());

        // LSO is at 24.
        sharePartition.updateCacheAndOffsets(24);

        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(24, sharePartition.startOffset());
        assertEquals(39, sharePartition.endOffset());
        assertEquals(7, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Release acquired records for MEMBER_ID.
        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));

        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(10L).batchState());

        assertEquals("member-2", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());

        expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(21L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(22L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(23L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(24L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));

        assertEquals(0, sharePartition.deliveryCompleteCount());

        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(20L).offsetState());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(25L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(25L).batchState());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(30L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(30L).batchState());

        expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(35L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(36L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(37L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(38L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(39L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(35L).offsetState());
    }

    @Test
    public void testReleaseAcquiredRecordsBatchesPostStartOffsetMovementToStartOfBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        // LSO is at 10.
        sharePartition.updateCacheAndOffsets(10);

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Release acquired records.
        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(5L).batchState());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());

        // The records after the start offset are in non-Terminal states. Thus, deliveryCompleteCount is not changed.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testReleaseAcquiredRecordsBatchesPostStartOffsetMovementToMiddleOfBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        // LSO is at 11.
        sharePartition.updateCacheAndOffsets(11);

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Release acquired records.
        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(5L).batchState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));

        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // The records after the start offset are in non-Terminal states. Thus, deliveryCompleteCount is not changed.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testReleaseAcquiredRecordsDecreaseDeliveryCount() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(12, 13, List.of(AcknowledgeType.ACCEPT.id))));

        // Records 12 and 13 are ACKNOWLEDGED.
        assertEquals(2, sharePartition.deliveryCompleteCount());

        // LSO is at 11.
        sharePartition.updateCacheAndOffsets(11);

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(2, sharePartition.deliveryCompleteCount());

        // Before release, the delivery count was incremented.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // Release acquired records.
        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        // Check delivery count.
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        // After release, the delivery count was decremented.
        expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
        assertEquals(2, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockTimeoutForBatchesPostStartOffsetMovement() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        sharePartition.acquire("member-2", ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 15, fetchPartitionData(memoryRecords(15, 5)), FETCH_ISOLATION_HWM);

        fetchAcquiredRecords(sharePartition, memoryRecords(20, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(25, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(30, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(35, 5), 5);

        // Acknowledge records.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(6, 7, List.of(AcknowledgeType.ACCEPT.id)),
                new ShareAcknowledgementBatch(8, 8, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(25, 29, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(35, 37, List.of(AcknowledgeType.RELEASE.id))
        ));

        assertEquals(2, sharePartition.deliveryCompleteCount());

        // LSO is at 24.
        sharePartition.updateCacheAndOffsets(24);

        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(24, sharePartition.startOffset());
        assertEquals(39, sharePartition.endOffset());
        assertEquals(7, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> {
                Map<Long, InFlightState> expectedOffsetStateMap1 = new HashMap<>();
                expectedOffsetStateMap1.put(5L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap1.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap1.put(7L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap1.put(8L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap1.put(9L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));

                Map<Long, InFlightState> expectedOffsetStateMap2 = new HashMap<>();
                expectedOffsetStateMap2.put(20L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap2.put(21L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap2.put(22L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap2.put(23L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap2.put(24L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));

                Map<Long, InFlightState> expectedOffsetStateMap3 = new HashMap<>();
                expectedOffsetStateMap3.put(35L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap3.put(36L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap3.put(37L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap3.put(38L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap3.put(39L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));

                return sharePartition.cachedState().get(5L).offsetState().equals(expectedOffsetStateMap1) &&
                        sharePartition.cachedState().get(20L).offsetState().equals(expectedOffsetStateMap2) &&
                        sharePartition.cachedState().get(25L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(30L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(35L).offsetState().equals(expectedOffsetStateMap3);
            },
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of(5L, 6L, 7L, 8L, 9L), 20L, List.of(20L, 21L, 22L, 23L, 24L), 25L, List.of(), 30L, List.of(), 35L, List.of(35L, 36L, 37L, 38L, 39L))));

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(10L).batchState());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(15L).batchState());

        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockTimeoutForBatchesPostStartOffsetMovementToStartOfBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        // LSO is at 10.
        sharePartition.updateCacheAndOffsets(10);

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> sharePartition.cachedState().get(5L).batchMemberId().equals(EMPTY_MEMBER_ID) &&
                    sharePartition.cachedState().get(5L).batchState() == RecordState.ARCHIVED &&
                    sharePartition.cachedState().get(10L).batchMemberId().equals(EMPTY_MEMBER_ID) &&
                    sharePartition.cachedState().get(10L).batchState() == RecordState.AVAILABLE,
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of(), 10L, List.of())));

        // All records after startOffset are in non-Terminal states. Thus, deliveryCompleteCount is not changed.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockTimeoutForBatchesPostStartOffsetMovementToMiddleOfBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        // LSO is at 11.
        sharePartition.updateCacheAndOffsets(11);

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> {
                Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
                expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(11L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
                return sharePartition.cachedState().get(10L).offsetState().equals(expectedOffsetStateMap) &&
                        sharePartition.cachedState().get(5L).batchMemberId().equals(EMPTY_MEMBER_ID) &&
                        sharePartition.cachedState().get(5L).batchState() == RecordState.ARCHIVED;
            },
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(5L, List.of(), 10L, List.of(10L, 11L, 12L, 13L, 14L))));

        // All records after startOffset are in non-Terminal states. Thus, deliveryCompleteCount is not changed.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testScheduleAcquisitionLockTimeoutValueFromGroupConfig() {
        GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        GroupConfig groupConfig = Mockito.mock(GroupConfig.class);
        int expectedDurationMs = 500;
        Mockito.when(groupConfigManager.groupConfig(GROUP_ID)).thenReturn(Optional.of(groupConfig));
        Mockito.when(groupConfig.shareRecordLockDurationMs()).thenReturn(expectedDurationMs);

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withGroupConfigManager(groupConfigManager).build();

        AcquisitionLockTimerTask timerTask = sharePartition.scheduleAcquisitionLockTimeout(MEMBER_ID, 100L, 200L);

        Mockito.verify(groupConfigManager, Mockito.times(2)).groupConfig(GROUP_ID);
        Mockito.verify(groupConfig).shareRecordLockDurationMs();
        assertEquals(expectedDurationMs, timerTask.delayMs);
    }

    @Test
    public void testScheduleAcquisitionLockTimeoutValueUpdatesSuccessfully() {
        GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        GroupConfig groupConfig = Mockito.mock(GroupConfig.class);
        int expectedDurationMs1 = 500;
        int expectedDurationMs2 = 1000;
        Mockito.when(groupConfigManager.groupConfig(GROUP_ID)).thenReturn(Optional.of(groupConfig));
        // First invocation of shareRecordLockDurationMs() returns 500, and the second invocation returns 1000
        Mockito.when(groupConfig.shareRecordLockDurationMs())
            .thenReturn(expectedDurationMs1)
            .thenReturn(expectedDurationMs2);

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withGroupConfigManager(groupConfigManager).build();

        AcquisitionLockTimerTask timerTask1 = sharePartition.scheduleAcquisitionLockTimeout(MEMBER_ID, 100L, 200L);

        Mockito.verify(groupConfigManager, Mockito.times(2)).groupConfig(GROUP_ID);
        Mockito.verify(groupConfig).shareRecordLockDurationMs();
        assertEquals(expectedDurationMs1, timerTask1.delayMs);

        AcquisitionLockTimerTask timerTask2 = sharePartition.scheduleAcquisitionLockTimeout(MEMBER_ID, 100L, 200L);

        Mockito.verify(groupConfigManager, Mockito.times(4)).groupConfig(GROUP_ID);
        Mockito.verify(groupConfig, Mockito.times(2)).shareRecordLockDurationMs();
        assertEquals(expectedDurationMs2, timerTask2.delayMs);
    }

    @Test
    public void testAcknowledgeBatchAndOffsetPostLsoMovement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        // LSO is at 12.
        sharePartition.updateCacheAndOffsets(12);
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(12, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Check cached state map.
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());

        // Acknowledge with RELEASE action.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(2, 6, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(10, 14, List.of(AcknowledgeType.RELEASE.id))));

        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());
        // No record is moved to Terminal state, thus deliveryCompleteCount is not changed.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(12, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());
        assertNotNull(sharePartition.cachedState().get(2L).batchAcquisitionLockTimeoutTask());

        // Check cached offset state map.
        Map<Long, InFlightState>  expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));

        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());

        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcknowledgeBatchPostLsoMovement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(20, 5), 5);

        // LSO is at 14.
        sharePartition.updateCacheAndOffsets(14);
        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(14, sharePartition.startOffset());
        assertEquals(24, sharePartition.endOffset());
        assertEquals(3, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Acknowledge with ACCEPT action.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(2, 14, List.of(AcknowledgeType.ACCEPT.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());
        // Only record 14 is post startOffset and in a Terminal state. Thus, only that is considered for deliveryCompleteCount.
        assertEquals(1, sharePartition.deliveryCompleteCount());

        assertEquals(25, sharePartition.nextFetchOffset());
        // For cached state corresponding to entry 2, the offset states will be ARCHIVED, ARCHIVED, ARCHIVED, ARCHIVED and ACKNOWLEDGED.
        // Hence, it will get removed when calling maybeUpdateCachedStateAndOffsets() internally.
        assertEquals(14, sharePartition.startOffset());
        assertEquals(24, sharePartition.endOffset());
        assertEquals(3, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());

        // Check cached state offset map.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testLsoMovementThenAcquisitionLockTimeoutThenAcknowledge() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);

        // LSO is at 7.
        sharePartition.updateCacheAndOffsets(7);
        assertEquals(7, sharePartition.nextFetchOffset());
        assertEquals(7, sharePartition.startOffset());
        assertEquals(7, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());

        // Check cached state map.
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());
        assertNotNull(sharePartition.cachedState().get(2L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.nextFetchOffset() == 7 && sharePartition.cachedState().isEmpty() &&
                            sharePartition.startOffset() == 7 && sharePartition.endOffset() == 7,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of()));
        assertEquals(0, sharePartition.deliveryCompleteCount());

        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Acknowledge with RELEASE action. This contains a batch that doesn't exist at all.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(2, 14, List.of(AcknowledgeType.RELEASE.id))));

        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testLsoMovementThenAcquisitionLockTimeoutThenAcknowledgeBatchLastOffsetAheadOfStartOffsetBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(1, 2), 2);

        // LSO is at 3.
        sharePartition.updateCacheAndOffsets(3);
        assertEquals(3, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.startOffset());
        assertEquals(3, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Check cached state map.
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(1L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(1L).batchState());
        assertNotNull(sharePartition.cachedState().get(1L).batchAcquisitionLockTimeoutTask());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
                () -> sharePartition.nextFetchOffset() == 3 && sharePartition.cachedState().isEmpty() &&
                        sharePartition.startOffset() == 3 && sharePartition.endOffset() == 3,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> assertionFailedMessage(sharePartition, Map.of()));

        assertEquals(0, sharePartition.deliveryCompleteCount());

        fetchAcquiredRecords(sharePartition, memoryRecords(3, 2), 2);
        fetchAcquiredRecords(sharePartition, memoryRecords(5, 3), 3);

        assertEquals(8, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.startOffset());
        assertEquals(7, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Acknowledge with RELEASE action. This contains a batch that doesn't exist at all.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(1, 7, List.of(AcknowledgeType.RELEASE.id))));

        assertEquals(3, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.startOffset());
        assertEquals(7, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(3L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(3L).batchState());
        assertNull(sharePartition.cachedState().get(3L).batchAcquisitionLockTimeoutTask());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testWriteShareGroupStateWithNullResponse() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Void> result = sharePartition.writeShareGroupState(List.of());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, result);
    }

    @Test
    public void testWriteShareGroupStateWithNullTopicsData() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(null);
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        CompletableFuture<Void> result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, result);
    }

    @Test
    public void testWriteShareGroupStateWithInvalidTopicsData() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        // TopicsData is empty.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of());
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        CompletableFuture<Void> writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, writeResult);

        // TopicsData contains more results than expected.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of()),
                new TopicData<>(Uuid.randomUuid(), List.of())));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, writeResult);

        // TopicsData contains no partition data.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of())));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, writeResult);

        // TopicsData contains wrong topicId.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(Uuid.randomUuid(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, writeResult);

        // TopicsData contains more partition data than expected.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message()),
                        PartitionFactory.newPartitionErrorData(1, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, writeResult);

        // TopicsData contains wrong partition.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(1, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, writeResult);
    }

    @Test
    public void testWriteShareGroupStateWithWriteException() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition1 = SharePartitionBuilder.builder().withPersister(persister).build();

        Mockito.when(persister.writeState(Mockito.any())).thenReturn(FutureUtils.failedFuture(new RuntimeException("Write exception")));
        CompletableFuture<Void> writeResult = sharePartition1.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(IllegalStateException.class, writeResult);

        persister = Mockito.mock(Persister.class);
        // Throw exception for write state.
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition2 = SharePartitionBuilder.builder().withPersister(persister).build();

        Mockito.when(persister.writeState(Mockito.any())).thenThrow(new RuntimeException("Write exception"));
        assertThrows(RuntimeException.class, () -> sharePartition2.writeShareGroupState(anyList()));
    }

    @Test
    public void testWriteShareGroupState() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        CompletableFuture<Void> result = sharePartition.writeShareGroupState(anyList());
        assertNull(result.join());
        assertFalse(result.isCompletedExceptionally());
    }

    @Test
    public void testWriteShareGroupStateFailure() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withState(SharePartitionState.ACTIVE)
            .build();
        // Mock Write state RPC to return error response, NOT_COORDINATOR.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NOT_COORDINATOR.code(), Errors.NOT_COORDINATOR.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        CompletableFuture<Void> result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(CoordinatorNotAvailableException.class, result);

        // Mock Write state RPC to return error response, COORDINATOR_NOT_AVAILABLE.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.COORDINATOR_NOT_AVAILABLE.code(), Errors.COORDINATOR_NOT_AVAILABLE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(CoordinatorNotAvailableException.class, result);

        // Mock Write state RPC to return error response, COORDINATOR_LOAD_IN_PROGRESS.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.COORDINATOR_LOAD_IN_PROGRESS.code(), Errors.COORDINATOR_LOAD_IN_PROGRESS.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(CoordinatorNotAvailableException.class, result);

        // Mock Write state RPC to return error response, GROUP_ID_NOT_FOUND.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(GroupIdNotFoundException.class, result);

        // Mock Write state RPC to return error response, UNKNOWN_TOPIC_OR_PARTITION.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), Errors.UNKNOWN_TOPIC_OR_PARTITION.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(UnknownTopicOrPartitionException.class, result);

        // Mock Write state RPC to return error response, FENCED_STATE_EPOCH.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.FENCED_STATE_EPOCH.code(), Errors.FENCED_STATE_EPOCH.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(NotLeaderOrFollowerException.class, result);

        // Mock Write state RPC to return error response, FENCED_LEADER_EPOCH.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.FENCED_LEADER_EPOCH.code(), Errors.FENCED_LEADER_EPOCH.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(NotLeaderOrFollowerException.class, result);

        // Mock Write state RPC to return error response, UNKNOWN_SERVER_ERROR.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.UNKNOWN_SERVER_ERROR.code(), Errors.UNKNOWN_SERVER_ERROR.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(UnknownServerException.class, result);
    }

    @Test
    public void testWriteShareGroupStateWithNoOpStatePersister() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        List<PersisterStateBatch> stateBatches = List.of(
                new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3));

        CompletableFuture<Void> result = sharePartition.writeShareGroupState(stateBatches);
        assertNull(result.join());
        assertFalse(result.isCompletedExceptionally());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgeTypeAccept() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(250), 250);

        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(0, 249, List.of(AcknowledgeType.ACCEPT.id))));

        assertEquals(250, sharePartition.nextFetchOffset());
        // The SPSO should only move when the initial records in cached state are acknowledged with type ACKNOWLEDGE or ARCHIVED.
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireRecords());
        // The records have been accepted, thus they are removed from the cached state.
        assertEquals(0, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgeTypeReject() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(250), 250);

        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(0, 249, List.of((AcknowledgeType.REJECT.id)))));

        assertEquals(250, sharePartition.nextFetchOffset());
        // The SPSO should only move when the initial records in cached state are acknowledged with type ACKNOWLEDGE or ARCHIVED.
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireRecords());
        // The records have been rejected, thus they are removed from the cached state.
        assertEquals(0, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgeTypeRelease() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(250), 250);
        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(0, 249, List.of(AcknowledgeType.RELEASE.id))));

        // The SPSO should only move when the initial records in cached state are acknowledged with type ACKNOWLEDGE or ARCHIVED.
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireRecords());
        // The records have been released, thus they are not removed from the cached state.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgementsFromBeginningForBatchSubset() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightRecords(20)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(15), 15);
        assertTrue(sharePartition.canAcquireRecords());

        fetchAcquiredRecords(sharePartition, memoryRecords(15, 15), 15);
        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(0, 12, List.of(AcknowledgeType.ACCEPT.id))));

        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(0L).offsetState().get(12L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(13L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(13, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgementsFromBeginningForEntireBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightRecords(20)
            .withState(SharePartitionState.ACTIVE)
            .build();
        fetchAcquiredRecords(sharePartition, memoryRecords(15), 15);
        assertTrue(sharePartition.canAcquireRecords());

        fetchAcquiredRecords(sharePartition, memoryRecords(15, 15), 15);
        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(0, 14, List.of(AcknowledgeType.REJECT.id))));

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgementsInBetween() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightRecords(20)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(15), 15);
        assertTrue(sharePartition.canAcquireRecords());

        fetchAcquiredRecords(sharePartition, memoryRecords(15, 15), 15);
        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(10, 14, List.of(AcknowledgeType.REJECT.id))));

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(9L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).offsetState().get(10L).state());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        assertFalse(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
        // Records 10 -> 14 are in ARCHIVED state, and so deliveryCompleteCount is 5.
        assertEquals(5, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAllRecordsInCachedStateAreAcknowledged() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightRecords(20)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(15), 15);
        assertTrue(sharePartition.canAcquireRecords());

        fetchAcquiredRecords(sharePartition, memoryRecords(15, 15), 15);
        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(0, 29, List.of(AcknowledgeType.ACCEPT.id))));

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(30, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
        // Cache state is empty.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeUpdateCachedStateMultipleAcquisitionsAndAcknowledgements() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightRecords(100)
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(20), 20);
        assertTrue(sharePartition.canAcquireRecords());

        fetchAcquiredRecords(sharePartition, memoryRecords(20, 20), 20);
        assertTrue(sharePartition.canAcquireRecords());

        fetchAcquiredRecords(sharePartition, memoryRecords(40, 20), 20);
        assertTrue(sharePartition.canAcquireRecords());

        // First Acknowledgement for the first batch of records 0-19.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(0, 19, List.of(AcknowledgeType.ACCEPT.id))));

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(20, sharePartition.startOffset());
        assertEquals(59, sharePartition.endOffset());
        assertEquals(60, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        fetchAcquiredRecords(sharePartition, memoryRecords(60, 20), 20);
        assertTrue(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(20, 49, List.of(AcknowledgeType.ACCEPT.id))));

        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(40L).offsetState().get(49L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(40L).offsetState().get(50L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(60L).batchState());
        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(50, sharePartition.startOffset());
        assertEquals(79, sharePartition.endOffset());
        assertEquals(80, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        fetchAcquiredRecords(sharePartition, memoryRecords(80, 100), 100);
        assertFalse(sharePartition.canAcquireRecords());

        // Final Acknowledgement, all records are acknowledged here.
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(50, 179, List.of(AcknowledgeType.REJECT.id))));

        assertEquals(0, sharePartition.cachedState().size());
        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(180, sharePartition.startOffset());
        assertEquals(180, sharePartition.endOffset());
        assertEquals(180, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        fetchAcquiredRecords(sharePartition, memoryRecords(180, 20), 20);

        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(180L).batchState());
        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(180, sharePartition.startOffset());
        assertEquals(199, sharePartition.endOffset());
        assertEquals(200, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testMaybeUpdateCachedStateGapAfterLastOffsetAcknowledged() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(11L, 20L, RecordState.AVAILABLE.id, (short) 1),
                        new PersisterStateBatch(31L, 40L, RecordState.ARCHIVED.id, (short) 1) // There is a gap from 21 to 30
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();
        assertEquals(10, sharePartition.deliveryCompleteCount());

        // Acquiring the first AVAILABLE batch from 11 to 20
        fetchAcquiredRecords(sharePartition, memoryRecords(11, 10), 10);
        assertTrue(sharePartition.canAcquireRecords());

        // Sending acknowledgement for the first batch from 11 to 20
        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(11, 20, List.of(AcknowledgeType.ACCEPT.id))));

        assertTrue(sharePartition.canAcquireRecords());
        // After the acknowledgement is done successfully, maybeUpdateCachedStateAndOffsets method is invoked to see
        // if the start offset can be moved ahead. The last offset acknowledged is 20. But instead of moving start
        // offset to the next batch in the cached state (31 to 40), it is moved to the next offset of the last
        // acknowledged offset (21). This is because there is an acquirable gap in the cached state from 21 to 30.
        assertEquals(21, sharePartition.startOffset());
        assertEquals(40, sharePartition.endOffset());
        assertEquals(21, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.deliveryCompleteCount());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        assertEquals(21, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());
    }

    @Test
    public void testCanAcquireRecordsReturnsTrue() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());

        fetchAcquiredRecords(sharePartition, memoryRecords(150), 150);

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());
    }

    @Test
    public void testCanAcquireRecordsChangeResponsePostAcknowledgement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());

        fetchAcquiredRecords(sharePartition, memoryRecords(150), 150);
        assertTrue(sharePartition.canAcquireRecords());

        fetchAcquiredRecords(sharePartition, memoryRecords(150, 100), 100);
        assertFalse(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(0, 249, List.of(AcknowledgeType.ACCEPT.id))));

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
    }

    @Test
    public void testCanAcquireRecordsAfterReleaseAcknowledgement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(150), 150);
        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        fetchAcquiredRecords(sharePartition, memoryRecords(150, 100), 100);
        assertFalse(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(0, 89, List.of(AcknowledgeType.RELEASE.id))));

        // The SPSO should only move when the initial records in cached state are acknowledged with type ACKNOWLEDGE or ARCHIVED.
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        // The records have been released, thus they are still available for being acquired.
        assertTrue(sharePartition.canAcquireRecords());
    }

    @Test
    public void testCanAcquireRecordsAfterArchiveAcknowledgement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(150), 150);
        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        fetchAcquiredRecords(sharePartition, memoryRecords(150, 100), 100);
        assertFalse(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(0, 89, List.of(AcknowledgeType.REJECT.id))));

        // The SPSO should only move when the initial records in cached state are acknowledged with type ACKNOWLEDGE or ARCHIVED.
        assertEquals(90, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireRecords());
    }

    @Test
    public void testCanAcquireRecordsAfterAcceptAcknowledgement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(150), 150);
        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        fetchAcquiredRecords(sharePartition, memoryRecords(150, 100), 100);
        assertFalse(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        sharePartition.acknowledge(MEMBER_ID, List.of(
                        new ShareAcknowledgementBatch(0, 89, List.of(AcknowledgeType.ACCEPT.id))));

        // The SPSO should only move when the initial records in cached state are acknowledged with type ACKNOWLEDGE or ARCHIVED.
        assertEquals(90, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireRecords());
    }

    @Test
    public void testAcknowledgeBatchWithWriteShareGroupStateFailure() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), Errors.UNKNOWN_TOPIC_OR_PARTITION.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 10), 10);

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID,
                List.of(new ShareAcknowledgementBatch(5, 14, List.of(AcknowledgeType.ACCEPT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(UnknownTopicOrPartitionException.class, ackResult);

        // Due to failure in writeShareGroupState, the cached state should not be updated.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
    }

    @Test
    public void testAcknowledgeOffsetWithWriteShareGroupStateFailure() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 6), 6);
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                List.of(new ShareAcknowledgementBatch(8, 10, List.of(AcknowledgeType.REJECT.id))));
        assertTrue(ackResult.isCompletedExceptionally());

        // Due to failure in writeShareGroupState, the cached state should not be updated.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(5L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(6L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(7L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(8L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(9L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(10L).state());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(5L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(6L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(7L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(8L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(9L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(10L).memberId());
    }

    @Test
    public void testAcknowledgeSubsetWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 7), 7);
        sharePartition.acknowledge(MEMBER_ID,
                List.of(new ShareAcknowledgementBatch(5, 7, List.of(AcknowledgeType.ACCEPT.id))));

        // Acknowledge subset with another member.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge("member-2",
                List.of(new ShareAcknowledgementBatch(9, 11, List.of(AcknowledgeType.ACCEPT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRecordStateException.class, ackResult);
    }

    @Test
    public void testAcknowledgeWithAnotherMemberRollbackBatchError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);

        sharePartition.acquire("member-2", ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 10, fetchPartitionData(memoryRecords(10, 5)), FETCH_ISOLATION_HWM);

        fetchAcquiredRecords(sharePartition, memoryRecords(15, 5), 5);

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.RELEASE.id)),
                // Acknowledging batch with another member will cause failure and rollback.
                new ShareAcknowledgementBatch(10, 14, List.of(AcknowledgeType.ACCEPT.id)),
                new ShareAcknowledgementBatch(15, 19, List.of(AcknowledgeType.ACCEPT.id))));

        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRecordStateException.class, ackResult);

        // State should be rolled back to the previous state for any changes.
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
    }

    @Test
    public void testAcknowledgeWithAnotherMemberRollbackSubsetError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);
        sharePartition.acquire("member-2", ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 15, fetchPartitionData(memoryRecords(15, 5)), FETCH_ISOLATION_HWM);

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(10, 14, List.of(AcknowledgeType.ACCEPT.id)),
                // Acknowledging subset with another member will cause failure and rollback.
                new ShareAcknowledgementBatch(16, 18, List.of(AcknowledgeType.ACCEPT.id))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(InvalidRecordStateException.class, ackResult);

        assertEquals(3, sharePartition.cachedState().size());
        // Check the state of the cache. State should be rolled back to the previous state for any changes.
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxDeliveryCount(2)
            .withState(SharePartitionState.ACTIVE)
            .build();
        MemoryRecords records = memoryRecords(5, 10);

        fetchAcquiredRecords(sharePartition, records, 10);
        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(5, 14, List.of(AcknowledgeType.RELEASE.id))));

        fetchAcquiredRecords(sharePartition, records, 10);
        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(5, 14, List.of(AcknowledgeType.RELEASE.id))));

        // All the records in the batch reached the max delivery count, hence they got archived and the cached state cleared.
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(15, sharePartition.endOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubset() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxDeliveryCount(2)
            .withState(SharePartitionState.ACTIVE)
            .build();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(10, 5);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(15, 5);

        fetchAcquiredRecords(sharePartition, records1, 5);
        fetchAcquiredRecords(sharePartition, records2, 5);

        sharePartition.acknowledge(MEMBER_ID, new ArrayList<>(List.of(
                new ShareAcknowledgementBatch(10, 12, List.of(AcknowledgeType.ACCEPT.id)),
                new ShareAcknowledgementBatch(13, 16, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(17, 19, List.of(AcknowledgeType.ACCEPT.id)))));

        // Send next batch from offset 13, only 2 records should be acquired.
        fetchAcquiredRecords(sharePartition, records1, 2);
        // Send next batch from offset 15, only 2 records should be acquired.
        fetchAcquiredRecords(sharePartition, records2, 2);

        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(13, 16, List.of(AcknowledgeType.RELEASE.id))));

        assertEquals(20, sharePartition.nextFetchOffset());
        // Cached state will be empty because after the second release, the acquired records will now have moved to
        // ARCHIVE state, since their max delivery count exceeded. Also, now since all the records are either in ACKNOWLEDGED or ARCHIVED
        // state, cached state should be empty.
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetAndCachedStateNotCleared() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxDeliveryCount(2)
            .withState(SharePartitionState.ACTIVE)
            .build();
        // First fetch request with 5 records starting from offset 0.
        MemoryRecords records1 = memoryRecords(5);

        fetchAcquiredRecords(sharePartition, records1, 5);
        sharePartition.acknowledge(MEMBER_ID, new ArrayList<>(List.of(
                new ShareAcknowledgementBatch(0, 1, List.of(AcknowledgeType.RELEASE.id)))));

        // Send next batch from offset 0, only 2 records should be acquired.
        fetchAcquiredRecords(sharePartition, memoryRecords(2), 2);
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(0, 4, List.of(AcknowledgeType.RELEASE.id))));

        assertEquals(2, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(0L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(1L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(2L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(3L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(4L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testNextFetchOffsetPostAcquireAndAcknowledgeFunctionality() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(10);
        String memberId1 = "memberId-1";
        String memberId2 = "memberId-2";

        sharePartition.acquire(memberId1, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, DEFAULT_FETCH_OFFSET, fetchPartitionData(records1), FETCH_ISOLATION_HWM);

        assertFalse(sharePartition.findNextFetchOffset());
        assertEquals(10, sharePartition.nextFetchOffset());

        sharePartition.acquire(memberId2, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 10, fetchPartitionData(memoryRecords(10, 10)), FETCH_ISOLATION_HWM);

        assertFalse(sharePartition.findNextFetchOffset());
        assertEquals(20, sharePartition.nextFetchOffset());

        sharePartition.acknowledge(memberId1, List.of(
                new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.RELEASE.id))));

        assertTrue(sharePartition.findNextFetchOffset());
        assertEquals(5, sharePartition.nextFetchOffset());

        sharePartition.acquire(memberId1, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, DEFAULT_FETCH_OFFSET, fetchPartitionData(records1), FETCH_ISOLATION_HWM);

        assertTrue(sharePartition.findNextFetchOffset());
        assertEquals(20, sharePartition.nextFetchOffset());
        assertFalse(sharePartition.findNextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWithMultipleConsumers() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightRecords(100)
            .withState(SharePartitionState.ACTIVE)
            .build();
        MemoryRecords records1 = memoryRecords(3);
        String memberId1 = MEMBER_ID;
        String memberId2 = "member-2";

        sharePartition.acquire(memberId1, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, DEFAULT_FETCH_OFFSET, fetchPartitionData(records1), FETCH_ISOLATION_HWM);
        assertEquals(3, sharePartition.nextFetchOffset());

        sharePartition.acknowledge(memberId1, List.of(
                new ShareAcknowledgementBatch(0, 2, List.of(AcknowledgeType.RELEASE.id))));
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        sharePartition.acquire(memberId2, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 3, fetchPartitionData(memoryRecords(3, 2)), FETCH_ISOLATION_HWM);
        assertEquals(0, sharePartition.nextFetchOffset());

        sharePartition.acquire(memberId1, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, DEFAULT_FETCH_OFFSET, fetchPartitionData(records1), FETCH_ISOLATION_HWM);
        assertEquals(5, sharePartition.nextFetchOffset());

        sharePartition.acknowledge(memberId2, List.of(
                new ShareAcknowledgementBatch(3, 4, List.of(AcknowledgeType.RELEASE.id))));
        assertEquals(3, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testNumberOfWriteCallsOnUpdates() {
        SharePartition sharePartition = Mockito.spy(SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build());

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(2, 6, List.of(AcknowledgeType.ACCEPT.id))));
        // Acknowledge records will induce 1 write state RPC call via function isWriteShareGroupStateSuccessful.
        Mockito.verify(sharePartition, Mockito.times(1)).writeShareGroupState(anyList());

        sharePartition.releaseAcquiredRecords(MEMBER_ID);
        // Release acquired records will induce 0 write state RPC call via function isWriteShareGroupStateSuccessful
        // because the in-flight batch has been acknowledged. Hence, the total calls remain 1.
        Mockito.verify(sharePartition, Mockito.times(1)).writeShareGroupState(anyList());
    }

    @Test
    public void testReacquireSubsetWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(5, 5);

        fetchAcquiredRecords(sharePartition, records1, 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 12), 12);

        sharePartition.acknowledge(MEMBER_ID, List.of(
                new ShareAcknowledgementBatch(5, 11, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(12, 13, List.of(ACKNOWLEDGE_TYPE_GAP_ID)),
                new ShareAcknowledgementBatch(14, 15, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(17, 20, List.of(AcknowledgeType.RELEASE.id))));

        // Records 12-13 have been identified as gaps, hence they are kept in the cache as ARCHIVED state.
        assertEquals(2, sharePartition.deliveryCompleteCount());

        // Reacquire with another member.
        sharePartition.acquire("member-2", ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 5, fetchPartitionData(records1), FETCH_ISOLATION_HWM);
        assertEquals(10, sharePartition.nextFetchOffset());

        // Reacquire with another member.
        sharePartition.acquire("member-2", ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 10, fetchPartitionData(memoryRecords(10, 7)), FETCH_ISOLATION_HWM);
        assertEquals(17, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(2, sharePartition.cachedState().get(5L).batchDeliveryCount());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        // Records 10-11, 14-15 were reacquired by member-2.
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        // Records 12-13 were kept as gapOffsets, hence they are not reacquired and are kept in ARCHIVED state.
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        // Record 16 was not released in the acknowledgements. It was included in the reacquire by member-2,
        // still its ownership is with member-1 and delivery count is 1.
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(21L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testMaybeInitializeWhenReadStateRpcReturnsZeroAvailableRecords() {
        List<PersisterStateBatch> stateBatches = new ArrayList<>();
        stateBatches.add(new PersisterStateBatch(233L, 233L, RecordState.ARCHIVED.id, (short) 1));
        for (int i = 0; i < 500; i++) {
            stateBatches.add(new PersisterStateBatch(234L + i, 234L + i, RecordState.ACKNOWLEDGED.id, (short) 1));
        }

        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionAllData(0, 3, 233L, Errors.NONE.code(), Errors.NONE.message(),
                                stateBatches)))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertTrue(sharePartition.cachedState().isEmpty());
        assertEquals(734, sharePartition.nextFetchOffset());
        assertEquals(734, sharePartition.startOffset());
        assertEquals(734, sharePartition.endOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquireWithWriteShareGroupStateDelay() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns true with a delay of 5 sec.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));

        CompletableFuture<WriteShareGroupStateResult> future = new CompletableFuture<>();
        // persister.writeState RPC will not complete instantaneously due to which commit won't happen for acknowledged offsets.
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future);

        fetchAcquiredRecords(sharePartition, memoryRecords(5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);

        List<ShareAcknowledgementBatch> acknowledgementBatches = new ArrayList<>();
        acknowledgementBatches.add(new ShareAcknowledgementBatch(2, 3, List.of(AcknowledgeType.RELEASE.id)));
        acknowledgementBatches.add(new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.RELEASE.id)));
        // Acknowledge 2-3, 5-9 offsets with RELEASE acknowledge type.
        sharePartition.acknowledge(MEMBER_ID, acknowledgementBatches);

        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(0L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(2L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(3L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(4L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());

        // Even though offsets 2-3, 5-9 are in available state, but they won't be acquired since they are still in transition from ACQUIRED
        // to AVAILABLE state as the write state RPC has not completed yet, so the commit hasn't happened yet.
        fetchAcquiredRecords(sharePartition, memoryRecords(15), 5);

        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(0L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(2L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(3L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(4L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());

        // persister.writeState RPC will complete now. This is going to commit all the acknowledged batches. Hence, their
        // rollBack state will become null and they will be available for acquire again.
        future.complete(writeShareGroupStateResult);
        fetchAcquiredRecords(sharePartition, memoryRecords(15), 7);
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(0L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(2L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(3L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(4L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
    }

    @Test
    public void testComputeStartOffsetAdvanceResultWhenGapAtBeginning() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(31L, 40L, RecordState.ARCHIVED.id, (short) 1)
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        sharePartition.maybeInitialize();

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);

        // Since there is a gap in the beginning, the persisterReadResultGapWindow window is same as the cachedState
        assertEquals(11, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(40, persisterReadResultGapWindow.endOffset());

        SharePartition.OffsetAndMetadata result = sharePartition.findLastOffsetAcknowledgedAndMetadata();

        // Since the persisterReadResultGapWindow window begins at startOffset, we cannot count any of the offsets as acknowledged.
        // Thus, lastAckedOffset should be -1 and numTerminalRecords should be 0.
        assertEquals(-1, result.lastAcknowledgedOffset());
        assertEquals(0, result.numTerminalRecords());
    }

    @Test
    public void testCacheUpdateWhenBatchHasOngoingTransition() {
        Persister persister = Mockito.mock(Persister.class);

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withPersister(persister)
            .build();
        // Acquire a single batch.
        fetchAcquiredRecords(
            sharePartition.acquire(MEMBER_ID, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 21,
                fetchPartitionData(memoryRecords(21, 10)), FETCH_ISOLATION_HWM
            ), 10
        );

        // Validate that there is no ongoing transition.
        assertFalse(sharePartition.cachedState().get(21L).batchHasOngoingStateTransition());
        // Return a future which will be completed later, so the batch state has ongoing transition.
        CompletableFuture<WriteShareGroupStateResult> future = new CompletableFuture<>();
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future);
        // Acknowledge batch to create ongoing transition.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(21, 30, List.of(AcknowledgeType.ACCEPT.id))));

        // Assert the start offset has not moved and batch has ongoing transition.
        assertEquals(21L, sharePartition.startOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().get(21L).batchHasOngoingStateTransition());

        // Validate that offset can't be moved because batch has ongoing transition.
        assertFalse(sharePartition.canMoveStartOffset());

        SharePartition.OffsetAndMetadata result = sharePartition.findLastOffsetAcknowledgedAndMetadata();
        assertEquals(-1, result.lastAcknowledgedOffset());
        assertEquals(0, result.numTerminalRecords());

        // Complete the future so acknowledge API can be completed, which updates the cache.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        future.complete(writeShareGroupStateResult);

        // Validate the cache has been updated.
        assertEquals(31L, sharePartition.startOffset());
        assertTrue(sharePartition.cachedState().isEmpty());
    }

    @Test
    public void testCacheUpdateWhenOffsetStateHasOngoingTransition() {
        Persister persister = Mockito.mock(Persister.class);

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withPersister(persister)
            .build();
        // Acquire a single batch.
        fetchAcquiredRecords(
            sharePartition.acquire(MEMBER_ID, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 21,
                fetchPartitionData(memoryRecords(21, 10)), FETCH_ISOLATION_HWM
            ), 10
        );

        // Validate that there is no ongoing transition.
        assertFalse(sharePartition.cachedState().get(21L).batchHasOngoingStateTransition());
        assertNull(sharePartition.cachedState().get(21L).offsetState());
        // Return a future which will be completed later, so the batch state has ongoing transition.
        CompletableFuture<WriteShareGroupStateResult> future = new CompletableFuture<>();
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future);
        // Acknowledge offsets to create ongoing transition.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(21, 23, List.of(AcknowledgeType.ACCEPT.id))));

        // Assert the start offset has not moved and offset state is now maintained. Offset state should
        // have ongoing transition.
        assertEquals(21L, sharePartition.startOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(21L).offsetState());
        assertTrue(sharePartition.cachedState().get(21L).offsetState().get(21L).hasOngoingStateTransition());
        assertTrue(sharePartition.cachedState().get(21L).offsetState().get(22L).hasOngoingStateTransition());
        assertTrue(sharePartition.cachedState().get(21L).offsetState().get(23L).hasOngoingStateTransition());
        // Only 21, 22 and 23 offsets should have ongoing state transition as the acknowledge request
        // contains 21-23 offsets.
        assertFalse(sharePartition.cachedState().get(21L).offsetState().get(24L).hasOngoingStateTransition());

        // Validate that offset can't be moved because batch has ongoing transition.
        assertFalse(sharePartition.canMoveStartOffset());

        SharePartition.OffsetAndMetadata result = sharePartition.findLastOffsetAcknowledgedAndMetadata();
        assertEquals(-1, result.lastAcknowledgedOffset());
        assertEquals(0, result.numTerminalRecords());

        // Complete the future so acknowledge API can be completed, which updates the cache.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        future.complete(writeShareGroupStateResult);

        // Validate the cache has been updated.
        assertEquals(24L, sharePartition.startOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(21L));
    }

    /**
     * Test the case where the fetch batch has first record offset greater than the record batch start offset.
     * Such batches can exist for compacted topics.
     */
    @Test
    public void testAcquireAndAcknowledgeWithRecordsAheadOfRecordBatchStartOffset() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        // Set the base offset at 5.
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.NONE,
            TimestampType.CREATE_TIME, 5, 2)) {
            // Append records from offset 10.
            memoryRecords(10, 2).records().forEach(builder::append);
            // Append records from offset 15.
            memoryRecords(15, 2).records().forEach(builder::append);
        }
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Complete batch from 5-16 will be acquired, hence 12 records.
        fetchAcquiredRecords(sharePartition, records, 12);
        // Partially acknowledge the batch from 5-16.
        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(5, 9, List.of(ACKNOWLEDGE_TYPE_GAP_ID)),
            new ShareAcknowledgementBatch(10, 11, List.of(AcknowledgeType.ACCEPT.id)),
            new ShareAcknowledgementBatch(12, 14, List.of(AcknowledgeType.REJECT.id)),
            new ShareAcknowledgementBatch(15, 16, List.of(AcknowledgeType.RELEASE.id))));

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));
        assertNotNull(sharePartition.cachedState().get(5L).offsetState());

        // after acknowledgements, the start offset moves to 15, and thus there are no Terminal records post that.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));

        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
    }

    /**
     * Test the case where the available cached batches never appear again in fetch response within the
     * previous fetch offset range. Also remove records from the previous fetch batches.
     * <p>
     * Such case can arise with compacted topics where complete batches are removed or records within
     * batches are removed.
     */
    @Test
    public void testAcquireWhenBatchesAreRemovedFromBetweenInSubsequentFetchData() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Create 3 batches of records for a single acquire.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 0, 5).close();
        memoryRecordsBuilder(buffer, 5, 15).close();
        memoryRecordsBuilder(buffer, 20, 15).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Acquire batch (0-34) which shall create single cache entry.
        fetchAcquiredRecords(sharePartition, records, 35);
        // Acquire another 3 individual batches of records.
        fetchAcquiredRecords(sharePartition, memoryRecords(40, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(45, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(50, 15), 15);
        // Release all batches in the cache.
        sharePartition.releaseAcquiredRecords(MEMBER_ID);
        // Validate cache has 4 entries.
        assertEquals(4, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Compact all batches and remove some of the batches from the fetch response.
        buffer = ByteBuffer.allocate(4096);
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.NONE,
            TimestampType.CREATE_TIME, 0, 2)) {
            // Append only 2 records for 0 offset batch starting from offset 1.
            memoryRecords(1, 2).records().forEach(builder::append);
        }
        // Do not include batch from offset 5. And compact batch starting at offset 20.
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.NONE,
            TimestampType.CREATE_TIME, 20, 2)) {
            // Append 2 records for 20 offset batch starting from offset 20.
            memoryRecords(20, 2).records().forEach(builder::append);
            // And append 2 records matching the end offset of the batch.
            memoryRecords(33, 2).records().forEach(builder::append);
        }
        // Send the full batch at offset 40.
        memoryRecordsBuilder(buffer, 40, 5).close();
        // Do not include batch from offset 45. And compact the batch at offset 50.
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.NONE,
            TimestampType.CREATE_TIME, 50, 2)) {
            // Append 5 records for 50 offset batch starting from offset 51.
            memoryRecords(51, 5).records().forEach(builder::append);
            // Append 2 records for in middle of the batch.
            memoryRecords(58, 2).records().forEach(builder::append);
            // And append 1 record prior to the end offset.
            memoryRecords(63, 1).records().forEach(builder::append);
        }
        buffer.flip();
        records = MemoryRecords.readableRecords(buffer);
        // Acquire the new compacted batches. The acquire method determines the acquisition range using
        // the first and last offsets of the fetched batches and acquires all available cached batches
        // within that range. That means the batch from offset 45-49 which is not included in the
        // fetch response will also be acquired. Similarly, for the batch from offset 5-19 which is
        // anyway in the bigger cached batch of 0-34, will also be acquired. This avoids iterating
        // through individual fetched batch boundaries; the client is responsible for reporting any
        // data gaps via acknowledgements. This test also covers the edge case where the last fetched
        // batch is compacted, and its last offset is before the previously cached version's last offset.
        // In this situation, the last batch's offset state tracking is initialized. This is handled
        // correctly because the client will send individual offset acknowledgements, which require offset
        // state tracking anyway. While this last scenario is unlikely in practice (as a batch's reported
        // last offset should remain correct even after compaction), the test verifies its proper handling.
        fetchAcquiredRecords(sharePartition, records, 59);
        assertEquals(64, sharePartition.nextFetchOffset());
        assertEquals(4, sharePartition.cachedState().size());
        sharePartition.cachedState().forEach((offset, inFlightState) -> {
            // All batches other than the last batch should have batch state maintained.
            if (offset < 50) {
                assertNotNull(inFlightState.batchState());
                assertEquals(RecordState.ACQUIRED, inFlightState.batchState());
            } else {
                assertNotNull(inFlightState.offsetState());
                inFlightState.offsetState().forEach((recordOffset, offsetState) -> {
                    // All offsets other than the last offset should be acquired.
                    RecordState recordState = recordOffset < 64 ? RecordState.ACQUIRED : RecordState.AVAILABLE;
                    assertEquals(recordState, offsetState.state(), "Incorrect state for offset: " + recordOffset);
                });
            }
        });
        // All in flight records are in a non-Terminal state.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    /**
     * This test verifies that cached batches which are no longer returned in fetch responses (starting
     * from the fetchOffset) are correctly archived. Archiving these batches is crucial for the SPSO
     * and the next fetch offset to advance. Without archiving, these offsets would be stuck, as the
     * cached batches would remain available.
     * <p>
     * This scenario can occur with compacted topics when entire batches, previously held in the cache,
     * are removed from the log at the offset where reading occurs.
     */
    @Test
    public void testAcquireWhenBatchesRemovedForFetchOffset() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 15), 15);
        // Release the batches in the cache.
        sharePartition.releaseAcquiredRecords(MEMBER_ID);
        // Validate cache has 3 entries.
        assertEquals(3, sharePartition.cachedState().size());

        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Compact second batch and remove first batch from the fetch response.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.NONE,
            TimestampType.CREATE_TIME, 5, 2)) {
            // Append only 4 records for 5th offset batch starting from offset 6.
            memoryRecords(6, 4).records().forEach(builder::append);
        }
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        // Only second batch should be acquired and first batch offsets should be archived. Send
        // fetchOffset as 0.
        fetchAcquiredRecords(sharePartition, records, 0, 0, 5);
        assertEquals(10, sharePartition.nextFetchOffset());
        // The next fetch offset has been updated, but the start offset should remain unchanged since
        // the acquire operation only marks offsets as archived. The start offset will be correctly
        // updated once any records are acknowledged.
        assertEquals(0, sharePartition.startOffset());

        // Releasing acquired records updates the cache and moves the start offset.
        sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertEquals(5, sharePartition.startOffset());
        assertEquals(5, sharePartition.nextFetchOffset());
        // Validate first batch has been removed from the cache.
        assertEquals(2, sharePartition.cachedState().size());
        sharePartition.cachedState().forEach((offset, inFlightState) -> {
            assertNotNull(inFlightState.batchState());
            assertEquals(RecordState.AVAILABLE, inFlightState.batchState());
        });

        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    /**
     * This test verifies that cached batches which are no longer returned in fetch responses are
     * correctly archived, when fetchOffset is within an already cached batch. Archiving these batches/offsets
     * is crucial for the SPSO and the next fetch offset to advance.
     * <p>
     * This scenario can occur with compacted topics when fetch triggers from an offset which is within
     * a cached batch, and respective batch is removed from the log.
     */
    @Test
    public void testAcquireWhenBatchesRemovedForFetchOffsetWithinBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 15), 15);
        // Acknowledge subset of the first batch offsets.
        sharePartition.acknowledge(MEMBER_ID, List.of(
            // Accept the 3 offsets of first batch.
            new ShareAcknowledgementBatch(5, 7, List.of(AcknowledgeType.ACCEPT.id)))).join();

        // After acknowledgements, the start offset moves past Terminal records, hence deliveryCompleteCount is 0.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Release the remaining batches/offsets in the cache.
        sharePartition.releaseAcquiredRecords(MEMBER_ID).join();
        // Validate cache has 2 entries.
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Mark fetch offset within the first batch to 8, first available offset.
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 15), 8, 0, 15);
        assertEquals(25, sharePartition.nextFetchOffset());
        // The next fetch offset has been updated, but the start offset should remain unchanged since
        // the acquire operation only marks offsets as archived. The start offset will be correctly
        // updated once any records are acknowledged.
        assertEquals(8, sharePartition.startOffset());
        // Since the fetchOffset in the acquire request was prior to the actual records fetched, the records 8 and 9 are marked
        // as ARCHIVED. Thus, there are 2 Terminal records in the cache.
        assertEquals(2, sharePartition.deliveryCompleteCount());

        // Releasing acquired records updates the cache and moves the start offset.
        sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertEquals(10, sharePartition.startOffset());
        assertEquals(10, sharePartition.nextFetchOffset());
        // Validate first batch has been removed from the cache.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        // Since the start offset has moved past all Terminal records, the count is 0.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    /**
     * This test verifies that when cached batch consists of multiple fetched batches but batches are
     * removed from the log, starting at fetch offset, then cached batch is updated.
     * <p>
     * This scenario can occur with compacted topics when entire batches, previously held in the cache,
     * are removed from the log at the offset where reading occurs.
     */
    @Test
    public void testAcquireWhenBatchesRemovedForFetchOffsetForSameCachedBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Create 3 batches of records for a single acquire.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 0, 5).close();
        memoryRecordsBuilder(buffer, 5, 15).close();
        memoryRecordsBuilder(buffer, 20, 15).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        // Acquire batch (0-34) which shall create single cache entry.
        fetchAcquiredRecords(sharePartition, records, 35);
        // Release the batches in the cache.
        sharePartition.releaseAcquiredRecords(MEMBER_ID);
        // Validate cache has 1 entry.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Compact second batch and remove first batch from the fetch response.
        buffer = ByteBuffer.allocate(4096);
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.NONE,
            TimestampType.CREATE_TIME, 5, 2)) {
            // Append only 4 records for 5th offset batch starting from offset 6.
            memoryRecords(6, 4).records().forEach(builder::append);
        }
        buffer.flip();
        records = MemoryRecords.readableRecords(buffer);

        // Only second batch should be acquired and first batch offsets should be archived. Send
        // fetchOffset as 0.
        fetchAcquiredRecords(sharePartition, records, 0, 0, 5);
        assertEquals(10, sharePartition.nextFetchOffset());
        // The next fetch offset has been updated, but the start offset should remain unchanged since
        // the acquire operation only marks offsets as archived. The start offset will be correctly
        // updated once any records are acknowledged.
        assertEquals(0, sharePartition.startOffset());
        assertEquals(5, sharePartition.deliveryCompleteCount());

        // Releasing acquired records updates the cache and moves the start offset.
        sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertEquals(5, sharePartition.startOffset());
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        sharePartition.cachedState().forEach((offset, inFlightState) -> {
            assertNotNull(inFlightState.offsetState());
            inFlightState.offsetState().forEach((recordOffset, offsetState) -> {
                RecordState recordState = recordOffset < 5 ? RecordState.ARCHIVED : RecordState.AVAILABLE;
                assertEquals(recordState, offsetState.state());
            });
        });
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    private String assertionFailedMessage(SharePartition sharePartition, Map<Long, List<Long>> offsets) {
        StringBuilder errorMessage = new StringBuilder(ACQUISITION_LOCK_NEVER_GOT_RELEASED + String.format(
            " timer size: %d, next fetch offset: %d\n",
            sharePartition.timer().size(),
            sharePartition.nextFetchOffset()));
        for (Map.Entry<Long, List<Long>> entry : offsets.entrySet()) {
            if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                errorMessage.append(String.format("batch start offset: %d\n", entry.getKey()));
                for (Long offset : entry.getValue()) {
                    errorMessage.append(String.format("\toffset: %d, offset state: %s, offset acquisition lock timeout task present: %b\n",
                        offset, sharePartition.cachedState().get(entry.getKey()).offsetState().get(offset).state().id(),
                        sharePartition.cachedState().get(entry.getKey()).offsetState().get(offset).acquisitionLockTimeoutTask() != null));
                }
            } else {
                errorMessage.append(String.format("batch start offset: %d, batch state: %s, batch acquisition lock timeout task present: %b\n",
                    entry.getKey(), sharePartition.cachedState().get(entry.getKey()).batchState().id(),
                    sharePartition.cachedState().get(entry.getKey()).batchAcquisitionLockTimeoutTask() != null));
            }
        }
        return errorMessage.toString();
    }

    @Test
    public void testFilterRecordBatchesFromAcquiredRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        List<AcquiredRecords> acquiredRecords1 = List.of(
            new AcquiredRecords().setFirstOffset(1).setLastOffset(5).setDeliveryCount((short) 1),
            new AcquiredRecords().setFirstOffset(10).setLastOffset(15).setDeliveryCount((short) 2),
            new AcquiredRecords().setFirstOffset(20).setLastOffset(25).setDeliveryCount((short) 1)
        );
        List<RecordBatch> recordBatches1 = List.of(
            memoryRecordsBuilder(2, 3).build().batches().iterator().next(),
            memoryRecordsBuilder(12, 3).build().batches().iterator().next()
        );
        assertEquals(
            List.of(
                new AcquiredRecords().setFirstOffset(1).setLastOffset(1).setDeliveryCount((short) 1),
                new AcquiredRecords().setFirstOffset(5).setLastOffset(5).setDeliveryCount((short) 1),
                new AcquiredRecords().setFirstOffset(10).setLastOffset(11).setDeliveryCount((short) 2),
                new AcquiredRecords().setFirstOffset(15).setLastOffset(15).setDeliveryCount((short) 2),
                new AcquiredRecords().setFirstOffset(20).setLastOffset(25).setDeliveryCount((short) 1)),
            sharePartition.filterRecordBatchesFromAcquiredRecords(acquiredRecords1, recordBatches1));

        List<AcquiredRecords> acquiredRecords2 = List.of(
            new AcquiredRecords().setFirstOffset(1).setLastOffset(4).setDeliveryCount((short) 3),
            new AcquiredRecords().setFirstOffset(5).setLastOffset(8).setDeliveryCount((short) 3),
            new AcquiredRecords().setFirstOffset(9).setLastOffset(30).setDeliveryCount((short) 2),
            new AcquiredRecords().setFirstOffset(31).setLastOffset(40).setDeliveryCount((short) 3)
        );
        List<RecordBatch> recordBatches2 = List.of(
            memoryRecordsBuilder(5, 21).build().batches().iterator().next(),
            memoryRecordsBuilder(31, 5).build().batches().iterator().next()
        );
        assertEquals(
            List.of(
                new AcquiredRecords().setFirstOffset(1).setLastOffset(4).setDeliveryCount((short) 3),
                new AcquiredRecords().setFirstOffset(26).setLastOffset(30).setDeliveryCount((short) 2),
                new AcquiredRecords().setFirstOffset(36).setLastOffset(40).setDeliveryCount((short) 3)

            ), sharePartition.filterRecordBatchesFromAcquiredRecords(acquiredRecords2, recordBatches2)
        );

        // Record batches is empty.
        assertEquals(acquiredRecords2, sharePartition.filterRecordBatchesFromAcquiredRecords(acquiredRecords2, List.of()));

        List<AcquiredRecords> acquiredRecords3 = List.of(
            new AcquiredRecords().setFirstOffset(0).setLastOffset(19).setDeliveryCount((short) 1)
        );
        List<RecordBatch> recordBatches3 = List.of(
            memoryRecordsBuilder(8, 1).build().batches().iterator().next(),
            memoryRecordsBuilder(18, 1).build().batches().iterator().next()
        );

        assertEquals(
            List.of(
                new AcquiredRecords().setFirstOffset(0).setLastOffset(7).setDeliveryCount((short) 1),
                new AcquiredRecords().setFirstOffset(9).setLastOffset(17).setDeliveryCount((short) 1),
                new AcquiredRecords().setFirstOffset(19).setLastOffset(19).setDeliveryCount((short) 1)

            ), sharePartition.filterRecordBatchesFromAcquiredRecords(acquiredRecords3, recordBatches3)
        );
    }

    @Test
    public void testAcquireWithReadCommittedIsolationLevel() {
        SharePartition sharePartition = Mockito.spy(SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build());

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 10, 5).close();
        memoryRecordsBuilder(buffer, 15, 5).close();
        memoryRecordsBuilder(buffer, 20, 15).close();
        memoryRecordsBuilder(buffer, 50, 8).close();
        memoryRecordsBuilder(buffer, 58, 10).close();
        memoryRecordsBuilder(buffer, 70, 5).close();

        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        FetchPartitionData fetchPartitionData = fetchPartitionData(records, newAbortedTransactions());

        // We are mocking the result of function fetchAbortedTransactionRecordBatches. The records present at these offsets need to be archived.
        // We won't be utilizing the aborted transactions passed in fetchPartitionData.
        when(sharePartition.fetchAbortedTransactionRecordBatches(fetchPartitionData.records.batches(), fetchPartitionData.abortedTransactions.get())).thenReturn(
            List.of(
                memoryRecordsBuilder(10, 5).build().batches().iterator().next(),
                memoryRecordsBuilder(58, 10).build().batches().iterator().next(),
                memoryRecordsBuilder(70, 5).build().batches().iterator().next()
            )
        );

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(
            sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                10 /* Batch size */,
                100,
                DEFAULT_FETCH_OFFSET,
                fetchPartitionData,
                FetchIsolation.TXN_COMMITTED),
            45 /* Gap of 15 records will be added to second batch, gap of 2 records will also be added to fourth batch */);

        assertEquals(List.of(
            new AcquiredRecords().setFirstOffset(15).setLastOffset(19).setDeliveryCount((short) 1),
            new AcquiredRecords().setFirstOffset(20).setLastOffset(49).setDeliveryCount((short) 1),
            new AcquiredRecords().setFirstOffset(50).setLastOffset(57).setDeliveryCount((short) 1),
            new AcquiredRecords().setFirstOffset(68).setLastOffset(69).setDeliveryCount((short) 1)
        ), acquiredRecordsList);
        assertEquals(75, sharePartition.nextFetchOffset());

        // Checking cached state.
        assertEquals(4, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().containsKey(10L));
        assertTrue(sharePartition.cachedState().containsKey(20L));
        assertTrue(sharePartition.cachedState().containsKey(50L));
        assertTrue(sharePartition.cachedState().containsKey(70L));
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertNotNull(sharePartition.cachedState().get(50L).offsetState());

        assertEquals(19L, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(49L, sharePartition.cachedState().get(20L).lastOffset());
        assertEquals(69L, sharePartition.cachedState().get(50L).lastOffset());
        assertEquals(74L, sharePartition.cachedState().get(70L).lastOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(70L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(70L).batchMemberId());

        assertNotNull(sharePartition.cachedState().get(20L).batchAcquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(70L).batchAcquisitionLockTimeoutTask());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(18L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(19L).acquisitionLockTimeoutTask());

        expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(50L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(51L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(52L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(53L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(54L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(55L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(56L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(57L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(58L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(59L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(60L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(61L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(62L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(63L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(64L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(65L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(66L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(67L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(68L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(69L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(50L).offsetState());

        assertNotNull(sharePartition.cachedState().get(50L).offsetState().get(50L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(50L).offsetState().get(51L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(50L).offsetState().get(52L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(50L).offsetState().get(53L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(50L).offsetState().get(54L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(50L).offsetState().get(55L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(50L).offsetState().get(56L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(50L).offsetState().get(57L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(50L).offsetState().get(58L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(50L).offsetState().get(59L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(50L).offsetState().get(60L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(50L).offsetState().get(61L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(50L).offsetState().get(62L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(50L).offsetState().get(63L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(50L).offsetState().get(64L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(50L).offsetState().get(65L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(50L).offsetState().get(66L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(50L).offsetState().get(67L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(50L).offsetState().get(68L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(50L).offsetState().get(69L).acquisitionLockTimeoutTask());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testContainsAbortMarker() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Record batch is not a control batch.
        RecordBatch recordBatch = mock(RecordBatch.class);
        when(recordBatch.isControlBatch()).thenReturn(false);
        assertFalse(sharePartition.containsAbortMarker(recordBatch));

        // Record batch is a control batch but doesn't contain any records.
        recordBatch = mock(RecordBatch.class);
        Iterator batchIterator = mock(Iterator.class);
        when(batchIterator.hasNext()).thenReturn(false);
        when(recordBatch.iterator()).thenReturn(batchIterator);
        when(recordBatch.isControlBatch()).thenReturn(true);
        assertFalse(sharePartition.containsAbortMarker(recordBatch));

        // Record batch is a control batch which contains a record of type ControlRecordType.ABORT.
        recordBatch = mock(RecordBatch.class);
        batchIterator = mock(Iterator.class);
        when(batchIterator.hasNext()).thenReturn(true);
        DefaultRecord record = mock(DefaultRecord.class);
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        // Buffer has to be created in a way that ControlRecordType.parse(buffer) returns ControlRecordType.ABORT.
        buffer.putShort((short) 5);
        buffer.putShort(ControlRecordType.ABORT.type());
        buffer.putInt(23432); // some field added in version 5
        buffer.flip();
        when(record.key()).thenReturn(buffer);
        when(batchIterator.next()).thenReturn(record);
        when(recordBatch.iterator()).thenReturn(batchIterator);
        when(recordBatch.isControlBatch()).thenReturn(true);
        assertTrue(sharePartition.containsAbortMarker(recordBatch));

        // Record batch is a control batch which contains a record of type ControlRecordType.COMMIT.
        recordBatch = mock(RecordBatch.class);
        batchIterator = mock(Iterator.class);
        when(batchIterator.hasNext()).thenReturn(true);
        record = mock(DefaultRecord.class);
        buffer = ByteBuffer.allocate(4096);
        // Buffer has to be created in a way that ControlRecordType.parse(buffer) returns ControlRecordType.COMMIT.
        buffer.putShort((short) 5);
        buffer.putShort(ControlRecordType.COMMIT.type());
        buffer.putInt(23432); // some field added in version 5
        buffer.flip();
        when(record.key()).thenReturn(buffer);
        when(batchIterator.next()).thenReturn(record);
        when(recordBatch.iterator()).thenReturn(batchIterator);
        when(recordBatch.isControlBatch()).thenReturn(true);
        assertFalse(sharePartition.containsAbortMarker(recordBatch));
    }

    @Test
    public void testFetchAbortedTransactionRecordBatchesForOnlyAbortedTransactions() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Case 1 - Creating 10 transactional records in a single batch followed by a ABORT marker record for producerId 1.
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        newTransactionalRecords(buffer, ControlRecordType.ABORT, 10, 1, 0);
        buffer.flip();
        Records records = MemoryRecords.readableRecords(buffer);

        List<FetchResponseData.AbortedTransaction> abortedTransactions = List.of(
            new FetchResponseData.AbortedTransaction().setFirstOffset(0).setProducerId(1)
        );
        // records from 0 to 9 should be archived because they are a part of aborted transactions.
        List<RecordBatch> actual = sharePartition.fetchAbortedTransactionRecordBatches(records.batches(), abortedTransactions);
        assertEquals(1, actual.size());
        assertEquals(0, actual.get(0).baseOffset());
        assertEquals(9, actual.get(0).lastOffset());
        assertEquals(1, actual.get(0).producerId());

        // Case 2: 3 individual batches each followed by a ABORT marker record for producerId 1.
        buffer = ByteBuffer.allocate(1024);
        newTransactionalRecords(buffer, ControlRecordType.ABORT, 1, 1, 0);
        newTransactionalRecords(buffer, ControlRecordType.ABORT, 1, 1, 2);
        newTransactionalRecords(buffer, ControlRecordType.ABORT, 1, 1, 4);
        buffer.flip();
        records = MemoryRecords.readableRecords(buffer);
        abortedTransactions = List.of(
            new FetchResponseData.AbortedTransaction().setFirstOffset(0).setProducerId(1),
            new FetchResponseData.AbortedTransaction().setFirstOffset(2).setProducerId(1),
            new FetchResponseData.AbortedTransaction().setFirstOffset(4).setProducerId(1)
        );

        actual = sharePartition.fetchAbortedTransactionRecordBatches(records.batches(), abortedTransactions);
        assertEquals(3, actual.size());
        assertEquals(0, actual.get(0).baseOffset());
        assertEquals(0, actual.get(0).lastOffset());
        assertEquals(1, actual.get(0).producerId());
        assertEquals(2, actual.get(1).baseOffset());
        assertEquals(2, actual.get(1).lastOffset());
        assertEquals(1, actual.get(1).producerId());
        assertEquals(4, actual.get(2).baseOffset());
        assertEquals(4, actual.get(2).lastOffset());
        assertEquals(1, actual.get(2).producerId());

        // Case 3: The producer id of records is different, so they should not be archived,
        buffer = ByteBuffer.allocate(1024);
        // We are creating 10 transactional records followed by a ABORT marker record for producerId 2.
        newTransactionalRecords(buffer, ControlRecordType.ABORT, 10, 2, 0);
        buffer.flip();
        records = MemoryRecords.readableRecords(buffer);
        abortedTransactions = List.of(
            new FetchResponseData.AbortedTransaction().setFirstOffset(0).setProducerId(1)
        );

        actual = sharePartition.fetchAbortedTransactionRecordBatches(records.batches(), abortedTransactions);
        assertEquals(0, actual.size());
    }

    @Test
    public void testFetchAbortedTransactionRecordBatchesForAbortedAndCommittedTransactions() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        newTransactionalRecords(buffer, ControlRecordType.ABORT, 2, 1, 0);
        newTransactionalRecords(buffer, ControlRecordType.COMMIT, 2, 2, 3);
        newTransactionalRecords(buffer, ControlRecordType.ABORT, 2, 2, 6);
        newTransactionalRecords(buffer, ControlRecordType.ABORT, 2, 1, 9);
        newTransactionalRecords(buffer, ControlRecordType.COMMIT, 2, 1, 12);
        newTransactionalRecords(buffer, ControlRecordType.ABORT, 2, 1, 15);
        buffer.flip();
        Records records = MemoryRecords.readableRecords(buffer);

        // Case 1 - Aborted transactions does not contain the record batch from offsets 6-7 with producer id 2.
        List<FetchResponseData.AbortedTransaction> abortedTransactions = List.of(
            new FetchResponseData.AbortedTransaction().setFirstOffset(0).setProducerId(1),
            new FetchResponseData.AbortedTransaction().setFirstOffset(6).setProducerId(1),
            new FetchResponseData.AbortedTransaction().setFirstOffset(9).setProducerId(1),
            new FetchResponseData.AbortedTransaction().setFirstOffset(15).setProducerId(1)
        );

        List<RecordBatch> actual = sharePartition.fetchAbortedTransactionRecordBatches(records.batches(), abortedTransactions);
        assertEquals(3, actual.size());
        assertEquals(0, actual.get(0).baseOffset());
        assertEquals(1, actual.get(0).lastOffset());
        assertEquals(1, actual.get(0).producerId());
        assertEquals(9, actual.get(1).baseOffset());
        assertEquals(10, actual.get(1).lastOffset());
        assertEquals(1, actual.get(1).producerId());
        assertEquals(15, actual.get(2).baseOffset());
        assertEquals(16, actual.get(2).lastOffset());
        assertEquals(1, actual.get(2).producerId());

        // Case 2 - Aborted transactions contains the record batch from offsets 6-7 with producer id 2.
        abortedTransactions = List.of(
            new FetchResponseData.AbortedTransaction().setFirstOffset(0).setProducerId(1),
            new FetchResponseData.AbortedTransaction().setFirstOffset(6).setProducerId(2),
            new FetchResponseData.AbortedTransaction().setFirstOffset(9).setProducerId(1),
            new FetchResponseData.AbortedTransaction().setFirstOffset(15).setProducerId(1)
        );

        actual = sharePartition.fetchAbortedTransactionRecordBatches(records.batches(), abortedTransactions);
        assertEquals(4, actual.size());
        assertEquals(0, actual.get(0).baseOffset());
        assertEquals(1, actual.get(0).lastOffset());
        assertEquals(1, actual.get(0).producerId());
        assertEquals(6, actual.get(1).baseOffset());
        assertEquals(7, actual.get(1).lastOffset());
        assertEquals(2, actual.get(1).producerId());
        assertEquals(9, actual.get(2).baseOffset());
        assertEquals(10, actual.get(2).lastOffset());
        assertEquals(1, actual.get(2).producerId());
        assertEquals(15, actual.get(3).baseOffset());
        assertEquals(16, actual.get(3).lastOffset());
        assertEquals(1, actual.get(3).producerId());
    }

    @Test
    public void testTerminalRecordsUpdatedWhenAbortedTransactionBatchesAreArchived() {

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Create 3 batches: first batch (0-8), middle batch with ABORTED transactions (10-18), last batch (20-28), each having
        // a transaction marker at the end.
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        
        // First batch: normal records (0-8)
        newTransactionalRecords(buffer, ControlRecordType.COMMIT, 9, 1, 0);
        
        // Middle batch: ABORTED transaction records (10-18)
        newTransactionalRecords(buffer, ControlRecordType.ABORT, 9, 2, 10);
        
        // Last batch: normal records (20-28)
        newTransactionalRecords(buffer, ControlRecordType.COMMIT, 9, 3, 20);
        
        buffer.flip();
        Records records = MemoryRecords.readableRecords(buffer);

        // Create aborted transactions list for the middle batch
        List<FetchResponseData.AbortedTransaction> abortedTransactions = List.of(
            new FetchResponseData.AbortedTransaction().setFirstOffset(10).setProducerId(2)
        );

        FetchPartitionData fetchPartitionData = fetchPartitionData(records, abortedTransactions);

        // The batchSize is set to 1 to make sure that the batch with ABORTED transactions don't contain the transaction end
        // marker. During the acquire methodology, initially all 30 records will be acquired. But when the aborted transactions
        // are filtered, records 10 -> 18 will be filtered out of acquired records, leaving the acquired records count to be 21.
        ShareAcquiredRecords shareAcquiredRecords = sharePartition.acquire(
            MEMBER_ID, ShareAcquireMode.BATCH_OPTIMIZED, 1, MAX_FETCH_RECORDS, 0, fetchPartitionData, FetchIsolation.TXN_COMMITTED
        );

        // Verify that 21 records were acquired.
        assertEquals(21, shareAcquiredRecords.count());
        assertEquals(6, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(9L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(19L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(29L).batchState());
        // Records 10 -> 18 are ARCHIVED, hence deliveryCompleteCount should be 9.
        assertEquals(9, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testTerminalRecordsUpdatedWhenAbortedTransactionOffsetsAreArchived() {

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Create 3 batches: first batch (0-1), middle batch with ABORTED transactions (3-4), last batch (6-7), each having
        // a transaction marker at the end.
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        // First batch: normal records (0-1)
        newTransactionalRecords(buffer, ControlRecordType.COMMIT, 2, 1, 0);

        // Middle batch: ABORTED transaction records (3-4)
        newTransactionalRecords(buffer, ControlRecordType.ABORT, 2, 2, 3);

        // Last batch: normal records (6-7)
        newTransactionalRecords(buffer, ControlRecordType.COMMIT, 2, 3, 6);

        buffer.flip();
        Records records = MemoryRecords.readableRecords(buffer);

        // Create aborted transactions list for the middle batch
        List<FetchResponseData.AbortedTransaction> abortedTransactions = List.of(
            new FetchResponseData.AbortedTransaction().setFirstOffset(3).setProducerId(2)
        );

        FetchPartitionData fetchPartitionData = fetchPartitionData(records, abortedTransactions);

        // All the 9 records will be acquired as a single cached state batch. During the acquire code flow, initially all
        // 9 records will be acquired. But when the aborted transactions are filtered, records 3 -> 4 will be filtered
        // out of acquired records, leaving the acquired records count to be 7.
        ShareAcquiredRecords shareAcquiredRecords = sharePartition.acquire(
            MEMBER_ID, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 0, fetchPartitionData, FetchIsolation.TXN_COMMITTED
        );

        // Verify that 7 records were acquired.
        assertEquals(7, shareAcquiredRecords.count());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(0L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(2L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).offsetState().get(3L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).offsetState().get(4L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(5L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(6L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(7L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(8L).state());
        // Records 3 -> 4 are ARCHIVED, hence deliveryCompleteCount should be 2.
        assertEquals(2, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testFetchLockReleasedByDifferentId() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();
        Uuid fetchId1 = Uuid.randomUuid();
        Uuid fetchId2 = Uuid.randomUuid();

        // Initially, fetch lock is not acquired.
        assertNull(sharePartition.fetchLock());
        // fetchId1 acquires the fetch lock.
        assertTrue(sharePartition.maybeAcquireFetchLock(fetchId1));
        // If we release fetch lock by fetchId2, it will work. Currently, we have kept the release of fetch lock as non-strict
        // such that even if the caller's id for releasing fetch lock does not match the id that holds the lock, we will
        // still release it. This has been done to avoid the scenarios where we hold the fetch lock for a share partition
        // forever due to faulty code. In the future, we plan to make the locks handling strict, then this test case needs to be updated.
        sharePartition.releaseFetchLock(fetchId2);
        assertNull(sharePartition.fetchLock()); // Fetch lock has been released.
    }

    @Test
    public void testAcquireWhenBatchHasOngoingTransition() {
        Persister persister = Mockito.mock(Persister.class);

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withPersister(persister)
            .build();
        // Acquire a single batch with member-1.
        fetchAcquiredRecords(
            sharePartition.acquire(MEMBER_ID, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 21,
                fetchPartitionData(memoryRecords(21, 10)), FETCH_ISOLATION_HWM
            ), 10
        );

        // Validate that there is no ongoing transition.
        assertFalse(sharePartition.cachedState().get(21L).batchHasOngoingStateTransition());
        // Return a future which will be completed later, so the batch state has ongoing transition.
        CompletableFuture<WriteShareGroupStateResult> future = new CompletableFuture<>();
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future);
        // Acknowledge batch to create ongoing transition.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(21, 30, List.of(AcknowledgeType.RELEASE.id))));

        // Since the future is not yet completed, deliveryCompleteCount will not be updated yet.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Assert the start offset has not moved and batch has ongoing transition.
        assertEquals(21L, sharePartition.startOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().get(21L).batchHasOngoingStateTransition());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(21L).batchMemberId());

        // Acquire the same batch with member-2. This function call will return with 0 records since there is an ongoing
        // transition for this batch.
        fetchAcquiredRecords(
            sharePartition.acquire("member-2", ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 21,
                fetchPartitionData(memoryRecords(21, 10)), FETCH_ISOLATION_HWM
            ), 0
        );

        // Since no new records are acquired, deliveryCompleteCount will remain the same.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(21L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(21L).batchMemberId());

        // Complete the future so acknowledge API can be completed, which updates the cache. Now the records can be acquired.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        future.complete(writeShareGroupStateResult);

        // Since the records successfully acknowledged are moved to AVAILABLE state, deliveryCompleteCount will still not change.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Acquire the same batch with member-2. 10 records will be acquired.
        fetchAcquiredRecords(
            sharePartition.acquire("member-2", ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 21,
                fetchPartitionData(memoryRecords(21, 10)), FETCH_ISOLATION_HWM
            ), 10
        );
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(21L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(21L).batchMemberId());
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testNextFetchOffsetWhenBatchHasOngoingTransition() {
        Persister persister = Mockito.mock(Persister.class);

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withPersister(persister)
            .build();

        // Acquire a single batch 0-9 with member-1.
        fetchAcquiredRecords(
            sharePartition.acquire(MEMBER_ID, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 0,
                fetchPartitionData(memoryRecords(10)), FETCH_ISOLATION_HWM
            ), 10
        );

        // Acquire a single batch 10-19 with member-1.
        fetchAcquiredRecords(
            sharePartition.acquire(MEMBER_ID, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 10,
                fetchPartitionData(memoryRecords(10, 10)), FETCH_ISOLATION_HWM
            ), 10
        );

        // Validate that there is no ongoing transition.
        assertEquals(2, sharePartition.cachedState().size());
        assertFalse(sharePartition.cachedState().get(0L).batchHasOngoingStateTransition());
        assertFalse(sharePartition.cachedState().get(10L).batchHasOngoingStateTransition());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());

        // Return futures which will be completed later, so the batch state has ongoing transition.
        CompletableFuture<WriteShareGroupStateResult> future1 = new CompletableFuture<>();
        CompletableFuture<WriteShareGroupStateResult> future2 = new CompletableFuture<>();

        // Mocking the persister write state RPC to return future 1 and future 2 when acknowledgement occurs for
        // offsets 0-9 and 10-19 respectively.
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future1).thenReturn(future2);

        // Acknowledge batch to create ongoing transition.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(0, 9, List.of(AcknowledgeType.RELEASE.id))));
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(10, 19, List.of(AcknowledgeType.RELEASE.id))));

        // deliveryCompleteCount will not be updated, because the acknowledgment type is RELEASE.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Complete future2 so second acknowledge API can be completed, which updates the cache.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        future2.complete(writeShareGroupStateResult);

        // Since the records successfully acknowledged are moved to AVAILABLE state, deliveryCompleteCount will still not change.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Offsets 0-9 will have ongoing state transition since future1 is not complete yet.
        // Offsets 10-19 won't have ongoing state transition since future2 has been completed.
        assertTrue(sharePartition.cachedState().get(0L).batchHasOngoingStateTransition());
        assertFalse(sharePartition.cachedState().get(10L).batchHasOngoingStateTransition());

        // nextFetchOffset should return 10 and not 0 because batch 0-9 is undergoing state transition.
        assertEquals(10, sharePartition.nextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWhenOffsetsHaveOngoingTransition() {
        Persister persister = Mockito.mock(Persister.class);

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withPersister(persister)
            .build();

        // Acquire a single batch 0-50 with member-1.
        fetchAcquiredRecords(
            sharePartition.acquire(MEMBER_ID, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 0,
                fetchPartitionData(memoryRecords(50)), FETCH_ISOLATION_HWM
            ), 50
        );

        // Validate that there is no ongoing transition.
        assertFalse(sharePartition.cachedState().get(0L).batchHasOngoingStateTransition());

        // Return futures which will be completed later, so the batch state has ongoing transition.
        CompletableFuture<WriteShareGroupStateResult> future1 = new CompletableFuture<>();
        CompletableFuture<WriteShareGroupStateResult> future2 = new CompletableFuture<>();

        // Mocking the persister write state RPC to return future 1 and future 2 when acknowledgement occurs for
        // offsets 5-9 and 20-24 respectively.
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future1).thenReturn(future2);

        // Acknowledge batch to create ongoing transition.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.RELEASE.id))));
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(20, 24, List.of(AcknowledgeType.RELEASE.id))));

        // deliveryCompleteCount will not be updated, because the acknowledgment type is RELEASE.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Complete future2 so second acknowledge API can be completed, which updates the cache.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        future2.complete(writeShareGroupStateResult);

        // Since the records successfully acknowledged are moved to AVAILABLE state, deliveryCompleteCount will still not change.
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Offsets 5-9 will have ongoing state transition since future1 is not complete yet.
        // Offsets 20-24 won't have ongoing state transition since future2 has been completed.
        assertTrue(sharePartition.cachedState().get(0L).offsetState().get(5L).hasOngoingStateTransition());
        assertTrue(sharePartition.cachedState().get(0L).offsetState().get(6L).hasOngoingStateTransition());
        assertTrue(sharePartition.cachedState().get(0L).offsetState().get(7L).hasOngoingStateTransition());
        assertTrue(sharePartition.cachedState().get(0L).offsetState().get(8L).hasOngoingStateTransition());
        assertTrue(sharePartition.cachedState().get(0L).offsetState().get(9L).hasOngoingStateTransition());
        assertFalse(sharePartition.cachedState().get(0L).offsetState().get(20L).hasOngoingStateTransition());
        assertFalse(sharePartition.cachedState().get(0L).offsetState().get(21L).hasOngoingStateTransition());
        assertFalse(sharePartition.cachedState().get(0L).offsetState().get(22L).hasOngoingStateTransition());
        assertFalse(sharePartition.cachedState().get(0L).offsetState().get(23L).hasOngoingStateTransition());
        assertFalse(sharePartition.cachedState().get(0L).offsetState().get(24L).hasOngoingStateTransition());

        // nextFetchOffset should return 20 and not 5 because offsets 5-9 is undergoing state transition.
        assertEquals(20, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcquisitionLockTimeoutWithConcurrentAcknowledgement() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withPersister(persister)
            .build();

        // Create 2 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 0, 5).close();
        memoryRecordsBuilder(buffer, 5, 15).close();

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Acquire 10 records.
        fetchAcquiredRecords(sharePartition.acquire(
              MEMBER_ID,
              ShareAcquireMode.BATCH_OPTIMIZED,
              5, /* Batch size of 5 so cache can have 2 entries */
              10,
              DEFAULT_FETCH_OFFSET,
              fetchPartitionData(records, 0),
              FETCH_ISOLATION_HWM),
            20);

        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(2, sharePartition.timer().size());

        // Return 2 future which will be completed later.
        CompletableFuture<WriteShareGroupStateResult> future1 = new CompletableFuture<>();
        CompletableFuture<WriteShareGroupStateResult> future2 = new CompletableFuture<>();
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future1).thenReturn(future2);

        // Store the corresponding batch timer tasks.
        TimerTask timerTask1 = sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask();
        TimerTask timerTask2 = sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask();

        // Acknowledge 1 offset in first batch as Accept to create offset tracking, accept complete
        // sencond batch. And mark offset 0 as release so cached state do not move ahead.
        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(0, 0, List.of(AcknowledgeType.RELEASE.id)),
            new ShareAcknowledgementBatch(1, 1, List.of(AcknowledgeType.ACCEPT.id)),
            new ShareAcknowledgementBatch(5, 19, List.of(AcknowledgeType.ACCEPT.id))));

        // Assert the start offset has not moved.
        assertEquals(0L, sharePartition.startOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(0L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(2L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        // Verify ongoing transition states.
        assertTrue(sharePartition.cachedState().get(0L).offsetState().get(0L).hasOngoingStateTransition());
        assertTrue(sharePartition.cachedState().get(0L).offsetState().get(1L).hasOngoingStateTransition());
        assertFalse(sharePartition.cachedState().get(0L).offsetState().get(2L).hasOngoingStateTransition());
        assertTrue(sharePartition.cachedState().get(5L).batchHasOngoingStateTransition());

        // Records 1 and 5 -> 19 are acked with ACKNOWLEDGE type, thus deliveryCompleteCount will account for these.
        assertEquals(16, sharePartition.deliveryCompleteCount());

        // Validate first timer task is already cancelled.
        assertTrue(timerTask1.isCancelled());
        assertFalse(timerTask2.isCancelled());

        // Fetch offset state timer tasks.
        TimerTask timerTaskOffsetState1 = sharePartition.cachedState().get(0L).offsetState().get(0L).acquisitionLockTimeoutTask();
        TimerTask timerTaskOffsetState2 = sharePartition.cachedState().get(0L).offsetState().get(1L).acquisitionLockTimeoutTask();
        TimerTask timerTaskOffsetState3 = sharePartition.cachedState().get(0L).offsetState().get(2L).acquisitionLockTimeoutTask();

        // Complete futures.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        future1.complete(writeShareGroupStateResult);
        future2.complete(writeShareGroupStateResult);

        // Now that the futures are completed, offsets 1 and 5 -> 19 are all committed to the final ACKNOWLEDGED state.
        // The deliveryCompleteCount will remain same as before the future is completed.
        assertEquals(16, sharePartition.deliveryCompleteCount());

        // Verify timer tasks are now cancelled, except unacknowledged offsets.
        assertEquals(2, sharePartition.cachedState().size());
        assertTrue(timerTask2.isCancelled());
        assertTrue(timerTaskOffsetState1.isCancelled());
        assertTrue(timerTaskOffsetState2.isCancelled());
        assertFalse(timerTaskOffsetState3.isCancelled());

        // Verify the state prior executing the timer tasks.
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(2L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());

        // Running expired timer tasks should not mark offsets available, except for offset 2.
        timerTask1.run();
        // State should remain same.
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(2L).state());

        timerTask2.run();
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());

        timerTaskOffsetState2.run();
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());

        // Should update the state to available as the timer task is not yet expired.
        timerTaskOffsetState3.run();
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(2L).state());

        assertEquals(16, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testLsoMovementWithWriteStateRPCFailuresInAcknowledgement() {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withPersister(persister)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        // Validate that there is no ongoing transition.
        assertFalse(sharePartition.cachedState().get(2L).batchHasOngoingStateTransition());
        assertFalse(sharePartition.cachedState().get(7L).batchHasOngoingStateTransition());

        // Return futures which will be completed later, so the batch state has ongoing transition.
        CompletableFuture<WriteShareGroupStateResult> future1 = new CompletableFuture<>();
        CompletableFuture<WriteShareGroupStateResult> future2 = new CompletableFuture<>();

        // Mocking the persister write state RPC to return future 1 and future 2 when acknowledgement occurs for
        // offsets 2-6 and 7-11 respectively.
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future1).thenReturn(future2);

        // Acknowledge batch to create ongoing transition.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(2, 6, List.of(AcknowledgeType.RELEASE.id))));
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(7, 11, List.of(AcknowledgeType.ACCEPT.id))));

        // Validate that there is no ongoing transition.
        assertTrue(sharePartition.cachedState().get(2L).batchHasOngoingStateTransition());
        assertTrue(sharePartition.cachedState().get(7L).batchHasOngoingStateTransition());
        // Records 7 -> 11 are acked with ACCEPT type, thus deliveryCompleteCount will account for these.
        assertEquals(5, sharePartition.deliveryCompleteCount());

        // Move LSO to 7, so some records/offsets can be marked archived for the first batch.
        sharePartition.updateCacheAndOffsets(7L);

        // Start offset will be moved.
        assertEquals(12L, sharePartition.nextFetchOffset());
        assertEquals(7L, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().get(2L).batchHasOngoingStateTransition());
        assertTrue(sharePartition.cachedState().get(7L).batchHasOngoingStateTransition());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(2L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(7L).batchState());

        assertEquals(5, sharePartition.deliveryCompleteCount());

        // Complete future1 exceptionally so acknowledgement for 2-6 offsets will be completed.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        future1.complete(writeShareGroupStateResult);

        // The completion of future1 with exception should not impact the cached state since those records have already
        // been archived.
        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(7, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertFalse(sharePartition.cachedState().get(2L).batchHasOngoingStateTransition());
        assertTrue(sharePartition.cachedState().get(7L).batchHasOngoingStateTransition());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(2L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(7L).batchState());
        assertEquals(5, sharePartition.deliveryCompleteCount());

        future2.complete(writeShareGroupStateResult);
        assertEquals(12L, sharePartition.nextFetchOffset());
        assertEquals(7, sharePartition.startOffset());
        assertEquals(11, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(2L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());
        // After the write RPC failure, the record states are rolled back and deliveryCompleteCount is calculated
        // from scratch. Since there is no Terminal record now in flight, deliveryCompleteCount becomes 0.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAcquisitionLockTimeoutWithWriteStateRPCFailure() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withPersister(persister)
            .build();

        fetchAcquiredRecords(
            sharePartition.acquire(MEMBER_ID, ShareAcquireMode.BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, 0,
                fetchPartitionData(memoryRecords(2)), FETCH_ISOLATION_HWM
            ), 2
        );

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());

        // Return a future which will be completed later, so the batch state has ongoing transition.
        CompletableFuture<WriteShareGroupStateResult> future = new CompletableFuture<>();
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future);

        // Acknowledge batch to create ongoing transition.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(0, 1, List.of(AcknowledgeType.ACCEPT.id))));
        // Assert the start offset has not moved and batch has ongoing transition.
        assertEquals(0L, sharePartition.startOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().get(0L).batchHasOngoingStateTransition());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        // Timer task has not been expired yet.
        assertFalse(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask().hasExpired());
        // Record are acked with ACKNOWLEDGED type, thus deliveryCompleteCount will account for these.
        assertEquals(2, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire. This will not cause any change because the record is not in ACQUIRED state.
        // This will remove the entry of the timer task from timer.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> sharePartition.cachedState().get(0L).batchState() == RecordState.ACKNOWLEDGED &&
                sharePartition.cachedState().get(0L).batchDeliveryCount() == 1 &&
                sharePartition.timer().size() == 0,
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(0L, List.of())));

        // Acquisition lock timeout task has run already and is not null.
        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        // Timer task should be expired now.
        assertTrue(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask().hasExpired());

        assertEquals(2, sharePartition.deliveryCompleteCount());

        // Complete future exceptionally so acknowledgement for 0-1 offsets will be completed.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        future.complete(writeShareGroupStateResult);

        // Even though write state RPC has failed and corresponding acquisition lock timeout task has expired,
        // the record should not stuck in ACQUIRED state with no acquisition lock timeout task.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        assertNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        // After the write RPC failure, the record states are rolled back and deliveryCompleteCount is calculated
        // from scratch. Since there is no Terminal record now in flight, deliveryCompleteCount becomes 0.
        assertEquals(0, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testRecordArchivedWithWriteStateRPCFailure() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2)
            .withPersister(persister)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        // Futures which will be completed later, so the batch state has ongoing transition.
        CompletableFuture<WriteShareGroupStateResult> future1 = new CompletableFuture<>();
        CompletableFuture<WriteShareGroupStateResult> future2 = new CompletableFuture<>();
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future1).thenReturn(future2);

        // Acknowledge batches.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(3, 3, List.of(AcknowledgeType.ACCEPT.id))));
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(7, 11, List.of(AcknowledgeType.ACCEPT.id))));

        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(2L).offsetState().get(3L).state());
        assertEquals(1, sharePartition.cachedState().get(2L).offsetState().get(3L).deliveryCount());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(7L).batchState());
        assertEquals(1, sharePartition.cachedState().get(7L).batchDeliveryCount());

        // Records 3 and 7 -> 11 are acked with ACKNOWLEDGE type, thus deliveryCompleteCount will account for these.
        assertEquals(6, sharePartition.deliveryCompleteCount());

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));

        future1.complete(writeShareGroupStateResult);
        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).offsetState().get(3L).state());
        assertEquals(1, sharePartition.cachedState().get(2L).offsetState().get(3L).deliveryCount());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(7L).batchState());
        assertEquals(1, sharePartition.cachedState().get(7L).batchDeliveryCount());
        assertEquals(5, sharePartition.deliveryCompleteCount());

        future2.complete(writeShareGroupStateResult);
        assertEquals(12L, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).offsetState().get(3L).state());
        assertEquals(1, sharePartition.cachedState().get(2L).offsetState().get(3L).deliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());
        assertEquals(1, sharePartition.cachedState().get(7L).batchDeliveryCount());

        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Allowing acquisition lock to expire. This will also ensure that acquisition lock timeout task
        // is run successfully post write state RPC failure.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> sharePartition.cachedState().get(2L).offsetState().get(3L).state() == RecordState.AVAILABLE  &&
                sharePartition.cachedState().get(7L).batchState() == RecordState.AVAILABLE &&
                sharePartition.cachedState().get(2L).offsetState().get(3L).deliveryCount() == 1 &&
                sharePartition.cachedState().get(7L).batchDeliveryCount() == 1 &&
                sharePartition.timer().size() == 0,
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(2L, List.of(3L), 7L, List.of())));
        // Acquisition lock timeout task has run already and next fetch offset is moved to 2.
        assertEquals(2, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.deliveryCompleteCount());

        // Send the same batches again.
        fetchAcquiredRecords(sharePartition, memoryRecords(2, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(7, 5), 5);

        future1 = new CompletableFuture<>();
        future2 = new CompletableFuture<>();
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future1).thenReturn(future2);

        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(3, 3, List.of(AcknowledgeType.ACCEPT.id))));
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(7, 11, List.of(AcknowledgeType.ACCEPT.id))));

        assertEquals(6, sharePartition.deliveryCompleteCount());

        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        // Verify the timer tasks have run and the state is archived for the offsets which are not acknowledged,
        // but the acquisition lock timeout task should be just expired for acknowledged offsets, though
        // the state should not be archived.
        TestUtils.waitForCondition(
            () -> sharePartition.cachedState().get(2L).offsetState().get(2L).state() == RecordState.ARCHIVED  &&
                sharePartition.cachedState().get(2L).offsetState().get(3L).state() == RecordState.ACKNOWLEDGED  &&
                sharePartition.cachedState().get(2L).offsetState().get(3L).acquisitionLockTimeoutTask().hasExpired() &&
                sharePartition.cachedState().get(7L).batchState() == RecordState.ACKNOWLEDGED &&
                sharePartition.cachedState().get(7L).batchAcquisitionLockTimeoutTask().hasExpired(),
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(2L, List.of(3L), 7L, List.of())));

        // After the acquisition lock timeout task has expired, records 2, 4 -> 6 are archived, and thus deliveryCompleteCount
        // increases by 4.
        assertEquals(10, sharePartition.deliveryCompleteCount());

        future1.complete(writeShareGroupStateResult);
        // Now the state should be archived for the offsets despite the write state RPC failure, as the
        // delivery count has reached the max delivery count and the acquisition lock timeout task
        // has already expired for the offsets which were acknowledged.
        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(2L).offsetState().get(3L).state());
        assertEquals(2, sharePartition.cachedState().get(2L).offsetState().get(3L).deliveryCount());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(7L).batchState());
        assertEquals(2, sharePartition.cachedState().get(7L).batchDeliveryCount());

        future2.complete(writeShareGroupStateResult);
        assertEquals(12L, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(7L).batchState());
        assertEquals(2, sharePartition.cachedState().get(7L).batchDeliveryCount());
        // At this point, the batch 2 -> 6 is removed from the cached state and startOffset is moved to 7. Thus, in flight
        // contains records 7 -> 11 which are archived. Therefore, deliveryCompleteCount becomes 5.
        assertEquals(5, sharePartition.deliveryCompleteCount());
    }

    @Test
    public void testAckTypeToRecordStateMapping() {
        // This test will help catch bugs if the map changes.
        Map<Byte, RecordState> actualMap = SharePartition.ackTypeToRecordStateMapping();
        assertEquals(4, actualMap.size());

        Map<Byte, RecordState> expected = Map.of(
            (byte) 0, RecordState.ARCHIVED,
            AcknowledgeType.ACCEPT.id, RecordState.ACKNOWLEDGED,
            AcknowledgeType.RELEASE.id, RecordState.AVAILABLE,
            AcknowledgeType.REJECT.id, RecordState.ARCHIVED
        );

        for (byte key : expected.keySet()) {
            assertEquals(expected.get(key), actualMap.get(key));
        }
    }

    @Test
    public void testFetchAckTypeMapForBatch() {
        ShareAcknowledgementBatch batch = mock(ShareAcknowledgementBatch.class);
        when(batch.acknowledgeTypes()).thenReturn(List.of((byte) -1));
        assertThrows(IllegalArgumentException.class, () -> SharePartition.fetchAckTypeMapForBatch(batch));
    }

    @Test
    public void testRenewAcknowledgeWithCompleteBatchAck() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2)
            .withPersister(persister)
            .build();

        List<AcquiredRecords> records = fetchAcquiredRecords(sharePartition, memoryRecords(0, 1), 1);
        assertEquals(1, records.size());
        assertEquals(records.get(0).firstOffset(), records.get(0).lastOffset());
        assertEquals(1, sharePartition.cachedState().size());
        InFlightBatch batch = sharePartition.cachedState().get(0L);
        AcquisitionLockTimerTask taskOrig = batch.batchAcquisitionLockTimeoutTask();

        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(0, 0, List.of(AcknowledgeType.RENEW.id))));
        assertTrue(taskOrig.isCancelled()); // Original acq lock cancelled.
        assertNotEquals(taskOrig, batch.batchAcquisitionLockTimeoutTask()); // Lock changes.
        assertEquals(1, sharePartition.timer().size()); // Timer jobs
        assertEquals(RecordState.ACQUIRED, batch.batchState());
        Mockito.verify(persister, Mockito.times(0)).writeState(Mockito.any());  // No persister call.

        // Expire timer
        // On expiration state will transition to AVAILABLE resulting in persister write RPC
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        mockTimer.advanceClock(ACQUISITION_LOCK_TIMEOUT_MS + 1);    // Trigger expire

        assertNull(batch.batchAcquisitionLockTimeoutTask());
        assertEquals(RecordState.AVAILABLE, batch.batchState());    // Verify batch record state
        assertEquals(0, sharePartition.timer().size()); // Timer jobs
        Mockito.verify(persister, Mockito.times(1)).writeState(Mockito.any());  // 1 persister call.
    }

    @Test
    public void testRenewAcknowledgeOnExpiredBatch() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2)
            .withPersister(persister)
            .build();

        List<AcquiredRecords> records = fetchAcquiredRecords(sharePartition, memoryRecords(0, 1), 1);
        assertEquals(1, records.size());
        assertEquals(records.get(0).firstOffset(), records.get(0).lastOffset());
        assertEquals(1, sharePartition.cachedState().size());
        InFlightBatch batch = sharePartition.cachedState().get(0L);
        AcquisitionLockTimerTask taskOrig = batch.batchAcquisitionLockTimeoutTask();

        // Expire acq lock timeout.
        // Persister mocking for recordState transition.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));

        when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        mockTimer.advanceClock(ACQUISITION_LOCK_TIMEOUT_MS + 1);
        TestUtils.waitForCondition(() -> batch.batchAcquisitionLockTimeoutTask() == null, "Acq lock timeout not cancelled.");
        CompletableFuture<Void> future = sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(0, 0, List.of(AcknowledgeType.RENEW.id))));

        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail("No exception thrown");
        } catch (Exception e) {
            assertNotNull(e);
            assertInstanceOf(InvalidRecordStateException.class, e.getCause());
        }
        assertTrue(taskOrig.isCancelled()); // Original acq lock cancelled.
        assertNotEquals(taskOrig, batch.batchAcquisitionLockTimeoutTask()); // Lock changes.
        assertEquals(0, sharePartition.timer().size()); // Timer jobs
        assertEquals(RecordState.AVAILABLE, batch.batchState());
        Mockito.verify(persister, Mockito.times(1)).writeState(Mockito.any());  // 1 persister call to update record state.
    }

    @Test
    public void testRenewAcknowledgeWithPerOffsetAck() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2)
            .withPersister(persister)
            .build();

        List<AcquiredRecords> records = fetchAcquiredRecords(sharePartition, memoryRecords(0, 2), 2);
        assertEquals(1, records.size());
        assertEquals(records.get(0).firstOffset() + 1, records.get(0).lastOffset());
        assertEquals(1, sharePartition.cachedState().size());
        InFlightBatch batch = sharePartition.cachedState().get(0L);
        assertEquals(RecordState.ACQUIRED, batch.batchState());
        AcquisitionLockTimerTask taskOrig = batch.batchAcquisitionLockTimeoutTask();

        // For ACCEPT ack call.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));

        when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(0, 1,
            List.of(AcknowledgeType.RENEW.id, AcknowledgeType.ACCEPT.id))));

        assertTrue(taskOrig.isCancelled()); // Original acq lock cancelled.
        assertNotEquals(taskOrig, sharePartition.cachedState().get(0L).offsetState().get(0L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());

        InFlightState offset0 = sharePartition.cachedState().get(0L).offsetState().get(0L);
        InFlightState offset1 = sharePartition.cachedState().get(0L).offsetState().get(1L);
        assertEquals(RecordState.ACQUIRED, offset0.state());
        assertNotNull(offset0.acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size()); // Timer jobs

        assertEquals(RecordState.ACKNOWLEDGED, offset1.state());
        assertNull(offset1.acquisitionLockTimeoutTask());

        Mockito.verify(persister, Mockito.times(1)).writeState(Mockito.any());

        // Expire timer
        mockTimer.advanceClock(ACQUISITION_LOCK_TIMEOUT_MS + 1);    // Trigger expire

        assertNull(offset0.acquisitionLockTimeoutTask());
        assertEquals(RecordState.AVAILABLE, offset0.state());    // Verify batch record state
        assertEquals(0, sharePartition.timer().size()); // Timer jobs
        Mockito.verify(persister, Mockito.times(2)).writeState(Mockito.any());  // 1 more persister call.
    }

    @Test
    public void testLsoMovementWithBatchRenewal() {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2)
            .withPersister(persister)
            .build();

        List<AcquiredRecords> records = fetchAcquiredRecords(sharePartition, memoryRecords(0, 10), 10);
        assertEquals(1, records.size());
        assertNotEquals(records.get(0).firstOffset(), records.get(0).lastOffset());
        assertEquals(1, sharePartition.cachedState().size());
        InFlightBatch batch = sharePartition.cachedState().get(0L);
        AcquisitionLockTimerTask taskOrig = batch.batchAcquisitionLockTimeoutTask();

        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(0, 9, List.of(AcknowledgeType.RENEW.id))));
        sharePartition.updateCacheAndOffsets(5);

        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(5, sharePartition.startOffset());
        assertEquals(9, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());

        assertTrue(taskOrig.isCancelled()); // Original acq lock cancelled.
        assertNotEquals(taskOrig, batch.batchAcquisitionLockTimeoutTask()); // Lock changes.
        assertEquals(1, sharePartition.timer().size()); // Timer jobs
        Mockito.verify(persister, Mockito.times(0)).writeState(Mockito.any());  // No persister call.
    }

    @Test
    public void testLsoMovementWithPerOffsetRenewal() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2)
            .withPersister(persister)
            .build();

        List<AcquiredRecords> records = fetchAcquiredRecords(sharePartition, memoryRecords(0, 5), 5);
        assertEquals(1, records.size());
        assertEquals(records.get(0).firstOffset() + 4, records.get(0).lastOffset());
        assertEquals(1, sharePartition.cachedState().size());
        InFlightBatch batch = sharePartition.cachedState().get(0L);
        assertEquals(RecordState.ACQUIRED, batch.batchState());
        AcquisitionLockTimerTask taskOrig = batch.batchAcquisitionLockTimeoutTask();

        // For ACCEPT ack call.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));

        when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(0, 4,
            List.of(AcknowledgeType.RENEW.id, AcknowledgeType.ACCEPT.id, AcknowledgeType.RENEW.id, AcknowledgeType.ACCEPT.id, AcknowledgeType.RENEW.id))));

        sharePartition.updateCacheAndOffsets(3);

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.startOffset());
        assertEquals(4, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());

        assertTrue(taskOrig.isCancelled()); // Original acq lock cancelled.
        assertNotEquals(taskOrig, sharePartition.cachedState().get(0L).offsetState().get(0L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());

        InFlightState offset0 = sharePartition.cachedState().get(0L).offsetState().get(0L);
        InFlightState offset1 = sharePartition.cachedState().get(0L).offsetState().get(1L);
        InFlightState offset2 = sharePartition.cachedState().get(0L).offsetState().get(2L);
        InFlightState offset3 = sharePartition.cachedState().get(0L).offsetState().get(3L);
        InFlightState offset4 = sharePartition.cachedState().get(0L).offsetState().get(4L);

        assertEquals(RecordState.ACQUIRED, offset0.state());
        assertNotNull(offset0.acquisitionLockTimeoutTask());

        assertEquals(RecordState.ACKNOWLEDGED, offset1.state());
        assertNull(offset1.acquisitionLockTimeoutTask());

        assertEquals(RecordState.ACQUIRED, offset2.state());
        assertNotNull(offset2.acquisitionLockTimeoutTask());

        assertEquals(RecordState.ACKNOWLEDGED, offset3.state());
        assertNull(offset3.acquisitionLockTimeoutTask());

        assertEquals(RecordState.ACQUIRED, offset4.state());
        assertNotNull(offset4.acquisitionLockTimeoutTask());

        assertEquals(3, sharePartition.timer().size()); // Timer jobs - 3 because the renewed offsets are non-contiguous.

        // Expire timer
        mockTimer.advanceClock(ACQUISITION_LOCK_TIMEOUT_MS + 1);    // Trigger expire
        List<RecordState> expectedStates = List.of(RecordState.ARCHIVED, RecordState.ACKNOWLEDGED, RecordState.ARCHIVED, RecordState.ACKNOWLEDGED, RecordState.AVAILABLE);
        for (long i = 0; i <= 4; i++) {
            InFlightState offset = sharePartition.cachedState().get(0L).offsetState().get(i);
            assertNull(offset.acquisitionLockTimeoutTask());
            assertEquals(expectedStates.get((int) i), offset.state());
        }

        assertEquals(0, sharePartition.timer().size()); // Timer jobs

        Mockito.verify(persister, Mockito.times(4)).writeState(Mockito.any());
    }

    @Test
    public void testRenewAcknowledgeWithPerOffsetAndBatchMix() {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2)
            .withPersister(persister)
            .build();

        // Batch
        List<AcquiredRecords> recordsB = fetchAcquiredRecords(sharePartition, memoryRecords(0, 1), 1);
        assertEquals(1, recordsB.size());
        assertEquals(recordsB.get(0).firstOffset(), recordsB.get(0).lastOffset());
        assertEquals(1, sharePartition.cachedState().size());
        InFlightBatch batchB = sharePartition.cachedState().get(0L);
        AcquisitionLockTimerTask taskOrigB = batchB.batchAcquisitionLockTimeoutTask();

        // Per offset
        List<AcquiredRecords> recordsO = fetchAcquiredRecords(sharePartition, memoryRecords(1, 2), 2);
        assertEquals(1, recordsO.size());
        assertEquals(recordsO.get(0).firstOffset() + 1, recordsO.get(0).lastOffset());
        assertEquals(2, sharePartition.cachedState().size());
        InFlightBatch batchO = sharePartition.cachedState().get(0L);
        assertEquals(RecordState.ACQUIRED, batchO.batchState());
        AcquisitionLockTimerTask taskOrigO = batchO.batchAcquisitionLockTimeoutTask();

        // For ACCEPT ack call.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));

        when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(0, 0, List.of(AcknowledgeType.RENEW.id)),
            new ShareAcknowledgementBatch(1, 2, List.of(AcknowledgeType.RENEW.id, AcknowledgeType.ACCEPT.id))
        ));

        // Batch checks
        assertTrue(taskOrigB.isCancelled()); // Original acq lock cancelled.
        assertNotEquals(taskOrigB, batchB.batchAcquisitionLockTimeoutTask()); // Lock changes.

        // Per offset checks
        assertTrue(taskOrigO.isCancelled()); // Original acq lock cancelled.
        assertNotEquals(taskOrigO, sharePartition.cachedState().get(1L).offsetState().get(1L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(1L).offsetState());

        InFlightState offset1 = sharePartition.cachedState().get(1L).offsetState().get(1L);
        InFlightState offset2 = sharePartition.cachedState().get(1L).offsetState().get(2L);
        assertEquals(RecordState.ACQUIRED, offset1.state());
        assertNotNull(offset1.acquisitionLockTimeoutTask());

        assertEquals(RecordState.ACKNOWLEDGED, offset2.state());
        assertNull(offset2.acquisitionLockTimeoutTask());

        assertEquals(2, sharePartition.timer().size()); // Timer jobs one for batch and one for single renewal in per offset.
        Mockito.verify(persister, Mockito.times(1)).writeState(Mockito.any());
    }

    @Test
    public void testAcquireSingleBatchInRecordLimitMode() {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = Mockito.spy(SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withPersister(persister)
            .build());

        // Member-1 attempts to acquire records in strict mode with a maximum fetch limit of 5 records.
        MemoryRecords records = memoryRecords(10);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            2,
            5,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            5);

        assertArrayEquals(expectedAcquiredRecord(0, 4, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(9, sharePartition.cachedState().get(0L).lastOffset());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchMemberId());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchDeliveryCount());

        assertEquals(10, sharePartition.cachedState().get(0L).offsetState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(0L).state());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(0L).offsetState().get(0L).memberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(4L).state());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(0L).offsetState().get(0L).memberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(5L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(9L).state());

        // Acquire the same batch with member-2. 5 records will be acquired.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            "member-2",
            ShareAcquireMode.RECORD_LIMIT,
            2,
            5,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            5);

        // Should acquire the subset of records in InflightBatch which are still available.
        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(5, 5, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(6, 6, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(7, 7, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(8, 8, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(9, 9, 1));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(9, sharePartition.cachedState().get(0L).lastOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(5L).state());
        assertEquals("member-2", sharePartition.cachedState().get(0L).offsetState().get(5L).memberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(9L).state());
        assertEquals("member-2", sharePartition.cachedState().get(0L).offsetState().get(5L).memberId());
    }

    @Test
    public void testAcquireMultipleBatchesInRecordLimitMode() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = Mockito.spy(SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withPersister(persister)
            .build());

        // Create 3 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 10, 5).close();
        memoryRecordsBuilder(buffer, 15, 15).close();
        memoryRecordsBuilder(buffer, 30, 15).close();

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Acquire 10 records.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            BATCH_SIZE,
            10,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records, 10),
            FETCH_ISOLATION_HWM),
            10);

        assertArrayEquals(expectedAcquiredRecord(10, 19, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(29, sharePartition.cachedState().get(10L).lastOffset());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchMemberId());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).offsetState().get(19L).state());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).offsetState().get(19L).memberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(20L).state());
    }

    @Test
    public void testAcquireWhenInsufficientRecordsInRecordLimitMode() {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = Mockito.spy(SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withPersister(persister)
            .build());

        // Create 3 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 10, 5).close();
        memoryRecordsBuilder(buffer, 15, 5).close();
        memoryRecordsBuilder(buffer, 20, 5).close();

        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        // Requested 20 records, but only 15 available.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            2,
            20,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            15);
        assertArrayEquals(expectedAcquiredRecord(10, 24, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(24, sharePartition.cachedState().get(10L).lastOffset());

        // Since all the records in 3 batches are acquired, the offset state of the InFlight batch should be null and batch state should be ACQUIRED.
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(1, sharePartition.timer().size());
    }

    @Test
    public void testAcquireAndAcknowledgeMultipleSubsetRecordInRecordLimitMode() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build();

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.NONE,
            TimestampType.CREATE_TIME, 5, 2)) {
            // Append records from offset 10.
            memoryRecords(10, 4).records().forEach(builder::append);
            // Append records from offset 19.
            memoryRecords(16, 5).records().forEach(builder::append);
        }

        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            2,
            10,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            10);
        assertArrayEquals(expectedAcquiredRecord(5, 14, 1).toArray(), acquiredRecordsList.toArray());

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState());

        // Partially acknowledge the batch from 5-12.
        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(5, 9, List.of(ACKNOWLEDGE_TYPE_GAP_ID)),
            new ShareAcknowledgementBatch(10, 10, List.of(AcknowledgeType.RELEASE.id)),
            new ShareAcknowledgementBatch(11, 11, List.of(AcknowledgeType.ACCEPT.id)),
            new ShareAcknowledgementBatch(12, 12, List.of(AcknowledgeType.ACCEPT.id))));

        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));
        assertNotNull(sharePartition.cachedState().get(5L).offsetState());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            "member-2",
            ShareAcquireMode.RECORD_LIMIT,
            2,
            10,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            7);

        // Acquired batches will contain the following ->
        // 1. 10-10 (released offsets)
        // 2. 15-20 (new records)
        assertEquals(1, sharePartition.cachedState().size());
        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(10, 10, 2));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(15, 15, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(16, 16, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(17, 17, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(18, 18, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(19, 19, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(20, 20, 1));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-2"));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-2"));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-2"));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-2"));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-2"));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-2"));

        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
    }

    @Test
    public void testAcquireMultipleRecordsWithOverlapAndNewBatchInRecordLimitMode() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(5);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            BATCH_SIZE,
            3,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records, 0),
            FETCH_ISOLATION_HWM),
            3);

        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(0, 2, 1));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(3, sharePartition.nextFetchOffset());

        // Add records from 0-9 offsets, 3-5 should be acquired and 0-2 should be ignored.
        records = memoryRecords(10);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            BATCH_SIZE,
            3,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records, 0),
            FETCH_ISOLATION_HWM),
            3);

        expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(3, 3, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(4, 4, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(5, 5, 1));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(6, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(0L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(1L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(2L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(3L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(4L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testAcknowledgeInRecordLimitMode() {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = Mockito.spy(SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withPersister(persister)
            .build());

        MemoryRecords records = memoryRecords(10);
        // Acquire 1 record.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            2,
            1,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            1);

        assertArrayEquals(expectedAcquiredRecord(0, 0, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(1, sharePartition.nextFetchOffset());

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));

        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(0, 0, List.of(AcknowledgeType.ACCEPT.id))));

        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());
        assertEquals(1, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.startOffset());
        assertEquals(9, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(1L).state());

        // Acquire 2 records.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            2,
            2,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            2);

        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(1, 1, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(2, 2, 1));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());

        // Ack only 1 record
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(1, 1, List.of(AcknowledgeType.ACCEPT.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());
        assertEquals(3, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.startOffset());
        assertEquals(9, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(2L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(3L).state());
    }

    @Test
    public void testAcquisitionLockSingleRecordBatchInRecordLimitMode() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> sharePartition.timer().size() == 0 &&
                sharePartition.nextFetchOffset() == 10 &&
                sharePartition.cachedState().get(10L).batchState() == RecordState.AVAILABLE,
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(10L, List.of())));

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            BATCH_SIZE,
            2,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(memoryRecords(10, 5), 10),
            FETCH_ISOLATION_HWM),
            2);

        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(10, 10, 2));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(11, 11, 2));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(12, sharePartition.nextFetchOffset());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcquisitionLockTimeoutMultipleRecordBatchInRecordLimitMode() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = Mockito.spy(SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withPersister(persister)
            .build());

        // Create 3 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 0, 5).close();
        memoryRecordsBuilder(buffer, 5, 15).close();

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Acquire 3 records.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            BATCH_SIZE,
            2,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records, 10),
            FETCH_ISOLATION_HWM),
            2);

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        assertArrayEquals(expectedAcquiredRecord(0, 1, 1).toArray(), acquiredRecordsList.toArray());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        // There should be 2 timer tasks for 2 offsets.
        assertEquals(2, sharePartition.timer().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(2L).state());
        assertFalse(sharePartition.cachedState().get(0L).offsetState().get(0L).acquisitionLockTimeoutTask().hasExpired());
        assertFalse(sharePartition.cachedState().get(0L).offsetState().get(1L).acquisitionLockTimeoutTask().hasExpired());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> sharePartition.cachedState().get(0L).offsetState().get(0L).state() == RecordState.AVAILABLE &&
                sharePartition.cachedState().get(0L).offsetState().get(1L).state() == RecordState.AVAILABLE &&
                sharePartition.cachedState().get(0L).offsetState().get(0L).acquisitionLockTimeoutTask() == null &&
                sharePartition.cachedState().get(0L).offsetState().get(1L).acquisitionLockTimeoutTask() == null &&
                sharePartition.timer().size() == 0,
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(0L, List.of(0L, 1L))));
        // Acquisition lock timeout task has run already and next fetch offset is moved to 0.
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().get(0L).offsetState().get(0L).deliveryCount());
        assertEquals(1, sharePartition.cachedState().get(0L).offsetState().get(1L).deliveryCount());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            BATCH_SIZE,
            2,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records, 10),
            FETCH_ISOLATION_HWM),
            2);
        // delivery count increased to 2
        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(0, 0, 2));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(1, 1, 2));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());

        // Ack offset at 1 and let the other offset to expire again.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(1, 1, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS);
        TestUtils.waitForCondition(
            () -> sharePartition.cachedState().get(0L).offsetState().get(0L).state() == RecordState.AVAILABLE &&
                sharePartition.cachedState().get(0L).offsetState().get(0L).acquisitionLockTimeoutTask() == null &&
                sharePartition.timer().size() == 0,
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> assertionFailedMessage(sharePartition, Map.of(0L, List.of(0L, 1L))));

        assertEquals(0, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcquireCachedStateInitialGapOverlapsWithActualPartitionGapInRecordLimitMode() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(41L, 50L, RecordState.ARCHIVED.id, (short) 1) // There is a gap from 31-40
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();
        sharePartition.maybeInitialize();

        // Creating 2 batches starting from 16, such that there is a natural gap from 11 to 15
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 16, 20).close();
        memoryRecordsBuilder(buffer, 36, 25).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        // Acquire 20 records.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            BATCH_SIZE,
            20,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records, 0),
            FETCH_ISOLATION_HWM),
            20);

        // Acquired batches will contain the following ->
        // 1. 16-20 (gap offsets)
        // 2. 31-40 (gap offsets)
        // 3. 51-55 (new offsets)
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(16, 20, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(31, 40, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(51, 55, 1));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(16, sharePartition.startOffset());
        assertEquals(60, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(56, sharePartition.nextFetchOffset());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNull(persisterReadResultGapWindow);
    }

    @Test
    public void testAcquireCachedStateGapInBetweenOverlapsWithActualPartitionGapInRecordLimitMode() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 11L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(21L, 30L, RecordState.ACKNOWLEDGED.id, (short) 2), // There is a gap from 11 to 20
                        new PersisterStateBatch(41L, 50L, RecordState.ARCHIVED.id, (short) 1) // There is a gap from 31-40
                    ))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();
        sharePartition.maybeInitialize();

        // Creating 3 batches starting from 11, such that there is a natural gap from 36 to 40
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 11, 10).close();
        memoryRecordsBuilder(buffer, 21, 15).close();
        memoryRecordsBuilder(buffer, 41, 20).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            BATCH_SIZE,
            15,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records, 0),
            FETCH_ISOLATION_HWM),
            15);

        // Acquired batches will contain the following ->
        // 1. 11-20 (gap offsets)
        // 2. 31-35 (gap offsets)
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(11, 20, 1));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(31, 35, 1));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(50, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(36, sharePartition.nextFetchOffset());

        GapWindow persisterReadResultGapWindow = sharePartition.persisterReadResultGapWindow();
        assertNotNull(persisterReadResultGapWindow);
        // Gap still exists from 36 to 40
        assertEquals(36L, persisterReadResultGapWindow.gapStartOffset());
        assertEquals(50L, persisterReadResultGapWindow.endOffset());
    }

    @Test
    public void testAcquisitionLockOnReleasingAcknowledgedMultipleSubsetRecordBatchWithGapOffsetsInRecordLimitMode() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(5, 2);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(10, 2);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            BATCH_SIZE,
            2,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records1, 0),
            FETCH_ISOLATION_HWM),
            2);
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.RECORD_LIMIT,
            BATCH_SIZE,
            5,
            DEFAULT_FETCH_OFFSET,
            fetchPartitionData(records2, 0),
            FETCH_ISOLATION_HWM),
            5);

        // Acquired batches will contain the following ->
        // 1. 10-14, including 12-13 (gap offsets)
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(10, 14, 1));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());

        // 6 timer tasks for 10-14 offsets and batch 0-1
        assertEquals(6, sharePartition.timer().size());
        for (int i = 10; i <= 14; i++) {
            assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).offsetState().get((long) i).state());
            assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).offsetState().get((long) i).memberId());
            assertFalse(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask().hasExpired());
        }
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(15L).state());

        // Acknowledging over subset of second batch with subset of gap offsets.
        sharePartition.acknowledge(MEMBER_ID, List.of(new ShareAcknowledgementBatch(10, 12, List.of(AcknowledgeType.ACCEPT.id, AcknowledgeType.ACCEPT.id, ACKNOWLEDGE_TYPE_GAP_ID))));


        // Release acquired records for "member-1".
        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());
        assertEquals(5, sharePartition.nextFetchOffset());

        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertNull(sharePartition.cachedState().get(5L).offsetState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 0, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testMultipleMemberAcquireInDifferentAcquireModes() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        // Create 3 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 10, 5).close();
        memoryRecordsBuilder(buffer, 15, 15).close();
        memoryRecordsBuilder(buffer, 30, 15).close();

        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        // Member-1 acquires two full batches in batch_optimized mode.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            10,
            5,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            20);

        // Acknowledge a subset of records from member-1.
        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(10, 14, List.of(AcknowledgeType.RELEASE.id)),
            new ShareAcknowledgementBatch(15, 20, List.of(AcknowledgeType.ACCEPT.id)),
            new ShareAcknowledgementBatch(21, 22, List.of(AcknowledgeType.RELEASE.id)),
            new ShareAcknowledgementBatch(23, 28, List.of(AcknowledgeType.ACCEPT.id))));

        // Member-2 acquires records in record_limit mode.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            "member-2",
            ShareAcquireMode.RECORD_LIMIT,
            BATCH_SIZE,
            2,
            5,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            2);
        List<AcquiredRecords> expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(10, 10, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(11, 11, 2));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());
        assertEquals(12, sharePartition.nextFetchOffset());

        // Member-3 acquires records in batch_optimized mode.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            "member-3",
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            10,
            5,
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM),
            20);

        // Acquired batches will contain the following ->
        // 1. 12-14 (released offsets)
        // 2. 21-22 (released offsets)
        // 3. 30-44 (new offsets)
        expectedAcquiredRecord = new ArrayList<>(expectedAcquiredRecord(12, 12, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(13, 13, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(14, 14, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(21, 21, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(22, 22, 2));
        expectedAcquiredRecord.addAll(expectedAcquiredRecord(30, 44, 1));
        assertArrayEquals(expectedAcquiredRecord.toArray(), acquiredRecordsList.toArray());
        assertEquals(45, sharePartition.nextFetchOffset());
    }

    @Test
    public void testLsoMovementWithPendingAcknowledgements() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2)
            .withPersister(persister)
            .build();

        List<AcquiredRecords> records = fetchAcquiredRecords(sharePartition, memoryRecords(0, 5), 5);
        assertEquals(1, records.size());
        assertEquals(0, records.get(0).firstOffset());
        assertEquals(4, records.get(0).lastOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        AcquisitionLockTimerTask taskOrig = sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask();

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        // Acknowledge offsets 1 and 3 out of 0-4 with ACCEPT.
        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(1, 1, List.of(AcknowledgeType.ACCEPT.id)),
            new ShareAcknowledgementBatch(3, 3, List.of(AcknowledgeType.ACCEPT.id))));

        // Move LSO to 3.
        sharePartition.updateCacheAndOffsets(3);

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.startOffset());
        assertEquals(4, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());

        assertTrue(taskOrig.isCancelled()); // Original acq lock cancelled.
        assertNotEquals(taskOrig, sharePartition.cachedState().get(0L).offsetState().get(0L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());

        InFlightState offset0 = sharePartition.cachedState().get(0L).offsetState().get(0L);
        InFlightState offset1 = sharePartition.cachedState().get(0L).offsetState().get(1L);
        InFlightState offset2 = sharePartition.cachedState().get(0L).offsetState().get(2L);
        InFlightState offset3 = sharePartition.cachedState().get(0L).offsetState().get(3L);
        InFlightState offset4 = sharePartition.cachedState().get(0L).offsetState().get(4L);

        assertEquals(RecordState.ACQUIRED, offset0.state());
        assertNotNull(offset0.acquisitionLockTimeoutTask());

        assertEquals(RecordState.ACKNOWLEDGED, offset1.state());
        assertNull(offset1.acquisitionLockTimeoutTask());

        assertEquals(RecordState.ACQUIRED, offset2.state());
        assertNotNull(offset2.acquisitionLockTimeoutTask());

        assertEquals(RecordState.ACKNOWLEDGED, offset3.state());
        assertNull(offset3.acquisitionLockTimeoutTask());

        assertEquals(RecordState.ACQUIRED, offset4.state());
        assertNotNull(offset4.acquisitionLockTimeoutTask());

        assertEquals(3, sharePartition.timer().size()); // offsets 0,2 and 4 are still in ACQUIRED state.

        // Expire acquisition lock timeout
        mockTimer.advanceClock(ACQUISITION_LOCK_TIMEOUT_MS + 1);
        List<RecordState> expectedStates = List.of(RecordState.ARCHIVED, RecordState.ACKNOWLEDGED, RecordState.ARCHIVED, RecordState.ACKNOWLEDGED, RecordState.AVAILABLE);
        for (long i = 0; i <= 4; i++) {
            InFlightState offset = sharePartition.cachedState().get(0L).offsetState().get(i);
            assertNull(offset.acquisitionLockTimeoutTask());
            assertEquals(expectedStates.get((int) i), offset.state());
        }

        assertEquals(0, sharePartition.timer().size()); // All timer jobs have completed
        Mockito.verify(persister, Mockito.times(4)).writeState(Mockito.any());
    }

    @Test
    public void testLsoMovementWithPendingAcknowledgementsForBatches() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2)
            .withPersister(persister)
            .build();

        fetchAcquiredRecords(sharePartition, memoryRecords(0, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(5, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(10, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(15, 5), 5);
        fetchAcquiredRecords(sharePartition, memoryRecords(20, 5), 5);
        assertEquals(5, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        // Acknowledge batches 5-9 and 15-19 with ACCEPT.
        sharePartition.acknowledge(MEMBER_ID, List.of(
            new ShareAcknowledgementBatch(5, 9, List.of(AcknowledgeType.ACCEPT.id)),
            new ShareAcknowledgementBatch(15, 19, List.of(AcknowledgeType.ACCEPT.id))));

        // Move LSO to 12.
        sharePartition.updateCacheAndOffsets(12);

        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(12, sharePartition.startOffset());
        assertEquals(24, sharePartition.endOffset());
        assertEquals(5, sharePartition.cachedState().size());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(15L).batchAcquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(20L).batchAcquisitionLockTimeoutTask());

        assertEquals(3, sharePartition.timer().size());

        // Expire acquisition lock timeout.
        mockTimer.advanceClock(ACQUISITION_LOCK_TIMEOUT_MS + 1);

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(12, sharePartition.startOffset());
        assertEquals(24, sharePartition.endOffset());
        assertEquals(5, sharePartition.cachedState().size());

        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        // Batch 10-14 will now be tracked on a per-offset basis.
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(10L).offsetState().get(10L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(10L).offsetState().get(11L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(12L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(13L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(14L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(20L).batchState());

        assertNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(15L).batchAcquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(20L).batchAcquisitionLockTimeoutTask());

        assertEquals(0, sharePartition.timer().size()); // All timer jobs have completed
        Mockito.verify(persister, Mockito.times(4)).writeState(Mockito.any());
    }

    @Test
    public void testThrottleRecordsWhenPendingDeliveriesExist() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 19L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(20L, 22L, RecordState.ARCHIVED.id, (short) 2),
                        new PersisterStateBatch(26L, 30L, RecordState.AVAILABLE.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(5, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(5, sharePartition.nextFetchOffset());

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 7, 13).close();
        memoryRecordsBuilder(buffer, 20, 8).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Set max fetch records to 500, records should be acquired till the offset 26 of the fetched batch.
        // 16 records should be returned: 7-19, 23-25
        // The record at offset 26 has a delivery count of 3 and is a subject to be throttled; it should be skipped.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                5,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            16);

        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(7, 14, 1));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(15, 19, 3));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(23, 25, 1));

        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        assertEquals(26, sharePartition.nextFetchOffset());
        assertEquals(23, sharePartition.cachedState().get(23L).firstOffset());
        assertEquals(25, sharePartition.cachedState().get(23L).lastOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(7L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(23L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(26L).batchState());
        assertEquals(30L, sharePartition.endOffset());
        assertEquals(3, sharePartition.deliveryCompleteCount());

        // The record at offset 26 has a delivery count of 3 and is a subject to be throttled;
        // First acquisition attempt fails: batch size should be halved (5 -> 2)
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                26,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            2);

        expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(26, 26, 4));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(27, 27, 4));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
    }

    @Test
    public void testAcquireRecordsHalvesBatchSizeOnEachFailureUntilSingleRecordOnLastAttempt() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 34L, RecordState.AVAILABLE.id, (short) 4)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withPersister(persister)
            .withMaxDeliveryCount(7)
            .withDefaultAcquisitionLockTimeoutMs(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS)
            .build();

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(5, sharePartition.startOffset());
        assertEquals(34, sharePartition.endOffset());
        assertEquals(5, sharePartition.nextFetchOffset());

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 15, 20).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        // The record at offset 15 has a delivery count of 4 and is a subject to be throttled
        // First acquisition attempt fails: batch size should be halved (20 -> 10)
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                5,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            10);
        final List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(15, 15, 5));
        IntStream.range(1, 10).forEach(i -> expectedAcquiredRecords.addAll(expectedAcquiredRecord(15 + i, 15 + i, 5)));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS + 1);

        // Second failure: batch size halved again (now ~1/4 of original, 20 -> 5)
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                5,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            5);
        final List<AcquiredRecords> expectedAcquiredRecords2 = new ArrayList<>(expectedAcquiredRecord(15, 15, 6));
        IntStream.range(1, 5).forEach(i -> expectedAcquiredRecords2.addAll(expectedAcquiredRecord(15 + i, 15 + i, 6)));
        assertArrayEquals(expectedAcquiredRecords2.toArray(), acquiredRecordsList.toArray());

        // Allowing acquisition lock to expire.
        mockTimer.advanceClock(DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS + 1);

        List<AcquiredRecords> expectedLastAttemptAcquiredRecords;
        // Last delivery attempt, records are delivered individually.
        for (int i = 0; i < 5; i++) {
            acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                    MEMBER_ID,
                    ShareAcquireMode.BATCH_OPTIMIZED,
                    BATCH_SIZE,
                    500,
                    5,
                    fetchPartitionData(records),
                    FETCH_ISOLATION_HWM),
                1);
            expectedLastAttemptAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(15 + i, 15 + i, 7));
            assertArrayEquals(expectedLastAttemptAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        }

        // The record at offset 20 has a delivery count of 5 and is a subject to be throttled;
        // Second acquisition attempt fails: batch size should be ~1/4 of original, 20 -> 5
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                5,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            5);
        final List<AcquiredRecords> expectedAcquiredRecords3 = new ArrayList<>(expectedAcquiredRecord(20, 20, 6));
        IntStream.range(1, 5).forEach(i -> expectedAcquiredRecords3.addAll(expectedAcquiredRecord(20 + i, 20 + i, 6)));
        assertArrayEquals(expectedAcquiredRecords3.toArray(), acquiredRecordsList.toArray());

        // The record at offset 25 has a delivery count of 4 and is a subject to be throttled;
        // First acquisition attempt fails: batch size should be ~1/2 of original, 20 -> 10
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                5,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            10);
        final List<AcquiredRecords> expectedAcquiredRecords4 = new ArrayList<>(expectedAcquiredRecord(25, 25, 5));
        IntStream.range(1, 10).forEach(i -> expectedAcquiredRecords4.addAll(expectedAcquiredRecord(25 + i, 25 + i, 5)));
        assertArrayEquals(expectedAcquiredRecords4.toArray(), acquiredRecordsList.toArray());
    }

    @Test
    public void testLastAttemptRecordIsolationWithMixedDeliveryCount() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 15L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 34L, RecordState.AVAILABLE.id, (short) 2)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(34, sharePartition.endOffset());
        assertEquals(15, sharePartition.nextFetchOffset());

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 15, 20).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                15,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            20);

        // Release middle batch.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(17, 17, List.of(AcknowledgeType.RELEASE.id)),
                new ShareAcknowledgementBatch(20, 20, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                15,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            2);

        // Release all batch.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(15, 34, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).offsetState().get(15L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).offsetState().get(16L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).offsetState().get(17L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).offsetState().get(18L).state());
        assertEquals(3, sharePartition.cachedState().get(15L).offsetState().get(15L).deliveryCount());
        assertEquals(3, sharePartition.cachedState().get(15L).offsetState().get(16L).deliveryCount());
        assertEquals(4, sharePartition.cachedState().get(15L).offsetState().get(17L).deliveryCount());
        assertEquals(3, sharePartition.cachedState().get(15L).offsetState().get(18L).deliveryCount());

        // The record at offset 17 (delivery count of 4) is on its last attempt and should be delivered alone,
        // so this acquisition correctly stops at offset 16.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                5,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            2);
        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(15, 15, 4));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(16, 16, 4));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());

        // The record at offset 17 should ba delivered alone.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                5,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            1);
        expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(17, 17, 5));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
    }

    @Test
    public void testAcquisitionNotThrottledIfHighDeliveryCountRecordNotAcquired() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionAllData(0, 3, 15L, Errors.NONE.code(), Errors.NONE.message(),
                    List.of(
                        new PersisterStateBatch(15L, 19L, RecordState.AVAILABLE.id, (short) 1)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(List.of(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        assertEquals(SharePartitionState.ACTIVE, sharePartition.partitionState());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(19, sharePartition.endOffset());
        assertEquals(15, sharePartition.nextFetchOffset());

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 15, 5).close();
        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                15,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            5);

        // Release middle batch.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(15, 15, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                15,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            1);

        // Release middle batch.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            List.of(new ShareAcknowledgementBatch(16, 19, List.of(AcknowledgeType.RELEASE.id))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).offsetState().get(15L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).offsetState().get(16L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).offsetState().get(17L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).offsetState().get(18L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).offsetState().get(19L).state());
        assertEquals(3, sharePartition.cachedState().get(15L).offsetState().get(15L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(15L).offsetState().get(16L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(15L).offsetState().get(17L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(15L).offsetState().get(18L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(15L).offsetState().get(19L).deliveryCount());

        // This acquisition should not be throttled, as the high-delivery-count record (offset 15) was not acquired.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
                MEMBER_ID,
                ShareAcquireMode.BATCH_OPTIMIZED,
                BATCH_SIZE,
                500,
                5,
                fetchPartitionData(records),
                FETCH_ISOLATION_HWM),
            4);
        List<AcquiredRecords> expectedAcquiredRecords = new ArrayList<>(expectedAcquiredRecord(16, 16, 3));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(17, 17, 3));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(18, 18, 3));
        expectedAcquiredRecords.addAll(expectedAcquiredRecord(19, 19, 3));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
    }

    /**
     * This function produces transactional data of a given no. of records followed by a transactional marker (COMMIT/ABORT).
     */
    private void newTransactionalRecords(ByteBuffer buffer, ControlRecordType controlRecordType, int numRecords, long producerId, long baseOffset) {
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
            RecordBatch.CURRENT_MAGIC_VALUE,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            baseOffset,
            MOCK_TIME.milliseconds(),
            producerId,
            (short) 0,
            0,
            true,
            RecordBatch.NO_PARTITION_LEADER_EPOCH)) {
            for (int i = 0; i < numRecords; i++)
                builder.append(new SimpleRecord(MOCK_TIME.milliseconds(), "key".getBytes(), "value".getBytes()));

            builder.build();
        }
        writeTransactionMarker(buffer, controlRecordType, (int) baseOffset + numRecords, producerId);
    }

    private void writeTransactionMarker(ByteBuffer buffer, ControlRecordType controlRecordType, int offset, long producerId) {
        MemoryRecords.writeEndTransactionalMarker(buffer,
            offset,
            MOCK_TIME.milliseconds(),
            0,
            producerId,
            (short) 0,
            new EndTransactionMarker(controlRecordType, 0));
    }

    private List<FetchResponseData.AbortedTransaction> newAbortedTransactions() {
        FetchResponseData.AbortedTransaction abortedTransaction = new FetchResponseData.AbortedTransaction();
        abortedTransaction.setFirstOffset(0);
        abortedTransaction.setProducerId(1000L);
        return List.of(abortedTransaction);
    }

    private FetchPartitionData fetchPartitionData(Records records) {
        return fetchPartitionData(records, 0);
    }

    private FetchPartitionData fetchPartitionData(Records records, List<FetchResponseData.AbortedTransaction> abortedTransactions) {
        return fetchPartitionData(records, 0, abortedTransactions);
    }

    private FetchPartitionData fetchPartitionData(Records records, long logStartOffset) {
        return new FetchPartitionData(Errors.NONE, 5, logStartOffset, records,
            Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false);
    }

    private FetchPartitionData fetchPartitionData(Records records, long logStartOffset, List<FetchResponseData.AbortedTransaction> abortedTransactions) {
        return new FetchPartitionData(Errors.NONE, 5, logStartOffset, records,
            Optional.empty(), OptionalLong.empty(), Optional.of(abortedTransactions), OptionalInt.empty(), false);
    }

    private List<AcquiredRecords> fetchAcquiredRecords(SharePartition sharePartition, Records records, long logStartOffset, int expectedOffsetCount) {
        return fetchAcquiredRecords(sharePartition, records, records.batches().iterator().next().baseOffset(), logStartOffset, expectedOffsetCount);
    }

    private List<AcquiredRecords> fetchAcquiredRecords(SharePartition sharePartition, Records records, long fetchOffset, long logStartOffset, int expectedOffsetCount) {
        ShareAcquiredRecords shareAcquiredRecords = sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            MAX_FETCH_RECORDS,
            fetchOffset,
            fetchPartitionData(records, logStartOffset),
            FETCH_ISOLATION_HWM);
        return fetchAcquiredRecords(shareAcquiredRecords, expectedOffsetCount);
    }

    private List<AcquiredRecords> fetchAcquiredRecords(SharePartition sharePartition, Records records, int expectedOffsetCount) {
        ShareAcquiredRecords shareAcquiredRecords = sharePartition.acquire(
            MEMBER_ID,
            ShareAcquireMode.BATCH_OPTIMIZED,
            BATCH_SIZE,
            MAX_FETCH_RECORDS,
            records.batches().iterator().next().baseOffset(),
            fetchPartitionData(records),
            FETCH_ISOLATION_HWM);
        return fetchAcquiredRecords(shareAcquiredRecords, expectedOffsetCount);
    }

    private List<AcquiredRecords> fetchAcquiredRecords(ShareAcquiredRecords shareAcquiredRecords, int expectedOffsetCount) {
        assertNotNull(shareAcquiredRecords);
        assertEquals(expectedOffsetCount, shareAcquiredRecords.count());
        return shareAcquiredRecords.acquiredRecords();
    }

    private MemoryRecords memoryRecords(int numOfRecords) {
        return memoryRecords(0, numOfRecords);
    }

    private MemoryRecords memoryRecords(long startOffset, int numOfRecords) {
        try (MemoryRecordsBuilder builder = memoryRecordsBuilder(startOffset, numOfRecords)) {
            return builder.build();
        }
    }

    private List<AcquiredRecords> expectedAcquiredRecord(long baseOffset, long lastOffset, int deliveryCount) {
        return List.of(new AcquiredRecords()
            .setFirstOffset(baseOffset)
            .setLastOffset(lastOffset)
            .setDeliveryCount((short) deliveryCount));
    }

    private List<AcquiredRecords> expectedAcquiredRecords(MemoryRecords memoryRecords, int deliveryCount) {
        List<AcquiredRecords> acquiredRecordsList = new ArrayList<>();
        memoryRecords.batches().forEach(batch -> acquiredRecordsList.add(new AcquiredRecords()
            .setFirstOffset(batch.baseOffset())
            .setLastOffset(batch.lastOffset())
            .setDeliveryCount((short) deliveryCount)));
        return acquiredRecordsList;
    }

    private List<AcquiredRecords> expectedAcquiredRecords(long baseOffset, long lastOffset, int deliveryCount) {
        List<AcquiredRecords> acquiredRecordsList = new ArrayList<>();
        for (long i = baseOffset; i <= lastOffset; i++) {
            acquiredRecordsList.add(new AcquiredRecords()
                .setFirstOffset(i)
                .setLastOffset(i)
                .setDeliveryCount((short) deliveryCount));
        }
        return acquiredRecordsList;
    }

    public void mockPersisterReadStateMethod(Persister persister) {
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(List.of(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), List.of(
                        PartitionFactory.newPartitionAllData(0, 0, 0L, Errors.NONE.code(), Errors.NONE.message(),
                                List.of())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
    }

    private static class SharePartitionBuilder {

        private int defaultAcquisitionLockTimeoutMs = 30000;
        private int maxDeliveryCount = MAX_DELIVERY_COUNT;
        private int maxInflightRecords = MAX_IN_FLIGHT_RECORDS;

        private Persister persister = new NoOpStatePersister();
        private ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
        private GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        private SharePartitionState state = SharePartitionState.EMPTY;
        private Time time = MOCK_TIME;
        private SharePartitionMetrics sharePartitionMetrics = Mockito.mock(SharePartitionMetrics.class);

        private SharePartitionBuilder withMaxInflightRecords(int maxInflightRecords) {
            this.maxInflightRecords = maxInflightRecords;
            return this;
        }

        private SharePartitionBuilder withPersister(Persister persister) {
            this.persister = persister;
            return this;
        }

        private SharePartitionBuilder withDefaultAcquisitionLockTimeoutMs(int acquisitionLockTimeoutMs) {
            this.defaultAcquisitionLockTimeoutMs = acquisitionLockTimeoutMs;
            return this;
        }

        private SharePartitionBuilder withMaxDeliveryCount(int maxDeliveryCount) {
            this.maxDeliveryCount = maxDeliveryCount;
            return this;
        }

        private SharePartitionBuilder withReplicaManager(ReplicaManager replicaManager) {
            this.replicaManager = replicaManager;
            return this;
        }

        private SharePartitionBuilder withGroupConfigManager(GroupConfigManager groupConfigManager) {
            this.groupConfigManager = groupConfigManager;
            return this;
        }

        private SharePartitionBuilder withState(SharePartitionState state) {
            this.state = state;
            return this;
        }

        private SharePartitionBuilder withTime(Time time) {
            this.time = time;
            return this;
        }

        private SharePartitionBuilder withSharePartitionMetrics(SharePartitionMetrics sharePartitionMetrics) {
            this.sharePartitionMetrics = sharePartitionMetrics;
            return this;
        }

        public static SharePartitionBuilder builder() {
            return new SharePartitionBuilder();
        }

        public SharePartition build() {
            return new SharePartition(GROUP_ID, TOPIC_ID_PARTITION, 0, maxInflightRecords, maxDeliveryCount,
                    defaultAcquisitionLockTimeoutMs, mockTimer, time, persister, replicaManager, groupConfigManager,
                    state, Mockito.mock(SharePartitionListener.class), sharePartitionMetrics);
        }
    }
}
