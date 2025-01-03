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
import kafka.server.share.SharePartition.InFlightState;
import kafka.server.share.SharePartition.RecordState;
import kafka.server.share.SharePartition.SharePartitionState;
import kafka.server.share.SharePartitionManager.SharePartitionListener;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.ShareGroupAutoOffsetResetStrategy;
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.persister.NoOpShareStatePersister;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.persister.PersisterStateBatch;
import org.apache.kafka.server.share.persister.ReadShareGroupStateResult;
import org.apache.kafka.server.share.persister.TopicData;
import org.apache.kafka.server.share.persister.WriteShareGroupStateResult;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static kafka.server.share.SharePartition.EMPTY_MEMBER_ID;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;

public class SharePartitionTest {

    private static final String ACQUISITION_LOCK_NEVER_GOT_RELEASED = "Acquisition lock never got released.";
    private static final String GROUP_ID = "test-group";
    private static final int MAX_DELIVERY_COUNT = 5;
    private static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
    private static final String MEMBER_ID = "member-1";
    private static Timer mockTimer;
    private static final Time MOCK_TIME = new MockTime();
    private static final short MAX_IN_FLIGHT_MESSAGES = 200;
    private static final int ACQUISITION_LOCK_TIMEOUT_MS = 100;
    private static final int DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS = 300;
    private static final int MAX_FETCH_RECORDS = Integer.MAX_VALUE;

    @BeforeEach
    public void setUp() {
        mockTimer = new SystemTimerReaper("share-group-lock-timeout-test-reaper",
            new SystemTimer("share-group-lock-test-timeout"));
    }

    @AfterEach
    public void tearDown() throws Exception {
        mockTimer.close();
    }

    @Test
    public void testRecordStateValidateTransition() {
        // Null check.
        assertThrows(NullPointerException.class, () -> RecordState.AVAILABLE.validateTransition(null));
        // Same state transition check.
        assertThrows(IllegalStateException.class, () -> RecordState.AVAILABLE.validateTransition(RecordState.AVAILABLE));
        assertThrows(IllegalStateException.class, () -> RecordState.ACQUIRED.validateTransition(RecordState.ACQUIRED));
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.ACKNOWLEDGED));
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.ARCHIVED));
        // Invalid state transition to any other state from Acknowledged state.
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.AVAILABLE));
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.ACQUIRED));
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.ARCHIVED));
        // Invalid state transition to any other state from Archived state.
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.AVAILABLE));
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.ACKNOWLEDGED));
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.ARCHIVED));
        // Invalid state transition to any other state from Available state other than Acquired.
        assertThrows(IllegalStateException.class, () -> RecordState.AVAILABLE.validateTransition(RecordState.ACKNOWLEDGED));
        assertThrows(IllegalStateException.class, () -> RecordState.AVAILABLE.validateTransition(RecordState.ARCHIVED));

        // Successful transition from Available to Acquired.
        assertEquals(RecordState.ACQUIRED, RecordState.AVAILABLE.validateTransition(RecordState.ACQUIRED));
        // Successful transition from Acquired to any state.
        assertEquals(RecordState.AVAILABLE, RecordState.ACQUIRED.validateTransition(RecordState.AVAILABLE));
        assertEquals(RecordState.ACKNOWLEDGED, RecordState.ACQUIRED.validateTransition(RecordState.ACKNOWLEDGED));
        assertEquals(RecordState.ARCHIVED, RecordState.ACQUIRED.validateTransition(RecordState.ARCHIVED));
    }

    @Test
    public void testRecordStateForId() {
        assertEquals(RecordState.AVAILABLE, RecordState.forId((byte) 0));
        assertEquals(RecordState.ACQUIRED, RecordState.forId((byte) 1));
        assertEquals(RecordState.ACKNOWLEDGED, RecordState.forId((byte) 2));
        assertEquals(RecordState.ARCHIVED, RecordState.forId((byte) 4));
        // Invalid check.
        assertThrows(IllegalArgumentException.class, () -> RecordState.forId((byte) 5));
    }

    @Test
    public void testMaybeInitialize() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    Arrays.asList(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

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
    }

    @Test
    public void testMaybeInitializeDefaultStartEpochGroupConfigReturnsEarliest() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    Collections.emptyList())))));
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
    }

    @Test
    public void testMaybeInitializeDefaultStartEpochGroupConfigReturnsLatest() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    Collections.emptyList())))));
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
    }

    @Test
    public void testMaybeInitializeDefaultStartEpochGroupConfigReturnsByDuration() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    Collections.emptyList())))));
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

        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(MOCK_TIME.milliseconds() - TimeUnit.HOURS.toMillis(1), 15L, Optional.empty());
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
    }

    @Test
    public void testMaybeInitializeDefaultStartEpochGroupConfigNotPresent() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    Collections.emptyList())))));
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
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    Collections.emptyList())))));
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
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    Collections.emptyList())))));
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
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(
                    0, PartitionFactory.DEFAULT_STATE_EPOCH,
                    PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_ERROR_CODE,
                    PartitionFactory.DEFAULT_ERR_MESSAGE,
                    Collections.emptyList())))));
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
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    Arrays.asList(
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
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    Arrays.asList(
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
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.NONE.code(), Errors.NONE.message(), Collections.emptyList()))))
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
    }

    @Test
    public void testMaybeInitializeWithErrorPartitionResponse() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);

        // Mock NOT_COORDINATOR error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.NOT_COORDINATOR.code(), Errors.NOT_COORDINATOR.message(),
                    Collections.emptyList())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, CoordinatorNotAvailableException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock COORDINATOR_NOT_AVAILABLE error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.COORDINATOR_NOT_AVAILABLE.code(), Errors.COORDINATOR_NOT_AVAILABLE.message(),
                    Collections.emptyList())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, CoordinatorNotAvailableException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock COORDINATOR_LOAD_IN_PROGRESS error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.COORDINATOR_LOAD_IN_PROGRESS.code(), Errors.COORDINATOR_LOAD_IN_PROGRESS.message(),
                    Collections.emptyList())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, CoordinatorNotAvailableException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock GROUP_ID_NOT_FOUND error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message(),
                    Collections.emptyList())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, GroupIdNotFoundException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock UNKNOWN_TOPIC_OR_PARTITION error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), Errors.UNKNOWN_TOPIC_OR_PARTITION.message(),
                    Collections.emptyList())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, UnknownTopicOrPartitionException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock FENCED_STATE_EPOCH error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.FENCED_STATE_EPOCH.code(), Errors.FENCED_STATE_EPOCH.message(),
                    Collections.emptyList())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, FencedStateEpochException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock FENCED_LEADER_EPOCH error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.FENCED_LEADER_EPOCH.code(), Errors.FENCED_LEADER_EPOCH.message(),
                    Collections.emptyList())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, NotLeaderOrFollowerException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());

        // Mock UNKNOWN_SERVER_ERROR error.
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.UNKNOWN_SERVER_ERROR.code(), Errors.UNKNOWN_SERVER_ERROR.message(),
                    Collections.emptyList())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, UnknownServerException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithInvalidStartOffsetStateBatches() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 3, 6L, Errors.NONE.code(), Errors.NONE.message(),
                    Arrays.asList(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalStateException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithInvalidTopicIdResponse() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(Uuid.randomUuid(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    Arrays.asList(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalStateException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithInvalidPartitionResponse() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(1, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                    Arrays.asList(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalStateException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithNoOpShareStatePersister() {
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
        assertFutureThrows(result, IllegalStateException.class);
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
        assertFutureThrows(result, IllegalStateException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition.partitionState());
    }

    @Test
    public void testMaybeInitializeWithEmptyTopicsData() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.emptyList());
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        CompletableFuture<Void> result = sharePartition.maybeInitialize();
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalStateException.class);
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
        assertFutureThrows(result, RuntimeException.class);
        assertEquals(SharePartitionState.FAILED, sharePartition1.partitionState());

        persister = Mockito.mock(Persister.class);
        // Throw exception for read state.
        Mockito.when(persister.readState(Mockito.any())).thenThrow(new RuntimeException("Read exception"));
        SharePartition sharePartition2 = SharePartitionBuilder.builder().withPersister(persister).build();

        assertThrows(RuntimeException.class, sharePartition2::maybeInitialize);
    }

    @Test
    public void testAcquireSingleRecord() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(1);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 3, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            1);

        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(1, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(0, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testAcquireMultipleRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(5, 10);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcquireWithMaxFetchRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        // Less-number of records than max fetch records.
        MemoryRecords records = memoryRecords(5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            10,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
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
            10,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
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
    public void testAcquireWithMultipleBatchesAndMaxFetchRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        // Create 3 batches of records.
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        memoryRecordsBuilder(buffer, 5, 10).close();
        memoryRecordsBuilder(buffer, 15, 15).close();
        memoryRecordsBuilder(buffer, 15, 30).close();

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        // Acquire 10 records.
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            10,
            new FetchPartitionData(Errors.NONE, 20, 10, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
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
    }

    @Test
    public void testAcquireMultipleRecordsWithOverlapAndNewBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(5, 0);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());

        // Add records from 0-9 offsets, 5-9 should be acquired and 0-4 should be ignored.
        records = memoryRecords(10, 0);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertArrayEquals(expectedAcquiredRecords(memoryRecords(5, 5), 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
    }

    @Test
    public void testAcquireSameBatchAgain() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(5, 10);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            0);

        // No records should be returned as the batch is already acquired.
        assertEquals(0, acquiredRecordsList.size());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Send subset of the same batch again, no records should be returned.
        MemoryRecords subsetRecords = memoryRecords(2, 10);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, subsetRecords,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            0);

        // No records should be returned as the batch is already acquired.
        assertEquals(0, acquiredRecordsList.size());
        assertEquals(15, sharePartition.nextFetchOffset());
        // Cache shouldn't be tracking per offset records
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcquireWithEmptyFetchRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, MemoryRecords.EMPTY,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            0);

        assertEquals(0, acquiredRecordsList.size());
        assertEquals(0, sharePartition.nextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetInitialState() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        assertEquals(0, sharePartition.nextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWithCachedStateAcquired() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertEquals(5, sharePartition.nextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWithFindAndCachedStateEmpty() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        sharePartition.findNextFetchOffset(true);
        assertTrue(sharePartition.findNextFetchOffset());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertFalse(sharePartition.findNextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWithFindAndCachedState() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        sharePartition.findNextFetchOffset(true);
        assertTrue(sharePartition.findNextFetchOffset());
        sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertEquals(5, sharePartition.nextFetchOffset());
        assertFalse(sharePartition.findNextFetchOffset());
    }

    @Test
    public void testCanAcquireRecordsWithEmptyCache() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightMessages(1).build();
        assertTrue(sharePartition.canAcquireRecords());
    }

    @Test
    public void testCanAcquireRecordsWithCachedDataAndLimitNotReached() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightMessages(6).build();
        sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        // Limit not reached as only 6 in-flight messages is the limit.
        assertTrue(sharePartition.canAcquireRecords());
    }

    @Test
    public void testCanAcquireRecordsWithCachedDataAndLimitReached() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightMessages(1)
            .withState(SharePartitionState.ACTIVE)
            .build();
        sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        // Limit reached as only one in-flight message is the limit.
        assertFalse(sharePartition.canAcquireRecords());
    }

    @Test
    public void testMaybeAcquireAndReleaseFetchLock() {
        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(-1L, 0L, Optional.empty());
        Mockito.doReturn(new OffsetResultHolder(Optional.of(timestampAndOffset), Optional.empty())).
            when(replicaManager).fetchOffsetForTimestamp(Mockito.any(TopicPartition.class), Mockito.anyLong(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        SharePartition sharePartition = SharePartitionBuilder.builder().withReplicaManager(replicaManager).build();
        sharePartition.maybeInitialize();
        assertTrue(sharePartition.maybeAcquireFetchLock());
        // Lock cannot be acquired again, as already acquired.
        assertFalse(sharePartition.maybeAcquireFetchLock());
        // Release the lock.
        sharePartition.releaseFetchLock();
        // Lock can be acquired again.
        assertTrue(sharePartition.maybeAcquireFetchLock());
    }

    @Test
    public void testAcknowledgeSingleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        MemoryRecords records1 = memoryRecords(1, 0);
        MemoryRecords records2 = memoryRecords(1, 1);

        // Another batch is acquired because if there is only 1 batch, and it is acknowledged, the batch will be removed from cachedState
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 10, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            1);

        assertEquals(1, acquiredRecordsList.size());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 10, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            1);

        assertEquals(1, acquiredRecordsList.size());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(1, 1, Collections.singletonList((byte) 1))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        assertEquals(2, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(1L).batchState());
        assertEquals(1, sharePartition.cachedState().get(1L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(1L).offsetState());
    }

    @Test
    public void testAcknowledgeMultipleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(10, 5);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            10);

        assertEquals(1, acquiredRecordsList.size());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(5, 14, Collections.singletonList((byte) 1))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testAcknowledgeMultipleRecordBatchWithGapOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(5, 10);
        // Gap from 15-17 offsets.
        recordsBuilder.appendWithOffset(18, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            2);

        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 30, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            9);

        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(19, sharePartition.nextFetchOffset());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Arrays.asList(
                new ShareAcknowledgementBatch(5, 6, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(10, 18, Arrays.asList(
                    (byte) 2, (byte) 2, (byte) 2,
                    (byte) 2, (byte) 2, (byte) 0,
                    (byte) 0, (byte) 0, (byte) 1
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
    }

    @Test
    public void testAcknowledgeMultipleSubsetRecordBatchWithGapOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            2);

        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 30, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            11);

        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Acknowledging over subset of both batch with subset of gap offsets.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(6, 18, Arrays.asList(
                (byte) 1, (byte) 1, (byte) 1,
                (byte) 1, (byte) 1, (byte) 1,
                (byte) 0, (byte) 0, (byte) 1,
                (byte) 0, (byte) 1, (byte) 0,
                (byte) 1))));
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
    }

    @Test
    public void testAcknowledgeOutOfRangeCachedData() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        // Acknowledge a batch when cache is empty.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(0, 15, Collections.singletonList((byte) 3))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRecordStateException.class);

        MemoryRecords records = memoryRecords(5, 5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(20, 25, Collections.singletonList((byte) 3))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRequestException.class);
    }

    @Test
    public void testAcknowledgeOutOfRangeCachedDataFirstBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        // Create data for the batch with offsets 0-4.
        MemoryRecords records = memoryRecords(5, 0);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());

        // Create data for the batch with offsets 20-24.
        records = memoryRecords(5, 20);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());

        // Acknowledge a batch when first batch violates the range.
        List<ShareAcknowledgementBatch> acknowledgeBatches = Arrays.asList(
            new ShareAcknowledgementBatch(0, 10, Collections.singletonList((byte) 1)),
            new ShareAcknowledgementBatch(20, 24, Collections.singletonList((byte) 1)));
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID, acknowledgeBatches);
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRequestException.class);

        // Create data for the batch with offsets 5-10.
        records = memoryRecords(6, 5);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            6);

        assertEquals(1, acquiredRecordsList.size());

        // Previous failed acknowledge request should succeed now.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID, acknowledgeBatches);
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());
    }

    @Test
    public void testAcknowledgeWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(5, 5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            "member-2",
            Collections.singletonList(new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 3))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRecordStateException.class);
    }

    @Test
    public void testAcknowledgeWhenOffsetNotAcquired() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(5, 5);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 2))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        // Acknowledge the same batch again but with ACCEPT type.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 1))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRecordStateException.class);

        // Re-acquire the same batch and then acknowledge subset with ACCEPT type.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());

        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(6, 8, Collections.singletonList((byte) 3))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

        // Re-acknowledge the subset batch with REJECT type.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(6, 8, Collections.singletonList((byte) 3))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRecordStateException.class);
    }

    @Test
    public void testAcknowledgeRollbackWithFullBatchError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(5, 10);
        MemoryRecords records3 = memoryRecords(5, 15);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());
        // Cached data with offset 5-19 should exist.
        assertEquals(3, sharePartition.cachedState().size());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Arrays.asList(
                new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(10, 14, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(15, 19, Collections.singletonList((byte) 1)),
                // Add another batch which should fail the request.
                new ShareAcknowledgementBatch(15, 19, Collections.singletonList((byte) 1))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRecordStateException.class);

        // Check the state of the cache. The state should be acquired itself.
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
    }

    @Test
    public void testAcknowledgeRollbackWithSubsetError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(5, 10);
        MemoryRecords records3 = memoryRecords(5, 15);
        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 0, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertEquals(1, acquiredRecordsList.size());
        // Cached data with offset 5-19 should exist.
        assertEquals(3, sharePartition.cachedState().size());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Arrays.asList(
                new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(10, 14, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(15, 19, Collections.singletonList((byte) 1)),
                // Add another batch which should fail the request.
                new ShareAcknowledgementBatch(16, 19, Collections.singletonList((byte) 1))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRecordStateException.class);

        // Check the state of the cache. The state should be acquired itself.
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        // Though the last batch is subset but the offset state map will not be exploded as the batch is
        // not in acquired state due to previous batch acknowledgement.
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
    }

    @Test
    public void testAcquireReleasedRecord() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records = memoryRecords(5, 10);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(12, 13, Collections.singletonList((byte) 2))));
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

        // Send the same fetch request batch again but only 2 offsets should come as acquired.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            2);

        assertArrayEquals(expectedAcquiredRecords(12, 13, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcquireReleasedRecordMultipleBatches() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(5, 10);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(5, 15);
        // Third fetch request with 5 records starting from offset 23, gap of 3 offsets.
        MemoryRecords records3 = memoryRecords(5, 23);
        // Fourth fetch request with 5 records starting from offset 28.
        MemoryRecords records4 = memoryRecords(5, 28);

        List<AcquiredRecords> acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 30, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 30, 3, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertArrayEquals(expectedAcquiredRecords(records3, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(28, sharePartition.nextFetchOffset());

        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 30, 3, records4,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

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
            Collections.singletonList(new ShareAcknowledgementBatch(12, 30, Collections.singletonList((byte) 2))));
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
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            3);

        assertArrayEquals(expectedAcquiredRecords(12, 14, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Though record2 batch exists to acquire but send batch record3, it should be acquired but
        // next fetch offset should not move.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 40, 3, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            5);

        assertArrayEquals(expectedAcquiredRecords(records3, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Acquire partial records from batch 2.
        MemoryRecords subsetRecords = memoryRecords(2, 17);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, subsetRecords,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            2);

        assertArrayEquals(expectedAcquiredRecords(17, 18, 2).toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(15, sharePartition.nextFetchOffset());

        // Acquire partial records from record 4 to further test if the next fetch offset move
        // accordingly once complete record 2 is also acquired.
        subsetRecords = memoryRecords(1, 28);
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, subsetRecords,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            1);

        assertArrayEquals(expectedAcquiredRecords(28, 28, 2).toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(15, sharePartition.nextFetchOffset());

        // Try to acquire complete record 2 though it's already partially acquired, the next fetch
        // offset should move.
        acquiredRecordsList = fetchAcquiredRecords(sharePartition.acquire(
            MEMBER_ID,
            MAX_FETCH_RECORDS,
            new FetchPartitionData(Errors.NONE, 20, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)),
            3);

        // Offset 15,16 and 19 should be acquired.
        List<AcquiredRecords> expectedAcquiredRecords = expectedAcquiredRecords(15, 16, 2);
        expectedAcquiredRecords.addAll(expectedAcquiredRecords(19, 19, 2));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(29, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcquisitionLockForAcquiringSingleRecord() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 3, 0, memoryRecords(1),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        TestUtils.waitForCondition(
                () -> sharePartition.nextFetchOffset() == 0 &&
                        sharePartition.cachedState().get(0L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(0L).batchDeliveryCount() == 1 &&
                        sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask() == null &&
                        sharePartition.timer().size() == 0,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);
    }

    @Test
    public void testAcquisitionLockForAcquiringMultipleRecords() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5, 10),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());

        // Allowing acquisition lock to expire.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0
                        && sharePartition.nextFetchOffset() == 10
                        && sharePartition.cachedState().get(10L).batchState() == RecordState.AVAILABLE
                        && sharePartition.cachedState().get(10L).batchDeliveryCount() == 1
                        && sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);
    }

    @Test
    public void testAcquisitionLockForAcquiringMultipleRecordsWithOverlapAndNewBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5, 0),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Add records from 0-9 offsets, 5-9 should be acquired and 0-4 should be ignored.
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(10, 0),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Allowing acquisition lock to expire. The acquisition lock timeout will cause release of records for all the acquired records.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 0 &&
                        sharePartition.cachedState().get(0L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(5L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask() == null &&
                        sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);
    }

    @Test
    public void testAcquisitionLockForAcquiringSameBatchAgain() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5, 10),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 10 &&
                        sharePartition.cachedState().get(10L).batchState() == RecordState.AVAILABLE,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

        // Acquire the same batch again.
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5, 10),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // Acquisition lock timeout task should be created on re-acquire action.
        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingSingleRecordBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 10, 0, memoryRecords(1, 0),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(new ShareAcknowledgementBatch(0, 0, Collections.singletonList((byte) 2))));

        assertNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

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
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleRecordBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(10, 5),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(new ShareAcknowledgementBatch(5, 14, Collections.singletonList((byte) 2))));

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Allowing acquisition lock to expire. This will not cause any change to cached state map since the batch is already acknowledged.
        // Hence, the acquisition lock timeout task would be cancelled already.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 5 &&
                        sharePartition.cachedState().get(5L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(5L).batchDeliveryCount() == 1 &&
                        sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleRecordBatchWithGapOffsets() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(5, 10);
        // Gap from 15-17 offsets.
        recordsBuilder.appendWithOffset(18, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();
        MemoryRecords records3 = memoryRecords(2, 1);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(1L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());

        sharePartition.acknowledge(MEMBER_ID,
                // Do not send gap offsets to verify that they are ignored and accepted as per client ack.
                Collections.singletonList(new ShareAcknowledgementBatch(5, 18, Collections.singletonList((byte) 1))));

        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(1L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire. The acquisition lock timeout will cause release of records for batch with starting offset 1.
        // Since, other records have been acknowledged.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 1 &&
                        sharePartition.cachedState().get(1L).batchAcquisitionLockTimeoutTask() == null &&
                        sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask() == null &&
                        sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(1L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());
    }

    @Test
    public void testAcquisitionLockForAcquiringSubsetBatchAgain() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(8, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 10 &&
                        sharePartition.cachedState().size() == 1 &&
                        sharePartition.cachedState().get(10L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

        // Acquire subset of records again.
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(3, 12),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

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
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleSubsetRecordBatchWithGapOffsets() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Acknowledging over subset of both batch with subset of gap offsets.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(new ShareAcknowledgementBatch(
                        6, 18, Arrays.asList(
                        (byte) 1, (byte) 1, (byte) 1,
                        (byte) 1, (byte) 1, (byte) 1,
                        (byte) 0, (byte) 0, (byte) 1,
                        (byte) 0, (byte) 1, (byte) 0,
                        (byte) 1))));

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

        // Allowing acquisition lock to expire for the offsets that have not been acknowledged yet.
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
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

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
    }

    @Test
    public void testAcquisitionLockTimeoutCauseMaxDeliveryCountExceed() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2) // Only 2 delivery attempts will be made before archiving the records
            .withState(SharePartitionState.ACTIVE)
            .build();

        // Adding memoryRecords(10, 0) in the sharePartition to make sure that SPSO doesn't move forward when delivery count of records2
        // exceed the max delivery count.
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 3, 0, memoryRecords(10, 0),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 3, 0, memoryRecords(10, 10),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 0 &&
                        sharePartition.cachedState().get(10L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(10L).batchDeliveryCount() == 1 &&
                        sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 3, 0, memoryRecords(10, 10),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(2, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNotNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire to archive the records that reach max delivery count.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 0 &&
                        // After the second delivery attempt fails to acknowledge the record correctly, the record should be archived.
                        sharePartition.cachedState().get(10L).batchState() == RecordState.ARCHIVED &&
                        sharePartition.cachedState().get(10L).batchDeliveryCount() == 2 &&
                        sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);
    }

    @Test
    public void testAcquisitionLockTimeoutCauseSPSOMoveForward() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2) // Only 2 delivery attempts will be made before archiving the records
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 3, 0, memoryRecords(10, 0),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 0 &&
                        sharePartition.cachedState().get(0L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(0L).batchDeliveryCount() == 1 &&
                        sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 3, 0, memoryRecords(5, 0),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

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
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

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
    }

    @Test
    public void testAcquisitionLockTimeoutCauseSPSOMoveForwardAndClearCachedState() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withMaxDeliveryCount(2) // Only 2 delivery attempts will be made before archiving the records
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 3, 0, memoryRecords(10, 0),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 0 &&
                        sharePartition.cachedState().get(0L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 3, 0, memoryRecords(10, 0),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(0L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire to archive the records that reach max delivery count.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        // After the second failed attempt to acknowledge the record batch successfully, the record batch is archived.
                        // Since this is the first batch in the share partition, SPSO moves forward and the cachedState is cleared
                        sharePartition.cachedState().isEmpty() &&
                        sharePartition.nextFetchOffset() == 10,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);
    }

    @Test
    public void testAcknowledgeAfterAcquisitionLockTimeout() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 5 &&
                        sharePartition.cachedState().get(5L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

        // Acknowledge with ACCEPT type should throw InvalidRecordStateException since they've been released due to acquisition lock timeout.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID,
                Collections.singletonList(new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 1))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRecordStateException.class);
        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Try acknowledging with REJECT type should throw InvalidRecordStateException since they've been released due to acquisition lock timeout.
        ackResult = sharePartition.acknowledge(MEMBER_ID,
                Collections.singletonList(new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 3))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRecordStateException.class);
        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockAfterDifferentAcknowledges() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Acknowledge with REJECT type.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(new ShareAcknowledgementBatch(5, 6, Collections.singletonList((byte) 2))));

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());

        // Acknowledge with ACCEPT type.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(new ShareAcknowledgementBatch(8, 9, Collections.singletonList((byte) 1))));

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire will only affect the offsets that have not been acknowledged yet.
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
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
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
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(10, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());

        // Allowing acquisition lock to expire. Even if write share group state RPC fails, state transition still happens.
        TestUtils.waitForCondition(
                () -> sharePartition.timer().size() == 0 &&
                        sharePartition.nextFetchOffset() == 5 &&
                        sharePartition.cachedState().size() == 1 &&
                        sharePartition.cachedState().get(5L).batchState() == RecordState.AVAILABLE &&
                        sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask() == null,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);
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
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(6, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(new ShareAcknowledgementBatch(8, 9, Collections.singletonList((byte) 1))));

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        // Allowing acquisition lock to expire. Even if write share group state RPC fails, state transition still happens.
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
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 10, 0, memoryRecords(1, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testReleaseMultipleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(10, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
    }

    @Test
    public void testReleaseMultipleAcknowledgedRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records0 = memoryRecords(5, 0);
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecords records2 = memoryRecords(9, 10);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records0,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(new ShareAcknowledgementBatch(5, 18, Collections.singletonList((byte) 1))));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testReleaseAcknowledgedMultipleSubsetRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(2, 5);

        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // Acknowledging over subset of both batch with subset of gap offsets.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(new ShareAcknowledgementBatch(6, 18, Arrays.asList(
                (byte) 1, (byte) 1, (byte) 1,
                (byte) 1, (byte) 1, (byte) 1,
                (byte) 0, (byte) 0, (byte) 1,
                (byte) 0, (byte) 1, (byte) 0,
                (byte) 1))));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
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
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testReleaseAcquiredRecordsWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(1, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        sharePartition.acquire("member-2", MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        // Acknowledging over subset of second batch with subset of gap offsets.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(new ShareAcknowledgementBatch(10, 18, Arrays.asList(
                (byte) 1, (byte) 1, (byte) 0, (byte) 0,
                (byte) 1, (byte) 0, (byte) 1, (byte) 0,
                (byte) 1))));

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
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

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
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testReleaseAcquiredRecordsWithAnotherMemberAndSubsetAcknowledged() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        sharePartition.acquire("member-2", MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        // Acknowledging over subset of second batch with subset of gap offsets.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(new ShareAcknowledgementBatch(10, 18, Arrays.asList(
                (byte) 1, (byte) 1, (byte) 0, (byte) 0,
                (byte) 1, (byte) 0, (byte) 1, (byte) 0,
                (byte) 1))));

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
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // Ack subset of records by "member-2".
        sharePartition.acknowledge("member-2",
                Collections.singletonList(new ShareAcknowledgementBatch(5, 5, Collections.singletonList((byte) 1))));

        // Release acquired records for "member-2".
        releaseResult = sharePartition.releaseAcquiredRecords("member-2");
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(6, sharePartition.nextFetchOffset());
        // Check cached state.
        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
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
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
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
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID,
                Collections.singletonList(new ShareAcknowledgementBatch(5, 6, Collections.singletonList((byte) 2))));

        sharePartition.acknowledge(MEMBER_ID,
                Collections.singletonList(new ShareAcknowledgementBatch(8, 9, Collections.singletonList((byte) 1))));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());
        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetAfterReleaseAcquiredRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxDeliveryCount(2)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, memoryRecords(10, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        MemoryRecords records2 = memoryRecords(5, 10);
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(10, 14, Collections.singletonList((byte) 2))));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetAfterReleaseAcquiredRecordsSubset() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxDeliveryCount(2)
            .withState(SharePartitionState.ACTIVE)
            .build();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(5, 10);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(5, 15);
        // third fetch request with 5 records starting from offset20.
        MemoryRecords records3 = memoryRecords(5, 20);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 50, 3, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, new ArrayList<>(Arrays.asList(
                new ShareAcknowledgementBatch(13, 16, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(17, 19, Collections.singletonList((byte) 3)),
                new ShareAcknowledgementBatch(20, 24, Collections.singletonList((byte) 2))
        )));

        // Send next batch from offset 13, only 2 records should be acquired.
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // Send next batch from offset 15, only 2 records should be acquired.
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(15L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(20L).batchMemberId());
        assertNull(sharePartition.cachedState().get(20L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(15L).offsetState());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetCacheCleared() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxDeliveryCount(2)
            .withState(SharePartitionState.ACTIVE)
            .build();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(5, 10);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(5, 15);
        // Third fetch request with 5 records starting from offset 20.
        MemoryRecords records3 = memoryRecords(5, 20);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 50, 3, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, new ArrayList<>(Arrays.asList(
                new ShareAcknowledgementBatch(10, 12, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(13, 16, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(17, 19, Collections.singletonList((byte) 3)),
                new ShareAcknowledgementBatch(20, 24, Collections.singletonList((byte) 2))
        )));

        // Send next batch from offset 13, only 2 records should be acquired.
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // Send next batch from offset 15, only 2 records should be acquired.
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testReleaseAcquiredRecordsSubsetWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS,
                new FetchPartitionData(Errors.NONE, 30, 0, memoryRecords(7, 5),
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID,
                Collections.singletonList(new ShareAcknowledgementBatch(5, 7, Collections.singletonList((byte) 1))));

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
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(10, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertTrue(releaseResult.isCompletedExceptionally());
        assertFutureThrows(releaseResult, GroupIdNotFoundException.class);

        // Due to failure in writeShareGroupState, the cached state should not be updated.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
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
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(6, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID,
                Collections.singletonList(new ShareAcknowledgementBatch(8, 9, Collections.singletonList((byte) 1))));

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertTrue(releaseResult.isCompletedExceptionally());
        assertFutureThrows(releaseResult, GroupIdNotFoundException.class);

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
    }

    @Test
    public void testAcquisitionLockOnReleasingMultipleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(10, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        // Acquisition lock timer task would be cancelled by the release acquired records operation.
        assertNull(sharePartition.cachedState().get(5L).batchAcquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnReleasingAcknowledgedMultipleSubsetRecordBatchWithGapOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // Acknowledging over subset of both batch with subset of gap offsets.
        sharePartition.acknowledge(MEMBER_ID,
                Collections.singletonList(new ShareAcknowledgementBatch(6, 18, Arrays.asList(
                        (byte) 1, (byte) 1, (byte) 1,
                        (byte) 1, (byte) 1, (byte) 1,
                        (byte) 0, (byte) 0, (byte) 1,
                        (byte) 0, (byte) 1, (byte) 0,
                        (byte) 1))));

        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
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
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 7),
                Optional.empty(),
                        OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS,
                new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 12), Optional.empty(),
                        OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS,
                new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 17), Optional.empty(),
                        OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS,
                new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 22), Optional.empty(),
                        OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS,
                new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 27), Optional.empty(),
                        OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS,
                new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 32), Optional.empty(),
                        OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, Arrays.asList(
                new ShareAcknowledgementBatch(2, 6, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(12, 16, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(22, 26, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(27, 31, Collections.singletonList((byte) 3))
        ));

        // LSO is at 20.
        sharePartition.updateCacheAndOffsets(20);

        assertEquals(22, sharePartition.nextFetchOffset());
        assertEquals(20, sharePartition.startOffset());
        assertEquals(36, sharePartition.endOffset());

        // For cached state corresponding to entry 2, the batch state will be ACKNOWLEDGED, hence it will be cleared as part of acknowledgment.
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
    }

    @Test
    public void testLsoMovementForArchivingOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 7),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(4, 8, Collections.singletonList((byte) 1))));

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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 7),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 7),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 7),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

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
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(7, 8, Collections.singletonList((byte) 1))));

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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2), Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 7), Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

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
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(7, 8, Collections.singletonList((byte) 2))));

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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2), Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 7), Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        // Acknowledge with RELEASE action.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(7, 8, Collections.singletonList((byte) 2))));

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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2), Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 7), Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        // Acknowledge with RELEASE action.
        sharePartition.acknowledge(MEMBER_ID, Arrays.asList(
                new ShareAcknowledgementBatch(7, 8, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(11, 11, Collections.singletonList((byte) 2))));

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
    public void testLsoMovementAheadOfEndOffsetPostAcknowledgment() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2), Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 7), Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        // Acknowledge with RELEASE action.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(7, 8, Collections.singletonList((byte) 2))));

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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2), Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 7), Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

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

        MemoryRecords records1 = memoryRecords(5, 2);
        // Gap of 7-9.
        MemoryRecords records2 = memoryRecords(5, 10);
        // Gap of 15-19.
        MemoryRecords records3 = memoryRecords(5, 20);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, records1, Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, records2, Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, records3, Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

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

        MemoryRecords records1 = memoryRecords(5, 2);
        // Gap of 7-9.
        MemoryRecords records2 = memoryRecords(5, 10);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, records1, Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, records2, Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        // Acknowledge with RELEASE action.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(10, 14, Collections.singletonList((byte) 2))));

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
    public void testLsoMovementPostGapsInAcknowledgments() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(5, 10);
        // Gap from 15-17 offsets.
        recordsBuilder.appendWithOffset(18, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records1, Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records2, Optional.empty(),
                OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, Arrays.asList(
                new ShareAcknowledgementBatch(5, 6, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(10, 18, Arrays.asList(
                        (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 2, (byte) 0, (byte) 0, (byte) 0, (byte) 2
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
    public void testReleaseAcquiredRecordsBatchesPostStartOffsetMovement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire("member-2", MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 15),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 20),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 25),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 30),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 35),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // Acknowledge records.
        sharePartition.acknowledge(MEMBER_ID, Arrays.asList(
                new ShareAcknowledgementBatch(6, 7, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(8, 8, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(25, 29, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(35, 37, Collections.singletonList((byte) 2))
        ));

        // LSO is at 24.
        sharePartition.updateCacheAndOffsets(24);

        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(24, sharePartition.startOffset());
        assertEquals(39, sharePartition.endOffset());
        assertEquals(7, sharePartition.cachedState().size());

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
        expectedOffsetStateMap.put(24L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));

        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(20L).offsetState());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(25L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(25L).batchState());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(30L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(30L).batchState());

        expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(35L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(36L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(37L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(38L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(39L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(35L).offsetState());
    }

    @Test
    public void testReleaseAcquiredRecordsBatchesPostStartOffsetMovementToStartOfBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // LSO is at 10.
        sharePartition.updateCacheAndOffsets(10);

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        // Release acquired records.
        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(5L).batchState());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
    }

    @Test
    public void testReleaseAcquiredRecordsBatchesPostStartOffsetMovementToMiddleOfBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // LSO is at 11.
        sharePartition.updateCacheAndOffsets(11);

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        // Release acquired records.
        CompletableFuture<Void> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertNull(releaseResult.join());
        assertFalse(releaseResult.isCompletedExceptionally());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(5L).batchState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));

        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcquisitionLockTimeoutForBatchesPostStartOffsetMovement() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire("member-2", MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 15),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 20),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 25),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 30),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 35),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // Acknowledge records.
        sharePartition.acknowledge(MEMBER_ID, Arrays.asList(
                new ShareAcknowledgementBatch(6, 7, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(8, 8, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(25, 29, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(35, 37, Collections.singletonList((byte) 2))
        ));

        // LSO is at 24.
        sharePartition.updateCacheAndOffsets(24);

        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(24, sharePartition.startOffset());
        assertEquals(39, sharePartition.endOffset());
        assertEquals(7, sharePartition.cachedState().size());

        // Allowing acquisition lock to expire.
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
            () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(10L).batchState());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(15L).batchState());
    }

    @Test
    public void testAcquisitionLockTimeoutForBatchesPostStartOffsetMovementToStartOfBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // LSO is at 10.
        sharePartition.updateCacheAndOffsets(10);

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        // Allowing acquisition lock to expire.
        TestUtils.waitForCondition(
            () -> sharePartition.cachedState().get(5L).batchMemberId().equals(EMPTY_MEMBER_ID) &&
                    sharePartition.cachedState().get(5L).batchState() == RecordState.ARCHIVED &&
                    sharePartition.cachedState().get(10L).batchMemberId().equals(EMPTY_MEMBER_ID) &&
                    sharePartition.cachedState().get(10L).batchState() == RecordState.AVAILABLE,
            DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
            () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);
    }

    @Test
    public void testAcquisitionLockTimeoutForBatchesPostStartOffsetMovementToMiddleOfBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // LSO is at 11.
        sharePartition.updateCacheAndOffsets(11);

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(11, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        // Allowing acquisition lock to expire.
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
            () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);
    }

    @Test
    public void testScheduleAcquisitionLockTimeoutValueFromGroupConfig() {
        GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        GroupConfig groupConfig = Mockito.mock(GroupConfig.class);
        int expectedDurationMs = 500;
        Mockito.when(groupConfigManager.groupConfig(GROUP_ID)).thenReturn(Optional.of(groupConfig));
        Mockito.when(groupConfig.shareRecordLockDurationMs()).thenReturn(expectedDurationMs);

        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withGroupConfigManager(groupConfigManager).build();

        SharePartition.AcquisitionLockTimerTask timerTask = sharePartition.scheduleAcquisitionLockTimeout(MEMBER_ID, 100L, 200L);

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
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withGroupConfigManager(groupConfigManager).build();

        SharePartition.AcquisitionLockTimerTask timerTask1 = sharePartition.scheduleAcquisitionLockTimeout(MEMBER_ID, 100L, 200L);

        Mockito.verify(groupConfigManager, Mockito.times(2)).groupConfig(GROUP_ID);
        Mockito.verify(groupConfig).shareRecordLockDurationMs();
        assertEquals(expectedDurationMs1, timerTask1.delayMs);

        SharePartition.AcquisitionLockTimerTask timerTask2 = sharePartition.scheduleAcquisitionLockTimeout(MEMBER_ID, 100L, 200L);

        Mockito.verify(groupConfigManager, Mockito.times(4)).groupConfig(GROUP_ID);
        Mockito.verify(groupConfig, Mockito.times(2)).shareRecordLockDurationMs();
        assertEquals(expectedDurationMs2, timerTask2.delayMs);
    }

    @Test
    public void testAcknowledgeBatchAndOffsetPostLsoMovement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // LSO is at 12.
        sharePartition.updateCacheAndOffsets(12);
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(12, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        // Check cached state map.
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(2L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(2L).batchState());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());

        // Acknowledge with RELEASE action.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID, Arrays.asList(
                new ShareAcknowledgementBatch(2, 6, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(10, 14, Collections.singletonList((byte) 2))));

        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

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
    }

    @Test
    public void testAcknowledgeBatchPostLsoMovement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 20),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

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

        // Acknowledge with ACCEPT action.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(2, 14, Collections.singletonList((byte) 1))));
        assertNull(ackResult.join());
        assertFalse(ackResult.isCompletedExceptionally());

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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 2),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

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

        // Allowing acquisition lock to expire.
        TestUtils.waitForCondition(
                () -> sharePartition.nextFetchOffset() == 7 && sharePartition.cachedState().isEmpty() &&
                            sharePartition.startOffset() == 7 && sharePartition.endOffset() == 7,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());

        // Acknowledge with RELEASE action. This contains a batch that doesn't exist at all.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(2, 14, Collections.singletonList((byte) 2))));

        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).batchAcquisitionLockTimeoutTask());
    }

    @Test
    public void testLsoMovementThenAcquisitionLockTimeoutThenAcknowledgeBatchLastOffsetAheadOfStartOffsetBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withDefaultAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(2, 1),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // LSO is at 3.
        sharePartition.updateCacheAndOffsets(3);
        assertEquals(3, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.startOffset());
        assertEquals(3, sharePartition.endOffset());
        assertEquals(1, sharePartition.cachedState().size());

        // Check cached state map.
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(1L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(1L).batchState());
        assertNotNull(sharePartition.cachedState().get(1L).batchAcquisitionLockTimeoutTask());

        // Allowing acquisition lock to expire.
        TestUtils.waitForCondition(
                () -> sharePartition.nextFetchOffset() == 3 && sharePartition.cachedState().isEmpty() &&
                        sharePartition.startOffset() == 3 && sharePartition.endOffset() == 3,
                DEFAULT_MAX_WAIT_ACQUISITION_LOCK_TIMEOUT_MS,
                () -> ACQUISITION_LOCK_NEVER_GOT_RELEASED);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(2, 3),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(3, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertEquals(8, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.startOffset());
        assertEquals(7, sharePartition.endOffset());
        assertEquals(2, sharePartition.cachedState().size());

        // Acknowledge with RELEASE action. This contains a batch that doesn't exist at all.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(1, 7, Collections.singletonList((byte) 2))));

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
    }

    @Test
    public void testWriteShareGroupStateWithNullResponse() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Void> result = sharePartition.writeShareGroupState(Collections.emptyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalStateException.class);
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
        assertFutureThrows(result, IllegalStateException.class);
    }

    @Test
    public void testWriteShareGroupStateWithInvalidTopicsData() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        // TopicsData is empty.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.emptyList());
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        CompletableFuture<Void> writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(writeResult, IllegalStateException.class);

        // TopicsData contains more results than expected.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Arrays.asList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.emptyList()),
                new TopicData<>(Uuid.randomUuid(), Collections.emptyList())));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(writeResult, IllegalStateException.class);

        // TopicsData contains no partition data.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.emptyList())));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(writeResult, IllegalStateException.class);

        // TopicsData contains wrong topicId.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(Uuid.randomUuid(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(writeResult, IllegalStateException.class);

        // TopicsData contains more partition data than expected.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Arrays.asList(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message()),
                        PartitionFactory.newPartitionErrorData(1, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(writeResult, IllegalStateException.class);

        // TopicsData contains wrong partition.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(1, Errors.NONE.code(), Errors.NONE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        writeResult = sharePartition.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(writeResult, IllegalStateException.class);
    }

    @Test
    public void testWriteShareGroupStateWithWriteException() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition1 = SharePartitionBuilder.builder().withPersister(persister).build();

        Mockito.when(persister.writeState(Mockito.any())).thenReturn(FutureUtils.failedFuture(new RuntimeException("Write exception")));
        CompletableFuture<Void> writeResult = sharePartition1.writeShareGroupState(anyList());
        assertTrue(writeResult.isCompletedExceptionally());
        assertFutureThrows(writeResult, IllegalStateException.class);

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
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
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
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.NOT_COORDINATOR.code(), Errors.NOT_COORDINATOR.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        CompletableFuture<Void> result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, CoordinatorNotAvailableException.class);

        // Mock Write state RPC to return error response, COORDINATOR_NOT_AVAILABLE.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionErrorData(0, Errors.COORDINATOR_NOT_AVAILABLE.code(), Errors.COORDINATOR_NOT_AVAILABLE.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, CoordinatorNotAvailableException.class);

        // Mock Write state RPC to return error response, COORDINATOR_LOAD_IN_PROGRESS.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionErrorData(0, Errors.COORDINATOR_LOAD_IN_PROGRESS.code(), Errors.COORDINATOR_LOAD_IN_PROGRESS.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, CoordinatorNotAvailableException.class);

        // Mock Write state RPC to return error response, GROUP_ID_NOT_FOUND.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, GroupIdNotFoundException.class);

        // Mock Write state RPC to return error response, UNKNOWN_TOPIC_OR_PARTITION.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionErrorData(0, Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), Errors.UNKNOWN_TOPIC_OR_PARTITION.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, UnknownTopicOrPartitionException.class);

        // Mock Write state RPC to return error response, FENCED_STATE_EPOCH.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionErrorData(0, Errors.FENCED_STATE_EPOCH.code(), Errors.FENCED_STATE_EPOCH.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, FencedStateEpochException.class);

        // Mock Write state RPC to return error response, FENCED_LEADER_EPOCH.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionErrorData(0, Errors.FENCED_LEADER_EPOCH.code(), Errors.FENCED_LEADER_EPOCH.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, NotLeaderOrFollowerException.class);

        // Mock Write state RPC to return error response, UNKNOWN_SERVER_ERROR.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionErrorData(0, Errors.UNKNOWN_SERVER_ERROR.code(), Errors.UNKNOWN_SERVER_ERROR.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        result = sharePartition.writeShareGroupState(anyList());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, UnknownServerException.class);
    }

    @Test
    public void testWriteShareGroupStateWithNoOpShareStatePersister() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();
        List<PersisterStateBatch> stateBatches = Arrays.asList(
                new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3));

        CompletableFuture<Void> result = sharePartition.writeShareGroupState(stateBatches);
        assertNull(result.join());
        assertFalse(result.isCompletedExceptionally());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgementTypeAccept() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0,  memoryRecords(250, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                        new ShareAcknowledgementBatch(0, 249, Collections.singletonList((byte) 1))));

        assertEquals(250, sharePartition.nextFetchOffset());
        // The SPSO should only move when the initial records in cached state are acknowledged with type ACKNOWLEDGE or ARCHIVED.
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireRecords());
        // The records have been accepted, thus they are removed from the cached state.
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgementTypeReject() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(250, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(0, 249, Collections.singletonList((byte) 3))));

        assertEquals(250, sharePartition.nextFetchOffset());
        // The SPSO should only move when the initial records in cached state are acknowledged with type ACKNOWLEDGE or ARCHIVED.
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireRecords());
        // The records have been rejected, thus they are removed from the cached state.
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgementTypeRelease() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(250, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(0, 249, Collections.singletonList((byte) 2))));

        // The SPSO should only move when the initial records in cached state are acknowledged with type ACKNOWLEDGE or ARCHIVED.
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireRecords());
        // The records have been released, thus they are not removed from the cached state.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgementsFromBeginningForBatchSubset() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightMessages(20)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(15, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertTrue(sharePartition.canAcquireRecords());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(15, 15),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(0, 12, Collections.singletonList((byte) 1))));

        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(0L).offsetState().get(12L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(13L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(13, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgementsFromBeginningForEntireBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightMessages(20)
            .withState(SharePartitionState.ACTIVE)
            .build();
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(15, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertTrue(sharePartition.canAcquireRecords());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(15, 15),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(0, 14, Collections.singletonList((byte) 3))));

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAcknowledgementsInBetween() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightMessages(20)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(15, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertTrue(sharePartition.canAcquireRecords());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(15, 15),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(10, 14, Collections.singletonList((byte) 3))));

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(9L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).offsetState().get(10L).state());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        assertFalse(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void testMaybeUpdateCachedStateWhenAllRecordsInCachedStateAreAcknowledged() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightMessages(20)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(15, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertTrue(sharePartition.canAcquireRecords());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(15, 15),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertFalse(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(0, 29, Collections.singletonList((byte) 1))));

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(30, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void testMaybeUpdateCachedStateMultipleAcquisitionsAndAcknowledgements() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightMessages(100)
            .withState(SharePartitionState.ACTIVE)
            .build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(20, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertTrue(sharePartition.canAcquireRecords());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(20, 20),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertTrue(sharePartition.canAcquireRecords());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(20, 40),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertTrue(sharePartition.canAcquireRecords());

        // First Acknowledgement for the first batch of records 0-19.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(0, 19, Collections.singletonList((byte) 1))));

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(20, sharePartition.startOffset());
        assertEquals(59, sharePartition.endOffset());
        assertEquals(60, sharePartition.nextFetchOffset());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(20, 60),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertTrue(sharePartition.canAcquireRecords());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(20, 49, Collections.singletonList((byte) 1))));

        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(40L).offsetState().get(49L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(40L).offsetState().get(50L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(60L).batchState());
        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(50, sharePartition.startOffset());
        assertEquals(79, sharePartition.endOffset());
        assertEquals(80, sharePartition.nextFetchOffset());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(100, 80),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertFalse(sharePartition.canAcquireRecords());

        // Final Acknowledgment, all records are acknowledged here.
        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(50, 179, Collections.singletonList((byte) 3))));

        assertEquals(0, sharePartition.cachedState().size());
        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(180, sharePartition.startOffset());
        assertEquals(180, sharePartition.endOffset());
        assertEquals(180, sharePartition.nextFetchOffset());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(20, 180),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(180L).batchState());
        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(180, sharePartition.startOffset());
        assertEquals(199, sharePartition.endOffset());
        assertEquals(200, sharePartition.nextFetchOffset());
    }

    @Test
    public void testCanAcquireRecordsReturnsTrue() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(150, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());
    }

    @Test
    public void testCanAcquireRecordsChangeResponsePostAcknowledgement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(150, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertTrue(sharePartition.canAcquireRecords());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(100, 150),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertFalse(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(0, 249, Collections.singletonList((byte) 1))));

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
    }

    @Test
    public void testCanAcquireRecordsAfterReleaseAcknowledgement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(150, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(100, 150),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertFalse(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(0, 89, Collections.singletonList((byte) 2))));

        // The SPSO should only move when the initial records in cached state are acknowledged with type ACKNOWLEDGE or ARCHIVED.
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        // The records have been released, thus they are still available for being acquired.
        assertTrue(sharePartition.canAcquireRecords());
    }

    @Test
    public void testCanAcquireRecordsAfterArchiveAcknowledgement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(150, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(100, 150),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertFalse(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(0, 89, Collections.singletonList((byte) 3))));

        // The SPSO should only move when the initial records in cached state are acknowledged with type ACKNOWLEDGE or ARCHIVED.
        assertEquals(90, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireRecords());
    }

    @Test
    public void testCanAcquireRecordsAfterAcceptAcknowledgement() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(150, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertTrue(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(100, 150),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(sharePartition.canAcquireRecords());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                        new ShareAcknowledgementBatch(0, 89, Collections.singletonList((byte) 1))));

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
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), Errors.UNKNOWN_TOPIC_OR_PARTITION.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(10, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID,
                Collections.singletonList(new ShareAcknowledgementBatch(5, 14, Collections.singletonList((byte) 1))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, UnknownTopicOrPartitionException.class);

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
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code(), Errors.GROUP_ID_NOT_FOUND.message())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(6, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new ShareAcknowledgementBatch(8, 10, Collections.singletonList((byte) 3))));
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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, memoryRecords(7, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID,
                Collections.singletonList(new ShareAcknowledgementBatch(5, 7, Collections.singletonList((byte) 1))));

        // Acknowledge subset with another member.
        CompletableFuture<Void> ackResult = sharePartition.acknowledge("member-2",
                Collections.singletonList(new ShareAcknowledgementBatch(9, 11, Collections.singletonList((byte) 1))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRecordStateException.class);
    }

    @Test
    public void testAcknowledgeWithAnotherMemberRollbackBatchError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withState(SharePartitionState.ACTIVE).build();

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire("member-2", MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 15),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID, Arrays.asList(
                new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 2)),
                // Acknowledging batch with another member will cause failure and rollback.
                new ShareAcknowledgementBatch(10, 14, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(15, 19, Collections.singletonList((byte) 1))));

        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRecordStateException.class);

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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire("member-2", MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 15),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        CompletableFuture<Void> ackResult = sharePartition.acknowledge(MEMBER_ID, Arrays.asList(
                new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(10, 14, Collections.singletonList((byte) 1)),
                // Acknowledging subset with another member will cause failure and rollback.
                new ShareAcknowledgementBatch(16, 18, Collections.singletonList((byte) 1))));
        assertTrue(ackResult.isCompletedExceptionally());
        assertFutureThrows(ackResult, InvalidRecordStateException.class);

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
        MemoryRecords records = memoryRecords(10, 5);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(5, 14, Collections.singletonList((byte) 2))));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(5, 14, Collections.singletonList((byte) 2))));

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
        MemoryRecords records1 = memoryRecords(5, 10);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(5, 15);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, new ArrayList<>(Arrays.asList(
                new ShareAcknowledgementBatch(10, 12, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(13, 16, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(17, 19, Collections.singletonList((byte) 1)))));

        // Send next batch from offset 13, only 2 records should be acquired.
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        // Send next batch from offset 15, only 2 records should be acquired.
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(13, 16, Collections.singletonList((byte) 2))));

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
        MemoryRecords records1 = memoryRecords(5, 0);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, new ArrayList<>(Collections.singletonList(
                new ShareAcknowledgementBatch(0, 1, Collections.singletonList((byte) 2)))));

        // Send next batch from offset 0, only 2 records should be acquired.
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 40, 3, memoryRecords(2, 0),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(0, 4, Collections.singletonList((byte) 2))));

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
        MemoryRecords records1 = memoryRecords(10, 0);
        String memberId1 = "memberId-1";
        String memberId2 = "memberId-2";

        sharePartition.acquire(memberId1, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertFalse(sharePartition.findNextFetchOffset());
        assertEquals(10, sharePartition.nextFetchOffset());

        sharePartition.acquire(memberId2, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(10, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertFalse(sharePartition.findNextFetchOffset());
        assertEquals(20, sharePartition.nextFetchOffset());

        sharePartition.acknowledge(memberId1, Collections.singletonList(
                new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 2))));

        assertTrue(sharePartition.findNextFetchOffset());
        assertEquals(5, sharePartition.nextFetchOffset());

        sharePartition.acquire(memberId1, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        assertTrue(sharePartition.findNextFetchOffset());
        assertEquals(20, sharePartition.nextFetchOffset());
        assertFalse(sharePartition.findNextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWithMultipleConsumers() {
        SharePartition sharePartition = SharePartitionBuilder.builder()
            .withMaxInflightMessages(100)
            .withState(SharePartitionState.ACTIVE)
            .build();
        MemoryRecords records1 = memoryRecords(3, 0);
        String memberId1 = MEMBER_ID;
        String memberId2 = "member-2";

        sharePartition.acquire(memberId1, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertEquals(3, sharePartition.nextFetchOffset());

        sharePartition.acknowledge(memberId1, Collections.singletonList(
                new ShareAcknowledgementBatch(0, 2, Collections.singletonList((byte) 2))));
        assertEquals(0, sharePartition.nextFetchOffset());

        sharePartition.acquire(memberId2, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(2, 3),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertEquals(0, sharePartition.nextFetchOffset());

        sharePartition.acquire(memberId1, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertEquals(5, sharePartition.nextFetchOffset());

        sharePartition.acknowledge(memberId2, Collections.singletonList(
                new ShareAcknowledgementBatch(3, 4, Collections.singletonList((byte) 2))));
        assertEquals(3, sharePartition.nextFetchOffset());
    }

    @Test
    public void testNumberOfWriteCallsOnUpdates() {
        SharePartition sharePartition = Mockito.spy(SharePartitionBuilder.builder()
            .withState(SharePartitionState.ACTIVE)
            .build());

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 10, 0, memoryRecords(5, 2),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, Collections.singletonList(
                new ShareAcknowledgementBatch(2, 6, Collections.singletonList((byte) 1))));
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

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, memoryRecords(12, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        sharePartition.acknowledge(MEMBER_ID, Arrays.asList(
                new ShareAcknowledgementBatch(5, 11, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(12, 13, Collections.singletonList((byte) 0)),
                new ShareAcknowledgementBatch(14, 15, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(17, 20, Collections.singletonList((byte) 2))));

        // Reacquire with another member.
        sharePartition.acquire("member-2", MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        assertEquals(10, sharePartition.nextFetchOffset());

        // Reacquire with another member.
        sharePartition.acquire("member-2", MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 30, 0, memoryRecords(7, 10),
                Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
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
        for (int i = 0; i < 500; i++) {
            stateBatches.add(new PersisterStateBatch(234L + i, 234L + i, RecordState.ACKNOWLEDGED.id, (short) 1));
        }
        stateBatches.add(new PersisterStateBatch(232L, 232L, RecordState.ARCHIVED.id, (short) 1));

        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionAllData(0, 3, 232L, Errors.NONE.code(), Errors.NONE.message(),
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
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())))));

        CompletableFuture<WriteShareGroupStateResult> future = new CompletableFuture<>();
        // persister.writeState RPC will not complete instantaneously due to which commit won't happen for acknowledged offsets.
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(future);

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 0),
            Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false));

        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(5, 5),
            Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false));

        List<ShareAcknowledgementBatch> acknowledgementBatches = new ArrayList<>();
        acknowledgementBatches.add(new ShareAcknowledgementBatch(2, 3, Collections.singletonList((byte) 2)));
        acknowledgementBatches.add(new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 2)));
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
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(15, 0),
            Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false));

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
        sharePartition.acquire(MEMBER_ID, MAX_FETCH_RECORDS, new FetchPartitionData(Errors.NONE, 20, 0, memoryRecords(15, 0),
            Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false));
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(0L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(2L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(3L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(4L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
    }

    private List<AcquiredRecords> fetchAcquiredRecords(ShareAcquiredRecords shareAcquiredRecords, int expectedOffsetCount) {
        assertNotNull(shareAcquiredRecords);
        assertEquals(expectedOffsetCount, shareAcquiredRecords.count());
        return shareAcquiredRecords.acquiredRecords();
    }

    private MemoryRecords memoryRecords(int numOfRecords) {
        return memoryRecords(numOfRecords, 0);
    }

    private MemoryRecords memoryRecords(int numOfRecords, long startOffset) {
        return memoryRecordsBuilder(numOfRecords, startOffset).build();
    }

    private MemoryRecordsBuilder memoryRecordsBuilder(int numOfRecords, long startOffset) {
        return memoryRecordsBuilder(ByteBuffer.allocate(1024), numOfRecords, startOffset);
    }

    private MemoryRecordsBuilder memoryRecordsBuilder(ByteBuffer buffer, int numOfRecords, long startOffset) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.NONE,
            TimestampType.CREATE_TIME, startOffset, 2);
        for (int i = 0; i < numOfRecords; i++) {
            builder.appendWithOffset(startOffset + i, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        }
        return builder;
    }

    private List<AcquiredRecords> expectedAcquiredRecord(long baseOffset, long lastOffset, int deliveryCount) {
        return Collections.singletonList(new AcquiredRecords()
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
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionAllData(0, 0, 0L, Errors.NONE.code(), Errors.NONE.message(),
                                Collections.emptyList())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
    }

    private static class SharePartitionBuilder {

        private int defaultAcquisitionLockTimeoutMs = 30000;
        private int maxDeliveryCount = MAX_DELIVERY_COUNT;
        private int maxInflightMessages = MAX_IN_FLIGHT_MESSAGES;

        private Persister persister = new NoOpShareStatePersister();
        private ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
        private GroupConfigManager groupConfigManager = Mockito.mock(GroupConfigManager.class);
        private SharePartitionState state = SharePartitionState.EMPTY;

        private SharePartitionBuilder withMaxInflightMessages(int maxInflightMessages) {
            this.maxInflightMessages = maxInflightMessages;
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

        public static SharePartitionBuilder builder() {
            return new SharePartitionBuilder();
        }

        public SharePartition build() {
            return new SharePartition(GROUP_ID, TOPIC_ID_PARTITION, 0, maxInflightMessages, maxDeliveryCount,
                    defaultAcquisitionLockTimeoutMs, mockTimer, MOCK_TIME, persister, replicaManager, groupConfigManager,
                    state, Mockito.mock(SharePartitionListener.class));
        }
    }
}
