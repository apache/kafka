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
package kafka.server;

import kafka.server.SharePartition.AcknowledgementBatch;
import kafka.server.SharePartition.InFlightState;
import kafka.server.SharePartition.RecordState;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static kafka.server.SharePartitionManagerTest.RECORD_LOCK_DURATION_MS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SharePartitionTest {

    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "member-1";
    private static final int MAX_DELIVERY_COUNT = 5;
    private static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
    private static Timer mockTimer;
    private static final Time MOCK_TIME = new MockTime();
    private static final int ACQUISITION_LOCK_TIMEOUT_MS = 100;
    private static final short MAX_IN_FLIGHT_MESSAGES = 200;

    @BeforeEach
    public void setUp() {
        mockTimer = new SystemTimerReaper("share-group-lock-timeout-test-reaper",
                new SystemTimer("share-group-lock-test-timeout"));
    }

    @AfterEach
    public void tearDown() throws Exception {
        mockTimer.close();
    }

    private SharePartition mockSharePartition() {
        return mockSharePartition(RECORD_LOCK_DURATION_MS, MAX_IN_FLIGHT_MESSAGES);
    }

    private SharePartition mockSharePartition(int acquisitionLockTimeoutMs) {
        return mockSharePartition(acquisitionLockTimeoutMs, MAX_IN_FLIGHT_MESSAGES);
    }

    private SharePartition mockSharePartition(short recordLockPartitionLimit) {
        return mockSharePartition(RECORD_LOCK_DURATION_MS, recordLockPartitionLimit);
    }

    private SharePartition mockSharePartition(int acquisitionLockTimeoutMs, short recordLockPartitionLimit) {
        return new SharePartition(GROUP_ID, TOPIC_ID_PARTITION, MAX_DELIVERY_COUNT,
                recordLockPartitionLimit, acquisitionLockTimeoutMs, mockTimer, MOCK_TIME);
    }

    private MemoryRecords memoryRecords(int numOfRecords) {
        return memoryRecords(numOfRecords, 0);
    }

    private MemoryRecords memoryRecords(int numOfRecords, long startOffset) {
        return memoryRecordsBuilder(numOfRecords, startOffset).build();
    }

    private MemoryRecordsBuilder memoryRecordsBuilder(int numOfRecords, long startOffset) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024),
                CompressionType.NONE, TimestampType.CREATE_TIME, startOffset);
        for (int i = 0; i < numOfRecords; i++) {
            builder.appendWithOffset(startOffset + i, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        }
        return builder;
    }

    private List<AcquiredRecords> expectedAcquiredRecords(MemoryRecords memoryRecords, int deliveryCount) {
        List<AcquiredRecords> acquiredRecordsList = new ArrayList<>();
        memoryRecords.batches().forEach(batch -> acquiredRecordsList.add(new AcquiredRecords()
                .setBaseOffset(batch.baseOffset())
                .setLastOffset(batch.lastOffset())
                .setDeliveryCount((short) deliveryCount)));
        return acquiredRecordsList;
    }

    private List<AcquiredRecords> expectedAcquiredRecords(long baseOffset, long lastOffset, int deliveryCount) {
        List<AcquiredRecords> acquiredRecordsList = new ArrayList<>();
        for (long i = baseOffset; i <= lastOffset; i++) {
            acquiredRecordsList.add(new AcquiredRecords()
                    .setBaseOffset(i)
                    .setLastOffset(i)
                    .setDeliveryCount((short) deliveryCount));
        }
        return acquiredRecordsList;
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
        assertEquals(RecordState.ARCHIVED, RecordState.forId((byte) 3));
        // Invalid check.
        assertThrows(IllegalArgumentException.class, () -> RecordState.forId((byte) 4));
    }

    @Test
    public void testAcquireSingleRecord() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records = memoryRecords(1);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 3, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(1, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).baseOffset());
        assertEquals(0, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).gapOffsets());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testAcquireMultipleRecords() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).baseOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).gapOffsets());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcquireMultipleRecordsWithOverlapAndNewBatch() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records = memoryRecords(5, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());

        // Add records from 0-9 offsets, 5-9 should be acquired and 0-4 should be ignored.
        records = memoryRecords(10, 0);
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(memoryRecords(5, 5), 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
    }

    @Test
    public void testAcquireSameBatchAgain() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        // No records should be returned as the batch is already acquired.
        assertEquals(0, result.join().size());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Send subset of the same batch again, no records should be returned.
        MemoryRecords subsetRecords = memoryRecords(2, 10);
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, subsetRecords,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        // No records should be returned as the batch is already acquired.
        assertEquals(0, result.join().size());
        assertEquals(15, sharePartition.nextFetchOffset());
        // Cache shouldn't be tracking per offset records
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcquireWithEmptyFetchRecords() {
        SharePartition sharePartition = mockSharePartition();
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, MemoryRecords.EMPTY,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        assertEquals(0, result.join().size());
        assertEquals(0, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcknowledgeSingleRecordBatch() {
        SharePartition sharePartition = mockSharePartition();

        MemoryRecords records1 = memoryRecords(1, 0);
        MemoryRecords records2 = memoryRecords(1, 1);

        // Another batch is acquired because if there is only 1 batch, and it is acknowledged, the batch will be removed from cachedState
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 10, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

       result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 10, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(1, 1, null, AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(2, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(1L).batchState());
        assertEquals(1, sharePartition.cachedState().get(1L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(1L).gapOffsets());
        assertNull(sharePartition.cachedState().get(1L).offsetState());
    }

    @Test
    public void testAcknowledgeMultipleRecordBatch() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records = memoryRecords(10, 5);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(5, 14, null, AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testAcknowledgeMultipleRecordBatchWithGapOffsets() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(5, 10);
        // Gap from 15-17 offsets.
        recordsBuilder.appendWithOffset(18, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(19, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(5, 18, Arrays.asList(15L, 16L, 17L), AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).gapOffsets());
        assertNotNull(sharePartition.cachedState().get(10L).gapOffsets());
        assertEquals(new HashSet<>(Arrays.asList(15L, 16L, 17L)), sharePartition.cachedState().get(10L).gapOffsets());
    }

    @Test
    public void testAcknowledgeMultipleSubsetRecordBatchWithGapOffsets() {
        SharePartition sharePartition = mockSharePartition();
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

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Acknowledging over subset of both batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(6, 18, Arrays.asList(12L, 13L, 15L, 17L, 19L), AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(21, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        assertNull(sharePartition.cachedState().get(5L).gapOffsets());
        assertNotNull(sharePartition.cachedState().get(10L).gapOffsets());
        // Gap offset 19 will be avoided as it's greater than the batch last offset.
        assertEquals(new HashSet<>(Arrays.asList(12L, 13L, 15L, 17L)), sharePartition.cachedState().get(10L).gapOffsets());
    }

    @Test
    public void testAcknowledgeOutOfRangeCachedData() {
        SharePartition sharePartition = mockSharePartition();
        // Acknowledge a batch when cache is empty.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(0, 15, null, AcknowledgeType.REJECT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(20, 25, null, AcknowledgeType.REJECT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRequestException.class, ackResult.join().get().getClass());
    }

    @Test
    public void testAcknowledgeWithAnotherMember() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            "member-2",
            Collections.singletonList(new AcknowledgementBatch(5, 9, null, AcknowledgeType.REJECT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
    }

    @Test
    public void testAcknowledgeWhenOffsetNotAcquired() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(5, 9, null, AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        // Acknowledge the same batch again but with ACCEPT type.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(5, 9, null, AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

        // Re-acquire the same batch and then acknowledge subset with ACCEPT type.
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(6, 8, null, AcknowledgeType.REJECT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        // Re-acknowledge the subset batch with REJECT type.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(6, 8, null, AcknowledgeType.REJECT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
    }

    @Test
    public void testAcknowledgeRollbackWithFullBatchError() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(5, 10);
        MemoryRecords records3 = memoryRecords(5, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-19 should exist.
        assertEquals(3, sharePartition.cachedState().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Arrays.asList(
                new AcknowledgementBatch(5, 9, null, AcknowledgeType.RELEASE),
                new AcknowledgementBatch(10, 14, null, AcknowledgeType.ACCEPT),
                new AcknowledgementBatch(15, 19, null, AcknowledgeType.ACCEPT),
                // Add another batch which should fail the request.
                new AcknowledgementBatch(15, 19, null, AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

        // Check the state of the cache. The state should be acquired itself.
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
    }

    @Test
    public void testAcknowledgeRollbackWithSubsetError() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(5, 10);
        MemoryRecords records3 = memoryRecords(5, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-19 should exist.
        assertEquals(3, sharePartition.cachedState().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Arrays.asList(
                new AcknowledgementBatch(5, 9, null, AcknowledgeType.RELEASE),
                new AcknowledgementBatch(10, 14, null, AcknowledgeType.ACCEPT),
                new AcknowledgementBatch(15, 19, null, AcknowledgeType.ACCEPT),
                // Add another batch which should fail the request.
                new AcknowledgementBatch(16, 19, null, AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

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
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(12, 13, null, AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).gapOffsets());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // Send the same fetch request batch again but only 2 offsets should come as acquired.
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(12, 13, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcquireReleasedRecordMultipleBatches() {
        SharePartition sharePartition = mockSharePartition();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(5, 10);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(5, 15);
        // Third fetch request with 5 records starting from offset 23, gap of 3 offsets.
        MemoryRecords records3 = memoryRecords(5, 23);
        // Fourth fetch request with 5 records starting from offset 28.
        MemoryRecords records4 = memoryRecords(5, 28);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 3, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records3, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(28, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 3, records4,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
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

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(12, 30, null, AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(4, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertNull(sharePartition.cachedState().get(15L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(23L).batchState());
        assertNull(sharePartition.cachedState().get(23L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(28L).batchState());
        assertNotNull(sharePartition.cachedState().get(28L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(28L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(29L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(30L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(31L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(32L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(28L).offsetState());

        // Send next batch from offset 12, only 3 records should be acquired.
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(12, 14, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Though record2 batch exists to acquire but send batch record3, it should be acquired but
        // next fetch offset should not move.
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 40, 3, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records3, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Acquire partial records from batch 2.
        MemoryRecords subsetRecords = memoryRecords(2, 17);
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, subsetRecords,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(17, 18, 2).toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(15, sharePartition.nextFetchOffset());

        // Acquire partial records from record 4 to further test if the next fetch offset move
        // accordingly once complete record 2 is also acquired.
        subsetRecords = memoryRecords(1, 28);
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, subsetRecords,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(28, 28, 2).toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(15, sharePartition.nextFetchOffset());

        // Try to acquire complete record 2 though it's already partially acquired, the next fetch
        // offset should move.
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        // Offset 15,16 and 19 should be acquired.
        List<AcquiredRecords> expectedAcquiredRecords = expectedAcquiredRecords(15, 16, 2);
        expectedAcquiredRecords.addAll(expectedAcquiredRecords(19, 19, 2));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(29, sharePartition.nextFetchOffset());
    }

    @Test
    public void testReleaseSingleRecordBatch() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records = memoryRecords(1, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 10, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).gapOffsets());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testReleaseMultipleRecordBatch() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records = memoryRecords(10, 5);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
    }

    @Test
    public void testReleaseMultipleAcknowledgedRecordBatch() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records0 = memoryRecords(5, 0);
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(5, 10);
        // Gap from 15-17 offsets.
        recordsBuilder.appendWithOffset(18, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records0,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records0, 1).toArray(), result.join().toArray());
        assertEquals(5, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(19, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 18, Arrays.asList(15L, 16L, 17L), AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(19, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).gapOffsets());
        assertNotNull(sharePartition.cachedState().get(10L).gapOffsets());
        assertEquals(new HashSet<>(Arrays.asList(15L, 16L, 17L)), sharePartition.cachedState().get(10L).gapOffsets());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).gapOffsets());
        assertNotNull(sharePartition.cachedState().get(10L).gapOffsets());
        assertEquals(new HashSet<>(Arrays.asList(15L, 16L, 17L)), sharePartition.cachedState().get(10L).gapOffsets());
    }

    @Test
    public void testReleaseAcknowledgedMultipleSubsetRecordBatch() {
        SharePartition sharePartition = mockSharePartition();
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

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Acknowledging over subset of both batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(6, 18, Arrays.asList(12L, 13L, 15L, 17L, 19L), AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(21, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testReleaseAcquiredRecordsWithAnotherMember() {
        SharePartition sharePartition = mockSharePartition();
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

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-2",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(6, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Acknowledging over subset of second batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(10, 18, Arrays.asList(12L, 13L, 15L, 17L, 19L), AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Release acquired records for "member-1".
        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(19, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // Release acquired records for "member-2".
        releaseResult = sharePartition.releaseAcquiredRecords("member-2");
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(5L).batchMemberId());
        // Check cached state.
        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testReleaseAcquiredRecordsWithAnotherMemberAndSubsetAcknowledged() {
        SharePartition sharePartition = mockSharePartition();
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

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-2",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Acknowledging over subset of second batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(10, 18, Arrays.asList(12L, 13L, 15L, 17L, 19L), AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Release acquired records for "member-1".
        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(19, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // Ack subset of records by "member-2".
        ackResult = sharePartition.acknowledge(
                "member-2",
                Collections.singletonList(new AcknowledgementBatch(5, 5, null, AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(19, sharePartition.nextFetchOffset());

        // Release acquired records for "member-2".
        releaseResult = sharePartition.releaseAcquiredRecords("member-2");
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(6, sharePartition.nextFetchOffset());

        // Check cached state.
        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, "member-2"));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, "member-2"));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testReleaseAcquiredRecordsForEmptyCachedData() {
        SharePartition sharePartition = mockSharePartition();
        // Release a batch when cache is empty.
        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testReleaseAcquiredRecordsAfterDifferentAcknowledges() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 6, null, AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(5, sharePartition.nextFetchOffset());

        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(8, 9, null, AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());
        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
    }

    @Test
    public void testMaxDeliveryCountLimitExceeded() {
        int maxDeliveryCount = 2;
        SharePartition sharePartition = new SharePartition(
                GROUP_ID,
                TOPIC_ID_PARTITION,
                maxDeliveryCount,
                MAX_IN_FLIGHT_MESSAGES,
                RECORD_LOCK_DURATION_MS,
                mockTimer,
                MOCK_TIME
        );
        MemoryRecords records = memoryRecords(10, 5);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 14, null, AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).gapOffsets());
        assertNull(sharePartition.cachedState().get(5L).offsetState());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 14, null, AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(15, sharePartition.endOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubset() {
        int maxDeliveryCount = 2;
        SharePartition sharePartition = new SharePartition(
                GROUP_ID, TOPIC_ID_PARTITION,
                maxDeliveryCount,
                MAX_IN_FLIGHT_MESSAGES,
                RECORD_LOCK_DURATION_MS,
                mockTimer,
                MOCK_TIME
        );
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(5, 10);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(5, 15);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(15L).offsetState());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                new ArrayList<>(Arrays.asList(
                        new AcknowledgementBatch(10, 12, null, AcknowledgeType.ACCEPT),
                        new AcknowledgementBatch(13, 16, null, AcknowledgeType.RELEASE),
                        new AcknowledgementBatch(17, 19, null, AcknowledgeType.ACCEPT)
                )));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(13, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(15L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(15L).offsetState());

        // Send next batch from offset 13, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(13, 14, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Send next batch from offset 15, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(15, 16, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        assertEquals(20, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(15L).offsetState());

        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(
                        new AcknowledgementBatch(13, 16, null, AcknowledgeType.RELEASE)
                ));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(20, sharePartition.nextFetchOffset());
        // cachedPartition will be empty because after the second release, the acquired records will now have moved to
        // ARCHIVE state (maxDeliveryCountExceeded). Also, now since all the records are either in ACKNOWLEDGED or ARCHIVED
        // state, cachedState should be empty.
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetWhileOthersAreAcquiredAgain() {
        int maxDeliveryCount = 2;
        SharePartition sharePartition = new SharePartition(
                GROUP_ID,
                TOPIC_ID_PARTITION,
                maxDeliveryCount,
                MAX_IN_FLIGHT_MESSAGES,
                RECORD_LOCK_DURATION_MS,
                mockTimer,
                MOCK_TIME
        );
        // First fetch request with 5 records starting from offset 0.
        MemoryRecords records1 = memoryRecords(5, 0);
        MemoryRecords recordsSubset = memoryRecords(2, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertNull(sharePartition.cachedState().get(0L).offsetState());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                new ArrayList<>(Arrays.asList(
                        new AcknowledgementBatch(0, 1, null, AcknowledgeType.RELEASE)
                )));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchState());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(0L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(1L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(2L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(3L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(4L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(0L).offsetState());

        // Send next batch from offset 0, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, recordsSubset,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(0, 1, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(0L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(1L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(2L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(3L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(4L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(0L).offsetState());

        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(
                        new AcknowledgementBatch(0, 4, null, AcknowledgeType.RELEASE)
                ));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(2, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchState());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(0L, new InFlightState(RecordState.ARCHIVED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(1L, new InFlightState(RecordState.ARCHIVED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(2L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(3L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(4L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetAfterReleaseAcquiredRecords() {
        int maxDeliveryCount = 2;
        SharePartition sharePartition = new SharePartition(
                GROUP_ID,
                TOPIC_ID_PARTITION,
                maxDeliveryCount,
                MAX_IN_FLIGHT_MESSAGES,
                RECORD_LOCK_DURATION_MS,
                mockTimer,
                MOCK_TIME
        );
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(
                        new AcknowledgementBatch(10, 14, null, AcknowledgeType.RELEASE)
                ));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetAfterReleaseAcquiredRecordsSubset() {
        int maxDeliveryCount = 2;
        SharePartition sharePartition = new SharePartition(
                GROUP_ID,
                TOPIC_ID_PARTITION,
                maxDeliveryCount,
                MAX_IN_FLIGHT_MESSAGES,
                RECORD_LOCK_DURATION_MS,
                mockTimer,
                MOCK_TIME
        );
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(5, 10);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(5, 15);
        // third fetch request with 5 records starting from offset20.
        MemoryRecords records3 = memoryRecords(5, 20);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 50, 3, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records3, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(25, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(15L).offsetState());
        assertNull(sharePartition.cachedState().get(20L).offsetState());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                new ArrayList<>(Arrays.asList(
                        new AcknowledgementBatch(10, 12, null, AcknowledgeType.ACCEPT),
                        new AcknowledgementBatch(13, 16, null, AcknowledgeType.RELEASE),
                        new AcknowledgementBatch(17, 19, null, AcknowledgeType.REJECT),
                        new AcknowledgementBatch(20, 24, null, AcknowledgeType.RELEASE)
                )));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(13, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(15L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(20L).batchState());
        assertNull(sharePartition.cachedState().get(20L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(15L).offsetState());

        // Send next batch from offset 13, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(13, 14, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Send next batch from offset 15, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(15, 16, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records3, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(25, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(15L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertNull(sharePartition.cachedState().get(20L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ARCHIVED, (short) 2, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ARCHIVED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(15L).offsetState());
    }

    @Test
    public void testAcquisitionLockForAcquiringSingleRecord() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
        MemoryRecords records = memoryRecords(1);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 3, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(1, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).baseOffset());
        assertEquals(0, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).gapOffsets());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNotNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).baseOffset());
        assertEquals(0, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).gapOffsets());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockForAcquiringMultipleRecords() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
        MemoryRecords records = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).baseOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).gapOffsets());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).baseOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).gapOffsets());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(0, sharePartition.timer().size());
        assertNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
    }

    @Test
    public void testAcquisitionLockForAcquiringMultipleRecordsWithOverlapAndNewBatch() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
        MemoryRecords records = memoryRecords(5, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Add records from 0-9 offsets, 5-9 should be acquired and 0-4 should be ignored.
        records = memoryRecords(10, 0);
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(memoryRecords(5, 5), 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockForAcquiringSameBatchAgain() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
        MemoryRecords records = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertEquals(0, sharePartition.timer().size());

        // Acquire the same batch again.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingSingleRecordBatch() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
        MemoryRecords records = memoryRecords(1, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 10, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertNotNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(0, 0, null, AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).gapOffsets());
        assertNull(sharePartition.cachedState().get(0L).offsetState());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).gapOffsets());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleRecordBatch() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
        MemoryRecords records = memoryRecords(10, 5);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 14, null, AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).gapOffsets());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).gapOffsets());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleRecordBatchWithGapOffsets() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(5, 10);
        // Gap from 15-17 offsets.
        recordsBuilder.appendWithOffset(18, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();
        MemoryRecords records3 = memoryRecords(2, 1);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records3, 1).toArray(), result.join().toArray());
        assertEquals(3, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(1L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(19, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 18, Arrays.asList(15L, 16L, 17L), AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(19, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());

        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(1L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(1, sharePartition.nextFetchOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(1L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());

        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(1L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockForAcquiringSubsetBatchAgain() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
        MemoryRecords records = memoryRecords(8, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(18, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Acquire subset of records again.
        MemoryRecords records2 = memoryRecords(3, 12);
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(3, result.join().size());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(10L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(11L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).offsetState().get(12L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).offsetState().get(13L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).offsetState().get(14L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(15L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(16L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(17L).state());

        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(10, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(10L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(11L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(12L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(13L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(14L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(15L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(16L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(17L).state());

        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleSubsetRecordBatchWithGapOffsets() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
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

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Acknowledging over subset of both batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(6, 18, Arrays.asList(12L, 13L, 15L, 17L, 19L), AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(21, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

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

        assertNull(sharePartition.cachedState().get(5L).gapOffsets());
        assertNotNull(sharePartition.cachedState().get(10L).gapOffsets());
        // Gap offset 19 will be avoided as it's greater than the batch last offset.
        assertEquals(new HashSet<>(Arrays.asList(12L, 13L, 15L, 17L)), sharePartition.cachedState().get(10L).gapOffsets());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

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

        // Gap offset 19 will be avoided as it's greater than the batch last offset.
        assertEquals(new HashSet<>(Arrays.asList(12L, 13L, 15L, 17L)), sharePartition.cachedState().get(10L).gapOffsets());
    }

    @Test
    public void testAcknowledgeAfterAcquisitionLockTimeout() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Acknowledge with ACCEPT type.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 9, null, AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Try acknowledging with REJECT type.
        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 9, null, AcknowledgeType.REJECT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnReleasingMultipleRecordBatch() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
        MemoryRecords records = memoryRecords(10, 5);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnReleasingAcknowledgedMultipleSubsetRecordBatchWithGapOffsets() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
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

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Acknowledging over subset of both batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(6, 18, Arrays.asList(12L, 13L, 15L, 17L, 19L), AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(21, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        assertNull(sharePartition.cachedState().get(5L).gapOffsets());
        assertNotNull(sharePartition.cachedState().get(10L).gapOffsets());
        // Gap offset 19 will be avoided as it's greater than the batch last offset.
        assertEquals(new HashSet<>(Arrays.asList(12L, 13L, 15L, 17L)), sharePartition.cachedState().get(10L).gapOffsets());


        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap1 = new HashMap<>();
        expectedOffsetStateMap1.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap1.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap1, sharePartition.cachedState().get(5L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap2 = new HashMap<>();
        expectedOffsetStateMap2.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap2.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap2.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap2.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap2.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap2.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap2.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap2.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap2.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap2.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap2.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap2, sharePartition.cachedState().get(10L).offsetState());

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

        // Gap offset 19 will be avoided as it's greater than the batch last offset.
        assertEquals(new HashSet<>(Arrays.asList(12L, 13L, 15L, 17L)), sharePartition.cachedState().get(10L).gapOffsets());

        // Allowing acquisition lock to expire. This won't change the state since the batches have been released.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        assertEquals(expectedOffsetStateMap1, sharePartition.cachedState().get(5L).offsetState());
        assertEquals(expectedOffsetStateMap2, sharePartition.cachedState().get(10L).offsetState());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockAfterDifferentAcknowledges() throws InterruptedException {
        SharePartition sharePartition = mockSharePartition(ACQUISITION_LOCK_TIMEOUT_MS);
        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 6, null, AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(5, sharePartition.nextFetchOffset());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());

        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(8, 9, null, AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        //Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcknowledgeSubsetWithAnotherMember() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(7, 5);

        sharePartition.acquire("member-1",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge("member-1",
                Collections.singletonList(new AcknowledgementBatch(5, 7, Collections.emptyList(), AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(12, sharePartition.nextFetchOffset());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchMemberId());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        // Acknowledge subset with another member.
        ackResult = sharePartition.acknowledge("member-2",
                Collections.singletonList(new AcknowledgementBatch(9, 11, Collections.emptyList(), AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
    }

    @Test
    public void testReleaseAcquiredRecordsSubsetWithAnotherMember() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(7, 5);

        sharePartition.acquire("member-1",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge("member-1",
                Collections.singletonList(new AcknowledgementBatch(5, 7, Collections.emptyList(), AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(12, sharePartition.nextFetchOffset());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchMemberId());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        // Release acquired records subset with another member.
        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords("member-2");
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());
        // No change in the offset state map since the member id is different.
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
    }

    @Test
    public void testReacquireSubsetWithAnotherMember() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(12, 10);

        sharePartition.acquire("member-1",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        sharePartition.acquire("member-1",
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(22, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge("member-1",
                Collections.singletonList(new AcknowledgementBatch(5, 15, Arrays.asList(12L, 13L), AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(5, sharePartition.nextFetchOffset());

        // Records 17-20 are released in the acknowledgement
        ackResult = sharePartition.acknowledge("member-1",
                Collections.singletonList(new AcknowledgementBatch(17, 20, Collections.emptyList(), AcknowledgeType.RELEASE)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(5, sharePartition.nextFetchOffset());

        // Reacquire with another member.
        sharePartition.acquire("member-2",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(10, sharePartition.nextFetchOffset());

        // Reacquire with another member.
        MemoryRecords records3 = memoryRecords(7, 10);
        sharePartition.acquire("member-2",
                new FetchPartitionData(Errors.NONE, 30, 0, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(17, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(2, sharePartition.cachedState().get(5L).batchDeliveryCount());

        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchMemberId());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        // Records 10-11, 14-15 were reacquired by member-2.
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        // Records 12-13 were kept as gapOffsets, hence they are not reacquired and are kept in ARCHIVED state.
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        // Record 16 was not released in the acknowledgements. It was included in the reacquire by member-2,
        // still its ownership is with member-1 and delivery count is 1.
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.AVAILABLE, (short) 1, "member-1"));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.AVAILABLE, (short) 1, "member-1"));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, "member-1"));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, "member-1"));
        expectedOffsetStateMap.put(21L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcknowledgeWithAnotherMemberRollbackBatchError() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(5, 10);
        MemoryRecords records3 = memoryRecords(5, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        result = sharePartition.acquire(
                "member-2",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Arrays.asList(
                        new AcknowledgementBatch(5, 9, null, AcknowledgeType.RELEASE),
                        // Acknowledging batch with another member will cause failure and rollback.
                        new AcknowledgementBatch(10, 14, null, AcknowledgeType.ACCEPT),
                        new AcknowledgementBatch(15, 19, null, AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

        // State should be rolled back to the previous state for any changes.
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
    }

    @Test
    public void testAcknowledgeWithAnotherMemberRollbackSubsetError() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(5, 10);
        MemoryRecords records3 = memoryRecords(5, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());

        result = sharePartition.acquire(
                "member-2",
                new FetchPartitionData(Errors.NONE, 20, 0, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Arrays.asList(
                        new AcknowledgementBatch(5, 9, null, AcknowledgeType.RELEASE),
                        new AcknowledgementBatch(10, 14, null, AcknowledgeType.ACCEPT),
                        // Acknowledging subset with another member will cause failure and rollback.
                        new AcknowledgementBatch(16, 18, null, AcknowledgeType.ACCEPT)));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

        assertEquals(3, sharePartition.cachedState().size());
        // Check the state of the cache. State should be rolled back to the previous state for any changes.
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
    }

    @Test
    public void testCanAcquireTrue() {
        SharePartition sharePartition = mockSharePartition();

        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());

        MemoryRecords records1 = memoryRecords(150, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());
    }

    @Test
    public void testCanAcquireFalse() {
        SharePartition sharePartition = mockSharePartition();

        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());

        MemoryRecords records1 = memoryRecords(150, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        MemoryRecords records2 = memoryRecords(100, 150);
        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 249, null, AcknowledgeType.ACCEPT)
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
    }

    @Test
    public void testCanAcquireRecordsReleasedAfterBeingAcquired() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(150, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        MemoryRecords records2 = memoryRecords(100, 150);
        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 89, null, AcknowledgeType.RELEASE)
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        // The records have been released, thus they are still available for being acquired
        assertFalse(sharePartition.canAcquireMore());
    }

    @Test
    public void testCanAcquireRecordsArchivedAfterBeingAcquired() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(150, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        MemoryRecords records2 = memoryRecords(100, 150);
        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 89, null, AcknowledgeType.REJECT)
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(90, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        // The records have been rejected, thus they are removed from the cachedState
        assertTrue(sharePartition.canAcquireMore());
    }

    @Test
    public void testCanAcquireRecordsAcknowledgedAfterBeingAcquired() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(150, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        MemoryRecords records2 = memoryRecords(100, 150);
        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 89, null, AcknowledgeType.ACCEPT)
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(90, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        // The records have been accepted, thus they are removed from the cachedState
        assertTrue(sharePartition.canAcquireMore());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementTypeAccept() {
        SharePartition sharePartition = mockSharePartition();

        assertEquals(0, sharePartition.cachedState().size());

        MemoryRecords records1 = memoryRecords(250, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertEquals(250, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 249, null, AcknowledgeType.ACCEPT)
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(250, sharePartition.nextFetchOffset());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireMore());
        // The records have been accepted, thus they are removed from the cachedState
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementTypeReject() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(250, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertEquals(250, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 249, null, AcknowledgeType.REJECT)
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(250, sharePartition.nextFetchOffset());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireMore());
        // The records have been rejected, thus they are removed from the cachedState
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementTypeRelease() {
        SharePartition sharePartition = mockSharePartition();
        MemoryRecords records1 = memoryRecords(250, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertEquals(250, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 249, null, AcknowledgeType.RELEASE)
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(0, sharePartition.nextFetchOffset());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertFalse(sharePartition.canAcquireMore());
        // The records have been released, thus they are not removed from the cachedState
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementsFromBeginningForBatchSubset() {
        short recordLockPartitionLimit = 20;
        SharePartition sharePartition = mockSharePartition(recordLockPartitionLimit);
        MemoryRecords records1 = memoryRecords(15, 0);
        MemoryRecords records2 = memoryRecords(15, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 12, null, AcknowledgeType.ACCEPT)
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(2, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().containsKey(0L));
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchState());
        assertEquals(0, sharePartition.cachedState().get(0L).baseOffset());
        assertEquals(14, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(0L).offsetState().get(12L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(13L).state());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        assertTrue(sharePartition.canAcquireMore());
        assertEquals(13, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementsFromBeginningForEntireBatch() {
        short recordLockPartitionLimit = 20;
        SharePartition sharePartition = mockSharePartition(recordLockPartitionLimit);
        MemoryRecords records1 = memoryRecords(15, 0);
        MemoryRecords records2 = memoryRecords(15, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 14, null, AcknowledgeType.REJECT)
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(1, sharePartition.cachedState().size());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        assertTrue(sharePartition.canAcquireMore());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementsInBetween() {
        short recordLockPartitionLimit = 20;
        SharePartition sharePartition = mockSharePartition(recordLockPartitionLimit);
        MemoryRecords records1 = memoryRecords(15, 0);
        MemoryRecords records2 = memoryRecords(15, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(10, 14, null, AcknowledgeType.REJECT)
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(2, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().containsKey(0L));
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchState());
        assertEquals(0, sharePartition.cachedState().get(0L).baseOffset());
        assertEquals(14, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(9L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).offsetState().get(10L).state());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAllRecordsInCachedStateAreAcknowledged() {
        short recordLockPartitionLimit = 20;
        SharePartition sharePartition = mockSharePartition(recordLockPartitionLimit);
        MemoryRecords records1 = memoryRecords(15, 0);
        MemoryRecords records2 = memoryRecords(15, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 29, null, AcknowledgeType.ACCEPT)
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(0, sharePartition.cachedState().size());

        assertTrue(sharePartition.canAcquireMore());
        assertEquals(30, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void tesMaybeUpdateCachedStateMultipleAcquisitionsAndAcknowledgements() {
        short recordLockPartitionLimit = 100;
        SharePartition sharePartition = mockSharePartition(recordLockPartitionLimit);
        MemoryRecords records1 = memoryRecords(20, 0);
        MemoryRecords records2 = memoryRecords(20, 20);
        MemoryRecords records3 = memoryRecords(20, 40);
        MemoryRecords records4 = memoryRecords(20, 60);
        MemoryRecords records5 = memoryRecords(100, 80);
        MemoryRecords records6 = memoryRecords(20, 180);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(19, sharePartition.endOffset());
        assertEquals(20, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(20L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(39, sharePartition.endOffset());
        assertEquals(40, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(20L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(40L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(40L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(40L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(59, sharePartition.endOffset());
        assertEquals(60, sharePartition.nextFetchOffset());

        // First Acknowledgement for the first batch of records
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 19, null, AcknowledgeType.ACCEPT)
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(20L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(40L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(40L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(40L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(20, sharePartition.startOffset());
        assertEquals(59, sharePartition.endOffset());
        assertEquals(60, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records4,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(20L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(40L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(40L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(40L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(60L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(60L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(60L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(20, sharePartition.startOffset());
        assertEquals(79, sharePartition.endOffset());
        assertEquals(80, sharePartition.nextFetchOffset());

        ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(20, 49, null, AcknowledgeType.ACCEPT)
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(2, sharePartition.cachedState().size());

        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(40L).batchState());
        assertEquals(40, sharePartition.cachedState().get(40L).baseOffset());
        assertEquals(59, sharePartition.cachedState().get(40L).lastOffset());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(40L).offsetState().get(49L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(40L).offsetState().get(50L).state());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(60L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(60L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(60L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(50, sharePartition.startOffset());
        assertEquals(79, sharePartition.endOffset());
        assertEquals(80, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records5,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(80L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(80L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(80L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(50, sharePartition.startOffset());
        assertEquals(179, sharePartition.endOffset());
        assertEquals(180, sharePartition.nextFetchOffset());

        // Final Acknowledgment, all records are acknowledged here
        ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(50, 179, null, AcknowledgeType.REJECT)
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(0, sharePartition.cachedState().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(180, sharePartition.startOffset());
        assertEquals(180, sharePartition.endOffset());
        assertEquals(180, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records6,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(180L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(180L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(180L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(180, sharePartition.startOffset());
        assertEquals(199, sharePartition.endOffset());
        assertEquals(200, sharePartition.nextFetchOffset());
    }
}
