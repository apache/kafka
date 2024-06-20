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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.group.share.NoOpShareStatePersister;
import org.apache.kafka.server.group.share.Persister;
import org.apache.kafka.server.share.ShareAcknowledgementBatch;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
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

import static kafka.server.share.SharePartition.EMPTY_MEMBER_ID;
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
    private static final short MAX_IN_FLIGHT_MESSAGES = 200;
    private static final ReplicaManager REPLICA_MANAGER = Mockito.mock(ReplicaManager.class);

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
    public void testAcquireSingleRecord() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(0, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testAcquireMultipleRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcquireMultipleRecordsWithOverlapAndNewBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, MemoryRecords.EMPTY,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        assertEquals(0, result.join().size());
        assertEquals(0, sharePartition.nextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetInitialState() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        assertEquals(0, sharePartition.nextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWithCachedStateAcquired() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertEquals(5, sharePartition.nextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWithFindAndCachedStateEmpty() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        sharePartition.findNextFetchOffset(true);
        assertTrue(sharePartition.findNextFetchOffset());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertFalse(sharePartition.findNextFetchOffset());
    }

    @Test
    public void testNextFetchOffsetWithFindAndCachedState() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        sharePartition.findNextFetchOffset(true);
        assertTrue(sharePartition.findNextFetchOffset());
        sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertEquals(5, sharePartition.nextFetchOffset());
        assertFalse(sharePartition.findNextFetchOffset());
    }

    @Test
    public void testCanFetchRecordsWithEmptyCache() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightMessages(1).build();
        assertTrue(sharePartition.canFetchRecords());
    }

    @Test
    public void testCanFetchRecordsWithCachedDataAndLimitNotReached() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightMessages(6).build();
        sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        // Limit not reached as only 6 in-flight messages is the limit.
        assertTrue(sharePartition.canFetchRecords());
    }

    @Test
    public void testCanFetchRecordsWithCachedDataAndLimitReached() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightMessages(1).build();
        sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, memoryRecords(5),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        // Limit reached as only one in-flight message is the limit.
        assertFalse(sharePartition.canFetchRecords());
    }

    @Test
    public void testMaybeAcquireAndReleaseFetchLock() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
        SharePartition sharePartition = SharePartitionBuilder.builder().build();

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
            Collections.singletonList(new ShareAcknowledgementBatch(1, 1, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(2, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(1L).batchState());
        assertEquals(1, sharePartition.cachedState().get(1L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(1L).offsetState());
    }

    @Test
    public void testAcknowledgeMultipleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(10, 5);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(5, 14, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testAcknowledgeMultipleRecordBatchWithGapOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
            Arrays.asList(
                new ShareAcknowledgementBatch(5, 6, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(10, 18, Arrays.asList(
                    (byte) 2, (byte) 2, (byte) 2,
                    (byte) 2, (byte) 2, (byte) 0,
                    (byte) 0, (byte) 0, (byte) 1
                ))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

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
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
            Collections.singletonList(new ShareAcknowledgementBatch(6, 18, Arrays.asList(
                // TODO: NOTE - untracked gap of 3 offsets from 7-9 has no effect on acknowledgment
                //  irrespective of acknowledgement type provided. While acquiring, the log start
                //  offset should be used to determine such gaps.
                (byte) 1, (byte) 1, (byte) 1,
                (byte) 1, (byte) 1, (byte) 1,
                (byte) 0, (byte) 0, (byte) 1,
                (byte) 0, (byte) 1, (byte) 0,
                (byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

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
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        // Acknowledge a batch when cache is empty.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(0, 15, Collections.singletonList((byte) 3))));
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
            Collections.singletonList(new ShareAcknowledgementBatch(20, 25, Collections.singletonList((byte) 3))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRequestException.class, ackResult.join().get().getClass());
    }

    @Test
    public void testAcknowledgeWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
            Collections.singletonList(new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 3))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
    }

    @Test
    public void testAcknowledgeWhenOffsetNotAcquired() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
            Collections.singletonList(new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        // Acknowledge the same batch again but with ACCEPT type.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 1))));
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
            Collections.singletonList(new ShareAcknowledgementBatch(6, 8, Collections.singletonList((byte) 3))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        // Re-acknowledge the subset batch with REJECT type.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new ShareAcknowledgementBatch(6, 8, Collections.singletonList((byte) 3))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
    }

    @Test
    public void testAcknowledgeRollbackWithFullBatchError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
                new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(10, 14, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(15, 19, Collections.singletonList((byte) 1)),
                // Add another batch which should fail the request.
                new ShareAcknowledgementBatch(15, 19, Collections.singletonList((byte) 1))));
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
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
                new ShareAcknowledgementBatch(5, 9, Collections.singletonList((byte) 2)),
                new ShareAcknowledgementBatch(10, 14, Collections.singletonList((byte) 1)),
                new ShareAcknowledgementBatch(15, 19, Collections.singletonList((byte) 1)),
                // Add another batch which should fail the request.
                new ShareAcknowledgementBatch(16, 19, Collections.singletonList((byte) 1))));
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
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
            Collections.singletonList(new ShareAcknowledgementBatch(12, 13, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

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
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
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
            Collections.singletonList(new ShareAcknowledgementBatch(12, 30, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

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

    private MemoryRecords memoryRecords(int numOfRecords) {
        return memoryRecords(numOfRecords, 0);
    }

    private MemoryRecords memoryRecords(int numOfRecords, long startOffset) {
        return memoryRecordsBuilder(numOfRecords, startOffset).build();
    }

    private MemoryRecordsBuilder memoryRecordsBuilder(int numOfRecords, long startOffset) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024),
            Compression.NONE, TimestampType.CREATE_TIME, startOffset);
        for (int i = 0; i < numOfRecords; i++) {
            builder.appendWithOffset(startOffset + i, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        }
        return builder;
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

    private static class SharePartitionBuilder {

        private int acquisitionLockTimeoutMs = 30000;
        private int maxDeliveryCount = MAX_DELIVERY_COUNT;
        private int maxInflightMessages = MAX_IN_FLIGHT_MESSAGES;
        private Persister persister = NoOpShareStatePersister.getInstance();
        private ReplicaManager replicaManager = REPLICA_MANAGER;

        private SharePartitionBuilder withMaxInflightMessages(int maxInflightMessages) {
            this.maxInflightMessages = maxInflightMessages;
            return this;
        }

        private SharePartitionBuilder withPersister(Persister persister) {
            this.persister = persister;
            return this;
        }

        public static SharePartitionBuilder builder() {
            return new SharePartitionBuilder();
        }

        public SharePartition build() {
            return new SharePartition(GROUP_ID, TOPIC_ID_PARTITION, maxInflightMessages, maxDeliveryCount,
                acquisitionLockTimeoutMs, mockTimer, MOCK_TIME, persister, replicaManager);
        }
    }
}
