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
import kafka.server.share.SharePartition.RecordState;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
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
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    private static class SharePartitionBuilder {

        private int acquisitionLockTimeoutMs = 30000;
        private int maxDeliveryCount = MAX_DELIVERY_COUNT;
        private int maxInflightMessages = MAX_IN_FLIGHT_MESSAGES;
        private ReplicaManager replicaManager = REPLICA_MANAGER;

        public static SharePartitionBuilder builder() {
            return new SharePartitionBuilder();
        }

        public SharePartition build() {
            return new SharePartition(GROUP_ID, TOPIC_ID_PARTITION, maxInflightMessages, maxDeliveryCount,
                acquisitionLockTimeoutMs, mockTimer, MOCK_TIME, replicaManager);
        }
    }
}
