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

import kafka.cluster.Partition;
import kafka.server.LogReadResult;
import kafka.server.ReplicaManager;
import kafka.server.ReplicaQuota;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.purgatory.DelayedOperationKey;
import org.apache.kafka.server.purgatory.DelayedOperationPurgatory;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchGroupKey;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetSnapshot;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static kafka.server.share.SharePartitionManagerTest.DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL;
import static kafka.server.share.SharePartitionManagerTest.PARTITION_MAX_BYTES;
import static kafka.server.share.SharePartitionManagerTest.buildLogReadResult;
import static kafka.server.share.SharePartitionManagerTest.mockReplicaManagerDelayedShareFetch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class DelayedShareFetchTest {
    private static final int MAX_WAIT_MS = 5000;
    private static final int MAX_FETCH_RECORDS = 100;
    private static final FetchParams FETCH_PARAMS = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(),
        FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS, 1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK,
        Optional.empty(), true);

    private Timer mockTimer;

    @BeforeEach
    public void setUp() {
        mockTimer = new SystemTimerReaper("DelayedShareFetchTestReaper",
            new SystemTimer("DelayedShareFetchTestTimer"));
    }

    @AfterEach
    public void tearDown() throws Exception {
        mockTimer.close();
    }

    @Test
    public void testDelayedShareFetchTryCompleteReturnsFalseDueToNonAcquirablePartitions() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes, MAX_FETCH_RECORDS);

        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .build());

        // Since there is no partition that can be acquired, tryComplete should return false.
        assertFalse(delayedShareFetch.tryComplete());
        assertFalse(delayedShareFetch.isCompleted());
        Mockito.verify(delayedShareFetch, times(0)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testTryCompleteWhenMinBytesNotSatisfiedOnFirstFetch() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                2, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes, MAX_FETCH_RECORDS);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp0.acquire(any(), anyInt(), any())).thenReturn(
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords().setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));

        // We are testing the case when the share partition is getting fetched for the first time, so for the first time
        // the fetchOffsetMetadata will return empty. Post the readFromLog call, the fetchOffsetMetadata will be
        // populated for the share partition, which has 1 as the positional difference, so it doesn't satisfy the minBytes(2).
        when(sp0.fetchOffsetMetadata(anyLong()))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        LogOffsetMetadata hwmOffsetMetadata = new LogOffsetMetadata(1, 1, 1);
        mockTopicIdPartitionFetchBytes(replicaManager, tp0, hwmOffsetMetadata);

        doAnswer(invocation -> buildLogReadResult(Collections.singleton(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();

        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManager(replicaManager)
            .withExceptionHandler(exceptionHandler)
            .build());
        assertFalse(delayedShareFetch.isCompleted());

        // Since sp1 cannot be acquired, tryComplete should return false.
        assertFalse(delayedShareFetch.tryComplete());
        assertFalse(delayedShareFetch.isCompleted());
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
        Mockito.verify(exceptionHandler, times(1)).accept(any(), any());
    }

    @Test
    public void testTryCompleteWhenMinBytesNotSatisfiedOnSubsequentFetch() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                2, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes, MAX_FETCH_RECORDS);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp0.acquire(any(), anyInt(), any())).thenReturn(
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords().setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));

        // We are testing the case when the share partition has been fetched before, hence we are mocking positionDiff
        // functionality to give the file position difference as 1 byte, so it doesn't satisfy the minBytes(2).
        LogOffsetMetadata hwmOffsetMetadata = mock(LogOffsetMetadata.class);
        when(hwmOffsetMetadata.positionDiff(any())).thenReturn(1);
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(mock(LogOffsetMetadata.class)));
        mockTopicIdPartitionFetchBytes(replicaManager, tp0, hwmOffsetMetadata);
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();

        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManager(replicaManager)
            .withExceptionHandler(exceptionHandler)
            .build());
        assertFalse(delayedShareFetch.isCompleted());

        // Since sp1 cannot be acquired, tryComplete should return false.
        assertFalse(delayedShareFetch.tryComplete());
        assertFalse(delayedShareFetch.isCompleted());
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
        Mockito.verify(exceptionHandler, times(1)).accept(any(), any());
    }

    @Test
    public void testDelayedShareFetchTryCompleteReturnsTrue() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes, MAX_FETCH_RECORDS);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp0.acquire(anyString(), anyInt(), any(FetchPartitionData.class))).thenReturn(
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords().setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));
        doAnswer(invocation -> buildLogReadResult(Collections.singleton(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp0, 1);
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManager(replicaManager)
            .build());
        assertFalse(delayedShareFetch.isCompleted());

        // Since sp1 can be acquired, tryComplete should return true.
        assertTrue(delayedShareFetch.tryComplete());
        assertTrue(delayedShareFetch.isCompleted());
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testEmptyFutureReturnedByDelayedShareFetchOnComplete() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            future, partitionMaxBytes, MAX_FETCH_RECORDS);

        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManager(replicaManager)
            .withSharePartitions(sharePartitions)
            .build());
        assertFalse(delayedShareFetch.isCompleted());
        delayedShareFetch.forceComplete();

        // Since no partition could be acquired, the future should be empty and replicaManager.readFromLog should not be called.
        assertEquals(0, future.join().size());
        Mockito.verify(replicaManager, times(0)).readFromLog(
                any(), any(), any(ReplicaQuota.class), anyBoolean());
        assertTrue(delayedShareFetch.isCompleted());
        Mockito.verify(delayedShareFetch, times(0)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testReplicaManagerFetchShouldHappenOnComplete() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes, MAX_FETCH_RECORDS);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        when(sp0.acquire(anyString(), anyInt(), any(FetchPartitionData.class))).thenReturn(
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords().setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));
        doAnswer(invocation -> buildLogReadResult(Collections.singleton(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManager(replicaManager)
            .withSharePartitions(sharePartitions)
            .build());
        assertFalse(delayedShareFetch.isCompleted());
        delayedShareFetch.forceComplete();

        // Since we can acquire records from sp0, replicaManager.readFromLog should be called once and only for sp0.
        Mockito.verify(replicaManager, times(1)).readFromLog(
                any(), any(), any(ReplicaQuota.class), anyBoolean());
        Mockito.verify(sp0, times(1)).nextFetchOffset();
        Mockito.verify(sp1, times(0)).nextFetchOffset();
        assertTrue(delayedShareFetch.isCompleted());
        assertTrue(shareFetch.isCompleted());
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testToCompleteAnAlreadyCompletedFuture() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            future, partitionMaxBytes, MAX_FETCH_RECORDS);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(false);

        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManager(replicaManager)
            .withSharePartitions(sharePartitions)
            .build());
        assertFalse(delayedShareFetch.isCompleted());

        // Force completing the share fetch request for the first time should complete the future with an empty map.
        delayedShareFetch.forceComplete();
        assertTrue(delayedShareFetch.isCompleted());
        // Verifying that the first forceComplete calls acquirablePartitions method in DelayedShareFetch.
        Mockito.verify(delayedShareFetch, times(1)).acquirablePartitions();
        assertEquals(0, future.join().size());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();

        // Force completing the share fetch request for the second time should hit the future completion check and not
        // proceed ahead in the function.
        delayedShareFetch.forceComplete();
        assertTrue(delayedShareFetch.isCompleted());
        // Verifying that the second forceComplete does not call acquirablePartitions method in DelayedShareFetch.
        Mockito.verify(delayedShareFetch, times(1)).acquirablePartitions();
        Mockito.verify(delayedShareFetch, times(0)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testForceCompleteTriggersDelayedActionsQueue() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(topicId, new TopicPartition("foo", 2));
        Map<TopicIdPartition, Integer> partitionMaxBytes1 = new HashMap<>();
        partitionMaxBytes1.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes1.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        // No share partition is available for acquiring initially.
        when(sp0.maybeAcquireFetchLock()).thenReturn(false);
        when(sp1.maybeAcquireFetchLock()).thenReturn(false);
        when(sp2.maybeAcquireFetchLock()).thenReturn(false);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions1 = new LinkedHashMap<>();
        sharePartitions1.put(tp0, sp0);
        sharePartitions1.put(tp1, sp1);
        sharePartitions1.put(tp2, sp2);

        ShareFetch shareFetch1 = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes1, MAX_FETCH_RECORDS);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, replicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, true, true);
        mockReplicaManagerDelayedShareFetch(replicaManager, delayedShareFetchPurgatory);

        List<DelayedOperationKey> delayedShareFetchWatchKeys = new ArrayList<>();
        partitionMaxBytes1.keySet().forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        DelayedShareFetch delayedShareFetch1 = DelayedShareFetchTest.DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch1)
            .withReplicaManager(replicaManager)
            .withSharePartitions(sharePartitions1)
            .build();

        // We add a delayed share fetch entry to the purgatory which will be waiting for completion since neither of the
        // partitions in the share fetch request can be acquired.
        delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch1, delayedShareFetchWatchKeys);

        assertEquals(2, delayedShareFetchPurgatory.watched());
        assertFalse(shareFetch1.isCompleted());
        assertTrue(delayedShareFetch1.lock().tryLock());
        delayedShareFetch1.lock().unlock();

        Map<TopicIdPartition, Integer> partitionMaxBytes2 = new HashMap<>();
        partitionMaxBytes2.put(tp1, PARTITION_MAX_BYTES);
        partitionMaxBytes2.put(tp2, PARTITION_MAX_BYTES);
        ShareFetch shareFetch2 = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes2, MAX_FETCH_RECORDS);

        doAnswer(invocation -> buildLogReadResult(Collections.singleton(tp1))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions2 = new LinkedHashMap<>();
        sharePartitions2.put(tp0, sp0);
        sharePartitions2.put(tp1, sp1);
        sharePartitions2.put(tp2, sp2);

        DelayedShareFetch delayedShareFetch2 = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch2)
            .withReplicaManager(replicaManager)
            .withSharePartitions(sharePartitions2)
            .build());

        // sp1 can be acquired now
        when(sp1.maybeAcquireFetchLock()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp1.acquire(anyString(), anyInt(), any(FetchPartitionData.class))).thenReturn(
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords().setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));

        // when forceComplete is called for delayedShareFetch2, since tp1 is common in between delayed share fetch
        // requests, it should add a "check and complete" action for request key tp1 on the purgatory.
        delayedShareFetch2.forceComplete();
        assertTrue(delayedShareFetch2.isCompleted());
        assertTrue(shareFetch2.isCompleted());
        Mockito.verify(replicaManager, times(1)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        assertFalse(delayedShareFetch1.isCompleted());
        Mockito.verify(replicaManager, times(1)).addToActionQueue(any());
        Mockito.verify(replicaManager, times(0)).tryCompleteActions();
        Mockito.verify(delayedShareFetch2, times(1)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch2.lock().tryLock());
        delayedShareFetch2.lock().unlock();
    }

    @Test
    public void testCombineLogReadResponse() {
        String groupId = "grp";
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            future, partitionMaxBytes, MAX_FETCH_RECORDS);

        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManager(replicaManager)
            .withSharePartitions(sharePartitions)
            .build();

        LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = new LinkedHashMap<>();
        topicPartitionData.put(tp0, mock(FetchRequest.PartitionData.class));
        topicPartitionData.put(tp1, mock(FetchRequest.PartitionData.class));

        // Case 1 - logReadResponse contains tp0.
        LinkedHashMap<TopicIdPartition, LogReadResult> logReadResponse = new LinkedHashMap<>();
        logReadResponse.put(tp0, mock(LogReadResult.class));

        doAnswer(invocation -> buildLogReadResult(Collections.singleton(tp1))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());
        LinkedHashMap<TopicIdPartition, LogReadResult> combinedLogReadResponse = delayedShareFetch.combineLogReadResponse(topicPartitionData, logReadResponse);
        assertEquals(topicPartitionData.keySet(), combinedLogReadResponse.keySet());
        assertEquals(combinedLogReadResponse.get(tp0), logReadResponse.get(tp0));

        // Case 2 - logReadResponse contains tp0 and tp1.
        logReadResponse = new LinkedHashMap<>();
        logReadResponse.put(tp0, mock(LogReadResult.class));
        logReadResponse.put(tp1, mock(LogReadResult.class));
        combinedLogReadResponse = delayedShareFetch.combineLogReadResponse(topicPartitionData, logReadResponse);
        assertEquals(topicPartitionData.keySet(), combinedLogReadResponse.keySet());
        assertEquals(combinedLogReadResponse.get(tp0), logReadResponse.get(tp0));
        assertEquals(combinedLogReadResponse.get(tp1), logReadResponse.get(tp1));
    }

    @Test
    public void testExceptionInMinBytesCalculation() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);

        when(sp0.maybeAcquireFetchLock()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        ShareFetch shareFetch = new ShareFetch(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes, MAX_FETCH_RECORDS);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.acquire(any(), anyInt(), any())).thenReturn(
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords().setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));
        doAnswer(invocation -> buildLogReadResult(Collections.singleton(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        // Mocking partition object to throw an exception during min bytes calculation while calling fetchOffsetSnapshot
        Partition partition = mock(Partition.class);
        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(partition);
        when(partition.fetchOffsetSnapshot(any(), anyBoolean())).thenThrow(new RuntimeException("Exception thrown"));

        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManager(replicaManager)
            .withExceptionHandler(exceptionHandler)
            .build());

        // Try complete should return false as the share partition has errored out.
        assertFalse(delayedShareFetch.tryComplete());
        // Fetch should remain pending and should be completed on request timeout.
        assertFalse(delayedShareFetch.isCompleted());
        // The request should be errored out as topic partition should get added as erroneous.
        assertTrue(shareFetch.errorInAllPartitions());

        Mockito.verify(exceptionHandler, times(1)).accept(any(), any());
        Mockito.verify(replicaManager, times(1)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());

        // Force complete the request as it's still pending. Return false from the share partition lock acquire.
        when(sp0.maybeAcquireFetchLock()).thenReturn(false);
        assertTrue(delayedShareFetch.forceComplete());
        assertTrue(delayedShareFetch.isCompleted());

        // Read from log and release partition locks should not be called as the request is errored out.
        Mockito.verify(replicaManager, times(1)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
        Mockito.verify(exceptionHandler, times(1)).accept(any(), any());
    }

    @Test
    public void testLocksReleasedForCompletedFetch() {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions1 = new LinkedHashMap<>();
        sharePartitions1.put(tp0, sp0);

        ReplicaManager replicaManager = mock(ReplicaManager.class);
        doAnswer(invocation -> buildLogReadResult(Collections.singleton(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp0, 1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), Map.of(tp0, PARTITION_MAX_BYTES), MAX_FETCH_RECORDS);

        DelayedShareFetch delayedShareFetch = DelayedShareFetchTest.DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions1)
            .withReplicaManager(replicaManager)
            .build();

        DelayedShareFetch spy = spy(delayedShareFetch);
        doReturn(false).when(spy).forceComplete();

        assertFalse(spy.tryComplete());
        Mockito.verify(sp0, times(1)).releaseFetchLock();
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testLocksReleasedAcquireException() {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        when(sp0.canAcquireRecords()).thenThrow(new RuntimeException("Acquire exception"));

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), Map.of(tp0, PARTITION_MAX_BYTES), MAX_FETCH_RECORDS);

        DelayedShareFetch delayedShareFetch = DelayedShareFetchTest.DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .build();

        assertFalse(delayedShareFetch.tryComplete());
        Mockito.verify(sp0, times(1)).releaseFetchLock();
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    static void mockTopicIdPartitionToReturnDataEqualToMinBytes(ReplicaManager replicaManager, TopicIdPartition topicIdPartition, int minBytes) {
        LogOffsetMetadata hwmOffsetMetadata = new LogOffsetMetadata(1, 1, minBytes);
        LogOffsetSnapshot endOffsetSnapshot = new LogOffsetSnapshot(1, mock(LogOffsetMetadata.class),
            hwmOffsetMetadata, mock(LogOffsetMetadata.class));
        Partition partition = mock(Partition.class);
        when(partition.isLeader()).thenReturn(true);
        when(partition.getLeaderEpoch()).thenReturn(1);
        when(partition.fetchOffsetSnapshot(any(), anyBoolean())).thenReturn(endOffsetSnapshot);
        when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition())).thenReturn(partition);
    }

    private void mockTopicIdPartitionFetchBytes(ReplicaManager replicaManager, TopicIdPartition topicIdPartition, LogOffsetMetadata hwmOffsetMetadata) {
        LogOffsetSnapshot endOffsetSnapshot = new LogOffsetSnapshot(1, mock(LogOffsetMetadata.class),
            hwmOffsetMetadata, mock(LogOffsetMetadata.class));
        Partition partition = mock(Partition.class);
        when(partition.fetchOffsetSnapshot(any(), anyBoolean())).thenReturn(endOffsetSnapshot);
        when(replicaManager.getPartitionOrException(topicIdPartition.topicPartition())).thenReturn(partition);
    }

    @SuppressWarnings("unchecked")
    private static BiConsumer<SharePartitionKey, Throwable> mockExceptionHandler() {
        return mock(BiConsumer.class);
    }

    static class DelayedShareFetchBuilder {
        ShareFetch shareFetch = mock(ShareFetch.class);
        private ReplicaManager replicaManager = mock(ReplicaManager.class);
        private BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        private LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = mock(LinkedHashMap.class);

        DelayedShareFetchBuilder withShareFetchData(ShareFetch shareFetch) {
            this.shareFetch = shareFetch;
            return this;
        }

        DelayedShareFetchBuilder withReplicaManager(ReplicaManager replicaManager) {
            this.replicaManager = replicaManager;
            return this;
        }

        DelayedShareFetchBuilder withExceptionHandler(BiConsumer<SharePartitionKey, Throwable> exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        DelayedShareFetchBuilder withSharePartitions(LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions) {
            this.sharePartitions = sharePartitions;
            return this;
        }

        public static DelayedShareFetchBuilder builder() {
            return new DelayedShareFetchBuilder();
        }

        public DelayedShareFetch build() {
            return new DelayedShareFetch(
                shareFetch,
                replicaManager,
                exceptionHandler,
                sharePartitions);
        }
    }
}
