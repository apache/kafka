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
import kafka.server.QuotaFactory;
import kafka.server.ReplicaManager;

import org.apache.kafka.clients.consumer.internals.ShareAcquireMode;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogManager;
import org.apache.kafka.server.purgatory.DelayedOperationKey;
import org.apache.kafka.server.purgatory.DelayedOperationPurgatory;
import org.apache.kafka.server.quota.ReplicaQuota;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.share.PartitionMetadataProvider;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchGroupKey;
import org.apache.kafka.server.share.fetch.PartitionMaxBytesStrategy;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.share.metrics.ShareGroupMetrics;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.ServerTestUtils;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetSnapshot;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

import static kafka.server.share.PendingRemoteFetches.RemoteFetch;
import static kafka.server.share.SharePartitionManagerTest.DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL;
import static kafka.server.share.SharePartitionManagerTest.REMOTE_FETCH_MAX_WAIT_MS;
import static kafka.server.share.SharePartitionManagerTest.buildEmptyLogReadResult;
import static kafka.server.share.SharePartitionManagerTest.buildLogReadResult;
import static kafka.server.share.SharePartitionManagerTest.mockReplicaManagerDelayedShareFetch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class DelayedShareFetchTest {
    private static final int MAX_WAIT_MS = 5000;
    private static final int BATCH_SIZE = 500;
    private static final byte BATCH_OPTIMIZED = ShareAcquireMode.BATCH_OPTIMIZED.id();
    private static final int MAX_FETCH_RECORDS = 100;
    private static final FetchParams FETCH_PARAMS = new FetchParams(
        FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS, 1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK,
        Optional.empty(), true);
    private static final FetchDataInfo REMOTE_FETCH_INFO = new FetchDataInfo(new LogOffsetMetadata(0, 0, 0),
        MemoryRecords.EMPTY, false, Optional.empty(), Optional.of(mock(RemoteStorageFetchInfo.class)));
    private static final BrokerTopicStats BROKER_TOPIC_STATS = new BrokerTopicStats();

    private Timer mockTimer;

    @BeforeEach
    public void setUp() {
        ServerTestUtils.clearYammerMetrics();
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

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp1.canAcquireRecords()).thenReturn(false);

        Partition p0 = mock(Partition.class);
        when(p0.isLeader()).thenReturn(true);

        Partition p1 = mock(Partition.class);
        when(p1.isLeader()).thenReturn(true);

        ReplicaManager replicaManager = mock(ReplicaManager.class);
        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);
        when(replicaManager.getPartitionOrException(tp1.topicPartition())).thenReturn(p1);

        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(new MockTime());
        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withShareGroupMetrics(shareGroupMetrics)
            .withFetchId(fetchId)
            .withReplicaManagerLogReader(replicaManager)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        // Since there is no partition that can be acquired, tryComplete should return false.
        assertFalse(delayedShareFetch.tryComplete());
        assertFalse(delayedShareFetch.isCompleted());
        Mockito.verify(delayedShareFetch, times(0)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        // Metrics shall not be recorded as no partition is acquired.
        assertNull(shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId));
        assertNull(shareGroupMetrics.topicPartitionsFetchRatio(groupId));
        assertEquals(0, delayedShareFetch.expiredRequestMeter().count());

        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testTryCompleteWhenMinBytesNotSatisfiedOnFirstFetch() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(
            new FetchParams(FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                2, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);

        // We are testing the case when the share partition is getting fetched for the first time, so for the first time
        // the fetchOffsetMetadata will return empty. Post the first readFromLog call, the fetchOffsetMetadata will be
        // populated for the share partition, which has 1 as the positional difference, so it doesn't satisfy the minBytes(2).
        when(sp0.fetchOffsetMetadata(anyLong()))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        LogOffsetMetadata hwmOffsetMetadata = new LogOffsetMetadata(1, 1, 1);

        doAnswer(invocation -> buildLogReadResult(List.of(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));

        Partition p0 = mock(Partition.class);
        mockTopicIdPartitionFetchBytes(hwmOffsetMetadata, p0);
        when(p0.isLeader()).thenReturn(true);

        Partition p1 = mock(Partition.class);
        when(p1.isLeader()).thenReturn(true);

        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);
        when(replicaManager.getPartitionOrException(tp1.topicPartition())).thenReturn(p1);

        Time time = mock(Time.class);
        when(time.hiResClockMs()).thenReturn(100L).thenReturn(110L);
        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(time);
        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withExceptionHandler(exceptionHandler)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withShareGroupMetrics(shareGroupMetrics)
            .withTime(time)
            .withFetchId(fetchId)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        assertFalse(delayedShareFetch.isCompleted());

        // Since minBytes(2) is not satisfied (only 1 byte available from sp0), and sp1 cannot be acquired, tryComplete should return false.
        assertFalse(delayedShareFetch.tryComplete());
        assertFalse(delayedShareFetch.isCompleted());
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        // Though the request is not completed but sp0 was acquired and hence the metric should be recorded.
        assertEquals(1, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).count());
        assertEquals(10, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).sum());
        // Since the request is not completed, the fetch ratio should be null.
        assertNull(shareGroupMetrics.topicPartitionsFetchRatio(groupId));

        delayedShareFetch.lock().unlock();
        Mockito.verify(exceptionHandler, never()).accept(any(), any());
    }

    @Test
    public void testTryCompleteWhenMinBytesNotSatisfiedOnSubsequentFetch() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(
            new FetchParams(FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                2, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);

        // We are testing the case when the share partition has been fetched before, hence we are mocking positionDiff
        // functionality to give the file position difference as 1 byte, so it doesn't satisfy the minBytes(2).
        LogOffsetMetadata hwmOffsetMetadata = mock(LogOffsetMetadata.class);
        when(hwmOffsetMetadata.positionDiff(any())).thenReturn(1);
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(mock(LogOffsetMetadata.class)));
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();

        Partition p0 = mock(Partition.class);
        mockTopicIdPartitionFetchBytes(hwmOffsetMetadata, p0);
        when(p0.isLeader()).thenReturn(true);

        Partition p1 = mock(Partition.class);
        when(p1.isLeader()).thenReturn(true);

        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);
        when(replicaManager.getPartitionOrException(tp1.topicPartition())).thenReturn(p1);

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withExceptionHandler(exceptionHandler)
            .withFetchId(fetchId)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        assertFalse(delayedShareFetch.isCompleted());

        // Since sp1 cannot be acquired, tryComplete should return false.
        assertFalse(delayedShareFetch.tryComplete());
        assertFalse(delayedShareFetch.isCompleted());
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
        Mockito.verify(exceptionHandler, never()).accept(any(), any());
    }

    @Test
    public void testDelayedShareFetchTryCompleteReturnsTrue() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        doAnswer(invocation -> buildLogReadResult(List.of(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp0, 1);

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));

        Time time = mock(Time.class);
        when(time.hiResClockMs()).thenReturn(120L).thenReturn(140L);
        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(time);
        Uuid fetchId = Uuid.randomUuid();
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withShareGroupMetrics(shareGroupMetrics)
            .withTime(time)
            .withFetchId(fetchId)
            .withExceptionHandler(exceptionHandler)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        assertFalse(delayedShareFetch.isCompleted());

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any()))
                .thenReturn(Map.of(tp0, mock(ShareFetchResponseData.PartitionData.class)));

            // Since sp0 can be acquired, tryComplete should return true.
            assertTrue(delayedShareFetch.tryComplete());
            assertTrue(delayedShareFetch.isCompleted());
            Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());
            assertTrue(delayedShareFetch.lock().tryLock());
            assertEquals(1, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).count());
            assertEquals(20, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).sum());
            assertEquals(1, shareGroupMetrics.topicPartitionsFetchRatio(groupId).count());
            assertEquals(50, shareGroupMetrics.topicPartitionsFetchRatio(groupId).sum());
            Mockito.verify(exceptionHandler, never()).accept(any(), any());

            delayedShareFetch.lock().unlock();
        }
    }

    @Test
    public void testEmptyFutureReturnedByDelayedShareFetchOnComplete() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            future, List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp1.canAcquireRecords()).thenReturn(false);

        Time time = mock(Time.class);
        when(time.hiResClockMs()).thenReturn(90L).thenReturn(140L);
        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(time);
        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions)
            .withShareGroupMetrics(shareGroupMetrics)
            .withTime(time)
            .withFetchId(fetchId)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        assertFalse(delayedShareFetch.isCompleted());
        delayedShareFetch.forceComplete();

        // Since no partition could be acquired, the future should be empty and replicaManager.readFromLog should not be called.
        assertEquals(0, future.join().size());
        Mockito.verify(replicaManager, times(0)).readFromLog(
                any(), any(), any(ReplicaQuota.class), anyBoolean());
        assertTrue(delayedShareFetch.isCompleted());
        Mockito.verify(delayedShareFetch, times(0)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        // As the request is completed by onComplete then both metrics shall be recorded.
        assertEquals(1, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).count());
        assertEquals(50, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).sum());
        assertEquals(1, shareGroupMetrics.topicPartitionsFetchRatio(groupId).count());
        assertEquals(0, shareGroupMetrics.topicPartitionsFetchRatio(groupId).sum());

        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testReplicaManagerFetchShouldHappenOnComplete() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        doAnswer(invocation -> buildLogReadResult(List.of(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));

        Time time = mock(Time.class);
        when(time.hiResClockMs()).thenReturn(10L).thenReturn(140L);
        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(time);
        Uuid fetchId = Uuid.randomUuid();
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withShareGroupMetrics(shareGroupMetrics)
            .withTime(time)
            .withFetchId(fetchId)
            .withExceptionHandler(exceptionHandler)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any()))
                .thenReturn(Map.of(tp0, mock(ShareFetchResponseData.PartitionData.class)));

            assertFalse(delayedShareFetch.isCompleted());
            delayedShareFetch.forceComplete();

            Mockito.verify(exceptionHandler, never()).accept(any(), any());
            // Since we can acquire records from sp0, replicaManager.readFromLog should be called once and only for sp0.
            Mockito.verify(replicaManager, times(1)).readFromLog(
                any(), any(), any(ReplicaQuota.class), anyBoolean());
            Mockito.verify(sp0, times(1)).nextFetchOffset();
            Mockito.verify(sp1, times(0)).nextFetchOffset();
            assertTrue(delayedShareFetch.isCompleted());
            assertTrue(shareFetch.isCompleted());
            Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());
            assertTrue(delayedShareFetch.lock().tryLock());
            assertEquals(1, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).count());
            assertEquals(130, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).sum());
            assertEquals(1, shareGroupMetrics.topicPartitionsFetchRatio(groupId).count());
            assertEquals(50, shareGroupMetrics.topicPartitionsFetchRatio(groupId).sum());

            delayedShareFetch.lock().unlock();
        }
    }

    @Test
    public void testToCompleteAnAlreadyCompletedFuture() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        when(sp0.canAcquireRecords()).thenReturn(false);

        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(new MockTime());
        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions)
            .withShareGroupMetrics(shareGroupMetrics)
            .withFetchId(fetchId)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        assertFalse(delayedShareFetch.isCompleted());

        // Force completing the share fetch request for the first time should complete the future with an empty map.
        delayedShareFetch.forceComplete();
        assertTrue(delayedShareFetch.isCompleted());
        // Verifying that the first forceComplete calls acquirablePartitions method in DelayedShareFetch.
        Mockito.verify(delayedShareFetch, times(1)).acquirablePartitions(sharePartitions);
        assertEquals(0, future.join().size());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();

        // Force completing the share fetch request for the second time should hit the future completion check and not
        // proceed ahead in the function.
        delayedShareFetch.forceComplete();
        assertTrue(delayedShareFetch.isCompleted());
        // Verifying that the second forceComplete does not call acquirablePartitions method in DelayedShareFetch.
        Mockito.verify(delayedShareFetch, times(1)).acquirablePartitions(sharePartitions);
        Mockito.verify(delayedShareFetch, times(0)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        // Assert both metrics shall be recorded only once.
        assertEquals(1, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).count());
        assertEquals(1, shareGroupMetrics.topicPartitionsFetchRatio(groupId).count());

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
        List<TopicIdPartition> topicIdPartitions1 = List.of(tp0, tp1);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions1 = new LinkedHashMap<>();
        sharePartitions1.put(tp0, sp0);
        sharePartitions1.put(tp1, sp1);
        sharePartitions1.put(tp2, sp2);

        ShareFetch shareFetch1 = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), topicIdPartitions1, BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, replicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(replicaManager, delayedShareFetchPurgatory);

        List<DelayedOperationKey> delayedShareFetchWatchKeys = new ArrayList<>();
        topicIdPartitions1.forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        Partition p0 = mock(Partition.class);
        when(p0.isLeader()).thenReturn(true);

        Partition p1 = mock(Partition.class);
        when(p1.isLeader()).thenReturn(true);

        Partition p2 = mock(Partition.class);
        when(p2.isLeader()).thenReturn(true);

        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);
        when(replicaManager.getPartitionOrException(tp1.topicPartition())).thenReturn(p1);
        when(replicaManager.getPartitionOrException(tp2.topicPartition())).thenReturn(p2);

        Uuid fetchId1 = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch1 = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch1)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions1)
            .withFetchId(fetchId1)
            .build();

        // No share partition is available for acquiring initially.
        when(sp0.maybeAcquireFetchLock(fetchId1)).thenReturn(false);
        when(sp1.maybeAcquireFetchLock(fetchId1)).thenReturn(false);
        when(sp2.maybeAcquireFetchLock(fetchId1)).thenReturn(false);

        // We add a delayed share fetch entry to the purgatory which will be waiting for completion since neither of the
        // partitions in the share fetch request can be acquired.
        delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch1, delayedShareFetchWatchKeys);

        assertEquals(2, delayedShareFetchPurgatory.watched());
        assertFalse(shareFetch1.isCompleted());
        assertTrue(delayedShareFetch1.lock().tryLock());
        delayedShareFetch1.lock().unlock();

        ShareFetch shareFetch2 = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        doAnswer(invocation -> buildLogReadResult(List.of(tp1))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp1));

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions2 = new LinkedHashMap<>();
        sharePartitions2.put(tp0, sp0);
        sharePartitions2.put(tp1, sp1);
        sharePartitions2.put(tp2, sp2);

        Uuid fetchId2 = Uuid.randomUuid();
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        DelayedShareFetch delayedShareFetch2 = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch2)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions2)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withFetchId(fetchId2)
            .withExceptionHandler(exceptionHandler)
            .build());

        // sp1 can be acquired now
        when(sp1.maybeAcquireFetchLock(fetchId2)).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any()))
                .thenReturn(Map.of(tp0, mock(ShareFetchResponseData.PartitionData.class)));

            // when forceComplete is called for delayedShareFetch2, since tp1 is common in between delayed share fetch
            // requests, it should add a "check and complete" action for request key tp1 on the purgatory.
            delayedShareFetch2.forceComplete();
            assertTrue(delayedShareFetch2.isCompleted());
            assertTrue(shareFetch2.isCompleted());
            Mockito.verify(exceptionHandler, never()).accept(any(), any());
            Mockito.verify(replicaManager, times(1)).readFromLog(
                any(), any(), any(ReplicaQuota.class), anyBoolean());
            assertFalse(delayedShareFetch1.isCompleted());
            Mockito.verify(replicaManager, times(1)).addToActionQueue(any());
            Mockito.verify(replicaManager, times(0)).tryCompleteActions();
            Mockito.verify(delayedShareFetch2, times(1)).releasePartitionLocks(any());
            assertTrue(delayedShareFetch2.lock().tryLock());
            delayedShareFetch2.lock().unlock();
        }
    }

    @Test
    public void testForceCompleteNotTriggersDelayedActionsQueueWhenFetchDataIsEmpty() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(topicId, new TopicPartition("foo", 2));
        List<TopicIdPartition> topicIdPartitions1 = List.of(tp0, tp1);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions1 = new LinkedHashMap<>();
        sharePartitions1.put(tp0, sp0);
        sharePartitions1.put(tp1, sp1);
        sharePartitions1.put(tp2, sp2);

        ShareFetch shareFetch1 = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), topicIdPartitions1, BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory = new DelayedOperationPurgatory<>(
            "TestShareFetch", mockTimer, replicaManager.localBrokerId(),
            DELAYED_SHARE_FETCH_PURGATORY_PURGE_INTERVAL, false, true);
        mockReplicaManagerDelayedShareFetch(replicaManager, delayedShareFetchPurgatory);

        List<DelayedOperationKey> delayedShareFetchWatchKeys = new ArrayList<>();
        topicIdPartitions1.forEach(topicIdPartition -> delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));

        Uuid fetchId1 = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch1 = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch1)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions1)
            .withFetchId(fetchId1)
            .build();

        // No share partition is available for acquiring initially.
        when(sp0.maybeAcquireFetchLock(fetchId1)).thenReturn(false);
        when(sp1.maybeAcquireFetchLock(fetchId1)).thenReturn(false);
        when(sp2.maybeAcquireFetchLock(fetchId1)).thenReturn(false);

        // We add a delayed share fetch entry to the purgatory which will be waiting for completion since neither of the
        // partitions in the share fetch request can be acquired.
        delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch1, delayedShareFetchWatchKeys);

        assertEquals(2, delayedShareFetchPurgatory.watched());
        assertFalse(shareFetch1.isCompleted());
        assertTrue(delayedShareFetch1.lock().tryLock());
        delayedShareFetch1.lock().unlock();

        ShareFetch shareFetch2 = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        doAnswer(invocation -> buildEmptyLogReadResult(List.of(tp1))).when(replicaManager).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp1));

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions2 = new LinkedHashMap<>();
        sharePartitions2.put(tp0, sp0);
        sharePartitions2.put(tp1, sp1);
        sharePartitions2.put(tp2, sp2);

        Uuid fetchId2 = Uuid.randomUuid();
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        DelayedShareFetch delayedShareFetch2 = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch2)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions2)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withFetchId(fetchId2)
            .withExceptionHandler(exceptionHandler)
            .build());

        // sp1 can be acquired now.
        when(sp1.maybeAcquireFetchLock(fetchId2)).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class, Mockito.CALLS_REAL_METHODS)) {
            ShareFetchResponseData.PartitionData mockedPartitionData = mock(ShareFetchResponseData.PartitionData.class);
            // Empty fetched data.
            when(mockedPartitionData.records()).thenReturn(MemoryRecords.EMPTY);
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any()))
                .thenReturn(Map.of(tp0, mockedPartitionData));

            // when forceComplete is called for delayedShareFetch2, since tp1 is common in between delayed share fetch
            // requests, it will not add a "check and complete" action for request key tp1 on the purgatory since no
            // data was returned in tp1.
            delayedShareFetch2.forceComplete();
            assertTrue(delayedShareFetch2.isCompleted());
            assertTrue(shareFetch2.isCompleted());
            Mockito.verify(exceptionHandler, never()).accept(any(), any());
            Mockito.verify(replicaManager, times(1)).readFromLog(
                any(), any(), any(ReplicaQuota.class), anyBoolean());
            assertFalse(delayedShareFetch1.isCompleted());
            Mockito.verify(replicaManager, times(0)).addToActionQueue(any());
            Mockito.verify(replicaManager, times(0)).tryCompleteActions();
            Mockito.verify(delayedShareFetch2, times(1)).releasePartitionLocks(any());
            assertTrue(delayedShareFetch2.lock().tryLock());
            delayedShareFetch2.lock().unlock();
        }
    }

    @Test
    public void testCombineLogReadResponse() {
        String groupId = "grp";
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(
            new FetchParams(FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            future, List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp1));

        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .build();

        LinkedHashMap<TopicIdPartition, Long> topicPartitionData = new LinkedHashMap<>();
        topicPartitionData.put(tp0, 0L);
        topicPartitionData.put(tp1, 0L);

        // Case 1 - logReadResponse contains tp0.
        LinkedHashMap<TopicIdPartition, LogReadResult> logReadResponse = new LinkedHashMap<>();
        LogReadResult logReadResult = mock(LogReadResult.class);
        Records records = mock(Records.class);
        when(records.sizeInBytes()).thenReturn(2);
        FetchDataInfo fetchDataInfo = new FetchDataInfo(mock(LogOffsetMetadata.class), records);
        when(logReadResult.info()).thenReturn(fetchDataInfo);
        logReadResponse.put(tp0, logReadResult);

        doAnswer(invocation -> buildLogReadResult(List.of(tp1))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());
        LinkedHashMap<TopicIdPartition, LogReadResult> combinedLogReadResponse = delayedShareFetch.combineLogReadResponse(topicPartitionData, logReadResponse).join();
        assertEquals(topicPartitionData.keySet(), combinedLogReadResponse.keySet());
        assertEquals(combinedLogReadResponse.get(tp0), logReadResponse.get(tp0));

        // Case 2 - logReadResponse contains tp0 and tp1.
        logReadResponse = new LinkedHashMap<>();
        logReadResponse.put(tp0, mock(LogReadResult.class));
        logReadResponse.put(tp1, mock(LogReadResult.class));
        combinedLogReadResponse = delayedShareFetch.combineLogReadResponse(topicPartitionData, logReadResponse).join();
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

        SharePartition sp0 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        ShareFetch shareFetch = new ShareFetch(
            new FetchParams(FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.canAcquireRecords()).thenReturn(true);
        doAnswer(invocation -> buildLogReadResult(List.of(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        // Mocking partition object to throw an exception during min bytes calculation while calling fetchOffsetSnapshot
        Partition partition = mock(Partition.class);
        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(partition);
        when(partition.fetchOffsetSnapshot(any(), anyBoolean())).thenThrow(new RuntimeException("Exception thrown"));

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));

        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        Time time = mock(Time.class);
        when(time.hiResClockMs()).thenReturn(100L).thenReturn(110L).thenReturn(170L);
        ShareGroupMetrics shareGroupMetrics = new ShareGroupMetrics(time);
        Uuid fetchId = Uuid.randomUuid();

        Partition p0 = mock(Partition.class);
        when(p0.isLeader()).thenReturn(true);

        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);

        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withExceptionHandler(exceptionHandler)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withShareGroupMetrics(shareGroupMetrics)
            .withTime(time)
            .withFetchId(fetchId)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);

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
        Mockito.verify(sp0, times(1)).releaseFetchLock(fetchId);

        // Force complete the request as it's still pending. Return false from the share partition lock acquire.
        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(false);
        assertTrue(delayedShareFetch.forceComplete());
        assertTrue(delayedShareFetch.isCompleted());

        // Read from log and release partition locks should not be called as the request is errored out.
        Mockito.verify(replicaManager, times(1)).readFromLog(
            any(), any(), any(ReplicaQuota.class), anyBoolean());
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        assertEquals(2, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).count());
        assertEquals(70, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).sum());
        assertEquals(10, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).min());
        assertEquals(60, shareGroupMetrics.topicPartitionsAcquireTimeMs(groupId).max());
        assertEquals(1, shareGroupMetrics.topicPartitionsFetchRatio(groupId).count());
        assertEquals(0, shareGroupMetrics.topicPartitionsFetchRatio(groupId).sum());

        delayedShareFetch.lock().unlock();
        Mockito.verify(exceptionHandler, times(1)).accept(any(), any());
    }

    @Test
    public void testTryCompleteReleasesLocksWhenAsyncReadImmediatelyThrows() {
        // Reproduces the fast-path lock check: when maybeReadFromLog's future is already completed
        // but exceptionally.
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.canAcquireRecords()).thenReturn(true);
        // Empty fetch offset metadata forces maybeReadFromLog to actually read from the log.
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        // A synchronous throw from readFromLog makes ReplicaManagerLogReader.readAsync return an
        // already-completed (failed) future, so tryComplete takes the fast path with a completed future.
        when(replicaManager.readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean()))
            .thenThrow(new RuntimeException("Read failed"));

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withExceptionHandler(exceptionHandler)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withFetchId(fetchId)
            .build());

        // The first acquire (in tryComplete) succeeds; a subsequent acquire attempt fails, mirroring the
        // real SharePartition where the lock is already held by this fetchId and cannot be re-acquired.
        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true).thenReturn(false);

        assertFalse(delayedShareFetch.isCompleted());
        // The fast-path exception handling completes the request with the error.
        assertTrue(delayedShareFetch.tryComplete());
        assertTrue(delayedShareFetch.isCompleted());
        assertTrue(shareFetch.errorInAllPartitions());
        assertTrue(future.isDone());

        // The fetch lock must be released (exactly once).
        Mockito.verify(sp0, times(1)).releaseFetchLock(fetchId);
        Mockito.verify(exceptionHandler, times(1)).accept(any(), any());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> response = future.join();
        assertEquals(Set.of(tp0), response.keySet());
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), response.get(tp0).errorCode());
        assertTrue(response.get(tp0).errorMessage().contains("Read failed"));
    }

    @Test
    public void testTryCompleteLocksReleasedOnCompleteException() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        doAnswer(invocation -> buildLogReadResult(List.of(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp0, 1);

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));
        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withFetchId(fetchId)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        assertFalse(delayedShareFetch.isCompleted());
        // Throw exception for onComplete.
        doThrow(new RuntimeException()).when(delayedShareFetch).onComplete();
        // Try to complete the request.
        assertFalse(delayedShareFetch.tryComplete());

        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(any());
        Mockito.verify(sp0, times(1)).releaseFetchLock(fetchId);
    }

    @Test
    public void testLocksReleasedForCompletedFetch() {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions1 = new LinkedHashMap<>();
        sharePartitions1.put(tp0, sp0);

        ReplicaManager replicaManager = mock(ReplicaManager.class);
        doAnswer(invocation -> buildLogReadResult(List.of(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp0, 1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions1)
            .withReplicaManagerLogReader(replicaManager)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withFetchId(fetchId)
            .build();

        DelayedShareFetch spy = spy(delayedShareFetch);
        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        doReturn(false).when(spy).forceComplete();

        assertFalse(spy.tryComplete());
        Mockito.verify(sp0, times(1)).releaseFetchLock(fetchId);
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testLocksReleasedAcquireException() {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.canAcquireRecords()).thenThrow(new RuntimeException("Acquire exception"));

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        Uuid fetchId = Uuid.randomUuid();

        Partition p0 = mock(Partition.class);
        when(p0.isLeader()).thenReturn(true);

        ReplicaManager replicaManager = mock(ReplicaManager.class);
        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);

        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withFetchId(fetchId)
            .withReplicaManagerLogReader(replicaManager)
            .build();

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        assertFalse(delayedShareFetch.tryComplete());
        Mockito.verify(sp0, times(1)).releaseFetchLock(fetchId);
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @Test
    public void testTryCompleteWhenPartitionMaxBytesStrategyThrowsException() {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        SharePartition sp0 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();

        ShareFetch shareFetch = new ShareFetch(
            new FetchParams(FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                2, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        // partitionMaxBytesStrategy.maxBytes() function throws an exception
        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mock(PartitionMaxBytesStrategy.class);
        when(partitionMaxBytesStrategy.maxBytes(anyInt(), any(), anyInt())).thenThrow(new IllegalArgumentException("Exception thrown"));

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withExceptionHandler(mockExceptionHandler())
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withFetchId(fetchId)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        assertFalse(delayedShareFetch.isCompleted());
        assertTrue(delayedShareFetch.tryComplete());
        assertTrue(delayedShareFetch.isCompleted());
        // releasePartitionLocks is called twice - first time from tryComplete and second time from onComplete
        Mockito.verify(delayedShareFetch, times(2)).releasePartitionLocks(any());
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();

        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = future.join();
        assertEquals(1, partitionDataMap.size());
        assertTrue(partitionDataMap.containsKey(tp0));
        assertEquals("Exception thrown", partitionDataMap.get(tp0).errorMessage());
    }

    @Test
    public void testPartitionMaxBytesFromUniformStrategyWhenAllPartitionsAreAcquirable() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 2));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 3));
        TopicIdPartition tp4 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 4));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        SharePartition sp3 = mock(SharePartition.class);
        SharePartition sp4 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(true);
        when(sp3.canAcquireRecords()).thenReturn(true);
        when(sp4.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);
        sharePartitions.put(tp3, sp3);
        sharePartitions.put(tp4, sp4);

        ShareFetch shareFetch = new ShareFetch(new FetchParams(
            FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS, 1, 1024 * 1020,
            FetchIsolation.HIGH_WATERMARK, Optional.empty(), true), groupId,
            Uuid.randomUuid().toString(), new CompletableFuture<>(), List.of(tp0, tp1, tp2, tp3, tp4), BATCH_OPTIMIZED, BATCH_SIZE,
            MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        // All 5 partitions are acquirable.
        doAnswer(invocation -> buildLogReadResult(sharePartitions.keySet().stream().toList())).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        when(sp2.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        when(sp3.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        when(sp4.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));

        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp0, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp1, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp2, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp3, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp4, 1);

        Uuid fetchId = Uuid.randomUuid();
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withPartitionMaxBytesStrategy(PartitionMaxBytesStrategy.type(PartitionMaxBytesStrategy.StrategyType.UNIFORM))
            .withFetchId(fetchId)
            .withExceptionHandler(exceptionHandler)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp2.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp3.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp4.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any()))
                .thenReturn(Map.of(
                    tp0, mock(ShareFetchResponseData.PartitionData.class),
                    tp1, mock(ShareFetchResponseData.PartitionData.class),
                    tp2, mock(ShareFetchResponseData.PartitionData.class),
                    tp3, mock(ShareFetchResponseData.PartitionData.class),
                    tp4, mock(ShareFetchResponseData.PartitionData.class)));

            assertTrue(delayedShareFetch.tryComplete());
            assertTrue(delayedShareFetch.isCompleted());

            // Since all partitions are acquirable, maxbytes per partition = requestMaxBytes(i.e. 1024*1020) / acquiredTopicPartitions(i.e. 5)
            int expectedPartitionMaxBytes = 1024 * 1020 / 5;
            LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> expectedReadPartitionInfo = new LinkedHashMap<>();
            sharePartitions.keySet().forEach(topicIdPartition -> expectedReadPartitionInfo.put(topicIdPartition,
                new FetchRequest.PartitionData(
                    topicIdPartition.topicId(),
                    0,
                    0,
                    expectedPartitionMaxBytes,
                    Optional.empty()
                )));

            Mockito.verify(exceptionHandler, never()).accept(any(), any());
            Mockito.verify(replicaManager, times(1)).readFromLog(
                shareFetch.fetchParams(),
                CollectionConverters.asScala(
                    sharePartitions.keySet().stream().map(topicIdPartition ->
                        new Tuple2<>(topicIdPartition, expectedReadPartitionInfo.get(topicIdPartition))).collect(Collectors.toList())
                ),
                QuotaFactory.UNBOUNDED_QUOTA,
                true);
        }
    }

    @Test
    public void testPartitionMaxBytesFromUniformStrategyWhenFewPartitionsAreAcquirable() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 2));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 3));
        TopicIdPartition tp4 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 4));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        SharePartition sp3 = mock(SharePartition.class);
        SharePartition sp4 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(false);
        when(sp3.canAcquireRecords()).thenReturn(false);
        when(sp4.canAcquireRecords()).thenReturn(false);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);
        sharePartitions.put(tp3, sp3);
        sharePartitions.put(tp4, sp4);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0, tp1, tp2, tp3, tp4), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        // Only 2 out of 5 partitions are acquirable.
        Set<TopicIdPartition> acquirableTopicPartitions = new LinkedHashSet<>();
        acquirableTopicPartitions.add(tp0);
        acquirableTopicPartitions.add(tp1);
        doAnswer(invocation -> buildLogReadResult(acquirableTopicPartitions.stream().toList())).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));

        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp0, 1);
        mockTopicIdPartitionToReturnDataEqualToMinBytes(replicaManager, tp1, 1);

        Uuid fetchId = Uuid.randomUuid();
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withPartitionMaxBytesStrategy(PartitionMaxBytesStrategy.type(PartitionMaxBytesStrategy.StrategyType.UNIFORM))
            .withFetchId(fetchId)
            .withExceptionHandler(exceptionHandler)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp2.maybeAcquireFetchLock(fetchId)).thenReturn(false);
        when(sp3.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp4.maybeAcquireFetchLock(fetchId)).thenReturn(false);

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any()))
                .thenReturn(Map.of(
                    tp0, mock(ShareFetchResponseData.PartitionData.class),
                    tp1, mock(ShareFetchResponseData.PartitionData.class)));

            assertTrue(delayedShareFetch.tryComplete());
            assertTrue(delayedShareFetch.isCompleted());

            // Since only 2 partitions are acquirable, maxbytes per partition = requestMaxBytes(i.e. 1024*1024) / acquiredTopicPartitions(i.e. 2)
            int expectedPartitionMaxBytes = 1024 * 1024 / 2;
            LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> expectedReadPartitionInfo = new LinkedHashMap<>();
            acquirableTopicPartitions.forEach(topicIdPartition -> expectedReadPartitionInfo.put(topicIdPartition,
                new FetchRequest.PartitionData(
                    topicIdPartition.topicId(),
                    0,
                    0,
                    expectedPartitionMaxBytes,
                    Optional.empty()
                )));

            Mockito.verify(replicaManager, times(1)).readFromLog(
                shareFetch.fetchParams(),
                CollectionConverters.asScala(
                    acquirableTopicPartitions.stream().map(topicIdPartition ->
                        new Tuple2<>(topicIdPartition, expectedReadPartitionInfo.get(topicIdPartition))).collect(Collectors.toList())
                ),
                QuotaFactory.UNBOUNDED_QUOTA,
                true);
            Mockito.verify(exceptionHandler, never()).accept(any(), any());
        }
    }

    @Test
    public void testPartitionMaxBytesFromUniformStrategyInCombineLogReadResponse() {
        String groupId = "grp";
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 2));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);

        ShareFetch shareFetch = new ShareFetch(
            new FetchParams(FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                1, 1024 * 1020, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0, tp1, tp2), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions)
            .withPartitionMaxBytesStrategy(PartitionMaxBytesStrategy.type(PartitionMaxBytesStrategy.StrategyType.UNIFORM))
            .build();

        LinkedHashMap<TopicIdPartition, Long> topicPartitionData = new LinkedHashMap<>();
        topicPartitionData.put(tp0, 0L);
        topicPartitionData.put(tp1, 0L);
        topicPartitionData.put(tp2, 0L);

        // Existing fetched data already contains tp0.
        LinkedHashMap<TopicIdPartition, LogReadResult> logReadResponse = new LinkedHashMap<>();
        LogReadResult logReadResult = mock(LogReadResult.class);
        Records records = mock(Records.class);
        when(records.sizeInBytes()).thenReturn(2);
        FetchDataInfo fetchDataInfo = new FetchDataInfo(mock(LogOffsetMetadata.class), records);
        when(logReadResult.info()).thenReturn(fetchDataInfo);
        logReadResponse.put(tp0, logReadResult);

        Set<TopicIdPartition> fetchableTopicPartitions = new LinkedHashSet<>();
        fetchableTopicPartitions.add(tp1);
        fetchableTopicPartitions.add(tp2);
        // We will be doing replica manager fetch only for tp1 and tp2.
        doAnswer(invocation -> buildLogReadResult(fetchableTopicPartitions.stream().toList())).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());
        LinkedHashMap<TopicIdPartition, LogReadResult> combinedLogReadResponse = delayedShareFetch.combineLogReadResponse(topicPartitionData, logReadResponse).join();

        assertEquals(topicPartitionData.keySet(), combinedLogReadResponse.keySet());
        // Since only 2 partitions are fetchable but the third one has already been fetched, maxbytes per partition = requestMaxBytes(i.e. 1024*1020) / acquiredTopicPartitions(i.e. 3)
        int expectedPartitionMaxBytes = 1024 * 1020 / 3;
        LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> expectedReadPartitionInfo = new LinkedHashMap<>();
        fetchableTopicPartitions.forEach(topicIdPartition -> expectedReadPartitionInfo.put(topicIdPartition,
            new FetchRequest.PartitionData(
                topicIdPartition.topicId(),
                0,
                0,
                expectedPartitionMaxBytes,
                Optional.empty()
            )));

        Mockito.verify(replicaManager, times(1)).readFromLog(
            shareFetch.fetchParams(),
            CollectionConverters.asScala(
                fetchableTopicPartitions.stream().map(topicIdPartition ->
                    new Tuple2<>(topicIdPartition, expectedReadPartitionInfo.get(topicIdPartition))).collect(Collectors.toList())
            ),
            QuotaFactory.UNBOUNDED_QUOTA,
            true);
    }

    @Test
    public void testOnCompleteExecutionOnTimeout() {
        ShareFetch shareFetch = new ShareFetch(
            FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);
        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .build();
        assertFalse(delayedShareFetch.isCompleted());
        assertFalse(shareFetch.isCompleted());
        // Call run to execute onComplete and onExpiration.
        delayedShareFetch.run();
        assertTrue(shareFetch.isCompleted());
        assertEquals(1, delayedShareFetch.expiredRequestMeter().count());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRemoteStorageFetchTryCompleteReturnsFalse() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 2));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0, tp1, tp2), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.nextFetchOffset()).thenReturn(10L);
        when(sp1.nextFetchOffset()).thenReturn(20L);
        when(sp2.nextFetchOffset()).thenReturn(30L);

        // Fetch offset matches with the cached entry for sp0 but not for sp1 and sp2. Hence, a replica manager fetch will happen for sp1 and sp2.
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(10, 1, 0)));
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp2.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        // Mocking local log read result for tp1 and remote storage read result for tp2.
        doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(tp1), Set.of(tp2))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        // Remote fetch related mocks. Remote fetch object does not complete within tryComplete in this mock.
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(remoteLogManager.asyncRead(any(), any())).thenReturn(mock(Future.class));
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        Partition p0 = mock(Partition.class);
        when(p0.isLeader()).thenReturn(true);

        Partition p1 = mock(Partition.class);
        when(p1.isLeader()).thenReturn(true);

        Partition p2 = mock(Partition.class);
        when(p2.isLeader()).thenReturn(true);

        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);
        when(replicaManager.getPartitionOrException(tp1.topicPartition())).thenReturn(p1);
        when(replicaManager.getPartitionOrException(tp2.topicPartition())).thenReturn(p2);

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0, tp1, tp2)))
            .withFetchId(fetchId)
            .build());

        // All the topic partitions are acquirable.
        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp2.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        assertFalse(delayedShareFetch.isCompleted());
        assertFalse(delayedShareFetch.tryComplete());
        assertFalse(delayedShareFetch.isCompleted());
        // Remote fetch object gets created for delayed share fetch object.
        assertNotNull(delayedShareFetch.pendingRemoteFetches());
        // Verify the locks are released for local log read topic partitions tp0 and tp1.
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp0, tp1));
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    /**
     * Validates the tryComplete() that handles an in-flight remote fetch: when tryComplete() is
     * re-invoked (e.g. via a purgatory checkAndComplete) while a remote fetch initiated by an earlier
     * tryComplete() is still pending, it must route to maybeCompletePendingRemoteFetch() rather than
     * re-acquiring partitions or starting a new read. It stays in purgatory while the remote fetch is
     * pending and completes once the remote fetch is done.
     */
    @Test
    public void testTryCompleteWithPendingRemoteFetchInFlight() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        SharePartition sp0 = mock(SharePartition.class);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.nextFetchOffset()).thenReturn(10L);
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        // tp0 is served from remote storage.
        doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(), Set.of(tp0)))
            .when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        // Capture the remote read completion callback but do not invoke it yet, so the remote fetch stays
        // in flight after the first tryComplete().
        AtomicReference<Consumer<RemoteLogReadResult>> remoteCallback = new AtomicReference<>();
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(remoteLogManager.asyncRead(any(), any())).thenAnswer(invocation -> {
            remoteCallback.set(invocation.getArgument(1));
            return mock(Future.class);
        });
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        Partition p0 = mock(Partition.class);
        when(p0.isLeader()).thenReturn(true);
        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0)))
            .withFetchId(fetchId)
            .build());

        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class)) {
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any()))
                .thenReturn(Map.of(tp0, mock(ShareFetchResponseData.PartitionData.class)));

            // First tryComplete initiates the remote fetch which does not complete synchronously.
            assertFalse(delayedShareFetch.tryComplete());
            assertNotNull(delayedShareFetch.pendingRemoteFetches());
            assertFalse(delayedShareFetch.isCompleted());

            // Second tryComplete re-enters with a pending remote fetch in flight (the branch under test).
            // The remote fetch is still not done, so it stays in purgatory and does not re-acquire the
            // partition or start a new read.
            assertFalse(delayedShareFetch.tryComplete());
            assertFalse(delayedShareFetch.isCompleted());
            Mockito.verify(sp0, times(1)).maybeAcquireFetchLock(fetchId);
            Mockito.verify(sp0, times(0)).releaseFetchLock(fetchId);

            // Complete the in-flight remote fetch, then a subsequent tryComplete completes via the same
            // branch (maybeCompletePendingRemoteFetch now sees the remote fetch as done).
            remoteCallback.get().accept(new RemoteLogReadResult(Optional.of(REMOTE_FETCH_INFO), Optional.empty()));
            assertTrue(delayedShareFetch.tryComplete());
            assertTrue(delayedShareFetch.isCompleted());
            assertTrue(shareFetch.isCompleted());
            assertEquals(Set.of(tp0), future.join().keySet());
            Mockito.verify(sp0, times(1)).maybeAcquireFetchLock(fetchId);
            Mockito.verify(sp0, times(1)).releaseFetchLock(fetchId);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRemoteStorageFetchPartitionLeaderChanged() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            new CompletableFuture<>(), List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.nextFetchOffset()).thenReturn(10L);

        // Fetch offset does not match with the cached entry for sp0, hence, a replica manager fetch will happen for sp0.
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        // Mocking remote storage read result for tp0.
        doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(), Set.of(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        // Remote fetch related mocks. Remote fetch object does not complete within tryComplete in this mock.
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(remoteLogManager.asyncRead(any(), any())).thenReturn(mock(Future.class));
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        Partition p0 = mock(Partition.class);
        when(p0.isLeader()).thenReturn(false);

        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0)))
            .withFetchId(fetchId)
            .build());

        // All the topic partitions are acquirable.
        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        // Mock the behaviour of replica manager such that remote storage fetch completion timer task completes on adding it to the watch queue.
        doAnswer(invocationOnMock -> {
            TimerTask timerTask = invocationOnMock.getArgument(0);
            timerTask.run();
            return null;
        }).when(replicaManager).addShareFetchTimerRequest(any());

        assertFalse(delayedShareFetch.isCompleted());
        assertTrue(delayedShareFetch.tryComplete());
        assertTrue(delayedShareFetch.isCompleted());
        // Remote fetch object gets created for delayed share fetch object.
        assertNotNull(delayedShareFetch.pendingRemoteFetches());
        // Verify the locks are released for local log read topic partitions tp0.
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp0));
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRemoteStorageFetchTryCompleteThrowsException() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 2));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0, tp1, tp2), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.nextFetchOffset()).thenReturn(10L);
        when(sp1.nextFetchOffset()).thenReturn(20L);
        when(sp2.nextFetchOffset()).thenReturn(25L);

        // Fetch offset does not match with the cached entry for sp0, sp1 and sp2. Hence, a replica manager fetch will happen for all.
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp2.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        // Mocking local log read result for tp0 and remote storage read result for tp1 and tp2.
        doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(tp0), Set.of(tp1, tp2))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        // Remote fetch related mocks. Exception will be thrown during the creation of remoteFetch object for tp2.
        // remoteFetchTask gets created for tp1 successfully.
        Future<Void> remoteFetchTask = mock(Future.class);
        doAnswer(invocation -> {
            when(remoteFetchTask.isCancelled()).thenReturn(true);
            return false;
        }).when(remoteFetchTask).cancel(false);
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(remoteLogManager.asyncRead(any(), any()))
            .thenReturn(remoteFetchTask) // for tp1
            .thenThrow(new RejectedExecutionException("Exception thrown"));  // for tp2
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withExceptionHandler(exceptionHandler)
            .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0, tp1, tp2)))
            .withFetchId(fetchId)
            .build());

        // All the topic partitions are acquirable.
        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp2.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        assertFalse(delayedShareFetch.isCompleted());
        // tryComplete returns true and goes to forceComplete once the exception occurs.
        assertTrue(delayedShareFetch.tryComplete());
        assertTrue(delayedShareFetch.isCompleted());
        // The future of shareFetch completes.
        assertTrue(shareFetch.isCompleted());
        // The remoteFetchTask created for tp1 is cancelled successfully.
        assertTrue(remoteFetchTask.isCancelled());
        assertFalse(future.isCompletedExceptionally());
        assertEquals(Set.of(tp1, tp2), future.join().keySet());
        // Exception occurred and was handled.
        Mockito.verify(exceptionHandler, times(2)).accept(any(), any());
        // Verify the locks are released for all local and remote read topic partitions tp0, tp1 and tp2 because of exception occurrence.
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp0));
        Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp1, tp2));
        Mockito.verify(delayedShareFetch, times(1)).onComplete();
        assertTrue(delayedShareFetch.lock().tryLock());
        delayedShareFetch.lock().unlock();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRemoteStorageFetchTryCompletionDueToBrokerBecomingOffline() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 2));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0, tp1, tp2), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.nextFetchOffset()).thenReturn(10L);
        when(sp1.nextFetchOffset()).thenReturn(20L);
        when(sp2.nextFetchOffset()).thenReturn(30L);

        // Fetch offset matches with the cached entry for sp0 but not for sp1 and sp2. Hence, a replica manager fetch will happen for sp1 and sp2 during tryComplete.
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.of(new LogOffsetMetadata(10, 1, 0)));
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp2.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class)) {
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
            partitionDataMap.put(tp0, mock(ShareFetchResponseData.PartitionData.class));
            partitionDataMap.put(tp1, mock(ShareFetchResponseData.PartitionData.class));
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any())).thenReturn(partitionDataMap);

            // Mocking local log read result for tp1 and remote storage read result for tp2 on first replicaManager readFromLog call(from tryComplete).
            // Mocking local log read result for tp0 and tp1 on second replicaManager readFromLog call(from onComplete).
            doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(tp1), Set.of(tp2))
            ).doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(tp0, tp1), Set.of())
            ).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

            // Remote fetch related mocks. Remote fetch object does not complete within tryComplete in this mock but the broker becomes unavailable.
            Future<Void> remoteFetchTask = mock(Future.class);
            doAnswer(invocation -> {
                when(remoteFetchTask.isCancelled()).thenReturn(true);
                return false;
            }).when(remoteFetchTask).cancel(false);

            when(remoteFetchTask.cancel(false)).thenReturn(true);
            RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
            when(remoteLogManager.asyncRead(any(), any())).thenReturn(remoteFetchTask);
            when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));
            when(replicaManager.getPartitionOrException(tp2.topicPartition())).thenThrow(mock(KafkaStorageException.class));

            // Mock the behaviour of replica manager such that remote storage fetch completion timer task completes on adding it to the watch queue.
            doAnswer(invocationOnMock -> {
                TimerTask timerTask = invocationOnMock.getArgument(0);
                timerTask.run();
                return null;
            }).when(replicaManager).addShareFetchTimerRequest(any());

            Uuid fetchId = Uuid.randomUuid();
            DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
                .withShareFetchData(shareFetch)
                .withSharePartitions(sharePartitions)
                .withReplicaManagerLogReader(replicaManager)
                .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0, tp1, tp2)))
                .withFetchId(fetchId)
                .build());

            // All the topic partitions are acquirable.
            when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
            when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);
            when(sp2.maybeAcquireFetchLock(fetchId)).thenReturn(true);

            assertFalse(delayedShareFetch.isCompleted());
            assertTrue(delayedShareFetch.tryComplete());

            assertTrue(delayedShareFetch.isCompleted());
            // Pending remote fetch object gets created for delayed share fetch.
            assertNotNull(delayedShareFetch.pendingRemoteFetches());
            List<RemoteFetch> remoteFetches = delayedShareFetch.pendingRemoteFetches().remoteFetches();
            assertEquals(1, remoteFetches.size());
            assertTrue(remoteFetches.get(0).remoteFetchTask().isCancelled());
            // Partition locks should be released for all 3 topic partitions. tp0 and tp1 (local log read)
            // locks are released twice: once when the remote fetch is prepared (as non-remote partitions)
            // and again after they are re-acquired for the additional local read in onComplete. tp2 (remote
            // storage read) lock is released once when the remote fetch completes.
            Mockito.verify(delayedShareFetch, times(2)).releasePartitionLocks(Set.of(tp0, tp1));
            Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp2));
            assertTrue(shareFetch.isCompleted());
            // Share fetch response contained tp0 and tp1 (local fetch) but not tp2, since it errored out.
            assertEquals(Set.of(tp0, tp1), future.join().keySet());
            assertTrue(delayedShareFetch.lock().tryLock());
            delayedShareFetch.lock().unlock();
        }
    }

    @Test
    public void testRemoteStorageFetchRequestCompletionOnFutureCompletionFailure() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.nextFetchOffset()).thenReturn(10L);
        // Fetch offset does not match with the cached entry for sp0. Hence, a replica manager fetch will happen for sp0.
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        // Mocking remote storage read result for tp0.
        doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(), Set.of(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        // Remote fetch related mocks. Remote fetch object completes within tryComplete in this mock, hence request will move on to forceComplete.
        RemoteLogReadResult remoteFetchResult = new RemoteLogReadResult(
            Optional.empty(),
            Optional.of(new TimeoutException("Error occurred while creating remote fetch result")) // Remote fetch result is returned with an error.
        );
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        doAnswer(invocationOnMock -> {
            // Make sure that the callback is called to populate remoteFetchResult for the mock behaviour.
            Consumer<RemoteLogReadResult> callback = invocationOnMock.getArgument(1);
            callback.accept(remoteFetchResult);
            return CompletableFuture.completedFuture(remoteFetchResult);
        }).when(remoteLogManager).asyncRead(any(), any());
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        Uuid fetchId = Uuid.randomUuid();

        Partition p0 = mock(Partition.class);
        when(p0.isLeader()).thenReturn(true);

        Partition p1 = mock(Partition.class);
        when(p1.isLeader()).thenReturn(true);

        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);
        when(replicaManager.getPartitionOrException(tp1.topicPartition())).thenReturn(p1);

        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0, tp1)))
            .withFetchId(fetchId)
            .withExceptionHandler(exceptionHandler)
            .build());

        // sp0 is acquirable, sp1 is not acquirable.
        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(false);

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any()))
                .thenReturn(Map.of(tp0, new ShareFetchResponseData.PartitionData().setErrorCode(Errors.REQUEST_TIMED_OUT.code())));

            assertFalse(delayedShareFetch.isCompleted());
            assertTrue(delayedShareFetch.tryComplete());

            assertTrue(delayedShareFetch.isCompleted());
            // Pending remote fetch object gets created for delayed share fetch.
            assertNotNull(delayedShareFetch.pendingRemoteFetches());
            // Verify the locks are released for tp0.
            Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp0));
            Mockito.verify(exceptionHandler, never()).accept(any(), any());
            assertTrue(shareFetch.isCompleted());
            assertEquals(Set.of(tp0), future.join().keySet());
            assertEquals(Errors.REQUEST_TIMED_OUT.code(), future.join().get(tp0).errorCode());
            assertTrue(delayedShareFetch.lock().tryLock());
            delayedShareFetch.lock().unlock();
        }
    }

    @Test
    public void testRemoteStorageFetchRequestCompletionOnFutureCompletionSuccessfully() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.nextFetchOffset()).thenReturn(10L);
        // Fetch offset does not match with the cached entry for sp0. Hence, a replica manager fetch will happen for sp0.
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        // Mocking remote storage read result for tp0.
        doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(), Set.of(tp0))).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        // Remote fetch related mocks. Remote fetch object completes within tryComplete in this mock, hence request will move on to forceComplete.
        RemoteLogReadResult remoteFetchResult = new RemoteLogReadResult(
            Optional.of(REMOTE_FETCH_INFO),
            Optional.empty() // Remote fetch result is returned successfully without error.
        );
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        doAnswer(invocationOnMock -> {
            // Make sure that the callback is called to populate remoteFetchResult for the mock behaviour.
            Consumer<RemoteLogReadResult> callback = invocationOnMock.getArgument(1);
            callback.accept(remoteFetchResult);
            return CompletableFuture.completedFuture(remoteFetchResult);
        }).when(remoteLogManager).asyncRead(any(), any());
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        Uuid fetchId = Uuid.randomUuid();

        Partition p0 = mock(Partition.class);
        when(p0.isLeader()).thenReturn(true);

        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);

        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0)))
            .withFetchId(fetchId)
            .build());

        // sp0 is acquirable.
        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class)) {
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
            partitionDataMap.put(tp0, mock(ShareFetchResponseData.PartitionData.class));
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any())).thenReturn(partitionDataMap);

            assertFalse(delayedShareFetch.isCompleted());
            assertTrue(delayedShareFetch.tryComplete());

            assertTrue(delayedShareFetch.isCompleted());
            // Pending remote fetch object gets created for delayed share fetch.
            assertNotNull(delayedShareFetch.pendingRemoteFetches());
            // Verify the locks are released for tp0.
            Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp0));
            assertTrue(shareFetch.isCompleted());
            assertEquals(Set.of(tp0), future.join().keySet());
            assertEquals(Errors.NONE.code(), future.join().get(tp0).errorCode());
            assertTrue(delayedShareFetch.lock().tryLock());
            delayedShareFetch.lock().unlock();
        }
    }

    @Test
    public void testRemoteStorageFetchRequestCompletionAlongWithLocalLogRead() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 2));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0, tp1, tp2), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.nextFetchOffset()).thenReturn(10L);
        when(sp1.nextFetchOffset()).thenReturn(20L);
        when(sp2.nextFetchOffset()).thenReturn(30L);

        // Fetch offset does not match with the cached entry for sp0, sp1 and sp2. Hence, a replica manager fetch will happen for all of them in tryComplete.
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp2.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class)) {
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
            partitionDataMap.put(tp0, mock(ShareFetchResponseData.PartitionData.class));
            partitionDataMap.put(tp1, mock(ShareFetchResponseData.PartitionData.class));
            partitionDataMap.put(tp2, mock(ShareFetchResponseData.PartitionData.class));
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any())).thenReturn(partitionDataMap);

            // Mocking local log read result for tp0, tp1 and remote storage read result for tp2 on first replicaManager readFromLog call(from tryComplete).
            // Mocking local log read result for tp0 and tp1 on second replicaManager readFromLog call(from onComplete).
            doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(tp0, tp1), Set.of(tp2))
            ).doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(tp0, tp1), Set.of())
            ).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

            // Remote fetch related mocks. Remote fetch object completes within tryComplete in this mock, hence request will move on to forceComplete.
            RemoteLogReadResult remoteFetchResult = new RemoteLogReadResult(
                Optional.of(REMOTE_FETCH_INFO),
                Optional.empty() // Remote fetch result is returned successfully without error.
            );
            RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
            doAnswer(invocationOnMock -> {
                // Make sure that the callback is called to populate remoteFetchResult for the mock behaviour.
                Consumer<RemoteLogReadResult> callback = invocationOnMock.getArgument(1);
                callback.accept(remoteFetchResult);
                return CompletableFuture.completedFuture(remoteFetchResult);
            }).when(remoteLogManager).asyncRead(any(), any());
            when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

            Partition p0 = mock(Partition.class);
            when(p0.isLeader()).thenReturn(true);

            Partition p1 = mock(Partition.class);
            when(p1.isLeader()).thenReturn(true);

            Partition p2 = mock(Partition.class);
            when(p2.isLeader()).thenReturn(true);

            when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);
            when(replicaManager.getPartitionOrException(tp1.topicPartition())).thenReturn(p1);
            when(replicaManager.getPartitionOrException(tp2.topicPartition())).thenReturn(p2);

            Uuid fetchId = Uuid.randomUuid();
            DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
                .withShareFetchData(shareFetch)
                .withReplicaManagerLogReader(replicaManager)
                .withSharePartitions(sharePartitions)
                .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0, tp1, tp2)))
                .withFetchId(fetchId)
                .build());

            // All the topic partitions are acquirable.
            when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
            when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);
            when(sp2.maybeAcquireFetchLock(fetchId)).thenReturn(true);

            assertFalse(delayedShareFetch.isCompleted());
            assertTrue(delayedShareFetch.tryComplete());

            assertTrue(delayedShareFetch.isCompleted());
            // Pending remote fetch object gets created for delayed share fetch.
            assertNotNull(delayedShareFetch.pendingRemoteFetches());
            // the future of shareFetch completes.
            assertTrue(shareFetch.isCompleted());
            assertEquals(Set.of(tp0, tp1, tp2), future.join().keySet());
            // Verify the locks are released for both local log and remote storage read topic partitions.
            // tp0 and tp1 (local log read) locks are released twice: once when the remote fetch is prepared
            // (as non-remote partitions) and again after they are re-acquired for the additional local read
            // in onComplete. tp2 (remote storage read) lock is released once when the remote fetch completes.
            Mockito.verify(delayedShareFetch, times(2)).releasePartitionLocks(Set.of(tp0, tp1));
            Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp2));
            assertEquals(Errors.NONE.code(), future.join().get(tp0).errorCode());
            assertEquals(Errors.NONE.code(), future.join().get(tp1).errorCode());
            assertEquals(Errors.NONE.code(), future.join().get(tp2).errorCode());
            assertTrue(delayedShareFetch.lock().tryLock());
            delayedShareFetch.lock().unlock();
        }
    }

    /**
     * Tests that when a remote fetch completes alongside a successful additional local read (with data),
     * the remote-fetch completion path does not re-enqueue the local partitions to the action queue.
     * The local partitions' completion is separately triggered by processFetchResultAndComplete, so
     * completeRemoteFetchRequest must only enqueue the remote (acquired) partitions.
     */
    @Test
    public void testRemoteStorageFetchCompletionDoesNotDuplicateLocalPartitionsInActionQueue() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 2));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0, tp1, tp2), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.nextFetchOffset()).thenReturn(10L);
        when(sp1.nextFetchOffset()).thenReturn(20L);
        when(sp2.nextFetchOffset()).thenReturn(30L);
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp2.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class)) {
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
            partitionDataMap.put(tp0, mock(ShareFetchResponseData.PartitionData.class));
            partitionDataMap.put(tp1, mock(ShareFetchResponseData.PartitionData.class));
            partitionDataMap.put(tp2, mock(ShareFetchResponseData.PartitionData.class));
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any())).thenReturn(partitionDataMap);

            // First read (tryComplete): tp0, tp1 local and tp2 remote. Second read (additional local read
            // in onComplete): tp0, tp1 returned WITH data so that they appear in partitionsWithData.
            doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(tp0, tp1), Set.of(tp2))
            ).doAnswer(invocation -> buildLogReadResult(List.of(tp0, tp1))
            ).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

            // Remote fetch object completes within tryComplete so the request moves on to onComplete.
            RemoteLogReadResult remoteFetchResult = new RemoteLogReadResult(Optional.of(REMOTE_FETCH_INFO), Optional.empty());
            RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
            doAnswer(invocationOnMock -> {
                Consumer<RemoteLogReadResult> callback = invocationOnMock.getArgument(1);
                callback.accept(remoteFetchResult);
                return CompletableFuture.completedFuture(remoteFetchResult);
            }).when(remoteLogManager).asyncRead(any(), any());
            when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

            Partition p0 = mock(Partition.class);
            when(p0.isLeader()).thenReturn(true);
            Partition p1 = mock(Partition.class);
            when(p1.isLeader()).thenReturn(true);
            Partition p2 = mock(Partition.class);
            when(p2.isLeader()).thenReturn(true);
            when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);
            when(replicaManager.getPartitionOrException(tp1.topicPartition())).thenReturn(p1);
            when(replicaManager.getPartitionOrException(tp2.topicPartition())).thenReturn(p2);

            // Execute the action queue runnables synchronously so completeDelayedShareFetchRequest is invoked.
            doAnswer(invocation -> {
                Runnable runnable = invocation.getArgument(0);
                runnable.run();
                return null;
            }).when(replicaManager).addToActionQueue(any());

            Uuid fetchId = Uuid.randomUuid();
            DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
                .withShareFetchData(shareFetch)
                .withReplicaManagerLogReader(replicaManager)
                .withSharePartitions(sharePartitions)
                .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0, tp1, tp2)))
                .withFetchId(fetchId)
                .build());

            when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
            when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);
            when(sp2.maybeAcquireFetchLock(fetchId)).thenReturn(true);

            assertTrue(delayedShareFetch.tryComplete());
            assertTrue(delayedShareFetch.isCompleted());
            assertTrue(shareFetch.isCompleted());

            // The local partitions tp0 and tp1 are enqueued twice: once when they are released as
            // non-remote partitions while the remote fetch is prepared in tryComplete, and again
            // after they are re-acquired for the additional local read in onComplete (via
            // processFetchResultAndComplete). The remote-fetch completion (completeRemoteFetchRequest)
            // must not enqueue them a third time - it only enqueues the remote (acquired) partitions.
            Mockito.verify(replicaManager, times(2)).completeDelayedShareFetchRequest(
                new DelayedShareFetchGroupKey(shareFetch.groupId(), tp0.topicId(), tp0.partition()));
            Mockito.verify(replicaManager, times(2)).completeDelayedShareFetchRequest(
                new DelayedShareFetchGroupKey(shareFetch.groupId(), tp1.topicId(), tp1.partition()));
            // The remote partition tp2 is enqueued once, by completeRemoteFetchRequest.
            Mockito.verify(replicaManager, times(1)).completeDelayedShareFetchRequest(
                new DelayedShareFetchGroupKey(shareFetch.groupId(), tp2.topicId(), tp2.partition()));

            // Validate acquire and release locks for the partitions.
            Mockito.verify(sp0, times(2)).maybeAcquireFetchLock(fetchId);
            Mockito.verify(sp1, times(2)).maybeAcquireFetchLock(fetchId);
            Mockito.verify(sp2, times(1)).maybeAcquireFetchLock(fetchId);
            Mockito.verify(sp0, times(2)).releaseFetchLock(fetchId);
            Mockito.verify(sp1, times(2)).releaseFetchLock(fetchId);
            Mockito.verify(sp2, times(1)).releaseFetchLock(fetchId);
        }
    }

    /**
     * On the slow path, tryComplete records partitionsAcquired = ALL acquired partitions (remote and
     * non-remote) before the async local read completes. When the completed read then routes some
     * partitions to a remote fetch, maybeProcessRemoteFetch releases the non-remote partitions and must
     * also remove them from partitionsAcquired.
     */
    @Test
    public void testAsyncReadRemoteFetchRemovesNonRemotePartitionsFromAcquiredOnRelease() {
        String groupId = "grp";
        LogReader logReader = mock(LogReader.class);
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        PartitionMetadataProvider metadataProvider = mock(PartitionMetadataProvider.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0)); // remote
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1)); // non-remote

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp0.nextFetchOffset()).thenReturn(10L);
        when(sp1.nextFetchOffset()).thenReturn(20L);
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            future, List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        // Slow path: the first readAsync stays pending, so tryComplete records partitionsAcquired = {tp0, tp1}
        // and stores the pending fetch. The second readAsync is the additional local read for tp1 in onComplete.
        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> asyncReadFuture = new CompletableFuture<>();
        when(logReader.readAsync(any(), any(), any(), any(), anyBoolean()))
            .thenReturn(asyncReadFuture)
            .thenReturn(CompletableFuture.completedFuture(buildLogReadResultMap(List.of(tp1))));

        Uuid fetchId = Uuid.randomUuid();
        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        // Remote fetch for tp0 completes synchronously so onComplete runs during the second tryComplete.
        RemoteLogReadResult remoteFetchResult = new RemoteLogReadResult(Optional.of(REMOTE_FETCH_INFO), Optional.empty());
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        doAnswer(inv -> {
            Consumer<RemoteLogReadResult> callback = inv.getArgument(1);
            callback.accept(remoteFetchResult);
            return CompletableFuture.completedFuture(remoteFetchResult);
        }).when(remoteLogManager).asyncRead(any(), any());
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        Partition p0 = mock(Partition.class);
        when(p0.isLeader()).thenReturn(true);
        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);

        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManager(replicaManager)
            .withLogReader(logReader)
            .withMetadataProvider(metadataProvider)
            .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0, tp1)))
            .withFetchId(fetchId)
            .build());

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class)) {
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
            partitionDataMap.put(tp0, mock(ShareFetchResponseData.PartitionData.class));
            partitionDataMap.put(tp1, mock(ShareFetchResponseData.PartitionData.class));
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any())).thenReturn(partitionDataMap);

            // First tryComplete: slow path, records partitionsAcquired = {tp0, tp1}.
            assertFalse(delayedShareFetch.tryComplete());
            assertNotNull(delayedShareFetch.pendingFetch());

            Mockito.verify(sp0, times(1)).maybeAcquireFetchLock(fetchId);
            Mockito.verify(sp1, times(1)).maybeAcquireFetchLock(fetchId);
            Mockito.verify(sp0, times(0)).releaseFetchLock(fetchId);
            Mockito.verify(sp1, times(0)).releaseFetchLock(fetchId);
            // Complete the async local read: tp0 is tiered (remote), tp1 is a local partition.
            asyncReadFuture.complete(buildLocalAndRemoteFetchResultMap(Set.of(tp1), Set.of(tp0)));

            // Next tryComplete transitions tp0 to a remote fetch (releasing tp1 as a
            // non-remote partition and removing it from partitionsAcquired), and completes.
            assertTrue(delayedShareFetch.tryComplete());
            assertTrue(delayedShareFetch.isCompleted());
            assertTrue(shareFetch.isCompleted());

            Mockito.verify(sp0, times(1)).maybeAcquireFetchLock(fetchId);
            // tp1 was released as a non-remote partition and removed from partitionsAcquired, so the remote
            // completion re-acquires it for the additional local read (a second maybeAcquireFetchLock) and
            // performs a second readAsync - rather than leaving tp1 in partitionsAcquired.
            Mockito.verify(sp1, times(2)).maybeAcquireFetchLock(fetchId);
            Mockito.verify(sp0, times(1)).releaseFetchLock(fetchId);
            Mockito.verify(sp1, times(2)).releaseFetchLock(fetchId);
            Mockito.verify(logReader, times(2)).readAsync(any(), any(), any(), any(), anyBoolean());
        }
    }

    @Test
    public void testRemoteStorageFetchHappensForAllTopicPartitions() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0, tp1), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        when(sp0.nextFetchOffset()).thenReturn(10L);
        when(sp1.nextFetchOffset()).thenReturn(10L);
        // Fetch offset does not match with the cached entry for sp0 and sp1. Hence, a replica manager fetch will happen for both.
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        LinkedHashSet<TopicIdPartition> remoteStorageFetchPartitions = new LinkedHashSet<>();
        remoteStorageFetchPartitions.add(tp0);
        remoteStorageFetchPartitions.add(tp1);

        // Mocking remote storage read result for tp0 and tp1.
        doAnswer(invocation -> buildLocalAndRemoteFetchResult(Set.of(), remoteStorageFetchPartitions)).when(replicaManager).readFromLog(any(), any(), any(ReplicaQuota.class), anyBoolean());

        // Remote fetch related mocks. Remote fetch object completes within tryComplete in this mock, hence request will move on to forceComplete.
        RemoteLogReadResult remoteFetchResult = new RemoteLogReadResult(
            Optional.of(REMOTE_FETCH_INFO),
            Optional.empty() // Remote fetch result is returned successfully without error.
        );
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        doAnswer(invocationOnMock -> {
            // Make sure that the callback is called to populate remoteFetchResult for the mock behaviour.
            Consumer<RemoteLogReadResult> callback = invocationOnMock.getArgument(1);
            callback.accept(remoteFetchResult);
            return CompletableFuture.completedFuture(remoteFetchResult);
        }).when(remoteLogManager).asyncRead(any(), any());
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        Uuid fetchId = Uuid.randomUuid();

        Partition p0 = mock(Partition.class);
        when(p0.isLeader()).thenReturn(true);

        Partition p1 = mock(Partition.class);
        when(p1.isLeader()).thenReturn(true);

        when(replicaManager.getPartitionOrException(tp0.topicPartition())).thenReturn(p0);
        when(replicaManager.getPartitionOrException(tp1.topicPartition())).thenReturn(p1);

        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManagerLogReader(replicaManager)
            .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0, tp1)))
            .withFetchId(fetchId)
            .build());

        // sp0 and sp1 are acquirable.
        when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
        when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class)) {
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
            partitionDataMap.put(tp0, mock(ShareFetchResponseData.PartitionData.class));
            partitionDataMap.put(tp1, mock(ShareFetchResponseData.PartitionData.class));
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any())).thenReturn(partitionDataMap);

            assertFalse(delayedShareFetch.isCompleted());
            assertTrue(delayedShareFetch.tryComplete());

            assertTrue(delayedShareFetch.isCompleted());
            // Pending remote fetch object gets created for delayed share fetch.
            assertNotNull(delayedShareFetch.pendingRemoteFetches());
            // Verify the locks are released for both tp0 and tp1.
            Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp0, tp1));
            assertTrue(shareFetch.isCompleted());
            // Share fetch response contains both remote storage fetch topic partitions.
            assertEquals(Set.of(tp0, tp1), future.join().keySet());
            assertEquals(Errors.NONE.code(), future.join().get(tp0).errorCode());
            assertEquals(Errors.NONE.code(), future.join().get(tp1).errorCode());
            assertTrue(delayedShareFetch.lock().tryLock());
            delayedShareFetch.lock().unlock();
        }
    }

    @Test
    public void testRemoteStorageFetchCompletionPostRegisteringCallbackByPendingFetchesCompletion() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        SharePartition sp0 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.nextFetchOffset()).thenReturn(10L);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        PendingRemoteFetches pendingRemoteFetches = mock(PendingRemoteFetches.class);
        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions)
            .withPartitionMaxBytesStrategy(PartitionMaxBytesStrategy.type(PartitionMaxBytesStrategy.StrategyType.UNIFORM))
            .withPendingRemoteFetches(pendingRemoteFetches)
            .withFetchId(fetchId)
            .build());

        LinkedHashMap<TopicIdPartition, Long> partitionsAcquired = new LinkedHashMap<>();
        partitionsAcquired.put(tp0, 10L);

        // Manually update acquired partitions.
        delayedShareFetch.updatePartitionsAcquired(partitionsAcquired);

        // Mock remote fetch result.
        RemoteFetch remoteFetch = mock(RemoteFetch.class);
        when(remoteFetch.topicIdPartition()).thenReturn(tp0);
        when(remoteFetch.remoteFetchResult()).thenReturn(CompletableFuture.completedFuture(
            new RemoteLogReadResult(Optional.of(REMOTE_FETCH_INFO), Optional.empty()))
        );
        when(remoteFetch.logReadResult()).thenReturn(new LogReadResult(
            REMOTE_FETCH_INFO,
            Optional.empty(),
            -1L,
            -1L,
            -1L,
            -1L,
            -1L,
            OptionalLong.empty(),
            OptionalInt.empty(),
            Errors.NONE
        ));
        when(pendingRemoteFetches.remoteFetches()).thenReturn(List.of(remoteFetch));
        when(pendingRemoteFetches.isDone()).thenReturn(false);

        // Make sure that the callback is called to complete remote storage share fetch result.
        doAnswer(invocationOnMock -> {
            BiConsumer<Void, Throwable> callback = invocationOnMock.getArgument(0);
            callback.accept(mock(Void.class), null);
            return null;
        }).when(pendingRemoteFetches).invokeCallbackOnCompletion(any());

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class)) {
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
            partitionDataMap.put(tp0, mock(ShareFetchResponseData.PartitionData.class));
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any())).thenReturn(partitionDataMap);

            assertFalse(delayedShareFetch.isCompleted());
            delayedShareFetch.forceComplete();
            assertTrue(delayedShareFetch.isCompleted());
            // the future of shareFetch completes.
            assertTrue(shareFetch.isCompleted());
            assertEquals(Set.of(tp0), future.join().keySet());
            // Verify the locks are released for tp0.
            Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp0));
            assertTrue(delayedShareFetch.outsidePurgatoryCallbackLock());
            assertTrue(delayedShareFetch.lock().tryLock());
            delayedShareFetch.lock().unlock();
        }
    }

    @Test
    public void testRemoteStorageFetchCompletionPostRegisteringCallbackByTimerTaskCompletion() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        SharePartition sp0 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.nextFetchOffset()).thenReturn(10L);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        PendingRemoteFetches pendingRemoteFetches = mock(PendingRemoteFetches.class);
        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions)
            .withPartitionMaxBytesStrategy(PartitionMaxBytesStrategy.type(PartitionMaxBytesStrategy.StrategyType.UNIFORM))
            .withPendingRemoteFetches(pendingRemoteFetches)
            .withFetchId(fetchId)
            .build());

        LinkedHashMap<TopicIdPartition, Long> partitionsAcquired = new LinkedHashMap<>();
        partitionsAcquired.put(tp0, 10L);

        // Manually update acquired partitions.
        delayedShareFetch.updatePartitionsAcquired(partitionsAcquired);

        // Mock remote fetch result.
        RemoteFetch remoteFetch = mock(RemoteFetch.class);
        when(remoteFetch.topicIdPartition()).thenReturn(tp0);
        when(remoteFetch.remoteFetchResult()).thenReturn(CompletableFuture.completedFuture(
            new RemoteLogReadResult(Optional.of(REMOTE_FETCH_INFO), Optional.empty()))
        );
        when(remoteFetch.logReadResult()).thenReturn(new LogReadResult(
            REMOTE_FETCH_INFO,
            Optional.empty(),
            -1L,
            -1L,
            -1L,
            -1L,
            -1L,
            OptionalLong.empty(),
            OptionalInt.empty(),
            Errors.NONE
        ));
        when(pendingRemoteFetches.remoteFetches()).thenReturn(List.of(remoteFetch));
        when(pendingRemoteFetches.isDone()).thenReturn(false);

        // Make sure that the callback to complete remote storage share fetch result is not called.
        doAnswer(invocationOnMock -> null).when(pendingRemoteFetches).invokeCallbackOnCompletion(any());

        // Mock the behaviour of replica manager such that remote storage fetch completion timer task completes on adding it to the watch queue.
        doAnswer(invocationOnMock -> {
            TimerTask timerTask = invocationOnMock.getArgument(0);
            timerTask.run();
            return null;
        }).when(replicaManager).addShareFetchTimerRequest(any());

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class)) {
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
            partitionDataMap.put(tp0, mock(ShareFetchResponseData.PartitionData.class));
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any())).thenReturn(partitionDataMap);

            assertFalse(delayedShareFetch.isCompleted());
            delayedShareFetch.forceComplete();
            assertTrue(delayedShareFetch.isCompleted());
            // the future of shareFetch completes.
            assertTrue(shareFetch.isCompleted());
            assertEquals(Set.of(tp0), future.join().keySet());
            // Verify the locks are released for tp0.
            Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp0));
            assertTrue(delayedShareFetch.outsidePurgatoryCallbackLock());
            assertTrue(delayedShareFetch.lock().tryLock());
            delayedShareFetch.lock().unlock();
        }
    }

    @Test
    public void testRemoteStorageFetchCompletionNotTriggerActionsQueue() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        SharePartition sp0 = mock(SharePartition.class);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.nextFetchOffset()).thenReturn(10L);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS,
            BROKER_TOPIC_STATS);

        PendingRemoteFetches pendingRemoteFetches = mock(PendingRemoteFetches.class);
        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManagerLogReader(replicaManager)
            .withSharePartitions(sharePartitions)
            .withPartitionMaxBytesStrategy(PartitionMaxBytesStrategy.type(PartitionMaxBytesStrategy.StrategyType.UNIFORM))
            .withPendingRemoteFetches(pendingRemoteFetches)
            .withFetchId(fetchId)
            .build());

        LinkedHashMap<TopicIdPartition, Long> partitionsAcquired = new LinkedHashMap<>();
        partitionsAcquired.put(tp0, 10L);
        // Manually update acquired partitions maps.
        delayedShareFetch.updatePartitionsAcquired(partitionsAcquired);

        // Mock remote fetch result.
        RemoteFetch remoteFetch = mock(RemoteFetch.class);
        when(remoteFetch.topicIdPartition()).thenReturn(tp0);
        when(remoteFetch.remoteFetchResult()).thenReturn(CompletableFuture.completedFuture(
            new RemoteLogReadResult(Optional.of(REMOTE_FETCH_INFO), Optional.empty()))
        );
        when(remoteFetch.logReadResult()).thenReturn(new LogReadResult(
            REMOTE_FETCH_INFO,
            Optional.empty(),
            -1L,
            -1L,
            -1L,
            -1L,
            -1L,
            OptionalLong.empty(),
            OptionalInt.empty(),
            Errors.NONE
        ));
        when(pendingRemoteFetches.remoteFetches()).thenReturn(List.of(remoteFetch));
        when(pendingRemoteFetches.isDone()).thenReturn(false);

        doAnswer(invocationOnMock -> null).when(pendingRemoteFetches).invokeCallbackOnCompletion(any());
        // Mock the behaviour of replica manager such that remote storage fetch completion timer task completes on adding it to the watch queue.
        doAnswer(invocationOnMock -> {
            TimerTask timerTask = invocationOnMock.getArgument(0);
            timerTask.run();
            return null;
        }).when(replicaManager).addShareFetchTimerRequest(any());

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class)) {
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
            // 0 bytes fetched data for tp0.
            ShareFetchResponseData.PartitionData mockedPartitionData0 = mock(ShareFetchResponseData.PartitionData.class);
            when(mockedPartitionData0.records()).thenReturn(MemoryRecords.EMPTY);
            partitionDataMap.put(tp0, mockedPartitionData0);
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any())).thenReturn(partitionDataMap);

            assertFalse(delayedShareFetch.isCompleted());
            delayedShareFetch.forceComplete();
            assertTrue(delayedShareFetch.isCompleted());
            // The future of shareFetch completes.
            assertTrue(shareFetch.isCompleted());
            assertEquals(Set.of(tp0), future.join().keySet());
            // Verify the locks are released for tp0 but not added to action queue since no records are fetched from remote storage.
            Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp0));
            Mockito.verify(replicaManager, times(0)).addToActionQueue(any());
            assertTrue(delayedShareFetch.outsidePurgatoryCallbackLock());
            assertTrue(delayedShareFetch.lock().tryLock());
            delayedShareFetch.lock().unlock();
        }
    }

    @Test
    public void testTryCompleteAsyncCompletion() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        LogReader logReader = mock(LogReader.class);
        PartitionMetadataProvider metadataProvider = mock(PartitionMetadataProvider.class);
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> asyncReadFuture = new CompletableFuture<>();
        when(logReader.readAsync(any(), any(), any(), any(), anyBoolean())).thenReturn(asyncReadFuture);

        when(sp0.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.nextFetchOffset()).thenReturn(0L);
        when(sp0.fetchOffsetMetadata(anyLong()))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        when(metadataProvider.endOffsetMetadata(any(), any())).thenReturn(new LogOffsetMetadata(1, 1, 1));

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withSharePartitions(sharePartitions)
            .withReplicaManager(replicaManager)
            .withLogReader(logReader)
            .withMetadataProvider(metadataProvider)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withFetchId(fetchId)
            .build();

        // Pending fetch shall be null as of now.
        assertNull(delayedShareFetch.pendingFetch());
        // Async read should create pending fetch as the future is not completed yet
        assertFalse(delayedShareFetch.tryComplete());
        // Validate pending fetch is created.
        assertNotNull(delayedShareFetch.pendingFetch());
        // Validate locks are held.
        Mockito.verify(sp0, times(0)).releaseFetchLock(any());
        // Invoke tryComplete again to simulate async read completion but should return false as future
        // is not yet completed.
        assertFalse(delayedShareFetch.tryComplete());
        // The result future should still not be completed.
        assertFalse(shareFetch.isCompleted());
        // Complete future to simulate async read completion. The registered callback should trigger a
        // purgatory check rather than completing the request directly on the completing thread.
        asyncReadFuture.complete(buildLogReadResultMap(List.of(tp0)));
        // Verify the callback handler executed the request completion.
        Mockito.verify(replicaManager).completeDelayedShareFetchRequest(any());
        // The request should not be completed yet as the purgatory check (mocked) is a no-op here.
        assertFalse(delayedShareFetch.isCompleted());
        // Simulate the purgatory re-invoking tryComplete now that the async read is done.
        assertTrue(delayedShareFetch.tryComplete());
        // Force complete should have been executed and the request must be completed.
        assertTrue(delayedShareFetch.isCompleted());

        // Verify readFromLog was called (which is used by async readAsync internally)
        Mockito.verify(logReader).readAsync(any(), any(), any(), any(), anyBoolean());
        Mockito.verify(sp0).releaseFetchLock(fetchId);
        // The future of shareFetch completes.
        assertTrue(shareFetch.isCompleted());
        assertEquals(Set.of(tp0), future.join().keySet());
    }

    /**
     * Tests that when async read encounters an exception, request completes with error.
     */
    @Test
    public void testAsyncReadExceptionReleasesLocks() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        LogReader logReader = mock(LogReader.class);
        PartitionMetadataProvider metadataProvider = mock(PartitionMetadataProvider.class);
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> asyncReadFuture = new CompletableFuture<>();
        when(logReader.readAsync(any(), any(), any(), any(), anyBoolean())).thenReturn(asyncReadFuture);

        when(sp0.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.nextFetchOffset()).thenReturn(0L);
        when(sp0.fetchOffsetMetadata(anyLong()))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        when(metadataProvider.endOffsetMetadata(any(), any())).thenReturn(new LogOffsetMetadata(1, 1, 1));

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManager(replicaManager)
            .withLogReader(logReader)
            .withMetadataProvider(metadataProvider)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withSharePartitions(sharePartitions)
            .withFetchId(fetchId)
            .build();

        // Initially no pending fetch
        assertNull(delayedShareFetch.pendingFetch());
        // First tryComplete triggers async read
        assertFalse(delayedShareFetch.tryComplete());
        assertNotNull(delayedShareFetch.pendingFetch());
        // Complete with exception.
        asyncReadFuture.completeExceptionally(new RuntimeException("Simulated read failure"));
        Mockito.verify(replicaManager).completeDelayedShareFetchRequest(any());
        // The request should not be completed yet as the purgatory check (mocked) is a no-op here.
        assertFalse(delayedShareFetch.isCompleted());

        // Simulate the purgatory re-invoking tryComplete. get() throws, so the request is force-completed
        // with an error and the partition locks are released.
        assertTrue(delayedShareFetch.tryComplete());
        assertTrue(delayedShareFetch.isCompleted());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> response = future.join();
        assertEquals(Set.of(tp0), response.keySet());
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), response.get(tp0).errorCode());
        assertTrue(response.get(tp0).errorMessage().contains("Simulated read failure"));
        Mockito.verify(sp0).releaseFetchLock(fetchId);
    }

    /**
     * Tests that when the async read fails and the error handling in completeShareFetchAsyncRequest
     * itself throws (here the exception handler callback), the partition locks are still released
     * This guards the try-finally around the metric recording and exception handling in the get()-failure
     * path.
     */
    @Test
    public void testCompleteShareFetchAsyncRequestReleasesLocksWhenExceptionHandlingThrows() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        LogReader logReader = mock(LogReader.class);
        PartitionMetadataProvider metadataProvider = mock(PartitionMetadataProvider.class);
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> asyncReadFuture = new CompletableFuture<>();
        when(logReader.readAsync(any(), any(), any(), any(), anyBoolean())).thenReturn(asyncReadFuture);

        when(sp0.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.nextFetchOffset()).thenReturn(0L);
        when(sp0.fetchOffsetMetadata(anyLong()))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        when(metadataProvider.endOffsetMetadata(any(), any())).thenReturn(new LogOffsetMetadata(1, 1, 1));

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));

        // The exception handler throws while handling the async read failure, simulating a failure
        // inside the catch block's error handling. Locks must still be released via the finally block.
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        doThrow(new RuntimeException("handler boom")).when(exceptionHandler).accept(any(), any());

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withReplicaManager(replicaManager)
            .withLogReader(logReader)
            .withMetadataProvider(metadataProvider)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withSharePartitions(sharePartitions)
            .withExceptionHandler(exceptionHandler)
            .withFetchId(fetchId)
            .build();

        // First tryComplete triggers the async read, which stays pending (slow path).
        assertFalse(delayedShareFetch.tryComplete());
        assertNotNull(delayedShareFetch.pendingFetch());
        // Complete the async read exceptionally.
        asyncReadFuture.completeExceptionally(new RuntimeException("Simulated read failure"));

        // Purgatory re-invokes tryComplete; get() throws, and the error handling (exception handler)
        // then throws too. The finally block must still release the partition locks before the throw propagates.
        assertThrows(RuntimeException.class, delayedShareFetch::tryComplete);
        Mockito.verify(sp0).releaseFetchLock(fetchId);
    }

    /**
     * Tests that onComplete handles the case where async fetch is still pending.
     * This simulates timeout scenario where async read hasn't completed yet.
     */
    @Test
    public void testOnCompleteWithPendingAsyncFetch() {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        LogReader logReader = mock(LogReader.class);
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        PartitionMetadataProvider metadataProvider = mock(PartitionMetadataProvider.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> asyncReadFuture = new CompletableFuture<>();
        when(logReader.readAsync(any(), any(), any(), any(), anyBoolean())).thenReturn(asyncReadFuture);

        when(sp0.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.nextFetchOffset()).thenReturn(0L);
        when(sp0.fetchOffsetMetadata(anyLong()))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        when(metadataProvider.endOffsetMetadata(any(), any())).thenReturn(new LogOffsetMetadata(1, 1, 1));

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withLogReader(logReader)
            .withMetadataProvider(metadataProvider)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withSharePartitions(sharePartitions)
            .withReplicaManager(replicaManager)
            .withFetchId(fetchId)
            .build();

        // Initially no pending fetch
        assertNull(delayedShareFetch.pendingFetch());
        // First tryComplete triggers async read
        assertFalse(delayedShareFetch.tryComplete());
        assertNotNull(delayedShareFetch.pendingFetch());
        // Simulate timeout: onComplete is invoked while the async read is still in-flight (not done).
        delayedShareFetch.forceComplete();
        // As the async read had not completed, the request is completed with an empty response, the
        // pending fetch is cleared and the partition locks are released.
        assertNull(delayedShareFetch.pendingFetch());
        assertTrue(shareFetch.isCompleted());
        assertTrue(future.isDone());
        assertEquals(Set.of(), future.join().keySet());
        Mockito.verify(sp0).releaseFetchLock(fetchId);
        // Pending fetch is marked to null.
        assertNull(delayedShareFetch.pendingFetch());

        // Complete the pending fetch successfully to validate that it doesn't invoke the callback.
        asyncReadFuture.complete(buildLogReadResultMap(List.of(tp0)));
        // Verify the callback handler should not be executed on the request completion now.
        Mockito.verify(replicaManager, times(0)).completeDelayedShareFetchRequest(any());
    }

    /**
     * Tests that onComplete handles the case where async fetch is just completed, on timeout.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testOnCompleteWithCompletedAsyncFetch() throws Exception {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        LogReader logReader = mock(LogReader.class);
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        PartitionMetadataProvider metadataProvider = mock(PartitionMetadataProvider.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> asyncReadFuture = mock(CompletableFuture.class);
        when(logReader.readAsync(any(), any(), any(), any(), anyBoolean())).thenReturn(asyncReadFuture);

        when(sp0.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.nextFetchOffset()).thenReturn(0L);
        when(sp0.fetchOffsetMetadata(anyLong()))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        when(metadataProvider.endOffsetMetadata(any(), any())).thenReturn(new LogOffsetMetadata(1, 1, 1));

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));

        // Run false for tryComplete and true for onComplete.
        when(asyncReadFuture.isDone()).thenReturn(false).thenReturn(true);
        // Teurn successful response when fetched in onComplete.
        when(asyncReadFuture.get()).thenReturn(buildLogReadResultMap(List.of(tp0)));

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withLogReader(logReader)
            .withMetadataProvider(metadataProvider)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withSharePartitions(sharePartitions)
            .withReplicaManager(replicaManager)
            .withFetchId(fetchId)
            .build();

        // Initially no pending fetch
        assertNull(delayedShareFetch.pendingFetch());
        // First tryComplete triggers async read
        assertFalse(delayedShareFetch.tryComplete());
        assertNotNull(delayedShareFetch.pendingFetch());
        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedShareFetchUtils.when(
                    () -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any()))
                .thenReturn(Map.of(tp0, mock(ShareFetchResponseData.PartitionData.class)));
            // Simulate timeout: onComplete is invoked while the async read is still in-flight (not done).
            delayedShareFetch.forceComplete();
            // As the async read has completed now since second invocation to isDone returns true from
            // mocked response, the request is completed with data.
            assertNull(delayedShareFetch.pendingFetch());
            assertTrue(shareFetch.isCompleted());
            assertTrue(future.isDone());
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> response = future.join();
            assertEquals(Set.of(tp0), response.keySet());
            // Request should be completed without any error. Do not validate the response as it's returned
            // from mocked ShareFetchUtils.
            assertEquals(Errors.NONE.code(), response.get(tp0).errorCode());
            Mockito.verify(sp0).releaseFetchLock(fetchId);
            // Pending fetch is marked to null.
            assertNull(delayedShareFetch.pendingFetch());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAsyncReadMinBytesNotSatisfiedClearsAcquiredStateForRetry() throws Exception {
        // On next re-try, tryComplete() must not assume the released locks are still held. Since the acquired
        // state was cleared, it re-acquires the partitions afresh (a second maybeAcquireFetchLock).
        verifyAsyncReadMinBytesNotSatisfiedClearsAcquiredState(false);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAsyncReadMinBytesNotSatisfiedClearsAcquiredStateForOnComplete() throws Exception {
        // On timeout, onComplete() must not assume the released locks are still held. Since the acquired
        // state was cleared, it re-acquires the partitions afresh (a second maybeAcquireFetchLock).
        verifyAsyncReadMinBytesNotSatisfiedClearsAcquiredState(true);
    }

    private void verifyAsyncReadMinBytesNotSatisfiedClearsAcquiredState(boolean requestTimedOut) throws Exception {
        String groupId = "grp";
        Uuid topicId = Uuid.randomUuid();
        LogReader logReader = mock(LogReader.class);
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        PartitionMetadataProvider metadataProvider = mock(PartitionMetadataProvider.class);
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));

        SharePartition sp0 = mock(SharePartition.class);
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        // Set a high minBytes so that the async read result does not satisfy minBytes and the request
        // is returned to the purgatory.
        FetchParams fetchParams = new FetchParams(FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS, 5000, 10000,
            FetchIsolation.HIGH_WATERMARK, Optional.empty());
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(fetchParams, groupId, Uuid.randomUuid().toString(),
            future, List.of(tp0), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> asyncReadFuture = mock(CompletableFuture.class);
        when(logReader.readAsync(any(), any(), any(), any(), anyBoolean())).thenReturn(asyncReadFuture);
        // isDone returns false for the first tryComplete (store pending fetch) and true for the second
        // tryComplete (process the completed result).
        when(asyncReadFuture.isDone()).thenReturn(false).thenReturn(true);
        when(asyncReadFuture.get()).thenReturn(buildLogReadResultMap(List.of(tp0)));

        when(sp0.maybeAcquireFetchLock(any())).thenReturn(true);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp0.nextFetchOffset()).thenReturn(0L);
        when(sp0.fetchOffsetMetadata(anyLong()))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(new LogOffsetMetadata(0, 1, 0)));
        when(metadataProvider.endOffsetMetadata(any(), any())).thenReturn(new LogOffsetMetadata(1, 1, 1));

        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mockPartitionMaxBytes(Set.of(tp0));

        Uuid fetchId = Uuid.randomUuid();
        DelayedShareFetch delayedShareFetch = DelayedShareFetchBuilder.builder()
            .withShareFetchData(shareFetch)
            .withLogReader(logReader)
            .withMetadataProvider(metadataProvider)
            .withPartitionMaxBytesStrategy(partitionMaxBytesStrategy)
            .withSharePartitions(sharePartitions)
            .withReplicaManager(replicaManager)
            .withFetchId(fetchId)
            .build();

        // First tryComplete: async read not done, so a pending fetch is created and tp0 lock is held
        // (partitionsAcquired is populated internally).
        assertNull(delayedShareFetch.pendingFetch());
        assertFalse(delayedShareFetch.tryComplete());
        assertNotNull(delayedShareFetch.pendingFetch());
        Mockito.verify(sp0, times(1)).maybeAcquireFetchLock(fetchId);

        // Second tryComplete (purgatory re-check): async read is done but minBytes is not satisfied, so
        // the lock is released, the pending fetch is cleared and the request returns to the purgatory.
        assertFalse(delayedShareFetch.tryComplete());
        assertNull(delayedShareFetch.pendingFetch());
        Mockito.verify(sp0, times(1)).releaseFetchLock(fetchId);

        // Trigger onComplete() if requestTimedOut else tryComplete(). The re-try must not assume the
        // released locks are still held. Since the acquired state was cleared, it re-acquires the
        // partitions afresh (a second maybeAcquireFetchLock).
        if (requestTimedOut) {
            delayedShareFetch.forceComplete();
        } else {
            delayedShareFetch.tryComplete();
        }
        Mockito.verify(sp0, times(2)).maybeAcquireFetchLock(fetchId);
    }

    @Test
    public void testRemoteStorageFetchLocalReadFailureReleasesLocks() {
        verifyRemoteStorageFetchLocalReadFailureReleasesLocks(true);
    }

    @Test
    public void testRemoteStorageFetchLocalReadCompletionFailureReleasesLocks() {
        verifyRemoteStorageFetchLocalReadFailureReleasesLocks(false);
    }

    private void verifyRemoteStorageFetchLocalReadFailureReleasesLocks(boolean syncFailure) {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        LogReader logReader = mock(LogReader.class);
        PartitionMetadataProvider metadataProvider = mock(PartitionMetadataProvider.class);
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 2));

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);
        SharePartition sp2 = mock(SharePartition.class);
        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(true);
        when(sp2.canAcquireRecords()).thenReturn(true);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);
        sharePartitions.put(tp2, sp2);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, "grp", Uuid.randomUuid().toString(),
            future, List.of(tp0, tp1, tp2), BATCH_OPTIMIZED, BATCH_SIZE, MAX_FETCH_RECORDS, BROKER_TOPIC_STATS);

        when(sp0.nextFetchOffset()).thenReturn(10L);
        when(sp1.nextFetchOffset()).thenReturn(20L);
        when(sp2.nextFetchOffset()).thenReturn(30L);
        when(sp0.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp1.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());
        when(sp2.fetchOffsetMetadata(anyLong())).thenReturn(Optional.empty());

        // First read (tryComplete): tp0, tp1 local and tp2 remote fetch. The second read (additional local
        // read in onComplete) fails, exercising an error path in readAndProcessFetchResultCompletion:
        // either synchronously (readAsync throws) or asynchronously (an exceptionally-completed future).
        LinkedHashMap<TopicIdPartition, LogReadResult> firstReadResult = buildLocalAndRemoteFetchResultMap(Set.of(tp0, tp1), Set.of(tp2));
        if (syncFailure) {
            when(logReader.readAsync(any(), any(), any(), any(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(firstReadResult))
                .thenThrow(new RuntimeException("sync local read failure"));
        } else {
            when(logReader.readAsync(any(), any(), any(), any(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(firstReadResult))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("async local read failure")));
        }

        try (MockedStatic<ShareFetchUtils> mockedShareFetchUtils = Mockito.mockStatic(ShareFetchUtils.class)) {
            mockedShareFetchUtils.when(() -> ShareFetchUtils.processFetchResponse(any(), any(), any(), any(), any()))
                .thenReturn(Map.of(tp2, mock(ShareFetchResponseData.PartitionData.class)));

            // Remote fetch for tp2 completes within tryComplete so the request moves on to
            // onComplete where the additional local read is attempted.
            RemoteLogReadResult remoteFetchResult = new RemoteLogReadResult(Optional.of(REMOTE_FETCH_INFO), Optional.empty());
            RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
            doAnswer(invocationOnMock -> {
                Consumer<RemoteLogReadResult> callback = invocationOnMock.getArgument(1);
                callback.accept(remoteFetchResult);
                return CompletableFuture.completedFuture(remoteFetchResult);
            }).when(remoteLogManager).asyncRead(any(), any());
            when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

            // maybeCompletePendingRemoteFetch checks partition leadership for the remote fetch partition.
            Partition p2 = mock(Partition.class);
            when(p2.isLeader()).thenReturn(true);
            when(replicaManager.getPartitionOrException(tp2.topicPartition())).thenReturn(p2);

            Uuid fetchId = Uuid.randomUuid();
            DelayedShareFetch delayedShareFetch = spy(DelayedShareFetchBuilder.builder()
                .withShareFetchData(shareFetch)
                .withReplicaManager(replicaManager)
                .withLogReader(logReader)
                .withMetadataProvider(metadataProvider)
                .withSharePartitions(sharePartitions)
                .withPartitionMaxBytesStrategy(mockPartitionMaxBytes(Set.of(tp0, tp1, tp2)))
                .withFetchId(fetchId)
                .build());

            when(sp0.maybeAcquireFetchLock(fetchId)).thenReturn(true);
            when(sp1.maybeAcquireFetchLock(fetchId)).thenReturn(true);
            when(sp2.maybeAcquireFetchLock(fetchId)).thenReturn(true);

            assertTrue(delayedShareFetch.tryComplete());
            assertTrue(delayedShareFetch.isCompleted());
            assertTrue(shareFetch.isCompleted());

            // tp0 and tp1 (local partitions being fetched) locks must be released even though the
            // additional local read failed: once when the remote fetch is prepared (as non-remote
            // partitions) and once in the error path. tp2 (remote fetch) lock is released once.
            Mockito.verify(delayedShareFetch, times(2)).releasePartitionLocks(Set.of(tp0, tp1));
            Mockito.verify(delayedShareFetch, times(1)).releasePartitionLocks(Set.of(tp2));
        }
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

    private void mockTopicIdPartitionFetchBytes(LogOffsetMetadata hwmOffsetMetadata, Partition partition) {
        LogOffsetSnapshot endOffsetSnapshot = new LogOffsetSnapshot(1, mock(LogOffsetMetadata.class),
            hwmOffsetMetadata, mock(LogOffsetMetadata.class));
        when(partition.fetchOffsetSnapshot(any(), anyBoolean())).thenReturn(endOffsetSnapshot);
    }

    private PartitionMaxBytesStrategy mockPartitionMaxBytes(Set<TopicIdPartition> partitions) {
        PartitionMaxBytesStrategy partitionMaxBytesStrategy = mock(PartitionMaxBytesStrategy.class);
        LinkedHashMap<TopicIdPartition, Integer> maxBytes = new LinkedHashMap<>();
        partitions.forEach(partition -> maxBytes.put(partition, 1));
        when(partitionMaxBytesStrategy.maxBytes(anyInt(), any(), anyInt())).thenReturn(maxBytes);
        return partitionMaxBytesStrategy;
    }

    private Seq<Tuple2<TopicIdPartition, LogReadResult>> buildLocalAndRemoteFetchResult(
        Set<TopicIdPartition> localLogReadTopicIdPartitions,
        Set<TopicIdPartition> remoteReadTopicIdPartitions) {
        List<Tuple2<TopicIdPartition, LogReadResult>> logReadResults = new ArrayList<>();
        localLogReadTopicIdPartitions.forEach(topicIdPartition -> logReadResults.add(new Tuple2<>(topicIdPartition, new LogReadResult(
            new FetchDataInfo(new LogOffsetMetadata(0, 0, 0), MemoryRecords.EMPTY),
            Optional.empty(),
            -1L,
            -1L,
            -1L,
            -1L,
            -1L,
            OptionalLong.empty(),
            OptionalInt.empty(),
            Errors.NONE
        ))));
        remoteReadTopicIdPartitions.forEach(topicIdPartition -> logReadResults.add(new Tuple2<>(topicIdPartition, new LogReadResult(
            REMOTE_FETCH_INFO,
            Optional.empty(),
            -1L,
            -1L,
            -1L,
            -1L,
            -1L,
            OptionalLong.empty(),
            OptionalInt.empty(),
            Errors.NONE
        ))));
        return CollectionConverters.asScala(logReadResults).toSeq();
    }

    private LinkedHashMap<TopicIdPartition, LogReadResult> buildLocalAndRemoteFetchResultMap(
        Set<TopicIdPartition> localLogReadTopicIdPartitions,
        Set<TopicIdPartition> remoteReadTopicIdPartitions) {
        return CollectionConverters.asJava(buildLocalAndRemoteFetchResult(localLogReadTopicIdPartitions, remoteReadTopicIdPartitions)).stream()
            .collect(Collectors.toMap(
                Tuple2::_1,
                Tuple2::_2,
                (oldValue, newValue) -> oldValue,
                LinkedHashMap::new
            ));
    }

    @SuppressWarnings("unchecked")
    private static BiConsumer<SharePartitionKey, Throwable> mockExceptionHandler() {
        return mock(BiConsumer.class);
    }

    private static LinkedHashMap<TopicIdPartition, LogReadResult> buildLogReadResultMap(List<TopicIdPartition> topicIdPartitions) {
        return CollectionConverters.asJava(buildLogReadResult(topicIdPartitions)).stream()
            .collect(Collectors.toMap(
                Tuple2::_1,
                Tuple2::_2,
                (oldValue, newValue) -> oldValue,
                LinkedHashMap::new
            ));
    }

    @SuppressWarnings("unchecked")
    static class DelayedShareFetchBuilder {
        private ShareFetch shareFetch = mock(ShareFetch.class);
        private ReplicaManager replicaManager = mock(ReplicaManager.class);
        private LogReader logReader = mock(LogReader.class);
        private PartitionMetadataProvider metadataProvider = mock(PartitionMetadataProvider.class);
        private BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mockExceptionHandler();
        private LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = mock(LinkedHashMap.class);
        private PartitionMaxBytesStrategy partitionMaxBytesStrategy = mock(PartitionMaxBytesStrategy.class);
        private Time time = new MockTime();
        private Optional<PendingRemoteFetches> pendingRemoteFetches = Optional.empty();
        private ShareGroupMetrics shareGroupMetrics = mock(ShareGroupMetrics.class);
        private Uuid fetchId = Uuid.randomUuid();

        DelayedShareFetchBuilder withShareFetchData(ShareFetch shareFetch) {
            this.shareFetch = shareFetch;
            return this;
        }

        DelayedShareFetchBuilder withReplicaManagerLogReader(ReplicaManager replicaManager) {
            this.replicaManager = replicaManager;
            this.logReader = new ReplicaManagerLogReader(replicaManager);
            this.metadataProvider = new ReplicaManagerPartitionMetadataProvider(replicaManager);
            return this;
        }

        DelayedShareFetchBuilder withReplicaManager(ReplicaManager replicaManager) {
            this.replicaManager = replicaManager;
            return this;
        }

        DelayedShareFetchBuilder withLogReader(LogReader logReader) {
            this.logReader = logReader;
            return this;
        }

        DelayedShareFetchBuilder withMetadataProvider(PartitionMetadataProvider metadataProvider) {
            this.metadataProvider = metadataProvider;
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

        DelayedShareFetchBuilder withPartitionMaxBytesStrategy(PartitionMaxBytesStrategy partitionMaxBytesStrategy) {
            this.partitionMaxBytesStrategy = partitionMaxBytesStrategy;
            return this;
        }

        private DelayedShareFetchBuilder withShareGroupMetrics(ShareGroupMetrics shareGroupMetrics) {
            this.shareGroupMetrics = shareGroupMetrics;
            return this;
        }

        private DelayedShareFetchBuilder withTime(Time time) {
            this.time = time;
            return this;
        }

        private DelayedShareFetchBuilder withPendingRemoteFetches(PendingRemoteFetches pendingRemoteFetches) {
            this.pendingRemoteFetches = Optional.of(pendingRemoteFetches);
            return this;
        }

        private DelayedShareFetchBuilder withFetchId(Uuid fetchId) {
            this.fetchId = fetchId;
            return this;
        }

        public static DelayedShareFetchBuilder builder() {
            return new DelayedShareFetchBuilder();
        }

        public DelayedShareFetch build() {
            return new DelayedShareFetch(
                shareFetch,
                replicaManager,
                logReader,
                metadataProvider,
                exceptionHandler,
                sharePartitions,
                partitionMaxBytesStrategy,
                shareGroupMetrics,
                time,
                pendingRemoteFetches,
                fetchId,
                REMOTE_FETCH_MAX_WAIT_MS);
        }
    }
}
