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
import kafka.server.ReplicaQuota;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.storage.internals.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;


public class DelayedShareFetchTest {
    private static final int PARTITION_MAX_BYTES = 40000;
    private static final int MAX_WAIT_MS = 5000;

    @Test
    public void testDelayedShareFetchTryCompleteReturnsFalse() {
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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = new DelayedShareFetch(shareFetchPartitionData, mock(ReplicaManager.class), partitionCacheMap);
        // Since there is no partition that can be acquired, tryComplete should return false.
        assertFalse(delayedShareFetch.tryComplete());
        assertFalse(delayedShareFetch.isCompleted());
    }

    @Test
    public void testDelayedShareFetchTryCompleteReturnsTrue() {
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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = new DelayedShareFetch(shareFetchPartitionData, mock(ReplicaManager.class), partitionCacheMap);
        assertFalse(delayedShareFetch.isCompleted());
        // Since sp1 can be acquired, tryComplete should return true.
        assertTrue(delayedShareFetch.tryComplete());
        assertTrue(delayedShareFetch.isCompleted());
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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(false);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = new DelayedShareFetch(shareFetchPartitionData, replicaManager, partitionCacheMap);
        assertFalse(delayedShareFetch.isCompleted());
        delayedShareFetch.forceComplete();

        // Since no partition could be acquired, the future should be empty and replicaManager.fetchMessages should not be called.
        assertEquals(0, shareFetchPartitionData.future().join().size());
        Mockito.verify(replicaManager, times(0)).fetchMessages(
                any(), any(), any(ReplicaQuota.class), any());
        assertTrue(delayedShareFetch.isCompleted());
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

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, MAX_WAIT_MS,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, Uuid.randomUuid().toString(),
                new CompletableFuture<>(), partitionMaxBytes);

        when(sp0.canAcquireRecords()).thenReturn(true);
        when(sp1.canAcquireRecords()).thenReturn(false);
        DelayedShareFetch delayedShareFetch = new DelayedShareFetch(shareFetchPartitionData, replicaManager, partitionCacheMap);
        assertFalse(delayedShareFetch.isCompleted());
        delayedShareFetch.forceComplete();

        // Since we can acquire records from sp0, replicaManager.fetchMessages should be called once and only for sp0.
        Mockito.verify(replicaManager, times(1)).fetchMessages(
                any(), any(), any(ReplicaQuota.class), any());
        Mockito.verify(sp0, times(1)).nextFetchOffset();
        Mockito.verify(sp1, times(0)).nextFetchOffset();
        assertTrue(delayedShareFetch.isCompleted());
    }

    @Test
    public void testProcessFetchResponse() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.nextFetchOffset()).thenReturn((long) 3);
        when(sp1.nextFetchOffset()).thenReturn((long) 3);

        when(sp0.acquire(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1))));
        when(sp1.acquire(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1))));

        doNothing().when(sp1).updateCacheAndOffsets(any(Long.class));
        doNothing().when(sp0).updateCacheAndOffsets(any(Long.class));

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, memberId,
                new CompletableFuture<>(), partitionMaxBytes);

        DelayedShareFetch delayedShareFetch = new DelayedShareFetch(shareFetchPartitionData, mock(ReplicaManager.class), partitionCacheMap);

        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("0".getBytes(), "v".getBytes()),
            new SimpleRecord("1".getBytes(), "v".getBytes()),
            new SimpleRecord("2".getBytes(), "v".getBytes()),
            new SimpleRecord(null, "value".getBytes()));

        MemoryRecords records1 = MemoryRecords.withRecords(100L, Compression.NONE,
            new SimpleRecord("0".getBytes(), "v".getBytes()),
            new SimpleRecord("1".getBytes(), "v".getBytes()),
            new SimpleRecord("2".getBytes(), "v".getBytes()),
            new SimpleRecord(null, "value".getBytes()));

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData = new ArrayList<>();
        responseData.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
            records, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        responseData.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 100L,
            records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result =
            delayedShareFetch.processFetchResponse(shareFetchPartitionData, responseData);

        assertTrue(result.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData = result.join();
        assertEquals(2, resultData.size());
        assertTrue(resultData.containsKey(tp0));
        assertTrue(resultData.containsKey(tp1));
        assertEquals(0, resultData.get(tp0).partitionIndex());
        assertEquals(1, resultData.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData.get(tp1).errorCode());
        assertEquals(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)),
            resultData.get(tp0).acquiredRecords());
        assertEquals(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1)),
            resultData.get(tp1).acquiredRecords());
    }

    @Test
    public void testProcessFetchResponseWithEmptyRecords() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.nextFetchOffset()).thenReturn((long) 3);
        when(sp1.nextFetchOffset()).thenReturn((long) 3);

        when(sp0.acquire(any(), any())).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
        when(sp1.acquire(any(), any())).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        doNothing().when(sp1).updateCacheAndOffsets(any(Long.class));
        doNothing().when(sp0).updateCacheAndOffsets(any(Long.class));

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, memberId,
                new CompletableFuture<>(), partitionMaxBytes);

        DelayedShareFetch delayedShareFetch = new DelayedShareFetch(shareFetchPartitionData, mock(ReplicaManager.class), partitionCacheMap);

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData = new ArrayList<>();
        responseData.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
            MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        responseData.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
            MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result =
            delayedShareFetch.processFetchResponse(shareFetchPartitionData, responseData);

        assertTrue(result.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData = result.join();
        assertEquals(2, resultData.size());
        assertTrue(resultData.containsKey(tp0));
        assertTrue(resultData.containsKey(tp1));
        assertEquals(0, resultData.get(tp0).partitionIndex());
        assertEquals(1, resultData.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData.get(tp1).errorCode());
        assertEquals(Collections.emptyList(), resultData.get(tp0).acquiredRecords());
        assertEquals(Collections.emptyList(), resultData.get(tp1).acquiredRecords());
    }

    @Test
    public void testProcessFetchResponseWithLsoMovementForTopicPartition() {
        String groupId = "grp";
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));

        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = Mockito.mock(SharePartition.class);
        SharePartition sp1 = Mockito.mock(SharePartition.class);

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()),
                groupId, Uuid.randomUuid().toString(), new CompletableFuture<>(), partitionMaxBytes);

        DelayedShareFetch delayedShareFetch = spy(new DelayedShareFetch(shareFetchPartitionData, mock(ReplicaManager.class), partitionCacheMap));

        // Mocking the offsetForEarliestTimestamp method to return a valid LSO.
        Mockito.doReturn(1L).when(delayedShareFetch).offsetForEarliestTimestamp(any(TopicIdPartition.class));

        when(sp0.nextFetchOffset()).thenReturn((long) 0, (long) 5);
        when(sp1.nextFetchOffset()).thenReturn((long) 4, (long) 4);

        when(sp0.acquire(anyString(), any(FetchPartitionData.class))).thenReturn(
                CompletableFuture.completedFuture(Collections.emptyList()),
                CompletableFuture.completedFuture(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1))));
        when(sp1.acquire(anyString(), any(FetchPartitionData.class))).thenReturn(
                CompletableFuture.completedFuture(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1))),
                CompletableFuture.completedFuture(Collections.emptyList()));

        doNothing().when(sp1).updateCacheAndOffsets(any(Long.class));
        doNothing().when(sp0).updateCacheAndOffsets(any(Long.class));

        MemoryRecords records1 = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord("0".getBytes(), "v".getBytes()),
                new SimpleRecord("1".getBytes(), "v".getBytes()),
                new SimpleRecord("2".getBytes(), "v".getBytes()),
                new SimpleRecord(null, "value".getBytes()));

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData1 = new ArrayList<>();
        responseData1.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.OFFSET_OUT_OF_RANGE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        responseData1.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result1 =
                delayedShareFetch.processFetchResponse(shareFetchPartitionData, responseData1);

        assertTrue(result1.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData1 = result1.join();
        assertEquals(2, resultData1.size());
        assertTrue(resultData1.containsKey(tp0));
        assertTrue(resultData1.containsKey(tp1));
        assertEquals(0, resultData1.get(tp0).partitionIndex());
        assertEquals(1, resultData1.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData1.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData1.get(tp1).errorCode());

        // Since we have OFFSET_OUT_OF_RANGE exception for tp1 and no exception for tp2 from SharePartition class,
        // we should have 1 call for updateCacheAndOffsets for tp0 and 0 calls for tp1.
        Mockito.verify(sp0, times(1)).updateCacheAndOffsets(any(Long.class));
        Mockito.verify(sp1, times(0)).updateCacheAndOffsets(any(Long.class));

        MemoryRecords records2 = MemoryRecords.withRecords(100L, Compression.NONE,
                new SimpleRecord("0".getBytes(), "v".getBytes()),
                new SimpleRecord("1".getBytes(), "v".getBytes()),
                new SimpleRecord("2".getBytes(), "v".getBytes()),
                new SimpleRecord(null, "value".getBytes()));

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData2 = new ArrayList<>();
        responseData2.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records2, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        responseData2.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result2 =
                delayedShareFetch.processFetchResponse(shareFetchPartitionData, responseData2);

        assertTrue(result2.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData2 = result2.join();
        assertEquals(2, resultData2.size());
        assertTrue(resultData2.containsKey(tp0));
        assertTrue(resultData2.containsKey(tp1));
        assertEquals(0, resultData2.get(tp0).partitionIndex());
        assertEquals(1, resultData2.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData2.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData2.get(tp1).errorCode());

        // Since we don't see any exception for tp1 and tp2 from SharePartition class,
        // the updateCacheAndOffsets calls should remain the same as the previous case.
        Mockito.verify(sp0, times(1)).updateCacheAndOffsets(any(Long.class));
        Mockito.verify(sp1, times(0)).updateCacheAndOffsets(any(Long.class));
    }
}
