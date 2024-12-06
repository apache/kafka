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

import kafka.log.OffsetResultHolder;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import scala.Option;

import static kafka.server.share.SharePartitionManagerTest.PARTITION_MAX_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class ShareFetchUtilsTest {

    private static final FetchParams FETCH_PARAMS = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(),
        FetchRequest.ORDINARY_CONSUMER_ID, -1, 0, 1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK,
        Optional.empty(), true);
    private static final BiConsumer<SharePartitionKey, Throwable> EXCEPTION_HANDLER = (key, exception) -> {
        // No-op
    };

    @Test
    public void testProcessFetchResponse() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.nextFetchOffset()).thenReturn((long) 3);
        when(sp1.nextFetchOffset()).thenReturn((long) 3);

        when(sp0.acquire(anyString(), anyInt(), any(FetchPartitionData.class))).thenReturn(
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));
        when(sp1.acquire(anyString(), anyInt(), any(FetchPartitionData.class))).thenReturn(
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1)));

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, memberId,
            new CompletableFuture<>(), partitionMaxBytes, 100);

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

        Map<TopicIdPartition, FetchPartitionData> responseData = new HashMap<>();
        responseData.put(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        responseData.put(tp1, new FetchPartitionData(Errors.NONE, 0L, 100L,
                records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData =
                ShareFetchUtils.processFetchResponse(shareFetch, responseData, sharePartitions, mock(ReplicaManager.class), EXCEPTION_HANDLER);

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
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.nextFetchOffset()).thenReturn((long) 3);
        when(sp1.nextFetchOffset()).thenReturn((long) 3);

        when(sp0.acquire(anyString(), anyInt(), any(FetchPartitionData.class))).thenReturn(ShareAcquiredRecords.empty());
        when(sp1.acquire(anyString(), anyInt(), any(FetchPartitionData.class))).thenReturn(ShareAcquiredRecords.empty());

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, memberId,
            new CompletableFuture<>(), partitionMaxBytes, 100);

        Map<TopicIdPartition, FetchPartitionData> responseData = new HashMap<>();
        responseData.put(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
            MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false));
        responseData.put(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
            MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false));
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData =
            ShareFetchUtils.processFetchResponse(shareFetch, responseData, sharePartitions, mock(ReplicaManager.class), EXCEPTION_HANDLER);

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

        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));

        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = Mockito.mock(SharePartition.class);
        SharePartition sp1 = Mockito.mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes, 100);

        ReplicaManager replicaManager = mock(ReplicaManager.class);

        // Mock the replicaManager.fetchOffsetForTimestamp method to return a timestamp and offset for the topic partition.
        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(100L, 1L, Optional.empty());
        doReturn(new OffsetResultHolder(Option.apply(timestampAndOffset), Option.empty())).when(replicaManager).fetchOffsetForTimestamp(any(TopicPartition.class), anyLong(), any(), any(), anyBoolean());

        when(sp0.nextFetchOffset()).thenReturn((long) 0, (long) 5);
        when(sp1.nextFetchOffset()).thenReturn((long) 4, (long) 4);

        when(sp0.acquire(anyString(), anyInt(), any(FetchPartitionData.class))).thenReturn(
            ShareAcquiredRecords.empty(),
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));
        when(sp1.acquire(anyString(), anyInt(), any(FetchPartitionData.class))).thenReturn(
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1)),
            ShareAcquiredRecords.empty());

        MemoryRecords records1 = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("0".getBytes(), "v".getBytes()),
            new SimpleRecord("1".getBytes(), "v".getBytes()),
            new SimpleRecord("2".getBytes(), "v".getBytes()),
            new SimpleRecord(null, "value".getBytes()));

        Map<TopicIdPartition, FetchPartitionData> responseData1 = new HashMap<>();
        responseData1.put(tp0, new FetchPartitionData(Errors.OFFSET_OUT_OF_RANGE, 0L, 0L,
            MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false));
        responseData1.put(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
            records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false));
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData1 =
            ShareFetchUtils.processFetchResponse(shareFetch, responseData1, sharePartitions, replicaManager, EXCEPTION_HANDLER);

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

        Map<TopicIdPartition, FetchPartitionData> responseData2 = new HashMap<>();
        responseData2.put(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
            records2, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false));
        responseData2.put(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
            MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false));
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData2 =
            ShareFetchUtils.processFetchResponse(shareFetch, responseData2, sharePartitions, replicaManager, EXCEPTION_HANDLER);

        assertEquals(2, resultData2.size());
        assertTrue(resultData2.containsKey(tp0));
        assertTrue(resultData2.containsKey(tp1));
        assertEquals(0, resultData2.get(tp0).partitionIndex());
        assertEquals(1, resultData2.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData2.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData2.get(tp1).errorCode());

        // Since we don't see any exception for tp1 and tp2 from SharePartition class,
        // the updateCacheAndOffsets calls should remain the same as the previous case.
        Mockito.verify(sp0, times(1)).updateCacheAndOffsets(1L);
        Mockito.verify(sp1, times(0)).updateCacheAndOffsets(any(Long.class));
    }

    @Test
    public void testProcessFetchResponseWhenNoRecordsAreAcquired() {
        String groupId = "grp";

        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = Collections.singletonMap(tp0, PARTITION_MAX_BYTES);

        SharePartition sp0 = Mockito.mock(SharePartition.class);
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes, 100);

        ReplicaManager replicaManager = mock(ReplicaManager.class);

        // Mock the replicaManager.fetchOffsetForTimestamp method to return a timestamp and offset for the topic partition.
        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(100L, 1L, Optional.empty());
        doReturn(new OffsetResultHolder(Option.apply(timestampAndOffset), Option.empty())).when(replicaManager).fetchOffsetForTimestamp(any(TopicPartition.class), anyLong(), any(), any(), anyBoolean());
        when(sp0.acquire(anyString(), anyInt(), any(FetchPartitionData.class))).thenReturn(ShareAcquiredRecords.empty());

        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("0".getBytes(), "v".getBytes()),
            new SimpleRecord("1".getBytes(), "v".getBytes()),
            new SimpleRecord("2".getBytes(), "v".getBytes()),
            new SimpleRecord(null, "value".getBytes()));

        // When no records are acquired from share partition.
        Map<TopicIdPartition, FetchPartitionData> responseData = Collections.singletonMap(
            tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData =
            ShareFetchUtils.processFetchResponse(shareFetch, responseData, sharePartitions, replicaManager, EXCEPTION_HANDLER);

        assertEquals(1, resultData.size());
        assertTrue(resultData.containsKey(tp0));
        assertEquals(0, resultData.get(tp0).partitionIndex());
        assertNull(resultData.get(tp0).records());
        assertTrue(resultData.get(tp0).acquiredRecords().isEmpty());
        assertEquals(Errors.NONE.code(), resultData.get(tp0).errorCode());

        // When fetch partition data has OFFSET_OUT_OF_RANGE error.
        responseData = Collections.singletonMap(
            tp0, new FetchPartitionData(Errors.OFFSET_OUT_OF_RANGE, 0L, 0L,
                records, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        resultData = ShareFetchUtils.processFetchResponse(shareFetch, responseData, sharePartitions, replicaManager, EXCEPTION_HANDLER);

        assertEquals(1, resultData.size());
        assertTrue(resultData.containsKey(tp0));
        assertEquals(0, resultData.get(tp0).partitionIndex());
        assertNull(resultData.get(tp0).records());
        assertTrue(resultData.get(tp0).acquiredRecords().isEmpty());
        assertEquals(Errors.NONE.code(), resultData.get(tp0).errorCode());

        Mockito.verify(sp0, times(1)).updateCacheAndOffsets(1L);
    }

    @Test
    public void testProcessFetchResponseWithMaxFetchRecords() {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));

        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        SharePartition sp0 = Mockito.mock(SharePartition.class);
        SharePartition sp1 = Mockito.mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        when(sp0.nextFetchOffset()).thenReturn((long) 0, (long) 5);
        when(sp1.nextFetchOffset()).thenReturn((long) 4, (long) 4);

        Uuid memberId = Uuid.randomUuid();
        // Set max fetch records to 10
        ShareFetch shareFetch = new ShareFetch(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()),
            groupId, memberId.toString(), new CompletableFuture<>(), partitionMaxBytes, 10);

        MemoryRecords records1 = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("0".getBytes(), "v".getBytes()),
            new SimpleRecord("1".getBytes(), "v".getBytes()),
            new SimpleRecord("2".getBytes(), "v".getBytes()),
            new SimpleRecord(null, "value".getBytes()));

        FetchPartitionData fetchPartitionData1 = new FetchPartitionData(Errors.NONE, 0L, 0L,
            records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false);
        FetchPartitionData fetchPartitionData2 = new FetchPartitionData(Errors.NONE, 0L, 0L,
            records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false);

        when(sp0.acquire(memberId.toString(), 10, fetchPartitionData1)).thenReturn(
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0).setLastOffset(1).setDeliveryCount((short) 1)));
        when(sp1.acquire(memberId.toString(), 8, fetchPartitionData2)).thenReturn(
            ShareAcquiredRecords.fromAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1)));

        // Send the topic partitions in order so can validate if correct mock is called, accounting
        // the offset count for the acquired records from the previous share partition acquire.
        Map<TopicIdPartition, FetchPartitionData> responseData1 = new LinkedHashMap<>();
        responseData1.put(tp0, fetchPartitionData1);
        responseData1.put(tp1, fetchPartitionData2);

        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData1 =
            ShareFetchUtils.processFetchResponse(shareFetch, responseData1, sharePartitions,
                mock(ReplicaManager.class), EXCEPTION_HANDLER);

        assertEquals(2, resultData1.size());
        assertTrue(resultData1.containsKey(tp0));
        assertTrue(resultData1.containsKey(tp1));
        assertEquals(0, resultData1.get(tp0).partitionIndex());
        assertEquals(1, resultData1.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData1.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData1.get(tp1).errorCode());
        assertEquals(1, resultData1.get(tp0).acquiredRecords().size());
        assertEquals(0, resultData1.get(tp0).acquiredRecords().get(0).firstOffset());
        assertEquals(1, resultData1.get(tp0).acquiredRecords().get(0).lastOffset());
        assertEquals(1, resultData1.get(tp1).acquiredRecords().size());
        assertEquals(100, resultData1.get(tp1).acquiredRecords().get(0).firstOffset());
        assertEquals(103, resultData1.get(tp1).acquiredRecords().get(0).lastOffset());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessFetchResponseWithOffsetFetchException() {
        SharePartition sp0 = Mockito.mock(SharePartition.class);
        when(sp0.leaderEpoch()).thenReturn(1);

        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        ShareFetch shareFetch = mock(ShareFetch.class);
        when(shareFetch.groupId()).thenReturn("grp");
        ReplicaManager replicaManager = mock(ReplicaManager.class);

        // Mock the replicaManager.fetchOffsetForTimestamp method to throw exception.
        Throwable exception = new FencedLeaderEpochException("Fenced exception");
        doThrow(exception).when(replicaManager).fetchOffsetForTimestamp(any(TopicPartition.class), anyLong(), any(), any(), anyBoolean());
        when(sp0.acquire(anyString(), anyInt(), any(FetchPartitionData.class))).thenReturn(ShareAcquiredRecords.empty());

        // When no records are acquired from share partition.
        Map<TopicIdPartition, FetchPartitionData> responseData = Collections.singletonMap(
            tp0, new FetchPartitionData(Errors.OFFSET_OUT_OF_RANGE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));

        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mock(BiConsumer.class);
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData =
            ShareFetchUtils.processFetchResponse(shareFetch, responseData, sharePartitions,
                replicaManager, exceptionHandler);

        assertTrue(resultData.isEmpty());
        Mockito.verify(shareFetch, times(1)).addErroneous(tp0, exception);
        Mockito.verify(exceptionHandler, times(1)).accept(new SharePartitionKey("grp", tp0), exception);
        Mockito.verify(sp0, times(0)).updateCacheAndOffsets(any(Long.class));
    }
}
