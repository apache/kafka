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
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.share.fetch.ShareFetchData;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import scala.Option;

import static kafka.server.share.SharePartitionManagerTest.PARTITION_MAX_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class ShareFetchUtilsTest {

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
                Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));
        when(sp1.acquire(any(), any())).thenReturn(
                Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1)));

        doNothing().when(sp1).updateCacheAndOffsets(any(Long.class));
        doNothing().when(sp0).updateCacheAndOffsets(any(Long.class));

        SharePartitionManager sharePartitionManager = mock(SharePartitionManager.class);
        when(sharePartitionManager.sharePartition(groupId, tp0)).thenReturn(sp0);
        when(sharePartitionManager.sharePartition(groupId, tp1)).thenReturn(sp1);

        ShareFetchData shareFetchData = new ShareFetchData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, memberId,
                new CompletableFuture<>(), partitionMaxBytes);

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
                ShareFetchUtils.processFetchResponse(shareFetchData, responseData, sharePartitionManager, mock(ReplicaManager.class));

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

        when(sp0.acquire(any(), any())).thenReturn(Collections.emptyList());
        when(sp1.acquire(any(), any())).thenReturn(Collections.emptyList());

        doNothing().when(sp1).updateCacheAndOffsets(any(Long.class));
        doNothing().when(sp0).updateCacheAndOffsets(any(Long.class));

        SharePartitionManager sharePartitionManager = mock(SharePartitionManager.class);
        when(sharePartitionManager.sharePartition(groupId, tp0)).thenReturn(sp0);
        when(sharePartitionManager.sharePartition(groupId, tp1)).thenReturn(sp1);

        ShareFetchData shareFetchData = new ShareFetchData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, memberId,
                new CompletableFuture<>(), partitionMaxBytes);

        Map<TopicIdPartition, FetchPartitionData> responseData = new HashMap<>();
        responseData.put(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        responseData.put(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false));
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData =
                ShareFetchUtils.processFetchResponse(shareFetchData, responseData, sharePartitionManager, mock(ReplicaManager.class));

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

        SharePartitionManager sharePartitionManager = mock(SharePartitionManager.class);
        when(sharePartitionManager.sharePartition(groupId, tp0)).thenReturn(sp0);
        when(sharePartitionManager.sharePartition(groupId, tp1)).thenReturn(sp1);

        ShareFetchData shareFetchData = new ShareFetchData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()),
                groupId, Uuid.randomUuid().toString(), new CompletableFuture<>(), partitionMaxBytes);

        ReplicaManager replicaManager = mock(ReplicaManager.class);

        // Mock the replicaManager.fetchOffsetForTimestamp method to return a timestamp and offset for the topic partition.
        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(100L, 1L, Optional.empty());
        doReturn(new OffsetResultHolder(Option.apply(timestampAndOffset), Option.empty())).when(replicaManager).fetchOffsetForTimestamp(any(TopicPartition.class), anyLong(), any(), any(), anyBoolean());

        when(sp0.nextFetchOffset()).thenReturn((long) 0, (long) 5);
        when(sp1.nextFetchOffset()).thenReturn((long) 4, (long) 4);

        when(sp0.acquire(anyString(), any(FetchPartitionData.class))).thenReturn(
                Collections.emptyList(),
                Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));
        when(sp1.acquire(anyString(), any(FetchPartitionData.class))).thenReturn(
                Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1)),
                Collections.emptyList());

        doNothing().when(sp1).updateCacheAndOffsets(any(Long.class));
        doNothing().when(sp0).updateCacheAndOffsets(any(Long.class));

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
                ShareFetchUtils.processFetchResponse(shareFetchData, responseData1, sharePartitionManager, replicaManager);

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
                ShareFetchUtils.processFetchResponse(shareFetchData, responseData2, sharePartitionManager, replicaManager);

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
        Uuid fooId = Uuid.randomUuid();

        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = Collections.singletonMap(tp0, PARTITION_MAX_BYTES);

        SharePartition sp0 = Mockito.mock(SharePartition.class);
        SharePartitionManager sharePartitionManager = mock(SharePartitionManager.class);
        when(sharePartitionManager.sharePartition(groupId, tp0)).thenReturn(sp0);

        ShareFetchData shareFetchData = new ShareFetchData(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()),
            groupId, Uuid.randomUuid().toString(), new CompletableFuture<>(), partitionMaxBytes);

        ReplicaManager replicaManager = mock(ReplicaManager.class);

        // Mock the replicaManager.fetchOffsetForTimestamp method to return a timestamp and offset for the topic partition.
        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(100L, 1L, Optional.empty());
        doReturn(new OffsetResultHolder(Option.apply(timestampAndOffset), Option.empty())).when(replicaManager).fetchOffsetForTimestamp(any(TopicPartition.class), anyLong(), any(), any(), anyBoolean());

        when(sp0.acquire(anyString(), any(FetchPartitionData.class))).thenReturn(Collections.emptyList());
        doNothing().when(sp0).updateCacheAndOffsets(any(Long.class));

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
            ShareFetchUtils.processFetchResponse(shareFetchData, responseData, sharePartitionManager, replicaManager);

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

        resultData = ShareFetchUtils.processFetchResponse(shareFetchData, responseData, sharePartitionManager, replicaManager);

        assertEquals(1, resultData.size());
        assertTrue(resultData.containsKey(tp0));
        assertEquals(0, resultData.get(tp0).partitionIndex());
        assertNull(resultData.get(tp0).records());
        assertTrue(resultData.get(tp0).acquiredRecords().isEmpty());
        assertEquals(Errors.NONE.code(), resultData.get(tp0).errorCode());

        Mockito.verify(sp0, times(1)).updateCacheAndOffsets(1L);
    }
}
