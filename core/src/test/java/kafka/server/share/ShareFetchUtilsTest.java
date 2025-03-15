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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.share.fetch.ShareFetchPartitionData;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static kafka.server.share.SharePartitionManagerTest.PARTITION_MAX_BYTES;
import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.createFileRecords;
import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.createShareAcquiredRecords;
import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.memoryRecordsBuilder;
import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.orderedMap;
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
    private static final int BATCH_SIZE = 500;
    private static final BiConsumer<SharePartitionKey, Throwable> EXCEPTION_HANDLER = (key, exception) -> {
        // No-op
    };
    private static final BrokerTopicStats BROKER_TOPIC_STATS = new BrokerTopicStats();

    @Test
    public void testProcessFetchResponse() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));
        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes = orderedMap(PARTITION_MAX_BYTES, tp0, tp1);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.nextFetchOffset()).thenReturn((long) 3);
        when(sp1.nextFetchOffset()).thenReturn((long) 3);

        when(sp0.acquire(anyString(), anyInt(), anyInt(), anyLong(), any(FetchPartitionData.class))).thenReturn(
            createShareAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));
        when(sp1.acquire(anyString(), anyInt(), anyInt(), anyLong(), any(FetchPartitionData.class))).thenReturn(
            createShareAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1)));

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, memberId,
            new CompletableFuture<>(), partitionMaxBytes, BATCH_SIZE, 100, BROKER_TOPIC_STATS);

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

        List<ShareFetchPartitionData> responseData = List.of(
            new ShareFetchPartitionData(tp0, 0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)),
            new ShareFetchPartitionData(tp1, 0, new FetchPartitionData(Errors.NONE, 0L, 100L,
                records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false))
        );
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
        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes = orderedMap(PARTITION_MAX_BYTES, tp0, tp1);

        SharePartition sp0 = mock(SharePartition.class);
        SharePartition sp1 = mock(SharePartition.class);

        when(sp0.nextFetchOffset()).thenReturn((long) 3);
        when(sp1.nextFetchOffset()).thenReturn((long) 3);

        when(sp0.acquire(anyString(), anyInt(), anyInt(), anyLong(), any(FetchPartitionData.class))).thenReturn(ShareAcquiredRecords.empty());
        when(sp1.acquire(anyString(), anyInt(), anyInt(), anyLong(), any(FetchPartitionData.class))).thenReturn(ShareAcquiredRecords.empty());

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, memberId,
            new CompletableFuture<>(), partitionMaxBytes, BATCH_SIZE, 100, BROKER_TOPIC_STATS);

        List<ShareFetchPartitionData> responseData = List.of(
            new ShareFetchPartitionData(tp0, 0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)),
            new ShareFetchPartitionData(tp1, 0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false))
        );
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

        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes = orderedMap(PARTITION_MAX_BYTES, tp0, tp1);

        SharePartition sp0 = Mockito.mock(SharePartition.class);
        SharePartition sp1 = Mockito.mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes, BATCH_SIZE, 100, BROKER_TOPIC_STATS);

        ReplicaManager replicaManager = mock(ReplicaManager.class);

        // Mock the replicaManager.fetchOffsetForTimestamp method to return a timestamp and offset for the topic partition.
        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(100L, 1L, Optional.empty());
        doReturn(new OffsetResultHolder(Optional.of(timestampAndOffset), Optional.empty())).when(replicaManager).fetchOffsetForTimestamp(any(TopicPartition.class), anyLong(), any(), any(), anyBoolean());

        when(sp0.nextFetchOffset()).thenReturn((long) 0, (long) 5);
        when(sp1.nextFetchOffset()).thenReturn((long) 4, (long) 4);

        when(sp0.acquire(anyString(), anyInt(), anyInt(), anyLong(), any(FetchPartitionData.class))).thenReturn(
            ShareAcquiredRecords.empty(),
            createShareAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)));
        when(sp1.acquire(anyString(), anyInt(), anyInt(), anyLong(), any(FetchPartitionData.class))).thenReturn(
            createShareAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1)),
            ShareAcquiredRecords.empty());

        MemoryRecords records1 = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("0".getBytes(), "v".getBytes()),
            new SimpleRecord("1".getBytes(), "v".getBytes()),
            new SimpleRecord("2".getBytes(), "v".getBytes()),
            new SimpleRecord(null, "value".getBytes()));

        List<ShareFetchPartitionData> responseData1 = List.of(
            new ShareFetchPartitionData(tp0, 0, new FetchPartitionData(Errors.OFFSET_OUT_OF_RANGE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)),
            new ShareFetchPartitionData(tp1, 0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false))
        );
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

        List<ShareFetchPartitionData> responseData2 = List.of(
            new ShareFetchPartitionData(tp0, 0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records2, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)),
            new ShareFetchPartitionData(tp1, 0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false))
        );
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
        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes = orderedMap(PARTITION_MAX_BYTES, tp0);

        SharePartition sp0 = Mockito.mock(SharePartition.class);
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);

        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, Uuid.randomUuid().toString(),
            new CompletableFuture<>(), partitionMaxBytes, BATCH_SIZE, 100, BROKER_TOPIC_STATS);

        ReplicaManager replicaManager = mock(ReplicaManager.class);

        // Mock the replicaManager.fetchOffsetForTimestamp method to return a timestamp and offset for the topic partition.
        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(100L, 1L, Optional.empty());
        doReturn(new OffsetResultHolder(Optional.of(timestampAndOffset), Optional.empty())).when(replicaManager).fetchOffsetForTimestamp(any(TopicPartition.class), anyLong(), any(), any(), anyBoolean());
        when(sp0.acquire(anyString(), anyInt(), anyInt(), anyLong(), any(FetchPartitionData.class))).thenReturn(ShareAcquiredRecords.empty());

        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("0".getBytes(), "v".getBytes()),
            new SimpleRecord("1".getBytes(), "v".getBytes()),
            new SimpleRecord("2".getBytes(), "v".getBytes()),
            new SimpleRecord(null, "value".getBytes()));

        // When no records are acquired from share partition.
        List<ShareFetchPartitionData> responseData = List.of(
            new ShareFetchPartitionData(tp0, 0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));

        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData =
            ShareFetchUtils.processFetchResponse(shareFetch, responseData, sharePartitions, replicaManager, EXCEPTION_HANDLER);

        assertEquals(1, resultData.size());
        assertTrue(resultData.containsKey(tp0));
        assertEquals(0, resultData.get(tp0).partitionIndex());
        assertNull(resultData.get(tp0).records());
        assertTrue(resultData.get(tp0).acquiredRecords().isEmpty());
        assertEquals(Errors.NONE.code(), resultData.get(tp0).errorCode());

        // When fetch partition data has OFFSET_OUT_OF_RANGE error.
        responseData = List.of(
            new ShareFetchPartitionData(tp0, 0, new FetchPartitionData(Errors.OFFSET_OUT_OF_RANGE, 0L, 0L,
                records, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));

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
    public void testProcessFetchResponseWithMaxFetchRecords() throws IOException {
        String groupId = "grp";
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 1));

        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes = orderedMap(PARTITION_MAX_BYTES, tp0, tp1);

        SharePartition sp0 = Mockito.mock(SharePartition.class);
        SharePartition sp1 = Mockito.mock(SharePartition.class);

        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions = new LinkedHashMap<>();
        sharePartitions.put(tp0, sp0);
        sharePartitions.put(tp1, sp1);

        when(sp0.nextFetchOffset()).thenReturn((long) 0, (long) 5);
        when(sp1.nextFetchOffset()).thenReturn((long) 4, (long) 4);

        Uuid memberId = Uuid.randomUuid();
        // Set max fetch records to 10
        ShareFetch shareFetch = new ShareFetch(FETCH_PARAMS, groupId, memberId.toString(),
            new CompletableFuture<>(), partitionMaxBytes, BATCH_SIZE, 10, BROKER_TOPIC_STATS);

        LinkedHashMap<Long, Integer> recordsPerOffset = new LinkedHashMap<>();
        recordsPerOffset.put(0L, 1);
        recordsPerOffset.put(1L, 1);
        recordsPerOffset.put(2L, 1);
        recordsPerOffset.put(3L, 1);
        Records records1 = createFileRecords(recordsPerOffset);

        recordsPerOffset.clear();
        recordsPerOffset.put(100L, 4);
        Records records2 = createFileRecords(recordsPerOffset);

        FetchPartitionData fetchPartitionData1 = new FetchPartitionData(Errors.NONE, 0L, 0L,
            records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false);
        FetchPartitionData fetchPartitionData2 = new FetchPartitionData(Errors.NONE, 0L, 0L,
            records2, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false);

        when(sp0.acquire(memberId.toString(), BATCH_SIZE, 10, 0, fetchPartitionData1)).thenReturn(
            createShareAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0).setLastOffset(1).setDeliveryCount((short) 1)));
        when(sp1.acquire(memberId.toString(), BATCH_SIZE, 8, 0, fetchPartitionData2)).thenReturn(
            createShareAcquiredRecords(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1)));

        // Send the topic partitions in order so can validate if correct mock is called, accounting
        // the offset count for the acquired records from the previous share partition acquire.
        List<ShareFetchPartitionData> responseData = List.of(
            new ShareFetchPartitionData(tp0, 0, fetchPartitionData1),
            new ShareFetchPartitionData(tp1, 0, fetchPartitionData2)
        );

        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData =
            ShareFetchUtils.processFetchResponse(shareFetch, responseData, sharePartitions,
                mock(ReplicaManager.class), EXCEPTION_HANDLER);

        assertEquals(2, resultData.size());
        assertTrue(resultData.containsKey(tp0));
        assertTrue(resultData.containsKey(tp1));
        assertEquals(0, resultData.get(tp0).partitionIndex());
        assertEquals(1, resultData.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData.get(tp1).errorCode());
        assertEquals(1, resultData.get(tp0).acquiredRecords().size());
        assertEquals(0, resultData.get(tp0).acquiredRecords().get(0).firstOffset());
        assertEquals(1, resultData.get(tp0).acquiredRecords().get(0).lastOffset());
        assertEquals(1, resultData.get(tp1).acquiredRecords().size());
        assertEquals(100, resultData.get(tp1).acquiredRecords().get(0).firstOffset());
        assertEquals(103, resultData.get(tp1).acquiredRecords().get(0).lastOffset());

        // Validate the slicing for fetched data happened for tp0 records, not for tp1 records.
        assertTrue(records1.sizeInBytes() > resultData.get(tp0).records().sizeInBytes());
        assertEquals(records2.sizeInBytes(), resultData.get(tp1).records().sizeInBytes());
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
        when(sp0.acquire(anyString(), anyInt(), anyInt(), anyLong(), any(FetchPartitionData.class))).thenReturn(ShareAcquiredRecords.empty());

        // When no records are acquired from share partition.
        List<ShareFetchPartitionData> responseData = List.of(
            new ShareFetchPartitionData(tp0, 0, new FetchPartitionData(Errors.OFFSET_OUT_OF_RANGE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));

        BiConsumer<SharePartitionKey, Throwable> exceptionHandler = mock(BiConsumer.class);
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData =
            ShareFetchUtils.processFetchResponse(shareFetch, responseData, sharePartitions,
                replicaManager, exceptionHandler);

        assertTrue(resultData.isEmpty());
        Mockito.verify(shareFetch, times(1)).addErroneous(tp0, exception);
        Mockito.verify(exceptionHandler, times(1)).accept(new SharePartitionKey("grp", tp0), exception);
        Mockito.verify(sp0, times(0)).updateCacheAndOffsets(any(Long.class));
    }

    @Test
    public void testMaybeSliceFetchRecordsSingleBatch() throws IOException {
        // Create 1 batch of records with 10 records.
        FileRecords records = createFileRecords(Map.of(5L, 10));

        // Acquire all offsets, should return same records.
        List<AcquiredRecords> acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(5).setLastOffset(14).setDeliveryCount((short) 1));
        Records slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 10));
        assertEquals(records, slicedRecords);

        // Acquire offsets out of first offset bound should return the records for the matching batch.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(2).setLastOffset(14).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 10));
        assertEquals(records, slicedRecords);

        // Acquire offsets out of last offset bound should return the records for the matching batch.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(5).setLastOffset(20).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 5));
        assertEquals(records, slicedRecords);

        // Acquire only subset of batch offsets, starting from the first offset.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(5).setLastOffset(8).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 1));
        assertEquals(records, slicedRecords);

        // Acquire only subset of batch offsets, ending at the last offset.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(8).setLastOffset(14).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 1));
        assertEquals(records, slicedRecords);

        // Acquire only subset of batch offsets, within the batch.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(8).setLastOffset(10).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 1));
        assertEquals(records, slicedRecords);
    }

    @Test
    public void testMaybeSliceFetchRecordsMultipleBatches() throws IOException {
        // Create 3 batches of records with 3, 2 and 4 records respectively.
        LinkedHashMap<Long, Integer> recordsPerOffset = new LinkedHashMap<>();
        recordsPerOffset.put(0L, 3);
        recordsPerOffset.put(3L, 2);
        recordsPerOffset.put(7L, 4); // Gap of 2 offsets between batches.
        FileRecords records = createFileRecords(recordsPerOffset);

        // Acquire all offsets, should return same records.
        List<AcquiredRecords> acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(0).setLastOffset(10).setDeliveryCount((short) 1));
        Records slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 11));
        assertEquals(records, slicedRecords);

        // Acquire offsets from all batches, but only first record from last batch. Should return
        // all batches.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(0).setLastOffset(7).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 5));
        assertEquals(records, slicedRecords);

        // Acquire only first batch offsets, should return only first batch.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(0).setLastOffset(2).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 5));
        assertTrue(records.sizeInBytes() > slicedRecords.sizeInBytes());
        List<RecordBatch> recordBatches = TestUtils.toList(slicedRecords.batches());
        assertEquals(1, recordBatches.size());
        assertEquals(0, recordBatches.get(0).baseOffset());
        assertEquals(2, recordBatches.get(0).lastOffset());

        // Acquire only second batch offsets, should return only second batch.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(3).setLastOffset(4).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 5));
        assertTrue(records.sizeInBytes() > slicedRecords.sizeInBytes());
        recordBatches = TestUtils.toList(slicedRecords.batches());
        assertEquals(1, recordBatches.size());
        assertEquals(3, recordBatches.get(0).baseOffset());
        assertEquals(4, recordBatches.get(0).lastOffset());

        // Acquire only last batch offsets, should return only last batch.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(7).setLastOffset(10).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 1));
        assertTrue(records.sizeInBytes() > slicedRecords.sizeInBytes());
        recordBatches = TestUtils.toList(slicedRecords.batches());
        assertEquals(1, recordBatches.size());
        assertEquals(7, recordBatches.get(0).baseOffset());
        assertEquals(10, recordBatches.get(0).lastOffset());

        // Acquire only subset of first batch offsets, should return only first batch.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(1).setLastOffset(1).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 1));
        assertTrue(records.sizeInBytes() > slicedRecords.sizeInBytes());
        recordBatches = TestUtils.toList(slicedRecords.batches());
        assertEquals(1, recordBatches.size());
        assertEquals(0, recordBatches.get(0).baseOffset());
        assertEquals(2, recordBatches.get(0).lastOffset());

        // Acquire only subset of second batch offsets, should return only second batch.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(4).setLastOffset(4).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 1));
        assertTrue(records.sizeInBytes() > slicedRecords.sizeInBytes());
        recordBatches = TestUtils.toList(slicedRecords.batches());
        assertEquals(1, recordBatches.size());
        assertEquals(3, recordBatches.get(0).baseOffset());
        assertEquals(4, recordBatches.get(0).lastOffset());

        // Acquire only subset of last batch offsets, should return only last batch.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(8).setLastOffset(8).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 1));
        assertTrue(records.sizeInBytes() > slicedRecords.sizeInBytes());
        recordBatches = TestUtils.toList(slicedRecords.batches());
        assertEquals(1, recordBatches.size());
        assertEquals(7, recordBatches.get(0).baseOffset());
        assertEquals(10, recordBatches.get(0).lastOffset());

        // Acquire including gaps between batches, should return 2 batches.
        acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(4).setLastOffset(8).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 1));
        assertTrue(records.sizeInBytes() > slicedRecords.sizeInBytes());
        recordBatches = TestUtils.toList(slicedRecords.batches());
        assertEquals(2, recordBatches.size());
        assertEquals(3, recordBatches.get(0).baseOffset());
        assertEquals(4, recordBatches.get(0).lastOffset());
        assertEquals(7, recordBatches.get(1).baseOffset());
        assertEquals(10, recordBatches.get(1).lastOffset());

        // Acquire with multiple acquired records, should return matching batches.
        acquiredRecords = List.of(
            new AcquiredRecords().setFirstOffset(0).setLastOffset(2).setDeliveryCount((short) 1),
            new AcquiredRecords().setFirstOffset(3).setLastOffset(4).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 1));
        assertTrue(records.sizeInBytes() > slicedRecords.sizeInBytes());
        recordBatches = TestUtils.toList(slicedRecords.batches());
        assertEquals(2, recordBatches.size());
        assertEquals(0, recordBatches.get(0).baseOffset());
        assertEquals(2, recordBatches.get(0).lastOffset());
        assertEquals(3, recordBatches.get(1).baseOffset());
        assertEquals(4, recordBatches.get(1).lastOffset());

        // Acquire with multiple acquired records of individual offsets from single batch, should return
        // matching batch.
        acquiredRecords = List.of(
            new AcquiredRecords().setFirstOffset(8).setLastOffset(8).setDeliveryCount((short) 1),
            new AcquiredRecords().setFirstOffset(9).setLastOffset(9).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 1));
        assertTrue(records.sizeInBytes() > slicedRecords.sizeInBytes());
        recordBatches = TestUtils.toList(slicedRecords.batches());
        assertEquals(1, recordBatches.size());
        assertEquals(7, recordBatches.get(0).baseOffset());
        assertEquals(10, recordBatches.get(0).lastOffset());

        // Acquire with multiple acquired records of individual offsets from multiple batch, should return
        // multiple matching batches.
        acquiredRecords = List.of(
            new AcquiredRecords().setFirstOffset(1).setLastOffset(1).setDeliveryCount((short) 1),
            new AcquiredRecords().setFirstOffset(9).setLastOffset(9).setDeliveryCount((short) 1));
        slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(records, new ShareAcquiredRecords(acquiredRecords, 1));
        assertEquals(records.sizeInBytes(), slicedRecords.sizeInBytes());
    }

    @Test
    public void testMaybeSliceFetchRecordsException() throws IOException {
        // Create 1 batch of records with 3 records.
        FileRecords records = createFileRecords(Map.of(0L, 3));
        // Send empty acquired records which should trigger an exception and same file records should
        // be returned. The method doesn't expect empty acquired records.
        Records slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(
            records, new ShareAcquiredRecords(List.of(), 3));
        assertEquals(records, slicedRecords);
    }

    @Test
    public void testMaybeSliceFetchRecordsNonFileRecords() {
        // Send memory records which should be returned as is.
        try (MemoryRecordsBuilder records = memoryRecordsBuilder(2, 0)) {
            List<AcquiredRecords> acquiredRecords = List.of(new AcquiredRecords().setFirstOffset(0).setLastOffset(1).setDeliveryCount((short) 1));
            Records slicedRecords = ShareFetchUtils.maybeSliceFetchRecords(
                records.build(), new ShareAcquiredRecords(acquiredRecords, 2));
            assertEquals(records.build(), slicedRecords);
        }
    }
}
