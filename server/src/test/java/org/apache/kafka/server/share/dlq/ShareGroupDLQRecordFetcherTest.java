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

package org.apache.kafka.server.share.dlq;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShareGroupDLQRecordFetcherTest {

    private static final MockTime MOCK_TIME = new MockTime();
    private static final String GROUP_ID = "test-group";
    private static final TopicIdPartition TOPIC_ID_PARTITION =
        new TopicIdPartition(Uuid.randomUuid(), 0, "source-topic");
    private static final int MAX_FETCH_BYTES = 1024 * 1024;

    private final LogReader logReader = mock(LogReader.class);

    private static ShareGroupDLQRecordParameter param(long firstOffset, long lastOffset) {
        return new ShareGroupDLQRecordParameter(
            GROUP_ID, TOPIC_ID_PARTITION, firstOffset, lastOffset, Optional.empty(), Optional.empty());
    }

    private ShareGroupDLQRecordFetcher fetcher(ShareGroupDLQRecordParameter param) {
        return new ShareGroupDLQRecordFetcher(logReader, MOCK_TIME, param, MAX_FETCH_BYTES);
    }

    private Map<Long, Record> fetch(ShareGroupDLQRecordParameter param) throws Exception {
        return fetcher(param).fetch().get(10, TimeUnit.SECONDS);
    }

    private static RemoteStorageFetchInfo remoteFetchInfo() {
        return new RemoteStorageFetchInfo(
            MAX_FETCH_BYTES,
            true,
            TOPIC_ID_PARTITION,
            new FetchRequest.PartitionData(TOPIC_ID_PARTITION.topicId(), 0L, 0L, MAX_FETCH_BYTES, Optional.empty()),
            FetchIsolation.HIGH_WATERMARK);
    }

    private static LogReadResult localResult(SimpleRecord... records) {
        LogReadResult result = mock(LogReadResult.class);
        when(result.error()).thenReturn(Errors.NONE);
        when(result.info()).thenReturn(new FetchDataInfo(null, MemoryRecords.withRecords(Compression.NONE, records)));
        return result;
    }

    private static LogReadResult errorResult(Errors error) {
        LogReadResult result = mock(LogReadResult.class);
        when(result.error()).thenReturn(error);
        return result;
    }

    // A local read result indicating the requested offset has been tiered off to remote storage.
    private static LogReadResult remoteResult() {
        LogReadResult result = mock(LogReadResult.class);
        when(result.error()).thenReturn(Errors.NONE);
        when(result.info()).thenReturn(new FetchDataInfo(
            null, MemoryRecords.EMPTY, false, Optional.empty(), Optional.of(remoteFetchInfo())));
        return result;
    }

    private static FetchDataInfo records(SimpleRecord... records) {
        return new FetchDataInfo(null, MemoryRecords.withRecords(Compression.NONE, records));
    }

    @SuppressWarnings("unchecked")
    private void whenLocalReads(LogReadResult first, LogReadResult... rest) {
        LinkedHashMap<TopicIdPartition, LogReadResult>[] maps = new LinkedHashMap[rest.length + 1];
        maps[0] = mapOf(first);
        for (int i = 0; i < rest.length; i++) {
            maps[i + 1] = mapOf(rest[i]);
        }
        var stub = when(logReader.read(any(), anySet(), any(), any())).thenReturn(maps[0]);
        for (int i = 1; i < maps.length; i++) {
            stub = stub.thenReturn(maps[i]);
        }
    }

    private static LinkedHashMap<TopicIdPartition, LogReadResult> mapOf(LogReadResult result) {
        LinkedHashMap<TopicIdPartition, LogReadResult> map = new LinkedHashMap<>();
        map.put(TOPIC_ID_PARTITION, result);
        return map;
    }

    private static SimpleRecord record(String key, String value) {
        return new SimpleRecord(MOCK_TIME.milliseconds(),
            key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    private static void assertRecord(Map<Long, Record> result, long offset, String key, String value) {
        Record record = result.get(offset);
        assertTrue(record != null, "Expected a record at offset " + offset);
        assertArrayEquals(key.getBytes(StandardCharsets.UTF_8), toArray(record.key()));
        assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), toArray(record.value()));
    }

    private static byte[] toArray(ByteBuffer buffer) {
        return Utils.toArray(buffer);
    }

    @Test
    public void testFetchAllLocalRecordsInSingleRead() throws Exception {
        whenLocalReads(localResult(record("k0", "v0"), record("k1", "v1"), record("k2", "v2")));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
        verify(logReader, never()).readRemote(any());
    }

    @Test
    public void testFetchLocalRecordsAcrossMultipleReads() throws Exception {
        // First read returns only the first two offsets; the next read returns the batch containing the
        // remaining offset (records at or before the already-read position are skipped by the fetcher).
        whenLocalReads(
            localResult(record("k0", "v0"), record("k1", "v1")),
            localResult(record("k0", "v0"), record("k1", "v1"), record("k2", "v2")));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
        verify(logReader, never()).readRemote(any());
    }

    @Test
    public void testLocalReadErrorYieldsNoRecords() throws Exception {
        whenLocalReads(errorResult(Errors.UNKNOWN_SERVER_ERROR));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertTrue(result.isEmpty());
        verify(logReader, never()).readRemote(any());
    }

    @Test
    public void testMissingPartitionInReadResultYieldsNoRecords() throws Exception {
        when(logReader.read(any(), anySet(), any(), any())).thenReturn(new LinkedHashMap<>());

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertTrue(result.isEmpty());
    }

    @Test
    public void testFetchAllRecordsFromRemoteStorage() throws Exception {
        whenLocalReads(remoteResult());
        when(logReader.readRemote(any())).thenReturn(CompletableFuture.completedFuture(
            records(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"))));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
        verify(logReader).readRemote(any());
    }

    @Test
    public void testRemoteFetchFailureSkipsRecords() throws Exception {
        whenLocalReads(remoteResult());
        when(logReader.readRemote(any())).thenReturn(CompletableFuture.failedFuture(
            new IllegalStateException("remote log manager not configured")));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertTrue(result.isEmpty());
        verify(logReader).readRemote(any());
    }

    @Test
    public void testFetchMixedLocalAndRemoteRecords() throws Exception {
        // Offset 0 is read locally; the next read indicates the rest has been tiered to remote storage.
        whenLocalReads(localResult(record("k0", "v0")), remoteResult());
        when(logReader.readRemote(any())).thenReturn(CompletableFuture.completedFuture(
            records(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"))));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
        verify(logReader).readRemote(any());
    }

    @Test
    public void testAsyncRemoteCompletionResumesLoop() throws Exception {
        whenLocalReads(remoteResult());
        // Return a future that is not yet complete to exercise the asynchronous resume path (the fetch
        // returns before the remote read completes, and the loop is resumed from the callback).
        CompletableFuture<FetchDataInfo> remoteFuture = new CompletableFuture<>();
        when(logReader.readRemote(any())).thenReturn(remoteFuture);

        CompletableFuture<Map<Long, Record>> resultFuture =
            new ShareGroupDLQRecordFetcher(logReader, MOCK_TIME, param(0L, 2L), MAX_FETCH_BYTES).fetch();
        assertFalse(resultFuture.isDone(), "Fetch should be waiting on the pending remote read");

        remoteFuture.complete(records(record("k0", "v0"), record("k1", "v1"), record("k2", "v2")));

        Map<Long, Record> result = resultFuture.get(10, TimeUnit.SECONDS);
        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
    }

    @Test
    public void testSingleOffsetFetch() throws Exception {
        whenLocalReads(localResult(record("k0", "v0")));

        Map<Long, Record> result = fetch(param(0L, 0L));

        assertEquals(1, result.size());
        assertRecord(result, 0L, "k0", "v0");
    }

    @Test
    public void testFetchInterleavingTwoLocalAndTwoRemoteReads() throws Exception {
        // Offsets 0..3 served by alternating reads: offset 0 local, offset 1 remote, offset 2 local,
        // offset 3 remote. This drives the loop through two local reads and two remote reads. Each read
        // returns a batch starting at base offset 0, so records at or before the requested offset are
        // skipped and only the offset being requested in that iteration is collected.
        whenLocalReads(
            localResult(record("k0", "v0")),                                            // offset 0 (local)
            remoteResult(),                                                             // offset 1 (tiered)
            localResult(record("k0", "v0"), record("k1", "v1"), record("k2", "v2")),    // offset 2 (local)
            remoteResult());                                                            // offset 3 (tiered)

        when(logReader.readRemote(any()))
            .thenReturn(CompletableFuture.completedFuture(
                records(record("k0", "v0"), record("k1", "v1"))))                       // serves offset 1
            .thenReturn(CompletableFuture.completedFuture(
                records(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"), record("k3", "v3")))); // serves offset 3

        Map<Long, Record> result = fetch(param(0L, 3L));

        assertEquals(4, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
        assertRecord(result, 3L, "k3", "v3");
        verify(logReader, times(4)).read(any(), anySet(), any(), any());
        verify(logReader, times(2)).readRemote(any());
    }

    // ---- fetchLocal ----

    @Test
    public void testFetchLocalReturnsNextOffsetOnLocalRecords() {
        whenLocalReads(localResult(record("k0", "v0"), record("k1", "v1"), record("k2", "v2")));

        ShareGroupDLQRecordFetcher fetcher = fetcher(param(0L, 2L));
        OptionalLong advanced = fetcher.fetchLocal(0L);

        // All three requested offsets were read, so the loop should continue past the range.
        assertEquals(OptionalLong.of(3L), advanced);
        assertFalse(fetcher.result().isDone(), "Result should remain pending while there is progress");
        verify(logReader, never()).readRemote(any());
    }

    @Test
    public void testFetchLocalAbortsWhenPartitionMissing() throws Exception {
        when(logReader.read(any(), anySet(), any(), any())).thenReturn(new LinkedHashMap<>());

        ShareGroupDLQRecordFetcher fetcher = fetcher(param(0L, 2L));
        OptionalLong advanced = fetcher.fetchLocal(0L);

        assertEquals(OptionalLong.empty(), advanced);
        assertTrue(fetcher.result().isDone());
        assertTrue(fetcher.result().get(10, TimeUnit.SECONDS).isEmpty());
    }

    @Test
    public void testFetchLocalAbortsOnReadError() throws Exception {
        whenLocalReads(errorResult(Errors.UNKNOWN_SERVER_ERROR));

        ShareGroupDLQRecordFetcher fetcher = fetcher(param(0L, 2L));
        OptionalLong advanced = fetcher.fetchLocal(0L);

        assertEquals(OptionalLong.empty(), advanced);
        assertTrue(fetcher.result().isDone());
        assertTrue(fetcher.result().get(10, TimeUnit.SECONDS).isEmpty());
    }

    @Test
    public void testFetchLocalCompletesWhenNoProgress() throws Exception {
        // A read that returns no records does not advance the read position.
        whenLocalReads(localResult());

        ShareGroupDLQRecordFetcher fetcher = fetcher(param(0L, 2L));
        OptionalLong advanced = fetcher.fetchLocal(0L);

        assertEquals(OptionalLong.empty(), advanced);
        assertTrue(fetcher.result().isDone());
        assertTrue(fetcher.result().get(10, TimeUnit.SECONDS).isEmpty());
    }

    @Test
    public void testFetchLocalDelegatesToRemoteWhenTiered() {
        whenLocalReads(remoteResult());
        when(logReader.readRemote(any())).thenReturn(CompletableFuture.completedFuture(
            records(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"))));

        ShareGroupDLQRecordFetcher fetcher = fetcher(param(0L, 2L));
        OptionalLong advanced = fetcher.fetchLocal(0L);

        // The tiered read returned all offsets, so fetchLocal returns the remote read's result.
        assertEquals(OptionalLong.of(3L), advanced);
        assertFalse(fetcher.result().isDone());
        verify(logReader).readRemote(any());
    }

    // ---- fetchRemote ----

    @Test
    public void testFetchRemoteReturnsNextOffsetWhenComplete() {
        when(logReader.readRemote(any())).thenReturn(CompletableFuture.completedFuture(
            records(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"))));

        ShareGroupDLQRecordFetcher fetcher = fetcher(param(0L, 2L));
        OptionalLong advanced = fetcher.fetchRemote(remoteFetchInfo(), 0L);

        assertEquals(OptionalLong.of(3L), advanced);
        assertFalse(fetcher.result().isDone());
    }

    @Test
    public void testFetchRemoteSkipsAndCompletesOnFailure() throws Exception {
        when(logReader.readRemote(any())).thenReturn(CompletableFuture.failedFuture(
            new IllegalStateException("remote log manager not configured")));

        ShareGroupDLQRecordFetcher fetcher = fetcher(param(0L, 2L));
        OptionalLong advanced = fetcher.fetchRemote(remoteFetchInfo(), 0L);

        // The failed remote read read nothing, so no progress -> the loop stops and the result completes.
        assertEquals(OptionalLong.empty(), advanced);
        assertTrue(fetcher.result().isDone());
        assertTrue(fetcher.result().get(10, TimeUnit.SECONDS).isEmpty());
    }

    @Test
    public void testFetchRemotePendingReturnsEmptyAndResumesOnCompletion() throws Exception {
        CompletableFuture<FetchDataInfo> remoteFuture = new CompletableFuture<>();
        when(logReader.readRemote(any())).thenReturn(remoteFuture);

        ShareGroupDLQRecordFetcher fetcher = fetcher(param(0L, 2L));
        OptionalLong advanced = fetcher.fetchRemote(remoteFetchInfo(), 0L);

        // Pending remote read: fetchRemote returns empty (loop suspends) and the result stays pending.
        assertEquals(OptionalLong.empty(), advanced);
        assertFalse(fetcher.result().isDone());

        // Completing the remote read resumes the loop from the callback and completes the fetch.
        remoteFuture.complete(records(record("k0", "v0"), record("k1", "v1"), record("k2", "v2")));

        Map<Long, Record> result = fetcher.result().get(10, TimeUnit.SECONDS);
        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
    }

    // ---- additional branch coverage ----

    @Test
    public void testFetchCompletesEmptyWhenLocalReadThrows() throws Exception {
        // An unexpected error from the log reader must not escape; the copy is skipped entirely.
        when(logReader.read(any(), anySet(), any(), any())).thenThrow(new RuntimeException("boom"));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertTrue(result.isEmpty());
    }

    @Test
    public void testCollectStopsAtEndOffsetWhenReadReturnsExtraRecords() throws Exception {
        // The read returns more records than the requested range [0, 1]; offsets beyond endOffset are ignored.
        whenLocalReads(localResult(record("k0", "v0"), record("k1", "v1"), record("k2", "v2")));

        Map<Long, Record> result = fetch(param(0L, 1L));

        assertEquals(2, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertNull(result.get(2L));
    }

    @Test
    public void testFetchRemoteSkipsWhenReadReturnsNullData() throws Exception {
        // A remote read that completes with null data leaves the offsets unread (skipped).
        when(logReader.readRemote(any())).thenReturn(CompletableFuture.completedFuture(null));

        ShareGroupDLQRecordFetcher fetcher = fetcher(param(0L, 2L));
        OptionalLong advanced = fetcher.fetchRemote(remoteFetchInfo(), 0L);

        assertEquals(OptionalLong.empty(), advanced);
        assertTrue(fetcher.result().get(10, TimeUnit.SECONDS).isEmpty());
    }

    @Test
    public void testFetchRemotePendingCompletesWhenNoProgress() throws Exception {
        CompletableFuture<FetchDataInfo> remoteFuture = new CompletableFuture<>();
        when(logReader.readRemote(any())).thenReturn(remoteFuture);

        ShareGroupDLQRecordFetcher fetcher = fetcher(param(0L, 2L));
        assertEquals(OptionalLong.empty(), fetcher.fetchRemote(remoteFetchInfo(), 0L));
        assertFalse(fetcher.result().isDone());

        // Completing with no records makes no progress, so the resumed loop stops and completes empty.
        remoteFuture.complete(records());

        assertTrue(fetcher.result().get(10, TimeUnit.SECONDS).isEmpty());
    }

    @Test
    public void testFetchRemotePendingCompletesEmptyWhenProcessingThrows() throws Exception {
        CompletableFuture<FetchDataInfo> remoteFuture = new CompletableFuture<>();
        when(logReader.readRemote(any())).thenReturn(remoteFuture);

        ShareGroupDLQRecordFetcher fetcher = fetcher(param(0L, 2L));
        fetcher.fetchRemote(remoteFetchInfo(), 0L);

        // An unexpected error while processing the resumed remote records must not escape the callback.
        Records throwing = mock(Records.class);
        when(throwing.batches()).thenThrow(new RuntimeException("boom"));
        remoteFuture.complete(new FetchDataInfo(null, throwing));

        assertTrue(fetcher.result().get(10, TimeUnit.SECONDS).isEmpty());
    }
}
