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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogReadResult;

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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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

    // ---- helpers ----

    // A read result carrying the given data and error. Other read metadata is irrelevant to the fetcher.
    private static LogReadResult logReadResult(FetchDataInfo info, Errors error) {
        return new LogReadResult(info, Optional.empty(), 0L, 0L, 0L, 0L, -1L, OptionalLong.empty(), error);
    }

    // A successful read carrying the given records (offsets assigned from 0 by MemoryRecords#withRecords).
    private static LogReadResult success(SimpleRecord... records) {
        return logReadResult(
            new FetchDataInfo(null, MemoryRecords.withRecords(Compression.NONE, records)), Errors.NONE);
    }

    // A failed read - partial-data tolerant, so it still carries a (here empty) FetchDataInfo.
    private static LogReadResult failure(Errors error) {
        return logReadResult(new FetchDataInfo(null, MemoryRecords.EMPTY), error);
    }

    private static LinkedHashMap<TopicIdPartition, LogReadResult> resultMap(LogReadResult result) {
        LinkedHashMap<TopicIdPartition, LogReadResult> map = new LinkedHashMap<>();
        map.put(TOPIC_ID_PARTITION, result);
        return map;
    }

    private static CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> done(LogReadResult result) {
        return CompletableFuture.completedFuture(resultMap(result));
    }

    @SafeVarargs
    private void whenReadAsync(CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> first,
                              CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>>... rest) {
        var stub = when(logReader.readAsync(any(), anySet(), any(), any(), anyBoolean())).thenReturn(first);
        for (CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> future : rest) {
            stub = stub.thenReturn(future);
        }
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
    public void testFetchAllRecordsInSingleRead() throws Exception {
        whenReadAsync(done(success(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"))));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
        // The DLQ copy always asks readAsync to follow tiered offsets to the remote tier.
        verify(logReader).readAsync(any(), anySet(), any(), any(), eq(true));
    }

    @Test
    public void testFetchRecordsAcrossMultipleReads() throws Exception {
        // First read returns only the first two offsets; the next read returns the batch containing the
        // remaining offset (records at or before the already-read position are skipped by the fetcher).
        whenReadAsync(
            done(success(record("k0", "v0"), record("k1", "v1"))),
            done(success(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"))));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
        verify(logReader, times(2)).readAsync(any(), anySet(), any(), any(), eq(true));
    }

    @Test
    public void testReadErrorYieldsNoRecords() throws Exception {
        whenReadAsync(done(failure(Errors.UNKNOWN_SERVER_ERROR)));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertTrue(result.isEmpty());
    }

    @Test
    public void testReadErrorMidRangeReturnsRecordsReadSoFar() throws Exception {
        // The first read succeeds and the second fails; the records already collected are still returned.
        whenReadAsync(
            done(success(record("k0", "v0"), record("k1", "v1"))),
            done(failure(Errors.UNKNOWN_SERVER_ERROR)));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertEquals(2, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertNull(result.get(2L));
    }

    @Test
    public void testMissingPartitionResultYieldsNoRecords() throws Exception {
        // readAsync returns no entry for the partition - nothing can be read.
        when(logReader.readAsync(any(), anySet(), any(), any(), anyBoolean()))
            .thenReturn(CompletableFuture.completedFuture(new LinkedHashMap<>()));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertTrue(result.isEmpty());
    }

    @Test
    public void testNoProgressYieldsNoRecords() throws Exception {
        // A read that returns no records does not advance the read position, so the loop terminates.
        whenReadAsync(done(success()));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertTrue(result.isEmpty());
    }

    @Test
    public void testSingleOffsetFetch() throws Exception {
        whenReadAsync(done(success(record("k0", "v0"))));

        Map<Long, Record> result = fetch(param(0L, 0L));

        assertEquals(1, result.size());
        assertRecord(result, 0L, "k0", "v0");
    }

    @Test
    public void testRecordsBeyondEndOffsetIgnored() throws Exception {
        // The read returns more records than the requested range [0, 1]; offsets beyond endOffset are ignored.
        whenReadAsync(done(success(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"))));

        Map<Long, Record> result = fetch(param(0L, 1L));

        assertEquals(2, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertNull(result.get(2L));
    }

    @Test
    public void testAsyncReadResumesLoopWhenPending() throws Exception {
        // A not-yet-complete future (e.g. an in-flight remote read) suspends the loop; the fetch returns
        // before the read completes and resumes from the callback.
        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> pending = new CompletableFuture<>();
        when(logReader.readAsync(any(), anySet(), any(), any(), anyBoolean())).thenReturn(pending);

        CompletableFuture<Map<Long, Record>> resultFuture = fetcher(param(0L, 2L)).fetch();
        assertFalse(resultFuture.isDone(), "Fetch should be waiting on the pending read");

        pending.complete(resultMap(success(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"))));

        Map<Long, Record> result = resultFuture.get(10, TimeUnit.SECONDS);
        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
    }

    @Test
    public void testAsyncReadResumeWithNoProgressCompletesEmpty() throws Exception {
        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> pending = new CompletableFuture<>();
        when(logReader.readAsync(any(), anySet(), any(), any(), anyBoolean())).thenReturn(pending);

        CompletableFuture<Map<Long, Record>> resultFuture = fetcher(param(0L, 2L)).fetch();
        assertFalse(resultFuture.isDone());

        // Completing with no records makes no progress, so the resumed loop stops and completes empty.
        pending.complete(resultMap(success()));

        assertTrue(resultFuture.get(10, TimeUnit.SECONDS).isEmpty());
    }

    @Test
    public void testAsyncReadResumeCompletesEmptyWhenProcessingThrows() throws Exception {
        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> pending = new CompletableFuture<>();
        when(logReader.readAsync(any(), anySet(), any(), any(), anyBoolean())).thenReturn(pending);

        CompletableFuture<Map<Long, Record>> resultFuture = fetcher(param(0L, 2L)).fetch();

        // An unexpected error while processing the resumed records must not escape the callback.
        Records throwing = mock(Records.class);
        when(throwing.batches()).thenThrow(new RuntimeException("boom"));
        pending.complete(resultMap(logReadResult(new FetchDataInfo(null, throwing), Errors.NONE)));

        assertTrue(resultFuture.get(10, TimeUnit.SECONDS).isEmpty());
    }

    @Test
    public void testFetchCompletesEmptyWhenReadAsyncThrows() throws Exception {
        // An unexpected error from the log reader must not escape; the copy is skipped entirely.
        when(logReader.readAsync(any(), anySet(), any(), any(), anyBoolean())).thenThrow(new RuntimeException("boom"));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertTrue(result.isEmpty());
    }
}
