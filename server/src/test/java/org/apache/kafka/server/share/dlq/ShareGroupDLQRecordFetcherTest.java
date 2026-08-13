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
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.common.utils.internals.SingleByteBufferOutputStream;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogReadResult;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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
    private static final int MAX_DECOMPRESSED_BYTES = 1024 * 1024;

    private final LogReader logReader = mock(LogReader.class);

    private static ShareGroupDLQRecordParameter param(long firstOffset, long lastOffset) {
        return new ShareGroupDLQRecordParameter(
            GROUP_ID, TOPIC_ID_PARTITION, firstOffset, lastOffset, Optional.empty(), Optional.empty());
    }

    private ShareGroupDLQRecordFetcher fetcher(ShareGroupDLQRecordParameter param) {
        return fetcher(param, MAX_DECOMPRESSED_BYTES);
    }

    private ShareGroupDLQRecordFetcher fetcher(ShareGroupDLQRecordParameter param, int maxDecompressedBytes) {
        return new ShareGroupDLQRecordFetcher(logReader, MOCK_TIME, param, MAX_FETCH_BYTES, maxDecompressedBytes);
    }

    private Map<Long, Record> fetch(ShareGroupDLQRecordParameter param) throws Exception {
        return fetcher(param).fetch().get(10, TimeUnit.SECONDS).records();
    }

    // The exact on-wire size of a single uncompressed record, computed via the real API rather than
    // hardcoded, so budget-boundary tests can align a budget precisely with a record's own size.
    private static int recordSizeInBytes(SimpleRecord simpleRecord) {
        MemoryRecords batch = MemoryRecords.withRecords(Compression.NONE, simpleRecord);
        return batch.batches().iterator().next().iterator().next().sizeInBytes();
    }

    // ---- helpers ----

    // A read result carrying the given data and error. Other read metadata is irrelevant to the fetcher.
    private static LogReadResult logReadResult(FetchDataInfo info, Errors error) {
        return new LogReadResult(info, Optional.empty(), 0L, 0L, 0L, 0L, -1L, OptionalLong.empty(), error);
    }

    // A successful read carrying the given records (offsets assigned from 0 by MemoryRecords#withRecords).
    private static LogReadResult success(SimpleRecord... records) {
        return success(Compression.NONE, records);
    }

    private static LogReadResult success(Compression compression, SimpleRecord... records) {
        return logReadResult(
            new FetchDataInfo(null, MemoryRecords.withRecords(compression, records)), Errors.NONE);
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

        CompletableFuture<ShareGroupDLQRecordFetcher.FetchResult> resultFuture = fetcher(param(0L, 2L)).fetch();
        assertFalse(resultFuture.isDone(), "Fetch should be waiting on the pending read");

        pending.complete(resultMap(success(record("k0", "v0"), record("k1", "v1"), record("k2", "v2"))));

        Map<Long, Record> result = resultFuture.get(10, TimeUnit.SECONDS).records();
        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
    }

    @Test
    public void testAsyncReadResumeWithNoProgressCompletesEmpty() throws Exception {
        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> pending = new CompletableFuture<>();
        when(logReader.readAsync(any(), anySet(), any(), any(), anyBoolean())).thenReturn(pending);

        CompletableFuture<ShareGroupDLQRecordFetcher.FetchResult> resultFuture = fetcher(param(0L, 2L)).fetch();
        assertFalse(resultFuture.isDone());

        // Completing with no records makes no progress, so the resumed loop stops and completes empty.
        pending.complete(resultMap(success()));

        assertTrue(resultFuture.get(10, TimeUnit.SECONDS).records().isEmpty());
    }

    @Test
    public void testAsyncReadResumeCompletesEmptyWhenProcessingThrows() throws Exception {
        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> pending = new CompletableFuture<>();
        when(logReader.readAsync(any(), anySet(), any(), any(), anyBoolean())).thenReturn(pending);

        CompletableFuture<ShareGroupDLQRecordFetcher.FetchResult> resultFuture = fetcher(param(0L, 2L)).fetch();

        // An unexpected error while processing the resumed records must not escape the callback.
        Records throwing = mock(Records.class);
        when(throwing.batches()).thenThrow(new RuntimeException("boom"));
        pending.complete(resultMap(logReadResult(new FetchDataInfo(null, throwing), Errors.NONE)));

        assertTrue(resultFuture.get(10, TimeUnit.SECONDS).records().isEmpty());
    }

    @Test
    public void testFetchCompletesEmptyWhenReadAsyncThrows() throws Exception {
        // An unexpected error from the log reader must not escape; the copy is skipped entirely.
        when(logReader.readAsync(any(), anySet(), any(), any(), anyBoolean())).thenThrow(new RuntimeException("boom"));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertTrue(result.isEmpty());
    }

    @Test
    public void testCompressedBatchFetchedCorrectly() throws Exception {
        // A normal, well within-budget compressed batch is unaffected by the bounded decompression path.
        whenReadAsync(done(success(Compression.gzip().build(),
            record("k0", "v0"), record("k1", "v1"), record("k2", "v2"))));

        Map<Long, Record> result = fetch(param(0L, 2L));

        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("compressedBatchCases")
    public void testCompressedBatchPossibilitiesReturnCorrectMapAndOffset(
            String description, List<SimpleRecord> batchRecords, int maxDecompressedBytes,
            Set<Long> expectedPresentOffsets, long expectedLastResolvedOffset) throws Exception {
        MemoryRecords batch = MemoryRecords.withRecords(0L, Compression.gzip().build(),
            batchRecords.toArray(new SimpleRecord[0]));
        whenReadAsync(done(logReadResult(new FetchDataInfo(null, batch), Errors.NONE)));

        long lastOffset = batchRecords.size() - 1;
        ShareGroupDLQRecordFetcher.FetchResult result =
            fetcher(param(0L, lastOffset), maxDecompressedBytes).fetch().get(10, TimeUnit.SECONDS);

        assertEquals(expectedPresentOffsets.size(), result.records().size(), description);
        for (long offset = 0; offset <= lastOffset; offset++) {
            Record record = result.records().get(offset);
            if (expectedPresentOffsets.contains(offset)) {
                assertTrue(record != null, description + ": expected offset " + offset + " to be present");
                SimpleRecord expected = batchRecords.get((int) offset);
                assertArrayEquals(toArray(expected.key()), toArray(record.key()), description);
                assertArrayEquals(toArray(expected.value()), toArray(record.value()), description);
            } else {
                assertNull(record, description + ": expected offset " + offset + " to be absent");
            }
        }
        assertEquals(expectedLastResolvedOffset, result.lastResolvedOffset(), description);
    }

    // Covers every shape of compressed-batch decompression outcome: a single record that fits, a single
    // record that can never fit on its own, several small records that individually fit but collectively
    // exceed the budget, and a mix of one oversized record alongside several individually-fitting ones -
    // in the last two cases, decompressBounded() rejects the whole batch (see its class-level doc), so
    // even the individually-fitting records within it are excluded, not just the oversized one.
    private static Stream<Arguments> compressedBatchCases() {
        SimpleRecord small0 = record("k0", "v".repeat(20));
        SimpleRecord small1 = record("k1", "v".repeat(20));
        SimpleRecord small2 = record("k2", "v".repeat(20));
        SimpleRecord small3 = record("k3", "v".repeat(20));
        SimpleRecord small4 = record("k4", "v".repeat(20));
        SimpleRecord big0 = record("k0", "v".repeat(2000));
        int smallSize = recordSizeInBytes(small0);

        return Stream.of(
            Arguments.of("single small record fits within budget",
                List.of(small0), smallSize, Set.of(0L), 0L),
            Arguments.of("single large record can never fit and is permanently excluded",
                List.of(big0), smallSize, Set.of(), 0L),
            Arguments.of("multiple small records all fit within budget",
                List.of(small0, small1, small2), smallSize * 3, Set.of(0L, 1L, 2L), 2L),
            Arguments.of("many small records whose combined size exceeds budget - whole batch excluded",
                List.of(small0, small1, small2, small3, small4), smallSize * 3, Set.of(), 4L),
            Arguments.of("combination of one large and several small records - whole batch excluded",
                List.of(big0, small1, small2, small3, small4), smallSize * 3, Set.of(), 4L)
        );
    }

    @Test
    public void testSingleReadWithUncompressedMagicV2AndLegacyCompressedBatchesAllCollected() throws Exception {
        // One read returning three consecutive batches, each using a different collection path in
        // collectFromBatch()'s dispatch: uncompressed (collectUncompressed), magic v2 compressed
        // (collectFromCompressedBatch, the bounded-buffer path), and legacy magic v0/v1 compressed
        // (collectWithCumulativeCap, the streaming fallback). All comfortably fit within budget, so this
        // is purely about correct dispatch and offset/content tracking across a heterogeneous sequence,
        // not budget edge cases (already covered by the batch-specific tests).
        MemoryRecords uncompressedBatch = MemoryRecords.withRecords(0L, Compression.NONE, record("k0", "v0"));
        MemoryRecords magicV2CompressedBatch = MemoryRecords.withRecords(1L, Compression.gzip().build(), record("k1", "v1"));
        MemoryRecords legacyCompressedBatch = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 2L,
            Compression.gzip().build(), record("k2", "v2"));

        whenReadAsync(done(logReadResult(new FetchDataInfo(null,
            concatBatches(uncompressedBatch, magicV2CompressedBatch, legacyCompressedBatch)), Errors.NONE)));

        Map<Long, Record> result = fetcher(param(0L, 2L), 10_000).fetch().get(10, TimeUnit.SECONDS).records();

        assertEquals(3, result.size());
        assertRecord(result, 0L, "k0", "v0");
        assertRecord(result, 1L, "k1", "v1");
        assertRecord(result, 2L, "k2", "v2");
    }

    @Test
    public void testFetchStopsOnceEnoughContentCollectedWithoutScanningWholeRange() throws Exception {
        // A large range (0..100), but a budget sized to exactly the one record's own size: once that
        // record is collected the target has been reached, and the fetch stops issuing further reads
        // rather than scanning all the way to endOffset looking for more.
        SimpleRecord simpleRecord = record("k0", "v".repeat(50));
        int recordSize = recordSizeInBytes(simpleRecord);
        whenReadAsync(done(success(simpleRecord)));

        Map<Long, Record> result = fetcher(param(0L, 100L), recordSize).fetch().get(10, TimeUnit.SECONDS).records();

        assertEquals(1, result.size());
        assertRecord(result, 0L, "k0", "v".repeat(50));
        verify(logReader, times(1)).readAsync(any(), anySet(), any(), any(), anyBoolean());
    }

    @Test
    public void testMultipleBatchesInSingleReadStopAtBudgetBetweenBatches() throws Exception {
        // Two uncompressed batches concatenated into ONE read response - the first batch alone already
        // reaches the budget. collectRecords() must stop BEFORE starting the second batch (not just wait
        // for runFrom()'s between-reads check), or a single read containing many batches could keep being
        // processed past the point where the fetch should have stopped.
        String value = "v".repeat(50);
        MemoryRecords batch0 = MemoryRecords.withRecords(0L, Compression.NONE, record("k0", value));
        MemoryRecords batch1 = MemoryRecords.withRecords(1L, Compression.NONE, record("k1", value));
        int recordSize = recordSizeInBytes(record("k0", value));

        whenReadAsync(done(logReadResult(new FetchDataInfo(null, concatBatches(batch0, batch1)), Errors.NONE)));

        Map<Long, Record> result = fetcher(param(0L, 1L), recordSize).fetch().get(10, TimeUnit.SECONDS).records();

        assertEquals(1, result.size());
        assertRecord(result, 0L, "k0", value);
        assertNull(result.get(1L), "Second batch must not be collected once the first alone reached budget");
    }

    @Test
    public void testUncompressedBatchStopsMidBatchOnceBudgetReached() throws Exception {
        // A single uncompressed batch with several records, budget sized to exactly the first record's
        // own size - collectUncompressed() must stop mid-batch, not just between batches/reads, since a
        // single uncompressed batch carries no decompression risk and so has no other size cap of its own.
        // The record that would push collectedBytes over budget is excluded (not included), and the fetch
        // stops there rather than examining records after it in the same batch.
        String value = "v".repeat(50);
        int recordSize = recordSizeInBytes(record("k0", value));
        whenReadAsync(done(success(record("k0", value), record("k1", value), record("k2", value))));

        Map<Long, Record> result = fetcher(param(0L, 2L), recordSize).fetch().get(10, TimeUnit.SECONDS).records();

        assertEquals(1, result.size());
        assertRecord(result, 0L, "k0", value);
        assertNull(result.get(1L), "Budget was reached after the first record; one that doesn't fit alongside it must not be collected");
        assertNull(result.get(2L), "The fetch stops at the first record that doesn't fit; later records in the same batch must not be examined");
    }

    @Test
    public void testCompressedBatchAfterUncompressedRespectsCombinedBudget() throws Exception {
        // The first batch is uncompressed and alone consumes most of the budget. The second batch is
        // compressed and, on its own, decompresses to well within the full budget - decompression succeeds
        // - but it still must not be collected, because it doesn't fit alongside collectedBytes from the
        // uncompressed batch already retained this fetch. Proves the per-record fit check gates on
        // collectedBytes accumulated across every batch in the fetch, not just what the current batch
        // itself decompresses to.
        String uncompressedValue = "v".repeat(150);
        String compressedValue = "x".repeat(80);
        MemoryRecords uncompressedBatch = MemoryRecords.withRecords(0L, Compression.NONE, record("k0", uncompressedValue));
        MemoryRecords compressedBatch = MemoryRecords.withRecords(1L, Compression.gzip().build(), record("k1", compressedValue));

        whenReadAsync(done(logReadResult(
            new FetchDataInfo(null, concatBatches(uncompressedBatch, compressedBatch)), Errors.NONE)));

        Map<Long, Record> result = fetcher(param(0L, 1L), 200).fetch().get(10, TimeUnit.SECONDS).records();

        assertRecord(result, 0L, "k0", uncompressedValue);
        assertNull(result.get(1L),
            "Compressed batch must not be collected - it doesn't fit alongside what the uncompressed batch already retained");
    }

    @Test
    public void testOversizedUncompressedRecordExcludedButLaterRecordStillCollected() throws Exception {
        // The first record's own size exceeds the entire budget on its own - it can never fit, in this
        // fetch or any other - so it's permanently excluded rather than blocking the smaller record after
        // it, which fits comfortably since the excluded record never consumed any of collectedBytes.
        SimpleRecord small = record("k1", "v1");
        int budget = recordSizeInBytes(small);
        whenReadAsync(done(success(record("k0", "v".repeat(500)), small)));

        Map<Long, Record> result = fetcher(param(0L, 1L), budget).fetch().get(10, TimeUnit.SECONDS).records();

        assertNull(result.get(0L), "A record whose own size exceeds the budget must be permanently excluded");
        assertRecord(result, 1L, "k1", "v1");
    }

    @Test
    public void testHardStopPreventsFurtherReads() throws Exception {
        // Once a record is found that doesn't fit alongside what's already been collected, the fetch stops
        // immediately - it must not attempt further reads for the rest of the range, even though offsets
        // remain unexamined. Everything from that point on is left untouched for a fresh fetch to retry.
        String value = "v".repeat(50);
        int recordSize = recordSizeInBytes(record("k0", value));
        whenReadAsync(
            done(logReadResult(new FetchDataInfo(null,
                MemoryRecords.withRecords(0L, Compression.NONE, record("k0", value))), Errors.NONE)),
            done(logReadResult(new FetchDataInfo(null,
                MemoryRecords.withRecords(1L, Compression.NONE, record("k1", value))), Errors.NONE)),
            done(logReadResult(new FetchDataInfo(null,
                MemoryRecords.withRecords(2L, Compression.NONE, record("k2", value))), Errors.NONE)));

        ShareGroupDLQRecordFetcher.FetchResult result =
            fetcher(param(0L, 10L), recordSize + 1).fetch().get(10, TimeUnit.SECONDS);

        assertEquals(1, result.records().size());
        assertRecord(result.records(), 0L, "k0", value);
        assertEquals(0L, result.lastResolvedOffset(), "Only the first record was resolved; the rest must be left for a fresh fetch");
        verify(logReader, times(2)).readAsync(any(), anySet(), any(), any(), anyBoolean());
    }

    @Test
    public void testCompressedBatchExceedingDecompressedBudgetSkipsBatch() throws Exception {
        // Highly compressible values so the batch is small on the wire but decompresses well past a
        // tiny budget - simulates a decompression-bomb-shaped batch. The whole batch is decompressed
        // into a bounded buffer before any record is parsed, so a batch that can never fit within the
        // budget is permanently excluded, yielding none of its records rather than a partial subset.
        String bigValue = "x".repeat(10_000);
        whenReadAsync(done(success(Compression.gzip().build(),
            record("k0", bigValue), record("k1", bigValue), record("k2", bigValue))));

        ShareGroupDLQRecordFetcher.FetchResult result =
            fetcher(param(0L, 2L), 100).fetch().get(10, TimeUnit.SECONDS);

        assertTrue(result.records().isEmpty(), "Expected the over-budget batch to be skipped, yielding no records");
        assertEquals(2L, result.lastResolvedOffset(), "The whole excluded batch must count as resolved so it is never retried");
    }

    @Test
    public void testCompressedBatchExceedingDecompressedBudgetSkippedButLaterBatchStillCollected() throws Exception {
        // First batch is highly compressible so it alone decompresses well past a tiny budget; the second
        // batch is uncompressed and carries no decompression risk. Only the first batch's offsets should be
        // permanently excluded - the fetch must not abort the whole range, so the second batch is still
        // collected.
        String bigValue = "x".repeat(10_000);
        MemoryRecords compressedBatch = MemoryRecords.withRecords(0L, Compression.gzip().build(),
            record("k0", bigValue), record("k1", bigValue), record("k2", bigValue));
        MemoryRecords uncompressedBatch = MemoryRecords.withRecords(3L, Compression.NONE, record("k3", "v3"));

        whenReadAsync(done(logReadResult(
            new FetchDataInfo(null, concatBatches(compressedBatch, uncompressedBatch)), Errors.NONE)));

        Map<Long, Record> result = fetcher(param(0L, 3L), 100).fetch().get(10, TimeUnit.SECONDS).records();

        assertNull(result.get(0L));
        assertNull(result.get(1L));
        assertNull(result.get(2L));
        assertRecord(result, 3L, "k3", "v3");
    }

    @Test
    public void testLegacyCompressedBatchExceedingCumulativeCapSkipsBatchButLaterBatchStillCollected() throws Exception {
        // Legacy magic v0/v1 batches use the cumulative-cap fallback (no bounded-buffer path for that
        // format). Each record's own size is checked individually as it's decompressed: a record that can
        // never fit on its own is permanently excluded, but the scan continues rather than aborting the
        // whole fetch - a later, uncompressed batch remains unaffected.
        String bigValue = "x".repeat(10_000);
        MemoryRecords legacyCompressedBatch = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0L,
            Compression.gzip().build(), record("k0", bigValue), record("k1", bigValue), record("k2", bigValue));
        MemoryRecords uncompressedBatch = MemoryRecords.withRecords(3L, Compression.NONE, record("k3", "v3"));

        whenReadAsync(done(logReadResult(
            new FetchDataInfo(null, concatBatches(legacyCompressedBatch, uncompressedBatch)), Errors.NONE)));

        Map<Long, Record> result = fetcher(param(0L, 3L), 100).fetch().get(10, TimeUnit.SECONDS).records();

        assertNull(result.get(0L));
        assertNull(result.get(1L));
        assertNull(result.get(2L));
        assertRecord(result, 3L, "k3", "v3");
    }

    // Concatenates the given batches' on-wire bytes into a single Records, simulating one read returning
    // multiple consecutive batches.
    private static Records concatBatches(MemoryRecords... batches) {
        int size = 0;
        for (MemoryRecords batch : batches) {
            size += batch.buffer().remaining();
        }
        ByteBuffer combined = ByteBuffer.allocate(size);
        for (MemoryRecords batch : batches) {
            combined.put(batch.buffer().duplicate());
        }
        combined.flip();
        return MemoryRecords.readableRecords(combined);
    }

    @Test
    public void testSingleRecordWithFabricatedLengthRejectedWithoutLargeAllocation() throws Exception {
        // Distinct from testCompressedBatchExceedingDecompressedBudgetStopsEarly (which uses a real,
        // legitimately-compressible payload to blow a small budget): here the batch is tiny even once
        // decompressed - well within budget - but the single record's own declared length lies about how
        // much data follows it, simulating a maliciously corrupted record rather than a genuinely large
        // one. Verifies the record is rejected via the bounded buffer's own remaining-capacity check
        // (DefaultRecord.readFrom throwing InvalidRecordException) rather than the fetcher ever attempting
        // an allocation sized by the fabricated (here, ~1GB) declared length.
        whenReadAsync(done(logReadResult(new FetchDataInfo(null, corruptedLengthBatch()), Errors.NONE)));

        Map<Long, Record> result = fetch(param(0L, 0L));

        assertTrue(result.isEmpty(), "Corrupted record must be rejected, not force a large allocation");
    }

    // Builds a single-record, gzip-compressed batch whose record claims a declared body size (~1GB) far
    // larger than what actually follows it - fabricating a corrupted length rather than a genuinely large
    // payload, to exercise DefaultRecord.readFrom's bounds check in isolation from the budget check.
    private static Records corruptedLengthBatch() throws Exception {
        // A normal single-record gzip batch, to source real header + compressed record bytes from.
        MemoryRecords source = MemoryRecords.withRecords(Compression.gzip().build(), record("k", "v"));
        DefaultRecordBatch batch = (DefaultRecordBatch) source.batches().iterator().next();

        // Decompress the record region to plain bytes.
        ByteArrayOutputStream plainOut = new ByteArrayOutputStream();
        try (InputStream in = batch.recordInputStream(BufferSupplier.NO_CACHING)) {
            in.transferTo(plainOut);
        }
        byte[] plain = plainOut.toByteArray();

        // Replace the record's declared body-size varint (its first byte, for a record this small) with
        // one that claims a huge size, far more than actually remains.
        ByteBuffer corruptedPlain = ByteBuffer.allocate(plain.length + 8);
        ByteUtils.writeVarint(1_000_000_000, corruptedPlain);
        corruptedPlain.put(plain, 1, plain.length - 1);
        corruptedPlain.flip();
        byte[] corrupted = new byte[corruptedPlain.remaining()];
        corruptedPlain.get(corrupted);

        // Re-compress the corrupted plain bytes with the same codec.
        SingleByteBufferOutputStream compressedOut = new SingleByteBufferOutputStream(256);
        try (OutputStream out = Compression.gzip().build().wrapForOutput(compressedOut, RecordBatch.CURRENT_MAGIC_VALUE)) {
            out.write(corrupted);
        }
        compressedOut.buffer().flip();

        // Splice: the original header (unchanged) + newly-compressed corrupted record region, with the
        // batch's LENGTH field (Int32 at offset 8: size of everything after the length field itself)
        // patched to match the new total size.
        ByteBuffer originalFull = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(originalFull);
        originalFull.flip();
        originalFull.limit(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);

        ByteBuffer full = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD + compressedOut.buffer().remaining());
        full.put(originalFull);
        full.put(compressedOut.buffer());
        full.flip();
        full.putInt(8, full.limit() - 12);

        return MemoryRecords.readableRecords(full);
    }
}
