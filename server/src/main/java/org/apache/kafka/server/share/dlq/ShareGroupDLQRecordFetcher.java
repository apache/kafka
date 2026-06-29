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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.storage.internals.log.LogReadResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Reads the original source records for the offset range described by a {@link ShareGroupDLQRecordParameter}
 * so they can be copied into a DLQ record. Reads are issued one batch at a time in a loop via
 * {@link LogReader#readAsync}, which combines the local read with the follow-up remote read for any offset
 * tiered off the local log. When a read is already complete the loop continues in place; when it is still
 * pending (a remote read in flight) the loop returns and is resumed from the callback - so the calling
 * thread is never blocked on remote storage IO and the synchronous path never recurses.
 *
 * <p>Best-effort: the returned future always completes normally with whatever records could be read.
 * Offsets that cannot be read - locally or remotely - are simply absent from the map, leaving the caller
 * to produce a DLQ record with headers only for them.
 *
 * <p>Instances are single-use: create one fetcher per {@link #fetch()} call.
 */
public class ShareGroupDLQRecordFetcher {
    private static final Logger log = LoggerFactory.getLogger(ShareGroupDLQRecordFetcher.class);

    private final LogReader logReader;
    private final Time time;
    private final ShareGroupDLQRecordParameter param;

    private final TopicIdPartition tp;
    private final long endOffset;
    private final int recordCount;
    private final long startTime;
    private final Map<Long, Record> recordMap;
    // We are fetching data for one TopicIdPartition only. Hence, there is no need to keep recreating
    // the maxBytes map, and we can re-use a single copy. In similar vein, we needn't clear the offsets
    // map either and just update the value corresponding to the TopicIdPartition key across iterations.
    private final LinkedHashMap<TopicIdPartition, Long> offsets = new LinkedHashMap<>();
    private final LinkedHashMap<TopicIdPartition, Integer> maxBytesMap = new LinkedHashMap<>();
    private final CompletableFuture<Map<Long, Record>> result = new CompletableFuture<>();
    private final FetchParams fetchParams;

    public ShareGroupDLQRecordFetcher(LogReader logReader, Time time, ShareGroupDLQRecordParameter param, int maxFetchBytes) {
        this.logReader = logReader;
        this.time = time;
        this.param = param;
        this.tp = param.topicIdPartition();
        this.endOffset = param.lastOffset();
        this.recordCount = (int) (param.lastOffset() - param.firstOffset() + 1);
        this.startTime = time.hiResClockMs();
        this.recordMap = new HashMap<>(recordCount);
        this.maxBytesMap.put(tp, maxFetchBytes);
        this.fetchParams = new FetchParams(
            FetchRequest.CONSUMER_REPLICA_ID,           // -1, reading as a consumer
            -1,                                         // replicaEpoch
            0L,                                         // maxWaitMs - don't block
            1,                                          // minBytes
            maxFetchBytes,                              // maxBytes
            FetchIsolation.HIGH_WATERMARK,              // committed only
            Optional.empty()                            // clientMetadata
        );
    }

    /**
     * Fetches the source records for the configured offset range.
     *
     * @return A future that always completes normally with the records that could be read, keyed by offset.
     */
    public CompletableFuture<Map<Long, Record>> fetch() {
        try {
            runFrom(param.firstOffset());
        } catch (Exception e) {
            // Never let an unexpected error escape; skip record copy entirely.
            log.warn("Unexpected error fetching records for {}. Skipping record copy.", param, e);
            result.complete(Map.of());
        }
        return result;
    }

    /**
     * Drives the reads in a loop via {@link LogReader#readAsync}. When the per-offset read is already
     * complete (local data, or remote data already resolved) the loop continues in place; when it is still
     * pending (remote read in flight) the loop returns and is resumed from the callback - so the synchronous
     * path never recurses and the async path resumes on a fresh stack (the remote storage reader thread).
     */
    private void runFrom(long startFrom) {
        long nextOffset = startFrom;
        while (nextOffset <= endOffset) {
            offsets.put(tp, nextOffset);

            CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> future =
                logReader.readAsync(fetchParams, Set.of(tp), offsets, maxBytesMap, true);

            if (!future.isDone()) {
                // A remote read is in flight: resume from the callback so the calling thread is unblocked.
                long readFrom = nextOffset;
                future.whenComplete((results, exception) -> resume(readFrom, logReadResult(results), exception));
                return;
            }

            // Safe (non-blocking) because the future is done. readAsync is partial-data tolerant, so it
            // completes normally; any unexpected exceptional completion is caught by fetch().
            long advanced = collect(nextOffset, logReadResult(future.getNow(null)));
            if (advanced <= nextOffset) {
                complete();     // no progress, stop
                return;
            }
            nextOffset = advanced;
        }
        complete();
    }

    /**
     * Extracts the read result for the partition being fetched, or {@code null} when none was produced
     * (e.g. the read returned no entry for the partition).
     */
    private LogReadResult logReadResult(LinkedHashMap<TopicIdPartition, LogReadResult> results) {
        return results == null ? null : results.get(tp);
    }

    /**
     * Resumes the read loop after an asynchronous (remote) read completes. Runs after runFrom() has already
     * returned, so invoking runFrom() here does not grow the original call stack.
     */
    private void resume(long readFrom, LogReadResult logReadResult, Throwable exception) {
        try {
            if (exception != null) {
                log.warn("Unable to read records at offset {} for {}. Skipping it.", readFrom, param, exception);
                complete();
                return;
            }
            long advanced = collect(readFrom, logReadResult);
            if (advanced <= readFrom) {
                complete();         // no progress, stop
            } else {
                runFrom(advanced);  // resume the loop
            }
        } catch (Exception e) {
            log.warn("Unexpected error processing records for {}. Skipping record copy.", param, e);
            result.complete(Map.of());
        }
    }

    /**
     * Collects the records from a completed read into the map and returns the offset to read from next.
     * A read that failed (carries an error) or returned no usable data leaves the offsets unread (skipped),
     * which the loop treats as no progress.
     */
    private long collect(long readFrom, LogReadResult logReadResult) {
        if (logReadResult == null) {
            return readFrom;
        }
        if (logReadResult.error() != Errors.NONE) {
            log.warn("Unable to read records at offset {} for {} due to error {}. Skipping it.",
                readFrom, param, logReadResult.error());
            return readFrom;
        }
        return collectRecords(logReadResult.info().records, readFrom);
    }

    /**
     * Adds the records within the requested range to the map and returns the offset to read from next
     * (never moves backwards). Records below readFrom or above endOffset are ignored.
     */
    private long collectRecords(Records records, long readFrom) {
        long nextOffset = readFrom;
        for (RecordBatch batch : records.batches()) {
            for (Record record : batch) {
                // A fetch can return a batch whose base offset is below the requested offset, so skip
                // any record at or before the read position to avoid re-processing and dragging
                // nextOffset backwards.
                if (record.offset() < readFrom) continue;
                if (record.offset() > endOffset) return nextOffset;
                recordMap.put(record.offset(), record);
                nextOffset = Math.max(nextOffset, record.offset() + 1); // never moves backwards
            }
        }
        return nextOffset;
    }

    /**
     * Completes the result future with an immutable snapshot of the records collected so far. Offsets
     * that could not be read are absent from the map; the caller produces a headers-only DLQ record for them.
     */
    private void complete() {
        log.trace("Log fetch took {} ms for {} records starting at {} for {}", time.hiResClockMs() - startTime,
            recordCount, param.firstOffset(), param);
        if (recordCount != recordMap.size()) {
            log.info("Total offsets requested: {}, Records found: {}", recordCount, recordMap.size());
        }
        result.complete(Map.copyOf(recordMap));
    }
}
