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
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Reads the original source records for the offset range described by a {@link ShareGroupDLQRecordParameter}
 * so they can be copied into a DLQ record. Local reads are performed inline in a loop; when an offset has
 * been tiered off the local log the records are fetched asynchronously from the remote tier and the loop
 * resumes once the read completes (so the caller's thread is never blocked on remote storage IO).
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

    // Visibility for testing
    CompletableFuture<Map<Long, Record>> result() {
        return result;
    }

    /**
     * Drives synchronous local reads in a loop. When an offset resides in the remote tier the records
     * are read asynchronously: if the remote read is already complete the loop continues in place, and
     * if it is still pending the loop returns and is resumed from the callback - so the synchronous path
     * never recurses and the async path resumes on a fresh stack (the remote storage reader thread).
     */
    private void runFrom(long startFrom) {
        long nextOffset = startFrom;
        while (nextOffset <= endOffset) {
            OptionalLong advanced = fetchLocal(nextOffset);
            if (advanced.isEmpty()) {
                // The loop stopped: the result has already been completed (read error, or no progress),
                // or a remote read is pending and will resume the loop from its callback.
                return;
            }
            nextOffset = advanced.getAsLong();
        }
        complete();
    }

    /**
     * Reads readFrom from the local log. If the offset has been tiered off the local log, delegates to
     * the remote tier. Returns the next offset to read from so the loop can continue, or
     * OptionalLong.empty() if the loop should stop now - either because the result has been completed
     * (read error, or no progress) or because a remote read is pending and will resume from its callback.
     */
    // Visibility for testing
    OptionalLong fetchLocal(long readFrom) {
        offsets.put(tp, readFrom);

        LinkedHashMap<TopicIdPartition, LogReadResult> readResult =
            logReader.read(fetchParams, Set.of(tp), offsets, maxBytesMap);

        LogReadResult res = readResult.get(tp);
        if (res == null) {
            log.warn("Unable to fetch actual record at offset {} for {}.", readFrom, param);
            result.complete(Map.of());
            return OptionalLong.empty();
        }

        if (res.error().code() != Errors.NONE.code()) {
            log.warn("Unable to fetch actual record at offset {} for {} due to error {}.",
                readFrom, param, res.error());
            result.complete(Map.of());
            return OptionalLong.empty();
        }

        if (res.info().delayedRemoteStorageFetch.isPresent()) {
            return fetchRemote(res.info().delayedRemoteStorageFetch.get(), readFrom);
        }

        long advanced = collectRecords(res.info().records, readFrom);
        // If the read position did not advance this iteration we have made no progress (reached HWM/LEO
        // or only stale records were returned). Bail out to guarantee termination rather than
        // re-fetching the same offset forever.
        if (advanced <= readFrom) {
            complete();     // no progress, stop
            return OptionalLong.empty();
        }
        return OptionalLong.of(advanced);
    }

    /**
     * Reads a tiered offset from the remote tier. Returns the next offset to read from so the loop can
     * continue, or OptionalLong.empty() if the loop should stop now - either because the read is still
     * pending (it will resume from its callback on the remote storage reader thread, leaving the sender
     * thread unblocked) or because the read made no progress and the result has already been completed.
     */
    // Visibility for testing
    OptionalLong fetchRemote(RemoteStorageFetchInfo remoteStorageFetchInfo, long readFrom) {
        CompletableFuture<FetchDataInfo> remote = logReader.readRemote(remoteStorageFetchInfo);

        if (!remote.isDone()) {
            remote.whenComplete((fetchDataInfo, exception) -> resumeRemote(readFrom, fetchDataInfo, exception));
            return OptionalLong.empty();
        }

        // The read is already complete, so process it in place without blocking and keep looping.
        FetchDataInfo fetchDataInfo = null;
        Throwable exception = null;
        try {
            // Safe (non-blocking) because the future is done.
            fetchDataInfo = remote.getNow(null);
        } catch (CompletionException e) {
            exception = e.getCause();
        }

        long advanced = processRemoteOutcome(readFrom, fetchDataInfo, exception);
        if (advanced <= readFrom) {
            complete();     // no progress, stop
            return OptionalLong.empty();
        }
        return OptionalLong.of(advanced);
    }

    /**
     * Resumes the read loop after an asynchronous remote read completes. Runs after runFrom() has
     * already returned, so invoking runFrom() here does not grow the original call stack.
     */
    private void resumeRemote(long readFrom, FetchDataInfo fetchDataInfo, Throwable exception) {
        try {
            long advanced = processRemoteOutcome(readFrom, fetchDataInfo, exception);
            if (advanced <= readFrom) {
                complete();         // no progress, stop
            } else {
                runFrom(advanced);  // resume the loop
            }
        } catch (Exception e) {
            log.warn("Unexpected error processing remote records for {}. Skipping record copy.", param, e);
            result.complete(Map.of());
        }
    }

    /**
     * Turns the outcome of a remote read into records and collects them. A failed or empty read leaves
     * the offsets unread (skipped), consistent with any other unavailable offset.
     */
    private long processRemoteOutcome(long readFrom, FetchDataInfo fetchDataInfo, Throwable exception) {
        Records records;
        if (exception != null || fetchDataInfo == null) {
            log.warn("Offset {} for {} is in remote storage but could not be read. Skipping it.", readFrom, param, exception);
            records = MemoryRecords.EMPTY;
        } else {
            records = fetchDataInfo.records;
        }
        return collectRecords(records, readFrom);
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
