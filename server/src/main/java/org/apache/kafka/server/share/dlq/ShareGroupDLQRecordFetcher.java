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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.DefaultRecord;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.CloseableIterator;
import org.apache.kafka.common.utils.internals.SingleByteBufferOutputStream;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.storage.internals.log.LogReadResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
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
 * <p>Best-effort: the returned future always completes normally with a {@link FetchResult} holding whatever
 * records could be read, plus the offset through which a definitive outcome was reached this fetch. Offsets
 * within that boundary that have no entry in {@link FetchResult#records()} were either permanently excluded
 * because they (or their containing batch) can never fit within {@code maxDecompressedBytes}, or belong to
 * a range given up on entirely after a genuine failure (a read error, or an unexpected exception) - either
 * way, the caller should produce a DLQ record with headers only for them. Offsets beyond the boundary were
 * never examined because this fetch ran out of budget before reaching them, and are eligible for a fresh
 * fetch with its own full budget.
 *
 * <p>{@code maxFetchBytes} only bounds the compressed, on-the-wire size of each read; a compressed batch
 * can still decompress into something far larger in memory. To keep a pathologically compressible batch
 * from ballooning broker heap usage, each batch is decompressed into a size-capped flat buffer before any
 * record is parsed (see {@link #decompressBounded}), bounded by {@code maxDecompressedBytes} - so even a
 * single record with a fabricated huge length can't force a large allocation. A batch whose decompressed
 * size exceeds {@code maxDecompressedBytes} on its own can never fit in any fetch, so it is permanently
 * excluded rather than retried.
 *
 * <p>{@code maxDecompressedBytes} also bounds the total amount of content this fetch collects: once the
 * records actually collected (compressed or not) reach that many bytes, or a record is found that doesn't
 * fit alongside what's already been collected, the fetch stops rather than continuing to walk the rest of
 * the requested range - reading further would just be discarded work, since callers only ever need one
 * message's worth of records at a time (see {@code ShareGroupDLQStateManager}, which resolves one produce
 * round at a time and passes its {@code dlqTopicMaxMessageBytes} as this budget).
 *
 * <p>Instances are single-use: create one fetcher per {@link #fetch()} call.
 */
public class ShareGroupDLQRecordFetcher {
    private static final Logger log = LoggerFactory.getLogger(ShareGroupDLQRecordFetcher.class);

    // Size of the transfer buffer used to pull decompressed bytes out of the codec's stream a chunk at a
    // time (see decompressBounded). Fixed and deliberately independent of maxDecompressedBytes: it only
    // affects how many read() calls happen, not whether the budget check is correct, so tying it to the
    // (policy-configured, potentially large) budget would allocate a scratch buffer sized by configuration
    // rather than by the data actually being copied. Matches the precedent in ClientTelemetryUtils.decompress.
    private static final int DECOMPRESS_CHUNK_BYTES = 8 * 1024;

    private final LogReader logReader;
    private final Time time;
    private final ShareGroupDLQRecordParameter param;

    private final TopicIdPartition tp;
    private final long endOffset;
    private final int recordCount;
    private final long startTime;
    private final Map<Long, Record> recordMap;
    private final long maxDecompressedBytes;
    private final BufferSupplier bufferSupplier = BufferSupplier.create();
    // Cumulative sizeInBytes() of every record actually collected into recordMap so far, regardless of
    // whether it came from a compressed or uncompressed batch. Compared against maxDecompressedBytes to
    // decide whether the next record still fits alongside what's already been collected this fetch.
    private long collectedBytes = 0;
    // The largest offset such that every offset from param.firstOffset() through it has a definitive,
    // in-scope outcome this fetch: collected into recordMap, or permanently excluded because it (or its
    // containing batch) can never fit within maxDecompressedBytes. Never advances past a record or batch
    // that was merely deferred for not fitting alongside collectedBytes - those, and everything after them,
    // are left untouched for a fresh fetch (with its own full budget) to attempt. Starts one before the
    // requested range so a fetch that resolves nothing still reports that correctly.
    private long lastResolvedOffset;
    // Set once a record is found that doesn't fit alongside collectedBytes but isn't itself permanently
    // excluded - signals every loop in this class to stop immediately rather than trying further batches or
    // reads, since that record and everything after it must be left untouched for a fresh fetch to attempt.
    private boolean deferredRemainder = false;
    // We are fetching data for one TopicIdPartition only. Hence, there is no need to keep recreating
    // the maxBytes map, and we can re-use a single copy. In similar vein, we needn't clear the offsets
    // map either and just update the value corresponding to the TopicIdPartition key across iterations.
    private final LinkedHashMap<TopicIdPartition, Long> offsets = new LinkedHashMap<>();
    private final LinkedHashMap<TopicIdPartition, Integer> maxBytesMap = new LinkedHashMap<>();
    private final CompletableFuture<FetchResult> result = new CompletableFuture<>();
    private final FetchParams fetchParams;

    public ShareGroupDLQRecordFetcher(LogReader logReader, Time time, ShareGroupDLQRecordParameter param,
                                       int maxFetchBytes, int maxDecompressedBytes) {
        this.logReader = logReader;
        this.time = time;
        this.param = param;
        this.tp = param.topicIdPartition();
        this.endOffset = param.lastOffset();
        this.recordCount = (int) (param.lastOffset() - param.firstOffset() + 1);
        this.startTime = time.hiResClockMs();
        this.recordMap = new HashMap<>(recordCount);
        this.maxBytesMap.put(tp, maxFetchBytes);
        this.maxDecompressedBytes = maxDecompressedBytes;
        this.lastResolvedOffset = param.firstOffset() - 1;
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
     * @return A future that always completes normally with the records that could be read, plus the
     *         boundary through which a definitive outcome was reached - see {@link FetchResult}.
     */
    public CompletableFuture<FetchResult> fetch() {
        try {
            runFrom(param.firstOffset());
        } catch (Throwable e) {
            // Never let an unexpected error - including an OutOfMemoryError from a maliciously
            // compressible record, or an InvalidRecordException/KafkaException from a rejected
            // malformed one - escape. Uses completeGivingUpOnRemainder() (not result.complete(...))
            // so that any records already collected from earlier batches in this call are still
            // returned, rather than discarding a partially-successful copy because of one bad batch.
            log.warn("Unexpected error fetching records for {}. Returning records fetched so far.", param, e);
            completeGivingUpOnRemainder();
        }
        return result;
    }

    /**
     * Drives the reads in a loop via {@link LogReader#readAsync}. When the per-offset read is already
     * complete (local data, or remote data already resolved) the loop continues in place; when it is still
     * pending (remote read in flight) the loop returns and is resumed from the callback - so the synchronous
     * path never recurses and the async path resumes on a fresh stack (the remote storage reader thread).
     *
     * <p>Also stops once {@link #shouldStop()} is true - there's no need to keep reading once enough usable
     * content has been collected for the caller's purposes, or a record has been found that doesn't fit
     * alongside it, even if {@link #endOffset} hasn't been reached yet.
     */
    private void runFrom(long startFrom) {
        long nextOffset = startFrom;
        while (nextOffset <= endOffset && !shouldStop()) {
            offsets.put(tp, nextOffset);

            CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> future =
                logReader.readAsync(fetchParams, Set.of(tp), offsets, maxBytesMap, true);

            if (!future.isDone()) {
                // A remote read is in flight: resume from the callback so the calling thread is unblocked.
                // Uses whenCompleteAsync (not whenComplete) to close a race between the isDone() check
                // above and callback registration: if the future completes in that gap, whenComplete
                // would run the callback inline on this thread, letting resume() recurse into runFrom()
                // on the same stack. whenCompleteAsync always dispatches off-thread, so that can't happen.
                long readFrom = nextOffset;
                future.whenCompleteAsync((results, exception) -> resume(readFrom, logReadResult(results), exception));
                return;
            }

            // Safe (non-blocking) because the future is done. readAsync is partial-data tolerant, so it
            // completes normally; any unexpected exceptional completion is caught by fetch().
            long advanced = collect(nextOffset, logReadResult(future.getNow(null)));
            if (advanced <= nextOffset) {
                completeAfterNoProgress();     // no progress - stop
                return;
            }
            nextOffset = advanced;
        }
        complete();
    }

    /**
     * True once enough content has been collected for the caller's purposes, or a record has been found
     * that doesn't fit alongside {@link #collectedBytes} - either way, every loop in this class should stop
     * immediately rather than attempting further batches or reads.
     */
    private boolean shouldStop() {
        return deferredRemainder || collectedBytes >= maxDecompressedBytes;
    }

    /**
     * Completes after a read makes no progress. This happens for two very different reasons that need
     * different handling: {@link #deferredRemainder} being set means a record was found that doesn't fit
     * alongside {@link #collectedBytes} - a budget-related stop worth retrying fresh, so the boundary is
     * preserved as-is via {@link #complete()}. Otherwise, nothing advanced because the read itself found
     * nothing new (an error, an empty result, or a batch containing only already-seen offsets) - a genuine
     * dead end that a retry with a fresh budget wouldn't fix, so {@link #completeGivingUpOnRemainder()}
     * gives up on the rest of the range instead of leaving it to be retried forever.
     */
    private void completeAfterNoProgress() {
        if (deferredRemainder) {
            complete();
        } else {
            completeGivingUpOnRemainder();
        }
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
                completeGivingUpOnRemainder();
                return;
            }
            long advanced = collect(readFrom, logReadResult);
            if (advanced <= readFrom) {
                completeAfterNoProgress();  // no progress - stop
            } else {
                runFrom(advanced);          // resume the loop
            }
        } catch (Throwable e) {
            // Never let an unexpected error - including an OutOfMemoryError from a maliciously
            // compressible record, or an InvalidRecordException/KafkaException from a rejected
            // malformed one - escape; return whatever was collected so far.
            log.warn("Unexpected error processing records for {}. Returning records fetched so far.", param, e);
            completeGivingUpOnRemainder();
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
     *
     * <p>Also stops before starting a new batch once {@link #shouldStop()} is true - a single read can
     * return many batches (up to {@code maxFetchBytes} worth), so this check can't wait for {@link
     * #runFrom}'s between-reads check alone, or a read containing several batches could keep being
     * processed past the point where the fetch should have stopped.
     */
    private long collectRecords(Records records, long readFrom) {
        long nextOffset = readFrom;
        for (RecordBatch batch : records.batches()) {
            if (shouldStop()) return nextOffset;
            nextOffset = collectFromBatch(batch, nextOffset);
        }
        return nextOffset;
    }

    private long collectFromBatch(RecordBatch batch, long readFrom) {
        if (!batch.isCompressed()) {
            // No decompression risk to bound: the on-wire size already bounds what's in memory, so no
            // cap applies (unlike the compressed cases below, capping this would reject legitimately
            // large uncompressed records for no safety benefit).
            return collectUncompressed(batch, readFrom);
        }
        if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            return collectFromCompressedBatch(asDefaultRecordBatch(batch), readFrom);
        }
        // Legacy magic v0/v1 compressed batch, effectively unseen in practice: no bounded-buffer path
        // available for that format, so fall back to the cumulative cap.
        return collectWithCumulativeCap(batch, readFrom);
    }

    private long collectUncompressed(RecordBatch batch, long readFrom) {
        long nextOffset = readFrom;
        for (Record record : batch) {
            // A fetch can return a batch whose base offset is below the requested offset, so skip
            // any record at or before the read position to avoid re-processing and dragging
            // nextOffset backwards.
            if (record.offset() < readFrom) continue;
            if (record.offset() > endOffset) return nextOffset;
            long recordSize = record.sizeInBytes();
            if (recordSize > maxDecompressedBytes) {
                // This one record can never fit within maxDecompressedBytes, in this fetch or any other -
                // exclude it permanently rather than leaving it for a retry that could never succeed.
                nextOffset = Math.max(nextOffset, record.offset() + 1);
                lastResolvedOffset = Math.max(lastResolvedOffset, record.offset());
                continue;
            }
            if (collectedBytes + recordSize > maxDecompressedBytes) {
                // Fits on its own, but not alongside what's already been collected this fetch - stop here
                // and leave this record, and everything after it, untouched for a fresh fetch to attempt.
                deferredRemainder = true;
                return nextOffset;
            }
            recordMap.put(record.offset(), record);
            collectedBytes += recordSize;
            nextOffset = Math.max(nextOffset, record.offset() + 1); // never moves backwards
            lastResolvedOffset = Math.max(lastResolvedOffset, record.offset());
        }
        return nextOffset;
    }

    /**
     * Materializes the batch's own on-wire bytes into a plain in-memory {@link DefaultRecordBatch}. This is
     * needed because a real local log read ({@code FileRecords#batches()}) returns a lazy file-channel-backed
     * wrapper ({@code DefaultRecordBatch.DefaultFileChannelRecordBatch}) whose underlying
     * {@code DefaultRecordBatch} is not reachable from outside {@code org.apache.kafka.common.record.internal}
     * (package-private constructor, protected loader) - only already-in-memory batches (e.g. {@link
     * MemoryRecords}) are directly a {@code DefaultRecordBatch}. The copy is bounded by {@link
     * RecordBatch#sizeInBytes()} - the compressed, on-wire size, itself already bounded by this fetch's
     * {@code maxFetchBytes} cap - so this is safe regardless of how large the batch decompresses to.
     */
    private DefaultRecordBatch asDefaultRecordBatch(RecordBatch batch) {
        if (batch instanceof DefaultRecordBatch defaultBatch) {
            return defaultBatch;
        }
        ByteBuffer buffer = ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(buffer);
        buffer.flip();
        return (DefaultRecordBatch) MemoryRecords.readableRecords(buffer).batches().iterator().next();
    }

    /**
     * Decompresses the batch into a size-bounded buffer first (see {@link #decompressBounded}), then parses
     * records out of that already-bounded buffer via the slice-based {@link DefaultRecord#readFrom(ByteBuffer,
     * long, long, int, Long)} - the same one the uncompressed path uses - so a record's declared length can
     * never cause an allocation larger than the buffer we already control.
     */
    private long collectFromCompressedBatch(DefaultRecordBatch batch, long readFrom) {
        ByteBuffer decompressed = decompressBounded(batch);
        if (decompressed == null) {
            // The batch's decompressed size exceeds maxDecompressedBytes on its own, so it can never fit -
            // in this fetch or any other. Exclude it permanently and move on to whatever batches follow.
            long lastOffsetInRange = Math.min(batch.lastOffset(), endOffset);
            lastResolvedOffset = Math.max(lastResolvedOffset, lastOffsetInRange);
            return Math.max(readFrom, lastOffsetInRange + 1);
        }

        long baseOffset = batch.baseOffset();
        long baseTimestamp = batch.baseTimestamp();
        int baseSequence = batch.baseSequence();
        Long logAppendTime = batch.timestampType() == TimestampType.LOG_APPEND_TIME ? batch.maxTimestamp() : null;

        long nextOffset = readFrom;
        while (decompressed.hasRemaining()) {
            Record record = DefaultRecord.readFrom(decompressed, baseOffset, baseTimestamp, baseSequence, logAppendTime);
            // A fetch can return a batch whose base offset is below the requested offset, so skip
            // any record at or before the read position to avoid re-processing and dragging
            // nextOffset backwards.
            if (record.offset() < readFrom) continue;
            if (record.offset() > endOffset) return nextOffset;
            long recordSize = record.sizeInBytes();
            if (collectedBytes + recordSize > maxDecompressedBytes) {
                // The whole batch already fit within maxDecompressedBytes on its own (decompression would
                // have failed above otherwise), so this record only fails to fit alongside what's already
                // been collected - stop here and leave it, and everything after it, untouched for a fresh
                // fetch to attempt.
                deferredRemainder = true;
                return nextOffset;
            }
            recordMap.put(record.offset(), record);
            collectedBytes += recordSize;
            nextOffset = Math.max(nextOffset, record.offset() + 1); // never moves backwards
            lastResolvedOffset = Math.max(lastResolvedOffset, record.offset());
        }
        return nextOffset;
    }

    /**
     * Decompresses the batch's record region into a flat buffer, reading in {@link #DECOMPRESS_CHUNK_BYTES}
     * chunks and checking the cumulative total against {@link #maxDecompressedBytes} before each chunk is
     * kept - before any record-level parsing happens. A single record with a fabricated huge length can't
     * cause a large allocation this way: parsing only begins once the buffer is already fully bounded, and
     * the buffer-based reader rejects a record whose declared length doesn't fit what's left of it.
     *
     * <p>Bounded by the full {@code maxDecompressedBytes} regardless of {@link #collectedBytes}: whether a
     * batch fits is a property of the batch alone, not of how much of the budget earlier batches in this
     * fetch happened to use, so every batch gets the same, full-sized chance to decompress.
     *
     * @return the bounded, flipped buffer ready for reading, or {@code null} if the batch's decompressed
     *         size exceeds {@link #maxDecompressedBytes} on its own.
     */
    private ByteBuffer decompressBounded(DefaultRecordBatch batch) {
        int chunkBytes = Math.min(batch.sizeInBytes(), DECOMPRESS_CHUNK_BYTES);
        try (InputStream in = batch.recordInputStream(bufferSupplier);
             SingleByteBufferOutputStream out = new SingleByteBufferOutputStream(chunkBytes)) {
            byte[] chunk = new byte[chunkBytes];
            int nRead;
            long total = 0;
            while ((nRead = in.read(chunk, 0, chunk.length)) != -1) {
                total += nRead;
                if (total > maxDecompressedBytes) {
                    log.warn("Decompressed batch data for {} exceeded the {} byte budget on its own. " +
                        "The batch can never fit and will be permanently skipped.", param, maxDecompressedBytes);
                    return null;
                }
                out.write(chunk, 0, nRead);
            }
            out.buffer().flip();
            return out.buffer();
        } catch (IOException e) {
            throw new KafkaException("Failed to decompress batch for " + param, e);
        }
    }

    /**
     * Fallback used only for legacy magic v0/v1 compressed batches (no bounded-buffer path available for
     * that format - see {@link #asDefaultRecordBatch}): iterates records one at a time via {@link
     * RecordBatch#streamingIterator}. Unlike {@link #collectFromCompressedBatch}, this does not prevent a
     * single oversized record's initial allocation before its size is known - it only decides, once a
     * record has already been decompressed, whether to keep it or discard it.
     */
    private long collectWithCumulativeCap(RecordBatch batch, long readFrom) {
        long nextOffset = readFrom;
        try (CloseableIterator<Record> iterator = batch.streamingIterator(bufferSupplier)) {
            while (iterator.hasNext()) {
                Record record = iterator.next();
                if (record.offset() < readFrom) continue;
                if (record.offset() > endOffset) return nextOffset;

                long recordSize = record.sizeInBytes();
                if (recordSize > maxDecompressedBytes) {
                    // This one record can never fit within maxDecompressedBytes, in this fetch or any
                    // other - exclude it permanently rather than leaving it for a retry that could never
                    // succeed.
                    nextOffset = Math.max(nextOffset, record.offset() + 1);
                    lastResolvedOffset = Math.max(lastResolvedOffset, record.offset());
                    continue;
                }
                if (collectedBytes + recordSize > maxDecompressedBytes) {
                    // Fits on its own, but not alongside what's already been collected this fetch - stop
                    // here and leave it, and everything after it, untouched for a fresh fetch to attempt.
                    deferredRemainder = true;
                    return nextOffset;
                }

                recordMap.put(record.offset(), record);
                collectedBytes += recordSize;
                nextOffset = Math.max(nextOffset, record.offset() + 1);
                lastResolvedOffset = Math.max(lastResolvedOffset, record.offset());
            }
        }
        return nextOffset;
    }

    /**
     * Completes the result future with an immutable snapshot of the records collected so far, plus the
     * boundary through which a definitive outcome was reached this fetch.
     */
    private void complete() {
        bufferSupplier.close();
        log.trace("Log fetch took {} ms for {} records starting at {} for {}", time.hiResClockMs() - startTime,
            recordCount, param.firstOffset(), param);
        if (recordCount != recordMap.size()) {
            log.info("Total offsets requested: {}, Records found: {}", recordCount, recordMap.size());
        }
        result.complete(new FetchResult(Map.copyOf(recordMap), lastResolvedOffset));
    }

    /**
     * Like {@link #complete()}, but first treats the entire remaining range - from wherever this fetch got
     * to, through {@link #endOffset} - as settled. Used when the reason for stopping is a genuine failure
     * (a read error, an unexpected exception, or a read that made no progress) rather than running out of
     * budget: unlike a budget-related stop, retrying a failure like this with a fresh budget wouldn't help,
     * so there's nothing to gain by leaving the remainder for a later fetch to retry - the caller should
     * treat it as permanently unavailable and produce headers only for it, the same as any offset excluded
     * for not fitting the decompression budget.
     */
    private void completeGivingUpOnRemainder() {
        lastResolvedOffset = Math.max(lastResolvedOffset, endOffset);
        complete();
    }

    /**
     * @param records the source records that could be read, keyed by offset
     * @param lastResolvedOffset the largest offset such that every offset from the requested range's start
     *                           through it has a definitive, in-scope outcome - collected into
     *                           {@code records}, or permanently excluded because it (or its containing
     *                           batch) can never fit within the configured decompression budget. Offsets
     *                           beyond it were never examined and should be retried, with a fresh budget,
     *                           by a later fetch.
     */
    public record FetchResult(Map<Long, Record> records, long lastResolvedOffset) {
    }
}
