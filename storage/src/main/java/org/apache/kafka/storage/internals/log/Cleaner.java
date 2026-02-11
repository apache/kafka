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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.utils.Throttler;

import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * This class holds the actual logic for cleaning a log.
 */
public class Cleaner {
    private final BufferSupplier decompressionBufferSupplier = BufferSupplier.create();

    private final Logger logger;
    private final int id;
    private final OffsetMap offsetMap;
    private final int ioBufferSize;
    private final int maxIoBufferSize;
    private final double dupBufferLoadFactor;
    private final Throttler throttler;
    private final Time time;
    private final Consumer<TopicPartition> checkDone;

    /**
     * Buffer used for read i/o
     */
    private ByteBuffer readBuffer;

    /**
     * Buffer used for write i/o
     */
    private ByteBuffer writeBuffer;

    /**
     *
     * @param id An identifier used for logging
     * @param offsetMap The map used for deduplication
     * @param ioBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
     * @param maxIoBufferSize The maximum size of a message that can appear in the log
     * @param dupBufferLoadFactor The maximum percent full for the deduplication buffer
     * @param throttler The throttler instance to use for limiting I/O rate
     * @param time The time instance
     * @param checkDone Check if the cleaning for a partition is finished or aborted
     */
    public Cleaner(int id,
                   OffsetMap offsetMap,
                   int ioBufferSize,
                   int maxIoBufferSize,
                   double dupBufferLoadFactor,
                   Throttler throttler,
                   Time time,
                   Consumer<TopicPartition> checkDone) {
        this.id = id;
        this.offsetMap = offsetMap;
        this.ioBufferSize = ioBufferSize;
        this.maxIoBufferSize = maxIoBufferSize;
        this.dupBufferLoadFactor = dupBufferLoadFactor;
        this.throttler = throttler;
        this.time = time;
        this.checkDone = checkDone;
        logger = new LogContext("Cleaner " + id + ": ").logger(Cleaner.class);

        readBuffer = ByteBuffer.allocate(ioBufferSize);
        writeBuffer = ByteBuffer.allocate(ioBufferSize);

        assert offsetMap.slots() * dupBufferLoadFactor > 1 :
                "offset map is too small to fit in even a single message, so log cleaning will never make progress. " +
                        "You can increase log.cleaner.dedupe.buffer.size or decrease log.cleaner.threads";
    }

    public int id() {
        return id;
    }

    // Only for testing
    public OffsetMap offsetMap() {
        return offsetMap;
    }

    /**
     * Clean the given log.
     *
     * @param cleanable The log to be cleaned
     *
     * @return The first offset not cleaned and the statistics for this round of cleaning
     */
    public Map.Entry<Long, CleanerStats> clean(LogToClean cleanable) throws IOException, DigestException {
        return doClean(cleanable, time.milliseconds());
    }

    /**
     * Clean the given log.
     *
     * @param cleanable The log to be cleaned
     * @param currentTime The current timestamp for doing cleaning
     *
     * @return The first offset not cleaned and the statistics for this round of cleaning
     * */
    public Map.Entry<Long, CleanerStats> doClean(LogToClean cleanable, long currentTime) throws IOException, DigestException {
        UnifiedLog log = cleanable.log();

        logger.info("Beginning cleaning of log {}", log.name());

        // figure out the timestamp below which it is safe to remove delete tombstones
        // this position is defined to be a configurable time beneath the last modified time of the last clean segment
        // this timestamp is only used on the older message formats older than MAGIC_VALUE_V2
        List<LogSegment> segments = log.logSegments(0, cleanable.firstDirtyOffset());
        long legacyDeleteHorizonMs = segments.isEmpty()
                ? 0L
                : segments.get(segments.size() - 1).lastModified() - log.config().deleteRetentionMs;

        CleanerStats stats = new CleanerStats(Time.SYSTEM);

        // build the offset map
        logger.info("Building offset map for {}...", log.name());
        long upperBoundOffset = cleanable.firstUncleanableOffset();
        buildOffsetMap(log, cleanable.firstDirtyOffset(), upperBoundOffset, offsetMap, stats);
        long endOffset = offsetMap.latestOffset() + 1;
        stats.indexDone();

        // determine the timestamp up to which the log will be cleaned
        // this is the lower of the last active segment and the compaction lag
        segments = log.logSegments(0, cleanable.firstUncleanableOffset());
        long cleanableHorizonMs = segments.isEmpty()
                ? 0L
                : segments.get(segments.size() - 1).lastModified();

        // group the segments and clean the groups
        logger.info("Cleaning log {} (cleaning prior to {}, discarding tombstones prior to upper bound deletion horizon {})...",
                log.name(), new Date(cleanableHorizonMs), new Date(legacyDeleteHorizonMs));
        CleanedTransactionMetadata transactionMetadata = new CleanedTransactionMetadata();

        List<List<LogSegment>> groupedSegments = groupSegmentsBySize(
                log.logSegments(0, endOffset),
                log.config().segmentSize(),
                log.config().maxIndexSize,
                cleanable.firstUncleanableOffset()
        );

        for (List<LogSegment> group : groupedSegments) {
            cleanSegments(log, group, offsetMap, currentTime, stats, transactionMetadata, legacyDeleteHorizonMs, upperBoundOffset);
        }

        // record buffer utilization
        stats.bufferUtilization = offsetMap.utilization();

        stats.allDone();

        return Map.entry(endOffset, stats);
    }

    /**
     * Clean a group of segments into a single replacement segment.
     *
     * @param log The log being cleaned
     * @param segments The group of segments being cleaned
     * @param map The offset map to use for cleaning segments
     * @param currentTime The current time in milliseconds
     * @param stats Collector for cleaning statistics
     * @param transactionMetadata State of ongoing transactions which is carried between the cleaning
     *                            of the grouped segments
     * @param legacyDeleteHorizonMs The delete horizon used for tombstones whose version is less than 2
     * @param upperBoundOffsetOfCleaningRound The upper bound offset of this round of cleaning
     */
    @SuppressWarnings("finally")
    public void cleanSegments(UnifiedLog log,
                               List<LogSegment> segments,
                               OffsetMap map,
                               long currentTime,
                               CleanerStats stats,
                               CleanedTransactionMetadata transactionMetadata,
                               long legacyDeleteHorizonMs,
                               long upperBoundOffsetOfCleaningRound) throws IOException {
        // create a new segment with a suffix appended to the name of the log and indexes
        LogSegment cleaned = UnifiedLog.createNewCleanedSegment(log.dir(), log.config(), segments.get(0).baseOffset());
        transactionMetadata.setCleanedIndex(Optional.of(cleaned.txnIndex()));

        try {
            // clean segments into the new destination segment
            Iterator<LogSegment> iter = segments.iterator();
            Optional<LogSegment> currentSegmentOpt = Optional.of(iter.next());
            Map<Long, LastRecord> lastOffsetOfActiveProducers = log.lastRecordsOfActiveProducers();

            while (currentSegmentOpt.isPresent()) {
                LogSegment currentSegment = currentSegmentOpt.get();
                Optional<LogSegment> nextSegmentOpt = iter.hasNext() ? Optional.of(iter.next()) : Optional.empty();

                // Note that it is important to collect aborted transactions from the full log segment
                // range since we need to rebuild the full transaction index for the new segment.
                long startOffset = currentSegment.baseOffset();
                long upperBoundOffset = nextSegmentOpt.map(LogSegment::baseOffset).orElse(currentSegment.readNextOffset());
                List<AbortedTxn> abortedTransactions = log.collectAbortedTransactions(startOffset, upperBoundOffset);
                transactionMetadata.addAbortedTransactions(abortedTransactions);

                boolean retainLegacyDeletesAndTxnMarkers = currentSegment.lastModified() > legacyDeleteHorizonMs;
                logger.info(
                        "Cleaning {} in log {} into {} with an upper bound deletion horizon {} computed from " +
                        "the segment last modified time of {},{} deletes.",
                        currentSegment, log.name(), cleaned.baseOffset(), legacyDeleteHorizonMs, currentSegment.lastModified(),
                        retainLegacyDeletesAndTxnMarkers ? "retaining" : "discarding"
                );

                try {
                    cleanInto(
                            log.topicPartition(),
                            currentSegment.log(),
                            cleaned,
                            map,
                            retainLegacyDeletesAndTxnMarkers,
                            log.config().deleteRetentionMs,
                            log.config().maxMessageSize(),
                            transactionMetadata,
                            lastOffsetOfActiveProducers,
                            upperBoundOffsetOfCleaningRound,
                            stats,
                            currentTime
                    );
                } catch (LogSegmentOffsetOverflowException e) {
                    // Split the current segment. It's also safest to abort the current cleaning process, so that we retry from
                    // scratch once the split is complete.
                    logger.info("Caught segment overflow error during cleaning: {}", e.getMessage());
                    log.splitOverflowedSegment(currentSegment);
                    throw new LogCleaningAbortedException();
                }
                currentSegmentOpt = nextSegmentOpt;
            }

            cleaned.onBecomeInactiveSegment();
            // flush new segment to disk before swap
            cleaned.flush();

            // update the modification date to retain the last modified date of the original files
            long modified = segments.get(segments.size() - 1).lastModified();
            cleaned.setLastModified(modified);

            // swap in new segment
            logger.info("Swapping in cleaned segment {} for segment(s) {} in log {}", cleaned, segments, log);
            log.replaceSegments(List.of(cleaned), segments);
        } catch (LogCleaningAbortedException e) {
            try {
                cleaned.deleteIfExists();
            } catch (Exception deleteException) {
                e.addSuppressed(deleteException);
            } finally {
                throw e;
            }
        }
    }

    /**
     * Clean the given source log segment into the destination segment using the key=>offset mapping
     * provided.
     *
     * @param topicPartition The topic and partition of the log segment to clean
     * @param sourceRecords The dirty log segment
     * @param dest The cleaned log segment
     * @param map The key=>offset mapping
     * @param retainLegacyDeletesAndTxnMarkers Should tombstones (lower than version 2) and markers be retained while cleaning this segment
     * @param deleteRetentionMs Defines how long a tombstone should be kept as defined by log configuration
     * @param maxLogMessageSize The maximum message size of the corresponding topic
     * @param transactionMetadata The state of ongoing transactions which is carried between the cleaning of the grouped segments
     * @param lastRecordsOfActiveProducers The active producers and its last data offset
     * @param upperBoundOffsetOfCleaningRound Next offset of the last batch in the source segment
     * @param stats Collector for cleaning statistics
     * @param currentTime The time at which the clean was initiated
     */
    private void cleanInto(TopicPartition topicPartition,
                           FileRecords sourceRecords,
                           LogSegment dest,
                           OffsetMap map,
                           boolean retainLegacyDeletesAndTxnMarkers,
                           long deleteRetentionMs,
                           int maxLogMessageSize,
                           CleanedTransactionMetadata transactionMetadata,
                           Map<Long, LastRecord> lastRecordsOfActiveProducers,
                           long upperBoundOffsetOfCleaningRound,
                           CleanerStats stats,
                           long currentTime) throws IOException {
        MemoryRecords.RecordFilter logCleanerFilter = new MemoryRecords.RecordFilter(currentTime, deleteRetentionMs) {
            private boolean discardBatchRecords;

            @Override
            public BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                // we piggy-back on the tombstone retention logic to delay deletion of transaction markers.
                // note that we will never delete a marker until all the records from that transaction are removed.
                boolean canDiscardBatch = shouldDiscardBatch(batch, transactionMetadata);

                if (batch.isControlBatch()) {
                    discardBatchRecords = canDiscardBatch && batch.deleteHorizonMs().isPresent() && batch.deleteHorizonMs().getAsLong() <= this.currentTime;
                } else {
                    discardBatchRecords = canDiscardBatch;
                }

                // We retain the batch in order to preserve the state of active producers. There are three cases:
                // 1) The producer is no longer active, which means we can delete all records for that producer.
                // 2) The producer is still active and has a last data offset. We retain the batch that contains
                //    this offset since it also contains the last sequence number for this producer.
                // 3) The last entry in the log is a transaction marker. We retain this marker since it has the
                //    last producer epoch, which is needed to ensure fencing.
                boolean isBatchLastRecordOfProducer = Optional.ofNullable(lastRecordsOfActiveProducers.get(batch.producerId()))
                        .map(lastRecord -> {
                            if (lastRecord.lastDataOffset().isPresent()) {
                                return batch.lastOffset() == lastRecord.lastDataOffset().getAsLong();
                            } else {
                                return batch.isControlBatch() && batch.producerEpoch() == lastRecord.producerEpoch();
                            }
                        })
                        .orElse(false);

                BatchRetention batchRetention;
                if (batch.hasProducerId() && isBatchLastRecordOfProducer)
                    batchRetention = BatchRetention.RETAIN_EMPTY;
                else if (batch.nextOffset() == upperBoundOffsetOfCleaningRound) {
                    // retain the last batch of the cleaning round, even if it's empty, so that last offset information
                    // is not lost after cleaning.
                    batchRetention = BatchRetention.RETAIN_EMPTY;
                } else if (discardBatchRecords)
                    batchRetention = BatchRetention.DELETE;
                else
                    batchRetention = BatchRetention.DELETE_EMPTY;

                return new BatchRetentionResult(batchRetention, canDiscardBatch && batch.isControlBatch());
            }

            @Override
            public boolean shouldRetainRecord(RecordBatch batch, Record record) {
                if (discardBatchRecords) {
                    // The batch is only retained to preserve producer sequence information; the records can be removed
                    return false;
                } else if (batch.isControlBatch()) {
                    return true;
                } else {
                    try {
                        return Cleaner.this.shouldRetainRecord(map, retainLegacyDeletesAndTxnMarkers, batch, record, stats, currentTime);
                    } catch (DigestException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };

        int position = 0;
        while (position < sourceRecords.sizeInBytes()) {
            checkDone.accept(topicPartition);
            // read a chunk of messages and copy any that are to be retained to the write buffer to be written out
            readBuffer.clear();
            writeBuffer.clear();

            sourceRecords.readInto(readBuffer, position);
            MemoryRecords records = MemoryRecords.readableRecords(readBuffer);
            throttler.maybeThrottle(records.sizeInBytes());
            MemoryRecords.FilterResult result = records.filterTo(logCleanerFilter, writeBuffer, decompressionBufferSupplier);

            stats.readMessages(result.messagesRead(), result.bytesRead());
            stats.recopyMessages(result.messagesRetained(), result.bytesRetained());

            position += result.bytesRead();

            // if any messages are to be retained, write them out
            ByteBuffer outputBuffer = result.outputBuffer();
            if (outputBuffer.position() > 0) {
                outputBuffer.flip();
                MemoryRecords retained = MemoryRecords.readableRecords(outputBuffer);
                // it's OK not to hold the Log's lock in this case, because this segment is only accessed by other threads
                // after `Log.replaceSegments` (which acquires the lock) is called
                dest.append(result.maxOffset(), retained);
                throttler.maybeThrottle(outputBuffer.limit());
            }

            // if we read bytes but didn't get even one complete batch, our I/O buffer is too small, grow it and try again
            // `result.bytesRead` contains bytes from `messagesRead` and any discarded batches.
            if (readBuffer.limit() > 0 && result.bytesRead() == 0)
                growBuffersOrFail(sourceRecords, position, maxLogMessageSize, records);
        }
        restoreBuffers();
    }


    /**
     * Grow buffers to process next batch of records from `sourceRecords.` Buffers are doubled in size
     * up to a maximum of `maxLogMessageSize`. In some scenarios, a record could be bigger than the
     * current maximum size configured for the log. For example:
     * <ol>
     *   <li>A compacted topic using compression may contain a message set slightly larger than max.message.bytes</li>
     *   <li>max.message.bytes of a topic could have been reduced after writing larger messages</li>
     * </ol>
     * In these cases, grow the buffer to hold the next batch.
     *
     * @param sourceRecords The dirty log segment records to process
     * @param position The current position in the read buffer to read from
     * @param maxLogMessageSize The maximum record size in bytes for the topic
     * @param memoryRecords The memory records in read buffer
     */
    private void growBuffersOrFail(FileRecords sourceRecords,
                                   int position,
                                   int maxLogMessageSize,
                                   MemoryRecords memoryRecords) throws IOException {
        int maxSize;

        if (readBuffer.capacity() >= maxLogMessageSize) {
            Integer nextBatchSize = memoryRecords.firstBatchSize();
            String logDesc = String.format("log segment %s at position %d", sourceRecords.file(), position);
            if (nextBatchSize == null) {
                throw new IllegalStateException("Could not determine next batch size for " + logDesc);
            }
            if (nextBatchSize <= 0) {
                throw new IllegalStateException("Invalid batch size " + nextBatchSize + " for " + logDesc);
            }
            if (nextBatchSize <= readBuffer.capacity()) {
                throw new IllegalStateException("Batch size " + nextBatchSize + " < buffer size " + readBuffer.capacity() + ", but not processed for " + logDesc);
            }
            long bytesLeft = sourceRecords.channel().size() - position;
            if (nextBatchSize > bytesLeft) {
                throw new CorruptRecordException("Log segment may be corrupt, batch size " + nextBatchSize + " > " + bytesLeft + " bytes left in segment for " + logDesc);
            }

            maxSize = nextBatchSize;
        } else {
            maxSize = maxLogMessageSize;
        }

        growBuffers(maxSize);
    }

    /**
     * Check if a batch should be discarded by cleaned transaction state.
     *
     * @param batch The batch of records to check
     * @param transactionMetadata The maintained transaction state about cleaning
     *
     * @return if the batch can be discarded
     */
    private boolean shouldDiscardBatch(RecordBatch batch,
                                       CleanedTransactionMetadata transactionMetadata) {
        if (batch.isControlBatch())
            return transactionMetadata.onControlBatchRead(batch);
        else
            return transactionMetadata.onBatchRead(batch);
    }

    /**
     * Check if a record should be retained.
     *
     * @param map The offset map(key=>offset) to use for cleaning segments
     * @param retainDeletesForLegacyRecords Should tombstones (lower than version 2) and markers be retained while cleaning this segment
     * @param batch The batch of records that the record belongs to
     * @param record The record to check
     * @param stats The collector for cleaning statistics
     * @param currentTime The current time that used to compare with the delete horizon time of the batch when judging a non-legacy record
     *
     * @return if the record  can be retained
     */
    private boolean shouldRetainRecord(OffsetMap map,
                                       boolean retainDeletesForLegacyRecords,
                                       RecordBatch batch,
                                       Record record,
                                       CleanerStats stats,
                                       long currentTime) throws DigestException {
        boolean pastLatestOffset = record.offset() > map.latestOffset();
        if (pastLatestOffset) {
            return true;
        }

        if (record.hasKey()) {
            ByteBuffer key = record.key();
            long foundOffset = map.get(key);
            /* First,the message must have the latest offset for the key
             * then there are two cases in which we can retain a message:
             *   1) The message has value
             *   2) The message doesn't have value but it can't be deleted now.
             */
            boolean latestOffsetForKey = record.offset() >= foundOffset;
            boolean legacyRecord = batch.magic() < RecordBatch.MAGIC_VALUE_V2;

            boolean shouldRetainDeletes;
            if (!legacyRecord) {
                shouldRetainDeletes = batch.deleteHorizonMs().isEmpty() || currentTime < batch.deleteHorizonMs().getAsLong();
            } else {
                shouldRetainDeletes = retainDeletesForLegacyRecords;
            }

            boolean isRetainedValue = record.hasValue() || shouldRetainDeletes;
            return latestOffsetForKey && isRetainedValue;
        } else {
            stats.invalidMessage();
            return false;
        }
    }

    /**
     * Double the I/O buffer capacity.
     *
     * @param maxLogMessageSize The maximum record size in bytes allowed
     */
    private void growBuffers(int maxLogMessageSize) {
        int maxBufferSize = Math.max(maxLogMessageSize, maxIoBufferSize);
        if (readBuffer.capacity() >= maxBufferSize || writeBuffer.capacity() >= maxBufferSize)
            throw new IllegalStateException("This log contains a message larger than maximum allowable size of " + maxBufferSize + ".");
        int newSize = Math.min(readBuffer.capacity() * 2, maxBufferSize);
        logger.info("Growing cleaner I/O buffers from {} bytes to {} bytes.", readBuffer.capacity(), newSize);
        readBuffer = ByteBuffer.allocate(newSize);
        writeBuffer = ByteBuffer.allocate(newSize);
    }

    /**
     * Restore the I/O buffer capacity to its original size.
     */
    private void restoreBuffers() {
        if (readBuffer.capacity() > ioBufferSize)
            readBuffer = ByteBuffer.allocate(ioBufferSize);
        if (writeBuffer.capacity() > ioBufferSize)
            writeBuffer = ByteBuffer.allocate(ioBufferSize);
    }

    /**
     * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
     * We collect a group of such segments together into a single destination segment.
     * This prevents segment sizes from shrinking too much.
     *
     * @param segments The log segments to group
     * @param maxSize the maximum size in bytes for the total of all log data in a group
     * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
     * @param firstUncleanableOffset The upper(exclusive) offset to clean to
     *
     * @return A list of grouped segments
     */
    public List<List<LogSegment>> groupSegmentsBySize(List<LogSegment> segments, int maxSize, int maxIndexSize, long firstUncleanableOffset) throws IOException {
        List<List<LogSegment>> grouped = new ArrayList<>();

        while (!segments.isEmpty()) {
            List<LogSegment> group = new ArrayList<>();
            group.add(segments.get(0));

            long logSize = segments.get(0).size();
            long indexSize = segments.get(0).offsetIndex().sizeInBytes();
            long timeIndexSize = segments.get(0).timeIndex().sizeInBytes();

            segments = segments.subList(1, segments.size());

            while (!segments.isEmpty() &&
                    logSize + segments.get(0).size() <= maxSize &&
                    indexSize + segments.get(0).offsetIndex().sizeInBytes() <= maxIndexSize &&
                    timeIndexSize + segments.get(0).timeIndex().sizeInBytes() <= maxIndexSize &&
                    //if first segment size is 0, we don't need to do the index offset range check.
                    //this will avoid empty log left every 2^31 message.
                    (segments.get(0).size() == 0 ||
                            lastOffsetForFirstSegment(segments, firstUncleanableOffset) - group.get(group.size() - 1).baseOffset() <= Integer.MAX_VALUE)) {
                group.add(0, segments.get(0));
                logSize += segments.get(0).size();
                indexSize += segments.get(0).offsetIndex().sizeInBytes();
                timeIndexSize += segments.get(0).timeIndex().sizeInBytes();
                segments = segments.subList(1, segments.size());
            }

            Collections.reverse(group);
            grouped.add(0, group);
        }

        Collections.reverse(grouped);

        return grouped;
    }

    /**
     * We want to get the last offset in the first log segment in segs.
     * LogSegment.nextOffset() gives the exact last offset in a segment, but can be expensive since it requires
     * scanning the segment from the last index entry.
     * Therefore, we estimate the last offset of the first log segment by using
     * the base offset of the next segment in the list.
     * If the next segment doesn't exist, first Uncleanable Offset will be used.
     *
     * @param segs Remaining segments to group
     * @param firstUncleanableOffset The upper(exclusive) offset to clean to
     * @return The estimated last offset for the first segment in segs
     */
    private long lastOffsetForFirstSegment(List<LogSegment> segs, long firstUncleanableOffset) {
        if (segs.size() > 1) {
            /* if there is a next segment, use its base offset as the bounding offset to guarantee we know
             * the worst case offset */
            return segs.get(1).baseOffset() - 1;
        } else {
            //for the last segment in the list, use the first uncleanable offset.
            return firstUncleanableOffset - 1;
        }
    }

    /**
     * Build a map of key_hash => offset for the keys in the cleanable dirty portion of the log to use in cleaning.
     *
     * @param log The log to use
     * @param start The offset at which dirty messages begin
     * @param end The ending offset for the map that is being built
     * @param map The map in which to store the mappings
     * @param stats Collector for cleaning statistics
     */
    public void buildOffsetMap(UnifiedLog log,
                               long start,
                               long end,
                               OffsetMap map,
                               CleanerStats stats) throws IOException, DigestException {
        map.clear();
        List<LogSegment> dirty = log.logSegments(start, end);
        List<Long> nextSegmentStartOffsets = new ArrayList<>();
        if (!dirty.isEmpty()) {
            for (int i = 1; i < dirty.size(); i++) {
                nextSegmentStartOffsets.add(dirty.get(i).baseOffset());
            }
            nextSegmentStartOffsets.add(end);
        }
        logger.info("Building offset map for log {} for {} segments in offset range [{}, {}).", log.name(), dirty.size(), start, end);

        CleanedTransactionMetadata transactionMetadata = new CleanedTransactionMetadata();
        List<AbortedTxn> abortedTransactions = log.collectAbortedTransactions(start, end);
        transactionMetadata.addAbortedTransactions(abortedTransactions);

        // Add all the cleanable dirty segments. We must take at least map.slots * load_factor,
        // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
        boolean full = false;
        for (int i = 0; i < dirty.size() && !full; i++) {
            LogSegment segment = dirty.get(i);
            long nextSegmentStartOffset = nextSegmentStartOffsets.get(i);

            checkDone.accept(log.topicPartition());

            full = buildOffsetMapForSegment(
                    log.topicPartition(),
                    segment,
                    map,
                    start,
                    nextSegmentStartOffset,
                    log.config().maxMessageSize(),
                    transactionMetadata,
                    stats
            );
            if (full) {
                logger.debug("Offset map is full, {} segments fully mapped, segment with base offset {} is partially mapped",
                        dirty.indexOf(segment), segment.baseOffset());
            }
        }

        logger.info("Offset map for log {} complete.", log.name());
    }

    /**
     * Add the messages in the given segment to the offset map.
     *
     * @param topicPartition The topic and partition of the log segment to build offset
     * @param segment The segment to index
     * @param map The map in which to store the key=>offset mapping
     * @param startOffset The offset at which dirty messages begin
     * @param nextSegmentStartOffset The base offset for next segment when building current segment
     * @param maxLogMessageSize The maximum size in bytes for record allowed
     * @param transactionMetadata The state of ongoing transactions for the log between offset range to build
     * @param stats Collector for cleaning statistics
     *
     * @return If the map was filled whilst loading from this segment
     */
    private boolean buildOffsetMapForSegment(TopicPartition topicPartition,
                                             LogSegment segment,
                                             OffsetMap map,
                                             long startOffset,
                                             long nextSegmentStartOffset,
                                             int maxLogMessageSize,
                                             CleanedTransactionMetadata transactionMetadata,
                                             CleanerStats stats) throws IOException, DigestException {
        int position = segment.offsetIndex().lookup(startOffset).position();
        int maxDesiredMapSize = (int) (map.slots() * dupBufferLoadFactor);

        while (position < segment.log().sizeInBytes()) {
            checkDone.accept(topicPartition);
            readBuffer.clear();
            try {
                segment.log().readInto(readBuffer, position);
            } catch (Exception e) {
                throw new KafkaException("Failed to read from segment " + segment + " of partition " + topicPartition +
                        " while loading offset map", e);
            }
            MemoryRecords records = MemoryRecords.readableRecords(readBuffer);
            throttler.maybeThrottle(records.sizeInBytes());

            int startPosition = position;
            for (MutableRecordBatch batch : records.batches()) {
                if (batch.isControlBatch()) {
                    transactionMetadata.onControlBatchRead(batch);
                    stats.indexMessagesRead(1);
                } else {
                    boolean isAborted = transactionMetadata.onBatchRead(batch);
                    if (isAborted) {
                        // If the batch is aborted, do not bother populating the offset map.
                        // Note that abort markers are supported in v2 and above, which means count is defined.
                        stats.indexMessagesRead(batch.countOrNull());
                    } else {
                        try (CloseableIterator<Record> recordsIterator = batch.streamingIterator(decompressionBufferSupplier)) {
                            for (Record record : (Iterable<Record>) () -> recordsIterator) {
                                if (record.hasKey() && record.offset() >= startOffset) {
                                    if (map.size() < maxDesiredMapSize) {
                                        map.put(record.key(), record.offset());
                                    } else {
                                        return true;
                                    }
                                }
                                stats.indexMessagesRead(1);
                            }
                        }
                    }
                }

                if (batch.lastOffset() >= startOffset)
                    map.updateLatestOffset(batch.lastOffset());
            }
            int bytesRead = records.validBytes();
            position += bytesRead;
            stats.indexBytesRead(bytesRead);

            // if we didn't read even one complete message, our read buffer may be too small
            if (position == startPosition)
                growBuffersOrFail(segment.log(), position, maxLogMessageSize, records);
        }

        // In the case of offsets gap, fast forward to latest expected offset in this segment.
        map.updateLatestOffset(nextSegmentStartOffset - 1L);

        restoreBuffers();

        return false;
    }
}
