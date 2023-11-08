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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.FileTime;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.FileRecords.LogOffsetPosition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import static java.util.Arrays.asList;

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileRecords containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * This class is not thread-safe.
 */
public class LogSegment implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogSegment.class);
    private static final Timer LOG_FLUSH_TIMER;

    static {
        KafkaMetricsGroup logFlushStatsMetricsGroup = new KafkaMetricsGroup(LogSegment.class) {
            @Override
            public MetricName metricName(String name, Map<String, String> tags) {
                // Override the group and type names for compatibility - this metrics group was previously defined within
                // a Scala object named `kafka.log.LogFlushStats`
                return KafkaMetricsGroup.explicitMetricName("kafka.log", "LogFlushStats", name, tags);
            }
        };
        LOG_FLUSH_TIMER = logFlushStatsMetricsGroup.newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    }

    private final FileRecords log;
    private final LazyIndex<OffsetIndex> lazyOffsetIndex;
    private final LazyIndex<TimeIndex> lazyTimeIndex;
    private final TransactionIndex txnIndex;
    private final long baseOffset;
    private final int indexIntervalBytes;
    private final long rollJitterMs;
    private final Time time;

    // The timestamp we used for time based log rolling and for ensuring max compaction delay
    // volatile for LogCleaner to see the update
    private volatile OptionalLong rollingBasedTimestamp = OptionalLong.empty();

    /* The maximum timestamp and offset we see so far */
    private volatile TimestampOffset maxTimestampAndOffsetSoFar = TimestampOffset.UNKNOWN;

    private long created;

    /* the number of bytes since we last added an entry in the offset index */
    private int bytesSinceLastIndexEntry = 0;

    /**
     * Create a LogSegment with the provided parameters.
     *
     * @param log The file records containing log entries
     * @param lazyOffsetIndex The offset index
     * @param lazyTimeIndex The timestamp index
     * @param txnIndex The transaction index
     * @param baseOffset A lower bound on the offsets in this segment
     * @param indexIntervalBytes The approximate number of bytes between entries in the index
     * @param rollJitterMs The maximum random jitter subtracted from the scheduled segment roll time
     * @param time The time instance
     */
    public LogSegment(FileRecords log,
                      LazyIndex<OffsetIndex> lazyOffsetIndex,
                      LazyIndex<TimeIndex> lazyTimeIndex,
                      TransactionIndex txnIndex,
                      long baseOffset,
                      int indexIntervalBytes,
                      long rollJitterMs,
                      Time time) {
        this.log = log;
        this.lazyOffsetIndex = lazyOffsetIndex;
        this.lazyTimeIndex = lazyTimeIndex;
        this.txnIndex = txnIndex;
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
        this.rollJitterMs = rollJitterMs;
        this.time = time;
        this.created = time.milliseconds();
    }

    // Visible for testing
    public LogSegment(LogSegment segment) {
        this(segment.log, segment.lazyOffsetIndex, segment.lazyTimeIndex, segment.txnIndex, segment.baseOffset,
                segment.indexIntervalBytes, segment.rollJitterMs, segment.time);
    }

    public OffsetIndex offsetIndex() throws IOException {
        return lazyOffsetIndex.get();
    }

    public File offsetIndexFile() {
        return lazyOffsetIndex.file();
    }

    public TimeIndex timeIndex() throws IOException {
        return lazyTimeIndex.get();
    }

    public File timeIndexFile() {
        return lazyTimeIndex.file();
    }

    public long baseOffset() {
        return baseOffset;
    }

    public FileRecords log() {
        return log;
    }

    public long rollJitterMs() {
        return rollJitterMs;
    }

    public TransactionIndex txnIndex() {
        return txnIndex;
    }

    public boolean shouldRoll(RollParams rollParams) throws IOException {
        boolean reachedRollMs = timeWaitedForRoll(rollParams.now, rollParams.maxTimestampInMessages) > rollParams.maxSegmentMs - rollJitterMs;
        int size = size();
        return size > rollParams.maxSegmentBytes - rollParams.messagesSize ||
            (size > 0 && reachedRollMs) ||
            offsetIndex().isFull() || timeIndex().isFull() || !canConvertToRelativeOffset(rollParams.maxOffsetInMessages);
    }

    public void resizeIndexes(int size) throws IOException {
        offsetIndex().resize(size);
        timeIndex().resize(size);
    }

    public void sanityCheck(boolean timeIndexFileNewlyCreated) throws IOException {
        if (offsetIndexFile().exists()) {
            // Resize the time index file to 0 if it is newly created.
            if (timeIndexFileNewlyCreated)
              timeIndex().resize(0);
            // Sanity checks for time index and offset index are skipped because
            // we will recover the segments above the recovery point in recoverLog()
            // in any case so sanity checking them here is redundant.
            txnIndex.sanityCheck();
        } else
            throw new NoSuchFileException("Offset index file " + offsetIndexFile().getAbsolutePath() + " does not exist");
    }

    /**
     * The first time this is invoked, it will result in a time index lookup (including potential materialization of
     * the time index).
     */
    public TimestampOffset readMaxTimestampAndOffsetSoFar() throws IOException {
        if (maxTimestampAndOffsetSoFar == TimestampOffset.UNKNOWN)
            maxTimestampAndOffsetSoFar = timeIndex().lastEntry();
        return maxTimestampAndOffsetSoFar;
    }

    /**
     * The maximum timestamp we see so far.
     *
     * Note that this may result in time index materialization.
     */
    public long maxTimestampSoFar() throws IOException {
        return readMaxTimestampAndOffsetSoFar().timestamp;
    }

    /**
     * Note that this may result in time index materialization.
     */
    private long offsetOfMaxTimestampSoFar() throws IOException {
        return readMaxTimestampAndOffsetSoFar().offset;
    }

    /* Return the size in bytes of this log segment */
    public int size() {
        return log.sizeInBytes();
    }

    /**
     * checks that the argument offset can be represented as an integer offset relative to the baseOffset.
     */
    private boolean canConvertToRelativeOffset(long offset) throws IOException {
        return offsetIndex().canAppendOffset(offset);
    }

    /**
     * Append the given messages starting with the given offset. Add
     * an entry to the index if needed.
     *
     * It is assumed this method is being called from within a lock, it is not thread-safe otherwise.
     *
     * @param largestOffset The last offset in the message set
     * @param largestTimestampMs The largest timestamp in the message set.
     * @param shallowOffsetOfMaxTimestamp The offset of the message that has the largest timestamp in the messages to append.
     * @param records The log entries to append.
     * @throws LogSegmentOffsetOverflowException if the largest offset causes index offset overflow
     */
    public void append(long largestOffset,
                       long largestTimestampMs,
                       long shallowOffsetOfMaxTimestamp,
                       MemoryRecords records) throws IOException {
        if (records.sizeInBytes() > 0) {
            LOGGER.trace("Inserting {} bytes at end offset {} at position {} with largest timestamp {} at shallow offset {}",
                records.sizeInBytes(), largestOffset, log.sizeInBytes(), largestTimestampMs, shallowOffsetOfMaxTimestamp);
            int physicalPosition = log.sizeInBytes();
            if (physicalPosition == 0)
                rollingBasedTimestamp = OptionalLong.of(largestTimestampMs);

            ensureOffsetInRange(largestOffset);

            // append the messages
            long appendedBytes = log.append(records);
            LOGGER.trace("Appended {} to {} at end offset {}", appendedBytes, log.file(), largestOffset);
            // Update the in memory max timestamp and corresponding offset.
            if (largestTimestampMs > maxTimestampSoFar()) {
                maxTimestampAndOffsetSoFar = new TimestampOffset(largestTimestampMs, shallowOffsetOfMaxTimestamp);
            }
            // append an entry to the index (if needed)
            if (bytesSinceLastIndexEntry > indexIntervalBytes) {
                offsetIndex().append(largestOffset, physicalPosition);
                timeIndex().maybeAppend(maxTimestampSoFar(), offsetOfMaxTimestampSoFar());
                bytesSinceLastIndexEntry = 0;
            }
            bytesSinceLastIndexEntry += records.sizeInBytes();
        }
    }

    private void ensureOffsetInRange(long offset) throws IOException {
        if (!canConvertToRelativeOffset(offset))
            throw new LogSegmentOffsetOverflowException(this, offset);
    }

    private int appendChunkFromFile(FileRecords records, int position, BufferSupplier bufferSupplier) throws IOException {
        int bytesToAppend = 0;
        long maxTimestamp = Long.MIN_VALUE;
        long offsetOfMaxTimestamp = Long.MIN_VALUE;
        long maxOffset = Long.MIN_VALUE;
        ByteBuffer readBuffer = bufferSupplier.get(1024 * 1024);

        // find all batches that are valid to be appended to the current log segment and
        // determine the maximum offset and timestamp
        Iterator<FileChannelRecordBatch> nextBatches = records.batchesFrom(position).iterator();
        FileChannelRecordBatch batch;
        while ((batch = nextAppendableBatch(nextBatches, readBuffer, bytesToAppend)) != null) {
            if (batch.maxTimestamp() > maxTimestamp) {
                maxTimestamp = batch.maxTimestamp();
                offsetOfMaxTimestamp = batch.lastOffset();
            }
            maxOffset = batch.lastOffset();
            bytesToAppend += batch.sizeInBytes();
        }

        if (bytesToAppend > 0) {
            // Grow buffer if needed to ensure we copy at least one batch
            if (readBuffer.capacity() < bytesToAppend)
                readBuffer = bufferSupplier.get(bytesToAppend);

            readBuffer.limit(bytesToAppend);
            records.readInto(readBuffer, position);

            append(maxOffset, maxTimestamp, offsetOfMaxTimestamp, MemoryRecords.readableRecords(readBuffer));
        }

        bufferSupplier.release(readBuffer);
        return bytesToAppend;
    }

    private FileChannelRecordBatch nextAppendableBatch(Iterator<FileChannelRecordBatch> recordBatches,
                                                       ByteBuffer readBuffer,
                                                       int bytesToAppend) throws IOException {
        if (recordBatches.hasNext()) {
            FileChannelRecordBatch batch = recordBatches.next();
            if (canConvertToRelativeOffset(batch.lastOffset()) &&
                    (bytesToAppend == 0 || bytesToAppend + batch.sizeInBytes() < readBuffer.capacity()))
                return batch;
        }
        return null;
    }

    /**
     * Append records from a file beginning at the given position until either the end of the file
     * is reached or an offset is found which is too large to convert to a relative offset for the indexes.
     *
     * @return the number of bytes appended to the log (may be less than the size of the input if an
     *         offset is encountered which would overflow this segment)
     */
    public int appendFromFile(FileRecords records, int start) throws IOException {
        int position = start;
        BufferSupplier bufferSupplier = new BufferSupplier.GrowableBufferSupplier();
        while (position < start + records.sizeInBytes()) {
            int bytesAppended = appendChunkFromFile(records, position, bufferSupplier);
            if (bytesAppended == 0)
                return position - start;
            position += bytesAppended;
        }
        return position - start;
    }

    /* not thread safe */
    public void updateTxnIndex(CompletedTxn completedTxn, long lastStableOffset) throws IOException {
        if (completedTxn.isAborted) {
            LOGGER.trace("Writing aborted transaction {} to transaction index, last stable offset is {}", completedTxn, lastStableOffset);
            txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset));
        }
    }

    private void updateProducerState(ProducerStateManager producerStateManager, RecordBatch batch) throws IOException {
        if (batch.hasProducerId()) {
            long producerId = batch.producerId();
            ProducerAppendInfo appendInfo = producerStateManager.prepareUpdate(producerId, AppendOrigin.REPLICATION);
            Optional<CompletedTxn> maybeCompletedTxn = appendInfo.append(batch, Optional.empty());
            producerStateManager.update(appendInfo);
            if (maybeCompletedTxn.isPresent()) {
                CompletedTxn completedTxn = maybeCompletedTxn.get();
                long lastStableOffset = producerStateManager.lastStableOffset(completedTxn);
                updateTxnIndex(completedTxn, lastStableOffset);
                producerStateManager.completeTxn(completedTxn);
            }
        }
        producerStateManager.updateMapEndOffset(batch.lastOffset() + 1);
    }

    /**
     * Equivalent to {@code translateOffset(offset, 0)}.
     *
     * See {@link #translateOffset(long, int)} for details.
     */
    public LogOffsetPosition translateOffset(long offset) throws IOException {
        return translateOffset(offset, 0);
    }

    /**
     * Find the physical file position for the first message with offset >= the requested offset.
     *
     * The startingFilePosition argument is an optimization that can be used if we already know a valid starting position
     * in the file higher than the greatest-lower-bound from the index.
     *
     * This method is thread-safe.
     *
     * @param offset The offset we want to translate
     * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
     * when omitted, the search will begin at the position in the offset index.
     * @return The position in the log storing the message with the least offset >= the requested offset and the size of the
     *        message or null if no message meets this criteria.
     */
    LogOffsetPosition translateOffset(long offset, int startingFilePosition) throws IOException {
        OffsetPosition mapping = offsetIndex().lookup(offset);
        return log.searchForOffsetWithSize(offset, Math.max(mapping.position, startingFilePosition));
    }

    /**
     * Equivalent to {@code read(startOffset, maxSize, size())}.
     *
     * See {@link #read(long, int, long, boolean)} for details.
     */
    public FetchDataInfo read(long startOffset, int maxSize) throws IOException {
        return read(startOffset, maxSize, size());
    }

    /**
     * Equivalent to {@code read(startOffset, maxSize, maxPosition, false)}.
     *
     * See {@link #read(long, int, long, boolean)} for details.
     */
    public FetchDataInfo read(long startOffset, int maxSize, long maxPosition) throws IOException {
        return read(startOffset, maxSize, maxPosition, false);
    }

    /**
     * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
     * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
     *
     * This method is thread-safe.
     *
     * @param startOffset A lower bound on the first offset to include in the message set we read
     * @param maxSize The maximum number of bytes to include in the message set we read
     * @param maxPosition The maximum position in the log segment that should be exposed for read
     * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxSize` (if one exists)
     *
     * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
     *         or null if the startOffset is larger than the largest offset in this log
     */
    public FetchDataInfo read(long startOffset, int maxSize, long maxPosition, boolean minOneMessage) throws IOException {
        if (maxSize < 0)
            throw new IllegalArgumentException("Invalid max size " + maxSize + " for log read from segment " + log);

        LogOffsetPosition startOffsetAndSize = translateOffset(startOffset);

        // if the start position is already off the end of the log, return null
        if (startOffsetAndSize == null)
            return null;

        int startPosition = startOffsetAndSize.position;
        LogOffsetMetadata offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition);

        int adjustedMaxSize = maxSize;
        if (minOneMessage)
            adjustedMaxSize = Math.max(maxSize, startOffsetAndSize.size);

        // return a log segment but with zero size in the case below
        if (adjustedMaxSize == 0)
            return new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY);

        // calculate the length of the message set to read based on whether or not they gave us a maxOffset
        int fetchSize = Math.min((int) (maxPosition - startPosition), adjustedMaxSize);

        return new FetchDataInfo(offsetMetadata, log.slice(startPosition, fetchSize),
            adjustedMaxSize < startOffsetAndSize.size, Optional.empty());
    }

    public OptionalLong fetchUpperBoundOffset(OffsetPosition startOffsetPosition, int fetchSize) throws IOException {
        Optional<OffsetPosition> position = offsetIndex().fetchUpperBoundOffset(startOffsetPosition, fetchSize);
        if (position.isPresent())
            return OptionalLong.of(position.get().offset);
        return OptionalLong.empty();
    }

    /**
     * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes
     * from the end of the log and index.
     *
     * This method is not thread-safe.
     *
     * @param producerStateManager Producer state corresponding to the segment's base offset. This is needed to recover
     *                             the transaction index.
     * @param leaderEpochCache Optionally a cache for updating the leader epoch during recovery.
     * @return The number of bytes truncated from the log
     * @throws LogSegmentOffsetOverflowException if the log segment contains an offset that causes the index offset to overflow
     */
    public int recover(ProducerStateManager producerStateManager, Optional<LeaderEpochFileCache> leaderEpochCache) throws IOException {
        offsetIndex().reset();
        timeIndex().reset();
        txnIndex.reset();
        int validBytes = 0;
        int lastIndexEntry = 0;
        maxTimestampAndOffsetSoFar = TimestampOffset.UNKNOWN;
        try {
            for (RecordBatch batch : log.batches()) {
                batch.ensureValid();
                ensureOffsetInRange(batch.lastOffset());

                // The max timestamp is exposed at the batch level, so no need to iterate the records
                if (batch.maxTimestamp() > maxTimestampSoFar()) {
                    maxTimestampAndOffsetSoFar = new TimestampOffset(batch.maxTimestamp(), batch.lastOffset());
                }

                // Build offset index
                if (validBytes - lastIndexEntry > indexIntervalBytes) {
                    offsetIndex().append(batch.lastOffset(), validBytes);
                    timeIndex().maybeAppend(maxTimestampSoFar(), offsetOfMaxTimestampSoFar());
                    lastIndexEntry = validBytes;
                }
                validBytes += batch.sizeInBytes();

                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                    leaderEpochCache.ifPresent(cache -> {
                        if (batch.partitionLeaderEpoch() >= 0 &&
                                (!cache.latestEpoch().isPresent() || batch.partitionLeaderEpoch() > cache.latestEpoch().getAsInt()))
                            cache.assign(batch.partitionLeaderEpoch(), batch.baseOffset());
                    });
                    updateProducerState(producerStateManager, batch);
                }
            }
        } catch (CorruptRecordException | InvalidRecordException e) {
            LOGGER.warn("Found invalid messages in log segment {} at byte offset {}.", log.file().getAbsolutePath(),
                validBytes, e);
        }
        int truncated = log.sizeInBytes() - validBytes;
        if (truncated > 0)
            LOGGER.debug("Truncated {} invalid bytes at the end of segment {} during recovery", truncated, log.file().getAbsolutePath());

        log.truncateTo(validBytes);
        offsetIndex().trimToValidSize();
        // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
        timeIndex().maybeAppend(maxTimestampSoFar(), offsetOfMaxTimestampSoFar(), true);
        timeIndex().trimToValidSize();
        return truncated;
    }

    /**
     * Check whether the last offset of the last batch in this segment overflows the indexes.
     */
    public boolean hasOverflow() throws IOException {
        long nextOffset = readNextOffset();
        return nextOffset > baseOffset && !canConvertToRelativeOffset(nextOffset - 1);
    }

    public TxnIndexSearchResult collectAbortedTxns(long fetchOffset, long upperBoundOffset) {
        return txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset);
    }

    @Override
    public String toString() {
        // We don't call `largestRecordTimestamp` below to avoid materializing the time index when `toString` is invoked
        return "LogSegment(baseOffset=" + baseOffset +
            ", size=" + size() +
            ", lastModifiedTime=" + lastModified() +
            ", largestRecordTimestamp=" + maxTimestampAndOffsetSoFar.timestamp +
            ")";
    }

    /**
     * Truncate off all index and log entries with offsets >= the given offset.
     * If the given offset is larger than the largest message in this segment, do nothing.
     *
     * This method is not thread-safe.
     *
     * @param offset The offset to truncate to
     * @return The number of log bytes truncated
     */
    public int truncateTo(long offset) throws IOException {
        // Do offset translation before truncating the index to avoid needless scanning
        // in case we truncate the full index
        LogOffsetPosition mapping = translateOffset(offset);
        OffsetIndex offsetIndex = offsetIndex();
        TimeIndex timeIndex = timeIndex();

        offsetIndex.truncateTo(offset);
        timeIndex.truncateTo(offset);
        txnIndex.truncateTo(offset);

        // After truncation, reset and allocate more space for the (new currently active) index
        offsetIndex.resize(offsetIndex.maxIndexSize());
        timeIndex.resize(timeIndex.maxIndexSize());

        int bytesTruncated;
        if (mapping == null)
            bytesTruncated = 0;
        else
            bytesTruncated = log.truncateTo(mapping.position);

        if (log.sizeInBytes() == 0) {
            created = time.milliseconds();
            rollingBasedTimestamp = OptionalLong.empty();
        }

        bytesSinceLastIndexEntry = 0;
        if (maxTimestampSoFar() >= 0)
            maxTimestampAndOffsetSoFar = readLargestTimestamp();

        return bytesTruncated;
    }

    private TimestampOffset readLargestTimestamp() throws IOException {
        // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
        TimestampOffset lastTimeIndexEntry = timeIndex().lastEntry();
        OffsetPosition offsetPosition = offsetIndex().lookup(lastTimeIndexEntry.offset);

        // Scan the rest of the messages to see if there is a larger timestamp after the last time index entry.
        FileRecords.TimestampAndOffset maxTimestampOffsetAfterLastEntry = log.largestTimestampAfter(offsetPosition.position);
        if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.timestamp)
            return new TimestampOffset(maxTimestampOffsetAfterLastEntry.timestamp, maxTimestampOffsetAfterLastEntry.offset);

        return lastTimeIndexEntry;
    }

    /**
     * Calculate the offset that would be used for the next message to be append to this segment.
     * Note that this is expensive.
     *
     * This method is thread-safe.
     */
    public long readNextOffset() throws IOException {
        FetchDataInfo fetchData = read(offsetIndex().lastOffset(), log.sizeInBytes());
        if (fetchData == null)
            return baseOffset;
        else
            return fetchData.records.lastBatch()
                .map(batch -> batch.nextOffset())
                .orElse(baseOffset);
    }

    /**
     * Flush this log segment to disk.
     *
     * This method is thread-safe.
     */
    public void flush() throws IOException {
        try {
            LOG_FLUSH_TIMER.time(new Callable<Void>() {
                // lambdas cannot declare a more specific exception type, so we use an anonymous inner class
                @Override
                public Void call() throws IOException {
                    log.flush();
                    offsetIndex().flush();
                    timeIndex().flush();
                    txnIndex.flush();
                    return null;
                }
            });
        } catch (Exception e) {
            if (e instanceof IOException)
                throw (IOException) e;
            else if (e instanceof RuntimeException)
                throw (RuntimeException) e;
            else
                throw new IllegalStateException("Unexpected exception thrown: " + e, e);
        }
    }

    /**
     * Update the directory reference for the log and indices in this segment. This would typically be called after a
     * directory is renamed.
     */
    void updateParentDir(File dir) {
        log.updateParentDir(dir);
        lazyOffsetIndex.updateParentDir(dir);
        lazyTimeIndex.updateParentDir(dir);
        txnIndex.updateParentDir(dir);
    }

    /**
     * Change the suffix for the index and log files for this log segment
     * IOException from this method should be handled by the caller
     */
    public void changeFileSuffixes(String oldSuffix, String newSuffix) throws IOException {
        log.renameTo(new File(Utils.replaceSuffix(log.file().getPath(), oldSuffix, newSuffix)));
        lazyOffsetIndex.renameTo(new File(Utils.replaceSuffix(offsetIndexFile().getPath(), oldSuffix, newSuffix)));
        lazyTimeIndex.renameTo(new File(Utils.replaceSuffix(timeIndexFile().getPath(), oldSuffix, newSuffix)));
        txnIndex.renameTo(new File(Utils.replaceSuffix(txnIndex.file().getPath(), oldSuffix, newSuffix)));
    }

    public boolean hasSuffix(String suffix) {
        return log.file().getName().endsWith(suffix) &&
            offsetIndexFile().getName().endsWith(suffix) &&
            timeIndexFile().getName().endsWith(suffix) &&
            txnIndex.file().getName().endsWith(suffix);
    }

    /**
     * Append the largest time index entry to the time index and trim the log and indexes.
     *
     * The time index entry appended will be used to decide when to delete the segment.
     */
    public void onBecomeInactiveSegment() throws IOException {
        timeIndex().maybeAppend(maxTimestampSoFar(), offsetOfMaxTimestampSoFar(), true);
        offsetIndex().trimToValidSize();
        timeIndex().trimToValidSize();
        log.trim();
    }

    /**
     * If not previously loaded,
     * load the timestamp of the first message into memory.
     */
    private void loadFirstBatchTimestamp() {
        if (!rollingBasedTimestamp.isPresent()) {
            Iterator<FileChannelRecordBatch> iter = log.batches().iterator();
            if (iter.hasNext())
                rollingBasedTimestamp = OptionalLong.of(iter.next().maxTimestamp());
        }
    }

    /**
     * The time this segment has waited to be rolled.
     * If the first message batch has a timestamp we use its timestamp to determine when to roll a segment. A segment
     * is rolled if the difference between the new batch's timestamp and the first batch's timestamp exceeds the
     * segment rolling time.
     * If the first batch does not have a timestamp, we use the wall clock time to determine when to roll a segment. A
     * segment is rolled if the difference between the current wall clock time and the segment create time exceeds the
     * segment rolling time.
     */
    public long timeWaitedForRoll(long now, long messageTimestamp) {
        // Load the timestamp of the first message into memory
        loadFirstBatchTimestamp();
        long ts = rollingBasedTimestamp.orElse(-1L);
        if (ts >= 0)
            return messageTimestamp - ts;
        return now - created;
    }

    /**
     * @return the first batch timestamp if the timestamp is available. Otherwise return Long.MaxValue
     */
    long getFirstBatchTimestamp() {
        loadFirstBatchTimestamp();
        OptionalLong timestamp = rollingBasedTimestamp;
        if (timestamp.isPresent() && timestamp.getAsLong() >= 0)
            return timestamp.getAsLong();
        return Long.MAX_VALUE;
    }

    /**
     * Search the message offset based on timestamp and offset.
     *
     * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
     *
     * - If all the messages in the segment have smaller offsets, return None
     * - If all the messages in the segment have smaller timestamps, return None
     * - If all the messages in the segment have larger timestamps, or no message in the segment has a timestamp
     *   the returned the offset will be max(the base offset of the segment, startingOffset) and the timestamp will be Message.NoTimestamp.
     * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
     *   is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
     *
     * This method only returns None when 1) all messages' offset < startOffing or 2) the log is not empty but we did not
     * see any message when scanning the log from the indexed position. The latter could happen if the log is truncated
     * after we get the indexed position but before we scan the log from there. In this case we simply return None and the
     * caller will need to check on the truncated log and maybe retry or even do the search on another log segment.
     *
     * @param timestampMs The timestamp to search for.
     * @param startingOffset The starting offset to search.
     * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there is no such message.
     */
    public Optional<FileRecords.TimestampAndOffset> findOffsetByTimestamp(long timestampMs, long startingOffset) throws IOException {
        // Get the index entry with a timestamp less than or equal to the target timestamp
        TimestampOffset timestampOffset = timeIndex().lookup(timestampMs);
        int position = offsetIndex().lookup(Math.max(timestampOffset.offset, startingOffset)).position;

        // Search the timestamp
        return Optional.ofNullable(log.searchForTimestamp(timestampMs, position, startingOffset));
    }

    /**
     * Close this log segment
     */
    @Override
    public void close() throws IOException {
        if (maxTimestampAndOffsetSoFar != TimestampOffset.UNKNOWN)
            Utils.swallow(LOGGER, Level.WARN, "maybeAppend", () -> timeIndex().maybeAppend(maxTimestampSoFar(), offsetOfMaxTimestampSoFar(), true));
        Utils.closeQuietly(lazyOffsetIndex, "offsetIndex", LOGGER);
        Utils.closeQuietly(lazyTimeIndex, "timeIndex", LOGGER);
        Utils.closeQuietly(log, "log", LOGGER);
        Utils.closeQuietly(txnIndex, "txnIndex", LOGGER);
    }

    /**
     * Close file handlers used by the log segment but don't write to disk. This is used when the disk may have failed
     */
    void closeHandlers() {
        Utils.swallow(LOGGER, Level.WARN, "offsetIndex", () -> lazyOffsetIndex.closeHandler());
        Utils.swallow(LOGGER, Level.WARN, "timeIndex", () -> lazyTimeIndex.closeHandler());
        Utils.swallow(LOGGER, Level.WARN, "log", () -> log.closeHandlers());
        Utils.closeQuietly(txnIndex, "txnIndex", LOGGER);
    }

    /**
     * Delete this log segment from the filesystem.
     */
    public void deleteIfExists() throws IOException {
        try {
            Utils.tryAll(asList(
                () -> deleteTypeIfExists(() -> log.deleteIfExists(), "log", log.file(), true),
                () -> deleteTypeIfExists(() -> lazyOffsetIndex.deleteIfExists(), "offset index", offsetIndexFile(), true),
                () -> deleteTypeIfExists(() -> lazyTimeIndex.deleteIfExists(), "time index", timeIndexFile(), true),
                () -> deleteTypeIfExists(() -> txnIndex.deleteIfExists(), "transaction index", txnIndex.file(), false)));
        } catch (Throwable t) {
            if (t instanceof IOException)
                throw (IOException) t;
            if (t instanceof Error)
                throw (Error) t;
            if (t instanceof RuntimeException)
                throw (RuntimeException) t;
            throw new IllegalStateException("Unexpected exception: " + t.getMessage(), t);
        }
    }

    // Helper method for `deleteIfExists()`
    private Void deleteTypeIfExists(StorageAction<Boolean, IOException> delete, String fileType, File file, boolean logIfMissing) throws IOException {
        try {
            if (delete.execute())
                LOGGER.info("Deleted {} {}.", fileType, file.getAbsolutePath());
            else if (logIfMissing)
                LOGGER.info("Failed to delete {} {} because it does not exist.", fileType, file.getAbsolutePath());
            return null;
        } catch (IOException e) {
            throw new IOException("Delete of " + fileType + " " + file.getAbsolutePath() + " failed.", e);
        }
    }

    // Visible for testing
    public boolean deleted() {
        return !log.file().exists() && !offsetIndexFile().exists() && !timeIndexFile().exists() && !txnIndex.file().exists();
    }


    /**
     * The last modified time of this log segment as a unix time stamp
     */
    public long lastModified() {
        return log.file().lastModified();
    }

    /**
     * The largest timestamp this segment contains, if maxTimestampSoFar >= 0, otherwise None.
     */
    public OptionalLong largestRecordTimestamp() throws IOException {
        long maxTimestampSoFar = maxTimestampSoFar();
        if (maxTimestampSoFar >= 0)
            return OptionalLong.of(maxTimestampSoFar);
        return OptionalLong.empty();
    }

    /**
     * The largest timestamp this segment contains.
     */
    public long largestTimestamp() throws IOException {
        long maxTimestampSoFar = maxTimestampSoFar();
        if (maxTimestampSoFar >= 0)
            return maxTimestampSoFar;
        return lastModified();
    }

    /**
     * Change the last modified time for this log segment
     */
    public void setLastModified(long ms) throws IOException {
        FileTime fileTime = FileTime.fromMillis(ms);
        Files.setLastModifiedTime(log.file().toPath(), fileTime);
        Files.setLastModifiedTime(offsetIndexFile().toPath(), fileTime);
        Files.setLastModifiedTime(timeIndexFile().toPath(), fileTime);
    }

    public static LogSegment open(File dir, long baseOffset, LogConfig config, Time time, int initFileSize, boolean preallocate) throws IOException {
        return open(dir, baseOffset, config, time, false, initFileSize, preallocate, "");
    }

    public static LogSegment open(File dir, long baseOffset, LogConfig config, Time time, boolean fileAlreadyExists,
                                  int initFileSize, boolean preallocate, String fileSuffix) throws IOException {
        int maxIndexSize = config.maxIndexSize;
        return new LogSegment(
            FileRecords.open(LogFileUtils.logFile(dir, baseOffset, fileSuffix), fileAlreadyExists, initFileSize, preallocate),
            LazyIndex.forOffset(LogFileUtils.offsetIndexFile(dir, baseOffset, fileSuffix), baseOffset, maxIndexSize),
            LazyIndex.forTime(LogFileUtils.timeIndexFile(dir, baseOffset, fileSuffix), baseOffset, maxIndexSize),
            new TransactionIndex(baseOffset, LogFileUtils.transactionIndexFile(dir, baseOffset, fileSuffix)),
            baseOffset,
            config.indexInterval,
            config.randomSegmentJitter(),
            time);
    }

    public static void deleteIfExists(File dir, long baseOffset, String fileSuffix) throws IOException {
        deleteFileIfExists(LogFileUtils.offsetIndexFile(dir, baseOffset, fileSuffix));
        deleteFileIfExists(LogFileUtils.timeIndexFile(dir, baseOffset, fileSuffix));
        deleteFileIfExists(LogFileUtils.transactionIndexFile(dir, baseOffset, fileSuffix));
        deleteFileIfExists(LogFileUtils.logFile(dir, baseOffset, fileSuffix));
    }

    private static boolean deleteFileIfExists(File file) throws IOException {
        return Files.deleteIfExists(file.toPath());
    }

}
