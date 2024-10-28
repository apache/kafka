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
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.require;
import static org.apache.kafka.storage.internals.log.LogFileUtils.CLEANED_FILE_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.DELETED_FILE_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.DELETE_DIR_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.FUTURE_DIR_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.STRAY_DIR_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.SWAP_FILE_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.isLogFile;

/**
 * An append-only log for storing messages locally. The log is a sequence of LogSegments, each with a base offset.
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 * NOTE: this class is not thread-safe, and it relies on the thread safety provided by the Log class.
 */
public class LocalLog {

    private static final Logger LOG = LoggerFactory.getLogger(LocalLog.class);

    public static final Pattern DELETE_DIR_PATTERN = Pattern.compile("^(\\S+)-(\\S+)\\.(\\S+)" + LogFileUtils.DELETE_DIR_SUFFIX);
    public static final Pattern FUTURE_DIR_PATTERN = Pattern.compile("^(\\S+)-(\\S+)\\.(\\S+)" + LogFileUtils.FUTURE_DIR_SUFFIX);
    public static final Pattern STRAY_DIR_PATTERN = Pattern.compile("^(\\S+)-(\\S+)\\.(\\S+)" + LogFileUtils.STRAY_DIR_SUFFIX);
    public static final long UNKNOWN_OFFSET = -1L;

    // Last time the log was flushed
    private final AtomicLong lastFlushedTime;
    private final String logIdent;
    private final LogSegments segments;
    private final Scheduler scheduler;
    private final Time time;
    private final TopicPartition topicPartition;
    private final LogDirFailureChannel logDirFailureChannel;
    private final Logger logger;

    private volatile LogOffsetMetadata nextOffsetMetadata;
    // The memory mapped buffer for index files of this log will be closed with either delete() or closeHandlers()
    // After memory mapped buffer is closed, no disk IO operation should be performed for this log.
    private volatile boolean isMemoryMappedBufferClosed = false;
    // Cache value of parent directory to avoid allocations in hot paths like ReplicaManager.checkpointHighWatermarks
    private volatile String parentDir;
    private volatile LogConfig config;
    private volatile long recoveryPoint;
    private File dir;

    /**
     * @param dir The directory in which log segments are created.
     * @param config The log configuration settings
     * @param segments The non-empty log segments recovered from disk
     * @param recoveryPoint The offset at which to begin the next recovery i.e. the first offset which has not been flushed to disk
     * @param nextOffsetMetadata The offset where the next message could be appended
     * @param scheduler The thread pool scheduler used for background actions
     * @param time The time instance used for checking the clock
     * @param topicPartition The topic partition associated with this log
     * @param logDirFailureChannel The LogDirFailureChannel instance to asynchronously handle Log dir failure
     */
    public LocalLog(File dir,
                    LogConfig config,
                    LogSegments segments,
                    long recoveryPoint,
                    LogOffsetMetadata nextOffsetMetadata,
                    Scheduler scheduler,
                    Time time,
                    TopicPartition topicPartition,
                    LogDirFailureChannel logDirFailureChannel) {
        this.dir = dir;
        this.config = config;
        this.segments = segments;
        this.recoveryPoint = recoveryPoint;
        this.nextOffsetMetadata = nextOffsetMetadata;
        this.scheduler = scheduler;
        this.time = time;
        this.topicPartition = topicPartition;
        this.logDirFailureChannel = logDirFailureChannel;
        this.logIdent = "[LocalLog partition=" + topicPartition + ", dir=" + dir + "] ";
        this.logger = new LogContext(logIdent).logger(LocalLog.class);
        // Last time the log was flushed
        this.lastFlushedTime = new AtomicLong(time.milliseconds());
        this.parentDir = dir.getParent();
    }

    public File dir() {
        return dir;
    }

    public Logger logger() {
        return logger;
    }

    public LogConfig config() {
        return config;
    }

    public LogSegments segments() {
        return segments;
    }

    public Scheduler scheduler() {
        return scheduler;
    }

    public LogOffsetMetadata nextOffsetMetadata() {
        return nextOffsetMetadata;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public LogDirFailureChannel logDirFailureChannel() {
        return logDirFailureChannel;
    }

    public long recoveryPoint() {
        return recoveryPoint;
    }

    public Time time() {
        return time;
    }

    public String name() {
        return dir.getName();
    }

    public String parentDir() {
        return parentDir;
    }

    public File parentDirFile() {
        return new File(parentDir);
    }

    public boolean isFuture() {
        return dir.getName().endsWith(LogFileUtils.FUTURE_DIR_SUFFIX);
    }

    private <T> T maybeHandleIOException(Supplier<String> errorMsgSupplier, StorageAction<T, IOException> function) {
        return maybeHandleIOException(logDirFailureChannel, parentDir, errorMsgSupplier, function);
    }

    /**
     * Rename the directory of the log
     * @param name the new dir name
     * @throws KafkaStorageException if rename fails
     */
    public boolean renameDir(String name) {
        return maybeHandleIOException(
            () -> "Error while renaming dir for " + topicPartition + " in log dir " +  dir.getParent(),
            () -> {
                File renamedDir = new File(dir.getParent(), name);
                Utils.atomicMoveWithFallback(dir.toPath(), renamedDir.toPath());
                if (!renamedDir.equals(dir)) {
                    dir = renamedDir;
                    parentDir = renamedDir.getParent();
                    segments.updateParentDir(renamedDir);
                    return true;
                } else {
                    return false;
                }
            }
        );
    }

    /**
     * Update the existing configuration to the new provided configuration.
     * @param newConfig the new configuration to be updated to
     */
    public void updateConfig(LogConfig newConfig) {
        LogConfig oldConfig = config;
        config = newConfig;
        RecordVersion oldRecordVersion = oldConfig.recordVersion();
        RecordVersion newRecordVersion = newConfig.recordVersion();
        if (newRecordVersion.precedes(oldRecordVersion)) {
            logger.warn("Record format version has been downgraded from {} to {}.", oldRecordVersion, newRecordVersion);
        }
    }

    public void checkIfMemoryMappedBufferClosed() {
        if (isMemoryMappedBufferClosed) {
            throw new KafkaStorageException("The memory mapped buffer for log of " + topicPartition + " is already closed");
        }
    }

    public void updateRecoveryPoint(long newRecoveryPoint) {
        recoveryPoint = newRecoveryPoint;
    }

    /**
     * Update recoveryPoint to provided offset and mark the log as flushed, if the offset is greater
     * than the existing recoveryPoint.
     *
     * @param offset the offset to be updated
     */
    public void markFlushed(long offset) {
        checkIfMemoryMappedBufferClosed();
        if (offset > recoveryPoint) {
            updateRecoveryPoint(offset);
            lastFlushedTime.set(time.milliseconds());
        }
    }

    /**
     * The number of messages appended to the log since the last flush
     */
    public long unflushedMessages() {
        return logEndOffset() - recoveryPoint;
    }

    /**
     * Flush local log segments for all offsets up to offset-1.
     * Does not update the recovery point.
     *
     * @param offset The offset to flush up to (non-inclusive)
     */
    public void flush(long offset) throws IOException {
        long currentRecoveryPoint = recoveryPoint;
        if (currentRecoveryPoint <= offset) {
            Collection<LogSegment> segmentsToFlush = segments.values(currentRecoveryPoint, offset);
            for (LogSegment logSegment : segmentsToFlush) {
                logSegment.flush();
            }
            // If there are any new segments, we need to flush the parent directory for crash consistency.
            if (segmentsToFlush.stream().anyMatch(s -> s.baseOffset() >= currentRecoveryPoint)) {
                // The directory might be renamed concurrently for topic deletion, which may cause NoSuchFileException here.
                // Since the directory is to be deleted anyways, we just swallow NoSuchFileException and let it go.
                Utils.flushDirIfExists(dir.toPath());
            }
        }
    }

    /**
     * The time this log is last known to have been fully flushed to disk
     */
    public long lastFlushTime() {
        return lastFlushedTime.get();
    }

    /**
     * The offset metadata of the next message that will be appended to the log
     */
    public LogOffsetMetadata logEndOffsetMetadata() {
        return nextOffsetMetadata;
    }

    /**
     * The offset of the next message that will be appended to the log
     */
    public long logEndOffset() {
        return nextOffsetMetadata.messageOffset;
    }

    /**
     * Update end offset of the log, and update the recoveryPoint.
     *
     * @param endOffset the new end offset of the log
     */
    public void updateLogEndOffset(long endOffset) {
        nextOffsetMetadata = new LogOffsetMetadata(endOffset, segments.activeSegment().baseOffset(), segments.activeSegment().size());
        if (recoveryPoint > endOffset) {
            updateRecoveryPoint(endOffset);
        }
    }

    /**
     * Close file handlers used by log but don't write to disk.
     * This is called if the log directory is offline.
     */
    public void closeHandlers() {
        segments.closeHandlers();
        isMemoryMappedBufferClosed = true;
    }

    /**
     * Closes the segments of the log.
     */
    public void close() {
        maybeHandleIOException(
            () -> "Error while renaming dir for " + topicPartition + " in dir " + dir.getParent(),
            () -> {
                checkIfMemoryMappedBufferClosed();
                segments.close();
                return null;
            }
        );
    }

    /**
     * Completely delete this log directory with no delay.
     */
    public void deleteEmptyDir() {
        maybeHandleIOException(
            () -> "Error while deleting dir for " + topicPartition + " in dir " + dir.getParent(),
            () -> {
                if (!segments.isEmpty()) {
                    throw new IllegalStateException("Can not delete directory when " + segments.numberOfSegments() + " segments are still present");
                }
                if (!isMemoryMappedBufferClosed) {
                    throw new IllegalStateException("Can not delete directory when memory mapped buffer for log of " + topicPartition + " is still open.");
                }
                Utils.delete(dir);
                return null;
            }
        );
    }

    /**
     * Completely delete all segments with no delay.
     * @return the deleted segments
     */
    public List<LogSegment> deleteAllSegments() {
        return maybeHandleIOException(
            () -> "Error while deleting all segments for $topicPartition in dir ${dir.getParent}",
            () -> {
                List<LogSegment> deletableSegments = new ArrayList<>(segments.values());
                removeAndDeleteSegments(
                        segments.values(),
                        false,
                        toDelete -> logger.info("Deleting segments as the log has been deleted: {}", toDelete.stream()
                            .map(LogSegment::toString)
                            .collect(Collectors.joining(", "))));
                isMemoryMappedBufferClosed = true;
                return deletableSegments;
            }
        );
    }

    /**
     * This method deletes the given log segments by doing the following for each of them:
     * <ul>
     *  <li>It removes the segment from the segment map so that it will no longer be used for reads.
     *  <li>It renames the index and log files by appending .deleted to the respective file name
     *  <li>It can either schedule an asynchronous delete operation to occur in the future or perform the deletion synchronously
     * </ul>
     * Asynchronous deletion allows reads to happen concurrently without synchronization and without the possibility of
     * physically deleting a file while it is being read.
     * This method does not convert IOException to KafkaStorageException, the immediate caller
     * is expected to catch and handle IOException.
     *
     * @param segmentsToDelete The log segments to schedule for deletion
     * @param asyncDelete Whether the segment files should be deleted asynchronously
     * @param reason The reason for the segment deletion
     */
    public void removeAndDeleteSegments(Collection<LogSegment> segmentsToDelete,
                                         boolean asyncDelete,
                                         SegmentDeletionReason reason) throws IOException {
        if (!segmentsToDelete.isEmpty()) {
            // Most callers hold an iterator into the `segments` collection and `removeAndDeleteSegment` mutates it by
            // removing the deleted segment, we should force materialization of the iterator here, so that results of the
            // iteration remain valid and deterministic. We should also pass only the materialized view of the
            // iterator to the logic that actually deletes the segments.
            List<LogSegment> toDelete = new ArrayList<>(segmentsToDelete);
            reason.logReason(toDelete);
            toDelete.forEach(segment -> segments.remove(segment.baseOffset()));
            deleteSegmentFiles(toDelete, asyncDelete, dir, topicPartition, config, scheduler, logDirFailureChannel, logIdent);
        }
    }

    /**
     * This method deletes the given segment and creates a new segment with the given new base offset. It ensures an
     * active segment exists in the log at all times during this process.
     * <br/>
     * Asynchronous deletion allows reads to happen concurrently without synchronization and without the possibility of
     * physically deleting a file while it is being read.
     * <br/>
     * This method does not convert IOException to KafkaStorageException, the immediate caller
     * is expected to catch and handle IOException.
     *
     * @param newOffset The base offset of the new segment
     * @param segmentToDelete The old active segment to schedule for deletion
     * @param asyncDelete Whether the segment files should be deleted asynchronously
     * @param reason The reason for the segment deletion
     */
    public LogSegment createAndDeleteSegment(long newOffset,
                                              LogSegment segmentToDelete,
                                              boolean asyncDelete,
                                              SegmentDeletionReason reason) throws IOException {
        if (newOffset == segmentToDelete.baseOffset()) {
            segmentToDelete.changeFileSuffixes("", LogFileUtils.DELETED_FILE_SUFFIX);
        }
        LogSegment newSegment = LogSegment.open(dir,
                newOffset,
                config,
                time,
                config.initFileSize(),
                config.preallocate);
        segments.add(newSegment);

        reason.logReason(singletonList(segmentToDelete));
        if (newOffset != segmentToDelete.baseOffset()) {
            segments.remove(segmentToDelete.baseOffset());
        }
        deleteSegmentFiles(singletonList(segmentToDelete), asyncDelete, dir, topicPartition, config, scheduler, logDirFailureChannel, logIdent);
        return newSegment;
    }

    /**
     * Given a message offset, find its corresponding offset metadata in the log.
     * If the message offset is out of range, throw an OffsetOutOfRangeException
     */
    public LogOffsetMetadata convertToOffsetMetadataOrThrow(long offset) throws IOException {
        FetchDataInfo fetchDataInfo = read(offset, 1, false, nextOffsetMetadata, false);
        return fetchDataInfo.fetchOffsetMetadata;
    }

    /**
     * Read messages from the log.
     *
     * @param startOffset The offset to begin reading at
     * @param maxLength The maximum number of bytes to read
     * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
     * @param maxOffsetMetadata The metadata of the maximum offset to be fetched
     * @param includeAbortedTxns If true, aborted transactions are included
     * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset
     * @return The fetch data information including fetch starting offset metadata and messages read.
     */
    public FetchDataInfo read(long startOffset,
                       int maxLength,
                       boolean minOneMessage,
                       LogOffsetMetadata maxOffsetMetadata,
                       boolean includeAbortedTxns) throws IOException {
        return maybeHandleIOException(
                () -> "Exception while reading from " + topicPartition + " in dir " + dir.getParent(),
                () -> {
                    logger.trace("Reading maximum $maxLength bytes at offset {} from log with total length {} bytes",
                            startOffset, segments.sizeInBytes());

                    LogOffsetMetadata endOffsetMetadata = nextOffsetMetadata;
                    long endOffset = endOffsetMetadata.messageOffset;
                    Optional<LogSegment> segmentOpt = segments.floorSegment(startOffset);
                    // return error on attempt to read beyond the log end offset
                    if (startOffset > endOffset || segmentOpt.isEmpty()) {
                        throw new OffsetOutOfRangeException("Received request for offset " + startOffset + " for partition " + topicPartition + ", " +
                                "but we only have log segments upto " + endOffset + ".");
                    }
                    if (startOffset == maxOffsetMetadata.messageOffset) return emptyFetchDataInfo(maxOffsetMetadata, includeAbortedTxns);
                    if (startOffset > maxOffsetMetadata.messageOffset) return emptyFetchDataInfo(convertToOffsetMetadataOrThrow(startOffset), includeAbortedTxns);

                    // Do the read on the segment with a base offset less than the target offset
                    // but if that segment doesn't contain any messages with an offset greater than that
                    // continue to read from successive segments until we get some messages or we reach the end of the log
                    FetchDataInfo fetchDataInfo = null;
                    while (fetchDataInfo == null && segmentOpt.isPresent()) {
                        LogSegment segment = segmentOpt.get();
                        long baseOffset = segment.baseOffset();

                        // 1. If `maxOffsetMetadata#segmentBaseOffset < segment#baseOffset`, then return maxPosition as empty.
                        // 2. Use the max-offset position if it is on this segment; otherwise, the segment size is the limit.
                        // 3. When maxOffsetMetadata is message-offset-only, then we don't know the relativePositionInSegment so
                        //    return maxPosition as empty to avoid reading beyond the max-offset
                        Optional<Long> maxPositionOpt;
                        if (segment.baseOffset() < maxOffsetMetadata.segmentBaseOffset)
                            maxPositionOpt = Optional.of((long) segment.size());
                        else if (segment.baseOffset() == maxOffsetMetadata.segmentBaseOffset && !maxOffsetMetadata.messageOffsetOnly())
                            maxPositionOpt = Optional.of((long) maxOffsetMetadata.relativePositionInSegment);
                        else
                            maxPositionOpt = Optional.empty();

                        fetchDataInfo = segment.read(startOffset, maxLength, maxPositionOpt, minOneMessage);
                        if (fetchDataInfo != null) {
                            if (includeAbortedTxns) {
                                fetchDataInfo = addAbortedTransactions(startOffset, segment, fetchDataInfo);
                            }
                        } else {
                            segmentOpt = segments.higherSegment(baseOffset);
                        }
                    }
                    if (fetchDataInfo != null) {
                        return fetchDataInfo;
                    } else {
                        // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
                        // this can happen when all messages with offset larger than start offsets have been deleted.
                        // In this case, we will return the empty set with log end offset metadata
                        return new FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY);
                    }
                }
        );
    }

    public void append(long lastOffset, long largestTimestamp, long shallowOffsetOfMaxTimestamp, MemoryRecords records) throws IOException {
        segments.activeSegment().append(lastOffset, largestTimestamp, shallowOffsetOfMaxTimestamp, records);
        updateLogEndOffset(lastOffset + 1);
    }

    FetchDataInfo addAbortedTransactions(long startOffset, LogSegment segment, FetchDataInfo fetchInfo) throws IOException {
        int fetchSize = fetchInfo.records.sizeInBytes();
        OffsetPosition startOffsetPosition = new OffsetPosition(
                fetchInfo.fetchOffsetMetadata.messageOffset,
                fetchInfo.fetchOffsetMetadata.relativePositionInSegment);
        long upperBoundOffset = segment.fetchUpperBoundOffset(startOffsetPosition, fetchSize)
                .orElse(segments.higherSegment(segment.baseOffset())
                        .map(LogSegment::baseOffset).orElse(logEndOffset()));

        List<FetchResponseData.AbortedTransaction> abortedTransactions = new ArrayList<>();
        Consumer<List<AbortedTxn>> accumulator = abortedTxns -> {
            for (AbortedTxn abortedTxn : abortedTxns)
                abortedTransactions.add(abortedTxn.asAbortedTransaction());
        };
        collectAbortedTransactions(startOffset, upperBoundOffset, segment, accumulator);
        return new FetchDataInfo(fetchInfo.fetchOffsetMetadata,
                fetchInfo.records,
                fetchInfo.firstEntryIncomplete,
                Optional.of(abortedTransactions));
    }

    private void collectAbortedTransactions(long startOffset, long upperBoundOffset,
                                            LogSegment startingSegment,
                                            Consumer<List<AbortedTxn>> accumulator) {
        Iterator<LogSegment> higherSegments = segments.higherSegments(startingSegment.baseOffset()).iterator();
        Optional<LogSegment> segmentEntryOpt = Optional.of(startingSegment);
        while (segmentEntryOpt.isPresent()) {
            LogSegment segment = segmentEntryOpt.get();
            TxnIndexSearchResult searchResult = segment.collectAbortedTxns(startOffset, upperBoundOffset);
            accumulator.accept(searchResult.abortedTransactions);
            if (searchResult.isComplete) return;
            segmentEntryOpt = nextItem(higherSegments);
        }
    }

    public List<AbortedTxn> collectAbortedTransactions(long logStartOffset, long baseOffset, long upperBoundOffset) {
        Optional<LogSegment> segmentEntry = segments.floorSegment(baseOffset);
        List<AbortedTxn> allAbortedTxns = new ArrayList<>();
        segmentEntry.ifPresent(logSegment -> collectAbortedTransactions(logStartOffset, upperBoundOffset, logSegment, allAbortedTxns::addAll));
        return allAbortedTxns;
    }

    /**
     * Roll the log over to a new active segment starting with the current logEndOffset.
     * This will trim the index to the exact size of the number of entries it currently contains.
     *
     * @param expectedNextOffset The expected next offset after the segment is rolled
     *
     * @return The newly rolled segment
     */
    public LogSegment roll(Long expectedNextOffset) {
        return maybeHandleIOException(
            () -> "Error while rolling log segment for " + topicPartition + " in dir " + dir.getParent(),
            () -> {
                long start = time.hiResClockMs();
                checkIfMemoryMappedBufferClosed();
                long newOffset = Math.max(expectedNextOffset, logEndOffset());
                File logFile = LogFileUtils.logFile(dir, newOffset, "");
                LogSegment activeSegment = segments.activeSegment();
                if (segments.contains(newOffset)) {
                    // segment with the same base offset already exists and loaded
                    if (activeSegment.baseOffset() == newOffset && activeSegment.size() == 0) {
                        // We have seen this happen (see KAFKA-6388) after shouldRoll() returns true for an
                        // active segment of size zero because of one of the indexes is "full" (due to _maxEntries == 0).
                        logger.warn("Trying to roll a new log segment with start offset {}=max(provided offset = {}, LEO = {}) " +
                                    "while it already exists and is active with size 0. Size of time index: {}, size of offset index: {}.",
                                newOffset, expectedNextOffset, logEndOffset(), activeSegment.timeIndex().entries(), activeSegment.offsetIndex().entries());
                        LogSegment newSegment = createAndDeleteSegment(
                                newOffset,
                                activeSegment,
                                true,
                                toDelete -> logger.info("Deleting segments as part of log roll: {}", toDelete.stream()
                                    .map(LogSegment::toString)
                                    .collect(Collectors.joining(", "))));
                        updateLogEndOffset(nextOffsetMetadata.messageOffset);
                        logger.info("Rolled new log segment at offset {} in {} ms.", newOffset, time.hiResClockMs() - start);
                        return newSegment;
                    } else {
                        throw new KafkaException("Trying to roll a new log segment for topic partition " + topicPartition + " with start offset " + newOffset +
                                " =max(provided offset = " + expectedNextOffset + ", LEO = " + logEndOffset() + ") while it already exists. Existing " +
                                "segment is " + segments.get(newOffset) + ".");
                    }
                } else if (!segments.isEmpty() && newOffset < activeSegment.baseOffset()) {
                    throw new KafkaException(
                            "Trying to roll a new log segment for topic partition " + topicPartition + " with " +
                            "start offset " + newOffset + " =max(provided offset = " + expectedNextOffset + ", LEO = " + logEndOffset() + ") lower than start offset of the active segment " + activeSegment);
                } else {
                    File offsetIdxFile = LogFileUtils.offsetIndexFile(dir, newOffset);
                    File timeIdxFile = LogFileUtils.timeIndexFile(dir, newOffset);
                    File txnIdxFile = LogFileUtils.transactionIndexFile(dir, newOffset);
                    for (File file : Arrays.asList(logFile, offsetIdxFile, timeIdxFile, txnIdxFile)) {
                        if (file.exists()) {
                            logger.warn("Newly rolled segment file {} already exists; deleting it first", file.getAbsolutePath());
                            Files.delete(file.toPath());
                        }
                    }
                    if (segments.lastSegment().isPresent()) {
                        segments.lastSegment().get().onBecomeInactiveSegment();
                    }
                }
                LogSegment newSegment = LogSegment.open(dir,
                        newOffset,
                        config,
                        time,
                        config.initFileSize(),
                        config.preallocate);
                segments.add(newSegment);

                // We need to update the segment base offset and append position data of the metadata when log rolls.
                // The next offset should not change.
                updateLogEndOffset(nextOffsetMetadata.messageOffset);
                logger.info("Rolled new log segment at offset {} in {} ms.", newOffset, time.hiResClockMs() - start);
                return newSegment;
            }
        );
    }

    /**
     *  Delete all data in the local log and start at the new offset.
     *
     *  @param newOffset The new offset to start the log with
     *  @return the list of segments that were scheduled for deletion
     */
    public List<LogSegment> truncateFullyAndStartAt(long newOffset) {
        return maybeHandleIOException(
            () -> "Error while truncating the entire log for " + topicPartition + " in dir " + dir.getParent(),
            () -> {
                logger.debug("Truncate and start at offset {}", newOffset);
                checkIfMemoryMappedBufferClosed();
                List<LogSegment> segmentsToDelete = new ArrayList<>(segments.values());

                if (!segmentsToDelete.isEmpty()) {
                    removeAndDeleteSegments(segmentsToDelete.subList(0, segmentsToDelete.size() - 1), true, new LogTruncation(logger));
                    // Use createAndDeleteSegment() to create new segment first and then delete the old last segment to prevent missing
                    // active segment during the deletion process
                    createAndDeleteSegment(newOffset, segmentsToDelete.get(segmentsToDelete.size() - 1), true, new LogTruncation(logger));
                }
                updateLogEndOffset(newOffset);
                return segmentsToDelete;
            }
        );
    }

    /**
     * Truncate this log so that it ends with the greatest offset < targetOffset.
     *
     * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
     * @return the list of segments that were scheduled for deletion
     */
    public Collection<LogSegment> truncateTo(long targetOffset) throws IOException {
        Collection<LogSegment> deletableSegments = segments.filter(segment -> segment.baseOffset() > targetOffset);
        removeAndDeleteSegments(deletableSegments, true, new LogTruncation(logger));
        segments.activeSegment().truncateTo(targetOffset);
        updateLogEndOffset(targetOffset);
        return deletableSegments;
    }

    /**
     * Return a directory name to rename the log directory to for async deletion.
     * The name will be in the following format: "topic-partitionId.uniqueId-delete".
     * If the topic name is too long, it will be truncated to prevent the total name
     * from exceeding 255 characters.
     */
    public static String logDeleteDirName(TopicPartition topicPartition) {
        return logDirNameWithSuffixCappedLength(topicPartition, LogFileUtils.DELETE_DIR_SUFFIX);
    }

    /**
     * Return a directory name to rename the log directory to for stray partition deletion.
     * The name will be in the following format: "topic-partitionId.uniqueId-stray".
     * If the topic name is too long, it will be truncated to prevent the total name
     * from exceeding 255 characters.
     */
    public static String logStrayDirName(TopicPartition topicPartition) {
        return logDirNameWithSuffixCappedLength(topicPartition, LogFileUtils.STRAY_DIR_SUFFIX);
    }

    /**
     * Return a future directory name for the given topic partition. The name will be in the following
     * format: topic-partition.uniqueId-future where topic, partition and uniqueId are variables.
     */
    public static String logFutureDirName(TopicPartition topicPartition) {
        return logDirNameWithSuffix(topicPartition, LogFileUtils.FUTURE_DIR_SUFFIX);
    }

    /**
     * Return a new directory name in the following format: "${topic}-${partitionId}.${uniqueId}${suffix}".
     * If the topic name is too long, it will be truncated to prevent the total name
     * from exceeding 255 characters.
     */
    private static String logDirNameWithSuffixCappedLength(TopicPartition topicPartition, String suffix) {
        String uniqueId = UUID.randomUUID().toString().replaceAll("-", "");
        String fullSuffix = "-" + topicPartition.partition() + "." + uniqueId + suffix;
        int prefixLength = Math.min(topicPartition.topic().length(), 255 - fullSuffix.length());
        return topicPartition.topic().substring(0, prefixLength) + fullSuffix;
    }

    private static String logDirNameWithSuffix(TopicPartition topicPartition, String suffix) {
        String uniqueId = UUID.randomUUID().toString().replaceAll("-", "");
        return logDirName(topicPartition) + "." + uniqueId + suffix;
    }

    /**
     * Return a directory name for the given topic partition. The name will be in the following
     * format: topic-partition where topic, partition are variables.
     */
    public static String logDirName(TopicPartition topicPartition) {
        return topicPartition.topic() + "-" + topicPartition.partition();
    }

    private static KafkaException exception(File dir) throws IOException {
        return new KafkaException("Found directory " + dir.getCanonicalPath() + ", '" + dir.getName() + "' is not in the form of " +
                "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
                "Kafka's log directories (and children) should only contain Kafka topic data.");
    }

    /**
     * Parse the topic and partition out of the directory name of a log
     */
    public static TopicPartition parseTopicPartitionName(File dir) throws IOException {
        if (dir == null) {
            throw new KafkaException("dir should not be null");
        }
        String dirName = dir.getName();
        if (dirName.isEmpty() || !dirName.contains("-")) {
            throw exception(dir);
        }
        if (dirName.endsWith(DELETE_DIR_SUFFIX) && !DELETE_DIR_PATTERN.matcher(dirName).matches() ||
                dirName.endsWith(FUTURE_DIR_SUFFIX) && !FUTURE_DIR_PATTERN.matcher(dirName).matches() ||
                dirName.endsWith(STRAY_DIR_SUFFIX) && !STRAY_DIR_PATTERN.matcher(dirName).matches()) {
            throw exception(dir);
        }
        String name = (dirName.endsWith(DELETE_DIR_SUFFIX) || dirName.endsWith(FUTURE_DIR_SUFFIX) || dirName.endsWith(STRAY_DIR_SUFFIX))
            ? dirName.substring(0, dirName.lastIndexOf('.'))
            : dirName;

        int index = name.lastIndexOf('-');
        String topic = name.substring(0, index);
        String partitionString = name.substring(index + 1);
        if (topic.isEmpty() || partitionString.isEmpty()) {
            throw exception(dir);
        }
        try {
            return new TopicPartition(topic, Integer.parseInt(partitionString));
        } catch (NumberFormatException nfe) {
            throw exception(dir);
        }
    }

    /**
     * Wraps the value of iterator.next() in an Optional instance.
     *
     * @param iterator given iterator to iterate over
     * @return if a next element exists, Optional#empty otherwise.
     * @param <T> the type of object held within the iterator
     */
    public static <T> Optional<T> nextItem(Iterator<T> iterator) {
        return iterator.hasNext() ? Optional.of(iterator.next()) : Optional.empty();
    }

    private static FetchDataInfo emptyFetchDataInfo(LogOffsetMetadata fetchOffsetMetadata, boolean includeAbortedTxns) {
        Optional<List<FetchResponseData.AbortedTransaction>> abortedTransactions = includeAbortedTxns
            ? Optional.of(Collections.emptyList())
            : Optional.empty();
        return new FetchDataInfo(fetchOffsetMetadata, MemoryRecords.EMPTY, false, abortedTransactions);
    }

    /**
     * Invokes the provided function and handles any IOException raised by the function by marking the
     * provided directory offline.
     *
     * @param logDirFailureChannel Used to asynchronously handle log directory failure.
     * @param logDir               The log directory to be marked offline during an IOException.
     * @param errorMsgSupplier     The supplier for the error message to be used when marking the log directory offline.
     * @param function             The function to be executed.
     * @return The value returned by the function after a successful invocation
     */
    public static <T> T maybeHandleIOException(LogDirFailureChannel logDirFailureChannel,
                                               String logDir,
                                               Supplier<String> errorMsgSupplier,
                                               StorageAction<T, IOException> function) {
        if (logDirFailureChannel.hasOfflineLogDir(logDir)) {
            throw new KafkaStorageException("The log dir " + logDir + " is already offline due to a previous IO exception.");
        }
        try {
            return function.execute();
        } catch (IOException ioe) {
            String errorMsg = errorMsgSupplier.get();
            logDirFailureChannel.maybeAddOfflineLogDir(logDir, errorMsg, ioe);
            throw new KafkaStorageException(errorMsg, ioe);
        }
    }

    /**
     * Perform physical deletion of the index and log files for the given segment.
     * Prior to the deletion, the index and log files are renamed by appending .deleted to the
     * respective file name. Allows these files to be optionally deleted asynchronously.
     * <br/>
     * This method assumes that the file exists. It does not need to convert IOException
     * (thrown from changeFileSuffixes) to KafkaStorageException because it is either called before
     * all logs are loaded or the caller will catch and handle IOException.
     *
     * @param segmentsToDelete The segments to be deleted
     * @param asyncDelete If true, the deletion of the segments is done asynchronously
     * @param dir The directory in which the log will reside
     * @param topicPartition The topic
     * @param config The log configuration settings
     * @param scheduler The thread pool scheduler used for background actions
     * @param logDirFailureChannel The LogDirFailureChannel to asynchronously handle log dir failure
     * @param logPrefix The logging prefix
     * @throws IOException if the file can't be renamed and still exists
     */
    public static void deleteSegmentFiles(Collection<LogSegment> segmentsToDelete,
                                          boolean asyncDelete,
                                          File dir,
                                          TopicPartition topicPartition,
                                          LogConfig config,
                                          Scheduler scheduler,
                                          LogDirFailureChannel logDirFailureChannel,
                                          String logPrefix) throws IOException {
        for (LogSegment segment : segmentsToDelete) {
            if (!segment.hasSuffix(DELETED_FILE_SUFFIX)) {
                segment.changeFileSuffixes("", DELETED_FILE_SUFFIX);
            }
        }

        Runnable deleteSegments = () -> {
            LOG.info("{}Deleting segment files {}", logPrefix, segmentsToDelete.stream().map(LogSegment::toString).collect(Collectors.joining(", ")));
            String parentDir = dir.getParent();
            maybeHandleIOException(logDirFailureChannel, parentDir,
                    () -> "Error while deleting segments for " + topicPartition + " in dir " + parentDir,
                    () -> {
                        for (LogSegment segment : segmentsToDelete) {
                            segment.deleteIfExists();
                        }
                        return null;
                    });
        };
        if (asyncDelete) {
            scheduler.scheduleOnce("delete-file", deleteSegments, config.fileDeleteDelayMs);
        } else {
            deleteSegments.run();
        }
    }

    public static LogSegment createNewCleanedSegment(File dir, LogConfig logConfig, long baseOffset) throws IOException {
        LogSegment.deleteIfExists(dir, baseOffset, CLEANED_FILE_SUFFIX);
        return LogSegment.open(dir, baseOffset, logConfig, Time.SYSTEM, false, logConfig.initFileSize(), logConfig.preallocate, CLEANED_FILE_SUFFIX);
    }

    /**
     * Split a segment into one or more segments such that there is no offset overflow in any of them. The
     * resulting segments will contain the exact same messages that are present in the input segment. On successful
     * completion of this method, the input segment will be deleted and will be replaced by the resulting new segments.
     * See replaceSegments for recovery logic, in case the broker dies in the middle of this operation.
     * <br/>
     * Note that this method assumes we have already determined that the segment passed in contains records that cause
     * offset overflow.
     * <br/>
     * The split logic overloads the use of .clean files that LogCleaner typically uses to make the process of replacing
     * the input segment with multiple new segments atomic and recoverable in the event of a crash. See replaceSegments
     * and completeSwapOperations for the implementation to make this operation recoverable on crashes.</p>
     *
     * @param segment Segment to split
     * @param existingSegments The existing segments of the log
     * @param dir The directory in which the log will reside
     * @param topicPartition The topic
     * @param config The log configuration settings
     * @param scheduler The thread pool scheduler used for background actions
     * @param logDirFailureChannel The LogDirFailureChannel to asynchronously handle log dir failure
     * @param logPrefix The logging prefix
     * @return List of new segments that replace the input segment
     */
    public static SplitSegmentResult splitOverflowedSegment(LogSegment segment,
                                                     LogSegments existingSegments,
                                                     File dir,
                                                     TopicPartition topicPartition,
                                                     LogConfig config,
                                                     Scheduler scheduler,
                                                     LogDirFailureChannel logDirFailureChannel,
                                                     String logPrefix) throws IOException {
        require(isLogFile(segment.log().file()), "Cannot split file " + segment.log().file().getAbsoluteFile());
        require(segment.hasOverflow(), "Split operation is only permitted for segments with overflow, and the problem path is " + segment.log().file().getAbsoluteFile());

        LOG.info("{}Splitting overflowed segment {}", logPrefix, segment);

        List<LogSegment> newSegments = new ArrayList<>();
        try {
            int position = 0;
            FileRecords sourceRecords = segment.log();
            while (position < sourceRecords.sizeInBytes()) {
                FileLogInputStream.FileChannelRecordBatch firstBatch = sourceRecords.batchesFrom(position).iterator().next();
                LogSegment newSegment = createNewCleanedSegment(dir, config, firstBatch.baseOffset());
                newSegments.add(newSegment);
                int bytesAppended = newSegment.appendFromFile(sourceRecords, position);
                if (bytesAppended == 0) {
                    throw new IllegalStateException("Failed to append records from position " + position + " in " + segment);
                }
                position += bytesAppended;
            }
            // prepare new segments
            int totalSizeOfNewSegments = 0;
            for (LogSegment splitSegment : newSegments) {
                splitSegment.onBecomeInactiveSegment();
                splitSegment.flush();
                splitSegment.setLastModified(segment.lastModified());
                totalSizeOfNewSegments += splitSegment.log().sizeInBytes();
            }
            // size of all the new segments combined must equal size of the original segment
            if (totalSizeOfNewSegments != segment.log().sizeInBytes()) {
                throw new IllegalStateException("Inconsistent segment sizes after split before: " + segment.log().sizeInBytes() + " after: " + totalSizeOfNewSegments);
            }
            // replace old segment with new ones
            LOG.info("{}Replacing overflowed segment $segment with split segments {}", logPrefix, newSegments);
            List<LogSegment> deletedSegments = replaceSegments(existingSegments, newSegments, singletonList(segment),
                    dir, topicPartition, config, scheduler, logDirFailureChannel, logPrefix, false);
            return new SplitSegmentResult(deletedSegments, newSegments);
        } catch (Exception e) {
            for (LogSegment splitSegment : newSegments) {
                splitSegment.close();
                splitSegment.deleteIfExists();
            }
            throw e;
        }
    }

    /**
     * Swap one or more new segment in place and delete one or more existing segments in a crash-safe
     * manner. The old segments will be asynchronously deleted.
     * <br/>
     * This method does not need to convert IOException to KafkaStorageException because it is either
     * called before all logs are loaded or the caller will catch and handle IOException
     * <br/>
     * The sequence of operations is:
     * <ol>
     * <li>Cleaner creates one or more new segments with suffix .cleaned and invokes replaceSegments() on
     *   the Log instance. If broker crashes at this point, the clean-and-swap operation is aborted and
     *   the .cleaned files are deleted on recovery in LogLoader.
     * <li>New segments are renamed .swap. If the broker crashes before all segments were renamed to .swap, the
     *   clean-and-swap operation is aborted - .cleaned as well as .swap files are deleted on recovery in
     *   LogLoader. We detect this situation by maintaining a specific order in which files are renamed
     *   from .cleaned to .swap. Basically, files are renamed in descending order of offsets. On recovery,
     *   all .swap files whose offset is greater than the minimum-offset .clean file are deleted.
     * <li>If the broker crashes after all new segments were renamed to .swap, the operation is completed,
     *   the swap operation is resumed on recovery as described in the next step.
     * <li>Old segment files are renamed to .deleted and asynchronous delete is scheduled. If the broker
     *   crashes, any .deleted files left behind are deleted on recovery in LogLoader.
     *   replaceSegments() is then invoked to complete the swap with newSegment recreated from the
     *   .swap file and oldSegments containing segments which were not renamed before the crash.
     * <li>Swap segment(s) are renamed to replace the existing segments, completing this operation.
     *   If the broker crashes, any .deleted files which may be left behind are deleted
     *   on recovery in LogLoader.
     * </ol>
     *
     * @param existingSegments The existing segments of the log
     * @param newSegments The new log segment to add to the log
     * @param oldSegments The old log segments to delete from the log
     * @param dir The directory in which the log will reside
     * @param topicPartition The topic
     * @param config The log configuration settings
     * @param scheduler The thread pool scheduler used for background actions
     * @param logDirFailureChannel The LogDirFailureChannel to asynchronously handle log dir failure
     * @param logPrefix The logging prefix
     * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
     */
    public static List<LogSegment> replaceSegments(LogSegments existingSegments,
                                                   List<LogSegment> newSegments,
                                                   List<LogSegment> oldSegments,
                                                   File dir,
                                                   TopicPartition topicPartition,
                                                   LogConfig config,
                                                   Scheduler scheduler,
                                                   LogDirFailureChannel logDirFailureChannel,
                                                   String logPrefix,
                                                   boolean isRecoveredSwapFile) throws IOException {
        List<LogSegment> sortedNewSegments = new ArrayList<>(newSegments);
        sortedNewSegments.sort(Comparator.comparingLong(LogSegment::baseOffset));
        // Some old segments may have been removed from index and scheduled for async deletion after the caller reads segments
        // but before this method is executed. We want to filter out those segments to avoid calling deleteSegmentFiles()
        // multiple times for the same segment.
        List<LogSegment> sortedOldSegments = oldSegments.stream()
                .filter(seg -> existingSegments.contains(seg.baseOffset()))
                .sorted(Comparator.comparingLong(LogSegment::baseOffset))
                .collect(Collectors.toList());

        // need to do this in two phases to be crash safe AND do the deletion asynchronously
        // if we crash in the middle of this we complete the swap in loadSegments()
        List<LogSegment> reversedSegmentsList = new ArrayList<>(sortedNewSegments);
        Collections.reverse(reversedSegmentsList);

        for (LogSegment segment : reversedSegmentsList) {
            if (!isRecoveredSwapFile) {
                segment.changeFileSuffixes(CLEANED_FILE_SUFFIX, SWAP_FILE_SUFFIX);
            }
            existingSegments.add(segment);
        }
        Set<Long> newSegmentBaseOffsets = sortedNewSegments.stream().map(LogSegment::baseOffset).collect(Collectors.toSet());

        // delete the old files
        List<LogSegment> deletedNotReplaced = new ArrayList<>();
        for (LogSegment segment : sortedOldSegments) {
            // remove the index entry
            if (segment.baseOffset() != sortedNewSegments.get(0).baseOffset()) {
                existingSegments.remove(segment.baseOffset());
            }
            deleteSegmentFiles(
                    singletonList(segment),
                    true,
                    dir,
                    topicPartition,
                    config,
                    scheduler,
                    logDirFailureChannel,
                    logPrefix);
            if (!newSegmentBaseOffsets.contains(segment.baseOffset())) {
                deletedNotReplaced.add(segment);
            }
        }

        // okay we are safe now, remove the swap suffix
        for (LogSegment logSegment : sortedNewSegments) {
            logSegment.changeFileSuffixes(SWAP_FILE_SUFFIX, "");
        }
        Utils.flushDir(dir.toPath());
        return deletedNotReplaced;
    }

    public static class SplitSegmentResult {

        public final List<LogSegment> deletedSegments;
        public final List<LogSegment> newSegments;

        /**
         * Holds the result of splitting a segment into one or more segments, see LocalLog.splitOverflowedSegment().
         *
         * @param deletedSegments segments deleted when splitting a segment
         * @param newSegments new segments created when splitting a segment
         */
        public SplitSegmentResult(List<LogSegment> deletedSegments, List<LogSegment> newSegments) {
            this.deletedSegments = deletedSegments;
            this.newSegments = newSegments;
        }
    }
}
