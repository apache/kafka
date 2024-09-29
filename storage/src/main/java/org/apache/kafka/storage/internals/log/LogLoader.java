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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidOffsetException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;

import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class LogLoader {

    private static final String SNAPSHOT_DELETE_SUFFIX = ".checkpoint.deleted";

    private final File dir;
    private final TopicPartition topicPartition;
    private final LogConfig config;
    private final Scheduler scheduler;
    private final Time time;
    private final LogDirFailureChannel logDirFailureChannel;
    private final boolean hadCleanShutdown;
    private final LogSegments segments;
    private final long logStartOffsetCheckpoint;
    private final long recoveryPointCheckpoint;
    private final Optional<LeaderEpochFileCache> leaderEpochCache;
    private final ProducerStateManager producerStateManager;
    private final ConcurrentMap<String, Integer> numRemainingSegments;
    private final boolean isRemoteLogEnabled;
    private final Logger logger;
    private final String logPrefix;

    /**
     * @param dir The directory from which log segments need to be loaded
     * @param topicPartition The topic partition associated with the log being loaded
     * @param config The configuration settings for the log being loaded
     * @param scheduler The thread pool scheduler used for background actions
     * @param time The time instance used for checking the clock
     * @param logDirFailureChannel The {@link LogDirFailureChannel} instance to asynchronously handle log directory failure
     * @param hadCleanShutdown Boolean flag to indicate whether the associated log previously had a clean shutdown
     * @param segments The {@link LogSegments} instance into which segments recovered from disk will be populated
     * @param logStartOffsetCheckpoint The checkpoint of the log start offset
     * @param recoveryPointCheckpoint The checkpoint of the offset at which to begin the recovery
     * @param leaderEpochCache An optional {@link LeaderEpochFileCache} instance to be updated during recovery
     * @param producerStateManager The {@link ProducerStateManager} instance to be updated during recovery
     * @param numRemainingSegments The remaining segments to be recovered in this log keyed by recovery thread name
     * @param isRemoteLogEnabled Boolean flag to indicate whether the remote storage is enabled or not
     */
    public LogLoader(
            File dir,
            TopicPartition topicPartition,
            LogConfig config,
            Scheduler scheduler,
            Time time,
            LogDirFailureChannel logDirFailureChannel,
            boolean hadCleanShutdown,
            LogSegments segments,
            long logStartOffsetCheckpoint,
            long recoveryPointCheckpoint,
            Optional<LeaderEpochFileCache> leaderEpochCache,
            ProducerStateManager producerStateManager,
            ConcurrentMap<String, Integer> numRemainingSegments,
            boolean isRemoteLogEnabled) {
        this.dir = dir;
        this.topicPartition = topicPartition;
        this.config = config;
        this.scheduler = scheduler;
        this.time = time;
        this.logDirFailureChannel = logDirFailureChannel;
        this.hadCleanShutdown = hadCleanShutdown;
        this.segments = segments;
        this.logStartOffsetCheckpoint = logStartOffsetCheckpoint;
        this.recoveryPointCheckpoint = recoveryPointCheckpoint;
        this.leaderEpochCache = leaderEpochCache;
        this.producerStateManager = producerStateManager;
        this.numRemainingSegments = numRemainingSegments;
        this.isRemoteLogEnabled = isRemoteLogEnabled;
        this.logPrefix = "[LogLoader partition=" + topicPartition + ", dir=" + dir.getParent() + "] ";
        this.logger = new LogContext(logPrefix).logger(LogLoader.class);
    }

    /**
     * Load the log segments from the log files on disk, and returns the components of the loaded log.
     * Additionally, it also suitably updates the provided {@link LeaderEpochFileCache} and
     * {@link ProducerStateManager} to reflect the contents of the loaded log.
     * <br/>
     * In the context of the calling thread, this function does not need to convert {@link IOException} to
     * {@link KafkaStorageException} because it is only called before all logs are loaded.
     *
     * @return the offsets of the Log successfully loaded from disk
     *
     * @throws LogSegmentOffsetOverflowException if we encounter a .swap file with messages that
     *                                           overflow index offset
     */
    public LoadedLogOffsets load() throws IOException {
        // First pass: through the files in the log directory and remove any temporary files
        // and find any interrupted swap operations
        Set<File> swapFiles = removeTempFilesAndCollectSwapFiles();

        // The remaining valid swap files must come from compaction or segment split operation. We can
        // simply rename them to regular segment files. But, before renaming, we should figure out which
        // segments are compacted/split and delete these segment files: this is done by calculating
        // min/maxSwapFileOffset.
        // We store segments that require renaming in this code block, and do the actual renaming later.
        long minSwapFileOffset = Long.MAX_VALUE;
        long maxSwapFileOffset = Long.MIN_VALUE;
        for (File swapFile : swapFiles) {
            if (!LogFileUtils.isLogFile(new File(Utils.replaceSuffix(swapFile.getPath(), LogFileUtils.SWAP_FILE_SUFFIX, "")))) {
                continue;
            }
            long baseOffset = LogFileUtils.offsetFromFile(swapFile);
            LogSegment segment = LogSegment.open(swapFile.getParentFile(),
                    baseOffset,
                    config,
                    time,
                    false,
                    0,
                    false,
                    LogFileUtils.SWAP_FILE_SUFFIX);
            logger.info("Found log file {} from interrupted swap operation, which is recoverable from {} files by renaming.", swapFile.getPath(), LogFileUtils.SWAP_FILE_SUFFIX);
            minSwapFileOffset = Math.min(segment.baseOffset(), minSwapFileOffset);
            maxSwapFileOffset = Math.max(segment.readNextOffset(), maxSwapFileOffset);
        }

        // Second pass: delete segments that are between minSwapFileOffset and maxSwapFileOffset. As
        // discussed above, these segments were compacted or split but haven't been renamed to .delete
        // before shutting down the broker.
        File[] files = dir.listFiles();
        if (files == null) files = new File[0];
        for (File file : files) {
            if (!file.isFile()) {
                continue;
            }
            try {
                if (!file.getName().endsWith(LogFileUtils.SWAP_FILE_SUFFIX)) {
                    long offset = LogFileUtils.offsetFromFile(file);
                    if (offset >= minSwapFileOffset && offset < maxSwapFileOffset) {
                        logger.info("Deleting segment files {} that is compacted but has not been deleted yet.", file.getName());
                        boolean ignore = file.delete();
                    }
                }
            } catch (StringIndexOutOfBoundsException | NumberFormatException e) {
                // ignore offsetFromFile with files that do not include an offset in the file name
            }
        }

        // Third pass: rename all swap files.
        files = dir.listFiles();
        if (files == null) files = new File[0];
        for (File file : files) {
            if (!file.isFile()) {
                continue;
            }
            if (file.getName().endsWith(LogFileUtils.SWAP_FILE_SUFFIX)) {
                logger.info("Recovering file {} by renaming from {} files.", file.getName(), LogFileUtils.SWAP_FILE_SUFFIX);
                boolean ignore = file.renameTo(new File(Utils.replaceSuffix(file.getPath(), LogFileUtils.SWAP_FILE_SUFFIX, "")));
            }
        }

        // Fourth pass: load all the log and index files.
        // We might encounter legacy log segments with offset overflow (KAFKA-6264). We need to split such segments. When
        // this happens, restart loading segment files from scratch.
        retryOnOffsetOverflow(() -> {
            // In case we encounter a segment with offset overflow, the retry logic will split it after which we need to retry
            // loading of segments. In that case, we also need to close all segments that could have been left open in previous
            // call to loadSegmentFiles().
            segments.close();
            segments.clear();
            loadSegmentFiles();
            return null;
        });

        RecoveryOffsets recoveryOffsets;
        if (!dir.getAbsolutePath().endsWith(LogFileUtils.DELETE_DIR_SUFFIX)) {
            recoveryOffsets = retryOnOffsetOverflow(this::recoverLog);
            // reset the index size of the currently active log segment to allow more entries
            segments.lastSegment().get().resizeIndexes(config.maxIndexSize);
        } else {
            if (segments.isEmpty()) {
                segments.add(LogSegment.open(dir, 0, config, time, config.initFileSize(), false));
            }
            recoveryOffsets = new RecoveryOffsets(0L, 0L);
        }

        leaderEpochCache.ifPresent(lec -> lec.truncateFromEndAsyncFlush(recoveryOffsets.nextOffset));
        long newLogStartOffset = isRemoteLogEnabled
            ? logStartOffsetCheckpoint
            : Math.max(logStartOffsetCheckpoint, segments.firstSegment().get().baseOffset());

        // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
        leaderEpochCache.ifPresent(lec -> lec.truncateFromStartAsyncFlush(logStartOffsetCheckpoint));

        // Any segment loading or recovery code must not use producerStateManager, so that we can build the full state here
        // from scratch.
        if (!producerStateManager.isEmpty()) {
            throw new IllegalStateException("Producer state must be empty during log initialization");
        }

        // Reload all snapshots into the ProducerStateManager cache, the intermediate ProducerStateManager used
        // during log recovery may have deleted some files without the LogLoader.producerStateManager instance witnessing the
        // deletion.
        producerStateManager.removeStraySnapshots(segments.baseOffsets());
        UnifiedLog.rebuildProducerState(
                producerStateManager,
                segments,
                newLogStartOffset,
                recoveryOffsets.nextOffset,
                config.recordVersion(),
                time,
                hadCleanShutdown,
                logPrefix);
        LogSegment activeSegment = segments.lastSegment().get();
        return new LoadedLogOffsets(
                newLogStartOffset,
                recoveryOffsets.newRecoveryPoint,
                new LogOffsetMetadata(recoveryOffsets.nextOffset, activeSegment.baseOffset(), activeSegment.size()));
    }

    /**
     * Removes any temporary files found in log directory, and creates a list of all .swap files which could be swapped
     * in place of existing segment(s). For log splitting, we know that any .swap file whose base offset is higher than
     * the smallest offset .clean file could be part of an incomplete split operation. Such .swap files are also deleted
     * by this method.
     *
     * @return Set of .swap files that are valid to be swapped in as segment files and index files
     */
    private Set<File> removeTempFilesAndCollectSwapFiles() throws IOException {
        Set<File> swapFiles = new HashSet<>();
        Set<File> cleanedFiles = new HashSet<>();
        long minCleanedFileOffset = Long.MAX_VALUE;

        File[] files = dir.listFiles();
        if (files == null) files = new File[0];
        for (File file : files) {
            if (!file.isFile()) {
                continue;
            }
            if (!file.canRead()) {
                throw new IOException("Could not read file " + file);
            }
            String filename = file.getName();

            // Delete stray files marked for deletion, but skip KRaft snapshots.
            // These are handled in the recovery logic in `KafkaMetadataLog`.
            if (filename.endsWith(LogFileUtils.DELETED_FILE_SUFFIX) && !filename.endsWith(SNAPSHOT_DELETE_SUFFIX)) {
                logger.debug("Deleting stray temporary file {}", file.getAbsolutePath());
                Files.deleteIfExists(file.toPath());
            } else if (filename.endsWith(LogFileUtils.CLEANED_FILE_SUFFIX)) {
                minCleanedFileOffset = Math.min(LogFileUtils.offsetFromFile(file), minCleanedFileOffset);
                cleanedFiles.add(file);
            } else if (filename.endsWith(LogFileUtils.SWAP_FILE_SUFFIX)) {
                swapFiles.add(file);
            }
        }

        // KAFKA-6264: Delete all .swap files whose base offset is greater than the minimum .cleaned segment offset. Such .swap
        // files could be part of an incomplete split operation that could not complete. See LocalLog#splitOverflowedSegment
        // for more details about the split operation.
        Set<File> invalidSwapFiles = new HashSet<>();
        Set<File> validSwapFiles = new HashSet<>();
        for (File file : swapFiles) {
            if (LogFileUtils.offsetFromFile(file) >= minCleanedFileOffset) {
                invalidSwapFiles.add(file);
            } else {
                validSwapFiles.add(file);
            }
        }
        for (File file : invalidSwapFiles) {
            logger.debug("Deleting invalid swap file {} minCleanedFileOffset: {}", file.getAbsoluteFile(), minCleanedFileOffset);
            Files.deleteIfExists(file.toPath());
        }

        // Now that we have deleted all .swap files that constitute an incomplete split operation, let's delete all .clean files
        for (File file : cleanedFiles) {
            logger.debug("Deleting stray .clean file {}", file.getAbsolutePath());
            Files.deleteIfExists(file.toPath());
        }

        return validSwapFiles;
    }

    /**
     * Retries the provided function only whenever an LogSegmentOffsetOverflowException is raised by
     * it during execution. Before every retry, the overflowed segment is split into one or more segments
     * such that there is no offset overflow in any of them.
     *
     * @param function The function to be executed
     * @return The value returned by the function, if successful
     * @throws IllegalStateException whenever the executed function throws any exception other than
     *                   LogSegmentOffsetOverflowException, the same exception is raised to the caller
     */
    private <T> T retryOnOffsetOverflow(StorageAction<T, IOException> function) throws IOException {
        while (true) {
            try {
                return function.execute();
            } catch (LogSegmentOffsetOverflowException lsooe) {
                logger.info("Caught segment overflow error: {}. Split segment and retry.", lsooe.getMessage());
                LocalLog.SplitSegmentResult result = LocalLog.splitOverflowedSegment(
                        lsooe.segment,
                        segments,
                        dir,
                        topicPartition,
                        config,
                        scheduler,
                        logDirFailureChannel,
                        logPrefix);
                deleteProducerSnapshotsAsync(result.deletedSegments);
            }
        }
    }

    /**
     * Loads segments from disk.
     * <br/>
     * This method does not need to convert {@link IOException} to {@link KafkaStorageException} because it is only
     * called before all logs are loaded. It is possible that we encounter a segment with index offset overflow in
     * which case the {@link LogSegmentOffsetOverflowException} will be thrown. Note that any segments that were opened
     * before we encountered the exception will remain open and the caller is responsible for closing them
     * appropriately, if needed.
     *
     * @throws LogSegmentOffsetOverflowException if the log directory contains a segment with messages that overflow the index offset
     */
    private void loadSegmentFiles() throws IOException {
        // load segments in ascending order because transactional data from one segment may depend on the
        // segments that come before it
        File[] files = dir.listFiles();
        if (files == null) files = new File[0];
        List<File> sortedFiles = Arrays.stream(files).filter(File::isFile).sorted().collect(Collectors.toList());
        for (File file : sortedFiles) {
            if (LogFileUtils.isIndexFile(file)) {
                // if it is an index file, make sure it has a corresponding .log file
                long offset = LogFileUtils.offsetFromFile(file);
                File logFile = LogFileUtils.logFile(dir, offset);
                if (!logFile.exists()) {
                    logger.warn("Found an orphaned index file {}, with no corresponding log file.", file.getAbsolutePath());
                    Files.deleteIfExists(file.toPath());
                }
            } else if (LogFileUtils.isLogFile(file)) {
                // if it's a log file, load the corresponding log segment
                long baseOffset = LogFileUtils.offsetFromFile(file);
                boolean timeIndexFileNewlyCreated = !LogFileUtils.timeIndexFile(dir, baseOffset).exists();
                LogSegment segment = LogSegment.open(dir, baseOffset, config, time, true, 0, false, "");
                try {
                    segment.sanityCheck(timeIndexFileNewlyCreated);
                } catch (NoSuchFileException nsfe) {
                    if (hadCleanShutdown || segment.baseOffset() < recoveryPointCheckpoint) {
                        logger.error("Could not find offset index file corresponding to log file {}, recovering segment and rebuilding index files...", segment.log().file().getAbsolutePath());
                    }
                    recoverSegment(segment);
                } catch (CorruptIndexException cie) {
                    logger.warn("Found a corrupted index file corresponding to log file {} due to {}, recovering segment and rebuilding index files...", segment.log().file().getAbsolutePath(), cie.getMessage());
                    recoverSegment(segment);
                }
                segments.add(segment);
            }
        }
    }

    /**
     * Just recovers the given segment.
     *
     * @param segment The {@link LogSegment} to recover
     * @return The number of bytes truncated from the segment
     * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index offset overflow
     */
    private int recoverSegment(LogSegment segment) throws IOException {
        ProducerStateManager producerStateManager = new ProducerStateManager(
                topicPartition,
                dir,
                this.producerStateManager.maxTransactionTimeoutMs(),
                this.producerStateManager.producerStateManagerConfig(),
                time);
        UnifiedLog.rebuildProducerState(
                producerStateManager,
                segments,
                logStartOffsetCheckpoint,
                segment.baseOffset(),
                config.recordVersion(),
                time,
                false,
                logPrefix);
        int bytesTruncated = segment.recover(producerStateManager, leaderEpochCache);
        // once we have recovered the segment's data, take a snapshot to ensure that we won't
        // need to reload the same segment again while recovering another segment.
        producerStateManager.takeSnapshot();
        return bytesTruncated;
    }

    /** return the log end offset if valid */
    private Optional<Long> deleteSegmentsIfLogStartGreaterThanLogEnd() throws IOException {
        if (segments.nonEmpty()) {
            long logEndOffset = segments.lastSegment().get().readNextOffset();
            if (logEndOffset >= logStartOffsetCheckpoint) {
                return Optional.of(logEndOffset);
            } else {
                logger.warn("Deleting all segments because logEndOffset ({}) " +
                        "is smaller than logStartOffset {}. " +
                        "This could happen if segment files were deleted from the file system.", logEndOffset, logStartOffsetCheckpoint);
                removeAndDeleteSegmentsAsync(segments.values());
                leaderEpochCache.ifPresent(LeaderEpochFileCache::clearAndFlush);
                producerStateManager.truncateFullyAndStartAt(logStartOffsetCheckpoint);
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    static class RecoveryOffsets {

        final long newRecoveryPoint;
        final long nextOffset;

        RecoveryOffsets(long newRecoveryPoint, long nextOffset) {
            this.newRecoveryPoint = newRecoveryPoint;
            this.nextOffset = nextOffset;
        }
    }

    /**
     * Recover the log segments (if there was an unclean shutdown). Ensures there is at least one
     * active segment, and returns the updated recovery point and next offset after recovery. Along
     * the way, the method suitably updates the {@link LeaderEpochFileCache} or {@link ProducerStateManager}
     * inside the provided LogComponents.
     * <br/>
     * This method does not need to convert {@link IOException} to {@link KafkaStorageException} because it is only
     * called before all logs are loaded.
     *
     * @return a {@link RecoveryOffsets} instance containing newRecoveryPoint and nextOffset.
     * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset overflow
     */
    RecoveryOffsets recoverLog() throws IOException {
        // If we have the clean shutdown marker, skip recovery.
        if (!hadCleanShutdown) {
            Collection<LogSegment> unflushed = segments.values(recoveryPointCheckpoint, Long.MAX_VALUE);
            int numUnflushed = unflushed.size();
            Iterator<LogSegment> unflushedIter = unflushed.iterator();
            boolean truncated = false;
            int numFlushed = 0;
            String threadName = Thread.currentThread().getName();
            numRemainingSegments.put(threadName, numUnflushed);

            while (unflushedIter.hasNext() && !truncated) {
                LogSegment segment = unflushedIter.next();
                logger.info("Recovering unflushed segment {}. {} recovered for {}.", segment.baseOffset(), numFlushed / numUnflushed, topicPartition);
                int truncatedBytes;
                try {
                    truncatedBytes = recoverSegment(segment);
                } catch (InvalidOffsetException | IOException ioe) {
                    long startOffset = segment.baseOffset();
                    logger.warn("Found invalid offset during recovery. Deleting the corrupt segment and creating an empty one with starting offset {}", startOffset);
                    truncatedBytes = segment.truncateTo(startOffset);
                }
                if (truncatedBytes > 0) {
                    // we had an invalid message, delete all remaining log
                    logger.warn("Corruption found in segment {}, truncating to offset {}", segment.baseOffset(), segment.readNextOffset());
                    Collection<LogSegment> unflushedRemaining = new ArrayList<>();
                    unflushedIter.forEachRemaining(unflushedRemaining::add);
                    removeAndDeleteSegmentsAsync(unflushedRemaining);
                    truncated = true;
                    // segment is truncated, so set remaining segments to 0
                    numRemainingSegments.put(threadName, 0);
                } else {
                    numFlushed += 1;
                    numRemainingSegments.put(threadName, numUnflushed - numFlushed);
                }
            }
        }

        Optional<Long> logEndOffsetOptional = deleteSegmentsIfLogStartGreaterThanLogEnd();
        if (segments.isEmpty()) {
            // no existing segments, create a new mutable segment beginning at logStartOffset
            segments.add(LogSegment.open(dir, logStartOffsetCheckpoint, config, time, config.initFileSize(), config.preallocate));
        }

        // Update the recovery point if there was a clean shutdown and did not perform any changes to
        // the segment. Otherwise, we just ensure that the recovery point is not ahead of the log end
        // offset. To ensure correctness and to make it easier to reason about, it's best to only advance
        // the recovery point when the log is flushed. If we advanced the recovery point here, we could
        // skip recovery for unflushed segments if the broker crashed after we checkpoint the recovery
        // point and before we flush the segment.
        if (hadCleanShutdown && logEndOffsetOptional.isPresent()) {
            return new RecoveryOffsets(logEndOffsetOptional.get(), logEndOffsetOptional.get());
        } else {
            long logEndOffset = logEndOffsetOptional.orElse(segments.lastSegment().get().readNextOffset());
            return new RecoveryOffsets(Math.min(recoveryPointCheckpoint, logEndOffset), logEndOffset);
        }
    }

    /**
     * This method deletes the given log segments and the associated producer snapshots, by doing the
     * following for each of them:
     * <ul>
     * <li>It removes the segment from the segment map so that it will no longer be used for reads.
     * <li>It schedules asynchronous deletion of the segments that allows reads to happen concurrently without
     *    synchronization and without the possibility of physically deleting a file while it is being
     *    read.
     * </ul>
     * This method does not need to convert {@link IOException} to {@link KafkaStorageException} because it is either
     * called before all logs are loaded or the immediate caller will catch and handle IOException
     *
     * @param segmentsToDelete The log segments to schedule for deletion
     */
    private void removeAndDeleteSegmentsAsync(Collection<LogSegment> segmentsToDelete) throws IOException {
        if (!segmentsToDelete.isEmpty()) {
            List<LogSegment> toDelete = new ArrayList<>(segmentsToDelete);
            logger.info("Deleting segments as part of log recovery: {}", toDelete.stream().map(LogSegment::toString).collect(Collectors.joining(", ")));
            toDelete.forEach(segment -> segments.remove(segment.baseOffset()));
            LocalLog.deleteSegmentFiles(
                    toDelete,
                    true,
                    dir,
                    topicPartition,
                    config,
                    scheduler,
                    logDirFailureChannel,
                    logPrefix);
            deleteProducerSnapshotsAsync(segmentsToDelete);
        }
    }

    private void deleteProducerSnapshotsAsync(Collection<LogSegment> segments) throws IOException {
        UnifiedLog.deleteProducerSnapshots(
                segments,
                producerStateManager,
                true,
                scheduler,
                config,
                logDirFailureChannel,
                dir.getParent(),
                topicPartition);
    }
}
