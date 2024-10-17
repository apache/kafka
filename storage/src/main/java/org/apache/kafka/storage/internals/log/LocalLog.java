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
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.require;
import static org.apache.kafka.storage.internals.log.LogFileUtils.CLEANED_FILE_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.DELETED_FILE_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.SWAP_FILE_SUFFIX;
import static org.apache.kafka.storage.internals.log.LogFileUtils.isLogFile;

public class LocalLog {

    private static final Logger LOG = LoggerFactory.getLogger(LocalLog.class);

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
                if (bytesAppended == 0)
                    throw new IllegalStateException("Failed to append records from position " + position + " in " + segment);

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
            List<LogSegment> deletedSegments = replaceSegments(existingSegments, newSegments, Collections.singletonList(segment),
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

        // need to do this in two phases to be crash safe AND do the delete asynchronously
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
                    Collections.singletonList(segment),
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
