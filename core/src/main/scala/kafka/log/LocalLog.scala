/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import kafka.utils.Logging
import org.apache.kafka.common.errors.{KafkaStorageException, OffsetOutOfRangeException}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.{AbortedTxn, FetchDataInfo, LogConfig, LogDirFailureChannel, LogFileUtils, LogOffsetMetadata, LogSegment, LogSegments, OffsetPosition}

import java.io.{File, IOException}
import java.nio.file.Files
import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern
import java.util.{Collections, Optional}
import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, immutable}
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

/**
 * Holds the result of splitting a segment into one or more segments, see LocalLog.splitOverflowedSegment().
 *
 * @param deletedSegments segments deleted when splitting a segment
 * @param newSegments new segments created when splitting a segment
 */
case class SplitSegmentResult(deletedSegments: Iterable[LogSegment], newSegments: Iterable[LogSegment])

/**
 * An append-only log for storing messages locally. The log is a sequence of LogSegments, each with a base offset.
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * NOTE: this class is not thread-safe, and it relies on the thread safety provided by the Log class.
 *
 * @param _dir The directory in which log segments are created.
 * @param config The log configuration settings
 * @param segments The non-empty log segments recovered from disk
 * @param recoveryPoint The offset at which to begin the next recovery i.e. the first offset which has not been flushed to disk
 * @param nextOffsetMetadata The offset where the next message could be appended
 * @param scheduler The thread pool scheduler used for background actions
 * @param time The time instance used for checking the clock
 * @param topicPartition The topic partition associated with this log
 * @param logDirFailureChannel The LogDirFailureChannel instance to asynchronously handle Log dir failure
 */
class LocalLog(@volatile private var _dir: File,
               @volatile private[log] var config: LogConfig,
               private[log] val segments: LogSegments,
               @volatile private[log] var recoveryPoint: Long,
               @volatile private var nextOffsetMetadata: LogOffsetMetadata,
               private[log] val scheduler: Scheduler,
               private[log] val time: Time,
               private[log] val topicPartition: TopicPartition,
               private[log] val logDirFailureChannel: LogDirFailureChannel) extends Logging {

  import kafka.log.LocalLog._

  this.logIdent = s"[LocalLog partition=$topicPartition, dir=${dir.getParent}] "

  // The memory mapped buffer for index files of this log will be closed with either delete() or closeHandlers()
  // After memory mapped buffer is closed, no disk IO operation should be performed for this log.
  @volatile private[log] var isMemoryMappedBufferClosed = false

  // Cache value of parent directory to avoid allocations in hot paths like ReplicaManager.checkpointHighWatermarks
  @volatile private var _parentDir: String = dir.getParent

  // Last time the log was flushed
  private val lastFlushedTime = new AtomicLong(time.milliseconds)

  private[log] def dir: File = _dir

  private[log] def name: String = dir.getName

  private[log] def parentDir: String = _parentDir

  private[log] def parentDirFile: File = new File(_parentDir)

  private[log] def isFuture: Boolean = dir.getName.endsWith(LocalLog.FutureDirSuffix)

  private def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    LocalLog.maybeHandleIOException(logDirFailureChannel, parentDir, msg) {
      fun
    }
  }

  /**
   * Rename the directory of the log
   * @param name the new dir name
   * @throws KafkaStorageException if rename fails
   */
  private[log] def renameDir(name: String): Boolean = {
    maybeHandleIOException(s"Error while renaming dir for $topicPartition in log dir ${dir.getParent}") {
      val renamedDir = new File(dir.getParent, name)
      Utils.atomicMoveWithFallback(dir.toPath, renamedDir.toPath)
      if (renamedDir != dir) {
        _dir = renamedDir
        _parentDir = renamedDir.getParent
        segments.updateParentDir(renamedDir)
        true
      } else {
        false
      }
    }
  }

  /**
   * Update the existing configuration to the new provided configuration.
   * @param newConfig the new configuration to be updated to
   */
  private[log] def updateConfig(newConfig: LogConfig): Unit = {
    val oldConfig = config
    config = newConfig
    val oldRecordVersion = oldConfig.recordVersion
    val newRecordVersion = newConfig.recordVersion
    if (newRecordVersion.precedes(oldRecordVersion))
      warn(s"Record format version has been downgraded from $oldRecordVersion to $newRecordVersion.")
  }

  private[log] def checkIfMemoryMappedBufferClosed(): Unit = {
    if (isMemoryMappedBufferClosed)
      throw new KafkaStorageException(s"The memory mapped buffer for log of $topicPartition is already closed")
  }

  private[log] def updateRecoveryPoint(newRecoveryPoint: Long): Unit = {
    recoveryPoint = newRecoveryPoint
  }

  /**
   * Update recoveryPoint to provided offset and mark the log as flushed, if the offset is greater
   * than the existing recoveryPoint.
   *
   * @param offset the offset to be updated
   */
  private[log] def markFlushed(offset: Long): Unit = {
    checkIfMemoryMappedBufferClosed()
    if (offset > recoveryPoint) {
      updateRecoveryPoint(offset)
      lastFlushedTime.set(time.milliseconds)
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  private[log] def unflushedMessages: Long = logEndOffset - recoveryPoint

  /**
   * Flush local log segments for all offsets up to offset-1.
   * Does not update the recovery point.
   *
   * @param offset The offset to flush up to (non-inclusive)
   */
  private[log] def flush(offset: Long): Unit = {
    val currentRecoveryPoint = recoveryPoint
    if (currentRecoveryPoint <= offset) {
      val segmentsToFlush = segments.values(currentRecoveryPoint, offset)
      segmentsToFlush.forEach(_.flush())
      // If there are any new segments, we need to flush the parent directory for crash consistency.
      if (segmentsToFlush.stream().anyMatch(_.baseOffset >= currentRecoveryPoint)) {
        // The directory might be renamed concurrently for topic deletion, which may cause NoSuchFileException here.
        // Since the directory is to be deleted anyways, we just swallow NoSuchFileException and let it go.
        Utils.flushDirIfExists(dir.toPath)
      }
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  private[log] def lastFlushTime: Long = lastFlushedTime.get

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  private[log] def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   * The offset of the next message that will be appended to the log
   */
  private[log] def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Update end offset of the log, and update the recoveryPoint.
   *
   * @param endOffset the new end offset of the log
   */
  private[log] def updateLogEndOffset(endOffset: Long): Unit = {
    nextOffsetMetadata = new LogOffsetMetadata(endOffset, segments.activeSegment.baseOffset, segments.activeSegment.size)
    if (recoveryPoint > endOffset) {
      updateRecoveryPoint(endOffset)
    }
  }

  /**
   * Close file handlers used by log but don't write to disk.
   * This is called if the log directory is offline.
   */
  private[log] def closeHandlers(): Unit = {
    segments.closeHandlers()
    isMemoryMappedBufferClosed = true
  }

  /**
   * Closes the segments of the log.
   */
  private[log] def close(): Unit = {
    maybeHandleIOException(s"Error while renaming dir for $topicPartition in dir ${dir.getParent}") {
      checkIfMemoryMappedBufferClosed()
      segments.close()
    }
  }

  /**
   * Completely delete this log directory with no delay.
   */
  private[log] def deleteEmptyDir(): Unit = {
    maybeHandleIOException(s"Error while deleting dir for $topicPartition in dir ${dir.getParent}") {
      if (segments.nonEmpty) {
        throw new IllegalStateException(s"Can not delete directory when ${segments.numberOfSegments} segments are still present")
      }
      if (!isMemoryMappedBufferClosed) {
        throw new IllegalStateException(s"Can not delete directory when memory mapped buffer for log of $topicPartition is still open.")
      }
      Utils.delete(dir)
    }
  }

  /**
   * Completely delete all segments with no delay.
   * @return the deleted segments
   */
  private[log] def deleteAllSegments(): Iterable[LogSegment] = {
    maybeHandleIOException(s"Error while deleting all segments for $topicPartition in dir ${dir.getParent}") {
      val deletableSegments = new util.ArrayList(segments.values).asScala
      removeAndDeleteSegments(segments.values.asScala, asyncDelete = false, LogDeletion(this))
      isMemoryMappedBufferClosed = true
      deletableSegments
    }
  }

  /**
   * This method deletes the given log segments by doing the following for each of them:
   * - It removes the segment from the segment map so that it will no longer be used for reads.
   * - It renames the index and log files by appending .deleted to the respective file name
   * - It can either schedule an asynchronous delete operation to occur in the future or perform the deletion synchronously
   *
   * Asynchronous deletion allows reads to happen concurrently without synchronization and without the possibility of
   * physically deleting a file while it is being read.
   *
   * This method does not convert IOException to KafkaStorageException, the immediate caller
   * is expected to catch and handle IOException.
   *
   * @param segmentsToDelete The log segments to schedule for deletion
   * @param asyncDelete Whether the segment files should be deleted asynchronously
   * @param reason The reason for the segment deletion
   */
  private[log] def removeAndDeleteSegments(segmentsToDelete: Iterable[LogSegment],
                                           asyncDelete: Boolean,
                                           reason: SegmentDeletionReason): Unit = {
    if (segmentsToDelete.nonEmpty) {
      // Most callers hold an iterator into the `segments` collection and `removeAndDeleteSegment` mutates it by
      // removing the deleted segment, we should force materialization of the iterator here, so that results of the
      // iteration remain valid and deterministic. We should also pass only the materialized view of the
      // iterator to the logic that actually deletes the segments.
      val toDelete = segmentsToDelete.toList
      reason.logReason(toDelete)
      toDelete.foreach { segment =>
        segments.remove(segment.baseOffset)
      }
      LocalLog.deleteSegmentFiles(toDelete, asyncDelete, dir, topicPartition, config, scheduler, logDirFailureChannel, logIdent)
    }
  }

  /**
   * This method deletes the given segment and creates a new segment with the given new base offset. It ensures an
   * active segment exists in the log at all times during this process.
   *
   * Asynchronous deletion allows reads to happen concurrently without synchronization and without the possibility of
   * physically deleting a file while it is being read.
   *
   * This method does not convert IOException to KafkaStorageException, the immediate caller
   * is expected to catch and handle IOException.
   *
   * @param newOffset The base offset of the new segment
   * @param segmentToDelete The old active segment to schedule for deletion
   * @param asyncDelete Whether the segment files should be deleted asynchronously
   * @param reason The reason for the segment deletion
   */
  private[log] def createAndDeleteSegment(newOffset: Long,
                                          segmentToDelete: LogSegment,
                                          asyncDelete: Boolean,
                                          reason: SegmentDeletionReason): LogSegment = {
    if (newOffset == segmentToDelete.baseOffset)
      segmentToDelete.changeFileSuffixes("", LogFileUtils.DELETED_FILE_SUFFIX)

    val newSegment = LogSegment.open(dir,
      newOffset,
      config,
      time,
      config.initFileSize,
      config.preallocate)
    segments.add(newSegment)

    reason.logReason(List(segmentToDelete))
    if (newOffset != segmentToDelete.baseOffset)
      segments.remove(segmentToDelete.baseOffset)
    LocalLog.deleteSegmentFiles(List(segmentToDelete), asyncDelete, dir, topicPartition, config, scheduler, logDirFailureChannel, logIdent)

    newSegment
  }

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, throw an OffsetOutOfRangeException
   */
  private[log] def convertToOffsetMetadataOrThrow(offset: Long): LogOffsetMetadata = {
    val fetchDataInfo = read(offset,
      maxLength = 1,
      minOneMessage = false,
      maxOffsetMetadata = nextOffsetMetadata,
      includeAbortedTxns = false)
    fetchDataInfo.fetchOffsetMetadata
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
  def read(startOffset: Long,
           maxLength: Int,
           minOneMessage: Boolean,
           maxOffsetMetadata: LogOffsetMetadata,
           includeAbortedTxns: Boolean): FetchDataInfo = {
    maybeHandleIOException(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
      trace(s"Reading maximum $maxLength bytes at offset $startOffset from log with " +
        s"total length ${segments.sizeInBytes} bytes")

      val endOffsetMetadata = nextOffsetMetadata
      val endOffset = endOffsetMetadata.messageOffset
      var segmentOpt = segments.floorSegment(startOffset)

      // return error on attempt to read beyond the log end offset
      if (startOffset > endOffset || !segmentOpt.isPresent)
        throw new OffsetOutOfRangeException(s"Received request for offset $startOffset for partition $topicPartition, " +
          s"but we only have log segments upto $endOffset.")

      if (startOffset == maxOffsetMetadata.messageOffset)
        emptyFetchDataInfo(maxOffsetMetadata, includeAbortedTxns)
      else if (startOffset > maxOffsetMetadata.messageOffset)
        emptyFetchDataInfo(convertToOffsetMetadataOrThrow(startOffset), includeAbortedTxns)
      else {
        // Do the read on the segment with a base offset less than the target offset
        // but if that segment doesn't contain any messages with an offset greater than that
        // continue to read from successive segments until we get some messages or we reach the end of the log
        var fetchDataInfo: FetchDataInfo = null
        while (fetchDataInfo == null && segmentOpt.isPresent) {
          val segment = segmentOpt.get
          val baseOffset = segment.baseOffset

          val maxPosition =
          // Use the max offset position if it is on this segment; otherwise, the segment size is the limit.
            if (maxOffsetMetadata.segmentBaseOffset == segment.baseOffset) maxOffsetMetadata.relativePositionInSegment
            else segment.size

          fetchDataInfo = segment.read(startOffset, maxLength, maxPosition, minOneMessage)
          if (fetchDataInfo != null) {
            if (includeAbortedTxns)
              fetchDataInfo = addAbortedTransactions(startOffset, segment, fetchDataInfo)
          } else segmentOpt = segments.higherSegment(baseOffset)
        }

        if (fetchDataInfo != null) fetchDataInfo
        else {
          // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
          // this can happen when all messages with offset larger than start offsets have been deleted.
          // In this case, we will return the empty set with log end offset metadata
          new FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
        }
      }
    }
  }

  private[log] def append(lastOffset: Long, largestTimestamp: Long, shallowOffsetOfMaxTimestamp: Long, records: MemoryRecords): Unit = {
    segments.activeSegment.append(lastOffset, largestTimestamp, shallowOffsetOfMaxTimestamp, records)
    updateLogEndOffset(lastOffset + 1)
  }

  private def addAbortedTransactions(startOffset: Long, segment: LogSegment,
                                     fetchInfo: FetchDataInfo): FetchDataInfo = {
    val fetchSize = fetchInfo.records.sizeInBytes
    val startOffsetPosition = new OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
      fetchInfo.fetchOffsetMetadata.relativePositionInSegment)
    val upperBoundOffset = segment.fetchUpperBoundOffset(startOffsetPosition, fetchSize).orElse(
      segments.higherSegment(segment.baseOffset).asScala.map(s => s.baseOffset).getOrElse(logEndOffset))

    val abortedTransactions = ListBuffer.empty[FetchResponseData.AbortedTransaction]
    def accumulator(abortedTxns: Seq[AbortedTxn]): Unit = abortedTransactions ++= abortedTxns.map(_.asAbortedTransaction)
    collectAbortedTransactions(startOffset, upperBoundOffset, segment, accumulator)

    new FetchDataInfo(fetchInfo.fetchOffsetMetadata,
      fetchInfo.records,
      fetchInfo.firstEntryIncomplete,
      Optional.of(abortedTransactions.toList.asJava))
  }

  private def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long,
                                         startingSegment: LogSegment,
                                         accumulator: Seq[AbortedTxn] => Unit): Unit = {
    val higherSegments = segments.higherSegments(startingSegment.baseOffset).iterator
    var segmentEntryOpt = Option(startingSegment)
    while (segmentEntryOpt.isDefined) {
      val segment = segmentEntryOpt.get
      val searchResult = segment.collectAbortedTxns(startOffset, upperBoundOffset)
      accumulator(searchResult.abortedTransactions.asScala)
      if (searchResult.isComplete)
        return
      segmentEntryOpt = nextOption(higherSegments)
    }
  }

  private[log] def collectAbortedTransactions(logStartOffset: Long, baseOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    val segmentEntry = segments.floorSegment(baseOffset)
    val allAbortedTxns = ListBuffer.empty[AbortedTxn]
    def accumulator(abortedTxns: Seq[AbortedTxn]): Unit = allAbortedTxns ++= abortedTxns
    segmentEntry.ifPresent(segment => collectAbortedTransactions(logStartOffset, upperBoundOffset, segment, accumulator))
    allAbortedTxns.toList
  }

  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
   *
   * @param expectedNextOffset The expected next offset after the segment is rolled
   *
   * @return The newly rolled segment
   */
  private[log] def roll(expectedNextOffset: Option[Long] = None): LogSegment = {
    maybeHandleIOException(s"Error while rolling log segment for $topicPartition in dir ${dir.getParent}") {
      val start = time.hiResClockMs()
      checkIfMemoryMappedBufferClosed()
      val newOffset = math.max(expectedNextOffset.getOrElse(0L), logEndOffset)
      val logFile = LogFileUtils.logFile(dir, newOffset, "")
      val activeSegment = segments.activeSegment
      if (segments.contains(newOffset)) {
        // segment with the same base offset already exists and loaded
        if (activeSegment.baseOffset == newOffset && activeSegment.size == 0) {
          // We have seen this happen (see KAFKA-6388) after shouldRoll() returns true for an
          // active segment of size zero because of one of the indexes is "full" (due to _maxEntries == 0).
          warn(s"Trying to roll a new log segment with start offset $newOffset " +
            s"=max(provided offset = $expectedNextOffset, LEO = $logEndOffset) while it already " +
            s"exists and is active with size 0. Size of time index: ${activeSegment.timeIndex.entries}," +
            s" size of offset index: ${activeSegment.offsetIndex.entries}.")
          val newSegment = createAndDeleteSegment(newOffset, activeSegment, asyncDelete = true, LogRoll(this))
          updateLogEndOffset(nextOffsetMetadata.messageOffset)
          info(s"Rolled new log segment at offset $newOffset in ${time.hiResClockMs() - start} ms.")
          return newSegment
        } else {
          throw new KafkaException(s"Trying to roll a new log segment for topic partition $topicPartition with start offset $newOffset" +
            s" =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) while it already exists. Existing " +
            s"segment is ${segments.get(newOffset)}.")
        }
      } else if (segments.nonEmpty && newOffset < activeSegment.baseOffset) {
        throw new KafkaException(
          s"Trying to roll a new log segment for topic partition $topicPartition with " +
            s"start offset $newOffset =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) lower than start offset of the active segment $activeSegment")
      } else {
        val offsetIdxFile = LogFileUtils.offsetIndexFile(dir, newOffset)
        val timeIdxFile = LogFileUtils.timeIndexFile(dir, newOffset)
        val txnIdxFile = LogFileUtils.transactionIndexFile(dir, newOffset)

        for (file <- List(logFile, offsetIdxFile, timeIdxFile, txnIdxFile) if file.exists) {
          warn(s"Newly rolled segment file ${file.getAbsolutePath} already exists; deleting it first")
          Files.delete(file.toPath)
        }

        segments.lastSegment.ifPresent(_.onBecomeInactiveSegment())
      }

      val newSegment = LogSegment.open(dir,
        newOffset,
        config,
        time,
        config.initFileSize,
        config.preallocate)
      segments.add(newSegment)

      // We need to update the segment base offset and append position data of the metadata when log rolls.
      // The next offset should not change.
      updateLogEndOffset(nextOffsetMetadata.messageOffset)

      info(s"Rolled new log segment at offset $newOffset in ${time.hiResClockMs() - start} ms.")

      newSegment
    }
  }

  /**
   *  Delete all data in the local log and start at the new offset.
   *
   *  @param newOffset The new offset to start the log with
   *  @return the list of segments that were scheduled for deletion
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long): Iterable[LogSegment] = {
    maybeHandleIOException(s"Error while truncating the entire log for $topicPartition in dir ${dir.getParent}") {
      debug(s"Truncate and start at offset $newOffset")
      checkIfMemoryMappedBufferClosed()
      val segmentsToDelete = new util.ArrayList(segments.values).asScala

      if (segmentsToDelete.nonEmpty) {
        removeAndDeleteSegments(segmentsToDelete.dropRight(1), asyncDelete = true, LogTruncation(this))
        // Use createAndDeleteSegment() to create new segment first and then delete the old last segment to prevent missing
        // active segment during the deletion process
        createAndDeleteSegment(newOffset, segmentsToDelete.last, asyncDelete = true, LogTruncation(this))
      }

      updateLogEndOffset(newOffset)

      segmentsToDelete
    }
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   *
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   * @return the list of segments that were scheduled for deletion
   */
  private[log] def truncateTo(targetOffset: Long): Iterable[LogSegment] = {
    val deletableSegments = segments.filter(segment => segment.baseOffset > targetOffset).asScala
    removeAndDeleteSegments(deletableSegments, asyncDelete = true, LogTruncation(this))
    segments.activeSegment.truncateTo(targetOffset)
    updateLogEndOffset(targetOffset)
    deletableSegments
  }
}

/**
 * Helper functions for logs
 */
object LocalLog extends Logging {

  /** a file that is scheduled to be deleted */
  private[log] val DeletedFileSuffix = LogFileUtils.DELETED_FILE_SUFFIX

  /** A temporary file that is being used for log cleaning */
  private[log] val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  private[log] val SwapFileSuffix = ".swap"

  /** a directory that is scheduled to be deleted */
  private[log] val DeleteDirSuffix = "-delete"

  /** a directory that is used for future partition */
  private[log] val FutureDirSuffix = "-future"

  /** a directory that is used for stray partition */
  private[log] val StrayDirSuffix = "-stray"

  private[log] val DeleteDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$DeleteDirSuffix")
  private[log] val FutureDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$FutureDirSuffix")
  private[log] val StrayDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$StrayDirSuffix")

  private[log] val UnknownOffset = -1L

  /**
   * Return a directory name to rename the log directory to for async deletion.
   * The name will be in the following format: "topic-partitionId.uniqueId-delete".
   * If the topic name is too long, it will be truncated to prevent the total name
   * from exceeding 255 characters.
   */
  private[log] def logDeleteDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffixCappedLength(topicPartition, DeleteDirSuffix)
  }

  /**
   * Return a directory name to rename the log directory to for stray partition deletion.
   * The name will be in the following format: "topic-partitionId.uniqueId-stray".
   * If the topic name is too long, it will be truncated to prevent the total name
   * from exceeding 255 characters.
   */
  private[log] def logStrayDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffixCappedLength(topicPartition, StrayDirSuffix)
  }

  /**
   * Return a future directory name for the given topic partition. The name will be in the following
   * format: topic-partition.uniqueId-future where topic, partition and uniqueId are variables.
   */
  private[log] def logFutureDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffix(topicPartition, FutureDirSuffix)
  }

  /**
   * Return a new directory name in the following format: "${topic}-${partitionId}.${uniqueId}${suffix}".
   * If the topic name is too long, it will be truncated to prevent the total name
   * from exceeding 255 characters.
   */
  private[log] def logDirNameWithSuffixCappedLength(topicPartition: TopicPartition, suffix: String): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    val fullSuffix = s"-${topicPartition.partition()}.$uniqueId$suffix"
    val prefixLength = Math.min(topicPartition.topic().length, 255 - fullSuffix.length)
    s"${topicPartition.topic().substring(0, prefixLength)}$fullSuffix"
  }

  private[log] def logDirNameWithSuffix(topicPartition: TopicPartition, suffix: String): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    s"${logDirName(topicPartition)}.$uniqueId$suffix"
  }

  /**
   * Return a directory name for the given topic partition. The name will be in the following
   * format: topic-partition where topic, partition are variables.
   */
  private[log] def logDirName(topicPartition: TopicPartition): String = {
    s"${topicPartition.topic}-${topicPartition.partition}"
  }

  /**
   * Parse the topic and partition out of the directory name of a log
   */
  private[log] def parseTopicPartitionName(dir: File): TopicPartition = {
    if (dir == null)
      throw new KafkaException("dir should not be null")

    def exception(dir: File): KafkaException = {
      new KafkaException(s"Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
        "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
        "Kafka's log directories (and children) should only contain Kafka topic data.")
    }

    val dirName = dir.getName
    if (dirName == null || dirName.isEmpty || !dirName.contains('-'))
      throw exception(dir)
    if (dirName.endsWith(DeleteDirSuffix) && !DeleteDirPattern.matcher(dirName).matches ||
      dirName.endsWith(FutureDirSuffix) && !FutureDirPattern.matcher(dirName).matches ||
      dirName.endsWith(StrayDirSuffix) && !StrayDirPattern.matcher(dirName).matches)
      throw exception(dir)

    val name: String =
      if (dirName.endsWith(DeleteDirSuffix) || dirName.endsWith(FutureDirSuffix) || dirName.endsWith(StrayDirSuffix))
        dirName.substring(0, dirName.lastIndexOf('.'))
      else dirName

    val index = name.lastIndexOf('-')
    val topic = name.substring(0, index)
    val partitionString = name.substring(index + 1)
    if (topic.isEmpty || partitionString.isEmpty)
      throw exception(dir)

    val partition =
      try partitionString.toInt
      catch { case _: NumberFormatException => throw exception(dir) }

    new TopicPartition(topic, partition)
  }

  private[log] def isIndexFile(file: File): Boolean = {
    val fileName = file.getName
    fileName.endsWith(LogFileUtils.INDEX_FILE_SUFFIX) || fileName.endsWith(LogFileUtils.TIME_INDEX_FILE_SUFFIX) || fileName.endsWith(LogFileUtils.TXN_INDEX_FILE_SUFFIX)
  }

  private[log] def isLogFile(file: File): Boolean =
    file.getPath.endsWith(LogFileUtils.LOG_FILE_SUFFIX)

  /**
   * Invokes the provided function and handles any IOException raised by the function by marking the
   * provided directory offline.
   *
   * @param logDirFailureChannel Used to asynchronously handle log directory failure.
   * @param logDir The log directory to be marked offline during an IOException.
   * @param errorMsg The error message to be used when marking the log directory offline.
   * @param fun The function to be executed.
   * @return The value returned by the function after a successful invocation
   */
  private[log] def maybeHandleIOException[T](logDirFailureChannel: LogDirFailureChannel,
                                             logDir: String,
                                             errorMsg: => String)(fun: => T): T = {
    if (logDirFailureChannel.hasOfflineLogDir(logDir)) {
      throw new KafkaStorageException(s"The log dir $logDir is already offline due to a previous IO exception.")
    }
    try {
      fun
    } catch {
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(logDir, errorMsg, e)
        throw new KafkaStorageException(errorMsg, e)
    }
  }

  /**
   * Split a segment into one or more segments such that there is no offset overflow in any of them. The
   * resulting segments will contain the exact same messages that are present in the input segment. On successful
   * completion of this method, the input segment will be deleted and will be replaced by the resulting new segments.
   * See replaceSegments for recovery logic, in case the broker dies in the middle of this operation.
   *
   * Note that this method assumes we have already determined that the segment passed in contains records that cause
   * offset overflow.
   *
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
  private[log] def splitOverflowedSegment(segment: LogSegment,
                                          existingSegments: LogSegments,
                                          dir: File,
                                          topicPartition: TopicPartition,
                                          config: LogConfig,
                                          scheduler: Scheduler,
                                          logDirFailureChannel: LogDirFailureChannel,
                                          logPrefix: String): SplitSegmentResult = {
    require(isLogFile(segment.log.file), s"Cannot split file ${segment.log.file.getAbsoluteFile}")
    require(segment.hasOverflow, s"Split operation is only permitted for segments with overflow, and the problem path is ${segment.log.file.getAbsoluteFile}")

    info(s"${logPrefix}Splitting overflowed segment $segment")

    val newSegments = ListBuffer[LogSegment]()
    try {
      var position = 0
      val sourceRecords = segment.log

      while (position < sourceRecords.sizeInBytes) {
        val firstBatch = sourceRecords.batchesFrom(position).asScala.head
        val newSegment = createNewCleanedSegment(dir, config, firstBatch.baseOffset)
        newSegments += newSegment

        val bytesAppended = newSegment.appendFromFile(sourceRecords, position)
        if (bytesAppended == 0)
          throw new IllegalStateException(s"Failed to append records from position $position in $segment")

        position += bytesAppended
      }

      // prepare new segments
      var totalSizeOfNewSegments = 0
      newSegments.foreach { splitSegment =>
        splitSegment.onBecomeInactiveSegment()
        splitSegment.flush()
        splitSegment.setLastModified(segment.lastModified)
        totalSizeOfNewSegments += splitSegment.log.sizeInBytes
      }
      // size of all the new segments combined must equal size of the original segment
      if (totalSizeOfNewSegments != segment.log.sizeInBytes)
        throw new IllegalStateException("Inconsistent segment sizes after split" +
          s" before: ${segment.log.sizeInBytes} after: $totalSizeOfNewSegments")

      // replace old segment with new ones
      info(s"${logPrefix}Replacing overflowed segment $segment with split segments $newSegments")
      val newSegmentsToAdd = newSegments.toSeq
      val deletedSegments = LocalLog.replaceSegments(existingSegments, newSegmentsToAdd, List(segment),
        dir, topicPartition, config, scheduler, logDirFailureChannel, logPrefix)
      SplitSegmentResult(deletedSegments.toSeq, newSegmentsToAdd)
    } catch {
      case e: Exception =>
        newSegments.foreach { splitSegment =>
          splitSegment.close()
          splitSegment.deleteIfExists()
        }
        throw e
    }
  }

  /**
   * Swap one or more new segment in place and delete one or more existing segments in a crash-safe
   * manner. The old segments will be asynchronously deleted.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either
   * called before all logs are loaded or the caller will catch and handle IOException
   *
   * The sequence of operations is:
   *
   * - Cleaner creates one or more new segments with suffix .cleaned and invokes replaceSegments() on
   *   the Log instance. If broker crashes at this point, the clean-and-swap operation is aborted and
   *   the .cleaned files are deleted on recovery in LogLoader.
   * - New segments are renamed .swap. If the broker crashes before all segments were renamed to .swap, the
   *   clean-and-swap operation is aborted - .cleaned as well as .swap files are deleted on recovery in
   *   in LogLoader. We detect this situation by maintaining a specific order in which files are renamed
   *   from .cleaned to .swap. Basically, files are renamed in descending order of offsets. On recovery,
   *   all .swap files whose offset is greater than the minimum-offset .clean file are deleted.
   * - If the broker crashes after all new segments were renamed to .swap, the operation is completed,
   *   the swap operation is resumed on recovery as described in the next step.
   * - Old segment files are renamed to .deleted and asynchronous delete is scheduled. If the broker
   *   crashes, any .deleted files left behind are deleted on recovery in LogLoader.
   *   replaceSegments() is then invoked to complete the swap with newSegment recreated from the
   *   .swap file and oldSegments containing segments which were not renamed before the crash.
   * - Swap segment(s) are renamed to replace the existing segments, completing this operation.
   *   If the broker crashes, any .deleted files which may be left behind are deleted
   *   on recovery in LogLoader.
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
  private[log] def replaceSegments(existingSegments: LogSegments,
                                   newSegments: Seq[LogSegment],
                                   oldSegments: Seq[LogSegment],
                                   dir: File,
                                   topicPartition: TopicPartition,
                                   config: LogConfig,
                                   scheduler: Scheduler,
                                   logDirFailureChannel: LogDirFailureChannel,
                                   logPrefix: String,
                                   isRecoveredSwapFile: Boolean = false): Iterable[LogSegment] = {
    val sortedNewSegments = newSegments.sortBy(_.baseOffset)
    // Some old segments may have been removed from index and scheduled for async deletion after the caller reads segments
    // but before this method is executed. We want to filter out those segments to avoid calling deleteSegmentFiles()
    // multiple times for the same segment.
    val sortedOldSegments = oldSegments.filter(seg => existingSegments.contains(seg.baseOffset)).sortBy(_.baseOffset)

    // need to do this in two phases to be crash safe AND do the delete asynchronously
    // if we crash in the middle of this we complete the swap in loadSegments()
    if (!isRecoveredSwapFile)
      sortedNewSegments.reverse.foreach(_.changeFileSuffixes(CleanedFileSuffix, SwapFileSuffix))
    sortedNewSegments.reverse.foreach(existingSegments.add)
    val newSegmentBaseOffsets = sortedNewSegments.map(_.baseOffset).toSet

    // delete the old files
    val deletedNotReplaced = sortedOldSegments.map { seg =>
      // remove the index entry
      if (seg.baseOffset != sortedNewSegments.head.baseOffset)
        existingSegments.remove(seg.baseOffset)
      deleteSegmentFiles(
        List(seg),
        asyncDelete = true,
        dir,
        topicPartition,
        config,
        scheduler,
        logDirFailureChannel,
        logPrefix)
      if (newSegmentBaseOffsets.contains(seg.baseOffset)) Option.empty else Some(seg)
    }.filter(item => item.isDefined).map(item => item.get)

    // okay we are safe now, remove the swap suffix
    sortedNewSegments.foreach(_.changeFileSuffixes(SwapFileSuffix, ""))
    Utils.flushDir(dir.toPath)
    deletedNotReplaced
  }

  /**
   * Perform physical deletion of the index and log files for the given segment.
   * Prior to the deletion, the index and log files are renamed by appending .deleted to the
   * respective file name. Allows these files to be optionally deleted asynchronously.
   *
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
  private[log] def deleteSegmentFiles(segmentsToDelete: immutable.Iterable[LogSegment],
                                      asyncDelete: Boolean,
                                      dir: File,
                                      topicPartition: TopicPartition,
                                      config: LogConfig,
                                      scheduler: Scheduler,
                                      logDirFailureChannel: LogDirFailureChannel,
                                      logPrefix: String): Unit = {
    segmentsToDelete.foreach { segment =>
      if (!segment.hasSuffix(LogFileUtils.DELETED_FILE_SUFFIX))
        segment.changeFileSuffixes("", LogFileUtils.DELETED_FILE_SUFFIX)
    }

    def deleteSegments(): Unit = {
      info(s"${logPrefix}Deleting segment files ${segmentsToDelete.mkString(",")}")
      val parentDir = dir.getParent
      maybeHandleIOException(logDirFailureChannel, parentDir, s"Error while deleting segments for $topicPartition in dir $parentDir") {
        segmentsToDelete.foreach { segment =>
          segment.deleteIfExists()
        }
      }
    }

    if (asyncDelete)
      scheduler.scheduleOnce("delete-file", () => deleteSegments(), config.fileDeleteDelayMs)
    else
      deleteSegments()
  }

  private[log] def emptyFetchDataInfo(fetchOffsetMetadata: LogOffsetMetadata,
                                      includeAbortedTxns: Boolean): FetchDataInfo = {
    val abortedTransactions: Optional[java.util.List[FetchResponseData.AbortedTransaction]] =
      if (includeAbortedTxns) Optional.of(Collections.emptyList())
      else Optional.empty()
    new FetchDataInfo(fetchOffsetMetadata,
      MemoryRecords.EMPTY,
      false,
      abortedTransactions)
  }

  private[log] def createNewCleanedSegment(dir: File, logConfig: LogConfig, baseOffset: Long): LogSegment = {
    LogSegment.deleteIfExists(dir, baseOffset, CleanedFileSuffix)
    LogSegment.open(dir, baseOffset, logConfig, Time.SYSTEM, false, logConfig.initFileSize, logConfig.preallocate, CleanedFileSuffix)
  }

  /**
   * Wraps the value of iterator.next() in an option.
   * Note: this facility is a part of the Iterator class starting from scala v2.13.
   *
   * @param iterator
   * @tparam T the type of object held within the iterator
   * @return Some(iterator.next) if a next element exists, None otherwise.
   */
  private[log] def nextOption[T](iterator: util.Iterator[T]): Option[T] = {
    if (iterator.hasNext)
      Some(iterator.next())
    else
      None
  }
}

trait SegmentDeletionReason {
  def logReason(toDelete: List[LogSegment]): Unit
}

case class LogTruncation(log: LocalLog) extends SegmentDeletionReason {
  override def logReason(toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments as part of log truncation: ${toDelete.mkString(",")}")
  }
}

case class LogRoll(log: LocalLog) extends SegmentDeletionReason {
  override def logReason(toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments as part of log roll: ${toDelete.mkString(",")}")
  }
}

case class LogDeletion(log: LocalLog) extends SegmentDeletionReason {
  override def logReason(toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments as the log has been deleted: ${toDelete.mkString(",")}")
  }
}
