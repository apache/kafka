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

import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException}
import java.text.NumberFormat
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, TimeUnit}

import kafka.api.KAFKA_0_10_0_IV0
import kafka.common.{InvalidOffsetException, KafkaException, LongRef}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerTopicStats, FetchDataInfo, LogDirFailureChannel, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.errors.{CorruptRecordException, KafkaStorageException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{IsolationLevel, ListOffsetRequest}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Seq, Set, mutable}
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.utils.{Time, Utils}
import kafka.message.{BrokerCompressionCodec, CompressionCodec, NoCompressionCodec}
import kafka.server.checkpoints.{LeaderEpochCheckpointFile, LeaderEpochFile}
import kafka.server.epoch.{LeaderEpochCache, LeaderEpochFileCache}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import java.util.Map.{Entry => JEntry}
import java.lang.{Long => JLong}
import java.util.regex.Pattern

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, -1L,
    RecordsProcessingStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false)

  def unknownLogAppendInfoWithLogStartOffset(logStartOffset: Long): LogAppendInfo =
    LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordsProcessingStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false)
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 *
 * @param firstOffset The first offset in the message set unless the message format is less than V2 and we are appending
 *                    to the follower.
 * @param lastOffset The last offset in the message set
 * @param maxTimestamp The maximum timestamp of the message set.
 * @param offsetOfMaxTimestamp The offset of the message with the maximum timestamp.
 * @param logAppendTime The log append time (if used) of the message set, otherwise Message.NoTimestamp
 * @param logStartOffset The start offset of the log at the time of this append.
 * @param recordsProcessingStats Statistics collected during record processing, `null` if `assignOffsets` is `false`
 * @param sourceCodec The source codec used in the message set (send by the producer)
 * @param targetCodec The target codec of the message set(after applying the broker compression configuration if any)
 * @param shallowCount The number of shallow messages
 * @param validBytes The number of valid bytes
 * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
 */
case class LogAppendInfo(var firstOffset: Option[Long],
                         var lastOffset: Long,
                         var maxTimestamp: Long,
                         var offsetOfMaxTimestamp: Long,
                         var logAppendTime: Long,
                         var logStartOffset: Long,
                         var recordsProcessingStats: RecordsProcessingStats,
                         sourceCodec: CompressionCodec,
                         targetCodec: CompressionCodec,
                         shallowCount: Int,
                         validBytes: Int,
                         offsetsMonotonic: Boolean) {
  /**
    * Get the first offset if it exists, else get the last offset.
    * @return The offset of first message if it exists; else offset of the last message.
    */
  def firstOrLastOffset: Long = firstOffset.getOrElse(lastOffset)

  /**
    * Get the (maximum) number of messages described by LogAppendInfo
    * @return Maximum possible number of messages described by LogAppendInfo
    */
  def numMessages: Long = {
    firstOffset match {
      case Some(firstOffsetVal) if (firstOffsetVal >= 0 && lastOffset >= 0) => (lastOffset - firstOffsetVal + 1)
      case _ => 0
    }
  }
}

/**
 * A class used to hold useful metadata about a completed transaction. This is used to build
 * the transaction index after appending to the log.
 *
 * @param producerId The ID of the producer
 * @param firstOffset The first offset (inclusive) of the transaction
 * @param lastOffset The last offset (inclusive) of the transaction. This is always the offset of the
 *                   COMMIT/ABORT control record which indicates the transaction's completion.
 * @param isAborted Whether or not the transaction was aborted
 */
case class CompletedTxn(producerId: Long, firstOffset: Long, lastOffset: Long, isAborted: Boolean) {
  override def toString: String = {
    "CompletedTxn(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"isAborted=$isAborted)"
  }
}

/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * @param dir The directory in which log segments are created.
 * @param config The log configuration settings
 * @param logStartOffset The earliest offset allowed to be exposed to kafka client.
 *                       The logStartOffset can be updated by :
 *                       - user's DeleteRecordsRequest
 *                       - broker's log retention
 *                       - broker's log truncation
 *                       The logStartOffset is used to decide the following:
 *                       - Log deletion. LogSegment whose nextOffset <= log's logStartOffset can be deleted.
 *                         It may trigger log rolling if the active segment is deleted.
 *                       - Earliest offset of the log in response to ListOffsetRequest. To avoid OffsetOutOfRange exception after user seeks to earliest offset,
 *                         we make sure that logStartOffset <= log's highWatermark
 *                       Other activities such as log cleaning are not affected by logStartOffset.
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler The thread pool scheduler used for background actions
 * @param brokerTopicStats Container for Broker Topic Yammer Metrics
 * @param time The time instance used for checking the clock
 * @param maxProducerIdExpirationMs The maximum amount of time to wait before a producer id is considered expired
 * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
 */
@threadsafe
class Log(@volatile var dir: File,
          @volatile var config: LogConfig,
          @volatile var logStartOffset: Long,
          @volatile var recoveryPoint: Long,
          scheduler: Scheduler,
          brokerTopicStats: BrokerTopicStats,
          time: Time,
          val maxProducerIdExpirationMs: Int,
          val producerIdExpirationCheckIntervalMs: Int,
          val topicPartition: TopicPartition,
          val producerStateManager: ProducerStateManager,
          logDirFailureChannel: LogDirFailureChannel) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._

  this.logIdent = s"[Log partition=$topicPartition, dir=${dir.getParent}] "

  /* A lock that guards all modifications to the log */
  private val lock = new Object
  // The memory mapped buffer for index files of this log will be closed for index files of this log will be closed with either delete() or closeHandlers()
  // After memory mapped buffer is closed, no disk IO operation should be performed for this log
  @volatile private var isMemoryMappedBufferClosed = false

  /* last time it was flushed */
  private val lastFlushedTime = new AtomicLong(time.milliseconds)

  def initFileSize: Int = {
    if (config.preallocate)
      config.segmentSize
    else
      0
  }

  def updateConfig(updatedKeys: Set[String], newConfig: LogConfig): Unit = {
    if ((updatedKeys.contains(LogConfig.RetentionMsProp)
      || updatedKeys.contains(LogConfig.MessageTimestampDifferenceMaxMsProp))
      && topicPartition.partition == 0  // generate warnings only for one partition of each topic
      && newConfig.retentionMs < newConfig.messageTimestampDifferenceMaxMs)
      warn(s"${LogConfig.RetentionMsProp} for topic ${topicPartition.topic} is set to ${newConfig.retentionMs}. It is smaller than " +
        s"${LogConfig.MessageTimestampDifferenceMaxMsProp}'s value ${newConfig.messageTimestampDifferenceMaxMs}. " +
        s"This may result in frequent log rolling.")
    this.config = newConfig
  }

  private def checkIfMemoryMappedBufferClosed(): Unit = {
    if (isMemoryMappedBufferClosed)
      throw new KafkaStorageException(s"The memory mapped buffer for log of $topicPartition is already closed")
  }

  @volatile private var nextOffsetMetadata: LogOffsetMetadata = _

  /* The earliest offset which is part of an incomplete transaction. This is used to compute the
   * last stable offset (LSO) in ReplicaManager. Note that it is possible that the "true" first unstable offset
   * gets removed from the log (through record or segment deletion). In this case, the first unstable offset
   * will point to the log start offset, which may actually be either part of a completed transaction or not
   * part of a transaction at all. However, since we only use the LSO for the purpose of restricting the
   * read_committed consumer to fetching decided data (i.e. committed, aborted, or non-transactional), this
   * temporary abuse seems justifiable and saves us from scanning the log after deletion to find the first offsets
   * of each ongoing transaction in order to compute a new first unstable offset. It is possible, however,
   * that this could result in disagreement between replicas depending on when they began replicating the log.
   * In the worst case, the LSO could be seen by a consumer to go backwards.
   */
  @volatile var firstUnstableOffset: Option[LogOffsetMetadata] = None

  /* Keep track of the current high watermark in order to ensure that segments containing offsets at or above it are
   * not eligible for deletion. This means that the active segment is only eligible for deletion if the high watermark
   * equals the log end offset (which may never happen for a partition under consistent load). This is needed to
   * prevent the log start offset (which is exposed in fetch responses) from getting ahead of the high watermark.
   */
  @volatile private var replicaHighWatermark: Option[Long] = None

  /* the actual segments of the log */
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]

  @volatile private var _leaderEpochCache: LeaderEpochCache = initializeLeaderEpochCache()

  locally {
    val startMs = time.milliseconds

    val nextOffset = loadSegments()

    /* Calculate the offset of the next message */
    nextOffsetMetadata = new LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size)

    _leaderEpochCache.clearAndFlushLatest(nextOffsetMetadata.messageOffset)

    logStartOffset = math.max(logStartOffset, segments.firstEntry.getValue.baseOffset)

    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    _leaderEpochCache.clearAndFlushEarliest(logStartOffset)

    loadProducerState(logEndOffset, reloadFromCleanShutdown = hasCleanShutdownFile)

    info(s"Completed load of log with ${segments.size} segments, log start offset $logStartOffset and " +
      s"log end offset $logEndOffset in ${time.milliseconds() - startMs} ms")
  }

  private val tags = {
    val maybeFutureTag = if (isFuture) Map("is-future" -> "true") else Map.empty[String, String]
    Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString) ++ maybeFutureTag
  }

  newGauge("NumLogSegments",
    new Gauge[Int] {
      def value = numberOfSegments
    },
    tags)

  newGauge("LogStartOffset",
    new Gauge[Long] {
      def value = logStartOffset
    },
    tags)

  newGauge("LogEndOffset",
    new Gauge[Long] {
      def value = logEndOffset
    },
    tags)

  newGauge("Size",
    new Gauge[Long] {
      def value = size
    },
    tags)

  scheduler.schedule(name = "PeriodicProducerExpirationCheck", fun = () => {
    lock synchronized {
      producerStateManager.removeExpiredProducers(time.milliseconds)
    }
  }, period = producerIdExpirationCheckIntervalMs, delay = producerIdExpirationCheckIntervalMs, unit = TimeUnit.MILLISECONDS)

  /** The name of this log */
  def name  = dir.getName()

  def leaderEpochCache = _leaderEpochCache

  private def initializeLeaderEpochCache(): LeaderEpochCache = {
    // create the log directory if it doesn't exist
    Files.createDirectories(dir.toPath)
    new LeaderEpochFileCache(topicPartition, () => logEndOffsetMetadata,
      new LeaderEpochCheckpointFile(LeaderEpochFile.newFile(dir), logDirFailureChannel))
  }

  private def removeTempFilesAndCollectSwapFiles(): Set[File] = {

    def deleteIndicesIfExist(baseFile: File, swapFile: File, fileType: String): Unit = {
      info(s"Found $fileType file ${swapFile.getAbsolutePath} from interrupted swap operation. Deleting index files (if they exist).")
      val offset = offsetFromFile(baseFile)
      Files.deleteIfExists(Log.offsetIndexFile(dir, offset).toPath)
      Files.deleteIfExists(Log.timeIndexFile(dir, offset).toPath)
      Files.deleteIfExists(Log.transactionIndexFile(dir, offset).toPath)
    }

    var swapFiles = Set[File]()

    for (file <- dir.listFiles if file.isFile) {
      if (!file.canRead)
        throw new IOException(s"Could not read file $file")
      val filename = file.getName
      if (filename.endsWith(DeletedFileSuffix) || filename.endsWith(CleanedFileSuffix)) {
        debug(s"Deleting stray temporary file ${file.getAbsolutePath}")
        Files.deleteIfExists(file.toPath)
      } else if (filename.endsWith(SwapFileSuffix)) {
        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the index files, complete the swap operation later
        // if an index just delete the index files, they will be rebuilt
        val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        if (isIndexFile(baseFile)) {
          deleteIndicesIfExist(baseFile, file, "index")
        } else if (isLogFile(baseFile)) {
          deleteIndicesIfExist(baseFile, file, "log")
          swapFiles += file
        }
      }
    }
    swapFiles
  }

  // This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded
  private def loadSegmentFiles(): Unit = {
    // load segments in ascending order because transactional data from one segment may depend on the
    // segments that come before it
    for (file <- dir.listFiles.sortBy(_.getName) if file.isFile) {
      if (isIndexFile(file)) {
        // if it is an index file, make sure it has a corresponding .log file
        val offset = offsetFromFile(file)
        val logFile = Log.logFile(dir, offset)
        if (!logFile.exists) {
          warn(s"Found an orphaned index file ${file.getAbsolutePath}, with no corresponding log file.")
          Files.deleteIfExists(file.toPath)
        }
      } else if (isLogFile(file)) {
        // if it's a log file, load the corresponding log segment
        val baseOffset = offsetFromFile(file)
        val timeIndexFileNewlyCreated = !Log.timeIndexFile(dir, baseOffset).exists()
        val segment = LogSegment.open(dir = dir,
          baseOffset = baseOffset,
          config,
          time = time,
          fileAlreadyExists = true)

        try segment.sanityCheck(timeIndexFileNewlyCreated)
        catch {
          case _: NoSuchFileException =>
            error(s"Could not find offset index file corresponding to log file ${segment.log.file.getAbsolutePath}, " +
              "recovering segment and rebuilding index files...")
            recoverSegment(segment)
          case e: CorruptIndexException =>
            warn(s"Found a corrupted index file corresponding to log file ${segment.log.file.getAbsolutePath} due " +
              s"to ${e.getMessage}}, recovering segment and rebuilding index files...")
            recoverSegment(segment)
        }
        addSegment(segment)
      }
    }
  }

  private def recoverSegment(segment: LogSegment, leaderEpochCache: Option[LeaderEpochCache] = None): Int = lock synchronized {
    val stateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    stateManager.truncateAndReload(logStartOffset, segment.baseOffset, time.milliseconds)
    logSegments(stateManager.mapEndOffset, segment.baseOffset).foreach { segment =>
      val startOffset = math.max(segment.baseOffset, stateManager.mapEndOffset)
      val fetchDataInfo = segment.read(startOffset, None, Int.MaxValue)
      if (fetchDataInfo != null)
        loadProducersFromLog(stateManager, fetchDataInfo.records)
    }
    stateManager.updateMapEndOffset(segment.baseOffset)

    // take a snapshot for the first recovered segment to avoid reloading all the segments if we shutdown before we
    // checkpoint the recovery point
    stateManager.takeSnapshot()

    val bytesTruncated = segment.recover(stateManager, leaderEpochCache)

    // once we have recovered the segment's data, take a snapshot to ensure that we won't
    // need to reload the same segment again while recovering another segment.
    stateManager.takeSnapshot()
    bytesTruncated
  }

  // This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded
  private def completeSwapOperations(swapFiles: Set[File]): Unit = {
    for (swapFile <- swapFiles) {
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      val baseOffset = offsetFromFile(logFile)
      val swapSegment = LogSegment.open(swapFile.getParentFile,
        baseOffset = baseOffset,
        config,
        time = time,
        fileSuffix = SwapFileSuffix)
      info(s"Found log file ${swapFile.getPath} from interrupted swap operation, repairing.")
      recoverSegment(swapSegment)
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.readNextOffset)
      replaceSegments(swapSegment, oldSegments.toSeq, isRecoveredSwapFile = true)
    }
  }

  // Load the log segments from the log files on disk and return the next offset
  // This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded
  private def loadSegments(): Long = {
    // first do a pass through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    // now do a second pass and load all the log and index files
    loadSegmentFiles()

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    completeSwapOperations(swapFiles)

    if (logSegments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at offset 0
      addSegment(LogSegment.open(dir = dir,
        baseOffset = 0,
        config,
        time = time,
        fileAlreadyExists = false,
        initFileSize = this.initFileSize,
        preallocate = config.preallocate))
      0
    } else if (!dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {
      val nextOffset = recoverLog()
      // reset the index size of the currently active log segment to allow more entries
      activeSegment.resizeIndexes(config.maxIndexSize)
      nextOffset
    } else 0
  }

  private def updateLogEndOffset(messageOffset: Long) {
    nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size)
  }

  /**
   * Recover the log segments and return the next offset after recovery.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all
   * logs are loaded.
   */
  private def recoverLog(): Long = {
    // if we have the clean shutdown marker, skip recovery
    if (!hasCleanShutdownFile) {
      // okay we need to actually recover this log
      val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
      while (unflushed.hasNext) {
        val segment = unflushed.next
        info(s"Recovering unflushed segment ${segment.baseOffset}")
        val truncatedBytes =
          try {
            recoverSegment(segment, Some(_leaderEpochCache))
          } catch {
            case _: InvalidOffsetException =>
              val startOffset = segment.baseOffset
              warn("Found invalid offset during recovery. Deleting the corrupt segment and " +
                s"creating an empty one with starting offset $startOffset")
              segment.truncateTo(startOffset)
          }
        if (truncatedBytes > 0) {
          // we had an invalid message, delete all remaining log
          warn(s"Corruption found in segment ${segment.baseOffset}, truncating to offset ${segment.readNextOffset}")
          unflushed.foreach(deleteSegment)
        }
      }
    }
    recoveryPoint = activeSegment.readNextOffset
    recoveryPoint
  }

  private def loadProducerState(lastOffset: Long, reloadFromCleanShutdown: Boolean): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    val messageFormatVersion = config.messageFormatVersion.recordVersion.value
    info(s"Loading producer state from offset $lastOffset with message format version $messageFormatVersion")

    // We want to avoid unnecessary scanning of the log to build the producer state when the broker is being
    // upgraded. The basic idea is to use the absence of producer snapshot files to detect the upgrade case,
    // but we have to be careful not to assume too much in the presence of broker failures. The two most common
    // upgrade cases in which we expect to find no snapshots are the following:
    //
    // 1. The broker has been upgraded, but the topic is still on the old message format.
    // 2. The broker has been upgraded, the topic is on the new message format, and we had a clean shutdown.
    //
    // If we hit either of these cases, we skip producer state loading and write a new snapshot at the log end
    // offset (see below). The next time the log is reloaded, we will load producer state using this snapshot
    // (or later snapshots). Otherwise, if there is no snapshot file, then we have to rebuild producer state
    // from the first segment.

    if (producerStateManager.latestSnapshotOffset.isEmpty && (messageFormatVersion < RecordBatch.MAGIC_VALUE_V2 || reloadFromCleanShutdown)) {
      // To avoid an expensive scan through all of the segments, we take empty snapshots from the start of the
      // last two segments and the last offset. This should avoid the full scan in the case that the log needs
      // truncation.
      val nextLatestSegmentBaseOffset = lowerSegment(activeSegment.baseOffset).map(_.baseOffset)
      val offsetsToSnapshot = Seq(nextLatestSegmentBaseOffset, Some(activeSegment.baseOffset), Some(lastOffset))
      offsetsToSnapshot.flatten.foreach { offset =>
        producerStateManager.updateMapEndOffset(offset)
        producerStateManager.takeSnapshot()
      }
    } else {
      val isEmptyBeforeTruncation = producerStateManager.isEmpty && producerStateManager.mapEndOffset >= lastOffset
      producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds())

      // Only do the potentially expensive reloading if the last snapshot offset is lower than the log end
      // offset (which would be the case on first startup) and there were active producers prior to truncation
      // (which could be the case if truncating after initial loading). If there weren't, then truncating
      // shouldn't change that fact (although it could cause a producerId to expire earlier than expected),
      // and we can skip the loading. This is an optimization for users which are not yet using
      // idempotent/transactional features yet.
      if (lastOffset > producerStateManager.mapEndOffset && !isEmptyBeforeTruncation) {
        logSegments(producerStateManager.mapEndOffset, lastOffset).foreach { segment =>
          val startOffset = Utils.max(segment.baseOffset, producerStateManager.mapEndOffset, logStartOffset)
          producerStateManager.updateMapEndOffset(startOffset)
          producerStateManager.takeSnapshot()

          val fetchDataInfo = segment.read(startOffset, Some(lastOffset), Int.MaxValue)
          if (fetchDataInfo != null)
            loadProducersFromLog(producerStateManager, fetchDataInfo.records)
        }
      }

      producerStateManager.updateMapEndOffset(lastOffset)
      updateFirstUnstableOffset()
    }
  }

  private def loadProducersFromLog(producerStateManager: ProducerStateManager, records: Records): Unit = {
    val loadedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    records.batches.asScala.foreach { batch =>
      if (batch.hasProducerId) {
        val maybeCompletedTxn = updateProducers(batch, loadedProducers, isFromClient = false)
        maybeCompletedTxn.foreach(completedTxns += _)
      }
    }
    loadedProducers.values.foreach(producerStateManager.update)
    completedTxns.foreach(producerStateManager.completeTxn)
  }

  private[log] def activeProducersWithLastSequence: Map[Long, Int] = lock synchronized {
    producerStateManager.activeProducers.map { case (producerId, producerIdEntry) =>
      (producerId, producerIdEntry.lastSeq)
    }
  }

  /**
   * Check if we have the "clean shutdown" file
   */
  private def hasCleanShutdownFile: Boolean = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log.
   * The memory mapped buffer for index files of this log will be left open until the log is deleted.
   */
  def close() {
    debug("Closing log")
    lock synchronized {
      checkIfMemoryMappedBufferClosed()
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in dir ${dir.getParent}") {
        // We take a snapshot at the last written offset to hopefully avoid the need to scan the log
        // after restarting and to ensure that we cannot inadvertently hit the upgrade optimization
        // (the clean shutdown file is written after the logs are all closed).
        producerStateManager.takeSnapshot()
        logSegments.foreach(_.close())
      }
    }
  }

  /**
    * Rename the directory of the log
    *
    * @throws KafkaStorageException if rename fails
    */
  def renameDir(name: String) {
    lock synchronized {
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in log dir ${dir.getParent}") {
        val renamedDir = new File(dir.getParent, name)
        Utils.atomicMoveWithFallback(dir.toPath, renamedDir.toPath)
        if (renamedDir != dir) {
          dir = renamedDir
          logSegments.foreach(_.updateDir(renamedDir))
          producerStateManager.logDir = dir
          // re-initialize leader epoch cache so that LeaderEpochCheckpointFile.checkpoint can correctly reference
          // the checkpoint file in renamed log directory
          _leaderEpochCache = initializeLeaderEpochCache()
        }
      }
    }
  }

  /**
   * Close file handlers used by log but don't write to disk. This is called if the log directory is offline
   */
  def closeHandlers() {
    debug("Closing handlers")
    lock synchronized {
      logSegments.foreach(_.closeHandlers())
      isMemoryMappedBufferClosed = true
    }
  }

  /**
   * Append this message set to the active segment of the log, assigning offsets and Partition Leader Epochs
   *
   * @param records The records to append
   * @param isFromClient Whether or not this append is from a producer
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsLeader(records: MemoryRecords, leaderEpoch: Int, isFromClient: Boolean = true): LogAppendInfo = {
    append(records, isFromClient, assignOffsets = true, leaderEpoch)
  }

  /**
   * Append this message set to the active segment of the log without assigning offsets or Partition Leader Epochs
   *
   * @param records The records to append
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
    append(records, isFromClient = false, assignOffsets = false, leaderEpoch = -1)
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   *
   * This method will generally be responsible for assigning offsets to the messages,
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   *
   * @param records The log records to append
   * @param isFromClient Whether or not this append is from a producer
   * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   * @param leaderEpoch The partition's leader epoch which will be applied to messages when offsets are assigned on the leader
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  private def append(records: MemoryRecords, isFromClient: Boolean, assignOffsets: Boolean, leaderEpoch: Int): LogAppendInfo = {
    maybeHandleIOException(s"Error while appending records to $topicPartition in dir ${dir.getParent}") {
      val appendInfo = analyzeAndValidateRecords(records, isFromClient = isFromClient)

      // return if we have no valid messages or if this is a duplicate of the last appended entry
      if (appendInfo.shallowCount == 0)
        return appendInfo

      // trim any invalid bytes or partial messages before appending it to the on-disk log
      var validRecords = trimInvalidBytes(records, appendInfo)

      // they are valid, insert them in the log
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        if (assignOffsets) {
          // assign offsets to the message set
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          appendInfo.firstOffset = Some(offset.value)
          val now = time.milliseconds
          val validateAndOffsetAssignResult = try {
            LogValidator.validateMessagesAndAssignOffsets(validRecords,
              offset,
              time,
              now,
              appendInfo.sourceCodec,
              appendInfo.targetCodec,
              config.compact,
              config.messageFormatVersion.recordVersion.value,
              config.messageTimestampType,
              config.messageTimestampDifferenceMaxMs,
              leaderEpoch,
              isFromClient)
          } catch {
            case e: IOException =>
              throw new KafkaException(s"Error validating messages while appending to log $name", e)
          }
          validRecords = validateAndOffsetAssignResult.validatedRecords
          appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
          appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
          appendInfo.lastOffset = offset.value - 1
          appendInfo.recordsProcessingStats = validateAndOffsetAssignResult.recordsProcessingStats
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.logAppendTime = now

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          if (validateAndOffsetAssignResult.messageSizeMaybeChanged) {
            for (batch <- validRecords.batches.asScala) {
              if (batch.sizeInBytes > config.maxMessageSize) {
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                throw new RecordTooLargeException(s"Message batch size is ${batch.sizeInBytes} bytes in append to" +
                  s"partition $topicPartition which exceeds the maximum configured size of ${config.maxMessageSize}.")
              }
            }
          }
        } else {
          // we are taking the offsets we are given
          if (!appendInfo.offsetsMonotonic || appendInfo.firstOrLastOffset < nextOffsetMetadata.messageOffset)
            throw new IllegalArgumentException(s"Out of order offsets found in append to $topicPartition: " +
              records.records.asScala.map(_.offset))
        }

        // update the epoch cache with the epoch stamped onto the message by the leader
        validRecords.batches.asScala.foreach { batch =>
          if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
            _leaderEpochCache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
        }

        // check messages set size may be exceed config.segmentSize
        if (validRecords.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException(s"Message batch size is ${validRecords.sizeInBytes} bytes in append " +
            s"to partition $topicPartition, which exceeds the maximum configured segment size of ${config.segmentSize}.")
        }

        // now that we have valid records, offsets assigned, and timestamps updated, we need to
        // validate the idempotent/transactional state of the producers and collect some metadata
        val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(validRecords, isFromClient)
        maybeDuplicate.foreach { duplicate =>
          appendInfo.firstOffset = Some(duplicate.firstOffset)
          appendInfo.lastOffset = duplicate.lastOffset
          appendInfo.logAppendTime = duplicate.timestamp
          appendInfo.logStartOffset = logStartOffset
          return appendInfo
        }

        // maybe roll the log if this segment is full
        val segment = maybeRoll(validRecords.sizeInBytes, appendInfo)

        val logOffsetMetadata = LogOffsetMetadata(
          messageOffset = appendInfo.firstOrLastOffset,
          segmentBaseOffset = segment.baseOffset,
          relativePositionInSegment = segment.size)

        segment.append(largestOffset = appendInfo.lastOffset,
          largestTimestamp = appendInfo.maxTimestamp,
          shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
          records = validRecords)

        // update the producer state
        for ((producerId, producerAppendInfo) <- updatedProducers) {
          producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
          producerStateManager.update(producerAppendInfo)
        }

        // update the transaction index with the true last stable offset. The last offset visible
        // to consumers using READ_COMMITTED will be limited by this value and the high watermark.
        for (completedTxn <- completedTxns) {
          val lastStableOffset = producerStateManager.completeTxn(completedTxn)
          segment.updateTxnIndex(completedTxn, lastStableOffset)
        }

        // always update the last producer id map offset so that the snapshot reflects the current offset
        // even if there isn't any idempotent data being written
        producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

        // increment the log end offset
        updateLogEndOffset(appendInfo.lastOffset + 1)

        // update the first unstable offset (which is used to compute LSO)
        updateFirstUnstableOffset()

        trace(s"Appended message set with last offset: ${appendInfo.lastOffset}, " +
          s"first offset: ${appendInfo.firstOffset}, " +
          s"next offset: ${nextOffsetMetadata.messageOffset}, " +
          s"and messages: $validRecords")

        if (unflushedMessages >= config.flushInterval)
          flush()

        appendInfo
      }
    }
  }

  def onHighWatermarkIncremented(highWatermark: Long): Unit = {
    lock synchronized {
      replicaHighWatermark = Some(highWatermark)
      producerStateManager.onHighWatermarkUpdated(highWatermark)
      updateFirstUnstableOffset()
    }
  }

  private def updateFirstUnstableOffset(): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    val updatedFirstStableOffset = producerStateManager.firstUnstableOffset match {
      case Some(logOffsetMetadata) if logOffsetMetadata.messageOffsetOnly || logOffsetMetadata.messageOffset < logStartOffset =>
        val offset = math.max(logOffsetMetadata.messageOffset, logStartOffset)
        val segment = segments.floorEntry(offset).getValue
        val position  = segment.translateOffset(offset)
        Some(LogOffsetMetadata(offset, segment.baseOffset, position.position))
      case other => other
    }

    if (updatedFirstStableOffset != this.firstUnstableOffset) {
      debug(s"First unstable offset updated to $updatedFirstStableOffset")
      this.firstUnstableOffset = updatedFirstStableOffset
    }
  }

  /**
   * Increment the log start offset if the provided offset is larger.
   */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long) {
    // We don't have to write the log start offset to log-start-offset-checkpoint immediately.
    // The deleteRecordsOffset may be lost only if all in-sync replicas of this broker are shutdown
    // in an unclean manner within log.flush.start.offset.checkpoint.interval.ms. The chance of this happening is low.
    maybeHandleIOException(s"Exception while increasing log start offset for $topicPartition to $newLogStartOffset in dir ${dir.getParent}") {
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        if (newLogStartOffset > logStartOffset) {
          info(s"Incrementing log start offset to $newLogStartOffset")
          logStartOffset = newLogStartOffset
          _leaderEpochCache.clearAndFlushEarliest(logStartOffset)
          producerStateManager.truncateHead(logStartOffset)
          updateFirstUnstableOffset()
        }
      }
    }
  }

  private def analyzeAndValidateProducerState(records: MemoryRecords, isFromClient: Boolean):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    for (batch <- records.batches.asScala if batch.hasProducerId) {
      val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

      // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
      // If we find a duplicate, we return the metadata of the appended batch to the client.
      if (isFromClient) {
        maybeLastEntry.flatMap(_.findDuplicateBatch(batch)).foreach { duplicate =>
          return (updatedProducers, completedTxns.toList, Some(duplicate))
        }
      }

      val maybeCompletedTxn = updateProducers(batch, updatedProducers, isFromClient = isFromClient)
      maybeCompletedTxn.foreach(completedTxns += _)
    }
    (updatedProducers, completedTxns.toList, None)
  }

  /**
   * Validate the following:
   * <ol>
   * <li> each message matches its CRC
   * <li> each message size is valid
   * <li> that the sequence numbers of the incoming record batches are consistent with the existing state and with each other.
   * </ol>
   *
   * Also compute the following quantities:
   * <ol>
   * <li> First offset in the message set
   * <li> Last offset in the message set
   * <li> Number of messages
   * <li> Number of valid bytes
   * <li> Whether the offsets are monotonically increasing
   * <li> Whether any compression codec is used (if many are used, then the last one is given)
   * </ol>
   */
  private def analyzeAndValidateRecords(records: MemoryRecords, isFromClient: Boolean): LogAppendInfo = {
    var shallowMessageCount = 0
    var validBytesCount = 0
    var firstOffset: Option[Long] = None
    var lastOffset = -1L
    var sourceCodec: CompressionCodec = NoCompressionCodec
    var monotonic = true
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    var readFirstMessage = false

    for (batch <- records.batches.asScala) {
      // we only validate V2 and higher to avoid potential compatibility issues with older clients
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2 && isFromClient && batch.baseOffset != 0)
        throw new InvalidRecordException(s"The baseOffset of the record batch in the append to $topicPartition should " +
          s"be 0, but it is ${batch.baseOffset}")

      // update the first offset if on the first message. For magic versions older than 2, we use the last offset
      // to avoid the need to decompress the data (the last offset can be obtained directly from the wrapper message).
      // For magic version 2, we can get the first offset directly from the batch header.
      // When appending to the leader, we will update LogAppendInfo.baseOffset with the correct value. In the follower
      // case, validation will be more lenient.
      // Also indicate whether we have the accurate first offset or not
      if (!readFirstMessage) {
        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
          firstOffset = Some(batch.baseOffset)
        readFirstMessage = true
      }

      // check that offsets are monotonically increasing
      if (lastOffset >= batch.lastOffset)
        monotonic = false

      // update the last offset seen
      lastOffset = batch.lastOffset

      // Check if the message sizes are valid.
      val batchSize = batch.sizeInBytes
      if (batchSize > config.maxMessageSize) {
        brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
        brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        throw new RecordTooLargeException(s"The record batch size in the append to $topicPartition is $batchSize bytes " +
          s"which exceeds the maximum configured value of ${config.maxMessageSize}.")
      }

      // check the validity of the message by checking CRC
      batch.ensureValid()

      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = lastOffset
      }

      shallowMessageCount += 1
      validBytesCount += batchSize

      val messageCodec = CompressionCodec.getCompressionCodec(batch.compressionType.id)
      if (messageCodec != NoCompressionCodec)
        sourceCodec = messageCodec
    }

    // Apply broker-side compression if any
    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)
    LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordsProcessingStats.EMPTY, sourceCodec, targetCodec, shallowMessageCount, validBytesCount, monotonic)
  }

  private def updateProducers(batch: RecordBatch,
                              producers: mutable.Map[Long, ProducerAppendInfo],
                              isFromClient: Boolean): Option[CompletedTxn] = {
    val producerId = batch.producerId
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, isFromClient))
    appendInfo.append(batch)
  }

  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   *
   * @param records The records to trim
   * @param info The general information of the message set
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   */
  private def trimInvalidBytes(records: MemoryRecords, info: LogAppendInfo): MemoryRecords = {
    val validBytes = info.validBytes
    if (validBytes < 0)
      throw new CorruptRecordException(s"Cannot append record batch with illegal length $validBytes to " +
        s"log for $topicPartition. A possible cause is a corrupted produce request.")
    if (validBytes == records.sizeInBytes) {
      records
    } else {
      // trim invalid bytes
      val validByteBuffer = records.buffer.duplicate()
      validByteBuffer.limit(validBytes)
      MemoryRecords.readableRecords(validByteBuffer)
    }
  }

  private[log] def readUncommitted(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None,
                                   minOneMessage: Boolean = false): FetchDataInfo = {
    read(startOffset, maxLength, maxOffset, minOneMessage, isolationLevel = IsolationLevel.READ_UNCOMMITTED)
  }

  /**
   * Read messages from the log.
   *
   * @param startOffset The offset to begin reading at
   * @param maxLength The maximum number of bytes to read
   * @param maxOffset The offset to read up to, exclusive. (i.e. this offset NOT included in the resulting message set)
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   * @param isolationLevel The isolation level of the fetcher. The READ_UNCOMMITTED isolation level has the traditional
   *                       read semantics (e.g. consumers are limited to fetching up to the high watermark). In
   *                       READ_COMMITTED, consumers are limited to fetching up to the last stable offset. Additionally,
   *                       in READ_COMMITTED, the transaction index is consulted after fetching to collect the list
   *                       of aborted transactions in the fetch range which the consumer uses to filter the fetched
   *                       records before they are returned to the user. Note that fetches from followers always use
   *                       READ_UNCOMMITTED.
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None, minOneMessage: Boolean = false,
           isolationLevel: IsolationLevel): FetchDataInfo = {
    maybeHandleIOException(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
      trace(s"Reading $maxLength bytes from offset $startOffset of length $size bytes")

      // Because we don't use lock for reading, the synchronization is a little bit tricky.
      // We create the local variables to avoid race conditions with updates to the log.
      val currentNextOffsetMetadata = nextOffsetMetadata
      val next = currentNextOffsetMetadata.messageOffset
      if (startOffset == next) {
        val abortedTransactions =
          if (isolationLevel == IsolationLevel.READ_COMMITTED) Some(List.empty[AbortedTransaction])
          else None
        return FetchDataInfo(currentNextOffsetMetadata, MemoryRecords.EMPTY, firstEntryIncomplete = false,
          abortedTransactions = abortedTransactions)
      }

      var segmentEntry = segments.floorEntry(startOffset)

      // return error on attempt to read beyond the log end offset or read below log start offset
      if (startOffset > next || segmentEntry == null || startOffset < logStartOffset)
        throw new OffsetOutOfRangeException(s"Received request for offset $startOffset for partition $topicPartition, " +
          s"but we only have log segments in the range $logStartOffset to $next.")

      // Do the read on the segment with a base offset less than the target offset
      // but if that segment doesn't contain any messages with an offset greater than that
      // continue to read from successive segments until we get some messages or we reach the end of the log
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue

        // If the fetch occurs on the active segment, there might be a race condition where two fetch requests occur after
        // the message is appended but before the nextOffsetMetadata is updated. In that case the second fetch may
        // cause OffsetOutOfRangeException. To solve that, we cap the reading up to exposed position instead of the log
        // end of the active segment.
        val maxPosition = {
          if (segmentEntry == segments.lastEntry) {
            val exposedPos = nextOffsetMetadata.relativePositionInSegment.toLong
            // Check the segment again in case a new segment has just rolled out.
            if (segmentEntry != segments.lastEntry)
            // New log segment has rolled out, we can read up to the file end.
              segment.size
            else
              exposedPos
          } else {
            segment.size
          }
        }
        val fetchInfo = segment.read(startOffset, maxOffset, maxLength, maxPosition, minOneMessage)
        if (fetchInfo == null) {
          segmentEntry = segments.higherEntry(segmentEntry.getKey)
        } else {
          return isolationLevel match {
            case IsolationLevel.READ_UNCOMMITTED => fetchInfo
            case IsolationLevel.READ_COMMITTED => addAbortedTransactions(startOffset, segmentEntry, fetchInfo)
          }
        }
      }

      // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
      // this can happen when all messages with offset larger than start offsets have been deleted.
      // In this case, we will return the empty set with log end offset metadata
      FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
    }
  }

  private[log] def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    val segmentEntry = segments.floorEntry(startOffset)
    val allAbortedTxns = ListBuffer.empty[AbortedTxn]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = allAbortedTxns ++= abortedTxns
    collectAbortedTransactions(logStartOffset, upperBoundOffset, segmentEntry, accumulator)
    allAbortedTxns.toList
  }

  private def addAbortedTransactions(startOffset: Long, segmentEntry: JEntry[JLong, LogSegment],
                                     fetchInfo: FetchDataInfo): FetchDataInfo = {
    val fetchSize = fetchInfo.records.sizeInBytes
    val startOffsetPosition = OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
      fetchInfo.fetchOffsetMetadata.relativePositionInSegment)
    val upperBoundOffset = segmentEntry.getValue.fetchUpperBoundOffset(startOffsetPosition, fetchSize).getOrElse {
      val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
      if (nextSegmentEntry != null)
        nextSegmentEntry.getValue.baseOffset
      else
        logEndOffset
    }

    val abortedTransactions = ListBuffer.empty[AbortedTransaction]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = abortedTransactions ++= abortedTxns.map(_.asAbortedTransaction)
    collectAbortedTransactions(startOffset, upperBoundOffset, segmentEntry, accumulator)

    FetchDataInfo(fetchOffsetMetadata = fetchInfo.fetchOffsetMetadata,
      records = fetchInfo.records,
      firstEntryIncomplete = fetchInfo.firstEntryIncomplete,
      abortedTransactions = Some(abortedTransactions.toList))
  }

  private def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long,
                                         startingSegmentEntry: JEntry[JLong, LogSegment],
                                         accumulator: List[AbortedTxn] => Unit): Unit = {
    var segmentEntry = startingSegmentEntry
    while (segmentEntry != null) {
      val searchResult = segmentEntry.getValue.collectAbortedTxns(startOffset, upperBoundOffset)
      accumulator(searchResult.abortedTransactions)
      if (searchResult.isComplete)
        return
      segmentEntry = segments.higherEntry(segmentEntry.getKey)
    }
  }

  /**
   * Get an offset based on the given timestamp
   * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
   * given timestamp.
   *
   * If no such message is found, the log end offset is returned.
   *
   * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
   * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
   *
   * @param targetTimestamp The given timestamp for offset fetching.
   * @return The offset of the first message whose timestamp is greater than or equals to the given timestamp.
   *         None if no such message is found.
   */
  def fetchOffsetsByTimestamp(targetTimestamp: Long): Option[TimestampOffset] = {
    maybeHandleIOException(s"Error while fetching offset by timestamp for $topicPartition in dir ${dir.getParent}") {
      debug(s"Searching offset for timestamp $targetTimestamp")

      if (config.messageFormatVersion < KAFKA_0_10_0_IV0 &&
        targetTimestamp != ListOffsetRequest.EARLIEST_TIMESTAMP &&
        targetTimestamp != ListOffsetRequest.LATEST_TIMESTAMP)
        throw new UnsupportedForMessageFormatException(s"Cannot search offsets based on timestamp because message format version " +
          s"for partition $topicPartition is ${config.messageFormatVersion} which is earlier than the minimum " +
          s"required version $KAFKA_0_10_0_IV0")

      // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
      // constant time access while being safe to use with concurrent collections unlike `toArray`.
      val segmentsCopy = logSegments.toBuffer
      // For the earliest and latest, we do not need to return the timestamp.
      if (targetTimestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
        return Some(TimestampOffset(RecordBatch.NO_TIMESTAMP, logStartOffset))
      else if (targetTimestamp == ListOffsetRequest.LATEST_TIMESTAMP)
        return Some(TimestampOffset(RecordBatch.NO_TIMESTAMP, logEndOffset))

      val targetSeg = {
        // Get all the segments whose largest timestamp is smaller than target timestamp
        val earlierSegs = segmentsCopy.takeWhile(_.largestTimestamp < targetTimestamp)
        // We need to search the first segment whose largest timestamp is greater than the target timestamp if there is one.
        if (earlierSegs.length < segmentsCopy.length)
          Some(segmentsCopy(earlierSegs.length))
        else
          None
      }

      targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp, logStartOffset))
    }
  }

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, return None to the caller.
   */
  def convertToOffsetMetadata(offset: Long): Option[LogOffsetMetadata] = {
    try {
      val fetchDataInfo = readUncommitted(offset, 1)
      Some(fetchDataInfo.fetchOffsetMetadata)
    } catch {
      case _: OffsetOutOfRangeException => None
    }
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   *
   * @param predicate A function that takes in a candidate log segment and the next higher segment
   *                  (if there is one) and returns true iff it is deletable
   * @return The number of segments deleted
   */
  private def deleteOldSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean, reason: String): Int = {
    lock synchronized {
      val deletable = deletableSegments(predicate)
      if (deletable.nonEmpty)
        info(s"Found deletable segments with base offsets [${deletable.map(_.baseOffset).mkString(",")}] due to $reason")
      deleteSegments(deletable)
    }
  }

  private def deleteSegments(deletable: Iterable[LogSegment]): Int = {
    maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        if (segments.size == numToDelete)
          roll()
        lock synchronized {
          checkIfMemoryMappedBufferClosed()
          // remove the segments for lookups
          deletable.foreach(deleteSegment)
          maybeIncrementLogStartOffset(segments.firstEntry.getValue.baseOffset)
        }
      }
      numToDelete
    }
  }

  /**
   * Find segments starting from the oldest until the user-supplied predicate is false or the segment
   * containing the current high watermark is reached. We do not delete segments with offsets at or beyond
   * the high watermark to ensure that the log start offset can never exceed it. If the high watermark
   * has not yet been initialized, no segments are eligible for deletion.
   *
   * A final segment that is empty will never be returned (since we would just end up re-creating it).
   *
   * @param predicate A function that takes in a candidate log segment and the next higher segment
   *                  (if there is one) and returns true iff it is deletable
   * @return the segments ready to be deleted
   */
  private def deletableSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean): Iterable[LogSegment] = {
    if (segments.isEmpty || replicaHighWatermark.isEmpty) {
      Seq.empty
    } else {
      val highWatermark = replicaHighWatermark.get
      val deletable = ArrayBuffer.empty[LogSegment]
      var segmentEntry = segments.firstEntry
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue
        val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
        val (nextSegment, upperBoundOffset, isLastSegmentAndEmpty) = if (nextSegmentEntry != null)
          (nextSegmentEntry.getValue, nextSegmentEntry.getValue.baseOffset, false)
        else
          (null, logEndOffset, segment.size == 0)

        if (highWatermark >= upperBoundOffset && predicate(segment, Option(nextSegment)) && !isLastSegmentAndEmpty) {
          deletable += segment
          segmentEntry = nextSegmentEntry
        } else {
          segmentEntry = null
        }
      }
      deletable
    }
  }

  /**
   * Delete any log segments that have either expired due to time based retention
   * or because the log size is > retentionSize
   */
  def deleteOldSegments(): Int = {
    if (!config.delete) return 0
    deleteRetentionMsBreachedSegments() + deleteRetentionSizeBreachedSegments() + deleteLogStartOffsetBreachedSegments()
  }

  private def deleteRetentionMsBreachedSegments(): Int = {
    if (config.retentionMs < 0) return 0
    val startMs = time.milliseconds
    deleteOldSegments((segment, _) => startMs - segment.largestTimestamp > config.retentionMs,
      reason = s"retention time ${config.retentionMs}ms breach")
  }

  private def deleteRetentionSizeBreachedSegments(): Int = {
    if (config.retentionSize < 0 || size < config.retentionSize) return 0
    var diff = size - config.retentionSize
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]) = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }

    deleteOldSegments(shouldDelete, reason = s"retention size in bytes ${config.retentionSize} breach")
  }

  private def deleteLogStartOffsetBreachedSegments(): Int = {
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]) =
      nextSegmentOpt.exists(_.baseOffset <= logStartOffset)

    deleteOldSegments(shouldDelete, reason = s"log start offset $logStartOffset breach")
  }

  def isFuture: Boolean = dir.getName.endsWith(Log.FutureDirSuffix)

  /**
   * The size of the log in bytes
   */
  def size: Long = Log.sizeInBytes(logSegments)

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   * The offset of the next message that will be appended to the log
   */
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Roll the log over to a new empty log segment if necessary.
   *
   * @param messagesSize The messages set size in bytes.
   * @param appendInfo log append information
   * logSegment will be rolled if one of the following conditions met
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed since the timestamp of first message in the segment (or since the create time if
   * the first message does not have a timestamp)
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   */
  private def maybeRoll(messagesSize: Int, appendInfo: LogAppendInfo): LogSegment = {
    val segment = activeSegment
    val now = time.milliseconds

    val maxTimestampInMessages = appendInfo.maxTimestamp
    val maxOffsetInMessages = appendInfo.lastOffset

    if (segment.shouldRoll(messagesSize, maxTimestampInMessages, maxOffsetInMessages, now)) {
      debug(s"Rolling new log segment (log_size = ${segment.size}/${config.segmentSize}}, " +
          s"offset_index_size = ${segment.offsetIndex.entries}/${segment.offsetIndex.maxEntries}, " +
          s"time_index_size = ${segment.timeIndex.entries}/${segment.timeIndex.maxEntries}, " +
          s"inactive_time_ms = ${segment.timeWaitedForRoll(now, maxTimestampInMessages)}/${config.segmentMs - segment.rollJitterMs}).")

      /*
        maxOffsetInMessages - Integer.MAX_VALUE is a heuristic value for the first offset in the set of messages.
        Since the offset in messages will not differ by more than Integer.MAX_VALUE, this is guaranteed <= the real
        first offset in the set. Determining the true first offset in the set requires decompression, which the follower
        is trying to avoid during log append. Prior behavior assigned new baseOffset = logEndOffset from old segment.
        This was problematic in the case that two consecutive messages differed in offset by
        Integer.MAX_VALUE.toLong + 2 or more.  In this case, the prior behavior would roll a new log segment whose
        base offset was too low to contain the next message.  This edge case is possible when a replica is recovering a
        highly compacted topic from scratch.
        Note that this is only required for pre-V2 message formats because these do not store the first message offset
        in the header.
      */
      appendInfo.firstOffset match {
        case Some(firstOffset) => roll(firstOffset)
        case None => roll(maxOffsetInMessages - Integer.MAX_VALUE)
      }
    } else {
      segment
    }
  }

  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
   *
   * @return The newly rolled segment
   */
  def roll(expectedNextOffset: Long = 0): LogSegment = {
    maybeHandleIOException(s"Error while rolling log segment for $topicPartition in dir ${dir.getParent}") {
      val start = time.hiResClockMs()
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        val newOffset = math.max(expectedNextOffset, logEndOffset)
        val logFile = Log.logFile(dir, newOffset)
        val offsetIdxFile = offsetIndexFile(dir, newOffset)
        val timeIdxFile = timeIndexFile(dir, newOffset)
        val txnIdxFile = transactionIndexFile(dir, newOffset)
        for (file <- List(logFile, offsetIdxFile, timeIdxFile, txnIdxFile) if file.exists) {
          warn(s"Newly rolled segment file ${file.getAbsolutePath} already exists; deleting it first")
          Files.delete(file.toPath)
        }

        Option(segments.lastEntry).foreach(_.getValue.onBecomeInactiveSegment())

        // take a snapshot of the producer state to facilitate recovery. It is useful to have the snapshot
        // offset align with the new segment offset since this ensures we can recover the segment by beginning
        // with the corresponding snapshot file and scanning the segment data. Because the segment base offset
        // may actually be ahead of the current producer state end offset (which corresponds to the log end offset),
        // we manually override the state offset here prior to taking the snapshot.
        producerStateManager.updateMapEndOffset(newOffset)
        producerStateManager.takeSnapshot()

        val segment = LogSegment.open(dir,
          baseOffset = newOffset,
          config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = initFileSize,
          preallocate = config.preallocate)
        val prev = addSegment(segment)
        if (prev != null)
          throw new KafkaException(s"Trying to roll a new log segment for topic partition $topicPartition with " +
            s"start offset $newOffset while it already exists.")
        // We need to update the segment base offset and append position data of the metadata when log rolls.
        // The next offset should not change.
        updateLogEndOffset(nextOffsetMetadata.messageOffset)
        // schedule an asynchronous flush of the old segment
        scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

        info(s"Rolled new log segment at offset $newOffset in ${time.hiResClockMs() - start} ms.")

        segment
      }
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  def unflushedMessages: Long = this.logEndOffset - this.recoveryPoint

  /**
   * Flush all log segments
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * Flush log segments for all offsets up to offset-1
   *
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
   */
  def flush(offset: Long) : Unit = {
    maybeHandleIOException(s"Error while flushing log for $topicPartition in dir ${dir.getParent} with offset $offset") {
      if (offset <= this.recoveryPoint)
        return
      debug(s"Flushing log up to offset $offset, last flushed: $lastFlushTime,  current time: ${time.milliseconds()}, " +
        s"unflushed: $unflushedMessages")
      for (segment <- logSegments(this.recoveryPoint, offset))
        segment.flush()

      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        if (offset > this.recoveryPoint) {
          this.recoveryPoint = offset
          lastFlushedTime.set(time.milliseconds)
        }
      }
    }
  }

  /**
   * Cleanup old producer snapshots after the recovery point is checkpointed. It is useful to retain
   * the snapshots from the recent segments in case we need to truncate and rebuild the producer state.
   * Otherwise, we would always need to rebuild from the earliest segment.
   *
   * More specifically:
   *
   * 1. We always retain the producer snapshot from the last two segments. This solves the common case
   * of truncating to an offset within the active segment, and the rarer case of truncating to the previous segment.
   *
   * 2. We only delete snapshots for offsets less than the recovery point. The recovery point is checkpointed
   * periodically and it can be behind after a hard shutdown. Since recovery starts from the recovery point, the logic
   * of rebuilding the producer snapshots in one pass and without loading older segments is simpler if we always
   * have a producer snapshot for all segments being recovered.
   *
   * Return the minimum snapshots offset that was retained.
   */
  def deleteSnapshotsAfterRecoveryPointCheckpoint(): Long = {
    val minOffsetToRetain = minSnapshotsOffsetToRetain
    producerStateManager.deleteSnapshotsBefore(minOffsetToRetain)
    minOffsetToRetain
  }

  // Visible for testing, see `deleteSnapshotsAfterRecoveryPointCheckpoint()` for details
  private[log] def minSnapshotsOffsetToRetain: Long = {
    lock synchronized {
      val twoSegmentsMinOffset = lowerSegment(activeSegment.baseOffset).getOrElse(activeSegment).baseOffset
      // Prefer segment base offset
      val recoveryPointOffset = lowerSegment(recoveryPoint).map(_.baseOffset).getOrElse(recoveryPoint)
      math.min(recoveryPointOffset, twoSegmentsMinOffset)
    }
  }

  private def lowerSegment(offset: Long): Option[LogSegment] =
    Option(segments.lowerEntry(offset)).map(_.getValue)

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  private[log] def delete() {
    maybeHandleIOException(s"Error while deleting log for $topicPartition in dir ${dir.getParent}") {
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        removeLogMetrics()
        logSegments.foreach(_.deleteIfExists())
        segments.clear()
        _leaderEpochCache.clear()
        Utils.delete(dir)
        // File handlers will be closed if this log is deleted
        isMemoryMappedBufferClosed = true
      }
    }
  }

  // visible for testing
  private[log] def takeProducerSnapshot(): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    producerStateManager.takeSnapshot()
  }

  // visible for testing
  private[log] def latestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.latestSnapshotOffset
  }

  // visible for testing
  private[log] def oldestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.oldestSnapshotOffset
  }

  // visible for testing
  private[log] def latestProducerStateEndOffset: Long = lock synchronized {
    producerStateManager.mapEndOffset
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   *
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   * @return True iff targetOffset < logEndOffset
   */
  private[log] def truncateTo(targetOffset: Long): Boolean = {
    maybeHandleIOException(s"Error while truncating log to offset $targetOffset for $topicPartition in dir ${dir.getParent}") {
      if (targetOffset < 0)
        throw new IllegalArgumentException(s"Cannot truncate partition $topicPartition to a negative offset (%d).".format(targetOffset))
      if (targetOffset >= logEndOffset) {
        info(s"Truncating to $targetOffset has no effect as the largest offset in the log is ${logEndOffset - 1}")
        false
      } else {
        info(s"Truncating to offset $targetOffset")
        lock synchronized {
          checkIfMemoryMappedBufferClosed()
          if (segments.firstEntry.getValue.baseOffset > targetOffset) {
            truncateFullyAndStartAt(targetOffset)
          } else {
            val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
            deletable.foreach(deleteSegment)
            activeSegment.truncateTo(targetOffset)
            updateLogEndOffset(targetOffset)
            this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
            this.logStartOffset = math.min(targetOffset, this.logStartOffset)
            _leaderEpochCache.clearAndFlushLatest(targetOffset)
            loadProducerState(targetOffset, reloadFromCleanShutdown = false)
          }
          true
        }
      }
    }
  }

  /**
   *  Delete all data in the log and start at the new offset
   *
   *  @param newOffset The new offset to start the log with
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long) {
    maybeHandleIOException(s"Error while truncating the entire log for $topicPartition in dir ${dir.getParent}") {
      debug(s"Truncate and start at offset $newOffset")
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        val segmentsToDelete = logSegments.toList
        segmentsToDelete.foreach(deleteSegment)
        addSegment(LogSegment.open(dir,
          baseOffset = newOffset,
          config = config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = initFileSize,
          preallocate = config.preallocate))
        updateLogEndOffset(newOffset)
        _leaderEpochCache.clearAndFlush()

        producerStateManager.truncate()
        producerStateManager.updateMapEndOffset(newOffset)
        updateFirstUnstableOffset()

        this.recoveryPoint = math.min(newOffset, this.recoveryPoint)
        this.logStartOffset = newOffset
      }
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime: Long = lastFlushedTime.get

  /**
   * The active segment that is currently taking appends
   */
  def activeSegment = segments.lastEntry.getValue

  /**
   * All the log segments in this log ordered from oldest to newest
   */
  def logSegments: Iterable[LogSegment] = segments.values.asScala

  /**
   * Get all segments beginning with the segment that includes "from" and ending with the segment
   * that includes up to "to-1" or the end of the log (if to > logEndOffset)
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    lock synchronized {
      val view = Option(segments.floorKey(from)).map { floor =>
        segments.subMap(floor, to)
      }.getOrElse(segments.headMap(to))
      view.values.asScala
    }
  }

  override def toString = "Log(" + dir + ")"

  /**
   * This method performs an asynchronous log segment delete by doing the following:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It schedules an asynchronous delete operation to occur in the future
   * </ol>
   * This allows reads to happen concurrently without synchronization and without the possibility of physically
   * deleting a file while it is being read from.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either called before all logs are loaded
   * or the immediate caller will catch and handle IOException
   *
   * @param segment The log segment to schedule for deletion
   */
  private def deleteSegment(segment: LogSegment) {
    info(s"Scheduling log segment [baseOffset ${segment.baseOffset}, size ${segment.size}] for deletion.")
    lock synchronized {
      segments.remove(segment.baseOffset)
      asyncDeleteSegment(segment)
    }
  }

  /**
   * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
   *
   * This method does not need to convert IOException (thrown from changeFileSuffixes) to KafkaStorageException because
   * it is either called before all logs are loaded or the caller will catch and handle IOException
   *
   * @throws IOException if the file can't be renamed and still exists
   */
  private def asyncDeleteSegment(segment: LogSegment) {
    segment.changeFileSuffixes("", Log.DeletedFileSuffix)
    def deleteSeg() {
      info(s"Deleting segment ${segment.baseOffset}")
      maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
        segment.deleteIfExists()
      }
    }
    scheduler.schedule("delete-file", deleteSeg _, delay = config.fileDeleteDelayMs)
  }

  /**
   * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
   * be asynchronously deleted.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either called before all logs are loaded
   * or the caller will catch and handle IOException
   *
   * The sequence of operations is:
   * <ol>
   *   <li> Cleaner creates new segment with suffix .cleaned and invokes replaceSegments().
   *        If broker crashes at this point, the clean-and-swap operation is aborted and
   *        the .cleaned file is deleted on recovery in loadSegments().
   *   <li> New segment is renamed .swap. If the broker crashes after this point before the whole
   *        operation is completed, the swap operation is resumed on recovery as described in the next step.
   *   <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
   *        If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
   *        replaceSegments() is then invoked to complete the swap with newSegment recreated from
   *        the .swap file and oldSegments containing segments which were not renamed before the crash.
   *   <li> Swap segment is renamed to replace the existing segment, completing this operation.
   *        If the broker crashes, any .deleted files which may be left behind are deleted
   *        on recovery in loadSegments().
   * </ol>
   *
   * @param newSegment The new log segment to add to the log
   * @param oldSegments The old log segments to delete from the log
   * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
   */
  private[log] def replaceSegments(newSegment: LogSegment, oldSegments: Seq[LogSegment], isRecoveredSwapFile: Boolean = false) {
    lock synchronized {
      checkIfMemoryMappedBufferClosed()
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      if (!isRecoveredSwapFile)
        newSegment.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix)
      addSegment(newSegment)

      // delete the old files
      for (seg <- oldSegments) {
        // remove the index entry
        if (seg.baseOffset != newSegment.baseOffset)
          segments.remove(seg.baseOffset)
        // delete segment
        asyncDeleteSegment(seg)
      }
      // okay we are safe now, remove the swap suffix
      newSegment.changeFileSuffixes(Log.SwapFileSuffix, "")
    }
  }

  /**
   * remove deleted log metrics
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric("NumLogSegments", tags)
    removeMetric("LogStartOffset", tags)
    removeMetric("LogEndOffset", tags)
    removeMetric("Size", tags)
  }
  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   *
   * @param segment The segment to add
   */
  def addSegment(segment: LogSegment) = this.segments.put(segment.baseOffset, segment)

  private def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    try {
      fun
    } catch {
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir.getParent, msg, e)
        throw new KafkaStorageException(msg, e)
    }
  }

}

/**
 * Helper functions for logs
 */
object Log {

  /** a log file */
  val LogFileSuffix = ".log"

  /** an index file */
  val IndexFileSuffix = ".index"

  /** a time index file */
  val TimeIndexFileSuffix = ".timeindex"

  val ProducerSnapshotFileSuffix = ".snapshot"

  /** an (aborted) txn index */
  val TxnIndexFileSuffix = ".txnindex"

  /** a file that is scheduled to be deleted */
  val DeletedFileSuffix = ".deleted"

  /** A temporary file that is being used for log cleaning */
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8 and higher.
   * This is used to avoid unnecessary recovery after a clean shutdown. In theory this could be
   * avoided by passing in the recovery point, however finding the correct position to do this
   * requires accessing the offset index which may not be safe in an unclean shutdown.
   * For more information see the discussion in PR#2104
   */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /** a directory that is scheduled to be deleted */
  val DeleteDirSuffix = "-delete"

  /** a directory that is used for future partition */
  val FutureDirSuffix = "-future"

  private val DeleteDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$DeleteDirSuffix")
  private val FutureDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$FutureDirSuffix")

  val UnknownLogStartOffset = -1L

  def apply(dir: File,
            config: LogConfig,
            logStartOffset: Long,
            recoveryPoint: Long,
            scheduler: Scheduler,
            brokerTopicStats: BrokerTopicStats,
            time: Time = Time.SYSTEM,
            maxProducerIdExpirationMs: Int,
            producerIdExpirationCheckIntervalMs: Int,
            logDirFailureChannel: LogDirFailureChannel): Log = {
    val topicPartition = Log.parseTopicPartitionName(dir)
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    new Log(dir, config, logStartOffset, recoveryPoint, scheduler, brokerTopicStats, time, maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs, topicPartition, producerStateManager, logDirFailureChannel)
  }

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   *
   * @param offset The offset to use in the file name
   * @return The filename
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }

  /**
   * Construct a log file name in the given dir with the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name (e.g. "", ".deleted", ".cleaned", ".swap", etc.)
   */
  def logFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix + suffix)

  /**
    * Return a directory name to rename the log directory to for async deletion. The name will be in the following
    * format: topic-partition.uniqueId-delete where topic, partition and uniqueId are variables.
    */
  def logDeleteDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffix(topicPartition, DeleteDirSuffix)
  }

  /**
    * Return a future directory name for the given topic partition. The name will be in the following
    * format: topic-partition.uniqueId-future where topic, partition and uniqueId are variables.
    */
  def logFutureDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffix(topicPartition, FutureDirSuffix)
  }

  private def logDirNameWithSuffix(topicPartition: TopicPartition, suffix: String): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    s"${logDirName(topicPartition)}.$uniqueId$suffix"
  }

  /**
    * Return a directory name for the given topic partition. The name will be in the following
    * format: topic-partition where topic, partition are variables.
    */
  def logDirName(topicPartition: TopicPartition): String = {
    s"${topicPartition.topic}-${topicPartition.partition}"
  }

  /**
   * Construct an index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def offsetIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix + suffix)

  /**
   * Construct a time index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def timeIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix + suffix)

  /**
   * Construct a producer id snapshot file using the given offset.
   *
   * @param dir The directory in which the log will reside
   * @param offset The last offset (exclusive) included in the snapshot
   */
  def producerSnapshotFile(dir: File, offset: Long): File =
    new File(dir, filenamePrefixFromOffset(offset) + ProducerSnapshotFileSuffix)

  /**
   * Construct a transaction index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def transactionIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TxnIndexFileSuffix + suffix)

  def offsetFromFile(file: File): Long = {
    val filename = file.getName
    filename.substring(0, filename.indexOf('.')).toLong
  }

  /**
    * Calculate a log's size (in bytes) based on its log segments
    *
    * @param segments The log segments to calculate the size of
    * @return Sum of the log segments' sizes (in bytes)
    */
  def sizeInBytes(segments: Iterable[LogSegment]): Long =
    segments.map(_.size.toLong).sum

  /**
   * Parse the topic and partition out of the directory name of a log
   */
  def parseTopicPartitionName(dir: File): TopicPartition = {
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
        dirName.endsWith(FutureDirSuffix) && !FutureDirPattern.matcher(dirName).matches)
      throw exception(dir)

    val name: String =
      if (dirName.endsWith(DeleteDirSuffix) || dirName.endsWith(FutureDirSuffix)) dirName.substring(0, dirName.lastIndexOf('.'))
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

  private def isIndexFile(file: File): Boolean = {
    val filename = file.getName
    filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix) || filename.endsWith(TxnIndexFileSuffix)
  }

  private def isLogFile(file: File): Boolean =
    file.getPath.endsWith(LogFileSuffix)

}
