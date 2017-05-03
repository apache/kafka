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
import java.nio.file.Files
import java.text.NumberFormat
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, TimeUnit}

import kafka.api.KAFKA_0_10_0_IV0
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerTopicStats, FetchDataInfo, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.errors.{CorruptRecordException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{IsolationLevel, ListOffsetRequest}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.utils.{Time, Utils}
import kafka.message.{BrokerCompressionCodec, CompressionCodec, NoCompressionCodec}
import kafka.server.checkpoints.{LeaderEpochCheckpointFile, LeaderEpochFile}
import kafka.server.epoch.{LeaderEpochCache, LeaderEpochFileCache}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction

import java.util.Map.{Entry => JEntry}
import java.lang.{Long => JLong}

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(-1, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP,
    NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false,
    producerAppendInfos = Map.empty[Long, ProducerAppendInfo],
    completedTransactions = Nil,
    isDuplicate = false)
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 *
 * @param firstOffset The first offset in the message set
 * @param lastOffset The last offset in the message set
 * @param maxTimestamp The maximum timestamp of the message set.
 * @param offsetOfMaxTimestamp The offset of the message with the maximum timestamp.
 * @param logAppendTime The log append time (if used) of the message set, otherwise Message.NoTimestamp
 * @param sourceCodec The source codec used in the message set (send by the producer)
 * @param targetCodec The target codec of the message set(after applying the broker compression configuration if any)
 * @param shallowCount The number of shallow messages
 * @param validBytes The number of valid bytes
 * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
 * @param producerAppendInfos A map from PID to a ProducerAppendInfo, which is used to validate the producer
 *                            metadata (i.e. epoch and sequence number) for all produced or replicated data
 * @param completedTransactions A list of all transactions completed in this append. This list is sorted by the
 *                              last offset of the transactions and is used to keep the transaction index up to date.
 * @param isDuplicate Indicates whether the message set is a duplicate of a message at the tail of the log.
 */
case class LogAppendInfo(var firstOffset: Long,
                         var lastOffset: Long,
                         var maxTimestamp: Long,
                         var offsetOfMaxTimestamp: Long,
                         var logAppendTime: Long,
                         sourceCodec: CompressionCodec,
                         targetCodec: CompressionCodec,
                         shallowCount: Int,
                         validBytes: Int,
                         offsetsMonotonic: Boolean,
                         producerAppendInfos: Map[Long, ProducerAppendInfo],
                         completedTransactions: List[CompletedTxn] = null,
                         isDuplicate: Boolean = false)

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
case class CompletedTxn(producerId: Long,
                        firstOffset: Long,
                        lastOffset: Long,
                        isAborted: Boolean) extends Ordered[CompletedTxn] {
  override def compare(that: CompletedTxn): Int = {
    val res = this.firstOffset compare that.firstOffset
    if (res == 0)
      this.producerId compare that.producerId
    else
      res
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
 * @param time The time instance used for checking the clock
 * @param maxPidExpirationMs The maximum amount of time to wait before a PID is considered expired
 * @param pidExpirationCheckIntervalMs How often to check for PIDs which need to be expired
 */
@threadsafe
class Log(@volatile var dir: File,
          @volatile var config: LogConfig,
          @volatile var logStartOffset: Long = 0L,
          @volatile var recoveryPoint: Long = 0L,
          scheduler: Scheduler,
          time: Time = Time.SYSTEM,
          val maxPidExpirationMs: Int = 60 * 60 * 1000,
          val pidExpirationCheckIntervalMs: Int = 10 * 60 * 1000) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._

  /* A lock that guards all modifications to the log */
  private val lock = new Object

  /* last time it was flushed */
  private val lastflushedTime = new AtomicLong(time.milliseconds)

  def initFileSize() : Int = {
    if (config.preallocate)
      config.segmentSize
    else
      0
  }

  val topicPartition: TopicPartition = Log.parseTopicPartitionName(dir)

  @volatile private var nextOffsetMetadata: LogOffsetMetadata = _

  /* The earliest offset which is part of an incomplete transaction. This is used to compute the LSO. */
  @volatile var firstUnstableOffset: Option[LogOffsetMetadata] = None

  private val producerStateManager = new ProducerStateManager(config, topicPartition, dir, maxPidExpirationMs)

  /* the actual segments of the log */
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]

  val leaderEpochCache: LeaderEpochCache = initializeLeaderEpochCache()

  locally {
    val startMs = time.milliseconds

    loadSegments()

    /* Calculate the offset of the next message */
    nextOffsetMetadata = new LogOffsetMetadata(activeSegment.nextOffset, activeSegment.baseOffset,
      activeSegment.size.toInt)

    leaderEpochCache.clearLatest(nextOffsetMetadata.messageOffset)

    logStartOffset = math.max(logStartOffset, segments.firstEntry().getValue.baseOffset)

    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    leaderEpochCache.clearEarliest(logStartOffset)

    buildAndRecoverPidMap(logEndOffset)

    info("Completed load of log %s with %d log segments, log start offset %d and log end offset %d in %d ms"
      .format(name, segments.size(), logStartOffset, logEndOffset, time.milliseconds - startMs))
  }

  private val tags = Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString)

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
  }, period = pidExpirationCheckIntervalMs, unit = TimeUnit.MILLISECONDS)

  /** The name of this log */
  def name  = dir.getName()

  private def initializeLeaderEpochCache(): LeaderEpochCache = {
    // create the log directory if it doesn't exist
    Files.createDirectories(dir.toPath)
    new LeaderEpochFileCache(topicPartition, () => logEndOffsetMetadata,
      new LeaderEpochCheckpointFile(LeaderEpochFile.newFile(dir)))
  }

  private def removeTempFilesAndCollectSwapFiles(): Set[File] = {
    var swapFiles = Set[File]()

    for (file <- dir.listFiles if file.isFile) {
      if(!file.canRead)
        throw new IOException("Could not read file " + file)
      val filename = file.getName
      if(filename.endsWith(DeletedFileSuffix) || filename.endsWith(CleanedFileSuffix)) {
        // if the file ends in .deleted or .cleaned, delete it
        Files.deleteIfExists(file.toPath)
      } else if(filename.endsWith(SwapFileSuffix)) {
        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the .index file, complete the swap operation later
        // if an index just delete it, it will be rebuilt
        val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        if (isIndexFile(baseFile)) {
          Files.deleteIfExists(file.toPath)
        } else if (isLogFile(baseFile)) {
          // delete the index files
          val offset = offsetFromFilename(baseFile.getName)
          Files.deleteIfExists(Log.indexFilename(dir, offset).toPath)
          Files.deleteIfExists(Log.timeIndexFilename(dir, offset).toPath)
          Files.deleteIfExists(Log.txnIndexFilename(dir, offset).toPath)
          swapFiles += file
        }
      }
    }
    swapFiles
  }

  private def loadSegmentFiles(): Unit = {
    // load segments in ascending order because transactional data from one segment may depend on the
    // segments that come before it
    for (file <- dir.listFiles.sortBy(_.getName) if file.isFile) {
      val filename = file.getName
      if (isIndexFile(file)) {
        // if it is an index file, make sure it has a corresponding .log file
        val offset = offsetFromFilename(filename)
        val logFile = logFilename(dir, offset)
        if (!logFile.exists) {
          warn("Found an orphaned index file, %s, with no corresponding log file.".format(file.getAbsolutePath))
          Files.deleteIfExists(file.toPath)
        }
      } else if (isLogFile(file)) {
        // if it's a log file, load the corresponding log segment
        val startOffset = offsetFromFilename(filename)
        val indexFile = Log.indexFilename(dir, startOffset)
        val timeIndexFile = Log.timeIndexFilename(dir, startOffset)

        val indexFileExists = indexFile.exists()
        val timeIndexFileExists = timeIndexFile.exists()
        val segment = new LogSegment(dir = dir,
          startOffset = startOffset,
          indexIntervalBytes = config.indexInterval,
          maxIndexSize = config.maxIndexSize,
          rollJitterMs = config.randomSegmentJitter,
          time = time,
          fileAlreadyExists = true)

        if (indexFileExists) {
          try {
            segment.index.sanityCheck()
            // Resize the time index file to 0 if it is newly created.
            if (!timeIndexFileExists)
              segment.timeIndex.resize(0)
            segment.timeIndex.sanityCheck()
            segment.txnIndex.sanityCheck()
          } catch {
            case e: java.lang.IllegalArgumentException =>
              warn(s"Found a corrupted index file due to ${e.getMessage}}. deleting ${timeIndexFile.getAbsolutePath}, " +
                s"${indexFile.getAbsolutePath} and rebuilding index...")
              Files.deleteIfExists(timeIndexFile.toPath)
              Files.delete(indexFile.toPath)
              recoverSegment(segment)
          }
        } else {
          error("Could not find index file corresponding to log file %s, rebuilding index...".format(segment.log.file.getAbsolutePath))
          recoverSegment(segment)
        }
        segments.put(startOffset, segment)
      }
    }
  }

  private def recoverSegment(segment: LogSegment, leaderEpochCache: Option[LeaderEpochCache] = None): Int = {
    val stateManager = new ProducerStateManager(config, topicPartition, dir, maxPidExpirationMs)
    stateManager.truncateAndReload(logStartOffset, segment.baseOffset, time.milliseconds)
    logSegments(stateManager.mapEndOffset, segment.baseOffset).foreach { segment =>
      val startOffset = math.max(segment.baseOffset, stateManager.mapEndOffset)
      val fetchDataInfo = segment.read(startOffset, Some(segment.baseOffset), Int.MaxValue)
      if (fetchDataInfo != null)
        loadPidsFromLog(stateManager, fetchDataInfo.records)
    }

    // Once we have loaded the segment's data, we take a snapshot to ensure that we won't
    // need to reload the same segments a second time if we have to recover another segment.
    stateManager.updateMapEndOffset(segment.baseOffset)
    stateManager.takeSnapshot()

    val activeProducers = stateManager.activeProducers.values
    segment.recover(config.maxMessageSize, activeProducers, leaderEpochCache)
  }

  private def completeSwapOperations(swapFiles: Set[File]): Unit = {
    for (swapFile <- swapFiles) {
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      val filename = logFile.getName
      val startOffset = offsetFromFilename(filename)
      val indexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, IndexFileSuffix) + SwapFileSuffix)
      val index =  new OffsetIndex(indexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
      val timeIndexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, TimeIndexFileSuffix) + SwapFileSuffix)
      val timeIndex = new TimeIndex(timeIndexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
      val txnIndexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, TxnIndexFileSuffix) + SwapFileSuffix)
      val txnIndex = new TransactionIndex(startOffset, txnIndexFile)
      val swapSegment = new LogSegment(FileRecords.open(swapFile),
        index = index,
        timeIndex = timeIndex,
        txnIndex = txnIndex,
        baseOffset = startOffset,
        indexIntervalBytes = config.indexInterval,
        rollJitterMs = config.randomSegmentJitter,
        time = time)
      info("Found log file %s from interrupted swap operation, repairing.".format(swapFile.getPath))
      recoverSegment(swapSegment)
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.nextOffset())
      replaceSegments(swapSegment, oldSegments.toSeq, isRecoveredSwapFile = true)
    }
  }

  /* Load the log segments from the log files on disk */
  private def loadSegments() {
    // first do a pass through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    // now do a second pass and load all the log and index files
    loadSegmentFiles()

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    completeSwapOperations(swapFiles)

    if(logSegments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at offset 0
      segments.put(0L, new LogSegment(dir = dir,
                                     startOffset = 0,
                                     indexIntervalBytes = config.indexInterval,
                                     maxIndexSize = config.maxIndexSize,
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time,
                                     fileAlreadyExists = false,
                                     initFileSize = this.initFileSize(),
                                     preallocate = config.preallocate))
    } else if (!dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {
      recoverLog()
      // reset the index size of the currently active log segment to allow more entries
      activeSegment.index.resize(config.maxIndexSize)
      activeSegment.timeIndex.resize(config.maxIndexSize)
    }
  }

  private def updateLogEndOffset(messageOffset: Long) {
    nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size.toInt)
  }

  private def recoverLog() {
    // if we have the clean shutdown marker, skip recovery
    if(hasCleanShutdownFile) {
      this.recoveryPoint = activeSegment.nextOffset()
      return
    }

    // okay we need to actually recovery this log
    val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
    while(unflushed.hasNext) {
      val segment = unflushed.next
      info("Recovering unflushed segment %d in log %s.".format(segment.baseOffset, name))
      val truncatedBytes =
        try {
          recoverSegment(segment, Some(leaderEpochCache))
        } catch {
          case _: InvalidOffsetException =>
            val startOffset = segment.baseOffset
            warn("Found invalid offset during recovery for log " + dir.getName +". Deleting the corrupt segment and " +
                 "creating an empty one with starting offset " + startOffset)
            segment.truncateTo(startOffset)
        }
      if(truncatedBytes > 0) {
        // we had an invalid message, delete all remaining log
        warn("Corruption found in segment %d of log %s, truncating to offset %d.".format(segment.baseOffset, name,
          segment.nextOffset()))
        unflushed.foreach(deleteSegment)
      }
    }
  }
  private def buildAndRecoverPidMap(lastOffset: Long): Unit = {
    buildAndRecoverPidMap(producerStateManager, lastOffset)
    updateFirstUnstableOffset()
  }

  private def buildAndRecoverPidMap(producerStateManager: ProducerStateManager, lastOffset: Long) {
    info(s"Recovering PID mapping from offset $lastOffset for partition $topicPartition")
    val currentTimeMs = time.milliseconds
    producerStateManager.truncateAndReload(logStartOffset, lastOffset, currentTimeMs)
    logSegments(producerStateManager.mapEndOffset, lastOffset).foreach { segment =>
      val startOffset = math.max(segment.baseOffset, producerStateManager.mapEndOffset)
      val fetchDataInfo = segment.read(startOffset, Some(lastOffset), Int.MaxValue)
      if (fetchDataInfo != null)
        loadPidsFromLog(producerStateManager, fetchDataInfo.records)
    }
    producerStateManager.updateMapEndOffset(lastOffset)
  }

  private def loadPidsFromLog(producerStateManager: ProducerStateManager, records: Records): Unit = {
    val pidsToLoad = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    records.batches.asScala.foreach { batch =>
      if (batch.hasProducerId) {
        val lastEntry = producerStateManager.lastEntry(batch.producerId)
        updateProducer(batch, pidsToLoad, completedTxns, lastEntry, assignOffsets = false, loadingFromLog = true)
      }
    }
    pidsToLoad.values.foreach(producerStateManager.update)
    completedTxns.foreach(producerStateManager.completeTxn)
  }

  private[log] def activePids: Map[Long, ProducerIdEntry] = lock synchronized {
    producerStateManager.activeProducers
  }

  /**
   * Check if we have the "clean shutdown" file
   */
  private def hasCleanShutdownFile = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log
   */
  def close() {
    debug(s"Closing log $name")
    lock synchronized {
      producerStateManager.takeSnapshot()
      logSegments.foreach(_.close())
    }
  }

  /**
   * Append this message set to the active segment of the log, assigning offsets and Partition Leader Epochs
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
  private def append(records: MemoryRecords, isFromClient: Boolean, assignOffsets: Boolean = true, leaderEpoch: Int): LogAppendInfo = {
    val appendInfo = analyzeAndValidateRecords(records, assignOffsets)

    // return if we have no valid messages or if this is a duplicate of the last appended entry
    if (appendInfo.shallowCount == 0 || appendInfo.isDuplicate)
      return appendInfo

    // trim any invalid bytes or partial messages before appending it to the on-disk log
    var validRecords = trimInvalidBytes(records, appendInfo)

    try {
      // they are valid, insert them in the log
      lock synchronized {

        if (assignOffsets) {
          // assign offsets to the message set
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          appendInfo.firstOffset = offset.value
          val now = time.milliseconds
          val validateAndOffsetAssignResult = try {
            LogValidator.validateMessagesAndAssignOffsets(validRecords,
              offset,
              now,
              appendInfo.sourceCodec,
              appendInfo.targetCodec,
              config.compact,
              config.messageFormatVersion.messageFormatVersion,
              config.messageTimestampType,
              config.messageTimestampDifferenceMaxMs,
              leaderEpoch,
              isFromClient)
          } catch {
            case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
          }
          validRecords = validateAndOffsetAssignResult.validatedRecords
          appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
          appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
          appendInfo.lastOffset = offset.value - 1
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.logAppendTime = now

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          if (validateAndOffsetAssignResult.messageSizeMaybeChanged) {
            for (batch <- validRecords.batches.asScala) {
              if (batch.sizeInBytes > config.maxMessageSize) {
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                throw new RecordTooLargeException("Message batch size is %d bytes which exceeds the maximum configured size of %d."
                  .format(batch.sizeInBytes, config.maxMessageSize))
              }
            }
          }
        } else {
          // we are taking the offsets we are given
          if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
            throw new IllegalArgumentException("Out of order offsets found in " + records.records.asScala.map(_.offset))
        }

        // update the epoch cache with the epoch stamped onto the message by the leader
        validRecords.batches.asScala.foreach { batch =>
          if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
            leaderEpochCache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
        }

        // check messages set size may be exceed config.segmentSize
        if (validRecords.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException("Message batch size is %d bytes which exceeds the maximum configured segment size of %d."
            .format(validRecords.sizeInBytes, config.segmentSize))
        }

        // maybe roll the log if this segment is full
        val segment = maybeRoll(messagesSize = validRecords.sizeInBytes,
          maxTimestampInMessages = appendInfo.maxTimestamp,
          maxOffsetInMessages = appendInfo.lastOffset)

        segment.append(firstOffset = appendInfo.firstOffset,
          largestOffset = appendInfo.lastOffset,
          largestTimestamp = appendInfo.maxTimestamp,
          shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
          records = validRecords)

        // update the producer state
        for ((producerId, producerAppendInfo) <- appendInfo.producerAppendInfos) {
          if (assignOffsets)
            // the timestamp may have been overwritten with the log append time, so update the value here
            producerAppendInfo.assignTimestamp(appendInfo.maxTimestamp)
          trace(s"Updating pid with sequence: $producerId -> ${producerAppendInfo.lastEntry}")
          producerStateManager.update(producerAppendInfo)
        }

        // update the transaction index and last stable offset. The ordering is important.
        // The index must be updated before we can advance the LSO, but the LSO value must take
        // into account the completed transaction.
        for (completedTxn <- appendInfo.completedTransactions) {
          val firstUnstableOffset = producerStateManager.firstUndecidedOffsetExcluding(completedTxn)
          val lastStableOffset = firstUnstableOffset.getOrElse(completedTxn.lastOffset + 1)
          segment.updateTxnIndex(completedTxn, lastStableOffset)
          producerStateManager.completeTxn(completedTxn)
        }

        // always update the last pid map offset so that the snapshot reflects the current offset
        // even if there isn't any idempotent data being written
        producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

        // increment the log end offset
        updateLogEndOffset(appendInfo.lastOffset + 1)

        // update the first unstable offset (which is used to compute LSO)
        updateFirstUnstableOffset()

        trace("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s"
          .format(this.name, appendInfo.firstOffset, nextOffsetMetadata.messageOffset, validRecords))

        if (unflushedMessages >= config.flushInterval)
          flush()

        appendInfo
      }
    } catch {
      case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
    }
  }

  def onHighWatermarkIncremented(highWatermark: Long): Unit = {
    lock synchronized {
      producerStateManager.onHighWatermarkUpdated(highWatermark)
      updateFirstUnstableOffset()
    }
  }

  private def updateFirstUnstableOffset(): Unit = {
    this.firstUnstableOffset = producerStateManager.firstUnstableOffset.flatMap { offset =>
      if (firstUnstableOffset.exists(_.messageOffset == offset)) {
        firstUnstableOffset
      } else {
        val segment = segments.floorEntry(offset).getValue
        val position  = segment.translateOffset(offset)
        Some(LogOffsetMetadata(offset, segment.baseOffset, position.position))
      }
    }
  }

  /**
   * Increment the log start offset if the provided offset is larger.
   */
  def maybeIncrementLogStartOffset(offset: Long) {
    // We don't have to write the log start offset to log-start-offset-checkpoint immediately.
    // The deleteRecordsOffset may be lost only if all in-sync replicas of this broker are shutdown
    // in an unclean manner within log.flush.start.offset.checkpoint.interval.ms. The chance of this happening is low.
    lock synchronized {
      if (offset > logStartOffset) {
        logStartOffset = offset
      }
    }
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
  private def analyzeAndValidateRecords(records: MemoryRecords, assignOffsets: Boolean): LogAppendInfo = {
    var shallowMessageCount = 0
    var validBytesCount = 0
    var firstOffset = -1L
    var lastOffset = -1L
    var sourceCodec: CompressionCodec = NoCompressionCodec
    var monotonic = true
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    val producerAppendInfos = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]

    for (batch <- records.batches.asScala) {
      // we only validate V2 and higher to avoid potential compatibility issues with older clients
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2 && assignOffsets && batch.baseOffset != 0)
        throw new InvalidRecordException(s"The baseOffset of the record batch should be 0, but it is ${batch.baseOffset}")

      // update the first offset if on the first message. For magic versions older than 2, we use the last offset
      // to avoid the need to decompress the data (the last offset can be obtained directly from the wrapper message).
      // For magic version 2, we can get the first offset directly from the batch header.
      if (firstOffset < 0)
        firstOffset = if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) batch.baseOffset else batch.lastOffset

      // check that offsets are monotonically increasing
      if (lastOffset >= batch.lastOffset)
        monotonic = false

      // update the last offset seen
      lastOffset = batch.lastOffset

      // Check if the message sizes are valid.
      val batchSize = batch.sizeInBytes
      if (batchSize > config.maxMessageSize) {
        BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        throw new RecordTooLargeException(s"The record batch size is $batchSize bytes which exceeds the maximum configured " +
          s"value of ${config.maxMessageSize}.")
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

      if (batch.hasProducerId) {
        if (assignOffsets) {
          val pid = batch.producerId
          val isDuplicate = producerStateManager.lastEntry(pid) match {
            case Some(lastEntry) if lastEntry.isDuplicate(batch) =>
              // This request is a duplicate so return the information about the existing entry. Note that for requests
              // coming from the client, there will only be one RecordBatch per request, so there will be only one iteration
              // of the loop and the values below will not be updated more than once.
              firstOffset = lastEntry.firstOffset
              lastOffset = lastEntry.lastOffset
              maxTimestamp = lastEntry.timestamp
              true

            case maybeLastEntry =>
              updateProducer(batch, producerAppendInfos, completedTxns, maybeLastEntry, assignOffsets = true,
                loadingFromLog = false)
              false
          }

          val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)
          return LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, RecordBatch.NO_TIMESTAMP,
            sourceCodec, targetCodec, shallowMessageCount, validBytesCount, monotonic, producerAppendInfos.toMap,
            completedTxns.toList, isDuplicate = isDuplicate)
        } else {
          val lastEntry = producerStateManager.lastEntry(batch.producerId)
          updateProducer(batch, producerAppendInfos, completedTxns, lastEntry, assignOffsets = false,
            loadingFromLog = false)
        }
      }
    }

    // Apply broker-side compression if any
    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)
    LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, RecordBatch.NO_TIMESTAMP, sourceCodec,
      targetCodec, shallowMessageCount, validBytesCount, monotonic, producerAppendInfos.toMap, completedTxns.toList,
      isDuplicate = false)
  }

  private def updateProducer(batch: RecordBatch,
                             producers: mutable.Map[Long, ProducerAppendInfo],
                             completedTxns: ListBuffer[CompletedTxn],
                             lastEntry: Option[ProducerIdEntry],
                             assignOffsets: Boolean,
                             loadingFromLog: Boolean): Unit = {
    val pid = batch.producerId
    val (firstOffset, lastOffset) = if (assignOffsets)
      (nextOffsetMetadata.messageOffset, nextOffsetMetadata.messageOffset + batch.countOrNull - 1)
    else
      (batch.baseOffset, batch.lastOffset)

    val appendInfo = producers.getOrElse(pid, {
      val appendInfo = new ProducerAppendInfo(pid, lastEntry, loadingFromLog)
      producers.put(pid, appendInfo)
      appendInfo
    })

    if (batch.baseSequence == RecordBatch.CONTROL_SEQUENCE) {
      val record = batch.iterator.next()
      val controlRecordType = ControlRecordType.parse(record.key)
      val completedTxn = appendInfo.appendControlRecord(controlRecordType, batch.producerEpoch, firstOffset,
        record.timestamp)
      completedTxns += completedTxn
    } else {
      appendInfo.append(batch.producerEpoch, batch.baseSequence, batch.lastSequence, batch.maxTimestamp, lastOffset,
        batch.isTransactional)
    }
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
      throw new CorruptRecordException("Illegal length of message set " + validBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests")
    if (validBytes == records.sizeInBytes) {
      records
    } else {
      // trim invalid bytes
      val validByteBuffer = records.buffer.duplicate()
      validByteBuffer.limit(validBytes)
      MemoryRecords.readableRecords(validByteBuffer)
    }
  }

  /**
   * Read messages from the log.
   *
   * @param startOffset The offset to begin reading at
   * @param maxLength The maximum number of bytes to read
   * @param maxOffset The offset to read up to, exclusive. (i.e. this offset NOT included in the resulting message set)
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   *
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None,
           minOneMessage: Boolean = false,
           isolationLevel: IsolationLevel = IsolationLevel.READ_UNCOMMITTED): FetchDataInfo = {
    trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))

    // Because we don't use lock for reading, the synchronization is a little bit tricky.
    // We create the local variables to avoid race conditions with updates to the log.
    val currentNextOffsetMetadata = nextOffsetMetadata
    val next = currentNextOffsetMetadata.messageOffset
    if(startOffset == next)
      return FetchDataInfo(currentNextOffsetMetadata, MemoryRecords.EMPTY)

    var segmentEntry = segments.floorEntry(startOffset)

    // return error on attempt to read beyond the log end offset or read below log start offset
    if(startOffset > next || segmentEntry == null || startOffset < logStartOffset)
      throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, logStartOffset, next))

    // Do the read on the segment with a base offset less than the target offset
    // but if that segment doesn't contain any messages with an offset greater than that
    // continue to read from successive segments until we get some messages or we reach the end of the log
    while(segmentEntry != null) {
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
    val abortedTransactions = collectAbortedTransactions(startOffset, upperBoundOffset, segmentEntry)
    FetchDataInfo(fetchOffsetMetadata = fetchInfo.fetchOffsetMetadata,
      records = fetchInfo.records,
      firstEntryIncomplete = fetchInfo.firstEntryIncomplete,
      abortedTransactions = Some(abortedTransactions))
  }

  private def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long,
                                         startingSegmentEntry: JEntry[JLong, LogSegment]): List[AbortedTransaction] = {
    var segmentEntry = startingSegmentEntry
    val abortedTransactions = ListBuffer.empty[AbortedTransaction]

    while (segmentEntry != null) {
      val searchResult = segmentEntry.getValue.collectAbortedTxns(startOffset, upperBoundOffset)
      abortedTransactions ++= searchResult.abortedTransactions
      if (searchResult.isComplete)
        return abortedTransactions.toList

      segmentEntry = segments.higherEntry(segmentEntry.getKey)
    }
    abortedTransactions.toList
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

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, return unknown offset metadata
   */
  def convertToOffsetMetadata(offset: Long): LogOffsetMetadata = {
    try {
      val fetchDataInfo = read(offset, 1)
      fetchDataInfo.fetchOffsetMetadata
    } catch {
      case _: OffsetOutOfRangeException => LogOffsetMetadata.UnknownOffsetMetadata
    }
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   *
   * @param predicate A function that takes in a single log segment and returns true iff it is deletable
   * @return The number of segments deleted
   */
  private def deleteOldSegments(predicate: LogSegment => Boolean): Int = {
    lock synchronized {
      val deletable = deletableSegments(predicate)
      deleteSegments(deletable)
    }
  }

  private def deleteSegments(deletable: Iterable[LogSegment]): Int = {
    val numToDelete = deletable.size
    if (numToDelete > 0) {
      // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
      if (segments.size == numToDelete)
        roll()
      lock synchronized {
        // remove the segments for lookups
        deletable.foreach(deleteSegment)
        logStartOffset = math.max(logStartOffset, segments.firstEntry().getValue.baseOffset)
        leaderEpochCache.clearEarliest(logStartOffset)
        producerStateManager.evictUnretainedPids(logStartOffset)
      }
    }
    numToDelete
  }

  /**
    * Find segments starting from the oldest until the user-supplied predicate is false.
    * A final segment that is empty will never be returned (since we would just end up re-creating it).
    * @param predicate A function that takes in a single log segment and returns true iff it is deletable
    * @return the segments ready to be deleted
    */
  private def deletableSegments(predicate: LogSegment => Boolean) = {
    val lastEntry = segments.lastEntry
    if (lastEntry == null) Seq.empty
    else logSegments.takeWhile(s => predicate(s) && (s.baseOffset != lastEntry.getValue.baseOffset || s.size > 0))
  }

  /**
    * Delete any log segments that have either expired due to time based retention
    * or because the log size is > retentionSize
    */
  def deleteOldSegments(): Int = {
    if (!config.delete) return 0
    deleteRetentionMsBreachedSegments() + deleteRetentionSizeBreachedSegments() + deleteLogStartOffsetBreachedSegments()
  }

  private def deleteRetentionMsBreachedSegments() : Int = {
    if (config.retentionMs < 0) return 0
    val startMs = time.milliseconds
    deleteOldSegments(startMs - _.largestTimestamp > config.retentionMs)
  }

  private def deleteRetentionSizeBreachedSegments() : Int = {
    if (config.retentionSize < 0 || size < config.retentionSize) return 0
    var diff = size - config.retentionSize
    def shouldDelete(segment: LogSegment) = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }
    deleteOldSegments(shouldDelete)
  }

  private def deleteLogStartOffsetBreachedSegments() : Int = {
    // keep active segment to avoid frequent log rolling due to user's DeleteRecordsRequest
    lock synchronized {
      val deletable = {
        if (segments.size() < 2)
          Seq.empty
        else
          logSegments.sliding(2).takeWhile { iterable =>
            val nextSegment = iterable.toSeq(1)
            nextSegment.baseOffset <= logStartOffset
          }.map(_.toSeq(0)).toSeq
      }
      deleteSegments(deletable)
    }
  }

  /**
   * The size of the log in bytes
   */
  def size: Long = logSegments.map(_.size).sum

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
   * @param messagesSize The messages set size in bytes
   * @param maxTimestampInMessages The maximum timestamp in the messages.
   * logSegment will be rolled if one of the following conditions met
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed since the timestamp of first message in the segment (or since the create time if
   * the first message does not have a timestamp)
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   */
  private def maybeRoll(messagesSize: Int, maxTimestampInMessages: Long, maxOffsetInMessages: Long): LogSegment = {
    val segment = activeSegment
    val now = time.milliseconds
    val reachedRollMs = segment.timeWaitedForRoll(now, maxTimestampInMessages) > config.segmentMs - segment.rollJitterMs
    if (segment.size > config.segmentSize - messagesSize ||
        (segment.size > 0 && reachedRollMs) ||
        segment.index.isFull || segment.timeIndex.isFull || !segment.canConvertToRelativeOffset(maxOffsetInMessages)) {
      debug(s"Rolling new log segment in $name (log_size = ${segment.size}/${config.segmentSize}}, " +
          s"index_size = ${segment.index.entries}/${segment.index.maxEntries}, " +
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
       */
      roll(maxOffsetInMessages - Integer.MAX_VALUE)
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
    val start = time.nanoseconds
    lock synchronized {
      val newOffset = math.max(expectedNextOffset, logEndOffset)
      val logFile = logFilename(dir, newOffset)
      val indexFile = indexFilename(dir, newOffset)
      val timeIndexFile = timeIndexFilename(dir, newOffset)
      val txnIndexFile = txnIndexFilename(dir, newOffset)
      for(file <- List(logFile, indexFile, timeIndexFile, txnIndexFile) if file.exists) {
        warn("Newly rolled segment file " + file.getName + " already exists; deleting it first")
        file.delete()
      }

      segments.lastEntry() match {
        case null =>
        case entry => {
          val seg = entry.getValue
          seg.onBecomeInactiveSegment()
          seg.index.trimToValidSize()
          seg.timeIndex.trimToValidSize()
          seg.log.trim()
        }
      }

      // take a snapshot of the producer states to facilitate recovery
      producerStateManager.takeSnapshot()

      val segment = new LogSegment(dir,
                                   startOffset = newOffset,
                                   indexIntervalBytes = config.indexInterval,
                                   maxIndexSize = config.maxIndexSize,
                                   rollJitterMs = config.randomSegmentJitter,
                                   time = time,
                                   fileAlreadyExists = false,
                                   initFileSize = initFileSize,
                                   preallocate = config.preallocate)
      val prev = addSegment(segment)
      if(prev != null)
        throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.".format(name, newOffset))
      // We need to update the segment base offset and append position data of the metadata when log rolls.
      // The next offset should not change.
      updateLogEndOffset(nextOffsetMetadata.messageOffset)
      // schedule an asynchronous flush of the old segment
      scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

      info("Rolled new log segment for '" + name + "' in %.0f ms.".format((System.nanoTime - start) / (1000.0*1000.0)))

      segment
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  def unflushedMessages() = this.logEndOffset - this.recoveryPoint

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
    if (offset <= this.recoveryPoint)
      return
    debug("Flushing log '" + name + " up to offset " + offset + ", last flushed: " + lastFlushTime + " current time: " +
          time.milliseconds + " unflushed = " + unflushedMessages)
    for(segment <- logSegments(this.recoveryPoint, offset))
      segment.flush()

    // always retain a snapshot from the start of the active segment
    val minSnapshotOffsetRetained = math.min(offset, activeSegment.baseOffset)
    producerStateManager.deleteSnapshotsBefore(minSnapshotOffsetRetained)

    lock synchronized {
      if(offset > this.recoveryPoint) {
        this.recoveryPoint = offset
        lastflushedTime.set(time.milliseconds)
      }
    }
  }

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  private[log] def delete() {
    lock synchronized {
      logSegments.foreach(_.delete())
      segments.clear()
      leaderEpochCache.clear()
      Utils.delete(dir)
    }
  }

  // visible for testing
  private[log] def takeProducerSnapshot(): Unit = producerStateManager.takeSnapshot()
  private[log] def latestProducerSnapshotOffset: Option[Long] = producerStateManager.latestSnapshotOffset
  private[log] def oldestProducerSnapshotOffset: Option[Long] = producerStateManager.oldestSnapshotOffset
  private[log] def latestPidMapOffset: Long = producerStateManager.mapEndOffset

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   *
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   */
  private[log] def truncateTo(targetOffset: Long) {
    if(targetOffset < 0)
      throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset))
    if(targetOffset >= logEndOffset) {
      info("Truncating %s to %d has no effect as the largest offset in the log is %d.".format(name, targetOffset, logEndOffset-1))
      return
    }
    info("Truncating log %s to offset %d.".format(name, targetOffset))
    lock synchronized {
      if(segments.firstEntry.getValue.baseOffset > targetOffset) {
        truncateFullyAndStartAt(targetOffset)
      } else {
        val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
        deletable.foreach(deleteSegment)
        activeSegment.truncateTo(targetOffset)
        updateLogEndOffset(targetOffset)
        this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
        this.logStartOffset = math.min(targetOffset, this.logStartOffset)
        leaderEpochCache.clearLatest(targetOffset)
        buildAndRecoverPidMap(targetOffset)
      }
    }
  }

  /**
   *  Delete all data in the log and start at the new offset
   *
   *  @param newOffset The new offset to start the log with
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long) {
    debug(s"Truncate and start log '$name' at offset $newOffset")
    lock synchronized {
      val segmentsToDelete = logSegments.toList
      segmentsToDelete.foreach(deleteSegment)
      addSegment(new LogSegment(dir,
                                newOffset,
                                indexIntervalBytes = config.indexInterval,
                                maxIndexSize = config.maxIndexSize,
                                rollJitterMs = config.randomSegmentJitter,
                                time = time,
                                fileAlreadyExists = false,
                                initFileSize = initFileSize,
                                preallocate = config.preallocate))
      updateLogEndOffset(newOffset)
      leaderEpochCache.clear()

      producerStateManager.truncate()
      producerStateManager.updateMapEndOffset(newOffset)
      updateFirstUnstableOffset()

      this.recoveryPoint = math.min(newOffset, this.recoveryPoint)
      this.logStartOffset = newOffset
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime(): Long = lastflushedTime.get

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
      val floor = segments.floorKey(from)
      if(floor eq null)
        segments.headMap(to).values.asScala
      else
        segments.subMap(floor, true, to, false).values.asScala
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
   * @param segment The log segment to schedule for deletion
   */
  private def deleteSegment(segment: LogSegment) {
    info("Scheduling log segment %d for log %s for deletion.".format(segment.baseOffset, name))
    lock synchronized {
      segments.remove(segment.baseOffset)
      asyncDeleteSegment(segment)
    }
  }

  /**
   * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
   *
   * @throws KafkaStorageException if the file can't be renamed and still exists
   */
  private def asyncDeleteSegment(segment: LogSegment) {
    segment.changeFileSuffixes("", Log.DeletedFileSuffix)
    def deleteSeg() {
      info("Deleting segment %d from log %s.".format(segment.baseOffset, name))
      segment.delete()
    }
    scheduler.schedule("delete-file", deleteSeg _, delay = config.fileDeleteDelayMs)
  }

  /**
   * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
   * be asynchronously deleted.
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
  private[log] def replaceSegments(newSegment: LogSegment, oldSegments: Seq[LogSegment], isRecoveredSwapFile : Boolean = false) {
    lock synchronized {
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      if (!isRecoveredSwapFile)
        newSegment.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix)
      addSegment(newSegment)

      // delete the old files
      for(seg <- oldSegments) {
        // remove the index entry
        if(seg.baseOffset != newSegment.baseOffset)
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

  val PidSnapshotFileSuffix = ".snapshot"

  /** an (aborted) txn index */
  val TxnIndexFileSuffix = ".txnindex"

  /** a file that is scheduled to be deleted */
  val DeletedFileSuffix = ".deleted"

  /** A temporary file that is being used for log cleaning */
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
   * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
  /** TODO: Get rid of CleanShutdownFile in 0.8.2 */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /** a directory that is scheduled to be deleted */
  val DeleteDirSuffix = "-delete"

  val UnknownLogStartOffset = -1L

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
   * Construct a log file name in the given dir with the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def logFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix)

  /**
   * Construct an index file name in the given dir using the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def indexFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix)

  /**
   * Construct a time index file name in the given dir using the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def timeIndexFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix)

  /**
   * Construct a PID snapshot file using the given offset.
   *
   * @param dir The directory in which the log will reside
   * @param offset The last offset (exclusive) included in the snapshot
   */
  def pidSnapshotFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + PidSnapshotFileSuffix)

  def txnIndexFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + TxnIndexFileSuffix)

  def offsetFromFilename(filename: String): Long =
    filename.substring(0, filename.indexOf('.')).toLong

  /**
   * Parse the topic and partition out of the directory name of a log
   */
  def parseTopicPartitionName(dir: File): TopicPartition = {

    def exception(dir: File): KafkaException = {
      new KafkaException("Found directory " + dir.getCanonicalPath + ", " +
        "'" + dir.getName + "' is not in the form of topic-partition or " +
        "ongoing-deleting directory(topic-partition.uniqueId-delete)\n" +
        "If a directory does not contain Kafka topic data it should not exist in Kafka's log " +
        "directory")
    }

    val dirName = dir.getName
    if (dirName == null || dirName.isEmpty || !dirName.contains('-'))
      throw exception(dir)
    if (dirName.endsWith(DeleteDirSuffix) && !dirName.matches("^(\\S+)-(\\S+)\\.(\\S+)" + DeleteDirSuffix))
      throw exception(dir)

    val name: String =
      if (dirName.endsWith(DeleteDirSuffix)) dirName.substring(0, dirName.indexOf('.'))
      else dirName

    val index = name.lastIndexOf('-')
    val topic = name.substring(0, index)
    val partition = name.substring(index + 1)
    if (topic.length < 1 || partition.length < 1)
      throw exception(dir)

    new TopicPartition(topic, partition.toInt)
  }

  private def isIndexFile(file: File): Boolean = {
    val filename = file.getName
    filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix) || filename.endsWith(TxnIndexFileSuffix)
  }

  private def isLogFile(file: File): Boolean =
    file.getPath.endsWith(LogFileSuffix)

}
