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

import com.yammer.metrics.core.MetricName

import java.io.{File, IOException}
import java.nio.file.Files
import java.util.Optional
import java.util.concurrent.TimeUnit
import kafka.api.{ApiVersion, KAFKA_0_10_0_IV0}
import kafka.common.{LongRef, OffsetsOutOfOrderException, UnexpectedAppendOffsetException}
import kafka.log.AppendOrigin.RaftLeader
import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{BrokerTopicStats, FetchDataInfo, FetchHighWatermark, FetchIsolation, FetchLogEnd, FetchTxnCommitted, LogDirFailureChannel, LogOffsetMetadata, OffsetAndEpoch, PartitionMetadataFile, RequestLocal}
import kafka.utils._
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.{DescribeProducersResponseData, FetchResponseData}
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET
import org.apache.kafka.common.requests.ProduceResponse.RecordError
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{InvalidRecordException, KafkaException, TopicPartition, Uuid}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, immutable, mutable}

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(None, -1, None, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, -1L,
    RecordConversionStats.EMPTY, CompressionType.NONE, CompressionConfig.NONE, -1, -1, offsetsMonotonic = false, -1L)

  def unknownLogAppendInfoWithLogStartOffset(logStartOffset: Long): LogAppendInfo =
    LogAppendInfo(None, -1, None, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, CompressionType.NONE, CompressionConfig.NONE, -1, -1,
      offsetsMonotonic = false, -1L)

  /**
   * In ProduceResponse V8+, we add two new fields record_errors and error_message (see KIP-467).
   * For any record failures with InvalidTimestamp or InvalidRecordException, we construct a LogAppendInfo object like the one
   * in unknownLogAppendInfoWithLogStartOffset, but with additiona fields recordErrors and errorMessage
   */
  def unknownLogAppendInfoWithAdditionalInfo(logStartOffset: Long, recordErrors: Seq[RecordError], errorMessage: String): LogAppendInfo =
    LogAppendInfo(None, -1, None, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, CompressionType.NONE, CompressionConfig.NONE, -1, -1,
      offsetsMonotonic = false, -1L, recordErrors, errorMessage)
}

sealed trait LeaderHwChange
object LeaderHwChange {
  case object Increased extends LeaderHwChange
  case object Same extends LeaderHwChange
  case object None extends LeaderHwChange
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 *
 * @param firstOffset The first offset in the message set unless the message format is less than V2 and we are appending
 *                    to the follower. If the message is a duplicate message the segment base offset and relative position
 *                    in segment will be unknown.
 * @param lastOffset The last offset in the message set
 * @param lastLeaderEpoch The partition leader epoch corresponding to the last offset, if available.
 * @param maxTimestamp The maximum timestamp of the message set.
 * @param offsetOfMaxTimestamp The offset of the message with the maximum timestamp.
 * @param logAppendTime The log append time (if used) of the message set, otherwise Message.NoTimestamp
 * @param logStartOffset The start offset of the log at the time of this append.
 * @param recordConversionStats Statistics collected during record processing, `null` if `assignOffsets` is `false`
 * @param sourceType The source compression codec used in the message set (send by the producer)
 * @param targetConfig The target compression configuration of the message set(after applying the broker compression configuration if any)
 * @param shallowCount The number of shallow messages
 * @param validBytes The number of valid bytes
 * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
 * @param lastOffsetOfFirstBatch The last offset of the first batch
 * @param leaderHwChange Incremental if the high watermark needs to be increased after appending record.
 *                       Same if high watermark is not changed. None is the default value and it means append failed
 *
 */
case class LogAppendInfo(var firstOffset: Option[LogOffsetMetadata],
                         var lastOffset: Long,
                         var lastLeaderEpoch: Option[Int],
                         var maxTimestamp: Long,
                         var offsetOfMaxTimestamp: Long,
                         var logAppendTime: Long,
                         var logStartOffset: Long,
                         var recordConversionStats: RecordConversionStats,
                         sourceType: CompressionType,
                         targetConfig: CompressionConfig,
                         shallowCount: Int,
                         validBytes: Int,
                         offsetsMonotonic: Boolean,
                         lastOffsetOfFirstBatch: Long,
                         recordErrors: Seq[RecordError] = List(),
                         errorMessage: String = null,
                         leaderHwChange: LeaderHwChange = LeaderHwChange.None) {
  /**
   * Get the first offset if it exists, else get the last offset of the first batch
   * For magic versions 2 and newer, this method will return first offset. For magic versions
   * older than 2, we use the last offset of the first batch as an approximation of the first
   * offset to avoid decompressing the data.
   */
  def firstOrLastOffsetOfFirstBatch: Long = firstOffset.map(_.messageOffset).getOrElse(lastOffsetOfFirstBatch)

  /**
   * Get the (maximum) number of messages described by LogAppendInfo
   * @return Maximum possible number of messages described by LogAppendInfo
   */
  def numMessages: Long = {
    firstOffset match {
      case Some(firstOffsetVal) if (firstOffsetVal.messageOffset >= 0 && lastOffset >= 0) =>
        (lastOffset - firstOffsetVal.messageOffset + 1)
      case _ => 0
    }
  }
}

/**
 * Container class which represents a snapshot of the significant offsets for a partition. This allows fetching
 * of these offsets atomically without the possibility of a leader change affecting their consistency relative
 * to each other. See [[UnifiedLog.fetchOffsetSnapshot()]].
 */
case class LogOffsetSnapshot(logStartOffset: Long,
                             logEndOffset: LogOffsetMetadata,
                             highWatermark: LogOffsetMetadata,
                             lastStableOffset: LogOffsetMetadata)

/**
 * Another container which is used for lower level reads using  [[kafka.cluster.Partition.readRecords()]].
 */
case class LogReadInfo(fetchedData: FetchDataInfo,
                       divergingEpoch: Option[FetchResponseData.EpochEndOffset],
                       highWatermark: Long,
                       logStartOffset: Long,
                       logEndOffset: Long,
                       lastStableOffset: Long)

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
 * A class used to hold params required to decide to rotate a log segment or not.
 */
case class RollParams(maxSegmentMs: Long,
                      maxSegmentBytes: Int,
                      maxTimestampInMessages: Long,
                      maxOffsetInMessages: Long,
                      messagesSize: Int,
                      now: Long)

object RollParams {
  def apply(config: LogConfig, appendInfo: LogAppendInfo, messagesSize: Int, now: Long): RollParams = {
   new RollParams(config.maxSegmentMs,
     config.segmentSize,
     appendInfo.maxTimestamp,
     appendInfo.lastOffset,
     messagesSize, now)
  }
}

sealed trait LogStartOffsetIncrementReason
case object ClientRecordDeletion extends LogStartOffsetIncrementReason {
  override def toString: String = "client delete records request"
}
case object LeaderOffsetIncremented extends LogStartOffsetIncrementReason {
  override def toString: String = "leader offset increment"
}
case object SegmentDeletion extends LogStartOffsetIncrementReason {
  override def toString: String = "segment deletion"
}
case object SnapshotGenerated extends LogStartOffsetIncrementReason {
  override def toString: String = "snapshot generated"
}

/**
 * A log which presents a unified view of local and tiered log segments.
 *
 * The log consists of tiered and local segments with the tiered portion of the log being optional. There could be an
 * overlap between the tiered and local segments. The active segment is always guaranteed to be local. If tiered segments
 * are present, they always appear at the beginning of the log, followed by an optional region of overlap, followed by the local
 * segments including the active segment.
 *
 * NOTE: this class handles state and behavior specific to tiered segments as well as any behavior combining both tiered
 * and local segments. The state and behavior specific to local segments are handled by the encapsulated LocalLog instance.
 *
 * @param logStartOffset The earliest offset allowed to be exposed to kafka client.
 *                       The logStartOffset can be updated by :
 *                       - user's DeleteRecordsRequest
 *                       - broker's log retention
 *                       - broker's log truncation
 *                       - broker's log recovery
 *                       The logStartOffset is used to decide the following:
 *                       - Log deletion. LogSegment whose nextOffset <= log's logStartOffset can be deleted.
 *                         It may trigger log rolling if the active segment is deleted.
 *                       - Earliest offset of the log in response to ListOffsetRequest. To avoid OffsetOutOfRange exception after user seeks to earliest offset,
 *                         we make sure that logStartOffset <= log's highWatermark
 *                       Other activities such as log cleaning are not affected by logStartOffset.
 * @param localLog The LocalLog instance containing non-empty log segments recovered from disk
 * @param brokerTopicStats Container for Broker Topic Yammer Metrics
 * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
 * @param leaderEpochCache The LeaderEpochFileCache instance (if any) containing state associated
 *                         with the provided logStartOffset and nextOffsetMetadata
 * @param producerStateManager The ProducerStateManager instance containing state associated with the provided segments
 * @param _topicId optional Uuid to specify the topic ID for the topic if it exists. Should only be specified when
 *                first creating the log through Partition.makeLeader or Partition.makeFollower. When reloading a log,
 *                this field will be populated by reading the topic ID value from partition.metadata if it exists.
 * @param keepPartitionMetadataFile boolean flag to indicate whether the partition.metadata file should be kept in the
 *                                  log directory. A partition.metadata file is only created when the raft controller is used
 *                                  or the ZK controller and this broker's inter-broker protocol version is at least 2.8.
 *                                  This file will persist the topic ID on the broker. If inter-broker protocol for a ZK controller
 *                                  is downgraded below 2.8, a topic ID may be lost and a new ID generated upon re-upgrade.
 *                                  If the inter-broker protocol version on a ZK cluster is below 2.8, partition.metadata
 *                                  will be deleted to avoid ID conflicts upon re-upgrade.
 */
@threadsafe
class UnifiedLog(@volatile var logStartOffset: Long,
                 private val localLog: LocalLog,
                 brokerTopicStats: BrokerTopicStats,
                 val producerIdExpirationCheckIntervalMs: Int,
                 @volatile var leaderEpochCache: Option[LeaderEpochFileCache],
                 val producerStateManager: ProducerStateManager,
                 @volatile private var _topicId: Option[Uuid],
                 val keepPartitionMetadataFile: Boolean) extends Logging with KafkaMetricsGroup {

  import kafka.log.UnifiedLog._

  this.logIdent = s"[UnifiedLog partition=$topicPartition, dir=$parentDir] "

  /* A lock that guards all modifications to the log */
  private val lock = new Object

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
  @volatile private var firstUnstableOffsetMetadata: Option[LogOffsetMetadata] = None

  /* Keep track of the current high watermark in order to ensure that segments containing offsets at or above it are
   * not eligible for deletion. This means that the active segment is only eligible for deletion if the high watermark
   * equals the log end offset (which may never happen for a partition under consistent load). This is needed to
   * prevent the log start offset (which is exposed in fetch responses) from getting ahead of the high watermark.
   */
  @volatile private var highWatermarkMetadata: LogOffsetMetadata = LogOffsetMetadata(logStartOffset)

  @volatile var partitionMetadataFile : PartitionMetadataFile = null

  locally {
    initializePartitionMetadata()
    updateLogStartOffset(logStartOffset)
    maybeIncrementFirstUnstableOffset()
    initializeTopicId()
  }

  /**
   * Initialize topic ID information for the log by maintaining the partition metadata file and setting the in-memory _topicId.
   * Delete partition metadata file if the version does not support topic IDs.
   * Set _topicId based on a few scenarios:
   *   - Recover topic ID if present and topic IDs are supported. Ensure we do not try to assign a provided topicId that is inconsistent
   *     with the ID on file.
   *   - If we were provided a topic ID when creating the log, partition metadata files are supported, and one does not yet exist
   *     set _topicId and write to the partition metadata file.
   *   - Otherwise set _topicId to None
   */
  def initializeTopicId(): Unit =  {
    if (partitionMetadataFile.exists()) {
      if (keepPartitionMetadataFile) {
        val fileTopicId = partitionMetadataFile.read().topicId
        if (_topicId.isDefined && !_topicId.contains(fileTopicId))
          throw new InconsistentTopicIdException(s"Tried to assign topic ID $topicId to log for topic partition $topicPartition," +
            s"but log already contained topic ID $fileTopicId")

        _topicId = Some(fileTopicId)

      } else {
        try partitionMetadataFile.delete()
        catch {
          case e: IOException =>
            error(s"Error while trying to delete partition metadata file ${partitionMetadataFile}", e)
        }
      }
    } else if (keepPartitionMetadataFile) {
      _topicId.foreach(partitionMetadataFile.record)
      scheduler.schedule("flush-metadata-file", maybeFlushMetadataFile)
    } else {
      // We want to keep the file and the in-memory topic ID in sync.
      _topicId = None
    }
  }

  def topicId: Option[Uuid] = _topicId

  def dir: File = localLog.dir

  def parentDir: String = localLog.parentDir

  def parentDirFile: File = localLog.parentDirFile

  def name: String = localLog.name

  def recoveryPoint: Long = localLog.recoveryPoint

  def topicPartition: TopicPartition = localLog.topicPartition

  def time: Time = localLog.time

  def scheduler: Scheduler = localLog.scheduler

  def config: LogConfig = localLog.config

  def logDirFailureChannel: LogDirFailureChannel = localLog.logDirFailureChannel

  def updateConfig(newConfig: LogConfig): LogConfig = {
    val oldConfig = localLog.config
    localLog.updateConfig(newConfig)
    val oldRecordVersion = oldConfig.recordVersion
    val newRecordVersion = newConfig.recordVersion
    if (newRecordVersion != oldRecordVersion)
      initializeLeaderEpochCache()
    oldConfig
  }

  def highWatermark: Long = highWatermarkMetadata.messageOffset

  /**
   * Update the high watermark to a new offset. The new high watermark will be lower
   * bounded by the log start offset and upper bounded by the log end offset.
   *
   * This is intended to be called when initializing the high watermark or when updating
   * it on a follower after receiving a Fetch response from the leader.
   *
   * @param hw the suggested new value for the high watermark
   * @return the updated high watermark offset
   */
  def updateHighWatermark(hw: Long): Long = {
    updateHighWatermark(LogOffsetMetadata(hw))
  }

  /**
   * Update high watermark with offset metadata. The new high watermark will be lower
   * bounded by the log start offset and upper bounded by the log end offset.
   *
   * @param highWatermarkMetadata the suggested high watermark with offset metadata
   * @return the updated high watermark offset
   */
  def updateHighWatermark(highWatermarkMetadata: LogOffsetMetadata): Long = {
    val endOffsetMetadata = localLog.logEndOffsetMetadata
    val newHighWatermarkMetadata = if (highWatermarkMetadata.messageOffset < logStartOffset) {
      LogOffsetMetadata(logStartOffset)
    } else if (highWatermarkMetadata.messageOffset >= endOffsetMetadata.messageOffset) {
      endOffsetMetadata
    } else {
      highWatermarkMetadata
    }

    updateHighWatermarkMetadata(newHighWatermarkMetadata)
    newHighWatermarkMetadata.messageOffset
  }

  /**
   * Update the high watermark to a new value if and only if it is larger than the old value. It is
   * an error to update to a value which is larger than the log end offset.
   *
   * This method is intended to be used by the leader to update the high watermark after follower
   * fetch offsets have been updated.
   *
   * @return the old high watermark, if updated by the new value
   */
  def maybeIncrementHighWatermark(newHighWatermark: LogOffsetMetadata): Option[LogOffsetMetadata] = {
    if (newHighWatermark.messageOffset > logEndOffset)
      throw new IllegalArgumentException(s"High watermark $newHighWatermark update exceeds current " +
        s"log end offset ${localLog.logEndOffsetMetadata}")

    lock.synchronized {
      val oldHighWatermark = fetchHighWatermarkMetadata

      // Ensure that the high watermark increases monotonically. We also update the high watermark when the new
      // offset metadata is on a newer segment, which occurs whenever the log is rolled to a new segment.
      if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset ||
        (oldHighWatermark.messageOffset == newHighWatermark.messageOffset && oldHighWatermark.onOlderSegment(newHighWatermark))) {
        updateHighWatermarkMetadata(newHighWatermark)
        Some(oldHighWatermark)
      } else {
        None
      }
    }
  }

  /**
   * Get the offset and metadata for the current high watermark. If offset metadata is not
   * known, this will do a lookup in the index and cache the result.
   */
  private def fetchHighWatermarkMetadata: LogOffsetMetadata = {
    localLog.checkIfMemoryMappedBufferClosed()

    val offsetMetadata = highWatermarkMetadata
    if (offsetMetadata.messageOffsetOnly) {
      lock.synchronized {
        val fullOffset = convertToOffsetMetadataOrThrow(highWatermark)
        updateHighWatermarkMetadata(fullOffset)
        fullOffset
      }
    } else {
      offsetMetadata
    }
  }

  private def updateHighWatermarkMetadata(newHighWatermark: LogOffsetMetadata): Unit = {
    if (newHighWatermark.messageOffset < 0)
      throw new IllegalArgumentException("High watermark offset should be non-negative")

    lock synchronized {
      if (newHighWatermark.messageOffset < highWatermarkMetadata.messageOffset) {
        warn(s"Non-monotonic update of high watermark from $highWatermarkMetadata to $newHighWatermark")
      }

      highWatermarkMetadata = newHighWatermark
      producerStateManager.onHighWatermarkUpdated(newHighWatermark.messageOffset)
      maybeIncrementFirstUnstableOffset()
    }
    trace(s"Setting high watermark $newHighWatermark")
  }

  /**
   * Get the first unstable offset. Unlike the last stable offset, which is always defined,
   * the first unstable offset only exists if there are transactions in progress.
   *
   * @return the first unstable offset, if it exists
   */
  private[log] def firstUnstableOffset: Option[Long] = firstUnstableOffsetMetadata.map(_.messageOffset)

  private def fetchLastStableOffsetMetadata: LogOffsetMetadata = {
    localLog.checkIfMemoryMappedBufferClosed()

    // cache the current high watermark to avoid a concurrent update invalidating the range check
    val highWatermarkMetadata = fetchHighWatermarkMetadata

    firstUnstableOffsetMetadata match {
      case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermarkMetadata.messageOffset =>
        if (offsetMetadata.messageOffsetOnly) {
          lock synchronized {
            val fullOffset = convertToOffsetMetadataOrThrow(offsetMetadata.messageOffset)
            if (firstUnstableOffsetMetadata.contains(offsetMetadata))
              firstUnstableOffsetMetadata = Some(fullOffset)
            fullOffset
          }
        } else {
          offsetMetadata
        }
      case _ => highWatermarkMetadata
    }
  }

  /**
   * The last stable offset (LSO) is defined as the first offset such that all lower offsets have been "decided."
   * Non-transactional messages are considered decided immediately, but transactional messages are only decided when
   * the corresponding COMMIT or ABORT marker is written. This implies that the last stable offset will be equal
   * to the high watermark if there are no transactional messages in the log. Note also that the LSO cannot advance
   * beyond the high watermark.
   */
  def lastStableOffset: Long = {
    firstUnstableOffsetMetadata match {
      case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermark => offsetMetadata.messageOffset
      case _ => highWatermark
    }
  }

  def lastStableOffsetLag: Long = highWatermark - lastStableOffset

  /**
    * Fully materialize and return an offset snapshot including segment position info. This method will update
    * the LogOffsetMetadata for the high watermark and last stable offset if they are message-only. Throws an
    * offset out of range error if the segment info cannot be loaded.
    */
  def fetchOffsetSnapshot: LogOffsetSnapshot = {
    val lastStable = fetchLastStableOffsetMetadata
    val highWatermark = fetchHighWatermarkMetadata

    LogOffsetSnapshot(
      logStartOffset,
      localLog.logEndOffsetMetadata,
      highWatermark,
      lastStable
    )
  }

  private val tags = {
    val maybeFutureTag = if (isFuture) Map("is-future" -> "true") else Map.empty[String, String]
    Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString) ++ maybeFutureTag
  }

  newGauge(LogMetricNames.NumLogSegments, () => numberOfSegments, tags)
  newGauge(LogMetricNames.LogStartOffset, () => logStartOffset, tags)
  newGauge(LogMetricNames.LogEndOffset, () => logEndOffset, tags)
  newGauge(LogMetricNames.Size, () => size, tags)

  val producerExpireCheck = scheduler.schedule(name = "PeriodicProducerExpirationCheck", fun = () => {
    lock synchronized {
      producerStateManager.removeExpiredProducers(time.milliseconds)
    }
  }, period = producerIdExpirationCheckIntervalMs, delay = producerIdExpirationCheckIntervalMs, unit = TimeUnit.MILLISECONDS)

  // For compatibility, metrics are defined to be under `Log` class
  override def metricName(name: String, tags: scala.collection.Map[String, String]): MetricName = {
    val pkg = getClass.getPackage
    val pkgStr = if (pkg == null) "" else pkg.getName
    explicitMetricName(pkgStr, "Log", name, tags)
  }

  private def recordVersion: RecordVersion = config.recordVersion

  private def initializePartitionMetadata(): Unit = lock synchronized {
    val partitionMetadata = PartitionMetadataFile.newFile(dir)
    partitionMetadataFile = new PartitionMetadataFile(partitionMetadata, logDirFailureChannel)
  }

  private def maybeFlushMetadataFile(): Unit = {
    partitionMetadataFile.maybeFlush()
  }

  /** Only used for ZK clusters when we update and start using topic IDs on existing topics */
  def assignTopicId(topicId: Uuid): Unit = {
    _topicId match {
      case Some(currentId) =>
        if (!currentId.equals(topicId)) {
          throw new InconsistentTopicIdException(s"Tried to assign topic ID $topicId to log for topic partition $topicPartition," +
            s"but log already contained topic ID $currentId")
        }

      case None =>
        if (keepPartitionMetadataFile) {
          _topicId = Some(topicId)
          if (!partitionMetadataFile.exists()) {
            partitionMetadataFile.record(topicId)
            scheduler.schedule("flush-metadata-file", maybeFlushMetadataFile)
          }
        }
    }
  }

  private def initializeLeaderEpochCache(): Unit = lock synchronized {
    leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(dir, topicPartition, logDirFailureChannel, recordVersion, logIdent)
  }

  private def updateHighWatermarkWithLogEndOffset(): Unit = {
    // Update the high watermark in case it has gotten ahead of the log end offset following a truncation
    // or if a new segment has been rolled and the offset metadata needs to be updated.
    if (highWatermark >= localLog.logEndOffset) {
      updateHighWatermarkMetadata(localLog.logEndOffsetMetadata)
    }
  }

  private def updateLogStartOffset(offset: Long): Unit = {
    logStartOffset = offset

    if (highWatermark < offset) {
      updateHighWatermark(offset)
    }

    if (localLog.recoveryPoint < offset) {
      localLog.updateRecoveryPoint(offset)
    }
  }

  // Rebuild producer state until lastOffset. This method may be called from the recovery code path, and thus must be
  // free of all side-effects, i.e. it must not update any log-specific state.
  private def rebuildProducerState(lastOffset: Long,
                                   producerStateManager: ProducerStateManager): Unit = lock synchronized {
    localLog.checkIfMemoryMappedBufferClosed()
    UnifiedLog.rebuildProducerState(producerStateManager, localLog.segments, logStartOffset, lastOffset, recordVersion, time,
      reloadFromCleanShutdown = false, logIdent)
  }

  def activeProducers: Seq[DescribeProducersResponseData.ProducerState] = {
    lock synchronized {
      producerStateManager.activeProducers.map { case (producerId, state) =>
        new DescribeProducersResponseData.ProducerState()
          .setProducerId(producerId)
          .setProducerEpoch(state.producerEpoch)
          .setLastSequence(state.lastSeq)
          .setLastTimestamp(state.lastTimestamp)
          .setCoordinatorEpoch(state.coordinatorEpoch)
          .setCurrentTxnStartOffset(state.currentTxnFirstOffset.getOrElse(-1L))
      }
    }.toSeq
  }

  private[log] def activeProducersWithLastSequence: Map[Long, Int] = lock synchronized {
    producerStateManager.activeProducers.map { case (producerId, producerIdEntry) =>
      (producerId, producerIdEntry.lastSeq)
    }
  }

  private[log] def lastRecordsOfActiveProducers: Map[Long, LastRecord] = lock synchronized {
    producerStateManager.activeProducers.map { case (producerId, producerIdEntry) =>
      val lastDataOffset = if (producerIdEntry.lastDataOffset >= 0 ) Some(producerIdEntry.lastDataOffset) else None
      val lastRecord = LastRecord(lastDataOffset, producerIdEntry.producerEpoch)
      producerId -> lastRecord
    }
  }

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = localLog.segments.numberOfSegments

  /**
   * Close this log.
   * The memory mapped buffer for index files of this log will be left open until the log is deleted.
   */
  def close(): Unit = {
    debug("Closing log")
    lock synchronized {
      maybeFlushMetadataFile()
      localLog.checkIfMemoryMappedBufferClosed()
      producerExpireCheck.cancel(true)
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in dir ${dir.getParent}") {
        // We take a snapshot at the last written offset to hopefully avoid the need to scan the log
        // after restarting and to ensure that we cannot inadvertently hit the upgrade optimization
        // (the clean shutdown file is written after the logs are all closed).
        producerStateManager.takeSnapshot()
      }
      localLog.close()
    }
  }

  /**
   * Rename the directory of the local log
   *
   * @throws KafkaStorageException if rename fails
   */
  def renameDir(name: String): Unit = {
    lock synchronized {
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in log dir ${dir.getParent}") {
        // Flush partitionMetadata file before initializing again
        maybeFlushMetadataFile()
        if (localLog.renameDir(name)) {
          producerStateManager.updateParentDir(dir)
          // re-initialize leader epoch cache so that LeaderEpochCheckpointFile.checkpoint can correctly reference
          // the checkpoint file in renamed log directory
          initializeLeaderEpochCache()
          initializePartitionMetadata()
        }
      }
    }
  }

  /**
   * Close file handlers used by this log but don't write to disk. This is called if the log directory is offline
   */
  def closeHandlers(): Unit = {
    debug("Closing handlers")
    lock synchronized {
      localLog.closeHandlers()
    }
  }

  /**
   * Append this message set to the active segment of the local log, assigning offsets and Partition Leader Epochs
   *
   * @param records The records to append
   * @param origin Declares the origin of the append which affects required validations
   * @param interBrokerProtocolVersion Inter-broker message protocol version
   * @param requestLocal request local instance
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsLeader(records: MemoryRecords,
                     leaderEpoch: Int,
                     origin: AppendOrigin = AppendOrigin.Client,
                     interBrokerProtocolVersion: ApiVersion = ApiVersion.latestVersion,
                     requestLocal: RequestLocal = RequestLocal.NoCaching): LogAppendInfo = {
    val validateAndAssignOffsets = origin != AppendOrigin.RaftLeader
    append(records, origin, interBrokerProtocolVersion, validateAndAssignOffsets, leaderEpoch, Some(requestLocal), ignoreRecordSize = false)
  }

  /**
   * Append this message set to the active segment of the local log without assigning offsets or Partition Leader Epochs
   *
   * @param records The records to append
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
    append(records,
      origin = AppendOrigin.Replication,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      validateAndAssignOffsets = false,
      leaderEpoch = -1,
      None,
      // disable to check the validation of record size since the record is already accepted by leader.
      ignoreRecordSize = true)
  }

  /**
   * Append this message set to the active segment of the local log, rolling over to a fresh segment if necessary.
   *
   * This method will generally be responsible for assigning offsets to the messages,
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   *
   * @param records The log records to append
   * @param origin Declares the origin of the append which affects required validations
   * @param interBrokerProtocolVersion Inter-broker message protocol version
   * @param validateAndAssignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   * @param leaderEpoch The partition's leader epoch which will be applied to messages when offsets are assigned on the leader
   * @param requestLocal The request local instance if assignOffsets is true
   * @param ignoreRecordSize true to skip validation of record size.
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @throws OffsetsOutOfOrderException If out of order offsets found in 'records'
   * @throws UnexpectedAppendOffsetException If the first or last offset in append is less than next offset
   * @return Information about the appended messages including the first and last offset.
   */
  private def append(records: MemoryRecords,
                     origin: AppendOrigin,
                     interBrokerProtocolVersion: ApiVersion,
                     validateAndAssignOffsets: Boolean,
                     leaderEpoch: Int,
                     requestLocal: Option[RequestLocal],
                     ignoreRecordSize: Boolean): LogAppendInfo = {
    // We want to ensure the partition metadata file is written to the log dir before any log data is written to disk.
    // This will ensure that any log data can be recovered with the correct topic ID in the case of failure.
    maybeFlushMetadataFile()

    val appendInfo = analyzeAndValidateRecords(records, origin, ignoreRecordSize, leaderEpoch)

    // return if we have no valid messages or if this is a duplicate of the last appended entry
    if (appendInfo.shallowCount == 0) appendInfo
    else {

      // trim any invalid bytes or partial messages before appending it to the on-disk log
      var validRecords = trimInvalidBytes(records, appendInfo)

      // they are valid, insert them in the log
      lock synchronized {
        maybeHandleIOException(s"Error while appending records to $topicPartition in dir ${dir.getParent}") {
          localLog.checkIfMemoryMappedBufferClosed()
          if (validateAndAssignOffsets) {
            // assign offsets to the message set
            val offset = new LongRef(localLog.logEndOffset)
            appendInfo.firstOffset = Some(LogOffsetMetadata(offset.value))
            val now = time.milliseconds
            val validateAndOffsetAssignResult = try {
              LogValidator.validateMessagesAndAssignOffsets(validRecords,
                topicPartition,
                offset,
                time,
                now,
                appendInfo.sourceType,
                appendInfo.targetConfig,
                config.compact,
                config.recordVersion.value,
                config.messageTimestampType,
                config.messageTimestampDifferenceMaxMs,
                leaderEpoch,
                origin,
                interBrokerProtocolVersion,
                brokerTopicStats,
                requestLocal.getOrElse(throw new IllegalArgumentException(
                  "requestLocal should be defined if assignOffsets is true")))
            } catch {
              case e: IOException =>
                throw new KafkaException(s"Error validating messages while appending to log $name", e)
            }
            validRecords = validateAndOffsetAssignResult.validatedRecords
            appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
            appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
            appendInfo.lastOffset = offset.value - 1
            appendInfo.recordConversionStats = validateAndOffsetAssignResult.recordConversionStats
            if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
              appendInfo.logAppendTime = now

            // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
            // format conversion)
            if (!ignoreRecordSize && validateAndOffsetAssignResult.messageSizeMaybeChanged) {
              validRecords.batches.forEach { batch =>
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
            if (!appendInfo.offsetsMonotonic)
              throw new OffsetsOutOfOrderException(s"Out of order offsets found in append to $topicPartition: " +
                records.records.asScala.map(_.offset))

            if (appendInfo.firstOrLastOffsetOfFirstBatch < localLog.logEndOffset) {
              // we may still be able to recover if the log is empty
              // one example: fetching from log start offset on the leader which is not batch aligned,
              // which may happen as a result of AdminClient#deleteRecords()
              val firstOffset = appendInfo.firstOffset match {
                case Some(offsetMetadata) => offsetMetadata.messageOffset
                case None => records.batches.asScala.head.baseOffset()
              }

              val firstOrLast = if (appendInfo.firstOffset.isDefined) "First offset" else "Last offset of the first batch"
              throw new UnexpectedAppendOffsetException(
                s"Unexpected offset in append to $topicPartition. $firstOrLast " +
                  s"${appendInfo.firstOrLastOffsetOfFirstBatch} is less than the next offset ${localLog.logEndOffset}. " +
                  s"First 10 offsets in append: ${records.records.asScala.take(10).map(_.offset)}, last offset in" +
                  s" append: ${appendInfo.lastOffset}. Log start offset = $logStartOffset",
                firstOffset, appendInfo.lastOffset)
            }
          }

          // update the epoch cache with the epoch stamped onto the message by the leader
          validRecords.batches.forEach { batch =>
            if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
              maybeAssignEpochStartOffset(batch.partitionLeaderEpoch, batch.baseOffset)
            } else {
              // In partial upgrade scenarios, we may get a temporary regression to the message format. In
              // order to ensure the safety of leader election, we clear the epoch cache so that we revert
              // to truncation by high watermark after the next leader election.
              leaderEpochCache.filter(_.nonEmpty).foreach { cache =>
                warn(s"Clearing leader epoch cache after unexpected append with message format v${batch.magic}")
                cache.clearAndFlush()
              }
            }
          }

          // check messages set size may be exceed config.segmentSize
          if (validRecords.sizeInBytes > config.segmentSize) {
            throw new RecordBatchTooLargeException(s"Message batch size is ${validRecords.sizeInBytes} bytes in append " +
              s"to partition $topicPartition, which exceeds the maximum configured segment size of ${config.segmentSize}.")
          }

          // maybe roll the log if this segment is full
          val segment = maybeRoll(validRecords.sizeInBytes, appendInfo)

          val logOffsetMetadata = LogOffsetMetadata(
            messageOffset = appendInfo.firstOrLastOffsetOfFirstBatch,
            segmentBaseOffset = segment.baseOffset,
            relativePositionInSegment = segment.size)

          // now that we have valid records, offsets assigned, and timestamps updated, we need to
          // validate the idempotent/transactional state of the producers and collect some metadata
          val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(
            logOffsetMetadata, validRecords, origin)

          maybeDuplicate match {
            case Some(duplicate) =>
              appendInfo.firstOffset = Some(LogOffsetMetadata(duplicate.firstOffset))
              appendInfo.lastOffset = duplicate.lastOffset
              appendInfo.logAppendTime = duplicate.timestamp
              appendInfo.logStartOffset = logStartOffset
            case None =>
              // Before appending update the first offset metadata to include segment information
              appendInfo.firstOffset = appendInfo.firstOffset.map { offsetMetadata =>
                offsetMetadata.copy(segmentBaseOffset = segment.baseOffset, relativePositionInSegment = segment.size)
              }

              // Append the records, and increment the local log end offset immediately after the append because a
              // write to the transaction index below may fail and we want to ensure that the offsets
              // of future appends still grow monotonically. The resulting transaction index inconsistency
              // will be cleaned up after the log directory is recovered. Note that the end offset of the
              // ProducerStateManager will not be updated and the last stable offset will not advance
              // if the append to the transaction index fails.
              localLog.append(appendInfo.lastOffset, appendInfo.maxTimestamp, appendInfo.offsetOfMaxTimestamp, validRecords)
              updateHighWatermarkWithLogEndOffset()

              // update the producer state
              updatedProducers.values.foreach(producerAppendInfo => producerStateManager.update(producerAppendInfo))

              // update the transaction index with the true last stable offset. The last offset visible
              // to consumers using READ_COMMITTED will be limited by this value and the high watermark.
              completedTxns.foreach { completedTxn =>
                val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
                segment.updateTxnIndex(completedTxn, lastStableOffset)
                producerStateManager.completeTxn(completedTxn)
              }

              // always update the last producer id map offset so that the snapshot reflects the current offset
              // even if there isn't any idempotent data being written
              producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

              // update the first unstable offset (which is used to compute LSO)
              maybeIncrementFirstUnstableOffset()

              trace(s"Appended message set with last offset: ${appendInfo.lastOffset}, " +
                s"first offset: ${appendInfo.firstOffset}, " +
                s"next offset: ${localLog.logEndOffset}, " +
                s"and messages: $validRecords")

              if (localLog.unflushedMessages >= config.flushInterval) flush()
          }
          appendInfo
        }
      }
    }
  }

  def maybeAssignEpochStartOffset(leaderEpoch: Int, startOffset: Long): Unit = {
    leaderEpochCache.foreach { cache =>
      cache.assign(leaderEpoch, startOffset)
    }
  }

  def latestEpoch: Option[Int] = leaderEpochCache.flatMap(_.latestEpoch)

  def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = {
    leaderEpochCache.flatMap { cache =>
      val (foundEpoch, foundOffset) = cache.endOffsetFor(leaderEpoch, logEndOffset)
      if (foundOffset == UNDEFINED_EPOCH_OFFSET)
        None
      else
        Some(OffsetAndEpoch(foundOffset, foundEpoch))
    }
  }

  private def maybeIncrementFirstUnstableOffset(): Unit = lock synchronized {
    localLog.checkIfMemoryMappedBufferClosed()

    val updatedFirstUnstableOffset = producerStateManager.firstUnstableOffset match {
      case Some(logOffsetMetadata) if logOffsetMetadata.messageOffsetOnly || logOffsetMetadata.messageOffset < logStartOffset =>
        val offset = math.max(logOffsetMetadata.messageOffset, logStartOffset)
        Some(convertToOffsetMetadataOrThrow(offset))
      case other => other
    }

    if (updatedFirstUnstableOffset != this.firstUnstableOffsetMetadata) {
      debug(s"First unstable offset updated to $updatedFirstUnstableOffset")
      this.firstUnstableOffsetMetadata = updatedFirstUnstableOffset
    }
  }

  /**
   * Increment the log start offset if the provided offset is larger.
   *
   * If the log start offset changed, then this method also update a few key offset such that
   * `logStartOffset <= logStableOffset <= highWatermark`. The leader epoch cache is also updated
   * such that all of offsets referenced in that component point to valid offset in this log.
   *
   * @throws OffsetOutOfRangeException if the log start offset is greater than the high watermark
   * @return true if the log start offset was updated; otherwise false
   */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long, reason: LogStartOffsetIncrementReason): Boolean = {
    // We don't have to write the log start offset to log-start-offset-checkpoint immediately.
    // The deleteRecordsOffset may be lost only if all in-sync replicas of this broker are shutdown
    // in an unclean manner within log.flush.start.offset.checkpoint.interval.ms. The chance of this happening is low.
    var updatedLogStartOffset = false
    maybeHandleIOException(s"Exception while increasing log start offset for $topicPartition to $newLogStartOffset in dir ${dir.getParent}") {
      lock synchronized {
        if (newLogStartOffset > highWatermark)
          throw new OffsetOutOfRangeException(s"Cannot increment the log start offset to $newLogStartOffset of partition $topicPartition " +
            s"since it is larger than the high watermark $highWatermark")

        localLog.checkIfMemoryMappedBufferClosed()
        if (newLogStartOffset > logStartOffset) {
          updatedLogStartOffset = true
          updateLogStartOffset(newLogStartOffset)
          info(s"Incremented log start offset to $newLogStartOffset due to $reason")
          leaderEpochCache.foreach(_.truncateFromStart(logStartOffset))
          producerStateManager.onLogStartOffsetIncremented(newLogStartOffset)
          maybeIncrementFirstUnstableOffset()
        }
      }
    }

    updatedLogStartOffset
  }

  private def analyzeAndValidateProducerState(appendOffsetMetadata: LogOffsetMetadata,
                                              records: MemoryRecords,
                                              origin: AppendOrigin):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    var relativePositionInSegment = appendOffsetMetadata.relativePositionInSegment

    records.batches.forEach { batch =>
      if (batch.hasProducerId) {
        // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
        // If we find a duplicate, we return the metadata of the appended batch to the client.
        if (origin == AppendOrigin.Client) {
          val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

          maybeLastEntry.flatMap(_.findDuplicateBatch(batch)).foreach { duplicate =>
            return (updatedProducers, completedTxns.toList, Some(duplicate))
          }
        }

        // We cache offset metadata for the start of each transaction. This allows us to
        // compute the last stable offset without relying on additional index lookups.
        val firstOffsetMetadata = if (batch.isTransactional)
          Some(LogOffsetMetadata(batch.baseOffset, appendOffsetMetadata.segmentBaseOffset, relativePositionInSegment))
        else
          None

        val maybeCompletedTxn = updateProducers(producerStateManager, batch, updatedProducers, firstOffsetMetadata, origin)
        maybeCompletedTxn.foreach(completedTxns += _)
      }

      relativePositionInSegment += batch.sizeInBytes
    }
    (updatedProducers, completedTxns.toList, None)
  }

  /**
   * Validate the following:
   * <ol>
   * <li> each message matches its CRC
   * <li> each message size is valid (if ignoreRecordSize is false)
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
  private def analyzeAndValidateRecords(records: MemoryRecords,
                                        origin: AppendOrigin,
                                        ignoreRecordSize: Boolean,
                                        leaderEpoch: Int): LogAppendInfo = {
    var shallowMessageCount = 0
    var validBytesCount = 0
    var firstOffset: Option[LogOffsetMetadata] = None
    var lastOffset = -1L
    var lastLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH
    var sourceType: CompressionType = CompressionType.NONE
    var monotonic = true
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    var readFirstMessage = false
    var lastOffsetOfFirstBatch = -1L

    records.batches.forEach { batch =>
      if (origin == RaftLeader && batch.partitionLeaderEpoch != leaderEpoch) {
        throw new InvalidRecordException("Append from Raft leader did not set the batch epoch correctly")
      }
      // we only validate V2 and higher to avoid potential compatibility issues with older clients
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2 && origin == AppendOrigin.Client && batch.baseOffset != 0)
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
          firstOffset = Some(LogOffsetMetadata(batch.baseOffset))
        lastOffsetOfFirstBatch = batch.lastOffset
        readFirstMessage = true
      }

      // check that offsets are monotonically increasing
      if (lastOffset >= batch.lastOffset)
        monotonic = false

      // update the last offset seen
      lastOffset = batch.lastOffset
      lastLeaderEpoch = batch.partitionLeaderEpoch

      // Check if the message sizes are valid.
      val batchSize = batch.sizeInBytes
      if (!ignoreRecordSize && batchSize > config.maxMessageSize) {
        brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
        brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        throw new RecordTooLargeException(s"The record batch size in the append to $topicPartition is $batchSize bytes " +
          s"which exceeds the maximum configured value of ${config.maxMessageSize}.")
      }

      // check the validity of the message by checking CRC
      if (!batch.isValid) {
        brokerTopicStats.allTopicsStats.invalidMessageCrcRecordsPerSec.mark()
        throw new CorruptRecordException(s"Record is corrupt (stored crc = ${batch.checksum()}) in topic partition $topicPartition.")
      }

      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = lastOffset
      }

      shallowMessageCount += 1
      validBytesCount += batchSize

      val messageCompressionType = batch.compressionType
      if (messageCompressionType != CompressionType.NONE)
        sourceType = messageCompressionType
    }

    // Apply broker-side compression if any
    val targetConfig = config.compressionConfig(sourceType)
    val lastLeaderEpochOpt: Option[Int] = if (lastLeaderEpoch != RecordBatch.NO_PARTITION_LEADER_EPOCH)
      Some(lastLeaderEpoch)
    else
      None
    LogAppendInfo(firstOffset, lastOffset, lastLeaderEpochOpt, maxTimestamp, offsetOfMaxTimestamp, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, sourceType, targetConfig, shallowMessageCount, validBytesCount, monotonic, lastOffsetOfFirstBatch)
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

  private def checkLogStartOffset(offset: Long): Unit = {
    if (offset < logStartOffset)
      throw new OffsetOutOfRangeException(s"Received request for offset $offset for partition $topicPartition, " +
        s"but we only have log segments starting from offset: $logStartOffset.")
  }

  /**
   * Read messages from the log.
   *
   * @param startOffset The offset to begin reading at
   * @param maxLength The maximum number of bytes to read
   * @param isolation The fetch isolation, which controls the maximum offset we are allowed to read
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  def read(startOffset: Long,
           maxLength: Int,
           isolation: FetchIsolation,
           minOneMessage: Boolean): FetchDataInfo = {
    checkLogStartOffset(startOffset)
    val maxOffsetMetadata = isolation match {
      case FetchLogEnd => localLog.logEndOffsetMetadata
      case FetchHighWatermark => fetchHighWatermarkMetadata
      case FetchTxnCommitted => fetchLastStableOffsetMetadata
    }
    localLog.read(startOffset, maxLength, minOneMessage, maxOffsetMetadata, isolation == FetchTxnCommitted)
  }

  private[log] def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    localLog.collectAbortedTransactions(logStartOffset, startOffset, upperBoundOffset)
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
  @nowarn("cat=deprecation")
  def fetchOffsetByTimestamp(targetTimestamp: Long): Option[TimestampAndOffset] = {
    maybeHandleIOException(s"Error while fetching offset by timestamp for $topicPartition in dir ${dir.getParent}") {
      debug(s"Searching offset for timestamp $targetTimestamp")

      if (config.messageFormatVersion < KAFKA_0_10_0_IV0 &&
        targetTimestamp != ListOffsetsRequest.EARLIEST_TIMESTAMP &&
        targetTimestamp != ListOffsetsRequest.LATEST_TIMESTAMP)
        throw new UnsupportedForMessageFormatException(s"Cannot search offsets based on timestamp because message format version " +
          s"for partition $topicPartition is ${config.messageFormatVersion} which is earlier than the minimum " +
          s"required version $KAFKA_0_10_0_IV0")

      // For the earliest and latest, we do not need to return the timestamp.
      if (targetTimestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
        // The first cached epoch usually corresponds to the log start offset, but we have to verify this since
        // it may not be true following a message format version bump as the epoch will not be available for
        // log entries written in the older format.
        val earliestEpochEntry = leaderEpochCache.flatMap(_.earliestEntry)
        val epochOpt = earliestEpochEntry match {
          case Some(entry) if entry.startOffset <= logStartOffset => Optional.of[Integer](entry.epoch)
          case _ => Optional.empty[Integer]()
        }
        Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logStartOffset, epochOpt))
      } else if (targetTimestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
        val latestEpochOpt = leaderEpochCache.flatMap(_.latestEpoch).map(_.asInstanceOf[Integer])
        val epochOptional = Optional.ofNullable(latestEpochOpt.orNull)
        Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logEndOffset, epochOptional))
      } else if (targetTimestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
        // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
        // constant time access while being safe to use with concurrent collections unlike `toArray`.
        val segmentsCopy = logSegments.toBuffer
        val latestTimestampSegment = segmentsCopy.maxBy(_.maxTimestampSoFar)
        val latestEpochOpt = leaderEpochCache.flatMap(_.latestEpoch).map(_.asInstanceOf[Integer])
        val epochOptional = Optional.ofNullable(latestEpochOpt.orNull)
        val latestTimestampAndOffset = latestTimestampSegment.maxTimestampAndOffsetSoFar
        Some(new TimestampAndOffset(latestTimestampAndOffset.timestamp,
          latestTimestampAndOffset.offset,
          epochOptional))
      } else {
        // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
        // constant time access while being safe to use with concurrent collections unlike `toArray`.
        val segmentsCopy = logSegments.toBuffer
        // We need to search the first segment whose largest timestamp is >= the target timestamp if there is one.
        val targetSeg = segmentsCopy.find(_.largestTimestamp >= targetTimestamp)
        targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp, logStartOffset))
      }
    }
  }

  def legacyFetchOffsetsBefore(timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
    // constant time access while being safe to use with concurrent collections unlike `toArray`.
    val allSegments = logSegments.toBuffer
    val lastSegmentHasSize = allSegments.last.size > 0

    val offsetTimeArray =
      if (lastSegmentHasSize)
        new Array[(Long, Long)](allSegments.length + 1)
      else
        new Array[(Long, Long)](allSegments.length)

    for (i <- allSegments.indices)
      offsetTimeArray(i) = (math.max(allSegments(i).baseOffset, logStartOffset), allSegments(i).lastModified)
    if (lastSegmentHasSize)
      offsetTimeArray(allSegments.length) = (logEndOffset, time.milliseconds)

    var startIndex = -1
    timestamp match {
      case ListOffsetsRequest.LATEST_TIMESTAMP =>
        startIndex = offsetTimeArray.length - 1
      case ListOffsetsRequest.EARLIEST_TIMESTAMP =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -= 1
        }
    }

    val retSize = maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    for (j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(-_)
  }

  /**
    * Given a message offset, find its corresponding offset metadata in the log.
    * If the message offset is out of range, throw an OffsetOutOfRangeException
    */
  private def convertToOffsetMetadataOrThrow(offset: Long): LogOffsetMetadata = {
    checkLogStartOffset(offset)
    localLog.convertToOffsetMetadataOrThrow(offset)
  }

  /**
   * Delete any local log segments starting with the oldest segment and moving forward until until
   * the user-supplied predicate is false or the segment containing the current high watermark is reached.
   * We do not delete segments with offsets at or beyond the high watermark to ensure that the log start
   * offset can never exceed it. If the high watermark has not yet been initialized, no segments are eligible
   * for deletion.
   *
   * @param predicate A function that takes in a candidate log segment and the next higher segment
   *                  (if there is one) and returns true iff it is deletable
   * @param reason The reason for the segment deletion
   * @return The number of segments deleted
   */
  private def deleteOldSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean,
                                reason: SegmentDeletionReason): Int = {
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      highWatermark >= nextSegmentOpt.map(_.baseOffset).getOrElse(localLog.logEndOffset) &&
        predicate(segment, nextSegmentOpt)
    }
    lock synchronized {
      val deletable = localLog.deletableSegments(shouldDelete)
      if (deletable.nonEmpty)
        deleteSegments(deletable, reason)
      else
        0
    }
  }

  private def deleteSegments(deletable: Iterable[LogSegment], reason: SegmentDeletionReason): Int = {
    maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        if (localLog.segments.numberOfSegments == numToDelete)
          roll()
        lock synchronized {
          localLog.checkIfMemoryMappedBufferClosed()
          // remove the segments for lookups
          localLog.removeAndDeleteSegments(deletable, asyncDelete = true, reason)
          deleteProducerSnapshots(deletable, asyncDelete = true)
          maybeIncrementLogStartOffset(localLog.segments.firstSegmentBaseOffset.get, SegmentDeletion)
        }
      }
      numToDelete
    }
  }

  /**
   * If topic deletion is enabled, delete any local log segments that have either expired due to time based retention
   * or because the log size is > retentionSize.
   *
   * Whether or not deletion is enabled, delete any local log segments that are before the log start offset
   */
  def deleteOldSegments(): Int = {
    if (config.delete) {
      deleteLogStartOffsetBreachedSegments() +
        deleteRetentionSizeBreachedSegments() +
        deleteRetentionMsBreachedSegments()
    } else {
      deleteLogStartOffsetBreachedSegments()
    }
  }

  private def deleteRetentionMsBreachedSegments(): Int = {
    if (config.retentionMs < 0) return 0
    val startMs = time.milliseconds

    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      startMs - segment.largestTimestamp > config.retentionMs
    }

    deleteOldSegments(shouldDelete, RetentionMsBreach(this))
  }

  private def deleteRetentionSizeBreachedSegments(): Int = {
    if (config.retentionSize < 0 || size < config.retentionSize) return 0
    var diff = size - config.retentionSize
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }

    deleteOldSegments(shouldDelete, RetentionSizeBreach(this))
  }

  private def deleteLogStartOffsetBreachedSegments(): Int = {
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      nextSegmentOpt.exists(_.baseOffset <= logStartOffset)
    }

    deleteOldSegments(shouldDelete, StartOffsetBreach(this))
  }

  def isFuture: Boolean = localLog.isFuture

  /**
   * The size of the log in bytes
   */
  def size: Long = localLog.segments.sizeInBytes

  /**
   * The offset of the next message that will be appended to the log
   */
  def logEndOffset: Long =  localLog.logEndOffset

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  def logEndOffsetMetadata: LogOffsetMetadata = localLog.logEndOffsetMetadata

  /**
   * Roll the log over to a new empty log segment if necessary.
   * The segment will be rolled if one of the following conditions met:
   * 1. The logSegment is full
   * 2. The maxTime has elapsed since the timestamp of first message in the segment (or since the
   *    create time if the first message does not have a timestamp)
   * 3. The index is full
   *
   * @param messagesSize The messages set size in bytes.
   * @param appendInfo log append information
   *
   * @return  The currently active segment after (perhaps) rolling to a new segment
   */
  private def maybeRoll(messagesSize: Int, appendInfo: LogAppendInfo): LogSegment = lock synchronized {
    val segment = localLog.segments.activeSegment
    val now = time.milliseconds

    val maxTimestampInMessages = appendInfo.maxTimestamp
    val maxOffsetInMessages = appendInfo.lastOffset

    if (segment.shouldRoll(RollParams(config, appendInfo, messagesSize, now))) {
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
      val rollOffset = appendInfo
        .firstOffset
        .map(_.messageOffset)
        .getOrElse(maxOffsetInMessages - Integer.MAX_VALUE)

      roll(Some(rollOffset))
    } else {
      segment
    }
  }

  /**
   * Roll the local log over to a new active segment starting with the expectedNextOffset (when provided),
   * or localLog.logEndOffset otherwise. This will trim the index to the exact size of the number of entries
   * it currently contains.
   *
   * @return The newly rolled segment
   */
  def roll(expectedNextOffset: Option[Long] = None): LogSegment = lock synchronized {
    val newSegment = localLog.roll(expectedNextOffset)
    // Take a snapshot of the producer state to facilitate recovery. It is useful to have the snapshot
    // offset align with the new segment offset since this ensures we can recover the segment by beginning
    // with the corresponding snapshot file and scanning the segment data. Because the segment base offset
    // may actually be ahead of the current producer state end offset (which corresponds to the log end offset),
    // we manually override the state offset here prior to taking the snapshot.
    producerStateManager.updateMapEndOffset(newSegment.baseOffset)
    producerStateManager.takeSnapshot()
    updateHighWatermarkWithLogEndOffset()
    // Schedule an asynchronous flush of the old segment
    scheduler.schedule("flush-log", () => flush(newSegment.baseOffset))
    newSegment
  }

  /**
   * Flush all local log segments
   */
  def flush(): Unit = flush(logEndOffset)

  /**
   * Flush local log segments for all offsets up to offset-1
   *
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
   */
  def flush(offset: Long): Unit = {
    maybeHandleIOException(s"Error while flushing log for $topicPartition in dir ${dir.getParent} with offset $offset") {
      if (offset > localLog.recoveryPoint) {
        debug(s"Flushing log up to offset $offset, last flushed: $lastFlushTime,  current time: ${time.milliseconds()}, " +
          s"unflushed: ${localLog.unflushedMessages}")
        localLog.flush(offset)
        lock synchronized {
          localLog.markFlushed(offset)
        }
      }
    }
  }

  /**
   * Completely delete the local log directory and all contents from the file system with no delay
   */
  private[log] def delete(): Unit = {
    maybeHandleIOException(s"Error while deleting log for $topicPartition in dir ${dir.getParent}") {
      lock synchronized {
        localLog.checkIfMemoryMappedBufferClosed()
        producerExpireCheck.cancel(true)
        leaderEpochCache.foreach(_.clear())
        val deletedSegments = localLog.deleteAllSegments()
        deleteProducerSnapshots(deletedSegments, asyncDelete = false)
        localLog.deleteEmptyDir()
      }
    }
  }

  // visible for testing
  private[log] def takeProducerSnapshot(): Unit = lock synchronized {
    localLog.checkIfMemoryMappedBufferClosed()
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
  private[kafka] def truncateTo(targetOffset: Long): Boolean = {
    maybeHandleIOException(s"Error while truncating log to offset $targetOffset for $topicPartition in dir ${dir.getParent}") {
      if (targetOffset < 0)
        throw new IllegalArgumentException(s"Cannot truncate partition $topicPartition to a negative offset (%d).".format(targetOffset))
      if (targetOffset >= localLog.logEndOffset) {
        info(s"Truncating to $targetOffset has no effect as the largest offset in the log is ${localLog.logEndOffset - 1}")

        // Always truncate epoch cache since we may have a conflicting epoch entry at the
        // end of the log from the leader. This could happen if this broker was a leader
        // and inserted the first start offset entry, but then failed to append any entries
        // before another leader was elected.
        lock synchronized {
          leaderEpochCache.foreach(_.truncateFromEnd(logEndOffset))
        }

        false
      } else {
        info(s"Truncating to offset $targetOffset")
        lock synchronized {
          localLog.checkIfMemoryMappedBufferClosed()
          if (localLog.segments.firstSegmentBaseOffset.get > targetOffset) {
            truncateFullyAndStartAt(targetOffset)
          } else {
            val deletedSegments = localLog.truncateTo(targetOffset)
            deleteProducerSnapshots(deletedSegments, asyncDelete = true)
            leaderEpochCache.foreach(_.truncateFromEnd(targetOffset))
            logStartOffset = math.min(targetOffset, logStartOffset)
            rebuildProducerState(targetOffset, producerStateManager)
            if (highWatermark >= localLog.logEndOffset)
              updateHighWatermark(localLog.logEndOffsetMetadata)
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
  def truncateFullyAndStartAt(newOffset: Long): Unit = {
    maybeHandleIOException(s"Error while truncating the entire log for $topicPartition in dir ${dir.getParent}") {
      debug(s"Truncate and start at offset $newOffset")
      lock synchronized {
        localLog.truncateFullyAndStartAt(newOffset)
        leaderEpochCache.foreach(_.clearAndFlush())
        producerStateManager.truncateFullyAndStartAt(newOffset)
        logStartOffset = newOffset
        rebuildProducerState(newOffset, producerStateManager)
        updateHighWatermark(localLog.logEndOffsetMetadata)
      }
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime: Long = localLog.lastFlushTime

  /**
   * The active segment that is currently taking appends
   */
  def activeSegment: LogSegment = localLog.segments.activeSegment

  /**
   * All the log segments in this log ordered from oldest to newest
   */
  def logSegments: Iterable[LogSegment] = localLog.segments.values

  /**
   * Get all segments beginning with the segment that includes "from" and ending with the segment
   * that includes up to "to-1" or the end of the log (if to > logEndOffset).
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = lock synchronized {
    localLog.segments.values(from, to)
  }

  def nonActiveLogSegmentsFrom(from: Long): Iterable[LogSegment] = lock synchronized {
    localLog.segments.nonActiveLogSegmentsFrom(from)
  }

  override def toString: String = {
    val logString = new StringBuilder
    logString.append(s"Log(dir=$dir")
    topicId.foreach(id => logString.append(s", topicId=$id"))
    logString.append(s", topic=${topicPartition.topic}")
    logString.append(s", partition=${topicPartition.partition}")
    logString.append(s", highWatermark=$highWatermark")
    logString.append(s", lastStableOffset=$lastStableOffset")
    logString.append(s", logStartOffset=$logStartOffset")
    logString.append(s", logEndOffset=$logEndOffset")
    logString.append(")")
    logString.toString
  }

  private[log] def replaceSegments(newSegments: Seq[LogSegment], oldSegments: Seq[LogSegment]): Unit = {
    lock synchronized {
      localLog.checkIfMemoryMappedBufferClosed()
      val deletedSegments = UnifiedLog.replaceSegments(localLog.segments, newSegments, oldSegments, dir, topicPartition,
        config, scheduler, logDirFailureChannel, logIdent)
      deleteProducerSnapshots(deletedSegments, asyncDelete = true)
    }
  }

  /**
   * This function does not acquire Log.lock. The caller has to make sure log segments don't get deleted during
   * this call, and also protects against calling this function on the same segment in parallel.
   *
   * Currently, it is used by LogCleaner threads on log compact non-active segments only with LogCleanerManager's lock
   * to ensure no other logcleaner threads and retention thread can work on the same segment.
   */
  private[log] def getFirstBatchTimestampForSegments(segments: Iterable[LogSegment]): Iterable[Long] = {
    LogSegments.getFirstBatchTimestampForSegments(segments)
  }

  /**
   * remove deleted log metrics
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric(LogMetricNames.NumLogSegments, tags)
    removeMetric(LogMetricNames.LogStartOffset, tags)
    removeMetric(LogMetricNames.LogEndOffset, tags)
    removeMetric(LogMetricNames.Size, tags)
  }

  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   * @param segment The segment to add
   */
  @threadsafe
  private[log] def addSegment(segment: LogSegment): LogSegment = localLog.segments.add(segment)

  private def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    LocalLog.maybeHandleIOException(logDirFailureChannel, parentDir, msg) {
      fun
    }
  }

  private[log] def splitOverflowedSegment(segment: LogSegment): List[LogSegment] = lock synchronized {
    val result = UnifiedLog.splitOverflowedSegment(segment, localLog.segments, dir, topicPartition, config, scheduler, logDirFailureChannel, logIdent)
    deleteProducerSnapshots(result.deletedSegments, asyncDelete = true)
    result.newSegments.toList
  }

  private[log] def deleteProducerSnapshots(segments: Iterable[LogSegment], asyncDelete: Boolean): Unit = {
    UnifiedLog.deleteProducerSnapshots(segments, producerStateManager, asyncDelete, scheduler, config, logDirFailureChannel, parentDir, topicPartition)
  }
}

object UnifiedLog extends Logging {
  val LogFileSuffix = LocalLog.LogFileSuffix

  val IndexFileSuffix = LocalLog.IndexFileSuffix

  val TimeIndexFileSuffix = LocalLog.TimeIndexFileSuffix

  val ProducerSnapshotFileSuffix = ".snapshot"

  val TxnIndexFileSuffix = LocalLog.TxnIndexFileSuffix

  val DeletedFileSuffix = LocalLog.DeletedFileSuffix

  val CleanedFileSuffix = LocalLog.CleanedFileSuffix

  val SwapFileSuffix = LocalLog.SwapFileSuffix

  val DeleteDirSuffix = LocalLog.DeleteDirSuffix

  val FutureDirSuffix = LocalLog.FutureDirSuffix

  private[log] val DeleteDirPattern = LocalLog.DeleteDirPattern
  private[log] val FutureDirPattern = LocalLog.FutureDirPattern

  val UnknownOffset = LocalLog.UnknownOffset

  def apply(dir: File,
            config: LogConfig,
            logStartOffset: Long,
            recoveryPoint: Long,
            scheduler: Scheduler,
            brokerTopicStats: BrokerTopicStats,
            time: Time = Time.SYSTEM,
            maxProducerIdExpirationMs: Int,
            producerIdExpirationCheckIntervalMs: Int,
            logDirFailureChannel: LogDirFailureChannel,
            lastShutdownClean: Boolean = true,
            topicId: Option[Uuid],
            keepPartitionMetadataFile: Boolean): UnifiedLog = {
    // create the log directory if it doesn't exist
    Files.createDirectories(dir.toPath)
    val topicPartition = UnifiedLog.parseTopicPartitionName(dir)
    val segments = new LogSegments(topicPartition)
    val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(
      dir,
      topicPartition,
      logDirFailureChannel,
      config.recordVersion,
      s"[UnifiedLog partition=$topicPartition, dir=${dir.getParent}] ")
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    val offsets = LogLoader.load(LoadLogParams(
      dir,
      topicPartition,
      config,
      scheduler,
      time,
      logDirFailureChannel,
      lastShutdownClean,
      segments,
      logStartOffset,
      recoveryPoint,
      maxProducerIdExpirationMs,
      leaderEpochCache,
      producerStateManager))
    val localLog = new LocalLog(dir, config, segments, offsets.recoveryPoint,
      offsets.nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel)
    new UnifiedLog(offsets.logStartOffset,
      localLog,
      brokerTopicStats,
      producerIdExpirationCheckIntervalMs,
      leaderEpochCache,
      producerStateManager,
      topicId,
      keepPartitionMetadataFile)
  }

  def logFile(dir: File, offset: Long, suffix: String = ""): File = LocalLog.logFile(dir, offset, suffix)

  def logDeleteDirName(topicPartition: TopicPartition): String = LocalLog.logDeleteDirName(topicPartition)

  def logFutureDirName(topicPartition: TopicPartition): String = LocalLog.logFutureDirName(topicPartition)

  def logDirName(topicPartition: TopicPartition): String = LocalLog.logDirName(topicPartition)

  def offsetIndexFile(dir: File, offset: Long, suffix: String = ""): File = LocalLog.offsetIndexFile(dir, offset, suffix)

  def timeIndexFile(dir: File, offset: Long, suffix: String = ""): File = LocalLog.timeIndexFile(dir, offset, suffix)

  def deleteFileIfExists(file: File, suffix: String = ""): Unit =
    Files.deleteIfExists(new File(file.getPath + suffix).toPath)

  /**
   * Construct a producer id snapshot file using the given offset.
   *
   * @param dir    The directory in which the log will reside
   * @param offset The last offset (exclusive) included in the snapshot
   */
  def producerSnapshotFile(dir: File, offset: Long): File =
    new File(dir, LocalLog.filenamePrefixFromOffset(offset) + ProducerSnapshotFileSuffix)

  def transactionIndexFile(dir: File, offset: Long, suffix: String = ""): File = LocalLog.transactionIndexFile(dir, offset, suffix)

  def offsetFromFileName(filename: String): Long = LocalLog.offsetFromFileName(filename)

  def offsetFromFile(file: File): Long = LocalLog.offsetFromFile(file)

  def sizeInBytes(segments: Iterable[LogSegment]): Long = LogSegments.sizeInBytes(segments)

  def parseTopicPartitionName(dir: File): TopicPartition = LocalLog.parseTopicPartitionName(dir)

  private[log] def isIndexFile(file: File): Boolean = LocalLog.isIndexFile(file)

  private[log] def isLogFile(file: File): Boolean = LocalLog.isLogFile(file)

  private def loadProducersFromRecords(producerStateManager: ProducerStateManager, records: Records): Unit = {
    val loadedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    records.batches.forEach { batch =>
      if (batch.hasProducerId) {
        val maybeCompletedTxn = updateProducers(
          producerStateManager,
          batch,
          loadedProducers,
          firstOffsetMetadata = None,
          origin = AppendOrigin.Replication)
        maybeCompletedTxn.foreach(completedTxns += _)
      }
    }
    loadedProducers.values.foreach(producerStateManager.update)
    completedTxns.foreach(producerStateManager.completeTxn)
  }

  private def updateProducers(producerStateManager: ProducerStateManager,
                              batch: RecordBatch,
                              producers: mutable.Map[Long, ProducerAppendInfo],
                              firstOffsetMetadata: Option[LogOffsetMetadata],
                              origin: AppendOrigin): Option[CompletedTxn] = {
    val producerId = batch.producerId
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, origin))
    appendInfo.append(batch, firstOffsetMetadata)
  }

  /**
   * If the recordVersion is >= RecordVersion.V2, then create and return a LeaderEpochFileCache.
   * Otherwise, the message format is considered incompatible and the existing LeaderEpoch file
   * is deleted.
   *
   * @param dir                  The directory in which the log will reside
   * @param topicPartition       The topic partition
   * @param logDirFailureChannel The LogDirFailureChannel to asynchronously handle log dir failure
   * @param recordVersion The record version
   * @param logPrefix The logging prefix
   * @return The new LeaderEpochFileCache instance (if created), none otherwise
   */
  def maybeCreateLeaderEpochCache(dir: File,
                                  topicPartition: TopicPartition,
                                  logDirFailureChannel: LogDirFailureChannel,
                                  recordVersion: RecordVersion,
                                  logPrefix: String): Option[LeaderEpochFileCache] = {
    val leaderEpochFile = LeaderEpochCheckpointFile.newFile(dir)

    def newLeaderEpochFileCache(): LeaderEpochFileCache = {
      val checkpointFile = new LeaderEpochCheckpointFile(leaderEpochFile, logDirFailureChannel)
      new LeaderEpochFileCache(topicPartition, checkpointFile)
    }

    if (recordVersion.precedes(RecordVersion.V2)) {
      val currentCache = if (leaderEpochFile.exists())
        Some(newLeaderEpochFileCache())
      else
        None

      if (currentCache.exists(_.nonEmpty))
        warn(s"${logPrefix}Deleting non-empty leader epoch cache due to incompatible message format $recordVersion")

      Files.deleteIfExists(leaderEpochFile.toPath)
      None
    } else {
      Some(newLeaderEpochFileCache())
    }
  }

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
    LocalLog.replaceSegments(existingSegments,
      newSegments,
      oldSegments,
      dir,
      topicPartition,
      config,
      scheduler,
      logDirFailureChannel,
      logPrefix,
      isRecoveredSwapFile)
  }

  private[log] def deleteSegmentFiles(segmentsToDelete: immutable.Iterable[LogSegment],
                                      asyncDelete: Boolean,
                                      dir: File,
                                      topicPartition: TopicPartition,
                                      config: LogConfig,
                                      scheduler: Scheduler,
                                      logDirFailureChannel: LogDirFailureChannel,
                                      logPrefix: String): Unit = {
    LocalLog.deleteSegmentFiles(segmentsToDelete, asyncDelete, dir, topicPartition, config, scheduler, logDirFailureChannel, logPrefix)
  }

  /**
   * Rebuilds producer state until the provided lastOffset. This function may be called from the
   * recovery code path, and thus must be free of all side-effects, i.e. it must not update any
   * log-specific state.
   *
   * @param producerStateManager    The ProducerStateManager instance to be rebuilt.
   * @param segments                The segments of the log whose producer state is being rebuilt
   * @param logStartOffset          The log start offset
   * @param lastOffset              The last offset upto which the producer state needs to be rebuilt
   * @param recordVersion           The record version
   * @param time                    The time instance used for checking the clock
   * @param reloadFromCleanShutdown True if the producer state is being built after a clean shutdown,
   *                                false otherwise.
   * @param logPrefix The logging prefix
   */
  private[log] def rebuildProducerState(producerStateManager: ProducerStateManager,
                                        segments: LogSegments,
                                        logStartOffset: Long,
                                        lastOffset: Long,
                                        recordVersion: RecordVersion,
                                        time: Time,
                                        reloadFromCleanShutdown: Boolean,
                                        logPrefix: String): Unit = {
    val offsetsToSnapshot =
      if (segments.nonEmpty) {
        val lastSegmentBaseOffset = segments.lastSegment.get.baseOffset
        val nextLatestSegmentBaseOffset = segments.lowerSegment(lastSegmentBaseOffset).map(_.baseOffset)
        Seq(nextLatestSegmentBaseOffset, Some(lastSegmentBaseOffset), Some(lastOffset))
      } else {
        Seq(Some(lastOffset))
      }
    info(s"${logPrefix}Loading producer state till offset $lastOffset with message format version ${recordVersion.value}")

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
    if (recordVersion.value < RecordBatch.MAGIC_VALUE_V2 ||
      (producerStateManager.latestSnapshotOffset.isEmpty && reloadFromCleanShutdown)) {
      // To avoid an expensive scan through all of the segments, we take empty snapshots from the start of the
      // last two segments and the last offset. This should avoid the full scan in the case that the log needs
      // truncation.
      offsetsToSnapshot.flatten.foreach { offset =>
        producerStateManager.updateMapEndOffset(offset)
        producerStateManager.takeSnapshot()
      }
    } else {
      info(s"${logPrefix}Reloading from producer snapshot and rebuilding producer state from offset $lastOffset")
      val isEmptyBeforeTruncation = producerStateManager.isEmpty && producerStateManager.mapEndOffset >= lastOffset
      val producerStateLoadStart = time.milliseconds()
      producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds())
      val segmentRecoveryStart = time.milliseconds()

      // Only do the potentially expensive reloading if the last snapshot offset is lower than the log end
      // offset (which would be the case on first startup) and there were active producers prior to truncation
      // (which could be the case if truncating after initial loading). If there weren't, then truncating
      // shouldn't change that fact (although it could cause a producerId to expire earlier than expected),
      // and we can skip the loading. This is an optimization for users which are not yet using
      // idempotent/transactional features yet.
      if (lastOffset > producerStateManager.mapEndOffset && !isEmptyBeforeTruncation) {
        val segmentOfLastOffset = segments.floorSegment(lastOffset)

        segments.values(producerStateManager.mapEndOffset, lastOffset).foreach { segment =>
          val startOffset = Utils.max(segment.baseOffset, producerStateManager.mapEndOffset, logStartOffset)
          producerStateManager.updateMapEndOffset(startOffset)

          if (offsetsToSnapshot.contains(Some(segment.baseOffset)))
            producerStateManager.takeSnapshot()

          val maxPosition = if (segmentOfLastOffset.contains(segment)) {
            Option(segment.translateOffset(lastOffset))
              .map(_.position)
              .getOrElse(segment.size)
          } else {
            segment.size
          }

          val fetchDataInfo = segment.read(startOffset,
            maxSize = Int.MaxValue,
            maxPosition = maxPosition)
          if (fetchDataInfo != null)
            loadProducersFromRecords(producerStateManager, fetchDataInfo.records)
        }
      }
      producerStateManager.updateMapEndOffset(lastOffset)
      producerStateManager.takeSnapshot()
      info(s"${logPrefix}Producer state recovery took ${segmentRecoveryStart - producerStateLoadStart}ms for snapshot load " +
        s"and ${time.milliseconds() - segmentRecoveryStart}ms for segment recovery from offset $lastOffset")
    }
  }

  private[log] def splitOverflowedSegment(segment: LogSegment,
                                          existingSegments: LogSegments,
                                          dir: File,
                                          topicPartition: TopicPartition,
                                          config: LogConfig,
                                          scheduler: Scheduler,
                                          logDirFailureChannel: LogDirFailureChannel,
                                          logPrefix: String): SplitSegmentResult = {
    LocalLog.splitOverflowedSegment(segment, existingSegments, dir, topicPartition, config, scheduler, logDirFailureChannel, logPrefix)
  }

  private[log] def deleteProducerSnapshots(segments: Iterable[LogSegment],
                                           producerStateManager: ProducerStateManager,
                                           asyncDelete: Boolean,
                                           scheduler: Scheduler,
                                           config: LogConfig,
                                           logDirFailureChannel: LogDirFailureChannel,
                                           parentDir: String,
                                           topicPartition: TopicPartition): Unit = {
    val snapshotsToDelete = segments.flatMap { segment =>
      producerStateManager.removeAndMarkSnapshotForDeletion(segment.baseOffset)}
    def deleteProducerSnapshots(): Unit = {
      LocalLog.maybeHandleIOException(logDirFailureChannel,
        parentDir,
        s"Error while deleting producer state snapshots for $topicPartition in dir $parentDir") {
        snapshotsToDelete.foreach { snapshot =>
          snapshot.deleteIfExists()
        }
      }
    }

    if (asyncDelete)
      scheduler.schedule("delete-producer-snapshot", () => deleteProducerSnapshots(), delay = config.fileDeleteDelayMs)
    else
      deleteProducerSnapshots()
  }

  private[log] def createNewCleanedSegment(dir: File, logConfig: LogConfig, baseOffset: Long): LogSegment = {
    LocalLog.createNewCleanedSegment(dir, logConfig, baseOffset)
  }
}

object LogMetricNames {
  val NumLogSegments: String = "NumLogSegments"
  val LogStartOffset: String = "LogStartOffset"
  val LogEndOffset: String = "LogEndOffset"
  val Size: String = "Size"

  def allMetricNames: List[String] = {
    List(NumLogSegments, LogStartOffset, LogEndOffset, Size)
  }
}

case class RetentionMsBreach(log: UnifiedLog) extends SegmentDeletionReason {
  override def logReason(toDelete: List[LogSegment]): Unit = {
    val retentionMs = log.config.retentionMs
    toDelete.foreach { segment =>
      segment.largestRecordTimestamp match {
        case Some(_) =>
          log.info(s"Deleting segment $segment due to retention time ${retentionMs}ms breach based on the largest " +
            s"record timestamp in the segment")
        case None =>
          log.info(s"Deleting segment $segment due to retention time ${retentionMs}ms breach based on the " +
            s"last modified time of the segment")
      }
    }
  }
}

case class RetentionSizeBreach(log: UnifiedLog) extends SegmentDeletionReason {
  override def logReason(toDelete: List[LogSegment]): Unit = {
    var size = log.size
    toDelete.foreach { segment =>
      size -= segment.size
      log.info(s"Deleting segment $segment due to retention size ${log.config.retentionSize} breach. Log size " +
        s"after deletion will be $size.")
    }
  }
}

case class StartOffsetBreach(log: UnifiedLog) extends SegmentDeletionReason {
  override def logReason(toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments due to log start offset ${log.logStartOffset} breach: ${toDelete.mkString(",")}")
  }
}
