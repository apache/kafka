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
package kafka.raft

import kafka.log.UnifiedLog
import kafka.raft.KafkaMetadataLog.FullTruncation
import kafka.raft.KafkaMetadataLog.RetentionMsBreach
import kafka.raft.KafkaMetadataLog.RetentionSizeBreach
import kafka.raft.KafkaMetadataLog.SnapshotDeletionReason
import kafka.raft.KafkaMetadataLog.UnknownReason
import kafka.utils.Logging
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
import org.apache.kafka.raft.{Isolation, KafkaRaftClient, LogAppendInfo, LogFetchInfo, LogOffsetMetadata, OffsetAndEpoch, OffsetMetadata, ReplicatedLog, SegmentPosition, ValidOffsetAndEpoch}
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.server.config.{KRaftConfigs, ServerLogConfigs}
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.snapshot.FileRawSnapshotReader
import org.apache.kafka.snapshot.FileRawSnapshotWriter
import org.apache.kafka.snapshot.NotifyingRawSnapshotWriter
import org.apache.kafka.snapshot.RawSnapshotReader
import org.apache.kafka.snapshot.RawSnapshotWriter
import org.apache.kafka.snapshot.SnapshotPath
import org.apache.kafka.snapshot.Snapshots
import org.apache.kafka.storage.internals
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, LogDirFailureChannel, LogStartOffsetIncrementReason, ProducerStateManagerConfig}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.io.File
import java.nio.file.{Files, NoSuchFileException, Path}
import java.util.{Optional, Properties}
import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.OptionConverters.{RichOption, RichOptional}

final class KafkaMetadataLog private (
  val log: UnifiedLog,
  time: Time,
  scheduler: Scheduler,
  // Access to this object needs to be synchronized because it is used by the snapshotting thread to notify the
  // polling thread when snapshots are created. This object is also used to store any opened snapshot reader.
  snapshots: mutable.TreeMap[OffsetAndEpoch, Option[FileRawSnapshotReader]],
  topicPartition: TopicPartition,
  config: MetadataLogConfig
) extends ReplicatedLog with Logging {

  this.logIdent = s"[MetadataLog partition=$topicPartition, nodeId=${config.nodeId}] "

  override def read(startOffset: Long, readIsolation: Isolation): LogFetchInfo = {
    val isolation = readIsolation match {
      case Isolation.COMMITTED => FetchIsolation.HIGH_WATERMARK
      case Isolation.UNCOMMITTED => FetchIsolation.LOG_END
      case _ => throw new IllegalArgumentException(s"Unhandled read isolation $readIsolation")
    }

    val fetchInfo = log.read(startOffset,
      maxLength = config.maxFetchSizeInBytes,
      isolation = isolation,
      minOneMessage = true)

    new LogFetchInfo(
      fetchInfo.records,

      new LogOffsetMetadata(
        fetchInfo.fetchOffsetMetadata.messageOffset,
        Optional.of(new SegmentPosition(
          fetchInfo.fetchOffsetMetadata.segmentBaseOffset,
          fetchInfo.fetchOffsetMetadata.relativePositionInSegment))
        )
    )
  }

  override def appendAsLeader(records: Records, epoch: Int): LogAppendInfo = {
    if (records.sizeInBytes == 0)
      throw new IllegalArgumentException("Attempt to append an empty record set")

    handleAndConvertLogAppendInfo(
      log.appendAsLeader(records.asInstanceOf[MemoryRecords],
        leaderEpoch = epoch,
        origin = AppendOrigin.RAFT_LEADER,
        requestLocal = RequestLocal.noCaching
      )
    )
  }

  override def appendAsFollower(records: Records): LogAppendInfo = {
    if (records.sizeInBytes == 0)
      throw new IllegalArgumentException("Attempt to append an empty record set")

    handleAndConvertLogAppendInfo(log.appendAsFollower(records.asInstanceOf[MemoryRecords]))
  }

  private def handleAndConvertLogAppendInfo(appendInfo: internals.log.LogAppendInfo): LogAppendInfo = {
    if (appendInfo.firstOffset != UnifiedLog.UnknownOffset)
      new LogAppendInfo(appendInfo.firstOffset, appendInfo.lastOffset)
    else
      throw new KafkaException(s"Append failed unexpectedly")
  }

  override def lastFetchedEpoch: Int = {
    log.latestEpoch.getOrElse {
      latestSnapshotId().map[Int] { snapshotId =>
        val logEndOffset = endOffset().offset
        if (snapshotId.offset == startOffset && snapshotId.offset == logEndOffset) {
          // Return the epoch of the snapshot when the log is empty
          snapshotId.epoch
        } else {
          throw new KafkaException(
            s"Log doesn't have a last fetch epoch and there is a snapshot ($snapshotId). " +
            s"Expected the snapshot's end offset to match the log's end offset ($logEndOffset) " +
            s"and the log start offset ($startOffset)"
          )
        }
      }.orElse(0)
    }
  }

  override def endOffsetForEpoch(epoch: Int): OffsetAndEpoch = {
    (log.endOffsetForEpoch(epoch), earliestSnapshotId().toScala) match {
      case (Some(offsetAndEpoch), Some(snapshotId)) if (
        offsetAndEpoch.offset == snapshotId.offset &&
        offsetAndEpoch.leaderEpoch == epoch) =>

        // The epoch is smaller than the smallest epoch on the log. Override the diverging
        // epoch to the oldest snapshot which should be the snapshot at the log start offset
        new OffsetAndEpoch(snapshotId.offset, snapshotId.epoch)

      case (Some(offsetAndEpoch), _) =>
        new OffsetAndEpoch(offsetAndEpoch.offset, offsetAndEpoch.leaderEpoch)

      case (None, _) =>
        new OffsetAndEpoch(endOffset.offset, lastFetchedEpoch)
    }
  }

  override def endOffset: LogOffsetMetadata = {
    val endOffsetMetadata = log.logEndOffsetMetadata
    new LogOffsetMetadata(
      endOffsetMetadata.messageOffset,
      Optional.of(new SegmentPosition(
        endOffsetMetadata.segmentBaseOffset,
        endOffsetMetadata.relativePositionInSegment)
      )
    )
  }

  override def startOffset: Long = {
    log.logStartOffset
  }

  override def truncateTo(offset: Long): Unit = {
    if (offset < highWatermark.offset) {
      throw new IllegalArgumentException(s"Attempt to truncate to offset $offset, which is below " +
        s"the current high watermark ${highWatermark.offset}")
    }
    log.truncateTo(offset)
  }

  override def truncateToLatestSnapshot(): Boolean = {
    val latestEpoch = log.latestEpoch.getOrElse(0)
    val (truncated, forgottenSnapshots) = latestSnapshotId().toScala match {
      case Some(snapshotId) if (
          snapshotId.epoch > latestEpoch ||
          (snapshotId.epoch == latestEpoch && snapshotId.offset > endOffset().offset)
        ) =>
        // Truncate the log fully if the latest snapshot is greater than the log end offset
        log.truncateFullyAndStartAt(snapshotId.offset)

        // Forget snapshots less than the log start offset
        snapshots synchronized {
          (true, forgetSnapshotsBefore(snapshotId))
        }
      case _ =>
        (false, mutable.TreeMap.empty[OffsetAndEpoch, Option[FileRawSnapshotReader]])
    }

    removeSnapshots(forgottenSnapshots, FullTruncation)
    truncated
  }

  override def initializeLeaderEpoch(epoch: Int): Unit = {
    log.maybeAssignEpochStartOffset(epoch, log.logEndOffset)
  }

  override def updateHighWatermark(offsetMetadata: LogOffsetMetadata): Unit = {
    // This API returns the new high watermark, which may be different from the passed offset
    val logHighWatermark = offsetMetadata.metadata.toScala match {
      case Some(segmentPosition: SegmentPosition) =>
        log.updateHighWatermark(
          new internals.log.LogOffsetMetadata(
            offsetMetadata.offset,
            segmentPosition.baseOffset,
            segmentPosition.relativePosition
          )
        )
      case _ =>
        log.updateHighWatermark(offsetMetadata.offset)
    }

    // Temporary log message until we fix KAFKA-14825
    if (logHighWatermark != offsetMetadata.offset) {
      warn(
        s"Log's high watermark ($logHighWatermark) is different from the local replica's high watermark ($offsetMetadata)"
      )
    }
  }

  override def highWatermark: LogOffsetMetadata = {
    val hwm = log.fetchOffsetSnapshot.highWatermark
    val segmentPosition: Optional[OffsetMetadata] = if (!hwm.messageOffsetOnly) {
      Optional.of(new SegmentPosition(hwm.segmentBaseOffset, hwm.relativePositionInSegment))
    } else {
      Optional.empty()
    }

    new LogOffsetMetadata(hwm.messageOffset, segmentPosition)
  }

  override def flush(forceFlushActiveSegment: Boolean): Unit = {
    log.flush(forceFlushActiveSegment)
  }

  /**
   * Return the topic partition associated with the log.
   */
  override def topicPartition(): TopicPartition = {
    topicPartition
  }

  /**
   * Return the topic ID associated with the log.
   */
  override def topicId(): Uuid = {
    log.topicId.get
  }

  override def createNewSnapshot(snapshotId: OffsetAndEpoch): Optional[RawSnapshotWriter] = {
    if (snapshotId.offset < startOffset) {
      info(s"Cannot create a snapshot with an id ($snapshotId) less than the log start offset ($startOffset)")
      return Optional.empty()
    }

    val highWatermarkOffset = highWatermark.offset
    if (snapshotId.offset > highWatermarkOffset) {
      throw new IllegalArgumentException(
        s"Cannot create a snapshot with an id ($snapshotId) greater than the high-watermark ($highWatermarkOffset)"
      )
    }

    val validOffsetAndEpoch = validateOffsetAndEpoch(snapshotId.offset, snapshotId.epoch)
    if (validOffsetAndEpoch.kind() != ValidOffsetAndEpoch.Kind.VALID) {
      throw new IllegalArgumentException(
        s"Snapshot id ($snapshotId) is not valid according to the log: $validOffsetAndEpoch"
      )
    }

    /*
      Perform a check that the requested snapshot offset is batch aligned via a log read, which
      returns the base offset of the batch that contains the requested offset. A snapshot offset
      is one greater than the last offset contained in the snapshot, and cannot go past the high
      watermark.

      This check is necessary because Raft replication code assumes the snapshot offset is the
      start of a batch. If a follower applies a non-batch aligned snapshot at offset (X) and
      fetches from this offset, the returned batch will start at offset (X - M), and the
      follower will be unable to append it since (X - M) < (X).
     */
    val baseOffset = read(snapshotId.offset, Isolation.COMMITTED).startOffsetMetadata.offset
    if (snapshotId.offset != baseOffset) {
      throw new IllegalArgumentException(
        s"Cannot create snapshot at offset (${snapshotId.offset}) because it is not batch aligned. " +
        s"The batch containing the requested offset has a base offset of ($baseOffset)"
      )
    }

    createNewSnapshotUnchecked(snapshotId)
  }

  override def createNewSnapshotUnchecked(snapshotId: OffsetAndEpoch): Optional[RawSnapshotWriter] = {
    val containsSnapshotId = snapshots synchronized {
      snapshots.contains(snapshotId)
    }

    if (containsSnapshotId) {
      Optional.empty()
    } else {
      Optional.of(
        new NotifyingRawSnapshotWriter(
          FileRawSnapshotWriter.create(log.dir.toPath, snapshotId),
          onSnapshotFrozen
        )
      )
    }
  }

  override def readSnapshot(snapshotId: OffsetAndEpoch): Optional[RawSnapshotReader] = {
    snapshots synchronized {
      val reader = snapshots.get(snapshotId) match {
        case None =>
          // Snapshot doesn't exists
          None
        case Some(None) =>
          // Snapshot exists but has never been read before
          try {
            val snapshotReader = Some(FileRawSnapshotReader.open(log.dir.toPath, snapshotId))
            snapshots.put(snapshotId, snapshotReader)
            snapshotReader
          } catch {
            case _: NoSuchFileException =>
              // Snapshot doesn't exists in the data dir; remove
              val path = Snapshots.snapshotPath(log.dir.toPath, snapshotId)
              warn(s"Couldn't read $snapshotId; expected to find snapshot file $path")
              snapshots.remove(snapshotId)
              None
          }
        case Some(value) =>
          // Snapshot exists and it is already open; do nothing
          value
      }

      reader.toJava.asInstanceOf[Optional[RawSnapshotReader]]
    }
  }

  override def latestSnapshot(): Optional[RawSnapshotReader] = {
    snapshots synchronized {
      latestSnapshotId().flatMap(readSnapshot)
    }
  }

  override def latestSnapshotId(): Optional[OffsetAndEpoch] = {
    snapshots synchronized {
      snapshots.lastOption.map { case (snapshotId, _) => snapshotId }.toJava
    }
  }

  override def earliestSnapshotId(): Optional[OffsetAndEpoch] = {
    snapshots synchronized {
      snapshots.headOption.map { case (snapshotId, _) => snapshotId }.toJava
    }
  }

  override def onSnapshotFrozen(snapshotId: OffsetAndEpoch): Unit = {
    snapshots synchronized {
      snapshots.put(snapshotId, None)
    }
  }

  /**
   * Delete snapshots that come before a given snapshot ID. This is done by advancing the log start offset to the given
   * snapshot and cleaning old log segments.
   *
   * This will only happen if the following invariants all hold true:
   *
   * <li>The given snapshot precedes the latest snapshot</li>
   * <li>The offset of the given snapshot is greater than the log start offset</li>
   * <li>The log layer can advance the offset to the given snapshot</li>
   *
   * This method is thread-safe
   */
  override def deleteBeforeSnapshot(snapshotId: OffsetAndEpoch): Boolean = {
    deleteBeforeSnapshot(snapshotId, UnknownReason)
  }

  private def deleteBeforeSnapshot(snapshotId: OffsetAndEpoch, reason: SnapshotDeletionReason): Boolean = {
    val (deleted, forgottenSnapshots) = snapshots synchronized {
      latestSnapshotId().toScala match {
        case Some(latestSnapshotId) if
          snapshots.contains(snapshotId) &&
          startOffset < snapshotId.offset &&
          snapshotId.offset <= latestSnapshotId.offset &&
          log.maybeIncrementLogStartOffset(snapshotId.offset, LogStartOffsetIncrementReason.SnapshotGenerated) =>
            // Delete all segments that have a "last offset" less than the log start offset
            val deletedSegments = log.deleteOldSegments()
            // Remove older snapshots from the snapshots cache
            val forgottenSnapshots = forgetSnapshotsBefore(snapshotId)
            (deletedSegments != 0 || forgottenSnapshots.nonEmpty, forgottenSnapshots)
        case _ =>
          (false, mutable.TreeMap.empty[OffsetAndEpoch, Option[FileRawSnapshotReader]])
      }
    }
    removeSnapshots(forgottenSnapshots, reason)
    deleted
  }

  /**
   * Force all known snapshots to have an open reader so we can know their sizes. This method is not thread-safe
   */
  private def loadSnapshotSizes(): Seq[(OffsetAndEpoch, Long)] = {
    snapshots.keys.toSeq.flatMap {
      snapshotId => readSnapshot(snapshotId).toScala.map { reader => (snapshotId, reader.sizeInBytes())}
    }
  }

  /**
   * Return the max timestamp of the first batch in a snapshot, if the snapshot exists and has records
   */
  private def readSnapshotTimestamp(snapshotId: OffsetAndEpoch): Option[Long] = {
    readSnapshot(snapshotId).toScala.map { reader =>
      Snapshots.lastContainedLogTimestamp(reader)
    }
  }

  /**
   * Perform cleaning of old snapshots and log segments based on size and time.
   *
   * If our configured retention size has been violated, we perform cleaning as follows:
   *
   * <li>Find oldest snapshot and delete it</li>
   * <li>Advance log start offset to end of next oldest snapshot</li>
   * <li>Delete log segments which wholly precede the new log start offset</li>
   *
   * This process is repeated until the retention size is no longer violated, or until only
   * a single snapshot remains.
   */
  override def maybeClean(): Boolean = {
    snapshots synchronized {
      var didClean = false
      didClean |= cleanSnapshotsRetentionSize()
      didClean |= cleanSnapshotsRetentionMs()
      didClean
    }
  }

  /**
   * Iterate through the snapshots a test the given predicate to see if we should attempt to delete it. Since
   * we have some additional invariants regarding snapshots and log segments we cannot simply delete a snapshot in
   * all cases.
   *
   * For the given predicate, we are testing if the snapshot identified by the first argument should be deleted.
   * The predicate returns a Some with the reason if the snapshot should be deleted and a None if the snapshot
   * should not be deleted
   */
  private def cleanSnapshots(predicate: OffsetAndEpoch => Option[SnapshotDeletionReason]): Boolean = {
    if (snapshots.size < 2) {
      return false
    }

    var didClean = false
    snapshots.keys.toSeq.sliding(2).foreach {
      case Seq(snapshot: OffsetAndEpoch, nextSnapshot: OffsetAndEpoch) =>
        predicate(snapshot) match {
          case Some(reason) =>
            if (deleteBeforeSnapshot(nextSnapshot, reason)) {
              didClean = true
            } else {
              return didClean
            }

          case None =>
            return didClean

        }

      case _ => false // Shouldn't get here with the sliding window
    }

    didClean
  }

  private def cleanSnapshotsRetentionMs(): Boolean = {
    if (config.retentionMillis < 0)
      return false

    // Keep deleting snapshots as long as the
    def shouldClean(snapshotId: OffsetAndEpoch): Option[SnapshotDeletionReason] = {
      readSnapshotTimestamp(snapshotId).flatMap { timestamp =>
        val now = time.milliseconds()
        if (now - timestamp > config.retentionMillis) {
          Some(RetentionMsBreach(now, timestamp, config.retentionMillis))
        } else {
          None
        }
      }
    }

    cleanSnapshots(shouldClean)
  }

  private def cleanSnapshotsRetentionSize(): Boolean = {
    if (config.retentionMaxBytes < 0)
      return false

    val snapshotSizes = loadSnapshotSizes().toMap

    var snapshotTotalSize: Long = snapshotSizes.values.sum

    // Keep deleting snapshots and segments as long as we exceed the retention size
    def shouldClean(snapshotId: OffsetAndEpoch): Option[SnapshotDeletionReason] = {
      snapshotSizes.get(snapshotId).flatMap { snapshotSize =>
        if (log.size + snapshotTotalSize > config.retentionMaxBytes) {
          val oldSnapshotTotalSize = snapshotTotalSize
          snapshotTotalSize -= snapshotSize
          Some(RetentionSizeBreach(log.size, oldSnapshotTotalSize, config.retentionMaxBytes))
        } else {
          None
        }
      }
    }

    cleanSnapshots(shouldClean)
  }

  /**
   * Forget the snapshots earlier than a given snapshot id and return the associated
   * snapshot readers.
   *
   * This method assumes that the lock for `snapshots` is already held.
   */
  @nowarn("cat=deprecation") // Needed for TreeMap.until
  private def forgetSnapshotsBefore(
    logStartSnapshotId: OffsetAndEpoch
  ): mutable.TreeMap[OffsetAndEpoch, Option[FileRawSnapshotReader]] = {
    val expiredSnapshots = snapshots.until(logStartSnapshotId).clone()
    snapshots --= expiredSnapshots.keys

    expiredSnapshots
  }

  /**
   * Rename the given snapshots on the log directory. Asynchronously, close and delete the
   * given snapshots after some delay.
   */
  private def removeSnapshots(
    expiredSnapshots: mutable.TreeMap[OffsetAndEpoch, Option[FileRawSnapshotReader]],
    reason: SnapshotDeletionReason,
  ): Unit = {
    expiredSnapshots.foreach { case (snapshotId, _) =>
      info(reason.reason(snapshotId))
      Snapshots.markForDelete(log.dir.toPath, snapshotId)
    }

    if (expiredSnapshots.nonEmpty) {
      scheduler.scheduleOnce(
        "delete-snapshot-files",
        () => KafkaMetadataLog.deleteSnapshotFiles(log.dir.toPath, expiredSnapshots, this),
        config.fileDeleteDelayMs
      )
    }
  }

  override def close(): Unit = {
    log.close()
    snapshots synchronized {
      snapshots.values.flatten.foreach(_.close())
      snapshots.clear()
    }
  }

  private[raft] def snapshotCount(): Int = {
    snapshots synchronized {
      snapshots.size
    }
  }
}

object KafkaMetadataLog extends Logging {
  def apply(
    topicPartition: TopicPartition,
    topicId: Uuid,
    dataDir: File,
    time: Time,
    scheduler: Scheduler,
    config: MetadataLogConfig
  ): KafkaMetadataLog = {
    val props = new Properties()
    props.setProperty(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, config.maxBatchSizeInBytes.toString)
    props.setProperty(TopicConfig.SEGMENT_BYTES_CONFIG, config.logSegmentBytes.toString)
    props.setProperty(TopicConfig.SEGMENT_MS_CONFIG, config.logSegmentMillis.toString)
    props.setProperty(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT.toString)

    // Disable time and byte retention when deleting segments
    props.setProperty(TopicConfig.RETENTION_MS_CONFIG, "-1")
    props.setProperty(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    LogConfig.validate(props)
    val defaultLogConfig = new LogConfig(props)

    if (config.logSegmentBytes < config.logSegmentMinBytes) {
      throw new InvalidConfigurationException(
        s"Cannot set ${KRaftConfigs.METADATA_LOG_SEGMENT_BYTES_CONFIG} below ${config.logSegmentMinBytes}: ${config.logSegmentBytes}"
      )
    } else if (defaultLogConfig.retentionMs >= 0) {
      throw new InvalidConfigurationException(
        s"Cannot set ${TopicConfig.RETENTION_MS_CONFIG} above -1: ${defaultLogConfig.retentionMs}."
      )
    } else if (defaultLogConfig.retentionSize >= 0) {
      throw new InvalidConfigurationException(
        s"Cannot set ${TopicConfig.RETENTION_BYTES_CONFIG} above -1: ${defaultLogConfig.retentionSize}."
      )
    }

    val log = UnifiedLog(
      dir = dataDir,
      config = defaultLogConfig,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = scheduler,
      brokerTopicStats = new BrokerTopicStats,
      time = time,
      maxTransactionTimeoutMs = Int.MaxValue,
      producerStateManagerConfig = new ProducerStateManagerConfig(Int.MaxValue, false),
      producerIdExpirationCheckIntervalMs = Int.MaxValue,
      logDirFailureChannel = new LogDirFailureChannel(5),
      lastShutdownClean = false,
      topicId = Some(topicId),
      keepPartitionMetadataFile = true
    )

    val metadataLog = new KafkaMetadataLog(
      log,
      time,
      scheduler,
      recoverSnapshots(log),
      topicPartition,
      config
    )

    // Print a warning if users have overridden the internal config
    if (config.logSegmentMinBytes != KafkaRaftClient.MAX_BATCH_SIZE_BYTES) {
      metadataLog.error(s"Overriding ${KRaftConfigs.METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG} is only supported for testing. Setting " +
        s"this value too low may lead to an inability to write batches of metadata records.")
    }

    // When recovering, truncate fully if the latest snapshot is after the log end offset. This can happen to a follower
    // when the follower crashes after downloading a snapshot from the leader but before it could truncate the log fully.
    metadataLog.truncateToLatestSnapshot()

    metadataLog
  }

  private def recoverSnapshots(
    log: UnifiedLog
  ): mutable.TreeMap[OffsetAndEpoch, Option[FileRawSnapshotReader]] = {
    val snapshotsToRetain = mutable.TreeMap.empty[OffsetAndEpoch, Option[FileRawSnapshotReader]]
    val snapshotsToDelete = mutable.Buffer.empty[SnapshotPath]

    // Scan the log directory; deleting partial snapshots and older snapshot, only remembering immutable snapshots start
    // from logStartOffset
    val filesInDir = Files.newDirectoryStream(log.dir.toPath)

    try {
      filesInDir.forEach { path =>
        Snapshots.parse(path).ifPresent { snapshotPath =>
          // Collect partial snapshot, deleted snapshot and older snapshot for deletion
          if (snapshotPath.partial
            || snapshotPath.deleted
            || snapshotPath.snapshotId.offset < log.logStartOffset) {
            snapshotsToDelete.append(snapshotPath)
          } else {
            snapshotsToRetain.put(snapshotPath.snapshotId, None)
          }
        }
      }

      // Before deleting any snapshots, we should ensure that the retained snapshots are
      // consistent with the current state of the log. If the log start offset is not 0,
      // then we must have a snapshot which covers the initial state up to the current
      // log start offset.
      if (log.logStartOffset > 0) {
        val latestSnapshotId = snapshotsToRetain.lastOption.map(_._1)
        if (!latestSnapshotId.exists(snapshotId => snapshotId.offset >= log.logStartOffset)) {
          throw new IllegalStateException("Inconsistent snapshot state: there must be a snapshot " +
            s"at an offset larger then the current log start offset ${log.logStartOffset}, but the " +
            s"latest snapshot is $latestSnapshotId")
        }
      }

      snapshotsToDelete.foreach { snapshotPath =>
        Files.deleteIfExists(snapshotPath.path)
        info(s"Deleted unneeded snapshot file with path $snapshotPath")
      }
    } finally {
      filesInDir.close()
    }

    info(s"Initialized snapshots with IDs ${snapshotsToRetain.keys} from ${log.dir}")
    snapshotsToRetain
  }

  private def deleteSnapshotFiles(
    logDir: Path,
    expiredSnapshots: mutable.TreeMap[OffsetAndEpoch, Option[FileRawSnapshotReader]],
    logging: Logging
  ): Unit = {
    expiredSnapshots.foreach { case (snapshotId, snapshotReader) =>
      snapshotReader.foreach { reader =>
        Utils.closeQuietly(reader, "reader")
      }
      Snapshots.deleteIfExists(logDir, snapshotId)
    }
  }

  private sealed trait SnapshotDeletionReason {
    def reason(snapshotId: OffsetAndEpoch): String
  }

  private final case class RetentionMsBreach(now: Long, timestamp: Long, retentionMillis: Long) extends SnapshotDeletionReason {
    override def reason(snapshotId: OffsetAndEpoch): String = {
      s"""Marking snapshot $snapshotId for deletion because its timestamp ($timestamp) is now ($now) older than the
          |retention ($retentionMillis)""".stripMargin
    }
  }

  private final case class RetentionSizeBreach(
    logSize: Long,
    snapshotsSize: Long,
    retentionMaxBytes: Long
  ) extends SnapshotDeletionReason {
    override def reason(snapshotId: OffsetAndEpoch): String = {
      s"""Marking snapshot $snapshotId for deletion because the log size ($logSize) and snapshots size ($snapshotsSize)
          |is greater than $retentionMaxBytes""".stripMargin
    }
  }

  private final object FullTruncation extends SnapshotDeletionReason {
    override def reason(snapshotId: OffsetAndEpoch): String = {
      s"Marking snapshot $snapshotId for deletion because the partition was fully truncated"
    }
  }

  private final object UnknownReason extends SnapshotDeletionReason {
    override def reason(snapshotId: OffsetAndEpoch): String = {
      s"Marking snapshot $snapshotId for deletion for unknown reason"
    }
  }
}
