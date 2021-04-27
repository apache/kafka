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

import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException}
import java.util.concurrent.ConcurrentSkipListSet
import java.util.{Optional, Properties}

import kafka.api.ApiVersion
import kafka.log.{AppendOrigin, Log, LogConfig, LogOffsetSnapshot, SnapshotGenerated}
import kafka.server.{BrokerTopicStats, FetchHighWatermark, FetchLogEnd, LogDirFailureChannel}
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
import org.apache.kafka.raft.{Isolation, LogAppendInfo, LogFetchInfo, LogOffsetMetadata, OffsetAndEpoch, OffsetMetadata, ReplicatedLog}
import org.apache.kafka.snapshot.{FileRawSnapshotReader, FileRawSnapshotWriter, RawSnapshotReader, RawSnapshotWriter, SnapshotPath, Snapshots}

import scala.compat.java8.OptionConverters._

final class KafkaMetadataLog private (
  log: Log,
  scheduler: Scheduler,
  // This object needs to be thread-safe because it is used by the snapshotting thread to notify the
  // polling thread when snapshots are created.
  snapshotIds: ConcurrentSkipListSet[OffsetAndEpoch],
  topicPartition: TopicPartition,
  maxFetchSizeInBytes: Int,
  val fileDeleteDelayMs: Long // Visible for testing,
) extends ReplicatedLog with Logging {

  override def read(startOffset: Long, readIsolation: Isolation): LogFetchInfo = {
    val isolation = readIsolation match {
      case Isolation.COMMITTED => FetchHighWatermark
      case Isolation.UNCOMMITTED => FetchLogEnd
      case _ => throw new IllegalArgumentException(s"Unhandled read isolation $readIsolation")
    }

    val fetchInfo = log.read(startOffset,
      maxLength = maxFetchSizeInBytes,
      isolation = isolation,
      minOneMessage = true)

    new LogFetchInfo(
      fetchInfo.records,

      new LogOffsetMetadata(
        fetchInfo.fetchOffsetMetadata.messageOffset,
        Optional.of(SegmentPosition(
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
        origin = AppendOrigin.RaftLeader
      )
    )
  }

  override def appendAsFollower(records: Records): LogAppendInfo = {
    if (records.sizeInBytes == 0)
      throw new IllegalArgumentException("Attempt to append an empty record set")

    handleAndConvertLogAppendInfo(log.appendAsFollower(records.asInstanceOf[MemoryRecords]))
  }

  private def handleAndConvertLogAppendInfo(appendInfo: kafka.log.LogAppendInfo): LogAppendInfo = {
    appendInfo.firstOffset match {
      case Some(firstOffset) =>
        if (firstOffset.relativePositionInSegment == 0) {
          // Assume that a new segment was created if the relative position is 0
          log.deleteOldSegments()
        }
        new LogAppendInfo(firstOffset.messageOffset, appendInfo.lastOffset)
      case None =>
        throw new KafkaException(s"Append failed unexpectedly: ${appendInfo.errorMessage}")
    }
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
    (log.endOffsetForEpoch(epoch), earliestSnapshotId().asScala) match {
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
      Optional.of(SegmentPosition(
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
    latestSnapshotId().asScala match {
      case Some(snapshotId) if (snapshotId.epoch > latestEpoch ||
        (snapshotId.epoch == latestEpoch && snapshotId.offset > endOffset().offset)) =>
        // Truncate the log fully if the latest snapshot is greater than the log end offset

        log.truncateFullyAndStartAt(snapshotId.offset)
        // Delete snapshot after truncating
        removeSnapshotFilesBefore(snapshotId)

        true

      case _ => false
    }
  }

  override def initializeLeaderEpoch(epoch: Int): Unit = {
    log.maybeAssignEpochStartOffset(epoch, log.logEndOffset)
  }

  override def updateHighWatermark(offsetMetadata: LogOffsetMetadata): Unit = {
    offsetMetadata.metadata.asScala match {
      case Some(segmentPosition: SegmentPosition) => log.updateHighWatermark(
        new kafka.server.LogOffsetMetadata(
          offsetMetadata.offset,
          segmentPosition.baseOffset,
          segmentPosition.relativePosition)
      )
      case _ =>
        // FIXME: This API returns the new high watermark, which may be different from the passed offset
        log.updateHighWatermark(offsetMetadata.offset)
    }
  }

  override def highWatermark: LogOffsetMetadata = {
    val LogOffsetSnapshot(_, _, hwm, _) = log.fetchOffsetSnapshot
    val segmentPosition: Optional[OffsetMetadata] = if (hwm.messageOffsetOnly) {
      Optional.of(SegmentPosition(hwm.segmentBaseOffset, hwm.relativePositionInSegment))
    } else {
      Optional.empty()
    }

    new LogOffsetMetadata(hwm.messageOffset, segmentPosition)
  }

  override def flush(): Unit = {
    log.flush()
  }

  override def lastFlushedOffset(): Long = {
    log.recoveryPoint
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

  override def createSnapshot(snapshotId: OffsetAndEpoch): RawSnapshotWriter = {
    // Do not let the state machine create snapshots older than the latest snapshot
    latestSnapshotId().ifPresent { latest =>
      if (latest.epoch > snapshotId.epoch || latest.offset > snapshotId.offset) {
        // Since snapshots are less than the high-watermark absolute offset comparison is okay.
        throw new IllegalArgumentException(
          s"Attempting to create a snapshot ($snapshotId) that is not greater than the latest snapshot ($latest)"
        )
      }
    }

    FileRawSnapshotWriter.create(log.dir.toPath, snapshotId, Optional.of(this))
  }

  override def readSnapshot(snapshotId: OffsetAndEpoch): Optional[RawSnapshotReader] = {
    try {
      if (snapshotIds.contains(snapshotId)) {
        Optional.of(FileRawSnapshotReader.open(log.dir.toPath, snapshotId))
      } else {
        Optional.empty()
      }
    } catch {
      case _: NoSuchFileException =>
        Optional.empty()
    }
  }

  override def latestSnapshotId(): Optional[OffsetAndEpoch] = {
    val descending = snapshotIds.descendingIterator
    if (descending.hasNext) {
      Optional.of(descending.next)
    } else {
      Optional.empty()
    }
  }

  override def earliestSnapshotId(): Optional[OffsetAndEpoch] = {
    val ascendingIterator = snapshotIds.iterator
    if (ascendingIterator.hasNext) {
      Optional.of(ascendingIterator.next)
    } else {
      Optional.empty()
    }
  }

  override def onSnapshotFrozen(snapshotId: OffsetAndEpoch): Unit = {
    snapshotIds.add(snapshotId)
  }

  override def deleteBeforeSnapshot(logStartSnapshotId: OffsetAndEpoch): Boolean = {
    latestSnapshotId().asScala match {
      case Some(snapshotId) if (snapshotIds.contains(logStartSnapshotId) &&
        startOffset < logStartSnapshotId.offset &&
        logStartSnapshotId.offset <= snapshotId.offset &&
        log.maybeIncrementLogStartOffset(logStartSnapshotId.offset, SnapshotGenerated)) =>
        log.deleteOldSegments()

        // Delete snapshot after increasing LogStartOffset
        removeSnapshotFilesBefore(logStartSnapshotId)

        true

      case _ => false
    }
  }

  /**
   * Removes all snapshots on the log directory whose epoch and end offset is less than the giving epoch and end offset.
   */
  private def removeSnapshotFilesBefore(logStartSnapshotId: OffsetAndEpoch): Unit = {
    val expiredSnapshotIdsIter = snapshotIds.headSet(logStartSnapshotId, false).iterator
    while (expiredSnapshotIdsIter.hasNext) {
      val snapshotId = expiredSnapshotIdsIter.next()
      // If snapshotIds contains a snapshot id, the KafkaRaftClient and Listener can expect that the snapshot exists
      // on the file system, so we should first remove snapshotId and then delete snapshot file.
      expiredSnapshotIdsIter.remove()

      val path = Snapshots.snapshotPath(log.dir.toPath, snapshotId)
      val destination = Snapshots.deleteRename(path, snapshotId)
      try {
        Utils.atomicMoveWithFallback(path, destination, false)
      } catch {
        case e: IOException =>
          error(s"Error renaming snapshot file: $path to $destination", e)
      }
      scheduler.schedule(
        "delete-snapshot-file",
        () => Snapshots.deleteSnapshotIfExists(log.dir.toPath, snapshotId),
        fileDeleteDelayMs)
    }
  }

  override def close(): Unit = {
    log.close()
  }
}

object KafkaMetadataLog {

  def apply(
    topicPartition: TopicPartition,
    topicId: Uuid,
    dataDir: File,
    time: Time,
    scheduler: Scheduler,
    maxBatchSizeInBytes: Int,
    maxFetchSizeInBytes: Int
  ): KafkaMetadataLog = {
    val props = new Properties()
    props.put(LogConfig.MaxMessageBytesProp, maxBatchSizeInBytes.toString)
    props.put(LogConfig.MessageFormatVersionProp, ApiVersion.latestVersion.toString)

    LogConfig.validateValues(props)
    val defaultLogConfig = LogConfig(props)

    val log = Log(
      dir = dataDir,
      config = defaultLogConfig,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = scheduler,
      brokerTopicStats = new BrokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = Int.MaxValue,
      producerIdExpirationCheckIntervalMs = Int.MaxValue,
      logDirFailureChannel = new LogDirFailureChannel(5),
      lastShutdownClean = false,
      topicId = Some(topicId),
      keepPartitionMetadataFile = false
    )

    val metadataLog = new KafkaMetadataLog(
      log,
      scheduler,
      recoverSnapshots(log),
      topicPartition,
      maxFetchSizeInBytes,
      defaultLogConfig.fileDeleteDelayMs
    )

    // When recovering, truncate fully if the latest snapshot is after the log end offset. This can happen to a follower
    // when the follower crashes after downloading a snapshot from the leader but before it could truncate the log fully.
    metadataLog.truncateToLatestSnapshot()

    metadataLog
  }

  private def recoverSnapshots(
    log: Log
  ): ConcurrentSkipListSet[OffsetAndEpoch] = {
    val snapshotIds = new ConcurrentSkipListSet[OffsetAndEpoch]()
    // Scan the log directory; deleting partial snapshots and older snapshot, only remembering immutable snapshots start
    // from logStartOffset
    Files
      .walk(log.dir.toPath, 1)
      .map[Optional[SnapshotPath]] { path =>
        if (path != log.dir.toPath) {
          Snapshots.parse(path)
        } else {
          Optional.empty()
        }
      }
      .forEach { path =>
        path.ifPresent { snapshotPath =>
          if (snapshotPath.partial ||
            snapshotPath.deleted ||
            snapshotPath.snapshotId.offset < log.logStartOffset) {
            // Delete partial snapshot, deleted snapshot and older snapshot
            Files.deleteIfExists(snapshotPath.path)
          } else {
            snapshotIds.add(snapshotPath.snapshotId)
          }
        }
      }
    snapshotIds
  }

}
