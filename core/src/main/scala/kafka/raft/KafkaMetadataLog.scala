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

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.util.NoSuchElementException
import java.util.Optional
import java.util.concurrent.ConcurrentSkipListSet

import kafka.log.{AppendOrigin, Log, SnapshotGenerated, LogOffsetSnapshot}
import kafka.server.{FetchHighWatermark, FetchLogEnd}
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.raft.Isolation
import org.apache.kafka.raft.LogAppendInfo
import org.apache.kafka.raft.LogFetchInfo
import org.apache.kafka.raft.LogOffsetMetadata
import org.apache.kafka.raft.OffsetAndEpoch
import org.apache.kafka.raft.OffsetMetadata
import org.apache.kafka.raft.ReplicatedLog
import org.apache.kafka.snapshot.FileRawSnapshotReader
import org.apache.kafka.snapshot.FileRawSnapshotWriter
import org.apache.kafka.snapshot.RawSnapshotReader
import org.apache.kafka.snapshot.RawSnapshotWriter
import org.apache.kafka.snapshot.SnapshotPath
import org.apache.kafka.snapshot.Snapshots

import scala.compat.java8.OptionConverters._

final class KafkaMetadataLog private (
  log: Log,
  // This object needs to be thread-safe because it is used by the snapshotting thread to notify the
  // polling thread when snapshots are created.
  snapshotIds: ConcurrentSkipListSet[OffsetAndEpoch],
  topicPartition: TopicPartition,
  maxFetchSizeInBytes: Int
) extends ReplicatedLog {

  /* The oldest snapshot id is the snapshot at the log start offset. Since the KafkaMetadataLog doesn't
   * currently delete snapshots, it is possible for the oldest snapshot id to not be the smallest
   * snapshot id in the snapshotIds set.
   */
  private[this] var oldestSnapshotId = snapshotIds
    .stream()
    .filter(_.offset == startOffset)
    .findAny()

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
        origin = AppendOrigin.Coordinator
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
      latestSnapshotId.map[Int] { snapshotId =>
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
    (log.endOffsetForEpoch(epoch), oldestSnapshotId.asScala) match {
      case (Some(offsetAndEpoch), Some(snapshotId)) if (
        offsetAndEpoch.offset == snapshotId.offset &&
        offsetAndEpoch.leaderEpoch == epoch) =>

        // The epoch is smaller thant the smallest epoch on the log. Overide the diverging
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
    log.truncateTo(offset)
  }

  override def truncateToLatestSnapshot(): Boolean = {
    val latestEpoch = log.latestEpoch.getOrElse(0)
    latestSnapshotId.asScala match {
      case Some(snapshotId) if (snapshotId.epoch > latestEpoch ||
        (snapshotId.epoch == latestEpoch && snapshotId.offset > endOffset().offset)) =>
        // Truncate the log fully if the latest snapshot is greater than the log end offset

        log.truncateFullyAndStartAt(snapshotId.offset)
        oldestSnapshotId = latestSnapshotId

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

  override def createSnapshot(snapshotId: OffsetAndEpoch): RawSnapshotWriter = {
    // Do not let the state machine create snapshots older than the latest snapshot
    latestSnapshotId().ifPresent { latest =>
      if (latest.epoch > snapshotId.epoch || latest.offset > snapshotId.offset) {
        // Since snapshots are less than the high-watermark absolute offset comparison is okay.
        throw new IllegalArgumentException(
          s"Attemting to create a snapshot ($snapshotId) that is not greater than the latest snapshot ($latest)"
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
    try {
      Optional.of(snapshotIds.last)
    } catch {
      case _: NoSuchElementException =>
        Optional.empty()
    }
  }

  override def oldestSnapshotId(): Optional[OffsetAndEpoch] = {
    oldestSnapshotId
  }

  override def onSnapshotFrozen(snapshotId: OffsetAndEpoch): Unit = {
    snapshotIds.add(snapshotId)
  }

  override def deleteBeforeSnapshot(logStartSnapshotId: OffsetAndEpoch): Boolean = {
    latestSnapshotId.asScala match {
      case Some(snapshotId) if (snapshotIds.contains(logStartSnapshotId) &&
        startOffset < logStartSnapshotId.offset &&
        logStartSnapshotId.offset <= snapshotId.offset &&
        log.maybeIncrementLogStartOffset(logStartSnapshotId.offset, SnapshotGenerated)) =>

        log.deleteOldSegments()
        oldestSnapshotId = Optional.of(logStartSnapshotId)

        true

      case _ => false
    }
  }

  override def close(): Unit = {
    log.close()
  }
}

object KafkaMetadataLog {
  def apply(
    log: Log,
    topicPartition: TopicPartition,
    maxFetchSizeInBytes: Int = 1024 * 1024
  ): KafkaMetadataLog = {
    val snapshotIds = new ConcurrentSkipListSet[OffsetAndEpoch]()
    // Scan the log directory; deleting partial snapshots and remembering immutable snapshots
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
          if (snapshotPath.partial) {
            Files.deleteIfExists(snapshotPath.path)
          } else {
            snapshotIds.add(snapshotPath.snapshotId)
          }
        }
      }

    val replicatedLog = new KafkaMetadataLog(log, snapshotIds, topicPartition, maxFetchSizeInBytes)
    // When recovering, truncate fully if the latest snapshot is after the log end offset. This can happen to a follower
    // when the follower crashes after downloading a snapshot from the leader but before it could truncate the log fully.
    replicatedLog.truncateToLatestSnapshot()

    replicatedLog
  }
}
