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

import kafka.log.{AppendOrigin, Log, SnapshotGenerated}
import kafka.server.{FetchHighWatermark, FetchLogEnd}
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.raft
import org.apache.kafka.raft.{LogAppendInfo, LogFetchInfo, LogOffsetMetadata, Isolation, ReplicatedLog}
import org.apache.kafka.snapshot.FileRawSnapshotReader
import org.apache.kafka.snapshot.FileRawSnapshotWriter
import org.apache.kafka.snapshot.RawSnapshotReader
import org.apache.kafka.snapshot.RawSnapshotWriter
import org.apache.kafka.snapshot.Snapshots

import scala.compat.java8.OptionConverters._

final class KafkaMetadataLog private (
  log: Log,
  snapshotIds: ConcurrentSkipListSet[raft.OffsetAndEpoch],
  topicPartition: TopicPartition,
  maxFetchSizeInBytes: Int
) extends ReplicatedLog {

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

    val appendInfo = log.appendAsLeader(records.asInstanceOf[MemoryRecords],
      leaderEpoch = epoch,
      origin = AppendOrigin.Coordinator)

    if (appendInfo.rolled) {
      log.deleteOldSegments()
    }

    new LogAppendInfo(appendInfo.firstOffset.getOrElse {
      throw new KafkaException("Append failed unexpectedly")
    }, appendInfo.lastOffset)
  }

  override def appendAsFollower(records: Records): LogAppendInfo = {
    if (records.sizeInBytes == 0)
      throw new IllegalArgumentException("Attempt to append an empty record set")

    val appendInfo = log.appendAsFollower(records.asInstanceOf[MemoryRecords])

    if (appendInfo.rolled) {
      log.deleteOldSegments()
    }

    new LogAppendInfo(appendInfo.firstOffset.getOrElse {
      throw new KafkaException("Append failed unexpectedly")
    }, appendInfo.lastOffset)
  }

  override def lastFetchedEpoch: Int = {
    log.latestEpoch.getOrElse {
      latestSnapshotId.map { snapshotId =>
        val logEndOffset = endOffset().offset
        if (snapshotId.offset == logEndOffset) {
          snapshotId.epoch
        } else {
          throw new KafkaException(
            s"Log doesn't have a last fetch epoch and there is a snapshot ($snapshotId). " +
            s"Expected the snapshot's end offset to match the logs end offset ($logEndOffset)"
          )
        }
      } orElse(0)
    }
  }

  override def endOffsetForEpoch(leaderEpoch: Int): Optional[raft.OffsetAndEpoch] = {
    val endOffsetOpt = log.endOffsetForEpoch(leaderEpoch).map { offsetAndEpoch =>
      new raft.OffsetAndEpoch(offsetAndEpoch.offset, offsetAndEpoch.leaderEpoch)
    }
    endOffsetOpt.asJava
  }

  override def endOffset: LogOffsetMetadata = {
    val endOffsetMetadata = log.logEndOffsetMetadata
    new LogOffsetMetadata(
      endOffsetMetadata.messageOffset,
      Optional.of(SegmentPosition(
        endOffsetMetadata.segmentBaseOffset,
        endOffsetMetadata.relativePositionInSegment))
      )
  }

  override def startOffset: Long = {
    log.logStartOffset
  }

  override def truncateTo(offset: Long): Unit = {
    log.truncateTo(offset)
  }

  override def truncateFullyToLatestSnapshot(): Boolean = {
    // Truncate the log fully if the latest snapshot is greater than the log end offset
    var truncated = false
    latestSnapshotId.ifPresent { snapshotId =>
      if (snapshotId.epoch > log.latestEpoch.getOrElse(0) ||
        (snapshotId.epoch == log.latestEpoch.getOrElse(0) && snapshotId.offset > endOffset().offset)) {
        log.truncateFullyAndStartAt(snapshotId.offset)
        truncated = true
      }
    }

    truncated
  }

  override def initializeLeaderEpoch(epoch: Int): Unit = {
    log.maybeAssignEpochStartOffset(epoch, log.logEndOffset)
  }

  override def updateHighWatermark(offsetMetadata: LogOffsetMetadata): Unit = {
    offsetMetadata.metadata.asScala match {
      case Some(segmentPosition: SegmentPosition) => log.updateHighWatermarkOffsetMetadata(
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

  override def createSnapshot(snapshotId: raft.OffsetAndEpoch): RawSnapshotWriter = {
    // TODO: Talk to Jason about truncation past the high-watermark since it can lead to truncation past snapshots.
    // This can result in the leader having a snapshot that is less that the follower's snapshot. I think that the Raft Client
    // checks against this and aborts. If so, then this check and exception is okay.

    // Do let the state machine create snapshots older than the latest snapshot
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

  override def readSnapshot(snapshotId: raft.OffsetAndEpoch): Optional[RawSnapshotReader] = {
    try {
      if (snapshotIds.contains(snapshotId)) {
        Optional.of(FileRawSnapshotReader.open(log.dir.toPath, snapshotId))
      } else {
        Optional.empty()
      }
    } catch {
      case e: NoSuchFileException => Optional.empty()
    }
  }

  override def latestSnapshotId(): Optional[raft.OffsetAndEpoch] = {
    try {
      Optional.of(snapshotIds.last)
    } catch {
      case _: NoSuchElementException =>
        Optional.empty()
    }
  }

  override def snapshotFrozen(snapshotId: raft.OffsetAndEpoch): Unit = {
    snapshotIds.add(snapshotId)
  }

  override def maybeUpdateLogStartOffset(): Boolean = {
    var updated = false
    latestSnapshotId.ifPresent { snapshotId =>
      if (startOffset < snapshotId.offset &&
          log.maybeIncrementLogStartOffset(snapshotId.offset, SnapshotGenerated)) {
        log.deleteOldSegments()
        updated = true
      } else if (startOffset > snapshotId.offset) {
        throw new KafkaException(s"Log start offset ($startOffset) is greater than the latest snapshot ($snapshotId)")
      }
    }

    updated
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
    val snapshotIds = new ConcurrentSkipListSet[raft.OffsetAndEpoch]()
    // Scan the log directory; deleting partial snapshots and remembering immutable snapshots
    Files
      .walk(log.dir.toPath, 1)
      .map { path =>
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
    replicatedLog.truncateFullyToLatestSnapshot()

    replicatedLog
  }
}
