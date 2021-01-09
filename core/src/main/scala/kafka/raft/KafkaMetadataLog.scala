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

import java.nio.file.NoSuchFileException
import java.util.Optional

import kafka.log.{AppendOrigin, Log}
import kafka.server.{FetchHighWatermark, FetchLogEnd}
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.raft
import org.apache.kafka.raft.{LogAppendInfo, LogFetchInfo, LogOffsetMetadata, Isolation, ReplicatedLog}
import org.apache.kafka.snapshot.FileRawSnapshotReader
import org.apache.kafka.snapshot.FileRawSnapshotWriter
import org.apache.kafka.snapshot.RawSnapshotReader
import org.apache.kafka.snapshot.RawSnapshotWriter

import scala.compat.java8.OptionConverters._

class KafkaMetadataLog(
  log: Log,
  topicPartition: TopicPartition,
  maxFetchSizeInBytes: Int = 1024 * 1024
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
    new LogAppendInfo(appendInfo.firstOffset.getOrElse {
      throw new KafkaException("Append failed unexpectedly")
    }, appendInfo.lastOffset)
  }

  override def appendAsFollower(records: Records): LogAppendInfo = {
    if (records.sizeInBytes == 0)
      throw new IllegalArgumentException("Attempt to append an empty record set")

    val appendInfo = log.appendAsFollower(records.asInstanceOf[MemoryRecords])
    new LogAppendInfo(appendInfo.firstOffset.getOrElse {
      throw new KafkaException("Append failed unexpectedly")
    }, appendInfo.lastOffset)
  }

  override def lastFetchedEpoch: Int = {
    log.latestEpoch.getOrElse(0)
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
    FileRawSnapshotWriter.create(log.dir.toPath, snapshotId)
  }

  override def readSnapshot(snapshotId: raft.OffsetAndEpoch): Optional[RawSnapshotReader] = {
    try {
      Optional.of(FileRawSnapshotReader.open(log.dir.toPath, snapshotId))
    } catch {
      case e: NoSuchFileException => Optional.empty()
    }
  }

  override def close(): Unit = {
    log.close()
  }
}
