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

import java.util.{Optional, OptionalLong}

import kafka.log.{AppendOrigin, Log}
import kafka.server.{FetchHighWatermark, FetchLogEnd}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.raft
import org.apache.kafka.raft.{LogAppendInfo, LogFetchInfo, LogOffsetMetadata, ReplicatedLog}

import scala.compat.java8.OptionConverters._

class KafkaMetadataLog(log: Log,
                       topicPartition: TopicPartition,
                       maxFetchSizeInBytes: Int = 1024 * 1024) extends ReplicatedLog {

  override def read(startOffset: Long, endOffsetExclusive: OptionalLong): LogFetchInfo = {
    val isolation = if (endOffsetExclusive.isPresent)
      FetchHighWatermark
    else
      FetchLogEnd

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
    // TODO: Does this handle empty log case (when epoch is None) as we expect?
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

  override def assignEpochStartOffset(epoch: Int, startOffset: Long): Unit = {
    log.maybeAssignEpochStartOffset(epoch, startOffset)
  }

  override def updateHighWatermark(offsetMetadata: LogOffsetMetadata): Unit = {
    offsetMetadata.metadata.asScala match {
      case Some(segmentPosition: SegmentPosition) => log.updateHighWatermarkOffsetMetadata(
        new kafka.server.LogOffsetMetadata(
          offsetMetadata.offset,
          segmentPosition.baseOffset,
          segmentPosition.relativePosition)
      )
      case _ => log.updateHighWatermark(offsetMetadata.offset)
    }
  }

  /**
   * Return the topic partition associated with the log.
   */
  override def topicPartition(): TopicPartition = {
    topicPartition
  }

  override def close(): Unit = {
    log.close()
  }
}
