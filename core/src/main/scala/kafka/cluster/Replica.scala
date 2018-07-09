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

package kafka.cluster

import kafka.log.Log
import kafka.utils.Logging
import kafka.server.{LogOffsetMetadata, LogReadResult}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.utils.Time

class Replica(val brokerId: Int,
              val topicPartition: TopicPartition,
              time: Time = Time.SYSTEM,
              initialHighWatermarkValue: Long = 0L,
              @volatile var log: Option[Log] = None) extends Logging {
  // the high watermark offset value, in non-leader replicas only its message offsets are kept
  @volatile private[this] var highWatermarkMetadata = new LogOffsetMetadata(initialHighWatermarkValue)
  // the log end offset value, kept in all replicas;
  // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch
  @volatile private[this] var logEndOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata
  // the log start offset value, kept in all replicas;
  // for local replica it is the log's start offset, for remote replicas its value is only updated by follower fetch
  @volatile private[this] var _logStartOffset = Log.UnknownLogStartOffset

  // The log end offset value at the time the leader received the last FetchRequest from this follower
  // This is used to determine the lastCaughtUpTimeMs of the follower
  @volatile private[this] var lastFetchLeaderLogEndOffset = 0L

  // The time when the leader received the last FetchRequest from this follower
  // This is used to determine the lastCaughtUpTimeMs of the follower
  @volatile private[this] var lastFetchTimeMs = 0L

  // lastCaughtUpTimeMs is the largest time t such that the offset of most recent FetchRequest from this follower >=
  // the LEO of leader at time t. This is used to determine the lag of this follower and ISR of this partition.
  @volatile private[this] var _lastCaughtUpTimeMs = 0L

  def isLocal: Boolean = log.isDefined

  def lastCaughtUpTimeMs = _lastCaughtUpTimeMs

  val epochs = log.map(_.leaderEpochCache)

  info(s"Replica loaded for partition $topicPartition with initial high watermark $initialHighWatermarkValue")
  log.foreach(_.onHighWatermarkIncremented(initialHighWatermarkValue))

  /*
   * If the FetchRequest reads up to the log end offset of the leader when the current fetch request is received,
   * set `lastCaughtUpTimeMs` to the time when the current fetch request was received.
   *
   * Else if the FetchRequest reads up to the log end offset of the leader when the previous fetch request was received,
   * set `lastCaughtUpTimeMs` to the time when the previous fetch request was received.
   *
   * This is needed to enforce the semantics of ISR, i.e. a replica is in ISR if and only if it lags behind leader's LEO
   * by at most `replicaLagTimeMaxMs`. These semantics allow a follower to be added to the ISR even if the offset of its
   * fetch request is always smaller than the leader's LEO, which can happen if small produce requests are received at
   * high frequency.
   */
  def updateLogReadResult(logReadResult: LogReadResult) {
    if (logReadResult.info.fetchOffsetMetadata.messageOffset >= logReadResult.leaderLogEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, logReadResult.fetchTimeMs)
    else if (logReadResult.info.fetchOffsetMetadata.messageOffset >= lastFetchLeaderLogEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, lastFetchTimeMs)

    logStartOffset = logReadResult.followerLogStartOffset
    logEndOffset = logReadResult.info.fetchOffsetMetadata
    lastFetchLeaderLogEndOffset = logReadResult.leaderLogEndOffset
    lastFetchTimeMs = logReadResult.fetchTimeMs
  }

  def resetLastCaughtUpTime(curLeaderLogEndOffset: Long, curTimeMs: Long, lastCaughtUpTimeMs: Long) {
    lastFetchLeaderLogEndOffset = curLeaderLogEndOffset
    lastFetchTimeMs = curTimeMs
    _lastCaughtUpTimeMs = lastCaughtUpTimeMs
  }

  private def logEndOffset_=(newLogEndOffset: LogOffsetMetadata) {
    if (isLocal) {
      throw new KafkaException(s"Should not set log end offset on partition $topicPartition's local replica $brokerId")
    } else {
      logEndOffsetMetadata = newLogEndOffset
      trace(s"Setting log end offset for replica $brokerId for partition $topicPartition to [$logEndOffsetMetadata]")
    }
  }

  def logEndOffset: LogOffsetMetadata =
    if (isLocal)
      log.get.logEndOffsetMetadata
    else
      logEndOffsetMetadata

  /**
   * Increment the log start offset if the new offset is greater than the previous log start offset. The replica
   * must be local and the new log start offset must be lower than the current high watermark.
   */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long) {
    if (isLocal) {
      if (newLogStartOffset > highWatermark.messageOffset)
        throw new OffsetOutOfRangeException(s"Cannot increment the log start offset to $newLogStartOffset of partition $topicPartition " +
          s"since it is larger than the high watermark ${highWatermark.messageOffset}")
      log.get.maybeIncrementLogStartOffset(newLogStartOffset)
    } else {
      throw new KafkaException(s"Should not try to delete records on partition $topicPartition's non-local replica $brokerId")
    }
  }

  private def logStartOffset_=(newLogStartOffset: Long) {
    if (isLocal) {
      throw new KafkaException(s"Should not set log start offset on partition $topicPartition's local replica $brokerId " +
                               s"without attempting to delete records of the log")
    } else {
      _logStartOffset = newLogStartOffset
      trace(s"Setting log start offset for remote replica $brokerId for partition $topicPartition to [$newLogStartOffset]")
    }
  }

  def logStartOffset: Long =
    if (isLocal)
      log.get.logStartOffset
    else
      _logStartOffset

  def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
    if (isLocal) {
      if (newHighWatermark.messageOffset < 0)
        throw new IllegalArgumentException("High watermark offset should be non-negative")

      highWatermarkMetadata = newHighWatermark
      log.foreach(_.onHighWatermarkIncremented(newHighWatermark.messageOffset))
      trace(s"Setting high watermark for replica $brokerId partition $topicPartition to [$newHighWatermark]")
    } else {
      throw new KafkaException(s"Should not set high watermark on partition $topicPartition's non-local replica $brokerId")
    }
  }

  def highWatermark: LogOffsetMetadata = highWatermarkMetadata

  /**
   * The last stable offset (LSO) is defined as the first offset such that all lower offsets have been "decided."
   * Non-transactional messages are considered decided immediately, but transactional messages are only decided when
   * the corresponding COMMIT or ABORT marker is written. This implies that the last stable offset will be equal
   * to the high watermark if there are no transactional messages in the log. Note also that the LSO cannot advance
   * beyond the high watermark.
   */
  def lastStableOffset: LogOffsetMetadata = {
    log.map { log =>
      log.firstUnstableOffset match {
        case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermark.messageOffset => offsetMetadata
        case _ => highWatermark
      }
    }.getOrElse(throw new KafkaException(s"Cannot fetch last stable offset on partition $topicPartition's " +
      s"non-local replica $brokerId"))
  }

  /*
   * Convert hw to local offset metadata by reading the log at the hw offset.
   * If the hw offset is out of range, return the first offset of the first log segment as the offset metadata.
   */
  def convertHWToLocalOffsetMetadata() {
    if (isLocal) {
      highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset).getOrElse {
        log.get.convertToOffsetMetadata(logStartOffset).getOrElse {
          val firstSegmentOffset = log.get.logSegments.head.baseOffset
          new LogOffsetMetadata(firstSegmentOffset, firstSegmentOffset, 0)
        }
      }
    } else {
      throw new KafkaException(s"Should not construct complete high watermark on partition $topicPartition's non-local replica $brokerId")
    }
  }

  override def equals(that: Any): Boolean = that match {
    case other: Replica => brokerId == other.brokerId && topicPartition == other.topicPartition
    case _ => false
  }

  override def hashCode: Int = 31 + topicPartition.hashCode + 17 * brokerId

  override def toString: String = {
    val replicaString = new StringBuilder
    replicaString.append("ReplicaId: " + brokerId)
    replicaString.append("; Topic: " + topicPartition.topic)
    replicaString.append("; Partition: " + topicPartition.partition)
    replicaString.append("; isLocal: " + isLocal)
    replicaString.append("; lastCaughtUpTimeMs: " + lastCaughtUpTimeMs)
    if (isLocal) {
      replicaString.append("; Highwatermark: " + highWatermark)
      replicaString.append("; LastStableOffset: " + lastStableOffset)
    }
    replicaString.toString
  }
}
