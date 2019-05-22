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

import kafka.common.KafkaException
import kafka.log.{Log, LogOffsetSnapshot}
import kafka.server.{LogOffsetMetadata, OffsetAndEpoch}
import kafka.utils.Logging
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.TopicPartition

/**
 * Class to hold information about local partition log. A Partition
 * can have at most two logs:
 * <ol>
 * <li> Log where all current active write are going (called "local replica").
 * <li> If an "AlterLogDirs" command is issued to move log directory,
 *      then the Partition gets another log (called "future replica").
 * </ol>
 *
 * Having two logs at same time is a temporary state and as soon as log directory
 * is successfully moved a Partition will again have only one Log.
 */
class LogInfo private(val brokerId: Int,
                      val topicPartition: TopicPartition,
                      initialHighWatermarkValue: Long = 0L,
                      @volatile var log: Log) extends Logging {

  // the high watermark offset value, in non-leader logs only its message offsets are kept
  @volatile private[this] var highWatermarkMetadata = new LogOffsetMetadata(initialHighWatermarkValue)

  info(s"Log loaded for partition $topicPartition with initial high watermark $initialHighWatermarkValue")
  log.onHighWatermarkIncremented(initialHighWatermarkValue)

  def latestEpoch: Option[Int] = {
    log.latestEpoch
  }

  def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = {
    log.endOffsetForEpoch(leaderEpoch)
  }

  def logStartOffset: Long =
    log.logStartOffset

  def logEndOffsetMetadata: LogOffsetMetadata =
    log.logEndOffsetMetadata

  def logEndOffset: Long = logEndOffsetMetadata.messageOffset

  def offsetSnapshot: LogOffsetSnapshot = {
    LogOffsetSnapshot(
      logStartOffset = logStartOffset,
      logEndOffset = logEndOffsetMetadata,
      highWatermark =  highWatermark,
      lastStableOffset = lastStableOffset)
  }

  /**
   * Increment the log start offset if the new offset is greater than the previous log start offset. The replica
   * must be local and the new log start offset must be lower than the current high watermark.
   */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long) {
    if (newLogStartOffset > highWatermark.messageOffset)
      throw new OffsetOutOfRangeException(s"Cannot increment the log start offset to $newLogStartOffset of partition $topicPartition " +
        s"since it is larger than the high watermark ${highWatermark.messageOffset}")
    log.maybeIncrementLogStartOffset(newLogStartOffset)
  }

  def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
    if (newHighWatermark.messageOffset < 0)
      throw new IllegalArgumentException("High watermark offset should be non-negative")

    highWatermarkMetadata = newHighWatermark
    log.onHighWatermarkIncremented(newHighWatermark.messageOffset)
    trace(s"Setting high watermark for replica $brokerId partition $topicPartition to [$newHighWatermark]")
  }

  def highWatermark: LogOffsetMetadata = highWatermarkMetadata

  /*
   * Convert hw to local offset metadata by reading the log at the hw offset.
   * If the hw offset is out of range, return the first offset of the first log segment as the offset metadata.
   */
  def convertHWToLocalOffsetMetadata() {
    highWatermarkMetadata = log.convertToOffsetMetadata(highWatermarkMetadata.messageOffset).getOrElse {
      log.convertToOffsetMetadata(log.logStartOffset).getOrElse {
        val firstSegmentOffset = log.logSegments.head.baseOffset
        new LogOffsetMetadata(firstSegmentOffset, firstSegmentOffset, 0)
      }
    }
  }

  /**
   * The last stable offset (LSO) is defined as the first offset such that all lower offsets have been "decided."
   * Non-transactional messages are considered decided immediately, but transactional messages are only decided when
   * the corresponding COMMIT or ABORT marker is written. This implies that the last stable offset will be equal
   * to the high watermark if there are no transactional messages in the log. Note also that the LSO cannot advance
   * beyond the high watermark.
   */
  def lastStableOffset: LogOffsetMetadata = {
      log.firstUnstableOffset match {
        case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermark.messageOffset => offsetMetadata
        case _ => highWatermark
      }
  }

  override def toString: String = {
    val replicaString = new StringBuilder
    replicaString.append("Log(brokerId=" + brokerId)
    replicaString.append(s", topic=${topicPartition.topic}")
    replicaString.append(s", partition=${topicPartition.partition}")
    replicaString.append(s", highWatermark=$highWatermark")
    replicaString.append(s", lastStableOffset=$lastStableOffset")
    replicaString.append(")")
    replicaString.toString
  }
}

object LogInfo {

  def apply(brokerId: Int,
            topicPartition: TopicPartition,
            initialHighWatermarkValue: Long,
            log: Log): LogInfo = {
    if (log == null) {
      throw new KafkaException(s"Null value for log passed. Log cannot be created for $brokerId for $topicPartition")
    }
    new LogInfo(brokerId, topicPartition, initialHighWatermarkValue, log)
  }
}
