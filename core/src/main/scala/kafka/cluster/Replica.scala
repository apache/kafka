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

import kafka.log.{Log}
import kafka.utils.Logging
import kafka.server.{LogOffsetMetadata}
import org.apache.kafka.common.{TopicPartition}

class Replica(val brokerId: Int, val topicPartition: TopicPartition) extends Logging {
  // the log end offset value, kept in all replicas;
  // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch
  @volatile private[this] var _logEndOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata
  // the log start offset value, kept in all replicas;
  // for local replica it is the log's start offset, for remote replicas its value is only updated by follower fetch
  @volatile private[this] var _logStartOffset = Log.UnknownOffset

  // The log end offset value at the time the leader received the last FetchRequest from this follower
  // This is used to determine the lastCaughtUpTimeMs of the follower
  @volatile private[this] var lastFetchLeaderLogEndOffset = 0L

  // The time when the leader received the last FetchRequest from this follower
  // This is used to determine the lastCaughtUpTimeMs of the follower
  @volatile private[this] var lastFetchTimeMs = 0L

  // lastCaughtUpTimeMs is the largest time t such that the offset of most recent FetchRequest from this follower >=
  // the LEO of leader at time t. This is used to determine the lag of this follower and ISR of this partition.
  @volatile private[this] var _lastCaughtUpTimeMs = 0L

  // highWatermark is the leader's high watermark after the most recent FetchRequest from this follower. This is
  // used to determine the maximum HW this follower knows about. See KIP-392
  @volatile private[this] var _lastSentHighWatermark = 0L

  @volatile private[this] var _lastReassignmentLag: Long = 0L

  def logStartOffset: Long = _logStartOffset

  def logEndOffsetMetadata: LogOffsetMetadata = _logEndOffsetMetadata

  def logEndOffset: Long = logEndOffsetMetadata.messageOffset

  def lastCaughtUpTimeMs: Long = _lastCaughtUpTimeMs

  def lastSentHighWatermark: Long = _lastSentHighWatermark

  def lastReassignmentLag: Long = _lastReassignmentLag

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
  def updateFetchState(followerFetchOffsetMetadata: LogOffsetMetadata,
                       followerStartOffset: Long,
                       followerFetchTimeMs: Long,
                       leaderEndOffset: Long,
                       isAddingReplica: Boolean): Unit = {
    if (followerFetchOffsetMetadata.messageOffset >= leaderEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, followerFetchTimeMs)
    else if (followerFetchOffsetMetadata.messageOffset >= lastFetchLeaderLogEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, lastFetchTimeMs)

    _logStartOffset = followerStartOffset
    _logEndOffsetMetadata = followerFetchOffsetMetadata
    lastFetchLeaderLogEndOffset = leaderEndOffset
    lastFetchTimeMs = followerFetchTimeMs
    trace(s"Updated state of replica to $this")
  }

  /**
    * Update the high watermark of this remote replica. This is used to track what we think is the last known HW to
    * a remote follower. Since this is recorded when we send a response, there is no way to guarantee that the follower
    * actually receives this HW. So we consider this to be an upper bound on what the follower knows.
    *
    * When handling fetches, the last sent high watermark for a replica is checked to see if we should return immediately
    * in order to propagate the HW more expeditiously. See KIP-392
    */
  def updateLastSentHighWatermark(highWatermark: Long): Unit = {
    _lastSentHighWatermark = highWatermark
    trace(s"Updated HW of replica to $highWatermark")
  }

  def updateReassignmentLag(messageOffset: Long, isAddingReplica: Boolean): Unit = {
    if (isAddingReplica)
      _lastReassignmentLag = Math.max(0, _lastSentHighWatermark - messageOffset)
    else
      _lastReassignmentLag = 0L
  }

  def resetLastCaughtUpTime(curLeaderLogEndOffset: Long, curTimeMs: Long, lastCaughtUpTimeMs: Long): Unit = {
    lastFetchLeaderLogEndOffset = curLeaderLogEndOffset
    lastFetchTimeMs = curTimeMs
    _lastCaughtUpTimeMs = lastCaughtUpTimeMs
    trace(s"Reset state of replica to $this")
  }

  override def toString: String = {
    val replicaString = new StringBuilder
    replicaString.append("Replica(replicaId=" + brokerId)
    replicaString.append(s", topic=${topicPartition.topic}")
    replicaString.append(s", partition=${topicPartition.partition}")
    replicaString.append(s", lastCaughtUpTimeMs=$lastCaughtUpTimeMs")
    replicaString.append(s", logStartOffset=$logStartOffset")
    replicaString.append(s", logEndOffset=$logEndOffset")
    replicaString.append(s", logEndOffsetMetadata=$logEndOffsetMetadata")
    replicaString.append(s", lastFetchLeaderLogEndOffset=$lastFetchLeaderLogEndOffset")
    replicaString.append(s", lastFetchTimeMs=$lastFetchTimeMs")
    replicaString.append(s", lastSentHighWatermark=$lastSentHighWatermark")
    replicaString.append(")")
    replicaString.toString
  }

  override def equals(that: Any): Boolean = that match {
    case other: Replica => brokerId == other.brokerId && topicPartition == other.topicPartition
    case _ => false
  }

  override def hashCode: Int = 31 + topicPartition.hashCode + 17 * brokerId
}
