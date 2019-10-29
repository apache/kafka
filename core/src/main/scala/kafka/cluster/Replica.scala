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

import java.util
import java.util.concurrent.locks.ReentrantLock
import java.util.function.BiFunction

import kafka.log.Log
import kafka.server.{FollowerPendingFetchAvailabilityConfig, LogOffsetMetadata}
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

class PendingRequests {
  private val delegate = new util.HashMap[Long, Int]
  private val lock = new ReentrantLock()

  def add(offset:Long): Int = {
    CoreUtils.inLock(lock) {
      delegate.compute(offset, new BiFunction[Long, Int, Int] {
        override def apply(k: Long, v: Int): Int = Option(v) match {
          // Increment by 1 if it already exists.
          case Some(x) => x + 1
          // Initialize with 1.
          case _ => 1
        }
      })
    }
  }

  def remove(offset: Long): Boolean = {
    CoreUtils.inLock(lock) {
      Option(delegate.get(offset)) match {
        case Some(x) =>
          // if it is == 1, it should be removed.
          if (x == 1) delegate.remove(offset)
          // if it is > 1, decrement by 1 and set it.
          else if (x > 1) delegate.put(offset, x - 1)
          else // It should never reach here as the value is removed from the map when it has a value of 1.
            throw new IllegalStateException("Value should not be <= 0")
          
          true
        case _ => false
      }
    }
  }

  def contains(offset:Long): Boolean = {
    CoreUtils.inLock(lock) {
      delegate.containsKey(offset)
    }
  }

  def clear(): Unit = {
    CoreUtils.inLock(lock) {
      delegate.clear()
    }
  }

  def isEmpty() : Boolean ={
    CoreUtils.inLock(lock) {
      delegate.isEmpty
    }
  }
}

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

  // pending fetch request offsets which have not yet been finished processing.
  private val pendingRequests = new PendingRequests

  def mayBeInSync(reqTime: Long, replicaLagTimeMaxMs:Long): Boolean = {
    // 1 - if the lastCaughtUpTime is not lagging beyond replicaLagTimeMaxMs
    // 2 - if there are any pending fetch requests earlier to the lastfetch LEO then this replica can be considered as
    // insync to avoid making this replica out of sync when fetch request processing takes longer.
    reqTime - lastCaughtUpTimeMs <= replicaLagTimeMaxMs || !pendingRequests.isEmpty()
  }

  def logStartOffset: Long = _logStartOffset

  def logEndOffsetMetadata: LogOffsetMetadata = _logEndOffsetMetadata

  def logEndOffset: Long = logEndOffsetMetadata.messageOffset

  def lastCaughtUpTimeMs: Long = _lastCaughtUpTimeMs

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
                       followerPendingFetchAvailabilityConfig: FollowerPendingFetchAvailabilityConfig = Partition.defaultFollowerPendingFetchAvailabilityConfig,
                       time: Time = Time.SYSTEM): Unit = {

    val messageOffset = followerFetchOffsetMetadata.messageOffset

    val fetchTimeMs: Long = if (followerPendingFetchAvailabilityConfig.enable && followerFetchTimeMs > 0
      && pendingRequests.contains(messageOffset)) time.milliseconds()
    else followerFetchTimeMs

    if (followerFetchOffsetMetadata.messageOffset >= leaderEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, fetchTimeMs)
    else if (followerFetchOffsetMetadata.messageOffset >= lastFetchLeaderLogEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, lastFetchTimeMs)

    _logStartOffset = followerStartOffset
    _logEndOffsetMetadata = followerFetchOffsetMetadata
    lastFetchLeaderLogEndOffset = leaderEndOffset
    lastFetchTimeMs = followerFetchTimeMs
    trace(s"Updated state of replica to $this")
  }

  def updatePendingFetchMessageOffsetAsProcessed(messageOffset: Long): Unit = {
    pendingRequests.remove(messageOffset)
  }

  /**
   * Update Replica state with pending fetch requests if the requested offset is >= LEO when last fetch request is made.
   * This replica is considered insync if this fetch request could not be finished with in replica.lag.time.max
   *
   * @param fetchOffset
   */
  def updateFetchStatePreRead(fetchOffset: Long): Unit = {
    if(fetchOffset >= lastFetchLeaderLogEndOffset) pendingRequests.add(fetchOffset)
  }

  def clearPendingFetchRequests() : Unit = {
    trace(s"Current pending fetch request offsets before they are cleared: $pendingRequests")
    pendingRequests.clear()
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
    replicaString.append(s", pendingRequestOffsets=$pendingRequests")
    replicaString.append(")")
    replicaString.toString
  }

  override def equals(that: Any): Boolean = that match {
    case other: Replica => brokerId == other.brokerId && topicPartition == other.topicPartition
    case _ => false
  }

  override def hashCode: Int = 31 + topicPartition.hashCode + 17 * brokerId
}
