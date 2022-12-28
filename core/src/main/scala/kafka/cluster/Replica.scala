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

import kafka.log.UnifiedLog
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.log.internals.LogOffsetMetadata

import java.util.concurrent.atomic.AtomicReference

case class ReplicaState(
  // The log start offset value, kept in all replicas; for local replica it is the
  // log's start offset, for remote replicas its value is only updated by follower fetch.
  logStartOffset: Long,

  // The log end offset value, kept in all replicas; for local replica it is the
  // log's end offset, for remote replicas its value is only updated by follower fetch.
  logEndOffsetMetadata: LogOffsetMetadata,

  // The log end offset value at the time the leader received the last FetchRequest from this follower.
  // This is used to determine the lastCaughtUpTimeMs of the follower. It is reset by the leader
  // when a LeaderAndIsr request is received and might be reset when the leader appends a record
  // to its log.
  lastFetchLeaderLogEndOffset: Long,

  // The time when the leader received the last FetchRequest from this follower.
  // This is used to determine the lastCaughtUpTimeMs of the follower.
  lastFetchTimeMs: Long,

  // lastCaughtUpTimeMs is the largest time t such that the offset of most recent FetchRequest from this follower >=
  // the LEO of leader at time t. This is used to determine the lag of this follower and ISR of this partition.
  lastCaughtUpTimeMs: Long
) {
  /**
   * Returns the current log end offset of the replica.
   */
  def logEndOffset: Long = logEndOffsetMetadata.messageOffset

  /**
   * Returns true when the replica is considered as "caught-up". A replica is
   * considered "caught-up" when its log end offset is equals to the log end
   * offset of the leader OR when its last caught up time minus the current
   * time is smaller than the max replica lag.
   */
  def isCaughtUp(
    leaderEndOffset: Long,
    currentTimeMs: Long,
    replicaMaxLagMs: Long
  ): Boolean = {
    leaderEndOffset == logEndOffset || currentTimeMs - lastCaughtUpTimeMs <= replicaMaxLagMs
  }
}

object ReplicaState {
  val Empty: ReplicaState = ReplicaState(
    logEndOffsetMetadata = LogOffsetMetadata.UNKNOWN_OFFSET_METADATA,
    logStartOffset = UnifiedLog.UnknownOffset,
    lastFetchLeaderLogEndOffset = 0L,
    lastFetchTimeMs = 0L,
    lastCaughtUpTimeMs = 0L
  )
}

class Replica(val brokerId: Int, val topicPartition: TopicPartition) extends Logging {
  private val replicaState = new AtomicReference[ReplicaState](ReplicaState.Empty)

  def stateSnapshot: ReplicaState = replicaState.get

  /**
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
  def updateFetchState(
    followerFetchOffsetMetadata: LogOffsetMetadata,
    followerStartOffset: Long,
    followerFetchTimeMs: Long,
    leaderEndOffset: Long
  ): Unit = {
    replicaState.updateAndGet { currentReplicaState =>
      val lastCaughtUpTime = if (followerFetchOffsetMetadata.messageOffset >= leaderEndOffset) {
        math.max(currentReplicaState.lastCaughtUpTimeMs, followerFetchTimeMs)
      } else if (followerFetchOffsetMetadata.messageOffset >= currentReplicaState.lastFetchLeaderLogEndOffset) {
        math.max(currentReplicaState.lastCaughtUpTimeMs, currentReplicaState.lastFetchTimeMs)
      } else {
        currentReplicaState.lastCaughtUpTimeMs
      }

      ReplicaState(
        logStartOffset = followerStartOffset,
        logEndOffsetMetadata = followerFetchOffsetMetadata,
        lastFetchLeaderLogEndOffset = math.max(leaderEndOffset, currentReplicaState.lastFetchLeaderLogEndOffset),
        lastFetchTimeMs = followerFetchTimeMs,
        lastCaughtUpTimeMs = lastCaughtUpTime
      )
    }
  }

  /**
   * When the leader is elected or re-elected, the state of the follower is reinitialized
   * accordingly.
   */
  def resetReplicaState(
    currentTimeMs: Long,
    leaderEndOffset: Long,
    isNewLeader: Boolean,
    isFollowerInSync: Boolean
  ): Unit = {
    replicaState.updateAndGet { currentReplicaState =>
      // When the leader is elected or re-elected, the follower's last caught up time
      // is set to the current time if the follower is in the ISR, else to 0. The latter
      // is done to ensure that the high watermark is not hold back unnecessarily for
      // a follower which is not in the ISR anymore.
      val lastCaughtUpTimeMs = if (isFollowerInSync) currentTimeMs else 0L

      if (isNewLeader) {
        ReplicaState(
          logStartOffset = UnifiedLog.UnknownOffset,
          logEndOffsetMetadata = LogOffsetMetadata.UNKNOWN_OFFSET_METADATA,
          lastFetchLeaderLogEndOffset = UnifiedLog.UnknownOffset,
          lastFetchTimeMs = 0L,
          lastCaughtUpTimeMs = lastCaughtUpTimeMs
        )
      } else {
        ReplicaState(
          logStartOffset = currentReplicaState.logStartOffset,
          logEndOffsetMetadata = currentReplicaState.logEndOffsetMetadata,
          lastFetchLeaderLogEndOffset = leaderEndOffset,
          // When the leader is re-elected, the follower's last fetch time is
          // set to the current time if the follower is in the ISR, else to 0.
          // The latter is done to ensure that the follower is not brought back
          // into the ISR before a fetch is received.
          lastFetchTimeMs = if (isFollowerInSync) currentTimeMs else 0L,
          lastCaughtUpTimeMs = lastCaughtUpTimeMs
        )
      }
    }
    trace(s"Reset state of replica to $this")
  }

  override def toString: String = {
    val replicaState = this.replicaState.get
    val replicaString = new StringBuilder
    replicaString.append(s"Replica(replicaId=$brokerId")
    replicaString.append(s", topic=${topicPartition.topic}")
    replicaString.append(s", partition=${topicPartition.partition}")
    replicaString.append(s", lastCaughtUpTimeMs=${replicaState.lastCaughtUpTimeMs}")
    replicaString.append(s", logStartOffset=${replicaState.logStartOffset}")
    replicaString.append(s", logEndOffset=${replicaState.logEndOffsetMetadata.messageOffset}")
    replicaString.append(s", logEndOffsetMetadata=${replicaState.logEndOffsetMetadata}")
    replicaString.append(s", lastFetchLeaderLogEndOffset=${replicaState.lastFetchLeaderLogEndOffset}")
    replicaString.append(s", lastFetchTimeMs=${replicaState.lastFetchTimeMs}")
    replicaString.append(")")
    replicaString.toString
  }

  override def equals(that: Any): Boolean = that match {
    case other: Replica => brokerId == other.brokerId && topicPartition == other.topicPartition
    case _ => false
  }

  override def hashCode: Int = 31 + topicPartition.hashCode + 17 * brokerId
}
