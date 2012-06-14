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

import kafka.common.NoLeaderForPartitionException
import kafka.utils.{SystemTime, Time, Logging}
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZkUtils._
import java.util.concurrent.locks.ReentrantLock
import java.lang.IllegalStateException

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
class Partition(val topic: String,
                val partitionId: Int,
                time: Time = SystemTime,
                var inSyncReplicas: Set[Replica] = Set.empty[Replica]) extends Logging {
  private var leaderReplicaId: Option[Int] = None
  private var assignedReplicas: Set[Replica] = Set.empty[Replica]
  private var highWatermarkUpdateTime: Long = -1L
  private val leaderISRUpdateLock = new ReentrantLock()

  def leaderId(newLeader: Option[Int] = None): Option[Int] = {
    try {
      leaderISRUpdateLock.lock()
      if(newLeader.isDefined) {
        info("Updating leader for topic %s partition %d to replica %d".format(topic, partitionId, newLeader.get))
        leaderReplicaId = newLeader
      }
      leaderReplicaId
    }finally {
      leaderISRUpdateLock.unlock()
    }
  }

  def assignedReplicas(replicas: Option[Set[Replica]] = None): Set[Replica] = {
    replicas match {
      case Some(ar) =>
        assignedReplicas = ar
      case None =>
    }
    assignedReplicas
  }

  def getReplica(replicaId: Int): Option[Replica] = assignedReplicas().find(_.brokerId == replicaId)

  def addReplica(replica: Replica): Boolean = {
    if(!assignedReplicas.contains(replica)) {
      assignedReplicas += replica
      true
    }else false
  }

  def updateReplicaLEO(replica: Replica, leo: Long) {
    replica.leoUpdateTime = time.milliseconds
    replica.logEndOffset(Some(leo))
    debug("Updating the leo to %d for replica %d".format(leo, replica.brokerId))
  }

  def leaderReplica(): Replica = {
    val leaderReplicaId = leaderId()
    if(leaderReplicaId.isDefined) {
      val leaderReplica = assignedReplicas().find(_.brokerId == leaderReplicaId.get)
      if(leaderReplica.isDefined) leaderReplica.get
      else throw new IllegalStateException("No replica for leader %d in the replica manager"
        .format(leaderReplicaId.get))
    }else
      throw new NoLeaderForPartitionException("Leader for topic %s partition %d does not exist"
        .format(topic, partitionId))
  }

  def leaderHW(newHw: Option[Long] = None): Long = {
    newHw match {
      case Some(highWatermark) =>
        leaderReplica().highWatermark(newHw)
        highWatermarkUpdateTime = time.milliseconds
        highWatermark
      case None =>
        leaderReplica().highWatermark()
    }
  }

  def hwUpdateTime: Long = highWatermarkUpdateTime

  def getOutOfSyncReplicas(keepInSyncTimeMs: Long, keepInSyncBytes: Long): Set[Replica] = {
    /**
     * there are two cases that need to be handled here -
     * 1. Stuck followers: If the leo of the replica is less than the leo of leader and the leo hasn't been updated
     *                     for keepInSyncTimeMs ms, the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the leo of the slowest follower is behind the leo of the leader by keepInSyncBytes, the
     *                     follower is not catching up and should be removed from the ISR
    **/
    // Case 1 above
    val possiblyStuckReplicas = inSyncReplicas.filter(r => r.logEndOffset() < leaderReplica().logEndOffset())
    info("Possibly stuck replicas for topic %s partition %d are %s".format(topic, partitionId,
      possiblyStuckReplicas.map(_.brokerId).mkString(",")))
    val stuckReplicas = possiblyStuckReplicas.filter(r => r.logEndOffsetUpdateTime() < (time.milliseconds - keepInSyncTimeMs))
    info("Stuck replicas for topic %s partition %d are %s".format(topic, partitionId, stuckReplicas.map(_.brokerId).mkString(",")))
    val leader = leaderReplica()
    // Case 2 above
    val slowReplicas = inSyncReplicas.filter(r => (leader.logEndOffset() - r.logEndOffset()) > keepInSyncBytes)
    info("Slow replicas for topic %s partition %d are %s".format(topic, partitionId, slowReplicas.map(_.brokerId).mkString(",")))
    stuckReplicas ++ slowReplicas
  }

  def updateISR(newISR: Set[Int], zkClientOpt: Option[ZkClient] = None) {
    try {
      leaderISRUpdateLock.lock()
      zkClientOpt match {
        case Some(zkClient) =>
          // update ISR in ZK
          updateISRInZk(newISR, zkClient)
        case None =>
      }
      // update partition's ISR in cache
      inSyncReplicas = newISR.map {r =>
        getReplica(r) match {
          case Some(replica) => replica
          case None => throw new IllegalStateException("ISR update failed. No replica for id %d".format(r))
        }
      }
      info("Updated ISR for for topic %s partition %d to %s in cache".format(topic, partitionId, newISR.mkString(",")))
    }catch {
      case e => throw new IllegalStateException("Failed to update ISR for topic %s ".format(topic) +
        "partition %d to %s".format(partitionId, newISR.mkString(",")), e)
    }finally {
      leaderISRUpdateLock.unlock()
    }
  }

  private def updateISRInZk(newISR: Set[Int], zkClient: ZkClient) = {
    val replicaListAndEpochString = readDataMaybeNull(zkClient, getTopicPartitionInSyncPath(topic, partitionId.toString))
    if(replicaListAndEpochString == null) {
      throw new NoLeaderForPartitionException(("Illegal partition state. ISR cannot be updated for topic " +
        "%s partition %d since leader and ISR does not exist in ZK".format(topic, partitionId)))
    }
    else {
      val replicasAndEpochInfo = replicaListAndEpochString.split(";")
      val epoch = replicasAndEpochInfo.last
      updatePersistentPath(zkClient, getTopicPartitionInSyncPath(topic, partitionId.toString),
        "%s;%s".format(newISR.mkString(","), epoch))
      info("Updating ISR for for topic %s partition %d to %s in ZK".format(topic, partitionId, newISR.mkString(",")))
    }
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Partition]))
      return false
    val other = that.asInstanceOf[Partition]
    if(topic.equals(other.topic) && partitionId == other.partitionId)
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*partitionId
  }

  override def toString(): String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderId())
    partitionString.append("; Assigned replicas: " + assignedReplicas().map(_.brokerId).mkString(","))
    partitionString.append("; In Sync replicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString()
  }
}
