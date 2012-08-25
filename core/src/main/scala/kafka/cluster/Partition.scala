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

import scala.collection._
import kafka.utils._
import java.lang.Object
import kafka.api.LeaderAndISR
import kafka.server.ReplicaManager
import kafka.common.ErrorMapping

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
class Partition(val topic: String,
                val partitionId: Int,
                time: Time,
                val replicaManager: ReplicaManager) extends Logging {
  private val localBrokerId = replicaManager.config.brokerId
  private val logManager = replicaManager.logManager
  private val replicaFetcherManager = replicaManager.replicaFetcherManager
  private val highwaterMarkCheckpoint = replicaManager.highWatermarkCheckpoint
  private val zkClient = replicaManager.zkClient
  var leaderReplicaIdOpt: Option[Int] = None
  var inSyncReplicas: Set[Replica] = Set.empty[Replica]
  private val assignedReplicaMap = new Pool[Int,Replica]
  private val leaderISRUpdateLock = new Object
  private var zkVersion: Int = LeaderAndISR.initialZKVersion
  private var leaderEpoch: Int = LeaderAndISR.initialLeaderEpoch - 1
  this.logIdent = "Partition [%s, %d] on broker %d, ".format(topic, partitionId, localBrokerId)

  private def isReplicaLocal(replicaId: Int) : Boolean = (replicaId == localBrokerId)

  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    val replicaOpt = getReplica(replicaId)
    replicaOpt match {
      case Some(replica) => replica
      case None =>
        if (isReplicaLocal(replicaId)) {
          val log = logManager.getOrCreateLog(topic, partitionId)
          val localReplica = new Replica(replicaId, this, time,
                                         highwaterMarkCheckpoint.read(topic, partitionId), Some(log))
          addReplicaIfNotExists(localReplica)
        }
        else {
          val remoteReplica = new Replica(replicaId, this, time)
          addReplicaIfNotExists(remoteReplica)
        }
        getReplica(replicaId).get
    }
  }

  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = {
    val replica = assignedReplicaMap.get(replicaId)
    if (replica == null)
      None
    else
      Some(replica)
  }

  def leaderReplicaIfLocal(): Option[Replica] = {
    leaderISRUpdateLock synchronized {
      leaderReplicaIdOpt match {
        case Some(leaderReplicaId) =>
          if (leaderReplicaId == localBrokerId)
            getReplica(localBrokerId)
          else
            None
        case None => None
      }
    }
  }

  def addReplicaIfNotExists(replica: Replica) = {
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)
  }

  def assignedReplicas(): Set[Replica] = {
    assignedReplicaMap.values.toSet
  }


  /**
   *  If the leaderEpoch of the incoming request is higher than locally cached epoch, make it the new leader of follower to the new leader.
   */
  def makeLeaderOrFollower(topic: String, partitionId: Int, leaderAndISR: LeaderAndISR, isMakingLeader: Boolean): Boolean = {
    leaderISRUpdateLock synchronized {
      if (leaderEpoch >= leaderAndISR.leaderEpoch){
        info("Current leaderEpoch [%d] is larger or equal to the requested leaderEpoch [%d], discard the become %s request"
          .format(leaderEpoch, leaderAndISR.leaderEpoch, if(isMakingLeader) "leader" else "follower"))
        return false
      }
      if(isMakingLeader)
        makeLeader(topic, partitionId, leaderAndISR)
      else
        makeFollower(topic, partitionId, leaderAndISR)
      true
    }
  }

  /**
   *  If the leaderEpoch of the incoming request is higher than locally cached epoch, make the local replica the leader in the following steps.
   *  1. stop the existing replica fetcher
   *  2. create replicas in ISR if needed (the ISR expand/shrink logic needs replicas in ISR to be available)
   *  3. reset LogEndOffset for remote replicas (there could be old LogEndOffset from the time when this broker was the leader last time)
   *  4. set the new leader and ISR
   */
  private def makeLeader(topic: String, partitionId: Int, leaderAndISR: LeaderAndISR) {
      trace("Started to become leader at the request %s".format(leaderAndISR.toString()))
      // stop replica fetcher thread, if any
      replicaFetcherManager.removeFetcher(topic, partitionId)

      val newInSyncReplicas = leaderAndISR.ISR.map(r => getOrCreateReplica(r)).toSet
      // reset LogEndOffset for remote replicas
      assignedReplicas.foreach(r => if (r.brokerId != localBrokerId) r.logEndOffset = ReplicaManager.UnknownLogEndOffset)
      inSyncReplicas = newInSyncReplicas
      leaderEpoch = leaderAndISR.leaderEpoch
      zkVersion = leaderAndISR.zkVersion
      leaderReplicaIdOpt = Some(localBrokerId)
      // we may need to increment high watermark since ISR could be down to 1
      maybeIncrementLeaderHW(getReplica().get)
  }

  /**
   *  1. stop any existing fetcher on this partition from the local replica
   *  2. make sure local replica exists and truncate the log to high watermark
   *  3. set the leader and set ISR to empty
   *  4. start a fetcher to the new leader
   */
  private def makeFollower(topic: String, partitionId: Int, leaderAndISR: LeaderAndISR) = {
      trace("Started to become follower at the request %s".format(leaderAndISR.toString()))
      val newLeaderBrokerId: Int = leaderAndISR.leader
      info("Starting the follower state transition to follow leader %d for topic %s partition %d"
                   .format(newLeaderBrokerId, topic, partitionId))
      val leaderBroker = ZkUtils.getBrokerInfoFromIds(zkClient, List(newLeaderBrokerId)).head
      // stop fetcher thread to previous leader
      replicaFetcherManager.removeFetcher(topic, partitionId)

      // make sure local replica exists
      val localReplica = getOrCreateReplica()
      localReplica.log.get.truncateTo(localReplica.highWatermark)
      inSyncReplicas = Set.empty[Replica]
      leaderEpoch = leaderAndISR.leaderEpoch
      zkVersion = leaderAndISR.zkVersion
      leaderReplicaIdOpt = Some(newLeaderBrokerId)
      // start fetcher thread to current leader
      replicaFetcherManager.addFetcher(topic, partitionId, localReplica.logEndOffset, leaderBroker)
  }

  def updateLeaderHWAndMaybeExpandISR(replicaId: Int, offset: Long) {
    leaderISRUpdateLock synchronized {
      debug("Recording follower %d position %d for topic %s partition %d".format(replicaId, offset, topic, partitionId))
      val replica = getOrCreateReplica(replicaId)
      replica.logEndOffset = offset

      // check if this replica needs to be added to the ISR
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          val replica = getReplica(replicaId).get
          val leaderHW = leaderReplica.highWatermark
          if (replica.logEndOffset >= leaderHW) {
            // expand ISR
            val newInSyncReplicas = inSyncReplicas + replica
            info("Expanding ISR for topic %s partition %d to %s".format(topic, partitionId, newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in ZK and cache
            updateISR(newInSyncReplicas)
          }
          maybeIncrementLeaderHW(leaderReplica)
        case None => // nothing to do if no longer leader
      }
    }
  }

  def checkEnoughReplicasReachOffset(requiredOffset: Long, requiredAcks: Int): (Boolean, Short) = {
    leaderISRUpdateLock synchronized {
      leaderReplicaIfLocal() match {
        case Some(_) =>
          val numAcks = inSyncReplicas.count(r => {
            if (!r.isLocal)
              r.logEndOffset >= requiredOffset
            else
              true /* also count the local (leader) replica */
          })
          trace("%d/%d acks satisfied for %s-%d".format(numAcks, requiredAcks, topic, partitionId))
          if ((requiredAcks < 0 && numAcks >= inSyncReplicas.size) ||
              (requiredAcks > 0 && numAcks >= requiredAcks)) {
            /*
            * requiredAcks < 0 means acknowledge after all replicas in ISR
            * are fully caught up to the (local) leader's offset
            * corresponding to this produce request.
            */
            (true, ErrorMapping.NoError)
          } else
            (false, ErrorMapping.NoError)
        case None =>
          (false, ErrorMapping.NotLeaderForPartitionCode)
      }
    }
  }
  
  def maybeIncrementLeaderHW(leaderReplica: Replica) {
    val allLogEndOffsets = inSyncReplicas.map(_.logEndOffset)
    val newHighWatermark = allLogEndOffsets.min
    val oldHighWatermark = leaderReplica.highWatermark
    if(newHighWatermark > oldHighWatermark)
      leaderReplica.highWatermark = newHighWatermark
    else
      debug("Old hw for topic %s partition %d is %d. New hw is %d. All leo's are %s"
            .format(topic, partitionId, oldHighWatermark, newHighWatermark, allLogEndOffsets.mkString(",")))
  }

  def maybeShrinkISR(replicaMaxLagTimeMs: Long,  replicaMaxLagBytes: Long) {
    leaderISRUpdateLock synchronized {
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs, replicaMaxLagBytes)
          if(outOfSyncReplicas.size > 0) {
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.size > 0)
            info("Shrinking ISR for topic %s partition %d to %s".format(topic, partitionId, newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache
            updateISR(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1
            maybeIncrementLeaderHW(leaderReplica)
          }
        case None => // do nothing if no longer leader
      }
    }
  }

  def getOutOfSyncReplicas(leaderReplica: Replica, keepInSyncTimeMs: Long, keepInSyncBytes: Long): Set[Replica] = {
    /**
     * there are two cases that need to be handled here -
     * 1. Stuck followers: If the leo of the replica is less than the leo of leader and the leo hasn't been updated
     *                     for keepInSyncTimeMs ms, the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the leo of the slowest follower is behind the leo of the leader by keepInSyncBytes, the
     *                     follower is not catching up and should be removed from the ISR
    **/
    val leaderLogEndOffset = leaderReplica.logEndOffset
    val candidateReplicas = inSyncReplicas - leaderReplica
    // Case 1 above
    val possiblyStuckReplicas = candidateReplicas.filter(r => r.logEndOffset < leaderLogEndOffset)
    debug("Possibly stuck replicas for topic %s partition %d are %s".format(topic, partitionId,
      possiblyStuckReplicas.map(_.brokerId).mkString(",")))
    val stuckReplicas = possiblyStuckReplicas.filter(r => r.logEndOffsetUpdateTimeMs < (time.milliseconds - keepInSyncTimeMs))
    debug("Stuck replicas for topic %s partition %d are %s".format(topic, partitionId, stuckReplicas.map(_.brokerId).mkString(",")))
    // Case 2 above
    val slowReplicas = candidateReplicas.filter(r => r.logEndOffset >= 0 && (leaderLogEndOffset - r.logEndOffset) > keepInSyncBytes)
    debug("Slow replicas for topic %s partition %d are %s".format(topic, partitionId, slowReplicas.map(_.brokerId).mkString(",")))
    stuckReplicas ++ slowReplicas
  }

  private def updateISR(newISR: Set[Replica]) {
    info("Updated ISR for topic %s partition %d to %s".format(topic, partitionId, newISR.mkString(",")))
    val newLeaderAndISR = new LeaderAndISR(localBrokerId, leaderEpoch, newISR.map(r => r.brokerId).toList, zkVersion)
    val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(zkClient,
      ZkUtils.getTopicPartitionLeaderAndISRPath(topic, partitionId), newLeaderAndISR.toString, zkVersion)
    if (updateSucceeded){
      inSyncReplicas = newISR
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newISR.mkString(","), zkVersion))
    } else {
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
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
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; Assigned replicas: " + assignedReplicaMap.keys.mkString(","))
    partitionString.append("; In Sync replicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString()
  }
}