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
import kafka.api.LeaderAndIsr
import kafka.server.ReplicaManager
import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.common.{NotLeaderForPartitionException, ErrorMapping}
import kafka.controller.{LeaderIsrAndControllerEpoch, KafkaController}
import org.apache.log4j.Logger
import kafka.message.ByteBufferMessageSet


/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
class Partition(val topic: String,
                val partitionId: Int,
                var replicationFactor: Int,
                time: Time,
                val replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {
  private val localBrokerId = replicaManager.config.brokerId
  private val logManager = replicaManager.logManager
  private val replicaFetcherManager = replicaManager.replicaFetcherManager
  private val zkClient = replicaManager.zkClient
  var leaderReplicaIdOpt: Option[Int] = None
  var inSyncReplicas: Set[Replica] = Set.empty[Replica]
  private val assignedReplicaMap = new Pool[Int,Replica]
  private val leaderIsrUpdateLock = new Object
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)
  private val stateChangeLogger = Logger.getLogger(KafkaController.stateChangeLogger)

  private def isReplicaLocal(replicaId: Int) : Boolean = (replicaId == localBrokerId)

  newGauge(
    topic + "-" + partitionId + "-UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    }
  )

  def isUnderReplicated(): Boolean = {
    leaderIsrUpdateLock synchronized {
      inSyncReplicas.size < replicationFactor
    }
  }

  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    val replicaOpt = getReplica(replicaId)
    replicaOpt match {
      case Some(replica) => replica
      case None =>
        if (isReplicaLocal(replicaId)) {
          val log = logManager.getOrCreateLog(topic, partitionId)
          val offset = replicaManager.highWatermarkCheckpoints(log.dir.getParent).read(topic, partitionId).min(log.logEndOffset)
          val localReplica = new Replica(replicaId, this, time, offset, Some(log))
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
    leaderIsrUpdateLock synchronized {
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
   *  If the leaderEpoch of the incoming request is higher than locally cached epoch, make the local replica the leader in the following steps.
   *  1. stop the existing replica fetcher
   *  2. create replicas in ISR if needed (the ISR expand/shrink logic needs replicas in ISR to be available)
   *  3. reset LogEndOffset for remote replicas (there could be old LogEndOffset from the time when this broker was the leader last time)
   *  4. set the new leader and ISR
   */
  def makeLeader(controllerId: Int, topic: String, partitionId: Int,
                 leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch, correlationId: Int): Boolean = {
    leaderIsrUpdateLock synchronized {
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      if (leaderEpoch >= leaderAndIsr.leaderEpoch){
        stateChangeLogger.trace(("Broker %d discarded the become-leader request with correlation id %d from " +
                                 "controller %d epoch %d for partition [%s,%d] since current leader epoch %d is >= the request's leader epoch %d")
                                   .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch, topic,
                                           partitionId, leaderEpoch, leaderAndIsr.leaderEpoch))
        return false
      }
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
      // stop replica fetcher thread, if any
      replicaFetcherManager.removeFetcher(topic, partitionId)

      val newInSyncReplicas = leaderAndIsr.isr.map(r => getOrCreateReplica(r)).toSet
      // reset LogEndOffset for remote replicas
      assignedReplicas.foreach(r => if (r.brokerId != localBrokerId) r.logEndOffset = ReplicaManager.UnknownLogEndOffset)
      inSyncReplicas = newInSyncReplicas
      leaderEpoch = leaderAndIsr.leaderEpoch
      zkVersion = leaderAndIsr.zkVersion
      leaderReplicaIdOpt = Some(localBrokerId)
      // we may need to increment high watermark since ISR could be down to 1
      maybeIncrementLeaderHW(getReplica().get)
      true
    }
  }

  /**
   *  If the leaderEpoch of the incoming request is higher than locally cached epoch, make the local replica the follower in the following steps.
   *  1. stop any existing fetcher on this partition from the local replica
   *  2. make sure local replica exists and truncate the log to high watermark
   *  3. set the leader and set ISR to empty
   *  4. start a fetcher to the new leader
   */
  def makeFollower(controllerId: Int, topic: String, partitionId: Int, leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                   aliveLeaders: Set[Broker], correlationId: Int): Boolean = {
    leaderIsrUpdateLock synchronized {
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      if (leaderEpoch >= leaderAndIsr.leaderEpoch) {
        stateChangeLogger.trace(("Broker %d discarded the become-follower request with correlation id %d from " +
                                 "controller %d epoch %d for partition [%s,%d] since current leader epoch %d is >= the request's leader epoch %d")
                                   .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch, topic,
                                           partitionId, leaderEpoch, leaderAndIsr.leaderEpoch))
        return false
      }
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
      // make sure local replica exists. This reads the last check pointed high watermark from disk. On startup, it is
      // important to ensure that this operation happens for every single partition in a leader and isr request, else
      // some high watermark values could be overwritten with 0. This leads to replicas fetching from the earliest offset
      // on the leader
      val localReplica = getOrCreateReplica()
      val newLeaderBrokerId: Int = leaderAndIsr.leader
      aliveLeaders.find(_.id == newLeaderBrokerId) match {
        case Some(leaderBroker) =>
          // stop fetcher thread to previous leader
          replicaFetcherManager.removeFetcher(topic, partitionId)
          localReplica.log.get.truncateTo(localReplica.highWatermark)
          inSyncReplicas = Set.empty[Replica]
          leaderEpoch = leaderAndIsr.leaderEpoch
          zkVersion = leaderAndIsr.zkVersion
          leaderReplicaIdOpt = Some(newLeaderBrokerId)
          // start fetcher thread to current leader
          replicaFetcherManager.addFetcher(topic, partitionId, localReplica.logEndOffset, leaderBroker)
        case None => // leader went down
          stateChangeLogger.trace("Broker %d aborted the become-follower state change with correlation id %d from " +
            " controller %d epoch %d since leader %d for partition [%s,%d] is unavailable during the state change operation"
                                     .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch,
                                              newLeaderBrokerId, topic, partitionId))
      }
      true
    }
  }

  def updateLeaderHWAndMaybeExpandIsr(replicaId: Int, offset: Long) {
    leaderIsrUpdateLock synchronized {
      debug("Recording follower %d position %d for partition [%s,%d].".format(replicaId, offset, topic, partitionId))
      val replica = getOrCreateReplica(replicaId)
      replica.logEndOffset = offset

      // check if this replica needs to be added to the ISR
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          val replica = getReplica(replicaId).get
          val leaderHW = leaderReplica.highWatermark
          if (!inSyncReplicas.contains(replica) && replica.logEndOffset >= leaderHW) {
            // expand ISR
            val newInSyncReplicas = inSyncReplicas + replica
            info("Expanding ISR for topic %s partition %d from %s to %s"
                 .format(topic, partitionId, inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in ZK and cache
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }
          maybeIncrementLeaderHW(leaderReplica)
        case None => // nothing to do if no longer leader
      }
    }
  }

  def checkEnoughReplicasReachOffset(requiredOffset: Long, requiredAcks: Int): (Boolean, Short) = {
    leaderIsrUpdateLock synchronized {
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

  /**
   * There is no need to acquire the leaderIsrUpdate lock here since all callers of this private API acquire that lock
   * @param leaderReplica
   */
  private def maybeIncrementLeaderHW(leaderReplica: Replica) {
    val allLogEndOffsets = inSyncReplicas.map(_.logEndOffset)
    val newHighWatermark = allLogEndOffsets.min
    val oldHighWatermark = leaderReplica.highWatermark
    if(newHighWatermark > oldHighWatermark) {
      leaderReplica.highWatermark = newHighWatermark
      debug("Highwatermark for topic %s partition %d updated to %d".format(topic, partitionId, newHighWatermark))
    }
    else
      debug("Old hw for topic %s partition %d is %d. New hw is %d. All leo's are %s"
        .format(topic, partitionId, oldHighWatermark, newHighWatermark, allLogEndOffsets.mkString(",")))
  }

  def maybeShrinkIsr(replicaMaxLagTimeMs: Long,  replicaMaxLagMessages: Long) {
    leaderIsrUpdateLock synchronized {
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs, replicaMaxLagMessages)
          if(outOfSyncReplicas.size > 0) {
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.size > 0)
            info("Shrinking ISR for topic %s partition %d from %s to %s".format(topic, partitionId,
              inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1
            maybeIncrementLeaderHW(leaderReplica)
            replicaManager.isrShrinkRate.mark()
          }
        case None => // do nothing if no longer leader
      }
    }
  }

  def getOutOfSyncReplicas(leaderReplica: Replica, keepInSyncTimeMs: Long, keepInSyncMessages: Long): Set[Replica] = {
    /**
     * there are two cases that need to be handled here -
     * 1. Stuck followers: If the leo of the replica is less than the leo of leader and the leo hasn't been updated
     *                     for keepInSyncTimeMs ms, the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the leo of the slowest follower is behind the leo of the leader by keepInSyncMessages, the
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
    val slowReplicas = candidateReplicas.filter(r => r.logEndOffset >= 0 && (leaderLogEndOffset - r.logEndOffset) > keepInSyncMessages)
    debug("Slow replicas for topic %s partition %d are %s".format(topic, partitionId, slowReplicas.map(_.brokerId).mkString(",")))
    stuckReplicas ++ slowReplicas
  }

  def appendMessagesToLeader(messages: ByteBufferMessageSet): (Long, Long) = {
    leaderIsrUpdateLock synchronized {
      val leaderReplicaOpt = leaderReplicaIfLocal()
      leaderReplicaOpt match {
        case Some(leaderReplica) =>
          val log = leaderReplica.log.get
          val (start, end) = log.append(messages, assignOffsets = true)
          // we may need to increment high watermark since ISR could be down to 1
          maybeIncrementLeaderHW(leaderReplica)
          (start, end)
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
            .format(topic, partitionId, localBrokerId))
      }
    }
  }

  private def updateIsr(newIsr: Set[Replica]) {
    debug("Updated ISR for topic %s partition %d to %s".format(topic, partitionId, newIsr.mkString(",")))
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(r => r.brokerId).toList, zkVersion)
    // use the epoch of the controller that made the leadership decision, instead of the current controller epoch
    val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(zkClient,
      ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partitionId),
      ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch), zkVersion)
    if (updateSucceeded){
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
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
    leaderIsrUpdateLock synchronized {
      val partitionString = new StringBuilder
      partitionString.append("Topic: " + topic)
      partitionString.append("; Partition: " + partitionId)
      partitionString.append("; Leader: " + leaderReplicaIdOpt)
      partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
      partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
      partitionString.toString()
    }
  }
}
