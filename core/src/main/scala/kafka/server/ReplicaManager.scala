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
package kafka.server

import kafka.cluster.{Broker, Partition, Replica}
import collection._
import mutable.HashMap
import org.I0Itec.zkclient.ZkClient
import java.util.concurrent.atomic.AtomicBoolean
import kafka.utils._
import kafka.log.LogManager
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import java.util.concurrent.TimeUnit
import kafka.common._
import kafka.api.{StopReplicaRequest, PartitionStateInfo, LeaderAndIsrRequest}
import kafka.controller.KafkaController
import org.apache.log4j.Logger


object ReplicaManager {
  val UnknownLogEndOffset = -1L
}

class ReplicaManager(val config: KafkaConfig, 
                     time: Time, 
                     val zkClient: ZkClient, 
                     kafkaScheduler: KafkaScheduler,
                     val logManager: LogManager) extends Logging with KafkaMetricsGroup {
  /* epoch of the controller that last changed the leader */
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  private val localBrokerId = config.brokerId
  private val allPartitions = new Pool[(String, Int), Partition]
  private var leaderPartitions = new mutable.HashSet[Partition]()
  private val leaderPartitionsLock = new Object
  val replicaFetcherManager = new ReplicaFetcherManager(config, this)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  val highWatermarkCheckpoints = config.logDirs.map(dir => (dir, new HighwaterMarkCheckpoint(dir))).toMap
  private var hwThreadInitialized = false
  this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: "
  private val stateChangeLogger = Logger.getLogger(KafkaController.stateChangeLogger)

  newGauge(
    "LeaderCount",
    new Gauge[Int] {
      def value = {
        leaderPartitionsLock synchronized {
          leaderPartitions.size
        }
      }
    }
  )
  newGauge(
    "PartitionCount",
    new Gauge[Int] {
      def value = allPartitions.size
    }
  )
  newGauge(
    "UnderReplicatedPartitions",
    new Gauge[Int] {
      def value = {
        leaderPartitionsLock synchronized {
          leaderPartitions.count(_.isUnderReplicated)
        }
      }
    }
  )
  val isrExpandRate = newMeter("IsrExpandsPerSec",  "expands", TimeUnit.SECONDS)
  val isrShrinkRate = newMeter("ISRShrinksPerSec",  "shrinks", TimeUnit.SECONDS)


  def startHighWaterMarksCheckPointThread() = {
    if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      kafkaScheduler.scheduleWithRate(checkpointHighWatermarks, "highwatermark-checkpoint-thread", 0, config.replicaHighWatermarkCheckpointIntervalMs)
  }

  /**
   * This function is only used in two places: in Partition.updateISR() and KafkaApis.handleProducerRequest().
   * In the former case, the partition should have been created, in the latter case, return -1 will put the request into purgatory
   */
  def getReplicationFactorForPartition(topic: String, partitionId: Int) = {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case Some(partition) =>
        partition.replicationFactor
      case None =>
        -1
    }
  }

  def startup() {
    // start ISR expiration thread
    kafkaScheduler.scheduleWithRate(maybeShrinkIsr, "isr-expiration-thread-", 0, config.replicaLagTimeMaxMs)
  }

  def stopReplica(topic: String, partitionId: Int, deletePartition: Boolean): Short  = {
    stateChangeLogger.trace("Broker %d handling stop replica for partition [%s,%d]".format(localBrokerId, topic, partitionId))
    val errorCode = ErrorMapping.NoError
    getReplica(topic, partitionId) match {
      case Some(replica) =>
        replicaFetcherManager.removeFetcher(topic, partitionId)
        /* TODO: handle deleteLog in a better way */
        //if (deletePartition)
        //  logManager.deleteLog(topic, partition)
        leaderPartitionsLock synchronized {
          leaderPartitions -= replica.partition
        }
        if(deletePartition)
          allPartitions.remove((topic, partitionId))
      case None => //do nothing if replica no longer exists
    }
    stateChangeLogger.trace("Broker %d finished handling stop replica for partition [%s,%d]".format(localBrokerId, topic, partitionId))
    errorCode
  }

  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[(String, Int), Short], Short) = {
    val responseMap = new collection.mutable.HashMap[(String, Int), Short]
    if(stopReplicaRequest.controllerEpoch < controllerEpoch) {
      stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d."
        .format(localBrokerId, stopReplicaRequest.controllerEpoch) +
            " Latest known controller epoch is %d " + controllerEpoch)
      (responseMap, ErrorMapping.StaleControllerEpochCode)
    } else {
      controllerEpoch = stopReplicaRequest.controllerEpoch
      val responseMap = new HashMap[(String, Int), Short]
      for((topic, partitionId) <- stopReplicaRequest.partitions){
        val errorCode = stopReplica(topic, partitionId, stopReplicaRequest.deletePartitions)
        responseMap.put((topic, partitionId), errorCode)
      }
      (responseMap, ErrorMapping.NoError)
    }
  }

  def getOrCreatePartition(topic: String, partitionId: Int, replicationFactor: Int): Partition = {
    var partition = allPartitions.get((topic, partitionId))
    if (partition == null) {
      allPartitions.putIfNotExists((topic, partitionId), new Partition(topic, partitionId, replicationFactor, time, this))
      partition = allPartitions.get((topic, partitionId))
    }
    partition
  }

  def getPartition(topic: String, partitionId: Int): Option[Partition] = {
    val partition = allPartitions.get((topic, partitionId))
    if (partition == null)
      None
    else
      Some(partition)
  }

  def getReplicaOrException(topic: String, partition: Int): Replica = {
    val replicaOpt = getReplica(topic, partition)
    if(replicaOpt.isDefined)
      return replicaOpt.get
    else
      throw new ReplicaNotAvailableException("Replica %d is not available for partition [%s,%d]".format(config.brokerId, topic, partition))
  }

  def getLeaderReplicaIfLocal(topic: String, partitionId: Int): Replica =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None =>
        throw new UnknownTopicOrPartitionException("Partition [%s,%d] doesn't exist on %d".format(topic, partitionId, config.brokerId))
      case Some(partition) =>
        partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => leaderReplica
          case None =>
            throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
                    .format(topic, partitionId, config.brokerId))
        }
    }
  }

  def getReplica(topic: String, partitionId: Int, replicaId: Int = config.brokerId): Option[Replica] =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None => None
      case Some(partition) => partition.getReplica(replicaId)
    }
  }

  def becomeLeaderOrFollower(leaderAndISRRequest: LeaderAndIsrRequest): (collection.Map[(String, Int), Short], Short) = {
    leaderAndISRRequest.partitionStateInfos.foreach(p =>
      stateChangeLogger.trace("Broker %d handling LeaderAndIsr request correlation id %d received from controller %d epoch %d for partition [%s,%d]"
                                .format(localBrokerId, leaderAndISRRequest.correlationId, leaderAndISRRequest.controllerId,
                                        leaderAndISRRequest.controllerEpoch, p._1._1, p._1._2)))
    info("Handling LeaderAndIsr request %s".format(leaderAndISRRequest))

    val responseMap = new collection.mutable.HashMap[(String, Int), Short]
    if(leaderAndISRRequest.controllerEpoch < controllerEpoch) {
      stateChangeLogger.warn("Broker %d received LeaderAndIsr request correlation id %d with an old controller epoch %d. Latest known controller epoch is %d"
                                .format(localBrokerId, leaderAndISRRequest.controllerEpoch, leaderAndISRRequest.correlationId, controllerEpoch))
      (responseMap, ErrorMapping.StaleControllerEpochCode)
    }else {
      val controllerId = leaderAndISRRequest.controllerId
      controllerEpoch = leaderAndISRRequest.controllerEpoch
      for((topicAndPartition, partitionStateInfo) <- leaderAndISRRequest.partitionStateInfos) {
        var errorCode = ErrorMapping.NoError
        val topic = topicAndPartition._1
        val partitionId = topicAndPartition._2

        val requestedLeaderId = partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader
        try {
          if(requestedLeaderId == config.brokerId)
            makeLeader(controllerId, controllerEpoch, topic, partitionId, partitionStateInfo, leaderAndISRRequest.correlationId)
          else
            makeFollower(controllerId, controllerEpoch, topic, partitionId, partitionStateInfo, leaderAndISRRequest.aliveLeaders,
                         leaderAndISRRequest.correlationId)
        } catch {
          case e =>
            val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d " +
                            "epoch %d for partition %s").format(localBrokerId, leaderAndISRRequest.correlationId, leaderAndISRRequest.controllerId,
                                                                leaderAndISRRequest.controllerEpoch, topicAndPartition)
            stateChangeLogger.error(errorMsg, e)
            errorCode = ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]])
        }
        responseMap.put(topicAndPartition, errorCode)
        stateChangeLogger.trace("Broker %d handled LeaderAndIsr request correlationId %d received from controller %d epoch %d for partition [%s,%d]"
          .format(localBrokerId, leaderAndISRRequest.correlationId, leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch,
          topicAndPartition._1, topicAndPartition._2))
      }
      info("Handled leader and isr request %s".format(leaderAndISRRequest))
      // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
      // have been completely populated before starting the checkpointing there by avoiding weird race conditions
      if (!hwThreadInitialized) {
        startHighWaterMarksCheckPointThread()
        hwThreadInitialized = true
      }
      replicaFetcherManager.shutdownIdleFetcherThreads()
      (responseMap, ErrorMapping.NoError)
    }
  }

  private def makeLeader(controllerId: Int, epoch:Int, topic: String, partitionId: Int,
                         partitionStateInfo: PartitionStateInfo, correlationId: Int) = {
    val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
    stateChangeLogger.trace(("Broker %d received LeaderAndIsr request correlationId %d from controller %d epoch %d " +
                             "starting the become-leader transition for partition [%s,%d]")
                               .format(localBrokerId, correlationId, controllerId, epoch, topic, partitionId))
    val partition = getOrCreatePartition(topic, partitionId, partitionStateInfo.replicationFactor)
    if (partition.makeLeader(controllerId, topic, partitionId, leaderIsrAndControllerEpoch, correlationId)) {
      // also add this partition to the list of partitions for which the leader is the current broker
      leaderPartitionsLock synchronized {
        leaderPartitions += partition
      } 
    }
    stateChangeLogger.trace("Broker %d completed become-leader transition for partition [%s,%d]".format(localBrokerId, topic, partitionId))
  }

  private def makeFollower(controllerId: Int, epoch: Int, topic: String, partitionId: Int,
                           partitionStateInfo: PartitionStateInfo, aliveLeaders: Set[Broker], correlationId: Int) {
    val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
    stateChangeLogger.trace(("Broker %d received LeaderAndIsr request correlationId %d from controller %d epoch %d " +
                             "starting the become-follower transition for partition [%s,%d]")
                               .format(localBrokerId, correlationId, controllerId, epoch, topic, partitionId))

    val partition = getOrCreatePartition(topic, partitionId, partitionStateInfo.replicationFactor)
    if (partition.makeFollower(controllerId, topic, partitionId, leaderIsrAndControllerEpoch, aliveLeaders, correlationId)) {
      // remove this replica's partition from the ISR expiration queue
      leaderPartitionsLock synchronized {
        leaderPartitions -= partition
      }
    }
    stateChangeLogger.trace("Broker %d completed the become-follower transition for partition [%s,%d]".format(localBrokerId, topic, partitionId))
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    leaderPartitionsLock synchronized {
      leaderPartitions.foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs, config.replicaLagMaxMessages))
    }
  }

  def recordFollowerPosition(topic: String, partitionId: Int, replicaId: Int, offset: Long) = {
    val partitionOpt = getPartition(topic, partitionId)
    if(partitionOpt.isDefined) {
      partitionOpt.get.updateLeaderHWAndMaybeExpandIsr(replicaId, offset)
    } else {
      warn("While recording the follower position, the partition [%s,%d] hasn't been created, skip updating leader HW".format(topic, partitionId))
    }
  }

  /**
   * Flushes the highwatermark value for all partitions to the highwatermark file
   */
  def checkpointHighWatermarks() {
    val replicas = allPartitions.values.map(_.getReplica(config.brokerId)).collect{case Some(replica) => replica}
    val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParent)
    for((dir, reps) <- replicasByDir) {
      val hwms = reps.map(r => (TopicAndPartition(r.topic, r.partitionId) -> r.highWatermark)).toMap
      highWatermarkCheckpoints(dir).write(hwms)
    }
  }

  def shutdown() {
    info("Shut down")
    replicaFetcherManager.shutdown()
    checkpointHighWatermarks()
    info("Shutted down completely")
  }
}
