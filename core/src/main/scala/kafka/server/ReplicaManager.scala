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
  private val allPartitions = new Pool[(String, Int), Partition]
  private var leaderPartitions = new mutable.HashSet[Partition]()
  private val leaderPartitionsLock = new Object
  val replicaFetcherManager = new ReplicaFetcherManager(config, this)
  this.logIdent = "Replica Manager on Broker " + config.brokerId + ": "
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  val highWatermarkCheckpoints = config.logDirs.map(dir => (dir, new HighwaterMarkCheckpoint(dir))).toMap

  newGauge(
    "LeaderCount",
    new Gauge[Int] {
      def getValue = leaderPartitions.size
    }
  )
  newGauge(
    "UnderReplicatedPartitions",
    new Gauge[Int] {
      def getValue = {
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
      kafkaScheduler.scheduleWithRate(checkpointHighWatermarks, "highwatermark-checkpoint-thread", 0, config.highWaterMarkCheckpointIntervalMs)
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
    kafkaScheduler.scheduleWithRate(maybeShrinkIsr, "isr-expiration-thread-", 0, config.replicaMaxLagTimeMs)
  }

  def stopReplica(topic: String, partitionId: Int, deletePartition: Boolean): Short  = {
    trace("Handling stop replica for partition [%s, %d]".format(topic, partitionId))
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
        allPartitions.remove((topic, partitionId))
        info("After removing partition (%s, %d), the rest of allReplicas is: [%s]".format(topic, partitionId, allPartitions))
      case None => //do nothing if replica no longer exists
    }
    trace("Finish handling stop replica [%s, %d]".format(topic, partitionId))
    errorCode
  }

  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[(String, Int), Short], Short) = {
    val responseMap = new collection.mutable.HashMap[(String, Int), Short]
    if(stopReplicaRequest.controllerEpoch < controllerEpoch) {
      error("Received stop replica request from an old controller epoch %d.".format(stopReplicaRequest.controllerEpoch) +
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
      throw new ReplicaNotAvailableException("Replica %d is not available for partiton [%s, %d] yet".format(config.brokerId, topic, partition))
  }

  def getLeaderReplicaIfLocal(topic: String, partitionId: Int): Replica =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None =>
        throw new UnknownTopicOrPartitionException("Topic %s partition %d doesn't exist on %d".format(topic, partitionId, config.brokerId))
      case Some(partition) =>
        partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => leaderReplica
          case None =>
            throw new LeaderNotAvailableException("Leader not local for topic %s partition %d on broker %d"
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
    info("Handling leader and isr request %s".format(leaderAndISRRequest))
    val responseMap = new collection.mutable.HashMap[(String, Int), Short]
    if(leaderAndISRRequest.controllerEpoch < controllerEpoch) {
      error("Received leader and isr request from an old controller epoch %d.".format(leaderAndISRRequest.controllerEpoch) +
        " Latest known controller epoch is %d " + controllerEpoch)
      (responseMap, ErrorMapping.StaleControllerEpochCode)
    }else {
      controllerEpoch = leaderAndISRRequest.controllerEpoch
      for((topicAndPartition, partitionStateInfo) <- leaderAndISRRequest.partitionStateInfos) {
        var errorCode = ErrorMapping.NoError
        val topic = topicAndPartition._1
        val partitionId = topicAndPartition._2

        val requestedLeaderId = partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader
        try {
          if(requestedLeaderId == config.brokerId)
            makeLeader(topic, partitionId, partitionStateInfo)
          else
            makeFollower(topic, partitionId, partitionStateInfo, leaderAndISRRequest.leaders)
        } catch {
          case e =>
            error("Error processing leaderAndISR request %s".format(leaderAndISRRequest), e)
            errorCode = ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]])
        }
        responseMap.put(topicAndPartition, errorCode)
      }
      info("Completed leader and isr request %s".format(leaderAndISRRequest))
      replicaFetcherManager.shutdownIdleFetcherThreads()
      (responseMap, ErrorMapping.NoError)
    }
  }

  private def makeLeader(topic: String, partitionId: Int, partitionStateInfo: PartitionStateInfo) = {
    val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
    info("Becoming Leader for topic [%s] partition [%d]".format(topic, partitionId))
    val partition = getOrCreatePartition(topic, partitionId, partitionStateInfo.replicationFactor)
    if (partition.makeLeader(topic, partitionId, leaderIsrAndControllerEpoch)) {
      // also add this partition to the list of partitions for which the leader is the current broker
      leaderPartitionsLock synchronized {
        leaderPartitions += partition
      } 
    }
    info("Completed the leader state transition for topic %s partition %d".format(topic, partitionId))
  }

  private def makeFollower(topic: String, partitionId: Int, partitionStateInfo: PartitionStateInfo,
                           liveBrokers: Set[Broker]) {
    val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
    val leaderBrokerId: Int = leaderIsrAndControllerEpoch.leaderAndIsr.leader
    info("Starting the follower state transition to follow leader %d for topic %s partition %d"
                 .format(leaderBrokerId, topic, partitionId))

    val partition = getOrCreatePartition(topic, partitionId, partitionStateInfo.replicationFactor)
    if (partition.makeFollower(topic, partitionId, leaderIsrAndControllerEpoch, liveBrokers)) {
      // remove this replica's partition from the ISR expiration queue
      leaderPartitionsLock synchronized {
        leaderPartitions -= partition
      }
    }
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    leaderPartitionsLock synchronized {
      leaderPartitions.foreach(partition => partition.maybeShrinkIsr(config.replicaMaxLagTimeMs, config.replicaMaxLagBytes))
    }
  }

  def recordFollowerPosition(topic: String, partitionId: Int, replicaId: Int, offset: Long) = {
    val partitionOpt = getPartition(topic, partitionId)
    if(partitionOpt.isDefined) {
      partitionOpt.get.updateLeaderHWAndMaybeExpandIsr(replicaId, offset)
    } else {
      warn("While recording the follower position, the partition [%s, %d] hasn't been created, skip updating leader HW".format(topic, partitionId))
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
