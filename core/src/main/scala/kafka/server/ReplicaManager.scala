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

import collection._
import mutable.HashMap
import kafka.cluster.{Broker, Partition, Replica}
import kafka.utils._
import kafka.log.LogManager
import kafka.metrics.KafkaMetricsGroup
import kafka.common._
import kafka.api.{StopReplicaRequest, PartitionStateInfo, LeaderAndIsrRequest}
import kafka.controller.KafkaController
import org.apache.log4j.Logger
import org.I0Itec.zkclient.ZkClient
import com.yammer.metrics.core.Gauge
import java.util.concurrent.atomic.AtomicBoolean
import java.io.{IOException, File}
import java.util.concurrent.TimeUnit


object ReplicaManager {
  val UnknownLogEndOffset = -1L
  val HighWatermarkFilename = "replication-offset-checkpoint"
}

class ReplicaManager(val config: KafkaConfig, 
                     time: Time, 
                     val zkClient: ZkClient, 
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean ) extends Logging with KafkaMetricsGroup {
  /* epoch of the controller that last changed the leader */
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  private val localBrokerId = config.brokerId
  private val allPartitions = new Pool[(String, Int), Partition]
  private var leaderPartitions = new mutable.HashSet[Partition]()
  private val leaderPartitionsLock = new Object
  private val replicaStateChangeLock = new Object
  val replicaFetcherManager = new ReplicaFetcherManager(config, this)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  val highWatermarkCheckpoints = config.logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, ReplicaManager.HighWatermarkFilename)))).toMap
  private var hwThreadInitialized = false
  this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: "
  val stateChangeLogger = Logger.getLogger(KafkaController.stateChangeLogger)

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
      def value = underReplicatedPartitionCount()
    }
  )
  val isrExpandRate = newMeter("IsrExpandsPerSec",  "expands", TimeUnit.SECONDS)
  val isrShrinkRate = newMeter("IsrShrinksPerSec",  "shrinks", TimeUnit.SECONDS)

  def underReplicatedPartitionCount(): Int = {
    leaderPartitionsLock synchronized {
      leaderPartitions.count(_.isUnderReplicated)
    }
  }

  def startHighWaterMarksCheckPointThread() = {
    if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
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
    scheduler.schedule("isr-expiration", maybeShrinkIsr, period = config.replicaLagTimeMaxMs, unit = TimeUnit.MILLISECONDS)
  }

  def stopReplica(topic: String, partitionId: Int, deletePartition: Boolean): Short  = {
    stateChangeLogger.trace("Broker %d handling stop replica (delete=%s) for partition [%s,%d]".format(localBrokerId,
      deletePartition.toString, topic, partitionId))
    val errorCode = ErrorMapping.NoError
    getPartition(topic, partitionId) match {
      case Some(partition) =>
        leaderPartitionsLock synchronized {
          leaderPartitions -= partition
        }
        if(deletePartition) {
          val removedPartition = allPartitions.remove((topic, partitionId))
          if (removedPartition != null)
            removedPartition.delete() // this will delete the local log
        }
      case None => //do nothing if replica no longer exists. This can happen during delete topic retries
        stateChangeLogger.trace("Broker %d ignoring stop replica (delete=%s) for partition [%s,%d] as replica doesn't exist on broker"
          .format(localBrokerId, deletePartition, topic, partitionId))
    }
    stateChangeLogger.trace("Broker %d finished handling stop replica (delete=%s) for partition [%s,%d]"
      .format(localBrokerId, deletePartition, topic, partitionId))
    errorCode
  }

  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicAndPartition, Short], Short) = {
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[TopicAndPartition, Short]
      if(stopReplicaRequest.controllerEpoch < controllerEpoch) {
        stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d."
          .format(localBrokerId, stopReplicaRequest.controllerEpoch) +
          " Latest known controller epoch is %d " + controllerEpoch)
        (responseMap, ErrorMapping.StaleControllerEpochCode)
      } else {
        controllerEpoch = stopReplicaRequest.controllerEpoch
        // First stop fetchers for all partitions, then stop the corresponding replicas
        replicaFetcherManager.removeFetcherForPartitions(stopReplicaRequest.partitions.map(r => TopicAndPartition(r.topic, r.partition)))
        for(topicAndPartition <- stopReplicaRequest.partitions){
          val errorCode = stopReplica(topicAndPartition.topic, topicAndPartition.partition, stopReplicaRequest.deletePartitions)
          responseMap.put(topicAndPartition, errorCode)
        }
        (responseMap, ErrorMapping.NoError)
      }
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
    leaderAndISRRequest.partitionStateInfos.foreach { case ((topic, partition), stateInfo) =>
      stateChangeLogger.trace("Broker %d received LeaderAndIsr request %s correlation id %d from controller %d epoch %d for partition [%s,%d]"
                                .format(localBrokerId, stateInfo, leaderAndISRRequest.correlationId,
                                        leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch, topic, partition))
    }
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[(String, Int), Short]
      if(leaderAndISRRequest.controllerEpoch < controllerEpoch) {
        leaderAndISRRequest.partitionStateInfos.foreach { case ((topic, partition), stateInfo) =>
        stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d since " +
          "its controller epoch %d is old. Latest known controller epoch is %d").format(localBrokerId, leaderAndISRRequest.controllerId,
          leaderAndISRRequest.correlationId, leaderAndISRRequest.controllerEpoch, controllerEpoch))
        }
        (responseMap, ErrorMapping.StaleControllerEpochCode)
      } else {
        val controllerId = leaderAndISRRequest.controllerId
        val correlationId = leaderAndISRRequest.correlationId
        controllerEpoch = leaderAndISRRequest.controllerEpoch

        // First check partition's leader epoch
        val partitionState = new HashMap[Partition, PartitionStateInfo]()
        leaderAndISRRequest.partitionStateInfos.foreach{ case ((topic, partitionId), partitionStateInfo) =>
          val partition = getOrCreatePartition(topic, partitionId, partitionStateInfo.replicationFactor)
          val partitionLeaderEpoch = partition.getLeaderEpoch()
          // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
          // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
          if (partitionLeaderEpoch < partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch) {
            if(partitionStateInfo.allReplicas.contains(config.brokerId))
              partitionState.put(partition, partitionStateInfo)
            else {
              stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
                "epoch %d for partition [%s,%d] as itself is not in assigned replica list %s")
                .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                topic, partition.partitionId, partitionStateInfo.allReplicas.mkString(",")))
            }
          } else {
            // Otherwise record the error code in response
            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
              "epoch %d for partition [%s,%d] since its associated leader epoch %d is old. Current leader epoch is %d")
              .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
              topic, partition.partitionId, partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch, partitionLeaderEpoch))
            responseMap.put((topic, partitionId), ErrorMapping.StaleLeaderEpochCode)
          }
        }

        val partitionsTobeLeader = partitionState
          .filter{ case (partition, partitionStateInfo) => partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader == config.brokerId}
        val partitionsToBeFollower = (partitionState -- partitionsTobeLeader.keys)

        if (!partitionsTobeLeader.isEmpty) makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, leaderAndISRRequest.correlationId, responseMap)
        if (!partitionsToBeFollower.isEmpty) makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, leaderAndISRRequest.leaders, leaderAndISRRequest.correlationId, responseMap)

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
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int, epoch: Int,
                          partitionState: Map[Partition, PartitionStateInfo],
                          correlationId: Int, responseMap: mutable.Map[(String, Int), Short]) = {
    partitionState.foreach(state =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId))))

    for (partition <- partitionState.keys)
      responseMap.put((partition.topic, partition.partitionId), ErrorMapping.NoError)

    try {
      // First stop fetchers for all the partitions
      replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(new TopicAndPartition(_)))
      partitionState.foreach { state =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-leader request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(state._1.topic, state._1.partitionId)))
      }
      // Update the partition information to be the leader
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        partition.makeLeader(controllerId, partitionStateInfo, correlationId)}

      // Finally add these partitions to the list of partitions for which the leader is the current broker
      leaderPartitionsLock synchronized {
        leaderPartitions ++= partitionState.keySet
      }
    } catch {
      case e: Throwable =>
        partitionState.foreach { state =>
          val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d" +
            " epoch %d for partition %s").format(localBrokerId, correlationId, controllerId, epoch,
                                                TopicAndPartition(state._1.topic, state._1.partitionId))
          stateChangeLogger.error(errorMsg, e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it
   */
  private def makeFollowers(controllerId: Int, epoch: Int, partitionState: Map[Partition, PartitionStateInfo],
                            leaders: Set[Broker], correlationId: Int, responseMap: mutable.Map[(String, Int), Short]) {
    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }

    for (partition <- partitionState.keys)
      responseMap.put((partition.topic, partition.partitionId), ErrorMapping.NoError)

    try {
      leaderPartitionsLock synchronized {
        leaderPartitions --= partitionState.keySet
      }

      var partitionsToMakeFollower: Set[Partition] = Set()

      // TODO: Delete leaders from LeaderAndIsrRequest in 0.8.1
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch
        val newLeaderBrokerId = leaderIsrAndControllerEpoch.leaderAndIsr.leader
        leaders.find(_.id == newLeaderBrokerId) match {
          case Some(leaderBroker) =>
            if (partition.makeFollower(controllerId, partitionStateInfo, correlationId))
              partitionsToMakeFollower += partition
            else
              stateChangeLogger.info(("Broker %d skipped the become-follower state change after marking its partition as follower with correlation id %d from " +
                "controller %d epoch %d for partition [%s,%d] since the new leader %d is the same as the old leader")
                .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch,
                partition.topic, partition.partitionId, newLeaderBrokerId))
          case None =>
            // The leader broker should always be present in the leaderAndIsrRequest.
            // If not, we should record the error message and abort the transition process for this partition
            stateChangeLogger.error(("Broker %d aborted the become-follower state change with correlation id %d from " +
              "controller %d epoch %d for partition [%s,%d] since new leader %d is not currently available")
              .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch,
              partition.topic, partition.partitionId, newLeaderBrokerId))
        }
      }

      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(new TopicAndPartition(_)))
      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(partition.topic, partition.partitionId)))
      }

      logManager.truncateTo(partitionsToMakeFollower.map(partition => (new TopicAndPartition(partition), partition.getOrCreateReplica().highWatermark)).toMap)

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d truncated logs and checkpointed recovery boundaries for partition [%s,%d] as part of " +
          "become-follower request with correlation id %d from controller %d epoch %d").format(localBrokerId,
          partition.topic, partition.partitionId, correlationId, controllerId, epoch))
      }

      if (isShuttingDown.get()) {
        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d skipped the adding-fetcher step of the become-follower state change with correlation id %d from " +
            "controller %d epoch %d for partition [%s,%d] since it is shutting down").format(localBrokerId, correlationId,
            controllerId, epoch, partition.topic, partition.partitionId))
        }
      }
      else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
          new TopicAndPartition(partition) -> BrokerAndInitialOffset(leaders.find(_.id == partition.leaderReplicaIdOpt.get).get, partition.getReplica().get.logEndOffset)).toMap
        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)

        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d started fetcher to new leader as part of become-follower request from controller " +
            "%d epoch %d with correlation id %d for partition [%s,%d]")
            .format(localBrokerId, controllerId, epoch, correlationId, partition.topic, partition.partitionId))
        }
      }
    } catch {
      case e: Throwable =>
        val errorMsg = ("Error on broker %d while processing LeaderAndIsr request with correlationId %d received from controller %d " +
          "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
        stateChangeLogger.error(errorMsg, e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.foreach { state =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
    }
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    var curLeaderPartitions: List[Partition] = null
    leaderPartitionsLock synchronized {
      curLeaderPartitions = leaderPartitions.toList
    }
    curLeaderPartitions.foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs, config.replicaLagMaxMessages))
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
      val hwms = reps.map(r => (new TopicAndPartition(r) -> r.highWatermark)).toMap
      try {
        highWatermarkCheckpoints(dir).write(hwms)
      } catch {
        case e: IOException =>
          fatal("Error writing to highwatermark file: ", e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  def shutdown() {
    info("Shut down")
    replicaFetcherManager.shutdown()
    checkpointHighWatermarks()
    info("Shutted down completely")
  }
}
