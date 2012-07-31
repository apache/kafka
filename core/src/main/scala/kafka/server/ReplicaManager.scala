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

import kafka.log.Log
import kafka.cluster.{Partition, Replica}
import collection.mutable
import mutable.ListBuffer
import org.I0Itec.zkclient.ZkClient
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.{KafkaScheduler, ZkUtils, Time, Logging}
import kafka.common.{KafkaException, InvalidPartitionException}

class ReplicaManager(val config: KafkaConfig, time: Time, zkClient: ZkClient, kafkaScheduler: KafkaScheduler)
  extends Logging {

  private var allReplicas = new mutable.HashMap[(String, Int), Partition]()
  private var leaderReplicas = new ListBuffer[Partition]()
  private val leaderReplicaLock = new ReentrantLock()
  private val replicaFetcherManager = new ReplicaFetcherManager(config, this)
  private val highwaterMarkCheckpoint = new HighwaterMarkCheckpoint(config.logDir)
  info("Created highwatermark file %s on broker %d".format(highwaterMarkCheckpoint.name, config.brokerId))

  def startup() {
    // start the highwatermark checkpoint thread
    kafkaScheduler.scheduleWithRate(checkpointHighwaterMarks, "highwatermark-checkpoint-thread", 0,
      config.defaultFlushIntervalMs)
    // start ISR expiration thread
    kafkaScheduler.scheduleWithRate(maybeShrinkISR, "isr-expiration-thread-", 0, config.replicaMaxLagTimeMs)
  }

  def addLocalReplica(topic: String, partitionId: Int, log: Log, assignedReplicaIds: Set[Int]): Replica = {
    val partition = getOrCreatePartition(topic, partitionId, assignedReplicaIds)
    val localReplica = new Replica(config.brokerId, partition, topic, time,
      Some(readCheckpointedHighWatermark(topic, partitionId)), Some(log))

    val replicaOpt = partition.getReplica(config.brokerId)
    replicaOpt match {
      case Some(replica) =>
        info("Changing remote replica %s into a local replica".format(replica.toString))
        replica.log match {
          case None =>
            replica.log = Some(log)
          case Some(log) => // nothing to do since log already exists
        }
      case None =>
        partition.addReplica(localReplica)
    }
    val assignedReplicas = assignedReplicaIds.map(partition.getReplica(_).get)
    partition.assignedReplicas(Some(assignedReplicas))
    // get the replica objects for the assigned replicas for this partition
    info("Added local replica %d for topic %s partition %s on broker %d"
      .format(localReplica.brokerId, localReplica.topic, localReplica.partition.partitionId, localReplica.brokerId))
    localReplica
  }

  def getOrCreatePartition(topic: String, partitionId: Int, assignedReplicaIds: Set[Int]): Partition = {
    val newPartition = allReplicas.contains((topic, partitionId))
    newPartition match {
      case true => // partition exists, do nothing
        allReplicas.get((topic, partitionId)).get
      case false => // create remote replicas for each replica id in assignedReplicas
        val partition = new Partition(topic, partitionId, time)
        allReplicas += (topic, partitionId) -> partition
        (assignedReplicaIds - config.brokerId).foreach(
          replicaId => addRemoteReplica(topic, partitionId, replicaId, partition))
        partition
    }
  }

  def ensurePartitionExists(topic: String, partitionId: Int): Partition = {
    val partitionOpt = allReplicas.get((topic, partitionId))
    partitionOpt match {
      case Some(partition) => partition
      case None =>
        throw new InvalidPartitionException("Partition for topic %s partition %d doesn't exist in replica manager on %d"
          .format(topic, partitionId, config.brokerId))
    }
  }

  def addRemoteReplica(topic: String, partitionId: Int, replicaId: Int, partition: Partition): Replica = {
    val remoteReplica = new Replica(replicaId, partition, topic, time)

    val replicaAdded = partition.addReplica(remoteReplica)
    if(replicaAdded)
      info("Added remote replica %d for topic %s partition %s on broker %d"
        .format(remoteReplica.brokerId, remoteReplica.topic, remoteReplica.partition.partitionId, config.brokerId))
    remoteReplica
  }

  def getReplica(topic: String, partitionId: Int, replicaId: Int = config.brokerId): Option[Replica] = {
    val replicasOpt = allReplicas.get((topic, partitionId))
    replicasOpt match {
      case Some(replicas) =>
        replicas.getReplica(replicaId)
      case None =>
        None
    }
  }

  def getLeaderReplica(topic: String, partitionId: Int): Option[Replica] = {
    val replicasOpt = allReplicas.get((topic, partitionId))
    replicasOpt match {
      case Some(replicas) =>
        Some(replicas.leaderReplica())
      case None =>
        throw new KafkaException("Getting leader replica failed. Partition replica metadata for topic " +
          "%s partition %d doesn't exist in Replica manager on %d".format(topic, partitionId, config.brokerId))
    }
  }

  private def updateReplicaLeo(replica: Replica, fetchOffset: Long) {
    // set the replica leo
    val partition = ensurePartitionExists(replica.topic, replica.partition.partitionId)
    partition.updateReplicaLeo(replica, fetchOffset)
  }

  private def maybeIncrementLeaderHW(replica: Replica) {
    // set the replica leo
    val partition = ensurePartitionExists(replica.topic, replica.partition.partitionId)
    // set the leader HW to min of the leo of all replicas
    val allLeos = partition.inSyncReplicas.map(_.logEndOffset())
    val newHw = allLeos.min
    val oldHw = partition.leaderHW()
    if(newHw > oldHw) {
      debug("Updating leader HW for topic %s partition %d to %d".format(replica.topic, replica.partition.partitionId, newHw))
      partition.leaderHW(Some(newHw))
    }else
      debug("Old hw for topic %s partition %d is %d. New hw is %d. All leo's are %s".format(replica.topic,
        replica.partition.partitionId, oldHw, newHw, allLeos.mkString(",")))
  }

  def makeLeader(replica: Replica, currentISRInZk: Seq[Int]) {
    info("Broker %d started the leader state transition for topic %s partition %d"
      .format(config.brokerId, replica.topic, replica.partition.partitionId))
    try {
      // read and cache the ISR
      replica.partition.leaderId(Some(replica.brokerId))
      replica.partition.updateISR(currentISRInZk.toSet)
      // stop replica fetcher thread, if any
      replicaFetcherManager.removeFetcher(replica.topic, replica.partition.partitionId)
      // also add this partition to the list of partitions for which the leader is the current broker
      leaderReplicaLock.lock()
      leaderReplicas += replica.partition
      info("Broker %d completed the leader state transition for topic %s partition %d"
        .format(config.brokerId, replica.topic, replica.partition.partitionId))
    }catch {
      case e => error("Broker %d failed to complete the leader state transition for topic %s partition %d"
        .format(config.brokerId, replica.topic, replica.partition.partitionId), e)
    }finally {
      leaderReplicaLock.unlock()
    }
  }

  def makeFollower(replica: Replica, leaderBrokerId: Int, zkClient: ZkClient) {
    info("Broker %d starting the follower state transition to follow leader %d for topic %s partition %d"
      .format(config.brokerId, leaderBrokerId, replica.topic, replica.partition.partitionId))
    try {
      // set the leader for this partition correctly on this broker
      replica.partition.leaderId(Some(leaderBrokerId))
      replica.log match {
        case Some(log) =>  // log is already started
          log.truncateTo(replica.highWatermark())
        case None =>
      }
      // get leader for this replica
      val leaderBroker = ZkUtils.getBrokerInfoFromIds(zkClient, List(leaderBrokerId)).head
      val currentLeaderBroker = replicaFetcherManager.fetcherSourceBroker(replica.topic, replica.partition.partitionId)
      // become follower only if it is not already following the same leader
      if( currentLeaderBroker == None || currentLeaderBroker.get != leaderBroker.id) {
        info("broker %d becoming follower to leader %d for topic %s partition %d"
          .format(config.brokerId, leaderBrokerId, replica.topic, replica.partition.partitionId))
        // stop fetcher thread to previous leader
        replicaFetcherManager.removeFetcher(replica.topic, replica.partition.partitionId)
        // start fetcher thread to current leader
        replicaFetcherManager.addFetcher(replica.topic, replica.partition.partitionId, replica.logEndOffset(), leaderBroker)
      }
      // remove this replica's partition from the ISR expiration queue
      leaderReplicaLock.lock()
      leaderReplicas -= replica.partition
      info("Broker %d completed the follower state transition to follow leader %d for topic %s partition %d"
        .format(config.brokerId, leaderBrokerId, replica.topic, replica.partition.partitionId))
    }catch {
      case e => error("Broker %d failed to complete the follower state transition to follow leader %d for topic %s partition %d"
        .format(config.brokerId, leaderBrokerId, replica.topic, replica.partition.partitionId), e)
    }finally {
      leaderReplicaLock.unlock()
    }
  }

  private def maybeShrinkISR(): Unit = {
    try {
      info("Evaluating ISR list of partitions to see which replicas can be removed from the ISR"
        .format(config.replicaMaxLagTimeMs))
      leaderReplicaLock.lock()
      leaderReplicas.foreach { partition =>
      // shrink ISR if a follower is slow or stuck
        val outOfSyncReplicas = partition.getOutOfSyncReplicas(config.replicaMaxLagTimeMs, config.replicaMaxLagBytes)
        if(outOfSyncReplicas.size > 0) {
          val newInSyncReplicas = partition.inSyncReplicas -- outOfSyncReplicas
          assert(newInSyncReplicas.size > 0)
          info("Shrinking ISR for topic %s partition %d to %s".format(partition.topic, partition.partitionId,
            newInSyncReplicas.map(_.brokerId).mkString(",")))
          // update ISR in zk and in memory
          partition.updateISR(newInSyncReplicas.map(_.brokerId), Some(zkClient))
        }
      }
    }catch {
      case e1 => error("Error in ISR expiration thread. Shutting down due to ", e1)
    }finally {
      leaderReplicaLock.unlock()
    }
  }

  private def checkIfISRCanBeExpanded(replica: Replica): Boolean = {
    val partition = ensurePartitionExists(replica.topic, replica.partition.partitionId)
    if(partition.inSyncReplicas.contains(replica)) false
    else if(partition.assignedReplicas().contains(replica)) {
      val leaderHW = partition.leaderHW()
      replica.logEndOffset() >= leaderHW
    }
    else throw new KafkaException("Replica %s is not in the assigned replicas list for ".format(replica.toString) +
      " topic %s partition %d on broker %d".format(replica.topic, replica.partition.partitionId, config.brokerId))
  }

  def recordFollowerPosition(topic: String, partition: Int, replicaId: Int, offset: Long, zkClient: ZkClient) = {
    val replicaOpt = getReplica(topic, partition, replicaId)
    replicaOpt match {
      case Some(replica) =>
        updateReplicaLeo(replica, offset)
        // check if this replica needs to be added to the ISR
        if(checkIfISRCanBeExpanded(replica)) {
          val newISR = replica.partition.inSyncReplicas + replica
          // update ISR in ZK and cache
          replica.partition.updateISR(newISR.map(_.brokerId), Some(zkClient))
        }
        debug("Recording follower %d position %d for topic %s partition %d".format(replicaId, offset, topic, partition))
        maybeIncrementLeaderHW(replica)
      case None =>
        throw new KafkaException("No replica %d in replica manager on %d".format(replicaId, config.brokerId))
    }
  }

  def recordLeaderLogEndOffset(topic: String, partition: Int, logEndOffset: Long) = {
    val replicaOpt = getReplica(topic, partition, config.brokerId)
    replicaOpt match {
      case Some(replica) => replica.logEndOffset(Some(logEndOffset))
      case None =>
        throw new KafkaException("No replica %d in replica manager on %d".format(config.brokerId, config.brokerId))
    }
  }

  /**
   * Flushes the highwatermark value for all partitions to the highwatermark file
   */
  private def checkpointHighwaterMarks() {
    val highwaterMarksForAllPartitions = allReplicas.map { partition =>
      val topic = partition._1._1
      val partitionId = partition._1._2
      val localReplicaOpt = partition._2.getReplica(config.brokerId)
      val hw = localReplicaOpt match {
        case Some(localReplica) => localReplica.highWatermark()
        case None =>
          error("Error while checkpointing highwatermark for topic %s partition %d.".format(topic, partitionId) +
            " Replica metadata doesn't exist in replica manager on broker " + config.brokerId)
          0L
      }
      (topic, partitionId) -> hw
    }.toMap
    highwaterMarkCheckpoint.write(highwaterMarksForAllPartitions)
    info("Checkpointed highwatermarks")
  }

  /**
   * Reads the checkpointed highWatermarks for all partitions
   * @returns checkpointed value of highwatermark for topic, partition. If one doesn't exist, returns 0
   */
  def readCheckpointedHighWatermark(topic: String, partition: Int): Long = highwaterMarkCheckpoint.read(topic, partition)

  def shutdown() {
    replicaFetcherManager.shutdown()
    checkpointHighwaterMarks()
    info("Replica manager shutdown on broker " + config.brokerId)
  }
}
