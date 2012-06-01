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
import java.lang.IllegalStateException
import mutable.ListBuffer
import org.I0Itec.zkclient.ZkClient
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.{KafkaScheduler, ZkUtils, Time, Logging}
import kafka.common.InvalidPartitionException

class ReplicaManager(val config: KafkaConfig, time: Time, zkClient: ZkClient) extends Logging {

  private var allReplicas = new mutable.HashMap[(String, Int), Partition]()
  private var leaderReplicas = new ListBuffer[Partition]()
  private val leaderReplicaLock = new ReentrantLock()
  private var isrExpirationScheduler = new KafkaScheduler(1, "isr-expiration-thread-", true)
  // start ISR expiration thread
  isrExpirationScheduler.startUp
  isrExpirationScheduler.scheduleWithRate(maybeShrinkISR, 0, config.keepInSyncTimeMs)

  def addLocalReplica(topic: String, partitionId: Int, log: Log, assignedReplicaIds: Set[Int]): Replica = {
    val partition = getOrCreatePartition(topic, partitionId, assignedReplicaIds)
    val localReplica = new Replica(config.brokerId, partition, topic, Some(log))

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
    val remoteReplica = new Replica(replicaId, partition, topic)

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
        throw new IllegalStateException("Getting leader replica failed. Partition replica metadata for topic " +
      "%s partition %d doesn't exist in Replica manager on %d".format(topic, partitionId, config.brokerId))
    }
  }

  def getPartition(topic: String, partitionId: Int): Option[Partition] =
    allReplicas.get((topic, partitionId))

  def updateReplicaLEO(replica: Replica, fetchOffset: Long) {
    // set the replica leo
    val partition = ensurePartitionExists(replica.topic, replica.partition.partitionId)
    partition.updateReplicaLEO(replica, fetchOffset)
  }

  def maybeIncrementLeaderHW(replica: Replica) {
    // set the replica leo
    val partition = ensurePartitionExists(replica.topic, replica.partition.partitionId)
    // set the leader HW to min of the leo of all replicas
    val allLeos = partition.inSyncReplicas.map(_.logEndOffset())
    val newHw = allLeos.min
    val oldHw = partition.leaderHW()
    if(newHw > oldHw) {
      debug("Updating leader HW for topic %s partition %d to %d".format(replica.topic, replica.partition.partitionId, newHw))
      partition.leaderHW(Some(newHw))
    }
  }

  def makeLeader(replica: Replica, currentISRInZk: Seq[Int]) {
    // stop replica fetcher thread, if any
    replica.stopReplicaFetcherThread()
    // read and cache the ISR
    replica.partition.leaderId(Some(replica.brokerId))
    replica.partition.updateISR(currentISRInZk.toSet)
    // also add this partition to the list of partitions for which the leader is the current broker
    try {
      leaderReplicaLock.lock()
      leaderReplicas += replica.partition
    }finally {
      leaderReplicaLock.unlock()
    }
  }

  def makeFollower(replica: Replica, leaderBrokerId: Int, zkClient: ZkClient) {
    info("Broker %d becoming follower to leader %d for topic %s partition %d"
      .format(config.brokerId, leaderBrokerId, replica.topic, replica.partition.partitionId))
    // remove this replica's partition from the ISR expiration queue
    try {
      leaderReplicaLock.lock()
      leaderReplicas -= replica.partition
    }finally {
      leaderReplicaLock.unlock()
    }
    replica.log match {
      case Some(log) =>  // log is already started
        log.recoverUptoLastCheckpointedHW()
      case None =>
    }
    // get leader for this replica
    val leaderBroker = ZkUtils.getBrokerInfoFromIds(zkClient, List(leaderBrokerId)).head
    val isReplicaAFollower = replica.getIfFollowerAndLeader()
    // Become follower only if it is not already following the same leader
    if(!(isReplicaAFollower._1 && (isReplicaAFollower._2 == leaderBroker.id))) {
      // stop fetcher thread to previous leader
      replica.stopReplicaFetcherThread()
      // start fetcher thread to current leader
      replica.startReplicaFetcherThread(leaderBroker, config)
    }
  }

  def maybeShrinkISR(): Unit = {
    try {
      info("Evaluating ISR list of partitions to see which replicas can be removed from the ISR"
        .format(config.keepInSyncTimeMs))

      leaderReplicaLock.lock()
      leaderReplicas.foreach { partition =>
         // shrink ISR if a follower is slow or stuck
        val outOfSyncReplicas = partition.getOutOfSyncReplicas(config.keepInSyncTimeMs, config.keepInSyncBytes)
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

  def checkIfISRCanBeExpanded(replica: Replica): Boolean = {
    val partition = ensurePartitionExists(replica.topic, replica.partition.partitionId)
    if(partition.inSyncReplicas.contains(replica)) false
    else if(partition.assignedReplicas().contains(replica)) {
      val leaderHW = partition.leaderHW()
      replica.logEndOffset() >= leaderHW
    }
    else throw new IllegalStateException("Replica %s is not in the assigned replicas list for ".format(replica.toString) +
      " topic %s partition %d on broker %d".format(replica.topic, replica.partition.partitionId, config.brokerId))
  }

  def recordFollowerPosition(topic: String, partition: Int, replicaId: Int, offset: Long, zkClient: ZkClient) = {
    val replicaOpt = getReplica(topic, partition, replicaId)
    replicaOpt match {
      case Some(replica) =>
        updateReplicaLEO(replica, offset)
        // check if this replica needs to be added to the ISR
        if(checkIfISRCanBeExpanded(replica)) {
          val newISR = replica.partition.inSyncReplicas + replica
          // update ISR in ZK and cache
          replica.partition.updateISR(newISR.map(_.brokerId), Some(zkClient))
        }
        maybeIncrementLeaderHW(replica)
      case None =>
        throw new IllegalStateException("No replica %d in replica manager on %d".format(replicaId, config.brokerId))
    }
  }

  def recordLeaderLogUpdate(topic: String, partition: Int) = {
    val replicaOpt = getReplica(topic, partition, config.brokerId)
    replicaOpt match {
      case Some(replica) =>
        replica.logEndOffsetUpdateTime(Some(time.milliseconds))
      case None =>
        throw new IllegalStateException("No replica %d in replica manager on %d".format(config.brokerId, config.brokerId))
    }
  }

  def close() {
    isrExpirationScheduler.shutdown()
    allReplicas.foreach(_._2.assignedReplicas().foreach(_.close()))
  }
}
