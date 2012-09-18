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

import kafka.cluster.{Partition, Replica}
import collection._
import org.I0Itec.zkclient.ZkClient
import java.util.concurrent.atomic.AtomicBoolean
import kafka.utils._
import kafka.log.LogManager
import kafka.api.{LeaderAndIsrRequest, LeaderAndIsr}
import kafka.common.{UnknownTopicOrPartitionException, LeaderNotAvailableException, ErrorMapping}
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import java.util.concurrent.TimeUnit

object ReplicaManager {
  val UnknownLogEndOffset = -1L
}

class ReplicaManager(val config: KafkaConfig, time: Time, val zkClient: ZkClient, kafkaScheduler: KafkaScheduler,
                     val logManager: LogManager) extends Logging with KafkaMetricsGroup {
  private val allPartitions = new Pool[(String, Int), Partition]
  private var leaderPartitions = new mutable.HashSet[Partition]()
  private val leaderPartitionsLock = new Object
  val replicaFetcherManager = new ReplicaFetcherManager(config, this)
  this.logIdent = "Replica Manager on Broker " + config.brokerId + ": "

  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  val highWatermarkCheckpoint = new HighwaterMarkCheckpoint(config.logDir)
  info("Created highwatermark file %s".format(highWatermarkCheckpoint.name))

  newGauge(
    "LeaderCount",
    new Gauge[Int] {
      def value() = leaderPartitions.size
    }
  )
  newGauge(
    "UnderReplicatedPartitions",
    new Gauge[Int] {
      def value() = {
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
      kafkaScheduler.scheduleWithRate(checkpointHighWatermarks, "highwatermark-checkpoint-thread", 0, config.defaultFlushIntervalMs)
  }

  def startup() {
    // start ISR expiration thread
    kafkaScheduler.scheduleWithRate(maybeShrinkISR, "isr-expiration-thread-", 0, config.replicaMaxLagTimeMs)
  }

  def stopReplica(topic: String, partitionId: Int): Short  = {
    trace("Handling stop replica for partition [%s, %d]".format(topic, partitionId))
    val errorCode = ErrorMapping.NoError
    getReplica(topic, partitionId) match {
      case Some(replica) =>
        replicaFetcherManager.removeFetcher(topic, partitionId)
        /* TODO: handle deleteLog in a better way */
        //logManager.deleteLog(topic, partition)
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

  def getOrCreatePartition(topic: String, partitionId: Int): Partition = {
    var partition = allPartitions.get((topic, partitionId))
    if (partition == null) {
      allPartitions.putIfNotExists((topic, partitionId), new Partition(topic, partitionId, time, this))
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

  def getOrCreateReplica(topic: String, partitionId: Int, replicaId: Int = config.brokerId): Replica =  {
    getOrCreatePartition(topic, partitionId).getOrCreateReplica(replicaId)
  }

  def getReplica(topic: String, partitionId: Int, replicaId: Int = config.brokerId): Option[Replica] =  {
    val partitionOpt = getPartition(topic, partitionId)
    partitionOpt match {
      case None => None
      case Some(partition) => partition.getReplica(replicaId)
    }
  }

  def becomeLeaderOrFollower(leaderAndISRRequest: LeaderAndIsrRequest): collection.Map[(String, Int), Short] = {
    info("Handling leader and isr request %s".format(leaderAndISRRequest))
    val responseMap = new collection.mutable.HashMap[(String, Int), Short]

    for((partitionInfo, leaderAndISR) <- leaderAndISRRequest.leaderAndISRInfos){
      var errorCode = ErrorMapping.NoError
      val topic = partitionInfo._1
      val partitionId = partitionInfo._2

      val requestedLeaderId = leaderAndISR.leader
      try {
        if(requestedLeaderId == config.brokerId)
          makeLeader(topic, partitionId, leaderAndISR)
        else
          makeFollower(topic, partitionId, leaderAndISR)
      } catch {
        case e =>
          error("Error processing leaderAndISR request %s".format(leaderAndISRRequest), e)
          errorCode = ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]])
      }
      responseMap.put(partitionInfo, errorCode)
    }

    /**
     *  If IsInit flag is on, this means that the controller wants to treat topics not in the request
     *  as deleted.
     *  TODO: Handle this properly as part of KAFKA-330
     */
//    if(leaderAndISRRequest.isInit == LeaderAndIsrRequest.IsInit){
//      startHighWaterMarksCheckPointThread
//      val partitionsToRemove = allPartitions.filter(p => !leaderAndISRRequest.leaderAndISRInfos.contains(p._1)).map(entry => entry._1)
//      info("Init flag is set in leaderAndISR request, partitions to remove: %s".format(partitionsToRemove))
//      partitionsToRemove.foreach(p => stopReplica(p._1, p._2))
//    }

    responseMap
  }

  private def makeLeader(topic: String, partitionId: Int, leaderAndISR: LeaderAndIsr) = {
    info("Becoming Leader for topic [%s] partition [%d]".format(topic, partitionId))
    val partition = getOrCreatePartition(topic, partitionId)
    if (partition.makeLeaderOrFollower(topic, partitionId, leaderAndISR, true)) {
      // also add this partition to the list of partitions for which the leader is the current broker
      leaderPartitionsLock synchronized {
        leaderPartitions += partition
      } 
    }
    info("Completed the leader state transition for topic %s partition %d".format(topic, partitionId))
  }

  private def makeFollower(topic: String, partitionId: Int, leaderAndISR: LeaderAndIsr) {
    val leaderBrokerId: Int = leaderAndISR.leader
    info("Starting the follower state transition to follow leader %d for topic %s partition %d"
                 .format(leaderBrokerId, topic, partitionId))

    val partition = getOrCreatePartition(topic, partitionId)
    if (partition.makeLeaderOrFollower(topic, partitionId, leaderAndISR, false)) {
      // remove this replica's partition from the ISR expiration queue
      leaderPartitionsLock synchronized {
        leaderPartitions -= partition
      }
    }
  }

  private def maybeShrinkISR(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    leaderPartitionsLock synchronized {
      leaderPartitions.foreach(partition => partition.maybeShrinkISR(config.replicaMaxLagTimeMs, config.replicaMaxLagBytes))
    }
  }

  def recordFollowerPosition(topic: String, partitionId: Int, replicaId: Int, offset: Long) = {
    val partition = getOrCreatePartition(topic, partitionId)
    partition.updateLeaderHWAndMaybeExpandISR(replicaId, offset)
  }

  /**
   * Flushes the highwatermark value for all partitions to the highwatermark file
   */
  def checkpointHighWatermarks() {
    val highWaterarksForAllPartitions = allPartitions.map {
      partition =>
        val topic = partition._1._1
        val partitionId = partition._1._2
        val localReplicaOpt = partition._2.getReplica(config.brokerId)
        val hw = localReplicaOpt match {
          case Some(localReplica) => localReplica.highWatermark
          case None =>
            error("Highwatermark for topic %s partition %d doesn't exist during checkpointing"
                  .format(topic, partitionId))
             0L
        }
        (topic, partitionId) -> hw
    }.toMap
    highWatermarkCheckpoint.write(highWaterarksForAllPartitions)
    trace("Checkpointed high watermark data: %s".format(highWaterarksForAllPartitions))
  }

  def shutdown() {
    info("Shut down")
    replicaFetcherManager.shutdown()
    checkpointHighWatermarks()
    info("Shutted down completely")
  }
}
