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

import kafka.cluster._
import kafka.common.TopicAndPartition

import kafka.api._
import kafka.controller.{KafkaController, LeaderIsrAndControllerEpoch}
import org.apache.kafka.common.Node

import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import scala.collection.{mutable, Seq, Set}
import scala.collection.JavaConverters._
import kafka.utils.Logging
import kafka.utils.CoreUtils._

import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 */
private[server] class MetadataCache(brokerId: Int) extends Logging {
  private val stateChangeLogger = KafkaController.stateChangeLogger
  private val cache: mutable.Map[String, mutable.Map[Int, PartitionStateInfo]] =
    new mutable.HashMap[String, mutable.Map[Int, PartitionStateInfo]]()
  private var aliveBrokers: Map[Int, Broker] = Map()
  private val partitionMetadataLock = new ReentrantReadWriteLock()

  this.logIdent = "[Kafka Metadata Cache on broker %d] ".format(brokerId)

  private def getAliveEndpoints(brokers: Iterable[Int], protocol: SecurityProtocol): Seq[BrokerEndPoint] = {
    val result = new mutable.ArrayBuffer[BrokerEndPoint](math.min(aliveBrokers.size, brokers.size))
    brokers.foreach { brokerId =>
      getAliveEndpoint(brokerId, protocol).foreach(result +=)
    }
    result
  }

  private def getAliveEndpoint(brokerId: Int, protocol: SecurityProtocol): Option[BrokerEndPoint] =
    aliveBrokers.get(brokerId).map(_.getBrokerEndPoint(protocol))

  private def getPartitionMetadata(topic: String, protocol: SecurityProtocol): Option[Iterable[PartitionMetadata]] = {
    cache.get(topic).map { partitions =>
      partitions.map { case (partitionId, partitionState) =>
        val topicPartition = TopicAndPartition(topic, partitionId)

        val leaderAndIsr = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr
        val maybeLeader = aliveBrokers.get(leaderAndIsr.leader)

        val replicas = partitionState.allReplicas
        val replicaInfo = getAliveEndpoints(replicas, protocol)

        maybeLeader match {
          case None =>
            debug("Error while fetching metadata for %s: leader not available".format(topicPartition))
            new PartitionMetadata(partitionId, None, replicaInfo, Seq.empty[BrokerEndPoint],
              Errors.LEADER_NOT_AVAILABLE.code)

          case Some(leader) =>
            val isr = leaderAndIsr.isr
            val isrInfo = getAliveEndpoints(isr, protocol)

            if (replicaInfo.size < replicas.size) {
              debug("Error while fetching metadata for %s: replica information not available for following brokers %s"
                .format(topicPartition, replicas.filterNot(replicaInfo.map(_.id).contains).mkString(",")))

              new PartitionMetadata(partitionId, Some(leader.getBrokerEndPoint(protocol)), replicaInfo, isrInfo, Errors.REPLICA_NOT_AVAILABLE.code)
            } else if (isrInfo.size < isr.size) {
              debug("Error while fetching metadata for %s: in sync replica information not available for following brokers %s"
                .format(topicPartition, isr.filterNot(isrInfo.map(_.id).contains).mkString(",")))
              new PartitionMetadata(partitionId, Some(leader.getBrokerEndPoint(protocol)), replicaInfo, isrInfo, Errors.REPLICA_NOT_AVAILABLE.code)
            } else {
              new PartitionMetadata(partitionId, Some(leader.getBrokerEndPoint(protocol)), replicaInfo, isrInfo, Errors.NONE.code)
            }
        }
      }
    }
  }

  def getTopicMetadata(topics: Set[String], protocol: SecurityProtocol): Seq[TopicMetadata] = {
    inReadLock(partitionMetadataLock) {
      val topicsRequested = if (topics.isEmpty) cache.keySet else topics
      topicsRequested.toSeq.flatMap { topic =>
        getPartitionMetadata(topic, protocol).map { partitionMetadata =>
          new TopicMetadata(topic, partitionMetadata.toBuffer, Errors.NONE.code)
        }
      }
    }
  }

  def hasTopicMetadata(topic: String): Boolean = {
    inReadLock(partitionMetadataLock) {
      cache.contains(topic)
    }
  }

  def getAllTopics(): Set[String] = {
    inReadLock(partitionMetadataLock) {
      cache.keySet.toSet
    }
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    inReadLock(partitionMetadataLock) {
      topics -- cache.keySet
    }
  }

  def getAliveBrokers: Seq[Broker] = {
    inReadLock(partitionMetadataLock) {
      aliveBrokers.values.toBuffer
    }
  }

  private def addOrUpdatePartitionInfo(topic: String,
                                       partitionId: Int,
                                       stateInfo: PartitionStateInfo) {
    inWriteLock(partitionMetadataLock) {
      cache.get(topic) match {
        case Some(infos) => infos.put(partitionId, stateInfo)
        case None => {
          val newInfos: mutable.Map[Int, PartitionStateInfo] = new mutable.HashMap[Int, PartitionStateInfo]
          cache.put(topic, newInfos)
          newInfos.put(partitionId, stateInfo)
        }
      }
    }
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[PartitionStateInfo] = {
    inReadLock(partitionMetadataLock) {
      cache.get(topic) match {
        case Some(partitionInfos) => partitionInfos.get(partitionId)
        case None => None
      }
    }
  }

  def updateCache(updateMetadataRequest: UpdateMetadataRequest) {
    inWriteLock(partitionMetadataLock) {
      aliveBrokers = updateMetadataRequest.aliveBrokers.map(b => (b.id, b)).toMap
      updateMetadataRequest.partitionStateInfos.foreach { case(tp, info) =>
        if (info.leaderIsrAndControllerEpoch.leaderAndIsr.leader == LeaderAndIsr.LeaderDuringDelete) {
          removePartitionInfo(tp.topic, tp.partition)
          stateChangeLogger.trace(("Broker %d deleted partition %s from metadata cache in response to UpdateMetadata request " +
            "sent by controller %d epoch %d with correlation id %d")
            .format(brokerId, tp, updateMetadataRequest.controllerId,
            updateMetadataRequest.controllerEpoch, updateMetadataRequest.correlationId))
        } else {
          addOrUpdatePartitionInfo(tp.topic, tp.partition, info)
          stateChangeLogger.trace(("Broker %d cached leader info %s for partition %s in response to UpdateMetadata request " +
            "sent by controller %d epoch %d with correlation id %d")
            .format(brokerId, info, tp, updateMetadataRequest.controllerId,
            updateMetadataRequest.controllerEpoch, updateMetadataRequest.correlationId))
        }
      }
    }
  }

  def contains(topic: String): Boolean = {
    inReadLock(partitionMetadataLock) {
      cache.contains(topic)
    }
  }

  private def removePartitionInfo(topic: String, partitionId: Int) = {
    cache.get(topic) match {
      case Some(infos) => {
        infos.remove(partitionId)
        if(infos.isEmpty) {
          cache.remove(topic)
        }
        true
      }
      case None => false
    }
  }
}
