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

import kafka.cluster.{EndPoint, Broker}
import kafka.common.TopicAndPartition

import kafka.api._
import kafka.controller.KafkaController.StateChangeLogger
import kafka.controller.LeaderIsrAndControllerEpoch
import org.apache.kafka.common.Node
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{MetadataResponse, UpdateMetadataRequest}
import org.apache.kafka.common.requests.UpdateMetadataRequest.PartitionState
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
  private val cache: mutable.Map[String, mutable.Map[Int, PartitionStateInfo]] =
    new mutable.HashMap[String, mutable.Map[Int, PartitionStateInfo]]()
  private var aliveBrokers: Map[Int, Broker] = Map()
  private val partitionMetadataLock = new ReentrantReadWriteLock()

  this.logIdent = "[Kafka Metadata Cache on broker %d] ".format(brokerId)

  private def getAliveEndpoints(brokers: Iterable[Int], protocol: SecurityProtocol): List[Node] = {
    brokers.flatMap(aliveBrokers.get).toSeq.map(_.getNode(protocol)).toList
  }

  private def getPartitionMetadata(topic: String, protocol: SecurityProtocol): List[MetadataResponse.PartitionMetadata] = {
    cache(topic).map { case (partitionId, partitionState) =>
      val topicPartition = TopicAndPartition(topic, partitionId)

      val leaderAndIsr = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr
      val leader = aliveBrokers.get(leaderAndIsr.leader).map(_.getNode(protocol))

      val replicas = partitionState.allReplicas
      val replicaInfo = getAliveEndpoints(replicas, protocol)

      if (leader.isEmpty) {
        debug("Error while fetching metadata for %s: leader not available".format(topicPartition))
        new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, partitionId, Node.noNode(),
          replicaInfo.asJava, java.util.Collections.emptyList())
      } else {
        val isr = leaderAndIsr.isr
        val isrInfo = getAliveEndpoints(isr, protocol)

        if (replicaInfo.size < replicas.size) {
          debug("Error while fetching metadata for %s: replica information not available for following brokers %s"
            .format(topicPartition, replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(",")))

          new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader.get,
            replicaInfo.asJava, isrInfo.asJava)
        } else if (isrInfo.size < isr.size) {
          debug("Error while fetching metadata for %s: in sync replica information not available for following brokers %s"
            .format(topicPartition, isr.filterNot(isrInfo.map(_.id).contains(_)).mkString(",")))
          new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader.get,
            replicaInfo.asJava, isrInfo.asJava)
        } else {
          new MetadataResponse.PartitionMetadata(Errors.NONE, partitionId, leader.get, replicaInfo.asJava,
            isrInfo.asJava)
        }
      }
    }.toList
  }

  def getTopicMetadata(topics: Set[String], protocol: SecurityProtocol): mutable.Buffer[MetadataResponse.TopicMetadata] = {
    inReadLock(partitionMetadataLock) {
      val isAllTopics = topics.isEmpty
      val topicsRequested = if (isAllTopics) cache.keySet else getExistingTopics(topics)
      topicsRequested.map { topic =>
        val partitionMetadata = getPartitionMetadata(topic, protocol)
        new MetadataResponse.TopicMetadata(Errors.NONE, topic, partitionMetadata.asJava)
      }.toBuffer
    }
  }

  def hasTopicMetadata(topic: String): Boolean = {
    inReadLock(partitionMetadataLock) {
      cache.contains(topic)
    }
  }

  def getAllTopics(): Set[String] = {
    inReadLock(partitionMetadataLock) {
      Set(cache.keys.toSeq: _*)
    }
  }

  def getExistingTopics(topics: Set[String]): Set[String] = {
    inReadLock(partitionMetadataLock) {
      cache.keySet & topics
    }
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    inReadLock(partitionMetadataLock) {
      topics -- cache.keySet
    }
  }

  def getAliveBrokers(): Seq[Broker] = {
    inReadLock(partitionMetadataLock) {
      aliveBrokers.values.toSeq
    }
  }

  def addOrUpdatePartitionInfo(topic: String,
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

  def updateCache(correlationId: Int,
                  updateMetadataRequest: UpdateMetadataRequest,
                  brokerId: Int,
                  stateChangeLogger: StateChangeLogger) {
    inWriteLock(partitionMetadataLock) {
      aliveBrokers = updateMetadataRequest.liveBrokers.asScala.map { broker =>
        val endPoints = broker.endPoints.asScala.map { case (protocol, ep) =>
          (protocol, EndPoint(ep.host, ep.port, protocol))
        }.toMap
        (broker.id, Broker(broker.id, endPoints))
      }.toMap

      updateMetadataRequest.partitionStates.asScala.foreach { case (tp, info) =>
        if (info.leader == LeaderAndIsr.LeaderDuringDelete) {
          removePartitionInfo(tp.topic, tp.partition)
          stateChangeLogger.trace(("Broker %d deleted partition %s from metadata cache in response to UpdateMetadata request " +
            "sent by controller %d epoch %d with correlation id %d")
            .format(brokerId, tp, updateMetadataRequest.controllerId,
              updateMetadataRequest.controllerEpoch, correlationId))
        } else {
          val partitionInfo = partitionStateToPartitionStateInfo(info)
          addOrUpdatePartitionInfo(tp.topic, tp.partition, partitionInfo)
          stateChangeLogger.trace(("Broker %d cached leader info %s for partition %s in response to UpdateMetadata request " +
            "sent by controller %d epoch %d with correlation id %d")
            .format(brokerId, info, tp, updateMetadataRequest.controllerId,
              updateMetadataRequest.controllerEpoch, correlationId))
        }
      }
    }
  }

  private def partitionStateToPartitionStateInfo(partitionState: PartitionState): PartitionStateInfo = {
    val leaderAndIsr = LeaderAndIsr(partitionState.leader, partitionState.leaderEpoch, partitionState.isr.asScala.map(_.toInt).toList, partitionState.zkVersion)
    val leaderInfo = LeaderIsrAndControllerEpoch(leaderAndIsr, partitionState.controllerEpoch)
    PartitionStateInfo(leaderInfo, partitionState.replicas.asScala.map(_.toInt))
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
