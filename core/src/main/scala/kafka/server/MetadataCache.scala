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

import java.util.EnumMap
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.{Seq, Set, mutable}
import scala.collection.JavaConverters._
import kafka.cluster.{Broker, EndPoint}
import kafka.api._
import kafka.common.{BrokerEndPointNotAvailableException, Topic, TopicAndPartition}
import kafka.controller.{KafkaController, LeaderIsrAndControllerEpoch}
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.Node
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.UpdateMetadataRequest.PartitionState
import org.apache.kafka.common.requests.{MetadataResponse, UpdateMetadataRequest}

/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 */
private[server] class MetadataCache(brokerId: Int) extends Logging {
  private val stateChangeLogger = KafkaController.stateChangeLogger
  private val cache = mutable.Map[String, mutable.Map[Int, PartitionStateInfo]]()
  private var controllerId: Option[Int] = None
  private val aliveBrokers = mutable.Map[Int, Broker]()
  private val aliveNodes = mutable.Map[Int, collection.Map[SecurityProtocol, Node]]()
  private val partitionMetadataLock = new ReentrantReadWriteLock()

  this.logIdent = s"[Kafka Metadata Cache on broker $brokerId] "

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def getEndpoints(brokers: Iterable[Int], protocol: SecurityProtocol, filterUnavailableEndpoints: Boolean): Seq[Node] = {
    val result = new mutable.ArrayBuffer[Node](math.min(aliveBrokers.size, brokers.size))
    brokers.foreach { brokerId =>
      val endpoint = getAliveEndpoint(brokerId, protocol) match {
        case None => if (!filterUnavailableEndpoints) Some(new Node(brokerId, "", -1)) else None
        case Some(node) => Some(node)
      }
      endpoint.foreach(result +=)
    }
    result
  }

  private def getAliveEndpoint(brokerId: Int, protocol: SecurityProtocol): Option[Node] =
    aliveNodes.get(brokerId).map { nodeMap =>
      nodeMap.getOrElse(protocol,
        throw new BrokerEndPointNotAvailableException(s"Broker `$brokerId` does not support security protocol `$protocol`"))
    }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  private def getPartitionMetadata(topic: String, protocol: SecurityProtocol, errorUnavailableEndpoints: Boolean): Option[Iterable[MetadataResponse.PartitionMetadata]] = {
    cache.get(topic).map { partitions =>
      partitions.map { case (partitionId, partitionState) =>
        val topicPartition = TopicAndPartition(topic, partitionId)

        val leaderAndIsr = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr
        val maybeLeader = getAliveEndpoint(leaderAndIsr.leader, protocol)

        val replicas = partitionState.allReplicas
        val replicaInfo = getEndpoints(replicas, protocol, errorUnavailableEndpoints)

        maybeLeader match {
          case None =>
            debug(s"Error while fetching metadata for $topicPartition: leader not available")
            new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, partitionId, Node.noNode(),
              replicaInfo.asJava, java.util.Collections.emptyList())

          case Some(leader) =>
            val isr = leaderAndIsr.isr
            val isrInfo = getEndpoints(isr, protocol, errorUnavailableEndpoints)

            if (replicaInfo.size < replicas.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicas.filterNot(replicaInfo.map(_.id).contains).mkString(",")}")

              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfo.asJava, isrInfo.asJava)
            } else if (isrInfo.size < isr.size) {
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isr.filterNot(isrInfo.map(_.id).contains).mkString(",")}")
              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfo.asJava, isrInfo.asJava)
            } else {
              new MetadataResponse.PartitionMetadata(Errors.NONE, partitionId, leader, replicaInfo.asJava,
                isrInfo.asJava)
            }
        }
      }
    }
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  def getTopicMetadata(topics: Set[String], protocol: SecurityProtocol, errorUnavailableEndpoints: Boolean = false): Seq[MetadataResponse.TopicMetadata] = {
    inReadLock(partitionMetadataLock) {
      topics.toSeq.flatMap { topic =>
        getPartitionMetadata(topic, protocol, errorUnavailableEndpoints).map { partitionMetadata =>
          new MetadataResponse.TopicMetadata(Errors.NONE, topic, Topic.isInternal(topic), partitionMetadata.toBuffer.asJava)
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
      val infos = cache.getOrElseUpdate(topic, mutable.Map())
      infos(partitionId) = stateInfo
    }
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[PartitionStateInfo] = {
    inReadLock(partitionMetadataLock) {
      cache.get(topic).flatMap(_.get(partitionId))
    }
  }

  def getControllerId: Option[Int] = controllerId

  def updateCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) {
    inWriteLock(partitionMetadataLock) {
      controllerId = updateMetadataRequest.controllerId match {
          case id if id < 0 => None
          case id => Some(id)
        }
      aliveNodes.clear()
      aliveBrokers.clear()
      updateMetadataRequest.liveBrokers.asScala.foreach { broker =>
        val nodes = new EnumMap[SecurityProtocol, Node](classOf[SecurityProtocol])
        val endPoints = new EnumMap[SecurityProtocol, EndPoint](classOf[SecurityProtocol])
        broker.endPoints.asScala.foreach { case (protocol, ep) =>
          endPoints.put(protocol, EndPoint(ep.host, ep.port, protocol))
          nodes.put(protocol, new Node(broker.id, ep.host, ep.port))
        }
        aliveBrokers(broker.id) = Broker(broker.id, endPoints.asScala, Option(broker.rack))
        aliveNodes(broker.id) = nodes.asScala
      }

      updateMetadataRequest.partitionStates.asScala.foreach { case (tp, info) =>
        val controllerId = updateMetadataRequest.controllerId
        val controllerEpoch = updateMetadataRequest.controllerEpoch
        if (info.leader == LeaderAndIsr.LeaderDuringDelete) {
          removePartitionInfo(tp.topic, tp.partition)
          stateChangeLogger.trace(s"Broker $brokerId deleted partition $tp from metadata cache in response to UpdateMetadata " +
            s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
        } else {
          val partitionInfo = partitionStateToPartitionStateInfo(info)
          addOrUpdatePartitionInfo(tp.topic, tp.partition, partitionInfo)
          stateChangeLogger.trace(s"Broker $brokerId cached leader info $partitionInfo for partition $tp in response to " +
            s"UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
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

  private def removePartitionInfo(topic: String, partitionId: Int): Boolean = {
    cache.get(topic).map { infos =>
      infos.remove(partitionId)
      if (infos.isEmpty) cache.remove(topic)
      true
    }.getOrElse(false)
  }

}
