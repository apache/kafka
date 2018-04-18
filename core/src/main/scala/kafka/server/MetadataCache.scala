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

import java.util.Collections
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.{Seq, Set, mutable}
import scala.collection.JavaConverters._
import kafka.cluster.{Broker, EndPoint}
import kafka.api._
import kafka.common.TopicAndPartition
import kafka.controller.StateChangeLogger
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.{Cluster, Node, PartitionInfo, TopicPartition}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataResponse, UpdateMetadataRequest}

/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 */
class MetadataCache(brokerId: Int) extends Logging {

  private val cache = mutable.Map[String, mutable.Map[Int, UpdateMetadataRequest.PartitionState]]()
  @volatile private var controllerId: Option[Int] = None
  private val aliveBrokers = mutable.Map[Int, Broker]()
  private val aliveNodes = mutable.Map[Int, collection.Map[ListenerName, Node]]()
  private val partitionMetadataLock = new ReentrantReadWriteLock()

  this.logIdent = s"[MetadataCache brokerId=$brokerId] "
  private val stateChangeLogger = new StateChangeLogger(brokerId, inControllerContext = false, None)

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def getEndpoints(brokers: Iterable[Int], listenerName: ListenerName, filterUnavailableEndpoints: Boolean): Seq[Node] = {
    val result = new mutable.ArrayBuffer[Node](math.min(aliveBrokers.size, brokers.size))
    brokers.foreach { brokerId =>
      val endpoint = getAliveEndpoint(brokerId, listenerName) match {
        case None => if (!filterUnavailableEndpoints) Some(new Node(brokerId, "", -1)) else None
        case Some(node) => Some(node)
      }
      endpoint.foreach(result +=)
    }
    result
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  private def getPartitionMetadata(topic: String, listenerName: ListenerName, errorUnavailableEndpoints: Boolean): Option[Iterable[MetadataResponse.PartitionMetadata]] = {
    cache.get(topic).map { partitions =>
      partitions.map { case (partitionId, partitionState) =>
        val topicPartition = TopicAndPartition(topic, partitionId)
        val maybeLeader = getAliveEndpoint(partitionState.basePartitionState.leader, listenerName)
        val replicas = partitionState.basePartitionState.replicas.asScala.map(_.toInt)
        val replicaInfo = getEndpoints(replicas, listenerName, errorUnavailableEndpoints)
        val offlineReplicaInfo = getEndpoints(partitionState.offlineReplicas.asScala.map(_.toInt), listenerName, errorUnavailableEndpoints)

        maybeLeader match {
          case None =>
            debug(s"Error while fetching metadata for $topicPartition: leader not available")
            new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, partitionId, Node.noNode(),
              replicaInfo.asJava, java.util.Collections.emptyList(), offlineReplicaInfo.asJava)

          case Some(leader) =>
            val isr = partitionState.basePartitionState.isr.asScala.map(_.toInt)
            val isrInfo = getEndpoints(isr, listenerName, errorUnavailableEndpoints)

            if (replicaInfo.size < replicas.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicas.filterNot(replicaInfo.map(_.id).contains).mkString(",")}")

              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfo.asJava, isrInfo.asJava, offlineReplicaInfo.asJava)
            } else if (isrInfo.size < isr.size) {
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isr.filterNot(isrInfo.map(_.id).contains).mkString(",")}")
              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfo.asJava, isrInfo.asJava, offlineReplicaInfo.asJava)
            } else {
              new MetadataResponse.PartitionMetadata(Errors.NONE, partitionId, leader, replicaInfo.asJava,
                isrInfo.asJava, offlineReplicaInfo.asJava)
            }
        }
      }
    }
  }

  private def getAliveEndpoint(brokerId: Int, listenerName: ListenerName): Option[Node] =
    inReadLock(partitionMetadataLock) {
      // Returns None if broker is not alive or if the broker does not have a listener named `listenerName`.
      // Since listeners can be added dynamically, a broker with a missing listener could be a transient error.
      aliveNodes.get(brokerId).flatMap(_.get(listenerName))
    }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  def getTopicMetadata(topics: Set[String], listenerName: ListenerName, errorUnavailableEndpoints: Boolean = false): Seq[MetadataResponse.TopicMetadata] = {
    inReadLock(partitionMetadataLock) {
      topics.toSeq.flatMap { topic =>
        getPartitionMetadata(topic, listenerName, errorUnavailableEndpoints).map { partitionMetadata =>
          new MetadataResponse.TopicMetadata(Errors.NONE, topic, Topic.isInternal(topic), partitionMetadata.toBuffer.asJava)
        }
      }
    }
  }

  def getAllTopics(): Set[String] = {
    inReadLock(partitionMetadataLock) {
      cache.keySet.toSet
    }
  }

  def getAllPartitions(): Map[TopicPartition, UpdateMetadataRequest.PartitionState] = {
    inReadLock(partitionMetadataLock) {
      cache.flatMap { case (topic, partitionStates) =>
        partitionStates.map { case (partition, state ) => (new TopicPartition(topic, partition), state) }
      }.toMap
    }
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    inReadLock(partitionMetadataLock) {
      topics -- cache.keySet
    }
  }

  def isBrokerAlive(brokerId: Int): Boolean = {
    inReadLock(partitionMetadataLock) {
      aliveBrokers.contains(brokerId)
    }
  }

  def getAliveBrokers: Seq[Broker] = {
    inReadLock(partitionMetadataLock) {
      aliveBrokers.values.toBuffer
    }
  }

  private def addOrUpdatePartitionInfo(topic: String,
                                       partitionId: Int,
                                       stateInfo: UpdateMetadataRequest.PartitionState) {
    inWriteLock(partitionMetadataLock) {
      val infos = cache.getOrElseUpdate(topic, mutable.Map())
      infos(partitionId) = stateInfo
    }
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[UpdateMetadataRequest.PartitionState] = {
    inReadLock(partitionMetadataLock) {
      cache.get(topic).flatMap(_.get(partitionId))
    }
  }

  // if the leader is not known, return None;
  // if the leader is known and corresponding node is available, return Some(node)
  // if the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
  def getPartitionLeaderEndpoint(topic: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    inReadLock(partitionMetadataLock) {
      cache.get(topic).flatMap(_.get(partitionId)) map { partitionInfo =>
        val leaderId = partitionInfo.basePartitionState.leader

        aliveNodes.get(leaderId) match {
          case Some(nodeMap) =>
            nodeMap.getOrElse(listenerName, Node.noNode)
          case None =>
            Node.noNode
        }
      }
    }
  }

  def getControllerId: Option[Int] = controllerId

  def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    inReadLock(partitionMetadataLock) {
      val nodes = aliveNodes.map { case (id, nodes) => (id, nodes.get(listenerName).orNull) }
      def node(id: Integer): Node = nodes.get(id).orNull
      val partitions = getAllPartitions()
        .filter { case (_, state) => state.basePartitionState.leader != LeaderAndIsr.LeaderDuringDelete }
        .map { case (tp, state) =>
          new PartitionInfo(tp.topic, tp.partition, node(state.basePartitionState.leader),
            state.basePartitionState.replicas.asScala.map(node).toArray,
            state.basePartitionState.isr.asScala.map(node).toArray,
            state.offlineReplicas.asScala.map(node).toArray)
        }
      val unauthorizedTopics = Collections.emptySet[String]
      val internalTopics = getAllTopics().filter(Topic.isInternal).asJava
      new Cluster(clusterId, nodes.values.filter(_ != null).toList.asJava,
        partitions.toList.asJava,
        unauthorizedTopics, internalTopics,
        getControllerId.map(id => node(id)).orNull)
    }
  }

  // This method returns the deleted TopicPartitions received from UpdateMetadataRequest
  def updateCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest): Seq[TopicPartition] = {
    inWriteLock(partitionMetadataLock) {
      controllerId = updateMetadataRequest.controllerId match {
          case id if id < 0 => None
          case id => Some(id)
        }
      aliveNodes.clear()
      aliveBrokers.clear()
      updateMetadataRequest.liveBrokers.asScala.foreach { broker =>
        // `aliveNodes` is a hot path for metadata requests for large clusters, so we use java.util.HashMap which
        // is a bit faster than scala.collection.mutable.HashMap. When we drop support for Scala 2.10, we could
        // move to `AnyRefMap`, which has comparable performance.
        val nodes = new java.util.HashMap[ListenerName, Node]
        val endPoints = new mutable.ArrayBuffer[EndPoint]
        broker.endPoints.asScala.foreach { ep =>
          endPoints += EndPoint(ep.host, ep.port, ep.listenerName, ep.securityProtocol)
          nodes.put(ep.listenerName, new Node(broker.id, ep.host, ep.port))
        }
        aliveBrokers(broker.id) = Broker(broker.id, endPoints, Option(broker.rack))
        aliveNodes(broker.id) = nodes.asScala
      }
      aliveNodes.get(brokerId).foreach { listenerMap =>
        val listeners = listenerMap.keySet
        if (!aliveNodes.values.forall(_.keySet == listeners))
          error(s"Listeners are not identical across brokers: $aliveNodes")
      }

      val deletedPartitions = new mutable.ArrayBuffer[TopicPartition]
      updateMetadataRequest.partitionStates.asScala.foreach { case (tp, info) =>
        val controllerId = updateMetadataRequest.controllerId
        val controllerEpoch = updateMetadataRequest.controllerEpoch
        if (info.basePartitionState.leader == LeaderAndIsr.LeaderDuringDelete) {
          removePartitionInfo(tp.topic, tp.partition)
          stateChangeLogger.trace(s"Deleted partition $tp from metadata cache in response to UpdateMetadata " +
            s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
          deletedPartitions += tp
        } else {
          addOrUpdatePartitionInfo(tp.topic, tp.partition, info)
          stateChangeLogger.trace(s"Cached leader info $info for partition $tp in response to " +
            s"UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
        }
      }
      deletedPartitions
    }
  }

  def contains(topic: String): Boolean = {
    inReadLock(partitionMetadataLock) {
      cache.contains(topic)
    }
  }

  def contains(tp: TopicPartition): Boolean = getPartitionInfo(tp.topic, tp.partition).isDefined

  private def removePartitionInfo(topic: String, partitionId: Int): Boolean = {
    cache.get(topic).exists { infos =>
      infos.remove(partitionId)
      if (infos.isEmpty) cache.remove(topic)
      true
    }
  }

}
