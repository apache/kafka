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

import java.util.{Collections, Optional}
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.{Seq, Set, mutable}
import scala.collection.JavaConverters._
import kafka.cluster.{Broker, EndPoint}
import kafka.api._
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

  private val partitionMetadataLock = new ReentrantReadWriteLock()
  //this is the cache state. every MetadataSnapshot instance is immutable, and updates (performed under a lock)
  //replace the value with a completely new one. this means reads (which are not under any lock) need to grab
  //the value of this var (into a val) ONCE and retain that read copy for the duration of their operation.
  //multiple reads of this value risk getting different snapshots.
  @volatile private var metadataSnapshot: MetadataSnapshot = MetadataSnapshot(partitionStates = mutable.AnyRefMap.empty,
    controllerId = None, aliveBrokers = mutable.LongMap.empty, aliveNodes = mutable.LongMap.empty)

  this.logIdent = s"[MetadataCache brokerId=$brokerId] "
  private val stateChangeLogger = new StateChangeLogger(brokerId, inControllerContext = false, None)

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here. Relatedly, `brokers` is
  // `Iterable[Integer]` instead of `Iterable[Int]` to avoid a collection copy.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def getEndpoints(snapshot: MetadataSnapshot, brokers: Iterable[java.lang.Integer], listenerName: ListenerName, filterUnavailableEndpoints: Boolean): Seq[Node] = {
    val result = new mutable.ArrayBuffer[Node](math.min(snapshot.aliveBrokers.size, brokers.size))
    brokers.foreach { brokerId =>
      val endpoint = getAliveEndpoint(snapshot, brokerId, listenerName) match {
        case None => if (!filterUnavailableEndpoints) Some(new Node(brokerId, "", -1)) else None
        case Some(node) => Some(node)
      }
      endpoint.foreach(result +=)
    }
    result
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  // If errorUnavailableListeners=true, return LISTENER_NOT_FOUND if listener is missing on the broker.
  // Otherwise, return LEADER_NOT_AVAILABLE for broker unavailable and missing listener (Metadata response v5 and below).
  private def getPartitionMetadata(snapshot: MetadataSnapshot, topic: String, listenerName: ListenerName, errorUnavailableEndpoints: Boolean,
                                   errorUnavailableListeners: Boolean): Option[Iterable[MetadataResponse.PartitionMetadata]] = {
    snapshot.partitionStates.get(topic).map { partitions =>
      partitions.map { case (partitionId, partitionState) =>
        val topicPartition = new TopicPartition(topic, partitionId.toInt)
        val leaderBrokerId = partitionState.basePartitionState.leader
        val leaderEpoch = partitionState.basePartitionState.leaderEpoch
        val maybeLeader = getAliveEndpoint(snapshot, leaderBrokerId, listenerName)
        val replicas = partitionState.basePartitionState.replicas.asScala
        val replicaInfo = getEndpoints(snapshot, replicas, listenerName, errorUnavailableEndpoints)
        val offlineReplicaInfo = getEndpoints(snapshot, partitionState.offlineReplicas.asScala, listenerName, errorUnavailableEndpoints)

        maybeLeader match {
          case None =>
            val error = if (!snapshot.aliveBrokers.contains(brokerId)) { // we are already holding the read lock
              debug(s"Error while fetching metadata for $topicPartition: leader not available")
              Errors.LEADER_NOT_AVAILABLE
            } else {
              debug(s"Error while fetching metadata for $topicPartition: listener $listenerName not found on leader $leaderBrokerId")
              if (errorUnavailableListeners) Errors.LISTENER_NOT_FOUND else Errors.LEADER_NOT_AVAILABLE
            }
            new MetadataResponse.PartitionMetadata(error, partitionId.toInt, Node.noNode(),
              Optional.empty(), replicaInfo.asJava, java.util.Collections.emptyList(),
              offlineReplicaInfo.asJava)

          case Some(leader) =>
            val isr = partitionState.basePartitionState.isr.asScala
            val isrInfo = getEndpoints(snapshot, isr, listenerName, errorUnavailableEndpoints)

            if (replicaInfo.size < replicas.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicas.filterNot(replicaInfo.map(_.id).contains).mkString(",")}")

              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId.toInt, leader,
                Optional.empty(), replicaInfo.asJava, isrInfo.asJava, offlineReplicaInfo.asJava)
            } else if (isrInfo.size < isr.size) {
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isr.filterNot(isrInfo.map(_.id).contains).mkString(",")}")
              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId.toInt, leader,
                Optional.empty(), replicaInfo.asJava, isrInfo.asJava, offlineReplicaInfo.asJava)
            } else {
              new MetadataResponse.PartitionMetadata(Errors.NONE, partitionId.toInt, leader, Optional.of(leaderEpoch),
                replicaInfo.asJava, isrInfo.asJava, offlineReplicaInfo.asJava)
            }
        }
      }
    }
  }

  private def getAliveEndpoint(snapshot: MetadataSnapshot, brokerId: Int, listenerName: ListenerName): Option[Node] =
    // Returns None if broker is not alive or if the broker does not have a listener named `listenerName`.
    // Since listeners can be added dynamically, a broker with a missing listener could be a transient error.
    snapshot.aliveNodes.get(brokerId).flatMap(_.get(listenerName))

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  def getTopicMetadata(topics: Set[String], listenerName: ListenerName, errorUnavailableEndpoints: Boolean = false,
                       errorUnavailableListeners: Boolean = false): Seq[MetadataResponse.TopicMetadata] = {
    val snapshot = metadataSnapshot
    topics.toSeq.flatMap { topic =>
      getPartitionMetadata(snapshot, topic, listenerName, errorUnavailableEndpoints, errorUnavailableListeners).map { partitionMetadata =>
        new MetadataResponse.TopicMetadata(Errors.NONE, topic, Topic.isInternal(topic), partitionMetadata.toBuffer.asJava)
      }
    }
  }

  def getAllTopics(): Set[String] = {
    getAllTopics(metadataSnapshot)
  }

  def getAllPartitions(): Set[TopicPartition] = {
    metadataSnapshot.partitionStates.flatMap { case (topicName, partitionsAndStates) =>
      partitionsAndStates.keys.map(partitionId => new TopicPartition(topicName, partitionId.toInt))
    }.toSet
  }

  private def getAllTopics(snapshot: MetadataSnapshot): Set[String] = {
    snapshot.partitionStates.keySet
  }

  private def getAllPartitions(snapshot: MetadataSnapshot): Map[TopicPartition, UpdateMetadataRequest.PartitionState] = {
    snapshot.partitionStates.flatMap { case (topic, partitionStates) =>
      partitionStates.map { case (partition, state ) => (new TopicPartition(topic, partition.toInt), state) }
    }.toMap
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    topics -- metadataSnapshot.partitionStates.keySet
  }

  def isBrokerAlive(brokerId: Int): Boolean = {
    metadataSnapshot.aliveBrokers.contains(brokerId)
  }

  def getAliveBrokers: Seq[Broker] = {
    metadataSnapshot.aliveBrokers.values.toBuffer
  }

  private def addOrUpdatePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataRequest.PartitionState]],
                                       topic: String,
                                       partitionId: Int,
                                       stateInfo: UpdateMetadataRequest.PartitionState) {
    val infos = partitionStates.getOrElseUpdate(topic, mutable.LongMap())
    infos(partitionId) = stateInfo
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[UpdateMetadataRequest.PartitionState] = {
    metadataSnapshot.partitionStates.get(topic).flatMap(_.get(partitionId))
  }

  // if the leader is not known, return None;
  // if the leader is known and corresponding node is available, return Some(node)
  // if the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
  def getPartitionLeaderEndpoint(topic: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    val snapshot = metadataSnapshot
    snapshot.partitionStates.get(topic).flatMap(_.get(partitionId)) map { partitionInfo =>
      val leaderId = partitionInfo.basePartitionState.leader

      snapshot.aliveNodes.get(leaderId) match {
        case Some(nodeMap) =>
          nodeMap.getOrElse(listenerName, Node.noNode)
        case None =>
          Node.noNode
      }
    }
  }

  def getControllerId: Option[Int] = metadataSnapshot.controllerId

  def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    val snapshot = metadataSnapshot
    val nodes = snapshot.aliveNodes.map { case (id, nodes) => (id, nodes.get(listenerName).orNull) }
    def node(id: Integer): Node = nodes.get(id.toLong).orNull
    val partitions = getAllPartitions(snapshot)
      .filter { case (_, state) => state.basePartitionState.leader != LeaderAndIsr.LeaderDuringDelete }
      .map { case (tp, state) =>
        new PartitionInfo(tp.topic, tp.partition, node(state.basePartitionState.leader),
          state.basePartitionState.replicas.asScala.map(node).toArray,
          state.basePartitionState.isr.asScala.map(node).toArray,
          state.offlineReplicas.asScala.map(node).toArray)
      }
    val unauthorizedTopics = Collections.emptySet[String]
    val internalTopics = getAllTopics(snapshot).filter(Topic.isInternal).asJava
    new Cluster(clusterId, nodes.values.filter(_ != null).toList.asJava,
      partitions.toList.asJava,
      unauthorizedTopics, internalTopics,
      snapshot.controllerId.map(id => node(id)).orNull)
  }

  // This method returns the deleted TopicPartitions received from UpdateMetadataRequest
  def updateMetadata(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest): Seq[TopicPartition] = {
    inWriteLock(partitionMetadataLock) {

      val aliveBrokers = new mutable.LongMap[Broker](metadataSnapshot.aliveBrokers.size)
      val aliveNodes = new mutable.LongMap[collection.Map[ListenerName, Node]](metadataSnapshot.aliveNodes.size)
      val controllerId = updateMetadataRequest.controllerId match {
          case id if id < 0 => None
          case id => Some(id)
        }

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
      if (updateMetadataRequest.partitionStates().isEmpty) {
        metadataSnapshot = MetadataSnapshot(metadataSnapshot.partitionStates, controllerId, aliveBrokers, aliveNodes)
      } else {
        //since kafka may do partial metadata updates, we start by copying the previous state
        val partitionStates = new mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataRequest.PartitionState]](metadataSnapshot.partitionStates.size)
        metadataSnapshot.partitionStates.foreach { case (topic, oldPartitionStates) =>
          val copy = new mutable.LongMap[UpdateMetadataRequest.PartitionState](oldPartitionStates.size)
          copy ++= oldPartitionStates
          partitionStates += (topic -> copy)
        }
        updateMetadataRequest.partitionStates.asScala.foreach { case (tp, info) =>
          val controllerId = updateMetadataRequest.controllerId
          val controllerEpoch = updateMetadataRequest.controllerEpoch
          if (info.basePartitionState.leader == LeaderAndIsr.LeaderDuringDelete) {
            removePartitionInfo(partitionStates, tp.topic, tp.partition)
            stateChangeLogger.trace(s"Deleted partition $tp from metadata cache in response to UpdateMetadata " +
              s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
            deletedPartitions += tp
          } else {
            addOrUpdatePartitionInfo(partitionStates, tp.topic, tp.partition, info)
            stateChangeLogger.trace(s"Cached leader info $info for partition $tp in response to " +
              s"UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
          }
        }
        metadataSnapshot = MetadataSnapshot(partitionStates, controllerId, aliveBrokers, aliveNodes)
      }
      deletedPartitions
    }
  }

  def contains(topic: String): Boolean = {
    metadataSnapshot.partitionStates.contains(topic)
  }

  def contains(tp: TopicPartition): Boolean = getPartitionInfo(tp.topic, tp.partition).isDefined

  private def removePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataRequest.PartitionState]], topic: String, partitionId: Int): Boolean = {
    partitionStates.get(topic).exists { infos =>
      infos.remove(partitionId)
      if (infos.isEmpty) partitionStates.remove(topic)
      true
    }
  }

  case class MetadataSnapshot(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataRequest.PartitionState]],
                              controllerId: Option[Int],
                              aliveBrokers: mutable.LongMap[Broker],
                              aliveNodes: mutable.LongMap[collection.Map[ListenerName, Node]])

}
