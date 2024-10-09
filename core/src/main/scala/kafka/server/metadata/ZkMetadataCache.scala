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

package kafka.server.metadata

import java.util
import java.util.{Collections, Optional}
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import scala.collection.{Seq, Set, mutable}
import scala.jdk.CollectionConverters._
import kafka.cluster.{Broker, EndPoint}
import kafka.controller.StateChangeLogger
import kafka.server.{CachedControllerId, KRaftCachedControllerId, MetadataCache, ZkCachedControllerId}
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.admin.BrokerMetadata
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataPartitionState, UpdateMetadataTopicState}
import org.apache.kafka.common.{Cluster, Node, PartitionInfo, TopicPartition, Uuid}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition
import org.apache.kafka.common.message.UpdateMetadataRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractControlRequest, ApiVersionsResponse, MetadataResponse, UpdateMetadataRequest}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.metadata.LeaderAndIsr
import org.apache.kafka.server.BrokerFeatures
import org.apache.kafka.server.common.{FinalizedFeatures, MetadataVersion}

import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.TimeoutException
import scala.math.max

// Raised whenever there was an error in updating the FinalizedFeatureCache with features.
class FeatureCacheUpdateException(message: String) extends RuntimeException(message) {
}

trait ZkFinalizedFeatureCache {
  def waitUntilFeatureEpochOrThrow(minExpectedEpoch: Long, timeoutMs: Long): Unit

  def getFeatureOption: Option[FinalizedFeatures]
}

case class MetadataSnapshot(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                            topicIds: Map[String, Uuid],
                            controllerId: Option[CachedControllerId],
                            aliveBrokers: mutable.LongMap[Broker],
                            aliveNodes: mutable.LongMap[collection.Map[ListenerName, Node]]) {
  val topicNames: Map[Uuid, String] = topicIds.map { case (topicName, topicId) => (topicId, topicName) }
}

object ZkMetadataCache {
  def transformKRaftControllerFullMetadataRequest(
    currentMetadata: MetadataSnapshot,
    requestControllerEpoch: Int,
    requestTopicStates: util.List[UpdateMetadataTopicState],
    handleLogMessage: String => Unit,
  ): util.List[UpdateMetadataTopicState] = {
    val topicIdToNewState = new util.HashMap[Uuid, UpdateMetadataTopicState]()
    requestTopicStates.forEach(state => topicIdToNewState.put(state.topicId(), state))
    val newRequestTopicStates = new util.ArrayList[UpdateMetadataTopicState]()
    currentMetadata.topicNames.foreachEntry((id, name) => {
      try {
        Option(topicIdToNewState.get(id)) match {
          case None =>
            currentMetadata.partitionStates.get(name) match {
              case None => handleLogMessage(s"Error: topic $name appeared in currentMetadata.topicNames, " +
                "but not in currentMetadata.partitionStates.")
              case Some(curPartitionStates) =>
                handleLogMessage(s"Removing topic $name with ID $id from the metadata cache since " +
                  "the full UMR did not include it.")
                newRequestTopicStates.add(createDeletionEntries(name,
                  id,
                  curPartitionStates.values,
                  requestControllerEpoch))
            }
          case Some(newTopicState) =>
            val indexToState = new util.HashMap[Integer, UpdateMetadataPartitionState]
            newTopicState.partitionStates().forEach(part => indexToState.put(part.partitionIndex, part))
            currentMetadata.partitionStates.get(name) match {
              case None => handleLogMessage(s"Error: topic $name appeared in currentMetadata.topicNames, " +
                "but not in currentMetadata.partitionStates.")
              case Some(curPartitionStates) =>
                curPartitionStates.foreach(state => indexToState.remove(state._1.toInt))
                if (!indexToState.isEmpty) {
                  handleLogMessage(s"Removing ${indexToState.size()} partition(s) from topic $name with " +
                    s"ID $id from the metadata cache since the full UMR did not include them.")
                  newRequestTopicStates.add(createDeletionEntries(name,
                    id,
                    indexToState.values().asScala,
                    requestControllerEpoch))
                }
            }
        }
      } catch {
        case e: Exception => handleLogMessage(s"Error: $e")
      }
    })
    if (newRequestTopicStates.isEmpty) {
      // If the output is the same as the input, optimize by just returning the input.
      requestTopicStates
    } else {
      // If the output has some new entries, they should all appear at the beginning. This will
      // ensure that the old stuff is cleared out before the new stuff is added. We will need a
      // new list for this, of course.
      newRequestTopicStates.addAll(requestTopicStates)
      newRequestTopicStates
    }
  }

  def createDeletionEntries(
    topicName: String,
    topicId: Uuid,
    partitions: Iterable[UpdateMetadataPartitionState],
    requestControllerEpoch: Int
  ): UpdateMetadataTopicState = {
    val topicState = new UpdateMetadataRequestData.UpdateMetadataTopicState()
      .setTopicId(topicId)
      .setTopicName(topicName)
      .setPartitionStates(new util.ArrayList())
    partitions.foreach(partition => {
      val lisr = LeaderAndIsr.duringDelete(partition.isr())
      val newPartitionState = new UpdateMetadataPartitionState()
        .setPartitionIndex(partition.partitionIndex())
        .setTopicName(topicName)
        .setLeader(lisr.leader)
        .setLeaderEpoch(lisr.leaderEpoch)
        .setControllerEpoch(requestControllerEpoch)
        .setReplicas(partition.replicas())
        .setZkVersion(lisr.partitionEpoch)
        .setIsr(lisr.isr)
      topicState.partitionStates().add(newPartitionState)
    })
    topicState
  }
}

/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 */
class ZkMetadataCache(
  brokerId: Int,
  metadataVersion: MetadataVersion,
  brokerFeatures: BrokerFeatures,
  zkMigrationEnabled: Boolean = false)
  extends MetadataCache with ZkFinalizedFeatureCache with Logging {

  private val partitionMetadataLock = new ReentrantReadWriteLock()
  //this is the cache state. every MetadataSnapshot instance is immutable, and updates (performed under a lock)
  //replace the value with a completely new one. this means reads (which are not under any lock) need to grab
  //the value of this var (into a val) ONCE and retain that read copy for the duration of their operation.
  //multiple reads of this value risk getting different snapshots.
  @volatile private var metadataSnapshot: MetadataSnapshot = MetadataSnapshot(
    partitionStates = mutable.AnyRefMap.empty,
    topicIds = Map.empty,
    controllerId = None,
    aliveBrokers = mutable.LongMap.empty,
    aliveNodes = mutable.LongMap.empty)

  this.logIdent = s"[MetadataCache brokerId=$brokerId] "
  private val stateChangeLogger = new StateChangeLogger(brokerId, inControllerContext = false, None)

  // Features are updated via ZK notification (see FinalizedFeatureChangeListener)
  @volatile private var _features: Option[FinalizedFeatures] = Option.empty
  private val featureLock = new ReentrantLock()
  private val featureCond = featureLock.newCondition()

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here. Relatedly, `brokers` is
  // `List[Integer]` instead of `List[Int]` to avoid a collection copy.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def maybeFilterAliveReplicas(snapshot: MetadataSnapshot,
                                       brokers: java.util.List[Integer],
                                       listenerName: ListenerName,
                                       filterUnavailableEndpoints: Boolean): java.util.List[Integer] = {
    if (!filterUnavailableEndpoints) {
      brokers
    } else {
      val res = new util.ArrayList[Integer](math.min(snapshot.aliveBrokers.size, brokers.size))
      for (brokerId <- brokers.asScala) {
        if (hasAliveEndpoint(snapshot, brokerId, listenerName))
          res.add(brokerId)
      }
      res
    }
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  // If errorUnavailableListeners=true, return LISTENER_NOT_FOUND if listener is missing on the broker.
  // Otherwise, return LEADER_NOT_AVAILABLE for broker unavailable and missing listener (Metadata response v5 and below).
  private def getPartitionMetadata(snapshot: MetadataSnapshot, topic: String, listenerName: ListenerName, errorUnavailableEndpoints: Boolean,
                                   errorUnavailableListeners: Boolean): Option[Iterable[MetadataResponsePartition]] = {
    snapshot.partitionStates.get(topic).map { partitions =>
      partitions.map { case (partitionId, partitionState) =>
        val topicPartition = new TopicPartition(topic, partitionId.toInt)
        val leaderBrokerId = partitionState.leader
        val leaderEpoch = partitionState.leaderEpoch
        val maybeLeader = getAliveEndpoint(snapshot, leaderBrokerId, listenerName)

        val replicas = partitionState.replicas
        val filteredReplicas = maybeFilterAliveReplicas(snapshot, replicas, listenerName, errorUnavailableEndpoints)

        val isr = partitionState.isr
        val filteredIsr = maybeFilterAliveReplicas(snapshot, isr, listenerName, errorUnavailableEndpoints)

        val offlineReplicas = partitionState.offlineReplicas

        maybeLeader match {
          case None =>
            val error = if (!snapshot.aliveBrokers.contains(leaderBrokerId)) { // we are already holding the read lock
              debug(s"Error while fetching metadata for $topicPartition: leader not available")
              Errors.LEADER_NOT_AVAILABLE
            } else {
              debug(s"Error while fetching metadata for $topicPartition: listener $listenerName " +
                s"not found on leader $leaderBrokerId")
              if (errorUnavailableListeners) Errors.LISTENER_NOT_FOUND else Errors.LEADER_NOT_AVAILABLE
            }

            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partitionId.toInt)
              .setLeaderId(MetadataResponse.NO_LEADER_ID)
              .setLeaderEpoch(leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)

          case Some(_) =>
            val error = if (filteredReplicas.size < replicas.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicas.asScala.filterNot(filteredReplicas.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else if (filteredIsr.size < isr.size) {
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isr.asScala.filterNot(filteredIsr.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else {
              Errors.NONE
            }

            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partitionId.toInt)
              .setLeaderId(maybeLeader.map(_.id()).getOrElse(MetadataResponse.NO_LEADER_ID))
              .setLeaderEpoch(leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)
        }
      }
    }
  }

  /**
   * Check whether a broker is alive and has a registered listener matching the provided name.
   * This method was added to avoid unnecessary allocations in [[maybeFilterAliveReplicas]], which is
   * a hotspot in metadata handling.
   */
  private def hasAliveEndpoint(snapshot: MetadataSnapshot, brokerId: Int, listenerName: ListenerName): Boolean = {
    snapshot.aliveNodes.get(brokerId).exists(_.contains(listenerName))
  }

  /**
   * Get the endpoint matching the provided listener if the broker is alive. Note that listeners can
   * be added dynamically, so a broker with a missing listener could be a transient error.
   *
   * @return None if broker is not alive or if the broker does not have a listener named `listenerName`.
   */
  private def getAliveEndpoint(snapshot: MetadataSnapshot, brokerId: Int, listenerName: ListenerName): Option[Node] = {
    snapshot.aliveNodes.get(brokerId).flatMap(_.get(listenerName))
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  def getTopicMetadata(topics: Set[String],
                       listenerName: ListenerName,
                       errorUnavailableEndpoints: Boolean = false,
                       errorUnavailableListeners: Boolean = false): Seq[MetadataResponseTopic] = {
    val snapshot = metadataSnapshot
    topics.toSeq.flatMap { topic =>
      getPartitionMetadata(snapshot, topic, listenerName, errorUnavailableEndpoints, errorUnavailableListeners).map { partitionMetadata =>
        new MetadataResponseTopic()
          .setErrorCode(Errors.NONE.code)
          .setName(topic)
          .setTopicId(snapshot.topicIds.getOrElse(topic, Uuid.ZERO_UUID))
          .setIsInternal(Topic.isInternal(topic))
          .setPartitions(partitionMetadata.toBuffer.asJava)
      }
    }
  }

  def topicNamesToIds(): util.Map[String, Uuid] = {
    Collections.unmodifiableMap(metadataSnapshot.topicIds.asJava)
  }

  def topicIdsToNames(): util.Map[Uuid, String] = {
    Collections.unmodifiableMap(metadataSnapshot.topicNames.asJava)
  }

  /**
   * This method returns a map from topic names to IDs and a map from topic IDs to names
   */
  def topicIdInfo(): (util.Map[String, Uuid], util.Map[Uuid, String]) = {
    val snapshot = metadataSnapshot
    (Collections.unmodifiableMap(snapshot.topicIds.asJava), Collections.unmodifiableMap(snapshot.topicNames.asJava))
  }

  override def getAllTopics(): Set[String] = {
    getAllTopics(metadataSnapshot)
  }

  override def getTopicPartitions(topicName: String): Set[TopicPartition] = {
    metadataSnapshot.partitionStates.getOrElse(topicName, Map.empty).values.
      map(p => new TopicPartition(topicName, p.partitionIndex())).toSet
  }

  private def getAllTopics(snapshot: MetadataSnapshot): Set[String] = {
    snapshot.partitionStates.keySet
  }

  private def getAllPartitions(snapshot: MetadataSnapshot): Map[TopicPartition, UpdateMetadataPartitionState] = {
    snapshot.partitionStates.flatMap { case (topic, partitionStates) =>
      partitionStates.map { case (partition, state) => (new TopicPartition(topic, partition.toInt), state) }
    }.toMap
  }

  override def hasAliveBroker(brokerId: Int): Boolean = metadataSnapshot.aliveBrokers.contains(brokerId)

  override def getAliveBrokers(): Iterable[BrokerMetadata] = {
    metadataSnapshot.aliveBrokers.values.map(b => new BrokerMetadata(b.id, Optional.ofNullable(b.rack.orNull)))
  }

  override def getAliveBrokerNode(brokerId: Int, listenerName: ListenerName): Option[Node] = {
    val snapshot = metadataSnapshot
    snapshot.aliveBrokers.get(brokerId).flatMap(_.getNode(listenerName))
  }

  override def getAliveBrokerNodes(listenerName: ListenerName): Iterable[Node] = {
    metadataSnapshot.aliveBrokers.values.flatMap(_.getNode(listenerName))
  }

  def getTopicId(topicName: String): Uuid = {
    metadataSnapshot.topicIds.getOrElse(topicName, Uuid.ZERO_UUID)
  }

  def getTopicName(topicId: Uuid): Option[String] = {
    metadataSnapshot.topicNames.get(topicId)
  }

  private def addOrUpdatePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                                       topic: String,
                                       partitionId: Int,
                                       stateInfo: UpdateMetadataPartitionState): Unit = {
    val infos = partitionStates.getOrElseUpdate(topic, mutable.LongMap.empty)
    infos(partitionId) = stateInfo
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[UpdateMetadataPartitionState] = {
    metadataSnapshot.partitionStates.get(topic).flatMap(_.get(partitionId))
  }

  def numPartitions(topic: String): Option[Int] = {
    metadataSnapshot.partitionStates.get(topic).map(_.size)
  }

  // if the leader is not known, return None;
  // if the leader is known and corresponding node is available, return Some(node)
  // if the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
  def getPartitionLeaderEndpoint(topic: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    val snapshot = metadataSnapshot
    snapshot.partitionStates.get(topic).flatMap(_.get(partitionId)) map { partitionInfo =>
      val leaderId = partitionInfo.leader

      snapshot.aliveNodes.get(leaderId) match {
        case Some(nodeMap) =>
          nodeMap.getOrElse(listenerName, Node.noNode)
        case None =>
          Node.noNode
      }
    }
  }

  def getPartitionReplicaEndpoints(tp: TopicPartition, listenerName: ListenerName): Map[Int, Node] = {
    val snapshot = metadataSnapshot
    snapshot.partitionStates.get(tp.topic).flatMap(_.get(tp.partition)).map { partitionInfo =>
      val replicaIds = partitionInfo.replicas
      replicaIds.asScala
        .map(replicaId => replicaId.intValue() -> {
          snapshot.aliveBrokers.get(replicaId.longValue()) match {
            case Some(broker) =>
              broker.getNode(listenerName).getOrElse(Node.noNode())
            case None =>
              Node.noNode()
          }
        }).toMap
        .filter(pair => pair match {
          case (_, node) => !node.isEmpty
        })
    }.getOrElse(Map.empty[Int, Node])
  }

  def getControllerId: Option[CachedControllerId] = {
    metadataSnapshot.controllerId
  }

  def getRandomAliveBrokerId: Option[Int] = {
    val aliveBrokers = metadataSnapshot.aliveBrokers.values.toList
    Some(aliveBrokers(ThreadLocalRandom.current().nextInt(aliveBrokers.size)).id)
  }

  def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    val snapshot = metadataSnapshot
    val nodes = snapshot.aliveNodes.flatMap { case (id, nodesByListener) =>
      nodesByListener.get(listenerName).map { node =>
        id -> node
      }
    }

    def node(id: Integer): Node = {
      nodes.getOrElse(id.toLong, new Node(id, "", -1))
    }

    def controllerId(snapshot: MetadataSnapshot): Option[Node] = {
      snapshot.controllerId.flatMap {
        case ZkCachedControllerId(id) => getAliveBrokerNode(id, listenerName)
        case KRaftCachedControllerId(_) => getRandomAliveBrokerId.flatMap(getAliveBrokerNode(_, listenerName))
      }
    }

    val partitions = getAllPartitions(snapshot)
      .filter { case (_, state) => state.leader != LeaderAndIsr.LEADER_DURING_DELETE }
      .map { case (tp, state) =>
        new PartitionInfo(tp.topic, tp.partition, node(state.leader),
          state.replicas.asScala.map(node).toArray,
          state.isr.asScala.map(node).toArray,
          state.offlineReplicas.asScala.map(node).toArray)
      }
    val unauthorizedTopics = Collections.emptySet[String]
    val internalTopics = getAllTopics(snapshot).filter(Topic.isInternal).asJava
    new Cluster(clusterId, nodes.values.toBuffer.asJava,
      partitions.toBuffer.asJava,
      unauthorizedTopics, internalTopics,
      controllerId(snapshot).orNull)
  }

  // This method returns the deleted TopicPartitions received from UpdateMetadataRequest.
  // Note: if this ZK broker is migrating to KRaft, a singular UMR may sometimes both delete a
  // partition and re-create a new partition with that same name. In that case, it will not appear
  // in the return value of this function.
  def updateMetadata(
    correlationId: Int,
    originalUpdateMetadataRequest: UpdateMetadataRequest
  ): Seq[TopicPartition] = {
    var updateMetadataRequest = originalUpdateMetadataRequest
    inWriteLock(partitionMetadataLock) {
      if (
        updateMetadataRequest.isKRaftController &&
        updateMetadataRequest.updateType() == AbstractControlRequest.Type.FULL
      ) {
        if (updateMetadataRequest.version() < 8) {
          stateChangeLogger.error(s"Received UpdateMetadataRequest with Type=FULL (2), but version of " +
            updateMetadataRequest.version() + ", which should not be possible. Not treating this as a full " +
            "metadata update")
        } else if (!zkMigrationEnabled) {
          stateChangeLogger.error(s"Received UpdateMetadataRequest with Type=FULL (2), but ZK migrations " +
            s"are not enabled on this broker. Not treating this as a full metadata update")
        } else {
          // When handling a UMR from a KRaft controller, we may have to insert some partition
          // deletions at the beginning, to handle the different way topic deletion works in KRaft
          // mode (and also migration mode).
          //
          // After we've done that, we re-create the whole UpdateMetadataRequest object using the
          // updated list of topic info. This ensures that UpdateMetadataRequest.normalize is called
          // on the new, updated topic data. Note that we don't mutate the old request object; it may
          // be used elsewhere.
          val newTopicStates = ZkMetadataCache.transformKRaftControllerFullMetadataRequest(
            metadataSnapshot,
            updateMetadataRequest.controllerEpoch(),
            updateMetadataRequest.topicStates(),
            logMessage => if (logMessage.startsWith("Error")) {
              stateChangeLogger.error(logMessage)
            } else {
              stateChangeLogger.info(logMessage)
            })

          // It would be nice if we could call duplicate() here, but we don't want to copy the
          // old topicStates array. That would be quite costly, and we're not going to use it anyway.
          // Instead, we copy each field that we need.
          val originalRequestData = updateMetadataRequest.data()
          val newData = new UpdateMetadataRequestData().
            setControllerId(originalRequestData.controllerId()).
            setIsKRaftController(originalRequestData.isKRaftController).
            setType(originalRequestData.`type`()).
            setControllerEpoch(originalRequestData.controllerEpoch()).
            setBrokerEpoch(originalRequestData.brokerEpoch()).
            setTopicStates(newTopicStates).
            setLiveBrokers(originalRequestData.liveBrokers())
          updateMetadataRequest = new UpdateMetadataRequest(newData, updateMetadataRequest.version())
        }
      }

      val aliveBrokers = new mutable.LongMap[Broker](metadataSnapshot.aliveBrokers.size)
      val aliveNodes = new mutable.LongMap[collection.Map[ListenerName, Node]](metadataSnapshot.aliveNodes.size)
      val controllerIdOpt: Option[CachedControllerId] = updateMetadataRequest.controllerId match {
        case id if id < 0 => None
        case id =>
          if (updateMetadataRequest.isKRaftController)
            Some(KRaftCachedControllerId(id))
          else
            Some(ZkCachedControllerId(id))
      }

      updateMetadataRequest.liveBrokers.forEach { broker =>
        // `aliveNodes` is a hot path for metadata requests for large clusters, so we use java.util.HashMap which
        // is a bit faster than scala.collection.mutable.HashMap. When we drop support for Scala 2.10, we could
        // move to `AnyRefMap`, which has comparable performance.
        val nodes = new java.util.HashMap[ListenerName, Node]
        val endPoints = new mutable.ArrayBuffer[EndPoint]
        broker.endpoints.forEach { ep =>
          val listenerName = new ListenerName(ep.listener)
          endPoints += new EndPoint(ep.host, ep.port, listenerName, SecurityProtocol.forId(ep.securityProtocol))
          nodes.put(listenerName, new Node(broker.id, ep.host, ep.port, broker.rack()))
        }
        aliveBrokers(broker.id) = Broker(broker.id, endPoints, Option(broker.rack))
        aliveNodes(broker.id) = nodes.asScala
      }
      aliveNodes.get(brokerId).foreach { listenerMap =>
        val listeners = listenerMap.keySet
        if (!aliveNodes.values.forall(_.keySet == listeners))
          error(s"Listeners are not identical across brokers: $aliveNodes")
      }

      val topicIds = mutable.Map.empty[String, Uuid]
      topicIds ++= metadataSnapshot.topicIds
      val (newTopicIds, newZeroIds) = updateMetadataRequest.topicStates().asScala
        .map(topicState => (topicState.topicName(), topicState.topicId()))
        .partition { case (_, topicId) => topicId != Uuid.ZERO_UUID }
      newZeroIds.foreach { case (zeroIdTopic, _) => topicIds.remove(zeroIdTopic) }
      topicIds ++= newTopicIds.toMap

      val deletedPartitions = new java.util.LinkedHashSet[TopicPartition]
      if (!updateMetadataRequest.partitionStates.iterator.hasNext) {
        metadataSnapshot = MetadataSnapshot(metadataSnapshot.partitionStates, topicIds.toMap,
          controllerIdOpt, aliveBrokers, aliveNodes)
      } else {
        //since kafka may do partial metadata updates, we start by copying the previous state
        val partitionStates = new mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]](metadataSnapshot.partitionStates.size)
        metadataSnapshot.partitionStates.foreachEntry { (topic, oldPartitionStates) =>
          val copy = new mutable.LongMap[UpdateMetadataPartitionState](oldPartitionStates.size)
          copy ++= oldPartitionStates
          partitionStates(topic) = copy
        }

        val traceEnabled = stateChangeLogger.isTraceEnabled
        val controllerId = updateMetadataRequest.controllerId
        val controllerEpoch = updateMetadataRequest.controllerEpoch
        val newStates = updateMetadataRequest.partitionStates.asScala
        newStates.foreach { state =>
          // per-partition logging here can be very expensive due going through all partitions in the cluster
          val tp = new TopicPartition(state.topicName, state.partitionIndex)
          if (state.leader == LeaderAndIsr.LEADER_DURING_DELETE) {
            removePartitionInfo(partitionStates, topicIds, tp.topic, tp.partition)
            if (traceEnabled)
              stateChangeLogger.trace(s"Deleted partition $tp from metadata cache in response to UpdateMetadata " +
                s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
            deletedPartitions.add(tp)
          } else {
            addOrUpdatePartitionInfo(partitionStates, tp.topic, tp.partition, state)
            deletedPartitions.remove(tp)
            if (traceEnabled)
              stateChangeLogger.trace(s"Cached leader info $state for partition $tp in response to " +
                s"UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
          }
        }
        val cachedPartitionsCount = newStates.size - deletedPartitions.size
        stateChangeLogger.info(s"Add $cachedPartitionsCount partitions and deleted ${deletedPartitions.size} partitions from metadata cache " +
          s"in response to UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")

        metadataSnapshot = MetadataSnapshot(partitionStates, topicIds.toMap, controllerIdOpt, aliveBrokers, aliveNodes)
      }
      deletedPartitions.asScala.toSeq
    }
  }

  def contains(topic: String): Boolean = {
    metadataSnapshot.partitionStates.contains(topic)
  }

  def contains(tp: TopicPartition): Boolean = getPartitionInfo(tp.topic, tp.partition).isDefined

  private def removePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                                  topicIds: mutable.Map[String, Uuid], topic: String, partitionId: Int): Boolean = {
    partitionStates.get(topic).exists { infos =>
      infos.remove(partitionId)
      if (infos.isEmpty) {
        partitionStates.remove(topic)
        topicIds.remove(topic)
      }
      true
    }
  }

  override def metadataVersion(): MetadataVersion = metadataVersion

  override def features(): FinalizedFeatures = _features match {
    case Some(features) => features
    case None => new FinalizedFeatures(metadataVersion,
      Collections.emptyMap(),
      ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH,
      false)
  }

  /**
   * Updates the cache to the latestFeatures, and updates the existing epoch to latestEpoch.
   * Expects that the latestEpoch should be always greater than the existing epoch (when the
   * existing epoch is defined).
   *
   * @param latestFeatures   the latest finalized features to be set in the cache
   * @param latestEpoch      the latest epoch value to be set in the cache
   *
   * @throws                 FeatureCacheUpdateException if the cache update operation fails
   *                         due to invalid parameters or incompatibilities with the broker's
   *                         supported features. In such a case, the existing cache contents are
   *                         not modified.
   */
  def updateFeaturesOrThrow(latestFeatures: Map[String, Short], latestEpoch: Long): Unit = {
    val latest = new FinalizedFeatures(metadataVersion,
      latestFeatures.map(kv => (kv._1, kv._2.asInstanceOf[java.lang.Short])).asJava,
      latestEpoch,
      false)
    val existing = _features
    if (existing.isDefined && existing.get.finalizedFeaturesEpoch() > latest.finalizedFeaturesEpoch()) {
      val errorMsg = s"FinalizedFeatureCache update failed due to invalid epoch in new $latest." +
        s" The existing cache contents are $existing."
      throw new FeatureCacheUpdateException(errorMsg)
    } else {
      val incompatibleFeatures = brokerFeatures.incompatibleFeatures(
        latest.finalizedFeatures().asScala.map(kv => (kv._1, kv._2.toShort: java.lang.Short)).toMap.asJava)
      if (!incompatibleFeatures.isEmpty) {
        val errorMsg = "FinalizedFeatureCache update failed since feature compatibility" +
          s" checks failed! Supported ${brokerFeatures.supportedFeatures} has incompatibilities" +
          s" with the latest $latest."
        throw new FeatureCacheUpdateException(errorMsg)
      } else {
        val logMsg = s"Updated cache from existing $existing to latest $latest."
        inLock(featureLock) {
          _features = Some(latest)
          featureCond.signalAll()
        }
        info(logMsg)
      }
    }
  }

  /**
   * Clears all existing finalized features and epoch from the cache.
   */
  def clearFeatures(): Unit = {
    inLock(featureLock) {
      _features = None
      featureCond.signalAll()
    }
  }

  /**
   * Waits no more than timeoutMs for the cache's feature epoch to reach an epoch >= minExpectedEpoch.
   *
   * @param minExpectedEpoch   the minimum expected epoch to be reached by the cache
   *                           (should be >= 0)
   * @param timeoutMs          the timeout (in milli seconds)
   *
   * @throws                   TimeoutException if the cache's epoch has not reached at least
   *                           minExpectedEpoch within timeoutMs.
   */
  def waitUntilFeatureEpochOrThrow(minExpectedEpoch: Long, timeoutMs: Long): Unit = {
    if (minExpectedEpoch < 0L) {
      throw new IllegalArgumentException(
        s"Expected minExpectedEpoch >= 0, but $minExpectedEpoch was provided.")
    }

    if (timeoutMs < 0L) {
      throw new IllegalArgumentException(s"Expected timeoutMs >= 0, but $timeoutMs was provided.")
    }
    val waitEndTimeNanos = System.nanoTime() + (timeoutMs * 1000000)
    inLock(featureLock) {
      while (!(_features.isDefined && _features.get.finalizedFeaturesEpoch() >= minExpectedEpoch)) {
        val nowNanos = System.nanoTime()
        if (nowNanos > waitEndTimeNanos) {
          throw new TimeoutException(
            s"Timed out after waiting for ${timeoutMs}ms for required condition to be met." +
              s" Current epoch: ${_features.map(fe => fe.finalizedFeaturesEpoch()).getOrElse("<none>")}.")
        }
        val sleepTimeMs = max(1L, (waitEndTimeNanos - nowNanos) / 1000000)
        featureCond.await(sleepTimeMs, TimeUnit.MILLISECONDS)
      }
    }
  }

  override def getFeatureOption: Option[FinalizedFeatures] = _features
}
