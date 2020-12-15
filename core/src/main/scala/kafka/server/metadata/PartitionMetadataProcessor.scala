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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.{Collections, Properties}

import kafka.api.LeaderAndIsr
import kafka.cluster.{Broker, EndPoint}
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.server.{ApisUtils, ConfigHandler, KafkaConfig, MetadataCache, MetadataSnapshot, QuotaFactory, ReplicaManager}
import kafka.utils.Implicits.MapExtensionMethods
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.metadata.{ConfigRecord, FenceBrokerRecord, IsrChangeRecord, PartitionRecord, RegisterBrokerRecord, RemoveTopicRecord, TopicRecord, UnfenceBrokerRecord}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.LeaderAndIsrRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.{Node, TopicPartition, Uuid}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._

/**
 * Updates the metadata cache and replica manager state based on metadata log messages and out-of-band events
 * originating from the local broker's metadata heartbeat.
 *
 * The changes due to various events are as follows:
 *
 * Out-of-band appearance of "register local broker" message from broker heartbeat:
 *     Set the local broker's epoch in the metadata.  We do not yet know the local broker's endpoints/features
 *     (we won't see those until we see the corresponding RegisterBrokerRecord in the metadata log).
 *
 * Out-of-band appearance of "fence local broker" message from broker heartbeat:
 *     This is a no-op for the broker because we perform one-sided (controller-only) fencing.
 *
 * Appearance of RegisterBrokerRecord in the metadata log:
 *     Update metadata about the indicated broker and its endpoints, and mark the broker as being alive.
 *
 * Appearance of FenceBrokerRecord in the metadata log:
 *     Mark the indicated broker as not being alive.
 *
 * Appearance of TopicRecord in the metadata log with Delete=false
 *     Create the topic in the metadata with no partitions (yet)
 *
 * Appearance of ConfigRecord in the metadata log
 *     Update configs as indicated
 *
 * Appearance of TopicRecord in the metadata log with Delete=true
 *     Remove all topic-partitions for the indicated topic from the metadata.
 *     Stop replica fetchers as required with deletion of log directories.
 *     The only remaining thing in the metadata will be the Topic UUID.
 *
 * Appearance of PartitionRecord in the metadata log:
 *     Update metadata about the indicated topic-partition and its leader/replicas.
 *     Become leader or follower or stop replica as required.
 *
 * Appearance of UnfenceBrokerRecord in the metadata log:
 *     Mark the indicated broker as being alive.
 *
 * Appearance of IsrChangeRecord in the metadata log:
 *     Update metadata about the indicated topic-partition and its leader/ISRs.
 *     React as though we received a LeaderAndIsr message if the local broker is the leader or in the ISR.
 *
 * Appearance of RemoveTopicRecord in the metadata log:
 *     Remove the Topic UUID from the metadata.
 *
 * @param kafkaConfig the kafkaConfig
 * @param clusterId the Kafka Cluster Id
 * @param metadataCache the metadata cache
 * @param groupCoordinator the group coordinator to be notified of topic deletions
 * @param quotaManagers the quota managers to be given an opportunity to update quota metric configs
 *                      when partition counts change
 * @param replicaManager the replica manager
 * @param txnCoordinator the transaction coordinator
 * @param configHandlers the config handlers
 */
class PartitionMetadataProcessor(kafkaConfig: KafkaConfig,
                                 clusterId: String,
                                 metadataCache: MetadataCache,
                                 groupCoordinator: GroupCoordinator,
                                 quotaManagers: QuotaFactory.QuotaManagers,
                                 replicaManager: ReplicaManager,
                                 txnCoordinator: TransactionCoordinator,
                                 configHandlers: Map[ConfigResource.Type, ConfigHandler]) extends BrokerMetadataProcessor
  with ConfigRepository with Logging {
  // used only for onLeadershipChange() (TODO: factor out?)
  private val apisUtils = new ApisUtils(new LogContext(""),
    null, None, null, null, Some(groupCoordinator), Some(txnCoordinator))

  // visible for testing
  private[metadata] var brokerEpoch: Long = -1

  // Define a concurrent map for configs.
  private val configMap: ConcurrentMap[ConfigResource, Map[String, String]] = new ConcurrentHashMap[ConfigResource, Map[String, String]]

  override def configProperties(configResource: ConfigResource): Properties = {
    if (configResource.`type`() != ConfigResource.Type.TOPIC && configResource.`type`() != ConfigResource.Type.BROKER) {
      throw new IllegalArgumentException(s"Unknown config resource type: ${configResource.`type`()}")
    }
    val retval = new Properties()
    configMap.getOrDefault(configResource, Map.empty).foreach { case (key, value) => retval.put(key, value) }
    retval
  }

  // visible for testing
  private[metadata] object MetadataMgr {
    def apply() =
      new MetadataMgr(0, 0, 0)
    def apply(numBrokersAdding : Int, numTopicsAdding: Int, numBrokersFencing: Int) =
      new MetadataMgr(numBrokersAdding, numTopicsAdding, numBrokersFencing)
  }

  // visible for testing
  private[metadata] class MetadataMgr(numBrokersAdding : Int, numTopicsAdding: Int, numBrokersFencing: Int) {
    // support coalescing multiple config changes together and writing them on-demand
    val configResourceChanges: mutable.Map[ConfigResource, mutable.Map[String, String]] = mutable.Map.empty

    // must call writeConfigs() to apply these changes
    def addConfigResourceChange(configResource: ConfigResource, name: String, value: String): Unit = {
      if (configResource.`type`() == ConfigResource.Type.BROKER && !configResource.isDefault) {
        configResource.name().toInt // check for valid broker ID and throw NumberFormatException if not
      }
      val changesForResource = configResourceChanges.getOrElse(configResource, mutable.Map.empty)
      val firstChangeForResource = changesForResource.isEmpty
      changesForResource(name) = value
      if (firstChangeForResource) {
        configResourceChanges.put(configResource, changesForResource)
      }
    }

    // write all the coalesced config changes and return the affected resources (if any)
    def writeConfigChangesIfNecessary(): Seq[ConfigResource] = {
      if (configResourceChanges.isEmpty) {
        List.empty
      } else {
        configResourceChanges.foreach { case (configResource, changedConfigsForResource) =>
          val oldConfigsForResource: Map[String, String] = configMap.getOrDefault(configResource, Map.empty)
          val newConfigsForResource: Map[String, String] = oldConfigsForResource ++ changedConfigsForResource
          // be sure to remove anything overwritten with a null value -- those are deletes
          configMap.put(configResource, newConfigsForResource.filter(tuple => tuple._2 != null))
        }
        val retval = configResourceChanges.keySet.toSeq
        configResourceChanges.clear()
        retval
      }
    }

    // define functions to retrieve and copy stuff on-demand
    private var metadataSnapshot: Option[MetadataSnapshot] = None
    def getMetadataSnapshot(): MetadataSnapshot = {
      metadataSnapshot match {
        case Some(snapshot) => snapshot
        case None =>
          metadataSnapshot = Some(metadataCache.readState()) // so we don't copy it again
          metadataSnapshot.get
      }
    }

    private var copiedAliveBrokers: Option[mutable.LongMap[Broker]] = None
    private var copiedAliveNodes: Option[mutable.LongMap[collection.Map[ListenerName, Node]]] = None
    def hasBrokerChanges(): Boolean = copiedAliveBrokers.isDefined

    def copyAliveBrokersAndNodes(): Unit = {
      val metadataSnapshot = getMetadataSnapshot()
      val maxPossibleCapacity = metadataSnapshot.aliveBrokers.size + numBrokersAdding
      val nextAliveBrokers = new mutable.LongMap[Broker](maxPossibleCapacity)
      val nextAliveNodes = new mutable.LongMap[collection.Map[ListenerName, Node]](maxPossibleCapacity)
      for ((existingBrokerId, existingBroker) <- metadataSnapshot.aliveBrokers) {
        nextAliveBrokers(existingBrokerId) = existingBroker
      }
      for ((existingBrokerId, existingListenerNameToNodeMap) <- metadataSnapshot.aliveNodes) {
        nextAliveNodes(existingBrokerId) = existingListenerNameToNodeMap
      }
      copiedAliveBrokers = Some(nextAliveBrokers)  // so we don't copy it again
      copiedAliveNodes = Some(nextAliveNodes)  // so we don't copy it again
    }

    // get the alive brokers, copying first if necessary
    def getCopiedAliveBrokers(): mutable.LongMap[Broker] = {
      copiedAliveBrokers match {
        case Some(map) => map
        case None =>
          copyAliveBrokersAndNodes()  // so we don't copy it again
          copiedAliveBrokers.get
      }
    }
    // get the current alive brokers, either the copy if we have already copied or the original if not
    def getCurrentAliveBrokers(): mutable.LongMap[Broker] = {
      copiedAliveBrokers match {
        case Some(map) => map
        case None => getMetadataSnapshot().aliveBrokers
      }
    }
    // get the alive nodes, copying first if necessary
    def getCopiedAliveNodes(): mutable.LongMap[collection.Map[ListenerName, Node]] = {
      copiedAliveNodes match {
        case Some(map) => map
        case None =>
          copyAliveBrokersAndNodes()  // so we don't copy it again
          copiedAliveNodes.get
      }
    }
    // get the current alive node, either the copy if we have already copied or the original if not
    def getCurrentAliveNodes(): mutable.LongMap[collection.Map[ListenerName, Node]] = {
      copiedAliveNodes match {
        case Some(map) => map
        case None => getMetadataSnapshot().aliveNodes
      }
    }

    private var copiedTopicIdMap: Option[util.Map[Uuid, String]] = None
    def hasTopicIdMapChanges(): Boolean = copiedTopicIdMap.isDefined

    // get the topicId-to-name map, copying first if necessary
    def getCopiedTopicIdMap(): util.Map[Uuid, String] = {
      copiedTopicIdMap match {
        case Some(map) => map
        case None =>
          val metadataSnapshot = getMetadataSnapshot()
          val maxPossibleCapacity = metadataSnapshot.topicIdMap.size() + numTopicsAdding
          val copy = new util.HashMap[Uuid, String](maxPossibleCapacity)
          copy.putAll(metadataSnapshot.topicIdMap)
          copiedTopicIdMap = Some(copy) // so we don't copy it again
          copy
      }
    }
    // get the current topicId-to-name map, either the copy if we have already copied or the original if not
    def getCurrentTopicIdMap(): util.Map[Uuid, String] = {
      copiedTopicIdMap match {
        case Some(map) => map
        case None => getMetadataSnapshot().topicIdMap
      }
    }

    private var copiedPartitionStates: Option[mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]]] = None
    def hasPartitionStateChanges(): Boolean = copiedPartitionStates.isDefined

    // get the partition states map, copying first if necessary
    def getCopiedPartitionStates(): mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]] = {
      copiedPartitionStates match {
        case Some(map) => map
        case None =>
          val metadataSnapshot = getMetadataSnapshot()
          val copy = metadataSnapshot.copyPartitionStates()
          copiedPartitionStates = Some(copy) // so we don't copy it again
          copy
      }
    }
    // get the current partition states map, either the copy if we have already copied or the original if not
    def getCurrentPartitionStates(): mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]] = {
      copiedPartitionStates match {
        case Some(map) => map
        case None => getMetadataSnapshot().partitionStates
      }
    }

    private var copiedFencedBrokers: Option[mutable.LongMap[Broker]] = None
    def hasFencedBrokerChanges(): Boolean = copiedFencedBrokers.isDefined

    // get the fenced brokers map, copying first if necessary
    def getCopiedFencedBrokers(): mutable.LongMap[Broker] = {
      copiedFencedBrokers match {
        case Some(map) => map
        case None =>
          val metadataSnapshot = getMetadataSnapshot()
          val maxPossibleCapacity = metadataSnapshot.fencedBrokers.size + numBrokersFencing
          val copy = new mutable.LongMap[Broker](maxPossibleCapacity)
          for ((existingBrokerId, existingBroker) <- metadataSnapshot.fencedBrokers) {
            copy(existingBrokerId) = existingBroker
          }
          copiedFencedBrokers = Some(copy)
          copy
      }
    }
    // get the current fenced broker map, either the copy if we have already copied or the original if not
    def getCurrentFencedBrokers(): mutable.LongMap[Broker] = {
      copiedFencedBrokers match {
        case Some(map) => map
        case None => getMetadataSnapshot().fencedBrokers
      }
    }

    private var copiedBrokerEpochs: Option[mutable.LongMap[Long]] = None
    def hasBrokerEpochChanges(): Boolean = copiedBrokerEpochs.isDefined

    // get the broker epochs map, copying first if necessary
    def getCopiedBrokerEpochs(): mutable.LongMap[Long] = {
      copiedBrokerEpochs match {
        case Some(map) => map
        case None =>
          val metadataSnapshot = getMetadataSnapshot()
          val maxPossibleCapacity = metadataSnapshot.brokerEpochs.size + numBrokersAdding
          val copy = new mutable.LongMap[Long](maxPossibleCapacity)
          for ((existingBrokerId, existingBrokerEpoch) <- metadataSnapshot.brokerEpochs) {
            copy(existingBrokerId) = existingBrokerEpoch
          }
          copiedBrokerEpochs = Some(copy)
          copy
      }
    }
    // get the current broker epochs map, either the copy if we have already copied or the original if not
    def getCurrentBrokerEpochs(): mutable.LongMap[Long] = {
      copiedBrokerEpochs match {
        case Some(map) => map
        case None => getMetadataSnapshot().brokerEpochs
      }
    }

    var requiresUpdateQuotaMetricConfigs = false

    // Collect information about which topic partitions need replicas stopped
    // and process it at the end since it requires acquisition of a write lock.
    // The value is the current leader epoch, or LeaderAndIsr.EpochDuringDelete if we are no longer hosting it
    val topicPartitionsNeedingStopReplica: mutable.Map[TopicPartition, Int] = mutable.Map.empty

    // Collect information about which Leader/Follower changes we need to make for the local broker
    // and process it at the end since it requires acquisition of a write lock.
    val topicPartitionsNeedingLeaderFollowerChanges: mutable.Map[TopicPartition, LeaderAndIsrPartitionState] = mutable.Map.empty
  }

  override def process(event: BrokerMetadataEvent): Unit = {
    event match {
      case metadataLogEvent: MetadataLogEvent => process(metadataLogEvent)
      case registerBrokerEvent: RegisterBrokerEvent => process(registerBrokerEvent)
      case _ => // no-op
    }
  }

  def process(metadataLogEvent: MetadataLogEvent): Unit = {
    // We have to copy any data structures that we are going to modify, and when we perform the copy
    // we want to define a new capacity that will be high enough to prevent additional internal copying, so
    // iterate over all messages once to determine new data structure capacities
    // before iterating again to process the messages.  Only count adds since it is possible that all adds could come
    // before all deletes, and oversizing the capacity is a small memory cost relative to the cost in time and code
    // complexity to determine the absolute minimum capacities.
    var numBrokersAdding = 0
    var numBrokersFencing = 0
    var numTopicsAdding = 0
    metadataLogEvent.apiMessages.asScala.foreach {
      case _: RegisterBrokerRecord | _: UnfenceBrokerRecord => numBrokersAdding += 1
      case _: FenceBrokerRecord => numBrokersFencing += 1
      case topic: TopicRecord => if (!topic.deleting()) numTopicsAdding += 1
      case _ => // no need to do anything for other record types
    }

    val mgr = MetadataMgr(numBrokersAdding, numTopicsAdding: Int, numBrokersFencing)

    // Iterate over all messages again to process them
    metadataLogEvent.apiMessages.asScala.foreach { msg =>
      try {
        msg match {
          case brokerRecord: RegisterBrokerRecord => process(brokerRecord, mgr)
          case fenceBroker: FenceBrokerRecord => process(fenceBroker, mgr)
          case unfenceBroker: UnfenceBrokerRecord => process(unfenceBroker, mgr)
          case topicRecord: TopicRecord => process(topicRecord, mgr)
          case removeTopicRecord: RemoveTopicRecord => process(removeTopicRecord, mgr)
          case partitionRecord: PartitionRecord => process(partitionRecord, mgr)
          case isrChangeRecord: IsrChangeRecord => process(isrChangeRecord, mgr)
          case configRecord: ConfigRecord => process(configRecord, mgr)
          case _ => // we don't process messages of this type -- whatever it is -- so ignore it
        }
      } catch {
        case e: Exception => error(s"Uncaught error processing metadata message: $msg", e)
      }
    }
    // We're done iterating through the batch and applying messages as required to data structure copies
    // (which may or may not have caused any changes).
    // Apply any changes we've made and perform any additional logging or notification as necessary
    if (mgr.hasBrokerChanges()) {
      metadataCache.logListenersNotIdenticalIfNecessary(mgr.getCurrentAliveNodes())
    }
    // update metadata cache if necessary
    if (mgr.hasPartitionStateChanges() || mgr.hasBrokerChanges() || mgr.hasTopicIdMapChanges()
      || mgr.hasFencedBrokerChanges() || mgr.hasBrokerEpochChanges()) {
      // Be sure to update metadata cache before potentially updating quota metric configs below
      // because the decision to update quota metric configs requires an accurate cluster metadata cache
      metadataCache.updatePartitionMetadata(
        // We know that at least one of these data structures has changed, but we don't want to copy
        // anything that hasn't changed already, so use the current version; this will
        // use the copy if there is one or the original if not.
        mgr.getCurrentPartitionStates(),
        mgr.getCurrentAliveBrokers(),
        mgr.getCurrentAliveNodes(),
        mgr.getCurrentTopicIdMap(),
        mgr.getCurrentFencedBrokers(),
        mgr.getCurrentBrokerEpochs())
      if (mgr.requiresUpdateQuotaMetricConfigs) {
        quotaManagers.clientQuotaCallback.foreach { callback =>
          val listenerName = kafkaConfig.interBrokerListenerName // TODO: confirm that this is correct
          if (callback.updateClusterMetadata(metadataCache.getClusterMetadata(clusterId, listenerName))) {
            quotaManagers.fetch.updateQuotaMetricConfigs()
            quotaManagers.produce.updateQuotaMetricConfigs()
            quotaManagers.request.updateQuotaMetricConfigs()
            quotaManagers.controllerMutation.updateQuotaMetricConfigs()
          }
        }
      }
    }
    // write any pending config changes and react to them accordingly
    val configResourcesChanged = mgr.writeConfigChangesIfNecessary()
    configResourcesChanged.foreach(configResource => {
      val configHandler = configHandlers.get(configResource.`type`())
      if (configHandler.isEmpty) {
        warn(s"No config handler for $configResource")
      } else {
        configHandler.get.processConfigChanges(configResource.name(), configProperties(configResource))
      }
    })
    // send stop replica as required
    if (mgr.topicPartitionsNeedingStopReplica.nonEmpty) {
      val correlationId = -1 // remove after bridge release
      val controllerId = mgr.getMetadataSnapshot().controllerId.getOrElse(-1)
      val controllerEpoch = replicaManager.controllerEpoch // requirement is it can't go backwards, so use current value
      val brokerEpoch = 0 // unused, so provide an arbitrary number
      val partitionStates: Map[TopicPartition, StopReplicaPartitionState] =
        mgr.topicPartitionsNeedingStopReplica.foldLeft(mutable.Map.empty[TopicPartition, StopReplicaPartitionState]) {
          case (map, (tp, leaderEpoch)) =>
            map(tp) = new StopReplicaPartitionState()
              .setPartitionIndex(tp.partition())
              .setDeletePartition(leaderEpoch == LeaderAndIsr.EpochDuringDelete)
              .setLeaderEpoch(leaderEpoch)
            map
        }
      val (result, _) = replicaManager.stopReplicas(
        correlationId, controllerId, controllerEpoch, brokerEpoch, partitionStates)
      // notify coordinators if any of their partitions are deleting
      result.forKeyValue { (topicPartition, error) =>
        if (error == Errors.NONE) {
          if (topicPartition.topic == Topic.GROUP_METADATA_TOPIC_NAME
            && partitionStates(topicPartition).deletePartition) {
            groupCoordinator.onResignation(topicPartition.partition)
          } else if (topicPartition.topic == Topic.TRANSACTION_STATE_TOPIC_NAME
            && partitionStates(topicPartition).deletePartition) {
            val partitionState = partitionStates(topicPartition)
            val leaderEpoch = if (partitionState.leaderEpoch >= 0)
              Some(partitionState.leaderEpoch)
            else
              None
            txnCoordinator.onResignation(topicPartition.partition, coordinatorEpoch = leaderEpoch)
          }
        }
      }
      CoreUtils.swallow(replicaManager.getReplicaFetcherManager().shutdownIdleFetcherThreads(), this)
    }
    // become leader or follower as required
    if (mgr.topicPartitionsNeedingLeaderFollowerChanges.nonEmpty) {
      val controllerId = mgr.getMetadataSnapshot().controllerId.getOrElse(-1)
      val controllerEpoch = replicaManager.controllerEpoch // can't go backwards, so use the current one
      val liveLeaders: util.Collection[Node] = Collections.emptyList() // I think this is not used?
      val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(
        ApiKeys.LEADER_AND_ISR.latestVersion(),
        controllerId, controllerEpoch, brokerEpoch,
        mgr.topicPartitionsNeedingLeaderFollowerChanges.values.toList.asJava,
        liveLeaders).build()
      val correlationId = -1 // just pick any value
      // no need to log results since they already get logged
      replicaManager.becomeLeaderOrFollower(correlationId,
        leaderAndIsrRequest,
        apisUtils.onLeadershipChange,
        mgr.getCurrentAliveBrokers().values.toSeq)
    }
  }

  /**
   * Handle Broker Record
   *
   * Update metadata about the indicated broker and its endpoints, and mark the broker as being alive.
   *
   * @param brokerRecord the record to process
   * @param mgr the current state to use
   */
  private def process(brokerRecord: RegisterBrokerRecord, mgr: MetadataMgr): Unit = {
    val nodes = new util.HashMap[ListenerName, Node]
    val endPoints = new ArrayBuffer[EndPoint]
    val brokerId = brokerRecord.brokerId()
    brokerRecord.endPoints().forEach { ep =>
      val listenerName = new ListenerName(ep.name())
      endPoints += new EndPoint(ep.host, ep.port, listenerName, SecurityProtocol.forId(ep.securityProtocol))
      nodes.put(listenerName, new Node(brokerId, ep.host, ep.port))
    }
    // copy if necessary and update the copies
    // add/update it as alive
    mgr.getCopiedAliveBrokers()(brokerId) = Broker(brokerId, endPoints, Option(brokerRecord.rack))
    mgr.getCopiedAliveNodes()(brokerId) = nodes.asScala
    // remove it from fenced if necessary
    mgr.getCopiedFencedBrokers().remove(brokerId)
    // set the broker epoch
    mgr.getCopiedBrokerEpochs()(brokerId) = brokerRecord.brokerEpoch()
  }

  /**
   * Handle Fence Broker Record
   *
   * Mark the indicated broker as not being alive.
   * If it is the local broker, update metadata to remove it from all partition leadership (leader will be unknown),
   * but leave ISRs alone and don't stop existing replica fetchers, if any.
   *
   * @param fenceBroker the record to process
   * @param mgr the current state to use
   */
  private def process(fenceBroker: FenceBrokerRecord, mgr: MetadataMgr): Unit = {
    val brokerId = fenceBroker.id()
    // check the current state, whether copied already or not
    if (!mgr.getCurrentAliveBrokers().contains(brokerId)) {
      // The broker is not considered alive.  Log at ERROR level because this state should not occur.
      error(s"Skipping fence broker message because the broker is not considered alive: $fenceBroker")
    } else {
      // sanity-check the fenced broker epoch
      val currentBrokerEpoch = mgr.getCurrentBrokerEpochs().getOrElse(brokerId, Int.MinValue)
      if (fenceBroker.epoch() != currentBrokerEpoch) {
        error(s"Skipping fence broker message because current broker epoch ($currentBrokerEpoch)" +
          s" is not the epoch being fenced: $fenceBroker")
      } else {
        // copy if necessary and update the copies
        // move the broker from alive to fenced
        val aliveBrokersCopy = mgr.getCopiedAliveBrokers()
        val aliveNodesCopy = mgr.getCopiedAliveNodes()
        // we know from above that it is alive
        mgr.getCopiedFencedBrokers()(brokerId) = aliveBrokersCopy.remove(brokerId).get
        aliveNodesCopy.remove(brokerId)
        // no need to change the broker epoch since the existing one will be the one that is fenced
      }
    }
  }

  /**
   * Handle Unfence Broker Record
   *
   * Mark the indicated broker as being alive.
   *
   * @param unfenceBroker the record to process
   * @param mgr the current state to use
   */
  private def process(unfenceBroker: UnfenceBrokerRecord, mgr: MetadataMgr): Unit = {
    val brokerId = unfenceBroker.id()
    // check the current state, whether copied already or not
    if (mgr.getCurrentAliveBrokers().contains(brokerId)) {
      // The broker is already considered alive.
      error(s"Skipping unfence broker message because the broker is already considered alive: $unfenceBroker")
    } else if (!mgr.getCurrentFencedBrokers().contains(brokerId)) {
      // The broker is not considered fenced.
      error(s"Skipping unfence broker message because the broker is not considered fenced: $unfenceBroker")
    } else {
      // sanity-check the unfenced broker epoch
      val currentBrokerEpoch = mgr.getCurrentBrokerEpochs().getOrElse(brokerId, Int.MinValue)
      if (unfenceBroker.epoch() != currentBrokerEpoch) {
        error(s"Skipping unfence broker message because current broker epoch ($currentBrokerEpoch)" +
          s" is not the epoch being unfenced: $unfenceBroker")
      } else {
        // copy if necessary and update the copies
        // move the broker from fenced to alive
        // we know from above that it is fenced
        val broker = mgr.getCopiedFencedBrokers().remove(brokerId).get
        mgr.getCopiedAliveBrokers()(brokerId) = broker
        // we don't save the node anywhere when a broker is fenced, so recreate it
        val nodes = new util.HashMap[ListenerName, Node]
        broker.endPoints.foreach(ep => {
          nodes.put(ep.listenerName, new Node(brokerId, ep.host, ep.port))
        })
        mgr.getCopiedAliveNodes()(brokerId) = nodes.asScala
        // no need to change the broker epoch since the existing one will be the one that is unfenced
      }
    }
  }

  /**
   * Handle Topic Record
   *
   * If adding:
   * Create the topic in the metadata with no partitions (yet)
   *
   * If deleting:
   * Remove all topic-partitions for the indicated topic from the metadata.
   * Stop replica fetchers as required with deletion of log directories.
   * The only remaining thing in the metadata will be the Topic UUID.
   *
   * @param topicRecord the record to process
   * @param mgr the current state to use
   */
  private def process(topicRecord: TopicRecord, mgr: MetadataMgr): Unit = {
    val topicId = topicRecord.topicId()
    val topicName = topicRecord.name()
    if (!topicRecord.deleting()) { // topic adding
      if (mgr.getCurrentTopicIdMap().containsKey(topicId) || mgr.getCurrentPartitionStates().get(topicName).isDefined) {
        error(s"Skipping metadata message for a new topic that already exists: $topicRecord")
      } else {
        mgr.getCopiedPartitionStates()(topicName) = mutable.LongMap.empty
        mgr.getCopiedTopicIdMap().put(topicId, topicName)
        if (metadataCache.stateChangeTraceEnabled()) {
          metadataCache.logStateChangeTrace(s"Caching new topic $topicId/$topicName with no partitions (yet) via metadata log")
        }
      }
    } else { // topic deleting
      val currentPartitionStatesForTopic = mgr.getCurrentPartitionStates().get(topicName)
      if (currentPartitionStatesForTopic.isEmpty) {
        error(s"Skipping metadata message for a topic to be deleted that doesn't exist: $topicRecord")
      } else {
        val deletingPartitionsForThisTopic = currentPartitionStatesForTopic.get.keySet.map(
          partition => new TopicPartition(topicName, partition.toInt))
        val copiedPartitionStates = mgr.getCopiedPartitionStates()
        // collect the topic partitions where we are a replica so we can stop/delete
        deletingPartitionsForThisTopic.filter(tp =>
          copiedPartitionStates.get(topicName).exists(infos => infos.get(tp.partition()).exists(partitionState =>
            partitionState.replicas().contains(kafkaConfig.brokerId)))).foreach(partition =>
          mgr.topicPartitionsNeedingStopReplica(partition) = LeaderAndIsr.EpochDuringDelete)
        // delete the partitions from the metadata
        deletingPartitionsForThisTopic.foreach(tp => {
          MetadataCache.removePartitionInfo(copiedPartitionStates, topicName, tp.partition())
          if (metadataCache.stateChangeTraceEnabled())
            metadataCache.logStateChangeTrace(s"Deleting partition $tp from metadata cache in response to a TopicRecord on the metadata log")
        })
        // notify group coordinator of the deleting partitions right away since this isn't too expensive
        groupCoordinator.handleDeletedPartitions(deletingPartitionsForThisTopic.toSeq)
        mgr.requiresUpdateQuotaMetricConfigs = true
      }
    }
  }

  /**
   * Handle Partition Record
   *
   * Update metadata about the indicated topic-partition and its leader/replicas.
   * Become leader or follower or stop replica as required.
   *
   * @param partition the record to process
   * @param mgr the current state to use
   */
  private def process(partition: PartitionRecord, mgr: MetadataMgr): Unit = {
    val topicName = mgr.getCurrentTopicIdMap().get(partition.topicId())
    if (topicName == null) {
      error(s"Unable to process PartitionRecord due to unknown topicId: $partition")
    } else {
      // add or update the partition info, being sure to copy the data structure first if necessary
      val partitionId = partition.partitionId()
      MetadataCache.addOrUpdatePartitionInfo(mgr.getCopiedPartitionStates(), topicName, partitionId,
        new UpdateMetadataPartitionState()
          .setPartitionIndex(partitionId)
          .setControllerEpoch(replicaManager.controllerEpoch)
          .setLeader(partition.leader())
          .setLeaderEpoch(partition.leaderEpoch())
          .setIsr(partition.isr())
          .setReplicas(partition.replicas())
        // TODO: ignore offline replicas until we support the JBOD disk failure use case
      )
      val brokerId = kafkaConfig.brokerId
      val isLeaderOrFollower = partition.leader == brokerId || partition.replicas.contains(brokerId)

      if (isLeaderOrFollower) {
        val topicPartition = new TopicPartition(topicName, partitionId)
        val currentStateChangeRequest = mgr.topicPartitionsNeedingLeaderFollowerChanges.get(topicPartition)
        val isNewPartition = currentStateChangeRequest.isEmpty || currentStateChangeRequest.get.isNew
        val controllerEpoch = replicaManager.controllerEpoch // can't go backwards, so use the current one
        mgr.topicPartitionsNeedingLeaderFollowerChanges(topicPartition) =
          new LeaderAndIsrPartitionState()
            .setAddingReplicas(Option(partition.addingReplicas).getOrElse(Collections.emptyList[Integer]))
            .setControllerEpoch(controllerEpoch)
            .setIsNew(isNewPartition)
            .setIsr(partition.isr)
            .setLeader(partition.leader)
            .setLeaderEpoch(partition.leaderEpoch)
            .setPartitionIndex(partitionId)
            .setRemovingReplicas(Option(partition.removingReplicas).getOrElse(Collections.emptyList[Integer]))
            .setReplicas(partition.replicas)
            .setTopicName(topicName)
        // cancel any request to remove the local replica that may have already appeared in this batch
        mgr.topicPartitionsNeedingStopReplica.remove(topicPartition)
      }
    }
  }

  /**
   * Handle ISR Change Record
   *
   * Update metadata about the indicated topic-partition and its leader/ISRs.
   * React as though we received a LeaderAndIsr message if the local broker is the leader or in the ISR.
   *
   * @param isrChange the record to process
   * @param mgr the current state to use
   */
  private def process(isrChange: IsrChangeRecord, mgr: MetadataMgr): Unit = {
    val topicName = mgr.getCurrentTopicIdMap().get(isrChange.topicId())
    if (topicName == null) {
      error(s"Unable to process IsrChangeRecord due to unknown topicId: $isrChange")
    } else {
      // add or update the partition info, being sure to copy the data structure first if necessary
      val partitionStates = mgr.getCopiedPartitionStates()
      val currentPartitionStatesForTopic = partitionStates.get(topicName)
      if (currentPartitionStatesForTopic.isEmpty) {
        error(s"Unable to process IsrChangeRecord due to no partition state for topic: $isrChange")
      } else {
        val partitionId = isrChange.partitionId()
        val currentPartitionState = currentPartitionStatesForTopic.get.get(partitionId)
        if (currentPartitionState.isEmpty) {
          error(s"Unable to process IsrChangeRecord due to no partition state within topic for partition: $isrChange")
        } else {
          val currentLeader = currentPartitionState.get.leader()
          val newLeader = isrChange.leader()
          val newLeaderEpoch = isrChange.leaderEpoch()
          val newIsr = isrChange.isr()
          val replicas = currentPartitionState.get.replicas()
          MetadataCache.addOrUpdatePartitionInfo(partitionStates, topicName, partitionId,
            new UpdateMetadataPartitionState()
              .setPartitionIndex(partitionId)
              .setControllerEpoch(replicaManager.controllerEpoch)
              .setLeader(newLeader)
              .setLeaderEpoch(newLeaderEpoch)
              .setIsr(newIsr)
              .setReplicas(replicas)
            // TODO: ignore offline replicas until we support the JBOD disk failure use case
          )
          val brokerId = kafkaConfig.brokerId
          if (replicas.contains(brokerId)) {
            // we only need to invoke the logic to become a leader or a follower if our role is changing
            if (currentLeader != newLeader && (currentLeader == brokerId || newLeader == brokerId)) {
              // schedule a change to leader or follower as required, being sure to keep any "isNew" status
              // and remembering all the added and deleted replicas
              // in case we are seeing this partition multiple times in the same batch
              val tp = new TopicPartition(topicName, partitionId)
              val currentStateChangeRequest = mgr.topicPartitionsNeedingLeaderFollowerChanges.get(tp)
              // default is to not be considered a new partition for this broker, so it's only new if already is
              val isNewPartition = currentStateChangeRequest.isDefined && currentStateChangeRequest.get.isNew
              val replicasAlreadyAddedInThisBatch: util.List[Integer] =
                if (currentStateChangeRequest.isDefined) {
                  currentStateChangeRequest.get.addingReplicas()
                } else {
                  Collections.emptyList()
                }
              val replicasAlreadyRemovedInThisBatch: util.List[Integer] =
                if (currentStateChangeRequest.isDefined) {
                  currentStateChangeRequest.get.removingReplicas()
                } else {
                  Collections.emptyList()
                }
              val controllerEpoch = replicaManager.controllerEpoch // can't go backwards, so use the current one
              mgr.topicPartitionsNeedingLeaderFollowerChanges(tp) =
                new LeaderAndIsrPartitionState()
                  .setAddingReplicas(replicasAlreadyAddedInThisBatch)
                  .setControllerEpoch(controllerEpoch)
                  .setIsNew(isNewPartition)
                  .setIsr(newIsr)
                  .setLeader(newLeader)
                  .setLeaderEpoch(newLeaderEpoch)
                  .setPartitionIndex(partitionId)
                  .setRemovingReplicas(replicasAlreadyRemovedInThisBatch)
                  .setReplicas(replicas)
                  .setTopicName(topicName)
              // cancel any request to remove the local replica that may have already appeared in this batch
              mgr.topicPartitionsNeedingStopReplica.remove(tp)
            }
          }
        }
      }
    }
  }

  /**
   * Handle Remove Topic Record
   *
   * Remove the Topic UUID from the metadata.
   *
   * @param removeTopic the record to process
   * @param mgr the current state to use
   */
  private def process(removeTopic: RemoveTopicRecord, mgr: MetadataMgr): Unit = {
    mgr.getCopiedTopicIdMap().remove(removeTopic.topicId())
  }

  /**
   * Handle Config Record
   *
   * Update configs as indicated.
   *
   * @param config the record to process
   */
  private def process(config: ConfigRecord, mgr: MetadataMgr): Unit = {
    mgr.addConfigResourceChange(new ConfigResource(ConfigResource.Type.forId(config.resourceType()), config.resourceName()),
      config.name(), config.value())
  }

  def process(outOfBandRegisterLocalBrokerEvent: RegisterBrokerEvent): Unit = {
    val requestedBrokerEpoch = outOfBandRegisterLocalBrokerEvent.brokerEpoch
    if (requestedBrokerEpoch < 0) {
      throw new IllegalArgumentException(s"Cannot change broker epoch to a negative value: $requestedBrokerEpoch")
    }
    // idempotent, and disallow changing the epoch once it is set to a positive value
    if (brokerEpoch >= 0) {
      if (requestedBrokerEpoch != brokerEpoch) {
        throw new IllegalArgumentException(s"Cannot change broker epoch from already-set value $brokerEpoch: $requestedBrokerEpoch")
      }
    } else {
      brokerEpoch = requestedBrokerEpoch
      val numBrokersAdding = 1
      val mgr = MetadataMgr(numBrokersAdding, 0, 0)
      mgr.getCopiedBrokerEpochs().put(kafkaConfig.brokerId, requestedBrokerEpoch)
      metadataCache.updatePartitionMetadata(
        mgr.getCurrentPartitionStates(),
        mgr.getCurrentAliveBrokers(),
        mgr.getCurrentAliveNodes(),
        mgr.getCurrentTopicIdMap(),
        mgr.getCurrentFencedBrokers(),
        mgr.getCurrentBrokerEpochs())
    }
  }
}
