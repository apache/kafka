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
package kafka.controller

import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Gauge
import kafka.admin.AdminOperationException
import kafka.api._
import kafka.common._
import kafka.controller.KafkaController.ElectLeadersCallback
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server._
import kafka.utils._
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk._
import kafka.zookeeper.{StateChangeHandler, ZNodeChangeHandler, ZNodeChildChangeHandler}
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException, StaleBrokerEpochException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractControlRequest, AbstractResponse, ApiError, LeaderAndIsrResponse}
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.JavaConverters._
import scala.collection._
import scala.util.{Failure, Try}

sealed trait ElectionTrigger
final case object AutoTriggered extends ElectionTrigger
final case object ZkTriggered extends ElectionTrigger
final case object AdminClientTriggered extends ElectionTrigger

object KafkaController extends Logging {
  val InitialControllerEpoch = 0
  val InitialControllerEpochZkVersion = 0

  type ElectLeadersCallback = Map[TopicPartition, Either[ApiError, Int]] => Unit
}

class KafkaController(val config: KafkaConfig,
                      zkClient: KafkaZkClient,
                      time: Time,
                      metrics: Metrics,
                      initialBrokerInfo: BrokerInfo,
                      initialBrokerEpoch: Long,
                      tokenManager: DelegationTokenManager,
                      threadNamePrefix: Option[String] = None)
  extends ControllerEventProcessor with Logging with KafkaMetricsGroup {

  this.logIdent = s"[Controller id=${config.brokerId}] "

  @volatile private var brokerInfo = initialBrokerInfo
  @volatile private var _brokerEpoch = initialBrokerEpoch

  private val stateChangeLogger = new StateChangeLogger(config.brokerId, inControllerContext = true, None)
  val controllerContext = new ControllerContext
  var controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics,
    stateChangeLogger, threadNamePrefix)

  // have a separate scheduler for the controller to be able to start and stop independently of the kafka server
  // visible for testing
  private[controller] val kafkaScheduler = new KafkaScheduler(1)

  // visible for testing
  private[controller] val eventManager = new ControllerEventManager(config.brokerId, this, time,
    controllerContext.stats.rateAndTimeMetrics)

  private val brokerRequestBatch = new ControllerBrokerRequestBatch(config, controllerChannelManager,
    eventManager, controllerContext, stateChangeLogger)
  val replicaStateMachine: ReplicaStateMachine = new ZkReplicaStateMachine(config, stateChangeLogger, controllerContext, zkClient,
    new ControllerBrokerRequestBatch(config, controllerChannelManager, eventManager, controllerContext, stateChangeLogger))
  val partitionStateMachine: PartitionStateMachine = new ZkPartitionStateMachine(config, stateChangeLogger, controllerContext, zkClient,
    new ControllerBrokerRequestBatch(config, controllerChannelManager, eventManager, controllerContext, stateChangeLogger))
  val topicDeletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
    partitionStateMachine, new ControllerDeletionClient(this, zkClient))

  private val controllerChangeHandler = new ControllerChangeHandler(eventManager)
  private val brokerChangeHandler = new BrokerChangeHandler(eventManager)
  private val brokerModificationsHandlers: mutable.Map[Int, BrokerModificationsHandler] = mutable.Map.empty
  private val topicChangeHandler = new TopicChangeHandler(eventManager)
  private val topicDeletionHandler = new TopicDeletionHandler(eventManager)
  private val partitionModificationsHandlers: mutable.Map[String, PartitionModificationsHandler] = mutable.Map.empty
  private val partitionReassignmentHandler = new PartitionReassignmentHandler(eventManager)
  private val preferredReplicaElectionHandler = new PreferredReplicaElectionHandler(eventManager)
  private val isrChangeNotificationHandler = new IsrChangeNotificationHandler(eventManager)
  private val logDirEventNotificationHandler = new LogDirEventNotificationHandler(eventManager)

  @volatile private var activeControllerId = -1
  @volatile private var offlinePartitionCount = 0
  @volatile private var preferredReplicaImbalanceCount = 0
  @volatile private var globalTopicCount = 0
  @volatile private var globalPartitionCount = 0

  /* single-thread scheduler to clean expired tokens */
  private val tokenCleanScheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "delegation-token-cleaner")

  newGauge(
    "ActiveControllerCount",
    new Gauge[Int] {
      def value = if (isActive) 1 else 0
    }
  )

  newGauge(
    "OfflinePartitionsCount",
    new Gauge[Int] {
      def value: Int = offlinePartitionCount
    }
  )

  newGauge(
    "PreferredReplicaImbalanceCount",
    new Gauge[Int] {
      def value: Int = preferredReplicaImbalanceCount
    }
  )

  newGauge(
    "ControllerState",
    new Gauge[Byte] {
      def value: Byte = state.value
    }
  )

  newGauge(
    "GlobalTopicCount",
    new Gauge[Int] {
      def value: Int = globalTopicCount
    }
  )

  newGauge(
    "GlobalPartitionCount",
    new Gauge[Int] {
      def value: Int = globalPartitionCount
    }
  )

  /**
   * Returns true if this broker is the current controller.
   */
  def isActive: Boolean = activeControllerId == config.brokerId

  def brokerEpoch: Long = _brokerEpoch

  def epoch: Int = controllerContext.epoch

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  def startup() = {
    zkClient.registerStateChangeHandler(new StateChangeHandler {
      override val name: String = StateChangeHandlers.ControllerHandler
      override def afterInitializingSession(): Unit = {
        eventManager.put(RegisterBrokerAndReelect)
      }
      override def beforeInitializingSession(): Unit = {
        val queuedEvent = eventManager.clearAndPut(Expire)

        // Block initialization of the new session until the expiration event is being handled,
        // which ensures that all pending events have been processed before creating the new session
        queuedEvent.awaitProcessing()
      }
    })
    eventManager.put(Startup)
    eventManager.start()
  }

  /**
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown() = {
    eventManager.close()
    onControllerResignation()
  }

  /**
   * On controlled shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   *
   * @param id Id of the broker to shutdown.
   * @param brokerEpoch The broker epoch in the controlled shutdown request
   * @return The number of partitions that the broker still leads.
   */
  def controlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    val controlledShutdownEvent = ControlledShutdown(id, brokerEpoch, controlledShutdownCallback)
    eventManager.put(controlledShutdownEvent)
  }

  private[kafka] def updateBrokerInfo(newBrokerInfo: BrokerInfo): Unit = {
    this.brokerInfo = newBrokerInfo
    zkClient.updateBrokerInfo(newBrokerInfo)
  }

  private[kafka] def enableDefaultUncleanLeaderElection(): Unit = {
    eventManager.put(UncleanLeaderElectionEnable)
  }

  private[kafka] def enableTopicUncleanLeaderElection(topic: String): Unit = {
    if (isActive) {
      eventManager.put(TopicUncleanLeaderElectionEnable(topic))
    }
  }

  private def state: ControllerState = eventManager.state

  /**
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   * It does the following things on the become-controller state change -
   * 1. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 2. Starts the controller's channel manager
   * 3. Starts the replica state machine
   * 4. Starts the partition state machine
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   */
  private def onControllerFailover() {
    info("Registering handlers")

    // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
    val childChangeHandlers = Seq(brokerChangeHandler, topicChangeHandler, topicDeletionHandler, logDirEventNotificationHandler,
      isrChangeNotificationHandler)
    childChangeHandlers.foreach(zkClient.registerZNodeChildChangeHandler)
    val nodeChangeHandlers = Seq(preferredReplicaElectionHandler, partitionReassignmentHandler)
    nodeChangeHandlers.foreach(zkClient.registerZNodeChangeHandlerAndCheckExistence)

    info("Deleting log dir event notifications")
    zkClient.deleteLogDirEventNotifications(controllerContext.epochZkVersion)
    info("Deleting isr change notifications")
    zkClient.deleteIsrChangeNotifications(controllerContext.epochZkVersion)
    info("Initializing controller context")
    initializeControllerContext()
    info("Fetching topic deletions in progress")
    val (topicsToBeDeleted, topicsIneligibleForDeletion) = fetchTopicDeletionsInProgress()
    info("Initializing topic deletion manager")
    topicDeletionManager.init(topicsToBeDeleted, topicsIneligibleForDeletion)

    // We need to send UpdateMetadataRequest after the controller context is initialized and before the state machines
    // are started. The is because brokers need to receive the list of live brokers from UpdateMetadataRequest before
    // they can process the LeaderAndIsrRequests that are generated by replicaStateMachine.startup() and
    // partitionStateMachine.startup().
    info("Sending update metadata request")
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)

    replicaStateMachine.startup()
    partitionStateMachine.startup()

    info(s"Ready to serve as the new controller with epoch $epoch")
    maybeTriggerPartitionReassignment(controllerContext.partitionsBeingReassigned.keySet)
    topicDeletionManager.tryTopicDeletion()
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    onReplicaElection(pendingPreferredReplicaElections, ElectionType.PREFERRED, ZkTriggered)
    info("Starting the controller scheduler")
    kafkaScheduler.startup()
    if (config.autoLeaderRebalanceEnable) {
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
    }

    if (config.tokenAuthEnabled) {
      info("starting the token expiry check scheduler")
      tokenCleanScheduler.startup()
      tokenCleanScheduler.schedule(name = "delete-expired-tokens",
        fun = () => tokenManager.expireTokens,
        period = config.delegationTokenExpiryCheckIntervalMs,
        unit = TimeUnit.MILLISECONDS)
    }
  }

  private def scheduleAutoLeaderRebalanceTask(delay: Long, unit: TimeUnit): Unit = {
    kafkaScheduler.schedule("auto-leader-rebalance-task", () => eventManager.put(AutoPreferredReplicaLeaderElection),
      delay = delay, unit = unit)
  }

  /**
   * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
   * required to clean up internal controller data structures
   */
  private def onControllerResignation() {
    debug("Resigning")
    // de-register listeners
    zkClient.unregisterZNodeChildChangeHandler(isrChangeNotificationHandler.path)
    zkClient.unregisterZNodeChangeHandler(partitionReassignmentHandler.path)
    zkClient.unregisterZNodeChangeHandler(preferredReplicaElectionHandler.path)
    zkClient.unregisterZNodeChildChangeHandler(logDirEventNotificationHandler.path)
    unregisterBrokerModificationsHandler(brokerModificationsHandlers.keySet)

    // shutdown leader rebalance scheduler
    kafkaScheduler.shutdown()
    offlinePartitionCount = 0
    preferredReplicaImbalanceCount = 0
    globalTopicCount = 0
    globalPartitionCount = 0

    // stop token expiry check scheduler
    if (tokenCleanScheduler.isStarted)
      tokenCleanScheduler.shutdown()

    // de-register partition ISR listener for on-going partition reassignment task
    unregisterPartitionReassignmentIsrChangeHandlers()
    // shutdown partition state machine
    partitionStateMachine.shutdown()
    zkClient.unregisterZNodeChildChangeHandler(topicChangeHandler.path)
    unregisterPartitionModificationsHandlers(partitionModificationsHandlers.keys.toSeq)
    zkClient.unregisterZNodeChildChangeHandler(topicDeletionHandler.path)
    // shutdown replica state machine
    replicaStateMachine.shutdown()
    zkClient.unregisterZNodeChildChangeHandler(brokerChangeHandler.path)

    controllerChannelManager.shutdown()
    controllerContext.resetContext()

    info("Resigned")
  }

  /*
   * This callback is invoked by the controller's LogDirEventNotificationListener with the list of broker ids who
   * have experienced new log directory failures. In response the controller should send LeaderAndIsrRequest
   * to all these brokers to query the state of their replicas. Replicas with an offline log directory respond with
   * KAFKA_STORAGE_ERROR, which will be handled by the LeaderAndIsrResponseReceived event.
   */
  private def onBrokerLogDirFailure(brokerIds: Seq[Int]) {
    // send LeaderAndIsrRequest for all replicas on those brokers to see if they are still online.
    info(s"Handling log directory failure for brokers ${brokerIds.mkString(",")}")
    val replicasOnBrokers = controllerContext.replicasOnBrokers(brokerIds.toSet)
    replicaStateMachine.handleStateChanges(replicasOnBrokers.toSeq, OnlineReplica)
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Sends update metadata request to all live and shutting down brokers
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. It checks whether there are reassigned replicas assigned to any newly started brokers. If
   *    so, it performs the reassignment logic for each topic/partition.
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
   */
  private def onBrokerStartup(newBrokers: Seq[Int]) {
    info(s"New broker startup callback for ${newBrokers.mkString(",")}")
    newBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
    val newBrokersSet = newBrokers.toSet
    val existingBrokers = controllerContext.liveOrShuttingDownBrokerIds -- newBrokers
    // Send update metadata request to all the existing brokers in the cluster so that they know about the new brokers
    // via this update. No need to include any partition states in the request since there are no partition state changes.
    sendUpdateMetadataRequest(existingBrokers.toSeq, Set.empty)
    // Send update metadata request to all the new brokers in the cluster with a full set of partition states for initialization.
    // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
    // common controlled shutdown case, the metadata will reach the new brokers faster.
    sendUpdateMetadataRequest(newBrokers, controllerContext.partitionLeadershipInfo.keySet)
    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
    replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers.toSeq, OnlineReplica)
    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // check if reassignment of some partitions need to be restarted
    val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter {
      case (_, reassignmentContext) => reassignmentContext.newReplicas.exists(newBrokersSet.contains)
    }
    partitionsWithReplicasOnNewBrokers.foreach { case (tp, context) => onPartitionReassignment(tp, context) }
    // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
    // on the newly restarted brokers, there is a chance that topic deletion can resume
    val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
    if (replicasForTopicsToBeDeleted.nonEmpty) {
      info(s"Some replicas ${replicasForTopicsToBeDeleted.mkString(",")} for topics scheduled for deletion " +
        s"${controllerContext.topicsToBeDeleted.mkString(",")} are on the newly restarted brokers " +
        s"${newBrokers.mkString(",")}. Signaling restart of topic deletion for these topics")
      topicDeletionManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }
    registerBrokerModificationsHandler(newBrokers)
  }

  private def registerBrokerModificationsHandler(brokerIds: Iterable[Int]): Unit = {
    debug(s"Register BrokerModifications handler for $brokerIds")
    brokerIds.foreach { brokerId =>
      val brokerModificationsHandler = new BrokerModificationsHandler(eventManager, brokerId)
      zkClient.registerZNodeChangeHandlerAndCheckExistence(brokerModificationsHandler)
      brokerModificationsHandlers.put(brokerId, brokerModificationsHandler)
    }
  }

  private def unregisterBrokerModificationsHandler(brokerIds: Iterable[Int]): Unit = {
    debug(s"Unregister BrokerModifications handler for $brokerIds")
    brokerIds.foreach { brokerId =>
      brokerModificationsHandlers.remove(brokerId).foreach(handler => zkClient.unregisterZNodeChangeHandler(handler.path))
    }
  }

  /*
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It will call onReplicaBecomeOffline(...) with the list of replicas on those failed brokers as input.
   */
  private def onBrokerFailure(deadBrokers: Seq[Int]) {
    info(s"Broker failure callback for ${deadBrokers.mkString(",")}")
    deadBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    if (deadBrokersThatWereShuttingDown.nonEmpty)
      info(s"Removed ${deadBrokersThatWereShuttingDown.mkString(",")} from list of shutting down brokers.")
    val allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokers.toSet)
    onReplicasBecomeOffline(allReplicasOnDeadBrokers)

    unregisterBrokerModificationsHandler(deadBrokers)
  }

  private def onBrokerUpdate(updatedBrokerId: Int) {
    info(s"Broker info update callback for $updatedBrokerId")
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)
  }

  /**
    * This method marks the given replicas as offline. It does the following -
    * 1. Marks the given partitions as offline
    * 2. Triggers the OnlinePartition state change for all new/offline partitions
    * 3. Invokes the OfflineReplica state change on the input list of newly offline replicas
    * 4. If no partitions are affected then send UpdateMetadataRequest to live or shutting down brokers
    *
    * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point. This is because
    * the partition state machine will refresh our cache for us when performing leader election for all new/offline
    * partitions coming online.
    */
  private def onReplicasBecomeOffline(newOfflineReplicas: Set[PartitionAndReplica]): Unit = {
    val (newOfflineReplicasForDeletion, newOfflineReplicasNotForDeletion) =
      newOfflineReplicas.partition(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))

    val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
      !controllerContext.isReplicaOnline(partitionAndLeader._2.leaderAndIsr.leader, partitionAndLeader._1) &&
        !topicDeletionManager.isTopicQueuedUpForDeletion(partitionAndLeader._1.topic)).keySet

    // trigger OfflinePartition state for all partitions whose current leader is one amongst the newOfflineReplicas
    partitionStateMachine.handleStateChanges(partitionsWithoutLeader.toSeq, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // trigger OfflineReplica state change for those newly offline replicas
    replicaStateMachine.handleStateChanges(newOfflineReplicasNotForDeletion.toSeq, OfflineReplica)

    // fail deletion of topics that are affected by the offline replicas
    if (newOfflineReplicasForDeletion.nonEmpty) {
      // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
      // deleted when its log directory is offline. This will prevent the replica from being in TopicDeletionStarted state indefinitely
      // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
      topicDeletionManager.failReplicaDeletion(newOfflineReplicasForDeletion)
    }

    // If replica failure did not require leader re-election, inform brokers of the offline brokers
    // Note that during leader re-election, brokers update their metadata
    if (partitionsWithoutLeader.isEmpty) {
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)
    }
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  private def onNewPartitionCreation(newPartitions: Set[TopicPartition]) {
    info(s"New partition creation callback for ${newPartitions.mkString(",")}")
    partitionStateMachine.handleStateChanges(newPartitions.toSeq, NewPartition)
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, NewReplica)
    partitionStateMachine.handleStateChanges(
      newPartitions.toSeq,
      OnlinePartition,
      Some(OfflinePartitionLeaderElectionStrategy(false))
    )
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, OnlineReplica)
  }

  /**
   * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
   * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   * RAR = Reassigned replicas
   * OAR = Original list of replicas for partition
   * AR = current assigned replicas
   *
   * 1. Update AR in ZK with OAR + RAR.
   * 2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR). We do this by forcing an update
   *    of the leader epoch in zookeeper.
   * 3. Start new replicas RAR - OAR by moving replicas in RAR - OAR to NewReplica state.
   * 4. Wait until all replicas in RAR are in sync with the leader.
   * 5  Move all replicas in RAR to OnlineReplica state.
   * 6. Set AR to RAR in memory.
   * 7. If the leader is not in RAR, elect a new leader from RAR. If new leader needs to be elected from RAR, a LeaderAndIsr
   *    will be sent. If not, then leader epoch will be incremented in zookeeper and a LeaderAndIsr request will be sent.
   *    In any case, the LeaderAndIsr request will have AR = RAR. This will prevent the leader from adding any replica in
   *    RAR - OAR back in the isr.
   * 8. Move all replicas in OAR - RAR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *    isr to remove OAR - RAR in zookeeper and send a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *    After that, we send a StopReplica (delete = false) to the replicas in OAR - RAR.
   * 9. Move all replicas in OAR - RAR to NonExistentReplica state. This will send a StopReplica (delete = true) to
   *    the replicas in OAR - RAR to physically delete the replicas on disk.
   * 10. Update AR in ZK with RAR.
   * 11. Update the /admin/reassign_partitions path in ZK to remove this partition.
   * 12. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
   *
   * For example, if OAR = {1, 2, 3} and RAR = {4,5,6}, the values in the assigned replica (AR) and leader/isr path in ZK
   * may go through the following transition.
   * AR                 leader/isr
   * {1,2,3}            1/{1,2,3}           (initial state)
   * {1,2,3,4,5,6}      1/{1,2,3}           (step 2)
   * {1,2,3,4,5,6}      1/{1,2,3,4,5,6}     (step 4)
   * {1,2,3,4,5,6}      4/{1,2,3,4,5,6}     (step 7)
   * {1,2,3,4,5,6}      4/{4,5,6}           (step 8)
   * {4,5,6}            4/{4,5,6}           (step 10)
   *
   * Note that we have to update AR in ZK with RAR last since it's the only place where we store OAR persistently.
   * This way, if the controller crashes before that step, we can still recover.
   */
  private def onPartitionReassignment(topicPartition: TopicPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    if (!areReplicasInIsr(topicPartition, reassignedReplicas)) {
      info(s"New replicas ${reassignedReplicas.mkString(",")} for partition $topicPartition being reassigned not yet " +
        "caught up with the leader")
      val newReplicasNotInOldReplicaList = reassignedReplicas.toSet -- controllerContext.partitionReplicaAssignment(topicPartition).toSet
      val newAndOldReplicas = (reassignedPartitionContext.newReplicas ++ controllerContext.partitionReplicaAssignment(topicPartition)).toSet
      //1. Update AR in ZK with OAR + RAR.
      updateAssignedReplicasForPartition(topicPartition, newAndOldReplicas.toSeq)
      //2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR).
      updateLeaderEpochAndSendRequest(topicPartition, controllerContext.partitionReplicaAssignment(topicPartition),
        newAndOldReplicas.toSeq)
      //3. replicas in RAR - OAR -> NewReplica
      startNewReplicasForReassignedPartition(topicPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList)
      info(s"Waiting for new replicas ${reassignedReplicas.mkString(",")} for partition $topicPartition being " +
        "reassigned to catch up with the leader")
    } else {
      //4. Wait until all replicas in RAR are in sync with the leader.
      val oldReplicas = controllerContext.partitionReplicaAssignment(topicPartition).toSet -- reassignedReplicas.toSet
      //5. replicas in RAR -> OnlineReplica
      reassignedReplicas.foreach { replica =>
        replicaStateMachine.handleStateChanges(Seq(PartitionAndReplica(topicPartition, replica)), OnlineReplica)
      }
      //6. Set AR to RAR in memory.
      //7. Send LeaderAndIsr request with a potential new leader (if current leader not in RAR) and
      //   a new AR (using RAR) and same isr to every broker in RAR
      moveReassignedPartitionLeaderIfRequired(topicPartition, reassignedPartitionContext)
      //8. replicas in OAR - RAR -> Offline (force those replicas out of isr)
      //9. replicas in OAR - RAR -> NonExistentReplica (force those replicas to be deleted)
      stopOldReplicasOfReassignedPartition(topicPartition, reassignedPartitionContext, oldReplicas)
      //10. Update AR in ZK with RAR.
      updateAssignedReplicasForPartition(topicPartition, reassignedReplicas)
      //11. Update the /admin/reassign_partitions path in ZK to remove this partition.
      removePartitionsFromReassignedPartitions(Set(topicPartition))
      //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
      // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
      topicDeletionManager.resumeDeletionForTopics(Set(topicPartition.topic))
    }
  }

  /**
   * Trigger partition reassignment for the provided partitions if the assigned replicas are not the same as the
   * reassigned replicas (as defined in `ControllerContext.partitionsBeingReassigned`) and if the topic has not been
   * deleted.
   *
   * `partitionsBeingReassigned` must be populated with all partitions being reassigned before this method is invoked
   * as explained in the method documentation of `removePartitionFromReassignedPartitions` (which is invoked by this
   * method).
   *
   * @throws IllegalStateException if a partition is not in `partitionsBeingReassigned`
   */
  private def maybeTriggerPartitionReassignment(topicPartitions: Set[TopicPartition]) {
    val partitionsToBeRemovedFromReassignment = scala.collection.mutable.Set.empty[TopicPartition]
    topicPartitions.foreach { tp =>
      if (topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)) {
        info(s"Skipping reassignment of $tp since the topic is currently being deleted")
        partitionsToBeRemovedFromReassignment.add(tp)
      } else {
        val reassignedPartitionContext = controllerContext.partitionsBeingReassigned.get(tp).getOrElse {
          throw new IllegalStateException(s"Initiating reassign replicas for partition $tp not present in " +
            s"partitionsBeingReassigned: ${controllerContext.partitionsBeingReassigned.mkString(", ")}")
        }
        val newReplicas = reassignedPartitionContext.newReplicas
        val topic = tp.topic
        val assignedReplicas = controllerContext.partitionReplicaAssignment(tp)
        if (assignedReplicas.nonEmpty) {
          if (assignedReplicas == newReplicas) {
            info(s"Partition $tp to be reassigned is already assigned to replicas " +
              s"${newReplicas.mkString(",")}. Ignoring request for partition reassignment.")
            partitionsToBeRemovedFromReassignment.add(tp)
          } else {
            try {
              info(s"Handling reassignment of partition $tp to new replicas ${newReplicas.mkString(",")}")
              // first register ISR change listener
              reassignedPartitionContext.registerReassignIsrChangeHandler(zkClient)
              // mark topic ineligible for deletion for the partitions being reassigned
              topicDeletionManager.markTopicIneligibleForDeletion(Set(topic),
                reason = "topic reassignment in progress")
              onPartitionReassignment(tp, reassignedPartitionContext)
            } catch {
              case e: ControllerMovedException =>
                error(s"Error completing reassignment of partition $tp because controller has moved to another broker", e)
                throw e
              case e: Throwable =>
                error(s"Error completing reassignment of partition $tp", e)
                // remove the partition from the admin path to unblock the admin client
                partitionsToBeRemovedFromReassignment.add(tp)
            }
          }
        } else {
            error(s"Ignoring request to reassign partition $tp that doesn't exist.")
            partitionsToBeRemovedFromReassignment.add(tp)
        }
      }
    }
    removePartitionsFromReassignedPartitions(partitionsToBeRemovedFromReassignment)
  }

  /**
    * Attempt to elect a replica as leader for each of the given partitions.
    * @param partitions The partitions to have a new leader elected
    * @param electionType The type of election to perform
    * @param electionTrigger The reason for trigger this election
    * @return A map of failed and successful elections. The keys are the topic partitions and the corresponding values are
    *         either the exception that was thrown or new leader & ISR.
    */
  private[this] def onReplicaElection(
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    electionTrigger: ElectionTrigger
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    info(s"Starting replica leader election ($electionType) for partitions " +
      s"${partitions.mkString(",")} triggered by $electionTrigger")
    try {
      val strategy = electionType match {
        case ElectionType.PREFERRED => PreferredReplicaPartitionLeaderElectionStrategy
        case ElectionType.UNCLEAN =>
          /* Let's be conservative and only trigger unclean election if the election type is unclean and it was
           * triggered by the admin client
           */
          OfflinePartitionLeaderElectionStrategy(allowUnclean = electionTrigger == AdminClientTriggered)
      }

      // If the request is triggered administratively, ensure we have the latest
      // Leader/ISR state before beginning the election.
      if (electionTrigger == AdminClientTriggered || electionTrigger == ZkTriggered) {
        updateLeaderAndIsrCache(partitions.toSeq)
      }

      val results = partitionStateMachine.handleStateChanges(
        partitions.toSeq,
        OnlinePartition,
        Some(strategy)
      )
      if (electionTrigger != AdminClientTriggered) {
        results.foreach {
          case (tp, Left(throwable)) =>
            if (throwable.isInstanceOf[ControllerMovedException]) {
              info(s"Error completing replica leader election ($electionType) for partition $tp because controller has moved to another broker.", throwable)
              throw throwable
            } else {
              error(s"Error completing replica leader election ($electionType) for partition $tp", throwable)
            }
          case (_, Right(_)) => // Ignored; No need to log or throw exception for the success cases
        }
      }

      results
    } finally {
      if (electionTrigger != AdminClientTriggered) {
        removePartitionsFromPreferredReplicaElection(partitions, electionTrigger == AutoTriggered)
      }
    }
  }

  private def initializeControllerContext() {
    // update controller cache with delete topic information
    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    controllerContext.setLiveBrokerAndEpochs(curBrokerAndEpochs)
    info(s"Initialized broker epochs cache: ${controllerContext.liveBrokerIdAndEpochs}")
    controllerContext.allTopics = zkClient.getAllTopicsInCluster
    registerPartitionModificationsHandlers(controllerContext.allTopics.toSeq)
    zkClient.getReplicaAssignmentForTopics(controllerContext.allTopics.toSet).foreach {
      case (topicPartition, assignedReplicas) => controllerContext.updatePartitionReplicaAssignment(topicPartition, assignedReplicas)
    }
    controllerContext.partitionLeadershipInfo.clear()
    controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
    // register broker modifications handlers
    registerBrokerModificationsHandler(controllerContext.liveOrShuttingDownBrokerIds)
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache()
    // start the channel manager
    controllerChannelManager.startup()
    initializePartitionReassignment()
    info(s"Currently active brokers in the cluster: ${controllerContext.liveBrokerIds}")
    info(s"Currently shutting brokers in the cluster: ${controllerContext.shuttingDownBrokerIds}")
    info(s"Current list of topics in the cluster: ${controllerContext.allTopics}")
  }

  private def fetchPendingPreferredReplicaElections(): Set[TopicPartition] = {
    val partitionsUndergoingPreferredReplicaElection = zkClient.getPreferredReplicaElection
    // check if they are already completed or topic was deleted
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      val topicDeleted = replicas.isEmpty
      val successful =
        if (!topicDeleted) controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == replicas.head else false
      successful || topicDeleted
    }
    val pendingPreferredReplicaElectionsIgnoringTopicDeletion = partitionsUndergoingPreferredReplicaElection -- partitionsThatCompletedPreferredReplicaElection
    val pendingPreferredReplicaElectionsSkippedFromTopicDeletion = pendingPreferredReplicaElectionsIgnoringTopicDeletion.filter(partition => topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
    val pendingPreferredReplicaElections = pendingPreferredReplicaElectionsIgnoringTopicDeletion -- pendingPreferredReplicaElectionsSkippedFromTopicDeletion
    info(s"Partitions undergoing preferred replica election: ${partitionsUndergoingPreferredReplicaElection.mkString(",")}")
    info(s"Partitions that completed preferred replica election: ${partitionsThatCompletedPreferredReplicaElection.mkString(",")}")
    info(s"Skipping preferred replica election for partitions due to topic deletion: ${pendingPreferredReplicaElectionsSkippedFromTopicDeletion.mkString(",")}")
    info(s"Resuming preferred replica election for partitions: ${pendingPreferredReplicaElections.mkString(",")}")
    pendingPreferredReplicaElections
  }

  private def initializePartitionReassignment() {
    // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
    val partitionsBeingReassigned = zkClient.getPartitionReassignment
    info(s"Partitions being reassigned: $partitionsBeingReassigned")

    controllerContext.partitionsBeingReassigned ++= partitionsBeingReassigned.iterator.map { case (tp, newReplicas) =>
      val reassignIsrChangeHandler = new PartitionReassignmentIsrChangeHandler(eventManager, tp)
      tp -> ReassignedPartitionsContext(newReplicas, reassignIsrChangeHandler)
    }
  }

  private def fetchTopicDeletionsInProgress(): (Set[String], Set[String]) = {
    val topicsToBeDeleted = zkClient.getTopicDeletions.toSet
    val topicsWithOfflineReplicas = controllerContext.allTopics.filter { topic => {
      val replicasForTopic = controllerContext.replicasForTopic(topic)
      replicasForTopic.exists(r => !controllerContext.isReplicaOnline(r.replica, r.topicPartition))
    }}
    val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsBeingReassigned.keySet.map(_.topic)
    val topicsIneligibleForDeletion = topicsWithOfflineReplicas | topicsForWhichPartitionReassignmentIsInProgress
    info(s"List of topics to be deleted: ${topicsToBeDeleted.mkString(",")}")
    info(s"List of topics ineligible for deletion: ${topicsIneligibleForDeletion.mkString(",")}")
    (topicsToBeDeleted, topicsIneligibleForDeletion)
  }

  private def updateLeaderAndIsrCache(partitions: Seq[TopicPartition] = controllerContext.allPartitions.toSeq) {
    val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
    leaderIsrAndControllerEpochs.foreach { case (partition, leaderIsrAndControllerEpoch) =>
      controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
    }
  }

  private def areReplicasInIsr(partition: TopicPartition, replicas: Seq[Int]): Boolean = {
    def areReplicasInCachedIsr: Boolean = {
      controllerContext.partitionLeadershipInfo.get(partition).exists { leaderIsrAndControllerEpoch =>
        replicas.forall(leaderIsrAndControllerEpoch.leaderAndIsr.isr.contains)
      }
    }

    if (areReplicasInCachedIsr)
      true
    else {
      updateLeaderAndIsrCache(Seq(partition))
      areReplicasInCachedIsr
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicPartition: TopicPartition,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader
    // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
    // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
    val oldAndNewReplicas = controllerContext.partitionReplicaAssignment(topicPartition)
    controllerContext.updatePartitionReplicaAssignment(topicPartition, reassignedReplicas)
    if (!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is not in the new list of replicas ${reassignedReplicas.mkString(",")}. Re-electing leader")
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
    } else {
      // check if the leader is alive or not
      if (controllerContext.isReplicaOnline(currentLeader, topicPartition)) {
        info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
          s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} and is alive")
        // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
        updateLeaderEpochAndSendRequest(topicPartition, oldAndNewReplicas, reassignedReplicas)
      } else {
        info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
          s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} but is dead")
        partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
      }
    }
  }

  private def stopOldReplicasOfReassignedPartition(topicPartition: TopicPartition,
                                                   reassignedPartitionContext: ReassignedPartitionsContext,
                                                   oldReplicas: Set[Int]) {
    // first move the replica to offline state (the controller removes it from the ISR)
    val replicasToBeDeleted = oldReplicas.map(PartitionAndReplica(topicPartition, _))
    replicaStateMachine.handleStateChanges(replicasToBeDeleted.toSeq, OfflineReplica)
    // send stop replica command to the old replicas
    replicaStateMachine.handleStateChanges(replicasToBeDeleted.toSeq, ReplicaDeletionStarted)
    // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
    replicaStateMachine.handleStateChanges(replicasToBeDeleted.toSeq, ReplicaDeletionSuccessful)
    replicaStateMachine.handleStateChanges(replicasToBeDeleted.toSeq, NonExistentReplica)
  }

  private def updateAssignedReplicasForPartition(partition: TopicPartition,
                                                 replicas: Seq[Int]) {
    controllerContext.updatePartitionReplicaAssignment(partition, replicas)
    val setDataResponse = zkClient.setTopicAssignmentRaw(partition.topic, controllerContext.partitionReplicaAssignmentForTopic(partition.topic), controllerContext.epochZkVersion)
    setDataResponse.resultCode match {
      case Code.OK =>
        info(s"Updated assigned replicas for partition $partition being reassigned to ${replicas.mkString(",")}")
        // update the assigned replica list after a successful zookeeper write
        controllerContext.updatePartitionReplicaAssignment(partition, replicas)
      case Code.NONODE => throw new IllegalStateException(s"Topic ${partition.topic} doesn't exist")
      case _ => throw new KafkaException(setDataResponse.resultException.get)
    }
  }

  private def startNewReplicasForReassignedPartition(topicPartition: TopicPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext,
                                                     newReplicas: Set[Int]) {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Seq(PartitionAndReplica(topicPartition, replica)), NewReplica)
    }
  }

  private def updateLeaderEpochAndSendRequest(partition: TopicPartition, replicasToReceiveRequest: Seq[Int], newAssignedReplicas: Seq[Int]) {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    updateLeaderEpoch(partition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        try {
          brokerRequestBatch.newBatch()
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, partition,
            updatedLeaderIsrAndControllerEpoch, newAssignedReplicas, isNew = false)
          brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        } catch {
          case e: IllegalStateException =>
            handleIllegalState(e)
        }
        stateChangeLog.trace(s"Sent LeaderAndIsr request $updatedLeaderIsrAndControllerEpoch with new assigned replica " +
          s"list ${newAssignedReplicas.mkString(",")} to leader ${updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader} " +
          s"for partition being reassigned $partition")
      case None => // fail the reassignment
        stateChangeLog.error("Failed to send LeaderAndIsr request with new assigned replica list " +
          s"${newAssignedReplicas.mkString( ",")} to leader for partition being reassigned $partition")
    }
  }

  private def registerPartitionModificationsHandlers(topics: Seq[String]) = {
    topics.foreach { topic =>
      val partitionModificationsHandler = new PartitionModificationsHandler(eventManager, topic)
      partitionModificationsHandlers.put(topic, partitionModificationsHandler)
    }
    partitionModificationsHandlers.values.foreach(zkClient.registerZNodeChangeHandler)
  }

  private[controller] def unregisterPartitionModificationsHandlers(topics: Seq[String]) = {
    topics.foreach { topic =>
      partitionModificationsHandlers.remove(topic).foreach(handler => zkClient.unregisterZNodeChangeHandler(handler.path))
    }
  }

  private def unregisterPartitionReassignmentIsrChangeHandlers() {
    controllerContext.partitionsBeingReassigned.values.foreach(_.unregisterReassignIsrChangeHandler(zkClient))
  }

  /**
   * Remove partition from partitions being reassigned in ZooKeeper and ControllerContext. If the partition reassignment
   * is complete (i.e. there is no other partition with a reassignment in progress), the reassign_partitions znode
   * is deleted.
   *
   * `ControllerContext.partitionsBeingReassigned` must be populated with all partitions being reassigned before this
   * method is invoked to avoid premature deletion of the `reassign_partitions` znode.
   */
  private def removePartitionsFromReassignedPartitions(partitionsToBeRemoved: Set[TopicPartition]) {
    partitionsToBeRemoved.map(controllerContext.partitionsBeingReassigned).foreach { reassignContext =>
      reassignContext.unregisterReassignIsrChangeHandler(zkClient)
    }

    val updatedPartitionsBeingReassigned = controllerContext.partitionsBeingReassigned -- partitionsToBeRemoved

    info(s"Removing partitions $partitionsToBeRemoved from the list of reassigned partitions in zookeeper")

    // write the new list to zookeeper
    if (updatedPartitionsBeingReassigned.isEmpty) {
      info(s"No more partitions need to be reassigned. Deleting zk path ${ReassignPartitionsZNode.path}")
      zkClient.deletePartitionReassignment(controllerContext.epochZkVersion)
      // Ensure we detect future reassignments
      eventManager.put(PartitionReassignment)
    } else {
      val reassignment = updatedPartitionsBeingReassigned.mapValues(_.newReplicas)
      try zkClient.setOrCreatePartitionReassignment(reassignment, controllerContext.epochZkVersion)
      catch {
        case e: KeeperException => throw new AdminOperationException(e)
      }
    }

    controllerContext.partitionsBeingReassigned --= partitionsToBeRemoved
  }

  private def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicPartition],
                                                           isTriggeredByAutoRebalance : Boolean) {
    for (partition <- partitionsToBeRemoved) {
      // check the status
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if (currentLeader == preferredReplica) {
        info(s"Partition $partition completed preferred replica leader election. New leader is $preferredReplica")
      } else {
        warn(s"Partition $partition failed to complete preferred replica leader election to $preferredReplica. " +
          s"Leader is still $currentLeader")
      }
    }
    if (!isTriggeredByAutoRebalance) {
      zkClient.deletePreferredReplicaElection(controllerContext.epochZkVersion)
      // Ensure we detect future preferred replica leader elections
      eventManager.put(ReplicaLeaderElection(None, ElectionType.PREFERRED, ZkTriggered))
    }
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   *
   * @param brokers The brokers that the update metadata request should be sent to
   */
  private[controller] def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicPartition]) {
    try {
      brokerRequestBatch.newBatch()
      brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   *
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  private def updateLeaderEpoch(partition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    debug(s"Updating leader epoch for partition $partition")
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      zkWriteCompleteOrUnnecessary = zkClient.getTopicPartitionStates(Seq(partition)).get(partition) match {
        case Some(leaderIsrAndControllerEpoch) =>
          val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
          if (controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably " +
              s"means the current controller with epoch $epoch went through a soft failure and another " +
              s"controller was elected with epoch $controllerEpoch. Aborting state change by this controller")
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = leaderAndIsr.newEpochAndZkVersion
          // update the new leadership decision in zookeeper or retry
          val UpdateLeaderAndIsrResult(finishedUpdates, _) =
            zkClient.updateLeaderAndIsr(immutable.Map(partition -> newLeaderAndIsr), epoch, controllerContext.epochZkVersion)

          finishedUpdates.headOption.exists {
            case (_, Right(leaderAndIsr)) =>
              finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, epoch))
              info(s"Updated leader epoch for partition $partition to ${leaderAndIsr.leaderEpoch}")
              true
            case (_, Left(e)) =>
              throw e
          }
        case None =>
          throw new IllegalStateException(s"Cannot update leader epoch for partition $partition as " +
            "leaderAndIsr path is empty. This could mean we somehow tried to reassign a partition that doesn't exist")
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  private def checkAndTriggerAutoLeaderRebalance(): Unit = {
    trace("Checking need to trigger auto leader balancing")
    val preferredReplicasForTopicsByBrokers: Map[Int, Map[TopicPartition, Seq[Int]]] =
      controllerContext.allPartitions.filterNot {
        tp => topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
      }.map { tp =>
        (tp, controllerContext.partitionReplicaAssignment(tp) )
      }.toMap.groupBy { case (_, assignedReplicas) => assignedReplicas.head }

    debug(s"Preferred replicas by broker $preferredReplicasForTopicsByBrokers")

    // for each broker, check if a preferred replica election needs to be triggered
    preferredReplicasForTopicsByBrokers.foreach { case (leaderBroker, topicPartitionsForBroker) =>
      val topicsNotInPreferredReplica = topicPartitionsForBroker.filter { case (topicPartition, _) =>
        val leadershipInfo = controllerContext.partitionLeadershipInfo.get(topicPartition)
        leadershipInfo.exists(_.leaderAndIsr.leader != leaderBroker)
      }
      debug(s"Topics not in preferred replica for broker $leaderBroker $topicsNotInPreferredReplica")

      val imbalanceRatio = topicsNotInPreferredReplica.size.toDouble / topicPartitionsForBroker.size
      trace(s"Leader imbalance ratio for broker $leaderBroker is $imbalanceRatio")

      // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
      // that need to be on this broker
      if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
        // do this check only if the broker is live and there are no partitions being reassigned currently
        // and preferred replica election is not in progress
        val candidatePartitions = topicsNotInPreferredReplica.keys.filter(tp => controllerContext.isReplicaOnline(leaderBroker, tp) &&
          controllerContext.partitionsBeingReassigned.isEmpty &&
          !topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic) &&
          controllerContext.allTopics.contains(tp.topic))
        onReplicaElection(candidatePartitions.toSet, ElectionType.PREFERRED, AutoTriggered)
      }
    }
  }

  private def processAutoPreferredReplicaLeaderElection(): Unit = {
    if (!isActive) return
    try {
      info("Processing automatic preferred replica leader election")
      checkAndTriggerAutoLeaderRebalance()
    } finally {
      scheduleAutoLeaderRebalanceTask(delay = config.leaderImbalanceCheckIntervalSeconds, unit = TimeUnit.SECONDS)
    }
  }

  private def processUncleanLeaderElectionEnable(): Unit = {
    if (!isActive) return
    info("Unclean leader election has been enabled by default")
    partitionStateMachine.triggerOnlinePartitionStateChange()
  }

  private def processTopicUncleanLeaderElectionEnable(topic: String): Unit = {
    if (!isActive) return
    info(s"Unclean leader election has been enabled for topic $topic")
    partitionStateMachine.triggerOnlinePartitionStateChange(topic)
  }

  private def preemptControlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    controlledShutdownCallback(Failure(new ControllerMovedException("Controller moved to another broker")))
  }

  private def processControlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    val controlledShutdownResult = Try { doControlledShutdown(id, brokerEpoch) }
    controlledShutdownCallback(controlledShutdownResult)
  }

  private def doControlledShutdown(id: Int, brokerEpoch: Long): Set[TopicPartition] = {
    if (!isActive) {
      throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
    }

    // broker epoch in the request is unknown if the controller hasn't been upgraded to use KIP-380
    // so we will keep the previous behavior and don't reject the request
    if (brokerEpoch != AbstractControlRequest.UNKNOWN_BROKER_EPOCH) {
      val cachedBrokerEpoch = controllerContext.liveBrokerIdAndEpochs(id)
      if (brokerEpoch < cachedBrokerEpoch) {
        val stateBrokerEpochErrorMessage = "Received controlled shutdown request from an old broker epoch " +
          s"$brokerEpoch for broker $id. Current broker epoch is $cachedBrokerEpoch."
        info(stateBrokerEpochErrorMessage)
        throw new StaleBrokerEpochException(stateBrokerEpochErrorMessage)
      }
    }

    info(s"Shutting down broker $id")

    if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
      throw new BrokerNotAvailableException(s"Broker id $id does not exist.")

    controllerContext.shuttingDownBrokerIds.add(id)
    debug(s"All shutting down brokers: ${controllerContext.shuttingDownBrokerIds.mkString(",")}")
    debug(s"Live brokers: ${controllerContext.liveBrokerIds.mkString(",")}")

    val partitionsToActOn = controllerContext.partitionsOnBroker(id).filter { partition =>
      controllerContext.partitionReplicaAssignment(partition).size > 1 &&
        controllerContext.partitionLeadershipInfo.contains(partition) &&
        !topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic)
    }
    val (partitionsLedByBroker, partitionsFollowedByBroker) = partitionsToActOn.partition { partition =>
      controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == id
    }
    partitionStateMachine.handleStateChanges(partitionsLedByBroker.toSeq, OnlinePartition, Some(ControlledShutdownPartitionLeaderElectionStrategy))
    try {
      brokerRequestBatch.newBatch()
      partitionsFollowedByBroker.foreach { partition =>
        brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), partition, deletePartition = false)
      }
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
    // If the broker is a follower, updates the isr in ZK and notifies the current leader
    replicaStateMachine.handleStateChanges(partitionsFollowedByBroker.map(partition =>
      PartitionAndReplica(partition, id)).toSeq, OfflineReplica)
    def replicatedPartitionsBrokerLeads() = {
      trace(s"All leaders = ${controllerContext.partitionLeadershipInfo.mkString(",")}")
      controllerContext.partitionLeadershipInfo.filter {
        case (topicPartition, leaderIsrAndControllerEpoch) =>
          !topicDeletionManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&
            leaderIsrAndControllerEpoch.leaderAndIsr.leader == id &&
            controllerContext.partitionReplicaAssignment(topicPartition).size > 1
      }.keys
    }
    replicatedPartitionsBrokerLeads().toSet
  }

  private def processLeaderAndIsrResponseReceived(leaderAndIsrResponseObj: AbstractResponse, brokerId: Int): Unit = {
    if (!isActive) return
    val leaderAndIsrResponse = leaderAndIsrResponseObj.asInstanceOf[LeaderAndIsrResponse]

    if (leaderAndIsrResponse.error != Errors.NONE) {
      stateChangeLogger.error(s"Received error in LeaderAndIsr response $leaderAndIsrResponse from broker $brokerId")
      return
    }

    val offlineReplicas = leaderAndIsrResponse.responses.asScala.collect {
      case (tp, error) if error == Errors.KAFKA_STORAGE_ERROR => tp
    }
    val onlineReplicas = leaderAndIsrResponse.responses.asScala.collect {
      case (tp, error) if error == Errors.NONE => tp
    }
    val previousOfflineReplicas = controllerContext.replicasOnOfflineDirs.getOrElse(brokerId, Set.empty[TopicPartition])
    val currentOfflineReplicas = previousOfflineReplicas -- onlineReplicas ++ offlineReplicas
    controllerContext.replicasOnOfflineDirs.put(brokerId, currentOfflineReplicas)
    val newOfflineReplicas = currentOfflineReplicas -- previousOfflineReplicas

    if (newOfflineReplicas.nonEmpty) {
      stateChangeLogger.info(s"Mark replicas ${newOfflineReplicas.mkString(",")} on broker $brokerId as offline")
      onReplicasBecomeOffline(newOfflineReplicas.map(PartitionAndReplica(_, brokerId)))
    }
  }

  private def processTopicDeletionStopReplicaResponseReceived(replicaId: Int,
                                                              requestError: Errors,
                                                              partitionErrors: Map[TopicPartition, Errors]): Unit = {
    if (!isActive) return
    debug(s"Delete topic callback invoked on StopReplica response received from broker $replicaId: " +
      s"request error = $requestError, partition errors = $partitionErrors")

    val partitionsInError = if (requestError != Errors.NONE)
      partitionErrors.keySet
    else
      partitionErrors.filter { case (_, error) => error != Errors.NONE }.keySet

    val replicasInError = partitionsInError.map(PartitionAndReplica(_, replicaId))
    // move all the failed replicas to ReplicaDeletionIneligible
    topicDeletionManager.failReplicaDeletion(replicasInError)
    if (replicasInError.size != partitionErrors.size) {
      // some replicas could have been successfully deleted
      val deletedReplicas = partitionErrors.keySet -- partitionsInError
      topicDeletionManager.completeReplicaDeletion(deletedReplicas.map(PartitionAndReplica(_, replicaId)))
    }
  }

  private def processStartup(): Unit = {
    zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
    elect()
  }

  private def updateMetrics(): Unit = {
    offlinePartitionCount =
      if (!isActive) {
        0
      } else {
        controllerContext.offlinePartitionCount
      }

    preferredReplicaImbalanceCount =
      if (!isActive) {
        0
      } else {
        controllerContext.allPartitions.count { topicPartition =>
          val replicas = controllerContext.partitionReplicaAssignment(topicPartition)
          val preferredReplicaOpt = replicas.headOption
          preferredReplicaOpt.exists { preferredReplica =>
            val leadershipInfo = controllerContext.partitionLeadershipInfo.get(topicPartition)
            leadershipInfo.exists(_.leaderAndIsr.leader != preferredReplica) &&
              !topicDeletionManager.isTopicQueuedUpForDeletion(topicPartition.topic)
          }
        }
      }

    globalTopicCount = if (!isActive) 0 else controllerContext.allTopics.size

    globalPartitionCount = if (!isActive) 0 else controllerContext.partitionLeadershipInfo.size
  }

  // visible for testing
  private[controller] def handleIllegalState(e: IllegalStateException): Nothing = {
    // Resign if the controller is in an illegal state
    error("Forcing the controller to resign")
    brokerRequestBatch.clear()
    triggerControllerMove()
    throw e
  }

  private def triggerControllerMove(): Unit = {
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    if (!isActive) {
      warn("Controller has already moved when trying to trigger controller movement")
      return
    }
    try {
      val expectedControllerEpochZkVersion = controllerContext.epochZkVersion
      activeControllerId = -1
      onControllerResignation()
      zkClient.deleteController(expectedControllerEpochZkVersion)
    } catch {
      case _: ControllerMovedException =>
        warn("Controller has already moved when trying to trigger controller movement")
    }
  }

  private def maybeResign(): Unit = {
    val wasActiveBeforeChange = isActive
    zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    if (wasActiveBeforeChange && !isActive) {
      onControllerResignation()
    }
  }

  private def elect(): Unit = {
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    /*
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition,
     * it's possible that the controller has already been elected when we get here. This check will prevent the following
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    if (activeControllerId != -1) {
      debug(s"Broker $activeControllerId has been elected as the controller, so stopping the election process.")
      return
    }

    try {
      val (epoch, epochZkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(config.brokerId)
      controllerContext.epoch = epoch
      controllerContext.epochZkVersion = epochZkVersion
      activeControllerId = config.brokerId

      info(s"${config.brokerId} successfully elected as the controller. Epoch incremented to ${controllerContext.epoch} " +
        s"and epoch zk version is now ${controllerContext.epochZkVersion}")

      onControllerFailover()
    } catch {
      case e: ControllerMovedException =>
        maybeResign()

        if (activeControllerId != -1)
          debug(s"Broker $activeControllerId was elected as controller instead of broker ${config.brokerId}", e)
        else
          warn("A controller has been elected but just resigned, this will result in another round of election", e)

      case t: Throwable =>
        error(s"Error while electing or becoming controller on broker ${config.brokerId}. " +
          s"Trigger controller movement immediately", t)
        triggerControllerMove()
    }
  }

  private def processBrokerChange(): Unit = {
    if (!isActive) return
    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    val curBrokerIdAndEpochs = curBrokerAndEpochs map { case (broker, epoch) => (broker.id, epoch) }
    val curBrokerIds = curBrokerIdAndEpochs.keySet
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
    val newBrokerIds = curBrokerIds -- liveOrShuttingDownBrokerIds
    val deadBrokerIds = liveOrShuttingDownBrokerIds -- curBrokerIds
    val bouncedBrokerIds = (curBrokerIds & liveOrShuttingDownBrokerIds)
      .filter(brokerId => curBrokerIdAndEpochs(brokerId) > controllerContext.liveBrokerIdAndEpochs(brokerId))
    val newBrokerAndEpochs = curBrokerAndEpochs.filterKeys(broker => newBrokerIds.contains(broker.id))
    val bouncedBrokerAndEpochs = curBrokerAndEpochs.filterKeys(broker => bouncedBrokerIds.contains(broker.id))
    val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
    val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
    val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
    val bouncedBrokerIdsSorted = bouncedBrokerIds.toSeq.sorted
    info(s"Newly added brokers: ${newBrokerIdsSorted.mkString(",")}, " +
      s"deleted brokers: ${deadBrokerIdsSorted.mkString(",")}, " +
      s"bounced brokers: ${bouncedBrokerIdsSorted.mkString(",")}, " +
      s"all live brokers: ${liveBrokerIdsSorted.mkString(",")}")

    newBrokerAndEpochs.keySet.foreach(controllerChannelManager.addBroker)
    bouncedBrokerIds.foreach(controllerChannelManager.removeBroker)
    bouncedBrokerAndEpochs.keySet.foreach(controllerChannelManager.addBroker)
    deadBrokerIds.foreach(controllerChannelManager.removeBroker)
    if (newBrokerIds.nonEmpty) {
      controllerContext.addLiveBrokersAndEpochs(newBrokerAndEpochs)
      onBrokerStartup(newBrokerIdsSorted)
    }
    if (bouncedBrokerIds.nonEmpty) {
      controllerContext.removeLiveBrokers(bouncedBrokerIds)
      onBrokerFailure(bouncedBrokerIdsSorted)
      controllerContext.addLiveBrokersAndEpochs(bouncedBrokerAndEpochs)
      onBrokerStartup(bouncedBrokerIdsSorted)
    }
    if (deadBrokerIds.nonEmpty) {
      controllerContext.removeLiveBrokers(deadBrokerIds)
      onBrokerFailure(deadBrokerIdsSorted)
    }

    if (newBrokerIds.nonEmpty || deadBrokerIds.nonEmpty || bouncedBrokerIds.nonEmpty) {
      info(s"Updated broker epochs cache: ${controllerContext.liveBrokerIdAndEpochs}")
    }
  }

  private def processBrokerModification(brokerId: Int): Unit = {
    if (!isActive) return
    val newMetadataOpt = zkClient.getBroker(brokerId)
    val oldMetadataOpt = controllerContext.liveOrShuttingDownBroker(brokerId)
    if (newMetadataOpt.nonEmpty && oldMetadataOpt.nonEmpty) {
      val oldMetadata = oldMetadataOpt.get
      val newMetadata = newMetadataOpt.get
      if (newMetadata.endPoints != oldMetadata.endPoints) {
        info(s"Updated broker metadata: $oldMetadata -> $newMetadata")
        controllerContext.updateBrokerMetadata(oldMetadata, newMetadata)
        onBrokerUpdate(brokerId)
      }
    }
  }

  private def processTopicChange(): Unit = {
    if (!isActive) return
    val topics = zkClient.getAllTopicsInCluster
    val newTopics = topics -- controllerContext.allTopics
    val deletedTopics = controllerContext.allTopics -- topics
    controllerContext.allTopics = topics

    registerPartitionModificationsHandlers(newTopics.toSeq)
    val addedPartitionReplicaAssignment = zkClient.getReplicaAssignmentForTopics(newTopics)
    deletedTopics.foreach(controllerContext.removeTopic)
    addedPartitionReplicaAssignment.foreach {
      case (topicAndPartition, newReplicas) => controllerContext.updatePartitionReplicaAssignment(topicAndPartition, newReplicas)
    }
    info(s"New topics: [$newTopics], deleted topics: [$deletedTopics], new partition replica assignment " +
      s"[$addedPartitionReplicaAssignment]")
    if (addedPartitionReplicaAssignment.nonEmpty)
      onNewPartitionCreation(addedPartitionReplicaAssignment.keySet)
  }

  private def processLogDirEventNotification(): Unit = {
    if (!isActive) return
    val sequenceNumbers = zkClient.getAllLogDirEventNotifications
    try {
      val brokerIds = zkClient.getBrokerIdsFromLogDirEvents(sequenceNumbers)
      onBrokerLogDirFailure(brokerIds)
    } finally {
      // delete processed children
      zkClient.deleteLogDirEventNotifications(sequenceNumbers, controllerContext.epochZkVersion)
    }
  }

  private def processPartitionModifications(topic: String): Unit = {
    def restorePartitionReplicaAssignment(topic: String, newPartitionReplicaAssignment : immutable.Map[TopicPartition, Seq[Int]]): Unit = {
      info("Restoring the partition replica assignment for topic %s".format(topic))

      val existingPartitions = zkClient.getChildren(TopicPartitionsZNode.path(topic))
      val existingPartitionReplicaAssignment = newPartitionReplicaAssignment.filter(p =>
        existingPartitions.contains(p._1.partition.toString))

      zkClient.setTopicAssignment(topic, existingPartitionReplicaAssignment, controllerContext.epochZkVersion)
    }

    if (!isActive) return
    val partitionReplicaAssignment = zkClient.getReplicaAssignmentForTopics(immutable.Set(topic))
    val partitionsToBeAdded = partitionReplicaAssignment.filter { case (topicPartition, _) =>
      controllerContext.partitionReplicaAssignment(topicPartition).isEmpty
    }
    if (topicDeletionManager.isTopicQueuedUpForDeletion(topic))
      if (partitionsToBeAdded.nonEmpty) {
        warn("Skipping adding partitions %s for topic %s since it is currently being deleted"
          .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))

        restorePartitionReplicaAssignment(topic, partitionReplicaAssignment)
      } else {
        // This can happen if existing partition replica assignment are restored to prevent increasing partition count during topic deletion
        info("Ignoring partition change during topic deletion as no new partitions are added")
      }
    else {
      if (partitionsToBeAdded.nonEmpty) {
        info(s"New partitions to be added $partitionsToBeAdded")
        partitionsToBeAdded.foreach { case (topicPartition, assignedReplicas) =>
          controllerContext.updatePartitionReplicaAssignment(topicPartition, assignedReplicas)
        }
        onNewPartitionCreation(partitionsToBeAdded.keySet)
      }
    }
  }

  private def processTopicDeletion(): Unit = {
    if (!isActive) return
    var topicsToBeDeleted = zkClient.getTopicDeletions.toSet
    debug(s"Delete topics listener fired for topics ${topicsToBeDeleted.mkString(",")} to be deleted")
    val nonExistentTopics = topicsToBeDeleted -- controllerContext.allTopics
    if (nonExistentTopics.nonEmpty) {
      warn(s"Ignoring request to delete non-existing topics ${nonExistentTopics.mkString(",")}")
      zkClient.deleteTopicDeletions(nonExistentTopics.toSeq, controllerContext.epochZkVersion)
    }
    topicsToBeDeleted --= nonExistentTopics
    if (config.deleteTopicEnable) {
      if (topicsToBeDeleted.nonEmpty) {
        info(s"Starting topic deletion for topics ${topicsToBeDeleted.mkString(",")}")
        // mark topic ineligible for deletion if other state changes are in progress
        topicsToBeDeleted.foreach { topic =>
          val partitionReassignmentInProgress =
            controllerContext.partitionsBeingReassigned.keySet.map(_.topic).contains(topic)
          if (partitionReassignmentInProgress)
            topicDeletionManager.markTopicIneligibleForDeletion(Set(topic),
              reason = "topic reassignment in progress")
        }
        // add topic to deletion list
        topicDeletionManager.enqueueTopicsForDeletion(topicsToBeDeleted)
      }
    } else {
      // If delete topic is disabled remove entries under zookeeper path : /admin/delete_topics
      info(s"Removing $topicsToBeDeleted since delete topic is disabled")
      zkClient.deleteTopicDeletions(topicsToBeDeleted.toSeq, controllerContext.epochZkVersion)
    }
  }

  private def processPartitionReassignment(): Unit = {
    if (!isActive) return

    // We need to register the watcher if the path doesn't exist in order to detect future reassignments and we get
    // the `path exists` check for free
    if (zkClient.registerZNodeChangeHandlerAndCheckExistence(partitionReassignmentHandler)) {
      val partitionReassignment = zkClient.getPartitionReassignment

      // Populate `partitionsBeingReassigned` with all partitions being reassigned before invoking
      // `maybeTriggerPartitionReassignment` (see method documentation for the reason)
      partitionReassignment.foreach { case (tp, newReplicas) =>
        val reassignIsrChangeHandler = new PartitionReassignmentIsrChangeHandler(eventManager, tp)
        controllerContext.partitionsBeingReassigned.put(tp, ReassignedPartitionsContext(newReplicas, reassignIsrChangeHandler))
      }

      maybeTriggerPartitionReassignment(partitionReassignment.keySet)
    }
  }

  private def processPartitionReassignmentIsrChange(partition: TopicPartition): Unit = {
    if (!isActive) return
    // check if this partition is still being reassigned or not
    controllerContext.partitionsBeingReassigned.get(partition).foreach { reassignedPartitionContext =>
      val reassignedReplicas = reassignedPartitionContext.newReplicas.toSet
      zkClient.getTopicPartitionStates(Seq(partition)).get(partition) match {
        case Some(leaderIsrAndControllerEpoch) => // check if new replicas have joined ISR
          val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
          val caughtUpReplicas = reassignedReplicas & leaderAndIsr.isr.toSet
          if (caughtUpReplicas == reassignedReplicas) {
            // resume the partition reassignment process
            info(s"${caughtUpReplicas.size}/${reassignedReplicas.size} replicas have caught up with the leader for " +
              s"partition $partition being reassigned. Resuming partition reassignment")
            onPartitionReassignment(partition, reassignedPartitionContext)
          }
          else {
            info(s"${caughtUpReplicas.size}/${reassignedReplicas.size} replicas have caught up with the leader for " +
              s"partition $partition being reassigned. Replica(s) " +
              s"${(reassignedReplicas -- leaderAndIsr.isr.toSet).mkString(",")} still need to catch up")
          }
        case None => error(s"Error handling reassignment of partition $partition to replicas " +
          s"${reassignedReplicas.mkString(",")} as it was never created")
      }
    }
  }

  private def processIsrChangeNotification(): Unit = {
    def processUpdateNotifications(partitions: Seq[TopicPartition]) {
      val liveBrokers: Seq[Int] = controllerContext.liveOrShuttingDownBrokerIds.toSeq
      debug(s"Sending MetadataRequest to Brokers: $liveBrokers for TopicPartitions: $partitions")
      sendUpdateMetadataRequest(liveBrokers, partitions.toSet)
    }

    if (!isActive) return
    val sequenceNumbers = zkClient.getAllIsrChangeNotifications
    try {
      val partitions = zkClient.getPartitionsFromIsrChangeNotifications(sequenceNumbers)
      if (partitions.nonEmpty) {
        updateLeaderAndIsrCache(partitions)
        processUpdateNotifications(partitions)
      }
    } finally {
      // delete the notifications
      zkClient.deleteIsrChangeNotifications(sequenceNumbers, controllerContext.epochZkVersion)
    }
  }

  def electLeaders(
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    callback: ElectLeadersCallback
  ): Unit = {
    eventManager.put(ReplicaLeaderElection(Some(partitions), electionType, AdminClientTriggered, callback))
  }

  private def preemptReplicaLeaderElection(
    partitionsFromAdminClientOpt: Option[Set[TopicPartition]],
    callback: ElectLeadersCallback
  ): Unit = {
    callback(
      partitionsFromAdminClientOpt.fold(Map.empty[TopicPartition, Either[ApiError, Int]]) { partitions =>
        partitions.map(partition => partition -> Left(new ApiError(Errors.NOT_CONTROLLER, null)))(breakOut)
      }
    )
  }

  private def processReplicaLeaderElection(
    partitionsFromAdminClientOpt: Option[Set[TopicPartition]],
    electionType: ElectionType,
    electionTrigger: ElectionTrigger,
    callback: ElectLeadersCallback
  ): Unit = {
    if (!isActive) {
      callback(partitionsFromAdminClientOpt.fold(Map.empty[TopicPartition, Either[ApiError, Int]]) { partitions =>
        partitions.map(partition => partition -> Left(new ApiError(Errors.NOT_CONTROLLER, null)))(breakOut)
      })
    } else {
      // We need to register the watcher if the path doesn't exist in order to detect future preferred replica
      // leader elections and we get the `path exists` check for free
      if (electionTrigger == AdminClientTriggered || zkClient.registerZNodeChangeHandlerAndCheckExistence(preferredReplicaElectionHandler)) {
        val partitions = partitionsFromAdminClientOpt.getOrElse(zkClient.getPreferredReplicaElection)
        val (knownPartitions, unknownPartitions) = partitions.partition(tp => controllerContext.allPartitions.contains(tp))
        unknownPartitions.foreach { p =>
          info(s"Skipping replica leader election ($electionType) for partition $p by $electionTrigger since it doesn't exist.")
        }

        val (partitionsBeingDeleted, livePartitions) = knownPartitions.partition(partition =>
            topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
        if (partitionsBeingDeleted.nonEmpty) {
          warn(s"Skipping replica leader election ($electionType) for partitions $partitionsBeingDeleted " +
            s"by $electionTrigger since the respective topics are being deleted")
        }

        // partition those that have a valid leader
        val (electablePartitions, alreadyValidLeader) = livePartitions.partition { partition =>
          electionType match {
            case ElectionType.PREFERRED =>
              val assignedReplicas = controllerContext.partitionReplicaAssignment(partition)
              val preferredReplica = assignedReplicas.head
              val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
              currentLeader != preferredReplica

            case ElectionType.UNCLEAN =>
              val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
              currentLeader == LeaderAndIsr.NoLeader || !controllerContext.liveBrokerIds.contains(currentLeader)
          }
        }

        val results = onReplicaElection(electablePartitions, electionType, electionTrigger).mapValues {
          case Left(ex) =>
            if (ex.isInstanceOf[StateChangeFailedException]) {
              val error = if (electionType == ElectionType.PREFERRED) {
                Errors.PREFERRED_LEADER_NOT_AVAILABLE
              } else {
                Errors.ELIGIBLE_LEADERS_NOT_AVAILABLE
              }
              Left(new ApiError(error, ex.getMessage))
            } else {
              Left(ApiError.fromThrowable(ex))
            }
          case Right(leaderAndIsr) => Right(leaderAndIsr.leader)
        } ++
        alreadyValidLeader.map(_ -> Left(new ApiError(Errors.ELECTION_NOT_NEEDED))) ++
        partitionsBeingDeleted.map(
          _ -> Left(new ApiError(Errors.INVALID_TOPIC_EXCEPTION, "The topic is being deleted"))
        ) ++
        unknownPartitions.map(
          _ -> Left(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist."))
        )

        debug(s"Waiting for any successful result for election type ($electionType) by $electionTrigger for partitions: $results")
        callback(results)
      }
    }
  }

  private def processControllerChange(): Unit = {
    maybeResign()
  }

  private def processReelect(): Unit = {
    maybeResign()
    elect()
  }

  private def processRegisterBrokerAndReelect(): Unit = {
    _brokerEpoch = zkClient.registerBroker(brokerInfo)
    processReelect()
  }

  private def processExpire(): Unit = {
    activeControllerId = -1
    onControllerResignation()
  }


  override def process(event: ControllerEvent): Unit = {
    try {
      event match {
        case event: MockEvent =>
          // Used only in test cases
          event.process()
        case ShutdownEventThread =>
          error("Received a ShutdownEventThread event. This type of event is supposed to be handle by ControllerEventThread")
        case AutoPreferredReplicaLeaderElection =>
          processAutoPreferredReplicaLeaderElection()
        case ReplicaLeaderElection(partitions, electionType, electionTrigger, callback) =>
          processReplicaLeaderElection(partitions, electionType, electionTrigger, callback)
        case UncleanLeaderElectionEnable =>
          processUncleanLeaderElectionEnable()
        case TopicUncleanLeaderElectionEnable(topic) =>
          processTopicUncleanLeaderElectionEnable(topic)
        case ControlledShutdown(id, brokerEpoch, callback) =>
          processControlledShutdown(id, brokerEpoch, callback)
        case LeaderAndIsrResponseReceived(response, brokerId) =>
          processLeaderAndIsrResponseReceived(response, brokerId)
        case TopicDeletionStopReplicaResponseReceived(replicaId, requestError, partitionErrors) =>
          processTopicDeletionStopReplicaResponseReceived(replicaId, requestError, partitionErrors)
        case BrokerChange =>
          processBrokerChange()
        case BrokerModifications(brokerId) =>
          processBrokerModification(brokerId)
        case ControllerChange =>
          processControllerChange()
        case Reelect =>
          processReelect()
        case RegisterBrokerAndReelect =>
          processRegisterBrokerAndReelect()
        case Expire =>
          processExpire()
        case TopicChange =>
          processTopicChange()
        case LogDirEventNotification =>
          processLogDirEventNotification()
        case PartitionModifications(topic) =>
          processPartitionModifications(topic)
        case TopicDeletion =>
          processTopicDeletion()
        case PartitionReassignment =>
          processPartitionReassignment()
        case PartitionReassignmentIsrChange(partition) =>
          processPartitionReassignmentIsrChange(partition)
        case IsrChangeNotification =>
          processIsrChangeNotification()
        case Startup =>
          processStartup()
      }
    } catch {
      case e: ControllerMovedException =>
        info(s"Controller moved to another broker when processing $event.", e)
        maybeResign()
      case e: Throwable =>
        error(s"Error processing event $event", e)
    } finally {
      updateMetrics()
    }
  }

  override def preempt(event: ControllerEvent): Unit = {
    event match {
      case ReplicaLeaderElection(partitions, _, _, callback) =>
        preemptReplicaLeaderElection(partitions, callback)
      case ControlledShutdown(id, brokerEpoch, callback) =>
        preemptControlledShutdown(id, brokerEpoch, callback)
      case _ =>
    }
  }
}

class BrokerChangeHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = BrokerIdsZNode.path

  override def handleChildChange(): Unit = {
    eventManager.put(BrokerChange)
  }
}

class BrokerModificationsHandler(eventManager: ControllerEventManager, brokerId: Int) extends ZNodeChangeHandler {
  override val path: String = BrokerIdZNode.path(brokerId)

  override def handleDataChange(): Unit = {
    eventManager.put(BrokerModifications(brokerId))
  }
}

class TopicChangeHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = TopicsZNode.path

  override def handleChildChange(): Unit = eventManager.put(TopicChange)
}

class LogDirEventNotificationHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = LogDirEventNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(LogDirEventNotification)
}

object LogDirEventNotificationHandler {
  val Version: Long = 1L
}

class PartitionModificationsHandler(eventManager: ControllerEventManager, topic: String) extends ZNodeChangeHandler {
  override val path: String = TopicZNode.path(topic)

  override def handleDataChange(): Unit = eventManager.put(PartitionModifications(topic))
}

class TopicDeletionHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = DeleteTopicsZNode.path

  override def handleChildChange(): Unit = eventManager.put(TopicDeletion)
}

class PartitionReassignmentHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = ReassignPartitionsZNode.path

  // Note that the event is also enqueued when the znode is deleted, but we do it explicitly instead of relying on
  // handleDeletion(). This approach is more robust as it doesn't depend on the watcher being re-registered after
  // it's consumed during data changes (we ensure re-registration when the znode is deleted).
  override def handleCreation(): Unit = eventManager.put(PartitionReassignment)
}

class PartitionReassignmentIsrChangeHandler(eventManager: ControllerEventManager, partition: TopicPartition) extends ZNodeChangeHandler {
  override val path: String = TopicPartitionStateZNode.path(partition)

  override def handleDataChange(): Unit = eventManager.put(PartitionReassignmentIsrChange(partition))
}

class IsrChangeNotificationHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = IsrChangeNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(IsrChangeNotification)
}

object IsrChangeNotificationHandler {
  val Version: Long = 1L
}

class PreferredReplicaElectionHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = PreferredReplicaElectionZNode.path

  override def handleCreation(): Unit = eventManager.put(ReplicaLeaderElection(None, ElectionType.PREFERRED, ZkTriggered))
}

class ControllerChangeHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = ControllerZNode.path

  override def handleCreation(): Unit = eventManager.put(ControllerChange)
  override def handleDeletion(): Unit = eventManager.put(Reelect)
  override def handleDataChange(): Unit = eventManager.put(ControllerChange)
}

case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       reassignIsrChangeHandler: PartitionReassignmentIsrChangeHandler) {

  def registerReassignIsrChangeHandler(zkClient: KafkaZkClient): Unit =
    zkClient.registerZNodeChangeHandler(reassignIsrChangeHandler)

  def unregisterReassignIsrChangeHandler(zkClient: KafkaZkClient): Unit =
    zkClient.unregisterZNodeChangeHandler(reassignIsrChangeHandler.path)

}

case class PartitionAndReplica(topicPartition: TopicPartition, replica: Int) {
  def topic: String = topicPartition.topic
  def partition: Int = topicPartition.partition

  override def toString: String = {
    s"[Topic=$topic,Partition=$partition,Replica=$replica]"
  }
}

case class LeaderIsrAndControllerEpoch(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString: String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ZkVersion:" + leaderAndIsr.zkVersion + ")")
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

private[controller] class ControllerStats extends KafkaMetricsGroup {
  val uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)

  val rateAndTimeMetrics: Map[ControllerState, KafkaTimer] = ControllerState.values.flatMap { state =>
    state.rateAndTimeMetricName.map { metricName =>
      state -> new KafkaTimer(newTimer(metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
    }
  }.toMap

}

sealed trait ControllerEvent {
  def state: ControllerState
}

case object ControllerChange extends ControllerEvent {
  override def state = ControllerState.ControllerChange
}

case object Reelect extends ControllerEvent {
  override def state = ControllerState.ControllerChange
}

case object RegisterBrokerAndReelect extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
}

case object Expire extends ControllerEvent {
  override def state = ControllerState.ControllerChange
}

case object ShutdownEventThread extends ControllerEvent {
  def state = ControllerState.ControllerShutdown
}

case object AutoPreferredReplicaLeaderElection extends ControllerEvent {
  def state = ControllerState.AutoLeaderBalance
}

case object UncleanLeaderElectionEnable extends ControllerEvent {
  def state = ControllerState.UncleanLeaderElectionEnable
}

case class TopicUncleanLeaderElectionEnable(topic: String) extends ControllerEvent {
  def state = ControllerState.TopicUncleanLeaderElectionEnable
}

case class ControlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit) extends ControllerEvent {
  def state = ControllerState.ControlledShutdown
}

case class LeaderAndIsrResponseReceived(LeaderAndIsrResponseObj: AbstractResponse, brokerId: Int) extends ControllerEvent {
  def state = ControllerState.LeaderAndIsrResponseReceived
}

case class TopicDeletionStopReplicaResponseReceived(replicaId: Int,
                                                    requestError: Errors,
                                                    partitionErrors: Map[TopicPartition, Errors]) extends ControllerEvent {
  def state = ControllerState.TopicDeletion
}

case object Startup extends ControllerEvent {
  def state = ControllerState.ControllerChange
}

case object BrokerChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.BrokerChange
}

case class BrokerModifications(brokerId: Int) extends ControllerEvent {
  override def state: ControllerState = ControllerState.BrokerChange
}

case object TopicChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicChange
}

case object LogDirEventNotification extends ControllerEvent {
  override def state: ControllerState = ControllerState.LogDirChange
}

case class PartitionModifications(topic: String) extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicChange
}

case object TopicDeletion extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicDeletion
}

case object PartitionReassignment extends ControllerEvent {
  override def state: ControllerState = ControllerState.PartitionReassignment
}

case class PartitionReassignmentIsrChange(partition: TopicPartition) extends ControllerEvent {
  override def state: ControllerState = ControllerState.PartitionReassignment
}

case object IsrChangeNotification extends ControllerEvent {
  override def state: ControllerState = ControllerState.IsrChange
}

case class ReplicaLeaderElection(
  partitionsFromAdminClientOpt: Option[Set[TopicPartition]],
  electionType: ElectionType,
  electionTrigger: ElectionTrigger,
  callback: ElectLeadersCallback = _ => {}
) extends ControllerEvent {
  override def state: ControllerState = ControllerState.ManualLeaderBalance
}

// Used only in test cases
abstract class MockEvent(val state: ControllerState) extends ControllerEvent {
  def process(): Unit
}
