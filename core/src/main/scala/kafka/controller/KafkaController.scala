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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.admin.AdminOperationException
import kafka.api._
import kafka.common._
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server._
import kafka.utils._
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk._
import kafka.zookeeper.{StateChangeHandler, ZNodeChangeHandler, ZNodeChildChangeHandler}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException, StaleBrokerEpochException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractControlRequest, AbstractResponse, ApiError, LeaderAndIsrResponse, StopReplicaResponse}
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.{mutable, _}
import scala.util.{Failure, Try}

object KafkaController extends Logging {
  val InitialControllerEpoch = 0
  val InitialControllerEpochZkVersion = 0

  /**
   * ControllerEventThread will shutdown once it sees this event
   */
  private[controller] case object ShutdownEventThread extends ControllerEvent {
    def state = ControllerState.ControllerShutdown
    override def process(): Unit = ()
  }

  // Used only by test
  private[controller] case class AwaitOnLatch(latch: CountDownLatch) extends ControllerEvent {
    override def state: ControllerState = ControllerState.ControllerChange
    override def process(): Unit = latch.await()
  }

}

class KafkaController(val config: KafkaConfig, zkClient: KafkaZkClient, time: Time, metrics: Metrics,
                      initialBrokerInfo: BrokerInfo, initialBrokerEpoch: Long, tokenManager: DelegationTokenManager,
                      threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {

  this.logIdent = s"[Controller id=${config.brokerId}] "

  @volatile private var brokerInfo = initialBrokerInfo
  @volatile private var _brokerEpoch = initialBrokerEpoch

  private val stateChangeLogger = new StateChangeLogger(config.brokerId, inControllerContext = true, None)
  val controllerContext = new ControllerContext

  // have a separate scheduler for the controller to be able to start and stop independently of the kafka server
  // visible for testing
  private[controller] val kafkaScheduler = new KafkaScheduler(1)

  // visible for testing
  private[controller] val eventManager = new ControllerEventManager(config.brokerId,
    controllerContext.stats.rateAndTimeMetrics, _ => updateMetrics(), () => maybeResign())

  val topicDeletionManager = new TopicDeletionManager(this, eventManager, zkClient)
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(this, stateChangeLogger)
  val replicaStateMachine = new ReplicaStateMachine(config, stateChangeLogger, controllerContext, topicDeletionManager, zkClient, mutable.Map.empty, new ControllerBrokerRequestBatch(this, stateChangeLogger))
  val partitionStateMachine = new PartitionStateMachine(config, stateChangeLogger, controllerContext, zkClient, mutable.Map.empty, new ControllerBrokerRequestBatch(this, stateChangeLogger))
  partitionStateMachine.setTopicDeletionManager(topicDeletionManager)

  private val controllerChangeHandler = new ControllerChangeHandler(this, eventManager)
  private val brokerChangeHandler = new BrokerChangeHandler(this, eventManager)
  private val brokerModificationsHandlers: mutable.Map[Int, BrokerModificationsHandler] = mutable.Map.empty
  private val topicChangeHandler = new TopicChangeHandler(this, eventManager)
  private val topicDeletionHandler = new TopicDeletionHandler(this, eventManager)
  private val partitionModificationsHandlers: mutable.Map[String, PartitionModificationsHandler] = mutable.Map.empty
  private val partitionReassignmentHandler = new PartitionReassignmentHandler(this, eventManager)
  private val preferredReplicaElectionHandler = new PreferredReplicaElectionHandler(this, eventManager)
  private val isrChangeNotificationHandler = new IsrChangeNotificationHandler(this, eventManager)
  private val logDirEventNotificationHandler = new LogDirEventNotificationHandler(this, eventManager)

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
        val expireEvent = new Expire
        eventManager.clearAndPut(expireEvent)

        // Block initialization of the new session until the expiration event is being handled,
        // which ensures that all pending events have been processed before creating the new session
        expireEvent.waitUntilProcessingStarted()
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
    onPreferredReplicaElection(pendingPreferredReplicaElections, ZkTriggered)
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

    // reset topic deletion manager
    topicDeletionManager.reset()

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
        s"${topicDeletionManager.topicsToBeDeleted.mkString(",")} are on the newly restarted brokers " +
        s"${newBrokers.mkString(",")}. Signaling restart of topic deletion for these topics")
      topicDeletionManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }
    registerBrokerModificationsHandler(newBrokers)
  }

  private def registerBrokerModificationsHandler(brokerIds: Iterable[Int]): Unit = {
    debug(s"Register BrokerModifications handler for $brokerIds")
    brokerIds.foreach { brokerId =>
      val brokerModificationsHandler = new BrokerModificationsHandler(this, eventManager, brokerId)
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
    partitionStateMachine.handleStateChanges(newPartitions.toSeq, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, OnlineReplica)
  }

  private def getMinIsrSize(topic: String): Int = {
    Option(zkClient.getEntityConfigs(ConfigType.Topic, topic).getProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)).getOrElse("1").toInt
  }

  def calculateReassignmentStep(topicPartition: TopicPartition, reassignedReplicas: Seq[Int]): ReassignmentStep = {
    val replicas = getCurrentReplicas(topicPartition)
    val leader = controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader
    debug(s"Triggering reassignment step calculation. CurrentReplicas: ${replicas.mkString(",")}, leader: $leader, $reassignedReplicas")
    ReassignmentHelper.calculateReassignmentStep(reassignedReplicas, replicas, leader, controllerContext.liveBrokerIds, getMinIsrSize(topicPartition.topic()))
  }

  private def getCurrentReplicas(topicPartition: TopicPartition) = {
    controllerContext.partitionReplicaAssignment(topicPartition)
  }

  /**
   * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
   * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   *
   * NR = New Replicas: they are added in the current reassignment step
   * DR = Dropped Replicas: they will be removed in the current reassignment step
   * CR = Current assigned replicas
   * TR = Target replicas, that is CR + NR - DR.
   *
   *  1. Wait until CR is entirely in ISR. This will make sure that we're starting off with a solid base for reassignment.
   *  2. Calculate the next reassignment step based on the reassignment context.
   *  3. Wait until all target brokers are alive. This will make sure that reassignment starts only when the target
   *     brokers can perform the actual reassignment step.
   *  4. All new replicas -> OnlineReplica when they are in ISR
   *  5. Update CR in Zookeeper with TR.
   *  6. Send LeaderAndIsr request to all replicas in CR + NR.
   *  7. Start new replicas in NR by moving them to NewReplica state.
   *  8. Set CR to TR in memory.
   *  9. Send LeaderAndIsr request with a potential new leader (if current leader not in TR) and
   *     a new CR (using TR) and same isr to every broker in TR
   * 10. Replicas in DR -> Offline (force those replicas out of isr)
   * 11. Replicas in DR -> NonExistentReplica (force those replicas to be deleted)
   * 12. Update the /admin/reassign_partitions path in ZK to remove this partition.
   * 13. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
   */
  def onPartitionReassignment(topicPartition: TopicPartition, reassignedPartitionContext: ReassignedPartitionsContext): Unit = {
    val currentReplicas = getCurrentReplicas(topicPartition)
    // 1. Wait until all current replicas are in ISR
    if (atLeastOneReplicaInIsr(topicPartition, currentReplicas)) {
      // 2. Calculate the next reassigment step
      val reassignedReplicas = reassignedPartitionContext.newReplicas
      val reassignmentStep = calculateReassignmentStep(topicPartition, reassignedReplicas)

      // 3. Wait until all target brokers are alive
      val deadTargetReplicas = reassignmentStep.targetReplicas.toSet -- controllerContext.liveBrokerIds
      if (deadTargetReplicas.isEmpty) {
        info(s"New replicas ${reassignedReplicas.mkString(",")} for partition $topicPartition being reassigned not yet " +
        "caught up with the leader")
        // 4. Update AR in ZK with TR: added and removed replicas will be reflected in Zookeeper
        updateAssignedReplicasForPartition(topicPartition, reassignmentStep.targetReplicas)
        // 5. Send LeaderAndIsr request to every replica in AR + NR: add or remove will be sent out to the brokers
        val currentAndAddedReplicas = currentReplicas ++ Seq(reassignmentStep.add).flatten
        updateLeaderEpochAndSendRequest(topicPartition, currentAndAddedReplicas, reassignmentStep.targetReplicas)
        // 6. Replicas in NR -> NewReplica
        if (reassignmentStep.add.nonEmpty) {
          startNewReplicasForReassignedPartition(topicPartition, reassignmentStep.add.toSet)
          info(s"Starting new replicas ${reassignedReplicas.mkString(",")} for partition $topicPartition being " +
          "reassigned to catch up with the leader")
        }
        val newReplicasInIsr = selectIsr(replicaStateMachine.replicasInState(topicPartition.topic, NewReplica).toSeq)
        // 7. All new replicas -> OnlineReplica when they are in ISR
        replicaStateMachine.handleStateChanges(newReplicasInIsr, OnlineReplica)
        if (reassignmentStep.drop.nonEmpty) {
          // 8. Set AR to TR in memory.
          // 9. Send LeaderAndIsr request with a potential new leader (if current leader not in TR) and
          //   a new AR (using TR) and same isr to every broker in TR
          debug(s"Leader election, if needed, using remaining live replicas ${reassignmentStep.targetReplicas}")
          moveReassignedPartitionLeaderIfRequired(topicPartition, currentAndAddedReplicas, reassignmentStep.targetReplicas)
          // 10. replicas in DR -> Offline (force those replicas out of isr)
          // 11. replicas in DR -> NonExistentReplica (force those replicas to be deleted)
          if (!reassignmentStep.drop.contains(zkClient.getLeaderForPartition(topicPartition).get)) {
            debug(s"Stopping [${reassignmentStep.drop.mkString(",")}] when leader is ${zkClient.getLeaderForPartition(topicPartition).get}")
            stopOldReplicasOfReassignedPartition(topicPartition, reassignedPartitionContext, reassignmentStep.drop.toSet)
          }
        }
        val allNewReplicasInISR = areReplicasInIsr(topicPartition, reassignedReplicas)
        if (allNewReplicasInISR) {
          info("All replicas are in sync, reassign finished")
          // 12. Update the /admin/reassign_partitions path in ZK to remove this partition.
          removePartitionsFromReassignedPartitions(Set(topicPartition))
          // 13. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
          sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
          // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
          topicDeletionManager.resumeDeletionForTopics(Set(topicPartition.topic))
        }
      }
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
    // get first element of "not yet finished" list from ZK
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
              topicDeletionManager.markTopicIneligibleForDeletion(Set(topic))
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

  sealed trait ElectionType
  object AutoTriggered extends ElectionType
  object ZkTriggered extends ElectionType
  object AdminClientTriggered extends ElectionType

  /**
    * Attempt to elect the preferred replica as leader for each of the given partitions.
    * @param partitions The partitions to have their preferred leader elected
    * @param electionType The election type
    * @return A map of failed elections where keys are partitions which had an error and the corresponding value is
    *         the exception that was thrown.
    */
  private def onPreferredReplicaElection(partitions: Set[TopicPartition],
                                         electionType: ElectionType): Map[TopicPartition, Throwable] = {
    info(s"Starting preferred replica leader election for partitions ${partitions.mkString(",")}")
    try {
      val results = partitionStateMachine.handleStateChanges(partitions.toSeq, OnlinePartition,
        Option(PreferredReplicaPartitionLeaderElectionStrategy))
      if (electionType != AdminClientTriggered) {
        results.foreach { case (tp, throwable) =>
          if (throwable.isInstanceOf[ControllerMovedException]) {
            error(s"Error completing preferred replica leader election for partition $tp because controller has moved to another broker.", throwable)
            throw throwable
          } else {
            error(s"Error completing preferred replica leader election for partition $tp", throwable)
          }
        }
      }
      return results;
    } finally {
      if (electionType != AdminClientTriggered)
        removePartitionsFromPreferredReplicaElection(partitions, electionType == AutoTriggered)
    }
  }

  private def initializeControllerContext() {
    // update controller cache with delete topic information
    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    controllerContext.setLiveBrokerAndEpochs(curBrokerAndEpochs)
    info(s"Initialized broker epochs cache: ${controllerContext.liveBrokerIdAndEpochs}")
    controllerContext.allTopics = zkClient.getAllTopicsInCluster.toSet
    registerPartitionModificationsHandlers(controllerContext.allTopics.toSeq)
    zkClient.getReplicaAssignmentForTopics(controllerContext.allTopics.toSet).foreach {
      case (topicPartition, assignedReplicas) => controllerContext.updatePartitionReplicaAssignment(topicPartition, assignedReplicas)
    }
    controllerContext.partitionLeadershipInfo.clear()
    controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
    // register broker modifications handlers
    registerBrokerModificationsHandler(controllerContext.liveBrokers.map(_.id))
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache()
    // start the channel manager
    startChannelManager()
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
      val reassignIsrChangeHandler = new PartitionReassignmentIsrChangeHandler(this, eventManager, tp)
      tp -> new ReassignedPartitionsContext(newReplicas, reassignIsrChangeHandler)
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

  private def startChannelManager() {
    controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics,
      stateChangeLogger, threadNamePrefix)
    controllerContext.controllerChannelManager.startup()
  }

  private def updateLeaderAndIsrCache(partitions: Seq[TopicPartition] = controllerContext.allPartitions.toSeq) {
    val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
    leaderIsrAndControllerEpochs.foreach { case (partition, leaderIsrAndControllerEpoch) =>
      controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
    }
  }

  private def areReplicasInIsr(partition: TopicPartition, replicas: Seq[Int]): Boolean = {
    zkClient.getTopicPartitionStates(Seq(partition)).get(partition).exists { leaderIsrAndControllerEpoch =>
      replicas.forall(leaderIsrAndControllerEpoch.leaderAndIsr.isr.contains)
    }
  }

  private def selectIsr(replicas: Seq[PartitionAndReplica]): Seq[PartitionAndReplica] = {
    val replicasByPartitions = replicas.groupBy(r => r.topicPartition)
    val partitionStates = zkClient.getTopicPartitionStates(replicasByPartitions.keys.toSeq)
    replicasByPartitions.map{ case (partition, replicasForPartition) =>
      val isr = partitionStates(partition).leaderAndIsr.isr
      val inIsr = replicasForPartition.filter(r => isr.contains(r.replica))
      partition -> inIsr
    }.values.flatten.toSeq
  }

  private def atLeastOneReplicaInIsr(partition: TopicPartition, replicas: Seq[Int]): Boolean = {
    zkClient.getTopicPartitionState(partition).exists{ leaderIsrAndControllerEpoch =>
      replicas.intersect(leaderIsrAndControllerEpoch.leaderAndIsr.isr).nonEmpty
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicPartition: TopicPartition, oldAndNewReplicas: Seq[Int],
                                                      reassignedReplicas: Seq[Int]) {
    val currentLeader = controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader
    // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
    // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
    controllerContext.updatePartitionReplicaAssignment(topicPartition, reassignedReplicas)
    if (!reassignedReplicas.contains(currentLeader)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is not in the new list of replicas ${reassignedReplicas.mkString(",")}. Re-electing leader")
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Option(ReassignPartitionLeaderElectionStrategy))
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
        partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Option(ReassignPartitionLeaderElectionStrategy))
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
                                                     newReplicas: Set[Int]) {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { id =>
      replicaStateMachine.handleStateChanges(Seq(PartitionAndReplica(topicPartition, id)), NewReplica)
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
      val partitionModificationsHandler = new PartitionModificationsHandler(this, eventManager, topic)
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
      eventManager.put(PreferredReplicaLeaderElection(None))
    }
  }

  private[controller] def sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                                      callback: AbstractResponse => Unit = null) = {
    controllerContext.controllerChannelManager.sendRequest(brokerId, apiKey, request, callback)
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
          val UpdateLeaderAndIsrResult(successfulUpdates, _, failedUpdates) =
            zkClient.updateLeaderAndIsr(immutable.Map(partition -> newLeaderAndIsr), epoch, controllerContext.epochZkVersion)
          if (successfulUpdates.contains(partition)) {
            val finalLeaderAndIsr = successfulUpdates(partition)
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(finalLeaderAndIsr, epoch))
            info(s"Updated leader epoch for partition $partition to ${finalLeaderAndIsr.leaderEpoch}")
            true
          } else if (failedUpdates.contains(partition)) {
            throw failedUpdates(partition)
          } else false
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
        onPreferredReplicaElection(candidatePartitions.toSet, AutoTriggered)
      }
    }
  }

  case object AutoPreferredReplicaLeaderElection extends ControllerEvent {

    def state = ControllerState.AutoLeaderBalance

    override def process(): Unit = {
      if (!isActive) return
      try {
        checkAndTriggerAutoLeaderRebalance()
      } finally {
        scheduleAutoLeaderRebalanceTask(delay = config.leaderImbalanceCheckIntervalSeconds, unit = TimeUnit.SECONDS)
      }
    }
  }

  case object UncleanLeaderElectionEnable extends ControllerEvent {

    def state = ControllerState.UncleanLeaderElectionEnable

    override def process(): Unit = {
      if (!isActive) return
      partitionStateMachine.triggerOnlinePartitionStateChange()
    }
  }

  case class TopicUncleanLeaderElectionEnable(topic: String) extends ControllerEvent {

    def state = ControllerState.TopicUncleanLeaderElectionEnable

    override def process(): Unit = {
      if (!isActive) return
      partitionStateMachine.triggerOnlinePartitionStateChange(topic)
    }
  }

  case class ControlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit) extends PreemptableControllerEvent {

    def state = ControllerState.ControlledShutdown

    override def handlePreempt(): Unit = {
      controlledShutdownCallback(Failure(new ControllerMovedException("Controller moved to another broker")))
    }

    override def handleProcess(): Unit = {
      val controlledShutdownResult = Try { doControlledShutdown(id) }
      controlledShutdownCallback(controlledShutdownResult)
    }

    private def doControlledShutdown(id: Int): Set[TopicPartition] = {
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
      partitionStateMachine.handleStateChanges(partitionsLedByBroker.toSeq, OnlinePartition, Option(ControlledShutdownPartitionLeaderElectionStrategy))
      try {
        brokerRequestBatch.newBatch()
        partitionsFollowedByBroker.foreach { partition =>
          brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), partition, deletePartition = false,
            (_, _) => ())
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
  }

  case class LeaderAndIsrResponseReceived(LeaderAndIsrResponseObj: AbstractResponse, brokerId: Int) extends ControllerEvent {

    def state = ControllerState.LeaderAndIsrResponseReceived

    override def process(): Unit = {
      import JavaConverters._
      if (!isActive) return
      val leaderAndIsrResponse = LeaderAndIsrResponseObj.asInstanceOf[LeaderAndIsrResponse]

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
  }

  case class TopicDeletionStopReplicaResponseReceived(stopReplicaResponseObj: AbstractResponse, replicaId: Int) extends ControllerEvent {

    def state = ControllerState.TopicDeletion

    override def process(): Unit = {
      import JavaConverters._
      if (!isActive) return
      val stopReplicaResponse = stopReplicaResponseObj.asInstanceOf[StopReplicaResponse]
      debug(s"Delete topic callback invoked for $stopReplicaResponse")
      val responseMap = stopReplicaResponse.responses.asScala
      val partitionsInError =
        if (stopReplicaResponse.error != Errors.NONE) responseMap.keySet
        else responseMap.filter { case (_, error) => error != Errors.NONE }.keySet
      val replicasInError = partitionsInError.map(PartitionAndReplica(_, replicaId))
      // move all the failed replicas to ReplicaDeletionIneligible
      topicDeletionManager.failReplicaDeletion(replicasInError)
      if (replicasInError.size != responseMap.size) {
        // some replicas could have been successfully deleted
        val deletedReplicas = responseMap.keySet -- partitionsInError
        topicDeletionManager.completeReplicaDeletion(deletedReplicas.map(PartitionAndReplica(_, replicaId)))
      }
    }
  }

  case object Startup extends ControllerEvent {

    def state = ControllerState.ControllerChange

    override def process(): Unit = {
      zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
      elect()
    }

  }

  private def updateMetrics(): Unit = {
    offlinePartitionCount =
      if (!isActive) {
        0
      } else {
        partitionStateMachine.offlinePartitionCount
      }

    preferredReplicaImbalanceCount =
      if (!isActive) {
        0
      } else {
        controllerContext.allPartitions.count { topicPartition =>
          val replicas = controllerContext.partitionReplicaAssignment(topicPartition)
          val preferredReplica = replicas.head
          val leadershipInfo = controllerContext.partitionLeadershipInfo.get(topicPartition)
          leadershipInfo.map(_.leaderAndIsr.leader != preferredReplica).getOrElse(false) &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(topicPartition.topic)
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

  case object BrokerChange extends ControllerEvent {
    override def state: ControllerState = ControllerState.BrokerChange

    override def process(): Unit = {
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

      newBrokerAndEpochs.keySet.foreach(controllerContext.controllerChannelManager.addBroker)
      bouncedBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)
      bouncedBrokerAndEpochs.keySet.foreach(controllerContext.controllerChannelManager.addBroker)
      deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)
      if (newBrokerIds.nonEmpty) {
        controllerContext.addLiveBrokersAndEpochs(newBrokerAndEpochs)
        onBrokerStartup(newBrokerIdsSorted)
      }
      if (bouncedBrokerIds.nonEmpty) {
        controllerContext.removeLiveBrokersAndEpochs(bouncedBrokerIds)
        onBrokerFailure(bouncedBrokerIdsSorted)
        controllerContext.addLiveBrokersAndEpochs(bouncedBrokerAndEpochs)
        onBrokerStartup(bouncedBrokerIdsSorted)
      }
      if (deadBrokerIds.nonEmpty) {
        controllerContext.removeLiveBrokersAndEpochs(deadBrokerIds)
        onBrokerFailure(deadBrokerIdsSorted)
      }

      if (newBrokerIds.nonEmpty || deadBrokerIds.nonEmpty || bouncedBrokerIds.nonEmpty) {
        info(s"Updated broker epochs cache: ${controllerContext.liveBrokerIdAndEpochs}")
      }
    }
  }

  case class BrokerModifications(brokerId: Int) extends ControllerEvent {
    override def state: ControllerState = ControllerState.BrokerChange

    override def process(): Unit = {
      if (!isActive) return
      val newMetadata = zkClient.getBroker(brokerId)
      val oldMetadata = controllerContext.liveBrokers.find(_.id == brokerId)
      if (newMetadata.nonEmpty && oldMetadata.nonEmpty && newMetadata.map(_.endPoints) != oldMetadata.map(_.endPoints)) {
        info(s"Updated broker: ${newMetadata.get}")

        controllerContext.updateBrokerMetadata(oldMetadata, newMetadata)
        onBrokerUpdate(brokerId)
      }
    }
  }

  case object TopicChange extends ControllerEvent {
    override def state: ControllerState = ControllerState.TopicChange

    override def process(): Unit = {
      if (!isActive) return
      val topics = zkClient.getAllTopicsInCluster.toSet
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
  }

  case object LogDirEventNotification extends ControllerEvent {
    override def state: ControllerState = ControllerState.LogDirChange

    override def process(): Unit = {
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
  }

  case class PartitionModifications(topic: String) extends ControllerEvent {
    override def state: ControllerState = ControllerState.TopicChange

    def restorePartitionReplicaAssignment(topic: String, newPartitionReplicaAssignment : immutable.Map[TopicPartition, Seq[Int]]): Unit = {
      info("Restoring the partition replica assignment for topic %s".format(topic))

      val existingPartitions = zkClient.getChildren(TopicPartitionsZNode.path(topic))
      val existingPartitionReplicaAssignment = newPartitionReplicaAssignment.filter(p =>
        existingPartitions.contains(p._1.partition.toString))

      zkClient.setTopicAssignment(topic, existingPartitionReplicaAssignment, controllerContext.epochZkVersion)
    }

    override def process(): Unit = {
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
  }

  case object TopicDeletion extends ControllerEvent {
    override def state: ControllerState = ControllerState.TopicDeletion

    override def process(): Unit = {
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
              topicDeletionManager.markTopicIneligibleForDeletion(Set(topic))
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
  }

  case object PartitionReassignment extends ControllerEvent {
    override def state: ControllerState = ControllerState.PartitionReassignment

    override def process(): Unit = {
      if (!isActive) return

      // We need to register the watcher if the path doesn't exist in order to detect future reassignments and we get
      // the `path exists` check for free
      if (zkClient.registerZNodeChangeHandlerAndCheckExistence(partitionReassignmentHandler)) {
        val partitionReassignment = zkClient.getPartitionReassignment

        // Populate `partitionsBeingReassigned` with all partitions being reassigned before invoking
        // `maybeTriggerPartitionReassignment` (see method documentation for the reason)
        partitionReassignment.foreach { case (tp, newReplicas) =>
          val reassignIsrChangeHandler = new PartitionReassignmentIsrChangeHandler(KafkaController.this, eventManager,
            tp)
          controllerContext.partitionsBeingReassigned.put(tp, ReassignedPartitionsContext(newReplicas, reassignIsrChangeHandler))
        }

        maybeTriggerPartitionReassignment(partitionReassignment.keySet)
      }
    }
  }

  case class PartitionReassignmentIsrChange(partition: TopicPartition) extends ControllerEvent {
    override def state: ControllerState = ControllerState.PartitionReassignment

    override def process(): Unit = {
      if (!isActive) return
      // check if this partition is still being reassigned or not
      controllerContext.partitionsBeingReassigned.get(partition).foreach { reassignedPartitionContext =>
        val reassignedReplicas = reassignedPartitionContext.newReplicas.toSet
        zkClient.getTopicPartitionStates(Seq(partition)).get(partition) match {
          case Some(leaderIsrAndControllerEpoch) => // check if new replicas have joined ISR
            val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr.toSet
            val catchingUpReplicas = reassignedReplicas & getCurrentReplicas(partition).toSet
            val caughtUpReplicas = catchingUpReplicas.filter(isr.contains(_))
            if (caughtUpReplicas == catchingUpReplicas) {
              // resume the partition reassignment process
              info(s"All replicas have caught up with the leader for partition $partition being reassigned. Resuming partition reassignment")
              onPartitionReassignment(partition, reassignedPartitionContext)
            }
            else {
              info(s"${caughtUpReplicas.size}/${catchingUpReplicas.size} replicas have caught up with the leader for " +
                s"partition $partition being reassigned. Replica(s) " +
                s"${(catchingUpReplicas -- isr).mkString(",")} still need to catch up")
            }
          case None => error(s"Error handling reassignment of partition $partition to replicas " +
                         s"${reassignedReplicas.mkString(",")} as it was never created")
        }
      }
    }
  }

  case object IsrChangeNotification extends ControllerEvent {
    override def state: ControllerState = ControllerState.IsrChange

    override def process(): Unit = {
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

    private def processUpdateNotifications(partitions: Seq[TopicPartition]) {
      val liveBrokers: Seq[Int] = controllerContext.liveOrShuttingDownBrokerIds.toSeq
      debug(s"Sending MetadataRequest to Brokers: $liveBrokers for TopicPartitions: $partitions")
      sendUpdateMetadataRequest(liveBrokers, partitions.toSet)
    }
  }

  type ElectPreferredLeadersCallback = (Map[TopicPartition, Int], Map[TopicPartition, ApiError])=>Unit

  def electPreferredLeaders(partitions: Set[TopicPartition], callback: ElectPreferredLeadersCallback = { (_,_) => }): Unit =
    eventManager.put(PreferredReplicaLeaderElection(Some(partitions), AdminClientTriggered, callback))

  case class PreferredReplicaLeaderElection(partitionsFromAdminClientOpt: Option[Set[TopicPartition]],
                                            electionType: ElectionType = ZkTriggered,
                                            callback: ElectPreferredLeadersCallback = (_,_) =>{}) extends PreemptableControllerEvent {
    override def state: ControllerState = ControllerState.ManualLeaderBalance

    override def handlePreempt(): Unit = {
      callback(Map.empty, partitionsFromAdminClientOpt match {
        case Some(partitions) => partitions.map(partition => partition -> new ApiError(Errors.NOT_CONTROLLER, null)).toMap
        case None => Map.empty
      })
    }

    override def handleProcess(): Unit = {
      if (!isActive) {
        callback(Map.empty, partitionsFromAdminClientOpt match {
          case Some(partitions) => partitions.map(partition => partition -> new ApiError(Errors.NOT_CONTROLLER, null)).toMap
          case None => Map.empty
        })
      } else {
        // We need to register the watcher if the path doesn't exist in order to detect future preferred replica
        // leader elections and we get the `path exists` check for free
        if (electionType == AdminClientTriggered || zkClient.registerZNodeChangeHandlerAndCheckExistence(preferredReplicaElectionHandler)) {
          val partitions = partitionsFromAdminClientOpt match {
            case Some(partitions) => partitions
            case None => zkClient.getPreferredReplicaElection
          }

          val (validPartitions, invalidPartitions) = partitions.partition(tp => controllerContext.allPartitions.contains(tp))
          invalidPartitions.foreach { p =>
            info(s"Skipping preferred replica leader election for partition ${p} since it doesn't exist.")
          }

          val (partitionsBeingDeleted, livePartitions) = validPartitions.partition(partition =>
            topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
          if (partitionsBeingDeleted.nonEmpty) {
            warn(s"Skipping preferred replica election for partitions $partitionsBeingDeleted " +
              s"since the respective topics are being deleted")
          }
          // partition those where preferred is already leader
          val (electablePartitions, alreadyPreferred) = livePartitions.partition { partition =>
            val assignedReplicas = controllerContext.partitionReplicaAssignment(partition)
            val preferredReplica = assignedReplicas.head
            val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
            currentLeader != preferredReplica
          }

          val electionErrors = onPreferredReplicaElection(electablePartitions, electionType)
          val successfulPartitions = electablePartitions -- electionErrors.keySet
          val results = electionErrors.map { case (partition, ex) =>
            val apiError = if (ex.isInstanceOf[StateChangeFailedException])
              new ApiError(Errors.PREFERRED_LEADER_NOT_AVAILABLE, ex.getMessage)
            else
              ApiError.fromThrowable(ex)
            partition -> apiError
          } ++
            alreadyPreferred.map(_ -> ApiError.NONE) ++
            partitionsBeingDeleted.map(_ -> new ApiError(Errors.INVALID_TOPIC_EXCEPTION, "The topic is being deleted")) ++
            invalidPartitions.map ( tp => tp -> new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, s"The partition does not exist.")
            )
          debug(s"PreferredReplicaLeaderElection waiting: $successfulPartitions, results: $results")
          callback(successfulPartitions.map(
              tp => tp->controllerContext.partitionReplicaAssignment(tp).head).toMap,
            results)
        }
      }
    }
  }

  case object ControllerChange extends ControllerEvent {
    override def state = ControllerState.ControllerChange

    override def process(): Unit = {
      maybeResign()
    }
  }

  case object Reelect extends ControllerEvent {
    override def state = ControllerState.ControllerChange

    override def process(): Unit = {
      maybeResign()
      elect()
    }
  }

  case object RegisterBrokerAndReelect extends ControllerEvent {
    override def state: ControllerState = ControllerState.ControllerChange

    override def process(): Unit = {
      _brokerEpoch = zkClient.registerBroker(brokerInfo)
      Reelect.process()
    }
  }

  // We can't make this a case object due to the countDownLatch field
  class Expire extends ControllerEvent {
    private val processingStarted = new CountDownLatch(1)
    override def state = ControllerState.ControllerChange

    override def process(): Unit = {
      processingStarted.countDown()
      activeControllerId = -1
      onControllerResignation()
    }

    def waitUntilProcessingStarted(): Unit = {
      processingStarted.await()
    }
  }

}

object ReassignmentHelper {

  // Rules
  // - in the last step order should match the requested order
  def calculateReassignmentStep(finalTargetReplicas: Seq[Int], currentReplicas: Seq[Int], leader: Int, liveBrokerIds: Set[Int], minIsrSize: Int): ReassignmentStep = {
    val drop: Seq[Int] = calculateExcessISRs(finalTargetReplicas, currentReplicas, leader)
    val add = calculateReplicasToAdd(finalTargetReplicas, currentReplicas, liveBrokerIds, minIsrSize)
    val targetReplicasInThisStep = currentReplicas.filterNot(drop.contains) ++ add
    val isLastStep = targetReplicasInThisStep.toSet == finalTargetReplicas.toSet
    val targetReplicasInThisStepInOrder = if (isLastStep) finalTargetReplicas else targetReplicasInThisStep
    ReassignmentStep(currentReplicas, drop, add, targetReplicasInThisStepInOrder)
  }

  private def calculateExcessISRs(finalTargetReplicas: Seq[Int], currentReplicas: Seq[Int], leader: Int) = {
    val numberOfExcessISRs = currentReplicas.size - finalTargetReplicas.size
    // TODO: what if it's empty
    val notInReassigned = currentReplicas.filterNot(finalTargetReplicas.contains)
    val notInReassignedPreferredOrder = notInReassigned.sortBy(brokerId => if (brokerId == leader) 1 else 0)
    println(s"calculated_size: $numberOfExcessISRs, ordered_set_size: ${notInReassignedPreferredOrder.size}, " +
      s"finalTargetReplicas: $finalTargetReplicas, currentReplicas: $currentReplicas, " +
      s"notInReassignedPreferredOrder: $notInReassignedPreferredOrder, dropped: ${notInReassignedPreferredOrder.take(numberOfExcessISRs)}")
    notInReassignedPreferredOrder.take(numberOfExcessISRs)
  }

  // Selects the final leader first if the broker is alive and if it's not yet reassigned, otherwise the next few replica that is
  // missing for the min ISR, or at least one (if there's any left to add)
  private def calculateReplicasToAdd(finalTargetReplicas: Seq[Int], currentReplicas: Seq[Int], liveBrokerIds: Set[Int], minIsrSize: Int): Seq[Int] = {
    val notYetReassigned = finalTargetReplicas.filterNot(currentReplicas.contains)
    val finalLeader = finalTargetReplicas.headOption
    if (finalLeader.exists(notYetReassigned.contains) && finalLeader.exists(liveBrokerIds.contains)) {
      Seq(finalLeader).flatten
    } else {
      val minIsrDeficit = minIsrSize - (currentReplicas.toSet -- liveBrokerIds).size
      val addSize = Math.max(minIsrDeficit, 1)
      notYetReassigned.filter(liveBrokerIds.contains).take(addSize)
    }
  }
}

case class ReassignmentStep(currentReplicas: Seq[Int], drop: Seq[Int], add: Seq[Int], targetReplicas: Seq[Int]) {
  override def toString: String = {
    val currentFragment = currentReplicas.mkString("current replicas=", ","," ")
    val dropFragment = if (drop.isEmpty) "" else drop.mkString("-", ","," ")
    val addFragment = if (add.isEmpty) "" else s"+${add.mkString(",+")} "
    val targetFragment = targetReplicas.mkString("target replicas=", ",", "")
    s"$currentFragment$dropFragment$addFragment$targetFragment"
  }
}

class BrokerChangeHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = BrokerIdsZNode.path

  override def handleChildChange(): Unit = {
    eventManager.put(controller.BrokerChange)
  }
}

class BrokerModificationsHandler(controller: KafkaController, eventManager: ControllerEventManager, brokerId: Int) extends ZNodeChangeHandler {
  override val path: String = BrokerIdZNode.path(brokerId)

  override def handleDataChange(): Unit = {
    eventManager.put(controller.BrokerModifications(brokerId))
  }
}

class TopicChangeHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = TopicsZNode.path

  override def handleChildChange(): Unit = eventManager.put(controller.TopicChange)
}

class LogDirEventNotificationHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = LogDirEventNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(controller.LogDirEventNotification)
}

object LogDirEventNotificationHandler {
  val Version: Long = 1L
}

class PartitionModificationsHandler(controller: KafkaController, eventManager: ControllerEventManager, topic: String) extends ZNodeChangeHandler {
  override val path: String = TopicZNode.path(topic)

  override def handleDataChange(): Unit = eventManager.put(controller.PartitionModifications(topic))
}

class TopicDeletionHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = DeleteTopicsZNode.path

  override def handleChildChange(): Unit = eventManager.put(controller.TopicDeletion)
}

class PartitionReassignmentHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = ReassignPartitionsZNode.path

  // Note that the event is also enqueued when the znode is deleted, but we do it explicitly instead of relying on
  // handleDeletion(). This approach is more robust as it doesn't depend on the watcher being re-registered after
  // it's consumed during data changes (we ensure re-registration when the znode is deleted).
  override def handleCreation(): Unit = eventManager.put(controller.PartitionReassignment)
}

class PartitionReassignmentIsrChangeHandler(controller: KafkaController, eventManager: ControllerEventManager, partition: TopicPartition) extends ZNodeChangeHandler {
  override val path: String = TopicPartitionStateZNode.path(partition)

  override def handleDataChange(): Unit = eventManager.put(controller.PartitionReassignmentIsrChange(partition))
}

class IsrChangeNotificationHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = IsrChangeNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(controller.IsrChangeNotification)
}

object IsrChangeNotificationHandler {
  val Version: Long = 1L
}

class PreferredReplicaElectionHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = PreferredReplicaElectionZNode.path

  override def handleCreation(): Unit = eventManager.put(controller.PreferredReplicaLeaderElection(None))
}

class ControllerChangeHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = ControllerZNode.path

  override def handleCreation(): Unit = eventManager.put(controller.ControllerChange)
  override def handleDeletion(): Unit = eventManager.put(controller.Reelect)
  override def handleDataChange(): Unit = eventManager.put(controller.ControllerChange)
}

case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       val reassignIsrChangeHandler: PartitionReassignmentIsrChangeHandler) {

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
  val enqueueTimeMs: Long = Time.SYSTEM.milliseconds()

  def state: ControllerState
  def process(): Unit
}

/**
  * A `ControllerEvent`, such as one with a client callback, which needs specific handling in the event of ZK session expiration.
  */
sealed trait PreemptableControllerEvent extends ControllerEvent {

  val spent = new AtomicBoolean(false)

  final def preempt(): Unit = {
    if (!spent.getAndSet(true))
      handlePreempt()
  }

  final def process(): Unit = {
    if (!spent.getAndSet(true))
      handleProcess()
  }

  def handlePreempt(): Unit

  def handleProcess(): Unit
}
