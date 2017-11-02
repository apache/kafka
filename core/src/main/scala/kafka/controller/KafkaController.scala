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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.admin.AdminOperationException
import kafka.api._
import kafka.common._
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server._
import kafka.utils._
import kafka.zk._
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zookeeper.{ZNodeChangeHandler, ZNodeChildChangeHandler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, LeaderAndIsrResponse, StopReplicaResponse}
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.{Code, NodeExistsException}

import scala.collection._
import scala.util.Try

object KafkaController extends Logging {
  val InitialControllerEpoch = 1
  val InitialControllerEpochZkVersion = 1
}

class KafkaController(val config: KafkaConfig, zkClient: KafkaZkClient, time: Time, metrics: Metrics, threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  this.logIdent = s"[Controller id=${config.brokerId}] "

  private val stateChangeLogger = new StateChangeLogger(config.brokerId, inControllerContext = true, None)
  val controllerContext = new ControllerContext

  // have a separate scheduler for the controller to be able to start and stop independently of the kafka server
  // visible for testing
  private[controller] val kafkaScheduler = new KafkaScheduler(1)

  // visible for testing
  private[controller] val eventManager = new ControllerEventManager(controllerContext.stats.rateAndTimeMetrics,
    _ => updateMetrics())

  val topicDeletionManager = new TopicDeletionManager(this, eventManager, zkClient)
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(this, stateChangeLogger)
  val replicaStateMachine = new ReplicaStateMachine(config, stateChangeLogger, controllerContext, topicDeletionManager, zkClient, mutable.Map.empty, new ControllerBrokerRequestBatch(this, stateChangeLogger))
  val partitionStateMachine = new PartitionStateMachine(config, stateChangeLogger, controllerContext, topicDeletionManager, zkClient, mutable.Map.empty, new ControllerBrokerRequestBatch(this, stateChangeLogger))

  private val controllerChangeHandler = new ControllerChangeHandler(this, eventManager)
  private val brokerChangeHandler = new BrokerChangeHandler(this, eventManager)
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

  def epoch: Int = controllerContext.epoch

  def state: ControllerState = eventManager.state

  def clientId: String = {
    val controllerListener = config.listeners.find(_.listenerName == config.interBrokerListenerName).getOrElse(
      throw new IllegalArgumentException(s"No listener with name ${config.interBrokerListenerName} is configured."))
    "id_%d-host_%s-port_%d".format(config.brokerId, controllerListener.host, controllerListener.port)
  }

  /**
   * On clean shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   *
   * @param id Id of the broker to shutdown.
   * @return The number of partitions that the broker still leads.
   */
  def shutdownBroker(id: Int, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    val controlledShutdownEvent = ControlledShutdown(id, controlledShutdownCallback)
    eventManager.put(controlledShutdownEvent)
  }

  /**
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   * It does the following things on the become-controller state change -
   * 1. Register controller epoch changed listener
   * 2. Increments the controller epoch
   * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 4. Starts the controller's channel manager
   * 5. Starts the replica state machine
   * 6. Starts the partition state machine
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   */
  def onControllerFailover() {
    info("Reading controller epoch from ZooKeeper")
    readControllerEpochFromZooKeeper()
    info("Incrementing controller epoch in ZooKeeper")
    incrementControllerEpoch()
    info("Registering handlers")

    // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
    val childChangeHandlers = Seq(brokerChangeHandler, topicChangeHandler, topicDeletionHandler, logDirEventNotificationHandler,
      isrChangeNotificationHandler)
    childChangeHandlers.foreach(zkClient.registerZNodeChildChangeHandler)
    val nodeChangeHandlers = Seq(preferredReplicaElectionHandler, partitionReassignmentHandler)
    nodeChangeHandlers.foreach(zkClient.registerZNodeChangeHandlerAndCheckExistence)

    info("Deleting log dir event notifications")
    zkClient.deleteLogDirEventNotifications()
    info("Deleting isr change notifications")
    zkClient.deleteIsrChangeNotifications()
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
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

    replicaStateMachine.startup()
    partitionStateMachine.startup()

    info(s"Ready to serve as the new controller with epoch $epoch")
    maybeTriggerPartitionReassignment()
    topicDeletionManager.tryTopicDeletion()
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    onPreferredReplicaElection(pendingPreferredReplicaElections)
    info("Starting the controller scheduler")
    kafkaScheduler.startup()
    if (config.autoLeaderRebalanceEnable) {
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
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
  def onControllerResignation() {
    debug("Resigning")
    // de-register listeners
    zkClient.unregisterZNodeChildChangeHandler(isrChangeNotificationHandler.path)
    zkClient.unregisterZNodeChangeHandler(partitionReassignmentHandler.path)
    zkClient.unregisterZNodeChangeHandler(preferredReplicaElectionHandler.path)
    zkClient.unregisterZNodeChildChangeHandler(logDirEventNotificationHandler.path)

    // reset topic deletion manager
    topicDeletionManager.reset()

    // shutdown leader rebalance scheduler
    kafkaScheduler.shutdown()
    offlinePartitionCount = 0
    preferredReplicaImbalanceCount = 0
    globalTopicCount = 0
    globalPartitionCount = 0

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

    resetControllerContext()

    info("Resigned")
  }

  /**
   * Returns true if this broker is the current controller.
   */
  def isActive: Boolean = activeControllerId == config.brokerId

  /*
   * This callback is invoked by the controller's LogDirEventNotificationListener with the list of broker ids who
   * have experienced new log directory failures. In response the controller should send LeaderAndIsrRequest
   * to all these brokers to query the state of their replicas
   */
  def onBrokerLogDirFailure(brokerIds: Seq[Int]) {
    // send LeaderAndIsrRequest for all replicas on those brokers to see if they are still online.
    val replicasOnBrokers = controllerContext.replicasOnBrokers(brokerIds.toSet)
    replicaStateMachine.handleStateChanges(replicasOnBrokers.toSeq, OnlineReplica)
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Sends update metadata request to all live and shutting down brokers
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
   *    so, it performs the reassignment logic for each topic/partition.
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
   */
  def onBrokerStartup(newBrokers: Seq[Int]) {
    info("New broker startup callback for %s".format(newBrokers.mkString(",")))
    newBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
    val newBrokersSet = newBrokers.toSet
    // send update metadata request to all live and shutting down brokers. Old brokers will get to know of the new
    // broker via this update.
    // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
    // common controlled shutdown case, the metadata will reach the new brokers faster
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
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
      info(("Some replicas %s for topics scheduled for deletion %s are on the newly restarted brokers %s. " +
        "Signaling restart of topic deletion for these topics").format(replicasForTopicsToBeDeleted.mkString(","),
        topicDeletionManager.topicsToBeDeleted.mkString(","), newBrokers.mkString(",")))
      topicDeletionManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }
  }

  /*
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It will call onReplicaBecomeOffline(...) with the list of replicas on those failed brokers as input.
   */
  def onBrokerFailure(deadBrokers: Seq[Int]) {
    info("Broker failure callback for %s".format(deadBrokers.mkString(",")))
    deadBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    info("Removed %s from list of shutting down brokers.".format(deadBrokersThatWereShuttingDown))
    val allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokers.toSet)
    onReplicasBecomeOffline(allReplicasOnDeadBrokers)
  }

  /**
    * This method marks the given replicas as offline. It does the following -
    * 1. Mark the given partitions as offline
    * 2. Triggers the OnlinePartition state change for all new/offline partitions
    * 3. Invokes the OfflineReplica state change on the input list of newly offline replicas
    * 4. If no partitions are affected then send UpdateMetadataRequest to live or shutting down brokers
    *
    * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
    * the partition state machine will refresh our cache for us when performing leader election for all new/offline
    * partitions coming online.
    */
  def onReplicasBecomeOffline(newOfflineReplicas: Set[PartitionAndReplica]): Unit = {
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

    // fail deletion of topics that affected by the offline replicas
    if (newOfflineReplicasForDeletion.nonEmpty) {
      // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
      // deleted when its log directory is offline. This will prevent the replica from being in TopicDeletionStarted state indefinitely
      // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
      topicDeletionManager.failReplicaDeletion(newOfflineReplicasForDeletion)
    }

    // If replica failure did not require leader re-election, inform brokers of the offline replica
    // Note that during leader re-election, brokers update their metadata
    if (partitionsWithoutLeader.isEmpty) {
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    }
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  def onNewPartitionCreation(newPartitions: Set[TopicPartition]) {
    info("New partition creation callback for %s".format(newPartitions.mkString(",")))
    partitionStateMachine.handleStateChanges(newPartitions.toSeq, NewPartition)
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, NewReplica)
    partitionStateMachine.handleStateChanges(newPartitions.toSeq, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
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
  def onPartitionReassignment(topicPartition: TopicPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
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
      info(s"Waiting for new replicas ${reassignedReplicas.mkString(",")} for partition ${topicPartition} being " +
        "reassigned to catch up with the leader")
    } else {
      //4. Wait until all replicas in RAR are in sync with the leader.
      val oldReplicas = controllerContext.partitionReplicaAssignment(topicPartition).toSet -- reassignedReplicas.toSet
      //5. replicas in RAR -> OnlineReplica
      reassignedReplicas.foreach { replica =>
        replicaStateMachine.handleStateChanges(Seq(new PartitionAndReplica(topicPartition, replica)), OnlineReplica)
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
      removePartitionFromReassignedPartitions(topicPartition)
      info(s"Removed partition $topicPartition from the list of reassigned partitions in zookeeper")
      controllerContext.partitionsBeingReassigned.remove(topicPartition)
      //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
      // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
      topicDeletionManager.resumeDeletionForTopics(Set(topicPartition.topic))
    }
  }

  private def watchIsrChangesForReassignedPartition(partition: TopicPartition,
                                                    reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignIsrChangeHandler = new PartitionReassignmentIsrChangeHandler(this, eventManager, partition)
    reassignedPartitionContext.reassignIsrChangeHandler = reassignIsrChangeHandler
    // register listener on the leader and isr path to wait until they catch up with the current leader
    zkClient.registerZNodeChangeHandler(reassignIsrChangeHandler)
  }

  def initiateReassignReplicasForTopicPartition(topicPartition: TopicPartition,
                                                reassignedPartitionContext: ReassignedPartitionsContext) {
    val newReplicas = reassignedPartitionContext.newReplicas
    val topic = topicPartition.topic
    try {
      val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicPartition)
      assignedReplicasOpt match {
        case Some(assignedReplicas) =>
          if (assignedReplicas == newReplicas) {
            throw new KafkaException(s"Partition $topicPartition to be reassigned is already assigned to replicas " +
              s"${newReplicas.mkString(",")}. Ignoring request for partition reassignment")
          } else {
            info(s"Handling reassignment of partition $topicPartition to new replicas ${newReplicas.mkString(",")}")
            // first register ISR change listener
            watchIsrChangesForReassignedPartition(topicPartition, reassignedPartitionContext)
            controllerContext.partitionsBeingReassigned.put(topicPartition, reassignedPartitionContext)
            // mark topic ineligible for deletion for the partitions being reassigned
            topicDeletionManager.markTopicIneligibleForDeletion(Set(topic))
            onPartitionReassignment(topicPartition, reassignedPartitionContext)
          }
        case None => throw new KafkaException(s"Attempt to reassign partition $topicPartition that doesn't exist")
      }
    } catch {
      case e: Throwable =>
        error(s"Error completing reassignment of partition $topicPartition", e)
        // remove the partition from the admin path to unblock the admin client
        removePartitionFromReassignedPartitions(topicPartition)
    }
  }

  def onPreferredReplicaElection(partitions: Set[TopicPartition], isTriggeredByAutoRebalance: Boolean = false) {
    info("Starting preferred replica leader election for partitions %s".format(partitions.mkString(",")))
    try {
      partitionStateMachine.handleStateChanges(partitions.toSeq, OnlinePartition, Option(PreferredReplicaPartitionLeaderElectionStrategy))
    } catch {
      case e: Throwable => error("Error completing preferred replica leader election for partitions %s".format(partitions.mkString(",")), e)
    } finally {
      removePartitionsFromPreferredReplicaElection(partitions, isTriggeredByAutoRebalance)
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  def startup() = {
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

  def sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: AbstractResponse => Unit = null) = {
    controllerContext.controllerChannelManager.sendRequest(brokerId, apiKey, request, callback)
  }

  def incrementControllerEpoch(): Unit = {
    val newControllerEpoch = controllerContext.epoch + 1
    val setDataResponse = zkClient.setControllerEpochRaw(newControllerEpoch, controllerContext.epochZkVersion)
    setDataResponse.resultCode match {
      case Code.OK =>
        controllerContext.epochZkVersion = setDataResponse.stat.getVersion
        controllerContext.epoch = newControllerEpoch
      case Code.NONODE =>
        // if path doesn't exist, this is the first controller whose epoch should be 1
        // the following call can still fail if another controller gets elected between checking if the path exists and
        // trying to create the controller epoch path
        val createResponse = zkClient.createControllerEpochRaw(KafkaController.InitialControllerEpoch)
        createResponse.resultCode match {
          case Code.OK =>
            controllerContext.epoch = KafkaController.InitialControllerEpoch
            controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion
          case Code.NODEEXISTS =>
            throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure")
          case _ =>
            val exception = createResponse.resultException.get
            error("Error while incrementing controller epoch", exception)
            throw exception
        }
      case _ =>
        throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure")
    }
    info(s"Epoch incremented to ${controllerContext.epoch}")
  }

  private def initializeControllerContext() {
    // update controller cache with delete topic information
    controllerContext.liveBrokers = zkClient.getAllBrokersInCluster.toSet
    controllerContext.allTopics = zkClient.getAllTopicsInCluster.toSet
    registerPartitionModificationsHandlers(controllerContext.allTopics.toSeq)
    controllerContext.partitionReplicaAssignment = mutable.Map.empty ++ zkClient.getReplicaAssignmentForTopics(controllerContext.allTopics.toSet)
    controllerContext.partitionLeadershipInfo = new mutable.HashMap[TopicPartition, LeaderIsrAndControllerEpoch]
    controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache()
    // start the channel manager
    startChannelManager()
    initializePartitionReassignment()
    info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
    info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds))
    info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
  }

  private def fetchPendingPreferredReplicaElections(): Set[TopicPartition] = {
    val partitionsUndergoingPreferredReplicaElection = zkClient.getPreferredReplicaElection
    // check if they are already completed or topic was deleted
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
      val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition)
      val topicDeleted = replicasOpt.isEmpty
      val successful =
        if(!topicDeleted) controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == replicasOpt.get.head else false
      successful || topicDeleted
    }
    val pendingPreferredReplicaElectionsIgnoringTopicDeletion = partitionsUndergoingPreferredReplicaElection -- partitionsThatCompletedPreferredReplicaElection
    val pendingPreferredReplicaElectionsSkippedFromTopicDeletion = pendingPreferredReplicaElectionsIgnoringTopicDeletion.filter(partition => topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
    val pendingPreferredReplicaElections = pendingPreferredReplicaElectionsIgnoringTopicDeletion -- pendingPreferredReplicaElectionsSkippedFromTopicDeletion
    info("Partitions undergoing preferred replica election: %s".format(partitionsUndergoingPreferredReplicaElection.mkString(",")))
    info("Partitions that completed preferred replica election: %s".format(partitionsThatCompletedPreferredReplicaElection.mkString(",")))
    info("Skipping preferred replica election for partitions due to topic deletion: %s".format(pendingPreferredReplicaElectionsSkippedFromTopicDeletion.mkString(",")))
    info("Resuming preferred replica election for partitions: %s".format(pendingPreferredReplicaElections.mkString(",")))
    pendingPreferredReplicaElections
  }

  private def resetControllerContext(): Unit = {
    if (controllerContext.controllerChannelManager != null) {
      controllerContext.controllerChannelManager.shutdown()
      controllerContext.controllerChannelManager = null
    }
    controllerContext.shuttingDownBrokerIds.clear()
    controllerContext.epoch = 0
    controllerContext.epochZkVersion = 0
    controllerContext.allTopics = Set.empty
    controllerContext.partitionReplicaAssignment.clear()
    controllerContext.partitionLeadershipInfo.clear()
    controllerContext.partitionsBeingReassigned.clear()
    controllerContext.liveBrokers = Set.empty
  }

  private def initializePartitionReassignment() {
    // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
    val partitionsBeingReassigned = zkClient.getPartitionReassignment
    // check if they are already completed or topic was deleted
    val reassignedPartitions = partitionsBeingReassigned.filter { case (tp, reassignmentReplicas) =>
      controllerContext.partitionReplicaAssignment.get(tp) match {
        case None => true // topic deleted
        case Some(currentReplicas) => currentReplicas == reassignmentReplicas // reassignment completed
      }
    }.keys
    reassignedPartitions.foreach(removePartitionFromReassignedPartitions)
    val partitionsToReassign = partitionsBeingReassigned -- reassignedPartitions
    controllerContext.partitionsBeingReassigned ++= partitionsToReassign.mapValues(new ReassignedPartitionsContext(_))
    info(s"Partitions being reassigned: $partitionsBeingReassigned")
    info(s"Partitions already reassigned: $reassignedPartitions")
    info(s"Resuming reassignment of partitions: $partitionsToReassign")
  }

  private def fetchTopicDeletionsInProgress(): (Set[String], Set[String]) = {
    val topicsToBeDeleted = zkClient.getTopicDeletions.toSet
    val topicsWithOfflineReplicas = controllerContext.partitionReplicaAssignment.filter { case (partition, replicas) =>
      replicas.exists(r => !controllerContext.isReplicaOnline(r, partition))
    }.keySet.map(_.topic)
    val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsBeingReassigned.keySet.map(_.topic)
    val topicsIneligibleForDeletion = topicsWithOfflineReplicas | topicsForWhichPartitionReassignmentIsInProgress
    info("List of topics to be deleted: %s".format(topicsToBeDeleted.mkString(",")))
    info("List of topics ineligible for deletion: %s".format(topicsIneligibleForDeletion.mkString(",")))
    (topicsToBeDeleted, topicsIneligibleForDeletion)
  }

  private def maybeTriggerPartitionReassignment() {
    controllerContext.partitionsBeingReassigned.foreach { case (tp, reassignContext) =>
      initiateReassignReplicasForTopicPartition(tp, reassignContext)
    }
  }

  private def startChannelManager() {
    controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics,
      stateChangeLogger, threadNamePrefix)
    controllerContext.controllerChannelManager.startup()
  }

  def updateLeaderAndIsrCache(partitions: Seq[TopicPartition] = controllerContext.partitionReplicaAssignment.keys.toSeq) {
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

  private def moveReassignedPartitionLeaderIfRequired(topicPartition: TopicPartition,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader
    // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
    // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
    val oldAndNewReplicas = controllerContext.partitionReplicaAssignment(topicPartition)
    controllerContext.partitionReplicaAssignment.put(topicPartition, reassignedReplicas)
    if(!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
      info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicPartition) +
        "is not in the new list of replicas %s. Re-electing leader".format(reassignedReplicas.mkString(",")))
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Option(ReassignPartitionLeaderElectionStrategy))
    } else {
      // check if the leader is alive or not
      if (controllerContext.isReplicaOnline(currentLeader, topicPartition)) {
        info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicPartition) +
          "is already in the new list of replicas %s and is alive".format(reassignedReplicas.mkString(",")))
        // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
        updateLeaderEpochAndSendRequest(topicPartition, oldAndNewReplicas, reassignedReplicas)
      } else {
        info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicPartition) +
          "is already in the new list of replicas %s but is dead".format(reassignedReplicas.mkString(",")))
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
    val partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.filter(_._1.topic == partition.topic)
    partitionsAndReplicasForThisTopic.put(partition, replicas)
    val setDataResponse = zkClient.setTopicAssignmentRaw(partition.topic, partitionsAndReplicasForThisTopic.toMap)
    if (setDataResponse.resultCode == Code.OK) {
      info("Updated assigned replicas for partition %s being reassigned to %s ".format(partition, replicas.mkString(",")))
      // update the assigned replica list after a successful zookeeper write
      controllerContext.partitionReplicaAssignment.put(partition, replicas)
    } else if (setDataResponse.resultCode == Code.NONODE) {
      throw new IllegalStateException("Topic %s doesn't exist".format(partition.topic))
    } else {
      throw new KafkaException(setDataResponse.resultException.get)
    }
  }

  private def startNewReplicasForReassignedPartition(topicPartition: TopicPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext,
                                                     newReplicas: Set[Int]) {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Seq(new PartitionAndReplica(topicPartition, replica)), NewReplica)
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

  def unregisterPartitionModificationsHandlers(topics: Seq[String]) = {
    topics.foreach { topic =>
      partitionModificationsHandlers.remove(topic)
        .foreach(handler => zkClient.unregisterZNodeChangeHandler(handler.path))
    }
  }

  private def unregisterPartitionReassignmentIsrChangeHandlers() {
    controllerContext.partitionsBeingReassigned.values.foreach { reassignedPartitionsContext =>
      zkClient.unregisterZNodeChangeHandler(reassignedPartitionsContext.reassignIsrChangeHandler.path)
    }
  }

  private def readControllerEpochFromZooKeeper() {
    // initialize the controller epoch and zk version by reading from zookeeper
    val epochAndStatOpt = zkClient.getControllerEpoch
    epochAndStatOpt.foreach { case (epoch, stat) =>
      controllerContext.epoch = epoch
      controllerContext.epochZkVersion = stat.getVersion
      info("Initialized controller epoch to %d and zk version %d".format(controllerContext.epoch, controllerContext.epochZkVersion))
    }
  }

  def removePartitionFromReassignedPartitions(topicPartition: TopicPartition) {
    controllerContext.partitionsBeingReassigned.get(topicPartition).foreach { reassignContext =>
      // stop watching the ISR changes for this partition
      zkClient.unregisterZNodeChangeHandler(reassignContext.reassignIsrChangeHandler.path)
    }

    val updatedPartitionsBeingReassigned = controllerContext.partitionsBeingReassigned - topicPartition

    // write the new list to zookeeper
    if (updatedPartitionsBeingReassigned.isEmpty) {
      info(s"No more partitions need to be reassigned. Deleting zk path ${ReassignPartitionsZNode.path}")
      zkClient.deletePartitionReassignment()
      // Ensure we detect future reassignments
      eventManager.put(PartitionReassignment)
    } else {
      val reassignment = updatedPartitionsBeingReassigned.mapValues(_.newReplicas)
      try zkClient.setOrCreatePartitionReassignment(reassignment)
      catch {
        case e: KeeperException => throw new AdminOperationException(e)
      }
    }

    controllerContext.partitionsBeingReassigned.remove(topicPartition)
  }

  def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicPartition],
                                                   isTriggeredByAutoRebalance : Boolean) {
    for (partition <- partitionsToBeRemoved) {
      // check the status
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if (currentLeader == preferredReplica) {
        info("Partition %s completed preferred replica leader election. New leader is %d".format(partition, preferredReplica))
      } else {
        warn("Partition %s failed to complete preferred replica leader election. Leader is %d".format(partition, currentLeader))
      }
    }
    if (!isTriggeredByAutoRebalance)
      zkClient.deletePreferredReplicaElection()
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   *
   * @param brokers The brokers that the update metadata request should be sent to
   */
  def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicPartition] = Set.empty[TopicPartition]) {
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
    debug("Updating leader epoch for partition %s.".format(partition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      zkWriteCompleteOrUnnecessary = zkClient.getTopicPartitionStates(Seq(partition)).get(partition) match {
        case Some(leaderIsrAndControllerEpoch) =>
          val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
          if (controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = leaderAndIsr.newEpochAndZkVersion
          // update the new leadership decision in zookeeper or retry
          val UpdateLeaderAndIsrResult(successfulUpdates, _, failedUpdates) =
            zkClient.updateLeaderAndIsr(immutable.Map(partition -> newLeaderAndIsr), epoch)
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
      controllerContext.partitionReplicaAssignment.filterNot { case (tp, _) =>
        topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
      }.groupBy { case (_, assignedReplicas) => assignedReplicas.head }
    debug(s"Preferred replicas by broker $preferredReplicasForTopicsByBrokers")

    // for each broker, check if a preferred replica election needs to be triggered
    preferredReplicasForTopicsByBrokers.foreach { case (leaderBroker, topicPartitionsForBroker) =>
      val topicsNotInPreferredReplica = topicPartitionsForBroker.filter { case (topicPartition, _) =>
        val leadershipInfo = controllerContext.partitionLeadershipInfo.get(topicPartition)
        leadershipInfo.exists(_.leaderAndIsr.leader != leaderBroker)
      }
      debug(s"Topics not in preferred replica $topicsNotInPreferredReplica")

      val imbalanceRatio = topicsNotInPreferredReplica.size.toDouble / topicPartitionsForBroker.size
      trace(s"Leader imbalance ratio for broker $leaderBroker is $imbalanceRatio")

      // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
      // that need to be on this broker
      if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
        topicsNotInPreferredReplica.keys.foreach { topicPartition =>
          // do this check only if the broker is live and there are no partitions being reassigned currently
          // and preferred replica election is not in progress
          if (controllerContext.isReplicaOnline(leaderBroker, topicPartition) &&
            controllerContext.partitionsBeingReassigned.isEmpty &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&
            controllerContext.allTopics.contains(topicPartition.topic)) {
            onPreferredReplicaElection(Set(topicPartition), isTriggeredByAutoRebalance = true)
          }
        }
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

  case class ControlledShutdown(id: Int, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit) extends ControllerEvent {

    def state = ControllerState.ControlledShutdown

    override def process(): Unit = {
      val controlledShutdownResult = Try { doControlledShutdown(id) }
      controlledShutdownCallback(controlledShutdownResult)
    }

    private def doControlledShutdown(id: Int): Set[TopicPartition] = {
      if (!isActive) {
        throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
      }

      info("Shutting down broker " + id)

      if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
        throw new BrokerNotAvailableException("Broker id %d does not exist.".format(id))

      controllerContext.shuttingDownBrokerIds.add(id)
      debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds.mkString(","))
      debug("Live brokers: " + controllerContext.liveBrokerIds.mkString(","))

      val partitionsToActOn = controllerContext.partitionsOnBroker(id).filter { partition =>
        controllerContext.partitionReplicaAssignment(partition).size > 1 && controllerContext.partitionLeadershipInfo.contains(partition)
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
        trace("All leaders = " + controllerContext.partitionLeadershipInfo.mkString(","))
        controllerContext.partitionLeadershipInfo.filter {
          case (topicPartition, leaderIsrAndControllerEpoch) =>
            leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment(topicPartition).size > 1
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
      debug("Delete topic callback invoked for %s".format(stopReplicaResponse))
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
        controllerContext.partitionLeadershipInfo.count { case (tp, leadershipInfo) =>
          !controllerContext.liveOrShuttingDownBrokerIds.contains(leadershipInfo.leaderAndIsr.leader) &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
        }
      }

    preferredReplicaImbalanceCount =
      if (!isActive) {
        0
      } else {
        controllerContext.partitionReplicaAssignment.count { case (topicPartition, replicas) =>
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
    onControllerResignation()
    activeControllerId = -1
    zkClient.deleteController()
  }

  def expire(): Unit = {
    val expireEvent = new Expire
    eventManager.clearAndPut(expireEvent)
    expireEvent.waitUntilProcessed()
  }

  def newSession(): Unit = {
    eventManager.put(Reelect)
  }

  def elect(): Unit = {
    val timestamp = time.milliseconds
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    /*
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition,
     * it's possible that the controller has already been elected when we get here. This check will prevent the following
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    if (activeControllerId != -1) {
      debug("Broker %d has been elected as the controller, so stopping the election process.".format(activeControllerId))
      return
    }

    try {
      zkClient.checkedEphemeralCreate(ControllerZNode.path, ControllerZNode.encode(config.brokerId, timestamp))
      info(config.brokerId + " successfully elected as the controller")
      activeControllerId = config.brokerId
      onControllerFailover()
    } catch {
      case _: NodeExistsException =>
        // If someone else has written the path, then
        activeControllerId = zkClient.getControllerId.getOrElse(-1)

        if (activeControllerId != -1)
          debug("Broker %d was elected as controller instead of broker %d".format(activeControllerId, config.brokerId))
        else
          warn("A controller has been elected but just resigned, this will result in another round of election")

      case e2: Throwable =>
        error("Error while electing or becoming controller on broker %d".format(config.brokerId), e2)
        triggerControllerMove()
    }
  }

  case object BrokerChange extends ControllerEvent {
    override def state: ControllerState = ControllerState.BrokerChange

    override def process(): Unit = {
      if (!isActive) return
      val curBrokers = zkClient.getAllBrokersInCluster.toSet
      val curBrokerIds = curBrokers.map(_.id)
      val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
      val newBrokerIds = curBrokerIds -- liveOrShuttingDownBrokerIds
      val deadBrokerIds = liveOrShuttingDownBrokerIds -- curBrokerIds
      val newBrokers = curBrokers.filter(broker => newBrokerIds(broker.id))
      controllerContext.liveBrokers = curBrokers
      val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
      val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
      val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
      info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
        .format(newBrokerIdsSorted.mkString(","), deadBrokerIdsSorted.mkString(","), liveBrokerIdsSorted.mkString(",")))
      newBrokers.foreach(controllerContext.controllerChannelManager.addBroker)
      deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)
      if (newBrokerIds.nonEmpty)
        onBrokerStartup(newBrokerIdsSorted)
      if (deadBrokerIds.nonEmpty)
        onBrokerFailure(deadBrokerIdsSorted)
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
      controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
        !deletedTopics.contains(p._1.topic))
      controllerContext.partitionReplicaAssignment ++= addedPartitionReplicaAssignment
      info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
        deletedTopics, addedPartitionReplicaAssignment))
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
        zkClient.deleteLogDirEventNotifications(sequenceNumbers)
      }
    }
  }

  case class PartitionModifications(topic: String) extends ControllerEvent {
    override def state: ControllerState = ControllerState.TopicChange

    override def process(): Unit = {
      if (!isActive) return
      val partitionReplicaAssignment = zkClient.getReplicaAssignmentForTopics(immutable.Set(topic))
      val partitionsToBeAdded = partitionReplicaAssignment.filter(p =>
        !controllerContext.partitionReplicaAssignment.contains(p._1))
      if(topicDeletionManager.isTopicQueuedUpForDeletion(topic))
        error("Skipping adding partitions %s for topic %s since it is currently being deleted"
          .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))
      else {
        if (partitionsToBeAdded.nonEmpty) {
          info(s"New partitions to be added $partitionsToBeAdded")
          controllerContext.partitionReplicaAssignment ++= partitionsToBeAdded
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
        zkClient.deleteTopicDeletions(nonExistentTopics.toSeq)
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
        zkClient.deleteTopicDeletions(topicsToBeDeleted.toSeq)
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
        val partitionsToBeReassigned = partitionReassignment -- controllerContext.partitionsBeingReassigned.keys
        partitionsToBeReassigned.foreach { case (tp, context) =>
          if (topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)) {
            error(s"Skipping reassignment of $tp since the topic is currently being deleted")
            removePartitionFromReassignedPartitions(tp)
          } else {
            initiateReassignReplicasForTopicPartition(tp, ReassignedPartitionsContext(context))
          }
        }
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
          case None => error("Error handling reassignment of partition %s to replicas %s as it was never created"
            .format(partition, reassignedReplicas.mkString(",")))
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
        zkClient.deleteIsrChangeNotifications(sequenceNumbers)
      }
    }

    private def processUpdateNotifications(partitions: Seq[TopicPartition]) {
      val liveBrokers: Seq[Int] = controllerContext.liveOrShuttingDownBrokerIds.toSeq
      debug("Sending MetadataRequest to Brokers:" + liveBrokers + " for TopicPartitions:" + partitions)
      sendUpdateMetadataRequest(liveBrokers, partitions.toSet)
    }
  }

  case object PreferredReplicaLeaderElection extends ControllerEvent {
    override def state: ControllerState = ControllerState.ManualLeaderBalance

    override def process(): Unit = {
      if (!isActive) return
      zkClient.registerZNodeChangeHandlerAndCheckExistence(preferredReplicaElectionHandler)
      val partitions = zkClient.getPreferredReplicaElection
      val partitionsForTopicsToBeDeleted = partitions.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
      if (partitionsForTopicsToBeDeleted.nonEmpty) {
        error("Skipping preferred replica election for partitions %s since the respective topics are being deleted"
          .format(partitionsForTopicsToBeDeleted))
      }
      onPreferredReplicaElection(partitions -- partitionsForTopicsToBeDeleted)
    }
  }

  case object ControllerChange extends ControllerEvent {
    override def state = ControllerState.ControllerChange

    override def process(): Unit = {
      val wasActiveBeforeChange = isActive
      zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
      activeControllerId = zkClient.getControllerId.getOrElse(-1)
      if (wasActiveBeforeChange && !isActive) {
        onControllerResignation()
      }
    }
  }

  case object Reelect extends ControllerEvent {
    override def state = ControllerState.ControllerChange

    override def process(): Unit = {
      val wasActiveBeforeChange = isActive
      zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
      activeControllerId = zkClient.getControllerId.getOrElse(-1)
      if (wasActiveBeforeChange && !isActive) {
        onControllerResignation()
      }
      elect()
    }
  }

  // We can't make this a case object due to the countDownLatch field
  class Expire extends ControllerEvent {
    private val countDownLatch = new CountDownLatch(1)
    override def state = ControllerState.ControllerChange

    override def process(): Unit = {
      countDownLatch.countDown()
      activeControllerId = -1
      onControllerResignation()
    }

    def waitUntilProcessed(): Unit = {
      countDownLatch.await()
    }
  }

}

class BrokerChangeHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = BrokerIdsZNode.path

  override def handleChildChange(): Unit = eventManager.put(controller.BrokerChange)
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

  override def handleCreation(): Unit = eventManager.put(controller.PreferredReplicaLeaderElection)
  override def handleDeletion(): Unit = eventManager.put(controller.PreferredReplicaLeaderElection)
  override def handleDataChange(): Unit = eventManager.put(controller.PreferredReplicaLeaderElection)
}

class ControllerChangeHandler(controller: KafkaController, eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = ControllerZNode.path

  override def handleCreation(): Unit = eventManager.put(controller.ControllerChange)
  override def handleDeletion(): Unit = eventManager.put(controller.Reelect)
  override def handleDataChange(): Unit = eventManager.put(controller.ControllerChange)
}

case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       var reassignIsrChangeHandler: PartitionReassignmentIsrChangeHandler = null)

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
      state -> new KafkaTimer(newTimer(s"$metricName", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
    }
  }.toMap

}

sealed trait ControllerEvent {
  def state: ControllerState
  def process(): Unit
}
