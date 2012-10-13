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

import collection._
import collection.immutable.Set
import kafka.cluster.Broker
import kafka.api._
import kafka.utils.ZkUtils._
import org.apache.zookeeper.Watcher.Event.KeeperState
import kafka.server.{ZookeeperLeaderElector, KafkaConfig}
import java.util.concurrent.TimeUnit
import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import com.yammer.metrics.core.Gauge
import org.I0Itec.zkclient.{IZkDataListener, IZkStateListener, ZkClient}
import kafka.utils.{Utils, ZkUtils, Logging}
import org.I0Itec.zkclient.exception.ZkNoNodeException
import java.lang.{IllegalStateException, Object}
import kafka.admin.PreferredReplicaLeaderElectionCommand
import kafka.common.{TopicAndPartition, KafkaException}

class ControllerContext(val zkClient: ZkClient,
                        var controllerChannelManager: ControllerChannelManager = null,
                        val controllerLock: Object = new Object,
                        var liveBrokers: Set[Broker] = null,
                        var liveBrokerIds: Set[Int] = null,
                        var allTopics: Set[String] = null,
                        var partitionReplicaAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = null,
                        var allLeaders: mutable.Map[TopicAndPartition, Int] = null,
                        var partitionsBeingReassigned: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] =
                        new mutable.HashMap,
                        var partitionsUndergoingPreferredReplicaElection: mutable.Set[TopicAndPartition] = new mutable.HashSet)

class KafkaController(val config : KafkaConfig, zkClient: ZkClient) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Controller " + config.brokerId + "]: "
  private var isRunning = true
  val controllerContext = new ControllerContext(zkClient)
  private val partitionStateMachine = new PartitionStateMachine(this)
  private val replicaStateMachine = new ReplicaStateMachine(this)
  private val controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover,
    config.brokerId)
  private val reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext)
  private val preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext)

  newGauge(
    "ActiveControllerCount",
    new Gauge[Int] {
      def getValue() = if (isActive) 1 else 0
    }
  )

  /**
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   * It does the following things on the become-controller state change -
   * 1. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 2. Starts the controller's channel manager
   * 3. Starts the replica state machine
   * 4. Starts the partition state machine
   */
  def onControllerFailover() {
    if(isRunning) {
      info("Broker %d starting become controller state transition".format(config.brokerId))
      // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
      registerReassignedPartitionsListener()
      registerPreferredReplicaElectionListener()
      partitionStateMachine.registerListeners()
      replicaStateMachine.registerListeners()
      initializeControllerContext()
      partitionStateMachine.startup()
      replicaStateMachine.startup()
      info("Broker %d is ready to serve as the new controller".format(config.brokerId))
    }else
      info("Controller has been shut down, aborting startup/failover")
  }

  /**
   * Returns true if this broker is the current controller.
   */
  def isActive(): Boolean = {
    controllerContext.controllerChannelManager != null
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Updates the leader and ISR cache. We have to do this since we don't register zookeeper listeners to update
   *    leader and ISR for every partition as they take place
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. Invokes the OnlineReplica state change on the input list of newly started brokers
   */
  def onBrokerStartup(newBrokers: Seq[Int]) {
    info("New broker startup callback for %s".format(newBrokers.mkString(",")))
    // update leader and isr cache for broker
    updateLeaderAndIsrCache()
    // update partition state machine
    partitionStateMachine.triggerOnlinePartitionStateChange()
    replicaStateMachine.handleStateChanges(getAllReplicasOnBroker(zkClient, controllerContext.allTopics.toSeq, newBrokers),
      OnlineReplica)
    // check if reassignment of some partitions need to be restarted
    val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter(p =>
      p._2.newReplicas.foldLeft(false)((a, replica) => newBrokers.contains(replica) || a))
    partitionsWithReplicasOnNewBrokers.foreach(p => onPartitionReassignment(p._1, p._2))
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It does the following -
   * 1. Updates the leader and ISR cache. We have to do this since we don't register zookeeper listeners to update
   *    leader and ISR for every partition as they take place
   * 2. Mark partitions with dead leaders offline
   * 3. Triggers the OnlinePartition state change for all new/offline partitions
   * 4. Invokes the OfflineReplica state change on the input list of newly started brokers
   */
  def onBrokerFailure(deadBrokers: Seq[Int]) {
    info("Broker failure callback for %s".format(deadBrokers.mkString(",")))
    // update leader and isr cache for broker
    updateLeaderAndIsrCache()
    // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers
    val partitionsWithoutLeader = controllerContext.allLeaders.filter(partitionAndLeader =>
      deadBrokers.contains(partitionAndLeader._2)).keySet
    partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // handle dead replicas
    replicaStateMachine.handleStateChanges(getAllReplicasOnBroker(zkClient, controllerContext.allTopics.toSeq, deadBrokers),
      OfflineReplica)
  }

  /**
   * This callback is invoked by the partition state machine's topic change listener with the list of failed brokers
   * as input. It does the following -
   * 1. Registers partition change listener. This is not required until KAFKA-347
   * 2. Invokes the new partition callback
   */
  def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]) {
    info("New topic creation callback for %s".format(newPartitions.mkString(",")))
    // subscribe to partition changes
    topics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
    onNewPartitionCreation(newPartitions)
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]) {
    info("New partition creation callback for %s".format(newPartitions.mkString(",")))
    partitionStateMachine.handleStateChanges(newPartitions, NewPartition)
    replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), NewReplica)
    partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition)
    replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), OnlineReplica)
  }

  /**
   * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
   * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
   * Reassigning replicas for a partition goes through a few stages -
   * RAR = Reassigned replicas
   * AR = Original list of replicas for partition
   * 1. Register listener for ISR changes to detect when the RAR is a subset of the ISR
   * 2. Start new replicas RAR - AR.
   * 3. Wait until new replicas are in sync with the leader
   * 4. If the leader is not in RAR, elect a new leader from RAR
   * 5. Stop old replicas AR - RAR
   * 6. Write new AR
   * 7. Remove partition from the /admin/reassign_partitions path
   */
  def onPartitionReassignment(topicAndPartition: TopicAndPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas) match {
      case true =>
        // mark the new replicas as online
        reassignedReplicas.foreach { replica =>
          replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
            replica)), OnlineReplica)
        }
        // check if current leader is in the new replicas list. If not, controller needs to trigger leader election
        moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext)
        // stop older replicas
        stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext)
        // write the new list of replicas for this partition in zookeeper
        updateAssignedReplicasForPartition(topicAndPartition, reassignedPartitionContext)
        // update the /admin/reassign_partitions path to remove this partition
        removePartitionFromReassignedPartitions(topicAndPartition)
        info("Removed partition %s from the list of reassigned partitions in zookeeper".format(topicAndPartition))
        controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
      case false =>
        info("New replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
          "reassigned not yet caught up with the leader")
        // start new replicas
        startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext)
        info("Waiting for new replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
          "reassigned to catch up with the leader")
    }
  }

  def onPreferredReplicaElection(partitions: Set[TopicAndPartition]) {
    info("Starting preferred replica leader election for partitions %s".format(partitions.mkString(",")))
    controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitions
    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, preferredReplicaPartitionLeaderSelector)
  }

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  def startup() = {
    controllerContext.controllerLock synchronized {
      info("Controller starting up");
      registerSessionExpirationListener()
      isRunning = true
      controllerElector.startup
      info("Controller startup complete")
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown() = {
    controllerContext.controllerLock synchronized {
      isRunning = false
      partitionStateMachine.shutdown()
      replicaStateMachine.shutdown()
      if(controllerContext.controllerChannelManager != null) {
        controllerContext.controllerChannelManager.shutdown()
        controllerContext.controllerChannelManager = null
        info("Controller shutdown complete")
      }
    }
  }

  def sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null) = {
    controllerContext.controllerChannelManager.sendRequest(brokerId, request, callback)
  }

  private def registerSessionExpirationListener() = {
    zkClient.subscribeStateChanges(new SessionExpirationListener())
  }

  private def initializeControllerContext() {
    controllerContext.liveBrokers = ZkUtils.getAllBrokersInCluster(zkClient).toSet
    controllerContext.liveBrokerIds = controllerContext.liveBrokers.map(_.id)
    controllerContext.allTopics = ZkUtils.getAllTopics(zkClient).toSet
    controllerContext.partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient,
      controllerContext.allTopics.toSeq)
    controllerContext.allLeaders = new mutable.HashMap[TopicAndPartition, Int]
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache()
    // start the channel manager
    startChannelManager()
    info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
    info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
    initializeAndMaybeTriggerPartitionReassignment()
    initializeAndMaybeTriggerPreferredReplicaElection()
  }

  private def initializeAndMaybeTriggerPartitionReassignment() {
    // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
    val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient)
    // check if they are already completed
    val reassignedPartitions = partitionsBeingReassigned.filter(partition =>
      controllerContext.partitionReplicaAssignment(partition._1) == partition._2.newReplicas).map(_._1)
    reassignedPartitions.foreach(p => removePartitionFromReassignedPartitions(p))
    controllerContext.partitionsBeingReassigned ++= partitionsBeingReassigned
    controllerContext.partitionsBeingReassigned --= reassignedPartitions
    info("Partitions being reassigned: %s".format(partitionsBeingReassigned.toString()))
    info("Partitions already reassigned: %s".format(reassignedPartitions.toString()))
    info("Resuming reassignment of partitions: %s".format(controllerContext.partitionsBeingReassigned.toString()))
    controllerContext.partitionsBeingReassigned.foreach(partition => onPartitionReassignment(partition._1, partition._2))
  }

  private def initializeAndMaybeTriggerPreferredReplicaElection() {
    // read the partitions undergoing preferred replica election from zookeeper path
    val partitionsUndergoingPreferredReplicaElection = ZkUtils.getPartitionsUndergoingPreferredReplicaElection(zkClient)
    // check if they are already completed
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter(partition =>
      controllerContext.allLeaders(partition) == controllerContext.partitionReplicaAssignment(partition).head)
    controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitionsUndergoingPreferredReplicaElection
    controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsThatCompletedPreferredReplicaElection
    info("Partitions undergoing preferred replica election: %s".format(partitionsUndergoingPreferredReplicaElection.mkString(",")))
    info("Partitions that completed preferred replica election: %s".format(partitionsThatCompletedPreferredReplicaElection.mkString(",")))
    info("Resuming preferred replica election for partitions: %s".format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
    onPreferredReplicaElection(controllerContext.partitionsUndergoingPreferredReplicaElection.toSet)
  }

  private def startChannelManager() {
    controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext.liveBrokers, config)
    controllerContext.controllerChannelManager.startup()
  }

  private def updateLeaderAndIsrCache() {
    val leaderAndIsrInfo = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, controllerContext.allTopics.toSeq)
    for((topicPartition, leaderAndIsr) <- leaderAndIsrInfo) {
      // If the leader specified in the leaderAndIsr is no longer alive, there is no need to recover it
      controllerContext.liveBrokerIds.contains(leaderAndIsr.leader) match {
        case true =>
          controllerContext.allLeaders.put(topicPartition, leaderAndIsr.leader)
        case false =>
          debug("While refreshing controller's leader and isr cache, leader %d for ".format(leaderAndIsr.leader) +
            "partition %s is dead, just ignore it".format(topicPartition))
      }
    }
  }

  private def areReplicasInIsr(topic: String, partition: Int, replicas: Seq[Int]): Boolean = {
    getLeaderAndIsrForPartition(zkClient, topic, partition) match {
      case Some(leaderAndIsr) =>
        val replicasNotInIsr = replicas.filterNot(r => leaderAndIsr.isr.contains(r))
        replicasNotInIsr.isEmpty
      case None => false
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicAndPartition: TopicAndPartition,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val currentLeader = controllerContext.allLeaders(topicAndPartition)
    if(!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
      info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
        "is not in the new list of replicas %s. Re-electing leader".format(reassignedReplicas.mkString(",")))
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
    }else {
      // check if the leader is alive or not
      controllerContext.liveBrokerIds.contains(currentLeader) match {
        case true =>
          info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
            "is already in the new list of replicas %s and is alive".format(reassignedReplicas.mkString(",")))
        case false =>
          info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
            "is already in the new list of replicas %s but is dead".format(reassignedReplicas.mkString(",")))
          partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
      }
    }
  }

  private def stopOldReplicasOfReassignedPartition(topicAndPartition: TopicAndPartition,
                                                   reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    // send stop replica state change request to the old replicas
    val oldReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).toSet -- reassignedReplicas.toSet
    // first move the replica to offline state (the controller removes it from the ISR)
    oldReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topic, partition, replica)), OfflineReplica)
    }
    // send stop replica command to the old replicas
    oldReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topic, partition, replica)), NonExistentReplica)
    }
  }

  private def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                                 reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.filter(_._1.topic.equals(topicAndPartition.topic))
    partitionsAndReplicasForThisTopic.put(topicAndPartition, reassignedReplicas)
    updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic)
    info("Updated assigned replicas for partition %s being reassigned to %s ".format(topicAndPartition, reassignedReplicas.mkString(",")))
    // update the assigned replica list after a successful zookeeper write
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas)
    // stop watching the ISR changes for this partition
    zkClient.unsubscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
      controllerContext.partitionsBeingReassigned(topicAndPartition).isrChangeListener)
    // update the assigned replica list
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas)
  }

  private def startNewReplicasForReassignedPartition(topicAndPartition: TopicAndPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext) {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    val assignedReplicaSet = Set.empty[Int] ++ controllerContext.partitionReplicaAssignment(topicAndPartition)
    val reassignedReplicaSet = Set.empty[Int] ++ reassignedPartitionContext.newReplicas
    val newReplicas: Seq[Int] = (reassignedReplicaSet -- assignedReplicaSet).toSeq
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), NewReplica)
    }
  }

  private def registerReassignedPartitionsListener() = {
    zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, new PartitionsReassignedListener(this))
  }

  private def registerPreferredReplicaElectionListener() {
    zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, new PreferredReplicaElectionListener(this))
  }

  def removePartitionFromReassignedPartitions(topicAndPartition: TopicAndPartition) {
    // read the current list of reassigned partitions from zookeeper
    val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient)
    // remove this partition from that list
    val updatedPartitionsBeingReassigned = partitionsBeingReassigned - topicAndPartition
    // write the new list to zookeeper
    ZkUtils.updatePartitionReassignmentData(zkClient, updatedPartitionsBeingReassigned.mapValues(_.newReplicas))
    // update the cache
    controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
  }

  def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                         newReplicaAssignmentForTopic: Map[TopicAndPartition, Seq[Int]]) {
    try {
      val zkPath = ZkUtils.getTopicPath(topicAndPartition.topic)
      val jsonPartitionMap = Utils.mapToJson(newReplicaAssignmentForTopic.map(e =>
        (e._1.partition.toString -> e._2.map(_.toString))))
      ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionMap)
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionMap))
    } catch {
      case e: ZkNoNodeException => throw new IllegalStateException("Topic %s doesn't exist".format(topicAndPartition.topic))
      case e2 => throw new KafkaException(e2.toString)
    }
  }

  def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicAndPartition]) {
    partitionsToBeRemoved.foreach { partition =>
      // check the status
      val currentLeader = controllerContext.allLeaders(partition)
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if(currentLeader == preferredReplica) {
        info("Partition %s completed preferred replica leader election. New leader is %d".format(partition, preferredReplica))
      }else {
        warn("Partition %s failed to complete preferred replica leader election. Leader is %d".format(partition, currentLeader))
      }
    }
    ZkUtils.deletePath(zkClient, ZkUtils.PreferredReplicaLeaderElectionPath)
    controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsToBeRemoved
  }

  private def getAllReplicasForPartition(partitions: Set[TopicAndPartition]): Set[PartitionAndReplica] = {
    partitions.map { p =>
      val replicas = controllerContext.partitionReplicaAssignment(p)
      replicas.map(r => new PartitionAndReplica(p.topic, p.partition, r))
    }.flatten
  }

  class SessionExpirationListener() extends IZkStateListener with Logging {
    this.logIdent = "[SessionExpirationListener on " + config.brokerId + "], "
    @throws(classOf[Exception])
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleNewSession() {
      controllerContext.controllerLock synchronized {
        partitionStateMachine.shutdown()
        replicaStateMachine.shutdown()
        if(controllerContext.controllerChannelManager != null) {
          info("session expires, clean up the state")
          controllerContext.controllerChannelManager.shutdown()
          controllerContext.controllerChannelManager = null
        }
        controllerElector.elect
      }
    }
  }
}

/**
 * Starts the partition reassignment process unless -
 * 1. Partition previously existed
 * 2. New replicas are the same as existing replicas
 * 3. Any replica in the new set of replicas are dead
 * If any of the above conditions are satisfied, it logs an error and removes the partition from list of reassigned
 * partitions.
 */
class PartitionsReassignedListener(controller: KafkaController) extends IZkDataListener with Logging {
  this.logIdent = "[PartitionsReassignedListener on " + controller.config.brokerId + "]: "
  val zkClient = controller.controllerContext.zkClient
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions are reassigned by the admin command
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    debug("Partitions reassigned listener fired for path %s. Record partitions to be reassigned %s"
      .format(dataPath, data))
    val partitionsReassignmentData = ZkUtils.parsePartitionReassignmentData(data.toString)
    val newPartitions = partitionsReassignmentData.filterNot(p => controllerContext.partitionsBeingReassigned.contains(p._1))
    newPartitions.foreach { partitionToBeReassigned =>
      controllerContext.controllerLock synchronized {
        val topic = partitionToBeReassigned._1.topic
        val partition = partitionToBeReassigned._1.partition
        val newReplicas = partitionToBeReassigned._2
        val topicAndPartition = partitionToBeReassigned._1
        val aliveNewReplicas = newReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
        try {
          val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicAndPartition)
          assignedReplicasOpt match {
            case Some(assignedReplicas) =>
              if(assignedReplicas == newReplicas) {
                throw new KafkaException("Partition %s to be reassigned is already assigned to replicas"
                  .format(topicAndPartition) +
                  " %s. Ignoring request for partition reassignment".format(newReplicas.mkString(",")))
              }else {
                if(aliveNewReplicas == newReplicas) {
                  info("Handling reassignment of partition %s to new replicas %s".format(topicAndPartition,
                    newReplicas.mkString(",")))
                  val context = createReassignmentContextForPartition(topic, partition, newReplicas)
                  controllerContext.partitionsBeingReassigned.put(topicAndPartition, context)
                  controller.onPartitionReassignment(topicAndPartition, context)
                }else {
                  // some replica in RAR is not alive. Fail partition reassignment
                  throw new KafkaException("Only %s replicas out of the new set of replicas".format(aliveNewReplicas.mkString(",")) +
                    " %s for partition %s to be reassigned are alive. ".format(newReplicas.mkString(","), topicAndPartition) +
                    "Failing partition reassignment")
                }
              }
            case None => throw new KafkaException("Attempt to reassign partition %s that doesn't exist"
              .format(topicAndPartition))
          }
        }catch {
          case e => error("Error completing reassignment of partition %s".format(topicAndPartition), e)
          // remove the partition from the admin path to unblock the admin client
          controller.removePartitionFromReassignedPartitions(topicAndPartition)
        }
      }
    }
  }

  /**
   * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }

  private def createReassignmentContextForPartition(topic: String,
                                                    partition: Int,
                                                    newReplicas: Seq[Int]): ReassignedPartitionsContext = {
    val context = new ReassignedPartitionsContext(newReplicas)
    // first register ISR change listener
    watchIsrChangesForReassignedPartition(topic, partition, context)
    context
  }

  private def watchIsrChangesForReassignedPartition(topic: String, partition: Int,
                                                    reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val isrChangeListener = new ReassignedPartitionsIsrChangeListener(controller, topic, partition,
      reassignedReplicas.toSet)
    reassignedPartitionContext.isrChangeListener = isrChangeListener
    // register listener on the leader and isr path to wait until they catch up with the current leader
    zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener)
  }
}

class ReassignedPartitionsIsrChangeListener(controller: KafkaController, topic: String, partition: Int,
                                            reassignedReplicas: Set[Int])
  extends IZkDataListener with Logging {
  this.logIdent = "[ReassignedPartitionsIsrChangeListener on controller " + controller.config.brokerId + "]: "
  val zkClient = controller.controllerContext.zkClient
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions need to move leader to preferred replica
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    try {
      controllerContext.controllerLock synchronized {
        debug("Reassigned partitions isr change listener fired for path %s with children %s".format(dataPath, data))
        // check if this partition is still being reassigned or not
        val topicAndPartition = TopicAndPartition(topic, partition)
        controllerContext.partitionsBeingReassigned.get(topicAndPartition) match {
          case Some(reassignedPartitionContext) =>
            // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object
            val newLeaderAndIsrOpt = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition)
            newLeaderAndIsrOpt match {
              case Some(leaderAndIsr) => // check if new replicas have joined ISR
                val caughtUpReplicas = reassignedReplicas & leaderAndIsr.isr.toSet
                if(caughtUpReplicas == reassignedReplicas) {
                  // resume the partition reassignment process
                  info("%d/%d replicas have caught up with the leader for partition [%s, %d] being reassigned."
                    .format(caughtUpReplicas.size, reassignedReplicas.size, topic, partition) +
                    "Resuming partition reassignment")
                  controller.onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
                }else {
                  info("%d/%d replicas have caught up with the leader for partition [%s, %d] being reassigned."
                    .format(caughtUpReplicas.size, reassignedReplicas.size, topic, partition) +
                    "Replica(s) %s still need to catch up".format((reassignedReplicas -- leaderAndIsr.isr.toSet).mkString(",")))
                }
              case None => error("Error handling reassignment of partition [%s, %d] to replicas %s as it was never created"
                .format(topic, partition, reassignedReplicas.mkString(",")))
            }
          case None =>
        }
      }
    }catch {
      case e => error("Error while handling partition reassignment", e)
    }
  }

  /**
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

/**
 * Starts the preferred replica leader election for the list of partitions specified under
 * /admin/preferred_replica_election -
 */
class PreferredReplicaElectionListener(controller: KafkaController) extends IZkDataListener with Logging {
  this.logIdent = "[PreferredReplicaElectionListener on " + controller.config.brokerId + "]: "
  val zkClient = controller.controllerContext.zkClient
  val controllerContext = controller.controllerContext

  /**
   * Invoked when some partitions are reassigned by the admin command
   * @throws Exception On any error.
   */
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    debug("Preferred replica election listener fired for path %s. Record partitions to undergo preferred replica election" +
      " %s".format(dataPath, data.toString))
    val partitionsForPreferredReplicaElection =
      PreferredReplicaLeaderElectionCommand.parsePreferredReplicaJsonData(data.toString)
    val newPartitions = partitionsForPreferredReplicaElection -- controllerContext.partitionsUndergoingPreferredReplicaElection
    controllerContext.controllerLock synchronized {
      try {
        controller.onPreferredReplicaElection(newPartitions)
      } catch {
        case e => error("Error completing preferred replica leader election for partitions %s"
          .format(partitionsForPreferredReplicaElection.mkString(",")), e)
      } finally {
        controller.removePartitionsFromPreferredReplicaElection(newPartitions)
      }
    }
  }

  /**
   * @throws Exception
   *             On any error.
   */
  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
  }
}

case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       var isrChangeListener: ReassignedPartitionsIsrChangeListener = null)

case class PartitionAndReplica(topic: String, partition: Int, replica: Int)

object ControllerStat extends KafkaMetricsGroup {
  val offlinePartitionRate = newMeter("OfflinePartitionsPerSec",  "partitions", TimeUnit.SECONDS)
  val uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec",  "elections", TimeUnit.SECONDS)
  val leaderElectionTimer = new KafkaTimer(newTimer("LeaderElectionRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
