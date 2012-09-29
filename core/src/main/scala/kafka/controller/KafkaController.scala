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
import org.I0Itec.zkclient.{IZkStateListener, ZkClient}
import org.apache.zookeeper.Watcher.Event.KeeperState
import kafka.utils.{ZkUtils, Logging}
import java.lang.Object
import kafka.server.{ZookeeperLeaderElector, KafkaConfig}
import java.util.concurrent.TimeUnit
import kafka.metrics.{KafkaTimer, KafkaMetricsGroup}
import com.yammer.metrics.core.Gauge

class ControllerContext(val zkClient: ZkClient,
                        var controllerChannelManager: ControllerChannelManager = null,
                        val controllerLock: Object = new Object,
                        var liveBrokers: Set[Broker] = null,
                        var liveBrokerIds: Set[Int] = null,
                        var allTopics: Set[String] = null,
                        var partitionReplicaAssignment: mutable.Map[(String, Int), Seq[Int]] = null,
                        var allLeaders: mutable.Map[(String, Int), Int] = null)

class KafkaController(val config : KafkaConfig, zkClient: ZkClient) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Controller " + config.brokerId + "], "
  private var isRunning = true
  val controllerContext = new ControllerContext(zkClient)
  private val partitionStateMachine = new PartitionStateMachine(this)
  private val replicaStateMachine = new ReplicaStateMachine(this)
  private val controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover,
    config.brokerId)

  newGauge(
    "ActiveControllerCount",
    new Gauge[Int] {
      def value() = if (isActive) 1 else 0
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
    replicaStateMachine.handleStateChanges(newBrokers, OnlineReplica)
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
      deadBrokers.contains(partitionAndLeader._2)).keySet.toSeq
    partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // handle dead replicas
    replicaStateMachine.handleStateChanges(deadBrokers, OfflineReplica)
  }

  /**
   * This callback is invoked by the partition state machine's topic change listener with the list of failed brokers
   * as input. It does the following -
   * 1. Registers partition change listener. This is not required until KAFKA-347
   * 2. Invokes the new partition callback
   */
  def onNewTopicCreation(topics: Set[String], newPartitions: Seq[(String, Int)]) {
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
  def onNewPartitionCreation(newPartitions: Seq[(String, Int)]) {
    info("New partition creation callback for %s".format(newPartitions.mkString(",")))
    partitionStateMachine.handleStateChanges(newPartitions, NewPartition)
    partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition)
  }

  /* TODO: kafka-330  This API is unused until we introduce the delete topic functionality.
  remove the unneeded leaderAndISRPath that the previous controller didn't get a chance to remove*/
  def onTopicDeletion(topics: Set[String], replicaAssignment: mutable.Map[(String, Int), Seq[Int]]) {
    val brokerToPartitionToStopReplicaMap = new collection.mutable.HashMap[Int, collection.mutable.HashSet[(String, Int)]]
    for((topicPartition, brokers) <- replicaAssignment){
      for (broker <- brokers){
        if (!brokerToPartitionToStopReplicaMap.contains(broker))
          brokerToPartitionToStopReplicaMap.put(broker, new collection.mutable.HashSet[(String, Int)])
        brokerToPartitionToStopReplicaMap(broker).add(topicPartition)
      }
      controllerContext.allLeaders.remove(topicPartition)
      ZkUtils.deletePath(zkClient, ZkUtils.getTopicPartitionLeaderAndIsrPath(topicPartition._1, topicPartition._2))
    }
    for((broker, partitionToStopReplica) <- brokerToPartitionToStopReplicaMap){
      val stopReplicaRequest = new StopReplicaRequest(partitionToStopReplica)
      info("Handling deleted topics: [%s] the stopReplicaRequest sent to broker %d is [%s]".format(topics, broker, stopReplicaRequest))
      sendRequest(broker, stopReplicaRequest)
    }
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
    zkClient.subscribeStateChanges(new SessionExpireListener())
  }

  private def initializeControllerContext() {
    controllerContext.liveBrokers = ZkUtils.getAllBrokersInCluster(zkClient).toSet
    controllerContext.liveBrokerIds = controllerContext.liveBrokers.map(_.id)
    controllerContext.allTopics = ZkUtils.getAllTopics(zkClient).toSet
    controllerContext.partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient,
      controllerContext.allTopics.toSeq)
    controllerContext.allLeaders = new mutable.HashMap[(String, Int), Int]
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache()
    // start the channel manager
    startChannelManager()
    info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
    info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
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
            "partition [%s, %d] is dead, just ignore it".format(topicPartition._1, topicPartition._2))
      }
    }
  }

  class SessionExpireListener() extends IZkStateListener with Logging {
    this.logIdent = "[Controller " + config.brokerId + "], "
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

object ControllerStat extends KafkaMetricsGroup {
  val offlinePartitionRate = newMeter("OfflinePartitionsPerSec",  "partitions", TimeUnit.SECONDS)
  val uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec",  "elections", TimeUnit.SECONDS)
  val leaderElectionTimer = new KafkaTimer(newTimer("LeaderElectionRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
