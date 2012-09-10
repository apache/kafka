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

import collection.mutable.HashMap
import collection._
import collection.immutable.Set
import kafka.cluster.Broker
import kafka.api._
import kafka.network.{Receive, BlockingChannel}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.{IZkStateListener, ZkClient, IZkDataListener, IZkChildListener}
import org.apache.zookeeper.Watcher.Event.KeeperState
import collection.JavaConversions._
import kafka.utils.{ShutdownableThread, ZkUtils, Logging}
import java.lang.Object
import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue}
import kafka.common.{KafkaException, PartitionOfflineException}


class RequestSendThread(val controllerId: Int,
                        val toBrokerId: Int,
                        val queue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],
                        val channel: BlockingChannel)
  extends ShutdownableThread("Controller-%d-to-broker-%d-send-thread".format(controllerId, toBrokerId)) {
  private val lock = new Object()

  override def doWork(): Unit = {
    val queueItem = queue.take()
    val request = queueItem._1
    val callback = queueItem._2

    var receive: Receive = null

    try{
      lock synchronized {
        channel.send(request)
        receive = channel.receive()
        var response: RequestOrResponse = null
        request.requestId.get match {
          case RequestKeys.LeaderAndISRRequest =>
            response = LeaderAndISRResponse.readFrom(receive.buffer)
          case RequestKeys.StopReplicaRequest =>
            response = StopReplicaResponse.readFrom(receive.buffer)
        }
        trace("got a response %s".format(controllerId, response, toBrokerId))

        if(callback != null){
          callback(response)
        }
      }
    } catch {
      case e =>
        // log it and let it go. Let controller shut it down.
        debug("Exception occurs", e)
    }
  }
}

class ControllerChannelManager private (config: KafkaConfig) extends Logging {
  private val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "], "

  def this(allBrokers: Set[Broker], config : KafkaConfig) {
    this(config)
    allBrokers.foreach(addNewBroker(_))
  }

  def startup() = {
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => removeExistingBroker(brokerState._1))
    }
  }

  def sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null) {
    brokerLock synchronized {
      brokerStateInfo(brokerId).messageQueue.put((request, callback))
    }
  }

  def addBroker(broker: Broker) {
    brokerLock synchronized {
      addNewBroker(broker)
      startRequestSendThread(broker.id)
    }
  }

  def removeBroker(brokerId: Int) {
    brokerLock synchronized {
      removeExistingBroker(brokerId)
    }
  }

  private def addNewBroker(broker: Broker) {
    val messageQueue = new LinkedBlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)](config.controllerMessageQueueSize)
    val channel = new BlockingChannel(broker.host, broker.port,
      BlockingChannel.UseDefaultBufferSize,
      BlockingChannel.UseDefaultBufferSize,
      config.controllerSocketTimeoutMs)
    channel.connect()
    val requestThread = new RequestSendThread(config.brokerId, broker.id, messageQueue, channel)
    requestThread.setDaemon(false)
    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(channel, broker, messageQueue, requestThread))
  }

  private def removeExistingBroker(brokerId: Int) {
    try {
      brokerStateInfo(brokerId).channel.disconnect()
      brokerStateInfo(brokerId).requestSendThread.shutdown()
      brokerStateInfo.remove(brokerId)
    }catch {
      case e => error("Error while removing broker by the controller", e)
    }
  }

  private def startRequestSendThread(brokerId: Int) {
    brokerStateInfo(brokerId).requestSendThread.start()
  }
}

case class ControllerBrokerStateInfo(channel: BlockingChannel,
                                     broker: Broker,
                                     messageQueue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],
                                     requestSendThread: RequestSendThread)

class KafkaController(config : KafkaConfig, zkClient: ZkClient) extends Logging {
  this.logIdent = "[Controller " + config.brokerId + "], "
  private var isRunning = true
  private val controllerLock = new Object
  private var controllerChannelManager: ControllerChannelManager = null
  private var liveBrokers : Set[Broker] = null
  private var liveBrokerIds : Set[Int] = null
  private var allTopics: Set[String] = null
  private var allPartitionReplicaAssignment: mutable.Map[(String, Int), Seq[Int]] = null
  private var allLeaders: mutable.Map[(String, Int), Int] = null

  // Return true if this controller succeeds in the controller leader election
  private def tryToBecomeController(): Boolean = {
    val controllerStatus =
      try {
        ZkUtils.createEphemeralPathExpectConflict(zkClient, ZkUtils.ControllerPath, config.brokerId.toString)
        // Only the broker elected as the new controller can execute following code, otherwise
        // some exception will be thrown.
        registerBrokerChangeListener()
        registerTopicChangeListener()
        liveBrokers = ZkUtils.getAllBrokersInCluster(zkClient).toSet
        liveBrokerIds = liveBrokers.map(_.id)
        info("Currently active brokers in the cluster: %s".format(liveBrokerIds))
        allTopics = ZkUtils.getAllTopics(zkClient).toSet
        info("Current list of topics in the cluster: %s".format(allTopics))
        allPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, allTopics.iterator)
        info("Partition replica assignment: %s".format(allPartitionReplicaAssignment))
        allLeaders = new mutable.HashMap[(String, Int), Int]
        controllerChannelManager = new ControllerChannelManager(liveBrokers, config)
        controllerChannelManager.startup()
        true
      } catch {
        case e: ZkNodeExistsException =>
          registerControllerExistsListener()
          false
        case e2 => throw e2
      }
    controllerStatus
  }

  private def controllerRegisterOrFailover() {
    if(isRunning) {
      if(tryToBecomeController()) {
        readAndSendLeaderAndIsrFromZookeeper(liveBrokerIds, allTopics)
        onBrokerChange()
        // If there are some partition with leader not initialized, init the leader for them
        val partitionReplicaAssignment = allPartitionReplicaAssignment.filter(m => !allLeaders.contains(m._1))
        debug("work on init leaders: %s, current cache for all leader is: %s".format(partitionReplicaAssignment.toString(), allLeaders))
        initLeaders(partitionReplicaAssignment)
      }
    }else
      info("Controller has been shut down, aborting startup procedure")
  }

  def isActive(): Boolean = {
    controllerChannelManager != null
  }

  def startup() = {
    controllerLock synchronized {
      info("Controller starting up");
      registerSessionExpirationListener()
      registerControllerExistsListener()
      isRunning = true
      controllerRegisterOrFailover()
      info("Controller startup complete")
    }
  }

  def shutdown() = {
    controllerLock synchronized {
      if(controllerChannelManager != null) {
        info("Controller shutting down")
        controllerChannelManager.shutdown()
        controllerChannelManager = null
        info("Controller shutdown complete")
      }
      isRunning = false
    }
  }

  def sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null) = {
    controllerChannelManager.sendRequest(brokerId, request, callback)
  }

  private def registerBrokerChangeListener() = {
    zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, new BrokerChangeListener())
  }

  private def registerTopicChangeListener() = {
    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, new TopicChangeListener())
  }

  private def registerSessionExpirationListener() = {
    zkClient.subscribeStateChanges(new SessionExpireListener())
  }

  private def registerControllerExistsListener(){
    zkClient.subscribeDataChanges(ZkUtils.ControllerPath, new ControllerExistsListener())
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
      controllerLock synchronized {
        if(controllerChannelManager != null) {
          info("session expires, clean up the state")
          controllerChannelManager.shutdown()
          controllerChannelManager = null
        }
        controllerRegisterOrFailover()
      }
    }
  }

  /**
   * @param brokerIds The set of currently active brokers in the cluster, as known to the controller
   * @param topics The set of topics known to the controller by reading from zookeeper
   * This API reads the list of partitions that exist for all the topics in the specified list of input topics.
   * For each of those partitions, it reads the assigned replica list so that it can send the appropriate leader and
   * isr state change request to all the brokers in the assigned replica list. It arranges the leader and isr state
   * change requests by broker id. At the end, it circles through this map, sending the required INIT state change requests
   * to each broker. This API is called when -
   * 1. A new broker starts up
   * 2. A new controller is elected
   */
  private def readAndSendLeaderAndIsrFromZookeeper(brokerIds: Set[Int], topics: Set[String]) = {
    val leaderAndIsrInfo = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, topics.iterator)
    val brokerToLeaderAndIsrInfoMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int), LeaderAndIsr]]
    for((topicPartition, leaderAndIsr) <- leaderAndIsrInfo) {
      // If the leader specified in the leaderAndIsr is no longer alive, there is no need to recover it
      liveBrokerIds.contains(leaderAndIsr.leader) match {
        case true =>
          val brokersAssignedToThisPartitionOpt = allPartitionReplicaAssignment.get(topicPartition)
          brokersAssignedToThisPartitionOpt match {
            case Some(brokersAssignedToThisPartition) =>
              val relatedBrokersAssignedToThisPartition = brokersAssignedToThisPartitionOpt.get.filter(brokerIds.contains(_))
              relatedBrokersAssignedToThisPartition.foreach(b => {
                brokerToLeaderAndIsrInfoMap.getOrElseUpdate(b, new mutable.HashMap[(String, Int), LeaderAndIsr])
                brokerToLeaderAndIsrInfoMap(b).put(topicPartition, leaderAndIsr)
              })
              allLeaders.put(topicPartition, leaderAndIsr.leader)
            case None => warn(("While refreshing controller's leader and isr cache, no replica assignment was found " +
              "for partition [%s, %d]. Rest of the partition replica assignment is %s").format(topicPartition._1,
              topicPartition._2, allPartitionReplicaAssignment))
          }
        case false =>
          debug("While refreshing controller's leader and isr cache, broker %d is not alive any more, just ignore it"
            .format(leaderAndIsr.leader))
      }
    }
    debug(("While refreshing controller's leader and isr cache, the state change requests for each broker is " +
      "[%s]").format(brokerToLeaderAndIsrInfoMap.toString()))

    brokerToLeaderAndIsrInfoMap.foreach(m =>{
      val broker = m._1
      val leaderAndIsrs = m._2
      val leaderAndIsrRequest = new LeaderAndIsrRequest(LeaderAndIsrRequest.IsInit, leaderAndIsrs)
      info("After refreshing controller's leader and isr cache, the leader and ISR change state change request sent to" +
        " new broker [%s] is [%s]".format(broker, leaderAndIsrRequest.toString))
      sendRequest(broker, leaderAndIsrRequest)
    })
    info("After refreshing controller's leader and isr cache for brokers %s, the leaders assignment is %s"
      .format(brokerIds, allLeaders))
  }

  private def initLeaders(partitionReplicaAssignment: collection.mutable.Map[(String, Int), Seq[Int]]) {
    val brokerToLeaderAndISRInfoMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int),LeaderAndIsr]]
    for((topicPartition, replicaAssignment) <- partitionReplicaAssignment) {
      val liveAssignedReplicas = replicaAssignment.filter(r => liveBrokerIds.contains(r))
      debug("for topic [%s], partition [%d], live assigned replicas are: [%s]"
        .format(topicPartition._1,
        topicPartition._2,
        liveAssignedReplicas))
      if(!liveAssignedReplicas.isEmpty) {
        debug("live assigned replica is not empty, check zkClient: %s".format(zkClient))
        val leader = liveAssignedReplicas.head
        var leaderAndISR: LeaderAndIsr = null
        var updateLeaderISRZKPathSucceeded: Boolean = false
        while(!updateLeaderISRZKPathSucceeded) {
          val curLeaderAndISROpt = ZkUtils.getLeaderAndIsrForPartition(zkClient, topicPartition._1, topicPartition._2)
          debug("curLeaderAndISROpt is %s, zkClient is %s ".format(curLeaderAndISROpt, zkClient))
          if(curLeaderAndISROpt == None){
            debug("during initializing leader of parition (%s, %d), the current leader and isr in zookeeper is empty".format(topicPartition._1, topicPartition._2))
            leaderAndISR = new LeaderAndIsr(leader, liveAssignedReplicas.toList)
            ZkUtils.createPersistentPath(zkClient, ZkUtils.getTopicPartitionLeaderAndIsrPath(topicPartition._1, topicPartition._2), leaderAndISR.toString)
            updateLeaderISRZKPathSucceeded = true
          } else {
            debug("During initializing leader of parition (%s, %d),".format(topicPartition._1, topicPartition._2) +
              " the current leader and isr in zookeeper is not empty")
            val curZkPathVersion = curLeaderAndISROpt.get.zkVersion
            leaderAndISR = new LeaderAndIsr(leader, curLeaderAndISROpt.get.leaderEpoch + 1,liveAssignedReplicas.toList,
              curLeaderAndISROpt.get.zkVersion + 1)
            val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(zkClient,
              ZkUtils.getTopicPartitionLeaderAndIsrPath(topicPartition._1, topicPartition._2),
              leaderAndISR.toString, curZkPathVersion)
            if(updateSucceeded) {
              leaderAndISR.zkVersion = newVersion
            }
            updateLeaderISRZKPathSucceeded = updateSucceeded
          }
        }
        liveAssignedReplicas.foreach(b => {
          if(!brokerToLeaderAndISRInfoMap.contains(b))
            brokerToLeaderAndISRInfoMap.put(b, new mutable.HashMap[(String, Int), LeaderAndIsr])
          brokerToLeaderAndISRInfoMap(b).put(topicPartition, leaderAndISR)
        }
        )
        allLeaders.put(topicPartition, leaderAndISR.leader)
      }
      else{
        warn("during initializing leader of parition (%s, %d), assigned replicas are [%s], live brokers are [%s], no assigned replica is alive".format(topicPartition._1, topicPartition._2, replicaAssignment.mkString(","), liveBrokerIds))
      }
    }

    info("after leaders initialization for partition replica assignments %s, the cached leaders in controller is %s, and the broker to request map is: %s".format(partitionReplicaAssignment, allLeaders, brokerToLeaderAndISRInfoMap))
    brokerToLeaderAndISRInfoMap.foreach(m =>{
      val broker = m._1
      val leaderAndISRs = m._2
      val leaderAndISRRequest = new LeaderAndIsrRequest(LeaderAndIsrRequest.NotInit, leaderAndISRs)
      info("at initializing leaders for new partitions, the leaderAndISR request sent to broker %d is %s".format(broker, leaderAndISRRequest))
      sendRequest(broker, leaderAndISRRequest)
    })
  }

  /**
   * @param newBrokers The list of brokers that are started up. This is an optional argument that can be empty when
   * new controller is being elected
   * The purpose of this API is to send the leader state change request to all live replicas of partitions that
   * currently don't have an alive leader. It first finds the partitions with dead leaders, then it looks up the list
   * of assigned replicas for those partitions that are alive. It reads the leader and isr info for those partitions
   * from zookeeper.
   * It can happen that when the controller is in the middle of updating the new leader info in zookeeper,
   * the leader changes the ISR for the partition. Due to this, the zookeeper path's version will be different than
   * what was known to the controller. So it's new leader update will fail. The controller retries the leader election
   * based on the new ISR until it's leader update in zookeeper succeeds.
   * Once the write to zookeeper succeeds, it sends the leader state change request to the live assigned replicas for
   * each affected partition.
   */
  private def onBrokerChange(newBrokers: Set[Int] = Set.empty[Int]) {
    /** handle the new brokers, send request for them to initialize the local log **/
    if(newBrokers.size != 0)
      readAndSendLeaderAndIsrFromZookeeper(newBrokers, allTopics)

    /** handle leader election for the partitions whose leader is no longer alive **/
    val brokerToLeaderAndIsrInfoMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int), LeaderAndIsr]]
    // retain only partitions whose leaders are not alive
    val partitionsWithDeadLeaders = allLeaders.filter(partitionAndLeader => !liveBrokerIds.contains(partitionAndLeader._2))
    partitionsWithDeadLeaders.foreach { partitionAndLeader =>
      val topic = partitionAndLeader._1._1
      val partition = partitionAndLeader._1._2

      try {
        allPartitionReplicaAssignment.get((topic, partition)) match {
          case Some(assignedReplicas) =>
            val liveAssignedReplicasToThisPartition = assignedReplicas.filter(r => liveBrokerIds.contains(r))
            ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition) match {
              case Some(currentLeaderAndIsr) =>
                try {
                  // elect new leader or throw exception
                  val newLeaderAndIsr = electLeaderForPartition(topic, partition, currentLeaderAndIsr, assignedReplicas)
                  // store new leader and isr info in cache
                  liveAssignedReplicasToThisPartition.foreach { b =>
                    brokerToLeaderAndIsrInfoMap.getOrElseUpdate(b, new mutable.HashMap[(String, Int), LeaderAndIsr])
                    brokerToLeaderAndIsrInfoMap(b).put((topic, partition), newLeaderAndIsr)
                  }
                }catch {
                  case e => error("Error while electing leader for partition [%s, %d]".format(topic, partition))
                }
              case None => throw new KafkaException(("On broker changes, " +
                "there's no leaderAndISR information for partition (%s, %d) in zookeeper").format(topic, partition))
            }
          case None => throw new KafkaException(("While handling broker changes, the " +
            "partition [%s, %d] doesn't have assigned replicas. The replica assignment for other partitions is %s")
            .format(topic, partition, allPartitionReplicaAssignment))
        }
      }catch {
        case e: PartitionOfflineException =>
          error("All replicas for partition [%s, %d] are dead.".format(topic, partition) +
            " Marking this partition offline")
      }
    }
    debug("After leader election, leader cache is updated to %s".format(allLeaders))
    brokerToLeaderAndIsrInfoMap.foreach(m => {
      val broker = m._1
      val leaderAndISRInfo = m._2
      val leaderAndISRRequest = new LeaderAndIsrRequest(LeaderAndIsrRequest.NotInit, leaderAndISRInfo)
      sendRequest(broker, leaderAndISRRequest)
      info("On broker changes, the LeaderAndIsrRequest send to broker [%d] is [%s]".format(broker, leaderAndISRRequest))
    })
  }

  /**
   * @param topic                      The topic of the partition whose leader needs to be elected
   * @param partition                  The partition whose leader needs to be elected
   * @param currentLeaderAndIsr        The leader and isr information stored for this partition in zookeeper
   * @param assignedReplicas           The list of replicas assigned to the input partition
   * @throws PartitionOfflineException If no replica in the assigned replicas list is alive
   * This API selects a new leader for the input partition -
   * 1. If at least one broker from the isr is alive, it picks a broker from the isr as the new leader
   * 2. Else, it picks some alive broker from the assigned replica list as the new leader
   * 3. If no broker in the assigned replica list is alive, it throws PartitionOfflineException
   * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
   * TODO: If a leader cannot be elected for a partition, it should be marked offline and exposed through some metric
   */
  private def electLeaderForPartition(topic: String, partition: Int, currentLeaderAndIsr: LeaderAndIsr,
                                      assignedReplicas: Seq[Int]):LeaderAndIsr = {
    var zookeeperPathUpdateSucceeded: Boolean = false
    var newLeaderAndIsr: LeaderAndIsr = currentLeaderAndIsr
    while(!zookeeperPathUpdateSucceeded) {
      val liveAssignedReplicasToThisPartition = assignedReplicas.filter(r => liveBrokerIds.contains(r))
      val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => liveBrokerIds.contains(r))
      val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
      val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
      debug("Leader, epoch, ISR and zkPathVersion for partition (%s, %d) are: [%d], [%d], [%s], [%d]"
        .format(topic, partition, currentLeaderAndIsr.leader, currentLeaderEpoch, currentLeaderAndIsr.isr,
        currentLeaderIsrZkPathVersion))
      newLeaderAndIsr = liveBrokersInIsr.isEmpty match {
        case true =>
          debug("No broker is ISR is alive, picking the leader from the alive assigned replicas: %s"
            .format(liveAssignedReplicasToThisPartition.mkString(",")))
          liveAssignedReplicasToThisPartition.isEmpty match {
            case true => throw new PartitionOfflineException(("No replica for partition " +
              "([%s, %d]) is alive. Live brokers are: [%s],".format(topic, partition, liveBrokerIds)) +
              " Assigned replicas are: [%s]".format(assignedReplicas))
            case false =>
              val newLeader = liveAssignedReplicasToThisPartition.head
              warn("No broker in ISR is alive, elected leader from the alive replicas is [%s], ".format(newLeader) +
                "There's potential data loss")
              new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, List(newLeader), currentLeaderIsrZkPathVersion + 1)
          }
        case false =>
          val newLeader = liveBrokersInIsr.head
          debug("Some broker in ISR is alive, picking the leader from the ISR: " + newLeader)
          new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr.toList, currentLeaderIsrZkPathVersion + 1)
      }
      info("New leader and ISR for partition [%s, %d] is %s".format(topic, partition, newLeaderAndIsr.toString()))
      // update the new leadership decision in zookeeper or retry
      val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(zkClient,
        ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
        newLeaderAndIsr.toString, currentLeaderAndIsr.zkVersion)
      newLeaderAndIsr.zkVersion = newVersion
      zookeeperPathUpdateSucceeded = updateSucceeded
    }
    // update the leader cache
    allLeaders.put((topic, partition), newLeaderAndIsr.leader)
    newLeaderAndIsr
  }

  class BrokerChangeListener() extends IZkChildListener with Logging {
    this.logIdent = "[Controller " + config.brokerId + "], "
    def handleChildChange(parentPath : String, currentBrokerList : java.util.List[String]) {
      controllerLock synchronized {
        val curBrokerIds = currentBrokerList.map(_.toInt).toSet
        val newBrokerIds = curBrokerIds -- liveBrokerIds
        val newBrokers = newBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
        val deletedBrokerIds = liveBrokerIds -- curBrokerIds
        liveBrokers = curBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
        liveBrokerIds = liveBrokers.map(_.id)
        info("Newly added brokers: %s, deleted brokers: %s, all brokers: %s"
          .format(newBrokerIds.mkString(","), deletedBrokerIds.mkString(","), liveBrokerIds.mkString(",")))
        newBrokers.foreach(controllerChannelManager.addBroker(_))
        deletedBrokerIds.foreach(controllerChannelManager.removeBroker(_))
        onBrokerChange(newBrokerIds)
      }
    }
  }

  private def handleNewTopics(topics: Set[String], partitionReplicaAssignment: mutable.Map[(String, Int), Seq[Int]]) {
    // get relevant partitions to this broker
    val partitionReplicaAssignment = allPartitionReplicaAssignment.filter(p => topics.contains(p._1._1))
    debug("handling new topics, the partition replica assignment to be handled is %s".format(partitionReplicaAssignment))
    initLeaders(partitionReplicaAssignment)
  }

  private def handleDeletedTopics(topics: Set[String], partitionReplicaAssignment: mutable.Map[(String, Int), Seq[Int]]) {
    val brokerToPartitionToStopReplicaMap = new collection.mutable.HashMap[Int, collection.mutable.HashSet[(String, Int)]]
    for((topicPartition, brokers) <- partitionReplicaAssignment){
      for (broker <- brokers){
        if (!brokerToPartitionToStopReplicaMap.contains(broker))
          brokerToPartitionToStopReplicaMap.put(broker, new collection.mutable.HashSet[(String, Int)])
        brokerToPartitionToStopReplicaMap(broker).add(topicPartition)
      }
      allLeaders.remove(topicPartition)
      info("after deleting topics %s, allLeader is updated to %s and the broker to stop replia request map is %s".format(topics, allLeaders, brokerToPartitionToStopReplicaMap))
      ZkUtils.deletePath(zkClient, ZkUtils.getTopicPartitionLeaderAndIsrPath(topicPartition._1, topicPartition._2))
    }
    for((broker, partitionToStopReplica) <- brokerToPartitionToStopReplicaMap){
      val stopReplicaRequest = new StopReplicaRequest(partitionToStopReplica)
      info("handling deleted topics: [%s] the stopReplicaRequest sent to broker %d is [%s]".format(topics, broker, stopReplicaRequest))
      sendRequest(broker, stopReplicaRequest)
    }
    /* TODO: kafka-330  remove the unneeded leaderAndISRPath that the previous controller didn't get a chance to remove*/
  }

  class TopicChangeListener extends IZkChildListener with Logging {
    this.logIdent = "[Controller " + config.brokerId + "], "

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]) {
      controllerLock synchronized {
        info("topic/partition change listener fired for path " + parentPath)
        val currentChildren = JavaConversions.asBuffer(children).toSet
        val newTopics = currentChildren -- allTopics
        val deletedTopics = allTopics -- currentChildren
        val deletedPartitionReplicaAssignment = allPartitionReplicaAssignment.filter(p => deletedTopics.contains(p._1._1))
        allTopics = currentChildren

        val addedPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, newTopics.iterator)
        allPartitionReplicaAssignment = allPartitionReplicaAssignment.filter(p => !deletedTopics.contains(p._1._1))
        allPartitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
        info("new topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics, deletedTopics, allPartitionReplicaAssignment))
        handleNewTopics(newTopics, addedPartitionReplicaAssignment)
        handleDeletedTopics(deletedTopics, deletedPartitionReplicaAssignment)
      }
    }
  }

  class ControllerExistsListener extends IZkDataListener with Logging {
    this.logIdent = "[Controller " + config.brokerId + "], "

    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      // do nothing, since No logic is needed here
    }

    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      controllerLock synchronized {
        info("Current controller failed, participating in election for a new controller")
        controllerRegisterOrFailover()
      }
    }
  }
}