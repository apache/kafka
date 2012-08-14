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
import kafka.utils.{ZkUtils, Logging}
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, BlockingQueue}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import java.util.concurrent.atomic.AtomicBoolean
import org.I0Itec.zkclient.{IZkStateListener, ZkClient, IZkDataListener, IZkChildListener}
import org.apache.zookeeper.Watcher.Event.KeeperState
import collection.JavaConversions._
import java.lang.Object

class RequestSendThread(val controllerId: Int,
                        val toBrokerId: Int,
                        val queue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],
                        val channel: BlockingChannel)
        extends Thread("requestSendThread-" + toBrokerId) with Logging {
  this.logIdent = "Controller %d, request send thread to broker %d, ".format(controllerId, toBrokerId)
  val isRunning: AtomicBoolean = new AtomicBoolean(true)
  private val shutDownLatch = new CountDownLatch(1)
  private val lock = new Object()

  def shutDown(): Unit = {
    info("shutting down")
    isRunning.set(false)
    interrupt()
    shutDownLatch.await()
    info("shutted down completed")
  }

  override def run(): Unit = {
    try{
      while(isRunning.get()){
        val queueItem = queue.take()
        val request = queueItem._1
        val callback = queueItem._2

        var receive: Receive = null
        try{
          lock synchronized {
            channel.send(request)
            receive = channel.receive()
          }
        } catch {
          case e =>
            // log it and let it go. Let controller shut it down.
            debug("Exception occurs", e)
        }

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
    } catch{
      case e: InterruptedException => warn("intterrupted. Shutting down")
      case e1 => error("Error due to ", e1)
    }
    shutDownLatch.countDown()
  }
}

class ControllerChannelManager(allBrokers: Set[Broker], config : KafkaConfig) extends Logging{
  private val brokers = new HashMap[Int, Broker]
  private val messageChannels = new HashMap[Int, BlockingChannel]
  private val messageQueues = new HashMap[Int, BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)]]
  private val messageThreads = new HashMap[Int, RequestSendThread]
  this.logIdent = "Channel manager on controller " + config.brokerId + ", "
  for(broker <- allBrokers){
    brokers.put(broker.id, broker)
    info("channel to broker " + broker.id + " created" + " at host: " + broker.host + " and port: " + broker.port)
    val channel = new BlockingChannel(broker.host, broker.port,
                                      BlockingChannel.UseDefaultBufferSize,
                                      BlockingChannel.UseDefaultBufferSize,
                                      config.controllerSocketTimeoutMs)
    channel.connect()
    messageChannels.put(broker.id, channel)
    messageQueues.put(broker.id, new LinkedBlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)](config.controllerMessageQueueSize))
  }

  def startUp() = {
    for((brokerId, broker) <- brokers){
      val thread = new RequestSendThread(config.brokerId, brokerId, messageQueues(brokerId), messageChannels(brokerId))
      thread.setDaemon(false)
      thread.start()
      messageThreads.put(broker.id, thread)
    }
  }

  def shutDown() = {
    for((brokerId, broker) <- brokers){
      removeBroker(brokerId)
    }
  }

  def sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null){
    messageQueues(brokerId).put((request, callback))
  }

  def addBroker(broker: Broker){
    brokers.put(broker.id, broker)
    messageQueues.put(broker.id, new LinkedBlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)](config.controllerMessageQueueSize))
    val channel = new BlockingChannel(broker.host, broker.port,
                                      BlockingChannel.UseDefaultBufferSize,
                                      BlockingChannel.UseDefaultBufferSize,
                                      config.controllerSocketTimeoutMs)
    channel.connect()
    messageChannels.put(broker.id, channel)
    val thread = new RequestSendThread(config.brokerId, broker.id, messageQueues(broker.id), messageChannels(broker.id))
    thread.setDaemon(false)
    thread.start()
    messageThreads.put(broker.id, thread)
  }

  def removeBroker(brokerId: Int){
    brokers.remove(brokerId)
    try {
      messageChannels(brokerId).disconnect()
      messageChannels.remove(brokerId)
      messageQueues.remove(brokerId)
      messageThreads(brokerId).shutDown()
      messageThreads.remove(brokerId)
    }catch {
      case e => error("Error while removing broker by the controller", e)
    }
  }
}

class KafkaController(config : KafkaConfig, zkClient: ZkClient) extends Logging {
  this.logIdent = "Controller " + config.brokerId + ", "
  info("startup");
  private val controllerLock = new Object
  private var controllerChannelManager: ControllerChannelManager = null
  private var allBrokers : Set[Broker] = null
  private var allBrokerIds : Set[Int] = null
  private var allTopics: Set[String] = null
  private var allPartitionReplicaAssignment: mutable.Map[(String, Int), Seq[Int]] = null
  private var allLeaders: mutable.Map[(String, Int), Int] = null

  // Return true if this controller succeeds in the controller competition
  private def tryToBecomeController(): Boolean = {
    try {
      ZkUtils.createEphemeralPathExpectConflict(zkClient, ZkUtils.ControllerPath, config.brokerId.toString)
      // Only the broker successfully registering as the controller can execute following code, otherwise
      // some exception will be thrown.
      registerBrokerChangeListener()
      registerTopicChangeListener()
      allBrokers = ZkUtils.getAllBrokersInCluster(zkClient).toSet
      allBrokerIds = allBrokers.map(_.id)
      info("all brokers: %s".format(allBrokerIds))
      allTopics = ZkUtils.getAllTopics(zkClient).toSet
      info("all topics: %s".format(allTopics))
      allPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, allTopics.iterator)
      info("allPartitionReplicaAssignment: %s".format(allPartitionReplicaAssignment))
      allLeaders = new mutable.HashMap[(String, Int), Int]
      controllerChannelManager = new ControllerChannelManager(allBrokers, config)
      controllerChannelManager.startUp()
      return true
    } catch {
      case e: ZkNodeExistsException =>
        registerControllerExistListener()
        info("broker didn't succeed registering as the controller since it's taken by someone else")
        return false
      case e2 => throw e2
    }
  }

  private def controllerRegisterOrFailover(){
    info("try to become controller")
    if(tryToBecomeController() == true){
      info("won the controller competition and work on leader and isr recovery")
      deliverLeaderAndISRFromZookeeper(allBrokerIds, allTopics)
      debug("work on broker changes")
      onBrokerChange()

      // If there are some partition with leader not initialized, init the leader for them
      val partitionReplicaAssignment = allPartitionReplicaAssignment.clone()
      for((topicPartition, replicas) <- partitionReplicaAssignment){
        if (allLeaders.contains(topicPartition)){
          partitionReplicaAssignment.remove(topicPartition)
        }
      }
      debug("work on init leaders: %s, current cache for all leader is: %s".format(partitionReplicaAssignment.toString(), allLeaders))
      initLeaders(partitionReplicaAssignment)
    }
  }

  def isActive(): Boolean = {
    controllerChannelManager != null
  }

  def startup() = {
    controllerLock synchronized {
      registerSessionExpirationListener()
      registerControllerExistListener()
      controllerRegisterOrFailover()
    }
  }

  def shutDown() = {
    controllerLock synchronized {
      if(controllerChannelManager != null){
        info("shut down")
        controllerChannelManager.shutDown()
        controllerChannelManager = null
        info("shutted down completely")
      }
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

  private def registerControllerExistListener(){
    zkClient.subscribeDataChanges(ZkUtils.ControllerPath, new ControllerExistListener())
  }

  class SessionExpireListener() extends IZkStateListener with Logging {
    this.logIdent = "Controller " + config.brokerId + ", "
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
        info("session expires, clean up the state")
        controllerChannelManager.shutDown()
        controllerChannelManager = null
        controllerRegisterOrFailover()
      }
    }
  }

  /**
   * Used to populate the leaderAndISR from zookeeper to affected brokers when the brokers comes up
   */
  private def deliverLeaderAndISRFromZookeeper(brokerIds: Set[Int], topics: Set[String]) = {
    val leaderAndISRInfos = ZkUtils.getPartitionLeaderAndISRForTopics(zkClient, topics.iterator)
    val brokerToLeaderAndISRInfosMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int), LeaderAndISR]]
    for((topicPartition, leaderAndISR) <- leaderAndISRInfos){
      // If the leader specified in the leaderAndISR is no longer alive, there is no need to recover it
      if(allBrokerIds.contains(leaderAndISR.leader)){
        val brokersAssignedToThisPartitionOpt = allPartitionReplicaAssignment.get(topicPartition)
        if(brokersAssignedToThisPartitionOpt == None){
          warn("during leaderAndISR recovery, there's no replica assignment for partition [%s, %d] with allPartitionReplicaAssignment: %s".format(topicPartition._1, topicPartition._2, allPartitionReplicaAssignment))
        } else{
          val relatedBrokersAssignedToThisPartition = brokersAssignedToThisPartitionOpt.get.filter(brokerIds.contains(_))
          relatedBrokersAssignedToThisPartition.foreach(b => {
            if(!brokerToLeaderAndISRInfosMap.contains(b))
              brokerToLeaderAndISRInfosMap.put(b, new mutable.HashMap[(String, Int), LeaderAndISR])
            brokerToLeaderAndISRInfosMap(b).put(topicPartition, leaderAndISR)
          })
          allLeaders.put(topicPartition, leaderAndISR.leader)
        }
      } else
        debug("during leaderAndISR recovery, the leader %d is not alive any more, just ignore it".format(leaderAndISR.leader))
    }
    info("during leaderAndISR recovery, the broker to request map is [%s]".format(brokerToLeaderAndISRInfosMap.toString()))

    brokerToLeaderAndISRInfosMap.foreach(m =>{
      val broker = m._1
      val leaderAndISRs = m._2
      val leaderAndISRRequest = new LeaderAndISRRequest(LeaderAndISRRequest.IsInit, leaderAndISRs)
      info("during leaderAndISR recovery, the leaderAndISRRequest sent to new broker [%s] is [%s]".format(broker, leaderAndISRRequest.toString))
      sendRequest(broker, leaderAndISRRequest)
    })

    info("after leaderAndISR recovery for brokers %s, the leaders assignment is %s".format(brokerIds, allLeaders))
  }


  private def initLeaders(partitionReplicaAssignment: collection.mutable.Map[(String, Int), Seq[Int]]) {
    val brokerToLeaderAndISRInfosMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int),LeaderAndISR]]
    for((topicPartition, replicaAssignment) <- partitionReplicaAssignment) {
      val liveAssignedReplicas = replicaAssignment.filter(r => allBrokerIds.contains(r))
      debug("for topic [%s], partition [%d], live assigned replicas are: [%s]"
                    .format(topicPartition._1,
                            topicPartition._2,
                            liveAssignedReplicas))
      if(!liveAssignedReplicas.isEmpty){
        debug("live assigned replica is not empty, check zkClient: %s".format(zkClient))
        val leader = liveAssignedReplicas.head
        var leaderAndISR: LeaderAndISR = null
        var updateLeaderISRZKPathSucceeded: Boolean = false
        while(!updateLeaderISRZKPathSucceeded){
          val curLeaderAndISROpt = ZkUtils.getLeaderAndISRForPartition(zkClient, topicPartition._1, topicPartition._2)
          debug("curLeaderAndISROpt is %s, zkClient is %s ".format(curLeaderAndISROpt, zkClient))
          if(curLeaderAndISROpt == None){
            debug("during initializing leader of parition (%s, %d), the current leader and isr in zookeeper is empty".format(topicPartition._1, topicPartition._2))
            leaderAndISR = new LeaderAndISR(leader, liveAssignedReplicas.toList)
            ZkUtils.createPersistentPath(zkClient, ZkUtils.getTopicPartitionLeaderAndISRPath(topicPartition._1, topicPartition._2), leaderAndISR.toString)
            updateLeaderISRZKPathSucceeded = true
          } else{
            debug("during initializing leader of parition (%s, %d), the current leader and isr in zookeeper is not empty".format(topicPartition._1, topicPartition._2))
            val curZkPathVersion = curLeaderAndISROpt.get.zkVersion
            leaderAndISR = new LeaderAndISR(leader, curLeaderAndISROpt.get.leaderEpoch + 1,liveAssignedReplicas.toList,  curLeaderAndISROpt.get.zkVersion + 1)
            val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(zkClient, ZkUtils.getTopicPartitionLeaderAndISRPath(topicPartition._1, topicPartition._2), leaderAndISR.toString, curZkPathVersion)
            if(updateSucceeded){
              leaderAndISR.zkVersion = newVersion
            }
            updateLeaderISRZKPathSucceeded = updateSucceeded
          }
        }
        liveAssignedReplicas.foreach(b => {
          if(!brokerToLeaderAndISRInfosMap.contains(b))
            brokerToLeaderAndISRInfosMap.put(b, new mutable.HashMap[(String, Int), LeaderAndISR])
          brokerToLeaderAndISRInfosMap(b).put(topicPartition, leaderAndISR)
        }
        )
        allLeaders.put(topicPartition, leaderAndISR.leader)
      }
      else{
        warn("during initializing leader of parition (%s, %d), assigned replicas are [%s], live brokers are [%s], no assigned replica is alive".format(topicPartition._1, topicPartition._2, replicaAssignment.mkString(","), allBrokerIds))
      }
    }

    info("after leaders initialization for partition replica assignments %s, the cached leaders in controller is %s, and the broker to request map is: %s".format(partitionReplicaAssignment, allLeaders, brokerToLeaderAndISRInfosMap))
    brokerToLeaderAndISRInfosMap.foreach(m =>{
      val broker = m._1
      val leaderAndISRs = m._2
      val leaderAndISRRequest = new LeaderAndISRRequest(LeaderAndISRRequest.NotInit, leaderAndISRs)
      info("at initializing leaders for new partitions, the leaderAndISR request sent to broker %d is %s".format(broker, leaderAndISRRequest))
      sendRequest(broker, leaderAndISRRequest)
    })
  }


  private def onBrokerChange(newBrokers: Set[Int] = null){
    /** handle the new brokers, send request for them to initialize the local log **/
    if(newBrokers != null && newBrokers.size != 0)
      deliverLeaderAndISRFromZookeeper(newBrokers, allTopics)

    /** handle leader election for the partitions whose leader is no longer alive **/
    val brokerToLeaderAndISRInfosMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int), LeaderAndISR]]
    allLeaders.foreach(m =>{
      val topicPartition = m._1
      val leader = m._2
      // We only care about the partitions, whose leader is no longer alive
      if(!allBrokerIds.contains(leader)){
        var updateLeaderISRZKPathSucceeded: Boolean = false
        while(!updateLeaderISRZKPathSucceeded){
          val assignedReplicasOpt = allPartitionReplicaAssignment.get(topicPartition)
          if(assignedReplicasOpt == None)
            throw new IllegalStateException("On broker changes, the assigned replica for [%s, %d], shouldn't be None, the general assignment is %s".format(topicPartition._1, topicPartition._2, allPartitionReplicaAssignment))
          val assignedReplicas = assignedReplicasOpt.get
          val liveAssignedReplicasToThisPartition = assignedReplicas.filter(r => allBrokerIds.contains(r))
          val curLeaderAndISROpt = ZkUtils.getLeaderAndISRForPartition(zkClient, topicPartition._1, topicPartition._2)
          if(curLeaderAndISROpt == None){
            throw new IllegalStateException("On broker change, there's no leaderAndISR information for partition (%s, %d) in zookeeper".format(topicPartition._1, topicPartition._2))
          }
          val curLeaderAndISR = curLeaderAndISROpt.get
          val leader = curLeaderAndISR.leader
          var newLeader: Int = -1
          val leaderEpoch = curLeaderAndISR.leaderEpoch
          val ISR = curLeaderAndISR.ISR
          val curZkPathVersion = curLeaderAndISR.zkVersion
          debug("leader, epoch, ISR and zkPathVersion for partition (%s, %d) are: [%d], [%d], [%s], [%d]".format(topicPartition._1, topicPartition._2, leader, leaderEpoch, ISR, curZkPathVersion))
          // The leader is no longer alive, need reelection, we only care about the leader change here, the ISR change can be handled by the leader
          var leaderAndISR: LeaderAndISR = null
          // The ISR contains at least 1 broker in the live broker list
          val liveBrokersInISR = ISR.filter(r => allBrokerIds.contains(r))
          if(!liveBrokersInISR.isEmpty){
            newLeader = liveBrokersInISR.head
            leaderAndISR = new LeaderAndISR(newLeader, leaderEpoch +1, liveBrokersInISR.toList, curZkPathVersion + 1)
            debug("some broker in ISR is alive, new leader and ISR is %s".format(leaderAndISR.toString()))
          } else{
            debug("live broker in ISR is empty, see live assigned replicas: %s".format(liveAssignedReplicasToThisPartition))
            if (!liveAssignedReplicasToThisPartition.isEmpty){
              newLeader = liveAssignedReplicasToThisPartition.head
              leaderAndISR = new LeaderAndISR(newLeader, leaderEpoch + 1, List(newLeader), curZkPathVersion + 1)
              warn("on broker change, no broker in ISR is alive, new leader elected is [%s], there's potential data loss".format(newLeader))
            } else
              error("on broker change, for partition ([%s, %d]), live brokers are: [%s], assigned replicas are: [%s]; no asigned replica is alive".format(topicPartition._1, topicPartition._2, allBrokerIds, assignedReplicas))
          }
          debug("the leader and ISR converted string: [%s]".format(leaderAndISR))
          val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPath(zkClient, ZkUtils.getTopicPartitionLeaderAndISRPath(topicPartition._1, topicPartition._2), leaderAndISR.toString, curZkPathVersion)
          if(updateSucceeded){
            leaderAndISR.zkVersion = newVersion
            liveAssignedReplicasToThisPartition.foreach(b => {
              if(!brokerToLeaderAndISRInfosMap.contains(b))
                brokerToLeaderAndISRInfosMap.put(b, new mutable.HashMap[(String, Int), LeaderAndISR])
              brokerToLeaderAndISRInfosMap(b).put(topicPartition, leaderAndISR)
            })
            allLeaders.put(topicPartition, newLeader)
            info("on broker changes, allLeader is updated to %s".format(allLeaders))
          }
          updateLeaderISRZKPathSucceeded = updateSucceeded
        }
      }
    })
    brokerToLeaderAndISRInfosMap.foreach(m => {
      val broker = m._1
      val leaderAndISRInfos = m._2
      val leaderAndISRRequest = new LeaderAndISRRequest(LeaderAndISRRequest.NotInit, leaderAndISRInfos)
      sendRequest(broker, leaderAndISRRequest)
      info("on broker change, the LeaderAndISRRequest send to brokers [%d] is [%s]".format(broker, leaderAndISRRequest))
    })
  }

  class BrokerChangeListener() extends IZkChildListener with Logging {
    this.logIdent = "Controller " + config.brokerId + ", "
    def handleChildChange(parentPath : String, javaCurChildren : java.util.List[String]) {
      controllerLock synchronized {
        info("broker change listener triggered")
        val curChildrenSeq: Seq[String] = javaCurChildren
        val curBrokerIdsSeq = curChildrenSeq.map(_.toInt)
        val curBrokerIds = curBrokerIdsSeq.toSet
        val addedBrokerIds = curBrokerIds -- allBrokerIds
        val addedBrokersSeq = ZkUtils.getBrokerInfoFromIds(zkClient, addedBrokerIds.toSeq)
        val deletedBrokerIds = allBrokerIds -- curBrokerIds
        allBrokers = ZkUtils.getBrokerInfoFromIds(zkClient, curBrokerIdsSeq).toSet
        allBrokerIds = allBrokers.map(_.id)
        info("added brokers: %s, deleted brokers: %s, all brokers: %s".format(addedBrokerIds, deletedBrokerIds, allBrokerIds))
        addedBrokersSeq.foreach(controllerChannelManager.addBroker(_))
        deletedBrokerIds.foreach(controllerChannelManager.removeBroker(_))
        onBrokerChange(addedBrokerIds)
      }
    }
  }

  private def handleNewTopics(topics: Set[String], partitionReplicaAssignment: mutable.Map[(String, Int), Seq[Int]]) {
    // get relevant partitions to this broker
    val partitionReplicaAssignment = allPartitionReplicaAssignment.filter(p => topics.contains(p._1._1))
    trace("handling new topics, the partition replica assignment to be handled is %s".format(partitionReplicaAssignment))
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
      ZkUtils.deletePath(zkClient, ZkUtils.getTopicPartitionLeaderAndISRPath(topicPartition._1, topicPartition._2))
    }
    for((broker, partitionToStopReplica) <- brokerToPartitionToStopReplicaMap){
      val stopReplicaRequest = new StopReplicaRequest(partitionToStopReplica)
      info("handling deleted topics: [%s] the stopReplicaRequest sent to broker %d is [%s]".format(topics, broker, stopReplicaRequest))
      sendRequest(broker, stopReplicaRequest)
    }
    /*TODO: kafka-330  remove the unneeded leaderAndISRPath that the previous controller didn't get a chance to remove*/
  }

  class TopicChangeListener extends IZkChildListener with Logging {
    this.logIdent = "Controller " + config.brokerId + ", "

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      controllerLock synchronized {
        info("topic/partition change listener fired for path " + parentPath)
        val currentChildren = JavaConversions.asBuffer(curChilds).toSet
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

  class ControllerExistListener extends IZkDataListener with Logging {
    this.logIdent = "Controller " + config.brokerId + ", "

    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      // do nothing, since No logic is needed here
    }

    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      controllerLock synchronized {
        info("the current controller failed, competes to be new controller")
        controllerRegisterOrFailover()
      }
    }
  }
}