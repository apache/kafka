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

import kafka.common.KafkaZookeeperClient
import collection.mutable.HashMap
import collection.immutable.Set
import kafka.cluster.Broker
import kafka.api._
import java.lang.Object
import kafka.network.{Receive, BlockingChannel}
import kafka.utils.{ZkUtils, Logging}
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, BlockingQueue}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import java.util.concurrent.atomic.AtomicBoolean
import org.I0Itec.zkclient.{IZkStateListener, ZkClient, IZkDataListener, IZkChildListener}
import org.apache.zookeeper.Watcher.Event.KeeperState


class RequestSendThread(val brokerId: Int,
                        val queue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],
                        val channel: BlockingChannel)
        extends Thread("requestSendThread-" + brokerId) with Logging {
  val isRunning: AtomicBoolean = new AtomicBoolean(true)
  private val shutDownLatch = new CountDownLatch(1)
  private val lock = new Object

  def shutDown(): Unit = {
    info("Shutting down controller request send thread to broker %d".format(brokerId))
    isRunning.set(false)
    interrupt()
    shutDownLatch.await()
    info("Controller request send thread to broker %d shutting down completed".format(brokerId))
  }

  override def run(): Unit = {
    try{
      info("In controller, thread for broker: " + brokerId + " started running")
      while(isRunning.get()){
        val queueItem = queue.take()
        val request = queueItem._1
        val callback = queueItem._2

        var receive: Receive = null
        lock synchronized {
          channel.send(request)
          receive = channel.receive()
        }

        var response: RequestOrResponse = null
        request.requestId.get match {
          case RequestKeys.LeaderAndISRRequest =>
            response = LeaderAndISRResponse.readFrom(receive.buffer)
          case RequestKeys.StopReplicaRequest =>
            response = StopReplicaResponse.readFrom(receive.buffer)
        }
        if(callback != null){
          callback(response)
        }
      }
    } catch{
      case e: InterruptedException => warn("Controller request send thread to broker %d is intterrupted. Shutting down".format(brokerId))
      case e1 => error("Error in controller request send thread to broker %d down due to ".format(brokerId), e1)
    }
    shutDownLatch.countDown()
  }
}

class ControllerChannelManager(allBrokers: Set[Broker], config : KafkaConfig) extends Logging{
  private val brokers = new HashMap[Int, Broker]
  private val messageChannels = new HashMap[Int, BlockingChannel]
  private val messageQueues = new HashMap[Int, BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)]]
  private val messageThreads = new HashMap[Int, RequestSendThread]
  for(broker <- allBrokers){
    brokers.put(broker.id, broker)
    info("channel for broker " + broker.id + " created" + " at host: " + broker.host + " and port: " + broker.port)
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
      val thread = new RequestSendThread(brokerId, messageQueues(brokerId), messageChannels(brokerId))
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
    info("channel for broker " + broker.id + " created" + " at host: " + broker.host + " and port: " + broker.port)
    val channel = new BlockingChannel(broker.host, broker.port,
                                      BlockingChannel.UseDefaultBufferSize,
                                      BlockingChannel.UseDefaultBufferSize,
                                      config.controllerSocketTimeoutMs)
    channel.connect()
    messageChannels.put(broker.id, channel)
    val thread = new RequestSendThread(broker.id, messageQueues(broker.id), messageChannels(broker.id))
    thread.setDaemon(false)
    thread.start()
    messageThreads.put(broker.id, thread)
  }

  def removeBroker(brokerId: Int){
    brokers.remove(brokerId)
    messageChannels(brokerId).disconnect()
    messageChannels.remove(brokerId)
    messageQueues.remove(brokerId)
    messageThreads(brokerId).shutDown()
    messageThreads.remove(brokerId)
  }
}

class KafkaController(config : KafkaConfig) extends Logging {
  info("controller startup");
  private val lock = new Object

  private var zkClient: ZkClient = null
  private var controllerChannelManager: ControllerChannelManager = null
  private var allBrokers : Set[Broker] = null
  private var allTopics: Set[String] = null

  private def tryToBecomeController() = {
    lock synchronized {
      val curController = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.ControllerPath)
      if (curController == null){
        try {
          ZkUtils.createEphemeralPathExpectConflict(zkClient, ZkUtils.ControllerPath, config.brokerId.toString())

          // Only the broker successfully registering as the controller can execute following code, otherwise
          // some exception will be thrown.
          registerBrokerChangeListener()
          registerTopicChangeListener()
          allBrokers = ZkUtils.getAllBrokersInCluster(zkClient).toSet
          allTopics = ZkUtils.getAllTopics(zkClient).toSet
          controllerChannelManager = new ControllerChannelManager(allBrokers, config)
          controllerChannelManager.startUp()
        } catch {
          case e: ZkNodeExistsException =>
            registerControllerExistListener()
            info("Broker " + config.brokerId + " didn't succeed registering as the controller since it's taken by someone else")
          case e2 => throw e2
        }
      }
      else info("Broker " + config.brokerId + " see not null skip " + " current controller " + curController)
    }
  }

  def isActive(): Boolean = {
    controllerChannelManager != null
  }

  def startup() = {
    zkClient = KafkaZookeeperClient.getZookeeperClient(config)
    registerSessionExpirationListener()
    registerControllerExistListener()
    tryToBecomeController()
  }

  def shutDown() = {
    if(controllerChannelManager != null){
      controllerChannelManager.shutDown()
    }
    zkClient.close()
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

  class SessionExpireListener() extends IZkStateListener {
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
      info("Controller session expires, clean up the state, current controller: " + config.brokerId)
      controllerChannelManager.shutDown()
      controllerChannelManager = null
      info("Controller session expires, the channel manager shut downr: " + config.brokerId)
      tryToBecomeController()
    }
  }

  class BrokerChangeListener() extends IZkChildListener with Logging {
    def handleChildChange(parentPath : String, javaCurChildren : java.util.List[String]) {
      import scala.collection.JavaConversions._
      lock synchronized {
        info("Broker change listener at controller triggerred")
        val allBrokerIds = allBrokers.map(_.id)
        val curChildrenSeq: Seq[String] = javaCurChildren
        val curBrokerIdsSeq = curChildrenSeq.map(_.toInt)
        val curBrokerIds = curBrokerIdsSeq.toSet
        val addedBrokerIds = curBrokerIds -- allBrokerIds
        val addedBrokersSeq = ZkUtils.getBrokerInfoFromIds(zkClient, addedBrokerIds.toSeq)
        info("Added brokers: " + addedBrokerIds.toString())
        val deletedBrokerIds = allBrokerIds -- curBrokerIds
        info("Deleted brokers: " + deletedBrokerIds.toString())

        allBrokers = ZkUtils.getBrokerInfoFromIds(zkClient, curBrokerIdsSeq).toSet

        for(broker <- addedBrokersSeq){
          controllerChannelManager.addBroker(broker)
        }
        for (brokerId <- deletedBrokerIds){
          controllerChannelManager.removeBroker(brokerId)
        }
        /** TODO: add other broker change handler logic**/
      }
    }
  }

  class TopicChangeListener extends IZkChildListener with Logging {
    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      // TODO: Incomplete, do not need to review this time
    }
  }

  class ControllerExistListener extends IZkDataListener with Logging {
    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      // do nothing, since No logic is needed here
    }

    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      info("Controller fail over, broker " + config.brokerId + " try to become controller")
      tryToBecomeController()
    }
  }
}