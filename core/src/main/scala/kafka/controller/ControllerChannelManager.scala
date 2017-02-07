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

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import kafka.api._
import kafka.cluster.Broker
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients.{ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, ListenerName, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests
import org.apache.kafka.common.requests.{UpdateMetadataRequest, _}
import org.apache.kafka.common.requests.UpdateMetadataRequest.EndPoint
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.{Set, mutable}
import scala.collection.mutable.HashMap

class ControllerChannelManager(controllerContext: ControllerContext, config: KafkaConfig, time: Time, metrics: Metrics, threadNamePrefix: Option[String] = None) extends Logging {
  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  controllerContext.liveBrokers.foreach(addNewBroker)

  def startup() = {
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.values.foreach(removeExistingBroker)
    }
  }

  def sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: AbstractResponse => Unit = null) {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(apiKey, request, callback))
        case None =>
          warn("Not sending request %s to broker %d, since it is offline.".format(request, brokerId))
      }
    }
  }

  def addBroker(broker: Broker) {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if(!brokerStateInfo.contains(broker.id)) {
        addNewBroker(broker)
        startRequestSendThread(broker.id)
      }
    }
  }

  def removeBroker(brokerId: Int) {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  private def addNewBroker(broker: Broker) {
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug("Controller %d trying to connect to broker %d".format(config.brokerId, broker.id))
    val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerListenerName)
    val brokerNode = new Node(broker.id, brokerEndPoint.host, brokerEndPoint.port)
    val networkClient = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        config.interBrokerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        config.interBrokerListenerName,
        config.saslMechanismInterBrokerProtocol,
        config.saslInterBrokerHandshakeRequestEnable
      )
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> broker.id.toString).asJava,
        false,
        channelBuilder
      )
      new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        time,
        false
      )
    }
    val threadName = threadNamePrefix match {
      case None => "Controller-%d-to-broker-%d-send-thread".format(config.brokerId, broker.id)
      case Some(name) => "%s:Controller-%d-to-broker-%d-send-thread".format(name, config.brokerId, broker.id)
    }

    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, threadName)
    requestThread.setDaemon(false)
    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue, requestThread))
  }

  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo) {
    try {
      brokerState.networkClient.close()
      brokerState.messageQueue.clear()
      brokerState.requestSendThread.shutdown()
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  protected def startRequestSendThread(brokerId: Int) {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if(requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

case class QueueItem(apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                     callback: AbstractResponse => Unit)

class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val queue: BlockingQueue[QueueItem],
                        val networkClient: NetworkClient,
                        val brokerNode: Node,
                        val config: KafkaConfig,
                        val time: Time,
                        name: String)
  extends ShutdownableThread(name = name) {

  private val lock = new Object()
  private val stateChangeLogger = KafkaController.stateChangeLogger
  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  override def doWork(): Unit = {

    def backoff(): Unit = CoreUtils.swallowTrace(Thread.sleep(100))

    val QueueItem(apiKey, requestBuilder, callback) = queue.take()
    import NetworkClientBlockingOps._
    var clientResponse: ClientResponse = null
    try {
      lock synchronized {
        var isSendSuccessful = false
        while (isRunning.get() && !isSendSuccessful) {
          // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
          // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
          try {
            if (!brokerReady()) {
              isSendSuccessful = false
              backoff()
            }
            else {
              val clientRequest = networkClient.newClientRequest(brokerNode.idString, requestBuilder,
                time.milliseconds(), true)
              clientResponse = networkClient.blockingSendAndReceive(clientRequest)(time)
              isSendSuccessful = true
            }
          } catch {
            case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
              warn(("Controller %d epoch %d fails to send request %s to broker %s. " +
                "Reconnecting to broker.").format(controllerId, controllerContext.epoch,
                  requestBuilder.toString, brokerNode.toString), e)
              networkClient.close(brokerNode.idString)
              isSendSuccessful = false
              backoff()
          }
        }
        if (clientResponse != null) {
          val api = ApiKeys.forId(clientResponse.requestHeader.apiKey)
          if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA_KEY)
            throw new KafkaException(s"Unexpected apiKey received: $apiKey")

          val response = clientResponse.responseBody

          stateChangeLogger.trace("Controller %d epoch %d received response %s for a request sent to broker %s"
            .format(controllerId, controllerContext.epoch, response.toString, brokerNode.toString))

          if (callback != null) {
            callback(response)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error("Controller %d fails to send a request to broker %s".format(controllerId, brokerNode.toString), e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
        networkClient.close(brokerNode.idString)
    }
  }

  private def brokerReady(): Boolean = {
    import NetworkClientBlockingOps._
    try {
      if (!networkClient.isReady(brokerNode)(time)) {
        if (!networkClient.blockingReady(brokerNode, socketTimeoutMs)(time))
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info("Controller %d connected to %s for sending state change requests".format(controllerId, brokerNode.toString))
      }

      true
    } catch {
      case e: Throwable =>
        warn("Controller %d's connection to broker %s was unsuccessful".format(controllerId, brokerNode.toString), e)
        networkClient.close(brokerNode.idString)
        false
    }
  }

}

class ControllerBrokerRequestBatch(controller: KafkaController) extends  Logging {
  val controllerContext = controller.controllerContext
  val controllerId: Int = controller.config.brokerId
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, PartitionStateInfo]]
  val stopReplicaRequestMap = mutable.Map.empty[Int, Seq[StopReplicaRequestInfo]]
  val updateMetadataRequestBrokerSet = mutable.Set.empty[Int]
  val updateMetadataRequestPartitionInfoMap = mutable.Map.empty[TopicPartition, PartitionStateInfo]
  private val stateChangeLogger = KafkaController.stateChangeLogger

  def newBatch() {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
    if (stopReplicaRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
    if (updateMetadataRequestBrokerSet.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some UpdateMetadata state changes to brokers %s with partition info %s might be lost ".format(
          updateMetadataRequestBrokerSet.toString(), updateMetadataRequestPartitionInfoMap.toString()))
  }

  def clear() {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }

  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicas: Seq[Int], callback: AbstractResponse => Unit = null) {
    val topicPartition = new TopicPartition(topic, partition)

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      result.put(topicPartition, PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.toSet))
    }

    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq,
                                       Set(TopicAndPartition(topic, partition)))
  }

  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int, deletePartition: Boolean,
                                      callback: (AbstractResponse, Int) => Unit = null) {
    brokerIds.filter(b => b >= 0).foreach { brokerId =>
      stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[StopReplicaRequestInfo])
      val v = stopReplicaRequestMap(brokerId)
      if(callback != null)
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition, (r: AbstractResponse) => callback(r, brokerId))
      else
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition)
    }
  }

  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition],
                                         callback: AbstractResponse => Unit = null) {
    def updateMetadataRequestPartitionInfo(partition: TopicAndPartition, beingDeleted: Boolean) {
      val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition)
      leaderIsrAndControllerEpochOpt match {
        case Some(leaderIsrAndControllerEpoch) =>
          val replicas = controllerContext.partitionReplicaAssignment(partition).toSet
          val partitionStateInfo = if (beingDeleted) {
            val leaderAndIsr = new LeaderAndIsr(LeaderAndIsr.LeaderDuringDelete, leaderIsrAndControllerEpoch.leaderAndIsr.isr)
            PartitionStateInfo(LeaderIsrAndControllerEpoch(leaderAndIsr, leaderIsrAndControllerEpoch.controllerEpoch), replicas)
          } else {
            PartitionStateInfo(leaderIsrAndControllerEpoch, replicas)
          }
          updateMetadataRequestPartitionInfoMap.put(new TopicPartition(partition.topic, partition.partition), partitionStateInfo)
        case None =>
          info("Leader not yet assigned for partition %s. Skip sending UpdateMetadataRequest.".format(partition))
      }
    }

    val filteredPartitions = {
      val givenPartitions = if (partitions.isEmpty)
        controllerContext.partitionLeadershipInfo.keySet
      else
        partitions
      if (controller.deleteTopicManager.partitionsToBeDeleted.isEmpty)
        givenPartitions
      else
        givenPartitions -- controller.deleteTopicManager.partitionsToBeDeleted
    }

    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
    filteredPartitions.foreach(partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = false))
    controller.deleteTopicManager.partitionsToBeDeleted.foreach(partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = true))
  }

  def sendRequestsToBrokers(controllerEpoch: Int) {
    try {
      leaderAndIsrRequestMap.foreach { case (broker, partitionStateInfos) =>
        partitionStateInfos.foreach { case (topicPartition, state) =>
          val typeOfRequest = if (broker == state.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader" else "become-follower"
          stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request %s to broker %d " +
                                   "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest,
                                                                   state.leaderIsrAndControllerEpoch, broker,
                                                                   topicPartition.topic, topicPartition.partition))
        }
        val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet
        val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.getNode(controller.config.interBrokerListenerName)
        }
        val partitionStates = partitionStateInfos.map { case (topicPartition, partitionStateInfo) =>
          val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
          val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
            leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
            partitionStateInfo.allReplicas.map(Integer.valueOf).asJava)
          topicPartition -> partitionState
        }
        val leaderAndIsrRequest = new LeaderAndIsrRequest.
            Builder(controllerId, controllerEpoch, partitionStates.asJava, leaders.asJava)
        controller.sendRequest(broker, ApiKeys.LEADER_AND_ISR, leaderAndIsrRequest, null)
      }
      leaderAndIsrRequestMap.clear()

      updateMetadataRequestPartitionInfoMap.foreach(p => stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request %s " +
        "to brokers %s for partition %s").format(controllerId, controllerEpoch, p._2.leaderIsrAndControllerEpoch,
        updateMetadataRequestBrokerSet.toString(), p._1)))
      val partitionStates = updateMetadataRequestPartitionInfoMap.map { case (topicPartition, partitionStateInfo) =>
        val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
        val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
          leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
          partitionStateInfo.allReplicas.map(Integer.valueOf).asJava)
        topicPartition -> partitionState
      }

      val version: Short =
        if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_2_IV0) 3
        else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2
        else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
        else 0

      val updateMetadataRequest = {
        val liveBrokers = if (version == 0) {
          // Version 0 of UpdateMetadataRequest only supports PLAINTEXT.
          controllerContext.liveOrShuttingDownBrokers.map { broker =>
            val securityProtocol = SecurityProtocol.PLAINTEXT
            val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
            val node = broker.getNode(listenerName)
            val endPoints = Seq(new EndPoint(node.host, node.port, securityProtocol, listenerName))
            new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
          }
        } else {
          controllerContext.liveOrShuttingDownBrokers.map { broker =>
            val endPoints = broker.endPoints.map { endPoint =>
              new UpdateMetadataRequest.EndPoint(endPoint.host, endPoint.port, endPoint.securityProtocol, endPoint.listenerName)
            }
            new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
          }
        }
        new UpdateMetadataRequest.Builder(
          controllerId, controllerEpoch, partitionStates.asJava, liveBrokers.asJava).
          setVersion(version)
      }

      updateMetadataRequestBrokerSet.foreach { broker =>
        controller.sendRequest(broker, ApiKeys.UPDATE_METADATA_KEY, updateMetadataRequest, null)
      }
      updateMetadataRequestBrokerSet.clear()
      updateMetadataRequestPartitionInfoMap.clear()

      stopReplicaRequestMap.foreach { case (broker, replicaInfoList) =>
        val stopReplicaWithDelete = replicaInfoList.filter(_.deletePartition).map(_.replica).toSet
        val stopReplicaWithoutDelete = replicaInfoList.filterNot(_.deletePartition).map(_.replica).toSet
        debug("The stop replica request (delete = true) sent to broker %d is %s"
          .format(broker, stopReplicaWithDelete.mkString(",")))
        debug("The stop replica request (delete = false) sent to broker %d is %s"
          .format(broker, stopReplicaWithoutDelete.mkString(",")))

        val (replicasToGroup, replicasToNotGroup) = replicaInfoList.partition(r => !r.deletePartition && r.callback == null)

        // Send one StopReplicaRequest for all partitions that require neither delete nor callback. This potentially
        // changes the order in which the requests are sent for the same partitions, but that's OK.
        val stopReplicaRequest = new StopReplicaRequest.Builder(controllerId, controllerEpoch, false,
          replicasToGroup.map(r => new TopicPartition(r.replica.topic, r.replica.partition)).toSet.asJava)
        controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest)

        replicasToNotGroup.foreach { r =>
          val stopReplicaRequest = new StopReplicaRequest.Builder(
              controllerId, controllerEpoch, r.deletePartition,
              Set(new TopicPartition(r.replica.topic, r.replica.partition)).asJava)
          controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest, r.callback)
        }
      }
      stopReplicaRequestMap.clear()
    } catch {
      case e: Throwable =>
        if (leaderAndIsrRequestMap.nonEmpty) {
          error("Haven't been able to send leader and isr requests, current state of " +
              s"the map is $leaderAndIsrRequestMap. Exception message: $e")
        }
        if (updateMetadataRequestBrokerSet.nonEmpty) {
          error(s"Haven't been able to send metadata update requests to brokers $updateMetadataRequestBrokerSet, " +
                s"current state of the partition info is $updateMetadataRequestPartitionInfoMap. Exception message: $e")
        }
        if (stopReplicaRequestMap.nonEmpty) {
          error("Haven't been able to send stop replica requests, current state of " +
              s"the map is $stopReplicaRequestMap. Exception message: $e")
        }
        throw new IllegalStateException(e)
    }
  }
}

case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node,
                                     messageQueue: BlockingQueue[QueueItem],
                                     requestSendThread: RequestSendThread)

case class StopReplicaRequestInfo(replica: PartitionAndReplica, deletePartition: Boolean, callback: AbstractResponse => Unit = null)

class Callbacks private (var leaderAndIsrResponseCallback: AbstractResponse => Unit = null,
                         var updateMetadataResponseCallback: AbstractResponse => Unit = null,
                         var stopReplicaResponseCallback: (AbstractResponse, Int) => Unit = null)

object Callbacks {
  class CallbackBuilder {
    var leaderAndIsrResponseCbk: AbstractResponse => Unit = null
    var updateMetadataResponseCbk: AbstractResponse => Unit = null
    var stopReplicaResponseCbk: (AbstractResponse, Int) => Unit = null

    def leaderAndIsrCallback(cbk: AbstractResponse => Unit): CallbackBuilder = {
      leaderAndIsrResponseCbk = cbk
      this
    }

    def updateMetadataCallback(cbk: AbstractResponse => Unit): CallbackBuilder = {
      updateMetadataResponseCbk = cbk
      this
    }

    def stopReplicaCallback(cbk: (AbstractResponse, Int) => Unit): CallbackBuilder = {
      stopReplicaResponseCbk = cbk
      this
    }

    def build: Callbacks = {
      new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk)
    }
  }
}
