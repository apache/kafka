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

import kafka.network.{Receive, BlockingChannel}
import kafka.utils.{Utils, Logging, ShutdownableThread}
import collection.mutable.HashMap
import kafka.cluster.Broker
import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue}
import kafka.server.KafkaConfig
import collection.mutable
import kafka.api._
import org.apache.log4j.Logger
import scala.Some
import kafka.common.TopicAndPartition
import kafka.api.RequestOrResponse
import collection.Set

class ControllerChannelManager (private val controllerContext: ControllerContext, config: KafkaConfig) extends Logging {
  private val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  controllerContext.liveBrokers.foreach(addNewBroker(_))

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
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put((request, callback))
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
      removeExistingBroker(brokerId)
    }
  }

  private def addNewBroker(broker: Broker) {
    val messageQueue = new LinkedBlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)](config.controllerMessageQueueSize)
    debug("Controller %d trying to connect to broker %d".format(config.brokerId,broker.id))
    val channel = new BlockingChannel(broker.host, broker.port,
      BlockingChannel.UseDefaultBufferSize,
      BlockingChannel.UseDefaultBufferSize,
      config.controllerSocketTimeoutMs)
    val requestThread = new RequestSendThread(config.brokerId, controllerContext, broker, messageQueue, channel)
    requestThread.setDaemon(false)
    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(channel, broker, messageQueue, requestThread))
  }

  private def removeExistingBroker(brokerId: Int) {
    try {
      brokerStateInfo(brokerId).channel.disconnect()
      brokerStateInfo(brokerId).messageQueue.clear()
      brokerStateInfo(brokerId).requestSendThread.shutdown()
      brokerStateInfo.remove(brokerId)
    }catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  private def startRequestSendThread(brokerId: Int) {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if(requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val toBroker: Broker,
                        val queue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],
                        val channel: BlockingChannel)
  extends ShutdownableThread("Controller-%d-to-broker-%d-send-thread".format(controllerId, toBroker.id)) {
  private val lock = new Object()
  private val stateChangeLogger = KafkaController.stateChangeLogger
  connectToBroker(toBroker, channel)

  override def doWork(): Unit = {
    val queueItem = queue.take()
    val request = queueItem._1
    val callback = queueItem._2
    var receive: Receive = null
    try {
      lock synchronized {
        var isSendSuccessful = false
        while(isRunning.get() && !isSendSuccessful) {
          // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
          // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
          try {
            channel.send(request)
            receive = channel.receive()
            isSendSuccessful = true
          } catch {
            case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
              warn(("Controller %d epoch %d fails to send request %s to broker %s. " +
                "Reconnecting to broker.").format(controllerId, controllerContext.epoch,
                request.toString, toBroker.toString()), e)
              channel.disconnect()
              connectToBroker(toBroker, channel)
              isSendSuccessful = false
              // backoff before retrying the connection and send
              Utils.swallow(Thread.sleep(300))
          }
        }
        var response: RequestOrResponse = null
        request.requestId.get match {
          case RequestKeys.LeaderAndIsrKey =>
            response = LeaderAndIsrResponse.readFrom(receive.buffer)
          case RequestKeys.StopReplicaKey =>
            response = StopReplicaResponse.readFrom(receive.buffer)
          case RequestKeys.UpdateMetadataKey =>
            response = UpdateMetadataResponse.readFrom(receive.buffer)
        }
        stateChangeLogger.trace("Controller %d epoch %d received response %s for a request sent to broker %s"
                                  .format(controllerId, controllerContext.epoch, response.toString, toBroker.toString))

        if(callback != null) {
          callback(response)
        }
      }
    } catch {
      case e: Throwable =>
        error("Controller %d fails to send a request to broker %s".format(controllerId, toBroker.toString()), e)
        // If there is any socket error (eg, socket timeout), the channel is no longer usable and needs to be recreated.
        channel.disconnect()
    }
  }

  private def connectToBroker(broker: Broker, channel: BlockingChannel) {
    try {
      channel.connect()
      info("Controller %d connected to %s for sending state change requests".format(controllerId, broker.toString()))
    } catch {
      case e: Throwable => {
        channel.disconnect()
        error("Controller %d's connection to broker %s was unsuccessful".format(controllerId, broker.toString()), e)
      }
    }
  }
}

class ControllerBrokerRequestBatch(controller: KafkaController) extends  Logging {
  val controllerContext = controller.controllerContext
  val controllerId: Int = controller.config.brokerId
  val clientId: String = controller.clientId
  val leaderAndIsrRequestMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int), PartitionStateInfo]]
  val stopReplicaRequestMap = new mutable.HashMap[Int, Seq[StopReplicaRequestInfo]]
  val updateMetadataRequestMap = new mutable.HashMap[Int, mutable.HashMap[TopicAndPartition, PartitionStateInfo]]
  private val stateChangeLogger = KafkaController.stateChangeLogger

  def newBatch() {
    // raise error if the previous batch is not empty
    if(leaderAndIsrRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
    if(stopReplicaRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
    if(updateMetadataRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some UpdateMetadata state changes %s might be lost ".format(updateMetadataRequestMap.toString()))
  }

  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicas: Seq[Int], callback: (RequestOrResponse) => Unit = null) {
    val topicAndPartition: TopicAndPartition = TopicAndPartition(topic, partition)

    brokerIds.filter(b => b >= 0).foreach {
      brokerId =>
        leaderAndIsrRequestMap.getOrElseUpdate(brokerId, new mutable.HashMap[(String, Int), PartitionStateInfo])
        leaderAndIsrRequestMap(brokerId).put((topic, partition),
          PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.toSet))
    }

    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq,
                                       Set(topicAndPartition))
  }

  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int, deletePartition: Boolean,
                                      callback: (RequestOrResponse, Int) => Unit = null) {
    brokerIds.filter(b => b >= 0).foreach { brokerId =>
      stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[StopReplicaRequestInfo])
      val v = stopReplicaRequestMap(brokerId)
      if(callback != null)
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition, (r: RequestOrResponse) => { callback(r, brokerId) })
      else
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition)
    }
  }

  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition],
                                         callback: (RequestOrResponse) => Unit = null) {
    def updateMetadataRequestMapFor(partition: TopicAndPartition, beingDeleted: Boolean) {
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
          brokerIds.filter(b => b >= 0).foreach { brokerId =>
            updateMetadataRequestMap.getOrElseUpdate(brokerId, new mutable.HashMap[TopicAndPartition, PartitionStateInfo])
            updateMetadataRequestMap(brokerId).put(partition, partitionStateInfo)
          }
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
    filteredPartitions.foreach(partition => updateMetadataRequestMapFor(partition, beingDeleted = false))
    controller.deleteTopicManager.partitionsToBeDeleted.foreach(partition => updateMetadataRequestMapFor(partition, beingDeleted = true))
  }

  def sendRequestsToBrokers(controllerEpoch: Int, correlationId: Int) {
    leaderAndIsrRequestMap.foreach { m =>
      val broker = m._1
      val partitionStateInfos = m._2.toMap
      val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet
      val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id))
      val leaderAndIsrRequest = new LeaderAndIsrRequest(partitionStateInfos, leaders, controllerId, controllerEpoch, correlationId, clientId)
      for (p <- partitionStateInfos) {
        val typeOfRequest = if (broker == p._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader" else "become-follower"
        stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request %s with correlationId %d to broker %d " +
                                 "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest,
                                                                 p._2.leaderIsrAndControllerEpoch, correlationId, broker,
                                                                 p._1._1, p._1._2))
      }
      controller.sendRequest(broker, leaderAndIsrRequest, null)
    }
    leaderAndIsrRequestMap.clear()
    updateMetadataRequestMap.foreach { m =>
      val broker = m._1
      val partitionStateInfos = m._2.toMap
      val updateMetadataRequest = new UpdateMetadataRequest(controllerId, controllerEpoch, correlationId, clientId,
        partitionStateInfos, controllerContext.liveOrShuttingDownBrokers)
      partitionStateInfos.foreach(p => stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request %s with " +
        "correlationId %d to broker %d for partition %s").format(controllerId, controllerEpoch, p._2.leaderIsrAndControllerEpoch,
        correlationId, broker, p._1)))
      controller.sendRequest(broker, updateMetadataRequest, null)
    }
    updateMetadataRequestMap.clear()
    stopReplicaRequestMap foreach { case(broker, replicaInfoList) =>
      val stopReplicaWithDelete = replicaInfoList.filter(p => p.deletePartition == true).map(i => i.replica).toSet
      val stopReplicaWithoutDelete = replicaInfoList.filter(p => p.deletePartition == false).map(i => i.replica).toSet
      debug("The stop replica request (delete = true) sent to broker %d is %s"
        .format(broker, stopReplicaWithDelete.mkString(",")))
      debug("The stop replica request (delete = false) sent to broker %d is %s"
        .format(broker, stopReplicaWithoutDelete.mkString(",")))
      replicaInfoList.foreach { r =>
        val stopReplicaRequest = new StopReplicaRequest(r.deletePartition,
          Set(TopicAndPartition(r.replica.topic, r.replica.partition)), controllerId, controllerEpoch, correlationId)
        controller.sendRequest(broker, stopReplicaRequest, r.callback)
      }
    }
    stopReplicaRequestMap.clear()
  }
}

case class ControllerBrokerStateInfo(channel: BlockingChannel,
                                     broker: Broker,
                                     messageQueue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],
                                     requestSendThread: RequestSendThread)

case class StopReplicaRequestInfo(replica: PartitionAndReplica, deletePartition: Boolean, callback: (RequestOrResponse) => Unit = null)

class Callbacks private (var leaderAndIsrResponseCallback:(RequestOrResponse) => Unit = null,
                         var updateMetadataResponseCallback:(RequestOrResponse) => Unit = null,
                         var stopReplicaResponseCallback:(RequestOrResponse, Int) => Unit = null)

object Callbacks {
  class CallbackBuilder {
    var leaderAndIsrResponseCbk:(RequestOrResponse) => Unit = null
    var updateMetadataResponseCbk:(RequestOrResponse) => Unit = null
    var stopReplicaResponseCbk:(RequestOrResponse, Int) => Unit = null

    def leaderAndIsrCallback(cbk: (RequestOrResponse) => Unit): CallbackBuilder = {
      leaderAndIsrResponseCbk = cbk
      this
    }

    def updateMetadataCallback(cbk: (RequestOrResponse) => Unit): CallbackBuilder = {
      updateMetadataResponseCbk = cbk
      this
    }

    def stopReplicaCallback(cbk: (RequestOrResponse, Int) => Unit): CallbackBuilder = {
      stopReplicaResponseCbk = cbk
      this
    }

    def build: Callbacks = {
      new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk)
    }
  }
}
