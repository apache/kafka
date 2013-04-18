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
import kafka.utils.{Logging, ShutdownableThread}
import collection.mutable.HashMap
import kafka.cluster.Broker
import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue}
import kafka.server.KafkaConfig
import collection.mutable
import kafka.api._
import org.apache.log4j.Logger

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
    val channel = new BlockingChannel(broker.host, broker.port,
      BlockingChannel.UseDefaultBufferSize,
      BlockingChannel.UseDefaultBufferSize,
      config.controllerSocketTimeoutMs)
    channel.connect()
    val requestThread = new RequestSendThread(config.brokerId, controllerContext, broker.id, messageQueue, channel)
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
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if(requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val toBrokerId: Int,
                        val queue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],
                        val channel: BlockingChannel)
  extends ShutdownableThread("Controller-%d-to-broker-%d-send-thread".format(controllerId, toBrokerId)) {
  private val lock = new Object()
  private val stateChangeLogger = Logger.getLogger(KafkaController.stateChangeLogger)

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
          case RequestKeys.LeaderAndIsrKey =>
            response = LeaderAndIsrResponse.readFrom(receive.buffer)
          case RequestKeys.StopReplicaKey =>
            response = StopReplicaResponse.readFrom(receive.buffer)
        }
        stateChangeLogger.trace("Controller %d epoch %d received response correlationId %d for a request sent to broker %d"
                                  .format(controllerId, controllerContext.epoch, response.correlationId, toBrokerId))

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

class ControllerBrokerRequestBatch(sendRequest: (Int, RequestOrResponse, (RequestOrResponse) => Unit) => Unit, controllerId: Int)
  extends  Logging {
  val leaderAndIsrRequestMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int), PartitionStateInfo]]
  val stopReplicaRequestMap = new mutable.HashMap[Int, Seq[(String, Int)]]
  val stopAndDeleteReplicaRequestMap = new mutable.HashMap[Int, Seq[(String, Int)]]
  private val stateChangeLogger = Logger.getLogger(KafkaController.stateChangeLogger)

  def newBatch() {
    // raise error if the previous batch is not empty
    if(leaderAndIsrRequestMap.size > 0 || stopReplicaRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    stopAndDeleteReplicaRequestMap.clear()
  }

  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch, replicationFactor: Int) {
    brokerIds.foreach { brokerId =>
      leaderAndIsrRequestMap.getOrElseUpdate(brokerId, new mutable.HashMap[(String, Int), PartitionStateInfo])
      leaderAndIsrRequestMap(brokerId).put((topic, partition), PartitionStateInfo(leaderIsrAndControllerEpoch, replicationFactor))
    }
  }

  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int, deletePartition: Boolean) {
    brokerIds.foreach { brokerId =>
      stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[(String, Int)])
      stopAndDeleteReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[(String, Int)])
      if (deletePartition) {
        val v = stopAndDeleteReplicaRequestMap(brokerId)
        stopAndDeleteReplicaRequestMap(brokerId) = v :+ (topic, partition)
      }
      else {
        val v = stopReplicaRequestMap(brokerId)
        stopReplicaRequestMap(brokerId) = v :+ (topic, partition)
      }
    }
  }

  def sendRequestsToBrokers(controllerEpoch: Int, correlationId: Int, liveBrokers: Set[Broker]) {
    leaderAndIsrRequestMap.foreach { m =>
      val broker = m._1
      val partitionStateInfos = m._2.toMap
      val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet
      val leaders = liveBrokers.filter(b => leaderIds.contains(b.id))
      val leaderAndIsrRequest = new LeaderAndIsrRequest(partitionStateInfos, leaders, controllerId, controllerEpoch, correlationId)
      for (p <- partitionStateInfos) {
        val typeOfRequest = if (broker == p._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader" else "become-follower"
        stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request with correlationId %d to broker %d " +
                                 "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest, correlationId, broker,
                                                                 p._1._1, p._1._2))
      }
      sendRequest(broker, leaderAndIsrRequest, null)
    }
    leaderAndIsrRequestMap.clear()
    Seq((stopReplicaRequestMap, false), (stopAndDeleteReplicaRequestMap, true)) foreach {
      case(m, deletePartitions) => {
        m foreach {
          case(broker, replicas) =>
            if (replicas.size > 0) {
              debug("The stop replica request (delete = %s) sent to broker %d is %s"
                .format(deletePartitions, broker, replicas.mkString(",")))
              val stopReplicaRequest = new StopReplicaRequest(deletePartitions, Set.empty[(String, Int)] ++ replicas, controllerId,
                                                              controllerEpoch, correlationId)
              sendRequest(broker, stopReplicaRequest, null)
            }
        }
        m.clear()
      }
    }
  }
}

case class ControllerBrokerStateInfo(channel: BlockingChannel,
                                     broker: Broker,
                                     messageQueue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],
                                     requestSendThread: RequestSendThread)

