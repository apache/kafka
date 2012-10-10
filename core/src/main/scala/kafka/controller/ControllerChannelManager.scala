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
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if(requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

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
          case RequestKeys.LeaderAndIsrKey =>
            response = LeaderAndISRResponse.readFrom(receive.buffer)
          case RequestKeys.StopReplicaKey =>
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

// TODO: When we add more types of requests, we can generalize this class a bit. Right now, it just handles LeaderAndIsr
// request
class ControllerBrokerRequestBatch(sendRequest: (Int, RequestOrResponse, (RequestOrResponse) => Unit) => Unit)
  extends  Logging {
  val brokerRequestMap = new mutable.HashMap[Int, mutable.HashMap[(String, Int), LeaderAndIsr]]

  def newBatch() {
    // raise error if the previous batch is not empty
    if(brokerRequestMap.size > 0)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some state changes %s might be lost ".format(brokerRequestMap.toString()))
    brokerRequestMap.clear()
  }

  def addRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int, leaderAndIsr: LeaderAndIsr) {
    brokerIds.foreach { brokerId =>
      brokerRequestMap.getOrElseUpdate(brokerId, new mutable.HashMap[(String, Int), LeaderAndIsr])
      brokerRequestMap(brokerId).put((topic, partition), leaderAndIsr)
    }
  }

  def sendRequestsToBrokers() {
    brokerRequestMap.foreach { m =>
      val broker = m._1
      val leaderAndIsr = m._2
      val leaderAndIsrRequest = new LeaderAndIsrRequest(leaderAndIsr)
      info("Sending to broker %d leaderAndIsr request of %s".format(broker, leaderAndIsrRequest))
      sendRequest(broker, leaderAndIsrRequest, null)
    }
    brokerRequestMap.clear()
  }
}

case class ControllerBrokerStateInfo(channel: BlockingChannel,
                                     broker: Broker,
                                     messageQueue: BlockingQueue[(RequestOrResponse, (RequestOrResponse) => Unit)],
                                     requestSendThread: RequestSendThread)

