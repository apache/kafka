/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.common

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import kafka.cluster.Broker
import kafka.utils.{ShutdownableThread, ZkUtils}
import org.apache.kafka.clients.{ClientResponse, NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse}
import org.apache.kafka.common.utils.Time

import scala.collection.mutable

object InterBrokerSendThread {

  sealed trait CallBackResult { def byte: Byte }

  case object OK extends CallBackResult { val byte: Byte = 0 }

  case object Retry extends CallBackResult { val byte: Byte = 1 }

  case object Fatal extends CallBackResult { val byte: Byte = 2 }

  type RequestCompletionCallback = AbstractResponse => CallBackResult
}

/**
 *  Abstract class for inter-broker send thread that utilize a non-blocking network client.
 */
class InterBrokerSendThread(name: String,
                            zkUtils: ZkUtils,
                            networkClient: NetworkClient,
                            interBrokerListenerName: ListenerName,
                            time: Time)
  extends ShutdownableThread(name, isInterruptible = false) {

  private val brokerStateMap: mutable.Map[Int, DestinationBrokerState] = new mutable.HashMap[Int, DestinationBrokerState]()

  private def addNewBroker(broker: Broker) {
    val messageQueue = new LinkedBlockingQueue[RequestAndCallback]
    val brokerEndPoint = broker.getBrokerEndPoint(interBrokerListenerName)
    val brokerNode = new Node(broker.id, brokerEndPoint.host, brokerEndPoint.port)

    brokerStateMap.put(broker.id, DestinationBrokerState(brokerNode, messageQueue))

    debug(s"Added destination broker ${broker.id}")
  }

  private def removeExistingBroker(brokerState: DestinationBrokerState) {
    try {
      val state = brokerStateMap.remove(brokerState.destBrokerNode.id)

      if (state.isDefined && !state.get.requestQueue.isEmpty)
        info(s"Removing destination broker ${brokerState.destBrokerNode.id} while it still have pending requests ${state.get.requestQueue}")

      debug(s"Removed destination broker ${brokerState.destBrokerNode.id}")
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  def addBroker(broker: Broker) {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerStateMap synchronized {
      if(!brokerStateMap.contains(broker.id)) {
        addNewBroker(broker)
      }
    }
  }

  def removeBroker(brokerId: Int) {
    brokerStateMap synchronized {
      removeExistingBroker(brokerStateMap(brokerId))
    }
  }

  def addRequest(destBrokerId: Int, request: AbstractRequest.Builder[_ <: AbstractRequest], callback: InterBrokerSendThread.RequestCompletionCallback): Boolean = {
    val brokerStateView = brokerStateMap synchronized brokerStateMap.toMap

    if (brokerStateView.contains(destBrokerId)) {
      brokerStateView(destBrokerId).requestQueue.add(RequestAndCallback(request, callback))
      true
    } else {
      false
    }
  }

  override def doWork() {
    val brokerStates = brokerStateMap synchronized brokerStateMap.toMap.values

    val now = time.milliseconds()
    var poolTimeout = Long.MaxValue

    // for each reachable destination broker, try to send the first queued request to it;
    // only remove the reques from the queue upon successful callback to ensure ordering
    for (brokerState: DestinationBrokerState <- brokerStates) {
      val destNode = brokerState.destBrokerNode
      val requests = brokerState.requestQueue
      if (networkClient.ready(destNode, now)) {
        val requestAndCallback = requests.peek()
        if (requestAndCallback != null) {
          val callback: RequestCompletionHandler = new RequestCompletionHandler() {
            override def onComplete(response: ClientResponse) {
              val result = requestAndCallback.callback(response.responseBody())
              result match {
                case InterBrokerSendThread.OK =>
                  debug(s"Request with apiKey ${requestAndCallback.request.apiKey} to destination broker ${destNode.id} has completed OK")

                  if (requestAndCallback != requests.poll())
                    throw new IllegalStateException("The responded request does not match the first queued request; this should never happen")

                case InterBrokerSendThread.Retry =>
                  debug(s"Request with apiKey ${requestAndCallback.request.apiKey} to destination broker ${destNode.id} did not complete, will retry in the next iteration")

                case InterBrokerSendThread.Fatal =>
                  info(s"Request with apiKey ${requestAndCallback.request.apiKey} to destination broker ${destNode.id} has failed permanently, will not retry but remove it from the queue")

                  if (requestAndCallback != requests.poll())
                    throw new IllegalStateException("The responded request does not match the first queued request; this should never happen")
              }
            }
          }
          val clientRequest = networkClient.newClientRequest(Integer.toString(destNode.id), requestAndCallback.request, now, true, callback)
          networkClient.send(clientRequest, now)
        }
      } else {
        // pool timeout would be the minimum of connection delay if there are any dest yet to be reached;
        // otherwise it is infinity
        poolTimeout = Math.min(poolTimeout, networkClient.connectionDelay(destNode, now))
      }
    }

    networkClient.poll(poolTimeout, now)
  }
}

case class DestinationBrokerState(destBrokerNode: Node, requestQueue: BlockingQueue[RequestAndCallback])

case class RequestAndCallback(request: AbstractRequest.Builder[_ <: AbstractRequest], callback: InterBrokerSendThread.RequestCompletionCallback)