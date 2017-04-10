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

import kafka.utils.{ShutdownableThread, ZkUtils}
import org.apache.kafka.clients.{ClientResponse, NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Time

import scala.collection.immutable

object InterBrokerSendThread {

}

/**
 *  Abstract class for inter-broker send thread that utilize a non-blocking network client.
 */
abstract class InterBrokerSendThread(name: String,
                                     networkClient: NetworkClient,
                                     time: Time)
  extends ShutdownableThread(name, isInterruptible = false) {

  // generate a request for each required destination broker
  def generateRequests(): immutable.Map[Node, RequestAndCompletionHandler]

  override def doWork() {
    val now = time.milliseconds()
    var pollTimeout = Long.MaxValue

    val requestsToSend: immutable.Map[Node, RequestAndCompletionHandler] = generateRequests()
    for ((destNode: Node, requestAndCallback: RequestAndCompletionHandler) <- requestsToSend) {
      val destination = Integer.toString(destNode.id)
      val completionHandler = requestAndCallback.handler
      val clientRequest = networkClient.newClientRequest(destination,
        requestAndCallback.request,
        now,
        true,
        completionHandler)

      if (networkClient.ready(destNode, now)) {
        networkClient.send(clientRequest, now)
      } else {
        val disConnectedResponse: ClientResponse = new ClientResponse(clientRequest.makeHeader(requestAndCallback.request.desiredOrLatestVersion()),
          completionHandler, destination,
          now /* createdTimeMs */, now /* receivedTimeMs */, true /* disconnected */, null /* versionMismatch */, null /* responseBody */)

        // pool timeout would be the minimum of connection delay if there are any dest yet to be reached;
        // otherwise it is infinity
        pollTimeout = Math.min(pollTimeout, networkClient.connectionDelay(destNode, now))

        completionHandler.onComplete(disConnectedResponse)
      }
    }
    networkClient.poll(pollTimeout, now)
  }
}

case class RequestAndCompletionHandler(request: AbstractRequest.Builder[_ <: AbstractRequest], handler: RequestCompletionHandler)