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

import kafka.utils.ShutdownableThread
import org.apache.kafka.clients.{ClientResponse, NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Time


/**
 *  Class for inter-broker send thread that utilize a non-blocking network client.
 */
class InterBrokerSendThread(name: String,
                            networkClient: NetworkClient,
                            requestGenerator: () => Iterable[RequestAndCompletionHandler],
                            time: Time)
  extends ShutdownableThread(name, isInterruptible = false) {

  override def doWork() {
    val now = time.milliseconds()
    var pollTimeout = Long.MaxValue

    val requestsToSend: Iterable[RequestAndCompletionHandler] = requestGenerator()

    for (request: RequestAndCompletionHandler <- requestsToSend) {
      val destination = Integer.toString(request.destination.id())
      val completionHandler = request.handler
      // TODO: Need to check inter broker protocol and error if new request is not supported
      val clientRequest = networkClient.newClientRequest(destination,
        request.request,
        now,
        true,
        completionHandler)

      if (networkClient.ready(request.destination, now)) {
        networkClient.send(clientRequest, now)
      } else {
        val disConnectedResponse: ClientResponse = new ClientResponse(clientRequest.makeHeader(request.request.desiredOrLatestVersion()),
          completionHandler, destination,
          now /* createdTimeMs */, now /* receivedTimeMs */, true /* disconnected */, null /* versionMismatch */, null /* responseBody */)

        // poll timeout would be the minimum of connection delay if there are any dest yet to be reached;
        // otherwise it is infinity
        pollTimeout = Math.min(pollTimeout, networkClient.connectionDelay(request.destination, now))

        completionHandler.onComplete(disConnectedResponse)
      }
    }
    networkClient.poll(pollTimeout, now)
  }
}

case class RequestAndCompletionHandler(destination: Node, request: AbstractRequest.Builder[_ <: AbstractRequest], handler: RequestCompletionHandler)