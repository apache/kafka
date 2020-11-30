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
package kafka.raft

import java.net.InetSocketAddress
import java.util
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import kafka.utils.Logging
import org.apache.kafka.clients.{ClientRequest, ClientResponse, KafkaClient}
import org.apache.kafka.common.message._
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{KafkaException, Node}
import org.apache.kafka.raft.{NetworkChannel, RaftMessage, RaftRequest, RaftResponse, RaftUtil}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object KafkaNetworkChannel {

  private[raft] def buildResponse(responseData: ApiMessage): AbstractResponse = {
    responseData match {
      case voteResponse: VoteResponseData =>
        new VoteResponse(voteResponse)
      case beginEpochResponse: BeginQuorumEpochResponseData =>
        new BeginQuorumEpochResponse(beginEpochResponse)
      case endEpochResponse: EndQuorumEpochResponseData =>
        new EndQuorumEpochResponse(endEpochResponse)
      case fetchResponse: FetchResponseData =>
        new FetchResponse(fetchResponse)
      case _ =>
        throw new IllegalArgumentException(s"Unexpected type for responseData: $responseData")
    }
  }

  private[raft] def buildRequest(requestData: ApiMessage): AbstractRequest.Builder[_ <: AbstractRequest] = {
    requestData match {
      case voteRequest: VoteRequestData =>
        new VoteRequest.Builder(voteRequest)
      case beginEpochRequest: BeginQuorumEpochRequestData =>
        new BeginQuorumEpochRequest.Builder(beginEpochRequest)
      case endEpochRequest: EndQuorumEpochRequestData =>
        new EndQuorumEpochRequest.Builder(endEpochRequest)
      case fetchRequest: FetchRequestData =>
        // Since we already have the request, we go through a simplified builder
        new AbstractRequest.Builder[FetchRequest](ApiKeys.FETCH) {
          override def build(version: Short): FetchRequest = new FetchRequest(fetchRequest, version)
        }
      case _ =>
        throw new IllegalArgumentException(s"Unexpected type for requestData: $requestData")
    }
  }

  private[raft] def responseData(response: AbstractResponse): ApiMessage = {
    response match {
      case voteResponse: VoteResponse => voteResponse.data
      case beginEpochResponse: BeginQuorumEpochResponse => beginEpochResponse.data
      case endEpochResponse: EndQuorumEpochResponse => endEpochResponse.data
      case fetchResponse: FetchResponse[_] => fetchResponse.data
      case _ => throw new IllegalArgumentException(s"Unexpected type for response: $response")
    }
  }

  private[raft] def requestData(request: AbstractRequest): ApiMessage = {
    request match {
      case voteRequest: VoteRequest => voteRequest.data
      case beginEpochRequest: BeginQuorumEpochRequest => beginEpochRequest.data
      case endEpochRequest: EndQuorumEpochRequest => endEpochRequest.data
      case fetchRequest: FetchRequest => fetchRequest.data
      case _ => throw new IllegalArgumentException(s"Unexpected type for request: $request")
    }
  }

}

class KafkaNetworkChannel(time: Time,
                          client: KafkaClient,
                          clientId: String,
                          retryBackoffMs: Int,
                          requestTimeoutMs: Int) extends NetworkChannel with Logging {
  import KafkaNetworkChannel._

  type ResponseHandler = AbstractResponse => Unit

  private val correlationIdCounter = new AtomicInteger(0)
  private val pendingInbound = mutable.Map.empty[Long, ResponseHandler]
  private val undelivered = new ArrayBlockingQueue[RaftMessage](10)
  private val pendingOutbound = new ArrayBlockingQueue[RaftRequest.Outbound](10)
  private val endpoints = mutable.HashMap.empty[Int, Node]

  override def newCorrelationId(): Int = correlationIdCounter.getAndIncrement()

  private def buildClientRequest(req: RaftRequest.Outbound): ClientRequest = {
    val destination = req.destinationId.toString
    val request = buildRequest(req.data)
    val correlationId = req.correlationId
    val createdTimeMs = req.createdTimeMs
    new ClientRequest(destination, request, correlationId, clientId, createdTimeMs, true,
      requestTimeoutMs, null)
  }

  override def send(message: RaftMessage): Unit = {
    message match {
      case request: RaftRequest.Outbound =>
        if (!pendingOutbound.offer(request))
          throw new KafkaException("Pending outbound queue is full")

      case response: RaftResponse.Outbound =>
        pendingInbound.remove(response.correlationId).foreach { onResponseReceived: ResponseHandler =>
          onResponseReceived(buildResponse(response.data))
        }
      case _ =>
        throw new IllegalArgumentException("Unhandled message type " + message)
    }
  }

  private def sendOutboundRequests(currentTimeMs: Long): Unit = {
    while (!pendingOutbound.isEmpty) {
      val request = pendingOutbound.peek()
      endpoints.get(request.destinationId) match {
        case Some(node) =>
          if (client.connectionFailed(node)) {
            pendingOutbound.poll()
            val apiKey = ApiKeys.forId(request.data.apiKey)
            val disconnectResponse = RaftUtil.errorResponse(apiKey, Errors.BROKER_NOT_AVAILABLE)
            val success = undelivered.offer(new RaftResponse.Inbound(
              request.correlationId, disconnectResponse, request.destinationId))
            if (!success) {
              throw new KafkaException("Undelivered queue is full")
            }

            // Make sure to reset the connection state
            client.ready(node, currentTimeMs)
          } else if (client.ready(node, currentTimeMs)) {
            pendingOutbound.poll()
            val clientRequest = buildClientRequest(request)
            client.send(clientRequest, currentTimeMs)
          } else {
            // We will retry this request on the next poll
            return
          }

        case None =>
          pendingOutbound.poll()
          val apiKey = ApiKeys.forId(request.data.apiKey)
          val responseData = RaftUtil.errorResponse(apiKey, Errors.BROKER_NOT_AVAILABLE)
          val response = new RaftResponse.Inbound(request.correlationId, responseData, request.destinationId)
          if (!undelivered.offer(response))
            throw new KafkaException("Undelivered queue is full")
      }
    }
  }

  def getConnectionInfo(nodeId: Int): Node = {
    if (!endpoints.contains(nodeId))
      null
    else
      endpoints(nodeId)
  }

  def allConnections(): Set[Node] = {
    endpoints.values.toSet
  }

  private def buildInboundRaftResponse(response: ClientResponse): RaftResponse.Inbound = {
    val header = response.requestHeader()
    val data = if (response.authenticationException != null) {
      RaftUtil.errorResponse(header.apiKey, Errors.CLUSTER_AUTHORIZATION_FAILED)
    } else if (response.wasDisconnected) {
      RaftUtil.errorResponse(header.apiKey, Errors.BROKER_NOT_AVAILABLE)
    } else {
      responseData(response.responseBody)
    }
    new RaftResponse.Inbound(header.correlationId, data, response.destination.toInt)
  }

  private def pollInboundResponses(timeoutMs: Long, inboundMessages: util.List[RaftMessage]): Unit = {
    val responses = client.poll(timeoutMs, time.milliseconds())
    for (response <- responses.asScala) {
      inboundMessages.add(buildInboundRaftResponse(response))
    }
  }

  private def drainInboundRequests(inboundMessages: util.List[RaftMessage]): Unit = {
    undelivered.drainTo(inboundMessages)
  }

  private def pollInboundMessages(timeoutMs: Long): util.List[RaftMessage] = {
    val pollTimeoutMs = if (!undelivered.isEmpty) {
      0L
    } else if (!pendingOutbound.isEmpty) {
      retryBackoffMs
    } else {
      timeoutMs
    }
    val messages = new util.ArrayList[RaftMessage]
    pollInboundResponses(pollTimeoutMs, messages)
    drainInboundRequests(messages)
    messages
  }

  override def receive(timeoutMs: Long): util.List[RaftMessage] = {
    sendOutboundRequests(time.milliseconds())
    pollInboundMessages(timeoutMs)
  }

  override def wakeup(): Unit = {
    client.wakeup()
  }

  override def updateEndpoint(id: Int, address: InetSocketAddress): Unit = {
    val node = new Node(id, address.getHostString, address.getPort)
    endpoints.put(id, node)
  }

  def postInboundRequest(request: AbstractRequest, onResponseReceived: ResponseHandler): Unit = {
    val data = requestData(request)
    val correlationId = newCorrelationId()
    val req = new RaftRequest.Inbound(correlationId, data, time.milliseconds())
    pendingInbound.put(correlationId, onResponseReceived)
    if (!undelivered.offer(req))
      throw new KafkaException("Undelivered queue is full")
    wakeup()
  }

  override def close(): Unit = {
    client.close()
  }

}
