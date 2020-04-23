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
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.{ClientRequest, ClientResponse, KafkaClient}
import org.apache.kafka.common.message._
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{KafkaException, Node}
import org.apache.kafka.raft.{NetworkChannel, RaftMessage, RaftRequest, RaftResponse}

import scala.collection.JavaConverters._
import scala.collection.mutable

class KafkaNetworkChannel(time: Time,
                          client: KafkaClient,
                          clientId: String,
                          retryBackoffMs: Int,
                          requestTimeoutMs: Int) extends NetworkChannel {
  type ResponseHandler = AbstractResponse => Unit

  private val requestIdCounter = new AtomicInteger(0)
  private val pendingInbound = mutable.Map.empty[Long, ResponseHandler]
  private val undelivered = new ArrayBlockingQueue[RaftMessage](10)
  private val pendingOutbound = new ArrayBlockingQueue[RaftRequest.Outbound](10)
  private val endpoints = mutable.HashMap.empty[Int, Node]

  override def newRequestId(): Int = requestIdCounter.getAndIncrement()

  private def buildResponse(responseData: ApiMessage): AbstractResponse = {
    responseData match {
      case voteResponse: VoteResponseData =>
        new VoteResponse(voteResponse)
      case beginEpochResponse: BeginQuorumEpochResponseData =>
        new BeginQuorumEpochResponse(beginEpochResponse)
      case endEpochResponse: EndQuorumEpochResponseData =>
        new EndQuorumEpochResponse(endEpochResponse)
      case fetchRecordsResponse: FetchQuorumRecordsResponseData =>
        new FetchQuorumRecordsResponse(fetchRecordsResponse)
      case findLeaderResponse: FindQuorumResponseData =>
        new FindQuorumResponse(findLeaderResponse)
    }
  }

  private def buildRequest(requestData: ApiMessage): AbstractRequest.Builder[_ <: AbstractRequest] = {
    requestData match {
      case voteRequest: VoteRequestData =>
        new VoteRequest.Builder(voteRequest)
      case beginEpochRequest: BeginQuorumEpochRequestData =>
        new BeginQuorumEpochRequest.Builder(beginEpochRequest)
      case endEpochRequest: EndQuorumEpochRequestData =>
        new EndQuorumEpochRequest.Builder(endEpochRequest)
      case fetchRecordsRequest: FetchQuorumRecordsRequestData =>
        new FetchQuorumRecordsRequest.Builder(fetchRecordsRequest)
      case findLeaderRequest: FindQuorumRequestData =>
        new FindQuorumRequest.Builder(findLeaderRequest)
    }
  }

  private def buildClientRequest(req: RaftRequest.Outbound): ClientRequest = {
    val destination = req.destinationId.toString
    val request = buildRequest(req.data)
    val correlationId = req.requestId
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
        pendingInbound.remove(response.requestId).foreach { onResponseReceived: ResponseHandler =>
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
          if (client.isReady(node, currentTimeMs)) {
            pendingOutbound.poll()
            val clientRequest = buildClientRequest(request)
            client.send(clientRequest, currentTimeMs)
          } else {
            // We will retry this request on the next poll
            return
          }

        case None =>
          pendingOutbound.poll()
          val responseData = errorResponseData(apiKeyForRequest(request), Errors.BROKER_NOT_AVAILABLE)
          val response = new RaftResponse.Inbound(request.requestId, responseData, request.destinationId)
          if (!undelivered.offer(response))
            throw new KafkaException("Undelivered queue is full")
      }
    }
  }

  private def apiKeyForRequest(request: RaftRequest): ApiKeys = {
    // TODO: could probably push ApiKey into RaftRequest to make this annoying method go away
    request.data match {
      case _: VoteRequestData => ApiKeys.VOTE
      case _: BeginQuorumEpochRequestData => ApiKeys.BEGIN_QUORUM_EPOCH
      case _: EndQuorumEpochRequestData => ApiKeys.END_QUORUM_EPOCH
      case _: FetchQuorumRecordsRequestData => ApiKeys.FETCH_QUORUM_RECORDS
      case _: FindQuorumRequestData => ApiKeys.FIND_QUORUM
      case _ => throw new IllegalArgumentException(s"Unknown request type ${request.data}")
    }
  }

  private def errorResponseData(apiKey: ApiKeys, error: Errors): ApiMessage = {
    apiKey match {
      case ApiKeys.VOTE =>
        new VoteResponseData()
          .setErrorCode(error.code)
          .setVoteGranted(false)
          .setLeaderEpoch(-1)
          .setLeaderId(-1)
      case ApiKeys.BEGIN_QUORUM_EPOCH =>
        new BeginQuorumEpochResponseData()
          .setErrorCode(error.code)
          .setLeaderEpoch(-1)
          .setLeaderId(-1)
      case ApiKeys.END_QUORUM_EPOCH =>
        new EndQuorumEpochResponseData()
          .setErrorCode(error.code)
          .setLeaderEpoch(-1)
          .setLeaderId(-1)
      case ApiKeys.FETCH_QUORUM_RECORDS =>
        new FetchQuorumRecordsResponseData()
          .setErrorCode(error.code)
          .setLeaderEpoch(-1)
          .setLeaderId(-1)
          .setHighWatermark(-1)
          .setRecords(ByteBuffer.wrap(Array.emptyByteArray))
      case ApiKeys.FIND_QUORUM =>
        new FindQuorumResponseData()
          .setErrorCode(error.code)
          .setLeaderEpoch(-1)
          .setLeaderId(-1)
      case _ =>
        throw new IllegalArgumentException(s"Received response for unexpected request type: $apiKey")
    }
  }

  private def buildInboundRaftResponse(response: ClientResponse): RaftResponse.Inbound = {
    val header = response.requestHeader()
    val data = if (response.wasDisconnected) {
      errorResponseData(header.apiKey, Errors.BROKER_NOT_AVAILABLE)
    } else if (response.authenticationException != null) {
      errorResponseData(header.apiKey, Errors.CLUSTER_AUTHORIZATION_FAILED)
    } else {
      responseData(response.responseBody)
    }
    new RaftResponse.Inbound(header.correlationId, data, response.destination.toInt)
  }

  private def responseData(response: AbstractResponse): ApiMessage = {
    response match {
      case voteResponse: VoteResponse => voteResponse.data
      case beginEpochResponse: BeginQuorumEpochResponse => beginEpochResponse.data
      case endEpochResponse: EndQuorumEpochResponse => endEpochResponse.data
      case fetchRecordsResponse: FetchQuorumRecordsResponse => fetchRecordsResponse.data
      case findLeaderResponse: FindQuorumResponse => findLeaderResponse.data
    }
  }

  private def requestData(request: AbstractRequest): ApiMessage = {
    request match {
      case voteRequest: VoteRequest => voteRequest.data
      case beginEpochRequest: BeginQuorumEpochRequest => beginEpochRequest.data
      case endEpochRequest: EndQuorumEpochRequest => endEpochRequest.data
      case fetchRecordsRequest: FetchQuorumRecordsRequest => fetchRecordsRequest.data
      case findLeaderRequest: FindQuorumRequest => findLeaderRequest.data
    }
  }

  private def pollInboundResponses(timeoutMs: Long): util.List[RaftMessage] = {
    val pollTimeoutMs = if (!undelivered.isEmpty) {
      0L
    } else if (!pendingOutbound.isEmpty) {
      retryBackoffMs
    } else {
      timeoutMs
    }
    val responses = client.poll(pollTimeoutMs, time.milliseconds())
    val messages = new util.ArrayList[RaftMessage]
    for (response <- responses.asScala) {
      messages.add(buildInboundRaftResponse(response))
    }
    undelivered.drainTo(messages)
    messages
  }

  override def receive(timeoutMs: Long): util.List[RaftMessage] = {
    sendOutboundRequests(time.milliseconds())
    pollInboundResponses(timeoutMs)
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
    val requestId = newRequestId()
    val req = new RaftRequest.Inbound(requestId, data, time.milliseconds())
    pendingInbound.put(requestId, onResponseReceived)
    if (!undelivered.offer(req))
      throw new KafkaException("Undelivered queue is full")
    wakeup()
  }

}
