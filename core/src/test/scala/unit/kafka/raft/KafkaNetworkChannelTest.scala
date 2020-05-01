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
import java.util.Collections
import java.util.concurrent.atomic.AtomicReference

import org.apache.kafka.clients.MockClient
import org.apache.kafka.clients.MockClient.MockMetadataUpdater
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.{BeginQuorumEpochRequestData, BeginQuorumEpochResponseData, EndQuorumEpochRequestData, EndQuorumEpochResponseData, FetchQuorumRecordsRequestData, FetchQuorumRecordsResponseData, FindQuorumRequestData, FindQuorumResponseData, VoteRequestData, VoteResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.requests.{AbstractResponse, BeginQuorumEpochResponse, EndQuorumEpochResponse, FetchQuorumRecordsResponse, FindQuorumResponse, VoteResponse}
import org.apache.kafka.common.utils.{MockTime, Time}
import org.apache.kafka.raft.{RaftRequest, RaftResponse}
import org.junit.Assert._
import org.junit.Test

import scala.jdk.CollectionConverters._

class KafkaNetworkChannelTest {
  import KafkaNetworkChannelTest._

  private val clusterId = "clusterId"
  private val clientId = "clientId"
  private val retryBackoffMs = 100
  private val requestTimeoutMs = 30000
  private val time = new MockTime()
  private val client = new MockClient(time, new StubMetadataUpdater)
  private val channel = new KafkaNetworkChannel(time, client, clientId, retryBackoffMs, requestTimeoutMs)

  @Test
  def testSendToUnknownDestination(): Unit = {
    val destinationId = 2
    assertBrokerNotAvailable(destinationId)
  }

  @Test
  def testSendToBlackedOutDestination(): Unit = {
    val destinationId = 2
    val destinationNode = new Node(destinationId, "127.0.0.1", 9092)
    channel.updateEndpoint(destinationId, new InetSocketAddress(destinationNode.host, destinationNode.port))
    client.blackout(destinationNode, 500)
    assertBrokerNotAvailable(destinationId)
  }

  @Test
  def testSendAndDisconnect(): Unit = {
    val destinationId = 2
    val destinationNode = new Node(destinationId, "127.0.0.1", 9092)
    channel.updateEndpoint(destinationId, new InetSocketAddress(destinationNode.host, destinationNode.port))

    for (apiKey <- RaftApis) {
      val response = KafkaNetworkChannel.buildResponse(buildTestErrorResponse(apiKey, Errors.INVALID_REQUEST))
      client.prepareResponseFrom(response, destinationNode, true)
      sendAndAssertErrorResponse(apiKey, destinationId, Errors.BROKER_NOT_AVAILABLE)
    }
  }

  @Test
  def testSendAndFailAuthentication(): Unit = {
    val destinationId = 2
    val destinationNode = new Node(destinationId, "127.0.0.1", 9092)
    channel.updateEndpoint(destinationId, new InetSocketAddress(destinationNode.host, destinationNode.port))

    for (apiKey <- RaftApis) {
      client.createPendingAuthenticationError(destinationNode, 100)
      sendAndAssertErrorResponse(apiKey, destinationId, Errors.CLUSTER_AUTHORIZATION_FAILED)

      // reset to clear blackout time
      client.reset()
    }
  }

  private def assertBrokerNotAvailable(destinationId: Int): Unit = {
    for (apiKey <- RaftApis) {
      sendAndAssertErrorResponse(apiKey, destinationId, Errors.BROKER_NOT_AVAILABLE)
    }
  }

  @Test
  def testSendAndReceiveOutboundRequest(): Unit = {
    val destinationId = 2
    val destinationNode = new Node(destinationId, "127.0.0.1", 9092)
    channel.updateEndpoint(destinationId, new InetSocketAddress(destinationNode.host, destinationNode.port))

    for (apiKey <- RaftApis) {
      val expectedError = Errors.INVALID_REQUEST
      val response = KafkaNetworkChannel.buildResponse(buildTestErrorResponse(apiKey, expectedError))
      client.prepareResponseFrom(response, destinationNode)
      sendAndAssertErrorResponse(apiKey, destinationId, expectedError)
    }
  }

  @Test
  def testReceiveAndSendInboundRequest(): Unit = {
    for (apiKey <- RaftApis) {
      val request = KafkaNetworkChannel.buildRequest(buildTestRequest(apiKey))
      val responseRef = new AtomicReference[AbstractResponse]()

      channel.postInboundRequest(request.build(), responseRef.set)
      val inbound = channel.receive(1000).asScala
      assertEquals(1, inbound.size)

      val inboundRequest = inbound.head.asInstanceOf[RaftRequest.Inbound]
      val requestId = inboundRequest.requestId()

      val errorResponse = buildTestErrorResponse(apiKey, Errors.INVALID_REQUEST)
      val outboundResponse = new RaftResponse.Outbound(requestId, errorResponse)
      channel.send(outboundResponse)
      channel.receive(1000)

      assertNotNull(responseRef.get)
      assertEquals(1, responseRef.get.errorCounts.get(Errors.INVALID_REQUEST))
    }
  }

  private def sendAndAssertErrorResponse(apiKey: ApiKeys,
                                         destinationId: Int,
                                         error: Errors): Unit = {
    val requestId = channel.newRequestId()
    val createdTimeMs = time.milliseconds()
    val apiRequest = buildTestRequest(apiKey)
    val request = new RaftRequest.Outbound(requestId, apiRequest, destinationId, createdTimeMs)

    channel.send(request)
    val responses = channel.receive(1000).asScala
    assertEquals(1, responses.size)

    val response = responses.head.asInstanceOf[RaftResponse.Inbound]
    assertEquals(destinationId, response.sourceId)
    assertEquals(requestId, response.requestId)
    assertEquals(apiKey, ApiKeys.forId(response.data.apiKey))
    assertEquals(error, extractError(response.data))
  }

  private def buildTestRequest(key: ApiKeys): ApiMessage = {
    key match {
      case ApiKeys.BEGIN_QUORUM_EPOCH =>
        new BeginQuorumEpochRequestData()
          .setClusterId(clusterId)
          .setLeaderEpoch(5)
          .setLeaderId(1)

      case ApiKeys.END_QUORUM_EPOCH =>
        new EndQuorumEpochRequestData()
          .setClusterId(clusterId)
          .setLeaderEpoch(5)
          .setReplicaId(1)
          .setLeaderId(1)

      case ApiKeys.VOTE =>
        new VoteRequestData()
          .setClusterId(clusterId)
          .setCandidateEpoch(5)
          .setCandidateId(1)
          .setLastEpoch(4)
          .setLastEpochEndOffset(329)

      case ApiKeys.FETCH_QUORUM_RECORDS =>
        new FetchQuorumRecordsRequestData()
          .setClusterId(clusterId)
          .setLeaderEpoch(5)
          .setReplicaId(1)
          .setFetchOffset(333)
          .setLastFetchedEpoch(5)

      case ApiKeys.FIND_QUORUM =>
        new FindQuorumRequestData()
          .setHost("localhost")
          .setPort(9092)
          .setReplicaId(1)
          .setBootTimestamp(time.milliseconds())

      case _ =>
        throw new AssertionError(s"Unexpected api $key")
    }
  }

  private def buildTestErrorResponse(key: ApiKeys, error: Errors): ApiMessage = {
    key match {
      case ApiKeys.BEGIN_QUORUM_EPOCH =>
        new BeginQuorumEpochResponseData()
          .setErrorCode(error.code)

      case ApiKeys.END_QUORUM_EPOCH =>
        new EndQuorumEpochResponseData()
          .setErrorCode(error.code)

      case ApiKeys.VOTE =>
        new VoteResponseData()
          .setErrorCode(error.code)

      case ApiKeys.FETCH_QUORUM_RECORDS =>
        new FetchQuorumRecordsResponseData()
          .setErrorCode(error.code)

      case ApiKeys.FIND_QUORUM =>
        new FindQuorumResponseData()
          .setErrorCode(error.code)

      case _ =>
        throw new AssertionError(s"Unexpected api $key")
    }
  }

  private def extractError(response: ApiMessage): Errors = {
    val code = response match {
      case res: BeginQuorumEpochResponseData => res.errorCode
      case res: EndQuorumEpochResponseData => res.errorCode
      case res: FetchQuorumRecordsResponseData => res.errorCode
      case res: VoteResponseData => res.errorCode
      case res: FindQuorumResponseData => res.errorCode
    }
    Errors.forCode(code)
  }

}

object KafkaNetworkChannelTest {
  val RaftApis = Seq(
    ApiKeys.VOTE,
    ApiKeys.BEGIN_QUORUM_EPOCH,
    ApiKeys.END_QUORUM_EPOCH,
    ApiKeys.FETCH_QUORUM_RECORDS,
    ApiKeys.FIND_QUORUM
  )

  private class StubMetadataUpdater extends MockMetadataUpdater {
    override def fetchNodes(): util.List[Node] = Collections.emptyList()

    override def isUpdateNeeded: Boolean = false

    override def update(time: Time, update: MockClient.MetadataUpdate): Unit = {}
  }
}
