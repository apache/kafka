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

import org.apache.kafka.clients.MockClient.MockMetadataUpdater
import org.apache.kafka.clients.{ApiVersion, MockClient, NodeApiVersions}
import org.apache.kafka.common.message.{BeginQuorumEpochResponseData, EndQuorumEpochResponseData, FetchResponseData, VoteResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.requests.{AbstractResponse, BeginQuorumEpochRequest, EndQuorumEpochRequest, VoteRequest, VoteResponse}
import org.apache.kafka.common.utils.{MockTime, Time}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.raft.{RaftRequest, RaftResponse, RaftUtil}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.jdk.CollectionConverters._

class KafkaNetworkChannelTest {
  import KafkaNetworkChannelTest._

  private val clusterId = "clusterId"
  private val clientId = "clientId"
  private val retryBackoffMs = 100
  private val requestTimeoutMs = 30000
  private val time = new MockTime()
  private val client = new MockClient(time, new StubMetadataUpdater)
  private val topicPartition = new TopicPartition("topic", 0)
  private val channel = new KafkaNetworkChannel(time, client, clientId, retryBackoffMs, requestTimeoutMs)

  @Before
  def setupSupportedApis(): Unit = {
    val supportedApis = RaftApis.map(api => new ApiVersion(api))
    client.setNodeApiVersions(NodeApiVersions.create(supportedApis.asJava))
  }

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
    client.backoff(destinationNode, 500)
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

      // reset to clear backoff time
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
      val request = KafkaNetworkChannel.buildRequest(buildTestRequest(apiKey)).build()
      val responseRef = new AtomicReference[AbstractResponse]()

      channel.postInboundRequest(request, responseRef.set)
      val inbound = channel.receive(1000).asScala
      assertEquals(1, inbound.size)

      val inboundRequest = inbound.head.asInstanceOf[RaftRequest.Inbound]
      val errorResponse = buildTestErrorResponse(apiKey, Errors.INVALID_REQUEST)
      val outboundResponse = new RaftResponse.Outbound(inboundRequest.correlationId, errorResponse)
      channel.send(outboundResponse)
      channel.receive(1000)

      assertNotNull(responseRef.get)
      assertEquals(Errors.INVALID_REQUEST, extractError(KafkaNetworkChannel.responseData(responseRef.get)))
    }
  }

  private def sendAndAssertErrorResponse(apiKey: ApiKeys,
                                         destinationId: Int,
                                         error: Errors): Unit = {
    val correlationId = channel.newCorrelationId()
    val createdTimeMs = time.milliseconds()
    val apiRequest = buildTestRequest(apiKey)
    val request = new RaftRequest.Outbound(correlationId, apiRequest, destinationId, createdTimeMs)

    channel.send(request)
    val responses = channel.receive(1000).asScala
    assertEquals(1, responses.size)

    val response = responses.head.asInstanceOf[RaftResponse.Inbound]
    assertEquals(destinationId, response.sourceId)
    assertEquals(correlationId, response.correlationId)
    assertEquals(apiKey, ApiKeys.forId(response.data.apiKey))
    assertEquals(error, extractError(response.data))
  }

  private def buildTestRequest(key: ApiKeys): ApiMessage = {
    val leaderEpoch = 5
    val leaderId = 1
    key match {
      case ApiKeys.BEGIN_QUORUM_EPOCH =>
        BeginQuorumEpochRequest.singletonRequest(topicPartition, clusterId, leaderEpoch, leaderId)

      case ApiKeys.END_QUORUM_EPOCH =>
        EndQuorumEpochRequest.singletonRequest(topicPartition, clusterId, leaderId,
          leaderEpoch, Collections.singletonList(2))

      case ApiKeys.VOTE =>
        val lastEpoch = 4
        VoteRequest.singletonRequest(topicPartition, clusterId, leaderEpoch, leaderId, lastEpoch, 329)

      case ApiKeys.FETCH =>
        val request = RaftUtil.singletonFetchRequest(topicPartition, fetchPartition => {
          fetchPartition
            .setCurrentLeaderEpoch(5)
            .setFetchOffset(333)
            .setLastFetchedEpoch(5)
        })
        request.setReplicaId(1)

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
        VoteResponse.singletonResponse(error, topicPartition, Errors.NONE, 1, 5, false);

      case ApiKeys.FETCH =>
        new FetchResponseData()
          .setErrorCode(error.code)

      case _ =>
        throw new AssertionError(s"Unexpected api $key")
    }
  }

  private def extractError(response: ApiMessage): Errors = {
    val code = (response: @unchecked) match {
      case res: BeginQuorumEpochResponseData => res.errorCode
      case res: EndQuorumEpochResponseData => res.errorCode
      case res: FetchResponseData => res.errorCode
      case res: VoteResponseData => res.errorCode
    }
    Errors.forCode(code)
  }

}

object KafkaNetworkChannelTest {
  val RaftApis = Seq(
    ApiKeys.VOTE,
    ApiKeys.BEGIN_QUORUM_EPOCH,
    ApiKeys.END_QUORUM_EPOCH,
    ApiKeys.FETCH,
  )

  private class StubMetadataUpdater extends MockMetadataUpdater {
    override def fetchNodes(): util.List[Node] = Collections.emptyList()

    override def isUpdateNeeded: Boolean = false

    override def update(time: Time, update: MockClient.MetadataUpdate): Unit = {}
  }
}
