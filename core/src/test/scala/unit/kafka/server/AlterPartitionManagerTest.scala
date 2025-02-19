/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Collections
import java.util.stream.{Stream => JStream}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.{AuthenticationException, OperationNotAttemptedException, UnknownServerException, UnsupportedVersionException}
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState
import org.apache.kafka.common.message.{AlterPartitionRequestData, AlterPartitionResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.MessageUtil
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.{AbstractRequest, AlterPartitionRequest, AlterPartitionResponse}
import org.apache.kafka.metadata.{LeaderAndIsr, LeaderRecoveryState}
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, MetadataVersion, NodeToControllerChannelManager}
import org.apache.kafka.server.common.MetadataVersion.{IBP_3_0_IV1, IBP_3_2_IV0, IBP_3_5_IV1}
import org.apache.kafka.server.util.{MockScheduler, MockTime}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, reset, times, verify}
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class AlterPartitionManagerTest {

  val topic = "test-topic"
  val topicId = Uuid.randomUuid()
  val time = new MockTime
  val metrics = new Metrics
  val brokerId = 1

  var brokerToController: NodeToControllerChannelManager = _

  val tp0 = new TopicIdPartition(topicId, 0, topic)
  val tp1 = new TopicIdPartition(topicId, 1, topic)
  val tp2 = new TopicIdPartition(topicId, 2, topic)

  @BeforeEach
  def setup(): Unit = {
    brokerToController = mock(classOf[NodeToControllerChannelManager])
  }

  @ParameterizedTest
  @MethodSource(Array("provideMetadataVersions"))
  def testBasic(metadataVersion: MetadataVersion): Unit = {
    val scheduler = new MockScheduler(time)
    val alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => metadataVersion)
    alterPartitionManager.start()
    alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List(1, 2, 3).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10))
    verify(brokerToController).start()
    verify(brokerToController).sendRequest(any(), any())
  }

  @ParameterizedTest
  @MethodSource(Array("provideMetadataVersions"))
  def testBasicWithBrokerEpoch(metadataVersion: MetadataVersion): Unit = {
    val scheduler = new MockScheduler(time)
    val alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 101, () => metadataVersion)
    alterPartitionManager.start()
    val isrWithBrokerEpoch = ListBuffer[BrokerState]()
    for (ii <- 1 to 3) {
      isrWithBrokerEpoch += new BrokerState().setBrokerId(ii).setBrokerEpoch(100 + ii)
    }
    alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, LeaderRecoveryState.RECOVERED, isrWithBrokerEpoch.toList.asJava, 10))

    val expectedAlterPartitionData = new AlterPartitionRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(101)
    val topicData = new AlterPartitionRequestData.TopicData()
      .setTopicName(topic)
      .setTopicId(topicId)

    val newIsrWithBrokerEpoch = new ListBuffer[BrokerState]()
    newIsrWithBrokerEpoch.append(new BrokerState().setBrokerId(1).setBrokerEpoch(101))
    newIsrWithBrokerEpoch.append(new BrokerState().setBrokerId(2).setBrokerEpoch(102))
    newIsrWithBrokerEpoch.append(new BrokerState().setBrokerId(3).setBrokerEpoch(103))
    topicData.partitions.add(new AlterPartitionRequestData.PartitionData()
      .setPartitionIndex(0)
      .setLeaderEpoch(1)
      .setPartitionEpoch(10)
      .setNewIsrWithEpochs(newIsrWithBrokerEpoch.toList.asJava))

    expectedAlterPartitionData.topics.add(topicData)

    verify(brokerToController).start()
    val captor = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[_ <: AbstractRequest]])
    verify(brokerToController).sendRequest(captor.capture(), any())
    assertEquals(expectedAlterPartitionData, captor.getValue.asInstanceOf[AlterPartitionRequest.Builder].build().data())
  }

  @ParameterizedTest
  @MethodSource(Array("provideLeaderRecoveryState"))
  def testBasicSentLeaderRecoveryState(
    metadataVersion: MetadataVersion,
    leaderRecoveryState: LeaderRecoveryState
  ): Unit = {
    val requestCapture = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[AlterPartitionRequest]])

    val scheduler = new MockScheduler(time)
    val alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => metadataVersion)
    alterPartitionManager.start()
    alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List(1).map(Int.box).asJava, leaderRecoveryState, 10))
    verify(brokerToController).start()
    verify(brokerToController).sendRequest(requestCapture.capture(), any())

    val request = requestCapture.getValue.build()
    val expectedLeaderRecoveryState = if (metadataVersion.isAtLeast(IBP_3_2_IV0)) leaderRecoveryState else LeaderRecoveryState.RECOVERED
    assertEquals(expectedLeaderRecoveryState.value, request.data.topics.get(0).partitions.get(0).leaderRecoveryState())
  }

  @ParameterizedTest
  @MethodSource(Array("provideMetadataVersions"))
  def testOverwriteWithinBatch(metadataVersion: MetadataVersion): Unit = {
    val capture: ArgumentCaptor[AbstractRequest.Builder[AlterPartitionRequest]] = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[AlterPartitionRequest]])
    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => metadataVersion)
    alterPartitionManager.start()

    // Only send one ISR update for a given topic+partition
    val firstSubmitFuture = alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List(1, 2, 3).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10))
    assertFalse(firstSubmitFuture.isDone)

    val failedSubmitFuture = alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List(1, 2).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10))
    assertTrue(failedSubmitFuture.isCompletedExceptionally)
    assertFutureThrows(classOf[OperationNotAttemptedException], failedSubmitFuture)

    // Simulate response
    val alterPartitionResp = partitionResponse()
    val resp = makeClientResponse(
      response = alterPartitionResp,
      version = ApiKeys.ALTER_PARTITION.latestVersion
    )
    verify(brokerToController).sendRequest(capture.capture(), callbackCapture.capture())
    callbackCapture.getValue.onComplete(resp)

    // Now we can submit this partition again
    val newSubmitFuture = alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List(1).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10))
    assertFalse(newSubmitFuture.isDone)

    verify(brokerToController).start()
    verify(brokerToController, times(2)).sendRequest(capture.capture(), callbackCapture.capture())

    // Make sure we sent the right request ISR={1}
    val request = capture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    if (request.version() < 3) {
      assertEquals(request.data.topics.get(0).partitions.get(0).newIsr.size, 1)
    } else {
      assertEquals(request.data.topics.get(0).partitions.get(0).newIsrWithEpochs.size, 1)
    }
  }

  @ParameterizedTest
  @MethodSource(Array("provideMetadataVersions"))
  def testSingleBatch(metadataVersion: MetadataVersion): Unit = {
    val capture: ArgumentCaptor[AbstractRequest.Builder[AlterPartitionRequest]] = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[AlterPartitionRequest]])
    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => metadataVersion)
    alterPartitionManager.start()

    // First request will send batch of one
    alterPartitionManager.submit(new TopicIdPartition(topicId, 0, topic),
      new LeaderAndIsr(1, 1, List(1, 2, 3).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10))

    // Other submissions will queue up until a response
    for (i <- 1 to 9) {
      alterPartitionManager.submit(new TopicIdPartition(topicId, i, topic),
        new LeaderAndIsr(1, 1, List(1, 2, 3).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10))
    }

    // Simulate response, omitting partition 0 will allow it to stay in unsent queue
    val alterPartitionResp = new AlterPartitionResponse(new AlterPartitionResponseData())
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterPartitionResp)

    // On the callback, we check for unsent items and send another request
    verify(brokerToController).sendRequest(capture.capture(), callbackCapture.capture())
    callbackCapture.getValue.onComplete(resp)

    verify(brokerToController).start()
    verify(brokerToController, times(2)).sendRequest(capture.capture(), callbackCapture.capture())

    // Verify the last request sent had all 10 items
    val request = capture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    assertEquals(request.data().topics().get(0).partitions().size(), 10)
  }

  @Test
  def testSubmitFromCallback(): Unit = {
    // prepare a partition level retriable error response
    val alterPartitionRespWithPartitionError = partitionResponse(tp0, Errors.UNKNOWN_SERVER_ERROR)
    val errorResponse = makeClientResponse(alterPartitionRespWithPartitionError, ApiKeys.ALTER_PARTITION.latestVersion)

    val leaderId = 1
    val leaderEpoch = 1
    val partitionEpoch = 10
    val isr = List(1, 2, 3)
    val leaderAndIsr = new LeaderAndIsr(leaderId, leaderEpoch, isr.map(Int.box).asJava, LeaderRecoveryState.RECOVERED, partitionEpoch)
    val callbackCapture = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => IBP_3_2_IV0)
    alterPartitionManager.start()
    val future = alterPartitionManager.submit(tp0, leaderAndIsr)
    val finalFuture = new CompletableFuture[LeaderAndIsr]()
    future.whenComplete { (_, e) =>
      if (e != null) {
        // Retry when error.
        alterPartitionManager.submit(tp0, leaderAndIsr).whenComplete { (result, e) =>
          if (e != null) {
            finalFuture.completeExceptionally(e)
          } else {
            finalFuture.complete(result)
          }
        }
      } else {
        finalFuture.completeExceptionally(new AssertionError("Expected the future to be failed"))
      }
    }

    verify(brokerToController).start()
    verify(brokerToController).sendRequest(any(), callbackCapture.capture())
    reset(brokerToController)
    callbackCapture.getValue.onComplete(errorResponse)

    // Complete the retry request
    val retryAlterPartitionResponse = partitionResponse(tp0, Errors.NONE, partitionEpoch, leaderId, leaderEpoch, isr)
    val retryResponse = makeClientResponse(retryAlterPartitionResponse, ApiKeys.ALTER_PARTITION.latestVersion)

    verify(brokerToController).sendRequest(any(), callbackCapture.capture())
    callbackCapture.getValue.onComplete(retryResponse)

    assertEquals(leaderAndIsr, finalFuture.get(200, TimeUnit.MILLISECONDS))
    // No more items in unsentIsrUpdates
    assertFalse(alterPartitionManager.unsentIsrUpdates.containsKey(tp0.topicPartition))
  }

  @Test
  def testAuthorizationFailed(): Unit = {
    testRetryOnTopLevelError(Errors.CLUSTER_AUTHORIZATION_FAILED)
  }

  @Test
  def testStaleBrokerEpoch(): Unit = {
    testRetryOnTopLevelError(Errors.STALE_BROKER_EPOCH)
  }

  @Test
  def testUnknownServer(): Unit = {
    testRetryOnTopLevelError(Errors.UNKNOWN_SERVER_ERROR)
  }

  @Test
  def testRetryOnAuthenticationFailure(): Unit = {
    testRetryOnErrorResponse(new ClientResponse(null, null, "", 0L, 0L,
      false, null, new AuthenticationException("authentication failed"), null))
  }

  @Test
  def testRetryOnUnsupportedVersionError(): Unit = {
    testRetryOnErrorResponse(new ClientResponse(null, null, "", 0L, 0L,
      false, new UnsupportedVersionException("unsupported version"), null, null))
  }

  private def testRetryOnTopLevelError(error: Errors): Unit = {
    val alterPartitionResp = new AlterPartitionResponse(new AlterPartitionResponseData().setErrorCode(error.code))
    val response = makeClientResponse(alterPartitionResp, ApiKeys.ALTER_PARTITION.latestVersion)
    testRetryOnErrorResponse(response)
  }

  private def testRetryOnErrorResponse(response: ClientResponse): Unit = {
    val leaderAndIsr = new LeaderAndIsr(1, 1, List(1, 2, 3).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10)
    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => IBP_3_2_IV0)
    alterPartitionManager.start()
    alterPartitionManager.submit(tp0, leaderAndIsr)

    verify(brokerToController).start()
    verify(brokerToController).sendRequest(any(), callbackCapture.capture())
    callbackCapture.getValue.onComplete(response)

    // Any top-level error, we want to retry, so we don't clear items from the pending map
    assertTrue(alterPartitionManager.unsentIsrUpdates.containsKey(tp0.topicPartition))

    reset(brokerToController)

    // After some time, we will retry failed requests
    time.sleep(100)
    scheduler.tick()

    // After a successful response, we can submit another AlterIsrItem
    val retryAlterPartitionResponse = partitionResponse()
    val retryResponse = makeClientResponse(retryAlterPartitionResponse, ApiKeys.ALTER_PARTITION.latestVersion)

    verify(brokerToController).sendRequest(any(), callbackCapture.capture())
    callbackCapture.getValue.onComplete(retryResponse)

    assertFalse(alterPartitionManager.unsentIsrUpdates.containsKey(tp0.topicPartition))
  }

  @Test
  def testInvalidUpdateVersion(): Unit = {
    checkPartitionError(Errors.INVALID_UPDATE_VERSION)
  }

  @Test
  def testUnknownTopicPartition(): Unit = {
    checkPartitionError(Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testNotLeaderOrFollower(): Unit = {
    checkPartitionError(Errors.NOT_LEADER_OR_FOLLOWER)
  }

  @Test
  def testInvalidRequest(): Unit = {
    checkPartitionError(Errors.INVALID_REQUEST)
  }

  private def checkPartitionError(error: Errors): Unit = {
    val alterPartitionManager = testPartitionError(tp0, error)
    // Any partition-level error should clear the item from the pending queue allowing for future updates
    val future = alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List(1, 2, 3).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10))
    assertFalse(future.isDone)
  }

  private def testPartitionError(tp: TopicIdPartition, error: Errors): AlterPartitionManager = {
    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    reset(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => IBP_3_2_IV0)
    alterPartitionManager.start()

    val future = alterPartitionManager.submit(tp, new LeaderAndIsr(1, 1, List(1, 2, 3).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10))

    verify(brokerToController).start()
    verify(brokerToController).sendRequest(any(), callbackCapture.capture())
    reset(brokerToController)

    val alterPartitionResp = partitionResponse(tp, error)
    val resp = makeClientResponse(alterPartitionResp, ApiKeys.ALTER_PARTITION.latestVersion)
    callbackCapture.getValue.onComplete(resp)
    assertTrue(future.isCompletedExceptionally)
    assertFutureThrows(error.exception.getClass, future)
    alterPartitionManager
  }

  @ParameterizedTest
  @MethodSource(Array("provideMetadataVersions"))
  def testOneInFlight(metadataVersion: MetadataVersion): Unit = {
    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val alterPartitionManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => metadataVersion)
    alterPartitionManager.start()

    // First submit will send the request
    alterPartitionManager.submit(tp0, new LeaderAndIsr(1, 1, List(1, 2, 3).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10))

    // These will become pending unsent items
    alterPartitionManager.submit(tp1, new LeaderAndIsr(1, 1, List(1, 2, 3).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10))
    alterPartitionManager.submit(tp2, new LeaderAndIsr(1, 1, List(1, 2, 3).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10))

    verify(brokerToController).start()
    verify(brokerToController).sendRequest(any(), callbackCapture.capture())

    // Once the callback runs, another request will be sent
    reset(brokerToController)

    val alterPartitionResp = new AlterPartitionResponse(new AlterPartitionResponseData())
    val resp = makeClientResponse(alterPartitionResp, ApiKeys.ALTER_PARTITION.latestVersion)
    callbackCapture.getValue.onComplete(resp)
  }

  @ParameterizedTest
  @MethodSource(Array("provideMetadataVersions"))
  def testPartitionMissingInResponse(metadataVersion: MetadataVersion): Unit = {
    val expectedVersion = ApiKeys.ALTER_PARTITION.latestVersion
    val leaderAndIsr = new LeaderAndIsr(1, 1, List(1, 2, 3).map(Int.box).asJava, LeaderRecoveryState.RECOVERED, 10)
    val brokerEpoch = 2
    val scheduler = new MockScheduler(time)
    val brokerToController = Mockito.mock(classOf[NodeToControllerChannelManager])
    val alterPartitionManager = new DefaultAlterPartitionManager(
      brokerToController,
      scheduler,
      time,
      brokerId,
      () => brokerEpoch,
      () => metadataVersion
    )
    alterPartitionManager.start()

    // The first `submit` will send the `AlterIsr` request
    val future1 = alterPartitionManager.submit(tp0, leaderAndIsr)
    val callback1 = verifySendRequest(brokerToController, alterPartitionRequestMatcher(
      expectedTopicPartitions = Set(tp0),
      expectedVersion = expectedVersion
    ))

    // Additional calls while the `AlterIsr` request is inflight will be queued
    val future2 = alterPartitionManager.submit(tp1, leaderAndIsr)
    val future3 = alterPartitionManager.submit(tp2, leaderAndIsr)

    // Respond to the first request, which will also allow the next request to get sent
    callback1.onComplete(makeClientResponse(
      response = partitionResponse(tp0, Errors.UNKNOWN_SERVER_ERROR),
      version = expectedVersion
    ))
    assertFutureThrows(classOf[UnknownServerException], future1)
    assertFalse(future2.isDone)
    assertFalse(future3.isDone)

    // Verify the second request includes both expected partitions, but only respond with one of them
    val callback2 = verifySendRequest(brokerToController, alterPartitionRequestMatcher(
      expectedTopicPartitions = Set(tp1, tp2),
      expectedVersion = expectedVersion
    ))
    callback2.onComplete(makeClientResponse(
      response = partitionResponse(tp2, Errors.UNKNOWN_SERVER_ERROR),
      version = expectedVersion
    ))
    assertFutureThrows(classOf[UnknownServerException], future3)
    assertFalse(future2.isDone)

    // The missing partition should be retried
    val callback3 = verifySendRequest(brokerToController, alterPartitionRequestMatcher(
      expectedTopicPartitions = Set(tp1),
      expectedVersion = expectedVersion
    ))
    callback3.onComplete(makeClientResponse(
      response = partitionResponse(tp1, Errors.UNKNOWN_SERVER_ERROR),
      version = expectedVersion
    ))
    assertFutureThrows(classOf[UnknownServerException], future2)
  }

  private def verifySendRequest(
    brokerToController: NodeToControllerChannelManager,
    expectedRequest: ArgumentMatcher[AbstractRequest.Builder[_ <: AbstractRequest]]
  ): ControllerRequestCompletionHandler = {
    val callbackCapture = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    Mockito.verify(brokerToController).sendRequest(
      ArgumentMatchers.argThat(expectedRequest),
      callbackCapture.capture()
    )

    Mockito.reset(brokerToController)

    callbackCapture.getValue
  }

  private def alterPartitionRequestMatcher(
    expectedTopicPartitions: Set[TopicIdPartition],
    expectedVersion: Short
  ): ArgumentMatcher[AbstractRequest.Builder[_ <: AbstractRequest]] = {
    request => {
      assertEquals(ApiKeys.ALTER_PARTITION, request.apiKey)

      val alterPartitionRequest = request.asInstanceOf[AlterPartitionRequest.Builder].build()
      assertEquals(expectedVersion, alterPartitionRequest.version)

      val requestTopicPartitions = alterPartitionRequest.data.topics.asScala.flatMap { topicData =>
        topicData.partitions.asScala.map { partitionData =>
          new TopicIdPartition(topicData.topicId, partitionData.partitionIndex, topicData.topicName)
        }
      }.toSet

      expectedTopicPartitions == requestTopicPartitions
    }
  }

  private def makeClientResponse(
    response: AlterPartitionResponse,
    version: Short
  ): ClientResponse = {
    new ClientResponse(
      new RequestHeader(response.apiKey, version, "", 0),
      null,
      "",
      0L,
      0L,
      false,
      null,
      null,
      // Response is serialized and deserialized to ensure that its does
      // not contain ignorable fields used by other versions.
      AlterPartitionResponse.parse(MessageUtil.toByteBuffer(response.data, version), version)
    )
  }

  private def partitionResponse(
    tp: TopicIdPartition = tp0,
    error: Errors = Errors.NONE,
    partitionEpoch: Int = 0,
    leaderId: Int = 0,
    leaderEpoch: Int = 0,
    isr: List[Int] = List.empty
  ): AlterPartitionResponse = {
    new AlterPartitionResponse(new AlterPartitionResponseData()
      .setTopics(Collections.singletonList(
        new AlterPartitionResponseData.TopicData()
          .setTopicName(tp.topic)
          .setTopicId(tp.topicId)
          .setPartitions(Collections.singletonList(
            new AlterPartitionResponseData.PartitionData()
              .setPartitionIndex(tp.partition)
              .setPartitionEpoch(partitionEpoch)
              .setLeaderEpoch(leaderEpoch)
              .setLeaderId(leaderId)
              .setIsr(isr.map(Integer.valueOf).asJava)
              .setErrorCode(error.code))))))
  }
}

object AlterPartitionManagerTest {
  def provideMetadataVersions(): JStream[MetadataVersion] = {
    JStream.of(
      // Supports KIP-903: include broker epoch in AlterPartition request
      IBP_3_5_IV1,
      // Supports KIP-704: unclean leader recovery
      IBP_3_2_IV0,
      // Supports KIP-497: alter partition
      IBP_3_0_IV1
    )
  }

  def provideLeaderRecoveryState(): JStream[Arguments] = {
    // Multiply metadataVersions by leaderRecoveryState
    provideMetadataVersions().flatMap { metadataVersion =>
      JStream.of(
        Arguments.of(metadataVersion, LeaderRecoveryState.RECOVERED),
        Arguments.of(metadataVersion, LeaderRecoveryState.RECOVERING)
      )
    }
  }
}
