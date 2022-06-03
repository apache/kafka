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
import kafka.api.LeaderAndIsr
import kafka.utils.{MockScheduler, MockTime}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.{AuthenticationException, InvalidUpdateVersionException, OperationNotAttemptedException, UnknownServerException, UnsupportedVersionException}
import org.apache.kafka.common.message.AlterPartitionResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.requests.{AbstractRequest, AlterPartitionRequest, AlterPartitionResponse}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.{IBP_2_7_IV2, IBP_3_2_IV0}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mock, reset, times, verify}
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

import scala.jdk.CollectionConverters._

class AlterPartitionManagerTest {

  val topic = "test-topic"
  val topicId = Uuid.randomUuid()
  val time = new MockTime
  val metrics = new Metrics
  val brokerId = 1

  var brokerToController: BrokerToControllerChannelManager = _

  val tp0 = new TopicIdPartition(topicId, 0, topic)
  val tp1 = new TopicIdPartition(topicId, 1, topic)
  val tp2 = new TopicIdPartition(topicId, 2, topic)

  @BeforeEach
  def setup(): Unit = {
    brokerToController = mock(classOf[BrokerToControllerChannelManager])
  }

  @ParameterizedTest
  @MethodSource(Array("provideMetadataVersions"))
  def testBasic(metadataVersion: MetadataVersion): Unit = {
    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => metadataVersion)
    alterIsrManager.start()
    alterIsrManager.submit(tp0, new LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)
    verify(brokerToController).start()
    verify(brokerToController).sendRequest(any(), any())
  }

  @ParameterizedTest
  @MethodSource(Array("provideLeaderRecoveryState"))
  def testBasicSentLeaderRecoveryState(
    metadataVersion: MetadataVersion,
    leaderRecoveryState: LeaderRecoveryState
  ): Unit = {
    val requestCapture = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[AlterPartitionRequest]])

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => metadataVersion)
    alterIsrManager.start()
    alterIsrManager.submit(tp0, new LeaderAndIsr(1, 1, List(1), leaderRecoveryState, 10), 0)
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
    val alterIsrManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => metadataVersion)
    alterIsrManager.start()

    // Only send one ISR update for a given topic+partition
    val firstSubmitFuture = alterIsrManager.submit(tp0, LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)
    assertFalse(firstSubmitFuture.isDone)

    val failedSubmitFuture = alterIsrManager.submit(tp0, LeaderAndIsr(1, 1, List(1,2), LeaderRecoveryState.RECOVERED, 10), 0)
    assertTrue(failedSubmitFuture.isCompletedExceptionally)
    assertFutureThrows(failedSubmitFuture, classOf[OperationNotAttemptedException])

    // Simulate response
    val alterPartitionResp = partitionResponse(tp0, Errors.NONE)
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterPartitionResp)
    verify(brokerToController).sendRequest(capture.capture(), callbackCapture.capture())
    callbackCapture.getValue.onComplete(resp)

    // Now we can submit this partition again
    val newSubmitFuture = alterIsrManager.submit(tp0, LeaderAndIsr(1, 1, List(1), LeaderRecoveryState.RECOVERED, 10), 0)
    assertFalse(newSubmitFuture.isDone)

    verify(brokerToController).start()
    verify(brokerToController, times(2)).sendRequest(capture.capture(), callbackCapture.capture())

    // Make sure we sent the right request ISR={1}
    val request = capture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    assertEquals(request.data().topics().get(0).partitions().get(0).newIsr().size(), 1)
  }

  @ParameterizedTest
  @MethodSource(Array("provideMetadataVersions"))
  def testSingleBatch(metadataVersion: MetadataVersion): Unit = {
    val capture: ArgumentCaptor[AbstractRequest.Builder[AlterPartitionRequest]] = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[AlterPartitionRequest]])
    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => metadataVersion)
    alterIsrManager.start()

    // First request will send batch of one
    alterIsrManager.submit(new TopicIdPartition(topicId, 0, topic),
      LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)

    // Other submissions will queue up until a response
    for (i <- 1 to 9) {
      alterIsrManager.submit(new TopicIdPartition(topicId, i, topic),
        LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)
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
    val response = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterPartitionResp)
    testRetryOnErrorResponse(response)
  }

  private def testRetryOnErrorResponse(response: ClientResponse): Unit = {
    val leaderAndIsr = new LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10)
    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => IBP_3_2_IV0)
    alterIsrManager.start()
    alterIsrManager.submit(tp0, leaderAndIsr, 0)

    verify(brokerToController).start()
    verify(brokerToController).sendRequest(any(), callbackCapture.capture())
    callbackCapture.getValue.onComplete(response)

    // Any top-level error, we want to retry, so we don't clear items from the pending map
    assertTrue(alterIsrManager.unsentIsrUpdates.containsKey(tp0.topicPartition))

    reset(brokerToController)

    // After some time, we will retry failed requests
    time.sleep(100)
    scheduler.tick()

    // After a successful response, we can submit another AlterIsrItem
    val retryAlterPartitionResponse = partitionResponse(tp0, Errors.NONE)
    val retryResponse = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, retryAlterPartitionResponse)

    verify(brokerToController).sendRequest(any(), callbackCapture.capture())
    callbackCapture.getValue.onComplete(retryResponse)

    assertFalse(alterIsrManager.unsentIsrUpdates.containsKey(tp0.topicPartition))
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
    val alterIsrManager = testPartitionError(tp0, error)
    // Any partition-level error should clear the item from the pending queue allowing for future updates
    val future = alterIsrManager.submit(tp0, LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)
    assertFalse(future.isDone)
  }

  private def testPartitionError(tp: TopicIdPartition, error: Errors): AlterPartitionManager = {
    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    reset(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => IBP_3_2_IV0)
    alterIsrManager.start()

    val future = alterIsrManager.submit(tp, LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)

    verify(brokerToController).start()
    verify(brokerToController).sendRequest(any(), callbackCapture.capture())
    reset(brokerToController)

    val alterPartitionResp = partitionResponse(tp, error)
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterPartitionResp)
    callbackCapture.getValue.onComplete(resp)
    assertTrue(future.isCompletedExceptionally)
    assertFutureThrows(future, error.exception.getClass)
    alterIsrManager
  }

  @ParameterizedTest
  @MethodSource(Array("provideMetadataVersions"))
  def testOneInFlight(metadataVersion: MetadataVersion): Unit = {
    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => 2, () => metadataVersion)
    alterIsrManager.start()

    // First submit will send the request
    alterIsrManager.submit(tp0, LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)

    // These will become pending unsent items
    alterIsrManager.submit(tp1, LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)
    alterIsrManager.submit(tp2, LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)

    verify(brokerToController).start()
    verify(brokerToController).sendRequest(any(), callbackCapture.capture())

    // Once the callback runs, another request will be sent
    reset(brokerToController)

    val alterPartitionResp = new AlterPartitionResponse(new AlterPartitionResponseData())
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterPartitionResp)
    callbackCapture.getValue.onComplete(resp)
  }

  @ParameterizedTest
  @MethodSource(Array("provideMetadataVersions"))
  def testPartitionMissingInResponse(metadataVersion: MetadataVersion): Unit = {
    brokerToController = Mockito.mock(classOf[BrokerToControllerChannelManager])

    val brokerEpoch = 2
    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterPartitionManager(brokerToController, scheduler, time, brokerId, () => brokerEpoch, () => metadataVersion)
    alterIsrManager.start()

    def verifySendAlterIsr(topicPartitions: Set[TopicIdPartition]): ControllerRequestCompletionHandler = {
      val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] =
        ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
      Mockito.verify(brokerToController).sendRequest(
        ArgumentMatchers.argThat(alterPartitionRequestMatcher(topicPartitions)),
        callbackCapture.capture()
      )
      Mockito.reset(brokerToController)
      callbackCapture.getValue
    }

    def clientResponse(topicPartition: TopicIdPartition, error: Errors): ClientResponse = {
      val alterIsrResponse = partitionResponse(topicPartition, error)
      new ClientResponse(null, null, "", 0L, 0L,
        false, null, null, alterIsrResponse)
    }

    // The first `submit` will send the `AlterIsr` request
    val future1 = alterIsrManager.submit(tp0, LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)
    val callback1 = verifySendAlterIsr(Set(tp0))

    // Additional calls while the `AlterIsr` request is inflight will be queued
    val future2 = alterIsrManager.submit(tp1, LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)
    val future3 = alterIsrManager.submit(tp2, LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10), 0)

    // Respond to the first request, which will also allow the next request to get sent
    callback1.onComplete(clientResponse(tp0, Errors.UNKNOWN_SERVER_ERROR))
    assertFutureThrows(future1, classOf[UnknownServerException])
    assertFalse(future2.isDone)
    assertFalse(future3.isDone)

    // Verify the second request includes both expected partitions, but only respond with one of them
    val callback2 = verifySendAlterIsr(Set(tp1, tp2))
    callback2.onComplete(clientResponse(tp2, Errors.UNKNOWN_SERVER_ERROR))
    assertFutureThrows(future3, classOf[UnknownServerException])
    assertFalse(future2.isDone)

    // The missing partition should be retried
    val callback3 = verifySendAlterIsr(Set(tp1))
    callback3.onComplete(clientResponse(tp1, Errors.UNKNOWN_SERVER_ERROR))
    assertFutureThrows(future2, classOf[UnknownServerException])
  }

  @ParameterizedTest
  @MethodSource(Array("provideMetadataVersions"))
  def testPartialTopicIds(metadataVersion: MetadataVersion): Unit = {
    val canUseTopicIds = metadataVersion.isAtLeast(MetadataVersion.IBP_2_8_IV0)

    val foo = new TopicIdPartition(Uuid.ZERO_UUID, 0, "foo")
    val bar = new TopicIdPartition(Uuid.randomUuid(), 0, "bar")
    val zar = new TopicIdPartition(Uuid.randomUuid(), 0, "zar")

    val leaderAndIsr = LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 10)
    val controlledEpoch = 0
    val brokerEpoch = 2
    val scheduler = new MockScheduler(time)
    val brokerToController = Mockito.mock(classOf[BrokerToControllerChannelManager])
    val alterPartitionManager = new DefaultAlterPartitionManager(
      brokerToController,
      scheduler,
      time,
      brokerId,
      () => brokerEpoch,
      () => metadataVersion
    )
    alterPartitionManager.start()

    def verifySendAlterIsr(
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

    // Submits an alter isr update with zar, which has a topic id.
    val future1 = alterPartitionManager.submit(zar, leaderAndIsr, controlledEpoch)

    // The latest version is expected if IBP >= 2.8 is used and all the submitted partitions
    // have topic ids; version 1 should be used otherwise.
    val callback1 = verifySendAlterIsr(alterPartitionRequestMatcher(
      expectedTopicPartitions = Set(zar),
      expectedVersion = if (canUseTopicIds) ApiKeys.ALTER_PARTITION.latestVersion else 1
    ))

    // Submits two additional alter isr changes with foo and bar while the previous one
    // is still inflight. foo has no topic id, bar has one.
    val future2 = alterPartitionManager.submit(foo, leaderAndIsr, controlledEpoch)
    val future3 = alterPartitionManager.submit(bar, leaderAndIsr, controlledEpoch)

    // Completes the first request. That triggers the next one.
    callback1.onComplete(makeClientResponse(makeAlterPartition(Seq(
      makeAlterPartitionTopicData(zar, Errors.NONE),
    ))))

    assertTrue(future1.isDone)
    assertFalse(future2.isDone)
    assertFalse(future3.isDone)

    // Version 1 is expected because foo does not have a topic id.
    val callback2 = verifySendAlterIsr(alterPartitionRequestMatcher(
      expectedTopicPartitions = Set(foo, bar),
      expectedVersion = 1
    ))

    // Completes the second request.
    callback2.onComplete(makeClientResponse(makeAlterPartition(Seq(
      makeAlterPartitionTopicData(foo, Errors.NONE),
      makeAlterPartitionTopicData(bar, Errors.NONE),
    ))))

    assertTrue(future1.isDone)
    assertTrue(future2.isDone)
    assertTrue(future3.isDone)
  }

  private def alterPartitionRequestMatcher(
    expectedTopicPartitions: Set[TopicIdPartition],
    expectedVersion: Short = ApiKeys.ALTER_PARTITION.latestVersion
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

  private def makeClientResponse(response: AbstractResponse): ClientResponse = {
    new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, response)
  }

  private def makeAlterPartition(
    topics: Seq[AlterPartitionResponseData.TopicData]
  ): AlterPartitionResponse = {
    new AlterPartitionResponse(new AlterPartitionResponseData().setTopics(topics.asJava))
  }

  private def makeAlterPartitionTopicData(
    topicIdPartition: TopicIdPartition,
    error: Errors
  ): AlterPartitionResponseData.TopicData = {
    new AlterPartitionResponseData.TopicData()
      .setTopicName(topicIdPartition.topic)
      .setTopicId(topicIdPartition.topicId)
      .setPartitions(Collections.singletonList(
        new AlterPartitionResponseData.PartitionData()
          .setPartitionIndex(topicIdPartition.partition)
          .setErrorCode(error.code)))
  }

  @Test
  def testZkBasic(): Unit = {
    val scheduler = new MockScheduler(time)
    scheduler.startup()

    val kafkaZkClient = Mockito.mock(classOf[KafkaZkClient])
    Mockito.doAnswer(_ => (true, 2))
      .when(kafkaZkClient)
      .conditionalUpdatePath(anyString(), any(), ArgumentMatchers.eq(1), any())
    Mockito.doAnswer(_ => (false, 2))
      .when(kafkaZkClient)
      .conditionalUpdatePath(anyString(), any(), ArgumentMatchers.eq(3), any())

    val zkIsrManager = new ZkAlterPartitionManager(scheduler, time, kafkaZkClient)
    zkIsrManager.start()

    // Correct ZK version
    val future1 = zkIsrManager.submit(tp0, LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 1), 0)
    assertTrue(future1.isDone)
    assertEquals(LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 2), future1.get)

    // Wrong ZK version
    val future2 = zkIsrManager.submit(tp0, LeaderAndIsr(1, 1, List(1,2,3), LeaderRecoveryState.RECOVERED, 3), 0)
    assertTrue(future2.isCompletedExceptionally)
    assertFutureThrows(future2, classOf[InvalidUpdateVersionException])
  }

  private def partitionResponse(tp: TopicIdPartition, error: Errors): AlterPartitionResponse = {
    new AlterPartitionResponse(new AlterPartitionResponseData()
      .setTopics(Collections.singletonList(
        new AlterPartitionResponseData.TopicData()
          .setTopicName(tp.topic)
          .setTopicId(tp.topicId)
          .setPartitions(Collections.singletonList(
            new AlterPartitionResponseData.PartitionData()
              .setPartitionIndex(tp.partition())
              .setErrorCode(error.code))))))
  }
}

object AlterPartitionManagerTest {
  def provideMetadataVersions(): JStream[MetadataVersion] = {
    JStream.of(
      // Supports KIP-704: unclean leader recovery
      IBP_3_2_IV0,
      // Supports KIP-497: alter partition
      IBP_2_7_IV2
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
