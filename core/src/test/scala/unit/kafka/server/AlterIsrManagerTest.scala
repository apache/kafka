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

import kafka.api.LeaderAndIsr
import kafka.utils.{MockScheduler, MockTime}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{AuthenticationException, InvalidUpdateVersionException, OperationNotAttemptedException, UnknownServerException, UnsupportedVersionException}
import org.apache.kafka.common.message.AlterIsrResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AlterIsrRequest, AlterIsrResponse}
import org.apache.kafka.test.TestUtils
import org.easymock.EasyMock
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.{ArgumentMatchers, Mockito}

class AlterIsrManagerTest {

  val topic = "test-topic"
  val time = new MockTime
  val metrics = new Metrics
  val brokerId = 1

  var brokerToController: BrokerToControllerChannelManager = _

  val tp0 = new TopicPartition(topic, 0)
  val tp1 = new TopicPartition(topic, 1)
  val tp2 = new TopicPartition(topic, 2)

  @BeforeEach
  def setup(): Unit = {
    brokerToController = EasyMock.createMock(classOf[BrokerToControllerChannelManager])
  }

  @Test
  def testBasic(): Unit = {
    EasyMock.expect(brokerToController.start())
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.anyObject())).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterIsrManager(brokerToController, scheduler, time, brokerId, () => 2)
    alterIsrManager.start()
    alterIsrManager.submit(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)
    EasyMock.verify(brokerToController)
  }

  @Test
  def testOverwriteWithinBatch(): Unit = {
    val capture = EasyMock.newCapture[AbstractRequest.Builder[AlterIsrRequest]]()
    val callbackCapture = EasyMock.newCapture[ControllerRequestCompletionHandler]()

    EasyMock.expect(brokerToController.start())
    EasyMock.expect(brokerToController.sendRequest(EasyMock.capture(capture), EasyMock.capture(callbackCapture))).times(2)
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterIsrManager(brokerToController, scheduler, time, brokerId, () => 2)
    alterIsrManager.start()

    // Only send one ISR update for a given topic+partition
    val firstSubmitFuture = alterIsrManager.submit(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)
    assertFalse(firstSubmitFuture.isDone)

    val failedSubmitFuture = alterIsrManager.submit(tp0, new LeaderAndIsr(1, 1, List(1,2), 10), 0)
    assertTrue(failedSubmitFuture.isCompletedExceptionally)
    TestUtils.assertFutureThrows(failedSubmitFuture, classOf[OperationNotAttemptedException])

    // Simulate response
    val alterIsrResp = partitionResponse(tp0, Errors.NONE)
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterIsrResp)
    callbackCapture.getValue.onComplete(resp)

    // Now we can submit this partition again
    val newSubmitFuture = alterIsrManager.submit(tp0, new LeaderAndIsr(1, 1, List(1), 10), 0)
    assertFalse(newSubmitFuture.isDone)

    EasyMock.verify(brokerToController)

    // Make sure we sent the right request ISR={1}
    val request = capture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    assertEquals(request.data().topics().get(0).partitions().get(0).newIsr().size(), 1)
  }

  @Test
  def testSingleBatch(): Unit = {
    val capture = EasyMock.newCapture[AbstractRequest.Builder[AlterIsrRequest]]()
    val callbackCapture = EasyMock.newCapture[ControllerRequestCompletionHandler]()

    EasyMock.expect(brokerToController.start())
    EasyMock.expect(brokerToController.sendRequest(EasyMock.capture(capture), EasyMock.capture(callbackCapture))).times(2)
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterIsrManager(brokerToController, scheduler, time, brokerId, () => 2)
    alterIsrManager.start()

    // First request will send batch of one
    alterIsrManager.submit(new TopicPartition(topic, 0),
      new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)

    // Other submissions will queue up until a response
    for (i <- 1 to 9) {
      alterIsrManager.submit(new TopicPartition(topic, i),
        new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)
    }

    // Simulate response, omitting partition 0 will allow it to stay in unsent queue
    val alterIsrResp = new AlterIsrResponse(new AlterIsrResponseData())
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterIsrResp)

    // On the callback, we check for unsent items and send another request
    callbackCapture.getValue.onComplete(resp)

    EasyMock.verify(brokerToController)

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
    val alterIsrResp = new AlterIsrResponse(new AlterIsrResponseData().setErrorCode(error.code))
    val response = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterIsrResp)
    testRetryOnErrorResponse(response)
  }

  private def testRetryOnErrorResponse(response: ClientResponse): Unit = {
    val leaderAndIsr = new LeaderAndIsr(1, 1, List(1,2,3), 10)
    val callbackCapture = EasyMock.newCapture[ControllerRequestCompletionHandler]()

    EasyMock.expect(brokerToController.start())
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.capture(callbackCapture))).times(1)
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterIsrManager(brokerToController, scheduler, time, brokerId, () => 2)
    alterIsrManager.start()
    alterIsrManager.submit(tp0, leaderAndIsr, 0)

    EasyMock.verify(brokerToController)

    callbackCapture.getValue.onComplete(response)

    // Any top-level error, we want to retry, so we don't clear items from the pending map
    assertTrue(alterIsrManager.unsentIsrUpdates.containsKey(tp0))

    EasyMock.reset(brokerToController)
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.capture(callbackCapture))).times(1)
    EasyMock.replay(brokerToController)

    // After some time, we will retry failed requests
    time.sleep(100)
    scheduler.tick()

    // After a successful response, we can submit another AlterIsrItem
    val retryAlterIsrResponse = partitionResponse(tp0, Errors.NONE)
    val retryResponse = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, retryAlterIsrResponse)
    callbackCapture.getValue.onComplete(retryResponse)

    EasyMock.verify(brokerToController)

    assertFalse(alterIsrManager.unsentIsrUpdates.containsKey(tp0))
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

  private def checkPartitionError(error: Errors): Unit = {
    val alterIsrManager = testPartitionError(tp0, error)
    // Any partition-level error should clear the item from the pending queue allowing for future updates
    val future = alterIsrManager.submit(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)
    assertFalse(future.isDone)
  }

  private def testPartitionError(tp: TopicPartition, error: Errors): AlterIsrManager = {
    val callbackCapture = EasyMock.newCapture[ControllerRequestCompletionHandler]()
    EasyMock.reset(brokerToController)
    EasyMock.expect(brokerToController.start())
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.capture(callbackCapture))).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterIsrManager(brokerToController, scheduler, time, brokerId, () => 2)
    alterIsrManager.start()

    val future = alterIsrManager.submit(tp, new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)

    EasyMock.verify(brokerToController)
    EasyMock.reset(brokerToController)

    val alterIsrResp = partitionResponse(tp, error)
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterIsrResp)
    callbackCapture.getValue.onComplete(resp)
    assertTrue(future.isCompletedExceptionally)
    TestUtils.assertFutureThrows(future, error.exception.getClass)
    alterIsrManager
  }

  @Test
  def testOneInFlight(): Unit = {
    val callbackCapture = EasyMock.newCapture[ControllerRequestCompletionHandler]()
    EasyMock.reset(brokerToController)
    EasyMock.expect(brokerToController.start())
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.capture(callbackCapture))).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterIsrManager(brokerToController, scheduler, time, brokerId, () => 2)
    alterIsrManager.start()

    // First submit will send the request
    alterIsrManager.submit(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)

    // These will become pending unsent items
    alterIsrManager.submit(tp1, new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)
    alterIsrManager.submit(tp2, new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)

    EasyMock.verify(brokerToController)

    // Once the callback runs, another request will be sent
    EasyMock.reset(brokerToController)
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.capture(callbackCapture))).once()
    EasyMock.replay(brokerToController)
    val alterIsrResp = new AlterIsrResponse(new AlterIsrResponseData())
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterIsrResp)
    callbackCapture.getValue.onComplete(resp)
    EasyMock.verify(brokerToController)
  }

  @Test
  def testPartitionMissingInResponse(): Unit = {
    val callbackCapture = EasyMock.newCapture[ControllerRequestCompletionHandler]()
    EasyMock.reset(brokerToController)
    EasyMock.expect(brokerToController.start())
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.capture(callbackCapture))).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterIsrManager(brokerToController, scheduler, time, brokerId, () => 2)
    alterIsrManager.start()

    // FIXME: This test is broken. The first `submit` sends the request

    val future1 = alterIsrManager.submit(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)
    val future2 = alterIsrManager.submit(tp1, new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)
    val future3 = alterIsrManager.submit(tp2, new LeaderAndIsr(1, 1, List(1,2,3), 10), 0)

    // Three partitions were sent, but only one returned
    val alterIsrResp = partitionResponse(tp0, Errors.UNKNOWN_SERVER_ERROR)
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterIsrResp)
    callbackCapture.getValue.onComplete(resp)

    Seq(future1, future2, future3).foreach { future =>
      assertTrue(future1.isCompletedExceptionally)
      assertThrows(classOf[UnknownServerException], () => future.get())
    }

    EasyMock.verify(brokerToController)
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

    val zkIsrManager = new ZkIsrManager(scheduler, time, kafkaZkClient)
    zkIsrManager.start()

    // Correct ZK version
    val future1 = zkIsrManager.submit(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 1), 0)
    assertTrue(future1.isDone)
    assertEquals(new LeaderAndIsr(1, 1, List(1,2,3), 2), future1.get)

    // Wrong ZK version
    val future2 = zkIsrManager.submit(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 3), 0)
    assertTrue(future2.isCompletedExceptionally)
    TestUtils.assertFutureThrows(future2, classOf[InvalidUpdateVersionException])
  }

  private def partitionResponse(tp: TopicPartition, error: Errors): AlterIsrResponse = {
    new AlterIsrResponse(new AlterIsrResponseData()
      .setTopics(Collections.singletonList(
        new AlterIsrResponseData.TopicData()
          .setName(tp.topic())
          .setPartitions(Collections.singletonList(
            new AlterIsrResponseData.PartitionData()
              .setPartitionIndex(tp.partition())
              .setErrorCode(error.code))))))
  }
}
