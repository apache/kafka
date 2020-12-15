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
import java.util.concurrent.atomic.AtomicInteger
import kafka.api.LeaderAndIsr
import kafka.utils.{MockScheduler, MockTime}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.AlterIsrResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AlterIsrRequest, AlterIsrResponse}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{Before, Test}
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

  @Before
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
    alterIsrManager.submit(AlterIsrItem(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}, 0))
    time.sleep(50)
    scheduler.tick()

    EasyMock.verify(brokerToController)
  }

  @Test
  def testOverwriteWithinBatch(): Unit = {
    val capture = EasyMock.newCapture[AbstractRequest.Builder[AlterIsrRequest]]()
    EasyMock.expect(brokerToController.start())
    EasyMock.expect(brokerToController.sendRequest(EasyMock.capture(capture), EasyMock.anyObject())).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterIsrManager(brokerToController, scheduler, time, brokerId, () => 2)
    alterIsrManager.start()

    // Only send one ISR update for a given topic+partition
    assertTrue(alterIsrManager.submit(AlterIsrItem(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}, 0)))
    assertFalse(alterIsrManager.submit(AlterIsrItem(tp0, new LeaderAndIsr(1, 1, List(1,2), 10), _ => {}, 0)))

    time.sleep(50)
    scheduler.tick()

    EasyMock.verify(brokerToController)

    val request = capture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    assertEquals(request.data().topics().get(0).partitions().get(0).newIsr().size(), 3)
  }

  @Test
  def testSingleBatch(): Unit = {
    val capture = EasyMock.newCapture[AbstractRequest.Builder[AlterIsrRequest]]()
    EasyMock.expect(brokerToController.start())
    EasyMock.expect(brokerToController.sendRequest(EasyMock.capture(capture), EasyMock.anyObject())).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterIsrManager(brokerToController, scheduler, time, brokerId, () => 2)
    alterIsrManager.start()

    for (i <- 0 to 9) {
      alterIsrManager.submit(AlterIsrItem(new TopicPartition(topic, i),
        new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}, 0))
      time.sleep(1)
    }

    time.sleep(50)
    scheduler.tick()

    // This should not be included in the batch
    alterIsrManager.submit(AlterIsrItem(new TopicPartition(topic, 10),
      new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}, 0))

    EasyMock.verify(brokerToController)

    val request = capture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    assertEquals(request.data().topics().get(0).partitions().size(), 10)
  }

  @Test
  def testAuthorizationFailed(): Unit = {
    val isrs = Seq(AlterIsrItem(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => { }, 0))
    val manager = testTopLevelError(isrs, Errors.CLUSTER_AUTHORIZATION_FAILED)
    // On authz error, we log the exception and keep retrying
    assertFalse(manager.submit(AlterIsrItem(tp0, null, _ => { }, 0)))
  }

  @Test
  def testStaleBrokerEpoch(): Unit = {
    val isrs = Seq(AlterIsrItem(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => { }, 0))
    val manager = testTopLevelError(isrs, Errors.STALE_BROKER_EPOCH)
    // On stale broker epoch, we want to retry, so we don't clear items from the pending map
    assertFalse(manager.submit(AlterIsrItem(tp0, null, _ => { }, 0)))
  }

  @Test
  def testOtherErrors(): Unit = {
    val isrs = Seq(AlterIsrItem(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => { }, 0))
    val manager = testTopLevelError(isrs, Errors.UNKNOWN_SERVER_ERROR)
    // On other unexpected errors, we also want to retry
    assertFalse(manager.submit(AlterIsrItem(tp0, null, _ => { }, 0)))
  }

  def testTopLevelError(isrs: Seq[AlterIsrItem], error: Errors): AlterIsrManager = {
    val callbackCapture = EasyMock.newCapture[ControllerRequestCompletionHandler]()

    EasyMock.expect(brokerToController.start())
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.capture(callbackCapture))).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterIsrManager(brokerToController, scheduler, time, brokerId, () => 2)
    alterIsrManager.start()
    isrs.foreach(alterIsrManager.submit)

    time.sleep(100)
    scheduler.tick()

    EasyMock.verify(brokerToController)

    val alterIsrResp = new AlterIsrResponse(new AlterIsrResponseData().setErrorCode(error.code))
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterIsrResp)
    callbackCapture.getValue.onComplete(resp)
    alterIsrManager
  }

  @Test
  def testPartitionErrors(): Unit = {
    val errors = Seq(Errors.INVALID_UPDATE_VERSION, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NOT_LEADER_OR_FOLLOWER)
    errors.foreach(error => {
      val alterIsrManager = testPartitionError(tp0, error)
      // Any partition-level error should clear the item from the pending queue allowing for future updates
      assertTrue(alterIsrManager.submit(AlterIsrItem(tp0, null, _ => { }, 0)))
    })
  }

  def testPartitionError(tp: TopicPartition, error: Errors): AlterIsrManager = {
    val callbackCapture = EasyMock.newCapture[ControllerRequestCompletionHandler]()
    EasyMock.reset(brokerToController)
    EasyMock.expect(brokerToController.start())
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.capture(callbackCapture))).once()
    EasyMock.replay(brokerToController)

    val scheduler = new MockScheduler(time)
    val alterIsrManager = new DefaultAlterIsrManager(brokerToController, scheduler, time, brokerId, () => 2)
    alterIsrManager.start()

    var capturedError: Option[Errors] = None
    val callback = (result:  Either[Errors, LeaderAndIsr]) => {
      result match {
        case Left(error: Errors) => capturedError = Some(error)
        case Right(_) => fail("Should have seen error")
      }
    }

    alterIsrManager.submit(AlterIsrItem(tp, new LeaderAndIsr(1, 1, List(1,2,3), 10), callback, 0))

    time.sleep(100)
    scheduler.tick()

    EasyMock.verify(brokerToController)

    val alterIsrResp = new AlterIsrResponse(new AlterIsrResponseData()
      .setTopics(Collections.singletonList(
        new AlterIsrResponseData.TopicData()
          .setName(tp.topic())
          .setPartitions(Collections.singletonList(
            new AlterIsrResponseData.PartitionData()
              .setPartitionIndex(tp.partition())
              .setErrorCode(error.code))))))
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterIsrResp)
    callbackCapture.getValue.onComplete(resp)
    assertTrue(capturedError.isDefined)
    assertEquals(capturedError.get, error)
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
    alterIsrManager.submit(AlterIsrItem(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}, 0))

    time.sleep(100)
    scheduler.tick() // Triggers a request

    // Enqueue more updates
    alterIsrManager.submit(AlterIsrItem(tp1, new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}, 0))
    alterIsrManager.submit(AlterIsrItem(tp2, new LeaderAndIsr(1, 1, List(1,2,3), 10), _ => {}, 0))

    time.sleep(100)
    scheduler.tick() // Trigger the schedule again, but no request this time

    EasyMock.verify(brokerToController)

    // Even an empty response will clear the in-flight
    val alterIsrResp = new AlterIsrResponse(new AlterIsrResponseData())
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterIsrResp)
    callbackCapture.getValue.onComplete(resp)

    EasyMock.reset(brokerToController)
    EasyMock.expect(brokerToController.sendRequest(EasyMock.anyObject(), EasyMock.capture(callbackCapture))).once()
    EasyMock.replay(brokerToController)

    time.sleep(100)
    scheduler.tick()
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

    val count = new AtomicInteger(0)
    val callback = (result:  Either[Errors, LeaderAndIsr]) => {
      count.incrementAndGet()
      return
    }
    alterIsrManager.submit(AlterIsrItem(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 10), callback, 0))
    alterIsrManager.submit(AlterIsrItem(tp1, new LeaderAndIsr(1, 1, List(1,2,3), 10), callback, 0))
    alterIsrManager.submit(AlterIsrItem(tp2, new LeaderAndIsr(1, 1, List(1,2,3), 10), callback, 0))


    time.sleep(100)
    scheduler.tick()

    EasyMock.verify(brokerToController)

    // Three partitions were sent, but only one returned
    val alterIsrResp = new AlterIsrResponse(new AlterIsrResponseData()
      .setTopics(Collections.singletonList(
        new AlterIsrResponseData.TopicData()
          .setName(tp0.topic())
          .setPartitions(Collections.singletonList(
            new AlterIsrResponseData.PartitionData()
              .setPartitionIndex(tp0.partition())
              .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()))))))
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterIsrResp)
    callbackCapture.getValue.onComplete(resp)

    assertEquals("Expected all callbacks to run", count.get, 3)
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

    def expectMatch(expect: Either[Errors, LeaderAndIsr])(result: Either[Errors, LeaderAndIsr]): Unit = {
      assertEquals(expect, result)
    }

    // Correct ZK version
    assertTrue(zkIsrManager.submit(AlterIsrItem(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 1),
      expectMatch(Right(new LeaderAndIsr(1, 1, List(1,2,3), 2))), 0)))

    // Wrong ZK version
    assertTrue(zkIsrManager.submit(AlterIsrItem(tp0, new LeaderAndIsr(1, 1, List(1,2,3), 3),
      expectMatch(Left(Errors.INVALID_UPDATE_VERSION)), 0)))
  }
}
