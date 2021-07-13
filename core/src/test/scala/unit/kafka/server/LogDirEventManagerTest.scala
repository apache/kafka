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
package unit.kafka.server

import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kafka.server.{AlterReplicaStateItem, BrokerToControllerChannelManager, ControllerRequestCompletionHandler, LogDirEventManager, LogDirEventManagerImpl}
import kafka.utils.{MockScheduler, MockTime}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.AlterReplicaStateResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AlterReplicaStateRequest, AlterReplicaStateResponse}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.times
import org.mockito.{ArgumentCaptor, Mockito}

import scala.jdk.CollectionConverters._

class LogDirEventManagerTest {

  var brokerToController: BrokerToControllerChannelManager = _
  val time = new MockTime
  val metrics = new Metrics
  val brokerId = 1
  val offlineStateByte: Byte = kafka.controller.OfflineReplica.state

  val topic = "test-topic"
  val reason = "unit-test"
  val tp0 = new TopicPartition(topic, 0)
  val tp1 = new TopicPartition(topic, 1)
  val tp2 = new TopicPartition(topic, 2)

  @BeforeEach
  def setup(): Unit = {
    brokerToController = Mockito.mock(classOf[BrokerToControllerChannelManager])
  }

  @Test
  def testOnlyOneRequestInFlight(): Unit = {
    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] =
      ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val logDirEventManager = new LogDirEventManagerImpl(brokerToController, scheduler, time, brokerId, () => 2)
    logDirEventManager.start()

    // Enqueue more updates
    val stateChangeItems1 = AlterReplicaStateItem(Collections.singletonList(tp0),offlineStateByte, reason, _ => {})
    val stateChangeItems2 = AlterReplicaStateItem(Collections.singletonList(tp1),offlineStateByte, reason, _ => {})
    logDirEventManager.handleAlterReplicaStateChanges(stateChangeItems1)
    logDirEventManager.handleAlterReplicaStateChanges(stateChangeItems2)

    time.sleep(50)
    scheduler.tick()

    Mockito.verify(brokerToController, times(1))
      .sendRequest(any(), callbackCapture.capture())

    // Even an empty response will clear the in-flight
    val alterReplicaStateResp = new AlterReplicaStateResponse(new AlterReplicaStateResponseData())
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterReplicaStateResp)
    callbackCapture.getValue.onComplete(resp)

    Mockito.reset(brokerToController)

    time.sleep(100)
    scheduler.tick()

    Mockito.verify(brokerToController, times(1))
      .sendRequest(any(), any())
  }

  @Test
  def testBuildRequest(): Unit = {
    Mockito.reset(brokerToController)

    val capture: ArgumentCaptor[AbstractRequest.Builder[AlterReplicaStateRequest]] =
      ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[AlterReplicaStateRequest]])

    val scheduler = new MockScheduler(time)
    val logDirEventManager = new LogDirEventManagerImpl(brokerToController, scheduler, time, brokerId, () => 2)
    logDirEventManager.start()

    val stateChangeItem = AlterReplicaStateItem(Range(0, 10).map(new TopicPartition(topic, _)).asJava, offlineStateByte, reason, _ => {})
    logDirEventManager.handleAlterReplicaStateChanges(stateChangeItem)

    time.sleep(50)
    scheduler.tick()

    Mockito.verify(brokerToController, times(1))
      .sendRequest(capture.capture(), any())

    val request = capture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    assertEquals(request.data().topics().get(0).partitions().size(), 10)
  }

  @Test
  def testTopError(): Unit = {
    val errors = Array(Errors.CLUSTER_AUTHORIZATION_FAILED,
      Errors.STALE_BROKER_EPOCH,
      Errors.NOT_CONTROLLER,
      Errors.UNKNOWN_REPLICA_STATE,
      Errors.UNKNOWN_SERVER_ERROR)

    for (error <- errors) {
      val stateChangeItem = AlterReplicaStateItem(Collections.singletonList(tp0), offlineStateByte, reason, _ => {})
      val manager = testTopLevelError(stateChangeItem, error)
      // On stale broker epoch, we want to retry
      assertEquals(1, manager.pendingAlterReplicaStateItemCount())
    }
  }

  private def testTopLevelError(stateChangeItems: AlterReplicaStateItem, error: Errors): LogDirEventManager = {
    Mockito.reset(brokerToController)

    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] =
      ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val logDirEventManager = new LogDirEventManagerImpl(brokerToController, scheduler, time, brokerId, () => 2)
    logDirEventManager.start()
    logDirEventManager.handleAlterReplicaStateChanges(stateChangeItems)

    time.sleep(50)
    scheduler.tick()
    Mockito.verify(brokerToController, times(1))
      .sendRequest(any(), callbackCapture.capture())

    val alterReplicaStateResp = new AlterReplicaStateResponse(new AlterReplicaStateResponseData().setErrorCode(error.code))
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterReplicaStateResp)
    callbackCapture.getValue.onComplete(resp)
    logDirEventManager
  }

  @Test
  def testPartitionLevelError(): Unit = {
    val errors = Seq(Errors.UNKNOWN_TOPIC_OR_PARTITION,
      Errors.UNKNOWN_SERVER_ERROR)

    errors.foreach(error => {
      val manager = testPartitionError(tp0, error)
      // Any partition-level error should also retry
      assertEquals(1, manager.pendingAlterReplicaStateItemCount())
    })
  }

  private def testPartitionError(tp: TopicPartition, error: Errors): LogDirEventManager = {
    Mockito.reset(brokerToController)

    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] =
      ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    val scheduler = new MockScheduler(time)
    val logDirEventManager = new LogDirEventManagerImpl(brokerToController, scheduler, time, brokerId, () => 2)

    val stateChangeItem = AlterReplicaStateItem(Collections.singletonList(tp0), offlineStateByte, reason, _ => {})
    logDirEventManager.start()
    logDirEventManager.handleAlterReplicaStateChanges(stateChangeItem)

    time.sleep(50)
    scheduler.tick()
    Mockito.verify(brokerToController, times(1))
      .sendRequest(any(), callbackCapture.capture())

    val alterReplicaStateResp = new AlterReplicaStateResponse(new AlterReplicaStateResponseData()
      .setTopics(Collections.singletonList(
        new AlterReplicaStateResponseData.TopicData()
          .setName(tp.topic())
          .setPartitions(Collections.singletonList(
            new AlterReplicaStateResponseData.PartitionData()
              .setPartitionIndex(tp.partition())
              .setErrorCode(error.code))))))
    val resp = new ClientResponse(null, null, "", 0L, 0L, false, null, null, alterReplicaStateResp)
    callbackCapture.getValue.onComplete(resp)
    logDirEventManager
  }

  @Test
  def testNoError(): Unit = {
    Mockito.reset(brokerToController)

    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] =
      ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val logDirEventManager = new LogDirEventManagerImpl(brokerToController, scheduler, time, brokerId, () => 2)

    val stateChangeItem = AlterReplicaStateItem(Collections.singletonList(tp0), offlineStateByte, reason, _ => {})
    logDirEventManager.start()
    logDirEventManager.handleAlterReplicaStateChanges(stateChangeItem)

    time.sleep(50)
    scheduler.tick()
    Mockito.verify(brokerToController, times(1))
      .sendRequest(any(), callbackCapture.capture())

    val alterReplicaStateResp = new AlterReplicaStateResponse(new AlterReplicaStateResponseData()
      .setTopics(Collections.singletonList(
        new AlterReplicaStateResponseData.TopicData()
          .setName(tp0.topic())
          .setPartitions(Collections.singletonList(
            new AlterReplicaStateResponseData.PartitionData()
              .setPartitionIndex(tp0.partition()))))))
    val resp = new ClientResponse(null, null, "", 0L, 0L, false, null, null, alterReplicaStateResp)
    callbackCapture.getValue.onComplete(resp)
    assertEquals(0, logDirEventManager.pendingAlterReplicaStateItemCount())
  }

  @Test
  def testPartialSuccess(): Unit = {
    Mockito.reset(brokerToController)

    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] =
      ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val logDirEventManager = new LogDirEventManagerImpl(brokerToController, scheduler, time, brokerId, () => 2)

    val stateChangeItem = AlterReplicaStateItem(Collections.singletonList(tp0), offlineStateByte, reason, _ => {})
    logDirEventManager.start()
    logDirEventManager.handleAlterReplicaStateChanges(stateChangeItem)

    time.sleep(50)
    scheduler.tick()
    Mockito.verify(brokerToController, times(1))
      .sendRequest(any(), callbackCapture.capture())

    val alterReplicaStateResp = new AlterReplicaStateResponse(new AlterReplicaStateResponseData()
      .setTopics(Collections.singletonList(
        new AlterReplicaStateResponseData.TopicData()
          .setName(tp0.topic())
          .setPartitions(
            Seq(new AlterReplicaStateResponseData.PartitionData().setPartitionIndex(tp0.partition()),
              new AlterReplicaStateResponseData.PartitionData().setPartitionIndex(tp0.partition())
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())).asJava )
      )))
    val resp = new ClientResponse(null, null, "", 0L, 0L, false, null, null, alterReplicaStateResp)
    callbackCapture.getValue.onComplete(resp)
    assertEquals(1, logDirEventManager.pendingAlterReplicaStateItemCount())

    val requestCapture: ArgumentCaptor[AbstractRequest.Builder[AlterReplicaStateRequest]] =
      ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[AlterReplicaStateRequest]])
    Mockito.reset(brokerToController)

    time.sleep(50)
    scheduler.tick()
    Mockito.verify(brokerToController, times(1))
      .sendRequest(requestCapture.capture(), any())

    val request = requestCapture.getValue.build()
    assertEquals(request.data().topics().size(), 1)
    // new request only contains 1 partition
    assertEquals(request.data().topics().get(0).partitions().size(), 1)
  }

  @Test
  def testPartitionMissingInResponse(): Unit = {
    Mockito.reset(brokerToController)

    val callbackCapture: ArgumentCaptor[ControllerRequestCompletionHandler] =
      ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])

    val scheduler = new MockScheduler(time)
    val logDirEventManager = new LogDirEventManagerImpl(brokerToController, scheduler, time, brokerId, () => 2)
    logDirEventManager.start()

    val count = new AtomicInteger(0)
    val callback = (_:  Either[Errors, TopicPartition]) => {
      count.incrementAndGet()
      return
    }

    val stateChangeItem = AlterReplicaStateItem(Seq(tp0, tp1, tp2).asJava, offlineStateByte, reason, callback)

    logDirEventManager.handleAlterReplicaStateChanges(stateChangeItem)
    time.sleep(50)
    scheduler.tick()

    Mockito.verify(brokerToController, times(1))
      .sendRequest(any(), callbackCapture.capture())

    // Three partitions were sent, but only one returned
    val alterReplicaStateResp = new AlterReplicaStateResponse(new AlterReplicaStateResponseData()
      .setTopics(Collections.singletonList(
        new AlterReplicaStateResponseData.TopicData()
          .setName(tp0.topic())
          .setPartitions(Collections.singletonList(
            new AlterReplicaStateResponseData.PartitionData()
              .setPartitionIndex(tp0.partition())
              .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()))))))
    val resp = new ClientResponse(null, null, "", 0L, 0L,
      false, null, null, alterReplicaStateResp)
    callbackCapture.getValue.onComplete(resp)

    assertEquals(count.get, 3, "Expected all callbacks to run")
    assertEquals(1, logDirEventManager.pendingAlterReplicaStateItemCount())
  }
}
