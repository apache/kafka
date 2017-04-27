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
package kafka.coordinator.transaction

import kafka.api.{LeaderAndIsr, PartitionStateInfo}
import kafka.common.{BrokerEndPointNotAvailableException, BrokerNotAvailableException, InterBrokerSendThread}
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache}
import kafka.utils.{MockTime, TestUtils}
import kafka.utils.timer.MockTimer
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Node, TopicPartition}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TransactionMarkerChannelManagerTest {
  private val metadataCache = EasyMock.createNiceMock(classOf[MetadataCache])
  private val interBrokerSendThread = EasyMock.createNiceMock(classOf[InterBrokerSendThread])
  private val networkClient = EasyMock.createNiceMock(classOf[NetworkClient])
  private val channel = new TransactionMarkerChannel(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
    metadataCache,
    networkClient,
    new MockTime())
  private val purgatory = new DelayedOperationPurgatory[DelayedTxnMarker]("txn-purgatory-name",
    new MockTimer,
    reaperEnabled = false)
  private val requestGenerator = TransactionMarkerChannelManager.requestGenerator(channel, purgatory)
  private val partition1 = new TopicPartition("topic1", 0)
  private val partition2 = new TopicPartition("topic1", 1)
  private val broker1 = new Node(1, "host", 10)
  private val broker2 = new Node(2, "otherhost", 10)
  private val metadataPartition = 0
  private val channelManager = new TransactionMarkerChannelManager(
    KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:2181")),
    metadataCache,
    purgatory,
    interBrokerSendThread,
    channel)

  @Test
  def shouldGenerateEmptyMapWhenNoRequestsOutstanding(): Unit = {
    assertTrue(requestGenerator().isEmpty)
  }

  @Test
  def shouldGenerateRequestPerBroker(): Unit ={
    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))

    EasyMock.expect(metadataCache.getPartitionInfo(partition2.topic(), partition2.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(2, 0, List.empty, 0), 0), Set.empty)))

    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(1), EasyMock.anyObject())).andReturn(Some(broker1))
    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(2), EasyMock.anyObject())).andReturn(Some(broker2))

    EasyMock.replay(metadataCache)

    channel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](partition1, partition2))


    val expectedBroker1Request = new WriteTxnMarkersRequest.Builder(0,
      Utils.mkList(new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, TransactionResult.COMMIT, Utils.mkList(partition1)))).build()
    val expectedBroker2Request = new WriteTxnMarkersRequest.Builder(0,
      Utils.mkList(new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, TransactionResult.COMMIT, Utils.mkList(partition2)))).build()

    val requests: Map[Node, WriteTxnMarkersRequest] = requestGenerator().map{ result =>
      (result.destination, result.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build())
    }.toMap

    val broker1Request = requests(broker1)
    val broker2Request = requests(broker2)

    assertEquals(2, requests.size)
    assertEquals(expectedBroker1Request, broker1Request)
    assertEquals(expectedBroker2Request, broker2Request)

  }

  @Test
  def shouldGenerateRequestPerPartitionPerBroker(): Unit ={
    val partitionOneEpoch = 0
    val partitionTwoEpoch = 1

    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, partitionOneEpoch, List.empty, 0), 0), Set.empty)))


    EasyMock.expect(metadataCache.getPartitionInfo(partition2.topic(), partition2.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, partitionTwoEpoch, List.empty, 0), 0), Set.empty)))
    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(1), EasyMock.anyObject())).andReturn(Some(broker1)).anyTimes()
    EasyMock.replay(metadataCache)

    channel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, partitionOneEpoch, Set[TopicPartition](partition1))
    channel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, partitionTwoEpoch, Set[TopicPartition](partition2))

    val expectedPartition1Request = new WriteTxnMarkersRequest.Builder(0,
      Utils.mkList(new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, TransactionResult.COMMIT, Utils.mkList(partition1)))).build()
    val expectedPartition2Request = new WriteTxnMarkersRequest.Builder(1,
      Utils.mkList(new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, TransactionResult.COMMIT, Utils.mkList(partition2)))).build()

    val requests = requestGenerator().map { result =>
      val markersRequest = result.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build()
      (markersRequest.coordinatorEpoch(), (result.destination, markersRequest))
    }.toMap

    val request1 = requests(partitionOneEpoch)
    val request2 = requests(partitionTwoEpoch)
    assertEquals(broker1, request1._1)
    assertEquals(broker1, request2._1)
    assertEquals(2, requests.size)
    assertEquals(expectedPartition1Request, request1._2)
    assertEquals(expectedPartition2Request, request2._2)
  }

  @Test
  def shouldDrainBrokerQueueWhenGeneratingRequests(): Unit = {
    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))
    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(1), EasyMock.anyObject())).andReturn(Some(broker1))
    EasyMock.replay(metadataCache)

    channel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](partition1))

    assertTrue(requestGenerator().nonEmpty)
    assertTrue(requestGenerator().isEmpty)
  }

  @Test
  def shouldRetryGettingLeaderWhenNotFound(): Unit = {
    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(None)
      .andReturn(None)
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))

    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(1), EasyMock.anyObject())).andReturn(Some(broker1))
    EasyMock.replay(metadataCache)

    channel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](partition1))

    EasyMock.verify(metadataCache)
  }

  @Test
  def shouldRetryGettingLeaderWhenBrokerEndPointNotAvailableException(): Unit = {
    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))
      .times(2)
    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(1), EasyMock.anyObject()))
      .andThrow(new BrokerEndPointNotAvailableException())
      .andReturn(Some(broker1))
    EasyMock.replay(metadataCache)

    channel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](partition1))

    EasyMock.verify(metadataCache)
  }

  @Test
  def shouldRetryGettingLeaderWhenLeaderDoesntExist(): Unit = {
    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))
      .times(2)

    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(1), EasyMock.anyObject()))
      .andReturn(None)
      .andReturn(Some(broker1))

    EasyMock.replay(metadataCache)

    channel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](partition1))

    EasyMock.verify(metadataCache)
  }

  @Test
  def shouldAddPendingTxnRequest(): Unit = {
    val metadata = new TransactionMetadata(1, 0, 0, PrepareCommit, mutable.Set[TopicPartition](partition1, partition2), 0, 0L)
    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))
    EasyMock.expect(metadataCache.getPartitionInfo(partition2.topic(), partition2.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(2, 0, List.empty, 0), 0), Set.empty)))
    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(1), EasyMock.anyObject())).andReturn(Some(broker1))
    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(2), EasyMock.anyObject())).andReturn(Some(broker2))

    EasyMock.replay(metadataCache)

    channelManager.addTxnMarkerRequest(metadataPartition, metadata, 0, completionCallback)

    assertEquals(Some(metadata), channel.pendingTxnMetadata(metadataPartition, 1))

  }

  @Test
  def shouldAddRequestToBrokerQueue(): Unit = {
    val metadata = new TransactionMetadata(1, 0, 0, PrepareCommit, mutable.Set[TopicPartition](partition1), 0, 0L)

    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))
    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(1), EasyMock.anyObject())).andReturn(Some(broker1))
    EasyMock.replay(metadataCache)

    channelManager.addTxnMarkerRequest(metadataPartition, metadata, 0, completionCallback)
    assertEquals(1, requestGenerator().size)
  }

  @Test
  def shouldAddDelayedTxnMarkerToPurgatory(): Unit = {
    val metadata = new TransactionMetadata(1, 0, 0, PrepareCommit, mutable.Set[TopicPartition](partition1), 0, 0L)
    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))
    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(1), EasyMock.anyObject())).andReturn(Some(broker1))

    EasyMock.replay(metadataCache)

    channelManager.addTxnMarkerRequest(metadataPartition, metadata, 0, completionCallback)
    assertEquals(1,purgatory.watched)
  }

  @Test
  def shouldStartInterBrokerThreadOnStartup(): Unit = {
    EasyMock.expect(interBrokerSendThread.start())
    EasyMock.replay(interBrokerSendThread)
    channelManager.start()
    EasyMock.verify(interBrokerSendThread)
  }


  @Test
  def shouldStopInterBrokerThreadOnShutdown(): Unit = {
    EasyMock.expect(interBrokerSendThread.shutdown())
    EasyMock.replay(interBrokerSendThread)
    channelManager.shutdown()
    EasyMock.verify(interBrokerSendThread)
  }

  @Test
  def shouldClearPurgatoryForPartitionWhenPartitionEmigrated(): Unit = {
    val metadata1 = new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set[TopicPartition](partition1), 0, 0)
    purgatory.tryCompleteElseWatch(new DelayedTxnMarker(metadata1, (error:Errors) => {}),Seq(0L))
    channel.maybeAddPendingRequest(0, metadata1)

    val metadata2 = new TransactionMetadata(1, 0, 0, PrepareCommit, mutable.Set[TopicPartition](partition1), 0, 0)
    purgatory.tryCompleteElseWatch(new DelayedTxnMarker(metadata2, (error:Errors) => {}),Seq(1L))
    channel.maybeAddPendingRequest(0, metadata2)

    val metadata3 = new TransactionMetadata(2, 0, 0, PrepareCommit, mutable.Set[TopicPartition](partition1), 0, 0)
    purgatory.tryCompleteElseWatch(new DelayedTxnMarker(metadata3, (error:Errors) => {}),Seq(2L))
    channel.maybeAddPendingRequest(1, metadata3)

    channelManager.removeStateForPartition(0)

    assertEquals(1, purgatory.watched)
    // should not complete as they've been removed
    purgatory.checkAndComplete(0L)
    purgatory.checkAndComplete(1L)
    
    assertEquals(1, purgatory.watched)
  }

  def completionCallback(errors: Errors): Unit = {
  }
}
