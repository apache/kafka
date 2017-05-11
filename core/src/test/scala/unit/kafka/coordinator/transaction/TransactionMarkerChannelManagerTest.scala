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
import kafka.common.{BrokerEndPointNotAvailableException, InterBrokerSendThread}
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache}
import kafka.utils.timer.MockTimer
import kafka.utils.TestUtils
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.utils.{MockTime, Utils}
import org.apache.kafka.common.{Node, TopicPartition}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TransactionMarkerChannelManagerTest {
  private val metadataCache = EasyMock.createNiceMock(classOf[MetadataCache])
  private val networkClient = EasyMock.createNiceMock(classOf[NetworkClient])
  private val txnStateManager = EasyMock.createNiceMock(classOf[TransactionStateManager])

  private val partition1 = new TopicPartition("topic1", 0)
  private val partition2 = new TopicPartition("topic1", 1)
  private val broker1 = new Node(1, "host", 10)
  private val broker2 = new Node(2, "otherhost", 10)

  private val transactionalId1 = "txnId1"
  private val transactionalId2 = "txnId2"
  private val producerId1 = 0.asInstanceOf[Long]
  private val producerId2 = 0.asInstanceOf[Long]
  private val producerEpoch = 0.asInstanceOf[Short]
  private val txnTopicPartition = 0
  private val coordinatorEpoch = 0
  private val txnTimeoutMs = 0
  private val txnResult = TransactionResult.COMMIT

  private val txnMarkerPurgatory = new DelayedOperationPurgatory[DelayedTxnMarker]("txn-purgatory-name",
    new MockTimer,
    reaperEnabled = false)
  private val time = new MockTime

  private val channelManager = new TransactionMarkerChannelManager(
    KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:2181")),
    metadataCache,
    networkClient,
    txnStateManager,
    txnMarkerPurgatory,
    time)

  private val senderThread = channelManager.senderThread

  @Test
  def shouldGenerateEmptyMapWhenNoRequestsOutstanding(): Unit = {
    assertTrue(senderThread.generateRequests().isEmpty)
  }

  @Test
  def shouldGenerateRequestPerBroker(): Unit = {
    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition1.topic),
      EasyMock.eq(partition1.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker1))

    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition2.topic),
      EasyMock.eq(partition2.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker2))

    EasyMock.replay(metadataCache)

    val txnMetadata = new TransactionMetadata(producerId1, producerEpoch, txnTimeoutMs, PrepareCommit, mutable.Set[TopicPartition](partition1, partition2), 0L, 0L)
    channelManager.addTxnMarkersToSend(transactionalId1, coordinatorEpoch, txnResult, txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))

    channelManager.queueForBroker(broker1).get.totalNumMarkers()

    val expectedBroker1Request = new WriteTxnMarkersRequest.Builder(
      Utils.mkList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, TransactionResult.COMMIT, Utils.mkList(partition1)))).build()
    val expectedBroker2Request = new WriteTxnMarkersRequest.Builder(
      Utils.mkList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, TransactionResult.COMMIT, Utils.mkList(partition2)))).build()

    val requests: Map[Node, WriteTxnMarkersRequest] = senderThread.generateRequests().map { handler =>
      (handler.destination, handler.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build())
    }.toMap

    assertEquals(List((broker1, expectedBroker1Request), (broker2, expectedBroker2Request)), requests)
    assertTrue(senderThread.generateRequests().isEmpty)
  }

  @Test
  def shouldGenerateRequestPerPartitionPerBroker(): Unit = {
    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition1.topic),
      EasyMock.eq(partition1.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker1))

    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition2.topic),
      EasyMock.eq(partition2.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker1))

    EasyMock.replay(metadataCache)

    val txnMetadata1 = new TransactionMetadata(producerId1, producerEpoch, txnTimeoutMs, PrepareCommit, mutable.Set[TopicPartition](partition1), 0L, 0L)
    val txnMetadata2 = new TransactionMetadata(producerId2, producerEpoch, txnTimeoutMs, PrepareCommit, mutable.Set[TopicPartition](partition2), 0L, 0L)
    channelManager.addTxnMarkersToSend(transactionalId1, coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds()))
    channelManager.addTxnMarkersToSend(transactionalId2, coordinatorEpoch, txnResult, txnMetadata2, txnMetadata2.prepareComplete(time.milliseconds()))

    val expectedPartition1Request = new WriteTxnMarkersRequest.Builder(
      Utils.mkList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, TransactionResult.COMMIT, Utils.mkList(partition1)))).build()
    val expectedPartition2Request = new WriteTxnMarkersRequest.Builder(
      Utils.mkList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId2, producerEpoch, coordinatorEpoch, TransactionResult.COMMIT, Utils.mkList(partition2)))).build()

    val requests: Map[Node, WriteTxnMarkersRequest] = senderThread.generateRequests().map { handler =>
      (handler.destination, handler.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build())
    }.toMap

    assertEquals(List((broker1, expectedPartition1Request), (broker1, expectedPartition2Request)), requests)
    assertTrue(senderThread.generateRequests().isEmpty)
  }

  @Test
  def shouldRetryGettingLeaderWhenNotFound(): Unit = {
    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition1.topic),
      EasyMock.eq(partition1.partition),
      EasyMock.anyObject())
    ).andReturn(None)
     .andReturn(None)
     .andReturn(Some(broker1))

    EasyMock.replay(metadataCache)

    channelManager.addTxnMarkersToBrokerQueue(transactionalId1, producerId1, producerEpoch, TransactionResult.COMMIT, coordinatorEpoch, Set[TopicPartition](partition1))

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

    channelManager.addTxnMarkersToSend(txnTopicPartition, metadata, 0, completionCallback)

    assertEquals(Some(metadata), channel.pendingTxnMetadata(txnTopicPartition, 1))
  }

  @Test
  def shouldAddRequestToBrokerQueue(): Unit = {
    val metadata = new TransactionMetadata(1, 0, 0, PrepareCommit, mutable.Set[TopicPartition](partition1), 0, 0L)

    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))
    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(1), EasyMock.anyObject())).andReturn(Some(broker1))
    EasyMock.replay(metadataCache)

    channelManager.addTxnMarkersToSend(txnTopicPartition, metadata, 0, completionCallback)
    assertEquals(1, requestGenerator().size)
  }

  @Test
  def shouldAddDelayedTxnMarkerToPurgatory(): Unit = {
    val metadata = new TransactionMetadata(1, 0, 0, PrepareCommit, mutable.Set[TopicPartition](partition1), 0, 0L)
    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))
    EasyMock.expect(metadataCache.getAliveEndpoint(EasyMock.eq(1), EasyMock.anyObject())).andReturn(Some(broker1))

    EasyMock.replay(metadataCache)

    channelManager.addTxnMarkersToSend(txnTopicPartition, metadata, 0, completionCallback)
    assertEquals(1,txnMarkerPurgatory.watched)
  }

  @Test
  def shouldStartInterBrokerThreadOnStartup(): Unit = {
    EasyMock.expect(sendThread.start())
    EasyMock.replay(sendThread)
    channelManager.start()
    EasyMock.verify(sendThread)
  }


  @Test
  def shouldStopInterBrokerThreadOnShutdown(): Unit = {
    EasyMock.expect(sendThread.shutdown())
    EasyMock.replay(sendThread)
    channelManager.shutdown()
    EasyMock.verify(sendThread)
  }

  @Test
  def shouldClearPurgatoryForPartitionWhenPartitionEmigrated(): Unit = {
    val metadata1 = new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set[TopicPartition](partition1), 0, 0)
    txnMarkerPurgatory.tryCompleteElseWatch(new DelayedTxnMarker(metadata1, (error:Errors) => {}),Seq(0L))
    channel.maybeAddPendingRequest(0, metadata1)

    val metadata2 = new TransactionMetadata(1, 0, 0, PrepareCommit, mutable.Set[TopicPartition](partition1), 0, 0)
    txnMarkerPurgatory.tryCompleteElseWatch(new DelayedTxnMarker(metadata2, (error:Errors) => {}),Seq(1L))
    channel.maybeAddPendingRequest(0, metadata2)

    val metadata3 = new TransactionMetadata(2, 0, 0, PrepareCommit, mutable.Set[TopicPartition](partition1), 0, 0)
    txnMarkerPurgatory.tryCompleteElseWatch(new DelayedTxnMarker(metadata3, (error:Errors) => {}),Seq(2L))
    channel.maybeAddPendingRequest(1, metadata3)

    channelManager.removeMarkersForTxnTopicPartition(0)

    assertEquals(1, txnMarkerPurgatory.watched)
    // should not complete as they've been removed
    txnMarkerPurgatory.checkAndComplete(0L)
    txnMarkerPurgatory.checkAndComplete(1L)
    
    assertEquals(1, txnMarkerPurgatory.watched)
  }

  def completionCallback(errors: Errors): Unit = {
  }
}
