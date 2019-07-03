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

import java.util.Arrays.asList
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache}
import kafka.utils.timer.MockTimer
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.requests.{RequestHeader, TransactionResult, WriteTxnMarkersRequest, WriteTxnMarkersResponse}
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.{Node, TopicPartition}
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.Assert._
import org.junit.Test
import com.yammer.metrics.Metrics
import kafka.common.RequestAndCompletionHandler
import org.apache.kafka.common.protocol.{ApiKeys, Errors}

import scala.collection.JavaConverters._
import scala.collection.mutable

class TransactionMarkerChannelManagerTest {
  private val metadataCache: MetadataCache = EasyMock.createNiceMock(classOf[MetadataCache])
  private val networkClient: NetworkClient = EasyMock.createNiceMock(classOf[NetworkClient])
  private val txnStateManager: TransactionStateManager = EasyMock.createNiceMock(classOf[TransactionStateManager])

  private val partition1 = new TopicPartition("topic1", 0)
  private val partition2 = new TopicPartition("topic1", 1)
  private val broker1 = new Node(1, "host", 10)
  private val broker2 = new Node(2, "otherhost", 10)

  private val transactionalId1 = "txnId1"
  private val transactionalId2 = "txnId2"
  private val producerId1 = 0.asInstanceOf[Long]
  private val producerId2 = 1.asInstanceOf[Long]
  private val producerEpoch = 0.asInstanceOf[Short]
  private val txnTopicPartition1 = 0
  private val txnTopicPartition2 = 1
  private val coordinatorEpoch = 0
  private val txnTimeoutMs = 0
  private val txnResult = TransactionResult.COMMIT
  private val txnMetadata1 = new TransactionMetadata(transactionalId1, producerId1, producerEpoch, txnTimeoutMs,
    PrepareCommit, mutable.Set[TopicPartition](partition1, partition2), 0L, 0L)
  private val txnMetadata2 = new TransactionMetadata(transactionalId2, producerId2, producerEpoch, txnTimeoutMs,
    PrepareCommit, mutable.Set[TopicPartition](partition1), 0L, 0L)

  private val capturedErrorsCallback: Capture[Errors => Unit] = EasyMock.newCapture()

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

  private def mockCache(): Unit = {
    EasyMock.expect(txnStateManager.partitionFor(transactionalId1))
      .andReturn(txnTopicPartition1)
      .anyTimes()
    EasyMock.expect(txnStateManager.partitionFor(transactionalId2))
      .andReturn(txnTopicPartition2)
      .anyTimes()
    EasyMock.expect(txnStateManager.getTransactionState(EasyMock.eq(transactionalId1)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))))
      .anyTimes()
    EasyMock.expect(txnStateManager.getTransactionState(EasyMock.eq(transactionalId2)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata2))))
      .anyTimes()
    val stateLock = new ReentrantReadWriteLock
    EasyMock.expect(txnStateManager.stateReadLock)
      .andReturn(stateLock.readLock)
      .anyTimes()
  }

  @Test
  def shouldGenerateEmptyMapWhenNoRequestsOutstanding(): Unit = {
    assertTrue(channelManager.generateRequests().isEmpty)
  }

  @Test
  def shouldGenerateRequestPerPartitionPerBroker(): Unit = {
    mockCache()
    EasyMock.replay(txnStateManager)

    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition1.topic),
      EasyMock.eq(partition1.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker1)).anyTimes()
    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition2.topic),
      EasyMock.eq(partition2.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker2)).anyTimes()

    EasyMock.replay(metadataCache)

    channelManager.addTxnMarkersToSend(transactionalId1, coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds()))
    channelManager.addTxnMarkersToSend(transactionalId2, coordinatorEpoch, txnResult, txnMetadata2, txnMetadata2.prepareComplete(time.milliseconds()))

    assertEquals(2, txnMarkerPurgatory.watched)
    assertEquals(2, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition2))
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition2))

    val expectedBroker1Request = new WriteTxnMarkersRequest.Builder(
      asList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, asList(partition1)),
        new WriteTxnMarkersRequest.TxnMarkerEntry(producerId2, producerEpoch, coordinatorEpoch, txnResult, asList(partition1)))).build()
    val expectedBroker2Request = new WriteTxnMarkersRequest.Builder(
      asList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, asList(partition2)))).build()

    val requests: Map[Node, WriteTxnMarkersRequest] = channelManager.generateRequests().map { handler =>
      (handler.destination, handler.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build())
    }.toMap

    assertEquals(Map(broker1 -> expectedBroker1Request, broker2 -> expectedBroker2Request), requests)
    assertTrue(channelManager.generateRequests().isEmpty)
  }

  @Test
  def shouldSkipSendMarkersWhenLeaderNotFound(): Unit = {
    mockCache()
    EasyMock.replay(txnStateManager)

    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition1.topic),
      EasyMock.eq(partition1.partition),
      EasyMock.anyObject())
    ).andReturn(None).anyTimes()
    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition2.topic),
      EasyMock.eq(partition2.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker2)).anyTimes()

    EasyMock.replay(metadataCache)

    channelManager.addTxnMarkersToSend(transactionalId1, coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds()))
    channelManager.addTxnMarkersToSend(transactionalId2, coordinatorEpoch, txnResult, txnMetadata2, txnMetadata2.prepareComplete(time.milliseconds()))

    assertEquals(1, txnMarkerPurgatory.watched)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers)
    assertTrue(channelManager.queueForBroker(broker1.id).isEmpty)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition2))
  }

  @Test
  def shouldSaveForLaterWhenLeaderUnknownButNotAvailable(): Unit = {
    mockCache()
    EasyMock.replay(txnStateManager)

    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition1.topic),
      EasyMock.eq(partition1.partition),
      EasyMock.anyObject())
    ).andReturn(Some(Node.noNode))
      .andReturn(Some(Node.noNode))
      .andReturn(Some(Node.noNode))
      .andReturn(Some(Node.noNode))
      .andReturn(Some(broker1))
      .andReturn(Some(broker1))
    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition2.topic),
      EasyMock.eq(partition2.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker2)).anyTimes()

    EasyMock.replay(metadataCache)

    channelManager.addTxnMarkersToSend(transactionalId1, coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds()))
    channelManager.addTxnMarkersToSend(transactionalId2, coordinatorEpoch, txnResult, txnMetadata2, txnMetadata2.prepareComplete(time.milliseconds()))

    assertEquals(2, txnMarkerPurgatory.watched)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers)
    assertTrue(channelManager.queueForBroker(broker1.id).isEmpty)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition2))
    assertEquals(2, channelManager.queueForUnknownBroker.totalNumMarkers)
    assertEquals(1, channelManager.queueForUnknownBroker.totalNumMarkers(txnTopicPartition1))
    assertEquals(1, channelManager.queueForUnknownBroker.totalNumMarkers(txnTopicPartition2))

    val expectedBroker1Request = new WriteTxnMarkersRequest.Builder(
      asList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, asList(partition1)),
        new WriteTxnMarkersRequest.TxnMarkerEntry(producerId2, producerEpoch, coordinatorEpoch, txnResult, asList(partition1)))).build()
    val expectedBroker2Request = new WriteTxnMarkersRequest.Builder(
      asList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, asList(partition2)))).build()

    val firstDrainedRequests: Map[Node, WriteTxnMarkersRequest] = channelManager.generateRequests().map { handler =>
      (handler.destination, handler.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build())
    }.toMap

    assertEquals(Map(broker2 -> expectedBroker2Request), firstDrainedRequests)

    val secondDrainedRequests: Map[Node, WriteTxnMarkersRequest] = channelManager.generateRequests().map { handler =>
      (handler.destination, handler.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build())
    }.toMap

    assertEquals(Map(broker1 -> expectedBroker1Request), secondDrainedRequests)
  }

  @Test
  def shouldRemoveMarkersForTxnPartitionWhenPartitionEmigrated(): Unit = {
    mockCache()
    EasyMock.replay(txnStateManager)

    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition1.topic),
      EasyMock.eq(partition1.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker1)).anyTimes()
    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition2.topic),
      EasyMock.eq(partition2.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker2)).anyTimes()

    EasyMock.replay(metadataCache)

    channelManager.addTxnMarkersToSend(transactionalId1, coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds()))
    channelManager.addTxnMarkersToSend(transactionalId2, coordinatorEpoch, txnResult, txnMetadata2, txnMetadata2.prepareComplete(time.milliseconds()))

    assertEquals(2, txnMarkerPurgatory.watched)
    assertEquals(2, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition2))
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition2))

    channelManager.removeMarkersForTxnTopicPartition(txnTopicPartition1)

    assertEquals(1, txnMarkerPurgatory.watched)
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(0, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition2))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers)
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition2))
  }

  @Test
  def shouldCompleteAppendToLogOnEndTxnWhenSendMarkersSucceed(): Unit = {
    mockCache()

    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition1.topic),
      EasyMock.eq(partition1.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker1)).anyTimes()
    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition2.topic),
      EasyMock.eq(partition2.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker2)).anyTimes()

    val txnTransitionMetadata2 = txnMetadata2.prepareComplete(time.milliseconds())

    EasyMock.expect(txnStateManager.appendTransactionToLog(
      EasyMock.eq(transactionalId2),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(txnTransitionMetadata2),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          txnMetadata2.completeTransitionTo(txnTransitionMetadata2)
          capturedErrorsCallback.getValue.apply(Errors.NONE)
        }
      }).once()
    EasyMock.replay(txnStateManager, metadataCache)

    channelManager.addTxnMarkersToSend(transactionalId2, coordinatorEpoch, txnResult, txnMetadata2, txnTransitionMetadata2)

    val requestAndHandlers: Iterable[RequestAndCompletionHandler] = channelManager.generateRequests()

    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.NONE))
    for (requestAndHandler <- requestAndHandlers) {
      requestAndHandler.handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
        null, null, 0, 0, false, null, null, response))
    }

    EasyMock.verify(txnStateManager)

    assertEquals(0, txnMarkerPurgatory.watched)
    assertEquals(0, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(None, txnMetadata2.pendingState)
    assertEquals(CompleteCommit, txnMetadata2.state)
  }

  @Test
  def shouldAbortAppendToLogOnEndTxnWhenNotCoordinatorError(): Unit = {
    mockCache()

    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition1.topic),
      EasyMock.eq(partition1.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker1)).anyTimes()
    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition2.topic),
      EasyMock.eq(partition2.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker2)).anyTimes()

    val txnTransitionMetadata2 = txnMetadata2.prepareComplete(time.milliseconds())

    EasyMock.expect(txnStateManager.appendTransactionToLog(
      EasyMock.eq(transactionalId2),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(txnTransitionMetadata2),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          txnMetadata2.pendingState = None
          capturedErrorsCallback.getValue.apply(Errors.NOT_COORDINATOR)
        }
      }).once()
    EasyMock.replay(txnStateManager, metadataCache)

    channelManager.addTxnMarkersToSend(transactionalId2, coordinatorEpoch, txnResult, txnMetadata2, txnTransitionMetadata2)

    val requestAndHandlers: Iterable[RequestAndCompletionHandler] = channelManager.generateRequests()

    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.NONE))
    for (requestAndHandler <- requestAndHandlers) {
      requestAndHandler.handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
        null, null, 0, 0, false, null, null, response))
    }

    EasyMock.verify(txnStateManager)

    assertEquals(0, txnMarkerPurgatory.watched)
    assertEquals(0, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(None, txnMetadata2.pendingState)
    assertEquals(PrepareCommit, txnMetadata2.state)
  }

  @Test
  def shouldRetryAppendToLogOnEndTxnWhenCoordinatorNotAvailableError(): Unit = {
    mockCache()

    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition1.topic),
      EasyMock.eq(partition1.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker1)).anyTimes()
    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.eq(partition2.topic),
      EasyMock.eq(partition2.partition),
      EasyMock.anyObject())
    ).andReturn(Some(broker2)).anyTimes()

    val txnTransitionMetadata2 = txnMetadata2.prepareComplete(time.milliseconds())

    EasyMock.expect(txnStateManager.appendTransactionToLog(
      EasyMock.eq(transactionalId2),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(txnTransitionMetadata2),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          capturedErrorsCallback.getValue.apply(Errors.COORDINATOR_NOT_AVAILABLE)
        }
      })
      .andAnswer(new IAnswer[Unit] {
      override def answer(): Unit = {
        txnMetadata2.completeTransitionTo(txnTransitionMetadata2)
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      }
    })

    EasyMock.replay(txnStateManager, metadataCache)

    channelManager.addTxnMarkersToSend(transactionalId2, coordinatorEpoch, txnResult, txnMetadata2, txnTransitionMetadata2)

    val requestAndHandlers: Iterable[RequestAndCompletionHandler] = channelManager.generateRequests()

    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.NONE))
    for (requestAndHandler <- requestAndHandlers) {
      requestAndHandler.handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
        null, null, 0, 0, false, null, null, response))
    }

    // call this again so that append log will be retried
    channelManager.generateRequests()

    EasyMock.verify(txnStateManager)

    assertEquals(0, txnMarkerPurgatory.watched)
    assertEquals(0, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(None, txnMetadata2.pendingState)
    assertEquals(CompleteCommit, txnMetadata2.state)
  }

  private def createPidErrorMap(errors: Errors) = {
    val pidMap = new java.util.HashMap[java.lang.Long, java.util.Map[TopicPartition, Errors]]()
    val errorsMap = new java.util.HashMap[TopicPartition, Errors]()
    errorsMap.put(partition1, errors)
    pidMap.put(producerId2, errorsMap)
    pidMap
  }

  @Test
  def shouldCreateMetricsOnStarting(): Unit = {
    val metrics = Metrics.defaultRegistry.allMetrics.asScala

    assertEquals(1, metrics
      .filterKeys(_.getMBeanName == "kafka.coordinator.transaction:type=TransactionMarkerChannelManager,name=UnknownDestinationQueueSize")
      .size)
    assertEquals(1, metrics
      .filterKeys(_.getMBeanName == "kafka.coordinator.transaction:type=TransactionMarkerChannelManager,name=LogAppendRetryQueueSize")
      .size)
  }
}
