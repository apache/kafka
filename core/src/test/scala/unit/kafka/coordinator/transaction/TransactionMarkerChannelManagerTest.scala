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

import java.util
import java.util.Arrays.asList
import java.util.Collections
import java.util.concurrent.{Callable, Executors, Future}
import kafka.server.{KafkaConfig, MetadataCache}
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{RequestHeader, TransactionResult, WriteTxnMarkersRequest, WriteTxnMarkersResponse}
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics}
import org.apache.kafka.server.util.RequestAndCompletionHandler
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, mockConstruction, times, verify, verifyNoMoreInteractions, when}

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.util.Try

class TransactionMarkerChannelManagerTest {
  private val metadataCache: MetadataCache = mock(classOf[MetadataCache])
  private val networkClient: NetworkClient = mock(classOf[NetworkClient])
  private val txnStateManager: TransactionStateManager = mock(classOf[TransactionStateManager])

  private val partition1 = new TopicPartition("topic1", 0)
  private val partition2 = new TopicPartition("topic1", 1)
  private val broker1 = new Node(1, "host", 10)
  private val broker2 = new Node(2, "otherhost", 10)

  private val transactionalId1 = "txnId1"
  private val transactionalId2 = "txnId2"
  private val producerId1 = 0.asInstanceOf[Long]
  private val producerId2 = 1.asInstanceOf[Long]
  private val producerEpoch = 0.asInstanceOf[Short]
  private val lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH
  private val txnTopicPartition1 = 0
  private val txnTopicPartition2 = 1
  private val coordinatorEpoch = 0
  private val txnTimeoutMs = 0
  private val txnResult = TransactionResult.COMMIT
  private val txnMetadata1 = new TransactionMetadata(transactionalId1, producerId1, producerId1, RecordBatch.NO_PRODUCER_ID,
    producerEpoch, lastProducerEpoch, txnTimeoutMs, PrepareCommit, mutable.Set[TopicPartition](partition1, partition2), 0L, 0L)
  private val txnMetadata2 = new TransactionMetadata(transactionalId2, producerId2, producerId2, RecordBatch.NO_PRODUCER_ID,
    producerEpoch, lastProducerEpoch, txnTimeoutMs, PrepareCommit, mutable.Set[TopicPartition](partition1), 0L, 0L)

  private val capturedErrorsCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])
  private val time = new MockTime

  private val channelManager = new TransactionMarkerChannelManager(
    KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:2181")),
    metadataCache,
    networkClient,
    txnStateManager,
    time)

  private def mockCache(): Unit = {
    when(txnStateManager.partitionFor(transactionalId1))
      .thenReturn(txnTopicPartition1)
    when(txnStateManager.partitionFor(transactionalId2))
      .thenReturn(txnTopicPartition2)
    when(txnStateManager.getTransactionState(ArgumentMatchers.eq(transactionalId1)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))))
    when(txnStateManager.getTransactionState(ArgumentMatchers.eq(transactionalId2)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata2))))
  }

  @Test
  def testRemoveMetricsOnClose(): Unit = {
    val mockMetricsGroupCtor = mockConstruction(classOf[KafkaMetricsGroup])
    try {
      val transactionMarkerChannelManager = new TransactionMarkerChannelManager(
        KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:2181")),
        metadataCache,
        networkClient,
        txnStateManager,
        time)
      transactionMarkerChannelManager.shutdown()
      val mockMetricsGroup = mockMetricsGroupCtor.constructed.get(0)
      TransactionMarkerChannelManager.MetricNames.foreach(metricName => verify(mockMetricsGroup).newGauge(ArgumentMatchers.eq(metricName), any()))
      TransactionMarkerChannelManager.MetricNames.foreach(verify(mockMetricsGroup).removeMetric(_))
      // assert that we have verified all invocations on
      verifyNoMoreInteractions(mockMetricsGroup)
    } finally {
      mockMetricsGroupCtor.close()
    }
  }

  @Test
  def shouldOnlyWriteTxnCompletionOnce(): Unit = {
    mockCache()

    val expectedTransition = txnMetadata2.prepareComplete(time.milliseconds(), true)

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Some(broker1))

    when(txnStateManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(expectedTransition),
      capturedErrorsCallback.capture(),
      any(),
      any()))
      .thenAnswer(_ => {
        txnMetadata2.completeTransitionTo(expectedTransition)
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      })

    var addMarkerFuture: Future[Try[Unit]] = null
    val executor = Executors.newFixedThreadPool(1)
    txnMetadata2.lock.lock()
    try {
      addMarkerFuture = executor.submit((() => {
        Try(channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult,
            txnMetadata2, expectedTransition))
      }): Callable[Try[Unit]])

      val header = new RequestHeader(ApiKeys.WRITE_TXN_MARKERS, 0, "client", 1)
      val response = new WriteTxnMarkersResponse(
        Collections.singletonMap(producerId2: java.lang.Long, Collections.singletonMap(partition1, Errors.NONE)))
      val clientResponse = new ClientResponse(header, null, null,
        time.milliseconds(), time.milliseconds(), false, null, null,
        response)

      TestUtils.waitUntilTrue(() => {
        val requests = channelManager.generateRequests().asScala
        if (requests.nonEmpty) {
          assertEquals(1, requests.size)
          val request = requests.head
          request.handler.onComplete(clientResponse)
          true
        } else {
          false
        }
      }, "Timed out waiting for expected WriteTxnMarkers request")
    } finally {
      txnMetadata2.lock.unlock()
      executor.shutdown()
    }

    assertNotNull(addMarkerFuture)
    assertTrue(addMarkerFuture.get().isSuccess,
      "Add marker task failed with exception " + addMarkerFuture.get().get)

    verify(txnStateManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(expectedTransition),
      capturedErrorsCallback.capture(),
      any(),
      any())
  }

  @Test
  def shouldGenerateEmptyMapWhenNoRequestsOutstanding(): Unit = {
    assertTrue(channelManager.generateRequests().isEmpty)
  }

  @Test
  def shouldGenerateRequestPerPartitionPerBroker(): Unit = {
    mockCache()

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Some(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Some(broker2))

    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds(), true))
    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata2, txnMetadata2.prepareComplete(time.milliseconds(), true))

    assertEquals(2, channelManager.numTxnsWithPendingMarkers)
    assertEquals(2, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition2))
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition2))

    val expectedBroker1Request = new WriteTxnMarkersRequest.Builder(ApiKeys.WRITE_TXN_MARKERS.latestVersion(),
      asList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, asList(partition1)),
        new WriteTxnMarkersRequest.TxnMarkerEntry(producerId2, producerEpoch, coordinatorEpoch, txnResult, asList(partition1)))).build()
    val expectedBroker2Request = new WriteTxnMarkersRequest.Builder(ApiKeys.WRITE_TXN_MARKERS.latestVersion(),
      asList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, asList(partition2)))).build()

    val requests: Map[Node, WriteTxnMarkersRequest] = channelManager.generateRequests().asScala.map { handler =>
      (handler.destination, handler.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build())
    }.toMap

    assertEquals(Map(broker1 -> expectedBroker1Request, broker2 -> expectedBroker2Request), requests)
    assertTrue(channelManager.generateRequests().isEmpty)
  }

  @Test
  def shouldSkipSendMarkersWhenLeaderNotFound(): Unit = {
    mockCache()

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(None)
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Some(broker2))

    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds(), true))

    assertEquals(1, channelManager.numTxnsWithPendingMarkers)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers)
    assertTrue(channelManager.queueForBroker(broker1.id).isEmpty)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition2))
  }

  @Test
  def shouldSaveForLaterWhenLeaderUnknownButNotAvailable(): Unit = {
    mockCache()

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Some(Node.noNode))
      .thenReturn(Some(Node.noNode))
      .thenReturn(Some(Node.noNode))
      .thenReturn(Some(Node.noNode))
      .thenReturn(Some(broker1))
      .thenReturn(Some(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Some(broker2))

    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds(), true))
    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata2, txnMetadata2.prepareComplete(time.milliseconds(), true))

    assertEquals(2, channelManager.numTxnsWithPendingMarkers)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers)
    assertTrue(channelManager.queueForBroker(broker1.id).isEmpty)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition2))
    assertEquals(2, channelManager.queueForUnknownBroker.totalNumMarkers)
    assertEquals(1, channelManager.queueForUnknownBroker.totalNumMarkers(txnTopicPartition1))
    assertEquals(1, channelManager.queueForUnknownBroker.totalNumMarkers(txnTopicPartition2))

    val expectedBroker1Request = new WriteTxnMarkersRequest.Builder(ApiKeys.WRITE_TXN_MARKERS.latestVersion(),
      asList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, asList(partition1)),
        new WriteTxnMarkersRequest.TxnMarkerEntry(producerId2, producerEpoch, coordinatorEpoch, txnResult, asList(partition1)))).build()
    val expectedBroker2Request = new WriteTxnMarkersRequest.Builder(ApiKeys.WRITE_TXN_MARKERS.latestVersion(),
      asList(new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, asList(partition2)))).build()

    val firstDrainedRequests: Map[Node, WriteTxnMarkersRequest] = channelManager.generateRequests().asScala.map { handler =>
      (handler.destination, handler.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build())
    }.toMap

    assertEquals(Map(broker2 -> expectedBroker2Request), firstDrainedRequests)

    val secondDrainedRequests: Map[Node, WriteTxnMarkersRequest] = channelManager.generateRequests().asScala.map { handler =>
      (handler.destination, handler.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build())
    }.toMap

    assertEquals(Map(broker1 -> expectedBroker1Request), secondDrainedRequests)
  }

  @Test
  def shouldRemoveMarkersForTxnPartitionWhenPartitionEmigrated(): Unit = {
    mockCache()

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Some(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Some(broker2))

    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds(), true))
    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata2, txnMetadata2.prepareComplete(time.milliseconds(), true))

    assertEquals(2, channelManager.numTxnsWithPendingMarkers)
    assertEquals(2, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition2))
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition2))

    channelManager.removeMarkersForTxnTopicPartition(txnTopicPartition1)

    assertEquals(1, channelManager.numTxnsWithPendingMarkers)
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

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Some(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Some(broker2))

    val txnTransitionMetadata2 = txnMetadata2.prepareComplete(time.milliseconds(), true)

    when(txnStateManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitionMetadata2),
      capturedErrorsCallback.capture(),
      any(),
      any()))
      .thenAnswer(_ => {
        txnMetadata2.completeTransitionTo(txnTransitionMetadata2)
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      })

    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata2, txnTransitionMetadata2)

    val requestAndHandlers: Iterable[RequestAndCompletionHandler] = channelManager.generateRequests().asScala

    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.NONE))
    for (requestAndHandler <- requestAndHandlers) {
      requestAndHandler.handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.WRITE_TXN_MARKERS, 0, "client", 1),
        null, null, 0, 0, false, null, null, response))
    }

    verify(txnStateManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitionMetadata2),
      capturedErrorsCallback.capture(),
      any(),
      any())

    assertEquals(0, channelManager.numTxnsWithPendingMarkers)
    assertEquals(0, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(None, txnMetadata2.pendingState)
    assertEquals(CompleteCommit, txnMetadata2.state)
  }

  @Test
  def shouldAbortAppendToLogOnEndTxnWhenNotCoordinatorError(): Unit = {
    mockCache()

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Some(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Some(broker2))

    val txnTransitionMetadata2 = txnMetadata2.prepareComplete(time.milliseconds(), true)

    when(txnStateManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitionMetadata2),
      capturedErrorsCallback.capture(),
      any(),
      any()))
      .thenAnswer(_ => {
        txnMetadata2.pendingState = None
        capturedErrorsCallback.getValue.apply(Errors.NOT_COORDINATOR)
      })

    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata2, txnTransitionMetadata2)

    val requestAndHandlers: Iterable[RequestAndCompletionHandler] = channelManager.generateRequests().asScala

    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.NONE))
    for (requestAndHandler <- requestAndHandlers) {
      requestAndHandler.handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.WRITE_TXN_MARKERS, 0, "client", 1),
        null, null, 0, 0, false, null, null, response))
    }

    verify(txnStateManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitionMetadata2),
      capturedErrorsCallback.capture(),
      any(),
      any())

    assertEquals(0, channelManager.numTxnsWithPendingMarkers)
    assertEquals(0, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(None, txnMetadata2.pendingState)
    assertEquals(PrepareCommit, txnMetadata2.state)
  }

  @Test
  def shouldRetryAppendToLogOnEndTxnWhenCoordinatorNotAvailableError(): Unit = {
    mockCache()

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Some(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Some(broker2))

    val txnTransitionMetadata2 = txnMetadata2.prepareComplete(time.milliseconds(), true)

    when(txnStateManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitionMetadata2),
      capturedErrorsCallback.capture(),
      any(),
      any()))
      .thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.COORDINATOR_NOT_AVAILABLE))
      .thenAnswer(_ => {
        txnMetadata2.completeTransitionTo(txnTransitionMetadata2)
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      })

    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata2, txnTransitionMetadata2)

    val requestAndHandlers: Iterable[RequestAndCompletionHandler] = channelManager.generateRequests().asScala

    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.NONE))
    for (requestAndHandler <- requestAndHandlers) {
      requestAndHandler.handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.WRITE_TXN_MARKERS, 0, "client", 1),
        null, null, 0, 0, false, null, null, response))
    }

    // call this again so that append log will be retried
    channelManager.generateRequests()

    verify(txnStateManager, times(2)).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitionMetadata2),
      capturedErrorsCallback.capture(),
      any(),
      any())

    assertEquals(0, channelManager.numTxnsWithPendingMarkers)
    assertEquals(0, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(None, txnMetadata2.pendingState)
    assertEquals(CompleteCommit, txnMetadata2.state)
  }

  private def createPidErrorMap(errors: Errors): util.HashMap[java.lang.Long, util.Map[TopicPartition, Errors]] = {
    val pidMap = new java.util.HashMap[java.lang.Long, java.util.Map[TopicPartition, Errors]]()
    val errorsMap = new java.util.HashMap[TopicPartition, Errors]()
    errorsMap.put(partition1, errors)
    pidMap.put(producerId2, errorsMap)
    pidMap
  }

  @Test
  def shouldCreateMetricsOnStarting(): Unit = {
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala

    assertEquals(1, metrics.count { case (k, _) =>
      k.getMBeanName == "kafka.coordinator.transaction:type=TransactionMarkerChannelManager,name=UnknownDestinationQueueSize"
    })
    assertEquals(1, metrics.count { case (k, _) =>
      k.getMBeanName == "kafka.coordinator.transaction:type=TransactionMarkerChannelManager,name=LogAppendRetryQueueSize"
    })
  }
}
