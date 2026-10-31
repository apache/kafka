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
import java.util.Optional
import java.util.concurrent.{Callable, Executors, Future}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.internal.RecordBatch
import org.apache.kafka.common.requests.{RequestHeader, TransactionResult, WriteTxnMarkersRequest, WriteTxnMarkersResponse}
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.utils.internals.LogContext
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.{Node, TopicPartition, Uuid}
import org.apache.kafka.common.metadata.{PartitionChangeRecord, PartitionRecord, RegisterBrokerRecord, TopicRecord}
import org.apache.kafka.common.metadata.RegisterBrokerRecord.{BrokerEndpoint, BrokerEndpointCollection}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.coordinator.transaction.{CoordinatorEpochAndTxnMetadata, TransactionMetadata, TransactionState}
import org.apache.kafka.metadata.{LeaderConstants, MetadataCache, MetadataCacheFixtures}
import org.apache.kafka.server.common.{MetadataVersion, TransactionVersion}
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics}
import org.apache.kafka.server.util.{InterBrokerSendThread, RequestAndCompletionHandler}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, ValueSource}
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{clearInvocations, mock, mockConstruction, times, verify, verifyNoMoreInteractions, when}

import scala.jdk.CollectionConverters._
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
  private val coordinatorEpoch2 = 1
  private val txnTimeoutMs = 0
  private val txnResult = TransactionResult.COMMIT
  private val txnMetadata1 = new TransactionMetadata(transactionalId1, producerId1, producerId1, RecordBatch.NO_PRODUCER_ID,
    producerEpoch, lastProducerEpoch, txnTimeoutMs, TransactionState.PREPARE_COMMIT, util.Set.of(partition1, partition2), 0L, 0L, TransactionVersion.TV_2)
  private val txnMetadata2 = new TransactionMetadata(transactionalId2, producerId2, producerId2, RecordBatch.NO_PRODUCER_ID,
    producerEpoch, lastProducerEpoch, txnTimeoutMs, TransactionState.PREPARE_COMMIT, util.Set.of(partition1), 0L, 0L, TransactionVersion.TV_2)

  private val capturedErrorsCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])
  private val time = new MockTime

  private val channelManager = new TransactionMarkerChannelManager(
    KafkaConfig.fromProps(TestUtils.createBrokerConfig(1)),
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
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))))
    when(txnStateManager.getTransactionState(ArgumentMatchers.eq(transactionalId2)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata2))))
    when(metadataCache.metadataVersion())
      .thenReturn(MetadataVersion.latestProduction())
  }

  @Test
  def testRemoveMetricsOnClose(): Unit = {
    val mockMetricsGroupCtor = mockConstruction(classOf[KafkaMetricsGroup])
    try {
      val transactionMarkerChannelManager = new TransactionMarkerChannelManager(
        KafkaConfig.fromProps(TestUtils.createBrokerConfig(1)),
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

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def shouldOnlyWriteTxnCompletionOnce(isTransactionV2Enabled: Boolean): Unit = {
    mockCache()

    // Adjust txn metadata based on the transaction version.
    adjustTransactionMetadataForVersion(isTransactionV2Enabled, txnMetadata2)

    val expectedTransition = txnMetadata2.prepareComplete(time.milliseconds())

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Optional.of(broker1))

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
    try {
      txnMetadata2.inLock(() => {
        addMarkerFuture = executor.submit((() => {
          Try(channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult,
            txnMetadata2, expectedTransition))
        }): Callable[Try[Unit]])

        val header = new RequestHeader(ApiKeys.WRITE_TXN_MARKERS, 0, "client", 1)
        val response = new WriteTxnMarkersResponse(
          util.Map.of(producerId2: java.lang.Long, util.Map.of(partition1, Errors.NONE)))
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
      })
    } finally {
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
  def shouldNotLoseTxnCompletionAfterLoad(): Unit = {
    mockCache()

    val expectedTransition = txnMetadata2.prepareComplete(time.milliseconds())

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Optional.of(broker1))

    // Build a successful client response.
    val header = new RequestHeader(ApiKeys.WRITE_TXN_MARKERS, 0, "client", 1)
    val successfulResponse = new WriteTxnMarkersResponse(
      util.Map.of(producerId2: java.lang.Long, util.Map.of(partition1, Errors.NONE)))
    val successfulClientResponse = new ClientResponse(header, null, null,
      time.milliseconds(), time.milliseconds(), false, null, null,
      successfulResponse)

    // Build a disconnected client response.
    val disconnectedClientResponse = new ClientResponse(header, null, null,
      time.milliseconds(), time.milliseconds(), true, null, null,
      null)

    // Test matrix to cover various scenarios:
    val clientResponses = Seq(successfulClientResponse, disconnectedClientResponse)
    val getTransactionStateResponses = Seq(
      // NOT_COORDINATOR error case
      Left(Errors.NOT_COORDINATOR),
      // COORDINATOR_LOAD_IN_PROGRESS
      Left(Errors.COORDINATOR_LOAD_IN_PROGRESS),
      // "Newly loaded" transaction state with the new epoch.
      Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch2, txnMetadata2)))
    )

    clientResponses.foreach { clientResponse =>
      getTransactionStateResponses.foreach { getTransactionStateResponse =>
        // Reset data from previous iteration.
        txnMetadata2.topicPartitions.add(partition1)
        clearInvocations(txnStateManager)
        // Send out markers for a transaction before load.
        channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult,
          txnMetadata2, expectedTransition)

        // Drain the marker to make it "in-flight".
        val requests1 = channelManager.generateRequests().asScala
        assertEquals(1, requests1.size)

        // Simulate a partition load:
        // 1. Remove the markers from the channel manager.
        // 2. Simulate the corresponding test case scenario.
        // 3. Add the markers back to the channel manager.
        channelManager.removeMarkersForTxnTopicPartition(txnTopicPartition2)
        when(txnStateManager.getTransactionState(ArgumentMatchers.eq(transactionalId2)))
          .thenReturn(getTransactionStateResponse)
        channelManager.addTxnMarkersToSend(coordinatorEpoch2, txnResult,
          txnMetadata2, expectedTransition)

        // Complete the marker from the previous epoch.
        requests1.head.handler.onComplete(clientResponse)

        // Now drain and complete the marker from the new epoch.
        when(txnStateManager.getTransactionState(ArgumentMatchers.eq(transactionalId2)))
          .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch2, txnMetadata2))))
        val requests2 = channelManager.generateRequests().asScala
        assertEquals(1, requests2.size)
        requests2.head.handler.onComplete(successfulClientResponse)

        verify(txnStateManager).appendTransactionToLog(
          ArgumentMatchers.eq(transactionalId2),
          ArgumentMatchers.eq(coordinatorEpoch2),
          ArgumentMatchers.eq(expectedTransition),
          capturedErrorsCallback.capture(),
          any(),
          any())
      }
    }
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
    ).thenReturn(Optional.of(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Optional.of(broker2))

    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds()))
    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata2, txnMetadata2.prepareComplete(time.milliseconds()))

    assertEquals(2, channelManager.numTxnsWithPendingMarkers)
    assertEquals(2, channelManager.queueForBroker(broker1.id).get.totalNumMarkers)
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(1, channelManager.queueForBroker(broker1.id).get.totalNumMarkers(txnTopicPartition2))
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition2))

    val expectedBroker1Request = new WriteTxnMarkersRequest.Builder(
      util.List.of(
        new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, util.List.of(partition1), TransactionVersion.TV_2.featureLevel()),
        new WriteTxnMarkersRequest.TxnMarkerEntry(producerId2, producerEpoch, coordinatorEpoch, txnResult, util.List.of(partition1), TransactionVersion.TV_2.featureLevel())
      )
    ).build()
    val expectedBroker2Request = new WriteTxnMarkersRequest.Builder(
      util.List.of(
        new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, util.List.of(partition2), TransactionVersion.TV_2.featureLevel())
      )
    ).build()

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
    ).thenReturn(Optional.empty())
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Optional.of(broker2))

    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds()))

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
    ).thenReturn(Optional.of(Node.noNode))
      .thenReturn(Optional.of(Node.noNode))
      .thenReturn(Optional.of(Node.noNode))
      .thenReturn(Optional.of(Node.noNode))
      .thenReturn(Optional.of(broker1))
      .thenReturn(Optional.of(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Optional.of(broker2))

    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds()))
    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata2, txnMetadata2.prepareComplete(time.milliseconds()))

    assertEquals(2, channelManager.numTxnsWithPendingMarkers)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers)
    assertTrue(channelManager.queueForBroker(broker1.id).isEmpty)
    assertEquals(1, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition1))
    assertEquals(0, channelManager.queueForBroker(broker2.id).get.totalNumMarkers(txnTopicPartition2))
    assertEquals(2, channelManager.queueForUnknownBroker.totalNumMarkers)
    assertEquals(1, channelManager.queueForUnknownBroker.totalNumMarkers(txnTopicPartition1))
    assertEquals(1, channelManager.queueForUnknownBroker.totalNumMarkers(txnTopicPartition2))

    val expectedBroker1Request = new WriteTxnMarkersRequest.Builder(
      util.List.of(
        new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, util.List.of(partition1), TransactionVersion.TV_2.featureLevel()),
        new WriteTxnMarkersRequest.TxnMarkerEntry(producerId2, producerEpoch, coordinatorEpoch, txnResult, util.List.of(partition1), TransactionVersion.TV_2.featureLevel())
      )
    ).build()
    val expectedBroker2Request = new WriteTxnMarkersRequest.Builder(
      util.List.of(
        new WriteTxnMarkersRequest.TxnMarkerEntry(producerId1, producerEpoch, coordinatorEpoch, txnResult, util.List.of(partition2), TransactionVersion.TV_2.featureLevel())
      )
    ).build()

    val firstDrainedRequests: Map[Node, WriteTxnMarkersRequest] = channelManager.generateRequests().asScala.map { handler =>
      (handler.destination, handler.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build())
    }.toMap

    assertEquals(Map(broker2 -> expectedBroker2Request), firstDrainedRequests)

    val secondDrainedRequests: Map[Node, WriteTxnMarkersRequest] = channelManager.generateRequests().asScala.map { handler =>
      (handler.destination, handler.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build())
    }.toMap

    assertEquals(Map(broker1 -> expectedBroker1Request), secondDrainedRequests)
  }

  @ParameterizedTest
  @EnumSource(classOf[TransactionResult])
  def shouldRetryMarkersUntilLeaderEndpointIsAvailable(result: TransactionResult): Unit = {
    mockCache()
    txnMetadata2.state(if (result == TransactionResult.COMMIT) TransactionState.PREPARE_COMMIT else TransactionState.PREPARE_ABORT)
    val expectedTransition = txnMetadata2.prepareComplete(time.milliseconds())
    val cache = MetadataCacheFixtures.createCache()
    val topicId = Uuid.randomUuid()
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    MetadataCacheFixtures.updateCache(cache, util.List.of[ApiMessage](
      new TopicRecord().setName(partition1.topic).setTopicId(topicId),
      new PartitionRecord()
        .setTopicId(topicId)
        .setPartitionId(partition1.partition)
        .setLeader(LeaderConstants.NO_LEADER)
        .setReplicas(util.List.of[Integer](broker1.id))
        .setIsr(util.List.of[Integer]())
    ))

    val manager = new TransactionMarkerChannelManager(
      KafkaConfig.fromProps(TestUtils.createBrokerConfig(1)), cache, networkClient, txnStateManager, time)
    try {
      manager.addTxnMarkersToSend(coordinatorEpoch, result, txnMetadata2, expectedTransition)

      // A known partition without a leader must retain its marker and remain pending.
      assertTrue(manager.generateRequests().isEmpty)
      assertEquals(1, manager.queueForUnknownBroker.totalNumMarkers)
      assertEquals(1, manager.numTxnsWithPendingMarkers)
      assertTrue(txnMetadata2.topicPartitions.contains(partition1))
      verify(txnStateManager, times(0)).appendTransactionToLog(any(), any(), any(), any(), any(), any())

      MetadataCacheFixtures.updateCache(cache, util.List.of[ApiMessage](
        new PartitionChangeRecord()
          .setTopicId(topicId)
          .setPartitionId(partition1.partition)
          .setLeader(broker1.id)
          .setIsr(util.List.of[Integer](broker1.id))
      ))

      // Knowing the leader ID is insufficient while its broker registration is missing.
      assertTrue(manager.generateRequests().isEmpty)
      assertEquals(1, manager.queueForUnknownBroker.totalNumMarkers)
      assertEquals(1, manager.numTxnsWithPendingMarkers)
      assertTrue(txnMetadata2.topicPartitions.contains(partition1))
      verify(txnStateManager, times(0)).appendTransactionToLog(any(), any(), any(), any(), any(), any())

      MetadataCacheFixtures.updateCache(cache, util.List.of[ApiMessage](
        new RegisterBrokerRecord()
          .setBrokerId(broker1.id)
          .setRack("rack1")
          .setFenced(false)
          .setEndPoints(new BrokerEndpointCollection(util.List.of(
            new BrokerEndpoint()
              .setHost(broker1.host)
              .setPort(broker1.port)
              .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
              .setName(listenerName.value)
          )))
      ))

      val requests = manager.generateRequests().asScala.toList
      assertEquals(1, requests.size)
      assertEquals(new Node(broker1.id, broker1.host, broker1.port, "rack1"), requests.head.destination)
      val request = requests.head.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build()
      assertEquals(util.List.of(new WriteTxnMarkersRequest.TxnMarkerEntry(
        producerId2, producerEpoch, coordinatorEpoch, result, util.List.of(partition1), TransactionVersion.TV_2.featureLevel()
      )), request.markers())
      assertEquals(0, manager.queueForUnknownBroker.totalNumMarkers)
      assertEquals(1, manager.numTxnsWithPendingMarkers)
      verify(txnStateManager, times(0)).appendTransactionToLog(any(), any(), any(), any(), any(), any())

      val response = new WriteTxnMarkersResponse(
        util.Map.of(producerId2: java.lang.Long, util.Map.of(partition1, Errors.NONE)))
      requests.head.handler.onComplete(new ClientResponse(
        new RequestHeader(ApiKeys.WRITE_TXN_MARKERS, 0, "client", 1), null, null,
        time.milliseconds(), time.milliseconds(), false, null, null, response))

      assertTrue(txnMetadata2.topicPartitions.isEmpty)
      assertEquals(0, manager.numTxnsWithPendingMarkers)
      verify(txnStateManager).appendTransactionToLog(
        ArgumentMatchers.eq(transactionalId2), ArgumentMatchers.eq(coordinatorEpoch),
        ArgumentMatchers.eq(expectedTransition), any(), any(), any())
      assertTrue(manager.generateRequests().isEmpty)
    } finally {
      manager.shutdown()
    }
  }

  @Test
  def shouldRemoveMarkersForTxnPartitionWhenPartitionEmigrated(): Unit = {
    mockCache()

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Optional.of(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Optional.of(broker2))

    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata1, txnMetadata1.prepareComplete(time.milliseconds()))
    channelManager.addTxnMarkersToSend(coordinatorEpoch, txnResult, txnMetadata2, txnMetadata2.prepareComplete(time.milliseconds()))

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

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def shouldCompleteAppendToLogOnEndTxnWhenSendMarkersSucceed(isTransactionV2Enabled : Boolean): Unit = {
    mockCache()

    // Adjust txn metadata based on the transaction version.
    adjustTransactionMetadataForVersion(isTransactionV2Enabled, txnMetadata2)

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Optional.of(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Optional.of(broker2))

    val txnTransitionMetadata2 = txnMetadata2.prepareComplete(time.milliseconds())

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
    assertEquals(Optional.empty(), txnMetadata2.pendingState)
    assertEquals(TransactionState.COMPLETE_COMMIT, txnMetadata2.state)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[TransactionResult], names = Array("COMMIT", "ABORT"))
  def shouldCompleteTxnWithoutSendingMarkersWhenNoPartitions(result: TransactionResult): Unit = {
    // With transaction V2, ending a transaction with no partitions writes PrepareCommit or PrepareAbort with no partitions.
    val (prepareState, completeState) =
      if (result == TransactionResult.COMMIT) (TransactionState.PREPARE_COMMIT, TransactionState.COMPLETE_COMMIT)
      else (TransactionState.PREPARE_ABORT, TransactionState.COMPLETE_ABORT)
    val txnMetadata = new TransactionMetadata(transactionalId1, producerId1, producerId1, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, lastProducerEpoch, txnTimeoutMs, prepareState, util.Set.of, 0L, 0L, TransactionVersion.TV_2)
    when(txnStateManager.getTransactionState(ArgumentMatchers.eq(transactionalId1)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val txnTransitionMetadata = txnMetadata.prepareComplete(time.milliseconds())

    when(txnStateManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId1),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitionMetadata),
      capturedErrorsCallback.capture(),
      any(),
      any()))
      .thenAnswer(_ => {
        txnMetadata.completeTransitionTo(txnTransitionMetadata)
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      })

    channelManager.addTxnMarkersToSend(coordinatorEpoch, result, txnMetadata, txnTransitionMetadata)

    // The completion is written right away since there are no markers to wait for.
    verify(txnStateManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId1),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitionMetadata),
      any(),
      any(),
      any())
    assertTrue(channelManager.generateRequests().isEmpty)
    assertEquals(0, channelManager.numTxnsWithPendingMarkers)
    assertEquals(Optional.empty(), txnMetadata.pendingState)
    assertEquals(completeState, txnMetadata.state)
  }

  @Test
  def shouldAbortAppendToLogOnEndTxnWhenNotCoordinatorError(): Unit = {
    mockCache()

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Optional.of(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Optional.of(broker2))

    val txnTransitionMetadata2 = txnMetadata2.prepareComplete(time.milliseconds())

    when(txnStateManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitionMetadata2),
      capturedErrorsCallback.capture(),
      any(),
      any()))
      .thenAnswer(_ => {
        txnMetadata2.pendingState(util.Optional.empty())
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
    assertEquals(Optional.empty(), txnMetadata2.pendingState)
    assertEquals(TransactionState.PREPARE_COMMIT, txnMetadata2.state)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def shouldRetryAppendToLogOnEndTxnWhenCoordinatorNotAvailableError(isTransactionV2Enabled: Boolean): Unit = {
    mockCache()

    // Adjust txn metadata based on the transaction version.
    adjustTransactionMetadataForVersion(isTransactionV2Enabled, txnMetadata2)

    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition1.topic),
      ArgumentMatchers.eq(partition1.partition),
      any())
    ).thenReturn(Optional.of(broker1))
    when(metadataCache.getPartitionLeaderEndpoint(
      ArgumentMatchers.eq(partition2.topic),
      ArgumentMatchers.eq(partition2.partition),
      any())
    ).thenReturn(Optional.of(broker2))

    val txnTransitionMetadata2 = txnMetadata2.prepareComplete(time.milliseconds())

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
    assertEquals(Optional.empty(), txnMetadata2.pendingState)
    assertEquals(TransactionState.COMPLETE_COMMIT, txnMetadata2.state)
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

  @Test
  def shouldEnableApiVersionDiscoveryInFactoryMethod(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val metrics = new Metrics()
    val logContext = new LogContext()
    try {
      val channelManager = TransactionMarkerChannelManager(
        config,
        metrics,
        metadataCache,
        txnStateManager,
        time,
        logContext
      )
      try {
        val field = classOf[InterBrokerSendThread].getDeclaredField("networkClient")
        field.setAccessible(true)
        val client = field.get(channelManager).asInstanceOf[NetworkClient]
        assertTrue(client.discoverBrokerVersions(),
          "TransactionMarkerChannelManager should enable API version discovery to " +
            "ensure compatibility during rolling upgrades")
      } finally {
        channelManager.shutdown()
      }
    } finally {
      metrics.close()
    }
  }

  /**
   * Adjusts the transaction metadata based on the transaction version.
   * When transaction V2 is enabled, the producer epoch is incremented
   * by 1 after every prepareCommit/prepareAbort.
   *
   * Use this method to adjust the transaction metadata before
   * prepareComplete is called with TV2.
   *
   * @param isTransactionV2Enabled    Whether Transaction Version 2 (TV2) is enabled.
   * @param txnMetadata               The transaction metadata to be adjusted.
   */
  private def adjustTransactionMetadataForVersion(
    isTransactionV2Enabled: Boolean,
    txnMetadata: TransactionMetadata
  ): Unit = {
    if (isTransactionV2Enabled) {
      txnMetadata.clientTransactionVersion(TransactionVersion.TV_2)
      txnMetadata.setProducerEpoch((producerEpoch + 1).toShort)
      txnMetadata.setLastProducerEpoch(producerEpoch)
    } else {
      txnMetadata.clientTransactionVersion(TransactionVersion.TV_1)
    }
  }
}
