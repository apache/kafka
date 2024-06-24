/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import com.yammer.metrics.core.{Histogram, Meter}
import kafka.utils.Implicits.MapExtensionMethods
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.errors.{AuthenticationException, SaslAuthenticationException, UnsupportedVersionException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.{AddPartitionsToTxnTopic, AddPartitionsToTxnTopicCollection, AddPartitionsToTxnTransaction, AddPartitionsToTxnTransactionCollection}
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractResponse, AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, MetadataResponse}
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.RequestAndCompletionHandler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyLong, anyString}
import org.mockito.MockedConstruction.Context
import org.mockito.Mockito.{mock, mockConstruction, times, verify, verifyNoMoreInteractions, when}

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class AddPartitionsToTxnManagerTest {
  private val networkClient: NetworkClient = mock(classOf[NetworkClient])
  private val metadataCache: MetadataCache = mock(classOf[MetadataCache])
  private val partitionFor: String => Int = mock(classOf[String => Int])

  private val time = new MockTime

  private var addPartitionsToTxnManager: AddPartitionsToTxnManager = _

  private val topic = "foo"
  private val topicPartitions = List(new TopicPartition(topic, 1), new TopicPartition(topic, 2), new TopicPartition(topic, 3))

  private val node0 = new Node(0, "host1", 0)
  private val node1 = new Node(1, "host2", 1)
  private val node2 = new Node(2, "host2", 2)

  private val transactionalId1 = "txn1"
  private val transactionalId2 = "txn2"
  private val transactionalId3 = "txn3"

  private val producerId1 = 0L
  private val producerId2 = 1L
  private val producerId3 = 2L

  private val authenticationErrorResponse = clientResponse(null, authException = new SaslAuthenticationException(""))
  private val versionMismatchResponse = clientResponse(null, mismatchException = new UnsupportedVersionException(""))
  private val disconnectedResponse = clientResponse(null, disconnected = true)
  private val transactionSupportedOperation = genericError

  private val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:2181"))

  @BeforeEach
  def setup(): Unit = {
    addPartitionsToTxnManager = new AddPartitionsToTxnManager(
      config,
      networkClient,
      metadataCache,
      partitionFor,
      time
    )
  }

  @AfterEach
  def teardown(): Unit = {
    addPartitionsToTxnManager.shutdown()
  }

  private def setErrors(errors: mutable.Map[TopicPartition, Errors])(callbackErrors: Map[TopicPartition, Errors]): Unit = {
    callbackErrors.forKeyValue(errors.put)
  }

  @Test
  def testAddTxnData(): Unit = {
    when(partitionFor.apply(transactionalId1)).thenReturn(0)
    when(partitionFor.apply(transactionalId2)).thenReturn(1)
    when(partitionFor.apply(transactionalId3)).thenReturn(0)
    mockTransactionStateMetadata(0, 0, Some(node0))
    mockTransactionStateMetadata(1, 1, Some(node1))

    val transaction1Errors = mutable.Map[TopicPartition, Errors]()
    val transaction2Errors = mutable.Map[TopicPartition, Errors]()
    val transaction3Errors = mutable.Map[TopicPartition, Errors]()

    addPartitionsToTxnManager.verifyTransaction(transactionalId1, producerId1, producerEpoch = 0, topicPartitions, setErrors(transaction1Errors), transactionSupportedOperation)
    addPartitionsToTxnManager.verifyTransaction(transactionalId2, producerId2, producerEpoch = 0, topicPartitions, setErrors(transaction2Errors), transactionSupportedOperation)
    addPartitionsToTxnManager.verifyTransaction(transactionalId3, producerId3, producerEpoch = 0, topicPartitions, setErrors(transaction3Errors), transactionSupportedOperation)

    // We will try to add transaction1 3 more times (retries). One will have the same epoch, one will have a newer epoch, and one will have an older epoch than the new one we just added.
    val transaction1RetryWithSameEpochErrors = mutable.Map[TopicPartition, Errors]()
    val transaction1RetryWithNewerEpochErrors = mutable.Map[TopicPartition, Errors]()
    val transaction1RetryWithOldEpochErrors = mutable.Map[TopicPartition, Errors]()

    // Trying to add more transactional data for the same transactional ID, producer ID, and epoch should simply replace the old data and send a retriable response.
    addPartitionsToTxnManager.verifyTransaction(transactionalId1, producerId1, producerEpoch = 0, topicPartitions, setErrors(transaction1RetryWithSameEpochErrors), transactionSupportedOperation)
    val expectedNetworkErrors = topicPartitions.map(_ -> Errors.NETWORK_EXCEPTION).toMap
    assertEquals(expectedNetworkErrors, transaction1Errors)

    // Trying to add more transactional data for the same transactional ID and producer ID, but new epoch should replace the old data and send an error response for it.
    addPartitionsToTxnManager.verifyTransaction(transactionalId1, producerId1, producerEpoch = 1, topicPartitions, setErrors(transaction1RetryWithNewerEpochErrors), transactionSupportedOperation)
    val expectedEpochErrors = topicPartitions.map(_ -> Errors.INVALID_PRODUCER_EPOCH).toMap
    assertEquals(expectedEpochErrors, transaction1RetryWithSameEpochErrors)

    // Trying to add more transactional data for the same transactional ID and producer ID, but an older epoch should immediately return with error and keep the old data queued to send.
    addPartitionsToTxnManager.verifyTransaction(transactionalId1, producerId1, producerEpoch = 0, topicPartitions, setErrors(transaction1RetryWithOldEpochErrors), transactionSupportedOperation)
    assertEquals(expectedEpochErrors, transaction1RetryWithOldEpochErrors)

    val requestsAndHandlers = addPartitionsToTxnManager.generateRequests().asScala
    requestsAndHandlers.foreach { requestAndHandler =>
      if (requestAndHandler.destination == node0) {
        assertEquals(time.milliseconds(), requestAndHandler.creationTimeMs)
        assertEquals(
          AddPartitionsToTxnRequest.Builder.forBroker(
            new AddPartitionsToTxnTransactionCollection(Seq(
              transactionData(transactionalId3, producerId3),
              transactionData(transactionalId1, producerId1, producerEpoch = 1)
            ).iterator.asJava)
          ).data,
          requestAndHandler.request.asInstanceOf[AddPartitionsToTxnRequest.Builder].data // insertion order
        )
      } else {
        verifyRequest(node1, transactionalId2, producerId2, requestAndHandler)
      }
    }
  }

  @Test
  def testGenerateRequests(): Unit = {
    when(partitionFor.apply(transactionalId1)).thenReturn(0)
    when(partitionFor.apply(transactionalId2)).thenReturn(1)
    when(partitionFor.apply(transactionalId3)).thenReturn(2)
    mockTransactionStateMetadata(0, 0, Some(node0))
    mockTransactionStateMetadata(1, 1, Some(node1))
    mockTransactionStateMetadata(2, 2, Some(node2))

    val transactionErrors = mutable.Map[TopicPartition, Errors]()

    addPartitionsToTxnManager.verifyTransaction(transactionalId1, producerId1, producerEpoch = 0, topicPartitions, setErrors(transactionErrors), transactionSupportedOperation)
    addPartitionsToTxnManager.verifyTransaction(transactionalId2, producerId2, producerEpoch = 0, topicPartitions, setErrors(transactionErrors), transactionSupportedOperation)

    val requestsAndHandlers = addPartitionsToTxnManager.generateRequests().asScala
    assertEquals(2, requestsAndHandlers.size)
    // Note: handlers are tested in testAddPartitionsToTxnHandlerErrorHandling
    requestsAndHandlers.foreach { requestAndHandler =>
      if (requestAndHandler.destination == node0) {
        verifyRequest(node0, transactionalId1, producerId1, requestAndHandler)
      } else {
        verifyRequest(node1, transactionalId2, producerId2, requestAndHandler)
      }
    }

    addPartitionsToTxnManager.verifyTransaction(transactionalId2, producerId2, producerEpoch = 0, topicPartitions, setErrors(transactionErrors), transactionSupportedOperation)
    addPartitionsToTxnManager.verifyTransaction(transactionalId3, producerId3, producerEpoch = 0, topicPartitions, setErrors(transactionErrors), transactionSupportedOperation)

    // Test creationTimeMs increases too.
    time.sleep(10)

    val requestsAndHandlers2 = addPartitionsToTxnManager.generateRequests().asScala
    // The request for node1 should not be added because one request is already inflight.
    assertEquals(1, requestsAndHandlers2.size)
    requestsAndHandlers2.foreach { requestAndHandler =>
      verifyRequest(node2, transactionalId3, producerId3, requestAndHandler)
    }

    // Complete the request for node1 so the new one can go through.
    requestsAndHandlers.filter(_.destination == node1).head.handler.onComplete(authenticationErrorResponse)
    val requestsAndHandlers3 = addPartitionsToTxnManager.generateRequests().asScala
    assertEquals(1, requestsAndHandlers3.size)
    requestsAndHandlers3.foreach { requestAndHandler =>
      verifyRequest(node1, transactionalId2, producerId2, requestAndHandler)
    }
  }

  @Test
  def testTransactionCoordinatorResolution(): Unit = {
    when(partitionFor.apply(transactionalId1)).thenReturn(0)

    def checkError(): Unit = {
      val errors = mutable.Map[TopicPartition, Errors]()

      addPartitionsToTxnManager.verifyTransaction(
        transactionalId1,
        producerId1,
        producerEpoch = 0,
        topicPartitions,
        setErrors(errors),
        transactionSupportedOperation
      )

      assertEquals(topicPartitions.map(tp => tp -> Errors.COORDINATOR_NOT_AVAILABLE).toMap, errors)
    }

    // The transaction state topic does not exist.
    when(metadataCache.getPartitionInfo(Topic.TRANSACTION_STATE_TOPIC_NAME, 0))
      .thenReturn(Option.empty)
    checkError()

    // The partition has no leader.
    mockTransactionStateMetadata(0, -1, Option.empty)
    checkError()

    // The leader is not available.
    mockTransactionStateMetadata(0, 0, Option.empty)
    checkError()
  }

  @Test
  def testAddPartitionsToTxnHandlerErrorHandling(): Unit = {
    when(partitionFor.apply(transactionalId1)).thenReturn(0)
    when(partitionFor.apply(transactionalId2)).thenReturn(0)
    mockTransactionStateMetadata(0, 0, Some(node0))

    val transaction1Errors = mutable.Map[TopicPartition, Errors]()
    val transaction2Errors = mutable.Map[TopicPartition, Errors]()

    def addTransactionsToVerify(): Unit = {
      transaction1Errors.clear()
      transaction2Errors.clear()

      addPartitionsToTxnManager.verifyTransaction(transactionalId1, producerId1, producerEpoch = 0, topicPartitions, setErrors(transaction1Errors), transactionSupportedOperation)
      addPartitionsToTxnManager.verifyTransaction(transactionalId2, producerId2, producerEpoch = 0, topicPartitions, setErrors(transaction2Errors), transactionSupportedOperation)
    }

    def addTransactionsToVerifyRequestVersion(operationExpected: TransactionSupportedOperation): Unit = {
      transaction1Errors.clear()
      transaction2Errors.clear()

      addPartitionsToTxnManager.verifyTransaction(transactionalId1, producerId1, producerEpoch = 0, topicPartitions, setErrors(transaction1Errors), operationExpected)
      addPartitionsToTxnManager.verifyTransaction(transactionalId2, producerId2, producerEpoch = 0, topicPartitions, setErrors(transaction2Errors), operationExpected)
    }

    val expectedAuthErrors = topicPartitions.map(_ -> Errors.SASL_AUTHENTICATION_FAILED).toMap
    addTransactionsToVerify()
    receiveResponse(authenticationErrorResponse)
    assertEquals(expectedAuthErrors, transaction1Errors)
    assertEquals(expectedAuthErrors, transaction2Errors)

    // On version mismatch we ignore errors and keep handling.
    val expectedVersionMismatchErrors = mutable.HashMap[TopicPartition, Errors]()
    addTransactionsToVerify()
    receiveResponse(versionMismatchResponse)
    assertEquals(expectedVersionMismatchErrors, transaction1Errors)
    assertEquals(expectedVersionMismatchErrors, transaction2Errors)

    val expectedDisconnectedErrors = topicPartitions.map(_ -> Errors.NETWORK_EXCEPTION).toMap
    addTransactionsToVerify()
    receiveResponse(disconnectedResponse)
    assertEquals(expectedDisconnectedErrors, transaction1Errors)
    assertEquals(expectedDisconnectedErrors, transaction2Errors)

    val expectedTopLevelErrors = topicPartitions.map(_ -> Errors.INVALID_TXN_STATE).toMap
    val topLevelErrorAddPartitionsResponse = new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData().setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code()))
    val topLevelErrorResponse = clientResponse(topLevelErrorAddPartitionsResponse)
    addTransactionsToVerify()
    receiveResponse(topLevelErrorResponse)
    assertEquals(expectedTopLevelErrors, transaction1Errors)
    assertEquals(expectedTopLevelErrors, transaction2Errors)

    val preConvertedTransaction1Errors = topicPartitions.map(_ -> Errors.PRODUCER_FENCED).toMap
    val expectedTransaction1Errors = topicPartitions.map(_ -> Errors.INVALID_PRODUCER_EPOCH).toMap
    val preConvertedTransaction2Errors = Map(new TopicPartition("foo", 1) -> Errors.NONE,
      new TopicPartition("foo", 2) -> Errors.INVALID_TXN_STATE,
      new TopicPartition("foo", 3) -> Errors.NONE)
    val expectedTransaction2Errors = Map(new TopicPartition("foo", 2) -> Errors.INVALID_TXN_STATE)

    val transaction1ErrorResponse = AddPartitionsToTxnResponse.resultForTransaction(transactionalId1, preConvertedTransaction1Errors.asJava)
    val transaction2ErrorResponse = AddPartitionsToTxnResponse.resultForTransaction(transactionalId2, preConvertedTransaction2Errors.asJava)
    val mixedErrorsAddPartitionsResponse = new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData()
      .setResultsByTransaction(new AddPartitionsToTxnResultCollection(Seq(transaction1ErrorResponse, transaction2ErrorResponse).iterator.asJava)))
    val mixedErrorsResponse = clientResponse(mixedErrorsAddPartitionsResponse)

    addTransactionsToVerify()
    receiveResponse(mixedErrorsResponse)
    assertEquals(expectedTransaction1Errors, transaction1Errors)
    assertEquals(expectedTransaction2Errors, transaction2Errors)

    val preConvertedTransactionAbortableErrorsTxn1 = topicPartitions.map(_ -> Errors.TRANSACTION_ABORTABLE).toMap
    val preConvertedTransactionAbortableErrorsTxn2 = Map(new TopicPartition("foo", 1) -> Errors.NONE,
      new TopicPartition("foo", 2) -> Errors.TRANSACTION_ABORTABLE,
      new TopicPartition("foo", 3) -> Errors.NONE)
    val transactionAbortableErrorResponseTxn1 = AddPartitionsToTxnResponse.resultForTransaction(transactionalId1, preConvertedTransactionAbortableErrorsTxn1.asJava)
    val transactionAbortableErrorResponseTxn2 = AddPartitionsToTxnResponse.resultForTransaction(transactionalId2, preConvertedTransactionAbortableErrorsTxn2.asJava)
    val mixedErrorsAddPartitionsResponseAbortableError = new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData()
      .setResultsByTransaction(new AddPartitionsToTxnResultCollection(Seq(transactionAbortableErrorResponseTxn1, transactionAbortableErrorResponseTxn2).iterator.asJava)))
    val mixedAbortableErrorsResponse = clientResponse(mixedErrorsAddPartitionsResponseAbortableError)

    val expectedTransactionAbortableErrorsTxn1LowerVersion = topicPartitions.map(_ -> Errors.INVALID_TXN_STATE).toMap
    val expectedTransactionAbortableErrorsTxn2LowerVersion = Map(new TopicPartition("foo", 2) -> Errors.INVALID_TXN_STATE)

    val expectedTransactionAbortableErrorsTxn1HigherVersion = topicPartitions.map(_ -> Errors.TRANSACTION_ABORTABLE).toMap
    val expectedTransactionAbortableErrorsTxn2HigherVersion = Map(new TopicPartition("foo", 2) -> Errors.TRANSACTION_ABORTABLE)

    addTransactionsToVerifyRequestVersion(defaultError)
    receiveResponse(mixedAbortableErrorsResponse)
    assertEquals(expectedTransactionAbortableErrorsTxn1LowerVersion, transaction1Errors)
    assertEquals(expectedTransactionAbortableErrorsTxn2LowerVersion, transaction2Errors)

    addTransactionsToVerifyRequestVersion(genericError)
    receiveResponse(mixedAbortableErrorsResponse)
    assertEquals(expectedTransactionAbortableErrorsTxn1HigherVersion, transaction1Errors)
    assertEquals(expectedTransactionAbortableErrorsTxn2HigherVersion, transaction2Errors)
  }

  @Test
  def testAddPartitionsToTxnManagerMetrics(): Unit = {
    val startTime = time.milliseconds()
    val transactionErrors = mutable.Map[TopicPartition, Errors]()

    var maxVerificationTime: Long = 0
    val mockVerificationFailureMeter = mock(classOf[Meter])
    val mockVerificationTime = mock(classOf[Histogram])

    when(partitionFor.apply(transactionalId1)).thenReturn(0)
    when(partitionFor.apply(transactionalId2)).thenReturn(1)
    mockTransactionStateMetadata(0, 0, Some(node0))
    mockTransactionStateMetadata(1, 1, Some(node1))

    // Update max verification time when we see a higher verification time.
    when(mockVerificationTime.update(anyLong())).thenAnswer { invocation =>
      val newTime = invocation.getArgument(0).asInstanceOf[Long]
      if (newTime > maxVerificationTime)
        maxVerificationTime = newTime
    }

    val mockMetricsGroupCtor = mockConstruction(classOf[KafkaMetricsGroup], (mock: KafkaMetricsGroup, context: Context) => {
      when(mock.newMeter(ArgumentMatchers.eq(AddPartitionsToTxnManager.VerificationFailureRateMetricName), anyString(), any(classOf[TimeUnit]))).thenReturn(mockVerificationFailureMeter)
      when(mock.newHistogram(ArgumentMatchers.eq(AddPartitionsToTxnManager.VerificationTimeMsMetricName))).thenReturn(mockVerificationTime)
    })

    val addPartitionsManagerWithMockedMetrics = new AddPartitionsToTxnManager(
      config,
      networkClient,
      metadataCache,
      partitionFor,
      time
    )

    try {
      addPartitionsManagerWithMockedMetrics.verifyTransaction(transactionalId1, producerId1, producerEpoch = 0, topicPartitions, setErrors(transactionErrors), transactionSupportedOperation)
      addPartitionsManagerWithMockedMetrics.verifyTransaction(transactionalId2, producerId2, producerEpoch = 0, topicPartitions, setErrors(transactionErrors), transactionSupportedOperation)

      time.sleep(100)

      val requestsAndHandlers = addPartitionsManagerWithMockedMetrics.generateRequests()
      var requestsHandled = 0

      requestsAndHandlers.forEach { requestAndCompletionHandler =>
        time.sleep(100)
        requestAndCompletionHandler.handler.onComplete(authenticationErrorResponse)
        requestsHandled += 1
        verify(mockVerificationTime, times(requestsHandled)).update(anyLong())
        assertEquals(maxVerificationTime, time.milliseconds() - startTime)
        verify(mockVerificationFailureMeter, times(requestsHandled)).mark(3) // since there are 3 partitions
      }

      // shutdown the manager so that metrics are removed.
      addPartitionsManagerWithMockedMetrics.shutdown()

      val mockMetricsGroup = mockMetricsGroupCtor.constructed.get(0)

      verify(mockMetricsGroup).newMeter(ArgumentMatchers.eq(AddPartitionsToTxnManager.VerificationFailureRateMetricName), anyString(), any(classOf[TimeUnit]))
      verify(mockMetricsGroup).newHistogram(ArgumentMatchers.eq(AddPartitionsToTxnManager.VerificationTimeMsMetricName))
      verify(mockMetricsGroup).removeMetric(AddPartitionsToTxnManager.VerificationFailureRateMetricName)
      verify(mockMetricsGroup).removeMetric(AddPartitionsToTxnManager.VerificationTimeMsMetricName)

      // assert that we have verified all invocations on the metrics group.
      verifyNoMoreInteractions(mockMetricsGroup)
    } finally {
      if (mockMetricsGroupCtor != null) {
        mockMetricsGroupCtor.close()
      }
      if (addPartitionsManagerWithMockedMetrics.isRunning) {
        addPartitionsManagerWithMockedMetrics.shutdown()
      }
    }
  }

  private def mockTransactionStateMetadata(partitionIndex: Int, leaderId: Int, leaderNode: Option[Node]): Unit = {
    when(metadataCache.getPartitionInfo(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionIndex))
      .thenReturn(Some(
        new UpdateMetadataPartitionState()
          .setTopicName(Topic.TRANSACTION_STATE_TOPIC_NAME)
          .setPartitionIndex(partitionIndex)
          .setLeader(leaderId)))
    if (leaderId != MetadataResponse.NO_LEADER_ID) {
      when(metadataCache.getAliveBrokerNode(leaderId, config.interBrokerListenerName))
        .thenReturn(leaderNode)
    }
  }

  private def clientResponse(
    response: AbstractResponse,
    authException: AuthenticationException = null,
    mismatchException: UnsupportedVersionException = null,
    disconnected: Boolean = false
  ): ClientResponse = {
    new ClientResponse(null, null, null, 0, 0, disconnected, mismatchException, authException, response)
  }

  private def transactionData(
    transactionalId: String,
    producerId: Long,
    producerEpoch: Short = 0
  ): AddPartitionsToTxnTransaction = {
    new AddPartitionsToTxnTransaction()
      .setTransactionalId(transactionalId)
      .setProducerId(producerId)
      .setProducerEpoch(producerEpoch)
      .setVerifyOnly(true)
      .setTopics(new AddPartitionsToTxnTopicCollection(
        Seq(new AddPartitionsToTxnTopic()
          .setName(topic)
          .setPartitions(Seq[Integer](1, 2, 3).asJava)).iterator.asJava))
  }

  private def receiveResponse(response: ClientResponse): Unit = {
    addPartitionsToTxnManager.generateRequests().asScala.head.handler.onComplete(response)
  }

  private def verifyRequest(
    expectedDestination: Node,
    transactionalId: String,
    producerId: Long,
    requestAndHandler: RequestAndCompletionHandler
  ): Unit = {
    assertEquals(time.milliseconds(), requestAndHandler.creationTimeMs)
    assertEquals(expectedDestination, requestAndHandler.destination)
    assertEquals(
      AddPartitionsToTxnRequest.Builder.forBroker(
        new AddPartitionsToTxnTransactionCollection(
          Seq(transactionData(transactionalId, producerId)).iterator.asJava
        )
      ).data,
      requestAndHandler.request.asInstanceOf[AddPartitionsToTxnRequest.Builder].data
    )
  }
}
