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

package unit.kafka.server

import kafka.common.RequestAndCompletionHandler
import kafka.server.{AddPartitionsToTxnManager, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.errors.{AuthenticationException, SaslAuthenticationException, UnsupportedVersionException}
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.{AddPartitionsToTxnTopic, AddPartitionsToTxnTopicCollection, AddPartitionsToTxnTransaction, AddPartitionsToTxnTransactionCollection}
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractResponse, AddPartitionsToTxnRequest, AddPartitionsToTxnResponse}
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito.mock

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class AddPartitionsToTxnManagerTest {
  private val networkClient: NetworkClient = mock(classOf[NetworkClient])

  private val time = new MockTime

  private var addPartitionsToTxnManager: AddPartitionsToTxnManager = _

  val topic = "foo"
  val topicPartitions = List(new TopicPartition(topic, 1), new TopicPartition(topic, 2), new TopicPartition(topic, 3))

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

  @BeforeEach
  def setup(): Unit = {
    addPartitionsToTxnManager = new AddPartitionsToTxnManager(
      KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:2181")),
      networkClient,
      time)
  }

  @AfterEach
  def teardown(): Unit = {
    addPartitionsToTxnManager.shutdown()
  }

  def setErrors(errors: mutable.Map[TopicPartition, Errors])(callbackErrors: Map[TopicPartition, Errors]): Unit = {
    callbackErrors.foreach {
      case (tp, error) => errors.put(tp, error)
    }
  }

  @Test
  def testAddTxnData(): Unit = {
    val transaction1Errors = mutable.Map[TopicPartition, Errors]()
    val transaction2Errors = mutable.Map[TopicPartition, Errors]()
    val transaction3Errors = mutable.Map[TopicPartition, Errors]()

    addPartitionsToTxnManager.addTxnData(node0, transactionData(transactionalId1, producerId1), setErrors(transaction1Errors))
    addPartitionsToTxnManager.addTxnData(node1, transactionData(transactionalId2, producerId2), setErrors(transaction2Errors))
    addPartitionsToTxnManager.addTxnData(node0, transactionData(transactionalId3, producerId3), setErrors(transaction3Errors))

    val transaction1AgainErrorsOldEpoch = mutable.Map[TopicPartition, Errors]()
    val transaction1AgainErrorsNewEpoch = mutable.Map[TopicPartition, Errors]()
    // Trying to add more transactional data for the same transactional ID, producer ID, and epoch should simply replace the old data and send a retriable response.
    addPartitionsToTxnManager.addTxnData(node0, transactionData(transactionalId1, producerId1), setErrors(transaction1AgainErrorsOldEpoch))
    val expectedNetworkErrors = topicPartitions.map(_ -> Errors.NETWORK_EXCEPTION).toMap
    assertEquals(expectedNetworkErrors, transaction1Errors)

    // Trying to add more transactional data for the same transactional ID and producer ID, but new epoch should replace the old data and send an error response for it.
    addPartitionsToTxnManager.addTxnData(node0, transactionData(transactionalId1, producerId1, producerEpoch = 1), setErrors(transaction1AgainErrorsNewEpoch))
    
    val expectedEpochErrors = topicPartitions.map(_ -> Errors.INVALID_PRODUCER_EPOCH).toMap
    assertEquals(expectedEpochErrors, transaction1AgainErrorsOldEpoch)

    val requestsAndHandlers = addPartitionsToTxnManager.generateRequests()
    requestsAndHandlers.foreach { requestAndHandler =>
      if (requestAndHandler.destination == node0) {
        assertEquals(time.milliseconds(), requestAndHandler.creationTimeMs)
        assertEquals(AddPartitionsToTxnRequest.Builder.forBroker(
          new AddPartitionsToTxnTransactionCollection(Seq(transactionData(transactionalId3, producerId3), transactionData(transactionalId1, producerId1, producerEpoch = 1)).iterator.asJava)).data,
          requestAndHandler.request.asInstanceOf[AddPartitionsToTxnRequest.Builder].data) // insertion order
      } else {
        verifyRequest(node1, transactionalId2, producerId2, requestAndHandler)
      }
    }
  }

  @Test
  def testGenerateRequests(): Unit = {
    val transactionErrors = mutable.Map[TopicPartition, Errors]()

    addPartitionsToTxnManager.addTxnData(node0, transactionData(transactionalId1, producerId1), setErrors(transactionErrors))
    addPartitionsToTxnManager.addTxnData(node1, transactionData(transactionalId2, producerId2), setErrors(transactionErrors))

    val requestsAndHandlers = addPartitionsToTxnManager.generateRequests()
    assertEquals(2, requestsAndHandlers.size)
    // Note: handlers are tested in testAddPartitionsToTxnHandlerErrorHandling
    requestsAndHandlers.foreach{ requestAndHandler =>
      if (requestAndHandler.destination == node0) {
        verifyRequest(node0, transactionalId1, producerId1, requestAndHandler)
      } else {
        verifyRequest(node1, transactionalId2, producerId2, requestAndHandler)
      }
    }

    addPartitionsToTxnManager.addTxnData(node1, transactionData(transactionalId2, producerId2), setErrors(transactionErrors))
    addPartitionsToTxnManager.addTxnData(node2, transactionData(transactionalId3, producerId3), setErrors(transactionErrors))

    // Test creationTimeMs increases too.
    time.sleep(1000)

    val requestsAndHandlers2 = addPartitionsToTxnManager.generateRequests()
    // The request for node1 should not be added because one request is already inflight.
    assertEquals(1, requestsAndHandlers2.size)
    requestsAndHandlers2.foreach { requestAndHandler =>
      verifyRequest(node2, transactionalId3, producerId3, requestAndHandler)
    }

    // Complete the request for node1 so the new one can go through.
    requestsAndHandlers.filter(_.destination == node1).head.handler.onComplete(authenticationErrorResponse)
    val requestsAndHandlers3 = addPartitionsToTxnManager.generateRequests()
    assertEquals(1, requestsAndHandlers3.size)
    requestsAndHandlers3.foreach { requestAndHandler =>
      verifyRequest(node1, transactionalId2, producerId2, requestAndHandler)
    }
  }

  @Test
  def testAddPartitionsToTxnHandlerErrorHandling(): Unit = {
    val transaction1Errors = mutable.Map[TopicPartition, Errors]()
    val transaction2Errors = mutable.Map[TopicPartition, Errors]()

    def addTransactionsToVerify(): Unit = {
      transaction1Errors.clear()
      transaction2Errors.clear()

      addPartitionsToTxnManager.addTxnData(node0, transactionData(transactionalId1, producerId1), setErrors(transaction1Errors))
      addPartitionsToTxnManager.addTxnData(node0, transactionData(transactionalId2, producerId2), setErrors(transaction2Errors))
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

    val expectedTopLevelErrors = topicPartitions.map(_ -> Errors.INVALID_RECORD).toMap
    val topLevelErrorAddPartitionsResponse = new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData().setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code()))
    val topLevelErrorResponse = clientResponse(topLevelErrorAddPartitionsResponse)
    addTransactionsToVerify()
    receiveResponse(topLevelErrorResponse)
    assertEquals(expectedTopLevelErrors, transaction1Errors)
    assertEquals(expectedTopLevelErrors, transaction2Errors)

    val preConvertedTransaction1Errors = topicPartitions.map(_ -> Errors.PRODUCER_FENCED).toMap
    val expectedTransaction1Errors = topicPartitions.map(_ -> Errors.INVALID_PRODUCER_EPOCH).toMap
    val preConvertedTransaction2Errors = Map(new TopicPartition("foo", 1) -> Errors.NONE, 
      new TopicPartition("foo", 2) -> Errors.INVALID_RECORD,
      new TopicPartition("foo", 3) -> Errors.NONE)
    val expectedTransaction2Errors = Map(new TopicPartition("foo", 2) -> Errors.INVALID_RECORD)

    val transaction1ErrorResponse = AddPartitionsToTxnResponse.resultForTransaction(transactionalId1, preConvertedTransaction1Errors.asJava)
    val transaction2ErrorResponse = AddPartitionsToTxnResponse.resultForTransaction(transactionalId2, preConvertedTransaction2Errors.asJava)
    val mixedErrorsAddPartitionsResponse = new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData()
      .setResultsByTransaction(new AddPartitionsToTxnResultCollection(Seq(transaction1ErrorResponse, transaction2ErrorResponse).iterator.asJava)))
    val mixedErrorsResponse = clientResponse(mixedErrorsAddPartitionsResponse)

    addTransactionsToVerify()
    receiveResponse(mixedErrorsResponse)
    assertEquals(expectedTransaction1Errors, transaction1Errors)
    assertEquals(expectedTransaction2Errors, transaction2Errors)
  }

  private def clientResponse(response: AbstractResponse, authException: AuthenticationException = null, mismatchException: UnsupportedVersionException = null, disconnected: Boolean = false): ClientResponse = {
    new ClientResponse(null, null, null, 0, 0, disconnected, mismatchException, authException, response)
  }

  private def transactionData(transactionalId: String, producerId: Long, producerEpoch: Short = 0): AddPartitionsToTxnTransaction = {
    new AddPartitionsToTxnTransaction()
      .setTransactionalId(transactionalId)
      .setProducerId(producerId)
      .setProducerEpoch(producerEpoch)
      .setTopics(new AddPartitionsToTxnTopicCollection(
        Seq(new AddPartitionsToTxnTopic()
          .setName(topic)
          .setPartitions(Seq[Integer](1, 2, 3).asJava)).iterator.asJava))
  }

  private def receiveResponse(response: ClientResponse): Unit = {
    addPartitionsToTxnManager.generateRequests().head.handler.onComplete(response)
  }

  private def verifyRequest(expectedDestination: Node, transactionalId: String, producerId: Long, requestAndHandler: RequestAndCompletionHandler): Unit = {
    assertEquals(time.milliseconds(), requestAndHandler.creationTimeMs)
    assertEquals(expectedDestination, requestAndHandler.destination)
    assertEquals(AddPartitionsToTxnRequest.Builder.forBroker(
      new AddPartitionsToTxnTransactionCollection(Seq(transactionData(transactionalId, producerId)).iterator.asJava)).data,
      requestAndHandler.request.asInstanceOf[AddPartitionsToTxnRequest.Builder].data)
  }
}
