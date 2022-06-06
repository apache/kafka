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

import java.{lang, util}
import java.util.Arrays.asList

import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{RequestHeader, TransactionResult, WriteTxnMarkersRequest, WriteTxnMarkersResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, verify, when}

import scala.collection.mutable

class TransactionMarkerRequestCompletionHandlerTest {

  private val brokerId = 0
  private val txnTopicPartition = 0
  private val transactionalId = "txnId1"
  private val producerId = 0.asInstanceOf[Long]
  private val producerEpoch = 0.asInstanceOf[Short]
  private val lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH
  private val txnTimeoutMs = 0
  private val coordinatorEpoch = 0
  private val txnResult = TransactionResult.COMMIT
  private val topicPartition = new TopicPartition("topic1", 0)
  private val txnIdAndMarkers = asList(
      TxnIdAndMarkerEntry(transactionalId, new WriteTxnMarkersRequest.TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, txnResult, asList(topicPartition))))

  private val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, lastProducerEpoch,
    txnTimeoutMs, PrepareCommit, mutable.Set[TopicPartition](topicPartition), 0L, 0L)

  private val markerChannelManager: TransactionMarkerChannelManager =
    mock(classOf[TransactionMarkerChannelManager])

  private val txnStateManager: TransactionStateManager = mock(classOf[TransactionStateManager])

  private val handler = new TransactionMarkerRequestCompletionHandler(brokerId, txnStateManager, markerChannelManager, txnIdAndMarkers)

  private def mockCache(): Unit = {
    when(txnStateManager.partitionFor(transactionalId))
      .thenReturn(txnTopicPartition)
    when(txnStateManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
  }

  @Test
  def shouldReEnqueuePartitionsWhenBrokerDisconnected(): Unit = {
    mockCache()

    handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
      null, null, 0, 0, true, null, null, null))

    verify(markerChannelManager).addTxnMarkersToBrokerQueue(transactionalId,
      producerId, producerEpoch, txnResult, coordinatorEpoch, Set[TopicPartition](topicPartition))
  }

  @Test
  def shouldThrowIllegalStateExceptionIfErrorCodeNotAvailableForPid(): Unit = {
    mockCache()

    val response = new WriteTxnMarkersResponse(new java.util.HashMap[java.lang.Long, java.util.Map[TopicPartition, Errors]]())

    assertThrows(classOf[IllegalStateException], () => handler.onComplete(new ClientResponse(new RequestHeader(
      ApiKeys.PRODUCE, 0, "client", 1), null, null, 0, 0, false, null, null, response)))
  }

  @Test
  def shouldCompleteDelayedOperationWhenNoErrors(): Unit = {
    mockCache()

    verifyCompleteDelayedOperationOnError(Errors.NONE)
  }

  @Test
  def shouldCompleteDelayedOperationWhenNotCoordinator(): Unit = {
    when(txnStateManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.NOT_COORDINATOR))

    verifyRemoveDelayedOperationOnError(Errors.NONE)
  }

  @Test
  def shouldCompleteDelayedOperationWhenCoordinatorLoading(): Unit = {
    when(txnStateManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))

    verifyRemoveDelayedOperationOnError(Errors.NONE)
  }

  @Test
  def shouldCompleteDelayedOperationWhenCoordinatorEpochChanged(): Unit = {
    when(txnStateManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch+1, txnMetadata))))

    verifyRemoveDelayedOperationOnError(Errors.NONE)
  }

  @Test
  def shouldCompleteDelayedOperationWhenInvalidProducerEpoch(): Unit = {
    mockCache()

    verifyRemoveDelayedOperationOnError(Errors.INVALID_PRODUCER_EPOCH)
  }

  @Test
  def shouldCompleteDelayedOperationWheCoordinatorEpochFenced(): Unit = {
    mockCache()

    verifyRemoveDelayedOperationOnError(Errors.TRANSACTION_COORDINATOR_FENCED)
  }

  @Test
  def shouldThrowIllegalStateExceptionWhenUnknownError(): Unit = {
    verifyThrowIllegalStateExceptionOnError(Errors.UNKNOWN_SERVER_ERROR)
  }

  @Test
  def shouldThrowIllegalStateExceptionWhenCorruptMessageError(): Unit = {
    verifyThrowIllegalStateExceptionOnError(Errors.CORRUPT_MESSAGE)
  }

  @Test
  def shouldThrowIllegalStateExceptionWhenMessageTooLargeError(): Unit = {
    verifyThrowIllegalStateExceptionOnError(Errors.MESSAGE_TOO_LARGE)
  }

  @Test
  def shouldThrowIllegalStateExceptionWhenRecordListTooLargeError(): Unit = {
    verifyThrowIllegalStateExceptionOnError(Errors.RECORD_LIST_TOO_LARGE)
  }

  @Test
  def shouldThrowIllegalStateExceptionWhenInvalidRequiredAcksError(): Unit = {
    verifyThrowIllegalStateExceptionOnError(Errors.INVALID_REQUIRED_ACKS)
  }

  @Test
  def shouldRetryPartitionWhenUnknownTopicOrPartitionError(): Unit = {
    verifyRetriesPartitionOnError(Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def shouldRetryPartitionWhenNotLeaderOrFollowerError(): Unit = {
    verifyRetriesPartitionOnError(Errors.NOT_LEADER_OR_FOLLOWER)
  }

  @Test
  def shouldRetryPartitionWhenNotEnoughReplicasError(): Unit = {
    verifyRetriesPartitionOnError(Errors.NOT_ENOUGH_REPLICAS)
  }

  @Test
  def shouldRetryPartitionWhenNotEnoughReplicasAfterAppendError(): Unit = {
    verifyRetriesPartitionOnError(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
  }

  @Test
  def shouldRetryPartitionWhenKafkaStorageError(): Unit = {
    verifyRetriesPartitionOnError(Errors.KAFKA_STORAGE_ERROR)
  }

  @Test
  def shouldRemoveTopicPartitionFromWaitingSetOnUnsupportedForMessageFormat(): Unit = {
    mockCache()
    verifyCompleteDelayedOperationOnError(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT)
  }

  private def verifyRetriesPartitionOnError(error: Errors): Unit = {
    mockCache()

    val response = new WriteTxnMarkersResponse(createProducerIdErrorMap(error))
    handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
      null, null, 0, 0, false, null, null, response))

    assertEquals(txnMetadata.topicPartitions, mutable.Set[TopicPartition](topicPartition))
    verify(markerChannelManager).addTxnMarkersToBrokerQueue(transactionalId,
      producerId, producerEpoch, txnResult, coordinatorEpoch, Set[TopicPartition](topicPartition))
  }

  private def verifyThrowIllegalStateExceptionOnError(error: Errors) = {
    mockCache()

    val response = new WriteTxnMarkersResponse(createProducerIdErrorMap(error))
    assertThrows(classOf[IllegalStateException], () => handler.onComplete(new ClientResponse(new RequestHeader(
      ApiKeys.PRODUCE, 0, "client", 1), null, null, 0, 0, false, null, null, response)))
  }

  private def verifyCompleteDelayedOperationOnError(error: Errors): Unit = {

    var completed = false
    when(markerChannelManager.maybeWriteTxnCompletion(transactionalId))
      .thenAnswer(_ => completed = true)

    val response = new WriteTxnMarkersResponse(createProducerIdErrorMap(error))
    handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
      null, null, 0, 0, false, null, null, response))

    assertTrue(txnMetadata.topicPartitions.isEmpty)
    assertTrue(completed)
  }

  private def verifyRemoveDelayedOperationOnError(error: Errors): Unit = {

    var removed = false
    when(markerChannelManager.removeMarkersForTxnId(transactionalId))
      .thenAnswer(_ => removed = true)

    val response = new WriteTxnMarkersResponse(createProducerIdErrorMap(error))
    handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
      null, null, 0, 0, false, null, null, response))

    assertTrue(removed)
  }


  private def createProducerIdErrorMap(errors: Errors) = {
    val pidMap = new java.util.HashMap[lang.Long, util.Map[TopicPartition, Errors]]()
    val errorsMap = new util.HashMap[TopicPartition, Errors]()
    errorsMap.put(topicPartition, errors)
    pidMap.put(producerId, errorsMap)
    pidMap
  }
}
