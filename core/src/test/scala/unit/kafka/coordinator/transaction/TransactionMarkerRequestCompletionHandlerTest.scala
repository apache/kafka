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

import kafka.server.DelayedOperationPurgatory
import kafka.utils.timer.MockTimer
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{RequestHeader, TransactionResult, WriteTxnMarkersRequest, WriteTxnMarkersResponse}
import org.apache.kafka.common.utils.Utils
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TransactionMarkerRequestCompletionHandlerTest {

  private val purgatory = new DelayedOperationPurgatory[DelayedTxnMarker]("txn-marker-purgatory", new MockTimer, reaperEnabled = false)
  private val topicPartition = new TopicPartition("topic1", 0)
  private val brokerId = 0
  private val txnTopicPartition = 0
  private val transactionalId = "txnId1"
  private val producerId = 0.asInstanceOf[Long]
  private val producerEpoch = 0.asInstanceOf[Short]
  private val txnTimeoutMs = 0
  private val coordinatorEpoch = 0
  private val txnResult = TransactionResult.COMMIT
  private val txnIdAndMarkers =
    Utils.mkList(
      TxnIdAndMarkerEntry(transactionalId, new WriteTxnMarkersRequest.TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, txnResult, Utils.mkList(topicPartition))))

  private val txnMetadata = new TransactionMetadata(producerId, producerEpoch, txnTimeoutMs, PrepareCommit, mutable.Set.empty, 0L, 0L)

  private val markerChannel = EasyMock.createNiceMock(classOf[TransactionMarkerChannel])

  private val txnStateManager = EasyMock.createNiceMock(classOf[TransactionStateManager])

  private val handler = new TransactionMarkerRequestCompletionHandler(brokerId, txnTopicPartition, txnStateManager, markerChannel, txnIdAndMarkers, purgatory)

  private def mockCache(): Unit = {
    EasyMock.expect(txnStateManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))
      .anyTimes()
  }

  @Test
  def shouldReEnqueuePartitionsWhenBrokerDisconnected(): Unit = {
    mockCache()
    EasyMock.expect(markerChannel.addTxnMarkersToSend(transactionalId, producerId, producerEpoch, txnResult, coordinatorEpoch, Set[TopicPartition](topicPartition)))
    EasyMock.replay(markerChannel)

    handler.onComplete(new ClientResponse(new RequestHeader(0, 0, "client", 1), null, null, 0, 0, true, null, null))

    EasyMock.verify(markerChannel)
  }

  @Test
  def shouldThrowIllegalStateExceptionIfErrorCodeNotAvailableForPid(): Unit = {
    mockCache()
    EasyMock.replay(markerChannel)

    val response = new WriteTxnMarkersResponse(new java.util.HashMap[java.lang.Long, java.util.Map[TopicPartition, Errors]]())

    try {
      handler.onComplete(new ClientResponse(new RequestHeader(0, 0, "client", 1), null, null, 0, 0, false, null, response))
      fail("should have thrown illegal argument exception")
    } catch {
      case ise: IllegalStateException => // ok
    }
  }

  @Test
  def shouldCompleteDelayedOperationWhenNoErrors(): Unit = {
    mockCache()
    EasyMock.replay(markerChannel)

    var completed = false
    purgatory.tryCompleteElseWatch(new DelayedTxnMarker(txnMetadata, _ => {
      completed = true
    }), Seq(0L))

    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.NONE))
    handler.onComplete(new ClientResponse(new RequestHeader(0, 0, "client", 1), null, null, 0, 0, false, null, response))

    assertTrue(txnMetadata.topicPartitions.isEmpty)
    assertTrue(completed)
  }

  @Test
  def shouldThrowIllegalStateExceptionWhenErrorNotHandled(): Unit = {
    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.UNKNOWN))
    val metadata = new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set[TopicPartition](topicPartition), 0, 0)
    EasyMock.replay(markerChannel)

    try {
      handler.onComplete(new ClientResponse(new RequestHeader(0, 0, "client", 1), null, null, 0, 0, false, null, response))
      fail("should have thrown illegal state exception")
    } catch {
      case ise: IllegalStateException => // ol
    }

  }

  @Test
  def shouldRetryPartitionWhenUnknownTopicOrPartitionError(): Unit = {
    verifyRetriesPartitionOnError(Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def shouldRetryPartitionWhenNotLeaderForPartitionError(): Unit = {
    verifyRetriesPartitionOnError(Errors.NOT_LEADER_FOR_PARTITION)
  }

  @Test
  def shouldRetryPartitionWhenNotEnoughReplicasError(): Unit = {
    verifyRetriesPartitionOnError(Errors.NOT_ENOUGH_REPLICAS)
  }

  @Test
  def shouldRetryPartitionWhenNotEnoughReplicasAfterAppendError(): Unit = {
    verifyRetriesPartitionOnError(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
  }

  private def verifyRetriesPartitionOnError(errors: Errors) = {
    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.UNKNOWN_TOPIC_OR_PARTITION))
    val metadata = new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set[TopicPartition](topicPartition), 0, 0)

    EasyMock.expect(markerChannel.addTxnMarkersToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](topicPartition)))
    EasyMock.replay(markerChannel)

    handler.onComplete(new ClientResponse(new RequestHeader(0, 0, "client", 1), null, null, 0, 0, false, null, response))

    assertEquals(metadata.topicPartitions, mutable.Set[TopicPartition](topicPartition))
    EasyMock.verify(markerChannel)
  }

  private def createPidErrorMap(errors: Errors) = {
    val pidMap = new java.util.HashMap[lang.Long, util.Map[TopicPartition, Errors]]()
    val errorsMap = new util.HashMap[TopicPartition, Errors]()
    errorsMap.put(topicPartition, errors)
    pidMap.put(producerId, errorsMap)
    pidMap
  }

}
