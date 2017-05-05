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

  private val markerChannel = EasyMock.createNiceMock(classOf[TransactionMarkerChannel])
  private val purgatory = new DelayedOperationPurgatory[DelayedTxnMarker]("txn-purgatory-name", new MockTimer, reaperEnabled = false)
  private val topic1 = new TopicPartition("topic1", 0)
  private val txnMarkers =
    Utils.mkList(
      new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, 0, TransactionResult.COMMIT, Utils.mkList(topic1)))

  private val handler = new TransactionMarkerRequestCompletionHandler(markerChannel, purgatory, 0, txnMarkers, 0)

  @Test
  def shouldReEnqueuePartitionsWhenBrokerDisconnected(): Unit = {
    EasyMock.expect(markerChannel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](topic1)))
    EasyMock.replay(markerChannel)

    handler.onComplete(new ClientResponse(new RequestHeader(0, 0, "client", 1), null, null, 0, 0, true, null, null))

    EasyMock.verify(markerChannel)
  }

  @Test
  def shouldThrowIllegalStateExceptionIfErrorsNullForPid(): Unit = {
    val response = new WriteTxnMarkersResponse(new java.util.HashMap[java.lang.Long, java.util.Map[TopicPartition, Errors]]())

    EasyMock.replay(markerChannel)

    try {
      handler.onComplete(new ClientResponse(new RequestHeader(0, 0, "client", 1), null, null, 0, 0, false, null, response))
      fail("should have thrown illegal argument exception")
    } catch {
      case ise: IllegalStateException => // ok
    }
  }

  @Test
  def shouldRemoveCompletedPartitionsFromMetadataWhenNoErrors(): Unit = {
    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.NONE))

    val metadata = new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set[TopicPartition](topic1), 0, 0)
    EasyMock.expect(markerChannel.pendingTxnMetadata(0, 0))
      .andReturn(Some(metadata))
    EasyMock.replay(markerChannel)

    handler.onComplete(new ClientResponse(new RequestHeader(0, 0, "client", 1), null, null, 0, 0, false, null, response))

    assertTrue(metadata.topicPartitions.isEmpty)
  }

  @Test
  def shouldTryCompleteDelayedTxnOperation(): Unit = {
    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.NONE))

    val metadata = new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set[TopicPartition](topic1), 0, 0)
    var completed = false

    purgatory.tryCompleteElseWatch(new DelayedTxnMarker(metadata, (errors:Errors) => {
      completed = true
    }), Seq(0L))

    EasyMock.expect(markerChannel.pendingTxnMetadata(0, 0))
      .andReturn(Some(metadata))

    EasyMock.replay(markerChannel)

    handler.onComplete(new ClientResponse(new RequestHeader(0, 0, "client", 1), null, null, 0, 0, false, null, response))
    assertTrue(completed)
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

  @Test
  def shouldThrowIllegalStateExceptionWhenErrorNotHandled(): Unit = {
    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.UNKNOWN))
    val metadata = new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set[TopicPartition](topic1), 0, 0)
    EasyMock.replay(markerChannel)

    try {
      handler.onComplete(new ClientResponse(new RequestHeader(0, 0, "client", 1), null, null, 0, 0, false, null, response))
      fail("should have thrown illegal state exception")
    } catch {
      case ise: IllegalStateException => // ol
    }

  }

  private def verifyRetriesPartitionOnError(errors: Errors) = {
    val response = new WriteTxnMarkersResponse(createPidErrorMap(Errors.UNKNOWN_TOPIC_OR_PARTITION))
    val metadata = new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set[TopicPartition](topic1), 0, 0)

    EasyMock.expect(markerChannel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](topic1)))
    EasyMock.replay(markerChannel)

    handler.onComplete(new ClientResponse(new RequestHeader(0, 0, "client", 1), null, null, 0, 0, false, null, response))

    assertEquals(metadata.topicPartitions, mutable.Set[TopicPartition](topic1))
    EasyMock.verify(markerChannel)
  }

  private def createPidErrorMap(errors: Errors) = {
    val pidMap = new java.util.HashMap[lang.Long, util.Map[TopicPartition, Errors]]()
    val errorsMap = new util.HashMap[TopicPartition, Errors]()
    errorsMap.put(topic1, errors)
    pidMap.put(0L, errorsMap)
    pidMap
  }

}
