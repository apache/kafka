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

import kafka.utils.MockTime
import org.apache.kafka.common.record.RecordBatch
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TransactionMetadataTest {

  val time = new MockTime()

  @Test
  def testInitializeEpoch(): Unit = {
    val transactionalId = "txnlId"
    val producerId = 23423L
    val producerEpoch = RecordBatch.NO_PRODUCER_EPOCH

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      producerEpoch = producerEpoch,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())

    val transitMetadata = txnMetadata.prepareIncrementProducerEpoch(30000, time.milliseconds())
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(0, txnMetadata.producerEpoch)
  }

  @Test
  def testNormalEpochBump(): Unit = {
    val transactionalId = "txnlId"
    val producerId = 23423L
    val producerEpoch = 735.toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      producerEpoch = producerEpoch,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())

    val transitMetadata = txnMetadata.prepareIncrementProducerEpoch(30000, time.milliseconds())
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch + 1, txnMetadata.producerEpoch)
  }

  @Test(expected = classOf[IllegalStateException])
  def testBumpEpochNotAllowedIfEpochsExhausted(): Unit = {
    val transactionalId = "txnlId"
    val producerId = 23423L
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      producerEpoch = producerEpoch,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())
    assertTrue(txnMetadata.isProducerEpochExhausted)

    txnMetadata.prepareIncrementProducerEpoch(30000, time.milliseconds())
  }

  @Test
  def testFenceProducerAfterEpochsExhausted(): Unit = {
    val transactionalId = "txnlId"
    val producerId = 23423L
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      producerEpoch = producerEpoch,
      txnTimeoutMs = 30000,
      state = Ongoing,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())
    assertTrue(txnMetadata.isProducerEpochExhausted)

    txnMetadata.prepareFenceProducerEpoch()
    assertEquals(Short.MaxValue, txnMetadata.producerEpoch)

    val transitMetadata = txnMetadata.prepareAbortOrCommit(PrepareAbort, time.milliseconds())
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, transitMetadata.producerId)
  }

  @Test(expected = classOf[IllegalStateException])
  def testFenceProducerNotAllowedIfItWouldOverflow(): Unit = {
    val transactionalId = "txnlId"
    val producerId = 23423L
    val producerEpoch = Short.MaxValue

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      producerEpoch = producerEpoch,
      txnTimeoutMs = 30000,
      state = Ongoing,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())
    assertTrue(txnMetadata.isProducerEpochExhausted)
    txnMetadata.prepareFenceProducerEpoch()
  }

  @Test
  def testRotateProducerId(): Unit = {
    val transactionalId = "txnlId"
    val producerId = 23423L
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      producerEpoch = producerEpoch,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())

    val newProducerId = 9893L
    val transitMetadata = txnMetadata.prepareProducerIdRotation(newProducerId, 30000, time.milliseconds())
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(newProducerId, txnMetadata.producerId)
    assertEquals(0, txnMetadata.producerEpoch)
  }

  @Test(expected = classOf[IllegalStateException])
  def testRotateProducerIdInOngoingState(): Unit = {
    testRotateProducerIdInOngoingState(Ongoing)
  }

  @Test(expected = classOf[IllegalStateException])
  def testRotateProducerIdInPrepareAbortState(): Unit = {
    testRotateProducerIdInOngoingState(PrepareAbort)
  }

  @Test(expected = classOf[IllegalStateException])
  def testRotateProducerIdInPrepareCommitState(): Unit = {
    testRotateProducerIdInOngoingState(PrepareCommit)
  }

  private def testRotateProducerIdInOngoingState(state: TransactionState): Unit = {
    val transactionalId = "txnlId"
    val producerId = 23423L
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      producerEpoch = producerEpoch,
      txnTimeoutMs = 30000,
      state = state,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())
    val newProducerId = 9893L
    txnMetadata.prepareProducerIdRotation(newProducerId, 30000, time.milliseconds())
  }


}
