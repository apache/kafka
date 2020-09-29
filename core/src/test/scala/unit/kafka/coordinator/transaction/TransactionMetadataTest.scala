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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

import scala.collection.mutable

class TransactionMetadataTest {

  val time = new MockTime()
  val producerId = 23423L
  val transactionalId = "txnlId"

  @Test
  def testInitializeEpoch(): Unit = {
    val producerEpoch = RecordBatch.NO_PRODUCER_EPOCH

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())

    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, None)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(0, txnMetadata.producerEpoch)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
  }

  @Test
  def testNormalEpochBump(): Unit = {
    val producerEpoch = 735.toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())

    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, None)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch + 1, txnMetadata.producerEpoch)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
  }

  @Test(expected = classOf[IllegalStateException])
  def testBumpEpochNotAllowedIfEpochsExhausted(): Unit = {
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())
    assertTrue(txnMetadata.isProducerEpochExhausted)

    txnMetadata.prepareIncrementProducerEpoch(30000, None, time.milliseconds())
  }

  @Test
  def testTolerateUpdateTimeShiftDuringEpochBump(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds())

    // let new time be smaller
    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Option(producerEpoch),
      Some(time.milliseconds() - 1))
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch + 1, txnMetadata.producerEpoch)
    assertEquals(producerEpoch, txnMetadata.lastProducerEpoch)
    assertEquals(1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testTolerateUpdateTimeResetDuringProducerIdRotation(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds())

    // let new time be smaller
    val transitMetadata = txnMetadata.prepareProducerIdRotation(producerId + 1, 30000, time.milliseconds() - 1, recordLastEpoch = true)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId + 1, txnMetadata.producerId)
    assertEquals(producerEpoch, txnMetadata.lastProducerEpoch)
    assertEquals(0, txnMetadata.producerEpoch)
    assertEquals(1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testTolerateTimeShiftDuringAddPartitions(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = time.milliseconds(),
      txnLastUpdateTimestamp = time.milliseconds())

    // let new time be smaller; when transting from Empty the start time would be updated to the update-time
    var transitMetadata = txnMetadata.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic1", 0)), time.milliseconds() - 1)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(Set[TopicPartition](new TopicPartition("topic1", 0)), txnMetadata.topicPartitions)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)

    // add another partition, check that in Ongoing state the start timestamp would not change to update time
    transitMetadata = txnMetadata.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds() - 2)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(Set[TopicPartition](new TopicPartition("topic1", 0), new TopicPartition("topic2", 0)), txnMetadata.topicPartitions)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 2, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testTolerateTimeShiftDuringPrepareCommit(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Ongoing,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds())

    // let new time be smaller
    val transitMetadata = txnMetadata.prepareAbortOrCommit(PrepareCommit, time.milliseconds() - 1)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(PrepareCommit, txnMetadata.state)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testTolerateTimeShiftDuringPrepareAbort(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Ongoing,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds())

    // let new time be smaller
    val transitMetadata = txnMetadata.prepareAbortOrCommit(PrepareAbort, time.milliseconds() - 1)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(PrepareAbort, txnMetadata.state)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testTolerateTimeShiftDuringCompleteCommit(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = PrepareCommit,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds())

    // let new time be smaller
    val transitMetadata = txnMetadata.prepareComplete(time.milliseconds() - 1)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(CompleteCommit, txnMetadata.state)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testTolerateTimeShiftDuringCompleteAbort(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = PrepareAbort,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds())

    // let new time be smaller
    val transitMetadata = txnMetadata.prepareComplete(time.milliseconds() - 1)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(CompleteAbort, txnMetadata.state)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testFenceProducerAfterEpochsExhausted(): Unit = {
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Ongoing,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())
    assertTrue(txnMetadata.isProducerEpochExhausted)

    val fencingTransitMetadata = txnMetadata.prepareFenceProducerEpoch()
    assertEquals(Short.MaxValue, fencingTransitMetadata.producerEpoch)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, fencingTransitMetadata.lastProducerEpoch)
    assertEquals(Some(PrepareEpochFence), txnMetadata.pendingState)

    // We should reset the pending state to make way for the abort transition.
    txnMetadata.pendingState = None

    val transitMetadata = txnMetadata.prepareAbortOrCommit(PrepareAbort, time.milliseconds())
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, transitMetadata.producerId)
  }

  @Test(expected = classOf[IllegalStateException])
  def testFenceProducerNotAllowedIfItWouldOverflow(): Unit = {
    val producerEpoch = Short.MaxValue

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Ongoing,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())
    assertTrue(txnMetadata.isProducerEpochExhausted)
    txnMetadata.prepareFenceProducerEpoch()
  }

  @Test
  def testRotateProducerId(): Unit = {
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())

    val newProducerId = 9893L
    val transitMetadata = txnMetadata.prepareProducerIdRotation(newProducerId, 30000, time.milliseconds(), recordLastEpoch = true)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(newProducerId, txnMetadata.producerId)
    assertEquals(producerId, txnMetadata.lastProducerId)
    assertEquals(0, txnMetadata.producerEpoch)
    assertEquals(producerEpoch, txnMetadata.lastProducerEpoch)
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

  @Test
  def testAttemptedEpochBumpWithNewlyCreatedMetadata(): Unit = {
    val producerEpoch = 735.toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())

    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Some(producerEpoch))
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(0, txnMetadata.producerEpoch)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
  }

  @Test
  def testEpochBumpWithCurrentEpochProvided(): Unit = {
    val producerEpoch = 735.toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())

    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Some(producerEpoch))
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch + 1, txnMetadata.producerEpoch)
    assertEquals(producerEpoch, txnMetadata.lastProducerEpoch)
  }

  @Test
  def testAttemptedEpochBumpWithLastEpoch(): Unit = {
    val producerEpoch = 735.toShort
    val lastProducerEpoch = (producerEpoch - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = lastProducerEpoch,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())

    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Some(lastProducerEpoch))
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(lastProducerEpoch, txnMetadata.lastProducerEpoch)
  }

  @Test
  def testAttemptedEpochBumpWithFencedEpoch(): Unit = {
    val producerEpoch = 735.toShort
    val lastProducerEpoch = (producerEpoch - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = producerId,
      producerEpoch = producerEpoch,
      lastProducerEpoch = lastProducerEpoch,
      txnTimeoutMs = 30000,
      state = Empty,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())

    val result = txnMetadata.prepareIncrementProducerEpoch(30000, Some((lastProducerEpoch - 1).toShort),
      time.milliseconds())
    assertEquals(Left(Errors.PRODUCER_FENCED), result)
  }

  private def testRotateProducerIdInOngoingState(state: TransactionState): Unit = {
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      lastProducerId = producerId,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = state,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds())
    val newProducerId = 9893L
    txnMetadata.prepareProducerIdRotation(newProducerId, 30000, time.milliseconds(), recordLastEpoch = false)
  }

  private def prepareSuccessfulIncrementProducerEpoch(txnMetadata: TransactionMetadata,
                                                      expectedProducerEpoch: Option[Short],
                                                      now: Option[Long] = None): TxnTransitMetadata = {
    val result = txnMetadata.prepareIncrementProducerEpoch(30000, expectedProducerEpoch,
      now.getOrElse(time.milliseconds()))
    result.getOrElse(Assertions.fail(s"prepareIncrementProducerEpoch failed with $result"))
  }

}
