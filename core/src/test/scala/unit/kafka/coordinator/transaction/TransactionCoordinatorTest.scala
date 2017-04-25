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

import kafka.server.DelayedOperationPurgatory
import kafka.utils.timer.MockTimer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.MockTime
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TransactionCoordinatorTest {

  val time = new MockTime()

  var nextPid: Long = 0L
  val pidManager: ProducerIdManager = EasyMock.createNiceMock(classOf[ProducerIdManager])
  val transactionManager: TransactionStateManager = EasyMock.createNiceMock(classOf[TransactionStateManager])
  val transactionMarkerChannelManager: TransactionMarkerChannelManager = EasyMock.createNiceMock(classOf[TransactionMarkerChannelManager])
  val capturedTxn: Capture[TransactionMetadata] = EasyMock.newCapture()
  val capturedErrorsCallback: Capture[Errors => Unit] = EasyMock.newCapture()
  val brokerId = 0

  private val txnMarkerPurgatory = new DelayedOperationPurgatory[DelayedTxnMarker]("test", new MockTimer, reaperEnabled = false)
  private val partitions = mutable.Set[TopicPartition](new TopicPartition("topic1", 0))

  val coordinator: TransactionCoordinator = new TransactionCoordinator(brokerId,
    pidManager,
    transactionManager,
    transactionMarkerChannelManager,
    txnMarkerPurgatory,
    time)

  var result: InitPidResult = _
  var error: Errors = Errors.NONE

  val transactionTimeoutMs = 1000

  private def mockPidManager(): Unit = {
    EasyMock.expect(pidManager.nextPid())
      .andAnswer(new IAnswer[Long] {
        override def answer(): Long = {
          nextPid += 1
          nextPid - 1
        }
      })
      .anyTimes()
  }

  private def initPidGenericMocks(transactionalId: String): Unit = {
    mockPidManager()
    EasyMock.expect(transactionManager.isCoordinatorFor(EasyMock.eq(transactionalId)))
      .andReturn(true)
      .anyTimes()

    EasyMock.expect(transactionManager.isCoordinatorLoadingInProgress(EasyMock.anyString()))
      .andReturn(false)
      .anyTimes()
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)
      .anyTimes()
  }


  @Test
  def shouldAcceptInitPidAndReturnNextPidWhenTransactionalIdIsEmpty(): Unit = {
    mockPidManager()
    EasyMock.replay(pidManager)

    coordinator.handleInitPid("", transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(0L, 0, Errors.NONE), result)
    coordinator.handleInitPid("", transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(1L, 0, Errors.NONE), result)
  }

  @Test
  def shouldAcceptInitPidAndReturnNextPidWhenTransactionalIdIsNull(): Unit = {
    mockPidManager()
    EasyMock.replay(pidManager)

    coordinator.handleInitPid(null, transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(0L, 0, Errors.NONE), result)
    coordinator.handleInitPid(null, transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(1L, 0, Errors.NONE), result)
  }

  @Test
  def shouldInitPidWithEpochZeroForNewTransactionalId(): Unit = {
    val transactionalId = "a"
    initPidGenericMocks(transactionalId)
    EasyMock.expect(transactionManager.addTransaction(EasyMock.eq(transactionalId), EasyMock.capture(capturedTxn)))
      .andAnswer(new IAnswer[TransactionMetadata] {
        override def answer(): TransactionMetadata = {
          capturedTxn.getValue
        }
      })
      .once()
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andAnswer(new IAnswer[Option[TransactionMetadata]] {
        override def answer(): Option[TransactionMetadata] = {
          if (capturedTxn.hasCaptured) {
            Some(capturedTxn.getValue)
          } else {
            None
          }
        }
      })
      .once()

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.capture(capturedTxn),
      EasyMock.capture(capturedErrorsCallback)))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          capturedErrorsCallback.getValue.apply(Errors.NONE)
        }
      })
      .anyTimes()
    EasyMock.replay(pidManager, transactionManager)

    coordinator.handleInitPid("a", transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(0L, 0, Errors.NONE), result)
  }

  @Test
  def shouldRespondWithNotCoordinatorOnInitPidWhenNotCoordinatorForId(): Unit = {
    mockPidManager()
    EasyMock.replay(pidManager)
    coordinator.handleInitPid("some-pid", transactionTimeoutMs, initPidMockCallback)
    assertEquals(InitPidResult(-1, -1, Errors.NOT_COORDINATOR), result)
  }

  @Test
  def shouldRespondWithInvalidPidMappingOnAddPartitionsToTransactionWhenTransactionalIdNotPresent(): Unit = {
    val transactionalId = "a"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(None)
    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 1, partitions, errorsCallback)
    assertEquals(Errors.INVALID_PID_MAPPING, error)
  }

  @Test
  def shouldRespondWithInvalidRequestAddPartitionsToTransactionWhenTransactionalIdIsEmpty(): Unit = {
    coordinator.handleAddPartitionsToTransaction("", 0L, 1, partitions, errorsCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithInvalidRequestAddPartitionsToTransactionWhenTransactionalIdIsNull(): Unit = {
    coordinator.handleAddPartitionsToTransaction(null, 0L, 1, partitions, errorsCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithNotCoordinatorOnAddPartitionsWhenNotCoordinator(): Unit = {
    coordinator.handleAddPartitionsToTransaction("txn", 0L, 1, partitions, errorsCallback)
    assertEquals(Errors.NOT_COORDINATOR, error)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnAddPartitionsWhenCoordintorLoading(): Unit = {
    val transactionalId = "a"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.isCoordinatorLoadingInProgress(transactionalId))
    .andReturn(true)

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 1, partitions, errorsCallback)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, error)
  }

  @Test
  def shouldRespondWithInvalidTnxStateOnAddPartitionsWhenStateIsPrepareCommit(): Unit = {
    validateInvalidTxnState(PrepareCommit)
  }

  @Test
  def shouldRespondWithInvalidTnxStateOnAddPartitionsWhenStateIsPrepareAbort(): Unit = {
    validateInvalidTxnState(PrepareAbort)
  }

  @Test
  def shouldRespondWithInvalidTnxStateOnAddPartitionsWhenStateIsCompleteCommit(): Unit = {
    validateInvalidTxnState(CompleteCommit)
  }

  @Test
  def shouldRespondWithInvalidTnxStateOnAddPartitionsWhenStateIsCompleteAbort(): Unit = {
    validateInvalidTxnState(CompleteAbort)
  }

  def validateInvalidTxnState(state: TransactionState): Unit = {
    val transactionalId = "a"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(new TransactionMetadata(0, 0, 0, state, mutable.Set.empty, 0, 0)))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
  }

  @Test
  def shouldRespondWithInvalidTnxProduceEpochOnAddPartitionsWhenEpochsAreDifferent(): Unit = {
    val transactionalId = "a"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(new TransactionMetadata(0, 10, 0, PrepareCommit, mutable.Set.empty, 0, 0)))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_EPOCH, error)
  }

  @Test
  def shouldAppendNewMetadataToLogOnAddPartitionsWhenPartitionsAdded(): Unit = {
    val transactionalId = "a"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(new TransactionMetadata(0, 0, 0, Empty, mutable.Set.empty, 0, 0)))

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(new TransactionMetadata(0, 0, 0, Ongoing, partitions, time.milliseconds(), time.milliseconds())),
      EasyMock.capture(capturedErrorsCallback)
    ))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldRespondWithErrorsNoneOnAddPartitionWhenNoErrorsAndPartitionsTheSame(): Unit = {
    val transactionalId = "a"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(new TransactionMetadata(0, 0, 0, Empty, partitions, 0, 0)))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)

  }

  @Test
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenTxnIdDoesntExist(): Unit = {
    val transactionId = "unknown"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionId)).andReturn(None)
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PID_MAPPING, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenPidDosentMatchMapped(): Unit = {
    val transactionId = "known"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(new TransactionMetadata(10, 0, 0, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PID_MAPPING, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReplyWithProducerFencedOnEndTxnWhenEpochIsNotSameAsTransaction(): Unit = {
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_EPOCH, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteCommitAndResultIsCommit(): Unit ={
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteAbortAndResultIsAbort(): Unit ={
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.ABORT, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteAbortAndResultIsNotAbort(): Unit = {
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteCommitAndResultIsNotCommit(): Unit = {
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.ABORT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsPrepareCommit(): Unit = {
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, PrepareCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsPrepareAbort(): Unit = {
    val transactionId = "known"
    val pid = 10
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(new TransactionMetadata(pid, 1, 1, PrepareAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }


  @Test
  def shouldAppendPrepareCommitToLogOnEndTxnWhenStatusIsOngoingAndResultIsCommit(): Unit = {
    val transactionId = "known"
    val pid = 10
    val epoch:Short = 1
    val txnTimeoutMs = 1

    mockPrepare(transactionId, pid, epoch, txnTimeoutMs, PrepareCommit)

    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, epoch, TransactionResult.COMMIT, errorsCallback)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldAppendPrepareAbortToLogOnEndTxnWhenStatusIsOngoingAndResultIsAbort(): Unit = {
    val transactionId = "known"
    val pid = 10
    val epoch:Short = 1
    val txnTimeoutMs = 1

    mockPrepare(transactionId, pid, epoch, txnTimeoutMs, PrepareAbort)

    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionId, pid, epoch, TransactionResult.ABORT, errorsCallback)
    EasyMock.verify(transactionManager)
  }


  @Test
  def shouldAppendCompleteAbortToLogOnEndTxnWhenStatusIsOngoingAndResultIsAbort(): Unit = {
    val transactionId = "known"
    val pid = 10
    val epoch:Short = 1
    val txnTimeoutMs = 1

    mockComplete(transactionId, pid, epoch, txnTimeoutMs, PrepareAbort)

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.handleEndTransaction(transactionId, pid, epoch, TransactionResult.ABORT, errorsCallback)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldAppendCompleteCommitToLogOnEndTxnWhenStatusIsOngoingAndResultIsCommit(): Unit = {
    val transactionId = "known"
    val pid = 10
    val epoch:Short = 1
    val txnTimeoutMs = 1

    mockComplete(transactionId, pid, epoch, txnTimeoutMs, PrepareCommit)

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.handleEndTransaction(transactionId, pid, epoch, TransactionResult.COMMIT, errorsCallback)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsNull(): Unit = {
    coordinator.handleEndTransaction(null, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsEmpty(): Unit = {
    coordinator.handleEndTransaction("", 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithNotCoordinatorOnEndTxnWhenIsNotCoordinatorForId(): Unit = {
    coordinator.handleEndTransaction("id", 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.NOT_COORDINATOR, error)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnEndTxnWhenCoordinatorIsLoading(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(EasyMock.anyString()))
      .andReturn(true)
    EasyMock.expect(transactionManager.isCoordinatorLoadingInProgress(EasyMock.anyString()))
      .andReturn(true)

    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction("id", 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, error)
  }

  @Test
  def shouldIncrementEpochAndUpdateMetadataOnHandleInitPidWhenExistingEmptyTransaction(): Unit = {
    validateIncrementEpochAndUpdateMetadata(Empty)
  }

  @Test
  def shouldIncrementEpochAndUpdateMetadataOnHandleInitPidWhenExistingCompleteTransaction(): Unit = {
    validateIncrementEpochAndUpdateMetadata(CompleteAbort)
  }

  @Test
  def shouldIncrementEpochAndUpdateMetadataOnHandleInitPidWhenExistingCompleteCommitTransaction(): Unit = {
    validateIncrementEpochAndUpdateMetadata(CompleteCommit)
  }

  @Test
  def shouldWaitForCommitToCompleteOnHandleInitPidAndExistingTransactionInPrepareCommitState(): Unit ={
    validateWaitsForCompletionBeforeRespondingWithIncrementedEpoch(PrepareCommit)
  }

  @Test
  def shouldWaitForCommitToCompleteOnHandleInitPidAndExistingTransactionInPrepareAbortState(): Unit ={
    validateWaitsForCompletionBeforeRespondingWithIncrementedEpoch(PrepareAbort)
  }

  @Test
  def shouldAbortTransactionOnHandleInitPidWhenExistingTransactionInOngoingState(): Unit = {
    val transactionId = "tid"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)

    val metadata = new TransactionMetadata(0, 0, 0, Ongoing, mutable.Set[TopicPartition](new TopicPartition("topic", 1)), 0, 0)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(metadata))
      .once()

    mockComplete(transactionId, 0, 0, 10, PrepareAbort)

    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true).anyTimes()
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()

    val completedMetadata = new TransactionMetadata(0, 0, 0, CompleteAbort, mutable.Set.empty[TopicPartition], 0, 0)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(completedMetadata))
      .anyTimes()

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionId),
      EasyMock.anyObject(classOf[TransactionMetadata]),
      EasyMock.capture(capturedErrorsCallback)
    )).andAnswer(new IAnswer[Unit] {
      override def answer(): Unit = {
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      }
    })

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.handleInitPid(transactionId, 10, initPidMockCallback)

    assertEquals(InitPidResult(0, 1, Errors.NONE), result)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldRemoveTransactionsForPartitionOnEmigration(): Unit = {
    EasyMock.expect(transactionManager.removeTransactionsForPartition(0))
    EasyMock.expect(transactionMarkerChannelManager.removeStateForPartition(0))
    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.handleTxnEmigration(0)

    EasyMock.verify(transactionManager, transactionMarkerChannelManager)
  }

  private def validateWaitsForCompletionBeforeRespondingWithIncrementedEpoch(state: TransactionState) = {
    val transactionId = "tid"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true).anyTimes()
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()

    val metadata = new TransactionMetadata(0, 0, 0, state, mutable.Set[TopicPartition](new TopicPartition("topic", 1)), 0, 0)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(metadata)).anyTimes()

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionId),
      EasyMock.anyObject(classOf[TransactionMetadata]),
      EasyMock.capture(capturedErrorsCallback)
    )).andAnswer(new IAnswer[Unit] {
      override def answer(): Unit = {
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      }
    })

    EasyMock.replay(transactionManager)

    coordinator.handleInitPid(transactionId, 10, initPidMockCallback)
    // no result yet as hasn't completed
    assertNull(result)
    // complete the transaction
    metadata.topicPartitions.clear()
    metadata.state = if (state == PrepareCommit) CompleteCommit else CompleteAbort
    txnMarkerPurgatory.checkAndComplete(0L)

    assertEquals(InitPidResult(0, 1, Errors.NONE), result)
    assertEquals(new TransactionMetadata(0, 1, 10, Empty, mutable.Set.empty, 0, time.milliseconds()), metadata)
  }

  private def validateIncrementEpochAndUpdateMetadata(state: TransactionState) = {
    val transactionId = "tid"
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)

    val metadata = new TransactionMetadata(0, 0, 0, state, mutable.Set.empty[TopicPartition], 0, 0)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(metadata))

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionId),
      EasyMock.anyObject(classOf[TransactionMetadata]),
      EasyMock.capture(capturedErrorsCallback)
    )).andAnswer(new IAnswer[Unit] {
      override def answer(): Unit = {
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      }
    })

    EasyMock.replay(transactionManager)

    coordinator.handleInitPid(transactionId, 10, initPidMockCallback)

    assertEquals(InitPidResult(0, 1, Errors.NONE), result)
    assertEquals(10, metadata.txnTimeoutMs)
    assertEquals(time.milliseconds(), metadata.lastUpdateTimestamp)
    assertEquals(1, metadata.producerEpoch)
    assertEquals(0, metadata.pid)
  }

  private def mockPrepare(transactionId: String,
                          pid: Int,
                          epoch: Short,
                          txnTimeoutMs: Int,
                          transactionState: TransactionState,
                          runCallback: Boolean = false) = {
    val originalMetadata = new TransactionMetadata(pid,
      epoch,
      txnTimeoutMs,
      Ongoing,
      collection.mutable.Set.empty[TopicPartition],
      0,
      time.milliseconds())

    val prepareCommitMetadata = new TransactionMetadata(pid,
      epoch,
      txnTimeoutMs,
      transactionState,
      collection.mutable.Set.empty[TopicPartition],
      0,
      time.milliseconds())

    EasyMock.expect(transactionManager.isCoordinatorFor(transactionId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(originalMetadata))
      .once()

    EasyMock.expect(transactionManager.appendTransactionToLog(EasyMock.eq(transactionId),
      EasyMock.eq(prepareCommitMetadata),
      EasyMock.capture(capturedErrorsCallback)))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          if (runCallback) capturedErrorsCallback.getValue.apply(Errors.NONE)
        }
      }).once()
    prepareCommitMetadata
  }

  private def mockComplete(transactionId: String,
                           pid: Int,
                           epoch: Short,
                           txnTimeoutMs: Int,
                           transactionState: TransactionState) = {


    val prepareMetadata: TransactionMetadata = mockPrepare(transactionId, pid, epoch, txnTimeoutMs, transactionState, true)
    val finalState = if (transactionState == PrepareAbort) CompleteAbort else CompleteCommit

    EasyMock.expect(transactionManager.coordinatorEpochFor(transactionId))
      .andReturn(Some(0))

    EasyMock.expect(transactionMarkerChannelManager.addTxnMarkerRequest(
      EasyMock.eq(0),
      EasyMock.anyObject(),
      EasyMock.anyInt(),
      EasyMock.capture(capturedErrorsCallback)
    )).andAnswer(new IAnswer[Unit] {
      override def answer(): Unit = {
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      }
    })

    EasyMock.expect(transactionManager.getTransactionState(transactionId))
      .andReturn(Some(prepareMetadata))
      .once()

    val completedMetadata = new TransactionMetadata(pid,
      epoch,
      txnTimeoutMs,
      finalState,
      prepareMetadata.topicPartitions,
      prepareMetadata.transactionStartTime,
      prepareMetadata.lastUpdateTimestamp)

    EasyMock.expect(transactionManager.appendTransactionToLog(EasyMock.eq(transactionId),
      EasyMock.eq(completedMetadata),
      EasyMock.capture(capturedErrorsCallback)))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          capturedErrorsCallback.getValue.apply(Errors.NONE)
        }
      }).once()


  }


  def initPidMockCallback(ret: InitPidResult): Unit = {
    result = ret
  }

  def errorsCallback(ret: Errors): Unit = {
    error = ret
  }
}
