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

import kafka.utils.MockScheduler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{LogContext, MockTime}
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
  val coordinatorEpoch = 0
  private val transactionalId = "known"
  private val producerId = 10
  private val producerEpoch:Short = 1
  private val txnTimeoutMs = 1

  private val partitions = mutable.Set[TopicPartition](new TopicPartition("topic1", 0))
  private val scheduler = new MockScheduler(time)

  val coordinator = new TransactionCoordinator(brokerId,
    scheduler,
    pidManager,
    transactionManager,
    transactionMarkerChannelManager,
    time,
    new LogContext)

  var result: InitProducerIdResult = _
  var error: Errors = Errors.NONE

  private def mockPidManager(): Unit = {
    EasyMock.expect(pidManager.generateProducerId())
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
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)
      .anyTimes()
  }

  @Test
  def shouldReturnInvalidRequestWhenTransactionalIdIsEmpty(): Unit = {
    mockPidManager()
    EasyMock.replay(pidManager)

    coordinator.handleInitProducerId("", txnTimeoutMs, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1L, -1, Errors.INVALID_REQUEST), result)
    coordinator.handleInitProducerId("", txnTimeoutMs, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1L, -1, Errors.INVALID_REQUEST), result)
  }

  @Test
  def shouldAcceptInitPidAndReturnNextPidWhenTransactionalIdIsNull(): Unit = {
    mockPidManager()
    EasyMock.replay(pidManager)

    coordinator.handleInitProducerId(null, txnTimeoutMs, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(0L, 0, Errors.NONE), result)
    coordinator.handleInitProducerId(null, txnTimeoutMs, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(1L, 0, Errors.NONE), result)
  }

  @Test
  def shouldInitPidWithEpochZeroForNewTransactionalId(): Unit = {
    initPidGenericMocks(transactionalId)

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(None))
      .once()

    EasyMock.expect(transactionManager.putTransactionStateIfNotExists(EasyMock.eq(transactionalId), EasyMock.capture(capturedTxn)))
      .andAnswer(new IAnswer[Either[Errors, CoordinatorEpochAndTxnMetadata]] {
        override def answer(): Either[Errors, CoordinatorEpochAndTxnMetadata] = {
          assertTrue(capturedTxn.hasCaptured)
          Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, capturedTxn.getValue))
        }
      })
      .once()

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.anyObject().asInstanceOf[TxnTransitMetadata],
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          capturedErrorsCallback.getValue.apply(Errors.NONE)
        }
      })
      .anyTimes()
    EasyMock.replay(pidManager, transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(nextPid - 1, 0, Errors.NONE), result)
  }

  @Test
  def shouldGenerateNewProducerIdIfEpochsExhausted(): Unit = {
    initPidGenericMocks(transactionalId)

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, (Short.MaxValue - 1).toShort,
      txnTimeoutMs, Empty, mutable.Set.empty, time.milliseconds(), time.milliseconds())

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.anyObject().asInstanceOf[TxnTransitMetadata],
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()
    )).andAnswer(new IAnswer[Unit] {
      override def answer(): Unit = {
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      }
    })

    EasyMock.replay(pidManager, transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, initProducerIdMockCallback)
    assertNotEquals(producerId, result.producerId)
    assertEquals(0, result.producerEpoch)
    assertEquals(Errors.NONE, result.error)
  }

  @Test
  def shouldRespondWithNotCoordinatorOnInitPidWhenNotCoordinator(): Unit = {
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)
      .anyTimes()
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Left(Errors.NOT_COORDINATOR))
    EasyMock.replay(transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1, -1, Errors.NOT_COORDINATOR), result)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnInitPidWhenCoordintorLoading(): Unit = {
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)
      .anyTimes()
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))
    EasyMock.replay(transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1, -1, Errors.COORDINATOR_LOAD_IN_PROGRESS), result)
  }

  @Test
  def shouldRespondWithInvalidPidMappingOnAddPartitionsToTransactionWhenTransactionalIdNotPresent(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(None))
    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 1, partitions, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
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
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Left(Errors.NOT_COORDINATOR))
    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 1, partitions, errorsCallback)
    assertEquals(Errors.NOT_COORDINATOR, error)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnAddPartitionsWhenCoordintorLoading(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 1, partitions, errorsCallback)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, error)
  }

  @Test
  def shouldRespondWithConcurrentTransactionsOnAddPartitionsWhenStateIsPrepareCommit(): Unit = {
    validateConcurrentTransactions(PrepareCommit)
  }

  @Test
  def shouldRespondWithConcurrentTransactionOnAddPartitionsWhenStateIsPrepareAbort(): Unit = {
    validateConcurrentTransactions(PrepareAbort)
  }

  def validateConcurrentTransactions(state: TransactionState): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, 0, 0, 0, state, mutable.Set.empty, 0, 0)))))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
  }

  @Test
  def shouldRespondWithInvalidTnxProduceEpochOnAddPartitionsWhenEpochsAreDifferent(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, 0, 10, 0, PrepareCommit, mutable.Set.empty, 0, 0)))))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_EPOCH, error)
  }

  @Test
  def shouldAppendNewMetadataToLogOnAddPartitionsWhenPartitionsAdded(): Unit = {
    validateSuccessfulAddPartitions(Empty)
  }

  @Test
  def shouldRespondWithSuccessOnAddPartitionsWhenStateIsOngoing(): Unit = {
    validateSuccessfulAddPartitions(Ongoing)
  }

  @Test
  def shouldRespondWithSuccessOnAddPartitionsWhenStateIsCompleteCommit(): Unit = {
    validateSuccessfulAddPartitions(CompleteCommit)
  }

  @Test
  def shouldRespondWithSuccessOnAddPartitionsWhenStateIsCompleteAbort(): Unit = {
    validateSuccessfulAddPartitions(CompleteAbort)
  }

  def validateSuccessfulAddPartitions(previousState: TransactionState): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, previousState,
      mutable.Set.empty, time.milliseconds(), time.milliseconds())

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.anyObject().asInstanceOf[TxnTransitMetadata],
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()
    ))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, producerId, producerEpoch, partitions, errorsCallback)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldRespondWithErrorsNoneOnAddPartitionWhenNoErrorsAndPartitionsTheSame(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, 0, 0, 0, Empty, partitions, 0, 0)))))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)

  }

  @Test
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenTxnIdDoesntExist(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(None))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenPidDosentMatchMapped(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, 10, 0, 0, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReplyWithProducerFencedOnEndTxnWhenEpochIsNotSameAsTransaction(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, 1, 1, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_EPOCH, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteCommitAndResultIsCommit(): Unit ={
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, 1, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteAbortAndResultIsAbort(): Unit ={
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, 1, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.ABORT, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteAbortAndResultIsNotAbort(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, 1, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteCommitAndResultIsNotCommit(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, 1, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.ABORT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnConcurrentTxnRequestOnEndTxnRequestWhenStatusIsPrepareCommit(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, 1, 1, PrepareCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsPrepareAbort(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, 1, 1, PrepareAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldAppendPrepareCommitToLogOnEndTxnWhenStatusIsOngoingAndResultIsCommit(): Unit = {
    mockPrepare(PrepareCommit)

    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.COMMIT, errorsCallback)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldAppendPrepareAbortToLogOnEndTxnWhenStatusIsOngoingAndResultIsAbort(): Unit = {
    mockPrepare(PrepareAbort)

    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.ABORT, errorsCallback)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsNull(): Unit = {
    coordinator.handleEndTransaction(null, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsEmpty(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Left(Errors.NOT_COORDINATOR))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction("", 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithNotCoordinatorOnEndTxnWhenIsNotCoordinatorForId(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Left(Errors.NOT_COORDINATOR))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.NOT_COORDINATOR, error)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnEndTxnWhenCoordinatorIsLoading(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))

    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, errorsCallback)
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
    validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(PrepareCommit)
  }

  @Test
  def shouldWaitForCommitToCompleteOnHandleInitPidAndExistingTransactionInPrepareAbortState(): Unit ={
    validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(PrepareAbort)
  }

  @Test
  def shouldAbortTransactionOnHandleInitPidWhenExistingTransactionInOngoingState(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, Ongoing,
      partitions, time.milliseconds(), time.milliseconds())

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)

    EasyMock.expect(transactionManager.putTransactionStateIfNotExists(EasyMock.eq(transactionalId), EasyMock.anyObject[TransactionMetadata]()))
      .andReturn(Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))
      .anyTimes()

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .anyTimes()

    val originalMetadata = new TransactionMetadata(transactionalId, producerId, (producerEpoch + 1).toShort,
      txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(originalMetadata.prepareAbortOrCommit(PrepareAbort, time.milliseconds())),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          capturedErrorsCallback.getValue.apply(Errors.NONE)
        }
      })

    EasyMock.replay(transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldUseLastEpochToFenceWhenEpochsAreExhausted(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, (Short.MaxValue - 1).toShort,
      txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    assertTrue(txnMetadata.isProducerEpochExhausted)

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)

    EasyMock.expect(transactionManager.putTransactionStateIfNotExists(EasyMock.eq(transactionalId), EasyMock.anyObject[TransactionMetadata]()))
      .andReturn(Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))
      .anyTimes()

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .anyTimes()

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(TxnTransitMetadata(
        producerId = producerId,
        producerEpoch = Short.MaxValue,
        txnTimeoutMs = txnTimeoutMs,
        txnState = PrepareAbort,
        topicPartitions = partitions.toSet,
        txnStartTimestamp = time.milliseconds(),
        txnLastUpdateTimestamp = time.milliseconds())),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          capturedErrorsCallback.getValue.apply(Errors.NONE)
        }
      })

    EasyMock.replay(transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, initProducerIdMockCallback)
    assertEquals(Short.MaxValue, txnMetadata.producerEpoch)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldRemoveTransactionsForPartitionOnEmigration(): Unit = {
    EasyMock.expect(transactionManager.removeTransactionsForTxnTopicPartition(0, coordinatorEpoch))
    EasyMock.expect(transactionMarkerChannelManager.removeMarkersForTxnTopicPartition(0))
    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.handleTxnEmigration(0, coordinatorEpoch)

    EasyMock.verify(transactionManager, transactionMarkerChannelManager)
  }

  @Test
  def shouldAbortExpiredTransactionsInOngoingState(): Unit = {
    val now = time.milliseconds()
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, Ongoing,
      partitions, now, now)


    EasyMock.expect(transactionManager.timedOutTransactions())
      .andReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .once()

    val expectedTransition = TxnTransitMetadata(producerId, producerEpoch, txnTimeoutMs, PrepareAbort,
      partitions.toSet, now, now + TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)

    EasyMock.expect(transactionManager.appendTransactionToLog(EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(expectedTransition),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {}
      })
    .once()

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.startup(false)
    time.sleep(TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)
    scheduler.tick()
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldNotAbortExpiredTransactionsThatHaveAPendingStateTransition(): Unit = {
    val metadata = new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, Ongoing,
      partitions, time.milliseconds(), time.milliseconds())
    metadata.prepareAbortOrCommit(PrepareCommit, time.milliseconds())

    EasyMock.expect(transactionManager.timedOutTransactions())
      .andReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata))))

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.startup(false)
    time.sleep(TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)
    scheduler.tick()
    EasyMock.verify(transactionManager)
  }

  private def validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(state: TransactionState) = {
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()

    val metadata = new TransactionMetadata(transactionalId, 0, 0, 0, state, mutable.Set[TopicPartition](new TopicPartition("topic", 1)), 0, 0)
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata)))).anyTimes()

    EasyMock.replay(transactionManager)

    coordinator.handleInitProducerId(transactionalId, 10, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
  }

  private def validateIncrementEpochAndUpdateMetadata(state: TransactionState) = {
    EasyMock.expect(pidManager.generateProducerId())
      .andReturn(producerId)
      .anyTimes()

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)

    val metadata = new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, state, mutable.Set.empty[TopicPartition], time.milliseconds(), time.milliseconds())
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata))))

    val capturedNewMetadata: Capture[TxnTransitMetadata] = EasyMock.newCapture()
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.capture(capturedNewMetadata),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()
    )).andAnswer(new IAnswer[Unit] {
      override def answer(): Unit = {
        metadata.completeTransitionTo(capturedNewMetadata.getValue)
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      }
    })

    EasyMock.replay(pidManager, transactionManager)

    val newTxnTimeoutMs = 10
    coordinator.handleInitProducerId(transactionalId, newTxnTimeoutMs, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(producerId, (producerEpoch + 1).toShort, Errors.NONE), result)
    assertEquals(newTxnTimeoutMs, metadata.txnTimeoutMs)
    assertEquals(time.milliseconds(), metadata.txnLastUpdateTimestamp)
    assertEquals((producerEpoch + 1).toShort, metadata.producerEpoch)
    assertEquals(producerId, metadata.producerId)
  }

  private def mockPrepare(transactionState: TransactionState, runCallback: Boolean = false): TransactionMetadata = {
    val now = time.milliseconds()
    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs,
      Ongoing, partitions, now, now)

    val transition = TxnTransitMetadata(producerId, producerEpoch, txnTimeoutMs, transactionState,
      partitions.toSet, now, now)

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, originalMetadata))))
      .once()
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(transition),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          if (runCallback)
            capturedErrorsCallback.getValue.apply(Errors.NONE)
        }
      }).once()

    new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, transactionState, partitions,
      time.milliseconds(), time.milliseconds())
  }

  def initProducerIdMockCallback(ret: InitProducerIdResult): Unit = {
    result = ret
  }

  def errorsCallback(ret: Errors): Unit = {
    error = ret
  }
}
