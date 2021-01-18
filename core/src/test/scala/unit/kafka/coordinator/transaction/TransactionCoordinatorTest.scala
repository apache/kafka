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
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{LogContext, MockTime, ProducerIdAndEpoch}
import org.easymock.{Capture, EasyMock}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.mutable

class TransactionCoordinatorTest {

  val time = new MockTime()

  var nextPid: Long = 0L
  val pidManager: ProducerIdManager = EasyMock.createNiceMock(classOf[ProducerIdManager])
  val transactionManager: TransactionStateManager = EasyMock.createNiceMock(classOf[TransactionStateManager])
  val transactionMarkerChannelManager: TransactionMarkerChannelManager = EasyMock.createNiceMock(classOf[TransactionMarkerChannelManager])
  val capturedTxn: Capture[TransactionMetadata] = EasyMock.newCapture()
  val capturedErrorsCallback: Capture[Errors => Unit] = EasyMock.newCapture()
  val capturedTxnTransitMetadata: Capture[TxnTransitMetadata] = EasyMock.newCapture()
  val brokerId = 0
  val coordinatorEpoch = 0
  private val transactionalId = "known"
  private val producerId = 10
  private val producerEpoch: Short = 1
  private val txnTimeoutMs = 1

  private val partitions = mutable.Set[TopicPartition](new TopicPartition("topic1", 0))
  private val scheduler = new MockScheduler(time)

  val coordinator = new TransactionCoordinator(brokerId,
    TransactionConfig(),
    scheduler,
    pidManager,
    transactionManager,
    transactionMarkerChannelManager,
    time,
    new LogContext)

  var result: InitProducerIdResult = _
  var error: Errors = Errors.NONE

  private def mockPidManager(): Unit = {
    EasyMock.expect(pidManager.generateProducerId()).andAnswer(() => {
      nextPid += 1
      nextPid - 1
    }).anyTimes()
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

    coordinator.handleInitProducerId("", txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1L, -1, Errors.INVALID_REQUEST), result)
    coordinator.handleInitProducerId("", txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1L, -1, Errors.INVALID_REQUEST), result)
  }

  @Test
  def shouldAcceptInitPidAndReturnNextPidWhenTransactionalIdIsNull(): Unit = {
    mockPidManager()
    EasyMock.replay(pidManager)

    coordinator.handleInitProducerId(null, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(0L, 0, Errors.NONE), result)
    coordinator.handleInitProducerId(null, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(1L, 0, Errors.NONE), result)
  }

  @Test
  def shouldInitPidWithEpochZeroForNewTransactionalId(): Unit = {
    initPidGenericMocks(transactionalId)

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(None))
      .once()

    EasyMock.expect(transactionManager.putTransactionStateIfNotExists(EasyMock.capture(capturedTxn)))
      .andAnswer(() => {
        assertTrue(capturedTxn.hasCaptured)
        Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, capturedTxn.getValue))
      }).once()

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.anyObject().asInstanceOf[TxnTransitMetadata],
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => capturedErrorsCallback.getValue.apply(Errors.NONE)).anyTimes()
    EasyMock.replay(pidManager, transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(nextPid - 1, 0, Errors.NONE), result)
  }

  @Test
  def shouldGenerateNewProducerIdIfNoStateAndProducerIdAndEpochProvided(): Unit = {
    initPidGenericMocks(transactionalId)

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(None))
      .once()

    EasyMock.expect(transactionManager.putTransactionStateIfNotExists(EasyMock.capture(capturedTxn)))
      .andAnswer(() => {
        assertTrue(capturedTxn.hasCaptured)
        Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, capturedTxn.getValue))
      }).once()

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.anyObject().asInstanceOf[TxnTransitMetadata],
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => capturedErrorsCallback.getValue.apply(Errors.NONE)).anyTimes()
    EasyMock.replay(pidManager, transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId, producerEpoch)),
      initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(nextPid - 1, 0, Errors.NONE), result)
  }

  @Test
  def shouldGenerateNewProducerIdIfEpochsExhausted(): Unit = {
    initPidGenericMocks(transactionalId)

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (Short.MaxValue - 1).toShort,
      (Short.MaxValue - 2).toShort, txnTimeoutMs, Empty, mutable.Set.empty, time.milliseconds(), time.milliseconds())

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.anyObject().asInstanceOf[TxnTransitMetadata],
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()
    )).andAnswer(() => capturedErrorsCallback.getValue.apply(Errors.NONE))

    EasyMock.replay(pidManager, transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
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

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
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

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
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
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, state, mutable.Set.empty, 0, 0)))))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
  }

  @Test
  def shouldRespondWithProducerFencedOnAddPartitionsWhenEpochsAreDifferent(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, 10, 9, 0, PrepareCommit, mutable.Set.empty, 0, 0)))))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)
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
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort,
      txnTimeoutMs, previousState, mutable.Set.empty, time.milliseconds(), time.milliseconds())

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
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, Empty, partitions, 0, 0)))))

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
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 10, 10, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReplyWithProducerFencedOnEndTxnWhenEpochIsNotSameAsTransaction(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort, 1, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteCommitAndResultIsCommit(): Unit ={
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteAbortAndResultIsAbort(): Unit ={
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.ABORT, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteAbortAndResultIsNotAbort(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteCommitAndResultIsNotCommit(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort,1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())
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
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort, 1, PrepareCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsPrepareAbort(): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId, 1, RecordBatch.NO_PRODUCER_EPOCH, 1, PrepareAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))
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
  def shouldReturnInvalidEpochOnEndTxnWhenEpochIsLarger(): Unit = {
    val serverProducerEpoch = 1.toShort
    verifyEndTxnEpoch(serverProducerEpoch, (serverProducerEpoch + 1).toShort)
  }

  @Test
  def shouldReturnInvalidEpochOnEndTxnWhenEpochIsSmaller(): Unit = {
    val serverProducerEpoch = 1.toShort
    verifyEndTxnEpoch(serverProducerEpoch, (serverProducerEpoch - 1).toShort)
  }

  private def verifyEndTxnEpoch(metadataEpoch: Short, requestEpoch: Short): Unit = {
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, producerId, producerId, metadataEpoch, 0, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, producerId, requestEpoch, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)
    EasyMock.verify(transactionManager)
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
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      (producerEpoch - 1).toShort, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)

    EasyMock.expect(transactionManager.putTransactionStateIfNotExists(EasyMock.anyObject[TransactionMetadata]()))
      .andReturn(Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))
      .anyTimes()

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .anyTimes()

    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (producerEpoch + 1).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(originalMetadata.prepareAbortOrCommit(PrepareAbort, time.milliseconds())),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => capturedErrorsCallback.getValue.apply(Errors.NONE))

    EasyMock.replay(transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldFailToAbortTransactionOnHandleInitPidWhenProducerEpochIsSmaller(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      (producerEpoch - 1).toShort, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)

    EasyMock.expect(transactionManager.putTransactionStateIfNotExists(EasyMock.anyObject[TransactionMetadata]()))
      .andReturn(Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))
      .anyTimes()

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .times(1)

    val bumpedTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (producerEpoch + 2).toShort,
      (producerEpoch - 1).toShort, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, bumpedTxnMetadata))))
      .times(1)

    EasyMock.replay(transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.PRODUCER_FENCED), result)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldNotRepeatedlyBumpEpochDueToInitPidDuringOngoingTxnIfAppendToLogFails(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)
      .anyTimes()

    EasyMock.expect(transactionManager.putTransactionStateIfNotExists(EasyMock.anyObject[TransactionMetadata]()))
      .andReturn(Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))
      .anyTimes()

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andAnswer(() => Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .anyTimes()

    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (producerEpoch + 1).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    val txnTransitMetadata = originalMetadata.prepareAbortOrCommit(PrepareAbort, time.milliseconds())
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(txnTransitMetadata),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => {
      capturedErrorsCallback.getValue.apply(Errors.NOT_ENOUGH_REPLICAS)
      txnMetadata.pendingState = None
    }).times(2)

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(txnTransitMetadata),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => {
      capturedErrorsCallback.getValue.apply(Errors.NONE)

      // For the successful call, execute the state transitions that would happen in appendTransactionToLog()
      txnMetadata.completeTransitionTo(txnTransitMetadata)
      txnMetadata.prepareComplete(time.milliseconds())
    }).once()

    EasyMock.replay(transactionManager)

    // For the first two calls, verify that the epoch was only bumped once
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1, -1, Errors.NOT_ENOUGH_REPLICAS), result)

    assertEquals((producerEpoch + 1).toShort, txnMetadata.producerEpoch)
    assertTrue(txnMetadata.hasFailedEpochFence)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1, -1, Errors.NOT_ENOUGH_REPLICAS), result)

    assertEquals((producerEpoch + 1).toShort, txnMetadata.producerEpoch)
    assertTrue(txnMetadata.hasFailedEpochFence)

    // For the last, successful call, verify that the epoch was not bumped further
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)

    assertEquals((producerEpoch + 1).toShort, txnMetadata.producerEpoch)
    assertFalse(txnMetadata.hasFailedEpochFence)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldUseLastEpochToFenceWhenEpochsAreExhausted(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (Short.MaxValue - 1).toShort,
      (Short.MaxValue - 2).toShort, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    assertTrue(txnMetadata.isProducerEpochExhausted)

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)

    EasyMock.expect(transactionManager.putTransactionStateIfNotExists(EasyMock.anyObject[TransactionMetadata]()))
      .andReturn(Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))
      .anyTimes()

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .times(2)

    val postFenceTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, Short.MaxValue,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, PrepareAbort, partitions, time.milliseconds(), time.milliseconds())

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, postFenceTxnMetadata))))
      .once()

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(TxnTransitMetadata(
        producerId = producerId,
        lastProducerId = producerId,
        producerEpoch = Short.MaxValue,
        lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
        txnTimeoutMs = txnTimeoutMs,
        txnState = PrepareAbort,
        topicPartitions = partitions.toSet,
        txnStartTimestamp = time.milliseconds(),
        txnLastUpdateTimestamp = time.milliseconds())),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => capturedErrorsCallback.getValue.apply(Errors.NONE))

    EasyMock.replay(transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(Short.MaxValue, txnMetadata.producerEpoch)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
    EasyMock.verify(transactionManager)
  }

  @Test
  def testInitProducerIdWithNoLastProducerData(): Unit = {
    // If the metadata doesn't include the previous producer data (for example, if it was written to the log by a broker
    // on an old version), the retry case should fail
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, RecordBatch.NO_PRODUCER_ID, (producerEpoch + 1).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds)

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))).once
    EasyMock.replay(transactionManager)

    // Simulate producer trying to continue after new producer has already been initialized
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId, producerEpoch)),
      initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Errors.PRODUCER_FENCED), result)
  }

  @Test
  def testFenceProducerWhenMappingExistsWithDifferentProducerId(): Unit = {
    // Existing transaction ID maps to new producer ID
    val txnMetadata = new TransactionMetadata(transactionalId, producerId + 1, producerId, producerEpoch,
      (producerEpoch - 1).toShort, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds)

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))).once
    EasyMock.replay(transactionManager)

    // Simulate producer trying to continue after new producer has already been initialized
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId, producerEpoch)),
      initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Errors.PRODUCER_FENCED), result)
  }

  @Test
  def testInitProducerIdWithCurrentEpochProvided(): Unit = {
    mockPidManager()

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, 10,
      9, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds)

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))).times(2)

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.anyObject().asInstanceOf[TxnTransitMetadata],
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => {
      capturedErrorsCallback.getValue.apply(Errors.NONE)
      txnMetadata.pendingState = None
    }).times(2)

    EasyMock.replay(pidManager, transactionManager)

    // Re-initialization should succeed and bump the producer epoch
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId, 10)),
      initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(producerId, 11, Errors.NONE), result)

    // Simulate producer retrying after successfully re-initializing but failing to receive the response
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId, 10)),
      initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(producerId, 11, Errors.NONE), result)
  }

  @Test
  def testInitProducerIdStaleCurrentEpochProvided(): Unit = {
    mockPidManager()

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, 10,
      9, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds)

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))).times(2)

    val capturedTxnTransitMetadata = Capture.newInstance[TxnTransitMetadata]
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.capture(capturedTxnTransitMetadata),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => {
      capturedErrorsCallback.getValue.apply(Errors.NONE)
      txnMetadata.pendingState = None
      txnMetadata.producerEpoch = capturedTxnTransitMetadata.getValue.producerEpoch
      txnMetadata.lastProducerEpoch = capturedTxnTransitMetadata.getValue.lastProducerEpoch
    }).times(2)

    EasyMock.replay(pidManager, transactionManager)

    // With producer epoch at 10, new producer calls InitProducerId and should get epoch 11
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(producerId, 11, Errors.NONE), result)

    // Simulate old producer trying to continue from epoch 10
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId, 10)),
      initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Errors.PRODUCER_FENCED), result)
  }

  @Test
  def testRetryInitProducerIdAfterProducerIdRotation(): Unit = {
    // Existing transaction ID maps to new producer ID
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (Short.MaxValue - 1).toShort,
      (Short.MaxValue - 2).toShort, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds)

    EasyMock.expect(pidManager.generateProducerId())
      .andReturn(producerId + 1)
      .anyTimes()

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))).times(2)

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.capture(capturedTxnTransitMetadata),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => {
      capturedErrorsCallback.getValue.apply(Errors.NONE)
      txnMetadata.pendingState = None
      txnMetadata.producerId = capturedTxnTransitMetadata.getValue.producerId
      txnMetadata.lastProducerId = capturedTxnTransitMetadata.getValue.lastProducerId
      txnMetadata.producerEpoch = capturedTxnTransitMetadata.getValue.producerEpoch
      txnMetadata.lastProducerEpoch = capturedTxnTransitMetadata.getValue.lastProducerEpoch
    }).once

    EasyMock.replay(pidManager, transactionManager)

    // Bump epoch and cause producer ID to be rotated
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId,
      (Short.MaxValue - 1).toShort)), initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(producerId + 1, 0, Errors.NONE), result)

    // Simulate producer retrying old request after producer bump
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId,
      (Short.MaxValue - 1).toShort)), initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(producerId + 1, 0, Errors.NONE), result)
  }

  @Test
  def testInitProducerIdWithInvalidEpochAfterProducerIdRotation(): Unit = {
    // Existing transaction ID maps to new producer ID
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (Short.MaxValue - 1).toShort,
      (Short.MaxValue - 2).toShort, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds)

    EasyMock.expect(pidManager.generateProducerId())
      .andReturn(producerId + 1)
      .anyTimes()

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))).times(2)

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.capture(capturedTxnTransitMetadata),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => {
      capturedErrorsCallback.getValue.apply(Errors.NONE)
      txnMetadata.pendingState = None
      txnMetadata.producerId = capturedTxnTransitMetadata.getValue.producerId
      txnMetadata.lastProducerId = capturedTxnTransitMetadata.getValue.lastProducerId
      txnMetadata.producerEpoch = capturedTxnTransitMetadata.getValue.producerEpoch
      txnMetadata.lastProducerEpoch = capturedTxnTransitMetadata.getValue.lastProducerEpoch
    }).once

    EasyMock.replay(pidManager, transactionManager)

    // Bump epoch and cause producer ID to be rotated
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId,
      (Short.MaxValue - 1).toShort)), initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(producerId + 1, 0, Errors.NONE), result)

    // Validate that producer with old producer ID and stale epoch is fenced
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId,
      (Short.MaxValue - 2).toShort)), initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Errors.PRODUCER_FENCED), result)
  }

  @Test
  def shouldRemoveTransactionsForPartitionOnEmigration(): Unit = {
    EasyMock.expect(transactionManager.removeTransactionsForTxnTopicPartition(0, coordinatorEpoch))
    EasyMock.expect(transactionMarkerChannelManager.removeMarkersForTxnTopicPartition(0))
    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.onResignation(0, Some(coordinatorEpoch))

    EasyMock.verify(transactionManager, transactionMarkerChannelManager)
  }

  @Test
  def shouldAbortExpiredTransactionsInOngoingStateAndBumpEpoch(): Unit = {
    val now = time.milliseconds()
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now)

    EasyMock.expect(transactionManager.timedOutTransactions())
      .andReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .times(2)

    val expectedTransition = TxnTransitMetadata(producerId, producerId, (producerEpoch + 1).toShort, RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs, PrepareAbort, partitions.toSet, now, now + TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)

    EasyMock.expect(transactionManager.appendTransactionToLog(EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(expectedTransition),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => {}).once()

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.startup(false)
    time.sleep(TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)
    scheduler.tick()
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldNotAcceptSmallerEpochDuringTransactionExpiration(): Unit = {
    val now = time.milliseconds()
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now)

    EasyMock.expect(transactionManager.timedOutTransactions())
      .andReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val bumpedTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (producerEpoch + 2).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now)
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, bumpedTxnMetadata))))

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    def checkOnEndTransactionComplete(txnIdAndPidEpoch: TransactionalIdAndProducerIdEpoch)(error: Errors): Unit = {
      assertEquals(Errors.PRODUCER_FENCED, error)
    }
    coordinator.abortTimedOutTransactions(checkOnEndTransactionComplete)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldNotAbortExpiredTransactionsThatHaveAPendingStateTransition(): Unit = {
    val metadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    metadata.prepareAbortOrCommit(PrepareCommit, time.milliseconds())

    EasyMock.expect(transactionManager.timedOutTransactions())
      .andReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata)))).once()

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.startup(false)
    time.sleep(TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)
    scheduler.tick()
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldNotBumpEpochWhenAbortingExpiredTransactionIfAppendToLogFails(): Unit = {
    val now = time.milliseconds()
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now)


    EasyMock.expect(transactionManager.timedOutTransactions())
      .andReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .times(2)

    val txnMetadataAfterAppendFailure = new TransactionMetadata(transactionalId, producerId, producerId, (producerEpoch + 1).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now)
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andAnswer(() => Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadataAfterAppendFailure))))
      .once

    val bumpedEpoch = (producerEpoch + 1).toShort
    val expectedTransition = TxnTransitMetadata(producerId, producerId, bumpedEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs,
      PrepareAbort, partitions.toSet, now, now + TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)

    EasyMock.expect(transactionManager.appendTransactionToLog(EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(expectedTransition),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => capturedErrorsCallback.getValue.apply(Errors.NOT_ENOUGH_REPLICAS)).once()

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.startup(false)
    time.sleep(TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)
    scheduler.tick()
    EasyMock.verify(transactionManager)

    assertEquals((producerEpoch + 1).toShort, txnMetadataAfterAppendFailure.producerEpoch)
    assertTrue(txnMetadataAfterAppendFailure.hasFailedEpochFence)
  }

  @Test
  def shouldNotBumpEpochWithPendingTransaction(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    txnMetadata.prepareAbortOrCommit(PrepareCommit, time.milliseconds())

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    EasyMock.replay(transactionManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId, 10)),
      initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Errors.CONCURRENT_TRANSACTIONS), result)

    EasyMock.verify(transactionManager)
  }

  private def validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(state: TransactionState): Unit = {
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()

    val metadata = new TransactionMetadata(transactionalId, 0, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, state, mutable.Set[TopicPartition](new TopicPartition("topic", 1)), 0, 0)
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata)))).anyTimes()

    EasyMock.replay(transactionManager)

    coordinator.handleInitProducerId(transactionalId, 10, None, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
  }

  private def validateIncrementEpochAndUpdateMetadata(state: TransactionState): Unit = {
    EasyMock.expect(pidManager.generateProducerId())
      .andReturn(producerId)
      .anyTimes()

    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)

    val metadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, state, mutable.Set.empty[TopicPartition], time.milliseconds(), time.milliseconds())
    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata))))

    val capturedNewMetadata: Capture[TxnTransitMetadata] = EasyMock.newCapture()
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.capture(capturedNewMetadata),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject()
    )).andAnswer(() => {
      metadata.completeTransitionTo(capturedNewMetadata.getValue)
      capturedErrorsCallback.getValue.apply(Errors.NONE)
    })

    EasyMock.replay(pidManager, transactionManager)

    val newTxnTimeoutMs = 10
    coordinator.handleInitProducerId(transactionalId, newTxnTimeoutMs, None, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(producerId, (producerEpoch + 1).toShort, Errors.NONE), result)
    assertEquals(newTxnTimeoutMs, metadata.txnTimeoutMs)
    assertEquals(time.milliseconds(), metadata.txnLastUpdateTimestamp)
    assertEquals((producerEpoch + 1).toShort, metadata.producerEpoch)
    assertEquals(producerId, metadata.producerId)
  }

  private def mockPrepare(transactionState: TransactionState, runCallback: Boolean = false): TransactionMetadata = {
    val now = time.milliseconds()
    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs, Ongoing, partitions, now, now)

    val transition = TxnTransitMetadata(producerId, producerId, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs,
      transactionState, partitions.toSet, now, now)

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, originalMetadata))))
      .once()
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(transition),
      EasyMock.capture(capturedErrorsCallback),
      EasyMock.anyObject())
    ).andAnswer(() => {
      if (runCallback)
        capturedErrorsCallback.getValue.apply(Errors.NONE)
    }).once()

    new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs, transactionState, partitions, time.milliseconds(), time.milliseconds())
  }

  def initProducerIdMockCallback(ret: InitProducerIdResult): Unit = {
    result = ret
  }

  def errorsCallback(ret: Errors): Unit = {
    error = ret
  }
}
