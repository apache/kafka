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
import kafka.utils.MockScheduler
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
  val coordinatorEpoch = 0
  private val transactionalId = "known"
  private val pid = 10
  private val epoch:Short = 1
  private val txnTimeoutMs = 1

  private val txnMarkerPurgatory = new DelayedOperationPurgatory[DelayedTxnMarker]("test", new MockTimer, reaperEnabled = false)
  private val partitions = mutable.Set[TopicPartition](new TopicPartition("topic1", 0))
  private val scheduler = new MockScheduler(time)

  val coordinator: TransactionCoordinator = new TransactionCoordinator(brokerId,
    scheduler,
    pidManager,
    transactionManager,
    transactionMarkerChannelManager,
    txnMarkerPurgatory,
    time)

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

    coordinator.handleInitProducerId("", txnTimeoutMs, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(0L, 0, Errors.NONE), result)
    coordinator.handleInitProducerId("", txnTimeoutMs, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(1L, 0, Errors.NONE), result)
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
      .andAnswer(new IAnswer[Option[CoordinatorEpochAndTxnMetadata]] {
        override def answer(): Option[CoordinatorEpochAndTxnMetadata] = {
          if (capturedTxn.hasCaptured)
            Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, capturedTxn.getValue))
          else
            None
        }
      })
      .once()

    EasyMock.expect(transactionManager.addTransaction(EasyMock.eq(transactionalId), EasyMock.capture(capturedTxn)))
      .andAnswer(new IAnswer[CoordinatorEpochAndTxnMetadata] {
        override def answer(): CoordinatorEpochAndTxnMetadata = {
          CoordinatorEpochAndTxnMetadata(coordinatorEpoch, capturedTxn.getValue)
        }
      })
      .once()

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.anyObject().asInstanceOf[TransactionMetadataTransition],
      EasyMock.capture(capturedErrorsCallback)))
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
  def shouldRespondWithNotCoordinatorOnInitPidWhenNotCoordinatorForId(): Unit = {
    mockPidManager()
    EasyMock.replay(pidManager)
    coordinator.handleInitProducerId("some-pid", txnTimeoutMs, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1, -1, Errors.NOT_COORDINATOR), result)
  }

  @Test
  def shouldRespondWithInvalidPidMappingOnAddPartitionsToTransactionWhenTransactionalIdNotPresent(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)

    EasyMock.expect(transactionManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(None)
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
    coordinator.handleAddPartitionsToTransaction("txn", 0L, 1, partitions, errorsCallback)
    assertEquals(Errors.NOT_COORDINATOR, error)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnAddPartitionsWhenCoordintorLoading(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.isCoordinatorLoadingInProgress(transactionalId))
    .andReturn(true)

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
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(0, 0, 0, state, mutable.Set.empty, 0, 0))))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
  }

  @Test
  def shouldRespondWithInvalidTnxProduceEpochOnAddPartitionsWhenEpochsAreDifferent(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(0, 10, 0, PrepareCommit, mutable.Set.empty, 0, 0))))

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
    val txnMetadata = new TransactionMetadata(pid, epoch, txnTimeoutMs, previousState, mutable.Set.empty, time.milliseconds(), time.milliseconds())

    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))

    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.anyObject().asInstanceOf[TransactionMetadataTransition],
      EasyMock.capture(capturedErrorsCallback)
    ))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, pid, epoch, partitions, errorsCallback)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldRespondWithErrorsNoneOnAddPartitionWhenNoErrorsAndPartitionsTheSame(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(0, 0, 0, Empty, partitions, 0, 0))))

    EasyMock.replay(transactionManager)

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)

  }

  @Test
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenTxnIdDoesntExist(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId)).andReturn(None)
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenPidDosentMatchMapped(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(10, 0, 0, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds()))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReplyWithProducerFencedOnEndTxnWhenEpochIsNotSameAsTransaction(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(pid, 1, 1, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds()))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, pid, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_EPOCH, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteCommitAndResultIsCommit(): Unit ={
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(pid, 1, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds()))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteAbortAndResultIsAbort(): Unit ={
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(pid, 1, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds()))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, pid, 1, TransactionResult.ABORT, errorsCallback)
    assertEquals(Errors.NONE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteAbortAndResultIsNotAbort(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(pid, 1, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds()))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteCommitAndResultIsNotCommit(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(pid, 1, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds()))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, pid, 1, TransactionResult.ABORT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnConcurrentTxnRequestOnEndTxnRequestWhenStatusIsPrepareCommit(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(pid, 1, 1, PrepareCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds()))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsPrepareAbort(): Unit = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(pid, 1, 1, PrepareAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds()))))
    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, pid, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    EasyMock.verify(transactionManager)
  }


  @Test
  def shouldAppendPrepareCommitToLogOnEndTxnWhenStatusIsOngoingAndResultIsCommit(): Unit = {
    mockPrepare(PrepareCommit)

    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, pid, epoch, TransactionResult.COMMIT, errorsCallback)

    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldAppendPrepareAbortToLogOnEndTxnWhenStatusIsOngoingAndResultIsAbort(): Unit = {
    mockPrepare(PrepareAbort)

    EasyMock.replay(transactionManager)

    coordinator.handleEndTransaction(transactionalId, pid, epoch, TransactionResult.ABORT, errorsCallback)
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
    validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(PrepareCommit)
  }

  @Test
  def shouldWaitForCommitToCompleteOnHandleInitPidAndExistingTransactionInPrepareAbortState(): Unit ={
    validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(PrepareAbort)
  }

  @Test
  def shouldAbortTransactionOnHandleInitPidWhenExistingTransactionInOngoingState(): Unit = {
    val txnMetadata = new TransactionMetadata(pid, epoch, txnTimeoutMs, Ongoing, partitions, 0, 0)

    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
      .anyTimes()
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)

    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))
      .anyTimes()

    val originalMetadata = new TransactionMetadata(pid, epoch, txnTimeoutMs, Ongoing, partitions, 0, 0)
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(originalMetadata.prepareAbortOrCommit(PrepareAbort, time.milliseconds())),
      EasyMock.capture(capturedErrorsCallback)))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          capturedErrorsCallback.getValue.apply(Errors.NONE)
        }
      })

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldRemoveTransactionsForPartitionOnEmigration(): Unit = {
    EasyMock.expect(transactionManager.removeTransactionsForTxnTopicPartition(0))
    EasyMock.expect(transactionMarkerChannelManager.removeMarkersForTxnTopicPartition(0))
    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.handleTxnEmigration(0)

    EasyMock.verify(transactionManager, transactionMarkerChannelManager)
  }

  @Test
  def shouldAbortExpiredTransactionsInOngoingState(): Unit = {
    val txnMetadata = new TransactionMetadata(pid, epoch, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())

    EasyMock.expect(transactionManager.transactionsToExpire())
      .andReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, pid, epoch)))
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))
      .once()

    val newMetadata = txnMetadata.copy().prepareAbortOrCommit(PrepareAbort, time.milliseconds() + TransactionStateManager.DefaultRemoveExpiredTransactionsIntervalMs)

    EasyMock.expect(transactionManager.appendTransactionToLog(EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(newMetadata),
      EasyMock.capture(capturedErrorsCallback)))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {}
      })
    .once()

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.startup(false)
    time.sleep(TransactionStateManager.DefaultRemoveExpiredTransactionsIntervalMs)
    scheduler.tick()
    EasyMock.verify(transactionManager)
  }

  @Test
  def shouldNotAbortExpiredTransactionsThatHaveAPendingStateTransition(): Unit = {
    val metadata = new TransactionMetadata(pid, epoch, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    metadata.prepareAbortOrCommit(PrepareCommit, time.milliseconds())

    EasyMock.expect(transactionManager.transactionsToExpire())
      .andReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, pid, epoch)))

    EasyMock.replay(transactionManager, transactionMarkerChannelManager)

    coordinator.startup(false)
    time.sleep(TransactionStateManager.DefaultRemoveExpiredTransactionsIntervalMs)
    scheduler.tick()
    EasyMock.verify(transactionManager)
  }

  private def validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(state: TransactionState) = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true).anyTimes()
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true).anyTimes()

    val metadata = new TransactionMetadata(0, 0, 0, state, mutable.Set[TopicPartition](new TopicPartition("topic", 1)), 0, 0)
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata))).anyTimes()

    EasyMock.replay(transactionManager)

    coordinator.handleInitProducerId(transactionalId, 10, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
  }

  private def validateIncrementEpochAndUpdateMetadata(state: TransactionState) = {
    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
    EasyMock.expect(transactionManager.validateTransactionTimeoutMs(EasyMock.anyInt()))
      .andReturn(true)

    val metadata = new TransactionMetadata(pid, epoch, txnTimeoutMs, state, mutable.Set.empty[TopicPartition], time.milliseconds(), time.milliseconds())
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata)))

    val capturedNewMetadata: Capture[TransactionMetadataTransition] = EasyMock.newCapture()
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.capture(capturedNewMetadata),
      EasyMock.capture(capturedErrorsCallback)
    )).andAnswer(new IAnswer[Unit] {
      override def answer(): Unit = {
        metadata.completeTransitionTo(capturedNewMetadata.getValue)
        capturedErrorsCallback.getValue.apply(Errors.NONE)
      }
    })

    EasyMock.replay(transactionManager)

    val newTxnTimeoutMs = 10
    coordinator.handleInitProducerId(transactionalId, newTxnTimeoutMs, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(pid, (epoch + 1).toShort, Errors.NONE), result)
    assertEquals(newTxnTimeoutMs, metadata.txnTimeoutMs)
    assertEquals(time.milliseconds(), metadata.txnLastUpdateTimestamp)
    assertEquals((epoch + 1).toShort, metadata.producerEpoch)
    assertEquals(pid, metadata.producerId)
  }

  private def mockPrepare(transactionState: TransactionState, runCallback: Boolean = false): TransactionMetadata = {
    val now = time.milliseconds()
    val originalMetadata = new TransactionMetadata(pid, epoch, txnTimeoutMs, Ongoing, partitions, now, now)

    EasyMock.expect(transactionManager.isCoordinatorFor(transactionalId))
      .andReturn(true)
      .anyTimes()
    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, originalMetadata)))
      .once()
    EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(originalMetadata.copy().prepareAbortOrCommit(transactionState, now)),
      EasyMock.capture(capturedErrorsCallback)))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          if (runCallback)
            capturedErrorsCallback.getValue.apply(Errors.NONE)
        }
      }).once()

    new TransactionMetadata(pid, epoch, txnTimeoutMs, transactionState, partitions, time.milliseconds(), time.milliseconds())
  }

  private def mockComplete(transactionState: TransactionState, appendError: Errors = Errors.NONE): TransactionMetadata = {
    val now = time.milliseconds()
    val prepareMetadata = mockPrepare(transactionState, true)

    val (finalState, txnResult) = if (transactionState == PrepareAbort)
      (CompleteAbort, TransactionResult.ABORT)
    else
      (CompleteCommit, TransactionResult.COMMIT)

    val completedMetadata = new TransactionMetadata(pid, epoch, txnTimeoutMs, finalState,
      collection.mutable.Set.empty[TopicPartition],
      prepareMetadata.txnStartTimestamp,
      prepareMetadata.txnLastUpdateTimestamp)

    EasyMock.expect(transactionManager.getTransactionState(transactionalId))
      .andReturn(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, prepareMetadata)))
      .once()

    val newMetadata = prepareMetadata.copy().prepareComplete(now)
    EasyMock.expect(transactionMarkerChannelManager.addTxnMarkersToSend(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(txnResult),
      EasyMock.eq(prepareMetadata),
      EasyMock.eq(newMetadata))
    ).once()

    val firstAnswer = EasyMock.expect(transactionManager.appendTransactionToLog(
      EasyMock.eq(transactionalId),
      EasyMock.eq(coordinatorEpoch),
      EasyMock.eq(newMetadata),
      EasyMock.capture(capturedErrorsCallback)))
      .andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          capturedErrorsCallback.getValue.apply(appendError)
        }
      })

    // let it succeed next time
    if (appendError != Errors.NONE && appendError != Errors.NOT_COORDINATOR) {
      firstAnswer.andAnswer(new IAnswer[Unit] {
        override def answer(): Unit = {
          capturedErrorsCallback.getValue.apply(Errors.NONE)
        }
      })
    }

    completedMetadata
  }

  def initProducerIdMockCallback(ret: InitProducerIdResult): Unit = {
    result = ret
  }

  def errorsCallback(ret: Errors): Unit = {
    error = ret
  }
}
