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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{AddPartitionsToTxnResponse, TransactionResult}
import org.apache.kafka.common.utils.{LogContext, MockTime, ProducerIdAndEpoch}
import org.apache.kafka.server.util.MockScheduler
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{mock, times, verify, when}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Success

class TransactionCoordinatorTest {

  val time = new MockTime()

  var nextPid: Long = 0L
  val pidGenerator: ProducerIdManager = mock(classOf[ProducerIdManager])
  val transactionManager: TransactionStateManager = mock(classOf[TransactionStateManager])
  val transactionMarkerChannelManager: TransactionMarkerChannelManager = mock(classOf[TransactionMarkerChannelManager])
  val capturedTxn: ArgumentCaptor[TransactionMetadata] = ArgumentCaptor.forClass(classOf[TransactionMetadata])
  val capturedErrorsCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])
  val capturedTxnTransitMetadata: ArgumentCaptor[TxnTransitMetadata] = ArgumentCaptor.forClass(classOf[TxnTransitMetadata])
  val brokerId = 0
  val coordinatorEpoch = 0
  private val transactionalId = "known"
  private val producerId = 10L
  private val producerEpoch: Short = 1
  private val txnTimeoutMs = 1

  private val partitions = mutable.Set[TopicPartition](new TopicPartition("topic1", 0))
  private val scheduler = new MockScheduler(time)

  val coordinator = new TransactionCoordinator(
    TransactionConfig(),
    scheduler,
    () => pidGenerator,
    transactionManager,
    transactionMarkerChannelManager,
    time,
    new LogContext)
  val transactionStatePartitionCount = 1
  var result: InitProducerIdResult = _
  var error: Errors = Errors.NONE

  private def mockPidGenerator(): Unit = {
    when(pidGenerator.generateProducerId()).thenAnswer(_ => {
      nextPid += 1
      Success(nextPid - 1)
    })
  }

  private def initPidGenericMocks(transactionalId: String): Unit = {
    mockPidGenerator()
    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)
  }

  @Test
  def shouldReturnInvalidRequestWhenTransactionalIdIsEmpty(): Unit = {
    mockPidGenerator()

    coordinator.handleInitProducerId("", txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1L, -1, Errors.INVALID_REQUEST), result)
    coordinator.handleInitProducerId("", txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1L, -1, Errors.INVALID_REQUEST), result)
  }

  @Test
  def shouldAcceptInitPidAndReturnNextPidWhenTransactionalIdIsNull(): Unit = {
    mockPidGenerator()

    coordinator.handleInitProducerId(null, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(0L, 0, Errors.NONE), result)
    coordinator.handleInitProducerId(null, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(1L, 0, Errors.NONE), result)
  }

  @Test
  def shouldInitPidWithEpochZeroForNewTransactionalId(): Unit = {
    initPidGenericMocks(transactionalId)

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(None))

    when(transactionManager.putTransactionStateIfNotExists(capturedTxn.capture()))
      .thenAnswer(_ => {
        Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, capturedTxn.getValue))
      })

    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any[TxnTransitMetadata],
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.NONE))

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(nextPid - 1, 0, Errors.NONE), result)
  }

  @Test
  def shouldGenerateNewProducerIdIfNoStateAndProducerIdAndEpochProvided(): Unit = {
    initPidGenericMocks(transactionalId)

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(None))

    when(transactionManager.putTransactionStateIfNotExists(capturedTxn.capture()))
      .thenAnswer(_ => {
        Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, capturedTxn.getValue))
      })

    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any[TxnTransitMetadata],
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.NONE))

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId, producerEpoch)),
      initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(nextPid - 1, 0, Errors.NONE), result)
  }

  @Test
  def shouldGenerateNewProducerIdIfEpochsExhausted(): Unit = {
    initPidGenericMocks(transactionalId)

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (Short.MaxValue - 1).toShort,
      (Short.MaxValue - 2).toShort, txnTimeoutMs, Empty, mutable.Set.empty, time.milliseconds(), time.milliseconds())

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any[TxnTransitMetadata],
      capturedErrorsCallback.capture(),
      any(),
      any()
    )).thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.NONE))

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
    assertNotEquals(producerId, result.producerId)
    assertEquals(0, result.producerEpoch)
    assertEquals(Errors.NONE, result.error)
  }

  @Test
  def shouldRespondWithNotCoordinatorOnInitPidWhenNotCoordinator(): Unit = {
    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.NOT_COORDINATOR))

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1, -1, Errors.NOT_COORDINATOR), result)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnInitPidWhenCoordinatorLoading(): Unit = {
    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1, -1, Errors.COORDINATOR_LOAD_IN_PROGRESS), result)
  }

  @Test
  def shouldRespondWithInvalidPidMappingOnAddPartitionsToTransactionWhenTransactionalIdNotPresent(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(None))

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
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.NOT_COORDINATOR))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 1, partitions, errorsCallback)
    assertEquals(Errors.NOT_COORDINATOR, error)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnAddPartitionsWhenCoordinatorLoading(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 1, partitions, errorsCallback)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, error)
  }
 
  @Test 
  def testVerifyPartitionHandling(): Unit = {
    var errors: Map[TopicPartition, Errors] = Map.empty

    def verifyPartitionsInTxnCallback(result: AddPartitionsToTxnResult): Unit = {
      errors = AddPartitionsToTxnResponse.errorsForTransaction(result.topicResults()).asScala.toMap
    }
    // If producer ID is not the same, return INVALID_PRODUCER_ID_MAPPING
    val wrongPidTxnMetadata = new TransactionMetadata(transactionalId, 1, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, PrepareCommit, partitions, 0, 0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, wrongPidTxnMetadata))))

    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) =>
      assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    }
    

    // If producer epoch is not equal, return PRODUCER_FENCED
    val oldEpochTxnMetadata = new TransactionMetadata(transactionalId, 0, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, PrepareCommit, partitions, 0, 0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, oldEpochTxnMetadata))))

    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 1, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) =>
      assertEquals(Errors.PRODUCER_FENCED, error)
    }
    
    // If the txn state is Prepare or AbortCommit, we return CONCURRENT_TRANSACTIONS
    val emptyTxnMetadata = new TransactionMetadata(transactionalId, 0, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, PrepareCommit, partitions, 0, 0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, emptyTxnMetadata))))
    
    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) => 
      assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    }

    // Pending state does not matter, we will just check if the partitions are in the txnMetadata.
    val ongoingTxnMetadata = new TransactionMetadata(transactionalId, 0, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, Ongoing, mutable.Set.empty, 0, 0)
    ongoingTxnMetadata.pendingState = Some(CompleteCommit)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, ongoingTxnMetadata))))

    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) =>
      assertEquals(Errors.INVALID_TXN_STATE, error)
    }
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
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, state, mutable.Set.empty, 0, 0)))))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
  }

  @Test
  def shouldRespondWithProducerFencedOnAddPartitionsWhenEpochsAreDifferent(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, 10, 9, 0, PrepareCommit, mutable.Set.empty, 0, 0)))))

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

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    coordinator.handleAddPartitionsToTransaction(transactionalId, producerId, producerEpoch, partitions, errorsCallback)

    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any[TxnTransitMetadata],
      any(),
      any(),
      any()
    )
  }

  @Test
  def shouldRespondWithErrorsNoneOnAddPartitionWhenNoErrorsAndPartitionsTheSame(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, Empty, partitions, 0, 0)))))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.NONE, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldRespondWithErrorsNoneOnAddPartitionWhenOngoingVerifyOnlyAndPartitionsTheSame(): Unit = {
    var errors: Map[TopicPartition, Errors] = Map.empty
    def verifyPartitionsInTxnCallback(result: AddPartitionsToTxnResult): Unit = {
      errors = AddPartitionsToTxnResponse.errorsForTransaction(result.topicResults()).asScala.toMap
    }
    
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, Ongoing, partitions, 0, 0)))))

    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) =>
      assertEquals(Errors.NONE, error)
    }
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }
  
  @Test
  def shouldRespondWithInvalidTxnStateWhenVerifyOnlyAndPartitionNotPresent(): Unit = {
    var errors: Map[TopicPartition, Errors] = Map.empty
    def verifyPartitionsInTxnCallback(result: AddPartitionsToTxnResult): Unit = {
      errors = AddPartitionsToTxnResponse.errorsForTransaction(result.topicResults()).asScala.toMap
    }
    
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, Empty, partitions, 0, 0)))))
    
    val extraPartitions = partitions ++ Set(new TopicPartition("topic2", 0))
    
    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, extraPartitions, verifyPartitionsInTxnCallback)
    assertEquals(Errors.INVALID_TXN_STATE, errors(new TopicPartition("topic2", 0)))
    assertEquals(Errors.NONE, errors(new TopicPartition("topic1", 0)))
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenTxnIdDoesntExist(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(None))

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenPidDosentMatchMapped(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 10, 10, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReplyWithProducerFencedOnEndTxnWhenEpochIsNotSameAsTransaction(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort, 1, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))

    coordinator.handleEndTransaction(transactionalId, producerId, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteCommitAndResultIsCommit(): Unit ={
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.NONE, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteAbortAndResultIsAbort(): Unit ={
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.ABORT, errorsCallback)
    assertEquals(Errors.NONE, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteAbortAndResultIsNotAbort(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteCommitAndResultIsNotCommit(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort,1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.ABORT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReturnConcurrentTxnRequestOnEndTxnRequestWhenStatusIsPrepareCommit(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, (producerEpoch - 1).toShort, 1, PrepareCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsPrepareAbort(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId, 1, RecordBatch.NO_PRODUCER_EPOCH, 1, PrepareAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))

    coordinator.handleEndTransaction(transactionalId, producerId, 1, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldAppendPrepareCommitToLogOnEndTxnWhenStatusIsOngoingAndResultIsCommit(): Unit = {
    mockPrepare(PrepareCommit)

    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.COMMIT, errorsCallback)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any(),
      any(),
      any(),
      any())
  }

  @Test
  def shouldAppendPrepareAbortToLogOnEndTxnWhenStatusIsOngoingAndResultIsAbort(): Unit = {
    mockPrepare(PrepareAbort)

    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.ABORT, errorsCallback)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any(),
      any(),
      any(),
      any())
  }

  @Test
  def shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsNull(): Unit = {
    coordinator.handleEndTransaction(null, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsEmpty(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.NOT_COORDINATOR))

    coordinator.handleEndTransaction("", 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithNotCoordinatorOnEndTxnWhenIsNotCoordinatorForId(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.NOT_COORDINATOR))

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.NOT_COORDINATOR, error)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnEndTxnWhenCoordinatorIsLoading(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))

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
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, producerId, producerId, metadataEpoch, 0, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds())))))

    coordinator.handleEndTransaction(transactionalId, producerId, requestEpoch, TransactionResult.COMMIT, errorsCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
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

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (producerEpoch + 1).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any[TxnTransitMetadata],
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.NONE))

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
    verify(transactionManager).validateTransactionTimeoutMs(anyInt())
    verify(transactionManager, times(3)).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(originalMetadata.prepareAbortOrCommit(PrepareAbort, time.milliseconds())),
      any(),
      any(),
      any())
  }

  @Test
  def shouldFailToAbortTransactionOnHandleInitPidWhenProducerEpochIsSmaller(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      (producerEpoch - 1).toShort, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)

    val bumpedTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (producerEpoch + 2).toShort,
      (producerEpoch - 1).toShort, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, bumpedTxnMetadata))))

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.PRODUCER_FENCED), result)

    verify(transactionManager).validateTransactionTimeoutMs(anyInt())
    verify(transactionManager, times(2)).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldNotRepeatedlyBumpEpochDueToInitPidDuringOngoingTxnIfAppendToLogFails(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)

    when(transactionManager.putTransactionStateIfNotExists(any[TransactionMetadata]()))
      .thenReturn(Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenAnswer(_ => Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (producerEpoch + 1).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    val txnTransitMetadata = originalMetadata.prepareAbortOrCommit(PrepareAbort, time.milliseconds())
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitMetadata),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => {
      capturedErrorsCallback.getValue.apply(Errors.NOT_ENOUGH_REPLICAS)
      txnMetadata.pendingState = None
    }).thenAnswer(_ => {
      capturedErrorsCallback.getValue.apply(Errors.NOT_ENOUGH_REPLICAS)
      txnMetadata.pendingState = None
    }).thenAnswer(_ => {
      capturedErrorsCallback.getValue.apply(Errors.NONE)

      // For the successful call, execute the state transitions that would happen in appendTransactionToLog()
      txnMetadata.completeTransitionTo(txnTransitMetadata)
      txnMetadata.prepareComplete(time.milliseconds())
    })

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

    verify(transactionManager, times(3)).validateTransactionTimeoutMs(anyInt())
    verify(transactionManager, times(9)).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager, times(3)).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitMetadata),
      capturedErrorsCallback.capture(),
      any(),
      any())
  }

  @Test
  def shouldUseLastEpochToFenceWhenEpochsAreExhausted(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (Short.MaxValue - 1).toShort,
      (Short.MaxValue - 2).toShort, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    assertTrue(txnMetadata.isProducerEpochExhausted)

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)

    val postFenceTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, Short.MaxValue,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, PrepareAbort, partitions, time.milliseconds(), time.milliseconds())
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, postFenceTxnMetadata))))

    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(TxnTransitMetadata(
        producerId = producerId,
        lastProducerId = producerId,
        producerEpoch = Short.MaxValue,
        lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
        txnTimeoutMs = txnTimeoutMs,
        txnState = PrepareAbort,
        topicPartitions = partitions.toSet,
        txnStartTimestamp = time.milliseconds(),
        txnLastUpdateTimestamp = time.milliseconds())),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.NONE))

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)
    assertEquals(Short.MaxValue, txnMetadata.producerEpoch)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
    verify(transactionManager).validateTransactionTimeoutMs(anyInt())
    verify(transactionManager, times(3)).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(TxnTransitMetadata(
        producerId = producerId,
        lastProducerId = producerId,
        producerEpoch = Short.MaxValue,
        lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
        txnTimeoutMs = txnTimeoutMs,
        txnState = PrepareAbort,
        topicPartitions = partitions.toSet,
        txnStartTimestamp = time.milliseconds(),
        txnLastUpdateTimestamp = time.milliseconds())),
      any(),
      any(),
      any())
  }

  @Test
  def testInitProducerIdWithNoLastProducerData(): Unit = {
    // If the metadata doesn't include the previous producer data (for example, if it was written to the log by a broker
    // on an old version), the retry case should fail
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, RecordBatch.NO_PRODUCER_ID, (producerEpoch + 1).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds)

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

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

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    // Simulate producer trying to continue after new producer has already been initialized
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId, producerEpoch)),
      initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Errors.PRODUCER_FENCED), result)
  }

  @Test
  def testInitProducerIdWithCurrentEpochProvided(): Unit = {
    mockPidGenerator()

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, 10,
      9, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds)

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any[TxnTransitMetadata],
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => {
      capturedErrorsCallback.getValue.apply(Errors.NONE)
      txnMetadata.pendingState = None
    })

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
    mockPidGenerator()

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, 10,
      9, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds)

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val capturedTxnTransitMetadata : ArgumentCaptor[TxnTransitMetadata] = ArgumentCaptor.forClass(classOf[TxnTransitMetadata])
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      capturedTxnTransitMetadata.capture(),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => {
      capturedErrorsCallback.getValue.apply(Errors.NONE)
      txnMetadata.pendingState = None
      txnMetadata.producerEpoch = capturedTxnTransitMetadata.getValue.producerEpoch
      txnMetadata.lastProducerEpoch = capturedTxnTransitMetadata.getValue.lastProducerEpoch
    })

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

    when(pidGenerator.generateProducerId())
      .thenReturn(Success(producerId + 1))

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      capturedTxnTransitMetadata.capture(),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => {
      capturedErrorsCallback.getValue.apply(Errors.NONE)
      txnMetadata.pendingState = None
      txnMetadata.producerId = capturedTxnTransitMetadata.getValue.producerId
      txnMetadata.lastProducerId = capturedTxnTransitMetadata.getValue.lastProducerId
      txnMetadata.producerEpoch = capturedTxnTransitMetadata.getValue.producerEpoch
      txnMetadata.lastProducerEpoch = capturedTxnTransitMetadata.getValue.lastProducerEpoch
    })

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

    when(pidGenerator.generateProducerId())
      .thenReturn(Success(producerId + 1))

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      capturedTxnTransitMetadata.capture(),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => {
      capturedErrorsCallback.getValue.apply(Errors.NONE)
      txnMetadata.pendingState = None
      txnMetadata.producerId = capturedTxnTransitMetadata.getValue.producerId
      txnMetadata.lastProducerId = capturedTxnTransitMetadata.getValue.lastProducerId
      txnMetadata.producerEpoch = capturedTxnTransitMetadata.getValue.producerEpoch
      txnMetadata.lastProducerEpoch = capturedTxnTransitMetadata.getValue.lastProducerEpoch
    })

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
    coordinator.onResignation(0, Some(coordinatorEpoch))
    verify(transactionManager).removeTransactionsForTxnTopicPartition(0, coordinatorEpoch)
    verify(transactionMarkerChannelManager).removeMarkersForTxnTopicPartition(0)
  }

  @Test
  def shouldAbortExpiredTransactionsInOngoingStateAndBumpEpoch(): Unit = {
    val now = time.milliseconds()
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now)

    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val expectedTransition = TxnTransitMetadata(producerId, producerId, (producerEpoch + 1).toShort, RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs, PrepareAbort, partitions.toSet, now, now + TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)

    when(transactionManager.appendTransactionToLog(ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(expectedTransition),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => {})

    coordinator.startup(() => transactionStatePartitionCount, false)
    time.sleep(TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)
    scheduler.tick()
    verify(transactionManager).timedOutTransactions()
    verify(transactionManager, times(2)).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(expectedTransition),
      capturedErrorsCallback.capture(),
      any(),
      any())
  }

  @Test
  def shouldNotAcceptSmallerEpochDuringTransactionExpiration(): Unit = {
    val now = time.milliseconds()
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now)

    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val bumpedTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, (producerEpoch + 2).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, bumpedTxnMetadata))))

    def checkOnEndTransactionComplete(txnIdAndPidEpoch: TransactionalIdAndProducerIdEpoch)(error: Errors): Unit = {
      assertEquals(Errors.PRODUCER_FENCED, error)
    }
    coordinator.abortTimedOutTransactions(checkOnEndTransactionComplete)

    verify(transactionManager).timedOutTransactions()
    verify(transactionManager, times(2)).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldNotAbortExpiredTransactionsThatHaveAPendingStateTransition(): Unit = {
    val metadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    metadata.prepareAbortOrCommit(PrepareCommit, time.milliseconds())

    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata))))

    coordinator.startup(() => transactionStatePartitionCount, false)
    time.sleep(TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)
    scheduler.tick()
    verify(transactionManager).timedOutTransactions()
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldNotBumpEpochWhenAbortingExpiredTransactionIfAppendToLogFails(): Unit = {
    val now = time.milliseconds()
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now)

    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))

    val txnMetadataAfterAppendFailure = new TransactionMetadata(transactionalId, producerId, producerId, (producerEpoch + 1).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadataAfterAppendFailure))))

    val bumpedEpoch = (producerEpoch + 1).toShort
    val expectedTransition = TxnTransitMetadata(producerId, producerId, bumpedEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs,
      PrepareAbort, partitions.toSet, now, now + TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)

    when(transactionManager.appendTransactionToLog(ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(expectedTransition),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.NOT_ENOUGH_REPLICAS))

    coordinator.startup(() => transactionStatePartitionCount, false)
    time.sleep(TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs)
    scheduler.tick()

    verify(transactionManager).timedOutTransactions()
    verify(transactionManager, times(3)).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(expectedTransition),
      capturedErrorsCallback.capture(),
      any(),
      any())

    assertEquals((producerEpoch + 1).toShort, txnMetadataAfterAppendFailure.producerEpoch)
    assertTrue(txnMetadataAfterAppendFailure.hasFailedEpochFence)
  }

  @Test
  def shouldNotBumpEpochWithPendingTransaction(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())
    txnMetadata.prepareAbortOrCommit(PrepareCommit, time.milliseconds())

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Some(new ProducerIdAndEpoch(producerId, 10)),
      initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Errors.CONCURRENT_TRANSACTIONS), result)

    verify(transactionManager).validateTransactionTimeoutMs(anyInt())
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def testDescribeTransactionsWithEmptyTransactionalId(): Unit = {
    coordinator.startup(() => transactionStatePartitionCount, enableTransactionalIdExpiration = false)
    val result = coordinator.handleDescribeTransactions("")
    assertEquals("", result.transactionalId)
    assertEquals(Errors.INVALID_REQUEST, Errors.forCode(result.errorCode))
  }

  @Test
  def testDescribeTransactionsWithExpiringTransactionalId(): Unit = {
    coordinator.startup(() => transactionStatePartitionCount, enableTransactionalIdExpiration = false)

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Dead, mutable.Set.empty, time.milliseconds(),
      time.milliseconds())
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val result = coordinator.handleDescribeTransactions(transactionalId)
    assertEquals(transactionalId, result.transactionalId)
    assertEquals(Errors.TRANSACTIONAL_ID_NOT_FOUND, Errors.forCode(result.errorCode))
  }

  @Test
  def testDescribeTransactionsWhileCoordinatorLoading(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))

    coordinator.startup(() => transactionStatePartitionCount, enableTransactionalIdExpiration = false)
    val result = coordinator.handleDescribeTransactions(transactionalId)
    assertEquals(transactionalId, result.transactionalId)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, Errors.forCode(result.errorCode))

    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def testDescribeTransactions(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds())

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    coordinator.startup(() => transactionStatePartitionCount, enableTransactionalIdExpiration = false)
    val result = coordinator.handleDescribeTransactions(transactionalId)
    assertEquals(Errors.NONE, Errors.forCode(result.errorCode))
    assertEquals(transactionalId, result.transactionalId)
    assertEquals(producerId, result.producerId)
    assertEquals(producerEpoch, result.producerEpoch)
    assertEquals(txnTimeoutMs, result.transactionTimeoutMs)
    assertEquals(time.milliseconds(), result.transactionStartTimeMs)

    val addedPartitions = result.topics.asScala.flatMap { topicData =>
      topicData.partitions.asScala.map(partition => new TopicPartition(topicData.topic, partition))
    }.toSet
    assertEquals(partitions, addedPartitions)

    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  private def validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(state: TransactionState): Unit = {
    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)

    val metadata = new TransactionMetadata(transactionalId, 0, 0, 0, RecordBatch.NO_PRODUCER_EPOCH, 0, state, mutable.Set[TopicPartition](new TopicPartition("topic", 1)), 0, 0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata))))

    coordinator.handleInitProducerId(transactionalId, 10, None, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
  }

  private def validateIncrementEpochAndUpdateMetadata(state: TransactionState): Unit = {
    when(pidGenerator.generateProducerId())
      .thenReturn(Success(producerId))

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)

    val metadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, state, mutable.Set.empty[TopicPartition], time.milliseconds(), time.milliseconds())
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata))))

    val capturedNewMetadata: ArgumentCaptor[TxnTransitMetadata] = ArgumentCaptor.forClass(classOf[TxnTransitMetadata])
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      capturedNewMetadata.capture(),
      capturedErrorsCallback.capture(),
      any(),
      any()
    )).thenAnswer(_ => {
      metadata.completeTransitionTo(capturedNewMetadata.getValue)
      capturedErrorsCallback.getValue.apply(Errors.NONE)
    })

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

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, originalMetadata))))
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(transition),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => {
      if (runCallback)
        capturedErrorsCallback.getValue.apply(Errors.NONE)
    })

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
