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
import org.apache.kafka.coordinator.transaction.{ProducerIdManager, TransactionStateManagerConfig}
import org.apache.kafka.server.common.TransactionVersion
import org.apache.kafka.server.common.TransactionVersion.{TV_0, TV_2}
import org.apache.kafka.server.util.MockScheduler
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

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
  private val producerId2 = 11L

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
  var newProducerId: Long = RecordBatch.NO_PRODUCER_ID
  var newEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH

  private def mockPidGenerator(): Unit = {
    when(pidGenerator.generateProducerId()).thenAnswer(_ => {
      nextPid += 1
      nextPid - 1
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

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID, (Short.MaxValue - 1).toShort,
      (Short.MaxValue - 2).toShort, txnTimeoutMs, Empty, mutable.Set.empty, time.milliseconds(), time.milliseconds(), TV_0)

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
    val wrongPidTxnMetadata = new TransactionMetadata(transactionalId, 1, 0, RecordBatch.NO_PRODUCER_ID,
      0, RecordBatch.NO_PRODUCER_EPOCH, 0, PrepareCommit, partitions, 0, 0, TV_0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, wrongPidTxnMetadata))))

    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) =>
      assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    }

    // If producer epoch is not equal, return PRODUCER_FENCED
    val oldEpochTxnMetadata = new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
      0, RecordBatch.NO_PRODUCER_EPOCH, 0, PrepareCommit, partitions, 0, 0, TV_0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, oldEpochTxnMetadata))))

    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 1, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) =>
      assertEquals(Errors.PRODUCER_FENCED, error)
    }
    
    // If the txn state is Prepare or AbortCommit, we return CONCURRENT_TRANSACTIONS
    val emptyTxnMetadata = new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
      0, RecordBatch.NO_PRODUCER_EPOCH, 0, PrepareCommit, partitions, 0, 0, TV_0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, emptyTxnMetadata))))
    
    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) => 
      assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    }

    // Pending state does not matter, we will just check if the partitions are in the txnMetadata.
    val ongoingTxnMetadata = new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
      0, RecordBatch.NO_PRODUCER_EPOCH, 0, Ongoing, mutable.Set.empty, 0, 0, TV_0)
    ongoingTxnMetadata.pendingState = Some(CompleteCommit)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, ongoingTxnMetadata))))

    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) =>
      assertEquals(Errors.TRANSACTION_ABORTABLE, error)
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
    // Since the clientTransactionVersion doesn't matter, use 2 since the states are PrepareCommit and PrepareAbort.
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
          0, RecordBatch.NO_PRODUCER_EPOCH, 0, state, mutable.Set.empty, 0, 0, TV_2)))))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
  }

  @Test
  def shouldRespondWithProducerFencedOnAddPartitionsWhenEpochsAreDifferent(): Unit = {
    // Since the clientTransactionVersion doesn't matter, use 2 since the state is PrepareCommit.
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
          10, 9, 0, PrepareCommit, mutable.Set.empty, 0, 0, TV_2)))))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)
  }

  @Test
  def shouldAppendNewMetadataToLogOnAddPartitionsWhenPartitionsAdded(): Unit = {
    validateSuccessfulAddPartitions(Empty, 0)
  }

  @Test
  def shouldRespondWithSuccessOnAddPartitionsWhenStateIsOngoing(): Unit = {
    validateSuccessfulAddPartitions(Ongoing, 0)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldRespondWithSuccessOnAddPartitionsWhenStateIsCompleteCommit(clientTransactionVersion: Short): Unit = {
    validateSuccessfulAddPartitions(CompleteCommit, clientTransactionVersion)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldRespondWithSuccessOnAddPartitionsWhenStateIsCompleteAbort(clientTransactionVersion: Short): Unit = {
    validateSuccessfulAddPartitions(CompleteAbort, clientTransactionVersion)
  }

  def validateSuccessfulAddPartitions(previousState: TransactionState, transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort, txnTimeoutMs, previousState, mutable.Set.empty, time.milliseconds(), time.milliseconds(), clientTransactionVersion)

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
        new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
          0, RecordBatch.NO_PRODUCER_EPOCH, 0, Empty, partitions, 0, 0, TV_0)))))

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
        new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
          0, RecordBatch.NO_PRODUCER_EPOCH, 0, Ongoing, partitions, 0, 0, TV_0)))))

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
        new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
          0, RecordBatch.NO_PRODUCER_EPOCH, 0, Empty, partitions, 0, 0, TV_0)))))
    
    val extraPartitions = partitions ++ Set(new TopicPartition("topic2", 0))
    
    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, extraPartitions, verifyPartitionsInTxnCallback)
    assertEquals(Errors.TRANSACTION_ABORTABLE, errors(new TopicPartition("topic2", 0)))
    assertEquals(Errors.NONE, errors(new TopicPartition("topic1", 0)))
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenTxnIdDoesntExist(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(None))

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenPidDosentMatchMapped(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 10, 10, RecordBatch.NO_PRODUCER_ID,
          0, RecordBatch.NO_PRODUCER_EPOCH, 0, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), TV_0)))))

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldReplyWithProducerFencedOnEndTxnWhenEpochIsNotSameAsTransaction(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID, producerEpoch,
          (producerEpoch - 1).toShort, 1, Ongoing, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), TV_0)))))

    coordinator.handleEndTransaction(transactionalId, producerId, 0, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testEndTxnWhenStatusIsCompleteCommitAndResultIsCommitInV1(isRetry: Boolean): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID, producerEpoch,
          (producerEpoch - 1).toShort, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)))))

    val epoch = if (isRetry) producerEpoch - 1 else producerEpoch
    coordinator.handleEndTransaction(transactionalId, producerId, epoch.toShort, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    if (isRetry) {
      assertEquals(Errors.PRODUCER_FENCED, error)
    } else {
      assertEquals(Errors.NONE, error)
      verify(transactionManager, never()).appendTransactionToLog(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )
    }
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testEndTxnWhenStatusIsCompleteCommitAndResultIsCommitInV2(isRetry: Boolean): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(2)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID, producerEpoch,
          (producerEpoch - 1).toShort, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)))))

    val epoch = if (isRetry) producerEpoch - 1 else producerEpoch
    coordinator.handleEndTransaction(transactionalId, producerId, epoch.toShort, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    if (isRetry) {
      assertEquals(Errors.NONE, error)
    } else {
      assertEquals(Errors.INVALID_TXN_STATE, error)
    }
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testEndTxnWhenStatusIsCompleteAbortAndResultIsAbortInV1(isRetry: Boolean): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(0)
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val nextProducerEpoch = if (isRetry) producerEpoch - 1 else producerEpoch
    coordinator.handleEndTransaction(transactionalId, producerId, nextProducerEpoch.toShort , TransactionResult.ABORT, clientTransactionVersion, endTxnCallback)
    if (isRetry) {
      assertEquals(Errors.PRODUCER_FENCED, error)
    } else {
      assertEquals(Errors.NONE, error)
    }
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def shouldReturnOkOnEndTxnWhenStatusIsCompleteAbortAndResultIsAbortInV2(isRetry: Boolean): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(2)
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val nextProducerEpoch = if (isRetry) producerEpoch - 1 else producerEpoch
    coordinator.handleEndTransaction(transactionalId, producerId, nextProducerEpoch.toShort , TransactionResult.ABORT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.NONE, error)
    if (isRetry) {
      verify(transactionManager, never()).appendTransactionToLog(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )
    } else {
      val newMetadata = ArgumentCaptor.forClass(classOf[TxnTransitMetadata]);
        verify(transactionManager).appendTransactionToLog(
          ArgumentMatchers.eq(transactionalId),
          ArgumentMatchers.any(),
          newMetadata.capture(),
          ArgumentMatchers.any(),
          ArgumentMatchers.any(),
          ArgumentMatchers.any()
        )
      assertEquals(producerEpoch + 1, newMetadata.getValue.asInstanceOf[TxnTransitMetadata].producerEpoch, newMetadata.getValue.asInstanceOf[TxnTransitMetadata].toString)
      assertEquals(time.milliseconds(), newMetadata.getValue.asInstanceOf[TxnTransitMetadata].txnStartTimestamp, newMetadata.getValue.asInstanceOf[TxnTransitMetadata].toString)
    }
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteAbortAndResultIsNotAbort(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort, 1, CompleteAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    coordinator.handleEndTransaction(transactionalId, producerId, requestEpoch(clientTransactionVersion), TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteCommitAndResultIsNotCommit(): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(0)
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort,1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.ABORT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testEndTxnRequestWhenStatusIsCompleteCommitAndResultIsAbortInV1(isRetry: Boolean): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(0)
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val epoch = if (isRetry) producerEpoch - 1 else producerEpoch
    coordinator.handleEndTransaction(transactionalId, producerId, epoch.toShort, TransactionResult.ABORT, clientTransactionVersion, endTxnCallback)
    if (isRetry) {
      assertEquals(Errors.PRODUCER_FENCED, error)
    } else {
      assertEquals(Errors.INVALID_TXN_STATE, error)
    }
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testEndTxnRequestWhenStatusIsCompleteCommitAndResultIsAbortInV2(isRetry: Boolean): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(2)
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val epoch = if (isRetry) producerEpoch - 1 else producerEpoch
    coordinator.handleEndTransaction(transactionalId, producerId, epoch.toShort, TransactionResult.ABORT, clientTransactionVersion, endTxnCallback)
    if (isRetry) {
      assertEquals(Errors.INVALID_TXN_STATE, error)
    } else {
      assertEquals(Errors.NONE, error)
      val newMetadata = ArgumentCaptor.forClass(classOf[TxnTransitMetadata]);
      verify(transactionManager).appendTransactionToLog(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.any(),
        newMetadata.capture(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )
      assertEquals(producerEpoch + 1, newMetadata.getValue.asInstanceOf[TxnTransitMetadata].producerEpoch, newMetadata.getValue.asInstanceOf[TxnTransitMetadata].toString)
      assertEquals(time.milliseconds(), newMetadata.getValue.asInstanceOf[TxnTransitMetadata].txnStartTimestamp, newMetadata.getValue.asInstanceOf[TxnTransitMetadata].toString)
    }
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldReturnConcurrentTransactionsOnEndTxnRequestWhenStatusIsPrepareCommit(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId,
        RecordBatch.NO_PRODUCER_ID, producerEpoch, (producerEpoch - 1).toShort, 1, PrepareCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)))))

    coordinator.handleEndTransaction(transactionalId, producerId, requestEpoch(clientTransactionVersion), TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsPrepareAbort(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId,
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, 1, PrepareAbort, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)))))

    coordinator.handleEndTransaction(transactionalId, producerId, requestEpoch(clientTransactionVersion), TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def TestEndTxnRequestWhenEmptyTransactionStateForAbortInV1(): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId,
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, 1, Empty, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)))))

    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.ABORT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def TestEndTxnRequestWhenEmptyTransactionStateForAbortInV2(isRetry: Boolean): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(2)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId,
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, 1, Empty, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)))))

    val epoch = if (isRetry) producerEpoch - 1 else producerEpoch
    coordinator.handleEndTransaction(transactionalId, producerId, epoch.toShort, TransactionResult.ABORT, clientTransactionVersion, endTxnCallback)
    if (isRetry) {
      assertEquals(Errors.PRODUCER_FENCED, error)
    } else {
      assertEquals(Errors.NONE, error)
      val newMetadata = ArgumentCaptor.forClass(classOf[TxnTransitMetadata]);
      verify(transactionManager).appendTransactionToLog(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.any(),
        newMetadata.capture(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )
      assertEquals(producerEpoch + 1, newMetadata.getValue.asInstanceOf[TxnTransitMetadata].producerEpoch, newMetadata.getValue.asInstanceOf[TxnTransitMetadata].toString)
      assertEquals(time.milliseconds(), newMetadata.getValue.asInstanceOf[TxnTransitMetadata].txnStartTimestamp, newMetadata.getValue.asInstanceOf[TxnTransitMetadata].toString)
    }
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def TestEndTxnRequestWhenEmptyTransactionStateForCommitInV2(isRetry: Boolean): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(2)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId,
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, 1, Empty, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)))))

    val epoch = if (isRetry) producerEpoch - 1 else producerEpoch
    coordinator.handleEndTransaction(transactionalId, producerId, epoch.toShort, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    if (isRetry) {
      assertEquals(Errors.PRODUCER_FENCED, error)
    } else {
      assertEquals(Errors.INVALID_TXN_STATE, error)
    }
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReturnInvalidTxnRequestOnEndTxnV2IfNotEndTxnV2Retry(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId,
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, 1, PrepareCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), TV_2)))))

    // If producerEpoch is the same, this is not a retry of the EndTxnRequest, but the next EndTxnRequest. Return PRODUCER_FENCED.
    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.COMMIT, TV_2, endTxnCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId,
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), TV_2)))))

    // If producerEpoch is the same, this is not a retry of the EndTxnRequest, but the next EndTxnRequest. Return INVALID_TXN_STATE.
    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.COMMIT, TV_2, endTxnCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    verify(transactionManager, times(2)).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReturnOkOnEndTxnV2IfEndTxnV2RetryEpochOverflow(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId,
        producerId2, Short.MaxValue, (Short.MaxValue - 1).toShort, 1, PrepareCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), TV_2)))))

    // Return CONCURRENT_TRANSACTIONS while transaction is still completing
    coordinator.handleEndTransaction(transactionalId, producerId, (Short.MaxValue - 1).toShort, TransactionResult.COMMIT, TV_2, endTxnCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId2, producerId,
        RecordBatch.NO_PRODUCER_ID, 0, RecordBatch.NO_PRODUCER_EPOCH, 1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), TV_2)))))

    coordinator.handleEndTransaction(transactionalId, producerId, (Short.MaxValue - 1).toShort, TransactionResult.COMMIT, TV_2, endTxnCallback)
    assertEquals(Errors.NONE, error)
    assertNotEquals(RecordBatch.NO_PRODUCER_ID, newProducerId)
    assertNotEquals(producerId, newProducerId)
    assertEquals(0, newEpoch)
    verify(transactionManager, times(2)).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldAppendPrepareCommitToLogOnEndTxnWhenStatusIsOngoingAndResultIsCommit(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    mockPrepare(PrepareCommit, clientTransactionVersion)

    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any(),
      any(),
      any(),
      any())
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldAppendPrepareAbortToLogOnEndTxnWhenStatusIsOngoingAndResultIsAbort(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    mockPrepare(PrepareAbort, clientTransactionVersion)

    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.ABORT, clientTransactionVersion, endTxnCallback)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any(),
      any(),
      any(),
      any())
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsNull(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    coordinator.handleEndTransaction(null, 0, 0, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsEmpty(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.NOT_COORDINATOR))

    coordinator.handleEndTransaction("", 0, 0, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldRespondWithNotCoordinatorOnEndTxnWhenIsNotCoordinatorForId(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.NOT_COORDINATOR))

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.NOT_COORDINATOR, error)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldRespondWithCoordinatorLoadInProgressOnEndTxnWhenCoordinatorIsLoading(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))

    coordinator.handleEndTransaction(transactionalId, 0, 0, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, error)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldReturnInvalidEpochOnEndTxnWhenEpochIsLarger(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    val serverProducerEpoch = 1.toShort
    verifyEndTxnEpoch(serverProducerEpoch, (serverProducerEpoch + 1).toShort, clientTransactionVersion)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldReturnInvalidEpochOnEndTxnWhenEpochIsSmaller(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    val serverProducerEpoch = 2.toShort
    // Since we bump epoch in transactionV2 the request should be one producer ID older
    verifyEndTxnEpoch(serverProducerEpoch, requestEpoch(clientTransactionVersion), clientTransactionVersion)
  }

  private def verifyEndTxnEpoch(metadataEpoch: Short, requestEpoch: Short, clientTransactionVersion: TransactionVersion): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID, metadataEpoch, 1,
          1, CompleteCommit, collection.mutable.Set.empty[TopicPartition], 0, time.milliseconds(), clientTransactionVersion)))))

    coordinator.handleEndTransaction(transactionalId, producerId, requestEpoch, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldIncrementEpochAndUpdateMetadataOnHandleInitPidWhenExistingEmptyTransaction(): Unit = {
    validateIncrementEpochAndUpdateMetadata(Empty, 0)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldIncrementEpochAndUpdateMetadataOnHandleInitPidWhenExistingCompleteTransaction(clientTransactionVersion: Short): Unit = {
    validateIncrementEpochAndUpdateMetadata(CompleteAbort, clientTransactionVersion)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldIncrementEpochAndUpdateMetadataOnHandleInitPidWhenExistingCompleteCommitTransaction(clientTransactionVersion: Short): Unit = {
    validateIncrementEpochAndUpdateMetadata(CompleteCommit, clientTransactionVersion)
  }

  @Test
  def shouldWaitForCommitToCompleteOnHandleInitPidAndExistingTransactionInPrepareCommitState(): Unit = {
    validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(PrepareCommit)
  }

  @Test
  def shouldWaitForCommitToCompleteOnHandleInitPidAndExistingTransactionInPrepareAbortState(): Unit = {
    validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(PrepareAbort)
  }

  @Test
  def shouldAbortTransactionOnHandleInitPidWhenExistingTransactionInOngoingState(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds(), TV_0)

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)

    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      (producerEpoch + 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds(), TV_0)
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
      ArgumentMatchers.eq(originalMetadata.prepareAbortOrCommit(PrepareAbort, TV_0, RecordBatch.NO_PRODUCER_ID, time.milliseconds(), false)),
      any(),
      any(),
      any())
  }

  @Test
  def shouldFailToAbortTransactionOnHandleInitPidWhenProducerEpochIsSmaller(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds(), TV_0)

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)

    val bumpedTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      (producerEpoch + 2).toShort, (producerEpoch - 1).toShort, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds(), TV_0)

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, bumpedTxnMetadata))))

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)
    coordinator.handleInitProducerId(transactionalId, txnTimeoutMs, None, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.PRODUCER_FENCED), result)

    verify(transactionManager).validateTransactionTimeoutMs(anyInt())
    verify(transactionManager, times(2)).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldNotRepeatedlyBumpEpochDueToInitPidDuringOngoingTxnIfAppendToLogFails(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds(), TV_0)

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)

    when(transactionManager.putTransactionStateIfNotExists(any[TransactionMetadata]()))
      .thenReturn(Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenAnswer(_ => Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)

    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      (producerEpoch + 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds(), TV_0)
    val txnTransitMetadata = originalMetadata.prepareAbortOrCommit(PrepareAbort, TV_0, RecordBatch.NO_PRODUCER_ID, time.milliseconds(), false)
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
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      (Short.MaxValue - 1).toShort, (Short.MaxValue - 2).toShort, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds(), TV_0)
    assertTrue(txnMetadata.isProducerEpochExhausted)

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)

    val postFenceTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      Short.MaxValue, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, PrepareAbort, partitions, time.milliseconds(), time.milliseconds(), TV_0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, postFenceTxnMetadata))))

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)

    // InitProducerId uses FenceProducerEpoch so clientTransactionVersion is 0.
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(TxnTransitMetadata(
        producerId = producerId,
        prevProducerId = producerId,
        nextProducerId = RecordBatch.NO_PRODUCER_ID,
        producerEpoch = Short.MaxValue,
        lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
        txnTimeoutMs = txnTimeoutMs,
        txnState = PrepareAbort,
        topicPartitions = partitions.toSet,
        txnStartTimestamp = time.milliseconds(),
        txnLastUpdateTimestamp = time.milliseconds(),
        clientTransactionVersion = TV_0)),
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
        prevProducerId = producerId,
        nextProducerId = RecordBatch.NO_PRODUCER_ID,
        producerEpoch = Short.MaxValue,
        lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
        txnTimeoutMs = txnTimeoutMs,
        txnState = PrepareAbort,
        topicPartitions = partitions.toSet,
        txnStartTimestamp = time.milliseconds(),
        txnLastUpdateTimestamp = time.milliseconds(),
        clientTransactionVersion = TV_0)),
      any(),
      any(),
      any())
  }

  @Test
  def testInitProducerIdWithNoLastProducerData(): Unit = {
    // If the metadata doesn't include the previous producer data (for example, if it was written to the log by a broker
    // on an old version), the retry case should fail
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_EPOCH, (producerEpoch + 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds, TV_0)

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
    val txnMetadata = new TransactionMetadata(transactionalId, producerId + 1, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, producerEpoch, (producerEpoch - 1).toShort, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds, TV_0)

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

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, 10, 9, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds, TV_0)

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

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, 10, 9, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds, TV_0)

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
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, (Short.MaxValue - 1).toShort, (Short.MaxValue - 2).toShort, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds, TV_0)

    when(pidGenerator.generateProducerId())
      .thenReturn(producerId + 1)

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
      txnMetadata.previousProducerId = capturedTxnTransitMetadata.getValue.prevProducerId
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
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, (Short.MaxValue - 1).toShort, (Short.MaxValue - 2).toShort, txnTimeoutMs, Empty, partitions, time.milliseconds, time.milliseconds, TV_0)

    when(pidGenerator.generateProducerId())
      .thenReturn(producerId + 1)

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
      txnMetadata.previousProducerId = capturedTxnTransitMetadata.getValue.prevProducerId
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
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now, TV_0)

    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    // Transaction timeouts use FenceProducerEpoch so clientTransactionVersion is 0.
    val expectedTransition = TxnTransitMetadata(producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH, (producerEpoch + 1).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, PrepareAbort, partitions.toSet, now,
      now + TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_DEFAULT, TV_0)

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)

    when(transactionManager.appendTransactionToLog(ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(expectedTransition),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => {})

    coordinator.startup(() => transactionStatePartitionCount, false)
    time.sleep(TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_DEFAULT)
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
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now, TV_0)

    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)

    val bumpedTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, (producerEpoch + 2).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now, TV_0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, bumpedTxnMetadata))))

    def checkOnEndTransactionComplete(txnIdAndPidEpoch: TransactionalIdAndProducerIdEpoch)(error: Errors, producerId: Long, producerEpoch: Short): Unit = {
      assertEquals(Errors.PRODUCER_FENCED, error)
    }
    coordinator.abortTimedOutTransactions(checkOnEndTransactionComplete)

    verify(transactionManager).timedOutTransactions()
    verify(transactionManager, times(2)).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldNotAbortExpiredTransactionsThatHaveAPendingStateTransition(): Unit = {
    val metadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds(), TV_0)
    metadata.prepareAbortOrCommit(PrepareCommit, TV_0, RecordBatch.NO_PRODUCER_ID, time.milliseconds(), false)

    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata))))

    coordinator.startup(() => transactionStatePartitionCount, false)
    time.sleep(TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_DEFAULT)
    scheduler.tick()
    verify(transactionManager).timedOutTransactions()
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldNotBumpEpochWhenAbortingExpiredTransactionIfAppendToLogFails(): Unit = {
    val now = time.milliseconds()
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now, TV_0)

    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))

    val txnMetadataAfterAppendFailure = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, (producerEpoch + 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now, TV_0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadataAfterAppendFailure))))

    // Transaction timeouts use FenceProducerEpoch so clientTransactionVersion is 0.
    val bumpedEpoch = (producerEpoch + 1).toShort
    val expectedTransition = TxnTransitMetadata(producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH, bumpedEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, PrepareAbort, partitions.toSet, now,
      now + TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_DEFAULT, TV_0)

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)

    when(transactionManager.appendTransactionToLog(ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(expectedTransition),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.NOT_ENOUGH_REPLICAS))

    coordinator.startup(() => transactionStatePartitionCount, false)
    time.sleep(TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_DEFAULT)
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
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds(), TV_0)
    txnMetadata.prepareAbortOrCommit(PrepareCommit, TV_0, RecordBatch.NO_PRODUCER_ID, time.milliseconds(), false)

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

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Dead, mutable.Set.empty, time.milliseconds(),
      time.milliseconds(), TV_0)
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
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, time.milliseconds(), time.milliseconds(), TV_0)

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

    // Since the clientTransactionVersion doesn't matter, use 2 since the states are PrepareCommit and PrepareAbort.
    val metadata = new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_EPOCH,
      0, RecordBatch.NO_PRODUCER_EPOCH, 0, state, mutable.Set[TopicPartition](new TopicPartition("topic", 1)), 0, 0, TV_2)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata))))

    coordinator.handleInitProducerId(transactionalId, 10, None, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
  }

  private def validateIncrementEpochAndUpdateMetadata(state: TransactionState, transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    when(pidGenerator.generateProducerId())
      .thenReturn(producerId)

    when(transactionManager.validateTransactionTimeoutMs(anyInt()))
      .thenReturn(true)

    val metadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH,
      producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, state, mutable.Set.empty[TopicPartition], time.milliseconds(), time.milliseconds(), clientTransactionVersion)
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

  private def mockPrepare(transactionState: TransactionState, clientTransactionVersion: TransactionVersion, runCallback: Boolean = false): TransactionMetadata = {
    val now = time.milliseconds()
    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH,
      producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, Ongoing, partitions, now, now, TV_0)

    val transition = TxnTransitMetadata(producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, transactionState, partitions.toSet, now, now, clientTransactionVersion)

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

    new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, transactionState, partitions, time.milliseconds(), time.milliseconds(), clientTransactionVersion)
  }

  def initProducerIdMockCallback(ret: InitProducerIdResult): Unit = {
    result = ret
  }

  def errorsCallback(ret: Errors): Unit = {
    error = ret
  }

  def endTxnCallback(ret: Errors, producerId: Long, epoch: Short): Unit = {
    error = ret
    newProducerId = producerId
    newEpoch = epoch
  }

  def requestEpoch(clientTransactionVersion: TransactionVersion): Short = {
    if (clientTransactionVersion.supportsEpochBump())
      (producerEpoch - 1).toShort
    else
      producerEpoch
  }
}
