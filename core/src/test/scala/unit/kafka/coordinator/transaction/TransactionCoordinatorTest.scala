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
import org.apache.kafka.common.record.internal.RecordBatch
import org.apache.kafka.common.requests.{AddPartitionsToTxnResponse, TransactionResult}
import org.apache.kafka.common.utils.{LogContext, MockTime, ProducerIdAndEpoch}
import org.apache.kafka.coordinator.transaction.{ProducerIdManager, TransactionMetadata, TransactionState, TransactionStateManagerConfig, TxnTransitMetadata}
import org.apache.kafka.server.common.{RequestLocal, TransactionVersion}
import org.apache.kafka.server.common.TransactionVersion.{TV_0, TV_2}
import org.apache.kafka.server.util.MockScheduler
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, ValueSource}
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt}
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers}

import java.util
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

  private val partitions = new util.HashSet[TopicPartition]()
  partitions.add(new TopicPartition("topic1", 0))
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
    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
  }

  /**
   * Sets up an ONGOING 2PC transaction, configures mocks,
   * and calls InitProducerId(keepPreparedTxn=true) to bump client epoch.
   * Returns the transit metadata captor for verifying subsequent operations.
   */
  private def setupPrepared2PcTxnWithBumpedClientEpoch(initialEpoch: Short = producerEpoch, testProducerId: Long = producerId): ArgumentCaptor[TxnTransitMetadata] = {
    // Setup: ONGOING transaction with no next producer epoch set
    // Use Integer.MAX_VALUE for timeout to indicate this is a distributed 2PC transaction
    val txnMetadata = new TransactionMetadata(
      transactionalId,
      testProducerId, testProducerId, RecordBatch.NO_PRODUCER_ID,
      initialEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      Integer.MAX_VALUE /*2PC*/, TransactionState.ONGOING, partitions,
      time.milliseconds(), time.milliseconds(), TV_2
    )
    // Verify this is correctly identified as a distributed 2PC transaction
    assertTrue(txnMetadata.isDistributedTwoPhaseCommitTxn)

    // Add partitions so the transaction has data
    txnMetadata.addPartitions(partitions)

    // Configure mocks
    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
    when(transactionManager.isTransaction2pcEnabled())
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    when(transactionManager.transactionVersionLevel()).thenReturn(TV_2)

    val capturedTransitMetadata: ArgumentCaptor[TxnTransitMetadata] = ArgumentCaptor.forClass(classOf[TxnTransitMetadata])
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      capturedTransitMetadata.capture(),
      capturedErrorsCallback.capture(),
      any(),
      any()
    )).thenAnswer(invocation => {
      val metadata = invocation.getArgument[TxnTransitMetadata](2)
      txnMetadata.completeTransitionTo(metadata)
      capturedErrorsCallback.getValue.apply(Errors.NONE)
    })

    val bumpedClientEpoch = (initialEpoch + 1).toShort

    // Action: Call InitProducerId(keepPreparedTxn=true) to set up dual identity
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = true,
      keepPreparedTxn = true,
      None,
      initProducerIdMockCallback
    )

    // Verify dual identity was set up in InitProducerId result
    assertEquals(testProducerId, result.producerId)
    assertEquals(bumpedClientEpoch, result.producerEpoch)
    assertEquals(testProducerId, result.ongoingTxnProducerId)
    assertEquals(initialEpoch, result.ongoingTxnProducerEpoch)
    assertEquals(Errors.NONE, result.error)

    // Verify dual identity was set up in TransactionMetadata
    assertEquals(testProducerId, txnMetadata.nextProducerId)
    assertEquals(bumpedClientEpoch, txnMetadata.nextProducerEpoch)

    capturedTransitMetadata
  }

  @Test
  def shouldReturnInvalidRequestWhenTransactionalIdIsEmpty(): Unit = {
    mockPidGenerator()

    coordinator.handleInitProducerId("", txnTimeoutMs, enableTwoPCFlag = false,
      keepPreparedTxn = false, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1L, -1, Errors.INVALID_REQUEST), result)
    coordinator.handleInitProducerId("", txnTimeoutMs, enableTwoPCFlag = false,
      keepPreparedTxn = false, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1L, -1, Errors.INVALID_REQUEST), result)
  }

  @Test
  def shouldReturnInvalidRequestWhenKeepPreparedIsTrue(): Unit = {
    mockPidGenerator()

    coordinator.handleInitProducerId("", txnTimeoutMs, enableTwoPCFlag = false,
      keepPreparedTxn = true, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1L, -1, Errors.INVALID_REQUEST), result)
  }

  @Test
  def shouldReturnInvalidRequestWhen2PCEnabledButBroker2PCConfigFalse(): Unit = {
    mockPidGenerator()

    coordinator.handleInitProducerId("", txnTimeoutMs, enableTwoPCFlag = true,
      keepPreparedTxn = false, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(-1L, -1, Errors.INVALID_REQUEST), result)
  }

  @Test
  def shouldAcceptInitPidAndReturnNextPidWhenTransactionalIdIsNull(): Unit = {
    mockPidGenerator()

    coordinator.handleInitProducerId(null, txnTimeoutMs, enableTwoPCFlag = false,
      keepPreparedTxn = false, None, initProducerIdMockCallback)
    assertEquals(InitProducerIdResult(0L, 0, Errors.NONE), result)
    coordinator.handleInitProducerId(null, txnTimeoutMs, enableTwoPCFlag = false,
      keepPreparedTxn = false, None, initProducerIdMockCallback)
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

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )
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

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, producerEpoch)),
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(nextPid - 1, 0, Errors.NONE), result)
  }

  @Test
  def shouldGenerateNewProducerIdIfEpochsExhausted(): Unit = {
    initPidGenericMocks(transactionalId)

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID, (Short.MaxValue - 1).toShort,
      (Short.MaxValue - 2).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.EMPTY, util.Set.of, time.milliseconds(), time.milliseconds(), TV_0)

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

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )
    assertNotEquals(producerId, result.producerId)
    assertEquals(0, result.producerEpoch)
    assertEquals(Errors.NONE, result.error)
  }

  @Test
  def shouldGenerateNewProducerIdIfEpochsExhaustedV2(): Unit = {
    initPidGenericMocks(transactionalId)

    val txnMetadata1 = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID, (Short.MaxValue - 1).toShort,
      (Short.MaxValue - 2).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, util.Set.of, time.milliseconds(), time.milliseconds(), TV_2)
    // We start with txnMetadata1 so we can transform the metadata to TransactionState.PREPARE_COMMIT.
    val txnMetadata2 = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID, (Short.MaxValue - 1).toShort,
      (Short.MaxValue - 2).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, util.Set.of, time.milliseconds(), time.milliseconds(), TV_2)
    val transitMetadata = txnMetadata2.prepareAbortOrCommit(TransactionState.PREPARE_COMMIT, TV_2, producerId2, RecordBatch.NO_PRODUCER_EPOCH, time.milliseconds(), false)
    txnMetadata2.completeTransitionTo(transitMetadata)

    assertEquals(producerId, txnMetadata2.producerId)
    assertEquals(Short.MaxValue, txnMetadata2.producerEpoch)

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata2))))

    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any[TxnTransitMetadata],
      capturedErrorsCallback.capture(),
      any(),
      any()
    )).thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.NONE))

    coordinator.handleEndTransaction(transactionalId, producerId, (Short.MaxValue - 1).toShort, TransactionResult.COMMIT, TV_2, endTxnCallback)
    assertEquals(producerId2, newProducerId)
    assertEquals(0, newEpoch)
    assertEquals(Errors.NONE, error)
  }

  @Test
  def shouldRespondWithNotCoordinatorOnInitPidWhenNotCoordinator(): Unit = {
    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.NOT_COORDINATOR))

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(-1, -1, Errors.NOT_COORDINATOR), result)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnInitPidWhenCoordinatorLoading(): Unit = {
    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(-1, -1, Errors.COORDINATOR_LOAD_IN_PROGRESS), result)
  }

  @Test
  def shouldRespondWithInvalidPidMappingOnAddPartitionsToTransactionWhenTransactionalIdNotPresent(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(None))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 1, partitions, errorsCallback, TV_0)
    assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
  }

  @Test
  def shouldRespondWithInvalidRequestAddPartitionsToTransactionWhenTransactionalIdIsEmpty(): Unit = {
    coordinator.handleAddPartitionsToTransaction("", 0L, 1, partitions, errorsCallback, TV_0)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithInvalidRequestAddPartitionsToTransactionWhenTransactionalIdIsNull(): Unit = {
    coordinator.handleAddPartitionsToTransaction(null, 0L, 1, partitions, errorsCallback, TV_0)
    assertEquals(Errors.INVALID_REQUEST, error)
  }

  @Test
  def shouldRespondWithNotCoordinatorOnAddPartitionsWhenNotCoordinator(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.NOT_COORDINATOR))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 1, partitions, errorsCallback, TV_0)
    assertEquals(Errors.NOT_COORDINATOR, error)
  }

  @Test
  def shouldRespondWithCoordinatorLoadInProgressOnAddPartitionsWhenCoordinatorLoading(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 1, partitions, errorsCallback, TV_0)
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
      0, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      0, TransactionState.PREPARE_COMMIT, partitions, 0, 0, TV_0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, wrongPidTxnMetadata))))

    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) =>
      assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error)
    }

    // If producer epoch is not equal, return PRODUCER_FENCED
    val oldEpochTxnMetadata = new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
      0, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      0, TransactionState.PREPARE_COMMIT, partitions, 0, 0, TV_0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, oldEpochTxnMetadata))))

    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 1, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) =>
      assertEquals(Errors.PRODUCER_FENCED, error)
    }
    
    // If the txn state is Prepare or AbortCommit, we return CONCURRENT_TRANSACTIONS
    val emptyTxnMetadata = new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
      0, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      0, TransactionState.PREPARE_COMMIT, partitions, 0, 0, TV_0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, emptyTxnMetadata))))
    
    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) => 
      assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    }

    // Pending state does not matter, we will just check if the partitions are in the txnMetadata.
    val ongoingTxnMetadata = new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
      0, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      0, TransactionState.ONGOING, util.Set.of, 0, 0, TV_0)
    ongoingTxnMetadata.pendingState(util.Optional.of(TransactionState.COMPLETE_COMMIT))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(new CoordinatorEpochAndTxnMetadata(coordinatorEpoch, ongoingTxnMetadata))))

    coordinator.handleVerifyPartitionsInTransaction(transactionalId, 0L, 0, partitions, verifyPartitionsInTxnCallback)
    errors.foreach { case (_, error) =>
      assertEquals(Errors.TRANSACTION_ABORTABLE, error)
    }
  }

  @Test
  def shouldRespondWithConcurrentTransactionsOnAddPartitionsWhenStateIsPrepareCommit(): Unit = {
    validateConcurrentTransactions(TransactionState.PREPARE_COMMIT)
  }

  @Test
  def shouldRespondWithConcurrentTransactionOnAddPartitionsWhenStateIsPrepareAbort(): Unit = {
    validateConcurrentTransactions(TransactionState.PREPARE_ABORT)
  }

  def validateConcurrentTransactions(state: TransactionState): Unit = {
    // Since the clientTransactionVersion doesn't matter, use 2 since the states are TransactionState.PREPARE_COMMIT and TransactionState.PREPARE_ABORT.
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
          0, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      0, state, util.Set.of, 0, 0, TV_2)))))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback, TV_2)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
  }

  @Test
  def shouldRespondWithProducerFencedOnAddPartitionsWhenEpochsAreDifferent(): Unit = {
    // Since the clientTransactionVersion doesn't matter, use 2 since the state is TransactionState.PREPARE_COMMIT.
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
          10, 9, RecordBatch.NO_PRODUCER_EPOCH, 0, TransactionState.PREPARE_COMMIT, util.Set.of, 0, 0, TV_2)))))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback, TV_2)
    assertEquals(Errors.PRODUCER_FENCED, error)
  }

  @Test
  def shouldAppendNewMetadataToLogOnAddPartitionsWhenPartitionsAdded(): Unit = {
    validateSuccessfulAddPartitions(TransactionState.EMPTY, 0)
  }

  @Test
  def shouldRespondWithSuccessOnAddPartitionsWhenStateIsOngoing(): Unit = {
    validateSuccessfulAddPartitions(TransactionState.ONGOING, 0)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldRespondWithSuccessOnAddPartitionsWhenStateIsCompleteCommit(clientTransactionVersion: Short): Unit = {
    validateSuccessfulAddPartitions(TransactionState.COMPLETE_COMMIT, clientTransactionVersion)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldRespondWithSuccessOnAddPartitionsWhenStateIsCompleteAbort(clientTransactionVersion: Short): Unit = {
    validateSuccessfulAddPartitions(TransactionState.COMPLETE_ABORT, clientTransactionVersion)
  }

  def validateSuccessfulAddPartitions(previousState: TransactionState, transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, previousState, util.Set.of, time.milliseconds(), time.milliseconds(), clientTransactionVersion)

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    coordinator.handleAddPartitionsToTransaction(transactionalId, producerId, producerEpoch, partitions, errorsCallback, clientTransactionVersion)

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
          0, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      0, TransactionState.EMPTY, partitions, 0, 0, TV_0)))))

    coordinator.handleAddPartitionsToTransaction(transactionalId, 0L, 0, partitions, errorsCallback, TV_0)
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
          0, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      0, TransactionState.ONGOING, partitions, 0, 0, TV_0)))))

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
          0, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      0, TransactionState.EMPTY, partitions, 0, 0, TV_0)))))

    val extraPartitions = new util.HashSet[TopicPartition](partitions)
    extraPartitions.add(new TopicPartition("topic2", 0))
    
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
  def shouldReplyWithInvalidPidMappingOnEndTxnWhenPidDoesntMatchMapped(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
        new TransactionMetadata(transactionalId, 10, 10, RecordBatch.NO_PRODUCER_ID,
          0, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      0, TransactionState.ONGOING, util.Set.of, 0, time.milliseconds(), TV_0)))))

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
          (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.ONGOING, util.Set.of, 0, time.milliseconds(), TV_0)))))

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
          (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.COMPLETE_COMMIT, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)))))

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
          (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.COMPLETE_COMMIT, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)))))

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
      producerEpoch, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.COMPLETE_ABORT, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    val nextProducerEpoch = if (isRetry) producerEpoch - 1 else producerEpoch
    coordinator.handleEndTransaction(transactionalId, producerId, nextProducerEpoch.toShort, TransactionResult.ABORT, clientTransactionVersion, endTxnCallback)
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
      producerEpoch, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.COMPLETE_ABORT, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)
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
      producerEpoch, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.COMPLETE_ABORT, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)
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
      producerEpoch, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.COMPLETE_COMMIT, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)
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
      producerEpoch, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.COMPLETE_COMMIT, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)
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
      producerEpoch, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.COMPLETE_COMMIT, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)
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
        RecordBatch.NO_PRODUCER_ID, producerEpoch, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.PREPARE_COMMIT, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)))))

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
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      1, TransactionState.PREPARE_ABORT, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)))))

    coordinator.handleEndTransaction(transactionalId, producerId, requestEpoch(clientTransactionVersion), TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    // TV2 returns CONCURRENT_TRANSACTIONS for all PREPARE_COMMIT/PREPARE_ABORT states to avoid reaching the IllegalStateException
    // TV0 returns INVALID_TXN_STATE when transaction type doesn't match
    val expectedError = if (clientTransactionVersion.supportsEpochBump()) Errors.CONCURRENT_TRANSACTIONS else Errors.INVALID_TXN_STATE
    assertEquals(expectedError, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def TestEndTxnRequestWhenEmptyTransactionStateForAbortInV1(): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId,
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      1, TransactionState.EMPTY, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)))))

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
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      1, TransactionState.EMPTY, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)))))

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
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      1, TransactionState.EMPTY, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)))))

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
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      1, TransactionState.PREPARE_COMMIT, util.Set.of, 0, time.milliseconds(), TV_2)))))

    // TV2 returns CONCURRENT_TRANSACTIONS for all PREPARE_COMMIT/PREPARE_ABORT states to avoid reaching the IllegalStateException
    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.COMMIT, TV_2, endTxnCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId,
        RecordBatch.NO_PRODUCER_ID, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      1, TransactionState.COMPLETE_COMMIT, util.Set.of, 0, time.milliseconds(), TV_2)))))

    // If producerEpoch is the same, this is not a retry of the EndTxnRequest, but the next EndTxnRequest. Return INVALID_TXN_STATE.
    coordinator.handleEndTransaction(transactionalId, producerId, producerEpoch, TransactionResult.COMMIT, TV_2, endTxnCallback)
    assertEquals(Errors.INVALID_TXN_STATE, error)
    verify(transactionManager, times(2)).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReturnOkOnEndTxnV2IfEndTxnV2RetryEpochOverflow(): Unit = {
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId, producerId,
        producerId2, Short.MaxValue, (Short.MaxValue - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.PREPARE_COMMIT, util.Set.of, 0, time.milliseconds(), TV_2)))))

    // Return CONCURRENT_TRANSACTIONS while transaction is still completing
    coordinator.handleEndTransaction(transactionalId, producerId, (Short.MaxValue - 1).toShort, TransactionResult.COMMIT, TV_2, endTxnCallback)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, new TransactionMetadata(transactionalId, producerId2, producerId,
        RecordBatch.NO_PRODUCER_ID, 0, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      1, TransactionState.COMPLETE_COMMIT, util.Set.of, 0, time.milliseconds(), TV_2)))))

    coordinator.handleEndTransaction(transactionalId, producerId, (Short.MaxValue - 1).toShort, TransactionResult.COMMIT, TV_2, endTxnCallback)
    assertEquals(Errors.NONE, error)
    assertNotEquals(RecordBatch.NO_PRODUCER_ID, newProducerId)
    assertNotEquals(producerId, newProducerId)
    assertEquals(0, newEpoch)
    verify(transactionManager, times(2)).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldReturnConcurrentTxnOnAddPartitionsIfEndTxnV2EpochOverflowAndNotComplete(): Unit = {
    val prepareWithPending = new TransactionMetadata(transactionalId, producerId, producerId,
      producerId2, Short.MaxValue, (Short.MaxValue - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, 1, TransactionState.PREPARE_COMMIT, util.Set.of, 0, time.milliseconds(), TV_2)
    val txnTransitMetadata = prepareWithPending.prepareComplete(time.milliseconds())

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, prepareWithPending))))

    // Return CONCURRENT_TRANSACTIONS while transaction is still completing
    coordinator.handleAddPartitionsToTransaction(transactionalId, producerId2, 0, partitions, errorsCallback, TV_2)
    assertEquals(Errors.CONCURRENT_TRANSACTIONS, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))

    prepareWithPending.completeTransitionTo(txnTransitMetadata)
    assertEquals(TransactionState.COMPLETE_COMMIT, prepareWithPending.state)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, prepareWithPending))))
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any[TxnTransitMetadata],
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.NONE))

    coordinator.handleAddPartitionsToTransaction(transactionalId, producerId2, 0, partitions, errorsCallback, TV_2)

    assertEquals(Errors.NONE, error)
    verify(transactionManager, times(2)).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldAppendPrepareCommitToLogOnEndTxnWhenStatusIsOngoingAndResultIsCommit(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    mockPrepare(TransactionState.PREPARE_COMMIT, clientTransactionVersion)

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
    mockPrepare(TransactionState.PREPARE_ABORT, clientTransactionVersion)

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
        new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID, metadataEpoch, 1, RecordBatch.NO_PRODUCER_EPOCH,
          1, TransactionState.COMPLETE_COMMIT, util.Set.of, 0, time.milliseconds(), clientTransactionVersion)))))

    coordinator.handleEndTransaction(transactionalId, producerId, requestEpoch, TransactionResult.COMMIT, clientTransactionVersion, endTxnCallback)
    assertEquals(Errors.PRODUCER_FENCED, error)
    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldIncrementEpochAndUpdateMetadataOnHandleInitPidWhenExistingEmptyTransaction(): Unit = {
    validateIncrementEpochAndUpdateMetadata(TransactionState.EMPTY, 0)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldIncrementEpochAndUpdateMetadataOnHandleInitPidWhenExistingCompleteTransaction(clientTransactionVersion: Short): Unit = {
    validateIncrementEpochAndUpdateMetadata(TransactionState.COMPLETE_ABORT, clientTransactionVersion)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def shouldIncrementEpochAndUpdateMetadataOnHandleInitPidWhenExistingCompleteCommitTransaction(clientTransactionVersion: Short): Unit = {
    validateIncrementEpochAndUpdateMetadata(TransactionState.COMPLETE_COMMIT, clientTransactionVersion)
  }

  @Test
  def shouldWaitForCommitToCompleteOnHandleInitPidAndExistingTransactionInPrepareCommitState(): Unit = {
    validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(TransactionState.PREPARE_COMMIT)
  }

  @Test
  def shouldWaitForCommitToCompleteOnHandleInitPidAndExistingTransactionInPrepareAbortState(): Unit = {
    validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(TransactionState.PREPARE_ABORT)
  }

  @ParameterizedTest(name = "enableTwoPCFlag={0}, keepPreparedTxn={1}")
  @CsvSource(Array("false, false"))
  def shouldAbortTransactionOnHandleInitPidWhenExistingTransactionInOngoingState(
    enableTwoPCFlag: Boolean,
    keepPreparedTxn:  Boolean
  ): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_0)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)

    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      (producerEpoch + 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_0)
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any[TxnTransitMetadata],
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.NONE))

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag,
      keepPreparedTxn,
      None,
      initProducerIdMockCallback
    )

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
    verify(transactionManager).validateTransactionTimeoutMs(anyBoolean(), anyInt())
    verify(transactionManager, times(3)).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(originalMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_0, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, time.milliseconds(), false)),
      any(),
      any(),
      any())
  }

  @Test
  def shouldFailToAbortTransactionOnHandleInitPidWhenProducerEpochIsSmaller(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_0)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)

    val bumpedTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      (producerEpoch + 2).toShort, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_0)

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, bumpedTxnMetadata))))

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )

    assertEquals(InitProducerIdResult(-1, -1, Errors.PRODUCER_FENCED), result)

    verify(transactionManager).validateTransactionTimeoutMs(anyBoolean(), anyInt())
    verify(transactionManager, times(2)).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  @Test
  def shouldNotRepeatedlyBumpEpochDueToInitPidDuringOngoingTxnIfAppendToLogFails(): Unit = {
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_0)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)

    when(transactionManager.putTransactionStateIfNotExists(any[TransactionMetadata]()))
      .thenReturn(Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))

    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenAnswer(_ => Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)

    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      (producerEpoch + 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_0)
    val txnTransitMetadata = originalMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_0, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, time.milliseconds(), false)
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(txnTransitMetadata),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => {
      capturedErrorsCallback.getValue.apply(Errors.NOT_ENOUGH_REPLICAS)
      txnMetadata.pendingState(util.Optional.empty())
    }).thenAnswer(_ => {
      capturedErrorsCallback.getValue.apply(Errors.NOT_ENOUGH_REPLICAS)
      txnMetadata.pendingState(util.Optional.empty())
    }).thenAnswer(_ => {
      capturedErrorsCallback.getValue.apply(Errors.NONE)

      // For the successful call, execute the state transitions that would happen in appendTransactionToLog()
      txnMetadata.completeTransitionTo(txnTransitMetadata)
      txnMetadata.prepareComplete(time.milliseconds())
    })

    // For the first two calls, verify that the epoch was only bumped once
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(-1, -1, Errors.NOT_ENOUGH_REPLICAS), result)

    assertEquals((producerEpoch + 1).toShort, txnMetadata.producerEpoch)
    assertTrue(txnMetadata.hasFailedEpochFence)

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(-1, -1, Errors.NOT_ENOUGH_REPLICAS), result)

    assertEquals((producerEpoch + 1).toShort, txnMetadata.producerEpoch)
    assertTrue(txnMetadata.hasFailedEpochFence)

    // For the last, successful call, verify that the epoch was not bumped further
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)

    assertEquals((producerEpoch + 1).toShort, txnMetadata.producerEpoch)
    assertFalse(txnMetadata.hasFailedEpochFence)

    verify(transactionManager, times(3)).validateTransactionTimeoutMs(anyBoolean(), anyInt())
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
      (Short.MaxValue - 1).toShort, (Short.MaxValue - 2).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_0)
    assertTrue(txnMetadata.isProducerEpochExhausted)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)

    val postFenceTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      Short.MaxValue, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.PREPARE_ABORT, partitions, time.milliseconds(), time.milliseconds(), TV_0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, postFenceTxnMetadata))))

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)

    // InitProducerId uses FenceProducerEpoch so clientTransactionVersion is 0.
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(new TxnTransitMetadata(
        producerId,
        producerId,
        RecordBatch.NO_PRODUCER_ID,
        Short.MaxValue,
        RecordBatch.NO_PRODUCER_EPOCH,
        RecordBatch.NO_PRODUCER_EPOCH,
        txnTimeoutMs,
        TransactionState.PREPARE_ABORT,
        partitions,
        time.milliseconds(),
        time.milliseconds(),
        TV_0)),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(_ => capturedErrorsCallback.getValue.apply(Errors.NONE))

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )
    assertEquals(Short.MaxValue, txnMetadata.producerEpoch)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
    verify(transactionManager).validateTransactionTimeoutMs(anyBoolean(), anyInt())
    verify(transactionManager, times(3)).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(new TxnTransitMetadata(
        producerId,
        producerId,
        RecordBatch.NO_PRODUCER_ID,
        Short.MaxValue,
        RecordBatch.NO_PRODUCER_EPOCH,
        RecordBatch.NO_PRODUCER_EPOCH,
        txnTimeoutMs,
        TransactionState.PREPARE_ABORT,
        partitions,
        time.milliseconds(),
        time.milliseconds(),
        TV_0)),
      any(),
      any(),
      any())
  }

  @Test
  def shouldNotCauseEpochOverflowWhenInitPidDuringOngoingTxnV2(): Unit = {
    // When InitProducerId is called with an ongoing transaction at epoch 32766 (Short.MaxValue - 1),
    // it should not cause an epoch overflow by incrementing twice.
    // The only true increment happens in prepareAbortOrCommit
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      (Short.MaxValue - 1).toShort, (Short.MaxValue - 2).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_2)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    when(transactionManager.transactionVersionLevel()).thenReturn(TV_2)

    // Capture the transition metadata to verify epoch increments
    val capturedTxnTransitMetadata: ArgumentCaptor[TxnTransitMetadata] = ArgumentCaptor.forClass(classOf[TxnTransitMetadata])
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      capturedTxnTransitMetadata.capture(),
      capturedErrorsCallback.capture(),
      any(),
      any())
    ).thenAnswer(invocation => {
      val transitMetadata = invocation.getArgument[TxnTransitMetadata](2)
      // Simulate the metadata update that would happen in the real appendTransactionToLog
      txnMetadata.completeTransitionTo(transitMetadata)
      capturedErrorsCallback.getValue.apply(Errors.NONE)
    })

    // Handle InitProducerId with ongoing transaction at epoch 32766
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )

    // Verify that the epoch did not overflow (should be Short.MaxValue = 32767, not negative)
    assertEquals(Short.MaxValue, txnMetadata.producerEpoch)
    assertEquals(TransactionState.PREPARE_ABORT, txnMetadata.state)
    
    verify(transactionManager).validateTransactionTimeoutMs(anyBoolean(), anyInt())
    verify(transactionManager, times(3)).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(transactionManager).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any[TxnTransitMetadata],
      any(),
      any(),
      any())
  }

  @Test
  def shouldHandleTimeoutAtEpochOverflowBoundaryCorrectlyTV2(): Unit = {
    // Test the scenario where we have an ongoing transaction at epoch 32766 (Short.MaxValue - 1)
    // and the producer crashes/times out. This test verifies that the timeout handling
    // correctly manages the epoch overflow scenario without causing failures.

    val epochAtMaxBoundary = (Short.MaxValue - 1).toShort // 32766
    val now = time.milliseconds()
    
    val bumpedUpProducerId = producerId + 1L
    val bumpedUpEpoch = 0.toShort
    when(pidGenerator.generateProducerId())
      .thenReturn(bumpedUpProducerId)

    // Create transaction metadata at the epoch boundary that would cause overflow IFF double-incremented
    val txnMetadata = new TransactionMetadata(
      transactionalId,
      producerId,
      RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_ID,
      epochAtMaxBoundary,
      RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING,
      partitions,
      now,
      now,
      TV_2
    )
    assertTrue(txnMetadata.isProducerEpochExhausted)

    // Mock the transaction manager to return our test transaction as timed out
    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, epochAtMaxBoundary)))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    when(transactionManager.transactionVersionLevel()).thenReturn(TV_2)

    // Mock the append operation to simulate successful write and update the metadata
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      capturedTxnTransitMetadata.capture(),
      any[Errors => Unit](),
      any(),
      any())
    ).thenAnswer(invocation => {
      val transitMetadata = invocation.getArgument[TxnTransitMetadata](2)
      val callback = invocation.getArgument[Errors => Unit](3)
      txnMetadata.completeTransitionTo(transitMetadata)
      callback.apply(Errors.NONE)
    })

    // Track the actual behavior
    var callbackInvoked = false
    var resultError: Errors = null
    var resultProducerId: Long = -1
    var resultEpoch: Short = -1

    def checkOnEndTransactionComplete(txnIdAndPidEpoch: TransactionalIdAndProducerIdEpoch)
      (error: Errors, newProducerId: Long, newProducerEpoch: Short): Unit = {
        callbackInvoked = true
        resultError = error
        resultProducerId = newProducerId
        resultEpoch = newProducerEpoch
      }

    // Execute the timeout abort process
    coordinator.abortTimedOutTransactions(checkOnEndTransactionComplete)

    assertTrue(callbackInvoked, "Callback should have been invoked")
    assertEquals(Errors.NONE, resultError, "Expected no errors in the callback")
    assertEquals(bumpedUpProducerId, resultProducerId, "Expected producer ID should be rotated because of epoch exhausted.")
    assertEquals(bumpedUpEpoch, resultEpoch, "Expected producer epoch to be 0 as a result of ProducerId rotation.")
    
    // Verify the transaction metadata was correctly updated to the final epoch
    assertEquals(TransactionState.PREPARE_ABORT, txnMetadata.state())
    assertEquals(producerId, txnMetadata.producerId(), "Expected producer ID should not be rotated because txnMarker is not written yet.")
    assertEquals(Short.MaxValue, txnMetadata.producerEpoch,
      s"Expected transaction metadata producer epoch to be ${Short.MaxValue} " +
        s"after timeout handling, but was ${txnMetadata.producerEpoch}"
    )

    // Verify the basic flow was attempted
    verify(transactionManager).timedOutTransactions()
    verify(transactionManager, atLeast(1)).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(pidGenerator, times(1)).generateProducerId()
  }

  @Test
  def shouldHandleTimeoutAtEpochOverflowBoundaryCorrectlyAndLateClientAbortRequestTV2(): Unit = {
    // 1. The transaction coordinator aborts the transaction due to a timeout at epoch 32766 
    //    (timeout -> fenced -> prepare abort -> complete abort) 
    // 2. The client sends an abort request later.

    val epochAtMaxBoundary = (Short.MaxValue - 1).toShort // 32766
    val now = time.milliseconds()

    val rotatedProducerId = producerId + 1L
    val rotatedEpoch = 0.toShort
    when(pidGenerator.generateProducerId())
      .thenReturn(rotatedProducerId)

    // Create transaction metadata at the epoch boundary that would cause overflow IFF double-incremented
    val txnMetadata = new TransactionMetadata(
      transactionalId,
      producerId,
      RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_ID,
      epochAtMaxBoundary,
      RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs,
      TransactionState.ONGOING,
      partitions,
      now,
      now,
      TV_2
    )
    assertTrue(txnMetadata.isProducerEpochExhausted)

    // Mock the transaction manager to return our test transaction as timed out
    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, epochAtMaxBoundary)))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    when(transactionManager.transactionVersionLevel()).thenReturn(TV_2)

    // Mock the append operation to simulate successful write and update the metadata
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      capturedTxnTransitMetadata.capture(),
      any[Errors => Unit](),
      any(),
      any())
    ).thenAnswer(invocation => {
      val transitMetadata = invocation.getArgument[TxnTransitMetadata](2)
      val callback = invocation.getArgument[Errors => Unit](3)
      txnMetadata.completeTransitionTo(transitMetadata)
      callback.apply(Errors.NONE)
    })

    // Simulate marker write completion by appending COMPLETE_ABORT.
    doAnswer(invocation => {
      val markerCoordinatorEpoch = invocation.getArgument[Int](0)
      val markerTxnMetadata = invocation.getArgument[TransactionMetadata](2)
      val newTxnMetadata = invocation.getArgument[TxnTransitMetadata](3)
      transactionManager.appendTransactionToLog(
        markerTxnMetadata.transactionalId(),
        markerCoordinatorEpoch,
        newTxnMetadata,
        _ => (),
        _ == Errors.COORDINATOR_NOT_AVAILABLE,
        RequestLocal.noCaching
      )
      null
    }).when(transactionMarkerChannelManager).addTxnMarkersToSend(
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(TransactionResult.ABORT),
      ArgumentMatchers.eq(txnMetadata),
      any[TxnTransitMetadata]()
    )
    
    // Track the actual behavior
    var callbackInvoked = false
    var resultError: Errors = null
    var resultProducerId: Long = -1
    var resultEpoch: Short = -1

    def checkOnEndTransactionComplete(txnIdAndPidEpoch: TransactionalIdAndProducerIdEpoch)
       (error: Errors, newProducerId: Long, newProducerEpoch: Short): Unit = {
      callbackInvoked = true
      resultError = error
      resultProducerId = newProducerId
      resultEpoch = newProducerEpoch

      // TransitMetadata should be rotated.
      assertEquals(Errors.NONE, resultError, "Expected no errors in the callback")
      assertEquals(rotatedProducerId, resultProducerId, "Expected producer ID should be rotated because of epoch exhausted.")
      assertEquals(rotatedEpoch, resultEpoch, "Expected producer epoch to be 0 as a result of ProducerId rotation.")

      // The local transaction state is not updated yet.
      assertEquals(TransactionState.PREPARE_ABORT, txnMetadata.state())
      assertEquals(producerId, txnMetadata.producerId(), "Expected producer ID should not be rotated because txnMarker is not written yet.")
      assertEquals(Short.MaxValue, txnMetadata.producerEpoch,
        s"Expected transaction metadata producer epoch to be ${Short.MaxValue} " +
          s"after timeout handling, but was ${txnMetadata.producerEpoch}"
      )
    }

    // Execute the timeout abort process
    coordinator.abortTimedOutTransactions(checkOnEndTransactionComplete)

    // the transaction completion callback was invoked.
    assertTrue(callbackInvoked, "Callback should have been invoked")

    val capturedTransitions = capturedTxnTransitMetadata.getAllValues.asScala.toList
    val prepareAbortTransition = capturedTransitions.head
    assertEquals(TransactionState.PREPARE_ABORT, prepareAbortTransition.txnState)
    assertEquals(rotatedProducerId, prepareAbortTransition.nextProducerId)
    assertTrue(capturedTransitions.exists(_.txnState == TransactionState.COMPLETE_ABORT))

    // Verify the transaction metadata was correctly updated to the final epoch as a result of sendMarkerTxn.
    assertEquals(TransactionState.COMPLETE_ABORT, txnMetadata.state())
    assertEquals(rotatedProducerId, txnMetadata.producerId())
    assertEquals(rotatedEpoch, txnMetadata.producerEpoch)

    // Verify the basic flow was attempted
    verify(transactionManager).timedOutTransactions()
    verify(transactionManager, times(2)).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any(),
      any(),
      any(),
      any()
    )
    verify(transactionManager, atLeast(1)).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(pidGenerator, times(1)).generateProducerId()
    verify(transactionMarkerChannelManager, times(1)).addTxnMarkersToSend(
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(TransactionResult.ABORT),
      ArgumentMatchers.eq(txnMetadata),
      any[TxnTransitMetadata]()
    )

    // Simulate that client send abort request lately. 
    val clientPid = producerId
    val clientEpoch = epochAtMaxBoundary

    var clientCallbackInvoked = false
    var clientErr: Errors = null
    var clientReturnedPid: Long = -1L
    var clientReturnedEpoch: Short = -1

    def onClientEndTxn(error: Errors, newProducerId: Long, newProducerEpoch: Short): Unit = {
      clientCallbackInvoked = true
      clientErr = error
      clientReturnedPid = newProducerId
      clientReturnedEpoch = newProducerEpoch
    }

    // WHEN : Client tries to abort transaction after server abort transaction because of timeout.
    coordinator.handleEndTransaction(
      transactionalId,
      clientPid,
      clientEpoch,
      TransactionResult.ABORT,
      TV_2,
      onClientEndTxn,
      RequestLocal.noCaching
    )

    // THEN : It should be treated as a retry.
    assertTrue(clientCallbackInvoked)
    assertEquals(Errors.NONE, clientErr)
    assertEquals(rotatedProducerId, clientReturnedPid)
    assertEquals(rotatedEpoch, clientReturnedEpoch)
  }

  @Test
  def shouldRotateProducerIdWhenInitPidFencesOngoingTxnAtEpochOverflowBoundaryTV2(): Unit = {
    // 1. The transaction coordinator aborts the transaction because a new InitProducerId fences an ongoing transaction at epoch 32766. 
    //    (InitProducerId -> fenced -> prepare abort -> complete abort) 
    // 2. The client sends an abort request later.
    
    val epochAtMaxBoundary = (Short.MaxValue - 1).toShort // 32766
    val now = time.milliseconds()

    val rotatedProducerId = producerId + 1L
    val rotatedEpoch = 0.toShort
    when(pidGenerator.generateProducerId())
      .thenReturn(rotatedProducerId)

    val txnMetadata = new TransactionMetadata(
      transactionalId,
      producerId,
      RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_ID,
      epochAtMaxBoundary,
      RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs,
      TransactionState.ONGOING,
      partitions,
      now,
      now,
      TV_2
    )
    assertTrue(txnMetadata.isProducerEpochExhausted)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    when(transactionManager.transactionVersionLevel()).thenReturn(TV_2)

    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      capturedTxnTransitMetadata.capture(),
      any[Errors => Unit](),
      any(),
      any())
    ).thenAnswer(invocation => {
      val transitMetadata = invocation.getArgument[TxnTransitMetadata](2)
      val callback = invocation.getArgument[Errors => Unit](3)
      txnMetadata.completeTransitionTo(transitMetadata)
      callback.apply(Errors.NONE)
    })

    // Simulate marker write completion by appending COMPLETE_ABORT.
    doAnswer(invocation => {
      val markerCoordinatorEpoch = invocation.getArgument[Int](0)
      val markerTxnMetadata = invocation.getArgument[TransactionMetadata](2)
      val newTxnMetadata = invocation.getArgument[TxnTransitMetadata](3)
      transactionManager.appendTransactionToLog(
        markerTxnMetadata.transactionalId(),
        markerCoordinatorEpoch,
        newTxnMetadata,
        _ => (),
        _ == Errors.COORDINATOR_NOT_AVAILABLE,
        RequestLocal.noCaching
      )
      null
    }).when(transactionMarkerChannelManager).addTxnMarkersToSend(
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(TransactionResult.ABORT),
      ArgumentMatchers.eq(txnMetadata),
      any[TxnTransitMetadata]()
    )

    // WHEN1: Trigger fencing of the ongoing transaction.
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )

    // THEN1
    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)

    val capturedTransitions = capturedTxnTransitMetadata.getAllValues.asScala.toList
    val firstAbortTransition = capturedTransitions.head
    assertEquals(TransactionState.PREPARE_ABORT, firstAbortTransition.txnState)
    assertEquals(producerId, firstAbortTransition.producerId)
    assertEquals(Short.MaxValue, firstAbortTransition.producerEpoch)
    assertEquals(rotatedProducerId, firstAbortTransition.nextProducerId)
    assertTrue(capturedTransitions.exists(_.txnState == TransactionState.COMPLETE_ABORT))

    // Marker completion should rotate producer ID on COMPLETE_ABORT.
    assertEquals(TransactionState.COMPLETE_ABORT, txnMetadata.state())
    assertEquals(rotatedProducerId, txnMetadata.producerId())
    assertEquals(rotatedEpoch, txnMetadata.producerEpoch)

    // Client retries ABORT with old pid/epoch and should be treated as retryOnOverflow.
    var clientCallbackInvoked = false
    var clientErr: Errors = null
    var clientReturnedPid: Long = -1L
    var clientReturnedEpoch: Short = -1

    def onClientEndTxn(error: Errors, newProducerId: Long, newProducerEpoch: Short): Unit = {
      clientCallbackInvoked = true
      clientErr = error
      clientReturnedPid = newProducerId
      clientReturnedEpoch = newProducerEpoch
    }

    // WHEN2 : The client tries to abort the transaction after the coordinator has already aborted it. 
    coordinator.handleEndTransaction(
      transactionalId,
      producerId,
      epochAtMaxBoundary,
      TransactionResult.ABORT,
      TV_2,
      onClientEndTxn,
      RequestLocal.noCaching
    )

    // THEN2 : It should be treated as a retry.
    assertTrue(clientCallbackInvoked)
    assertEquals(Errors.NONE, clientErr)
    assertEquals(rotatedProducerId, clientReturnedPid)
    assertEquals(rotatedEpoch, clientReturnedEpoch)
    verify(transactionManager, times(2)).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any(),
      any(),
      any(),
      any()
    )
    verify(pidGenerator, times(1)).generateProducerId()
    verify(transactionMarkerChannelManager, times(1)).addTxnMarkersToSend(
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(TransactionResult.ABORT),
      ArgumentMatchers.eq(txnMetadata),
      any[TxnTransitMetadata]()
    )
  }

  @Test
  def shouldHandleTimeoutAtEpochOverflowBoundaryCorrectlyAndRetryInitProducerIdTV2(): Unit = {
    // 1. The transaction coordinator aborts the transaction due to a timeout at epoch 32766
    //    (timeout -> prepare abort -> complete abort with producerId rotation)
    // 2. The original client retries InitProducerId with the old producerId/epoch.

    val epochAtMaxBoundary = (Short.MaxValue - 1).toShort // 32766
    val now = time.milliseconds()

    val rotatedProducerId = producerId + 1L
    val rotatedEpoch = 0.toShort
    when(pidGenerator.generateProducerId())
      .thenReturn(rotatedProducerId)

    // Create transaction metadata at the epoch boundary that would cause overflow IFF double-incremented
    val txnMetadata = new TransactionMetadata(
      transactionalId,
      producerId,
      RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_ID,
      epochAtMaxBoundary,
      RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs,
      TransactionState.ONGOING,
      partitions,
      now,
      now,
      TV_2
    )
    assertTrue(txnMetadata.isProducerEpochExhausted)

    // Mock the transaction manager to return our test transaction as timed out
    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, epochAtMaxBoundary)))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    when(transactionManager.transactionVersionLevel()).thenReturn(TV_2)
    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)

    // Mock the append operation to simulate successful write and update the metadata
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      capturedTxnTransitMetadata.capture(),
      any[Errors => Unit](),
      any(),
      any())
    ).thenAnswer(invocation => {
      val transitMetadata = invocation.getArgument[TxnTransitMetadata](2)
      val callback = invocation.getArgument[Errors => Unit](3)
      txnMetadata.completeTransitionTo(transitMetadata)
      callback.apply(Errors.NONE)
    })

    // Simulate marker write completion by appending COMPLETE_ABORT.
    doAnswer(invocation => {
      val markerCoordinatorEpoch = invocation.getArgument[Int](0)
      val markerTxnMetadata = invocation.getArgument[TransactionMetadata](2)
      val newTxnMetadata = invocation.getArgument[TxnTransitMetadata](3)
      transactionManager.appendTransactionToLog(
        markerTxnMetadata.transactionalId(),
        markerCoordinatorEpoch,
        newTxnMetadata,
        _ => (),
        _ == Errors.COORDINATOR_NOT_AVAILABLE,
        RequestLocal.noCaching
      )
      null
    }).when(transactionMarkerChannelManager).addTxnMarkersToSend(
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(TransactionResult.ABORT),
      ArgumentMatchers.eq(txnMetadata),
      any[TxnTransitMetadata]()
    )

    // Track the actual behavior
    var callbackInvoked = false

    def checkOnEndTransactionComplete(txnIdAndPidEpoch: TransactionalIdAndProducerIdEpoch)
                                     (error: Errors, newProducerId: Long, newProducerEpoch: Short): Unit = {
      callbackInvoked = true

      // TransitMetadata should be rotated.
      assertEquals(Errors.NONE, error, "Expected no errors in the callback")
      assertEquals(rotatedProducerId, newProducerId, "Expected producer ID should be rotated because of epoch exhausted.")
      assertEquals(rotatedEpoch, newProducerEpoch, "Expected producer epoch to be 0 as a result of ProducerId rotation.")

      // The local transaction state is not updated yet.
      assertEquals(TransactionState.PREPARE_ABORT, txnMetadata.state())
      assertEquals(producerId, txnMetadata.producerId(), "Expected producer ID should not be rotated because txnMarker is not written yet.")
      assertEquals(Short.MaxValue, txnMetadata.producerEpoch,
        s"Expected transaction metadata producer epoch to be ${Short.MaxValue} " +
          s"after timeout handling, but was ${txnMetadata.producerEpoch}"
      )
    }

    // Execute the timeout abort process
    coordinator.abortTimedOutTransactions(checkOnEndTransactionComplete)

    // the transaction completion callback was invoked.
    assertTrue(callbackInvoked, "Callback should have been invoked")

    val capturedTransitions = capturedTxnTransitMetadata.getAllValues.asScala.toList
    val prepareAbortTransition = capturedTransitions.head
    assertEquals(TransactionState.PREPARE_ABORT, prepareAbortTransition.txnState)
    assertEquals(rotatedProducerId, prepareAbortTransition.nextProducerId)
    assertTrue(capturedTransitions.exists(_.txnState == TransactionState.COMPLETE_ABORT))

    // Verify the transaction metadata was correctly updated to the final epoch as a result of sendMarkerTxn.
    assertEquals(TransactionState.COMPLETE_ABORT, txnMetadata.state())
    assertEquals(rotatedProducerId, txnMetadata.producerId())
    assertEquals(rotatedEpoch, txnMetadata.producerEpoch)

    // Verify the basic flow was attempted
    verify(transactionManager).timedOutTransactions()
    verify(transactionManager, times(2)).appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any(),
      any(),
      any(),
      any()
    )
    verify(transactionManager, atLeast(1)).getTransactionState(ArgumentMatchers.eq(transactionalId))
    verify(pidGenerator, times(1)).generateProducerId()
    verify(transactionMarkerChannelManager, times(1)).addTxnMarkersToSend(
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(TransactionResult.ABORT),
      ArgumentMatchers.eq(txnMetadata),
      any[TxnTransitMetadata]()
    )

    // WHEN: The original client retries InitProducerId after the coordinator has already
    // completed the timeout-driven abort and rotated the producerId.
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, epochAtMaxBoundary)),
      initProducerIdMockCallback
    )
    
    // THEN
    val expectedResult = InitProducerIdResult(rotatedProducerId, rotatedEpoch, Errors.NONE) 
    assertEquals(expectedResult, result)
  }

  @Test
  def testInitProducerIdWithNoLastProducerData(): Unit = {
    // If the metadata doesn't include the previous producer data (for example, if it was written to the log by a broker
    // on an old version), the retry case should fail
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_EPOCH, (producerEpoch + 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.EMPTY, partitions, time.milliseconds, time.milliseconds, TV_0)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    // Simulate producer trying to continue after new producer has already been initialized
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, producerEpoch)),
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Errors.PRODUCER_FENCED), result)
  }

  @Test
  def testFenceProducerWhenMappingExistsWithDifferentProducerId(): Unit = {
    // Existing transaction ID maps to new producer ID
    val txnMetadata = new TransactionMetadata(transactionalId, producerId + 1, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, producerEpoch, (producerEpoch - 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.EMPTY, partitions, time.milliseconds, time.milliseconds, TV_0)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    // Simulate producer trying to continue after new producer has already been initialized
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, producerEpoch)),
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Errors.PRODUCER_FENCED), result)
  }

  @Test
  def testInitProducerIdWithCurrentEpochProvided(): Unit = {
    mockPidGenerator()

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_ID, 10, 9, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.EMPTY, partitions, time.milliseconds, time.milliseconds, TV_0)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
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
      txnMetadata.pendingState(util.Optional.empty())
    })

    // Re-initialization should succeed and bump the producer epoch
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, 10)),
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(producerId, 11, Errors.NONE), result)

    // Simulate producer retrying after successfully re-initializing but failing to receive the response
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, 10)),
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(producerId, 11, Errors.NONE), result)
  }

  @Test
  def testInitProducerIdStaleCurrentEpochProvided(): Unit = {
    mockPidGenerator()

    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_ID, 10, 9, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.EMPTY, partitions, time.milliseconds, time.milliseconds, TV_0)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
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
      txnMetadata.pendingState(util.Optional.empty())
      txnMetadata.setProducerEpoch(capturedTxnTransitMetadata.getValue.producerEpoch)
      txnMetadata.setLastProducerEpoch(capturedTxnTransitMetadata.getValue.lastProducerEpoch)
    })

    // With producer epoch at 10, new producer calls InitProducerId and should get epoch 11
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(producerId, 11, Errors.NONE), result)

    // Simulate old producer trying to continue from epoch 10
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, 10)),
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Errors.PRODUCER_FENCED), result)
  }

  @Test
  def testRetryInitProducerIdAfterProducerIdRotation(): Unit = {
    // Existing transaction ID maps to new producer ID
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, (Short.MaxValue - 1).toShort, (Short.MaxValue - 2).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.EMPTY, partitions, time.milliseconds, time.milliseconds, TV_0)

    when(pidGenerator.generateProducerId())
      .thenReturn(producerId + 1)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
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
      txnMetadata.pendingState(util.Optional.empty())
      txnMetadata.setProducerId(capturedTxnTransitMetadata.getValue.producerId)
      txnMetadata.setPrevProducerId(capturedTxnTransitMetadata.getValue.prevProducerId)
      txnMetadata.setProducerEpoch(capturedTxnTransitMetadata.getValue.producerEpoch)
      txnMetadata.setLastProducerEpoch(capturedTxnTransitMetadata.getValue.lastProducerEpoch)
    })

    // Bump epoch and cause producer ID to be rotated
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, (Short.MaxValue - 1).toShort)),
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(producerId + 1, 0, Errors.NONE), result)

    // Simulate producer retrying old request after producer bump
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, (Short.MaxValue - 1).toShort)),
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(producerId + 1, 0, Errors.NONE), result)
  }

  @Test
  def testInitProducerIdWithInvalidEpochAfterProducerIdRotation(): Unit = {
    // Existing transaction ID maps to new producer ID
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, (Short.MaxValue - 1).toShort, (Short.MaxValue - 2).toShort, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.EMPTY, partitions, time.milliseconds, time.milliseconds, TV_0)

    when(pidGenerator.generateProducerId())
      .thenReturn(producerId + 1)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
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
      txnMetadata.pendingState(util.Optional.empty())
      txnMetadata.setProducerId(capturedTxnTransitMetadata.getValue.producerId)
      txnMetadata.setPrevProducerId(capturedTxnTransitMetadata.getValue.prevProducerId)
      txnMetadata.setProducerEpoch(capturedTxnTransitMetadata.getValue.producerEpoch)
      txnMetadata.setLastProducerEpoch(capturedTxnTransitMetadata.getValue.lastProducerEpoch)
    })

    // Bump epoch and cause producer ID to be rotated
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, (Short.MaxValue - 1).toShort)),
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(producerId + 1, 0, Errors.NONE), result)

    // Validate that producer with old producer ID and stale epoch is fenced
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, (Short.MaxValue - 2).toShort)),
      initProducerIdMockCallback
    )
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
      RecordBatch.NO_PRODUCER_EPOCH, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, now, now, TV_0)

    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    // Transaction timeouts use FenceProducerEpoch so clientTransactionVersion is 0.
    val expectedTransition = new TxnTransitMetadata(producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH, (producerEpoch + 1).toShort,
      RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.PREPARE_ABORT, partitions, now,
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
      RecordBatch.NO_PRODUCER_EPOCH, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, now, now, TV_0)

    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    when(transactionManager.transactionVersionLevel()).thenReturn(TV_0)

    val bumpedTxnMetadata = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, (producerEpoch + 2).toShort, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, now, now, TV_0)
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
      RecordBatch.NO_PRODUCER_EPOCH, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_0)
    metadata.prepareAbortOrCommit(TransactionState.PREPARE_COMMIT, TV_0, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, time.milliseconds(), false)

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
      RecordBatch.NO_PRODUCER_EPOCH, producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, now, now, TV_0)

    when(transactionManager.timedOutTransactions())
      .thenReturn(List(TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))

    val txnMetadataAfterAppendFailure = new TransactionMetadata(transactionalId, producerId, producerId,
      RecordBatch.NO_PRODUCER_EPOCH, (producerEpoch + 1).toShort, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, now, now, TV_0)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadataAfterAppendFailure))))

    // Transaction timeouts use FenceProducerEpoch so clientTransactionVersion is 0.
    val bumpedEpoch = (producerEpoch + 1).toShort
    val expectedTransition = new TxnTransitMetadata(producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH, bumpedEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.PREPARE_ABORT, partitions, now,
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
      RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_0)
    txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_COMMIT, TV_0, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, time.milliseconds(), false)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      Some(new ProducerIdAndEpoch(producerId, 10)),
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, Errors.CONCURRENT_TRANSACTIONS), result)

    verify(transactionManager).validateTransactionTimeoutMs(anyBoolean(), anyInt())
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
      RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.DEAD, util.Set.of, time.milliseconds(),
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
      RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_0)

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

    val addedPartitions = result.topics.stream.flatMap(topicData =>
        topicData.partitions.stream
          .map(partition => new TopicPartition(topicData.topic, partition))
      )
      .collect(util.stream.Collectors.toSet());
    assertEquals(partitions, addedPartitions)

    verify(transactionManager).getTransactionState(ArgumentMatchers.eq(transactionalId))
  }

  private def validateRespondsWithConcurrentTransactionsOnInitPidWhenInPrepareState(state: TransactionState): Unit = {
    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)

    // Since the clientTransactionVersion doesn't matter, use 2 since the states are TransactionState.PREPARE_COMMIT and TransactionState.PREPARE_ABORT.
    val metadata = new TransactionMetadata(transactionalId, 0, 0, RecordBatch.NO_PRODUCER_ID,
      0, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH,
      0, state, util.Set.of[TopicPartition](new TopicPartition("topic", 1)), 0, 0, TV_2)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata))))

    coordinator.handleInitProducerId(transactionalId, 10, enableTwoPCFlag = false,
      keepPreparedTxn = false, None, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)
  }

  private def validateIncrementEpochAndUpdateMetadata(state: TransactionState, transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    when(pidGenerator.generateProducerId())
      .thenReturn(producerId)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)

    val metadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, state, util.Set.of, time.milliseconds(), time.milliseconds(), clientTransactionVersion)
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
    coordinator.handleInitProducerId(transactionalId, newTxnTimeoutMs, enableTwoPCFlag = false,
      keepPreparedTxn = false, None, initProducerIdMockCallback)

    assertEquals(InitProducerIdResult(producerId, (producerEpoch + 1).toShort, Errors.NONE), result)
    assertEquals(newTxnTimeoutMs, metadata.txnTimeoutMs)
    assertEquals(time.milliseconds(), metadata.txnLastUpdateTimestamp)
    assertEquals((producerEpoch + 1).toShort, metadata.producerEpoch)
    assertEquals(producerId, metadata.producerId)
  }

  private def mockPrepare(transactionState: TransactionState, clientTransactionVersion: TransactionVersion, runCallback: Boolean = false): TransactionMetadata = {
    val now = time.milliseconds()
    val originalMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_EPOCH,
      producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, now, now, TV_0)

    val transition = new TxnTransitMetadata(producerId, producerId, RecordBatch.NO_PRODUCER_ID, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, transactionState, partitions, now, now, clientTransactionVersion)

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

    new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID, producerEpoch,
      RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, transactionState, partitions, time.milliseconds(), time.milliseconds(), clientTransactionVersion)
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

  @Test
  def testTV2AllowsEpochReBumpingAfterFailedWrite(): Unit = {
    // Test the complete TV2 flow: failed write → epoch fence → abort → retry with epoch bump
    // This demonstrates that TV2 allows epoch re-bumping after failed writes (unlike TV1)
    val producerEpoch = 1.toShort
    val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, RecordBatch.NO_PRODUCER_ID,
      producerEpoch, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_PRODUCER_EPOCH, txnTimeoutMs, TransactionState.ONGOING, partitions, time.milliseconds(), time.milliseconds(), TV_2)

    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    when(transactionManager.transactionVersionLevel()).thenReturn(TV_2)

    // First attempt fails with COORDINATOR_NOT_AVAILABLE
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any(),
      any(),
      any(),
      any()
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument[Errors => Unit](3)

      // Simulate the real TransactionStateManager behavior: reset pendingState on failure
      // since handleInitProducerId doesn't provide a custom retryOnError function
      txnMetadata.pendingState(util.Optional.empty())

      // For TV2, hasFailedEpochFence is NOT set to true, allowing epoch bumps on retry
      // The epoch remains at its original value (1) since completeTransitionTo was never called

      callback.apply(Errors.COORDINATOR_NOT_AVAILABLE)
    })

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )
    assertEquals(InitProducerIdResult(-1, -1, Errors.COORDINATOR_NOT_AVAILABLE), result)

    // After the first failed attempt, the state should be:
    // - hasFailedEpochFence = false (NOT set for TV2)
    // - pendingState = None (reset by TransactionStateManager)
    // - producerEpoch = 1 (unchanged since completeTransitionTo was never called)
    // - transaction still ONGOING

    // Second attempt: Should abort the ongoing transaction
    reset(transactionManager)
    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    when(transactionManager.transactionVersionLevel()).thenReturn(TV_2)

    // Mock the appendTransactionToLog to succeed for the endTransaction call
    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any(),
      any(),
      any(),
      any()
    )).thenAnswer(invocation => {
      val newMetadata = invocation.getArgument[TxnTransitMetadata](2)
      val callback = invocation.getArgument[Errors => Unit](3)
      
      // Complete the transition and call the callback with success
      txnMetadata.completeTransitionTo(newMetadata)
      callback.apply(Errors.NONE)
    })

    // Mock the transactionMarkerChannelManager to simulate the second write (PREPARE_ABORT -> COMPLETE_ABORT)
    doAnswer(invocation => {
      val newMetadata = invocation.getArgument[TxnTransitMetadata](3)
      // Simulate the completion of transaction markers and the second write
      // This would normally happen asynchronously after markers are sent
      txnMetadata.completeTransitionTo(newMetadata) // This transitions to COMPLETE_ABORT
      txnMetadata.pendingState(util.Optional.empty())
      
      null
    }).when(transactionMarkerChannelManager).addTxnMarkersToSend(
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(TransactionResult.ABORT),
      ArgumentMatchers.eq(txnMetadata),
      any()
    )

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )

    // The second attempt should return CONCURRENT_TRANSACTIONS (this is intentional)
    assertEquals(InitProducerIdResult(-1, -1, Errors.CONCURRENT_TRANSACTIONS), result)

    // The transactionMarkerChannelManager mock should have completed the transition to COMPLETE_ABORT
    // Verify that hasFailedEpochFence was never set to true for TV2, allowing future epoch bumps
    assertFalse(txnMetadata.hasFailedEpochFence)

    // Third attempt: Client retries after CONCURRENT_TRANSACTIONS
    reset(transactionManager)
    when(transactionManager.validateTransactionTimeoutMs(anyBoolean(), anyInt()))
      .thenReturn(true)
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
    when(transactionManager.transactionVersionLevel()).thenReturn(TV_2)

    when(transactionManager.appendTransactionToLog(
      ArgumentMatchers.eq(transactionalId),
      ArgumentMatchers.eq(coordinatorEpoch),
      any(),
      any(),
      any(),
      any()
    )).thenAnswer(invocation => {
      val newMetadata = invocation.getArgument[TxnTransitMetadata](2)
      val callback = invocation.getArgument[Errors => Unit](3)
      
      // Complete the transition and call the callback with success
      txnMetadata.completeTransitionTo(newMetadata)
      callback.apply(Errors.NONE)
    })

    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = false,
      keepPreparedTxn = false,
      None,
      initProducerIdMockCallback
    )

    // The third attempt should succeed with epoch 3 (2 + 1)
    // This demonstrates that TV2 allows epoch re-bumping after failed writes
    assertEquals(InitProducerIdResult(producerId, 3.toShort, Errors.NONE), result)
    
    // Final verification that hasFailedEpochFence was never set to true for TV2
    assertFalse(txnMetadata.hasFailedEpochFence)
  }

  @Test
  def shouldIncrementClientEpochOnMultipleInitPidWithKeepPreparedTxn(): Unit = {
    // Test that multiple calls to InitProducerId(keepPreparedTxn=true) increment the client-facing epoch
    // while keeping the ongoing transaction epoch constant.

    // Setup: Use helper to set up 2PC transaction and make first InitProducerId call
    val capturedNewMetadata = setupPrepared2PcTxnWithBumpedClientEpoch()

    // Verify that epoch bumps a few times (the first bump happened during the setup)
    var iteration = 0
    do {
      val expectedClientEpoch = (producerEpoch + 1 + iteration).toShort

      // Verify: each previous iteration must've bumped client epoch
      assertEquals(producerId, result.producerId)
      assertEquals(expectedClientEpoch, result.producerEpoch)
      assertEquals(producerId, result.ongoingTxnProducerId)
      assertEquals(producerEpoch, result.ongoingTxnProducerEpoch)

      // Verify: captured metadata has proper nextProducerEpoch
      val transitMetadata = capturedNewMetadata.getAllValues.get(iteration)
      assertEquals(producerId, transitMetadata.nextProducerId)
      assertEquals(expectedClientEpoch, transitMetadata.nextProducerEpoch)

      coordinator.handleInitProducerId(
        transactionalId,
        txnTimeoutMs,
        enableTwoPCFlag = true,
        keepPreparedTxn = true,
        None,
        initProducerIdMockCallback
      )

      iteration = iteration + 1
    } while (iteration < 4)
  }

  @Test
  def shouldCompletePreparedTransactionWithOngoingTxnEpoch(): Unit = {
    // Test that a prepared transaction can be completed (committed or aborted) using the client-facing
    // epoch after InitProducerId(keepPrepared=true), while markers are sent with the bumped ongoing epoch.

    def testComplete(txnResult: TransactionResult): Unit = {
      val capturedTransitMetadata = setupPrepared2PcTxnWithBumpedClientEpoch()

      // Setup bumped from epoch 1 to epoch 2, now bump again from 2 to 3
      // Action: Call InitProducerId again to bump client epoch once more
      coordinator.handleInitProducerId(
        transactionalId,
        txnTimeoutMs,
        enableTwoPCFlag = true,
        keepPreparedTxn = true,
        None,
        initProducerIdMockCallback
      )

      // Verify: Second InitProducerId bumped client epoch from 2 to 3
      val secondClientEpoch = (producerEpoch + 2).toShort
      assertEquals(producerId, result.producerId)
      assertEquals(secondClientEpoch, result.producerEpoch)
      assertEquals(producerId, result.ongoingTxnProducerId)
      assertEquals(producerEpoch, result.ongoingTxnProducerEpoch)

      // Verify: captured metadata from second InitProducerId has proper nextProducerEpoch
      val secondInitMetadata = capturedTransitMetadata.getAllValues.get(1)
      assertEquals(producerId, secondInitMetadata.nextProducerId)
      assertEquals(secondClientEpoch, secondInitMetadata.nextProducerEpoch)

      val bumpedEpoch = (producerEpoch + 1).toShort  // Epoch bumped during prepare phase
      val tripleBumpedEpoch = (producerEpoch + 3).toShort  // NextProducerEpoch bumped a third time

      val expectedState = if (txnResult == TransactionResult.COMMIT) {
        TransactionState.PREPARE_COMMIT
      } else {
        TransactionState.PREPARE_ABORT
      }

      // Action: Call EndTransaction with second client-facing epoch
      // After second InitProducerId(keepPrepared=true), validation checks against latest clientProducerEpoch
      coordinator.handleEndTransaction(
        transactionalId,
        producerId,
        secondClientEpoch,  // Must use latest client-facing epoch for validation to pass
        txnResult,
        TV_2,
        endTxnCallback
      )

      // Verify: Transaction completes successfully
      assertEquals(Errors.NONE, error)

      // Verify: Transaction transitions to expected state.  Get the third call
      // (index 2) which is the complete, index 0 was first InitProducerId,
      // index 1 was second.
      val transitMetadata = capturedTransitMetadata.getAllValues.get(2)
      assertEquals(expectedState, transitMetadata.txnState)

      // Verify: Markers are sent with bumped epoch.
      // prepareAbortOrCommit bumps producerEpoch for TV2.
      assertEquals(producerId, transitMetadata.producerId)
      assertEquals(bumpedEpoch, transitMetadata.producerEpoch)

      // Verify: After complete, nextProducerEpoch is bumped again
      // (third time total).
      assertEquals(tripleBumpedEpoch, transitMetadata.nextProducerEpoch)
    }

    // Test both commit and abort
    testComplete(TransactionResult.COMMIT)
    testComplete(TransactionResult.ABORT)
  }

  @Test
  def shouldRejectAddPartitionsAfterInitProducerIdWithKeepPreparedTxn(): Unit = {
    // Test that trying to add partitions with the client-facing epoch (after
    // calling InitProducerId with keepPreparedTxn=true) returns
    // INVALID_TXN_STATE.  A properly implemented client should never try this,
    // but the server validates it.

    setupPrepared2PcTxnWithBumpedClientEpoch()  // Return value not used - only need the setup and mocks

    val clientEpoch = (producerEpoch + 1).toShort

    val newPartitions = new util.HashSet[TopicPartition]()
    newPartitions.add(new TopicPartition("topic2", 0))

    // Action: Client tries AddPartitions with the client-facing epoch
    // This is invalid because after InitProducerId(keepPreparedTxn=true), the client should
    // not add partitions.
    coordinator.handleAddPartitionsToTransaction(
      transactionalId,
      producerId,
      clientEpoch,
      newPartitions,
      errorsCallback,
      TV_2
    )

    // Verify: Returns INVALID_TXN_STATE (cannot add partitions in this state)
    assertEquals(Errors.INVALID_TXN_STATE, error)
  }

  @Test
  def shouldRotateProducerIdWhenClientEpochExhausted(): Unit = {
    // Test that when the client epoch reaches Short.MaxValue, a new producer ID is allocated
    // and the client epoch resets to 0, while the ongoing transaction identity remains unchanged.

    val startEpoch = (Short.MaxValue - 2).toShort  // 32765
    mockPidGenerator()

    // Setup: Use helper to create ONGOING transaction at epoch boundary and make first InitProducerId call
    val capturedTransitMetadata = setupPrepared2PcTxnWithBumpedClientEpoch(startEpoch)

    // Intermediate verify: epoch bumped to 32766, same producer ID
    val firstClientEpoch = (Short.MaxValue - 1).toShort
    assertEquals(producerId, result.producerId)
    assertEquals(firstClientEpoch, result.producerEpoch)
    assertEquals(producerId, result.ongoingTxnProducerId)
    assertEquals(startEpoch, result.ongoingTxnProducerEpoch)

    // Verify captured metadata from first call
    val firstMetadata = capturedTransitMetadata.getAllValues.get(0)
    assertEquals(producerId, firstMetadata.nextProducerId)
    assertEquals(firstClientEpoch, firstMetadata.nextProducerEpoch)

    // Second call: exhaust at 32767, should rotate producer ID
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = true,
      keepPreparedTxn = true,
      None,
      initProducerIdMockCallback
    )

    // Verify: new producer ID allocated, epoch reset to 0
    val rotatedProducerId = nextPid - 1  // Most recently allocated PID
    assertTrue(rotatedProducerId != producerId, "Producer ID should have rotated")
    assertEquals(rotatedProducerId, result.producerId)
    assertEquals(0, result.producerEpoch)
    // Ongoing transaction identity unchanged
    assertEquals(producerId, result.ongoingTxnProducerId)
    assertEquals(startEpoch, result.ongoingTxnProducerEpoch)

    // Verify captured metadata from second call shows rotation
    val secondMetadata = capturedTransitMetadata.getAllValues.get(1)
    assertEquals(rotatedProducerId, secondMetadata.nextProducerId)
    assertEquals(0, secondMetadata.nextProducerEpoch)
    // Original ongoing identity unchanged
    assertEquals(producerId, secondMetadata.producerId)
    assertEquals(startEpoch, secondMetadata.producerEpoch)
  }

  @Test
  def shouldHandleMultipleProducerIdRotationsForOngoingTxn(): Unit = {
    // Test that multiple producer ID rotations can occur during a single ongoing transaction,
    // and the ongoing transaction identity remains constant throughout.

    val startEpoch = (Short.MaxValue - 3).toShort  // 32764
    mockPidGenerator()

    // Setup: Use helper to create ONGOING transaction at epoch boundary and make first InitProducerId call
    // Helper will bump: 32764 → 32765, establishing dual identity
    val capturedTransitMetadata = setupPrepared2PcTxnWithBumpedClientEpoch(startEpoch)

    // Track rotations starting from the state after helper's InitProducerId call
    var rotationCount = 0
    val targetRotations = 2
    var currentClientEpoch: Short = result.producerEpoch  // 32765 after helper
    val allocatedProducerIds = scala.collection.mutable.Set[Long](producerId)

    // Loop until we've seen 2 rotations
    while (rotationCount < targetRotations) {
      coordinator.handleInitProducerId(
        transactionalId,
        txnTimeoutMs,
        enableTwoPCFlag = true,
        keepPreparedTxn = true,
        None,
        initProducerIdMockCallback
      )

      // Check if rotation occurred (epoch wrapped to 0)
      if (result.producerEpoch == 0 && currentClientEpoch != 0) {
        rotationCount += 1
        // Verify new producer ID allocated
        assertFalse(allocatedProducerIds.contains(result.producerId),
          s"Producer ID ${result.producerId} should be new on rotation ${rotationCount}")
        allocatedProducerIds.add(result.producerId)
      }

      // Verify ongoing transaction identity unchanged
      assertEquals(producerId, result.ongoingTxnProducerId)
      assertEquals(startEpoch, result.ongoingTxnProducerEpoch)

      currentClientEpoch = result.producerEpoch
    }

    assertEquals(2, rotationCount, "Should have seen exactly 2 rotations")
    assertEquals(3, allocatedProducerIds.size, "Should have 3 unique producer IDs (original + 2 rotations)")

    // Verify final state
    val finalMetadata = capturedTransitMetadata.getAllValues.get(capturedTransitMetadata.getAllValues.size - 1)
    assertEquals(0, finalMetadata.nextProducerEpoch, "Final epoch should be 0 after rotations")
    assertFalse(finalMetadata.nextProducerId == producerId, "Final producer ID should be rotated")
    // Ongoing identity still original
    assertEquals(producerId, finalMetadata.producerId)
    assertEquals(startEpoch, finalMetadata.producerEpoch)
  }

  @Test
  def shouldCompleteTransactionWithExhaustedEpochs(): Unit = {
    // Test that transactions can be committed or aborted successfully even
    // after producer ID rotation due to epoch exhaustion.  This tests the case
    // where overflow happens either during the second InitProducerId call or
    // during endTransaction.
    def testCompleteWithRotation(txnResult: TransactionResult, startEpoch: Short, testProducerId: Long): Unit = {

      // Setup: Use helper to create ONGOING transaction and make first
      // InitProducerId call.  Helper will bump: startEpoch → startEpoch + 1,
      // establishing dual identity.
      val capturedTransitMetadata = setupPrepared2PcTxnWithBumpedClientEpoch(startEpoch, testProducerId)

      // Call InitProducerId again - may rotate producer id (depends on startEpoch)
      coordinator.handleInitProducerId(
        transactionalId,
        txnTimeoutMs,
        enableTwoPCFlag = true,
        keepPreparedTxn = true,
        None,
        initProducerIdMockCallback
      )

      // Store client-facing producerId/epoch from InitProducerId result.
      // Rotation may have happened during InitProducerId or later during
      // EndTransaction.
      val clientProducerId = result.producerId
      val clientProducerEpoch = result.producerEpoch

      // Verify ongoing transaction producerId/epoch unchanged after
      // InitProducerId (regardless of when rotation happens).
      assertEquals(testProducerId, result.ongoingTxnProducerId, "Ongoing ID unchanged")
      assertEquals(startEpoch, result.ongoingTxnProducerEpoch, "Ongoing epoch unchanged")

      val expectedState = if (txnResult == TransactionResult.COMMIT) {
        TransactionState.PREPARE_COMMIT
      } else {
        TransactionState.PREPARE_ABORT
      }

      // Complete the transaction
      // TV2 validates against clientProducerId/clientProducerEpoch returned from InitProducerId
      coordinator.handleEndTransaction(
        transactionalId,
        clientProducerId,  // Use client's producer ID from InitProducerId result
        clientProducerEpoch,  // Use client's epoch from InitProducerId result
        txnResult,
        TV_2,
        endTxnCallback
      )

      // Capture the TxnTransitMetadata that was passed to addTxnMarkersToSend
      // This represents the COMPLETE_* transition metadata created by the internal prepareComplete() call
      val capturedMarkerMetadata: ArgumentCaptor[TxnTransitMetadata] = ArgumentCaptor.forClass(classOf[TxnTransitMetadata])
      verify(transactionMarkerChannelManager).addTxnMarkersToSend(
        ArgumentMatchers.eq(coordinatorEpoch),
        ArgumentMatchers.eq(txnResult),
        any[TransactionMetadata],
        capturedMarkerMetadata.capture()
      )

      // Verify successful completion
      assertEquals(Errors.NONE, error)

      // Verify transition to expected state.
      // Index 0: helper's InitProducerId
      // Index 1: test's InitProducerId (rotation)
      // Index 2: EndTransaction
      val prepareMetadata = capturedTransitMetadata.getAllValues.get(2)
      assertEquals(expectedState, prepareMetadata.txnState)

      // Verify that transaction markers are sent with original (not rotated)
      // producerId and startEpoch + 1.  The ongoing transaction maintains the
      // original producerId/epoch, so markers use those credentials.
      assertEquals(testProducerId, prepareMetadata.producerId,
        "Markers should use original producerId (ongoing transaction)")
      assertEquals((startEpoch + 1).toShort, prepareMetadata.producerEpoch,
        "Markers should use startEpoch + 1 (bumped from ongoing transaction epoch)")

      // Simulate markers being sent and transaction completing to
      // COMPLETE_COMMIT/COMPLETE_ABORT.  At this point, handleEndTransaction
      // has already internally called prepareComplete() in the
      // sendTxnMarkersCallback, so the metadata already has
      // pendingState=COMPLETE_COMMIT/COMPLETE_ABORT.  We just need to write
      // the transition to the log to complete it.

      // Use the TxnTransitMetadata created by the internal prepareComplete()
      val txnTransitMetadata = capturedMarkerMetadata.getValue

      // Append COMPLETE_* transition to log (mock calls completeTransitionTo)
      transactionManager.appendTransactionToLog(
        transactionalId,
        coordinatorEpoch,
        txnTransitMetadata,
        _ => {},  // No-op callback since we're just completing the transition
        _ => false,  // Don't retry on error
        null
      )

      // Verify that rotation happened (regardless of whether it occurred during
      // InitProducerId or EndTransaction). After transaction completes, the final
      // state should have:
      //  - A new producerId (different from the original testProducerId)
      //  - Epoch determined by when rotation occurred (see scenarios below)
      //  - prevProducerId set to the original testProducerId for retry detection
      assertNotEquals(testProducerId, txnTransitMetadata.producerId,
        "Producer ID should have rotated by transaction completion")

      // Verify epoch overflow occurred and total epoch increments.
      // We do 3 total epoch increments in this test:
      //   1. In setupPrepared2PcTxnWithBumpedClientEpoch: startEpoch → startEpoch + 1
      //   2. Second InitProducerId call: startEpoch + 1 → startEpoch + 2 (or rotation)
      //   3. EndTransaction: bumps nextProducerEpoch again (post-rotation or triggers rotation)
      // Since rotation happens when trying to exceed MaxValue - 1 (32766), the final
      // epoch wraps around.

      // Assert 1: Final epoch is less than startEpoch (overflow occurred)
      assertTrue(txnTransitMetadata.producerEpoch < startEpoch,
        s"Overflow should have occurred: finalEpoch=${txnTransitMetadata.producerEpoch} < startEpoch=$startEpoch")

      // Assert 2: Total epoch increments = 3 (accounting for overflow)
      // Formula: finalEpoch + (MaxValue - startEpoch) = total increments
      // This accounts for increments from startEpoch to MaxValue boundary, then
      // wrap to 0 and increments to finalEpoch.
      val totalIncrements = txnTransitMetadata.producerEpoch + Short.MaxValue - startEpoch
      assertEquals(3, totalIncrements,
        s"Should have 3 total epoch increments: finalEpoch=${txnTransitMetadata.producerEpoch} + " +
        s"(MaxValue=${Short.MaxValue} - startEpoch=$startEpoch) = $totalIncrements")

      assertEquals(testProducerId, txnTransitMetadata.prevProducerId,
        "prevProducerId should track original ID for retry detection")

      // Retry EndTransaction with same client credentials - should succeed as a retry
      coordinator.handleEndTransaction(
        transactionalId,
        clientProducerId,  // Use same client producer ID from InitProducerId result
        clientProducerEpoch,  // Use same client epoch from InitProducerId result
        txnResult,
        TV_2,
        endTxnCallback
      )

      // Verify retry succeeds with NONE
      assertEquals(Errors.NONE, error, "Retry with client credentials should succeed")

      // Reset mock to clear call history for next invocation
      reset(transactionMarkerChannelManager)
    }

    // Test both commit and abort with rotation - use different producer IDs
    // to avoid conflicts
    mockPidGenerator()  // Initialize PID generator

    // Scenario 1: Rotation happens during InitProducerId call
    // Example flow:
    //  1. (initial): Ongoing, pid=100, epoch=32765, nextPid=-1, nextEpoch=-1
    //  2. InitProducerId(keepPreparedTxn): Ongoing, pid=100, epoch=32765,
    //     nextPid=100, nextEpoch=32766
    //  3. InitProducerId(keepPreparedTxn): clientEpoch=32766 triggers
    //     isEpochExhausted() → rotation. Ongoing, pid=100, epoch=32765,
    //     nextPid=<new>, nextEpoch=0
    //  4. CommitTxn/AbortTxn: bumps nextEpoch 0→1 to fence delayed requests
    //  5. Complete: CompleteCommit, pid=<new>, epoch=1, prevPid=100
    val startEpoch = (Short.MaxValue - 2).toShort  // 32765
    testCompleteWithRotation(TransactionResult.COMMIT, startEpoch, 100L)
    testCompleteWithRotation(TransactionResult.ABORT, startEpoch, 200L)

    // Scenario 2: Rotation happens during EndTransaction call
    // Example flow:
    //  1. (initial): Ongoing, pid=300, epoch=32764, nextPid=-1, nextEpoch=-1
    //  2. InitProducerId(keepPreparedTxn): Ongoing, pid=300, epoch=32764,
    //     nextPid=300, nextEpoch=32765
    //  3. InitProducerId(keepPreparedTxn): clientEpoch=32765 < 32766
    //     (not exhausted) → just epoch bump. Ongoing, pid=300, epoch=32764,
    //     nextPid=300, nextEpoch=32766
    //  4. CommitTxn/AbortTxn: prepareAbortOrCommit bumps epoch 32764→32765,
    //     then generateTxnTransitMetadataForTxnCompletion sees
    //     isProducerEpochExhausted() → rotation. PrepareCommit, pid=32765,
    //     nextPid=<new>, nextEpoch=0
    //  5. Complete: CompleteCommit, pid=<new>, epoch=0, prevPid=300
    val startEpoch2 = (Short.MaxValue - 3).toShort  // 32764
    testCompleteWithRotation(TransactionResult.COMMIT, startEpoch2, 300L)
    testCompleteWithRotation(TransactionResult.ABORT, startEpoch2, 400L)
  }

  @Test
  def shouldHandleProducerIdRotationPersistence(): Unit = {
    // Test that producer ID rotation metadata is correctly persisted through state transitions.

    val startEpoch = (Short.MaxValue - 2).toShort  // 32765
    mockPidGenerator()

    // Setup: Use helper to create ONGOING transaction and make first InitProducerId call
    // Helper will bump: 32765 → 32766, establishing dual identity
    val capturedTransitMetadata = setupPrepared2PcTxnWithBumpedClientEpoch(startEpoch)

    // Call InitProducerId again - this will exhaust at 32767 and rotate
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = true,
      keepPreparedTxn = true,
      None,
      initProducerIdMockCallback
    )

    // Verify rotation occurred
    val rotatedProducerId = result.producerId
    val rotatedEpoch = result.producerEpoch
    assertNotEquals(producerId, rotatedProducerId, "Producer ID should have rotated")
    assertEquals(0, rotatedEpoch, "Client epoch should reset to 0")

    // Verify: Check captured metadata shows rotation with prevProducerId set
    val rotationMetadata = capturedTransitMetadata.getAllValues.get(1)
    assertEquals(rotatedProducerId, rotationMetadata.nextProducerId,
      "Rotated ID should be stored in nextProducerId")
    assertEquals(0, rotationMetadata.nextProducerEpoch,
      "Rotated epoch should be 0")
    assertEquals(producerId, rotationMetadata.prevProducerId,
      "prevProducerId should be set to original ID for retry detection")

    // Verify ongoing identity still uses original producer ID
    assertEquals(producerId, rotationMetadata.producerId, "Ongoing transaction producerId unchanged")
    assertEquals(startEpoch, rotationMetadata.producerEpoch, "Ongoing transaction epoch unchanged")

    // Complete the transaction (commit)
    coordinator.handleEndTransaction(
      transactionalId,
      rotatedProducerId,  // Use client's producer ID (rotated)
      rotatedEpoch,  // Use client's epoch (0 after rotation)
      TransactionResult.COMMIT,
      TV_2,
      endTxnCallback
    )

    assertEquals(Errors.NONE, error)

    // Verify transition to PREPARE_COMMIT
    val prepareCommitMetadata = capturedTransitMetadata.getAllValues.get(2)
    assertEquals(TransactionState.PREPARE_COMMIT, prepareCommitMetadata.txnState)

    // Verify: The key aspect of persistence is that prevProducerId was
    // correctly set during rotation.  This ensures that after the transaction
    // completes to EMPTY, retry detection will work correctly by recognizing
    // requests from the old producer ID.
    assertEquals(producerId, rotationMetadata.prevProducerId,
      "prevProducerId persisted for retry detection")

    // Verify: The rotation information is persisted in the metadata.  After
    // this transaction completes, the next transaction will start with
    // rotatedProducerId.
    assertEquals(rotatedProducerId, rotationMetadata.nextProducerId,
      "Rotated ID persisted in nextProducerId")
    assertEquals(0, rotationMetadata.nextProducerEpoch,
      "Rotated epoch persisted as 0")

    // Verify: The state transitions are captured correctly in metadata
    assertEquals(3, capturedTransitMetadata.getAllValues.size(),
      "Three state transitions captured")
    assertEquals(TransactionState.ONGOING,
      capturedTransitMetadata.getAllValues.get(0).txnState,
      "First: InitProducerId → ONGOING")
    assertEquals(TransactionState.ONGOING, rotationMetadata.txnState,
      "Second: InitProducerId with rotation → ONGOING")
    assertEquals(TransactionState.PREPARE_COMMIT, prepareCommitMetadata.txnState,
      "Third: EndTransaction → PREPARE_COMMIT")
  }

  @Test
  def shouldHandleConcurrentTransactionsInPrepareStatesTV2(): Unit = {
    // Test that attempting EndTransaction while already in a PREPARE state
    // returns CONCURRENT_TRANSACTIONS error in TV2.  This error is retriable -
    // the client should wait for the current transaction to complete and then
    // retry.

    def testConcurrentInPrepareState(prepareState: TransactionState, attemptedResult: TransactionResult): Unit = {
      // Setup ONGOING transaction with 2PC enabled
      val capturedTransitMetadata = setupPrepared2PcTxnWithBumpedClientEpoch()

      // Determine transaction result to reach desired PREPARE state
      val txnResult = if (prepareState == TransactionState.PREPARE_COMMIT) {
        TransactionResult.COMMIT
      } else {
        TransactionResult.ABORT
      }

      // Call EndTransaction to move to PREPARE state
      coordinator.handleEndTransaction(
        transactionalId,
        result.producerId,  // Use client producer ID
        result.producerEpoch,  // Use client epoch
        txnResult,
        TV_2,
        endTxnCallback
      )

      // Verify we're in the expected PREPARE state
      assertEquals(Errors.NONE, error, "EndTransaction should succeed")
      val transitMetadata = capturedTransitMetadata.getAllValues.get(1)
      assertEquals(prepareState, transitMetadata.txnState,
        s"Transaction should be in $prepareState state")

      val stateBefore = transitMetadata.txnState

      // Attempt another EndTransaction while still in PREPARE state
      coordinator.handleEndTransaction(
        transactionalId,
        result.producerId,  // Same client producer ID
        result.producerEpoch,  // Same client epoch
        attemptedResult,
        TV_2,
        endTxnCallback
      )

      // Verify: TV2 returns CONCURRENT_TRANSACTIONS (retriable error)
      assertEquals(Errors.CONCURRENT_TRANSACTIONS, error,
        s"Should return CONCURRENT_TRANSACTIONS when attempting $attemptedResult while in $prepareState")

      // Verify: State unchanged after rejection
      assertEquals(stateBefore, capturedTransitMetadata.getAllValues.get(1).txnState,
        "State should remain unchanged after CONCURRENT_TRANSACTIONS error")

      // Verify: No additional state transitions captured (request was rejected)
      assertEquals(2, capturedTransitMetadata.getAllValues.size(),
        "No new state transition should be captured for rejected request")
    }

    // Test Matrix: All 4 combinations of PREPARE state × attempted operation.
    // All should return CONCURRENT_TRANSACTIONS regardless of operation type.
    testConcurrentInPrepareState(TransactionState.PREPARE_COMMIT, TransactionResult.COMMIT)
    testConcurrentInPrepareState(TransactionState.PREPARE_COMMIT, TransactionResult.ABORT)
    testConcurrentInPrepareState(TransactionState.PREPARE_ABORT, TransactionResult.ABORT)
    testConcurrentInPrepareState(TransactionState.PREPARE_ABORT, TransactionResult.COMMIT)
  }

  @Test
  def shouldRejectKeepPreparedTxnWithExpectedEpoch(): Unit = {
    // Test that keepPreparedTxn=true with expectedEpoch returns INVALID_REQUEST.
    // This is defense-in-depth validation: properly implemented clients should
    // never send both flags together, as keepPreparedTxn is for initial calls
    // and expectedEpoch is for retries.

    // Setup EMPTY transaction and enable 2PC
    when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
      .thenReturn(Right(None))
    when(transactionManager.isTransaction2pcEnabled())
      .thenReturn(true)

    // Attempt InitProducerId with both keepPreparedTxn and expectedEpoch
    coordinator.handleInitProducerId(
      transactionalId,
      txnTimeoutMs,
      enableTwoPCFlag = true,
      keepPreparedTxn = true,
      expectedProducerIdAndEpoch = Some(new ProducerIdAndEpoch(producerId, producerEpoch)),
      initProducerIdMockCallback
    )

    // Verify: Returns INVALID_REQUEST
    assertEquals(Errors.INVALID_REQUEST, result.error,
      "Should return INVALID_REQUEST when both keepPreparedTxn and expectedEpoch are set")
  }

  @Test
  def shouldRejectKeepPreparedTxnWithoutOngoingTransaction(): Unit = {
    // Test that keepPreparedTxn=true without an ONGOING transaction returns an
    // appropriate error.  Properly implemented clients call keepPreparedTxn only
    // after a crash with a transaction in flight, but server validates this.

    def testKeepPreparedInState(state: TransactionState): Unit = {
      val txnMetadata = new TransactionMetadata(
        transactionalId,
        producerId,
        RecordBatch.NO_PRODUCER_ID,
        RecordBatch.NO_PRODUCER_ID,
        producerEpoch,
        RecordBatch.NO_PRODUCER_EPOCH,
        RecordBatch.NO_PRODUCER_EPOCH,
        txnTimeoutMs,
        state,
        new util.HashSet[TopicPartition](),
        time.milliseconds(),
        time.milliseconds(),
        TV_0
      )

      when(transactionManager.getTransactionState(ArgumentMatchers.eq(transactionalId)))
        .thenReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))

      // Attempt InitProducerId with keepPreparedTxn in non-ONGOING state
      coordinator.handleInitProducerId(
        transactionalId,
        txnTimeoutMs,
        enableTwoPCFlag = true,
        keepPreparedTxn = true,
        None,
        initProducerIdMockCallback
      )

      // Verify: Returns error (no ONGOING transaction to prepare)
      assertNotEquals(Errors.NONE, result.error,
        s"Should return error when keepPreparedTxn=true in $state state")
    }

    // Test keepPreparedTxn in states where there's no ongoing transaction
    testKeepPreparedInState(TransactionState.EMPTY)
    testKeepPreparedInState(TransactionState.COMPLETE_COMMIT)
    testKeepPreparedInState(TransactionState.COMPLETE_ABORT)
  }

  @Test
  def shouldValidateEpochsCorrectlyWithDualIdentity(): Unit = {
    // Test comprehensive epoch validation when dual identity is present
    // (ongoing transaction with client-facing epoch bumped).  Validates that
    // operations use the correct epoch: ongoing epoch for AddPartitions,
    // client epoch for EndTransaction.

    // Setup ONGOING transaction with dual identity:
    // - Ongoing: pid=10, epoch=1
    // - Client: nextPid=10, nextEpoch=2
    val capturedTransitMetadata = setupPrepared2PcTxnWithBumpedClientEpoch()
    val ongoingEpoch = producerEpoch  // 1
    val clientEpoch = result.producerEpoch  // 2

    // Scenario 1: AddPartitions with ongoing epoch (1) → Success
    // This is normal operation: adding partitions to the ongoing transaction.
    coordinator.handleAddPartitionsToTransaction(
      transactionalId,
      producerId,
      ongoingEpoch,  // Use ongoing epoch
      Set(new TopicPartition("topic1", 0)).asJava,
      errorsCallback,
      TV_2
    )
    assertEquals(Errors.NONE, error,
      "AddPartitions with ongoing epoch should succeed")

    // Scenario 2: AddPartitions with client epoch (2) → INVALID_TXN_STATE
    // This is already tested in shouldRejectAddPartitionsAfterInitProducerIdWithKeepPreparedTxn
    // but included here for completeness of the validation matrix.
    coordinator.handleAddPartitionsToTransaction(
      transactionalId,
      producerId,
      clientEpoch,  // Use client epoch (wrong for AddPartitions)
      Set(new TopicPartition("topic2", 0)).asJava,
      errorsCallback,
      TV_2
    )
    assertEquals(Errors.INVALID_TXN_STATE, error,
      "AddPartitions with client epoch should return INVALID_TXN_STATE")

    // Scenario 3: AddPartitions with future epoch (3) → PRODUCER_FENCED
    val futureEpoch = (clientEpoch + 1).toShort
    coordinator.handleAddPartitionsToTransaction(
      transactionalId,
      producerId,
      futureEpoch,  // Unknown future epoch
      Set(new TopicPartition("topic3", 0)).asJava,
      errorsCallback,
      TV_2
    )
    assertEquals(Errors.PRODUCER_FENCED, error,
      "AddPartitions with future epoch should return PRODUCER_FENCED")

    // Scenario 4: EndTransaction with ongoing epoch (1) → PRODUCER_FENCED (TV2)
    // Client should use client epoch for EndTransaction, not ongoing epoch.
    coordinator.handleEndTransaction(
      transactionalId,
      producerId,
      ongoingEpoch,  // Wrong epoch for EndTransaction in TV2
      TransactionResult.COMMIT,
      TV_2,
      endTxnCallback
    )
    assertEquals(Errors.PRODUCER_FENCED, error,
      "EndTransaction with ongoing epoch should return PRODUCER_FENCED in TV2")

    // Scenario 5: EndTransaction with client epoch (2) → Success
    // This is the correct 2PC completion path.
    coordinator.handleEndTransaction(
      transactionalId,
      producerId,
      clientEpoch,  // Correct client epoch
      TransactionResult.COMMIT,
      TV_2,
      endTxnCallback
    )
    assertEquals(Errors.NONE, error,
      "EndTransaction with client epoch should succeed")

    // Verify state transitioned to PREPARE_COMMIT
    // Index 0: InitProducerId transition from setup
    // Index 1: EndTransaction PREPARE_COMMIT transition
    val finalMetadata = capturedTransitMetadata.getAllValues.get(1)
    assertEquals(TransactionState.PREPARE_COMMIT, finalMetadata.txnState,
      "Transaction should transition to PREPARE_COMMIT")
  }
}
