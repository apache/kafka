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

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import javax.management.ObjectName
import kafka.server.ReplicaManager
import kafka.utils.TestUtils
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.InvalidRegularExpression
import org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME
import org.apache.kafka.common.metrics.{JmxReporter, KafkaMetricsContext, Metrics}
import org.apache.kafka.common.protocol.{Errors, MessageUtil}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.coordinator.transaction.{TransactionLog, TransactionMetadata, TransactionState, TxnTransitMetadata}
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.common.{FinalizedFeatures, MetadataVersion, RequestLocal, TransactionVersion}
import org.apache.kafka.server.common.TransactionVersion.{TV_0, TV_2}
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey
import org.apache.kafka.server.config.ServerLogConfigs
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.MockScheduler
import org.apache.kafka.storage.internals.log.{AppendOrigin, FetchDataInfo, LogConfig, LogOffsetMetadata, UnifiedLog}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong, anyShort}
import org.mockito.Mockito.{atLeastOnce, mock, reset, times, verify, when}

import java.util
import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._

class TransactionStateManagerTest {

  val partitionId = 0
  val numPartitions = 2
  val transactionTimeoutMs: Int = 1000
  val topicPartition = new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, partitionId)
  val transactionTopicId = Uuid.randomUuid()
  val coordinatorEpoch = 10

  val txnRecords: mutable.ArrayBuffer[SimpleRecord] = mutable.ArrayBuffer[SimpleRecord]()

  val time = new MockTime()
  val scheduler = new MockScheduler(time)
  val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  val metadataCache: MetadataCache = mock(classOf[MetadataCache])

  when(metadataCache.features()).thenReturn {
    new FinalizedFeatures(
      MetadataVersion.latestTesting(),
      util.Map.of(TransactionVersion.FEATURE_NAME, TransactionVersion.TV_2.featureLevel()),
      0)
  }
  
  val metrics = new Metrics()

  val txnConfig = TransactionConfig()
  val transactionManager: TransactionStateManager = new TransactionStateManager(0, scheduler,
    replicaManager, metadataCache, txnConfig, time, metrics)

  val transactionalId1: String = "one"
  val transactionalId2: String = "two"
  val txnMessageKeyBytes1: Array[Byte] = TransactionLog.keyToBytes(transactionalId1)
  val txnMessageKeyBytes2: Array[Byte] = TransactionLog.keyToBytes(transactionalId2)
  val producerIds: Map[String, Long] = Map[String, Long](transactionalId1 -> 1L, transactionalId2 -> 2L)
  var txnMetadata1: TransactionMetadata = transactionMetadata(transactionalId1, producerIds(transactionalId1))
  var txnMetadata2: TransactionMetadata = transactionMetadata(transactionalId2, producerIds(transactionalId2))

  var expectedError: Errors = Errors.NONE

  @BeforeEach
  def setUp(): Unit = {
    transactionManager.startup(() => numPartitions, enableTransactionalIdExpiration = false)
    // make sure the transactional id hashes to the assigning partition id
    assertEquals(partitionId, transactionManager.partitionFor(transactionalId1))
    assertEquals(partitionId, transactionManager.partitionFor(transactionalId2))
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 0))).thenReturn(new TopicIdPartition(transactionTopicId, 0, TRANSACTION_STATE_TOPIC_NAME))
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 1))).thenReturn(new TopicIdPartition(transactionTopicId, 1, TRANSACTION_STATE_TOPIC_NAME))
  }

  @AfterEach
  def tearDown(): Unit = {
    transactionManager.shutdown()
  }

  @Test
  def testValidateTransactionTimeout(): Unit = {
    assertTrue(transactionManager.validateTransactionTimeoutMs(enableTwoPC = false, 1))
    assertFalse(transactionManager.validateTransactionTimeoutMs(enableTwoPC = false, -1))
    assertFalse(transactionManager.validateTransactionTimeoutMs(enableTwoPC = false, 0))
    assertTrue(transactionManager.validateTransactionTimeoutMs(enableTwoPC = false, txnConfig.transactionMaxTimeoutMs))
    assertFalse(transactionManager.validateTransactionTimeoutMs(enableTwoPC = false, txnConfig.transactionMaxTimeoutMs + 1))
    // KIP-939 Always return true when two phase commit is enabled on transaction. Two phase commit is enabled in case of
    // externally coordinated distributed transactions.
    assertTrue(transactionManager.validateTransactionTimeoutMs(enableTwoPC = true, -1))
    assertTrue(transactionManager.validateTransactionTimeoutMs(enableTwoPC = true, 10))
    assertTrue(transactionManager.validateTransactionTimeoutMs(enableTwoPC = true, txnConfig.transactionMaxTimeoutMs + 1))
  }

  @Test
  def testAddGetPids(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new ConcurrentHashMap[String, TransactionMetadata]())

    assertEquals(Right(None), transactionManager.getTransactionState(transactionalId1))
    assertEquals(Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1)),
      transactionManager.putTransactionStateIfNotExists(txnMetadata1))
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))),
      transactionManager.getTransactionState(transactionalId1))
    assertEquals(Right(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata2)),
      transactionManager.putTransactionStateIfNotExists(txnMetadata2))
  }

  @Test
  def testDeletePartition(): Unit = {
    val metadata1 = transactionMetadata("b", 5L)
    val metadata2 = transactionMetadata("a", 10L)

    assertEquals(0, transactionManager.partitionFor(metadata1.transactionalId))
    assertEquals(1, transactionManager.partitionFor(metadata2.transactionalId))

    transactionManager.addLoadedTransactionsToCache(0, coordinatorEpoch, new ConcurrentHashMap[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(metadata1)

    transactionManager.addLoadedTransactionsToCache(1, coordinatorEpoch, new ConcurrentHashMap[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(metadata2)

    def cachedProducerEpoch(transactionalId: String): Option[Short] = {
      transactionManager.getTransactionState(transactionalId).toOption.flatten
        .map(_.transactionMetadata.producerEpoch)
    }

    assertEquals(Some(metadata1.producerEpoch), cachedProducerEpoch(metadata1.transactionalId))
    assertEquals(Some(metadata2.producerEpoch), cachedProducerEpoch(metadata2.transactionalId))

    transactionManager.removeTransactionsForTxnTopicPartition(0)

    assertEquals(None, cachedProducerEpoch(metadata1.transactionalId))
    assertEquals(Some(metadata2.producerEpoch), cachedProducerEpoch(metadata2.transactionalId))
  }

  @Test
  def testDeleteLoadingPartition(): Unit = {
    // Verify the handling of a call to delete state for a partition while it is in the
    // process of being loaded. Basically should be treated as a no-op.

    val startOffset = 0L
    val endOffset = 1L

    val fileRecordsMock = mock[FileRecords](classOf[FileRecords])
    val logMock = mock[UnifiedLog](classOf[UnifiedLog])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(logMock))
    when(logMock.logStartOffset).thenReturn(startOffset)
    when(logMock.read(ArgumentMatchers.eq(startOffset),
      anyInt(),
      ArgumentMatchers.eq(FetchIsolation.LOG_END),
      ArgumentMatchers.eq(true))
    ).thenReturn(new FetchDataInfo(new LogOffsetMetadata(startOffset), fileRecordsMock))
    when(replicaManager.getLogEndOffset(topicPartition)).thenReturn(Some(endOffset))

    txnMetadata1.state(TransactionState.PREPARE_COMMIT)
    txnMetadata1.addPartitions(util.Set.of(
      new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit(), TV_2)))

    // We create a latch which is awaited while the log is loading. This ensures that the deletion
    // is triggered before the loading returns
    val latch = new CountDownLatch(1)

    when(fileRecordsMock.sizeInBytes()).thenReturn(records.sizeInBytes)
    val bufferCapture: ArgumentCaptor[ByteBuffer] = ArgumentCaptor.forClass(classOf[ByteBuffer])
    when(fileRecordsMock.readInto(bufferCapture.capture(), anyInt())).thenAnswer(_ => {
      latch.await()
      val buffer = bufferCapture.getValue
      buffer.put(records.buffer.duplicate)
      buffer.flip()
    })

    val coordinatorEpoch = 0
    val partitionAndLeaderEpoch = TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch)

    val loadingThread = new Thread(() => {
      transactionManager.loadTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch, (_, _, _, _) => ())
    })
    loadingThread.start()
    TestUtils.waitUntilTrue(() => transactionManager.loadingPartitions.contains(partitionAndLeaderEpoch),
      "Timed out waiting for loading partition", pause = 10)

    transactionManager.removeTransactionsForTxnTopicPartition(partitionId)
    assertFalse(transactionManager.loadingPartitions.contains(partitionAndLeaderEpoch))

    latch.countDown()
    loadingThread.join()

    // Verify that transaction state was not loaded
    assertEquals(Left(Errors.NOT_COORDINATOR), transactionManager.getTransactionState(txnMetadata1.transactionalId))
  }

  @Test
  def testMakeFollowerLoadingPartition(): Unit = {
    // Verify the handling of a call to make a partition a follower while it is in the
    // process of being loaded. The partition should not be loaded.

    val startOffset = 0L
    val endOffset = 1L

    val fileRecordsMock = mock[FileRecords](classOf[FileRecords])
    val logMock = mock[UnifiedLog](classOf[UnifiedLog])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(logMock))
    when(logMock.logStartOffset).thenReturn(startOffset)
    when(logMock.read(ArgumentMatchers.eq(startOffset),
      anyInt(),
      ArgumentMatchers.eq(FetchIsolation.LOG_END),
      ArgumentMatchers.eq(true))
    ).thenReturn(new FetchDataInfo(new LogOffsetMetadata(startOffset), fileRecordsMock))
    when(replicaManager.getLogEndOffset(topicPartition)).thenReturn(Some(endOffset))

    txnMetadata1.state(TransactionState.PREPARE_COMMIT)
    txnMetadata1.addPartitions(util.Set.of(
      new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit(), TV_2)))

    // We create a latch which is awaited while the log is loading. This ensures that the follower transition
    // is triggered before the loading returns
    val latch = new CountDownLatch(1)

    when(fileRecordsMock.sizeInBytes()).thenReturn(records.sizeInBytes)
    val bufferCapture: ArgumentCaptor[ByteBuffer] = ArgumentCaptor.forClass(classOf[ByteBuffer])
    when(fileRecordsMock.readInto(bufferCapture.capture(), anyInt())).thenAnswer(_ => {
      latch.await()
      val buffer = bufferCapture.getValue
      buffer.put(records.buffer.duplicate)
      buffer.flip()
    })

    val coordinatorEpoch = 0
    val partitionAndLeaderEpoch = TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch)

    val loadingThread = new Thread(() => {
      transactionManager.loadTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch, (_, _, _, _) => ())
    })
    loadingThread.start()
    TestUtils.waitUntilTrue(() => transactionManager.loadingPartitions.contains(partitionAndLeaderEpoch),
      "Timed out waiting for loading partition", pause = 10)

    transactionManager.removeTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch + 1)
    assertFalse(transactionManager.loadingPartitions.contains(partitionAndLeaderEpoch))

    latch.countDown()
    loadingThread.join()

    // Verify that transaction state was not loaded
    assertEquals(Left(Errors.NOT_COORDINATOR), transactionManager.getTransactionState(txnMetadata1.transactionalId))
  }

  @Test
  def testLoadAndRemoveTransactionsForPartition(): Unit = {
    // generate transaction log messages for two pids traces:

    // pid1's transaction started with two partitions
    txnMetadata1.state(TransactionState.ONGOING)
    txnMetadata1.addPartitions(util.Set.of(new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit(), TV_2))

    // pid1's transaction adds three more partitions
    txnMetadata1.addPartitions(util.Set.of(new TopicPartition("topic2", 0),
      new TopicPartition("topic2", 1),
      new TopicPartition("topic2", 2)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit(), TV_2))

    // pid1's transaction is preparing to commit
    txnMetadata1.state(TransactionState.PREPARE_COMMIT)

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit(), TV_2))

    // pid2's transaction started with three partitions
    txnMetadata2.state(TransactionState.ONGOING)
    txnMetadata2.addPartitions(util.Set.of(new TopicPartition("topic3", 0),
      new TopicPartition("topic3", 1),
      new TopicPartition("topic3", 2)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2.prepareNoTransit(), TV_2))

    // pid2's transaction is preparing to abort
    txnMetadata2.state(TransactionState.PREPARE_ABORT)

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2.prepareNoTransit(), TV_2))

    // pid2's transaction has aborted
    txnMetadata2.state(TransactionState.COMPLETE_ABORT)

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2.prepareNoTransit(), TV_2))

    // pid2's epoch has advanced, with no ongoing transaction yet
    txnMetadata2.state(TransactionState.EMPTY)
    txnMetadata2.topicPartitions.clear()

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2.prepareNoTransit(), TV_2))

    val startOffset = 15L   // it should work for any start offset
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE, txnRecords.toArray: _*)

    prepareTxnLog(topicPartition, startOffset, records)

    // this partition should not be part of the owned partitions
    transactionManager.getTransactionState(transactionalId1).fold(
      err => assertEquals(Errors.NOT_COORDINATOR, err),
      _ => fail(transactionalId1 + "'s transaction state is already in the cache")
    )
    transactionManager.getTransactionState(transactionalId2).fold(
      err => assertEquals(Errors.NOT_COORDINATOR, err),
      _ => fail(transactionalId2 + "'s transaction state is already in the cache")
    )

    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, 0, (_, _, _, _) => ())

    // let the time advance to trigger the background thread loading
    scheduler.tick()

    transactionManager.getTransactionState(transactionalId1).fold(
      err => fail(transactionalId1 + "'s transaction state access returns error " + err),
      entry => entry.getOrElse(fail(transactionalId1 + "'s transaction state was not loaded into the cache"))
    )

    val cachedPidMetadata1 = transactionManager.getTransactionState(transactionalId1).fold(
      err => throw new AssertionError(transactionalId1 + "'s transaction state access returns error " + err),
      entry => entry.getOrElse(throw new AssertionError(transactionalId1 + "'s transaction state was not loaded into the cache"))
    )
    val cachedPidMetadata2 = transactionManager.getTransactionState(transactionalId2).fold(
      err => throw new AssertionError(transactionalId2 + "'s transaction state access returns error " + err),
      entry => entry.getOrElse(throw new AssertionError(transactionalId2 + "'s transaction state was not loaded into the cache"))
    )

    // they should be equal to the latest status of the transaction
    assertEquals(txnMetadata1, cachedPidMetadata1.transactionMetadata)
    assertEquals(txnMetadata2, cachedPidMetadata2.transactionMetadata)

    transactionManager.removeTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch)

    // let the time advance to trigger the background thread removing
    scheduler.tick()

    transactionManager.getTransactionState(transactionalId1).fold(
      err => assertEquals(Errors.NOT_COORDINATOR, err),
      _ => fail(transactionalId1 + "'s transaction state is still in the cache")
    )
    transactionManager.getTransactionState(transactionalId2).fold(
      err => assertEquals(Errors.NOT_COORDINATOR, err),
      _ => fail(transactionalId2 + "'s transaction state is still in the cache")
    )
  }

  @Test
  def testCompleteTransitionWhenAppendSucceeded(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new ConcurrentHashMap[String, TransactionMetadata]())

    // first insert the initial transaction metadata
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    prepareForTxnMessageAppend(Errors.NONE)
    expectedError = Errors.NONE

    // update the metadata to ongoing with two partitions
    val newMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)), time.milliseconds(), TV_0)

    // append the new metadata into log
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch, newMetadata, assertCallback, requestLocal = RequestLocal.withThreadConfinedCaching)

    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)
  }

  @Test
  def testAppendFailToCoordinatorNotAvailableError(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new ConcurrentHashMap[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    expectedError = Errors.COORDINATOR_NOT_AVAILABLE
    var failedMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic2", 0)), time.milliseconds(), TV_0)

    prepareForTxnMessageAppend(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    val requestLocal = RequestLocal.withThreadConfinedCaching
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, requestLocal = requestLocal)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)

    failedMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic2", 0)), time.milliseconds(), TV_0)
    prepareForTxnMessageAppend(Errors.NOT_ENOUGH_REPLICAS)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, requestLocal = requestLocal)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)

    failedMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic2", 0)), time.milliseconds(), TV_0)
    prepareForTxnMessageAppend(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, requestLocal = requestLocal)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)

    failedMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic2", 0)), time.milliseconds(), TV_0)
    prepareForTxnMessageAppend(Errors.REQUEST_TIMED_OUT)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, requestLocal = requestLocal)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)
  }

  @Test
  def testAppendFailToNotCoordinatorError(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new ConcurrentHashMap[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    expectedError = Errors.NOT_COORDINATOR
    var failedMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic2", 0)), time.milliseconds(), TV_0)

    prepareForTxnMessageAppend(Errors.NOT_LEADER_OR_FOLLOWER)
    val requestLocal = RequestLocal.withThreadConfinedCaching
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, requestLocal = requestLocal)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)

    failedMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic2", 0)), time.milliseconds(), TV_0)
    prepareForTxnMessageAppend(Errors.NONE)
    transactionManager.removeTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, requestLocal = requestLocal)

    prepareForTxnMessageAppend(Errors.NONE)
    transactionManager.removeTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch)
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch + 1, new ConcurrentHashMap[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, requestLocal = requestLocal)

    prepareForTxnMessageAppend(Errors.NONE)
    transactionManager.removeTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch)
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new ConcurrentHashMap[String, TransactionMetadata]())
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, requestLocal = requestLocal)
  }

  @Test
  def testAppendFailToCoordinatorLoadingError(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new ConcurrentHashMap[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    expectedError = Errors.COORDINATOR_LOAD_IN_PROGRESS
    val failedMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic2", 0)), time.milliseconds(), TV_0)

    prepareForTxnMessageAppend(Errors.NONE)
    transactionManager.removeTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch)
    transactionManager.addLoadingPartition(partitionId, coordinatorEpoch + 1)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback,  requestLocal = RequestLocal.withThreadConfinedCaching)
  }

  @Test
  def testAppendFailToUnknownError(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new ConcurrentHashMap[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    expectedError = Errors.UNKNOWN_SERVER_ERROR
    var failedMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic2", 0)), time.milliseconds(), TV_0)

    prepareForTxnMessageAppend(Errors.MESSAGE_TOO_LARGE)
    val requestLocal = RequestLocal.withThreadConfinedCaching
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, requestLocal = requestLocal)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)

    failedMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic2", 0)), time.milliseconds(), TV_0)
    prepareForTxnMessageAppend(Errors.RECORD_LIST_TOO_LARGE)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, requestLocal = requestLocal)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)
  }

  @Test
  def testPendingStateNotResetOnRetryAppend(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new ConcurrentHashMap[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    expectedError = Errors.COORDINATOR_NOT_AVAILABLE
    val failedMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic2", 0)), time.milliseconds(), TV_0)

    prepareForTxnMessageAppend(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, _ => true, RequestLocal.withThreadConfinedCaching)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertEquals(util.Optional.of(TransactionState.ONGOING), txnMetadata1.pendingState)
  }

  @Test
  def testAppendTransactionToLogWhileProducerFenced(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, 0, new ConcurrentHashMap[String, TransactionMetadata]())

    // first insert the initial transaction metadata
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    prepareForTxnMessageAppend(Errors.NONE)
    expectedError = Errors.NOT_COORDINATOR

    val newMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)), time.milliseconds(), TV_0)

    // modify the cache while trying to append the new metadata
    txnMetadata1.setProducerEpoch((txnMetadata1.producerEpoch + 1).toShort)

    // append the new metadata into log
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, newMetadata, assertCallback,  requestLocal = RequestLocal.withThreadConfinedCaching)
  }

  @Test
  def testAppendTransactionToLogWhilePendingStateChanged(): Unit = {
    // first insert the initial transaction metadata
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new ConcurrentHashMap[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    prepareForTxnMessageAppend(Errors.NONE)
    expectedError = Errors.INVALID_PRODUCER_EPOCH

    val newMetadata = txnMetadata1.prepareAddPartitions(util.Set.of(new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)), time.milliseconds(), TV_0)

    // modify the cache while trying to append the new metadata
    txnMetadata1.pendingState(util.Optional.empty())

    // append the new metadata into log
    assertThrows(classOf[IllegalStateException], () => transactionManager.appendTransactionToLog(transactionalId1,
      coordinatorEpoch = 10, newMetadata, assertCallback,  requestLocal = RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldReturnNotCoordinatorErrorIfTransactionIdPartitionNotOwned(): Unit = {
    transactionManager.getTransactionState(transactionalId1).fold(
      err => assertEquals(Errors.NOT_COORDINATOR, err),
      _ => fail(transactionalId1 + "'s transaction state is already in the cache")
    )
  }

  @Test
  def testListTransactionsWithCoordinatorLoadingInProgress(): Unit = {
    transactionManager.addLoadingPartition(partitionId = 0, coordinatorEpoch = 15)
    val listResponse = transactionManager.listTransactionStates(
      filterProducerIds = Set.empty,
      filterStateNames = Set.empty,
      -1L,
      null
    )
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, Errors.forCode(listResponse.errorCode))
  }

  @Test
  def testListTransactionsFiltering(): Unit = {
    for (partitionId <- 0 until numPartitions) {
      transactionManager.addLoadedTransactionsToCache(partitionId, 0, new ConcurrentHashMap[String, TransactionMetadata]())
    }

    def putTransaction(
      transactionalId: String,
      producerId: Long,
      state: TransactionState
    ): Unit = {
      val txnMetadata = transactionMetadata(transactionalId, producerId, state)
      transactionManager.putTransactionStateIfNotExists(txnMetadata).left.toOption.foreach { error =>
        fail(s"Failed to insert transaction $txnMetadata due to error $error")
      }
    }

    putTransaction(transactionalId = "t0", producerId = 0, state = TransactionState.ONGOING)
    putTransaction(transactionalId = "t1", producerId = 1, state = TransactionState.ONGOING)
    putTransaction(transactionalId = "my-special-0", producerId = 0, state = TransactionState.ONGOING)
    // update time to create transactions with various durations
    time.sleep(1000)
    putTransaction(transactionalId = "t2", producerId = 2, state = TransactionState.PREPARE_COMMIT)
    putTransaction(transactionalId = "t3", producerId = 3, state = TransactionState.PREPARE_ABORT)
    putTransaction(transactionalId = "your-special-1", producerId = 0, state = TransactionState.PREPARE_ABORT)
    time.sleep(1000)
    putTransaction(transactionalId = "t4", producerId = 4, state = TransactionState.COMPLETE_COMMIT)
    putTransaction(transactionalId = "t5", producerId = 5, state = TransactionState.COMPLETE_ABORT)
    putTransaction(transactionalId = "t6", producerId = 6, state = TransactionState.COMPLETE_ABORT)
    putTransaction(transactionalId = "t7", producerId = 7, state = TransactionState.PREPARE_EPOCH_FENCE)
    putTransaction(transactionalId = "their-special-2", producerId = 7, state = TransactionState.COMPLETE_ABORT)
    time.sleep(1000)
    // Note that `TransactionState.DEAD` transactions are never returned. This is a transient state
    // which is used when the transaction state is in the process of being deleted
    // (whether though expiration or coordinator unloading).
    putTransaction(transactionalId = "t8", producerId = 8, state = TransactionState.DEAD)

    def assertListTransactions(
      expectedTransactionalIds: Set[String],
      filterProducerIds: Set[Long] = Set.empty,
      filterStates: Set[String] = Set.empty,
      filterDuration: Long = -1L,
      filteredTransactionalIdPattern: String = null
    ): Unit = {
      val listResponse = transactionManager.listTransactionStates(filterProducerIds, filterStates, filterDuration, filteredTransactionalIdPattern)
      assertEquals(Errors.NONE, Errors.forCode(listResponse.errorCode))
      assertEquals(expectedTransactionalIds, listResponse.transactionStates.asScala.map(_.transactionalId).toSet)
      val expectedUnknownStates = filterStates.filter(state => TransactionState.fromName(state).isEmpty)
      assertEquals(expectedUnknownStates, listResponse.unknownStateFilters.asScala.toSet)
    }
    assertListTransactions(Set("t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "my-special-0", "your-special-1", "their-special-2"))
    assertListTransactions(Set("t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "my-special-0", "your-special-1", "their-special-2"), filterDuration = 0L)
    assertListTransactions(Set("t0", "t1", "t2", "t3", "my-special-0", "your-special-1"), filterDuration = 1000L)
    assertListTransactions(Set("t0", "t1", "my-special-0"), filterDuration = 2000L)
    assertListTransactions(Set(), filterDuration = 3000L)
    assertListTransactions(Set("t0", "t1", "my-special-0"), filterStates = Set("Ongoing"))
    assertListTransactions(Set("t0", "t1", "my-special-0"), filterStates = Set("Ongoing", "UnknownState"))
    assertListTransactions(Set("t2", "t4"), filterStates = Set("PrepareCommit", "CompleteCommit"))
    assertListTransactions(Set(), filterStates = Set("UnknownState"))
    assertListTransactions(Set("t5"), filterProducerIds = Set(5L))
    assertListTransactions(Set("t5", "t6"), filterProducerIds = Set(5L, 6L, 8L, 9L))
    assertListTransactions(Set("t4"), filterProducerIds = Set(4L, 5L), filterStates = Set("CompleteCommit"))
    assertListTransactions(Set("t4", "t5"), filterProducerIds = Set(4L, 5L), filterStates = Set("CompleteCommit", "CompleteAbort"))
    assertListTransactions(Set(), filterProducerIds = Set(3L, 6L), filterStates = Set("UnknownState"))
    assertListTransactions(Set(), filterProducerIds = Set(10L), filterStates = Set("CompleteCommit"))
    assertListTransactions(Set(), filterStates = Set("Dead"))
    assertListTransactions(Set("my-special-0", "your-special-1", "their-special-2"), filteredTransactionalIdPattern = ".*special-.*")
    assertListTransactions(Set(), filteredTransactionalIdPattern = "nothing")
    assertListTransactions(Set("my-special-0", "your-special-1"), filterDuration = 1000L, filteredTransactionalIdPattern = ".*special-.*")
    assertListTransactions(Set("their-special-2"), filterProducerIds = Set(7L), filterStates = Set("CompleteCommit", "CompleteAbort"), filteredTransactionalIdPattern = ".*special-.*")
  }

  @Test
  def testListTransactionsFilteringWithInvalidPattern(): Unit = {
    assertThrows(classOf[InvalidRegularExpression], () => transactionManager.listTransactionStates(Set.empty, Set.empty, -1L, "(ab(cd"))
  }

  @Test
  def shouldOnlyConsiderTransactionsInTheOngoingStateToAbort(): Unit = {
    for (partitionId <- 0 until numPartitions) {
      transactionManager.addLoadedTransactionsToCache(partitionId, 0, new ConcurrentHashMap[String, TransactionMetadata]())
    }

    transactionManager.putTransactionStateIfNotExists(transactionMetadata("ongoing", producerId = 0, state = TransactionState.ONGOING))
    transactionManager.putTransactionStateIfNotExists(transactionMetadata("not-expiring", producerId = 1, state = TransactionState.ONGOING, txnTimeout = 10000))
    transactionManager.putTransactionStateIfNotExists(transactionMetadata("prepare-commit", producerId = 2, state = TransactionState.PREPARE_COMMIT))
    transactionManager.putTransactionStateIfNotExists(transactionMetadata("prepare-abort", producerId = 3, state = TransactionState.PREPARE_ABORT))
    transactionManager.putTransactionStateIfNotExists(transactionMetadata("complete-commit", producerId = 4, state = TransactionState.COMPLETE_COMMIT))
    transactionManager.putTransactionStateIfNotExists(transactionMetadata("complete-abort", producerId = 5, state = TransactionState.COMPLETE_ABORT))

    time.sleep(2000)
    val expiring = transactionManager.timedOutTransactions()
    assertEquals(List(TransactionalIdAndProducerIdEpoch("ongoing", 0, 0)), expiring)
  }

  @Test
  def shouldWriteTxnMarkersForTransactionInPreparedCommitState(): Unit = {
    verifyWritesTxnMarkersInPrepareState(TransactionState.PREPARE_COMMIT)
  }

  @Test
  def shouldWriteTxnMarkersForTransactionInPreparedAbortState(): Unit = {
    verifyWritesTxnMarkersInPrepareState(TransactionState.PREPARE_ABORT)
  }

  @Test
  def shouldRemoveCompleteCommitExpiredTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.COMPLETE_COMMIT)
    verifyMetadataDoesntExist(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldRemoveCompleteAbortExpiredTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.COMPLETE_ABORT)
    verifyMetadataDoesntExist(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldRemoveEmptyExpiredTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.EMPTY)
    verifyMetadataDoesntExist(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldNotRemoveExpiredTransactionalIdsIfLogAppendFails(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NOT_ENOUGH_REPLICAS, TransactionState.COMPLETE_ABORT)
    verifyMetadataDoesExistAndIsUsable(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldNotRemoveOngoingTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.ONGOING)
    verifyMetadataDoesExistAndIsUsable(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldNotRemovePrepareAbortTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.PREPARE_ABORT)
    verifyMetadataDoesExistAndIsUsable(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldNotRemovePrepareCommitTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.PREPARE_COMMIT)
    verifyMetadataDoesExistAndIsUsable(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def testTransactionalExpirationWithTooSmallBatchSize(): Unit = {
    // The batch size is too small, but we nevertheless expect the
    // coordinator to attempt the append. This test mainly ensures
    // that the expiration task does not get stuck.

    val partitionIds = 0 until numPartitions
    val maxBatchSize = 16

    loadTransactionsForPartitions(partitionIds)
    val allTransactionalIds = loadExpiredTransactionalIds(numTransactionalIds = 20)

    reset(replicaManager)
    expectLogConfig(partitionIds, maxBatchSize)

    val attemptedAppends = mutable.Map.empty[TopicIdPartition, mutable.Buffer[MemoryRecords]]
    expectTransactionalIdExpiration(Errors.MESSAGE_TOO_LARGE, attemptedAppends)

    assertEquals(allTransactionalIds, listExpirableTransactionalIds())
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 0))).thenReturn(new TopicIdPartition(transactionTopicId, 0, TRANSACTION_STATE_TOPIC_NAME))
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 1))).thenReturn(new TopicIdPartition(transactionTopicId, 1, TRANSACTION_STATE_TOPIC_NAME))
    transactionManager.removeExpiredTransactionalIds()
    verify(replicaManager, atLeastOnce()).appendRecords(
      anyLong(),
      ArgumentMatchers.eq((-1).toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      any(),
      any(),
      any(),
      any(),
      any()
    )

    for (batches <- attemptedAppends.values; batch <- batches) {
      assertTrue(batch.sizeInBytes() > maxBatchSize)
    }

    assertEquals(allTransactionalIds, listExpirableTransactionalIds())
  }

  @Test
  def testTransactionalExpirationWithOfflineLogDir(): Unit = {
    val onlinePartitionId = 0
    val offlinePartitionId = 1

    val partitionIds = Seq(onlinePartitionId, offlinePartitionId)
    val maxBatchSize = 512

    loadTransactionsForPartitions(partitionIds)
    val allTransactionalIds = loadExpiredTransactionalIds(numTransactionalIds = 20)

    reset(replicaManager)

    // Partition 0 returns log config as normal
    expectLogConfig(Seq(onlinePartitionId), maxBatchSize)
    // No log config returned for partition 0 since it is offline
    when(replicaManager.getLogConfig(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, offlinePartitionId)))
      .thenReturn(None)
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 0))).thenReturn(new TopicIdPartition(transactionTopicId, 0, TRANSACTION_STATE_TOPIC_NAME))
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 1))).thenReturn(new TopicIdPartition(transactionTopicId, 1, TRANSACTION_STATE_TOPIC_NAME))
    val appendedRecords = mutable.Map.empty[TopicIdPartition, mutable.Buffer[MemoryRecords]]
    expectTransactionalIdExpiration(Errors.NONE, appendedRecords)

    assertEquals(allTransactionalIds, listExpirableTransactionalIds())
    transactionManager.removeExpiredTransactionalIds()
    verify(replicaManager).appendRecords(
      anyLong(),
      ArgumentMatchers.eq((-1).toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      any(),
      any(),
      any(),
      any(),
      any()
    )

    assertEquals(Set(onlinePartitionId), appendedRecords.keySet.map(_.partition))

    val (transactionalIdsForOnlinePartition, transactionalIdsForOfflinePartition) =
      allTransactionalIds.partition { transactionalId =>
        transactionManager.partitionFor(transactionalId) == onlinePartitionId
      }

    val expiredTransactionalIds = collectTransactionalIdsFromTombstones(appendedRecords)
    assertEquals(transactionalIdsForOnlinePartition, expiredTransactionalIds)
    assertEquals(transactionalIdsForOfflinePartition, listExpirableTransactionalIds())
  }

  @Test
  def testTransactionExpirationShouldRespectBatchSize(): Unit = {
    val partitionIds = 0 until numPartitions
    val maxBatchSize = 512

    loadTransactionsForPartitions(partitionIds)
    val allTransactionalIds = loadExpiredTransactionalIds(numTransactionalIds = 1000)

    reset(replicaManager)
    expectLogConfig(partitionIds, maxBatchSize)

    val appendedRecords = mutable.Map.empty[TopicIdPartition, mutable.Buffer[MemoryRecords]]
    expectTransactionalIdExpiration(Errors.NONE, appendedRecords)

    assertEquals(allTransactionalIds, listExpirableTransactionalIds())
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 0))).thenReturn(new TopicIdPartition(transactionTopicId, 0, TRANSACTION_STATE_TOPIC_NAME))
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 1))).thenReturn(new TopicIdPartition(transactionTopicId, 1, TRANSACTION_STATE_TOPIC_NAME))

    transactionManager.removeExpiredTransactionalIds()
    verify(replicaManager, atLeastOnce()).appendRecords(
      anyLong(),
      ArgumentMatchers.eq((-1).toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      any(),
      any(),
      any(),
      any(),
      any())

    assertEquals(Set.empty, listExpirableTransactionalIds())
    assertEquals(partitionIds.toSet, appendedRecords.keys.map(_.partition))

    appendedRecords.values.foreach { batches =>
      assertTrue(batches.size > 1) // Ensure a non-trivial test case
      assertTrue(batches.forall(_.sizeInBytes() < maxBatchSize))
    }

    val expiredTransactionalIds = collectTransactionalIdsFromTombstones(appendedRecords)
    assertEquals(allTransactionalIds, expiredTransactionalIds)
  }

  @Test
  def testTransactionExpirationShouldNotFailWithUninitializedTransactionMetadata(): Unit = {
    val partitionIds = 0 until numPartitions
    val maxBatchSize = 512
    val transactionalId = "id"
    val allTransactionalIds = Set(transactionalId)

    loadTransactionsForPartitions(partitionIds)

    // When TransactionMetadata is initialized for the first time, it has the following
    // shape. Then, the producer id and producer epoch are initialized and we try to
    // write the change. If the write fails (e.g. under min isr), the TransactionMetadata
    // is left at it is. If the transactional id is never reused, the TransactionMetadata
    // will be expired and it should succeed.
    val timestamp = time.milliseconds()
    val txnMetadata = new TransactionMetadata(transactionalId, 1, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
      RecordBatch.NO_PRODUCER_EPOCH, transactionTimeoutMs, TransactionState.EMPTY, util.Set.of, timestamp, timestamp, TV_0)
    transactionManager.putTransactionStateIfNotExists(txnMetadata)

    time.sleep(txnConfig.transactionalIdExpirationMs + 1)

    reset(replicaManager)
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 0))).thenReturn(new TopicIdPartition(transactionTopicId, 0, TRANSACTION_STATE_TOPIC_NAME))
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 1))).thenReturn(new TopicIdPartition(transactionTopicId, 1, TRANSACTION_STATE_TOPIC_NAME))
    expectLogConfig(partitionIds, maxBatchSize)

    val appendedRecords = mutable.Map.empty[TopicIdPartition, mutable.Buffer[MemoryRecords]]
    expectTransactionalIdExpiration(Errors.NONE, appendedRecords)

    transactionManager.removeExpiredTransactionalIds()
    verify(replicaManager, atLeastOnce()).appendRecords(
      anyLong(),
      ArgumentMatchers.eq((-1).toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      any(),
      any(),
      any(),
      any(),
      any()
    )

    val expiredTransactionalIds = collectTransactionalIdsFromTombstones(appendedRecords)
    assertEquals(allTransactionalIds, expiredTransactionalIds)
  }

  private def collectTransactionalIdsFromTombstones(
    appendedRecords: mutable.Map[TopicIdPartition, mutable.Buffer[MemoryRecords]]
  ): Set[String] = {
    val expiredTransactionalIds = mutable.Set.empty[String]
    appendedRecords.values.foreach { batches =>
      batches.foreach { records =>
        records.records.forEach { record =>
          val transactionalId = TransactionLog.readTxnRecordKey(record.key).asInstanceOf[String]
          assertNull(record.value)
          expiredTransactionalIds += transactionalId
          assertEquals(Right(None), transactionManager.getTransactionState(transactionalId))
        }
      }
    }
    expiredTransactionalIds.toSet
  }

  private def loadExpiredTransactionalIds(
    numTransactionalIds: Int
  ): Set[String] = {
    val allTransactionalIds = mutable.Set.empty[String]
    for (i <- 0 to numTransactionalIds) {
      val txnlId = s"id_$i"
      val producerId = i
      val txnMetadata = transactionMetadata(txnlId, producerId)
      txnMetadata.txnLastUpdateTimestamp(time.milliseconds() - txnConfig.transactionalIdExpirationMs)
      transactionManager.putTransactionStateIfNotExists(txnMetadata)
      allTransactionalIds += txnlId
    }
    allTransactionalIds.toSet
  }

  private def listExpirableTransactionalIds(): Set[String] = {
    val activeTransactionalIds = transactionManager.listTransactionStates(Set.empty, Set.empty, -1L, null)
      .transactionStates
      .asScala
      .map(_.transactionalId)

    activeTransactionalIds.filter { transactionalId =>
      transactionManager.getTransactionState(transactionalId) match {
        case Right(Some(epochAndMetadata)) =>
          val txnMetadata = epochAndMetadata.transactionMetadata
          val timeSinceLastUpdate = time.milliseconds() - txnMetadata.txnLastUpdateTimestamp
          timeSinceLastUpdate >= txnConfig.transactionalIdExpirationMs &&
            txnMetadata.state.isExpirationAllowed &&
            txnMetadata.pendingState.isEmpty
        case _ => false
      }
    }.toSet
  }

  @Test
  def testSuccessfulReimmigration(): Unit = {
    txnMetadata1.state(TransactionState.PREPARE_COMMIT)
    txnMetadata1.addPartitions(util.Set.of(new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit(), TV_2))
    val startOffset = 0L
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE, txnRecords.toArray: _*)

    prepareTxnLog(topicPartition, 0, records)

    // immigrate partition at epoch 0
    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch = 0, (_, _, _, _) => ())
    assertEquals(0, transactionManager.loadingPartitions.size)

    // Re-immigrate partition at epoch 1. This should be successful even though we didn't get to emigrate the partition.
    prepareTxnLog(topicPartition, 0, records)
    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch = 1, (_, _, _, _) => ())
    assertEquals(0, transactionManager.loadingPartitions.size)
    assertTrue(transactionManager.transactionMetadataCache.contains(partitionId))
    assertEquals(1, transactionManager.transactionMetadataCache(partitionId).coordinatorEpoch)
  }

  @Test
  def testLoadTransactionMetadataWithCorruptedLog(): Unit = {
    // Simulate a case where startOffset < endOffset but log is empty. This could theoretically happen
    // when all the records are expired and the active segment is truncated or when the partition
    // is accidentally corrupted.
    val startOffset = 0L
    val endOffset = 10L

    val logMock: UnifiedLog = mock(classOf[UnifiedLog])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(logMock))
    when(logMock.logStartOffset).thenReturn(startOffset)
    when(logMock.read(ArgumentMatchers.eq(startOffset),
      anyInt(),
      ArgumentMatchers.eq(FetchIsolation.LOG_END),
      ArgumentMatchers.eq(true))
    ).thenReturn(new FetchDataInfo(new LogOffsetMetadata(startOffset), MemoryRecords.EMPTY))
    when(replicaManager.getLogEndOffset(topicPartition)).thenReturn(Some(endOffset))

    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch = 0, (_, _, _, _) => ())

    // let the time advance to trigger the background thread loading
    scheduler.tick()

    verify(replicaManager).getLog(topicPartition)
    verify(logMock).logStartOffset
    verify(logMock).read(ArgumentMatchers.eq(startOffset),
      anyInt(),
      ArgumentMatchers.eq(FetchIsolation.LOG_END),
      ArgumentMatchers.eq(true))
    verify(replicaManager, times(2)).getLogEndOffset(topicPartition)
    assertEquals(0, transactionManager.loadingPartitions.size)
  }

  private def createEmptyBatch(baseOffset: Long, lastOffset: Long): MemoryRecords = {
    val buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD)
    DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.CURRENT_MAGIC_VALUE, RecordBatch.NO_PRODUCER_ID,
      RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, baseOffset, lastOffset, RecordBatch.NO_PARTITION_LEADER_EPOCH,
      TimestampType.CREATE_TIME, System.currentTimeMillis, false, false)
    buffer.flip
    MemoryRecords.readableRecords(buffer)
  }

  @Test
  def testLoadTransactionMetadataContainingSegmentEndingWithEmptyBatch(): Unit = {
    // Simulate a case where a log contains two segments and the first segment ending with an empty batch.
    txnMetadata1.state(TransactionState.PREPARE_COMMIT)
    txnMetadata1.addPartitions(util.Set.of(new TopicPartition("topic1", 0)))
    txnMetadata2.state(TransactionState.ONGOING)
    txnMetadata2.addPartitions(util.Set.of(new TopicPartition("topic2", 0)))

    // Create the first segment which contains two batches.
    // The first batch has one transactional record
    val txnRecords1 = new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit(), TV_2))
    val records1 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L, Compression.NONE, TimestampType.CREATE_TIME, txnRecords1)
    // The second batch is an empty batch.
    val records2 = createEmptyBatch(1L, 1L)

    val combinedBuffer = ByteBuffer.allocate(records1.buffer.limit + records2.buffer.limit)
    combinedBuffer.put(records1.buffer)
    combinedBuffer.put(records2.buffer)
    combinedBuffer.flip
    val firstSegmentRecords = MemoryRecords.readableRecords(combinedBuffer)

    // Create the second segment which contains one batch
    val txnRecords3 = new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2.prepareNoTransit(), TV_2))
    val secondSegmentRecords = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 2L, Compression.NONE, TimestampType.CREATE_TIME, txnRecords3)

    // Prepare a txn log
    reset(replicaManager)

    val logMock = mock(classOf[UnifiedLog])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(logMock))
    when(replicaManager.getLogEndOffset(topicPartition)).thenReturn(Some(3L))

    when(logMock.logStartOffset).thenReturn(0L)
    when(logMock.read(ArgumentMatchers.eq(0L),
      anyInt(),
      ArgumentMatchers.eq(FetchIsolation.LOG_END),
      ArgumentMatchers.eq(true)))
      .thenReturn(new FetchDataInfo(new LogOffsetMetadata(0L), firstSegmentRecords))
    when(logMock.read(ArgumentMatchers.eq(2L),
      anyInt(),
      ArgumentMatchers.eq(FetchIsolation.LOG_END),
      ArgumentMatchers.eq(true)))
      .thenReturn(new FetchDataInfo(new LogOffsetMetadata(2L), secondSegmentRecords))

    // Load transactions should not stuck.
    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch = 1, (_, _, _, _) => ())
    assertEquals(0, transactionManager.loadingPartitions.size)
    assertEquals(1, transactionManager.transactionMetadataCache.size)
    assertTrue(transactionManager.transactionMetadataCache.contains(partitionId))
    // all transactions should have been loaded
    val txnMetadataPool = transactionManager.transactionMetadataCache(partitionId).metadataPerTransactionalId
    assertEquals(2, txnMetadataPool.size)
    assertTrue(txnMetadataPool.containsKey(transactionalId1))
    assertTrue(txnMetadataPool.containsKey(transactionalId2))
  }

  private def verifyMetadataDoesExistAndIsUsable(transactionalId: String): Unit = {
    transactionManager.getTransactionState(transactionalId) match {
      case Left(_) => fail("shouldn't have been any errors")
      case Right(None) => fail("metadata should have been removed")
      case Right(Some(metadata)) =>
        assertTrue(metadata.transactionMetadata.pendingState.isEmpty, "metadata shouldn't be in a pending state")
    }
  }

  private def verifyMetadataDoesntExist(transactionalId: String): Unit = {
    transactionManager.getTransactionState(transactionalId) match {
      case Left(_) => fail("shouldn't have been any errors")
      case Right(Some(_)) => fail("metadata should have been removed")
      case Right(None) => // ok
    }
  }

  private def expectTransactionalIdExpiration(
    appendError: Errors,
    capturedAppends: mutable.Map[TopicIdPartition, mutable.Buffer[MemoryRecords]]
  ): Unit = {
    val recordsCapture: ArgumentCaptor[Map[TopicIdPartition, MemoryRecords]] = ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, MemoryRecords]])
    val callbackCapture: ArgumentCaptor[Map[TopicIdPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(
      anyLong(),
      ArgumentMatchers.eq((-1).toShort),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      recordsCapture.capture(),
      callbackCapture.capture(),
      any(),
      any(),
      any(),
      any()
    )).thenAnswer(_ => callbackCapture.getValue.apply(
      recordsCapture.getValue.map { case (topicPartition, records) =>
        val batches = capturedAppends.getOrElse(topicPartition, {
          val batches = mutable.Buffer.empty[MemoryRecords]
          capturedAppends += topicPartition -> batches
          batches
        })

        batches += records

        topicPartition -> new PartitionResponse(appendError, 0L, RecordBatch.NO_TIMESTAMP, 0L)
      }.toMap
    ))
  }

  private def loadTransactionsForPartitions(
    partitionIds: Seq[Int],
  ): Unit = {
    for (partitionId <- partitionIds) {
      transactionManager.addLoadedTransactionsToCache(partitionId, 0, new ConcurrentHashMap[String, TransactionMetadata]())
    }
  }

  private def expectLogConfig(
    partitionIds: Seq[Int],
    maxBatchSize: Int
  ): Unit = {
    val logConfig: LogConfig = mock(classOf[LogConfig])
    when(logConfig.maxMessageSize).thenReturn(maxBatchSize)

    for (partitionId <- partitionIds) {
      when(replicaManager.getLogConfig(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, partitionId)))
        .thenReturn(Some(logConfig))
    }
  }

  private def setupAndRunTransactionalIdExpiration(error: Errors, txnState: TransactionState): Unit = {
    val partitionIds = 0 until numPartitions

    loadTransactionsForPartitions(partitionIds)
    expectLogConfig(partitionIds, ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT)

    txnMetadata1.txnLastUpdateTimestamp(time.milliseconds() - txnConfig.transactionalIdExpirationMs)
    txnMetadata1.state(txnState)
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    txnMetadata2.txnLastUpdateTimestamp(time.milliseconds())
    transactionManager.putTransactionStateIfNotExists(txnMetadata2)

    val appendedRecords = mutable.Map.empty[TopicIdPartition, mutable.Buffer[MemoryRecords]]
    expectTransactionalIdExpiration(error, appendedRecords)

    transactionManager.removeExpiredTransactionalIds()

    val stateAllowsExpiration = txnState match {
      case TransactionState.EMPTY | TransactionState.COMPLETE_COMMIT | TransactionState.COMPLETE_ABORT => true
      case _ => false
    }

    if (stateAllowsExpiration) {
      val partitionId = transactionManager.partitionFor(transactionalId1)
      val topicPartition = new TopicIdPartition(transactionTopicId, partitionId, TRANSACTION_STATE_TOPIC_NAME)
      val expectedTombstone = new SimpleRecord(time.milliseconds(), TransactionLog.keyToBytes(transactionalId1), null)
      val expectedRecords = MemoryRecords.withRecords(TransactionLog.ENFORCED_COMPRESSION, expectedTombstone)
      assertEquals(Set(topicPartition), appendedRecords.keySet)
      assertEquals(Seq(expectedRecords), appendedRecords(topicPartition).toSeq)
    } else {
      assertEquals(Map.empty, appendedRecords)
    }
  }

  private def verifyWritesTxnMarkersInPrepareState(state: TransactionState): Unit = {
    txnMetadata1.state(state)
    txnMetadata1.addPartitions(util.Set.of(new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit(), TV_2))
    val startOffset = 0L
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE, txnRecords.toArray: _*)

    prepareTxnLog(topicPartition, 0, records)

    var txnId: String = null
    def rememberTxnMarkers(coordinatorEpoch: Int,
                           command: TransactionResult,
                           metadata: TransactionMetadata,
                           newMetadata: TxnTransitMetadata): Unit = {
      txnId = metadata.transactionalId
    }

    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, 0, rememberTxnMarkers)
    scheduler.tick()

    assertEquals(transactionalId1, txnId)
  }

  private def assertCallback(error: Errors): Unit = {
    assertEquals(expectedError, error)
  }

  private def transactionMetadata(transactionalId: String,
                                  producerId: Long,
                                  state: TransactionState = TransactionState.EMPTY,
                                  txnTimeout: Int = transactionTimeoutMs): TransactionMetadata = {
    val timestamp = time.milliseconds()
    new TransactionMetadata(transactionalId, producerId, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_ID, 0.toShort,
      RecordBatch.NO_PRODUCER_EPOCH, txnTimeout, state, util.Set.of, timestamp, timestamp, TV_0)
  }

  private def prepareTxnLog(topicPartition: TopicPartition,
                            startOffset: Long,
                            records: MemoryRecords): Unit = {
    reset(replicaManager)

    val logMock: UnifiedLog = mock(classOf[UnifiedLog])
    val fileRecordsMock: FileRecords = mock(classOf[FileRecords])

    val endOffset = startOffset + records.records.asScala.size

    when(replicaManager.getLog(topicPartition)).thenReturn(Some(logMock))
    when(replicaManager.getLogEndOffset(topicPartition)).thenReturn(Some(endOffset))

    when(logMock.logStartOffset).thenReturn(startOffset)
    when(logMock.read(ArgumentMatchers.eq(startOffset),
      anyInt(),
      ArgumentMatchers.eq(FetchIsolation.LOG_END),
      ArgumentMatchers.eq(true)))
      .thenReturn(new FetchDataInfo(new LogOffsetMetadata(startOffset), fileRecordsMock))

    when(fileRecordsMock.sizeInBytes()).thenReturn(records.sizeInBytes)

    val bufferCapture: ArgumentCaptor[ByteBuffer] = ArgumentCaptor.forClass(classOf[ByteBuffer])
    when(fileRecordsMock.readInto(bufferCapture.capture(), anyInt())).thenAnswer(_ => {
      val buffer = bufferCapture.getValue
      buffer.put(records.buffer.duplicate)
      buffer.flip()
    })
  }

  private def prepareForTxnMessageAppend(error: Errors): Unit = {
    reset(replicaManager)

    val capturedArgument: ArgumentCaptor[Map[TopicIdPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicIdPartition, PartitionResponse] => Unit])
    when(replicaManager.appendRecords(anyLong(),
      anyShort(),
      internalTopicsAllowed = ArgumentMatchers.eq(true),
      origin = ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any[Map[TopicIdPartition, MemoryRecords]],
      capturedArgument.capture(),
      any(),
      any(),
      any(),
      any()
    )).thenAnswer(_ => capturedArgument.getValue.apply(
      Map(new TopicIdPartition(transactionTopicId, partitionId, TRANSACTION_STATE_TOPIC_NAME) ->
        new PartitionResponse(error, 0L, RecordBatch.NO_TIMESTAMP, 0L)))
    )
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 0))).thenReturn(new TopicIdPartition(transactionTopicId, 0, TRANSACTION_STATE_TOPIC_NAME))
    when(replicaManager.topicIdPartition(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, 1))).thenReturn(new TopicIdPartition(transactionTopicId, 1, TRANSACTION_STATE_TOPIC_NAME))
  }

  @Test
  def testPartitionLoadMetric(): Unit = {
    val server = ManagementFactory.getPlatformMBeanServer
    val mBeanName = "kafka.server:type=transaction-coordinator-metrics"
    val reporter = new JmxReporter
    val metricsContext = new KafkaMetricsContext("kafka.server")
    reporter.contextChange(metricsContext)
    metrics.addReporter(reporter)

    def partitionLoadTime(attribute: String): Double = {
      server.getAttribute(new ObjectName(mBeanName), attribute).asInstanceOf[Double]
    }

    assertTrue(server.isRegistered(new ObjectName(mBeanName)))
    assertEquals(Double.NaN, partitionLoadTime( "partition-load-time-max"), 0)
    assertEquals(Double.NaN, partitionLoadTime("partition-load-time-avg"), 0)
    assertTrue(reporter.containsMbean(mBeanName))

    txnMetadata1.state(TransactionState.ONGOING)
    txnMetadata1.addPartitions(util.List.of(new TopicPartition("topic1", 1),
      new TopicPartition("topic1", 1)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit(), TV_2))

    val startOffset = 15L
    val records = MemoryRecords.withRecords(startOffset, Compression.NONE, txnRecords.toArray: _*)

    prepareTxnLog(topicPartition, startOffset, records)
    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, 0, (_, _, _, _) => ())
    scheduler.tick()

    assertTrue(partitionLoadTime("partition-load-time-max") >= 0)
    assertTrue(partitionLoadTime( "partition-load-time-avg") >= 0)
  }

  @Test
  def testIgnoreUnknownRecordType(): Unit = {
    txnMetadata1.state(TransactionState.PREPARE_COMMIT)
    txnMetadata1.addPartitions(util.Set.of(new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit(), TV_2))
    val startOffset = 0L

    val unknownKey = new TransactionLogKey()
    val unknownMessage = MessageUtil.toVersionPrefixedBytes(Short.MaxValue, unknownKey)
    val unknownRecord = new SimpleRecord(unknownMessage, unknownMessage)

    val records = MemoryRecords.withRecords(startOffset, Compression.NONE,
      (Seq(unknownRecord) ++ txnRecords).toArray: _*)

    prepareTxnLog(topicPartition, 0, records)

    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch = 1, (_, _, _, _) => ())
    assertEquals(0, transactionManager.loadingPartitions.size)
    assertTrue(transactionManager.transactionMetadataCache.contains(partitionId))
    val txnMetadataPool = transactionManager.transactionMetadataCache(partitionId).metadataPerTransactionalId
    assertFalse(txnMetadataPool.isEmpty)
    assertTrue(txnMetadataPool.containsKey(transactionalId1))
    val txnMetadata = txnMetadataPool.get(transactionalId1)
    assertEquals(txnMetadata1.transactionalId, txnMetadata.transactionalId)
    assertEquals(txnMetadata1.producerId, txnMetadata.producerId)
    assertEquals(txnMetadata1.prevProducerId, txnMetadata.prevProducerId)
    assertEquals(txnMetadata1.producerEpoch, txnMetadata.producerEpoch)
    assertEquals(txnMetadata1.lastProducerEpoch, txnMetadata.lastProducerEpoch)
    assertEquals(txnMetadata1.txnTimeoutMs, txnMetadata.txnTimeoutMs)
    assertEquals(txnMetadata1.state, txnMetadata.state)
    assertEquals(txnMetadata1.topicPartitions, txnMetadata.topicPartitions)
    assertEquals(1, transactionManager.transactionMetadataCache(partitionId).coordinatorEpoch)
  }

  @ParameterizedTest
  @EnumSource(classOf[TransactionVersion])
  def testTransactionVersionInTransactionManager(transactionVersion: TransactionVersion): Unit = {
    val metadataCache = mock(classOf[MetadataCache])
    when(metadataCache.features()).thenReturn {
      new FinalizedFeatures(
        MetadataVersion.latestTesting(),
        util.Map.of(TransactionVersion.FEATURE_NAME, transactionVersion.featureLevel()),
        0)
    }
    val transactionManager = new TransactionStateManager(0, scheduler,
      replicaManager, metadataCache, txnConfig, time, metrics)

    assertEquals(transactionVersion, transactionManager.transactionVersionLevel())
  }
}
