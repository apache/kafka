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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock
import javax.management.ObjectName
import kafka.log.{AppendOrigin, Log}
import kafka.server.{FetchDataInfo, FetchLogEnd, LogOffsetMetadata, ReplicaManager}
import kafka.utils.{MockScheduler, Pool, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME
import org.apache.kafka.common.metrics.{JmxReporter, KafkaMetricsContext, Metrics}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.MockTime
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue, fail}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, mutable}

class TransactionStateManagerTest {

  val partitionId = 0
  val numPartitions = 2
  val transactionTimeoutMs: Int = 1000
  val topicPartition = new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, partitionId)
  val coordinatorEpoch = 10

  val txnRecords: mutable.ArrayBuffer[SimpleRecord] = mutable.ArrayBuffer[SimpleRecord]()

  val time = new MockTime()
  val scheduler = new MockScheduler(time)
  val zkClient: KafkaZkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])
  val replicaManager: ReplicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])

  EasyMock.expect(zkClient.getTopicPartitionCount(TRANSACTION_STATE_TOPIC_NAME))
    .andReturn(Some(numPartitions))
    .anyTimes()

  EasyMock.replay(zkClient)
  val metrics = new Metrics()

  val txnConfig = TransactionConfig()
  val transactionManager: TransactionStateManager = new TransactionStateManager(0, zkClient, scheduler,
    replicaManager, txnConfig, time, metrics)

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
    // make sure the transactional id hashes to the assigning partition id
    assertEquals(partitionId, transactionManager.partitionFor(transactionalId1))
    assertEquals(partitionId, transactionManager.partitionFor(transactionalId2))
  }

  @AfterEach
  def tearDown(): Unit = {
    EasyMock.reset(zkClient, replicaManager)
    transactionManager.shutdown()
  }

  @Test
  def testValidateTransactionTimeout(): Unit = {
    assertTrue(transactionManager.validateTransactionTimeoutMs(1))
    assertFalse(transactionManager.validateTransactionTimeoutMs(-1))
    assertFalse(transactionManager.validateTransactionTimeoutMs(0))
    assertTrue(transactionManager.validateTransactionTimeoutMs(txnConfig.transactionMaxTimeoutMs))
    assertFalse(transactionManager.validateTransactionTimeoutMs(txnConfig.transactionMaxTimeoutMs + 1))
  }

  @Test
  def testAddGetPids(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new Pool[String, TransactionMetadata]())

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

    transactionManager.addLoadedTransactionsToCache(0, coordinatorEpoch, new Pool[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(metadata1)

    transactionManager.addLoadedTransactionsToCache(1, coordinatorEpoch, new Pool[String, TransactionMetadata]())
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

    val fileRecordsMock = EasyMock.mock[FileRecords](classOf[FileRecords])
    val logMock = EasyMock.mock[Log](classOf[Log])
    EasyMock.expect(replicaManager.getLog(topicPartition)).andStubReturn(Some(logMock))
    EasyMock.expect(logMock.logStartOffset).andStubReturn(startOffset)
    EasyMock.expect(logMock.read(EasyMock.eq(startOffset),
      maxLength = EasyMock.anyInt(),
      isolation = EasyMock.eq(FetchLogEnd),
      minOneMessage = EasyMock.eq(true))
    ).andReturn(FetchDataInfo(LogOffsetMetadata(startOffset), fileRecordsMock))
    EasyMock.expect(replicaManager.getLogEndOffset(topicPartition)).andStubReturn(Some(endOffset))

    txnMetadata1.state = PrepareCommit
    txnMetadata1.addPartitions(Set[TopicPartition](
      new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit())))

    // We create a latch which is awaited while the log is loading. This ensures that the deletion
    // is triggered before the loading returns
    val latch = new CountDownLatch(1)

    EasyMock.expect(fileRecordsMock.sizeInBytes()).andStubReturn(records.sizeInBytes)
    val bufferCapture = EasyMock.newCapture[ByteBuffer]
    fileRecordsMock.readInto(EasyMock.capture(bufferCapture), EasyMock.anyInt())
    EasyMock.expectLastCall().andAnswer(new IAnswer[Unit] {
      override def answer: Unit = {
        latch.await()
        val buffer = bufferCapture.getValue
        buffer.put(records.buffer.duplicate)
        buffer.flip()
      }
    })

    EasyMock.replay(logMock, fileRecordsMock, replicaManager)

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
  def testLoadAndRemoveTransactionsForPartition(): Unit = {
    // generate transaction log messages for two pids traces:

    // pid1's transaction started with two partitions
    txnMetadata1.state = Ongoing
    txnMetadata1.addPartitions(Set[TopicPartition](new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit()))

    // pid1's transaction adds three more partitions
    txnMetadata1.addPartitions(Set[TopicPartition](new TopicPartition("topic2", 0),
      new TopicPartition("topic2", 1),
      new TopicPartition("topic2", 2)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit()))

    // pid1's transaction is preparing to commit
    txnMetadata1.state = PrepareCommit

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit()))

    // pid2's transaction started with three partitions
    txnMetadata2.state = Ongoing
    txnMetadata2.addPartitions(Set[TopicPartition](new TopicPartition("topic3", 0),
      new TopicPartition("topic3", 1),
      new TopicPartition("topic3", 2)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2.prepareNoTransit()))

    // pid2's transaction is preparing to abort
    txnMetadata2.state = PrepareAbort

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2.prepareNoTransit()))

    // pid2's transaction has aborted
    txnMetadata2.state = CompleteAbort

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2.prepareNoTransit()))

    // pid2's epoch has advanced, with no ongoing transaction yet
    txnMetadata2.state = Empty
    txnMetadata2.topicPartitions.clear()

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2.prepareNoTransit()))

    val startOffset = 15L   // it should work for any start offset
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE, txnRecords.toArray: _*)

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
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new Pool[String, TransactionMetadata]())

    // first insert the initial transaction metadata
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    prepareForTxnMessageAppend(Errors.NONE)
    expectedError = Errors.NONE

    // update the metadata to ongoing with two partitions
    val newMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)), time.milliseconds())

    // append the new metadata into log
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch, newMetadata, assertCallback)

    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)
  }

  @Test
  def testAppendFailToCoordinatorNotAvailableError(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new Pool[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    expectedError = Errors.COORDINATOR_NOT_AVAILABLE
    var failedMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds())

    prepareForTxnMessageAppend(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)

    failedMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds())
    prepareForTxnMessageAppend(Errors.NOT_ENOUGH_REPLICAS)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)

    failedMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds())
    prepareForTxnMessageAppend(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)

    failedMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds())
    prepareForTxnMessageAppend(Errors.REQUEST_TIMED_OUT)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)
  }

  @Test
  def testAppendFailToNotCoordinatorError(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new Pool[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    expectedError = Errors.NOT_COORDINATOR
    var failedMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds())

    prepareForTxnMessageAppend(Errors.NOT_LEADER_OR_FOLLOWER)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)

    failedMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds())
    prepareForTxnMessageAppend(Errors.NONE)
    transactionManager.removeTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback)

    prepareForTxnMessageAppend(Errors.NONE)
    transactionManager.removeTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch)
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch + 1, new Pool[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback)

    prepareForTxnMessageAppend(Errors.NONE)
    transactionManager.removeTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch)
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new Pool[String, TransactionMetadata]())
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback)
  }

  @Test
  def testAppendFailToCoordinatorLoadingError(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new Pool[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    expectedError = Errors.COORDINATOR_LOAD_IN_PROGRESS
    val failedMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds())

    prepareForTxnMessageAppend(Errors.NONE)
    transactionManager.removeTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch)
    transactionManager.addLoadingPartition(partitionId, coordinatorEpoch + 1)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback)
  }

  @Test
  def testAppendFailToUnknownError(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new Pool[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    expectedError = Errors.UNKNOWN_SERVER_ERROR
    var failedMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds())

    prepareForTxnMessageAppend(Errors.MESSAGE_TOO_LARGE)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)

    failedMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds())
    prepareForTxnMessageAppend(Errors.RECORD_LIST_TOO_LARGE)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertTrue(txnMetadata1.pendingState.isEmpty)
  }

  @Test
  def testPendingStateNotResetOnRetryAppend(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new Pool[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    expectedError = Errors.COORDINATOR_NOT_AVAILABLE
    val failedMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds())

    prepareForTxnMessageAppend(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, failedMetadata, assertCallback, _ => true)
    assertEquals(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata1))), transactionManager.getTransactionState(transactionalId1))
    assertEquals(Some(Ongoing), txnMetadata1.pendingState)
  }

  @Test
  def testAppendTransactionToLogWhileProducerFenced(): Unit = {
    transactionManager.addLoadedTransactionsToCache(partitionId, 0, new Pool[String, TransactionMetadata]())

    // first insert the initial transaction metadata
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    prepareForTxnMessageAppend(Errors.NONE)
    expectedError = Errors.NOT_COORDINATOR

    val newMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)), time.milliseconds())

    // modify the cache while trying to append the new metadata
    txnMetadata1.producerEpoch = (txnMetadata1.producerEpoch + 1).toShort

    // append the new metadata into log
    transactionManager.appendTransactionToLog(transactionalId1, coordinatorEpoch = 10, newMetadata, assertCallback)
  }

  @Test
  def testAppendTransactionToLogWhilePendingStateChanged(): Unit = {
    // first insert the initial transaction metadata
    transactionManager.addLoadedTransactionsToCache(partitionId, coordinatorEpoch, new Pool[String, TransactionMetadata]())
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    prepareForTxnMessageAppend(Errors.NONE)
    expectedError = Errors.INVALID_PRODUCER_EPOCH

    val newMetadata = txnMetadata1.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)), time.milliseconds())

    // modify the cache while trying to append the new metadata
    txnMetadata1.pendingState = None

    // append the new metadata into log
    assertThrows(classOf[IllegalStateException], () => transactionManager.appendTransactionToLog(transactionalId1,
      coordinatorEpoch = 10, newMetadata, assertCallback))
  }

  @Test
  def shouldReturnNotCooridnatorErrorIfTransactionIdPartitionNotOwned(): Unit = {
    transactionManager.getTransactionState(transactionalId1).fold(
      err => assertEquals(Errors.NOT_COORDINATOR, err),
      _ => fail(transactionalId1 + "'s transaction state is already in the cache")
    )
  }

  @Test
  def shouldOnlyConsiderTransactionsInTheOngoingStateToAbort(): Unit = {
    for (partitionId <- 0 until numPartitions) {
      transactionManager.addLoadedTransactionsToCache(partitionId, 0, new Pool[String, TransactionMetadata]())
    }

    transactionManager.putTransactionStateIfNotExists(transactionMetadata("ongoing", producerId = 0, state = Ongoing))
    transactionManager.putTransactionStateIfNotExists(transactionMetadata("not-expiring", producerId = 1, state = Ongoing, txnTimeout = 10000))
    transactionManager.putTransactionStateIfNotExists(transactionMetadata("prepare-commit", producerId = 2, state = PrepareCommit))
    transactionManager.putTransactionStateIfNotExists(transactionMetadata("prepare-abort", producerId = 3, state = PrepareAbort))
    transactionManager.putTransactionStateIfNotExists(transactionMetadata("complete-commit", producerId = 4, state = CompleteCommit))
    transactionManager.putTransactionStateIfNotExists(transactionMetadata("complete-abort", producerId = 5, state = CompleteAbort))

    time.sleep(2000)
    val expiring = transactionManager.timedOutTransactions()
    assertEquals(List(TransactionalIdAndProducerIdEpoch("ongoing", 0, 0)), expiring)
  }

  @Test
  def shouldWriteTxnMarkersForTransactionInPreparedCommitState(): Unit = {
    verifyWritesTxnMarkersInPrepareState(PrepareCommit)
  }

  @Test
  def shouldWriteTxnMarkersForTransactionInPreparedAbortState(): Unit = {
    verifyWritesTxnMarkersInPrepareState(PrepareAbort)
  }

  @Test
  def shouldRemoveCompleteCommmitExpiredTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, CompleteCommit)
    verifyMetadataDoesntExist(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldRemoveCompleteAbortExpiredTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, CompleteAbort)
    verifyMetadataDoesntExist(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldRemoveEmptyExpiredTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, Empty)
    verifyMetadataDoesntExist(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldNotRemoveExpiredTransactionalIdsIfLogAppendFails(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NOT_ENOUGH_REPLICAS, CompleteAbort)
    verifyMetadataDoesExistAndIsUsable(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldNotRemoveOngoingTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, Ongoing)
    verifyMetadataDoesExistAndIsUsable(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldNotRemovePrepareAbortTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, PrepareAbort)
    verifyMetadataDoesExistAndIsUsable(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def shouldNotRemovePrepareCommitTransactionalIds(): Unit = {
    setupAndRunTransactionalIdExpiration(Errors.NONE, PrepareCommit)
    verifyMetadataDoesExistAndIsUsable(transactionalId1)
    verifyMetadataDoesExistAndIsUsable(transactionalId2)
  }

  @Test
  def testSuccessfulReimmigration(): Unit = {
    txnMetadata1.state = PrepareCommit
    txnMetadata1.addPartitions(Set[TopicPartition](new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit()))
    val startOffset = 0L
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE, txnRecords.toArray: _*)

    prepareTxnLog(topicPartition, 0, records)

    // immigrate partition at epoch 0
    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch = 0, (_, _, _, _) => ())
    assertEquals(0, transactionManager.loadingPartitions.size)

    // Re-immigrate partition at epoch 1. This should be successful even though we didn't get to emigrate the partition.
    prepareTxnLog(topicPartition, 0, records)
    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch = 1, (_, _, _, _) => ())
    assertEquals(0, transactionManager.loadingPartitions.size)
    assertTrue(transactionManager.transactionMetadataCache.get(partitionId).isDefined)
    assertEquals(1, transactionManager.transactionMetadataCache.get(partitionId).get.coordinatorEpoch)
  }

  @Test
  def testLoadTransactionMetadataWithCorruptedLog(): Unit = {
    // Simulate a case where startOffset < endOffset but log is empty. This could theoretically happen
    // when all the records are expired and the active segment is truncated or when the partition
    // is accidentally corrupted.
    val startOffset = 0L
    val endOffset = 10L

    val logMock: Log = EasyMock.mock(classOf[Log])
    EasyMock.expect(replicaManager.getLog(topicPartition)).andStubReturn(Some(logMock))
    EasyMock.expect(logMock.logStartOffset).andStubReturn(startOffset)
    EasyMock.expect(logMock.read(EasyMock.eq(startOffset),
      maxLength = EasyMock.anyInt(),
      isolation = EasyMock.eq(FetchLogEnd),
      minOneMessage = EasyMock.eq(true))
    ).andReturn(FetchDataInfo(LogOffsetMetadata(startOffset), MemoryRecords.EMPTY))
    EasyMock.expect(replicaManager.getLogEndOffset(topicPartition)).andStubReturn(Some(endOffset))

    EasyMock.replay(logMock)
    EasyMock.replay(replicaManager)

    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, coordinatorEpoch = 0, (_, _, _, _) => ())

    // let the time advance to trigger the background thread loading
    scheduler.tick()

    EasyMock.verify(logMock)
    EasyMock.verify(replicaManager)
    assertEquals(0, transactionManager.loadingPartitions.size)
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

  private def setupAndRunTransactionalIdExpiration(error: Errors, txnState: TransactionState): Unit = {
    for (partitionId <- 0 until numPartitions) {
      transactionManager.addLoadedTransactionsToCache(partitionId, 0, new Pool[String, TransactionMetadata]())
    }

    val capturedArgument: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()

    val partition = new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, transactionManager.partitionFor(transactionalId1))
    val recordsByPartition = Map(partition -> MemoryRecords.withRecords(TransactionLog.EnforcedCompressionType,
      new SimpleRecord(time.milliseconds() + txnConfig.removeExpiredTransactionalIdsIntervalMs, TransactionLog.keyToBytes(transactionalId1), null)))

    txnState match {
      case Empty | CompleteCommit | CompleteAbort =>

        EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
          EasyMock.eq((-1).toShort),
          EasyMock.eq(true),
          EasyMock.eq(AppendOrigin.Coordinator),
          EasyMock.eq(recordsByPartition),
          EasyMock.capture(capturedArgument),
          EasyMock.anyObject().asInstanceOf[Option[ReentrantLock]],
          EasyMock.anyObject()
        )).andAnswer(() => capturedArgument.getValue.apply(
          Map(partition -> new PartitionResponse(error, 0L, RecordBatch.NO_TIMESTAMP, 0L)))
        )
      case _ => // shouldn't append
    }

    EasyMock.replay(replicaManager)

    txnMetadata1.txnLastUpdateTimestamp = time.milliseconds() - txnConfig.transactionalIdExpirationMs
    txnMetadata1.state = txnState
    transactionManager.putTransactionStateIfNotExists(txnMetadata1)

    txnMetadata2.txnLastUpdateTimestamp = time.milliseconds()
    transactionManager.putTransactionStateIfNotExists(txnMetadata2)

    transactionManager.enableTransactionalIdExpiration()
    time.sleep(txnConfig.removeExpiredTransactionalIdsIntervalMs)

    scheduler.tick()

    EasyMock.verify(replicaManager)
  }

  private def verifyWritesTxnMarkersInPrepareState(state: TransactionState): Unit = {
    txnMetadata1.state = state
    txnMetadata1.addPartitions(Set[TopicPartition](new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit()))
    val startOffset = 0L
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE, txnRecords.toArray: _*)

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
                                  state: TransactionState = Empty,
                                  txnTimeout: Int = transactionTimeoutMs): TransactionMetadata = {
    TransactionMetadata(transactionalId, producerId, 0.toShort, txnTimeout, state, time.milliseconds())
  }

  private def prepareTxnLog(topicPartition: TopicPartition,
                            startOffset: Long,
                            records: MemoryRecords): Unit = {
    EasyMock.reset(replicaManager)

    val logMock: Log = EasyMock.mock(classOf[Log])
    val fileRecordsMock: FileRecords = EasyMock.mock(classOf[FileRecords])

    val endOffset = startOffset + records.records.asScala.size

    EasyMock.expect(replicaManager.getLog(topicPartition)).andStubReturn(Some(logMock))
    EasyMock.expect(replicaManager.getLogEndOffset(topicPartition)).andStubReturn(Some(endOffset))

    EasyMock.expect(logMock.logStartOffset).andStubReturn(startOffset)
    EasyMock.expect(logMock.read(EasyMock.eq(startOffset),
      maxLength = EasyMock.anyInt(),
      isolation = EasyMock.eq(FetchLogEnd),
      minOneMessage = EasyMock.eq(true)))
      .andReturn(FetchDataInfo(LogOffsetMetadata(startOffset), fileRecordsMock))

    EasyMock.expect(fileRecordsMock.sizeInBytes()).andStubReturn(records.sizeInBytes)

    val bufferCapture = EasyMock.newCapture[ByteBuffer]
    fileRecordsMock.readInto(EasyMock.capture(bufferCapture), EasyMock.anyInt())
    EasyMock.expectLastCall().andAnswer(new IAnswer[Unit] {
      override def answer: Unit = {
        val buffer = bufferCapture.getValue
        buffer.put(records.buffer.duplicate)
        buffer.flip()
      }
    })
    EasyMock.replay(logMock, fileRecordsMock, replicaManager)
  }

  private def prepareForTxnMessageAppend(error: Errors): Unit = {
    EasyMock.reset(replicaManager)

    val capturedArgument: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()
    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      internalTopicsAllowed = EasyMock.eq(true),
      origin = EasyMock.eq(AppendOrigin.Coordinator),
      EasyMock.anyObject().asInstanceOf[Map[TopicPartition, MemoryRecords]],
      EasyMock.capture(capturedArgument),
      EasyMock.anyObject().asInstanceOf[Option[ReentrantLock]],
      EasyMock.anyObject())
    ).andAnswer(() => capturedArgument.getValue.apply(
      Map(new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, partitionId) ->
        new PartitionResponse(error, 0L, RecordBatch.NO_TIMESTAMP, 0L)))
    )
    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject()))
      .andStubReturn(Some(RecordBatch.MAGIC_VALUE_V1))

    EasyMock.replay(replicaManager)
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

    txnMetadata1.state = Ongoing
    txnMetadata1.addPartitions(Set[TopicPartition](new TopicPartition("topic1", 1),
      new TopicPartition("topic1", 1)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1.prepareNoTransit()))

    val startOffset = 15L
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE, txnRecords.toArray: _*)

    prepareTxnLog(topicPartition, startOffset, records)
    transactionManager.loadTransactionsForTxnTopicPartition(partitionId, 0, (_, _, _, _) => ())
    scheduler.tick()

    assertTrue(partitionLoadTime("partition-load-time-max") >= 0)
    assertTrue(partitionLoadTime( "partition-load-time-avg") >= 0)
  }
}
