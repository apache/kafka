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

import java.nio.ByteBuffer

import kafka.coordinator.AbstractCoordinatorConcurrencyTest
import kafka.coordinator.AbstractCoordinatorConcurrencyTest._
import kafka.coordinator.transaction.TransactionCoordinatorConcurrencyTest._
import kafka.log.Log
import kafka.server.{DelayedOperationPurgatory, FetchDataInfo, KafkaConfig, LogOffsetMetadata, MetadataCache}
import kafka.utils.timer.MockTimer
import kafka.utils.{Pool, TestUtils}
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, FileRecords, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{LogContext, MockTime}
import org.easymock.{EasyMock, IAnswer}
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.Map
import scala.collection.mutable
import scala.collection.JavaConverters._

class TransactionCoordinatorConcurrencyTest extends AbstractCoordinatorConcurrencyTest[Transaction] {
  private val nTransactions = nThreads * 10
  private val coordinatorEpoch = 10
  private val numPartitions = nThreads * 5

  private val txnConfig = TransactionConfig()
  private var transactionCoordinator: TransactionCoordinator = _
  private var txnStateManager: TransactionStateManager = _
  private var txnMarkerChannelManager: TransactionMarkerChannelManager = _

  private val allOperations = Seq(
      new InitProducerIdOperation,
      new AddPartitionsToTxnOperation(Set(new TopicPartition("topic", 0))),
      new EndTxnOperation)

  private val allTransactions = mutable.Set[Transaction]()
  private val txnRecordsByPartition: Map[Int, mutable.ArrayBuffer[SimpleRecord]] =
    (0 until numPartitions).map { i => (i, mutable.ArrayBuffer[SimpleRecord]()) }.toMap

  @Before
  override def setUp() {
    super.setUp()

    EasyMock.expect(zkClient.getTopicPartitionCount(TRANSACTION_STATE_TOPIC_NAME))
      .andReturn(Some(numPartitions))
      .anyTimes()
    EasyMock.replay(zkClient)

    txnStateManager = new TransactionStateManager(0, zkClient, scheduler, replicaManager, txnConfig, time)
    for (i <- 0 until numPartitions)
      txnStateManager.addLoadedTransactionsToCache(i, coordinatorEpoch, new Pool[String, TransactionMetadata]())

    val producerId = 11
    val pidManager: ProducerIdManager = EasyMock.createNiceMock(classOf[ProducerIdManager])
    EasyMock.expect(pidManager.generateProducerId())
      .andReturn(producerId)
      .anyTimes()
    val txnMarkerPurgatory = new DelayedOperationPurgatory[DelayedTxnMarker]("txn-purgatory-name",
      new MockTimer,
      reaperEnabled = false)
    val brokerNode = new Node(0, "host", 10)
    val metadataCache: MetadataCache = EasyMock.createNiceMock(classOf[MetadataCache])
    EasyMock.expect(metadataCache.getPartitionLeaderEndpoint(
      EasyMock.anyString(),
      EasyMock.anyInt(),
      EasyMock.anyObject())
    ).andReturn(Some(brokerNode)).anyTimes()
    val networkClient: NetworkClient = EasyMock.createNiceMock(classOf[NetworkClient])
    txnMarkerChannelManager = new TransactionMarkerChannelManager(
      KafkaConfig.fromProps(serverProps),
      metadataCache,
      networkClient,
      txnStateManager,
      txnMarkerPurgatory,
      time) {
        override def shutdown(): Unit = {
          txnMarkerPurgatory.shutdown()
        }
    }

    transactionCoordinator = new TransactionCoordinator(brokerId = 0,
      txnConfig,
      scheduler,
      pidManager,
      txnStateManager,
      txnMarkerChannelManager,
      time,
      new LogContext)
    EasyMock.replay(pidManager)
    EasyMock.replay(metadataCache)
    EasyMock.replay(networkClient)
  }

  @After
  override def tearDown() {
    try {
      EasyMock.reset(zkClient, replicaManager)
      transactionCoordinator.shutdown()
    } finally {
      super.tearDown()
    }
  }

  @Test
  def testConcurrentGoodPathSequence(): Unit = {
    verifyConcurrentOperations(createTransactions, allOperations)
  }

  @Test
  def testConcurrentRandomSequences(): Unit = {
    verifyConcurrentRandomSequences(createTransactions, allOperations)
  }

  /**
    * Concurrently load one set of transaction state topic partitions and unload another
    * set of partitions. This tests partition leader changes of transaction state topic
    * that are handled by different threads concurrently. Verifies that the metadata of
    * unloaded partitions are removed from the transaction manager and that the transactions
    * from the newly loaded partitions are loaded correctly.
    */
  @Test
  def testConcurrentLoadUnloadPartitions(): Unit = {
    val partitionsToLoad = (0 until numPartitions / 2).toSet
    val partitionsToUnload = (numPartitions / 2 until numPartitions).toSet
    verifyConcurrentActions(loadUnloadActions(partitionsToLoad, partitionsToUnload))
  }

  /**
    * Concurrently load one set of transaction state topic partitions, unload a second set
    * of partitions and expire transactions on a third set of partitions. This tests partition
    * leader changes of transaction state topic that are handled by different threads concurrently
    * while expiry is performed on another thread. Verifies the state of transactions on all the partitions.
    */
  @Test
  def testConcurrentTransactionExpiration(): Unit = {
    val partitionsToLoad = (0 until numPartitions / 3).toSet
    val partitionsToUnload = (numPartitions / 3 until numPartitions * 2 / 3).toSet
    val partitionsWithExpiringTxn = (numPartitions * 2 / 3 until numPartitions).toSet
    val expiringTransactions = allTransactions.filter { txn =>
      partitionsWithExpiringTxn.contains(txnStateManager.partitionFor(txn.transactionalId))
    }.toSet
    val expireAction = new ExpireTransactionsAction(expiringTransactions)
    verifyConcurrentActions(loadUnloadActions(partitionsToLoad, partitionsToUnload) + expireAction)
  }

  override def enableCompletion(): Unit = {
    super.enableCompletion()

    def createResponse(request: WriteTxnMarkersRequest): WriteTxnMarkersResponse  = {
      val pidErrorMap = request.markers.asScala.map { marker =>
        (marker.producerId.asInstanceOf[java.lang.Long], marker.partitions.asScala.map { tp => (tp, Errors.NONE) }.toMap.asJava)
      }.toMap.asJava
      new WriteTxnMarkersResponse(pidErrorMap)
    }
    synchronized {
      txnMarkerChannelManager.generateRequests().foreach { requestAndHandler =>
        val request = requestAndHandler.request.asInstanceOf[WriteTxnMarkersRequest.Builder].build()
        val response = createResponse(request)
        requestAndHandler.handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
          null, null, 0, 0, false, null, null, response))
      }
    }
  }

  /**
    * Concurrently load `partitionsToLoad` and unload `partitionsToUnload`. Before the concurrent operations
    * are run `partitionsToLoad` must be unloaded first since all partitions were loaded during setUp.
    */
  private def loadUnloadActions(partitionsToLoad: Set[Int], partitionsToUnload: Set[Int]): Set[Action] = {
    val transactions = (1 to 10).flatMap(i => createTransactions(s"testConcurrentLoadUnloadPartitions$i-")).toSet
    transactions.foreach(txn => prepareTransaction(txn))
    val unload = partitionsToLoad.map(new UnloadTxnPartitionAction(_))
    unload.foreach(_.run())
    unload.foreach(_.await())
    partitionsToLoad.map(new LoadTxnPartitionAction(_)) ++ partitionsToUnload.map(new UnloadTxnPartitionAction(_))
  }

  private def createTransactions(txnPrefix: String): Set[Transaction] = {
    val transactions = (0 until nTransactions).map { i => new Transaction(s"$txnPrefix$i", i, time) }
    allTransactions ++= transactions
    transactions.toSet
  }

  private def verifyTransaction(txn: Transaction, expectedState: TransactionState): Unit = {
    val (metadata, success) = TestUtils.computeUntilTrue({
      enableCompletion()
      transactionMetadata(txn)
    })(metadata => metadata.nonEmpty && metadata.forall(m => m.state == expectedState && m.pendingState.isEmpty))
    assertTrue(s"Invalid metadata state $metadata", success)
  }

  private def transactionMetadata(txn: Transaction): Option[TransactionMetadata] = {
    txnStateManager.getTransactionState(txn.transactionalId) match {
      case Left(error) =>
        if (error == Errors.NOT_COORDINATOR)
          None
        else
          throw new AssertionError(s"Unexpected transaction error $error for $txn")
      case Right(Some(metadata)) =>
        Some(metadata.transactionMetadata)
      case Right(None) =>
        None
    }
  }

  private def prepareTransaction(txn: Transaction): Unit = {
    val partitionId = txnStateManager.partitionFor(txn.transactionalId)
    val txnRecords = txnRecordsByPartition(partitionId)
    val initPidOp = new InitProducerIdOperation()
    val addPartitionsOp = new AddPartitionsToTxnOperation(Set(new TopicPartition("topic", 0)))
      initPidOp.run(txn)
      initPidOp.awaitAndVerify(txn)
      addPartitionsOp.run(txn)
      addPartitionsOp.awaitAndVerify(txn)

      val txnMetadata = transactionMetadata(txn).getOrElse(throw new IllegalStateException(s"Transaction not found $txn"))
      txnRecords += new SimpleRecord(txn.txnMessageKeyBytes, TransactionLog.valueToBytes(txnMetadata.prepareNoTransit()))

      txnMetadata.state = PrepareCommit
      txnRecords += new SimpleRecord(txn.txnMessageKeyBytes, TransactionLog.valueToBytes(txnMetadata.prepareNoTransit()))

      prepareTxnLog(partitionId)
  }

  private def prepareTxnLog(partitionId: Int): Unit = {

    val logMock: Log =  EasyMock.mock(classOf[Log])
    val fileRecordsMock: FileRecords = EasyMock.mock(classOf[FileRecords])

    val topicPartition = new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, partitionId)
    val startOffset = replicaManager.getLogEndOffset(topicPartition).getOrElse(20L)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE, txnRecordsByPartition(partitionId): _*)
    val endOffset = startOffset + records.records.asScala.size

    EasyMock.expect(logMock.logStartOffset).andStubReturn(startOffset)
    EasyMock.expect(logMock.read(EasyMock.eq(startOffset),
      maxLength = EasyMock.anyInt(),
      maxOffset = EasyMock.eq(None),
      minOneMessage = EasyMock.eq(true),
      includeAbortedTxns = EasyMock.eq(false)))
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

    EasyMock.replay(logMock, fileRecordsMock)
    synchronized {
      replicaManager.updateLog(topicPartition, logMock, endOffset)
    }
  }

  abstract class TxnOperation[R] extends Operation {
    @volatile var result: Option[R] = None
    def resultCallback(r: R): Unit = this.result = Some(r)
  }

  class InitProducerIdOperation extends TxnOperation[InitProducerIdResult] {
    override def run(txn: Transaction): Unit = {
      transactionCoordinator.handleInitProducerId(txn.transactionalId, 60000, resultCallback)
    }
    override def awaitAndVerify(txn: Transaction): Unit = {
      val initPidResult = result.getOrElse(throw new IllegalStateException("InitProducerId has not completed"))
      assertEquals(Errors.NONE, initPidResult.error)
      verifyTransaction(txn, Empty)
    }
  }

  class AddPartitionsToTxnOperation(partitions: Set[TopicPartition]) extends TxnOperation[Errors] {
    override def run(txn: Transaction): Unit = {
      transactionMetadata(txn).foreach { txnMetadata =>
        transactionCoordinator.handleAddPartitionsToTransaction(txn.transactionalId,
            txnMetadata.producerId,
            txnMetadata.producerEpoch,
            partitions,
            resultCallback)
      }
    }
    override def awaitAndVerify(txn: Transaction): Unit = {
      val error = result.getOrElse(throw new IllegalStateException("AddPartitionsToTransaction has not completed"))
      assertEquals(Errors.NONE, error)
      verifyTransaction(txn, Ongoing)
    }
  }

  class EndTxnOperation extends TxnOperation[Errors] {
    override def run(txn: Transaction): Unit = {
      transactionMetadata(txn).foreach { txnMetadata =>
        transactionCoordinator.handleEndTransaction(txn.transactionalId,
          txnMetadata.producerId,
          txnMetadata.producerEpoch,
          transactionResult(txn),
          resultCallback)
      }
    }
    override def awaitAndVerify(txn: Transaction): Unit = {
      val error = result.getOrElse(throw new IllegalStateException("EndTransaction has not completed"))
      if (!txn.ended) {
        txn.ended = true
        assertEquals(Errors.NONE, error)
        val expectedState = if (transactionResult(txn) == TransactionResult.COMMIT) CompleteCommit else CompleteAbort
        verifyTransaction(txn, expectedState)
      } else
        assertEquals(Errors.INVALID_TXN_STATE, error)
    }
    // Test both commit and abort. Transactional ids used in the test have the format <prefix><index>
    // Use the last digit of the index to decide between commit and abort.
    private def transactionResult(txn: Transaction): TransactionResult = {
      val txnId = txn.transactionalId
      val lastDigit = txnId(txnId.length - 1).toInt
      if (lastDigit % 2 == 0) TransactionResult.COMMIT else TransactionResult.ABORT
    }
  }

  class LoadTxnPartitionAction(txnTopicPartitionId: Int) extends Action {
    override def run(): Unit = {
      transactionCoordinator.handleTxnImmigration(txnTopicPartitionId, coordinatorEpoch)
    }
    override def await(): Unit = {
      allTransactions.foreach { txn =>
        if (txnStateManager.partitionFor(txn.transactionalId) == txnTopicPartitionId) {
          verifyTransaction(txn, CompleteCommit)
        }
      }
    }
  }

  class UnloadTxnPartitionAction(txnTopicPartitionId: Int) extends Action {
    val txnRecords: mutable.ArrayBuffer[SimpleRecord] = mutable.ArrayBuffer[SimpleRecord]()
    override def run(): Unit = {
      transactionCoordinator.handleTxnEmigration(txnTopicPartitionId, coordinatorEpoch)
    }
    override def await(): Unit = {
      allTransactions.foreach { txn =>
        if (txnStateManager.partitionFor(txn.transactionalId) == txnTopicPartitionId)
          assertTrue("Transaction metadata not removed", transactionMetadata(txn).isEmpty)
      }
    }
  }

  class ExpireTransactionsAction(transactions: Set[Transaction]) extends Action {
    override def run(): Unit = {
      transactions.foreach { txn =>
        transactionMetadata(txn).foreach { txnMetadata =>
          txnMetadata.txnLastUpdateTimestamp = time.milliseconds() - txnConfig.transactionalIdExpirationMs
        }
      }
      txnStateManager.enableTransactionalIdExpiration()
      time.sleep(txnConfig.removeExpiredTransactionalIdsIntervalMs + 1)
    }

    override def await(): Unit = {
      val (_, success) = TestUtils.computeUntilTrue({
        replicaManager.tryCompleteDelayedRequests()
        transactions.forall(txn => transactionMetadata(txn).isEmpty)
      })(identity)
      assertTrue("Transaction not expired", success)
    }
  }
}

object TransactionCoordinatorConcurrencyTest {

  class Transaction(val transactionalId: String, producerId: Long, time: MockTime) extends CoordinatorMember {
    val txnMessageKeyBytes: Array[Byte] = TransactionLog.keyToBytes(transactionalId)
    @volatile var ended = false
    override def toString: String = transactionalId
  }
}
