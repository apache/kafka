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
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import kafka.coordinator.AbstractCoordinatorConcurrencyTest
import kafka.coordinator.AbstractCoordinatorConcurrencyTest._
import kafka.coordinator.transaction.TransactionCoordinatorConcurrencyTest._
import kafka.log.{LogConfig, UnifiedLog}
import kafka.server.{FetchDataInfo, FetchLogEnd, KafkaConfig, LogOffsetMetadata, MetadataCache, RequestLocal}
import kafka.utils.{Pool, TestUtils}
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, FileRecords, MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{LogContext, MockTime, ProducerIdAndEpoch}
import org.apache.kafka.common.{Node, TopicPartition}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyInt, anyString}
import org.mockito.Mockito.{mock, when}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, mutable}

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

  val producerId: Long = 11
  private var bumpProducerId = false

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    when(zkClient.getTopicPartitionCount(TRANSACTION_STATE_TOPIC_NAME))
      .thenReturn(Some(numPartitions))

    txnStateManager = new TransactionStateManager(0, scheduler, replicaManager, txnConfig, time,
      new Metrics())
    txnStateManager.startup(() => zkClient.getTopicPartitionCount(TRANSACTION_STATE_TOPIC_NAME).get,
      enableTransactionalIdExpiration = true)
    for (i <- 0 until numPartitions)
      txnStateManager.addLoadedTransactionsToCache(i, coordinatorEpoch, new Pool[String, TransactionMetadata]())

    val pidGenerator: ProducerIdManager = mock(classOf[ProducerIdManager])
    when(pidGenerator.generateProducerId())
      .thenAnswer(_ => if (bumpProducerId) producerId + 1 else producerId)
    val brokerNode = new Node(0, "host", 10)
    val metadataCache: MetadataCache = mock(classOf[MetadataCache])
    when(metadataCache.getPartitionLeaderEndpoint(
      anyString,
      anyInt,
      any[ListenerName])
    ).thenReturn(Some(brokerNode))
    val networkClient: NetworkClient = mock(classOf[NetworkClient])
    txnMarkerChannelManager = new TransactionMarkerChannelManager(
      KafkaConfig.fromProps(serverProps),
      metadataCache,
      networkClient,
      txnStateManager,
      time)

    transactionCoordinator = new TransactionCoordinator(
      txnConfig,
      scheduler,
      () => pidGenerator,
      txnStateManager,
      txnMarkerChannelManager,
      time,
      new LogContext)
  }

  @AfterEach
  override def tearDown(): Unit = {
    try {
      transactionCoordinator.shutdown()
    } finally {
      super.tearDown()
    }
  }

  @Test
  def testConcurrentGoodPathWithConcurrentPartitionLoading(): Unit = {
    // This is a somewhat contrived test case which reproduces the bug in KAFKA-9777.
    // When a new partition needs to be loaded, we acquire the write lock in order to
    // add the partition to the set of loading partitions. We should still be able to
    // make progress with transactions even while this is ongoing.

    val keepRunning = new AtomicBoolean(true)
    val t = new Thread() {
      override def run(): Unit = {
        while (keepRunning.get()) {
          txnStateManager.addLoadingPartition(numPartitions + 1, coordinatorEpoch)
        }
      }
    }
    t.start()

    verifyConcurrentOperations(createTransactions, allOperations)
    keepRunning.set(false)
    t.join()
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

  @Test
  def testConcurrentNewInitProducerIdRequests(): Unit = {
    val transactions = (1 to 100).flatMap(i => createTransactions(s"testConcurrentInitProducerID$i-"))
    bumpProducerId = true
    transactions.foreach { txn =>
      val txnMetadata = prepareExhaustedEpochTxnMetadata(txn)
      txnStateManager.putTransactionStateIfNotExists(txnMetadata)

      // Test simultaneous requests from an existing producer trying to bump the epoch and a new producer initializing
      val newProducerOp1 = new InitProducerIdOperation()
      val newProducerOp2 = new InitProducerIdOperation()
      verifyConcurrentActions(Set(newProducerOp1, newProducerOp2).map(_.actionNoVerify(txn)))

      // If only one request succeeds, assert that the epoch was successfully increased
      // If both requests succeed, the new producer must have run after the existing one and should have the higher epoch
      (newProducerOp1.result.get.error, newProducerOp2.result.get.error) match {
        case (Errors.NONE, Errors.NONE) =>
          assertNotEquals(newProducerOp1.result.get.producerEpoch, newProducerOp2.result.get.producerEpoch)
          // assertEquals(0, newProducerOp1.result.get.producerEpoch)
          // assertEquals(0, newProducerOp2.result.get.producerEpoch)
        case (Errors.NONE, _) =>
          assertEquals(0, newProducerOp1.result.get.producerEpoch)
        case (_, Errors.NONE) =>
          assertEquals(0, newProducerOp2.result.get.producerEpoch)
        case (_, _) => fail("One of two InitProducerId requests should succeed")
      }
    }
  }

  @Test
  def testConcurrentInitProducerIdRequestsOneNewOneContinuing(): Unit = {
    val transactions = (1 to 10).flatMap(i => createTransactions(s"testConcurrentInitProducerID$i-"))
    transactions.foreach { txn =>
      val firstInitReq = new InitProducerIdOperation()
      firstInitReq.run(txn)
      firstInitReq.awaitAndVerify(txn)

      // Test simultaneous requests from an existing producer trying to bump the epoch and a new producer initializing
      val producerIdAndEpoch = new ProducerIdAndEpoch(firstInitReq.result.get.producerId, firstInitReq.result.get.producerEpoch)
      val bumpEpochOp = new InitProducerIdOperation(Some(producerIdAndEpoch))
      val newProducerOp = new InitProducerIdOperation()
      verifyConcurrentActions(Set(bumpEpochOp, newProducerOp).map(_.actionNoVerify(txn)))

      // If only one request succeeds, assert that the epoch was successfully increased
      // If both requests succeed, the new producer must have run after the existing one and should have the higher epoch
      (bumpEpochOp.result.get.error, newProducerOp.result.get.error) match {
        case (Errors.NONE, Errors.NONE) =>
          assertEquals(producerIdAndEpoch.epoch + 2, newProducerOp.result.get.producerEpoch)
          assertEquals(producerIdAndEpoch.epoch + 1, bumpEpochOp.result.get.producerEpoch)
        case (Errors.NONE, _) =>
          assertEquals(producerIdAndEpoch.epoch + 1, bumpEpochOp.result.get.producerEpoch)
        case (_, Errors.NONE) =>
          assertEquals(producerIdAndEpoch.epoch + 1, newProducerOp.result.get.producerEpoch)
        case (_, _) => fail("One of two InitProducerId requests should succeed")
      }
    }
  }

  @Test
  def testConcurrentContinuingInitProducerIdRequests(): Unit = {
    val transactions = (1 to 100).flatMap(i => createTransactions(s"testConcurrentInitProducerID$i-"))
    transactions.foreach { txn =>
      // Test simultaneous requests from an existing producers trying to re-initialize when no state is present
      val producerIdAndEpoch = new ProducerIdAndEpoch(producerId, 10)
      val bumpEpochOp1 = new InitProducerIdOperation(Some(producerIdAndEpoch))
      val bumpEpochOp2 = new InitProducerIdOperation(Some(producerIdAndEpoch))
      verifyConcurrentActions(Set(bumpEpochOp1, bumpEpochOp2).map(_.actionNoVerify(txn)))

      // If only one request succeeds, assert that the epoch was successfully increased
      // If both requests succeed, the new producer must have run after the existing one and should have the higher epoch
      (bumpEpochOp1.result.get.error, bumpEpochOp2.result.get.error) match {
        case (Errors.NONE, Errors.NONE) =>
          fail("One of two InitProducerId requests should fail due to concurrent requests or non-matching epochs")
        case (Errors.NONE, _) =>
          assertEquals(0, bumpEpochOp1.result.get.producerEpoch)
        case (_, Errors.NONE) =>
          assertEquals(0, bumpEpochOp2.result.get.producerEpoch)
        case (_, _) => fail("One of two InitProducerId requests should succeed")
      }
    }
  }

  @Test
  def testConcurrentInitProducerIdRequestsWithRetry(): Unit = {
    val transactions = (1 to 10).flatMap(i => createTransactions(s"testConcurrentInitProducerID$i-"))
    transactions.foreach { txn =>
      val firstInitReq = new InitProducerIdOperation()
      firstInitReq.run(txn)
      firstInitReq.awaitAndVerify(txn)

      val initialProducerIdAndEpoch = new ProducerIdAndEpoch(firstInitReq.result.get.producerId, firstInitReq.result.get.producerEpoch)
      val bumpEpochReq = new InitProducerIdOperation(Some(initialProducerIdAndEpoch))
      bumpEpochReq.run(txn)
      bumpEpochReq.awaitAndVerify(txn)

      // Test simultaneous requests from an existing producer retrying the epoch bump and a new producer initializing
      val bumpedProducerIdAndEpoch = new ProducerIdAndEpoch(bumpEpochReq.result.get.producerId, bumpEpochReq.result.get.producerEpoch)
      val retryBumpEpochOp = new InitProducerIdOperation(Some(initialProducerIdAndEpoch))
      val newProducerOp = new InitProducerIdOperation()
      verifyConcurrentActions(Set(retryBumpEpochOp, newProducerOp).map(_.actionNoVerify(txn)))

      // If both requests succeed, the new producer must have run after the existing one and should have the higher epoch
      // If the retry succeeds and the new producer doesn't, assert that the already-bumped epoch was returned
      // If the new producer succeeds and the retry doesn't, assert the epoch was bumped
      (retryBumpEpochOp.result.get.error, newProducerOp.result.get.error) match {
        case (Errors.NONE, Errors.NONE) =>
          assertEquals(bumpedProducerIdAndEpoch.epoch + 1, newProducerOp.result.get.producerEpoch)
          assertEquals(bumpedProducerIdAndEpoch.epoch, retryBumpEpochOp.result.get.producerEpoch)
        case (Errors.NONE, _) =>
          assertEquals(bumpedProducerIdAndEpoch.epoch, retryBumpEpochOp.result.get.producerEpoch)
        case (_, Errors.NONE) =>
          assertEquals(bumpedProducerIdAndEpoch.epoch + 1, newProducerOp.result.get.producerEpoch)
        case (_, _) => fail("At least one InitProducerId request should succeed")
      }
    }
  }

  @Test
  def testConcurrentInitProducerRequestsAtPidBoundary(): Unit = {
    val transactions = (1 to 10).flatMap(i => createTransactions(s"testConcurrentInitProducerID$i-"))
    bumpProducerId = true
    transactions.foreach { txn =>
      val txnMetadata = prepareExhaustedEpochTxnMetadata(txn)
      txnStateManager.putTransactionStateIfNotExists(txnMetadata)

      // Test simultaneous requests from an existing producer attempting to bump the epoch and a new producer initializing
      val bumpEpochOp = new InitProducerIdOperation(Some(new ProducerIdAndEpoch(producerId, (Short.MaxValue - 1).toShort)))
      val newProducerOp = new InitProducerIdOperation()
      verifyConcurrentActions(Set(bumpEpochOp, newProducerOp).map(_.actionNoVerify(txn)))

      // If the retry succeeds and the new producer doesn't, assert that the already-bumped epoch was returned
      // If the new producer succeeds and the retry doesn't, assert the epoch was bumped
      // If both requests succeed, the new producer must have run after the existing one and should have the higher epoch
      (bumpEpochOp.result.get.error, newProducerOp.result.get.error) match {
        case (Errors.NONE, Errors.NONE) =>
          assertEquals(0, bumpEpochOp.result.get.producerEpoch)
          assertEquals(producerId + 1, bumpEpochOp.result.get.producerId)

          assertEquals(1, newProducerOp.result.get.producerEpoch)
          assertEquals(producerId + 1, newProducerOp.result.get.producerId)
        case (Errors.NONE, _) =>
          assertEquals(0, bumpEpochOp.result.get.producerEpoch)
          assertEquals(producerId + 1, bumpEpochOp.result.get.producerId)
        case (_, Errors.NONE) =>
          assertEquals(0, newProducerOp.result.get.producerEpoch)
          assertEquals(producerId + 1, newProducerOp.result.get.producerId)
        case (_, _) => fail("One of two InitProducerId requests should succeed")
      }
    }

    bumpProducerId = false
  }

  @Test
  def testConcurrentInitProducerRequestsWithRetryAtPidBoundary(): Unit = {
    val transactions = (1 to 10).flatMap(i => createTransactions(s"testConcurrentInitProducerID$i-"))
    bumpProducerId = true
    transactions.foreach { txn =>
      val txnMetadata = prepareExhaustedEpochTxnMetadata(txn)
      txnStateManager.putTransactionStateIfNotExists(txnMetadata)

      val bumpEpochReq = new InitProducerIdOperation(Some(new ProducerIdAndEpoch(producerId, (Short.MaxValue - 1).toShort)))
      bumpEpochReq.run(txn)
      bumpEpochReq.awaitAndVerify(txn)

      // Test simultaneous requests from an existing producer attempting to bump the epoch and a new producer initializing
      val retryBumpEpochOp = new InitProducerIdOperation(Some(new ProducerIdAndEpoch(producerId, (Short.MaxValue - 1).toShort)))
      val newProducerOp = new InitProducerIdOperation()
      verifyConcurrentActions(Set(retryBumpEpochOp, newProducerOp).map(_.actionNoVerify(txn)))

      // If the retry succeeds and the new producer doesn't, assert that the already-bumped epoch was returned
      // If the new producer succeeds and the retry doesn't, assert the epoch was bumped
      // If both requests succeed, the new producer must have run after the existing one and should have the higher epoch
      (retryBumpEpochOp.result.get.error, newProducerOp.result.get.error) match {
        case (Errors.NONE, Errors.NONE) =>
          assertEquals(0, retryBumpEpochOp.result.get.producerEpoch)
          assertEquals(producerId + 1, retryBumpEpochOp.result.get.producerId)

          assertEquals(1, newProducerOp.result.get.producerEpoch)
          assertEquals(producerId + 1, newProducerOp.result.get.producerId)
        case (Errors.NONE, _) =>
          assertEquals(0, retryBumpEpochOp.result.get.producerEpoch)
          assertEquals(producerId + 1, retryBumpEpochOp.result.get.producerId)
        case (_, Errors.NONE) =>
          assertEquals(1, newProducerOp.result.get.producerEpoch)
          assertEquals(producerId + 1, newProducerOp.result.get.producerId)
        case (_, _) => fail("One of two InitProducerId requests should succeed")
      }
    }

    bumpProducerId = false
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
    assertTrue(success, s"Invalid metadata state $metadata")
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
    val logMock: UnifiedLog = mock(classOf[UnifiedLog])
    when(logMock.config).thenReturn(new LogConfig(Collections.emptyMap()))

    val fileRecordsMock: FileRecords = mock(classOf[FileRecords])

    val topicPartition = new TopicPartition(TRANSACTION_STATE_TOPIC_NAME, partitionId)
    val startOffset = replicaManager.getLogEndOffset(topicPartition).getOrElse(20L)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE, txnRecordsByPartition(partitionId).toArray: _*)
    val endOffset = startOffset + records.records.asScala.size

    when(logMock.logStartOffset).thenReturn(startOffset)
    when(logMock.read(ArgumentMatchers.eq(startOffset),
      maxLength = anyInt,
      isolation = ArgumentMatchers.eq(FetchLogEnd),
      minOneMessage = ArgumentMatchers.eq(true)))
      .thenReturn(FetchDataInfo(LogOffsetMetadata(startOffset), fileRecordsMock))

    when(fileRecordsMock.sizeInBytes()).thenReturn(records.sizeInBytes)
    val bufferCaptor: ArgumentCaptor[ByteBuffer] = ArgumentCaptor.forClass(classOf[ByteBuffer])
    when(fileRecordsMock.readInto(bufferCaptor.capture(), anyInt)).thenAnswer(_ => {
      val buffer = bufferCaptor.getValue
      buffer.put(records.buffer.duplicate)
      buffer.flip()
    })

    synchronized {
      replicaManager.updateLog(topicPartition, logMock, endOffset)
    }
  }

  private def prepareExhaustedEpochTxnMetadata(txn: Transaction): TransactionMetadata = {
    new TransactionMetadata(transactionalId = txn.transactionalId,
      producerId = producerId,
      lastProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = (Short.MaxValue - 1).toShort,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 60000,
      state = Empty,
      topicPartitions = collection.mutable.Set.empty[TopicPartition],
      txnLastUpdateTimestamp = time.milliseconds())
  }

  abstract class TxnOperation[R] extends Operation {
    @volatile var result: Option[R] = None
    def resultCallback(r: R): Unit = this.result = Some(r)
  }

  class InitProducerIdOperation(val producerIdAndEpoch: Option[ProducerIdAndEpoch] = None) extends TxnOperation[InitProducerIdResult] {
    override def run(txn: Transaction): Unit = {
      transactionCoordinator.handleInitProducerId(txn.transactionalId, 60000, producerIdAndEpoch, resultCallback,
        RequestLocal.withThreadConfinedCaching)
      replicaManager.tryCompleteActions()
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
            resultCallback,
            RequestLocal.withThreadConfinedCaching)
        replicaManager.tryCompleteActions()
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
          resultCallback,
          RequestLocal.withThreadConfinedCaching)
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
      transactionCoordinator.onElection(txnTopicPartitionId, coordinatorEpoch)
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
      transactionCoordinator.onResignation(txnTopicPartitionId, Some(coordinatorEpoch))
    }
    override def await(): Unit = {
      allTransactions.foreach { txn =>
        if (txnStateManager.partitionFor(txn.transactionalId) == txnTopicPartitionId)
          assertTrue(transactionMetadata(txn).isEmpty, "Transaction metadata not removed")
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
      replicaManager.tryCompleteActions()
      time.sleep(txnConfig.removeExpiredTransactionalIdsIntervalMs + 1)
    }

    override def await(): Unit = {
      val (_, success) = TestUtils.computeUntilTrue({
        replicaManager.tryCompleteActions()
        transactions.forall(txn => transactionMetadata(txn).isEmpty)
      })(identity)
      assertTrue(success, "Transaction not expired")
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
