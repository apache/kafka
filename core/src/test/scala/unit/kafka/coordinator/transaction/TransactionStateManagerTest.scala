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

import kafka.common.Topic.TransactionStateTopicName
import kafka.log.Log
import kafka.server.{FetchDataInfo, LogOffsetMetadata, ReplicaManager}
import kafka.utils.{MockScheduler, ZkUtils}
import kafka.utils.TestUtils.fail
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, FileRecords, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{After, Before, Test}
import org.easymock.EasyMock

import scala.collection.mutable
import scala.collection.JavaConverters._

class TransactionStateManagerTest {

  val partitionId = 0
  val numPartitions = 2
  val transactionTimeoutMs: Int = 1000
  val topicPartition = new TopicPartition(TransactionStateTopicName, partitionId)

  val txnRecords: mutable.ArrayBuffer[SimpleRecord] = mutable.ArrayBuffer[SimpleRecord]()

  val time = new MockTime()
  val scheduler = new MockScheduler(time)
  val zkUtils: ZkUtils = EasyMock.createNiceMock(classOf[ZkUtils])
  val replicaManager: ReplicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])

  EasyMock.expect(zkUtils.getTopicPartitionCount(TransactionStateTopicName))
    .andReturn(Some(numPartitions))
    .anyTimes()

  EasyMock.replay(zkUtils)

  val transactionManager: TransactionStateManager = new TransactionStateManager(0, zkUtils, scheduler, replicaManager, TransactionConfig(), time)

  val txnId1: String = "one"
  val txnId2: String = "two"
  val txnMessageKeyBytes1: Array[Byte] = TransactionLog.keyToBytes(txnId1)
  val txnMessageKeyBytes2: Array[Byte] = TransactionLog.keyToBytes(txnId2)
  val pidMappings: Map[String, Long] = Map[String, Long](txnId1 -> 1L, txnId2 -> 2L)
  var txnMetadata1: TransactionMetadata = new TransactionMetadata(pidMappings(txnId1), 1, transactionTimeoutMs)
  var txnMetadata2: TransactionMetadata = new TransactionMetadata(pidMappings(txnId2), 1, transactionTimeoutMs)

  @Before
  def setUp() {
    // make sure the transactional id hashes to the assigning partition id
    assertEquals(partitionId, transactionManager.partitionFor(txnId1))
    assertEquals(partitionId, transactionManager.partitionFor(txnId2))
  }

  @After
  def tearDown() {
    EasyMock.reset(zkUtils, replicaManager)
    transactionManager.shutdown()
  }

  @Test
  def testAddGetPids() {
    assertEquals(None, transactionManager.getTransaction(txnId1))
    assertEquals(txnMetadata1, transactionManager.addTransaction(txnId1, txnMetadata1))
    assertEquals(Some(txnMetadata1), transactionManager.getTransaction(txnId1))
    assertEquals(txnMetadata1, transactionManager.addTransaction(txnId1, txnMetadata2))
  }

  @Test
  def testLoadAndRemoveTransactionsForPartition() {
    // generate transaction log messages for two pids traces:

    // pid1's transaction started with two partitions
    txnMetadata1.state = Ongoing
    txnMetadata1.addPartitions(Set[TopicPartition](new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1))

    // pid1's transaction adds three more partitions
    txnMetadata1.addPartitions(Set[TopicPartition](new TopicPartition("topic2", 0),
      new TopicPartition("topic2", 1),
      new TopicPartition("topic2", 2)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1))

    // pid1's transaction is preparing to commit
    txnMetadata1.state = PrepareCommit

    txnRecords += new SimpleRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(txnMetadata1))

    // pid2's transaction started with three partitions
    txnMetadata2.state = Ongoing
    txnMetadata2.addPartitions(Set[TopicPartition](new TopicPartition("topic3", 0),
      new TopicPartition("topic3", 1),
      new TopicPartition("topic3", 2)))

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2))

    // pid2's transaction is preparing to abort
    txnMetadata2.state = PrepareAbort

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2))

    // pid2's transaction has aborted
    txnMetadata2.state = CompleteAbort

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2))

    // pid2's epoch has advanced, with no ongoing transaction yet
    txnMetadata2.state = Empty
    txnMetadata2.topicPartitions.clear()

    txnRecords += new SimpleRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(txnMetadata2))

    val startOffset = 15L   // it should work for any start offset
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE, txnRecords: _*)

    prepareTxnLog(topicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    // this partition should not be part of the owned partitions
    assertFalse(transactionManager.isCoordinatorFor(txnId1))
    assertFalse(transactionManager.isCoordinatorFor(txnId2))

    transactionManager.loadTransactionsForPartition(partitionId)

    // let the time advance to trigger the background thread loading
    scheduler.tick()

    val cachedPidMetadata1 = transactionManager.getTransaction(txnId1).getOrElse(fail(txnId1 + "'s transaction state was not loaded into the cache"))
    val cachedPidMetadata2 = transactionManager.getTransaction(txnId2).getOrElse(fail(txnId2 + "'s transaction state was not loaded into the cache"))

    // they should be equal to the latest status of the transaction
    assertEquals(txnMetadata1, cachedPidMetadata1)
    assertEquals(txnMetadata2, cachedPidMetadata2)

    // this partition should now be part of the owned partitions
    assertTrue(transactionManager.isCoordinatorFor(txnId1))
    assertTrue(transactionManager.isCoordinatorFor(txnId2))

    transactionManager.removeTransactionsForPartition(partitionId)

    // let the time advance to trigger the background thread removing
    scheduler.tick()

    assertFalse(transactionManager.isCoordinatorFor(txnId1))
    assertFalse(transactionManager.isCoordinatorFor(txnId2))

    assertEquals(None, transactionManager.getTransaction(txnId1))
    assertEquals(None, transactionManager.getTransaction(txnId2))
  }

  private def prepareTxnLog(topicPartition: TopicPartition,
                            startOffset: Long,
                            records: MemoryRecords): Unit = {
    val logMock =  EasyMock.mock(classOf[Log])
    val fileRecordsMock = EasyMock.mock(classOf[FileRecords])

    val endOffset = startOffset + records.records.asScala.size

    EasyMock.expect(replicaManager.getLog(topicPartition)).andStubReturn(Some(logMock))
    EasyMock.expect(replicaManager.getHighWatermark(topicPartition)).andStubReturn(Some(endOffset))

    EasyMock.expect(logMock.logStartOffset).andStubReturn(Some(startOffset))
    EasyMock.expect(logMock.read(EasyMock.eq(startOffset), EasyMock.anyInt(), EasyMock.eq(None), EasyMock.eq(true)))
      .andReturn(FetchDataInfo(LogOffsetMetadata(startOffset), fileRecordsMock))
    EasyMock.expect(fileRecordsMock.readInto(EasyMock.anyObject(classOf[ByteBuffer]), EasyMock.anyInt()))
      .andReturn(records.buffer)

    EasyMock.replay(logMock, fileRecordsMock)
  }
}
