/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.log

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, StandardOpenOption}
import java.util.{Collections, Optional, OptionalLong}
import java.util.concurrent.atomic.AtomicInteger
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{MockTime, Utils}
import org.apache.kafka.storage.internals.log.{AppendOrigin, CompletedTxn, LogFileUtils, LogOffsetMetadata, ProducerAppendInfo, ProducerStateEntry, ProducerStateManager, ProducerStateManagerConfig, TxnMetadata, VerificationStateEntry}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.{mock, when}

import java.util
import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.jdk.CollectionConverters._

class ProducerStateManagerTest {
  private var logDir: File = _
  private var stateManager: ProducerStateManager = _
  private val partition = new TopicPartition("test", 0)
  private val producerId = 1L
  private val maxTransactionTimeoutMs = 5 * 60 * 1000
  private val producerStateManagerConfig = new ProducerStateManagerConfig(kafka.server.Defaults.ProducerIdExpirationMs, true)
  private val lateTransactionTimeoutMs = maxTransactionTimeoutMs + ProducerStateManager.LATE_TRANSACTION_BUFFER_MS
  private val time = new MockTime

  @BeforeEach
  def setUp(): Unit = {
    logDir = TestUtils.tempDir()
    stateManager = new ProducerStateManager(partition, logDir, maxTransactionTimeoutMs,
      producerStateManagerConfig, time)
  }

  @AfterEach
  def tearDown(): Unit = {
    Utils.delete(logDir)
  }

  @Test
  def testBasicIdMapping(): Unit = {
    val epoch = 0.toShort

    // First entry for id 0 added
    append(stateManager, producerId, epoch, 0, 0L, 0L)

    // Second entry for id 0 added
    append(stateManager, producerId, epoch, 1, 0L, 1L)

    // Duplicates are checked separately and should result in OutOfOrderSequence if appended
    assertThrows(classOf[OutOfOrderSequenceException], () => append(stateManager, producerId, epoch, 1, 0L, 1L))

    // Invalid sequence number (greater than next expected sequence number)
    assertThrows(classOf[OutOfOrderSequenceException], () => append(stateManager, producerId, epoch, 5, 0L, 2L))

    // Change epoch
    append(stateManager, producerId, (epoch + 1).toShort, 0, 0L, 3L)

    // Incorrect epoch
    assertThrows(classOf[InvalidProducerEpochException], () => append(stateManager, producerId, epoch, 0, 0L, 4L))
  }

  @Test
  def testAppendTxnMarkerWithNoProducerState(): Unit = {
    val producerEpoch = 2.toShort
    appendEndTxnMarker(stateManager, producerId, producerEpoch, ControlRecordType.COMMIT, offset = 27L)

    val firstEntry = stateManager.lastEntry(producerId).orElseThrow(() => new RuntimeException("Expected last entry to be defined"))
    assertEquals(producerEpoch, firstEntry.producerEpoch)
    assertEquals(producerId, firstEntry.producerId)
    assertEquals(RecordBatch.NO_SEQUENCE, firstEntry.lastSeq)

    // Fencing should continue to work even if the marker is the only thing left
    assertThrows(classOf[InvalidProducerEpochException], () => append(stateManager, producerId, 0.toShort, 0, 0L, 4L))

    // If the transaction marker is the only thing left in the log, then an attempt to write using a
    // non-zero sequence number should cause an OutOfOrderSequenceException, so that the producer can reset its state
    assertThrows(classOf[OutOfOrderSequenceException], () => append(stateManager, producerId, producerEpoch, 17, 0L, 4L))

    // The broker should accept the request if the sequence number is reset to 0
    append(stateManager, producerId, producerEpoch, 0, 39L, 4L)
    val secondEntry = stateManager.lastEntry(producerId).orElseThrow(() => new RuntimeException("Expected last entry to be defined"))
    assertEquals(producerEpoch, secondEntry.producerEpoch)
    assertEquals(producerId, secondEntry.producerId)
    assertEquals(0, secondEntry.lastSeq)
  }

  @Test
  def testProducerSequenceWrapAround(): Unit = {
    val epoch = 15.toShort
    val sequence = Int.MaxValue
    val offset = 735L
    append(stateManager, producerId, epoch, sequence, offset, origin = AppendOrigin.REPLICATION)

    append(stateManager, producerId, epoch, 0, offset + 500)

    val maybeLastEntry = stateManager.lastEntry(producerId)
    assertTrue(maybeLastEntry.isPresent)

    val lastEntry = maybeLastEntry.get
    assertEquals(epoch, lastEntry.producerEpoch)

    assertEquals(Int.MaxValue, lastEntry.firstSeq)
    assertEquals(0, lastEntry.lastSeq)
  }

  @Test
  def testProducerSequenceWithWrapAroundBatchRecord(): Unit = {
    val epoch = 15.toShort

    val appendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.REPLICATION)
    // Sequence number wrap around
    appendInfo.appendDataBatch(epoch, Int.MaxValue - 10, 9, time.milliseconds(),
      new LogOffsetMetadata(2000L), 2020L, false)
    assertEquals(Optional.empty(), stateManager.lastEntry(producerId))
    stateManager.update(appendInfo)
    assertTrue(stateManager.lastEntry(producerId).isPresent)

    val lastEntry = stateManager.lastEntry(producerId).get
    assertEquals(Int.MaxValue-10, lastEntry.firstSeq)
    assertEquals(9, lastEntry.lastSeq)
    assertEquals(2000L, lastEntry.firstDataOffset)
    assertEquals(2020L, lastEntry.lastDataOffset)
  }

  @Test
  def testProducerSequenceInvalidWrapAround(): Unit = {
    val epoch = 15.toShort
    val sequence = Int.MaxValue
    val offset = 735L
    append(stateManager, producerId, epoch, sequence, offset, origin = AppendOrigin.REPLICATION)
    assertThrows(classOf[OutOfOrderSequenceException], () => append(stateManager, producerId, epoch, 1, offset + 500))
  }

  @Test
  def testNoValidationOnFirstEntryWhenLoadingLog(): Unit = {
    val epoch = 5.toShort
    val sequence = 16
    val offset = 735L
    append(stateManager, producerId, epoch, sequence, offset, origin = AppendOrigin.REPLICATION)

    val maybeLastEntry = stateManager.lastEntry(producerId)
    assertTrue(maybeLastEntry.isPresent)

    val lastEntry = maybeLastEntry.get
    assertEquals(epoch, lastEntry.producerEpoch)
    assertEquals(sequence, lastEntry.firstSeq)
    assertEquals(sequence, lastEntry.lastSeq)
    assertEquals(offset, lastEntry.lastDataOffset)
    assertEquals(offset, lastEntry.firstDataOffset)
  }

  @Test
  def testControlRecordBumpsProducerEpoch(): Unit = {
    val producerEpoch = 0.toShort
    append(stateManager, producerId, producerEpoch, 0, 0L)

    val bumpedProducerEpoch = 1.toShort
    appendEndTxnMarker(stateManager, producerId, bumpedProducerEpoch, ControlRecordType.ABORT, 1L)

    val maybeLastEntry = stateManager.lastEntry(producerId)
    assertTrue(maybeLastEntry.isPresent())

    val lastEntry = maybeLastEntry.get
    assertEquals(bumpedProducerEpoch, lastEntry.producerEpoch)
    assertEquals(OptionalLong.empty(), lastEntry.currentTxnFirstOffset)
    assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.firstSeq)
    assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.lastSeq)

    // should be able to append with the new epoch if we start at sequence 0
    append(stateManager, producerId, bumpedProducerEpoch, 0, 2L)
    assertEquals(Optional.of(0L), stateManager.lastEntry(producerId).map[Long](_.firstSeq))
  }

  @Test
  def testTxnFirstOffsetMetadataCached(): Unit = {
    val producerEpoch = 0.toShort
    val offset = 992342L
    val seq = 0
    val producerAppendInfo = new ProducerAppendInfo(partition, producerId, ProducerStateEntry.empty(producerId), AppendOrigin.CLIENT,
      stateManager.maybeCreateVerificationStateEntry(producerId, seq, producerEpoch))

    val firstOffsetMetadata = new LogOffsetMetadata(offset, 990000L, 234224)
    producerAppendInfo.appendDataBatch(producerEpoch, seq, seq, time.milliseconds(),
      firstOffsetMetadata, offset, true)
    stateManager.update(producerAppendInfo)

    assertEquals(Optional.of(firstOffsetMetadata), stateManager.firstUnstableOffset())
  }

  @Test
  def testSkipEmptyTransactions(): Unit = {
    val producerEpoch = 0.toShort
    val coordinatorEpoch = 27
    val seq = new AtomicInteger(0)

    def appendEndTxn(
      recordType: ControlRecordType,
      offset: Long,
      appendInfo: ProducerAppendInfo
    ): Option[CompletedTxn] = {
      appendInfo.appendEndTxnMarker(new EndTransactionMarker(recordType, coordinatorEpoch),
        producerEpoch, offset, time.milliseconds()).asScala
    }

    def appendData(
      startOffset: Long,
      endOffset: Long,
      appendInfo: ProducerAppendInfo
    ): Unit = {
      val count = (endOffset - startOffset).toInt
      appendInfo.appendDataBatch(producerEpoch, seq.get(), seq.addAndGet(count), time.milliseconds(),
        new LogOffsetMetadata(startOffset), endOffset, true)
      seq.incrementAndGet()
    }

    // Start one transaction in a separate append
    val firstAppend = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT)
    appendData(16L, 20L, firstAppend)
    assertTxnMetadataEquals(new TxnMetadata(producerId, 16L), firstAppend.startedTransactions.get(0))
    stateManager.update(firstAppend)
    stateManager.onHighWatermarkUpdated(21L)
    assertEquals(Optional.of(new LogOffsetMetadata(16L)), stateManager.firstUnstableOffset)

    // Now do a single append which completes the old transaction, mixes in
    // some empty transactions, one non-empty complete transaction, and one
    // incomplete transaction
    val secondAppend = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT)
    val firstCompletedTxn = appendEndTxn(ControlRecordType.COMMIT, 21, secondAppend)
    assertEquals(Some(new CompletedTxn(producerId, 16L, 21, false)), firstCompletedTxn)
    assertEquals(None, appendEndTxn(ControlRecordType.COMMIT, 22, secondAppend))
    assertEquals(None, appendEndTxn(ControlRecordType.ABORT, 23, secondAppend))
    appendData(24L, 27L, secondAppend)
    val secondCompletedTxn = appendEndTxn(ControlRecordType.ABORT, 28L, secondAppend)
    assertTrue(secondCompletedTxn.isDefined)
    assertEquals(None, appendEndTxn(ControlRecordType.ABORT, 29L, secondAppend))
    appendData(30L, 31L, secondAppend)

    val size = secondAppend.startedTransactions.size
    assertEquals(2, size)
    assertTxnMetadataEquals(new TxnMetadata(producerId, new LogOffsetMetadata(24L)), secondAppend.startedTransactions.get(0))
    assertTxnMetadataEquals(new TxnMetadata(producerId, new LogOffsetMetadata(30L)), secondAppend.startedTransactions.get(size - 1))
    stateManager.update(secondAppend)
    stateManager.completeTxn(firstCompletedTxn.get)
    stateManager.completeTxn(secondCompletedTxn.get)
    stateManager.onHighWatermarkUpdated(32L)
    assertEquals(Optional.of(new LogOffsetMetadata(30L)), stateManager.firstUnstableOffset)
  }

  def assertTxnMetadataEquals(expected: java.util.List[TxnMetadata], actual: java.util.List[TxnMetadata]): Unit = {
    val expectedIter = expected.iterator()
    val actualIter = actual.iterator()
    assertEquals(expected.size(), actual.size())
    while (expectedIter.hasNext && actualIter.hasNext) {
      assertTxnMetadataEquals(expectedIter.next(), actualIter.next())
    }
  }

  def assertTxnMetadataEquals(expected: TxnMetadata, actual: TxnMetadata): Unit = {
    assertEquals(expected.producerId, actual.producerId)
    assertEquals(expected.firstOffset, actual.firstOffset)
    assertEquals(expected.lastOffset, actual.lastOffset)
  }

  @Test
  def testHasLateTransaction(): Unit = {
    val producerId1 = 39L
    val epoch1 = 2.toShort

    val producerId2 = 57L
    val epoch2 = 9.toShort

    // Start two transactions with a delay between them
    append(stateManager, producerId1, epoch1, seq = 0, offset = 100, isTransactional = true)
    assertFalse(stateManager.hasLateTransaction(time.milliseconds()))

    time.sleep(500)
    append(stateManager, producerId2, epoch2, seq = 0, offset = 150, isTransactional = true)
    assertFalse(stateManager.hasLateTransaction(time.milliseconds()))

    // Only the first transaction is late
    time.sleep(lateTransactionTimeoutMs - 500 + 1)
    assertTrue(stateManager.hasLateTransaction(time.milliseconds()))

    // Both transactions are now late
    time.sleep(500)
    assertTrue(stateManager.hasLateTransaction(time.milliseconds()))

    // Finish the first transaction
    appendEndTxnMarker(stateManager, producerId1, epoch1, ControlRecordType.COMMIT, offset = 200)
    assertTrue(stateManager.hasLateTransaction(time.milliseconds()))

    // Now finish the second transaction
    appendEndTxnMarker(stateManager, producerId2, epoch2, ControlRecordType.COMMIT, offset = 250)
    assertFalse(stateManager.hasLateTransaction(time.milliseconds()))
  }

  @Test
  def testHasLateTransactionInitializedAfterReload(): Unit = {
    val producerId1 = 39L
    val epoch1 = 2.toShort

    val producerId2 = 57L
    val epoch2 = 9.toShort

    // Start two transactions with a delay between them
    append(stateManager, producerId1, epoch1, seq = 0, offset = 100, isTransactional = true)
    assertFalse(stateManager.hasLateTransaction(time.milliseconds()))

    time.sleep(500)
    append(stateManager, producerId2, epoch2, seq = 0, offset = 150, isTransactional = true)
    assertFalse(stateManager.hasLateTransaction(time.milliseconds()))

    // Take a snapshot and reload the state
    stateManager.takeSnapshot()
    time.sleep(lateTransactionTimeoutMs - 500 + 1)
    assertTrue(stateManager.hasLateTransaction(time.milliseconds()))

    // After reloading from the snapshot, the transaction should still be considered late
    val reloadedStateManager = new ProducerStateManager(partition, logDir, maxTransactionTimeoutMs,
      producerStateManagerConfig, time)
    reloadedStateManager.truncateAndReload(0L, stateManager.mapEndOffset, time.milliseconds())
    assertTrue(reloadedStateManager.hasLateTransaction(time.milliseconds()))
  }

  @Test
  def testHasLateTransactionUpdatedAfterPartialTruncation(): Unit = {
    val producerId = 39L
    val epoch = 2.toShort

    // Start one transaction and sleep until it is late
    append(stateManager, producerId, epoch, seq = 0, offset = 100, isTransactional = true)
    assertFalse(stateManager.hasLateTransaction(time.milliseconds()))
    time.sleep(lateTransactionTimeoutMs + 1)
    assertTrue(stateManager.hasLateTransaction(time.milliseconds()))

    // After truncation, the ongoing transaction will be cleared
    stateManager.truncateAndReload(0, 80, time.milliseconds())
    assertFalse(stateManager.hasLateTransaction(time.milliseconds()))
  }

  @Test
  def testHasLateTransactionUpdatedAfterFullTruncation(): Unit = {
    val producerId = 39L
    val epoch = 2.toShort

    // Start one transaction and sleep until it is late
    append(stateManager, producerId, epoch, seq = 0, offset = 100, isTransactional = true)
    assertFalse(stateManager.hasLateTransaction(time.milliseconds()))
    time.sleep(lateTransactionTimeoutMs + 1)
    assertTrue(stateManager.hasLateTransaction(time.milliseconds()))

    // After truncation, the ongoing transaction will be cleared
    stateManager.truncateFullyAndStartAt(150L)
    assertFalse(stateManager.hasLateTransaction(time.milliseconds()))
  }

  @Test
  def testLastStableOffsetCompletedTxn(): Unit = {
    val producerEpoch = 0.toShort
    val segmentBaseOffset = 990000L

    def beginTxn(producerId: Long, startOffset: Long): Unit = {
      val relativeOffset = (startOffset - segmentBaseOffset).toInt
      val producerAppendInfo = new ProducerAppendInfo(
        partition,
        producerId,
        ProducerStateEntry.empty(producerId),
        AppendOrigin.CLIENT,
        stateManager.maybeCreateVerificationStateEntry(producerId, 0, producerEpoch)
      )
      val firstOffsetMetadata = new LogOffsetMetadata(startOffset, segmentBaseOffset, 50 * relativeOffset)
      producerAppendInfo.appendDataBatch(producerEpoch, 0, 0, time.milliseconds(),
        firstOffsetMetadata, startOffset, true)
      stateManager.update(producerAppendInfo)
    }

    val producerId1 = producerId
    val startOffset1 = 992342L
    beginTxn(producerId1, startOffset1)

    val producerId2 = producerId + 1
    val startOffset2 = startOffset1 + 25
    beginTxn(producerId2, startOffset2)

    val producerId3 = producerId + 2
    val startOffset3 = startOffset1 + 57
    beginTxn(producerId3, startOffset3)

    val lastOffset1 = startOffset3 + 15
    val completedTxn1 = new CompletedTxn(producerId1, startOffset1, lastOffset1, false)
    assertEquals(startOffset2, stateManager.lastStableOffset(completedTxn1))
    stateManager.completeTxn(completedTxn1)
    stateManager.onHighWatermarkUpdated(lastOffset1 + 1)

    assertEquals(Optional.of(startOffset2), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))

    val lastOffset3 = lastOffset1 + 20
    val completedTxn3 = new CompletedTxn(producerId3, startOffset3, lastOffset3, false)
    assertEquals(startOffset2, stateManager.lastStableOffset(completedTxn3))
    stateManager.completeTxn(completedTxn3)
    stateManager.onHighWatermarkUpdated(lastOffset3 + 1)
    assertEquals(Optional.of(startOffset2), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))

    val lastOffset2 = lastOffset3 + 78
    val completedTxn2 = new CompletedTxn(producerId2, startOffset2, lastOffset2, false)
    assertEquals(lastOffset2 + 1, stateManager.lastStableOffset(completedTxn2))
    stateManager.completeTxn(completedTxn2)
    stateManager.onHighWatermarkUpdated(lastOffset2 + 1)
    assertEquals(Optional.empty(), stateManager.firstUnstableOffset)
  }

  @Test
  def testPrepareUpdateDoesNotMutate(): Unit = {
    val producerEpoch = 0.toShort

    val appendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT)
    appendInfo.appendDataBatch(producerEpoch, 0, 5, time.milliseconds(),
      new LogOffsetMetadata(15L), 20L, false)
    assertEquals(Optional.empty(), stateManager.lastEntry(producerId))
    stateManager.update(appendInfo)
    assertTrue(stateManager.lastEntry(producerId).isPresent())

    val nextAppendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT)
    nextAppendInfo.appendDataBatch(producerEpoch, 6, 10, time.milliseconds(),
      new LogOffsetMetadata(26L), 30L, false)
    assertTrue(stateManager.lastEntry(producerId).isPresent())

    var lastEntry = stateManager.lastEntry(producerId).get
    assertEquals(0, lastEntry.firstSeq)
    assertEquals(5, lastEntry.lastSeq)
    assertEquals(20L, lastEntry.lastDataOffset)

    stateManager.update(nextAppendInfo)
    lastEntry = stateManager.lastEntry(producerId).get
    assertEquals(0, lastEntry.firstSeq)
    assertEquals(10, lastEntry.lastSeq)
    assertEquals(30L, lastEntry.lastDataOffset)
  }

  @Test
  def updateProducerTransactionState(): Unit = {
    val producerEpoch = 0.toShort
    val coordinatorEpoch = 15
    val offset = 9L
    append(stateManager, producerId, producerEpoch, 0, offset)

    val appendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT)
    appendInfo.appendDataBatch(producerEpoch, 1, 5, time.milliseconds(),
      new LogOffsetMetadata(16L), 20L, true)
    var lastEntry = appendInfo.toEntry
    assertEquals(producerEpoch, lastEntry.producerEpoch)
    assertEquals(1, lastEntry.firstSeq)
    assertEquals(5, lastEntry.lastSeq)
    assertEquals(16L, lastEntry.firstDataOffset)
    assertEquals(20L, lastEntry.lastDataOffset)
    assertEquals(OptionalLong.of(16L), lastEntry.currentTxnFirstOffset)
    assertTxnMetadataEquals(java.util.Arrays.asList(new TxnMetadata(producerId, 16L)), appendInfo.startedTransactions)

    appendInfo.appendDataBatch(producerEpoch, 6, 10, time.milliseconds(),
      new LogOffsetMetadata(26L), 30L, true)
    lastEntry = appendInfo.toEntry
    assertEquals(producerEpoch, lastEntry.producerEpoch)
    assertEquals(1, lastEntry.firstSeq)
    assertEquals(10, lastEntry.lastSeq)
    assertEquals(16L, lastEntry.firstDataOffset)
    assertEquals(30L, lastEntry.lastDataOffset)
    assertEquals(OptionalLong.of(16L), lastEntry.currentTxnFirstOffset)
    assertTxnMetadataEquals(util.Arrays.asList(new TxnMetadata(producerId, 16L)), appendInfo.startedTransactions)

    val endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch)
    val completedTxnOpt = appendInfo.appendEndTxnMarker(endTxnMarker, producerEpoch, 40L, time.milliseconds())
    assertTrue(completedTxnOpt.isPresent)

    val completedTxn = completedTxnOpt.get
    assertEquals(producerId, completedTxn.producerId)
    assertEquals(16L, completedTxn.firstOffset)
    assertEquals(40L, completedTxn.lastOffset)
    assertFalse(completedTxn.isAborted)

    lastEntry = appendInfo.toEntry
    assertEquals(producerEpoch, lastEntry.producerEpoch)
    // verify that appending the transaction marker doesn't affect the metadata of the cached record batches.
    assertEquals(1, lastEntry.firstSeq)
    assertEquals(10, lastEntry.lastSeq)
    assertEquals(16L, lastEntry.firstDataOffset)
    assertEquals(30L, lastEntry.lastDataOffset)
    assertEquals(coordinatorEpoch, lastEntry.coordinatorEpoch)
    assertEquals(OptionalLong.empty(), lastEntry.currentTxnFirstOffset)
    assertTxnMetadataEquals(java.util.Arrays.asList(new TxnMetadata(producerId, 16L)), appendInfo.startedTransactions)
  }

  @Test
  def testOutOfSequenceAfterControlRecordEpochBump(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, isTransactional = true)
    append(stateManager, producerId, epoch, 1, 1L, isTransactional = true)

    val bumpedEpoch = 1.toShort
    appendEndTxnMarker(stateManager, producerId, bumpedEpoch, ControlRecordType.ABORT, 1L)

    // next append is invalid since we expect the sequence to be reset
    assertThrows(classOf[OutOfOrderSequenceException],
      () => append(stateManager, producerId, bumpedEpoch, 2, 2L, isTransactional = true))

    assertThrows(classOf[OutOfOrderSequenceException],
      () => append(stateManager, producerId, (bumpedEpoch + 1).toShort, 2, 2L, isTransactional = true))

    // Append with the bumped epoch should be fine if starting from sequence 0
    append(stateManager, producerId, bumpedEpoch, 0, 0L, isTransactional = true)
    assertEquals(bumpedEpoch, stateManager.lastEntry(producerId).get.producerEpoch)
    assertEquals(0, stateManager.lastEntry(producerId).get.lastSeq)
  }

  @Test
  def testNonTransactionalAppendWithOngoingTransaction(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, isTransactional = true)
    assertThrows(classOf[InvalidTxnStateException], () => append(stateManager, producerId, epoch, 1, 1L, isTransactional = false))
  }

  @Test
  def testTruncateAndReloadRemovesOutOfRangeSnapshots(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L)
    stateManager.takeSnapshot()
    append(stateManager, producerId, epoch, 1, 1L)
    stateManager.takeSnapshot()
    append(stateManager, producerId, epoch, 2, 2L)
    stateManager.takeSnapshot()
    append(stateManager, producerId, epoch, 3, 3L)
    stateManager.takeSnapshot()
    append(stateManager, producerId, epoch, 4, 4L)
    stateManager.takeSnapshot()

    stateManager.truncateAndReload(1L, 3L, time.milliseconds())

    assertEquals(OptionalLong.of(2L), stateManager.oldestSnapshotOffset)
    assertEquals(OptionalLong.of(3L), stateManager.latestSnapshotOffset)
  }

  @Test
  def testTakeSnapshot(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, 0L)
    append(stateManager, producerId, epoch, 1, 1L, 1L)

    // Take snapshot
    stateManager.takeSnapshot()

    // Check that file exists and it is not empty
    assertEquals(1, logDir.list().length, "Directory doesn't contain a single file as expected")
    assertTrue(logDir.list().head.nonEmpty, "Snapshot file is empty")
  }

  @Test
  def testRecoverFromSnapshotUnfinishedTransaction(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, isTransactional = true)
    append(stateManager, producerId, epoch, 1, 1L, isTransactional = true)

    stateManager.takeSnapshot()
    val recoveredMapping = new ProducerStateManager(partition, logDir,
      maxTransactionTimeoutMs, producerStateManagerConfig, time)
    recoveredMapping.truncateAndReload(0L, 3L, time.milliseconds)

    // The snapshot only persists the last appended batch metadata
    val loadedEntry = recoveredMapping.lastEntry(producerId)
    assertEquals(1, loadedEntry.get.firstDataOffset)
    assertEquals(1, loadedEntry.get.firstSeq)
    assertEquals(1, loadedEntry.get.lastDataOffset)
    assertEquals(1, loadedEntry.get.lastSeq)
    assertEquals(OptionalLong.of(0), loadedEntry.get.currentTxnFirstOffset)

    // entry added after recovery
    append(recoveredMapping, producerId, epoch, 2, 2L, isTransactional = true)
  }

  @Test
  def testRecoverFromSnapshotFinishedTransaction(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, isTransactional = true)
    append(stateManager, producerId, epoch, 1, 1L, isTransactional = true)
    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.ABORT, offset = 2L)

    stateManager.takeSnapshot()
    val recoveredMapping = new ProducerStateManager(partition, logDir,
      maxTransactionTimeoutMs, producerStateManagerConfig, time)
    recoveredMapping.truncateAndReload(0L, 3L, time.milliseconds)

    // The snapshot only persists the last appended batch metadata
    val loadedEntry = recoveredMapping.lastEntry(producerId)
    assertEquals(1, loadedEntry.get.firstDataOffset)
    assertEquals(1, loadedEntry.get.firstSeq)
    assertEquals(1, loadedEntry.get.lastDataOffset)
    assertEquals(1, loadedEntry.get.lastSeq)
    assertEquals(OptionalLong.empty(), loadedEntry.get.currentTxnFirstOffset)
  }

  @Test
  def testRecoverFromSnapshotEmptyTransaction(): Unit = {
    val epoch = 0.toShort
    val appendTimestamp = time.milliseconds()
    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.ABORT,
      offset = 0L, timestamp = appendTimestamp)
    stateManager.takeSnapshot()

    val recoveredMapping = new ProducerStateManager(partition, logDir,
      maxTransactionTimeoutMs, producerStateManagerConfig, time)
    recoveredMapping.truncateAndReload(0L, 1L, time.milliseconds)

    val lastEntry = recoveredMapping.lastEntry(producerId)
    assertTrue(lastEntry.isPresent())
    assertEquals(appendTimestamp, lastEntry.get.lastTimestamp)
    assertEquals(OptionalLong.empty(), lastEntry.get.currentTxnFirstOffset)
  }

  @Test
  def testProducerStateAfterFencingAbortMarker(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, isTransactional = true)
    appendEndTxnMarker(stateManager, producerId, (epoch + 1).toShort, ControlRecordType.ABORT, offset = 1L)

    val lastEntry = stateManager.lastEntry(producerId).get
    assertEquals(OptionalLong.empty(), lastEntry.currentTxnFirstOffset)
    assertEquals(-1, lastEntry.lastDataOffset)
    assertEquals(-1, lastEntry.firstDataOffset)

    // The producer should not be expired because we want to preserve fencing epochs
    stateManager.removeExpiredProducers(time.milliseconds())
    assertTrue(stateManager.lastEntry(producerId).isPresent())
  }

  @Test
  def testRemoveExpiredPidsOnReload(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, 0)
    append(stateManager, producerId, epoch, 1, 1L, 1)

    stateManager.takeSnapshot()
    val recoveredMapping = new ProducerStateManager(partition, logDir,
      maxTransactionTimeoutMs, producerStateManagerConfig, time)
    recoveredMapping.truncateAndReload(0L, 1L, 70000)

    // entry added after recovery. The pid should be expired now, and would not exist in the pid mapping. Hence
    // we should accept the append and add the pid back in
    append(recoveredMapping, producerId, epoch, 2, 2L, 70001)

    assertEquals(1, recoveredMapping.activeProducers.size)
    assertEquals(2, recoveredMapping.activeProducers.values().iterator().next().lastSeq)
    assertEquals(3L, recoveredMapping.mapEndOffset)
  }

  @Test
  def testAcceptAppendWithoutProducerStateOnReplica(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, 0)
    append(stateManager, producerId, epoch, 1, 1L, 1)

    stateManager.takeSnapshot()
    val recoveredMapping = new ProducerStateManager(partition, logDir,
      maxTransactionTimeoutMs, producerStateManagerConfig, time)
    recoveredMapping.truncateAndReload(0L, 1L, 70000)

    val sequence = 2
    // entry added after recovery. The pid should be expired now, and would not exist in the pid mapping. Nonetheless
    // the append on a replica should be accepted with the local producer state updated to the appended value.
    assertFalse(recoveredMapping.activeProducers.containsKey(producerId))
    append(recoveredMapping, producerId, epoch, sequence, 2L, 70001, origin = AppendOrigin.REPLICATION)
    assertTrue(recoveredMapping.activeProducers.containsKey(producerId))
    val producerStateEntry = recoveredMapping.activeProducers.get(producerId)
    assertEquals(epoch, producerStateEntry.producerEpoch)
    assertEquals(sequence, producerStateEntry.firstSeq)
    assertEquals(sequence, producerStateEntry.lastSeq)
  }

  @Test
  def testAcceptAppendWithSequenceGapsOnReplica(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, 0)
    val outOfOrderSequence = 3

    // First we ensure that we raise an OutOfOrderSequenceException is raised when the append comes from a client.
    assertThrows(classOf[OutOfOrderSequenceException], () => append(stateManager, producerId, epoch, outOfOrderSequence, 1L, 1, origin = AppendOrigin.CLIENT))
    assertTrue(stateManager.activeProducers.containsKey(producerId))
    val producerStateEntry = stateManager.activeProducers.get(producerId)
    assertNotNull(producerStateEntry)
    assertEquals(0L, producerStateEntry.lastSeq)

    append(stateManager, producerId, epoch, outOfOrderSequence, 1L, 1, origin = AppendOrigin.REPLICATION)
    val producerStateEntryForReplication = stateManager.activeProducers.get(producerId)
    assertNotNull(producerStateEntryForReplication)
    assertEquals(outOfOrderSequence, producerStateEntryForReplication.lastSeq)
  }

  @Test
  def testDeleteSnapshotsBefore(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L)
    append(stateManager, producerId, epoch, 1, 1L)
    stateManager.takeSnapshot()
    assertEquals(1, logDir.listFiles().length)
    assertEquals(Set(2), currentSnapshotOffsets)

    append(stateManager, producerId, epoch, 2, 2L)
    stateManager.takeSnapshot()
    assertEquals(2, logDir.listFiles().length)
    assertEquals(Set(2, 3), currentSnapshotOffsets)

    stateManager.deleteSnapshotsBefore(3L)
    assertEquals(1, logDir.listFiles().length)
    assertEquals(Set(3), currentSnapshotOffsets)

    stateManager.deleteSnapshotsBefore(4L)
    assertEquals(0, logDir.listFiles().length)
    assertEquals(Set(), currentSnapshotOffsets)
  }

  @Test
  def testTruncateFullyAndStartAt(): Unit = {
    val epoch = 0.toShort

    append(stateManager, producerId, epoch, 0, 0L)
    append(stateManager, producerId, epoch, 1, 1L)
    stateManager.takeSnapshot()
    assertEquals(1, logDir.listFiles().length)
    assertEquals(Set(2), currentSnapshotOffsets)

    append(stateManager, producerId, epoch, 2, 2L)
    stateManager.takeSnapshot()
    assertEquals(2, logDir.listFiles().length)
    assertEquals(Set(2, 3), currentSnapshotOffsets)

    stateManager.truncateFullyAndStartAt(0L)

    assertEquals(0, logDir.listFiles().length)
    assertEquals(Set(), currentSnapshotOffsets)

    append(stateManager, producerId, epoch, 0, 0L)
    stateManager.takeSnapshot()
    assertEquals(1, logDir.listFiles().length)
    assertEquals(Set(1), currentSnapshotOffsets)
  }

  @Test
  def testReloadSnapshots(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 1, 1L)
    append(stateManager, producerId, epoch, 2, 2L)
    stateManager.takeSnapshot()
    val pathAndDataList = logDir.listFiles().map(file => (file.toPath, Files.readAllBytes(file.toPath)))

    append(stateManager, producerId, epoch, 3, 3L)
    append(stateManager, producerId, epoch, 4, 4L)
    stateManager.takeSnapshot()
    assertEquals(2, logDir.listFiles().length)
    assertEquals(Set(3, 5), currentSnapshotOffsets)

    // Truncate to the range (3, 5), this will delete the earlier snapshot until offset 3.
    stateManager.truncateAndReload(3, 5, time.milliseconds())
    assertEquals(1, logDir.listFiles().length)
    assertEquals(Set(5), currentSnapshotOffsets)

    // Add the snapshot files until offset 3 to the log dir.
    pathAndDataList.foreach { case (path, data) => Files.write(path, data) }
    // Cleanup the in-memory snapshots and reload the snapshots from log dir.
    // It loads the earlier written snapshot files from log dir.
    stateManager.truncateFullyAndReloadSnapshots()

    assertEquals(OptionalLong.of(3), stateManager.latestSnapshotOffset)
    assertEquals(Set(3), currentSnapshotOffsets)
  }

  @Test
  def testFirstUnstableOffsetAfterTruncation(): Unit = {
    val epoch = 0.toShort
    val sequence = 0

    append(stateManager, producerId, epoch, sequence, offset = 99, isTransactional = true)
    assertEquals(Optional.of(99L), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))
    stateManager.takeSnapshot()

    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 105)
    stateManager.onHighWatermarkUpdated(106)
    assertEquals(Optional.empty(), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))
    stateManager.takeSnapshot()

    append(stateManager, producerId, epoch, sequence + 1, offset = 106)
    stateManager.truncateAndReload(0L, 106, time.milliseconds())
    assertEquals(Optional.empty(), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))

    stateManager.truncateAndReload(0L, 100L, time.milliseconds())
    assertEquals(Optional.of(99L), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))
  }

  @Test
  def testLoadFromSnapshotRetainsNonExpiredProducers(): Unit = {
    val epoch = 0.toShort
    val pid1 = 1L
    val pid2 = 2L

    append(stateManager, pid1, epoch, 0, 0L)
    append(stateManager, pid2, epoch, 0, 1L)
    stateManager.takeSnapshot()
    assertEquals(2, stateManager.activeProducers.size)

    stateManager.truncateAndReload(1L, 2L, time.milliseconds())
    assertEquals(2, stateManager.activeProducers.size)

    val entry1 = stateManager.lastEntry(pid1)
    assertTrue(entry1.isPresent)
    assertEquals(0, entry1.get.lastSeq)
    assertEquals(0L, entry1.get.lastDataOffset)

    val entry2 = stateManager.lastEntry(pid2)
    assertTrue(entry2.isPresent)
    assertEquals(0, entry2.get.lastSeq)
    assertEquals(1L, entry2.get.lastDataOffset)
  }

  @Test
  def testSkipSnapshotIfOffsetUnchanged(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, 0L)

    stateManager.takeSnapshot()
    assertEquals(1, logDir.listFiles().length)
    assertEquals(Set(1), currentSnapshotOffsets)

    // nothing changed so there should be no new snapshot
    stateManager.takeSnapshot()
    assertEquals(1, logDir.listFiles().length)
    assertEquals(Set(1), currentSnapshotOffsets)
  }

  @Test
  def testPidExpirationTimeout(): Unit = {
    val epoch = 5.toShort
    val sequence = 37
    append(stateManager, producerId, epoch, sequence, 1L)
    time.sleep(producerStateManagerConfig.producerIdExpirationMs + 1)
    stateManager.removeExpiredProducers(time.milliseconds)
    append(stateManager, producerId, epoch, sequence + 1, 2L)
    assertEquals(1, stateManager.activeProducers.size)
    assertEquals(sequence + 1, stateManager.activeProducers.values().iterator().next().lastSeq)
    assertEquals(3L, stateManager.mapEndOffset)
  }

  @Test
  def testFirstUnstableOffset(): Unit = {
    val epoch = 5.toShort
    val sequence = 0

    assertEquals(OptionalLong.empty(), stateManager.firstUndecidedOffset)

    append(stateManager, producerId, epoch, sequence, offset = 99, isTransactional = true)
    assertEquals(OptionalLong.of(99L), stateManager.firstUndecidedOffset)
    assertEquals(Optional.of(99L), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))

    val anotherPid = 2L
    append(stateManager, anotherPid, epoch, sequence, offset = 105, isTransactional = true)
    assertEquals(OptionalLong.of(99L), stateManager.firstUndecidedOffset)
    assertEquals(Optional.of(99L), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))

    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 109)
    assertEquals(OptionalLong.of(105L), stateManager.firstUndecidedOffset)
    assertEquals(Optional.of(99L), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))

    stateManager.onHighWatermarkUpdated(100L)
    assertEquals(Optional.of(99L), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))

    stateManager.onHighWatermarkUpdated(110L)
    assertEquals(Optional.of(105L), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))

    appendEndTxnMarker(stateManager, anotherPid, epoch, ControlRecordType.ABORT, offset = 112)
    assertEquals(OptionalLong.empty(), stateManager.firstUndecidedOffset)
    assertEquals(Optional.of(105L), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))

    stateManager.onHighWatermarkUpdated(113L)
    assertEquals(Optional.empty(), stateManager.firstUnstableOffset.map[Long](x => x.messageOffset))
  }

  @Test
  def testProducersWithOngoingTransactionsDontExpire(): Unit = {
    val epoch = 5.toShort
    val sequence = 0

    append(stateManager, producerId, epoch, sequence, offset = 99, isTransactional = true)
    assertEquals(OptionalLong.of(99L), stateManager.firstUndecidedOffset)

    time.sleep(producerStateManagerConfig.producerIdExpirationMs + 1)
    stateManager.removeExpiredProducers(time.milliseconds)

    assertTrue(stateManager.lastEntry(producerId).isPresent())
    assertEquals(OptionalLong.of(99L), stateManager.firstUndecidedOffset)

    stateManager.removeExpiredProducers(time.milliseconds)
    assertTrue(stateManager.lastEntry(producerId).isPresent)
  }

  @Test
  def testSequenceNotValidatedForGroupMetadataTopic(): Unit = {
    val partition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val stateManager = new ProducerStateManager(partition, logDir,
      maxTransactionTimeoutMs, producerStateManagerConfig, time)

    val epoch = 0.toShort
    append(stateManager, producerId, epoch, RecordBatch.NO_SEQUENCE, offset = 99,
      isTransactional = true, origin = AppendOrigin.COORDINATOR)
    append(stateManager, producerId, epoch, RecordBatch.NO_SEQUENCE, offset = 100,
      isTransactional = true, origin = AppendOrigin.COORDINATOR)
  }

  @Test
  def testOldEpochForControlRecord(): Unit = {
    val epoch = 5.toShort
    val sequence = 0

    assertEquals(OptionalLong.empty(), stateManager.firstUndecidedOffset)

    append(stateManager, producerId, epoch, sequence, offset = 99, isTransactional = true)
    assertThrows(classOf[InvalidProducerEpochException], () => appendEndTxnMarker(stateManager, producerId, 3.toShort,
      ControlRecordType.COMMIT, offset=100))
  }

  @Test
  def testCoordinatorFencing(): Unit = {
    val epoch = 5.toShort
    val sequence = 0

    append(stateManager, producerId, epoch, sequence, offset = 99, isTransactional = true)
    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 100, coordinatorEpoch = 1)

    val lastEntry = stateManager.lastEntry(producerId)
    assertEquals(Optional.of(1), lastEntry.map[Int](x => x.coordinatorEpoch))

    // writing with the current epoch is allowed
    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 101, coordinatorEpoch = 1)

    // bumping the epoch is allowed
    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 102, coordinatorEpoch = 2)

    // old epochs are not allowed
    assertThrows(classOf[TransactionCoordinatorFencedException], () => appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 103, coordinatorEpoch = 1))
  }

  @Test
  def testCoordinatorFencedAfterReload(): Unit = {
    val producerEpoch = 0.toShort
    append(stateManager, producerId, producerEpoch, 0, offset = 99, isTransactional = true)
    appendEndTxnMarker(stateManager, producerId, producerEpoch, ControlRecordType.COMMIT, offset = 100, coordinatorEpoch = 1)
    stateManager.takeSnapshot()

    val recoveredMapping = new ProducerStateManager(partition, logDir,
      maxTransactionTimeoutMs, producerStateManagerConfig, time)
    recoveredMapping.truncateAndReload(0L, 2L, 70000)

    // append from old coordinator should be rejected
    assertThrows(classOf[TransactionCoordinatorFencedException], () => appendEndTxnMarker(stateManager, producerId,
      producerEpoch, ControlRecordType.COMMIT, offset = 100, coordinatorEpoch = 0))
  }

  @Test
  def testLoadFromEmptySnapshotFile(): Unit = {
    testLoadFromCorruptSnapshot { file =>
      file.truncate(0L)
    }
  }

  @Test
  def testLoadFromTruncatedSnapshotFile(): Unit = {
    testLoadFromCorruptSnapshot { file =>
      // truncate to some arbitrary point in the middle of the snapshot
      assertTrue(file.size > 2)
      file.truncate(file.size / 2)
    }
  }

  @Test
  def testLoadFromCorruptSnapshotFile(): Unit = {
    testLoadFromCorruptSnapshot { file =>
      // write some garbage somewhere in the file
      assertTrue(file.size > 2)
      file.write(ByteBuffer.wrap(Array[Byte](37)), file.size / 2)
    }
  }

  @Test
  def testAppendEmptyControlBatch(): Unit = {
    val producerId = 23423L
    val baseOffset = 15

    val batch: RecordBatch = mock(classOf[RecordBatch])
    when(batch.isControlBatch).thenReturn(true)
    when(batch.iterator).thenReturn(Collections.emptyIterator[Record])

    // Appending the empty control batch should not throw and a new transaction shouldn't be started
    append(stateManager, producerId, baseOffset, batch, origin = AppendOrigin.CLIENT)
    assertEquals(OptionalLong.empty(), stateManager.lastEntry(producerId).get.currentTxnFirstOffset)
  }

  @Test
  def testRemoveStraySnapshotsKeepCleanShutdownSnapshot(): Unit = {
    // Test that when stray snapshots are removed, the largest stray snapshot is kept around. This covers the case where
    // the broker shutdown cleanly and emitted a snapshot file larger than the base offset of the active segment.

    // Create 3 snapshot files at different offsets.
    Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 5).toPath) // not stray
    Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 2).toPath) // stray
    Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 42).toPath) // not stray

    // claim that we only have one segment with a base offset of 5
    stateManager.removeStraySnapshots(Collections.singletonList(5))

    // The snapshot file at offset 2 should be considered a stray, but the snapshot at 42 should be kept
    // around because it is the largest snapshot.
    assertEquals(OptionalLong.of(42), stateManager.latestSnapshotOffset)
    assertEquals(OptionalLong.of(5), stateManager.oldestSnapshotOffset)
    assertEquals(Seq(5L, 42L), ProducerStateManager.listSnapshotFiles(logDir).asScala.map(_.offset).sorted)
  }

  @Test
  def testRemoveAllStraySnapshots(): Unit = {
    // Test that when stray snapshots are removed, we remove only the stray snapshots below the largest segment base offset.
    // Snapshots associated with an offset in the list of segment base offsets should remain.

    // Create 3 snapshot files at different offsets.
    Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 5).toPath) // stray
    Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 2).toPath) // stray
    Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 42).toPath) // not stray

    stateManager.removeStraySnapshots(Collections.singletonList(42))
    assertEquals(Seq(42L), ProducerStateManager.listSnapshotFiles(logDir).asScala.map(_.offset).sorted)

  }

  /**
   * Test that removeAndMarkSnapshotForDeletion will rename the SnapshotFile with
   * the deletion suffix and remove it from the producer state.
   */
  @Test
  def testRemoveAndMarkSnapshotForDeletion(): Unit = {
    Files.createFile(LogFileUtils.producerSnapshotFile(logDir, 5).toPath)
    val manager = new ProducerStateManager(partition, logDir, maxTransactionTimeoutMs, producerStateManagerConfig, time)
    assertTrue(manager.latestSnapshotOffset.isPresent)
    val snapshot = manager.removeAndMarkSnapshotForDeletion(5).get
    assertTrue(snapshot.file.toPath.toString.endsWith(LogFileUtils.DELETED_FILE_SUFFIX))
    assertTrue(!manager.latestSnapshotOffset.isPresent)
  }

  /**
   * Test that marking a snapshot for deletion when the file has already been deleted
   * returns None instead of the SnapshotFile. The snapshot file should be removed from
   * the in-memory state of the ProducerStateManager. This scenario can occur during log
   * recovery when the intermediate ProducerStateManager instance deletes a file without
   * updating the state of the "real" ProducerStateManager instance which is passed to the Log.
   */
  @Test
  def testRemoveAndMarkSnapshotForDeletionAlreadyDeleted(): Unit = {
    val file = LogFileUtils.producerSnapshotFile(logDir, 5)
    Files.createFile(file.toPath)
    val manager = new ProducerStateManager(partition, logDir, maxTransactionTimeoutMs, producerStateManagerConfig, time)
    assertTrue(manager.latestSnapshotOffset.isPresent)
    Files.delete(file.toPath)
    assertTrue(!manager.removeAndMarkSnapshotForDeletion(5).isPresent)
    assertTrue(!manager.latestSnapshotOffset.isPresent)
  }

  @Test
  def testEntryForVerification(): Unit = {
    val originalEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 0, 0)
    val originalEntryVerificationGuard = originalEntry.verificationGuard()

    def verifyEntry(producerId: Long, newEntry: VerificationStateEntry): Unit = {
      val entry = stateManager.verificationStateEntry(producerId)
      assertEquals(originalEntryVerificationGuard, entry.verificationGuard)
      assertEquals(entry.verificationGuard, newEntry.verificationGuard)
    }

    // If we already have an entry, reuse it.
    val updatedEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 0, 0)
    verifyEntry(producerId, updatedEntry)

    // Add the transactional data and clear the entry.
    append(stateManager, producerId, 0, 0, offset = 0, isTransactional = true)
    stateManager.clearVerificationStateEntry(producerId)
    assertNull(stateManager.verificationStateEntry(producerId))
  }

  @Test
  def testSequenceAndEpochInVerificationEntry(): Unit = {
    val originalEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 1, 0)
    val originalEntryVerificationGuard = originalEntry.verificationGuard()

    def verifyEntry(producerId: Long, newEntry: VerificationStateEntry, expectedSequence: Int, expectedEpoch: Short): Unit = {
      val entry = stateManager.verificationStateEntry(producerId)
      assertEquals(originalEntryVerificationGuard, entry.verificationGuard)
      assertEquals(entry.verificationGuard, newEntry.verificationGuard)
      assertEquals(expectedSequence, entry.lowestSequence)
      assertEquals(expectedEpoch, entry.epoch)
    }
    verifyEntry(producerId, originalEntry, 1, 0)

    // If we see a lower sequence, update to the lower one.
    val updatedEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 0, 0)
    verifyEntry(producerId, updatedEntry, 0, 0)

    // If we see a new epoch that is higher, update the sequence.
    val updatedEntryNewEpoch = stateManager.maybeCreateVerificationStateEntry(producerId, 2, 1)
    verifyEntry(producerId, updatedEntryNewEpoch, 2, 1)

    // Ignore a lower epoch.
    val updatedEntryOldEpoch = stateManager.maybeCreateVerificationStateEntry(producerId, 0, 0)
    verifyEntry(producerId, updatedEntryOldEpoch, 2, 1)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testThrowOutOfOrderSequenceWithVerificationSequenceCheck(dynamicallyDisable: Boolean): Unit = {
    val originalEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 0, 0)

    // Even if we dynamically disable, we should still execute the sequence check if we have an entry
    if (dynamicallyDisable)
      producerStateManagerConfig.setTransactionVerificationEnabled(false)

    // Trying to append with a higher sequence should fail
    assertThrows(classOf[OutOfOrderSequenceException], () => append(stateManager, producerId, 0, 4, offset = 0, isTransactional = true))

    assertEquals(originalEntry, stateManager.verificationStateEntry(producerId))
  }

  @Test
  def testVerificationStateEntryExpiration(): Unit = {
    val originalEntry = stateManager.maybeCreateVerificationStateEntry(producerId, 0, 0)

    // Before timeout we do not remove. Note: Accessing the verification entry does not update the time.
    time.sleep(producerStateManagerConfig.producerIdExpirationMs / 2)
    stateManager.removeExpiredProducers(time.milliseconds())
    assertEquals(originalEntry, stateManager.verificationStateEntry(producerId))

    time.sleep((producerStateManagerConfig.producerIdExpirationMs / 2) + 1)
    stateManager.removeExpiredProducers(time.milliseconds())
    assertNull(stateManager.verificationStateEntry(producerId))
  }

  private def testLoadFromCorruptSnapshot(makeFileCorrupt: FileChannel => Unit): Unit = {
    val epoch = 0.toShort
    val producerId = 1L

    append(stateManager, producerId, epoch, seq = 0, offset = 0L)
    stateManager.takeSnapshot()

    append(stateManager, producerId, epoch, seq = 1, offset = 1L)
    stateManager.takeSnapshot()

    // Truncate the last snapshot
    val latestSnapshotOffset = stateManager.latestSnapshotOffset
    assertEquals(OptionalLong.of(2L), latestSnapshotOffset)
    val snapshotToTruncate = LogFileUtils.producerSnapshotFile(logDir, latestSnapshotOffset.getAsLong)
    val channel = FileChannel.open(snapshotToTruncate.toPath, StandardOpenOption.WRITE)
    try {
      makeFileCorrupt(channel)
    } finally {
      channel.close()
    }

    // Ensure that the truncated snapshot is deleted and producer state is loaded from the previous snapshot
    val reloadedStateManager = new ProducerStateManager(partition, logDir,
      maxTransactionTimeoutMs, producerStateManagerConfig, time)
    reloadedStateManager.truncateAndReload(0L, 20L, time.milliseconds())
    assertFalse(snapshotToTruncate.exists())

    val loadedProducerState = reloadedStateManager.activeProducers.get(producerId)
    assertNotNull(loadedProducerState)
    assertEquals(0L, loadedProducerState.lastDataOffset)
  }

  private def appendEndTxnMarker(mapping: ProducerStateManager,
                                 producerId: Long,
                                 producerEpoch: Short,
                                 controlType: ControlRecordType,
                                 offset: Long,
                                 coordinatorEpoch: Int = 0,
                                 timestamp: Long = time.milliseconds()): Option[CompletedTxn] = {
    val producerAppendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.COORDINATOR)
    val endTxnMarker = new EndTransactionMarker(controlType, coordinatorEpoch)
    val completedTxnOpt = producerAppendInfo.appendEndTxnMarker(endTxnMarker, producerEpoch, offset, timestamp).asScala
    mapping.update(producerAppendInfo)
    completedTxnOpt.foreach(mapping.completeTxn)
    mapping.updateMapEndOffset(offset + 1)
    completedTxnOpt
  }

  private def append(stateManager: ProducerStateManager,
                     producerId: Long,
                     producerEpoch: Short,
                     seq: Int,
                     offset: Long,
                     timestamp: Long = time.milliseconds(),
                     isTransactional: Boolean = false,
                     origin : AppendOrigin = AppendOrigin.CLIENT): Unit = {
    val producerAppendInfo = stateManager.prepareUpdate(producerId, origin)
    producerAppendInfo.appendDataBatch(producerEpoch, seq, seq, timestamp,
      new LogOffsetMetadata(offset), offset, isTransactional)
    stateManager.update(producerAppendInfo)
    stateManager.updateMapEndOffset(offset + 1)
  }

  private def append(stateManager: ProducerStateManager,
                     producerId: Long,
                     offset: Long,
                     batch: RecordBatch,
                     origin: AppendOrigin): Unit = {
    val producerAppendInfo = stateManager.prepareUpdate(producerId, origin)
    producerAppendInfo.append(batch, Optional.empty())
    stateManager.update(producerAppendInfo)
    stateManager.updateMapEndOffset(offset + 1)
  }

  private def currentSnapshotOffsets: Set[Long] =
    logDir.listFiles.map(UnifiedLog.offsetFromFile).toSet

}
