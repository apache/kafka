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
import java.nio.file.StandardOpenOption
import java.util.Collections

import kafka.server.LogOffsetMetadata
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{MockTime, Utils}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

class ProducerStateManagerTest extends JUnitSuite {
  var logDir: File = null
  var stateManager: ProducerStateManager = null
  val partition = new TopicPartition("test", 0)
  val producerId = 1L
  val maxPidExpirationMs = 60 * 1000
  val time = new MockTime

  @Before
  def setUp(): Unit = {
    logDir = TestUtils.tempDir()
    stateManager = new ProducerStateManager(partition, logDir, maxPidExpirationMs)
  }

  @After
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
    assertThrows[OutOfOrderSequenceException] {
      append(stateManager, producerId, epoch, 1, 0L, 1L)
    }

    // Invalid sequence number (greater than next expected sequence number)
    assertThrows[OutOfOrderSequenceException] {
      append(stateManager, producerId, epoch, 5, 0L, 2L)
    }

    // Change epoch
    append(stateManager, producerId, (epoch + 1).toShort, 0, 0L, 3L)

    // Incorrect epoch
    assertThrows[ProducerFencedException] {
      append(stateManager, producerId, epoch, 0, 0L, 4L)
    }
  }

  @Test
  def testAppendTxnMarkerWithNoProducerState(): Unit = {
    val producerEpoch = 2.toShort
    appendEndTxnMarker(stateManager, producerId, producerEpoch, ControlRecordType.COMMIT, offset = 27L)

    val firstEntry = stateManager.lastEntry(producerId).getOrElse(fail("Expected last entry to be defined"))
    assertEquals(producerEpoch, firstEntry.producerEpoch)
    assertEquals(producerId, firstEntry.producerId)
    assertEquals(RecordBatch.NO_SEQUENCE, firstEntry.lastSeq)

    // Fencing should continue to work even if the marker is the only thing left
    assertThrows[ProducerFencedException] {
      append(stateManager, producerId, 0.toShort, 0, 0L, 4L)
    }

    // If the transaction marker is the only thing left in the log, then an attempt to write using a
    // non-zero sequence number should cause an UnknownProducerId, so that the producer can reset its state
    assertThrows[UnknownProducerIdException] {
      append(stateManager, producerId, producerEpoch, 17, 0L, 4L)
    }

    // The broker should accept the request if the sequence number is reset to 0
    append(stateManager, producerId, producerEpoch, 0, 39L, 4L)
    val secondEntry = stateManager.lastEntry(producerId).getOrElse(fail("Expected last entry to be defined"))
    assertEquals(producerEpoch, secondEntry.producerEpoch)
    assertEquals(producerId, secondEntry.producerId)
    assertEquals(0, secondEntry.lastSeq)
  }

  @Test
  def testProducerSequenceWrapAround(): Unit = {
    val epoch = 15.toShort
    val sequence = Int.MaxValue
    val offset = 735L
    append(stateManager, producerId, epoch, sequence, offset, isFromClient = false)

    append(stateManager, producerId, epoch, 0, offset + 500)

    val maybeLastEntry = stateManager.lastEntry(producerId)
    assertTrue(maybeLastEntry.isDefined)

    val lastEntry = maybeLastEntry.get
    assertEquals(epoch, lastEntry.producerEpoch)

    assertEquals(Int.MaxValue, lastEntry.firstSeq)
    assertEquals(0, lastEntry.lastSeq)
  }

  @Test
  def testProducerSequenceWithWrapAroundBatchRecord(): Unit = {
    val epoch = 15.toShort

    val appendInfo = stateManager.prepareUpdate(producerId, isFromClient = false)
    // Sequence number wrap around
    appendInfo.append(epoch, Int.MaxValue-10, 9, time.milliseconds(), 2000L, 2020L, isTransactional = false)
    assertEquals(None, stateManager.lastEntry(producerId))
    stateManager.update(appendInfo)
    assertTrue(stateManager.lastEntry(producerId).isDefined)

    val lastEntry = stateManager.lastEntry(producerId).get
    assertEquals(Int.MaxValue-10, lastEntry.firstSeq)
    assertEquals(9, lastEntry.lastSeq)
    assertEquals(2000L, lastEntry.firstOffset)
    assertEquals(2020L, lastEntry.lastDataOffset)
  }

  @Test(expected = classOf[OutOfOrderSequenceException])
  def testProducerSequenceInvalidWrapAround(): Unit = {
    val epoch = 15.toShort
    val sequence = Int.MaxValue
    val offset = 735L
    append(stateManager, producerId, epoch, sequence, offset, isFromClient = false)
    append(stateManager, producerId, epoch, 1, offset + 500)
  }

  @Test
  def testNoValidationOnFirstEntryWhenLoadingLog(): Unit = {
    val epoch = 5.toShort
    val sequence = 16
    val offset = 735L
    append(stateManager, producerId, epoch, sequence, offset, isFromClient = false)

    val maybeLastEntry = stateManager.lastEntry(producerId)
    assertTrue(maybeLastEntry.isDefined)

    val lastEntry = maybeLastEntry.get
    assertEquals(epoch, lastEntry.producerEpoch)
    assertEquals(sequence, lastEntry.firstSeq)
    assertEquals(sequence, lastEntry.lastSeq)
    assertEquals(offset, lastEntry.lastDataOffset)
    assertEquals(offset, lastEntry.firstOffset)
  }

  @Test
  def testControlRecordBumpsEpoch(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L)

    val bumpedEpoch = 1.toShort
    val (completedTxn, lastStableOffset) = appendEndTxnMarker(stateManager, producerId, bumpedEpoch, ControlRecordType.ABORT, 1L)
    assertEquals(1L, completedTxn.firstOffset)
    assertEquals(1L, completedTxn.lastOffset)
    assertEquals(2L, lastStableOffset)
    assertTrue(completedTxn.isAborted)
    assertEquals(producerId, completedTxn.producerId)

    val maybeLastEntry = stateManager.lastEntry(producerId)
    assertTrue(maybeLastEntry.isDefined)

    val lastEntry = maybeLastEntry.get
    assertEquals(bumpedEpoch, lastEntry.producerEpoch)
    assertEquals(None, lastEntry.currentTxnFirstOffset)
    assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.firstSeq)
    assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.lastSeq)

    // should be able to append with the new epoch if we start at sequence 0
    append(stateManager, producerId, bumpedEpoch, 0, 2L)
    assertEquals(Some(0), stateManager.lastEntry(producerId).map(_.firstSeq))
  }

  @Test
  def testTxnFirstOffsetMetadataCached(): Unit = {
    val producerEpoch = 0.toShort
    val offset = 992342L
    val seq = 0
    val producerAppendInfo = new ProducerAppendInfo(partition, producerId, ProducerStateEntry.empty(producerId), ValidationType.Full)
    producerAppendInfo.append(producerEpoch, seq, seq, time.milliseconds(), offset, offset, isTransactional = true)

    val logOffsetMetadata = new LogOffsetMetadata(messageOffset = offset, segmentBaseOffset = 990000L,
      relativePositionInSegment = 234224)
    producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
    stateManager.update(producerAppendInfo)

    assertEquals(Some(logOffsetMetadata), stateManager.firstUnstableOffset)
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
        ValidationType.Full)
      producerAppendInfo.append(producerEpoch, 0, 0, time.milliseconds(), startOffset, startOffset, isTransactional = true)
      val logOffsetMetadata = LogOffsetMetadata(messageOffset = startOffset, segmentBaseOffset = segmentBaseOffset,
        relativePositionInSegment = 50 * relativeOffset)
      producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
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
    val completedTxn1 = CompletedTxn(producerId1, startOffset1, lastOffset1, isAborted = false)
    assertEquals(startOffset2, stateManager.lastStableOffset(completedTxn1))
    stateManager.completeTxn(completedTxn1)
    stateManager.onHighWatermarkUpdated(lastOffset1 + 1)
    assertEquals(Some(startOffset2), stateManager.firstUnstableOffset.map(_.messageOffset))

    val lastOffset3 = lastOffset1 + 20
    val completedTxn3 = CompletedTxn(producerId3, startOffset3, lastOffset3, isAborted = false)
    assertEquals(startOffset2, stateManager.lastStableOffset(completedTxn3))
    stateManager.completeTxn(completedTxn3)
    stateManager.onHighWatermarkUpdated(lastOffset3 + 1)
    assertEquals(Some(startOffset2), stateManager.firstUnstableOffset.map(_.messageOffset))

    val lastOffset2 = lastOffset3 + 78
    val completedTxn2 = CompletedTxn(producerId2, startOffset2, lastOffset2, isAborted = false)
    assertEquals(lastOffset2 + 1, stateManager.lastStableOffset(completedTxn2))
    stateManager.completeTxn(completedTxn2)
    stateManager.onHighWatermarkUpdated(lastOffset2 + 1)
    assertEquals(None, stateManager.firstUnstableOffset)
  }

  @Test
  def testNonMatchingTxnFirstOffsetMetadataNotCached(): Unit = {
    val producerEpoch = 0.toShort
    val offset = 992342L
    val seq = 0
    val producerAppendInfo = new ProducerAppendInfo(partition, producerId, ProducerStateEntry.empty(producerId), ValidationType.Full)
    producerAppendInfo.append(producerEpoch, seq, seq, time.milliseconds(), offset, offset, isTransactional = true)

    // use some other offset to simulate a follower append where the log offset metadata won't typically
    // match any of the transaction first offsets
    val logOffsetMetadata = new LogOffsetMetadata(messageOffset = offset - 23429, segmentBaseOffset = 990000L,
      relativePositionInSegment = 234224)
    producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
    stateManager.update(producerAppendInfo)

    assertEquals(Some(LogOffsetMetadata(offset)), stateManager.firstUnstableOffset)
  }

  @Test
  def testPrepareUpdateDoesNotMutate(): Unit = {
    val producerEpoch = 0.toShort

    val appendInfo = stateManager.prepareUpdate(producerId, isFromClient = true)
    appendInfo.append(producerEpoch, 0, 5, time.milliseconds(), 15L, 20L, isTransactional = false)
    assertEquals(None, stateManager.lastEntry(producerId))
    stateManager.update(appendInfo)
    assertTrue(stateManager.lastEntry(producerId).isDefined)

    val nextAppendInfo = stateManager.prepareUpdate(producerId, isFromClient = true)
    nextAppendInfo.append(producerEpoch, 6, 10, time.milliseconds(), 26L, 30L, isTransactional = false)
    assertTrue(stateManager.lastEntry(producerId).isDefined)

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

    val appendInfo = stateManager.prepareUpdate(producerId, isFromClient = true)
    appendInfo.append(producerEpoch, 1, 5, time.milliseconds(), 16L, 20L, isTransactional = true)
    var lastEntry = appendInfo.toEntry
    assertEquals(producerEpoch, lastEntry.producerEpoch)
    assertEquals(1, lastEntry.firstSeq)
    assertEquals(5, lastEntry.lastSeq)
    assertEquals(16L, lastEntry.firstOffset)
    assertEquals(20L, lastEntry.lastDataOffset)
    assertEquals(Some(16L), lastEntry.currentTxnFirstOffset)
    assertEquals(List(new TxnMetadata(producerId, 16L)), appendInfo.startedTransactions)

    appendInfo.append(producerEpoch, 6, 10, time.milliseconds(), 26L, 30L, isTransactional = true)
    lastEntry = appendInfo.toEntry
    assertEquals(producerEpoch, lastEntry.producerEpoch)
    assertEquals(1, lastEntry.firstSeq)
    assertEquals(10, lastEntry.lastSeq)
    assertEquals(16L, lastEntry.firstOffset)
    assertEquals(30L, lastEntry.lastDataOffset)
    assertEquals(Some(16L), lastEntry.currentTxnFirstOffset)
    assertEquals(List(new TxnMetadata(producerId, 16L)), appendInfo.startedTransactions)

    val endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch)
    val completedTxn = appendInfo.appendEndTxnMarker(endTxnMarker, producerEpoch, 40L, time.milliseconds())
    assertEquals(producerId, completedTxn.producerId)
    assertEquals(16L, completedTxn.firstOffset)
    assertEquals(40L, completedTxn.lastOffset)
    assertFalse(completedTxn.isAborted)

    lastEntry = appendInfo.toEntry
    assertEquals(producerEpoch, lastEntry.producerEpoch)
    // verify that appending the transaction marker doesn't affect the metadata of the cached record batches.
    assertEquals(1, lastEntry.firstSeq)
    assertEquals(10, lastEntry.lastSeq)
    assertEquals(16L, lastEntry.firstOffset)
    assertEquals(30L, lastEntry.lastDataOffset)
    assertEquals(coordinatorEpoch, lastEntry.coordinatorEpoch)
    assertEquals(None, lastEntry.currentTxnFirstOffset)
    assertEquals(List(new TxnMetadata(producerId, 16L)), appendInfo.startedTransactions)
  }

  @Test
  def testOutOfSequenceAfterControlRecordEpochBump(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, isTransactional = true)
    append(stateManager, producerId, epoch, 1, 1L, isTransactional = true)

    val bumpedEpoch = 1.toShort
    appendEndTxnMarker(stateManager, producerId, bumpedEpoch, ControlRecordType.ABORT, 1L)

    // next append is invalid since we expect the sequence to be reset
    assertThrows[OutOfOrderSequenceException] {
      append(stateManager, producerId, bumpedEpoch, 2, 2L, isTransactional = true)
    }

    assertThrows[OutOfOrderSequenceException] {
      append(stateManager, producerId, (bumpedEpoch + 1).toShort, 2, 2L, isTransactional = true)
    }

    // Append with the bumped epoch should be fine if starting from sequence 0
    append(stateManager, producerId, bumpedEpoch, 0, 0L, isTransactional = true)
    assertEquals(bumpedEpoch, stateManager.lastEntry(producerId).get.producerEpoch)
    assertEquals(0, stateManager.lastEntry(producerId).get.lastSeq)
  }

  @Test(expected = classOf[InvalidTxnStateException])
  def testNonTransactionalAppendWithOngoingTransaction(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, isTransactional = true)
    append(stateManager, producerId, epoch, 1, 1L, isTransactional = false)
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

    assertEquals(Some(2L), stateManager.oldestSnapshotOffset)
    assertEquals(Some(3L), stateManager.latestSnapshotOffset)
  }

  @Test
  def testTakeSnapshot(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, 0L)
    append(stateManager, producerId, epoch, 1, 1L, 1L)

    // Take snapshot
    stateManager.takeSnapshot()

    // Check that file exists and it is not empty
    assertEquals("Directory doesn't contain a single file as expected", 1, logDir.list().length)
    assertTrue("Snapshot file is empty", logDir.list().head.length > 0)
  }

  @Test
  def testRecoverFromSnapshot(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L)
    append(stateManager, producerId, epoch, 1, 1L)

    stateManager.takeSnapshot()
    val recoveredMapping = new ProducerStateManager(partition, logDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(0L, 3L, time.milliseconds)

    // entry added after recovery
    append(recoveredMapping, producerId, epoch, 2, 2L)
  }

  @Test(expected = classOf[UnknownProducerIdException])
  def testRemoveExpiredPidsOnReload(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, 0)
    append(stateManager, producerId, epoch, 1, 1L, 1)

    stateManager.takeSnapshot()
    val recoveredMapping = new ProducerStateManager(partition, logDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(0L, 1L, 70000)

    // entry added after recovery. The pid should be expired now, and would not exist in the pid mapping. Hence
    // we should get an out of order sequence exception.
    append(recoveredMapping, producerId, epoch, 2, 2L, 70001)
  }

  @Test
  def testAcceptAppendWithoutProducerStateOnReplica(): Unit = {
    val epoch = 0.toShort
    append(stateManager, producerId, epoch, 0, 0L, 0)
    append(stateManager, producerId, epoch, 1, 1L, 1)

    stateManager.takeSnapshot()
    val recoveredMapping = new ProducerStateManager(partition, logDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(0L, 1L, 70000)

    val sequence = 2
    // entry added after recovery. The pid should be expired now, and would not exist in the pid mapping. Nonetheless
    // the append on a replica should be accepted with the local producer state updated to the appended value.
    assertFalse(recoveredMapping.activeProducers.contains(producerId))
    append(recoveredMapping, producerId, epoch, sequence, 2L, 70001, isFromClient = false)
    assertTrue(recoveredMapping.activeProducers.contains(producerId))
    val producerStateEntry = recoveredMapping.activeProducers.get(producerId).head
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
    try {
      append(stateManager, producerId, epoch, outOfOrderSequence, 1L, 1, isFromClient = true)
      fail("Expected an OutOfOrderSequenceException to be raised.")
    } catch {
      case _ : OutOfOrderSequenceException =>
      // Good!
      case _ : Exception =>
        fail("Expected an OutOfOrderSequenceException to be raised.")
    }

    assertEquals(0L, stateManager.activeProducers(producerId).lastSeq)
    append(stateManager, producerId, epoch, outOfOrderSequence, 1L, 1, isFromClient = false)
    assertEquals(outOfOrderSequence, stateManager.activeProducers(producerId).lastSeq)
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
  def testTruncate(): Unit = {
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

    stateManager.truncate()

    assertEquals(0, logDir.listFiles().length)
    assertEquals(Set(), currentSnapshotOffsets)

    append(stateManager, producerId, epoch, 0, 0L)
    stateManager.takeSnapshot()
    assertEquals(1, logDir.listFiles().length)
    assertEquals(Set(1), currentSnapshotOffsets)
  }

  @Test
  def testFirstUnstableOffsetAfterTruncation(): Unit = {
    val epoch = 0.toShort
    val sequence = 0

    append(stateManager, producerId, epoch, sequence, offset = 99, isTransactional = true)
    assertEquals(Some(99), stateManager.firstUnstableOffset.map(_.messageOffset))
    stateManager.takeSnapshot()

    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 105)
    stateManager.onHighWatermarkUpdated(106)
    assertEquals(None, stateManager.firstUnstableOffset.map(_.messageOffset))
    stateManager.takeSnapshot()

    append(stateManager, producerId, epoch, sequence + 1, offset = 106)
    stateManager.truncateAndReload(0L, 106, time.milliseconds())
    assertEquals(None, stateManager.firstUnstableOffset.map(_.messageOffset))

    stateManager.truncateAndReload(0L, 100L, time.milliseconds())
    assertEquals(Some(99), stateManager.firstUnstableOffset.map(_.messageOffset))
  }

  @Test
  def testFirstUnstableOffsetAfterEviction(): Unit = {
    val epoch = 0.toShort
    val sequence = 0
    append(stateManager, producerId, epoch, sequence, offset = 99, isTransactional = true)
    assertEquals(Some(99), stateManager.firstUnstableOffset.map(_.messageOffset))
    append(stateManager, 2L, epoch, 0, offset = 106, isTransactional = true)
    stateManager.truncateHead(100)
    assertEquals(Some(106), stateManager.firstUnstableOffset.map(_.messageOffset))
  }

  @Test
  def testTruncateHead(): Unit = {
    val epoch = 0.toShort

    append(stateManager, producerId, epoch, 0, 0L)
    append(stateManager, producerId, epoch, 1, 1L)
    stateManager.takeSnapshot()

    val anotherPid = 2L
    append(stateManager, anotherPid, epoch, 0, 2L)
    append(stateManager, anotherPid, epoch, 1, 3L)
    stateManager.takeSnapshot()
    assertEquals(Set(2, 4), currentSnapshotOffsets)

    stateManager.truncateHead(2)
    assertEquals(Set(2, 4), currentSnapshotOffsets)
    assertEquals(Set(anotherPid), stateManager.activeProducers.keySet)
    assertEquals(None, stateManager.lastEntry(producerId))

    val maybeEntry = stateManager.lastEntry(anotherPid)
    assertTrue(maybeEntry.isDefined)
    assertEquals(3L, maybeEntry.get.lastDataOffset)

    stateManager.truncateHead(3)
    assertEquals(Set(anotherPid), stateManager.activeProducers.keySet)
    assertEquals(Set(4), currentSnapshotOffsets)
    assertEquals(4, stateManager.mapEndOffset)

    stateManager.truncateHead(5)
    assertEquals(Set(), stateManager.activeProducers.keySet)
    assertEquals(Set(), currentSnapshotOffsets)
    assertEquals(5, stateManager.mapEndOffset)
  }

  @Test
  def testLoadFromSnapshotRemovesNonRetainedProducers(): Unit = {
    val epoch = 0.toShort
    val pid1 = 1L
    val pid2 = 2L

    append(stateManager, pid1, epoch, 0, 0L)
    append(stateManager, pid2, epoch, 0, 1L)
    stateManager.takeSnapshot()
    assertEquals(2, stateManager.activeProducers.size)

    stateManager.truncateAndReload(1L, 2L, time.milliseconds())
    assertEquals(1, stateManager.activeProducers.size)
    assertEquals(None, stateManager.lastEntry(pid1))

    val entry = stateManager.lastEntry(pid2)
    assertTrue(entry.isDefined)
    assertEquals(0, entry.get.lastSeq)
    assertEquals(1L, entry.get.lastDataOffset)
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
  def testStartOffset(): Unit = {
    val epoch = 0.toShort
    val pid2 = 2L
    append(stateManager, pid2, epoch, 0, 0L, 1L)
    append(stateManager, producerId, epoch, 0, 1L, 2L)
    append(stateManager, producerId, epoch, 1, 2L, 3L)
    append(stateManager, producerId, epoch, 2, 3L, 4L)
    stateManager.takeSnapshot()

    assertThrows[UnknownProducerIdException] {
      val recoveredMapping = new ProducerStateManager(partition, logDir, maxPidExpirationMs)
      recoveredMapping.truncateAndReload(0L, 1L, time.milliseconds)
      append(recoveredMapping, pid2, epoch, 1, 4L, 5L)
    }
  }

  @Test(expected = classOf[UnknownProducerIdException])
  def testPidExpirationTimeout() {
    val epoch = 5.toShort
    val sequence = 37
    append(stateManager, producerId, epoch, sequence, 1L)
    time.sleep(maxPidExpirationMs + 1)
    stateManager.removeExpiredProducers(time.milliseconds)
    append(stateManager, producerId, epoch, sequence + 1, 1L)
  }

  @Test
  def testFirstUnstableOffset() {
    val epoch = 5.toShort
    val sequence = 0

    assertEquals(None, stateManager.firstUndecidedOffset)

    append(stateManager, producerId, epoch, sequence, offset = 99, isTransactional = true)
    assertEquals(Some(99L), stateManager.firstUndecidedOffset)
    assertEquals(Some(99L), stateManager.firstUnstableOffset.map(_.messageOffset))

    val anotherPid = 2L
    append(stateManager, anotherPid, epoch, sequence, offset = 105, isTransactional = true)
    assertEquals(Some(99L), stateManager.firstUndecidedOffset)
    assertEquals(Some(99L), stateManager.firstUnstableOffset.map(_.messageOffset))

    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 109)
    assertEquals(Some(105L), stateManager.firstUndecidedOffset)
    assertEquals(Some(99L), stateManager.firstUnstableOffset.map(_.messageOffset))

    stateManager.onHighWatermarkUpdated(100L)
    assertEquals(Some(99L), stateManager.firstUnstableOffset.map(_.messageOffset))

    stateManager.onHighWatermarkUpdated(110L)
    assertEquals(Some(105L), stateManager.firstUnstableOffset.map(_.messageOffset))

    appendEndTxnMarker(stateManager, anotherPid, epoch, ControlRecordType.ABORT, offset = 112)
    assertEquals(None, stateManager.firstUndecidedOffset)
    assertEquals(Some(105L), stateManager.firstUnstableOffset.map(_.messageOffset))

    stateManager.onHighWatermarkUpdated(113L)
    assertEquals(None, stateManager.firstUnstableOffset.map(_.messageOffset))
  }

  @Test
  def testProducersWithOngoingTransactionsDontExpire() {
    val epoch = 5.toShort
    val sequence = 0

    append(stateManager, producerId, epoch, sequence, offset = 99, isTransactional = true)
    assertEquals(Some(99L), stateManager.firstUndecidedOffset)

    time.sleep(maxPidExpirationMs + 1)
    stateManager.removeExpiredProducers(time.milliseconds)

    assertTrue(stateManager.lastEntry(producerId).isDefined)
    assertEquals(Some(99L), stateManager.firstUndecidedOffset)

    stateManager.removeExpiredProducers(time.milliseconds)
    assertTrue(stateManager.lastEntry(producerId).isDefined)
  }

  @Test
  def testSequenceNotValidatedForGroupMetadataTopic(): Unit = {
    val partition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val stateManager = new ProducerStateManager(partition, logDir, maxPidExpirationMs)

    val epoch = 0.toShort
    append(stateManager, producerId, epoch, RecordBatch.NO_SEQUENCE, offset = 99, isTransactional = true)
    append(stateManager, producerId, epoch, RecordBatch.NO_SEQUENCE, offset = 100, isTransactional = true)

  }

  @Test(expected = classOf[ProducerFencedException])
  def testOldEpochForControlRecord(): Unit = {
    val epoch = 5.toShort
    val sequence = 0

    assertEquals(None, stateManager.firstUndecidedOffset)

    append(stateManager, producerId, epoch, sequence, offset = 99, isTransactional = true)
    appendEndTxnMarker(stateManager, producerId, 3.toShort, ControlRecordType.COMMIT, offset=100)
  }

  @Test
  def testCoordinatorFencing(): Unit = {
    val epoch = 5.toShort
    val sequence = 0

    append(stateManager, producerId, epoch, sequence, offset = 99, isTransactional = true)
    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 100, coordinatorEpoch = 1)

    val lastEntry = stateManager.lastEntry(producerId)
    assertEquals(Some(1), lastEntry.map(_.coordinatorEpoch))

    // writing with the current epoch is allowed
    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 101, coordinatorEpoch = 1)

    // bumping the epoch is allowed
    appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 102, coordinatorEpoch = 2)

    // old epochs are not allowed
    try {
      appendEndTxnMarker(stateManager, producerId, epoch, ControlRecordType.COMMIT, offset = 103, coordinatorEpoch = 1)
      fail("Expected coordinator to be fenced")
    } catch {
      case e: TransactionCoordinatorFencedException =>
    }
  }

  @Test(expected = classOf[TransactionCoordinatorFencedException])
  def testCoordinatorFencedAfterReload(): Unit = {
    val producerEpoch = 0.toShort
    append(stateManager, producerId, producerEpoch, 0, offset = 99, isTransactional = true)
    appendEndTxnMarker(stateManager, producerId, producerEpoch, ControlRecordType.COMMIT, offset = 100, coordinatorEpoch = 1)
    stateManager.takeSnapshot()

    val recoveredMapping = new ProducerStateManager(partition, logDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(0L, 2L, 70000)

    // append from old coordinator should be rejected
    appendEndTxnMarker(stateManager, producerId, producerEpoch, ControlRecordType.COMMIT, offset = 100, coordinatorEpoch = 0)
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
    val producerEpoch = 145.toShort
    val baseOffset = 15

    val batch: RecordBatch = EasyMock.createMock(classOf[RecordBatch])
    EasyMock.expect(batch.isControlBatch).andReturn(true).once
    EasyMock.expect(batch.iterator).andReturn(Collections.emptyIterator[Record]).once
    EasyMock.replay(batch)

    // Appending the empty control batch should not throw and a new transaction shouldn't be started
    append(stateManager, producerId, producerEpoch, baseOffset, batch, isFromClient = true)
    assertEquals(None, stateManager.lastEntry(producerId).get.currentTxnFirstOffset)
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
    assertEquals(Some(2L), latestSnapshotOffset)
    val snapshotToTruncate = Log.producerSnapshotFile(logDir, latestSnapshotOffset.get)
    val channel = FileChannel.open(snapshotToTruncate.toPath, StandardOpenOption.WRITE)
    try {
      makeFileCorrupt(channel)
    } finally {
      channel.close()
    }

    // Ensure that the truncated snapshot is deleted and producer state is loaded from the previous snapshot
    val reloadedStateManager = new ProducerStateManager(partition, logDir, maxPidExpirationMs)
    reloadedStateManager.truncateAndReload(0L, 20L, time.milliseconds())
    assertFalse(snapshotToTruncate.exists())

    val loadedProducerState = reloadedStateManager.activeProducers(producerId)
    assertEquals(0L, loadedProducerState.lastDataOffset)
  }

  private def appendEndTxnMarker(mapping: ProducerStateManager,
                                 producerId: Long,
                                 producerEpoch: Short,
                                 controlType: ControlRecordType,
                                 offset: Long,
                                 coordinatorEpoch: Int = 0,
                                 timestamp: Long = time.milliseconds()): (CompletedTxn, Long) = {
    val producerAppendInfo = stateManager.prepareUpdate(producerId, isFromClient = true)
    val endTxnMarker = new EndTransactionMarker(controlType, coordinatorEpoch)
    val completedTxn = producerAppendInfo.appendEndTxnMarker(endTxnMarker, producerEpoch, offset, timestamp)
    mapping.update(producerAppendInfo)
    val lastStableOffset = mapping.lastStableOffset(completedTxn)
    mapping.completeTxn(completedTxn)
    mapping.updateMapEndOffset(offset + 1)
    (completedTxn, lastStableOffset)
  }

  private def append(stateManager: ProducerStateManager,
                     producerId: Long,
                     producerEpoch: Short,
                     seq: Int,
                     offset: Long,
                     timestamp: Long = time.milliseconds(),
                     isTransactional: Boolean = false,
                     isFromClient : Boolean = true): Unit = {
    val producerAppendInfo = stateManager.prepareUpdate(producerId, isFromClient)
    producerAppendInfo.append(producerEpoch, seq, seq, timestamp, offset, offset, isTransactional)
    stateManager.update(producerAppendInfo)
    stateManager.updateMapEndOffset(offset + 1)
  }

  private def append(stateManager: ProducerStateManager,
                     producerId: Long,
                     producerEpoch: Short,
                     offset: Long,
                     batch: RecordBatch,
                     isFromClient : Boolean): Unit = {
    val producerAppendInfo = stateManager.prepareUpdate(producerId, isFromClient)
    producerAppendInfo.append(batch)
    stateManager.update(producerAppendInfo)
    stateManager.updateMapEndOffset(offset + 1)
  }

  private def currentSnapshotOffsets =
    logDir.listFiles.map(Log.offsetFromFile).toSet

}
