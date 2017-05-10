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

import kafka.server.LogOffsetMetadata
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.record.{ControlRecordType, EndTransactionMarker, RecordBatch}
import org.apache.kafka.common.utils.{MockTime, Utils}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

class ProducerStateManagerTest extends JUnitSuite {
  var idMappingDir: File = null
  var idMapping: ProducerStateManager = null
  val partition = new TopicPartition("test", 0)
  val pid = 1L
  val maxPidExpirationMs = 60 * 1000
  val time = new MockTime

  @Before
  def setUp(): Unit = {
    idMappingDir = TestUtils.tempDir()
    idMapping = new ProducerStateManager(partition, idMappingDir, maxPidExpirationMs)
  }

  @After
  def tearDown(): Unit = {
    Utils.delete(idMappingDir)
  }

  @Test
  def testBasicIdMapping(): Unit = {
    val epoch = 0.toShort

    // First entry for id 0 added
    append(idMapping, pid, 0, epoch, 0L, 0L)

    // Second entry for id 0 added
    append(idMapping, pid, 1, epoch, 0L, 1L)

    // Duplicate sequence number (matches previous sequence number)
    assertThrows[DuplicateSequenceNumberException] {
      append(idMapping, pid, 1, epoch, 0L, 1L)
    }

    // Invalid sequence number (greater than next expected sequence number)
    assertThrows[OutOfOrderSequenceException] {
      append(idMapping, pid, 5, epoch, 0L, 2L)
    }

    // Change epoch
    append(idMapping, pid, 0, (epoch + 1).toShort, 0L, 3L)

    // Incorrect epoch
    assertThrows[ProducerFencedException] {
      append(idMapping, pid, 0, epoch, 0L, 4L)
    }
  }

  @Test
  def testNoValidationOnFirstEntryWhenLoadingLog(): Unit = {
    val epoch = 5.toShort
    val sequence = 16
    val offset = 735L
    append(idMapping, pid, sequence, epoch, offset, isLoadingFromLog = true)

    val maybeLastEntry = idMapping.lastEntry(pid)
    assertTrue(maybeLastEntry.isDefined)

    val lastEntry = maybeLastEntry.get
    assertEquals(epoch, lastEntry.producerEpoch)
    assertEquals(sequence, lastEntry.firstSeq)
    assertEquals(sequence, lastEntry.lastSeq)
    assertEquals(offset, lastEntry.lastOffset)
    assertEquals(offset, lastEntry.firstOffset)
  }

  @Test
  def testControlRecordBumpsEpoch(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L)

    val bumpedEpoch = 1.toShort
    val (completedTxn, lastStableOffset) = appendEndTxnMarker(idMapping, pid, bumpedEpoch, ControlRecordType.ABORT, 1L)
    assertEquals(1L, completedTxn.firstOffset)
    assertEquals(1L, completedTxn.lastOffset)
    assertEquals(2L, lastStableOffset)
    assertTrue(completedTxn.isAborted)
    assertEquals(pid, completedTxn.producerId)

    val maybeLastEntry = idMapping.lastEntry(pid)
    assertTrue(maybeLastEntry.isDefined)

    val lastEntry = maybeLastEntry.get
    assertEquals(bumpedEpoch, lastEntry.producerEpoch)
    assertEquals(None, lastEntry.currentTxnFirstOffset)
    assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.firstSeq)
    assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.lastSeq)

    // should be able to append with the new epoch if we start at sequence 0
    append(idMapping, pid, 0, bumpedEpoch, 2L)
    assertEquals(Some(0), idMapping.lastEntry(pid).map(_.firstSeq))
  }

  @Test
  def testTxnFirstOffsetMetadataCached(): Unit = {
    val producerEpoch = 0.toShort
    val offset = 992342L
    val seq = 0
    val producerAppendInfo = new ProducerAppendInfo(pid, None, false)
    producerAppendInfo.append(producerEpoch, seq, seq, time.milliseconds(), offset, isTransactional = true)

    val logOffsetMetadata = new LogOffsetMetadata(messageOffset = offset, segmentBaseOffset = 990000L,
      relativePositionInSegment = 234224)
    producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
    idMapping.update(producerAppendInfo)

    assertEquals(Some(logOffsetMetadata), idMapping.firstUnstableOffset)
  }

  @Test
  def testNonMatchingTxnFirstOffsetMetadataNotCached(): Unit = {
    val producerEpoch = 0.toShort
    val offset = 992342L
    val seq = 0
    val producerAppendInfo = new ProducerAppendInfo(pid, None, false)
    producerAppendInfo.append(producerEpoch, seq, seq, time.milliseconds(), offset, isTransactional = true)

    // use some other offset to simulate a follower append where the log offset metadata won't typically
    // match any of the transaction first offsets
    val logOffsetMetadata = new LogOffsetMetadata(messageOffset = offset - 23429, segmentBaseOffset = 990000L,
      relativePositionInSegment = 234224)
    producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
    idMapping.update(producerAppendInfo)

    assertEquals(Some(LogOffsetMetadata(offset)), idMapping.firstUnstableOffset)
  }

  @Test
  def updateProducerTransactionState(): Unit = {
    val producerEpoch = 0.toShort
    val coordinatorEpoch = 15
    val offset = 9L
    append(idMapping, pid, 0, producerEpoch, offset)

    val appendInfo = new ProducerAppendInfo(pid, idMapping.lastEntry(pid), loadingFromLog = false)
    appendInfo.append(producerEpoch, 1, 5, time.milliseconds(), 20L, isTransactional = true)
    var lastEntry = appendInfo.lastEntry
    assertEquals(producerEpoch, lastEntry.producerEpoch)
    assertEquals(1, lastEntry.firstSeq)
    assertEquals(5, lastEntry.lastSeq)
    assertEquals(16L, lastEntry.firstOffset)
    assertEquals(20L, lastEntry.lastOffset)
    assertEquals(Some(16L), lastEntry.currentTxnFirstOffset)
    assertEquals(List(new TxnMetadata(pid, 16L)), appendInfo.startedTransactions)

    appendInfo.append(producerEpoch, 6, 10, time.milliseconds(), 30L, isTransactional = true)
    lastEntry = appendInfo.lastEntry
    assertEquals(producerEpoch, lastEntry.producerEpoch)
    assertEquals(6, lastEntry.firstSeq)
    assertEquals(10, lastEntry.lastSeq)
    assertEquals(26L, lastEntry.firstOffset)
    assertEquals(30L, lastEntry.lastOffset)
    assertEquals(Some(16L), lastEntry.currentTxnFirstOffset)
    assertEquals(List(new TxnMetadata(pid, 16L)), appendInfo.startedTransactions)

    val endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch)
    val completedTxn = appendInfo.appendEndTxnMarker(endTxnMarker, producerEpoch, 40L, time.milliseconds())
    assertEquals(pid, completedTxn.producerId)
    assertEquals(16L, completedTxn.firstOffset)
    assertEquals(40L, completedTxn.lastOffset)
    assertFalse(completedTxn.isAborted)

    lastEntry = appendInfo.lastEntry
    assertEquals(producerEpoch, lastEntry.producerEpoch)
    assertEquals(10, lastEntry.firstSeq)
    assertEquals(10, lastEntry.lastSeq)
    assertEquals(40L, lastEntry.firstOffset)
    assertEquals(40L, lastEntry.lastOffset)
    assertEquals(coordinatorEpoch, lastEntry.coordinatorEpoch)
    assertEquals(None, lastEntry.currentTxnFirstOffset)
    assertEquals(List(new TxnMetadata(pid, 16L)), appendInfo.startedTransactions)
  }

  @Test(expected = classOf[OutOfOrderSequenceException])
  def testOutOfSequenceAfterControlRecordEpochBump(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L)
    append(idMapping, pid, 1, epoch, 1L)

    val bumpedEpoch = 1.toShort
    appendEndTxnMarker(idMapping, pid, bumpedEpoch, ControlRecordType.ABORT, 1L)

    // next append is invalid since we expect the sequence to be reset
    append(idMapping, pid, 2, bumpedEpoch, 2L)
  }

  @Test(expected = classOf[InvalidTxnStateException])
  def testNonTransactionalAppendWithOngoingTransaction(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L, isTransactional = true)
    append(idMapping, pid, 1, epoch, 1L, isTransactional = false)
  }

  @Test
  def testTakeSnapshot(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L, 0L)
    append(idMapping, pid, 1, epoch, 1L, 1L)

    // Take snapshot
    idMapping.takeSnapshot()

    // Check that file exists and it is not empty
    assertEquals("Directory doesn't contain a single file as expected", 1, idMappingDir.list().length)
    assertTrue("Snapshot file is empty", idMappingDir.list().head.length > 0)
  }

  @Test
  def testRecoverFromSnapshot(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L)
    append(idMapping, pid, 1, epoch, 1L)

    idMapping.takeSnapshot()
    val recoveredMapping = new ProducerStateManager(partition, idMappingDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(0L, 3L, time.milliseconds)

    // entry added after recovery
    append(recoveredMapping, pid, 2, epoch, 2L)
  }

  @Test(expected = classOf[OutOfOrderSequenceException])
  def testRemoveExpiredPidsOnReload(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L, 0)
    append(idMapping, pid, 1, epoch, 1L, 1)

    idMapping.takeSnapshot()
    val recoveredMapping = new ProducerStateManager(partition, idMappingDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(0L, 1L, 70000)

    // entry added after recovery. The pid should be expired now, and would not exist in the pid mapping. Hence
    // we should get an out of order sequence exception.
    append(recoveredMapping, pid, 2, epoch, 2L, 70001)
  }

  @Test
  def testDeleteSnapshotsBefore(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L)
    append(idMapping, pid, 1, epoch, 1L)
    idMapping.takeSnapshot()
    assertEquals(1, idMappingDir.listFiles().length)
    assertEquals(Set(2), currentSnapshotOffsets)

    append(idMapping, pid, 2, epoch, 2L)
    idMapping.takeSnapshot()
    assertEquals(2, idMappingDir.listFiles().length)
    assertEquals(Set(2, 3), currentSnapshotOffsets)

    idMapping.deleteSnapshotsBefore(3L)
    assertEquals(1, idMappingDir.listFiles().length)
    assertEquals(Set(3), currentSnapshotOffsets)

    idMapping.deleteSnapshotsBefore(4L)
    assertEquals(0, idMappingDir.listFiles().length)
    assertEquals(Set(), currentSnapshotOffsets)
  }

  @Test
  def testTruncate(): Unit = {
    val epoch = 0.toShort

    append(idMapping, pid, 0, epoch, 0L)
    append(idMapping, pid, 1, epoch, 1L)
    idMapping.takeSnapshot()
    assertEquals(1, idMappingDir.listFiles().length)
    assertEquals(Set(2), currentSnapshotOffsets)

    append(idMapping, pid, 2, epoch, 2L)
    idMapping.takeSnapshot()
    assertEquals(2, idMappingDir.listFiles().length)
    assertEquals(Set(2, 3), currentSnapshotOffsets)

    idMapping.truncate()

    assertEquals(0, idMappingDir.listFiles().length)
    assertEquals(Set(), currentSnapshotOffsets)

    append(idMapping, pid, 0, epoch, 0L)
    idMapping.takeSnapshot()
    assertEquals(1, idMappingDir.listFiles().length)
    assertEquals(Set(1), currentSnapshotOffsets)
  }

  @Test
  def testFirstUnstableOffsetAfterTruncation(): Unit = {
    val epoch = 0.toShort
    val sequence = 0

    append(idMapping, pid, sequence, epoch, offset = 99, isTransactional = true)
    assertEquals(Some(99), idMapping.firstUnstableOffset.map(_.messageOffset))
    idMapping.takeSnapshot()

    appendEndTxnMarker(idMapping, pid, epoch, ControlRecordType.COMMIT, offset = 105)
    idMapping.onHighWatermarkUpdated(106)
    assertEquals(None, idMapping.firstUnstableOffset.map(_.messageOffset))
    idMapping.takeSnapshot()

    append(idMapping, pid, sequence + 1, epoch, offset = 106)
    idMapping.truncateAndReload(0L, 106, time.milliseconds())
    assertEquals(None, idMapping.firstUnstableOffset.map(_.messageOffset))

    idMapping.truncateAndReload(0L, 100L, time.milliseconds())
    assertEquals(Some(99), idMapping.firstUnstableOffset.map(_.messageOffset))
  }

  @Test
  def testFirstUnstableOffsetAfterEviction(): Unit = {
    val epoch = 0.toShort
    val sequence = 0
    append(idMapping, pid, sequence, epoch, offset = 99, isTransactional = true)
    assertEquals(Some(99), idMapping.firstUnstableOffset.map(_.messageOffset))
    append(idMapping, 2L, 0, epoch, offset = 106, isTransactional = true)
    idMapping.evictUnretainedProducers(100)
    assertEquals(Some(106), idMapping.firstUnstableOffset.map(_.messageOffset))
  }

  @Test
  def testEvictUnretainedPids(): Unit = {
    val epoch = 0.toShort

    append(idMapping, pid, 0, epoch, 0L)
    append(idMapping, pid, 1, epoch, 1L)
    idMapping.takeSnapshot()

    val anotherPid = 2L
    append(idMapping, anotherPid, 0, epoch, 2L)
    append(idMapping, anotherPid, 1, epoch, 3L)
    idMapping.takeSnapshot()
    assertEquals(Set(2, 4), currentSnapshotOffsets)

    idMapping.evictUnretainedProducers(2)
    assertEquals(Set(4), currentSnapshotOffsets)
    assertEquals(Set(anotherPid), idMapping.activeProducers.keySet)
    assertEquals(None, idMapping.lastEntry(pid))

    val maybeEntry = idMapping.lastEntry(anotherPid)
    assertTrue(maybeEntry.isDefined)
    assertEquals(3L, maybeEntry.get.lastOffset)

    idMapping.evictUnretainedProducers(3)
    assertEquals(Set(anotherPid), idMapping.activeProducers.keySet)
    assertEquals(Set(4), currentSnapshotOffsets)
    assertEquals(4, idMapping.mapEndOffset)

    idMapping.evictUnretainedProducers(5)
    assertEquals(Set(), idMapping.activeProducers.keySet)
    assertEquals(Set(), currentSnapshotOffsets)
    assertEquals(5, idMapping.mapEndOffset)
  }

  @Test
  def testSkipSnapshotIfOffsetUnchanged(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L, 0L)

    idMapping.takeSnapshot()
    assertEquals(1, idMappingDir.listFiles().length)
    assertEquals(Set(1), currentSnapshotOffsets)

    // nothing changed so there should be no new snapshot
    idMapping.takeSnapshot()
    assertEquals(1, idMappingDir.listFiles().length)
    assertEquals(Set(1), currentSnapshotOffsets)
  }

  @Test
  def testStartOffset(): Unit = {
    val epoch = 0.toShort
    val pid2 = 2L
    append(idMapping, pid2, 0, epoch, 0L, 1L)
    append(idMapping, pid, 0, epoch, 1L, 2L)
    append(idMapping, pid, 1, epoch, 2L, 3L)
    append(idMapping, pid, 2, epoch, 3L, 4L)
    idMapping.takeSnapshot()

    intercept[OutOfOrderSequenceException] {
      val recoveredMapping = new ProducerStateManager(partition, idMappingDir, maxPidExpirationMs)
      recoveredMapping.truncateAndReload(0L, 1L, time.milliseconds)
      append(recoveredMapping, pid2, 1, epoch, 4L, 5L)
    }
  }

  @Test(expected = classOf[OutOfOrderSequenceException])
  def testPidExpirationTimeout() {
    val epoch = 5.toShort
    val sequence = 37
    append(idMapping, pid, sequence, epoch, 1L)
    time.sleep(maxPidExpirationMs + 1)
    idMapping.removeExpiredProducers(time.milliseconds)
    append(idMapping, pid, sequence + 1, epoch, 1L)
  }

  @Test
  def testFirstUnstableOffset() {
    val epoch = 5.toShort
    val sequence = 0

    assertEquals(None, idMapping.firstUndecidedOffset)

    append(idMapping, pid, sequence, epoch, offset = 99, isTransactional = true)
    assertEquals(Some(99L), idMapping.firstUndecidedOffset)
    assertEquals(Some(99L), idMapping.firstUnstableOffset.map(_.messageOffset))

    val anotherPid = 2L
    append(idMapping, anotherPid, sequence, epoch, offset = 105, isTransactional = true)
    assertEquals(Some(99L), idMapping.firstUndecidedOffset)
    assertEquals(Some(99L), idMapping.firstUnstableOffset.map(_.messageOffset))

    appendEndTxnMarker(idMapping, pid, epoch, ControlRecordType.COMMIT, offset = 109)
    assertEquals(Some(105L), idMapping.firstUndecidedOffset)
    assertEquals(Some(99L), idMapping.firstUnstableOffset.map(_.messageOffset))

    idMapping.onHighWatermarkUpdated(100L)
    assertEquals(Some(99L), idMapping.firstUnstableOffset.map(_.messageOffset))

    idMapping.onHighWatermarkUpdated(110L)
    assertEquals(Some(105L), idMapping.firstUnstableOffset.map(_.messageOffset))

    appendEndTxnMarker(idMapping, anotherPid, epoch, ControlRecordType.ABORT, offset = 112)
    assertEquals(None, idMapping.firstUndecidedOffset)
    assertEquals(Some(105L), idMapping.firstUnstableOffset.map(_.messageOffset))

    idMapping.onHighWatermarkUpdated(113L)
    assertEquals(None, idMapping.firstUnstableOffset.map(_.messageOffset))
  }

  @Test
  def testProducersWithOngoingTransactionsDontExpire() {
    val epoch = 5.toShort
    val sequence = 0

    append(idMapping, pid, sequence, epoch, offset = 99, isTransactional = true)
    assertEquals(Some(99L), idMapping.firstUndecidedOffset)

    time.sleep(maxPidExpirationMs + 1)
    idMapping.removeExpiredProducers(time.milliseconds)

    assertTrue(idMapping.lastEntry(pid).isDefined)
    assertEquals(Some(99L), idMapping.firstUndecidedOffset)

    idMapping.removeExpiredProducers(time.milliseconds)
    assertTrue(idMapping.lastEntry(pid).isDefined)
  }

  @Test(expected = classOf[ProducerFencedException])
  def testOldEpochForControlRecord(): Unit = {
    val epoch = 5.toShort
    val sequence = 0

    assertEquals(None, idMapping.firstUndecidedOffset)

    append(idMapping, pid, sequence, epoch, offset = 99, isTransactional = true)
    appendEndTxnMarker(idMapping, pid, 3.toShort, ControlRecordType.COMMIT, offset=100)
  }

  @Test
  def testCoordinatorFencing(): Unit = {
    val epoch = 5.toShort
    val sequence = 0

    append(idMapping, pid, sequence, epoch, offset = 99, isTransactional = true)
    appendEndTxnMarker(idMapping, pid, epoch, ControlRecordType.COMMIT, offset = 100, coordinatorEpoch = 1)

    val lastEntry = idMapping.lastEntry(pid)
    assertEquals(Some(1), lastEntry.map(_.coordinatorEpoch))

    // writing with the current epoch is allowed
    appendEndTxnMarker(idMapping, pid, epoch, ControlRecordType.COMMIT, offset = 101, coordinatorEpoch = 1)

    // bumping the epoch is allowed
    appendEndTxnMarker(idMapping, pid, epoch, ControlRecordType.COMMIT, offset = 102, coordinatorEpoch = 2)

    // old epochs are not allowed
    try {
      appendEndTxnMarker(idMapping, pid, epoch, ControlRecordType.COMMIT, offset = 103, coordinatorEpoch = 1)
      fail("Expected coordinator to be fenced")
    } catch {
      case e: TransactionCoordinatorFencedException =>
    }
  }

  @Test(expected = classOf[TransactionCoordinatorFencedException])
  def testCoordinatorFencedAfterReload(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, offset = 99, isTransactional = true)
    appendEndTxnMarker(idMapping, pid, epoch, ControlRecordType.COMMIT, offset = 100, coordinatorEpoch = 1)
    idMapping.takeSnapshot()

    val recoveredMapping = new ProducerStateManager(partition, idMappingDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(0L, 2L, 70000)

    // append from old coordinator should be rejected
    appendEndTxnMarker(idMapping, pid, epoch, ControlRecordType.COMMIT, offset = 100, coordinatorEpoch = 0)
  }

  private def appendEndTxnMarker(mapping: ProducerStateManager,
                                 pid: Long,
                                 epoch: Short,
                                 controlType: ControlRecordType,
                                 offset: Long,
                                 coordinatorEpoch: Int = 0,
                                 timestamp: Long = time.milliseconds()): (CompletedTxn, Long) = {
    val producerAppendInfo = new ProducerAppendInfo(pid, mapping.lastEntry(pid).getOrElse(ProducerIdEntry.Empty))
    val endTxnMarker = new EndTransactionMarker(controlType, coordinatorEpoch)
    val completedTxn = producerAppendInfo.appendEndTxnMarker(endTxnMarker, epoch, offset, timestamp)
    mapping.update(producerAppendInfo)
    val lastStableOffset = mapping.completeTxn(completedTxn)
    mapping.updateMapEndOffset(offset + 1)
    (completedTxn, lastStableOffset)
  }

  private def append(mapping: ProducerStateManager,
                     pid: Long,
                     seq: Int,
                     epoch: Short,
                     offset: Long,
                     timestamp: Long = time.milliseconds(),
                     isTransactional: Boolean = false,
                     isLoadingFromLog: Boolean = false): Unit = {
    val producerAppendInfo = new ProducerAppendInfo(pid, mapping.lastEntry(pid), isLoadingFromLog)
    producerAppendInfo.append(epoch, seq, seq, timestamp, offset, isTransactional)
    mapping.update(producerAppendInfo)
    mapping.updateMapEndOffset(offset + 1)
  }

  private def currentSnapshotOffsets =
    idMappingDir.listFiles().map(file => Log.offsetFromFilename(file.getName)).toSet

}
