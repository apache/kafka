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
import java.util.Properties

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{DuplicateSequenceNumberException, OutOfOrderSequenceException, ProducerFencedException}
import org.apache.kafka.common.record.{ControlRecordType, InvalidRecordException, RecordBatch}
import org.apache.kafka.common.utils.{MockTime, Utils}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

class ProducerStateManagerTest extends JUnitSuite {
  var idMappingDir: File = null
  var config: LogConfig = null
  var idMapping: ProducerStateManager = null
  val partition = new TopicPartition("test", 0)
  val pid = 1L
  val maxPidExpirationMs = 60 * 1000
  val time = new MockTime

  @Before
  def setUp(): Unit = {
    config = LogConfig(new Properties)
    idMappingDir = TestUtils.tempDir()
    idMapping = new ProducerStateManager(config, partition, idMappingDir, maxPidExpirationMs)
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
    assertEquals(epoch, lastEntry.epoch)
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
    val completedTxn = appendControl(idMapping, pid, bumpedEpoch, ControlRecordType.ABORT, 1L)
    assertEquals(1L, completedTxn.firstOffset)
    assertEquals(1L, completedTxn.lastOffset)
    assertTrue(completedTxn.isAborted)
    assertEquals(pid, completedTxn.producerId)

    val maybeLastEntry = idMapping.lastEntry(pid)
    assertTrue(maybeLastEntry.isDefined)

    val lastEntry = maybeLastEntry.get
    assertEquals(bumpedEpoch, lastEntry.epoch)
    assertEquals(None, lastEntry.currentTxnFirstOffset)
    assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.firstSeq)
    assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.lastSeq)

    // should be able to append with the new epoch if we start at sequence 0
    append(idMapping, pid, 0, bumpedEpoch, 2L)
    assertEquals(Some(0), idMapping.lastEntry(pid).map(_.firstSeq))
  }

  @Test(expected = classOf[OutOfOrderSequenceException])
  def testOutOfSequenceAfterControlRecordEpochBump(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L)
    append(idMapping, pid, 1, epoch, 1L)

    val bumpedEpoch = 1.toShort
    appendControl(idMapping, pid, bumpedEpoch, ControlRecordType.ABORT, 1L)

    // next append is invalid since we expect the sequence to be reset
    append(idMapping, pid, 2, bumpedEpoch, 2L)
  }

  @Test(expected = classOf[InvalidRecordException])
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
    idMapping.maybeTakeSnapshot()

    // Check that file exists and it is not empty
    assertEquals("Directory doesn't contain a single file as expected", 1, idMappingDir.list().length)
    assertTrue("Snapshot file is empty", idMappingDir.list().head.length > 0)
  }

  @Test
  def testRecoverFromSnapshot(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L)
    append(idMapping, pid, 1, epoch, 1L)

    idMapping.maybeTakeSnapshot()
    val recoveredMapping = new ProducerStateManager(config, partition, idMappingDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(0L, 3L, time.milliseconds)

    // entry added after recovery
    append(recoveredMapping, pid, 2, epoch, 2L)
  }

  @Test(expected = classOf[OutOfOrderSequenceException])
  def testRemoveExpiredPidsOnReload(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L, 0)
    append(idMapping, pid, 1, epoch, 1L, 1)

    idMapping.maybeTakeSnapshot()
    val recoveredMapping = new ProducerStateManager(config, partition, idMappingDir, maxPidExpirationMs)
    recoveredMapping.truncateAndReload(0L, 1L, 70000)

    // entry added after recovery. The pid should be expired now, and would not exist in the pid mapping. Hence
    // we should get an out of order sequence exception.
    append(recoveredMapping, pid, 2, epoch, 2L, 70001)
  }

  @Test
  def testRemoveOldSnapshot(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L)
    append(idMapping, pid, 1, epoch, 1L)
    idMapping.maybeTakeSnapshot()
    assertEquals(1, idMappingDir.listFiles().length)
    assertEquals(Set(2), currentSnapshotOffsets)

    append(idMapping, pid, 2, epoch, 2L)
    idMapping.maybeTakeSnapshot()
    assertEquals(2, idMappingDir.listFiles().length)
    assertEquals(Set(2, 3), currentSnapshotOffsets)

    // we only retain two snapshot files, so the next snapshot should cause the oldest to be deleted
    append(idMapping, pid, 3, epoch, 3L)
    idMapping.maybeTakeSnapshot()
    assertEquals(2, idMappingDir.listFiles().length)
    assertEquals(Set(3, 4), currentSnapshotOffsets)
  }

  @Test
  def testTruncate(): Unit = {
    val epoch = 0.toShort

    append(idMapping, pid, 0, epoch, 0L)
    append(idMapping, pid, 1, epoch, 1L)
    idMapping.maybeTakeSnapshot()
    assertEquals(1, idMappingDir.listFiles().length)
    assertEquals(Set(2), currentSnapshotOffsets)

    append(idMapping, pid, 2, epoch, 2L)
    idMapping.maybeTakeSnapshot()
    assertEquals(2, idMappingDir.listFiles().length)
    assertEquals(Set(2, 3), currentSnapshotOffsets)

    idMapping.truncate()

    assertEquals(0, idMappingDir.listFiles().length)
    assertEquals(Set(), currentSnapshotOffsets)

    append(idMapping, pid, 0, epoch, 0L)
    idMapping.maybeTakeSnapshot()
    assertEquals(1, idMappingDir.listFiles().length)
    assertEquals(Set(1), currentSnapshotOffsets)
  }

  @Test
  def testFirstUnstableOffsetAfterTruncation(): Unit = {
    val epoch = 0.toShort
    val sequence = 0

    append(idMapping, pid, sequence, epoch, offset = 99, isTransactional = true)
    assertEquals(Some(99), idMapping.firstUnstableOffset)
    idMapping.maybeTakeSnapshot()

    appendControl(idMapping, pid, epoch, ControlRecordType.COMMIT, offset = 105)
    idMapping.ackTransactionsCompletedBefore(106)
    assertEquals(None, idMapping.firstUnstableOffset)
    idMapping.maybeTakeSnapshot()

    append(idMapping, pid, sequence + 1, epoch, offset = 106)
    idMapping.truncateAndReload(0L, 106, time.milliseconds())
    assertEquals(None, idMapping.firstUnstableOffset)

    idMapping.truncateAndReload(0L, 100L, time.milliseconds())
    assertEquals(Some(99), idMapping.firstUnstableOffset)
  }

  @Test
  def testFirstUnstableOffsetAfterEviction(): Unit = {
    val epoch = 0.toShort
    val sequence = 0
    append(idMapping, pid, sequence, epoch, offset = 99, isTransactional = true)
    assertEquals(Some(99), idMapping.firstUnstableOffset)
    append(idMapping, 2L, 0, epoch, offset = 106)
    idMapping.evictUnretainedPids(100)
    assertEquals(None, idMapping.firstUnstableOffset)
  }

  @Test
  def testEvictUnretainedPids(): Unit = {
    val epoch = 0.toShort

    append(idMapping, pid, 0, epoch, 0L)
    append(idMapping, pid, 1, epoch, 1L)
    idMapping.maybeTakeSnapshot()

    val anotherPid = 2L
    append(idMapping, anotherPid, 0, epoch, 2L)
    append(idMapping, anotherPid, 1, epoch, 3L)
    idMapping.maybeTakeSnapshot()
    assertEquals(Set(2, 4), currentSnapshotOffsets)

    idMapping.evictUnretainedPids(2)
    assertEquals(Set(4), currentSnapshotOffsets)
    assertEquals(Set(anotherPid), idMapping.activePids.keySet)
    assertEquals(None, idMapping.lastEntry(pid))

    val maybeEntry = idMapping.lastEntry(anotherPid)
    assertTrue(maybeEntry.isDefined)
    assertEquals(3L, maybeEntry.get.lastOffset)

    idMapping.evictUnretainedPids(3)
    assertEquals(Set(anotherPid), idMapping.activePids.keySet)
    assertEquals(Set(4), currentSnapshotOffsets)
    assertEquals(4, idMapping.mapEndOffset)

    idMapping.evictUnretainedPids(5)
    assertEquals(Set(), idMapping.activePids.keySet)
    assertEquals(Set(), currentSnapshotOffsets)
    assertEquals(5, idMapping.mapEndOffset)

    idMapping.maybeTakeSnapshot()
    // shouldn't be any new snapshot because the log is empty
    assertEquals(Set(), currentSnapshotOffsets)
  }

  @Test
  def testSkipSnapshotIfOffsetUnchanged(): Unit = {
    val epoch = 0.toShort
    append(idMapping, pid, 0, epoch, 0L, 0L)

    idMapping.maybeTakeSnapshot()
    assertEquals(1, idMappingDir.listFiles().length)
    assertEquals(Set(1), currentSnapshotOffsets)

    // nothing changed so there should be no new snapshot
    idMapping.maybeTakeSnapshot()
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
    idMapping.maybeTakeSnapshot()

    intercept[OutOfOrderSequenceException] {
      val recoveredMapping = new ProducerStateManager(config, partition, idMappingDir, maxPidExpirationMs)
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

    val anotherPid = 2L
    append(idMapping, anotherPid, sequence, epoch, offset = 105, isTransactional = true)
    assertEquals(Some(99L), idMapping.firstUndecidedOffset)

    appendControl(idMapping, pid, epoch, ControlRecordType.COMMIT, offset = 109)
    assertEquals(Some(105L), idMapping.firstUndecidedOffset)

    appendControl(idMapping, anotherPid, epoch, ControlRecordType.ABORT, offset = 112)
    assertEquals(None, idMapping.firstUndecidedOffset)
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
    appendControl(idMapping, pid, 3.toShort, ControlRecordType.COMMIT, offset=100)
  }

  private def appendControl(mapping: ProducerStateManager,
                            pid: Long,
                            epoch: Short,
                            controlType: ControlRecordType,
                            offset: Long,
                            timestamp: Long = time.milliseconds()): CompletedTxn = {
    val producerAppendInfo = new ProducerAppendInfo(pid, mapping.lastEntry(pid).getOrElse(ProducerIdEntry.Empty))
    val completedTxn = producerAppendInfo.appendControlRecord(controlType, pid, epoch, offset, timestamp)
    mapping.update(producerAppendInfo)
    mapping.completeTxn(completedTxn)
    mapping.updateMapEndOffset(offset + 1)
    completedTxn
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
