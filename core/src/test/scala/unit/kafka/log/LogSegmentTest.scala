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

import kafka.utils.TestUtils
import kafka.utils.TestUtils.checkEquals
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{MockTime, Time, Utils}
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection._

class LogSegmentTest {

  val topicPartition = new TopicPartition("topic", 0)
  val segments = mutable.ArrayBuffer[LogSegment]()
  var logDir: File = _

  /* create a segment with the given base offset */
  def createSegment(offset: Long,
                    indexIntervalBytes: Int = 10,
                    maxSegmentMs: Int = Int.MaxValue,
                    time: Time = Time.SYSTEM): LogSegment = {
    val ms = FileRecords.open(Log.logFile(logDir, offset))
    val idx = new OffsetIndex(Log.offsetIndexFile(logDir, offset), offset, maxIndexSize = 1000)
    val timeIdx = new TimeIndex(Log.timeIndexFile(logDir, offset), offset, maxIndexSize = 1500)
    val txnIndex = new TransactionIndex(offset, Log.transactionIndexFile(logDir, offset))
    val seg = new LogSegment(ms, idx, timeIdx, txnIndex, offset, indexIntervalBytes, 0, maxSegmentMs = maxSegmentMs,
      maxSegmentBytes = Int.MaxValue, time)
    segments += seg
    seg
  }

  /* create a ByteBufferMessageSet for the given messages starting from the given offset */
  def records(offset: Long, records: String*): MemoryRecords = {
    MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, offset, CompressionType.NONE, TimestampType.CREATE_TIME,
      records.map { s => new SimpleRecord(offset * 10, s.getBytes) }: _*)
  }

  @Before
  def setup(): Unit = {
    logDir = TestUtils.tempDir()
  }

  @After
  def teardown() {
    segments.foreach(_.close())
    Utils.delete(logDir)
  }

  /**
   * A read on an empty log segment should return null
   */
  @Test
  def testReadOnEmptySegment() {
    val seg = createSegment(40)
    val read = seg.read(startOffset = 40, maxSize = 300, maxOffset = None)
    assertNull("Read beyond the last offset in the segment should be null", read)
  }

  /**
   * Reading from before the first offset in the segment should return messages
   * beginning with the first message in the segment
   */
  @Test
  def testReadBeforeFirstOffset() {
    val seg = createSegment(40)
    val ms = records(50, "hello", "there", "little", "bee")
    seg.append(53, RecordBatch.NO_TIMESTAMP, -1L, ms)
    val read = seg.read(startOffset = 41, maxSize = 300, maxOffset = None).records
    checkEquals(ms.records.iterator, read.records.iterator)
  }

  /**
   * If we set the startOffset and maxOffset for the read to be the same value
   * we should get only the first message in the log
   */
  @Test
  def testMaxOffset() {
    val baseOffset = 50
    val seg = createSegment(baseOffset)
    val ms = records(baseOffset, "hello", "there", "beautiful")
    seg.append(52, RecordBatch.NO_TIMESTAMP, -1L, ms)
    def validate(offset: Long) =
      assertEquals(ms.records.asScala.filter(_.offset == offset).toList,
                   seg.read(startOffset = offset, maxSize = 1024, maxOffset = Some(offset+1)).records.records.asScala.toList)
    validate(50)
    validate(51)
    validate(52)
  }

  /**
   * If we read from an offset beyond the last offset in the segment we should get null
   */
  @Test
  def testReadAfterLast() {
    val seg = createSegment(40)
    val ms = records(50, "hello", "there")
    seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms)
    val read = seg.read(startOffset = 52, maxSize = 200, maxOffset = None)
    assertNull("Read beyond the last offset in the segment should give null", read)
  }

  /**
   * If we read from an offset which doesn't exist we should get a message set beginning
   * with the least offset greater than the given startOffset.
   */
  @Test
  def testReadFromGap() {
    val seg = createSegment(40)
    val ms = records(50, "hello", "there")
    seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms)
    val ms2 = records(60, "alpha", "beta")
    seg.append(61, RecordBatch.NO_TIMESTAMP, -1L, ms2)
    val read = seg.read(startOffset = 55, maxSize = 200, maxOffset = None)
    checkEquals(ms2.records.iterator, read.records.records.iterator)
  }

  /**
   * In a loop append two messages then truncate off the second of those messages and check that we can read
   * the first but not the second message.
   */
  @Test
  def testTruncate() {
    val seg = createSegment(40)
    var offset = 40
    for (_ <- 0 until 30) {
      val ms1 = records(offset, "hello")
      seg.append(offset, RecordBatch.NO_TIMESTAMP, -1L, ms1)
      val ms2 = records(offset + 1, "hello")
      seg.append(offset + 1, RecordBatch.NO_TIMESTAMP, -1L, ms2)
      // check that we can read back both messages
      val read = seg.read(offset, None, 10000)
      assertEquals(List(ms1.records.iterator.next(), ms2.records.iterator.next()), read.records.records.asScala.toList)
      // now truncate off the last message
      seg.truncateTo(offset + 1)
      val read2 = seg.read(offset, None, 10000)
      assertEquals(1, read2.records.records.asScala.size)
      checkEquals(ms1.records.iterator, read2.records.records.iterator)
      offset += 1
    }
  }

  @Test
  def testTruncateEmptySegment() {
    // This tests the scenario in which the follower truncates to an empty segment. In this
    // case we must ensure that the index is resized so that the log segment is not mistakenly
    // rolled due to a full index

    val maxSegmentMs = 300000
    val time = new MockTime
    val seg = createSegment(0, maxSegmentMs = maxSegmentMs, time = time)
    seg.close()

    val reopened = createSegment(0, maxSegmentMs = maxSegmentMs, time = time)
    assertEquals(0, seg.timeIndex.sizeInBytes)
    assertEquals(0, seg.offsetIndex.sizeInBytes)

    time.sleep(500)
    reopened.truncateTo(57)
    assertEquals(0, reopened.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP))
    assertFalse(reopened.timeIndex.isFull)
    assertFalse(reopened.offsetIndex.isFull)

    assertFalse(reopened.shouldRoll(messagesSize = 1024,
      maxTimestampInMessages = RecordBatch.NO_TIMESTAMP,
      maxOffsetInMessages = 100L,
      now = time.milliseconds()))

    // The segment should not be rolled even if maxSegmentMs has been exceeded
    time.sleep(maxSegmentMs + 1)
    assertEquals(maxSegmentMs + 1, reopened.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP))
    assertFalse(reopened.shouldRoll(messagesSize = 1024,
      maxTimestampInMessages = RecordBatch.NO_TIMESTAMP,
      maxOffsetInMessages = 100L,
      now = time.milliseconds()))

    // But we should still roll the segment if we cannot fit the next offset
    assertTrue(reopened.shouldRoll(messagesSize = 1024,
      maxTimestampInMessages = RecordBatch.NO_TIMESTAMP,
      maxOffsetInMessages = Int.MaxValue.toLong + 200,
      now = time.milliseconds()))
  }

  @Test
  def testReloadLargestTimestampAndNextOffsetAfterTruncation() {
    val numMessages = 30
    val seg = createSegment(40, 2 * records(0, "hello").sizeInBytes - 1)
    var offset = 40
    for (_ <- 0 until numMessages) {
      seg.append(offset, offset, offset, records(offset, "hello"))
      offset += 1
    }
    assertEquals(offset, seg.readNextOffset)

    val expectedNumEntries = numMessages / 2 - 1
    assertEquals(s"Should have $expectedNumEntries time indexes", expectedNumEntries, seg.timeIndex.entries)

    seg.truncateTo(41)
    assertEquals(s"Should have 0 time indexes", 0, seg.timeIndex.entries)
    assertEquals(s"Largest timestamp should be 400", 400L, seg.largestTimestamp)
    assertEquals(41, seg.readNextOffset)
  }

  /**
   * Test truncating the whole segment, and check that we can reappend with the original offset.
   */
  @Test
  def testTruncateFull() {
    // test the case where we fully truncate the log
    val time = new MockTime
    val seg = createSegment(40, time = time)
    seg.append(41, RecordBatch.NO_TIMESTAMP, -1L, records(40, "hello", "there"))

    // If the segment is empty after truncation, the create time should be reset
    time.sleep(500)
    assertEquals(500, seg.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP))

    seg.truncateTo(0)
    assertEquals(0, seg.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP))
    assertFalse(seg.timeIndex.isFull)
    assertFalse(seg.offsetIndex.isFull)
    assertNull("Segment should be empty.", seg.read(0, None, 1024))

    seg.append(41, RecordBatch.NO_TIMESTAMP, -1L, records(40, "hello", "there"))
  }

  /**
   * Append messages with timestamp and search message by timestamp.
   */
  @Test
  def testFindOffsetByTimestamp() {
    val messageSize = records(0, s"msg00").sizeInBytes
    val seg = createSegment(40, messageSize * 2 - 1)
    // Produce some messages
    for (i <- 40 until 50)
      seg.append(i, i * 10, i, records(i, s"msg$i"))

    assertEquals(490, seg.largestTimestamp)
    // Search for an indexed timestamp
    assertEquals(42, seg.findOffsetByTimestamp(420).get.offset)
    assertEquals(43, seg.findOffsetByTimestamp(421).get.offset)
    // Search for an un-indexed timestamp
    assertEquals(43, seg.findOffsetByTimestamp(430).get.offset)
    assertEquals(44, seg.findOffsetByTimestamp(431).get.offset)
    // Search beyond the last timestamp
    assertEquals(None, seg.findOffsetByTimestamp(491))
    // Search before the first indexed timestamp
    assertEquals(41, seg.findOffsetByTimestamp(401).get.offset)
    // Search before the first timestamp
    assertEquals(40, seg.findOffsetByTimestamp(399).get.offset)
  }

  /**
   * Test that offsets are assigned sequentially and that the nextOffset variable is incremented
   */
  @Test
  def testNextOffsetCalculation() {
    val seg = createSegment(40)
    assertEquals(40, seg.readNextOffset)
    seg.append(52, RecordBatch.NO_TIMESTAMP, -1L, records(50, "hello", "there", "you"))
    assertEquals(53, seg.readNextOffset)
  }

  /**
   * Test that we can change the file suffixes for the log and index files
   */
  @Test
  def testChangeFileSuffixes() {
    val seg = createSegment(40)
    val logFile = seg.log.file
    val indexFile = seg.offsetIndex.file
    seg.changeFileSuffixes("", ".deleted")
    assertEquals(logFile.getAbsolutePath + ".deleted", seg.log.file.getAbsolutePath)
    assertEquals(indexFile.getAbsolutePath + ".deleted", seg.offsetIndex.file.getAbsolutePath)
    assertTrue(seg.log.file.exists)
    assertTrue(seg.offsetIndex.file.exists)
  }

  /**
   * Create a segment with some data and an index. Then corrupt the index,
   * and recover the segment, the entries should all be readable.
   */
  @Test
  def testRecoveryFixesCorruptIndex() {
    val seg = createSegment(0)
    for(i <- 0 until 100)
      seg.append(i, RecordBatch.NO_TIMESTAMP, -1L, records(i, i.toString))
    val indexFile = seg.offsetIndex.file
    TestUtils.writeNonsenseToFile(indexFile, 5, indexFile.length.toInt)
    seg.recover(new ProducerStateManager(topicPartition, logDir))
    for(i <- 0 until 100)
      assertEquals(i, seg.read(i, Some(i + 1), 1024).records.records.iterator.next().offset)
  }

  @Test
  def testRecoverTransactionIndex(): Unit = {
    val segment = createSegment(100)
    val producerEpoch = 0.toShort
    val partitionLeaderEpoch = 15
    val sequence = 100

    val pid1 = 5L
    val pid2 = 10L

    // append transactional records from pid1
    segment.append(largestOffset = 101L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 100L, MemoryRecords.withTransactionalRecords(100L, CompressionType.NONE,
        pid1, producerEpoch, sequence, partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    // append transactional records from pid2
    segment.append(largestOffset = 103L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 102L, MemoryRecords.withTransactionalRecords(102L, CompressionType.NONE,
        pid2, producerEpoch, sequence, partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    // append non-transactional records
    segment.append(largestOffset = 105L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 104L, MemoryRecords.withRecords(104L, CompressionType.NONE,
        partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    // abort the transaction from pid2 (note LSO should be 100L since the txn from pid1 has not completed)
    segment.append(largestOffset = 106L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 106L, endTxnRecords(ControlRecordType.ABORT, pid2, producerEpoch, offset = 106L))

    // commit the transaction from pid1
    segment.append(largestOffset = 107L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 107L, endTxnRecords(ControlRecordType.COMMIT, pid1, producerEpoch, offset = 107L))

    var stateManager = new ProducerStateManager(topicPartition, logDir)
    segment.recover(stateManager)
    assertEquals(108L, stateManager.mapEndOffset)


    var abortedTxns = segment.txnIndex.allAbortedTxns
    assertEquals(1, abortedTxns.size)
    var abortedTxn = abortedTxns.head
    assertEquals(pid2, abortedTxn.producerId)
    assertEquals(102L, abortedTxn.firstOffset)
    assertEquals(106L, abortedTxn.lastOffset)
    assertEquals(100L, abortedTxn.lastStableOffset)

    // recover again, but this time assuming the transaction from pid2 began on a previous segment
    stateManager = new ProducerStateManager(topicPartition, logDir)
    stateManager.loadProducerEntry(new ProducerStateEntry(pid2,
      mutable.Queue[BatchMetadata](BatchMetadata(10, 10L, 5, RecordBatch.NO_TIMESTAMP)), producerEpoch, 0, Some(75L)))
    segment.recover(stateManager)
    assertEquals(108L, stateManager.mapEndOffset)

    abortedTxns = segment.txnIndex.allAbortedTxns
    assertEquals(1, abortedTxns.size)
    abortedTxn = abortedTxns.head
    assertEquals(pid2, abortedTxn.producerId)
    assertEquals(75L, abortedTxn.firstOffset)
    assertEquals(106L, abortedTxn.lastOffset)
    assertEquals(100L, abortedTxn.lastStableOffset)
  }

  private def endTxnRecords(controlRecordType: ControlRecordType,
                            producerId: Long,
                            producerEpoch: Short,
                            offset: Long,
                            partitionLeaderEpoch: Int = 0,
                            coordinatorEpoch: Int = 0,
                            timestamp: Long = RecordBatch.NO_TIMESTAMP): MemoryRecords = {
    val marker = new EndTransactionMarker(controlRecordType, coordinatorEpoch)
    MemoryRecords.withEndTransactionMarker(offset, timestamp, partitionLeaderEpoch, producerId, producerEpoch, marker)
  }

  /**
   * Create a segment with some data and an index. Then corrupt the index,
   * and recover the segment, the entries should all be readable.
   */
  @Test
  def testRecoveryFixesCorruptTimeIndex() {
    val seg = createSegment(0)
    for(i <- 0 until 100)
      seg.append(i, i * 10, i, records(i, i.toString))
    val timeIndexFile = seg.timeIndex.file
    TestUtils.writeNonsenseToFile(timeIndexFile, 5, timeIndexFile.length.toInt)
    seg.recover(new ProducerStateManager(topicPartition, logDir))
    for(i <- 0 until 100) {
      assertEquals(i, seg.findOffsetByTimestamp(i * 10).get.offset)
      if (i < 99)
        assertEquals(i + 1, seg.findOffsetByTimestamp(i * 10 + 1).get.offset)
    }
  }

  /**
   * Randomly corrupt a log a number of times and attempt recovery.
   */
  @Test
  def testRecoveryWithCorruptMessage() {
    val messagesAppended = 20
    for (_ <- 0 until 10) {
      val seg = createSegment(0)
      for (i <- 0 until messagesAppended)
        seg.append(i, RecordBatch.NO_TIMESTAMP, -1L, records(i, i.toString))
      val offsetToBeginCorruption = TestUtils.random.nextInt(messagesAppended)
      // start corrupting somewhere in the middle of the chosen record all the way to the end

      val recordPosition = seg.log.searchForOffsetWithSize(offsetToBeginCorruption, 0)
      val position = recordPosition.position + TestUtils.random.nextInt(15)
      TestUtils.writeNonsenseToFile(seg.log.file, position, (seg.log.file.length - position).toInt)
      seg.recover(new ProducerStateManager(topicPartition, logDir))
      assertEquals("Should have truncated off bad messages.", (0 until offsetToBeginCorruption).toList,
        seg.log.batches.asScala.map(_.lastOffset).toList)
      seg.deleteIfExists()
    }
  }

  private def createSegment(baseOffset: Long, fileAlreadyExists: Boolean, initFileSize: Int, preallocate: Boolean): LogSegment = {
    val tempDir = TestUtils.tempDir()
    val logConfig = LogConfig(Map(
      LogConfig.IndexIntervalBytesProp -> 10,
      LogConfig.SegmentIndexBytesProp -> 1000,
      LogConfig.SegmentJitterMsProp -> 0
    ).asJava)
    val seg = LogSegment.open(tempDir, baseOffset, logConfig, Time.SYSTEM, fileAlreadyExists = fileAlreadyExists,
      initFileSize = initFileSize, preallocate = preallocate)
    segments += seg
    seg
  }

  /* create a segment with   pre allocate, put message to it and verify */
  @Test
  def testCreateWithInitFileSizeAppendMessage() {
    val seg = createSegment(40, false, 512*1024*1024, true)
    val ms = records(50, "hello", "there")
    seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms)
    val ms2 = records(60, "alpha", "beta")
    seg.append(61, RecordBatch.NO_TIMESTAMP, -1L, ms2)
    val read = seg.read(startOffset = 55, maxSize = 200, maxOffset = None)
    checkEquals(ms2.records.iterator, read.records.records.iterator)
  }

  /* create a segment with   pre allocate and clearly shut down*/
  @Test
  def testCreateWithInitFileSizeClearShutdown() {
    val tempDir = TestUtils.tempDir()
    val logConfig = LogConfig(Map(
      LogConfig.IndexIntervalBytesProp -> 10,
      LogConfig.SegmentIndexBytesProp -> 1000,
      LogConfig.SegmentJitterMsProp -> 0
    ).asJava)

    val seg = LogSegment.open(tempDir, baseOffset = 40, logConfig, Time.SYSTEM, fileAlreadyExists = false,
      initFileSize = 512 * 1024 * 1024, preallocate = true)

    val ms = records(50, "hello", "there")
    seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms)
    val ms2 = records(60, "alpha", "beta")
    seg.append(61, RecordBatch.NO_TIMESTAMP, -1L, ms2)
    val read = seg.read(startOffset = 55, maxSize = 200, maxOffset = None)
    checkEquals(ms2.records.iterator, read.records.records.iterator)
    val oldSize = seg.log.sizeInBytes()
    val oldPosition = seg.log.channel.position
    val oldFileSize = seg.log.file.length
    assertEquals(512*1024*1024, oldFileSize)
    seg.close()
    //After close, file should be trimmed
    assertEquals(oldSize, seg.log.file.length)

    val segReopen = LogSegment.open(tempDir, baseOffset = 40, logConfig, Time.SYSTEM, fileAlreadyExists = true,
      initFileSize = 512 * 1024 * 1024, preallocate = true)
    segments += segReopen

    val readAgain = segReopen.read(startOffset = 55, maxSize = 200, maxOffset = None)
    checkEquals(ms2.records.iterator, readAgain.records.records.iterator)
    val size = segReopen.log.sizeInBytes()
    val position = segReopen.log.channel.position
    val fileSize = segReopen.log.file.length
    assertEquals(oldPosition, position)
    assertEquals(oldSize, size)
    assertEquals(size, fileSize)
  }

  @Test
  def shouldTruncateEvenIfOffsetPointsToAGapInTheLog() {
    val seg = createSegment(40)
    val offset = 40

    def records(offset: Long, record: String): MemoryRecords =
      MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, offset, CompressionType.NONE, TimestampType.CREATE_TIME,
        new SimpleRecord(offset * 1000, record.getBytes))

    //Given two messages with a gap between them (e.g. mid offset compacted away)
    val ms1 = records(offset, "first message")
    seg.append(offset, RecordBatch.NO_TIMESTAMP, -1L, ms1)
    val ms2 = records(offset + 3, "message after gap")
    seg.append(offset + 3, RecordBatch.NO_TIMESTAMP, -1L, ms2)

    // When we truncate to an offset without a corresponding log entry
    seg.truncateTo(offset + 1)

    //Then we should still truncate the record that was present (i.e. offset + 3 is gone)
    val log = seg.read(offset, None, 10000)
    assertEquals(offset, log.records.batches.iterator.next().baseOffset())
    assertEquals(1, log.records.batches.asScala.size)
  }

  @Test
  def testAppendFromFile(): Unit = {
    def records(offset: Long, size: Int): MemoryRecords =
      MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, offset, CompressionType.NONE, TimestampType.CREATE_TIME,
        new SimpleRecord(new Array[Byte](size)))

    // create a log file in a separate directory to avoid conflicting with created segments
    val tempDir = TestUtils.tempDir()
    val fileRecords = FileRecords.open(Log.logFile(tempDir, 0))

    // Simulate a scenario where we have a single log with an offset range exceeding Int.MaxValue
    fileRecords.append(records(0, 1024))
    fileRecords.append(records(500, 1024 * 1024 + 1))
    val sizeBeforeOverflow = fileRecords.sizeInBytes()
    fileRecords.append(records(Int.MaxValue + 5L, 1024))
    val sizeAfterOverflow = fileRecords.sizeInBytes()

    val segment = createSegment(0)
    val bytesAppended = segment.appendFromFile(fileRecords, 0)
    assertEquals(sizeBeforeOverflow, bytesAppended)
    assertEquals(sizeBeforeOverflow, segment.size)

    val overflowSegment = createSegment(Int.MaxValue)
    val overflowBytesAppended = overflowSegment.appendFromFile(fileRecords, sizeBeforeOverflow)
    assertEquals(sizeAfterOverflow - sizeBeforeOverflow, overflowBytesAppended)
    assertEquals(overflowBytesAppended, overflowSegment.size)

    Utils.delete(tempDir)
  }

}
