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

import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.utils.TestUtils
import kafka.utils.TestUtils.checkEquals
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{MockTime, Time, Utils}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class LogSegmentTest {
  private val topicPartition = new TopicPartition("topic", 0)
  private val segments = mutable.ArrayBuffer[LogSegment]()
  private var logDir: File = _

  /* create a segment with the given base offset */
  def createSegment(offset: Long,
                    indexIntervalBytes: Int = 10,
                    time: Time = Time.SYSTEM): LogSegment = {
    val seg = LogTestUtils.createSegment(offset, logDir, indexIntervalBytes, time)
    segments += seg
    seg
  }

  /* create a ByteBufferMessageSet for the given messages starting from the given offset */
  def records(offset: Long, records: String*): MemoryRecords = {
    MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, offset, CompressionType.NONE, TimestampType.CREATE_TIME,
      records.map { s => new SimpleRecord(offset * 10, s.getBytes) }: _*)
  }

  @BeforeEach
  def setup(): Unit = {
    logDir = TestUtils.tempDir()
  }

  @AfterEach
  def teardown(): Unit = {
    segments.foreach(_.close())
    Utils.delete(logDir)
  }

  /**
   * A read on an empty log segment should return null
   */
  @Test
  def testReadOnEmptySegment(): Unit = {
    val seg = createSegment(40)
    val read = seg.read(startOffset = 40, maxSize = 300)
    assertNull(read, "Read beyond the last offset in the segment should be null")
  }

  /**
   * Reading from before the first offset in the segment should return messages
   * beginning with the first message in the segment
   */
  @Test
  def testReadBeforeFirstOffset(): Unit = {
    val seg = createSegment(40)
    val ms = records(50, "hello", "there", "little", "bee")
    seg.append(53, RecordBatch.NO_TIMESTAMP, -1L, ms)
    val read = seg.read(startOffset = 41, maxSize = 300).records
    checkEquals(ms.records.iterator, read.records.iterator)
  }

  /**
   * If we read from an offset beyond the last offset in the segment we should get null
   */
  @Test
  def testReadAfterLast(): Unit = {
    val seg = createSegment(40)
    val ms = records(50, "hello", "there")
    seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms)
    val read = seg.read(startOffset = 52, maxSize = 200)
    assertNull(read, "Read beyond the last offset in the segment should give null")
  }

  /**
   * If we read from an offset which doesn't exist we should get a message set beginning
   * with the least offset greater than the given startOffset.
   */
  @Test
  def testReadFromGap(): Unit = {
    val seg = createSegment(40)
    val ms = records(50, "hello", "there")
    seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms)
    val ms2 = records(60, "alpha", "beta")
    seg.append(61, RecordBatch.NO_TIMESTAMP, -1L, ms2)
    val read = seg.read(startOffset = 55, maxSize = 200)
    checkEquals(ms2.records.iterator, read.records.records.iterator)
  }

  /**
   * In a loop append two messages then truncate off the second of those messages and check that we can read
   * the first but not the second message.
   */
  @Test
  def testTruncate(): Unit = {
    val seg = createSegment(40)
    var offset = 40
    for (_ <- 0 until 30) {
      val ms1 = records(offset, "hello")
      seg.append(offset, RecordBatch.NO_TIMESTAMP, -1L, ms1)
      val ms2 = records(offset + 1, "hello")
      seg.append(offset + 1, RecordBatch.NO_TIMESTAMP, -1L, ms2)
      // check that we can read back both messages
      val read = seg.read(offset, 10000)
      assertEquals(List(ms1.records.iterator.next(), ms2.records.iterator.next()), read.records.records.asScala.toList)
      // now truncate off the last message
      seg.truncateTo(offset + 1)
      val read2 = seg.read(offset, 10000)
      assertEquals(1, read2.records.records.asScala.size)
      checkEquals(ms1.records.iterator, read2.records.records.iterator)
      offset += 1
    }
  }

  @Test
  def testTruncateEmptySegment(): Unit = {
    // This tests the scenario in which the follower truncates to an empty segment. In this
    // case we must ensure that the index is resized so that the log segment is not mistakenly
    // rolled due to a full index

    val maxSegmentMs = 300000
    val time = new MockTime
    val seg = createSegment(0, time = time)
    // Force load indexes before closing the segment
    seg.timeIndex
    seg.offsetIndex
    seg.close()

    val reopened = createSegment(0, time = time)
    assertEquals(0, seg.timeIndex.sizeInBytes)
    assertEquals(0, seg.offsetIndex.sizeInBytes)

    time.sleep(500)
    reopened.truncateTo(57)
    assertEquals(0, reopened.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP))
    assertFalse(reopened.timeIndex.isFull)
    assertFalse(reopened.offsetIndex.isFull)

    var rollParams = RollParams(maxSegmentMs, maxSegmentBytes = Int.MaxValue, RecordBatch.NO_TIMESTAMP,
      maxOffsetInMessages = 100L, messagesSize = 1024, time.milliseconds())
    assertFalse(reopened.shouldRoll(rollParams))

    // The segment should not be rolled even if maxSegmentMs has been exceeded
    time.sleep(maxSegmentMs + 1)
    assertEquals(maxSegmentMs + 1, reopened.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP))
    rollParams = RollParams(maxSegmentMs, maxSegmentBytes = Int.MaxValue, RecordBatch.NO_TIMESTAMP,
      maxOffsetInMessages = 100L, messagesSize = 1024, time.milliseconds())
    assertFalse(reopened.shouldRoll(rollParams))

    // But we should still roll the segment if we cannot fit the next offset
    rollParams = RollParams(maxSegmentMs, maxSegmentBytes = Int.MaxValue, RecordBatch.NO_TIMESTAMP,
      maxOffsetInMessages = Int.MaxValue.toLong + 200L, messagesSize = 1024, time.milliseconds())
    assertTrue(reopened.shouldRoll(rollParams))
  }

  @Test
  def testReloadLargestTimestampAndNextOffsetAfterTruncation(): Unit = {
    val numMessages = 30
    val seg = createSegment(40, 2 * records(0, "hello").sizeInBytes - 1)
    var offset = 40
    for (_ <- 0 until numMessages) {
      seg.append(offset, offset, offset, records(offset, "hello"))
      offset += 1
    }
    assertEquals(offset, seg.readNextOffset)

    val expectedNumEntries = numMessages / 2 - 1
    assertEquals(expectedNumEntries, seg.timeIndex.entries, s"Should have $expectedNumEntries time indexes")

    seg.truncateTo(41)
    assertEquals(0, seg.timeIndex.entries, s"Should have 0 time indexes")
    assertEquals(400L, seg.largestTimestamp, s"Largest timestamp should be 400")
    assertEquals(41, seg.readNextOffset)
  }

  /**
   * Test truncating the whole segment, and check that we can reappend with the original offset.
   */
  @Test
  def testTruncateFull(): Unit = {
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
    assertNull(seg.read(0, 1024), "Segment should be empty.")

    seg.append(41, RecordBatch.NO_TIMESTAMP, -1L, records(40, "hello", "there"))
  }

  /**
   * Append messages with timestamp and search message by timestamp.
   */
  @Test
  def testFindOffsetByTimestamp(): Unit = {
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
  def testNextOffsetCalculation(): Unit = {
    val seg = createSegment(40)
    assertEquals(40, seg.readNextOffset)
    seg.append(52, RecordBatch.NO_TIMESTAMP, -1L, records(50, "hello", "there", "you"))
    assertEquals(53, seg.readNextOffset)
  }

  /**
   * Test that we can change the file suffixes for the log and index files
   */
  @Test
  def testChangeFileSuffixes(): Unit = {
    val seg = createSegment(40)
    val logFile = seg.log.file
    val indexFile = seg.lazyOffsetIndex.file
    val timeIndexFile = seg.lazyTimeIndex.file
    // Ensure that files for offset and time indices have not been created eagerly.
    assertFalse(seg.lazyOffsetIndex.file.exists)
    assertFalse(seg.lazyTimeIndex.file.exists)
    seg.changeFileSuffixes("", ".deleted")
    // Ensure that attempt to change suffixes for non-existing offset and time indices does not create new files.
    assertFalse(seg.lazyOffsetIndex.file.exists)
    assertFalse(seg.lazyTimeIndex.file.exists)
    // Ensure that file names are updated accordingly.
    assertEquals(logFile.getAbsolutePath + ".deleted", seg.log.file.getAbsolutePath)
    assertEquals(indexFile.getAbsolutePath + ".deleted", seg.lazyOffsetIndex.file.getAbsolutePath)
    assertEquals(timeIndexFile.getAbsolutePath + ".deleted", seg.lazyTimeIndex.file.getAbsolutePath)
    assertTrue(seg.log.file.exists)
    // Ensure lazy creation of offset index file upon accessing it.
    seg.lazyOffsetIndex.get
    assertTrue(seg.lazyOffsetIndex.file.exists)
    // Ensure lazy creation of time index file upon accessing it.
    seg.lazyTimeIndex.get
    assertTrue(seg.lazyTimeIndex.file.exists)
  }

  /**
   * Create a segment with some data and an index. Then corrupt the index,
   * and recover the segment, the entries should all be readable.
   */
  @Test
  def testRecoveryFixesCorruptIndex(): Unit = {
    val seg = createSegment(0)
    for(i <- 0 until 100)
      seg.append(i, RecordBatch.NO_TIMESTAMP, -1L, records(i, i.toString))
    val indexFile = seg.lazyOffsetIndex.file
    TestUtils.writeNonsenseToFile(indexFile, 5, indexFile.length.toInt)
    seg.recover(newProducerStateManager())
    for(i <- 0 until 100) {
      val records = seg.read(i, 1, minOneMessage = true).records.records
      assertEquals(i, records.iterator.next().offset)
    }
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
      shallowOffsetOfMaxTimestamp = 100L, records = MemoryRecords.withTransactionalRecords(100L, CompressionType.NONE,
        pid1, producerEpoch, sequence, partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    // append transactional records from pid2
    segment.append(largestOffset = 103L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 102L, records = MemoryRecords.withTransactionalRecords(102L, CompressionType.NONE,
        pid2, producerEpoch, sequence, partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    // append non-transactional records
    segment.append(largestOffset = 105L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 104L, records = MemoryRecords.withRecords(104L, CompressionType.NONE,
        partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    // abort the transaction from pid2 (note LSO should be 100L since the txn from pid1 has not completed)
    segment.append(largestOffset = 106L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 106L, records = endTxnRecords(ControlRecordType.ABORT, pid2, producerEpoch, offset = 106L))

    // commit the transaction from pid1
    segment.append(largestOffset = 107L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 107L, records = endTxnRecords(ControlRecordType.COMMIT, pid1, producerEpoch, offset = 107L))

    var stateManager = newProducerStateManager()
    segment.recover(stateManager)
    assertEquals(108L, stateManager.mapEndOffset)


    var abortedTxns = segment.txnIndex.allAbortedTxns
    assertEquals(1, abortedTxns.size)
    var abortedTxn = abortedTxns.get(0)
    assertEquals(pid2, abortedTxn.producerId)
    assertEquals(102L, abortedTxn.firstOffset)
    assertEquals(106L, abortedTxn.lastOffset)
    assertEquals(100L, abortedTxn.lastStableOffset)

    // recover again, but this time assuming the transaction from pid2 began on a previous segment
    stateManager = newProducerStateManager()
    stateManager.loadProducerEntry(new ProducerStateEntry(pid2,
      mutable.Queue[BatchMetadata](BatchMetadata(10, 10L, 5, RecordBatch.NO_TIMESTAMP)), producerEpoch,
      0, RecordBatch.NO_TIMESTAMP, Some(75L)))
    segment.recover(stateManager)
    assertEquals(108L, stateManager.mapEndOffset)

    abortedTxns = segment.txnIndex.allAbortedTxns
    assertEquals(1, abortedTxns.size)
    abortedTxn = abortedTxns.get(0)
    assertEquals(pid2, abortedTxn.producerId)
    assertEquals(75L, abortedTxn.firstOffset)
    assertEquals(106L, abortedTxn.lastOffset)
    assertEquals(100L, abortedTxn.lastStableOffset)
  }

  /**
   * Create a segment with some data, then recover the segment.
   * The epoch cache entries should reflect the segment.
   */
  @Test
  def testRecoveryRebuildsEpochCache(): Unit = {
    val seg = createSegment(0)

    val checkpoint: LeaderEpochCheckpoint = new LeaderEpochCheckpoint {
      private var epochs = Seq.empty[EpochEntry]

      override def write(epochs: Iterable[EpochEntry]): Unit = {
        this.epochs = epochs.toVector
      }

      override def read(): Seq[EpochEntry] = this.epochs
    }

    val cache = new LeaderEpochFileCache(topicPartition, checkpoint)
    seg.append(largestOffset = 105L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 104L, records = MemoryRecords.withRecords(104L, CompressionType.NONE, 0,
        new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    seg.append(largestOffset = 107L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 106L, records = MemoryRecords.withRecords(106L, CompressionType.NONE, 1,
        new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    seg.append(largestOffset = 109L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 108L, records = MemoryRecords.withRecords(108L, CompressionType.NONE, 1,
        new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    seg.append(largestOffset = 111L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
      shallowOffsetOfMaxTimestamp = 110, records = MemoryRecords.withRecords(110L, CompressionType.NONE, 2,
        new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    seg.recover(newProducerStateManager(), Some(cache))
    assertEquals(ArrayBuffer(EpochEntry(epoch = 0, startOffset = 104L),
                             EpochEntry(epoch = 1, startOffset = 106),
                             EpochEntry(epoch = 2, startOffset = 110)),
      cache.epochEntries)
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
  def testRecoveryFixesCorruptTimeIndex(): Unit = {
    val seg = createSegment(0)
    for(i <- 0 until 100)
      seg.append(i, i * 10, i, records(i, i.toString))
    val timeIndexFile = seg.lazyTimeIndex.file
    TestUtils.writeNonsenseToFile(timeIndexFile, 5, timeIndexFile.length.toInt)
    seg.recover(newProducerStateManager())
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
  def testRecoveryWithCorruptMessage(): Unit = {
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
      seg.recover(newProducerStateManager())
      assertEquals((0 until offsetToBeginCorruption).toList, seg.log.batches.asScala.map(_.lastOffset).toList,
        "Should have truncated off bad messages.")
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
  def testCreateWithInitFileSizeAppendMessage(): Unit = {
    val seg = createSegment(40, false, 512*1024*1024, true)
    val ms = records(50, "hello", "there")
    seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms)
    val ms2 = records(60, "alpha", "beta")
    seg.append(61, RecordBatch.NO_TIMESTAMP, -1L, ms2)
    val read = seg.read(startOffset = 55, maxSize = 200)
    checkEquals(ms2.records.iterator, read.records.records.iterator)
  }

  /* create a segment with   pre allocate and clearly shut down*/
  @Test
  def testCreateWithInitFileSizeClearShutdown(): Unit = {
    val tempDir = TestUtils.tempDir()
    val logConfig = LogConfig(Map(
      LogConfig.IndexIntervalBytesProp -> 10,
      LogConfig.SegmentIndexBytesProp -> 1000,
      LogConfig.SegmentJitterMsProp -> 0
    ).asJava)

    val seg = LogSegment.open(tempDir, baseOffset = 40, logConfig, Time.SYSTEM,
      initFileSize = 512 * 1024 * 1024, preallocate = true)

    val ms = records(50, "hello", "there")
    seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms)
    val ms2 = records(60, "alpha", "beta")
    seg.append(61, RecordBatch.NO_TIMESTAMP, -1L, ms2)
    val read = seg.read(startOffset = 55, maxSize = 200)
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

    val readAgain = segReopen.read(startOffset = 55, maxSize = 200)
    checkEquals(ms2.records.iterator, readAgain.records.records.iterator)
    val size = segReopen.log.sizeInBytes()
    val position = segReopen.log.channel.position
    val fileSize = segReopen.log.file.length
    assertEquals(oldPosition, position)
    assertEquals(oldSize, size)
    assertEquals(size, fileSize)
  }

  @Test
  def shouldTruncateEvenIfOffsetPointsToAGapInTheLog(): Unit = {
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
    val log = seg.read(offset, 10000)
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
    val fileRecords = FileRecords.open(UnifiedLog.logFile(tempDir, 0))

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

  private def newProducerStateManager(): ProducerStateManager = {
    new ProducerStateManager(
      topicPartition,
      logDir,
      maxTransactionTimeoutMs = 5 * 60 * 1000,
      producerStateManagerConfig = new ProducerStateManagerConfig(kafka.server.Defaults.ProducerIdExpirationMs),
      time = new MockTime()
    )
  }

}
