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

import java.io._
import java.util.Properties

import org.apache.kafka.common.errors.{CorruptRecordException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException}
import kafka.api.ApiVersion
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Before, Test}
import kafka.utils._
import kafka.server.KafkaConfig
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

class LogTest extends JUnitSuite {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val time = new MockTime()
  var config: KafkaConfig = null
  val logConfig = LogConfig()

  @Before
  def setUp() {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    config = KafkaConfig.fromProps(props)
  }

  @After
  def tearDown() {
    Utils.delete(tmpDir)
  }

  def createEmptyLogs(dir: File, offsets: Int*) {
    for(offset <- offsets) {
      Log.logFilename(dir, offset).createNewFile()
      Log.indexFilename(dir, offset).createNewFile()
    }
  }

  /**
   * Tests for time based log roll. This test appends messages then changes the time
   * using the mock clock to force the log to roll and checks the number of segments.
   */
  @Test
  def testTimeBasedLogRoll() {
    val set = TestUtils.singletonRecords("test".getBytes)

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentMsProp, (1 * 60 * 60L): java.lang.Long)

    // create a log
    val log = new Log(logDir,
                      LogConfig(logProps),
                      recoveryPoint = 0L,
                      scheduler = time.scheduler,
                      time = time)
    assertEquals("Log begins with a single empty segment.", 1, log.numberOfSegments)
    // Test the segment rolling behavior when messages do not have a timestamp.
    time.sleep(log.config.segmentMs + 1)
    log.append(set)
    assertEquals("Log doesn't roll if doing so creates an empty segment.", 1, log.numberOfSegments)

    log.append(set)
    assertEquals("Log rolls on this append since time has expired.", 2, log.numberOfSegments)

    for(numSegments <- 3 until 5) {
      time.sleep(log.config.segmentMs + 1)
      log.append(set)
      assertEquals("Changing time beyond rollMs and appending should create a new segment.", numSegments, log.numberOfSegments)
    }

    // Append a message with timestamp to a segment whose first messgae do not have a timestamp.
    val setWithTimestamp =
      TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds + log.config.segmentMs + 1)
    log.append(setWithTimestamp)
    assertEquals("Segment should not have been rolled out because the log rolling should be based on wall clock.", 4, log.numberOfSegments)

    // Test the segment rolling behavior when messages have timestamps.
    time.sleep(log.config.segmentMs + 1)
    log.append(setWithTimestamp)
    assertEquals("A new segment should have been rolled out", 5, log.numberOfSegments)

    // move the wall clock beyond log rolling time
    time.sleep(log.config.segmentMs + 1)
    log.append(setWithTimestamp)
    assertEquals("Log should not roll because the roll should depend on timestamp of the first message.", 5, log.numberOfSegments)

    val setWithExpiredTimestamp = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    log.append(setWithExpiredTimestamp)
    assertEquals("Log should roll because the timestamp in the message should make the log segment expire.", 6, log.numberOfSegments)

    val numSegments = log.numberOfSegments
    time.sleep(log.config.segmentMs + 1)
    log.append(MemoryRecords.withLogEntries())
    assertEquals("Appending an empty message set should not roll log even if sufficient time has passed.", numSegments, log.numberOfSegments)
  }

  /**
   * Test for jitter s for time based log roll. This test appends messages then changes the time
   * using the mock clock to force the log to roll and checks the number of segments.
   */
  @Test
  def testTimeBasedLogRollJitter() {
    val set = TestUtils.singletonRecords("test".getBytes)
    val maxJitter = 20 * 60L

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentMsProp, 1 * 60 * 60: java.lang.Long)
    logProps.put(LogConfig.SegmentJitterMsProp, maxJitter: java.lang.Long)
    // create a log
    val log = new Log(logDir,
      LogConfig(logProps),
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      time = time)
    assertEquals("Log begins with a single empty segment.", 1, log.numberOfSegments)
    log.append(set)

    time.sleep(log.config.segmentMs - maxJitter)
    log.append(set)
    assertEquals("Log does not roll on this append because it occurs earlier than max jitter", 1, log.numberOfSegments)
    time.sleep(maxJitter - log.activeSegment.rollJitterMs + 1)
    log.append(set)
    assertEquals("Log should roll after segmentMs adjusted by random jitter", 2, log.numberOfSegments)
  }

  /**
   * Test that appending more than the maximum segment size rolls the log
   */
  @Test
  def testSizeBasedLogRoll() {
    val set = TestUtils.singletonRecords("test".getBytes)
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * (setSize - 1) // each segment will be 10 messages

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    // We use need to use magic value 1 here because the test is message size sensitive.
    logProps.put(LogConfig.MessageFormatVersionProp, ApiVersion.latestVersion.toString)
    // create a log
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    // segments expire in size
    for (_ <- 1 to (msgPerSeg + 1)) {
      log.append(set)
    }
    assertEquals("There should be exactly 2 segments.", 2, log.numberOfSegments)
  }

  /**
   * Test that we can open and append to an empty log
   */
  @Test
  def testLoadEmptyLog() {
    createEmptyLogs(logDir, 0)
    val log = new Log(logDir, logConfig, recoveryPoint = 0L, time.scheduler, time = time)
    log.append(TestUtils.singletonRecords("test".getBytes))
  }

  /**
   * This test case appends a bunch of messages and checks that we can read them all back using sequential offsets.
   */
  @Test
  def testAppendAndReadWithSequentialOffsets() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 71: java.lang.Integer)
    // We use need to use magic value 1 here because the test is message size sensitive.
    logProps.put(LogConfig.MessageFormatVersionProp, ApiVersion.latestVersion.toString)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    val records = (0 until 100 by 2).map(id => Record.create(id.toString.getBytes)).toArray

    for(i <- records.indices)
      log.append(MemoryRecords.withRecords(records(i)))

    for(i <- records.indices) {
      val read = log.read(i, 100, Some(i+1)).records.shallowEntries.iterator.next()
      assertEquals("Offset read should match order appended.", i, read.offset)
      assertEquals("Message should match appended.", records(i), read.record)
    }
    assertEquals("Reading beyond the last message returns nothing.", 0, log.read(records.length, 100, None).records.shallowEntries.asScala.size)
  }

  /**
   * This test appends a bunch of messages with non-sequential offsets and checks that we can read the correct message
   * from any offset less than the logEndOffset including offsets not appended.
   */
  @Test
  def testAppendAndReadWithNonSequentialOffsets() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 71: java.lang.Integer)
    val log = new Log(logDir,  LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val records = messageIds.map(id => Record.create(id.toString.getBytes))

    // now test the case that we give the offsets and use non-sequential offsets
    for(i <- records.indices)
      log.append(MemoryRecords.withLogEntries(LogEntry.create(messageIds(i), records(i))), assignOffsets = false)
    for(i <- 50 until messageIds.max) {
      val idx = messageIds.indexWhere(_ >= i)
      val read = log.read(i, 100, None).records.shallowEntries.iterator.next()
      assertEquals("Offset read should match message id.", messageIds(idx), read.offset)
      assertEquals("Message should match appended.", records(idx), read.record)
    }
  }

  /**
   * This test covers an odd case where we have a gap in the offsets that falls at the end of a log segment.
   * Specifically we create a log where the last message in the first segment has offset 0. If we
   * then read offset 1, we should expect this read to come from the second segment, even though the
   * first segment has the greatest lower bound on the offset.
   */
  @Test
  def testReadAtLogGap() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 300: java.lang.Integer)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)

    // keep appending until we have two segments with only a single message in the second segment
    while(log.numberOfSegments == 1)
      log.append(MemoryRecords.withRecords(Record.create("42".getBytes)))

    // now manually truncate off all but one message from the first segment to create a gap in the messages
    log.logSegments.head.truncateTo(1)

    assertEquals("A read should now return the last message in the log", log.logEndOffset - 1,
      log.read(1, 200, None).records.shallowEntries.iterator.next().offset)
  }

  @Test
  def testReadWithMinMessage() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 71: java.lang.Integer)
    val log = new Log(logDir,  LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val records = messageIds.map(id => Record.create(id.toString.getBytes))

    // now test the case that we give the offsets and use non-sequential offsets
    for (i <- records.indices)
      log.append(MemoryRecords.withLogEntries(LogEntry.create(messageIds(i), records(i))), assignOffsets = false)

    for (i <- 50 until messageIds.max) {
      val idx = messageIds.indexWhere(_ >= i)
      val reads = Seq(
        log.read(i, 1, minOneMessage = true),
        log.read(i, 100, minOneMessage = true),
        log.read(i, 100, Some(10000), minOneMessage = true)
      ).map(_.records.shallowEntries.iterator.next())
      reads.foreach { read =>
        assertEquals("Offset read should match message id.", messageIds(idx), read.offset)
        assertEquals("Message should match appended.", records(idx), read.record)
      }

      assertEquals(Seq.empty, log.read(i, 1, Some(1), minOneMessage = true).records.shallowEntries.asScala.toIndexedSeq)
    }

  }

  @Test
  def testReadWithTooSmallMaxLength() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 71: java.lang.Integer)
    val log = new Log(logDir,  LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val records = messageIds.map(id => Record.create(id.toString.getBytes))

    // now test the case that we give the offsets and use non-sequential offsets
    for (i <- records.indices)
      log.append(MemoryRecords.withLogEntries(LogEntry.create(messageIds(i), records(i))), assignOffsets = false)

    for (i <- 50 until messageIds.max) {
      assertEquals(MemoryRecords.EMPTY, log.read(i, 0).records)

      // we return an incomplete message instead of an empty one for the case below
      // we use this mechanism to tell consumers of the fetch request version 2 and below that the message size is
      // larger than the fetch size
      // in fetch request version 3, we no longer need this as we return oversized messages from the first non-empty
      // partition
      val fetchInfo = log.read(i, 1)
      assertTrue(fetchInfo.firstEntryIncomplete)
      assertTrue(fetchInfo.records.isInstanceOf[FileRecords])
      assertEquals(1, fetchInfo.records.sizeInBytes)
    }
  }

  /**
   * Test reading at the boundary of the log, specifically
   * - reading from the logEndOffset should give an empty message set
   * - reading from the maxOffset should give an empty message set
   * - reading beyond the log end offset should throw an OffsetOutOfRangeException
   */
  @Test
  def testReadOutOfRange() {
    createEmptyLogs(logDir, 1024)
    val logProps = new Properties()

    // set up replica log starting with offset 1024 and with one message (at offset 1024)
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    log.append(MemoryRecords.withRecords(Record.create("42".getBytes)))

    assertEquals("Reading at the log end offset should produce 0 byte read.", 0, log.read(1025, 1000).records.sizeInBytes)

    try {
      log.read(0, 1000)
      fail("Reading below the log start offset should throw OffsetOutOfRangeException")
    } catch {
      case _: OffsetOutOfRangeException => // This is good.
    }

    try {
      log.read(1026, 1000)
      fail("Reading at beyond the log end offset should throw OffsetOutOfRangeException")
    } catch {
      case _: OffsetOutOfRangeException => // This is good.
    }

    assertEquals("Reading from below the specified maxOffset should produce 0 byte read.", 0, log.read(1025, 1000, Some(1024)).records.sizeInBytes)
  }

  /**
   * Test that covers reads and writes on a multisegment log. This test appends a bunch of messages
   * and then reads them all back and checks that the message read and offset matches what was appended.
   */
  @Test
  def testLogRolls() {
    /* create a multipart log with 100 messages */
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    val numMessages = 100
    val messageSets = (0 until numMessages).map(i => TestUtils.singletonRecords(i.toString.getBytes))
    messageSets.foreach(log.append(_))
    log.flush()

    /* do successive reads to ensure all our messages are there */
    var offset = 0L
    for(i <- 0 until numMessages) {
      val messages = log.read(offset, 1024*1024).records.shallowEntries
      val head = messages.iterator.next()
      assertEquals("Offsets not equal", offset, head.offset)
      assertEquals("Messages not equal at offset " + offset, messageSets(i).shallowEntries.iterator.next().record,
        head.record.convert(messageSets(i).shallowEntries.iterator.next().record.magic, TimestampType.NO_TIMESTAMP_TYPE))
      offset = head.offset + 1
    }
    val lastRead = log.read(startOffset = numMessages, maxLength = 1024*1024, maxOffset = Some(numMessages + 1)).records
    assertEquals("Should be no more messages", 0, lastRead.shallowEntries.asScala.size)

    // check that rolling the log forced a flushed the log--the flush is asyn so retry in case of failure
    TestUtils.retry(1000L){
      assertTrue("Log role should have forced flush", log.recoveryPoint >= log.activeSegment.baseOffset)
    }
  }

  /**
   * Test reads at offsets that fall within compressed message set boundaries.
   */
  @Test
  def testCompressedMessages() {
    /* this log should roll after every messageset */
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)

    /* append 2 compressed message sets, each with two messages giving offsets 0, 1, 2, 3 */
    log.append(MemoryRecords.withRecords(CompressionType.GZIP, Record.create("hello".getBytes), Record.create("there".getBytes)))
    log.append(MemoryRecords.withRecords(CompressionType.GZIP, Record.create("alpha".getBytes), Record.create("beta".getBytes)))

    def read(offset: Int) = log.read(offset, 4096).records.deepEntries.iterator

    /* we should always get the first message in the compressed set when reading any offset in the set */
    assertEquals("Read at offset 0 should produce 0", 0, read(0).next().offset)
    assertEquals("Read at offset 1 should produce 0", 0, read(1).next().offset)
    assertEquals("Read at offset 2 should produce 2", 2, read(2).next().offset)
    assertEquals("Read at offset 3 should produce 2", 2, read(3).next().offset)
  }

  /**
   * Test garbage collecting old segments
   */
  @Test
  def testThatGarbageCollectingSegmentsDoesntChangeOffset() {
    for(messagesToAppend <- List(0, 1, 25)) {
      logDir.mkdirs()
      // first test a log segment starting at 0
      val logProps = new Properties()
      logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
      logProps.put(LogConfig.RetentionMsProp, 0: java.lang.Integer)
      val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
      for(i <- 0 until messagesToAppend)
        log.append(TestUtils.singletonRecords(value = i.toString.getBytes, timestamp = time.milliseconds - 10))

      val currOffset = log.logEndOffset
      assertEquals(currOffset, messagesToAppend)

      // time goes by; the log file is deleted
      log.deleteOldSegments()

      assertEquals("Deleting segments shouldn't have changed the logEndOffset", currOffset, log.logEndOffset)
      assertEquals("We should still have one segment left", 1, log.numberOfSegments)
      assertEquals("Further collection shouldn't delete anything", 0, log.deleteOldSegments())
      assertEquals("Still no change in the logEndOffset", currOffset, log.logEndOffset)
      assertEquals("Should still be able to append and should get the logEndOffset assigned to the new append",
                   currOffset,
                   log.append(TestUtils.singletonRecords("hello".getBytes)).firstOffset)

      // cleanup the log
      log.delete()
    }
  }

  /**
   *  MessageSet size shouldn't exceed the config.segmentSize, check that it is properly enforced by
   * appending a message set larger than the config.segmentSize setting and checking that an exception is thrown.
   */
  @Test
  def testMessageSetSizeCheck() {
    val messageSet = MemoryRecords.withRecords(Record.create("You".getBytes), Record.create("bethe".getBytes))
    // append messages to log
    val configSegmentSize = messageSet.sizeInBytes - 1
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, configSegmentSize: java.lang.Integer)
    // We use need to use magic value 1 here because the test is message size sensitive.
    logProps.put(LogConfig.MessageFormatVersionProp, ApiVersion.latestVersion.toString)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)

    try {
      log.append(messageSet)
      fail("message set should throw RecordBatchTooLargeException.")
    } catch {
      case _: RecordBatchTooLargeException => // this is good
    }
  }

  @Test
  def testCompactedTopicConstraints() {
    val keyedMessage = Record.create(Record.CURRENT_MAGIC_VALUE, Record.NO_TIMESTAMP, "and here it is".getBytes, "this message has a key".getBytes)
    val anotherKeyedMessage = Record.create(Record.CURRENT_MAGIC_VALUE, Record.NO_TIMESTAMP, "another key".getBytes, "this message also has a key".getBytes)
    val unkeyedMessage = Record.create("this message does not have a key".getBytes)

    val messageSetWithUnkeyedMessage = MemoryRecords.withRecords(CompressionType.NONE, unkeyedMessage, keyedMessage)
    val messageSetWithOneUnkeyedMessage = MemoryRecords.withRecords(CompressionType.NONE, unkeyedMessage)
    val messageSetWithCompressedKeyedMessage = MemoryRecords.withRecords(CompressionType.GZIP, keyedMessage)
    val messageSetWithCompressedUnkeyedMessage = MemoryRecords.withRecords(CompressionType.GZIP, keyedMessage, unkeyedMessage)

    val messageSetWithKeyedMessage = MemoryRecords.withRecords(CompressionType.NONE, keyedMessage)
    val messageSetWithKeyedMessages = MemoryRecords.withRecords(CompressionType.NONE, keyedMessage, anotherKeyedMessage)

    val logProps = new Properties()
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)

    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time)

    try {
      log.append(messageSetWithUnkeyedMessage)
      fail("Compacted topics cannot accept a message without a key.")
    } catch {
      case _: CorruptRecordException => // this is good
    }
    try {
      log.append(messageSetWithOneUnkeyedMessage)
      fail("Compacted topics cannot accept a message without a key.")
    } catch {
      case _: CorruptRecordException => // this is good
    }
    try {
      log.append(messageSetWithCompressedUnkeyedMessage)
      fail("Compacted topics cannot accept a message without a key.")
    } catch {
      case _: CorruptRecordException => // this is good
    }

    // the following should succeed without any InvalidMessageException
    log.append(messageSetWithKeyedMessage)
    log.append(messageSetWithKeyedMessages)
    log.append(messageSetWithCompressedKeyedMessage)
  }

  /**
   * We have a max size limit on message appends, check that it is properly enforced by appending a message larger than the
   * setting and checking that an exception is thrown.
   */
  @Test
  def testMessageSizeCheck() {
    val first = MemoryRecords.withRecords(CompressionType.NONE, Record.create("You".getBytes), Record.create("bethe".getBytes))
    val second = MemoryRecords.withRecords(CompressionType.NONE, Record.create("change (I need more bytes)".getBytes))

    // append messages to log
    val maxMessageSize = second.sizeInBytes - 1
    val logProps = new Properties()
    logProps.put(LogConfig.MaxMessageBytesProp, maxMessageSize: java.lang.Integer)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)

    // should be able to append the small message
    log.append(first)

    try {
      log.append(second)
      fail("Second message set should throw MessageSizeTooLargeException.")
    } catch {
      case _: RecordTooLargeException => // this is good
    }
  }
  /**
   * Append a bunch of messages to a log and then re-open it both with and without recovery and check that the log re-initializes correctly.
   */
  @Test
  def testLogRecoversToCorrectOffset() {
    val numMessages = 100
    val messageSize = 100
    val segmentSize = 7 * messageSize
    val indexInterval = 3 * messageSize
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, indexInterval: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 4096: java.lang.Integer)
    val config = LogConfig(logProps)
    var log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)
    for(i <- 0 until numMessages)
      log.append(TestUtils.singletonRecords(value = TestUtils.randomBytes(messageSize),
        timestamp = time.milliseconds + i * 10))
    assertEquals("After appending %d messages to an empty log, the log end offset should be %d".format(numMessages, numMessages), numMessages, log.logEndOffset)
    val lastIndexOffset = log.activeSegment.index.lastOffset
    val numIndexEntries = log.activeSegment.index.entries
    val lastOffset = log.logEndOffset
    // After segment is closed, the last entry in the time index should be (largest timestamp -> last offset).
    val lastTimeIndexOffset = log.logEndOffset - 1
    val lastTimeIndexTimestamp  = log.activeSegment.largestTimestamp
    // Depending on when the last time index entry is inserted, an entry may or may not be inserted into the time index.
    val numTimeIndexEntries = log.activeSegment.timeIndex.entries + {
      if (log.activeSegment.timeIndex.lastEntry.offset == log.logEndOffset - 1) 0 else 1
    }
    log.close()

    def verifyRecoveredLog(log: Log) {
      assertEquals(s"Should have $numMessages messages when log is reopened w/o recovery", numMessages, log.logEndOffset)
      assertEquals("Should have same last index offset as before.", lastIndexOffset, log.activeSegment.index.lastOffset)
      assertEquals("Should have same number of index entries as before.", numIndexEntries, log.activeSegment.index.entries)
      assertEquals("Should have same last time index timestamp", lastTimeIndexTimestamp, log.activeSegment.timeIndex.lastEntry.timestamp)
      assertEquals("Should have same last time index offset", lastTimeIndexOffset, log.activeSegment.timeIndex.lastEntry.offset)
      assertEquals("Should have same number of time index entries as before.", numTimeIndexEntries, log.activeSegment.timeIndex.entries)
    }

    log = new Log(logDir, config, recoveryPoint = lastOffset, time.scheduler, time)
    verifyRecoveredLog(log)
    log.close()

    // test recovery case
    log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)
    verifyRecoveredLog(log)
    log.close()
  }

  /**
   * Test building the time index on the follower by setting assignOffsets to false.
   */
  @Test
  def testBuildTimeIndexWhenNotAssigningOffsets() {
    val numMessages = 100
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 10000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val config = LogConfig(logProps)
    val log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)

    val messages = (0 until numMessages).map { i =>
      MemoryRecords.withLogEntries(LogEntry.create(100 + i, Record.create(Record.MAGIC_VALUE_V1, time.milliseconds + i, i.toString.getBytes())))
    }
    messages.foreach(log.append(_, assignOffsets = false))
    val timeIndexEntries = log.logSegments.foldLeft(0) { (entries, segment) => entries + segment.timeIndex.entries }
    assertEquals(s"There should be ${numMessages - 1} time index entries", numMessages - 1, timeIndexEntries)
    assertEquals(s"The last time index entry should have timestamp ${time.milliseconds + numMessages - 1}",
      time.milliseconds + numMessages - 1, log.activeSegment.timeIndex.lastEntry.timestamp)
  }

  /**
   * Test that if we manually delete an index segment it is rebuilt when the log is re-opened
   */
  @Test
  def testIndexRebuild() {
    // publish the messages and close the log
    val numMessages = 200
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 200: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val config = LogConfig(logProps)
    var log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)
    for(i <- 0 until numMessages)
      log.append(TestUtils.singletonRecords(value = TestUtils.randomBytes(10), timestamp = time.milliseconds + i * 10))
    val indexFiles = log.logSegments.map(_.index.file)
    val timeIndexFiles = log.logSegments.map(_.timeIndex.file)
    log.close()

    // delete all the index files
    indexFiles.foreach(_.delete())
    timeIndexFiles.foreach(_.delete())

    // reopen the log
    log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)
    assertEquals("Should have %d messages when log is reopened".format(numMessages), numMessages, log.logEndOffset)
    assertTrue("The index should have been rebuilt", log.logSegments.head.index.entries > 0)
    assertTrue("The time index should have been rebuilt", log.logSegments.head.timeIndex.entries > 0)
    for(i <- 0 until numMessages) {
      assertEquals(i, log.read(i, 100, None).records.shallowEntries.iterator.next().offset)
      if (i == 0)
        assertEquals(log.logSegments.head.baseOffset, log.fetchOffsetsByTimestamp(time.milliseconds + i * 10).get.offset)
      else
        assertEquals(i, log.fetchOffsetsByTimestamp(time.milliseconds + i * 10).get.offset)
    }
    log.close()
  }

  /**
   * Test that if messages format version of the messages in a segment is before 0.10.0, the time index should be empty.
   */
  @Test
  def testRebuildTimeIndexForOldMessages() {
    val numMessages = 200
    val segmentSize = 200
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    logProps.put(LogConfig.MessageFormatVersionProp, "0.9.0")

    val config = LogConfig(logProps)
    var log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)
    for(i <- 0 until numMessages)
      log.append(TestUtils.singletonRecords(value = TestUtils.randomBytes(10), timestamp = time.milliseconds + i * 10))
    val timeIndexFiles = log.logSegments.map(_.timeIndex.file)
    log.close()

    // Delete the time index.
    timeIndexFiles.foreach(_.delete())

    // The rebuilt time index should be empty
    log = new Log(logDir, config, recoveryPoint = numMessages + 1, time.scheduler, time)
    val segArray = log.logSegments.toArray
    for (i <- 0 until segArray.size - 1)
      assertEquals("The time index should be empty", 0, segArray(i).timeIndex.entries)

  }

  /**
   * Test that if we have corrupted an index segment it is rebuilt when the log is re-opened
   */
  @Test
  def testCorruptIndexRebuild() {
    // publish the messages and close the log
    val numMessages = 200
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 200: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val config = LogConfig(logProps)
    var log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)
    for(i <- 0 until numMessages)
      log.append(TestUtils.singletonRecords(value = TestUtils.randomBytes(10), timestamp = time.milliseconds + i * 10))
    val indexFiles = log.logSegments.map(_.index.file)
    val timeIndexFiles = log.logSegments.map(_.timeIndex.file)
    log.close()

    // corrupt all the index files
    for( file <- indexFiles) {
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("  ")
      bw.close()
    }

    // corrupt all the index files
    for( file <- timeIndexFiles) {
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("  ")
      bw.close()
    }

    // reopen the log
    log = new Log(logDir, config, recoveryPoint = 200L, time.scheduler, time)
    assertEquals("Should have %d messages when log is reopened".format(numMessages), numMessages, log.logEndOffset)
    for(i <- 0 until numMessages) {
      assertEquals(i, log.read(i, 100, None).records.shallowEntries.iterator.next().offset)
      if (i == 0)
        assertEquals(log.logSegments.head.baseOffset, log.fetchOffsetsByTimestamp(time.milliseconds + i * 10).get.offset)
      else
        assertEquals(i, log.fetchOffsetsByTimestamp(time.milliseconds + i * 10).get.offset)
    }
    log.close()
  }

  /**
   * Test the Log truncate operations
   */
  @Test
  def testTruncateTo() {
    val set = TestUtils.singletonRecords("test".getBytes)
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * setSize  // each segment will be 10 messages

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)

    // create a log
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, scheduler = time.scheduler, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    for (_ <- 1 to msgPerSeg)
      log.append(set)

    assertEquals("There should be exactly 1 segments.", 1, log.numberOfSegments)
    assertEquals("Log end offset should be equal to number of messages", msgPerSeg, log.logEndOffset)

    val lastOffset = log.logEndOffset
    val size = log.size
    log.truncateTo(log.logEndOffset) // keep the entire log
    assertEquals("Should not change offset", lastOffset, log.logEndOffset)
    assertEquals("Should not change log size", size, log.size)
    log.truncateTo(log.logEndOffset + 1) // try to truncate beyond lastOffset
    assertEquals("Should not change offset but should log error", lastOffset, log.logEndOffset)
    assertEquals("Should not change log size", size, log.size)
    log.truncateTo(msgPerSeg/2) // truncate somewhere in between
    assertEquals("Should change offset", log.logEndOffset, msgPerSeg/2)
    assertTrue("Should change log size", log.size < size)
    log.truncateTo(0) // truncate the entire log
    assertEquals("Should change offset", 0, log.logEndOffset)
    assertEquals("Should change log size", 0, log.size)

    for (_ <- 1 to msgPerSeg)
      log.append(set)

    assertEquals("Should be back to original offset", log.logEndOffset, lastOffset)
    assertEquals("Should be back to original size", log.size, size)
    log.truncateFullyAndStartAt(log.logEndOffset - (msgPerSeg - 1))
    assertEquals("Should change offset", log.logEndOffset, lastOffset - (msgPerSeg - 1))
    assertEquals("Should change log size", log.size, 0)

    for (_ <- 1 to msgPerSeg)
      log.append(set)

    assertTrue("Should be ahead of to original offset", log.logEndOffset > msgPerSeg)
    assertEquals("log size should be same as before", size, log.size)
    log.truncateTo(0) // truncate before first start offset in the log
    assertEquals("Should change offset", 0, log.logEndOffset)
    assertEquals("Should change log size", log.size, 0)
  }

  /**
   * Verify that when we truncate a log the index of the last segment is resized to the max index size to allow more appends
   */
  @Test
  def testIndexResizingAtTruncation() {
    val setSize = TestUtils.singletonRecords(value = "test".getBytes).sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * setSize  // each segment will be 10 messages
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, (setSize - 1): java.lang.Integer)
    val config = LogConfig(logProps)
    val log = new Log(logDir, config, recoveryPoint = 0L, scheduler = time.scheduler, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    for (i<- 1 to msgPerSeg)
      log.append(TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds + i))
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    time.sleep(msgPerSeg)
    for (i<- 1 to msgPerSeg)
      log.append(TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds + i))
    assertEquals("There should be exactly 2 segment.", 2, log.numberOfSegments)
    val expectedEntries = msgPerSeg - 1

    assertEquals(s"The index of the first segment should have $expectedEntries entries", expectedEntries, log.logSegments.toList.head.index.maxEntries)
    assertEquals(s"The time index of the first segment should have $expectedEntries entries", expectedEntries, log.logSegments.toList.head.timeIndex.maxEntries)

    log.truncateTo(0)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
    assertEquals("The index of segment 1 should be resized to maxIndexSize", log.config.maxIndexSize/8, log.logSegments.toList.head.index.maxEntries)
    assertEquals("The time index of segment 1 should be resized to maxIndexSize", log.config.maxIndexSize/12, log.logSegments.toList.head.timeIndex.maxEntries)

    time.sleep(msgPerSeg)
    for (i<- 1 to msgPerSeg)
      log.append(TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds + i))
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
  }

  /**
   * When we open a log any index segments without an associated log segment should be deleted.
   */
  @Test
  def testBogusIndexSegmentsAreRemoved() {
    val bogusIndex1 = Log.indexFilename(logDir, 0)
    val bogusTimeIndex1 = Log.timeIndexFilename(logDir, 0)
    val bogusIndex2 = Log.indexFilename(logDir, 5)
    val bogusTimeIndex2 = Log.timeIndexFilename(logDir, 5)

    val set = TestUtils.singletonRecords("test".getBytes)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, set.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    val log = new Log(logDir,
                      LogConfig(logProps),
                      recoveryPoint = 0L,
                      time.scheduler,
                      time)

    assertTrue("The first index file should have been replaced with a larger file", bogusIndex1.length > 0)
    assertTrue("The first time index file should have been replaced with a larger file", bogusTimeIndex1.length > 0)
    assertFalse("The second index file should have been deleted.", bogusIndex2.exists)
    assertFalse("The second time index file should have been deleted.", bogusTimeIndex2.exists)

    // check that we can append to the log
    for (_ <- 0 until 10)
      log.append(set)

    log.delete()
  }

  /**
   * Verify that truncation works correctly after re-opening the log
   */
  @Test
  def testReopenThenTruncate() {
    val set = TestUtils.singletonRecords("test".getBytes)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, set.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 10000: java.lang.Integer)
    val config = LogConfig(logProps)

    // create a log
    var log = new Log(logDir,
                      config,
                      recoveryPoint = 0L,
                      time.scheduler,
                      time)

    // add enough messages to roll over several segments then close and re-open and attempt to truncate
    for (_ <- 0 until 100)
      log.append(set)
    log.close()
    log = new Log(logDir,
                  config,
                  recoveryPoint = 0L,
                  time.scheduler,
                  time)
    log.truncateTo(3)
    assertEquals("All but one segment should be deleted.", 1, log.numberOfSegments)
    assertEquals("Log end offset should be 3.", 3, log.logEndOffset)
  }

  /**
   * Test that deleted files are deleted after the appropriate time.
   */
  @Test
  def testAsyncDelete() {
    val set = TestUtils.singletonRecords("test".getBytes)
    val asyncDeleteMs = 1000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, set.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 10000: java.lang.Integer)
    logProps.put(LogConfig.FileDeleteDelayMsProp, asyncDeleteMs: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 0: java.lang.Integer)
    val config = LogConfig(logProps)

    val log = new Log(logDir,
                      config,
                      recoveryPoint = 0L,
                      time.scheduler,
                      time)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.append(set)

    // files should be renamed
    val segments = log.logSegments.toArray
    val oldFiles = segments.map(_.log.file) ++ segments.map(_.index.file)
    // expire all segments
    log.logSegments.foreach(_.lastModified = time.milliseconds - 1000L)

    log.deleteOldSegments()

    assertEquals("Only one segment should remain.", 1, log.numberOfSegments)
    assertTrue("All log and index files should end in .deleted", segments.forall(_.log.file.getName.endsWith(Log.DeletedFileSuffix)) &&
                                                                 segments.forall(_.index.file.getName.endsWith(Log.DeletedFileSuffix)))
    assertTrue("The .deleted files should still be there.", segments.forall(_.log.file.exists) &&
                                                            segments.forall(_.index.file.exists))
    assertTrue("The original file should be gone.", oldFiles.forall(!_.exists))

    // when enough time passes the files should be deleted
    val deletedFiles = segments.map(_.log.file) ++ segments.map(_.index.file)
    time.sleep(asyncDeleteMs + 1)
    assertTrue("Files should all be gone.", deletedFiles.forall(!_.exists))
  }

  /**
   * Any files ending in .deleted should be removed when the log is re-opened.
   */
  @Test
  def testOpenDeletesObsoleteFiles() {
    val set = TestUtils.singletonRecords("test".getBytes)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, set.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 0: java.lang.Integer)
    val config = LogConfig(logProps)
    var log = new Log(logDir,
                      config,
                      recoveryPoint = 0L,
                      time.scheduler,
                      time)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.append(set)

    // expire all segments
    log.logSegments.foreach(_.lastModified = time.milliseconds - 1000)
    log.deleteOldSegments()
    log.close()

    log = new Log(logDir,
                  config,
                  recoveryPoint = 0L,
                  time.scheduler,
                  time)
    assertEquals("The deleted segments should be gone.", 1, log.numberOfSegments)
  }

  @Test
  def testAppendMessageWithNullPayload() {
    val log = new Log(logDir,
                      LogConfig(),
                      recoveryPoint = 0L,
                      time.scheduler,
                      time)
    log.append(MemoryRecords.withRecords(Record.create(null)))
    val head = log.read(0, 4096, None).records.shallowEntries().iterator.next()
    assertEquals(0, head.offset)
    assertTrue("Message payload should be null.", head.record.hasNullValue)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAppendWithOutOfOrderOffsetsThrowsException() {
    val log = new Log(logDir,
      LogConfig(),
      recoveryPoint = 0L,
      time.scheduler,
      time)
    val messages = (0 until 2).map(id => Record.create(id.toString.getBytes)).toArray
    messages.foreach(record => log.append(MemoryRecords.withRecords(record)))
    val invalidMessage = MemoryRecords.withRecords(Record.create(1.toString.getBytes))
    log.append(invalidMessage, assignOffsets = false)
  }

  @Test
  def testCorruptLog() {
    // append some messages to create some segments
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    logProps.put(LogConfig.MaxMessageBytesProp, 64*1024: java.lang.Integer)
    val config = LogConfig(logProps)
    val set = TestUtils.singletonRecords("test".getBytes)
    val recoveryPoint = 50L
    for (_ <- 0 until 50) {
      // create a log and write some messages to it
      logDir.mkdirs()
      var log = new Log(logDir,
                        config,
                        recoveryPoint = 0L,
                        time.scheduler,
                        time)
      val numMessages = 50 + TestUtils.random.nextInt(50)
      for (_ <- 0 until numMessages)
        log.append(set)
      val messages = log.logSegments.flatMap(_.log.deepEntries.asScala.toList)
      log.close()

      // corrupt index and log by appending random bytes
      TestUtils.appendNonsenseToFile(log.activeSegment.index.file, TestUtils.random.nextInt(1024) + 1)
      TestUtils.appendNonsenseToFile(log.activeSegment.log.file, TestUtils.random.nextInt(1024) + 1)

      // attempt recovery
      log = new Log(logDir, config, recoveryPoint, time.scheduler, time)
      assertEquals(numMessages, log.logEndOffset)
      assertEquals("Messages in the log after recovery should be the same.", messages,
        log.logSegments.flatMap(_.log.deepEntries.asScala.toList))
      Utils.delete(logDir)
    }
  }

  @Test
  def testOverCompactedLogRecovery(): Unit = {
    // append some messages to create some segments
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.MaxMessageBytesProp, 64*1024: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    val config = LogConfig(logProps)
    val log = new Log(logDir,
      config,
      recoveryPoint = 0L,
      time.scheduler,
      time)
    val set1 = MemoryRecords.withRecords(0, Record.create("v1".getBytes(), "k1".getBytes()))
    val set2 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 2, Record.create("v3".getBytes(), "k3".getBytes()))
    val set3 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 3, Record.create("v4".getBytes(), "k4".getBytes()))
    val set4 = MemoryRecords.withRecords(Integer.MAX_VALUE.toLong + 4, Record.create("v5".getBytes(), "k5".getBytes()))
    //Writes into an empty log with baseOffset 0
    log.append(set1, false)
    assertEquals(0L, log.activeSegment.baseOffset)
    //This write will roll the segment, yielding a new segment with base offset = max(2, 1) = 2
    log.append(set2, false)
    assertEquals(2L, log.activeSegment.baseOffset)
    //This will also roll the segment, yielding a new segment with base offset = max(3, Integer.MAX_VALUE+3) = Integer.MAX_VALUE+3
    log.append(set3, false)
    assertEquals(Integer.MAX_VALUE.toLong + 3, log.activeSegment.baseOffset)
    //This will go into the existing log
    log.append(set4, false)
    assertEquals(Integer.MAX_VALUE.toLong + 3, log.activeSegment.baseOffset)
    log.close()
    val indexFiles = logDir.listFiles.filter(file => file.getName.contains(".index"))
    assertEquals(3, indexFiles.length)
    for (file <- indexFiles) {
      val offsetIndex = new OffsetIndex(file, file.getName.replace(".index","").toLong)
      assertTrue(offsetIndex.lastOffset >= 0)
      offsetIndex.close()
    }
    Utils.delete(logDir)
  }

  @Test
  def testCleanShutdownFile() {
    // append some messages to create some segments
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.MaxMessageBytesProp, 64*1024: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    val config = LogConfig(logProps)
    val set = TestUtils.singletonRecords("test".getBytes)
    val parentLogDir = logDir.getParentFile
    assertTrue("Data directory %s must exist", parentLogDir.isDirectory)
    val cleanShutdownFile = new File(parentLogDir, Log.CleanShutdownFile)
    cleanShutdownFile.createNewFile()
    assertTrue(".kafka_cleanshutdown must exist", cleanShutdownFile.exists())
    var recoveryPoint = 0L
    // create a log and write some messages to it
    var log = new Log(logDir,
      config,
      recoveryPoint = 0L,
      time.scheduler,
      time)
    for (_ <- 0 until 100)
      log.append(set)
    log.close()

    // check if recovery was attempted. Even if the recovery point is 0L, recovery should not be attempted as the
    // clean shutdown file exists.
    recoveryPoint = log.logEndOffset
    log = new Log(logDir, config, 0L, time.scheduler, time)
    assertEquals(recoveryPoint, log.logEndOffset)
    cleanShutdownFile.delete()
  }

  @Test
  def testParseTopicPartitionName() {
    val topic = "test_topic"
    val partition = "143"
    val dir = new File(logDir + topicPartitionName(topic, partition))
    val topicPartition = Log.parseTopicPartitionName(dir)
    assertEquals(topic, topicPartition.topic)
    assertEquals(partition.toInt, topicPartition.partition)
  }

  @Test
  def testParseTopicPartitionNameForEmptyName() {
    try {
      val dir = new File("")
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case _: Exception => // its GOOD!
    }
  }

  @Test
  def testParseTopicPartitionNameForNull() {
    try {
      val dir: File = null
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir)
    } catch {
      case _: Exception => // its GOOD!
    }
  }

  @Test
  def testParseTopicPartitionNameForMissingSeparator() {
    val topic = "test_topic"
    val partition = "1999"
    val dir = new File(logDir + File.separator + topic + partition)
    try {
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case _: Exception => // its GOOD!
    }
  }

  @Test
  def testParseTopicPartitionNameForMissingTopic() {
    val topic = ""
    val partition = "1999"
    val dir = new File(logDir + topicPartitionName(topic, partition))
    try {
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case _: Exception => // its GOOD!
    }
  }

  @Test
  def testParseTopicPartitionNameForMissingPartition() {
    val topic = "test_topic"
    val partition = ""
    val dir = new File(logDir + topicPartitionName(topic, partition))
    try {
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case _: Exception => // its GOOD!
    }
  }

  def topicPartitionName(topic: String, partition: String): String =
    File.separator + topic + "-" + partition

  @Test
  def testDeleteOldSegmentsMethod() {
    val set = TestUtils.singletonRecords("test".getBytes)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, set.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 0: java.lang.Integer)
    val config = LogConfig(logProps)
    val log = new Log(logDir,
      config,
      recoveryPoint = 0L,
      time.scheduler,
      time)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.append(set)

    // expire all segments
    log.logSegments.foreach(_.lastModified = time.milliseconds - 1000)
    log.deleteOldSegments()
    assertEquals("The deleted segments should be gone.", 1, log.numberOfSegments)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.append(set)

    log.delete()
    assertEquals("The number of segments should be 0", 0, log.numberOfSegments)
    assertEquals("The number of deleted segments should be zero.", 0, log.deleteOldSegments())
  }


  @Test
  def shouldDeleteSizeBasedSegments() {
    val set = TestUtils.singletonRecords("test".getBytes)
    val log = createLog(set.sizeInBytes, retentionBytes = set.sizeInBytes * 10)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.append(set)

    log.deleteOldSegments
    assertEquals("should have 2 segments", 2,log.numberOfSegments)
  }

  @Test
  def shouldNotDeleteSizeBasedSegmentsWhenUnderRetentionSize() {
    val set = TestUtils.singletonRecords("test".getBytes)
    val log = createLog(set.sizeInBytes, retentionBytes = set.sizeInBytes * 15)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.append(set)

    log.deleteOldSegments
    assertEquals("should have 3 segments", 3,log.numberOfSegments)
  }

  @Test
  def shouldDeleteTimeBasedSegmentsReadyToBeDeleted() {
    val set = TestUtils.singletonRecords("test".getBytes, timestamp = 10)
    val log = createLog(set.sizeInBytes, retentionMs = 10000)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.append(set)

    log.deleteOldSegments()
    assertEquals("There should be 1 segment remaining", 1, log.numberOfSegments)
  }

  @Test
  def shouldNotDeleteTimeBasedSegmentsWhenNoneReadyToBeDeleted() {
    val set = TestUtils.singletonRecords("test".getBytes, timestamp = time.milliseconds)
    val log = createLog(set.sizeInBytes, retentionMs = 10000000)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.append(set)

    log.deleteOldSegments()
    assertEquals("There should be 3 segments remaining", 3, log.numberOfSegments)
  }

  @Test
  def shouldNotDeleteSegmentsWhenPolicyDoesNotIncludeDelete() {
    val set = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes(), timestamp = 10L)
    val log = createLog(set.sizeInBytes,
      retentionMs = 10000,
      cleanupPolicy = "compact")

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.append(set)

    // mark oldest segment as older the retention.ms
    log.logSegments.head.lastModified = time.milliseconds - 20000

    val segments = log.numberOfSegments
    log.deleteOldSegments()
    assertEquals("There should be 3 segments remaining", segments, log.numberOfSegments)
  }

  @Test
  def shouldDeleteSegmentsReadyToBeDeletedWhenCleanupPolicyIsCompactAndDelete() {
    val set = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes,timestamp = 10L)
    val log = createLog(set.sizeInBytes,
      retentionMs = 10000,
      cleanupPolicy = "compact,delete")

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.append(set)

    log.deleteOldSegments()
    assertEquals("There should be 1 segment remaining", 1, log.numberOfSegments)
  }

  def createLog(messageSizeInBytes: Int, retentionMs: Int = -1,
                retentionBytes: Int = -1, cleanupPolicy: String = "delete"): Log = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, messageSizeInBytes * 5: Integer)
    logProps.put(LogConfig.RetentionMsProp, retentionMs: Integer)
    logProps.put(LogConfig.RetentionBytesProp, retentionBytes: Integer)
    logProps.put(LogConfig.CleanupPolicyProp, cleanupPolicy)
    val config = LogConfig(logProps)
    val log = new Log(logDir,
      config,
      recoveryPoint = 0L,
      time.scheduler,
      time)
    log
  }
}
