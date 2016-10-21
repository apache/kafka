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

import java.io.{DataOutputStream, File}
import java.nio._
import java.nio.file.Paths
import java.util.Properties

import kafka.common._
import kafka.message._
import kafka.utils._
import org.apache.kafka.common.record.{MemoryRecords, TimestampType}
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{After, Test}
import org.scalatest.junit.JUnitSuite

import scala.collection._

/**
 * Unit tests for the log cleaning logic
 */
class CleanerTest extends JUnitSuite {
  
  val tmpdir = TestUtils.tempDir()
  val dir = TestUtils.randomPartitionLogDir(tmpdir)
  val logProps = new Properties()
  logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.SegmentIndexBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
  val logConfig = LogConfig(logProps)
  val time = new MockTime()
  val throttler = new Throttler(desiredRatePerSec = Double.MaxValue, checkIntervalMs = Long.MaxValue, time = time)
  
  @After
  def teardown(): Unit = {
    Utils.delete(tmpdir)
  }
  
  /**
   * Test simple log cleaning
   */
  @Test
  def testCleanSegments(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // append messages to the log until we have four segments
    while(log.numberOfSegments < 4)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))
    val keysFound = keysInLog(log)
    assertEquals(0L until log.logEndOffset, keysFound)

    // pretend we have the following keys
    val keys = immutable.ListSet(1, 3, 5, 7, 9)
    val map = new FakeOffsetMap(Int.MaxValue)
    keys.foreach(k => map.put(key(k), Long.MaxValue))

    // clean the log
    cleaner.cleanSegments(log, log.logSegments.take(3).toSeq, map, 0L)
    val shouldRemain = keysInLog(log).filter(!keys.contains(_))
    assertEquals(shouldRemain, keysInLog(log))
  }

  /**
   * Test log cleaning with logs containing messages larger than default message size
   */
  @Test
  def testLargeMessage() {
    val largeMessageSize = 1024 * 1024
    // Create cleaner with very small default max message size
    val cleaner = makeCleaner(Int.MaxValue, maxMessageSize=1024)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, largeMessageSize * 16: java.lang.Integer)
    logProps.put(LogConfig.MaxMessageBytesProp, largeMessageSize * 2: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    while(log.numberOfSegments < 2)
      log.append(message(log.logEndOffset.toInt, Array.fill(largeMessageSize)(0: Byte)))
    val keysFound = keysInLog(log)
    assertEquals(0L until log.logEndOffset, keysFound)

    // pretend we have the following keys
    val keys = immutable.ListSet(1, 3, 5, 7, 9)
    val map = new FakeOffsetMap(Int.MaxValue)
    keys.foreach(k => map.put(key(k), Long.MaxValue))

    // clean the log
    cleaner.cleanSegments(log, Seq(log.logSegments.head), map, 0L)
    val shouldRemain = keysInLog(log).filter(!keys.contains(_))
    assertEquals(shouldRemain, keysInLog(log))
  }

  @Test
  def testCleaningWithDeletes(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))
    
    // append messages with the keys 0 through N
    while(log.numberOfSegments < 2)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))
      
    // delete all even keys between 0 and N
    val leo = log.logEndOffset
    for(key <- 0 until leo.toInt by 2)
      log.append(deleteMessage(key))
      
    // append some new unique keys to pad out to a new active segment
    while(log.numberOfSegments < 4)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))
      
    cleaner.clean(LogToClean(TopicAndPartition("test", 0), log, 0, log.activeSegment.baseOffset))
    val keys = keysInLog(log).toSet
    assertTrue("None of the keys we deleted should still exist.", 
               (0 until leo.toInt by 2).forall(!keys.contains(_)))
  }

  @Test
  def testPartialSegmentClean(): Unit = {
    // because loadFactor is 0.75, this means we can fit 2 messages in the map
    val cleaner = makeCleaner(2)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    log.append(message(0,0)) // offset 0
    log.append(message(1,1)) // offset 1
    log.append(message(0,0)) // offset 2
    log.append(message(1,1)) // offset 3
    log.append(message(0,0)) // offset 4
    // roll the segment, so we can clean the messages already appended
    log.roll()

    // clean the log with only one message removed
    cleaner.clean(LogToClean(TopicAndPartition("test", 0), log, 2, log.activeSegment.baseOffset))
    assertEquals(immutable.List(1,0,1,0), keysInLog(log))
    assertEquals(immutable.List(1,2,3,4), offsetsInLog(log))

    // continue to make progress, even though we can only clean one message at a time
    cleaner.clean(LogToClean(TopicAndPartition("test", 0), log, 3, log.activeSegment.baseOffset))
    assertEquals(immutable.List(0,1,0), keysInLog(log))
    assertEquals(immutable.List(2,3,4), offsetsInLog(log))

    cleaner.clean(LogToClean(TopicAndPartition("test", 0), log, 4, log.activeSegment.baseOffset))
    assertEquals(immutable.List(1,0), keysInLog(log))
    assertEquals(immutable.List(3,4), offsetsInLog(log))
  }

  @Test
  def testCleaningWithUncleanableSection(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // Number of distinct keys. For an effective test this should be small enough such that each log segment contains some duplicates.
    val N = 10
    val numCleanableSegments = 2
    val numTotalSegments = 7

    // append messages with the keys 0 through N-1, values equal offset
    while(log.numberOfSegments <= numCleanableSegments)
      log.append(message(log.logEndOffset.toInt % N, log.logEndOffset.toInt))

    // at this point one message past the cleanable segments has been added
    // the entire segment containing the first uncleanable offset should not be cleaned.
    val firstUncleanableOffset = log.logEndOffset + 1  // +1  so it is past the baseOffset

    while(log.numberOfSegments < numTotalSegments - 1)
      log.append(message(log.logEndOffset.toInt % N, log.logEndOffset.toInt))

    // the last (active) segment has just one message

    def distinctValuesBySegment = log.logSegments.map(s => s.log.map(m => TestUtils.readString(m.message.payload)).toSet.size).toSeq

    val disctinctValuesBySegmentBeforeClean = distinctValuesBySegment
    assertTrue("Test is not effective unless each segment contains duplicates. Increase segment size or decrease number of keys.",
      distinctValuesBySegment.reverse.tail.forall(_ > N))

    cleaner.clean(LogToClean(TopicAndPartition("test", 0), log, 0, firstUncleanableOffset))

    val distinctValuesBySegmentAfterClean = distinctValuesBySegment

    assertTrue("The cleanable segments should have fewer number of values after cleaning",
      disctinctValuesBySegmentBeforeClean.zip(distinctValuesBySegmentAfterClean).take(numCleanableSegments).forall { case (before, after) => after < before })
    assertTrue("The uncleanable segments should have the same number of values after cleaning", disctinctValuesBySegmentBeforeClean.zip(distinctValuesBySegmentAfterClean)
      .slice(numCleanableSegments, numTotalSegments).forall { x => x._1 == x._2 })
  }

  @Test
  def testLogToClean(): Unit = {
    // create a log with small segment size
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // create 6 segments with only one message in each segment
    val messageSet = TestUtils.singleMessageSet(payload = Array.fill[Byte](50)(0), key = 1.toString.getBytes)
    for (i <- 0 until 6)
      log.append(messageSet, assignOffsets = true)

    val logToClean = LogToClean(TopicAndPartition("test", 0), log, log.activeSegment.baseOffset, log.activeSegment.baseOffset)

    assertEquals("Total bytes of LogToClean should equal size of all segments excluding the active segment",
      logToClean.totalBytes, log.size - log.activeSegment.size)
  }

  @Test
  def testLogToCleanWithUncleanableSection(): Unit = {
    // create a log with small segment size
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // create 6 segments with only one message in each segment
    val messageSet = TestUtils.singleMessageSet(payload = Array.fill[Byte](50)(0), key = 1.toString.getBytes)
    for (i <- 0 until 6)
      log.append(messageSet, assignOffsets = true)

    // segments [0,1] are clean; segments [2, 3] are cleanable; segments [4,5] are uncleanable
    val segs = log.logSegments.toSeq
    val logToClean = LogToClean(TopicAndPartition("test", 0), log, segs(2).baseOffset, segs(4).baseOffset)

    val expectedCleanSize = segs.take(2).map(_.size).sum
    val expectedCleanableSize = segs.slice(2, 4).map(_.size).sum
    val expectedUncleanableSize = segs.drop(4).map(_.size).sum

    assertEquals("Uncleanable bytes of LogToClean should equal size of all segments prior the one containing first dirty",
      logToClean.cleanBytes, expectedCleanSize)
    assertEquals("Cleanable bytes of LogToClean should equal size of all segments from the one containing first dirty offset" +
      " to the segment prior to the one with the first uncleanable offset",
      logToClean.cleanableBytes, expectedCleanableSize)
    assertEquals("Total bytes should be the sum of the clean and cleanable segments", logToClean.totalBytes, expectedCleanSize + expectedCleanableSize)
    assertEquals("Total cleanable ratio should be the ratio of cleanable size to clean plus cleanable", logToClean.cleanableRatio,
      expectedCleanableSize / (expectedCleanSize + expectedCleanableSize).toDouble, 1.0e-6d)
  }

  @Test
  def testCleaningWithUnkeyedMessages(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)

    // create a log with compaction turned off so we can append unkeyed messages
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Delete)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // append unkeyed messages
    while(log.numberOfSegments < 2)
      log.append(unkeyedMessage(log.logEndOffset.toInt))
    val numInvalidMessages = unkeyedMessageCountInLog(log)

    val sizeWithUnkeyedMessages = log.size

    // append keyed messages
    while(log.numberOfSegments < 3)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))

    val expectedSizeAfterCleaning = log.size - sizeWithUnkeyedMessages
    cleaner.clean(LogToClean(TopicAndPartition("test", 0), log, 0, log.activeSegment.baseOffset))

    assertEquals("Log should only contain keyed messages after cleaning.", 0, unkeyedMessageCountInLog(log))
    assertEquals("Log should only contain keyed messages after cleaning.", expectedSizeAfterCleaning, log.size)
    assertEquals("Cleaner should have seen %d invalid messages.", numInvalidMessages, cleaner.stats.invalidMessagesRead)
  }
  
  /* extract all the keys from a log */
  def keysInLog(log: Log): Iterable[Int] =
    log.logSegments.flatMap(s => s.log.filter(!_.message.isNull).filter(_.message.hasKey).map(m => TestUtils.readString(m.message.key).toInt))

  /* extract all the offsets from a log */
  def offsetsInLog(log: Log): Iterable[Long] =
    log.logSegments.flatMap(s => s.log.filter(!_.message.isNull).filter(_.message.hasKey).map(m => m.offset))

  def unkeyedMessageCountInLog(log: Log) =
    log.logSegments.map(s => s.log.filter(!_.message.isNull).count(m => !m.message.hasKey)).sum

  def abortCheckDone(topicAndPartition: TopicAndPartition): Unit = {
    throw new LogCleaningAbortedException()
  }

  /**
   * Test that abortion during cleaning throws a LogCleaningAbortedException
   */
  @Test
  def testCleanSegmentsWithAbort(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue, abortCheckDone)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    // append messages to the log until we have four segments
    while(log.numberOfSegments < 4)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))

    val keys = keysInLog(log)
    val map = new FakeOffsetMap(Int.MaxValue)
    keys.foreach(k => map.put(key(k), Long.MaxValue))
    intercept[LogCleaningAbortedException] {
      cleaner.cleanSegments(log, log.logSegments.take(3).toSeq, map, 0L)
    }
  }

  /**
   * Validate the logic for grouping log segments together for cleaning
   */
  @Test
  def testSegmentGrouping(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 300: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))
    
    // append some messages to the log
    var i = 0
    while(log.numberOfSegments < 10) {
      log.append(TestUtils.singleMessageSet(payload = "hello".getBytes, key = "hello".getBytes))
      i += 1
    }
    
    // grouping by very large values should result in a single group with all the segments in it
    var groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = Int.MaxValue)
    assertEquals(1, groups.size)
    assertEquals(log.numberOfSegments, groups.head.size)
    checkSegmentOrder(groups)
    
    // grouping by very small values should result in all groups having one entry
    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = 1, maxIndexSize = Int.MaxValue)
    assertEquals(log.numberOfSegments, groups.size)
    assertTrue("All groups should be singletons.", groups.forall(_.size == 1))
    checkSegmentOrder(groups)
    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = 1)
    assertEquals(log.numberOfSegments, groups.size)
    assertTrue("All groups should be singletons.", groups.forall(_.size == 1))
    checkSegmentOrder(groups)

    val groupSize = 3
    
    // check grouping by log size
    val logSize = log.logSegments.take(groupSize).map(_.size).sum.toInt + 1
    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = logSize, maxIndexSize = Int.MaxValue)
    checkSegmentOrder(groups)
    assertTrue("All but the last group should be the target size.", groups.dropRight(1).forall(_.size == groupSize))
    
    // check grouping by index size
    val indexSize = log.logSegments.take(groupSize).map(_.index.sizeInBytes).sum + 1
    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = indexSize)
    checkSegmentOrder(groups)
    assertTrue("All but the last group should be the target size.", groups.dropRight(1).forall(_.size == groupSize))
  }
  
  /**
   * Validate the logic for grouping log segments together for cleaning when only a small number of
   * messages are retained, but the range of offsets is greater than Int.MaxValue. A group should not
   * contain a range of offsets greater than Int.MaxValue to ensure that relative offsets can be
   * stored in 4 bytes.
   */
  @Test
  def testSegmentGroupingWithSparseOffsets(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 300: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))
       
    // fill up first segment
    while (log.numberOfSegments == 1)
      log.append(TestUtils.singleMessageSet(payload = "hello".getBytes, key = "hello".getBytes))
    
    // forward offset and append message to next segment at offset Int.MaxValue
    val messageSet = new ByteBufferMessageSet(NoCompressionCodec, new LongRef(Int.MaxValue - 1),
      new Message("hello".getBytes, "hello".getBytes, Message.NoTimestamp, Message.MagicValue_V1))
    log.append(messageSet, assignOffsets = false)
    log.append(TestUtils.singleMessageSet(payload = "hello".getBytes, key = "hello".getBytes))
    assertEquals(Int.MaxValue, log.activeSegment.index.lastOffset)
    
    // grouping should result in a single group with maximum relative offset of Int.MaxValue
    var groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = Int.MaxValue)
    assertEquals(1, groups.size)
    
    // append another message, making last offset of second segment > Int.MaxValue
    log.append(TestUtils.singleMessageSet(payload = "hello".getBytes, key = "hello".getBytes))
    
    // grouping should not group the two segments to ensure that maximum relative offset in each group <= Int.MaxValue
    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = Int.MaxValue)
    assertEquals(2, groups.size)
    checkSegmentOrder(groups)
    
    // append more messages, creating new segments, further grouping should still occur
    while (log.numberOfSegments < 4)
      log.append(TestUtils.singleMessageSet(payload = "hello".getBytes, key = "hello".getBytes))

    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = Int.MaxValue)
    assertEquals(log.numberOfSegments - 1, groups.size)
    for (group <- groups)
      assertTrue("Relative offset greater than Int.MaxValue", group.last.index.lastOffset - group.head.index.baseOffset <= Int.MaxValue)
    checkSegmentOrder(groups)
    
  }
  
  private def checkSegmentOrder(groups: Seq[Seq[LogSegment]]): Unit = {
    val offsets = groups.flatMap(_.map(_.baseOffset))
    assertEquals("Offsets should be in increasing order.", offsets.sorted, offsets)
  }
  
  /**
   * Test building an offset map off the log
   */
  @Test
  def testBuildOffsetMap(): Unit = {
    val map = new FakeOffsetMap(1000)
    val log = makeLog()
    val cleaner = makeCleaner(Int.MaxValue)
    val start = 0
    val end = 500
    val offsets = writeToLog(log, (start until end) zip (start until end))
    def checkRange(map: FakeOffsetMap, start: Int, end: Int) {
      cleaner.buildOffsetMap(log, start, end, map)
      val endOffset = map.latestOffset + 1
      assertEquals("Last offset should be the end offset.", end, endOffset)
      assertEquals("Should have the expected number of messages in the map.", end-start, map.size)
      for(i <- start until end)
        assertEquals("Should find all the keys", i.toLong, map.get(key(i)))
      assertEquals("Should not find a value too small", -1L, map.get(key(start - 1)))
      assertEquals("Should not find a value too large", -1L, map.get(key(end)))
    }
    val segments = log.logSegments.toSeq
    checkRange(map, 0, segments(1).baseOffset.toInt)
    checkRange(map, segments(1).baseOffset.toInt, segments(3).baseOffset.toInt)
    checkRange(map, segments(3).baseOffset.toInt, log.logEndOffset.toInt)
  }
  
  
  /**
   * Tests recovery if broker crashes at the following stages during the cleaning sequence
   * <ol>
   *   <li> Cleaner has created .cleaned log containing multiple segments, swap sequence not yet started
   *   <li> .cleaned log renamed to .swap, old segment files not yet renamed to .deleted
   *   <li> .cleaned log renamed to .swap, old segment files renamed to .deleted, but not yet deleted
   *   <li> .swap suffix removed, completing the swap, but async delete of .deleted files not yet complete
   * </ol>
   */
  @Test
  def testRecoveryAfterCrash(): Unit = {
    val cleaner = makeCleaner(Int.MaxValue)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 300: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    logProps.put(LogConfig.FileDeleteDelayMsProp, 10: java.lang.Integer)

    val config = LogConfig.fromProps(logConfig.originals, logProps)
      
    def recoverAndCheck(config: LogConfig, expectedKeys : Iterable[Int]) : Log = {   
      // Recover log file and check that after recovery, keys are as expected
      // and all temporary files have been deleted
      val recoveredLog = makeLog(config = config)
      time.sleep(config.fileDeleteDelayMs + 1)
      for (file <- dir.listFiles) {
        assertFalse("Unexpected .deleted file after recovery", file.getName.endsWith(Log.DeletedFileSuffix))
        assertFalse("Unexpected .cleaned file after recovery", file.getName.endsWith(Log.CleanedFileSuffix))
        assertFalse("Unexpected .swap file after recovery", file.getName.endsWith(Log.SwapFileSuffix))
      }
      assertEquals(expectedKeys, keysInLog(recoveredLog))
      recoveredLog
    }
    
    // create a log and append some messages
    var log = makeLog(config = config)
    var messageCount = 0
    while(log.numberOfSegments < 10) {
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))
      messageCount += 1
    }
    val allKeys = keysInLog(log)
    
    // pretend we have odd-numbered keys
    val offsetMap = new FakeOffsetMap(Int.MaxValue)
    for (k <- 1 until messageCount by 2)
      offsetMap.put(key(k), Long.MaxValue)
     
    // clean the log
    cleaner.cleanSegments(log, log.logSegments.take(9).toSeq, offsetMap, 0L)
    var cleanedKeys = keysInLog(log)
    
    // 1) Simulate recovery just after .cleaned file is created, before rename to .swap
    //    On recovery, clean operation is aborted. All messages should be present in the log
    log.logSegments.head.changeFileSuffixes("", Log.CleanedFileSuffix)
    for (file <- dir.listFiles if file.getName.endsWith(Log.DeletedFileSuffix)) {
      Utils.atomicMoveWithFallback(file.toPath, Paths.get(CoreUtils.replaceSuffix(file.getPath, Log.DeletedFileSuffix, "")))
    }
    log = recoverAndCheck(config, allKeys)
    
    // clean again
    cleaner.cleanSegments(log, log.logSegments.take(9).toSeq, offsetMap, 0L)
    cleanedKeys = keysInLog(log)
    
    // 2) Simulate recovery just after swap file is created, before old segment files are
    //    renamed to .deleted. Clean operation is resumed during recovery. 
    log.logSegments.head.changeFileSuffixes("", Log.SwapFileSuffix)
    for (file <- dir.listFiles if file.getName.endsWith(Log.DeletedFileSuffix)) {
      Utils.atomicMoveWithFallback(file.toPath, Paths.get(CoreUtils.replaceSuffix(file.getPath, Log.DeletedFileSuffix, "")))
    }   
    log = recoverAndCheck(config, cleanedKeys)

    // add some more messages and clean the log again
    while(log.numberOfSegments < 10) {
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))
      messageCount += 1
    }
    for (k <- 1 until messageCount by 2)
      offsetMap.put(key(k), Long.MaxValue)    
    cleaner.cleanSegments(log, log.logSegments.take(9).toSeq, offsetMap, 0L)
    cleanedKeys = keysInLog(log)
    
    // 3) Simulate recovery after swap file is created and old segments files are renamed
    //    to .deleted. Clean operation is resumed during recovery.
    log.logSegments.head.changeFileSuffixes("", Log.SwapFileSuffix)
    log = recoverAndCheck(config, cleanedKeys)
    
    // add some more messages and clean the log again
    while(log.numberOfSegments < 10) {
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))
      messageCount += 1
    }
    for (k <- 1 until messageCount by 2)
      offsetMap.put(key(k), Long.MaxValue)    
    cleaner.cleanSegments(log, log.logSegments.take(9).toSeq, offsetMap, 0L)
    cleanedKeys = keysInLog(log)
    
    // 4) Simulate recovery after swap is complete, but async deletion
    //    is not yet complete. Clean operation is resumed during recovery.
    recoverAndCheck(config, cleanedKeys)
    
  }

  @Test
  def testBuildOffsetMapFakeLarge(): Unit = {
    val map = new FakeOffsetMap(1000)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 72: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 72: java.lang.Integer)
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    val logConfig = LogConfig(logProps)
    val log = makeLog(config = logConfig)
    val cleaner = makeCleaner(Int.MaxValue)
    val start = 0
    val end = 2
    val offsetSeq = Seq(0L, 7206178L)
    val offsets = writeToLog(log, (start until end) zip (start until end), offsetSeq)
    cleaner.buildOffsetMap(log, start, end, map)
    val endOffset = map.latestOffset
    assertEquals("Last offset should be the end offset.", 7206178L, endOffset)
    assertEquals("Should have the expected number of messages in the map.", end - start, map.size)
    assertEquals("Map should contain first value", 0L, map.get(key(0)))
    assertEquals("Map should contain second value", 7206178L, map.get(key(1)))
  }

  /**
   * Test building a partial offset map of part of a log segment
   */
  @Test
  def testBuildPartialOffsetMap(): Unit = {
    // because loadFactor is 0.75, this means we can fit 2 messages in the map
    val map = new FakeOffsetMap(3)
    val log = makeLog()
    val cleaner = makeCleaner(2)

    log.append(message(0,0))
    log.append(message(1,1))
    log.append(message(2,2))
    log.append(message(3,3))
    log.append(message(4,4))
    log.roll()

    cleaner.buildOffsetMap(log, 2, Int.MaxValue, map)
    assertEquals(2, map.size)
    assertEquals(-1, map.get(key(0)))
    assertEquals(2, map.get(key(2)))
    assertEquals(3, map.get(key(3)))
    assertEquals(-1, map.get(key(4)))
  }

  /**
   * This test verifies that messages corrupted by KAFKA-4298 are fixed by the cleaner
   */
  @Test
  def testCleanCorruptMessageSet() {
    val codec = SnappyCompressionCodec

    val logProps = new Properties()
    logProps.put(LogConfig.CompressionTypeProp, codec.name)
    val logConfig = LogConfig(logProps)

    val log = makeLog(config = logConfig)
    val cleaner = makeCleaner(10)

    // messages are constructed so that the payload matches the expecting offset to
    // make offset validation easier after cleaning

    // one compressed log entry with duplicates
    val dupSetKeys = (0 until 2) ++ (0 until 2)
    val dupSetOffset = 25
    val dupSet = dupSetKeys zip (dupSetOffset until dupSetOffset + dupSetKeys.size)

    // and one without (should still be fixed by the cleaner)
    val noDupSetKeys = 3 until 5
    val noDupSetOffset = 50
    val noDupSet = noDupSetKeys zip (noDupSetOffset until noDupSetOffset + noDupSetKeys.size)

    log.append(invalidCleanedMessage(dupSetOffset, dupSet, codec), assignOffsets = false)
    log.append(invalidCleanedMessage(noDupSetOffset, noDupSet, codec), assignOffsets = false)

    log.roll()

    cleaner.clean(LogToClean(TopicAndPartition("test", 0), log, 0, log.activeSegment.baseOffset))

    for (segment <- log.logSegments; shallowMessage <- segment.log.iterator; deepMessage <- ByteBufferMessageSet.deepIterator(shallowMessage)) {
      assertEquals(shallowMessage.message.magic, deepMessage.message.magic)
      val value = TestUtils.readString(deepMessage.message.payload).toLong
      assertEquals(deepMessage.offset, value)
    }
  }

  /**
   * Verify that the client can handle corrupted messages. Located here for now since the client
   * does not support writing messages with the old magic.
   */
  @Test
  def testClientHandlingOfCorruptMessageSet(): Unit = {
    import JavaConverters._

    val keys = 1 until 10
    val offset = 50
    val set = keys zip (offset until offset + keys.size)

    val corruptedMessage = invalidCleanedMessage(offset, set)
    val records = MemoryRecords.readableRecords(corruptedMessage.buffer)

    for (logEntry <- records.iterator.asScala) {
      val offset = logEntry.offset
      val value = TestUtils.readString(logEntry.record.value).toLong
      assertEquals(offset, value)
    }
  }

  private def writeToLog(log: Log, keysAndValues: Iterable[(Int, Int)], offsetSeq: Iterable[Long]): Iterable[Long] = {
    for(((key, value), offset) <- keysAndValues.zip(offsetSeq))
      yield log.append(messageWithOffset(key, value, offset), assignOffsets = false).firstOffset
  }

  private def invalidCleanedMessage(initialOffset: Long,
                                    keysAndValues: Iterable[(Int, Int)],
                                    codec: CompressionCodec = SnappyCompressionCodec): ByteBufferMessageSet = {
    // this function replicates the old versions of the cleaner which under some circumstances
    // would write invalid compressed message sets with the outer magic set to 1 and the inner
    // magic set to 0

    val messages = keysAndValues.map(kv =>
      new Message(key = kv._1.toString.getBytes,
        bytes = kv._2.toString.getBytes,
        timestamp = Message.NoTimestamp,
        magicValue = Message.MagicValue_V0))

    val messageWriter = new MessageWriter(math.min(math.max(MessageSet.messageSetSize(messages) / 2, 1024), 1 << 16))
    var lastOffset = initialOffset

    messageWriter.write(
      codec = codec,
      timestamp = Message.NoTimestamp,
      timestampType = TimestampType.CREATE_TIME,
      magicValue = Message.MagicValue_V1) { outputStream =>

      val output = new DataOutputStream(CompressionFactory(codec, Message.MagicValue_V1, outputStream))
      try {
        for (message <- messages) {
          val innerOffset = lastOffset - initialOffset
          output.writeLong(innerOffset)
          output.writeInt(message.size)
          output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
          lastOffset += 1
        }
      } finally {
        output.close()
      }
    }
    val buffer = ByteBuffer.allocate(messageWriter.size + MessageSet.LogOverhead)
    ByteBufferMessageSet.writeMessage(buffer, messageWriter, lastOffset - 1)
    buffer.rewind()

    new ByteBufferMessageSet(buffer)
  }

  private def messageWithOffset(key: Int, value: Int, offset: Long) =
    new ByteBufferMessageSet(NoCompressionCodec, Seq(offset),
                             new Message(key = key.toString.getBytes,
                                         bytes = value.toString.getBytes,
                                         timestamp = Message.NoTimestamp,
                                         magicValue = Message.MagicValue_V1))
  
  
  def makeLog(dir: File = dir, config: LogConfig = logConfig) =
    new Log(dir = dir, config = config, recoveryPoint = 0L, scheduler = time.scheduler, time = time)

  def noOpCheckDone(topicAndPartition: TopicAndPartition) { /* do nothing */  }

  def makeCleaner(capacity: Int, checkDone: (TopicAndPartition) => Unit = noOpCheckDone, maxMessageSize: Int = 64*1024) =
    new Cleaner(id = 0, 
                offsetMap = new FakeOffsetMap(capacity), 
                ioBufferSize = maxMessageSize,
                maxIoBufferSize = maxMessageSize,
                dupBufferLoadFactor = 0.75,
                throttler = throttler, 
                time = time,
                checkDone = checkDone )
  
  def writeToLog(log: Log, seq: Iterable[(Int, Int)]): Iterable[Long] = {
    for((key, value) <- seq)
      yield log.append(message(key, value)).firstOffset
  }
  
  def key(id: Int) = ByteBuffer.wrap(id.toString.getBytes)
  
  def message(key: Int, value: Int): ByteBufferMessageSet =
    message(key, value.toString.getBytes)

  def message(key: Int, value: Array[Byte]) =
    new ByteBufferMessageSet(new Message(key = key.toString.getBytes,
                                         bytes = value,
                                         timestamp = Message.NoTimestamp,
                                         magicValue = Message.MagicValue_V1))

  def unkeyedMessage(value: Int) =
    new ByteBufferMessageSet(new Message(bytes = value.toString.getBytes))

  def deleteMessage(key: Int) =
    new ByteBufferMessageSet(new Message(key = key.toString.getBytes,
                                         bytes = null,
                                         timestamp = Message.NoTimestamp,
                                         magicValue = Message.MagicValue_V1))
  
}

class FakeOffsetMap(val slots: Int) extends OffsetMap {
  val map = new java.util.HashMap[String, Long]()
  var lastOffset = -1L

  private def keyFor(key: ByteBuffer) =
    new String(Utils.readBytes(key.duplicate), "UTF-8")

  def put(key: ByteBuffer, offset: Long): Unit = {
    lastOffset = offset
    map.put(keyFor(key), offset)
  }
  
  def get(key: ByteBuffer): Long = {
    val k = keyFor(key)
    if(map.containsKey(k))
      map.get(k)
    else
      -1L
  }
  
  def clear(): Unit = map.clear()
  
  def size: Int = map.size

  def latestOffset: Long = lastOffset

  override def toString: String = map.toString()
}
