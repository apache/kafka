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

import kafka.common._
import kafka.message._
import kafka.utils._
import org.junit.Assert._
import org.junit.{After, Test}
import org.scalatest.junit.JUnitSuite

import scala.collection.Iterable


/**
  * Unit tests for the log cleaning logic
  */
class LogCleanerManagerTest extends JUnitSuite with Logging {

  val tmpdir = TestUtils.tempDir()
  val dir = TestUtils.randomPartitionLogDir(tmpdir)
  val logProps = new Properties()
  logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.SegmentIndexBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
  val logConfig = LogConfig(logProps)
  val time = new MockTime(1400000000000L)  // Tue May 13 16:53:20 UTC 2014
  val throttler = new Throttler(desiredRatePerSec = Double.MaxValue, checkIntervalMs = Long.MaxValue, time = time)

  @After
  def teardown() {
    CoreUtils.rm(tmpdir)
  }

  /**
    * Test computation of cleanable range with no minimum compaction lag settings active
    */
  @Test
  def testCleanableOffsetsForNone(): Unit = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    while(log.numberOfSegments < 8)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))

    val topicAndPartition = TopicAndPartition("log", 0)
    val lastClean = Map(topicAndPartition-> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicAndPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable offset starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The first uncleanable offset begins with the second block of log entries.", log.activeSegment.baseOffset, cleanableOffsets._2)
  }

  /**
    * Test computation of cleanable range with a minimum compaction lag time
    */
  @Test
  def testCleanableOffsetsForTime(): Unit = {
    val compactionLag = 60 * 60 * 1000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.MinCompactionLagMsProp, compactionLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val t0 = time.milliseconds
    while(log.numberOfSegments < 4)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))

    // force the modification time of the log segments
    log.logSegments.foreach(_.log.file.setLastModified(t0))
    val activeSegAtT0 = log.activeSegment

    time.sleep(compactionLag + 1)
    val t1 = time.milliseconds

    while(log.numberOfSegments < 8)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))

    // force the modification time of the log segments
    log.logSegments.foreach { s =>
      if (s.baseOffset >= activeSegAtT0.baseOffset)
        s.log.file.setLastModified(t1)
    }

    val topicAndPartition = TopicAndPartition("log", 0)
    val lastClean = Map(topicAndPartition-> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicAndPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable offset starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The first uncleanable offset begins with the second block of log entries.", activeSegAtT0.baseOffset, cleanableOffsets._2)
  }

  /**
    * Test computation of cleanable range with a minimum compaction lag time that is small enough that
    * the active segment contains it.
    */
  @Test
  def testCleanableOffsetsForShortTime(): Unit = {
    val compactionLag = 60 * 60 * 1000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.MinCompactionLagMsProp, compactionLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val t0 = time.milliseconds
    while(log.numberOfSegments < 8)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))

    // force the modification time of the log segments
    log.logSegments.foreach(_.log.file.setLastModified(t0))

    time.sleep(compactionLag + 1)
    val t1 = time.milliseconds

    val topicAndPartition = TopicAndPartition("log", 0)
    val lastClean = Map(topicAndPartition-> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicAndPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable offset starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The first uncleanable offset begins with active segment.", log.activeSegment.baseOffset, cleanableOffsets._2)
  }

  /**
    * Test computation of cleanable range with a minimum compaction lag size (bytes)
    */
  @Test
  def testCleanableOffsetsForSize(): Unit = {
    val byteLag = 2000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.MinCompactionLagBytesProp, byteLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    while(log.numberOfSegments < 8)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))

    debug(s"segment baseOffset->size: ${log.logSegments.map(s => s"${s.baseOffset}->${s.size}").mkString(", ")}")

    val sizeLagSegment = computeSizeLagSegment(log, byteLag)

    val logSize = log.logSegments.map(_.size).sum - log.activeSegment.size
    assertTrue("Log must be large enough to be an effective test.", logSize - log.activeSegment.size > byteLag)
    assertTrue("Active segment must be smaller than byte lag to be an effective test.", byteLag > log.activeSegment.size)

    val topicAndPartition = TopicAndPartition("log", 0)
    val lastClean = Map(topicAndPartition-> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicAndPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable region starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The uncleanable region begins at the base offset with ", sizeLagSegment.baseOffset, cleanableOffsets._2)
  }

  /*
   * Compute the first segment in the tail that contains the specified size
   */
  def computeSizeLagSegment(log: Log, byteLag: Long): LogSegment = {
    var lagSum = 0L
    var lagSegment: LogSegment = null
    log.logSegments.toSeq.reverse.toStream.takeWhile(_ => lagSum < byteLag).foreach {
      s =>
        lagSum += s.size
        lagSegment = s
    }
    lagSegment
  }

  /**
    * Test computation of cleanable range with a minimum compaction lag message count
    */
  @Test
  def testCleanableOffsetsForMessages(): Unit = {
    val messageLag = 50
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.MinCompactionLagMessagesProp, messageLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    while(log.numberOfSegments < 8)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))

    debug(s"segment baseOffset->nextOffset: ${log.logSegments.map(s => s"${s.baseOffset}->${s.nextOffset}").mkString(", ")}")

    val messages = readFromLog(log).toSeq
    val offsetOfMessageAtLag = messages.takeRight(messageLag).head._1 // since the message key equals the (zero-based) ordinal message number
    val firstUncleanableSegment = log.logSegments.find { s => s.nextOffset() > offsetOfMessageAtLag }.get

    assertTrue("Log must be large enough to be an effective test.", log.activeSegment.nextOffset - 1 > messageLag)

    val topicAndPartition = TopicAndPartition("log", 0)
    val lastClean = Map(topicAndPartition-> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicAndPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable region starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The uncleanable region begins at the base offset with ", firstUncleanableSegment.baseOffset, cleanableOffsets._2)
  }

  /**
    * Test computation of cleanable range with both a minimum compaction lag message count and minimum compaction
    * lag size (bytes)
    */
  @Test
  def testCleanableOffsetsForTwo(): Unit = {
    val messageLag = 70
    val byteLag = 2000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.MinCompactionLagMessagesProp, messageLag: java.lang.Integer)
    logProps.put(LogConfig.MinCompactionLagBytesProp, byteLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    while(log.numberOfSegments < 8)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))

    debug(s"segment baseOffset->nextOffset (size): ${log.logSegments.map(s => s"${s.baseOffset}->${s.nextOffset} (${s.size})").mkString(", ")}")

    val sizeLagSegment = computeSizeLagSegment(log, byteLag)

    val messages = readFromLog(log).toSeq
    val offsetOfMessageAtLag = messages.takeRight(messageLag).head._1 // since the message key equals the (zero-based) ordinal message number
    val messageLagSegment = log.logSegments.find { s => s.nextOffset() > offsetOfMessageAtLag }.get

    assertNotEquals("The message and size compaction lags should yield different offsets for an effective test.", sizeLagSegment.baseOffset, messageLagSegment.baseOffset)

    val smallerOffset = math.min(sizeLagSegment.baseOffset, messageLagSegment.baseOffset)

    val logSize = log.logSegments.map(_.size).sum - log.activeSegment.size
    assertTrue("Log must be large enough to be an effective test.", logSize - log.activeSegment.size > byteLag)
    assertTrue("Log must be large enough to be an effective test.", log.activeSegment.nextOffset - 1 > messageLag)
    assertTrue("Active segment should be smaller than the lag segments", log.activeSegment.baseOffset > sizeLagSegment.baseOffset)
    assertTrue("Active segment should have fewer messages than the lag segments", log.activeSegment.baseOffset > messageLagSegment.baseOffset)

    val topicAndPartition = TopicAndPartition("log", 0)
    val lastClean = Map(topicAndPartition-> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicAndPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable region starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The uncleanable region begins at the base offset with ", smallerOffset, cleanableOffsets._2)
  }

  def makeLog(dir: File = dir, config: LogConfig = logConfig) =
    new Log(dir = dir, config = config, recoveryPoint = 0L, scheduler = time.scheduler, time = time)

  def readFromLog(log: Log): Iterable[(Int, Int)] = {
    for (segment <- log.logSegments; entry <- segment.log; messageAndOffset <- {
      // create single message iterator or deep iterator depending on compression codec
      if (entry.message.compressionCodec == NoCompressionCodec)
        Stream.cons(entry, Stream.empty).iterator
      else
        ByteBufferMessageSet.deepIterator(entry)
    }) yield {
      val key = TestUtils.readString(messageAndOffset.message.key).toInt
      val value = TestUtils.readString(messageAndOffset.message.payload).toInt
      key -> value
    }
  }

  def writeToLog(log: Log, seq: Iterable[(Int, Int)]): Iterable[Long] = {
    for((key, value) <- seq)
      yield log.append(message(key, value)).firstOffset
  }

  def message(key: Int, value: Int) =
    new ByteBufferMessageSet(new Message(key = key.toString.getBytes,
      bytes = value.toString.getBytes,
      timestamp = Message.NoTimestamp,
      magicValue = Message.MagicValue_V1))

}
