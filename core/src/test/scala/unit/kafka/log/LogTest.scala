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
import java.util.ArrayList
import java.util.concurrent.atomic._
import junit.framework.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Before, Test}
import kafka.message._
import kafka.common.{MessageSizeTooLargeException, OffsetOutOfRangeException}
import kafka.utils._
import scala.Some
import kafka.server.KafkaConfig

class LogTest extends JUnitSuite {
  
  var logDir: File = null
  val time = new MockTime
  var config: KafkaConfig = null

  @Before
  def setUp() {
    logDir = TestUtils.tempDir()
    val props = TestUtils.createBrokerConfig(0, -1)
    config = new KafkaConfig(props)
  }

  @After
  def tearDown() {
    Utils.rm(logDir)
  }
  
  def createEmptyLogs(dir: File, offsets: Int*) {
    for(offset <- offsets) {
      Log.logFilename(dir, offset).createNewFile()
      Log.indexFilename(dir, offset).createNewFile()
    }
  }

  /** Test that the size and time based log segment rollout works. */
  @Test
  def testTimeBasedLogRoll() {
    val set = TestUtils.singleMessageSet("test".getBytes())
    val rollMs = 1 * 60 * 60L
    val time: MockTime = new MockTime()

    // create a log
    val log = new Log(logDir, 1000, config.messageMaxBytes, 1000, rollMs, needsRecovery = false, time = time)
    time.currentMs += rollMs + 1

    // segment age is less than its limit
    log.append(set)
    assertEquals("There should be exactly one segment.", 1, log.numberOfSegments)

    log.append(set)
    assertEquals("There should be exactly one segment.", 1, log.numberOfSegments)

    // segment expires in age
    time.currentMs += rollMs + 1
    log.append(set)
    assertEquals("There should be exactly 2 segments.", 2, log.numberOfSegments)

    time.currentMs += rollMs + 1
    val blank = Array[Message]()
    log.append(new ByteBufferMessageSet(new Message("blah".getBytes)))
    assertEquals("There should be exactly 3 segments.", 3, log.numberOfSegments)

    time.currentMs += rollMs + 1
    // the last segment expired in age, but was blank. So new segment should not be generated
    log.append(new ByteBufferMessageSet())
    assertEquals("There should be exactly 3 segments.", 3, log.numberOfSegments)
  }

  @Test
  def testSizeBasedLogRoll() {
    val set = TestUtils.singleMessageSet("test".getBytes())
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val logFileSize = msgPerSeg * (setSize - 1) // each segment will be 10 messages

    // create a log
    val log = new Log(logDir, logFileSize, config.messageMaxBytes, 1000, 10000, needsRecovery = false, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    // segments expire in size
    for (i<- 1 to (msgPerSeg + 1)) {
      log.append(set)
    }
    assertEquals("There should be exactly 2 segments.", 2, log.numberOfSegments)
  }

  @Test
  def testLoadEmptyLog() {
    createEmptyLogs(logDir, 0)
    new Log(logDir, 1024, config.messageMaxBytes, 1000, config.logRollHours*60*60*1000L, needsRecovery = false, time = time)
  }

  @Test
  def testAppendAndRead() {
    val log = new Log(logDir, 1024, config.messageMaxBytes, 1000, config.logRollHours*60*60*1000L, needsRecovery = false, time = time)
    val message = new Message(Integer.toString(42).getBytes())
    for(i <- 0 until 10)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, message))
    log.flush()
    val messages = log.read(0, 1024)
    var current = 0
    for(curr <- messages) {
      assertEquals("Read message should equal written", message, curr.message)
      current += 1
    }
    assertEquals(10, current)
  }

  @Test
  def testReadOutOfRange() {
    createEmptyLogs(logDir, 1024)
    val log = new Log(logDir, 1024, config.messageMaxBytes, 1000, config.logRollHours*60*60*1000L, needsRecovery = false, time = time)
    assertEquals("Reading just beyond end of log should produce 0 byte read.", 0, log.read(1024, 1000).sizeInBytes)
    try {
      log.read(0, 1024)
      fail("Expected exception on invalid read.")
    } catch {
      case e: OffsetOutOfRangeException => "This is good."
    }
    try {
      log.read(1025, 1000)
      fail("Expected exception on invalid read.")
    } catch {
      case e: OffsetOutOfRangeException => "This is good."
    }
  }

  /** Test that writing and reading beyond the log size boundary works */
  @Test
  def testLogRolls() {
    /* create a multipart log with 100 messages */
    val log = new Log(logDir, 100, config.messageMaxBytes, 1000, config.logRollHours*60*60*1000L, needsRecovery = false, time = time)
    val numMessages = 100
    val messageSets = (0 until numMessages).map(i => TestUtils.singleMessageSet(i.toString.getBytes))
    val offsets = messageSets.map(log.append(_)._1)
    log.flush

    /* do successive reads to ensure all our messages are there */
    var offset = 0L
    for(i <- 0 until numMessages) {
      val messages = log.read(offset, 1024*1024)
      assertEquals("Offsets not equal", offset, messages.head.offset)
      assertEquals("Messages not equal at offset " + offset, messageSets(i).head.message, messages.head.message)
      offset = messages.head.offset + 1
    }
    val lastRead = log.read(startOffset = numMessages, maxLength = 1024*1024, maxOffset = Some(numMessages + 1))
    assertEquals("Should be no more messages", 0, lastRead.size)
  }
  
  /** Test the case where we have compressed batches of messages */
  @Test
  def testCompressedMessages() {
    /* this log should roll after every messageset */
    val log = new Log(logDir, 10, config.messageMaxBytes, 1000, config.logRollHours*60*60*1000L, needsRecovery = false, time = time)
    
    /* append 2 compressed message sets, each with two messages giving offsets 0, 1, 2, 3 */
    log.append(new ByteBufferMessageSet(DefaultCompressionCodec, new Message("hello".getBytes), new Message("there".getBytes)))
    log.append(new ByteBufferMessageSet(DefaultCompressionCodec, new Message("alpha".getBytes), new Message("beta".getBytes)))
    
    def read(offset: Int) = ByteBufferMessageSet.decompress(log.read(offset, 4096).head.message)
    
    /* we should always get the first message in the compressed set when reading any offset in the set */
    assertEquals("Read at offset 0 should produce 0", 0, read(0).head.offset)
    assertEquals("Read at offset 1 should produce 0", 0, read(1).head.offset)
    assertEquals("Read at offset 2 should produce 2", 2, read(2).head.offset)
    assertEquals("Read at offset 3 should produce 2", 2, read(3).head.offset)
  }

  @Test
  def testFindSegment() {
    assertEquals("Search in empty segments list should find nothing", None, Log.findRange(makeRanges(), 45))
    assertEquals("Search in segment list just outside the range of the last segment should find last segment",
                 9, Log.findRange(makeRanges(5, 9, 12), 12).get.start)
    assertEquals("Search in segment list far outside the range of the last segment should find last segment",
                 9, Log.findRange(makeRanges(5, 9, 12), 100).get.start)
    assertEquals("Search in segment list far outside the range of the last segment should find last segment",
                 None, Log.findRange(makeRanges(5, 9, 12), -1))
    assertContains(makeRanges(5, 9, 12), 11)
    assertContains(makeRanges(5), 4)
    assertContains(makeRanges(5,8), 5)
    assertContains(makeRanges(5,8), 6)
  }
  
  @Test
  def testEdgeLogRollsStartingAtZero() {
    // first test a log segment starting at 0
    val log = new Log(logDir, 100, config.messageMaxBytes, 1000, config.logRollHours*60*60*1000L, needsRecovery = false, time = time)
    val curOffset = log.logEndOffset
    assertEquals(curOffset, 0)

    // time goes by; the log file is deleted
    log.markDeletedWhile(_ => true)

    // we now have a new log; the starting offset of the new log should remain 0
    assertEquals(curOffset, log.logEndOffset)
    log.delete()
  }

  @Test
  def testEdgeLogRollsStartingAtNonZero() {
    // second test an empty log segment starting at non-zero
    val log = new Log(logDir, 100, config.messageMaxBytes, 1000, config.logRollHours*60*60*1000L, needsRecovery = false, time = time)
    val numMessages = 1
    for(i <- 0 until numMessages)
      log.append(TestUtils.singleMessageSet(i.toString.getBytes))
    val curOffset = log.logEndOffset
    
    // time goes by; the log file is deleted
    log.markDeletedWhile(_ => true)

    // we now have a new log
    assertEquals(curOffset, log.logEndOffset)

    // time goes by; the log file (which is empty) is deleted again
    val deletedSegments = log.markDeletedWhile(_ => true)

    // we shouldn't delete the last empty log segment.
    assertTrue("We shouldn't delete the last empty log segment", deletedSegments.size == 0)

    // we now have a new log
    assertEquals(curOffset, log.logEndOffset)
  }

  @Test
  def testMessageSizeCheck() {
    val first = new ByteBufferMessageSet(NoCompressionCodec, new Message ("You".getBytes()), new Message("bethe".getBytes()))
    val second = new ByteBufferMessageSet(NoCompressionCodec, new Message("change".getBytes()))

    // append messages to log
    val maxMessageSize = second.sizeInBytes - 1
    val log = new Log(logDir, 100, maxMessageSize, 1000, config.logRollHours*60*60*1000L, needsRecovery = false, time = time)

    // should be able to append the small message
    log.append(first)

    try {
      log.append(second)
      fail("Second message set should throw MessageSizeTooLargeException.")
    } catch {
        case e:MessageSizeTooLargeException => // this is good
    }
  }
  
  @Test
  def testLogRecoversToCorrectOffset() {
    val numMessages = 100
    val messageSize = 100
    val segmentSize = 7 * messageSize
    val indexInterval = 3 * messageSize
    var log = new Log(logDir, segmentSize, config.messageMaxBytes, 1000, config.logRollHours*60*60*1000L, needsRecovery = false, indexIntervalBytes = indexInterval, maxIndexSize = 4096)
    for(i <- 0 until numMessages)
      log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(messageSize)))
    assertEquals("After appending %d messages to an empty log, the log end offset should be %d".format(numMessages, numMessages), numMessages, log.logEndOffset)
    val lastIndexOffset = log.segments.view.last.index.lastOffset
    val numIndexEntries = log.segments.view.last.index.entries
    log.close()
    
    // test non-recovery case
    log = new Log(logDir, segmentSize, config.messageMaxBytes, 1000, config.logRollHours*60*60*1000L, needsRecovery = false, indexIntervalBytes = indexInterval, maxIndexSize = 4096)
    assertEquals("Should have %d messages when log is reopened w/o recovery".format(numMessages), numMessages, log.logEndOffset)
    assertEquals("Should have same last index offset as before.", lastIndexOffset, log.segments.view.last.index.lastOffset)
    assertEquals("Should have same number of index entries as before.", numIndexEntries, log.segments.view.last.index.entries)
    log.close()
    
    // test 
    log = new Log(logDir, segmentSize, config.messageMaxBytes, 1000, config.logRollHours*60*60*1000L, needsRecovery = true, indexIntervalBytes = indexInterval, maxIndexSize = 4096)
    assertEquals("Should have %d messages when log is reopened with recovery".format(numMessages), numMessages, log.logEndOffset)
    assertEquals("Should have same last index offset as before.", lastIndexOffset, log.segments.view.last.index.lastOffset)
    assertEquals("Should have same number of index entries as before.", numIndexEntries, log.segments.view.last.index.entries)
    log.close()
  }

  @Test
  def testTruncateTo() {
    val set = TestUtils.singleMessageSet("test".getBytes())
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val logFileSize = msgPerSeg * (setSize - 1) // each segment will be 10 messages

    // create a log
    val log = new Log(logDir, logFileSize, config.messageMaxBytes, 1000, 10000, needsRecovery = false, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    for (i<- 1 to msgPerSeg)
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

    for (i<- 1 to msgPerSeg)
      log.append(set)
    
    assertEquals("Should be back to original offset", log.logEndOffset, lastOffset)
    assertEquals("Should be back to original size", log.size, size)
    log.truncateAndStartWithNewOffset(log.logEndOffset - (msgPerSeg - 1))
    assertEquals("Should change offset", log.logEndOffset, lastOffset - (msgPerSeg - 1))
    assertEquals("Should change log size", log.size, 0)

    for (i<- 1 to msgPerSeg)
      log.append(set)

    assertTrue("Should be ahead of to original offset", log.logEndOffset > msgPerSeg)
    assertEquals("log size should be same as before", size, log.size)
    log.truncateTo(0) // truncate before first start offset in the log
    assertEquals("Should change offset", 0, log.logEndOffset)
    assertEquals("Should change log size", log.size, 0)
  }

  @Test
  def testIndexResizingAtTruncation() {
    val set = TestUtils.singleMessageSet("test".getBytes())
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val logFileSize = msgPerSeg * (setSize - 1) // each segment will be 10 messages
    val log = new Log(logDir, logFileSize, config.messageMaxBytes, 1000, 10000, needsRecovery = false, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
    for (i<- 1 to msgPerSeg)
      log.append(set)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
    for (i<- 1 to msgPerSeg)
      log.append(set)
    assertEquals("There should be exactly 2 segment.", 2, log.numberOfSegments)
    assertEquals("The index of the first segment should be trim to empty", 0, log.segments.view(0).index.maxEntries)
    log.truncateTo(0)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
    assertEquals("The index of segment 1 should be resized to maxIndexSize", log.maxIndexSize/8, log.segments.view(0).index.maxEntries)
    for (i<- 1 to msgPerSeg)
      log.append(set)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
  }


  @Test
  def testAppendWithoutOffsetAssignment() {
    for(codec <- List(NoCompressionCodec, DefaultCompressionCodec)) {
      logDir.mkdir()
      var log = new Log(logDir, 
                        maxLogFileSize = 64*1024, 
                        maxMessageSize = config.messageMaxBytes,
                        maxIndexSize = 1000, 
                        indexIntervalBytes = 10000, 
                        needsRecovery = true)
      val messages = List("one", "two", "three", "four", "five", "six")
      val ms = new ByteBufferMessageSet(compressionCodec = codec, 
                                        offsetCounter = new AtomicLong(5), 
                                        messages = messages.map(s => new Message(s.getBytes)):_*)
      val firstOffset = ms.shallowIterator.toList.head.offset
      val lastOffset = ms.shallowIterator.toList.last.offset
      val (first, last) = log.append(ms, assignOffsets = false)
      assertEquals(last + 1, log.logEndOffset)
      assertEquals(firstOffset, first)
      assertEquals(lastOffset, last)
      assertTrue(log.read(5, 64*1024).size > 0)
      log.delete()
    }
  }
  
  /**
   * When we open a log any index segments without an associated log segment should be deleted.
   */
  @Test
  def testBogusIndexSegmentsAreRemoved() {
    val bogusIndex1 = Log.indexFilename(logDir, 0)
    val bogusIndex2 = Log.indexFilename(logDir, 5)
    
    val set = TestUtils.singleMessageSet("test".getBytes())
    val log = new Log(logDir, 
                      maxLogFileSize = set.sizeInBytes * 5, 
                      maxMessageSize = config.messageMaxBytes,
                      maxIndexSize = 1000, 
                      indexIntervalBytes = 1, 
                      needsRecovery = false)
    
    assertTrue("The first index file should have been replaced with a larger file", bogusIndex1.length > 0)
    assertFalse("The second index file should have been deleted.", bogusIndex2.exists)
    
    // check that we can append to the log
    for(i <- 0 until 10)
      log.append(set)
      
    log.delete()
  }

  @Test
  def testReopenThenTruncate() {
    val set = TestUtils.singleMessageSet("test".getBytes())

    // create a log
    var log = new Log(logDir, 
                      maxLogFileSize = set.sizeInBytes * 5, 
                      maxMessageSize = config.messageMaxBytes,
                      maxIndexSize = 1000, 
                      indexIntervalBytes = 10000, 
                      needsRecovery = true)
    
    // add enough messages to roll over several segments then close and re-open and attempt to truncate
    for(i <- 0 until 100)
      log.append(set)
    log.close()
    log = new Log(logDir, 
                  maxLogFileSize = set.sizeInBytes * 5, 
                  maxMessageSize = config.messageMaxBytes,
                  maxIndexSize = 1000, 
                  indexIntervalBytes = 10000, 
                  needsRecovery = true)
    log.truncateTo(3)
    assertEquals("All but one segment should be deleted.", 1, log.numberOfSegments)
    assertEquals("Log end offset should be 3.", 3, log.logEndOffset)
  }
  
  def assertContains(ranges: Array[Range], offset: Long) = {
    Log.findRange(ranges, offset) match {
      case Some(range) => 
        assertTrue(range + " does not contain " + offset, range.contains(offset))
      case None => fail("No range found, but expected to find " + offset)
    }
  }
  
  class SimpleRange(val start: Long, val size: Long) extends Range
  
  def makeRanges(breaks: Int*): Array[Range] = {
    val list = new ArrayList[Range]
    var prior = 0
    for(brk <- breaks) {
      list.add(new SimpleRange(prior, brk - prior))
      prior = brk
    }
    list.toArray(new Array[Range](list.size))
  }
  
}
