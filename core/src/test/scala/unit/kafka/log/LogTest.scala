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
  val logConfig = LogConfig()

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

  /**
   * Tests for time based log roll. This test appends messages then changes the time
   * using the mock clock to force the log to roll and checks the number of segments.
   */
  @Test
  def testTimeBasedLogRoll() {
    val set = TestUtils.singleMessageSet("test".getBytes())
    val time: MockTime = new MockTime()

    // create a log
    val log = new Log(logDir, 
                      logConfig.copy(segmentMs = 1 * 60 * 60L), 
                      needsRecovery = false, 
                      scheduler = time.scheduler, 
                      time = time)
    time.sleep(log.config.segmentMs + 1)

    // segment age is less than its limit
    log.append(set)
    assertEquals("There should be exactly one segment.", 1, log.numberOfSegments)

    log.append(set)
    assertEquals("There should still be exactly one segment.", 1, log.numberOfSegments)

    for(numSegments <- 2 until 4) {
      time.sleep(log.config.segmentMs + 1)
      log.append(set)
      assertEquals("Changing time beyond rollMs and appending should create a new segment.", numSegments, log.numberOfSegments)
    }

    val numSegments = log.numberOfSegments
    time.sleep(log.config.segmentMs + 1)
    log.append(new ByteBufferMessageSet())
    assertEquals("Appending an empty message set should not roll log even if succient time has passed.", numSegments, log.numberOfSegments)
  }

  /**
   * Test that appending more than the maximum segment size rolls the log
   */
  @Test
  def testSizeBasedLogRoll() {
    val set = TestUtils.singleMessageSet("test".getBytes)
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * (setSize - 1) // each segment will be 10 messages

    // create a log
    val log = new Log(logDir, logConfig.copy(segmentSize = segmentSize), needsRecovery = false, time.scheduler, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    // segments expire in size
    for (i<- 1 to (msgPerSeg + 1)) {
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
    val log = new Log(logDir, logConfig, needsRecovery = false, time.scheduler, time = time)
    log.append(TestUtils.singleMessageSet("test".getBytes))
  }

  /**
   * This test case appends a bunch of messages and checks that we can read them all back using sequential offsets.
   */
  @Test
  def testAppendAndReadWithSequentialOffsets() {
    val log = new Log(logDir, logConfig.copy(segmentSize = 71), needsRecovery = false, time.scheduler, time = time)
    val messages = (0 until 100 by 2).map(id => new Message(id.toString.getBytes)).toArray
    
    for(i <- 0 until messages.length)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, messages = messages(i)))
    for(i <- 0 until messages.length) {
      val read = log.read(i, 100, Some(i+1)).head
      assertEquals("Offset read should match order appended.", i, read.offset)
      assertEquals("Message should match appended.", messages(i), read.message)
    }
    assertEquals("Reading beyond the last message returns nothing.", 0, log.read(messages.length, 100, None).size)
  }
  
  /**
   * This test appends a bunch of messages with non-sequential offsets and checks that we can read the correct message
   * from any offset less than the logEndOffset including offsets not appended.
   */
  @Test
  def testAppendAndReadWithNonSequentialOffsets() {
    val log = new Log(logDir, logConfig.copy(segmentSize = 71), needsRecovery = false, time.scheduler, time = time)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val messages = messageIds.map(id => new Message(id.toString.getBytes))
    
    // now test the case that we give the offsets and use non-sequential offsets
    for(i <- 0 until messages.length)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, new AtomicLong(messageIds(i)), messages = messages(i)), assignOffsets = false)
    for(i <- 50 until messageIds.max) {
      val idx = messageIds.indexWhere(_ >= i)
      val read = log.read(i, 100, None).head
      assertEquals("Offset read should match message id.", messageIds(idx), read.offset)
      assertEquals("Message should match appended.", messages(idx), read.message)
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
    val log = new Log(logDir, logConfig.copy(segmentSize = 300), needsRecovery = false, time.scheduler, time = time)
    
    // keep appending until we have two segments with only a single message in the second segment
    while(log.numberOfSegments == 1)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, messages = new Message("42".getBytes))) 
    
    // now manually truncate off all but one message from the first segment to create a gap in the messages
    log.logSegments.head.truncateTo(1)
    
    assertEquals("A read should now return the last message in the log", log.logEndOffset-1, log.read(1, 200, None).head.offset)
  }
  
  /**
   * Test reading at the boundary of the log, specifically
   * - reading from the logEndOffset should give an empty message set
   * - reading beyond the log end offset should throw an OffsetOutOfRangeException
   */
  @Test
  def testReadOutOfRange() {
    createEmptyLogs(logDir, 1024)
    val log = new Log(logDir, logConfig.copy(segmentSize = 1024), needsRecovery = false, time.scheduler, time = time)
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

  /**
   * Test that covers reads and writes on a multisegment log. This test appends a bunch of messages
   * and then reads them all back and checks that the message read and offset matches what was appended.
   */
  @Test
  def testLogRolls() {
    /* create a multipart log with 100 messages */
    val log = new Log(logDir, logConfig.copy(segmentSize = 100), needsRecovery = false, time.scheduler, time = time)
    val numMessages = 100
    val messageSets = (0 until numMessages).map(i => TestUtils.singleMessageSet(i.toString.getBytes))
    messageSets.foreach(log.append(_))
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
  
  /**
   * Test reads at offsets that fall within compressed message set boundaries.
   */
  @Test
  def testCompressedMessages() {
    /* this log should roll after every messageset */
    val log = new Log(logDir, logConfig.copy(segmentSize = 10), needsRecovery = false, time.scheduler, time = time)
    
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
  
  /**
   * Test garbage collecting old segments
   */
  @Test
  def testThatGarbageCollectingSegmentsDoesntChangeOffset() {
    for(messagesToAppend <- List(0, 1, 25)) {
      logDir.mkdirs()
      // first test a log segment starting at 0
      val log = new Log(logDir, logConfig.copy(segmentSize = 100), needsRecovery = false, time.scheduler, time = time)
      for(i <- 0 until messagesToAppend)
        log.append(TestUtils.singleMessageSet(i.toString.getBytes))
      
      var currOffset = log.logEndOffset
      assertEquals(currOffset, messagesToAppend)

      // time goes by; the log file is deleted
      log.deleteOldSegments(_ => true)

      assertEquals("Deleting segments shouldn't have changed the logEndOffset", currOffset, log.logEndOffset)
      assertEquals("We should still have one segment left", 1, log.numberOfSegments)
      assertEquals("Further collection shouldn't delete anything", 0, log.deleteOldSegments(_ => true))
      assertEquals("Still no change in the logEndOffset", currOffset, log.logEndOffset)
      assertEquals("Should still be able to append and should get the logEndOffset assigned to the new append", 
                   currOffset,
                   log.append(TestUtils.singleMessageSet("hello".toString.getBytes)).firstOffset)
      
      // cleanup the log
      log.delete()
    }
  }

  /**
   * We have a max size limit on message appends, check that it is properly enforced by appending a message larger than the 
   * setting and checking that an exception is thrown.
   */
  @Test
  def testMessageSizeCheck() {
    val first = new ByteBufferMessageSet(NoCompressionCodec, new Message ("You".getBytes), new Message("bethe".getBytes))
    val second = new ByteBufferMessageSet(NoCompressionCodec, new Message("change".getBytes))

    // append messages to log
    val maxMessageSize = second.sizeInBytes - 1
    val log = new Log(logDir, logConfig.copy(maxMessageSize = maxMessageSize), needsRecovery = false, time.scheduler, time = time)

    // should be able to append the small message
    log.append(first)

    try {
      log.append(second)
      fail("Second message set should throw MessageSizeTooLargeException.")
    } catch {
        case e: MessageSizeTooLargeException => // this is good
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
    val config = logConfig.copy(segmentSize = segmentSize, indexInterval = indexInterval, maxIndexSize = 4096)
    var log = new Log(logDir, config, needsRecovery = false, time.scheduler, time)
    for(i <- 0 until numMessages)
      log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(messageSize)))
    assertEquals("After appending %d messages to an empty log, the log end offset should be %d".format(numMessages, numMessages), numMessages, log.logEndOffset)
    val lastIndexOffset = log.activeSegment.index.lastOffset
    val numIndexEntries = log.activeSegment.index.entries
    log.close()
    
    log = new Log(logDir, config, needsRecovery = false, time.scheduler, time)
    assertEquals("Should have %d messages when log is reopened w/o recovery".format(numMessages), numMessages, log.logEndOffset)
    assertEquals("Should have same last index offset as before.", lastIndexOffset, log.activeSegment.index.lastOffset)
    assertEquals("Should have same number of index entries as before.", numIndexEntries, log.activeSegment.index.entries)
    log.close()
    
    // test recovery case
    log = new Log(logDir, config, needsRecovery = true, time.scheduler, time)
    assertEquals("Should have %d messages when log is reopened with recovery".format(numMessages), numMessages, log.logEndOffset)
    assertEquals("Should have same last index offset as before.", lastIndexOffset, log.activeSegment.index.lastOffset)
    assertEquals("Should have same number of index entries as before.", numIndexEntries, log.activeSegment.index.entries)
    log.close()
  }
  
  /**
   * Test that if we manually delete an index segment it is rebuilt when the log is re-opened
   */
  @Test
  def testIndexRebuild() {
    // publish the messages and close the log
    val numMessages = 200
    val config = logConfig.copy(segmentSize = 200, indexInterval = 1)
    var log = new Log(logDir, config, needsRecovery = true, time.scheduler, time)
    for(i <- 0 until numMessages)
      log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(10)))
    val indexFiles = log.logSegments.map(_.index.file)
    log.close()
    
    // delete all the index files
    indexFiles.foreach(_.delete())
    
    // reopen the log
    log = new Log(logDir, config, needsRecovery = true, time.scheduler, time)    
    assertEquals("Should have %d messages when log is reopened".format(numMessages), numMessages, log.logEndOffset)
    for(i <- 0 until numMessages)
      assertEquals(i, log.read(i, 100, None).head.offset)
    log.close()
  }

  /**
   * Test the Log truncate operations
   */
  @Test
  def testTruncateTo() {
    val set = TestUtils.singleMessageSet("test".getBytes())
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * (setSize - 1) // each segment will be 10 messages

    // create a log
    val log = new Log(logDir, logConfig.copy(segmentSize = segmentSize), needsRecovery = false, scheduler = time.scheduler, time = time)
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
    log.truncateFullyAndStartAt(log.logEndOffset - (msgPerSeg - 1))
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

  /**
   * Verify that when we truncate a log the index of the last segment is resized to the max index size to allow more appends
   */
  @Test
  def testIndexResizingAtTruncation() {
    val set = TestUtils.singleMessageSet("test".getBytes())
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * (setSize - 1) // each segment will be 10 messages
    val config = logConfig.copy(segmentSize = segmentSize)
    val log = new Log(logDir, config, needsRecovery = false, scheduler = time.scheduler, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
    for (i<- 1 to msgPerSeg)
      log.append(set)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
    for (i<- 1 to msgPerSeg)
      log.append(set)
    assertEquals("There should be exactly 2 segment.", 2, log.numberOfSegments)
    assertEquals("The index of the first segment should be trimmed to empty", 0, log.logSegments.toList(0).index.maxEntries)
    log.truncateTo(0)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
    assertEquals("The index of segment 1 should be resized to maxIndexSize", log.config.maxIndexSize/8, log.logSegments.toList(0).index.maxEntries)
    for (i<- 1 to msgPerSeg)
      log.append(set)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
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
                      logConfig.copy(segmentSize = set.sizeInBytes * 5, 
                                     maxIndexSize = 1000, 
                                     indexInterval = 1),
                      needsRecovery = false,
                      time.scheduler,
                      time)
    
    assertTrue("The first index file should have been replaced with a larger file", bogusIndex1.length > 0)
    assertFalse("The second index file should have been deleted.", bogusIndex2.exists)
    
    // check that we can append to the log
    for(i <- 0 until 10)
      log.append(set)
      
    log.delete()
  }

  /**
   * Verify that truncation works correctly after re-opening the log
   */
  @Test
  def testReopenThenTruncate() {
    val set = TestUtils.singleMessageSet("test".getBytes())
    val config = logConfig.copy(segmentSize = set.sizeInBytes * 5, 
                                maxIndexSize = 1000, 
                                indexInterval = 10000)

    // create a log
    var log = new Log(logDir, 
                      config,
                      needsRecovery = true,
                      time.scheduler,
                      time)
    
    // add enough messages to roll over several segments then close and re-open and attempt to truncate
    for(i <- 0 until 100)
      log.append(set)
    log.close()
    log = new Log(logDir, 
                  config,
                  needsRecovery = true,
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
    val set = TestUtils.singleMessageSet("test".getBytes())
    val asyncDeleteMs = 1000
    val config = logConfig.copy(segmentSize = set.sizeInBytes * 5, 
                                fileDeleteDelayMs = asyncDeleteMs, 
                                maxIndexSize = 1000, 
                                indexInterval = 10000)
    val log = new Log(logDir,
                      config,
                      needsRecovery = true,                      
                      time.scheduler,
                      time)
    
    // append some messages to create some segments
    for(i <- 0 until 100)
      log.append(set)
    
    // files should be renamed
    val segments = log.logSegments.toArray
    val oldFiles = segments.map(_.log.file) ++ segments.map(_.index.file)
    log.deleteOldSegments((s) => true)
    
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
    val set = TestUtils.singleMessageSet("test".getBytes())
    val config = logConfig.copy(segmentSize = set.sizeInBytes * 5, maxIndexSize = 1000)
    var log = new Log(logDir,
                      config,
                      needsRecovery = false,
                      time.scheduler,
                      time)
    
    // append some messages to create some segments
    for(i <- 0 until 100)
      log.append(set)
    
    log.deleteOldSegments((s) => true)
    log.close()
    
    log = new Log(logDir, 
                  config,
                  needsRecovery = false,
                  time.scheduler,
                  time)
    assertEquals("The deleted segments should be gone.", 1, log.numberOfSegments)
  }
  
  @Test
  def testAppendMessageWithNullPayload() {
    var log = new Log(logDir,
                      LogConfig(),
                      needsRecovery = false,
                      time.scheduler,
                      time)
    log.append(new ByteBufferMessageSet(new Message(bytes = null)))
    val ms = log.read(0, 4096, None)
    assertEquals(0, ms.head.offset)
    assertTrue("Message payload should be null.", ms.head.message.isNull)
  }
  
}
