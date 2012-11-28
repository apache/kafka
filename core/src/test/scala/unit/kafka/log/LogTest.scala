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
import junit.framework.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Before, Test}
import kafka.utils.{Utils, TestUtils, Range, SystemTime, MockTime}
import kafka.common.{MessageSizeTooLargeException, OffsetOutOfRangeException}
import kafka.message.{NoCompressionCodec, ByteBufferMessageSet, Message}
import kafka.server.KafkaConfig

class LogTest extends JUnitSuite {
  
  var logDir: File = null
  var config:KafkaConfig = null
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
  
  def createEmptyLogs(dir: File, offsets: Int*) = {
    for(offset <- offsets)
      new File(dir, Integer.toString(offset) + Log.FileSuffix).createNewFile()
  }

  /** Test that the size and time based log segment rollout works. */
  @Test
  def testTimeBasedLogRoll() {
    val set = TestUtils.singleMessageSet("test".getBytes())
    val rollMs = 1 * 60 * 60L
    val time: MockTime = new MockTime()

    // create a log
    val log = new Log(logDir, time, 1000, config.maxMessageSize, 1000, rollMs, false)
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
    log.append(new ByteBufferMessageSet(blank:_*))
    assertEquals("There should be exactly 3 segments.", 3, log.numberOfSegments)

    time.currentMs += rollMs + 1
    // the last segment expired in age, but was blank. So new segment should not be generated
    log.append(set)
    assertEquals("There should be exactly 3 segments.", 3, log.numberOfSegments)
  }

  @Test
  def testSizeBasedLogRoll() {
    val set = TestUtils.singleMessageSet("test".getBytes())
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val segSize = msgPerSeg * (setSize - 1).asInstanceOf[Int] // each segment will be 10 messages

    // create a log
    val log = new Log(logDir, SystemTime, segSize, config.maxMessageSize, 1000, 10000, false)
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
    new Log(logDir, SystemTime, 1024, config.maxMessageSize, 1000, config.logRollHours*60*60*1000L, false)
  }
  
  @Test
  def testLoadInvalidLogsFails() {
    createEmptyLogs(logDir, 0, 15)
    try {
      new Log(logDir, SystemTime, 1024, config.maxMessageSize, 1000, config.logRollHours*60*60*1000L, false)
      fail("Allowed load of corrupt logs without complaint.")
    } catch {
      case e: IllegalStateException => "This is good"
    }
  }
  
  @Test
  def testAppendAndRead() {
    val log = new Log(logDir, SystemTime, 1024, config.maxMessageSize, 1000, config.logRollHours*60*60*1000L, false)
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
    val log = new Log(logDir, SystemTime, 1024, config.maxMessageSize, 1000, config.logRollHours*60*60*1000L, false)
    assertEquals("Reading just beyond end of log should produce 0 byte read.", 0L, log.read(1024, 1000).sizeInBytes)
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
    val log = new Log(logDir, SystemTime, 100, config.maxMessageSize, 1000, config.logRollHours*60*60*1000L, false)
    val numMessages = 100
    for(i <- 0 until numMessages)
      log.append(TestUtils.singleMessageSet(Integer.toString(i).getBytes()))
    log.flush
    
    /* now do successive reads and iterate over the resulting message sets counting the messages
     * we should find exact 100 messages.
     */
    var reads = 0
    var current = 0
    var offset = 0L
    var readOffset = 0L
    while(current < numMessages) {
      val messages = log.read(readOffset, 1024*1024)
      readOffset += messages.last.offset
      current += messages.size
      if(reads > 2*numMessages)
        fail("Too many read attempts.")
      reads += 1
    }
    assertEquals("We did not find all the messages we put in", numMessages, current)
  }
  
  @Test
  def testFindSegment() {
    assertEquals("Search in empty segments list should find nothing", None, Log.findRange(makeRanges(), 45))
    assertEquals("Search in segment list just outside the range of the last segment should find nothing",
                 None, Log.findRange(makeRanges(5, 9, 12), 12))
    try {
      Log.findRange(makeRanges(35), 36)
      fail("expect exception")
    }
    catch {
      case e: OffsetOutOfRangeException => "this is good"
    }

    try {
      Log.findRange(makeRanges(35,35), 36)
    }
    catch {
      case e: OffsetOutOfRangeException => "this is good"
    }

    assertContains(makeRanges(5, 9, 12), 11)
    assertContains(makeRanges(5), 4)
    assertContains(makeRanges(5,8), 5)
    assertContains(makeRanges(5,8), 6)
  }
  
  /** Test corner cases of rolling logs */
  @Test
  def testEdgeLogRolls() {
    {
      // first test a log segment starting at 0
      val log = new Log(logDir, SystemTime, 100, config.maxMessageSize, 1000, config.logRollHours*60*60*1000L, false)
      val curOffset = log.nextAppendOffset
      assertEquals(curOffset, 0)

      // time goes by; the log file is deleted
      log.markDeletedWhile(_ => true)

      // we now have a new log; the starting offset of the new log should remain 0
      assertEquals(curOffset, log.nextAppendOffset)
    }

    {
      // second test an empty log segment starting at none-zero
      val log = new Log(logDir, SystemTime, 100, config.maxMessageSize, 1000, config.logRollHours*60*60*1000L, false)
      val numMessages = 1
      for(i <- 0 until numMessages)
        log.append(TestUtils.singleMessageSet(Integer.toString(i).getBytes()))

      val curOffset = log.nextAppendOffset
      // time goes by; the log file is deleted
      log.markDeletedWhile(_ => true)

      // we now have a new log
      assertEquals(curOffset, log.nextAppendOffset)

      // time goes by; the log file (which is empty) is deleted again
      val deletedSegments = log.markDeletedWhile(_ => true)

      // we shouldn't delete the last empty log segment.
      assertTrue(deletedSegments.size == 0)

      // we now have a new log
      assertEquals(curOffset, log.nextAppendOffset)
    }
  }

  @Test
  def testMessageSizeCheck() {
    val first = new ByteBufferMessageSet(NoCompressionCodec, new Message ("You".getBytes()), new Message("bethe".getBytes()))
    val second = new ByteBufferMessageSet(NoCompressionCodec, new Message("change".getBytes()))

    // append messages to log
    val log = new Log(logDir, SystemTime, 100, 5, 1000, 24*7*60*60*1000L, false)

    var ret =
    try {
      log.append(first)
      true
    }
    catch {
      case e: MessageSizeTooLargeException => false
    }
    assert(ret, "First messageset should pass.")

    ret =
    try {
      log.append(second)
      false
    }
    catch {
      case e:MessageSizeTooLargeException => true
    }
    assert(ret, "Second message set should throw MessageSizeTooLargeException.")
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
