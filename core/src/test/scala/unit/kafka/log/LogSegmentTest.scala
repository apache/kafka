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

import org.junit.Assert._
import java.util.concurrent.atomic._

import kafka.common.LongRef
import org.junit.{After, Test}
import kafka.utils.TestUtils
import kafka.message._
import kafka.utils.SystemTime

import scala.collection._
 import scala.collection.mutable.ListBuffer

class LogSegmentTest {
  
  val segments = mutable.ArrayBuffer[LogSegment]()
  
  /* create a segment with the given base offset */
  def createSegment(offset: Long, indexIntervalBytes: Int = 10): LogSegment = {
    val msFile = TestUtils.tempFile()
    val ms = new FileMessageSet(msFile)
    val idxFile = TestUtils.tempFile()
    val timeIdxFile = TestUtils.tempFile()
    idxFile.delete()
    timeIdxFile.delete()
    val idx = new OffsetIndex(idxFile, offset, 1000)
    val timeIdx = new TimeIndex(timeIdxFile, offset, 1500)
    val seg = new LogSegment(ms, idx, timeIdx, offset, indexIntervalBytes, 0, SystemTime)
    segments += seg
    seg
  }
  
  /* create a ByteBufferMessageSet for the given messages starting from the given offset */
  def messages(offset: Long, messages: String*): ByteBufferMessageSet = {
    new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, 
                             offsetCounter = new LongRef(offset),
                             messages = messages.map(s => new Message(s.getBytes, offset * 10, Message.MagicValue_V1)):_*)
  }
  
  @After
  def teardown() {
    for(seg <- segments) {
      seg.index.delete()
      seg.log.delete()
    }
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
    val ms = messages(50, "hello", "there", "little", "bee")
    seg.append(50, Message.NoTimestamp, -1L, ms)
    val read = seg.read(startOffset = 41, maxSize = 300, maxOffset = None).messageSet
    assertEquals(ms.toList, read.toList)
  }
  
  /**
   * If we set the startOffset and maxOffset for the read to be the same value
   * we should get only the first message in the log
   */
  @Test
  def testMaxOffset() {
    val baseOffset = 50
    val seg = createSegment(baseOffset)
    val ms = messages(baseOffset, "hello", "there", "beautiful")
    seg.append(baseOffset, Message.NoTimestamp, -1L, ms)
    def validate(offset: Long) = 
      assertEquals(ms.filter(_.offset == offset).toList, 
                   seg.read(startOffset = offset, maxSize = 1024, maxOffset = Some(offset+1)).messageSet.toList)
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
    val ms = messages(50, "hello", "there")
    seg.append(50, Message.NoTimestamp, -1L, ms)
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
    val ms = messages(50, "hello", "there")
    seg.append(50, Message.NoTimestamp, -1L, ms)
    val ms2 = messages(60, "alpha", "beta")
    seg.append(60, Message.NoTimestamp, -1L, ms2)
    val read = seg.read(startOffset = 55, maxSize = 200, maxOffset = None)
    assertEquals(ms2.toList, read.messageSet.toList)
  }
  
  /**
   * In a loop append two messages then truncate off the second of those messages and check that we can read
   * the first but not the second message.
   */
  @Test
  def testTruncate() {
    val seg = createSegment(40)
    var offset = 40
    for(i <- 0 until 30) {
      val ms1 = messages(offset, "hello")
      seg.append(offset, Message.NoTimestamp, -1L, ms1)
      val ms2 = messages(offset + 1, "hello")
      seg.append(offset + 1, Message.NoTimestamp, -1L, ms2)
      // check that we can read back both messages
      val read = seg.read(offset, None, 10000)
      assertEquals(List(ms1.head, ms2.head), read.messageSet.toList)
      // now truncate off the last message
      seg.truncateTo(offset + 1)
      val read2 = seg.read(offset, None, 10000)
      assertEquals(1, read2.messageSet.size)
      assertEquals(ms1.head, read2.messageSet.head)
      offset += 1
    }
  }

  @Test
  def testReloadLargestTimestampAfterTruncation() {
    val numMessages = 30
    val seg = createSegment(40, 2 * messages(0, "hello").sizeInBytes - 1)
    var offset = 40
    for (i <- 0 until numMessages) {
      seg.append(offset, offset, offset, messages(offset, "hello"))
      offset += 1
    }
    val expectedNumEntries = numMessages / 2 - 1
    assertEquals(s"Should have $expectedNumEntries time indexes", expectedNumEntries, seg.timeIndex.entries)

    seg.truncateTo(41)
    assertEquals(s"Should have 0 time indexes", 0, seg.timeIndex.entries)
    assertEquals(s"Largest timestamp should be 400", 400L, seg.largestTimestamp)

  }

  /**
   * Test truncating the whole segment, and check that we can reappend with the original offset.
   */
  @Test
  def testTruncateFull() {
    // test the case where we fully truncate the log
    val seg = createSegment(40)
    seg.append(40, Message.NoTimestamp, -1L, messages(40, "hello", "there"))
    seg.truncateTo(0)
    assertNull("Segment should be empty.", seg.read(0, None, 1024))
    seg.append(40, Message.NoTimestamp, -1L, messages(40, "hello", "there"))
  }

  /**
   * Append messages with timestamp and search message by timestamp.
   */
  @Test
  def testFindOffsetByTimestamp() {
    val messageSize = messages(0, s"msg00").sizeInBytes
    val seg = createSegment(40, messageSize * 2 - 1)
    // Produce some messages
    for (i <- 40 until 50)
      seg.append(i, i * 10, i, messages(i, s"msg$i"))

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
    assertEquals(40, seg.nextOffset)
    seg.append(50, Message.NoTimestamp, -1L, messages(50, "hello", "there", "you"))
    assertEquals(53, seg.nextOffset())
  }
  
  /**
   * Test that we can change the file suffixes for the log and index files
   */
  @Test
  def testChangeFileSuffixes() {
    val seg = createSegment(40)
    val logFile = seg.log.file
    val indexFile = seg.index.file
    seg.changeFileSuffixes("", ".deleted")
    assertEquals(logFile.getAbsolutePath + ".deleted", seg.log.file.getAbsolutePath)
    assertEquals(indexFile.getAbsolutePath + ".deleted", seg.index.file.getAbsolutePath)
    assertTrue(seg.log.file.exists)
    assertTrue(seg.index.file.exists)
  }
  
  /**
   * Create a segment with some data and an index. Then corrupt the index,
   * and recover the segment, the entries should all be readable.
   */
  @Test
  def testRecoveryFixesCorruptIndex() {
    val seg = createSegment(0)
    for(i <- 0 until 100)
      seg.append(i, Message.NoTimestamp, -1L, messages(i, i.toString))
    val indexFile = seg.index.file
    TestUtils.writeNonsenseToFile(indexFile, 5, indexFile.length.toInt)
    seg.recover(64*1024)
    for(i <- 0 until 100)
      assertEquals(i, seg.read(i, Some(i + 1), 1024).messageSet.head.offset)
  }

  /**
   * Create a segment with some data and an index. Then corrupt the index,
   * and recover the segment, the entries should all be readable.
   */
  @Test
  def testRecoveryFixesCorruptTimeIndex() {
    val seg = createSegment(0)
    for(i <- 0 until 100)
      seg.append(i, i * 10, i, messages(i, i.toString))
    val timeIndexFile = seg.timeIndex.file
    TestUtils.writeNonsenseToFile(timeIndexFile, 5, timeIndexFile.length.toInt)
    seg.recover(64*1024)
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
    for(iteration <- 0 until 10) {
      val seg = createSegment(0)
      for(i <- 0 until messagesAppended)
        seg.append(i, Message.NoTimestamp, -1L, messages(i, i.toString))
      val offsetToBeginCorruption = TestUtils.random.nextInt(messagesAppended)
      // start corrupting somewhere in the middle of the chosen record all the way to the end
      val position = seg.log.searchForOffsetWithSize(offsetToBeginCorruption, 0)._1.position + TestUtils.random.nextInt(15)
      TestUtils.writeNonsenseToFile(seg.log.file, position, seg.log.file.length.toInt - position)
      seg.recover(64*1024)
      assertEquals("Should have truncated off bad messages.", (0 until offsetToBeginCorruption).toList, seg.log.map(_.offset).toList)
      seg.delete()
    }
  }

  /* create a segment with   pre allocate */
  def createSegment(offset: Long, fileAlreadyExists: Boolean, initFileSize: Int, preallocate: Boolean): LogSegment = {
    val tempDir = TestUtils.tempDir()
    val seg = new LogSegment(tempDir, offset, 10, 1000, 0, SystemTime, fileAlreadyExists = fileAlreadyExists, initFileSize = initFileSize, preallocate = preallocate)
    segments += seg
    seg
  }

  /* create a segment with   pre allocate, put message to it and verify */
  @Test
  def testCreateWithInitFileSizeAppendMessage() {
    val seg = createSegment(40, false, 512*1024*1024, true)
    val ms = messages(50, "hello", "there")
    seg.append(50, Message.NoTimestamp, -1L, ms)
    val ms2 = messages(60, "alpha", "beta")
    seg.append(60, Message.NoTimestamp, -1L, ms2)
    val read = seg.read(startOffset = 55, maxSize = 200, maxOffset = None)
    assertEquals(ms2.toList, read.messageSet.toList)
  }

  /* create a segment with   pre allocate and clearly shut down*/
  @Test
  def testCreateWithInitFileSizeClearShutdown() {
    val tempDir = TestUtils.tempDir()
    val seg = new LogSegment(tempDir, 40, 10, 1000, 0, SystemTime, false, 512*1024*1024, true)

    val ms = messages(50, "hello", "there")
    seg.append(50, Message.NoTimestamp, -1L, ms)
    val ms2 = messages(60, "alpha", "beta")
    seg.append(60, Message.NoTimestamp, -1L, ms2)
    val read = seg.read(startOffset = 55, maxSize = 200, maxOffset = None)
    assertEquals(ms2.toList, read.messageSet.toList)
    val oldSize = seg.log.sizeInBytes()
    val oldPosition = seg.log.channel.position
    val oldFileSize = seg.log.file.length
    assertEquals(512*1024*1024, oldFileSize)
    seg.close()
    //After close, file should be trimmed
    assertEquals(oldSize, seg.log.file.length)

    val segReopen = new LogSegment(tempDir, 40, 10, 1000, 0, SystemTime, true,  512*1024*1024, true)
    segments += segReopen

    val readAgain = segReopen.read(startOffset = 55, maxSize = 200, maxOffset = None)
    assertEquals(ms2.toList, readAgain.messageSet.toList)
    val size = segReopen.log.sizeInBytes()
    val position = segReopen.log.channel.position
    val fileSize = segReopen.log.file.length
    assertEquals(oldPosition, position)
    assertEquals(oldSize, size)
    assertEquals(size, fileSize)
  }
}
