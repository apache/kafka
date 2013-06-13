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

import junit.framework.Assert._
import java.util.concurrent.atomic._
import org.junit.{Test, After}
import org.scalatest.junit.JUnit3Suite
import kafka.utils.TestUtils
import kafka.message._
import kafka.utils.SystemTime
import scala.collection._

class LogSegmentTest extends JUnit3Suite {
  
  val segments = mutable.ArrayBuffer[LogSegment]()
  
  def createSegment(offset: Long): LogSegment = {
    val msFile = TestUtils.tempFile()
    val ms = new FileMessageSet(msFile)
    val idxFile = TestUtils.tempFile()
    idxFile.delete()
    val idx = new OffsetIndex(idxFile, offset, 1000)
    val seg = new LogSegment(ms, idx, offset, 10, SystemTime)
    segments += seg
    seg
  }
  
  def messages(offset: Long, messages: String*): ByteBufferMessageSet = {
    new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, 
                             offsetCounter = new AtomicLong(offset), 
                             messages = messages.map(s => new Message(s.getBytes)):_*)
  }
  
  @After
  def teardown() {
    for(seg <- segments) {
      seg.index.delete()
      seg.messageSet.delete()
    }
  }
  
  @Test
  def testReadOnEmptySegment() {
    val seg = createSegment(40)
    val read = seg.read(startOffset = 40, maxSize = 300, maxOffset = None)
    assertEquals(0, read.size)
  }
  
  @Test
  def testReadBeforeFirstOffset() {
    val seg = createSegment(40)
    val ms = messages(50, "hello", "there", "little", "bee")
    seg.append(50, ms)
    val read = seg.read(startOffset = 41, maxSize = 300, maxOffset = None)
    assertEquals(ms.toList, read.toList)
  }
  
  @Test
  def testReadSingleMessage() {
    val seg = createSegment(40)
    val ms = messages(50, "hello", "there")
    seg.append(50, ms)
    val read = seg.read(startOffset = 41, maxSize = 200, maxOffset = Some(50))
    assertEquals(new Message("hello".getBytes), read.head.message)
  }
  
  @Test
  def testReadAfterLast() {
    val seg = createSegment(40)
    val ms = messages(50, "hello", "there")
    seg.append(50, ms)
    val read = seg.read(startOffset = 52, maxSize = 200, maxOffset = None)
    assertEquals(0, read.size)
  }
  
  @Test
  def testReadFromGap() {
    val seg = createSegment(40)
    val ms = messages(50, "hello", "there")
    seg.append(50, ms)
    val ms2 = messages(60, "alpha", "beta")
    seg.append(60, ms2)
    val read = seg.read(startOffset = 55, maxSize = 200, maxOffset = None)
    assertEquals(ms2.toList, read.toList)
  }
  
  @Test
  def testTruncate() {
    val seg = createSegment(40)
    var offset = 40
    for(i <- 0 until 30) {
      val ms1 = messages(offset, "hello")
      seg.append(offset, ms1)
      val ms2 = messages(offset+1, "hello")
      seg.append(offset+1, ms2)
      // check that we can read back both messages
      val read = seg.read(offset, 10000, None)
      assertEquals(List(ms1.head, ms2.head), read.toList)
      // now truncate off the last message
      seg.truncateTo(offset + 1)
      val read2 = seg.read(offset, 10000, None)
      assertEquals(1, read2.size)
      assertEquals(ms1.head, read2.head)
      offset += 1
    }
  }
  
  @Test
  def testTruncateFull() {
    // test the case where we fully truncate the log
    val seg = createSegment(40)
    seg.append(40, messages(40, "hello", "there"))
    seg.truncateTo(0)
    seg.append(40, messages(40, "hello", "there"))    
  }
  
  @Test
  def testNextOffsetCalculation() {
    val seg = createSegment(40)
    assertEquals(40, seg.nextOffset)
    seg.append(50, messages(50, "hello", "there", "you"))
    assertEquals(53, seg.nextOffset())
  }
  
}