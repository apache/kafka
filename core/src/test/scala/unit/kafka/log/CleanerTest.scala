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
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Test}
import java.nio._
import java.io.File
import scala.collection._
import kafka.common._
import kafka.utils._
import kafka.message._

/**
 * Unit tests for the log cleaning logic
 */
class CleanerTest extends JUnitSuite {
  
  val dir = TestUtils.tempDir()
  val logConfig = LogConfig(segmentSize=1024, maxIndexSize=1024, compact=true)
  val time = new MockTime()
  val throttler = new Throttler(desiredRatePerSec = Double.MaxValue, checkIntervalMs = Long.MaxValue, time = time)
  
  @After
  def teardown() {
    Utils.rm(dir)
  }
  
  /**
   * Test simple log cleaning
   */
  @Test
  def testCleanSegments() {
    val cleaner = makeCleaner(Int.MaxValue)
    val log = makeLog(config = logConfig.copy(segmentSize = 1024))
    
    // append messages to the log until we have four segments
    while(log.numberOfSegments < 4)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt))
    val keysFound = keysInLog(log)
    assertEquals((0L until log.logEndOffset), keysFound)
    
    // pretend we have the following keys
    val keys = immutable.ListSet(1, 3, 5, 7, 9)
    val map = new FakeOffsetMap(Int.MaxValue)
    keys.foreach(k => map.put(key(k), Long.MaxValue))
    
    // clean the log
    cleaner.cleanSegments(log, log.logSegments.take(3).toSeq, map, 0L)
    val shouldRemain = keysInLog(log).filter(!keys.contains(_))
    assertEquals(shouldRemain, keysInLog(log))
  }
  
  @Test
  def testCleaningWithDeletes() {
    val cleaner = makeCleaner(Int.MaxValue)
    val log = makeLog(config = logConfig.copy(segmentSize = 1024))
    
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
      
    cleaner.clean(LogToClean(TopicAndPartition("test", 0), log, 0))
    val keys = keysInLog(log).toSet
    assertTrue("None of the keys we deleted should still exist.", 
               (0 until leo.toInt by 2).forall(!keys.contains(_)))
  }
  
  /* extract all the keys from a log */
  def keysInLog(log: Log): Iterable[Int] = 
    log.logSegments.flatMap(s => s.log.filter(!_.message.isNull).map(m => Utils.readString(m.message.key).toInt))

  def abortCheckDone(topicAndPartition: TopicAndPartition) {
    throw new LogCleaningAbortedException()
  }

  /**
   * Test that abortion during cleaning throws a LogCleaningAbortedException
   */
  @Test
  def testCleanSegmentsWithAbort() {
    val cleaner = makeCleaner(Int.MaxValue, abortCheckDone)
    val log = makeLog(config = logConfig.copy(segmentSize = 1024))

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
  def testSegmentGrouping() {
    val cleaner = makeCleaner(Int.MaxValue)
    val log = makeLog(config = logConfig.copy(segmentSize = 300, indexInterval = 1))
    
    // append some messages to the log
    var i = 0
    while(log.numberOfSegments < 10) {
      log.append(TestUtils.singleMessageSet("hello".getBytes))
      i += 1
    }
    
    // grouping by very large values should result in a single group with all the segments in it
    var groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = Int.MaxValue)
    assertEquals(1, groups.size)
    assertEquals(log.numberOfSegments, groups(0).size)
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
    val indexSize = log.logSegments.take(groupSize).map(_.index.sizeInBytes()).sum + 1
    groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Int.MaxValue, maxIndexSize = indexSize)
    checkSegmentOrder(groups)
    assertTrue("All but the last group should be the target size.", groups.dropRight(1).forall(_.size == groupSize))
  }
  
  private def checkSegmentOrder(groups: Seq[Seq[LogSegment]]) {
    val offsets = groups.flatMap(_.map(_.baseOffset))
    assertEquals("Offsets should be in increasing order.", offsets.sorted, offsets)
  }
  
  /**
   * Test building an offset map off the log
   */
  @Test
  def testBuildOffsetMap() {
    val map = new FakeOffsetMap(1000)
    val log = makeLog()
    val cleaner = makeCleaner(Int.MaxValue)
    val start = 0
    val end = 500
    val offsets = writeToLog(log, (start until end) zip (start until end))
    def checkRange(map: FakeOffsetMap, start: Int, end: Int) {
      val endOffset = cleaner.buildOffsetMap(log, start, end, map) + 1
      assertEquals("Last offset should be the end offset.", end, endOffset)
      assertEquals("Should have the expected number of messages in the map.", end-start, map.size)
      for(i <- start until end)
        assertEquals("Should find all the keys", i.toLong, map.get(key(i)))
      assertEquals("Should not find a value too small", -1L, map.get(key(start-1)))
      assertEquals("Should not find a value too large", -1L, map.get(key(end)))
    }
    val segments = log.logSegments.toSeq
    checkRange(map, 0, segments(1).baseOffset.toInt)
    checkRange(map, segments(1).baseOffset.toInt, segments(3).baseOffset.toInt)
    checkRange(map, segments(3).baseOffset.toInt, log.logEndOffset.toInt)
  }
  
  def makeLog(dir: File = dir, config: LogConfig = logConfig) =
    new Log(dir = dir, config = config, recoveryPoint = 0L, scheduler = time.scheduler, time = time)

  def noOpCheckDone(topicAndPartition: TopicAndPartition) { /* do nothing */  }

  def makeCleaner(capacity: Int, checkDone: (TopicAndPartition) => Unit = noOpCheckDone) =
    new Cleaner(id = 0, 
                offsetMap = new FakeOffsetMap(capacity), 
                ioBufferSize = 64*1024, 
                maxIoBufferSize = 64*1024,
                dupBufferLoadFactor = 0.75,                
                throttler = throttler, 
                time = time,
                checkDone = checkDone )
  
  def writeToLog(log: Log, seq: Iterable[(Int, Int)]): Iterable[Long] = {
    for((key, value) <- seq)
      yield log.append(message(key, value)).firstOffset
  }
  
  def key(id: Int) = ByteBuffer.wrap(id.toString.getBytes)
  
  def message(key: Int, value: Int) = 
    new ByteBufferMessageSet(new Message(key=key.toString.getBytes, bytes=value.toString.getBytes))
  
  def deleteMessage(key: Int) =
    new ByteBufferMessageSet(new Message(key=key.toString.getBytes, bytes=null))
  
}

class FakeOffsetMap(val slots: Int) extends OffsetMap {
  val map = new java.util.HashMap[String, Long]()
  
  private def keyFor(key: ByteBuffer) = 
    new String(Utils.readBytes(key.duplicate), "UTF-8")
  
  def put(key: ByteBuffer, offset: Long): Unit = 
    map.put(keyFor(key), offset)
  
  def get(key: ByteBuffer): Long = {
    val k = keyFor(key)
    if(map.containsKey(k))
      map.get(k)
    else
      -1L
  }
  
  def clear() = map.clear()
  
  def size: Int = map.size
  
}