/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.remote

import java.io.File
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import kafka.log.Log
import kafka.log.remote.RemoteLogIndexTest.generateEntries
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.junit.JUnitSuite

import scala.collection.mutable.ListBuffer

class TopicPartitionRemoteIndexTest extends JUnitSuite with Logging {

  private val partition = new TopicPartition("topic", 0)

  @Test
  def testAppendLookupIndexWithBaseAndReopenIndex(): Unit = {
    val entriesCt = 10
    val offsetStep = 100
    val dir = TestUtils.tempDir()
    val topicPartitionRemoteIndex = TopicPartitionRemoteIndex.open(partition, dir)

    // check for baseOffset with 0
    doTestIndexes(entriesCt, offsetStep, 0L, dir, topicPartitionRemoteIndex)

    // check for baseOffset with 10000
    doTestIndexes(entriesCt, offsetStep, 10000L, dir, topicPartitionRemoteIndex)

    // reopen the index and check whether offsets are defined.
    val reopenedPartitionIndex = TopicPartitionRemoteIndex.open(partition, dir)

    assertEquals(0L, reopenedPartitionIndex.startOffset.get)
    assertEquals(10000L, reopenedPartitionIndex.lastBatchStartOffset.get)
    assertEquals(10999L, reopenedPartitionIndex.lastOffset.get)
  }

  @Test
  def testNegativeOffset(): Unit = {
    val dir = TestUtils.tempDir()
    val topicPartitionRemoteIndex = TopicPartitionRemoteIndex.open(partition, dir)

    assertThrows[IllegalArgumentException](doTestIndexes(10, 50, -1L, dir, topicPartitionRemoteIndex))
  }

  private def doTestIndexes(entriesCt: Int, offsetStep: Integer, baseOffset: Long, dir: File,
                            partitionRemoteIndex: TopicPartitionRemoteIndex) = {
    val entries = generateEntries(entriesCt, offsetStep, baseOffset)
    partitionRemoteIndex.appendEntries(entries, baseOffset.toString)

    entries.foreach(entry => assertEquals(entry, partitionRemoteIndex.lookupEntryForOffset(entry.firstOffset).get))
  }

  @Test
  def testConcurrentAppendLookupsInIndex(): Unit = {
    val threadCt = 64
    val dir = TestUtils.tempDir()
    val rlmIndex = TopicPartitionRemoteIndex.open(partition, dir)
    var lastOffset = 1000L
    val stepOffset = 100
    val entriesCt = 100000
    val workers = new ListBuffer[Runnable]
    val failed: AtomicBoolean = new AtomicBoolean(false)
    val latch: CountDownLatch = new CountDownLatch(threadCt)

    for (i <- 1 to threadCt) {
      val baseOffset = lastOffset + 1
      val entries = generateEntries(entriesCt, stepOffset, baseOffset)
      lastOffset = entries.last.lastOffset
      workers += new Runnable() {
        override def run(): Unit = {
          val firstOffset = entries.head.firstOffset
          val mayBeAddedOffset = rlmIndex.appendEntries(entries,
            baseOffsetStr = Log.filenamePrefixFromOffset(firstOffset))

          val result = if (mayBeAddedOffset.isDefined) {
            entries.count(entry => {
              entry.equals(rlmIndex.lookupEntryForOffset(entry.firstOffset).get)
            }) == entries.size
          } else {
            true
          }
          if (!result) failed.compareAndSet(false, true)
          latch.countDown()
        }
      }
    }
    val executorService = Executors.newFixedThreadPool(threadCt)

    try {
      workers.toList.map(worker => executorService.submit(worker))
      latch.await(2, TimeUnit.MINUTES)
      assert(!failed.get())
    } finally {
      executorService.shutdownNow()
    }
  }

  @Test
  def testCleanupIndexUntilOffset(): Unit = {
    val dir = TestUtils.tempDir()
    val rlmIndex = TopicPartitionRemoteIndex.open(partition, dir)
    List(100, 300, 500, 700, 900).foreach(x =>
      rlmIndex.appendEntries(generateEntries(100, 1, x), x.toString))

    val indexes = rlmIndex.cleanupIndexesUntil(300)
    assertEquals(3, indexes.size)

    indexes.foreach(x => assert(x.file.getName.endsWith(Log.DeletedFileSuffix)))

    assert(rlmIndex.lookupEntryForOffset(100).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(150).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(200).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(250).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(300).isDefined)
    assert(rlmIndex.lookupEntryForOffset(450).isDefined)

    val indexesTill750 = rlmIndex.cleanupIndexesUntil(750)
    assertEquals(6, indexesTill750.size)

    assert(rlmIndex.lookupEntryForOffset(100).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(250).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(500).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(700).isDefined)
    assert(rlmIndex.lookupEntryForOffset(950).isDefined)

  }

  @Test
  def testLookupTimestamp(): Unit = {
    val dir = TestUtils.tempDir()
    val rlmIndex = TopicPartitionRemoteIndex.open(partition, dir)
    List(100, 300, 500, 700, 900).foreach(x =>
      rlmIndex.appendEntries(generateEntries(100, 2, x), x.toString))

    assertEquals(100, rlmIndex.lookupEntryForTimestamp(0, 0).get.firstOffset)
    assertEquals(100, rlmIndex.lookupEntryForTimestamp(100000, 100).get.firstOffset)
    assertEquals(100, rlmIndex.lookupEntryForTimestamp(100000, 101).get.firstOffset)
    assertEquals(102, rlmIndex.lookupEntryForTimestamp(100000, 102).get.firstOffset)
    assertEquals(300, rlmIndex.lookupEntryForTimestamp(100000, 300).get.firstOffset)
    assertEquals(180, rlmIndex.lookupEntryForTimestamp(180000, 0).get.firstOffset)
    assertEquals(600, rlmIndex.lookupEntryForTimestamp(600000, 0).get.firstOffset)
    assert(rlmIndex.lookupEntryForTimestamp(1100000, 0).isEmpty)
    assert(rlmIndex.lookupEntryForTimestamp(0, 1100).isEmpty)

    rlmIndex.appendEntries(generateEntries(100, 2, 1100), 1100.toString)
    assertEquals(1100, rlmIndex.lookupEntryForTimestamp(1100000, 0).get.firstOffset)
    assertEquals(1104, rlmIndex.lookupEntryForTimestamp(600000, 1105).get.firstOffset)
    assert(rlmIndex.lookupEntryForTimestamp(1300000, 0).isEmpty)

    rlmIndex.cleanupIndexesUntil(500)
    assertEquals(500, rlmIndex.lookupEntryForTimestamp(0, 0).get.firstOffset)
    assertEquals(600, rlmIndex.lookupEntryForTimestamp(600000, 0).get.firstOffset)
    assertEquals(1104, rlmIndex.lookupEntryForTimestamp(600000, 1105).get.firstOffset)
  }
}