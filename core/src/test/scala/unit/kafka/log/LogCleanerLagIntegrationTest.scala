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

import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.CompressionType
import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection._

/**
  * This is an integration test that tests the fully integrated log cleaner
  */
@RunWith(value = classOf[Parameterized])
class LogCleanerLagIntegrationTest(compressionCodecName: String) extends AbstractLogCleanerIntegrationTest with Logging {
  val msPerHour = 60 * 60 * 1000

  val minCompactionLag = 1 * msPerHour
  assertTrue("compactionLag must be divisible by 2 for this test", minCompactionLag % 2 == 0)

  val time = new MockTime(1400000000000L, 1000L)  // Tue May 13 16:53:20 UTC 2014 for `currentTimeMs`
  val cleanerBackOffMs = 200L
  val segmentSize = 512

  override def codec: CompressionType = CompressionType.forName(compressionCodecName)

  val topicPartitions = Array(new TopicPartition("log", 0), new TopicPartition("log", 1), new TopicPartition("log", 2))

  @Test
  def cleanerTest(): Unit = {
    cleaner = makeCleaner(partitions = topicPartitions,
      backOffMs = cleanerBackOffMs,
      minCompactionLagMs = minCompactionLag,
      segmentSize = segmentSize)
    val log = cleaner.logs.get(topicPartitions(0))

    // t = T0
    val T0 = time.milliseconds
    val appends0 = writeDups(numKeys = 100, numDups = 3, log, codec, timestamp = T0)
    val startSizeBlock0 = log.size
    debug(s"total log size at T0: $startSizeBlock0")

    val activeSegAtT0 = log.activeSegment
    debug(s"active segment at T0 has base offset: ${activeSegAtT0.baseOffset}")
    val sizeUpToActiveSegmentAtT0 = log.logSegments(0L, activeSegAtT0.baseOffset).map(_.size).sum
    debug(s"log size up to base offset of active segment at T0: $sizeUpToActiveSegmentAtT0")

    cleaner.startup()

    // T0 < t < T1
    // advance to a time still less than one compaction lag from start
    time.sleep(minCompactionLag/2)
    Thread.sleep(5 * cleanerBackOffMs) // give cleaning thread a chance to _not_ clean
    assertEquals("There should be no cleaning until the compaction lag has passed", startSizeBlock0, log.size)

    // t = T1 > T0 + compactionLag
    // advance to time a bit more than one compaction lag from start
    time.sleep(minCompactionLag/2 + 1)
    val T1 = time.milliseconds

    // write another block of data
    val appends1 = appends0 ++ writeDups(numKeys = 100, numDups = 3, log, codec, timestamp = T1)
    val firstBlock1SegmentBaseOffset = activeSegAtT0.baseOffset

    // the first block should get cleaned
    cleaner.awaitCleaned(new TopicPartition("log", 0), activeSegAtT0.baseOffset)

    // check the data is the same
    val read1 = readFromLog(log)
    assertEquals("Contents of the map shouldn't change.", appends1.toMap, read1.toMap)

    val compactedSize = log.logSegments(0L, activeSegAtT0.baseOffset).map(_.size).sum
    debug(s"after cleaning the compacted size up to active segment at T0: $compactedSize")
    val lastCleaned = cleaner.cleanerManager.allCleanerCheckpoints(new TopicPartition("log", 0))
    assertTrue(s"log cleaner should have processed up to offset $firstBlock1SegmentBaseOffset, but lastCleaned=$lastCleaned", lastCleaned >= firstBlock1SegmentBaseOffset)
    assertTrue(s"log should have been compacted: size up to offset of active segment at T0=$sizeUpToActiveSegmentAtT0 compacted size=$compactedSize",
      sizeUpToActiveSegmentAtT0 > compactedSize)
  }

  private def readFromLog(log: Log): Iterable[(Int, Int)] = {
    import JavaConverters._

    for (segment <- log.logSegments; record <- segment.log.records.asScala) yield {
      val key = TestUtils.readString(record.key).toInt
      val value = TestUtils.readString(record.value).toInt
      key -> value
    }
  }

  private def writeDups(numKeys: Int, numDups: Int, log: Log, codec: CompressionType, timestamp: Long): Seq[(Int, Int)] = {
    for (_ <- 0 until numDups; key <- 0 until numKeys) yield {
      val count = counter
      log.appendAsLeader(TestUtils.singletonRecords(value = counter.toString.getBytes, codec = codec,
              key = key.toString.getBytes, timestamp = timestamp), leaderEpoch = 0)
      incCounter()
      (key, count)
    }
  }
}

object LogCleanerLagIntegrationTest {
  def oneParameter: java.util.Collection[Array[String]] = {
    val l = new java.util.ArrayList[Array[String]]()
    l.add(Array("NONE"))
    l
  }

  @Parameters
  def parameters: java.util.Collection[Array[String]] = {
    val list = new java.util.ArrayList[Array[String]]()
    for (codec <- CompressionType.values)
      list.add(Array(codec.name))
    list
  }
}
