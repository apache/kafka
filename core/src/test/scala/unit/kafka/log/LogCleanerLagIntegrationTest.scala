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
import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import scala.collection._
import scala.jdk.CollectionConverters._

/**
  * This is an integration test that tests the fully integrated log cleaner
  */
class LogCleanerLagIntegrationTest extends AbstractLogCleanerIntegrationTest with Logging {
  val msPerHour = 60 * 60 * 1000

  val minCompactionLag = 1 * msPerHour
  assertTrue(minCompactionLag % 2 == 0, "compactionLag must be divisible by 2 for this test")

  val time = new MockTime(1400000000000L, 1000L)  // Tue May 13 16:53:20 UTC 2014 for `currentTimeMs`
  val cleanerBackOffMs = 200L
  val segmentSize = 512

  val topicPartitions = Array(new TopicPartition("log", 0), new TopicPartition("log", 1), new TopicPartition("log", 2))

  @ParameterizedTest
  @MethodSource(Array("parameters"))
  def cleanerTest(codec: CompressionType): Unit = {
    cleaner = makeCleaner(partitions = topicPartitions,
      backoffMs = cleanerBackOffMs,
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
    assertEquals(startSizeBlock0, log.size, "There should be no cleaning until the compaction lag has passed")

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
    assertEquals(appends1.toMap, read1.toMap, "Contents of the map shouldn't change.")

    val compactedSize = log.logSegments(0L, activeSegAtT0.baseOffset).map(_.size).sum
    debug(s"after cleaning the compacted size up to active segment at T0: $compactedSize")
    val lastCleaned = cleaner.cleanerManager.allCleanerCheckpoints(new TopicPartition("log", 0))
    assertTrue(lastCleaned >= firstBlock1SegmentBaseOffset, s"log cleaner should have processed up to offset $firstBlock1SegmentBaseOffset, but lastCleaned=$lastCleaned")
    assertTrue(sizeUpToActiveSegmentAtT0 > compactedSize, s"log should have been compacted: size up to offset of active segment at T0=$sizeUpToActiveSegmentAtT0 compacted size=$compactedSize")
  }

  private def readFromLog(log: UnifiedLog): Iterable[(Int, Int)] = {
    for (segment <- log.logSegments.asScala; record <- segment.log.records.asScala) yield {
      val key = TestUtils.readString(record.key).toInt
      val value = TestUtils.readString(record.value).toInt
      key -> value
    }
  }

  private def writeDups(numKeys: Int, numDups: Int, log: UnifiedLog, codec: CompressionType, timestamp: Long): Seq[(Int, Int)] = {
    for (_ <- 0 until numDups; key <- 0 until numKeys) yield {
      val count = counter
      log.appendAsLeader(TestUtils.singletonRecords(value = counter.toString.getBytes, codec = codec,
              key = key.toString.getBytes, timestamp = timestamp), leaderEpoch = 0)
      // move LSO forward to increase compaction bound
      log.updateHighWatermark(log.logEndOffset)
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

  def parameters: java.util.stream.Stream[Arguments] =
    java.util.Arrays.stream(CompressionType.values.map(codec => Arguments.of(codec)))
}
