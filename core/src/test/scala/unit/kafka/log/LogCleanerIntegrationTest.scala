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

import java.io.PrintWriter

import com.yammer.metrics.core.{Gauge, MetricName}
import kafka.metrics.{KafkaMetricsGroup, KafkaYammerMetrics}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, RecordBatch}
import org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS
import org.junit.Assert._
import org.junit.{After, Test}

import scala.collection.{Iterable, Seq}
import scala.jdk.CollectionConverters._

/**
  * This is an integration test that tests the fully integrated log cleaner
  */
class LogCleanerIntegrationTest extends AbstractLogCleanerIntegrationTest with KafkaMetricsGroup {

  val codec: CompressionType = CompressionType.LZ4

  val time = new MockTime()
  val topicPartitions = Array(new TopicPartition("log", 0), new TopicPartition("log", 1), new TopicPartition("log", 2))

  @After
  def cleanup(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  @Test(timeout = DEFAULT_MAX_WAIT_MS)
  def testMarksPartitionsAsOfflineAndPopulatesUncleanableMetrics(): Unit = {
    val largeMessageKey = 20
    val (_, largeMessageSet) = createLargeSingleMessageSet(largeMessageKey, RecordBatch.CURRENT_MAGIC_VALUE)
    val maxMessageSize = largeMessageSet.sizeInBytes
    cleaner = makeCleaner(partitions = topicPartitions, maxMessageSize = maxMessageSize, backOffMs = 100)

    def breakPartitionLog(tp: TopicPartition): Unit = {
      val log = cleaner.logs.get(tp)
      writeDups(numKeys = 20, numDups = 3, log = log, codec = codec)

      val partitionFile = log.logSegments.last.log.file()
      val writer = new PrintWriter(partitionFile)
      writer.write("jogeajgoea")
      writer.close()

      writeDups(numKeys = 20, numDups = 3, log = log, codec = codec)
    }

    breakPartitionLog(topicPartitions(0))
    breakPartitionLog(topicPartitions(1))

    cleaner.startup()

    val log = cleaner.logs.get(topicPartitions(0))
    val log2 = cleaner.logs.get(topicPartitions(1))
    val uncleanableDirectory = log.dir.getParent
    val uncleanablePartitionsCountGauge = getGauge[Int]("uncleanable-partitions-count", uncleanableDirectory)
    val uncleanableBytesGauge = getGauge[Long]("uncleanable-bytes", uncleanableDirectory)

    TestUtils.waitUntilTrue(() => uncleanablePartitionsCountGauge.value() == 2, "There should be 2 uncleanable partitions", 2000L)
    val expectedTotalUncleanableBytes = LogCleanerManager.calculateCleanableBytes(log, 0, log.logSegments.last.baseOffset)._2 +
      LogCleanerManager.calculateCleanableBytes(log2, 0, log2.logSegments.last.baseOffset)._2
    TestUtils.waitUntilTrue(() => uncleanableBytesGauge.value() == expectedTotalUncleanableBytes,
      s"There should be $expectedTotalUncleanableBytes uncleanable bytes", 1000L)

    val uncleanablePartitions = cleaner.cleanerManager.uncleanablePartitions(uncleanableDirectory)
    assertTrue(uncleanablePartitions.contains(topicPartitions(0)))
    assertTrue(uncleanablePartitions.contains(topicPartitions(1)))
    assertFalse(uncleanablePartitions.contains(topicPartitions(2)))
  }

  private def getGauge[T](filter: MetricName => Boolean): Gauge[T] = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .filter { case (k, _) => filter(k) }
      .headOption
      .getOrElse { fail(s"Unable to find metric") }
      .asInstanceOf[(Any, Gauge[Any])]
      ._2
      .asInstanceOf[Gauge[T]]
  }

  private def getGauge[T](metricName: String): Gauge[T] = {
    getGauge(mName => mName.getName.endsWith(metricName) && mName.getScope == null)
  }

  private def getGauge[T](metricName: String, metricScope: String): Gauge[T] = {
    getGauge(k => k.getName.endsWith(metricName) && k.getScope.endsWith(metricScope))
  }

  @Test
  def testMaxLogCompactionLag(): Unit = {
    val msPerHour = 60 * 60 * 1000

    val minCompactionLagMs = 1 * msPerHour
    val maxCompactionLagMs = 6 * msPerHour

    val cleanerBackOffMs = 200L
    val segmentSize = 512
    val topicPartitions = Array(new TopicPartition("log", 0), new TopicPartition("log", 1), new TopicPartition("log", 2))
    val minCleanableDirtyRatio = 1.0F

    cleaner = makeCleaner(partitions = topicPartitions,
      backOffMs = cleanerBackOffMs,
      minCompactionLagMs = minCompactionLagMs,
      segmentSize = segmentSize,
      maxCompactionLagMs= maxCompactionLagMs,
      minCleanableDirtyRatio = minCleanableDirtyRatio)
    val log = cleaner.logs.get(topicPartitions(0))

    val T0 = time.milliseconds
    writeKeyDups(numKeys = 100, numDups = 3, log, CompressionType.NONE, timestamp = T0, startValue = 0, step = 1)

    val startSizeBlock0 = log.size

    val activeSegAtT0 = log.activeSegment

    cleaner.startup()

    // advance to a time still less than maxCompactionLagMs from start
    time.sleep(maxCompactionLagMs/2)
    Thread.sleep(5 * cleanerBackOffMs) // give cleaning thread a chance to _not_ clean
    assertEquals("There should be no cleaning until the max compaction lag has passed", startSizeBlock0, log.size)

    // advance to time a bit more than one maxCompactionLagMs from start
    time.sleep(maxCompactionLagMs/2 + 1)
    val T1 = time.milliseconds

    // write the second block of data: all zero keys
    val appends1 = writeKeyDups(numKeys = 100, numDups = 1, log, CompressionType.NONE, timestamp = T1, startValue = 0, step = 0)

    // roll the active segment
    log.roll()
    val activeSegAtT1 = log.activeSegment
    val firstBlockCleanableSegmentOffset = activeSegAtT0.baseOffset

    // the first block should get cleaned
    cleaner.awaitCleaned(new TopicPartition("log", 0), firstBlockCleanableSegmentOffset)

    val read1 = readFromLog(log)
    val lastCleaned = cleaner.cleanerManager.allCleanerCheckpoints(new TopicPartition("log", 0))
    assertTrue(s"log cleaner should have processed at least to offset $firstBlockCleanableSegmentOffset, " +
      s"but lastCleaned=$lastCleaned", lastCleaned >= firstBlockCleanableSegmentOffset)

    //minCleanableDirtyRatio  will prevent second block of data from compacting
    assertNotEquals(s"log should still contain non-zero keys", appends1, read1)

    time.sleep(maxCompactionLagMs + 1)
    // the second block should get cleaned. only zero keys left
    cleaner.awaitCleaned(new TopicPartition("log", 0), activeSegAtT1.baseOffset)

    val read2 = readFromLog(log)

    assertEquals(s"log should only contains zero keys now", appends1, read2)

    val lastCleaned2 = cleaner.cleanerManager.allCleanerCheckpoints(new TopicPartition("log", 0))
    val secondBlockCleanableSegmentOffset = activeSegAtT1.baseOffset
    assertTrue(s"log cleaner should have processed at least to offset $secondBlockCleanableSegmentOffset, " +
      s"but lastCleaned=$lastCleaned2", lastCleaned2 >= secondBlockCleanableSegmentOffset)
  }

  private def readFromLog(log: Log): Iterable[(Int, Int)] = {
    for (segment <- log.logSegments; record <- segment.log.records.asScala) yield {
      val key = TestUtils.readString(record.key).toInt
      val value = TestUtils.readString(record.value).toInt
      key -> value
    }
  }

  private def writeKeyDups(numKeys: Int, numDups: Int, log: Log, codec: CompressionType, timestamp: Long, startValue: Int, step: Int): Seq[(Int, Int)] = {
    var valCounter = startValue
    for (_ <- 0 until numDups; key <- 0 until numKeys) yield {
      val curValue = valCounter
      log.appendAsLeader(TestUtils.singletonRecords(value = curValue.toString.getBytes, codec = codec,
        key = key.toString.getBytes, timestamp = timestamp), leaderEpoch = 0)
      valCounter += step
      (key, curValue)
    }
  }

  @Test
  def testIsThreadFailed(): Unit = {
    val metricName = "DeadThreadCount"
    cleaner = makeCleaner(partitions = topicPartitions, maxMessageSize = 100000, backOffMs = 100)
    cleaner.startup()
    assertEquals(0, cleaner.deadThreadCount)
    // we simulate the unexpected error with an interrupt
    cleaner.cleaners.foreach(_.interrupt())
    // wait until interruption is propagated to all the threads
    TestUtils.waitUntilTrue(
      () => cleaner.cleaners.foldLeft(true)((result, thread) => {
        thread.isThreadFailed && result
      }), "Threads didn't terminate unexpectedly"
    )
    assertEquals(cleaner.cleaners.size, getGauge[Int](metricName).value())
    assertEquals(cleaner.cleaners.size, cleaner.deadThreadCount)
  }
}
