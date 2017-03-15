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

import java.io.File
import java.util.Properties

import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.utils.Utils
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
class LogCleanerLagIntegrationTest(compressionCodecName: String) extends Logging {
  val msPerHour = 60 * 60 * 1000

  val compactionLag = 1 * msPerHour
  assertTrue("compactionLag must be divisible by 2 for this test", compactionLag % 2 == 0)

  val time = new MockTime(1400000000000L, 1000L)  // Tue May 13 16:53:20 UTC 2014 for `currentTimeMs`
  val cleanerBackOffMs = 200L
  val segmentSize = 100
  val deleteDelay = 1000
  val logName = "log"
  val logDir = TestUtils.tempDir()
  var counter = 0
  val topics = Array(new TopicPartition("log", 0), new TopicPartition("log", 1), new TopicPartition("log", 2))
  val compressionCodec = CompressionType.forName(compressionCodecName)

  @Test
  def cleanerTest(): Unit = {
    val cleaner = makeCleaner(parts = 3, backOffMs = cleanerBackOffMs)
    val log = cleaner.logs.get(topics(0))

    // t = T0
    val T0 = time.milliseconds
    val appends0 = writeDups(numKeys = 100, numDups = 3, log, compressionCodec, timestamp = T0)
    val startSizeBlock0 = log.size
    debug(s"total log size at T0: $startSizeBlock0")

    val activeSegAtT0 = log.activeSegment
    debug(s"active segment at T0 has base offset: ${activeSegAtT0.baseOffset}")
    val sizeUpToActiveSegmentAtT0 = log.logSegments(0L, activeSegAtT0.baseOffset).map(_.size).sum
    debug(s"log size up to base offset of active segment at T0: $sizeUpToActiveSegmentAtT0")

    cleaner.startup()

    // T0 < t < T1
    // advance to a time still less than one compaction lag from start
    time.sleep(compactionLag/2)
    Thread.sleep(5 * cleanerBackOffMs) // give cleaning thread a chance to _not_ clean
    assertEquals("There should be no cleaning until the compaction lag has passed", startSizeBlock0, log.size)

    // t = T1 > T0 + compactionLag
    // advance to time a bit more than one compaction lag from start
    time.sleep(compactionLag/2 + 1)
    val T1 = time.milliseconds

    // write another block of data
    val appends1 = appends0 ++ writeDups(numKeys = 100, numDups = 3, log, compressionCodec, timestamp = T1)
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

    cleaner.logs.remove(topics(0))
    cleaner.shutdown()
  }

  private def readFromLog(log: Log): Iterable[(Int, Int)] = {
    import JavaConverters._

    for (segment <- log.logSegments; logEntry <- segment.log.deepEntries.asScala) yield {
      val key = TestUtils.readString(logEntry.record.key).toInt
      val value = TestUtils.readString(logEntry.record.value).toInt
      key -> value
    }
  }

  private def writeDups(numKeys: Int, numDups: Int, log: Log, codec: CompressionType, timestamp: Long): Seq[(Int, Int)] = {
    for (_ <- 0 until numDups; key <- 0 until numKeys) yield {
      val count = counter
      log.append(TestUtils.singletonRecords(value = counter.toString.getBytes, codec = codec, key = key.toString.getBytes, timestamp = timestamp), assignOffsets = true)
      counter += 1
      (key, count)
    }
  }

  @After
  def teardown(): Unit = {
    time.scheduler.shutdown()
    Utils.delete(logDir)
  }

  /* create a cleaner instance and logs with the given parameters */
  private def makeCleaner(parts: Int,
                  minCleanableDirtyRatio: Float = 0.0F,
                  numThreads: Int = 1,
                  backOffMs: Long = 200L,
                  defaultPolicy: String = "compact",
                  policyOverrides: Map[String, String] = Map()): LogCleaner = {

    // create partitions and add them to the pool
    val logs = new Pool[TopicPartition, Log]()
    for(i <- 0 until parts) {
      val dir = new File(logDir, "log-" + i)
      dir.mkdirs()
      val logProps = new Properties()
      logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
      logProps.put(LogConfig.SegmentIndexBytesProp, 100*1024: java.lang.Integer)
      logProps.put(LogConfig.FileDeleteDelayMsProp, deleteDelay: java.lang.Integer)
      logProps.put(LogConfig.MinCompactionLagMsProp, compactionLag: java.lang.Integer)
      logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
      logProps.put(LogConfig.MinCleanableDirtyRatioProp, minCleanableDirtyRatio: java.lang.Float)

      val log = new Log(dir = dir,
        LogConfig(logProps),
        recoveryPoint = 0L,
        scheduler = time.scheduler,
        time = time)
      logs.put(new TopicPartition("log", i), log)
    }

    new LogCleaner(CleanerConfig(numThreads = numThreads, backOffMs = backOffMs),
      logDirs = Array(logDir),
      logs = logs,
      time = time)
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
