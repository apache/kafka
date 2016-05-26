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

import kafka.common.TopicAndPartition
import kafka.message._
import kafka.server.OffsetCheckpoint
import kafka.utils._
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
class LogCleanerIntegrationTest(compressionCodec: String) {

  val time = new MockTime()
  val segmentSize = 100
  val deleteDelay = 1000
  val logName = "log"
  val logDir = TestUtils.tempDir()
  var counter = 0
  val topics = Array(TopicAndPartition("log", 0), TopicAndPartition("log", 1), TopicAndPartition("log", 2))

  @Test
  def cleanerTest() {
    val cleaner = makeCleaner(parts = 3)
    val log = cleaner.logs.get(topics(0))

    val appends = writeDups(numKeys = 100, numDups = 3, log, CompressionCodec.getCompressionCodec(compressionCodec))
    val startSize = log.size
    cleaner.startup()

    val firstDirty = log.activeSegment.baseOffset
    // wait until cleaning up to base_offset, note that cleaning happens only when "log dirty ratio" is higher than LogConfig.MinCleanableDirtyRatioProp
    cleaner.awaitCleaned("log", 0, firstDirty)
    val compactedSize = log.logSegments.map(_.size).sum
    val lastCleaned = cleaner.cleanerManager.allCleanerCheckpoints.get(TopicAndPartition("log", 0)).get
    assertTrue(s"log cleaner should have processed up to offset $firstDirty, but lastCleaned=$lastCleaned", lastCleaned >= firstDirty)
    assertTrue(s"log should have been compacted:  startSize=$startSize compactedSize=$compactedSize", startSize > compactedSize)
    
    val read = readFromLog(log)
    assertEquals("Contents of the map shouldn't change.", appends.toMap, read.toMap)
    assertTrue(startSize > log.size)

    // write some more stuff and validate again
    val appends2 = appends ++ writeDups(numKeys = 100, numDups = 3, log, CompressionCodec.getCompressionCodec(compressionCodec))
    val firstDirty2 = log.activeSegment.baseOffset
    cleaner.awaitCleaned("log", 0, firstDirty2)

    val lastCleaned2 = cleaner.cleanerManager.allCleanerCheckpoints.get(TopicAndPartition("log", 0)).get
    assertTrue(s"log cleaner should have processed up to offset $firstDirty2", lastCleaned2 >= firstDirty2);

    val read2 = readFromLog(log)
    assertEquals("Contents of the map shouldn't change.", appends2.toMap, read2.toMap)

    // simulate deleting a partition, by removing it from logs
    // force a checkpoint
    // and make sure its gone from checkpoint file

    cleaner.logs.remove(topics(0))

    cleaner.updateCheckpoints(logDir)
    val checkpoints = new OffsetCheckpoint(new File(logDir,cleaner.cleanerManager.offsetCheckpointFile)).read()

    // we expect partition 0 to be gone
    assert(!checkpoints.contains(topics(0)))
    cleaner.shutdown()
  }

  def readFromLog(log: Log): Iterable[(Int, Int)] = {
    for (segment <- log.logSegments; entry <- segment.log; messageAndOffset <- {
      // create single message iterator or deep iterator depending on compression codec
      if (entry.message.compressionCodec == NoCompressionCodec)
        Stream.cons(entry, Stream.empty).iterator
      else
        ByteBufferMessageSet.deepIterator(entry)
    }) yield {
      val key = TestUtils.readString(messageAndOffset.message.key).toInt
      val value = TestUtils.readString(messageAndOffset.message.payload).toInt
      key -> value
    }
  }

  def writeDups(numKeys: Int, numDups: Int, log: Log, codec: CompressionCodec): Seq[(Int, Int)] = {
    for(dup <- 0 until numDups; key <- 0 until numKeys) yield {
      val count = counter
      log.append(TestUtils.singleMessageSet(payload = counter.toString.getBytes, codec = codec, key = key.toString.getBytes), assignOffsets = true)
      counter += 1
      (key, count)
    }
  }
    
  @After
  def teardown() {
    time.scheduler.shutdown()
    Utils.delete(logDir)
  }
  
  /* create a cleaner instance and logs with the given parameters */
  def makeCleaner(parts: Int, 
                  minCleanableDirtyRatio: Float = 0.0F,
                  numThreads: Int = 1,
                  defaultPolicy: String = "compact",
                  policyOverrides: Map[String, String] = Map()): LogCleaner = {
    
    // create partitions and add them to the pool
    val logs = new Pool[TopicAndPartition, Log]()
    for(i <- 0 until parts) {
      val dir = new File(logDir, "log-" + i)
      dir.mkdirs()
      val logProps = new Properties()
      logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
      logProps.put(LogConfig.SegmentIndexBytesProp, 100*1024: java.lang.Integer)
      logProps.put(LogConfig.FileDeleteDelayMsProp, deleteDelay: java.lang.Integer)
      logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
      logProps.put(LogConfig.MinCleanableDirtyRatioProp, minCleanableDirtyRatio: java.lang.Float)

      val log = new Log(dir = dir,
                        LogConfig(logProps),
                        recoveryPoint = 0L,
                        scheduler = time.scheduler,
                        time = time)
      logs.put(TopicAndPartition("log", i), log)      
    }
  
    new LogCleaner(CleanerConfig(numThreads = numThreads),
                   logDirs = Array(logDir),
                   logs = logs,
                   time = time)
  }

}

object LogCleanerIntegrationTest {
  @Parameters
  def parameters: java.util.Collection[Array[String]] = {
    val list = new java.util.ArrayList[Array[String]]()
    for (codec <- CompressionType.values)
      list.add(Array(codec.name))
    list
  }
}
