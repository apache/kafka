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
import scala.collection._
import org.junit._
import kafka.common.TopicAndPartition
import kafka.utils._
import kafka.message._
import org.scalatest.junit.JUnitSuite
import junit.framework.Assert._

/**
 * This is an integration test that tests the fully integrated log cleaner
 */
class LogCleanerIntegrationTest extends JUnitSuite {
  
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

    val appends = writeDups(numKeys = 100, numDups = 3, log)
    val startSize = log.size
    cleaner.startup()
    
    val lastCleaned = log.activeSegment.baseOffset
    // wait until we clean up to base_offset of active segment - minDirtyMessages
    cleaner.awaitCleaned("log", 0, lastCleaned)
    
    val read = readFromLog(log)
    assertEquals("Contents of the map shouldn't change.", appends.toMap, read.toMap)
    assertTrue(startSize > log.size)
    
    // write some more stuff and validate again
    val appends2 = appends ++ writeDups(numKeys = 100, numDups = 3, log)
    val lastCleaned2 = log.activeSegment.baseOffset
    cleaner.awaitCleaned("log", 0, lastCleaned2)
    val read2 = readFromLog(log)
    assertEquals("Contents of the map shouldn't change.", appends2.toMap, read2.toMap)
    
    cleaner.shutdown()
  }
  
  def readFromLog(log: Log): Iterable[(Int, Int)] = {
    for(segment <- log.logSegments; message <- segment.log) yield {
      val key = Utils.readString(message.message.key).toInt
      val value = Utils.readString(message.message.payload).toInt
      key -> value
    }
  }
  
  def writeDups(numKeys: Int, numDups: Int, log: Log): Seq[(Int, Int)] = {
    for(dup <- 0 until numDups; key <- 0 until numKeys) yield {
      val count = counter
      log.append(TestUtils.singleMessageSet(payload = counter.toString.getBytes, key = key.toString.getBytes), assignOffsets = true)
      counter += 1
      (key, count)
    }
  }
    
  @After
  def teardown() {
    Utils.rm(logDir)
  }
  
  /* create a cleaner instance and logs with the given parameters */
  def makeCleaner(parts: Int, 
                  minDirtyMessages: Int = 0, 
                  numThreads: Int = 1,
                  defaultPolicy: String = "dedupe",
                  policyOverrides: Map[String, String] = Map()): LogCleaner = {
    
    // create partitions and add them to the pool
    val logs = new Pool[TopicAndPartition, Log]()
    for(i <- 0 until parts) {
      val dir = new File(logDir, "log-" + i)
      dir.mkdirs()
      val log = new Log(dir = dir,
                        LogConfig(segmentSize = segmentSize, maxIndexSize = 100*1024, fileDeleteDelayMs = deleteDelay, dedupe = true),
                        needsRecovery = false,
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