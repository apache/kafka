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

import java.io._
import junit.framework.Assert._
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import kafka.server.KafkaConfig
import kafka.common._
import kafka.utils._

class LogManagerTest extends JUnit3Suite {

  val time: MockTime = new MockTime()
  val maxRollInterval = 100
  val maxLogAgeHours = 10
  var logDir: File = null
  var logManager: LogManager = null
  var config: KafkaConfig = null
  val name = "kafka"
  val veryLargeLogFlushInterval = 10000000L
  val scheduler = new KafkaScheduler(2)

  override def setUp() {
    super.setUp()
    config = new KafkaConfig(TestUtils.createBrokerConfig(0, -1)) {
                   override val logFileSize = 1024
                   override val flushInterval = 10000
                   override val logRetentionHours = maxLogAgeHours
                 }
    scheduler.startup
    logManager = new LogManager(config, scheduler, time)
    logManager.startup
    logDir = logManager.logDirs(0)
  }

  override def tearDown() {
    scheduler.shutdown()
    if(logManager != null)
      logManager.shutdown()
    Utils.rm(logDir)
    logManager.logDirs.map(Utils.rm(_))
    super.tearDown()
  }
  
  @Test
  def testCreateLog() {
    val log = logManager.getOrCreateLog(name, 0)
    val logFile = new File(config.logDirs(0), name + "-0")
    assertTrue(logFile.exists)
    log.append(TestUtils.singleMessageSet("test".getBytes()))
  }

  @Test
  def testGetLog() {
    val log = logManager.getLog(name, 0)
    val logFile = new File(config.logDirs(0), name + "-0")
    assertTrue(!logFile.exists)
  }

  @Test
  def testCleanupExpiredSegments() {
    val log = logManager.getOrCreateLog(name, 0)
    var offset = 0L
    for(i <- 0 until 1000) {
      var set = TestUtils.singleMessageSet("test".getBytes())
      val (start, end) = log.append(set)
      offset = end
    }
    log.flush

    assertTrue("There should be more than one segment now.", log.numberOfSegments > 1)

    // update the last modified time of all log segments
    val logSegments = log.segments.view
    logSegments.foreach(_.messageSet.file.setLastModified(time.currentMs))

    time.currentMs += maxLogAgeHours*60*60*1000 + 1
    logManager.cleanupLogs()
    assertEquals("Now there should only be only one segment.", 1, log.numberOfSegments)
    assertEquals("Should get empty fetch off new log.", 0, log.read(offset+1, 1024).sizeInBytes)

    try {
      log.read(0, 1024)
      fail("Should get exception from fetching earlier.")
    } catch {
      case e: OffsetOutOfRangeException => "This is good."
    }
    // log should still be appendable
    log.append(TestUtils.singleMessageSet("test".getBytes()))
  }

  @Test
  def testCleanupSegmentsToMaintainSize() {
    val setSize = TestUtils.singleMessageSet("test".getBytes()).sizeInBytes
    val retentionHours = 1
    val retentionMs = 1000 * 60 * 60 * retentionHours
    val props = TestUtils.createBrokerConfig(0, -1)
    logManager.shutdown()
    config = new KafkaConfig(props) {
      override val logFileSize = (10 * (setSize - 1)) // each segment will be 10 messages
      override val logRetentionSize = (5 * 10 * setSize + 10).asInstanceOf[Long]
      override val logRetentionHours = retentionHours
      override val flushInterval = 100
      override val logRollHours = maxRollInterval
    }
    logManager = new LogManager(config, scheduler, time)
    logManager.startup

    // create a log
    val log = logManager.getOrCreateLog(name, 0)
    var offset = 0L

    // add a bunch of messages that should be larger than the retentionSize
    for(i <- 0 until 1000) {
      val set = TestUtils.singleMessageSet("test".getBytes())
      val (start, end) = log.append(set)
      offset = start
    }
    // flush to make sure it's written to disk
    log.flush

    // should be exactly 100 full segments + 1 new empty one
    assertEquals("There should be example 100 segments.", 100, log.numberOfSegments)

    // this cleanup shouldn't find any expired segments but should delete some to reduce size
    logManager.cleanupLogs()
    assertEquals("Now there should be exactly 6 segments", 6, log.numberOfSegments)
    assertEquals("Should get empty fetch off new log.", 0, log.read(offset + 1, 1024).sizeInBytes)
    try {
      log.read(0, 1024)
      fail("Should get exception from fetching earlier.")
    } catch {
      case e: OffsetOutOfRangeException => "This is good."
    }
    // log should still be appendable
    log.append(TestUtils.singleMessageSet("test".getBytes()))
  }

  @Test
  def testTimeBasedFlush() {
    val props = TestUtils.createBrokerConfig(0, -1)
    logManager.shutdown()
    config = new KafkaConfig(props) {
                   override val logFileSize = 1024 *1024 *1024
                   override val flushSchedulerThreadRate = 50
                   override val flushInterval = Int.MaxValue
                   override val logRollHours = maxRollInterval
                   override val flushIntervalMap = Map("timebasedflush" -> 100)
                 }
    logManager = new LogManager(config, scheduler, time)
    logManager.startup
    val log = logManager.getOrCreateLog(name, 0)
    for(i <- 0 until 200) {
      var set = TestUtils.singleMessageSet("test".getBytes())
      log.append(set)
    }
    val ellapsed = System.currentTimeMillis - log.getLastFlushedTime
    assertTrue("The last flush time has to be within defaultflushInterval of current time (was %d)".format(ellapsed),
                     ellapsed < 2*config.flushSchedulerThreadRate)
  }
  
  @Test
  def testLeastLoadedAssignment() {
    // create a log manager with multiple data directories
    val props = TestUtils.createBrokerConfig(0, -1)
    val dirs = Seq(TestUtils.tempDir().getAbsolutePath, 
                   TestUtils.tempDir().getAbsolutePath, 
                   TestUtils.tempDir().getAbsolutePath)
    props.put("log.directories", dirs.mkString(","))
    logManager.shutdown()
    logManager = new LogManager(new KafkaConfig(props), scheduler, time)
    
    // verify that logs are always assigned to the least loaded partition
    for(partition <- 0 until 20) {
      logManager.getOrCreateLog("test", partition)
      assertEquals("We should have created the right number of logs", partition + 1, logManager.allLogs.size)
      val counts = logManager.allLogs.groupBy(_.dir.getParent).values.map(_.size)
      assertTrue("Load should balance evenly", counts.max <= counts.min + 1)
    }
  }
  
  def testTwoLogManagersUsingSameDirFails() {
    try {
      new LogManager(logManager.config, scheduler, time)
      fail("Should not be able to create a second log manager instance with the same data directory")
    } catch {
      case e: KafkaException => // this is good 
    }
  }
}
