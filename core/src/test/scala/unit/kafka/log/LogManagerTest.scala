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
import kafka.server.KafkaConfig
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Before, Test}
import kafka.utils.{Utils, MockTime, TestUtils}
import kafka.common.{InvalidTopicException, OffsetOutOfRangeException}
import collection.mutable.ArrayBuffer

class LogManagerTest extends JUnitSuite {

  val time: MockTime = new MockTime()
  val maxSegAge = 100
  val maxLogAge = 1000
  var logDir: File = null
  var logManager: LogManager = null
  var config:KafkaConfig = null

  @Before
  def setUp() {
    val props = TestUtils.createBrokerConfig(0, -1)
    config = new KafkaConfig(props) {
                   override val logFileSize = 1024
                   override val enableZookeeper = false
                   override val flushInterval = 100
                 }
    logManager = new LogManager(config, null, time, maxSegAge, -1, maxLogAge, false)
    logManager.startup
    logDir = logManager.logDir
  }

  @After
  def tearDown() {
    logManager.close()
    Utils.rm(logDir)
  }
  
  @Test
  def testCreateLog() {
    val name = "kafka"
    val log = logManager.getOrCreateLog(name, 0)
    val logFile = new File(config.logDir, name + "-0")
    assertTrue(logFile.exists)
    log.append(TestUtils.singleMessageSet("test".getBytes()))
  }

  @Test
  def testGetLog() {
    val name = "kafka"
    val log = logManager.getLog(name, 0)
    val logFile = new File(config.logDir, name + "-0")
    assertTrue(!logFile.exists)
  }

  @Test
  def testInvalidTopicName() {
    val invalidTopicNames = new ArrayBuffer[String]()
    invalidTopicNames += ("", ".", "..")
    var longName = "ATCG"
    for (i <- 3 to 8)
      longName += longName
    invalidTopicNames += longName
    val badChars = Array('/', '\u0000', '\u0001', '\u0018', '\u001F', '\u008F', '\uD805', '\uFFFA')
    for (weirdChar <- badChars) {
      invalidTopicNames += "Is" + weirdChar + "funny"
    }

    for (i <- 0 until invalidTopicNames.size) {
      try {
        logManager.getOrCreateLog(invalidTopicNames(i), 0)
        fail("Should throw InvalidTopicException.")
      }
      catch {
        case e: InvalidTopicException => "This is good."
      }
    }
  }

  @Test
  def testCleanupExpiredSegments() {
    val log = logManager.getOrCreateLog("cleanup", 0)
    var offset = 0L
    for(i <- 0 until 1000) {
      var set = TestUtils.singleMessageSet("test".getBytes())
      log.append(set)
      offset += set.sizeInBytes
    }
    log.flush

    assertTrue("There should be more than one segment now.", log.numberOfSegments > 1)

    // update the last modified time of all log segments
    val logSegments = log.segments.view
    logSegments.foreach(s => s.file.setLastModified(time.currentMs))

    time.currentMs += maxLogAge + 3000
    logManager.cleanupLogs()
    assertEquals("Now there should only be only one segment.", 1, log.numberOfSegments)
    assertEquals("Should get empty fetch off new log.", 0L, log.read(offset, 1024).sizeInBytes)
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
    logManager.close
    Thread.sleep(100)
    config = new KafkaConfig(props) {
      override val logFileSize = (10 * (setSize - 1)).asInstanceOf[Int] // each segment will be 10 messages
      override val enableZookeeper = false
      override val logRetentionSize = (5 * 10 * setSize + 10).asInstanceOf[Long]
      override val logRetentionHours = retentionHours
      override val flushInterval = 100
    }
    logManager = new LogManager(config, null, time, maxSegAge, -1, retentionMs, false)
    logManager.startup

    // create a log
    val log = logManager.getOrCreateLog("cleanup", 0)
    var offset = 0L

    // add a bunch of messages that should be larger than the retentionSize
    for(i <- 0 until 1000) {
      val set = TestUtils.singleMessageSet("test".getBytes())
      log.append(set)
      offset += set.sizeInBytes
    }
    // flush to make sure it's written to disk, then sleep to confirm
    log.flush
    Thread.sleep(2000)

    // should be exactly 100 full segments
    assertEquals("There should be example 100 segments.", 100, log.numberOfSegments)

    // this cleanup shouldn't find any expired segments but should delete some to reduce size
    logManager.cleanupLogs()
    assertEquals("Now there should be exactly 6 segments", 6, log.numberOfSegments)
    assertEquals("Should get empty fetch off new log.", 0L, log.read(offset, 1024).sizeInBytes)
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
    logManager.close
    Thread.sleep(100)
    config = new KafkaConfig(props) {
                   override val logFileSize = 1024 *1024 *1024 
                   override val enableZookeeper = false
                   override val flushSchedulerThreadRate = 50
                   override val flushInterval = Int.MaxValue
                   override val flushIntervalMap = Utils.getTopicFlushIntervals("timebasedflush:100")
                 }
    logManager = new LogManager(config, null, time, maxSegAge, -1, maxLogAge, false)
    logManager.startup
    val log = logManager.getOrCreateLog("timebasedflush", 0)
    for(i <- 0 until 200) {
      var set = TestUtils.singleMessageSet("test".getBytes())
      log.append(set)
    }

    assertTrue("The last flush time has to be within defaultflushInterval of current time ",
                     (System.currentTimeMillis - log.getLastFlushedTime) < 100)
  }

  @Test
  def testConfigurablePartitions() {
    val props = TestUtils.createBrokerConfig(0, -1)
    logManager.close
    Thread.sleep(100)
    config = new KafkaConfig(props) {
                   override val logFileSize = 256
                   override val enableZookeeper = false
                   override val topicPartitionsMap = Utils.getTopicPartitions("testPartition:2")
                   override val flushInterval = 100
                 }
    
    logManager = new LogManager(config, null, time, maxSegAge, -1, maxLogAge, false)
    logManager.startup
    
    for(i <- 0 until 2) {
      val log = logManager.getOrCreateLog("testPartition", i)
      for(i <- 0 until 250) {
        var set = TestUtils.singleMessageSet("test".getBytes())
        log.append(set)
      }
    }

    try
    {
      val log = logManager.getOrCreateLog("testPartition", 2)
      assertTrue("Should not come here", log != null)
    } catch {
       case _ =>
    }
  }
}
