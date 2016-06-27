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
import java.util.Properties

import kafka.common._
import kafka.server.OffsetCheckpoint
import kafka.utils._
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{After, Before, Test}

class LogManagerTest {

  val time: MockTime = new MockTime()
  val maxRollInterval = 100
  val maxLogAgeMs = 10*60*60*1000
  val logProps = new Properties()
  logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.SegmentIndexBytesProp, 4096: java.lang.Integer)
  logProps.put(LogConfig.RetentionMsProp, maxLogAgeMs: java.lang.Integer)
  val logConfig = LogConfig(logProps)
  var logDir: File = null
  var logManager: LogManager = null
  val name = "kafka"
  val veryLargeLogFlushInterval = 10000000L

  @Before
  def setUp() {
    logDir = TestUtils.tempDir()
    logManager = createLogManager()
    logManager.startup
    logDir = logManager.logDirs(0)
  }

  @After
  def tearDown() {
    if(logManager != null)
      logManager.shutdown()
    Utils.delete(logDir)
    logManager.logDirs.foreach(Utils.delete)
  }

  /**
   * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
   */
  @Test
  def testCreateLog() {
    val log = logManager.createLog(TopicAndPartition(name, 0), logConfig)
    val logFile = new File(logDir, name + "-0")
    assertTrue(logFile.exists)
    log.append(TestUtils.singleMessageSet("test".getBytes()))
  }

  /**
   * Test that get on a non-existent returns None and no log is created.
   */
  @Test
  def testGetNonExistentLog() {
    val log = logManager.getLog(TopicAndPartition(name, 0))
    assertEquals("No log should be found.", None, log)
    val logFile = new File(logDir, name + "-0")
    assertTrue(!logFile.exists)
  }

  /**
   * Test time-based log cleanup. First append messages, then set the time into the future and run cleanup.
   */
  @Test
  def testCleanupExpiredSegments() {
    val log = logManager.createLog(TopicAndPartition(name, 0), logConfig)
    var offset = 0L
    for(i <- 0 until 200) {
      var set = TestUtils.singleMessageSet("test".getBytes())
      val info = log.append(set)
      offset = info.lastOffset
    }
    assertTrue("There should be more than one segment now.", log.numberOfSegments > 1)

    log.logSegments.foreach(_.log.file.setLastModified(time.milliseconds))

    time.sleep(maxLogAgeMs + 1)
    assertEquals("Now there should only be only one segment in the index.", 1, log.numberOfSegments)
    time.sleep(log.config.fileDeleteDelayMs + 1)
    assertEquals("Files should have been deleted", log.numberOfSegments * 2, log.dir.list.length)
    assertEquals("Should get empty fetch off new log.", 0, log.read(offset+1, 1024).messageSet.sizeInBytes)

    try {
      log.read(0, 1024)
      fail("Should get exception from fetching earlier.")
    } catch {
      case e: OffsetOutOfRangeException => "This is good."
    }
    // log should still be appendable
    log.append(TestUtils.singleMessageSet("test".getBytes()))
  }

  /**
   * Test size-based cleanup. Append messages, then run cleanup and check that segments are deleted.
   */
  @Test
  def testCleanupSegmentsToMaintainSize() {
    val setSize = TestUtils.singleMessageSet("test".getBytes()).sizeInBytes
    logManager.shutdown()
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 10 * setSize: java.lang.Integer)
    logProps.put(LogConfig.RetentionBytesProp, 5L * 10L * setSize + 10L: java.lang.Long)
    val config = LogConfig.fromProps(logConfig.originals, logProps)

    logManager = createLogManager()
    logManager.startup

    // create a log
    val log = logManager.createLog(TopicAndPartition(name, 0), config)
    var offset = 0L

    // add a bunch of messages that should be larger than the retentionSize
    val numMessages = 200
    for(i <- 0 until numMessages) {
      val set = TestUtils.singleMessageSet("test".getBytes())
      val info = log.append(set)
      offset = info.firstOffset
    }

    assertEquals("Check we have the expected number of segments.", numMessages * setSize / config.segmentSize, log.numberOfSegments)

    // this cleanup shouldn't find any expired segments but should delete some to reduce size
    time.sleep(logManager.InitialTaskDelayMs)
    assertEquals("Now there should be exactly 6 segments", 6, log.numberOfSegments)
    time.sleep(log.config.fileDeleteDelayMs + 1)
    assertEquals("Files should have been deleted", log.numberOfSegments * 2, log.dir.list.length)
    assertEquals("Should get empty fetch off new log.", 0, log.read(offset + 1, 1024).messageSet.sizeInBytes)
    try {
      log.read(0, 1024)
      fail("Should get exception from fetching earlier.")
    } catch {
      case e: OffsetOutOfRangeException => "This is good."
    }
    // log should still be appendable
    log.append(TestUtils.singleMessageSet("test".getBytes()))
  }

  /**
   * Test that flush is invoked by the background scheduler thread.
   */
  @Test
  def testTimeBasedFlush() {
    logManager.shutdown()
    val logProps = new Properties()
    logProps.put(LogConfig.FlushMsProp, 1000: java.lang.Integer)
    val config = LogConfig.fromProps(logConfig.originals, logProps)

    logManager = createLogManager()
    logManager.startup
    val log = logManager.createLog(TopicAndPartition(name, 0), config)
    val lastFlush = log.lastFlushTime
    for(i <- 0 until 200) {
      var set = TestUtils.singleMessageSet("test".getBytes())
      log.append(set)
    }
    time.sleep(logManager.InitialTaskDelayMs)
    assertTrue("Time based flush should have been triggered triggered", lastFlush != log.lastFlushTime)
  }

  /**
   * Test that new logs that are created are assigned to the least loaded log directory
   */
  @Test
  def testLeastLoadedAssignment() {
    // create a log manager with multiple data directories
    val dirs = Array(TestUtils.tempDir(),
                     TestUtils.tempDir(),
                     TestUtils.tempDir())
    logManager.shutdown()
    logManager = createLogManager()

    // verify that logs are always assigned to the least loaded partition
    for(partition <- 0 until 20) {
      logManager.createLog(TopicAndPartition("test", partition), logConfig)
      assertEquals("We should have created the right number of logs", partition + 1, logManager.allLogs.size)
      val counts = logManager.allLogs.groupBy(_.dir.getParent).values.map(_.size)
      assertTrue("Load should balance evenly", counts.max <= counts.min + 1)
    }
  }

  /**
   * Test that it is not possible to open two log managers using the same data directory
   */
  @Test
  def testTwoLogManagersUsingSameDirFails() {
    try {
      createLogManager()
      fail("Should not be able to create a second log manager instance with the same data directory")
    } catch {
      case e: KafkaException => // this is good
    }
  }

  /**
   * Test that recovery points are correctly written out to disk
   */
  @Test
  def testCheckpointRecoveryPoints() {
    verifyCheckpointRecovery(Seq(TopicAndPartition("test-a", 1), TopicAndPartition("test-b", 1)), logManager)
  }

  /**
   * Test that recovery points directory checking works with trailing slash
   */
  @Test
  def testRecoveryDirectoryMappingWithTrailingSlash() {
    logManager.shutdown()
    logDir = TestUtils.tempDir()
    logManager = TestUtils.createLogManager(
      logDirs = Array(new File(logDir.getAbsolutePath + File.separator)))
    logManager.startup
    verifyCheckpointRecovery(Seq(TopicAndPartition("test-a", 1)), logManager)
  }

  /**
   * Test that recovery points directory checking works with relative directory
   */
  @Test
  def testRecoveryDirectoryMappingWithRelativeDirectory() {
    logManager.shutdown()
    logDir = new File("data" + File.separator + logDir.getName)
    logDir.mkdirs()
    logDir.deleteOnExit()
    logManager = createLogManager()
    logManager.startup
    verifyCheckpointRecovery(Seq(TopicAndPartition("test-a", 1)), logManager)
  }


  private def verifyCheckpointRecovery(topicAndPartitions: Seq[TopicAndPartition],
                                       logManager: LogManager) {
    val logs = topicAndPartitions.map(this.logManager.createLog(_, logConfig))
    logs.foreach(log => {
      for(i <- 0 until 50)
        log.append(TestUtils.singleMessageSet("test".getBytes()))

      log.flush()
    })

    logManager.checkpointRecoveryPointOffsets()
    val checkpoints = new OffsetCheckpoint(new File(logDir, logManager.RecoveryPointCheckpointFile)).read()

    topicAndPartitions.zip(logs).foreach {
      case(tp, log) => {
        assertEquals("Recovery point should equal checkpoint", checkpoints(tp), log.recoveryPoint)
      }
    }
  }


  private def createLogManager(logDirs: Array[File] = Array(this.logDir)): LogManager = {
    TestUtils.createLogManager(
      defaultConfig = logConfig,
      logDirs = logDirs,
      time = this.time)
  }
}
