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

import kafka.common._
import kafka.message._
import kafka.utils._
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{After, Test}
import org.scalatest.junit.JUnitSuite

/**
  * Unit tests for the log cleaning logic
  */
class LogCleanerManagerTest extends JUnitSuite with Logging {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val logProps = new Properties()
  logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.SegmentIndexBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
  val logConfig = LogConfig(logProps)
  val time = new MockTime(1400000000000L)  // Tue May 13 16:53:20 UTC 2014

  @After
  def tearDown(): Unit = {
    Utils.delete(tmpDir)
  }

  /**
    * When checking for logs with segments ready for deletion
    * we shouldn't consider logs where cleanup.policy=delete
    * as they are handled by the LogManager
    */
  @Test
  def testLogsWithSegmentsToDeleteShouldNotConsiderCleanupPolicyDeleteLogs(): Unit = {
    val messageSet = TestUtils.singleMessageSet("test".getBytes)
    val log: Log = createLog(messageSet.sizeInBytes * 5, LogConfig.Delete)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    val readyToDelete = cleanerManager.deletableLogs().size
    assertEquals("should have 0 logs ready to be deleted", 0, readyToDelete)
  }

  /**
    * We should find logs with segments ready to be deleted when cleanup.policy=compact,delete
    */
  @Test
  def testLogsWithSegmentsToDeleteShouldConsiderCleanupPolicyCompactDeleteLogs(): Unit = {
    val messageSet = TestUtils.singleMessageSet("test".getBytes, key="test".getBytes)
    val log: Log = createLog(messageSet.sizeInBytes * 5, LogConfig.Compact + "," + LogConfig.Delete)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    val readyToDelete = cleanerManager.deletableLogs().size
    assertEquals("should have 1 logs ready to be deleted", 1, readyToDelete)
  }

  /**
    * When looking for logs with segments ready to be deleted we shouldn't consider
    * logs with cleanup.policy=compact as they shouldn't have segments truncated.
    */
  @Test
  def testLogsWithSegmentsToDeleteShouldNotConsiderCleanupPolicyCompactLogs(): Unit = {
    val messageSet = TestUtils.singleMessageSet("test".getBytes, key="test".getBytes)
    val log: Log = createLog(messageSet.sizeInBytes * 5, LogConfig.Compact)
    val cleanerManager: LogCleanerManager = createCleanerManager(log)

    val readyToDelete = cleanerManager.deletableLogs().size
    assertEquals("should have 1 logs ready to be deleted", 0, readyToDelete)
  }

  /**
    * Test computation of cleanable range with no minimum compaction lag settings active
    */
  @Test
  def testCleanableOffsetsForNone(): Unit = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    while(log.numberOfSegments < 8)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt, timestamp = time.milliseconds))

    val topicAndPartition = TopicAndPartition("log", 0)
    val lastClean = Map(topicAndPartition-> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicAndPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable offset starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The first uncleanable offset begins with the active segment.", log.activeSegment.baseOffset, cleanableOffsets._2)
  }

  /**
    * Test computation of cleanable range with a minimum compaction lag time
    */
  @Test
  def testCleanableOffsetsForTime(): Unit = {
    val compactionLag = 60 * 60 * 1000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.MinCompactionLagMsProp, compactionLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val t0 = time.milliseconds
    while(log.numberOfSegments < 4)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt, timestamp = t0))

    val activeSegAtT0 = log.activeSegment

    time.sleep(compactionLag + 1)
    val t1 = time.milliseconds

    while (log.numberOfSegments < 8)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt, timestamp = t1))

    val topicAndPartition = TopicAndPartition("log", 0)
    val lastClean = Map(topicAndPartition-> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicAndPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable offset starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The first uncleanable offset begins with the second block of log entries.", activeSegAtT0.baseOffset, cleanableOffsets._2)
  }

  /**
    * Test computation of cleanable range with a minimum compaction lag time that is small enough that
    * the active segment contains it.
    */
  @Test
  def testCleanableOffsetsForShortTime(): Unit = {
    val compactionLag = 60 * 60 * 1000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.MinCompactionLagMsProp, compactionLag: java.lang.Integer)

    val log = makeLog(config = LogConfig.fromProps(logConfig.originals, logProps))

    val t0 = time.milliseconds
    while (log.numberOfSegments < 8)
      log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt, timestamp = t0))

    time.sleep(compactionLag + 1)

    val topicAndPartition = TopicAndPartition("log", 0)
    val lastClean = Map(topicAndPartition-> 0L)
    val cleanableOffsets = LogCleanerManager.cleanableOffsets(log, topicAndPartition, lastClean, time.milliseconds)
    assertEquals("The first cleanable offset starts at the beginning of the log.", 0L, cleanableOffsets._1)
    assertEquals("The first uncleanable offset begins with active segment.", log.activeSegment.baseOffset, cleanableOffsets._2)
  }

  private def createCleanerManager(log: Log): LogCleanerManager = {
    val logs = new Pool[TopicAndPartition, Log]()
    logs.put(TopicAndPartition("log", 0), log)
    val cleanerManager = new LogCleanerManager(Array(logDir), logs)
    cleanerManager
  }

  private def appendMessagesAndExpireSegments(set: ByteBufferMessageSet, log: Log): Unit = {
    // append some messages to create some segments
    for (i <- 0 until 100)
      log.append(set)

    // expire all segments
    log.logSegments.foreach(_.lastModified = time.milliseconds - 1000)
  }

  private def createLog(segmentSize: Int, cleanupPolicy: String = "delete"): Log = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: Integer)
    logProps.put(LogConfig.RetentionMsProp, 1: Integer)
    logProps.put(LogConfig.CleanupPolicyProp, cleanupPolicy)

    val config = LogConfig(logProps)
    val partitionDir = new File(logDir, "log-0")
    val log = new Log(partitionDir,
      config,
      recoveryPoint = 0L,
      time.scheduler,
      time)
    log
  }

  private def makeLog(dir: File = logDir, config: LogConfig = logConfig) =
    new Log(dir = dir, config = config, recoveryPoint = 0L, scheduler = time.scheduler, time = time)

  private def message(key: Int, value: Int, timestamp: Long) =
    new ByteBufferMessageSet(new Message(key = key.toString.getBytes,
      bytes = value.toString.getBytes,
      timestamp = timestamp,
      magicValue = Message.MagicValue_V1))

}
