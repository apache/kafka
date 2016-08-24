/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
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
import kafka.message.ByteBufferMessageSet
import kafka.utils.{MockTime, Pool, TestUtils}
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{After, Test}
import org.scalatest.junit.JUnitSuite

class LogCleanerManagerTest extends JUnitSuite {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val time = new MockTime()

  @After
  def tearDown() {
    Utils.delete(tmpDir)
  }

  /**
    * When checking for logs with segments ready for deletion
    * we shouldn't consider logs where cleanup.policy=delete
    * as they are handled by the LogManager
    */
  @Test
  def testLogsWithSegmentsToDeleteShouldNotConsiderCleanupPolicyDeleteLogs() {
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


  def createCleanerManager(log: Log): LogCleanerManager = {
    val logs = new Pool[TopicAndPartition, Log]()
    logs.put(TopicAndPartition("log", 0), log)
    val cleanerManager = new LogCleanerManager(Array(logDir), logs)
    cleanerManager
  }

  def appendMessagesAndExpireSegments(set: ByteBufferMessageSet, log: Log): Unit = {
    // append some messages to create some segments
    for (i <- 0 until 100)
      log.append(set)

    // expire all segments
    log.logSegments.foreach(_.lastModified = time.milliseconds - 1000)
  }

  def createLog(segmentSize: Int, cleanupPolicy: String = "delete"): Log = {
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


}
