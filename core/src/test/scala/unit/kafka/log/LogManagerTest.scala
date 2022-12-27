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

import com.yammer.metrics.core.{Gauge, MetricName}
import kafka.log.remote.RemoteIndexCache
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.server.metadata.{ConfigRepository, MockConfigRepository}
import kafka.server.{BrokerTopicStats, FetchDataInfo, FetchLogEnd}
import kafka.utils._
import org.apache.directory.api.util.FileUtils
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.mockito.Mockito.{doAnswer, doNothing, mock, never, spy, times, verify}

import java.io._
import java.nio.file.Files
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Future}
import java.util.{Collections, Properties}
import org.apache.kafka.server.log.internals.LogDirFailureChannel
import org.apache.kafka.server.metrics.KafkaYammerMetrics

import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

class LogManagerTest {

  val time = new MockTime()
  val maxRollInterval = 100
  val maxLogAgeMs = 10 * 60 * 1000
  val logProps = new Properties()
  logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.SegmentIndexBytesProp, 4096: java.lang.Integer)
  logProps.put(LogConfig.RetentionMsProp, maxLogAgeMs: java.lang.Integer)
  logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, Long.MaxValue.toString)
  val logConfig = LogConfig(logProps)
  var logDir: File = _
  var logManager: LogManager = _
  val name = "kafka"
  val veryLargeLogFlushInterval = 10000000L

  @BeforeEach
  def setUp(): Unit = {
    logDir = TestUtils.tempDir()
    logManager = createLogManager()
    logManager.startup(Set.empty)
  }

  @AfterEach
  def tearDown(): Unit = {
    if (logManager != null)
      logManager.shutdown()
    Utils.delete(logDir)
    // Some tests assign a new LogManager
    if (logManager != null)
      logManager.liveLogDirs.foreach(Utils.delete)
  }

  /**
   * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
   */
  @Test
  def testCreateLog(): Unit = {
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), topicId = None)
    assertEquals(1, logManager.liveLogDirs.size)

    val logFile = new File(logDir, name + "-0")
    assertTrue(logFile.exists)
    log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)
  }

  /**
   * Tests that all internal futures are completed before LogManager.shutdown() returns to the
   * caller during error situations.
   */
  @Test
  def testHandlingExceptionsDuringShutdown(): Unit = {
    // We create two directories logDir1 and logDir2 to help effectively test error handling
    // during LogManager.shutdown().
    val logDir1 = TestUtils.tempDir()
    val logDir2 = TestUtils.tempDir()
    var logManagerForTest: Option[LogManager] = Option.empty
    try {
      logManagerForTest = Some(createLogManager(Seq(logDir1, logDir2)))

      assertEquals(2, logManagerForTest.get.liveLogDirs.size)
      logManagerForTest.get.startup(Set.empty)

      val log1 = logManagerForTest.get.getOrCreateLog(new TopicPartition(name, 0), topicId = None)
      val log2 = logManagerForTest.get.getOrCreateLog(new TopicPartition(name, 1), topicId = None)

      val logFile1 = new File(logDir1, name + "-0")
      assertTrue(logFile1.exists)
      val logFile2 = new File(logDir2, name + "-1")
      assertTrue(logFile2.exists)

      log1.appendAsLeader(TestUtils.singletonRecords("test1".getBytes()), leaderEpoch = 0)
      log1.takeProducerSnapshot()
      log1.appendAsLeader(TestUtils.singletonRecords("test1".getBytes()), leaderEpoch = 0)

      log2.appendAsLeader(TestUtils.singletonRecords("test2".getBytes()), leaderEpoch = 0)
      log2.takeProducerSnapshot()
      log2.appendAsLeader(TestUtils.singletonRecords("test2".getBytes()), leaderEpoch = 0)

      // This should cause log1.close() to fail during LogManger shutdown sequence.
      FileUtils.deleteDirectory(logFile1)

      logManagerForTest.get.shutdown()

      assertFalse(Files.exists(new File(logDir1, LogLoader.CleanShutdownFile).toPath))
      assertTrue(Files.exists(new File(logDir2, LogLoader.CleanShutdownFile).toPath))
    } finally {
      logManagerForTest.foreach(manager => manager.liveLogDirs.foreach(Utils.delete))
    }
  }

  /**
   * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
   * The LogManager is configured with one invalid log directory which should be marked as offline.
   */
  @Test
  def testCreateLogWithInvalidLogDir(): Unit = {
    // Configure the log dir with the Nul character as the path, which causes dir.getCanonicalPath() to throw an
    // IOException. This simulates the scenario where the disk is not properly mounted (which is hard to achieve in
    // a unit test)
    val dirs = Seq(logDir, new File("\u0000"))

    logManager.shutdown()
    logManager = createLogManager(dirs)
    logManager.startup(Set.empty)

    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), isNew = true, topicId = None)
    val logFile = new File(logDir, name + "-0")
    assertTrue(logFile.exists)
    log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)
  }

  @Test
  def testCreateLogWithLogDirFallback(): Unit = {
    // Configure a number of directories one level deeper in logDir,
    // so they all get cleaned up in tearDown().
    val dirs = (0 to 4)
      .map(_.toString)
      .map(logDir.toPath.resolve(_).toFile)

    // Create a new LogManager with the configured directories and an overridden createLogDirectory.
    logManager.shutdown()
    logManager = spy(createLogManager(dirs))
    val brokenDirs = mutable.Set[File]()
    doAnswer { invocation =>
      // The first half of directories tried will fail, the rest goes through.
      val logDir = invocation.getArgument[File](0)
      if (brokenDirs.contains(logDir) || brokenDirs.size < dirs.length / 2) {
        brokenDirs.add(logDir)
        Failure(new Throwable("broken dir"))
      } else {
        invocation.callRealMethod().asInstanceOf[Try[File]]
      }
    }.when(logManager).createLogDirectory(any(), any())
    logManager.startup(Set.empty)

    // Request creating a new log.
    // LogManager should try using all configured log directories until one succeeds.
    logManager.getOrCreateLog(new TopicPartition(name, 0), isNew = true, topicId = None)

    // Verify that half the directories were considered broken,
    assertEquals(dirs.length / 2, brokenDirs.size)

    // and that exactly one log file was created,
    val containsLogFile: File => Boolean = dir => new File(dir, name + "-0").exists()
    assertEquals(1, dirs.count(containsLogFile), "More than one log file created")

    // and that it wasn't created in one of the broken directories.
    assertFalse(brokenDirs.exists(containsLogFile))
  }

  /**
   * Test that get on a non-existent returns None and no log is created.
   */
  @Test
  def testGetNonExistentLog(): Unit = {
    val log = logManager.getLog(new TopicPartition(name, 0))
    assertEquals(None, log, "No log should be found.")
    val logFile = new File(logDir, name + "-0")
    assertFalse(logFile.exists)
  }

  /**
   * Test time-based log cleanup. First append messages, then set the time into the future and run cleanup.
   */
  @Test
  def testCleanupExpiredSegments(): Unit = {
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), topicId = None)
    var offset = 0L
    for(_ <- 0 until 200) {
      val set = TestUtils.singletonRecords("test".getBytes())
      val info = log.appendAsLeader(set, leaderEpoch = 0)
      offset = info.lastOffset
    }
    assertTrue(log.numberOfSegments > 1, "There should be more than one segment now.")
    log.updateHighWatermark(log.logEndOffset)

    log.logSegments.foreach(_.log.file.setLastModified(time.milliseconds))

    time.sleep(maxLogAgeMs + 1)
    assertEquals(1, log.numberOfSegments, "Now there should only be only one segment in the index.")
    time.sleep(log.config.fileDeleteDelayMs + 1)

    log.logSegments.foreach(s => {
      s.lazyOffsetIndex.get
      s.lazyTimeIndex.get
    })

    // there should be a log file, two indexes, one producer snapshot, and the leader epoch checkpoint
    assertEquals(log.numberOfSegments * 4 + 1, log.dir.list.length, "Files should have been deleted")
    assertEquals(0, readLog(log, offset + 1).records.sizeInBytes, "Should get empty fetch off new log.")

    assertThrows(classOf[OffsetOutOfRangeException], () => readLog(log, 0), () => "Should get exception from fetching earlier.")
    // log should still be appendable
    log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)
  }

  /**
   * Test size-based cleanup. Append messages, then run cleanup and check that segments are deleted.
   */
  @Test
  def testCleanupSegmentsToMaintainSize(): Unit = {
    val setSize = TestUtils.singletonRecords("test".getBytes()).sizeInBytes
    logManager.shutdown()
    val segmentBytes = 10 * setSize
    val properties = new Properties()
    properties.put(LogConfig.SegmentBytesProp, segmentBytes.toString)
    properties.put(LogConfig.RetentionBytesProp, (5L * 10L * setSize + 10L).toString)
    val configRepository = MockConfigRepository.forTopic(name, properties)

    logManager = createLogManager(configRepository = configRepository)
    logManager.startup(Set.empty)

    // create a log
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), topicId = None)
    var offset = 0L

    // add a bunch of messages that should be larger than the retentionSize
    val numMessages = 200
    for (_ <- 0 until numMessages) {
      val set = TestUtils.singletonRecords("test".getBytes())
      val info = log.appendAsLeader(set, leaderEpoch = 0)
      offset = info.firstOffset.get.messageOffset
    }

    log.updateHighWatermark(log.logEndOffset)
    assertEquals(numMessages * setSize / segmentBytes, log.numberOfSegments, "Check we have the expected number of segments.")

    // this cleanup shouldn't find any expired segments but should delete some to reduce size
    time.sleep(logManager.InitialTaskDelayMs)
    assertEquals(6, log.numberOfSegments, "Now there should be exactly 6 segments")
    time.sleep(log.config.fileDeleteDelayMs + 1)

    // there should be a log file, two indexes (the txn index is created lazily),
    // and a producer snapshot file per segment, and the leader epoch checkpoint.
    assertEquals(log.numberOfSegments * 4 + 1, log.dir.list.length, "Files should have been deleted")
    assertEquals(0, readLog(log, offset + 1).records.sizeInBytes, "Should get empty fetch off new log.")
    assertThrows(classOf[OffsetOutOfRangeException], () => readLog(log, 0))
    // log should still be appendable
    log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)
  }

  /**
    * Ensures that LogManager doesn't run on logs with cleanup.policy=compact,delete
    * LogCleaner.CleanerThread handles all logs where compaction is enabled.
    */
  @Test
  def testDoesntCleanLogsWithCompactDeletePolicy(): Unit = {
    testDoesntCleanLogs(LogConfig.Compact + "," + LogConfig.Delete)
  }

  /**
    * Ensures that LogManager doesn't run on logs with cleanup.policy=compact
    * LogCleaner.CleanerThread handles all logs where compaction is enabled.
    */
  @Test
  def testDoesntCleanLogsWithCompactPolicy(): Unit = {
    testDoesntCleanLogs(LogConfig.Compact)
  }

  private def testDoesntCleanLogs(policy: String): Unit = {
    logManager.shutdown()
    val configRepository = MockConfigRepository.forTopic(name, LogConfig.CleanupPolicyProp, policy)

    logManager = createLogManager(configRepository = configRepository)
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), topicId = None)
    var offset = 0L
    for (_ <- 0 until 200) {
      val set = TestUtils.singletonRecords("test".getBytes(), key="test".getBytes())
      val info = log.appendAsLeader(set, leaderEpoch = 0)
      offset = info.lastOffset
    }

    val numSegments = log.numberOfSegments
    assertTrue(log.numberOfSegments > 1, "There should be more than one segment now.")

    log.logSegments.foreach(_.log.file.setLastModified(time.milliseconds))

    time.sleep(maxLogAgeMs + 1)
    assertEquals(numSegments, log.numberOfSegments, "number of segments shouldn't have changed")
  }

  /**
   * Test that flush is invoked by the background scheduler thread.
   */
  @Test
  def testTimeBasedFlush(): Unit = {
    logManager.shutdown()
    val configRepository = MockConfigRepository.forTopic(name, LogConfig.FlushMsProp, "1000")

    logManager = createLogManager(configRepository = configRepository)
    logManager.startup(Set.empty)
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), topicId = None)
    val lastFlush = log.lastFlushTime
    for (_ <- 0 until 200) {
      val set = TestUtils.singletonRecords("test".getBytes())
      log.appendAsLeader(set, leaderEpoch = 0)
    }
    time.sleep(logManager.InitialTaskDelayMs)
    assertTrue(lastFlush != log.lastFlushTime, "Time based flush should have been triggered")
  }

  /**
   * Test that new logs that are created are assigned to the least loaded log directory
   */
  @Test
  def testLeastLoadedAssignment(): Unit = {
    // create a log manager with multiple data directories
    val dirs = Seq(TestUtils.tempDir(),
                     TestUtils.tempDir(),
                     TestUtils.tempDir())
    logManager.shutdown()
    logManager = createLogManager(dirs)

    // verify that logs are always assigned to the least loaded partition
    for(partition <- 0 until 20) {
      logManager.getOrCreateLog(new TopicPartition("test", partition), topicId = None)
      assertEquals(partition + 1, logManager.allLogs.size, "We should have created the right number of logs")
      val counts = logManager.allLogs.groupBy(_.dir.getParent).values.map(_.size)
      assertTrue(counts.max <= counts.min + 1, "Load should balance evenly")
    }
  }

  /**
   * Tests that the log manager skips the remote-log-index-cache directory when loading the logs from disk
   */
  @Test
  def testLoadLogsSkipRemoteIndexCache(): Unit = {
    val logDir = TestUtils.tempDir()
    val remoteIndexCache = new File(logDir, RemoteIndexCache.DirName)
    remoteIndexCache.mkdir()
    logManager = createLogManager(Seq(logDir))
    logManager.loadLogs(logConfig, Map.empty)
  }

  /**
   * Test that it is not possible to open two log managers using the same data directory
   */
  @Test
  def testTwoLogManagersUsingSameDirFails(): Unit = {
    assertThrows(classOf[KafkaException], () => createLogManager())
  }

  /**
   * Test that recovery points are correctly written out to disk
   */
  @Test
  def testCheckpointRecoveryPoints(): Unit = {
    verifyCheckpointRecovery(Seq(new TopicPartition("test-a", 1), new TopicPartition("test-b", 1)), logManager, logDir)
  }

  /**
   * Test that recovery points directory checking works with trailing slash
   */
  @Test
  def testRecoveryDirectoryMappingWithTrailingSlash(): Unit = {
    logManager.shutdown()
    logManager = TestUtils.createLogManager(logDirs = Seq(new File(TestUtils.tempDir().getAbsolutePath + File.separator)))
    logManager.startup(Set.empty)
    verifyCheckpointRecovery(Seq(new TopicPartition("test-a", 1)), logManager, logManager.liveLogDirs.head)
  }

  /**
   * Test that recovery points directory checking works with relative directory
   */
  @Test
  def testRecoveryDirectoryMappingWithRelativeDirectory(): Unit = {
    logManager.shutdown()
    logManager = createLogManager(Seq(new File("data", logDir.getName).getAbsoluteFile))
    logManager.startup(Set.empty)
    verifyCheckpointRecovery(Seq(new TopicPartition("test-a", 1)), logManager, logManager.liveLogDirs.head)
  }

  private def verifyCheckpointRecovery(topicPartitions: Seq[TopicPartition], logManager: LogManager, logDir: File): Unit = {
    val logs = topicPartitions.map(logManager.getOrCreateLog(_, topicId = None))
    logs.foreach { log =>
      for (_ <- 0 until 50)
        log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)

      log.flush(false)
    }

    logManager.checkpointLogRecoveryOffsets()
    val checkpoints = new OffsetCheckpointFile(new File(logDir, LogManager.RecoveryPointCheckpointFile)).read()

    topicPartitions.zip(logs).foreach { case (tp, log) =>
      assertEquals(checkpoints(tp), log.recoveryPoint, "Recovery point should equal checkpoint")
    }
  }

  private def createLogManager(logDirs: Seq[File] = Seq(this.logDir),
                               configRepository: ConfigRepository = new MockConfigRepository,
                               recoveryThreadsPerDataDir: Int = 1): LogManager = {
    TestUtils.createLogManager(
      defaultConfig = logConfig,
      configRepository = configRepository,
      logDirs = logDirs,
      time = this.time,
      recoveryThreadsPerDataDir = recoveryThreadsPerDataDir)
  }

  @Test
  def testFileReferencesAfterAsyncDelete(): Unit = {
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), topicId = None)
    val activeSegment = log.activeSegment
    val logName = activeSegment.log.file.getName
    val indexName = activeSegment.offsetIndex.file.getName
    val timeIndexName = activeSegment.timeIndex.file.getName
    val txnIndexName = activeSegment.txnIndex.file.getName
    val indexFilesOnDiskBeforeDelete = activeSegment.log.file.getParentFile.listFiles.filter(_.getName.endsWith("index"))

    val removedLog = logManager.asyncDelete(new TopicPartition(name, 0)).get
    val removedSegment = removedLog.activeSegment
    val indexFilesAfterDelete = Seq(removedSegment.lazyOffsetIndex.file, removedSegment.lazyTimeIndex.file,
      removedSegment.txnIndex.file)

    assertEquals(new File(removedLog.dir, logName), removedSegment.log.file)
    assertEquals(new File(removedLog.dir, indexName), removedSegment.lazyOffsetIndex.file)
    assertEquals(new File(removedLog.dir, timeIndexName), removedSegment.lazyTimeIndex.file)
    assertEquals(new File(removedLog.dir, txnIndexName), removedSegment.txnIndex.file)

    // Try to detect the case where a new index type was added and we forgot to update the pointer
    // This will only catch cases where the index file is created eagerly instead of lazily
    indexFilesOnDiskBeforeDelete.foreach { fileBeforeDelete =>
      val fileInIndex = indexFilesAfterDelete.find(_.getName == fileBeforeDelete.getName)
      assertEquals(Some(fileBeforeDelete.getName), fileInIndex.map(_.getName),
        s"Could not find index file ${fileBeforeDelete.getName} in indexFilesAfterDelete")
      assertNotEquals("File reference was not updated in index", fileBeforeDelete.getAbsolutePath,
        fileInIndex.get.getAbsolutePath)
    }

    time.sleep(logManager.InitialTaskDelayMs)
    assertTrue(logManager.hasLogsToBeDeleted, "Logs deleted too early")
    time.sleep(logManager.currentDefaultConfig.fileDeleteDelayMs - logManager.InitialTaskDelayMs)
    assertFalse(logManager.hasLogsToBeDeleted, "Logs not deleted")
  }

  @Test
  def testCreateAndDeleteOverlyLongTopic(): Unit = {
    val invalidTopicName = String.join("", Collections.nCopies(253, "x"))
    logManager.getOrCreateLog(new TopicPartition(invalidTopicName, 0), topicId = None)
    logManager.asyncDelete(new TopicPartition(invalidTopicName, 0))
  }

  @Test
  def testCheckpointForOnlyAffectedLogs(): Unit = {
    val tps = Seq(
      new TopicPartition("test-a", 0),
      new TopicPartition("test-a", 1),
      new TopicPartition("test-a", 2),
      new TopicPartition("test-b", 0),
      new TopicPartition("test-b", 1))

    val allLogs = tps.map(logManager.getOrCreateLog(_, topicId = None))
    allLogs.foreach { log =>
      for (_ <- 0 until 50)
        log.appendAsLeader(TestUtils.singletonRecords("test".getBytes), leaderEpoch = 0)
      log.flush(false)
    }

    logManager.checkpointRecoveryOffsetsInDir(logDir)

    val checkpoints = new OffsetCheckpointFile(new File(logDir, LogManager.RecoveryPointCheckpointFile)).read()

    tps.zip(allLogs).foreach { case (tp, log) =>
      assertEquals(checkpoints(tp), log.recoveryPoint,
        "Recovery point should equal checkpoint")
    }
  }

  private def readLog(log: UnifiedLog, offset: Long, maxLength: Int = 1024): FetchDataInfo = {
    log.read(offset, maxLength, isolation = FetchLogEnd, minOneMessage = true)
  }

  /**
   * Test when a configuration of a topic is updated while its log is getting initialized,
   * the config is refreshed when log initialization is finished.
   */
  @Test
  def testTopicConfigChangeUpdatesLogConfig(): Unit = {
    logManager.shutdown()
    val spyConfigRepository = spy(new MockConfigRepository)
    logManager = createLogManager(configRepository = spyConfigRepository)
    val spyLogManager = spy(logManager)
    val mockLog = mock(classOf[UnifiedLog])

    val testTopicOne = "test-topic-one"
    val testTopicTwo = "test-topic-two"
    val testTopicOnePartition = new TopicPartition(testTopicOne, 1)
    val testTopicTwoPartition = new TopicPartition(testTopicTwo, 1)

    spyLogManager.initializingLog(testTopicOnePartition)
    spyLogManager.initializingLog(testTopicTwoPartition)

    spyLogManager.topicConfigUpdated(testTopicOne)

    spyLogManager.finishedInitializingLog(testTopicOnePartition, Some(mockLog))
    spyLogManager.finishedInitializingLog(testTopicTwoPartition, Some(mockLog))

    // testTopicOne configs loaded again due to the update
    verify(spyLogManager).initializingLog(ArgumentMatchers.eq(testTopicOnePartition))
    verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(testTopicOnePartition), ArgumentMatchers.any())
    verify(spyConfigRepository, times(1)).topicConfig(testTopicOne)

    // testTopicTwo configs not loaded again since there was no update
    verify(spyLogManager).initializingLog(ArgumentMatchers.eq(testTopicTwoPartition))
    verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(testTopicTwoPartition), ArgumentMatchers.any())
    verify(spyConfigRepository, never).topicConfig(testTopicTwo)
  }

  /**
   * Test if an error occurs when creating log, log manager removes corresponding
   * topic partition from the list of initializing partitions and no configs are retrieved.
   */
  @Test
  def testConfigChangeGetsCleanedUp(): Unit = {
    logManager.shutdown()
    val spyConfigRepository = spy(new MockConfigRepository)
    logManager = createLogManager(configRepository = spyConfigRepository)
    val spyLogManager = spy(logManager)

    val testTopicPartition = new TopicPartition("test-topic", 1)
    spyLogManager.initializingLog(testTopicPartition)
    spyLogManager.finishedInitializingLog(testTopicPartition, None)

    assertTrue(logManager.partitionsInitializing.isEmpty)
    verify(spyConfigRepository, never).topicConfig(testTopicPartition.topic)
  }

  /**
   * Test when a broker configuration change happens all logs in process of initialization
   * pick up latest config when finished with initialization.
   */
  @Test
  def testBrokerConfigChangeDeliveredToAllLogs(): Unit = {
    logManager.shutdown()
    val spyConfigRepository = spy(new MockConfigRepository)
    logManager = createLogManager(configRepository = spyConfigRepository)
    val spyLogManager = spy(logManager)
    val mockLog = mock(classOf[UnifiedLog])

    val testTopicOne = "test-topic-one"
    val testTopicTwo = "test-topic-two"
    val testTopicOnePartition = new TopicPartition(testTopicOne, 1)
    val testTopicTwoPartition = new TopicPartition(testTopicTwo, 1)

    spyLogManager.initializingLog(testTopicOnePartition)
    spyLogManager.initializingLog(testTopicTwoPartition)

    spyLogManager.brokerConfigUpdated()

    spyLogManager.finishedInitializingLog(testTopicOnePartition, Some(mockLog))
    spyLogManager.finishedInitializingLog(testTopicTwoPartition, Some(mockLog))

    verify(spyConfigRepository, times(1)).topicConfig(testTopicOne)
    verify(spyConfigRepository, times(1)).topicConfig(testTopicTwo)
  }

  /**
   * Test when compact is removed that cleaning of the partitions is aborted.
   */
  @Test
  def testTopicConfigChangeStopCleaningIfCompactIsRemoved(): Unit = {
    logManager.shutdown()
    logManager = createLogManager(configRepository = new MockConfigRepository)
    val spyLogManager = spy(logManager)

    val topic = "topic"
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)

    val oldProperties = new Properties()
    oldProperties.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    val oldLogConfig = LogConfig.fromProps(logConfig.originals, oldProperties)

    val log0 = spyLogManager.getOrCreateLog(tp0, topicId = None)
    log0.updateConfig(oldLogConfig)
    val log1 = spyLogManager.getOrCreateLog(tp1, topicId = None)
    log1.updateConfig(oldLogConfig)

    assertEquals(Set(log0, log1), spyLogManager.logsByTopic(topic).toSet)

    val newProperties = new Properties()
    newProperties.put(LogConfig.CleanupPolicyProp, LogConfig.Delete)

    spyLogManager.updateTopicConfig(topic, newProperties)

    assertTrue(log0.config.delete)
    assertTrue(log1.config.delete)
    assertFalse(log0.config.compact)
    assertFalse(log1.config.compact)

    verify(spyLogManager, times(1)).topicConfigUpdated(topic)
    verify(spyLogManager, times(1)).abortCleaning(tp0)
    verify(spyLogManager, times(1)).abortCleaning(tp1)
  }

  /**
   * Test even if no log is getting initialized, if config change events are delivered
   * things continue to work correctly. This test should not throw.
   *
   * This makes sure that events can be delivered even when no log is getting initialized.
   */
  @Test
  def testConfigChangesWithNoLogGettingInitialized(): Unit = {
    logManager.brokerConfigUpdated()
    logManager.topicConfigUpdated("test-topic")
    assertTrue(logManager.partitionsInitializing.isEmpty)
  }

  private def appendRecordsToLog(time: MockTime, parentLogDir: File, partitionId: Int, brokerTopicStats: BrokerTopicStats, expectedSegmentsPerLog: Int): Unit = {
    def createRecord = TestUtils.singletonRecords(value = "test".getBytes, timestamp = time.milliseconds)
    val tpFile = new File(parentLogDir, s"$name-$partitionId")
    val segmentBytes = 1024

    val log = LogTestUtils.createLog(tpFile, logConfig, brokerTopicStats, time.scheduler, time, 0, 0,
      5 * 60 * 1000, new ProducerStateManagerConfig(kafka.server.Defaults.ProducerIdExpirationMs), kafka.server.Defaults.ProducerIdExpirationCheckIntervalMs)

    assertTrue(expectedSegmentsPerLog > 0)
    // calculate numMessages to append to logs. It'll create "expectedSegmentsPerLog" log segments with segment.bytes=1024
    val numMessages = Math.floor(segmentBytes * expectedSegmentsPerLog / createRecord.sizeInBytes).asInstanceOf[Int]
    try {
      for (_ <- 0 until numMessages) {
        log.appendAsLeader(createRecord, leaderEpoch = 0)
      }

      assertEquals(expectedSegmentsPerLog, log.numberOfSegments)
    } finally {
      log.close()
    }
  }

  private def verifyRemainingLogsToRecoverMetric(spyLogManager: LogManager, expectedParams: Map[String, Int]): Unit = {
    val spyLogManagerClassName = spyLogManager.getClass().getSimpleName
    // get all `remainingLogsToRecover` metrics
    val logMetrics: ArrayBuffer[Gauge[Int]] = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .filter { case (metric, _) => metric.getType == s"$spyLogManagerClassName" && metric.getName == "remainingLogsToRecover" }
      .map { case (_, gauge) => gauge }
      .asInstanceOf[ArrayBuffer[Gauge[Int]]]

    assertEquals(expectedParams.size, logMetrics.size)

    val capturedPath: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    val expectedCallTimes = expectedParams.values.sum
    verify(spyLogManager, times(expectedCallTimes)).decNumRemainingLogs(any[ConcurrentMap[String, Int]], capturedPath.capture());

    val paths = capturedPath.getAllValues
    expectedParams.foreach {
      case (path, totalLogs) =>
        // make sure each path is called "totalLogs" times, which means it is decremented to 0 in the end
        assertEquals(totalLogs, Collections.frequency(paths, path))
    }

    // expected the end value is 0
    logMetrics.foreach { gauge => assertEquals(0, gauge.value()) }
  }

  private def verifyRemainingSegmentsToRecoverMetric(spyLogManager: LogManager,
                                                     logDirs: Seq[File],
                                                     recoveryThreadsPerDataDir: Int,
                                                     mockMap: ConcurrentHashMap[String, Int],
                                                     expectedParams: Map[String, Int]): Unit = {
    val spyLogManagerClassName = spyLogManager.getClass().getSimpleName
    // get all `remainingSegmentsToRecover` metrics
    val logSegmentMetrics: ArrayBuffer[Gauge[Int]] = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
          .filter { case (metric, _) => metric.getType == s"$spyLogManagerClassName" && metric.getName == "remainingSegmentsToRecover" }
          .map { case (_, gauge) => gauge }
          .asInstanceOf[ArrayBuffer[Gauge[Int]]]

    // expected each log dir has 1 metrics for each thread
    assertEquals(recoveryThreadsPerDataDir * logDirs.size, logSegmentMetrics.size)

    val capturedThreadName: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val capturedNumRemainingSegments: ArgumentCaptor[Int] = ArgumentCaptor.forClass(classOf[Int])

    // Since we'll update numRemainingSegments from totalSegments to 0 for each thread, so we need to add 1 here
    val expectedCallTimes = expectedParams.values.map( num => num + 1 ).sum
    verify(mockMap, times(expectedCallTimes)).put(capturedThreadName.capture(), capturedNumRemainingSegments.capture());

    // expected the end value is 0
    logSegmentMetrics.foreach { gauge => assertEquals(0, gauge.value()) }

    val threadNames = capturedThreadName.getAllValues
    val numRemainingSegments = capturedNumRemainingSegments.getAllValues

    expectedParams.foreach {
      case (threadName, totalSegments) =>
        // make sure we update the numRemainingSegments from totalSegments to 0 in order for each thread
        var expectedCurRemainingSegments = totalSegments + 1
        for (i <- 0 until threadNames.size) {
          if (threadNames.get(i).contains(threadName)) {
            expectedCurRemainingSegments -= 1
            assertEquals(expectedCurRemainingSegments, numRemainingSegments.get(i))
          }
        }
        assertEquals(0, expectedCurRemainingSegments)
    }
  }

  private def verifyLogRecoverMetricsRemoved(spyLogManager: LogManager): Unit = {
    val spyLogManagerClassName = spyLogManager.getClass().getSimpleName
    // get all `remainingLogsToRecover` metrics
    def logMetrics: mutable.Set[MetricName] = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala
      .filter { metric => metric.getType == s"$spyLogManagerClassName" && metric.getName == "remainingLogsToRecover" }

    assertTrue(logMetrics.isEmpty)

    // get all `remainingSegmentsToRecover` metrics
    val logSegmentMetrics: mutable.Set[MetricName] = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala
      .filter { metric => metric.getType == s"$spyLogManagerClassName" && metric.getName == "remainingSegmentsToRecover" }

    assertTrue(logSegmentMetrics.isEmpty)
  }

  @Test
  def testLogRecoveryMetrics(): Unit = {
    logManager.shutdown()
    val logDir1 = TestUtils.tempDir()
    val logDir2 = TestUtils.tempDir()
    val logDirs = Seq(logDir1, logDir2)
    val recoveryThreadsPerDataDir = 2
    // create logManager with expected recovery thread number
    logManager = createLogManager(logDirs, recoveryThreadsPerDataDir = recoveryThreadsPerDataDir)
    val spyLogManager = spy(logManager)

    assertEquals(2, spyLogManager.liveLogDirs.size)

    val mockTime = new MockTime()
    val mockMap = mock(classOf[ConcurrentHashMap[String, Int]])
    val mockBrokerTopicStats = mock(classOf[BrokerTopicStats])
    val expectedSegmentsPerLog = 2

    // create log segments for log recovery in each log dir
    appendRecordsToLog(mockTime, logDir1, 0, mockBrokerTopicStats, expectedSegmentsPerLog)
    appendRecordsToLog(mockTime, logDir2, 1, mockBrokerTopicStats, expectedSegmentsPerLog)

    // intercept loadLog method to pass expected parameter to do log recovery
    doAnswer { invocation =>
      val dir: File = invocation.getArgument(0)
      val topicConfigOverrides: mutable.Map[String, LogConfig] = invocation.getArgument(5)

      val topicPartition = UnifiedLog.parseTopicPartitionName(dir)
      val config = topicConfigOverrides.getOrElse(topicPartition.topic, logConfig)

      UnifiedLog(
        dir = dir,
        config = config,
        logStartOffset = 0,
        recoveryPoint = 0,
        maxTransactionTimeoutMs = 5 * 60 * 1000,
        producerStateManagerConfig = new ProducerStateManagerConfig(5 * 60 * 1000),
        producerIdExpirationCheckIntervalMs = kafka.server.Defaults.ProducerIdExpirationCheckIntervalMs,
        scheduler = mockTime.scheduler,
        time = mockTime,
        brokerTopicStats = mockBrokerTopicStats,
        logDirFailureChannel = mock(classOf[LogDirFailureChannel]),
        // not clean shutdown
        lastShutdownClean = false,
        topicId = None,
        keepPartitionMetadataFile = false,
        // pass mock map for verification later
        numRemainingSegments = mockMap)

    }.when(spyLogManager).loadLog(any[File], any[Boolean], any[Map[TopicPartition, Long]], any[Map[TopicPartition, Long]],
      any[LogConfig], any[Map[String, LogConfig]], any[ConcurrentMap[String, Int]])

    // do nothing for removeLogRecoveryMetrics for metrics verification
    doNothing().when(spyLogManager).removeLogRecoveryMetrics()

    // start the logManager to do log recovery
    spyLogManager.startup(Set.empty)

    // make sure log recovery metrics are added and removed
    verify(spyLogManager, times(1)).addLogRecoveryMetrics(any[ConcurrentMap[String, Int]], any[ConcurrentMap[String, Int]])
    verify(spyLogManager, times(1)).removeLogRecoveryMetrics()

    // expected 1 log in each log dir since we created 2 partitions with 2 log dirs
    val expectedRemainingLogsParams = Map[String, Int](logDir1.getAbsolutePath -> 1, logDir2.getAbsolutePath -> 1)
    verifyRemainingLogsToRecoverMetric(spyLogManager, expectedRemainingLogsParams)

    val expectedRemainingSegmentsParams = Map[String, Int](
      logDir1.getAbsolutePath -> expectedSegmentsPerLog, logDir2.getAbsolutePath -> expectedSegmentsPerLog)
    verifyRemainingSegmentsToRecoverMetric(spyLogManager, logDirs, recoveryThreadsPerDataDir, mockMap, expectedRemainingSegmentsParams)
  }

  @Test
  def testLogRecoveryMetricsShouldBeRemovedAfterLogRecovered(): Unit = {
    logManager.shutdown()
    val logDir1 = TestUtils.tempDir()
    val logDir2 = TestUtils.tempDir()
    val logDirs = Seq(logDir1, logDir2)
    val recoveryThreadsPerDataDir = 2
    // create logManager with expected recovery thread number
    logManager = createLogManager(logDirs, recoveryThreadsPerDataDir = recoveryThreadsPerDataDir)
    val spyLogManager = spy(logManager)

    assertEquals(2, spyLogManager.liveLogDirs.size)

    // start the logManager to do log recovery
    spyLogManager.startup(Set.empty)

    // make sure log recovery metrics are added and removed once
    verify(spyLogManager, times(1)).addLogRecoveryMetrics(any[ConcurrentMap[String, Int]], any[ConcurrentMap[String, Int]])
    verify(spyLogManager, times(1)).removeLogRecoveryMetrics()

    verifyLogRecoverMetricsRemoved(spyLogManager)
  }

  @Test
  def testMetricsExistWhenLogIsRecreatedBeforeDeletion(): Unit = {
    val topicName = "metric-test"
    def logMetrics: mutable.Set[MetricName] = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala.
      filter(metric => metric.getType == "Log" && metric.getScope.contains(topicName))

    val tp = new TopicPartition(topicName, 0)
    val metricTag = s"topic=${tp.topic},partition=${tp.partition}"

    def verifyMetrics(): Unit = {
      assertEquals(LogMetricNames.allMetricNames.size, logMetrics.size)
      logMetrics.foreach { metric =>
        assertTrue(metric.getMBeanName.contains(metricTag))
      }
    }

    // Create the Log and assert that the metrics are present
    logManager.getOrCreateLog(tp, topicId = None)
    verifyMetrics()

    // Trigger the deletion and assert that the metrics have been removed
    val removedLog = logManager.asyncDelete(tp).get
    assertTrue(logMetrics.isEmpty)

    // Recreate the Log and assert that the metrics are present
    logManager.getOrCreateLog(tp, topicId = None)
    verifyMetrics()

    // Advance time past the file deletion delay and assert that the removed log has been deleted but the metrics
    // are still present
    time.sleep(logConfig.fileDeleteDelayMs + 1)
    assertTrue(removedLog.logSegments.isEmpty)
    verifyMetrics()
  }

  @Test
  def testMetricsAreRemovedWhenMovingCurrentToFutureLog(): Unit = {
    val dir1 = TestUtils.tempDir()
    val dir2 = TestUtils.tempDir()
    logManager = createLogManager(Seq(dir1, dir2))
    logManager.startup(Set.empty)

    val topicName = "future-log"
    def logMetrics: mutable.Set[MetricName] = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala.
      filter(metric => metric.getType == "Log" && metric.getScope.contains(topicName))

    val tp = new TopicPartition(topicName, 0)
    val metricTag = s"topic=${tp.topic},partition=${tp.partition}"

    def verifyMetrics(logCount: Int): Unit = {
      assertEquals(LogMetricNames.allMetricNames.size * logCount, logMetrics.size)
      logMetrics.foreach { metric =>
        assertTrue(metric.getMBeanName.contains(metricTag))
      }
    }

    // Create the current and future logs and verify that metrics are present for both current and future logs
    logManager.maybeUpdatePreferredLogDir(tp, dir1.getAbsolutePath)
    logManager.getOrCreateLog(tp, topicId = None)
    logManager.maybeUpdatePreferredLogDir(tp, dir2.getAbsolutePath)
    logManager.getOrCreateLog(tp, isFuture = true, topicId = None)
    verifyMetrics(2)

    // Replace the current log with the future one and verify that only one set of metrics are present
    logManager.replaceCurrentWithFutureLog(tp)
    verifyMetrics(1)
    // the future log is gone, so we have to make sure the metrics gets gone also.
    assertEquals(0, logMetrics.count(m => m.getMBeanName.contains("is-future")))

    // Trigger the deletion of the former current directory and verify that one set of metrics is still present
    time.sleep(logConfig.fileDeleteDelayMs + 1)
    verifyMetrics(1)
  }

  @Test
  def testWaitForAllToComplete(): Unit = {
    var invokedCount = 0
    val success: Future[Boolean] = Mockito.mock(classOf[Future[Boolean]])
    Mockito.when(success.get()).thenAnswer { _ =>
      invokedCount += 1
      true
    }
    val failure: Future[Boolean] = Mockito.mock(classOf[Future[Boolean]])
    Mockito.when(failure.get()).thenAnswer{ _ =>
      invokedCount += 1
      throw new RuntimeException
    }

    var failureCount = 0
    // all futures should be evaluated
    assertFalse(LogManager.waitForAllToComplete(Seq(success, failure), _ => failureCount += 1))
    assertEquals(2, invokedCount)
    assertEquals(1, failureCount)
    assertFalse(LogManager.waitForAllToComplete(Seq(failure, success), _ => failureCount += 1))
    assertEquals(4, invokedCount)
    assertEquals(2, failureCount)
    assertTrue(LogManager.waitForAllToComplete(Seq(success, success), _ => failureCount += 1))
    assertEquals(6, invokedCount)
    assertEquals(2, failureCount)
    assertFalse(LogManager.waitForAllToComplete(Seq(failure, failure), _ => failureCount += 1))
    assertEquals(8, invokedCount)
    assertEquals(4, failureCount)
  }
}
